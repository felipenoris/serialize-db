# Etapa 5: motor Redshift

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.engine.redshift` implementa o mesmo protocolo. A conexão vem de `RedshiftConfig`:
`workgroup` (serverless), `database` (o banco da conexão), `share_database` (o banco do datashare que
guarda o esquema, quando não é o da conexão), `schema`, o `iam_role` opcional do `COPY` e do `UNLOAD`,
e `host`, `port`, `user` e `password` para o par informado, com os padrões nas variáveis
`SERIALIZE_DB_REDSHIFT_*`.

O caminho é o de [`../examples/redshift_native.py`](../examples/redshift_native.py), executado no
ambiente alvo em 2026-09-20: `redshift-serverless:GetWorkgroup` para o endereço e a porta,
`GetCredentials` para o par usuário e senha derivado da identidade IAM, e
`redshift_connector.connect` com esse par. A credencial dura no máximo uma hora, e `connect` a pede a
cada conexão em vez de guardá-la. Um par informado em `user` e `password` entra na mesma chamada. O
IAM interno do `redshift_connector` (`iam=True`) e o cluster provisionado
(`redshift:GetClusterCredentials`) não são caminhos da biblioteca: ninguém os executou no ambiente
alvo, que não tem cluster, e a regra do projeto é repetir o que rodou lá. A Data API não é caminho de
conexão da biblioteca (decisão do usuário de 2026-09-20): ela devolve `DECIMAL`, data e hora como
texto e limita o resultado a 500 MB, o que não serve à troca de lotes Arrow; ela fica nos exemplos e
em `RS-10`.

`connect` não passa `timeout` ao `redshift_connector`: lá ele é o tempo limite do socket, para
conectar e para ler, e um `COPY` ou um `UNLOAD` dura mais que qualquer espera razoável (10 s abortaram
uma visão de sistema no ambiente alvo em 2026-09-20, e a conexão não voltou a servir). Uma rede morta
aparece como o tempo limite do sistema.

`connect` roda `USE <share_database>` logo depois de abrir a sessão, e a partir daí
`qualified(name)` é `esquema.tabela`: é o caminho executado no ambiente alvo em 2026-09-20
([`../examples/redshift_copy_unload.py`](../examples/redshift_copy_unload.py)), onde o `CREATE`, o
`COPY`, o `SELECT` e o `UNLOAD` passaram assim. O nome em três partes fica para quem está conectado
a outro banco, como a Data API, e no SQLAlchemy ele só atravessa com `quoted_name(..., quote=False)`
([`sqlalchemy.md`](sqlalchemy.md)).

`credentials_clause()` monta como o `COPY` e o `UNLOAD` alcançam o S3, e quem decide é o `iam_role`
da configuração: com ele, `IAM_ROLE` com o ARN ou com `default`; sem ele, `ACCESS_KEY_ID`,
`SECRET_ACCESS_KEY` e `SESSION_TOKEN` da sessão `boto3`, que é o caminho do ambiente alvo, onde o
namespace não tem papel associado e por isso nem um ARN explícito funcionaria (`RS-6`). As
credenciais expiram, então a cláusula é montada por comando, nunca guardada; e **nenhum texto que a
carregue vai para log, para o relatório ou para arquivo**.

As restrições da escrita num datashare estão em [`redshift.md`](redshift.md). O `COPY` roda sem
cláusula `COMPUPDATE` alguma, e o de Parquet nem a aceita: a codificação das colunas vem do DDL ou
de `ENCODE AUTO`, e `ANALYZE COMPRESSION` numa amostra real é o que a fixa. O motivo de um `COPY`
recusado está em `sys_load_error_detail`, que a sessão lê no ambiente alvo (2026-09-20);
`stl_load_errors` cobre só clusters provisionados e é negada a um usuário comum.

| Primitiva | Redshift |
| --- | --- |
| `connect(config)` | `GetWorkgroup`, `GetCredentials` e `redshift_connector.connect` sem `timeout`; `USE <share_database>` quando o esquema vem de um datashare, conferido por `current_database()`; `search_path` no esquema; `cursor.paramstyle = "named"`; uma conexão por thread num `threading.local`, aberta no primeiro uso e fechada em `cleanup`, e `connection` devolve a da thread. A senha dura no máximo uma hora, então uma conexão derrubada pelo servidor é reaberta com credencial nova, uma vez por comando, e o comando é repetido; o que o servidor faz com uma conexão cuja senha expirou, e se ela cai no meio de um `COPY`, é questão em aberto ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). |
| `ingest(table, uri, version, partitions=None, materialize=True)` | `copy_manifest` dos arquivos dessas partições, `COPY ... FORMAT AS PARQUET MANIFEST` com a cláusula de credenciais numa staging sem a coluna de partição criada por `ddl`, e `INSERT INTO exec_<id>_<tabela> SELECT *, '<valor>'`; `JSON_PARSE` nas colunas `SUPER`. O `COPY` também lê um prefixo de pasta direto, sem manifesto, e converte `int32` da origem para a coluna `BIGINT` do contrato. |
| `stream(statement_or_sql, params=None, batch_size=100_000, prefetch=2)` | O statement compilado para o Redshift, ou o texto com `{prefix}` em `exec_<id>_`, executado numa conexão própria da thread auxiliar; cada lote é `RecordBatch.from_pylist` dos dicionários por nome de `cursor.fetchmany(batch_size)`, com o esquema do statement, ou os lotes de `ParquetFile.iter_batches` de um `UNLOAD` acima de um limite de linhas; o mesmo `BatchStream` do DuckDB. O `redshift_connector` é Python puro, e a thread auxiliar compete pelo GIL com o cliente ([`PLAN.md`](PLAN.md)). |
| `query(statement, **params)` | `stream(statement, params).read_all()`. |
| `execute(sql, params)` | `stream(sql, params).read_all()`, vazia para um comando sem resultado. |
| `loader(table, queue_depth=2)` | `write` faz `cast(batch, table)` na thread do cliente; a thread auxiliar grava um row group por lote com `ParquetWriter.write_batch` num arquivo de `staging/<execution_id>/`, e `close` fecha o arquivo e roda o `COPY`: nada entra antes dele, e uma exceção dentro do `with` apaga o arquivo sem `COPY`. |
| `load(table, data)` | Os lotes de `data` pelo `loader`; uma `pa.Table` abaixo de um limite de linhas entra por um único `INSERT` multilinha montado de `to_pylist()`, numa ida ao servidor. |
| `audit(table, partitions, **opcoes)` | O mesmo texto compilado para o Redshift; as demais partições e a tabela referenciada entram em stagings só com as colunas da chave, por `COPY ... MANIFEST`. |
| `export_partition(table, value, mode=None)` | `mode` é `"register"` ou `"rewrite"` (flag do usuário, 2026-09-21): do argumento, de `run.publish(export_mode=...)` ou de `SERIALIZE_DB_EXPORT_MODE`, `"register"` por omissão; a mesma partição sai igual pelos dois, e o teste de integração os compara. Os dois começam por `UNLOAD ('<select do contrato>') TO '<destino>/' PARTITION BY (<coluna de partição>) FORMAT PARQUET MANIFEST VERBOSE` com a cláusula de credenciais, que passou no ambiente alvo em 2026-09-21 a partir de uma tabela do datashare ([`../examples/redshift_manifest.py`](../examples/redshift_manifest.py), [`POC.md`](POC.md)): grava na convenção Hive com a coluna de partição fora dos arquivos, e `PARALLEL OFF` quando a partição cabe num arquivo, porque o `UNLOAD` fragmenta por slice (500.000 linhas em 32 arquivos) e `MAXFILESIZE` é teto, não piso. **`register`**: o destino é `<uri>/<execution_id>/`, dentro da pasta da tabela e vazio por construção, porque o `UNLOAD` falha num destino com arquivos e a pasta da partição já tem os das versões anteriores; `register_files` recebe `<execution_id>/<coluna>=<valor>/<arquivo>` por entrada do manifesto, o `count(*)` da fonte em `expected_rows`, faz as conferências da [etapa 3](PLAN-STAGE-3.md) antes do commit e a releitura depois. Os dados não passam pela máquina local, e os arquivos ficam como o Redshift os gravou até a compactação: `TIMESTAMP` em `INT96`, sem estatística de mínimo e máximo, lido de volta como `timestamp[us]` pelo delta-rs e pelo `delta_scan`, e `DECIMAL` em `FIXED_LEN_BYTE_ARRAY`. **`rewrite`**: o destino é `staging/<execution_id>/<tabela>/`, fora da tabela; a partição volta pelo leitor da [etapa 7](PLAN-STAGE-7.md) (`read_parquet` da pasta Hive, `INT96` a microssegundos, a coluna de partição com o valor do caminho, `cast`) e entra por `publish_partition`, onde o `write_deltalake` calcula estatística e valores de partição e recusa o que o contrato recusa. Os dados passam pela máquina local, e a memória do `write_deltalake` de uma partição de `cad_lancamentos` é a medição em aberto ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)), que a flag faz com o mesmo `UNLOAD`; `cleanup` apaga o staging. |
| `cleanup()` | `DROP TABLE` de `exec_<id>_*` e da staging; os objetos de `staging/<execution_id>/` apagados. |

O identificador de execução entra no nome do sandbox normalizado para `[a-z0-9_]`, dentro dos
127 bytes de um identificador do Redshift. Onde as tabelas `exec_<id>_*` nascem quando o esquema vem
de um datashare é questão em aberto entre o próprio datashare, onde o `CREATE TABLE` passou, e as
tabelas temporárias do banco da conexão, que morrem com a sessão; o banco local saiu das
alternativas em 2026-09-20, porque `has_database_privilege(dev, CREATE)` é falso e `TEMP` é
verdadeiro (`RS-9`, [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)).

Testes: `tests/test_engine_redshift.py` compara o SQL gerado (`COPY`, `INSERT ... SELECT`,
`UNLOAD`, DDL da staging) com texto esperado, sem conexão; os testes marcados `redshift` rodam a
mesma sequência com uma amostra no esquema autorizado, depois do
`test_redshift.py` da [etapa 0](PLAN-STAGE-0.md). Provas de conceito: `test_redshift.py` (sessão e
`paramstyle` nomeado, o `fetchmany` por lotes, banco do esquema e o `USE`, DDL, `COPY ... MANIFEST`, lista de
colunas e `FILLRECORD`, `VARCHAR`, `SUPER`, `UNLOAD`, ciclo da Data API), `test_sqlalchemy.py`
(`test_redshift_dialect_compiles_dml`, `test_three_part_name_needs_quoted_name_without_quotes`,
`test_sandbox_copy_of_table_and_schema_files_diff`) e `test_stdlib.py::test_execution_identifiers`
(o prefixo do sandbox).

Paralelismo: `test_redshift.py::test_parallel_copy_and_unload_on_two_connections`, dois `COPY` e dois
`UNLOAD` em tabelas distintas, uma conexão por thread, limitados pelas slots do WLM.
