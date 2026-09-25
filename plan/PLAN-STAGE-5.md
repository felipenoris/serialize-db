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
| `connect(config)` | `GetWorkgroup`, `GetCredentials` e `redshift_connector.connect` sem `timeout` e com `max_prepared_statements=0`, porque o cache de prepared statements do driver reaproveita um statement preparado antes de um `TRUNCATE` e o datashare o recusa com `34510` (leitura de 2026-09-21, [`redshift.md`](redshift.md)); `USE <share_database>` quando o esquema vem de um datashare, sem comando de conferência e sem criar a tabela de controle da [etapa 8](PLAN-STAGE-8.md) (decisão do usuário de 2026-09-23): um banco inexistente faz o `USE` falhar, e o primeiro comando em duas partes confirma a troca, porque `current_database()` continua `dev` depois dele (leitura de 2026-09-21); `search_path` no esquema; `cursor.paramstyle = "named"`; uma conexão por execução, aberta na construção do motor e fechada em `cleanup`, e um `threading.RLock` que todo comando toma pelo tempo do comando (decisão do usuário de 2026-09-22, a mesma do motor DuckDB): `session()` dá essa conexão ao cliente com o lock tomado pelo bloco, reentrante na mesma thread. A senha dura no máximo uma hora, e o Redshift Serverless encerra a sessão ociosa há 3.600 s e a transação aberta e inativa há 21.600 s ([`redshift.md`](redshift.md)), então uma conexão derrubada pelo servidor é reaberta com credencial nova, uma vez por comando, e o comando é repetido; a reconexão perde a tabela temporária que o pipeline tenha criado na sessão, e o log a nomeia; o que o servidor faz com uma conexão cuja senha expirou, e se ela cai no meio de um `COPY`, é questão em aberto ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). |
| `new_session()` | Uma sessão a mais para o que roda em paralelo: outra conexão pelo caminho de `connect` (credencial temporária própria, `USE` e `max_prepared_statements=0`), com o seu `RLock` e as mesmas primitivas, gerenciador de contexto. Ela vê as tabelas `exec_<id>_*` que a sessão principal confirmou e não as temporárias dela; o fim do `with` e o `cleanup` dela fecham só essa conexão, sem apagar tabela. `run.ingest` de mais de uma tabela abre uma por tabela; dois `COPY` em conexões abertas dentro da tarefa levaram 4,3 s e 3,8 s no ambiente alvo (2026-09-21, [`POC.md`](POC.md)). |
| `ingest(table, uri, version, partitions=None, materialize=True)` | `copy_manifest` dos arquivos dessas partições, `COPY ... FORMAT AS PARQUET MANIFEST FILLRECORD` com a cláusula de credenciais numa staging sem a coluna de partição criada por `ddl` (`FILLRECORD` carrega um arquivo anterior a uma coluna nova com ela nula, leitura de 2026-09-21; a cláusula em todo `COPY` é a proposta da [etapa 8](PLAN-STAGE-8.md)), e `INSERT INTO exec_<id>_<tabela> (<colunas>) SELECT ..., '<valor>', ...` na ordem do contrato, com `JSON_PARSE` nas colunas `SUPER`. O `COPY` também lê um prefixo de pasta direto, sem manifesto, e converte `int32` da origem para a coluna `BIGINT` do contrato. |
| `published(table, uri, version)` | A versão fixada como origem de consulta, sem ocupar o nome do modelo no sandbox: a staging `exec_<id>_<tabela>_publicado_<versão>`, criada por `ddl` e carregada uma vez por execução e por versão com as colunas do contrato por `copy_manifest` da versão fixada e `COPY ... FORMAT AS PARQUET MANIFEST`, e o `FromClause` devolvido é ela. É por ele que o pipeline lê as partições publicadas da tabela que o `loader` grava ([etapa 4](PLAN-STAGE-4.md), decisão do usuário de 2026-09-22); a auditoria usa a mesma staging, carregada só quando a junção roda, e `cleanup` a apaga. Chamado de novo depois de `run.publish` avançar `versions`, ele carrega a versão nova inteira numa staging nova, como o `delta_scan` com `version := v` do motor DuckDB, e as duas ficam no esquema até o `cleanup` (decisão do usuário de 2026-09-25); o código ainda não segue a decisão e devolve a staging da primeira versão ([`CURRENT_STATE.md`](CURRENT_STATE.md)). |
| `stream(statement_or_sql, params=None, batch_size=100_000)` | Sempre por `UNLOAD` (decisão do usuário de 2026-09-23): o motor não sabe o tamanho do resultado antes do `execute`, e o `redshift_connector` o materializa inteiro ali (leitura do código, 2026-09-21). O statement compilado para o Redshift pela cópia prefixada, ou o texto com `{prefix}` em `exec_<id>_`, com os valores do cliente como literais, entra em `UNLOAD ('<select>') TO 'staging/<execution_id>/stream/<uuid>/' <credenciais> FORMAT AS PARQUET MANIFEST VERBOSE PARALLEL OFF`, na sessão do motor, sob o lock e na thread de quem chama; `PARALLEL OFF` mantém a ordem do `ORDER BY`, como no DuckDB. O lock sai no fim do `UNLOAD`, e a thread auxiliar lê os lotes dos arquivos do manifesto por `ParquetFile.iter_batches(batch_size)`, com o esquema do statement, enquanto o cliente trabalha no lote anterior; a interface é o `BatchStream` do DuckDB, com uma fila de dois lotes, e `close` apaga o prefixo do `stream`. O `UNLOAD` recusa `LIMIT` no `select` externo (`42601`, leitura de 2026-09-23), e um resultado limitado cabe em `query`. O `UNLOAD` de um resultado vazio passa sem gravar manifesto nem arquivo (leitura de 2026-09-23): sem manifesto, `pg_last_unload_count()`, lido na mesma sessão, separa o `stream` vazio, com 0, da falta do manifesto, que sobe. |
| `query(statement_or_sql, params=None)` | O statement compilado para o Redshift pela cópia prefixada, com os `bindparam` do cliente e as constantes como parâmetros do driver, ou o texto com `{prefix}` em `exec_<id>_` por `bind`, sob o lock, e o resultado inteiro do cursor numa `pa.Table` montada por colunas, com o esquema de `schema_from_row_description`; vazia para um comando sem resultado. Os valores são os de `stream(statement_or_sql, params).read_all()`, e o teste de integração compara os dois. `execute` saiu da interface (decisão do usuário de 2026-09-23). |
| `loader(table, queue_depth=2)` | Um nome já ocupado no sandbox é recusado com `SandboxError` na abertura, antes do primeiro lote, como na [etapa 4](PLAN-STAGE-4.md) (decisão do usuário de 2026-09-23); `write` faz `cast(batch, table)` na thread do cliente; a thread auxiliar grava um row group por lote com `ParquetWriter.write_batch` num arquivo de `staging/<execution_id>/`, e `close` fecha o arquivo e roda, sob o lock e numa transação, o `CREATE TABLE` e o `COPY`: nada existe antes dele, um erro desfaz os dois, e uma exceção dentro do `with` apaga o arquivo sem criar a tabela. Como no `DuckDBLoader` da etapa 4: o arquivo nasce com o esquema do primeiro lote convertido, um lote com outro conjunto de colunas é `ContractError`, o lote recusado pelo `cast` impede a tabela mesmo que o cliente continue, e o `loader` sem lote cria a tabela vazia. |
| `load(table, data)` | Os lotes de `data` pelo `loader`, como no motor DuckDB, com a mesma recusa do DataFrame antes de qualquer carga, pelo primeiro item do iterável. O `INSERT` multilinha saiu (decisão do usuário de 2026-09-23): ele embutia os valores no texto, com o risco de escape e o teto de 16 MB por comando, para poupar o custo fixo do `COPY`, que a suíte mediu no ambiente alvo em 2026-09-23: 10 linhas em 0,91 s e 0,97 s pelo `COPY`, contra 0,53 s e 0,55 s pelo `INSERT` de várias linhas (melhor de três, [`POC.md`](POC.md)), e o usuário manteve o `load` pelo `loader` depois da leitura. |
| `audit(table, partitions, uri=None, version=None, foreign_keys=False, key_scope=None, referenced=None)` | O texto de `sql.render(check.statement, "redshift", metadata, prefix="exec_<id>_")` de cada `Check` de `audit.checks_and_not_run`, com a contagem por `count(CASE WHEN ...)` que a etapa 4 adotou porque o `COUNT` do Redshift não tem `FILTER`, e o mesmo `AuditReport`: `totals`, `rows`, `nonfinite_columns` e a amostra de cada contador por `audit.sample_statement`; `referenced` dá a URI e a versão fixada da tabela referenciada fora do sandbox; as demais partições e a tabela referenciada entram em stagings só com as colunas da chave, por `COPY ... MANIFEST`, carregadas só quando a junção roda: o `skip_when` da chave primária inteira de uma coluna, verdadeiro quando o menor valor da execução passa do `max_key` da versão fixada, dispensa a junção e a staging (decisão do usuário de 2026-09-23). |
| `export_partition(table, uri, value, metadata, expected_rows=None, columns_without_min_max=())` | A partição entra no Delta pelo registro dos arquivos do `UNLOAD`, o padrão que o usuário aprovou em 2026-09-24 para as etapas 4, 5 e 7, e troca para `publish_partition` quando `columns_without_min_max` não está vazio; a execução não escolhe o modo, porque a flag `export_mode` saiu com o `rewrite` das etapas [4](PLAN-STAGE-4.md) e [7](PLAN-STAGE-7.md) (decisão do usuário de 2026-09-24). Os dois caminhos começam por `UNLOAD ('<select das colunas do contrato, sem a de partição, da partição>') TO '<destino>/' FORMAT PARQUET MANIFEST VERBOSE` com a cláusula de credenciais, sem `PARTITION BY` (decisão do usuário de 2026-09-23): o caminho `<coluna>=<valor>/` é montado pela biblioteca, como no `COPY` do DuckDB, e `PARALLEL OFF` entra quando a partição cabe num arquivo, porque o `UNLOAD` fragmenta por slice (500.000 linhas em 32 arquivos) e `MAXFILESIZE` é teto, não piso. O `UNLOAD ... MANIFEST VERBOSE` passou no ambiente alvo em 2026-09-21 a partir de uma tabela do datashare, com `PARTITION BY` ([`../examples/redshift_manifest.py`](../examples/redshift_manifest.py), [`POC.md`](POC.md)); a forma sem ele, com `=` no prefixo, passou em 2026-09-23, com o `schema.elements` do manifesto sem a coluna de partição, e os arquivos registrados foram lidos pelo `delta_scan` ([`POC.md`](POC.md)). O destino é um prefixo novo por partição e por tentativa: o `UNLOAD` confere o destino como prefixo e recusa um prefixo com objetos abaixo (leitura de 2026-09-21: o mesmo prefixo e o prefixo pai reprovam, um subprefixo novo passa), `run.publish` exporta uma partição por chamada, e a reexecução com o mesmo `execution_id` ([etapa 6](PLAN-STAGE-6.md)) acharia ocupado um destino sem o `uuid`, que o arquivo do DuckDB também leva no nome. **O registro**: o destino é `<uri>/<coluna>=<valor>/<execution_id>_<uuid>/`, dentro da pasta da partição; `register_files` recebe `<coluna>=<valor>/<execution_id>_<uuid>/<arquivo>` por entrada do manifesto (`<slice>_part_<nn>.parquet`, com o número da slice variando entre execuções), o `count(*)` da fonte em `expected_rows`, faz as conferências da [etapa 3](PLAN-STAGE-3.md) antes do commit e a releitura depois. Os dados não passam pela máquina local, e os arquivos ficam como o Redshift os gravou até a compactação: `TIMESTAMP` em `INT96`, sem estatística de mínimo e máximo, lido de volta como `timestamp[us]` pelo delta-rs e pelo `delta_scan`, e `DECIMAL` em `FIXED_LEN_BYTE_ARRAY`. **A troca**: o destino é `staging/<execution_id>/<tabela>/<coluna>=<valor>/<uuid>/`, fora da tabela; a partição volta pelo leitor da [etapa 7](PLAN-STAGE-7.md) sobre os arquivos do manifesto (`read_parquet`, `INT96` a microssegundos, a coluna de partição acrescentada com o valor, `cast`) e entra por `publish_partition`, onde o `write_deltalake` calcula estatística e valores de partição e recusa o que o contrato recusa. Os dados passam pela máquina local, numa conexão DuckDB de `storage.duckdb_connect`, aberta com `environment_limits` e fechada depois da partição, e a memória do `write_deltalake` cresce com a partição, fora do `memory_limit`: 2.689 MB para 13.637.568 linhas na migração de 2026-09-23 e 12.357 MB para as 52.654.607 linhas da partição sintética de `cad_lancamentos` ([`POC.md`](POC.md)), que a máquina precisa ter; `cleanup` apaga o staging. Nos dois caminhos, `columns_without_min_max`, as colunas `Double` com valor não finito na partição, vem de `run.publish` e vai a `register_files` ou a `publish_partition` (decisão do usuário de 2026-09-23, [issue #59](https://github.com/felipenoris/serialize-db/issues/59)). O `UNLOAD` grava no rodapé de um grupo de linhas com `NaN` o mínimo e o máximo sem ele, e o leitor Parquet do DuckDB podou o grupo e perdeu a linha (leitura de 2026-09-23, [`POC.md`](POC.md)): no registro, o arquivo ficaria com esse rodapé, e a regra da issue #59 só valeria no log. Por isso a partição com `columns_without_min_max` não vazio sai pela troca, que vai ao log da execução como aviso (`log.warning`), com a tabela, a partição e as colunas, porque os dados passam pela máquina local (decisão do usuário de 2026-09-23). |
| `cleanup()` | `DROP TABLE` de `exec_<id>_*` e da staging; os objetos de `staging/<execution_id>/` apagados; a sessão fechada. |

O identificador de execução entra no nome do sandbox normalizado para `[a-z0-9_]`, dentro dos
127 bytes de um identificador do Redshift. As tabelas `exec_<id>_*` nascem no esquema do datashare,
onde o `CREATE TABLE` passou (decisão do usuário de 2026-09-22, que fechou a alternativa das tabelas
temporárias do banco da conexão: elas morrem com a sessão, que o serverless encerra depois de
3.600 s ociosa, ninguém as inspeciona de fora nem depois de uma auditoria reprovada, e nascem com
codificação `RAW`, [`redshift.md`](redshift.md)); o banco local saiu das alternativas em 2026-09-20,
porque `has_database_privilege(dev, CREATE)` é falso e `TEMP` é verdadeiro (`RS-9`). A sessão única
do motor deixa ao pipeline a tabela temporária de que ele precise, criada por `query` e visível
aos comandos seguintes, para um intermediário que não deva ficar no esquema.

Testes: `tests/test_engine_redshift.py` compara o SQL gerado (`COPY`, `INSERT ... SELECT`,
`UNLOAD`, DDL da staging) com texto esperado, sem conexão; os testes marcados `redshift` rodam a
mesma sequência com uma amostra no esquema autorizado, depois do
`test_redshift.py` da [etapa 0](PLAN-STAGE-0.md). Provas de conceito: `test_redshift.py` (sessão e
`paramstyle` nomeado, o `fetchmany` por lotes, banco do esquema e o `USE`, DDL, `COPY ... MANIFEST`, lista de
colunas e `FILLRECORD`, `VARCHAR`, `SUPER`, `UNLOAD`, ciclo da Data API, o comando repetido depois de um `TRUNCATE` com e sem o cache do driver; e as leituras das decisões de 2026-09-23, lidas no ambiente alvo naquele dia: `test_unload_to_a_hive_prefix_and_register`, `test_stream_by_unload_with_literal_values`, `test_unload_limit_empty_result_temp_table_and_super`, `test_row_description_oids_and_type_modifier`, `test_small_load_copy_cost`, `test_unload_footer_statistics_with_nan` e `test_audit_sql_under_search_path_and_nan_comparison`), `test_sqlalchemy.py`
(`test_redshift_dialect_compiles_dml`, `test_three_part_name_needs_quoted_name_without_quotes`,
`test_sandbox_copy_of_table_and_schema_files_diff`) e `test_stdlib.py::test_execution_identifiers`
(o prefixo do sandbox).

Paralelismo: `test_redshift.py::test_parallel_copy_and_unload_on_two_connections`, dois `COPY` e dois
`UNLOAD` em tabelas distintas em duas conexões, limitados pelas slots do WLM, é o caminho de
`publish_redshift` da [etapa 8](PLAN-STAGE-8.md), uma conexão por tabela, e o de `new_session`; a
sessão principal roda os comandos do pipeline em série. O que duas transações simultâneas fazem no
esquema do datashare está em `test_redshift_transactions.py` ([etapa 8](PLAN-STAGE-8.md)).

## Estratégia de implementação

- **O motor entra em `Execution`** pelo nome `"redshift"`, que hoje é `ContractError` em
  `Execution._build_engine` ([etapa 6](PLAN-STAGE-6.md)): a etapa troca a recusa pela construção
  do motor com a configuração das variáveis `SERIALIZE_DB_REDSHIFT_*`, o `execution_id` e o
  `Storage`, e o `serialize-db audit` sem `--sql` passa a aceitar `--engine redshift`.
- **`connect`** (interno, uma vez por execução) repete `examples/redshift_native.py`: `GetWorkgroup`,
  `GetCredentials(durationSeconds=3600)`, `redshift_connector.connect` sem `timeout`, ou o par
  informado. Com `share_database`, roda `USE <banco>` e nenhum comando de conferência (decisão do
  usuário de 2026-09-23): o `USE` num banco inexistente falha sozinho, e o primeiro comando que cita
  `<esquema>.<tabela>` confirma a troca pelo efeito de que o motor depende. `current_database()` não
  serve de conferência: a leitura de 2026-09-21 no ambiente alvo devolveu `dev` depois do `USE`, e o
  usuário confirmou no mesmo dia que o `USE` vale mesmo assim, como os exemplos de 2026-09-20 e
  2026-09-21 já mostravam ([`POC.md`](POC.md)). Um nome em duas partes só resolveria em silêncio no
  banco da conexão se ele tivesse um esquema com o mesmo nome, e o do ambiente alvo não tem
  (`sbx_aco_decon` só existe no datashare) nem deixa criar (`has_database_privilege(dev, CREATE)`
  falso, `RS-9`). A tabela de controle da [etapa 8](PLAN-STAGE-8.md) é criada só pelo usuário, por
  `create_publications_table`. Depois vêm `SET search_path TO <esquema>` e
  `cursor.paramstyle = "named"`. O `search_path` é o que resolve os nomes sem esquema do `ddl` da
  [etapa 1](PLAN-STAGE-1.md), do `render` e do `audit_sql`: no ambiente alvo, em 2026-09-23, ele
  passou no esquema do datashare depois do `USE`, o `CREATE TABLE` do `ddl` com o nome sem esquema
  criou a tabela nele, e `current_schema()` continuou nulo ([`POC.md`](POC.md)).
  Uma conexão derrubada pelo servidor é reaberta uma vez por comando, com credencial nova, e a
  reconexão perde as tabelas temporárias da sessão, que o log nomeia. O motor guarda essa conexão e
  um `threading.RLock` que todo comando toma pelo tempo do comando, e o cliente usa a conexão direto
  só dentro de `session()`, que toma o mesmo lock (decisão do usuário de 2026-09-22, a mesma do motor
  DuckDB): não há conexão por thread, e dois comandos da sessão não rodam ao mesmo tempo; o que roda
  em paralelo abre uma sessão a mais por `new_session()`, com credencial e `USE` próprios. A
  conexão vai com `max_prepared_statements=0`: o driver prepara cada comando sem nome logo antes de o
  executar, e nenhum statement guardado sobrevive a um `TRUNCATE` ou a um DDL de outra sessão
  ([`redshift.md`](redshift.md)).
- **`qualified`** é `f"{schema}.{name}"`; o nome em três partes não entra no motor.
- **`credentials_clause`** devolve `IAM_ROLE '<arn>'` ou `IAM_ROLE default` quando `iam_role` está
  configurado, e `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY` e `SESSION_TOKEN` das credenciais congeladas
  do `boto3` quando não está; ela é chamada dentro de cada comando, e o texto do comando passa por
  `mask` antes de qualquer log, relatório ou exceção.
- **`ingest`** grava `copy_manifest(uri, version, partitions, storage.uri_of(<ambiente>/staging/<execution_id>/<tabela>.manifest), storage)`,
  cria a staging `exec_<id>_<tabela>_staging` por `staging_ddl`, o `ddl` da [etapa 1](PLAN-STAGE-1.md)
  sem a coluna de partição, escrito nesta etapa sobre `column_ddl` e `quoted`,
  roda `COPY ... FORMAT AS PARQUET MANIFEST FILLRECORD` (decisão do usuário de 2026-09-23 para todo
  `COPY` da biblioteca, [etapa 8](PLAN-STAGE-8.md): um manifesto pode listar arquivos anteriores a
  uma coluna nova) e um
  `INSERT INTO exec_<id>_<tabela> (<colunas>) SELECT ..., '<valor>', ...` por partição, na ordem do contrato (sem o literal numa tabela sem partição), com `JSON_PARSE` nas colunas `SUPER`.
  `materialize=False` não existe aqui: o Redshift não lê o Delta no lugar.
- **`stream`** vai sempre por `UNLOAD` (decisão do usuário de 2026-09-23). O driver lê o resultado
  inteiro no `execute` e `fetchmany` só fatia a fila (`redshift_connector` 2.1.16, leitura do código
  em 2026-09-21), e o motor não sabe o tamanho do resultado antes desse `execute`: um limite de
  linhas entre os dois caminhos não teria como ser aplicado. O `UNLOAD` limita a memória e troca o
  parser Python do driver pela leitura nativa do Parquet, ao custo de um `UNLOAD` por `stream`
  (0,8 s a 1,9 s em 2026-09-21). O texto do `UNLOAD` é um literal que não recebe parâmetro, então os
  valores do cliente entram como literais: o statement Core é a cópia prefixada (`sql.prefixed` com
  `prefix=exec_<id>_`), com os valores dados por `statement.params(**params)` e os nomes conferidos
  como no motor DuckDB, compilada pelo dialeto Redshift com `paramstyle="named"`,
  `literal_binds=True` e `render_postcompile=True`, que expande o `IN` de lista (sem ele o texto
  sai com `__[POSTCOMPILE_...]`, leitura de 2026-09-23 no DuckDB, [etapa 4](PLAN-STAGE-4.md)). O
  `paramstyle="named"` é o de `render` ([etapa 2](PLAN-STAGE-2.md)): com o padrão `format`, o
  dialeto dobra o `%` dos literais (`'50%% certo'`), e o texto vai ao driver sem parâmetros, sem
  conversão que o desfaça (sonda local e leitura do código de 2026-09-23, [`POC.md`](POC.md)). O
  texto pronto (`sql.read_sql(..., prefix="exec_<id>_")`) passa por `sa.text(texto).bindparams(...)`
  com cada `bindparam` tipado pelo valor, `sa.bindparam(nome, value=valor, expanding=...)`, e pelo
  mesmo compilador: o `bindparam` de `text()` sem tipo não renderiza literal (`CompileError: No
  literal value renderer is available ... with datatype NULL`). Antes do comando, o motor percorre
  o statement e recusa todo `BindParameter` com `required`, como `render` faz: sob `literal_binds`,
  `compiled.binds` sai vazio, e o `bindparam` sem valor vira `NULL` calado, até num `IN` de lista,
  que sai `IN (NULL)` (leituras de 2026-09-22, [etapa 2](PLAN-STAGE-2.md), e de 2026-09-23). O
  dialeto dobra a aspa simples e a contrabarra (`'d''agua'`, `'barra \\ invertida'`), o escape do
  PostgreSQL, e o literal do `UNLOAD` dobra as duas de novo, porque também trata a contrabarra como
  escape: com só a aspa dobrada, o `UNLOAD` do caso da contrabarra passou sem achar linha no
  ambiente alvo, e com as duas dobradas os seis casos de `test_stream_by_unload_with_literal_values`
  deram as mesmas linhas pelos três caminhos nas execuções de 2026-09-23 às 22:56 e às 23:01
  ([`POC.md`](POC.md)).
  O `UNLOAD` roda na sessão do motor, sob o lock, na
  thread de quem chama: numa thread auxiliar ele esperaria pelo lock que o cliente segura num bloco
  `session()`, enquanto o cliente espera o primeiro lote. A thread auxiliar só lê os arquivos do
  manifesto por `ParquetFile.iter_batches(batch_size)` pelo `Storage`, com
  `coerce_int96_timestamp_unit="us"`, e entrega cada lote com o esquema do statement, ou o do
  arquivo no texto, numa fila de dois lotes. A coluna `SUPER` chega do arquivo como
  `extension<arrow.json>`, com o texto JSON (leitura de 2026-09-23), e sai em `string`, o tipo do
  JSON no motor DuckDB; o `cast` do contrato faz essa conversão (sonda local de 2026-09-23). Sem
  manifesto depois do `UNLOAD`, o motor lê `pg_last_unload_count()` na mesma sessão, ainda sob o
  lock: 0 é o resultado vazio, que o `UNLOAD` não grava (as execuções de 2026-09-23 leram 0 no
  `UNLOAD` vazio e 2 no da tabela temporária de duas linhas), e o `stream` sai sem lote, com o esquema
  do statement ou, no texto, o de `schema_from_row_description` sobre o texto por
  `select * from (<texto>) as t limit 0` no cursor (decisão do usuário de 2026-09-23); outra contagem sobe
  como o `FileNotFoundError` da leitura do manifesto, com a contagem na mensagem.
- **`query`** compila a cópia prefixada pelo dialeto Redshift com `paramstyle="named"`, sem
  `literal_binds` e com `render_postcompile=True`; `construct_params()` dá os valores e o marcador
  `:nome` fica como o `redshift_connector` o lê com `cursor.paramstyle = "named"`; o texto pronto
  passa por `bind(dialect="redshift")`. O resultado do `fetchall` vira a `pa.Table` **por colunas**:
  `zip(*rows)` e `pa.array(coluna, type=campo.type)`, com o esquema do statement ou o de
  `schema_from_row_description`. O rascunho mediu 0,03 s por colunas contra 0,10 s por dicionários
  em 200.000 linhas.
- **`schema_from_row_description`** traduz a descrição de cada coluna do resultado para o tipo Arrow
  (decisão do usuário de 2026-09-23). O `cursor.description` do `redshift_connector` 2.1.16 devolve
  só `(nome, oid, None, None, None, None, None)`; a precisão e a escala do `NUMERIC` estão no
  `type_modifier` de cada entrada de `cursor.ps["row_desc"]`, que o próprio driver usa para
  decodificar o `NUMERIC` binário: escala `(type_modifier - 4) & 0xFFFF` e precisão
  `((type_modifier - 4) >> 16) & 0xFFFF` (leitura do código, 2026-09-23, [`POC.md`](POC.md)). O
  atributo é privado, e o extra `redshift` fixa `redshift-connector==2.1.17`, cujo fonte tem o
  mesmo atributo (decisão do usuário de 2026-09-24). Os OIDs vêm de
  `redshift_connector.utils.oids.RedshiftOID`: `BOOLEAN` em `bool`; `SMALLINT`, `INTEGER` e
  `BIGINT` em `int16`, `int32` e `int64`; `REAL` e `FLOAT` em `float32` e `float64`; `NUMERIC` em
  `decimal128(p, s)`; `CHAR`, `BPCHAR`, `VARCHAR`, `TEXT`, `NAME` e `UNKNOWN` em `string`; `DATE`
  em `date32`; `TIMESTAMP` em `timestamp[us]`; `TIMESTAMPTZ` em `timestamp[us, UTC]`; e `SUPER` em
  `string`, como o JSON do motor DuckDB. `NAME` (OID 19) é o tipo dos identificadores do catálogo,
  o de `current_database()`, que reprovou a primeira execução no alvo em 2026-09-24
  ([`POC.md`](POC.md)). Outro OID, e um `NUMERIC` sem `type_modifier`, são
  recusados com `SandboxError`, que nomeia a coluna e o tipo (`get_datatype_name`). Uma precisão e
  uma escala fixas quebrariam: `pa.array` com `decimal128(18, 2)` recusou um valor de escala 6 e um
  de 17 dígitos inteiros (2026-09-23, [`POC.md`](POC.md)). A suíte leu o `row_desc` no ambiente
  alvo em 2026-09-23 ([`POC.md`](POC.md)): cada tipo do contrato e o `SUPER` chegam com o OID da
  tabela; o `sum` e o `avg` de `DECIMAL(18, 2)` saem `NUMERIC(38, 2)`, o `sum` de
  `DOUBLE PRECISION` sai `FLOAT`, o literal de texto `VARCHAR` e o literal `1.5` `NUMERIC(2, 1)`,
  todos na tabela; `TEXT` e `UNKNOWN` não apareceram.
- **`loader`** confere o nome na abertura, antes do primeiro lote, por `select 1 from
  <esquema>.<nome> limit 0` sob o lock: a resposta é o nome ocupado, recusado com `SandboxError`, e
  o erro de relação inexistente é o nome livre, porque `information_schema.columns` leu vazio o
  esquema do datashare em 2026-09-21. Ele grava um row group por lote com
  `ParquetWriter.write_batch` em `staging/<execution_id>/<tabela>/<uuid>.parquet` no S3 (ou na
  pasta local nos testes), e `close` roda, sob o lock e numa transação aberta por `BEGIN`, o
  `CREATE TABLE` por `ddl` e o `COPY` desse prefixo: nada existe antes dele, e um erro desfaz os
  dois; uma exceção dentro do `with` apaga o arquivo sem criar a tabela. É a regra do `loader` da
  [etapa 4](PLAN-STAGE-4.md) (decisão do usuário de 2026-09-23): um `load` esquecido numa thread faz
  a leitura concorrente falhar, em vez de ver a tabela vazia. `load` passa sempre pelo `loader`
  (decisão do usuário de 2026-09-23).
- **`audit`** roda `audit_sql(table, "redshift", prefix=exec_<id>_, published=<staging>)`; as
  demais partições e a tabela referenciada entram na staging `_publicado` de `published`, com
  todas as colunas do contrato, por `COPY ... MANIFEST` da versão fixada, carregada só quando a
  verificação que a cita roda. O texto do Redshift soma o `Double` onde a comparação
  estrita com os infinitos é verdadeira, conta os não finitos como os não nulos menos os finitos,
  sem negar a comparação, que na varredura de uma tabela não deu verdadeiro ao `NaN`, e dá `true`
  ao JSON da coluna `SUPER`, que o `is_valid_json` recusa (leituras de 2026-09-23,
  [etapa 4](PLAN-STAGE-4.md)). É essa contagem que dá `columns_without_min_max` e a troca para
  `publish_partition` da partição com `Double` não finito.
- **`export_partition`** começa por `UNLOAD ('<select do contrato sem a coluna de partição>') TO
  '<destino>/' <credenciais> FORMAT AS PARQUET MANIFEST VERBOSE`, sem `PARTITION BY` (decisão do
  usuário de 2026-09-23), com `PARALLEL OFF` quando a partição cabe num arquivo (o `UNLOAD` fragmenta
  por slice, 32 arquivos para 500.000 linhas). O último segmento do destino é novo a cada chamada,
  com um `uuid`. No registro, o destino é `<uri>/<coluna>=<valor>/<execution_id>_<uuid>/`,
  e cada entrada do manifesto vira um `RegisteredFile` com `path` relativo à pasta da tabela,
  `content_length`, `record_count` e as estatísticas do rodapé, lido por `storage.open_input_file`:
  o `null_count` de cada coluna que o rodapé conta em todos os grupos de linhas e o mínimo e o
  máximo dos tipos que transcrevem exato, somados os grupos de linhas, uma função que esta etapa
  acrescenta a `serialize_db.delta` ao lado de
  `file_from_return_stats` da [etapa 3](PLAN-STAGE-3.md); `register_files` confere e commita, com
  `expected_rows` do `count(*)` do sandbox, e relê. O `select` do `UNLOAD` lista as colunas do
  contrato na ordem dele e sem a de partição, porque `register_files` recusa o arquivo com a coluna
  de partição, fora da ordem ou com nulo numa coluna `NOT NULL`. Com `columns_without_min_max` não
  vazio, a partição vai pela troca: o destino é
  `staging/<execution_id>/<tabela>/<coluna>=<valor>/<uuid>/`, a partição volta pelo leitor da
  [etapa 7](PLAN-STAGE-7.md), sobre os arquivos do manifesto, numa conexão DuckDB aberta com
  `environment_limits` e fechada depois da partição, para `publish_partition`, cuja memória cresce
  com a partição fora do `memory_limit` (12.357 MB para 52.654.607 linhas, [`POC.md`](POC.md)), e
  o motor registra no log, em nível `WARNING`, a tabela, a partição, as colunas e a troca.
  Depois do commit, a troca relê a partição por `read_back`, com `expected_rows` ou, sem ele, o
  `count(*)` do sandbox, como o registro: o log e os dois leitores contam as linhas, e uma
  diferença restaura a versão anterior e sobe `RegistrationRefused` (decisão do usuário de
  2026-09-25); o código ainda não segue a decisão ([`CURRENT_STATE.md`](CURRENT_STATE.md)).
- **`cleanup`** roda `DROP TABLE IF EXISTS` de cada `exec_<id>_*`, um comando por chamada, apaga
  `staging/<execution_id>/` pelo `Storage` e fecha a sessão.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| conexão | `redshift-serverless:GetWorkgroup` e `GetCredentials`, endpoint VPC para as APIs (o ambiente alvo tem), `USAGE` e `CREATE` no esquema concedidos pelo produtor. | Sessão no banco da conexão com os nomes em duas partes resolvendo no datashare; nenhuma tabela criada; `paramstyle` nomeado; uma sessão por execução, sob o lock do motor. |
| `ingest` | Tabela Delta legível pela identidade da sessão, que o `COPY` leva; staging inexistente. | `exec_<id>_<tabela>` com as partições pedidas e a coluna de partição preenchida; a staging apagada. |
| `new_session` | O motor aberto; a identidade pode pedir outra credencial temporária. | Outra conexão no banco do datashare, com o seu lock; as tabelas confirmadas pela sessão principal visíveis, as temporárias dela não; o fim do `with` fecha só essa conexão. |
| `stream` | Texto ou statement válido, sem `LIMIT` no `select` externo e com valor em todo `bindparam`; `s3:PutObject` e `s3:GetObject` sob `staging/`. | Lotes com os tipos do contrato; o lock solto no fim do `UNLOAD`, e a thread do cliente livre para o lote atual; o prefixo do `stream` apagado no `close`. |
| `query` | Texto ou statement válido. | A `pa.Table` com os tipos do contrato; o lock solto no fim do `fetchall`. |
| `loader`, `load` | O nome da tabela livre no sandbox; lotes que passam por `cast`; `s3:PutObject` sob `staging/`. | Nada existe antes do `close`, que cria a tabela e carrega numa transação; nenhuma tabela no erro; `SandboxError` com o nome ocupado; o arquivo do staging apagado no erro. |
| `export_partition` | Auditoria aprovada; o destino novo, vazio por construção. | Uma versão nova no Delta; os arquivos como o Redshift os gravou (`INT96`, `FIXED_LEN_BYTE_ARRAY`, `optional`) no registro, normalizados na troca; nos dois caminhos, as linhas contadas pelo log e pelos dois leitores. |
| `cleanup` | Nenhum. | Nenhuma tabela `exec_<id>_*` no esquema; `staging/<execution_id>/` vazio; a sessão fechada. |

## Testes por caso

`tests/test_engine_redshift.py`: o texto de cada comando comparado com o esperado, sem conexão; os
testes marcados `redshift` repetem a sequência com uma amostra no esquema autorizado.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Configuração | `test_config_from_environment` | `SERIALIZE_DB_REDSHIFT_*` para `RedshiftConfig`; o par informado e o workgroup são caminhos alternativos; nenhum outro. |
| Prefixo | `test_sandbox_prefix_normalizes_and_limits` | `[a-z0-9_]`, 127 bytes. |
| Cláusula de credenciais | `test_credentials_clause_and_mask` | `IAM_ROLE` com ARN e `default`; as três chaves da sessão sem `iam_role`; `mask` tira os valores; nenhuma exceção carrega o texto sem máscara. |
| Comandos | `test_copy_insert_unload_text` | `COPY ... FORMAT AS PARQUET MANIFEST FILLRECORD` sem `COMPUPDATE` ([etapa 8](PLAN-STAGE-8.md)); `INSERT ... SELECT *, '<valor>'`; `UNLOAD ... MANIFEST VERBOSE` sem `PARTITION BY`, o `select` da exportação sem a coluna de partição, `PARALLEL OFF` opcional na exportação e fixo no `stream`, a contrabarra e as aspas do `select` dobradas; nomes em duas partes. |
| Exportação, troca e destino por tentativa | `test_export_registers_the_unloaded_files_and_swaps_on_nonfinite` (`local`) | Uma conexão de mentira que grava o arquivo do `UNLOAD` na pasta local: o registro em `<coluna>=<valor>/<execution_id>_<uuid>/` com o `select` sem a coluna de partição e o JSON serializado, o arquivo `INT96` no log com as linhas conferidas e sem o mínimo e o máximo do texto, `expected_rows` diferente recusado, dois destinos distintos para a mesma partição; com `columns_without_min_max` não vazio, o destino no `staging/`, `publish_partition` e o `WARNING` com a tabela, a partição e as colunas, e `expected_rows` diferente desfazendo o commit com `RegistrationRefused`; a partição vazia registrada por um arquivo sem linha; o valor numa tabela sem partição recusado antes de qualquer comando. |
| Ingestão e `published` | `test_ingest_loads_each_partition_through_the_staging` (`local`) | Por partição, o manifesto no `staging/`, o `DELETE` da staging, o `COPY ... MANIFEST FILLRECORD` e o `INSERT` com a lista de colunas; a partição sem arquivo não roda; o nome ocupado e a tabela sem versão são `SandboxError`; `published` carrega a versão inteira em `_publicado_<versão>` uma vez, e outra versão numa staging nova; `cleanup` apaga as tabelas e o `staging/`. |
| Loader sem conexão | `test_loader_writes_the_file_and_creates_the_table_in_a_transaction` (`local`) | O nome ocupado recusado; o arquivo no `staging/` pela thread auxiliar; `BEGIN`, o `CREATE TABLE`, a staging temporária com o `COPY` e o `INSERT ... JSON_PARSE`, `COMMIT`, e o arquivo apagado; a tabela sem JSON pelo `COPY` direto; a exceção no `with` e o lote recusado sem tabela; o DataFrame recusado. |
| Reconexão | `test_connection_dropped_by_the_server_is_reopened_once` | O `InterfaceError` do driver reabre a conexão uma vez, com o `USE` e o `search_path`, e repete o comando; dentro de uma transação o erro sobe, com o `ROLLBACK` tentado. |
| Lotes do arquivo | `test_stream_reads_the_unloaded_file_in_the_statement_schema` (`local`) | Os lotes do arquivo do `UNLOAD` no esquema do `row_desc`, com o `INT96` em microssegundos; `close` apaga o prefixo do stream. |
| DDL da staging | `test_staging_ddl_without_partition_column` | A staging sem a coluna de partição; a tabela do sandbox com ela. |
| Tabela do cursor | `test_table_from_cursor_by_columns` | Um cursor de mentira: a `pa.Table` com os tipos do esquema, igual ao caminho por dicionários. |
| Valores literais | `test_stream_literal_values` e `test_stream_literal_values_on_the_target` (`redshift`) | O texto do `UNLOAD` de um statement com texto, data, número e `IN` de lista, com o `%` sem dobrar; o texto pronto com os `bindparam` tipados pelo valor; um `bindparam` sem valor, também num `IN` de lista, recusado antes de qualquer comando; no alvo, valores com `'` e `\` voltam iguais pelo `stream` e pelo `query`, o `select` sem linha dá o stream vazio com o esquema, a tabela temporária criada por `query` é lida pelo `UNLOAD` do `stream` seguinte, e o `row_desc` dos agregados é leitura. |
| Resultado vazio | `test_stream_empty_result` (`local`) | Uma conexão de mentira em que o `UNLOAD` não grava manifesto: com `pg_last_unload_count()` em 0, o `stream` sai sem lote e com o esquema do statement, e o texto com o do `row_desc`; com 2, a falta do manifesto sobe; no alvo (`redshift`), um `select` sem linha. |
| Sessão única | `test_statements_serialize_on_the_single_session` (`local`) | Uma conexão de mentira que registra o início e o fim de cada comando: dois comandos de duas threads não se sobrepõem; um comando roda enquanto um `stream` ainda lê os arquivos, porque o lock solta no fim do `UNLOAD`; um `stream` aberto dentro de `session()`, na mesma thread, não trava; no alvo (`redshift`), a tabela temporária criada por `query` é lida pelo `UNLOAD` do `stream` seguinte. |
| Sessão a mais | `test_new_session_sees_committed_tables` (`redshift`) | A sessão de `new_session` vê a tabela `exec_<id>_*` confirmada pela principal e recusa a temporária dela; duas ingestões em duas sessões terminam, e a principal lê as duas tabelas. |
| Esquema de um texto | `test_schema_from_row_description` | Um `row_desc` de mentira: cada OID da tabela para o tipo Arrow; `NUMERIC` com a precisão e a escala do `type_modifier`; outro OID recusado com o nome da coluna; no alvo (`redshift`), o `row_desc` de um `select` com uma coluna de cada tipo do contrato, `SUPER`, `count(*)`, `sum` de `NUMERIC(18, 2)`, `sum` de `DOUBLE PRECISION` e um literal de texto. |
| Conexão real | `test_connect_uses_share_database` (`redshift`) | Depois do `USE`, o `CREATE TABLE` de uma tabela `exec_<id>_*` por nome em duas partes passa, nenhum comando da conexão cria `serialize_db_publications`, e `current_database()` é registrado como leitura. |
| Ciclo com amostra | `test_ingest_stream_loader_export` (`redshift`) | `ingest` de uma partição de um Delta no bucket, `stream` em lotes, `loader` por `COPY`, `export_partition` nos dois modos com as mesmas linhas, `cleanup` sem tabela restante. |
| Tabela no `close` | `test_loader_creates_the_table_at_close` (`redshift`) | O nome ocupado é recusado com `SandboxError` na abertura; antes do `close` a leitura da tabela falha com relação inexistente; um erro do `COPY` desfaz o `CREATE TABLE` no esquema do datashare, e o nome fica livre. |
| Custo fixo do `COPY` | `test_small_load_copy_cost` (`redshift`) | O tempo de um `load` de 10 linhas pelo `loader`, como leitura, nunca como reprovação: é a leitura que traria de volta o `INSERT` multilinha. |

## A implementação

O módulo `serialize_db.engine.redshift` (`RedshiftConfig`, `RedshiftEngine`, `sandbox_prefix`,
`schema_from_row_description`, `mask`, e o `RedshiftStream` e o `RedshiftLoader` dele), com os
auxiliares protegidos que a [etapa 8](PLAN-STAGE-8.md) reaproveita (`connect`,
`credentials_clause`, `staging_ddl`, `insert_from_staging`, `copy_text`, `unload_text`,
`relation_missing`, `relation_exists`, `serialization_failure`), `file_from_footer` e
`partition_values` em `serialize_db.delta`, `open_output_stream` em `serialize_db.storage`,
`required_parameters` em `serialize_db.sql` (o guarda que os dois motores usam) e os casos de
`tests/test_engine_redshift.py` substituem a interface e os rascunhos executados em 2026-09-21: as
assinaturas e as docstrings estão no código e na documentação do `pdoc`. O motor entrou em
`Execution` pelo nome `"redshift"` e no `serialize-db audit --engine redshift`
([etapa 6](PLAN-STAGE-6.md)), o extra `redshift` fixa `redshift-connector==2.1.17`, a versão do
ambiente de desenvolvimento, cujo fonte tem o mesmo `ps["row_desc"]` e o mesmo `type_modifier`
lidos em 2.1.16 (decisão do usuário de 2026-09-24), e a porta `driver_connect` é onde os testes
trocam o driver pela conexão do substituto local. O que a implementação mostrou está em
[`POC.md`](POC.md), seção "O que a implementação das etapas 5 e 8 mostrou"; os casos `redshift`
passaram no substituto local e, no ambiente alvo em 2026-09-24, cinco de seis às 05:10 e os seis
às 13:01 e 13:03, com a relação inexistente respondida com o SQLSTATE `XX000` e a mensagem
`Relation <nome> does not exist in the database.`, que `relation_missing` reconhece pela mensagem
([`POC.md`](POC.md)).

O que a implementação mudou em relação ao texto das seções acima, com o motivo:

- **A staging `_publicado` tem todas as colunas do contrato**, e a auditoria a carrega só quando
  uma verificação que a cita roda, depois do `skip_when`: o `COPY` de Parquet é posicional e não
  carrega só as colunas da chave de um arquivo com todas, e a lista de colunas exige a contagem do
  arquivo (leitura de 2026-09-21). É a mesma staging de `published`, carregada uma vez por
  execução, entre as sessões.
- **O `INSERT` da staging leva a lista de colunas**, com o valor da partição na posição da coluna
  de partição e `JSON_PARSE` no JSON, em vez de `SELECT *, '<valor>'`, que só serve a um modelo com
  a coluna de partição no fim.
- **O `loader` de uma tabela com coluna JSON carrega por uma staging temporária**, com o JSON em
  `VARCHAR(65535)`, e o `INSERT ... JSON_PARSE`, porque o `COPY` de Parquet numa coluna `SUPER`
  exige `SERIALIZETOJSON`, que ninguém leu no ambiente alvo sobre uma string; a tabela sem JSON
  recebe o `COPY` direto.
- **O esquema do `stream`** é sempre o do `row_desc` de `select * from (<texto>) as t limit 0`,
  também num statement Core, e cada lote do arquivo do `UNLOAD` é convertido para ele: o tipo de uma
  expressão do statement não é confiável, e o `stream` e o `query` saem com o mesmo esquema.
- **O `PARALLEL OFF` da exportação vale até 5.000.000 linhas** na partição (`_PARALLEL_OFF_ROWS`),
  acima disso o `UNLOAD` fragmenta por slice; o valor não foi medido no ambiente alvo.
- **O rodapé do `UNLOAD` dá ao log o mínimo e o máximo das colunas inteiras, `Double` e de data**,
  e não do texto: o rodapé pode guardar o texto truncado, e o PyArrow 25 não expõe a marca de
  exatidão do Parquet; o `null_count` entra de toda coluna que o rodapé conta em todos os grupos
  de linhas, e a coluna sem a contagem, como o timestamp `INT96`, fica fora do `nullCount` do log,
  porque o leitor que poda por ele leria zero nulos (leitura de 2026-09-25, [`POC.md`](POC.md)).
- **A partição vazia entra por um arquivo sem linha**, gravado pelo motor com o esquema do contrato
  sem a coluna de partição, porque o `UNLOAD` de um resultado vazio não grava arquivo e o
  `register_files` sem arquivo falha no delta-rs.
- **A reconexão** vale só fora de transação: dentro dela o erro sobe, porque a transação se perdeu.

## Decisões pendentes

Nenhuma. As decisões que o usuário tomou em 2026-09-23 estão escritas nas seções que as
descrevem: `connect` sem comando de conferência do `USE`, e a tabela de controle criada só pelo
usuário ([etapa 8](PLAN-STAGE-8.md)); `stream` sempre por `UNLOAD` e `query` pelo cursor, com o
esquema do `stream` vazio de um texto pelo `limit 0`; `load` sempre pelo `loader`, mantido depois da
leitura do custo do `COPY`; a precisão e a escala do `NUMERIC` pelo `type_modifier` do driver; a
exportação sem `PARTITION BY`, num prefixo novo por partição e por tentativa; e a partição com
`Double` não finito exportada pela troca para `publish_partition`, com o aviso no log; a decisão de
2026-09-24 tirou o `mode` de `export_partition`, e as de 2026-09-25 mandam a troca conferir as
linhas por `read_back`, como o registro, e põem a versão no nome da staging `_publicado`.
As execuções de 2026-09-23 às 22:56 e às 23:01 leram o que elas pediam ao ambiente alvo, salvo o
arquivo do `UNLOAD` com coluna `SUPER` registrado numa tabela Delta, que a suíte do motor leu em
2026-09-24: o delta-rs e o `delta_scan` dão a coluna como texto, `VARCHAR` com o JSON serializado
([`POC.md`](POC.md)).
