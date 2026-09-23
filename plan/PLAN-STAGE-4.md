# Etapa 4: `audit` e motor DuckDB

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.audit` monta as verificações a partir do contrato e não sabe qual motor as roda;
`serialize_db.engine` declara o protocolo `Engine`, que a execução também não conhece, e
`serialize_db.engine.duckdb` o implementa. O protocolo fixa os tipos da fronteira: `stream` devolve
um `BatchStream` de `pa.RecordBatch` e `loader` recebe lotes por `write`; `query` devolve a
`pa.Table` que `stream` montaria, e `load` entrega ao `loader` os lotes de uma `pa.Table`, de um
`RecordBatch`, de um `RecordBatchReader` ou de um iterável ([`PLAN.md`](PLAN.md), seção "A troca de
dados com o código cliente"). A implementação, `serialize_db.engine.duckdb`, é a referência; os
esboços `SandboxEngine`, `BatchStream` e `Loader` de `test_parallel.py` que a precederam saíram
das suítes de estudo, e os casos que só eles tinham estão em `tests/test_engine_duckdb.py`
(decisão do usuário de 2026-09-23).

Chave primária, unicidade e chave estrangeira ficam fora do DDL dos dois sandboxes (`schema.md`), e
a auditoria é onde elas são aplicadas. Cada verificação sai do `Table`, sem declaração adicional no
modelo:

| Verificação | De onde sai | Escopo |
| --- | --- | --- |
| Nulo em coluna `NOT NULL` | `column.nullable` | As partições da execução. |
| Chave repetida | `table.primary_key` e os `UniqueConstraint`, mais o que `keys` acrescenta | As partições da execução quando as colunas da chave incluem a coluna de partição ou a de `partition_source`; a tabela inteira quando não incluem, menos a chave primária inteira de uma coluna cujo menor valor na execução passa do `max_key` da versão fixada. |
| Órfão de chave estrangeira | `table.foreign_keys` | Só com `foreign_keys=True`; a tabela referenciada entra na versão fixada pela execução. |
| Partição fora da origem, e valor que não serve de nome de pasta | `partition_by` e `partition_source` de `table_options`: com `partition_source` declarado, a coluna de partição diferente de `strftime(<coluna de data>, '%Y-%m-%d')`; em toda tabela particionada, o valor fora da regra da partição, `[0-9A-Za-z][0-9A-Za-z_.-]*` (a partição é texto desde a decisão de 2026-09-22, a regra é a decisão do usuário de 2026-09-23, e a data é o caso da base atual) | As partições da execução. |
| Texto acima de `String(n)` em bytes e valor fora do `Numeric(18, 2)` | os tipos do contrato (`docs/index.md`); no Redshift, o `COPY` de uma string maior que o `VARCHAR` aborta (`Spectrum Scan Error` 15007, 2026-09-21), e esta verificação é a barreira | As partições da execução. |
| Documento JSON inválido, ou acima de 65.535 bytes se a decisão da [etapa 8](PLAN-STAGE-8.md) fixar o teto | as colunas JSON, que nem o Arrow nem o Delta validam; o teto é o do `VARCHAR` da staging do Redshift e da string que o `COPY` de Parquet aceita numa coluna `SUPER` (2026-09-21) | As partições da execução. |
| Totais de controle | as colunas `Numeric` e `Double`; as `Double` somadas como `DECIMAL(38, 6)` de cada valor, porque a soma em ponto flutuante depende da ordem | As partições da execução. |

Uma chave primária não é por partição: conferi-la só nas partições da execução não é unicidade.
Quando as colunas da chave não incluem a coluna de partição, a verificação compara o sandbox com as
demais partições da versão fixada — `delta_scan(uri, version := v) WHERE data_str NOT IN (...)` no
DuckDB, uma staging só com as colunas da chave, carregada por `COPY ... MANIFEST`, no Redshift. Custa
uma passagem nas colunas da chave da tabela inteira; `key_scope="partition"` a reduz às partições da
execução, e a escolha entra no relatório. Duas regras dispensam a passagem (decisão do usuário de
2026-09-23). A chave que inclui a coluna de `partition_source` fica nas partições da execução, porque a
verificação da derivação, na mesma auditoria, garante que a data de uma linha decide a sua partição:
`(data, sistema, contrato)` de `cad_contratos` e `(data, operacao)` de `cad_operacoes`. E a chave
primária inteira de uma coluna, que `next_ids` preenche acima do máximo da versão fixada, dispensa a
junção quando o menor valor das partições da execução passa do `max_key` dessa versão, lido das
estatísticas do log sem ler dados: a verificação leva a consulta desse mínimo em `skip_when`, que o
motor roda antes e que, verdadeira, aprova a verificação sem rodá-la, com o motivo no relatório. No
Redshift, a staging das colunas da chave só é carregada quando a junção roda, e o `cad_lancamentos`
da base de produção tem 141.901.795 linhas.

A chave estrangeira precisa da tabela referenciada, e só o que o pipeline usa é ingerido:
`foreign_keys=True` ingere a coluna referenciada das tabelas que faltarem no sandbox, na versão
fixada, e o anti-join roda contra ela. Sem o argumento, órfão nenhum é procurado, e o relatório
registra a verificação como não executada.

| Primitiva | O que faz |
| --- | --- |
| `checks(table, partitions=None, foreign_keys=False, key_scope=None, published=None, referenced=None, published_max_key=None)` | A lista de `Check` (nome, statement Core, o que reprova e, na chave primária inteira de uma coluna, o `skip_when` do mínimo contra `published_max_key`): os defeitos de linha num `count(CASE WHEN <defeito> THEN 1 END)` por coluna na mesma passagem, porque o Redshift não tem a cláusula `FILTER` nos agregados, uma consulta por chave e uma por chave estrangeira; `checks_and_not_run`, protegida, devolve também as que não rodam, com o motivo, e `sample_statement` a consulta da amostra de um contador. |
| `audit_sql(table, dialect, partitions=None, foreign_keys=False, key_scope=None, published=None, referenced=None, prefix="{prefix}")` | `{nome: texto}` por `sql.render` com o prefixo pedido, sem conexão e sem motor: o SQL que a auditoria vai rodar, para depuração. |
| `AuditReport` | Por verificação: nome, o SQL rodado, a contagem de defeitos, uma amostra das linhas reprovadas e o veredito; `passed` é a conjunção, e `report.sql()` devolve o texto de todas; `nonfinite_columns` dá, por valor de partição, as colunas `Double` com valor não finito, a lista que `run.publish` passa a `export_partition` como `columns_without_min_max`; `totals` dá, por valor de partição, a linha da verificação de linhas (a contagem, os contadores, as somas de controle e os não finitos), e `rows(valor)` a contagem que `run.publish` passa como `expected_rows`; `not_run` traz o motivo de cada verificação que não rodou, e `CheckResult.reason` o da dispensa pelo `skip_when`. |

| Primitiva | DuckDB |
| --- | --- |
| `connect(config)` | Banco em arquivo `<temp_directory>/<execution_id>.duckdb`, e em memória só com `DuckDBConfig(database=":memory:")` (decisão do usuário de 2026-09-22); `temp_directory` omitido é uma pasta nova de `tempfile.mkdtemp`, apagada com o banco em `cleanup`, porque o padrão `.tmp` do DuckDB é relativo à pasta corrente; a conexão de `storage.duckdb_connect(<arquivo do banco>, config)` da [etapa 3](PLAN-STAGE-3.md), que resolve `extension_directory` (`DuckDBConfig.extension_directory`, senão `SERIALIZE_DB_DUCKDB_EXTENSIONS`, senão `.duckdb/` ao lado do ambiente virtual), desliga `autoinstall_known_extensions` e `autoload_known_extensions` e aplica `duckdb_setup`; `threads`, `temp_directory` e `preserve_insertion_order = false`; `memory_limit` só quando a configuração o informa, e o log registra na abertura o `current_setting('memory_limit')` que o DuckDB escolheu e o espaço livre de `temp_directory` (decisão do usuário de 2026-09-22); uma conexão só, a sessão da execução, e um `threading.RLock` que todo comando toma pelo tempo do comando (decisão do usuário de 2026-09-22, a mesma do motor Redshift); `session()` dá a conexão crua ao cliente com o lock tomado pelo bloco. |
| `new_session()` | Uma sessão a mais sobre o mesmo banco: um motor sobre `cursor()` da conexão, com o seu `RLock`, as mesmas primitivas e a mesma pasta de transbordo, gerenciador de contexto. Ele vê o que a sessão principal confirmou e não as tabelas temporárias dela; o fim do `with` e o `cleanup` dele fecham só essa conexão. `run.ingest` abre uma por tabela, e o cliente a usa para o que roda em paralelo ([`PLAN.md`](PLAN.md), seção "Regras que as etapas obedecem"). |
| `ingest(table, uri, version, partitions=None, materialize=False)` | View com o nome do modelo sobre `delta_scan(uri, version := v)`, ou `CREATE TABLE ... AS SELECT ... FROM delta_scan(...)` com `materialize=True`; com `partitions`, as duas filtram por `<coluna> BETWEEN '<menor>' AND '<maior>' AND <coluna> IN (...)`, porque o `delta_scan` poda por `=` e por intervalo e abre todos os arquivos com um `IN` de mais de um valor (leitura de 2026-09-23). |
| `published(table, uri, version)` | A versão fixada como origem de consulta, sem ocupar nome no sandbox: o `FromClause` com as colunas do contrato que compila para `delta_scan('<uri>', version := <v>)`. É por ele que o pipeline lê as partições publicadas da tabela que ele mesmo grava, cujo nome no sandbox pertence ao `loader`, e é ele que a auditoria usa como `published` nas chaves que não incluem a coluna de partição (decisão do usuário de 2026-09-22). Sem versão fixada, numa tabela que ainda não existe, levanta `SandboxError` nomeando a tabela. |
| `stream(statement_or_sql, params=None, batch_size=100_000)` | Um statement Core com as tabelas do contrato trocadas pelas do sandbox por `sql.prefixed(prefix="")`, com os valores do cliente dados por `statement.params(**params)` e compilado por `duckdb_engine.Dialect(paramstyle="qmark")`, o estilo do driver do DuckDB, sem `literal_binds` e com `render_postcompile=True`, que expande o `IN` de lista e o `bindparam(..., expanding=True)` (sem ele o texto sai com `__[POSTCOMPILE_...]`, que o DuckDB recusa, leitura de 2026-09-23); os nomes de `params` conferidos contra os `bindparam` sem valor do statement, porque `params` ignora um nome a mais; as constantes e os valores do cliente juntados por `construct_params()` e passados como lista na ordem de `positiontup`, sem reescrever marcador (decisão do usuário de 2026-09-23, leituras de 2026-09-22 e 2026-09-23, [`POC.md`](POC.md); `param` não é exigido); ou um texto pronto, gerado por `render` ou lido por `sql.read_sql(..., prefix="")`, com `:nome` em `$nome` por `sql.bind`; roda numa thread auxiliar, na sessão, sob o lock e sem `Session` do SQLAlchemy, que entrega cada lote de `to_arrow_reader(batch_size)` à memória enquanto os lotes guardados cabem no orçamento de 64 MiB, e a um arquivo Arrow IPC com LZ4 na pasta de transbordo o lote que não cabe e os seguintes, e solta o lock quando o resultado acaba, sem esperar pelo cliente (decisão do usuário de 2026-09-23); dentro de `session()`, na mesma thread, a consulta roda na thread de quem chama. O motor devolve o `BatchStream`: iterável de `pa.RecordBatch` com os tipos do motor (`decimal128(18, 2)`, `date32`, JSON como `string`), `schema`, `read_next_batch`, `read_all`, `close`, gerenciador de contexto e `__arrow_c_stream__` (para `write_deltalake` e `RecordBatchReader.from_stream`, nunca para o `register` do DuckDB). O cliente lê a memória e depois o arquivo, na sua thread e na ordem da consulta, enquanto ela continua, com esperas com prazo; a thread não referencia o stream; `close` cancela por `interrupt()` a consulta que ainda roda e apaga o arquivo; o erro anterior ao primeiro lote chega na construção, e o posterior na leitura seguinte ao último lote entregue. |
| `query(statement_or_sql, params=None)` | O statement Core ou o texto pronto, pelo caminho de compilação de `stream`, sob o lock, por `to_arrow_table()`: a `pa.Table` com os tipos do motor, igual a `stream(statement_or_sql, params).read_all()`, sem arquivo; um comando sem resultado devolve a tabela `Count` ou `Success` do DuckDB. `execute` saiu da interface (decisão do usuário de 2026-09-23). |
| `loader(table, queue_depth=2)` | O gerenciador de contexto que grava lotes numa tabela nova do sandbox, criada por `ddl(table, "duckdb")` no `close`: um nome já ocupado, pela view do `ingest` ou pela tabela de um `loader` anterior, é recusado com `SandboxError` na abertura, antes do primeiro lote (decisão do usuário de 2026-09-22), por uma leitura do catálogo num cursor próprio, sem o lock da sessão (decisão do usuário de 2026-09-23), e um laço por partição mantém um `loader` só aberto; `write(data)` aceita `pa.RecordBatch` ou `pa.Table`, faz `cast(batch, table)` na thread do cliente e põe o lote numa fila limitada; uma thread auxiliar grava os lotes num arquivo Arrow IPC com LZ4 na pasta de transbordo, sem a sessão, e o `close` roda, sob o lock e numa transação, o `CREATE TABLE` e um único `INSERT ... BY NAME SELECT * FROM <leitor do arquivo>`; uma exceção dentro do `with`, um lote recusado pelo `cast` ou um loader abandonado apagam o arquivo sem criar a tabela, e um erro do `INSERT` desfaz o `CREATE`; `close` relança o erro da thread e o do `INSERT`; `rows` conta as linhas gravadas. O arquivo nasce com o esquema do primeiro lote convertido, e um lote com outro conjunto de colunas é `ContractError`; o `loader` sem lote cria a tabela vazia. Nada existe antes do `close`, e o leitor que o `INSERT` consome é o do arquivo, nativo: nenhum gerador Python é entregue ao DuckDB. |
| `load(table, data)` | `data` é uma `pa.Table`, um `pa.RecordBatch`, um `RecordBatchReader` ou um iterável de lotes: `with loader(table) as l: for batch in ...: l.write(batch)`; outro tipo é recusado com a mensagem que aponta `pa.Table.from_pandas` e `pa.RecordBatch.from_pandas`. |
| `audit(table, partitions, uri=None, version=None, foreign_keys=False, key_scope=None, referenced=None)` | `uri` e `version` são os da tabela fixada, e montam `published` e o `published_max_key` de `delta.max_key` sem depender de `Execution`; `referenced` dá, por nome de tabela, a URI e a versão fixada da tabela referenciada que o sandbox não tem, e a que o sandbox tem entra pelo nome do modelo. Roda o texto de `sql.render(check.statement, "duckdb", metadata, prefix="")` de cada verificação — `json_valid` e `strftime(data, '%Y-%m-%d')` são as funções do dialeto — e monta o `AuditReport`, com até 20 linhas inteiras de amostra por verificação reprovada (decisão do usuário de 2026-09-22): a verificação `linhas` conta por coluna e não tem linha para amostrar, então cada contador acima de zero ganha uma segunda consulta, `SELECT * ... WHERE <condição da coluna> LIMIT 20`, rodada só na reprovação; a comparação com as demais partições sai de `published` na versão fixada, e `passed` falso interrompe a execução. |
| `export_partition(table, uri, value, metadata, mode, expected_rows=None, columns_without_min_max=())` | `uri`, `metadata` e `expected_rows` vêm do chamador, porque o motor não conhece o `Database` e o commit precisa dos metadados. `mode` é a flag `export_mode` já resolvida pela [etapa 6](PLAN-STAGE-6.md) (`register` ou `rewrite`; o motor não lê `SERIALIZE_DB_EXPORT_MODE`, decisão do usuário de 2026-09-23), a mesma do motor Redshift ([etapa 5](PLAN-STAGE-5.md)) e da carga inicial ([etapa 7](PLAN-STAGE-7.md)). `columns_without_min_max`, as colunas `Double` com valor não finito na partição, vem de `run.publish` e vai a `register_files` ou a `publish_partition`. **`register`**: `COPY (SELECT <cada coluna do contrato em CAST para o tipo dele, sem a de partição> FROM <sandbox> WHERE <coluna de partição> = '<valor>' ORDER BY <sort_key>) TO '<uri>/<coluna de partição>=<valor>/<execution_id>_<uuid>.parquet' (FORMAT parquet, RETURN_STATS)`, com a contagem do sandbox no mesmo bloco da sessão, mais `register_files` com as estatísticas de `RETURN_STATS`, as conferências da [etapa 3](PLAN-STAGE-3.md) antes do commit e a releitura depois; a memória é constante (600 MB na reescrita medida em [`delta.md`](delta.md)). O `uuid` no nome impede uma reexecução com o mesmo `execution_id` de sobrescrever o arquivo que a versão anterior referencia. **`rewrite`**: o leitor do DuckDB da partição, `to_arrow_reader` sob o lock, passado por `cast` a `publish_partition`, sem `stream` nem arquivo, porque ali não há trabalho do cliente para sobrepor (decisão do usuário de 2026-09-23); o lock fica tomado pela escrita, como no `COPY` do `register`, o `write_deltalake` calcula estatística e nulidade, e a memória cresce com a partição (1.140 MB para 135 MB de Parquet na mesma medição). A mesma partição sai igual pelos dois, e o teste os compara. |
| `cleanup()` | Cancela por `interrupt()` o comando em curso, fecha a conexão e apaga o arquivo do banco, o seu `.wal` e a pasta de transbordo, e a pasta de `tempfile.mkdtemp`; o banco de um caminho que a configuração informou fica. Numa sessão a mais, fecha só o cursor; a segunda chamada não faz nada. |

Testes: `tests/test_audit.py`, sem gravar: o texto de cada verificação nos dois dialetos, a chave
lida do `primary_key` do modelo e o escopo escolhido pelas colunas da chave. `tests/test_engine_duckdb.py` sob a raiz local: o pipeline de exemplo (doze partições
materializadas e dimensões em view, um `select` com `join`, auditoria, com `nonfinite_columns` numa partição com `NaN`, exportação da partição nos dois modos com as mesmas linhas e sem o mínimo e o máximo dessa coluna) sobre um
Delta local criado no teste; o ciclo `query`, `to_pandas(types_mapper=pd.ArrowDtype)`, `from_pandas` e
`load` com `decimal128(18, 2)` e `date32` mantidos, `load` recusando um DataFrame e o `loader`
recusando o nome que a view do `ingest` ocupa, com a leitura da versão publicada por `published` no
lugar; o ciclo por
lotes: `stream` que entrega o primeiro lote enquanto a consulta roda e deixa outros comandos
rodarem, `stream` sobre uma tabela temporária e dentro de `session()`, `stream` fechado no meio, `loader` que não insere nada na exceção do cliente e no lote
recusado pelo `cast`, `load` de um iterável e de uma `pa.Table` com o mesmo resultado, e `query`
igual a `stream(...).read_all()`;
auditoria que reprova a chave repetida dentro da partição e a repetida contra uma partição já
publicada, e o órfão de chave estrangeira com a tabela referenciada fora do sandbox; `query` de um
texto com `%` em literal. Provas de conceito: `test_duckdb.py` (configuração, Arrow na entrada e na saída, o
leitor esvaziado pelo comando seguinte e preservado num cursor próprio, a consulta em streaming com
a memória medida em subprocesso, o `INSERT` sobre um leitor de gerador com a leitura antecipada e
os lotes que o leitor não confere, `DECIMAL`, JSON, `executemany`, `COPY` com `RETURN_STATS` e
particionado, banco em arquivo, `test_audit_queries` com o texto medido em bytes por `strlen`),
`poc_delta.py`
(`delta_scan`, poda, tempos, `ATTACH ... PIN_SNAPSHOT`),
`test_deltalake.py::test_duckdb_view_pins_version_and_reader_feeds_write`
(a view presa a `version := v` e o `to_arrow_reader()` no `write_deltalake` com predicado),
`test_sqlalchemy.py::test_arrow_path_on_raw_connection`,
`test_stdlib.py::test_engine_protocol_and_config_dataclass` e `test_concurrency.py` (o GIL liberado
pelo DuckDB, pelo delta-rs e pelo PyArrow, um `cursor()` por thread, a conexão compartilhada sem lock
que troca os resultados das threads, o arquivo do DuckDB recusado com outra configuração, dois
comandos em dois cursores, o intervalo de troca do GIL pago por cada retomada ao lado de uma thread
Python ocupada) e `test_parallel.py` (a ingestão de quatro tabelas por `delta_scan` em cursores
do DuckDB cru, as escritas paralelas no Delta, e cargas Arrow e `COPY ... TO` pedidos por quatro
threads a uma conexão) e
`test_pyarrow.py::test_record_batch_cast_and_conversions_share_buffers`; `test_duckdb.py` mede a
memória do stream transbordado (`test_spooled_stream_bounds_memory`).

## Estratégia de implementação

- **`checks`** monta os statements Core sobre a tabela do contrato, e `audit_sql` os renderiza pela
  cópia prefixada de `sql.render` com o prefixo pedido; `checks` não recebe `prefix` (decisão do
  usuário de 2026-09-23). Uma consulta de linhas, agrupada pela coluna de partição numa tabela
  particionada, reúne num `count(CASE WHEN <defeito> THEN 1 END)` por coluna, a forma que os dois
  motores aceitam, porque o `COUNT` do Redshift não tem a cláusula `FILTER` (documentação lida em
  2026-09-23): nulo em `NOT NULL`, texto acima de
  `String(n)` em bytes, JSON inválido, a coluna de partição diferente de `partition_text(partition_source)`
  quando o modelo declara `partition_source`, o valor de partição fora da regra da partição,
  `[0-9A-Za-z][0-9A-Za-z_.-]*` (a partição é texto desde 2026-09-22, e a data é o caso da base
  atual), e a soma de controle de
  cada coluna `Double` e `Numeric` como `DECIMAL(38, 6)`. A soma de uma coluna `Double` corre só
  sobre os valores finitos, `sum(CASE WHEN isfinite(x) THEN CAST(x AS DECIMAL(38, 6)) END)` no
  DuckDB, e a consulta conta à parte os não finitos de cada coluna, que dão `nonfinite_columns`:
  o `CAST` de um `NaN` ou de um infinito para
  `DECIMAL` falha com `ConversionException` e derrubaria a verificação inteira, e um filtro do
  agregado não o evita (leitura de 2026-09-23). A contagem entra no relatório sem reprovar, porque
  o `cast` aceita o `Double` não finito, e dá a lista `columns_without_min_max` da
  [issue #59](https://github.com/felipenoris/serialize-db/issues/59) (decisões do usuário de
  2026-09-23). No Redshift, `is_finite` sai `(x > '-Infinity'::float8 AND x < 'Infinity'::float8)`:
  no ambiente alvo, em 2026-09-23, o `NaN` de uma constante saiu igual a si mesmo, como no
  PostgreSQL, e o de uma varredura de tabela passou por `x NOT IN ('NaN'::float8, ...)`, como no
  IEEE, e chegou ao `CAST` da soma; a comparação estrita dá falso ao `NaN` pelas duas regras e
  espera a próxima execução da suíte ([`POC.md`](POC.md)). As funções com `@compiles` por dialeto
  fazem a portabilidade, e cada uma é subclasse de `FunctionElement` com `name`, como o `month_of`
  de [`sqlalchemy.md`](sqlalchemy.md), e não de `GenericFunction`, a classe do rascunho de
  2026-09-21, que se registra em `sa.func` para o processo inteiro: depois dela, o
  `sa.func.json_valid` do próprio cliente sai pela regra da biblioteca no Redshift, e a subclasse
  de `FunctionElement` deixa o `sa.func` intacto (leitura de 2026-09-23). Cada uma tem também uma
  regra padrão, o nome com os argumentos: sem ela a subclasse não compila fora dos dialetos que a
  declaram, e o dialeto do `duckdb_engine` é um compilador do PostgreSQL
  (`UnsupportedCompilationError`, leitura de 2026-09-23). `partition_text` é
  `strftime(x, '%Y-%m-%d')` no DuckDB e `to_char(x, 'YYYY-MM-DD')` no Redshift; `json_valid` fica
  no DuckDB e vira `true` no Redshift, onde a coluna JSON é `SUPER`, que sempre se serializa como
  documento JSON e que o `is_valid_json` recusa (`42883`, 2026-09-23); `text_bytes`, o comprimento
  em bytes que o `String(n)` mede como o `VARCHAR(n)` do Redshift, é `strlen` no DuckDB e
  `octet_length` no Redshift, porque o `length` dos dois conta caracteres e o `octet_length` do
  DuckDB só aceita `BLOB`; `partition_value_valid`, a regra da partição, é `regexp_full_match(x,
  '[0-9A-Za-z][0-9A-Za-z_.-]*')` no DuckDB, que concordou com o `re.fullmatch` do Python em doze
  valores (2026-09-23), e o operador POSIX `x ~ '^[0-9A-Za-z][0-9A-Za-z_.-]*$'` no Redshift. Uma
  consulta por chave (`GROUP BY ... HAVING count(*) > 1`) dentro das partições da execução, e,
  quando a chave não inclui a coluna de partição e
  `key_scope != "partition"`, uma segunda consulta que junta o sandbox com `published` (o
  `delta_scan` da versão fixada, ou a staging no Redshift) fora das partições da execução. A chave
  que inclui a coluna de `partition_source` não ganha a segunda consulta, e a chave primária inteira
  de uma coluna a ganha com o `skip_when` `SELECT min(<chave>) > <published_max_key>` nas partições
  da execução, quando `published_max_key` vem informado. Uma
  consulta por chave estrangeira (anti-join contra `referenced[tabela]`) só com
  `foreign_keys=True`; sem ele, o nome entra em `not_run`.
- **`audit_sql`** renderiza cada `Check` por `sql.render` com o prefixo pedido; é o texto que
  `serialize-db audit --sql` imprime, para depuração, e nenhum arquivo o guarda (decisão do usuário
  de 2026-09-23).
- **`AuditReport`** guarda por verificação o SQL rodado, a contagem e uma amostra; `passed` é a
  conjunção; `sql()` concatena os textos para o log da execução.
- **`DuckDBEngine.__init__`** abre a conexão raiz por `storage.duckdb_connect(<arquivo do banco>,
  config)` da [etapa 3](PLAN-STAGE-3.md), com `threads`, `temp_directory`,
  `preserve_insertion_order = false` e, quando a configuração os informa, `memory_limit` e
  `extension_directory`; a etapa 3 resolve a pasta de extensões, desliga a instalação e a carga
  automáticas e aplica `duckdb_setup`. O banco é um arquivo em `<temp_directory>/<execution_id>.duckdb` por
  padrão: no ambiente alvo a máquina tem 7,6 GiB de memória, 2 vCPUs e 29,8 GiB livres num só
  disco, e uma tabela materializada de doze partições de `cad_lancamentos` não cabe em memória, cabe
  em disco ([`POC.md`](POC.md), leitura de 2026-09-21). `temp_directory` omitido é uma pasta de
  `tempfile.mkdtemp`, e `cleanup` apaga a pasta com o banco dentro. O `memory_limit` fica no padrão
  do DuckDB, 80% da memória, e só é ajustado quando a configuração o informa, em bytes ou com
  unidade, porque o DuckDB recusa porcentagem (`Unknown unit for memory: '%'`, leitura de
  2026-09-22). O que o DuckDB escolheu (`current_setting('memory_limit')`) e o espaço livre de
  `temp_directory`, lido por `shutil.disk_usage`, vão para o log na abertura: é por ele que a
  primeira execução real mede quanto sobra para o pandas do cliente, que o limite do DuckDB não
  cobre. `threads` omitido fica no padrão do DuckDB, um por núcleo; ele é da instância, vale para a
  sessão principal e para as de `new_session()`, e muda em execução por `SET threads`. A leitura do
  S3 pede mais threads que núcleos, porque cada thread faz uma requisição HTTP por vez, e quantas é
  o que a primeira execução no ambiente alvo mede ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)). A
  conexão é a sessão da execução, e `session()` toma o `threading.RLock` e a dá ao
  bloco; toda primitiva toma o mesmo lock pelo tempo do seu comando, e nenhuma espera pelo código do
  cliente com ele tomado. O motor guarda a thread que está dentro de `session()`, e é por ela que
  `stream` sabe quando roda a consulta na thread de quem chama. `new_session()` devolve um motor
  sobre `cursor()` da conexão, com o seu lock e a mesma pasta de transbordo; o `cleanup` dele fecha
  só o cursor. O cursor nasce sem o lock da sessão principal: `cursor()` voltou em 0,04 ms com uma
  ordenação em curso na conexão (leitura de 2026-09-23), e a sessão a mais pedida durante um
  comando longo não espera por ele.
  Os arquivos intermediários de `stream` e de `loader` ficam na pasta de transbordo e saem no
  `close`.
- **`ingest`** cria `VIEW <nome do modelo> AS SELECT * FROM delta_scan('<uri>', version := <v>)`,
  ou a `TABLE` com `materialize=True`, na sessão. Com `partitions`, o filtro é `<coluna> BETWEEN
  '<menor>' AND '<maior>' AND <coluna> IN (...)`: numa tabela de 12 partições, o `IN` de dois
  valores e o `OR` abriram os 12 arquivos, e o intervalo abriu só os seus (log `FileSystem` do
  DuckDB, 2026-09-23, [`POC.md`](POC.md)). As `n` partições de `previous_partitions` são contíguas,
  e o intervalo é exato; numa lista salteada, ele limita a leitura e o `IN` filtra as linhas. O
  `EXPLAIN ANALYZE` dessa forma falha com `InternalException` na extensão `delta`, e o teste lê a
  poda pelo log.
- **`published`** devolve o `FromClause` que compila para `delta_scan('<uri>', version := <v>)`, com
  as colunas do contrato, e não cria objeto no sandbox: é a origem que a auditoria já precisa nas
  chaves fora da partição, e a que o pipeline usa para ler a tabela cujo nome no sandbox é a saída
  do `loader`. No Redshift ele é a staging da [etapa 5](PLAN-STAGE-5.md).
- **`stream`** devolve um `DuckDBStream`: um statement Core vira a cópia prefixada de
  `sql.prefixed(prefix="")`, recebe os valores do cliente por `statement.params(**params)` e é
  compilado por `duckdb_engine.Dialect(paramstyle="qmark")`, o estilo do driver do DuckDB, sem
  `literal_binds` e com `render_postcompile=True`; `construct_params()` junta as constantes e os
  valores, e a lista posicional sai na ordem de `compiled.positiontup`, sem reescrever marcador
  algum (decisão do usuário de 2026-09-23). A sonda de 2026-09-23 achou o `IN` de lista, que sem
  `render_postcompile` sai `__[POSTCOMPILE_...]`, e rodou o `qmark` com um `?` dentro de um literal
  de `text()`. Antes de
  compilar, os nomes de `params` são conferidos contra os `bindparam` sem valor do statement, e um
  nome a mais ou a menos é `SqlError`, como no `bind`: `params` ignora o nome a mais, e o valor que
  falta seria `InvalidRequestError` do SQLAlchemy. Um texto pronto, já sem o sentinela, passa por
  `bind`. Uma thread auxiliar roda a consulta na sessão, sob o lock, com
  `to_arrow_reader(batch_size)`, e guarda cada lote numa fila em memória enquanto os lotes guardados
  cabem no orçamento de 64 MiB; o primeiro lote que não cabe inaugura o arquivo Arrow IPC com LZ4 na
  pasta de transbordo, e todo lote seguinte vai para ele, para a ordem se manter (decisão do usuário
  de 2026-09-23). O estado entre a thread e o cliente fica num `Spool` sob uma
  `threading.Condition`; o cliente esvazia a fila e depois lê o arquivo, na sua thread, com esperas
  com prazo. A thread recebe só o que usa, nunca o stream, confere o `stop` antes da consulta e a
  cada lote, marca o fim ainda com o lock tomado e, parada pelo `stop`, apaga o arquivo que criou,
  porque ele pode nascer depois de o `__del__` apagar o caminho. `close` liga o `stop`, chama
  `interrupt()` na conexão quando a consulta ainda roda, sob a `Condition`, espera a thread e apaga
  o arquivo. Dentro de `session()`, na mesma thread, a consulta roda na thread de quem chama e o
  cliente lê os lotes depois dela, porque a auxiliar esperaria o bloco. Em 13.333.333 linhas, sem
  trabalho, com 5 ms de Python puro por lote e com pandas, o total foi 0,411 s, 0,673 s e 0,411 s,
  sem lote no arquivo; com o cliente atrasado, 0,908 s e 297 MB de pico com 96 lotes no arquivo
  (2026-09-23, [`POC.md`](POC.md)). O motor abre com `preserve_insertion_order = false`: num filtro
  que acha poucas linhas, todas no começo da tabela, o primeiro lote só sai no fim da consulta com
  duas threads (1,093 s de 1,093 s, contra 0,373 s com a ordem preservada); no filtro de uma
  partição entre doze, o do pipeline mensal, o primeiro lote chegou em 2 a 3 ms nos dois ajustes, e
  o `CREATE TABLE AS` de 17.000.000 de linhas levou 0,533 s e 802 MB acima da base com a ordem
  livre, contra 0,908 s e 876 MB (leituras de 2026-09-23, [`POC.md`](POC.md)).
- **`query`** roda sob o lock, pelo caminho de compilação de `stream` para um statement e por `bind`
  para um texto pronto, e devolve `to_arrow_table()`, sem arquivo.
- **`loader`** devolve um `DuckDBLoader`, com a recusa do nome ocupado na abertura:
  `duckdb_tables()` e `duckdb_views()` dizem o que existe, e um nome tomado levanta `SandboxError`
  com a mensagem que aponta `run.published(table)` para ler a versão publicada. O `CREATE TABLE IF
  NOT EXISTS` não serve de guarda: sobre uma view ele passa em silêncio e o `INSERT ... BY NAME`
  seguinte morre com `Catalog Error: <nome> is not an table`, e sobre uma tabela de outro formato
  ele também passa, deixando o `INSERT ... BY NAME` preencher com nulo a coluna que sobra (leituras
  de 2026-09-22). A guarda lê o catálogo num cursor próprio, sem o lock da sessão, e a abertura não
  toca a sessão: um `CREATE TABLE` sob o lock, na abertura de um `loader` aberto depois de um
  `stream`, como no exemplo mensal de [`PLAN.md`](PLAN.md), esperaria a consulta inteira do stream
  antes do primeiro lote, 0,811 s contra 0,006 s em 20.000.000 de linhas (leitura e decisão do
  usuário de 2026-09-23, [`POC.md`](POC.md)). O cursor não vê as tabelas temporárias da sessão
  principal, e um nome ocupado entre a abertura e o `close` falha no `CREATE` do `close`, sem
  inserir nada. Por isso um `load` disparado numa thread e esquecido sem `result()` não deixa a
  leitura ver dado velho: antes do `close`, a leitura da tabela falha com `CatalogException`, na
  sessão principal e numa sessão a mais, e a barreira por tabela que esperaria a carga ficou fora
  das etapas (decisão do usuário de 2026-09-23,
  `test_engine_duckdb.py::test_read_during_a_forgotten_load_fails_instead_of_reading_old_rows`).
  Depois da guarda, `write` faz `cast(batch, table)` na thread do cliente e enfileira,
  e a thread auxiliar grava os lotes num arquivo Arrow IPC com LZ4, sem a sessão. O `close` registra
  o leitor do arquivo com um nome único, que não é o de uma tabela do modelo, porque um leitor
  registrado ocupa um nome de view (leitura de 2026-09-22), e roda, sob o lock e numa transação,
  `CREATE TABLE <nome> (ddl)` e um único `INSERT INTO <nome> BY NAME SELECT * FROM <leitor>`: um erro
  do `INSERT` desfaz o `CREATE` pelo `ROLLBACK`, e o nome fica livre (leitura de 2026-09-23);
  qualquer erro apaga o arquivo e sobe em `close` ou no `write` seguinte. `load` embrulha
  `loader` para `pa.Table`, `RecordBatch`, `RecordBatchReader` e iteráveis, e recusa o resto com a
  mensagem que aponta `pa.Table.from_pandas`.
- **`audit`** roda `audit_sql(table, "duckdb", published=published(table, uri, version), ...)` e
  monta o `AuditReport`; `passed` falso não levanta aqui, levanta em `Execution.audit`. Cada
  verificação reprovada leva até 20 linhas inteiras de amostra: as verificações de chave e de
  órfão já devolvem as linhas, e a de `linhas`, que conta por coluna, ganha uma segunda consulta por
  contador acima de zero, `SELECT * ... WHERE <condição da coluna> LIMIT 20`, rodada só quando esse
  contador reprova. A amostra vai para o log, com o relatório; `_serialize_db/` não a recebe.
  `published_max_key` vem de `delta.max_key` sobre `delta.open_table(uri, storage, version)` quando
  a chave primária é inteira, de uma coluna e fora do escopo da partição; o `skip_when` de uma
  verificação roda antes dela e, verdadeiro, a aprova sem rodá-la, com o motivo no relatório.
- **`export_partition`** com `mode="register"`: `COPY (SELECT <colunas do contrato na ordem dele,
  sem a de partição> FROM <sandbox> WHERE <coluna> = '<valor>' ORDER BY <sort_key>) TO
  '<uri>/<coluna>=<valor>/<execution_id>_<uuid>.parquet' (FORMAT parquet, RETURN_STATS)`, com a pasta
  da partição criada antes por `storage.ensure_folder` na raiz local, porque o `COPY` para um arquivo
  não cria a pasta; a linha do `RETURN_STATS` vira um `RegisteredFile` por
  `delta.file_from_return_stats` (contagem, tamanho, `null_count` de todas as colunas, `min` e
  `max` dos tipos que transcrevem exato), e `delta.register_files` faz as conferências, o commit e a
  releitura, com `expected_rows` do `count(*)` do sandbox. A ordem das colunas e a coluna de
  partição fora do arquivo são conferências de `register_files` ([etapa 3](PLAN-STAGE-3.md)): o
  `COPY` posicional do Redshift leria o arquivo fora delas. Com `mode="rewrite"`: sob o lock, o `to_arrow_reader` da partição passado por `cast` a
  `delta.publish_partition`, sem `stream` nem arquivo. O modo chega resolvido por `Execution`.
- **`cleanup`** chama `interrupt()` na conexão antes de tomar o lock, porque a execução acabou e o
  comando em curso, de um stream que ninguém lê, é cancelado em vez de esperado (2 ms com uma
  ordenação em curso, decisão do usuário de 2026-09-23); depois fecha a conexão e apaga o arquivo do
  banco e a pasta de transbordo.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `checks`, `audit_sql` | Modelo aprovado por `check_models`; `published` informado quando alguma chave não inclui a coluna de partição. | Um `Check` por consulta, com o texto renderizável nos dois dialetos; sem conexão. |
| `DuckDBEngine` | Extensões na pasta configurada. | Uma conexão sobre o arquivo do banco, a sessão, sob um `RLock`; nenhum download; o `memory_limit` do DuckDB e o espaço livre de `temp_directory` no log. |
| `ingest` | Tabela Delta legível na versão pedida. | Uma view ou tabela com o nome do modelo, presa à versão; a tabela atual pode avançar sem afetar a leitura. |
| `published` | Tabela Delta legível na versão fixada. | Um `FromClause` sobre essa versão; nenhum objeto no sandbox, e o nome do modelo livre para o `loader`. |
| `new_session` | O motor aberto. | Outra conexão ao mesmo banco, com o seu lock; o que a sessão principal confirmou visível, as tabelas temporárias dela não; o fim do `with` fecha só essa conexão. |
| `stream` | Texto ou statement válido; parâmetros que fecham com o dicionário. | Lotes na ordem da consulta, o primeiro antes do fim de uma consulta sem operador bloqueante; no máximo o orçamento de 64 MiB de lotes em memória; a sessão livre quando a consulta acaba, sem esperar o cliente; a consulta cancelada e o arquivo apagado em `close`; o erro anterior ao primeiro lote no construtor, o posterior na leitura seguinte ao último lote entregue. |
| `loader`, `load` | O nome da tabela livre no sandbox; lotes que passam por `cast`. | Nada existe antes do `close`, que cria a tabela e insere numa transação; nenhuma tabela na exceção, no lote recusado ou no erro do `INSERT`; `rows` igual às linhas escritas; `SandboxError` com o nome ocupado, sem criar nem alterar objeto algum. |
| `audit` | Sandbox com a tabela; `uri` e `version` para as chaves fora da partição. | Um `AuditReport` com o SQL de cada verificação e até 20 linhas de amostra por verificação reprovada; nenhuma escrita. |
| `export_partition` | Auditoria aprovada (conferida por `Execution.publish`); a pasta da partição gravável. | Uma versão nova no Delta com a partição substituída e as estatísticas registradas; a mesma partição sai igual pelos dois modos. |
| `cleanup` | Nenhum. | O comando em curso cancelado; arquivo do banco e pasta de transbordo apagados; chamadas seguintes falham. |

## Testes por caso

`tests/test_audit.py` sem gravar, sobre o modelo cliente e uma tabela com uma coluna de cada tipo
que muda de função entre os motores; `tests/test_engine_duckdb.py` sob a raiz local, sobre um
Delta criado no teste a partir de um modelo com a partição, a origem dela, uma chave estrangeira e
uma chave única.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Texto das verificações | `test_audit_sql_per_dialect` | Cada `Check` do modelo cliente renderiza nos dois dialetos; `strftime` no DuckDB e `to_char` no Redshift; `json_valid` e `true`; `strlen` e `octet_length`; `isfinite` e a comparação estrita com os infinitos, que `test_redshift_is_finite_under_the_postgresql_rule` roda no DuckDB sobre o `NaN`, os infinitos, um número e o nulo. |
| Escopo da chave | `test_key_scope_follows_the_partition_column` | Chave com a coluna de partição ou a de `partition_source` gera uma consulta; sem elas, gera a segunda contra `published`; `key_scope="partition"` a suprime e o relatório registra; a chave primária inteira de uma coluna leva o `skip_when` do mínimo contra `published_max_key`. |
| Chave estrangeira | `test_foreign_key_check_only_on_request` | Sem `foreign_keys=True` o nome está em `not_run`; com ele, o anti-join contra `referenced`. |
| Defeitos plantados | `test_audit_finds_each_defect` | Nulo, texto acima do `String(n)` em bytes e dentro dele em caracteres, partição fora da origem, valor de partição fora da regra, JSON inválido numa tabela criada por SQL (a coluna `JSON` do DuckDB recusa o texto inválido na carga), chave repetida dentro da partição e contra a publicada. |
| Chave única publicada | `test_audit_unique_key_against_the_published_version` | A chave única sem a coluna de partição nem a de origem, repetida contra a versão publicada. |
| Órfão | `test_audit_orphan_against_a_referenced_table_outside_the_sandbox` | Com `foreign_keys=True`, a tabela referenciada fora do sandbox entra pela versão fixada de `referenced` e o órfão aparece; sem o argumento, a verificação fica em `not_run`. |
| Regra da partição | `test_partition_values_follow_the_rule` | O valor fora da regra é recusado antes de qualquer texto. |
| Pasta temporária | `test_temporary_folder_is_created_and_removed` | Sem `temp_directory`, a pasta nova sai inteira no `cleanup`. |
| Sessão | `test_engine_config_and_single_session` | `duckdb_settings()` com os valores pedidos, o `memory_limit` no padrão do DuckDB quando a configuração o omite, e o banco em arquivo dentro da pasta de `tempfile.mkdtemp`; três threads usam a mesma sessão, uma de cada vez; a tabela temporária criada por uma é visível às outras; uma primitiva chamada dentro de `session()` não trava. |
| Sessão a mais | `test_new_session_runs_beside_the_main_one` | A sessão de `new_session` vê a tabela confirmada pela principal e não a temporária dela, roda enquanto a principal está num bloco `session()`, e a principal vê o que ela confirma; o fim do `with` fecha só o cursor, e o arquivo do banco continua. |
| Ingestão presa | `test_ingest_pins_the_version` | Um `append` na tabela depois da abertura não aparece na view nem na tabela materializada. |
| Poda da ingestão | `test_ingest_opens_only_the_range_of_partitions` | A ingestão de partições contíguas e de uma lista salteada abre só os arquivos do intervalo, lidos pelo log `FileSystem` do DuckDB, e traz só as linhas pedidas. |
| Parâmetros | `test_statement_parameters_expand_in_lists` | Um statement com `IN` de lista, `NOT IN` e `bindparam(..., expanding=True)` roda por `query` e por `stream`; um nome de parâmetro a mais ou a menos é `SqlError` antes de rodar. |
| Versão publicada | `test_published_reads_the_pinned_version` | `published` lê a versão fixada sem criar objeto no sandbox, e o `loader` da mesma tabela fica com o nome do modelo. |
| Stream | `test_stream_delivers_each_batch_while_the_query_runs` | O primeiro lote com a consulta ainda rodando, a ordem, nenhum lote no arquivo com o cliente acompanhando, um comando no meio da leitura que só roda depois do fim da consulta, a tabela temporária lida pelo stream, o stream dentro de `session()`, o abandono que para a consulta e apaga o arquivo, e o erro da consulta na construção ou na leitura seguinte ao último lote, da memória ou do arquivo. |
| Orçamento | `test_stream_spills_after_the_budget_and_keeps_the_order` | Com o cliente lento e um orçamento de dois lotes, a memória nunca passa do orçamento, o resto vai para o arquivo, as linhas saem todas e na ordem, e o `close` apaga o arquivo. |
| Cancelamento | `test_close_and_cleanup_interrupt_the_running_query` | O `close` depois do primeiro lote de uma varredura longa cancela a consulta, e a sessão continua usável; o `cleanup` cancela a ordenação de um stream que outra thread ainda constrói. |
| Loader | `test_loader_creates_and_inserts_in_one_transaction_on_close` | Nada existe antes do `close`; exceção do cliente, lote recusado pelo `cast` e erro do `INSERT` não deixam tabela; `rows`; o loader abandonado sem `close` apaga o arquivo de transbordo e não cria a tabela. |
| Ordem do exemplo mensal | `test_loader_opened_after_a_stream_does_not_wait_for_its_query` | Com `stream` e depois `loader` no mesmo `with`, o primeiro lote chega com a consulta rodando, e a tabela tem todas as linhas no fim. |
| Pipeline de três estágios | `test_three_stage_pipeline_overlaps_read_work_and_write` | Leitura por `stream`, trabalho do cliente por lote e escrita por `loader` numa sessão única, sobre um banco em arquivo, dão as mesmas linhas que a versão por lote sem threads e que a versão por `pa.Table`; os tempos são leituras do relatório. |
| Carga esquecida | `test_read_during_a_forgotten_load_fails_instead_of_reading_old_rows` | Um `load` disparado numa thread sem `result()`: antes do `close` do `loader`, a leitura da tabela falha com `CatalogException` na sessão principal e numa sessão a mais, em vez de ler dado velho. |
| Nome ocupado | `test_loader_refuses_a_name_in_use` | O `loader` sobre a view do `ingest`, sobre a tabela do `ingest` materializado e sobre a tabela de um `loader` anterior levanta `SandboxError` antes do primeiro lote, e o objeto que estava lá não muda. |
| Formas por tabela | `test_query_and_load_match_stream_and_loader` | `query` de um statement e de um texto igual a `stream(...).read_all()`; `load` de `pa.Table`, `RecordBatch`, leitor e iterável com o mesmo resultado; DataFrame recusado com a mensagem. |
| Ciclo pandas | `test_pandas_round_trip_keeps_contract_types` | `to_pandas(types_mapper=pd.ArrowDtype)` e `from_pandas` mantêm `decimal128(18, 2)` e `date32`. |
| Exportação | `test_export_partition_modes_produce_the_same_partition` | `register` e `rewrite` sobre a mesma partição dão as mesmas linhas e somas; `register` poda pela estatística (`Scanning Files: 0/n`); `expected_rows` diferente recusa. |
| Pipeline de exemplo | `test_example_pipeline_in_a_file_backed_database` | Doze partições materializadas, dimensões em view, um `select` com `join`, auditoria, exportação; o arquivo `.duckdb` apagado por `cleanup`. |
| Dispensa da junção | `test_audit_skips_the_published_join_above_max_key` | Com as chaves da execução acima do `max_key` da versão fixada, a junção com as demais partições não roda e o relatório diz por quê; com uma chave abaixo dele, a junção roda e acha a repetição. |
| Amostra | `test_audit_report_samples_failing_rows` | Até 20 linhas inteiras por verificação reprovada; a de `linhas` busca as suas numa segunda consulta por contador acima de zero, e uma verificação aprovada não traz amostra; o `NaN` entra em `nonfinite_columns` sem reprovar. |
| Texto com `%` | `test_query_keeps_percent_literals` | `LIKE 'A%'`, num statement e num texto pronto, chega ao DuckDB como está. |

## A implementação

Os módulos `serialize_db.audit`, `serialize_db.engine` (o protocolo `Engine`, `BatchStream`,
`Loader` e `ExportMode`) e `serialize_db.engine.duckdb` (`DuckDBConfig`, `DuckDBEngine`, e o
`DuckDBStream` e o `DuckDBLoader` dele), com `SandboxError` em `serialize_db.errors`, e os casos de
`tests/test_audit.py` e `tests/test_engine_duckdb.py` substituem a interface e os rascunhos
executados em 2026-09-21: as assinaturas e as docstrings estão no código e na documentação do
`pdoc`. A sessão a mais de `new_session` é um `DuckDBEngine(..., parent=motor)`. O que a
implementação mostrou está em [`POC.md`](POC.md), seção
"O que a implementação da etapa 4 mostrou".

## Decisões pendentes

Nenhuma. As decisões que o usuário tomou em 2026-09-23 sobre as propostas da revisão estão escritas
nas seções que as descrevem: o estilo `qmark` no caminho do statement Core; o escopo das chaves da
auditoria pela coluna de `partition_source` e pelo `skip_when` contra o `max_key` da versão fixada;
a interface do motor, com `query(statement_or_sql, params=None)` no lugar de `query` e `execute`, o
`mode` de `export_partition` resolvido em `Execution`, os argumentos nomeados no lugar de
`**options`, `checks` sem `prefix` e o `rewrite` de `export_partition` sem `stream`; o `loader` que
confere o nome num cursor próprio e cria a tabela no `close`, numa transação; o `stream` híbrido,
com os lotes em memória até 64 MiB e o arquivo com LZ4 depois; e o `interrupt()` no `close` do
stream e no `cleanup`. O motor implementa as três últimas, e `tests/test_engine_duckdb.py` tem os
casos de cada uma.

As quatro decisões que o usuário tomou em 2026-09-22 — o `loader` recusando com `SandboxError` um
nome já ocupado no sandbox, o `memory_limit` no padrão do DuckDB, o banco em arquivo com
`temp_directory` em pasta nova, e a amostra de até 20 linhas inteiras por verificação reprovada —
estão escritas, cada uma, na seção que a descreve. A recusa do `loader` trouxe duas
decisões do mesmo dia: `published(table, uri, version)` no protocolo dos dois motores, por onde o
pipeline lê as partições publicadas da tabela que ele grava, e `SandboxError` como a exceção da
etapa em `serialize_db.errors`.
