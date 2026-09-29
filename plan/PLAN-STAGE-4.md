# Etapa 4: `audit` e motor DuckDB

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.audit` monta as verificações a partir do contrato e não sabe qual motor as roda;
`serialize_db.engine` declara o protocolo `Engine`, que a execução também não conhece, e
`serialize_db.engine.duckdb` o implementa. O protocolo fixa os tipos da fronteira: `stream` devolve
um `BatchStream` de `pa.RecordBatch` e `appender` recebe lotes por `write`; `query` devolve a
`pa.Table` que `stream` montaria, e `append` entrega ao `appender` os lotes de uma `pa.Table`, de um
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
| Texto acima de `String(n)` em bytes, texto de uma coluna `Uuid` acima de 36 bytes (decisão do usuário de 2026-09-25) e valor fora do `Numeric(18, 2)` | os tipos do contrato (`docs/index.md`); no Redshift, o `COPY` de uma string maior que o `VARCHAR` aborta (`Spectrum Scan Error` 15007, 2026-09-21), e esta verificação é a barreira | As partições da execução. |
| Documento JSON inválido, ou acima de 65.535 bytes se a decisão da [etapa 8](PLAN-STAGE-8.md) fixar o teto | as colunas JSON, que nem o Arrow nem o Delta validam; o teto é o do `VARCHAR` da staging do Redshift e da string que o `COPY` de Parquet aceita numa coluna `SUPER` (2026-09-21) | As partições da execução. |
| Totais de controle | as colunas `Numeric` e `Double`; as `Double` somadas como `DECIMAL(38, 6)` de cada valor, porque a soma em ponto flutuante depende da ordem | As partições da execução. |

Uma chave primária não é por partição: conferi-la só nas partições da execução não é unicidade.
Quando as colunas da chave não incluem a coluna de partição, a verificação compara o sandbox com as
demais partições da versão fixada — `delta_scan(uri, version := v) WHERE data_str NOT IN (...)` no
DuckDB, a staging `_versao` de `pinned_delta`, com todas as colunas do contrato, carregada por
`COPY ... MANIFEST`, no Redshift (o `COPY` posicional não carrega só as colunas da chave). Custa
uma passagem nas colunas da chave da tabela inteira; `key_scope="partition"` a reduz às partições da
execução, e a escolha entra no relatório. Duas regras dispensam a passagem (decisão do usuário de
2026-09-23). A chave que inclui a coluna de `partition_source` fica nas partições da execução, porque a
verificação da derivação, na mesma auditoria, garante que a data de uma linha decide a sua partição:
`(data, sistema, contrato)` de `cad_contratos` e `(data, operacao)` de `cad_operacoes`. E a chave
primária inteira de uma coluna, que `next_ids` preenche acima do máximo da versão fixada, dispensa a
junção quando o menor valor das partições da execução passa do `max_key` dessa versão, lido das
estatísticas do log sem ler dados: a verificação leva a consulta desse mínimo em `skip_when`, que o
motor roda antes e que, verdadeira, aprova a verificação sem rodá-la, com o motivo no relatório. No
Redshift, a staging `_versao` só é carregada quando a junção roda, e o `cad_lancamentos` da
base de produção tem 141.901.795 linhas.

A chave estrangeira precisa da tabela referenciada, e só o que o pipeline usa é ingerido:
`foreign_keys=True` ingere a coluna referenciada das tabelas que faltarem no sandbox, na versão
fixada, e o anti-join roda contra ela. Sem o argumento, órfão nenhum é procurado, e o relatório
registra a verificação como não executada.

| Primitiva | O que faz |
| --- | --- |
| `checks(table, partitions=None, foreign_keys=False, key_scope=None, pinned=None, referenced=None, pinned_max_key=None)` | A lista de `Check` (nome, statement Core, o que reprova e, na chave primária inteira de uma coluna, o `skip_when` do mínimo contra `pinned_max_key`): os defeitos de linha num `count(CASE WHEN <defeito> THEN 1 END)` por coluna na mesma passagem, porque o Redshift não tem a cláusula `FILTER` nos agregados, uma consulta por chave e uma por chave estrangeira; `checks_and_not_run`, protegida, devolve também as que não rodam, com o motivo, e `sample_statement` a consulta da amostra de um contador. |
| `audit_sql(table, dialect, partitions=None, foreign_keys=False, key_scope=None, pinned=None, referenced=None, prefix="{prefix}")` | `{nome: texto}` por `sql.render` com o prefixo pedido, sem conexão e sem motor: o SQL que a auditoria vai rodar, para depuração. |
| `AuditReport` | Por verificação: nome, o SQL rodado, a contagem de defeitos, uma amostra das linhas reprovadas e o veredito; `passed` é a conjunção, e `report.sql()` devolve o texto de todas; `nonfinite_columns` dá, por valor de partição, as colunas `Double` com valor não finito, a lista que `run.publish_delta` passa a `export_partition` como `columns_without_min_max`; `totals` dá, por valor de partição, a linha da verificação de linhas (a contagem, os contadores, as somas de controle e os não finitos), e `rows(valor)` a contagem que `run.publish_delta` passa como `expected_rows`; `not_run` traz o motivo de cada verificação que não rodou, e `CheckResult.reason` o da dispensa pelo `skip_when`. |

| Primitiva | DuckDB |
| --- | --- |
| `connect(config)` | Banco em arquivo `<temp_directory>/<execution_id>.duckdb`, e em memória só com `DuckDBConfig(database=":memory:")` (decisão do usuário de 2026-09-22); `temp_directory` omitido é uma pasta nova de `tempfile.mkdtemp`, apagada com o banco em `cleanup`, porque o padrão `.tmp` do DuckDB é relativo à pasta corrente; a abertura que falha, por uma extensão ausente ou pelo secret recusado, apaga como o `cleanup` o que o motor criou, a pasta de transbordo, o banco padrão `<execution_id>.duckdb` e a pasta de `tempfile.mkdtemp`, deixa a pasta de `temp_directory` informada e relança o erro; a conexão de `storage.duckdb_connect(<arquivo do banco>, config)` da [etapa 3](PLAN-STAGE-3.md), que resolve `extension_directory` (`DuckDBConfig.extension_directory`, senão `SERIALIZE_DB_DUCKDB_EXTENSIONS`, senão `.duckdb/` ao lado do ambiente virtual), desliga `autoinstall_known_extensions` e `autoload_known_extensions` e aplica `duckdb_setup`; `temp_directory`, `preserve_insertion_order = false` e os limites lidos do ambiente na abertura, quando a configuração os omite (`environment_limits`): `threads` são as CPUs que o processo pode usar e `memory_limit` é metade da memória que ele ainda pode usar, por `serialize_db.resources` (instrução do usuário de 2026-09-24); o log registra na abertura o `memory_limit` e as `threads` aplicados e o espaço livre de `temp_directory`; uma conexão só, a sessão da execução, e um `threading.RLock` que todo comando toma pelo tempo do comando (decisão do usuário de 2026-09-22, a mesma do motor Redshift); `session()` dá a conexão crua ao cliente com o lock tomado pelo bloco. |
| `new_session()` | Uma sessão a mais sobre o mesmo banco: um motor sobre `cursor()` da conexão, com o seu `RLock`, as mesmas primitivas e a mesma pasta de transbordo, gerenciador de contexto. Ele vê o que a sessão principal confirmou e não as tabelas temporárias dela; o fim do `with` e o `cleanup` dele fecham só essa conexão. `run.ingest` abre uma por tabela, e o cliente a usa para o que roda em paralelo ([`PLAN.md`](PLAN.md), seção "Regras que as etapas obedecem"). |
| `ingest(table, uri, version, partitions=None, materialize=False)` | View com o nome do modelo sobre `delta_scan(uri, version := v)`, ou, com `materialize=True`, a tabela criada por `ddl(table, "duckdb")` e carregada por `INSERT INTO <nome> BY NAME SELECT * FROM delta_scan(...)` numa transação, com os tipos do contrato e o `NOT NULL` (decisão do usuário de 2026-09-28; um JSON malformado no Delta falha o `ingest` e deixa o nome livre; o `INSERT` custou o mesmo que o `CREATE TABLE AS`, [`POC.md`](POC.md)); numa transação que o cliente abriu em `session()`, o `BEGIN TRANSACTION` do `ingest` materializado é recusado, porque o DuckDB não aninha transações (`cannot start a transaction within a transaction`), e o `ROLLBACK` da recusa encerra a transação do cliente, cujo `COMMIT` seguinte falha com `cannot commit - no transaction is active` em vez de voltar sem erro e sem o trabalho dela (leitura de 2026-09-28); com `partitions`, as duas filtram por `<coluna> BETWEEN '<menor>' AND '<maior>' AND <coluna> IN (...)`, porque o `delta_scan` poda por `=` e por intervalo e abre todos os arquivos com um `IN` de mais de um valor (leitura de 2026-09-23). |
| `pinned_delta(table, uri, version)` | A versão fixada como origem de consulta, sem ocupar nome no sandbox: o `FromClause` com as colunas do contrato que compila para `delta_scan('<uri>', version := <v>)`. É por ele que o pipeline lê a versão fixada da tabela que ele mesmo grava, cujo nome no sandbox pertence à tabela de `create_table` que o `appender` preenche, e é ele que a auditoria usa como `pinned` nas chaves que não incluem a coluna de partição (decisão do usuário de 2026-09-22). Sem versão fixada, numa tabela que ainda não existe, levanta `SandboxError` nomeando a tabela. |
| `stream(statement_or_sql, params=None, batch_size=100_000)` | Um statement Core com as tabelas do contrato trocadas pelas do sandbox por `sql.prefixed(prefix="")`, com os valores do cliente dados por `statement.params(**params)` e compilado por `duckdb_engine.Dialect(paramstyle="qmark")`, o estilo do driver do DuckDB, sem `literal_binds` e com `render_postcompile=True`, que expande o `IN` de lista e o `bindparam(..., expanding=True)` (sem ele o texto sai com `__[POSTCOMPILE_...]`, que o DuckDB recusa, leitura de 2026-09-23); os nomes de `params` conferidos contra os `bindparam` sem valor do statement, porque `params` ignora um nome a mais; as constantes e os valores do cliente juntados por `construct_params()` e passados como lista na ordem de `positiontup`, sem reescrever marcador (decisão do usuário de 2026-09-23, leituras de 2026-09-22 e 2026-09-23, [`POC.md`](POC.md); `param` não é exigido); ou um texto pronto, gerado por `render` ou lido por `sql.read_sql(..., prefix="")`, com `:nome` em `$nome` por `sql.bind`; roda numa thread auxiliar, na sessão, sob o lock e sem `Session` do SQLAlchemy, que entrega cada lote de `to_arrow_reader(batch_size)` à memória enquanto os lotes guardados cabem no orçamento de 64 MiB, e a um arquivo Arrow IPC com LZ4 na pasta de transbordo o lote que não cabe e os seguintes, e solta o lock quando o resultado acaba, sem esperar pelo cliente (decisão do usuário de 2026-09-23); dentro de `session()`, na mesma thread, a consulta roda na thread de quem chama. O motor devolve o `BatchStream`: iterável de `pa.RecordBatch` com os tipos do motor (`decimal128(18, 2)`, `date32`, JSON como `string`), `schema`, `read_next_batch`, `read_all`, `close`, gerenciador de contexto e `__arrow_c_stream__` (para `write_deltalake` e `RecordBatchReader.from_stream`, nunca para o `register` do DuckDB). O cliente lê a memória e depois o arquivo, na sua thread e na ordem da consulta, enquanto ela continua, com esperas com prazo; a thread não referencia o stream; `close` cancela por `interrupt()` a consulta que ainda roda e apaga o arquivo; o erro anterior ao primeiro lote chega na construção, e o posterior na leitura seguinte ao último lote entregue. |
| `query(statement_or_sql, params=None)` | O statement Core ou o texto pronto, pelo caminho de compilação de `stream`, sob o lock, por `to_arrow_table()`: a `pa.Table` com os tipos do motor, igual a `stream(statement_or_sql, params).read_all()`, sem arquivo; um comando sem resultado devolve a tabela `Count` ou `Success` do DuckDB. `execute` saiu da interface (decisão do usuário de 2026-09-23). |
| `create_table(table)` | A tabela vazia do modelo, por `ddl(table, "duckdb")` num cursor à parte da conexão, sem o lock da sessão, para não esperar a consulta de um `stream` aberto antes; um nome já ocupado, pela view ou pela tabela do `ingest` ou por um `create_table` anterior, é recusado com `SandboxError` antes da DDL, por uma leitura do catálogo num cursor à parte (decisão do usuário de 2026-09-28). Uma transação que o cliente abriu em `session()` e que já leu ou mudou o banco só vê a tabela depois de terminar: dentro dela, o comando sobre a tabela levanta `duckdb.CatalogException`. |
| `appender(table, queue_depth=2)` | O gerenciador de contexto que acrescenta lotes a uma tabela do sandbox, vinda do `ingest` com `materialize=True` ou de `create_table`: na abertura, uma leitura do catálogo num cursor à parte, sem o lock, recusa com `SandboxError` a tabela que não existe e a view do `ingest`; `write(data)` aceita `pa.RecordBatch` ou `pa.Table`, faz `cast(batch, table)` na thread do cliente e põe o lote numa fila limitada; uma thread auxiliar grava os lotes num arquivo Arrow IPC com LZ4 na pasta de transbordo, sem a sessão, e o `close` roda, sob o lock, um único `INSERT INTO <nome> BY NAME SELECT * FROM <leitor do arquivo>`, só quando algum lote foi gravado; uma exceção dentro do `with`, um lote recusado pelo `cast`, ainda que o cliente pegue a recusa, ou um appender abandonado apagam o arquivo sem inserir linha, e um erro do `INSERT` não deixa linha; `close` relança o erro da thread, a recusa do `cast`, o mesmo objeto que o `write` levantou, e o erro do `INSERT`, e a segunda chamada de `close`, como a da saída do `with` depois de um `close` explícito, não faz nada; `rows` conta as linhas gravadas. O arquivo nasce com o esquema do primeiro lote convertido, e um lote com outro conjunto de colunas é `ContractError`. Nenhuma linha entra antes do `close`, e o leitor que o `INSERT` consome é o do arquivo, nativo: nenhum gerador Python é entregue ao DuckDB (decisão do usuário de 2026-09-28, no lugar do `loader` que criava a tabela no `close`). |
| `append(table, data)` | `data` é uma `pa.Table`, um `pa.RecordBatch`, um `RecordBatchReader` ou um iterável de lotes: `with appender(table) as a: for batch in ...: a.write(batch)`; devolve as linhas gravadas; outro tipo é recusado com a mensagem que aponta `pa.Table.from_pandas` e `pa.RecordBatch.from_pandas`. |
| `audit(table, partitions, uri=None, version=None, foreign_keys=False, key_scope=None, referenced=None)` | `uri` e `version` são os da tabela fixada, e montam `pinned` e o `pinned_max_key` de `delta.max_key` sem depender de `Execution`; `referenced` dá, por nome de tabela, a URI e a versão fixada da tabela referenciada que o sandbox não tem, e a que o sandbox tem entra pelo nome do modelo. Roda o texto de `sql.render(check.statement, "duckdb", metadata, prefix="")` de cada verificação — `json_valid` e `strftime(data, '%Y-%m-%d')` são as funções do dialeto — e monta o `AuditReport`, com até 20 linhas inteiras de amostra por verificação reprovada (decisão do usuário de 2026-09-22): a verificação `linhas` conta por coluna e não tem linha para amostrar, então cada contador acima de zero ganha uma segunda consulta, `SELECT * ... WHERE <condição da coluna> LIMIT 20`, rodada só na reprovação; a comparação com as demais partições sai de `pinned` na versão fixada, e `passed` falso interrompe a execução. |
| `export_partition(table, uri, value, metadata, expected_rows=None, columns_without_min_max=())` | `uri`, `metadata` e `expected_rows` vêm do chamador, porque o motor não conhece o `Database` e o commit precisa dos metadados. A partição entra no Delta pelo registro do arquivo que o motor gravou, o padrão que o usuário aprovou em 2026-09-24 para as etapas 4, 5 e 7; o `rewrite` saiu do motor DuckDB, com a flag `export_mode` (decisão do usuário do mesmo dia). `columns_without_min_max`, as colunas `Double` com valor não finito na partição, vem de `run.publish_delta` e vai a `register_files`. O registro: `COPY (SELECT <cada coluna do contrato em CAST para o tipo dele, sem a de partição> FROM <sandbox> WHERE <coluna de partição> = '<valor>' ORDER BY <sort_key>) TO '<uri>/<coluna de partição>=<valor>/<execution_id>_<uuid>.parquet' (FORMAT parquet, RETURN_STATS)`, com a contagem do sandbox no mesmo bloco da sessão, mais `register_files` com as estatísticas de `RETURN_STATS`, as conferências da [etapa 3](PLAN-STAGE-3.md) antes do commit e a releitura depois; a memória é constante (600 MB na reescrita medida em [`delta.md`](delta.md)). O `uuid` no nome impede uma reexecução com o mesmo `execution_id` de sobrescrever o arquivo que a versão anterior referencia. O `write_deltalake` de um leitor, o caminho que saiu, crescia com a partição (1.140 MB para 135 MB de Parquet na mesma medição). |
| `cleanup()` | Cancela por `interrupt()` o comando em curso, fecha a conexão e apaga o arquivo do banco, o seu `.wal` e a pasta de transbordo, e a pasta de `tempfile.mkdtemp`; o banco de um caminho que a configuração informou fica. Numa sessão a mais, fecha só o cursor; a segunda chamada não faz nada. |

Testes: `tests/test_audit.py`, sem gravar: o texto de cada verificação nos dois dialetos, a chave
lida do `primary_key` do modelo e o escopo escolhido pelas colunas da chave.
`tests/test_engine_duckdb.py` sob a raiz local: o pipeline de exemplo (seis partições
materializadas e a dimensão `cad_contas` em view, um `select` com `join` em lotes para o
`appender`, auditoria, exportação da partição pelo registro do arquivo do `COPY`) sobre um Delta
local criado no teste; o `NaN` em `nonfinite_columns` sem reprovar, e a exportação de uma partição
com `NaN` sem o mínimo e o máximo dessa coluna; o ciclo `query`,
`to_pandas(types_mapper=pd.ArrowDtype)`, `from_pandas` e
`append` com `decimal128(18, 2)` e `date32` mantidos, `append` recusando um DataFrame e o
`appender` recusando a view do `ingest` e a tabela que não existe, com a leitura da versão fixada
por `pinned_delta` no lugar; o ciclo por
lotes: `stream` que entrega o primeiro lote enquanto a consulta roda e deixa outros comandos
rodarem, `stream` sobre uma tabela temporária e dentro de `session()`, `stream` fechado no meio, `appender` que não insere nada na exceção do cliente e no lote
recusado pelo `cast`, `append` de um iterável e de uma `pa.Table` com o mesmo resultado, e `query`
igual a `stream(...).read_all()`;
auditoria que reprova a chave repetida dentro da partição e a repetida contra uma partição da
versão fixada, e o órfão de chave estrangeira com a tabela referenciada fora do sandbox; `query` de um
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
  DuckDB, e a consulta conta à parte os não finitos de cada coluna, os não nulos menos os finitos,
  `count(x) - count(CASE WHEN isfinite(x) THEN 1 END)`, que dão `nonfinite_columns`:
  o `CAST` de um `NaN` ou de um infinito para
  `DECIMAL` falha com `ConversionException` e derrubaria a verificação inteira, e um filtro do
  agregado não o evita (leitura de 2026-09-23). A contagem entra no relatório sem reprovar, porque
  o `cast` aceita o `Double` não finito, e dá a lista `columns_without_min_max` da
  [issue #59](https://github.com/felipenoris/serialize-db/issues/59) (decisões do usuário de
  2026-09-23). No Redshift, `is_finite` sai `(x > '-Infinity'::float8 AND x < 'Infinity'::float8)`.
  No ambiente alvo, em 2026-09-23, o `NaN` de uma constante saiu igual a si mesmo, como no
  PostgreSQL; o de uma varredura de tabela passou por `x NOT IN ('NaN'::float8, ...)` e chegou ao
  `CAST` da soma, e depois a comparação estrita o deixou fora da soma, mas a negação dela também
  não o contou ([`POC.md`](POC.md)). Por isso a condição só entra afirmada, na soma e na contagem
  dos finitos, e as execuções de 2026-09-24 às 01:46 e às 01:49 contaram 2 dos 2 não finitos,
  asserção desde então. As funções com `@compiles` por dialeto
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
  `key_scope != "partition"`, uma segunda consulta que junta o sandbox com `pinned` (o
  `delta_scan` da versão fixada, ou a staging no Redshift) fora das partições da execução. A chave
  que inclui a coluna de `partition_source` não ganha a segunda consulta, e a chave primária inteira
  de uma coluna a ganha com o `skip_when` `SELECT min(<chave>) > <pinned_max_key>` nas partições
  da execução, quando `pinned_max_key` vem informado. Uma
  consulta por chave estrangeira (anti-join contra `referenced[tabela]`) só com
  `foreign_keys=True`; sem ele, o nome entra em `not_run`.
- **`audit_sql`** renderiza cada `Check` por `sql.render` com o prefixo pedido; é o texto que
  `serialize-db audit --sql` imprime, para depuração, e nenhum arquivo o guarda (decisão do usuário
  de 2026-09-23).
- **`AuditReport`** guarda por verificação o SQL rodado, a contagem e uma amostra; `passed` é a
  conjunção; `sql()` concatena os textos para o log da execução.
- **`DuckDBEngine.__init__`** abre a conexão raiz por `storage.duckdb_connect(<arquivo do banco>,
  config)` da [etapa 3](PLAN-STAGE-3.md), com `temp_directory`, `preserve_insertion_order = false`,
  `threads` e `memory_limit` e, quando a configuração a informa, `extension_directory`; a etapa 3
  resolve a pasta de extensões, desliga a instalação e a carga automáticas e aplica `duckdb_setup`.
  O banco é um arquivo em `<temp_directory>/<execution_id>.duckdb` por padrão: no ambiente alvo a
  máquina tem 7,6 GiB de memória, 2 vCPUs e 29,8 GiB livres num só disco, e uma tabela materializada
  de doze partições de `cad_lancamentos` não cabe em memória, cabe em disco ([`POC.md`](POC.md),
  leitura de 2026-09-21). `temp_directory` omitido é uma pasta de `tempfile.mkdtemp`, e `cleanup`
  apaga a pasta com o banco dentro; a abertura que falha apaga o mesmo antes de relançar o erro,
  porque sem o motor construído ninguém chama o `cleanup`. Os limites da instância saem do
  ambiente na abertura, nunca de um valor fixo, porque a máquina muda de tamanho (instrução do
  usuário de 2026-09-24):
  `environment_limits()` dá `threads` igual às CPUs que o processo pode usar e `memory_limit` igual
  a metade da memória que ele ainda pode usar, em MiB, porque o DuckDB recusa porcentagem (`Unknown
  unit for memory: '%'`, leitura de 2026-09-22). Ela fica em `serialize_db.resources`, protegida,
  fora do `__all__` dele, porque as conexões do DuckDB de `serialize_db.delta` da
  [etapa 3](PLAN-STAGE-3.md) a chamam e `delta` não depende de `engine`, e
  `serialize_db.engine.duckdb` a publica. As leituras são de `serialize_db.resources`:
  `available_cpus()` é a afinidade do processo (`os.process_cpu_count`) limitada pela menor cota de
  CPU do cgroup, arredondada para cima; `available_memory()` é a menor entre a memória física, o
  `MemAvailable` de `/proc/meminfo` e a folga do cgroup, no v1 e no v2, pelo menor limite no
  caminho do cgroup até a raiz: o limite menos o uso, com o cache de arquivos de volta menos a
  memória compartilhada contada nele (`total_cache` e `total_shmem` no v1, `file` e `shmem` no
  v2), porque o tmpfs, o `/dev/shm` e o mmap compartilhado entram no cache, e o kernel sem swap não
  os devolve. A metade segue a
  documentação do DuckDB, que pede de 50% a 60% da memória quando o sistema mata o processo, porque
  parte das alocações foge do limite: no `COPY` ordenado, o RSS do processo passou do limite em 13%
  a 21%, e com o padrão do DuckDB, 80% da memória, o kernel matou a migração de `cad_lancamentos` no
  ambiente alvo (leituras de 2026-09-24, [`POC.md`](POC.md)); a outra metade fica para o PyArrow, o
  delta-rs e o pandas do cliente, que o limite do DuckDB não cobre. Um `threads` ou um
  `memory_limit` na configuração fica no lugar do lido. Os valores aplicados e o espaço livre de
  `temp_directory`, lido por `shutil.disk_usage`, vão para o log na abertura. `threads` é da
  instância, vale para a sessão principal e para as de `new_session()`, e muda em execução por `SET
  threads`. A leitura do S3 pede mais threads que núcleos, porque cada thread faz uma requisição
  HTTP por vez, e a materialização num banco em arquivo pede a CPU: no ambiente alvo, com 4 vCPUs,
  em 2026-09-23, `probes/duckdb_threads.py` leu a partição de 393 MB em 4,1 s com 4 threads e em 1,9
  s a 2,1 s com 8 a 20, e a materializou em 12,7 s com 4 threads e em 13,2 s a 18,0 s com mais; com
  16 vCPUs de 8 núcleos físicos e o cache de arquivos externos desligado, em 2026-09-24, leu a
  partição de 542 MB em 2,03 s com 8 threads, 1,19 s com 16 e 0,84 s a 0,89 s com 48 a 80, e a
  materializou em 7,8 s com 8, 7,0 s com 16 e 11,1 s a 17,1 s com 32 a 80, com o pico do processo de
  1.880 MB a 6.427 MB; com 8 vCPUs de duas threads por núcleo físico, em 2026-09-27, leu a partição
  de 2.331 MB em 7,7 s com 8 threads e em 4,0 s a 4,1 s com 24 a 40, e a materializou em 52,7 s com
  4, 37,2 s com 8, 34,2 s com 16 e 35,4 s a 37,8 s com 24 a 40, com o pico do processo de 3.341 MB a
  6.362 MB, e em 2026-09-29, pela DDL e o `INSERT ... BY NAME`, em 55,3 s com 4, 39,6 s com 8,
  35,3 s com 16 e 34,9 s a 36,3 s com 24 a 40 ([`POC.md`](POC.md)). O padrão são as CPUs que o
  processo pode usar, decidido pela execução de 2026-09-24: a ingestão materializa, e com 16 vCPUs
  a metade das CPUs e o dobro delas perderam nela; com 8 vCPUs, o dobro ganhou de 5% a 12% numa
  tabela, com o pico cerca de 1 GB maior, e perdeu de 5% a 8% com quatro tabelas em série. A
  leitura agregada do S3 ganha de 1,4 a 1,9 vez com o
  triplo, para quem a pedir em `DuckDBConfig.threads`. O cache de arquivos externos do DuckDB fica
  ligado, o
  padrão: uma segunda leitura do mesmo arquivo na execução não volta ao S3. A conexão é a sessão da
  execução, e `session()` toma o `threading.RLock` e a dá ao bloco; toda primitiva toma o mesmo lock
  pelo tempo do seu comando, e nenhuma espera pelo código do cliente com ele tomado. O motor guarda
  a thread que está dentro de `session()`, e é por ela que `stream` sabe quando roda a consulta na
  thread de quem chama. `new_session()` devolve um motor sobre `cursor()` da conexão, com o seu lock
  e a mesma pasta de transbordo; o `cleanup` dele fecha só o cursor. O cursor nasce sem o lock da
  sessão principal: `cursor()` voltou em 0,04 ms com uma ordenação em curso na conexão (leitura de
  2026-09-23), e a sessão a mais pedida durante um comando longo não espera por ele.
  No S3, o motor segura a credencial de `aws_credentials()` da [etapa 3](PLAN-STAGE-3.md),
  resolvida antes da pasta e do banco, e a entrada de `session()` que toma o lock chama
  `renew_duckdb_secret`, que recria o secret quando a chave da credencial trocou (decisão do
  usuário de 2026-09-25); a entrada reentrante não o toca, porque o bloco pode ter uma transação
  aberta. As sessões de `new_session()` dividem a credencial e um lock do secret: sem o lock
  comum, oito cursores do mesmo banco recriando o secret deram `Catalog write-write conflict on
  alter with "serialize_db_s3"` em 1.249 de 1.600 tentativas (sonda de 2026-09-25,
  [`POC.md`](POC.md)). A entrada que falha na thread auxiliar do `stream` marca o fim com o erro,
  que a construção levanta. Um comando que dure mais que a chave do secret depois da entrada ainda
  falha com a chave vencida.
  Os arquivos intermediários de `stream` e de `appender` ficam na pasta de transbordo e saem no
  `close`.
- **`ingest`** cria `VIEW <nome do modelo> AS SELECT * FROM delta_scan('<uri>', version := <v>)`,
  ou, com `materialize=True`, a tabela por `ddl(table, "duckdb")` seguida de `INSERT INTO <nome> BY
  NAME SELECT * FROM delta_scan(...)`, numa transação, na sessão: a tabela ingerida tem os tipos do
  contrato e o `NOT NULL`, um JSON malformado no Delta falha o `ingest` e deixa o nome livre, e a
  view fica com os tipos do `delta_scan` (JSON em `VARCHAR`); o `INSERT` custou o mesmo que o
  `CREATE TABLE AS` que o precedeu, 2,919 s contra 2,737 s em 4.000.000 de linhas com 2 threads e
  2,565 s contra 2,499 s com 4, com o mesmo pico de memória (decisão do usuário de 2026-09-28,
  [`POC.md`](POC.md)). Com `partitions`, o filtro é `<coluna> BETWEEN
  '<menor>' AND '<maior>' AND <coluna> IN (...)`: numa tabela de 12 partições, o `IN` de dois
  valores e o `OR` abriram os 12 arquivos, e o intervalo abriu só os seus (log `FileSystem` do
  DuckDB, 2026-09-23, [`POC.md`](POC.md)). As `n` partições de `previous_partitions` são contíguas,
  e o intervalo é exato; numa lista salteada, ele limita a leitura e o `IN` filtra as linhas. O
  `EXPLAIN ANALYZE` dessa forma falha com `InternalException` na extensão `delta`, e o teste lê a
  poda pelo log.
- **`pinned_delta`** devolve o `FromClause` que compila para `delta_scan('<uri>', version := <v>)`,
  com as colunas do contrato, e não cria objeto no sandbox: é a origem que a auditoria já precisa
  nas chaves fora da partição, e a que o pipeline usa para ler a tabela cujo nome no sandbox é a
  tabela de `create_table` que o `appender` preenche. No Redshift ele é a staging da
  [etapa 5](PLAN-STAGE-5.md).
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
- **`create_table`** roda `ddl(table, "duckdb")` num cursor à parte da conexão, sem o lock da
  sessão, depois de recusar o nome ocupado: `duckdb_tables()` e `duckdb_views()` dizem o que
  existe, e um nome tomado levanta `SandboxError`. O `CREATE TABLE IF NOT EXISTS` não serve de
  guarda: sobre uma view ele passa em silêncio e o `INSERT ... BY NAME` seguinte morre com
  `Catalog Error: <nome> is not an table`, e sobre uma tabela de outro formato ele também passa,
  deixando o `INSERT ... BY NAME` preencher com nulo a coluna que sobra (leituras de 2026-09-22). O
  cursor não vê as tabelas temporárias da sessão principal. A DDL fora do lock existe porque um
  comando sob o lock, depois de um `stream` aberto, como no exemplo mensal de [`PLAN.md`](PLAN.md),
  esperaria a consulta inteira do stream antes do primeiro lote, 0,811 s contra 0,006 s em
  20.000.000 de linhas (leitura de 2026-09-23, [`POC.md`](POC.md)); a sonda de 2026-09-28 leu o
  `CREATE TABLE` num cursor em 0,002 s durante um `stream` que na sessão o fazia esperar 1,6 s a
  1,9 s ([`POC.md`](POC.md)). O DuckDB 1.5.5 fixa o retrato de uma transação no primeiro comando
  dela que lê ou muda o banco, e não no `BEGIN`: a transação que o cliente abriu em `session()` e
  que já leu ou mudou o banco não vê a tabela que o cursor à parte cria depois, até terminar, e a
  que ainda não leu nem mudou o banco a vê (leitura de 2026-09-28).
- **`appender`** devolve um `DuckDBAppender`, que na abertura lê o catálogo num cursor à parte,
  sem o lock, e recusa com `SandboxError` a tabela que não existe, apontando `create_table` e o
  `ingest` com `materialize=True`, e a view do `ingest`, que não recebe lotes. Depois da guarda,
  `write` faz `cast(batch, table)` na thread do cliente e enfileira, e a thread auxiliar grava os
  lotes num arquivo Arrow IPC com LZ4, sem a sessão. O `close` registra o leitor do arquivo com um
  nome único, que não é o de uma tabela do modelo, porque um leitor registrado ocupa um nome de view
  (leitura de 2026-09-22), e roda, sob o lock, um único `INSERT INTO <nome> BY NAME SELECT * FROM
  <leitor>`, só quando algum lote foi gravado: um erro do `INSERT` não deixa linha, e qualquer erro
  apaga o arquivo. O erro da thread sobe no `write` seguinte ou em `close`; a recusa do `cast` sobe
  no `write` do lote e de novo em `close`, ainda que o cliente a tenha pego, e nenhum lote entra,
  nem os aceitos antes e depois dela. O `close` explícito dentro do `with` insere os lotes, e a
  segunda chamada, a da saída do `with`, não faz nada, nos dois motores, como diz o `close` do
  protocolo `Appender`. A tabela existe antes do `close`, então
  uma leitura durante um `append` em curso vê a tabela sem as linhas novas, na sessão principal e
  numa sessão a mais (`test_engine_duckdb.py`,
  `test_read_during_an_append_in_flight_sees_the_table_without_the_new_rows`), o efeito que o
  usuário aceitou em 2026-09-28 no lugar da falha com `CatalogException` que o `loader` criando a
  tabela no `close` dava (decisão de 2026-09-23, substituída); a barreira por tabela que esperaria
  a carga segue fora das etapas. `append` embrulha `appender` para `pa.Table`, `RecordBatch`,
  `RecordBatchReader` e iteráveis, devolve as linhas gravadas e recusa o resto com a mensagem que
  aponta `pa.Table.from_pandas`.
- **`audit`** roda `audit_sql(table, "duckdb", pinned=pinned_delta(table, uri, version), ...)` e
  monta o `AuditReport`; `passed` falso não levanta aqui, levanta em `Execution.audit`. Cada
  verificação reprovada leva até 20 linhas inteiras de amostra: as verificações de chave e de
  órfão já devolvem as linhas, e a de `linhas`, que conta por coluna, ganha uma segunda consulta por
  contador acima de zero, `SELECT * ... WHERE <condição da coluna> LIMIT 20`, rodada só quando esse
  contador reprova. A amostra vai para o log, com o relatório; `_serialize_db/` não a recebe.
  `pinned_max_key` vem de `delta.max_key` sobre `delta.open_table(uri, storage, version)` quando
  a chave primária é inteira, de uma coluna e fora do escopo da partição; o `skip_when` de uma
  verificação roda antes dela e, verdadeiro, a aprova sem rodá-la, com o motivo no relatório.
- **`export_partition`**: `COPY (SELECT <colunas do contrato na ordem dele,
  sem a de partição> FROM <sandbox> WHERE <coluna> = '<valor>' ORDER BY <sort_key>) TO
  '<uri>/<coluna>=<valor>/<execution_id>_<uuid>.parquet' (FORMAT parquet, RETURN_STATS)`, com a pasta
  da partição criada antes por `storage.ensure_folder` na raiz local, porque o `COPY` para um arquivo
  não cria a pasta; a linha do `RETURN_STATS` vira um `RegisteredFile` por
  `delta.file_from_return_stats` (contagem, tamanho, `null_count` de todas as colunas, `min` e
  `max` dos tipos que transcrevem exato), e `delta.register_files` faz as conferências, o commit e a
  releitura, com `expected_rows` do `count(*)` do sandbox. A coluna de partição fora do
  arquivo e a ordem das colunas são conferências de `register_files` ([etapa 3](PLAN-STAGE-3.md)):
  o `COPY` do Redshift, que lista as colunas do rodapé, mandaria a coluna de partição para a
  staging, que não a tem, e a ordem, que nenhum leitor exige desde essa lista, segue conferida.
- **`cleanup`** chama `interrupt()` na conexão antes de tomar o lock, porque a execução acabou e o
  comando em curso, de um stream que ninguém lê, é cancelado em vez de esperado (2 ms com uma
  ordenação em curso, decisão do usuário de 2026-09-23); depois fecha a conexão e apaga o arquivo do
  banco e a pasta de transbordo.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `checks`, `audit_sql` | Modelo aprovado por `check_models`; `pinned` informado quando alguma chave não inclui a coluna de partição. | Um `Check` por consulta, com o texto renderizável nos dois dialetos; sem conexão. |
| `DuckDBEngine` | Extensões na pasta configurada; no S3, credencial na cadeia do `boto3`. | Uma conexão sobre o arquivo do banco, a sessão, sob um `RLock`; nenhum download; o `memory_limit` e as `threads` lidos do ambiente, ou os informados, e o espaço livre de `temp_directory` no log; na abertura que falha, nada do que o motor criou fica. |
| `ingest` | Tabela Delta legível na versão pedida; com `materialize=True`, nenhuma transação do cliente aberta em `session()`. | Uma view ou tabela com o nome do modelo, presa à versão; a tabela atual pode avançar sem afetar a leitura. |
| `pinned_delta` | Tabela Delta legível na versão fixada. | Um `FromClause` sobre essa versão; nenhum objeto no sandbox, e o nome do modelo livre para `create_table`. |
| `new_session` | O motor aberto. | Outra conexão ao mesmo banco, com o seu lock; o que a sessão principal confirmou visível, as tabelas temporárias dela não; o fim do `with` fecha só essa conexão. |
| `stream` | Texto ou statement válido; parâmetros que fecham com o dicionário. | Lotes na ordem da consulta, o primeiro antes do fim de uma consulta sem operador bloqueante; no máximo o orçamento de 64 MiB de lotes em memória; a sessão livre quando a consulta acaba, sem esperar o cliente; a consulta cancelada e o arquivo apagado em `close`; o erro anterior ao primeiro lote no construtor, o posterior na leitura seguinte ao último lote entregue, como `OSError` do leitor Arrow. Com mais de uma thread, o DuckDB 1.5.5 às vezes entrega `INTERRUPT Error: Interrupted!` no lugar do erro que outra thread da consulta achou, e a docstring do `stream` avisa o cliente (decisão do usuário de 2026-09-28, [`POC.md`](POC.md)). |
| `create_table` | O nome livre no sandbox. | A tabela vazia do modelo, pela DDL; `SandboxError` com o nome ocupado, sem criar nem alterar objeto algum. |
| `appender`, `append` | A tabela no sandbox, do `ingest` materializado ou de `create_table`; lotes que passam por `cast`. | Nenhuma linha antes do `close`, que insere num comando só; nenhuma linha na exceção, no lote recusado ou no erro do `INSERT`; `rows` igual às linhas escritas; `SandboxError` na tabela que não existe e na view do `ingest`, sem alterar objeto algum. |
| `audit` | Sandbox com a tabela; `uri` e `version` para as chaves fora da partição. | Um `AuditReport` com o SQL de cada verificação e até 20 linhas de amostra por verificação reprovada; nenhuma escrita. |
| `export_partition` | Auditoria aprovada (conferida por `Execution.publish_delta`); a pasta da partição gravável. | Uma versão nova no Delta com a partição substituída e as estatísticas registradas. |
| `cleanup` | Nenhum. | O comando em curso cancelado; arquivo do banco e pasta de transbordo apagados; chamadas seguintes falham. |

## Testes por caso

`tests/test_audit.py` sem gravar, sobre o modelo cliente e uma tabela com uma coluna de cada tipo
que muda de função entre os motores; `tests/test_engine_duckdb.py` sob a raiz local, sobre um
Delta criado no teste a partir de um modelo com a partição, a origem dela, uma chave estrangeira e
uma chave única; `tests/test_resources.py` sobre um `/proc` e um cgroup fabricados.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Texto das verificações | `test_audit_sql_per_dialect` | Cada `Check` do modelo cliente renderiza nos dois dialetos; `strftime` no DuckDB e `to_char` no Redshift; `json_valid` e `true`; `strlen` e `octet_length`; `isfinite` e a comparação estrita com os infinitos, que `test_redshift_is_finite_under_the_postgresql_rule` roda no DuckDB sobre o `NaN`, os infinitos, um número e o nulo; os não finitos como `count(x)` menos a contagem dos finitos, sem a condição negada. |
| Escopo da chave | `test_key_scope_follows_the_partition_column` | Chave com a coluna de partição ou a de `partition_source` gera uma consulta; sem elas, gera a segunda contra `pinned`; `key_scope="partition"` a suprime e o relatório registra; a chave primária inteira de uma coluna leva o `skip_when` do mínimo contra `pinned_max_key`. |
| Chave estrangeira | `test_foreign_key_check_only_on_request` | Sem `foreign_keys=True` o nome está em `not_run`; com ele, o anti-join contra `referenced`. |
| Defeitos plantados | `test_audit_finds_each_defect` | Nulo, texto acima do `String(n)` em bytes e dentro dele em caracteres, partição fora da origem, valor de partição fora da regra, JSON inválido numa tabela criada por SQL (a coluna `JSON` do DuckDB recusa o texto inválido na carga), chave repetida dentro da partição e contra a versão fixada. |
| Chave única na versão fixada | `test_audit_unique_key_against_the_pinned_version` | A chave única sem a coluna de partição nem a de origem, repetida contra a versão fixada. |
| Órfão | `test_audit_orphan_against_a_referenced_table_outside_the_sandbox` | Com `foreign_keys=True`, a tabela referenciada fora do sandbox entra pela versão fixada de `referenced` e o órfão aparece; sem o argumento, a verificação fica em `not_run`. |
| Regra da partição | `test_partition_values_follow_the_rule` | O valor fora da regra é recusado antes de qualquer texto. |
| Texto do `Uuid` | `test_rows_check_measures_uuid_text` | A verificação de linhas mede a coluna `Uuid` contra os 36 bytes do `VARCHAR(36)`, `strlen` no DuckDB e `octet_length` no Redshift sobre o `CAST` para texto; o texto do DuckDB conta um texto de 37 bytes entre o canônico e o nulo, e roda sobre a coluna `UUID` nativa de uma tabela criada por SQL, que o `strlen` sem o `CAST` recusa com `Binder Error` e a exportação converte em `VARCHAR(36)`. |
| Pasta temporária | `test_temporary_folder_is_created_and_removed` | Sem `temp_directory`, a pasta nova sai inteira no `cleanup`. |
| Abertura que falha | `test_failed_opening_removes_what_the_engine_created` | Com a extensão `delta` ausente da pasta de extensões, a abertura levanta `duckdb.IOException`; sem `temp_directory`, a pasta nova de `tempfile.mkdtemp` sai inteira; com `temp_directory`, o banco e a pasta de transbordo saem, e a pasta informada fica, vazia. |
| Sessão | `test_engine_config_and_single_session` | `duckdb_settings()` com os valores pedidos; sem eles, metade da memória disponível e a cota de CPU de um `/proc` e de um cgroup fabricados, e o `memory_limit` informado no lugar do lido; o banco em arquivo dentro da pasta de `tempfile.mkdtemp`; três threads usam a mesma sessão, uma de cada vez; a tabela temporária criada por uma é visível às outras; uma primitiva chamada dentro de `session()` não trava. |
| Memória compartilhada no cgroup | `tests/test_resources.py::test_shared_memory_stays_in_the_cgroup_usage` (`[v2]` e `[v1]`) | Com limite de 4 GiB, uso de 3 GiB, cache de arquivos de 1 GiB e 0,75 GiB de memória compartilhada contados nele, a folga é 4 - 3 + 1 - 0,75 GiB: `file` e `shmem` no v2, `total_cache` e `total_shmem` no v1, cujas chaves da pasta sozinha (`cache`, `shmem`) ficam de fora. |
| Sessão a mais | `test_new_session_runs_beside_the_main_one` | A sessão de `new_session` vê a tabela confirmada pela principal e não a temporária dela, roda enquanto a principal está num bloco `session()`, e a principal vê o que ela confirma; o fim do `with` fecha só o cursor, e o arquivo do banco continua. |
| Recriação do secret | `test_session_recreates_the_s3_secret_when_the_key_changes` (pulado sem a extensão `httpfs`) | Numa raiz S3 que o teste não lê, com uma credencial cuja chave o teste troca: nenhuma recriação com a chave da abertura; uma só entre quatro sessões de `new_session` que entram ao mesmo tempo; nenhuma na primitiva chamada dentro de `session()` e uma na entrada seguinte; e uma na entrada do `stream`, pela thread auxiliar. |
| Entrada que falha | `test_stream_raises_the_error_of_the_session_entry` (pulado sem a extensão `httpfs`) | A credencial vencida cujo endpoint não responde: `query` levanta o `CredentialRetrievalError`, e a construção do `stream` o levanta em vez de esperar o primeiro lote. |
| Chave vencida no bucket | `test_delta_scan_reads_after_the_secret_holds_a_stale_key` (`local` e `s3`) | Com uma chave que o S3 não conhece posta no secret dentro de um bloco, a entrada seguinte o recria com a chave da credencial que o motor segura, e a view sobre o `delta_scan` conta as linhas. |
| Ingestão presa | `test_ingest_pins_the_version` | Um `append` na tabela depois da abertura não aparece na view nem na tabela materializada. |
| Poda da ingestão | `test_ingest_opens_only_the_range_of_partitions` | A ingestão de partições contíguas e de uma lista salteada abre só os arquivos do intervalo, lidos pelo log `FileSystem` do DuckDB, e traz só as linhas pedidas. |
| Parâmetros | `test_statement_parameters_expand_in_lists` | Um statement com `IN` de lista, `NOT IN` e `bindparam(..., expanding=True)` roda por `query` e por `stream`; um nome de parâmetro a mais ou a menos é `SqlError` antes de rodar. |
| Versão fixada | `test_pinned_delta_reads_the_version_without_a_sandbox_name` | `pinned_delta` lê a versão fixada sem criar objeto no sandbox, e `create_table` da mesma tabela fica com o nome do modelo. |
| Stream | `test_stream_delivers_each_batch_while_the_query_runs` | O primeiro lote com a consulta ainda rodando, afirmado pela thread auxiliar viva nesse lote; as linhas conferidas pela contagem e pela soma, porque sem `ORDER BY` a ordem é a do DuckDB (`preserve_insertion_order = false`); nenhum lote no arquivo com o cliente acompanhando, um comando no meio da leitura que só roda depois do fim da consulta, a tabela temporária lida pelo stream, o stream dentro de `session()`, o abandono que para a consulta e apaga o arquivo, e o erro da consulta na construção ou na leitura seguinte ao último lote, da memória ou do arquivo; a consulta que falha no meio roda num motor de uma thread e sai como `OSError` do leitor Arrow (decisão do usuário de 2026-09-28). |
| Orçamento | `test_stream_spills_after_the_budget_and_keeps_the_order` | Com o cliente lento e um orçamento de dois lotes, a memória nunca passa do orçamento, o resto vai para o arquivo, as linhas saem todas e na ordem, e o `close` apaga o arquivo. |
| Cancelamento | `test_close_and_cleanup_interrupt_the_running_query` | O `close` depois do primeiro lote de uma varredura longa cancela a consulta, e a sessão continua usável; o `cleanup` cancela a ordenação de um stream que outra thread ainda constrói. |
| Contrato na ingestão | `test_materialized_ingest_applies_the_contract` | A tabela do `ingest` materializado tem os tipos e o `NOT NULL` da DDL (`JSON`, `DECIMAL(18,2)`, `DOUBLE NOT NULL`), e a view os do `delta_scan` (`VARCHAR`, nulável); uma partição com JSON malformado não entra na tabela, com `duckdb.Error`, e o nome fica livre; `partitions=[]` cria a tabela vazia. |
| Transação do cliente | `test_client_transaction_with_materialized_ingest_and_create_table` | Numa transação aberta em `session()`, o `ingest` com `materialize=True` levanta o `BEGIN` recusado (`TransactionException`), o `COMMIT` do cliente falha com `no transaction is active`, e nem a tabela do cliente nem a do `ingest` ficam; a tabela que `create_table` cria depois do primeiro comando da transação sobre o banco existe, a leitura dela na transação é `CatalogException`, que não desfaz a transação, e o `COMMIT` a deixa visível; a transação que ainda não leu nem mudou o banco vê a tabela criada depois do `BEGIN`. |
| Tabela vazia | `test_create_table_creates_the_empty_table_of_the_model` | `create_table` cria a tabela vazia com os tipos e o `NOT NULL` do contrato; o nome ocupado por ela, pela view ou pela tabela do `ingest` é `SandboxError`, e o objeto não muda. |
| Appender | `test_appender_inserts_in_one_statement_on_close` | A tabela não muda antes do `close`; exceção do cliente, lote recusado pelo `cast`, lote sem a coluna `NOT NULL` e erro do `INSERT` não deixam linha; `rows`; o appender abandonado sem `close` apaga o arquivo de transbordo e não insere; o appender sem lote não muda a tabela, e o seguinte acrescenta ao que está nela. |
| Segundo `close` | `test_appender_second_close_does_nothing` | O `close` explícito dentro do `with` insere os lotes, e o da saída do `with` não faz nada: as linhas entram uma vez, sem erro, e o arquivo de transbordo sai. |
| Recusas do `write` | `test_appender_write_refusals` | A recusa do `cast` que o cliente pega dentro do `with` sobe de novo no `close` como o mesmo objeto, e a tabela fica vazia, sem os lotes aceitos antes e depois dela; o lote sem a coluna `meta`, que o primeiro trouxe, é `ContractError`, e nada é inserido. |
| Ordem do exemplo mensal | `test_appender_and_create_table_after_a_stream_do_not_wait_for_its_query` | Com `stream` e depois `appender` no mesmo `with`, o primeiro lote chega com a consulta rodando, `create_table` de outra tabela dentro do `with` volta com a consulta ainda rodando, e a tabela tem todas as linhas no fim. |
| Pipeline de três estágios | `test_three_stage_pipeline_overlaps_read_work_and_write` | Leitura por `stream`, trabalho do cliente por lote e escrita por `appender` numa sessão única, sobre um banco em arquivo, dão as mesmas linhas que a versão por lote sem threads e que a versão por `pa.Table`; os tempos são leituras do relatório. |
| Leitura durante um append | `test_read_during_an_append_in_flight_sees_the_table_without_the_new_rows` | Um `append` disparado numa thread sem `result()`: antes do `close` do `appender`, a leitura da tabela vê as linhas anteriores e nenhuma das novas, na sessão principal e numa sessão a mais; depois do `close`, todas. |
| Tabela inexistente e view | `test_appender_refuses_the_view_and_the_missing_table` | O `appender` sobre a view do `ingest` e sobre uma tabela que não existe levanta `SandboxError` antes do primeiro lote, apontando `materialize=True` e `create_table`, e o objeto que estava lá não muda; a tabela do `ingest` materializado recebe `append`. |
| Formas por tabela | `test_query_and_append_match_stream_and_appender` | `query` de um statement e de um texto igual a `stream(...).read_all()`; `append` de `pa.Table`, `RecordBatch`, leitor e iterável com o mesmo resultado; DataFrame recusado com a mensagem. |
| Ciclo pandas | `test_pandas_round_trip_keeps_contract_types` | `to_pandas(types_mapper=pd.ArrowDtype)` e `from_pandas` mantêm `decimal128(18, 2)` e `date32`. |
| Exportação | `test_export_partition_registers_the_copy_file` | A partição registrada tem as linhas e as somas do sandbox, sem o mínimo e o máximo da coluna com `NaN`; o Delta poda pela estatística da chave (`Scanning Files: 0/n`); o arquivo leva o `execution_id` no nome; `expected_rows` diferente recusa sem commit; o valor numa tabela sem partição recusa antes do `COPY`, sem arquivo na pasta da tabela. |
| Pipeline de exemplo | `test_example_pipeline_in_a_file_backed_database` | Seis partições materializadas, a dimensão `cad_contas` em view, um `select` com `join` em lotes do `stream` para o `appender` de uma tabela de `create_table`, auditoria com `foreign_keys=True`, exportação com as linhas e as colunas não finitas da auditoria; a partição exportada sem os lançamentos da conta que o `join` deixa de fora; o arquivo `.duckdb` apagado por `cleanup`. |
| Dispensa da junção | `test_audit_skips_the_pinned_join_above_max_key` | Com as chaves da execução acima do `max_key` da versão fixada, a junção com as demais partições não roda e o relatório diz por quê; com uma chave abaixo dele, a junção roda e acha a repetição. |
| Amostra | `test_audit_report_samples_failing_rows` | Até 20 linhas inteiras por verificação reprovada; a de `linhas` busca as suas numa segunda consulta por contador acima de zero, e uma verificação aprovada não traz amostra; o `NaN` entra em `nonfinite_columns` sem reprovar. |
| Texto com `%` | `test_query_keeps_percent_literals` | `LIKE 'A%'`, num statement e num texto pronto, chega ao DuckDB como está. |

## A implementação

Os módulos `serialize_db.audit`, `serialize_db.engine` (o protocolo `Engine`, `BatchStream`, e
`Appender`) e `serialize_db.engine.duckdb` (`DuckDBConfig`, `DuckDBEngine`, `environment_limits`,
que ele publica de `serialize_db.resources`, e o `DuckDBStream` e o `DuckDBAppender` dele), com
`SandboxError` em `serialize_db.errors`, e os casos de `tests/test_audit.py` e
`tests/test_engine_duckdb.py` substituem a interface e os rascunhos
executados em 2026-09-21: as assinaturas e as docstrings estão no código e na documentação do
`pdoc`. A sessão a mais de `new_session` é um `DuckDBEngine(..., parent=motor)`. O que a
implementação mostrou está em [`POC.md`](POC.md), seção
"O que a implementação da etapa 4 mostrou".

## Decisões pendentes

Nenhuma. A decisão do usuário de 2026-09-24 que tirou o `rewrite` de `export_partition`, com a flag
`export_mode`, está escrita na seção que a descreve. As decisões que o usuário tomou em 2026-09-23
sobre as propostas da revisão estão escritas nas seções que as descrevem: o estilo `qmark` no
caminho do statement Core; o escopo das chaves da auditoria pela coluna de `partition_source` e pelo
`skip_when` contra o `max_key` da versão fixada; a interface do motor, com `query(statement_or_sql,
params=None)` no lugar de `query` e `execute`, os argumentos nomeados no lugar de `**options` e
`checks` sem `prefix`; o `loader` que conferia o nome num cursor próprio e criava a tabela no
`close`, numa transação, trocado em 2026-09-28 por `create_table` e `appender`; o `stream`
híbrido, com os lotes em memória até 64 MiB e o arquivo com LZ4 depois; e o `interrupt()` no
`close` do stream e no `cleanup`. O motor implementa as três últimas, e
`tests/test_engine_duckdb.py` tem os casos de cada uma.

As quatro decisões que o usuário tomou em 2026-09-22 — o `loader` recusando com `SandboxError` um
nome já ocupado no sandbox, o `memory_limit` no padrão do DuckDB, o banco em arquivo com
`temp_directory` em pasta nova, e a amostra de até 20 linhas inteiras por verificação reprovada —
estão escritas, cada uma, na seção que a descreve; a instrução do usuário de 2026-09-24 trocou o
`memory_limit` no padrão do DuckDB pelos limites lidos do ambiente, e a decisão de 2026-09-28
trocou a recusa do `loader` pela de `create_table`, que recusa o nome ocupado, e pela do
`appender`, que recusa a tabela inexistente e a view do `ingest`. A recusa do `loader` trouxe duas
decisões do mesmo dia: `pinned_delta(table, uri, version)` no protocolo dos dois motores, por onde o
pipeline lê a versão fixada da tabela que ele grava, e `SandboxError` como a exceção da
etapa em `serialize_db.errors`.
