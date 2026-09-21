# Etapa 4: `audit` e motor DuckDB

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.audit` monta as verificações a partir do contrato e não sabe qual motor as roda;
`serialize_db.engine` declara o protocolo `Engine`, que a execução também não conhece, e
`serialize_db.engine.duckdb` o implementa. O protocolo fixa os tipos da fronteira: `stream` devolve
um `BatchStream` de `pa.RecordBatch` e `loader` recebe lotes por `write`; `query` e `execute` devolvem
a `pa.Table` que `stream` montou, e `load` entrega ao `loader` os lotes de uma `pa.Table`, de um
`RecordBatch`, de um `RecordBatchReader` ou de um iterável ([`PLAN.md`](PLAN.md), seção "A troca de
dados com o código cliente"). Os esboços `BatchStream` e `Loader` de `test_parallel.py` são a
referência da implementação.

Chave primária, unicidade e chave estrangeira ficam fora do DDL dos dois sandboxes (`schema.md`), e
a auditoria é onde elas são aplicadas. Cada verificação sai do `Table`, sem declaração adicional no
modelo:

| Verificação | De onde sai | Escopo |
| --- | --- | --- |
| Nulo em coluna `NOT NULL` | `column.nullable` | As partições da execução. |
| Chave repetida | `table.primary_key` e os `UniqueConstraint`, mais o que `keys` acrescenta | As partições da execução quando as colunas da chave incluem a coluna de partição; a tabela inteira quando não incluem. |
| Órfão de chave estrangeira | `table.foreign_keys` | Só com `foreign_keys=True`; a tabela referenciada entra na versão fixada pela execução. |
| Partição fora da data | `partition_by` e `partition_source` de `table_options`: a coluna de partição diferente de `strftime(<coluna de data>, '%Y-%m-%d')` | As partições da execução. |
| Texto acima de `String(n)` e valor fora do `Numeric(18, 2)` | os tipos de `schema.md` | As partições da execução. |
| Documento JSON inválido | as colunas JSON, que nem o Arrow nem o Delta validam | As partições da execução. |
| Totais de controle | as colunas `Numeric` e `Double`; as `Double` somadas como `DECIMAL(38, 6)` de cada valor, porque a soma em ponto flutuante depende da ordem | As partições da execução. |

Uma chave primária não é por partição: conferi-la só nas partições da execução não é unicidade.
Quando as colunas da chave não incluem a coluna de partição, a verificação compara o sandbox com as
demais partições da versão fixada — `delta_scan(uri, version := v) WHERE data_str NOT IN (...)` no DuckDB, uma
staging só com as colunas da chave, carregada por `COPY ... MANIFEST`, no Redshift. Custa uma
passagem nas colunas da chave da tabela inteira; `key_scope="partition"` a reduz às partições da execução,
e a escolha entra no relatório.

A chave estrangeira precisa da tabela referenciada, e só o que o pipeline usa é ingerido:
`foreign_keys=True` ingere a coluna referenciada das tabelas que faltarem no sandbox, na versão
fixada, e o anti-join roda contra ela. Sem o argumento, órfão nenhum é procurado, e o relatório
registra a verificação como não executada.

| Primitiva | O que faz |
| --- | --- |
| `checks(table, partitions=None, foreign_keys=False, key_scope=None)` | A lista de `Check` (nome, statement Core, o que reprova): os defeitos de linha num `count(*) FILTER` por coluna na mesma passagem, uma consulta por chave e uma por chave estrangeira. |
| `audit_sql(table, dialect, **opcoes)` | `{nome: texto}` por `sql.render`, sem conexão e sem motor: o SQL que a auditoria vai rodar, para depuração. |
| `audit_files(metadata, **opcoes)` e `write_audit_files(metadata, directory, **opcoes)` | `{"<tabela>.audit.duckdb.sql": ..., "<tabela>.audit.redshift.sql": ...}` em memória e gravados, como os arquivos de `schema` e de `sql`; versionar a pasta é opcional e faz um modelo alterado aparecer no diff. |
| `AuditReport` | Por verificação: nome, o SQL rodado, a contagem de defeitos, uma amostra das linhas reprovadas e o veredito; `passed` é a conjunção, e `report.sql()` devolve o texto de todas. |

| Primitiva | DuckDB |
| --- | --- |
| `connect(config)` | Banco em arquivo `<pasta temporária>/<execution_id>.duckdb` ou em memória; `extension_directory` de `SERIALIZE_DB_DUCKDB_EXTENSIONS` (ou `.duckdb/` da pasta preparada), `autoinstall_known_extensions` e `autoload_known_extensions` desligados; `storage.duckdb_setup`; `threads`, `memory_limit`, `temp_directory` e `preserve_insertion_order = false`; `temp_directory` sempre explícito, com o espaço livre conferido e registrado no log, porque o padrão `.tmp` é relativo à pasta corrente; uma conexão por thread, o `cursor()` da raiz guardado num `threading.local` no primeiro uso e fechado em `cleanup`, e `connection` devolve a da thread. |
| `ingest(table, uri, version, partitions=None, materialize=False)` | View com o nome do modelo sobre `delta_scan(uri, version := v)`, ou `CREATE TABLE ... AS SELECT ... FROM delta_scan(...) WHERE data_str IN (...)` com `materialize=True`. |
| `stream(statement_or_sql, params=None, batch_size=100_000, prefetch=2)` | Um statement Core compilado para o dialeto, com as tabelas do contrato trocadas pelas do sandbox por `sql.prefixed`, ou o texto gerado por `render` ou escrito pelo pipeline, com `{prefix}` vazio e `:nome` em `$nome` por `sql.bind`; roda num `cursor()` próprio, sem `Session`, e devolve o `BatchStream`: iterável de `pa.RecordBatch` com os tipos do motor (`decimal128(18, 2)`, `date32`, JSON como `string`), `schema`, `read_next_batch`, `read_all`, `close`, gerenciador de contexto e `__arrow_c_stream__` (para `write_deltalake` e `RecordBatchReader.from_stream`, nunca para o `register` do DuckDB). Uma thread auxiliar puxa `prefetch` lotes de `to_arrow_reader(batch_size)` para uma fila limitada, com esperas com prazo e sem referência ao stream; `prefetch=0` dispensa a thread; `close` a interrompe e fecha o cursor; o erro da consulta chega na construção ou na leitura seguinte. |
| `query(statement, **params)` | `stream(statement, params).read_all()`: a `pa.Table` com os tipos do motor. |
| `execute(sql, params)` | `stream(sql, params).read_all()`; um comando sem resultado devolve a tabela `Count` ou `Success` do DuckDB. |
| `loader(table, queue_depth=2)` | O gerenciador de contexto que grava lotes numa tabela do sandbox criada por `ddl(table, "duckdb")`: `write(data)` aceita `pa.RecordBatch` ou `pa.Table`, faz `cast(batch, table)` na thread do cliente e põe o lote numa fila limitada; uma thread auxiliar, num `cursor()` próprio e numa transação explícita, registra cada lote e roda `INSERT ... BY NAME SELECT * FROM <lote>`, `commit` no `close` e `rollback` em qualquer erro, inclusive uma exceção dentro do `with` ou um loader abandonado; `close` relança o erro da thread; `rows` conta as linhas gravadas. Nada é visível antes do `commit`, e nenhum gerador Python é entregue ao DuckDB. |
| `load(table, data)` | `data` é uma `pa.Table`, um `pa.RecordBatch`, um `RecordBatchReader` ou um iterável de lotes: `with loader(table) as l: for batch in ...: l.write(batch)`; outro tipo é recusado com a mensagem que aponta `pa.Table.from_pandas` e `pa.RecordBatch.from_pandas`. |
| `audit(table, partitions, **opcoes)` | Roda o texto de `audit.audit_sql(table, "duckdb")` — `json_valid` e `strftime(data, '%Y-%m-%d')` são as funções do dialeto — e monta o `AuditReport`; a comparação com as demais partições sai de `delta_scan` na versão fixada, e `passed` falso interrompe a execução. |
| `export_partition(table, value, mode=None)` | `mode` é a flag `export_mode` da [etapa 6](PLAN-STAGE-6.md) (`register` ou `rewrite`, `SERIALIZE_DB_EXPORT_MODE` por omissão, `register` sem ela), a mesma do motor Redshift ([etapa 5](PLAN-STAGE-5.md)) e da carga inicial ([etapa 7](PLAN-STAGE-7.md)). **`register`**: `COPY (SELECT <colunas do contrato, sem a de partição> FROM <sandbox> WHERE <coluna de partição> = '<valor>') TO '<uri>/<coluna de partição>=<valor>/<execution_id>_<uuid>.parquet' (FORMAT parquet, RETURN_STATS)` mais `register_files` com as estatísticas de `RETURN_STATS`, as conferências da [etapa 3](PLAN-STAGE-3.md) antes do commit e a releitura depois; a memória é constante (600 MB na reescrita medida em [`delta.md`](delta.md)). O `uuid` no nome impede uma reexecução com o mesmo `execution_id` de sobrescrever o arquivo que a versão anterior referencia. **`rewrite`**: o `RecordBatchReader` da partição, passado por `cast`, para `publish_partition`, num cursor próprio; o `write_deltalake` calcula estatística e nulidade, e a memória cresce com a partição (1.140 MB para 135 MB de Parquet na mesma medição). A mesma partição sai igual pelos dois, e o teste os compara. |
| `cleanup()` | Fecha a conexão e apaga o arquivo do banco e a pasta de transbordo. |

Testes: `tests/test_audit.py`, sem gravar: o texto de cada verificação nos dois dialetos, o diff dos
arquivos gerados, a chave lida do `primary_key` do modelo e o escopo escolhido pelas colunas da
chave. `tests/test_engine_duckdb.py` sob a raiz local: o pipeline de exemplo (doze partições
materializadas e dimensões em view, um `select` com `join`, auditoria, exportação da partição nos dois modos com as mesmas linhas) sobre um
Delta local criado no teste; o ciclo `query`, `to_pandas(types_mapper=pd.ArrowDtype)`, `from_pandas` e
`load` com `decimal128(18, 2)` e `date32` mantidos, e `load` recusando um DataFrame; o ciclo por
lotes: `stream` com `prefetch` enquanto o cursor da thread roda outros comandos, `stream` fechado no
meio, `loader` que desfaz na exceção do cliente e no lote recusado pelo `cast`, `load` de um
iterável e de uma `pa.Table` com o mesmo resultado, e `query` igual a `stream(...).read_all()`;
auditoria que reprova a chave repetida dentro da partição e a repetida contra uma partição já
publicada, e o órfão de chave estrangeira com a tabela referenciada fora do sandbox; `execute` com
`%` em literal. Provas de conceito: `test_duckdb.py` (configuração, Arrow na entrada e na saída, o
leitor esvaziado pelo comando seguinte e preservado num cursor próprio, a consulta em streaming com
a memória medida em subprocesso, o `INSERT` sobre um leitor de gerador com a leitura antecipada e
os lotes que o leitor não confere, `DECIMAL`, JSON, `executemany`, `COPY` com `RETURN_STATS` e
particionado, banco em arquivo, `test_audit_queries`), `poc_delta.py`
(`delta_scan`, poda, tempos, `ATTACH ... PIN_SNAPSHOT`),
`test_deltalake.py::test_duckdb_view_pins_version_and_reader_feeds_write`
(a view presa a `version := v` e o `to_arrow_reader()` no `write_deltalake` com predicado),
`test_sqlalchemy.py::test_arrow_path_on_raw_connection`,
`test_stdlib.py::test_engine_protocol_and_config_dataclass` e `test_concurrency.py` (o GIL liberado
pelo DuckDB, pelo delta-rs e pelo PyArrow, um `cursor()` por thread, a conexão compartilhada que troca os
resultados das threads, o arquivo do DuckDB recusado com outra configuração, dois comandos em dois cursores,
o intervalo de troca do GIL pago por cada retomada ao lado de uma thread Python ocupada) e
`test_parallel.py` (o motor com um cursor por thread, os esboços `BatchStream` e `Loader` com o
pipeline de três estágios, a ingestão de quatro tabelas por `delta_scan` em paralelo, cargas Arrow e
`COPY ... TO` em paralelo) e `test_pyarrow.py::test_record_batch_cast_and_conversions_share_buffers`.
