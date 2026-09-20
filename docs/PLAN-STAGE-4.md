# Etapa 4: `audit` e motor DuckDB

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.audit` monta as verificações a partir do contrato e não sabe qual motor as roda;
`serialize_db.engine` declara o protocolo `Engine`, que a execução também não conhece, e
`serialize_db.engine.duckdb` o implementa. O protocolo fixa os tipos da fronteira: `load` recebe uma
`pa.Table`, `query` e `execute` devolvem uma `pa.Table`, e os `RecordBatchReader` ficam nas
primitivas internas.

Chave primária, unicidade e chave estrangeira ficam fora do DDL dos dois sandboxes (`schema.md`), e
a auditoria é onde elas são aplicadas. Cada verificação sai do `Table`, sem declaração adicional no
modelo:

| Verificação | De onde sai | Escopo |
| --- | --- | --- |
| Nulo em coluna `NOT NULL` | `column.nullable` | Os meses da execução. |
| Chave repetida | `table.primary_key` e os `UniqueConstraint`, mais o que `keys` acrescenta | Os meses da execução quando as colunas da chave incluem a coluna de partição; a tabela inteira quando não incluem. |
| Órfão de chave estrangeira | `table.foreign_keys` | Só com `foreign_keys=True`; a tabela referenciada entra na versão fixada pela execução. |
| `mes` fora de `data_ref` | a coluna de partição de `table_options` | Os meses da execução. |
| Texto acima de `String(n)` e valor fora do `Numeric(18, 2)` | os tipos de `schema.md` | Os meses da execução. |
| Documento JSON inválido | as colunas JSON, que nem o Arrow nem o Delta validam | Os meses da execução. |
| Totais de controle | as colunas `Numeric` | Os meses da execução. |

Uma chave primária não é mensal: conferi-la só nos meses da execução não é unicidade. Quando as
colunas da chave não incluem a coluna de partição, a verificação compara o sandbox com os demais
meses da versão fixada — `delta_scan(uri, version := v) WHERE mes NOT IN (...)` no DuckDB, uma
staging só com as colunas da chave, carregada por `COPY ... MANIFEST`, no Redshift. Custa uma
passagem nas colunas da chave da tabela inteira; `key_scope="month"` a reduz aos meses da execução,
e a escolha entra no relatório.

A chave estrangeira precisa da tabela referenciada, e só o que o pipeline usa é ingerido:
`foreign_keys=True` ingere a coluna referenciada das tabelas que faltarem no sandbox, na versão
fixada, e o anti-join roda contra ela. Sem o argumento, órfão nenhum é procurado, e o relatório
registra a verificação como não executada.

| Primitiva | O que faz |
| --- | --- |
| `checks(table, months=None, foreign_keys=False, key_scope=None)` | A lista de `Check` (nome, statement Core, o que reprova): os defeitos de linha num `count(*) FILTER` por coluna na mesma passagem, uma consulta por chave e uma por chave estrangeira. |
| `audit_sql(table, dialect, **opcoes)` | `{nome: texto}` por `sql.render`, sem conexão e sem motor: o SQL que a auditoria vai rodar, para depuração. |
| `audit_files(metadata, **opcoes)` e `write_audit_files(metadata, directory, **opcoes)` | `{"<tabela>.audit.duckdb.sql": ..., "<tabela>.audit.redshift.sql": ...}` em memória e gravados, como os arquivos de `schema` e de `sql`; versionar a pasta é opcional e faz um modelo alterado aparecer no diff. |
| `AuditReport` | Por verificação: nome, o SQL rodado, a contagem de defeitos, uma amostra das linhas reprovadas e o veredito; `passed` é a conjunção, e `report.sql()` devolve o texto de todas. |

| Primitiva | DuckDB |
| --- | --- |
| `connect(config)` | Banco em arquivo `<pasta temporária>/<execution_id>.duckdb` ou em memória; `extension_directory` de `SERIALIZE_DB_DUCKDB_EXTENSIONS` (ou `.duckdb/` da pasta preparada), `autoinstall_known_extensions` e `autoload_known_extensions` desligados; `storage.duckdb_setup`; `threads`, `memory_limit`, `temp_directory` e `preserve_insertion_order = false`; `temp_directory` sempre explícito, com o espaço livre conferido e registrado no log, porque o padrão `.tmp` é relativo à pasta corrente; uma conexão por thread, o `cursor()` da raiz guardado num `threading.local` no primeiro uso e fechado em `cleanup`, e `connection` devolve a da thread. |
| `ingest(table, uri, version, months=None, materialize=False)` | View com o nome do modelo sobre `delta_scan(uri, version := v)`, ou `CREATE TABLE ... AS SELECT ... FROM delta_scan(...) WHERE mes IN (...)` com `materialize=True`. |
| `query(statement, **params)` | O statement Core compilado para o dialeto, com as tabelas do contrato trocadas pelas do sandbox por `sql.prefixed`, e executado na conexão crua, sem `Session`; o resultado é a `pa.Table` de `to_arrow_table()`, com os tipos do motor (`decimal128(18, 2)`, `date32`, JSON como `string`). |
| `execute(sql, params)` | O texto gerado por `render` ou escrito pelo pipeline: `{prefix}` vira vazio, `:nome` vira `$nome` por `sql.bind`, e o resultado volta como `pa.Table` por `to_arrow_table()`; um comando sem resultado devolve a tabela `Count` ou `Success` do DuckDB. |
| `load(table, data)` | `cast(data, table)` e `INSERT ... BY NAME SELECT * FROM <tabela Arrow registrada>` (o registro vale só na conexão da thread, e sai na mesma chamada) numa tabela do sandbox criada por `ddl(table, "duckdb")`; `data` é uma `pa.Table`, e outro tipo é recusado com a mensagem que aponta `pa.Table.from_pandas`. |
| `audit(table, months, **opcoes)` | Roda o texto de `audit.audit_sql(table, "duckdb")` — `json_valid` e `strftime(data_ref, '%Y-%m')` são as funções do dialeto — e monta o `AuditReport`; a comparação com os demais meses sai de `delta_scan` na versão fixada, e `passed` falso interrompe a execução. |
| `export_month(table, month)` | O `RecordBatchReader` do mês, passado por `cast`, para `publish_month`, sem outro comando na conexão até o fim da escrita; ou `COPY ... TO '<uri>/mes=<mes>/<execution_id>.parquet' (FORMAT parquet, RETURN_STATS)` mais `register_files` para a tabela que não cabe na memória. |
| `cleanup()` | Fecha a conexão e apaga o arquivo do banco e a pasta de transbordo. |

Testes: `tests/test_audit.py`, sem gravar: o texto de cada verificação nos dois dialetos, o diff dos
arquivos gerados, a chave lida do `primary_key` do modelo e o escopo escolhido pelas colunas da
chave. `tests/test_engine_duckdb.py` sob a raiz local: o pipeline de exemplo (doze meses
materializados e dimensões em view, um `select` com `join`, auditoria, exportação do mês) sobre um
Delta local criado no teste; o ciclo `query`, `to_pandas(types_mapper=pd.ArrowDtype)`, `from_pandas` e
`load` com `decimal128(18, 2)` e `date32` mantidos, e `load` recusando um DataFrame; auditoria que
reprova a chave repetida dentro do mês e a repetida
contra um mês já publicado, e o órfão de chave estrangeira com a tabela referenciada fora do
sandbox; `execute` com `%` em literal. Provas de conceito: `test_duckdb.py` (configuração, Arrow na
entrada e na saída, o leitor esvaziado pelo comando seguinte, `DECIMAL`, JSON, `executemany`, `COPY`
com `RETURN_STATS` e particionado, banco em arquivo, `test_audit_queries`), `poc_delta.py`
(`delta_scan`, poda, tempos, `ATTACH ... PIN_SNAPSHOT`),
`test_deltalake.py::test_duckdb_view_pins_version_and_reader_feeds_write`
(a view presa a `version := v` e o `to_arrow_reader()` no `write_deltalake` com predicado),
`test_sqlalchemy.py::test_arrow_path_on_raw_connection`,
`test_stdlib.py::test_engine_protocol_and_config_dataclass` e `test_concurrency.py` (o GIL liberado
pelo DuckDB, pelo delta-rs e pelo PyArrow, um `cursor()` por thread, a conexão compartilhada que troca os
resultados das threads, o arquivo do DuckDB recusado com outra configuração, dois comandos em dois cursores,
o intervalo de troca do GIL pago por cada retomada ao lado de uma thread Python ocupada) e
`test_parallel.py` (o motor com um cursor por thread, a ingestão de quatro tabelas por `delta_scan` em
paralelo, cargas Arrow e `COPY ... TO` em paralelo).
