# Etapa 5: motor Redshift

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

`serialize_db.engine.redshift` implementa o mesmo protocolo. A conexão vem de `RedshiftConfig`:
senha (`host`, `port`, `database`, `user`, `password`) ou IAM (`cluster` ou `workgroup`), o
`iam_role` do `COPY` e do `UNLOAD`, e `schema`, com os padrões nas variáveis
`SERIALIZE_DB_REDSHIFT_*`. A senha é o caminho padrão, lida das variáveis ou do secret da conexão do
projeto no Secrets Manager, que tem endpoint VPC no laboratório; a autenticação por IAM e a Data API
precisam das APIs do Redshift, sem endpoint VPC no laboratório (`RS-14`), e ficam opcionais.

| Primitiva | Redshift |
| --- | --- |
| `connect(config)` | `redshift_connector.connect` com `timeout`; `search_path` no esquema; `cursor.paramstyle = "named"`; uma conexão por thread num `threading.local`, aberta no primeiro uso e fechada em `cleanup`, e `connection` devolve a da thread. |
| `ingest(table, uri, version, partitions=None, materialize=True)` | `copy_manifest` dos arquivos dessas partições, `COPY ... FORMAT AS PARQUET MANIFEST IAM_ROLE ...` numa staging sem a coluna de partição criada por `ddl`, e `INSERT INTO exec_<id>_<tabela> SELECT *, '<valor>'`; `JSON_PARSE` nas colunas `SUPER`. |
| `query(statement, **params)` | O statement compilado para o Redshift e executado na conexão crua; o resultado é uma `pa.Table` montada das tuplas do cursor com o esquema do statement (`from_pylist` de dicionários por nome), ou por `UNLOAD` acima de um limite de linhas. |
| `execute(sql, params)` | `{prefix}` vira `exec_<id>_`, o texto roda com o dicionário, e o resultado volta como `pa.Table`, vazia para um comando sem resultado. |
| `load(table, data)` | `cast(data, table)`, Parquet em `staging/<execution_id>/` por `pq.write_table` mais `COPY`; abaixo de um limite de linhas, um único `INSERT` multilinha montado de `to_pylist()`, numa ida ao servidor. |
| `audit(table, partitions, **opcoes)` | O mesmo texto compilado para o Redshift; as demais partições e a tabela referenciada entram em stagings só com as colunas da chave, por `COPY ... MANIFEST`. |
| `export_partition(table, value)` | `UNLOAD ('<select do contrato>') TO '<uri>/' PARTITION BY (<coluna de partição>) FORMAT PARQUET MANIFEST VERBOSE` mais `register_files` com as estatísticas do rodapé Parquet. |
| `cleanup()` | `DROP TABLE` de `exec_<id>_*` e da staging; os objetos de `staging/<execution_id>/` apagados. |

O identificador de execução entra no nome do sandbox normalizado para `[a-z0-9_]`, dentro dos
127 bytes de um identificador do Redshift. Testes: `tests/test_engine_redshift.py` compara o SQL
gerado (`COPY`, `INSERT ... SELECT`, `UNLOAD`, DDL da staging) com texto esperado, sem cluster; os
testes marcados `redshift` rodam a mesma sequência com uma amostra no esquema autorizado, depois do
`test_redshift.py` da [etapa 0](PLAN-STAGE-0.md). Provas de conceito: `test_redshift.py` (sessão e
`paramstyle` nomeado, DDL, `COPY ... MANIFEST`, lista de colunas e `FILLRECORD`, `VARCHAR`, `SUPER`,
`UNLOAD`), `test_sqlalchemy.py` (`test_redshift_dialect_compiles_dml`,
`test_sandbox_copy_of_table_and_schema_files_diff`) e `test_stdlib.py::test_execution_identifiers`
(o prefixo do sandbox).

Paralelismo: `test_redshift.py::test_parallel_copy_and_unload_on_two_connections`, dois `COPY` e dois
`UNLOAD` em tabelas distintas, uma conexão por thread, limitados pelas slots do WLM.
