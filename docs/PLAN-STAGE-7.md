# Etapa 7: carga inicial

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

A migração dos Parquet atuais para o Delta, uma passagem por tabela e por mês, reexecutável, em
`serialize_db.load`.

| Primitiva | O que faz |
| --- | --- |
| `initial_load(db, table, source, months=None)` | `create_table`; para cada mês, o DuckDB lê `source/mes=<mes>/*.parquet` (ou o layout que a origem tiver), `pc.round(x, 2)` leva os `Double` da origem à escala do contrato (os modelos atuais usam `Double` onde o contrato pede `Numeric(18, 2)`; o cast de `double` arredonda sem acusar, e a regra fica explícita e no relatório), `cast` converte para o contrato e `publish_month` grava em lotes. Uma carga interrompida recomeça do mês seguinte ao último publicado. |
| `load_report(db, table, source)` | Contagem e somas das colunas numéricas por mês, na origem e no Delta; a carga só termina quando coincidem. |
| `serialize-db load` | `--table`, `--source` e `--months`. |

`convert_to_deltalake` registra os arquivos no lugar, sem reescrever, só quando eles já têm os
tipos, a ordem de colunas e o layout Hive do contrato; não foi testado e não é o caminho padrão.
Depois da carga os leitores abrem o Delta, e as pastas de origem ficam como cópia até a primeira
publicação no Redshift. Testes: `tests/test_load.py` com Parquet gerados no teste sob a raiz local,
incluindo uma origem em `Double` e uma carga interrompida. Provas de conceito:
`test_deltalake.py::test_initial_load_from_parquet_folders` (o cast na consulta do DuckDB, o mês
por `overwrite` com predicado, a retomada pelos meses já presentes e o relatório de contagens e
somas), `test_pyarrow.py` (`test_hive_partitioned_dataset`,
`test_parquet_streaming_read_filters_and_pandas`) e
`test_duckdb.py::test_decimal_from_pandas_sample_versus_arrow_schema`.
