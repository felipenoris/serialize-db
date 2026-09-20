# Etapa 7: carga inicial

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

A migração dos Parquet atuais para o Delta, uma passagem por tabela e por partição, reexecutável, em
`serialize_db.load`.

## A base de origem

`probes/parquet_source.py` leu a base de desenvolvimento em 2026-09-20 ([`POC.md`](POC.md)): uma
pasta por tabela sob a raiz, 14 pastas, 205 arquivos, 3,76 GB e 187 milhões de linhas, mais
`schema.json` solto na raiz. As tabelas sem partição têm um único `chunk_0.parquet` na raiz da
tabela. As quatro particionadas usam Hive por `data_str=<AAAA-MM-DD>` (`cad_contratos`,
`cad_operacoes`, `rel_contrato_operacao`) e por `data_base_str=<AAAA-MM-DD>` (`cad_lancamentos`): o
valor é um fim de mês, vive só no caminho e é igual a `data`, ou a `data_base`, em toda linha da
partição; os meses não são contíguos (`2026-02-28`, `2026-03-31` e `2026-06-30`; `cad_lancamentos`
tem também `2026-01-31`). Cada partição tem até 36 arquivos `chunk_<n>.parquet` de até 1.000.000 de
linhas e um row group, numerados sem zeros à esquerda (`chunk_10` vem antes de `chunk_2` na ordem
alfabética). Os tipos são `int32`, `string`, `date32`, `double`, `bool` e `timestamp[ns]` gravado em
`INT96`, sem estatística de mínimo e máximo; todo arquivo de cada tabela tem o mesmo esquema, sem
`field_id`, com a chave `pandas` no rodapé, SNAPPY, sem dicionário, formato 1.0. `schema.json`, na
raiz, é o controle de esquema da biblioteca anterior no formato da reflexão do SQLAlchemy (colunas
com tipo, nulidade e chave primária, chaves estrangeiras, índices e restrições de unicidade): a
nulidade dos arquivos é a dele, e as chaves estrangeiras compostas do modelo de referência não
constam nele. `tests/source_db_projetado.py` reproduz essa estrutura em poucas linhas, com os dados
consistentes com o modelo e a cópia real de `schema.json`, e `tests/test_source_db_projetado.py` a
confere contra a seção 3 do relatório.

`cad_lancamentos` tem 2,83 GB em quatro partições, cerca de 35 milhões de linhas e 700 MB de Parquet
por partição. O `write_deltalake` de um `RecordBatchReader` cresceu com a entrada na medição da
reescrita (1.140 MB de RSS para 135 MB de Parquet, [`delta.md`](delta.md)), e o `COPY ... RETURN_STATS`
do DuckDB mais `create_write_transaction` ficou em 600 MB: a partição de `cad_lancamentos` vai pelo
segundo caminho, e a primeira carga de uma partição real mede os dois antes de fixar o padrão. A
auditoria de chave estrangeira não é barreira da carga: a base de desenvolvimento tem
`cad_lancamentos` de `data_base` 2026-01-31 sem `cad_contratos` dessa data e o contrato `desemb-999`
sem cadastro, inconsistências ignoradas por decisão de 2026-09-20, e o relatório registra os
órfãos.

| Primitiva | O que faz |
| --- | --- |
| `initial_load(db, table, source, partitions=None)` | `create_table`; descobre as partições da pasta da tabela (`<coluna>=<AAAA-MM-DD>/`, a mesma coluna e o mesmo valor do contrato, ou o arquivo único na raiz da tabela) e, para cada uma, o DuckDB lê `read_parquet('<partição>/*.parquet')`, a pasta inteira e nunca a ordem dos nomes, sem `hive_partitioning` (com ele o DuckDB converte `data_str` a `DATE`), confere que `partition_source` (`data`, ou `data_base`) é igual ao valor do caminho em toda linha e acrescenta a coluna de partição com esse valor. As conversões que a origem exige são aplicadas antes do `cast` e registradas no relatório: as chaves de `int32` a `int64`, sem perda, e o `timestamp` `INT96` de nanossegundos a microssegundos (`coerce_int96_timestamp_unit="us"` no PyArrow; o `TIMESTAMP` do DuckDB já trunca; a precisão perdida não importa, decisão de 2026-09-20). As colunas `double` entram como estão, e a nulidade é a do modelo: um nulo numa coluna `NOT NULL` é recusado pelo `cast`, com a coluna e a partição na mensagem. `cast` converte para o contrato e `publish_partition` grava. Uma carga interrompida recomeça da partição seguinte à última publicada; `partitions` filtra as partições encontradas. As tabelas fora do modelo (`alembic_version`, `meta_update_status`) e o que não é Parquet (`schema.json`) ficam fora da carga e entram no relatório. |
| `load_report(db, table, source)` | Contagem e somas por partição, na origem e no Delta: as colunas `double` somadas como `DECIMAL(38, 6)` de cada valor, porque a soma em ponto flutuante depende da ordem e os valores são os mesmos dos dois lados; as `Numeric`, quando existirem, como estão. A carga só termina quando coincidem. |
| `serialize-db load` | `--table`, `--source` e `--partitions`. |

`convert_to_deltalake` registra os arquivos no lugar, sem reescrever, só quando eles já têm os
tipos, a ordem de colunas e o layout Hive do contrato; a origem tem o layout (`data_str=<valor>/`)
e não os tipos (`INT96`, chaves em `int32`), e ele não é o caminho.
Depois da carga os leitores abrem o Delta, e as pastas de origem ficam como cópia até a primeira
publicação no Redshift. Testes: `tests/test_load.py` sobre a base fictícia de
`tests/source_db_projetado.py`, gravada sob a raiz local, incluindo a carga interrompida, as chaves
em `int64`, o `timestamp` truncado, o relatório de contagens e somas e as tabelas puladas. Provas
de conceito:
`test_deltalake.py::test_initial_load_from_parquet_folders` (o cast na consulta do DuckDB, o mês
por `overwrite` com predicado, a retomada pelos meses já presentes e o relatório de contagens e
somas), `test_pyarrow.py` (`test_hive_partitioned_dataset`,
`test_parquet_streaming_read_filters_and_pandas`) e
`test_duckdb.py::test_decimal_from_pandas_sample_versus_arrow_schema`.
