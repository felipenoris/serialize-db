# Etapa 7: carga inicial

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

A migração dos Parquet atuais para o Delta, uma passagem por tabela e por partição, reexecutável, em
`serialize_db.load`, implementada em 2026-09-24 sobre a base fictícia, com `serialize-db load` e o
script de migração fino sobre o pacote.

## A base de origem

`probes/parquet_source.py` leu a base de desenvolvimento em 2026-09-20 e a de produção em
2026-09-21 ([`POC.md`](POC.md)), as duas com a mesma estrutura: uma pasta por tabela sob a raiz, 14
pastas, 205 arquivos, 3,76 GB e 187 milhões de linhas, mais `schema.json` solto na raiz. As tabelas sem partição têm um único `chunk_0.parquet` na raiz da
tabela. As quatro particionadas usam Hive por `data_str=<AAAA-MM-DD>` (`cad_contratos`,
`cad_operacoes`, `rel_contrato_operacao`) e por `data_base_str=<AAAA-MM-DD>` (`cad_lancamentos`): o
valor é um fim de mês, vive só no caminho e é igual a `data`, ou a `data_base`, em toda linha da
partição; os meses não são contíguos (`2026-02-28`, `2026-03-31` e `2026-06-30`; `cad_lancamentos`
tem também `2026-01-31`). Cada partição tem até 36 arquivos `chunk_<n>.parquet` de até 1.000.000 de
linhas e um row group, numerados sem zeros à esquerda (`chunk_10` vem antes de `chunk_2` na ordem
alfabética). Os tipos são `int32`, `string`, `date32`, `double`, `bool` e `timestamp[ns]` gravado em
`INT96`, sem estatística de mínimo e máximo; todo arquivo de cada tabela tem o mesmo esquema, sem
`field_id`, SNAPPY, sem dicionário, formato 1.0, com a chave `pandas` no rodapé de todo arquivo na
base de desenvolvimento e de parte deles na de produção. `schema.json`, na raiz, é o controle de esquema da biblioteca anterior no formato da reflexão do SQLAlchemy (colunas
com tipo, nulidade e chave primária, chaves estrangeiras, índices e restrições de unicidade): a
nulidade dos arquivos é a dele, e as chaves estrangeiras compostas do modelo de referência não
constam nele. `tests/source_db_projetado.py` reproduz essa estrutura em poucas linhas, com os dados
consistentes com o modelo e a cópia real de `schema.json`, e `tests/test_source_db_projetado.py` a
confere contra a seção 3 do relatório. As duas bases diferem nos dados, não na estrutura: a de
produção tem 113 linhas a menos (`cad_contas`, `rel_contas_hierarquias`, `cad_lancamentos`), ids
máximos menores (`id_lancamento` 952.517.158 em vez de 1.113.599.996) e cinco casas no extremo de
`valor`; a carga não depende de nenhuma dessas diferenças.

`cad_lancamentos` tem 2,83 GB em quatro partições, cerca de 35 milhões de linhas e 700 MB de Parquet
por partição. O `write_deltalake` de um `RecordBatchReader` cresceu com a entrada na medição da
reescrita (1.140 MB de RSS para 135 MB de Parquet, [`delta.md`](delta.md)), e o
`COPY ... RETURN_STATS` do DuckDB mais `create_write_transaction` ficou em 600 MB: a carga
registra o arquivo do `COPY`, o padrão que o usuário aprovou em 2026-09-24 para as etapas 4, 5 e 7
depois das partições medidas em 2026-09-23 ([`POC.md`](POC.md)), sem o `rewrite`, que saiu da
etapa com a flag `export_mode`, e ordena cada partição pela `sort_key` (decisões do usuário do
mesmo dia). A auditoria de chave estrangeira não é barreira da
carga: as duas bases têm `cad_lancamentos` de `data_base` 2026-01-31 sem `cad_contratos` dessa data
e o contrato `desemb-999` sem cadastro, inconsistências ignoradas por decisão de 2026-09-20, e o
relatório registra os órfãos.

| Primitiva | O que faz |
| --- | --- |
| `discover_partitions(source, table)` | As partições da pasta da tabela na raiz da origem: `{valor: URI da pasta}` das pastas `<coluna>=<valor>/` com a coluna de partição do modelo e um valor da regra da partição, em ordem de nome, ou `{None: URI da pasta}` numa tabela sem partição; o resto da pasta (`notas.txt`, uma pasta com valor fora da regra) na tupla `skipped`; a pasta da tabela ausente é `FileNotFoundError`. `entries_outside_the_model(source, metadata)` lista o que a raiz tem fora do modelo (`alembic_version`, `meta_update_status`, `schema.json`), e `load_order(tables)` põe as tabelas sem partição antes das particionadas, cada grupo na ordem dada. |
| `partition_query(folder, table, value)` | O `SELECT` do DuckDB que leva a partição ao contrato: `read_parquet('<pasta>/*.parquet', hive_partitioning = false)`, a pasta inteira e nunca a ordem dos nomes (com `hive_partitioning` o DuckDB converteria `data_str` a `DATE`), `CAST("<coluna>" AS <tipo>)` por coluna com o tipo de `sql_type(coluna, "duckdb")` (as chaves de `int32` a `BIGINT`, sem perda; o `timestamp` `INT96` a `TIMESTAMP`, truncado a microssegundos, decisão de 2026-09-20; as `Double` como estão) e o valor do caminho na coluna de partição; todo identificador entre aspas, porque `to` é palavra reservada. |
| `initial_load(db, table, source, partitions=None, config=None)` | `create_table`; as partições de `discover_partitions` menos as já no log (`partition_values`), filtradas por `partitions`, que deixa de fora uma tabela sem partição; para cada uma, num motor DuckDB da chamada (`DuckDBEngine` com `config`, ou a configuração padrão: os limites lidos do ambiente e a pasta temporária do sistema), aberto uma vez e fechado no fim, porque o DuckDB só devolve a memória ao fechar: uma consulta conta as linhas, as linhas com `partition_source` (`data`, `data_base`) diferente do valor do caminho, os nulos das colunas `NOT NULL`, os textos acima de `String(n)` em bytes (`strlen`) e os valores não finitos de cada coluna `Double`, e um problema é `ContractError` com a tabela, a partição e a coluna, antes de qualquer gravação; depois `COPY (SELECT <colunas sem a de partição> FROM (<consulta>) ORDER BY <sort_key>) TO '<uri>/<coluna>=<valor>/carga-<id>_<uuid>.parquet' (FORMAT parquet, RETURN_STATS)` e `register_files` com o `RegisteredFile` de `file_from_return_stats`, a contagem da conferência em `expected_rows` e as colunas não finitas em `columns_without_min_max` (decisão do usuário de 2026-09-23, [issue #59](https://github.com/felipenoris/serialize-db/issues/59)), com as conferências do rodapé e a releitura da [etapa 3](PLAN-STAGE-3.md). Os commits levam `serialize_db_execution_id` `carga-<id>`. Devolve os valores gravados; a segunda chamada devolve a lista vazia, e uma carga interrompida recomeça da partição seguinte à última registrada. Com a origem no S3 e a raiz Delta numa pasta local, a conexão do motor recebe as extensões e o secret da origem (`duckdb_setup` do `Storage` da origem). |
| `load_report(db, table, source, config=None)` | Contagem e somas por partição, na origem (`read_parquet` com `hive_partitioning` e `hive_types_autocast = false`) e no Delta (`delta_scan`), num motor DuckDB da chamada: as colunas `Numeric` como `DECIMAL(38, 6)` e as `Double` da mesma forma só nos valores finitos, com os não finitos contados à parte, porque a soma em ponto flutuante depende da ordem, os valores são os mesmos dos dois lados e o `CAST` de um `NaN` ou de um infinito falha com `ConversionException` (leitura de 2026-09-23); uma partição presente num lado só, e toda partição de uma tabela ainda fora do Delta, com `None` nas linhas do outro; as conversões de tipo lidas no rodapé do primeiro arquivo e as entradas fora do padrão. `LoadReport.matches` exige contagens, somas e não finitos iguais em toda partição, e a carga só termina quando coincidem. |
| `serialize-db load` | `--metadata`, `--source`, `--root` (`SERIALIZE_DB_ROOT`), `--environment` (`SERIALIZE_DB_ENVIRONMENT`, `dev`), `--tables` (todas sem ele, na ordem de `load_order`) e `--partitions`: `initial_load` e depois `load_report` de cada tabela, com as partições gravadas, cada diferença, o veredito, as conversões e o que ficou fora do padrão e fora do modelo impressos; sai com 1 na partição fora do contrato e na diferença, com 2 no modelo fora do contrato (`check_models`), na tabela fora do modelo e na origem ausente. |

`convert_to_deltalake` registra os arquivos no lugar, sem reescrever, só quando eles já têm os
tipos, a ordem de colunas e o layout Hive do contrato; a origem tem o layout (`data_str=<valor>/`)
e não os tipos (`INT96`, chaves em `int32`), e ele não é o caminho.
Depois da carga os leitores abrem o Delta, e as pastas de origem ficam como cópia até a primeira
publicação no Redshift. Testes: `tests/test_load.py` sobre a base fictícia de
`tests/source_db_projetado.py`, gravada sob a raiz local, com a carga interrompida, as chaves em
`int64`, o `timestamp` truncado, o relatório de contagens e somas, as recusas sem commit, o `Double`
não finito, os órfãos da auditoria e a linha de comando (a seção "Testes por caso"). Provas de
conceito:
`test_deltalake.py::test_initial_load_from_parquet_folders` (cada pasta lida sem
`hive_partitioning`, com o valor do caminho na coluna de partição, a chave em `BIGINT` e o `double`
mantido, a partição por `overwrite` com predicado, a retomada pelas partições já presentes, o
relatório de contagens e somas em `DECIMAL(38, 6)` e o `data_str` que a detecção de tipos do Hive
lê como `DATE`), `test_pyarrow.py` (`test_hive_partitioned_dataset`,
`test_parquet_streaming_read_filters_and_pandas`) e
`test_duckdb.py::test_decimal_from_pandas_sample_versus_arrow_schema`.

## A migração adiantada

A base Delta sobre a qual as etapas 3 a 6 e 8 se desenvolveram saiu antes desta etapa, pelo caminho
aceito pelo usuário em 2026-09-21: `scripts/migrate_parquet_to_delta.py`, escrito e testado nesse
dia sobre `serialize_db.schema` ([etapa 1](PLAN-STAGE-1.md)), o `deltalake` e o DuckDB diretos, sem
as etapas 3 e 4, com o corpo que `initial_load` absorveu em 2026-09-24: a descoberta das partições,
a consulta com os `CAST` para o contrato (`sql_type(coluna, "duckdb")` e `quoted`, porque
`cad_contratos.to` é palavra reservada e a consulta sem aspas falha no DuckDB), a conferência numa
consulta só (a coluna de origem contra o valor do caminho, os nulos das colunas `NOT NULL`, os
textos acima de `String(n)` em bytes por `strlen`, e as colunas `Double` com `NaN` ou infinito), o
`COPY ... (FORMAT parquet, RETURN_STATS)` na ordem da `sort_key` e a ação `add` por
`create_write_transaction` com `numRecords`, `nullCount` e o mínimo e o máximo das colunas
inteiras, de data, `Double` e texto (decisão do usuário de 2026-09-22), a retomada pelo log e o
relatório de contagens e somas por partição, as `Double` só nos valores finitos e com os não finitos
contados. Desde 2026-09-24 o script é a ferramenta de operação sobre o pacote (decisão do usuário):
`load.load_order`, `load.initial_load` partição por partição, com as linhas, o tempo e o pico de
memória do processo impressos por partição, `load.load_report` e `load.entries_outside_the_model`,
mais o `--report` JSON com a máquina, as versões, os limites do DuckDB lidos do ambiente e os
argumentos, regravado depois de cada partição gravada com a tabela da vez em `in_progress`; a raiz
Delta é a de `Database`, `<raiz>/<ambiente>/<tabela>`, com `--environment`. A medição de cada
partição com e sem a ordem, cada variante num processo novo com o pico do processo filho (`VmHWM`),
saiu do script na mesma decisão: a bateria de 2026-09-24 mediu as quatro partições de
`cad_lancamentos` nas duas variantes, a ordem pela `sort_key` está decidida, e os números estão em
[`POC.md`](POC.md) e abaixo. `tests/test_migrate_parquet_to_delta.py` cobre o script sobre a base
fictícia (3 casos, marcador `local`): a linha de comando sobre a base inteira, duas vezes, com o
ambiente, cada tabela e o que ficou fora do modelo no relatório JSON; o relatório parcial de uma
carga interrompida numa partição fora do contrato; e a recusa do modelo com violações. A versão que
rodou no ambiente alvo em 2026-09-23 registrava o mínimo e o máximo de toda coluna `Double`, e o
relatório dela falharia com `ConversionException` numa coluna com `NaN` ou infinito. O primeiro
`delta_scan` sobre uma tabela criada por `delta_schema` leu toda coluna como nula por causa do
`parquet.field.id` que o esquema Delta herdava do Arrow, corrigido na etapa 1 no mesmo dia
([`POC.md`](POC.md)). O que as execuções no alvo mostraram:

- O usuário copiou a base de produção para um prefixo do bucket do projeto separado da raiz das
  tabelas Delta (`aws s3 sync`, a mesma estrutura de pastas): a carga só lê, e a cópia congela o
  snapshot lido em 2026-09-21, enquanto a base de produção muda a cada carga mensal (a última em
  2026-09-14).
- O `COPY ... TO 's3://...' (RETURN_STATS)` do DuckDB gravou no ambiente alvo as partições das
  onze tabelas que terminaram a migração de 2026-09-23 às
  23:05, com contagens e somas iguais às da origem ([`POC.md`](POC.md)); gravar em disco e subir
  pelo `boto3` fica de fora.
- A maior partição de `cad_lancamentos`, 2026-03-31, com 52.654.607 linhas, não terminou: numa
  máquina de 4 vCPUs e 15.786 MB, depois de gravar 2026-01-31 e 2026-02-28, o kernel matou o
  processo por falta de memória, com o `Killed` no terminal que o usuário viu algumas vezes
  (2026-09-24), sob o `memory_limit` padrão do DuckDB, 12,3 GiB. Cada chamada de `initial_load` abre o
  motor DuckDB com os limites lidos do ambiente naquele momento, `threads` nas CPUs que o processo
  pode usar e `memory_limit` na metade da memória que ele ainda pode usar
  (`serialize_db.engine.duckdb.environment_limits`, instrução do usuário de 2026-09-24), e o fecha
  no fim, e o script chama uma por partição: o DuckDB só devolve a memória ao fechar, e o RSS de uma conexão ficou em 1.188 MB depois do `DROP` da tabela que a
  consulta ordenada criou e voltou a 208 MB no `close` ([`POC.md`](POC.md)). Em 2026-09-24, numa
  máquina de 16 vCPUs e 31.159 MB, com `memory_limit` de 13,4 GiB e 16 threads, as quatro partições
  entraram em 6,4 s, 5,4 s, 10,1 s e 6,4 s, com o pico do processo em 9.678 MB depois das duas
  primeiras e 16.430 MB depois da 2026-03-31, 20% acima do limite e 53% da memória da máquina, e
  contagens e somas iguais; a medição da 2026-03-31 deu 10,6 s e 15.126 MB ordenada e 12,5 s e
  7.540 MB sem ordem, e a ordem foi mais rápida nas quatro partições com 16 threads, com arquivos
  do mesmo tamanho ([`POC.md`](POC.md)): a carga ordenada da maior partição pede uma máquina de
  32 GB, e a sem ordem cabe em 16 GB; o script regrava o relatório depois de cada passo.
  O secret da conexão guarda a credencial resolvida na abertura e leva `REFRESH auto` (decisão do
  usuário de 2026-09-24, [etapa 3](PLAN-STAGE-3.md)). Na execução de 2026-09-23, `cad_lancamentos` rodou
  sozinho na máquina, entre `cad_contratos` e `cad_operacoes`, na ordem de `SUITE.md`, e o probe
  das threads veio depois, às 23:21.
  Numa partição sintética com as mesmas linhas e colunas, num contêiner de 4 vCPUs e 16.095 MB, a
  ordem multiplicou o tempo do `register` por 3,6 (50,2 s contra 14,0 s) e o pico por 2,6
  (11.966 MB contra 4.517 MB), e o `COPY` ordenado direto no DuckDB levou 17,4 s com pico de
  7.432 MB sob 6 GiB, contra 17,2 s sob 12,3 GiB.

Os tipos do modelo cliente ficam fechados antes da execução: mudá-los depois é reescrever o
Delta. A `sort_key` de cada tabela particionada está decidida ([`PLAN-STAGE-1.md`](PLAN-STAGE-1.md),
2026-09-21); o log do Delta não a guarda, e mudá-la depois é reordenar as partições que interessam.
A carga pelo pacote rodou no ambiente alvo em 2026-09-24 às 14:16, pelo script, sobre a cópia da
base de produção, na raiz `<raiz>/prod/<tabela>`: as 12 tabelas com contagens e somas iguais,
`cad_lancamentos` em 22,7 s, 16,8 s, 36,9 s e 22,7 s com o pico do processo em 16.198 MB sob
`memory_limit` de 14.030 MiB, numa máquina de 16 vCPUs e 31.383 MB, e a auditoria com
`--foreign-keys` sobre 2026-01-31 leu os 989.852 órfãos de `orfao_data_base_sistema_contrato`, o
`data_base` sem `cad_contratos` ([`POC.md`](POC.md)). A repetição das 16:51, sobre a raiz
recarregada, leu o mesmo, com `cad_lancamentos` em 19,9 s, 15,3 s, 31,8 s e 19,4 s e o pico em
16.355 MB sob `memory_limit` de 14.036 MiB ([`POC.md`](POC.md)).

## A implementação

O módulo `serialize_db.load` (`PartitionReport`, `LoadReport`, `discover_partitions`,
`partition_query`, `initial_load`, `load_report`, `load_order`, e `entries_outside_the_model`,
protegida), o subcomando `serialize-db load` e os casos de `tests/test_load.py` substituem a
interface e os rascunhos executados em 2026-09-21: as assinaturas e as docstrings estão no código e
na documentação do `pdoc`. O que a implementação mudou do plano:

- A conferência da partição roda antes da gravação, numa consulta só, e recusa por `ContractError`
  sem arquivo gravado: o nulo numa coluna `NOT NULL`, o valor de `partition_source` fora do caminho
  e o texto acima de `String(n)`; a conferência de nulos do rodapé de `register_files` é a segunda
  barreira. A contagem da mesma consulta vai a `expected_rows`.
- `initial_load` e `load_report` recebem `config`, a `DuckDBConfig` do motor da chamada, porque os
  testes escrevem o banco e o transbordo do DuckDB sob a raiz autorizada; sem ela, a pasta
  temporária do sistema e os limites lidos do ambiente.
- `serialize-db load` recebe `--tables` (várias; todas sem ele, na ordem de `load_order`) no lugar
  de `--table`, como `serialize-db publish`, e `--root` e `--environment` da
  [etapa 6](PLAN-STAGE-6.md): a raiz Delta é `<raiz>/<ambiente>/<tabela>`, e não `<raiz>/<tabela>`
  como o script gravava antes.
- Uma pasta `<coluna>=<valor>` cujo valor não segue a regra da partição vai para `skipped`, porque
  `register_files` a recusaria depois do `COPY`.
- A auditoria de chave estrangeira não entra na carga nem no relatório: os órfãos das bases reais
  (o `data_base` sem `cad_contratos`, o contrato sem cadastro) são lidos por `serialize-db audit
  --foreign-keys` sobre o Delta carregado, e `test_foreign_key_orphans_are_reported_not_blocking`
  prova que a carga passa e a auditoria os registra.
- `get_add_actions` do delta-rs devolve uma tabela `arro3` sem `to_pylist`, convertida por
  `pa.table`; `delta_scan` sobre a pasta de uma tabela que não existe é `IOException`, e o
  relatório de uma tabela ainda fora do Delta sai com `None` nas linhas do Delta
  ([`POC.md`](POC.md)).

## Estratégia de implementação

- **`discover_partitions`** lista as entradas diretas da pasta da tabela pelo `pyarrow.fs` do
  `Storage` da origem: as pastas `<coluna>=<valor>` com a coluna de `table_options(table)` e o valor
  na regra `PARTITION_VALUE` viram `{valor: URI}`; numa tabela sem partição, a própria pasta com
  `None`; o resto vai para `skipped`, e o relatório o lista.
- **`partition_query`** monta o `SELECT` do DuckDB que leva a partição ao contrato:
  `read_parquet('<pasta>/*.parquet', hive_partitioning = false)`, `CAST("<coluna>" AS <tipo>) AS
  "<coluna>"` por coluna, o tipo por `sql_type(coluna, "duckdb")` e o nome por `quoted` da
  [etapa 1](PLAN-STAGE-1.md) (`BIGINT` nas chaves `int32`, `TIMESTAMP` no `INT96`, que o DuckDB
  trunca a microssegundos); `'<valor>' AS <coluna de partição>` numa tabela particionada. As colunas
  `double` passam como estão, sem arredondamento (decisão de 2026-09-20).
- **`initial_load`** cria a tabela (`create_table`), lê as partições já presentes
  (`partition_values`) e pula cada uma delas (a retomada); abre o motor DuckDB da chamada, com as
  extensões e o secret da origem quando ela está no S3 e a raiz Delta numa pasta local; para cada
  partição pendente, a consulta de conferência (a contagem, `count(*) FILTER (WHERE <coluna> <>
  strftime(<partition_source>, '%Y-%m-%d'))` quando o modelo declara `partition_source`, os nulos
  das colunas `NOT NULL`, os textos acima de `String(n)` e os não finitos de cada coluna `Double`
  para `columns_without_min_max`), a recusa por `ContractError` e a gravação: `COPY (SELECT
  <colunas sem a de partição> FROM (<consulta>) ORDER BY <sort_key>) TO
  '<uri>/<coluna>=<valor>/carga-<id>_<uuid>.parquet' (FORMAT parquet, RETURN_STATS)` e
  `register_files` com o `RegisteredFile` de `file_from_return_stats`, que traz as conferências do
  rodapé de cada arquivo e a releitura depois do commit pelos dois leitores, da
  [etapa 3](PLAN-STAGE-3.md). As `threads` e o `memory_limit` do motor vêm de `environment_limits`
  a cada abertura.
- **`load_report`** roda a mesma agregação nos dois lados, `count(*)`, `sum(CAST(<coluna> AS
  DECIMAL(38, 6)))` por coluna `Numeric` e, por coluna `Double`, a mesma soma só dos valores
  finitos (`CASE WHEN isfinite(<coluna>) THEN ... END`) com a contagem dos não finitos, agrupada
  pela coluna de partição (`hive_partitioning = true, hive_types_autocast = false` na origem,
  `delta_scan` no destino, só quando a tabela existe), e monta `LoadReport`; `matches` exige
  contagens e somas iguais em toda partição.
- **`serialize-db load`** confere o modelo por `check_models`, seleciona as tabelas como
  `serialize-db publish` e as ordena por `load_order`, chama `initial_load` e depois `load_report`
  de cada uma, imprime o relatório e sai com 1 quando `matches` é falso ou uma partição está fora
  do contrato. Ela reaproveita o que a [etapa 6](PLAN-STAGE-6.md) pôs em `serialize_db.cli`:
  `--metadata`, `--root` e `--environment` com os padrões `SERIALIZE_DB_*`, o `_name_argument` da
  regra da partição em `--partitions`, o `modulo:atributo` que não importa como erro de uso, e o
  `logging` em `INFO`; `db` é o `Database` da etapa 6.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `discover_partitions` | Pasta da tabela legível. | Um valor por pasta `<coluna>=<valor>`, ou `None`; o resto em `skipped`. |
| `partition_query` | Arquivos com as colunas do contrato. | Um `SELECT` cujo esquema Arrow é o do contrato, com a coluna de partição no fim. |
| `initial_load` | Tabela aprovada por `check_models`; a origem intocada (a carga só lê). | Uma versão por partição carregada; a segunda chamada não carrega nada; um valor de `partition_source` diferente do caminho aborta a partição sem commit. |
| `load_report` | Carga concluída. | Contagem e somas por partição dos dois lados e o veredito; a carga só termina com `matches` verdadeiro. |

## Testes por caso

`tests/test_load.py` sobre a base fictícia de `tests/source_db_projetado.py`, gravada sob a raiz
local (20 casos, marcador `local`), e `tests/test_migrate_parquet_to_delta.py` sobre o script (3).

| Caso | Teste | O que confere |
| --- | --- | --- |
| Descoberta | `test_discover_partitions_and_skipped_entries` | As quatro tabelas particionadas dão os valores do caminho; as oito sem partição dão `None`; um arquivo solto e uma pasta com valor fora da regra vão para `skipped`, uma pasta com valor que não é data é partição; `alembic_version`, `meta_update_status` e `schema.json` fora do modelo; a pasta da tabela ausente é `FileNotFoundError`. |
| Ordem da carga | `test_load_order_puts_unpartitioned_tables_first` | As tabelas sem partição antes das quatro particionadas, na ordem do modelo. |
| Consulta | `test_partition_query_casts_to_the_contract` | O esquema Arrow do `SELECT` é `arrow_schema(table)`; `id_lancamento` em `int64`, `timestamp` em `[us]`, `data_base_str` com o valor do caminho; a consulta de `cad_contratos` roda com `to` entre aspas. |
| Carga | `test_initial_load_loads_every_partition_once` | Primeira passagem carrega todas, com o nome, a partição, as retenções e o `execution_id` `carga-<id>` no log; segunda, nenhuma; a tabela sem partição com `None`; `partitions` filtra e deixa a tabela sem partição de fora. |
| Interrupção | `test_interrupted_load_resumes` | Uma exceção injetada no registro da terceira partição; a chamada seguinte carrega só as restantes. |
| Tipos | `test_keys_are_int64_and_timestamps_are_microseconds` | O Delta lê `int64` e `timestamp[us]`; o arquivo gravado tem `INT64` onde a origem tinha `INT32` e `INT96`, sem a coluna de partição. |
| Ordem das linhas | `test_rows_are_written_in_sort_key_order` | As linhas saem na ordem da `sort_key`, que não é a da origem. |
| Partição divergente | `test_source_value_different_from_the_path_aborts` | Uma linha com `data` fora do valor do caminho, e um `to` acima de `String(2)`, abortam a partição sem commit. |
| Nulo em `NOT NULL` | `test_null_in_not_null_column_is_refused` | As sete colunas de `cad_contratos` declaradas anuláveis nos arquivos, um nulo plantado em cada: recusa com coluna e partição. |
| `Double` não finito | `test_nonfinite_double_leaves_min_max_out_of_its_partition` | As partições com `NaN` e infinito sem mínimo e máximo no log e no rodapé; o relatório soma só os finitos e conta os não finitos nos dois lados; o `delta_scan` devolve as duas linhas num filtro acima de todo número finito (issue #59). |
| Relatório | `test_load_report_matches_and_detects_a_difference` | Igual depois da carga; `None` na tabela sem partição; as conversões; a tabela ainda fora do Delta com `None` no lado do Delta; uma linha apagada do Delta aparece como diferença. |
| Órfãos | `test_foreign_key_orphans_are_reported_not_blocking` | A carga da base inteira passa sem conferir chave estrangeira; uma conta apagada deixa órfãos que a auditoria do motor DuckDB registra em `orfao_id_conta`, com as outras chaves aprovadas. |
| Linha de comando | `test_cli_load_loads_the_base_and_reports` | `serialize-db load` sobre a base inteira, duas vezes; 1 na partição fora do contrato e no relatório com diferença; 2 no modelo fora do contrato, na tabela fora do modelo e na origem ausente. |
| O script | `test_main_migrates_the_whole_base`, `test_report_keeps_the_progress_of_an_interrupted_load`, `test_main_refuses_a_model_with_violations` | O relatório JSON da base inteira, duas vezes; o relatório parcial da carga interrompida; a recusa do modelo com violações. |

## Decisões pendentes

Nenhuma. As decisões do usuário de 2026-09-24 sobre a carga estão escritas nas seções acima: a
ordem pela `sort_key` e o registro do arquivo do `COPY`, com o `rewrite` fora das etapas 4 e 7,
junto com a flag `export_mode` de `Execution`, de `serialize-db run` e de `SERIALIZE_DB_EXPORT_MODE`
(o `publish_partition` fica para a troca da [etapa 5](PLAN-STAGE-5.md) na partição com `Double`
não finito); `serialize_db.load` com as primitivas do plano e o script fino sobre o pacote; e a
medição das variantes fora do script. Nas nove partições medidas no ambiente alvo em 2026-09-23, o
`rewrite` levou de 1,14 a 1,52 vez o tempo do `register`, com o pico até 696 MB maior na gravação
ordenada, e a ordem custou de 1,23 a 1,63 vez o tempo do `register` e deixou os arquivos com 74% a
92% do tamanho ([`POC.md`](POC.md)).
