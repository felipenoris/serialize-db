# Etapa 7: carga inicial

A entrega e o critério de aceite desta etapa estão na tabela de etapas de [`PLAN.md`](PLAN.md), que
também fixa as decisões, as regras que toda etapa obedece e a ordem do trabalho.

A migração dos Parquet atuais para o Delta, uma passagem por tabela e por partição, reexecutável, em
`serialize_db.load`.

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
`COPY ... RETURN_STATS` do DuckDB mais `create_write_transaction` ficou em 600 MB: a partição de
`cad_lancamentos` vai por `export_mode="register"`, e a primeira carga de uma partição real mede os
dois modos antes de fixar o padrão da flag. A auditoria de chave estrangeira não é barreira da
carga: as duas bases têm `cad_lancamentos` de `data_base` 2026-01-31 sem `cad_contratos` dessa data
e o contrato `desemb-999` sem cadastro, inconsistências ignoradas por decisão de 2026-09-20, e o
relatório registra os órfãos.

| Primitiva | O que faz |
| --- | --- |
| `initial_load(db, table, source, partitions=None, mode=None)` | `create_table`; descobre as partições da pasta da tabela (`<coluna>=<AAAA-MM-DD>/`, a mesma coluna e o mesmo valor do contrato, ou o arquivo único na raiz da tabela) e, para cada uma, o DuckDB lê `read_parquet('<partição>/*.parquet')`, a pasta inteira e nunca a ordem dos nomes, sem `hive_partitioning` (com ele o DuckDB converte `data_str` a `DATE`), confere que `partition_source` (`data`, ou `data_base`) é igual ao valor do caminho em toda linha e acrescenta a coluna de partição com esse valor. As conversões que a origem exige são aplicadas antes do `cast` e registradas no relatório: as chaves de `int32` a `int64`, sem perda, e o `timestamp` `INT96` de nanossegundos a microssegundos (`coerce_int96_timestamp_unit="us"` no PyArrow; o `TIMESTAMP` do DuckDB já trunca; a precisão perdida não importa, decisão de 2026-09-20). As colunas `double` entram como estão, e a nulidade é a do modelo: um nulo numa coluna `NOT NULL` é recusado pelo `cast`, com a coluna e a partição na mensagem. `cast` converte para o contrato e `mode`, a flag `export_mode` da [etapa 6](PLAN-STAGE-6.md), decide a gravação: `rewrite` grava por `publish_partition`; `register` roda `COPY (<a mesma consulta>) TO '<uri>/<coluna>=<valor>/<nome único>.parquet' (FORMAT parquet, RETURN_STATS)` e registra por `register_files`, com as conferências da [etapa 3](PLAN-STAGE-3.md). Uma carga interrompida recomeça da partição seguinte à última publicada; `partitions` filtra as partições encontradas. As tabelas fora do modelo (`alembic_version`, `meta_update_status`) e o que não é Parquet (`schema.json`) ficam fora da carga e entram no relatório. |
| `load_report(db, table, source)` | Contagem e somas por partição, na origem e no Delta: as colunas `double` somadas como `DECIMAL(38, 6)` de cada valor, porque a soma em ponto flutuante depende da ordem e os valores são os mesmos dos dois lados; as `Numeric`, quando existirem, como estão. A carga só termina quando coincidem. |
| `serialize-db load` | `--table`, `--source` e `--partitions`. |

`convert_to_deltalake` registra os arquivos no lugar, sem reescrever, só quando eles já têm os
tipos, a ordem de colunas e o layout Hive do contrato; a origem tem o layout (`data_str=<valor>/`)
e não os tipos (`INT96`, chaves em `int32`), e ele não é o caminho.
Depois da carga os leitores abrem o Delta, e as pastas de origem ficam como cópia até a primeira
publicação no Redshift. Testes: `tests/test_load.py` sobre a base fictícia de
`tests/source_db_projetado.py`, gravada sob a raiz local, incluindo a carga interrompida, as chaves
em `int64`, o `timestamp` truncado, os dois modos com as mesmas contagens e somas, o relatório de
contagens e somas e as tabelas puladas. Provas
de conceito:
`test_deltalake.py::test_initial_load_from_parquet_folders` (o cast na consulta do DuckDB, o mês
por `overwrite` com predicado, a retomada pelos meses já presentes e o relatório de contagens e
somas), `test_pyarrow.py` (`test_hive_partitioned_dataset`,
`test_parquet_streaming_read_filters_and_pandas`) e
`test_duckdb.py::test_decimal_from_pandas_sample_versus_arrow_schema`.

## A migração adiantada

A base Delta sobre a qual as etapas 3 a 6 e 8 se desenvolvem sai antes da etapa 7, pelo caminho
aceito pelo usuário em 2026-09-21: `scripts/migrate_parquet_to_delta.py` é o rascunho abaixo
promovido a ferramenta, sobre `serialize_db.schema` ([etapa 1](PLAN-STAGE-1.md)), o `deltalake` e
o DuckDB diretos, sem as etapas 3 e 4. O que ele faz, por tabela do modelo cliente e por partição:
`discover_partitions`, `partition_query` com os `CAST` para o contrato (`arrow_schema` do modelo
cliente dá os tipos, e o DuckDB os recebe pela tabela de tipos de [`schema.md`](schema.md)), a
conferência do valor do caminho contra `partition_source`, `COPY ... (FORMAT parquet,
RETURN_STATS)` para `<raiz>/<tabela>/<coluna>=<valor>/` e a `AddAction` por
`create_write_transaction` (o modo `register`), a retomada pelas partições já presentes e
`load_report`. A ordem no alvo: as dez tabelas sem partição, `cad_contratos`, `cad_operacoes`,
`rel_contrato_operacao` e `cad_lancamentos` por último; o relatório de contagens e somas fecha cada
tabela. Antes do alvo, três coisas:

- O script roda sobre `tests/source_db_projetado.py` em pasta local (62 arquivos), o material do
  teste desta etapa, com o mesmo relatório.
- O usuário copia a base de produção para um prefixo do bucket do projeto separado da raiz das
  tabelas Delta (`aws s3 sync`, a mesma estrutura de pastas): a carga só lê, e a cópia congela o
  snapshot lido em 2026-09-21, enquanto a base de produção muda a cada carga mensal (a última em
  2026-09-14).
- Duas medições, cada uma numa sonda de poucas linhas antes de entrar no script: o
  `COPY ... TO 's3://...' (RETURN_STATS)` do DuckDB no ambiente alvo (a alternativa é gravar em
  disco, 29,8 GiB livres, e subir pelo `boto3`), e a memória e o tempo de uma partição de
  `cad_lancamentos` (35 milhões de linhas, cerca de 700 MB de Parquet) sob o `memory_limit` de
  6,1 GiB, a medição de [`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md) que decide o padrão de
  `export_mode`.

Os tipos e a `sort_key` do modelo cliente ficam fechados antes da execução: mudá-los depois é
reescrever o Delta. Quando as etapas 3, 4 e 7 chegarem, o corpo do script vira `initial_load`, e
`tests/test_load.py` o cobre; até `serialize-db load` existir, o script em `scripts/` é a
ferramenta de operação.

## Interface

```python
"""Assinaturas de serialize_db.load; os corpos estão no rascunho abaixo."""
import dataclasses
from typing import Literal

import sqlalchemy as sa

ExportMode = Literal["register", "rewrite"]


@dataclasses.dataclass(frozen=True)
class PartitionReport:
    value: str | None
    source_rows: int
    delta_rows: int
    source_sums: dict[str, object]      # coluna numérica -> soma como DECIMAL(38, 6)
    delta_sums: dict[str, object]

    @property
    def matches(self) -> bool: ...


@dataclasses.dataclass(frozen=True)
class LoadReport:
    table: str
    partitions: tuple[PartitionReport, ...]
    skipped: tuple[str, ...]            # entradas da pasta fora do padrão: alembic_version, schema.json
    conversions: tuple[str, ...]        # "id_contrato: int32 -> int64", "carimbo: INT96 -> timestamp[us]"

    @property
    def matches(self) -> bool: ...


def discover_partitions(source: str, table: sa.Table, storage: object) -> dict[str | None, str]: ...
def partition_query(folder: str, table: sa.Table, value: str | None) -> str: ...
def initial_load(db: object, table: sa.Table, source: str, partitions: list[str] | None = None, mode: ExportMode | None = None) -> list[str]: ...
def load_report(db: object, table: sa.Table, source: str) -> LoadReport: ...
```

## Estratégia de implementação

- **`discover_partitions`** lista a pasta da tabela na origem: as entradas `<coluna>=<AAAA-MM-DD>`
  com a coluna de `table_options(table)` viram `{valor: pasta}`; numa tabela sem partição, a própria
  pasta com `None`; o resto (`alembic_version`, `schema.json`, pastas sem o padrão) vai para
  `skipped`, e o relatório o lista.
- **`partition_query`** monta o `SELECT` do DuckDB que leva a partição ao contrato:
  `read_parquet('<pasta>/*.parquet', hive_partitioning = false)`, a pasta inteira e nunca a ordem
  dos nomes; `CAST(<coluna> AS <tipo do contrato no DuckDB>)` por coluna (`BIGINT` nas chaves
  `int32`, `TIMESTAMP` no `INT96`, que o DuckDB trunca a microssegundos); `'<valor>' AS <coluna de
  partição>` numa tabela particionada. As colunas `double` passam como estão, sem arredondamento
  (decisão de 2026-09-20).
- **`initial_load`** cria a tabela (`create_table`), lê as partições já presentes em
  `get_add_actions` e pula cada uma delas (a retomada); para cada partição pendente, confere
  `count(*) ... WHERE <coluna> <> strftime(<partition_source>, '%Y-%m-%d')` igual a zero, e grava
  conforme `mode`: `register` roda `COPY (SELECT <colunas sem a de partição> FROM (<consulta>)) TO
  '<uri>/<coluna>=<valor>/carga_inicial_<uuid>.parquet' (FORMAT parquet, RETURN_STATS)` e chama
  `register_files` com o `RegisteredFile` da linha do `RETURN_STATS`; `rewrite` passa
  `con.execute(consulta).to_arrow_reader()` por `cast` e `publish_partition`. A conexão DuckDB é
  do motor DuckDB da etapa 4, aberta pela própria carga, sem `Execution`; um nulo numa coluna `NOT
  NULL` é recusado pelo `cast` (`rewrite`) ou pela conferência de `nullCount` (`register`), com a
  coluna e a partição na mensagem.
- **`load_report`** roda a mesma agregação nos dois lados, `count(*)` e `sum(CAST(<coluna> AS
  DECIMAL(38, 6)))` por coluna `Double` e `Numeric`, agrupada pela coluna de partição
  (`hive_partitioning = true, hive_types_autocast = false` na origem, `delta_scan` no destino), e
  monta `LoadReport`; `matches` exige contagens e somas iguais em toda partição.
- **`serialize-db load`** recebe `--table`, `--source`, `--partitions` e `--mode`, chama
  `initial_load` e depois `load_report`, e sai com 1 quando `matches` é falso.

## Pré-requisitos e pós-condições

| Primitiva | Pré-requisitos | Pós-condições |
| --- | --- | --- |
| `discover_partitions` | Pasta da tabela legível. | Um valor por pasta `<coluna>=<AAAA-MM-DD>`, ou `None`; o resto em `skipped`. |
| `partition_query` | Arquivos com as colunas do contrato. | Um `SELECT` cujo esquema Arrow é o do contrato, com a coluna de partição no fim. |
| `initial_load` | Tabela aprovada por `check_models`; a origem intocada (a carga só lê). | Uma versão por partição carregada; a segunda chamada não carrega nada; um valor de `partition_source` diferente do caminho aborta a partição sem commit. |
| `load_report` | Carga concluída. | Contagem e somas por partição dos dois lados e o veredito; a carga só termina com `matches` verdadeiro. |

## Testes por caso

`tests/test_load.py` sobre a base fictícia de `tests/source_db_projetado.py`, gravada sob a raiz
local.

| Caso | Teste | O que confere |
| --- | --- | --- |
| Descoberta | `test_discover_partitions_and_skipped_entries` | As quatro tabelas particionadas dão os valores do caminho; as dez sem partição dão `None`; `alembic_version`, `meta_update_status` e `schema.json` em `skipped`. |
| Consulta | `test_partition_query_casts_to_the_contract` | O esquema Arrow do `SELECT` é `arrow_schema(table)`; `id_lancamento` em `int64`, `timestamp` em `[us]`, `data_str` presente. |
| Carga | `test_initial_load_loads_every_partition_once` | Primeira passagem carrega todas; segunda, nenhuma; `--partitions` filtra. |
| Interrupção | `test_interrupted_load_resumes` | Uma exceção injetada depois da segunda partição; a chamada seguinte carrega só as restantes. |
| Modos | `test_both_modes_give_the_same_counts_and_sums` | `register` e `rewrite` sobre a mesma tabela dão o mesmo `LoadReport`. |
| Tipos | `test_keys_are_int64_and_timestamps_are_microseconds` | O Delta lê `int64` e `timestamp[us]`; o arquivo gravado tem `INT64` onde a origem tinha `INT32` e `INT96`. |
| Partição divergente | `test_source_value_different_from_the_path_aborts` | Uma linha com `data` fora do valor do caminho aborta a partição, sem commit. |
| Nulo em `NOT NULL` | `test_null_in_not_null_column_is_refused` | As sete colunas de `cad_contratos` com um nulo plantado: recusa com coluna e partição. |
| Relatório | `test_load_report_matches_and_detects_a_difference` | Igual depois da carga; uma linha apagada do Delta aparece como diferença. |
| Órfãos | `test_foreign_key_orphans_are_reported_not_blocking` | A auditoria de chave estrangeira registra os órfãos das duas bases de origem (o `data_base` sem `cad_contratos`, o contrato sem cadastro) e não barra a carga. |

## Rascunhos executados

O rascunho grava uma origem com a estrutura da base de desenvolvimento (Hive por `data_str`,
`chunk_<n>.parquet`, chaves `int32`, `timestamp` em `INT96`, formato 1.0 sem dicionário), roda a
carga em modo `register` duas vezes e o relatório. Ele rodou em 2026-09-21 com as versões fixadas.

```python
"""Etapa 7: a carga inicial de uma pasta Hive com INT96 e chaves int32: descoberta das partições, a consulta com os casts, o modo register e o relatório."""
import datetime as dt
import json
import os
import re
import tempfile
import time
import uuid
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable, Schema as DeltaSchema
from deltalake.transaction import AddAction

PARTITION, SOURCE = "data_str", "data"
CONTRACT = pa.schema([pa.field("id_contrato", pa.int64(), nullable=False), pa.field(SOURCE, pa.date32(), nullable=False), pa.field("contrato", pa.string(), nullable=False),
                      pa.field("taxa", pa.float64()), pa.field("carimbo", pa.timestamp("us"), nullable=False), pa.field(PARTITION, pa.string(), nullable=False)])
DUCKDB_TYPES = {"int64": "BIGINT", "date32[day]": "DATE", "string": "VARCHAR", "double": "DOUBLE", "timestamp[us]": "TIMESTAMP"}


def discover_partitions(source: str, partition_by: str | None) -> dict[str | None, str]:
    """{valor: pasta}: as pastas <coluna>=<AAAA-MM-DD>/ da tabela, ou a própria raiz numa tabela sem partição."""
    if not partition_by:
        return {None: source}
    found = {}
    for entry in sorted(os.listdir(source)):
        match = re.fullmatch(rf"{partition_by}=(\d{{4}}-\d{{2}}-\d{{2}})", entry)
        if match:
            found[match.group(1)] = os.path.join(source, entry)
        elif not entry.startswith("_") and entry != "schema.json":
            print(f"   ignorado fora do padrão: {entry}")
    return found


def partition_query(folder: str, value: str | None) -> str:
    """O SELECT do DuckDB que leva a partição ao contrato: a pasta inteira, cada coluna no tipo do contrato, a coluna de partição com o valor do caminho."""
    columns = [f"CAST({f.name} AS {DUCKDB_TYPES[str(f.type)]}) AS {f.name}" for f in CONTRACT if f.name != PARTITION]
    if value is not None:
        columns.append(f"'{value}' AS {PARTITION}")
    return f"SELECT {', '.join(columns)} FROM read_parquet('{folder}/*.parquet', hive_partitioning = false)"


def initial_load(con, uri: str, source: str, partition_by: str | None, mode: str = "register") -> list[str]:
    DeltaTable.create(uri, DeltaSchema.from_arrow(CONTRACT), partition_by=[partition_by] if partition_by else None, mode="ignore")
    loaded = set(DeltaTable(uri).get_add_actions(flatten=True).column(f"partition.{partition_by}").to_pylist()) if partition_by and DeltaTable(uri).version() > 0 else set()
    done = []
    for value, folder in discover_partitions(source, partition_by).items():
        if value in loaded:
            continue                                                                             # a retomada pula o que já está publicado
        query = partition_query(folder, value)
        mismatch = con.execute(f"SELECT count(*) FROM ({query}) WHERE {PARTITION} <> strftime({SOURCE}, '%Y-%m-%d')").fetchone()[0]
        if mismatch:
            raise ValueError(f"{value}: {mismatch} linhas com {SOURCE} diferente do valor do caminho")
        assert mode == "register"                                                                # rewrite: write_deltalake(uri, con.execute(query).to_arrow_reader(), mode="overwrite", predicate=...)
        relative = f"{partition_by}={value}/carga_inicial_{uuid.uuid4().hex}.parquet"
        Path(uri, relative).parent.mkdir(parents=True, exist_ok=True)
        inner = ", ".join(f.name for f in CONTRACT if f.name != PARTITION)
        (row,) = con.execute(f"COPY (SELECT {inner} FROM ({query})) TO '{Path(uri, relative)}' (FORMAT parquet, RETURN_STATS)").fetchall()
        stats = {k.strip('"'): v for k, v in row[4].items()}
        table = DeltaTable(uri)
        table.create_write_transaction([AddAction(path=relative, size=row[2], partition_values={partition_by: value}, modification_time=int(time.time() * 1000), data_change=True,
                                                  stats=json.dumps({"numRecords": row[1], "minValues": {"id_contrato": int(stats["id_contrato"]["min"])}, "maxValues": {"id_contrato": int(stats["id_contrato"]["max"])},
                                                                    "nullCount": {c: int(stats[c]["null_count"]) for c in stats}}))],
                                       mode="overwrite", schema=table.schema(), partition_by=[partition_by], partition_filters=[(partition_by, "=", value)])
        done.append(value)
    return done


def load_report(con, uri: str, source: str) -> list[tuple]:
    """Contagem e somas por partição na origem e no Delta; as colunas double somadas como DECIMAL(38, 6) de cada valor."""
    origin = con.execute(f"SELECT {PARTITION}, count(*), sum(CAST(taxa AS DECIMAL(38, 6))) FROM read_parquet('{source}/*/*.parquet', hive_partitioning = true, hive_types_autocast = false) GROUP BY 1 ORDER BY 1").fetchall()
    loaded = con.execute(f"SELECT {PARTITION}, count(*), sum(CAST(taxa AS DECIMAL(38, 6))) FROM delta_scan('{uri}') GROUP BY 1 ORDER BY 1").fetchall()
    return [(a, b, a == b) for a, b in zip(origin, loaded)]


with tempfile.TemporaryDirectory() as folder:
    source = os.path.join(folder, "db_projetado", "cad_contratos")
    for value, start in (("2026-02-28", 1), ("2026-03-31", 1001)):
        day = dt.date.fromisoformat(value)
        for chunk in range(3):                                                              # chunk_0, chunk_1, chunk_2: o padrão da origem
            first = start + chunk * 100
            pq.write_table(pa.table({"id_contrato": pa.array(range(first, first + 100), pa.int32()), SOURCE: pa.array([day] * 100, pa.date32()),
                                     "contrato": pa.array([f"c{k}" for k in range(first, first + 100)]), "taxa": pa.array([k / 1000 for k in range(first, first + 100)]),
                                     "carimbo": pa.array([dt.datetime(2026, 1, 1, 0, 0, 0, 123456) + dt.timedelta(microseconds=k) for k in range(first, first + 100)], pa.timestamp("ns"))}),
                           Path(source, f"{PARTITION}={value}", f"chunk_{chunk}.parquet").as_posix() if Path(source, f"{PARTITION}={value}").mkdir(parents=True, exist_ok=True) is None else "",
                           use_deprecated_int96_timestamps=True, version="1.0", use_dictionary=False)
    Path(folder, "db_projetado", "schema.json").write_text("{}")
    Path(source, "alembic_version").mkdir()

    con = duckdb.connect(config={"extension_directory": os.environ.get("SERIALIZE_DB_DUCKDB_EXTENSIONS", ".duckdb"), "autoinstall_known_extensions": False, "autoload_known_extensions": False})
    con.execute("LOAD delta")
    print("tipos físicos da origem:", {pq.ParquetFile(Path(source, f"{PARTITION}=2026-02-28", "chunk_0.parquet")).schema.column(i).physical_type for i in range(5)})
    print("consulta:", " ".join(partition_query(os.path.join(source, f"{PARTITION}=2026-02-28"), "2026-02-28").split())[:200], "...")
    uri = os.path.join(folder, "prod", "cad_contratos")
    print("primeira passagem:", initial_load(con, uri, source, PARTITION))
    print("segunda passagem:", initial_load(con, uri, source, PARTITION))
    print("esquema do Delta:", [f"{f.name}:{f.type}" for f in pa.schema(DeltaTable(uri).schema())])
    print("chave e carimbo relidos:", con.execute(f"SELECT typeof(id_contrato), max(id_contrato), typeof(carimbo), min(carimbo) FROM delta_scan('{uri}') GROUP BY ALL").fetchall())
    for row in load_report(con, uri, source):
        print("relatório:", row)
    physical = pq.ParquetFile(DeltaTable(uri).file_uris()[0]).schema
    print("tipos físicos gravados:", {physical.column(i).name: physical.column(i).physical_type for i in range(len(physical))})
```

Saída:

```
tipos físicos da origem: {'BYTE_ARRAY', 'INT32', 'DOUBLE', 'INT96'}
consulta: SELECT CAST(id_contrato AS BIGINT) AS id_contrato, CAST(data AS DATE) AS data, CAST(contrato AS VARCHAR) AS contrato, CAST(taxa AS DOUBLE) AS taxa, CAST(carimbo AS TIMESTAMP) AS carimbo, '2026-02-28'  ...
   ignorado fora do padrão: alembic_version
primeira passagem: ['2026-02-28', '2026-03-31']
   ignorado fora do padrão: alembic_version
segunda passagem: []
esquema do Delta: ['id_contrato:int64', 'data:date32[day]', 'contrato:string', 'taxa:double', 'carimbo:timestamp[us]', 'data_str:string']
chave e carimbo relidos: [('BIGINT', 1300, 'TIMESTAMP', datetime.datetime(2026, 1, 1, 0, 0, 0, 123457))]
relatório: (('2026-02-28', 300, Decimal('45.150000')), ('2026-02-28', 300, Decimal('45.150000')), True)
relatório: (('2026-03-31', 300, Decimal('345.150000')), ('2026-03-31', 300, Decimal('345.150000')), True)
tipos físicos gravados: {'id_contrato': 'INT64', 'data': 'INT32', 'contrato': 'BYTE_ARRAY', 'taxa': 'DOUBLE', 'carimbo': 'INT64'}
```

## Decisões pendentes

- **[decisão] A `sort_key` na consulta da carga.** O `COPY` sem `ORDER BY` grava na ordem dos
  arquivos; ordenar pela `sort_key` do modelo melhora a poda e custa uma ordenação por partição, que
  em `cad_lancamentos` é de 35 milhões de linhas sob `memory_limit`. O modelo cliente propõe a
  `sort_key` de cada tabela particionada ([`PLAN-STAGE-1.md`](PLAN-STAGE-1.md)), fechada antes da
  migração adiantada.
- **[decisão] O padrão de `export_mode` na carga**, `register` até a medição da partição de
  `cad_lancamentos` ([`OPEN_QUESTIONS.md`](OPEN_QUESTIONS.md)); o ambiente alvo tem 7,6 GiB, e o
  `write_deltalake` de um leitor cresceu com a entrada (1.140 MB para 135 MB de Parquet). A
  migração adiantada faz essa medição.
