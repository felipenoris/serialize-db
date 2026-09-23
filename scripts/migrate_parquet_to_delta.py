"""A migração adiantada: a base Parquet de origem vira tabelas Delta, uma partição por commit.

O script é o rascunho da etapa 7 (``plan/PLAN-STAGE-7.md``, seção "A migração adiantada")
promovido a ferramenta, sobre ``serialize_db.schema``, o ``deltalake`` e o DuckDB, sem as etapas
3 e 4; quando elas chegarem, o corpo vira ``serialize_db.load.initial_load``. Para cada tabela do
modelo, as sem partição primeiro e as particionadas depois, na ordem do modelo:

- ``discover_partitions`` lista a pasta da tabela na origem: as pastas ``<coluna>=<valor>``
  viram as partições, e o que não segue o padrão vai para o relatório;
- ``partition_query`` monta o ``SELECT`` do DuckDB que leva a partição ao contrato: a pasta
  inteira por ``read_parquet``, cada coluna em ``CAST`` para o tipo de ``sql_type`` (as chaves de
  ``int32`` a ``BIGINT``, o ``timestamp`` ``INT96`` a ``TIMESTAMP``, truncado a microssegundos),
  o valor do caminho na coluna de partição;
- a carga confere numa consulta que a coluna de origem da partição (``data``, ``data_base``),
  quando o modelo a declara em ``partition_source``, é igual ao valor do caminho em toda linha,
  que nenhuma coluna ``NOT NULL`` tem nulo e que nenhum texto passa do ``String(n)`` em bytes, e
  a mesma consulta acha as colunas ``Double`` com ``NaN`` ou infinito na partição; depois grava a
  partição no modo pedido: ``register`` roda ``COPY ... TO`` na pasta da tabela com
  ``RETURN_STATS`` e registra o arquivo no log por ``create_write_transaction``, com o
  ``nullCount`` de toda coluna e o mínimo e o máximo das inteiras, de data, ``Double`` e texto;
  ``rewrite`` passa o leitor da consulta por ``cast`` e ``write_deltalake``. Nos dois modos uma
  coluna ``Double`` com valor não finito na partição fica sem mínimo e máximo no log, e no rodapé
  do ``rewrite``: o DuckDB ordena o ``NaN`` acima de todo número e perde a linha quando poda por um
  máximo sem ele (issue #59, ``plan/PLAN-STAGE-3.md``). As linhas saem na ordem da ``sort_key`` do
  modelo, salvo ``--no-sort``;
- a retomada pula as partições já no log: a segunda execução não grava nada;
- ``load_report`` compara contagem e somas por partição entre a origem e o Delta, as colunas
  ``Double`` e ``Numeric`` somadas como ``DECIMAL(38, 6)``, as ``Double`` só nos valores finitos e
  com os não finitos contados à parte, e o script sai com 1 quando diferem.

Uma partição fora do contrato interrompe a execução sem commit, com a tabela, a partição e a
coluna na mensagem; a execução seguinte recomeça dela. As tabelas da origem fora do modelo
(``alembic_version``, ``meta_update_status``) e o ``schema.json`` da raiz ficam de fora e entram
no relatório. A auditoria de chaves estrangeiras é da etapa 7, não daqui.

Cada partição carregada imprime as linhas, o tempo e o pico de memória do processo até ali. Antes
da carga de cada tabela particionada, ``measure_table`` mede a gravação de cada partição pedida,
esteja ela no log ou não, nas quatro variantes de ``VARIANTS`` (``register`` e ``rewrite``, com e
sem a ordem da ``sort_key``): cada variante roda num processo novo, pelo ``spawn``, e grava numa
tabela descartável sob ``<raiz>/_medicao_<tabela>/``, apagada logo depois; o pico de memória é o do
próprio processo filho (``VmHWM`` no Linux), ao lado da base depois das importações e da conexão.
É a medição de ``plan/OPEN_QUESTIONS.md`` que decide o padrão de ``export_mode`` e a ordem da
carga, e o relatório leva também a máquina, as versões e as configurações do DuckDB
(``describe_environment``). A variante que falha, até pela falta de memória que mata o processo
filho, entra no relatório com o erro; ``--no-measure`` desliga a medição.

Origem e raiz aceitam pasta local ou ``s3://bucket/prefixo``: no S3 o DuckDB carrega ``httpfs``
e ``aws`` da pasta de extensões (``SERIALIZE_DB_DUCKDB_EXTENSIONS``, senão ``.duckdb/`` na raiz
do repositório) e cria um secret ``credential_chain`` com a região de ``AWS_REGION`` ou
``AWS_DEFAULT_REGION``, e o delta-rs recebe a mesma região. O ambiente alvo não tem variável de
proxy, e o script não faz a separação de ``HTTP_PROXY`` que os probes fazem.

Sobre a base fictícia de ``tests/source_db_projetado.py``, em pasta local:

    PYTHONPATH=tests uv run python scripts/migrate_parquet_to_delta.py \\
        --metadata client_model:Base.metadata --source /pasta/db_projetado --root /pasta/delta

No ambiente alvo, com a pasta preparada, sobre a cópia da base de produção:

    PYTHONPATH=tests .venv/bin/python scripts/migrate_parquet_to_delta.py \\
        --metadata client_model:Base.metadata \\
        --source s3://bucket/prefixo/db_projetado --root s3://bucket/prefixo/delta \\
        --tables cad_contratos --report relatorio.json

``tests/test_migrate_parquet_to_delta.py`` cobre o script sobre a base fictícia.
"""

from __future__ import annotations

import argparse
import dataclasses
import importlib
import importlib.metadata
import json
import multiprocessing
import os
import platform
import re
import resource
import sys
import time
import uuid
from collections.abc import Callable, Collection
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import sqlalchemy as sa
from deltalake import ColumnProperties, DeltaTable, WriterProperties, write_deltalake
from deltalake.exceptions import DeltaError
from deltalake.transaction import AddAction

from serialize_db import schema
from serialize_db.errors import ContractError

PARTITION_FOLDER = re.compile(r"(?P<column>[a-z_]+)=(?P<value>[^/=]+)")

# As retenções que a etapa 3 fixa em create_table: o log legível por dez anos e os arquivos
# removidos guardados por 400 dias, a janela em que toda versão continua legível.
RETENTION = {
    "delta.logRetentionDuration": "interval 3650 days",
    "delta.deletedFileRetentionDuration": "interval 400 days",
}

# ---------------------------------------------------------------- o relatório


@dataclasses.dataclass(frozen=True)
class PartitionReport:
    """Contagem, somas e não finitos de uma partição, na origem e no Delta."""

    value: str | None
    source_rows: int | None
    delta_rows: int | None
    source_sums: dict[str, Decimal | None]
    delta_sums: dict[str, Decimal | None]
    source_nonfinite: dict[str, int]
    delta_nonfinite: dict[str, int]

    @property
    def matches(self) -> bool:
        same_rows = self.source_rows == self.delta_rows
        same_sums = self.source_sums == self.delta_sums
        return same_rows and same_sums and self.source_nonfinite == self.delta_nonfinite


@dataclasses.dataclass(frozen=True)
class PartitionLoad:
    """Uma partição gravada agora: linhas, tempo, o RSS máximo do processo até ali e as colunas
    ``Double`` que ficaram sem mínimo e máximo."""

    value: str | None
    rows: int
    seconds: float
    peak_rss_mb: float
    nonfinite_columns: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class VariantMeasurement:
    """Uma variante da gravação de uma partição, medida num processo novo: as linhas, o tempo da
    conferência e da gravação, a memória do processo depois das importações e da conexão e o pico
    dele, os arquivos e os bytes gravados; ``error`` quando a variante falhou."""

    value: str | None
    mode: str
    sort: bool
    rows: int | None = None
    seconds: float | None = None
    base_mb: float | None = None
    peak_mb: float | None = None
    files: int | None = None
    bytes: int | None = None
    error: str | None = None


@dataclasses.dataclass(frozen=True)
class LoadReport:
    """O relatório de uma tabela: as partições conferidas, o que foi gravado agora, as entradas
    fora do padrão, as conversões de tipo da origem para o contrato e a medição das variantes."""

    table: str
    partitions: tuple[PartitionReport, ...]
    loaded: tuple[PartitionLoad, ...]
    skipped: tuple[str, ...]
    conversions: tuple[str, ...]
    measurements: tuple[VariantMeasurement, ...] = ()

    @property
    def matches(self) -> bool:
        return all(partition.matches for partition in self.partitions)


@dataclasses.dataclass(frozen=True)
class Settings:
    """O que a linha de comando fixa para a execução inteira."""

    mode: str
    sort: bool
    partitions: tuple[str, ...]
    storage_options: dict[str, str]


# ---------------------------------------------------------------- as pastas


@dataclasses.dataclass(frozen=True)
class Location:
    """Uma pasta da origem ou da raiz Delta: a URI que o DuckDB e o delta-rs recebem e o sistema de
    arquivos do PyArrow que a lista, com o caminho na forma dele."""

    uri: str
    filesystem: pafs.FileSystem
    path: str

    def child(self, name: str) -> Location:
        """A subpasta ``name``, com a URI e o caminho do sistema de arquivos."""
        return Location(f"{self.uri}/{name}", self.filesystem, f"{self.path}/{name}")

    def entries(self) -> list[pafs.FileInfo]:
        """As entradas da pasta, em ordem de nome."""
        found = self.filesystem.get_file_info(pafs.FileSelector(self.path))
        return sorted(found, key=lambda entry: entry.base_name)


def open_location(uri: str) -> Location:
    """A pasta local ou a URI ``s3://``, com o sistema de arquivos do PyArrow que a lista."""
    if "://" in uri:
        filesystem, path = pafs.FileSystem.from_uri(uri)
        return Location(uri.rstrip("/"), filesystem, path.rstrip("/"))
    path = Path(uri).expanduser().resolve()
    return Location(str(path), pafs.LocalFileSystem(), str(path))


def ensure_folder(location: Location) -> None:
    """Cria a pasta numa raiz local: o ``COPY`` do DuckDB não cria a pasta do arquivo, e no S3
    não há pasta a criar."""
    if isinstance(location.filesystem, pafs.LocalFileSystem):
        location.filesystem.create_dir(location.path, recursive=True)


def first_parquet(location: Location) -> str:
    """O caminho, na forma do sistema de arquivos, do primeiro arquivo Parquet da pasta."""
    for entry in location.entries():
        if entry.type == pafs.FileType.File and entry.base_name.endswith(".parquet"):
            return entry.path
    raise FileNotFoundError(f"{location.uri}: nenhum arquivo Parquet na pasta")


# ---------------------------------------------------------------- a origem


def discover_partitions(
    location: Location, options: schema.TableOptions
) -> tuple[dict[str | None, Location], list[str]]:
    """As partições da pasta da tabela e as entradas fora do padrão.

    Numa tabela particionada, ``{valor: pasta}`` das pastas ``<coluna>=<valor>`` com a coluna
    do modelo; numa tabela sem partição, ``{None: pasta}``.
    """
    if options.partition_by is None:
        return {None: location}, []
    found: dict[str | None, Location] = {}
    skipped = []
    for entry in location.entries():
        match = PARTITION_FOLDER.fullmatch(entry.base_name)
        is_partition = bool(match) and match.group("column") == options.partition_by
        if is_partition and entry.type == pafs.FileType.Directory:
            found[match.group("value")] = location.child(entry.base_name)
        else:
            skipped.append(entry.base_name)
    return found, skipped


def entries_outside_the_model(source: Location, metadata: sa.MetaData) -> list[str]:
    """As entradas da raiz da origem que não são pasta de uma tabela do modelo."""
    names = metadata.tables
    return [entry.base_name for entry in source.entries() if entry.base_name not in names]


def first_partition_folder(folder: Location, options: schema.TableOptions) -> Location:
    """A pasta cujo rodapé dá as conversões: a primeira partição, ou a própria pasta."""
    found, _ = discover_partitions(folder, options)
    return next(iter(found.values()))


def conversions(folder: Location, table: sa.Table) -> tuple[str, ...]:
    """As conversões de tipo da origem para o contrato, lidas no rodapé do primeiro arquivo."""
    footer = pq.ParquetFile(first_parquet(folder), filesystem=folder.filesystem)
    physical = {}
    for index in range(len(footer.schema)):
        physical[footer.schema.column(index).name] = footer.schema.column(index).physical_type
    contract = schema.arrow_schema(table)
    found = []
    for field in footer.schema_arrow:
        if field.name not in contract.names or contract.field(field.name).type == field.type:
            continue
        origin = "INT96" if physical[field.name] == "INT96" else str(field.type)
        found.append(f"{field.name}: {origin} -> {contract.field(field.name).type}")
    return tuple(found)


# ---------------------------------------------------------------- a consulta da partição


def partition_query(folder: Location, table: sa.Table, value: str | None) -> str:
    """O ``SELECT`` do DuckDB que leva a partição ao contrato.

    A pasta inteira por ``read_parquet``, cada coluna em ``CAST`` para o tipo de ``sql_type`` e a
    coluna de partição com o valor do caminho; todo identificador entre aspas.
    """
    options = schema.table_options(table)
    selected = []
    for column in table.columns:
        name = schema.quoted(column.name)
        if column.name == options.partition_by:
            selected.append(f"'{value}' AS {name}")
            continue
        selected.append(f"CAST({name} AS {schema.sql_type(column, 'duckdb')}) AS {name}")
    return (
        f"SELECT {', '.join(selected)} "
        f"FROM read_parquet('{folder.uri}/*.parquet', hive_partitioning = false)"
    )


def ordered_select(query: str, columns: list[str], sort_key: tuple[str, ...]) -> str:
    """As colunas sobre a consulta da partição, na ordem da ``sort_key`` quando há uma."""
    names = ", ".join(schema.quoted(name) for name in columns)
    text = f"SELECT {names} FROM ({query})"
    if sort_key:
        text += " ORDER BY " + ", ".join(schema.quoted(name) for name in sort_key)
    return text


@dataclasses.dataclass(frozen=True)
class PartitionCheck:
    """O que a consulta de conferência achou numa partição."""

    problems: list[str]  # o que está fora do contrato; recusa a partição
    nonfinite_columns: tuple[str, ...]  # as colunas Double com NaN ou infinito


def double_columns(table: sa.Table) -> list[str]:
    """As colunas de ponto flutuante do modelo; o contrato só tem ``Double``."""
    return [column.name for column in table.columns if isinstance(column.type, sa.Float)]


def check_partition(
    con: duckdb.DuckDBPyConnection, query: str, table: sa.Table, value: str | None
) -> PartitionCheck:
    """A conferência da partição, numa consulta só.

    Fora do contrato: linhas com a coluna de origem da partição diferente do valor do caminho,
    quando o modelo declara ``partition_source``, nulos nas colunas ``NOT NULL`` e textos acima do
    ``String(n)`` em bytes, a medida do ``VARCHAR(n)`` do Redshift (``strlen`` no DuckDB conta
    bytes; ``octet_length`` só existe para ``BLOB``). E as colunas ``Double`` com ``NaN`` ou
    infinito, que o contrato aceita e que ficam sem mínimo e máximo na gravação.
    """
    options = schema.table_options(table)
    measures = []
    labels = []
    if options.partition_by and options.partition_source:
        partition = schema.quoted(options.partition_by)
        source = schema.quoted(options.partition_source)
        measures.append(f"count(*) FILTER (WHERE {partition} <> strftime({source}, '%Y-%m-%d'))")
        labels.append(f"linhas com {options.partition_source} diferente de {value}")
    for column in table.columns:
        name = schema.quoted(column.name)
        if not column.nullable and column.name != options.partition_by:
            measures.append(f"count(*) FILTER (WHERE {name} IS NULL)")
            labels.append(f"nulos na coluna NOT NULL {column.name}")
        if isinstance(column.type, sa.String) and column.type.length:
            measures.append(f"count(*) FILTER (WHERE strlen({name}) > {column.type.length})")
            labels.append(f"textos acima de String({column.type.length}) em {column.name}")
    doubles = double_columns(table)
    for name in doubles:
        measures.append(f"count(*) FILTER (WHERE NOT isfinite({schema.quoted(name)}))")
    if not measures:
        return PartitionCheck([], ())

    # As contagens do contrato vêm primeiro, na ordem de labels, e as dos não finitos depois.
    counts = con.execute(f"SELECT {', '.join(measures)} FROM ({query})").fetchone()
    contract_counts = counts[: len(labels)]
    nonfinite_counts = counts[len(labels) :]
    problems = [f"{count} {label}" for count, label in zip(contract_counts, labels) if count]
    nonfinite = tuple(name for name, count in zip(doubles, nonfinite_counts) if count)
    return PartitionCheck(problems, nonfinite)


# ---------------------------------------------------------------- a gravação


def create_table(
    destination: Location, table: sa.Table, storage_options: dict[str, str]
) -> DeltaTable:
    """A tabela Delta com o esquema do contrato, a partição, o nome, o comentário e as
    retenções; repetir a chamada não muda a versão."""
    options = schema.table_options(table)
    partition_by = [options.partition_by] if options.partition_by else None
    return DeltaTable.create(
        destination.uri,
        schema.delta_schema(table),
        partition_by=partition_by,
        name=table.name,
        description=table.comment,
        mode="ignore",
        configuration=RETENTION,
        storage_options=storage_options or None,
    )


def loaded_partitions(delta: DeltaTable, options: schema.TableOptions) -> set[str | None]:
    """Os valores já no log, pelas ações ``add``; numa tabela sem partição, ``{None}``
    quando há alguma ação."""
    actions = pa.table(delta.get_add_actions(flatten=True))
    if actions.num_rows == 0:
        return set()
    if options.partition_by is None:
        return {None}
    return set(actions.column(f"partition.{options.partition_by}").to_pylist())


def stat_converter(field_type: pa.DataType) -> Callable[[str], object] | None:
    """A conversão do texto do ``RETURN_STATS`` para o valor que o log guarda, ou ``None`` quando o
    tipo fica sem mínimo e máximo.

    Inteiro, data, ``Double`` e texto transcrevem exato, e são os tipos que a etapa 3 registra.
    ``decimal`` e ``timestamp`` ficam de fora: o log guarda o mínimo e o máximo como número JSON,
    e um máximo abaixo do valor real poda o arquivo que tem a linha, sem erro, nos dois leitores
    (``plan/POC.md``).

    Exemplo:

        stat_converter(pa.int64())("42")     # 42
        stat_converter(pa.decimal128(18, 2)) # None
    """
    if pa.types.is_integer(field_type):
        return int
    if pa.types.is_floating(field_type):
        return float
    if pa.types.is_date(field_type) or pa.types.is_string(field_type):
        return str
    return None


def delta_stats(
    count: int,
    stats: dict[str, dict[str, str]],
    table: sa.Table,
    columns_without_min_max: Collection[str] = (),
) -> str:
    """O JSON de estatísticas da ação: ``numRecords``, ``nullCount`` de toda coluna e o mínimo e o
    máximo dos tipos que ``stat_converter`` transcreve, a partir do texto do ``RETURN_STATS``; as
    colunas de ``columns_without_min_max`` ficam sem mínimo e máximo."""
    typed: dict[str, dict[str, object]] = {"minValues": {}, "maxValues": {}, "nullCount": {}}
    for field in schema.arrow_schema(table):
        column = stats.get(field.name)
        if column is None:
            continue
        typed["nullCount"][field.name] = int(column["null_count"])
        convert = stat_converter(field.type)
        if convert is None or "min" not in column or field.name in columns_without_min_max:
            continue
        typed["minValues"][field.name] = convert(column["min"])
        typed["maxValues"][field.name] = convert(column["max"])
    return json.dumps({"numRecords": count, **typed})


def copy_partition_file(
    con: duckdb.DuckDBPyConnection,
    destination: Location,
    table: sa.Table,
    value: str | None,
    query: str,
    sort: bool,
) -> tuple[str, dict[str, object]]:
    """``COPY ... RETURN_STATS`` da partição para um arquivo novo na pasta dela; devolve o
    caminho relativo à pasta da tabela, como o log o guarda, e a linha do ``RETURN_STATS``."""
    options = schema.table_options(table)
    # A coluna de partição fica fora do arquivo: no Delta o valor dela está no caminho e na
    # ação add, e o leitor a reconstrói de lá.
    columns = [column.name for column in table.columns if column.name != options.partition_by]
    relative = f"carga_inicial_{uuid.uuid4().hex}.parquet"
    folder = destination
    if options.partition_by:
        folder = destination.child(f"{options.partition_by}={value}")
        relative = f"{options.partition_by}={value}/{relative}"
    ensure_folder(folder)
    select = ordered_select(query, columns, options.sort_key if sort else ())
    cursor = con.execute(
        f"COPY ({select}) TO '{destination.uri}/{relative}' (FORMAT parquet, RETURN_STATS)"
    )
    written = dict(zip([column[0] for column in cursor.description], cursor.fetchone()))
    return relative, written


def register_partition(
    con: duckdb.DuckDBPyConnection,
    delta: DeltaTable,
    destination: Location,
    table: sa.Table,
    value: str | None,
    query: str,
    settings: Settings,
    nonfinite_columns: tuple[str, ...],
) -> int:
    """Modo ``register``: o arquivo do ``COPY`` entra no log com as estatísticas, sem o mínimo e o
    máximo de ``nonfinite_columns``, num commit ``overwrite`` da partição; devolve as linhas
    gravadas. O rodapé do DuckDB já sai sem mínimo e máximo no grupo de linhas com ``NaN``."""
    options = schema.table_options(table)
    relative, written = copy_partition_file(con, destination, table, value, query, settings.sort)
    stats = {name.strip('"'): column for name, column in written["column_statistics"].items()}
    action = AddAction(
        path=relative,
        size=written["file_size_bytes"],
        partition_values={options.partition_by: value} if options.partition_by else {},
        modification_time=int(time.time() * 1000),
        data_change=True,
        stats=delta_stats(written["count"], stats, table, nonfinite_columns),
    )
    partition_by = [options.partition_by] if options.partition_by else None
    partition_filters = [(options.partition_by, "=", value)] if options.partition_by else None
    delta.create_write_transaction(
        [action],
        mode="overwrite",
        schema=delta.schema(),
        partition_by=partition_by,
        partition_filters=partition_filters,
    )
    return written["count"]


def rewrite_partition(
    con: duckdb.DuckDBPyConnection,
    destination: Location,
    table: sa.Table,
    value: str | None,
    query: str,
    settings: Settings,
    nonfinite_columns: tuple[str, ...],
) -> int:
    """Modo ``rewrite``: o leitor da consulta passa por ``cast`` e ``write_deltalake`` substitui a
    partição, sem estatística de ``nonfinite_columns`` no rodapé nem no log; devolve as linhas
    gravadas."""
    options = schema.table_options(table)
    columns = [column.name for column in table.columns]
    rows = con.execute(f"SELECT count(*) FROM ({query})").fetchone()[0]
    select = ordered_select(query, columns, options.sort_key if settings.sort else ())
    # Nenhum outro comando na conexão até o leitor ser consumido: o comando seguinte o esvazia.
    reader = schema.cast(con.execute(select).to_arrow_reader(), table)
    predicate = None
    if options.partition_by:
        predicate = f"{schema.quoted(options.partition_by)} = '{value}'"
    write_deltalake(
        destination.uri,
        reader,
        mode="overwrite",
        predicate=predicate,
        writer_properties=writer_properties(nonfinite_columns),
        storage_options=settings.storage_options or None,
    )
    return rows


def writer_properties(columns_without_min_max: Collection[str]) -> WriterProperties | None:
    """As propriedades do escritor do delta-rs que desligam a estatística das colunas: o rodapé
    sai sem mínimo e máximo delas, e o log também, porque o delta-rs o copia do rodapé; ``None``
    mantém o padrão.

    Exemplo:

        writer_properties(["valor"])   # WriterProperties com statistics_enabled="NONE" em valor
        writer_properties([])          # None
    """
    if not columns_without_min_max:
        return None
    no_statistics = ColumnProperties(statistics_enabled="NONE")
    column_properties = {name: no_statistics for name in columns_without_min_max}
    return WriterProperties(column_properties=column_properties)


def peak_rss_mb() -> float:
    """O pico de memória residente do próprio processo até agora, em MB.

    No Linux, o ``VmHWM`` de ``/proc/self/status``, em KB: o ``ru_maxrss`` de um processo novo
    começa no pico do processo pai, e a medição roda cada variante num processo filho. No macOS,
    o ``ru_maxrss``, em bytes.
    """
    if sys.platform == "darwin":
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)
    status = Path("/proc/self/status").read_text()
    kilobytes = re.search(r"^VmHWM:\s+(\d+)", status, re.MULTILINE).group(1)
    return int(kilobytes) / 1024


def load_partition(
    con: duckdb.DuckDBPyConnection,
    delta: DeltaTable,
    destination: Location,
    table: sa.Table,
    value: str | None,
    source_folder: Location,
    settings: Settings,
) -> PartitionLoad:
    """Confere a partição contra o contrato e a grava no modo pedido."""
    started = time.perf_counter()
    query = partition_query(source_folder, table, value)
    check = check_partition(con, query, table, value)
    if check.problems:
        raise ContractError(f"{table.name} partição {value}: {'; '.join(check.problems)}")
    nonfinite = check.nonfinite_columns
    if settings.mode == "register":
        rows = register_partition(con, delta, destination, table, value, query, settings, nonfinite)
    else:
        rows = rewrite_partition(con, destination, table, value, query, settings, nonfinite)
    elapsed = time.perf_counter() - started
    return PartitionLoad(value, rows, elapsed, peak_rss_mb(), nonfinite)


def selected_values(
    found: dict[str | None, Location], partitions: tuple[str, ...]
) -> list[str | None]:
    """Os valores encontrados que a linha de comando pediu; todos, sem ``--partitions``."""
    if not partitions:
        return list(found)
    return [value for value in found if value in partitions]


def initial_load(
    con: duckdb.DuckDBPyConnection,
    table: sa.Table,
    source: Location,
    root: Location,
    settings: Settings,
) -> tuple[list[PartitionLoad], list[str]]:
    """Grava no Delta cada partição da tabela ainda fora do log; devolve o que gravou e as
    entradas da pasta da tabela fora do padrão."""
    options = schema.table_options(table)
    destination = root.child(table.name)
    delta = create_table(destination, table, settings.storage_options)
    found, skipped = discover_partitions(source.child(table.name), options)
    already = loaded_partitions(delta, options)
    wanted = selected_values(found, settings.partitions)
    print(f"{table.name}: {len(found)} partições na origem, {len(already)} no log")
    loaded = []
    # Cada commit resolve a versão no log do armazenamento, e não na versão que o objeto
    # `delta` abriu, que não reflete os próprios commits: uma partição por commit, sem reabrir.
    for value in wanted:
        if value in already:
            continue
        load = load_partition(con, delta, destination, table, value, found[value], settings)
        line = (
            f"  {value}: {load.rows} linhas em {load.seconds:.1f} s; "
            f"RSS máximo do processo {load.peak_rss_mb:.0f} MB"
        )
        if load.nonfinite_columns:
            line += f"; sem mínimo e máximo: {', '.join(load.nonfinite_columns)}"
        print(line)
        loaded.append(load)
    return loaded, skipped


# ---------------------------------------------------------------- a medição das variantes

# As variantes que a medição grava de cada partição: os dois modos, com e sem a ordem da sort_key.
VARIANTS = (("register", True), ("rewrite", True), ("register", False), ("rewrite", False))

# As falhas de uma variante que entram no relatório sem parar a execução: o processo filho morto
# (a falta de memória, por exemplo), o erro do DuckDB, do delta-rs e do armazenamento.
MEASUREMENT_ERRORS = (BrokenProcessPool, duckdb.Error, DeltaError, OSError, MemoryError)


def measure_variant(
    table: sa.Table,
    source_folder: Location,
    destination: Location,
    value: str | None,
    settings: Settings,
    uses_s3: bool,
    region: str | None,
) -> VariantMeasurement:
    """Grava a partição numa tabela descartável, no modo e na ordem de ``settings``, e mede o
    processo; roda num processo novo, e a base é a memória dele depois das importações e da
    conexão."""
    con = connect_duckdb(uses_s3, region)
    base = peak_rss_mb()
    delta = create_table(destination, table, settings.storage_options)
    load = load_partition(con, delta, destination, table, value, source_folder, settings)
    con.close()

    # Os arquivos e os bytes que a variante gravou, pelas ações add do log.
    written = DeltaTable(destination.uri, storage_options=settings.storage_options or None)
    actions = pa.table(written.get_add_actions(flatten=True))
    return VariantMeasurement(
        value=value,
        mode=settings.mode,
        sort=settings.sort,
        rows=load.rows,
        seconds=round(load.seconds, 3),
        base_mb=round(base),
        peak_mb=round(load.peak_rss_mb),
        files=actions.num_rows,
        bytes=sum(actions.column("size_bytes").to_pylist()),
    )


def remove_folder(location: Location) -> None:
    """Apaga a pasta e o que há nela, quando ela existe."""
    info = location.filesystem.get_file_info(location.path)
    if info.type != pafs.FileType.NotFound:
        location.filesystem.delete_dir(location.path)


def print_measurement(measurement: VariantMeasurement) -> None:
    """A linha de uma variante medida."""
    order = "ordenada" if measurement.sort else "sem ordem"
    label = f"  medição {measurement.value} {measurement.mode} {order}"
    if measurement.error:
        print(f"{label}: {measurement.error}")
        return
    print(
        f"{label}: {measurement.rows} linhas em {measurement.seconds:.1f} s; pico do processo "
        f"{measurement.peak_mb:.0f} MB sobre a base de {measurement.base_mb:.0f} MB; "
        f"{measurement.files} arquivo(s), {measurement.bytes / 2**20:.0f} MB"
    )


def measure_partition(
    table: sa.Table,
    source_folder: Location,
    scratch: Location,
    value: str | None,
    settings: Settings,
    uses_s3: bool,
    region: str | None,
) -> list[VariantMeasurement]:
    """Mede a gravação da partição em cada variante de ``VARIANTS``, cada uma num processo novo e
    numa tabela sob ``scratch`` apagada logo depois; a variante que falha entra com o erro."""
    context = multiprocessing.get_context("spawn")
    measurements = []
    for mode, sort in VARIANTS:
        variant = dataclasses.replace(settings, mode=mode, sort=sort)
        order = "ordenada" if sort else "sem_ordem"
        destination = scratch.child(f"{value}_{mode}_{order}")
        try:
            # Um processo por variante: o pico de memória de cada uma é só dela.
            with ProcessPoolExecutor(max_workers=1, mp_context=context) as executor:
                future = executor.submit(
                    measure_variant,
                    table, source_folder, destination, value, variant, uses_s3, region,
                )
                measurement = future.result()
        except MEASUREMENT_ERRORS as error:
            message = f"{type(error).__name__}: {error}"
            measurement = VariantMeasurement(value, mode, sort, error=message)
        finally:
            remove_folder(destination)
        print_measurement(measurement)
        measurements.append(measurement)
    return measurements


def measure_table(
    table: sa.Table,
    source: Location,
    root: Location,
    settings: Settings,
    uses_s3: bool,
    region: str | None,
) -> list[VariantMeasurement]:
    """Mede cada partição pedida de uma tabela particionada, esteja ela no log ou não, sob a pasta
    ``_medicao_<tabela>`` da raiz, apagada no fim; uma tabela sem partição não é medida."""
    options = schema.table_options(table)
    if options.partition_by is None:
        return []
    found, _ = discover_partitions(source.child(table.name), options)
    scratch = root.child(f"_medicao_{table.name}")
    measurements = []
    try:
        for value in selected_values(found, settings.partitions):
            measurements += measure_partition(
                table, found[value], scratch, value, settings, uses_s3, region
            )
    finally:
        remove_folder(scratch)
    return measurements


# ---------------------------------------------------------------- o relatório da carga


def source_relation(folder: Location, options: schema.TableOptions) -> str:
    """A leitura da tabela inteira na origem: as pastas Hive com o valor como texto, ou o único
    nível de arquivos numa tabela sem partição."""
    if options.partition_by is None:
        return f"read_parquet('{folder.uri}/*.parquet')"
    return (
        f"read_parquet('{folder.uri}/*/*.parquet', "
        "hive_partitioning = true, hive_types_autocast = false)"
    )


@dataclasses.dataclass(frozen=True)
class Totals:
    """Contagem, somas e não finitos de uma partição, num dos lados do relatório."""

    rows: int | None  # None quando a partição falta desse lado
    sums: dict[str, Decimal | None]
    nonfinite: dict[str, int]


def total_measures(sums: list[str], doubles: list[str]) -> list[str]:
    """As agregações de uma partição: a contagem, a soma de cada coluna de ``sums`` como
    ``DECIMAL(38, 6)`` e a contagem dos não finitos de cada coluna de ``doubles``.

    A soma de uma coluna ``Double`` corre só sobre os valores finitos: o ``CAST`` de um ``NaN`` ou
    de um infinito para ``DECIMAL`` falha com ``ConversionException`` (``plan/POC.md``).
    """
    measures = ["count(*)"]
    for name in sums:
        quoted = schema.quoted(name)
        decimal = f"CAST({quoted} AS DECIMAL(38, 6))"
        if name in doubles:
            measures.append(f"sum(CASE WHEN isfinite({quoted}) THEN {decimal} END)")
        else:
            measures.append(f"sum({decimal})")
    for name in doubles:
        measures.append(f"count(*) FILTER (WHERE NOT isfinite({schema.quoted(name)}))")
    return measures


def totals_from_row(row: tuple, sums: list[str], doubles: list[str]) -> Totals:
    """As medidas de uma linha da agregação, na ordem de ``total_measures``."""
    sum_values = row[1 : 1 + len(sums)]
    nonfinite_values = row[1 + len(sums) :]
    return Totals(row[0], dict(zip(sums, sum_values)), dict(zip(doubles, nonfinite_values)))


def aggregate(
    con: duckdb.DuckDBPyConnection,
    relation: str,
    partition_by: str | None,
    sums: list[str],
    doubles: list[str],
) -> dict[str | None, Totals]:
    """Contagem, somas e não finitos por partição de ``relation``: ``{valor: Totals}``."""
    measures = ", ".join(total_measures(sums, doubles))
    if partition_by is None:
        row = con.execute(f"SELECT {measures} FROM {relation}").fetchone()
        return {None: totals_from_row(row, sums, doubles)}
    key = schema.quoted(partition_by)
    rows = con.execute(
        f"SELECT {key}, {measures} FROM {relation} GROUP BY 1 ORDER BY 1"
    ).fetchall()
    return {row[0]: totals_from_row(row[1:], sums, doubles) for row in rows}


def load_report(
    con: duckdb.DuckDBPyConnection,
    table: sa.Table,
    source: Location,
    root: Location,
    loaded: list[PartitionLoad],
    skipped: list[str],
    measurements: list[VariantMeasurement] | None = None,
) -> LoadReport:
    """Contagem e somas por partição na origem e no Delta, as colunas ``Double`` e ``Numeric``
    somadas como ``DECIMAL(38, 6)`` de cada valor, porque a soma em ponto flutuante depende da
    ordem e os valores são os mesmos dos dois lados; as ``Double`` só nos valores finitos, com os
    não finitos contados à parte. ``measurements`` entra no relatório como veio."""
    options = schema.table_options(table)
    sums = [column.name for column in table.columns if isinstance(column.type, sa.Numeric)]
    doubles = double_columns(table)
    folder = source.child(table.name)
    origin_relation = source_relation(folder, options)
    origin = aggregate(con, origin_relation, options.partition_by, sums, doubles)
    delta_relation = f"delta_scan('{root.child(table.name).uri}')"
    written = aggregate(con, delta_relation, options.partition_by, sums, doubles)
    missing = Totals(rows=None, sums={}, nonfinite={})
    partitions = []
    for value in sorted(set(origin) | set(written), key=str):
        before = origin.get(value, missing)
        after = written.get(value, missing)
        partitions.append(
            PartitionReport(
                value=value,
                source_rows=before.rows,
                delta_rows=after.rows,
                source_sums=before.sums,
                delta_sums=after.sums,
                source_nonfinite=before.nonfinite,
                delta_nonfinite=after.nonfinite,
            )
        )
    return LoadReport(
        table=table.name,
        partitions=tuple(partitions),
        loaded=tuple(loaded),
        skipped=tuple(skipped),
        conversions=conversions(first_partition_folder(folder, options), table),
        measurements=tuple(measurements or ()),
    )


def print_report(report: LoadReport) -> None:
    """As linhas do relatório de uma tabela, depois das partições gravadas."""
    for partition in report.partitions:
        if not partition.matches:
            print(
                f"  DIFERENÇA em {partition.value}: origem {partition.source_rows} linhas "
                f"{partition.source_sums} não finitos {partition.source_nonfinite}, Delta "
                f"{partition.delta_rows} linhas {partition.delta_sums} não finitos "
                f"{partition.delta_nonfinite}"
            )
    verdict = "contagens e somas iguais" if report.matches else "com diferenças"
    print(f"  relatório: {len(report.partitions)} partições conferidas, {verdict}")
    if report.conversions:
        print(f"  conversões: {', '.join(report.conversions)}")
    for entry in report.skipped:
        print(f"  ignorado fora do padrão: {entry}")


def write_report(
    path: str, reports: list[LoadReport], outside: list[str], environment: dict[str, object]
) -> None:
    """O relatório da execução em JSON, com as somas como texto e o ambiente que a mediu."""
    document = {
        "environment": environment,
        "tables": [dataclasses.asdict(report) | {"matches": report.matches} for report in reports],
        "outside_model": outside,
    }
    Path(path).write_text(json.dumps(document, indent=2, ensure_ascii=False, default=str))


# ---------------------------------------------------------------- a linha de comando


def resolve_metadata(spec: str) -> sa.MetaData:
    """O ``MetaData`` de ``modulo:atributo``, como ``client_model:Base.metadata``."""
    module_name, _, attribute = spec.partition(":")
    if not module_name or not attribute:
        raise argparse.ArgumentTypeError(f"esperado modulo:atributo, recebido {spec!r}")
    target = importlib.import_module(module_name)
    for name in attribute.split("."):
        target = getattr(target, name)
    if not isinstance(target, sa.MetaData):
        raise argparse.ArgumentTypeError(f"{spec} não é um sqlalchemy.MetaData")
    return target


def tables_in_load_order(metadata: sa.MetaData, names: list[str] | None) -> list[sa.Table]:
    """As tabelas do modelo na ordem da carga: as sem partição na ordem do modelo, depois as
    particionadas na ordem do modelo; ``names`` filtra."""
    tables = list(metadata.tables.values())
    if names:
        tables = [table for table in tables if table.name in names]
    unpartitioned = []
    partitioned = []
    for table in tables:
        if schema.table_options(table).partition_by is None:
            unpartitioned.append(table)
        else:
            partitioned.append(table)
    return unpartitioned + partitioned


def extension_directory() -> str | None:
    """A pasta de extensões do DuckDB: ``SERIALIZE_DB_DUCKDB_EXTENSIONS``, senão ``.duckdb/`` na
    raiz do repositório quando existe, senão a padrão do DuckDB."""
    configured = os.environ.get("SERIALIZE_DB_DUCKDB_EXTENSIONS")
    if configured:
        return configured
    local = Path(__file__).resolve().parent.parent / ".duckdb"
    return str(local) if local.is_dir() else None


def connect_duckdb(uses_s3: bool, region: str | None) -> duckdb.DuckDBPyConnection:
    """A conexão com as extensões da pasta configurada, sem instalação automática; com o S3,
    ``httpfs``, ``aws`` e o secret ``credential_chain`` da região."""
    config: dict[str, object] = {
        "autoinstall_known_extensions": False,
        "autoload_known_extensions": False,
    }
    directory = extension_directory()
    if directory:
        config["extension_directory"] = directory
    con = duckdb.connect(config=config)
    con.execute("LOAD delta")
    if uses_s3:
        con.execute("LOAD httpfs")
        con.execute("LOAD aws")
        con.execute(
            f"CREATE SECRET migracao (TYPE s3, PROVIDER credential_chain, REGION '{region}')"
        )
    return con


def duckdb_settings(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """As configurações do DuckDB que a medição da partição lê."""
    rows = con.execute(
        "SELECT name, value FROM duckdb_settings() "
        "WHERE name IN ('threads', 'memory_limit', 'temp_directory') ORDER BY name"
    ).fetchall()
    return dict(rows)


def print_duckdb_settings(con: duckdb.DuckDBPyConnection) -> None:
    """A versão do DuckDB e as configurações que a medição da partição lê."""
    settings = ", ".join(f"{name} {value}" for name, value in duckdb_settings(con).items())
    print(f"DuckDB {duckdb.__version__}: {settings}")


def describe_environment(
    con: duckdb.DuckDBPyConnection, arguments: argparse.Namespace
) -> dict[str, object]:
    """A máquina, as versões, as configurações do DuckDB e os parâmetros da execução, que o
    relatório leva para ler a medição: a memória e os núcleos mudam com a instância."""
    physical_memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    packages = ("duckdb", "deltalake", "pyarrow")
    return {
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpus": os.cpu_count(),
        "memory_total_mb": round(physical_memory / 2**20),
        "packages": {name: importlib.metadata.version(name) for name in packages},
        "duckdb_settings": duckdb_settings(con),
        "arguments": {
            "source": arguments.source,
            "root": arguments.root,
            "tables": arguments.tables,
            "partitions": arguments.partitions,
            "mode": arguments.mode,
            "sort": arguments.sort,
            "measure": arguments.measure,
        },
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Migra a base Parquet particionada de origem para tabelas Delta, uma "
        "partição por commit, e confere contagens e somas."
    )
    parser.add_argument(
        "--metadata",
        required=True,
        type=resolve_metadata,
        help="o MetaData do modelo, como client_model:Base.metadata",
    )
    parser.add_argument(
        "--source", required=True, help="a raiz da origem, pasta local ou s3://bucket/prefixo"
    )
    parser.add_argument(
        "--root",
        required=True,
        help="a raiz das tabelas Delta, pasta local ou s3://bucket/prefixo",
    )
    parser.add_argument(
        "--tables", nargs="+", metavar="TABELA", help="só estas tabelas do modelo"
    )
    parser.add_argument(
        "--partitions",
        nargs="+",
        metavar="AAAA-MM-DD",
        default=(),
        help="só estas partições; as tabelas sem partição ficam de fora",
    )
    parser.add_argument(
        "--mode",
        choices=("register", "rewrite"),
        default="register",
        help="register: o COPY do DuckDB registrado no log; rewrite: write_deltalake",
    )
    parser.add_argument(
        "--sort",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="ordenar cada partição pela sort_key do modelo (padrão)",
    )
    parser.add_argument(
        "--measure",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="antes da carga, medir cada partição das tabelas particionadas em register e rewrite, "
        "com e sem a ordem da sort_key, cada variante num processo novo e numa tabela descartável "
        "sob a raiz (padrão)",
    )
    parser.add_argument(
        "--report", metavar="ARQUIVO.json", help="grava o relatório da execução em JSON"
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv)
    metadata: sa.MetaData = arguments.metadata
    problems = schema.check_models(metadata)
    if problems:
        print("modelo fora do contrato:", *problems, sep="\n  ", file=sys.stderr)
        return 2
    unknown = set(arguments.tables or []) - set(metadata.tables)
    if unknown:
        parser.error(f"tabelas fora do modelo: {', '.join(sorted(unknown))}")
    source, root = open_location(arguments.source), open_location(arguments.root)
    uses_s3 = source.uri.startswith("s3://") or root.uri.startswith("s3://")
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if uses_s3 and not region:
        parser.error("o S3 precisa da região em AWS_REGION ou AWS_DEFAULT_REGION")
    settings = Settings(
        mode=arguments.mode,
        sort=arguments.sort,
        partitions=tuple(arguments.partitions),
        storage_options={"AWS_REGION": region} if uses_s3 else {},
    )
    con = connect_duckdb(uses_s3, region)
    print_duckdb_settings(con)
    environment = describe_environment(con, arguments)
    print(f"{environment['cpus']} CPUs, {environment['memory_total_mb']} MB de memória")
    reports = []
    try:
        for table in tables_in_load_order(metadata, arguments.tables):
            # A medição vem antes da carga, com o processo principal ainda sem consulta pesada.
            measurements = []
            if arguments.measure:
                measurements = measure_table(table, source, root, settings, uses_s3, region)
            loaded, skipped = initial_load(con, table, source, root, settings)
            report = load_report(con, table, source, root, loaded, skipped, measurements)
            print_report(report)
            reports.append(report)
    except ContractError as error:
        print(f"ContractError: {error}", file=sys.stderr)
        return 1
    finally:
        con.close()
    outside = entries_outside_the_model(source, metadata)
    if outside:
        print(f"fora do modelo: {', '.join(outside)}")
    if arguments.report:
        write_report(arguments.report, reports, outside, environment)
    matches = all(report.matches for report in reports)
    print(
        f"{len(reports)} tabelas conferidas, "
        f"{'contagens e somas iguais' if matches else 'com diferenças'}"
    )
    return 0 if matches else 1


if __name__ == "__main__":
    sys.exit(main())
