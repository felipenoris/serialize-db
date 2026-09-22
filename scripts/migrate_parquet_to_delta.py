"""A migração adiantada: a base Parquet de origem vira tabelas Delta, uma partição por commit.

O script é o rascunho da etapa 7 (``plan/PLAN-STAGE-7.md``, seção "A migração adiantada")
promovido a ferramenta, sobre ``serialize_db.schema``, o ``deltalake`` e o DuckDB, sem as etapas
3 e 4; quando elas chegarem, o corpo vira ``serialize_db.load.initial_load``. Para cada tabela do
modelo, as sem partição primeiro e as particionadas depois, na ordem do modelo:

- ``discover_partitions`` lista a pasta da tabela na origem: as pastas ``<coluna>=<AAAA-MM-DD>``
  viram as partições, e o que não segue o padrão vai para o relatório;
- ``partition_query`` monta o ``SELECT`` do DuckDB que leva a partição ao contrato: a pasta
  inteira por ``read_parquet``, cada coluna em ``CAST`` para o tipo de ``sql_type`` (as chaves de
  ``int32`` a ``BIGINT``, o ``timestamp`` ``INT96`` a ``TIMESTAMP``, truncado a microssegundos),
  o valor do caminho na coluna de partição;
- a carga confere numa consulta que a coluna de origem da partição (``data``, ``data_base``) é
  igual ao valor do caminho em toda linha, que nenhuma coluna ``NOT NULL`` tem nulo e que nenhum
  texto passa do ``String(n)`` em bytes, e grava a partição no modo pedido: ``register`` roda
  ``COPY ... TO`` na pasta da tabela com ``RETURN_STATS`` e registra o arquivo no log por
  ``create_write_transaction``, com o ``nullCount`` de toda coluna e o mínimo e o máximo das
  inteiras, de data, ``Double`` e texto; ``rewrite`` passa o leitor da consulta por ``cast`` e
  ``write_deltalake``. Nos dois modos as linhas saem na ordem da ``sort_key`` do modelo, salvo
  ``--no-sort``;
- a retomada pula as partições já no log: a segunda execução não grava nada;
- ``load_report`` compara contagem e somas por partição entre a origem e o Delta, as colunas
  ``Double`` e ``Numeric`` somadas como ``DECIMAL(38, 6)``, e o script sai com 1 quando diferem.

Uma partição fora do contrato interrompe a execução sem commit, com a tabela, a partição e a
coluna na mensagem; a execução seguinte recomeça dela. As tabelas da origem fora do modelo
(``alembic_version``, ``meta_update_status``) e o ``schema.json`` da raiz ficam de fora e entram
no relatório. A auditoria de chaves estrangeiras é da etapa 7, não daqui.

Cada partição imprime as linhas, o tempo e o RSS máximo do processo até ali: é a medição de
``plan/OPEN_QUESTIONS.md`` sobre a partição de ``cad_lancamentos``, feita com ``--tables
cad_lancamentos --partitions <valor>`` nos dois modos e com ``--no-sort``.

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
import json
import os
import re
import resource
import sys
import time
import uuid
from collections.abc import Callable
from decimal import Decimal
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import sqlalchemy as sa
from deltalake import DeltaTable, write_deltalake
from deltalake.transaction import AddAction

from serialize_db import schema
from serialize_db.errors import ContractError

PARTITION_FOLDER = re.compile(r"(?P<column>[a-z_]+)=(?P<value>\d{4}-\d{2}-\d{2})")

# As retenções que a etapa 3 fixa em create_table: o log legível por dez anos e os arquivos
# removidos guardados por 400 dias, a janela em que toda versão continua legível.
RETENTION = {
    "delta.logRetentionDuration": "interval 3650 days",
    "delta.deletedFileRetentionDuration": "interval 400 days",
}

# ---------------------------------------------------------------- o relatório


@dataclasses.dataclass(frozen=True)
class PartitionReport:
    """Contagem e somas de uma partição, na origem e no Delta."""

    value: str | None
    source_rows: int | None
    delta_rows: int | None
    source_sums: dict[str, Decimal | None]
    delta_sums: dict[str, Decimal | None]

    @property
    def matches(self) -> bool:
        return self.source_rows == self.delta_rows and self.source_sums == self.delta_sums


@dataclasses.dataclass(frozen=True)
class PartitionLoad:
    """Uma partição gravada agora: linhas, tempo e o RSS máximo do processo até ali."""

    value: str | None
    rows: int
    seconds: float
    peak_rss_mb: float


@dataclasses.dataclass(frozen=True)
class LoadReport:
    """O relatório de uma tabela: as partições conferidas, o que foi gravado agora, as entradas
    fora do padrão e as conversões de tipo da origem para o contrato."""

    table: str
    partitions: tuple[PartitionReport, ...]
    loaded: tuple[PartitionLoad, ...]
    skipped: tuple[str, ...]
    conversions: tuple[str, ...]

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

    Numa tabela particionada, ``{valor: pasta}`` das pastas ``<coluna>=<AAAA-MM-DD>`` com a
    coluna do modelo; numa tabela sem partição, ``{None: pasta}``.
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


def contract_problems(
    con: duckdb.DuckDBPyConnection, query: str, table: sa.Table, value: str | None
) -> list[str]:
    """O que a partição tem fora do contrato, numa consulta só.

    Linhas com a coluna de origem da partição diferente do valor do caminho, nulos nas colunas
    ``NOT NULL`` e textos acima do ``String(n)`` em bytes, a medida do ``VARCHAR(n)`` do Redshift
    (``strlen`` no DuckDB conta bytes; ``octet_length`` só existe para ``BLOB``).
    """
    options = schema.table_options(table)
    measures = []
    labels = []
    if options.partition_by:
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
    if not measures:
        return []
    counts = con.execute(f"SELECT {', '.join(measures)} FROM ({query})").fetchone()
    return [f"{count} {label}" for count, label in zip(counts, labels) if count]


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

    Inteiro, data, ``Double`` e texto transcrevem exato, e são os tipos que a etapa 3 registra
    (decisão do usuário de 2026-09-22). ``decimal`` e ``timestamp`` ficam de fora: o log guarda o
    mínimo e o máximo como número JSON, e um máximo abaixo do valor real poda o arquivo que tem a
    linha, sem erro, nos dois leitores (``plan/POC.md``).

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


def delta_stats(count: int, stats: dict[str, dict[str, str]], table: sa.Table) -> str:
    """O JSON de estatísticas da ação: ``numRecords``, ``nullCount`` de toda coluna e o mínimo e o
    máximo dos tipos que ``stat_converter`` transcreve, a partir do texto do ``RETURN_STATS``."""
    typed: dict[str, dict[str, object]] = {"minValues": {}, "maxValues": {}, "nullCount": {}}
    for field in schema.arrow_schema(table):
        column = stats.get(field.name)
        if column is None:
            continue
        typed["nullCount"][field.name] = int(column["null_count"])
        convert = stat_converter(field.type)
        if convert is None or "min" not in column:
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
) -> int:
    """Modo ``register``: o arquivo do ``COPY`` entra no log com as estatísticas, num commit
    ``overwrite`` da partição; devolve as linhas gravadas."""
    options = schema.table_options(table)
    relative, written = copy_partition_file(con, destination, table, value, query, settings.sort)
    stats = {name.strip('"'): column for name, column in written["column_statistics"].items()}
    action = AddAction(
        path=relative,
        size=written["file_size_bytes"],
        partition_values={options.partition_by: value} if options.partition_by else {},
        modification_time=int(time.time() * 1000),
        data_change=True,
        stats=delta_stats(written["count"], stats, table),
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
) -> int:
    """Modo ``rewrite``: o leitor da consulta passa por ``cast`` e ``write_deltalake`` substitui a
    partição; devolve as linhas gravadas."""
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
        storage_options=settings.storage_options or None,
    )
    return rows


def peak_rss_mb() -> float:
    """O RSS máximo do processo até agora, em MB: o Linux o mede em KB e o macOS em bytes."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
    return peak / divisor


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
    problems = contract_problems(con, query, table, value)
    if problems:
        raise ContractError(f"{table.name} partição {value}: {'; '.join(problems)}")
    if settings.mode == "register":
        rows = register_partition(con, delta, destination, table, value, query, settings)
    else:
        rows = rewrite_partition(con, destination, table, value, query, settings)
    return PartitionLoad(value, rows, time.perf_counter() - started, peak_rss_mb())


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
        print(
            f"  {value}: {load.rows} linhas em {load.seconds:.1f} s; "
            f"RSS máximo do processo {load.peak_rss_mb:.0f} MB"
        )
        loaded.append(load)
    return loaded, skipped


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


def aggregate(
    con: duckdb.DuckDBPyConnection, relation: str, partition_by: str | None, sums: list[str]
) -> dict[str | None, tuple[int, dict[str, Decimal | None]]]:
    """Contagem e somas por partição de ``relation``: ``{valor: (linhas, {coluna: soma})}``."""
    measures = ["count(*)"]
    for name in sums:
        measures.append(f"sum(CAST({schema.quoted(name)} AS DECIMAL(38, 6)))")
    if partition_by is None:
        row = con.execute(f"SELECT {', '.join(measures)} FROM {relation}").fetchone()
        return {None: (row[0], dict(zip(sums, row[1:])))}
    key = schema.quoted(partition_by)
    rows = con.execute(
        f"SELECT {key}, {', '.join(measures)} FROM {relation} GROUP BY 1 ORDER BY 1"
    ).fetchall()
    return {row[0]: (row[1], dict(zip(sums, row[2:]))) for row in rows}


def load_report(
    con: duckdb.DuckDBPyConnection,
    table: sa.Table,
    source: Location,
    root: Location,
    loaded: list[PartitionLoad],
    skipped: list[str],
) -> LoadReport:
    """Contagem e somas por partição na origem e no Delta, as colunas ``Double`` e ``Numeric``
    somadas como ``DECIMAL(38, 6)`` de cada valor, porque a soma em ponto flutuante depende da
    ordem e os valores são os mesmos dos dois lados."""
    options = schema.table_options(table)
    sums = [column.name for column in table.columns if isinstance(column.type, sa.Numeric)]
    folder = source.child(table.name)
    origin = aggregate(con, source_relation(folder, options), options.partition_by, sums)
    delta_relation = f"delta_scan('{root.child(table.name).uri}')"
    written = aggregate(con, delta_relation, options.partition_by, sums)
    partitions = []
    for value in sorted(set(origin) | set(written), key=str):
        source_rows, source_sums = origin.get(value, (None, {}))
        delta_rows, delta_sums = written.get(value, (None, {}))
        partitions.append(PartitionReport(value, source_rows, delta_rows, source_sums, delta_sums))
    return LoadReport(
        table=table.name,
        partitions=tuple(partitions),
        loaded=tuple(loaded),
        skipped=tuple(skipped),
        conversions=conversions(first_partition_folder(folder, options), table),
    )


def print_report(report: LoadReport) -> None:
    """As linhas do relatório de uma tabela, depois das partições gravadas."""
    for partition in report.partitions:
        if not partition.matches:
            print(
                f"  DIFERENÇA em {partition.value}: origem {partition.source_rows} linhas "
                f"{partition.source_sums}, Delta {partition.delta_rows} linhas "
                f"{partition.delta_sums}"
            )
    verdict = "contagens e somas iguais" if report.matches else "com diferenças"
    print(f"  relatório: {len(report.partitions)} partições conferidas, {verdict}")
    if report.conversions:
        print(f"  conversões: {', '.join(report.conversions)}")
    for entry in report.skipped:
        print(f"  ignorado fora do padrão: {entry}")


def write_report(path: str, reports: list[LoadReport], outside: list[str]) -> None:
    """O relatório da execução em JSON, com as somas como texto."""
    document = {
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


def print_duckdb_settings(con: duckdb.DuckDBPyConnection) -> None:
    """A versão do DuckDB e as configurações que a medição da partição lê."""
    rows = con.execute(
        "SELECT name, value FROM duckdb_settings() "
        "WHERE name IN ('threads', 'memory_limit', 'temp_directory') ORDER BY name"
    ).fetchall()
    settings = ", ".join(f"{name} {value}" for name, value in rows)
    print(f"DuckDB {duckdb.__version__}: {settings}")


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
    reports = []
    try:
        for table in tables_in_load_order(metadata, arguments.tables):
            loaded, skipped = initial_load(con, table, source, root, settings)
            report = load_report(con, table, source, root, loaded, skipped)
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
        write_report(arguments.report, reports, outside)
    matches = all(report.matches for report in reports)
    print(
        f"{len(reports)} tabelas conferidas, "
        f"{'contagens e somas iguais' if matches else 'com diferenças'}"
    )
    return 0 if matches else 1


if __name__ == "__main__":
    sys.exit(main())
