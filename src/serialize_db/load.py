"""A carga inicial: a base Parquet de origem vira tabelas Delta, uma partição por commit.

A origem é a base atual em Parquet: uma pasta por tabela sob a raiz, as tabelas particionadas em
pastas Hive ``<coluna>=<valor>/`` com vários ``chunk_<n>.parquet`` cada, as sem partição com os
arquivos na raiz da tabela (``tests/source_db_projetado.py`` reproduz a estrutura lida nas duas
bases reais). A carga só lê a origem, e a raiz Delta é a de ``Database``: cada tabela vai para
``<raiz>/<ambiente>/<tabela>``.

``initial_load`` cria a tabela Delta do contrato, descobre as partições da pasta da tabela, pula as
que já estão no log e, para cada uma das outras, num motor DuckDB próprio da chamada, aberto com os
limites lidos do ambiente e fechado no fim: lê a pasta inteira por ``read_parquet`` sem
``hive_partitioning`` (que converteria ``data_str`` a ``DATE``), leva cada coluna ao tipo do
contrato por ``CAST`` (as chaves de ``int32`` a ``BIGINT``, o ``timestamp`` de ``INT96`` truncado a
microssegundos) e põe o valor do caminho na coluna de partição; confere numa consulta que a coluna
de origem da partição (``data``, ``data_base``) é igual ao valor do caminho em toda linha, que
nenhuma coluna ``NOT NULL`` tem nulo e que nenhum texto passa de ``String(n)`` em bytes, e conta os
valores não finitos de cada coluna ``Double``; grava a partição na ordem da ``sort_key`` por
``COPY ... RETURN_STATS`` num arquivo novo da pasta dela e o registra por
``serialize_db.delta.register_files``, com as conferências do rodapé, a releitura e as colunas não
finitas sem mínimo e máximo (issue #59). Uma partição fora do contrato é ``ContractError`` antes de
qualquer gravação, com a tabela, a partição e a coluna; a chamada seguinte recomeça dela. Um valor
que não converte para o tipo do contrato, ou uma coluna do contrato ausente dos arquivos, falha no
``COPY`` com o erro do DuckDB, também sem commit. As
entradas da pasta da tabela fora do padrão, e as da raiz fora do modelo (``alembic_version``,
``meta_update_status``, ``schema.json``), ficam fora da carga e entram no relatório.

``load_report`` compara contagem e somas por partição entre a origem e o Delta: as colunas
``Numeric`` somadas como ``DECIMAL(38, 6)`` e as ``Double`` da mesma forma só nos valores finitos,
com os não finitos contados à parte, porque a soma em ponto flutuante depende da ordem e o ``CAST``
de um ``NaN`` ou de um infinito para ``DECIMAL`` falha. A carga só termina quando ``matches`` é
verdadeiro; ``serialize-db load`` roda as duas e sai com 1 na diferença.

Exemplo:

.. code-block:: python

    from serialize_db import load
    from serialize_db.execution import Database

    db = Database("s3://bucket/projeto/delta", "prd", Base.metadata)
    for table in load.load_order(db.tables()):
        load.initial_load(db, table, "s3://bucket/projeto/db_projetado")
        report = load.load_report(db, table, "s3://bucket/projeto/db_projetado")
        assert report.matches, report
"""

from __future__ import annotations

import dataclasses
import logging
import re
import time
import uuid
from collections.abc import Mapping, Sequence
from decimal import Decimal

import duckdb
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import sqlalchemy as sa

from serialize_db import delta
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.errors import ContractError
from serialize_db.execution import Database
from serialize_db.schema import (
    PARTITION_VALUE,
    arrow_schema,
    double_columns,
    literal,
    quoted,
    sql_type,
    table_options,
)
from serialize_db.storage import Storage

__all__ = [
    "LoadReport",
    "PartitionReport",
    "discover_partitions",
    "initial_load",
    "load_order",
    "load_report",
    "partition_query",
]

log = logging.getLogger("serialize_db.load")

# A pasta de uma partição na origem: a coluna de partição e um valor da regra da partição.
_PARTITION_FOLDER = re.compile(rf"(?P<column>[A-Za-z_][A-Za-z0-9_]*)=(?P<value>{PARTITION_VALUE})")


@dataclasses.dataclass(frozen=True)
class PartitionReport:
    """Contagem, somas e não finitos de uma partição, na origem e no Delta; ``None`` nas linhas
    do lado em que a partição falta.

    Exemplo:

    .. code-block:: python

        report.partitions[0].matches   # True quando contagem, somas e não finitos coincidem
    """

    value: str | None
    """O valor da partição; ``None`` na tabela sem partição."""
    source_rows: int | None
    """As linhas da partição na origem."""
    delta_rows: int | None
    """As linhas da partição no Delta."""
    source_sums: Mapping[str, Decimal | None]
    """``{coluna: soma}`` das colunas ``Numeric`` e ``Double`` na origem, como ``DECIMAL(38, 6)``
    e só dos valores finitos numa ``Double``; ``None`` na coluna sem valor a somar, e vazio do lado
    em que a partição falta."""
    delta_sums: Mapping[str, Decimal | None]
    """As somas no Delta, na forma de ``source_sums``."""
    source_nonfinite: Mapping[str, int]
    """``{coluna: contagem}`` dos ``NaN`` e infinitos de cada coluna ``Double`` na origem; vazio do
    lado em que a partição falta."""
    delta_nonfinite: Mapping[str, int]
    """Os não finitos no Delta, na forma de ``source_nonfinite``."""

    @property
    def matches(self) -> bool:
        """Se contagem, somas e não finitos coincidem nos dois lados."""
        same_rows = self.source_rows == self.delta_rows
        same_sums = self.source_sums == self.delta_sums
        return same_rows and same_sums and self.source_nonfinite == self.delta_nonfinite


@dataclasses.dataclass(frozen=True)
class LoadReport:
    """O relatório de uma tabela: as partições conferidas, as entradas da pasta fora do padrão e
    as conversões de tipo da origem para o contrato.

    Exemplo:

    .. code-block:: python

        report = load_report(db, Lancamento.__table__, "s3://bucket/projeto/db_projetado")
        report.matches       # True
        report.conversions   # ("id_lancamento: int32 -> int64", "timestamp: INT96 -> ...")
    """

    table: str
    """O nome da tabela."""
    partitions: tuple[PartitionReport, ...]
    """Uma conferência por partição presente na origem ou no Delta, em ordem de texto do valor."""
    skipped: tuple[str, ...]
    """As entradas da pasta da tabela na origem que não são pasta de partição; vazio numa tabela
    sem partição."""
    conversions: tuple[str, ...]
    """Uma frase por coluna cujo tipo no rodapé do primeiro arquivo da tabela na origem difere do
    contrato, como ``"carimbo: INT96 -> timestamp[us]"``."""

    @property
    def matches(self) -> bool:
        """Se toda partição coincide nos dois lados."""
        return all(partition.matches for partition in self.partitions)


# ---------------------------------------------------------------- a origem


def _entries(storage: Storage, prefix: str) -> list[pafs.FileInfo]:
    """As entradas diretas de uma pasta, em ordem de nome; a pasta ausente dá a lista vazia."""
    path = f"{storage.path}/{prefix}" if prefix else storage.path
    selector = pafs.FileSelector(path, allow_not_found=True)
    found = storage.filesystem.get_file_info(selector)
    return sorted(found, key=_base_name)


def _base_name(info: pafs.FileInfo) -> str:
    """O nome da entrada, a chave da ordenação de ``_entries``."""
    return info.base_name


def _table_folder(storage: Storage, table: sa.Table) -> str:
    """A pasta da tabela na origem, relativa à raiz dela; ausente é ``FileNotFoundError``."""
    if not storage.exists(table.name):
        raise FileNotFoundError(f"{storage.uri_of(table.name)}: a pasta da tabela não existe na "
                                "origem")
    return table.name


def discover_partitions(
    source: str, table: sa.Table
) -> tuple[dict[str | None, str], tuple[str, ...]]:
    """As partições da pasta da tabela na origem e as entradas fora do padrão.

    Exemplo:

    .. code-block:: python

        found, skipped = discover_partitions("/dados/db_projetado", Operacao.__table__)
        found      # {"2026-02-28": "/dados/db_projetado/cad_operacoes/data_str=2026-02-28", ...}
        skipped    # ("notas.txt",)

    :param source: a raiz da base Parquet de origem, com uma pasta por tabela; uma pasta local,
        ``file://`` ou ``s3://``.
    :param table: a tabela do modelo.
    :return: numa tabela particionada, ``{valor: URI da pasta}`` das pastas ``<coluna>=<valor>``
        com a coluna de partição do modelo e um valor da regra da partição, em ordem de nome, e o
        resto da pasta na segunda tupla; numa tabela sem partição,
        ``{None: URI da pasta da tabela}`` e a tupla vazia.
    :raises FileNotFoundError: a pasta da tabela ausente na origem.
    :raises ValueError: ``source`` no S3 sem região, ou em outro esquema.
    """
    storage = Storage.for_uri(source)
    folder = _table_folder(storage, table)
    partition_by = table_options(table).partition_by
    if partition_by is None:
        return {None: storage.uri_of(folder)}, ()
    found: dict[str | None, str] = {}
    skipped = []
    for entry in _entries(storage, folder):
        match = _PARTITION_FOLDER.fullmatch(entry.base_name)
        is_partition = match is not None and match.group("column") == partition_by
        if is_partition and entry.type == pafs.FileType.Directory:
            found[match.group("value")] = storage.uri_of(storage.join(folder, entry.base_name))
        else:
            skipped.append(entry.base_name)
    return found, tuple(skipped)


def entries_outside_the_model(source: str, metadata: sa.MetaData) -> tuple[str, ...]:
    """As entradas da raiz da origem que não são pasta de uma tabela do modelo; protegida, para a
    linha de comando e o script de migração."""
    storage = Storage.for_uri(source)
    names = metadata.tables
    outside = [entry.base_name for entry in _entries(storage, "") if entry.base_name not in names]
    return tuple(outside)


def load_order(tables: Sequence[sa.Table]) -> list[sa.Table]:
    """As tabelas na ordem da carga.

    Exemplo:

    .. code-block:: python

        [table.name for table in load_order(db.tables())][-1]   # "cad_lancamentos"

    :param tables: as tabelas do modelo.
    :return: as sem partição na ordem dada, depois as particionadas na ordem dada.
    """
    unpartitioned = []
    partitioned = []
    for table in tables:
        if table_options(table).partition_by is None:
            unpartitioned.append(table)
        else:
            partitioned.append(table)
    return unpartitioned + partitioned


# ---------------------------------------------------------------- a consulta da partição


def partition_query(folder: str, table: sa.Table, value: str | None) -> str:
    """O ``SELECT`` do DuckDB que leva a partição ao contrato.

    A pasta inteira por ``read_parquet`` sem ``hive_partitioning`` e cada coluna em ``CAST`` para
    o tipo de ``sql_type`` no dialeto do DuckDB; todo identificador entre aspas, porque ``to`` é
    palavra reservada.

    Exemplo:

    .. code-block:: python

        partition_query("/dados/db_projetado/cad_operacoes/data_str=2026-02-28",
                        Operacao.__table__, "2026-02-28")
        # SELECT CAST("id_operacao" AS BIGINT) AS "id_operacao", ..., '2026-02-28' AS "data_str"
        # FROM read_parquet('/dados/.../data_str=2026-02-28/*.parquet', hive_partitioning = false)

    :param folder: a URI da pasta da partição na origem, como ``discover_partitions`` a devolve;
        numa tabela sem partição, a pasta da tabela.
    :param table: a tabela do modelo.
    :param value: o valor do caminho, que a coluna de partição recebe; ``None`` na tabela sem
        partição.
    :return: o texto da consulta.
    :raises ContractError: a tabela com um tipo de coluna fora do contrato ou com mais de uma
        coluna de partição.
    """
    partition_by = table_options(table).partition_by
    selected = []
    for column in table.columns:
        name = quoted(column.name)
        if column.name == partition_by:
            selected.append(f"{literal(value)} AS {name}")
            continue
        selected.append(f"CAST({name} AS {sql_type(column, 'duckdb')}) AS {name}")
    files = literal(f"{folder}/*.parquet")
    return f"SELECT {', '.join(selected)} FROM read_parquet({files}, hive_partitioning = false)"


def _ordered_select(query: str, table: sa.Table) -> str:
    """As colunas do arquivo sobre a consulta da partição, sem a coluna de partição, que o
    caminho leva, na ordem da ``sort_key`` quando há uma."""
    options = table_options(table)
    names = [quoted(column.name) for column in table.columns
             if column.name != options.partition_by]
    text = f"SELECT {', '.join(names)} FROM ({query})"
    if options.sort_key:
        text += " ORDER BY " + ", ".join(quoted(name) for name in options.sort_key)
    return text


# ---------------------------------------------------------------- a conferência da partição


@dataclasses.dataclass(frozen=True)
class _PartitionCheck:
    """O que a consulta de conferência achou numa partição."""

    rows: int
    problems: tuple[str, ...]
    nonfinite_columns: tuple[str, ...]


def _check_partition(connection: duckdb.DuckDBPyConnection, query: str, table: sa.Table,
                     value: str | None) -> _PartitionCheck:
    """A conferência da partição, numa consulta só: as linhas, o que está fora do contrato e as
    colunas ``Double`` com ``NaN`` ou infinito, que o contrato aceita e ficam sem mínimo e máximo.

    Fora do contrato: linhas com a coluna de origem da partição diferente do valor do caminho,
    quando o modelo declara ``partition_source``; nulos nas colunas ``NOT NULL``; textos acima do
    ``String(n)`` em bytes, a medida do ``VARCHAR(n)`` do Redshift (``strlen`` conta bytes no
    DuckDB).
    """
    options = table_options(table)
    measures = ["count(*)"]
    labels = []
    if options.partition_by and options.partition_source:
        partition = quoted(options.partition_by)
        source = quoted(options.partition_source)
        measures.append(f"count(*) FILTER (WHERE {partition} <> strftime({source}, '%Y-%m-%d'))")
        labels.append(f"linhas com {options.partition_source} diferente de {value}")
    for column in table.columns:
        name = quoted(column.name)
        if not column.nullable and column.name != options.partition_by:
            measures.append(f"count(*) FILTER (WHERE {name} IS NULL)")
            labels.append(f"nulos na coluna NOT NULL {column.name}")
        if isinstance(column.type, sa.String) and column.type.length:
            measures.append(f"count(*) FILTER (WHERE strlen({name}) > {column.type.length})")
            labels.append(f"textos acima de String({column.type.length}) em {column.name}")
    doubles = double_columns(table)
    for name in doubles:
        measures.append(f"count(*) FILTER (WHERE NOT isfinite({quoted(name)}))")

    # A contagem vem primeiro, depois as medidas do contrato na ordem de labels e as dos não
    # finitos.
    counts = connection.execute(f"SELECT {', '.join(measures)} FROM ({query})").fetchone()
    rows = counts[0]
    contract_counts = counts[1:1 + len(labels)]
    nonfinite_counts = counts[1 + len(labels):]
    problems = [f"{count} {label}" for count, label in zip(contract_counts, labels) if count]
    nonfinite = [name for name, count in zip(doubles, nonfinite_counts) if count]
    return _PartitionCheck(rows, tuple(problems), tuple(nonfinite))


# ---------------------------------------------------------------- a gravação


def _copy_partition(connection: duckdb.DuckDBPyConnection, storage: Storage, table: sa.Table,
                    uri: str, value: str | None, query: str,
                    execution_id: str) -> delta.RegisteredFile:
    """``COPY ... RETURN_STATS`` da partição para um arquivo novo dentro da pasta dela, com o
    ``execution_id`` no nome; devolve o arquivo como ``register_files`` o recebe."""
    partition_by = table_options(table).partition_by
    table_path = storage.relative(uri)
    relative = f"{execution_id}_{uuid.uuid4().hex}.parquet"
    if partition_by is not None:
        relative = f"{partition_by}={value}/{relative}"
        storage.ensure_folder(storage.join(table_path, f"{partition_by}={value}"))
    target = storage.uri_of(storage.join(table_path, relative))
    select = _ordered_select(query, table)
    cursor = connection.execute(
        f"COPY ({select}) TO {literal(target)} (FORMAT parquet, RETURN_STATS)")
    names = [column[0] for column in cursor.description]
    row = dict(zip(names, cursor.fetchone()))
    return delta.file_from_return_stats(row, table, uri)


def _source_setup(connection: duckdb.DuckDBPyConnection, db: Database, source: str) -> None:
    """As extensões e o secret da origem no S3 quando a raiz Delta é uma pasta local; com a raiz no
    S3, a conexão do motor já os tem."""
    source_storage = Storage.for_uri(source)
    if source_storage.is_s3 and not db.storage.is_s3:
        source_storage.duckdb_setup(connection)


def _wanted_values(found: Mapping[str | None, str],
                   partitions: Sequence[str] | None) -> list[str | None]:
    """Os valores encontrados que o chamador pediu; todos sem ``partitions``, e nenhum numa tabela
    sem partição com ``partitions``."""
    if partitions is None:
        return list(found)
    return [value for value in found if value in partitions]


def _load_partition(engine: DuckDBEngine, storage: Storage, table: sa.Table, uri: str,
                    value: str | None, folder: str, metadata: Mapping[str, str]) -> None:
    """Grava uma partição no Delta: a conferência da consulta, o ``COPY`` para um arquivo novo e
    o registro dele, com o tempo no log. Uma partição fora do contrato é ``ContractError`` antes
    de qualquer gravação."""
    started = time.perf_counter()
    query = partition_query(folder, table, value)
    with engine.session() as connection:
        check = _check_partition(connection, query, table, value)
        if check.problems:
            raise ContractError(f"{table.name} partição {value}: {'; '.join(check.problems)}")
        file = _copy_partition(connection, storage, table, uri, value, query, engine.execution_id)
    delta.register_files(uri, table, [file], value, metadata, storage, expected_rows=check.rows,
                         columns_without_min_max=check.nonfinite_columns)
    log.info("%s %s: %d linhas em %.1f s", table.name, value, check.rows,
             time.perf_counter() - started)
    if check.nonfinite_columns:
        log.info("%s %s: sem mínimo e máximo em %s", table.name, value,
                 ", ".join(check.nonfinite_columns))


def initial_load(db: Database, table: sa.Table, source: str,
                 partitions: Sequence[str] | None = None,
                 config: DuckDBConfig | None = None) -> list[str | None]:
    """Grava no Delta cada partição da tabela ainda fora do log.

    A tabela nasce por ``create_table`` quando não existe. A segunda chamada não grava nada.

    Exemplo:

    .. code-block:: python

        initial_load(db, Operacao.__table__, "/dados/db_projetado")   # ["2026-02-28", "2026-03-31"]
        initial_load(db, Operacao.__table__, "/dados/db_projetado")   # []

    :param db: o banco, com a raiz Delta e o ambiente; a tabela Delta fica em
        ``<raiz>/<ambiente>/<tabela>``.
    :param table: a tabela do modelo.
    :param source: a raiz da base Parquet de origem, com uma pasta por tabela; uma pasta local,
        ``file://`` ou ``s3://``.
    :param partitions: os valores a gravar, que filtram as partições encontradas e deixam de fora
        uma tabela sem partição; ``None`` grava todas.
    :param config: a configuração do motor DuckDB da chamada; sem ela, a pasta temporária do
        sistema e os limites lidos do ambiente.
    :return: os valores gravados, na ordem da gravação; ``None`` é a tabela sem partição.
    :raises ContractError: uma partição fora do contrato nas conferências da consulta, sem
        commit, e a chamada seguinte recomeça dela.
    :raises duckdb.Error: um valor que não converte para o tipo do contrato, ou uma coluna do
        contrato ausente dos arquivos, no ``COPY``, sem commit.
    :raises RegistrationRefused: uma conferência de ``register_files`` reprovou o arquivo gravado,
        sem commit, ou a releitura reprovou e ``restore`` voltou a tabela à versão anterior.
    :raises ExecutionConflict: outro registro da mesma partição a partir da mesma versão.
    :raises FileNotFoundError: a pasta da tabela ausente na origem.
    :raises ValueError: ``source`` no S3 sem região, ou em outro esquema.
    """
    options = table_options(table)
    uri = db.uri(table)
    storage = db.storage
    dt = delta.create_table(uri, table, storage)
    found, _ = discover_partitions(source, table)
    already = set(delta.partition_values(dt, options.partition_by))
    log.info("%s: %d partições na origem, %d no log", table.name, len(found), len(already))
    missing = []
    for value in _wanted_values(found, partitions):
        if value not in already:
            missing.append(value)
    execution_id = f"carga-{uuid.uuid4().hex[:8]}"
    metadata = delta.commit_metadata(execution_id, {})
    with DuckDBEngine(config or DuckDBConfig(), execution_id, storage) as engine:
        with engine.session() as connection:
            _source_setup(connection, db, source)
        for value in missing:
            _load_partition(engine, storage, table, uri, value, found[value], metadata)
    return missing


# ---------------------------------------------------------------- o relatório


@dataclasses.dataclass(frozen=True)
class _Totals:
    """Contagem, somas e não finitos de uma partição, num dos lados do relatório."""

    rows: int | None
    sums: dict[str, Decimal | None]
    nonfinite: dict[str, int]


def _total_measures(sums: list[str], doubles: list[str]) -> list[str]:
    """As agregações de uma partição: a contagem, a soma de cada coluna de ``sums`` como
    ``DECIMAL(38, 6)``, só dos valores finitos numa ``Double``, e a contagem dos não finitos de
    cada coluna de ``doubles``."""
    measures = ["count(*)"]
    for name in sums:
        decimal = f"CAST({quoted(name)} AS DECIMAL(38, 6))"
        if name in doubles:
            measures.append(f"sum(CASE WHEN isfinite({quoted(name)}) THEN {decimal} END)")
        else:
            measures.append(f"sum({decimal})")
    for name in doubles:
        measures.append(f"count(*) FILTER (WHERE NOT isfinite({quoted(name)}))")
    return measures


def _totals_from_row(row: tuple, sums: list[str], doubles: list[str]) -> _Totals:
    """As medidas de uma linha da agregação, na ordem de ``_total_measures``."""
    sum_values = row[1:1 + len(sums)]
    nonfinite_values = row[1 + len(sums):]
    return _Totals(row[0], dict(zip(sums, sum_values)), dict(zip(doubles, nonfinite_values)))


def _aggregate(connection: duckdb.DuckDBPyConnection, relation: str, partition_by: str | None,
               sums: list[str], doubles: list[str]) -> dict[str | None, _Totals]:
    """Contagem, somas e não finitos por partição de ``relation``: ``{valor: totais}``."""
    measures = ", ".join(_total_measures(sums, doubles))
    if partition_by is None:
        row = connection.execute(f"SELECT {measures} FROM {relation}").fetchone()
        return {None: _totals_from_row(row, sums, doubles)}
    rows = connection.execute(
        f"SELECT {quoted(partition_by)}, {measures} FROM {relation} GROUP BY 1 ORDER BY 1"
    ).fetchall()
    return {row[0]: _totals_from_row(row[1:], sums, doubles) for row in rows}


def _source_relation(folder: str, partition_by: str | None) -> str:
    """A leitura da tabela inteira na origem: as pastas Hive com o valor como texto, ou o único
    nível de arquivos numa tabela sem partição."""
    if partition_by is None:
        return f"read_parquet({literal(folder + '/*.parquet')})"
    return (f"read_parquet({literal(folder + '/*/*.parquet')}, "
            "hive_partitioning = true, hive_types_autocast = false)")


def _delta_totals(connection: duckdb.DuckDBPyConnection, db: Database, table: sa.Table,
                  sums: list[str], doubles: list[str]) -> dict[str | None, _Totals]:
    """Contagem, somas e não finitos por partição no Delta; vazio na tabela ainda fora dele, que
    tem toda partição só na origem."""
    uri = db.uri(table)
    if not delta.table_exists(uri, db.storage):
        return {}
    partition_by = table_options(table).partition_by
    return _aggregate(connection, f"delta_scan({literal(uri)})", partition_by, sums, doubles)


def _partition_reports(
    in_source: Mapping[str | None, _Totals], in_delta: Mapping[str | None, _Totals],
) -> tuple[PartitionReport, ...]:
    """Uma conferência por partição presente num dos lados, em ordem de texto do valor; o lado
    em que a partição falta fica com ``None`` nas linhas."""
    absent = _Totals(rows=None, sums={}, nonfinite={})
    reports = []
    for value in sorted(set(in_source) | set(in_delta), key=str):
        source_totals = in_source.get(value, absent)
        delta_totals = in_delta.get(value, absent)
        reports.append(PartitionReport(
            value=value,
            source_rows=source_totals.rows,
            delta_rows=delta_totals.rows,
            source_sums=source_totals.sums,
            delta_sums=delta_totals.sums,
            source_nonfinite=source_totals.nonfinite,
            delta_nonfinite=delta_totals.nonfinite,
        ))
    return tuple(reports)


def _conversions(source: str, table: sa.Table) -> tuple[str, ...]:
    """As conversões de tipo da origem para o contrato, lidas no rodapé do primeiro arquivo da
    tabela: ``"id_contrato: int32 -> int64"``, ``"carimbo: INT96 -> timestamp[us]"``."""
    storage = Storage.for_uri(source)
    files = storage.list_files(table.name, ".parquet")
    if not files:
        return ()
    footer = pq.ParquetFile(storage.open_input_file(files[0]))
    physical = {}
    for index in range(len(footer.schema)):
        physical[footer.schema.column(index).name] = footer.schema.column(index).physical_type
    contract = arrow_schema(table)
    found = []
    for field in footer.schema_arrow:
        if field.name not in contract.names or contract.field(field.name).type == field.type:
            continue
        origin = "INT96" if physical[field.name] == "INT96" else str(field.type)
        found.append(f"{field.name}: {origin} -> {contract.field(field.name).type}")
    return tuple(found)


def load_report(db: Database, table: sa.Table, source: str,
                config: DuckDBConfig | None = None) -> LoadReport:
    """Contagem e somas por partição na origem e no Delta, e o veredito.

    A origem é lida por ``read_parquet`` com ``hive_partitioning`` e o valor como texto, o Delta
    por ``delta_scan``, num motor DuckDB próprio da chamada. As colunas ``Numeric`` somam como
    ``DECIMAL(38, 6)``; as ``Double`` da mesma forma só nos valores finitos, com os não finitos
    contados à parte.

    Exemplo:

    .. code-block:: python

        report = load_report(db, Operacao.__table__, "/dados/db_projetado")
        report.matches                          # True
        report.partitions[0].source_sums        # {"valor": Decimal("45.150000")}

    :param db: o banco, com a raiz Delta e o ambiente; a tabela Delta fica em
        ``<raiz>/<ambiente>/<tabela>``.
    :param table: a tabela do modelo.
    :param source: a raiz da base Parquet de origem, com uma pasta por tabela; uma pasta local,
        ``file://`` ou ``s3://``.
    :param config: a configuração do motor DuckDB da chamada; sem ela, a pasta temporária do
        sistema e os limites lidos do ambiente.
    :return: o relatório, com o veredito em ``matches``; uma partição presente num lado só tem
        ``None`` nas linhas do outro, e a tabela ainda fora do Delta tem toda partição assim.
    :raises FileNotFoundError: a pasta da tabela ausente na origem.
    :raises ValueError: ``source`` no S3 sem região, ou em outro esquema.
    """
    options = table_options(table)
    sums = [column.name for column in table.columns if isinstance(column.type, sa.Numeric)]
    doubles = double_columns(table)
    _, skipped = discover_partitions(source, table)
    folder = Storage.for_uri(source).uri_of(table.name)
    execution_id = f"relatorio-{uuid.uuid4().hex[:8]}"
    source_relation = _source_relation(folder, options.partition_by)
    with DuckDBEngine(config or DuckDBConfig(), execution_id, db.storage) as engine:
        with engine.session() as connection:
            _source_setup(connection, db, source)
            in_source = _aggregate(connection, source_relation, options.partition_by, sums,
                                   doubles)
            in_delta = _delta_totals(connection, db, table, sums, doubles)
    partitions = _partition_reports(in_source, in_delta)
    return LoadReport(table.name, partitions, skipped, _conversions(source, table))
