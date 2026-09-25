"""A migração adiantada: a base Parquet de origem vira tabelas Delta, uma partição por commit,
com o relatório da execução em JSON.

O script é a ferramenta de operação da carga inicial (``plan/PLAN-STAGE-7.md``) sobre
``serialize_db.load``: ``initial_load`` grava cada partição ainda fora do log, com a conferência
da partição, o ``COPY ... RETURN_STATS`` na ordem da ``sort_key`` e o registro pelo
``register_files``, e ``load_report`` confere contagem e somas por partição entre a origem e o
Delta. Para cada tabela do modelo, as sem partição primeiro e as particionadas depois, na ordem do
modelo (``load_order``), o script chama ``initial_load`` partição por partição, imprime as linhas,
o tempo e o pico de memória do processo até ali e, com ``--report``, regrava o JSON depois de cada
partição gravada, com a tabela da vez em ``in_progress``: um processo morto no meio da carga, pela
falta de memória por exemplo, deixa o que já conferiu e gravou. O relatório final leva a máquina, as
versões, os limites do DuckDB lidos do ambiente e os argumentos, cada tabela com o relatório de
``load_report`` e as partições gravadas agora, e o que a raiz da origem tem fora do modelo. A raiz
Delta é a de ``Database``: cada tabela vai para ``<raiz>/<ambiente>/<tabela>``, e a origem fica
intocada.

Uma partição fora do contrato interrompe a execução sem commit, com a tabela, a partição e a
coluna na mensagem, e a execução seguinte recomeça dela; o script sai com 1 nesse caso e quando o
relatório de alguma tabela acha diferença, e com 2 quando o modelo viola o contrato ou a origem não
tem a pasta de uma tabela. As tabelas da origem fora do modelo (``alembic_version``,
``meta_update_status``) e o ``schema.json`` da raiz ficam de fora e entram no relatório. A
auditoria de chaves estrangeiras é ``serialize-db audit --foreign-keys``, depois da carga.

Sobre a base fictícia de ``tests/source_db_projetado.py``, em pasta local:

    PYTHONPATH=tests uv run python scripts/migrate_parquet_to_delta.py \\
        --metadata client_model:Base.metadata --source /pasta/db_projetado \\
        --root /pasta/delta --environment prd

No ambiente alvo, com a pasta preparada, sobre a cópia da base de produção:

    PYTHONPATH=tests .venv/bin/python scripts/migrate_parquet_to_delta.py \\
        --metadata client_model:Base.metadata --environment prd \\
        --source s3://bucket/prefixo/db_projetado --root s3://bucket/prefixo/delta \\
        --tables cad_contratos --report relatorio.json

``tests/test_migrate_parquet_to_delta.py`` cobre o script sobre a base fictícia.
"""

from __future__ import annotations

import argparse
import dataclasses
import functools
import importlib
import importlib.metadata
import json
import os
import platform
import sys
import time
from collections.abc import Callable, Sequence
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import sqlalchemy as sa

from serialize_db import delta, load, schema
from serialize_db.engine.duckdb import environment_limits
from serialize_db.errors import ContractError
from serialize_db.execution import Database
from serialize_db.load import LoadReport
from serialize_db.resources import available_cpus, available_memory, peak_rss_mb

# ---------------------------------------------------------------- o relatório


@dataclasses.dataclass(frozen=True)
class PartitionLoad:
    """Uma partição gravada agora: linhas, tempo e o RSS máximo do processo até ali."""

    value: str | None
    rows: int
    seconds: float
    peak_rss_mb: float


@dataclasses.dataclass(frozen=True)
class TableReport:
    """O relatório de uma tabela: o de ``load_report`` e as partições gravadas agora."""

    report: LoadReport
    loaded: tuple[PartitionLoad, ...]

    def as_document(self) -> dict[str, object]:
        """A tabela como o JSON a leva: os campos do relatório, o veredito e as cargas."""
        document = dataclasses.asdict(self.report)
        document["matches"] = self.report.matches
        document["loaded"] = [dataclasses.asdict(item) for item in self.loaded]
        return document


# ---------------------------------------------------------------- a carga


def partition_rows(db: Database, table: sa.Table, value: str | None) -> int:
    """As linhas da partição na versão atual da tabela Delta, pelas ações ``add`` do log."""
    dt = delta.open_table(db.uri(table), db.storage)
    actions = pa.table(dt.get_add_actions(flatten=True)).to_pylist()
    partition_by = schema.table_options(table).partition_by
    total = 0
    for action in actions:
        if partition_by is None or action[f"partition.{partition_by}"] == value:
            total += action["num_records"]
    return total


def load_table(
    db: Database,
    table: sa.Table,
    source: str,
    partitions: Sequence[str] | None,
    progress: Callable[[list[PartitionLoad]], None] | None = None,
) -> list[PartitionLoad]:
    """A carga da tabela partição por partição, cada uma numa chamada de ``initial_load``, com a
    linha impressa e ``progress`` chamado depois de cada partição gravada; devolve o que gravou
    agora."""
    found, _ = load.discover_partitions(source, table)
    wanted = [value for value in found if partitions is None or value in partitions]
    loaded: list[PartitionLoad] = []
    for value in wanted:
        started = time.perf_counter()
        selected = None if value is None else [value]
        # initial_load devolve a lista vazia quando o log já tem a partição.
        if not load.initial_load(db, table, source, selected):
            continue
        rows = partition_rows(db, table, value)
        item = PartitionLoad(value, rows, time.perf_counter() - started, peak_rss_mb())
        print(f"  {value}: {rows} linhas em {item.seconds:.1f} s; "
              f"RSS máximo do processo {item.peak_rss_mb:.0f} MB")
        loaded.append(item)
        if progress is not None:
            progress(loaded)
    return loaded


def print_report(report: LoadReport) -> None:
    """As linhas do relatório de uma tabela, depois das partições gravadas."""
    for partition in report.partitions:
        if not partition.matches:
            print(f"  DIFERENÇA em {partition.value}: origem {partition.source_rows} linhas "
                  f"{dict(partition.source_sums)} não finitos {dict(partition.source_nonfinite)}, "
                  f"Delta {partition.delta_rows} linhas {dict(partition.delta_sums)} não finitos "
                  f"{dict(partition.delta_nonfinite)}")
    verdict = "contagens e somas iguais" if report.matches else "com diferenças"
    print(f"  relatório: {len(report.partitions)} partições conferidas, {verdict}")
    if report.conversions:
        print(f"  conversões: {', '.join(report.conversions)}")
    for entry in report.skipped:
        print(f"  ignorado fora do padrão: {entry}")


# ---------------------------------------------------------------- o JSON da execução


def describe_environment(arguments: argparse.Namespace) -> dict[str, object]:
    """A máquina, as versões, os limites do DuckDB lidos do ambiente e os parâmetros da execução,
    que o relatório leva: a memória e os núcleos mudam com a instância."""
    physical_memory = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    packages = ("duckdb", "deltalake", "pyarrow")
    return {
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "cpus": os.cpu_count(),
        "cpus_available": available_cpus(),
        "memory_total_mb": round(physical_memory / 2**20),
        "memory_available_mb": round(available_memory() / 2**20),
        "packages": {name: importlib.metadata.version(name) for name in packages},
        "duckdb_limits": environment_limits(),
        "arguments": {
            "source": arguments.source,
            "root": arguments.root,
            "environment": arguments.environment,
            "tables": arguments.tables,
            "partitions": arguments.partitions,
        },
    }


def write_progress(path: str, reports: list[TableReport], environment: dict[str, object],
                   table: str, loaded: list[PartitionLoad]) -> None:
    """O relatório parcial, regravado depois de cada partição gravada: as tabelas já conferidas
    e, em ``in_progress``, as partições gravadas da tabela da vez. O relatório final o
    substitui."""
    document = {
        "environment": environment,
        "tables": [report.as_document() for report in reports],
        "in_progress": {"table": table, "loaded": [dataclasses.asdict(item) for item in loaded]},
    }
    Path(path).write_text(json.dumps(document, indent=2, ensure_ascii=False, default=str))


def write_report(path: str, reports: list[TableReport], outside: Sequence[str],
                 environment: dict[str, object]) -> None:
    """O relatório da execução em JSON, com as somas como texto e o ambiente que a rodou."""
    document = {
        "environment": environment,
        "tables": [report.as_document() for report in reports],
        "outside_model": list(outside),
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


def build_parser() -> argparse.ArgumentParser:
    """Os argumentos da linha de comando do script."""
    parser = argparse.ArgumentParser(
        description="Migra a base Parquet particionada de origem para tabelas Delta, uma "
        "partição por commit, e confere contagens e somas."
    )
    parser.add_argument("--metadata", required=True, type=resolve_metadata,
                        help="o MetaData do modelo, como client_model:Base.metadata")
    parser.add_argument("--source", required=True,
                        help="a raiz da origem, pasta local ou s3://bucket/prefixo")
    parser.add_argument("--root", required=True,
                        help="a raiz das tabelas Delta, pasta local ou s3://bucket/prefixo")
    # A variável vazia conta como ausente, como nos subcomandos de serialize-db.
    environment_default = os.environ.get("SERIALIZE_DB_ENVIRONMENT") or "dsv"
    parser.add_argument("--environment", default=environment_default,
                        help="o ambiente sob a raiz, a pasta das tabelas (padrão: "
                             "SERIALIZE_DB_ENVIRONMENT, senão dsv; a variável vazia conta como "
                             "ausente)")
    parser.add_argument("--tables", nargs="+", metavar="TABELA", help="só estas tabelas do modelo")
    parser.add_argument("--partitions", nargs="+", metavar="AAAA-MM-DD", default=None,
                        help="só estas partições; as tabelas sem partição ficam de fora")
    parser.add_argument("--report", metavar="ARQUIVO.json",
                        help="grava o relatório da execução em JSON")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Roda a migração com os argumentos de ``argv``, ou os do processo, e devolve o código de
    saída."""
    parser = build_parser()
    arguments = parser.parse_args(argv)
    metadata: sa.MetaData = arguments.metadata
    # O modelo fora do contrato e a tabela fora do modelo param a execução antes da carga.
    problems = schema.check_models(metadata)
    if problems:
        print("modelo fora do contrato:", *problems, sep="\n  ", file=sys.stderr)
        return 2
    unknown = set(arguments.tables or []) - set(metadata.tables)
    if unknown:
        parser.error(f"tabelas fora do modelo: {', '.join(sorted(unknown))}")
    db = Database(arguments.root, arguments.environment, metadata)
    # A máquina e os limites do DuckDB, impressos antes da carga e levados no relatório.
    environment = describe_environment(arguments)
    limits = environment["duckdb_limits"]
    print(f"{environment['cpus']} CPUs, {environment['memory_total_mb']} MB de memória, "
          f"{environment['memory_available_mb']} MB disponíveis; DuckDB com "
          f"{limits['threads']} threads e memory_limit {limits['memory_limit']}")
    tables = list(metadata.tables.values())
    if arguments.tables:
        tables = [table for table in tables if table.name in arguments.tables]
    # Cada tabela na ordem da carga, gravada e conferida; com --report, o JSON parcial é regravado
    # no começo da tabela e depois de cada partição.
    reports: list[TableReport] = []
    try:
        for table in load.load_order(tables):
            print(f"{table.name}:")
            progress = None
            if arguments.report:
                progress = functools.partial(write_progress, arguments.report, reports,
                                             environment, table.name)
                progress([])
            loaded = load_table(db, table, arguments.source, arguments.partitions, progress)
            report = load.load_report(db, table, arguments.source)
            print_report(report)
            reports.append(TableReport(report, tuple(loaded)))
    except ContractError as error:
        print(f"ContractError: {error}", file=sys.stderr)
        return 1
    except FileNotFoundError as error:
        print(f"FileNotFoundError: {error}", file=sys.stderr)
        return 2
    # O relatório final leva o que a origem tem fora do modelo; o veredito dá o código de saída.
    outside = load.entries_outside_the_model(arguments.source, metadata)
    if outside:
        print(f"fora do modelo: {', '.join(outside)}")
    if arguments.report:
        write_report(arguments.report, reports, outside, environment)
    matches = all(item.report.matches for item in reports)
    print(f"{len(reports)} tabelas conferidas, "
          f"{'contagens e somas iguais' if matches else 'com diferenças'}")
    return 0 if matches else 1


if __name__ == "__main__":
    sys.exit(main())
