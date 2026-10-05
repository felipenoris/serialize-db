"""Sonda do limiar de ``PARALLEL OFF`` na exportação do motor Redshift.

``.claude/memory/OPEN_QUESTIONS.md`` ("O ``PARALLEL OFF`` e a reconexão do motor Redshift") registra
que o limiar de 5.000.000 linhas (``_PARALLEL_OFF_ROWS`` em ``serialize_db.engine.redshift``), até o
qual a exportação grava a partição em série num arquivo só, não foi medido. A sonda carrega a
primeira partição de ``cad_lancamentos`` na origem, leva-a ao sandbox pelo ``ingest`` do motor, cria
tabelas do sandbox com 1, 5, 10 e 20 milhões das linhas dela, e roda o ``UNLOAD`` da exportação de
cada uma e da partição inteira com ``PARALLEL OFF`` e sem ele, três vezes cada, alternando a ordem.
O ``select`` é o da exportação: as colunas sem a de partição, na ordem da ``sort_key``.

Checagens:

- cada ``UNLOAD`` com as linhas da tabela nos rodapés dos arquivos;
- o ``PARALLEL OFF`` com um arquivo só.

Leituras: por tamanho e modo, o menor tempo do ``UNLOAD``, os arquivos, o total e o maior em MB e
o tempo da leitura dos rodapés, que o registro faz um a um; e a razão entre os tempos com
``PARALLEL OFF`` e sem ele.

A sonda grava sob a raiz do S3 das suítes e cria no esquema de ``SERIALIZE_DB_REDSHIFT_SCHEMA`` as
tabelas ``exec_operacao_<id>_*``, que o ``cleanup`` do motor apaga no fim.

Exemplo:

.. code-block:: shell

    export SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo
    export SERIALIZE_DB_REDSHIFT_WORKGROUP=workgroup SERIALIZE_DB_REDSHIFT_SCHEMA=esquema
    .venv/bin/python probes/operacao/probe_unload_parallel.py s3://bucket/origem/db_projetado
"""

from __future__ import annotations

import dataclasses
import sys
import time
import uuid

import operation_lib as lib
import pyarrow.parquet as pq

from serialize_db import delta
from serialize_db.engine.redshift import (
    RedshiftConfig,
    RedshiftEngine,
    columns_without_partition,
    credentials_clause,
    unload_text,
)
from serialize_db.execution import Database
from serialize_db.schema import quoted, table_options

USAGE = "uso: .venv/bin/python probes/operacao/probe_unload_parallel.py <origem>"
SIZES = (1_000_000, 5_000_000, 10_000_000, 20_000_000)
REPETITIONS = 3


@dataclasses.dataclass
class Unloaded:
    """Um ``UNLOAD``: o tempo dele, os tamanhos dos arquivos, as linhas dos rodapés e o tempo da
    leitura dos rodapés."""

    seconds: float
    sizes: list[int]
    rows: int
    footer_seconds: float


def export_select(
    engine: RedshiftEngine,
    name: str,
) -> str:
    """O ``select`` da exportação sobre a tabela ``name`` do sandbox, como o do motor para
    ``cad_lancamentos``, que não tem coluna JSON: as colunas sem a de partição, na ordem da
    ``sort_key``."""
    columns = [quoted(column.name) for column in columns_without_partition(lib.TABLE)]
    order = [quoted(column) for column in table_options(lib.TABLE).sort_key]
    return f"SELECT {', '.join(columns)} FROM {engine.qualified(name)} ORDER BY {', '.join(order)}"


def unload(
    engine: RedshiftEngine,
    db: Database,
    select: str,
    prefix: str,
    parallel: bool,
) -> Unloaded:
    """O ``UNLOAD`` do ``select`` para o prefixo, cronometrado, e a leitura dos rodapés dos
    arquivos do manifesto."""
    storage = db.storage
    started = time.perf_counter()
    engine.execute(
        unload_text(select, storage.uri_of(prefix), credentials_clause(engine.config), parallel)
    )
    seconds = time.perf_counter() - started
    paths = engine.unloaded_paths(prefix)
    sizes = [storage.size(path) for path in paths]
    started = time.perf_counter()
    rows = 0
    for path in paths:
        rows += pq.ParquetFile(storage.open_input_file(path)).metadata.num_rows
    return Unloaded(seconds, sizes, rows, time.perf_counter() - started)


def mode_label(
    parallel: bool,
) -> str:
    """O modo como a linha do relatório o escreve."""
    return "paralelo" if parallel else "PARALLEL OFF"


def sized_table(
    engine: RedshiftEngine,
    rows: int,
    total: int,
) -> str:
    """A tabela do sandbox com ``rows`` linhas da partição: a do ``ingest`` para a partição
    inteira, ou uma nova por ``CREATE TABLE AS ... LIMIT``, que o ``cleanup`` apaga."""
    ingested = engine.prefix + lib.TABLE.name
    if rows == total:
        return ingested
    name = f"{engine.prefix}linhas_{rows}"
    engine.execute(
        f"CREATE TABLE {engine.qualified(name)} AS SELECT * FROM "
        f"{engine.qualified(ingested)} LIMIT {rows}"
    )
    engine.register_created(name)
    return name


def measure(
    engine: RedshiftEngine,
    db: Database,
    rows: int,
    total: int,
    problems: list[str],
) -> dict[bool, list[Unloaded]]:
    """Os ``UNLOAD`` de uma tabela de ``rows`` linhas nos dois modos, ``REPETITIONS`` vezes cada,
    com a ordem alternada; anota em ``problems`` as linhas e os arquivos fora do esperado."""
    select = export_select(engine, sized_table(engine, rows, total))
    runs: dict[bool, list[Unloaded]] = {False: [], True: []}
    for repetition in range(REPETITIONS):
        order = (False, True) if repetition % 2 == 0 else (True, False)
        for parallel in order:
            label = "paralelo" if parallel else "serie"
            prefix = db.storage.join(db.environment, "unload", f"{rows}-{label}-{repetition}")
            run = unload(engine, db, select, prefix, parallel)
            runs[parallel].append(run)
            print(
                f"{rows} linhas, {mode_label(parallel)}, repetição {repetition + 1}: "
                f"{run.seconds:.1f} s, {len(run.sizes)} arquivo(s), "
                f"{sum(run.sizes) / 2**20:.1f} MB, maior {max(run.sizes) / 2**20:.1f} MB, "
                f"rodapés em {run.footer_seconds:.2f} s"
            )
            if run.rows != rows:
                problems.append(f"{rows} linhas, {mode_label(parallel)}: {run.rows} nos rodapés")
            if not parallel and len(run.sizes) != 1:
                problems.append(f"{rows} linhas, PARALLEL OFF: {len(run.sizes)} arquivos")
    return runs


def print_summary(
    results: dict[int, dict[bool, list[Unloaded]]],
) -> None:
    """O menor tempo de cada tamanho e modo, com os arquivos e a razão entre os modos."""
    print("resumo, o menor tempo de cada modo:")
    for rows, runs in results.items():
        serial = min(runs[False], key=lambda run: run.seconds)
        parallel = min(runs[True], key=lambda run: run.seconds)
        print(
            f"  {rows} linhas: PARALLEL OFF {serial.seconds:.1f} s, 1 arquivo de "
            f"{sum(serial.sizes) / 2**20:.1f} MB; paralelo {parallel.seconds:.1f} s, "
            f"{len(parallel.sizes)} arquivo(s), maior {max(parallel.sizes) / 2**20:.1f} MB; "
            f"razão {serial.seconds / parallel.seconds:.2f}; rodapés "
            f"{serial.footer_seconds:.2f} s e {parallel.footer_seconds:.2f} s"
        )


def main() -> None:
    """Os ``UNLOAD`` de cada tamanho nos dois modos, com o relatório no terminal e em
    ``probes/output/``."""
    if len(sys.argv) != 2:
        print(USAGE, file=sys.stderr)
        sys.exit(2)
    source = sys.argv[1]
    db = lib.work_database("unload")
    storage = db.storage
    if not storage.is_s3:
        print("o UNLOAD grava no S3: a sonda pede SERIALIZE_DB_TEST_S3_ROOT", file=sys.stderr)
        sys.exit(2)
    value = lib.source_partitions(source, 1)[0]
    lib.load_partitions(db, source, [value])
    uri = lib.table_uri(db, lib.TABLE.name)
    version = delta.open_table(uri, storage).version()

    execution_id = f"operacao-{uuid.uuid4().hex[:8]}"
    staging = storage.join(db.environment, "staging", execution_id)
    engine = RedshiftEngine(RedshiftConfig.from_environment(), execution_id, storage, staging)
    problems: list[str] = []
    results: dict[int, dict[bool, list[Unloaded]]] = {}
    try:
        started = time.perf_counter()
        engine.ingest(lib.TABLE, uri, version)
        name = engine.qualified(engine.prefix + lib.TABLE.name)
        total = int(engine.execute(f"SELECT count(*) FROM {name}").fetchone()[0])
        print(f"ingest de {total} linhas no sandbox em {time.perf_counter() - started:.1f} s")
        sizes = [rows for rows in SIZES if rows < total] + [total]
        for rows in sizes:
            results[rows] = measure(engine, db, rows, total, problems)
    finally:
        engine.cleanup()
    lib.check("cada UNLOAD com as linhas da tabela, e o PARALLEL OFF num arquivo só", problems)
    print_summary(results)
    lib.finish(db)


if __name__ == "__main__":
    lib.run(main)
