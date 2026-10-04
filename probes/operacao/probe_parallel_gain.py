"""Sonda do ganho das APIs com threads sobre a execução em série, no ambiente alvo.

O ganho foi medido em 2026-10-04 num contêiner de 4 vCPUs, com o motor DuckDB numa pasta local
(``plan/POC.md``, seção "O que a medição das APIs com threads mostrou"); o que depende do S3, do
Redshift e das CPUs da máquina espera esta sonda (``plan/OPEN_QUESTIONS.md``, item "O ganho das
APIs com threads no ambiente alvo"). A sonda gera quatro tabelas iguais, ``cad_paralelo_a`` a
``cad_paralelo_d``, com ``--rows`` linhas cada numa partição, publica-as no Delta sob a raiz de
trabalho e mede cada variante ``--repetitions`` vezes, com a ordem invertida a cada repetição: o
tempo e o pico de memória residente do processo acima da base, zerado por
``/proc/self/clear_refs`` depois de devolver ao sistema a memória que o pool do Arrow e o
``malloc`` guardaram das medidas anteriores. Cada seção de ``--only`` compara a forma em série,
a primeira, com a forma com threads:

- ``duckdb``: no motor DuckDB da máquina, sobre ``cad_paralelo_a`` materializada no sandbox, o
  ``query`` seguido do laço contra o ``stream``; o trabalho de todos os lotes seguido do
  ``append`` contra o ``appender``; a série dos três contra ``stream`` e ``appender`` no mesmo
  ``with``; cada um com o trabalho do cliente ``nenhum`` (a coluna ``valor_projetado`` pelo
  ``pyarrow.compute``) e ``pandas`` (o lote no pandas, duas colunas calculadas nele e a volta ao
  Arrow); e 200 consultas pequenas em série, em quatro threads na sessão principal e em quatro
  threads com uma sessão a mais cada;
- ``pools``: sobre a raiz de trabalho, ``run.ingest`` das quatro tabelas com
  ``materialize=True`` contra uma chamada por tabela, ``run.publish_delta`` com
  ``max_workers=4`` contra 1, no motor DuckDB, e ``materialize`` das quatro no leitor Delta contra
  uma chamada por tabela;
- ``redshift``: no motor Redshift, ``run.ingest`` das quatro contra uma chamada por tabela; o
  ``stream`` lido todo antes do trabalho contra o trabalho em cada lote do ``stream``, porque o
  ``query`` do Redshift passa pelo cursor; o ``appender`` e os dois juntos, como no DuckDB; 80
  consultas pequenas, como no DuckDB, com a abertura de cada sessão a mais no tempo; e
  ``run.publish_delta`` com ``max_workers=4`` contra 1;
- ``publicacao``: ``publication.publish_redshift`` das quatro com ``max_workers=4`` contra 1, com
  a despublicação depois de cada medida, fora do tempo.

Checagem: cada medida contou as linhas ou as tabelas esperadas, e as linhas de cada tabela que o
``ingest`` e o ``materialize`` trouxeram são as geradas. Leituras: cada medida e, no resumo, o
menor tempo de cada variante, o pico dessa medida e a razão sobre a variante em série.

A sonda grava sob ``<raiz>/serialize-db-operacao/paralelo-<id>/``, num ambiente ``poc<id>``, e
apaga a pasta no fim. ``redshift`` e ``publicacao`` pedem a raiz no S3 e as variáveis
``SERIALIZE_DB_REDSHIFT_*``; o motor Redshift cria as tabelas ``exec_*`` do sandbox, que o
``cleanup`` apaga, e a publicação as tabelas ``poc<id>_*``, que a sonda apaga no fim com as linhas
de controle delas, criando a tabela de controle quando ela não existe e apagando-a só nesse caso.

Exemplo:

.. code-block:: shell

    export SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo
    export SERIALIZE_DB_REDSHIFT_WORKGROUP=workgroup SERIALIZE_DB_REDSHIFT_SCHEMA=esquema
    .venv/bin/python probes/operacao/probe_parallel_gain.py
    .venv/bin/python probes/operacao/probe_parallel_gain.py --rows 1000000 --only duckdb pools
"""

from __future__ import annotations

import argparse
import ctypes
import dataclasses
import functools
import gc
import logging
import operator
import sys
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import operation_lib as lib
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import redshift_connector
import sqlalchemy as sa

from serialize_db import publication
from serialize_db.engine import Engine, redshift
from serialize_db.engine.redshift import RedshiftConfig
from serialize_db.execution import Database, Execution
from serialize_db.reader import DeltaReader
from serialize_db.resources import environment_limits

SECTIONS = ("duckdb", "pools", "redshift", "publicacao")
PARTITION = "2026-08-31"
BATCH = 100_000
WORKERS = 4
QUERY_WIDTH = 1_000
DUCKDB_QUERIES = 200
REDSHIFT_QUERIES = 80
METADATA = sa.MetaData()
INFO = {"serialize_db": {"partition_by": ["data_base_str"]}}


# ---------------------------------------------------------------- o modelo e os dados


def entries_columns() -> list[sa.Column]:
    """As colunas de uma tabela de lançamentos particionada por ``data_base_str``."""
    return [
        sa.Column("id_lancamento", sa.BigInteger, primary_key=True, autoincrement=False),
        sa.Column("id_conta", sa.BigInteger, nullable=False),
        sa.Column("data_base", sa.Date, nullable=False),
        sa.Column("valor", sa.Double, nullable=False),
        sa.Column("historico", sa.String(40), nullable=False),
        sa.Column("data_base_str", sa.String(10), nullable=False),
    ]


def parallel_tables() -> list[sa.Table]:
    """As quatro tabelas iguais das medidas por tabela."""
    tables = []
    for letter in "abcd":
        tables.append(sa.Table(f"cad_paralelo_{letter}", METADATA, *entries_columns(), info=INFO))
    return tables


TABLES = parallel_tables()
SOURCE = TABLES[0]
OUTPUT = sa.Table(
    "cad_paralelo_saida",
    METADATA,
    *entries_columns(),
    sa.Column("valor_projetado", sa.Double, nullable=False),
    info=INFO,
)


def insert_rows_sql(
    table: sa.Table,
    rows: int,
) -> str:
    """O ``INSERT ... SELECT`` do DuckDB que gera ``rows`` lançamentos da partição."""
    return (
        f'INSERT INTO "{table.name}" '
        "SELECT range AS id_lancamento, range % 5000 AS id_conta, "
        "DATE '2026-08-01' + CAST(range % 31 AS INTEGER) AS data_base, "
        "(range % 1000003) / 100.0 AS valor, "
        "'historico ' || CAST(range % 100000 AS VARCHAR) AS historico, "
        f"'{PARTITION}' AS data_base_str "
        f"FROM range({rows})"
    )


def prepare(
    db: Database,
    rows: int,
) -> None:
    """As quatro tabelas geradas no sandbox DuckDB, auditadas e publicadas no Delta."""
    started = time.perf_counter()
    with Execution(db, "duckdb", PARTITION) as run:
        for table in TABLES:
            run.sandbox.create_table(table)
            run.sandbox.query(insert_rows_sql(table, rows))
            run.audit(table, [PARTITION])
        run.publish_delta(*TABLES, partitions=[PARTITION], max_workers=WORKERS)
    seconds = time.perf_counter() - started
    print(f"preparo: {len(TABLES)} tabelas de {rows} linhas no Delta em {seconds:.1f} s")


# ---------------------------------------------------------------- o trabalho do cliente


def project(
    batch: pa.RecordBatch,
) -> pa.RecordBatch:
    """O trabalho ``nenhum``: a coluna ``valor_projetado`` pelo ``pyarrow.compute``."""
    return batch.append_column("valor_projetado", pc.multiply(batch["valor"], 1.1))


def project_in_pandas(
    batch: pa.RecordBatch,
) -> pa.RecordBatch:
    """O trabalho ``pandas``: o lote no pandas, ``valor_projetado`` e o ``historico`` em
    maiúsculas calculados nele, e a volta ao Arrow."""
    frame = batch.to_pandas(types_mapper=pd.ArrowDtype)
    frame["valor_projetado"] = frame["valor"] * 1.1
    frame["historico"] = frame["historico"].str.upper()
    return pa.RecordBatch.from_pandas(frame, preserve_index=False)


WORKS = {"nenhum": project, "pandas": project_in_pandas}


# ---------------------------------------------------------------- a medida


@dataclasses.dataclass
class Run:
    """Uma medida: o tempo em segundos, o pico de memória residente acima da base em MB, ``None``
    sem leitura, e o que ela contou, linhas ou tabelas."""

    seconds: float
    peak_mb: float | None
    count: int


# As medidas que contaram outra coisa que a esperada, e as medidas de cada rótulo para o resumo.
PROBLEMS: list[str] = []
SUMMARY: list[tuple[str, dict[str, list[Run]]]] = []


def status_mb(
    field: str,
) -> float:
    """Um campo de memória de ``/proc/self/status``, em MB."""
    for line in Path("/proc/self/status").read_text().splitlines():
        if line.startswith(field + ":"):
            return int(line.split()[1]) / 1024
    raise RuntimeError(f"/proc/self/status sem {field}")


def reset_peak() -> float | None:
    """Devolve ao sistema a memória que o processo guarda das medidas anteriores, zera o pico de
    memória residente e devolve a memória residente atual, a base da medida; ``None`` quando o
    sistema não deixa zerar o pico."""
    # Sem a devolução, a medida seguinte reusa a memória que o pool do Arrow e o malloc guardaram,
    # e o pico dela sai abaixo do de um processo novo.
    gc.collect()
    pa.default_memory_pool().release_unused()
    try:
        ctypes.CDLL("libc.so.6").malloc_trim(0)
        Path("/proc/self/clear_refs").write_text("5")
    except OSError:
        return None
    return status_mb("VmRSS")


def timed(
    measured: Callable[[], int],
) -> Run:
    """Mede ``measured``, que devolve o que contou: o tempo e o pico acima da base."""
    base = reset_peak()
    started = time.perf_counter()
    count = measured()
    seconds = time.perf_counter() - started
    peak = None
    if base is not None:
        # A memória que uma thread solta entre o zero e a leitura da base deixa o pico abaixo dela.
        peak = max(status_mb("VmHWM") - base, 0.0)
    return Run(seconds, peak, count)


def timed_variant(
    function: Callable[..., int],
    *arguments: object,
) -> Callable[[], Run]:
    """A variante que mede ``function(*arguments)``."""
    return functools.partial(timed, functools.partial(function, *arguments))


def describe(
    run: Run,
) -> str:
    """A medida numa linha: o tempo, o pico acima da base e o que ela contou."""
    peak = "pico sem leitura"
    if run.peak_mb is not None:
        peak = f"pico +{run.peak_mb:.0f} MB"
    return f"{run.seconds:.3f} s, {peak}, contou {run.count}"


def measure(
    label: str,
    variants: dict[str, Callable[[], Run]],
    repetitions: int,
    expected: int,
) -> None:
    """Roda cada variante ``repetitions`` vezes, com a ordem invertida a cada repetição, imprime
    cada medida, anota em ``PROBLEMS`` a que contou outra coisa que ``expected`` e guarda as
    medidas para o resumo."""
    names = list(variants)
    runs: dict[str, list[Run]] = {}
    for name in names:
        runs[name] = []
    for repetition in range(repetitions):
        order = names if repetition % 2 == 0 else names[::-1]
        for name in order:
            run = variants[name]()
            runs[name].append(run)
            print(f"{label}, {name}, repetição {repetition + 1}: {describe(run)}")
            if run.count != expected:
                PROBLEMS.append(f"{label}, {name}: contou {run.count}, esperado {expected}")
    SUMMARY.append((label, runs))


def best(
    runs: list[Run],
) -> Run:
    """A medida mais rápida."""
    return min(runs, key=operator.attrgetter("seconds"))


def summary_line(
    label: str,
    runs: dict[str, list[Run]],
) -> str:
    """O menor tempo de cada variante, o pico dessa medida e a razão do menor tempo da primeira
    variante, a em série, sobre o dela."""
    names = list(runs)
    serial = best(runs[names[0]]).seconds
    parts = []
    for name in names:
        fastest = best(runs[name])
        peak = ""
        if fastest.peak_mb is not None:
            peak = f", pico +{fastest.peak_mb:.0f} MB"
        parts.append(f"{name} {fastest.seconds:.3f} s ({serial / fastest.seconds:.2f}x{peak})")
    return f"{label}: " + "; ".join(parts)


def check_rows(
    source: Engine | DeltaReader,
    rows: int,
    label: str,
) -> None:
    """Anota em ``PROBLEMS`` a tabela que não tem ``rows`` linhas no sandbox ou no leitor."""
    for table in TABLES:
        statement = sa.select(sa.func.count().label("n")).select_from(table)
        found = source.query(statement).column("n")[0].as_py()
        if found != rows:
            PROBLEMS.append(f"{label}: {table.name} com {found} linhas, esperadas {rows}")


# ---------------------------------------------------------------- stream, appender e os dois


def whole_by_query(
    engine: Engine,
) -> list[pa.RecordBatch]:
    """O resultado inteiro do ``query`` em lotes: a leitura em série do DuckDB."""
    return engine.query(sa.select(SOURCE)).to_batches(max_chunksize=BATCH)


def whole_by_stream(
    engine: Engine,
) -> list[pa.RecordBatch]:
    """Os lotes do ``stream`` lidos todos antes do trabalho: a leitura em série do Redshift."""
    with engine.stream(sa.select(SOURCE), batch_size=BATCH) as stream:
        return list(stream)


def work_in_series(
    engine: Engine,
    read_whole: Callable[[Engine], list[pa.RecordBatch]],
    work: Callable[[pa.RecordBatch], pa.RecordBatch],
) -> int:
    """O resultado inteiro e depois o trabalho de cada lote."""
    rows = 0
    for batch in read_whole(engine):
        work(batch)
        rows += batch.num_rows
    return rows


def work_on_stream(
    engine: Engine,
    work: Callable[[pa.RecordBatch], pa.RecordBatch],
) -> int:
    """O trabalho de cada lote do ``stream`` enquanto a consulta e a leitura seguem."""
    rows = 0
    with engine.stream(sa.select(SOURCE), batch_size=BATCH) as stream:
        for batch in stream:
            work(batch)
            rows += batch.num_rows
    return rows


def append_in_series(
    engine: Engine,
    batches: list[pa.RecordBatch],
    work: Callable[[pa.RecordBatch], pa.RecordBatch],
) -> int:
    """O trabalho de todos os lotes e depois o ``append`` deles."""
    produced = []
    for batch in batches:
        produced.append(work(batch))
    return engine.append(OUTPUT, produced)


def append_by_appender(
    engine: Engine,
    batches: list[pa.RecordBatch],
    work: Callable[[pa.RecordBatch], pa.RecordBatch],
) -> int:
    """Cada lote trabalhado entra no ``appender``, cuja thread grava o anterior."""
    with engine.appender(OUTPUT) as appender:
        for batch in batches:
            appender.write(work(batch))
    return appender.rows


def pipeline_in_series(
    engine: Engine,
    read_whole: Callable[[Engine], list[pa.RecordBatch]],
    work: Callable[[pa.RecordBatch], pa.RecordBatch],
) -> int:
    """O resultado inteiro, o trabalho de todos os lotes e o ``append`` deles."""
    return append_in_series(engine, read_whole(engine), work)


def pipeline_by_threads(
    engine: Engine,
    work: Callable[[pa.RecordBatch], pa.RecordBatch],
) -> int:
    """``stream`` e ``appender`` no mesmo ``with``: a leitura, o trabalho e a gravação juntos."""
    with (
        engine.stream(sa.select(SOURCE), batch_size=BATCH) as stream,
        engine.appender(OUTPUT) as appender,
    ):
        for batch in stream:
            appender.write(work(batch))
    return appender.rows


def emptied_after(
    engine: Engine,
    measured: Callable[[], int],
) -> Run:
    """Mede ``measured`` e esvazia a tabela de saída depois, fora do tempo."""
    run = timed(measured)
    engine.query(sa.delete(OUTPUT))
    return run


def emptied_variant(
    engine: Engine,
    function: Callable[..., int],
    *arguments: object,
) -> Callable[[], Run]:
    """A variante que mede ``function(*arguments)`` e esvazia a tabela de saída depois."""
    return functools.partial(emptied_after, engine, functools.partial(function, *arguments))


# ---------------------------------------------------------------- as consultas pequenas


def query_ranges(
    rows: int,
    count: int,
) -> list[tuple[int, int]]:
    """``count`` faixas iguais de ids espalhadas pela tabela, de até ``QUERY_WIDTH`` ids."""
    step = rows // count
    width = min(QUERY_WIDTH, step)
    ranges = []
    for index in range(count):
        low = index * step
        ranges.append((low, low + width - 1))
    return ranges


def count_in_ranges(
    engine: Engine,
    ranges: list[tuple[int, int]],
) -> int:
    """As linhas de cada faixa, uma consulta por faixa na sessão do motor, somadas."""
    statement = sa.select(sa.func.count().label("n")).where(
        SOURCE.c.id_lancamento.between(sa.bindparam("low"), sa.bindparam("high"))
    )
    total = 0
    for low, high in ranges:
        found = engine.query(statement, {"low": low, "high": high})
        total += found.column("n")[0].as_py()
    return total


def chunks_of(
    ranges: list[tuple[int, int]],
) -> list[list[tuple[int, int]]]:
    """As faixas repartidas entre ``WORKERS`` threads."""
    chunks = []
    for index in range(WORKERS):
        chunks.append(ranges[index::WORKERS])
    return chunks


def count_in_threads(
    engine: Engine,
    ranges: list[tuple[int, int]],
) -> int:
    """As faixas em ``WORKERS`` threads, todas na sessão principal."""
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = []
        for chunk in chunks_of(ranges):
            futures.append(pool.submit(count_in_ranges, engine, chunk))
        return sum(future.result() for future in futures)


def count_in_new_session(
    engine: Engine,
    ranges: list[tuple[int, int]],
) -> int:
    """As faixas numa sessão a mais, aberta e fechada aqui."""
    with engine.new_session() as session:
        return count_in_ranges(session, ranges)


def count_in_new_sessions(
    engine: Engine,
    ranges: list[tuple[int, int]],
) -> int:
    """As faixas em ``WORKERS`` threads, cada uma com a sua sessão a mais."""
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        futures = []
        for chunk in chunks_of(ranges):
            futures.append(pool.submit(count_in_new_session, engine, chunk))
        return sum(future.result() for future in futures)


# ---------------------------------------------------------------- as medidas de um motor


def engine_measures(
    engine: Engine,
    engine_label: str,
    read_whole: Callable[[Engine], list[pa.RecordBatch]],
    rows: int,
    repetitions: int,
    queries: int,
) -> None:
    """``stream``, ``appender``, os dois juntos e as consultas pequenas num motor com ``SOURCE`` e
    ``OUTPUT`` no sandbox."""
    batches = read_whole(engine)
    for work_name, work in WORKS.items():
        measure(
            f"{engine_label} stream, trabalho {work_name}",
            {
                "serie": timed_variant(work_in_series, engine, read_whole, work),
                "stream": timed_variant(work_on_stream, engine, work),
            },
            repetitions,
            rows,
        )
        measure(
            f"{engine_label} appender, trabalho {work_name}",
            {
                "serie": emptied_variant(engine, append_in_series, engine, batches, work),
                "appender": emptied_variant(engine, append_by_appender, engine, batches, work),
            },
            repetitions,
            rows,
        )
        measure(
            f"{engine_label} stream e appender, trabalho {work_name}",
            {
                "serie": emptied_variant(engine, pipeline_in_series, engine, read_whole, work),
                "threads": emptied_variant(engine, pipeline_by_threads, engine, work),
            },
            repetitions,
            rows,
        )
    ranges = query_ranges(rows, queries)
    expected = len(ranges) * (ranges[0][1] - ranges[0][0] + 1)
    measure(
        f"{engine_label} {queries} consultas pequenas",
        {
            "serie": timed_variant(count_in_ranges, engine, ranges),
            "threads na principal": timed_variant(count_in_threads, engine, ranges),
            "threads com sessão a mais": timed_variant(count_in_new_sessions, engine, ranges),
        },
        repetitions,
        expected,
    )


# ---------------------------------------------------------------- os pools por tabela


def ingest_one_by_one(
    run: Execution,
) -> int:
    """Uma chamada de ``run.ingest`` por tabela, na sessão principal."""
    for table in TABLES:
        run.ingest(table, materialize=True)
    return len(TABLES)


def ingest_together(
    run: Execution,
) -> int:
    """Uma chamada de ``run.ingest`` com as quatro tabelas, uma sessão a mais cada."""
    run.ingest(*TABLES, materialize=True)
    return len(TABLES)


def ingest_variant(
    db: Database,
    engine_name: str,
    ingest: Callable[[Execution], int],
    rows: int,
) -> Run:
    """Uma execução nova com a ingestão medida; as linhas de cada tabela no sandbox são conferidas
    depois, fora do tempo."""
    with Execution(db, engine_name, PARTITION) as run:
        measured = timed(functools.partial(ingest, run))
        check_rows(run.sandbox, rows, f"{engine_name} ingest")
    return measured


def publish_tables(
    run: Execution,
    workers: int,
) -> int:
    """``run.publish_delta`` das quatro tabelas, ``workers`` ao mesmo tempo."""
    versions = run.publish_delta(*TABLES, partitions=[PARTITION], max_workers=workers)
    return len(versions)


def publish_variant(
    db: Database,
    engine_name: str,
    workers: int,
) -> Run:
    """Uma execução nova que carrega e audita as quatro tabelas, fora do tempo, e mede a
    publicação delas no Delta, que substitui a partição."""
    with Execution(db, engine_name, PARTITION) as run:
        run.ingest(*TABLES, materialize=True)
        for table in TABLES:
            run.audit(table, [PARTITION])
        return timed(functools.partial(publish_tables, run, workers))


def materialize_one_by_one(
    reader: DeltaReader,
) -> int:
    """Uma chamada de ``materialize`` por tabela."""
    for table in TABLES:
        reader.materialize(table)
    return len(reader.materialized)


def materialize_together(
    reader: DeltaReader,
) -> int:
    """Uma chamada de ``materialize`` com as quatro tabelas, uma sessão a mais cada."""
    reader.materialize(*TABLES)
    return len(reader.materialized)


def materialize_variant(
    db: Database,
    materialize: Callable[[DeltaReader], int],
    rows: int,
) -> Run:
    """Um leitor Delta novo sobre a versão atual, com a materialização medida; as linhas são
    conferidas depois, fora do tempo."""
    with db.open_delta(channel="current") as reader:
        measured = timed(functools.partial(materialize, reader))
        check_rows(reader, rows, "materialize")
    return measured


def workers_variants(
    variant: Callable[..., Run],
    *arguments: object,
) -> dict[str, Callable[[], Run]]:
    """As variantes com ``max_workers`` 1 e ``WORKERS``, o número de tabelas por último."""
    return {
        "max_workers=1": functools.partial(variant, *arguments, 1),
        f"max_workers={WORKERS}": functools.partial(variant, *arguments, WORKERS),
    }


def ingest_variants(
    db: Database,
    engine_name: str,
    rows: int,
) -> dict[str, Callable[[], Run]]:
    """Uma chamada de ``run.ingest`` por tabela contra uma chamada com as quatro."""
    return {
        "uma por tabela": functools.partial(
            ingest_variant, db, engine_name, ingest_one_by_one, rows
        ),
        "juntas": functools.partial(ingest_variant, db, engine_name, ingest_together, rows),
    }


# ---------------------------------------------------------------- as seções


def duckdb_section(
    db: Database,
    rows: int,
    repetitions: int,
) -> None:
    """O motor DuckDB da máquina, com ``cad_paralelo_a`` materializada no sandbox."""
    with Execution(db, "duckdb", PARTITION) as run:
        run.ingest(SOURCE, materialize=True)
        run.sandbox.create_table(OUTPUT)
        engine_measures(run.sandbox, "duckdb", whole_by_query, rows, repetitions, DUCKDB_QUERIES)


def pools_section(
    db: Database,
    rows: int,
    repetitions: int,
) -> None:
    """Os pools por tabela do motor DuckDB e do leitor Delta sobre a raiz de trabalho."""
    count = len(TABLES)
    measure("duckdb ingest", ingest_variants(db, "duckdb", rows), repetitions, count)
    publish = workers_variants(publish_variant, db, "duckdb")
    measure("duckdb publish_delta", publish, repetitions, count)
    materialize = {
        "uma por tabela": functools.partial(materialize_variant, db, materialize_one_by_one, rows),
        "juntas": functools.partial(materialize_variant, db, materialize_together, rows),
    }
    measure("leitor materialize", materialize, repetitions, count)


def redshift_section(
    db: Database,
    rows: int,
    repetitions: int,
) -> None:
    """O motor Redshift: a ingestão das quatro tabelas, as medidas do motor sobre
    ``cad_paralelo_a`` e a publicação no Delta."""
    count = len(TABLES)
    measure("redshift ingest", ingest_variants(db, "redshift", rows), repetitions, count)
    with Execution(db, "redshift", PARTITION) as run:
        run.ingest(SOURCE)
        run.sandbox.create_table(OUTPUT)
        engine_measures(
            run.sandbox, "redshift", whole_by_stream, rows, repetitions, REDSHIFT_QUERIES
        )
    publish = workers_variants(publish_variant, db, "redshift")
    measure("redshift publish_delta", publish, repetitions, count)


def execute_all(
    config: RedshiftConfig,
    texts: list[str],
) -> None:
    """Os comandos numa conexão própria, em ordem."""
    connection = redshift.connect(config)
    try:
        cursor = connection.cursor()
        for text in texts:
            cursor.execute(text)
    finally:
        connection.close()


def control_table_exists(
    config: RedshiftConfig,
    control: str,
) -> bool:
    """Se a tabela de controle da publicação existe, por ``select 1 ... limit 0``."""
    try:
        execute_all(config, [f"SELECT 1 FROM {control} LIMIT 0"])
    except redshift_connector.Error as error:
        if redshift.relation_missing(error):
            return False
        raise
    return True


def publish_to_redshift(
    db: Database,
    config: RedshiftConfig,
    workers: int,
) -> int:
    """``publication.publish_redshift`` das quatro tabelas, ``workers`` ao mesmo tempo."""
    execution_id = f"paralelo-{uuid.uuid4().hex[:8]}"
    published = publication.publish_redshift(db, config, TABLES, execution_id, max_workers=workers)
    return len(published)


def publication_variant(
    db: Database,
    config: RedshiftConfig,
    workers: int,
) -> Run:
    """A publicação medida e a despublicação das quatro tabelas depois, fora do tempo."""
    run = timed(functools.partial(publish_to_redshift, db, config, workers))
    publication.unpublish_redshift(db, config, TABLES)
    return run


def publication_section(
    db: Database,
    repetitions: int,
) -> None:
    """A publicação das quatro tabelas no Redshift; no fim, as tabelas ``poc<id>_*`` e as linhas
    de controle delas saem, e a tabela de controle sai quando a sonda a criou."""
    config = RedshiftConfig.from_environment()
    control = f'"{config.schema}"."{publication.CONTROL_TABLE}"'
    created = not control_table_exists(config, control)
    if created:
        publication.create_publications_table(config)
    try:
        variants = workers_variants(publication_variant, db, config)
        measure("publish_redshift", variants, repetitions, len(TABLES))
    finally:
        cleanup = []
        for table in TABLES:
            cleanup.append(
                f'DROP TABLE IF EXISTS "{config.schema}"."{db.environment}_{table.name}"'
            )
        if created:
            cleanup.append(f"DROP TABLE {control}")
        else:
            cleanup.append(f"DELETE FROM {control} WHERE table_name LIKE '{db.environment}_%'")
        execute_all(config, cleanup)


# ---------------------------------------------------------------- a sonda


def parse_arguments() -> argparse.Namespace:
    """As opções da sonda."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rows", type=int, default=5_000_000, help="linhas de cada tabela")
    parser.add_argument("--repetitions", type=int, default=3, help="medidas de cada variante")
    parser.add_argument("--only", nargs="+", choices=SECTIONS, default=list(SECTIONS))
    return parser.parse_args()


def main() -> None:
    """As seções pedidas, o resumo e a checagem, com o relatório no terminal e em
    ``probes/output/``."""
    arguments = parse_arguments()
    work = lib.work_database("paralelo")
    needs_s3 = {"redshift", "publicacao"} & set(arguments.only)
    if needs_s3 and not work.storage.is_s3:
        print(
            f"{', '.join(sorted(needs_s3))} gravam no S3: a sonda pede SERIALIZE_DB_TEST_S3_ROOT",
            file=sys.stderr,
        )
        sys.exit(2)
    # A auditoria registra o SQL de cada verificação no INFO do log da execução, e cada medida
    # imprime a sua linha: do log da biblioteca, só os avisos.
    logging.getLogger("serialize_db").setLevel(logging.WARNING)
    db = Database(work.storage.uri, f"poc{uuid.uuid4().hex[:8]}", METADATA)
    print(
        f"ambiente das medidas: {db.environment}; {arguments.rows} linhas por tabela; "
        f"{arguments.repetitions} medidas por variante; seções {', '.join(arguments.only)}; "
        f"limites do DuckDB {environment_limits()}"
    )
    prepare(db, arguments.rows)

    # As seções na ordem de SECTIONS, cada uma só quando pedida.
    if "duckdb" in arguments.only:
        duckdb_section(db, arguments.rows, arguments.repetitions)
    if "pools" in arguments.only:
        pools_section(db, arguments.rows, arguments.repetitions)
    if "redshift" in arguments.only:
        redshift_section(db, arguments.rows, arguments.repetitions)
    if "publicacao" in arguments.only:
        publication_section(db, arguments.repetitions)

    print("resumo, o menor tempo de cada variante e a razão sobre a em série:")
    for label, runs in SUMMARY:
        print("  " + summary_line(label, runs))
    lib.check("cada medida contou as linhas ou as tabelas esperadas", PROBLEMS)
    lib.finish(work)


if __name__ == "__main__":
    lib.run(main)
