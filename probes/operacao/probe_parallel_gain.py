"""Sonda do ganho das APIs com threads sobre a execução em série, no ambiente alvo.

O ganho foi medido em 2026-10-04 num contêiner de 4 vCPUs, com o motor DuckDB numa pasta local, e
por esta sonda no ambiente alvo em 2026-10-05, com 8 vCPUs, o S3 e o Redshift (``docs/index.md``,
seção "Multithreading"). A sonda gera quatro tabelas iguais, ``cad_paralelo_a`` a
``cad_paralelo_d``, com ``--rows`` linhas cada numa partição, publica-as no Delta sob a raiz de
trabalho e mede cada variante ``--repetitions`` vezes, com a ordem invertida a cada repetição: o
tempo e o pico de memória residente do processo acima da base, zerado por ``/proc/self/clear_refs``
depois de devolver ao sistema a memória que o pool do Arrow e o ``malloc`` guardaram das medidas
anteriores. Cada seção de ``--only`` compara a forma em série, a primeira, com a forma com threads:

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
- ``publicacao``: ``publication.publish_redshift`` das quatro com ``max_workers=4`` contra 1,
  primeiro com a despublicação depois de cada medida, fora do tempo, e depois sem ela, cada medida
  trocando a versão publicada pela outra de duas versões do Delta com as mesmas linhas: a da
  partição escrita de novo antes dessas medidas e a de antes, publicada fora delas; e a primeira
  publicação das quatro pela própria sonda, em quatro conexões, com os comandos de
  ``publication_statements`` em três ordens, a da publicação (o ``CREATE TABLE``, a carga e o
  ``INSERT`` da linha de controle), a linha de controle logo depois do ``CREATE TABLE``, antes da
  staging e do ``COPY``, e as tabelas criadas e confirmadas antes da transação, fora do tempo, com
  as tabelas e as linhas de controle removidas depois de cada medida; as três ordens separam a
  hipótese de 2026-10-10, de que o planejamento do ``INSERT`` da linha de controle espera o
  ``CREATE TABLE`` sem commit das outras transações (``.claude/memory/concurrency.md``). Cada
  medida imprime também, lidos no log ``serialize_db.publication.commands``, de cada tipo de
  comando e da abertura das conexões, quantos houve, a soma e o maior tempo, e a linha do tempo de
  cada conexão, com o início e a duração de cada comando; e os locks que uma quinta sessão leu em
  ``svv_transactions`` a cada segundo durante a medida, os pendentes e os concedidos no objeto de
  um pendente. No fim da seção, as gravações da linha de controle de todas as medidas no
  ``sys_query_history``, com a execução que gravou cada uma e o tempo de fila, de espera por lock,
  de execução e de planejamento.

Checagem: cada medida contou as linhas ou as tabelas esperadas, a publicação sem despublicação
pelo ``UPDATE`` da linha de controle de cada tabela, a publicação pela sonda pelas transações
confirmadas, e as linhas de cada tabela que o ``ingest`` e o ``materialize`` trouxeram são as
geradas. Leituras: cada medida e, no resumo, o menor tempo de cada variante, o pico dessa medida e
a razão da primeira variante, a em série ou a ordem da publicação, sobre ela.

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
import datetime
import functools
import gc
import itertools
import logging
import operator
import re
import sys
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import operation_lib as lib
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import redshift_connector
import sqlalchemy as sa

from serialize_db import delta, publication
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
# A quinta sessão lê os locks a cada segundo durante cada publicação medida, e a leitura das
# gravações da linha de controle no sys_query_history espera as que faltam por até 120 s, de 10 em
# 10 s.
LOCK_POLL_SECONDS = 1.0
HISTORY_WAIT_SECONDS = 120
HISTORY_RETRY_SECONDS = 10
# Os tipos de comando que gravam a linha de controle, o nome das tabelas da sonda num texto e a
# execução que gravou a linha, no texto do INSERT ou do UPDATE; a da publicação pela sonda leva o
# nome da ordem dos comandos.
CONTROL_WRITES = ("INSERT controle", "UPDATE controle")
PARALLEL_TABLE = re.compile(r"cad_paralelo_[a-d]")
EXECUTION_ID = re.compile(r"paralelo-[a-z0-9-]+")
# O log em que a publicação dá o tempo da abertura de cada conexão e de cada comando, e em que a
# publicação pela sonda dá os seus.
COMMAND_LOG = logging.getLogger("serialize_db.publication.commands")
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
    variante, a em série ou a ordem da publicação, sobre o dela."""
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
    versions: Mapping[str, int] | None = None,
) -> int:
    """``publication.publish_redshift`` das quatro tabelas, ``workers`` ao mesmo tempo, nas
    ``versions`` do Delta ou, sem elas, na versão atual de cada tabela."""
    execution_id = f"paralelo-{uuid.uuid4().hex[:8]}"
    published = publication.publish_redshift(
        db, config, TABLES, execution_id, max_workers=workers, versions=versions
    )
    return len(published)


def current_versions(
    db: Database,
) -> dict[str, int]:
    """A versão atual de cada tabela no Delta."""
    versions = {}
    for table in TABLES:
        versions[table.name] = delta.open_table(db.uri(table), db.storage).version()
    return versions


def rewrite_partition(
    db: Database,
) -> dict[str, int]:
    """A partição das quatro tabelas escrita de novo no Delta, com as mesmas linhas, por uma
    execução do motor DuckDB; devolve a versão nova de cada tabela."""
    with Execution(db, "duckdb", PARTITION) as run:
        run.ingest(*TABLES, materialize=True)
        for table in TABLES:
            run.audit(table, [PARTITION])
        return run.publish_delta(*TABLES, partitions=[PARTITION], max_workers=WORKERS)


# ---------------------------------------------------------------- os comandos da publicação


@dataclasses.dataclass
class Command:
    """Um comando da publicação lido no log ``serialize_db.publication.commands``: o tipo, a tabela
    ``cad_paralelo_*`` que o texto cita, ``None`` sem nenhuma, a thread que o rodou, o início pelo
    relógio do sistema, em segundos desde a época, e a duração em segundos."""

    kind: str
    table: str | None
    thread: int
    started: float
    seconds: float


class CommandTimes(logging.Handler):
    """Os comandos que o log ``serialize_db.publication.commands`` dá em ``DEBUG``: a abertura de
    cada conexão e cada comando, na ordem em que terminaram."""

    def __init__(self) -> None:
        super().__init__(level=logging.DEBUG)
        self.commands: list[Command] = []

    def emit(
        self,
        record: logging.LogRecord,
    ) -> None:
        """Guarda o comando da linha, que sai no fim dele, na thread que o rodou, com o texto e os
        segundos nos argumentos."""
        text, seconds = record.args
        table = None
        match = PARALLEL_TABLE.search(text)
        if match:
            table = match.group(0)
        started = record.created - seconds
        self.commands.append(Command(command_kind(text), table, record.thread, started, seconds))


def command_kind(
    text: str,
) -> str:
    """O tipo de um comando da publicação no resumo: a primeira palavra, ``CREATE TEMP`` na
    staging, com ``controle`` no que lê ou grava a tabela de controle; ``conexão`` na abertura."""
    words = text.split()
    kind = words[0]
    if kind == "CREATE" and words[1] == "TEMP":
        kind = "CREATE TEMP"
    if publication.CONTROL_TABLE in text:
        kind += " controle"
    return kind


def count_commands(
    commands: list[Command],
    kinds: tuple[str, ...],
) -> int:
    """Quantos comandos são de um dos tipos ``kinds``."""
    count = 0
    for command in commands:
        if command.kind in kinds:
            count += 1
    return count


def commands_line(
    commands: list[Command],
) -> str:
    """Cada tipo de comando, na ordem em que apareceu: quantos, a soma e o maior tempo."""
    seconds_by_kind: dict[str, list[float]] = {}
    for command in commands:
        seconds_by_kind.setdefault(command.kind, []).append(command.seconds)
    parts = []
    for kind, values in seconds_by_kind.items():
        parts.append(f"{kind} {len(values)}x, soma {sum(values):.2f} s, máx {max(values):.2f} s")
    return "; ".join(parts)


def connections_of(
    commands: list[Command],
) -> list[list[Command]]:
    """Os comandos de cada conexão, na ordem da abertura: a abertura numa thread começa uma
    conexão, que leva os comandos seguintes da thread até a próxima abertura nela."""
    connections = []
    current: dict[int, list[Command]] = {}
    for command in commands:
        if command.kind == "conexão":
            current[command.thread] = []
            connections.append(current[command.thread])
        current[command.thread].append(command)
    return sorted(connections, key=lambda connection: connection[0].started)


def connection_table(
    connection: list[Command],
) -> str:
    """A primeira tabela ``cad_paralelo_*`` que os comandos da conexão citam."""
    for command in connection:
        if command.table is not None:
            return command.table
    return "sem tabela"


def clock(
    seconds: float,
) -> str:
    """O instante, em segundos desde a época, na hora UTC com milissegundos."""
    moment = datetime.datetime.fromtimestamp(seconds, datetime.UTC)
    return moment.strftime("%H:%M:%S.%f")[:-3]


def timeline_lines(
    commands: list[Command],
    origin: float,
) -> list[str]:
    """Uma linha por conexão, na ordem da abertura: a tabela que os comandos dela citam e cada
    comando com o início, em segundos desde ``origin``, e a duração."""
    lines = []
    for number, connection in enumerate(connections_of(commands), start=1):
        steps = []
        for command in connection:
            steps.append(f"{command.kind} {command.started - origin:.3f}+{command.seconds:.3f}")
        lines.append(f"conexão {number}, {connection_table(connection)}: " + "; ".join(steps))
    return lines


# ---------------------------------------------------------------- a quinta sessão


@dataclasses.dataclass
class FifthSession:
    """A quinta sessão, que lê os locks durante as publicações: a conexão e o pid dela, cujas
    linhas ficam fora das leituras."""

    connection: object
    pid: int


@dataclasses.dataclass
class LockReading:
    """Uma leitura de ``svv_transactions``: o envio pelo relógio do sistema, em segundos desde a
    época, as linhas das outras sessões, na ordem das colunas de ``LOCKS_QUERY``, e o erro que
    encerrou as leituras, ``None`` na leitura que respondeu."""

    sent: float
    rows: list[tuple]
    error: str | None = None


@dataclasses.dataclass(frozen=True)
class Lock:
    """Um lock lido em ``svv_transactions``, a chave das leituras que o viram: o pid, o modo, o
    tipo do objeto, a relação, ``None`` no tipo ``transactionid``, e se foi concedido."""

    pid: int
    mode: str
    kind: str
    relation: int | None
    granted: bool


LOCKS_QUERY = "SELECT pid, lock_mode, lockable_object_type, relation, granted FROM svv_transactions"


def open_fifth_session(
    config: RedshiftConfig,
) -> FifthSession:
    """A conexão da quinta sessão e o pid dela."""
    connection = redshift.connect(config)
    cursor = connection.cursor()
    cursor.execute("SELECT pg_backend_pid()")
    (pid,) = cursor.fetchone()
    return FifthSession(connection, int(pid))


def read_locks(
    fifth: FifthSession,
    stop: threading.Event,
    readings: list[LockReading],
) -> None:
    """Lê ``svv_transactions`` a cada ``LOCK_POLL_SECONDS`` até ``stop``, sem as linhas da própria
    sessão; o primeiro erro vai à última leitura e encerra as leituras."""
    cursor = fifth.connection.cursor()
    while True:
        sent = time.time()
        try:
            cursor.execute(LOCKS_QUERY)
            rows = cursor.fetchall()
        except (redshift_connector.Error, OSError) as error:
            readings.append(LockReading(sent, [], f"{type(error).__name__}: {error}"))
            return
        others = [tuple(row) for row in rows if row[0] != fifth.pid]
        readings.append(LockReading(sent, others))
        if stop.wait(LOCK_POLL_SECONDS):
            return


def readings_line(
    readings: list[LockReading],
    origin: float,
) -> str:
    """Quantas leituras responderam, de quando a quando, em segundos desde ``origin``, e os pids
    que elas viram."""
    answered = [reading for reading in readings if reading.error is None]
    if not answered:
        return "nenhuma leitura de svv_transactions respondeu"
    pids = set()
    for reading in answered:
        for row in reading.rows:
            pids.add(row[0])
    first = answered[0].sent - origin
    last = answered[-1].sent - origin
    listed = ", ".join(str(pid) for pid in sorted(pids)) or "nenhum"
    return (
        f"{len(answered)} leitura(s) de svv_transactions de {first:+.1f} s a {last:+.1f} s; "
        f"pids vistos: {listed}"
    )


def locks_seen(
    readings: list[LockReading],
    origin: float,
) -> dict[Lock, list[float]]:
    """Os instantes das leituras que viram cada lock, em segundos desde ``origin``."""
    seen: dict[Lock, list[float]] = {}
    for reading in readings:
        for pid, mode, kind, relation, granted in reading.rows:
            lock = Lock(pid, str(mode).strip(), str(kind).strip(), relation, bool(granted))
            seen.setdefault(lock, []).append(reading.sent - origin)
    return seen


def lock_line(
    lock: Lock,
    offsets: list[float],
) -> str:
    """Um lock: o pid, se ele tem ou espera o lock, o modo, o tipo e a relação, e a primeira e a
    última leitura que o viram."""
    state = "tem" if lock.granted else "espera"
    target = lock.kind if lock.relation is None else f"{lock.kind} {lock.relation}"
    return (
        f"pid {lock.pid} {state} {lock.mode} em {target}: de {offsets[0]:+.1f} s a "
        f"{offsets[-1]:+.1f} s, em {len(offsets)} leitura(s)"
    )


def lock_lines(
    readings: list[LockReading],
    origin: float,
) -> list[str]:
    """Os locks lidos durante a publicação, em segundos desde ``origin``: cada lock pendente e cada
    lock concedido no objeto de um pendente, na ordem da primeira leitura que o viu, ou ``nenhum
    lock pendente`` quando alguma leitura respondeu; e o erro que encerrou as leituras."""
    seen = locks_seen(readings, origin)
    # O objeto de cada lock pendente: o tipo e a relação.
    waited = set()
    for lock in seen:
        if not lock.granted:
            waited.add((lock.kind, lock.relation))
    answered = [reading for reading in readings if reading.error is None]
    lines = []
    if answered and not waited:
        lines.append("nenhum lock pendente")
    for lock, offsets in sorted(seen.items(), key=lambda item: item[1][0]):
        if (lock.kind, lock.relation) in waited:
            lines.append(lock_line(lock, offsets))
    if readings and readings[-1].error is not None:
        stopped = readings[-1].sent - origin
        lines.append(f"a leitura parou em {stopped:+.1f} s: {readings[-1].error}")
    return lines


# ---------------------------------------------------------------- as gravações da linha de controle


def control_history_query(
    since: float,
    environment: str,
) -> str:
    """A consulta dos comandos do ``sys_query_history`` que citam a tabela de controle e o
    ambiente, desde ``since``, em segundos desde a época, na ordem do início."""
    moment = datetime.datetime.fromtimestamp(since, datetime.UTC).strftime("%Y-%m-%d %H:%M:%S")
    return (
        "SELECT session_id, transaction_id, start_time, status, elapsed_time, queue_time, "
        "lock_wait_time, execution_time, compile_time, planning_time, query_text "
        f"FROM sys_query_history WHERE start_time >= '{moment}' "
        f"AND query_text ILIKE '%{publication.CONTROL_TABLE}%' "
        f"AND query_text ILIKE '%{environment}%' ORDER BY start_time"
    )


def control_writes(
    rows: list[tuple],
) -> list[tuple]:
    """As linhas do ``INSERT`` e do ``UPDATE`` da linha de controle, pela primeira palavra do
    texto, o último campo da linha."""
    writes = []
    for row in rows:
        words = str(row[-1]).split()
        if words and words[0].upper() in ("INSERT", "UPDATE"):
            writes.append(row)
    return writes


def seconds_of(
    microseconds: int | None,
) -> str:
    """Os microssegundos do ``sys_query_history`` em segundos; ``-`` sem valor."""
    if microseconds is None:
        return "-"
    return f"{microseconds / 1_000_000:.3f} s"


def history_line(
    row: tuple,
) -> str:
    """Uma gravação da linha de controle: o início em UTC, o pid da sessão, a transação, a tabela,
    o comando com a execução que o gravou entre parênteses, ``sem execução`` quando o texto não a
    traz, o estado e os tempos do ``sys_query_history``."""
    (
        session,
        transaction,
        start,
        status,
        elapsed,
        queue,
        lock_wait,
        execution,
        compile_time,
        planning,
        text,
    ) = row
    table = "sem tabela"
    match = PARALLEL_TABLE.search(str(text))
    if match:
        table = match.group(0)
    execution_id = "sem execução"
    match = EXECUTION_ID.search(str(text))
    if match:
        execution_id = match.group(0)
    command = str(text).split()[0].upper()
    return (
        f"{start.strftime('%H:%M:%S.%f')[:-3]} UTC, pid {session}, transação {transaction}, "
        f"{table}, {command} controle ({execution_id}), {str(status).strip()}: decorrido "
        f"{seconds_of(elapsed)}, "
        f"fila {seconds_of(queue)}, lock {seconds_of(lock_wait)}, execução "
        f"{seconds_of(execution)}, compilação {seconds_of(compile_time)}, planejamento "
        f"{seconds_of(planning)}"
    )


def read_control_history(
    fifth: FifthSession,
    since: float,
    environment: str,
    expected: int,
) -> list[str]:
    """As gravações da linha de controle no ``sys_query_history``: a primeira linha com quantas a
    leitura achou das ``expected`` e uma linha por gravação; a leitura se repete a cada
    ``HISTORY_RETRY_SECONDS`` até achar as ``expected`` ou passar ``HISTORY_WAIT_SECONDS``, e o
    erro da consulta vai à primeira linha."""
    query = control_history_query(since, environment)
    deadline = time.monotonic() + HISTORY_WAIT_SECONDS
    cursor = fifth.connection.cursor()
    while True:
        try:
            cursor.execute(query)
            writes = control_writes(list(cursor.fetchall()))
        except (redshift_connector.Error, OSError) as error:
            return [f"a consulta falhou: {type(error).__name__}: {error}"]
        if len(writes) >= expected or time.monotonic() >= deadline:
            break
        time.sleep(HISTORY_RETRY_SECONDS)
    lines = [f"{len(writes)} de {expected}"]
    for row in writes:
        lines.append(history_line(row))
    return lines


# ---------------------------------------------------------------- as medidas da publicação


def observed_publication(
    label: str,
    measured: Callable[[], int],
    commands: CommandTimes,
    fifth: FifthSession,
) -> tuple[Run, list[Command]]:
    """Mede a publicação com a quinta sessão lendo os locks numa thread, que para no ``finally``,
    e imprime a linha de cada tipo de comando, a linha do tempo de cada conexão e os locks lidos;
    devolve a medida e os comandos da publicação."""
    first = len(commands.commands)
    readings: list[LockReading] = []
    stop = threading.Event()
    reader = threading.Thread(target=read_locks, args=(fifth, stop, readings), name="quinta-sessao")
    reader.start()
    try:
        run = timed(measured)
    finally:
        stop.set()
        reader.join()

    # As linhas da publicação, com os instantes contados da abertura da primeira conexão.
    published = commands.commands[first:]
    origin = min(command.started for command in published)
    print(f"{label}, comandos: {commands_line(published)}")
    print(f"{label}, linha do tempo, início {clock(origin)} UTC:")
    for line in timeline_lines(published, origin):
        print(f"  {line}")
    print(f"{label}, quinta sessão: {readings_line(readings, origin)}")
    for line in lock_lines(readings, origin):
        print(f"  {line}")
    return run, published


def publication_variant(
    db: Database,
    config: RedshiftConfig,
    commands: CommandTimes,
    fifth: FifthSession,
    workers: int,
) -> Run:
    """A publicação medida, com as linhas de ``observed_publication``, e a despublicação das quatro
    tabelas depois, fora do tempo e das linhas."""
    label = f"publish_redshift, max_workers={workers}"
    measured = functools.partial(publish_to_redshift, db, config, workers)
    run, _ = observed_publication(label, measured, commands, fifth)
    publication.unpublish_redshift(db, config, TABLES)
    return run


def prepare_republication(
    db: Database,
    config: RedshiftConfig,
) -> Iterator[dict[str, int]]:
    """Prepara a publicação sem despublicação: a partição das quatro tabelas escrita de novo no
    Delta e a publicação, fora da medida, da versão de antes dessa escrita, que cria as tabelas
    publicadas e as linhas de controle; devolve o ciclo das versões que as medidas publicam, a nova
    e a de antes, uma de cada vez."""
    previous = current_versions(db)
    rewritten = rewrite_partition(db)
    started = time.perf_counter()
    publish_to_redshift(db, config, WORKERS, previous)
    seconds = time.perf_counter() - started
    print(
        f"publish_redshift sem despublicar: a partição escrita de novo no Delta nas versões "
        f"{dict(sorted(rewritten.items()))}, e as de antes, {previous}, publicadas fora da medida "
        f"em {seconds:.1f} s"
    )
    return itertools.cycle([rewritten, previous])


def republication_variant(
    db: Database,
    config: RedshiftConfig,
    commands: CommandTimes,
    fifth: FifthSession,
    alternation: Iterator[dict[str, int]],
    workers: int,
) -> Run:
    """A publicação medida da próxima versão de ``alternation`` sobre a publicada, sem
    despublicação entre as medidas, com as linhas de ``observed_publication``; a medida conta as
    linhas de controle gravadas pelo ``UPDATE``, uma por tabela que trocou de versão."""
    label = f"publish_redshift sem despublicar, max_workers={workers}"
    measured = functools.partial(publish_to_redshift, db, config, workers, next(alternation))
    run, published = observed_publication(label, measured, commands, fifth)
    return dataclasses.replace(run, count=count_commands(published, ("UPDATE controle",)))


# ---------------------------------------------------------------- a publicação pela sonda


@dataclasses.dataclass(frozen=True)
class Ordering:
    """Uma ordem dos comandos da primeira publicação de uma tabela pela sonda: o rótulo da medida,
    se o ``CREATE TABLE`` roda e é confirmado antes da transação, fora do tempo, e se a linha de
    controle entra logo depois do ``CREATE TABLE``, antes da staging e do ``COPY``."""

    label: str
    created_before: bool
    control_first: bool


# As ordens medidas, pelo nome que entra no execution_id, a da publicação primeiro. Elas separam a
# hipótese de 2026-10-10, de que o planejamento do INSERT da linha de controle espera o CREATE
# TABLE sem commit das outras transações: com ela, a linha antes do COPY espera enquanto nenhuma
# transação confirmou, e as tabelas criadas antes não esperam.
ORDERINGS = {
    "publicacao": Ordering("ordem da publicação", created_before=False, control_first=False),
    "linha-antes": Ordering(
        "linha de controle antes do COPY", created_before=False, control_first=True
    ),
    "criadas-antes": Ordering(
        "tabelas criadas antes da transação", created_before=True, control_first=False
    ),
}


def first_publication_statements(
    db: Database,
    config: RedshiftConfig,
    table: sa.Table,
    execution_id: str,
) -> list[str]:
    """Os comandos da primeira publicação da tabela na versão atual do Delta, como
    ``publication_statements`` os dá: o ``CREATE TABLE`` primeiro, a staging, a partição e o
    ``INSERT`` da linha de controle por último, com os manifestos gravados em
    ``<ambiente>/publicacao/<execução>/`` e a cláusula de credenciais montada agora."""
    uri = db.uri(table)
    version = delta.open_table(uri, db.storage).version()
    folder = db.storage.join(db.publication_prefix(execution_id), table.name, PARTITION)
    manifests = delta.copy_manifest(
        uri, version, [PARTITION], db.storage.uri_of(folder), db.storage
    )
    return publication.publication_statements(
        config.schema,
        db.environment,
        table,
        [PARTITION],
        {PARTITION: manifests},
        version,
        None,
        execution_id,
        redshift.credentials_clause(config),
    )


def ordered_statements(
    statements: list[str],
    ordering: Ordering,
) -> list[str]:
    """Os comandos da transação na ordem pedida: sem o ``CREATE TABLE``, o primeiro, quando a
    tabela foi criada antes, e com a linha de controle, o último, logo depois dele quando ela vem
    antes do ``COPY``."""
    create, *load, control = statements
    if ordering.created_before:
        return [*load, control]
    if ordering.control_first:
        return [create, control, *load]
    return statements


def execute_logged(
    connection: object,
    text: str,
) -> None:
    """Roda um comando num cursor novo e dá o tempo dele, com o texto mascarado, no log
    ``serialize_db.publication.commands``, como a publicação dá os seus."""
    started = time.perf_counter()
    try:
        connection.cursor().execute(text)
    finally:
        COMMAND_LOG.debug("%s em %.3f s", redshift.mask(text), time.perf_counter() - started)


def publish_one_in_order(
    db: Database,
    config: RedshiftConfig,
    execution_id: str,
    ordering: Ordering,
    table: sa.Table,
) -> int:
    """A transação da primeira publicação de uma tabela pela sonda, numa conexão própria, como a da
    publicação: o ``BEGIN``, a leitura da linha de controle, os comandos na ordem e o ``COMMIT``;
    devolve 1 quando confirmou e 0 com o erro do servidor impresso, e a conexão fechada descarta a
    transação aberta."""
    started = time.perf_counter()
    connection = redshift.connect(config)
    COMMAND_LOG.debug("%s em %.3f s", "conexão", time.perf_counter() - started)
    try:
        execute_logged(connection, "BEGIN")
        execute_logged(connection, publication.control_read(config.schema, db.environment, table))
        statements = first_publication_statements(db, config, table, execution_id)
        for text in ordered_statements(statements, ordering):
            execute_logged(connection, text)
        execute_logged(connection, "COMMIT")
    except redshift_connector.Error as error:
        print(
            f"{table.name}, {execution_id}: a transação falhou: {type(error).__name__}: "
            f"{redshift.mask(str(error))}"
        )
        return 0
    finally:
        connection.close()
    return 1


def publish_in_order(
    db: Database,
    config: RedshiftConfig,
    execution_id: str,
    ordering: Ordering,
) -> int:
    """A primeira publicação das quatro tabelas pela sonda, uma transação por tabela em ``WORKERS``
    threads; devolve quantas confirmaram."""
    publish = functools.partial(publish_one_in_order, db, config, execution_id, ordering)
    with ThreadPoolExecutor(max_workers=WORKERS) as pool:
        committed = list(pool.map(publish, TABLES))
    return sum(committed)


def removal_statements(
    config: RedshiftConfig,
    environment: str,
) -> list[str]:
    """Os comandos que apagam as tabelas publicadas da sonda, existam ou não, e as linhas de
    controle delas."""
    statements = []
    for table in TABLES:
        statements.append(f'DROP TABLE IF EXISTS "{config.schema}"."{environment}_{table.name}"')
    control = f'"{config.schema}"."{publication.CONTROL_TABLE}"'
    statements.append(f"DELETE FROM {control} WHERE table_name LIKE '{environment}_%'")
    return statements


def ordering_variant(
    db: Database,
    config: RedshiftConfig,
    commands: CommandTimes,
    fifth: FifthSession,
    name: str,
) -> Run:
    """A primeira publicação das quatro tabelas pela sonda, em quatro conexões, na ordem ``name``
    de ``ORDERINGS``, com as linhas de ``observed_publication``; o ``CREATE TABLE`` das tabelas
    criadas antes, confirmado um a um numa conexão própria, e a remoção das tabelas e das linhas
    de controle depois ficam fora do tempo."""
    ordering = ORDERINGS[name]
    if ordering.created_before:
        creates = []
        for table in TABLES:
            creates.append(publication.published_ddl(config.schema, db.environment, table))
        execute_all(config, creates)
    execution_id = f"paralelo-{name}-{uuid.uuid4().hex[:8]}"
    measured = functools.partial(publish_in_order, db, config, execution_id, ordering)
    label = f"publicação pela sonda, {ordering.label}"
    run, _ = observed_publication(label, measured, commands, fifth)
    execute_all(config, removal_statements(config, db.environment))
    return run


def ordering_variants(
    db: Database,
    config: RedshiftConfig,
    commands: CommandTimes,
    fifth: FifthSession,
) -> dict[str, Callable[[], Run]]:
    """As variantes da publicação pela sonda, uma por ordem de ``ORDERINGS``, pelo rótulo."""
    variants = {}
    for name, ordering in ORDERINGS.items():
        variants[ordering.label] = functools.partial(
            ordering_variant, db, config, commands, fifth, name
        )
    return variants


def publication_measures(
    db: Database,
    config: RedshiftConfig,
    commands: CommandTimes,
    fifth: FifthSession,
    repetitions: int,
) -> None:
    """As medidas da publicação com a despublicação depois de cada uma, sem ela e pela própria sonda
    nas ordens de ``ORDERINGS``, e as gravações da linha de controle de todas no
    ``sys_query_history``."""
    started = time.time()
    variants = workers_variants(publication_variant, db, config, commands, fifth)
    measure("publish_redshift", variants, repetitions, len(TABLES))
    alternation = prepare_republication(db, config)
    variants = workers_variants(republication_variant, db, config, commands, fifth, alternation)
    measure("publish_redshift sem despublicar", variants, repetitions, len(TABLES))

    # A publicação pela sonda, nas ordens de ORDERINGS, com as tabelas publicadas removidas antes,
    # fora da medida.
    execute_all(config, removal_statements(config, db.environment))
    variants = ordering_variants(db, config, commands, fifth)
    measure("publicação pela sonda, quatro conexões", variants, repetitions, len(TABLES))

    # As gravações desde dez minutos antes das medidas, a margem para a diferença entre o relógio
    # da máquina e o do servidor.
    expected = count_commands(commands.commands, CONTROL_WRITES)
    lines = read_control_history(fifth, started - 600, db.environment, expected)
    print(f"publish_redshift, gravações da linha de controle no sys_query_history: {lines[0]}")
    for line in lines[1:]:
        print(f"  {line}")


def publication_section(
    db: Database,
    repetitions: int,
) -> None:
    """A publicação das quatro tabelas no Redshift, com as medidas de ``publication_measures``, os
    tempos de cada comando lidos no log ``serialize_db.publication.commands`` e a quinta sessão
    aberta para elas; no fim, as tabelas ``poc<id>_*`` e as linhas de controle delas saem, e a
    tabela de controle sai quando a sonda a criou."""
    config = RedshiftConfig.from_environment()
    control = f'"{config.schema}"."{publication.CONTROL_TABLE}"'
    created = not control_table_exists(config, control)
    if created:
        publication.create_publications_table(config)
    commands = CommandTimes()
    COMMAND_LOG.setLevel(logging.DEBUG)
    COMMAND_LOG.propagate = False
    COMMAND_LOG.addHandler(commands)
    fifth = None
    try:
        fifth = open_fifth_session(config)
        publication_measures(db, config, commands, fifth, repetitions)
    finally:
        if fifth is not None:
            fifth.connection.close()
        COMMAND_LOG.removeHandler(commands)
        COMMAND_LOG.propagate = True
        COMMAND_LOG.setLevel(logging.NOTSET)
        cleanup = removal_statements(config, db.environment)
        if created:
            cleanup.append(f"DROP TABLE {control}")
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

    print("resumo, o menor tempo de cada variante e a razão da primeira variante sobre ela:")
    for label, runs in SUMMARY:
        print("  " + summary_line(label, runs))
    lib.check("cada medida contou as linhas ou as tabelas esperadas", PROBLEMS)
    lib.finish(work)


if __name__ == "__main__":
    lib.run(main)
