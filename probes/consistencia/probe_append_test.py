"""Dois escritores na mesma tabela do sandbox, o que ``append`` torna possível desde 2026-09-28:
dois ``append`` (o ``INSERT ... BY NAME`` no DuckDB, o ``COPY`` no Redshift) ao mesmo tempo em
sessões a mais, os dois na sessão principal em threads, um ``append`` ao lado de um ``UPDATE`` da
mesma tabela, e o ``create_table`` numa sessão a mais e na principal enquanto a principal roda um
``stream``. O desfecho de cada escrita (entrou, ou foi recusada com conflito) é leitura; a checagem
é a consistência do que ficou: as linhas de cada escrita que entrou, inteiras e sem id repetido, e
o ``UPDATE`` só nas linhas que ele alcançou. Pelo pytest com as fixtures das suítes: o motor
DuckDB na pasta local, o Redshift no substituto de ``tests/emulator.py`` ou no ambiente alvo.

.. code-block:: shell

    SERIALIZE_DB_TEST_LOCAL_ROOT=$HOME/serialize-db-local PYTHONPATH=tests \\
        .venv/bin/python -m pytest -p conftest -m local -s probes/consistencia/probe_append_test.py
    PYTHONPATH=tests .venv/bin/python -m pytest -p conftest -m redshift -s \\
        probes/consistencia/probe_append_test.py
    SERIALIZE_DB_TEST_EMULATOR=1 PYTHONPATH=tests .venv/bin/python -m pytest -p conftest \\
        -m redshift -s probes/consistencia/probe_append_test.py
"""
from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Protocol

import pyarrow as pa
import pyarrow.compute as pc
import pytest
import sqlalchemy as sa

from conftest import LocalLocation, S3Location, describe_error, redshift_config
from consistency_lib import report
from lancamentos_model import MONTHS, PROJECTED, entry_rows
from serialize_db.engine import redshift
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.engine.redshift import RedshiftEngine
from serialize_db.schema import quoted
from serialize_db.storage import Storage

# As linhas de cada escritor e as rodadas dos dois escritores, por motor: o COPY do Redshift tem
# custo fixo perto de 1 s, e a sobreposição não depende do volume.
ROWS = {"duckdb": 200_000, "redshift": 20_000}
ROUNDS = {"duckdb": 5, "redshift": 3}
# A consulta do stream no DuckDB: um md5 por linha, para a consulta durar mais que os CREATE.
STREAM_ROWS = 5_000_000
# A espera antes dos CREATE, para o stream já estar rodando a consulta sob o lock.
HEAD_START = 0.2
TOLERANCE = 1e-6
Outcome = tuple[bool, str, float]


class Writer(Protocol):
    """O que a sonda faz em cada motor, pelo ``create_table`` e o ``append`` dele."""

    engine: DuckDBEngine | RedshiftEngine

    def create(self, session: object, table: sa.Table) -> None: ...
    def append(self, session: object, table: sa.Table, rows: pa.Table) -> None: ...
    def update(self, table: sa.Table, limit: int) -> None: ...
    def totals(self, table: sa.Table) -> tuple[int, int, float]: ...
    def long_query(self, table: sa.Table) -> sa.sql.ClauseElement | str: ...
    def describe(self, error: Exception) -> str: ...


class DuckDBWriter:
    """As escritas do motor DuckDB: ``create_table`` e ``append``, o ``INSERT ... BY NAME`` do
    arquivo de transbordo do appender."""

    def __init__(self, engine: DuckDBEngine) -> None:
        self.engine = engine

    def create(self, session: DuckDBEngine, table: sa.Table) -> None:
        session.create_table(table)

    def append(self, session: DuckDBEngine, table: sa.Table, rows: pa.Table) -> None:
        session.append(table, rows)

    def update(self, table: sa.Table, limit: int) -> None:
        self.engine.query(sa.update(table).where(table.c.id_lancamento <= limit)
                          .values(valor=table.c.valor + 1))

    def totals(self, table: sa.Table) -> tuple[int, int, float]:
        found = self.engine.query(
            f'SELECT count(*) AS n, count(DISTINCT "id_lancamento") AS d, '
            f'coalesce(sum("valor"), 0.0) AS s FROM {quoted(table.name)}')
        row = found.to_pylist()[0]
        return row["n"], row["d"], float(row["s"])

    def long_query(self, table: sa.Table) -> str:
        self.engine.query(
            f"CREATE TABLE fonte AS SELECT range AS id FROM range({STREAM_ROWS})")
        return "SELECT id, md5(id::VARCHAR) AS s FROM fonte"

    def describe(self, error: Exception) -> str:
        first_line = str(error).splitlines()[0] if str(error) else ""
        return f"{type(error).__name__}: {first_line[:200]}"


class RedshiftWriter:
    """As escritas do motor Redshift: ``create_table`` e ``append``, o ``COPY`` do Parquet do
    appender no ``staging/`` da execução, pela staging temporária com ``JSON_PARSE`` porque a
    tabela tem coluna JSON."""

    def __init__(self, engine: RedshiftEngine) -> None:
        self.engine = engine

    def create(self, session: RedshiftEngine, table: sa.Table) -> None:
        session.create_table(table)

    def append(self, session: RedshiftEngine, table: sa.Table, rows: pa.Table) -> None:
        session.append(table, rows)

    def update(self, table: sa.Table, limit: int) -> None:
        self.engine.query(sa.update(table).where(table.c.id_lancamento <= limit)
                          .values(valor=table.c.valor + 1))

    def totals(self, table: sa.Table) -> tuple[int, int, float]:
        qualified = self.engine.qualified(self.engine.prefix + table.name)
        found = self.engine.query(
            f'SELECT count(*) AS n, count(DISTINCT "id_lancamento") AS d, '
            f'coalesce(sum("valor"), 0.0) AS s FROM {qualified}')
        row = found.to_pylist()[0]
        return row["n"], row["d"], float(row["s"])

    def long_query(self, table: sa.Table) -> sa.sql.ClauseElement:
        return sa.select(table)

    def describe(self, error: Exception) -> str:
        return redshift.mask(describe_error(error))


def outcome_of(writer: Writer, action: Callable[[], None]) -> Outcome:
    """Se a ação entrou, a descrição do erro quando não, e o tempo dela; o desfecho é leitura."""
    started = time.perf_counter()
    try:
        action()
    except Exception as error:  # noqa: BLE001 - o desfecho da escrita é a leitura da sonda
        return False, writer.describe(error), time.perf_counter() - started
    return True, "entrou", time.perf_counter() - started


def consistency(writer: Writer, table: sa.Table, entered: list[pa.Table],
                added: float = 0.0) -> list[str]:
    """As linhas e a soma de ``valor`` da tabela contra as escritas que entraram, mais ``added``
    do ``UPDATE``; um id repetido é problema."""
    rows, distinct, total = writer.totals(table)
    expected_rows = sum(part.num_rows for part in entered)
    expected_total = added
    for part in entered:
        expected_total += pc.sum(part["valor"]).as_py() or 0.0
    problems = []
    if rows != expected_rows:
        problems.append(f"{rows} linhas, esperadas {expected_rows}")
    if distinct != rows:
        problems.append(f"{rows - distinct} ids repetidos")
    if abs(total - expected_total) > TOLERANCE:
        problems.append(f"soma de valor {total}, esperada {expected_total}")
    return problems


def two_appends(writer: Writer, table: sa.Table, first: pa.Table, second: pa.Table,
                shared_session: bool) -> tuple[list[str], list[str]]:
    """Dois escritores na tabela ao mesmo tempo, cada um numa sessão a mais ou os dois na
    principal; devolve as leituras e os problemas."""
    barrier = threading.Barrier(2)

    def write(rows: pa.Table) -> Outcome:
        def run() -> None:
            if shared_session:
                barrier.wait(timeout=60)
                writer.append(writer.engine, table, rows)
                return
            with writer.engine.new_session() as session:
                barrier.wait(timeout=60)
                writer.append(session, table, rows)
        return outcome_of(writer, run)

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(write, [first, second]))
    entered = []
    readings = []
    for index, (rows, (ok, text, seconds)) in enumerate(zip((first, second), outcomes)):
        readings.append(f"escritor {index}: {text} em {seconds:.3f} s")
        if ok:
            entered.append(rows)
    return readings, consistency(writer, table, entered)


def append_beside_update(writer: Writer, table: sa.Table, seeded: pa.Table, appended: pa.Table,
                         limit: int) -> tuple[list[str], list[str]]:
    """Um ``append`` numa sessão a mais ao lado de um ``UPDATE`` da sessão principal sobre as
    linhas já gravadas de id até ``limit``; devolve as leituras e os problemas."""
    barrier = threading.Barrier(2)

    def do_append() -> None:
        with writer.engine.new_session() as session:
            barrier.wait(timeout=60)
            writer.append(session, table, appended)

    def do_update() -> None:
        barrier.wait(timeout=60)
        writer.update(table, limit)

    with ThreadPoolExecutor(max_workers=2) as pool:
        append_future = pool.submit(outcome_of, writer, do_append)
        update_future = pool.submit(outcome_of, writer, do_update)
        append_ok, append_text, append_seconds = append_future.result()
        update_ok, update_text, update_seconds = update_future.result()
    readings = [f"append: {append_text} em {append_seconds:.3f} s",
                f"update: {update_text} em {update_seconds:.3f} s"]
    entered = [seeded]
    if append_ok:
        entered.append(appended)
    added = float(limit) if update_ok else 0.0
    return readings, consistency(writer, table, entered, added)


def create_beside_stream(writer: Writer, query: sa.sql.ClauseElement | str,
                         other_table: sa.Table, main_table: sa.Table,
                         rows: pa.Table) -> tuple[list[str], list[str]]:
    """O ``CREATE TABLE`` numa sessão a mais e na principal enquanto a principal roda um
    ``stream``, e a tabela criada na sessão a mais recebendo um ``append`` da principal depois."""
    engine = writer.engine
    opened = threading.Event()

    def open_stream() -> tuple[object, float]:
        opened.set()
        started = time.perf_counter()
        stream = engine.stream(query, batch_size=100_000)
        return stream, time.perf_counter() - started

    def create_in_other_session() -> None:
        with engine.new_session() as other:
            writer.create(other, other_table)

    def create_in_main_session() -> None:
        writer.create(engine, main_table)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(open_stream)
        opened.wait(timeout=60)
        # A espera dá ao stream o tempo de tomar o lock e começar a consulta.
        time.sleep(HEAD_START)
        other_ok, other_text, other_seconds = outcome_of(writer, create_in_other_session)
        main_ok, main_text, main_seconds = outcome_of(writer, create_in_main_session)
        stream, first_batch_seconds = future.result()
    drain_started = time.perf_counter()
    streamed = 0
    with stream:
        for batch in stream:
            streamed += batch.num_rows
    drained = time.perf_counter() - drain_started
    readings = [f"CREATE na sessão a mais durante o stream: {other_text} em {other_seconds:.3f} s",
                f"CREATE na sessão principal durante o stream: {main_text} em {main_seconds:.3f} s",
                f"o stream devolveu o primeiro lote em {first_batch_seconds:.3f} s e esgotou "
                f"{streamed} linhas em {drained:.3f} s depois dos CREATE"]
    problems = []
    if not other_ok:
        problems.append(f"CREATE na sessão a mais: {other_text}")
    if not main_ok:
        problems.append(f"CREATE na sessão principal: {main_text}")
    if problems:
        return readings, problems

    def append_from_main_session() -> None:
        writer.append(engine, other_table, rows)

    ok, text, seconds = outcome_of(writer, append_from_main_session)
    readings.append(f"append da principal na tabela criada pela sessão a mais: {text} em "
                    f"{seconds:.3f} s")
    if not ok:
        problems.append(f"append na tabela da sessão a mais: {text}")
        return readings, problems
    return readings, consistency(writer, other_table, [rows])


def print_readings(readings: list[str]) -> None:
    for reading in readings:
        print("   ", reading)


def run_probe(writer: Writer, kind: str) -> None:
    """As seções na ordem do cabeçalho; imprime as leituras e falha com qualquer problema."""
    rows = ROWS[kind]
    first = entry_rows(MONTHS[0], 1, rows, PROJECTED)
    second = entry_rows(MONTHS[0], rows + 1, rows, PROJECTED)
    all_problems = []

    for round_number in range(ROUNDS[kind]):
        table = PROJECTED.to_metadata(sa.MetaData(), name=f"cad_append_{round_number}")
        writer.create(writer.engine, table)
        readings, problems = two_appends(writer, table, first, second, shared_session=False)
        print_readings(readings)
        report(f"A{round_number} dois append em sessões a mais", problems)
        all_problems += problems

    table = PROJECTED.to_metadata(sa.MetaData(), name="cad_append_sessao")
    writer.create(writer.engine, table)
    readings, problems = two_appends(writer, table, first, second, shared_session=True)
    print_readings(readings)
    report("B dois append na sessão principal em threads", problems)
    all_problems += problems

    table = PROJECTED.to_metadata(sa.MetaData(), name="cad_append_update")
    writer.create(writer.engine, table)
    writer.append(writer.engine, table, first)
    readings, problems = append_beside_update(writer, table, first, second, rows // 2)
    print_readings(readings)
    report("C um append ao lado de um UPDATE da mesma tabela", problems)
    all_problems += problems

    other_table = PROJECTED.to_metadata(sa.MetaData(), name="cad_criada_fora")
    main_table = PROJECTED.to_metadata(sa.MetaData(), name="cad_criada_dentro")
    query = writer.long_query(table)
    readings, problems = create_beside_stream(writer, query, other_table, main_table, first)
    print_readings(readings)
    report("D CREATE TABLE durante um stream", problems)
    all_problems += problems

    assert not all_problems, all_problems


@pytest.mark.local
def test_duckdb_concurrent_writers(local_location: LocalLocation) -> None:
    """O motor DuckDB num banco em arquivo da pasta local."""
    storage = Storage.for_uri(local_location.child("delta"))
    config = DuckDBConfig(temp_directory=local_location.child("sandbox"))
    engine = DuckDBEngine(config, "exec-append", storage)
    try:
        run_probe(DuckDBWriter(engine), "duckdb")
    finally:
        engine.cleanup()


@pytest.mark.redshift
@pytest.mark.s3
def test_redshift_concurrent_writers(s3_location: S3Location, redshift_driver: None) -> None:
    """O motor Redshift num sandbox próprio, com o ``staging/`` sob a raiz S3 da suíte; no
    substituto local, a conexão de ``tests/emulator.py``."""
    storage = Storage.for_uri(s3_location.child(f"consistencia/append-{uuid.uuid4().hex[:8]}"))
    execution_id = f"poc-{uuid.uuid4().hex[:8]}"
    engine = RedshiftEngine(redshift_config(), execution_id, storage,
                            f"prd/staging/{execution_id}")
    try:
        run_probe(RedshiftWriter(engine), "redshift")
    finally:
        engine.cleanup()
