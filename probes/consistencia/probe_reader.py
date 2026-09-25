"""O leitor Delta sob concorrência: 100.000 linhas de ``cad_tudo`` em quatro partições; duas
threads de ``query``, uma de ``stream`` e uma de texto com parâmetro leem sem parar enquanto
``materialize`` troca a view pela tabela inteira, pela parcial de duas partições e de novo, três
vezes; toda leitura é um dos estados consistentes e nenhuma thread vê erro; e um valor fora da
regra de partição recusado antes da troca, com a tabela intacta.

.. code-block:: shell

    SERIALIZE_DB_TEST_LOCAL_ROOT=$HOME/serialize-db-local \\
        .venv/bin/python probes/consistencia/probe_reader.py
"""
from __future__ import annotations

import threading
import time
from collections import Counter
from pathlib import Path

import duckdb
import pyarrow.compute as pc
import sqlalchemy as sa

from consistency_lib import (TUDO, Base, compare, edge_rows, finish, print_notes, probe_folder,
                             report, to_contract)
from serialize_db import delta
from serialize_db.engine.duckdb import DuckDBConfig
from serialize_db.errors import ContractError, SandboxError
from serialize_db.execution import Database
from serialize_db.reader import DeltaReader

MONTHS = ["2026-06-30", "2026-07-31", "2026-08-31", "2026-09-30"]
ROWS = 25_000
TOTAL = len(MONTHS) * ROWS
PARTIAL = MONTHS[2:]
COUNT_AND_SUM = sa.select(sa.func.count(), sa.func.sum(TUDO.c.id)).select_from(TUDO)
NOTES: set[str] = set()
# Os erros que uma leitura durante a troca poderia levantar: os do DuckDB e os do pacote.
READ_ERRORS = (duckdb.Error, SandboxError, ContractError)


def reader_config(folder: Path, name: str) -> DuckDBConfig:
    return DuckDBConfig(temp_directory=str(folder / f"leitor_{name}"))


def seed(db: Database) -> dict[str, object]:
    """As quatro partições publicadas, o snapshot ``s1`` e o canal ``default`` nele."""
    uri = db.uri(TUDO)
    delta.create_table(uri, TUDO, db.storage)
    expected = {}
    for index, month in enumerate(MONTHS):
        expected[month] = edge_rows(month, 1 + index * ROWS, ROWS)
        delta.publish_partition(uri, TUDO, month, expected[month], {}, db.storage)
    version = delta.open_table(uri, db.storage).version()
    delta.snapshot(db.storage, "prd", "s1", {TUDO.name: version})
    delta.set_channel(db.storage, "prd", "default", "s1")
    return expected


class Readings:
    """O que as threads de leitura viram: as contagens, as somas por contagem e os erros."""

    def __init__(self) -> None:
        self.counts: Counter = Counter()
        self.sums: set[tuple[int, int]] = set()
        self.errors: list[str] = []
        self.stop = threading.Event()


def query_loop(reader: DeltaReader, readings: Readings, index: int) -> None:
    """Conta e soma os ids por ``query`` até o sinal de parada."""
    while not readings.stop.is_set():
        try:
            row = list(reader.query(COUNT_AND_SUM).to_pylist()[0].values())
            readings.counts[row[0]] += 1
            readings.sums.add((row[0], int(row[1])))
        except READ_ERRORS as error:
            readings.errors.append(f"thread de query {index}: {type(error).__name__}: "
                                   f"{str(error)[:100]}")
            time.sleep(0.01)


def stream_loop(reader: DeltaReader, readings: Readings, index: int) -> None:
    """Conta as linhas por ``stream`` até o sinal de parada."""
    while not readings.stop.is_set():
        try:
            rows = 0
            with reader.stream(sa.select(TUDO), batch_size=7000) as stream:
                for batch in stream:
                    rows += batch.num_rows
            readings.counts[rows] += 1
        except READ_ERRORS as error:
            readings.errors.append(f"thread de stream {index}: {type(error).__name__}: "
                                   f"{str(error)[:100]}")
            time.sleep(0.01)


def text_loop(reader: DeltaReader, readings: Readings, index: int) -> None:
    """Conta as duas últimas partições por texto com parâmetro até o sinal de parada."""
    text = 'SELECT count(*) AS n FROM "cad_tudo" WHERE "data_str" >= :m'
    while not readings.stop.is_set():
        try:
            found = reader.query(text, {"m": PARTIAL[0]}).column(0)[0].as_py()
            readings.counts[("texto", found)] += 1
        except READ_ERRORS as error:
            readings.errors.append(f"thread de texto {index}: {type(error).__name__}: "
                                   f"{str(error)[:100]}")
            time.sleep(0.01)


def swap_while_reading(reader: DeltaReader, readings: Readings) -> list[tuple[str, float]]:
    """Três rodadas de ``materialize`` inteiro e parcial com as quatro threads lendo; devolve o
    tempo de cada troca."""
    threads = [threading.Thread(target=query_loop, args=(reader, readings, 0)),
               threading.Thread(target=query_loop, args=(reader, readings, 1)),
               threading.Thread(target=stream_loop, args=(reader, readings, 2)),
               threading.Thread(target=text_loop, args=(reader, readings, 3))]
    for thread in threads:
        thread.start()
    time.sleep(1.0)
    swaps = []
    try:
        for _ in range(3):
            started = time.perf_counter()
            reader.materialize(TUDO)
            swaps.append(("inteira", time.perf_counter() - started))
            time.sleep(0.7)
            started = time.perf_counter()
            reader.materialize(TUDO, partitions=PARTIAL)
            swaps.append(("parcial", time.perf_counter() - started))
            time.sleep(0.7)
    finally:
        readings.stop.set()
        for thread in threads:
            thread.join()
    return swaps


def check_materialize_under_reads(db: Database, folder: Path,
                                  expected: dict[str, object]) -> None:
    """Seção M: as leituras durante as trocas são a tabela inteira ou a parcial, com a soma dos
    ids certa e sem erro, e a materialização parcial é igual às sementes."""
    reader = db.open_delta(config=reader_config(folder, "principal"))
    problems = []
    if reader.snapshot != "s1" or reader.versions != {TUDO.name: 4}:
        problems.append(f"leitor {reader.snapshot} {reader.versions}")
    readings = Readings()
    swaps = swap_while_reading(reader, readings)
    print(f"   contagens vistas: {dict(readings.counts)}")
    print(f"   erros: {len(readings.errors)}")
    for error in readings.errors[:5]:
        print("   ", error)
    for name, seconds in swaps:
        print(f"   materialize {name}: {seconds:.2f} s")
    allowed = {TOTAL, len(PARTIAL) * ROWS}
    bad_counts = [c for c in readings.counts if not isinstance(c, tuple) and c not in allowed]
    if bad_counts:
        problems.append(f"contagens inconsistentes {bad_counts}")
    bad_text = [c for c in readings.counts if isinstance(c, tuple) and c[1] != 2 * ROWS]
    if bad_text:
        problems.append(f"contagens por texto {bad_text}")
    expected_sums = {TOTAL: sum(range(1, TOTAL + 1)),
                     2 * ROWS: sum(range(1 + 2 * ROWS, TOTAL + 1))}
    bad_sums = [s for s in readings.sums if expected_sums.get(s[0]) != s[1]]
    if bad_sums:
        problems.append(f"soma dos ids errada em {bad_sums}")
    if readings.errors:
        problems.append(f"{len(readings.errors)} erros nas threads: {readings.errors[0]}")
    if reader.materialized != {TUDO.name: PARTIAL}:
        problems.append(f"materialized {reader.materialized}")
    found = reader.query(sa.select(TUDO))
    for month in PARTIAL:
        part = found.filter(pc.field("data_str") == month)
        problems += compare(expected[month], to_contract(part, TUDO, NOTES), key="id",
                            label=f"materializada {month}")
    reader.close()
    report("M o leitor Delta com queries e streams durante o materialize", problems)


def check_refused_materialize(db: Database, folder: Path) -> None:
    """Seção F: um valor fora da regra de partição é recusado antes da troca, e a tabela
    materializada antes fica intacta."""
    reader = db.open_delta(config=reader_config(folder, "recusa"))
    problems = []
    reader.materialize(TUDO, partitions=MONTHS[:1])
    before = reader.query(COUNT_AND_SUM).to_pylist()[0]
    try:
        reader.materialize(TUDO, partitions=["2026 08 31"])
        problems.append("materialize aceitou um valor fora da regra")
    except ContractError as error:
        print(f"   recusado: {type(error).__name__}: {str(error)[:80]}")
    after = reader.query(COUNT_AND_SUM).to_pylist()[0]
    if before != after or list(before.values())[0] != ROWS:
        problems.append(f"a recusa mudou a tabela: {before} -> {after}")
    reader.close()
    report("F um materialize recusado mantém a tabela", problems)


def main() -> None:
    folder = probe_folder("leitor")
    db = Database(str(folder / "delta"), "prd", Base.metadata)
    expected = seed(db)
    check_materialize_under_reads(db, folder, expected)
    check_refused_materialize(db, folder)
    print_notes(NOTES)
    finish(folder)


if __name__ == "__main__":
    main()
