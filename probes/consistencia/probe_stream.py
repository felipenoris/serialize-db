"""O ``stream`` e o ``loader`` do motor DuckDB sob concorrência: 1.500.000 linhas de nove tipos
por um stream lento com o transbordo forçado, oito streams ao mesmo tempo na mesma conexão,
quatro pipelines de ``stream`` para ``loader`` em threads, um ``loader`` alimentado por quatro
threads, um stream depois de outro fechado no meio, um stream ao lado de um que falha e a pasta
de transbordo vazia no fim: as contagens, as somas, a ordem e o esquema dos lotes.

.. code-block:: shell

    SERIALIZE_DB_TEST_LOCAL_ROOT=$HOME/serialize-db-local \\
        .venv/bin/python probes/consistencia/probe_stream.py
"""
from __future__ import annotations

import random
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sa

from consistency_lib import finish, probe_folder, report
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine, DuckDBStream
from serialize_db.storage import Storage

ROWS = 1_500_000
EXPECTED_SUM = ROWS * (ROWS - 1) // 2
ORDERED = "SELECT * FROM numeros ORDER BY id"
# As linhas de nove tipos: um id, um texto, um double, um decimal, uma data, um instante, um
# booleano, um JSON e um texto nulo a cada três linhas.
CREATE_NUMBERS = f"""
    CREATE TABLE numeros AS
    SELECT range AS id, md5(range::VARCHAR) AS s, range / 7.0 AS d,
           (range % 1000)::DECIMAL(18, 2) AS p,
           DATE '2026-01-01' + (range % 365)::INTEGER AS dt,
           TIMESTAMP '2026-01-01 00:00:00' + INTERVAL (range % 86400) SECOND AS ts,
           range % 2 = 0 AS b, ('{{"k": ' || range || '}}')::JSON AS j,
           CASE WHEN range % 3 = 0 THEN NULL ELSE range::VARCHAR END AS n
    FROM range({ROWS})
"""
TOTALS = "count(*), sum(id), count(DISTINCT id), sum(d), sum(p), count(n)"


def consume(engine: DuckDBEngine, label: str, text: str, batch_size: int, budget: int,
            sleep: float, ordered: bool) -> list[str]:
    """Lê a consulta inteira por um ``DuckDBStream`` com o orçamento de memória dado e confere
    as linhas, a soma e a unicidade dos ids, a ordem quando pedida e o esquema de cada lote."""
    problems = []
    ids = []
    spilled = 0
    with DuckDBStream(engine, text, [], batch_size=batch_size, budget=budget) as stream:
        first_schema = stream.schema
        for batch in stream:
            if sleep:
                time.sleep(sleep)
            if not batch.schema.equals(first_schema):
                problems.append(f"{label}: o esquema do lote difere do stream.schema: "
                                f"{batch.schema} contra {first_schema}")
            ids.append(batch.column("id"))
        # O contador de lotes transbordados é do transbordo do stream: uma leitura.
        spilled = stream._spool.spilled
    found = pa.chunked_array(ids) if ids else pa.chunked_array([], pa.int64())
    count = len(found)
    if count != ROWS:
        problems.append(f"{label}: {count} linhas, esperadas {ROWS} (transbordados {spilled})")
    total = pc.sum(found).as_py() or 0
    if total != EXPECTED_SUM:
        problems.append(f"{label}: soma {total}, esperada {EXPECTED_SUM}")
    distinct = len(pc.unique(found))
    if distinct != count:
        problems.append(f"{label}: {count - distinct} ids repetidos")
    if ordered:
        values = found.to_pylist()
        if values != sorted(values):
            problems.append(f"{label}: fora de ordem (transbordados {spilled})")
    print(f"   {label}: linhas {count}, transbordados {spilled}")
    return problems


def check_slow_spilled_stream(engine: DuckDBEngine) -> None:
    """Seção A: um stream ordenado de lotes de 50.000 linhas, 3.000.000 bytes de orçamento e um
    cliente que dorme 5 ms por lote: o transbordo forçado."""
    problems = consume(engine, "A lento com transbordo", ORDERED, 50_000, 3_000_000, 0.005, True)
    report("A um stream lento com transbordo, ordenado", problems)


def check_concurrent_streams(engine: DuckDBEngine) -> None:
    """Seção B: oito streams ao mesmo tempo na mesma conexão, cada um com o seu tamanho de lote,
    orçamento e ritmo, sorteados por semente."""
    def one(index: int) -> list[str]:
        chooser = random.Random(index)
        batch_size = chooser.choice([1000, 33_333, 100_000, 250_000])
        budget = chooser.choice([1, 100_000, 5_000_000, 64 * 2**20])
        sleep = chooser.choice([0, 0.001])
        return consume(engine, f"B{index}", ORDERED, batch_size, budget, sleep, True)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(one, range(8)))
    report("B oito streams ao mesmo tempo", [p for result in results for p in result])


def output_table(index: int) -> sa.Table:
    """A tabela de saída ``saida_<index>`` com as nove colunas de ``numeros``."""
    return sa.Table(f"saida_{index}", sa.MetaData(),
                    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=False),
                    sa.Column("s", sa.String(32)), sa.Column("d", sa.Double),
                    sa.Column("p", sa.Numeric(18, 2)), sa.Column("dt", sa.Date),
                    sa.Column("ts", sa.DateTime), sa.Column("b", sa.Boolean),
                    sa.Column("j", sa.JSON), sa.Column("n", sa.String(20)))


def totals_differences(engine: DuckDBEngine, label: str, name: str) -> list[str]:
    """As diferenças entre os totais de ``numeros`` e os da tabela ``name``: a soma do double
    com tolerância relativa de 1e-6, porque a ordem da soma muda os últimos dígitos; e o
    conteúdo pelo ``EXCEPT`` nos dois sentidos."""
    problems = []
    found = engine.query(f'SELECT {TOTALS} FROM "{name}"').to_pylist()[0]
    expected = engine.query(f"SELECT {TOTALS} FROM numeros").to_pylist()[0]
    for column, a, b in zip(found.keys(), found.values(), expected.values()):
        if column == "sum(d)":
            equal = abs(a - b) <= 1e-6 * abs(b)
        else:
            equal = a == b
        if not equal:
            problems.append(f"{label}: {column} {a}, esperado {b}")
    differences = engine.query(
        f'SELECT count(*) FROM (SELECT * FROM numeros EXCEPT SELECT * FROM "{name}") '
        f'UNION ALL SELECT count(*) FROM (SELECT * FROM "{name}" EXCEPT SELECT * FROM numeros)'
    ).column(0).to_pylist()
    if differences != [0, 0]:
        problems.append(f"{label}: diferenças pelo EXCEPT {differences}")
    return problems


def check_pipelines(engine: DuckDBEngine) -> None:
    """Seção C: quatro pipelines de ``stream`` para ``loader`` em threads, cada um na sua tabela,
    com o conteúdo conferido pelos totais e pelo ``EXCEPT``."""
    def pipeline(index: int) -> list[str]:
        table = output_table(index)
        stream = DuckDBStream(engine, "SELECT * FROM numeros", [], batch_size=100_000,
                              budget=2_000_000)
        with stream, engine.loader(table) as loader:
            for batch in stream:
                loader.write(batch)
        print(f"   C{index}: linhas {loader.rows}")
        return totals_differences(engine, f"C{index}", table.name)

    with ThreadPoolExecutor(max_workers=4) as pool:
        results = list(pool.map(pipeline, range(4)))
    report("C quatro pipelines stream para loader em threads",
           [p for result in results for p in result])


def check_shared_loader(engine: DuckDBEngine) -> None:
    """Seção D: um ``loader`` escrito por quatro threads, cada uma com o seu quarto das linhas."""
    table = output_table(99)
    with engine.loader(table, queue_depth=2) as loader:
        def writer(remainder: int) -> None:
            stream = DuckDBStream(engine, f"SELECT * FROM numeros WHERE id % 4 = {remainder}", [],
                                  batch_size=50_000, budget=1_000_000)
            with stream:
                for batch in stream:
                    loader.write(batch)

        with ThreadPoolExecutor(max_workers=4) as pool:
            list(pool.map(writer, range(4)))
    found = engine.query('SELECT count(*), sum(id), count(DISTINCT id) FROM "saida_99"')
    values = list(found.to_pylist()[0].values())
    problems = [] if values == [ROWS, EXPECTED_SUM, ROWS] else [f"D: {values}"]
    report("D um loader com quatro threads escrevendo", problems)


def check_stream_after_close(engine: DuckDBEngine) -> None:
    """Seção E: um stream fechado depois do primeiro lote não afeta a consulta e o stream
    seguintes."""
    stream = DuckDBStream(engine, "SELECT * FROM numeros ORDER BY hash(id)", [], batch_size=1000,
                          budget=1)
    with stream:
        stream.read_next_batch()
    count = engine.query("SELECT count(*) FROM numeros").column(0)[0].as_py()
    problems = [] if count == ROWS else [f"contagem depois do close {count}"]
    problems += consume(engine, "E depois do close", ORDERED, 100_000, 64 * 2**20, 0, True)
    report("E um stream depois de outro fechado no meio", problems)


def check_stream_beside_failure(engine: DuckDBEngine) -> None:
    """Seção F: um stream que falha no meio da consulta, numa thread, ao lado de outro que lê
    tudo; o tipo do erro é leitura."""
    failing_text = ("SELECT CAST(CASE WHEN id = 1400000 THEN 'x' ELSE id::VARCHAR END AS INTEGER) "
                    "AS id FROM numeros")

    def failing() -> str:
        try:
            stream = DuckDBStream(engine, failing_text, [], batch_size=100_000, budget=1)
            with stream:
                for _ in stream:
                    pass
        except (duckdb.Error, OSError, pa.ArrowException) as error:
            return type(error).__name__
        return "sem erro"

    with ThreadPoolExecutor(max_workers=2) as pool:
        failing_future = pool.submit(failing)
        healthy_future = pool.submit(consume, engine, "F ao lado da falha", ORDERED, 100_000,
                                     64 * 2**20, 0, True)
        print("   o stream que falha:", failing_future.result())
        report("F um stream ao lado de outro que falha", healthy_future.result())


def check_spool_folder(engine: DuckDBEngine) -> None:
    """Seção G: a pasta de transbordo do motor vazia depois de todo stream e loader fechado."""
    spool_folder = Path(engine.spool_path("sonda")).parent
    left = sorted(path.name for path in spool_folder.iterdir())
    report("G a pasta de transbordo vazia", [] if not left else [f"restaram: {left}"])


def main() -> None:
    folder = probe_folder("stream")
    storage = Storage.for_uri(str(folder / "delta"))
    config = DuckDBConfig(temp_directory=str(folder / "sandbox"))
    engine = DuckDBEngine(config, "exec-stream", storage)
    try:
        engine.query(CREATE_NUMBERS)
        check_slow_spilled_stream(engine)
        check_concurrent_streams(engine)
        check_pipelines(engine)
        check_shared_loader(engine)
        check_stream_after_close(engine)
        check_stream_beside_failure(engine)
        check_spool_folder(engine)
    finally:
        engine.cleanup()
    finish(folder)


if __name__ == "__main__":
    main()
