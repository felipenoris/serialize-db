"""Leitura e escrita em paralelo no delta-rs e no DuckDB.

Sob a raiz local (marcador ``local``): várias tabelas Delta lidas em paralelo pelo delta-rs e
ingeridas em paralelo no DuckDB por ``delta_scan``, um cursor por tabela; escritas Delta em paralelo
em tabelas distintas e em meses distintos da mesma tabela, e o conflito de dois escritores no mesmo
mês; cargas Arrow e exportações ``COPY ... TO`` pedidas por várias threads a uma conexão DuckDB só,
sob um lock. Os tempos são leituras do relatório.

O motor DuckDB da biblioteca, com a sessão única, o stream e o loader, é testado em
``tests/test_engine_duckdb.py``. O Redshift está em ``test_redshift.py``
(``test_parallel_copy_and_unload_on_two_connections``).
"""

from __future__ import annotations

import functools
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError

from conftest import LocalLocation, record
from poc_delta import MONTHS, ROWS, connect_duckdb, run_in_threads, sample_table

FOUR_MONTHS = ("2026-01", "2026-02", "2026-03", "2026-04")
FOUR_MONTH_ROWS = 400_000


def four_month_table(con: duckdb.DuckDBPyConnection) -> pa.Table:
    """``FOUR_MONTH_ROWS`` linhas com ``mes`` em quatro valores, o inteiro, um decimal e um texto,
    geradas pelo DuckDB."""
    sql = (
        "SELECT range AS id, '2026-0' || (1 + range % 4) AS mes, "
        "CAST(((range * 7) % 1000) / 100.0 AS DECIMAL(18, 2)) AS valor, "
        f"'x' || range AS descricao FROM range({FOUR_MONTH_ROWS})"
    )
    return con.execute(sql).to_arrow_table()


# --- Delta ---------------------------------------------------------------------------------------


@pytest.mark.local
def test_several_delta_tables_read_and_ingested_in_parallel(local_location: LocalLocation) -> None:
    """Quatro tabelas Delta lidas pelo delta-rs e ingeridas no DuckDB por ``delta_scan``, em
    sequência na conexão raiz e em quatro threads, um cursor por tabela."""
    sample = sample_table()
    uris = [local_location.child(f"leitura/tabela_{k}") for k in range(4)]
    for uri in uris:
        write_deltalake(uri, sample, mode="overwrite", partition_by=["mes"])

    def read_all(uri: str) -> int:
        return DeltaTable(uri).to_pyarrow_table().num_rows

    started = time.perf_counter()
    sequential = [read_all(uri) for uri in uris]
    one_by_one = time.perf_counter() - started
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=4) as pool:
        parallel = list(pool.map(read_all, uris))
    in_threads = time.perf_counter() - started
    assert sequential == parallel == [ROWS] * 4
    record("parallel.timing.delta_read_4_tables",
           f"sequencial {one_by_one:.3f} s, quatro threads {in_threads:.3f} s")

    # A ingestão: cada tabela materializa um mês por delta_scan, em série na conexão raiz ou num
    # cursor por thread; a tabela confirmada pelo cursor é vista pela conexão raiz.
    con = connect_duckdb(["delta"])
    con.execute("SET threads = 2")

    def ingest(connection: duckdb.DuckDBPyConnection, prefix: str, k: int) -> None:
        connection.execute(f"CREATE TABLE {prefix}_{k} AS SELECT * FROM delta_scan('{uris[k]}') "
                           f"WHERE mes = '{MONTHS[0]}'")

    def ingest_in_own_cursor(k: int) -> None:
        cursor = con.cursor()
        try:
            ingest(cursor, "par", k)
        finally:
            cursor.close()

    started = time.perf_counter()
    for k in range(4):
        ingest(con, "seq", k)
    one_by_one = time.perf_counter() - started
    in_threads = run_in_threads([functools.partial(ingest_in_own_cursor, k) for k in range(4)])
    counts = []
    for k in range(4):
        counts.append(con.execute(f"SELECT count(*) FROM par_{k}").fetchone()[0])
    assert counts == [ROWS // 2] * 4
    record("parallel.timing.duckdb_ingest_4_tables_by_delta_scan",
           f"sequencial {one_by_one:.3f} s, quatro cursores {in_threads:.3f} s")
    con.close()


@pytest.mark.local
def test_delta_writes_in_parallel_by_table_and_by_month_and_the_conflict(
        local_location: LocalLocation) -> None:
    """Escritas em paralelo entram em tabelas distintas e em meses distintos da mesma tabela; no
    mesmo mês, um dos dois escritores falha com ``CommitFailedError``."""
    con = duckdb.connect(config={"threads": 2})
    data = four_month_table(con)
    con.close()

    # 1. Quatro tabelas em quatro threads: quatro commits independentes, cada log na sua pasta.
    uris = [local_location.child(f"escrita/tabela_{k}") for k in range(4)]

    def write_table(uri: str) -> None:
        write_deltalake(uri, data, mode="overwrite", partition_by=["mes"])

    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(write_table, uris))
    assert [DeltaTable(uri).version() for uri in uris] == [0] * 4

    # 2. A mesma tabela, quatro meses em quatro threads: cada overwrite com predicado troca os
    # arquivos do seu mês, o delta-rs refaz a tentativa de commit quando a versão avançou, e os
    # quatro entram.
    uri = uris[0]

    def replace_month(month: str) -> None:
        month_rows = data.filter(pc.equal(data.column("mes"), month))
        write_deltalake(uri, month_rows, mode="overwrite", predicate=f"mes = '{month}'")

    elapsed = run_in_threads([functools.partial(replace_month, month) for month in FOUR_MONTHS])
    table = DeltaTable(uri)
    assert table.version() == 4
    assert table.to_pyarrow_dataset().count_rows() == data.num_rows
    record("parallel.timing.delta_overwrite_4_months_same_table",
           f"{elapsed:.3f} s em quatro threads")

    # 3. Dois escritores carregados na mesma versão, o mesmo mês, duas threads: um commit entra, e o
    # outro é o conflito que a biblioteca converte em ExecutionConflict.
    first = DeltaTable(uri)
    second = DeltaTable(uri)
    month_rows = data.filter(pc.equal(data.column("mes"), FOUR_MONTHS[0]))
    predicate = f"mes = '{FOUR_MONTHS[0]}'"
    outcomes: list[str] = []

    def overwrite(writer: DeltaTable) -> None:
        try:
            write_deltalake(writer, month_rows, mode="overwrite", predicate=predicate)
            outcomes.append("commit")
        except CommitFailedError:
            outcomes.append("CommitFailedError")

    run_in_threads([functools.partial(overwrite, first), functools.partial(overwrite, second)])
    assert sorted(outcomes) == ["CommitFailedError", "commit"]
    assert DeltaTable(uri).version() == 5


# --- DuckDB --------------------------------------------------------------------------------------


@pytest.mark.local
def test_duckdb_loads_and_exports_from_threads_on_one_connection(
        local_location: LocalLocation) -> None:
    """Quatro cargas Arrow em tabelas distintas e quatro ``COPY ... TO`` pedidos por quatro
    threads a uma conexão só, em série sob um lock: o ``threadsafety`` 1 do ``duckdb`` não deixa
    duas threads usarem a conexão ao mesmo tempo."""
    con = duckdb.connect(str(local_location.path / "sandbox.duckdb"), config={"threads": 2})
    lock = threading.Lock()
    data = four_month_table(con)
    by_month = {}
    for month in FOUR_MONTHS:
        by_month[month] = data.filter(pc.equal(data.column("mes"), month))

    def load(k: int) -> None:
        # O registro da tabela Arrow e o CREATE correm sob o mesmo lock: outra thread nunca vê o
        # nome registrado.
        with lock:
            con.register("data_in", by_month[FOUR_MONTHS[k]])
            con.execute(f"CREATE TABLE carga_{k} AS SELECT * FROM data_in")
            con.unregister("data_in")

    elapsed = run_in_threads([functools.partial(load, k) for k in range(4)])
    tables = con.execute("SELECT table_name FROM duckdb_tables() ORDER BY table_name").fetchall()
    assert [row[0] for row in tables] == [f"carga_{k}" for k in range(4)]
    loaded_rows = 0
    for k in range(4):
        loaded_rows += con.execute(f"SELECT count(*) FROM carga_{k}").fetchone()[0]
    assert loaded_rows == data.num_rows
    record("parallel.timing.duckdb_load_4_tables",
           f"{elapsed:.3f} s em quatro threads, em série na conexão")

    folder = local_location.path / "exportacao"
    folder.mkdir()

    def export(k: int) -> None:
        target = folder / f"carga_{k}.parquet"
        with lock:
            con.execute(f"COPY carga_{k} TO '{target}' (FORMAT parquet)")

    elapsed = run_in_threads([functools.partial(export, k) for k in range(4)])
    for k, month in enumerate(FOUR_MONTHS):
        exported = pq.read_metadata(folder / f"carga_{k}.parquet").num_rows
        assert exported == by_month[month].num_rows
    record("parallel.timing.duckdb_copy_to_4_files",
           f"{elapsed:.3f} s em quatro threads, em série na conexão")
    con.close()
