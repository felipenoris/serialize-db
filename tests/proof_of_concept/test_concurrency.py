"""Threads do Python sobre os pacotes nativos: o GIL liberado, um cursor DuckDB por thread e o
paralelismo real.

Sem gravar arquivo: o laço Python que mantém a taxa noutra thread enquanto o DuckDB agrega e
converte para Arrow e o PyArrow grava e lê Parquet em memória (o GIL liberado); o ``threadsafety``
dos drivers, o ``cursor()`` por thread, a conexão compartilhada que entrega a uma thread o
resultado da outra, os dois ``connect()`` em memória que são bancos distintos; o intervalo de troca
do GIL pago por cada retomada ao lado de uma thread Python ocupada (``os.stat`` e o import
preguiçoso); o pool de threads do DuckDB, da instância e mudado em execução, ao lado da thread de
cada sessão, que executa a consulta dela. Sob a raiz local (marcador ``local``): o GIL liberado
pelo delta-rs e pelo ``delta_scan``, duas escritas Delta e dois ``CREATE TABLE AS`` em duas
threads, o arquivo do DuckDB compartilhado no processo pela mesma configuração e recusado com outra,
os leitores Delta presos à versão carregada enquanto um ``append`` entra.

O GIL liberado é asserção: com ele retido o laço Python para, e a taxa cai perto de zero. O ganho de
tempo de duas threads é leitura do relatório, porque depende dos núcleos livres da máquina.
"""

from __future__ import annotations

import functools
import importlib
import os
import sys
import threading
import time
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# Importado aqui, e não dentro de ``pq.read_table``, que o importa na primeira chamada: um import
# ao lado de uma thread Python ocupada leva segundos
# (``test_gil_reacquisition_waits_the_switch_interval``).
import pyarrow.dataset  # noqa: F401
import pytest
import redshift_connector
from deltalake import DeltaTable, write_deltalake

from conftest import LocalLocation, record
from poc_delta import APPENDED_ROWS, ROWS, connect_duckdb, run_in_threads, sample_table

LARGE_ROWS = 2_000_000

# Fração mínima da taxa de referência que o laço Python mantém durante uma chamada nativa. Com o GIL
# retido a taxa cai perto de zero; a disputa por núcleos a reduz, não a esse ponto.
GIL_RELEASED_SHARE = 0.25


def large_table(con: duckdb.DuckDBPyConnection) -> pa.Table:
    """Tabela Arrow de ``LARGE_ROWS`` linhas gerada pelo DuckDB: inteiro, resto, decimal e texto."""
    sql = (
        "SELECT range AS id, range % 12 AS m, ((range * 7) % 1000) / 100.0 AS v, "
        f"'x' || range AS s FROM range({LARGE_ROWS})"
    )
    return con.execute(sql).to_arrow_table()


def python_rate_during(action: Callable[[], object]) -> tuple[float, float]:
    """Roda ``action`` nesta thread enquanto outra conta iterações Python.

    Devolve a duração da ação e as iterações por segundo do laço. A thread contadora para em
    ``finally``: uma exceção na ação não a deixa girando.
    """
    stop = threading.Event()
    rate: dict[str, float] = {}

    def count() -> None:
        iterations = 0
        started = time.perf_counter()
        while not stop.is_set():
            iterations += 1
        rate["value"] = iterations / (time.perf_counter() - started)

    counter = threading.Thread(target=count)
    counter.start()
    started = time.perf_counter()
    try:
        action()
    finally:
        elapsed = time.perf_counter() - started
        stop.set()
        counter.join()

    return elapsed, rate["value"]


def reference_rate() -> float:
    """A taxa do laço com a thread principal em ``sleep``, que libera o GIL."""
    _, rate = python_rate_during(lambda: time.sleep(0.5))
    return rate


def assert_gil_released(reference: float, label: str, action: Callable[[], object]) -> None:
    """Mede ``action`` contra a referência, registra e reprova a taxa abaixo de
    ``GIL_RELEASED_SHARE``."""
    elapsed, rate = python_rate_during(action)
    share = rate / reference
    record(f"concurrency.gil.{label}",
           f"{elapsed:.3f} s; laço Python a {share:.0%} da referência ({rate / 1e6:.1f} M it/s)")
    assert share >= GIL_RELEASED_SHARE, (
        f"{label}: o laço Python caiu a {share:.0%} da referência, o GIL ficou retido")


def test_duckdb_and_pyarrow_release_the_gil() -> None:
    """O laço Python mantém a taxa enquanto o DuckDB agrega e converte para Arrow e o PyArrow grava
    e lê Parquet em memória."""
    con = duckdb.connect(config={"threads": 2})
    reference = reference_rate()
    record("concurrency.gil.reference",
           f"{reference / 1e6:.1f} M it/s com a thread principal em sleep")

    def aggregate() -> None:
        con.execute("SELECT sum(range * range) FROM range(60_000_000)").fetchall()

    assert_gil_released(reference, "duckdb_aggregate", aggregate)

    # A conversão guarda a tabela para as medições seguintes.
    produced: dict[str, pa.Table] = {}

    def convert_to_arrow() -> None:
        produced["table"] = large_table(con)

    assert_gil_released(reference, "duckdb_to_arrow_table", convert_to_arrow)

    # Parquet em memória: o mesmo escritor e leitor dos arquivos, sem tocar o disco.
    sink = pa.BufferOutputStream()
    assert_gil_released(reference, "pyarrow_write_parquet",
                        lambda: pq.write_table(produced["table"], sink))
    buffer = sink.getvalue()
    assert_gil_released(reference, "pyarrow_read_parquet",
                        lambda: pq.read_table(pa.BufferReader(buffer)))
    con.close()


def test_gil_reacquisition_waits_the_switch_interval() -> None:
    """Uma chamada nativa curta que solta e retoma o GIL espera o intervalo de troca enquanto outra
    thread roda Python puro.

    Duzentos ``os.stat`` levam décimos de milissegundo sozinhos e centenas de milissegundos ao lado
    do laço, um intervalo por retomada; ``sys.setswitchinterval`` menor encurta a espera. Um import
    preguiçoso paga o mesmo por cada ``stat`` do ``importlib``: ``import pyarrow.dataset``, que
    ``pq.read_table`` faz na primeira chamada, levou 15 s ao lado do laço contra 0,19 s sozinho
    (2026-09-20, macOS). A biblioteca importa tudo na abertura.
    """

    def stat_200_times() -> None:
        for _ in range(200):
            os.stat(".")

    started = time.perf_counter()
    stat_200_times()
    alone = time.perf_counter() - started
    beside, _ = python_rate_during(stat_200_times)
    default = sys.getswitchinterval()
    sys.setswitchinterval(default / 10)
    try:
        shorter, _ = python_rate_during(stat_200_times)
    finally:
        sys.setswitchinterval(default)
    record(
        "concurrency.gil.os_stat_200",
        f"sozinho {alone * 1e3:.1f} ms; ao lado do laço {beside:.3f} s; "
        f"com switchinterval {default / 10:.4f} s: {shorter:.3f} s",
    )
    assert beside >= 20 * alone
    assert shorter < beside / 2

    # Um módulo ainda não importado, se houver: o import ao lado do laço, como leitura.
    candidates = ("xml.dom.minidom", "email.mime.text", "wsgiref.simple_server")
    not_imported = [candidate for candidate in candidates if candidate not in sys.modules]
    if not_imported:
        name = not_imported[0]
        elapsed, _ = python_rate_during(functools.partial(importlib.import_module, name))
        record("concurrency.gil.lazy_import_beside_busy_thread", f"import {name}: {elapsed:.3f} s")


def test_drivers_share_the_module_not_the_connection() -> None:
    """``threadsafety`` 1 nos dois drivers: as threads compartilham o módulo, não a conexão.

    Um ``cursor()`` por thread grava no mesmo banco em memória; a conexão compartilhada entrega a
    uma thread o resultado da outra; dois ``connect()`` em memória são bancos distintos.
    """
    assert duckdb.threadsafety == 1
    assert redshift_connector.threadsafety == 1
    record("concurrency.threadsafety",
           f"duckdb {duckdb.threadsafety}, redshift_connector {redshift_connector.threadsafety}")

    # Um cursor por thread: cada thread cria a sua tabela, e a conexão raiz vê as duas.
    con = duckdb.connect(config={"threads": 2})

    def create(name: str) -> None:
        cursor = con.cursor()
        cursor.execute(f"CREATE TABLE {name} AS SELECT range AS x FROM range(1000)")
        cursor.close()

    run_in_threads([functools.partial(create, "t_a"), functools.partial(create, "t_b")])
    counts = con.execute("SELECT (SELECT count(*) FROM t_a), (SELECT count(*) FROM t_b)").fetchone()
    assert counts == (1000, 1000)

    # A conexão compartilhada: A executa, B executa na mesma conexão, e o fetchall de A traz as
    # linhas de B.
    a_executed = threading.Event()
    b_executed = threading.Event()
    fetched: dict[str, list[tuple]] = {}

    # Toda espera tem prazo: se uma thread falha antes do set, a outra falha em vez de travar a
    # suíte.
    def first() -> None:
        con.execute("SELECT 'a' AS who, count(*) AS n FROM range(5_000_000)")
        a_executed.set()
        assert b_executed.wait(timeout=10)
        fetched["a"] = con.fetchall()

    def second() -> None:
        assert a_executed.wait(timeout=10)
        con.execute("SELECT 'b' AS who, 0 AS n")
        b_executed.set()

    run_in_threads([first, second])
    assert fetched["a"] == [("b", 0)]
    record("concurrency.shared_connection",
           "o fetchall da thread A devolveu o resultado da thread B, sem erro")

    # Dois connect() em memória são dois bancos: a tabela criada num não existe no outro.
    other = duckdb.connect()
    with pytest.raises(duckdb.CatalogException, match="Table with name t_a does not exist"):
        other.execute("SELECT * FROM t_a")
    other.close()
    con.close()


def test_duckdb_thread_pool_is_global_and_each_caller_joins_it() -> None:
    """O ``threads`` do DuckDB é da instância: vale para todas as sessões, muda em execução por
    ``SET threads`` e recusa o escopo de sessão.

    A thread que chama cada sessão também executa a consulta dela, ao lado do pool: com
    ``threads = 1``, quatro sessões em quatro threads Python levam quase o tempo de uma. Com
    ``threads`` igual aos núcleos, o padrão, uma varredura grande já ocupa a máquina, e quatro
    sessões juntas levam o tempo das quatro em série. Os tempos são leituras do relatório, porque
    dependem dos núcleos livres.
    """
    con = duckdb.connect()
    settings = con.execute(
        "SELECT name, scope FROM duckdb_settings() WHERE name IN ('threads', 'external_threads')"
    ).fetchall()
    assert dict(settings) == {"threads": "GLOBAL", "external_threads": "GLOBAL"}
    with pytest.raises(duckdb.CatalogException, match='option "threads" cannot be set locally'):
        con.execute("SET SESSION threads = 3")

    # O valor que uma sessão grava é o que a outra lê: o pool é um só.
    other = con.cursor()
    con.execute("SET threads = 3")
    assert other.execute("SELECT current_setting('threads')").fetchone()[0] == 3
    other.close()

    # 20.000.000 de linhas são 163 grupos de 122.880, a unidade da varredura paralela.
    con.execute("CREATE TABLE numeros AS SELECT range AS id FROM range(20_000_000)")

    def scan(connection: duckdb.DuckDBPyConnection) -> None:
        connection.execute("SELECT sum(hash(id)) FROM numeros").fetchall()

    def four_sessions_together() -> float:
        cursors = [con.cursor() for _ in range(4)]
        elapsed = run_in_threads([functools.partial(scan, cursor) for cursor in cursors])
        for cursor in cursors:
            cursor.close()
        return elapsed

    readings = []
    for threads in (1, os.cpu_count()):
        con.execute(f"SET threads = {threads}")
        one = min(run_in_threads([functools.partial(scan, con)]) for _ in range(3))
        four = min(four_sessions_together() for _ in range(3))
        readings.append(f"threads={threads}: uma sessão {one:.3f} s, "
                        f"quatro juntas {four:.3f} s ({four / one:.2f}x)")
    record("concurrency.duckdb_thread_pool", "; ".join(readings))
    con.close()


@pytest.mark.local
def test_deltalake_and_delta_scan_release_the_gil(local_location: LocalLocation) -> None:
    """O laço Python mantém a taxa durante ``write_deltalake``, ``to_pyarrow_table`` e o
    ``delta_scan`` do DuckDB."""
    con = connect_duckdb(["delta"])
    con.execute("SET threads = 2")
    table = large_table(con)
    uri = local_location.child("gil")
    reference = reference_rate()

    def aggregate_by_delta_scan() -> None:
        con.execute(f"SELECT count(*), sum(v) FROM delta_scan('{uri}')").fetchall()

    assert_gil_released(reference, "deltalake_write",
                        lambda: write_deltalake(uri, table, mode="overwrite"))
    assert_gil_released(reference, "deltalake_to_pyarrow_table",
                        lambda: DeltaTable(uri).to_pyarrow_table())
    assert_gil_released(reference, "duckdb_delta_scan", aggregate_by_delta_scan)
    con.close()


@pytest.mark.local
def test_two_threads_run_native_work_in_parallel(local_location: LocalLocation) -> None:
    """Duas escritas Delta em tabelas distintas e dois ``CREATE TABLE AS`` em dois cursores, em
    sequência e em duas threads.

    Os dois commits e as duas tabelas existem nos dois casos. O ``threads`` do DuckDB é da
    instância, não do cursor: dois comandos em paralelo dividem o mesmo pool.
    """
    con = duckdb.connect(str(local_location.path / "parallel.duckdb"), config={"threads": 2})
    table = large_table(con)

    def compare(label: str, action: Callable[[int], object]) -> None:
        """``action(0)`` e ``action(1)`` em sequência, ``action(2)`` e ``action(3)`` em duas
        threads; os tempos vão ao relatório."""
        started = time.perf_counter()
        action(0)
        action(1)
        sequential = time.perf_counter() - started
        parallel = run_in_threads([functools.partial(action, 2), functools.partial(action, 3)])
        record(f"concurrency.timing.{label}",
               f"sequencial {sequential:.3f} s, duas threads {parallel:.3f} s "
               f"({sequential / parallel:.2f}x)")

    def delta_write(k: int) -> None:
        write_deltalake(local_location.child(f"w{k}"), table, mode="overwrite")

    def create_table(k: int) -> None:
        cursor = con.cursor()
        cursor.execute(f"CREATE TABLE c{k} AS SELECT range AS x FROM range(10_000_000)")
        cursor.close()

    compare("deltalake_write_two_tables", delta_write)
    for k in range(4):
        written = DeltaTable(local_location.child(f"w{k}"))
        assert written.version() == 0
        assert written.to_pyarrow_dataset().count_rows() == LARGE_ROWS

    compare("duckdb_create_table_as_two_cursors", create_table)
    listed = "SELECT list(table_name ORDER BY table_name) FROM duckdb_tables()"
    assert con.execute(listed).fetchone()[0] == ["c0", "c1", "c2", "c3"]
    con.close()


@pytest.mark.local
def test_duckdb_file_is_shared_in_the_process_by_the_same_configuration(
        local_location: LocalLocation) -> None:
    """Dois ``connect()`` do mesmo arquivo com a mesma configuração abrem a mesma instância.

    Outra configuração, ou ``read_only``, é recusada enquanto a primeira está aberta.
    """
    path = str(local_location.path / "shared.duckdb")
    first = duckdb.connect(path, config={"threads": 2})
    first.execute("CREATE TABLE t AS SELECT 1 AS x")
    second = duckdb.connect(path, config={"threads": 2})
    assert second.execute("SELECT x FROM t").fetchall() == [(1,)]

    refused = ("Can't open a connection to same database file with a different configuration "
               "than existing connections")
    with pytest.raises(duckdb.ConnectionException, match=refused):
        duckdb.connect(path, config={"threads": 3})
    with pytest.raises(duckdb.ConnectionException, match=refused):
        duckdb.connect(path, read_only=True)
    record("concurrency.duckdb_file_second_connect",
           "a mesma configuração compartilha a instância; outra configuração ou read_only: "
           "ConnectionException")
    second.close()
    first.close()


@pytest.mark.local
def test_delta_readers_keep_their_version_while_a_writer_commits(
        local_location: LocalLocation) -> None:
    """Quatro leitores carregam a versão 0, um ``append`` cria a versão 1 enquanto eles esperam, e
    cada um lê as linhas da versão 0."""
    uri = local_location.child("readers")
    sample = sample_table()
    write_deltalake(uri, sample, mode="overwrite", partition_by=["mes"])
    # Prazo na barreira e no evento: um leitor que falha não prende os outros nem a suíte.
    loaded = threading.Barrier(5, timeout=10)
    commit_done = threading.Event()
    seen: dict[int, tuple[int, int]] = {}

    def read(k: int) -> None:
        table = DeltaTable(uri)  # o snapshot é a versão carregada aqui, sem sessão nem bloqueio
        loaded.wait()
        assert commit_done.wait(timeout=10)
        seen[k] = (table.version(), table.to_pyarrow_table().num_rows)

    # A exceção de um leitor sobe no result() do futuro dele, em vez de só ser impressa pela
    # thread.
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(read, k) for k in range(4)]
        loaded.wait()
        write_deltalake(uri, sample.slice(0, APPENDED_ROWS), mode="append")
        commit_done.set()
        for future in futures:
            future.result()

    assert seen == {k: (0, ROWS) for k in range(4)}
    current = DeltaTable(uri)
    assert current.version() == 1
    assert current.to_pyarrow_dataset().count_rows() == ROWS + APPENDED_ROWS

    # Quatro leituras da mesma versão em paralelo contra uma: leitura do relatório.
    def read_current() -> None:
        DeltaTable(uri).to_pyarrow_table()

    started = time.perf_counter()
    current.to_pyarrow_table()
    one = time.perf_counter() - started
    four = run_in_threads([read_current] * 4)
    record("concurrency.timing.delta_read_one_vs_four_threads",
           f"uma leitura {one:.3f} s, quatro em paralelo {four:.3f} s")
