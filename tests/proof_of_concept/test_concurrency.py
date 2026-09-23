"""Threads do Python sobre os pacotes nativos: o GIL liberado, um cursor DuckDB por thread e o paralelismo real.

Sem gravar arquivo: o laço Python que mantém a taxa noutra thread enquanto o DuckDB agrega e
converte para Arrow e o PyArrow grava e lê Parquet em memória (o GIL liberado); o ``threadsafety``
dos drivers, o ``cursor()`` por thread, a conexão compartilhada que entrega a uma thread o
resultado da outra, os dois ``connect()`` em memória que são bancos distintos; o intervalo de troca do
GIL pago por cada retomada ao lado de uma thread Python ocupada (``os.stat`` e o import preguiçoso);
as faixas de identificadores tiradas de um contador sob ``Lock``. Sob a raiz local (marcador ``local``): o GIL
liberado pelo delta-rs e pelo ``delta_scan``, duas escritas Delta e dois ``CREATE TABLE AS`` em duas
threads, o arquivo do DuckDB compartilhado no processo pela mesma configuração e recusado com outra,
os leitores Delta presos à versão carregada enquanto um ``append`` entra.

O GIL liberado é asserção: com ele retido o laço Python para, e a taxa cai perto de zero. O ganho de
tempo de duas threads é leitura do relatório, porque depende dos núcleos livres da máquina.
"""

from __future__ import annotations

import importlib
import itertools
import os
import random
import sys
import threading
import time
from collections.abc import Callable

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

# Importado aqui, e não dentro de ``pq.read_table``, que o importa na primeira chamada: um import ao lado
# de uma thread Python ocupada leva segundos (``test_gil_reacquisition_waits_the_switch_interval``).
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
    sql = f"SELECT range AS id, range % 12 AS m, ((range * 7) % 1000) / 100.0 AS v, 'x' || range AS s FROM range({LARGE_ROWS})"
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
    """Mede ``action`` contra a referência, registra e reprova a taxa abaixo de ``GIL_RELEASED_SHARE``."""
    elapsed, rate = python_rate_during(action)
    share = rate / reference
    record(f"concurrency.gil.{label}", f"{elapsed:.3f} s; laço Python a {share:.0%} da referência ({rate / 1e6:.1f} M it/s)")
    assert share >= GIL_RELEASED_SHARE, f"{label}: o laço Python caiu a {share:.0%} da referência, o GIL ficou retido"


def test_duckdb_and_pyarrow_release_the_gil() -> None:
    """O laço Python mantém a taxa enquanto o DuckDB agrega e converte para Arrow e o PyArrow grava e lê Parquet em memória."""
    con = duckdb.connect(config={"threads": 2})
    reference = reference_rate()
    record("concurrency.gil.reference", f"{reference / 1e6:.1f} M it/s com a thread principal em sleep")

    assert_gil_released(reference, "duckdb_aggregate", lambda: con.execute("SELECT sum(range * range) FROM range(60_000_000)").fetchall())

    produced: dict[str, pa.Table] = {}
    assert_gil_released(reference, "duckdb_to_arrow_table", lambda: produced.setdefault("table", large_table(con)))

    # Parquet em memória: o mesmo escritor e leitor dos arquivos, sem tocar o disco.
    sink = pa.BufferOutputStream()
    assert_gil_released(reference, "pyarrow_write_parquet", lambda: pq.write_table(produced["table"], sink))
    buffer = sink.getvalue()
    assert_gil_released(reference, "pyarrow_read_parquet", lambda: pq.read_table(pa.BufferReader(buffer)))
    con.close()


def test_gil_reacquisition_waits_the_switch_interval() -> None:
    """Uma chamada nativa curta que solta e retoma o GIL espera o intervalo de troca enquanto outra thread roda Python puro.

    Duzentos ``os.stat`` levam décimos de milissegundo sozinhos e centenas de milissegundos ao lado do
    laço, um intervalo por retomada; ``sys.setswitchinterval`` menor encurta a espera. Um import
    preguiçoso paga o mesmo por cada ``stat`` do ``importlib``: ``import pyarrow.dataset``, que
    ``pq.read_table`` faz na primeira chamada, levou 15 s ao lado do laço contra 0,19 s sozinho
    (2026-09-20, macOS). A biblioteca importa tudo na abertura.
    """

    def stats() -> None:
        for _ in range(200):
            os.stat(".")

    started = time.perf_counter()
    stats()
    alone = time.perf_counter() - started
    beside, _ = python_rate_during(stats)
    default = sys.getswitchinterval()
    sys.setswitchinterval(default / 10)
    try:
        shorter, _ = python_rate_during(stats)
    finally:
        sys.setswitchinterval(default)
    record(
        "concurrency.gil.os_stat_200",
        f"sozinho {alone * 1e3:.1f} ms; ao lado do laço {beside:.3f} s; com switchinterval {default / 10:.4f} s: {shorter:.3f} s",
    )
    assert beside >= 20 * alone and shorter < beside / 2

    # Um módulo ainda não importado, se houver: o import ao lado do laço, como leitura.
    name = next((candidate for candidate in ("xml.dom.minidom", "email.mime.text", "wsgiref.simple_server") if candidate not in sys.modules), None)
    if name:
        elapsed, _ = python_rate_during(lambda: importlib.import_module(name))
        record("concurrency.gil.lazy_import_beside_busy_thread", f"import {name}: {elapsed:.3f} s")


def test_drivers_share_the_module_not_the_connection() -> None:
    """``threadsafety`` 1 nos dois drivers: as threads compartilham o módulo, não a conexão.

    Um ``cursor()`` por thread grava no mesmo banco em memória; a conexão compartilhada entrega a
    uma thread o resultado da outra; dois ``connect()`` em memória são bancos distintos.
    """
    assert duckdb.threadsafety == 1 and redshift_connector.threadsafety == 1
    record("concurrency.threadsafety", f"duckdb {duckdb.threadsafety}, redshift_connector {redshift_connector.threadsafety}")

    # Um cursor por thread: cada thread cria a sua tabela, e a conexão raiz vê as duas.
    con = duckdb.connect(config={"threads": 2})

    def create(name: str) -> None:
        cursor = con.cursor()
        cursor.execute(f"CREATE TABLE {name} AS SELECT range AS x FROM range(1000)")
        cursor.close()

    run_in_threads([lambda: create("t_a"), lambda: create("t_b")])
    assert con.execute("SELECT (SELECT count(*) FROM t_a), (SELECT count(*) FROM t_b)").fetchone() == (1000, 1000)

    # A conexão compartilhada: A executa, B executa na mesma conexão, e o fetchall de A traz as linhas de B.
    a_executed, b_executed = threading.Event(), threading.Event()
    fetched: dict[str, list[tuple]] = {}

    # Toda espera tem prazo: se uma thread falha antes do set, a outra falha em vez de travar a suíte.
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
    record("concurrency.shared_connection", "o fetchall da thread A devolveu o resultado da thread B, sem erro")

    # Dois connect() em memória são dois bancos: a tabela criada num não existe no outro.
    other = duckdb.connect()
    with pytest.raises(duckdb.CatalogException, match="Table with name t_a does not exist"):
        other.execute("SELECT * FROM t_a")
    other.close()
    con.close()


class RangeAllocator:
    """Faixas contíguas de inteiros tiradas de um contador sob ``Lock``, o que ``run.next_ids`` faria."""

    def __init__(self, start: int) -> None:
        self._next = start
        self._lock = threading.Lock()

    def take(self, n: int) -> range:
        with self._lock:
            start = self._next
            self._next += n

        return range(start, start + n)


def test_id_ranges_from_a_locked_counter() -> None:
    """Oito threads tiram faixas de tamanhos variados; as faixas são disjuntas e cobrem o intervalo sem buraco."""
    # Um gerador por thread, criado uma vez: cem tamanhos diferentes de 1 a 500 por thread.
    sizes = []
    for seed in range(8):
        generator = random.Random(seed)
        sizes.append([generator.randint(1, 500) for _ in range(100)])
    allocator = RangeAllocator(start=1_001)
    taken: list[range] = []
    collect = threading.Lock()

    def worker(k: int) -> None:
        for n in sizes[k]:
            taken_range = allocator.take(n)
            with collect:
                taken.append(taken_range)

    run_in_threads([lambda k=k: worker(k) for k in range(8)])

    ordered = sorted(taken, key=lambda taken_range: taken_range.start)
    total = sum(map(sum, sizes))
    assert ordered[0].start == 1_001 and ordered[-1].stop == 1_001 + total
    assert all(previous.stop == following.start for previous, following in itertools.pairwise(ordered))
    record("concurrency.id_ranges", f"{len(taken)} faixas, {total} identificadores, sem sobreposição nem buraco")


@pytest.mark.local
def test_deltalake_and_delta_scan_release_the_gil(local_location: LocalLocation) -> None:
    """O laço Python mantém a taxa durante ``write_deltalake``, ``to_pyarrow_table`` e o ``delta_scan`` do DuckDB."""
    con = connect_duckdb(["delta"])
    con.execute("SET threads = 2")
    table = large_table(con)
    uri = local_location.child("gil")
    reference = reference_rate()

    assert_gil_released(reference, "deltalake_write", lambda: write_deltalake(uri, table, mode="overwrite"))
    assert_gil_released(reference, "deltalake_to_pyarrow_table", lambda: DeltaTable(uri).to_pyarrow_table())
    assert_gil_released(reference, "duckdb_delta_scan", lambda: con.execute(f"SELECT count(*), sum(v) FROM delta_scan('{uri}')").fetchall())
    con.close()


@pytest.mark.local
def test_two_threads_run_native_work_in_parallel(local_location: LocalLocation) -> None:
    """Duas escritas Delta em tabelas distintas e dois ``CREATE TABLE AS`` em dois cursores, em sequência e em duas threads.

    Os dois commits e as duas tabelas existem nos dois casos. O ``threads`` do DuckDB é da instância,
    não do cursor: dois comandos em paralelo dividem o mesmo pool.
    """
    con = duckdb.connect(str(local_location.path / "parallel.duckdb"), config={"threads": 2})
    table = large_table(con)

    def compare(label: str, make: Callable[[int], Callable[[], object]]) -> None:
        started = time.perf_counter()
        make(0)()
        make(1)()
        sequential = time.perf_counter() - started
        parallel = run_in_threads([make(2), make(3)])
        record(f"concurrency.timing.{label}", f"sequencial {sequential:.3f} s, duas threads {parallel:.3f} s ({sequential / parallel:.2f}x)")

    def delta_write(k: int) -> Callable[[], object]:
        return lambda: write_deltalake(local_location.child(f"w{k}"), table, mode="overwrite")

    def create_table(k: int) -> Callable[[], object]:
        def action() -> None:
            cursor = con.cursor()
            cursor.execute(f"CREATE TABLE c{k} AS SELECT range AS x FROM range(10_000_000)")
            cursor.close()

        return action

    compare("deltalake_write_two_tables", delta_write)
    for k in range(4):
        written = DeltaTable(local_location.child(f"w{k}"))
        assert written.version() == 0 and written.to_pyarrow_dataset().count_rows() == LARGE_ROWS

    compare("duckdb_create_table_as_two_cursors", create_table)
    assert con.execute("SELECT list(table_name ORDER BY table_name) FROM duckdb_tables()").fetchone()[0] == ["c0", "c1", "c2", "c3"]
    con.close()


@pytest.mark.local
def test_duckdb_file_is_shared_in_the_process_by_the_same_configuration(local_location: LocalLocation) -> None:
    """Dois ``connect()`` do mesmo arquivo com a mesma configuração abrem a mesma instância; outra configuração, ou ``read_only``, é recusada enquanto a primeira está aberta."""
    path = str(local_location.path / "shared.duckdb")
    first = duckdb.connect(path, config={"threads": 2})
    first.execute("CREATE TABLE t AS SELECT 1 AS x")
    second = duckdb.connect(path, config={"threads": 2})
    assert second.execute("SELECT x FROM t").fetchall() == [(1,)]

    refused = "Can't open a connection to same database file with a different configuration than existing connections"
    with pytest.raises(duckdb.ConnectionException, match=refused):
        duckdb.connect(path, config={"threads": 3})
    with pytest.raises(duckdb.ConnectionException, match=refused):
        duckdb.connect(path, read_only=True)
    record("concurrency.duckdb_file_second_connect", "a mesma configuração compartilha a instância; outra configuração ou read_only: ConnectionException")
    second.close()
    first.close()


@pytest.mark.local
def test_delta_readers_keep_their_version_while_a_writer_commits(local_location: LocalLocation) -> None:
    """Quatro leitores carregam a versão 0, um ``append`` cria a versão 1 enquanto eles esperam, e cada um lê as linhas da versão 0."""
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

    readers = [threading.Thread(target=read, args=(k,)) for k in range(4)]
    for reader in readers:
        reader.start()
    loaded.wait()
    write_deltalake(uri, sample.slice(0, APPENDED_ROWS), mode="append")
    commit_done.set()
    for reader in readers:
        reader.join()

    assert seen == {k: (0, ROWS) for k in range(4)}
    current = DeltaTable(uri)
    assert current.version() == 1 and current.to_pyarrow_dataset().count_rows() == ROWS + APPENDED_ROWS

    # Quatro leituras da mesma versão em paralelo contra uma: leitura do relatório.
    started = time.perf_counter()
    current.to_pyarrow_table()
    one = time.perf_counter() - started
    four = run_in_threads([lambda: DeltaTable(uri).to_pyarrow_table() for _ in range(4)])
    record("concurrency.timing.delta_read_one_vs_four_threads", f"uma leitura {one:.3f} s, quatro em paralelo {four:.3f} s")
