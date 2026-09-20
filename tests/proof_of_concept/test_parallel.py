"""Leitura e escrita em paralelo em cada tecnologia, e as APIs da implementação do paralelismo.

Sem gravar arquivo: a conexão por thread do motor (``threading.local`` sobre ``cursor()``), a saída
em lotes ``BatchStream`` (a consulta num cursor próprio e uma thread que pré-busca os lotes numa fila
limitada) e a entrada em lotes ``Loader`` (o cliente empurra lotes numa fila e uma thread roda um
único ``INSERT``), o pipeline de três estágios que as encadeia, o pool de ``ingest`` e ``publish``
que termina o que está em curso e cancela o que não começou na primeira falha
(``shutdown(cancel_futures=True)``), a barreira por tabela com ``Condition`` e as tabelas de um
statement por ``find_tables`` ou pelo sentinela ``{prefix}``. Sob a raiz local (marcador
``local``): várias tabelas Delta lidas em paralelo pelo delta-rs e ingeridas em paralelo no DuckDB
por ``delta_scan`` em cursores; escritas Delta em paralelo em tabelas distintas e em meses distintos
da mesma tabela, e o conflito de dois escritores no mesmo mês; o início do alocador de
identificadores pelas estatísticas dos arquivos, com a varredura como reserva; cargas Arrow e
exportações ``COPY ... TO`` em paralelo no DuckDB. Os tempos são leituras do relatório.

O Redshift está em ``test_redshift.py`` (``test_parallel_copy_and_unload_on_two_connections``).
"""

from __future__ import annotations

import decimal
import json
import queue
import re
import threading
import time
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError
from deltalake.transaction import AddAction
from sqlalchemy.sql.util import find_tables

from conftest import LocalLocation, record
from poc_delta import MONTHS, ROWS, connect_duckdb, sample_table

FOUR_MONTHS = ("2026-01", "2026-02", "2026-03", "2026-04")


def four_month_table(con: duckdb.DuckDBPyConnection, rows: int = 400_000) -> pa.Table:
    """``rows`` linhas com ``mes`` em quatro valores, o inteiro, um decimal e um texto, geradas pelo DuckDB."""
    sql = (
        "SELECT range AS id, '2026-0' || (1 + range % 4) AS mes, CAST(((range * 7) % 1000) / 100.0 AS DECIMAL(18, 2)) AS valor, "
        f"'x' || range AS descricao FROM range({rows})"
    )
    return con.execute(sql).to_arrow_table()


def run_in_threads(actions: list[Callable[[], object]]) -> float:
    """Roda as ações em threads, uma por ação, e devolve o tempo até a última terminar."""
    threads = [threading.Thread(target=action) for action in actions]
    started = time.perf_counter()
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    return time.perf_counter() - started


# --- As APIs da implementação -------------------------------------------------------------------


class SandboxEngine:
    """O esboço do motor DuckDB com uma conexão por thread.

    A conexão raiz abre o banco; cada thread recebe um ``cursor()`` dela no primeiro uso, guardado
    num ``threading.local``, e ``cleanup`` fecha todos. O código cliente chama ``query`` e ``load``
    de qualquer thread sem ver conexão; ``connection`` é a conexão crua da thread.
    """

    def __init__(self, path: str = ":memory:") -> None:
        self._root = duckdb.connect(path, config={"threads": 2})
        self._local = threading.local()
        self._cursors: list[duckdb.DuckDBPyConnection] = []
        self._lock = threading.Lock()

    @property
    def connection(self) -> duckdb.DuckDBPyConnection:
        cursor = getattr(self._local, "cursor", None)
        if cursor is None:
            cursor = self._root.cursor()
            self._local.cursor = cursor
            with self._lock:
                self._cursors.append(cursor)

        return cursor

    def query(self, sql: str) -> pa.Table:
        return self.connection.execute(sql).to_arrow_table()

    def load(self, name: str, data: pa.Table) -> None:
        # O registro da tabela Arrow vale só na conexão que o fez, por isso entra e sai na mesma chamada.
        connection = self.connection
        connection.register("data_in", data)
        connection.execute(f"CREATE TABLE {name} AS SELECT * FROM data_in")
        connection.unregister("data_in")

    def cleanup(self) -> None:
        with self._lock:
            for cursor in self._cursors:
                cursor.close()
        self._root.close()


def test_engine_hands_each_thread_its_own_cursor() -> None:
    """Quatro threads carregam e consultam pelo mesmo motor: cada uma recebe um cursor próprio, todas veem as tabelas das outras, e ``cleanup`` fecha tudo."""
    engine = SandboxEngine()
    cursors: dict[int, int] = {}
    counts: dict[int, int] = {}

    def work(k: int) -> None:
        engine.load(f"t{k}", pa.table({"x": pa.array(range(1000 * (k + 1)), pa.int64())}))
        cursors[k] = id(engine.connection)
        counts[k] = engine.query(f"SELECT count(*) AS n FROM t{k}").column("n")[0].as_py()

    run_in_threads([lambda k=k: work(k) for k in range(4)])
    assert counts == {0: 1000, 1: 2000, 2: 3000, 3: 4000}
    assert len(set(cursors.values())) == 4 and id(engine.connection) not in cursors.values()

    # A thread principal, no seu cursor, vê as quatro tabelas.
    assert engine.query("SELECT list(table_name ORDER BY table_name) AS t FROM duckdb_tables()").column("t")[0].as_py() == ["t0", "t1", "t2", "t3"]

    engine.cleanup()
    with pytest.raises(duckdb.ConnectionException, match="Connection already closed"):
        engine.query("SELECT 1")


END = object()


def offer(sink: queue.Queue[object], item: object, stop: threading.Event) -> bool:
    """Põe ``item`` na fila com espera limitada; ``False`` quando ``stop`` chegou antes de haver lugar."""
    while not stop.is_set():
        try:
            sink.put(item, timeout=0.05)
            return True
        except queue.Full:
            continue
    return False


def pump(reader: pa.RecordBatchReader, sink: queue.Queue[object], stop: threading.Event) -> None:
    """A thread de ``BatchStream``: puxa os lotes do leitor para a fila até o fim, o erro da consulta ou o ``stop``.

    Recebe só o que usa, nunca o ``BatchStream``: sem esse ciclo de referências, um stream que o
    cliente abandona é coletado, o ``__del__`` liga o ``stop``, e a thread termina.
    """
    try:
        for batch in reader:
            if not offer(sink, batch, stop):
                return
        offer(sink, END, stop)
    except Exception as error:  # noqa: BLE001 - o erro da consulta vai ao consumidor
        offer(sink, error, stop)


class BatchStream:
    """O esboço da saída em lotes do motor (``stream`` da etapa 4).

    A consulta roda num cursor próprio, e uma thread auxiliar puxa os lotes para uma fila de
    ``prefetch`` lotes enquanto o cliente trabalha no lote atual; o cursor da thread do cliente fica
    livre. ``close`` interrompe a thread e fecha o cursor; o erro da consulta aparece na construção
    ou na leitura seguinte. Com ``prefetch=0`` não há thread, e o cliente puxa direto do leitor.
    ``__arrow_c_stream__`` entrega o leitor a ``write_deltalake`` e a ``RecordBatchReader.from_stream``;
    não ao ``register`` do DuckDB, cujo ``arrow_scan`` puxa o fluxo numa thread de leitura
    antecipada do Arrow que continua chamando Python depois de o comando terminar
    (``test_duckdb.py``); para levar lotes ao sandbox existe ``Loader``.
    """

    def __init__(self, root: duckdb.DuckDBPyConnection, sql: str, batch_size: int = 100_000, prefetch: int = 2) -> None:
        self._cursor = root.cursor()
        try:
            self._reader = self._cursor.execute(sql).to_arrow_reader(batch_size)
        except Exception:
            self._cursor.close()
            raise
        self.schema = self._reader.schema
        self._stop = threading.Event()
        self._queue: queue.Queue[object] | None = queue.Queue(maxsize=prefetch) if prefetch > 0 else None
        self._thread = threading.Thread(target=pump, args=(self._reader, self._queue, self._stop)) if self._queue is not None else None
        if self._thread is not None:
            self._thread.start()

    def read_next_batch(self) -> pa.RecordBatch:
        if self._queue is None:
            return self._reader.read_next_batch()
        while True:
            try:
                item = self._queue.get(timeout=0.05)
                break
            except queue.Empty:
                if self._stop.is_set():
                    raise StopIteration from None
        if item is END:
            self._queue.put(item)
            raise StopIteration
        if isinstance(item, Exception):
            raise item
        return item

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        while True:
            try:
                yield self.read_next_batch()
            except StopIteration:
                return

    def read_all(self) -> pa.Table:
        return pa.Table.from_batches(list(self), schema=self.schema)

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        return pa.RecordBatchReader.from_batches(self.schema, iter(self)).__arrow_c_stream__(requested_schema)

    def close(self) -> None:
        self._stop.set()
        if self._thread is not None:
            while True:  # esvazia a fila para a thread sair do put
                try:
                    self._queue.get_nowait()
                except queue.Empty:
                    break
            self._thread.join(timeout=10)
        self._cursor.close()

    def __enter__(self) -> BatchStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __del__(self) -> None:
        self._stop.set()


def insert_batches(cursor: duckdb.DuckDBPyConnection, table: str, source: queue.Queue[object], closed: threading.Event, outcome: dict[str, object]) -> None:
    """A thread de ``Loader``: uma transação, um ``INSERT`` por lote tirado da fila, ``commit`` no fim e ``rollback`` em qualquer erro.

    Cada lote é um ``RecordBatch`` em memória registrado por um comando: o ``arrow_scan`` o esgota
    dentro do comando, e nenhuma thread do DuckDB volta a chamar Python depois. Um ``closed`` sem o
    fim da fila é um loader abandonado, e a transação é desfeita.
    """
    cursor.begin()
    try:
        while True:
            try:
                item = source.get(timeout=0.05)
            except queue.Empty:
                if closed.is_set():
                    raise RuntimeError("loader encerrado sem close")
                continue
            if item is END:
                break
            if isinstance(item, BaseException):
                raise item
            cursor.register("lote", item)
            cursor.execute(f"INSERT INTO {table} BY NAME SELECT * FROM lote")
            cursor.unregister("lote")
            outcome["rows"] += item.num_rows
        cursor.commit()
    except BaseException as error:  # noqa: BLE001 - relançado em close, ou em write quando a thread já terminou
        cursor.rollback()
        outcome["error"] = error
    finally:
        cursor.close()


class Loader:
    """O esboço da entrada em lotes do motor (``loader`` da etapa 4).

    O cliente empurra lotes numa fila limitada, e uma thread auxiliar os insere numa transação num
    cursor próprio enquanto o cliente prepara o lote seguinte. ``write`` faz o cast do lote na thread
    do cliente, para o erro aparecer com o lote em mãos, e bloqueia quando a fila está cheia;
    ``close`` encerra a fila, espera o ``commit`` e relança o erro da thread. Uma exceção dentro do
    ``with`` desfaz a transação, e a tabela fica como estava; nada é visível antes do ``commit``.

    O ``INSERT`` único sobre um leitor da fila é mais rápido (``test_duckdb.py`` mede os dois), mas
    entrega um gerador Python ao ``arrow_scan``, cuja leitura antecipada puxa cerca de dez lotes além
    do que o comando consumiu e continua depois de ele falhar: o buffer foge do controle da fila, e a
    thread do Arrow ainda chamando Python na saída do processo o pendura.
    """

    def __init__(self, root: duckdb.DuckDBPyConnection, table: str, schema: pa.Schema, queue_depth: int = 2) -> None:
        self._schema = schema
        self._queue: queue.Queue[object] = queue.Queue(maxsize=queue_depth)
        self._closed = threading.Event()
        self._outcome: dict[str, object] = {"rows": 0, "error": None}
        self._thread = threading.Thread(target=insert_batches, args=(root.cursor(), table, self._queue, self._closed, self._outcome))
        self._thread.start()

    @property
    def rows(self) -> int:
        return self._outcome["rows"]

    @property
    def error(self) -> BaseException | None:
        return self._outcome["error"]

    def _put(self, item: object) -> bool:
        """Põe o item na fila; ``False`` quando a thread já terminou, com o erro dela em ``error``."""
        while self._thread.is_alive():
            try:
                self._queue.put(item, timeout=0.05)
                return True
            except queue.Full:
                continue
        return False

    def write(self, data: pa.RecordBatch | pa.Table) -> None:
        for batch in data.to_batches() if isinstance(data, pa.Table) else [data]:
            if not self._put(batch.cast(self._schema)):
                raise self.error or RuntimeError("a thread do loader terminou antes do fim da fila")

    def close(self, error: BaseException | None = None) -> None:
        self._put(error if error is not None else END)
        self._closed.set()
        self._thread.join()
        if error is None and self.error is not None:
            raise self.error

    def __enter__(self) -> Loader:
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        self.close(error=exc)

    def __del__(self) -> None:
        self._closed.set()


def test_batch_stream_prefetches_on_its_own_cursor_and_stops_early() -> None:
    """``BatchStream`` entrega os lotes na ordem da consulta enquanto o cursor da thread do cliente roda outros comandos, guarda no máximo ``prefetch`` lotes, para a thread num ``close`` antecipado e traz o erro da consulta ao cliente."""
    engine = SandboxEngine()
    engine.query("CREATE TABLE numeros AS SELECT range AS id, 'x' || range AS s FROM range(1_000_000)")

    seen = 0
    with BatchStream(engine._root, "SELECT id, s FROM numeros ORDER BY id", batch_size=100_000, prefetch=2) as stream:
        for batch in stream:
            assert batch.column("id")[0].as_py() == seen and stream._queue.qsize() <= 2
            engine.query("SELECT count(*) FROM numeros")  # o cursor da thread do cliente está livre
            seen += batch.num_rows
    assert seen == 1_000_000 and not stream._thread.is_alive()

    # Sem thread, o cliente puxa direto do leitor; read_all monta a tabela.
    with BatchStream(engine._root, "SELECT id FROM numeros WHERE id < 10 ORDER BY id", prefetch=0) as direct:
        assert direct.read_all().column("id").to_pylist() == list(range(10))

    # O close antecipado para a thread, presa na fila cheia, e fecha o cursor.
    stream = BatchStream(engine._root, "SELECT id FROM numeros", batch_size=1000, prefetch=2)
    stream.read_next_batch()
    time.sleep(0.05)
    assert stream._queue.full()
    stream.close()
    assert not stream._thread.is_alive()

    # __arrow_c_stream__: o stream é um leitor para quem o pede pelo PyCapsule, como write_deltalake.
    with BatchStream(engine._root, "SELECT id FROM numeros WHERE id < 5000", batch_size=1000) as source:
        assert pa.RecordBatchReader.from_stream(source).read_all().num_rows == 5000

    # Um stream abandonado sem close: a thread não referencia o objeto, o __del__ liga o stop, e ela termina.
    abandoned = BatchStream(engine._root, "SELECT id FROM numeros", batch_size=1000, prefetch=2)
    abandoned.read_next_batch()
    thread = abandoned._thread
    del abandoned
    thread.join(timeout=2)
    assert not thread.is_alive()

    # O erro da consulta chega ao cliente, nunca em silêncio: duckdb.Error na construção, ou OSError com a
    # mensagem do DuckDB quando o leitor Arrow o encontra no meio da leitura.
    failing_sql = "SELECT CAST(CASE WHEN id = 900_000 THEN 'x' ELSE CAST(id AS VARCHAR) END AS INTEGER) AS n FROM numeros"
    with pytest.raises((duckdb.ConversionException, OSError), match="Could not convert string 'x' to INT32") as failure:
        with BatchStream(engine._root, failing_sql, batch_size=100_000) as failing:
            for _ in failing:
                pass
    record("parallel.stream.query_error_class", f"{type(failure.value).__name__} na leitura")
    engine.cleanup()


def test_loader_writes_behind_in_one_statement_and_aborts_on_client_error() -> None:
    """``Loader`` insere cada lote numa transação enquanto o cliente prepara o lote seguinte: nada é visível antes do ``close``, uma exceção do cliente ou um lote fora do contrato desfaz a transação, e o erro do ``INSERT`` chega ao cliente."""
    engine = SandboxEngine()
    schema = pa.schema([("id", pa.int64()), ("valor", pa.decimal128(18, 2))])
    engine.query("CREATE TABLE destino (id BIGINT, valor DECIMAL(18, 2))")

    def batch(k: int, rows: int = 10_000, id_type: pa.DataType = pa.int64()) -> pa.RecordBatch:
        return pa.RecordBatch.from_pydict({"id": pa.array(range(k * rows, (k + 1) * rows), id_type), "valor": pa.array([decimal.Decimal(k) / 100] * rows, pa.decimal128(18, 2))})

    def count(table: str = "destino") -> int:
        return engine.query(f"SELECT count(*) AS n FROM {table}").column("n")[0].as_py()

    visible: list[int] = []
    with Loader(engine._root, "destino", schema) as loader:
        for k in range(20):
            loader.write(batch(k))
            visible.append(count())
    assert loader.rows == 200_000 and count() == 200_000 and set(visible) == {0}

    # Uma exceção dentro do with: a transação é desfeita, a tabela fica como estava, e a exceção que sobe é a do cliente.
    with pytest.raises(ValueError, match="falha do cliente"):
        with Loader(engine._root, "destino", schema) as loader:
            loader.write(batch(0))
            raise ValueError("falha do cliente")
    assert isinstance(loader.error, ValueError) and count() == 200_000

    # Um loader abandonado sem close: a thread desfaz a transação e termina.
    abandoned = Loader(engine._root, "destino", schema)
    abandoned.write(batch(0))
    thread = abandoned._thread
    del abandoned
    thread.join(timeout=2)
    assert not thread.is_alive() and count() == 200_000

    # Um lote fora do contrato é recusado no write, na thread do cliente, e nada entra.
    with pytest.raises(pa.ArrowInvalid, match="Rescaling"):
        with Loader(engine._root, "destino", schema) as loader:
            loader.write(pa.RecordBatch.from_pydict({"id": pa.array([1], pa.int64()), "valor": pa.array([decimal.Decimal("1.2345")], pa.decimal128(20, 4))}))
    assert count() == 200_000

    # O erro do INSERT chega ao cliente: uma coluna estreita demais para o valor.
    engine.query("CREATE TABLE estreita (id INTEGER, valor DECIMAL(18, 2))")
    with pytest.raises(duckdb.ConversionException):
        with Loader(engine._root, "estreita", schema) as loader:
            for k in range(20):
                loader.write(batch(k).set_column(0, "id", pa.array([2**40] * 10_000, pa.int64())))
    assert count("estreita") == 0
    engine.cleanup()


def test_three_stage_pipeline_overlaps_read_work_and_write() -> None:
    """Leitura por ``BatchStream``, trabalho do cliente por lote e escrita por ``Loader`` produzem as mesmas linhas que a versão por lote sem threads e que a versão por ``pa.Table``; os tempos são leituras do relatório."""
    engine = SandboxEngine()
    engine.query("CREATE TABLE fonte AS SELECT range AS id, CAST(((range * 7) % 1000) / 100.0 AS DECIMAL(18, 2)) AS valor, 'x' || range AS s FROM range(3_000_000)")
    target = pa.schema([("id", pa.int64()), ("valor", pa.decimal128(18, 2)), ("s", pa.string()), ("dobro", pa.float64())])
    for name in ("sequencial", "encadeado"):
        engine.query(f"CREATE TABLE {name} (id BIGINT, valor DECIMAL(18, 2), s VARCHAR, dobro DOUBLE)")
    sql = "SELECT id, valor, s FROM fonte"

    def work(data: pa.RecordBatch | pa.Table) -> pa.RecordBatch | pa.Table:
        frame = data.to_pandas(types_mapper=pd.ArrowDtype)
        frame["dobro"] = (frame["valor"] * 2).astype("double[pyarrow]")
        return type(data).from_pandas(frame, preserve_index=False).cast(target)

    def insert(name: str, data: pa.RecordBatch | pa.Table) -> None:
        connection = engine.connection
        connection.register("lote", data)
        connection.execute(f"INSERT INTO {name} BY NAME SELECT * FROM lote")
        connection.unregister("lote")

    timings: dict[str, float] = {}

    # A tabela inteira: to_arrow_table, o trabalho de uma vez, a carga de uma vez.
    started = time.perf_counter()
    engine.load("por_tabela", work(engine.query(sql)))
    timings["por_tabela"] = time.perf_counter() - started

    # Por lote, sem threads: o leitor num cursor, o trabalho e um INSERT por lote no cursor da thread.
    started = time.perf_counter()
    reading = engine._root.cursor()
    for batch in reading.execute(sql).to_arrow_reader(200_000):
        insert("sequencial", work(batch))
    reading.close()
    timings["sequencial"] = time.perf_counter() - started

    # Encadeado: a leitura na thread do stream, o trabalho na thread do cliente, a escrita na thread do loader.
    started = time.perf_counter()
    with BatchStream(engine._root, sql, batch_size=200_000) as stream, Loader(engine._root, "encadeado", target) as loader:
        for batch in stream:
            loader.write(work(batch))
    timings["encadeado"] = time.perf_counter() - started

    totals = {
        name: engine.query(f"SELECT count(*) AS n, sum(valor) AS v, sum(dobro::DECIMAL(18, 2)) AS d FROM {name}").to_pylist()[0]
        for name in ("por_tabela", "sequencial", "encadeado")
    }
    assert totals["por_tabela"] == totals["sequencial"] == totals["encadeado"] and totals["encadeado"]["n"] == 3_000_000
    record("parallel.timing.three_stage_pipeline_3M_rows", ", ".join(f"{name} {seconds:.3f} s" for name, seconds in timings.items()))
    engine.cleanup()


class PublishFailed(Exception):
    """A falha de uma tabela com o resultado das demais: concluída, cancelada antes de começar, ou a falha."""

    def __init__(self, outcomes: dict[str, str]) -> None:
        super().__init__(", ".join(f"{table}: {outcome}" for table, outcome in sorted(outcomes.items())))
        self.outcomes = outcomes


def publish_all(tables: list[str], action: Callable[[str], object], max_workers: int) -> dict[str, str]:
    """Roda ``action`` por tabela num pool.

    Na primeira falha nada novo começa, o que está em curso termina e entra no resultado, o que não
    começou é cancelado, e ``PublishFailed`` traz o resultado de cada tabela: os commits feitos ficam,
    porque o Delta não tem transação entre tabelas.
    """
    outcomes: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(action, table): table for table in tables}
        for future in as_completed(futures):
            error = future.exception()
            if error is None:
                outcomes[futures[future]] = "concluída"
                continue

            outcomes[futures[future]] = f"falhou: {type(error).__name__}: {error}"
            pool.shutdown(wait=True, cancel_futures=True)
            for other, table in futures.items():
                if other.cancelled():
                    outcomes[table] = "cancelada"
                elif table not in outcomes:
                    outcomes[table] = "concluída" if other.exception() is None else f"falhou: {other.exception()}"
            raise PublishFailed(outcomes)

    return outcomes


def test_publish_pool_finishes_running_tables_and_cancels_the_rest() -> None:
    """Seis tabelas, dois workers, a segunda falha: a primeira e a terceira terminam, as três últimas são canceladas, e a exceção lista cada resultado."""
    started: list[str] = []
    lock = threading.Lock()

    def publish(table: str) -> None:
        with lock:
            started.append(table)
        if table == "t1":
            raise ValueError("commit conflict")
        time.sleep(0.5)

    with pytest.raises(PublishFailed) as failure:
        publish_all([f"t{k}" for k in range(6)], publish, max_workers=2)

    outcomes = failure.value.outcomes
    assert outcomes["t1"] == "falhou: ValueError: commit conflict"
    assert {table for table, outcome in outcomes.items() if outcome == "concluída"} == {"t0", "t2"}
    assert {table for table, outcome in outcomes.items() if outcome == "cancelada"} == {"t3", "t4", "t5"}
    assert sorted(started) == ["t0", "t1", "t2"]
    assert publish_all(["a", "b"], lambda table: None, max_workers=2) == {"a": "concluída", "b": "concluída"}


class TableBarrier:
    """As tabelas do sandbox com uma carga em voo; ``wait_for`` bloqueia até as citadas ficarem livres."""

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._in_flight: set[str] = set()

    @contextmanager
    def loading(self, table: str) -> Iterator[None]:
        with self._condition:
            self._in_flight.add(table)
        try:
            yield
        finally:
            with self._condition:
                self._in_flight.discard(table)
                self._condition.notify_all()

    def wait_for(self, tables: set[str], timeout: float = 60.0) -> None:
        with self._condition:
            if not self._condition.wait_for(lambda: not (self._in_flight & tables), timeout=timeout):
                raise TimeoutError(f"carga em voo: {sorted(self._in_flight & tables)}")


PREFIX_SENTINEL = re.compile(r"\{prefix\}(\w+)")


def referenced_tables(statement: sa.sql.ClauseElement | str) -> set[str]:
    """As tabelas que um statement Core ou um texto gerado referencia: ``find_tables`` ou o sentinela ``{prefix}``."""
    if isinstance(statement, str):
        return set(PREFIX_SENTINEL.findall(statement))

    return {table.name for table in find_tables(statement, include_crud=True) if isinstance(table, sa.Table)}


def test_table_barrier_delays_the_read_until_the_load_lands() -> None:
    """A leitura que cita a tabela em voo espera a carga; a que cita outra tabela passa na hora; as tabelas saem do statement Core ou do sentinela."""
    metadata = sa.MetaData()
    entries = sa.Table("cad_lancamentos", metadata, sa.Column("id_lancamento", sa.BigInteger), sa.Column("id_contrato", sa.BigInteger))
    contracts = sa.Table("cad_contratos", metadata, sa.Column("id_contrato", sa.BigInteger))
    statement = sa.select(entries.c.id_lancamento).select_from(entries.join(contracts, entries.c.id_contrato == contracts.c.id_contrato))
    assert referenced_tables(statement) == {"cad_lancamentos", "cad_contratos"}
    assert referenced_tables("INSERT INTO {prefix}saldos SELECT * FROM {prefix}cad_lancamentos WHERE mes = :mes") == {"saldos", "cad_lancamentos"}
    assert referenced_tables(sa.insert(entries).from_select(["id_lancamento"], sa.select(contracts.c.id_contrato))) == {"cad_lancamentos", "cad_contratos"}

    barrier = TableBarrier()
    events: list[str] = []
    landed = threading.Event()

    def load() -> None:
        with barrier.loading("cad_lancamentos"):
            landed.wait()
            events.append("carga")

    loader = threading.Thread(target=load)
    loader.start()
    time.sleep(0.05)

    # A tabela livre passa na hora.
    started = time.perf_counter()
    barrier.wait_for(referenced_tables(sa.select(contracts.c.id_contrato)))
    assert time.perf_counter() - started < 0.05

    # A tabela em voo segura a leitura até a carga terminar.
    reader = threading.Thread(target=lambda: (barrier.wait_for(referenced_tables(statement)), events.append("leitura")))
    reader.start()
    time.sleep(0.05)
    assert events == []
    landed.set()
    loader.join()
    reader.join()
    assert events == ["carga", "leitura"]


def max_key(table: DeltaTable, column: str) -> int:
    """O maior valor de ``column`` na versão carregada.

    O máximo de ``max.<coluna>`` das ações ``add``, sem ler dados; a varredura da coluna quando um
    arquivo não tem a estatística; 0 na tabela vazia. É o início de ``run.next_ids``.
    """
    # get_add_actions devolve uma tabela arro3; pa.table a converte pelo PyCapsule Interface, sem cópia.
    actions = pa.table(table.get_add_actions(flatten=True))
    if actions.num_rows == 0:
        return 0

    name = f"max.{column}"
    if name in actions.column_names and actions.column(name).null_count == 0:
        return pc.max(actions.column(name)).as_py()

    return pc.max(table.to_pyarrow_dataset().to_table(columns=[column]).column(column)).as_py()


# --- Delta ---------------------------------------------------------------------------------------


@pytest.mark.local
def test_several_delta_tables_read_and_ingested_in_parallel(local_location: LocalLocation) -> None:
    """Quatro tabelas Delta lidas pelo delta-rs e ingeridas no DuckDB por ``delta_scan``, em sequência e em quatro threads, cada thread no seu cursor."""
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
    record("parallel.timing.delta_read_4_tables", f"sequencial {one_by_one:.3f} s, quatro threads {in_threads:.3f} s")

    # A ingestão: um cursor por thread, e cada um materializa um mês de uma tabela por delta_scan.
    con = connect_duckdb(["delta"])
    con.execute("SET threads = 2")

    def ingest(prefix: str, k: int) -> None:
        cursor = con.cursor()
        cursor.execute(f"CREATE TABLE {prefix}_{k} AS SELECT * FROM delta_scan('{uris[k]}') WHERE mes = '{MONTHS[0]}'")
        cursor.close()

    started = time.perf_counter()
    for k in range(4):
        ingest("seq", k)
    one_by_one = time.perf_counter() - started
    in_threads = run_in_threads([lambda k=k: ingest("par", k) for k in range(4)])
    counts = con.execute("SELECT (SELECT count(*) FROM par_0), (SELECT count(*) FROM par_1), (SELECT count(*) FROM par_2), (SELECT count(*) FROM par_3)").fetchone()
    assert counts == (ROWS // 2,) * 4
    record("parallel.timing.duckdb_ingest_4_tables_by_delta_scan", f"sequencial {one_by_one:.3f} s, quatro threads {in_threads:.3f} s")
    con.close()


@pytest.mark.local
def test_delta_writes_in_parallel_by_table_and_by_month_and_the_conflict(local_location: LocalLocation) -> None:
    """Escritas em paralelo entram em tabelas distintas e em meses distintos da mesma tabela; no mesmo mês, um dos dois escritores falha com ``CommitFailedError``."""
    con = duckdb.connect(config={"threads": 2})
    data = four_month_table(con)
    con.close()

    # 1. Quatro tabelas em quatro threads: quatro commits independentes, cada log na sua pasta.
    uris = [local_location.child(f"escrita/tabela_{k}") for k in range(4)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda uri: write_deltalake(uri, data, mode="overwrite", partition_by=["mes"]), uris))
    assert [DeltaTable(uri).version() for uri in uris] == [0] * 4

    # 2. A mesma tabela, quatro meses em quatro threads: cada overwrite com predicado troca os arquivos do
    #    seu mês, o delta-rs refaz a tentativa de commit quando a versão avançou, e os quatro entram.
    uri = uris[0]

    def replace_month(month: str) -> None:
        month_rows = data.filter(pc.equal(data.column("mes"), month))
        write_deltalake(uri, month_rows, mode="overwrite", predicate=f"mes = '{month}'")

    elapsed = run_in_threads([lambda month=month: replace_month(month) for month in FOUR_MONTHS])
    table = DeltaTable(uri)
    assert table.version() == 4 and table.to_pyarrow_dataset().count_rows() == data.num_rows
    record("parallel.timing.delta_overwrite_4_months_same_table", f"{elapsed:.3f} s em quatro threads")

    # 3. Dois escritores carregados na mesma versão, o mesmo mês, duas threads: um commit entra, e o
    #    outro é o conflito que a biblioteca converte em ExecutionConflict.
    first, second = DeltaTable(uri), DeltaTable(uri)
    month_rows = data.filter(pc.equal(data.column("mes"), FOUR_MONTHS[0]))
    outcomes: list[str] = []

    def overwrite(writer: DeltaTable) -> None:
        try:
            write_deltalake(writer, month_rows, mode="overwrite", predicate=f"mes = '{FOUR_MONTHS[0]}'")
            outcomes.append("commit")
        except CommitFailedError:
            outcomes.append("CommitFailedError")

    run_in_threads([lambda: overwrite(first), lambda: overwrite(second)])
    assert sorted(outcomes) == ["CommitFailedError", "commit"] and DeltaTable(uri).version() == 5


@pytest.mark.local
def test_id_allocator_starts_after_the_maximum_in_the_file_statistics(local_location: LocalLocation) -> None:
    """O início do alocador vem de ``max.<chave>`` das ações ``add``, sem ler dados; um arquivo registrado sem estatística obriga a varredura; a tabela vazia começa em 1."""
    uri = local_location.child("alocador")
    sample = sample_table()
    write_deltalake(uri, sample, mode="overwrite", partition_by=["mes"])
    table = DeltaTable(uri)
    actions = pa.table(table.get_add_actions(flatten=True))
    assert actions.column("max.id_operacao").null_count == 0 and max_key(table, "id_operacao") == ROWS - 1
    record("parallel.allocator.max_from_statistics", f"{actions.num_rows} arquivos com max.id_operacao; máximo {ROWS - 1}, sem ler dados")

    # Um arquivo registrado por outro programa, só com numRecords: a estatística da coluna fica nula, e a reserva varre a coluna.
    folder = Path(uri) / "mes=2026-03"
    folder.mkdir()
    file = folder / "externo.parquet"
    external = sample.slice(0, 10).drop_columns(["mes"]).set_column(0, "id_operacao", pa.array(range(ROWS, ROWS + 10), pa.int64()))
    pq.write_table(external, file)
    action = AddAction(
        path="mes=2026-03/externo.parquet",
        size=file.stat().st_size,
        partition_values={"mes": "2026-03"},
        modification_time=int(time.time() * 1000),
        data_change=True,
        stats=json.dumps({"numRecords": 10}),
    )
    table.create_write_transaction([action], mode="append", schema=table.schema(), partition_by=["mes"])
    table = DeltaTable(uri)
    assert pa.table(table.get_add_actions(flatten=True)).column("max.id_operacao").null_count == 1
    assert max_key(table, "id_operacao") == ROWS + 9

    # A tabela vazia começa em 1; a faixa é um range contíguo a partir do máximo.
    empty = DeltaTable.create(local_location.child("alocador_vazio"), schema=table.schema(), partition_by=["mes"])
    assert max_key(empty, "id_operacao") == 0
    start = max_key(table, "id_operacao") + 1
    assert list(range(start, start + 3)) == [ROWS + 10, ROWS + 11, ROWS + 12]


# --- DuckDB --------------------------------------------------------------------------------------


@pytest.mark.local
def test_duckdb_loads_and_exports_in_parallel_by_cursor(local_location: LocalLocation) -> None:
    """Quatro cargas Arrow em tabelas distintas e quatro ``COPY ... TO`` em quatro threads, cada uma no seu cursor; o registro da tabela Arrow vale só na conexão que o fez."""
    engine = SandboxEngine(str(local_location.path / "sandbox.duckdb"))
    data = four_month_table(engine.connection)
    by_month = {month: data.filter(pc.equal(data.column("mes"), month)) for month in FOUR_MONTHS}

    elapsed = run_in_threads([lambda k=k: engine.load(f"carga_{k}", by_month[FOUR_MONTHS[k]]) for k in range(4)])
    counts = engine.query("SELECT table_name, estimated_size FROM duckdb_tables() ORDER BY table_name").to_pylist()
    assert [row["table_name"] for row in counts] == [f"carga_{k}" for k in range(4)]
    assert engine.query("SELECT (SELECT count(*) FROM carga_0) + (SELECT count(*) FROM carga_1) + (SELECT count(*) FROM carga_2) + (SELECT count(*) FROM carga_3) AS n").column("n")[0].as_py() == data.num_rows
    record("parallel.timing.duckdb_load_4_tables", f"{elapsed:.3f} s em quatro threads")

    folder = local_location.path / "exportacao"
    folder.mkdir()

    def export(k: int) -> None:
        engine.connection.execute(f"COPY carga_{k} TO '{folder / f'carga_{k}.parquet'}' (FORMAT parquet)")

    elapsed = run_in_threads([lambda k=k: export(k) for k in range(4)])
    assert [pq.read_metadata(folder / f"carga_{k}.parquet").num_rows for k in range(4)] == [by_month[month].num_rows for month in FOUR_MONTHS]
    record("parallel.timing.duckdb_copy_to_4_files", f"{elapsed:.3f} s em quatro threads")

    # O registro de uma tabela Arrow pertence à conexão: outro cursor não a vê.
    engine.connection.register("somente_aqui", data)
    other = engine._root.cursor()
    with pytest.raises(duckdb.CatalogException, match="somente_aqui"):
        other.execute("SELECT count(*) FROM somente_aqui")
    other.close()
    engine.cleanup()
