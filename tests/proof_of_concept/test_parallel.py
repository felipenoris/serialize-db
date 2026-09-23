"""Leitura e escrita em paralelo em cada tecnologia, e as APIs da implementação do paralelismo.

Sem gravar arquivo: a sessão única do motor (uma conexão e um ``RLock``, a tabela temporária que
vale para todas as threads e ``session`` reentrante), a sessão a mais de ``new_session`` (outra
conexão ao mesmo banco, com o seu lock, que vê o que a principal confirmou e não as tabelas
temporárias dela), o pool de ``publish`` que termina o que está em curso e cancela o que não começou
na primeira falha (``shutdown(cancel_futures=True)``), a barreira por tabela com ``Condition`` e as
tabelas de um statement por ``find_tables`` ou pelo sentinela ``{prefix}``. Sob a raiz local
(marcador ``local``): a saída em lotes ``BatchStream`` (uma thread roda a consulta sob o lock e grava
cada lote num arquivo Arrow IPC assim que o DuckDB o entrega, e o cliente lê cada lote gravado
enquanto a consulta continua) e a entrada em lotes ``Loader`` (uma thread grava os lotes num arquivo,
e o ``close`` roda um único ``INSERT``), o pipeline de três estágios que as encadeia; várias tabelas
Delta lidas em paralelo pelo delta-rs e ingeridas em paralelo no DuckDB por ``delta_scan``, uma
sessão a mais por tabela; escritas Delta em paralelo em tabelas distintas e em meses distintos
da mesma tabela, e o conflito de dois escritores no mesmo mês; o início do alocador de
identificadores pelas estatísticas dos arquivos, com a varredura como reserva; cargas Arrow e
exportações ``COPY ... TO`` pedidas por várias threads à sessão única. Os tempos são leituras do
relatório.

O Redshift está em ``test_redshift.py`` (``test_parallel_copy_and_unload_on_two_connections``).
"""

from __future__ import annotations

import decimal
import functools
import json
import queue
import re
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from dataclasses import dataclass, field
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
from poc_delta import MONTHS, ROWS, connect_duckdb, run_in_threads, sample_table

FOUR_MONTHS = ("2026-01", "2026-02", "2026-03", "2026-04")


def four_month_table(con: duckdb.DuckDBPyConnection, rows: int = 400_000) -> pa.Table:
    """``rows`` linhas com ``mes`` em quatro valores, o inteiro, um decimal e um texto, geradas pelo DuckDB."""
    sql = (
        "SELECT range AS id, '2026-0' || (1 + range % 4) AS mes, CAST(((range * 7) % 1000) / 100.0 AS DECIMAL(18, 2)) AS valor, "
        f"'x' || range AS descricao FROM range({rows})"
    )
    return con.execute(sql).to_arrow_table()


# --- As APIs da implementação -------------------------------------------------------------------


class SandboxEngine:
    """O esboço do motor DuckDB com uma sessão por execução: uma conexão e um ``RLock``.

    Todo comando toma o lock pelo tempo do comando e o solta ao terminar, e quem segura o lock nunca
    espera pelo código do cliente: é o que deixa várias threads usarem uma conexão que o ``duckdb``
    declara ``threadsafety`` 1. Uma tabela temporária criada por um comando vale para os
    seguintes, de qualquer thread. ``session`` dá a conexão crua com o lock tomado pelo bloco; o
    ``RLock`` deixa uma primitiva ser chamada dentro do bloco, na mesma thread, sem travar.
    ``new_session`` abre uma sessão a mais sobre o mesmo banco, com a sua conexão e o seu lock, para o
    que roda em paralelo: ela vê o que esta sessão confirmou, não as tabelas temporárias dela.
    ``folder`` é a pasta de transbordo de ``BatchStream`` e de ``Loader``.
    """

    def __init__(self, database: str = ":memory:", folder: str | None = None, connection: duckdb.DuckDBPyConnection | None = None) -> None:
        # Uma sessão nova abre o banco; a de new_session recebe a conexão duplicada da principal.
        self._connection = connection if connection is not None else duckdb.connect(database, config={"threads": 2})
        self._lock = threading.RLock()
        self._owner: int | None = None  # a thread dentro de session, que BatchStream consulta
        self.folder = folder

    @contextmanager
    def session(self) -> Iterator[duckdb.DuckDBPyConnection]:
        with self._lock:
            outer_owner = self._owner
            self._owner = threading.get_ident()
            try:
                yield self._connection
            finally:
                self._owner = outer_owner

    def holds_session(self) -> bool:
        """Verdadeiro na thread que está dentro de ``session``: ali, outra thread que pedisse a sessão esperaria o bloco."""
        return self._owner == threading.get_ident()

    def new_session(self) -> SandboxEngine:
        """Uma sessão a mais sobre o mesmo banco: ``cursor()`` é outra conexão, com as suas tabelas temporárias e a sua transação."""
        with self.session() as connection:
            return SandboxEngine(folder=self.folder, connection=connection.cursor())

    def query(self, sql: str) -> pa.Table:
        with self.session() as connection:
            return connection.execute(sql).to_arrow_table()

    def load(self, name: str, data: pa.Table) -> None:
        # O registro da tabela Arrow e o CREATE correm no mesmo bloco: outra thread nunca vê o nome registrado.
        with self.session() as connection:
            connection.register("data_in", data)
            connection.execute(f"CREATE TABLE {name} AS SELECT * FROM data_in")
            connection.unregister("data_in")

    def spool_path(self, kind: str) -> str:
        """Um caminho novo na pasta de transbordo, para o arquivo de um stream ou de um loader."""
        if self.folder is None:
            raise ValueError("o motor sem pasta de transbordo não abre stream nem loader")
        return str(Path(self.folder) / f"{kind}_{uuid.uuid4().hex}.arrow")

    def cleanup(self) -> None:
        with self._lock:
            self._connection.close()

    def __enter__(self) -> SandboxEngine:
        return self

    def __exit__(self, *exc: object) -> None:
        self.cleanup()


def test_sandbox_serializes_threads_on_one_session() -> None:
    """Quatro threads carregam e consultam pelo mesmo motor, uma de cada vez na sessão: todas veem as tabelas das outras, a tabela temporária de uma vale para as demais, e uma primitiva chamada dentro de ``session`` não trava."""
    engine = SandboxEngine()
    counts: dict[int, int] = {}

    def work(k: int) -> None:
        engine.load(f"t{k}", pa.table({"x": pa.array(range(1000 * (k + 1)), pa.int64())}))
        counts[k] = engine.query(f"SELECT count(*) AS n FROM t{k}").column("n")[0].as_py()

    run_in_threads([functools.partial(work, k) for k in range(4)])
    assert counts == {0: 1000, 1: 2000, 2: 3000, 3: 4000}
    assert engine.query("SELECT list(table_name ORDER BY table_name) AS t FROM duckdb_tables() WHERE NOT temporary").column("t")[0].as_py() == ["t0", "t1", "t2", "t3"]

    # A tabela temporária é da sessão, e a sessão é uma só: outra thread a lê.
    engine.query("CREATE TEMP TABLE temporaria AS SELECT range AS id FROM range(10)")
    seen: dict[str, int] = {}

    def read_temporary() -> None:
        seen["outra_thread"] = engine.query("SELECT count(*) AS n FROM temporaria").column("n")[0].as_py()

    run_in_threads([read_temporary])
    assert seen == {"outra_thread": 10}

    # O bloco session segura a sessão; uma primitiva chamada nele, na mesma thread, entra no RLock.
    with engine.session() as connection:
        connection.execute("CREATE TABLE dentro_do_bloco AS SELECT 1 AS x")
        assert engine.query("SELECT count(*) AS n FROM dentro_do_bloco").column("n")[0].as_py() == 1
        assert engine.holds_session()
    assert not engine.holds_session()

    engine.cleanup()
    with pytest.raises(duckdb.ConnectionException, match="Connection already closed"):
        engine.query("SELECT 1")


def test_new_session_runs_beside_the_main_one() -> None:
    """``new_session`` abre outra conexão ao mesmo banco, com o seu lock: ela vê as tabelas que a sessão principal confirmou e não as temporárias dela, roda enquanto a principal está tomada, e a principal vê o que ela confirma."""
    engine = SandboxEngine()
    engine.query("CREATE TABLE confirmada AS SELECT range AS id FROM range(10)")
    engine.query("CREATE TEMP TABLE temporaria AS SELECT range AS id FROM range(10)")
    with engine.new_session() as other:
        assert other.query("SELECT count(*) AS n FROM confirmada").column("n")[0].as_py() == 10
        with pytest.raises(duckdb.CatalogException, match="temporaria"):
            other.query("SELECT count(*) FROM temporaria")

        # A sessão principal tomada por um bloco não segura a outra, pedida por outra thread.
        seen: dict[str, int] = {}

        def count_on_the_other_session() -> None:
            seen["n"] = other.query("SELECT count(*) AS n FROM confirmada").column("n")[0].as_py()

        with engine.session():
            run_in_threads([count_on_the_other_session])
        assert seen == {"n": 10}

        # O que a outra sessão confirma, a principal vê no comando seguinte.
        other.query("CREATE TABLE da_outra AS SELECT 1 AS x")
        assert engine.query("SELECT count(*) AS n FROM da_outra").column("n")[0].as_py() == 1
    engine.cleanup()


END = object()

# O arquivo de transbordo: Arrow IPC em formato de fluxo, com LZ4. Em 20.000.000 de linhas de três
# colunas, 162 MB contra 478 MB sem compressão, por 0,09 s a mais na escrita (2026-09-22, POC.md).
SPOOL_OPTIONS = pa.ipc.IpcWriteOptions(compression="lz4")


@dataclass
class SpoolProgress:
    """O que a thread de ``BatchStream`` conta ao cliente: os lotes já gravados no arquivo, o fim da consulta e o erro dela."""

    condition: threading.Condition = field(default_factory=threading.Condition)
    written: int = 0
    done: bool = False
    error: BaseException | None = None


def take(source: queue.Queue[object], stop: threading.Event) -> object | None:
    """O próximo item da fila, esperando em fatias de 50 ms; ``None`` quando ``stop`` chega com a fila vazia.

    A espera com prazo deixa quem lê perceber o ``stop``, em vez de ficar preso num ``get`` sem fim;
    nenhum item da fila é ``None``.
    """
    while True:
        try:
            return source.get(timeout=0.05)
        except queue.Empty:
            pass
        if stop.is_set():
            return None



def produce(engine: SandboxEngine, sql: str, batch_size: int, path: str, stop: threading.Event, progress: SpoolProgress) -> None:
    """Roda a consulta na sessão, sob o lock, e grava cada lote no arquivo de transbordo assim que o DuckDB o entrega.

    Nunca espera pelo cliente: o lock sai quando o resultado acaba ou quando ``stop`` chega, com o
    cliente ainda lendo o arquivo. Recebe só o que usa, nunca o ``BatchStream``: um stream que o
    cliente abandona é coletado, o ``__del__`` liga o ``stop``, e a consulta para no lote seguinte.
    """
    try:
        with engine.session() as connection:
            reader = connection.execute(sql).to_arrow_reader(batch_size)
            # O esquema entra no arquivo com o primeiro lote, ou no fechamento de um resultado vazio.
            with pa.OSFile(path, "wb") as sink, pa.ipc.new_stream(sink, reader.schema, options=SPOOL_OPTIONS) as writer:
                for batch in reader:
                    if stop.is_set():
                        return
                    writer.write_batch(batch)
                    with progress.condition:
                        progress.written += 1
                        progress.condition.notify_all()
    except Exception as error:  # noqa: BLE001 - o erro da consulta vai ao cliente
        progress.error = error
    finally:
        with progress.condition:
            progress.done = True
            progress.condition.notify_all()


class BatchStream:
    """O esboço da saída em lotes do motor (``stream`` da etapa 4) numa sessão única.

    Uma thread roda a consulta na sessão, sob o lock, e grava cada lote num arquivo de transbordo
    assim que o DuckDB o entrega; o cliente lê cada lote gravado, na sua thread, enquanto a consulta
    continua. O primeiro lote chega antes de a consulta terminar, e a consulta termina sem esperar
    pelo cliente: o lock sai com o resultado inteiro no arquivo, e o leitor do DuckDB, que o comando
    seguinte na mesma conexão esvaziaria (``test_duckdb.py``), já foi consumido. Por isso nada trava:
    outro comando, de qualquer thread, espera só a consulta. Dentro de ``session``, na mesma thread,
    a consulta roda na thread do cliente, porque a outra esperaria o bloco, e o bloco o stream. O erro
    que a consulta encontra antes do primeiro lote aparece na construção; o que ela encontra depois,
    na leitura seguinte ao último lote gravado, e em cada leitura depois dela. ``close`` para a
    consulta no lote seguinte e apaga o arquivo. ``__arrow_c_stream__`` entrega os lotes a
    ``write_deltalake`` e a ``RecordBatchReader.from_stream``; não ao ``register`` do DuckDB, cujo
    ``arrow_scan`` puxa o fluxo numa thread de leitura antecipada do Arrow que continua chamando
    Python depois de o comando terminar (``test_duckdb.py``); para levar lotes ao sandbox existe
    ``Loader``.
    """

    def __init__(self, engine: SandboxEngine, sql: str, batch_size: int = 100_000) -> None:
        # O stop e o caminho vêm antes de tudo: o __del__ de uma construção que falhou os usa.
        self._stop = threading.Event()
        self._path = engine.spool_path("stream")
        self._progress = SpoolProgress()
        self._source: pa.OSFile | None = None
        self._read = 0
        self._thread: threading.Thread | None = None
        arguments = (engine, sql, batch_size, self._path, self._stop, self._progress)
        if engine.holds_session():
            produce(*arguments)
        else:
            self._thread = threading.Thread(target=produce, args=arguments)
            self._thread.start()

        # A construção espera o primeiro lote ou o fim: o erro de uma consulta que nada entregou chega aqui.
        if not self._wait_for_batch() and self._progress.error is not None:
            self.close()
            raise self._progress.error
        self._source = pa.OSFile(self._path, "rb")
        self._reader = pa.ipc.open_stream(self._source)
        self.schema = self._reader.schema

    def _wait_for_batch(self) -> bool:
        """Espera um lote gravado e ainda não lido, ou o fim da consulta; ``True`` quando há lote.

        A espera tem prazo e confere o ``stop``: quem puxa o stream pode ser a thread de leitura
        antecipada de um leitor nativo, e ela não fica presa aqui depois de um ``close``.
        """
        with self._progress.condition:
            while self._progress.written <= self._read and not self._progress.done:
                if self._stop.is_set():
                    return False
                self._progress.condition.wait(timeout=0.05)
            return self._progress.written > self._read

    def read_next_batch(self) -> pa.RecordBatch:
        if not self._wait_for_batch():
            if self._progress.error is not None:
                raise self._progress.error
            raise StopIteration
        batch = self._reader.read_next_batch()
        self._read += 1
        return batch

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
            self._thread.join(timeout=10)
        if self._source is not None:
            self._source.close()
        Path(self._path).unlink(missing_ok=True)

    def __enter__(self) -> BatchStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __del__(self) -> None:
        # O stream abandonado: a consulta para no lote seguinte, e o arquivo sai.
        self._stop.set()
        Path(self._path).unlink(missing_ok=True)


def write_until_end(writer: pa.ipc.RecordBatchStreamWriter, source: queue.Queue[object], closed: threading.Event, outcome: dict[str, object]) -> None:
    """Grava cada lote tirado da fila até o fim dela; a exceção que o cliente pôs na fila, e o loader abandonado, sobem daqui."""
    item = take(source, closed)
    while item is not END:
        if item is None:
            raise RuntimeError("loader encerrado sem close")
        if isinstance(item, BaseException):
            raise item
        writer.write_batch(item)
        outcome["rows"] += item.num_rows
        item = take(source, closed)


def write_spool(path: str, schema: pa.Schema, source: queue.Queue[object], closed: threading.Event, outcome: dict[str, object]) -> None:
    """A thread de ``Loader``: grava no arquivo de transbordo os lotes tirados da fila, até o fim da fila.

    Não usa a sessão: o ``close`` carrega o arquivo depois, num comando só. Um ``closed`` sem o fim da
    fila é um loader abandonado, e a thread termina sem completar o arquivo.
    """
    try:
        with pa.OSFile(path, "wb") as sink, pa.ipc.new_stream(sink, schema, options=SPOOL_OPTIONS) as writer:
            write_until_end(writer, source, closed, outcome)
    except BaseException as error:  # noqa: BLE001 - relançado em close, ou em write quando a thread já terminou
        outcome["error"] = error


class Loader:
    """O esboço da entrada em lotes do motor (``loader`` da etapa 4) numa sessão única.

    O cliente empurra lotes numa fila limitada, e uma thread auxiliar os grava num arquivo de
    transbordo, sem a sessão, enquanto o cliente prepara o lote seguinte. ``write`` faz o cast do lote
    na thread do cliente, para o erro aparecer com o lote em mãos, e bloqueia quando a fila está
    cheia. ``close`` espera o arquivo e roda, sob o lock, um único ``INSERT ... BY NAME`` sobre o
    leitor do arquivo: um comando atômico, e nada é visível antes dele. Uma exceção dentro do
    ``with``, um lote recusado pelo cast ou um loader abandonado apagam o arquivo sem ``INSERT``, e a
    tabela fica como estava. O leitor que o ``INSERT`` consome é o do arquivo, nativo: nenhum gerador
    Python chega ao ``arrow_scan``, e o comando único não traz a leitura antecipada que foge da fila
    (``test_duckdb.py``).
    """

    def __init__(self, engine: SandboxEngine, table: str, schema: pa.Schema, queue_depth: int = 2) -> None:
        self._engine = engine
        self._table = table
        self._schema = schema
        self._path = engine.spool_path("loader")
        self._queue: queue.Queue[object] = queue.Queue(maxsize=queue_depth)
        self._closed = threading.Event()
        self._outcome: dict[str, object] = {"rows": 0, "error": None}
        self._thread = threading.Thread(target=write_spool, args=(self._path, schema, self._queue, self._closed, self._outcome))
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

    def _insert(self) -> None:
        """O arquivo inteiro num ``INSERT ... BY NAME``, sob o lock; o nome registrado é único e sai no mesmo bloco."""
        name = f"lote_{uuid.uuid4().hex[:8]}"
        with pa.OSFile(self._path, "rb") as source, self._engine.session() as connection:
            connection.register(name, pa.ipc.open_stream(source))
            try:
                connection.execute(f"INSERT INTO {self._table} BY NAME SELECT * FROM {name}")
            finally:
                connection.unregister(name)

    def close(self, error: BaseException | None = None) -> None:
        self._put(error if error is not None else END)
        self._closed.set()
        self._thread.join()
        try:
            if error is None and self.error is None:
                self._insert()
        finally:
            Path(self._path).unlink(missing_ok=True)
        if error is None and self.error is not None:
            raise self.error

    def __enter__(self) -> Loader:
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        self.close(error=exc)

    def __del__(self) -> None:
        self._closed.set()


@pytest.mark.local
def test_batch_stream_writes_each_batch_while_the_query_runs(local_location: LocalLocation) -> None:
    """``BatchStream`` entrega o primeiro lote enquanto a consulta roda e os lotes na ordem da consulta; outro comando espera só a consulta, um stream dentro de ``session`` não trava, a tabela temporária serve ao stream, o ``close`` antecipado e o abandono param a consulta, e o erro da consulta chega ao cliente."""
    engine = SandboxEngine(folder=local_location.child("transbordo_stream"))
    Path(engine.folder).mkdir()
    engine.query("CREATE TABLE numeros AS SELECT range AS id, 'x' || range AS s FROM range(3_000_000)")

    # O primeiro lote chega com a thread ainda gravando; um comando no meio da leitura espera só a
    # consulta, nunca o cliente. Os tempos são leituras.
    started = time.perf_counter()
    first_batch: float | None = None
    query_running = False
    last_id = -1
    seen = 0
    with BatchStream(engine, "SELECT id, s FROM numeros WHERE id % 3 <> 0", batch_size=100_000) as stream:
        for batch in stream:
            if first_batch is None:
                first_batch = time.perf_counter() - started
                query_running = stream._thread.is_alive()
            assert batch.column("id")[0].as_py() > last_id
            last_id = batch.column("id")[-1].as_py()
            engine.query("SELECT count(*) FROM numeros")
            seen += batch.num_rows
    total = time.perf_counter() - started
    assert seen == 2_000_000 and not stream._thread.is_alive()
    assert list(Path(engine.folder).iterdir()) == []  # o close apagou o arquivo
    record("parallel.stream.first_batch", f"{first_batch:.3f} s de {total:.3f} s, com a consulta ainda rodando: {query_running}")

    # A tabela temporária da sessão serve ao stream, como no Redshift; dentro de session, na mesma
    # thread, a consulta roda na thread do cliente, sem a outra.
    engine.query("CREATE TEMP TABLE pares AS SELECT id FROM numeros WHERE id % 2 = 0")
    with engine.session(), BatchStream(engine, "SELECT count(*) AS n FROM pares") as inside:
        assert inside._thread is None
        assert inside.read_all().column("n").to_pylist() == [1_500_000]

    # O close antecipado para a consulta no lote seguinte e apaga o arquivo.
    stream = BatchStream(engine, "SELECT id FROM numeros", batch_size=1000)
    stream.read_next_batch()
    stream.close()
    assert not stream._thread.is_alive() and list(Path(engine.folder).iterdir()) == []
    record("parallel.stream.batches_written_before_close", f"{stream._progress.written} de 3000")

    # __arrow_c_stream__: o stream é um leitor para quem o pede pelo PyCapsule, como write_deltalake.
    with BatchStream(engine, "SELECT id FROM numeros WHERE id < 5000", batch_size=1000) as source:
        assert pa.RecordBatchReader.from_stream(source).read_all().num_rows == 5000

    # Um stream abandonado sem close: a thread não referencia o objeto, o __del__ liga o stop, e ela termina.
    abandoned = BatchStream(engine, "SELECT id FROM numeros", batch_size=1000)
    abandoned.read_next_batch()
    thread = abandoned._thread
    del abandoned
    thread.join(timeout=5)
    assert not thread.is_alive() and list(Path(engine.folder).iterdir()) == []

    # O erro da consulta chega ao cliente na construção, quando nenhum lote saiu antes dele, ou na
    # leitura seguinte ao último lote gravado: duckdb.Error no execute, ou OSError com a mensagem do
    # DuckDB quando o leitor Arrow o encontra.
    failing_sql = "SELECT CAST(CASE WHEN id = 2_900_000 THEN 'x' ELSE CAST(id AS VARCHAR) END AS INTEGER) AS n FROM numeros"
    delivered = 0
    with pytest.raises((duckdb.ConversionException, OSError), match="Could not convert string 'x' to INT32") as failure:
        with BatchStream(engine, failing_sql, batch_size=100_000) as failing:
            for batch in failing:
                delivered += 1
    record("parallel.stream.query_error", f"{type(failure.value).__name__} depois de {delivered} lotes")
    assert list(Path(engine.folder).iterdir()) == []  # o arquivo parcial saiu
    engine.cleanup()


@pytest.mark.local
def test_loader_spools_and_inserts_in_one_statement_on_close(local_location: LocalLocation) -> None:
    """``Loader`` grava os lotes num arquivo enquanto o cliente prepara o lote seguinte e os insere num comando no ``close``: nada é visível antes dele, uma exceção do cliente ou um lote fora do contrato não insere nada, e o erro do ``INSERT`` chega ao cliente."""
    engine = SandboxEngine(folder=local_location.child("transbordo_loader"))
    Path(engine.folder).mkdir()
    schema = pa.schema([("id", pa.int64()), ("valor", pa.decimal128(18, 2))])
    engine.query("CREATE TABLE destino (id BIGINT, valor DECIMAL(18, 2))")

    def batch(k: int, rows: int = 10_000) -> pa.RecordBatch:
        return pa.RecordBatch.from_pydict({"id": pa.array(range(k * rows, (k + 1) * rows), pa.int64()), "valor": pa.array([decimal.Decimal(k) / 100] * rows, pa.decimal128(18, 2))})

    def count(table: str = "destino") -> int:
        return engine.query(f"SELECT count(*) AS n FROM {table}").column("n")[0].as_py()

    visible: list[int] = []
    with Loader(engine, "destino", schema) as loader:
        for k in range(20):
            loader.write(batch(k))
            visible.append(count())  # a sessão está livre enquanto o loader grava o arquivo
    assert loader.rows == 200_000 and count() == 200_000 and set(visible) == {0}
    assert list(Path(engine.folder).iterdir()) == []

    # Uma exceção dentro do with: nada é inserido, e a exceção que sobe é a do cliente.
    with pytest.raises(ValueError, match="falha do cliente"):
        with Loader(engine, "destino", schema) as loader:
            loader.write(batch(0))
            raise ValueError("falha do cliente")
    assert isinstance(loader.error, ValueError) and count() == 200_000

    # Um loader abandonado sem close: a thread termina, e nada é inserido.
    abandoned = Loader(engine, "destino", schema)
    abandoned.write(batch(0))
    thread = abandoned._thread
    del abandoned
    thread.join(timeout=2)
    assert not thread.is_alive() and count() == 200_000

    # Um lote fora do contrato é recusado no write, na thread do cliente, e nada entra.
    with pytest.raises(pa.ArrowInvalid, match="Rescaling"):
        with Loader(engine, "destino", schema) as loader:
            loader.write(pa.RecordBatch.from_pydict({"id": pa.array([1], pa.int64()), "valor": pa.array([decimal.Decimal("1.2345")], pa.decimal128(20, 4))}))
    assert count() == 200_000

    # O erro do INSERT chega ao cliente no close, e o comando único não deixa linha alguma.
    engine.query("CREATE TABLE estreita (id INTEGER, valor DECIMAL(18, 2))")
    with pytest.raises(duckdb.ConversionException), Loader(engine, "estreita", schema) as loader:
        for k in range(20):
            loader.write(batch(k).set_column(0, "id", pa.array([2**40] * 10_000, pa.int64())))
    assert count("estreita") == 0
    engine.cleanup()


@pytest.mark.local
def test_three_stage_pipeline_overlaps_read_work_and_write(local_location: LocalLocation) -> None:
    """Leitura por ``BatchStream``, trabalho do cliente por lote e escrita por ``Loader`` numa sessão única, sobre um banco em arquivo, produzem as mesmas linhas que a versão por lote sem threads e que a versão por ``pa.Table``; os tempos são leituras do relatório."""
    folder = Path(local_location.child("pipeline"))
    folder.mkdir()
    engine = SandboxEngine(str(folder / "sandbox.duckdb"), folder=str(folder))
    engine.query("CREATE TABLE fonte AS SELECT range AS id, CAST(((range * 7) % 1000) / 100.0 AS DECIMAL(18, 2)) AS valor, 'x' || range AS s FROM range(3_000_000)")
    target = pa.schema([("id", pa.int64()), ("valor", pa.decimal128(18, 2)), ("s", pa.string()), ("dobro", pa.float64())])
    for name in ("sequencial", "encadeado"):
        engine.query(f"CREATE TABLE {name} (id BIGINT, valor DECIMAL(18, 2), s VARCHAR, dobro DOUBLE)")
    sql = "SELECT id, valor, s FROM fonte"

    def work(data: pa.RecordBatch | pa.Table) -> pa.RecordBatch | pa.Table:
        frame = data.to_pandas(types_mapper=pd.ArrowDtype)
        frame["dobro"] = (frame["valor"] * 2).astype("double[pyarrow]")
        return type(data).from_pandas(frame, preserve_index=False).cast(target)

    def insert(name: str, data: pa.RecordBatch) -> None:
        with engine.session() as connection:
            connection.register("lote", data)
            connection.execute(f"INSERT INTO {name} BY NAME SELECT * FROM lote")
            connection.unregister("lote")

    timings: dict[str, float] = {}

    # A tabela inteira: to_arrow_table, o trabalho de uma vez, a carga de uma vez.
    started = time.perf_counter()
    engine.load("por_tabela", work(engine.query(sql)))
    timings["por_tabela"] = time.perf_counter() - started

    # Por lote, sem threads: dentro de session, o stream roda a consulta inteira na thread do
    # cliente, e o trabalho e um INSERT por lote entram no mesmo bloco.
    started = time.perf_counter()
    with engine.session(), BatchStream(engine, sql, batch_size=200_000) as stream:
        for batch in stream:
            insert("sequencial", work(batch))
    timings["sequencial"] = time.perf_counter() - started

    # Encadeado: a consulta e o arquivo na thread do stream, o trabalho na thread do cliente, a
    # escrita do arquivo na thread do loader, e o INSERT único no close.
    started = time.perf_counter()
    with BatchStream(engine, sql, batch_size=200_000) as stream, Loader(engine, "encadeado", target) as loader:
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


def outcome_of(future: Future) -> str:
    """O resultado de uma tarefa terminada ou cancelada, como ``PublishFailed`` o lista."""
    if future.cancelled():
        return "cancelada"
    error = future.exception()
    if error is None:
        return "concluída"
    return f"falhou: {type(error).__name__}: {error}"


def first_failure(futures: Iterable[Future]) -> Future | None:
    """A primeira tarefa que falha, na ordem em que as tarefas terminam; ``None`` quando todas concluem."""
    for future in as_completed(futures):
        if future.exception() is not None:
            return future
    return None


def publish_all(tables: list[str], action: Callable[[str], object], max_workers: int) -> dict[str, str]:
    """Roda ``action`` por tabela num pool.

    Na primeira falha nada novo começa, o que está em curso termina e entra no resultado, o que não
    começou é cancelado, e ``PublishFailed`` traz o resultado de cada tabela: os commits feitos ficam,
    porque o Delta não tem transação entre tabelas.
    """
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(action, table): table for table in tables}
        failed = first_failure(futures)
        # shutdown espera as tarefas em curso e cancela as que ainda não começaram.
        pool.shutdown(wait=True, cancel_futures=True)

    outcomes = {}
    for future, table in futures.items():
        outcomes[table] = outcome_of(future)
    if failed is not None:
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
            assert landed.wait(timeout=10)
            events.append("carga")

    loader = threading.Thread(target=load)
    loader.start()
    time.sleep(0.05)

    # A tabela livre passa na hora.
    started = time.perf_counter()
    barrier.wait_for(referenced_tables(sa.select(contracts.c.id_contrato)))
    assert time.perf_counter() - started < 0.05

    # A tabela em voo segura a leitura até a carga terminar.
    def read() -> None:
        barrier.wait_for(referenced_tables(statement))
        events.append("leitura")

    reader = threading.Thread(target=read)
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
    """Quatro tabelas Delta lidas pelo delta-rs e ingeridas no DuckDB por ``delta_scan``, em sequência na sessão principal e em quatro threads, uma sessão a mais por tabela."""
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

    # A ingestão: cada tabela materializa um mês por delta_scan, em série na sessão principal ou numa
    # sessão a mais por thread; a tabela confirmada pela outra sessão é vista pela principal.
    engine = SandboxEngine(connection=connect_duckdb(["delta"]))
    engine.query("SET threads = 2")

    def ingest(session: SandboxEngine, prefix: str, k: int) -> None:
        session.query(f"CREATE TABLE {prefix}_{k} AS SELECT * FROM delta_scan('{uris[k]}') WHERE mes = '{MONTHS[0]}'")

    def ingest_in_own_session(k: int) -> None:
        with engine.new_session() as session:
            ingest(session, "par", k)

    started = time.perf_counter()
    for k in range(4):
        ingest(engine, "seq", k)
    one_by_one = time.perf_counter() - started
    in_threads = run_in_threads([functools.partial(ingest_in_own_session, k) for k in range(4)])
    counts = engine.query("SELECT (SELECT count(*) FROM par_0) AS a, (SELECT count(*) FROM par_1) AS b, (SELECT count(*) FROM par_2) AS c, (SELECT count(*) FROM par_3) AS d").to_pylist()[0]
    assert list(counts.values()) == [ROWS // 2] * 4
    record("parallel.timing.duckdb_ingest_4_tables_by_delta_scan", f"sequencial {one_by_one:.3f} s, quatro sessões {in_threads:.3f} s")
    engine.cleanup()


@pytest.mark.local
def test_delta_writes_in_parallel_by_table_and_by_month_and_the_conflict(local_location: LocalLocation) -> None:
    """Escritas em paralelo entram em tabelas distintas e em meses distintos da mesma tabela; no mesmo mês, um dos dois escritores falha com ``CommitFailedError``."""
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

    # 2. A mesma tabela, quatro meses em quatro threads: cada overwrite com predicado troca os arquivos do
    #    seu mês, o delta-rs refaz a tentativa de commit quando a versão avançou, e os quatro entram.
    uri = uris[0]

    def replace_month(month: str) -> None:
        month_rows = data.filter(pc.equal(data.column("mes"), month))
        write_deltalake(uri, month_rows, mode="overwrite", predicate=f"mes = '{month}'")

    elapsed = run_in_threads([functools.partial(replace_month, month) for month in FOUR_MONTHS])
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

    run_in_threads([functools.partial(overwrite, first), functools.partial(overwrite, second)])
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
def test_duckdb_loads_and_exports_from_threads_on_one_session(local_location: LocalLocation) -> None:
    """Quatro cargas Arrow em tabelas distintas e quatro ``COPY ... TO`` pedidos por quatro threads, em série na sessão única; o registro da tabela Arrow vale só na conexão que o fez."""
    engine = SandboxEngine(str(local_location.path / "sandbox.duckdb"))
    with engine.session() as connection:
        data = four_month_table(connection)
    by_month = {month: data.filter(pc.equal(data.column("mes"), month)) for month in FOUR_MONTHS}

    def load(k: int) -> None:
        engine.load(f"carga_{k}", by_month[FOUR_MONTHS[k]])

    elapsed = run_in_threads([functools.partial(load, k) for k in range(4)])
    counts = engine.query("SELECT table_name, estimated_size FROM duckdb_tables() ORDER BY table_name").to_pylist()
    assert [row["table_name"] for row in counts] == [f"carga_{k}" for k in range(4)]
    assert engine.query("SELECT (SELECT count(*) FROM carga_0) + (SELECT count(*) FROM carga_1) + (SELECT count(*) FROM carga_2) + (SELECT count(*) FROM carga_3) AS n").column("n")[0].as_py() == data.num_rows
    record("parallel.timing.duckdb_load_4_tables", f"{elapsed:.3f} s em quatro threads, em série na sessão")

    folder = local_location.path / "exportacao"
    folder.mkdir()

    def export(k: int) -> None:
        with engine.session() as connection:
            connection.execute(f"COPY carga_{k} TO '{folder / f'carga_{k}.parquet'}' (FORMAT parquet)")

    elapsed = run_in_threads([functools.partial(export, k) for k in range(4)])
    assert [pq.read_metadata(folder / f"carga_{k}.parquet").num_rows for k in range(4)] == [by_month[month].num_rows for month in FOUR_MONTHS]
    record("parallel.timing.duckdb_copy_to_4_files", f"{elapsed:.3f} s em quatro threads, em série na sessão")

    # O registro de uma tabela Arrow pertence à conexão: um cursor dela, que é outra conexão, não a vê.
    with engine.session() as connection:
        connection.register("somente_aqui", data)
        other = connection.cursor()
        with pytest.raises(duckdb.CatalogException, match="somente_aqui"):
            other.execute("SELECT count(*) FROM somente_aqui")
        other.close()
        connection.unregister("somente_aqui")
    engine.cleanup()
