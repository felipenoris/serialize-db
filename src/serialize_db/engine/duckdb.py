"""O motor DuckDB: o sandbox da execução num banco em arquivo, com uma sessão sob um lock.

O banco nasce em ``<temp_directory>/<execution_id>.duckdb``, e ``cleanup`` o apaga com a pasta de
transbordo. O motor guarda uma conexão, a sessão da execução, e um ``threading.RLock`` que toda
primitiva toma pelo tempo do seu comando: uma tabela temporária que o pipeline crie vale para os
comandos seguintes, de qualquer thread, e nenhuma primitiva espera pelo código do cliente com o
lock tomado. ``session()`` dá a conexão crua ao bloco, com o lock tomado e reentrante na mesma
thread; ``new_session()`` abre um motor sobre ``cursor()`` da conexão, uma sessão a mais sobre o
mesmo banco, com o seu lock. Os limites da instância saem do ambiente na abertura, quando a
configuração os omite (``environment_limits``): ``threads`` são as CPUs que o processo pode usar, e
``memory_limit`` é metade da memória que ele ainda pode usar.

As primitivas:

- ``ingest`` cria uma view (ou tabela, com ``materialize=True``) com o nome do modelo sobre
  ``delta_scan(uri, version := v)``, e ``published`` devolve a versão fixada como origem de
  consulta, sem ocupar nome no sandbox;
- ``stream`` roda a consulta numa thread auxiliar, sob o lock, e entrega cada lote à memória
  enquanto os lotes guardados cabem em 64 MiB, e a um arquivo Arrow IPC com LZ4 na pasta de
  transbordo o lote que não cabe e os seguintes; ``query`` devolve a ``pa.Table``;
- ``loader`` confere o nome na abertura, grava os lotes num arquivo numa thread auxiliar e, no
  ``close``, cria a tabela e a carrega num único ``INSERT ... BY NAME``, numa transação; ``load`` é
  a forma por tabela;
- ``audit`` roda as verificações de ``serialize_db.audit`` e monta o ``AuditReport``;
- ``export_partition`` leva uma partição do sandbox ao Delta, por ``register_files`` do arquivo do
  ``COPY ... RETURN_STATS`` ou por ``publish_partition`` do leitor.

Um statement Core é compilado pela cópia prefixada de ``sql.prefixed``, com todo nome entre aspas,
pelo dialeto do DuckDB no estilo ``qmark``, com os valores do cliente dados por ``params`` e a lista
posicional na ordem do compilador; um texto pronto passa por ``sql.bind``.

Exemplo:

.. code-block:: python

    from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine

    with DuckDBEngine(DuckDBConfig(), "exec-2026-09-05", storage) as engine:
        engine.ingest(Lancamento.__table__, uri, 143, partitions=["2026-07-31", "2026-08-31"])
        engine.query(sa.select(sa.func.count()).select_from(Lancamento.__table__))
"""

from __future__ import annotations

import collections
import contextlib
import dataclasses
import itertools
import logging
import os
import queue
import shutil
import tempfile
import threading
import uuid
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence
from pathlib import Path

import duckdb
import duckdb_engine
import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.util import find_tables
from sqlalchemy.sql.visitors import iterate

from serialize_db import audit, delta, sql
from serialize_db.audit import AuditReport, CheckResult, KeyScope
from serialize_db.engine import ExportMode
from serialize_db.errors import ContractError, RegistrationRefused, SandboxError, SqlError
from serialize_db.resources import available_cpus, available_memory
from serialize_db.schema import (
    cast,
    check_partition_value,
    ddl,
    double_columns,
    literal,
    quoted,
    sequential_key,
    sql_type,
    table_options,
)
from serialize_db.storage import Storage

__all__ = ["DuckDBConfig", "DuckDBEngine", "environment_limits"]

log = logging.getLogger("serialize_db.engine.duckdb")

# O compilador dos statements Core: o estilo qmark é o do driver do DuckDB, e a lista posicional sai
# na ordem de positiontup, sem reescrever marcador algum.
_QMARK = duckdb_engine.Dialect(paramstyle="qmark")

# O arquivo de transbordo: Arrow IPC em formato de fluxo, com LZ4, um terço do tamanho sem
# compressão (2026-09-22).
_SPOOL_OPTIONS = pa.ipc.IpcWriteOptions(compression="lz4")

# A fração da memória disponível que vai para o memory_limit. A documentação do DuckDB pede de 50%
# a 60% da memória quando o sistema mata o processo, porque parte das alocações foge do limite: no
# COPY ordenado, o RSS do processo passou do limite em 13% a 21% (2026-09-24). A outra metade fica
# para o PyArrow, o delta-rs e o código do cliente.
_MEMORY_FRACTION = 0.5

# O orçamento de memória de cada stream: 64 MiB de lotes guardados à espera do cliente; com o
# cliente atrasado, o pico do processo ficou em 297 MB, contra 522 MB com 256 MiB (2026-09-23).
_MEMORY_BUDGET = 64 * 2**20

# A mensagem que recusa o que não é Arrow e aponta a conversão sem cópia.
_ARROW_ONLY = ("recebe pa.Table, pa.RecordBatch, pa.RecordBatchReader ou um iterável de lotes; um "
               "DataFrame vira pa.Table.from_pandas(frame, preserve_index=False) ou "
               "pa.RecordBatch.from_pandas(frame, preserve_index=False)")

_END = object()


def _delta_scan(uri: str, version: int) -> str:
    """A leitura da tabela Delta presa a uma versão, no texto do DuckDB."""
    return f"delta_scan({literal(uri)}, version := {int(version)})"


# ---------------------------------------------------------------- a compilação


def _required_parameters(statement: sa.sql.ClauseElement) -> set[str]:
    """Os nomes dos ``bindparam`` sem valor do statement, pelo percurso de todos os nós: sem
    ``literal_binds``, ``compiled.binds`` também os marca, mas ``params`` ignora o nome a mais."""
    names = set()
    for element in iterate(statement):
        if isinstance(element, sa.BindParameter) and element.required:
            names.add(element.key)
    return names


def _statement_metadata(statement: sa.sql.ClauseElement) -> sa.MetaData | None:
    """O ``MetaData`` das tabelas do contrato que o statement cita, o que ``sql.prefixed``
    recebe."""
    for table in find_tables(statement, include_crud=True):
        if isinstance(table, sa.Table):
            return table.metadata
    return None


def _compiled_statement(statement: sa.sql.ClauseElement,
                        params: Mapping[str, object] | None) -> tuple[str, list[object]]:
    """O texto do DuckDB e a lista posicional de um statement Core com os valores do cliente.

    Os nomes de ``params`` fecham com os ``bindparam`` sem valor, ou é ``SqlError``: ``params``
    ignora o nome a mais, e o valor que falta seria ``InvalidRequestError``. A cópia prefixada põe
    todo nome entre aspas; ``render_postcompile`` expande o ``IN`` de lista e o ``bindparam``
    expansível.
    """
    values = dict(params or {})
    required = _required_parameters(statement)
    if required != set(values):
        raise SqlError(f"parâmetros do statement {sorted(required)} e do dicionário "
                       f"{sorted(values)} não fecham")
    bound = statement.params(**values) if values else statement
    metadata = _statement_metadata(bound)
    if metadata is not None:
        bound = sql.prefixed(bound, metadata, prefix="")
    compiled = bound.compile(dialect=_QMARK, compile_kwargs={"render_postcompile": True})
    constructed = compiled.construct_params()
    arguments = []
    for name in compiled.positiontup or []:
        arguments.append(constructed[name])
    return str(compiled), arguments


# ---------------------------------------------------------------- o stream


@dataclasses.dataclass
class _Spool:
    """O que a thread da consulta entrega ao cliente, protegido pela ``condition``.

    ``schema`` é o do leitor da consulta, que existe antes do primeiro lote; ``in_memory`` guarda os
    lotes que cabem no orçamento, e ``memory_bytes`` o tamanho deles; ``spilled`` conta os lotes
    gravados no arquivo. Depois do primeiro lote no arquivo, todo lote seguinte vai para ele, para a
    ordem da consulta se manter: o cliente esvazia a memória antes de ler o arquivo.
    """

    condition: threading.Condition = dataclasses.field(default_factory=threading.Condition)
    schema: pa.Schema | None = None
    in_memory: collections.deque = dataclasses.field(default_factory=collections.deque)
    memory_bytes: int = 0
    spilled: int = 0
    done: bool = False
    error: BaseException | None = None


def _keep_in_memory(spool: _Spool, batch: pa.RecordBatch, budget: int) -> bool:
    """Guarda o lote na memória quando ele cabe no orçamento e o arquivo ainda não começou."""
    with spool.condition:
        if spool.spilled > 0 or spool.memory_bytes + batch.nbytes > budget:
            return False
        spool.in_memory.append(batch)
        spool.memory_bytes += batch.nbytes
        spool.condition.notify_all()
        return True


def _announce_spilled(spool: _Spool) -> None:
    """Conta um lote a mais no arquivo e acorda o cliente."""
    with spool.condition:
        spool.spilled += 1
        spool.condition.notify_all()


@dataclasses.dataclass
class _SpillFile:
    """O arquivo de transbordo de um stream, aberto no primeiro lote que não cabe no orçamento."""

    path: str
    sink: pa.OSFile | None = None
    writer: pa.ipc.RecordBatchStreamWriter | None = None

    def write(self, batch: pa.RecordBatch, schema: pa.Schema) -> None:
        """Grava o lote, abrindo o arquivo no primeiro."""
        if self.writer is None:
            self.sink = pa.OSFile(self.path, "wb")
            self.writer = pa.ipc.new_stream(self.sink, schema, options=_SPOOL_OPTIONS)
        self.writer.write_batch(batch)

    def close(self) -> None:
        """Fecha o arquivo, quando algum lote o abriu."""
        if self.writer is not None:
            self.writer.close()
            self.sink.close()


def _deliver(reader: pa.RecordBatchReader, spool: _Spool, spill: _SpillFile, budget: int,
             stop: threading.Event) -> None:
    """Entrega cada lote do leitor à memória ou ao arquivo, parando no ``stop``."""
    for batch in reader:
        if stop.is_set():
            return
        if _keep_in_memory(spool, batch, budget):
            continue
        spill.write(batch, reader.schema)
        _announce_spilled(spool)


def _produce(engine: DuckDBEngine, text: str, arguments: Sequence[object] | Mapping[str, object],
             batch_size: int, spill: _SpillFile, budget: int, stop: threading.Event,
             spool: _Spool) -> None:
    """Roda a consulta na sessão, sob o lock, e entrega os lotes assim que o DuckDB os produz.

    Nunca espera pelo cliente: o lock sai quando o resultado acaba, quando ``stop`` chega ou quando
    o ``interrupt`` do ``close`` cancela a consulta. O fim é marcado ainda com o lock tomado, e o
    ``close`` nunca cancela o comando seguinte da sessão. Recebe só o que usa, nunca o stream, para
    um stream abandonado ser coletado e o ``__del__`` ligar o ``stop``.
    """
    with engine.session() as connection:
        try:
            # Um close que chegou antes da consulta: o interrupt, com a conexão ociosa, não a
            # alcançaria.
            if stop.is_set():
                return
            reader = connection.execute(text, arguments).to_arrow_reader(batch_size)
            with spool.condition:
                spool.schema = reader.schema
            _deliver(reader, spool, spill, budget, stop)
        except Exception as error:  # noqa: BLE001 - o erro da consulta vai ao cliente
            spool.error = error
        finally:
            _finish(spool, spill, stop)


def _finish(spool: _Spool, spill: _SpillFile, stop: threading.Event) -> None:
    """Fecha o arquivo e marca o fim da consulta; parada pelo ``stop``, a thread apaga o arquivo que
    criou, porque ele pode nascer depois de o ``__del__`` apagar o caminho."""
    spill.close()
    if stop.is_set():
        Path(spill.path).unlink(missing_ok=True)
    with spool.condition:
        spool.done = True
        spool.condition.notify_all()


class DuckDBStream:
    """Os lotes de uma consulta do motor DuckDB, lidos enquanto ela roda.

    Uma thread roda a consulta na sessão, sob o lock, e entrega cada lote à memória enquanto os
    lotes guardados cabem no orçamento, e ao arquivo de transbordo o lote que não cabe e os
    seguintes; o cliente lê a memória e depois o arquivo, na sua thread e na ordem da consulta. O
    erro que a consulta encontra antes do primeiro lote aparece na construção; o que ela encontra
    depois, na leitura seguinte ao último lote entregue. Dentro de ``session()``, na mesma thread,
    a consulta roda na thread de quem chama. ``close`` cancela por ``interrupt()`` a consulta que
    ainda roda e apaga o arquivo. ``__arrow_c_stream__`` entrega os lotes a ``write_deltalake`` e a
    ``RecordBatchReader.from_stream``, não ao ``register`` do DuckDB, cujo ``arrow_scan`` puxaria o
    gerador Python numa thread que o motor não controla; para levar lotes ao sandbox existe
    ``loader``.
    """

    def __init__(self, engine: DuckDBEngine, text: str,
                 arguments: Sequence[object] | Mapping[str, object], batch_size: int = 100_000,
                 budget: int = _MEMORY_BUDGET) -> None:
        # O stop e o caminho vêm antes de tudo: o __del__ de uma construção que falhou os usa.
        self._stop = threading.Event()
        self._path = engine.spool_path("stream")
        self._engine = engine
        self._spool = _Spool()
        self._source: pa.OSFile | None = None
        self._file_reader: pa.ipc.RecordBatchStreamReader | None = None
        self._read_from_file = 0
        self._thread: threading.Thread | None = None
        spill = _SpillFile(self._path)
        arguments_of_produce = (engine, text, arguments, batch_size, spill, budget, self._stop,
                                self._spool)
        if engine.holds_session():
            _produce(*arguments_of_produce)
        else:
            self._thread = threading.Thread(target=_produce, args=arguments_of_produce, daemon=True)
            self._thread.start()
        # A construção espera o primeiro lote ou o fim: o erro de uma consulta que nada entregou
        # chega aqui.
        if self._wait_for_batch() is None and self._spool.error is not None:
            self.close()
            raise self._spool.error
        self.schema = self._spool.schema

    def _wait_for_batch(self) -> str | None:
        """Espera um lote não lido, em memória ou no arquivo; devolve de onde ele vem, ou ``None``
        no fim. A espera tem prazo e confere o ``stop``: quem puxa o stream pode ser a thread de
        leitura antecipada de um leitor nativo, e ela não fica presa aqui depois de um ``close``."""
        spool = self._spool
        with spool.condition:
            while not spool.in_memory and spool.spilled <= self._read_from_file and not spool.done:
                if self._stop.is_set():
                    return None
                spool.condition.wait(timeout=0.05)
            if spool.in_memory:
                return "memory"
            if spool.spilled > self._read_from_file:
                return "file"
            return None

    def _next_batch(self) -> pa.RecordBatch | None:
        """O próximo lote na ordem da consulta: primeiro a fila em memória, depois o arquivo."""
        source = self._wait_for_batch()
        if source is None:
            return None
        if source == "memory":
            with self._spool.condition:
                batch = self._spool.in_memory.popleft()
                self._spool.memory_bytes -= batch.nbytes
            return batch
        # O arquivo só é aberto quando tem um lote: antes dele, o esquema ainda não está gravado.
        if self._file_reader is None:
            self._source = pa.OSFile(self._path, "rb")
            self._file_reader = pa.ipc.open_stream(self._source)
        self._read_from_file += 1
        return self._file_reader.read_next_batch()

    def read_next_batch(self) -> pa.RecordBatch:
        """O próximo lote; ``StopIteration`` no fim, e o erro da consulta depois do último lote."""
        batch = self._next_batch()
        if batch is not None:
            return batch
        if self._spool.error is not None:
            raise self._spool.error
        raise StopIteration

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        while True:
            try:
                yield self.read_next_batch()
            except StopIteration:
                return

    def read_all(self) -> pa.Table:
        """Os lotes que faltam numa ``pa.Table``."""
        return pa.Table.from_batches(list(self), schema=self.schema)

    def __arrow_c_stream__(self, requested_schema: object = None) -> object:
        reader = pa.RecordBatchReader.from_batches(self.schema, iter(self))
        return reader.__arrow_c_stream__(requested_schema)

    def close(self) -> None:
        """Cancela a consulta que ainda roda e apaga o arquivo; a sessão continua usável."""
        self._stop.set()
        # Sob a condition, a thread não marca o fim, e o interrupt alcança só a consulta deste
        # stream.
        with self._spool.condition:
            if self._thread is not None and not self._spool.done:
                self._engine.interrupt()
        if self._thread is not None:
            self._thread.join(timeout=10)
        if self._source is not None:
            self._source.close()
        Path(self._path).unlink(missing_ok=True)

    def __enter__(self) -> DuckDBStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def __del__(self) -> None:
        # O stream abandonado: a consulta para no lote seguinte, e o arquivo sai.
        self._stop.set()
        Path(self._path).unlink(missing_ok=True)


# ---------------------------------------------------------------- o loader


def _take(source: queue.Queue, stop: threading.Event) -> object | None:
    """O próximo item da fila, esperando em fatias de 50 ms, para quem lê perceber o ``stop`` em
    vez de ficar preso num ``get`` sem fim; ``None`` quando ``stop`` chega com a fila vazia. Nenhum
    item da fila é ``None``."""
    while True:
        try:
            return source.get(timeout=0.05)
        except queue.Empty:
            pass
        if stop.is_set():
            return None


def _write_until_end(spill: _SpillFile, source: queue.Queue, closed: threading.Event,
                     outcome: dict[str, object]) -> None:
    """Grava cada lote tirado da fila até o fim dela; a exceção que o cliente pôs na fila, e o
    loader abandonado, sobem daqui."""
    item = _take(source, closed)
    while item is not _END:
        if item is None:
            raise RuntimeError("loader encerrado sem close")
        if isinstance(item, BaseException):
            raise item
        spill.write(item, item.schema)
        outcome["rows"] += item.num_rows
        item = _take(source, closed)


def _write_spool(spill: _SpillFile, source: queue.Queue, closed: threading.Event,
                 outcome: dict[str, object]) -> None:
    """A thread do loader: grava no arquivo os lotes da fila, sem a sessão.

    Um ``closed`` sem o fim da fila é um loader abandonado. Terminada com erro, o abandono, a
    exceção do cliente ou um lote recusado, a thread apaga o arquivo, que nenhum ``INSERT`` vai ler.
    """
    try:
        _write_until_end(spill, source, closed, outcome)
    except BaseException as error:  # noqa: BLE001 - relançado em close, ou no write seguinte
        outcome["error"] = error
    finally:
        spill.close()
    if outcome["error"] is not None:
        Path(spill.path).unlink(missing_ok=True)


def _checked_batches(data: pa.RecordBatch | pa.Table) -> list[pa.RecordBatch]:
    """Os lotes de um ``RecordBatch`` ou de uma ``pa.Table``; outro tipo é ``ContractError``."""
    if isinstance(data, pa.Table):
        return data.to_batches()
    if isinstance(data, pa.RecordBatch):
        return [data]
    raise ContractError(f"loader.write {_ARROW_ONLY}; recebido {type(data).__name__}")


class DuckDBLoader:
    """A carga em lotes de uma tabela nova do sandbox do motor DuckDB.

    A abertura confere o nome num cursor à parte, sem o lock da sessão, e recusa com
    ``SandboxError`` o nome que o ``ingest`` ou outro ``loader`` ocupou. ``write`` faz o ``cast`` do
    lote na thread do cliente, para o erro aparecer com o lote em mãos, e o põe numa fila limitada,
    que bloqueia quando está cheia; uma thread auxiliar grava os lotes num arquivo Arrow IPC com
    LZ4, sem a sessão, enquanto o cliente prepara o lote seguinte. ``close`` roda, sob o lock e numa
    transação, o ``CREATE TABLE`` do modelo e um único ``INSERT ... BY NAME`` sobre o leitor do
    arquivo: nada existe antes dele, e um erro desfaz os dois. Uma exceção dentro do ``with``, um
    lote recusado pelo ``cast`` ou um loader abandonado apagam o arquivo sem criar a tabela.
    """

    def __init__(self, engine: DuckDBEngine, table: sa.Table, queue_depth: int = 2) -> None:
        # O closed vem antes de tudo: o __del__ de uma abertura recusada o usa.
        self._closed = threading.Event()
        if engine.name_in_use(table.name):
            raise SandboxError(
                f"{table.name}: o nome já está ocupado no sandbox, pelo ingest ou por outro "
                f"loader; leia a versão publicada por run.published({table.name})")
        self._engine = engine
        self._table = table
        self._schema: pa.Schema | None = None
        self._refused: BaseException | None = None
        self._spill = _SpillFile(engine.spool_path("loader"))
        self._queue: queue.Queue = queue.Queue(maxsize=queue_depth)
        self._outcome: dict[str, object] = {"rows": 0, "error": None}
        self._thread = threading.Thread(
            target=_write_spool, args=(self._spill, self._queue, self._closed, self._outcome),
            daemon=True)
        self._thread.start()

    @property
    def rows(self) -> int:
        """As linhas gravadas até agora."""
        return self._outcome["rows"]

    @property
    def error(self) -> BaseException | None:
        """O erro da thread auxiliar ou o lote recusado, quando houve."""
        return self._refused or self._outcome["error"]

    def _put(self, item: object) -> bool:
        """Põe o item na fila; ``False`` quando a thread já terminou."""
        while self._thread.is_alive():
            try:
                self._queue.put(item, timeout=0.05)
                return True
            except queue.Full:
                continue
        return False

    def _converted(self, batch: pa.RecordBatch) -> pa.RecordBatch:
        """O lote no contrato, com as colunas do primeiro lote; outro conjunto é
        ``ContractError``."""
        converted = cast(batch, self._table)
        if self._schema is None:
            self._schema = converted.schema
        elif not converted.schema.equals(self._schema):
            raise ContractError(f"{self._table.name}: o lote traz {converted.schema.names}, e o "
                                f"primeiro trouxe {self._schema.names}")
        return converted

    def write(self, data: pa.RecordBatch | pa.Table) -> None:
        """Converte os lotes pelo contrato, na thread do cliente, e os põe na fila; um lote recusado
        faz o loader não criar a tabela."""
        for batch in _checked_batches(data):
            try:
                converted = self._converted(batch)
            except ContractError as error:
                self._refused = error
                raise
            if not self._put(converted):
                raise self.error or RuntimeError("a thread do loader terminou antes do fim da fila")

    def _create_and_insert(self) -> None:
        """A tabela criada e o arquivo inserido numa transação, sob o lock; o leitor registrado tem
        nome único e sai no mesmo bloco."""
        name = f"serialize_db_lote_{uuid.uuid4().hex[:8]}"
        with self._engine.session() as connection:
            connection.execute("BEGIN TRANSACTION")
            try:
                connection.execute(ddl(self._table, "duckdb"))
                if self._spill.writer is not None:
                    self._insert_file(connection, name)
                connection.execute("COMMIT")
            except BaseException:
                connection.execute("ROLLBACK")
                raise

    def _insert_file(self, connection: duckdb.DuckDBPyConnection, name: str) -> None:
        """O ``INSERT ... BY NAME`` do arquivo inteiro, pelo leitor nativo do Arrow IPC."""
        with pa.OSFile(self._spill.path, "rb") as source:
            connection.register(name, pa.ipc.open_stream(source))
            try:
                connection.execute(
                    f"INSERT INTO {quoted(self._table.name)} BY NAME SELECT * FROM {quoted(name)}")
            finally:
                connection.unregister(name)

    def close(self, error: BaseException | None = None) -> None:
        """Cria a tabela e insere os lotes; com ``error`` ou um lote recusado, só apaga o
        arquivo."""
        failure = error or self._refused
        self._put(failure if failure is not None else _END)
        self._closed.set()
        self._thread.join()
        try:
            if failure is None and self._outcome["error"] is None:
                self._create_and_insert()
        finally:
            Path(self._spill.path).unlink(missing_ok=True)
        if error is None and self.error is not None:
            raise self.error

    def __enter__(self) -> DuckDBLoader:
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        self.close(error=exc)

    def __del__(self) -> None:
        self._closed.set()


def _batches_of(data: object) -> Iterator[pa.RecordBatch]:
    """Os lotes de uma ``pa.Table``, de um lote, de um leitor ou de um iterável de lotes; outro
    tipo, um DataFrame inclusive, é ``ContractError`` antes de qualquer carga."""
    if isinstance(data, pa.Table):
        return iter(data.to_batches())
    if isinstance(data, pa.RecordBatch):
        return iter([data])
    if isinstance(data, pa.RecordBatchReader):
        return iter(data)
    if isinstance(data, (str, bytes)) or not isinstance(data, Iterable):
        raise ContractError(f"load {_ARROW_ONLY}; recebido {type(data).__name__}")
    iterator = iter(data)
    first = next(iterator, None)
    if first is None:
        return iter([])
    if not isinstance(first, pa.RecordBatch):
        raise ContractError(f"load {_ARROW_ONLY}; recebido {type(data).__name__}")
    return itertools.chain([first], iterator)


# ---------------------------------------------------------------- o motor


def environment_limits() -> dict[str, object]:
    """O ``threads`` e o ``memory_limit`` do DuckDB lidos do ambiente na chamada: as CPUs que o
    processo pode usar e metade da memória que ele ainda pode usar, por
    ``serialize_db.resources``. O motor os aplica na abertura quando a configuração os omite.

    Exemplo:

    .. code-block:: python

        environment_limits()   # {'threads': 4, 'memory_limit': '7306MiB'} em 4 vCPUs e 16 GiB
        duckdb.connect(config=environment_limits())
    """
    memory_limit = int(available_memory() * _MEMORY_FRACTION)
    return {"threads": available_cpus(), "memory_limit": f"{memory_limit // 2**20}MiB"}


@dataclasses.dataclass(frozen=True, kw_only=True)
class DuckDBConfig:
    """A configuração do sandbox DuckDB.

    Exemplo:

    .. code-block:: python

        DuckDBConfig(temp_directory="/dados/sandbox")
        DuckDBConfig(database=":memory:")   # em memória, só por pedido
    """

    database: str | None = None
    """``None``: ``<temp_directory>/<execution_id>.duckdb``, apagado em ``cleanup``;
    ``":memory:"`` só por pedido; outro caminho é usado e mantido."""
    threads: int | None = None
    """As threads da instância; ``None`` são as CPUs que o processo pode usar na abertura
    (``environment_limits``)."""
    memory_limit: str | None = None
    """Com unidade (``"4GiB"``); ``None`` é metade da memória que o processo ainda pode usar na
    abertura (``environment_limits``). O valor aplicado vai para o log."""
    temp_directory: str | None = None
    """A pasta do banco, do transbordo do DuckDB e dos arquivos de ``stream`` e ``loader``;
    ``None`` é uma pasta nova de ``tempfile.mkdtemp``, apagada em ``cleanup``."""
    extension_directory: str | None = None
    """A pasta das extensões; ``None`` é a que ``Storage.duckdb_connect`` resolve."""


def _connection_settings(config: DuckDBConfig, folder: str) -> dict[str, object]:
    """As opções da abertura da conexão: as fixas do motor, os limites lidos do ambiente e, no
    lugar deles, os que a configuração informa."""
    settings: dict[str, object] = {"temp_directory": folder, "preserve_insertion_order": False}
    settings.update(environment_limits())
    if config.threads is not None:
        settings["threads"] = config.threads
    if config.memory_limit is not None:
        settings["memory_limit"] = config.memory_limit
    if config.extension_directory is not None:
        settings["extension_directory"] = config.extension_directory
    return settings


class DuckDBEngine:
    """O sandbox DuckDB de uma execução: um banco, uma sessão e um ``RLock``.

    ``parent`` é o motor de que esta sessão a mais depende, dado por ``new_session``: a sessão
    abre sobre ``cursor()`` da conexão dele, com o seu lock, e o seu ``cleanup`` fecha só o cursor.

    Exemplo:

    .. code-block:: python

        engine = DuckDBEngine(DuckDBConfig(), "exec-2026-09-05", Storage.for_uri("/dados/delta"))
        try:
            engine.ingest(Operacao.__table__, uri, 3)
            print(engine.query("SELECT count(*) FROM cad_operacoes"))
        finally:
            engine.cleanup()
    """

    def __init__(self, config: DuckDBConfig, execution_id: str, storage: Storage,
                 parent: DuckDBEngine | None = None) -> None:
        self._config = config
        self.execution_id = check_partition_value(execution_id)
        self._storage = storage
        self._lock = threading.RLock()
        self._owner: int | None = None
        self._closed = False
        self._parent = parent
        if parent is not None:
            self._folder = parent._folder
            self._spool_folder = parent._spool_folder
            self._database = parent._database
            self._owns_folder = False
            self._connection = parent._connection.cursor()
            return
        self._owns_folder = config.temp_directory is None
        self._folder = config.temp_directory or tempfile.mkdtemp(prefix="serialize_db_")
        os.makedirs(self._folder, exist_ok=True)
        self._spool_folder = os.path.join(self._folder, f"{execution_id}_transbordo")
        os.makedirs(self._spool_folder, exist_ok=True)
        self._database = config.database or os.path.join(self._folder, f"{execution_id}.duckdb")
        self._connection = storage.duckdb_connect(self._database,
                                                  _connection_settings(config, self._folder))
        self._log_opening()

    def _log_opening(self) -> None:
        """O ``memory_limit`` e as ``threads`` aplicados e o espaço livre da pasta, para o log."""
        row = self._connection.execute(
            "SELECT current_setting('memory_limit'), current_setting('threads')").fetchone()
        free = shutil.disk_usage(self._folder).free / 2**30
        log.info("sandbox %s aberto em %s: memory_limit %s, threads %s, %.1f GiB livres em %s",
                 self.execution_id, self._database, row[0], row[1], free, self._folder)

    # ------------------------------------------------------------ a sessão

    @contextlib.contextmanager
    def session(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """A conexão crua com o lock tomado pelo bloco, reentrante na mesma thread: uma primitiva
        chamada dentro do bloco não trava, e um ``stream`` aberto nele roda a consulta na thread
        do bloco."""
        with self._lock:
            outer_owner = self._owner
            self._owner = threading.get_ident()
            try:
                yield self._connection
            finally:
                self._owner = outer_owner

    def holds_session(self) -> bool:
        """Se a thread que chama está dentro de ``session()``."""
        return self._owner == threading.get_ident()

    def new_session(self) -> DuckDBEngine:
        """Uma sessão a mais sobre o mesmo banco, gerenciador de contexto: vê o que a sessão
        principal confirmou e não as tabelas temporárias dela. O cursor nasce sem o lock da
        principal, porque ``cursor()`` não espera o comando em curso nela."""
        return DuckDBEngine(self._config, self.execution_id, self._storage, parent=self)

    def interrupt(self) -> None:
        """Cancela o comando em curso na conexão; não toma o lock, que está com quem roda o
        comando."""
        self._connection.interrupt()

    def spool_path(self, kind: str) -> str:
        """Um caminho novo na pasta de transbordo, para o arquivo de um stream ou de um loader."""
        return os.path.join(self._spool_folder, f"{kind}_{uuid.uuid4().hex}.arrow")

    def name_in_use(self, name: str) -> bool:
        """Se uma tabela ou view confirmada tem o nome, lido num cursor à parte, sem o lock da
        sessão: a abertura de um ``loader`` não espera a consulta de um ``stream`` aberto antes."""
        cursor = self._connection.cursor()
        try:
            found = cursor.execute(
                "SELECT (SELECT count(*) FROM duckdb_tables() WHERE table_name = $name) "
                "+ (SELECT count(*) FROM duckdb_views() WHERE view_name = $name)",
                {"name": name}).fetchone()[0]
        finally:
            cursor.close()
        return found > 0

    # ------------------------------------------------------------ a leitura do Delta

    def _partition_filter(self, table: sa.Table, partitions: Sequence[str] | None) -> str:
        """O ``WHERE`` das partições: o intervalo delas ao lado do ``IN``, porque o ``delta_scan``
        poda por ``=`` e por intervalo e abre todos os arquivos com um ``IN`` de mais de um
        valor."""
        if partitions is None:
            return ""
        partition_by = table_options(table).partition_by
        if partition_by is None:
            raise ContractError(
                f"{table.name}: tabela sem partição recebeu partitions={partitions}")
        values = sorted(check_partition_value(value) for value in partitions)
        if not values:
            return " WHERE false"
        column = quoted(partition_by)
        listed = ", ".join(literal(value) for value in values)
        return (f" WHERE {column} BETWEEN {literal(values[0])} AND {literal(values[-1])} "
                f"AND {column} IN ({listed})")

    def ingest(self, table: sa.Table, uri: str, version: int, partitions: list[str] | None = None,
               materialize: bool = False) -> None:
        """Uma view com o nome do modelo sobre a versão fixada da tabela Delta, ou uma tabela com
        ``materialize=True``; com ``partitions``, só essas partições.

        Um commit na tabela depois da abertura não muda o que a view lê. O nome ocupado é
        ``SandboxError``.

        Exemplo:

        .. code-block:: python

            engine.ingest(Lancamento.__table__, uri, 143, partitions=previous, materialize=True)
        """
        if version is None:
            raise SandboxError(f"{table.name}: sem versão fixada, a tabela não existe no Delta")
        if self.name_in_use(table.name):
            raise SandboxError(f"{table.name}: o nome já está ocupado no sandbox")
        kind = "TABLE" if materialize else "VIEW"
        where = self._partition_filter(table, partitions)
        text = (f"CREATE {kind} {quoted(table.name)} AS SELECT * FROM "
                f"{_delta_scan(uri, version)}{where}")
        with self.session() as connection:
            connection.execute(text)

    def published(self, table: sa.Table, uri: str, version: int | None) -> sa.FromClause:
        """A versão fixada da tabela como origem de consulta, sem ocupar nome no sandbox: o
        ``FromClause`` com as colunas do contrato que compila para
        ``delta_scan(uri, version := v)``.

        Numa tabela que ainda não existe, sem versão, é ``SandboxError``.

        Exemplo:

        .. code-block:: python

            previous = engine.published(Projetada.__table__, uri, 57)
            engine.query(sa.select(sa.func.max(previous.c.id_lancamento)))
        """
        if version is None:
            raise SandboxError(f"{table.name}: sem versão publicada, a tabela ainda não existe")
        columns = []
        for column in table.columns:
            columns.append(sa.column(quoted_name(column.name, quote=True), column.type))
        source = sa.table(quoted_name(_delta_scan(uri, version), quote=False), *columns)
        return source.alias(quoted_name(f"{table.name}_publicado", quote=True))

    # ------------------------------------------------------------ consulta, stream e carga

    def _compiled(self, statement_or_sql: sa.sql.ClauseElement | str,
                  params: Mapping[str, object] | None) -> tuple[str, object]:
        """O texto e os parâmetros do driver: o statement pelo caminho de compilação, o texto
        pronto por ``sql.bind``."""
        if isinstance(statement_or_sql, str):
            return sql.bind(statement_or_sql, dict(params or {}), "duckdb")
        return _compiled_statement(statement_or_sql, params)

    def query(self, statement_or_sql: sa.sql.ClauseElement | str,
              params: Mapping[str, object] | None = None) -> pa.Table:
        """O resultado inteiro como ``pa.Table``, sob o lock; um comando sem resultado devolve a
        tabela ``Count`` ou ``Success`` do DuckDB.

        Exemplo:

        .. code-block:: python

            engine.query(sa.select(tabela).where(tabela.c.data_str == sa.bindparam("p")),
                         {"p": "2026-08-31"})
        """
        text, arguments = self._compiled(statement_or_sql, params)
        with self.session() as connection:
            return connection.execute(text, arguments).to_arrow_table()

    def stream(self, statement_or_sql: sa.sql.ClauseElement | str,
               params: Mapping[str, object] | None = None,
               batch_size: int = 100_000) -> DuckDBStream:
        """Os lotes da consulta enquanto ela roda, com a memória limitada a 64 MiB de lotes.

        Exemplo:

        .. code-block:: python

            with engine.stream(sa.select(tabela), batch_size=100_000) as stream:
                for batch in stream:
                    work(batch)
        """
        text, arguments = self._compiled(statement_or_sql, params)
        return DuckDBStream(self, text, arguments, batch_size)

    def loader(self, table: sa.Table, queue_depth: int = 2) -> DuckDBLoader:
        """O gerenciador de contexto que grava lotes numa tabela nova do sandbox, criada no
        ``close``.

        Exemplo:

        .. code-block:: python

            with engine.loader(Projetada.__table__) as loader:
                loader.write(batch)
        """
        return DuckDBLoader(self, table, queue_depth)

    def load(
        self, table: sa.Table,
        data: pa.Table | pa.RecordBatch | pa.RecordBatchReader | Iterable[pa.RecordBatch],
    ) -> int:
        """Grava os lotes numa tabela nova pelo ``loader`` e devolve as linhas; um DataFrame é
        ``ContractError`` com a conversão sem cópia na mensagem.

        Exemplo:

        .. code-block:: python

            engine.load(Projetada.__table__, pa.Table.from_pandas(frame, preserve_index=False))
        """
        batches = _batches_of(data)
        with self.loader(table) as loader:
            for batch in batches:
                loader.write(batch)
        return loader.rows

    # ------------------------------------------------------------ a auditoria

    def _published_max_key(self, table: sa.Table, uri: str, version: int) -> int | None:
        """O ``max_key`` da versão publicada na chave sequencial, sem ler dados; ``None`` numa
        tabela sem ela."""
        key = sequential_key(table)
        if key is None:
            return None
        return delta.max_key(delta.open_table(uri, self._storage, version), key.name)

    def _referenced_sources(
        self, table: sa.Table, foreign_keys: bool,
        referenced: Mapping[str, tuple[str, int]] | None,
    ) -> dict[str, sa.FromClause]:
        """A origem da linha referenciada de cada chave estrangeira: a tabela do sandbox com o nome
        do modelo, ou a versão fixada informada em ``referenced``."""
        sources: dict[str, sa.FromClause] = {}
        if not foreign_keys:
            return sources
        for constraint in table.foreign_key_constraints:
            target = constraint.referred_table
            if self.name_in_use(target.name):
                sources[target.name] = target
            elif referenced is not None and target.name in referenced:
                uri, version = referenced[target.name]
                sources[target.name] = self.published(target, uri, version)
        return sources

    def _text(self, statement: sa.sql.ClauseElement, table: sa.Table) -> str:
        """O texto do DuckDB de uma verificação, sobre as tabelas do contrato."""
        return sql.render(statement, "duckdb", table.metadata, prefix="")

    def _rows_result(self, table: sa.Table, check: audit.Check,
                     partitions: Sequence[str] | None) -> tuple[CheckResult, dict, dict]:
        """A verificação de linhas: o resultado, as leituras por partição e os não finitos."""
        text = self._text(check.statement, table)
        rows = self.query(text).to_pylist()
        partition_by = table_options(table).partition_by
        doubles = double_columns(table)
        totals = {}
        nonfinite = {}
        # Uma linha por partição: o valor dela é a chave, e as demais colunas são as leituras.
        for row in rows:
            value = row[partition_by] if partition_by else None
            readings = {name: reading for name, reading in row.items() if name != partition_by}
            totals[value] = readings
            nonfinite[value] = tuple(name for name in doubles if readings[f"naofinito_{name}"])
        failing = []
        defects = 0
        for label in check.counters:
            counted = sum(readings[label] for readings in totals.values())
            defects += counted
            if counted:
                failing.append(label)
        sample = self._rows_sample(table, check, partitions, failing)
        return CheckResult(check.name, text, defects, sample, defects == 0), totals, nonfinite

    def _rows_sample(self, table: sa.Table, check: audit.Check, partitions: Sequence[str] | None,
                     failing: list[str]) -> pa.Table:
        """Até 20 linhas inteiras dos contadores reprovados, uma consulta por contador."""
        samples = []
        for label in failing:
            statement = audit.sample_statement(table, partitions, check.counters[label])
            samples.append(self.query(self._text(statement, table)))
        if not samples:
            return pa.table({})
        return pa.concat_tables(samples).slice(0, audit.SAMPLE_ROWS)

    def _check_result(self, table: sa.Table, check: audit.Check) -> CheckResult:
        """Uma verificação de chave ou de órfão; o ``skip_when`` verdadeiro a aprova sem
        rodá-la."""
        text = self._text(check.statement, table)
        if check.skip_when is not None:
            skipped = self.query(self._text(check.skip_when, table)).column(0)[0].as_py()
            if skipped is True:
                reason = ("dispensada: o menor valor da execução passa do maior da versão "
                          "publicada")
                return CheckResult(check.name, text, 0, pa.table({}), True, reason)
        found = self.query(text)
        return CheckResult(check.name, text, found.num_rows, found.slice(0, audit.SAMPLE_ROWS),
                           found.num_rows == 0)

    def audit(self, table: sa.Table, partitions: list[str] | None, uri: str | None = None,
              version: int | None = None, foreign_keys: bool = False,
              key_scope: KeyScope | None = None,
              referenced: Mapping[str, tuple[str, int]] | None = None) -> AuditReport:
        """Roda as verificações do contrato sobre a tabela do sandbox e devolve o relatório.

        ``uri`` e ``version`` são os da tabela fixada pela execução: dão a versão publicada, que as
        chaves fora da partição comparam, e o ``max_key`` do ``skip_when``. ``referenced`` dá, por
        tabela, a URI e a versão fixada da tabela referenciada que o sandbox não tem, para as
        chaves estrangeiras com ``foreign_keys=True``. A reprovação não levanta aqui: ``passed`` é
        falso, e ``Execution.audit`` levanta ``AuditFailed``.

        Exemplo:

        .. code-block:: python

            report = engine.audit(Projetada.__table__, ["2026-08-31"], uri, 57)
            report.passed, report.nonfinite_columns
        """
        published = None
        published_max_key = None
        if uri is not None and version is not None:
            published = self.published(table, uri, version)
            published_max_key = self._published_max_key(table, uri, version)
        sources = self._referenced_sources(table, foreign_keys, referenced)
        found, not_run = audit.checks_and_not_run(table, partitions, foreign_keys, key_scope,
                                                  published, sources, published_max_key)
        results = []
        totals: dict = {}
        nonfinite: dict = {}
        for check in found:
            if check.name == "linhas":
                result, totals, nonfinite = self._rows_result(table, check, partitions)
            else:
                result = self._check_result(table, check)
            results.append(result)
        return AuditReport(
            table=table.name,
            partitions=tuple(partitions) if partitions is not None else None,
            results=tuple(results),
            not_run=tuple(not_run),
            nonfinite_columns=nonfinite,
            totals=totals,
        )

    # ------------------------------------------------------------ a exportação

    def _partition_select(self, table: sa.Table, value: str | None, with_partition: bool) -> str:
        """O ``SELECT`` da partição no sandbox, cada coluna do contrato em ``CAST`` para o tipo
        dele, na ordem da ``sort_key``; com ``with_partition=False``, sem a coluna de partição."""
        options = table_options(table)
        columns = []
        for column in table.columns:
            if column.name == options.partition_by and not with_partition:
                continue
            name = quoted(column.name)
            columns.append(f"CAST({name} AS {sql_type(column, 'duckdb')}) AS {name}")
        text = f"SELECT {', '.join(columns)} FROM {quoted(table.name)}"
        if options.partition_by is not None:
            text += f" WHERE {quoted(options.partition_by)} = {literal(value)}"
        if options.sort_key:
            text += " ORDER BY " + ", ".join(quoted(name) for name in options.sort_key)
        return text

    def _count_text(self, table: sa.Table, value: str | None) -> str:
        """A contagem das linhas da partição no sandbox."""
        partition_by = table_options(table).partition_by
        text = f"SELECT count(*) FROM {quoted(table.name)}"
        if partition_by is not None:
            text += f" WHERE {quoted(partition_by)} = {literal(value)}"
        return text

    def _export_register(self, table: sa.Table, uri: str, value: str | None,
                         metadata: Mapping[str, str], expected_rows: int | None,
                         columns_without_min_max: Collection[str]) -> int:
        """O ``COPY ... RETURN_STATS`` da partição para um arquivo novo na pasta dela, registrado
        por ``register_files`` com as conferências, o commit e a releitura."""
        partition_by = table_options(table).partition_by
        table_path = self._storage.relative(uri)
        name = f"{self.execution_id}_{uuid.uuid4().hex}.parquet"
        relative = name
        if partition_by is not None:
            relative = f"{partition_by}={value}/{name}"
            self._storage.ensure_folder(self._storage.join(table_path, f"{partition_by}={value}"))
        target = self._storage.uri_of(self._storage.join(table_path, relative))
        select = self._partition_select(table, value, with_partition=False)
        with self.session() as connection:
            count = connection.execute(self._count_text(table, value)).fetchone()[0]
            cursor = connection.execute(
                f"COPY ({select}) TO {literal(target)} (FORMAT parquet, RETURN_STATS)")
            names = [column[0] for column in cursor.description]
            row = dict(zip(names, cursor.fetchone()))
        file = delta.file_from_return_stats(row, table, self._storage.uri_of(table_path))
        expected = expected_rows if expected_rows is not None else count
        return delta.register_files(uri, table, [file], value, metadata, self._storage, expected,
                                    columns_without_min_max)

    def _export_rewrite(self, table: sa.Table, uri: str, value: str | None,
                        metadata: Mapping[str, str], expected_rows: int | None,
                        columns_without_min_max: Collection[str]) -> int:
        """O leitor da partição passado por ``cast`` a ``publish_partition``, sob o lock: a escrita
        consome o leitor antes de o lock sair, porque o comando seguinte o esvaziaria."""
        select = self._partition_select(table, value, with_partition=True)
        with self.session() as connection:
            count = connection.execute(self._count_text(table, value)).fetchone()[0]
            if expected_rows is not None and count != expected_rows:
                raise RegistrationRefused(f"{table.name} partição {value}: {count} linhas no "
                                          f"sandbox, {expected_rows} esperadas")
            reader = connection.execute(select).to_arrow_reader()
            return delta.publish_partition(uri, table, value, cast(reader, table), metadata,
                                           self._storage, columns_without_min_max)

    def export_partition(self, table: sa.Table, uri: str, value: str | None,
                         metadata: Mapping[str, str], mode: ExportMode,
                         expected_rows: int | None = None,
                         columns_without_min_max: Collection[str] = ()) -> int:
        """Leva a partição do sandbox ao Delta e devolve a versão do commit.

        ``mode`` é o ``export_mode`` resolvido pela execução: ``"register"`` grava a partição por
        ``COPY ... (RETURN_STATS)`` num arquivo novo dentro da pasta dela e a registra por
        ``register_files``, com memória constante; ``"rewrite"`` passa o leitor da partição a
        ``publish_partition``, e a memória cresce com a partição. ``expected_rows``, a contagem da
        auditoria, confere as linhas; sem ela, a contagem do sandbox. As colunas de
        ``columns_without_min_max`` saem sem mínimo e máximo (issue #59).

        Exemplo:

        .. code-block:: python

            engine.export_partition(Projetada.__table__, uri, "2026-08-31",
                                    delta.commit_metadata("exec-42", versions), "register")
        """
        if table_options(table).partition_by is not None:
            check_partition_value(value)
        if mode == "register":
            return self._export_register(table, uri, value, metadata, expected_rows,
                                         columns_without_min_max)
        if mode == "rewrite":
            return self._export_rewrite(table, uri, value, metadata, expected_rows,
                                        columns_without_min_max)
        raise ValueError(f"export_mode {mode!r}: use 'register' ou 'rewrite'")

    # ------------------------------------------------------------ o encerramento

    def cleanup(self) -> None:
        """Cancela o comando em curso, fecha a conexão e apaga o banco e a pasta de transbordo.

        Numa sessão a mais, fecha só o cursor. A segunda chamada não faz nada.
        """
        if self._closed:
            return
        self._closed = True
        self._connection.interrupt()
        with self._lock:
            self._connection.close()
        if self._parent is not None:
            return
        shutil.rmtree(self._spool_folder, ignore_errors=True)
        if self._config.database is None:
            for path in (self._database, self._database + ".wal"):
                Path(path).unlink(missing_ok=True)
        if self._owns_folder:
            shutil.rmtree(self._folder, ignore_errors=True)

    def __enter__(self) -> DuckDBEngine:
        return self

    def __exit__(self, *exc: object) -> None:
        self.cleanup()
