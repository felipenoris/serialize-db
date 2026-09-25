"""Os motores da execução e a interface comum a eles.

``Engine`` é o protocolo que ``serialize_db.execution`` usa, com a mesma forma nos dois motores:
``serialize_db.engine.duckdb`` (etapa 4) e ``serialize_db.engine.redshift`` (etapa 5). Cada motor
guarda uma sessão por execução sob um ``threading.RLock`` que as primitivas tomam e soltam;
``session()`` dá a conexão crua com o lock tomado pelo bloco, reentrante na mesma thread, e
``new_session()`` abre uma sessão a mais sobre o mesmo banco, para o que roda em paralelo.

Os dados cruzam a fronteira em lotes Arrow: ``stream`` devolve um ``BatchStream`` de
``pa.RecordBatch``, e ``loader`` recebe lotes por ``write``; ``query`` devolve a ``pa.Table`` que
``stream`` montaria, e ``load`` entrega ao ``loader`` os lotes de uma ``pa.Table``, de um
``RecordBatch``, de um ``RecordBatchReader`` ou de um iterável.

Exemplo, com o motor DuckDB:

.. code-block:: python

    from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine

    with DuckDBEngine(DuckDBConfig(), "exec-2026-09-05", storage) as engine:
        engine.ingest(Operacao.__table__, uri, version, partitions=["2026-08-31"])
        with engine.stream(sa.select(Operacao)) as stream, \\
                engine.loader(Projetado.__table__) as loader:
            for batch in stream:
                loader.write(project(batch))
"""

from __future__ import annotations

import contextlib
import itertools
import queue
import threading
from collections.abc import Collection, Iterable, Iterator, Mapping
from typing import Protocol, runtime_checkable

import pyarrow as pa
import sqlalchemy as sa

from serialize_db.audit import AuditReport, KeyScope
from serialize_db.errors import ContractError

__all__ = ["BatchStream", "Engine", "Loader", "duckdb", "redshift"]

# A mensagem que recusa o que não é Arrow e aponta a conversão sem cópia.
ARROW_ONLY = ("recebe pa.Table, pa.RecordBatch, pa.RecordBatchReader ou um iterável de lotes; um "
              "DataFrame vira pa.Table.from_pandas(frame, preserve_index=False) ou "
              "pa.RecordBatch.from_pandas(frame, preserve_index=False)")


class BatchStream(Protocol):
    """Os lotes de uma consulta, lidos na ordem dela enquanto ela roda.

    O protocolo não é instanciado: a assinatura ``(*args, **kwargs)`` da classe é a do
    ``__init__`` que ``typing.Protocol`` dá a todo protocolo.

    Exemplo:

    .. code-block:: python

        with engine.stream(sa.select(Lancamento)) as stream:
            stream.schema.names   # as colunas, antes do primeiro lote
            for batch in stream:
                work(batch)
    """

    schema: pa.Schema
    """O esquema Arrow dos lotes, conhecido na abertura, antes do primeiro lote."""

    def read_next_batch(self) -> pa.RecordBatch:
        """O próximo lote, na ordem da consulta.

        O erro que a consulta ou a leitura encontra sobe depois do último lote entregue.

        Exemplo:

        .. code-block:: python

            first = stream.read_next_batch()

        :return: o lote, no esquema ``schema``.
        :raises StopIteration: no fim dos lotes.
        """

    def __iter__(self) -> Iterator[pa.RecordBatch]: ...

    def read_all(self) -> pa.Table:
        """Os lotes que faltam numa ``pa.Table``.

        Exemplo:

        .. code-block:: python

            rest = stream.read_all()

        :return: a tabela, no esquema ``schema``; vazia depois do último lote.
        """

    def close(self) -> None:
        """Para a consulta ou a leitura que ainda roda e apaga os arquivos do stream; a sessão
        continua usável.

        Exemplo:

        .. code-block:: python

            stream.close()   # o with do stream chama close na saída
        """

    def __enter__(self) -> BatchStream: ...
    def __exit__(self, *exc: object) -> None: ...
    def __arrow_c_stream__(self, requested_schema: object = None) -> object: ...


class Loader(Protocol):
    """A carga em lotes de uma tabela nova do sandbox, criada no ``close``.

    O protocolo não é instanciado: a assinatura ``(*args, **kwargs)`` da classe é a do
    ``__init__`` que ``typing.Protocol`` dá a todo protocolo.

    Exemplo:

    .. code-block:: python

        with engine.loader(Projetado.__table__) as loader:
            loader.write(batch)
        loader.rows   # as linhas da tabela criada
    """

    rows: int
    """As linhas gravadas até agora; depois do ``close``, as da tabela."""

    def write(self, data: pa.RecordBatch | pa.Table) -> None:
        """Converte os lotes pelo contrato, na thread de quem chama, e os põe na fila da
        gravação, que bloqueia quando está cheia.

        Exemplo:

        .. code-block:: python

            loader.write(batch)

        :param data: um ``pa.RecordBatch`` ou uma ``pa.Table``.
        :raises ContractError: ``data`` de outro tipo; ou um lote que o ``cast`` recusa, ou com
            colunas diferentes das do primeiro lote, e então o loader não cria a tabela.
        """

    def close(self) -> None:
        """Cria a tabela do modelo e carrega os lotes gravados numa transação: um erro desfaz
        os dois.

        Exemplo:

        .. code-block:: python

            loader.close()   # o with do loader chama close na saída

        :raises ContractError: o lote que o ``write`` recusou; nada é criado.
        """

    def __enter__(self) -> Loader: ...
    def __exit__(self, *exc: object) -> None: ...


@runtime_checkable
class Engine(Protocol):
    """A interface dos dois motores; ``Execution`` só depende dela.

    O protocolo não é instanciado: a assinatura ``(*args, **kwargs)`` da classe é a do
    ``__init__`` que ``typing.Protocol`` dá a todo protocolo.

    Exemplo:

    .. code-block:: python

        def project_entries(engine: Engine) -> int:
            with (engine.stream(sa.select(Lancamento)) as stream,
                  engine.loader(Projetado.__table__) as loader):
                for batch in stream:
                    loader.write(project(batch))
            return loader.rows
    """

    execution_id: str
    """O identificador da execução, na regra da partição (``schema.PARTITION_VALUE``)."""

    def session(self) -> contextlib.AbstractContextManager[object]:
        """A conexão crua com o lock tomado pelo bloco, reentrante na mesma thread: uma primitiva
        chamada dentro do bloco não trava.

        Exemplo:

        .. code-block:: python

            with engine.session() as connection:
                engine.query("SELECT 1")   # a primitiva dentro do bloco não trava

        :return: o gerenciador de contexto cujo ``with`` dá a conexão do driver.
        """

    def new_session(self) -> Engine:
        """Uma sessão a mais sobre o mesmo banco, com o seu lock, para o que roda em paralelo:
        vê o que a sessão principal confirmou e não as tabelas temporárias dela.

        Exemplo:

        .. code-block:: python

            with engine.new_session() as session:
                session.ingest(Contrato.__table__, uri, 88)

        :return: o motor da sessão a mais, gerenciador de contexto; o ``cleanup`` dele fecha só
            essa sessão.
        """

    def __enter__(self) -> Engine: ...
    def __exit__(self, *exc: object) -> None: ...

    def ingest(self, table: sa.Table, uri: str, version: int, partitions: list[str] | None = None,
               materialize: bool = False) -> None:
        """Leva ao sandbox as partições pedidas da versão fixada da tabela Delta, com o nome do
        modelo.

        Exemplo:

        .. code-block:: python

            engine.ingest(Lancamento.__table__, uri, 143, partitions=["2026-08-31"])

        :param table: a tabela do modelo, cujo nome a ingestão ocupa no sandbox.
        :param uri: a URI da tabela Delta.
        :param version: a versão fixada da tabela.
        :param partitions: os valores de partição a ler; ``None`` lê todas, e a lista vazia,
            nenhuma.
        :param materialize: ``True`` copia os dados para uma tabela do sandbox; com ``False``, o
            motor que lê o Delta no lugar cria uma view.
        :raises SandboxError: o nome ocupado no sandbox, ou a tabela que não existe no Delta,
            sem versão (``version=None``).
        :raises ContractError: ``partitions`` numa tabela sem partição, ou um valor fora da regra
            da partição.
        """

    def published(self, table: sa.Table, uri: str, version: int | None) -> sa.FromClause:
        """A versão fixada da tabela como origem de consulta, sem ocupar o nome do modelo no
        sandbox.

        Exemplo:

        .. code-block:: python

            previous = engine.published(Projetado.__table__, uri, 57)
            engine.query(sa.select(sa.func.max(previous.c.id_lancamento)))

        :param table: a tabela do modelo, que dá as colunas.
        :param uri: a URI da tabela Delta.
        :param version: a versão fixada.
        :return: o ``FromClause`` com as colunas do contrato, para os statements Core.
        :raises SandboxError: numa tabela que ainda não existe, sem versão (``version=None``).
        """

    def stream(self, statement_or_sql: sa.sql.ClauseElement | str,
               params: Mapping[str, object] | None = None,
               batch_size: int = 100_000) -> BatchStream:
        """Os lotes da consulta, lidos na ordem dela enquanto o cliente trabalha no lote
        anterior.

        Exemplo:

        .. code-block:: python

            with engine.stream(sa.select(Lancamento), batch_size=50_000) as stream:
                for batch in stream:
                    work(batch)

        :param statement_or_sql: um statement Core sobre as tabelas do modelo, que o motor
            compila para o sandbox, ou um texto pronto no SQL do motor, com os parâmetros como
            ``:nome``.
        :param params: os valores dos parâmetros, por nome, dos ``bindparam`` sem valor do
            statement ou dos marcadores do texto.
        :param batch_size: o máximo de linhas de cada lote.
        :return: o ``BatchStream`` dos lotes, gerenciador de contexto; o ``close`` apaga os
            arquivos dele.
        :raises SqlError: os nomes de ``params`` não fecham com os parâmetros do statement ou do
            texto.
        """

    def query(self, statement_or_sql: sa.sql.ClauseElement | str,
              params: Mapping[str, object] | None = None) -> pa.Table:
        """O resultado inteiro como ``pa.Table``, sob o lock.

        Exemplo:

        .. code-block:: python

            engine.query(sa.select(sa.func.count()).select_from(Lancamento.__table__))

        :param statement_or_sql: um statement Core sobre as tabelas do modelo, que o motor
            compila para o sandbox, ou um texto pronto no SQL do motor, com os parâmetros como
            ``:nome``.
        :param params: os valores dos parâmetros, por nome, dos ``bindparam`` sem valor do
            statement ou dos marcadores do texto.
        :return: a tabela do resultado.
        :raises SqlError: os nomes de ``params`` não fecham com os parâmetros do statement ou do
            texto.
        """

    def loader(self, table: sa.Table, queue_depth: int = 2) -> Loader:
        """O gerenciador de contexto que grava lotes numa tabela nova do sandbox, criada e
        carregada no ``close``.

        Exemplo:

        .. code-block:: python

            with engine.loader(Projetado.__table__) as loader:
                loader.write(batch)

        :param table: a tabela do modelo, cujo nome não pode estar ocupado no sandbox.
        :param queue_depth: os lotes convertidos que esperam a thread de gravação; com a fila
            cheia, o ``write`` bloqueia.
        :return: o ``Loader`` da tabela, que guarda os lotes num arquivo até o ``close``.
        :raises SandboxError: o nome que o ``ingest`` ou outro ``loader`` ocupou.
        """

    def load(
        self, table: sa.Table,
        data: pa.Table | pa.RecordBatch | pa.RecordBatchReader | Iterable[pa.RecordBatch],
    ) -> int:
        """Grava os lotes numa tabela nova pelo ``loader``.

        Exemplo:

        .. code-block:: python

            engine.load(Projetado.__table__, pa.Table.from_pandas(frame, preserve_index=False))

        :param table: a tabela do modelo, cujo nome não pode estar ocupado no sandbox.
        :param data: uma ``pa.Table``, um ``pa.RecordBatch``, um ``pa.RecordBatchReader`` ou um
            iterável de ``pa.RecordBatch``.
        :return: as linhas gravadas.
        :raises ContractError: um DataFrame, com a conversão sem cópia na mensagem, ou outro
            tipo em ``data``; ou um lote que o ``cast`` recusa, e a tabela não é criada.
        :raises SandboxError: o nome que o ``ingest`` ou outro ``loader`` ocupou.
        """

    def audit(self, table: sa.Table, partitions: list[str] | None, uri: str | None = None,
              version: int | None = None, foreign_keys: bool = False,
              key_scope: KeyScope | None = None,
              referenced: Mapping[str, tuple[str, int]] | None = None) -> AuditReport:
        """Roda as verificações do contrato sobre a tabela do sandbox.

        Exemplo:

        .. code-block:: python

            report = engine.audit(Projetado.__table__, ["2026-08-31"], uri, 57)
            report.passed

        :param table: a tabela do modelo, no sandbox.
        :param partitions: as partições da execução; ``None`` audita a tabela inteira do
            sandbox.
        :param uri: a URI da tabela fixada pela execução; com ``version``, dá a versão
            publicada, que as chaves fora da partição comparam, e o ``max_key`` do
            ``skip_when``.
        :param version: a versão fixada da tabela; sem ela, ou sem ``uri``, a chave publicada
            não roda.
        :param foreign_keys: ``True`` confere as chaves estrangeiras, contra a tabela
            referenciada do sandbox ou contra a versão de ``referenced``.
        :param key_scope: o escopo da unicidade; ``"partition"`` suprime a chave publicada, e
            ``"table"`` a confere também na chave com a coluna de ``partition_source``.
        :param referenced: por tabela, a URI e a versão fixada da tabela referenciada que o
            sandbox não tem, para as chaves estrangeiras com ``foreign_keys=True``.
        :return: o ``AuditReport``; a reprovação não levanta aqui: ``passed`` é falso, e
            ``Execution.audit`` levanta ``AuditFailed``.
        :raises ContractError: um valor de ``partitions`` fora da regra da partição.
        """

    def export_partition(self, table: sa.Table, uri: str, value: str | None,
                         metadata: Mapping[str, str], expected_rows: int | None = None,
                         columns_without_min_max: Collection[str] = ()) -> int:
        """Leva a partição do sandbox ao Delta num commit.

        Exemplo:

        .. code-block:: python

            engine.export_partition(Projetado.__table__, uri, "2026-08-31",
                                    delta.commit_metadata("exec-42", versions), expected_rows=1000)

        :param table: a tabela do modelo, no sandbox.
        :param uri: a URI da tabela Delta, sob a raiz do armazenamento.
        :param value: o valor da partição; ``None`` numa tabela sem partição, que sai inteira.
        :param metadata: os metadados do commit, de ``delta.commit_metadata``.
        :param expected_rows: a contagem da auditoria, que confere as linhas dos arquivos
            registrados; sem ela, a contagem do sandbox.
        :param columns_without_min_max: as colunas ``Double`` com valor não finito na partição,
            que saem sem mínimo e máximo (issue #59).
        :return: a versão do commit.
        :raises ContractError: o valor fora da regra da partição, ``None`` numa tabela
            particionada, ou um valor numa tabela sem partição.
        :raises RegistrationRefused: uma conferência dos arquivos reprovou, sem commit, ou a
            releitura desfez o commit.
        :raises ExecutionConflict: outro commit na mesma partição a partir da mesma versão.
        :raises ValueError: ``uri`` fora da raiz do armazenamento, no registro dos arquivos.
        """

    def cleanup(self) -> None:
        """Fecha a sessão e apaga o que a execução criou no sandbox; numa sessão a mais, fecha
        só ela. A segunda chamada não faz nada.

        Exemplo:

        .. code-block:: python

            engine.cleanup()   # o with do motor chama cleanup na saída
        """


# ---------------------------------------------------------------- os lotes e as filas dos motores


def batches_of(data: object) -> Iterator[pa.RecordBatch]:
    """Os lotes de uma ``pa.Table``, de um lote, de um leitor ou de um iterável de lotes; protegida,
    para o ``load`` dos motores. Outro tipo, um DataFrame inclusive, é ``ContractError`` antes de
    qualquer carga."""
    if isinstance(data, pa.Table):
        return iter(data.to_batches())
    if isinstance(data, pa.RecordBatch):
        return iter([data])
    if isinstance(data, pa.RecordBatchReader):
        return iter(data)
    if isinstance(data, (str, bytes)) or not isinstance(data, Iterable):
        raise ContractError(f"load {ARROW_ONLY}; recebido {type(data).__name__}")
    # O primeiro item decide se o iterável é de lotes; ele volta à frente dos demais.
    iterator = iter(data)
    first = next(iterator, None)
    if first is None:
        return iter([])
    if not isinstance(first, pa.RecordBatch):
        raise ContractError(f"load {ARROW_ONLY}; recebido {type(data).__name__}")
    return itertools.chain([first], iterator)


def checked_batches(data: pa.RecordBatch | pa.Table) -> list[pa.RecordBatch]:
    """Os lotes de um ``RecordBatch`` ou de uma ``pa.Table``; protegida, para o ``write`` dos
    loaders. Outro tipo é ``ContractError``."""
    if isinstance(data, pa.Table):
        return data.to_batches()
    if isinstance(data, pa.RecordBatch):
        return [data]
    raise ContractError(f"loader.write {ARROW_ONLY}; recebido {type(data).__name__}")


def take(source: queue.Queue, stop: threading.Event) -> object | None:
    """O próximo item da fila, esperando em fatias de 50 ms, para quem lê perceber o ``stop`` em
    vez de ficar preso num ``get`` sem fim; protegida, para as threads dos motores. ``None`` quando
    ``stop`` chega com a fila vazia; nenhum item da fila é ``None``."""
    while True:
        try:
            return source.get(timeout=0.05)
        except queue.Empty:
            pass
        if stop.is_set():
            return None
