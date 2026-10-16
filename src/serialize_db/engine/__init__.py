"""[public] Os motores da execução e a interface comum a eles.

Um motor é o objeto que executa os comandos no sandbox, o banco de trabalho de uma execução.
``Engine`` é o protocolo que ``serialize_db.execution`` usa, e os dois motores têm a mesma forma:
``serialize_db.engine.duckdb`` e ``serialize_db.engine.redshift``.

Cada motor guarda uma sessão por execução sob um ``threading.RLock``. Cada primitiva, um método do
motor que faz um passo do pipeline, toma o lock pelo tempo do seu comando e o solta. ``session()``
dá a conexão crua com o lock tomado pelo bloco, reentrante na mesma thread. ``new_session()`` abre
uma sessão a mais sobre o mesmo banco, para o que roda em paralelo.

Os dados cruzam a fronteira entre o cliente e o motor em lotes Arrow:

- ``stream`` devolve um ``BatchStream``, os lotes ``pa.RecordBatch`` da consulta, na ordem dela.
- ``query`` devolve a ``pa.Table`` inteira que ``stream`` montaria.
- ``appender`` recebe lotes por ``write`` e os acrescenta a uma tabela do sandbox.
- ``append`` entrega ao ``appender`` os lotes de uma ``pa.Table``, de um ``RecordBatch``, de um
  ``RecordBatchReader`` ou de um iterável.

A tabela que recebe os lotes vem do ``ingest`` com ``materialize=True`` ou de ``create_table``,
que cria a tabela vazia do modelo.

Exemplo, com o motor DuckDB:

.. code-block:: python

    from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine

    with DuckDBEngine(DuckDBConfig(), "exec-2026-09-05", storage) as engine:
        engine.ingest(Operacao.__table__, uri, version, partitions=["2026-08-31"])
        engine.create_table(Projetado.__table__)
        with engine.stream(sa.select(Operacao)) as stream, \\
                engine.appender(Projetado.__table__) as appender:
            for batch in stream:
                appender.write(project(batch))
"""

from __future__ import annotations

import contextlib
import dataclasses
import itertools
import queue
import threading
from collections.abc import Callable, Collection, Iterable, Iterator, Mapping, Sequence
from typing import Protocol, runtime_checkable

import pyarrow as pa
import sqlalchemy as sa

from serialize_db import audit, delta, sql
from serialize_db.audit import AuditReport, CheckResult, KeyScope
from serialize_db.errors import ContractError
from serialize_db.schema import Dialect, sequential_key
from serialize_db.storage import Storage

__all__ = ["Appender", "BatchStream", "Engine", "duckdb", "redshift"]

# A mensagem que recusa o que não é Arrow e aponta a conversão sem cópia.
ARROW_ONLY = (
    "recebe pa.Table, pa.RecordBatch, pa.RecordBatchReader ou um iterável de lotes; um "
    "DataFrame vira pa.Table.from_pandas(frame, preserve_index=False) ou "
    "pa.RecordBatch.from_pandas(frame, preserve_index=False)"
)


class BatchStream(Protocol):
    """[public] Os lotes de uma consulta, lidos na ordem dela enquanto ela roda.

    O protocolo não é instanciado. A assinatura ``(*args, **kwargs)`` da classe é a do
    ``__init__`` que ``typing.Protocol`` dá a todo protocolo.

    Exemplo:

    .. code-block:: python

        with engine.stream(sa.select(Lancamento)) as stream:
            stream.schema.names   # as colunas, antes do primeiro lote
            for batch in stream:
                work(batch)
    """

    schema: pa.Schema
    """[public] O esquema Arrow dos lotes, conhecido na abertura, antes do primeiro lote."""

    def read_next_batch(self) -> pa.RecordBatch:
        """[public] O próximo lote, na ordem da consulta.

        O erro que a consulta ou a leitura encontra sobe depois do último lote entregue.

        Exemplo:

        .. code-block:: python

            first = stream.read_next_batch()

        :return: o lote, no esquema ``schema``.
        :raises StopIteration: no fim dos lotes.
        """

    def __iter__(self) -> Iterator[pa.RecordBatch]: ...

    def read_all(self) -> pa.Table:
        """[public] Os lotes que faltam numa ``pa.Table``.

        Exemplo:

        .. code-block:: python

            rest = stream.read_all()

        :return: a tabela, no esquema ``schema``; vazia depois do último lote.
        """

    def close(self) -> None:
        """[public] Para a consulta ou a leitura que ainda roda e apaga os arquivos do stream. A
        sessão continua usável.

        Exemplo:

        .. code-block:: python

            stream.close()   # o with do stream chama close na saída
        """

    def __enter__(self) -> BatchStream: ...
    def __exit__(
        self,
        *exc: object,
    ) -> None: ...
    def __arrow_c_stream__(
        self,
        requested_schema: object = None,
    ) -> object: ...


class Appender(Protocol):
    """[public] O acréscimo em lotes a uma tabela do sandbox, inserido no ``close``.

    O protocolo não é instanciado. A assinatura ``(*args, **kwargs)`` da classe é a do
    ``__init__`` que ``typing.Protocol`` dá a todo protocolo.

    Exemplo:

    .. code-block:: python

        with engine.appender(Projetado.__table__) as appender:
            appender.write(batch)
        appender.rows   # as linhas acrescentadas
    """

    rows: int
    """[public] As linhas gravadas até agora. Depois de um ``close`` sem erro, as linhas
    acrescentadas à tabela."""

    def write(
        self,
        data: pa.RecordBatch | pa.Table,
    ) -> None:
        """[public] Converte os lotes pelo contrato, na thread de quem chama, e os põe na fila da
        gravação.

        O contrato é o modelo do cliente, e a conversão é a de ``serialize_db.schema.cast``. A
        fila bloqueia o ``write`` quando está cheia. Depois de um ``close`` com erro, ou com a
        thread de gravação encerrada por um erro, o ``write`` levanta esse erro de novo: a
        exceção que saiu do ``with``, o lote recusado ou o erro da gravação.

        Exemplo:

        .. code-block:: python

            appender.write(batch)

        :param data: um ``pa.RecordBatch`` ou uma ``pa.Table``.
        :raises serialize_db.errors.ContractError: ``data`` de outro tipo, recusado sem afetar o
            appender, que segue recebendo lotes. Também um lote que o ``cast`` recusa, ou com
            colunas diferentes das do primeiro lote; depois de um lote assim, o ``close`` não
            insere nada.
        :raises RuntimeError: o ``write`` depois de um ``close`` que terminou sem erro.
        """

    def close(self) -> None:
        """[public] Insere na tabela os lotes gravados, numa transação: um erro não deixa linha. A
        segunda chamada não faz nada.

        Exemplo:

        .. code-block:: python

            appender.close()   # o with do appender chama close na saída

        :raises serialize_db.errors.ContractError: o lote que o ``write`` recusou; nada é inserido.
        """

    def __enter__(self) -> Appender: ...
    def __exit__(
        self,
        *exc: object,
    ) -> None: ...


@runtime_checkable
class Engine(Protocol):
    """[public] A interface dos dois motores. ``Execution`` só depende dela.

    O protocolo não é instanciado. A assinatura ``(*args, **kwargs)`` da classe é a do
    ``__init__`` que ``typing.Protocol`` dá a todo protocolo.

    Exemplo:

    .. code-block:: python

        def project_entries(engine: Engine) -> int:
            engine.create_table(Projetado.__table__)
            with (engine.stream(sa.select(Lancamento)) as stream,
                  engine.appender(Projetado.__table__) as appender):
                for batch in stream:
                    appender.write(project(batch))
            return appender.rows
    """

    execution_id: str
    """[public] O identificador da execução, na regra da partição
    (``serialize_db.schema.PARTITION_VALUE``)."""

    def session(self) -> contextlib.AbstractContextManager[object]:
        """[public] A conexão crua com o lock tomado pelo bloco.

        O lock é reentrante na mesma thread: uma primitiva chamada dentro do bloco não trava.

        Exemplo:

        .. code-block:: python

            with engine.session() as connection:
                engine.query("SELECT 1")   # a primitiva dentro do bloco não trava

        :return: o gerenciador de contexto cujo ``with`` dá a conexão do driver.
        """

    def new_session(self) -> Engine:
        """[public] Uma sessão a mais sobre o mesmo banco, com o seu próprio lock, para o que roda
        em paralelo.

        A sessão a mais vê o que a sessão principal confirmou, e não vê as tabelas temporárias
        dela.

        Exemplo:

        .. code-block:: python

            with engine.new_session() as session:
                session.ingest(Contrato.__table__, uri, 88)

        :return: o motor da sessão a mais, gerenciador de contexto; o ``cleanup`` dele fecha só
            essa sessão.
        """

    def __enter__(self) -> Engine: ...
    def __exit__(
        self,
        *exc: object,
    ) -> None: ...

    def ingest(
        self,
        table: sa.Table,
        uri: str,
        version: int | None,
        partitions: list[str] | None = None,
        materialize: bool = False,
    ) -> None:
        """[public] Leva ao sandbox as partições pedidas da versão fixada da tabela Delta, com o
        nome do modelo.

        A versão fixada é a versão da tabela Delta que a execução lê na abertura e usa até o fim.
        Uma coluna da versão que o modelo não tem, o caso de um modelo atrasado, é recusada na
        carga por cada motor à sua maneira. Só a view do motor DuckDB a traz. O modelo tem de
        acompanhar a tabela.

        Exemplo:

        .. code-block:: python

            engine.ingest(Lancamento.__table__, uri, 143, partitions=["2026-08-31"])

        :param table: a tabela do modelo, cujo nome a ingestão ocupa no sandbox.
        :param uri: a URI da pasta da tabela Delta de origem.
        :param version: a versão fixada da tabela; ``None`` diz que a tabela ainda não existe no
            Delta, e a ingestão a recusa.
        :param partitions: os valores de partição a ler, com um valor repetido contado uma vez;
            ``None`` lê todas, e a lista vazia não lê nenhuma.
        :param materialize: ``True`` copia os dados para uma tabela do sandbox. Com ``False``, o
            motor DuckDB, que lê o Delta no lugar, cria uma view; o motor Redshift carrega a
            tabela de qualquer modo.
        :raises serialize_db.errors.SandboxError: o nome ocupado no sandbox, ou a tabela que não
            existe no Delta, sem versão (``version=None``).
        :raises serialize_db.errors.ContractError: ``partitions`` numa tabela sem partição, ou um
            valor fora da regra da partição.
        """

    def pinned_delta(
        self,
        table: sa.Table,
        uri: str,
        version: int | None,
    ) -> sa.FromClause:
        """[public] A versão fixada da tabela como origem de consulta, sem ocupar o nome do modelo
        no sandbox.

        Exemplo:

        .. code-block:: python

            previous = engine.pinned_delta(Projetado.__table__, uri, 57)
            engine.query(sa.select(sa.func.max(previous.c.id_lancamento)))

        :param table: a tabela do modelo, que dá as colunas.
        :param uri: a URI da pasta da tabela Delta lida.
        :param version: a versão fixada.
        :return: o ``FromClause`` com as colunas do contrato, para os statements Core.
        :raises serialize_db.errors.SandboxError: numa tabela que ainda não existe, sem versão
            (``version=None``).
        """

    def stream(
        self,
        statement_or_sql: sa.sql.ClauseElement | str,
        params: Mapping[str, object] | None = None,
        batch_size: int = 100_000,
    ) -> BatchStream:
        """[public] Os lotes da consulta, lidos na ordem dela enquanto o cliente trabalha no lote
        anterior.

        Exemplo:

        .. code-block:: python

            with engine.stream(sa.select(Lancamento), batch_size=50_000) as stream:
                for batch in stream:
                    work(batch)

        :param statement_or_sql: um statement Core sobre as tabelas do modelo, que o motor
            compila para o sandbox, ou um texto pronto no SQL do motor, com cada parâmetro
            marcado como ``:nome``.
        :param params: os valores dos parâmetros, por nome, para os ``bindparam`` sem valor do
            statement ou para os marcadores do texto.
        :param batch_size: o máximo de linhas de cada lote.
        :return: o ``BatchStream`` dos lotes, gerenciador de contexto; o ``close`` apaga os
            arquivos dele.
        :raises serialize_db.errors.SqlError: os nomes de ``params`` não fecham com os parâmetros do
            statement ou do texto.
        """

    def query(
        self,
        statement_or_sql: sa.sql.ClauseElement | str,
        params: Mapping[str, object] | None = None,
    ) -> pa.Table:
        """[public] O resultado inteiro como ``pa.Table``, sob o lock.

        Exemplo:

        .. code-block:: python

            engine.query(sa.select(sa.func.count()).select_from(Lancamento.__table__))

        :param statement_or_sql: um statement Core sobre as tabelas do modelo, que o motor
            compila para o sandbox, ou um texto pronto no SQL do motor, com cada parâmetro
            marcado como ``:nome``.
        :param params: os valores dos parâmetros, por nome, para os ``bindparam`` sem valor do
            statement ou para os marcadores do texto.
        :return: a tabela do resultado.
        :raises serialize_db.errors.SqlError: os nomes de ``params`` não fecham com os parâmetros do
            statement ou do texto.
        """

    def create_table(
        self,
        table: sa.Table,
    ) -> None:
        """[public] Cria no sandbox a tabela vazia do modelo, pelo DDL do motor, para os dados que
        não vêm do ``ingest``.

        Exemplo:

        .. code-block:: python

            engine.create_table(Projetado.__table__)

        :param table: a tabela do modelo, cujo nome não pode estar ocupado no sandbox.
        :raises serialize_db.errors.SandboxError: o nome que o ``ingest`` ou outro ``create_table``
            ocupou.
        """

    def appender(
        self,
        table: sa.Table,
        queue_depth: int = 2,
    ) -> Appender:
        """[public] O gerenciador de contexto que grava lotes numa tabela do sandbox e os insere no
        ``close``.

        A tabela vem do ``ingest`` com ``materialize=True`` ou de ``create_table``. Dois appenders
        na mesma tabela, ao mesmo tempo, entram os dois com todas as suas linhas, cada um numa
        sessão de ``new_session()`` ou os dois na sessão principal, onde um ``close`` espera o
        outro sob o lock. Um appender e um ``UPDATE`` das linhas que a tabela já tinha, ao mesmo
        tempo, também entram os dois; no Redshift, sob o isolamento de snapshot.

        Exemplo:

        .. code-block:: python

            with engine.appender(Projetado.__table__) as appender:
                appender.write(batch)

        :param table: a tabela do modelo, já criada no sandbox.
        :param queue_depth: os lotes convertidos que esperam a thread de gravação; com a fila
            cheia, o ``write`` bloqueia.
        :return: o ``Appender`` da tabela, que guarda os lotes num arquivo até o ``close``.
        :raises serialize_db.errors.SandboxError: a tabela que não existe no sandbox, ou o nome que
            uma view do ``ingest`` ocupa.
        """

    def append(
        self,
        table: sa.Table,
        data: pa.Table | pa.RecordBatch | pa.RecordBatchReader | Iterable[pa.RecordBatch],
    ) -> int:
        """[public] Acrescenta os lotes a uma tabela do sandbox pelo ``appender``.

        Dois ``append`` na mesma tabela ao mesmo tempo, ou um ``append`` e um ``UPDATE`` dela,
        entram os dois, como no ``appender``.

        Exemplo:

        .. code-block:: python

            engine.append(Projetado.__table__, pa.Table.from_pandas(frame, preserve_index=False))

        :param table: a tabela do modelo, já criada no sandbox.
        :param data: uma ``pa.Table``, um ``pa.RecordBatch``, um ``pa.RecordBatchReader`` ou um
            iterável de ``pa.RecordBatch``.
        :return: as linhas acrescentadas.
        :raises serialize_db.errors.ContractError: um DataFrame, recusado com a conversão sem cópia
            na mensagem, ou outro tipo em ``data``; ou um lote que o ``cast`` recusa, e nada é
            inserido.
        :raises serialize_db.errors.SandboxError: a tabela que não existe no sandbox, ou o nome que
            uma view do ``ingest`` ocupa.
        """

    def audit(
        self,
        table: sa.Table,
        partitions: list[str] | None,
        uri: str | None = None,
        version: int | None = None,
        foreign_keys: bool = False,
        key_scope: KeyScope | None = None,
        referenced: Mapping[str, tuple[str, int]] | None = None,
    ) -> AuditReport:
        """[public] Roda as verificações do contrato sobre a tabela do sandbox.

        As verificações são as que ``serialize_db.audit`` deriva do modelo, e o resultado é o
        ``AuditReport``.

        Exemplo:

        .. code-block:: python

            report = engine.audit(Projetado.__table__, ["2026-08-31"], uri, 57)
            report.passed

        :param table: a tabela do modelo, no sandbox.
        :param partitions: as partições da execução; ``None`` audita a tabela inteira do
            sandbox.
        :param uri: a URI da tabela Delta da execução. Com ``version``, a verificação
            ``chave_<colunas>_tabela`` compara as chaves das partições da execução com as das
            demais partições dessa versão. Essa verificação é dispensada sem rodar quando o menor
            valor da chave primária inteira de uma coluna na execução passa do maior registrado
            no log da versão.
        :param version: a versão fixada da tabela; sem ela, ou sem ``uri``, a verificação
            ``chave_<colunas>_tabela`` não roda.
        :param foreign_keys: ``True`` confere as chaves estrangeiras, contra a tabela
            referenciada do sandbox ou contra a versão de ``referenced``.
        :param key_scope: o escopo da unicidade; ``"partition"`` suprime a verificação
            ``chave_<colunas>_tabela``, e ``"table"`` a confere também na chave com a coluna
            de ``partition_source``.
        :param referenced: por tabela, a URI e a versão fixada da tabela referenciada que o
            sandbox não tem, para as chaves estrangeiras com ``foreign_keys=True``.
        :return: o ``AuditReport``. A reprovação não levanta aqui: ``passed`` é falso, e
            ``serialize_db.execution.Execution.audit`` levanta ``serialize_db.errors.AuditFailed``.
        :raises serialize_db.errors.ContractError: um valor de ``partitions`` fora da regra da
            partição.
        """

    def export_partition(
        self,
        table: sa.Table,
        uri: str,
        value: str | None,
        metadata: Mapping[str, str],
        expected_rows: int | None = None,
        columns_without_min_max: Collection[str] = (),
    ) -> int:
        """[public] Leva a partição do sandbox ao Delta num commit.

        O motor grava a partição em arquivos novos na pasta dela na tabela Delta, e o commit os
        registra no log da tabela.

        Exemplo:

        .. code-block:: python

            engine.export_partition(Projetado.__table__, uri, "2026-08-31",
                                    delta.commit_metadata("exec-42", versions), expected_rows=1000)

        :param table: a tabela do modelo, no sandbox.
        :param uri: a URI da pasta da tabela Delta de destino, sob a raiz do armazenamento.
        :param value: o valor da partição; ``None`` numa tabela sem partição, que sai inteira.
        :param metadata: os metadados do commit, de ``serialize_db.delta.commit_metadata``.
        :param expected_rows: a contagem de linhas da auditoria, conferida contra as linhas dos
            arquivos registrados; sem ela, vale a contagem do sandbox.
        :param columns_without_min_max: as colunas ``Double`` com valor não finito na partição,
            que saem sem mínimo e máximo (issue #59 do serialize-db).
        :return: a versão do commit.
        :raises serialize_db.errors.ContractError: o valor fora da regra da partição, ``None`` numa
            tabela particionada, ou um valor numa tabela sem partição.
        :raises serialize_db.errors.RegistrationRefused: uma conferência dos arquivos reprovou, sem
            commit, e os arquivos gravados ficam órfãos na pasta da partição até um
            ``vacuum_keeping_snapshots(..., apply=True, full=True)`` depois da retenção; ou a
            releitura desfez o commit.
        :raises serialize_db.errors.ExecutionConflict: outro commit na mesma partição a partir da
            mesma versão.
        :raises ValueError: ``uri`` fora da raiz do armazenamento.
        :raises serialize_db.errors.SandboxError: a memória que o processo ainda pode usar abaixo de
            2 MiB, ou negativa (``serialize_db.resources.environment_limits``), lida antes do
            commit. O commit não acontece, e os arquivos gravados ficam órfãos na pasta da
            partição até um ``vacuum_keeping_snapshots(..., apply=True, full=True)`` depois da
            retenção; na troca do motor Redshift, ficam no ``staging_prefix`` até o ``cleanup``.
        """

    def cleanup(self) -> None:
        """[public] Fecha a sessão e apaga o que a execução criou no sandbox. Numa sessão a mais,
        fecha só ela. A segunda chamada não faz nada.

        Exemplo:

        .. code-block:: python

            engine.cleanup()   # o with do motor chama cleanup na saída
        """


# ---------------------------------------------------------------- os lotes e as filas dos motores


def batches_of(
    data: object,
) -> Iterator[pa.RecordBatch]:
    """[protected] Os lotes de uma ``pa.Table``, de um lote, de um leitor ou de um iterável de
    lotes; para o ``append`` dos motores.

    Outro tipo, um DataFrame inclusive, é ``ContractError`` antes de qualquer gravação."""
    if isinstance(data, pa.Table):
        return iter(data.to_batches())
    if isinstance(data, pa.RecordBatch):
        return iter([data])
    if isinstance(data, pa.RecordBatchReader):
        return iter(data)
    if isinstance(data, (str, bytes)) or not isinstance(data, Iterable):
        raise ContractError(f"append {ARROW_ONLY}; recebido {type(data).__name__}")
    # O primeiro item decide se o iterável é de lotes; ele volta à frente dos demais.
    iterator = iter(data)
    first = next(iterator, None)
    if first is None:
        return iter([])
    if not isinstance(first, pa.RecordBatch):
        raise ContractError(f"append {ARROW_ONLY}; recebido {type(data).__name__}")
    return itertools.chain([first], iterator)


def checked_batches(
    data: pa.RecordBatch | pa.Table,
) -> list[pa.RecordBatch]:
    """[protected] Os lotes de um ``RecordBatch`` ou de uma ``pa.Table``; para o ``write`` dos
    appenders.

    Outro tipo é ``ContractError``."""
    if isinstance(data, pa.Table):
        return data.to_batches()
    if isinstance(data, pa.RecordBatch):
        return [data]
    raise ContractError(f"appender.write {ARROW_ONLY}; recebido {type(data).__name__}")


def take(
    source: queue.Queue,
    stop: threading.Event,
) -> object | None:
    """[protected] O próximo item da fila; para as threads dos motores.

    A espera vai em fatias de 50 ms, para quem lê perceber o ``stop`` em vez de ficar preso num
    ``get`` sem fim. Devolve ``None`` quando ``stop`` chega com a fila vazia; nenhum item da fila
    é ``None``."""
    while True:
        try:
            return source.get(timeout=0.05)
        except queue.Empty:
            pass
        if stop.is_set():
            return None


# -------------------------------------------------------------------- a auditoria dos dois motores


@dataclasses.dataclass(frozen=True)
class AuditSupport:
    """[protected] O que ``audit_report`` pede ao motor que roda a auditoria; para o ``audit`` dos
    dois motores, que o montam com os seus próprios métodos."""

    dialect: Dialect
    """[protected] O dialeto em que ``serialize_db.sql.render`` escreve o texto das verificações."""
    prefix: str
    """[protected] O prefixo das tabelas do sandbox, no texto das verificações e nos nomes que
    ``name_in_use`` confere; vazio no DuckDB."""
    storage: Storage
    """[protected] O armazenamento onde o log da versão fixada é lido, para o ``max_key``."""
    query: Callable[[str], pa.Table]
    """[protected] A consulta do motor: a ``pa.Table`` de um texto SQL."""
    run_check: Callable[[str], pa.Table]
    """[protected] A consulta de uma verificação de chave ou de órfão, com o que o texto cita
    carregado antes. No Redshift, carrega a staging ``_versao``; no DuckDB, é a própria
    ``query``."""
    name_in_use: Callable[[str], bool]
    """[protected] Se o nome, já com o prefixo, está ocupado no sandbox."""
    pinned_source: Callable[[sa.Table, str, int], sa.FromClause]
    """[protected] A versão fixada da tabela como origem de consulta das verificações."""


def audit_report(
    support: AuditSupport,
    table: sa.Table,
    partitions: list[str] | None,
    uri: str | None,
    version: int | None,
    foreign_keys: bool,
    key_scope: KeyScope | None,
    referenced: Mapping[str, tuple[str, int]] | None,
) -> AuditReport:
    """[protected] Roda as verificações do contrato sobre a tabela do sandbox e monta o
    ``AuditReport``; para o ``audit`` dos dois motores.

    Os dois motores recebem os mesmos argumentos e dizem nas suas docstrings o que cada um
    levanta. Com ``uri`` e ``version``, a versão fixada entra pelo ``pinned_source`` de
    ``support``, e o ``max_key`` dela dispensa a verificação da chave da tabela quando o menor
    valor da execução passa dele. Um valor de ``partitions`` fora da regra da partição é
    ``ContractError``."""
    pinned = None
    pinned_max_key = None
    if uri is not None and version is not None:
        pinned = support.pinned_source(table, uri, version)
        pinned_max_key = _pinned_max_key(support, table, uri, version)
    sources = _referenced_sources(support, table, foreign_keys, referenced)
    found, not_run = audit.checks_and_not_run(
        table, partitions, foreign_keys, key_scope, pinned, sources, pinned_max_key
    )
    results = []
    totals: dict = {}
    nonfinite: dict = {}
    for check in found:
        if check.name == "linhas":
            result, totals, nonfinite = _rows_result(support, table, check, partitions)
        else:
            result = _check_result(support, table, check)
        results.append(result)
    return AuditReport(
        table=table.name,
        partitions=tuple(partitions) if partitions is not None else None,
        results=tuple(results),
        not_run=tuple(not_run),
        nonfinite_columns=nonfinite,
        totals=totals,
    )


def _pinned_max_key(
    support: AuditSupport,
    table: sa.Table,
    uri: str,
    version: int,
) -> int | None:
    """[private] O ``max_key`` da versão fixada na chave sequencial; ``None`` numa tabela sem
    ela."""
    key = sequential_key(table)
    if key is None:
        return None
    return delta.max_key(delta.open_table(uri, support.storage, version), key.name)


def _referenced_sources(
    support: AuditSupport,
    table: sa.Table,
    foreign_keys: bool,
    referenced: Mapping[str, tuple[str, int]] | None,
) -> dict[str, sa.FromClause]:
    """[private] A origem da linha referenciada de cada chave estrangeira: a tabela do sandbox com
    o nome do modelo, ou a versão fixada informada em ``referenced``."""
    sources: dict[str, sa.FromClause] = {}
    if not foreign_keys:
        return sources
    for constraint in table.foreign_key_constraints:
        target = constraint.referred_table
        if support.name_in_use(support.prefix + target.name):
            sources[target.name] = target
        elif referenced is not None and target.name in referenced:
            uri, version = referenced[target.name]
            sources[target.name] = support.pinned_source(target, uri, version)
    return sources


def _check_text(
    support: AuditSupport,
    statement: sa.sql.ClauseElement,
    table: sa.Table,
) -> str:
    """[private] O texto de uma verificação no dialeto do motor, sobre as tabelas do sandbox."""
    return sql.render(statement, support.dialect, table.metadata, prefix=support.prefix)


def _rows_result(
    support: AuditSupport,
    table: sa.Table,
    check: audit.Check,
    partitions: Sequence[str] | None,
) -> tuple[CheckResult, dict, dict]:
    """[private] A verificação de linhas: o resultado, as leituras por partição e os não finitos."""
    text = _check_text(support, check.statement, table)
    rows = support.query(text).to_pylist()
    totals, nonfinite = audit.readings_by_partition(rows, table)
    defects, failing = audit.failing_counters(check.counters, totals)
    sample = _rows_sample(support, table, check, partitions, failing)
    return CheckResult(check.name, text, defects, sample, defects == 0), totals, nonfinite


def _rows_sample(
    support: AuditSupport,
    table: sa.Table,
    check: audit.Check,
    partitions: Sequence[str] | None,
    failing: list[str],
) -> pa.Table:
    """[private] Até 20 linhas inteiras dos contadores reprovados, uma consulta por contador."""
    samples = []
    for label in failing:
        statement = audit.sample_statement(table, partitions, check.counters[label])
        samples.append(support.query(_check_text(support, statement, table)))
    if not samples:
        return pa.table({})
    return pa.concat_tables(samples).slice(0, audit.SAMPLE_ROWS)


def _check_result(
    support: AuditSupport,
    table: sa.Table,
    check: audit.Check,
) -> CheckResult:
    """[private] Uma verificação de chave ou de órfão. O ``skip_when`` verdadeiro a aprova sem
    rodá-la, e ``run_check`` carrega o que ela cita só quando ela roda."""
    text = _check_text(support, check.statement, table)
    if check.skip_when is not None:
        skip_text = _check_text(support, check.skip_when, table)
        skipped = support.query(skip_text).column(0)[0].as_py()
        if skipped is True:
            reason = "dispensada: o menor valor da execução passa do maior da versão fixada"
            return CheckResult(check.name, text, 0, pa.table({}), True, reason)
    found = support.run_check(text)
    return CheckResult(
        check.name, text, found.num_rows, found.slice(0, audit.SAMPLE_ROWS), found.num_rows == 0
    )
