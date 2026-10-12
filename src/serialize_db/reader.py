"""[public] O acesso de leitura à base, com o modelo: o leitor Delta e o leitor Redshift.

``DeltaReader`` abre um DuckDB no processo do cliente com uma view por tabela do modelo, sobre a
versão de um snapshot da base Delta. ``RedshiftReader`` roda o mesmo statement nas tabelas
publicadas ``<ambiente>_<tabela>`` do esquema do Redshift. O time que tem o ``Database`` entra por
``db.open_delta()`` e ``db.open_redshift()``. O cliente que só enxerga o Redshift entra por
``open_redshift``, sem a raiz Delta. O resultado é Arrow, como nos motores: ``query`` devolve a
``pa.Table``, que ``to_pandas(types_mapper=pd.ArrowDtype)`` leva ao pandas, e ``stream`` entrega
os lotes.

Um snapshot é uma entrada do arquivo de controle do ambiente com a versão de cada tabela num
instante, sob um nome que não muda; um canal é um nome que aponta para um snapshot. O leitor
Delta lê, sem argumento, o snapshot do canal ``default`` do ambiente, que ``serialize-db channel``
move. ``snapshot=`` lê um snapshot pelo nome; o snapshot arquivado é lido pela cópia em
``<raiz>/<ambiente>/arquivo/<nome>/``. ``channel="current"`` lê a versão atual de cada tabela.
Cada view fica presa à versão lida na abertura, e a leitura entre tabelas é consistente num
snapshot. ``materialize`` copia uma tabela, ou parte das partições dela, para o banco local do
leitor. O filtro das partições é ``BETWEEN`` o menor e o maior valor ``AND`` o ``IN`` da lista,
como no ``ingest`` do motor DuckDB, porque o ``delta_scan`` do DuckDB poda as partições por ``=`` e
por intervalo, isto é, abre só os arquivos das partições que o filtro pede, e abre todos os
arquivos com um ``IN`` de mais de um valor sozinho.

O leitor Redshift lê as tabelas que ``serialize_db.publication`` publica, cada uma na sua
transação: uma consulta que junta duas tabelas durante uma publicação pode ler versões
diferentes.

Exemplo:

.. code-block:: python

    import pandas as pd
    import sqlalchemy as sa

    from serialize_db import Database
    from serialize_db.reader import open_redshift

    db = Database("s3://bucket/projeto/delta", "prd", Base.metadata)
    statement = sa.select(Lancamento).where(Lancamento.data_base_str == "2026-08-31")
    with db.open_delta() as reader:                      # o snapshot do canal default
        reader.materialize(Lancamento.__table__, partitions=["2026-08-31"])
        frame = reader.query(statement).to_pandas(types_mapper=pd.ArrowDtype)
    with open_redshift(Base.metadata, "prd") as reader:  # o cliente sem S3: só query
        frame = reader.query(statement).to_pandas(types_mapper=pd.ArrowDtype)
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import functools
import logging
import threading
import uuid
import weakref
from collections.abc import Iterator, Mapping
from typing import TYPE_CHECKING

import pyarrow as pa
import sqlalchemy as sa

from serialize_db import delta, sql
from serialize_db._pool import run_in_pool
from serialize_db.engine import BatchStream
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine, delta_scan
from serialize_db.errors import ContractError
from serialize_db.schema import check_partition_value, quoted
from serialize_db.storage import Storage

if TYPE_CHECKING:
    import duckdb

    from serialize_db.engine.redshift import RedshiftConfig
    from serialize_db.execution import Database

__all__ = ["DeltaReader", "ReaderStream", "RedshiftReader", "open_redshift"]

log = logging.getLogger("serialize_db.reader")


def _new_reader_id() -> str:
    """[private] Um identificador de leitor novo, ``reader-<AAAA-MM-DD>-<uuid8>``, com a data em
    UTC."""
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    return f"reader-{today}-{uuid.uuid4().hex[:8]}"


def _check_select(
    statement_or_sql: sa.sql.ClauseElement | str,
) -> None:
    """[private] A regra de leitura comum aos dois leitores: um statement Core que não é ``Select``
    nem ``CompoundSelect`` é ``ContractError`` antes de chamar o motor. Um texto SQL pronto passa
    como está."""
    if isinstance(statement_or_sql, str):
        return
    if not isinstance(statement_or_sql, (sa.Select, sa.CompoundSelect)):
        raise ContractError(
            f"o leitor roda só Select e CompoundSelect, e recebeu "
            f"{type(statement_or_sql).__name__}; um comando vai pela conexão de session()"
        )


# ---------------------------------------------------------------- as versões do leitor Delta


@dataclasses.dataclass(frozen=True)
class _Source:
    """[private] De onde o leitor Delta lê: o snapshot (``None`` no canal ``current``), a versão e a
    URI de cada tabela do modelo que ganha view, e o texto da origem para as mensagens e o
    log."""

    snapshot: str | None
    versions: dict[str, int]
    uris: dict[str, str]
    description: str


def _current_source(
    delta_db: Database,
) -> _Source:
    """[private] A origem do canal ``current``: a versão atual de cada tabela do modelo que existe
    no ambiente, lida como ``Execution`` a lê na abertura."""
    versions = {}
    uris = {}
    for table in delta_db.tables():
        uri = delta_db.uri(table)
        if delta.table_exists(uri, delta_db.storage):
            versions[table.name] = delta.open_table(uri, delta_db.storage).version()
            uris[table.name] = uri
    return _Source(None, versions, uris, f"a versão atual do ambiente {delta_db.environment}")


def _snapshot_source(
    delta_db: Database,
    name: str,
    entry: Mapping[str, int],
) -> _Source:
    """[private] A origem de um snapshot vivo: a versão da entrada ``entry`` de cada tabela do
    modelo que a entrada tem, na pasta da tabela no ambiente."""
    versions = {}
    uris = {}
    for table in delta_db.tables():
        if table.name in entry:
            versions[table.name] = entry[table.name]
            uris[table.name] = delta_db.uri(table)
    return _Source(name, versions, uris, f"o snapshot {name}")


def _archived_source(
    delta_db: Database,
    name: str,
    entry: Mapping[str, int],
) -> _Source:
    """[private] A origem de um snapshot arquivado: a cópia em ``arquivo/<nome>/<tabela>`` de cada
    tabela do modelo que a entrada arquivada tem, na versão atual da cópia, que não muda depois
    do ``archive``."""
    storage = delta_db.storage
    versions = {}
    uris = {}
    for table in delta_db.tables():
        if table.name not in entry:
            continue
        uri = storage.uri_of(storage.join(delta_db.archive_prefix(name), table.name))
        versions[table.name] = delta.open_table(uri, storage).version()
        uris[table.name] = uri
    return _Source(name, versions, uris, f"o snapshot arquivado {name}")


def _resolve_source(
    delta_db: Database,
    snapshot: str | None,
    channel: str | None,
) -> _Source:
    """[private] A origem pelo modo pedido: o canal ``current``, o snapshot pelo nome, vivo ou
    arquivado, ou o snapshot do canal, ``default`` sem argumento. ``snapshot`` e ``channel``
    juntos são ``ContractError``."""
    if snapshot is not None and channel is not None:
        raise ContractError(
            f"open_delta recebe snapshot={snapshot!r} ou channel={channel!r}, não os dois"
        )
    if channel == delta.CURRENT_CHANNEL:
        return _current_source(delta_db)
    control, _ = delta.read_snapshots(delta_db.storage, delta_db.environment)
    if snapshot is None:
        snapshot = delta.channel_snapshot(control, channel or delta.DEFAULT_CHANNEL)
    archived = control.get("archived", {})
    if snapshot in archived:
        return _archived_source(delta_db, snapshot, archived[snapshot])
    return _snapshot_source(delta_db, snapshot, delta.snapshot_versions(control, snapshot))


# ---------------------------------------------------------------- o stream do leitor Delta


class ReaderStream:
    """[public] Os lotes de uma consulta do leitor Delta: o ``BatchStream`` do motor com o leitor
    preso até o ``close``.

    ``DeltaReader.stream`` devolve este stream, que segura o leitor enquanto está aberto. Assim,
    um leitor fora de uma variável, como em ``db.open_delta().stream(...)``, não é coletado
    enquanto o stream está aberto, e o finalizador do leitor não fecha o motor debaixo da
    consulta. Os lotes, o esquema, o ``for``, o ``with`` e ``__arrow_c_stream__`` são os do
    stream do motor (``DuckDBStream``).

    Exemplo:

    .. code-block:: python

        with reader.stream(sa.select(Lancamento.__table__)) as stream:
            first = stream.read_next_batch()
            rest = stream.read_all()
    """

    schema: pa.Schema
    """[public] O esquema Arrow dos lotes, conhecido na abertura, antes do primeiro lote."""

    def __init__(
        self,
        reader: DeltaReader,
        stream: BatchStream,
    ) -> None:
        """[public] Embrulha o stream do motor e segura o leitor até o ``close``;
        ``DeltaReader.stream`` o constrói.

        :param reader: o leitor Delta que abriu a consulta, solto no ``close``.
        :param stream: o stream do motor DuckDB do leitor, aberto por ``DuckDBEngine.stream``.
        """
        self._reader: DeltaReader | None = reader
        self._stream = stream
        self.schema = stream.schema

    def read_next_batch(self) -> pa.RecordBatch:
        """[public] O próximo lote, na ordem da consulta.

        Exemplo:

        .. code-block:: python

            with reader.stream(sa.select(Lancamento.__table__)) as stream:
                first = stream.read_next_batch()

        :return: o lote, no esquema ``schema``.
        :raises StopIteration: no fim dos lotes.
        :raises OSError: o erro que a consulta encontra depois do último lote entregue, levantado
            pelo leitor Arrow com a mensagem do DuckDB.
        """
        return self._stream.read_next_batch()

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        # O gerador segura este stream, e com ele o leitor, enquanto o laço o percorre.
        yield from self._stream

    def read_all(self) -> pa.Table:
        """[public] Os lotes que faltam numa ``pa.Table``.

        Exemplo:

        .. code-block:: python

            with reader.stream(sa.select(Lancamento.__table__)) as stream:
                table = stream.read_all()

        :return: a tabela, no esquema ``schema``; vazia depois do último lote.
        :raises OSError: o erro que a consulta encontra depois do último lote entregue, levantado
            pelo leitor Arrow com a mensagem do DuckDB.
        """
        return self._stream.read_all()

    def __arrow_c_stream__(
        self,
        requested_schema: object = None,
    ) -> object:
        # O leitor Arrow puxa o gerador deste stream, que segura o leitor até o último lote.
        batches = pa.RecordBatchReader.from_batches(self.schema, iter(self))
        return batches.__arrow_c_stream__(requested_schema)

    def close(self) -> None:
        """[public] Fecha o stream do motor e solta o leitor. Quando nada mais segura o leitor, a
        coleta de lixo do Python o fecha.

        Exemplo:

        .. code-block:: python

            stream = reader.stream(sa.select(Lancamento.__table__))
            try:
                first = stream.read_next_batch()
            finally:
                stream.close()   # o with do stream chama close na saída
        """
        self._stream.close()
        self._reader = None

    def __enter__(self) -> ReaderStream:
        return self

    def __exit__(
        self,
        *exc: object,
    ) -> None:
        self.close()


# ---------------------------------------------------------------- o leitor Delta


class DeltaReader:
    """[public] O leitor da base Delta: um DuckDB no processo do cliente com uma view por tabela do
    modelo, presa à versão do snapshot lido.

    ``db.open_delta`` é a entrada. O leitor lê um snapshot, a versão de cada tabela do ambiente
    sob um nome, ou a versão atual de cada tabela pelo canal ``current``; ``versions`` diz a
    versão de cada view. ``query`` e ``stream`` rodam um statement Core sobre as views,
    ``materialize`` copia tabelas para o banco local, e ``close`` apaga o banco temporário.

    Exemplo:

    .. code-block:: python

        with db.open_delta(channel="current") as reader:
            reader.versions                       # {"cad_contas": 1, "cad_lancamentos": 143}
            reader.query(sa.select(sa.func.count()).select_from(Lancamento.__table__))
    """

    def __init__(
        self,
        delta_db: Database,
        snapshot: str | None = None,
        channel: str | None = None,
        config: DuckDBConfig | None = None,
    ) -> None:
        """[public] Lê as versões, abre o motor e cria as views, as tabelas em paralelo.
        ``db.open_delta`` é a entrada.

        :param delta_db: o banco Delta lido, com a raiz, o ambiente e os modelos.
        :param snapshot: o nome de um snapshot do ambiente. O snapshot arquivado é lido pela
            cópia em ``<raiz>/<ambiente>/arquivo/<nome>/``. ``None`` lê o canal.
        :param channel: o canal do ambiente. ``"default"`` é o mesmo que sem argumento, e
            ``"current"`` é a versão atual de cada tabela do modelo que existe no ambiente.
            ``None`` sem ``snapshot`` lê o canal ``default``.
        :param config: a configuração do DuckDB. ``None`` é ``DuckDBConfig()``, com os limites
            lidos do ambiente e uma pasta temporária nova, apagada no ``close``.
        :raises serialize_db.errors.ContractError: ``snapshot`` e ``channel`` juntos; o ambiente sem
            o canal, com o ``serialize-db channel`` que o cria e o canal ``current`` na mensagem; ou
            o snapshot que não existe.
        :raises duckdb.Error: a extensão ``delta`` ausente da pasta configurada, ou uma versão
            do snapshot que a tabela não tem mais, na criação da view.
        :raises serialize_db.errors.SandboxError: sem ``memory_limit`` em ``config``, a memória que
            o processo ainda pode usar abaixo de 2 MiB, ou negativa, na abertura do DuckDB
            (``environment_limits``).
        """
        source = _resolve_source(delta_db, snapshot, channel)
        self.snapshot = source.snapshot
        """[public] O snapshot lido; ``None`` no canal ``current``."""
        self.versions = source.versions
        """[public] A versão de cada tabela com view, pelo nome da tabela. No snapshot arquivado, é
        a versão de cada cópia em ``<raiz>/<ambiente>/arquivo/<nome>/``. A tabela do modelo ausente
        do snapshot não tem view."""
        self.materialized: dict[str, list[str] | None] = {}
        """[public] As tabelas copiadas para o banco local por ``materialize``, pelo nome, com as
        partições copiadas de cada uma; ``None`` na tabela copiada inteira."""
        self.reader_id = _new_reader_id()
        """[public] O identificador do leitor, ``reader-<AAAA-MM-DD>-<uuid8>``, que nomeia o banco
        temporário e a pasta de transbordo do motor."""
        self._delta_db = delta_db
        self._uris = source.uris
        self._source = source.description
        self._lock = threading.Lock()
        self._engine = DuckDBEngine(config or DuckDBConfig(), self.reader_id, delta_db.storage)
        # O finalizador guarda o motor, não o leitor: a coleta de um leitor sem close, ou o fim
        # normal do interpretador, apaga o banco temporário e a pasta de transbordo dele.
        self._finalizer = weakref.finalize(self, self._engine.cleanup)
        try:
            self._open_views()
        except BaseException:
            self.close()
            raise
        log.info(
            "leitor %s aberto sobre %s: versões %s", self.reader_id, self._source, self.versions
        )

    def _create_view(
        self,
        table: sa.Table,
    ) -> None:
        """[private] Cria a view de uma tabela pelo ``ingest`` de uma sessão a mais do motor,
        fechada no fim: uma tarefa do pool de ``_open_views``."""
        with self._engine.new_session() as session:
            session.ingest(table, self._uris[table.name], self.versions[table.name])

    def _open_views(self) -> None:
        """[private] Cria uma view por tabela do modelo presente em ``versions``, as tabelas em
        paralelo, uma sessão a mais cada."""
        tasks = []
        for table in self._delta_db.tables():
            if table.name in self.versions:
                tasks.append((table.name, functools.partial(self._create_view, table)))
        if tasks:
            run_in_pool(tasks, max_workers=len(tasks))

    # ------------------------------------------------------------ a materialização

    def _check_view(
        self,
        name: str,
    ) -> None:
        """[private] Recusa com ``ContractError`` a tabela do modelo sem view no leitor; a mensagem
        diz a origem das versões."""
        if name not in self.versions:
            raise ContractError(
                f"{name}: a tabela do modelo não tem view no leitor, porque não "
                f"está em {self._source}"
            )

    def _kind(
        self,
        name: str,
    ) -> str:
        """[private] O tipo do objeto que a tabela ocupa no banco local, para o ``DROP``: ``TABLE``
        depois de uma materialização, ``VIEW`` antes dela."""
        with self._lock:
            return "TABLE" if name in self.materialized else "VIEW"

    def _materialize_one(
        self,
        table: sa.Table,
        where: str,
        partitions: list[str] | None,
    ) -> None:
        """[private] Troca a view da tabela, ou a tabela de uma materialização anterior, por
        ``CREATE TABLE ... AS SELECT * FROM delta_scan(...)``, numa transação de uma sessão a mais
        do motor. A falha devolve o objeto anterior pelo ``ROLLBACK``. Uma tarefa do pool de
        ``materialize``."""
        name = quoted(table.name)
        select = f"SELECT * FROM {delta_scan(self._uris[table.name], self.versions[table.name])}"
        with self._engine.new_session() as session, session.session() as connection:
            connection.begin()
            try:
                connection.execute(f"DROP {self._kind(table.name)} {name}")
                connection.execute(f"CREATE TABLE {name} AS {select}{where}")
                connection.commit()
            except BaseException:
                connection.rollback()
                raise
        with self._lock:
            self.materialized[table.name] = partitions
        log.info(
            "leitor %s: %s materializada, partições %s",
            self.reader_id,
            table.name,
            "todas" if partitions is None else partitions,
        )

    def materialize(
        self,
        *tables: sa.Table,
        partitions: list[str] | None = None,
    ) -> None:
        """[public] Copia as tabelas para o banco local do leitor: o nome do modelo passa a ser uma
        tabela com os dados da versão lida, ou só das partições pedidas.

        Cada tabela é trocada numa transação, e as tabelas correm em paralelo, cada uma numa
        sessão a mais do motor. Uma chamada seguinte troca a tabela de novo. O filtro das
        partições é o do ``ingest`` do motor DuckDB: ``BETWEEN`` o menor e o maior valor, que o
        ``delta_scan`` poda, ``AND`` o ``IN`` da lista. A cópia é o ``SELECT *`` da versão: uma
        coluna da versão que o modelo não tem entra na tabela, como na view.

        Exemplo:

        .. code-block:: python

            reader.materialize(Conta.__table__)
            reader.materialize(Lancamento.__table__, partitions=["2026-07-31", "2026-08-31"])
            reader.materialized   # {"cad_contas": None, "cad_lancamentos": ["2026-07-31", ...]}

        :param tables: as tabelas do modelo com view no leitor.
        :param partitions: os valores de partição copiados, pela regra da partição. ``None``
            copia a tabela inteira.
        :raises serialize_db.errors.ContractError: uma tabela sem view no leitor, ``partitions``
            numa tabela sem partição ou um valor fora da regra da partição, antes de qualquer troca.
        :raises duckdb.Error: a falha da cópia de uma tabela. Essa tabela fica com o objeto
            anterior, view ou tabela, enquanto as outras terminam.
        """
        filters = {}
        for table in tables:
            self._check_view(table.name)
            filters[table.name] = self._engine.partition_filter(table, partitions)
        copied = None if partitions is None else list(partitions)
        tasks = []
        for table in tables:
            task = functools.partial(self._materialize_one, table, filters[table.name], copied)
            tasks.append((table.name, task))
        if tasks:
            run_in_pool(tasks, max_workers=len(tasks))

    # ------------------------------------------------------------ a leitura

    def _check_readable(
        self,
        statement_or_sql: sa.sql.ClauseElement | str,
    ) -> None:
        """[private] Confere a regra de leitura comum (``_check_select``) e, num statement Core, a
        view de cada tabela do modelo que ele cita. O texto pronto não é conferido: uma tabela sem
        view nele recebe o erro de catálogo do DuckDB."""
        _check_select(statement_or_sql)
        if isinstance(statement_or_sql, str):
            return
        for name in sorted(sql.referenced_tables(statement_or_sql)):
            if name in self._delta_db.metadata.tables:
                self._check_view(name)

    def query(
        self,
        statement_or_sql: sa.sql.ClauseElement | str,
        params: Mapping[str, object] | None = None,
    ) -> pa.Table:
        """[public] O resultado inteiro da consulta como ``pa.Table``, pelo motor DuckDB do leitor.

        Exemplo:

        .. code-block:: python

            reader.query(sa.select(Lancamento).where(Lancamento.data_base_str == sa.bindparam("p")),
                         {"p": "2026-08-31"})

        :param statement_or_sql: um ``Select`` ou ``CompoundSelect`` Core sobre as tabelas do
            modelo, ou um texto pronto no SQL do DuckDB. O texto cita as tabelas pelo nome do
            modelo e escreve os parâmetros como ``:nome``.
        :param params: os valores dos parâmetros, por nome, dos ``bindparam`` sem valor do
            statement ou dos marcadores do texto.
        :return: a tabela do resultado, nos tipos Arrow do contrato nas colunas do modelo.
        :raises serialize_db.errors.ContractError: um statement Core que não é consulta, ou que cita
            uma tabela do modelo sem view no leitor, sem chamar o motor.
        :raises serialize_db.errors.SqlError: os nomes de ``params`` não fecham com os parâmetros do
            statement ou do texto, ou o texto ainda traz o sentinela ``{prefix}``.
        :raises duckdb.Error: o texto que cita uma tabela sem view, ou uma coluna que o modelo
            ganhou depois da versão lida.
        """
        self._check_readable(statement_or_sql)
        return self._engine.query(statement_or_sql, params)

    def stream(
        self,
        statement_or_sql: sa.sql.ClauseElement | str,
        params: Mapping[str, object] | None = None,
        batch_size: int = 100_000,
    ) -> ReaderStream:
        """[public] Os lotes da consulta, entregues enquanto ela roda, pelo ``DuckDBEngine.stream``,
        com a memória limitada a 64 MiB de lotes.

        Uma thread do motor roda a consulta e entrega os lotes enquanto o cliente trabalha nos
        que já chegaram; acima dos 64 MiB, os lotes esperam num arquivo de transbordo.

        Exemplo:

        .. code-block:: python

            with reader.stream(sa.select(Lancamento), batch_size=100_000) as batches:
                for batch in batches:
                    work(batch)

        :param statement_or_sql: um ``Select`` ou ``CompoundSelect`` Core sobre as tabelas do
            modelo, ou um texto pronto no SQL do DuckDB. O texto cita as tabelas pelo nome do
            modelo e escreve os parâmetros como ``:nome``.
        :param params: os valores dos parâmetros, por nome, dos ``bindparam`` sem valor do
            statement ou dos marcadores do texto.
        :param batch_size: o máximo de linhas de cada lote.
        :return: o ``ReaderStream`` dos lotes, um ``BatchStream`` gerenciador de contexto, que
            segura o leitor até o ``close``. O ``close`` cancela a consulta que ainda roda e apaga
            o arquivo de transbordo.
        :raises serialize_db.errors.ContractError: um statement Core que não é consulta, ou que cita
            uma tabela do modelo sem view no leitor, sem chamar o motor.
        :raises serialize_db.errors.SqlError: os nomes de ``params`` não fecham com os parâmetros do
            statement ou do texto, ou o texto ainda traz o sentinela ``{prefix}``.
        :raises duckdb.Error: a consulta que falha antes do primeiro lote. O erro depois do
            primeiro lote sobe na leitura seguinte ao último lote entregue. Com mais de uma
            thread, o erro da consulta pode chegar como ``INTERRUPT Error: Interrupted!``, sem a
            causa, como descreve o ``DuckDBEngine.stream``.
        """
        self._check_readable(statement_or_sql)
        return ReaderStream(self, self._engine.stream(statement_or_sql, params, batch_size))

    @contextlib.contextmanager
    def session(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """[public] A conexão crua do motor, com o lock da sessão tomado pelo bloco: o caminho de um
        comando que não é consulta, como uma tabela temporária ao lado das views.

        Exemplo:

        .. code-block:: python

            with reader.session() as connection:
                connection.execute("CREATE TEMP TABLE ids AS SELECT range AS id FROM range(10)")
            reader.query("SELECT count(*) FROM ids")

        :return: o gerenciador de contexto cujo ``with`` dá a ``duckdb.DuckDBPyConnection`` da
            sessão. O bloco segura o leitor até o fim.
        """
        # O gerador segura o leitor, também o que não está numa variável, como em
        # db.open_delta().session(), até o fim do bloco.
        with self._engine.session() as connection:
            yield connection

    # ------------------------------------------------------------ o encerramento

    def close(self) -> None:
        """[public] Fecha o motor: cancela o comando em curso, fecha a conexão e apaga o banco
        temporário, com as tabelas materializadas, e a pasta de transbordo.

        O DuckDB só devolve a memória aqui. Num caderno, o leitor aberto sem ``with`` segura a
        memória até esta chamada. A segunda chamada não faz nada.

        Exemplo:

        .. code-block:: python

            reader = db.open_delta(channel="current")
            reader.query(sa.select(Conta))
            reader.close()   # sem ele, a coleta do leitor apaga a pasta temporária
        """
        self._finalizer()

    def __enter__(self) -> DeltaReader:
        return self

    def __exit__(
        self,
        *exc: object,
    ) -> None:
        self.close()


# ---------------------------------------------------------------- o leitor Redshift


class RedshiftReader:
    """[public] O leitor da base publicada no Redshift: as tabelas ``<ambiente>_<tabela>`` do
    esquema, numa sessão do motor Redshift com o prefixo do ambiente.

    ``open_redshift`` e ``db.open_redshift`` são as entradas. O leitor compila cada statement
    Core com o prefixo ``<ambiente>_`` no nome das tabelas do modelo. ``query`` devolve o
    resultado inteiro pelo cursor do driver, o caminho dos resultados pequenos; ``stream``
    entrega os lotes dos arquivos de um ``UNLOAD`` em ``unload_to``, o caminho dos resultados
    grandes.

    Exemplo:

    .. code-block:: python

        with open_redshift(Base.metadata, "prd", unload_to="s3://bucket-do-cliente/tmp") as reader:
            reader.query(sa.select(Conta))                          # pelo cursor
            with reader.stream(sa.select(Lancamento)) as batches:   # pelo UNLOAD
                for batch in batches:
                    work(batch)
    """

    def __init__(
        self,
        metadata: sa.MetaData,
        environment: str,
        config: RedshiftConfig | None = None,
        unload_to: str | None = None,
    ) -> None:
        """[public] Abre a sessão no esquema do Redshift. ``open_redshift`` e ``db.open_redshift``
        são as entradas.

        :param metadata: o ``MetaData`` dos modelos do cliente, cujas tabelas os statements citam.
        :param environment: o ambiente publicado, ``prd`` ou ``dsv``, que prefixa o nome das
            tabelas publicadas.
        :param config: a configuração do Redshift. ``None`` lê as variáveis
            ``SERIALIZE_DB_REDSHIFT_*`` (``RedshiftConfig.from_environment``).
        :param unload_to: a URI da pasta dos arquivos do ``UNLOAD`` de ``stream``. O leitor grava
            em ``<unload_to>/<id do leitor>/``, e o ``close`` esvazia essa pasta. ``None`` deixa
            ``stream`` indisponível, e o leitor não toca arquivo algum. O ``UNLOAD`` grava só no
            S3. Por isso, na conexão com o Redshift, pelo ``workgroup`` ou pelo ``host``, e na
            conexão com o substituto local das suítes, só uma pasta ``s3://bucket/prefixo``
            serve, os arquivos ficam nela até o ``close`` do stream e o do leitor, e uma pasta
            local faz o primeiro ``stream`` falhar, sem arquivo gravado. A pasta local serve só à
            conexão de mentira dos testes do pacote, que grava o arquivo pelo armazenamento do
            leitor.
        :raises serialize_db.errors.ContractError: o ambiente fora da regra da partição, ou a
            configuração sem conexão, isto é, sem ``workgroup`` e sem ``host``, ``user`` e
            ``password``; ou, sem ``config``, ``SERIALIZE_DB_REDSHIFT_PORT`` que não é número.
        :raises ValueError: ``unload_to`` no S3 sem região, ou noutro esquema de URI.
        """
        # O módulo do Redshift entra só com o leitor: importar o pacote não carrega o driver.
        from serialize_db.engine.redshift import RedshiftConfig, RedshiftEngine

        self.environment = check_partition_value(environment)
        """[public] O ambiente publicado, ``prd`` ou ``dsv``, que dá o prefixo ``<ambiente>_`` das
        tabelas."""
        self.metadata = metadata
        """[public] O ``MetaData`` dos modelos do cliente."""
        self.reader_id = _new_reader_id()
        """[public] O identificador do leitor, ``reader-<AAAA-MM-DD>-<uuid8>``, que nomeia a pasta
        dos arquivos do ``UNLOAD`` sob ``unload_to``."""
        if config is None:
            config = RedshiftConfig.from_environment()
        storage = None
        staging_prefix = None
        if unload_to is not None:
            storage = Storage.for_uri(unload_to)
            staging_prefix = self.reader_id
        self._engine = RedshiftEngine(
            config, self.reader_id, storage, staging_prefix, prefix=f"{self.environment}_"
        )

    def query(
        self,
        statement_or_sql: sa.sql.ClauseElement | str,
        params: Mapping[str, object] | None = None,
    ) -> pa.Table:
        """[public] O resultado inteiro da consulta como ``pa.Table``, pelo cursor do driver, que
        materializa o resultado inteiro no cliente: o caminho dos resultados pequenos.

        Exemplo:

        .. code-block:: python

            reader.query(sa.select(Lancamento).where(Lancamento.data_base_str == sa.bindparam("p")),
                         {"p": "2026-08-31"})

        :param statement_or_sql: um ``Select`` ou ``CompoundSelect`` Core sobre as tabelas do
            modelo, compilado com o prefixo ``<ambiente>_`` no nome de cada tabela, ou um texto
            pronto no SQL do Redshift. O texto escreve os parâmetros como ``:nome`` e traz o
            sentinela ``{prefix}`` antes do nome de cada tabela, que o leitor troca por
            ``<ambiente>_``.
        :param params: os valores dos parâmetros, por nome, dos ``bindparam`` sem valor do
            statement ou dos marcadores do texto.
        :return: a tabela do resultado, montada coluna a coluna com o esquema do ``row_desc`` do
            driver, a descrição das colunas do resultado.
        :raises serialize_db.errors.ContractError: um statement Core que não é consulta, sem comando
            no servidor.
        :raises serialize_db.errors.SqlError: os nomes de ``params`` não fecham com os parâmetros do
            statement ou do texto.
        :raises serialize_db.errors.SandboxError: uma coluna do resultado num tipo fora do contrato.
        :raises redshift_connector.Error: a tabela não publicada, que o servidor responde com
            ``XX000`` e ``Relation <nome> does not exist in the database.``.
        """
        _check_select(statement_or_sql)
        return self._engine.query(statement_or_sql, params)

    def stream(
        self,
        statement_or_sql: sa.sql.ClauseElement | str,
        params: Mapping[str, object] | None = None,
        batch_size: int = 100_000,
    ) -> BatchStream:
        """[public] Os lotes da consulta, lidos dos arquivos do ``UNLOAD ... PARALLEL OFF`` dela: o
        caminho dos resultados grandes.

        O ``UNLOAD`` grava o resultado da consulta em arquivos no S3, em
        ``<unload_to>/<id do leitor>/stream/<uuid>/``, e roda inteiro antes do primeiro lote. Uma
        thread lê os arquivos dois lotes à frente do cliente.

        Exemplo:

        .. code-block:: python

            with reader.stream(sa.select(Lancamento), batch_size=100_000) as batches:
                for batch in batches:
                    work(batch)

        :param statement_or_sql: um ``Select`` ou ``CompoundSelect`` Core sobre as tabelas do
            modelo, compilado com o prefixo ``<ambiente>_`` no nome de cada tabela, ou um texto
            pronto no SQL do Redshift. O texto escreve os parâmetros como ``:nome`` e traz o
            sentinela ``{prefix}`` antes do nome de cada tabela, que o leitor troca por
            ``<ambiente>_``.
        :param params: os valores dos parâmetros, por nome, dos ``bindparam`` sem valor do
            statement ou dos marcadores do texto. Os valores entram como literais, porque o
            ``UNLOAD`` não recebe parâmetro.
        :param batch_size: o máximo de linhas de cada lote, lido de cada arquivo do ``UNLOAD``.
        :return: o ``BatchStream`` dos lotes, um gerenciador de contexto. O ``BatchStream.close``
            apaga os arquivos do ``UNLOAD`` deste stream.
        :raises serialize_db.errors.ContractError: um statement Core que não é consulta, ou o leitor
            sem ``unload_to``, antes de qualquer comando no servidor.
        :raises serialize_db.errors.SqlError: os nomes de ``params`` não fecham com os parâmetros do
            statement ou do texto.
        :raises serialize_db.errors.SandboxError: uma coluna do resultado num tipo fora do contrato;
            ou, sem ``iam_role``, a sessão ``boto3`` sem credenciais para o ``UNLOAD``.
        :raises FileNotFoundError: a falta do manifesto, o arquivo que lista os arquivos gravados,
            depois de um ``UNLOAD`` de alguma linha.
        """
        _check_select(statement_or_sql)
        return self._engine.stream(statement_or_sql, params, batch_size)

    def session(self) -> contextlib.AbstractContextManager[object]:
        """[public] A conexão crua do motor, com o lock da sessão tomado pelo bloco: o caminho de um
        comando que não é consulta, como uma tabela temporária.

        Exemplo:

        .. code-block:: python

            with reader.session() as connection:
                connection.cursor().execute("CREATE TEMP TABLE ids (id BIGINT)")

        :return: o gerenciador de contexto cujo ``with`` dá a conexão do ``redshift_connector``
            da sessão, com o autocommit ligado.
        """
        return self._engine.session()

    def close(self) -> None:
        """[public] Fecha a sessão e esvazia a pasta ``<unload_to>/<id do leitor>/``, nada fora
        dela. Sem ``unload_to``, só fecha a sessão. A segunda chamada não faz nada.

        Exemplo:

        .. code-block:: python

            reader = open_redshift(Base.metadata, "prd")
            reader.query(sa.select(Conta))
            reader.close()
        """
        self._engine.cleanup()

    def __enter__(self) -> RedshiftReader:
        return self

    def __exit__(
        self,
        *exc: object,
    ) -> None:
        self.close()


def open_redshift(
    metadata: sa.MetaData,
    environment: str,
    config: RedshiftConfig | None = None,
    unload_to: str | None = None,
) -> RedshiftReader:
    """[public] Abre o leitor da base publicada no Redshift (``RedshiftReader``) para o cliente que
    tem o modelo e não a raiz Delta. O time que tem o ``Database`` entra por ``db.open_redshift``.

    Exemplo:

    .. code-block:: python

        with open_redshift(Base.metadata, "prd") as reader:   # SERIALIZE_DB_REDSHIFT_*, só query
            frame = reader.query(sa.select(Conta)).to_pandas(types_mapper=pd.ArrowDtype)

    :param metadata: o ``MetaData`` dos modelos do cliente.
    :param environment: o ambiente publicado, ``prd`` ou ``dsv``.
    :param config: a configuração do Redshift. ``None`` lê as variáveis
        ``SERIALIZE_DB_REDSHIFT_*``.
    :param unload_to: a URI da pasta dos arquivos do ``UNLOAD`` de ``stream``. O leitor grava em
        ``<unload_to>/<id do leitor>/``, e o ``close`` esvazia essa pasta. ``None`` deixa
        ``stream`` indisponível, e o leitor não toca arquivo algum. O ``UNLOAD`` grava só no S3.
        Por isso, na conexão com o Redshift, pelo ``workgroup`` ou pelo ``host``, e na conexão com
        o substituto local das suítes, só uma pasta ``s3://bucket/prefixo`` serve, os arquivos
        ficam nela até o ``close`` do stream e o do leitor, e uma pasta local faz o primeiro
        ``stream`` falhar, sem arquivo gravado. A pasta local serve só à conexão de mentira dos
        testes do pacote, que grava o arquivo pelo armazenamento do leitor.
    :return: o leitor, um gerenciador de contexto, cujo ``close`` fecha a sessão e esvazia
        ``<unload_to>/<id do leitor>/``.
    :raises serialize_db.errors.ContractError: o ambiente fora da regra da partição, ou a
        configuração sem conexão, isto é, sem ``workgroup`` e sem ``host``, ``user`` e
        ``password``; ou, sem ``config``, ``SERIALIZE_DB_REDSHIFT_PORT`` que não é número.
    :raises ValueError: ``unload_to`` no S3 sem região, ou noutro esquema de URI.
    """
    return RedshiftReader(metadata, environment, config, unload_to)
