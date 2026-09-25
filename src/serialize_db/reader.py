"""O acesso de leitura à base, com o modelo: o leitor Delta e o leitor Redshift.

``DeltaReader`` abre um DuckDB no processo com uma view por tabela do modelo sobre a versão de um
snapshot da base Delta, e ``RedshiftReader`` roda o mesmo statement nas tabelas publicadas
``<ambiente>_<tabela>`` do esquema. O time que tem o ``Database`` entra por ``db.open_delta()`` e
``db.open_redshift()``; o cliente que só enxerga o Redshift entra por ``open_redshift``, sem a
raiz Delta. O resultado é Arrow, como nos motores: ``query`` devolve a ``pa.Table``, que
``to_pandas(types_mapper=pd.ArrowDtype)`` leva ao pandas, e ``stream`` entrega os lotes.

O leitor Delta lê, sem argumento, o snapshot do canal ``default`` do ambiente, que ``serialize-db
channel`` move; ``snapshot=`` lê um snapshot pelo nome, o arquivado pela cópia em
``arquivo/<nome>/``; ``channel="current"`` lê a versão atual de cada tabela. Cada view fica presa
à versão lida na abertura, e a leitura entre tabelas é consistente num snapshot. O ``delta_scan``
poda as partições por ``=``, por ``BETWEEN`` e pelo ``IN`` ao lado de um intervalo, e abre todos
os arquivos com um ``IN`` de mais de um valor sozinho; ``materialize`` copia uma tabela, ou parte
das partições dela, para o banco local. O leitor Redshift lê as tabelas que
``serialize_db.publication`` publica, cada uma na sua transação: uma consulta que junta duas
tabelas durante uma publicação pode ler versões diferentes.

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
from collections.abc import Mapping
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

__all__ = ["DeltaReader", "RedshiftReader", "open_redshift"]

log = logging.getLogger("serialize_db.reader")


def _new_reader_id() -> str:
    """``reader-<AAAA-MM-DD>-<uuid8>``, com a data em UTC."""
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    return f"reader-{today}-{uuid.uuid4().hex[:8]}"


def _check_select(statement_or_sql: sa.sql.ClauseElement | str) -> None:
    """A regra de leitura comum: um statement Core que não é ``Select`` nem ``CompoundSelect`` é
    ``ContractError`` antes de chamar o motor; um texto pronto roda como está."""
    if isinstance(statement_or_sql, str):
        return
    if not isinstance(statement_or_sql, (sa.Select, sa.CompoundSelect)):
        raise ContractError(
            f"o leitor roda só Select e CompoundSelect, e recebeu "
            f"{type(statement_or_sql).__name__}; um comando vai pela conexão de session()")


# ---------------------------------------------------------------- as versões do leitor Delta


@dataclasses.dataclass(frozen=True)
class _Source:
    """De onde o leitor Delta lê: o snapshot (``None`` no canal ``current``), a versão e a URI de
    cada tabela do modelo com view, e o texto da origem para as mensagens."""

    snapshot: str | None
    versions: dict[str, int]
    uris: dict[str, str]
    description: str


def _current_source(db: Database) -> _Source:
    """A versão atual de cada tabela do modelo que existe no ambiente, como ``Execution`` na
    abertura."""
    versions = {}
    uris = {}
    for table in db.tables():
        uri = db.uri(table)
        if delta.table_exists(uri, db.storage):
            versions[table.name] = delta.open_table(uri, db.storage).version()
            uris[table.name] = uri
    return _Source(None, versions, uris, f"a versão atual do ambiente {db.environment}")


def _snapshot_source(db: Database, name: str, entry: Mapping[str, int]) -> _Source:
    """As versões da entrada do snapshot, nas tabelas do modelo que ela tem."""
    versions = {}
    uris = {}
    for table in db.tables():
        if table.name in entry:
            versions[table.name] = entry[table.name]
            uris[table.name] = db.uri(table)
    return _Source(name, versions, uris, f"o snapshot {name}")


def _archived_source(db: Database, name: str, entry: Mapping[str, int]) -> _Source:
    """As cópias de ``arquivo/<nome>/<tabela>`` das tabelas do modelo que a entrada arquivada tem,
    na versão atual de cada cópia, que não muda depois do ``archive``."""
    storage = db.storage
    versions = {}
    uris = {}
    for table in db.tables():
        if table.name not in entry:
            continue
        uri = storage.uri_of(storage.join(db.archive_prefix(name), table.name))
        versions[table.name] = delta.open_table(uri, storage).version()
        uris[table.name] = uri
    return _Source(name, versions, uris, f"o snapshot arquivado {name}")


def _resolve_source(db: Database, snapshot: str | None, channel: str | None) -> _Source:
    """As versões pelo modo pedido: o canal ``current``, o snapshot pelo nome, vivo ou arquivado,
    ou o snapshot do canal, o ``default`` sem argumento."""
    if snapshot is not None and channel is not None:
        raise ContractError(f"open_delta recebe snapshot={snapshot!r} ou channel={channel!r}, "
                            "não os dois")
    if channel == delta.CURRENT_CHANNEL:
        return _current_source(db)
    control, _ = delta.read_snapshots(db.storage, db.environment)
    if snapshot is None:
        snapshot = delta.channel_snapshot(control, channel or delta.DEFAULT_CHANNEL)
    archived = control.get("archived", {})
    if snapshot in archived:
        return _archived_source(db, snapshot, archived[snapshot])
    return _snapshot_source(db, snapshot, delta.snapshot_versions(control, snapshot))


# ---------------------------------------------------------------- o leitor Delta


class DeltaReader:
    """O leitor da base Delta: um DuckDB no processo com uma view por tabela do modelo, presa à
    versão do snapshot lido.

    Exemplo:

    .. code-block:: python

        with db.open_delta(channel="current") as reader:
            reader.versions                       # {"cad_contas": 1, "cad_lancamentos": 143}
            reader.query(sa.select(sa.func.count()).select_from(Lancamento.__table__))
    """

    def __init__(self, db: Database, snapshot: str | None = None, channel: str | None = None,
                 config: DuckDBConfig | None = None) -> None:
        """Lê as versões, abre o motor e cria as views; ``db.open_delta`` é a entrada.

        :param db: o banco, com a raiz, o ambiente e os modelos.
        :param snapshot: o nome de um snapshot do ambiente; o arquivado é lido pela cópia em
            ``arquivo/<nome>/``. ``None`` lê o canal.
        :param channel: o canal do ambiente, ``"default"``, o mesmo que sem argumento, ou
            ``"current"``, a versão atual de cada tabela do modelo que existe no ambiente.
            ``None`` com ``snapshot`` ``None`` é o canal ``default``.
        :param config: a configuração do DuckDB; ``None`` é ``DuckDBConfig()``, os limites lidos
            do ambiente e uma pasta temporária nova, apagada no ``close``.
        :raises ContractError: ``snapshot`` e ``channel`` juntos; o ambiente sem o canal, com o
            ``serialize-db channel`` que o cria e o canal ``current`` na mensagem; ou o snapshot
            que não existe.
        :raises duckdb.Error: a extensão ``delta`` ausente da pasta configurada, ou uma versão
            do snapshot que a tabela não tem mais, na criação da view.
        """
        source = _resolve_source(db, snapshot, channel)
        self.snapshot = source.snapshot
        """O snapshot lido; ``None`` no canal ``current``."""
        self.versions = source.versions
        """A versão de cada tabela com view, pelo nome da tabela; no snapshot arquivado, a versão
        de cada cópia em ``arquivo/<nome>/``. A tabela do modelo ausente do snapshot não tem
        view."""
        self.materialized: dict[str, list[str] | None] = {}
        """As tabelas copiadas para o banco local por ``materialize``, com as partições de cada
        uma; ``None`` na tabela inteira."""
        self.reader_id = _new_reader_id()
        """O identificador do leitor, ``reader-<AAAA-MM-DD>-<uuid8>``, que nomeia o banco
        temporário e a pasta de transbordo do motor."""
        self._db = db
        self._uris = source.uris
        self._source = source.description
        self._lock = threading.Lock()
        self._engine = DuckDBEngine(config or DuckDBConfig(), self.reader_id, db.storage)
        # O finalizador guarda o motor, não o leitor: a coleta de um leitor sem close, ou o fim
        # normal do interpretador, apaga o banco temporário e a pasta de transbordo dele.
        self._finalizer = weakref.finalize(self, self._engine.cleanup)
        try:
            self._open_views()
        except BaseException:
            self.close()
            raise
        log.info("leitor %s aberto sobre %s: versões %s", self.reader_id, self._source,
                 self.versions)

    def _create_view(self, table: sa.Table) -> None:
        """A view de uma tabela, numa sessão a mais do motor, fechada no fim."""
        with self._engine.new_session() as session:
            session.ingest(table, self._uris[table.name], self.versions[table.name])

    def _open_views(self) -> None:
        """Uma view por tabela do modelo presente nas versões, as tabelas em paralelo."""
        tasks = []
        for table in self._db.tables():
            if table.name in self.versions:
                tasks.append((table.name, functools.partial(self._create_view, table)))
        if tasks:
            run_in_pool(tasks, max_workers=len(tasks))

    # ------------------------------------------------------------ a materialização

    def _check_view(self, name: str) -> None:
        """A tabela do modelo sem view é ``ContractError`` com a origem das versões."""
        if name not in self.versions:
            raise ContractError(f"{name}: a tabela do modelo não tem view no leitor, porque não "
                                f"está em {self._source}")

    def _kind(self, name: str) -> str:
        """``TABLE`` depois de uma materialização, ``VIEW`` antes dela."""
        with self._lock:
            return "TABLE" if name in self.materialized else "VIEW"

    def _materialize_one(self, table: sa.Table, where: str,
                         partitions: list[str] | None) -> None:
        """A troca da view, ou da tabela de uma materialização anterior, por ``CREATE TABLE ... AS
        SELECT * FROM delta_scan(...)``, numa transação de uma sessão a mais; a falha devolve o
        objeto anterior pelo ``ROLLBACK``."""
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
        log.info("leitor %s: %s materializada, partições %s", self.reader_id, table.name,
                 "todas" if partitions is None else partitions)

    def materialize(self, *tables: sa.Table, partitions: list[str] | None = None) -> None:
        """Copia as tabelas para o banco local do leitor: o nome do modelo passa a ser uma tabela
        com os dados da versão lida, ou só das partições pedidas.

        Cada tabela troca numa transação, e as tabelas correm em paralelo, cada uma numa sessão a
        mais. Uma chamada seguinte troca a tabela de novo. O filtro das partições é o do
        ``ingest`` do motor, o intervalo ao lado do ``IN``.

        Exemplo:

        .. code-block:: python

            reader.materialize(Conta.__table__)
            reader.materialize(Lancamento.__table__, partitions=["2026-07-31", "2026-08-31"])
            reader.materialized   # {"cad_contas": None, "cad_lancamentos": ["2026-07-31", ...]}

        :param tables: as tabelas do modelo com view no leitor.
        :param partitions: os valores de partição copiados, pela regra da partição; ``None``
            copia a tabela inteira.
        :raises ContractError: uma tabela sem view no leitor, ``partitions`` numa tabela sem
            partição ou um valor fora da regra da partição, antes de qualquer troca.
        :raises duckdb.Error: a falha da cópia de uma tabela, que fica com o objeto anterior,
            view ou tabela, enquanto as outras terminam.
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

    def _check_readable(self, statement_or_sql: sa.sql.ClauseElement | str) -> None:
        """A regra de leitura comum e, num statement Core, a view de cada tabela do modelo que
        ele cita; o texto pronto recebe o erro de catálogo do DuckDB."""
        _check_select(statement_or_sql)
        if isinstance(statement_or_sql, str):
            return
        for name in sorted(sql.referenced_tables(statement_or_sql)):
            if name in self._db.metadata.tables:
                self._check_view(name)

    def query(self, statement_or_sql: sa.sql.ClauseElement | str,
              params: Mapping[str, object] | None = None) -> pa.Table:
        """O resultado inteiro como ``pa.Table``, pelo motor.

        Exemplo:

        .. code-block:: python

            reader.query(sa.select(Lancamento).where(Lancamento.data_base_str == sa.bindparam("p")),
                         {"p": "2026-08-31"})

        :param statement_or_sql: um ``Select`` ou ``CompoundSelect`` Core sobre as tabelas do
            modelo, ou um texto pronto no SQL do DuckDB, com os parâmetros como ``:nome``, que
            cita as tabelas pelo nome do modelo.
        :param params: os valores dos parâmetros, por nome, dos ``bindparam`` sem valor do
            statement ou dos marcadores do texto.
        :return: a tabela do resultado, nos tipos Arrow do contrato nas colunas do modelo.
        :raises ContractError: um statement Core que não é consulta, ou que cita uma tabela do
            modelo sem view no leitor, sem chamar o motor.
        :raises SqlError: os nomes de ``params`` não fecham com os parâmetros do statement ou do
            texto, ou o texto ainda traz o sentinela ``{prefix}``.
        :raises duckdb.Error: o texto que cita uma tabela sem view, ou uma coluna que o modelo
            ganhou depois da versão lida.
        """
        self._check_readable(statement_or_sql)
        return self._engine.query(statement_or_sql, params)

    def stream(self, statement_or_sql: sa.sql.ClauseElement | str,
               params: Mapping[str, object] | None = None,
               batch_size: int = 100_000) -> BatchStream:
        """Os lotes da consulta enquanto ela roda, pelo ``DuckDBEngine.stream``, com a memória
        limitada a 64 MiB de lotes.

        Exemplo:

        .. code-block:: python

            with reader.stream(sa.select(Lancamento), batch_size=100_000) as batches:
                for batch in batches:
                    work(batch)

        :param statement_or_sql: um ``Select`` ou ``CompoundSelect`` Core sobre as tabelas do
            modelo, ou um texto pronto no SQL do DuckDB, com os parâmetros como ``:nome``, que
            cita as tabelas pelo nome do modelo.
        :param params: os valores dos parâmetros, por nome, dos ``bindparam`` sem valor do
            statement ou dos marcadores do texto.
        :param batch_size: o máximo de linhas de cada lote.
        :return: o ``BatchStream`` dos lotes, gerenciador de contexto; o ``BatchStream.close``
            cancela a consulta que ainda roda e apaga o arquivo de transbordo.
        :raises ContractError: um statement Core que não é consulta, ou que cita uma tabela do
            modelo sem view no leitor, sem chamar o motor.
        :raises SqlError: os nomes de ``params`` não fecham com os parâmetros do statement ou do
            texto, ou o texto ainda traz o sentinela ``{prefix}``.
        :raises duckdb.Error: a consulta que falha antes do primeiro lote.
        """
        self._check_readable(statement_or_sql)
        return self._engine.stream(statement_or_sql, params, batch_size)

    def session(self) -> contextlib.AbstractContextManager[duckdb.DuckDBPyConnection]:
        """A conexão crua do motor, com o lock tomado pelo bloco: o caminho de um comando que não
        é consulta, como uma tabela temporária ao lado das views.

        Exemplo:

        .. code-block:: python

            with reader.session() as connection:
                connection.execute("CREATE TEMP TABLE ids AS SELECT range AS id FROM range(10)")
            reader.query("SELECT count(*) FROM ids")

        :return: o gerenciador de contexto cujo ``with`` dá a ``duckdb.DuckDBPyConnection`` da
            sessão.
        """
        return self._engine.session()

    # ------------------------------------------------------------ o encerramento

    def close(self) -> None:
        """Fecha o motor: cancela o comando em curso, fecha a conexão e apaga o banco temporário,
        com as tabelas materializadas, e a pasta de transbordo. O DuckDB só devolve a memória
        aqui: num caderno, o leitor sem ``with`` segura a memória até esta chamada. A segunda
        chamada não faz nada.

        Exemplo:

        .. code-block:: python

            reader = db.open_delta(channel="current")
            reader.query(sa.select(Conta))
            reader.close()   # sem ele, a coleta do leitor apaga a pasta temporária
        """
        self._finalizer()

    def __enter__(self) -> DeltaReader:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


# ---------------------------------------------------------------- o leitor Redshift


class RedshiftReader:
    """O leitor da base publicada no Redshift: as tabelas ``<ambiente>_<tabela>`` do esquema,
    numa sessão do motor Redshift com o prefixo do ambiente.

    Exemplo:

    .. code-block:: python

        with open_redshift(Base.metadata, "prd", unload_to="s3://bucket-do-cliente/tmp") as reader:
            reader.query(sa.select(Conta))                          # pelo cursor
            with reader.stream(sa.select(Lancamento)) as batches:   # pelo UNLOAD
                for batch in batches:
                    work(batch)
    """

    def __init__(self, metadata: sa.MetaData, environment: str,
                 config: RedshiftConfig | None = None, unload_to: str | None = None) -> None:
        """Abre a sessão no esquema; ``open_redshift`` e ``db.open_redshift`` são as entradas.

        :param metadata: o ``MetaData`` dos modelos do cliente, cujas tabelas os statements citam.
        :param environment: o ambiente publicado, ``prd`` ou ``dsv``, que prefixa o nome das
            tabelas publicadas.
        :param config: a configuração do Redshift; ``None`` lê as variáveis
            ``SERIALIZE_DB_REDSHIFT_*`` (``RedshiftConfig.from_environment``).
        :param unload_to: a URI da pasta dos arquivos do ``UNLOAD`` de ``stream``,
            ``s3://bucket/prefixo`` ou uma pasta local, sob a qual o leitor grava em
            ``<id do leitor>/`` e que o ``close`` esvazia; ``None`` deixa ``stream`` fora, e o
            leitor não toca arquivo algum.
        :raises ContractError: o ambiente fora da regra da partição, ou a configuração sem
            conexão: sem ``workgroup``, e sem ``host``, ``user`` e ``password``.
        :raises ValueError: ``unload_to`` no S3 sem região, ou noutro esquema de URI.
        """
        # O driver do Redshift é o extra "redshift": o módulo entra só quando o leitor entra.
        from serialize_db.engine.redshift import RedshiftConfig, RedshiftEngine

        self.environment = check_partition_value(environment)
        """O ambiente publicado, o prefixo ``<ambiente>_`` das tabelas."""
        self.metadata = metadata
        """O ``MetaData`` dos modelos."""
        self.reader_id = _new_reader_id()
        """O identificador do leitor, ``reader-<AAAA-MM-DD>-<uuid8>``, a pasta dos arquivos do
        ``UNLOAD`` sob ``unload_to``."""
        if config is None:
            config = RedshiftConfig.from_environment()
        storage = None
        staging_prefix = None
        if unload_to is not None:
            storage = Storage.for_uri(unload_to)
            staging_prefix = self.reader_id
        self._engine = RedshiftEngine(config, self.reader_id, storage, staging_prefix,
                                      prefix=f"{self.environment}_")

    def query(self, statement_or_sql: sa.sql.ClauseElement | str,
              params: Mapping[str, object] | None = None) -> pa.Table:
        """O resultado inteiro como ``pa.Table``, pelo cursor do driver, que materializa o
        resultado no cliente: o caminho dos resultados pequenos.

        Exemplo:

        .. code-block:: python

            reader.query(sa.select(Lancamento).where(Lancamento.data_base_str == sa.bindparam("p")),
                         {"p": "2026-08-31"})

        :param statement_or_sql: um ``Select`` ou ``CompoundSelect`` Core sobre as tabelas do
            modelo, compilado com o prefixo ``<ambiente>_``, ou um texto pronto no SQL do
            Redshift, com os parâmetros como ``:nome`` e o sentinela ``{prefix}`` antes do nome
            de cada tabela.
        :param params: os valores dos parâmetros, por nome, dos ``bindparam`` sem valor do
            statement ou dos marcadores do texto.
        :return: a tabela do resultado, montada por colunas com o esquema do ``row_desc``.
        :raises ContractError: um statement Core que não é consulta, sem comando no servidor.
        :raises SqlError: os nomes de ``params`` não fecham com os parâmetros do statement ou do
            texto.
        :raises SandboxError: uma coluna do resultado num tipo fora do contrato.
        :raises redshift_connector.Error: a tabela não publicada, que o servidor responde com
            ``XX000`` e ``Relation <nome> does not exist in the database.``.
        """
        _check_select(statement_or_sql)
        return self._engine.query(statement_or_sql, params)

    def stream(self, statement_or_sql: sa.sql.ClauseElement | str,
               params: Mapping[str, object] | None = None,
               batch_size: int = 100_000) -> BatchStream:
        """Os lotes da consulta lidos dos arquivos do ``UNLOAD ... PARALLEL OFF`` dela em
        ``<unload_to>/<id do leitor>/stream/<uuid>/``, dois lotes à frente do cliente: o caminho
        dos resultados grandes.

        Exemplo:

        .. code-block:: python

            with reader.stream(sa.select(Lancamento), batch_size=100_000) as batches:
                for batch in batches:
                    work(batch)

        :param statement_or_sql: um ``Select`` ou ``CompoundSelect`` Core sobre as tabelas do
            modelo, compilado com o prefixo ``<ambiente>_``, ou um texto pronto no SQL do
            Redshift, com os parâmetros como ``:nome`` e o sentinela ``{prefix}`` antes do nome
            de cada tabela.
        :param params: os valores dos parâmetros, por nome, dos ``bindparam`` sem valor do
            statement ou dos marcadores do texto; os valores entram como literais, porque o
            ``UNLOAD`` não recebe parâmetro.
        :param batch_size: o máximo de linhas de cada lote, lido de cada arquivo do ``UNLOAD``.
        :return: o ``BatchStream`` dos lotes, gerenciador de contexto; o ``BatchStream.close``
            apaga os arquivos do ``UNLOAD`` deste stream.
        :raises ContractError: um statement Core que não é consulta, ou o leitor sem
            ``unload_to``, antes de qualquer comando no servidor.
        :raises SqlError: os nomes de ``params`` não fecham com os parâmetros do statement ou do
            texto.
        :raises SandboxError: uma coluna do resultado num tipo fora do contrato; ou, sem
            ``iam_role``, a sessão ``boto3`` sem credenciais para o ``UNLOAD``.
        :raises FileNotFoundError: a falta do manifesto depois de um ``UNLOAD`` de alguma linha.
        """
        _check_select(statement_or_sql)
        return self._engine.stream(statement_or_sql, params, batch_size)

    def session(self) -> contextlib.AbstractContextManager[object]:
        """A conexão crua do motor, com o lock tomado pelo bloco: o caminho de um comando que não
        é consulta, como uma tabela temporária.

        Exemplo:

        .. code-block:: python

            with reader.session() as connection:
                connection.cursor().execute("CREATE TEMP TABLE ids (id BIGINT)")

        :return: o gerenciador de contexto cujo ``with`` dá a conexão do ``redshift_connector``
            da sessão, com o autocommit ligado.
        """
        return self._engine.session()

    def close(self) -> None:
        """Fecha a sessão e esvazia a pasta ``<unload_to>/<id do leitor>/``, nada fora dela; sem
        ``unload_to``, só fecha a sessão. A segunda chamada não faz nada.

        Exemplo:

        .. code-block:: python

            reader = open_redshift(Base.metadata, "prd")
            reader.query(sa.select(Conta))
            reader.close()
        """
        self._engine.cleanup()

    def __enter__(self) -> RedshiftReader:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


def open_redshift(metadata: sa.MetaData, environment: str, config: RedshiftConfig | None = None,
                  unload_to: str | None = None) -> RedshiftReader:
    """Abre o leitor da base publicada no Redshift para o cliente que tem o modelo e não a raiz
    Delta; o time que tem o ``Database`` entra por ``db.open_redshift``.

    Exemplo:

    .. code-block:: python

        with open_redshift(Base.metadata, "prd") as reader:   # SERIALIZE_DB_REDSHIFT_*, só query
            frame = reader.query(sa.select(Conta)).to_pandas(types_mapper=pd.ArrowDtype)

    :param metadata: o ``MetaData`` dos modelos do cliente.
    :param environment: o ambiente publicado, ``prd`` ou ``dsv``.
    :param config: a configuração do Redshift; ``None`` lê as variáveis
        ``SERIALIZE_DB_REDSHIFT_*``.
    :param unload_to: a URI da pasta dos arquivos do ``UNLOAD`` de ``stream``, num bucket ou
        numa pasta do cliente; ``None`` deixa ``stream`` fora.
    :return: o leitor, gerenciador de contexto, cujo ``close`` fecha a sessão e esvazia
        ``<unload_to>/<id do leitor>/``.
    :raises ContractError: o ambiente fora da regra da partição, ou a configuração sem
        conexão: sem ``workgroup``, e sem ``host``, ``user`` e ``password``.
    :raises ValueError: ``unload_to`` no S3 sem região, ou noutro esquema de URI.
    """
    return RedshiftReader(metadata, environment, config, unload_to)
