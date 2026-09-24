"""A execução de um pipeline: o banco (``Database``) e o ciclo de uma execução (``Execution``).

``Database`` junta a raiz do banco, o ambiente (``prd``, ``dsv``) e o ``MetaData`` dos modelos do
cliente, e monta os caminhos: a pasta de cada tabela é ``<raiz>/<ambiente>/<tabela>``. ``Execution``
é o gerenciador de contexto de uma execução: na entrada abre toda tabela do ambiente, fixa a versão
de cada uma e cria o sandbox do motor; na saída descarta o sandbox, grava o snapshot marcado e o
resumo no log. Entre os dois, o pipeline chama as primitivas: ``ingest`` traz as tabelas presas à
versão fixada, ``sandbox`` é o motor onde ele roda ``stream``, ``loader``, ``query`` e ``load``,
``next_ids`` dá as faixas da chave sequencial, ``audit`` confere o contrato, ``publish`` leva as
partições auditadas ao Delta e ``publish_redshift`` as leva aos clientes no Redshift, com a
configuração ``redshift`` que a execução recebe.

As primitivas podem ser chamadas de qualquer thread: cada comando do motor corre na sessão única,
sob o lock dela, e o estado mutável da execução (as versões, as auditorias aprovadas, o alocador)
fica sob um lock próprio. A partição e o ``execution_id`` seguem ``schema.PARTITION_VALUE``, porque
viram nome de pasta e literal SQL.

Exemplo:

.. code-block:: python

    import sqlalchemy as sa

    from serialize_db.execution import Database, Execution

    db = Database("s3://bucket/projeto/delta", "prd", Base.metadata)
    with Execution(db, "duckdb", "2026-08-31", execution_id="exec-2026-09-05") as run:
        previous = run.previous_partitions(Lancamento.__table__, 12)
        run.ingest(Lancamento.__table__, partitions=previous, materialize=True)
        with run.sandbox.stream(sa.select(Lancamento)) as stream, \\
                run.sandbox.loader(Projetado.__table__) as loader:
            for batch in stream:
                loader.write(project(batch, run.next_ids(Projetado.__table__, batch.num_rows)))
        run.audit(Projetado.__table__, ["2026-08-31"])
        run.publish(Projetado.__table__, partitions=["2026-08-31"])
"""

from __future__ import annotations

import dataclasses
import datetime
import functools
import logging
import threading
import time
import uuid
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from typing import TYPE_CHECKING

import pyarrow as pa
import sqlalchemy as sa
from deltalake import DeltaTable

from serialize_db import delta
from serialize_db._pool import run_in_pool
from serialize_db.audit import AuditReport, KeyScope
from serialize_db.engine import Engine
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.errors import (
    AuditFailed,
    ContractError,
    ExecutionConflict,
    PublicationError,
    SandboxError,
)
from serialize_db.schema import (
    check_partition_value,
    double_columns,
    sequential_key,
    table_options,
)
from serialize_db.storage import Storage, prepare_environment

if TYPE_CHECKING:
    from serialize_db.engine.redshift import RedshiftConfig

__all__ = ["Database", "Execution"]

log = logging.getLogger("serialize_db.execution")


# ---------------------------------------------------------------- o banco


@dataclasses.dataclass(frozen=True)
class Database:
    """A raiz do banco, o ambiente e os modelos do cliente.

    O ambiente é um nome de pasta e segue a regra da partição. ``storage`` nasce no primeiro uso, e
    a pasta local relativa vira absoluta: ``uri`` parte da raiz normalizada, a URI que o delta-rs e
    o DuckDB recebem. Os prefixos são caminhos relativos à raiz, os que os métodos de ``Storage``
    recebem.

    Exemplo:

    .. code-block:: python

        db = Database("s3://bucket/projeto/delta", "prd", Base.metadata)
        db.uri(Lancamento.__table__)   # "s3://bucket/projeto/delta/prd/cad_lancamentos"
        db.control_path()              # "prd/_serialize_db/snapshots.json"
    """

    root: str
    """A raiz do banco, como ``Storage.for_uri`` a recebe: ``s3://bucket/prefixo``, um caminho ou
    ``file://``; o S3 sem região e outro esquema são ``ValueError`` no primeiro uso de
    ``storage``."""
    environment: str
    """O ambiente, ``prd`` ou ``dsv``: as execuções de um não tocam as tabelas do outro. Fora da
    regra da partição, a construção é ``ContractError``."""
    metadata: sa.MetaData
    """O ``MetaData`` dos modelos do cliente: as tabelas que a execução abre e reconcilia."""

    def __post_init__(self) -> None:
        check_partition_value(self.environment)
        changed = prepare_environment()
        if changed:
            log.info("variáveis do ambiente acertadas para o delta-rs: %s", sorted(changed))

    @functools.cached_property
    def storage(self) -> Storage:
        """O armazenamento da raiz, criado no primeiro uso, sem tocar a rede."""
        return Storage.for_uri(self.root)

    def uri(self, table: sa.Table) -> str:
        """A pasta da tabela.

        Exemplo:

        .. code-block:: python

            db.uri(Lancamento.__table__)   # "s3://bucket/projeto/delta/prd/cad_lancamentos"

        :param table: a tabela do modelo.
        :return: a URI ``<raiz>/<ambiente>/<tabela>``, sem barra final.
        """
        return self.storage.uri_of(self.storage.join(self.environment, table.name))

    def control_path(self) -> str:
        """O arquivo de controle dos snapshots do ambiente.

        Exemplo:

        .. code-block:: python

            db.control_path()   # "prd/_serialize_db/snapshots.json"

        :return: o caminho relativo à raiz.
        """
        return self.storage.join(self.environment, delta.CONTROL_FILE)

    def staging_prefix(self, execution_id: str) -> str:
        """Os arquivos intermediários de uma execução.

        Exemplo:

        .. code-block:: python

            db.staging_prefix("exec-2026-09-05")   # "prd/staging/exec-2026-09-05"

        :param execution_id: o identificador da execução.
        :return: o prefixo ``<ambiente>/staging/<execution_id>``, relativo à raiz.
        """
        return self.storage.join(self.environment, "staging", execution_id)

    def publication_prefix(self, execution_id: str) -> str:
        """Os manifestos da publicação no Redshift de uma execução.

        Exemplo:

        .. code-block:: python

            db.publication_prefix("exec-2026-09-05")   # "prd/publicacao/exec-2026-09-05"

        :param execution_id: o identificador da execução que publica.
        :return: o prefixo ``<ambiente>/publicacao/<execution_id>``, relativo à raiz.
        """
        return self.storage.join(self.environment, "publicacao", execution_id)

    def archive_prefix(self, name: str) -> str:
        """A pasta de arquivo de um snapshot do banco.

        Exemplo:

        .. code-block:: python

            db.archive_prefix("2026T3")   # "prd/arquivo/2026T3"

        :param name: o nome do snapshot.
        :return: o caminho ``<ambiente>/arquivo/<nome>``, relativo à raiz.
        """
        return self.storage.join(self.environment, "arquivo", name)

    def tables(self) -> list[sa.Table]:
        """As tabelas dos modelos.

        Exemplo:

        .. code-block:: python

            [table.name for table in db.tables()]   # ["cad_contas", ..., "cad_lancamentos"]

        :return: as tabelas na ordem das chaves estrangeiras, a referenciada antes da que a
            referencia.
        """
        return list(self.metadata.sorted_tables)


# ---------------------------------------------------------------- as regras de entrada


def _checked_partition(value: str, db: Database) -> str:
    """A partição da execução: a regra da partição e o ``String(n)`` de cada coluna de partição do
    modelo, medido em bytes."""
    check_partition_value(value)
    for table in db.tables():
        column = table_options(table).partition_by
        if column is None:
            continue
        length = table.c[column].type.length
        if len(value.encode("utf-8")) > length:
            raise ContractError(
                f"partição {value!r} acima de String({length}) em {table.name}.{column}")
    return value


def _new_execution_id() -> str:
    """``exec-<AAAA-MM-DD>-<uuid8>``, com a data em UTC."""
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    return f"exec-{today}-{uuid.uuid4().hex[:8]}"


def _checked_partitions(partitions: Sequence[str] | None) -> list[str] | None:
    """Os valores de ``partitions`` pela regra da partição."""
    if partitions is None:
        return None
    checked = []
    for value in partitions:
        checked.append(check_partition_value(value))
    return checked


def _sequential_key(table: sa.Table) -> sa.Column:
    """A chave sequencial, a chave primária inteira de uma coluna, a única que ``next_ids``
    preenche; outra chave é ``ContractError``."""
    key = sequential_key(table)
    if key is None:
        names = [column.name for column in table.primary_key.columns]
        raise ContractError(f"{table.name}: next_ids serve à chave primária inteira de uma coluna, "
                            f"e a chave é {names}; numa chave composta o cliente decide os ids")
    return key


# ---------------------------------------------------------------- a execução


class Execution:
    """O ciclo de uma execução: as versões fixadas, o sandbox, a auditoria e a publicação.

    Exemplo:

    .. code-block:: python

        with Execution(db, "duckdb", "2026-08-31") as run:
            run.versions   # {"cad_lancamentos": 143, "cad_lancamentos_projetados": None}
    """

    def __init__(self, db: Database, engine: str | Engine, partition: str,
                 execution_id: str | None = None,
                 redshift: RedshiftConfig | None = None) -> None:
        """Guarda os parâmetros da execução, com a partição e o ``execution_id`` conferidos; nada
        é aberto antes da entrada do ``with``.

        :param db: o banco da execução.
        :param engine: o nome do motor, ``"duckdb"`` ou ``"redshift"``, ou um motor já
            construído, para os testes; um nome desconhecido é ``ContractError`` na entrada do
            ``with``.
        :param partition: a partição da execução, que segue ``schema.PARTITION_VALUE`` e cabe, em
            bytes, no ``String(n)`` de cada coluna de partição do modelo.
        :param execution_id: o identificador da execução, que segue ``schema.PARTITION_VALUE``;
            ausente, vira ``exec-<AAAA-MM-DD>-<uuid8>``, com a data em UTC.
        :param redshift: a configuração do Redshift
            (``serialize_db.engine.redshift.RedshiftConfig``), a do motor ``"redshift"`` e a de
            ``publish_redshift``; o motor ``"redshift"`` sem ela lê as variáveis
            ``SERIALIZE_DB_REDSHIFT_*``, e no motor ``"duckdb"`` sem ela ``publish_redshift`` é
            ``PublicationError``.
        :raises ContractError: a partição ou o ``execution_id`` fora da regra da partição, ou a
            partição acima do ``String(n)`` de uma coluna de partição.
        """
        self.db = db
        """O banco: a raiz, o ambiente e os modelos do cliente."""
        self.partition = _checked_partition(partition, db)
        """A partição da execução."""
        self.execution_id = check_partition_value(execution_id or _new_execution_id())
        """O identificador da execução, que vai aos metadados de cada commit e aos nomes do
        sandbox."""
        self._engine = engine
        self.redshift = redshift
        """A configuração do Redshift: a recebida, ou, no motor ``"redshift"`` sem ela, a das
        variáveis ``SERIALIZE_DB_REDSHIFT_*``, lida na entrada do ``with``."""
        self.versions: dict[str, int | None] = {}
        """A versão fixada de cada tabela do ambiente, pelo nome, ``None`` na que não existe: a
        entrada do ``with`` as lê, e ``publish`` avança a de cada tabela que grava."""
        self.sandbox: Engine | None = None
        """O motor da execução, onde o pipeline roda ``stream``, ``loader``, ``query`` e
        ``load``; ``None`` até a entrada do ``with``, e fechado na saída."""
        self._read: dict[str, int] = {}
        self._written: dict[str, int] = {}
        self._tables: dict[str, DeltaTable] = {}
        self._audits: dict[tuple[str, tuple[str, ...] | None], AuditReport] = {}
        self._next_ids: dict[str, int] = {}
        self._snapshot: str | None = None
        self._timings: dict[str, float] = {}
        self._lock = threading.Lock()

    # ------------------------------------------------------------ abertura e encerramento

    @contextmanager
    def _step(self, name: str) -> Iterator[None]:
        """Soma o tempo do passo, para o resumo no log."""
        started = time.perf_counter()
        try:
            yield
        finally:
            with self._lock:
                self._timings[name] = self._timings.get(name, 0.0) + time.perf_counter() - started

    def _open_tables(self) -> None:
        """Abre toda tabela do ambiente que existe e fixa a versão; a ausente fica ``None``."""
        storage = self.db.storage
        for table in self.db.tables():
            uri = self.db.uri(table)
            if not delta.table_exists(uri, storage):
                self.versions[table.name] = None
                continue
            dt = delta.open_table(uri, storage)
            self._tables[table.name] = dt
            self.versions[table.name] = dt.version()
            self._read[table.name] = dt.version()

    def _build_engine(self) -> Engine:
        """O motor pelo nome, ou o motor recebido."""
        if not isinstance(self._engine, str):
            return self._engine
        if self._engine == "duckdb":
            return DuckDBEngine(DuckDBConfig(), self.execution_id, self.db.storage)
        if self._engine == "redshift":
            # O driver do Redshift é o extra "redshift": o módulo entra só quando o motor entra.
            from serialize_db.engine.redshift import RedshiftConfig, RedshiftEngine

            if self.redshift is None:
                self.redshift = RedshiftConfig.from_environment()
            return RedshiftEngine(self.redshift, self.execution_id, self.db.storage,
                                  self.db.staging_prefix(self.execution_id))
        raise ContractError(f"motor {self._engine!r}: use 'duckdb' ou 'redshift'")

    def __enter__(self) -> Execution:
        with self._step("abertura"):
            self._open_tables()
            self.sandbox = self._build_engine()
        log.info("execução %s aberta: partição %s, versões %s", self.execution_id, self.partition,
                 self.versions)
        return self

    def __exit__(self, exc_type: object, exc: BaseException | None, tb: object) -> None:
        try:
            if self.sandbox is not None:
                self.sandbox.cleanup()
            if exc is None and self._snapshot is not None:
                self._write_snapshot()
        finally:
            timings = {name: round(seconds, 3) for name, seconds in self._timings.items()}
            outcome = "com erro" if exc is not None else "concluída"
            log.info("execução %s %s: partição %s, versões lidas %s, versões gravadas %s, "
                     "tempos %s", self.execution_id, outcome, self.partition, self._read,
                     self._written, timings)

    def _write_snapshot(self) -> None:
        """A entrada do snapshot marcado, com a versão de toda tabela do ambiente."""
        versions = {}
        for name, version in self.versions.items():
            if version is not None:
                versions[name] = version
        delta.snapshot(self.db.storage, self.db.environment, self._snapshot, versions)
        log.info("snapshot %s gravado: %s", self._snapshot, versions)

    # ------------------------------------------------------------ a leitura

    def _uri(self, table: sa.Table) -> str:
        return self.db.uri(table)

    def _version(self, table: sa.Table) -> int | None:
        with self._lock:
            return self.versions.get(table.name)

    def previous_partitions(self, table: sa.Table, n: int) -> list[str]:
        """Os ``n`` últimos valores de partição da tabela até a partição da execução, inclusive.

        Exemplo:

        .. code-block:: python

            run.previous_partitions(Lancamento.__table__, 12)   # ["2025-09-30", ..., "2026-08-31"]

        :param table: a tabela particionada do modelo.
        :param n: quantos valores, no máximo; zero ou negativo dá a lista vazia.
        :return: os valores na ordem de texto, lidos das ações ``add`` da versão fixada, ou a lista
            vazia numa tabela que não existe.
        :raises ContractError: a tabela sem partição.
        """
        partition_by = table_options(table).partition_by
        if partition_by is None:
            raise ContractError(f"{table.name}: tabela sem partição")
        if table.name not in self._tables:
            return []
        actions = pa.table(self._tables[table.name].get_add_actions(flatten=True))
        values = set()
        if actions.num_rows:
            values = set(actions.column(f"partition.{partition_by}").to_pylist())
        up_to_partition = []
        for value in sorted(values):
            if value <= self.partition:
                up_to_partition.append(value)
        if n <= 0:
            return []
        return up_to_partition[-n:]

    def _ingest_one(self, engine: Engine, table: sa.Table, partitions: list[str] | None,
                    materialize: bool) -> None:
        """A ingestão de uma tabela na versão fixada; a tabela que não existe é
        ``SandboxError``."""
        version = self._version(table)
        if version is None:
            raise SandboxError(
                f"{table.name}: a tabela não existe no ambiente {self.db.environment}")
        engine.ingest(table, self._uri(table), version, partitions, materialize)

    def _ingest_in_new_session(self, table: sa.Table, partitions: list[str] | None,
                               materialize: bool) -> None:
        """A ingestão de uma tabela numa sessão a mais do motor, fechada no fim."""
        with self.sandbox.new_session() as session:
            self._ingest_one(session, table, partitions, materialize)

    def ingest(self, *tables: sa.Table, partitions: list[str] | None = None,
               materialize: bool = False) -> None:
        """Traz as tabelas ao sandbox na versão fixada.

        Uma tabela entra na sessão principal; mais de uma entram todas em paralelo, cada uma numa
        sessão a mais do motor, e a chamada volta quando todas terminam. Uma falha não cancela as
        que já rodam: todas terminam, e a exceção leva o resultado de cada tabela numa nota.

        Exemplo:

        .. code-block:: python

            run.ingest(Contrato.__table__, Operacao.__table__)   # views sobre a versão fixada

        :param tables: as tabelas do modelo.
        :param partitions: os valores de partição a trazer, pela regra da partição; ``None`` traz
            a tabela inteira.
        :param materialize: no DuckDB, uma tabela em vez de uma view; no Redshift a tabela é
            sempre carregada.
        :raises ContractError: um valor fora da regra da partição, ou ``partitions`` numa tabela
            sem partição.
        :raises SandboxError: a tabela sem versão fixada, que não existe no ambiente, ou o nome
            dela já ocupado no sandbox.
        """
        checked = _checked_partitions(partitions)
        with self._step("ingest"):
            if len(tables) == 1:
                self._ingest_one(self.sandbox, tables[0], checked, materialize)
                return
            tasks = []
            for table in tables:
                task = functools.partial(self._ingest_in_new_session, table, checked, materialize)
                tasks.append((table.name, task))
            run_in_pool(tasks, max_workers=max(len(tables), 1))

    def published(self, table: sa.Table) -> sa.FromClause:
        """A versão fixada da tabela como origem de consulta, sem ocupar nome no sandbox: é por ela
        que o pipeline lê as partições publicadas da tabela que ele mesmo grava.

        Exemplo:

        .. code-block:: python

            previous = run.published(Projetado.__table__)
            run.sandbox.query(sa.select(sa.func.max(previous.c.id_projetado)))

        :param table: a tabela do modelo.
        :return: o ``FromClause`` com as colunas do contrato.
        :raises SandboxError: a tabela sem versão fixada, que ainda não existe.
        """
        return self.sandbox.published(table, self._uri(table), self._version(table))

    def _first_id(self, table: sa.Table, key: sa.Column) -> int:
        """O primeiro id de ``next_ids``: o maior da versão fixada mais um, ou 1 na tabela nova."""
        if table.name not in self._tables:
            return 1
        return delta.max_key(self._tables[table.name], key.name) + 1

    def next_ids(self, table: sa.Table, n: int) -> range:
        """Uma faixa de ``n`` inteiros contíguos da chave sequencial, acima do maior da versão
        fixada.

        O maior valor é lido uma vez por tabela, das estatísticas do log. As faixas de threads
        paralelas não se sobrepõem, e as de uma reexecução diferem.

        Exemplo:

        .. code-block:: python

            ids = run.next_ids(Projetado.__table__, batch.num_rows)   # range(1001, 1101)

        :param table: a tabela do modelo, cuja chave é a chave primária inteira de uma coluna.
        :param n: o tamanho da faixa.
        :return: a faixa; na tabela nova, a primeira começa em 1.
        :raises ContractError: outra chave, ou ``n`` negativo.
        """
        key = _sequential_key(table)
        if n < 0:
            raise ContractError(f"next_ids({table.name}, {n}): n negativo")
        with self._lock:
            if table.name not in self._next_ids:
                self._next_ids[table.name] = self._first_id(table, key)
            first = self._next_ids[table.name]
            self._next_ids[table.name] = first + n
        return range(first, first + n)

    # ------------------------------------------------------------ a auditoria

    def _referenced(self, table: sa.Table) -> dict[str, tuple[str, int]]:
        """A URI e a versão fixada de cada tabela que uma chave estrangeira aponta."""
        referenced = {}
        for constraint in table.foreign_key_constraints:
            target = constraint.referred_table
            version = self._version(target)
            if version is not None:
                referenced[target.name] = (self._uri(target), version)
        return referenced

    def audit(self, table: sa.Table, partitions: list[str] | None, foreign_keys: bool = False,
              key_scope: KeyScope | None = None) -> AuditReport:
        """Roda a auditoria do motor e guarda o relatório aprovado.

        O relatório, com o SQL de cada verificação e as amostras, vai para o log. A contagem por
        partição e as colunas ``Double`` com valor não finito do relatório aprovado são as que
        ``publish`` passa à exportação.

        Exemplo:

        .. code-block:: python

            run.audit(Projetado.__table__, ["2026-08-31"], foreign_keys=True)

        :param table: a tabela do modelo, carregada no sandbox.
        :param partitions: as partições da execução, pela regra da partição;
            ``partitions=None`` audita a tabela inteira do sandbox.
        :param foreign_keys: ``foreign_keys=True`` roda a verificação ``orfao_<colunas>`` de cada
            chave estrangeira, a chave sem a linha referenciada, procurada na tabela do sandbox ou
            na versão fixada da referenciada.
        :param key_scope: o escopo da unicidade na verificação ``chave_<colunas>_publicada``, a da
            chave contra as demais partições da versão publicada: o padrão a faz na chave sem a
            coluna de partição e sem a de ``partition_source``, ``key_scope="partition"`` a
            suprime, e ``key_scope="table"`` a faz também na chave com a coluna de
            ``partition_source``.
        :return: o relatório aprovado.
        :raises ContractError: um valor de ``partitions`` fora da regra da partição.
        :raises AuditFailed: a reprovação.
        """
        checked = _checked_partitions(partitions)
        version = self._version(table)
        uri = self._uri(table) if version is not None else None
        with self._step("audit"):
            report = self.sandbox.audit(table, checked, uri, version, foreign_keys, key_scope,
                                        self._referenced(table))
        failed = [result.name for result in report.results if not result.passed]
        log.info("auditoria de %s em %s: %s; não rodaram %s\n%s", table.name, checked,
                 "aprovada" if report.passed else f"reprovada em {failed}", list(report.not_run),
                 report.sql())
        for result in report.results:
            if not result.passed:
                log.warning("amostra de %s.%s: %s", table.name, result.name,
                            result.sample.to_pylist())
        if not report.passed:
            raise AuditFailed(f"{table.name} em {checked}: reprovada em {failed}; o relatório "
                              "está no log")
        key = (table.name, tuple(checked) if checked is not None else None)
        with self._lock:
            self._audits[key] = report
        return report

    # ------------------------------------------------------------ a publicação

    def _values(self, table: sa.Table, partitions: list[str] | None) -> list[str | None]:
        """As partições a publicar: as pedidas, ou ``[None]`` numa tabela sem partição."""
        partition_by = table_options(table).partition_by
        if partition_by is None:
            if partitions is not None:
                raise ContractError(
                    f"{table.name}: tabela sem partição recebeu partitions={partitions}")
            return [None]
        if partitions is None:
            raise ContractError(
                f"{table.name}: publish de uma tabela particionada exige partitions")
        return list(partitions)

    def _approved(self, table: sa.Table, partitions: list[str] | None,
                  audit: bool) -> AuditReport | None:
        """O relatório aprovado das partições, exigido quando ``audit`` é verdadeiro."""
        if not audit:
            log.warning("publish de %s em %s sem auditoria (audit=False)", table.name, partitions)
            return None
        key = (table.name, tuple(partitions) if partitions is not None else None)
        with self._lock:
            report = self._audits.get(key)
        if report is None:
            raise AuditFailed(f"{table.name}: publish exige a auditoria aprovada de {partitions} "
                              "na própria execução, ou audit=False")
        return report

    def _check_no_data_change(self, table: sa.Table, uri: str) -> None:
        """Confere que nenhuma alteração de dados entrou na tabela desde a versão fixada; um avanço
        só de metadados ou de manutenção atualiza a versão fixada. A tabela ausente nasce aqui."""
        storage = self.db.storage
        pinned = self._version(table)
        if pinned is None:
            delta.create_table(uri, table, storage)
            pinned = 0
        current = delta.open_table(uri, storage).version()
        if current != pinned:
            changed = delta.version_diff(uri, pinned, current, table, storage)
            if changed:
                raise ExecutionConflict(
                    f"{table.name}: outra execução gravou dados em {sorted(changed, key=str)} "
                    f"depois da versão fixada {pinned} (atual {current})")
        with self._lock:
            self.versions[table.name] = current

    def _publish_table(self, table: sa.Table, values: list[str | None],
                       report: AuditReport | None) -> int:
        """A reconciliação e a exportação de cada partição de uma tabela; devolve a versão final."""
        uri = self._uri(table)
        self._check_no_data_change(table, uri)
        delta.reconcile(uri, table, self.db.storage)
        version = self._version(table)
        for value in values:
            with self._lock:
                metadata = delta.commit_metadata(self.execution_id, dict(self._read),
                                                 self._snapshot)
            # Sem auditoria não há contagem a conferir, e toda coluna Double sai sem mínimo e
            # máximo, porque nada diz quais têm valor não finito.
            if report is None:
                expected = None
                nonfinite = double_columns(table)
            else:
                expected = report.rows(value)
                nonfinite = report.nonfinite_columns.get(value, ())
            version = self.sandbox.export_partition(table, uri, value, metadata,
                                                    expected_rows=expected,
                                                    columns_without_min_max=nonfinite)
            with self._lock:
                self.versions[table.name] = version
                self._written[table.name] = version
        return version

    def publish(self, *tables: sa.Table, partitions: list[str] | None = None, audit: bool = True,
                max_workers: int = 1) -> dict[str, int]:
        """Leva as partições auditadas de cada tabela ao Delta.

        As partições e a auditoria de toda tabela são conferidas antes do primeiro commit. Depois,
        por tabela, ``create_table`` se não existe, ``reconcile`` e ``export_partition`` por
        partição, que registra no log o arquivo que o motor gravou, com a contagem da auditoria em
        ``expected_rows`` e as colunas ``Double`` com valor não finito sem mínimo e máximo (todas
        as ``Double`` com ``audit=False``). As tabelas correm num pool de ``max_workers``: na
        primeira falha nada novo começa, o que está em curso termina, e a exceção leva o resultado
        de cada tabela numa nota.

        Exemplo:

        .. code-block:: python

            run.publish(Projetado.__table__, partitions=["2026-08-31"])   # {"cad_...": 58}

        :param tables: as tabelas do modelo, com as partições no sandbox.
        :param partitions: os valores de partição a publicar; ``None`` numa tabela sem partição,
            que é substituída inteira.
        :param audit: ``audit=False`` dispensa a exigência da auditoria aprovada, com um aviso no
            log.
        :param max_workers: quantas tabelas correm ao mesmo tempo.
        :return: ``{tabela: versão}``, com a versão do último commit de cada tabela.
        :raises ContractError: um valor de ``partitions`` fora da regra da partição,
            ``partitions`` numa tabela sem partição, ou ``None`` numa tabela particionada.
        :raises AuditFailed: uma tabela sem a auditoria aprovada nessas partições, na mesma ordem,
            na própria execução.
        :raises ExecutionConflict: uma alteração de dados na tabela desde a versão fixada,
            conferida antes de qualquer commit nela (a compactação e os commits só de metadados
            não contam), ou o commit de outro escritor na mesma partição durante a exportação.
        :raises SchemaDiffRefused: o diff entre o modelo e a tabela é destrutivo, e ``reconcile``
            não altera a tabela.
        :raises RegistrationRefused: uma conferência do arquivo exportado reprovou antes do commit,
            ou a releitura reprovou depois dele e a tabela voltou à versão anterior.
        :raises LogUnavailable: um arquivo do log entre a versão fixada e a atual não existe.
        """
        checked = _checked_partitions(partitions)
        # As partições e a auditoria de toda tabela são conferidas antes do primeiro commit.
        tasks = []
        for table in tables:
            values = self._values(table, checked)
            report = self._approved(table, checked, audit)
            task = functools.partial(self._publish_table, table, values, report)
            tasks.append((table.name, task))
        with self._step("publish"):
            return run_in_pool(tasks, max_workers)

    def publish_redshift(self, *tables: sa.Table, max_workers: int = 1) -> dict[str, int]:
        """Publica no Redshift a versão fixada de cada tabela.

        A configuração é a ``redshift`` da execução. Cada tabela corre numa conexão própria do
        pool de ``max_workers``, com a transação da publicação
        (``serialize_db.publication.publish_redshift``): só as partições alteradas desde a versão
        publicada trocam, e a tabela publicada na versão fixada não muda.

        Exemplo:

        .. code-block:: python

            run.publish(Projetado.__table__, partitions=["2026-08-31"])
            run.publish_redshift(Projetado.__table__)   # {"cad_...": 58}

        :param tables: as tabelas do modelo.
        :param max_workers: quantas tabelas correm ao mesmo tempo.
        :return: ``{tabela: versão}``, com a versão do Delta publicada em cada tabela.
        :raises PublicationError: a execução sem a configuração ``redshift``, sem tocar o
            Redshift; o esquema sem a tabela de controle ``serialize_db_publications``; ou a
            tabela sem versão fixada, que não existia no ambiente na abertura e que a execução
            ainda não gravou no Delta.
        :raises ExecutionConflict: a versão publicada mais nova que a fixada, a linha de controle
            alterada desde a leitura, ou outra publicação da mesma tabela ao mesmo tempo (o
            ``1023``, ou a tabela publicada criada por outra primeira publicação).
        :raises LogUnavailable: um arquivo do log entre a versão publicada e a fixada não existe.
        """
        if self.redshift is None:
            raise PublicationError("publish_redshift precisa da configuração do Redshift: "
                                   "Execution(..., redshift=RedshiftConfig(...))")
        from serialize_db import publication

        with self._lock:
            versions = {name: version for name, version in self.versions.items()
                        if version is not None}
        with self._step("publish_redshift"):
            return publication.publish_redshift(self.db, self.redshift, list(tables),
                                                self.execution_id, max_workers, versions)

    def snapshot(self, name: str) -> None:
        """Marca a execução: ``serialize_db_snapshot`` nos commits seguintes e, no encerramento sem
        erro, a entrada do snapshot com a versão de toda tabela do ambiente.

        Exemplo:

        .. code-block:: python

            run.snapshot("2026T3")

        :param name: o nome do snapshot, pela regra da partição; um nome já presente no arquivo de
            controle é ``ValueError`` na saída do ``with``, depois dos commits.
        :raises ContractError: o nome fora da regra da partição.
        """
        check_partition_value(name)
        with self._lock:
            self._snapshot = name
