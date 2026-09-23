"""A execução de um pipeline: o banco (``Database``) e o ciclo de uma execução (``Execution``).

``Database`` junta a raiz do banco, o ambiente (``prod``, ``dev``) e o ``MetaData`` dos modelos do
cliente, e monta os caminhos: a pasta de cada tabela é ``<raiz>/<ambiente>/<tabela>``. ``Execution``
é o gerenciador de contexto de uma execução: na entrada abre toda tabela do ambiente, fixa a versão
de cada uma e cria o sandbox do motor; na saída descarta o sandbox, grava o snapshot marcado e o
resumo no log. Entre os dois, o pipeline chama as primitivas: ``ingest`` traz as tabelas presas à
versão fixada, ``sandbox`` é o motor onde ele roda ``stream``, ``loader``, ``query`` e ``load``,
``next_ids`` dá as faixas da chave sequencial, ``audit`` confere o contrato e ``publish`` leva as
partições auditadas ao Delta.

As primitivas podem ser chamadas de qualquer thread: cada comando do motor corre na sessão única,
sob o lock dela, e o estado mutável da execução (as versões, as auditorias aprovadas, o alocador)
fica sob um lock próprio. A partição e o ``execution_id`` seguem ``schema.PARTITION_VALUE``, porque
viram nome de pasta e literal SQL.

Exemplo:

.. code-block:: python

    import sqlalchemy as sa

    from serialize_db.execution import Database, Execution

    db = Database("s3://bucket/projeto/delta", "prod", Base.metadata)
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
import os
import threading
import time
import uuid
from collections.abc import Callable, Iterator, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from contextlib import contextmanager

import pyarrow as pa
import sqlalchemy as sa
from deltalake import DeltaTable

from serialize_db import delta
from serialize_db.audit import AuditReport, KeyScope
from serialize_db.engine import Engine, ExportMode
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.errors import AuditFailed, ContractError, ExecutionConflict, SandboxError
from serialize_db.schema import (
    check_partition_value,
    double_columns,
    sequential_key,
    table_options,
)
from serialize_db.storage import Storage, prepare_environment

__all__ = ["Database", "Execution"]

log = logging.getLogger("serialize_db.execution")

_EXPORT_MODES = ("register", "rewrite")


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

        db = Database("s3://bucket/projeto/delta", "prod", Base.metadata)
        db.uri(Lancamento.__table__)   # "s3://bucket/projeto/delta/prod/cad_lancamentos"
        db.control_path()              # "prod/_serialize_db/snapshots.json"
    """

    root: str
    environment: str
    """O ambiente, ``prod`` ou ``dev``: as execuções de um não tocam as tabelas do outro."""
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
        """A pasta da tabela, ``<raiz>/<ambiente>/<tabela>``, sem barra final."""
        return self.storage.uri_of(self.storage.join(self.environment, table.name))

    def control_path(self) -> str:
        """O arquivo de controle dos snapshots do ambiente, relativo à raiz."""
        return self.storage.join(self.environment, delta.CONTROL_FILE)

    def staging_prefix(self, execution_id: str) -> str:
        """Os arquivos intermediários de uma execução, relativos à raiz."""
        return self.storage.join(self.environment, "staging", execution_id)

    def publication_prefix(self, execution_id: str) -> str:
        """Os manifestos da publicação no Redshift de uma execução, relativos à raiz."""
        return self.storage.join(self.environment, "publicacao", execution_id)

    def archive_prefix(self, name: str) -> str:
        """A pasta de arquivo de um snapshot do banco, relativa à raiz."""
        return self.storage.join(self.environment, "arquivo", name)

    def tables(self) -> list[sa.Table]:
        """As tabelas dos modelos, na ordem das chaves estrangeiras."""
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


def _checked_mode(mode: str) -> ExportMode:
    """O ``export_mode`` entre ``register`` e ``rewrite``."""
    if mode not in _EXPORT_MODES:
        raise ContractError(f"export_mode {mode!r}: use 'register' ou 'rewrite'")
    return mode


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


# ---------------------------------------------------------------- o pool das tabelas

# Uma tarefa do pool: o nome da tabela e a função que a processa.
_Task = tuple[str, Callable[[], object]]


def _outcome(future: Future) -> str:
    """O resultado de uma tarefa terminada ou cancelada, para a nota da exceção."""
    if future.cancelled():
        return "cancelada"
    error = future.exception()
    if error is None:
        return "concluída"
    return f"falhou: {type(error).__name__}: {error}"


def _raise_with_outcomes(outcomes: Mapping[str, str], error: BaseException) -> None:
    """Relança a exceção com o resultado de cada tabela numa nota: os commits feitos ficam, porque o
    Delta não tem transação entre tabelas."""
    lines = []
    for name, outcome in sorted(outcomes.items()):
        lines.append(f"{name}: {outcome}")
    error.add_note("resultado por tabela: " + "; ".join(lines))
    raise error


def _outcomes_of(futures: Mapping[Future, str]) -> dict[str, str]:
    """O resultado de cada tarefa terminada, pelo nome da tabela."""
    outcomes = {}
    for future, name in futures.items():
        outcomes[name] = _outcome(future)
    return outcomes


@dataclasses.dataclass
class _PoolState:
    """O estado do pool das tabelas: uma tarefa começa só com um worker livre e nenhuma falha,
    então, na primeira falha, o que está em curso termina e o que não começou fica de fora."""

    pool: ThreadPoolExecutor
    workers: int
    running: dict[Future, str] = dataclasses.field(default_factory=dict)
    finished: dict[Future, str] = dataclasses.field(default_factory=dict)
    failure: BaseException | None = None

    def start(self, waiting: list[_Task]) -> None:
        """Começa as tarefas que cabem nos workers livres, enquanto não houve falha."""
        while waiting and self.failure is None and len(self.running) < self.workers:
            name, action = waiting.pop(0)
            self.running[self.pool.submit(action)] = name

    def collect(self) -> None:
        """Espera a próxima tarefa terminar e guarda a primeira falha."""
        done, _ = wait(self.running, return_when=FIRST_COMPLETED)
        for future in done:
            self.finished[future] = self.running.pop(future)
            if future.exception() is not None and self.failure is None:
                self.failure = future.exception()


def _run_in_pool(tasks: list[_Task], max_workers: int) -> dict[str, object]:
    """Roda as tarefas num pool de ``max_workers`` e devolve o resultado de cada uma pelo nome.

    Uma tarefa começa só com um worker livre e nenhuma falha: na primeira falha, as tarefas em
    curso terminam, as que não começaram ficam canceladas, e a exceção sobe com o resultado de cada
    tarefa numa nota. Com um worker por tarefa, todas começam juntas e todas terminam.
    """
    waiting = list(tasks)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        state = _PoolState(pool, max_workers)
        state.start(waiting)
        while state.running:
            state.collect()
            state.start(waiting)
    outcomes = _outcomes_of(state.finished)
    for name, _ in waiting:
        outcomes[name] = "cancelada"
    if state.failure is not None:
        _raise_with_outcomes(outcomes, state.failure)
    results = {}
    for future, name in state.finished.items():
        results[name] = future.result()
    return results


# ---------------------------------------------------------------- a execução


class Execution:
    """O ciclo de uma execução: as versões fixadas, o sandbox, a auditoria e a publicação.

    ``engine`` é o nome do motor (``"duckdb"``) ou um motor já construído, para os testes.
    ``execution_id`` ausente vira ``exec-<AAAA-MM-DD>-<uuid8>``. ``export_mode`` é o modo dos
    ``publish`` que não informam o seu.

    Exemplo:

    .. code-block:: python

        with Execution(db, "duckdb", "2026-08-31") as run:
            run.versions   # {"cad_lancamentos": 143, "cad_lancamentos_projetados": None}
    """

    def __init__(self, db: Database, engine: str | Engine, partition: str,
                 execution_id: str | None = None, export_mode: ExportMode | None = None) -> None:
        self.db = db
        self.partition = _checked_partition(partition, db)
        self.execution_id = check_partition_value(execution_id or _new_execution_id())
        self._engine = engine
        self._export_mode = _checked_mode(export_mode) if export_mode is not None else None
        self.versions: dict[str, int | None] = {}
        self.sandbox: Engine | None = None
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
            raise ContractError("o motor redshift é a etapa 5, ainda não implementada")
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
        """Os ``n`` últimos valores de partição da tabela até a partição da execução, inclusive, na
        ordem de texto, lidos das ações ``add`` da versão fixada; vazio numa tabela que não existe.

        Exemplo:

        .. code-block:: python

            run.previous_partitions(Lancamento.__table__, 12)   # ["2025-09-30", ..., "2026-08-31"]
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
        return up_to_partition[-n:] if n > 0 else []

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
        """Traz as tabelas ao sandbox na versão fixada; sem ``partitions``, a tabela inteira.

        Uma tabela entra na sessão principal; mais de uma entram todas em paralelo, cada uma numa
        sessão a mais do motor, e a chamada volta quando todas terminam. Uma falha não cancela as
        que já rodam: todas terminam, e a exceção leva o resultado de cada tabela numa nota.

        Exemplo:

        .. code-block:: python

            run.ingest(Contrato.__table__, Operacao.__table__)   # views sobre a versão fixada
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
            _run_in_pool(tasks, max_workers=max(len(tables), 1))

    def published(self, table: sa.Table) -> sa.FromClause:
        """A versão fixada da tabela como origem de consulta, sem ocupar nome no sandbox: é por ela
        que o pipeline lê as partições publicadas da tabela que ele mesmo grava."""
        return self.sandbox.published(table, self._uri(table), self._version(table))

    def _first_id(self, table: sa.Table, key: sa.Column) -> int:
        """O primeiro id de ``next_ids``: o maior da versão fixada mais um, ou 1 na tabela nova."""
        if table.name not in self._tables:
            return 1
        return delta.max_key(self._tables[table.name], key.name) + 1

    def next_ids(self, table: sa.Table, n: int) -> range:
        """Uma faixa de ``n`` inteiros contíguos da chave sequencial, acima do maior da versão
        fixada.

        A chave é a chave primária inteira de uma coluna; outra chave é ``ContractError``. O maior
        valor é lido uma vez por tabela, das estatísticas do log; a tabela nova começa em 1. As
        faixas de threads paralelas não se sobrepõem, e as de uma reexecução diferem.

        Exemplo:

        .. code-block:: python

            ids = run.next_ids(Projetado.__table__, batch.num_rows)   # range(1001, 1101)
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
        """Roda a auditoria do motor e guarda o relatório aprovado; a reprovação é ``AuditFailed``.

        O relatório, com o SQL de cada verificação e as amostras, vai para o log. A contagem por
        partição e as colunas ``Double`` com valor não finito do relatório aprovado são as que
        ``publish`` passa à exportação.

        Exemplo:

        .. code-block:: python

            run.audit(Projetado.__table__, ["2026-08-31"], foreign_keys=True)
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

    def _resolved_mode(self, export_mode: ExportMode | None) -> ExportMode:
        """O modo, nesta ordem: o argumento de ``publish``, o de ``Execution``,
        ``SERIALIZE_DB_EXPORT_MODE`` e ``"register"``."""
        if export_mode is not None:
            return _checked_mode(export_mode)
        if self._export_mode is not None:
            return self._export_mode
        return _checked_mode(os.environ.get("SERIALIZE_DB_EXPORT_MODE") or "register")

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

    def _publish_table(self, table: sa.Table, values: list[str | None], report: AuditReport | None,
                       mode: ExportMode) -> int:
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
            version = self.sandbox.export_partition(table, uri, value, metadata, mode, expected,
                                                    nonfinite)
            with self._lock:
                self.versions[table.name] = version
                self._written[table.name] = version
        return version

    def publish(self, *tables: sa.Table, partitions: list[str] | None = None, audit: bool = True,
                max_workers: int = 1, export_mode: ExportMode | None = None) -> dict[str, int]:
        """Leva as partições auditadas de cada tabela ao Delta e devolve ``{tabela: versão}``.

        Exige a auditoria aprovada de cada tabela nessas partições na própria execução;
        ``audit=False`` dispensa a exigência e fica no log. Uma alteração de dados na tabela desde
        a versão fixada é ``ExecutionConflict`` sem commit. Depois ``create_table`` se não existe,
        ``reconcile`` e ``export_partition`` por partição, no ``export_mode`` resolvido, com a
        contagem da auditoria em ``expected_rows`` e as colunas ``Double`` com valor não finito sem
        mínimo e máximo (todas as ``Double`` com ``audit=False``). As tabelas correm num pool de
        ``max_workers``: na primeira falha nada novo começa, o que está em curso termina, e a
        exceção leva o resultado de cada tabela numa nota.

        Exemplo:

        .. code-block:: python

            run.publish(Projetado.__table__, partitions=["2026-08-31"])   # {"cad_...": 58}
        """
        checked = _checked_partitions(partitions)
        mode = self._resolved_mode(export_mode)
        # As partições e a auditoria de toda tabela são conferidas antes do primeiro commit.
        tasks = []
        for table in tables:
            values = self._values(table, checked)
            report = self._approved(table, checked, audit)
            task = functools.partial(self._publish_table, table, values, report, mode)
            tasks.append((table.name, task))
        with self._step("publish"):
            return _run_in_pool(tasks, max_workers)

    def snapshot(self, name: str) -> None:
        """Marca a execução: ``serialize_db_snapshot`` nos commits seguintes e, no encerramento sem
        erro, a entrada do snapshot com a versão de toda tabela do ambiente.

        Exemplo:

        .. code-block:: python

            run.snapshot("2026T3")
        """
        check_partition_value(name)
        with self._lock:
            self._snapshot = name
