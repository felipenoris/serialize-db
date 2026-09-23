"""``serialize_db.execution`` e ``serialize-db run|audit``: o ciclo de uma execução sobre um Delta local.

Os testes gravam sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``): a raiz Delta e o sandbox
ficam numa pasta nova por teste, e o motor DuckDB nasce nela, porque o padrão dele é uma pasta de
``tempfile.mkdtemp``. Os testes da linha de comando apontam a pasta temporária do processo para a
mesma pasta. O modelo é ``Lancamento``, particionado por ``data_base_str``, e ``Projetado``, a tabela
que o pipeline grava, com uma coluna ``Double``; ``Composta`` tem chave primária de duas colunas.

Eles conferem a abertura com as versões fixadas, a recusa da partição e do ``execution_id`` fora da
regra, as partições anteriores, as faixas de ``next_ids``, a ingestão de várias tabelas em sessões
a mais, a auditoria exigida e a reprovada, a reexecução, os conflitos, o pool da publicação, as
colunas não finitas e o modo passados à exportação, os metadados de commit, o snapshot e a linha de
comando. O motor de mentira registra as chamadas que a execução faz. A extensão ``delta`` do DuckDB
precisa estar na pasta de extensões.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import logging
import tempfile
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from conftest import LocalLocation
from serialize_db import cli, delta, schema
from serialize_db.audit import AuditReport
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.errors import AuditFailed, ContractError, ExecutionConflict
from serialize_db.execution import Database, Execution

pytestmark = pytest.mark.local

INFO = {"serialize_db": {"partition_by": ["data_base_str"], "partition_source": "data_base"}}


class Base(DeclarativeBase):
    pass


class Lancamento(Base):
    __tablename__ = "cad_lancamentos"
    __table_args__ = {"info": INFO}
    id_lancamento: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    data_base: Mapped[dt.date] = mapped_column(sa.Date)
    valor: Mapped[float] = mapped_column(sa.Double)
    data_base_str: Mapped[str] = mapped_column(sa.String(10))


class Projetado(Base):
    __tablename__ = "cad_lancamentos_projetados"
    __table_args__ = {"info": INFO}
    id_lancamento: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    data_base: Mapped[dt.date] = mapped_column(sa.Date)
    valor: Mapped[float] = mapped_column(sa.Double)
    data_base_str: Mapped[str] = mapped_column(sa.String(10))


class Composta(Base):
    __tablename__ = "rel_composta"
    id_a: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    id_b: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)


ENTRIES = Lancamento.__table__
PROJECTED = Projetado.__table__
MONTHS = ["2026-05-31", "2026-06-30", "2026-07-31", "2026-08-31", "2026-09-30"]


def rows(table: sa.Table, value: str, ids: range, valor: list[float] | None = None) -> pa.Table:
    """Linhas da partição ``value`` com os ids pedidos, no contrato da tabela."""
    data = pa.table({
        "id_lancamento": pa.array(list(ids), pa.int64()),
        "data_base": pa.array([dt.date.fromisoformat(value)] * len(ids), pa.date32()),
        "valor": pa.array(valor if valor is not None else [k / 2 for k in ids], pa.float64()),
        "data_base_str": pa.array([value] * len(ids)),
    })
    return schema.cast(data, table)


@pytest.fixture
def folder(local_location: LocalLocation) -> Path:
    """Uma pasta nova por teste sob a raiz da sessão."""
    path = Path(local_location.child(f"execucao/{uuid.uuid4().hex[:8]}"))
    path.mkdir(parents=True)
    return path


@pytest.fixture
def db(folder: Path) -> Database:
    """O banco do teste, com a tabela de entrada publicada em quatro meses, ids 1 a 40."""
    database = Database(str(folder / "delta"), "prod", Base.metadata)
    uri = database.uri(ENTRIES)
    delta.create_table(uri, ENTRIES, database.storage)
    for index, month in enumerate(MONTHS[:4]):
        ids = range(1 + index * 10, 11 + index * 10)
        delta.publish_partition(uri, ENTRIES, month, rows(ENTRIES, month, ids), {}, database.storage)
    return database


def engine_for(db: Database, folder: Path, execution_id: str) -> DuckDBEngine:
    """O motor DuckDB com o banco e o transbordo na pasta do teste."""
    return DuckDBEngine(DuckDBConfig(temp_directory=str(folder / f"sandbox_{execution_id}")), execution_id, db.storage)


def project(run: Execution, month: str, source: sa.Table = ENTRIES) -> None:
    """O pipeline do teste: a partição da entrada, com ids novos, na tabela projetada do sandbox."""
    with run.sandbox.stream(sa.select(source).where(source.c.data_base_str == month)) as stream, \
            run.sandbox.loader(PROJECTED) as loader:
        for batch in stream:
            ids = run.next_ids(PROJECTED, batch.num_rows)
            loader.write(batch.set_column(0, "id_lancamento", pa.array(list(ids), pa.int64())))


class FakeEngine:
    """Um motor de mentira que registra as chamadas; a exportação grava a partição pelo delta-rs."""

    def __init__(self, storage: object, nonfinite: dict | None = None) -> None:
        self.execution_id = "exec-mentira"
        self.storage = storage
        self.calls: list[tuple] = []
        self.nonfinite = nonfinite or {}
        self.lock = threading.Lock()

    @contextlib.contextmanager
    def session(self) -> Iterator[None]:
        yield None

    def new_session(self) -> FakeEngine:
        return self

    def __enter__(self) -> FakeEngine:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def ingest(self, table: sa.Table, uri: str, version: int, partitions: list[str] | None = None, materialize: bool = False) -> None:
        with self.lock:
            self.calls.append(("ingest", table.name, version, partitions, threading.get_ident()))

    def audit(self, table: sa.Table, partitions: list[str] | None, uri: str | None = None, version: int | None = None,
              foreign_keys: bool = False, key_scope: str | None = None, referenced: dict | None = None) -> AuditReport:
        self.calls.append(("audit", table.name, tuple(partitions or ())))
        totals = {value: {"linhas": 3} for value in partitions or [None]}
        nonfinite = {value: self.nonfinite.get(value, ()) for value in partitions or [None]}
        return AuditReport(table.name, tuple(partitions or ()), (), (), nonfinite, totals)

    def export_partition(self, table: sa.Table, uri: str, value: str | None, metadata: dict, mode: str,
                         expected_rows: int | None = None, columns_without_min_max: tuple = ()) -> int:
        with self.lock:
            self.calls.append(("export", table.name, value, mode, expected_rows, tuple(columns_without_min_max)))
        return delta.publish_partition(uri, table, value, rows(table, value, range(1, 4)), metadata, self.storage)

    def cleanup(self) -> None:
        self.calls.append(("cleanup",))


# ---------------------------------------------------------------- a abertura


def test_execution_opens_every_table_and_fixes_versions(db: Database, folder: Path, caplog: pytest.LogCaptureFixture) -> None:
    """``versions`` com as tabelas existentes e ``None`` nas ausentes; o log com as versões e o resumo."""
    engine = FakeEngine(db.storage)
    with caplog.at_level(logging.INFO, logger="serialize_db.execution"):
        with Execution(db, engine, "2026-08-31", "exec-2026-09-05") as run:
            assert run.versions == {"cad_lancamentos": 4, "cad_lancamentos_projetados": None, "rel_composta": None}
            assert run.execution_id == "exec-2026-09-05" and run.sandbox is engine
    messages = [record.getMessage() for record in caplog.records]
    assert "execução exec-2026-09-05 aberta: partição 2026-08-31" in messages[0]
    assert "versões lidas {'cad_lancamentos': 4}" in messages[-1] and engine.calls[-1] == ("cleanup",)
    generated = Execution(db, FakeEngine(db.storage), "2026-08-31").execution_id
    assert generated.startswith(f"exec-{dt.datetime.now(dt.timezone.utc).date().isoformat()}-")
    assert db.uri(ENTRIES) == db.storage.uri + "/prod/cad_lancamentos" and not db.uri(ENTRIES).endswith("/")


def test_execution_refuses_an_invalid_partition(db: Database) -> None:
    """O valor vazio, com ``/``, ``=``, espaço, ``'``, ``:``, ``%`` ou acento, começado por ``.``, ``_`` ou
    ``-``, ou acima do ``String(n)`` é recusado antes do sandbox, e o ``execution_id`` fora da regra
    também; ``2026-08-31`` e ``2026-Q1`` passam."""
    for value in ["", "2026/08/31", "a=b", "a b", "d'agua", "a:b", "a%b", "ação", ".x", "_x", "-x", "2026-08-31-mais"]:
        with pytest.raises(ContractError):
            Execution(db, FakeEngine(db.storage), value, "exec-1")
    with pytest.raises(ContractError):
        Execution(db, FakeEngine(db.storage), "2026-08-31", "exec 1")
    assert Execution(db, FakeEngine(db.storage), "2026-Q1", "exec-1").partition == "2026-Q1"
    with pytest.raises(ContractError):
        Database(db.root, "pr od", Base.metadata)


def test_previous_partitions_up_to_the_execution_partition(db: Database) -> None:
    """Só valores até a partição da execução, os ``n`` últimos, na ordem de texto."""
    with Execution(db, FakeEngine(db.storage), "2026-07-31") as run:
        assert run.previous_partitions(ENTRIES, 2) == ["2026-06-30", "2026-07-31"]
        assert run.previous_partitions(ENTRIES, 12) == ["2026-05-31", "2026-06-30", "2026-07-31"]
        assert run.previous_partitions(PROJECTED, 3) == []
        with pytest.raises(ContractError, match="sem partição"):
            run.previous_partitions(Composta.__table__, 1)


def test_next_ids_are_disjoint_across_threads(db: Database) -> None:
    """Duas threads têm faixas disjuntas, a primeira acima do máximo das estatísticas; a tabela nova
    começa em 1; a chave composta é ``ContractError``."""
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        ranges: list[range] = []

        def take() -> None:
            for _ in range(50):
                ranges.append(run.next_ids(ENTRIES, 3))

        threads = [threading.Thread(target=take) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        numbers = [number for block in ranges for number in block]
        assert len(numbers) == len(set(numbers)) == 300 and min(numbers) == 41
        assert run.next_ids(PROJECTED, 2) == range(1, 3)
        with pytest.raises(ContractError, match="chave composta"):
            run.next_ids(Composta.__table__, 1)


def test_ingest_of_several_tables_uses_extra_sessions(db: Database, folder: Path) -> None:
    """``ingest`` de uma tabela usa a sessão principal; de várias, uma sessão a mais por tabela, em
    paralelo, e a principal lê todas; a falha de uma leva o resultado das outras numa nota."""
    uri = db.uri(PROJECTED)
    delta.create_table(uri, PROJECTED, db.storage)
    delta.publish_partition(uri, PROJECTED, MONTHS[0], rows(PROJECTED, MONTHS[0], range(1, 6)), {}, db.storage)
    engine = engine_for(db, folder, "exec-1")
    with Execution(db, engine, "2026-08-31", "exec-1") as run:
        run.ingest(ENTRIES, PROJECTED, partitions=[MONTHS[0]])
        counts = run.sandbox.query("SELECT (SELECT count(*) FROM cad_lancamentos) AS a, (SELECT count(*) FROM cad_lancamentos_projetados) AS b")
        assert counts.to_pylist() == [{"a": 10, "b": 5}]
    fake = FakeEngine(db.storage)
    with Execution(db, fake, "2026-08-31") as run:
        with pytest.raises(Exception) as failure:
            run.ingest(ENTRIES, Composta.__table__)
        assert "não existe" in str(failure.value)
        assert "cad_lancamentos: concluída" in failure.value.__notes__[0]
        assert "rel_composta: falhou" in failure.value.__notes__[0]


# ---------------------------------------------------------------- a auditoria e a publicação


def test_publish_requires_the_audit(db: Database, folder: Path, caplog: pytest.LogCaptureFixture) -> None:
    """``publish`` sem a auditoria aprovada é ``AuditFailed``; com ``audit=False`` passa e o log registra."""
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        with pytest.raises(AuditFailed, match="exige a auditoria"):
            run.publish(PROJECTED, partitions=["2026-08-31"])
        with caplog.at_level(logging.WARNING, logger="serialize_db.execution"):
            assert run.publish(PROJECTED, partitions=["2026-08-31"], audit=False) == {PROJECTED.name: 1}
        assert any("sem auditoria" in record.getMessage() for record in caplog.records)
        with pytest.raises(ContractError, match="exige partitions"):
            run.publish(PROJECTED, audit=False)


def test_failed_audit_leaves_the_delta_untouched(db: Database, folder: Path) -> None:
    """A auditoria reprovada levanta ``AuditFailed``, a versão da tabela não muda e o sandbox sai."""
    engine = engine_for(db, folder, "exec-1")
    with pytest.raises(AuditFailed, match="chave_id_lancamento"):
        with Execution(db, engine, "2026-08-31", "exec-1") as run:
            run.sandbox.load(PROJECTED, pa.concat_tables([rows(PROJECTED, "2026-08-31", range(1, 4))] * 2))
            run.audit(PROJECTED, ["2026-08-31"])
            run.publish(PROJECTED, partitions=["2026-08-31"])
    assert not delta.table_exists(db.uri(PROJECTED), db.storage)
    assert not (folder / "sandbox_exec-1" / "exec-1.duckdb").exists()


def test_rerun_with_the_same_execution_id_produces_the_same_rows(db: Database, folder: Path) -> None:
    """A reexecução com o mesmo ``execution_id`` publica as mesmas linhas, com ids que podem diferir,
    numa versão a mais."""
    totals = []
    for attempt in range(2):
        engine = engine_for(db, folder, f"exec-rerun-{attempt}")
        with Execution(db, engine, "2026-08-31", f"exec-rerun-{attempt}") as run:
            run.ingest(ENTRIES, partitions=["2026-08-31"])
            project(run, "2026-08-31")
            run.audit(PROJECTED, ["2026-08-31"])
            versions = run.publish(PROJECTED, partitions=["2026-08-31"])
        dataset = delta.open_table(db.uri(PROJECTED), db.storage).to_pyarrow_dataset().to_table()
        totals.append((versions[PROJECTED.name], dataset.num_rows, sorted(dataset.column("valor").to_pylist())))
    assert totals[0][1:] == totals[1][1:] and totals[1][0] == totals[0][0] + 1


def test_publish_aborts_when_data_changed_since_open(db: Database, folder: Path) -> None:
    """Um ``append`` de outra execução depois da abertura é ``ExecutionConflict`` sem commit; uma
    compactação não é, e a versão fixada avança."""
    uri = db.uri(ENTRIES)
    engine = FakeEngine(db.storage)
    with Execution(db, engine, "2026-08-31") as run:
        from deltalake import write_deltalake

        write_deltalake(delta.open_table(uri, db.storage), rows(ENTRIES, "2026-08-31", range(100, 103)), mode="append")
        before = delta.open_table(uri, db.storage).version()
        run.audit(ENTRIES, ["2026-08-31"])
        with pytest.raises(ExecutionConflict, match="2026-08-31"):
            run.publish(ENTRIES, partitions=["2026-08-31"])
        assert delta.open_table(uri, db.storage).version() == before
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        delta.compact(uri, ENTRIES, ["2026-08-31"], db.storage)
        run.audit(ENTRIES, ["2026-08-31"])
        compacted = delta.open_table(uri, db.storage).version()
        assert run.publish(ENTRIES, partitions=["2026-08-31"]) == {ENTRIES.name: compacted + 1}


def test_two_executions_on_the_same_partition_conflict(db: Database) -> None:
    """Duas execuções abertas na mesma versão publicam a mesma partição: a segunda aborta."""
    first = Execution(db, FakeEngine(db.storage), "2026-08-31", "exec-a")
    second = Execution(db, FakeEngine(db.storage), "2026-08-31", "exec-b")
    with first, second:
        for run in (first, second):
            run.audit(ENTRIES, ["2026-08-31"])
        first.publish(ENTRIES, partitions=["2026-08-31"])
        with pytest.raises(ExecutionConflict):
            second.publish(ENTRIES, partitions=["2026-08-31"])


def test_publish_with_two_workers_matches_one(db: Database, folder: Path) -> None:
    """O mesmo resultado com ``max_workers=1`` e ``2``; a falha de uma tabela deixa as outras terminarem
    e leva o resultado de cada uma numa nota."""
    results = {}
    for workers in (1, 2):
        database = Database(str(folder / f"delta_{workers}"), "prod", Base.metadata)
        with Execution(database, FakeEngine(database.storage), "2026-08-31") as run:
            results[workers] = run.publish(ENTRIES, PROJECTED, partitions=["2026-08-31"], audit=False, max_workers=workers)
    assert results[1] == results[2] == {ENTRIES.name: 1, PROJECTED.name: 1}

    # Com um worker, a segunda tabela falha: a primeira terminou e a terceira nem começa.
    class FailingEngine(FakeEngine):
        def export_partition(self, table: sa.Table, *args: object, **kwargs: object) -> int:
            if table.name == PROJECTED.name:
                raise ExecutionConflict("conflito plantado")
            return super().export_partition(table, *args, **kwargs)

    extra = sa.Table("cad_extra", Base.metadata, *[column._copy() for column in PROJECTED.columns], info=INFO)
    try:
        database = Database(str(folder / "delta_falha"), "prod", Base.metadata)
        engine = FailingEngine(database.storage)
        with Execution(database, engine, "2026-08-31") as run:
            with pytest.raises(ExecutionConflict, match="conflito plantado") as failure:
                run.publish(ENTRIES, PROJECTED, extra, partitions=["2026-08-31"], audit=False, max_workers=1)
        note = failure.value.__notes__[0]
        assert "cad_lancamentos: concluída" in note and "cad_lancamentos_projetados: falhou" in note
        assert "cad_extra: cancelada" in note
        assert [call[1] for call in engine.calls if call[0] == "export"] == ["cad_lancamentos"]
    finally:
        Base.metadata.remove(extra)
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        with pytest.raises(ContractError, match="tabela sem partição"):
            run.publish(ENTRIES, Composta.__table__, partitions=["2026-08-31"], audit=False, max_workers=2)


def test_publish_passes_the_nonfinite_columns_to_the_export(db: Database, folder: Path) -> None:
    """O motor recebe, por partição, as colunas ``Double`` não finitas da auditoria e a contagem dela;
    a partição sem elas recebe a lista vazia; com ``audit=False``, todas as ``Double`` e nenhuma
    contagem; no motor DuckDB, a partição com ``NaN`` sai sem o mínimo e o máximo da coluna."""
    engine = FakeEngine(db.storage, nonfinite={"2026-07-31": ("valor",)})
    with Execution(db, engine, "2026-08-31") as run:
        run.audit(PROJECTED, ["2026-07-31", "2026-08-31"])
        run.publish(PROJECTED, partitions=["2026-07-31", "2026-08-31"])
        run.publish(PROJECTED, partitions=["2026-09-30"], audit=False)
    exports = [call[2:] for call in engine.calls if call[0] == "export"]
    assert exports == [("2026-07-31", "register", 3, ("valor",)), ("2026-08-31", "register", 3, ()),
                       ("2026-09-30", "register", None, ("valor",))]

    real = engine_for(db, folder, "exec-nan")
    with Execution(db, real, "2026-08-31", "exec-nan") as run:
        run.sandbox.load(PROJECTED, rows(PROJECTED, "2026-06-30", range(100, 103), valor=[1.0, float("nan"), 2.0]))
        report = run.audit(PROJECTED, ["2026-06-30"])
        assert report.nonfinite_columns == {"2026-06-30": ("valor",)}
        run.publish(PROJECTED, partitions=["2026-06-30"])
    actions = pa.table(delta.open_table(db.uri(PROJECTED), db.storage).get_add_actions(flatten=True))
    june = actions.filter(pa.compute.equal(actions.column("partition.data_base_str"), "2026-06-30"))
    assert june.column("max.valor").null_count == 1 and june.column("max.id_lancamento").to_pylist() == [102]


def test_export_mode_resolution(db: Database, monkeypatch: pytest.MonkeyPatch) -> None:
    """O argumento de ``publish`` vence o de ``Execution``, que vence ``SERIALIZE_DB_EXPORT_MODE``, que
    vence ``register``; um modo fora dos dois é ``ContractError``."""
    monkeypatch.delenv("SERIALIZE_DB_EXPORT_MODE", raising=False)
    modes = []
    for execution_mode, variable, publish_mode in ((None, None, None), (None, "rewrite", None),
                                                   ("register", "rewrite", None), ("register", "rewrite", "rewrite")):
        if variable is None:
            monkeypatch.delenv("SERIALIZE_DB_EXPORT_MODE", raising=False)
        else:
            monkeypatch.setenv("SERIALIZE_DB_EXPORT_MODE", variable)
        engine = FakeEngine(db.storage)
        with Execution(db, engine, "2026-08-31", export_mode=execution_mode) as run:
            run.publish(PROJECTED, partitions=["2026-08-31"], audit=False, export_mode=publish_mode)
        modes.append([call[3] for call in engine.calls if call[0] == "export"][0])
    assert modes == ["register", "rewrite", "register", "rewrite"]
    with pytest.raises(ContractError, match="export_mode"):
        Execution(db, FakeEngine(db.storage), "2026-08-31", export_mode="copy")


def test_commit_metadata_in_history(db: Database) -> None:
    """``serialize_db_execution_id`` e ``serialize_db_input_versions`` no ``history``;
    ``serialize_db_snapshot`` só na execução marcada."""
    with Execution(db, FakeEngine(db.storage), "2026-08-31", "exec-comum") as run:
        run.publish(PROJECTED, partitions=["2026-08-31"], audit=False)
    history = delta.open_table(db.uri(PROJECTED), db.storage).history(1)[0]
    assert history["serialize_db_execution_id"] == "exec-comum" and "serialize_db_snapshot" not in history
    assert json.loads(history["serialize_db_input_versions"]) == {"cad_lancamentos": 4}
    with Execution(db, FakeEngine(db.storage), "2026-08-31", "exec-marcada") as run:
        run.snapshot("2026T3")
        run.publish(PROJECTED, partitions=["2026-08-31"], audit=False)
    assert delta.open_table(db.uri(PROJECTED), db.storage).history(1)[0]["serialize_db_snapshot"] == "2026T3"


def test_snapshot_writes_the_control_file_at_exit(db: Database) -> None:
    """O snapshot marcado grava, no encerramento, a versão de toda tabela do ambiente, uma vez; a
    execução que falha não o grava."""
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        run.snapshot("2026T3")
        run.publish(PROJECTED, partitions=["2026-08-31"], audit=False)
        assert delta.read_snapshots(db.storage, "prod")[1] is None  # só no encerramento
    control, _ = delta.read_snapshots(db.storage, "prod")
    assert control == {"snapshots": {"2026T3": {"cad_lancamentos": 4, "cad_lancamentos_projetados": 1}}}
    with pytest.raises(RuntimeError):
        with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
            run.snapshot("2026T4")
            raise RuntimeError("falha do pipeline")
    assert list(delta.read_snapshots(db.storage, "prod")[0]["snapshots"]) == ["2026T3"]


# ---------------------------------------------------------------- a linha de comando


def pipeline_projetado(run: Execution) -> None:
    """O pipeline que ``serialize-db run`` chama: projeta, audita e publica a partição da execução."""
    run.ingest(ENTRIES, partitions=[run.partition])
    project(run, run.partition)
    run.audit(PROJECTED, [run.partition])
    run.publish(PROJECTED, partitions=[run.partition])


def pipeline_com_chave_repetida(run: Execution) -> None:
    """Um pipeline que grava uma chave repetida e reprova na auditoria."""
    run.sandbox.load(PROJECTED, pa.concat_tables([rows(PROJECTED, run.partition, range(1, 3))] * 2))
    run.audit(PROJECTED, [run.partition])


def pipeline_em_conflito(run: Execution) -> None:
    """Um pipeline que publica depois de outra execução gravar a mesma tabela."""
    uri = run.db.uri(ENTRIES)
    delta.publish_partition(uri, ENTRIES, run.partition, rows(ENTRIES, run.partition, range(900, 902)), {}, run.db.storage)
    run.publish(ENTRIES, partitions=[run.partition], audit=False)


def test_cli_run_parses_and_exits_by_result(db: Database, folder: Path, monkeypatch: pytest.MonkeyPatch,
                                            capsys: pytest.CaptureFixture) -> None:
    """``serialize-db run`` sai com 0 no pipeline que publica, 1 na auditoria reprovada e 2 no conflito,
    na partição e no ``execution_id`` fora da regra e sem ``--metadata``, sem traceback; o
    ``--export-mode`` chega a ``Execution``."""
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    monkeypatch.delenv("SERIALIZE_DB_ROOT", raising=False)
    common = ["run", "--root", db.root, "--environment", "prod", "--metadata", "test_execution:Base.metadata"]
    assert cli.main([*common, "--partition", "2026-08-31", "--export-mode", "rewrite", "test_execution:pipeline_projetado"]) == 0
    history = delta.open_table(db.uri(PROJECTED), db.storage).history(1)[0]
    assert history["operation"] == "WRITE"  # rewrite grava pelo write_deltalake
    assert cli.main([*common, "--partition", "2026-07-31", "test_execution:pipeline_com_chave_repetida"]) == 1
    assert cli.main([*common, "--partition", "2026-06-30", "test_execution:pipeline_em_conflito"]) == 2
    for arguments in (["--partition", "2026/08/31"], ["--partition", "2026-08-31", "--execution-id", "exec 1"]):
        with pytest.raises(SystemExit) as exit_info:
            cli.main([*common, *arguments, "test_execution:pipeline_projetado"])
        assert exit_info.value.code == 2
    with pytest.raises(SystemExit) as exit_info:
        cli.main(["run", "--root", db.root, "--partition", "2026-08-31", "test_execution:pipeline_projetado"])
    assert exit_info.value.code == 2
    with pytest.raises(SystemExit) as exit_info:
        cli.main([*common, "--partition", "2026-08-31", "test_execution:pipeline_inexistente"])
    assert exit_info.value.code == 2
    assert "Traceback" not in capsys.readouterr().err


def test_cli_audit_prints_the_sql_and_audits_the_published_version(db: Database, folder: Path, monkeypatch: pytest.MonkeyPatch,
                                                                   capsys: pytest.CaptureFixture) -> None:
    """``serialize-db audit --sql`` imprime o texto das verificações no dialeto, sem armazenamento; sem
    ``--sql``, audita a versão publicada e sai com 0 na aprovação."""
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    base = ["audit", "--metadata", "test_execution:Base.metadata", "--table", "cad_lancamentos"]
    assert cli.main([*base, "--engine", "redshift", "--partitions", "2026-08-31", "--sql"]) == 0
    printed = capsys.readouterr().out
    assert "-- linhas" in printed and "to_char(" in printed and '"{prefix}cad_lancamentos"' in printed
    assert cli.main([*base, "--partitions", "2026-08-31", "--root", db.root, "--environment", "prod"]) == 0
    printed = capsys.readouterr().out
    assert "cad_lancamentos na versão 4:" in printed and "chave_id_lancamento_publicada: aprovada" in printed
    assert cli.main([*base, "--table", "nao_existe", "--sql"]) == 2
