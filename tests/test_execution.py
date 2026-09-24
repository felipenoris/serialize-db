"""``serialize_db.execution`` e ``serialize-db run|audit``: o ciclo de uma execução sobre um Delta
local.

Os testes gravam sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``): a raiz Delta e o sandbox
ficam numa pasta nova por teste, e o motor DuckDB nasce nela, porque o padrão dele é uma pasta de
``tempfile.mkdtemp``. Os testes da linha de comando apontam a pasta temporária do processo para a
mesma pasta. O modelo é ``Lancamento``, particionado por ``data_base_str``, e ``Projetado``, a
tabela que o pipeline grava, com uma coluna ``Double``; ``Composta`` tem chave primária de duas
colunas.

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
import re
import tempfile
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pytest
import sqlalchemy as sa
from deltalake import write_deltalake
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from conftest import LocalLocation
from serialize_db import cli, delta, schema
from serialize_db.audit import AuditReport
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.errors import (
    AuditFailed,
    ContractError,
    ExecutionConflict,
    PublicationError,
    SandboxError,
)
from serialize_db.execution import Database, Execution
from serialize_db.storage import Storage

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
        data = rows(ENTRIES, month, ids)
        delta.publish_partition(uri, ENTRIES, month, data, {}, database.storage)
    return database


def engine_for(db: Database, folder: Path, execution_id: str) -> DuckDBEngine:
    """O motor DuckDB com o banco e o transbordo na pasta do teste."""
    config = DuckDBConfig(temp_directory=str(folder / f"sandbox_{execution_id}"))
    return DuckDBEngine(config, execution_id, db.storage)


def project(run: Execution, month: str) -> None:
    """O pipeline do teste: a partição da entrada, com ids novos, na tabela projetada do sandbox."""
    statement = sa.select(ENTRIES).where(ENTRIES.c.data_base_str == month)
    with run.sandbox.stream(statement) as stream, run.sandbox.loader(PROJECTED) as loader:
        for batch in stream:
            ids = pa.array(list(run.next_ids(PROJECTED, batch.num_rows)), pa.int64())
            loader.write(batch.set_column(0, "id_lancamento", ids))


class FakeEngine:
    """Um motor de mentira que registra as chamadas; a exportação grava a partição pelo delta-rs.

    ``calls`` guarda o nome de cada chamada na ordem, ``ingests`` a tabela e a thread de cada
    ``ingest``, e ``exports`` os argumentos de cada ``export_partition``. As threads do pool gravam
    nas listas sem lock: ``list.append`` é atômico.
    """

    def __init__(self, storage: Storage,
                 nonfinite: dict[str, tuple[str, ...]] | None = None) -> None:
        self.execution_id = "exec-mentira"
        self.storage = storage
        self.nonfinite = nonfinite or {}
        self.calls: list[str] = []
        self.ingests: list[dict] = []
        self.exports: list[dict] = []

    @contextlib.contextmanager
    def session(self) -> Iterator[None]:
        yield None

    def new_session(self) -> FakeEngine:
        self.calls.append("new_session")
        return self

    def __enter__(self) -> FakeEngine:
        return self

    def __exit__(self, *exc: object) -> None:
        return None

    def ingest(self, table: sa.Table, uri: str, version: int,
               partitions: list[str] | None = None, materialize: bool = False) -> None:
        self.calls.append("ingest")
        self.ingests.append({"table": table.name, "thread": threading.get_ident()})

    def audit(self, table: sa.Table, partitions: list[str] | None, uri: str | None = None,
              version: int | None = None, foreign_keys: bool = False,
              key_scope: str | None = None, referenced: dict | None = None) -> AuditReport:
        self.calls.append("audit")
        values = partitions or [None]
        totals = {value: {"linhas": 3} for value in values}
        nonfinite = {value: self.nonfinite.get(value, ()) for value in values}
        return AuditReport(table=table.name, partitions=tuple(partitions or ()), results=(),
                           not_run=(), nonfinite_columns=nonfinite, totals=totals)

    def export_partition(self, table: sa.Table, uri: str, value: str | None, metadata: dict,
                         expected_rows: int | None = None,
                         columns_without_min_max: tuple = ()) -> int:
        self.calls.append("export")
        self.exports.append({"table": table.name, "value": value,
                             "expected_rows": expected_rows,
                             "columns_without_min_max": tuple(columns_without_min_max)})
        data = rows(table, value, range(1, 4))
        return delta.publish_partition(uri, table, value, data, metadata, self.storage)

    def cleanup(self) -> None:
        self.calls.append("cleanup")


class FailingEngine(FakeEngine):
    """O motor de mentira cuja exportação de ``cad_lancamentos_projetados`` falha com
    ``ExecutionConflict``."""

    def export_partition(self, table: sa.Table, *args: object, **kwargs: object) -> int:
        if table.name == PROJECTED.name:
            raise ExecutionConflict("conflito plantado")
        return super().export_partition(table, *args, **kwargs)


# ---------------------------------------------------------------- a abertura


def test_execution_opens_every_table_and_fixes_versions(
    db: Database, caplog: pytest.LogCaptureFixture
) -> None:
    """``versions`` com as tabelas existentes e ``None`` nas ausentes; o log com as versões e o
    resumo; sem ``execution_id``, o gerado ``exec-<AAAA-MM-DD>-<uuid8>``; a pasta da tabela sem
    barra final."""
    engine = FakeEngine(db.storage)
    with caplog.at_level(logging.INFO, logger="serialize_db.execution"):
        with Execution(db, engine, "2026-08-31", "exec-2026-09-05") as run:
            expected = {"cad_lancamentos": 4, "cad_lancamentos_projetados": None,
                        "rel_composta": None}
            assert run.versions == expected
            assert run.execution_id == "exec-2026-09-05"
            assert run.sandbox is engine
    messages = [record.getMessage() for record in caplog.records]
    assert "execução exec-2026-09-05 aberta: partição 2026-08-31" in messages[0]
    assert "versões lidas {'cad_lancamentos': 4}" in messages[-1]
    assert engine.calls[-1] == "cleanup"

    # O execution_id gerado leva a data em UTC, lida antes e depois da construção.
    before = dt.datetime.now(dt.timezone.utc).date().isoformat()
    generated = Execution(db, FakeEngine(db.storage), "2026-08-31").execution_id
    after = dt.datetime.now(dt.timezone.utc).date().isoformat()
    match = re.fullmatch(r"exec-(\d{4}-\d{2}-\d{2})-[0-9a-f]{8}", generated)
    assert match
    assert match.group(1) in (before, after)

    # A pasta da tabela.
    assert db.uri(ENTRIES) == db.storage.uri + "/prod/cad_lancamentos"
    assert not db.uri(ENTRIES).endswith("/")


INVALID_PARTITIONS = ["", "2026/08/31", "a=b", "a b", "d'agua", "a:b", "a%b", "ação", ".x", "_x",
                      "-x", "2026-08-31-mais"]


@pytest.mark.parametrize("value", INVALID_PARTITIONS)
def test_execution_refuses_an_invalid_partition(db: Database, value: str) -> None:
    """O valor vazio, com ``/``, ``=``, espaço, ``'``, ``:``, ``%`` ou acento, começado por ``.``,
    ``_`` ou ``-``, ou acima do ``String(n)`` é recusado antes do sandbox, e o ``execution_id`` fora
    da regra também; ``2026-08-31`` e ``2026-Q1`` passam; o ambiente de ``Database`` fora da regra
    também é recusado."""
    with pytest.raises(ContractError):
        Execution(db, FakeEngine(db.storage), value, "exec-1")

    # O execution_id fora da regra, a partição que passa e o ambiente fora da regra.
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
        numbers = []
        for block in ranges:
            numbers.extend(block)
        assert len(numbers) == 300
        assert len(set(numbers)) == 300
        assert min(numbers) == 41
        assert run.next_ids(PROJECTED, 2) == range(1, 3)
        with pytest.raises(ContractError, match="chave composta"):
            run.next_ids(Composta.__table__, 1)


def test_ingest_of_several_tables_uses_extra_sessions(db: Database, folder: Path) -> None:
    """``ingest`` de uma tabela usa a sessão principal; de várias, uma sessão a mais por tabela, em
    paralelo, e a principal lê todas; a falha de uma leva o resultado das outras numa nota."""
    uri = db.uri(PROJECTED)
    delta.create_table(uri, PROJECTED, db.storage)
    published = rows(PROJECTED, MONTHS[0], range(1, 6))
    delta.publish_partition(uri, PROJECTED, MONTHS[0], published, {}, db.storage)
    engine = engine_for(db, folder, "exec-1")
    with Execution(db, engine, "2026-08-31", "exec-1") as run:
        run.ingest(ENTRIES, PROJECTED, partitions=[MONTHS[0]])
        counts = run.sandbox.query("SELECT (SELECT count(*) FROM cad_lancamentos) AS a, "
                                   "(SELECT count(*) FROM cad_lancamentos_projetados) AS b")
        assert counts.to_pylist() == [{"a": 10, "b": 5}]

    # No motor de mentira: uma sessão a mais por tabela, fora da thread principal, e a falha de
    # rel_composta, que não existe, leva o resultado de cad_lancamentos na nota.
    fake = FakeEngine(db.storage)
    with Execution(db, fake, "2026-08-31") as run:
        with pytest.raises(SandboxError, match="não existe") as failure:
            run.ingest(ENTRIES, Composta.__table__)
        assert "cad_lancamentos: concluída" in failure.value.__notes__[0]
        assert "rel_composta: falhou" in failure.value.__notes__[0]
    assert fake.calls.count("new_session") == 2
    assert [ingest["table"] for ingest in fake.ingests] == ["cad_lancamentos"]
    assert fake.ingests[0]["thread"] != threading.get_ident()


# ---------------------------------------------------------------- a auditoria e a publicação


def test_publish_requires_the_audit(db: Database, caplog: pytest.LogCaptureFixture) -> None:
    """``publish`` sem a auditoria aprovada é ``AuditFailed``; com ``audit=False`` passa e o log
    registra."""
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        with pytest.raises(AuditFailed, match="exige a auditoria"):
            run.publish(PROJECTED, partitions=["2026-08-31"])
        with caplog.at_level(logging.WARNING, logger="serialize_db.execution"):
            versions = run.publish(PROJECTED, partitions=["2026-08-31"], audit=False)
        assert versions == {PROJECTED.name: 1}
        assert "sem auditoria" in caplog.text
        with pytest.raises(ContractError, match="exige partitions"):
            run.publish(PROJECTED, audit=False)


def test_failed_audit_leaves_the_delta_untouched(db: Database, folder: Path) -> None:
    """A auditoria reprovada levanta ``AuditFailed``, a versão da tabela não muda e o sandbox
    sai."""
    engine = engine_for(db, folder, "exec-1")
    with pytest.raises(AuditFailed, match="chave_id_lancamento"):
        with Execution(db, engine, "2026-08-31", "exec-1") as run:
            # Cada id duas vezes.
            repeated = pa.concat_tables([rows(PROJECTED, "2026-08-31", range(1, 4))] * 2)
            run.sandbox.load(PROJECTED, repeated)
            run.audit(PROJECTED, ["2026-08-31"])
            run.publish(PROJECTED, partitions=["2026-08-31"])
    assert not delta.table_exists(db.uri(PROJECTED), db.storage)
    assert not (folder / "sandbox_exec-1" / "exec-1.duckdb").exists()


def test_rerun_with_the_same_execution_id_produces_the_same_rows(
    db: Database, folder: Path
) -> None:
    """A reexecução com o mesmo ``execution_id`` publica as mesmas linhas, com ids que podem
    diferir, numa versão a mais."""
    attempts = []
    for _ in range(2):
        engine = engine_for(db, folder, "exec-rerun")
        with Execution(db, engine, "2026-08-31", "exec-rerun") as run:
            run.ingest(ENTRIES, partitions=["2026-08-31"])
            project(run, "2026-08-31")
            run.audit(PROJECTED, ["2026-08-31"])
            versions = run.publish(PROJECTED, partitions=["2026-08-31"])
        published = delta.open_table(db.uri(PROJECTED), db.storage).to_pyarrow_dataset().to_table()
        attempts.append({"version": versions[PROJECTED.name], "rows": published.num_rows,
                         "valor": sorted(published.column("valor").to_pylist())})
    first, second = attempts
    assert second["rows"] == first["rows"]
    assert second["valor"] == first["valor"]
    assert second["version"] == first["version"] + 1


def test_publish_aborts_when_data_changed_since_open(db: Database) -> None:
    """Um ``append`` de outra execução depois da abertura é ``ExecutionConflict`` sem commit; uma
    compactação não é, e a versão fixada avança."""
    uri = db.uri(ENTRIES)
    engine = FakeEngine(db.storage)
    with Execution(db, engine, "2026-08-31") as run:
        appended = rows(ENTRIES, "2026-08-31", range(100, 103))
        write_deltalake(delta.open_table(uri, db.storage), appended, mode="append")
        before = delta.open_table(uri, db.storage).version()
        run.audit(ENTRIES, ["2026-08-31"])
        with pytest.raises(ExecutionConflict, match="2026-08-31"):
            run.publish(ENTRIES, partitions=["2026-08-31"])
        assert delta.open_table(uri, db.storage).version() == before

    # A compactação depois da abertura.
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
    """O mesmo resultado com ``max_workers=1`` e ``2``; a falha de uma tabela deixa as outras
    terminarem e leva o resultado de cada uma numa nota."""
    results = {}
    for workers in (1, 2):
        database = Database(str(folder / f"delta_{workers}"), "prod", Base.metadata)
        with Execution(database, FakeEngine(database.storage), "2026-08-31") as run:
            results[workers] = run.publish(ENTRIES, PROJECTED, partitions=["2026-08-31"],
                                           audit=False, max_workers=workers)
    assert results[1] == results[2] == {ENTRIES.name: 1, PROJECTED.name: 1}

    # Com um worker, a segunda tabela falha: a primeira terminou e a terceira nem começa. As três
    # tabelas ficam num MetaData próprio, sem tocar o de Base.
    metadata = sa.MetaData()
    entries_copy = ENTRIES.to_metadata(metadata)
    projected_copy = PROJECTED.to_metadata(metadata)
    extra = PROJECTED.to_metadata(metadata, name="cad_extra")
    database = Database(str(folder / "delta_falha"), "prod", metadata)
    engine = FailingEngine(database.storage)
    with Execution(database, engine, "2026-08-31") as run:
        with pytest.raises(ExecutionConflict, match="conflito plantado") as failure:
            run.publish(entries_copy, projected_copy, extra, partitions=["2026-08-31"],
                        audit=False, max_workers=1)
    note = failure.value.__notes__[0]
    assert "cad_lancamentos: concluída" in note
    assert "cad_lancamentos_projetados: falhou" in note
    assert "cad_extra: cancelada" in note
    assert [export["table"] for export in engine.exports] == ["cad_lancamentos"]

    # A tabela sem partição com partitions é ContractError.
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        with pytest.raises(ContractError, match="tabela sem partição"):
            run.publish(ENTRIES, Composta.__table__, partitions=["2026-08-31"], audit=False,
                        max_workers=2)


def test_publish_passes_the_nonfinite_columns_to_the_export(db: Database, folder: Path) -> None:
    """O motor recebe, por partição, as colunas ``Double`` não finitas da auditoria e a contagem
    dela; a partição sem elas recebe a lista vazia; com ``audit=False``, todas as ``Double`` e
    nenhuma contagem; no motor DuckDB, a partição com ``NaN`` sai sem o mínimo e o máximo da
    coluna."""
    engine = FakeEngine(db.storage, nonfinite={"2026-07-31": ("valor",)})
    with Execution(db, engine, "2026-08-31") as run:
        run.audit(PROJECTED, ["2026-07-31", "2026-08-31"])
        run.publish(PROJECTED, partitions=["2026-07-31", "2026-08-31"])
        run.publish(PROJECTED, partitions=["2026-09-30"], audit=False)
    exports = engine.exports
    assert [export["value"] for export in exports] == ["2026-07-31", "2026-08-31", "2026-09-30"]
    assert [export["expected_rows"] for export in exports] == [3, 3, None]
    nonfinite = [export["columns_without_min_max"] for export in exports]
    assert nonfinite == [("valor",), (), ("valor",)]

    # No motor DuckDB.
    real = engine_for(db, folder, "exec-nan")
    with Execution(db, real, "2026-08-31", "exec-nan") as run:
        with_nan = rows(PROJECTED, "2026-06-30", range(100, 103), valor=[1.0, float("nan"), 2.0])
        run.sandbox.load(PROJECTED, with_nan)
        report = run.audit(PROJECTED, ["2026-06-30"])
        assert report.nonfinite_columns == {"2026-06-30": ("valor",)}
        run.publish(PROJECTED, partitions=["2026-06-30"])
    published = delta.open_table(db.uri(PROJECTED), db.storage)
    actions = pa.table(published.get_add_actions(flatten=True))
    june = actions.filter(pc.equal(actions.column("partition.data_base_str"), "2026-06-30"))
    assert june.column("max.valor").null_count == 1
    assert june.column("max.id_lancamento").to_pylist() == [102]


def test_commit_metadata_in_history(db: Database) -> None:
    """``serialize_db_execution_id`` e ``serialize_db_input_versions`` no ``history``;
    ``serialize_db_snapshot`` só na execução marcada."""
    with Execution(db, FakeEngine(db.storage), "2026-08-31", "exec-comum") as run:
        run.publish(PROJECTED, partitions=["2026-08-31"], audit=False)
    history = delta.open_table(db.uri(PROJECTED), db.storage).history(1)[0]
    assert history["serialize_db_execution_id"] == "exec-comum"
    assert "serialize_db_snapshot" not in history
    assert json.loads(history["serialize_db_input_versions"]) == {"cad_lancamentos": 4}

    # A execução marcada.
    with Execution(db, FakeEngine(db.storage), "2026-08-31", "exec-marcada") as run:
        run.snapshot("2026T3")
        run.publish(PROJECTED, partitions=["2026-08-31"], audit=False)
    marked = delta.open_table(db.uri(PROJECTED), db.storage).history(1)[0]
    assert marked["serialize_db_snapshot"] == "2026T3"


def test_snapshot_writes_the_control_file_at_exit(db: Database) -> None:
    """O snapshot marcado grava, no encerramento, a versão de toda tabela do ambiente, uma vez; a
    execução que falha não o grava."""
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        run.snapshot("2026T3")
        run.publish(PROJECTED, partitions=["2026-08-31"], audit=False)
        _, fingerprint = delta.read_snapshots(db.storage, "prod")
        assert fingerprint is None  # só no encerramento
    control, _ = delta.read_snapshots(db.storage, "prod")
    versions = {"cad_lancamentos": 4, "cad_lancamentos_projetados": 1}
    assert control == {"snapshots": {"2026T3": versions}}

    # A execução que falha.
    with pytest.raises(RuntimeError):
        with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
            run.snapshot("2026T4")
            raise RuntimeError("falha do pipeline")
    control, _ = delta.read_snapshots(db.storage, "prod")
    assert list(control["snapshots"]) == ["2026T3"]


# ---------------------------------------------------------------- a linha de comando


def projected_pipeline(run: Execution) -> None:
    """O pipeline que ``serialize-db run`` chama: projeta, audita e publica a partição da
    execução."""
    run.ingest(ENTRIES, partitions=[run.partition])
    project(run, run.partition)
    run.audit(PROJECTED, [run.partition])
    run.publish(PROJECTED, partitions=[run.partition])


def repeated_key_pipeline(run: Execution) -> None:
    """Um pipeline que grava uma chave repetida e reprova na auditoria."""
    repeated = pa.concat_tables([rows(PROJECTED, run.partition, range(1, 3))] * 2)
    run.sandbox.load(PROJECTED, repeated)
    run.audit(PROJECTED, [run.partition])


def conflicting_pipeline(run: Execution) -> None:
    """Um pipeline que publica depois de outra execução gravar a mesma tabela."""
    uri = run.db.uri(ENTRIES)
    written = rows(ENTRIES, run.partition, range(900, 902))
    delta.publish_partition(uri, ENTRIES, run.partition, written, {}, run.db.storage)
    run.publish(ENTRIES, partitions=[run.partition], audit=False)


def redshift_pipeline(run: Execution) -> None:
    """Um pipeline que só confere o motor e a configuração do Redshift que a execução recebeu."""
    from serialize_db.engine.redshift import RedshiftConfig, RedshiftEngine

    assert isinstance(run.redshift, RedshiftConfig)
    assert run.redshift.schema == "esquema"
    assert isinstance(run.sandbox, RedshiftEngine) or run.redshift is not None


class IdleConnection:
    """Uma conexão do ``redshift_connector`` de mentira que aceita todo comando sem resposta."""

    autocommit = False

    def cursor(self) -> IdleConnection:
        return self

    def execute(self, text: str, params: object = None) -> None:
        return None

    def close(self) -> None:
        return None


def exit_code(arguments: list[str]) -> int | str | None:
    """O código do ``SystemExit`` com que ``serialize-db`` recusa os argumentos."""
    with pytest.raises(SystemExit) as exit_info:
        cli.main(arguments)
    return exit_info.value.code


def test_cli_run_parses_and_exits_by_result(db: Database, folder: Path,
                                            monkeypatch: pytest.MonkeyPatch,
                                            capsys: pytest.CaptureFixture) -> None:
    """``serialize-db run`` sai com 0 no pipeline que publica, 1 na auditoria reprovada e 2 no
    conflito, na partição e no ``execution_id`` fora da regra, sem ``--metadata`` e com o
    ``--export-mode`` que saiu da linha de comando, sem traceback."""
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    monkeypatch.delenv("SERIALIZE_DB_ROOT", raising=False)
    common = ["run", "--root", db.root, "--environment", "prod",
              "--metadata", "test_execution:Base.metadata"]
    projected = "test_execution:projected_pipeline"

    # O resultado do pipeline no código de saída.
    assert cli.main([*common, "--partition", "2026-08-31", projected]) == 0
    repeated_key = "test_execution:repeated_key_pipeline"
    assert cli.main([*common, "--partition", "2026-07-31", repeated_key]) == 1
    conflicting = "test_execution:conflicting_pipeline"
    assert cli.main([*common, "--partition", "2026-06-30", conflicting]) == 2

    # Os argumentos recusados pelo argparse.
    assert exit_code([*common, "--partition", "2026/08/31", projected]) == 2
    invalid_id = [*common, "--partition", "2026-08-31", "--execution-id", "exec 1", projected]
    assert exit_code(invalid_id) == 2
    assert exit_code(["run", "--root", db.root, "--partition", "2026-08-31", projected]) == 2
    missing = "test_execution:missing_pipeline"
    assert exit_code([*common, "--partition", "2026-08-31", missing]) == 2
    removed = [*common, "--partition", "2026-08-31", "--export-mode", "register", projected]
    assert exit_code(removed) == 2
    assert "Traceback" not in capsys.readouterr().err


def test_cli_run_hands_the_redshift_config_to_the_execution(
    db: Database, folder: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--engine redshift`` constrói o motor Redshift com as variáveis ``SERIALIZE_DB_REDSHIFT_*``
    e dá a configuração à execução; ``--redshift`` a dá a uma execução no motor DuckDB; sem os
    dois, ``publish_redshift`` é ``PublicationError``."""
    from serialize_db.engine import redshift

    monkeypatch.setattr(redshift, "driver_connect", lambda login: IdleConnection())
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    for name, value in (("HOST", "host"), ("USER", "usuario"), ("PASSWORD", "senha"),
                        ("SCHEMA", "esquema"), ("SHARE_DATABASE", "compartilhado")):
        monkeypatch.setenv(f"SERIALIZE_DB_REDSHIFT_{name}", value)
    common = ["run", "--root", db.root, "--environment", "prod", "--partition", "2026-08-31",
              "--metadata", "test_execution:Base.metadata", "test_execution:redshift_pipeline"]
    assert cli.main([*common, "--engine", "redshift"]) == 0
    assert cli.main([*common, "--redshift"]) == 0

    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        assert run.redshift is None
        with pytest.raises(PublicationError, match="redshift=RedshiftConfig"):
            run.publish_redshift(PROJECTED)


def test_cli_audit_prints_the_sql_and_audits_the_published_version(
    db: Database, folder: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """``serialize-db audit --sql`` imprime o texto das verificações no dialeto, sem armazenamento;
    sem ``--sql``, audita a versão publicada e sai com 0 na aprovação."""
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    metadata = ["audit", "--metadata", "test_execution:Base.metadata"]
    base = [*metadata, "--table", "cad_lancamentos"]
    assert cli.main([*base, "--engine", "redshift", "--partitions", "2026-08-31", "--sql"]) == 0
    printed = capsys.readouterr().out
    assert "-- linhas" in printed
    assert "to_char(" in printed
    assert '"{prefix}cad_lancamentos"' in printed
    published = [*base, "--partitions", "2026-08-31", "--root", db.root, "--environment", "prod"]
    assert cli.main(published) == 0
    printed = capsys.readouterr().out
    assert "cad_lancamentos na versão 4:" in printed
    assert "chave_id_lancamento_publicada: aprovada" in printed
    assert cli.main([*metadata, "--table", "nao_existe", "--sql"]) == 2
