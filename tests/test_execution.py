"""``serialize_db.execution`` e ``serialize-db run|audit``: o ciclo de uma execução sobre um Delta
local.

Os testes gravam sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``): a raiz Delta e o sandbox
ficam numa pasta nova por teste, e o motor DuckDB nasce nela, porque o padrão dele é uma pasta de
``tempfile.mkdtemp``. Os testes da linha de comando apontam a pasta temporária do processo para a
mesma pasta. O modelo é ``Lancamento``, particionado por ``data_base_str``, e ``Projetado``, a
tabela que o pipeline grava, com uma coluna ``Double``; ``Composta`` tem chave primária de duas
colunas. A auditoria da linha de comando que a ingestão reprova usa o modelo de
``tests/lancamentos_model.py``, com uma coluna JSON; o caso com ``--engine redshift`` é também
``redshift`` e ``s3``, no substituto local ou no ambiente alvo.

Eles conferem a abertura com as versões fixadas, a recusa da partição e do ``execution_id`` fora da
regra, as partições anteriores, a execução sem partição, as faixas de ``next_ids``, as duas leituras
na versão que ``publish_delta`` avançou, a ingestão de uma tabela na sessão principal e de várias em
sessões a mais, a auditoria exigida e a reprovada, as mensagens da tabela inteira, a reexecução, os
conflitos, o pool da publicação, as colunas não finitas e a contagem passadas à exportação, os
metadados de commit, o snapshot, o nome de snapshot já usado recusado antes de qualquer commit, o
log da saída que falha ao gravá-lo e a linha de comando, com a ingestão recusada impressa como
auditoria reprovada e o erro de acesso do ``ingest`` subindo com o traceback. O motor de mentira
registra as chamadas que a execução faz. A extensão ``delta`` do DuckDB precisa estar na pasta de
extensões.
"""

from __future__ import annotations

import argparse
import contextlib
import datetime
import json
import logging
import re
import tempfile
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import sqlalchemy as sa
from deltalake import write_deltalake
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

import lancamentos_model
from conftest import LocalLocation, S3Location, record, redshift_config
from serialize_db import cli, delta, schema
from serialize_db.audit import AuditReport
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.errors import AuditFailed, ContractError, ExecutionConflict, SandboxError
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
    data_base: Mapped[datetime.date] = mapped_column(sa.Date)
    valor: Mapped[float] = mapped_column(sa.Double)
    data_base_str: Mapped[str] = mapped_column(sa.String(10))


class Projetado(Base):
    __tablename__ = "cad_lancamentos_projetados"
    __table_args__ = {"info": INFO}
    id_lancamento: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    data_base: Mapped[datetime.date] = mapped_column(sa.Date)
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
    if valor is None:
        valor = [entry_id / 2 for entry_id in ids]
    data = pa.table({
        "id_lancamento": pa.array(list(ids), pa.int64()),
        "data_base": pa.array([datetime.date.fromisoformat(value)] * len(ids), pa.date32()),
        "valor": pa.array(valor, pa.float64()),
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
    database = Database(str(folder / "delta"), "prd", Base.metadata)
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
    """O pipeline do teste: a partição da entrada, com ids novos, na tabela projetada do sandbox,
    criada por ``create_table``."""
    statement = sa.select(ENTRIES).where(ENTRIES.c.data_base_str == month)
    run.sandbox.create_table(PROJECTED)
    with run.sandbox.stream(statement) as stream, run.sandbox.appender(PROJECTED) as appender:
        for batch in stream:
            ids = pa.array(list(run.next_ids(PROJECTED, batch.num_rows)), pa.int64())
            appender.write(batch.set_column(0, "id_lancamento", ids))


def create_and_append(run: Execution, table: sa.Table, data: pa.Table) -> int:
    """A tabela criada vazia no sandbox por ``create_table`` e as linhas acrescentadas por
    ``append``; devolve as linhas acrescentadas."""
    run.sandbox.create_table(table)
    return run.sandbox.append(table, data)


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
    before = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    generated = Execution(db, FakeEngine(db.storage), "2026-08-31").execution_id
    after = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    match = re.fullmatch(r"exec-(\d{4}-\d{2}-\d{2})-[0-9a-f]{8}", generated)
    assert match
    assert match.group(1) in (before, after)

    # A pasta da tabela.
    assert db.uri(ENTRIES) == db.storage.uri + "/prd/cad_lancamentos"
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
    """Só valores até a partição da execução, os ``n`` últimos, na ordem de texto; a tabela
    ausente e a tabela sem arquivos dão a lista vazia; a tabela sem partição e a execução sem
    partição são ``ContractError``."""
    with Execution(db, FakeEngine(db.storage), "2026-07-31") as run:
        assert run.previous_partitions(ENTRIES, 2) == ["2026-06-30", "2026-07-31"]
        assert run.previous_partitions(ENTRIES, 12) == ["2026-05-31", "2026-06-30", "2026-07-31"]
        assert run.previous_partitions(PROJECTED, 3) == []
        with pytest.raises(ContractError, match="sem partição"):
            run.previous_partitions(Composta.__table__, 1)

    # A tabela criada, sem arquivos.
    delta.create_table(db.uri(PROJECTED), PROJECTED, db.storage)
    with Execution(db, FakeEngine(db.storage), "2026-07-31") as run:
        assert run.previous_partitions(PROJECTED, 3) == []

    # A execução sem partição.
    with Execution(db, FakeEngine(db.storage), execution_id="exec-1") as run:
        with pytest.raises(ContractError, match="a execução exec-1 não tem partição"):
            run.previous_partitions(ENTRIES, 1)


def test_execution_without_partition_publishes_a_table_without_partition(
    db: Database, folder: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Sem partição, a execução abre sem a conferência da partição e o log diz ``sem partição`` e
    a auditoria da tabela inteira; a tabela sem partição nasce na primeira publicação, e a
    execução seguinte a traz inteira ao sandbox, acrescenta uma linha e a publica, substituindo a
    versão anterior inteira."""
    composite = Composta.__table__
    first_rows = pa.table({"id_a": pa.array([1, 2], pa.int64()),
                           "id_b": pa.array([1, 1], pa.int64())})
    with caplog.at_level(logging.INFO, logger="serialize_db.execution"):
        with Execution(db, engine_for(db, folder, "dom-1"), execution_id="dom-1") as run:
            assert run.partition is None
            create_and_append(run, composite, first_rows)
            run.audit(composite, None)
            assert run.publish_delta(composite) == {composite.name: 1}
    assert "execução dom-1 aberta: sem partição" in caplog.text
    assert "auditoria de rel_composta na tabela inteira: aprovada" in caplog.text
    assert "execução dom-1 concluída: sem partição" in caplog.text

    # A execução seguinte: a tabela inteira no sandbox, uma linha a mais por append.
    with Execution(db, engine_for(db, folder, "dom-2"), execution_id="dom-2") as run:
        run.ingest(composite, materialize=True)
        third = pa.table({"id_a": pa.array([3], pa.int64()), "id_b": pa.array([1], pa.int64())})
        assert run.sandbox.append(composite, third) == 1
        run.audit(composite, None)
        assert run.publish_delta(composite) == {composite.name: 2}
    published = delta.open_table(db.uri(composite), db.storage).to_pyarrow_dataset().to_table()
    assert sorted(published.column("id_a").to_pylist()) == [1, 2, 3]


def take_ids(run: Execution, ranges: list[range]) -> None:
    """Pede 50 faixas de 3 ids de ``cad_lancamentos``, como uma thread do pipeline."""
    for _ in range(50):
        ranges.append(run.next_ids(ENTRIES, 3))


def test_next_ids_are_disjoint_across_threads(db: Database) -> None:
    """Duas threads têm faixas disjuntas, a primeira acima do máximo das estatísticas; a tabela nova
    começa em 1; a chave composta é ``ContractError``."""
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        ranges: list[range] = []
        threads = [threading.Thread(target=take_ids, args=(run, ranges)) for _ in range(2)]
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


def test_previous_partitions_and_next_ids_read_the_version_publish_delta_advanced(
    db: Database, folder: Path
) -> None:
    """Depois de ``publish_delta``, ``previous_partitions`` e ``next_ids`` leem a versão fixada que
    ele avançou: a tabela ausente na abertura mostra a partição publicada e dá ids acima dos
    gravados, e a tabela aberta na versão 4 mostra a partição nova e o maior id dela."""
    engine = engine_for(db, folder, "exec-1")
    with Execution(db, engine, "2026-09-30", "exec-1") as run:
        # A tabela ausente na abertura, com os ids 1 a 3 gravados sem next_ids.
        create_and_append(run, PROJECTED, rows(PROJECTED, "2026-08-31", range(1, 4)))
        run.audit(PROJECTED, ["2026-08-31"])
        assert run.publish_delta(PROJECTED, partitions=["2026-08-31"]) == {PROJECTED.name: 1}
        assert run.previous_partitions(PROJECTED, 12) == ["2026-08-31"]
        assert run.next_ids(PROJECTED, 2) == range(4, 6)

        # A tabela aberta na versão 4, com a partição 2026-09-30 publicada, ids 41 a 43.
        create_and_append(run, ENTRIES, rows(ENTRIES, "2026-09-30", range(41, 44)))
        run.audit(ENTRIES, ["2026-09-30"])
        assert run.publish_delta(ENTRIES, partitions=["2026-09-30"]) == {ENTRIES.name: 5}
        assert run.previous_partitions(ENTRIES, 12) == MONTHS
        assert run.next_ids(ENTRIES, 1) == range(44, 45)


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
        counts = run.sandbox.query(
            "SELECT (SELECT count(*) FROM cad_lancamentos) AS lancamentos, "
            "(SELECT count(*) FROM cad_lancamentos_projetados) AS projetados")
        assert counts.to_pylist() == [{"lancamentos": 10, "projetados": 5}]

    # No motor de mentira, uma tabela: a sessão principal, na thread de quem chama.
    single = FakeEngine(db.storage)
    with Execution(db, single, "2026-08-31") as run:
        run.ingest(ENTRIES)
    assert "new_session" not in single.calls
    assert single.ingests == [{"table": "cad_lancamentos", "thread": threading.get_ident()}]

    # No motor de mentira, várias: uma sessão a mais por tabela, fora da thread principal, e a
    # falha de rel_composta, que não existe, leva o resultado de cad_lancamentos na nota.
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


def test_publish_delta_requires_the_audit(db: Database, caplog: pytest.LogCaptureFixture) -> None:
    """``publish_delta`` sem a auditoria aprovada é ``AuditFailed``; com ``audit=False`` passa e
    o log registra."""
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        with pytest.raises(AuditFailed, match="exige a auditoria"):
            run.publish_delta(PROJECTED, partitions=["2026-08-31"])
        with caplog.at_level(logging.WARNING, logger="serialize_db.execution"):
            versions = run.publish_delta(PROJECTED, partitions=["2026-08-31"], audit=False)
        assert versions == {PROJECTED.name: 1}
        assert "sem auditoria" in caplog.text
        with pytest.raises(ContractError, match="exige partitions"):
            run.publish_delta(PROJECTED, audit=False)


def test_messages_of_the_whole_table_name_it(db: Database, folder: Path,
                                             caplog: pytest.LogCaptureFixture) -> None:
    """Sem partições, o log e as mensagens de ``audit`` e ``publish_delta`` dizem ``na tabela
    inteira``, e não ``None``: a auditoria reprovada, a publicação sem a auditoria aprovada e a
    publicação com ``audit=False``."""
    composite = Composta.__table__
    repeated = pa.table({"id_a": pa.array([1, 1], pa.int64()),
                         "id_b": pa.array([1, 1], pa.int64())})
    with caplog.at_level(logging.INFO, logger="serialize_db.execution"):
        with Execution(db, engine_for(db, folder, "dom-1"), execution_id="dom-1") as run:
            create_and_append(run, composite, repeated)
            with pytest.raises(AuditFailed, match="rel_composta na tabela inteira: reprovada em"):
                run.audit(composite, None)
            with pytest.raises(AuditFailed, match="publish_delta na tabela inteira exige"):
                run.publish_delta(composite)
            assert run.publish_delta(composite, audit=False) == {composite.name: 1}
    assert "auditoria de rel_composta na tabela inteira: reprovada" in caplog.text
    assert "publish_delta de rel_composta na tabela inteira sem auditoria" in caplog.text
    assert "em None" not in caplog.text


def test_failed_audit_leaves_the_delta_untouched(db: Database, folder: Path) -> None:
    """A auditoria reprovada levanta ``AuditFailed``, a versão da tabela não muda e o sandbox
    sai."""
    engine = engine_for(db, folder, "exec-1")
    with pytest.raises(AuditFailed, match="chave_id_lancamento"):
        with Execution(db, engine, "2026-08-31", "exec-1") as run:
            # Cada id duas vezes.
            once = rows(PROJECTED, "2026-08-31", range(1, 4))
            repeated = pa.concat_tables([once, once])
            create_and_append(run, PROJECTED, repeated)
            run.audit(PROJECTED, ["2026-08-31"])
            run.publish_delta(PROJECTED, partitions=["2026-08-31"])
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
            versions = run.publish_delta(PROJECTED, partitions=["2026-08-31"])
        published = delta.open_table(db.uri(PROJECTED), db.storage).to_pyarrow_dataset().to_table()
        attempts.append({"version": versions[PROJECTED.name], "rows": published.num_rows,
                         "valor": sorted(published.column("valor").to_pylist())})
    first, second = attempts
    assert second["rows"] == first["rows"]
    assert second["valor"] == first["valor"]
    assert second["version"] == first["version"] + 1


def test_publish_delta_aborts_when_data_changed_since_open(db: Database) -> None:
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
            run.publish_delta(ENTRIES, partitions=["2026-08-31"])
        assert delta.open_table(uri, db.storage).version() == before

    # A compactação depois da abertura.
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        delta.compact(uri, ENTRIES, ["2026-08-31"], db.storage)
        run.audit(ENTRIES, ["2026-08-31"])
        compacted = delta.open_table(uri, db.storage).version()
        versions = run.publish_delta(ENTRIES, partitions=["2026-08-31"])
        assert versions == {ENTRIES.name: compacted + 1}


def test_two_executions_on_the_same_partition_conflict(db: Database) -> None:
    """Duas execuções abertas na mesma versão publicam a mesma partição: a segunda aborta."""
    first = Execution(db, FakeEngine(db.storage), "2026-08-31", "exec-a")
    second = Execution(db, FakeEngine(db.storage), "2026-08-31", "exec-b")
    with first, second:
        for run in (first, second):
            run.audit(ENTRIES, ["2026-08-31"])
        first.publish_delta(ENTRIES, partitions=["2026-08-31"])
        with pytest.raises(ExecutionConflict):
            second.publish_delta(ENTRIES, partitions=["2026-08-31"])


def test_publish_delta_with_two_workers_matches_one(db: Database, folder: Path) -> None:
    """O mesmo resultado com ``max_workers=1`` e ``2``; a falha de uma tabela deixa as outras
    terminarem e leva o resultado de cada uma numa nota."""
    results = {}
    for workers in (1, 2):
        database = Database(str(folder / f"delta_{workers}"), "prd", Base.metadata)
        with Execution(database, FakeEngine(database.storage), "2026-08-31") as run:
            results[workers] = run.publish_delta(ENTRIES, PROJECTED, partitions=["2026-08-31"],
                                           audit=False, max_workers=workers)
    assert results[1] == results[2] == {ENTRIES.name: 1, PROJECTED.name: 1}

    # Com um worker, a segunda tabela falha: a primeira terminou e a terceira nem começa. As três
    # tabelas ficam num MetaData próprio, sem tocar o de Base.
    metadata = sa.MetaData()
    entries_copy = ENTRIES.to_metadata(metadata)
    projected_copy = PROJECTED.to_metadata(metadata)
    extra = PROJECTED.to_metadata(metadata, name="cad_extra")
    database = Database(str(folder / "delta_falha"), "prd", metadata)
    engine = FailingEngine(database.storage)
    with Execution(database, engine, "2026-08-31") as run:
        with pytest.raises(ExecutionConflict, match="conflito plantado") as failure:
            run.publish_delta(entries_copy, projected_copy, extra, partitions=["2026-08-31"],
                        audit=False, max_workers=1)
    note = failure.value.__notes__[0]
    assert "cad_lancamentos: concluída" in note
    assert "cad_lancamentos_projetados: falhou" in note
    assert "cad_extra: cancelada" in note
    assert [export["table"] for export in engine.exports] == ["cad_lancamentos"]

    # A tabela sem partição com partitions é ContractError.
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        with pytest.raises(ContractError, match="tabela sem partição"):
            run.publish_delta(ENTRIES, Composta.__table__, partitions=["2026-08-31"], audit=False,
                        max_workers=2)


def test_publish_delta_passes_the_nonfinite_columns_to_the_export(
    db: Database, folder: Path
) -> None:
    """O motor recebe, por partição, as colunas ``Double`` não finitas da auditoria e a contagem
    dela; a partição sem elas recebe a lista vazia; com ``audit=False``, todas as ``Double`` e
    nenhuma contagem; no motor DuckDB, a partição com ``NaN`` sai sem o mínimo e o máximo da
    coluna."""
    engine = FakeEngine(db.storage, nonfinite={"2026-07-31": ("valor",)})
    with Execution(db, engine, "2026-08-31") as run:
        run.audit(PROJECTED, ["2026-07-31", "2026-08-31"])
        run.publish_delta(PROJECTED, partitions=["2026-07-31", "2026-08-31"])
        run.publish_delta(PROJECTED, partitions=["2026-09-30"], audit=False)
    exports = engine.exports
    assert [export["value"] for export in exports] == ["2026-07-31", "2026-08-31", "2026-09-30"]
    assert [export["expected_rows"] for export in exports] == [3, 3, None]
    nonfinite = [export["columns_without_min_max"] for export in exports]
    assert nonfinite == [("valor",), (), ("valor",)]

    # No motor DuckDB.
    duckdb_engine = engine_for(db, folder, "exec-nan")
    with Execution(db, duckdb_engine, "2026-08-31", "exec-nan") as run:
        with_nan = rows(PROJECTED, "2026-06-30", range(100, 103), valor=[1.0, float("nan"), 2.0])
        create_and_append(run, PROJECTED, with_nan)
        report = run.audit(PROJECTED, ["2026-06-30"])
        assert report.nonfinite_columns == {"2026-06-30": ("valor",)}
        run.publish_delta(PROJECTED, partitions=["2026-06-30"])
    published = delta.open_table(db.uri(PROJECTED), db.storage)
    actions = pa.table(published.get_add_actions(flatten=True))
    june = actions.filter(pc.equal(actions.column("partition.data_base_str"), "2026-06-30"))
    assert june.column("max.valor").null_count == 1
    assert june.column("max.id_lancamento").to_pylist() == [102]


def test_commit_metadata_in_history(db: Database) -> None:
    """``serialize_db_execution_id`` e ``serialize_db_input_versions`` no ``history``;
    ``serialize_db_snapshot`` só na execução marcada."""
    with Execution(db, FakeEngine(db.storage), "2026-08-31", "exec-comum") as run:
        run.publish_delta(PROJECTED, partitions=["2026-08-31"], audit=False)
    history = delta.open_table(db.uri(PROJECTED), db.storage).history(limit=1)[0]
    assert history["serialize_db_execution_id"] == "exec-comum"
    assert "serialize_db_snapshot" not in history
    assert json.loads(history["serialize_db_input_versions"]) == {"cad_lancamentos": 4}

    # A execução marcada.
    with Execution(db, FakeEngine(db.storage), "2026-08-31", "exec-marcada") as run:
        run.snapshot("2026T3")
        run.publish_delta(PROJECTED, partitions=["2026-08-31"], audit=False)
    marked = delta.open_table(db.uri(PROJECTED), db.storage).history(limit=1)[0]
    assert marked["serialize_db_snapshot"] == "2026T3"


def test_snapshot_writes_the_control_file_at_exit(db: Database,
                                                  caplog: pytest.LogCaptureFixture) -> None:
    """O snapshot marcado grava, no encerramento, a versão de toda tabela do ambiente, uma vez; a
    execução que falha não o grava; o nome que outro escritor grava depois da chamada é
    ``ContractError`` na saída, depois do commit da execução, e o resumo no log diz
    ``com erro``."""
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        run.snapshot("2026T3")
        run.publish_delta(PROJECTED, partitions=["2026-08-31"], audit=False)
        _, fingerprint = delta.read_snapshots(db.storage, "prd")
        assert fingerprint is None  # só no encerramento
    control, _ = delta.read_snapshots(db.storage, "prd")
    versions = {"cad_lancamentos": 4, "cad_lancamentos_projetados": 1}
    assert control == {"snapshots": {"2026T3": versions}}

    # A execução que falha.
    with pytest.raises(RuntimeError):
        with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
            run.snapshot("2026T4")
            raise RuntimeError("falha do pipeline")
    control, _ = delta.read_snapshots(db.storage, "prd")
    assert list(control["snapshots"]) == ["2026T3"]

    # Outro escritor grava o nome entre a chamada e a saída: a gravação do snapshot falha na
    # saída, depois do commit da execução, e o resumo diz com erro.
    with caplog.at_level(logging.INFO, logger="serialize_db.execution"):
        with pytest.raises(ContractError, match="o snapshot 2026T5 já existe"):
            with Execution(db, FakeEngine(db.storage), "2026-08-31", "exec-disputado") as run:
                run.snapshot("2026T5")
                run.publish_delta(PROJECTED, partitions=["2026-08-31"], audit=False)
                delta.snapshot(db.storage, "prd", "2026T5", {"cad_lancamentos": 4})
    assert delta.open_table(db.uri(PROJECTED), db.storage).version() == 2
    assert "execução exec-disputado com erro" in caplog.text
    assert "execução exec-disputado concluída" not in caplog.text


def test_snapshot_refuses_a_used_name_before_any_commit(db: Database) -> None:
    """O nome já presente em ``snapshots``, ou em ``archived`` depois do arquivamento, é
    ``ContractError`` na chamada de ``snapshot``, antes de qualquer commit, com o
    ``serialize-db channel`` na mensagem; o arquivo de controle não muda."""
    delta.snapshot(db.storage, "prd", "2026T2", {"cad_lancamentos": 4})
    delta.snapshot(db.storage, "prd", "2026T3", {"cad_lancamentos": 4})
    control = delta.archive_snapshot(db.storage, "prd", "2026T2")
    for name in ("2026T3", "2026T2"):
        refused = f"prd: o snapshot {name} já existe.*serialize-db channel"
        with pytest.raises(ContractError, match=refused):
            with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
                run.snapshot(name)
                run.publish_delta(PROJECTED, partitions=["2026-08-31"], audit=False)
        assert not delta.table_exists(db.uri(PROJECTED), db.storage)
    assert delta.read_snapshots(db.storage, "prd")[0] == control


# ---------------------------------------------------------------- a linha de comando


def projected_pipeline(run: Execution) -> None:
    """O pipeline que ``serialize-db run`` chama: projeta, audita e publica a partição da
    execução."""
    run.ingest(ENTRIES, partitions=[run.partition])
    project(run, run.partition)
    run.audit(PROJECTED, [run.partition])
    run.publish_delta(PROJECTED, partitions=[run.partition])


def repeated_key_pipeline(run: Execution) -> None:
    """Um pipeline que grava uma chave repetida e reprova na auditoria."""
    once = rows(PROJECTED, run.partition, range(1, 3))
    repeated = pa.concat_tables([once, once])
    create_and_append(run, PROJECTED, repeated)
    run.audit(PROJECTED, [run.partition])


def conflicting_pipeline(run: Execution) -> None:
    """Um pipeline que publica depois de outra execução gravar a mesma tabela."""
    uri = run.db.uri(ENTRIES)
    written = rows(ENTRIES, run.partition, range(900, 902))
    delta.publish_partition(uri, ENTRIES, run.partition, written, {}, run.db.storage)
    run.publish_delta(ENTRIES, partitions=[run.partition], audit=False)


def composite_pipeline(run: Execution) -> None:
    """O pipeline sem partição: grava, audita e publica ``rel_composta`` inteira."""
    data = pa.table({"id_a": pa.array([1, 2], pa.int64()), "id_b": pa.array([1, 1], pa.int64())})
    create_and_append(run, Composta.__table__, data)
    run.audit(Composta.__table__, None)
    run.publish_delta(Composta.__table__)


def previous_partitions_pipeline(run: Execution) -> None:
    """Um pipeline que pede as partições anteriores."""
    run.previous_partitions(ENTRIES, 1)


def snapshot_pipeline(run: Execution) -> None:
    """Um pipeline que marca o snapshot ``2026T3``, gravado na saída da execução."""
    run.snapshot("2026T3")


def marked_pipeline(run: Execution) -> None:
    """Um pipeline que marca o snapshot ``2026T3`` e publica a partição da execução."""
    run.snapshot("2026T3")
    projected_pipeline(run)


def raced_snapshot_pipeline(run: Execution) -> None:
    """Um pipeline que marca o snapshot ``2026T4``, que outro escritor grava antes da saída da
    execução."""
    run.snapshot("2026T4")
    delta.snapshot(run.db.storage, run.db.environment, "2026T4", {"cad_lancamentos": 4})


def redshift_engine_pipeline(run: Execution) -> None:
    """O pipeline de ``--engine redshift``: confere o motor Redshift e a configuração do ambiente
    que a execução recebeu."""
    from serialize_db.engine.redshift import RedshiftConfig, RedshiftEngine

    assert isinstance(run.sandbox, RedshiftEngine)
    assert isinstance(run.redshift, RedshiftConfig)
    assert run.redshift.schema == "esquema"


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
    conflito na tabela e no arquivo de controle, na partição e no ``execution_id`` fora da regra,
    sem ``--metadata`` e com o ``--export-mode`` que saiu da linha de comando, sem traceback."""
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    monkeypatch.delenv("SERIALIZE_DB_ROOT", raising=False)
    common = ["run", "--root", db.root, "--environment", "prd",
              "--metadata", "test_execution:Base.metadata"]
    projected = "test_execution:projected_pipeline"

    # O resultado do pipeline no código de saída.
    assert cli.main([*common, "--partition", "2026-08-31", projected]) == 0
    repeated_key = "test_execution:repeated_key_pipeline"
    assert cli.main([*common, "--partition", "2026-07-31", repeated_key]) == 1
    conflicting = "test_execution:conflicting_pipeline"
    assert cli.main([*common, "--partition", "2026-06-30", conflicting]) == 2

    # O conflito no arquivo de controle: outro escritor grava um snapshot novo depois de cada
    # leitura, a de run.snapshot e a da escrita condicional na saída da execução, e a impressão
    # digital lida fica velha.
    delta.snapshot(db.storage, "prd", "2026T2", {"cad_lancamentos": 4})
    original_read = delta.read_snapshots
    other_snapshots = []

    def read_before_another_writer(storage: Storage, environment: str) -> tuple[dict, str | None]:
        read = original_read(storage, environment)
        other_snapshots.append(f"outro{len(other_snapshots) + 1}")
        text = json.dumps({"snapshots": {other_snapshots[-1]: {}}}) + "\n"
        storage.write_text(storage.join(environment, delta.CONTROL_FILE), text)
        return read

    with monkeypatch.context() as patch:
        patch.setattr(delta, "read_snapshots", read_before_another_writer)
        snapshot = "test_execution:snapshot_pipeline"
        assert cli.main([*common, "--partition", "2026-08-31", snapshot]) == 2
    printed_errors = capsys.readouterr().err
    control_file = "prd/_serialize_db/snapshots.json"
    assert f"serialize-db run: conflito: {control_file} mudou desde a leitura" in printed_errors

    # Os argumentos recusados pelo argparse.
    assert exit_code([*common, "--partition", "2026/08/31", projected]) == 2
    invalid_id = [*common, "--partition", "2026-08-31", "--execution-id", "exec 1", projected]
    assert exit_code(invalid_id) == 2
    assert exit_code(["run", "--root", db.root, "--partition", "2026-08-31", projected]) == 2
    missing = "test_execution:missing_pipeline"
    assert exit_code([*common, "--partition", "2026-08-31", missing]) == 2
    removed = [*common, "--partition", "2026-08-31", "--export-mode", "register", projected]
    assert exit_code(removed) == 2
    printed_errors += capsys.readouterr().err
    assert "Traceback" not in printed_errors


def test_cli_run_without_partition(db: Database, folder: Path,
                                   monkeypatch: pytest.MonkeyPatch,
                                   capsys: pytest.CaptureFixture) -> None:
    """``serialize-db run`` sem ``--partition`` abre a execução sem partição: sai com 0 no pipeline
    que publica a tabela sem partição e com 2 no que pede as partições anteriores, sem
    traceback."""
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    monkeypatch.delenv("SERIALIZE_DB_ROOT", raising=False)
    common = ["run", "--root", db.root, "--environment", "prd",
              "--metadata", "test_execution:Base.metadata"]
    assert cli.main([*common, "test_execution:composite_pipeline"]) == 0
    published = delta.open_table(db.uri(Composta.__table__), db.storage)
    assert published.to_pyarrow_dataset().to_table().num_rows == 2
    assert cli.main([*common, "test_execution:previous_partitions_pipeline"]) == 2
    printed_errors = capsys.readouterr().err
    assert "não tem partição" in printed_errors
    assert "Traceback" not in printed_errors


def test_cli_run_exits_with_2_on_a_used_snapshot_name(db: Database, folder: Path,
                                                      monkeypatch: pytest.MonkeyPatch,
                                                      capsys: pytest.CaptureFixture) -> None:
    """``serialize-db run`` sai com 2, sem traceback, no pipeline que marca um nome de snapshot já
    usado, em ``snapshots`` ou em ``archived``, antes de qualquer commit e com o
    ``serialize-db channel`` na mensagem; e também quando outro escritor grava o nome entre a
    marcação e a saída da execução."""
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    monkeypatch.delenv("SERIALIZE_DB_ROOT", raising=False)
    common = ["run", "--root", db.root, "--environment", "prd",
              "--metadata", "test_execution:Base.metadata"]
    marked = "test_execution:marked_pipeline"
    assert cli.main([*common, "--partition", "2026-08-31", marked]) == 0
    projected_version = delta.open_table(db.uri(PROJECTED), db.storage).version()
    capsys.readouterr()  # descarta a saída da primeira execução

    # O nome em snapshots e, depois do arquivamento, em archived: nenhum commit na partição nova.
    assert cli.main([*common, "--partition", "2026-07-31", marked]) == 2
    delta.archive_snapshot(db.storage, "prd", "2026T3")
    assert cli.main([*common, "--partition", "2026-07-31", marked]) == 2
    assert delta.open_table(db.uri(PROJECTED), db.storage).version() == projected_version
    printed_errors = capsys.readouterr().err
    assert printed_errors.count("serialize-db run: prd: o snapshot 2026T3 já existe") == 2
    assert printed_errors.count("serialize-db channel") == 2

    # Outro escritor grava o nome entre a marcação e a saída.
    raced = "test_execution:raced_snapshot_pipeline"
    assert cli.main([*common, "--partition", "2026-07-31", raced]) == 2
    printed_errors += capsys.readouterr().err
    assert "serialize-db run: prd: o snapshot 2026T4 já existe" in printed_errors
    assert "Traceback" not in printed_errors


def test_cli_run_hands_the_redshift_config_to_the_execution(
    db: Database, folder: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``--engine redshift`` constrói o motor Redshift com as variáveis ``SERIALIZE_DB_REDSHIFT_*``
    e dá a configuração à execução; no motor DuckDB a execução não tem a configuração, e a
    publicação saiu dela: ``--redshift`` é um erro de uso."""
    from serialize_db.engine import redshift

    monkeypatch.setattr(redshift, "driver_connect", lambda login: IdleConnection())
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    for name, value in (("HOST", "host"), ("USER", "usuario"), ("PASSWORD", "senha"),
                        ("SCHEMA", "esquema"), ("SHARE_DATABASE", "compartilhado")):
        monkeypatch.setenv(f"SERIALIZE_DB_REDSHIFT_{name}", value)
    common = ["run", "--root", db.root, "--environment", "prd", "--partition", "2026-08-31",
              "--metadata", "test_execution:Base.metadata"]
    redshift_engine = [*common, "--engine", "redshift", "test_execution:redshift_engine_pipeline"]
    assert cli.main(redshift_engine) == 0
    assert exit_code([*common, "--redshift", "test_execution:projected_pipeline"]) == 2

    # No motor DuckDB, a execução não tem a configuração nem a publicação.
    with Execution(db, FakeEngine(db.storage), "2026-08-31") as run:
        assert run.redshift is None
        assert not hasattr(run, "publish_redshift")


def test_cli_exits_with_2_on_the_redshift_config_without_connection(
    db: Database, folder: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """``run`` e ``audit`` com ``--engine redshift`` e ``publish_redshift --init`` saem com 2,
    sem traceback, na configuração do Redshift sem conexão e na porta que não é número, com a
    variável na mensagem; com a conexão, ``run`` sai com 2 no ``--execution-id`` que não deixa 63
    bytes ao nome no prefixo do sandbox."""
    from serialize_db.engine import redshift

    monkeypatch.setattr(redshift, "driver_connect", lambda login: IdleConnection())
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    for name in ("WORKGROUP", "HOST", "USER", "PASSWORD", "PORT"):
        monkeypatch.delenv(f"SERIALIZE_DB_REDSHIFT_{name}", raising=False)
    common = ["--root", db.root, "--environment", "prd",
              "--metadata", "test_execution:Base.metadata"]
    run = ["run", *common, "--partition", "2026-08-31", "--engine", "redshift"]
    assert cli.main([*run, "test_execution:redshift_engine_pipeline"]) == 2
    audit = ["audit", *common, "--table", "cad_lancamentos", "--engine", "redshift"]
    assert cli.main(audit) == 2
    assert cli.main(["publish_redshift", "--init"]) == 2
    printed_errors = capsys.readouterr().err
    assert printed_errors.count("RedshiftConfig sem conexão") == 3

    # Com a conexão, o execution_id longo demais para o prefixo do sandbox.
    for name, value in (("HOST", "host"), ("USER", "usuario"), ("PASSWORD", "senha")):
        monkeypatch.setenv(f"SERIALIZE_DB_REDSHIFT_{name}", value)
    long_id = ["--execution-id", "x" * 60, "test_execution:redshift_engine_pipeline"]
    assert cli.main([*run, *long_id]) == 2
    printed_errors += capsys.readouterr().err
    assert "não deixa 63 bytes ao nome" in printed_errors

    # A porta que não é número.
    monkeypatch.setenv("SERIALIZE_DB_REDSHIFT_PORT", "cinco")
    with pytest.raises(ContractError, match="SERIALIZE_DB_REDSHIFT_PORT='cinco'"):
        redshift.RedshiftConfig.from_environment()
    assert cli.main([*run, "test_execution:redshift_engine_pipeline"]) == 2
    assert cli.main(audit) == 2
    assert cli.main(["publish_redshift", "--init"]) == 2
    port_errors = capsys.readouterr().err
    assert port_errors.count("SERIALIZE_DB_REDSHIFT_PORT='cinco'") == 3
    printed_errors += port_errors
    assert "Traceback" not in printed_errors


def test_cli_audit_prints_the_sql_and_audits_the_current_version(
    db: Database, folder: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """``serialize-db audit --sql`` imprime o texto das verificações no dialeto, sem armazenamento;
    sem ``--sql``, audita a versão atual do Delta e sai com 0 na aprovação."""
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    audit_command = ["audit", "--metadata", "test_execution:Base.metadata"]
    entries_audit = [*audit_command, "--table", "cad_lancamentos", "--partitions", "2026-08-31"]
    assert cli.main([*entries_audit, "--engine", "redshift", "--sql"]) == 0
    printed = capsys.readouterr().out
    assert "-- linhas" in printed
    assert "to_char(" in printed
    assert '"{prefix}cad_lancamentos"' in printed

    # A auditoria da versão atual do Delta.
    assert cli.main([*entries_audit, "--root", db.root, "--environment", "prd"]) == 0
    printed = capsys.readouterr().out
    assert "cad_lancamentos na versão 4:" in printed
    assert "chave_id_lancamento_tabela: aprovada" in printed
    assert "partição 2026-08-31: {'linhas': 10" in printed

    # A tabela fora do modelo.
    assert cli.main([*audit_command, "--table", "nao_existe", "--sql"]) == 2


def test_cli_audit_of_a_table_without_partition(
    db: Database, folder: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """``serialize-db audit`` de uma tabela sem partição imprime os totais da ``tabela inteira``;
    com ``--partitions``, com ou sem ``--sql``, sai com 2 antes de abrir um motor, sem
    traceback."""
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    composite = Composta.__table__
    uri = db.uri(composite)
    delta.create_table(uri, composite, db.storage)
    data = pa.table({"id_a": pa.array([1, 2], pa.int64()), "id_b": pa.array([1, 1], pa.int64())})
    delta.publish_partition(uri, composite, None, schema.cast(data, composite), {}, db.storage)
    composite_audit = ["audit", "--metadata", "test_execution:Base.metadata",
                       "--table", "rel_composta", "--root", db.root, "--environment", "prd"]
    assert cli.main(composite_audit) == 0
    printed = capsys.readouterr().out
    assert "rel_composta na versão 1:" in printed
    assert "tabela inteira: {'linhas': 2" in printed
    assert "partição None" not in printed

    # --partitions numa tabela sem partição, na auditoria e no texto.
    with_partitions = [*composite_audit, "--partitions", "2026-08-31"]
    assert cli.main(with_partitions) == 2
    assert cli.main([*with_partitions, "--sql"]) == 2
    printed_errors = capsys.readouterr().err
    assert printed_errors.count("rel_composta não tem partição") == 2
    assert "Traceback" not in printed_errors


def publish_defects(db: Database) -> int:
    """``cad_lancamentos_projetados`` de ``tests/lancamentos_model.py`` gravada por uma cópia do
    modelo com ``valor`` anulável: um nulo em ``valor``, ``NOT NULL`` no modelo, na primeira
    partição, e um JSON malformado em ``meta`` na segunda; devolve a versão atual."""
    loose = lancamentos_model.PROJECTED.to_metadata(sa.MetaData())
    loose.c.valor.nullable = True
    uri = db.uri(lancamentos_model.PROJECTED)
    delta.create_table(uri, loose, db.storage)
    null_month, json_month = lancamentos_model.MONTHS
    with_null = lancamentos_model.entry_rows(null_month, 1, 3, loose, valor=[1.0, None, 3.0])
    delta.publish_partition(uri, loose, null_month, with_null, {}, db.storage)
    malformed = lancamentos_model.entry_rows(json_month, 10, 3, loose)
    meta = malformed.schema.get_field_index("meta")
    malformed = malformed.set_column(meta, "meta", pa.array(['{"k": 1}', "{", "[]"]))
    return delta.publish_partition(uri, loose, json_month, malformed, {}, db.storage)


def defects_audit(db: Database) -> list[str]:
    """Os argumentos de ``serialize-db audit`` da tabela de ``publish_defects``, a completar com
    as partições."""
    return ["audit", "--metadata", "lancamentos_model:Base.metadata",
            "--table", "cad_lancamentos_projetados", "--root", db.root, "--environment", "prd"]


def test_cli_audit_prints_the_refused_ingest_as_a_failed_audit(
    folder: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """A auditoria da partição com um nulo numa coluna ``NOT NULL`` e a da partição com JSON
    malformado, que o ``ingest`` pelo DDL do modelo recusa, saem com 1 e imprimem o erro do banco
    como a reprovação da ingestão, sem as outras contagens e sem traceback."""
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    db = Database(str(folder / "delta"), "prd", lancamentos_model.Base.metadata)
    version = publish_defects(db)
    null_month, json_month = lancamentos_model.MONTHS

    # O nulo na coluna NOT NULL.
    assert cli.main([*defects_audit(db), "--partitions", null_month]) == 1
    printed = capsys.readouterr()
    assert printed.out.splitlines() == [
        f"cad_lancamentos_projetados na versão {version}:",
        "ingestão: reprovada (Constraint Error: NOT NULL constraint failed: "
        "cad_lancamentos_projetados.valor)",
    ]
    printed_errors = printed.err

    # O JSON malformado.
    assert cli.main([*defects_audit(db), "--partitions", json_month]) == 1
    printed = capsys.readouterr()
    lines = printed.out.splitlines()
    assert lines[0] == f"cad_lancamentos_projetados na versão {version}:"
    assert lines[1].startswith("ingestão: reprovada (Conversion Error: Malformed JSON")
    assert len(lines) == 2
    printed_errors += printed.err
    assert "Traceback" not in printed_errors


def test_cli_audit_lets_an_access_error_of_the_ingest_propagate(
    folder: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """Um erro do banco que não é a recusa de um valor, como o acesso negado a um arquivo da
    tabela, sobe do ``ingest`` da auditoria e de ``cli.main`` sem a ingestão reprovada impressa, e
    o script de console sai com o traceback."""
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    db = Database(str(folder / "delta"), "prd", lancamentos_model.Base.metadata)
    publish_defects(db)

    def ingest_without_access(engine: DuckDBEngine, *args: object, **options: object) -> None:
        raise duckdb.IOException("HTTP 403: acesso negado ao arquivo da tabela")

    monkeypatch.setattr(DuckDBEngine, "ingest", ingest_without_access)
    null_month = lancamentos_model.MONTHS[0]
    with pytest.raises(duckdb.IOException, match="HTTP 403: acesso negado"):
        cli.main([*defects_audit(db), "--partitions", null_month])
    assert "reprovada" not in capsys.readouterr().out


@pytest.mark.redshift
@pytest.mark.s3
def test_cli_audit_on_redshift_prints_the_refused_ingest(
    s3_location: S3Location, redshift_driver: None, folder: Path,
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """Com ``--engine redshift``, o ``INSERT`` da staging na tabela do modelo recusa o nulo na
    coluna ``NOT NULL`` e o JSON malformado do ``JSON_PARSE``: cada auditoria sai com 1 e imprime o
    erro do servidor como a reprovação da ingestão, sem as outras contagens e sem traceback. O
    relatório da sessão guarda a linha impressa, que no ambiente alvo traz a mensagem dele."""
    config = redshift_config()
    for name, value in (("SCHEMA", config.schema), ("HOST", config.host), ("USER", config.user),
                        ("PASSWORD", config.password), ("WORKGROUP", config.workgroup)):
        if value is None:
            monkeypatch.delenv(f"SERIALIZE_DB_REDSHIFT_{name}", raising=False)
        else:
            monkeypatch.setenv(f"SERIALIZE_DB_REDSHIFT_{name}", str(value))
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    root = s3_location.child(f"auditoria/{uuid.uuid4().hex[:8]}")
    db = Database(root, "prd", lancamentos_model.Base.metadata)
    version = publish_defects(db)
    for month in lancamentos_model.MONTHS:
        audit = [*defects_audit(db), "--engine", "redshift", "--partitions", month]
        exit_status = cli.main(audit)
        printed = capsys.readouterr()
        record(f"redshift.audit.refused_ingest.{month}", printed.out)
        assert exit_status == 1
        lines = printed.out.splitlines()
        assert lines[0] == f"cad_lancamentos_projetados na versão {version}:"
        assert lines[1].startswith("ingestão: reprovada (")
        assert len(lines) == 2
        assert "Traceback" not in printed.err


def test_cli_refuses_an_unknown_engine(
    db: Database, folder: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    """``SERIALIZE_DB_ENGINE`` fora de ``duckdb`` e ``redshift`` é erro de uso em ``run`` e em
    ``audit``, com e sem ``--sql``, como o mesmo valor em ``--engine``; o motor da auditoria
    recusa o nome desconhecido em vez de abrir o DuckDB."""
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    monkeypatch.setenv("SERIALIZE_DB_ENGINE", "Redshift")
    common = ["--root", db.root, "--environment", "prd",
              "--metadata", "test_execution:Base.metadata"]
    audit = ["audit", *common, "--table", "cad_lancamentos"]
    assert exit_code(audit) == 2
    assert exit_code([*audit, "--sql"]) == 2
    run = ["run", *common, "--partition", "2026-08-31", "test_execution:projected_pipeline"]
    assert exit_code(run) == 2
    assert exit_code([*run[:-1], "--engine", "Redshift", run[-1]]) == 2
    printed_errors = capsys.readouterr().err
    assert printed_errors.count("motor 'Redshift'") == 4
    assert "Traceback" not in printed_errors

    # O motor da auditoria, chamado com o nome desconhecido.
    with pytest.raises(ContractError, match="motor 'Redshift'"):
        cli._audit_engine(argparse.Namespace(engine="Redshift"), db, "auditoria-1")
