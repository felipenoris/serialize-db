"""Uma execução completa do pipeline mensal sobre a base de testes em Delta, no motor DuckDB.

A condição inicial é a base fictícia de ``tests/source_db_projetado.py`` carregada no Delta por
``serialize_db.load.initial_load``, as 12 tabelas do modelo cliente (``tests/client_model/``) com
as quatro partições de cada tabela particionada; ela é gravada uma vez por módulo sob
``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``), e cada teste trabalha numa cópia própria,
para publicar a partição nova sem tocar a base dos outros. ``monthly_pipeline`` é o pipeline, no
formato ``modulo:funcao`` que ``serialize-db run`` recebe: a execução na partição do mês seguinte
à última data-base publicada; a ingestão de toda tabela do modelo, as sem partição inteiras e as
particionadas só na última data-base; o ``SELECT`` com ``join`` dos saldos por conta do modelo
cliente sobre essa data-base; a geração da partição nova de ``cad_operacoes``,
``rel_contrato_operacao``, ``cad_contratos`` e ``cad_lancamentos`` a partir da última, por
``INSERT ... SELECT`` no sandbox com os ids de ``next_ids``; o ``join`` de contratos, relação e
operações que confere o rateio da partição nova; a auditoria com as chaves estrangeiras; e a
publicação no Delta.

Os testes conferem os saldos e as contagens contra a base fictícia em memória, o rateio de cada
contrato, as auditorias aprovadas com toda verificação rodada, a versão nova de cada tabela só com
a partição nova alterada, os metadados do commit, a releitura pelo leitor Delta, e a execução do
mês seguinte sobre a partição publicada, pela linha de comando e pela API. O motor ``"duckdb"`` da
execução e o leitor nascem na pasta temporária do processo, apontada para a pasta do teste. A
extensão ``delta`` do DuckDB precisa estar na pasta de extensões.
"""

from __future__ import annotations

import dataclasses
import datetime
import json
import shutil
import tempfile
import uuid
from pathlib import Path

import pyarrow as pa
import pytest
import sqlalchemy as sa

import source_db_projetado as source
from client_model import Base, Contrato, Lancamento, Operacao, RelContratoOperacao
from client_model.statements import STATEMENTS
from conftest import LocalLocation
from serialize_db import cli, delta, load
from serialize_db.audit import AuditReport
from serialize_db.engine.duckdb import DuckDBConfig
from serialize_db.execution import Database, Execution
from serialize_db.reader import DeltaReader
from serialize_db.schema import table_options

pytestmark = pytest.mark.local

OPERATIONS = Operacao.__table__
APPORTIONMENTS = RelContratoOperacao.__table__
CONTRACTS = Contrato.__table__
ENTRIES = Lancamento.__table__
# As tabelas que o pipeline grava, na ordem das chaves estrangeiras: a referenciada antes da que a
# referencia.
PRODUCED = (OPERATIONS, CONTRACTS, APPORTIONMENTS, ENTRIES)

LAST_BASE_DATE = source.PARTITION_VALUES[-1]
NEXT_BASE_DATE = "2026-07-31"
FOLLOWING_BASE_DATE = "2026-08-31"
EXECUTION_ID = "exec-2026-07-31"
# O carimbo dos lançamentos gerados: o instante da execução mensal.
WRITTEN_AT = datetime.datetime(2026, 8, 3, 9, 15, 0)

# A partição de referência do join do rateio, o parâmetro que a execução preenche.
REFERENCE = sa.bindparam("data_str", type_=sa.String(10))

# O rateio de cada contrato entre as operações dele na partição de referência: contratos, a
# relação e operações; fator_rateio soma 1 por contrato.
APPORTIONMENT_BY_CONTRACT = (
    sa.select(
        CONTRACTS.c.contrato,
        sa.func.count(OPERATIONS.c.id_operacao).label("operacoes"),
        sa.func.sum(APPORTIONMENTS.c.fator_rateio).label("rateio"),
    )
    .join_from(
        CONTRACTS,
        APPORTIONMENTS,
        sa.and_(
            APPORTIONMENTS.c.data == CONTRACTS.c.data,
            APPORTIONMENTS.c.sistema == CONTRACTS.c.sistema,
            APPORTIONMENTS.c.contrato == CONTRACTS.c.contrato,
        ),
    )
    .join(
        OPERATIONS,
        sa.and_(
            OPERATIONS.c.data == APPORTIONMENTS.c.data,
            OPERATIONS.c.operacao == APPORTIONMENTS.c.operacao,
        ),
    )
    .where(CONTRACTS.c.data_str == REFERENCE)
    .group_by(CONTRACTS.c.contrato)
    .order_by(CONTRACTS.c.contrato)
)


# ---------------------------------------------------------------- o pipeline mensal


@dataclasses.dataclass
class MonthlyReport:
    """O que a execução mensal leu e gravou, para as asserções dos testes."""

    previous: str
    """A última data-base publicada antes da partição da execução."""
    balances: pa.Table
    """Os saldos por conta na última data-base, o resultado de ``saldos_por_conta``."""
    produced: dict[str, int]
    """As linhas geradas na partição nova, por tabela."""
    apportionment: pa.Table
    """O rateio por contrato na partição nova, o resultado de ``APPORTIONMENT_BY_CONTRACT``."""
    audits: dict[str, AuditReport]
    """O relatório aprovado de cada tabela gravada."""
    versions: dict[str, int]
    """A versão publicada de cada tabela gravada."""


def last_base_date(run: Execution) -> str:
    """A última data-base publicada antes da partição da execução."""
    (previous,) = run.previous_partitions(ENTRIES, 1)
    return previous


def ingest_model(run: Execution, previous: str) -> None:
    """Traz ao sandbox toda tabela do modelo: as sem partição inteiras, como view, e as
    particionadas só na última data-base, materializadas, porque recebem as linhas da nova."""
    unpartitioned = []
    partitioned = []
    for table in run.db.tables():
        if table_options(table).partition_by is None:
            unpartitioned.append(table)
        else:
            partitioned.append(table)
    run.ingest(*unpartitioned)
    run.ingest(*partitioned, partitions=[previous], materialize=True)


def count_rows(run: Execution, table: sa.Table, condition: sa.ColumnElement) -> int:
    """Quantas linhas da tabela do sandbox satisfazem ``condition``."""
    statement = sa.select(sa.func.count()).select_from(table).where(condition)
    return run.sandbox.query(statement).column(0)[0].as_py()


def carry_forward(run: Execution, table: sa.Table, previous: str, current: str,
                  replaced: dict[str, sa.ColumnElement] | None = None,
                  only: sa.ColumnElement | None = None) -> int:
    """Leva as linhas da última data-base à nova, num ``INSERT ... SELECT`` no sandbox.

    A chave sequencial recebe os ids de ``next_ids``, a coluna de origem da partição recebe a nova
    data-base e a coluna de partição o valor dela; ``replaced`` troca outras colunas pela
    expressão dada, e ``only`` restringe as linhas levadas. Devolve quantas linhas entraram.
    """
    options = table_options(table)
    (key,) = table.primary_key.columns
    in_previous = table.c[options.partition_by] == previous
    if only is not None:
        in_previous = sa.and_(in_previous, only)
    count = count_rows(run, table, in_previous)
    ids = run.next_ids(table, count)

    # A n-ésima linha, na ordem da chave antiga, recebe o n-ésimo id da faixa.
    new_key = sa.func.row_number().over(order_by=key) + (ids.start - 1)
    replacements = {
        key.name: new_key,
        options.partition_source: sa.literal(datetime.date.fromisoformat(current)),
        options.partition_by: sa.literal(current),
        **(replaced or {}),
    }
    selected = []
    for column in table.columns:
        selected.append(replacements.get(column.name, column).label(column.name))
    names = [column.name for column in table.columns]
    statement = sa.insert(table).from_select(names, sa.select(*selected).where(in_previous))
    run.sandbox.query(statement)
    return count


def produce_next_month(run: Execution, previous: str, current: str) -> dict[str, int]:
    """Gera a partição nova das quatro tabelas a partir da última: operações, contratos e a
    relação entre eles como estão, e os lançamentos só nos meses posteriores à nova data-base, com
    o carimbo desta execução. Devolve as linhas geradas por tabela."""
    produced = {}
    for table in (OPERATIONS, CONTRACTS, APPORTIONMENTS):
        produced[table.name] = carry_forward(run, table, previous, current)
    still_ahead = ENTRIES.c.data > datetime.date.fromisoformat(current)
    produced[ENTRIES.name] = carry_forward(run, ENTRIES, previous, current,
                                           replaced={"timestamp": sa.literal(WRITTEN_AT)},
                                           only=still_ahead)
    return produced


def monthly_pipeline(run: Execution) -> MonthlyReport:
    """O pipeline mensal sobre a partição da execução: a ingestão do modelo, os saldos da última
    data-base, a partição nova das quatro tabelas, o rateio dela, a auditoria e a publicação.

    É a função ``modulo:funcao`` de ``serialize-db run``, que ignora o relatório devolvido.
    """
    current = run.partition
    previous = last_base_date(run)
    ingest_model(run, previous)
    balances = run.sandbox.query(STATEMENTS["saldos_por_conta"], {"data_base_str": previous})
    produced = produce_next_month(run, previous, current)
    apportionment = run.sandbox.query(APPORTIONMENT_BY_CONTRACT, {"data_str": current})
    audits = {}
    for table in PRODUCED:
        audits[table.name] = run.audit(table, [current], foreign_keys=True)
    versions = run.publish_delta(*PRODUCED, partitions=[current])
    return MonthlyReport(previous, balances, produced, apportionment, audits, versions)


# ---------------------------------------------------------------- a base de testes em Delta


@pytest.fixture(scope="module")
def delta_base(local_location: LocalLocation) -> Path:
    """A base de testes na versão Delta, a condição inicial de todo teste: a base Parquet fictícia
    carregada uma vez por módulo, tabela a tabela, na ordem da carga."""
    folder = Path(local_location.child("pipeline/base"))
    parquet = source.write_source(folder / "parquet")
    root = folder / "delta"
    db = Database(str(root), "prd", Base.metadata)
    config = DuckDBConfig(temp_directory=str(folder / "carga"))
    for table in load.load_order(db.tables()):
        load.initial_load(db, table, str(parquet.root), config=config)
    return root


@pytest.fixture
def folder(local_location: LocalLocation) -> Path:
    """Uma pasta nova por teste sob a raiz da sessão."""
    path = Path(local_location.child(f"pipeline/{uuid.uuid4().hex[:8]}"))
    path.mkdir(parents=True)
    return path


@pytest.fixture
def db(delta_base: Path, folder: Path, monkeypatch: pytest.MonkeyPatch) -> Database:
    """O banco do teste: uma cópia própria da base Delta, onde a partição nova é publicada; a
    pasta temporária do processo, onde o motor ``"duckdb"`` da execução e o leitor Delta nascem,
    aponta para a pasta do teste."""
    shutil.copytree(delta_base, folder / "delta")
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    return Database(str(folder / "delta"), "prd", Base.metadata)


# ---------------------------------------------------------------- o esperado, da base em memória


def source_tables() -> dict[str, pa.Table | dict[str, pa.Table]]:
    """A base fictícia em memória, a mesma que a carga gravou."""
    return source.build_tables()


def expected_balances(tables: dict, base_date: str) -> list[dict[str, object]]:
    """Os saldos por conta de ``saldos_por_conta`` calculados sobre a base em memória: a soma de
    ``valor`` dos lançamentos da data-base por conta que permite lançamentos, na ordem do
    número."""
    accounts = {row["id_conta"]: row for row in tables["cad_contas"].to_pylist()}
    totals: dict[tuple[str, str], float] = {}
    for row in tables["cad_lancamentos"][base_date].to_pylist():
        account = accounts[row["id_conta"]]
        if not account["permite_lancamentos"]:
            continue
        key = (account["numero"], account["nome"])
        totals[key] = totals.get(key, 0.0) + row["valor"]
    balances = []
    for (numero, nome), saldo in sorted(totals.items()):
        balances.append({"numero": numero, "nome": nome, "saldo": saldo})
    return balances


def entries_still_ahead(tables: dict, base_date: str, current: str) -> int:
    """Quantos lançamentos da data-base ``base_date`` projetam um mês posterior a ``current``."""
    month = datetime.date.fromisoformat(current)
    count = 0
    for row in tables["cad_lancamentos"][base_date].to_pylist():
        if row["data"] > month:
            count += 1
    return count


def expected_produced(tables: dict, current: str) -> dict[str, int]:
    """As linhas que a partição ``current`` recebe de cada tabela: as da última data-base, e nos
    lançamentos só as dos meses posteriores a ``current``."""
    produced = {}
    for table in (OPERATIONS, CONTRACTS, APPORTIONMENTS):
        produced[table.name] = tables[table.name][LAST_BASE_DATE].num_rows
    produced[ENTRIES.name] = entries_still_ahead(tables, LAST_BASE_DATE, current)
    return produced


def partition_counts(reader: DeltaReader, table: sa.Table) -> dict[str, int]:
    """As linhas de cada partição da tabela, pelo leitor Delta, em ordem de texto do valor."""
    partition_by = table_options(table).partition_by
    column = table.c[partition_by]
    statement = (sa.select(column, sa.func.count().label("linhas"))
                 .group_by(column).order_by(column))
    rows = reader.query(statement).to_pylist()
    return {row[partition_by]: row["linhas"] for row in rows}


def assert_balances_match(found: pa.Table, expected: list[dict[str, object]]) -> None:
    """As mesmas contas na mesma ordem, e cada saldo igual ao da base em memória, a menos da ordem
    da soma em ponto flutuante."""
    rows = found.to_pylist()
    found_accounts = [(row["numero"], row["nome"]) for row in rows]
    wanted_accounts = [(row["numero"], row["nome"]) for row in expected]
    assert found_accounts == wanted_accounts
    for row, wanted in zip(rows, expected, strict=True):
        assert row["saldo"] == pytest.approx(wanted["saldo"], rel=1e-12, abs=1e-3), row


def assert_apportionment_sums_to_one(found: pa.Table, contracts: int) -> None:
    """Todo contrato da partição nova está em uma ou duas operações, e os fatores dele somam
    exatamente 1."""
    rows = found.to_pylist()
    assert len(rows) == contracts
    for row in rows:
        assert row["operacoes"] in (1, 2), row
        assert row["rateio"] == 1.0, row


def assert_audits_ran_every_check(audits: dict[str, AuditReport], current: str) -> None:
    """Cada relatório aprovado cobre só a partição nova, sem verificação por rodar; as chaves
    estrangeiras compostas de ``rel_contrato_operacao`` e ``cad_lancamentos`` rodaram, e a chave
    sequencial foi dispensada da junção com a versão fixada, porque os ids novos passam do maior
    dela."""
    assert set(audits) == {table.name for table in PRODUCED}
    for report in audits.values():
        assert report.passed, report.results
        assert report.partitions == (current,)
        assert report.not_run == ()
    checks = {name: [result.name for result in report.results] for name, report in audits.items()}
    assert "orfao_data_operacao" in checks[APPORTIONMENTS.name]
    assert "orfao_data_base_sistema_contrato" in checks[ENTRIES.name]
    assert "orfao_id_conta" in checks[ENTRIES.name]
    for report in audits.values():
        against_table = []
        for result in report.results:
            if result.name.endswith("_tabela"):
                against_table.append(result)
        (table_check,) = against_table
        assert table_check.reason.startswith("dispensada"), table_check


# ---------------------------------------------------------------- a execução


def test_monthly_pipeline_publishes_the_next_base_date(db: Database) -> None:
    """A execução de 2026-07-31 lê 2026-06-30 como a última data-base, devolve os saldos por conta
    da base em memória, gera a partição nova com as contagens esperadas e o rateio somando 1 por
    contrato, aprova as quatro auditorias com toda verificação rodada e publica uma versão a mais
    em cada tabela, só com a partição nova alterada, com o ``execution_id`` e as versões lidas no
    commit; o leitor Delta lê as partições antigas intactas e a nova."""
    tables = source_tables()
    with Execution(db, "duckdb", NEXT_BASE_DATE, execution_id=EXECUTION_ID) as run:
        pinned = dict(run.versions)
        report = monthly_pipeline(run)

    # O que a execução leu e gerou.
    assert report.previous == LAST_BASE_DATE
    assert_balances_match(report.balances, expected_balances(tables, LAST_BASE_DATE))
    assert report.produced == expected_produced(tables, NEXT_BASE_DATE)
    assert_apportionment_sums_to_one(report.apportionment, report.produced[CONTRACTS.name])
    assert_audits_ran_every_check(report.audits, NEXT_BASE_DATE)

    # Uma versão a mais em cada tabela gravada, só com a partição nova alterada, e os metadados
    # do commit.
    assert report.versions == {table.name: pinned[table.name] + 1 for table in PRODUCED}
    for table in PRODUCED:
        uri = db.uri(table)
        partition_by = table_options(table).partition_by
        current = delta.open_table(uri, db.storage)
        assert current.version() == report.versions[table.name]
        values = delta.partition_values(current, partition_by)
        assert values == [*source.PARTITION_VALUES, NEXT_BASE_DATE], table.name
        changed = delta.version_diff(uri, pinned[table.name], current.version(), table, db.storage)
        assert changed == {NEXT_BASE_DATE}, table.name
        commit = delta.history(uri, db.storage)[0]
        assert commit["serialize_db_execution_id"] == EXECUTION_ID
        assert json.loads(commit["serialize_db_input_versions"]) == pinned
        assert "serialize_db_snapshot" not in commit

    # O leitor Delta: as partições antigas com as linhas da carga e a nova com as geradas, e os
    # lançamentos novos com a data-base nova, o carimbo da execução, os ids acima do maior da base
    # e os meses projetados todos posteriores à data-base.
    new_entries = (sa.select(ENTRIES.c.id_lancamento, ENTRIES.c.data_base, ENTRIES.c.data,
                             ENTRIES.c.timestamp)
                   .where(ENTRIES.c.data_base_str == NEXT_BASE_DATE))
    with db.open_delta(channel=delta.CURRENT_CHANNEL) as reader:
        for table in PRODUCED:
            counts = partition_counts(reader, table)
            for value in source.PARTITION_VALUES:
                assert counts[value] == tables[table.name][value].num_rows, (table.name, value)
            assert counts[NEXT_BASE_DATE] == report.produced[table.name], table.name
        entries = reader.query(new_entries).to_pylist()
    new_month = datetime.date.fromisoformat(NEXT_BASE_DATE)
    assert len(entries) == report.produced[ENTRIES.name]
    assert min(row["id_lancamento"] for row in entries) > source.MAX_ENTRY_ID
    assert {row["data_base"] for row in entries} == {new_month}
    assert {row["timestamp"] for row in entries} == {WRITTEN_AT}
    assert all(row["data"] > new_month for row in entries)


def test_following_month_runs_over_the_published_partition(
    db: Database, capsys: pytest.CaptureFixture
) -> None:
    """``serialize-db run`` roda o mesmo pipeline em 2026-07-31 e sai com 0; a execução seguinte,
    em 2026-08-31, lê a partição publicada como a última data-base, gera a dela com um mês
    projetado a menos nos lançamentos e publica mais uma versão em cada tabela."""
    tables = source_tables()
    arguments = ["run", "--root", db.root, "--environment", "prd", "--partition", NEXT_BASE_DATE,
                 "--metadata", "client_model:Base.metadata", "test_pipeline:monthly_pipeline"]
    assert cli.main(arguments) == 0
    assert "Traceback" not in capsys.readouterr().err

    with Execution(db, "duckdb", FOLLOWING_BASE_DATE) as run:
        pinned = dict(run.versions)
        report = monthly_pipeline(run)
    assert report.previous == NEXT_BASE_DATE
    expected = expected_produced(tables, FOLLOWING_BASE_DATE)
    assert expected[ENTRIES.name] < expected_produced(tables, NEXT_BASE_DATE)[ENTRIES.name]
    assert report.produced == expected
    assert_apportionment_sums_to_one(report.apportionment, report.produced[CONTRACTS.name])
    assert_audits_ran_every_check(report.audits, FOLLOWING_BASE_DATE)
    assert report.versions == {table.name: pinned[table.name] + 1 for table in PRODUCED}
    with db.open_delta(channel=delta.CURRENT_CHANNEL) as reader:
        for table in PRODUCED:
            counts = partition_counts(reader, table)
            assert list(counts) == [*source.PARTITION_VALUES, NEXT_BASE_DATE, FOLLOWING_BASE_DATE]
            assert counts[FOLLOWING_BASE_DATE] == report.produced[table.name], table.name
