"""``serialize_db.reader`` e ``Database.open_delta``/``open_redshift``: o acesso de leitura à base
com o modelo, pelo leitor Delta e pelo leitor Redshift.

Os casos do leitor Delta gravam sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``): o banco
de ``tests/lancamentos_model.py`` numa pasta por teste, que também recebe a pasta temporária do
processo, porque ``DuckDBConfig()`` cria a pasta do motor em ``tempfile.mkdtemp``. Eles conferem
o canal ``default`` sem argumento e o erro sem o canal; o snapshot pelo nome, vivo e arquivado; o
canal ``current`` e a view presa à versão da abertura; a tabela criada depois do snapshot, sem
view; a materialização inteira e parcial, em paralelo, e a que falha e devolve a view; os tipos
do contrato em ``query``, em ``stream`` e no pandas; a recusa dos comandos; a poda das partições
pelo log ``FileSystem`` do DuckDB; e o ``close`` e o finalizador que apagam a pasta do motor. Os
casos do leitor Redshift sem conexão rodam sobre a conexão de mentira de
``tests/test_engine_redshift.py``: o prefixo ``<ambiente>_`` no texto compilado, o sentinela, a
recusa dos comandos e do ``stream`` sem ``unload_to`` antes de qualquer comando, a configuração
lida das variáveis e o ``UNLOAD`` de ``db.open_redshift`` em ``staging/``. O caso marcado
``redshift``, ``s3`` e ``local`` publica uma tabela pela fixture de ``tests/test_publication.py``
e lê o mesmo resultado pelos dois leitores, com os arquivos do ``UNLOAD`` apagados no ``close``.
"""

from __future__ import annotations

import gc
import tempfile
import uuid
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pytest
import sqlalchemy as sa

from conftest import LocalLocation, S3Location, opened_partition_folders, record
from lancamentos_model import (
    ACCOUNTS,
    ENTRIES,
    MONTHS,
    PROJECTED,
    Base,
    account_rows,
    entry_rows,
)
from serialize_db import delta, publication, schema
from serialize_db.engine import redshift
from serialize_db.engine.duckdb import DuckDBEngine
from serialize_db.errors import ContractError
from serialize_db.execution import Database
from serialize_db.reader import DeltaReader, RedshiftReader, open_redshift
from serialize_db.storage import Storage
from test_engine_redshift import CONFIG, FakeConnection
from test_publication import Target, export_with_duckdb, target  # noqa: F401  (a fixture)

THREE_MONTHS = ["2026-06-30", "2026-07-31", "2026-08-31"]
METADATA = delta.commit_metadata("exec-0", {})


@pytest.fixture
def folder(local_location: LocalLocation, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Uma pasta nova por teste sob a raiz da sessão, que também recebe a pasta temporária do
    processo, onde ``DuckDBConfig()`` cria a pasta do motor."""
    path = Path(local_location.child(f"leitor/{uuid.uuid4().hex[:8]}"))
    path.mkdir(parents=True)
    monkeypatch.setattr(tempfile, "tempdir", str(path))
    return path


@pytest.fixture
def db(folder: Path) -> Database:
    """O banco do teste: os lançamentos em três partições, versões 1 a 3, as contas, versão 1, e
    os snapshots ``2026T2`` (lançamentos na versão 2) e ``2026T3`` (na 3), sem canal."""
    database = Database(str(folder / "delta"), "prd", Base.metadata)
    storage = database.storage
    delta.create_table(database.uri(ENTRIES), ENTRIES, storage)
    for index, month in enumerate(THREE_MONTHS):
        data = entry_rows(month, 1 + index * 10, 10)
        delta.publish_partition(database.uri(ENTRIES), ENTRIES, month, data, METADATA, storage)
    delta.create_table(database.uri(ACCOUNTS), ACCOUNTS, storage)
    delta.publish_partition(database.uri(ACCOUNTS), ACCOUNTS, None, account_rows(["A", "B", "C"]),
                            METADATA, storage)
    delta.snapshot(storage, "prd", "2026T2", {ENTRIES.name: 2, ACCOUNTS.name: 1})
    delta.snapshot(storage, "prd", "2026T3", {ENTRIES.name: 3, ACCOUNTS.name: 1})
    return database


def count(reader: DeltaReader, table: sa.Table, month: str | None = None) -> int:
    """As linhas da tabela pelo leitor, só as da partição ``month`` quando informada."""
    statement = sa.select(sa.func.count()).select_from(table)
    if month is not None:
        statement = statement.where(table.c.data_base_str == month)
    return reader.query(statement).column(0)[0].as_py()


def entry_ids(reader: DeltaReader) -> list[int]:
    """Os ids dos lançamentos pelo leitor, em ordem."""
    statement = sa.select(ENTRIES.c.id_lancamento).order_by(ENTRIES.c.id_lancamento)
    return reader.query(statement).column(0).to_pylist()


def objects(reader: DeltaReader, kind: str) -> list[str]:
    """As views ou as tabelas do banco do leitor, sem as internas do DuckDB; ``kind`` é ``views``
    ou ``tables``."""
    column = "view_name" if kind == "views" else "table_name"
    text = f"SELECT {column} FROM duckdb_{kind}() WHERE NOT internal ORDER BY 1"
    return reader.query(text).column(0).to_pylist()


def add_entries(db: Database, month: str, start: int, rows: int) -> None:
    """Substitui a partição ``month`` dos lançamentos por ``rows`` linhas a partir de ``start``."""
    delta.publish_partition(db.uri(ENTRIES), ENTRIES, month, entry_rows(month, start, rows),
                            METADATA, db.storage)


# ---------------------------------------------------------------- o leitor Delta


@pytest.mark.local
def test_default_channel_reads_the_snapshot_it_points_to(db: Database, folder: Path) -> None:
    """Sem o canal ``default`` no ambiente, ``open_delta()`` é ``ContractError`` com o comando que
    o cria e o canal ``current``, sem abrir o motor; com o canal, o leitor lê o snapshot apontado,
    com uma view por tabela dele; mover o canal muda o que o leitor seguinte lê, não o aberto."""
    with pytest.raises(ContractError, match="serialize-db channel") as failure:
        db.open_delta()
    assert delta.CURRENT_CHANNEL in str(failure.value)
    assert list(folder.glob("serialize_db_*")) == []

    delta.set_channel(db.storage, "prd", delta.DEFAULT_CHANNEL, "2026T2")
    with db.open_delta() as reader:
        assert reader.snapshot == "2026T2"
        assert reader.versions == {ACCOUNTS.name: 1, ENTRIES.name: 2}
        assert objects(reader, "views") == [ACCOUNTS.name, ENTRIES.name]
        assert count(reader, ENTRIES) == 20
        assert count(reader, ACCOUNTS) == 3
        delta.set_channel(db.storage, "prd", delta.DEFAULT_CHANNEL, "2026T3")
        assert count(reader, ENTRIES) == 20
    with db.open_delta(channel=delta.DEFAULT_CHANNEL) as reader:
        assert reader.snapshot == "2026T3"
        assert count(reader, ENTRIES) == 30


@pytest.mark.local
def test_named_snapshot_live_and_archived(db: Database) -> None:
    """``snapshot=`` lê o snapshot pelo nome; depois do ``archive``, o mesmo nome lê a cópia em
    ``arquivo/<nome>/``, na versão de cada cópia, com as mesmas linhas, enquanto a tabela viva
    avança; o nome ausente é ``ContractError``."""
    storage = db.storage
    with db.open_delta(snapshot="2026T2") as reader:
        assert reader.versions == {ACCOUNTS.name: 1, ENTRIES.name: 2}
        before = entry_ids(reader)
    assert before == list(range(1, 21))

    # O archive copia cada tabela do snapshot e move a entrada; a tabela viva avança.
    control, _ = delta.read_snapshots(storage, "prd")
    copied = {}
    for name, version in delta.snapshot_versions(control, "2026T2").items():
        source = storage.uri_of(storage.join("prd", name))
        destination = storage.uri_of(storage.join(db.archive_prefix("2026T2"), name))
        copied[name] = delta.deep_copy(source, version, destination, storage)
    delta.archive_snapshot(storage, "prd", "2026T2")
    add_entries(db, THREE_MONTHS[0], 100, 5)   # a versão 4 da tabela viva

    with db.open_delta(snapshot="2026T2") as reader:
        assert reader.snapshot == "2026T2"
        assert reader.versions == copied
        assert entry_ids(reader) == before
        text = "SELECT sql FROM duckdb_views() WHERE view_name = 'cad_lancamentos'"
        assert "arquivo/2026T2" in reader.query(text).column(0)[0].as_py()
    with pytest.raises(ContractError, match="2026T9 não existe"):
        db.open_delta(snapshot="2026T9")


@pytest.mark.local
def test_current_channel_reads_the_current_version_pinned_at_open(db: Database) -> None:
    """``channel="current"`` lê a versão atual de cada tabela do modelo que existe no ambiente,
    sem snapshot; o commit depois da abertura não muda o que a view lê; ``snapshot`` e
    ``channel`` juntos e o canal desconhecido são ``ContractError``."""
    with db.open_delta(channel=delta.CURRENT_CHANNEL) as reader:
        assert reader.snapshot is None
        assert reader.versions == {ACCOUNTS.name: 1, ENTRIES.name: 3}
        add_entries(db, THREE_MONTHS[0], 100, 5)   # a versão 4
        assert count(reader, ENTRIES) == 30
    with db.open_delta(channel=delta.CURRENT_CHANNEL) as reader:
        assert reader.versions[ENTRIES.name] == 4
        assert count(reader, ENTRIES) == 25
    with pytest.raises(ContractError, match="não os dois"):
        db.open_delta(snapshot="2026T3", channel=delta.CURRENT_CHANNEL)
    with pytest.raises(ContractError, match="o canal nada não existe"):
        db.open_delta(channel="nada")


@pytest.mark.local
def test_table_created_after_the_snapshot_has_no_view(db: Database) -> None:
    """A tabela do modelo fora do snapshot não tem view: o statement Core que a cita, também num
    join, é ``ContractError`` com a origem, em ``query``, em ``stream`` e em ``materialize``; o
    texto pronto recebe o erro de catálogo do DuckDB; o canal ``current`` a vê."""
    delta.create_table(db.uri(PROJECTED), PROJECTED, db.storage)
    delta.publish_partition(db.uri(PROJECTED), PROJECTED, THREE_MONTHS[2],
                            entry_rows(THREE_MONTHS[2], 1, 10, PROJECTED), METADATA, db.storage)
    joined = sa.select(ENTRIES.c.id_lancamento).join(
        PROJECTED, ENTRIES.c.id_lancamento == PROJECTED.c.id_lancamento)
    with db.open_delta(snapshot="2026T3") as reader:
        assert PROJECTED.name not in reader.versions
        assert objects(reader, "views") == [ACCOUNTS.name, ENTRIES.name]
        with pytest.raises(ContractError, match="snapshot 2026T3") as failure:
            reader.query(sa.select(PROJECTED))
        assert str(failure.value).startswith(PROJECTED.name)
        with pytest.raises(ContractError, match=PROJECTED.name):
            reader.query(joined)
        with pytest.raises(ContractError, match=PROJECTED.name):
            reader.stream(sa.select(PROJECTED))
        with pytest.raises(ContractError, match=PROJECTED.name):
            reader.materialize(PROJECTED)
        with pytest.raises(duckdb.CatalogException):
            reader.query(f'SELECT count(*) FROM "{PROJECTED.name}"')
    with db.open_delta(channel=delta.CURRENT_CHANNEL) as reader:
        assert reader.versions[PROJECTED.name] == 1
        assert count(reader, PROJECTED) == 10


@pytest.mark.local
def test_materialize_swaps_the_view_for_a_table(db: Database) -> None:
    """``materialize`` troca a view da tabela por uma tabela do banco local, inteira ou só das
    partições pedidas, as tabelas em paralelo, e troca de novo na chamada seguinte;
    ``partitions`` numa tabela sem partição e o valor fora da regra são ``ContractError`` antes
    de qualquer troca."""
    with db.open_delta(snapshot="2026T3") as reader:
        reader.materialize(ACCOUNTS)
        assert reader.materialized == {ACCOUNTS.name: None}
        assert objects(reader, "tables") == [ACCOUNTS.name]
        assert objects(reader, "views") == [ENTRIES.name]
        assert count(reader, ACCOUNTS) == 3

        wanted = [THREE_MONTHS[2], THREE_MONTHS[1]]
        reader.materialize(ENTRIES, partitions=wanted)
        assert reader.materialized == {ACCOUNTS.name: None, ENTRIES.name: wanted}
        assert objects(reader, "tables") == [ACCOUNTS.name, ENTRIES.name]
        assert objects(reader, "views") == []
        months = sa.select(ENTRIES.c.data_base_str).distinct().order_by(ENTRIES.c.data_base_str)
        assert reader.query(months).column(0).to_pylist() == sorted(wanted)
        assert count(reader, ENTRIES) == 20

        # A chamada seguinte troca as duas tabelas de novo, inteiras.
        reader.materialize(ACCOUNTS, ENTRIES)
        assert reader.materialized == {ACCOUNTS.name: None, ENTRIES.name: None}
        assert count(reader, ENTRIES) == 30
        assert count(reader, ACCOUNTS) == 3

        # As recusas, antes de qualquer troca: os lançamentos continuam inteiros.
        with pytest.raises(ContractError, match="sem partição"):
            reader.materialize(ENTRIES, ACCOUNTS, partitions=[THREE_MONTHS[0]])
        with pytest.raises(ContractError, match="regra da partição"):
            reader.materialize(ENTRIES, partitions=["2026/06/30"])
        assert reader.materialized == {ACCOUNTS.name: None, ENTRIES.name: None}
        assert count(reader, ENTRIES) == 30


@pytest.mark.local
def test_failed_materialization_keeps_the_view(db: Database) -> None:
    """A cópia que falha, com um arquivo da tabela apagado, sobe como ``duckdb.Error``, não entra
    em ``materialized``, e a view continua a responder pelas partições que ainda têm arquivo."""
    with db.open_delta(snapshot="2026T3") as reader:
        for file in Path(db.uri(ENTRIES)).glob(f"data_base_str={THREE_MONTHS[0]}/*.parquet"):
            file.unlink()
        with pytest.raises(duckdb.Error):
            reader.materialize(ENTRIES)
        assert reader.materialized == {}
        assert objects(reader, "views") == [ACCOUNTS.name, ENTRIES.name]
        assert objects(reader, "tables") == []
        assert count(reader, ENTRIES, THREE_MONTHS[2]) == 10


@pytest.mark.local
def test_query_stream_and_pandas_types(db: Database) -> None:
    """``query`` devolve as colunas do modelo nos tipos Arrow do contrato; ``stream`` entrega os
    lotes de até ``batch_size`` linhas, com as mesmas linhas; ``to_pandas`` com ``pd.ArrowDtype``
    mantém o decimal e a data; o texto pronto recebe os parâmetros por nome."""
    with db.open_delta(snapshot="2026T3") as reader:
        statement = sa.select(ENTRIES).order_by(ENTRIES.c.id_lancamento)
        table = reader.query(statement)
        assert table.num_rows == 30
        contract = schema.arrow_schema(ENTRIES)
        assert [field.type for field in table.schema] == [field.type for field in contract]
        with reader.stream(statement, batch_size=7) as batches:
            collected = list(batches)
        assert max(batch.num_rows for batch in collected) <= 7
        assert sum(batch.num_rows for batch in collected) == 30
        assert pa.Table.from_batches(collected).to_pylist() == table.to_pylist()

        frame = table.to_pandas(types_mapper=pd.ArrowDtype)
        assert str(frame["preco"].dtype) == "decimal128(18, 2)[pyarrow]"
        assert str(frame["data_base"].dtype) == "date32[day][pyarrow]"
        assert frame["id_lancamento"].tolist() == list(range(1, 31))

        text = 'SELECT count(*) AS n FROM "cad_lancamentos" WHERE "data_base_str" = :mes'
        counted = reader.query(text, {"mes": THREE_MONTHS[1]})
        assert counted.column("n")[0].as_py() == 10


@pytest.mark.local
def test_reader_runs_only_queries_and_session_takes_commands(db: Database) -> None:
    """Um statement Core que não é ``Select`` nem ``CompoundSelect`` é ``ContractError`` em
    ``query`` e em ``stream``, sem tocar as views; ``union_all`` passa; a conexão de ``session()``
    recebe um comando, como a tabela temporária que a consulta seguinte lê."""
    with db.open_delta(snapshot="2026T3") as reader:
        commands = [sa.delete(ENTRIES), sa.update(ENTRIES).values(area="x"),
                    sa.insert(ACCOUNTS).values(id_conta=9, numero="Z")]
        for command in commands:
            with pytest.raises(ContractError, match="Select e CompoundSelect"):
                reader.query(command)
            with pytest.raises(ContractError, match=type(command).__name__):
                reader.stream(command)
        assert count(reader, ENTRIES) == 30
        assert count(reader, ACCOUNTS) == 3
        union = sa.union_all(sa.select(ACCOUNTS.c.id_conta), sa.select(ACCOUNTS.c.id_conta))
        assert reader.query(union).num_rows == 6
        with reader.session() as connection:
            connection.execute("CREATE TEMP TABLE ids AS SELECT range AS id FROM range(10)")
        assert reader.query("SELECT count(*) AS n FROM ids").column("n")[0].as_py() == 10


@pytest.mark.local
@pytest.mark.parametrize(("name", "wanted", "opened_months"), [
    ("igualdade", THREE_MONTHS[1:2], THREE_MONTHS[1:2]),
    ("intervalo", THREE_MONTHS[1:3], THREE_MONTHS[1:3]),
    ("salteadas", [THREE_MONTHS[0], THREE_MONTHS[2]], THREE_MONTHS),
], ids=["igualdade", "intervalo", "salteadas"])
def test_delta_scan_prunes_by_equality_and_range(db: Database, name: str, wanted: list[str],
                                                 opened_months: list[str]) -> None:
    """O ``=`` abre só a pasta da partição e o ``BETWEEN`` só as do intervalo, enquanto o ``IN``
    de dois valores salteados abre todas, pelo log ``FileSystem`` do DuckDB; as linhas são as
    pedidas."""
    column = ENTRIES.c.data_base_str
    if name == "igualdade":
        predicate = column == wanted[0]
    elif name == "intervalo":
        predicate = column.between(wanted[0], wanted[-1])
    else:
        predicate = column.in_(wanted)
    statement = sa.select(ENTRIES.c.id_lancamento, ENTRIES.c.valor).where(predicate)
    with db.open_delta(snapshot="2026T3") as reader:
        with reader.session() as connection:
            connection.execute("CALL enable_logging('FileSystem')")
            connection.execute("CALL truncate_duckdb_logs()")
        assert reader.query(statement).num_rows == 10 * len(wanted)
        with reader.session() as connection:
            opened = opened_partition_folders(connection, "data_base_str")
            connection.execute("CALL disable_logging()")
    assert opened == {f"data_base_str={month}" for month in opened_months}


@pytest.mark.local
def test_close_and_the_finalizer_remove_the_engine_folder(db: Database, folder: Path,
                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    """``close`` chama o ``cleanup`` do motor uma vez, apaga a pasta que ``DuckDBConfig()`` criou
    e não faz nada na segunda chamada nem na coleta; o leitor sem ``close`` tem a pasta apagada
    quando é coletado."""
    calls = []
    original_cleanup = DuckDBEngine.cleanup

    def counting_cleanup(engine: DuckDBEngine) -> None:
        calls.append(engine)
        original_cleanup(engine)

    monkeypatch.setattr(DuckDBEngine, "cleanup", counting_cleanup)
    assert list(folder.glob("serialize_db_*")) == []
    reader = db.open_delta(snapshot="2026T3")
    created = list(folder.glob("serialize_db_*"))
    assert len(created) == 1
    assert count(reader, ACCOUNTS) == 3
    calls.clear()
    reader.close()
    assert len(calls) == 1
    assert not created[0].exists()
    reader.close()
    del reader
    gc.collect()
    assert len(calls) == 1

    # O leitor sem close: a coleta apaga a pasta.
    reader = db.open_delta(snapshot="2026T3")
    assert len(list(folder.glob("serialize_db_*"))) == 1
    del reader
    gc.collect()
    assert list(folder.glob("serialize_db_*")) == []


# ---------------------------------------------------------------- o leitor Redshift sem conexão


def test_redshift_reader_compiles_with_the_environment_prefix(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """O leitor Redshift compila o statement com o prefixo ``<ambiente>_`` e passa os parâmetros
    ao driver, troca o sentinela ``{prefix}`` do texto, recusa o comando e o ``stream`` sem
    ``unload_to`` antes de qualquer comando no servidor e fecha a conexão no ``close``; o
    ambiente fora da regra da partição é ``ContractError``."""
    connection = FakeConnection()
    monkeypatch.setattr(redshift, "driver_connect", lambda login: connection)
    reader = open_redshift(Base.metadata, "prd", CONFIG)
    assert reader.environment == "prd"
    assert reader.reader_id.startswith("reader-")
    statement = sa.select(ENTRIES.c.id_lancamento).where(ENTRIES.c.area == sa.bindparam("area"))
    assert reader.query(statement, {"area": "TI"}).num_rows == 0
    command = connection.commands[-1]
    assert '"prd_cad_lancamentos"' in command.text
    assert command.params == {"area": "TI"}
    reader.query('SELECT 1 FROM "{prefix}cad_contas"')
    assert connection.commands[-1].text == 'SELECT 1 FROM "prd_cad_contas"'

    before = len(connection.commands)
    with pytest.raises(ContractError, match="Delete"):
        reader.query(sa.delete(ENTRIES))
    with pytest.raises(ContractError, match="Update"):
        reader.stream(sa.update(ENTRIES).values(area="x"))
    with pytest.raises(ContractError, match="unload_to"):
        reader.stream(sa.select(ENTRIES))
    assert len(connection.commands) == before
    reader.close()
    assert connection.closed
    reader.close()
    with pytest.raises(ContractError, match="regra da partição"):
        open_redshift(Base.metadata, "prd/x", CONFIG)


def test_open_redshift_reads_the_configuration_from_the_environment(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """Sem ``config``, o leitor lê ``SERIALIZE_DB_REDSHIFT_*``: a conexão recebe o endereço das
    variáveis, e o ambiente ``dsv`` prefixa as tabelas."""
    logins = []
    connection = FakeConnection()

    def recording_connect(login: dict) -> FakeConnection:
        logins.append(login)
        return connection

    monkeypatch.setattr(redshift, "driver_connect", recording_connect)
    for name, value in (("HOST", "host"), ("USER", "usuario"), ("PASSWORD", "senha"),
                        ("SCHEMA", "esquema"), ("SHARE_DATABASE", "compartilhado")):
        monkeypatch.setenv(f"SERIALIZE_DB_REDSHIFT_{name}", value)
    monkeypatch.delenv("SERIALIZE_DB_REDSHIFT_WORKGROUP", raising=False)
    with open_redshift(Base.metadata, "dsv") as reader:
        assert isinstance(reader, RedshiftReader)
        reader.query('SELECT 1 FROM "{prefix}cad_contas"')
        assert connection.commands[-1].text == 'SELECT 1 FROM "dsv_cad_contas"'
    assert logins[0]["host"] == "host"
    assert logins[0]["user"] == "usuario"


@pytest.mark.local
def test_database_open_redshift_unloads_under_staging(db: Database,
                                                      monkeypatch: pytest.MonkeyPatch) -> None:
    """``db.open_redshift`` abre o leitor com o modelo do banco e o ``UNLOAD`` de ``stream`` em
    ``<raiz>/<ambiente>/staging/<id do leitor>/``, que o ``close`` esvazia."""
    rows = account_rows(["A", "B"])
    connection = FakeConnection(db.storage, unload_rows=rows)
    monkeypatch.setattr(redshift, "driver_connect", lambda login: connection)
    with db.open_redshift(CONFIG) as reader:
        with reader.stream(sa.select(ACCOUNTS)) as batches:
            assert batches.read_all().num_rows == 2
        reader_prefix = db.storage.join("prd", "staging", reader.reader_id)
        unload = [command.text for command in connection.commands
                  if command.text.startswith("UNLOAD")]
        assert len(unload) == 1
        assert f"TO '{db.storage.uri_of(reader_prefix)}/stream/" in unload[0]
        assert '"prd_cad_contas"' in unload[0]
    assert db.storage.list_files(reader_prefix) == []
    assert connection.closed


# ---------------------------------------------------------------- os dois leitores no alvo


@pytest.mark.redshift
@pytest.mark.s3
@pytest.mark.local
def test_redshift_reader_matches_the_delta_reader(target: Target,
                                                  s3_location: S3Location) -> None:
    """A tabela publicada lida pelo leitor Redshift, por ``query`` e por ``stream``, dá as mesmas
    linhas que o leitor Delta na versão atual; o ``close`` apaga os arquivos do ``UNLOAD`` do
    leitor, sob ``unload_to`` e sob o ``staging/`` do banco."""
    db = target.db
    export_with_duckdb(target, ENTRIES, MONTHS)
    publication.publish_redshift(db, target.config, [ENTRIES], "exec-1")
    statement = (
        sa.select(ENTRIES.c.id_lancamento, ENTRIES.c.preco, ENTRIES.c.carimbo, ENTRIES.c.codigo,
                  ENTRIES.c.data_base_str)
        .where(ENTRIES.c.data_base_str == MONTHS[1])
        .order_by(ENTRIES.c.id_lancamento)
    )
    with db.open_delta(channel=delta.CURRENT_CHANNEL) as reader:
        expected = reader.query(statement).to_pylist()
    assert len(expected) == 40

    unload_to = s3_location.child(f"leitor/{uuid.uuid4().hex[:8]}")
    with open_redshift(Base.metadata, target.environment, target.config, unload_to) as reader:
        queried = reader.query(statement).to_pylist()
        with reader.stream(statement, batch_size=25) as batches:
            streamed = batches.read_all().to_pylist()
        reader_id = reader.reader_id
    assert queried == expected
    assert streamed == expected
    assert Storage.for_uri(unload_to).list_files(reader_id) == []

    with db.open_redshift(target.config) as reader:
        with reader.stream(statement, batch_size=25) as batches:
            assert batches.read_all().to_pylist() == expected
        reader_id = reader.reader_id
    assert db.storage.list_files(db.storage.join(target.environment, "staging", reader_id)) == []
    record("reader.redshift_matches_delta_rows", len(expected))
