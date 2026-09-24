"""``serialize_db.publication``: a publicação para clientes no Redshift, sem conexão e sobre uma
amostra no esquema.

Os casos sem conexão conferem a tabela de controle exigida, o texto da transação e da
despublicação, a conferência da versão lida (igual, mais nova, o ``UPDATE`` sem linha, o ``1023``
e a tabela publicada que outra primeira publicação criou), a reconciliação pelas colunas de
``svv_all_columns`` e o estado, sobre uma conexão de mentira que registra os comandos e responde
a linha de controle (os que gravam um Delta na pasta local são ``local``). Os casos marcados
``redshift``, ``s3`` e ``local`` publicam no esquema de ``SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`` a
partir de um Delta sob ``SERIALIZE_DB_TEST_S3_ROOT``, exportado pelo motor DuckDB com o
``temp_directory`` sob ``SERIALIZE_DB_TEST_LOCAL_ROOT``, num ambiente ``poc<id>`` próprio, cujas
tabelas publicadas e linhas de controle saem no fim; a tabela de controle é criada quando não
existe e apagada só nesse caso. No substituto local
(``SERIALIZE_DB_TEST_EMULATOR``), a conexão é a de ``tests/emulator.py``, e o ``1023`` da
publicação simultânea vem do conflito entre duas transações do DuckDB. O modelo é o de
``tests/lancamentos_model.py``.
"""

from __future__ import annotations

import dataclasses
import json
import re
import tempfile
import threading
import uuid
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pytest
import redshift_connector
import sqlalchemy as sa

from conftest import LocalLocation, S3Location, record, redshift_config
from lancamentos_model import ACCOUNTS, ENTRIES, MONTHS, PROJECTED, Base, accounts, entries
from serialize_db import cli, delta, publication
from serialize_db.engine import redshift
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.engine.redshift import RedshiftConfig, mask
from serialize_db.errors import ExecutionConflict, PublicationError
from serialize_db.execution import Database, Execution
from serialize_db.publication import PublishedColumn
from serialize_db.storage import Storage

SCHEMA = "esquema"
CONFIG = RedshiftConfig(host="host", user="usuario", password="senha", database="dev",
                        share_database="compartilhado", schema=SCHEMA, iam_role="default")
METADATA = delta.commit_metadata("exec-0", {})
CONTROL = f'"{SCHEMA}"."serialize_db_publications"'


def server_error(message: str, code: str = "XX000") -> redshift_connector.ProgrammingError:
    """O erro do driver com os campos do servidor."""
    return redshift_connector.ProgrammingError({"S": "ERROR", "C": code, "M": message})


class FakeCursor:
    """O cursor de uma ``FakeConnection``."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.rowcount = -1
        self._rows: list = []

    def execute(self, text: str, params: object = None) -> FakeCursor:
        self._rows, self.rowcount = self.connection.answer(text)
        return self

    def fetchall(self) -> list:
        return list(self._rows)


class FakeConnection:
    """A conexão de mentira da publicação: registra os comandos, responde a tabela de controle,
    a linha de controle de cada tabela, as colunas de ``svv_all_columns`` e o ``rowcount`` do
    ``UPDATE``, e falha no comando que casa com ``fail``."""

    def __init__(self, control_table: bool = True, rows: dict[str, int] | None = None,
                 columns: list[tuple] | None = None, update_rowcount: int = 1,
                 fail: tuple[str, Exception] | None = None) -> None:
        self.control_table = control_table
        self.rows = dict(rows or {})
        self.columns = columns or []
        self.update_rowcount = update_rowcount
        self.fail = fail
        self.commands: list[str] = []
        self.autocommit = False
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def close(self) -> None:
        self.closed = True

    def answer(self, text: str) -> tuple[list, int]:
        self.commands.append(mask(text))
        if self.fail is not None and re.search(self.fail[0], text):
            raise self.fail[1]
        if text == f"SELECT 1 FROM {CONTROL} LIMIT 0":
            if self.control_table:
                return [], -1
            raise server_error('relation "serialize_db_publications" does not exist', "42P01")
        if match := re.fullmatch(rf"SELECT delta_version FROM {re.escape(CONTROL)} "
                                 r"WHERE table_name = '(\w+)'", text):
            name = match.group(1)
            return ([[self.rows[name]]] if name in self.rows else []), -1
        if match := re.fullmatch(rf'SELECT 1 FROM "{SCHEMA}"\."(\w+)" LIMIT 0', text):
            if match.group(1) in self.rows:
                return [], -1
            raise server_error(f'relation "{match.group(1)}" does not exist', "42P01")
        if text.startswith("SELECT column_name, data_type"):
            return list(self.columns), -1
        if text.startswith("UPDATE") or text.startswith("DELETE FROM " + CONTROL):
            return [], self.update_rowcount
        return [], -1

    def texts(self, first_word: str | None = None) -> list[str]:
        """Os comandos registrados, ou só os que começam por ``first_word``."""
        if first_word is None:
            return list(self.commands)
        return [text for text in self.commands if text.startswith(first_word)]


def use_fake(monkeypatch: pytest.MonkeyPatch, connection: FakeConnection) -> None:
    """A conexão de mentira no lugar do driver, para toda conexão que a publicação abrir."""
    monkeypatch.setattr(redshift, "driver_connect", lambda login: connection)


def local_db(local_location: LocalLocation, environment: str = "prod") -> Database:
    """Um banco numa pasta nova, com o modelo das suítes."""
    root = local_location.child(f"publicacao/{uuid.uuid4().hex[:8]}")
    return Database(root, environment, Base.metadata)


def published_entries(db: Database, months: list[str], rows: int = 30,
                      table: sa.Table = ENTRIES) -> int:
    """A tabela Delta com ``rows`` linhas por mês; devolve a última versão."""
    uri = db.uri(table)
    delta.create_table(uri, table, db.storage)
    version = 0
    for index, month in enumerate(months):
        data = entries(month, 1 + index * rows, rows, table)
        version = delta.publish_partition(uri, table, month, data, METADATA, db.storage)
    return version


# ---------------------------------------------------------------- sem conexão


@pytest.mark.local
def test_publish_requires_the_control_table(monkeypatch: pytest.MonkeyPatch,
                                            local_location: LocalLocation) -> None:
    """Sem a tabela de controle, ``publish_redshift``, ``unpublish_redshift`` e
    ``publication_status`` levantam ``PublicationError`` com o comando de inicialização e não
    rodam outro comando; ``control_ddl`` sem ``IF NOT EXISTS``; a tabela fora do Delta e a
    execução sem configuração também são ``PublicationError``."""
    db = local_db(local_location)
    published_entries(db, MONTHS)
    connection = FakeConnection(control_table=False)
    use_fake(monkeypatch, connection)
    for action in (lambda: publication.publish_redshift(db, CONFIG, [ENTRIES], "exec-1"),
                   lambda: publication.unpublish_redshift(db, CONFIG, [ENTRIES]),
                   lambda: publication.publication_status(db, CONFIG)):
        with pytest.raises(PublicationError, match="publish --init"):
            action()
    commands = [text for text in connection.texts()
                if not text.startswith(("USE ", "SET search_path"))]
    assert commands == [f"SELECT 1 FROM {CONTROL} LIMIT 0"] * 3
    assert connection.closed

    assert publication.control_ddl(SCHEMA) == (
        f"CREATE TABLE {CONTROL} (table_name VARCHAR(127), delta_version BIGINT, "
        "execution_id VARCHAR(127), published_at TIMESTAMP)")
    assert "IF NOT EXISTS" not in publication.control_ddl(SCHEMA)

    connection = FakeConnection()
    use_fake(monkeypatch, connection)
    with pytest.raises(PublicationError, match="não existe no ambiente"):
        publication.publish_redshift(db, CONFIG, [PROJECTED], "exec-1")
    publication.create_publications_table(CONFIG)
    assert connection.texts("CREATE") == [publication.control_ddl(SCHEMA)]

    with Execution(db, DuckDBEngine(DuckDBConfig(temp_directory=str(
            Path(db.root).parent / "sandbox")), "exec-2", db.storage), MONTHS[0]) as run:
        with pytest.raises(PublicationError, match="redshift=RedshiftConfig"):
            run.publish_redshift(ENTRIES)


def test_publication_statements_text() -> None:
    """Os comandos da primeira publicação e de uma seguinte, um por item, com a linha de controle
    por último, sem ``BEGIN``, ``COMMIT``, ``TRUNCATE`` nem ``COMPUPDATE``, nomes em duas partes e
    credenciais mascaradas; ``control_read``, ``published_ddl`` e ``unpublication_statements``."""
    manifests = {MONTHS[1]: "s3://b/prod/publicacao/exec-1/cad_lancamentos/2026-08-31.manifest"}
    credentials = "ACCESS_KEY_ID 'AKIA' SECRET_ACCESS_KEY 'segredo' SESSION_TOKEN 'token'"
    first = publication.publication_statements(SCHEMA, "prod", ENTRIES, [MONTHS[1]], manifests,
                                               58, None, "exec-1", credentials)
    published = f'"{SCHEMA}"."prod_cad_lancamentos"'
    staging = '"prod_cad_lancamentos_staging"'
    assert first[0] == publication.published_ddl(SCHEMA, "prod", ENTRIES)
    assert first[0].startswith(f'CREATE TABLE {published} (\n    "id_lancamento" BIGINT NOT NULL,')
    assert '    PRIMARY KEY ("id_lancamento")\n) SORTKEY ("data_base", "id_lancamento")' in first[0]
    assert first[1].startswith(f"CREATE TEMP TABLE {staging} (")
    assert "data_base_str" not in first[1] and '"meta" VARCHAR(65535)' in first[1]
    assert first[2] == f"DELETE FROM {published} WHERE \"data_base_str\" = '{MONTHS[1]}'"
    assert first[3] == f"DELETE FROM {staging}"
    assert first[4] == (f"COPY {staging}\nFROM '{manifests[MONTHS[1]]}'\n{credentials}\n"
                        "FORMAT AS PARQUET MANIFEST FILLRECORD")
    assert first[5].startswith(f"INSERT INTO {published} (")
    assert f"JSON_PARSE(\"meta\"), \"to\", \"codigo\", '{MONTHS[1]}' FROM {staging}" in first[5]
    assert first[6] == f"DROP TABLE {staging}"
    assert first[7] == (f"INSERT INTO {CONTROL} VALUES ('prod_cad_lancamentos', 58, 'exec-1', "
                        "getdate())")
    assert len(first) == 8
    joined = "\n".join(mask(text) for text in first)
    assert "segredo" not in joined and "'***'" in joined
    for forbidden in ("BEGIN", "COMMIT", "TRUNCATE", "COMPUPDATE"):
        assert forbidden not in joined

    # A publicação seguinte: sem o CREATE TABLE, a partição removida só com o DELETE, e o UPDATE
    # condicionado à versão lida.
    following = publication.publication_statements(SCHEMA, "prod", ENTRIES,
                                                   [MONTHS[0], MONTHS[1]], manifests, 58, 57,
                                                   "exec-2", credentials)
    assert following[0].startswith("CREATE TEMP TABLE")
    assert following[1] == f"DELETE FROM {published} WHERE \"data_base_str\" = '{MONTHS[0]}'"
    assert following[2] == f"DELETE FROM {published} WHERE \"data_base_str\" = '{MONTHS[1]}'"
    assert following[-1] == (f"UPDATE {CONTROL} SET delta_version = 58, execution_id = 'exec-2', "
                             "published_at = getdate() WHERE table_name = 'prod_cad_lancamentos' "
                             "AND delta_version = 57")
    assert len(following) == 8

    # A tabela sem partição: o DELETE inteiro e o INSERT sem literal.
    whole = publication.publication_statements(SCHEMA, "dev", ACCOUNTS, [None],
                                               {None: "s3://b/m"}, 3, None, "exec-3",
                                               "IAM_ROLE default")
    assert whole[2] == f'DELETE FROM "{SCHEMA}"."dev_cad_contas"'
    assert whole[5].endswith('SELECT "id_conta", "numero" FROM "dev_cad_contas_staging"')

    assert publication.control_read(SCHEMA, "prod", ENTRIES) == (
        f"SELECT delta_version FROM {CONTROL} WHERE table_name = 'prod_cad_lancamentos'")
    assert publication.unpublication_statements(SCHEMA, "prod", ENTRIES, 58) == [
        f"DROP TABLE {published}",
        f"DELETE FROM {CONTROL} WHERE table_name = 'prod_cad_lancamentos' AND delta_version = 58",
    ]


@pytest.mark.local
def test_publish_checks_the_version_read(monkeypatch: pytest.MonkeyPatch,
                                         local_location: LocalLocation) -> None:
    """A versão lida igual à do Delta encerra a transação por ``ROLLBACK`` sem outro comando; a
    lida acima dela é ``ExecutionConflict``; o ``rowcount`` 0 do ``UPDATE``, o ``1023`` e a tabela
    publicada que outra primeira publicação criou são ``ExecutionConflict`` com ``ROLLBACK``, e
    nenhum comando se repete; a lida abaixo publica só as partições de ``version_diff``."""
    db = local_db(local_location)
    published_entries(db, MONTHS)   # a versão 2

    connection = FakeConnection(rows={"prod_cad_lancamentos": 2})
    use_fake(monkeypatch, connection)
    assert publication.publish_redshift(db, CONFIG, [ENTRIES], "exec-1") == {ENTRIES.name: 2}
    assert connection.texts()[-3:] == [
        "BEGIN", publication.control_read(SCHEMA, "prod", ENTRIES), "ROLLBACK"]

    connection = FakeConnection(rows={"prod_cad_lancamentos": 3})
    use_fake(monkeypatch, connection)
    with pytest.raises(ExecutionConflict, match="mais nova"):
        publication.publish_redshift(db, CONFIG, [ENTRIES], "exec-1")
    assert connection.texts()[-1] == "ROLLBACK"

    # A versão lida abaixo: só a partição alterada, e o UPDATE por último.
    uri = db.uri(ENTRIES)
    delta.publish_partition(uri, ENTRIES, MONTHS[1], entries(MONTHS[1], 100, 5), METADATA,
                            db.storage)   # a versão 3
    connection = FakeConnection(rows={"prod_cad_lancamentos": 2})
    use_fake(monkeypatch, connection)
    assert publication.publish_redshift(db, CONFIG, [ENTRIES], "exec-1") == {ENTRIES.name: 3}
    texts = connection.texts()
    assert texts[-1] == "COMMIT"
    assert texts[-2].startswith(f"UPDATE {CONTROL} SET delta_version = 3")
    assert texts[-2].endswith("AND delta_version = 2")
    deletes = [text for text in texts if text.startswith(f'DELETE FROM "{SCHEMA}"')]
    assert deletes == [f'DELETE FROM "{SCHEMA}"."prod_cad_lancamentos" '
                       f"WHERE \"data_base_str\" = '{MONTHS[1]}'"]
    assert len(connection.texts("COPY")) == 1
    manifest_uri = re.search(r"FROM '([^']+)'", connection.texts("COPY")[0]).group(1)
    assert manifest_uri == db.storage.uri_of(
        f"prod/publicacao/exec-1/cad_lancamentos/{MONTHS[1]}.manifest")
    manifest = json.loads(db.storage.read_text(db.storage.relative(manifest_uri))[0])
    assert len(manifest["entries"]) == 1

    # O UPDATE sem linha, o 1023 e a tabela que outra primeira publicação criou.
    for label, connection in (
        ("mudou desde a leitura", FakeConnection(rows={"prod_cad_lancamentos": 2},
                                                 update_rowcount=0)),
        ("1023", FakeConnection(rows={"prod_cad_lancamentos": 2}, fail=(
            r"^DELETE FROM \"esquema\"", server_error(
                "1023 DETAIL: Serializable isolation violation on table - 12345")))),
        ("outra primeira publicação", FakeConnection(fail=(
            r"^CREATE TABLE \"esquema\"", server_error(
                'Relation "prod_cad_lancamentos" already exists', "42P07")))),
    ):
        use_fake(monkeypatch, connection)
        with pytest.raises(ExecutionConflict, match=label):
            publication.publish_redshift(db, CONFIG, [ENTRIES], "exec-1")
        assert connection.texts()[-1] == "ROLLBACK", label
        assert connection.texts().count("BEGIN") == 1, label

    # Outro erro do servidor sobe como veio, com o comando mascarado na nota.
    connection = FakeConnection(fail=(r"^COPY", server_error("Spectrum Scan Error")))
    use_fake(monkeypatch, connection)
    with pytest.raises(redshift_connector.ProgrammingError) as failure:
        publication.publish_redshift(db, CONFIG, [ENTRIES], "exec-1")
    assert failure.value.__notes__[0].startswith("comando: COPY")
    assert connection.texts()[-1] == "ROLLBACK"


def test_reconcile_published_add_column_and_recreate() -> None:
    """A tabela igual ao modelo não tem diff; a coluna anulável nova sai por ``ADD COLUMN`` no
    fim; a coluna removida, a ``NOT NULL`` nova, o tipo, a largura de ``VARCHAR(n)`` e a escala
    que mudaram são destrutivos, nas duas grafias de ``svv_all_columns``."""
    columns = [
        PublishedColumn("id_lancamento", "bigint", None, None, None),
        PublishedColumn("id_conta", "bigint", None, None, None),
        PublishedColumn("data_base", "date", None, None, None),
        PublishedColumn("carimbo", "timestamp without time zone", None, None, None),
        PublishedColumn("valor", "double precision", None, None, None),
        PublishedColumn("preco", "numeric", None, 18, 2),
        PublishedColumn("area", "character varying", 10, None, None),
        PublishedColumn("meta", "super", None, None, None),
        PublishedColumn("to", "character varying", 2, None, None),
        PublishedColumn("codigo", "character varying", 20, None, None),
        PublishedColumn("data_base_str", "character varying", 10, None, None),
    ]
    assert publication.reconcile_published(SCHEMA, "prod", ENTRIES, columns) == ([], [])
    spelled = [dataclasses.replace(columns[3], data_type="timestamp"),
               dataclasses.replace(columns[6], data_type="varchar"),
               dataclasses.replace(columns[5], data_type="decimal")]
    respelled = spelled + columns[:3] + [columns[4]] + columns[7:]
    assert publication.reconcile_published(SCHEMA, "prod", ENTRIES, respelled) == ([], [])

    statements, destructive = publication.reconcile_published(SCHEMA, "prod", ENTRIES,
                                                              columns[:-2])
    assert destructive == ["codigo: coluna NOT NULL nova", "data_base_str: coluna NOT NULL nova"]
    assert statements == []
    without_area = [column for column in columns if column.name != "area"]
    statements, destructive = publication.reconcile_published(SCHEMA, "prod", ENTRIES,
                                                              without_area)
    assert statements == [f'ALTER TABLE "{SCHEMA}"."prod_cad_lancamentos" ADD COLUMN '
                          '"area" VARCHAR(10)']
    assert destructive == []

    changed = list(columns)
    changed[6] = dataclasses.replace(columns[6], length=8)
    changed[5] = dataclasses.replace(columns[5], scale=4)
    changed[4] = dataclasses.replace(columns[4], data_type="real")
    changed.append(PublishedColumn("extra", "integer", None, None, None))
    _, destructive = publication.reconcile_published(SCHEMA, "prod", ENTRIES, changed)
    assert [line.split(":")[0] for line in destructive] == ["valor", "preco", "area", "extra"]
    assert destructive[-1] == "extra: removida do modelo"


@pytest.mark.local
def test_publication_status_lists_pending_partitions(monkeypatch: pytest.MonkeyPatch,
                                                     local_location: LocalLocation) -> None:
    """A versão publicada, a atual e as partições pendentes por tabela: todas na nunca publicada,
    as alteradas na publicada, nenhuma na publicada na versão atual; a tabela fora do Delta fica
    de fora."""
    db = local_db(local_location)
    published_entries(db, MONTHS)
    accounts_uri = db.uri(ACCOUNTS)
    delta.create_table(accounts_uri, ACCOUNTS, db.storage)
    delta.publish_partition(accounts_uri, ACCOUNTS, None, accounts(["A", "B"]), METADATA,
                            db.storage)
    delta.publish_partition(db.uri(ENTRIES), ENTRIES, MONTHS[1], entries(MONTHS[1], 100, 5),
                            METADATA, db.storage)
    connection = FakeConnection(rows={"prod_cad_lancamentos": 2, "prod_cad_contas": 1})
    use_fake(monkeypatch, connection)
    statuses = publication.publication_status(db, CONFIG)
    assert statuses == [
        publication.PublicationStatus("prod_cad_contas", 1, 1, ()),
        publication.PublicationStatus("prod_cad_lancamentos", 2, 3, (MONTHS[1],)),
    ]
    connection = FakeConnection()
    use_fake(monkeypatch, connection)
    statuses = publication.publication_status(db, CONFIG)
    assert statuses == [
        publication.PublicationStatus("prod_cad_contas", None, 1, (None,)),
        publication.PublicationStatus("prod_cad_lancamentos", None, 3, tuple(MONTHS)),
    ]


# ---------------------------------------------------------------- no esquema autorizado


@dataclasses.dataclass
class Target:
    """O banco numa raiz nova do bucket, a configuração e o ambiente ``poc<id>`` da publicação."""

    db: Database
    config: RedshiftConfig
    folder: Path

    @property
    def environment(self) -> str:
        return self.db.environment

    def published(self, table: sa.Table) -> str:
        return f'"{self.config.schema}"."{self.environment}_{table.name}"'

    def control(self) -> str:
        return f'"{self.config.schema}"."{publication.CONTROL_TABLE}"'


def rows_of(config: RedshiftConfig, text: str) -> list[tuple]:
    """As linhas de uma consulta numa conexão própria."""
    connection = redshift.connect(config)
    try:
        cursor = connection.cursor()
        cursor.execute(text)
        return [tuple(row) for row in cursor.fetchall()] if cursor.description else []
    finally:
        connection.close()


def relation_exists(config: RedshiftConfig, qualified: str) -> bool:
    """Se a tabela existe, por ``select 1 ... limit 0``."""
    try:
        rows_of(config, f"SELECT 1 FROM {qualified} LIMIT 0")
    except redshift_connector.Error as error:
        if redshift.relation_missing(error):
            return False
        raise
    return True


@pytest.fixture
def target(s3_location: S3Location, local_location: LocalLocation,
           redshift_driver: None) -> Iterator[Target]:
    """O banco do teste, o ambiente ``poc<id>`` e a tabela de controle, criada quando não existe
    e apagada só nesse caso; as tabelas publicadas e as linhas de controle do ambiente saem no
    fim."""
    config = redshift_config()
    environment = f"poc{uuid.uuid4().hex[:8]}"
    db = Database(s3_location.child(f"publicacao/{environment}"), environment, Base.metadata)
    folder = Path(local_location.child(f"publicacao/{environment}"))
    folder.mkdir(parents=True)
    created = not relation_exists(config, f'"{config.schema}"."{publication.CONTROL_TABLE}"')
    if created:
        publication.create_publications_table(config)
    target = Target(db, config, folder)
    yield target
    connection = redshift.connect(config)
    try:
        cursor = connection.cursor()
        for table in Base.metadata.sorted_tables:
            cursor.execute(f"DROP TABLE IF EXISTS {target.published(table)}")
        if created:
            cursor.execute(f"DROP TABLE {target.control()}")
        else:
            cursor.execute(f"DELETE FROM {target.control()} "
                           f"WHERE table_name LIKE '{environment}_%'")
    finally:
        connection.close()


def export_with_duckdb(target: Target, table: sa.Table, months: list[str], rows: int = 40,
                       execution_id: str = "exec-duckdb") -> int:
    """As partições exportadas pelo motor DuckDB, pelo registro do arquivo do ``COPY``: os
    arquivos que a publicação lê; devolve a versão."""
    db = target.db
    uri = db.uri(table)
    delta.create_table(uri, table, db.storage)
    config = DuckDBConfig(threads=2, temp_directory=str(target.folder / execution_id))
    with DuckDBEngine(config, execution_id, db.storage) as engine:
        engine.load(table, entries(months[0], 1, rows, table))
        version = engine.export_partition(table, uri, months[0], METADATA, rows, ())
        for index, month in enumerate(months[1:], 1):
            data = entries(month, 1 + index * rows, rows, table)
            with engine.session() as connection:
                connection.register("lote", data)
                connection.execute(f'INSERT INTO "{table.name}" BY NAME SELECT * FROM lote')
                connection.unregister("lote")
            version = engine.export_partition(table, uri, month, METADATA, rows, ())
    return version


def published_rows(target: Target, table: sa.Table) -> dict[str, list[tuple]]:
    """As linhas da tabela publicada por partição: o id, o preço, o carimbo e o JSON."""
    rows = rows_of(target.config, (
        'SELECT "data_base_str", "id_lancamento", "preco", "carimbo", JSON_SERIALIZE("meta") '
        f'FROM {target.published(table)} ORDER BY "id_lancamento"'))
    by_partition: dict[str, list[tuple]] = {}
    for value, *rest in rows:
        by_partition.setdefault(value, []).append(tuple(rest))
    return by_partition


def control_rows(target: Target) -> dict[str, tuple[int, str]]:
    """A versão e a execução de cada linha de controle do ambiente."""
    rows = rows_of(target.config, (
        f"SELECT table_name, delta_version, execution_id FROM {target.control()} "
        f"WHERE table_name LIKE '{target.environment}_%'"))
    return {name: (int(version), execution_id) for name, version, execution_id in rows}


@pytest.mark.redshift
@pytest.mark.s3
@pytest.mark.local
def test_first_publication_loads_every_partition(target: Target) -> None:
    """Sem linha de controle, a tabela publicada criada, todas as partições e o ``INSERT`` da
    linha de controle, numa transação, sobre os arquivos que o motor DuckDB exportou pelo
    registro, com ``Numeric(18, 2)``, ``DateTime`` e a coluna JSON; a publicação repetida na
    mesma versão não muda nada."""
    db = target.db
    version = export_with_duckdb(target, PROJECTED, MONTHS)
    assert publication.publish_redshift(db, target.config, [PROJECTED], "exec-1") == {
        PROJECTED.name: version}
    rows = published_rows(target, PROJECTED)
    assert {value: len(items) for value, items in rows.items()} == {MONTHS[0]: 40, MONTHS[1]: 40}
    expected = entries(MONTHS[1], 41, 40, PROJECTED)
    first = rows[MONTHS[1]][0]
    assert first[0] == 41
    assert first[1] == expected.column("preco")[0].as_py()
    assert first[2] == expected.column("carimbo")[0].as_py()
    assert json.loads(first[3]) == {"k": 41}
    assert control_rows(target) == {f"{target.environment}_{PROJECTED.name}": (version, "exec-1")}
    record("redshift.publication.first", f"{sum(len(v) for v in rows.values())} linhas de "
                                         f"{len(rows)} partições exportadas pelo motor DuckDB")

    assert publication.publish_redshift(db, target.config, [PROJECTED], "exec-2") == {
        PROJECTED.name: version}
    assert control_rows(target)[f"{target.environment}_{PROJECTED.name}"] == (version, "exec-1")


@pytest.mark.redshift
@pytest.mark.s3
@pytest.mark.local
def test_publish_only_changed_partitions(target: Target) -> None:
    """Duas publicações: a segunda, depois de uma partição alterada, troca só essa partição, e a
    linha de controle passa à versão nova; a partição removida no Delta sai da tabela
    publicada; o estado lista as partições pendentes antes de cada publicação."""
    db = target.db
    version = export_with_duckdb(target, PROJECTED, MONTHS)
    status = publication.publication_status(db, target.config)
    assert status == [publication.PublicationStatus(
        f"{target.environment}_{PROJECTED.name}", None, version, tuple(MONTHS))]
    publication.publish_redshift(db, target.config, [PROJECTED], "exec-1")

    uri = db.uri(PROJECTED)
    changed = delta.publish_partition(uri, PROJECTED, MONTHS[1], entries(MONTHS[1], 1000, 7,
                                                                        PROJECTED),
                                      METADATA, db.storage)
    status = publication.publication_status(db, target.config)
    assert status[0].published_version == version and status[0].current_version == changed
    assert status[0].pending_partitions == (MONTHS[1],)
    assert publication.publish_redshift(db, target.config, [PROJECTED], "exec-2") == {
        PROJECTED.name: changed}
    rows = published_rows(target, PROJECTED)
    assert [row[0] for row in rows[MONTHS[0]]] == list(range(1, 41))
    assert [row[0] for row in rows[MONTHS[1]]] == list(range(1000, 1007))
    assert control_rows(target) == {f"{target.environment}_{PROJECTED.name}": (changed, "exec-2")}

    # A partição removida no Delta: só o DELETE.
    removed = delta.publish_partition(uri, PROJECTED, MONTHS[0], entries(MONTHS[0], 1, 0,
                                                                        PROJECTED),
                                      METADATA, db.storage)
    assert publication.publish_redshift(db, target.config, [PROJECTED], "exec-3") == {
        PROJECTED.name: removed}
    rows = published_rows(target, PROJECTED)
    assert list(rows) == [MONTHS[1]]
    assert publication.publication_status(db, target.config)[0].pending_partitions == ()


class PausingConnection:
    """A conexão da publicação B, que para depois da leitura da linha de controle até a
    publicação A confirmar: as duas leem a mesma versão, e a segunda a gravar conflita."""

    def __init__(self, connection: object, read_done: threading.Event,
                 resume: threading.Event) -> None:
        self.connection = connection
        self.read_done = read_done
        self.resume = resume
        self.autocommit = False

    def __setattr__(self, name: str, value: object) -> None:
        if name == "autocommit" and "connection" in self.__dict__:
            self.connection.autocommit = value
        object.__setattr__(self, name, value)

    def cursor(self) -> object:
        cursor = self.connection.cursor()
        connection = self

        class Cursor:
            def execute(self, text: str, params: object = None) -> object:
                result = cursor.execute(text, params)
                if text.startswith("SELECT delta_version FROM"):
                    connection.read_done.set()
                    connection.resume.wait(timeout=120)
                return result

            def __getattr__(self, name: str) -> object:
                return getattr(cursor, name)

        return Cursor()

    def close(self) -> None:
        self.connection.close()


@pytest.mark.redshift
@pytest.mark.s3
@pytest.mark.local
def test_concurrent_publication_raises_execution_conflict(target: Target,
                                                          monkeypatch: pytest.MonkeyPatch) -> None:
    """Duas publicações da mesma tabela a partir da mesma versão lida: a segunda levanta
    ``ExecutionConflict``, pelo ``1023`` ou pelo ``UPDATE`` sem linha; a partição e a linha de
    controle ficam as da primeira."""
    db = target.db
    export_with_duckdb(target, PROJECTED, MONTHS)
    publication.publish_redshift(db, target.config, [PROJECTED], "exec-1")
    uri = db.uri(PROJECTED)
    changed = delta.publish_partition(uri, PROJECTED, MONTHS[1], entries(MONTHS[1], 500, 3,
                                                                        PROJECTED),
                                      METADATA, db.storage)

    read_done = threading.Event()
    resume = threading.Event()
    plain_connect = redshift.driver_connect

    def connect_b(login: dict) -> object:
        # Toda conexão de B pausa na leitura da linha de controle; a da conferência da tabela de
        # controle não a lê.
        return PausingConnection(plain_connect(login), read_done, resume)

    outcome: dict[str, object] = {}

    def publish_b() -> None:
        try:
            outcome["result"] = publication.publish_redshift(db, target.config, [PROJECTED],
                                                             "exec-b")
        except BaseException as error:  # noqa: BLE001 - o desfecho de B é a leitura
            outcome["error"] = error

    monkeypatch.setattr(redshift, "driver_connect", connect_b)
    thread = threading.Thread(target=publish_b)
    thread.start()
    assert read_done.wait(timeout=120)
    monkeypatch.setattr(redshift, "driver_connect", plain_connect)
    assert publication.publish_redshift(db, target.config, [PROJECTED], "exec-a") == {
        PROJECTED.name: changed}
    resume.set()
    thread.join(timeout=180)
    assert not thread.is_alive()
    record("redshift.publication.concurrent", repr(outcome.get("error", outcome.get("result"))))
    assert isinstance(outcome.get("error"), ExecutionConflict), outcome
    rows = published_rows(target, PROJECTED)
    assert [row[0] for row in rows[MONTHS[1]]] == [500, 501, 502]
    assert control_rows(target) == {f"{target.environment}_{PROJECTED.name}": (changed, "exec-a")}


@pytest.mark.redshift
@pytest.mark.s3
@pytest.mark.local
def test_unpublish_drops_the_table_and_the_control_row(target: Target) -> None:
    """Depois de uma publicação, ``unpublish_redshift`` apaga a tabela publicada e a linha de
    controle numa transação; a segunda chamada não acha linha e devolve ``None``; a publicação
    seguinte é uma primeira publicação."""
    db = target.db
    version = export_with_duckdb(target, PROJECTED, MONTHS)
    publication.publish_redshift(db, target.config, [PROJECTED], "exec-1")
    assert publication.unpublish_redshift(db, target.config, [PROJECTED]) == {
        PROJECTED.name: version}
    assert not relation_exists(target.config, target.published(PROJECTED))
    assert control_rows(target) == {}
    assert publication.unpublish_redshift(db, target.config, [PROJECTED]) == {
        PROJECTED.name: None}
    assert publication.publish_redshift(db, target.config, [PROJECTED], "exec-2") == {
        PROJECTED.name: version}
    assert control_rows(target) == {f"{target.environment}_{PROJECTED.name}": (version, "exec-2")}
    assert len(published_rows(target, PROJECTED)[MONTHS[0]]) == 40


@pytest.mark.redshift
@pytest.mark.s3
@pytest.mark.local
def test_failed_copy_leaves_control_row_untouched(target: Target,
                                                  monkeypatch: pytest.MonkeyPatch) -> None:
    """Um manifesto inválido na segunda partição: nenhuma partição trocada, controle intacto."""
    db = target.db
    version = export_with_duckdb(target, PROJECTED, MONTHS)
    publication.publish_redshift(db, target.config, [PROJECTED], "exec-1")
    uri = db.uri(PROJECTED)
    for month in MONTHS:
        delta.publish_partition(uri, PROJECTED, month, entries(month, 700, 2, PROJECTED),
                                METADATA, db.storage)
    plain = delta.copy_manifest

    def broken_manifest(uri: str, version: int, partitions: list | None, destination: str,
                        storage: Storage) -> str:
        if partitions == [MONTHS[1]]:
            storage.write_text(storage.relative(destination), json.dumps({"entries": [
                {"url": storage.uri_of("nao/existe.parquet"), "mandatory": True,
                 "meta": {"content_length": 1}}]}))
            return destination
        return plain(uri, version, partitions, destination, storage)

    monkeypatch.setattr(delta, "copy_manifest", broken_manifest)
    with pytest.raises(Exception) as failure:  # noqa: B017 - o erro do COPY: servidor ou S3
        publication.publish_redshift(db, target.config, [PROJECTED], "exec-2")
    record("redshift.publication.failed_copy", type(failure.value).__name__)
    assert not isinstance(failure.value, ExecutionConflict)
    rows = published_rows(target, PROJECTED)
    assert [row[0] for row in rows[MONTHS[0]]] == list(range(1, 41))
    assert [row[0] for row in rows[MONTHS[1]]] == list(range(41, 81))
    assert control_rows(target) == {f"{target.environment}_{PROJECTED.name}": (version, "exec-1")}


@pytest.mark.redshift
@pytest.mark.s3
@pytest.mark.local
def test_reconcile_published_on_the_target(target: Target) -> None:
    """A tabela publicada igual ao modelo não tem diff na leitura de ``svv_all_columns``; uma
    coluna anulável nova entra por ``ADD COLUMN`` e a publicação seguinte a preenche; a largura
    de ``VARCHAR(n)`` que muda despublica e recria a tabela com todas as partições."""
    db = target.db
    export_with_duckdb(target, PROJECTED, MONTHS)
    publication.publish_redshift(db, target.config, [PROJECTED], "exec-1")
    columns = rows_of(target.config, (
        "SELECT column_name, data_type, character_maximum_length, numeric_precision, "
        f"numeric_scale FROM svv_all_columns WHERE schema_name = '{target.config.schema}' "
        f"AND table_name = '{target.environment}_{PROJECTED.name}' ORDER BY ordinal_position"))
    record("redshift.publication.svv_all_columns", [list(column) for column in columns])
    listed = [PublishedColumn(str(c[0]), str(c[1]), c[2], c[3], c[4]) for c in columns]
    assert publication.reconcile_published(target.config.schema, target.environment, PROJECTED,
                                           listed) == ([], [])

    # A coluna anulável nova: ADD COLUMN, e a partição alterada a preenche.
    metadata = sa.MetaData()
    wider = PROJECTED.to_metadata(metadata)
    wider.append_column(sa.Column("canal", sa.String(5)))
    db_wider = Database(db.root, target.environment, metadata)
    delta.reconcile(db.uri(wider), wider, db.storage)
    with_channel = entries(MONTHS[1], 900, 2, PROJECTED).append_column(
        "canal", pa.array(["web", "app"]))
    changed = delta.publish_partition(db.uri(wider), wider, MONTHS[1], with_channel, METADATA,
                                      db.storage)
    assert publication.publish_redshift(db_wider, target.config, [wider], "exec-2") == {
        wider.name: changed}
    rows = rows_of(target.config, (
        f'SELECT "id_lancamento", "canal" FROM {target.published(wider)} '
        f"WHERE \"data_base_str\" = '{MONTHS[1]}' ORDER BY 1"))
    assert rows == [(900, "web"), (901, "app")]
    assert rows_of(target.config, f'SELECT count(*) FROM {target.published(wider)}')[0][0] == 42

    # A largura de VARCHAR(n) que muda: despublicada e recriada com todas as partições.
    narrower = sa.MetaData()
    narrow = wider.to_metadata(narrower)
    narrow.c.canal.type = sa.String(3)
    db_narrow = Database(db.root, target.environment, narrower)
    delta.reconcile(db.uri(narrow), narrow, db.storage)
    assert publication.publish_redshift(db_narrow, target.config, [narrow], "exec-3") == {
        narrow.name: changed}
    assert control_rows(target) == {f"{target.environment}_{PROJECTED.name}": (changed, "exec-3")}
    assert rows_of(target.config, f'SELECT count(*) FROM {target.published(narrow)}')[0][0] == 42


@pytest.mark.redshift
@pytest.mark.s3
@pytest.mark.local
def test_published_join_redistribution_is_read(target: Target) -> None:
    """O ``EXPLAIN`` de um join típico entre as tabelas publicadas, ``cad_lancamentos`` com
    ``cad_contas`` por ``id_conta``, depois da primeira publicação: os rótulos ``DS_*`` de cada
    passo, como leitura, nunca como reprovação."""
    db = target.db
    export_with_duckdb(target, ENTRIES, MONTHS)
    accounts_uri = db.uri(ACCOUNTS)
    delta.create_table(accounts_uri, ACCOUNTS, db.storage)
    delta.publish_partition(accounts_uri, ACCOUNTS, None, accounts(["A", "B", "C"]), METADATA,
                            db.storage)
    publication.publish_redshift(db, target.config, [ENTRIES, ACCOUNTS], "exec-1", max_workers=2)
    plan = rows_of(target.config, (
        f'EXPLAIN SELECT count(*) FROM {target.published(ENTRIES)} l '
        f'JOIN {target.published(ACCOUNTS)} c ON l."id_conta" = c."id_conta"'))
    labels = sorted({label for row in plan for label in re.findall(r"DS_\w+", str(row))})
    record("redshift.publication.join_plan", labels or [str(row) for row in plan][:5])


@pytest.mark.redshift
@pytest.mark.s3
@pytest.mark.local
def test_execution_publishes_to_redshift_and_the_cli(target: Target,
                                                     monkeypatch: pytest.MonkeyPatch,
                                                     capsys: pytest.CaptureFixture) -> None:
    """``Execution(..., redshift=config)`` no motor DuckDB publica no Delta e no Redshift;
    ``serialize-db publish --status`` e ``--unpublish`` seguem pela linha de comando."""
    db = target.db
    config = DuckDBConfig(threads=2, temp_directory=str(target.folder / "execucao"))
    engine = DuckDBEngine(config, "exec-run", db.storage)
    delta.create_table(db.uri(PROJECTED), PROJECTED, db.storage)
    with Execution(db, engine, MONTHS[1], "exec-run", redshift=target.config) as run:
        run.sandbox.load(PROJECTED, entries(MONTHS[1], 1, 25, PROJECTED))
        run.audit(PROJECTED, [MONTHS[1]])
        assert run.publish(PROJECTED, partitions=[MONTHS[1]]) == {PROJECTED.name: 1}
        assert run.publish_redshift(PROJECTED) == {PROJECTED.name: 1}
    assert len(published_rows(target, PROJECTED)[MONTHS[1]]) == 25
    assert control_rows(target) == {f"{target.environment}_{PROJECTED.name}": (1, "exec-run")}

    for name, value in (("SCHEMA", target.config.schema), ("HOST", target.config.host),
                        ("USER", target.config.user), ("PASSWORD", target.config.password),
                        ("WORKGROUP", target.config.workgroup)):
        if value is None:
            monkeypatch.delenv(f"SERIALIZE_DB_REDSHIFT_{name}", raising=False)
        else:
            monkeypatch.setenv(f"SERIALIZE_DB_REDSHIFT_{name}", str(value))
    monkeypatch.setattr(tempfile, "tempdir", str(target.folder))
    common = ["publish", "--root", db.root, "--environment", target.environment,
              "--metadata", "lancamentos_model:Base.metadata"]
    assert cli.main([*common, "--status"]) == 0
    printed = capsys.readouterr().out
    assert f"{target.environment}_{PROJECTED.name}: publicada 1, atual 1, pendentes []" in printed
    assert cli.main([*common, "--tables", PROJECTED.name, "--execution-id", "exec-cli"]) == 0
    assert "versão 1" in capsys.readouterr().out
    assert cli.main([*common, "--unpublish", "--tables", PROJECTED.name]) == 0
    assert f"{PROJECTED.name}: 1" in capsys.readouterr().out
    assert control_rows(target) == {}
    assert cli.main([*common, "--tables", "nao_existe"]) == 2
    assert "não está nos modelos" in capsys.readouterr().err
