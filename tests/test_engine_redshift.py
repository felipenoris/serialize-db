"""``serialize_db.engine.redshift``: o motor Redshift, sem conexão e sobre uma amostra no esquema.

Os casos sem conexão conferem o texto de cada comando, a configuração, o prefixo, a cláusula de
credenciais e a máscara, o esquema de um resultado pelo ``row_desc``, a tabela montada por colunas,
os valores do cliente como literais e o guarda do ``bindparam`` sem valor, o ``stream`` vazio, a
sessão única, a reconexão e a troca da partição com ``Double`` não finito, sobre uma conexão de
mentira que registra os comandos, responde ao que o motor pergunta e grava o arquivo de um
``UNLOAD`` numa pasta local (os que gravam são ``local``, sob ``SERIALIZE_DB_TEST_LOCAL_ROOT``). Os
casos marcados ``redshift`` repetem a sequência com uma amostra no esquema de
``SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`` e arquivos sob ``SERIALIZE_DB_TEST_S3_ROOT``: no ambiente alvo
pela conexão de ``examples/redshift_native.py``, e no substituto local
(``SERIALIZE_DB_TEST_EMULATOR``) pela conexão de ``tests/emulator.py``, que os testes dão ao motor
no lugar do ``redshift_connector``. O modelo é o de ``Lancamento``, particionado por
``data_base_str``, com uma chave estrangeira para ``Conta``, uma coluna JSON, uma ``DateTime`` e a
coluna ``to``, palavra reservada, e ``Projetado``, a tabela que o pipeline grava
(``tests/lancamentos_model.py``).
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import decimal
import io
import json
import logging
import re
import threading
import time
import uuid
from collections.abc import Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
import redshift_connector
import sqlalchemy as sa

from conftest import LocalLocation, S3Location, record, redshift_config
from lancamentos_model import ACCOUNTS, ENTRIES, MONTHS, PROJECTED, entries
from serialize_db import delta, schema
from serialize_db.engine import Engine, redshift
from serialize_db.engine.redshift import (
    RedshiftConfig,
    RedshiftEngine,
    mask,
    sandbox_prefix,
    schema_from_row_description,
)
from serialize_db.errors import ContractError, RegistrationRefused, SandboxError, SqlError
from serialize_db.storage import Storage


EXECUTION_ID = "exec-2026-09-05"
PREFIX = "exec_exec_2026_09_05_"
METADATA = delta.commit_metadata(EXECUTION_ID, {"cad_contas": 1})
# O iam_role evita a credencial do boto3 no COPY e no UNLOAD da conexão de mentira: o runner do
# GitHub não tem nenhuma; só test_credentials_clause_and_mask exercita o caminho do boto3.
CONFIG = RedshiftConfig(host="host", user="usuario", password="senha", database="dev",
                        share_database="compartilhado", schema="esquema", region="sa-east-1",
                        iam_role="default")

# O OID e o type_modifier de cada tipo Arrow, como o row_desc do driver os traz.
OID_OF = {
    pa.bool_(): 16, pa.int16(): 21, pa.int32(): 23, pa.int64(): 20, pa.float32(): 700,
    pa.float64(): 701, pa.string(): 1043, pa.date32(): 1082, pa.timestamp("us"): 1114,
    pa.timestamp("us", "UTC"): 1184,
}


def row_desc_of(arrow_schema: pa.Schema) -> list[dict]:
    """O ``row_desc`` do driver para um esquema Arrow."""
    fields = []
    for field in arrow_schema:
        modifier = -1
        oid = OID_OF.get(field.type)
        if pa.types.is_decimal(field.type):
            oid = 1700
            modifier = ((field.type.precision << 16) | field.type.scale) + 4
        fields.append({"label": field.name.encode(), "type_oid": oid, "type_modifier": modifier})
    return fields


def server_error(message: str, code: str = "XX000") -> redshift_connector.ProgrammingError:
    """O erro do driver com os campos do servidor."""
    return redshift_connector.ProgrammingError({"S": "ERROR", "C": code, "M": message})


# ---------------------------------------------------------------- a conexão de mentira


class FakeCursor:
    """O cursor de uma ``FakeConnection``: ``execute`` pergunta à conexão, que responde as linhas
    e o ``row_desc``."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection
        self.paramstyle = "format"
        self.description: list | None = None
        self.rowcount = -1
        self.ps: dict = {"row_desc": []}
        self._rows: list = []

    def execute(self, text: str, params: object = None) -> FakeCursor:
        row_desc, rows = self.connection.answer(text, params)
        self.ps = {"row_desc": row_desc}
        self.description = None
        if row_desc:
            self.description = [(field["label"].decode(), field["type_oid"], None, None, None,
                                 None, None) for field in row_desc]
        self._rows = rows
        return self

    def fetchone(self) -> list | None:
        return self._rows.pop(0) if self._rows else None

    def fetchall(self) -> list:
        rows, self._rows = self._rows, []
        return rows


@dataclasses.dataclass
class Command:
    """Um comando registrado pela conexão de mentira, com o instante de início e de fim."""

    text: str
    params: object
    started: float
    finished: float = 0.0


class FakeConnection:
    """A conexão de mentira: registra cada comando com o instante, dorme ``delay`` segundos por
    comando, sabe que tabelas existem, responde ``pg_last_unload_count()`` e, num ``UNLOAD``, grava
    ``unload_rows`` num Parquet da pasta local com o manifesto, como o Redshift faria."""

    def __init__(self, storage: Storage | None = None, unload_rows: pa.Table | None = None,
                 existing: set[str] = frozenset(), delay: float = 0.0,
                 drop_next: int = 0) -> None:
        self.storage = storage
        self.unload_rows = unload_rows if unload_rows is not None else pa.table({})
        self.existing = set(existing)
        self.delay = delay
        self.drop_next = drop_next
        self.commands: list[Command] = []
        self.last_unload_count = 0
        self.write_manifest = True
        self.autocommit = False
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def close(self) -> None:
        self.closed = True

    def answer(self, text: str, params: object) -> tuple[list, list]:
        """A resposta de um comando: as linhas de uma consulta conhecida, ou nada."""
        command = Command(text, params, time.perf_counter())
        self.commands.append(command)
        if self.drop_next > 0:
            self.drop_next -= 1
            raise redshift_connector.InterfaceError("BrokenPipe: server socket closed")
        time.sleep(self.delay)
        try:
            return self._answer(text)
        finally:
            command.finished = time.perf_counter()

    def _answer(self, text: str) -> tuple[list, list]:
        first = text.split(None, 1)[0].upper()
        if first == "UNLOAD":
            self._unload(text)
            return [], []
        if match := re.fullmatch(r'SELECT 1 FROM "esquema"\."(\w+)" LIMIT 0', text):
            if match.group(1) in self.existing:
                return [], []
            raise server_error(f'relation "{match.group(1)}" does not exist', "42P01")
        if text == "SELECT pg_last_unload_count()":
            return row_desc_of(pa.schema([("pg_last_unload_count", pa.int64())])), \
                [[self.last_unload_count]]
        if text.startswith("SELECT * FROM (") and text.endswith(") AS t LIMIT 0"):
            return row_desc_of(self.unload_rows.schema), []
        if text.startswith("SELECT count(*) FROM"):
            return row_desc_of(pa.schema([("count", pa.int64())])), \
                [[self.unload_rows.num_rows]]
        if first == "CREATE":
            self.existing.add(re.search(r'"(\w+)"', text).group(1))
        return [], []

    def _unload(self, text: str) -> None:
        """Grava as linhas do UNLOAD num arquivo do destino, e o manifesto."""
        self.last_unload_count = self.unload_rows.num_rows
        if self.unload_rows.num_rows == 0:
            return
        destination = re.search(r"TO '([^']+)'", text).group(1)
        prefix = self.storage.relative(destination)
        path = self.storage.join(prefix, "000.parquet")
        buffer = io.BytesIO()
        pq.write_table(self.unload_rows, buffer, use_deprecated_int96_timestamps=True)
        with self.storage.open_output_stream(path) as sink:
            sink.write(buffer.getvalue())
        if not self.write_manifest:
            return
        entry = {"url": self.storage.uri_of(path),
                 "meta": {"content_length": len(buffer.getvalue()),
                          "record_count": self.unload_rows.num_rows}}
        self.storage.write_text(self.storage.join(prefix, "manifest"),
                                json.dumps({"entries": [entry]}))

    def texts(self) -> list[str]:
        """Os comandos registrados, mascarados."""
        return [mask(command.text) for command in self.commands]


def fake_engine(monkeypatch: pytest.MonkeyPatch, connection: FakeConnection,
                storage: Storage | None = None) -> RedshiftEngine:
    """O motor sobre a conexão de mentira, com o ``staging/`` da execução na raiz local."""
    monkeypatch.setattr(redshift, "driver_connect", lambda login: connection)
    root = storage if storage is not None else Storage.for_uri("/tmp/sem-uso")
    return RedshiftEngine(CONFIG, EXECUTION_ID, root, f"prod/staging/{EXECUTION_ID}")


# ---------------------------------------------------------------- a configuração e os textos


def test_config_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """``SERIALIZE_DB_REDSHIFT_*`` para ``RedshiftConfig``; a variável vazia é ausente; o par
    informado e o workgroup são os caminhos de conexão, e sem os dois a conexão é recusada."""
    variables = {
        "SERIALIZE_DB_REDSHIFT_WORKGROUP": "wg", "SERIALIZE_DB_REDSHIFT_DATABASE": "banco",
        "SERIALIZE_DB_REDSHIFT_SHARE_DATABASE": "compartilhado",
        "SERIALIZE_DB_REDSHIFT_SCHEMA": "esquema", "SERIALIZE_DB_REDSHIFT_IAM_ROLE": "",
        "SERIALIZE_DB_REDSHIFT_PORT": "5440", "AWS_DEFAULT_REGION": "sa-east-1",
    }
    config = RedshiftConfig.from_environment(variables)
    assert config == RedshiftConfig(workgroup="wg", database="banco",
                                    share_database="compartilhado", schema="esquema", port=5440,
                                    region="sa-east-1")
    assert RedshiftConfig.from_environment({}) == RedshiftConfig()
    assert redshift.login_of(CONFIG) == {"host": "host", "port": 5439, "user": "usuario",
                                         "password": "senha"}
    with pytest.raises(ContractError, match="workgroup"):
        redshift.login_of(RedshiftConfig())


def test_sandbox_prefix_normalizes_and_limits() -> None:
    """``[a-z0-9_]`` e o espaço para o nome da tabela dentro dos 127 bytes."""
    assert sandbox_prefix("exec-2026-09-05") == PREFIX
    assert sandbox_prefix("Correção.Agosto") == "exec_corre__o_agosto_"
    assert sandbox_prefix("x" * 58).endswith("_")
    with pytest.raises(ContractError, match="127"):
        sandbox_prefix("x" * 59)


def test_credentials_clause_and_mask(monkeypatch: pytest.MonkeyPatch) -> None:
    """``IAM_ROLE`` com ARN e ``default``; as três chaves da sessão sem ``iam_role``; ``mask``
    tira os valores, e a nota de um erro leva o comando mascarado."""
    assert redshift.credentials_clause(CONFIG) == "IAM_ROLE default"
    arn = "arn:aws:iam::123456789012:role/papel"
    assert redshift.credentials_clause(dataclasses.replace(CONFIG, iam_role=arn)) == \
        f"IAM_ROLE '{arn}'"
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIACHAVE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "segredo")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    clause = redshift.credentials_clause(dataclasses.replace(CONFIG, iam_role=None))
    assert clause == "ACCESS_KEY_ID 'AKIACHAVE' SECRET_ACCESS_KEY 'segredo' SESSION_TOKEN 'token'"
    assert mask(clause) == "ACCESS_KEY_ID '***' SECRET_ACCESS_KEY '***' SESSION_TOKEN '***'"
    assert "segredo" not in mask(redshift.copy_text("t", "s3://b/m", clause, manifest=True))

    connection = FakeConnection()
    connection.existing = set()
    engine = fake_engine(monkeypatch, connection)
    text = redshift.copy_text('"esquema"."x"', "s3://b/m", clause, manifest=True)
    connection.drop_next = 0

    def refuse(text: str) -> tuple[list, list]:
        raise server_error("Spectrum Scan Error")

    connection._answer = refuse
    with pytest.raises(redshift_connector.ProgrammingError) as failure:
        engine.execute(text)
    assert failure.value.__notes__ == [f"comando: {mask(text)}"]
    assert "segredo" not in str(failure.value.__notes__)


def test_copy_insert_unload_text() -> None:
    """``COPY ... FORMAT AS PARQUET MANIFEST FILLRECORD`` sem ``COMPUPDATE``; o ``INSERT`` com a
    lista de colunas, o valor da partição no lugar dela e ``JSON_PARSE`` no JSON; o ``UNLOAD``
    com manifesto verboso, sem ``PARTITION BY``, ``PARALLEL OFF`` opcional e o ``select`` com a
    aspa e a contrabarra dobradas; nomes em duas partes."""
    credentials = "IAM_ROLE default"
    copied = redshift.copy_text('"esquema"."t_staging"', "s3://b/prod/staging/e/t/m.manifest",
                                credentials, manifest=True)
    assert copied == ('COPY "esquema"."t_staging"\nFROM \'s3://b/prod/staging/e/t/m.manifest\'\n'
                      "IAM_ROLE default\nFORMAT AS PARQUET MANIFEST FILLRECORD")
    assert redshift.copy_text("t", "s3://b/f.parquet", credentials, manifest=False).endswith(
        "FORMAT AS PARQUET FILLRECORD")
    assert "COMPUPDATE" not in copied

    inserted = redshift.insert_from_staging('"esquema"."t"', '"esquema"."t_staging"', ENTRIES,
                                            "2026-08-31")
    columns = ", ".join(f'"{name}"' for name in ENTRIES.c.keys())
    assert inserted == (
        f'INSERT INTO "esquema"."t" ({columns})\n'
        'SELECT "id_lancamento", "id_conta", "data_base", "carimbo", "valor", "preco", "area", '
        'JSON_PARSE("meta"), "to", "codigo", \'2026-08-31\' FROM "esquema"."t_staging"')
    # Sem o valor, a coluna de partição vem da staging: o loader.
    assert '"codigo", "data_base_str" FROM' in redshift.insert_from_staging(
        "t", "s", ENTRIES, None)

    select = "select \"texto\" from \"t\" where \"texto\" = 'd''agua' and x = 'barra \\\\ n'"
    unloaded = redshift.unload_text(select, "s3://b/prod/t/data_base_str=2026-08-31/e_1",
                                    credentials, parallel=False)
    assert unloaded == (
        "UNLOAD ('select \"texto\" from \"t\" where \"texto\" = ''d''''agua'' "
        "and x = ''barra \\\\\\\\ n''')\n"
        "TO 's3://b/prod/t/data_base_str=2026-08-31/e_1/'\nIAM_ROLE default\n"
        "FORMAT AS PARQUET MANIFEST VERBOSE PARALLEL OFF")
    assert "PARTITION BY" not in unloaded
    assert redshift.unload_text(select, "s3://b/x/", credentials, parallel=True).endswith(
        "MANIFEST VERBOSE")


def test_staging_ddl_without_partition_column() -> None:
    """A staging sem a coluna de partição, anulável, com o JSON em ``VARCHAR(65535)`` e sem
    cláusula física; a tabela do sandbox com ela, pelo ``ddl`` do modelo."""
    staging = redshift.staging_ddl(ENTRIES, '"esquema"."t_staging"',
                                   redshift.columns_without_partition(ENTRIES))
    assert staging.startswith('CREATE TABLE "esquema"."t_staging" (\n    "id_lancamento" BIGINT,')
    assert '"meta" VARCHAR(65535)' in staging
    assert '"carimbo" TIMESTAMP' in staging
    assert "data_base_str" not in staging
    assert "NOT NULL" not in staging and "SORTKEY" not in staging
    temporary = redshift.staging_ddl(ENTRIES, '"t_carga"', ENTRIES.columns, temporary=True)
    assert temporary.startswith('CREATE TEMP TABLE "t_carga" (')
    assert '"data_base_str" VARCHAR(10)' in temporary
    sandbox = schema.ddl(ENTRIES, "redshift", prefix=PREFIX)
    assert sandbox.startswith(f'CREATE TABLE "{PREFIX}cad_lancamentos" (')
    assert '"data_base_str" VARCHAR(10) NOT NULL' in sandbox
    assert sandbox.endswith('SORTKEY ("data_base", "id_lancamento")')


def test_table_from_cursor_by_columns() -> None:
    """Um cursor de mentira: a ``pa.Table`` com os tipos do ``row_desc``, igual ao caminho por
    dicionários; vazia com o esquema num resultado sem linha, e sem coluna num comando."""
    arrow_schema = pa.schema([("id", pa.int64()), ("valor", pa.decimal128(18, 2)),
                              ("dia", pa.date32()), ("area", pa.string()),
                              ("carimbo", pa.timestamp("us"))])
    rows = [[k, decimal.Decimal(k) / 4, dt.date(2026, 8, 28 + k),
             f"area {k}", dt.datetime(2026, 8, 28 + k, 12)] for k in range(3)]
    connection = FakeConnection()
    cursor = connection.cursor()
    cursor.ps = {"row_desc": row_desc_of(arrow_schema)}
    cursor.description = [(f.name, 0, None, None, None, None, None) for f in arrow_schema]
    cursor._rows = [list(row) for row in rows]
    table = redshift.table_from_cursor(cursor)
    assert table.schema.equals(arrow_schema)
    names = arrow_schema.names
    by_dicts = pa.Table.from_pylist([dict(zip(names, row)) for row in rows], schema=arrow_schema)
    assert table.equals(by_dicts)

    cursor._rows = []
    empty = redshift.table_from_cursor(cursor)
    assert empty.num_rows == 0 and empty.schema.equals(arrow_schema)
    cursor.description = None
    assert redshift.table_from_cursor(cursor).num_columns == 0


def test_schema_from_row_description() -> None:
    """Cada OID da tabela para o tipo Arrow; ``NUMERIC`` com a precisão e a escala do
    ``type_modifier``; outro OID, e o ``NUMERIC`` sem modificador, recusados com o nome da
    coluna."""
    row_desc = [
        {"label": b"c_bigint", "type_oid": 20, "type_modifier": -1},
        {"label": b"c_integer", "type_oid": 23, "type_modifier": -1},
        {"label": b"c_smallint", "type_oid": 21, "type_modifier": -1},
        {"label": b"c_double", "type_oid": 701, "type_modifier": -1},
        {"label": b"c_real", "type_oid": 700, "type_modifier": -1},
        {"label": b"c_decimal", "type_oid": 1700, "type_modifier": 1179654},
        {"label": b"c_sum", "type_oid": 1700, "type_modifier": ((38 << 16) | 2) + 4},
        {"label": b"c_varchar", "type_oid": 1043, "type_modifier": 44},
        {"label": b"c_char", "type_oid": 1042, "type_modifier": 6},
        {"label": b"c_text", "type_oid": 25, "type_modifier": -1},
        {"label": b"c_unknown", "type_oid": 705, "type_modifier": -1},
        {"label": b"c_name", "type_oid": 19, "type_modifier": -1},
        {"label": b"c_date", "type_oid": 1082, "type_modifier": -1},
        {"label": b"c_timestamp", "type_oid": 1114, "type_modifier": -1},
        {"label": b"c_timestamptz", "type_oid": 1184, "type_modifier": -1},
        {"label": b"c_boolean", "type_oid": 16, "type_modifier": -1},
        {"label": b"c_super", "type_oid": 4000, "type_modifier": 16384000},
    ]
    expected = pa.schema([
        ("c_bigint", pa.int64()), ("c_integer", pa.int32()), ("c_smallint", pa.int16()),
        ("c_double", pa.float64()), ("c_real", pa.float32()),
        ("c_decimal", pa.decimal128(18, 2)), ("c_sum", pa.decimal128(38, 2)),
        ("c_varchar", pa.string()), ("c_char", pa.string()), ("c_text", pa.string()),
        ("c_unknown", pa.string()), ("c_name", pa.string()), ("c_date", pa.date32()),
        ("c_timestamp", pa.timestamp("us")), ("c_timestamptz", pa.timestamp("us", "UTC")),
        ("c_boolean", pa.bool_()), ("c_super", pa.string()),
    ])
    assert schema_from_row_description(row_desc).equals(expected)
    with pytest.raises(SandboxError, match="c_geo.*OID 3000"):
        schema_from_row_description([{"label": b"c_geo", "type_oid": 3000, "type_modifier": -1}])
    with pytest.raises(SandboxError, match="c_num"):
        schema_from_row_description([{"label": b"c_num", "type_oid": 1700, "type_modifier": -1}])


def test_stream_literal_values(monkeypatch: pytest.MonkeyPatch) -> None:
    """O texto do ``UNLOAD`` de um statement com texto, data, número e ``IN`` de lista, com o ``%``
    sem dobrar e o prefixo do sandbox; o texto pronto com os ``bindparam`` tipados pelo valor; um
    ``bindparam`` sem valor, também num ``IN`` de lista, recusado antes de qualquer comando."""
    statement = (
        sa.select(ENTRIES.c.id_lancamento, ENTRIES.c.area)
        .where(ENTRIES.c.area.like(sa.bindparam("padrao")),
               ENTRIES.c.data_base >= sa.bindparam("dia"),
               ENTRIES.c.preco > sa.bindparam("preco"),
               ENTRIES.c.id_lancamento.in_(sa.bindparam("ids", expanding=True)))
        .order_by(ENTRIES.c.id_lancamento)
    )
    params = {"padrao": "50% d'agua \\ n", "dia": dt.date(2026, 8, 29),
              "preco": decimal.Decimal("1.00"), "ids": [1, 2, 3]}
    literal = redshift.literal_text(statement, params, PREFIX)
    assert f'FROM "{PREFIX}cad_lancamentos"' in literal
    assert "LIKE '50% d''agua \\\\ n'" in literal
    assert "\"data_base\" >= '2026-08-29'" in literal
    assert '"preco" > 1.00' in literal
    assert '"id_lancamento" IN (1, 2, 3)' in literal
    assert ":" not in literal.split("FROM")[1]

    text = ('SELECT "id_lancamento" FROM "{prefix}cad_lancamentos" WHERE "area" = :area '
            'AND "id_lancamento" IN :ids AND "data_base" > :dia')
    from_text = redshift.literal_text(text, {"area": "a'b", "ids": [4, 5],
                                             "dia": dt.date(2026, 1, 1)}, PREFIX)
    assert from_text == (f'SELECT "id_lancamento" FROM "{PREFIX}cad_lancamentos" WHERE "area" = '
                         "'a''b' AND \"id_lancamento\" IN (4, 5) AND \"data_base\" > '2026-01-01'")

    connection = FakeConnection()
    engine = fake_engine(monkeypatch, connection)
    before = len(connection.commands)
    with pytest.raises(SqlError, match="padrao"):
        engine.stream(statement, {"dia": dt.date(2026, 8, 29), "preco": decimal.Decimal("1"),
                                  "ids": [1]})
    with pytest.raises(SqlError, match="ids"):
        engine.stream(sa.select(ENTRIES).where(ENTRIES.c.id_lancamento.in_(
            sa.bindparam("ids", expanding=True))))
    with pytest.raises(SqlError, match="area"):
        engine.stream(text, {"ids": [1], "dia": dt.date(2026, 1, 1)})
    assert len(connection.commands) == before

    # O query: os valores como parâmetros do driver, no estilo named, e o IN expandido.
    compiled, values = redshift.compiled_for_cursor(statement, params, PREFIX)
    assert set(values) == {"padrao", "dia", "preco", "ids_1", "ids_2", "ids_3"}
    assert "(:ids_1, :ids_2, :ids_3)" in compiled and ":padrao" in compiled
    bound, bound_values = redshift.compiled_for_cursor(text, {"area": "x", "ids": [1],
                                                             "dia": dt.date(2026, 1, 1)}, PREFIX)
    assert f'"{PREFIX}cad_lancamentos"' in bound and bound_values["area"] == "x"


@pytest.mark.local
def test_stream_empty_result(monkeypatch: pytest.MonkeyPatch,
                             local_location: LocalLocation) -> None:
    """Uma conexão de mentira em que o ``UNLOAD`` não grava manifesto: com
    ``pg_last_unload_count()`` em 0, o ``stream`` sai sem lote e com o esquema do ``row_desc``; com
    2, a falta do manifesto sobe com a contagem."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    empty = entries(MONTHS[0], 1, 0)
    connection = FakeConnection(storage, unload_rows=empty)
    engine = fake_engine(monkeypatch, connection, storage)
    with engine.stream(sa.select(ENTRIES)) as stream:
        assert stream.schema.names == ENTRIES.c.keys()
        assert stream.schema.field("preco").type == pa.decimal128(18, 2)
        assert stream.schema.field("carimbo").type == pa.timestamp("us")
        assert list(stream) == []
        assert stream.read_all().num_rows == 0
    texts = connection.texts()
    assert texts[-3].startswith("SELECT * FROM (SELECT") and texts[-3].endswith(") AS t LIMIT 0")
    assert texts[-2].startswith("UNLOAD ('SELECT") and "PARALLEL OFF" in texts[-2]
    assert texts[-1] == "SELECT pg_last_unload_count()"

    connection.unload_rows = entries(MONTHS[0], 1, 2)
    connection.write_manifest = False
    with pytest.raises(FileNotFoundError, match="2 linha"):
        engine.stream(sa.select(ENTRIES))


@pytest.mark.local
def test_stream_reads_the_unloaded_file_in_the_statement_schema(
        monkeypatch: pytest.MonkeyPatch, local_location: LocalLocation) -> None:
    """Os lotes vêm do arquivo do ``UNLOAD``, com o ``INT96`` em microssegundos e cada lote no
    esquema do ``row_desc``; ``close`` apaga o prefixo do stream, e ``read_all`` dá as linhas."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    rows = entries(MONTHS[0], 1, 250)
    connection = FakeConnection(storage, unload_rows=rows)
    engine = fake_engine(monkeypatch, connection, storage)
    with engine.stream(sa.select(ENTRIES), batch_size=100) as stream:
        batches = list(stream)
        prefix = stream._prefix
        assert storage.list_files(prefix) == [f"{prefix}/000.parquet", f"{prefix}/manifest"]
    assert [batch.num_rows for batch in batches] == [100, 100, 50]
    assert pa.Table.from_batches(batches).equals(rows.cast(stream.schema))
    assert storage.list_files(prefix) == []
    with engine.stream(sa.select(ENTRIES)) as other:
        assert other.read_all().num_rows == 250


@pytest.mark.local
def test_statements_serialize_on_the_single_session(monkeypatch: pytest.MonkeyPatch,
                                                    local_location: LocalLocation) -> None:
    """Dois comandos de duas threads não se sobrepõem; um comando roda enquanto um ``stream`` ainda
    lê os arquivos, porque o lock solta no fim do ``UNLOAD``; um ``stream`` aberto dentro de
    ``session()``, na mesma thread, não trava; e ``new_session`` abre outra conexão."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    connection = FakeConnection(storage, unload_rows=entries(MONTHS[0], 1, 300), delay=0.05)
    engine = fake_engine(monkeypatch, connection, storage)
    assert isinstance(engine, Engine)

    threads = [threading.Thread(target=engine.query, args=(f"SELECT {k}",)) for k in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    windows = sorted((command.started, command.finished) for command in connection.commands[-3:])
    for (_, finished), (started, _) in zip(windows, windows[1:]):
        assert finished <= started

    with engine.stream(sa.select(ENTRIES), batch_size=10) as stream:
        first = stream.read_next_batch()
        started = time.perf_counter()
        engine.query("SELECT 1")
        assert time.perf_counter() - started < 1.0
        assert first.num_rows == 10
        assert sum(batch.num_rows for batch in stream) == 290

    with engine.session():
        with engine.stream(sa.select(ENTRIES)) as inner:
            assert inner.read_all().num_rows == 300

    connections = []
    def open_fake(login: dict) -> FakeConnection:
        connections.append(FakeConnection(storage))
        return connections[-1]

    monkeypatch.setattr(redshift, "driver_connect", open_fake)
    with engine.new_session() as other:
        other.query("SELECT 2")
    assert len(connections) == 1 and connections[0].closed
    assert [mask(c.text) for c in connections[0].commands] == [
        "USE compartilhado", "SET search_path TO esquema", "SELECT 2"]


def test_connection_dropped_by_the_server_is_reopened_once(
        monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    """Uma conexão derrubada (``InterfaceError``) é reaberta uma vez, com o ``USE`` e o
    ``search_path``, e o comando repetido; dentro de uma transação o erro sobe, com o
    ``ROLLBACK`` tentado."""
    connections = [FakeConnection(), FakeConnection()]
    monkeypatch.setattr(redshift, "driver_connect", lambda login: connections.pop(0))
    engine = RedshiftEngine(CONFIG, EXECUTION_ID, Storage.for_uri("/tmp/sem-uso"), "prod/staging")
    first = engine._connection
    first.drop_next = 1
    with caplog.at_level(logging.WARNING, logger="serialize_db.engine.redshift"):
        engine.query("SELECT 1")
    assert first.closed and engine._connection is not first
    assert [mask(c.text) for c in engine._connection.commands] == [
        "USE compartilhado", "SET search_path TO esquema", "SELECT 1"]
    assert "derrubada" in caplog.text

    with pytest.raises(redshift_connector.InterfaceError):
        with engine.transaction():
            engine._connection.drop_next = 1
            engine.execute("SELECT 2")
    assert [mask(c.text) for c in engine._connection.commands][-3:] == ["BEGIN", "SELECT 2",
                                                                          "ROLLBACK"]


@pytest.mark.local
def test_loader_writes_the_file_and_creates_the_table_in_a_transaction(
        monkeypatch: pytest.MonkeyPatch, local_location: LocalLocation) -> None:
    """O nome ocupado é recusado na abertura; o arquivo nasce no ``staging/`` na thread auxiliar;
    ``close`` roda ``BEGIN``, o ``CREATE TABLE``, a staging temporária com o ``COPY`` e o
    ``INSERT`` com ``JSON_PARSE``, e ``COMMIT``, e apaga o arquivo; uma exceção no ``with`` não
    cria a tabela; um lote recusado pelo ``cast`` também não."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    connection = FakeConnection(storage, existing={f"{PREFIX}cad_lancamentos"})
    engine = fake_engine(monkeypatch, connection, storage)
    with pytest.raises(SandboxError, match="ocupado"):
        engine.loader(ENTRIES)

    with engine.loader(PROJECTED) as loader:
        loader.write(entries(MONTHS[0], 1, 10, PROJECTED))
        loader.write(entries(MONTHS[0], 11, 5, PROJECTED).to_batches()[0])
    assert loader.rows == 15
    assert storage.list_files(f"prod/staging/{EXECUTION_ID}") == []
    texts = connection.texts()
    start = texts.index("BEGIN")
    assert texts[start + 1].startswith(f'CREATE TABLE "{PREFIX}cad_lancamentos_projetados" (')
    carga = f"{PREFIX}cad_lancamentos_projetados_carga"
    assert texts[start + 2].startswith(f'CREATE TEMP TABLE "{carga}"')
    assert texts[start + 3].startswith(f'COPY "{PREFIX}cad_lancamentos_projetados_carga"\nFROM ')
    assert "FORMAT AS PARQUET FILLRECORD" in texts[start + 3]
    assert texts[start + 4].startswith(
        f'INSERT INTO "esquema"."{PREFIX}cad_lancamentos_projetados" (')
    assert 'JSON_PARSE("meta")' in texts[start + 4]
    assert texts[start + 5:start + 7] == [f'DROP TABLE "{carga}"', "COMMIT"]

    # A tabela sem coluna JSON recebe o COPY direto.
    connection.existing.discard(f"{PREFIX}cad_contas")
    engine.load(ACCOUNTS, pa.table({"id_conta": pa.array([1], pa.int64()), "numero": ["A"]}))
    assert connection.texts()[-2].startswith(f'COPY "esquema"."{PREFIX}cad_contas"\nFROM ')

    # Uma exceção dentro do with: o arquivo sai e nada é criado.
    connection.existing.discard(f"{PREFIX}cad_lancamentos_projetados")
    before = len(connection.commands)
    with pytest.raises(RuntimeError, match="plantado"):
        with engine.loader(PROJECTED) as loader:
            loader.write(entries(MONTHS[0], 1, 10, PROJECTED))
            raise RuntimeError("erro plantado")
    assert connection.texts()[before:] == [
        f'SELECT 1 FROM "esquema"."{PREFIX}cad_lancamentos_projetados" LIMIT 0']
    assert storage.list_files(f"prod/staging/{EXECUTION_ID}") == []

    # O lote recusado pelo cast.
    with pytest.raises(ContractError):
        with engine.loader(PROJECTED) as loader:
            loader.write(pa.table({"id_lancamento": ["x"]}))
    assert "BEGIN" not in connection.texts()[before + 1:]

    # O DataFrame é recusado antes de qualquer carga.
    with pytest.raises(ContractError, match="from_pandas"):
        engine.load(PROJECTED, {"id_lancamento": [1]})


@pytest.mark.local
def test_ingest_loads_each_partition_through_the_staging(monkeypatch: pytest.MonkeyPatch,
                                                         local_location: LocalLocation) -> None:
    """Por partição, o manifesto no ``staging/``, o ``DELETE`` da staging, o ``COPY ... MANIFEST
    FILLRECORD`` e o ``INSERT`` com o valor; a staging apagada no fim; a partição sem arquivo
    não roda; o nome ocupado e a tabela sem versão são ``SandboxError``; ``published`` carrega a
    versão inteira em ``_publicado`` uma vez; ``cleanup`` apaga as tabelas e o ``staging/``."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    uri = storage.uri_of("prod/cad_lancamentos")
    delta.create_table(uri, ENTRIES, storage)
    for index, month in enumerate(MONTHS):
        delta.publish_partition(uri, ENTRIES, month, entries(month, 1 + index * 10, 10), METADATA,
                                storage)
    connection = FakeConnection(storage)
    engine = fake_engine(monkeypatch, connection, storage)
    with pytest.raises(SandboxError, match="versão"):
        engine.ingest(ENTRIES, uri, None)

    engine.ingest(ENTRIES, uri, 2, partitions=[MONTHS[1], "2026-09-30"])
    texts = connection.texts()
    name = f"{PREFIX}cad_lancamentos"
    staging = f"{name}_staging"
    assert any(text.startswith(f'CREATE TABLE "{name}" (') for text in texts)
    assert any(text.startswith(f'CREATE TABLE "esquema"."{staging}" (') for text in texts)
    copies = [text for text in texts if text.startswith("COPY")]
    assert len(copies) == 1
    manifest_uri = re.search(r"FROM '([^']+)'", copies[0]).group(1)
    staging_folder = storage.uri_of(f"prod/staging/{EXECUTION_ID}/cad_lancamentos/")
    assert manifest_uri.startswith(staging_folder)
    assert "FORMAT AS PARQUET MANIFEST FILLRECORD" in copies[0]
    manifest = json.loads(storage.read_text(storage.relative(manifest_uri))[0])
    assert len(manifest["entries"]) == 1
    assert f"data_base_str={MONTHS[1]}/" in manifest["entries"][0]["url"]
    inserts = [text for text in texts if text.startswith("INSERT")]
    assert inserts == [redshift.insert_from_staging(
        f'"esquema"."{name}"', f'"esquema"."{staging}"', ENTRIES, MONTHS[1])]
    assert texts[-1] == f'DROP TABLE IF EXISTS "esquema"."{staging}"'
    with pytest.raises(SandboxError, match="ocupado"):
        engine.ingest(ENTRIES, uri, 2)

    # published: a versão inteira, uma vez.
    source = engine.published(ENTRIES, uri, 2)
    assert str(sa.select(source.c.id_lancamento)).startswith(
        f'SELECT "{PREFIX}cad_lancamentos_publicado"."id_lancamento"')
    copies = [text for text in connection.texts() if text.startswith("COPY")]
    assert len(copies) == 3
    engine.published(ENTRIES, uri, 2)
    assert len([text for text in connection.texts() if text.startswith("COPY")]) == 3
    with pytest.raises(SandboxError):
        engine.published(ENTRIES, uri, None)

    engine.cleanup()
    dropped = [text for text in connection.texts() if text.startswith("DROP TABLE IF EXISTS")]
    assert dropped[-2:] == [f'DROP TABLE IF EXISTS "esquema"."{name}"',
                            f'DROP TABLE IF EXISTS "esquema"."{name}_publicado"']
    assert storage.list_files(f"prod/staging/{EXECUTION_ID}") == []
    assert connection.closed


@pytest.mark.local
def test_export_registers_the_unloaded_files_and_swaps_on_nonfinite(
        monkeypatch: pytest.MonkeyPatch, local_location: LocalLocation,
        caplog: pytest.LogCaptureFixture) -> None:
    """O registro: o ``UNLOAD`` sem ``PARTITION BY`` para
    ``<coluna>=<valor>/<execution_id>_<uuid>/`` na pasta da tabela, o ``select`` sem a coluna de
    partição e com o JSON serializado, e o arquivo como o Redshift o gravou (``INT96``) no log, com
    as linhas conferidas; dois destinos distintos para a mesma partição. A troca: com
    ``columns_without_min_max``, o destino no ``staging/`` e a partição por ``publish_partition``,
    com o aviso no log; a partição vazia entra por um arquivo sem linha."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    uri = storage.uri_of("prod/cad_lancamentos_projetados")
    delta.create_table(uri, PROJECTED, storage)
    rows = entries(MONTHS[1], 1, 1000, PROJECTED)
    unloaded = rows.drop_columns(["data_base_str"])
    connection = FakeConnection(storage, unload_rows=unloaded)
    engine = fake_engine(monkeypatch, connection, storage)

    version = engine.export_partition(PROJECTED, uri, MONTHS[1], METADATA, expected_rows=1000)
    assert version == 1
    unload = [text for text in connection.texts() if text.startswith("UNLOAD")][-1]
    select = re.search(r"UNLOAD \('(.+)'\)", unload, re.DOTALL).group(1)
    assert select.startswith('SELECT "id_lancamento", "id_conta", "data_base", "carimbo", "valor", '
                             '"preco", "area", JSON_SERIALIZE("meta") AS "meta", "to", "codigo" '
                             f'FROM "esquema"."{PREFIX}cad_lancamentos_projetados" '
                             f"WHERE \"data_base_str\" = ''{MONTHS[1]}''")
    assert select.endswith('ORDER BY "data_base", "id_lancamento"')
    assert "PARALLEL OFF" in unload
    destination = re.search(r"TO '([^']+)'", unload).group(1)
    pattern = re.escape(uri) + rf"/data_base_str={MONTHS[1]}/{EXECUTION_ID}_[0-9a-f]{{32}}/"
    assert re.fullmatch(pattern, destination)
    files = storage.list_files(storage.relative(uri), ".parquet")
    assert len(files) == 1 and files[0].startswith(
        f"prod/cad_lancamentos_projetados/data_base_str={MONTHS[1]}/{EXECUTION_ID}_")
    read = delta.open_table(uri, storage).to_pyarrow_table()
    assert read.num_rows == 1000
    assert read.schema.field("carimbo").type == pa.timestamp("us")
    stats = pa.table(delta.open_table(uri, storage).get_add_actions(flatten=True))
    assert stats.column("max.id_lancamento").to_pylist() == [1000]
    assert stats.column("min.valor").to_pylist() == [0.25]
    assert "max.codigo" not in stats.column_names or stats.column("max.codigo").null_count == 1

    # Duas exportações da mesma partição recebem destinos distintos; a contagem diferente recusa.
    with pytest.raises(RegistrationRefused):
        engine.export_partition(PROJECTED, uri, MONTHS[1], METADATA, expected_rows=999)
    unloads = [text for text in connection.texts() if text.startswith("UNLOAD")]
    destinations = {re.search(r"TO '([^']+)'", text).group(1) for text in unloads}
    assert len(destinations) == 2

    # A troca: o destino no staging e publish_partition, com o aviso.
    with_nan = rows.set_column(rows.schema.get_field_index("valor"), "valor",
                               pa.array([float("nan")] + [k / 4 for k in range(2, 1001)]))
    connection.unload_rows = with_nan.drop_columns(["data_base_str"])
    with caplog.at_level(logging.WARNING, logger="serialize_db.engine.redshift"):
        version = engine.export_partition(PROJECTED, uri, MONTHS[1], METADATA, 1000, ["valor"])
    assert version == 2
    assert "publish_partition" in caplog.text and "['valor']" in caplog.text
    unload = [text for text in connection.texts() if text.startswith("UNLOAD")][-1]
    destination = re.search(r"TO '([^']+)'", unload).group(1)
    assert destination.startswith(storage.uri_of(
        f"prod/staging/{EXECUTION_ID}/cad_lancamentos_projetados/data_base_str={MONTHS[1]}/"))
    stats = pa.table(delta.open_table(uri, storage).get_add_actions(flatten=True))
    assert stats.column("max.valor").null_count == 1
    read = delta.open_table(uri, storage).to_pyarrow_table()
    assert read.num_rows == 1000 and pc.sum(pc.is_nan(read.column("valor"))).as_py() == 1
    assert read.column("data_base_str").unique().to_pylist() == [MONTHS[1]]

    # A partição vazia: nenhum arquivo do UNLOAD, um arquivo sem linha registrado.
    connection.unload_rows = unloaded.slice(0, 0)
    version = engine.export_partition(PROJECTED, uri, MONTHS[1], METADATA, expected_rows=0)
    assert version == 3
    assert delta.open_table(uri, storage).to_pyarrow_table().num_rows == 0
    engine.cleanup()


# ---------------------------------------------------------------- no esquema autorizado


@dataclasses.dataclass
class Target:
    """A raiz S3 e o motor de um teste no esquema autorizado."""

    storage: Storage
    engine: RedshiftEngine
    execution_id: str

    def uri(self, table: sa.Table) -> str:
        return self.storage.uri_of(f"prod/{table.name}")

    def qualified(self, suffix: str) -> str:
        return self.engine.qualified(self.engine.prefix + suffix)


@pytest.fixture
def target(s3_location: S3Location, redshift_driver: None) -> Iterator[Target]:
    """O motor sobre uma raiz nova no bucket e um sandbox próprio; no substituto local, a conexão
    de ``tests/emulator.py`` no lugar do driver."""
    storage = Storage.for_uri(s3_location.child(f"engine/{uuid.uuid4().hex[:8]}"))
    execution_id = f"poc-{uuid.uuid4().hex[:8]}"
    engine = RedshiftEngine(redshift_config(), execution_id, storage,
                            f"prod/staging/{execution_id}")
    yield Target(storage, engine, execution_id)
    engine.cleanup()


def published_table(target: Target, table: sa.Table, months: list[str], rows: int = 100) -> int:
    """A tabela Delta com ``rows`` linhas por mês e ids contíguos a partir de 1; devolve a última
    versão."""
    uri = target.uri(table)
    delta.create_table(uri, table, target.storage)
    version = 0
    for index, month in enumerate(months):
        data = entries(month, 1 + index * rows, rows, table)
        version = delta.publish_partition(uri, table, month, data, METADATA, target.storage)
    return version


def count_of(engine: RedshiftEngine, name: str) -> int:
    """As linhas da tabela ``name`` do esquema."""
    counted = engine.query(f"SELECT count(*) AS n FROM {engine.qualified(name)}")
    return counted.column("n")[0].as_py()


@pytest.mark.redshift
@pytest.mark.s3
def test_connect_uses_share_database(target: Target) -> None:
    """Depois do ``USE``, o ``CREATE TABLE`` de uma tabela ``exec_<id>_*`` por nome em duas partes
    passa e ``name_in_use`` lê o nome livre; leituras, nunca asserções: o SQLSTATE e a mensagem da
    relação inexistente, ``current_database()``, que o Redshift descreve com o tipo ``name``
    (OID 19) e que continua ``dev`` depois do ``USE``, porque o ``USE`` muda a resolução dos nomes
    e não o banco da sessão (leitura de 2026-09-21), e a presença da tabela de controle, que nenhum
    comando da conexão cria."""
    engine = target.engine
    name = f"{engine.prefix}conexao"
    engine.execute(f"CREATE TABLE {engine.qualified(name)} (id BIGINT)")
    engine.register_created(name)
    assert engine.name_in_use(name)
    assert not engine.name_in_use(f"{engine.prefix}nada")
    with pytest.raises(redshift_connector.Error) as missing:
        engine.execute(f"SELECT 1 FROM {engine.qualified(engine.prefix + 'nada')} LIMIT 0")
    fields = missing.value.args[0]
    record("redshift.engine.relation_missing",
           {"sqlstate": fields.get("C"), "message": str(fields.get("M"))})
    record("redshift.engine.current_database",
           engine.query("select current_database()").column(0)[0].as_py())
    record("redshift.engine.control_table_present",
           engine.name_in_use("serialize_db_publications"))


@pytest.mark.redshift
@pytest.mark.s3
def test_ingest_stream_loader_export(target: Target, caplog: pytest.LogCaptureFixture) -> None:
    """``ingest`` de uma partição de um Delta no bucket, ``stream`` em lotes igual ao ``query``,
    ``loader`` por ``COPY``, a auditoria com a versão publicada, ``export_partition`` pelo registro
    e pela troca com as mesmas linhas, e ``cleanup`` sem tabela restante."""
    engine = target.engine
    storage = target.storage
    entries_version = published_table(target, ENTRIES, MONTHS, rows=120)
    uri = target.uri(ENTRIES)
    engine.ingest(ENTRIES, uri, entries_version, partitions=[MONTHS[1]])
    assert count_of(engine, f"{engine.prefix}cad_lancamentos") == 120

    statement = (sa.select(ENTRIES).where(ENTRIES.c.data_base_str == sa.bindparam("particao"),
                                          ENTRIES.c.to == "SP")
                 .order_by(ENTRIES.c.id_lancamento))
    params = {"particao": MONTHS[1]}
    by_query = engine.query(statement, params)
    with engine.stream(statement, params, batch_size=50) as stream:
        batches = list(stream)
    assert [batch.num_rows for batch in batches] == [50, 50, 20]
    by_stream = pa.Table.from_batches(batches)
    assert by_stream.schema.equals(by_query.schema)
    assert by_stream.equals(by_query)
    assert json.loads(by_query.column("meta").to_pylist()[0]) == {"k": 121}
    assert by_query.column("preco").type == pa.decimal128(18, 2)
    assert by_query.column("carimbo").type == pa.timestamp("us")

    # O loader grava a projeção; o nome ocupado é recusado.
    with engine.stream(statement, params, batch_size=40) as stream, \
            engine.loader(PROJECTED) as loader:
        for batch in stream:
            loader.write(batch)
    assert loader.rows == 120
    assert count_of(engine, f"{engine.prefix}cad_lancamentos_projetados") == 120
    with pytest.raises(SandboxError, match="ocupado"):
        engine.loader(PROJECTED)

    # A auditoria com a versão publicada e a chave estrangeira fora do sandbox.
    projected_uri = target.uri(PROJECTED)
    delta.create_table(projected_uri, PROJECTED, storage)
    accounts_uri = target.uri(ACCOUNTS)
    delta.create_table(accounts_uri, ACCOUNTS, storage)
    ids = pa.array([1, 2, 3], pa.int64())
    accounts = schema.cast(pa.table({"id_conta": ids, "numero": ["A", "B", "C"]}), ACCOUNTS)
    accounts_version = delta.publish_partition(accounts_uri, ACCOUNTS, None, accounts, METADATA,
                                               storage)
    report = engine.audit(PROJECTED, [MONTHS[1]], projected_uri, 0)
    assert report.passed, report.results
    assert [result.name for result in report.results] == [
        "linhas", "chave_id_lancamento", "chave_id_lancamento_publicada", "chave_codigo",
        "chave_codigo_publicada"]
    assert report.rows(MONTHS[1]) == 120 and report.nonfinite_columns[MONTHS[1]] == ()
    # A tabela publicada vazia dispensa a junção da chave sequencial e carrega a staging só para
    # a chave codigo; a chave estrangeira de cad_lancamentos entra pela versão fixada de
    # cad_contas, carregada em _publicado quando o anti-join roda.
    orphans = engine.audit(ENTRIES, [MONTHS[1]], uri, entries_version, foreign_keys=True,
                           referenced={"cad_contas": (accounts_uri, accounts_version)})
    assert orphans.passed, orphans.results
    assert [result.name for result in orphans.results][-1] == "orfao_id_conta"
    assert count_of(engine, f"{engine.prefix}cad_contas_publicado") == 3
    assert count_of(engine, f"{engine.prefix}cad_lancamentos_publicado") == 240

    # A exportação pelo registro: o arquivo do UNLOAD na pasta da partição, lido pelos leitores.
    version = engine.export_partition(PROJECTED, projected_uri, MONTHS[1], METADATA, 120, ())
    assert version == 1
    files = storage.list_files(storage.relative(projected_uri), ".parquet")
    assert len(files) >= 1
    assert files[0].startswith(f"prod/cad_lancamentos_projetados/data_base_str={MONTHS[1]}/"
                               f"{target.execution_id}_")
    read = delta.open_table(projected_uri, storage).to_pyarrow_table().sort_by("id_lancamento")
    assert read.num_rows == 120
    assert read.column("id_lancamento").to_pylist() == list(range(121, 241))
    assert read.column("preco").to_pylist() == by_query.column("preco").to_pylist()
    assert read.column("carimbo").to_pylist() == by_query.column("carimbo").to_pylist()
    assert json.loads(read.column("meta").to_pylist()[0]) == {"k": 121}
    with storage.duckdb_connect() as connection:
        counted = connection.execute(
            f"SELECT count(*), sum(valor) FROM delta_scan('{projected_uri}')").fetchone()
        meta_scan = connection.execute(
            f"SELECT typeof(meta), meta FROM delta_scan('{projected_uri}') "
            "WHERE id_lancamento = 121").fetchone()
    assert counted[0] == 120
    record("redshift.engine.delta_scan_meta", {"tipo": meta_scan[0], "valor": str(meta_scan[1])})

    # A troca: a partição com NaN pela máquina local, com as mesmas linhas e o aviso.
    with_nan = entries(MONTHS[0], 1, 120, PROJECTED,
                       valor=[float("nan")] + [k / 4 for k in range(2, 121)])
    engine.execute(f"DELETE FROM {target.qualified('cad_lancamentos_projetados')}")
    engine.execute(f"DROP TABLE {target.qualified('cad_lancamentos_projetados')}")
    engine._created.remove(f"{engine.prefix}cad_lancamentos_projetados")
    engine.load(PROJECTED, with_nan)
    with caplog.at_level(logging.WARNING, logger="serialize_db.engine.redshift"):
        version = engine.export_partition(PROJECTED, projected_uri, MONTHS[0], METADATA, 120,
                                          ["valor"])
    assert version == 2 and "publish_partition" in caplog.text
    read = delta.open_table(projected_uri, storage).to_pyarrow_table()
    assert read.num_rows == 240
    nan_rows = read.filter(pc.is_nan(read.column("valor")))
    assert nan_rows.num_rows == 1 and nan_rows.column("data_base_str")[0].as_py() == MONTHS[0]
    stats = pa.table(delta.open_table(projected_uri, storage).get_add_actions(flatten=True))
    by_partition = dict(zip(stats.column("partition.data_base_str").to_pylist(),
                            stats.column("max.valor").to_pylist()))
    assert by_partition[MONTHS[0]] is None and by_partition[MONTHS[1]] == 60.0

    # O cleanup: nenhuma tabela exec_<id>_* e o staging vazio.
    engine.cleanup()
    other = RedshiftEngine(redshift_config(), target.execution_id + "b", storage, "prod/staging/x")
    try:
        for suffix in ("cad_lancamentos", "cad_lancamentos_projetados", "cad_contas_publicado"):
            assert not other.name_in_use(f"{engine.prefix}{suffix}")
    finally:
        other.cleanup()
    assert storage.list_files(f"prod/staging/{target.execution_id}") == []


@pytest.mark.redshift
@pytest.mark.s3
def test_loader_creates_the_table_at_close(target: Target,
                                           monkeypatch: pytest.MonkeyPatch) -> None:
    """Antes do ``close`` a leitura da tabela falha com relação inexistente; um erro do ``COPY``
    desfaz o ``CREATE TABLE``, e o nome fica livre; o ``loader`` sem lote cria a tabela vazia."""
    engine = target.engine
    name = f"{engine.prefix}cad_lancamentos_projetados"
    with engine.loader(PROJECTED) as loader:
        loader.write(entries(MONTHS[0], 1, 10, PROJECTED))
        assert not engine.name_in_use(name)
    assert count_of(engine, name) == 10
    engine.execute(f"DROP TABLE {engine.qualified(name)}")
    engine._created.remove(name)

    # O COPY de um arquivo que não existe falha, e a transação desfaz o CREATE TABLE.
    def broken_copy(target_name: str, source: str, credentials: str, manifest: bool) -> str:
        return redshift.copy_text(target_name, source + ".ausente", credentials, manifest)

    monkeypatch.setattr(redshift, "copy_text", broken_copy)
    with pytest.raises(Exception):  # noqa: B017 - o erro do COPY: do servidor ou do S3
        engine.load(PROJECTED, entries(MONTHS[0], 1, 10, PROJECTED))
    monkeypatch.undo()
    assert not engine.name_in_use(name)
    if name in engine._created:
        engine._created.remove(name)

    with engine.loader(PROJECTED):
        pass
    assert count_of(engine, name) == 0


@pytest.mark.redshift
@pytest.mark.s3
def test_new_session_sees_committed_tables(target: Target) -> None:
    """A sessão de ``new_session`` vê a tabela ``exec_<id>_*`` confirmada pela principal e não a
    temporária dela; duas ingestões em duas sessões terminam, e a principal lê as duas."""
    engine = target.engine
    version = published_table(target, ENTRIES, MONTHS, rows=20)
    accounts_uri = target.uri(ACCOUNTS)
    delta.create_table(accounts_uri, ACCOUNTS, target.storage)
    accounts = schema.cast(pa.table({"id_conta": pa.array([1, 2, 3], pa.int64()),
                                     "numero": ["A", "B", "C"]}), ACCOUNTS)
    accounts_version = delta.publish_partition(accounts_uri, ACCOUNTS, None, accounts, METADATA,
                                               target.storage)
    temporary = f"{engine.prefix}temporaria"
    engine.query(f"CREATE TEMP TABLE {temporary} AS SELECT 1 AS x")

    def ingest(table: sa.Table, uri: str, pinned: int) -> None:
        with engine.new_session() as session:
            session.ingest(table, uri, pinned)

    threads = [threading.Thread(target=ingest, args=(ENTRIES, target.uri(ENTRIES), version)),
               threading.Thread(target=ingest, args=(ACCOUNTS, accounts_uri, accounts_version))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert count_of(engine, f"{engine.prefix}cad_lancamentos") == 40
    assert count_of(engine, f"{engine.prefix}cad_contas") == 3
    with engine.new_session() as session:
        assert session.name_in_use(f"{engine.prefix}cad_contas")
        with pytest.raises(redshift_connector.Error):
            session.query(f"SELECT * FROM {temporary}")
    assert engine.query(f"SELECT x FROM {temporary}").column("x").to_pylist() == [1]


@pytest.mark.redshift
@pytest.mark.s3
def test_stream_literal_values_on_the_target(target: Target) -> None:
    """Valores com ``'`` e ``\\`` voltam iguais pelo ``stream`` e pelo ``query``; um ``select``
    sem linha dá o stream vazio com o esquema; a tabela temporária criada por ``query`` é lida
    pelo ``UNLOAD`` do ``stream`` seguinte; o ``row_desc`` de cada tipo do contrato e dos
    agregados."""
    engine = target.engine
    texts = ["d'agua", "barra \\ invertida", "50% certo", "comum"]
    rows = entries(MONTHS[0], 1, 4, PROJECTED)
    rows = rows.set_column(rows.schema.get_field_index("codigo"), "codigo", pa.array(texts))
    engine.load(PROJECTED, rows)
    for text in texts:
        statement = sa.select(PROJECTED.c.id_lancamento, PROJECTED.c.codigo).where(
            PROJECTED.c.codigo == sa.bindparam("texto"))
        by_query = engine.query(statement, {"texto": text})
        with engine.stream(statement, {"texto": text}) as stream:
            by_stream = stream.read_all()
        assert by_query.num_rows == 1 and by_stream.equals(by_query), text
    like = sa.select(PROJECTED.c.id_lancamento).where(PROJECTED.c.codigo.like(sa.bindparam("p")))
    with engine.stream(like, {"p": "%'%"}) as stream:
        assert stream.read_all().column("id_lancamento").to_pylist() == [1]

    none = sa.select(PROJECTED).where(PROJECTED.c.id_lancamento < 0)
    with engine.stream(none) as stream:
        empty = stream.read_all()
    assert empty.num_rows == 0 and empty.schema.names == PROJECTED.c.keys()

    temporary = f"{engine.prefix}temporaria"
    engine.query(f"CREATE TEMP TABLE {temporary} AS SELECT 2 AS x")
    with engine.stream(f"SELECT x FROM {temporary}") as stream:
        assert stream.read_all().column("x").to_pylist() == [2]

    described = engine.query(
        'select count(*) as c_count, sum("preco") as c_sum, sum("valor") as c_sum_double, '
        f"'literal' as c_text, 1.5 as c_numeric "
        f"from {target.qualified('cad_lancamentos_projetados')}")
    record("redshift.engine.row_desc", {name: str(kind) for name, kind
                                        in zip(described.schema.names, described.schema.types)})
    assert described.column("c_count").to_pylist() == [4]
    assert pa.types.is_decimal(described.column("c_sum").type)


@pytest.mark.redshift
@pytest.mark.s3
def test_small_load_copy_cost(target: Target) -> None:
    """O tempo de um ``load`` de 10 linhas pelo ``loader``, como leitura, nunca como reprovação."""
    engine = target.engine
    name = f"{engine.prefix}cad_contas"
    accounts = schema.cast(pa.table({"id_conta": pa.array(range(1, 11), pa.int64()),
                                     "numero": [f"C{k}" for k in range(10)]}), ACCOUNTS)
    best = None
    for _ in range(3):
        started = time.perf_counter()
        engine.load(ACCOUNTS, accounts)
        elapsed = time.perf_counter() - started
        best = elapsed if best is None else min(best, elapsed)
        engine.execute(f"DROP TABLE {engine.qualified(name)}")
        engine._created.remove(name)
    record("redshift.engine.small_load", f"{best:.2f} s")
