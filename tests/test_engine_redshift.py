"""``serialize_db.engine.redshift``: o motor Redshift, sem conexão e sobre uma amostra no esquema.

Os casos sem conexão conferem o texto de cada comando, a configuração, o prefixo, a cláusula de
credenciais e a máscara, o esquema de um resultado pelo ``row_desc``, a tabela montada por colunas,
os valores do cliente como literais, o ``:`` dentro das aspas e o guarda do ``bindparam`` sem
valor, o ``stream`` vazio, a sessão única, a reconexão e o ``COMMIT`` que não reconecta, a lista de
colunas do ``COPY`` do appender e o segundo ``close``, e a troca da partição com ``Double`` não
finito, sobre uma conexão de mentira que registra os comandos, responde ao que o motor pergunta e
grava o arquivo de um ``UNLOAD`` numa pasta local (os que gravam são ``local``, sob
``SERIALIZE_DB_TEST_LOCAL_ROOT``). Os casos marcados ``redshift`` repetem a sequência, e leem dois
escritores na mesma tabela, com uma amostra no esquema de ``SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`` e
arquivos sob ``SERIALIZE_DB_TEST_S3_ROOT``: no ambiente alvo pela conexão de
``examples/redshift_native.py``, e no substituto local (``SERIALIZE_DB_TEST_EMULATOR``) pela conexão
de ``tests/emulator.py``, que os testes dão ao motor no lugar do ``redshift_connector``. O modelo é
o de ``Lancamento``, particionado por ``data_base_str``, com uma chave estrangeira para ``Conta``,
uma coluna JSON, uma ``DateTime`` e a coluna ``to``, palavra reservada, e ``Projetado``, a tabela
que o pipeline grava (``tests/lancamentos_model.py``), e ``cad_medidas``, sem JSON, com uma coluna
anulável no meio; ``cad_colunas``, só de texto, recebe uma coluna nova no meio ou troca duas de
lugar depois de uma partição gravada, e o ``ingest`` e o ``pinned_delta`` põem cada valor na coluna
de mesmo nome.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import io
import itertools
import json
import logging
import re
import threading
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
import redshift_connector
import sqlalchemy as sa
from deltalake import write_deltalake

from conftest import LocalLocation, S3Location, record, redshift_config
from lancamentos_model import ACCOUNTS, ENTRIES, MONTHS, PROJECTED, account_rows, entry_rows
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
CONFIG = RedshiftConfig(
    host="host",
    user="usuario",
    password="senha",
    database="dev",
    share_database="compartilhado",
    schema="esquema",
    region="sa-east-1",
    iam_role="default",
)

# Uma tabela sem coluna JSON com uma coluna anulável no meio, que o lote do appender pode não
# trazer.
MEASURES = sa.Table(
    "cad_medidas",
    sa.MetaData(),
    sa.Column("id_medida", sa.BigInteger, primary_key=True, autoincrement=False),
    sa.Column("altura", sa.BigInteger),
    sa.Column("largura", sa.BigInteger),
)


def text_columns_table(
    *names: str,
) -> sa.Table:
    """``cad_colunas``, particionada por ``parte``, com as colunas de texto ``names`` entre a chave
    ``id`` e a partição, na ordem pedida."""
    columns = [sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=False)]
    for name in names:
        columns.append(sa.Column(name, sa.String(10)))
    columns.append(sa.Column("parte", sa.String(10), nullable=False))
    return sa.Table(
        "cad_colunas", sa.MetaData(), *columns, info={"serialize_db": {"partition_by": ["parte"]}}
    )


def text_columns_rows(
    table: sa.Table,
    value: str,
    ids: list[int],
) -> pa.Table:
    """As linhas de ``cad_colunas`` na partição ``value``, no contrato: cada coluna de texto com o
    nome dela seguido do id, como ``a1``."""
    data = {"id": pa.array(ids, pa.int64())}
    for column in table.columns:
        if column.name not in ("id", "parte"):
            data[column.name] = pa.array([f"{column.name}{row_id}" for row_id in ids])
    data["parte"] = pa.array([value] * len(ids))
    return schema.cast(pa.table(data), table)


# O OID e o type_modifier de cada tipo Arrow, como o row_desc do driver os traz.
OID_OF = {
    pa.bool_(): 16,
    pa.int16(): 21,
    pa.int32(): 23,
    pa.int64(): 20,
    pa.float32(): 700,
    pa.float64(): 701,
    pa.string(): 1043,
    pa.date32(): 1082,
    pa.timestamp("us"): 1114,
    pa.timestamp("us", "UTC"): 1184,
}


def row_desc_of(
    arrow_schema: pa.Schema,
) -> list[dict]:
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


def server_error(
    message: str,
    code: str = "XX000",
) -> redshift_connector.ProgrammingError:
    """O erro do driver com os campos do servidor."""
    return redshift_connector.ProgrammingError({"S": "ERROR", "C": code, "M": message})


# ---------------------------------------------------------------- a conexão de mentira


class FakeCursor:
    """O cursor de uma ``FakeConnection``: ``execute`` pergunta à conexão, que responde as linhas
    e o ``row_desc``."""

    def __init__(
        self,
        connection: FakeConnection,
    ) -> None:
        self.connection = connection
        self.paramstyle = "format"
        self.description: list | None = None
        self.rowcount = -1
        self.ps: dict = {"row_desc": []}
        self._rows: list = []

    def execute(
        self,
        text: str,
        params: object = None,
    ) -> FakeCursor:
        row_desc, rows = self.connection.answer(text, params)
        self.ps = {"row_desc": row_desc}
        self.description = None
        if row_desc:
            self.description = [
                (field["label"].decode(), field["type_oid"], None, None, None, None, None)
                for field in row_desc
            ]
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
    ``unload_rows`` num Parquet da pasta local com o manifesto, como o Redshift faria; com
    ``fail``, cada comando recebe esse erro."""

    def __init__(
        self,
        storage: Storage | None = None,
        unload_rows: pa.Table | None = None,
        existing: set[str] = frozenset(),
        delay: float = 0.0,
        drop_next: int = 0,
    ) -> None:
        self.storage = storage
        self.unload_rows = unload_rows if unload_rows is not None else pa.table({})
        self.existing = set(existing)
        self.delay = delay
        self.drop_next = drop_next
        self.commands: list[Command] = []
        self.last_unload_count = 0
        self.write_manifest = True
        self.fail: Exception | None = None
        self.autocommit = False
        self.closed = False

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)

    def close(self) -> None:
        self.closed = True

    def answer(
        self,
        text: str,
        params: object,
    ) -> tuple[list, list]:
        """A resposta de um comando: as linhas de uma consulta conhecida, ou nada."""
        command = Command(text, params, time.perf_counter())
        self.commands.append(command)
        if self.drop_next > 0:
            self.drop_next -= 1
            raise redshift_connector.InterfaceError("BrokenPipe: server socket closed")
        time.sleep(self.delay)
        try:
            if self.fail is not None:
                raise self.fail
            return self._answer(text)
        finally:
            command.finished = time.perf_counter()

    def _answer(
        self,
        text: str,
    ) -> tuple[list, list]:
        first = text.split(None, 1)[0].upper()
        if first == "UNLOAD":
            self._unload(text)
            return [], []
        existence = re.fullmatch(r'SELECT 1 FROM "esquema"\."(\w+)" LIMIT 0', text)
        if existence is not None:
            if existence.group(1) in self.existing:
                return [], []
            raise server_error(f"Relation {existence.group(1)} does not exist in the database.")
        if text == "SELECT pg_last_unload_count()":
            count_schema = pa.schema([("pg_last_unload_count", pa.int64())])
            return row_desc_of(count_schema), [[self.last_unload_count]]
        if text.startswith("SELECT * FROM (") and text.endswith(") AS t LIMIT 0"):
            return row_desc_of(self.unload_rows.schema), []
        if text.startswith("SELECT count(*) FROM"):
            count_schema = pa.schema([("count", pa.int64())])
            return row_desc_of(count_schema), [[self.unload_rows.num_rows]]
        if first == "CREATE":
            self.existing.add(re.search(r'"(\w+)"', text).group(1))
        return [], []

    def _unload(
        self,
        text: str,
    ) -> None:
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
        entry = {
            "url": self.storage.uri_of(path),
            "meta": {
                "content_length": len(buffer.getvalue()),
                "record_count": self.unload_rows.num_rows,
            },
        }
        self.storage.write_text(
            self.storage.join(prefix, "manifest"), json.dumps({"entries": [entry]})
        )

    def texts(self) -> list[str]:
        """Os comandos registrados, mascarados."""
        return [mask(command.text) for command in self.commands]


def fake_engine(
    monkeypatch: pytest.MonkeyPatch,
    connection: FakeConnection,
    storage: Storage | None = None,
) -> RedshiftEngine:
    """O motor sobre a conexão de mentira, com o ``staging/`` da execução na raiz local."""
    monkeypatch.setattr(redshift, "driver_connect", lambda login: connection)
    root = storage if storage is not None else Storage.for_uri("/tmp/sem-uso")
    return RedshiftEngine(CONFIG, EXECUTION_ID, root, f"prd/staging/{EXECUTION_ID}")


# ---------------------------------------------------------------- a configuração e os textos


def test_config_from_environment(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``SERIALIZE_DB_REDSHIFT_*`` para ``RedshiftConfig``; a variável vazia é ausente; o par
    informado e o workgroup são os caminhos de conexão, e sem os dois a conexão é recusada."""
    variables = {
        "SERIALIZE_DB_REDSHIFT_WORKGROUP": "wg",
        "SERIALIZE_DB_REDSHIFT_DATABASE": "banco",
        "SERIALIZE_DB_REDSHIFT_SHARE_DATABASE": "compartilhado",
        "SERIALIZE_DB_REDSHIFT_SCHEMA": "esquema",
        "SERIALIZE_DB_REDSHIFT_IAM_ROLE": "",
        "SERIALIZE_DB_REDSHIFT_PORT": "5440",
        "AWS_DEFAULT_REGION": "sa-east-1",
    }
    config = RedshiftConfig.from_environment(variables)
    assert config == RedshiftConfig(
        workgroup="wg",
        database="banco",
        share_database="compartilhado",
        schema="esquema",
        port=5440,
        region="sa-east-1",
    )
    assert RedshiftConfig.from_environment({}) == RedshiftConfig()
    assert redshift.login_of(CONFIG) == {
        "host": "host",
        "port": 5439,
        "user": "usuario",
        "password": "senha",
    }
    with pytest.raises(ContractError, match="workgroup"):
        redshift.login_of(RedshiftConfig())


def test_sandbox_prefix_normalizes_and_limits() -> None:
    """``[a-z0-9_]`` e o espaço para o nome da tabela dentro dos 127 bytes."""
    assert sandbox_prefix("exec-2026-09-05") == PREFIX
    assert sandbox_prefix("Correção.Agosto") == "exec_corre__o_agosto_"
    assert sandbox_prefix("x" * 58).endswith("_")
    with pytest.raises(ContractError, match="127"):
        sandbox_prefix("x" * 59)


def test_credentials_clause_and_mask(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """``IAM_ROLE`` com ARN e ``default``; as três chaves da sessão sem ``iam_role``; ``mask``
    tira os valores, e a nota de um erro leva o comando mascarado."""
    assert redshift.credentials_clause(CONFIG) == "IAM_ROLE default"
    arn = "arn:aws:iam::123456789012:role/papel"
    with_arn = dataclasses.replace(CONFIG, iam_role=arn)
    assert redshift.credentials_clause(with_arn) == f"IAM_ROLE '{arn}'"
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "AKIACHAVE")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "segredo")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "token")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    clause = redshift.credentials_clause(dataclasses.replace(CONFIG, iam_role=None))
    assert clause == "ACCESS_KEY_ID 'AKIACHAVE' SECRET_ACCESS_KEY 'segredo' SESSION_TOKEN 'token'"
    assert mask(clause) == "ACCESS_KEY_ID '***' SECRET_ACCESS_KEY '***' SESSION_TOKEN '***'"
    assert "segredo" not in mask(redshift.copy_text("t", "s3://b/m", clause, manifest=True))

    # O erro do servidor leva o comando mascarado na nota.
    connection = FakeConnection()
    engine = fake_engine(monkeypatch, connection)
    text = redshift.copy_text('"esquema"."x"', "s3://b/m", clause, manifest=True)
    connection.fail = server_error("Spectrum Scan Error")
    with pytest.raises(redshift_connector.ProgrammingError) as failure:
        engine.execute(text)
    assert failure.value.__notes__ == [f"comando: {mask(text)}"]
    assert "segredo" not in str(failure.value.__notes__)


def test_copy_insert_unload_text() -> None:
    """``COPY ... FORMAT AS PARQUET MANIFEST FILLRECORD`` sem ``COMPUPDATE``, e com a lista de
    colunas entre aspas quando informada; o ``INSERT`` com a lista de colunas, o valor da partição
    no lugar dela e ``JSON_PARSE`` no JSON; o ``UNLOAD`` com manifesto verboso, sem
    ``PARTITION BY``, ``PARALLEL OFF`` opcional e o ``select`` com a aspa e a contrabarra
    dobradas; nomes em duas partes."""
    credentials = "IAM_ROLE default"
    copied = redshift.copy_text(
        '"esquema"."t_staging"', "s3://b/prd/staging/e/t/m.manifest", credentials, manifest=True
    )
    assert copied == (
        'COPY "esquema"."t_staging"\nFROM \'s3://b/prd/staging/e/t/m.manifest\'\n'
        "IAM_ROLE default\nFORMAT AS PARQUET MANIFEST FILLRECORD"
    )
    assert redshift.copy_text("t", "s3://b/f.parquet", credentials, manifest=False).endswith(
        "FORMAT AS PARQUET FILLRECORD"
    )
    assert "COMPUPDATE" not in copied

    # A lista de colunas, na ordem do arquivo, entre a tabela e o FROM.
    listed = redshift.copy_text(
        '"esquema"."t"',
        "s3://b/f.parquet",
        credentials,
        manifest=False,
        columns=["id_lancamento", "to"],
    )
    assert listed == (
        'COPY "esquema"."t" ("id_lancamento", "to")\nFROM \'s3://b/f.parquet\'\n'
        "IAM_ROLE default\nFORMAT AS PARQUET FILLRECORD"
    )

    # O INSERT da staging com a lista de colunas e o valor da partição.
    inserted = redshift.insert_from_staging(
        '"esquema"."t"', '"esquema"."t_staging"', ENTRIES, "2026-08-31"
    )
    columns = ", ".join(f'"{name}"' for name in ENTRIES.c.keys())
    assert inserted == (
        f'INSERT INTO "esquema"."t" ({columns})\n'
        'SELECT "id_lancamento", "id_conta", "data_base", "carimbo", "valor", "preco", "area", '
        'JSON_PARSE("meta"), "to", "codigo", \'2026-08-31\' FROM "esquema"."t_staging"'
    )
    # Sem o valor, a coluna de partição vem da staging: o appender.
    assert '"codigo", "data_base_str" FROM' in redshift.insert_from_staging("t", "s", ENTRIES, None)

    # O UNLOAD com as aspas e a barra do literal dobradas.
    select = "select \"texto\" from \"t\" where \"texto\" = 'd''agua' and x = 'barra \\\\ n'"
    unloaded = redshift.unload_text(
        select, "s3://b/prd/t/data_base_str=2026-08-31/e_1", credentials, parallel=False
    )
    assert unloaded == (
        "UNLOAD ('select \"texto\" from \"t\" where \"texto\" = ''d''''agua'' "
        "and x = ''barra \\\\\\\\ n''')\n"
        "TO 's3://b/prd/t/data_base_str=2026-08-31/e_1/'\nIAM_ROLE default\n"
        "FORMAT AS PARQUET MANIFEST VERBOSE PARALLEL OFF"
    )
    assert "PARTITION BY" not in unloaded
    assert redshift.unload_text(select, "s3://b/x/", credentials, parallel=True).endswith(
        "MANIFEST VERBOSE"
    )


def test_staging_ddl_without_partition_column() -> None:
    """A staging sem a coluna de partição, anulável, com o JSON em ``VARCHAR(65535)`` e sem
    cláusula física; a tabela do sandbox com ela, pelo ``ddl`` do modelo."""
    staging = redshift.staging_ddl(
        ENTRIES, '"esquema"."t_staging"', redshift.columns_without_partition(ENTRIES)
    )
    assert staging.startswith('CREATE TABLE "esquema"."t_staging" (\n    "id_lancamento" BIGINT,')
    assert '"meta" VARCHAR(65535)' in staging
    assert '"carimbo" TIMESTAMP' in staging
    assert "data_base_str" not in staging
    assert "NOT NULL" not in staging
    assert "SORTKEY" not in staging
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
    arrow_schema = pa.schema(
        [
            ("id", pa.int64()),
            ("valor", pa.decimal128(18, 2)),
            ("dia", pa.date32()),
            ("area", pa.string()),
            ("carimbo", pa.timestamp("us")),
        ]
    )
    rows = []
    for day in range(3):
        rows.append(
            [
                day,
                decimal.Decimal(day) / 4,
                datetime.date(2026, 8, 28 + day),
                f"area {day}",
                datetime.datetime(2026, 8, 28 + day, 12),
            ]
        )
    connection = FakeConnection()
    cursor = connection.cursor()
    cursor.ps = {"row_desc": row_desc_of(arrow_schema)}
    cursor.description = [(field.name, 0, None, None, None, None, None) for field in arrow_schema]
    cursor._rows = [list(row) for row in rows]
    table = redshift.table_from_cursor(cursor)
    assert table.schema.equals(arrow_schema)
    names = arrow_schema.names
    by_dicts = pa.Table.from_pylist([dict(zip(names, row)) for row in rows], schema=arrow_schema)
    assert table.equals(by_dicts)

    # O resultado vazio fica com o esquema; sem description, sem coluna.
    cursor._rows = []
    empty = redshift.table_from_cursor(cursor)
    assert empty.num_rows == 0
    assert empty.schema.equals(arrow_schema)
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
    expected = pa.schema(
        [
            ("c_bigint", pa.int64()),
            ("c_integer", pa.int32()),
            ("c_smallint", pa.int16()),
            ("c_double", pa.float64()),
            ("c_real", pa.float32()),
            ("c_decimal", pa.decimal128(18, 2)),
            ("c_sum", pa.decimal128(38, 2)),
            ("c_varchar", pa.string()),
            ("c_char", pa.string()),
            ("c_text", pa.string()),
            ("c_unknown", pa.string()),
            ("c_name", pa.string()),
            ("c_date", pa.date32()),
            ("c_timestamp", pa.timestamp("us")),
            ("c_timestamptz", pa.timestamp("us", "UTC")),
            ("c_boolean", pa.bool_()),
            ("c_super", pa.string()),
        ]
    )
    assert schema_from_row_description(row_desc).equals(expected)
    with pytest.raises(SandboxError, match="c_geo.*OID 3000"):
        schema_from_row_description([{"label": b"c_geo", "type_oid": 3000, "type_modifier": -1}])
    with pytest.raises(SandboxError, match="c_num"):
        schema_from_row_description([{"label": b"c_num", "type_oid": 1700, "type_modifier": -1}])


def test_stream_literal_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """O texto do ``UNLOAD`` de um statement com texto, data, número e ``IN`` de lista, com o ``%``
    sem dobrar e o prefixo do sandbox; o texto pronto com os ``bindparam`` tipados pelo valor; um
    ``bindparam`` sem valor, também num ``IN`` de lista, recusado antes de qualquer comando."""
    statement = (
        sa.select(ENTRIES.c.id_lancamento, ENTRIES.c.area)
        .where(
            ENTRIES.c.area.like(sa.bindparam("padrao")),
            ENTRIES.c.data_base >= sa.bindparam("dia"),
            ENTRIES.c.preco > sa.bindparam("preco"),
            ENTRIES.c.id_lancamento.in_(sa.bindparam("ids", expanding=True)),
        )
        .order_by(ENTRIES.c.id_lancamento)
    )
    params = {
        "padrao": "50% d'agua \\ n",
        "dia": datetime.date(2026, 8, 29),
        "preco": decimal.Decimal("1.00"),
        "ids": [1, 2, 3],
    }
    literal = redshift.literal_text(statement, params, PREFIX)
    assert f'FROM "{PREFIX}cad_lancamentos"' in literal
    assert "LIKE '50% d''agua \\\\ n'" in literal
    assert "\"data_base\" >= '2026-08-29'" in literal
    assert '"preco" > 1.00' in literal
    assert '"id_lancamento" IN (1, 2, 3)' in literal
    assert ":" not in literal.split("FROM")[1]

    # O texto com o sentinela e os parâmetros nomeados.
    text = (
        'SELECT "id_lancamento" FROM "{prefix}cad_lancamentos" WHERE "area" = :area '
        'AND "id_lancamento" IN :ids AND "data_base" > :dia'
    )
    from_text = redshift.literal_text(
        text, {"area": "a'b", "ids": [4, 5], "dia": datetime.date(2026, 1, 1)}, PREFIX
    )
    assert from_text == (
        f'SELECT "id_lancamento" FROM "{PREFIX}cad_lancamentos" WHERE "area" = '
        "'a''b' AND \"id_lancamento\" IN (4, 5) AND \"data_base\" > '2026-01-01'"
    )

    # Os parâmetros sem valor são SqlError antes de qualquer comando.
    connection = FakeConnection()
    engine = fake_engine(monkeypatch, connection)
    before = len(connection.commands)
    with pytest.raises(SqlError, match="padrao"):
        engine.stream(
            statement,
            {"dia": datetime.date(2026, 8, 29), "preco": decimal.Decimal("1"), "ids": [1]},
        )
    with pytest.raises(SqlError, match="ids"):
        engine.stream(
            sa.select(ENTRIES).where(
                ENTRIES.c.id_lancamento.in_(sa.bindparam("ids", expanding=True))
            )
        )
    with pytest.raises(SqlError, match="area"):
        engine.stream(text, {"ids": [1], "dia": datetime.date(2026, 1, 1)})
    assert len(connection.commands) == before

    # O query: os valores como parâmetros do driver, no estilo named, e o IN expandido.
    compiled, values = redshift.compiled_for_cursor(statement, params, PREFIX)
    assert set(values) == {"padrao", "dia", "preco", "ids_1", "ids_2", "ids_3"}
    assert "(:ids_1, :ids_2, :ids_3)" in compiled
    assert ":padrao" in compiled
    text_params = {"area": "x", "ids": [1], "dia": datetime.date(2026, 1, 1)}
    bound, bound_values = redshift.compiled_for_cursor(text, text_params, PREFIX)
    assert f'"{PREFIX}cad_lancamentos"' in bound
    assert bound_values["area"] == "x"


def test_literal_text_keeps_the_colons_of_quoted_regions() -> None:
    """Um ``:nome`` dentro de um literal de texto ou de um nome entre aspas, e a contrabarra antes
    de um ``:``, ficam como estão no texto do ``UNLOAD``, como no texto do cursor; o marcador fora
    das aspas recebe o valor."""
    texts = [
        "SELECT 'a :b' AS c",
        """SELECT JSON_PARSE('{"k":1}') AS doc""",
        'SELECT "taxa :base" FROM "t"',
        "SELECT '12:30:00' AS t, 'a\\\\:b' AS u",
    ]
    for text in texts:
        assert redshift.literal_text(text, None, PREFIX) == text
        assert redshift.compiled_for_cursor(text, None, PREFIX) == (text, {})

    # O marcador fora das aspas vira o literal, e o de dentro fica.
    text = (
        'SELECT "id_lancamento" FROM "{prefix}cad_lancamentos" '
        'WHERE "codigo" = \'ref :x1\' AND "id_lancamento" = :id'
    )
    assert redshift.literal_text(text, {"id": 5}, PREFIX) == (
        f'SELECT "id_lancamento" FROM "{PREFIX}cad_lancamentos" '
        'WHERE "codigo" = \'ref :x1\' AND "id_lancamento" = 5'
    )
    bound, values = redshift.compiled_for_cursor(text, {"id": 5}, PREFIX)
    assert "'ref :x1'" in bound
    assert values == {"id": 5}


def test_literal_text_keeps_the_backslash_before_a_colon_in_a_value() -> None:
    """O valor do cliente com contrabarra antes de ``:`` chega ao literal do ``UNLOAD`` com o
    mesmo valor: o compilador desfaz o ``\\:`` do texto também nos valores já renderizados, e a
    contrabarra a mais que o valor leva compensa. O Redshift lê ``\\\\`` como uma contrabarra e
    ``\\:`` como ``:``."""
    literal_by_value = {
        r"a\:b": r"'a\\\:b'",
        r"a\\:b": r"'a\\\\\:b'",
        r"a\: b": r"'a\\\: b'",
        r"a\::b": r"'a\\::b'",
    }
    for value, literal in literal_by_value.items():
        assert redshift.literal_text("SELECT :v AS x", {"v": value}, PREFIX) == (
            f"SELECT {literal} AS x"
        )
    # Na lista do IN, cada item.
    assert redshift.literal_text("SELECT 1 WHERE 'x' IN :v", {"v": [r"a\:b", "c"]}, PREFIX) == (
        r"SELECT 1 WHERE 'x' IN ('a\\\:b', 'c')"
    )


@pytest.mark.local
def test_stream_empty_result(
    monkeypatch: pytest.MonkeyPatch,
    local_location: LocalLocation,
) -> None:
    """Uma conexão de mentira em que o ``UNLOAD`` não grava manifesto: com
    ``pg_last_unload_count()`` em 0, o ``stream`` sai sem lote e com o esquema do ``row_desc``; com
    2, a falta do manifesto sobe com a contagem."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    empty = entry_rows(MONTHS[0], 1, 0)
    connection = FakeConnection(storage, unload_rows=empty)
    engine = fake_engine(monkeypatch, connection, storage)
    with engine.stream(sa.select(ENTRIES)) as stream:
        assert stream.schema.names == ENTRIES.c.keys()
        assert stream.schema.field("preco").type == pa.decimal128(18, 2)
        assert stream.schema.field("carimbo").type == pa.timestamp("us")
        assert list(stream) == []
        assert stream.read_all().num_rows == 0
    texts = connection.texts()
    assert texts[-3].startswith("SELECT * FROM (SELECT")
    assert texts[-3].endswith(") AS t LIMIT 0")
    assert texts[-2].startswith("UNLOAD ('SELECT")
    assert "PARALLEL OFF" in texts[-2]
    assert texts[-1] == "SELECT pg_last_unload_count()"

    # Linhas descarregadas sem manifesto: FileNotFoundError.
    connection.unload_rows = entry_rows(MONTHS[0], 1, 2)
    connection.write_manifest = False
    with pytest.raises(FileNotFoundError, match="2 linha"):
        engine.stream(sa.select(ENTRIES))


@pytest.mark.local
def test_stream_reads_the_unloaded_file_in_the_statement_schema(
    monkeypatch: pytest.MonkeyPatch,
    local_location: LocalLocation,
) -> None:
    """Os lotes vêm do arquivo do ``UNLOAD``, com o ``INT96`` em microssegundos e cada lote no
    esquema do ``row_desc``; ``close`` apaga o prefixo do stream, e ``read_all`` dá as linhas."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    rows = entry_rows(MONTHS[0], 1, 250)
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
def test_statements_serialize_on_the_single_session(
    monkeypatch: pytest.MonkeyPatch,
    local_location: LocalLocation,
) -> None:
    """Dois comandos de duas threads não se sobrepõem; um comando roda enquanto um ``stream`` ainda
    lê os arquivos, porque o lock solta no fim do ``UNLOAD``; um ``stream`` aberto dentro de
    ``session()``, na mesma thread, não trava; e ``new_session`` abre outra conexão."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    connection = FakeConnection(storage, unload_rows=entry_rows(MONTHS[0], 1, 300), delay=0.05)
    engine = fake_engine(monkeypatch, connection, storage)
    assert isinstance(engine, Engine)

    # Três comandos de três threads.
    threads = []
    for number in range(3):
        threads.append(threading.Thread(target=engine.query, args=(f"SELECT {number}",)))
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    # Cada comando termina antes de o seguinte começar.
    windows = sorted((command.started, command.finished) for command in connection.commands[-3:])
    for previous, following in itertools.pairwise(windows):
        assert previous[1] <= following[0]

    # Um comando enquanto o stream ainda lê os arquivos.
    with engine.stream(sa.select(ENTRIES), batch_size=10) as stream:
        first = stream.read_next_batch()
        started = time.perf_counter()
        engine.query("SELECT 1")
        assert time.perf_counter() - started < 1.0
        assert first.num_rows == 10
        assert sum(batch.num_rows for batch in stream) == 290

    # Um stream dentro de session(), na mesma thread.
    with engine.session():
        with engine.stream(sa.select(ENTRIES)) as inner:
            assert inner.read_all().num_rows == 300

    # new_session abre outra conexão, com o USE e o search_path, e a fecha na saída.
    connections = []

    def open_fake(
        login: dict,
    ) -> FakeConnection:
        connections.append(FakeConnection(storage))
        return connections[-1]

    monkeypatch.setattr(redshift, "driver_connect", open_fake)
    with engine.new_session() as other:
        other.query("SELECT 2")
    assert len(connections) == 1
    assert connections[0].closed
    assert connections[0].texts() == ["USE compartilhado", "SET search_path TO esquema", "SELECT 2"]


def test_connection_dropped_by_the_server_is_reopened_once(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Uma conexão derrubada (``InterfaceError``) é reaberta uma vez, com o ``USE`` e o
    ``search_path``, e o comando repetido; dentro de uma transação o erro sobe, com o
    ``ROLLBACK`` tentado."""
    connections = [FakeConnection(), FakeConnection()]
    monkeypatch.setattr(redshift, "driver_connect", lambda login: connections.pop(0))
    engine = RedshiftEngine(CONFIG, EXECUTION_ID, Storage.for_uri("/tmp/sem-uso"), "prd/staging")
    first = engine._connection
    first.drop_next = 1
    with caplog.at_level(logging.WARNING, logger="serialize_db.engine.redshift"):
        engine.query("SELECT 1")
    assert first.closed
    assert engine._connection is not first
    assert engine._connection.texts() == [
        "USE compartilhado",
        "SET search_path TO esquema",
        "SELECT 1",
    ]
    assert "derrubada" in caplog.text

    # Dentro de uma transação o erro sobe, com o ROLLBACK tentado.
    with pytest.raises(redshift_connector.InterfaceError):
        with engine.transaction():
            engine._connection.drop_next = 1
            engine.execute("SELECT 2")
    assert engine._connection.texts()[-3:] == ["BEGIN", "SELECT 2", "ROLLBACK"]


def test_connection_dropped_at_commit_raises_without_reconnecting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Uma conexão derrubada no ``COMMIT``: o ``InterfaceError`` sobe da transação, porque o
    ``COMMIT`` roda dentro dela e o resultado dele é desconhecido, e nenhum comando vai a uma
    segunda conexão; fechada a transação, a conexão derrubada volta a ser reaberta."""
    connections = [FakeConnection(), FakeConnection()]
    monkeypatch.setattr(redshift, "driver_connect", lambda login: connections.pop(0))
    engine = RedshiftEngine(CONFIG, EXECUTION_ID, Storage.for_uri("/tmp/sem-uso"), "prd/staging")
    first = engine._connection
    insert = 'INSERT INTO "esquema"."t" VALUES (1)'
    with pytest.raises(redshift_connector.InterfaceError):
        with engine.transaction():
            engine.execute(insert)
            first.drop_next = 1
    assert engine._connection is first
    assert len(connections) == 1
    assert first.texts()[-3:] == ["BEGIN", insert, "COMMIT"]

    # Fora da transação, o comando na conexão derrubada vai à conexão reaberta.
    first.drop_next = 1
    engine.execute("SELECT 1")
    assert connections == []
    assert engine._connection.texts()[-1] == "SELECT 1"


@pytest.mark.local
def test_create_table_and_appender_write_the_file_and_copy_in_a_transaction(
    monkeypatch: pytest.MonkeyPatch,
    local_location: LocalLocation,
) -> None:
    """``create_table`` roda o DDL do modelo e anota a tabela, e recusa o nome ocupado; o appender
    recusa a tabela que não existe; o arquivo nasce no ``staging/`` na thread auxiliar; ``close``
    grava ao lado dele o manifesto com o arquivo como a única entrada, obrigatória e com o
    tamanho gravado, roda ``BEGIN``, a staging temporária com o ``COPY ... MANIFEST`` e o
    ``INSERT`` com ``JSON_PARSE``, e ``COMMIT``, e apaga o arquivo e o manifesto; o appender sem
    lote roda só a conferência da tabela; uma exceção no ``with`` não roda comando algum; um lote
    recusado pelo ``cast`` também não."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    connection = FakeConnection(storage, existing={f"{PREFIX}cad_lancamentos"})
    engine = fake_engine(monkeypatch, connection, storage)
    with pytest.raises(SandboxError, match="ocupado"):
        engine.create_table(ENTRIES)
    with pytest.raises(SandboxError, match="create_table"):
        engine.appender(PROJECTED)

    # create_table roda o DDL, fora de transação, e anota a tabela para o cleanup.
    engine.create_table(PROJECTED)
    created = connection.texts()[-1]
    assert created.startswith(f'CREATE TABLE "{PREFIX}cad_lancamentos_projetados" (')
    assert f"{PREFIX}cad_lancamentos_projetados" in engine._created

    # O close carrega a tabela numa transação, pelo manifesto. O dublê lê o manifesto e o tamanho
    # do arquivo antes do COPY, e chama a função guardada antes da troca.
    original_write_file_manifest = redshift._write_file_manifest
    manifests = []

    def reading_manifest(
        sink: object,
        manifest_path: str,
    ) -> str:
        uri = original_write_file_manifest(sink, manifest_path)
        manifest_text, _ = storage.read_text(manifest_path)
        manifests.append(
            {
                "manifest": json.loads(manifest_text),
                "uri": uri,
                "manifest_path": manifest_path,
                "file_path": sink.path,
                "file_size": storage.size(sink.path),
            }
        )
        return uri

    monkeypatch.setattr(redshift, "_write_file_manifest", reading_manifest)
    with engine.appender(PROJECTED) as appender:
        appender.write(entry_rows(MONTHS[0], 1, 10, PROJECTED))
        appender.write(entry_rows(MONTHS[0], 11, 5, PROJECTED).to_batches()[0])
    assert appender.rows == 15
    assert storage.list_files(f"prd/staging/{EXECUTION_ID}") == []
    [written] = manifests
    entry = {
        "url": storage.uri_of(written["file_path"]),
        "mandatory": True,
        "meta": {"content_length": written["file_size"]},
    }
    assert written["manifest"] == {"entries": [entry]}
    assert written["file_size"] > 0
    assert written["uri"] == storage.uri_of(written["manifest_path"])
    stem = written["file_path"].removesuffix(".parquet")
    assert written["manifest_path"] == f"{stem}.manifest"
    texts = connection.texts()
    start = texts.index("BEGIN")
    carga = f"{PREFIX}cad_lancamentos_projetados_carga"
    columns = ", ".join(f'"{name}"' for name in PROJECTED.c.keys())
    assert texts[start + 1].startswith(f'CREATE TEMP TABLE "{carga}"')
    assert texts[start + 2].startswith(f"COPY \"{carga}\" ({columns})\nFROM '{written['uri']}'\n")
    assert "FORMAT AS PARQUET MANIFEST FILLRECORD" in texts[start + 2]
    assert texts[start + 3].startswith(
        f'INSERT INTO "esquema"."{PREFIX}cad_lancamentos_projetados" ('
    )
    assert 'JSON_PARSE("meta")' in texts[start + 3]
    assert texts[start + 4 : start + 6] == [f'DROP TABLE "{carga}"', "COMMIT"]

    # A tabela sem coluna JSON recebe o COPY direto.
    engine.create_table(ACCOUNTS)
    engine.append(ACCOUNTS, account_rows(["A"]))
    assert connection.texts()[-2].startswith(
        f'COPY "esquema"."{PREFIX}cad_contas" ("id_conta", "numero")\nFROM '
    )

    # O appender sem lote: só a conferência da tabela, sem BEGIN nem COPY.
    before = len(connection.commands)
    with engine.appender(PROJECTED):
        pass
    assert connection.texts()[before:] == [
        f'SELECT 1 FROM "esquema"."{PREFIX}cad_lancamentos_projetados" LIMIT 0'
    ]

    # Uma exceção dentro do with: o arquivo sai e nada roda além da conferência da tabela.
    before = len(connection.commands)
    with pytest.raises(RuntimeError, match="plantado"):
        with engine.appender(PROJECTED) as appender:
            appender.write(entry_rows(MONTHS[0], 1, 10, PROJECTED))
            raise RuntimeError("erro plantado")
    assert connection.texts()[before:] == [
        f'SELECT 1 FROM "esquema"."{PREFIX}cad_lancamentos_projetados" LIMIT 0'
    ]
    assert storage.list_files(f"prd/staging/{EXECUTION_ID}") == []

    # O lote recusado pelo cast.
    with pytest.raises(ContractError):
        with engine.appender(PROJECTED) as appender:
            appender.write(pa.table({"id_lancamento": ["x"]}))
    assert "BEGIN" not in connection.texts()[before + 1 :]

    # O DataFrame é recusado antes de qualquer gravação.
    with pytest.raises(ContractError, match="from_pandas"):
        engine.append(PROJECTED, {"id_lancamento": [1]})


@pytest.mark.local
def test_appender_copy_lists_the_file_columns(
    monkeypatch: pytest.MonkeyPatch,
    local_location: LocalLocation,
) -> None:
    """O ``COPY`` do appender lista as colunas do arquivo, as do primeiro lote na ordem do
    contrato, no ``COPY`` direto e no da staging ``_carga`` da tabela com JSON: o lote sem uma
    coluna anulável do meio não desloca as seguintes."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    connection = FakeConnection(storage)
    engine = fake_engine(monkeypatch, connection, storage)

    # O COPY direto, sem a coluna altura.
    engine.create_table(MEASURES)
    widths = pa.table(
        {"id_medida": pa.array([1], pa.int64()), "largura": pa.array([10], pa.int64())}
    )
    engine.append(MEASURES, widths)
    copied = [text for text in connection.texts() if text.startswith("COPY")][-1]
    assert copied.startswith(
        f'COPY "esquema"."{PREFIX}cad_medidas" ("id_medida", "largura")\nFROM '
    )
    assert copied.endswith("FORMAT AS PARQUET MANIFEST FILLRECORD")

    # A staging _carga, sem a coluna area; o INSERT dela leva todas as colunas.
    engine.create_table(PROJECTED)
    rows = entry_rows(MONTHS[0], 1, 3, PROJECTED).drop_columns(["area"])
    engine.append(PROJECTED, rows)
    copied = [text for text in connection.texts() if text.startswith("COPY")][-1]
    listed = ", ".join(f'"{name}"' for name in rows.column_names)
    assert copied.startswith(f'COPY "{PREFIX}cad_lancamentos_projetados_carga" ({listed})\nFROM ')


@pytest.mark.local
def test_appender_second_close_does_nothing(
    monkeypatch: pytest.MonkeyPatch,
    local_location: LocalLocation,
) -> None:
    """O ``close`` explícito dentro do ``with``: a saída do ``with`` chama o ``close`` de novo, e
    ele não roda outro ``COPY``."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    connection = FakeConnection(storage, existing={f"{PREFIX}cad_contas"})
    engine = fake_engine(monkeypatch, connection, storage)
    with engine.appender(ACCOUNTS) as appender:
        appender.write(account_rows(["A", "B"]))
        appender.close()
    assert appender.rows == 2
    assert len([text for text in connection.texts() if text.startswith("COPY")]) == 1
    assert connection.texts().count("COMMIT") == 1
    assert storage.list_files(f"prd/staging/{EXECUTION_ID}") == []


@pytest.mark.local
def test_ingest_loads_each_partition_through_the_staging(
    monkeypatch: pytest.MonkeyPatch,
    local_location: LocalLocation,
) -> None:
    """Por partição, o manifesto no ``staging/``, o ``DELETE`` da staging, o ``COPY ... MANIFEST
    FILLRECORD`` com a lista das colunas do arquivo e o ``INSERT`` com o valor; a staging apagada
    no fim; a partição sem arquivo não roda; o nome ocupado e a tabela sem versão são
    ``SandboxError``; ``pinned_delta`` carrega a versão inteira em ``_versao_<versão>`` uma vez, e
    outra versão numa staging nova; ``cleanup`` apaga as tabelas e o ``staging/``."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    uri = storage.uri_of("prd/cad_lancamentos")
    delta.create_table(uri, ENTRIES, storage)
    for index, month in enumerate(MONTHS):
        month_rows = entry_rows(month, 1 + index * 10, 10)
        delta.publish_partition(uri, ENTRIES, month, month_rows, METADATA, storage)
    connection = FakeConnection(storage)
    engine = fake_engine(monkeypatch, connection, storage)
    with pytest.raises(SandboxError, match="versão"):
        engine.ingest(ENTRIES, uri, None)

    # Duas partições pedidas, uma sem arquivo: um COPY e um INSERT.
    engine.ingest(ENTRIES, uri, 2, partitions=[MONTHS[1], "2026-09-30"])
    texts = connection.texts()
    name = f"{PREFIX}cad_lancamentos"
    staging = f"{name}_staging"
    assert any(text.startswith(f'CREATE TABLE "{name}" (') for text in texts)
    assert any(text.startswith(f'CREATE TABLE "esquema"."{staging}" (') for text in texts)
    copies = [text for text in texts if text.startswith("COPY")]
    assert len(copies) == 1
    manifest_uri = re.search(r"FROM '([^']+)'", copies[0]).group(1)
    staging_folder = storage.uri_of(f"prd/staging/{EXECUTION_ID}/cad_lancamentos/")
    assert manifest_uri.startswith(staging_folder)
    assert manifest_uri.endswith("/1.manifest")
    file_columns = ", ".join(
        f'"{column.name}"' for column in redshift.columns_without_partition(ENTRIES)
    )
    assert copies[0].startswith(f'COPY "esquema"."{staging}" ({file_columns})\n')
    assert "FORMAT AS PARQUET MANIFEST FILLRECORD" in copies[0]
    manifest = json.loads(storage.read_text(storage.relative(manifest_uri))[0])
    assert len(manifest["entries"]) == 1
    assert f"data_base_str={MONTHS[1]}/" in manifest["entries"][0]["url"]
    inserts = [text for text in texts if text.startswith("INSERT")]
    assert inserts == [
        redshift.insert_from_staging(
            f'"esquema"."{name}"', f'"esquema"."{staging}"', ENTRIES, MONTHS[1]
        )
    ]
    assert texts[-1] == f'DROP TABLE IF EXISTS "esquema"."{staging}"'
    with pytest.raises(SandboxError, match="ocupado"):
        engine.ingest(ENTRIES, uri, 2)

    # pinned_delta: a versão inteira, uma vez.
    source = engine.pinned_delta(ENTRIES, uri, 2)
    assert str(sa.select(source.c.id_lancamento)).startswith(
        f'SELECT "{PREFIX}cad_lancamentos_versao_2"."id_lancamento"'
    )
    copies = [text for text in connection.texts() if text.startswith("COPY")]
    assert len(copies) == 3
    engine.pinned_delta(ENTRIES, uri, 2)
    assert len([text for text in connection.texts() if text.startswith("COPY")]) == 3
    with pytest.raises(SandboxError):
        engine.pinned_delta(ENTRIES, uri, None)

    # Outra versão numa staging nova, com a partição única da versão 1.
    older = engine.pinned_delta(ENTRIES, uri, 1)
    assert str(sa.select(older.c.id_lancamento)).startswith(
        f'SELECT "{PREFIX}cad_lancamentos_versao_1"."id_lancamento"'
    )
    assert len([text for text in connection.texts() if text.startswith("COPY")]) == 4

    # O cleanup apaga as tabelas e o staging/.
    engine.cleanup()
    dropped = [text for text in connection.texts() if text.startswith("DROP TABLE IF EXISTS")]
    assert dropped[-3:] == [
        f'DROP TABLE IF EXISTS "esquema"."{name}"',
        f'DROP TABLE IF EXISTS "esquema"."{name}_versao_2"',
        f'DROP TABLE IF EXISTS "esquema"."{name}_versao_1"',
    ]
    assert storage.list_files(f"prd/staging/{EXECUTION_ID}") == []
    assert connection.closed


@pytest.mark.local
def test_ingest_counts_a_repeated_partition_once(
    monkeypatch: pytest.MonkeyPatch,
    local_location: LocalLocation,
) -> None:
    """A partição repetida em ``partitions`` entra uma vez: um ``COPY`` e um ``INSERT``."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    uri = storage.uri_of("prd/cad_lancamentos")
    delta.create_table(uri, ENTRIES, storage)
    rows = entry_rows(MONTHS[0], 1, 10)
    delta.publish_partition(uri, ENTRIES, MONTHS[0], rows, METADATA, storage)
    connection = FakeConnection(storage)
    engine = fake_engine(monkeypatch, connection, storage)
    engine.ingest(ENTRIES, uri, 1, partitions=[MONTHS[0], MONTHS[0]])
    texts = connection.texts()
    assert len([text for text in texts if text.startswith("COPY")]) == 1
    assert len([text for text in texts if text.startswith("INSERT")]) == 1


@pytest.mark.local
def test_export_registers_the_unloaded_files_and_swaps_on_nonfinite(
    monkeypatch: pytest.MonkeyPatch,
    local_location: LocalLocation,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """O registro: o ``UNLOAD`` sem ``PARTITION BY`` para
    ``<coluna>=<valor>/<execution_id>_<uuid>/`` na pasta da tabela, o ``select`` sem a coluna de
    partição e com o JSON serializado, e o arquivo como o Redshift o gravou (``INT96``) no log, com
    as linhas conferidas; dois destinos distintos para a mesma partição. A troca: com
    ``columns_without_min_max``, o destino no ``staging/`` e a partição por ``publish_partition``,
    com o aviso no log e a releitura, que desfaz o commit na contagem diferente; a partição vazia
    entra por um arquivo sem linha; o valor numa tabela sem partição recusa antes do
    ``UNLOAD``."""
    storage = Storage.for_uri(local_location.child(f"redshift/{uuid.uuid4().hex[:8]}"))
    uri = storage.uri_of("prd/cad_lancamentos_projetados")
    delta.create_table(uri, PROJECTED, storage)
    rows = entry_rows(MONTHS[1], 1, 1000, PROJECTED)
    unloaded = rows.drop_columns(["data_base_str"])
    connection = FakeConnection(storage, unload_rows=unloaded)
    engine = fake_engine(monkeypatch, connection, storage)

    # O registro dos arquivos do UNLOAD.
    version = engine.export_partition(PROJECTED, uri, MONTHS[1], METADATA, expected_rows=1000)
    assert version == 1
    unload = [text for text in connection.texts() if text.startswith("UNLOAD")][-1]
    select = re.search(r"UNLOAD \('(.+)'\)", unload, re.DOTALL).group(1)
    assert select.startswith(
        'SELECT "id_lancamento", "id_conta", "data_base", "carimbo", "valor", '
        '"preco", "area", JSON_SERIALIZE("meta") AS "meta", "to", "codigo" '
        f'FROM "esquema"."{PREFIX}cad_lancamentos_projetados" '
        f"WHERE \"data_base_str\" = ''{MONTHS[1]}''"
    )
    assert select.endswith('ORDER BY "data_base", "id_lancamento"')
    assert "PARALLEL OFF" in unload
    destination = re.search(r"TO '([^']+)'", unload).group(1)
    pattern = re.escape(uri) + rf"/data_base_str={MONTHS[1]}/{EXECUTION_ID}_[0-9a-f]{{32}}/"
    assert re.fullmatch(pattern, destination)
    files = storage.list_files(storage.relative(uri), ".parquet")
    assert len(files) == 1
    assert files[0].startswith(
        f"prd/cad_lancamentos_projetados/data_base_str={MONTHS[1]}/{EXECUTION_ID}_"
    )
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
    values = [float("nan")] + [number / 4 for number in range(2, 1001)]
    with_nan = rows.set_column(rows.schema.get_field_index("valor"), "valor", pa.array(values))
    connection.unload_rows = with_nan.drop_columns(["data_base_str"])
    with caplog.at_level(logging.WARNING, logger="serialize_db.engine.redshift"):
        version = engine.export_partition(
            PROJECTED,
            uri,
            MONTHS[1],
            METADATA,
            expected_rows=1000,
            columns_without_min_max=["valor"],
        )
    assert version == 2
    assert "publish_partition" in caplog.text
    assert "['valor']" in caplog.text
    unload = [text for text in connection.texts() if text.startswith("UNLOAD")][-1]
    destination = re.search(r"TO '([^']+)'", unload).group(1)
    assert destination.startswith(
        storage.uri_of(
            f"prd/staging/{EXECUTION_ID}/cad_lancamentos_projetados/data_base_str={MONTHS[1]}/"
        )
    )
    stats = pa.table(delta.open_table(uri, storage).get_add_actions(flatten=True))
    assert stats.column("max.valor").null_count == 1
    read = delta.open_table(uri, storage).to_pyarrow_table()
    assert read.num_rows == 1000
    assert pc.sum(pc.is_nan(read.column("valor"))).as_py() == 1
    assert read.column("data_base_str").unique().to_pylist() == [MONTHS[1]]

    # A partição vazia: nenhum arquivo do UNLOAD, um arquivo sem linha registrado.
    connection.unload_rows = unloaded.slice(0, 0)
    version = engine.export_partition(PROJECTED, uri, MONTHS[1], METADATA, expected_rows=0)
    assert version == 3
    assert delta.open_table(uri, storage).to_pyarrow_table().num_rows == 0

    # A troca relê a partição: a contagem diferente desfaz o commit.
    connection.unload_rows = with_nan.drop_columns(["data_base_str"])
    with pytest.raises(RegistrationRefused, match="releitura"):
        engine.export_partition(
            PROJECTED,
            uri,
            MONTHS[1],
            METADATA,
            expected_rows=999,
            columns_without_min_max=["valor"],
        )
    assert delta.open_table(uri, storage).to_pyarrow_table().num_rows == 0

    # O valor numa tabela sem partição recusa antes de qualquer comando.
    commands = len(connection.commands)
    with pytest.raises(ContractError, match="tabela sem partição recebeu o valor"):
        engine.export_partition(ACCOUNTS, storage.uri_of("prd/cad_contas"), MONTHS[1], METADATA)
    assert len(connection.commands) == commands
    engine.cleanup()


# ---------------------------------------------------------------- no esquema autorizado


@dataclasses.dataclass
class Target:
    """A raiz S3 e o motor de um teste no esquema autorizado."""

    storage: Storage
    engine: RedshiftEngine
    execution_id: str

    def uri(
        self,
        table: sa.Table,
    ) -> str:
        return self.storage.uri_of(f"prd/{table.name}")

    def qualified(
        self,
        suffix: str,
    ) -> str:
        return self.engine.qualified(self.engine.prefix + suffix)


@pytest.fixture
def target(
    s3_location: S3Location,
    redshift_driver: None,
) -> Iterator[Target]:
    """O motor sobre uma raiz nova no bucket e um sandbox próprio; no substituto local, a conexão
    de ``tests/emulator.py`` no lugar do driver."""
    storage = Storage.for_uri(s3_location.child(f"engine/{uuid.uuid4().hex[:8]}"))
    execution_id = f"poc-{uuid.uuid4().hex[:8]}"
    engine = RedshiftEngine(redshift_config(), execution_id, storage, f"prd/staging/{execution_id}")
    yield Target(storage, engine, execution_id)
    engine.cleanup()


def published_table(
    target: Target,
    table: sa.Table,
    months: list[str],
    rows: int = 100,
) -> int:
    """A tabela Delta com ``rows`` linhas por mês e ids contíguos a partir de 1; devolve a última
    versão."""
    uri = target.uri(table)
    delta.create_table(uri, table, target.storage)
    version = 0
    for index, month in enumerate(months):
        data = entry_rows(month, 1 + index * rows, rows, table)
        version = delta.publish_partition(uri, table, month, data, METADATA, target.storage)
    return version


def count_of(
    engine: RedshiftEngine,
    name: str,
) -> int:
    """As linhas da tabela ``name`` do esquema."""
    counted = engine.query(f"SELECT count(*) AS n FROM {engine.qualified(name)}")
    return counted.column("n")[0].as_py()


def totals_of(
    engine: RedshiftEngine,
    name: str,
) -> dict:
    """As linhas, os ids distintos e a soma de ``valor`` da tabela ``name`` do esquema."""
    totals = engine.query(
        f'SELECT count(*) AS linhas, count(DISTINCT "id_lancamento") AS ids, '
        f'sum("valor") AS soma FROM {engine.qualified(name)}'
    )
    return totals.to_pylist()[0]


@pytest.mark.redshift
@pytest.mark.s3
def test_connect_uses_share_database(
    target: Target,
) -> None:
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
    record(
        "redshift.engine.relation_missing",
        {"sqlstate": fields.get("C"), "message": str(fields.get("M"))},
    )
    record(
        "redshift.engine.current_database",
        engine.query("select current_database()").column(0)[0].as_py(),
    )
    record("redshift.engine.control_table_present", engine.name_in_use("serialize_db_publications"))


@pytest.mark.redshift
@pytest.mark.s3
def test_ingest_stream_appender_export(
    target: Target,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """``ingest`` de uma partição de um Delta no bucket, ``stream`` em lotes igual ao ``query``,
    ``create_table`` e o ``appender`` por ``COPY``, a auditoria com a versão fixada,
    ``export_partition`` pelo registro e pela troca com as mesmas linhas, e ``cleanup`` sem tabela
    restante."""
    engine = target.engine
    storage = target.storage
    entries_version = published_table(target, ENTRIES, MONTHS, rows=120)
    uri = target.uri(ENTRIES)
    engine.ingest(ENTRIES, uri, entries_version, partitions=[MONTHS[1]])
    assert count_of(engine, f"{engine.prefix}cad_lancamentos") == 120

    # O mesmo resultado pelo query e pelo stream.
    statement = (
        sa.select(ENTRIES)
        .where(ENTRIES.c.data_base_str == sa.bindparam("particao"), ENTRIES.c.to == "SP")
        .order_by(ENTRIES.c.id_lancamento)
    )
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

    # O appender grava a projeção na tabela de create_table; o nome ocupado é recusado.
    engine.create_table(PROJECTED)
    with (
        engine.stream(statement, params, batch_size=40) as stream,
        engine.appender(PROJECTED) as appender,
    ):
        for batch in stream:
            appender.write(batch)
    assert appender.rows == 120
    assert count_of(engine, f"{engine.prefix}cad_lancamentos_projetados") == 120
    with pytest.raises(SandboxError, match="ocupado"):
        engine.create_table(PROJECTED)

    # A auditoria com a versão fixada e a chave estrangeira fora do sandbox.
    projected_uri = target.uri(PROJECTED)
    delta.create_table(projected_uri, PROJECTED, storage)
    accounts_uri = target.uri(ACCOUNTS)
    delta.create_table(accounts_uri, ACCOUNTS, storage)
    accounts = account_rows(["A", "B", "C"])
    accounts_version = delta.publish_partition(
        accounts_uri, ACCOUNTS, None, accounts, METADATA, storage
    )
    report = engine.audit(PROJECTED, [MONTHS[1]], projected_uri, 0)
    assert report.passed, report.results
    assert [result.name for result in report.results] == [
        "linhas",
        "chave_id_lancamento",
        "chave_id_lancamento_tabela",
        "chave_codigo",
        "chave_codigo_tabela",
    ]
    assert report.rows(MONTHS[1]) == 120
    assert report.nonfinite_columns[MONTHS[1]] == ()
    # A versão fixada vazia dispensa a junção da chave sequencial e carrega a staging só para
    # a chave codigo; a chave estrangeira de cad_lancamentos entra pela versão fixada de
    # cad_contas, carregada em _versao_<versão> quando o anti-join roda.
    orphans = engine.audit(
        ENTRIES,
        [MONTHS[1]],
        uri,
        entries_version,
        foreign_keys=True,
        referenced={"cad_contas": (accounts_uri, accounts_version)},
    )
    assert orphans.passed, orphans.results
    assert [result.name for result in orphans.results][-1] == "orfao_id_conta"
    assert count_of(engine, f"{engine.prefix}cad_contas_versao_{accounts_version}") == 3
    entries_staging = f"{engine.prefix}cad_lancamentos_versao_{entries_version}"
    assert count_of(engine, entries_staging) == 240

    # A exportação pelo registro: o arquivo do UNLOAD na pasta da partição, lido pelos leitores.
    version = engine.export_partition(
        PROJECTED, projected_uri, MONTHS[1], METADATA, expected_rows=120
    )
    assert version == 1
    files = storage.list_files(storage.relative(projected_uri), ".parquet")
    assert len(files) >= 1
    assert files[0].startswith(
        f"prd/cad_lancamentos_projetados/data_base_str={MONTHS[1]}/{target.execution_id}_"
    )
    read = delta.open_table(projected_uri, storage).to_pyarrow_table().sort_by("id_lancamento")
    assert read.num_rows == 120
    assert read.column("id_lancamento").to_pylist() == list(range(121, 241))
    assert read.column("preco").to_pylist() == by_query.column("preco").to_pylist()
    assert read.column("carimbo").to_pylist() == by_query.column("carimbo").to_pylist()
    assert json.loads(read.column("meta").to_pylist()[0]) == {"k": 121}
    with storage.duckdb_connect() as connection:
        counted = connection.execute(
            f"SELECT count(*), sum(valor) FROM delta_scan('{projected_uri}')"
        ).fetchone()
        meta_scan = connection.execute(
            f"SELECT typeof(meta), meta FROM delta_scan('{projected_uri}') "
            "WHERE id_lancamento = 121"
        ).fetchone()
    assert counted[0] == 120
    record("redshift.engine.delta_scan_meta", {"tipo": meta_scan[0], "valor": str(meta_scan[1])})

    # A troca: a partição com NaN pela máquina local, com as mesmas linhas e o aviso; o append
    # entra na tabela esvaziada.
    with_nan = entry_rows(
        MONTHS[0],
        1,
        120,
        PROJECTED,
        valor=[float("nan")] + [number / 4 for number in range(2, 121)],
    )
    engine.execute(f"DELETE FROM {target.qualified('cad_lancamentos_projetados')}")
    assert engine.append(PROJECTED, with_nan) == 120
    with caplog.at_level(logging.WARNING, logger="serialize_db.engine.redshift"):
        version = engine.export_partition(
            PROJECTED,
            projected_uri,
            MONTHS[0],
            METADATA,
            expected_rows=120,
            columns_without_min_max=["valor"],
        )
    assert version == 2
    assert "publish_partition" in caplog.text
    read = delta.open_table(projected_uri, storage).to_pyarrow_table()
    assert read.num_rows == 240
    nan_rows = read.filter(pc.is_nan(read.column("valor")))
    assert nan_rows.num_rows == 1
    assert nan_rows.column("data_base_str")[0].as_py() == MONTHS[0]
    stats = pa.table(delta.open_table(projected_uri, storage).get_add_actions(flatten=True))
    by_partition = dict(
        zip(
            stats.column("partition.data_base_str").to_pylist(),
            stats.column("max.valor").to_pylist(),
        )
    )
    assert by_partition[MONTHS[0]] is None
    assert by_partition[MONTHS[1]] == 60.0

    # O cleanup: nenhuma tabela exec_<id>_* e o staging vazio.
    engine.cleanup()
    other = RedshiftEngine(redshift_config(), target.execution_id + "b", storage, "prd/staging/x")
    try:
        for suffix in (
            "cad_lancamentos",
            "cad_lancamentos_projetados",
            f"cad_contas_versao_{accounts_version}",
        ):
            assert not other.name_in_use(f"{engine.prefix}{suffix}")
    finally:
        other.cleanup()
    assert storage.list_files(f"prd/staging/{target.execution_id}") == []


@pytest.mark.redshift
@pytest.mark.s3
def test_appender_copies_the_file_at_close(
    target: Target,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tabela de ``create_table`` nasce vazia e fica vazia até o ``close``; o arquivo que some
    antes do ``COPY`` o faz falhar pela entrada obrigatória do manifesto, sem deixar linha, e a
    mensagem do servidor é uma leitura; o ``appender`` sem lote não muda a tabela; o segundo
    appender acrescenta; o ``close`` explícito dentro do ``with`` carrega o arquivo uma vez."""
    engine = target.engine
    name = f"{engine.prefix}cad_lancamentos_projetados"
    engine.create_table(PROJECTED)
    assert count_of(engine, name) == 0
    with engine.appender(PROJECTED) as appender:
        appender.write(entry_rows(MONTHS[0], 1, 10, PROJECTED))
        assert count_of(engine, name) == 0
    assert count_of(engine, name) == 10

    # O arquivo apagado depois do manifesto: o COPY falha pela entrada obrigatória, e a transação
    # não deixa linha. O dublê chama a função guardada antes da troca, porque
    # redshift._write_file_manifest, depois dela, é o próprio dublê.
    original_write_file_manifest = redshift._write_file_manifest
    manifests = []

    def manifest_without_file(
        sink: object,
        manifest_path: str,
    ) -> str:
        manifests.append(manifest_path)
        uri = original_write_file_manifest(sink, manifest_path)
        sink.storage.delete([sink.path])
        return uri

    monkeypatch.setattr(redshift, "_write_file_manifest", manifest_without_file)
    with pytest.raises(redshift_connector.Error) as missing:
        engine.append(PROJECTED, entry_rows(MONTHS[0], 11, 10, PROJECTED))
    monkeypatch.undo()
    assert len(manifests) == 1
    note = missing.value.__notes__[0]
    assert note.startswith("comando: COPY")
    assert f"FROM '{engine.storage.uri_of(manifests[0])}'" in note
    fields = missing.value.args[0]
    record(
        "redshift.engine.copy_missing_mandatory_file",
        {"sqlstate": fields.get("C"), "message": str(fields.get("M"))},
    )
    assert count_of(engine, name) == 10

    # O appender sem lote não muda a tabela; o seguinte acrescenta.
    with engine.appender(PROJECTED):
        pass
    assert count_of(engine, name) == 10
    assert engine.append(PROJECTED, entry_rows(MONTHS[0], 11, 5, PROJECTED)) == 5
    assert count_of(engine, name) == 15

    # O close explícito dentro do with: a saída do with não carrega o arquivo de novo.
    with engine.appender(PROJECTED) as appender:
        appender.write(entry_rows(MONTHS[0], 16, 5, PROJECTED))
        appender.close()
    assert count_of(engine, name) == 20


@pytest.mark.redshift
@pytest.mark.s3
def test_appender_loads_a_batch_without_a_middle_column(
    target: Target,
) -> None:
    """Um lote sem uma coluna anulável do meio da tabela: cada coluna do arquivo entra na de mesmo
    nome, e a que falta fica nula, pelo ``COPY`` direto e pela staging da tabela com JSON."""
    engine = target.engine
    engine.create_table(MEASURES)
    widths = pa.table(
        {"id_medida": pa.array([1, 2], pa.int64()), "largura": pa.array([10, 20], pa.int64())}
    )
    assert engine.append(MEASURES, widths) == 2
    measured = engine.query(sa.select(MEASURES).order_by(MEASURES.c.id_medida))
    assert measured.to_pylist() == [
        {"id_medida": 1, "altura": None, "largura": 10},
        {"id_medida": 2, "altura": None, "largura": 20},
    ]

    # A tabela com coluna JSON, pela staging _carga: o lote sem a coluna area.
    engine.create_table(PROJECTED)
    rows = entry_rows(MONTHS[0], 1, 3, PROJECTED)
    assert engine.append(PROJECTED, rows.drop_columns(["area"])) == 3
    loaded = engine.query(sa.select(PROJECTED).order_by(PROJECTED.c.id_lancamento))
    assert loaded.column("area").to_pylist() == [None, None, None]
    for column in ("to", "codigo", "data_base_str"):
        assert loaded.column(column).to_pylist() == rows.column(column).to_pylist(), column
    documents = [json.loads(text) for text in loaded.column("meta").to_pylist()]
    assert documents == [{"k": 1}, {"k": 2}, {"k": 3}]


@pytest.mark.redshift
@pytest.mark.s3
def test_ingest_and_pinned_delta_load_files_before_a_middle_column(
    target: Target,
) -> None:
    """Uma coluna anulável nova no meio do modelo depois de uma partição gravada: o ``ingest`` e o
    ``pinned_delta`` põem cada valor do arquivo anterior a ela na coluna de mesmo nome, com ela
    nula; na versão seguinte, a partição antiga tem também um arquivo com a coluna nova, e os
    dois grupos de colunas dela entram."""
    storage = target.storage
    before = text_columns_table("a", "b")
    after = text_columns_table("novo", "a", "b")
    uri = target.uri(before)
    delta.create_table(uri, before, storage)
    delta.publish_partition(
        uri, before, "p1", text_columns_rows(before, "p1", [1, 2]), METADATA, storage
    )
    delta.reconcile(uri, after, storage)
    version = delta.publish_partition(
        uri, after, "p2", text_columns_rows(after, "p2", [3]), METADATA, storage
    )
    expected = [
        {"id": 1, "novo": None, "a": "a1", "b": "b1", "parte": "p1"},
        {"id": 2, "novo": None, "a": "a2", "b": "b2", "parte": "p1"},
        {"id": 3, "novo": "novo3", "a": "a3", "b": "b3", "parte": "p2"},
    ]
    engine = target.engine
    engine.ingest(after, uri, version)
    assert engine.query(sa.select(after).order_by(after.c.id)).to_pylist() == expected
    pinned = engine.pinned_delta(after, uri, version)
    assert engine.query(sa.select(pinned).order_by(pinned.c.id)).to_pylist() == expected

    # A partição p1 com um arquivo de cada lista de colunas: um COPY por lista.
    appended = text_columns_rows(after, "p1", [4])
    write_deltalake(delta.open_table(uri, storage), appended, mode="append")
    pinned = engine.pinned_delta(after, uri, version + 1)
    loaded = engine.query(sa.select(pinned).order_by(pinned.c.id)).to_pylist()
    assert loaded == [*expected, {"id": 4, "novo": "novo4", "a": "a4", "b": "b4", "parte": "p1"}]


@pytest.mark.redshift
@pytest.mark.s3
def test_ingest_and_pinned_delta_load_reordered_columns(
    target: Target,
) -> None:
    """Duas colunas do modelo trocadas de lugar depois de uma partição gravada, sem diferença
    para o esquema Delta: o ``ingest`` e o ``pinned_delta`` põem cada valor na coluna de mesmo
    nome."""
    storage = target.storage
    written = text_columns_table("a", "b")
    reordered = text_columns_table("b", "a")
    uri = target.uri(written)
    delta.create_table(uri, written, storage)
    version = delta.publish_partition(
        uri, written, "p1", text_columns_rows(written, "p1", [1, 2]), METADATA, storage
    )
    diff = delta.schema_diff(reordered, delta.open_table(uri, storage))
    assert not diff.changes
    assert not diff.destructive
    expected = [
        {"id": 1, "b": "b1", "a": "a1", "parte": "p1"},
        {"id": 2, "b": "b2", "a": "a2", "parte": "p1"},
    ]
    engine = target.engine
    engine.ingest(reordered, uri, version)
    assert engine.query(sa.select(reordered).order_by(reordered.c.id)).to_pylist() == expected
    pinned = engine.pinned_delta(reordered, uri, version)
    assert engine.query(sa.select(pinned).order_by(pinned.c.id)).to_pylist() == expected


@pytest.mark.redshift
@pytest.mark.s3
def test_new_session_sees_committed_tables(
    target: Target,
) -> None:
    """A sessão de ``new_session`` vê a tabela ``exec_<id>_*`` confirmada pela principal e não a
    temporária dela; duas ingestões em duas sessões terminam, e a principal lê as duas."""
    engine = target.engine
    version = published_table(target, ENTRIES, MONTHS, rows=20)
    accounts_uri = target.uri(ACCOUNTS)
    delta.create_table(accounts_uri, ACCOUNTS, target.storage)
    accounts = account_rows(["A", "B", "C"])
    accounts_version = delta.publish_partition(
        accounts_uri, ACCOUNTS, None, accounts, METADATA, target.storage
    )
    temporary = f"{engine.prefix}temporaria"
    engine.query(f"CREATE TEMP TABLE {temporary} AS SELECT 1 AS x")

    def ingest(
        table: sa.Table,
        uri: str,
        pinned: int,
    ) -> None:
        with engine.new_session() as session:
            session.ingest(table, uri, pinned)

    threads = [
        threading.Thread(target=ingest, args=(ENTRIES, target.uri(ENTRIES), version)),
        threading.Thread(target=ingest, args=(ACCOUNTS, accounts_uri, accounts_version)),
    ]
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
def test_two_writers_on_the_same_table_both_enter(
    target: Target,
) -> None:
    """Dois appenders na mesma tabela, com os ``close`` ao mesmo tempo, entram os dois com todas
    as suas linhas: em duas sessões de ``new_session()``, cada ``COPY`` na sua conexão, e na
    sessão principal, um depois do outro sob o lock. Um appender numa sessão a mais e um
    ``UPDATE`` da principal sobre as linhas que a tabela já tinha, ao mesmo tempo, também entram
    os dois. A tabela tem coluna JSON, e cada ``COPY`` passa pela staging temporária da sua
    sessão."""
    engine = target.engine
    first = entry_rows(MONTHS[0], 1, 1_000, PROJECTED)
    second = entry_rows(MONTHS[0], 1_001, 1_000, PROJECTED)
    both_sum = pc.sum(first["valor"]).as_py() + pc.sum(second["valor"]).as_py()
    both = {"linhas": 2_000, "ids": 2_000, "soma": both_sum}
    barrier = threading.Barrier(2)

    def write(
        session: RedshiftEngine,
        table: sa.Table,
        rows: pa.Table,
    ) -> None:
        # A saída do with roda o close logo depois da barreira, junto com o outro escritor.
        with session.appender(table) as appender:
            appender.write(rows)
            barrier.wait(timeout=60)

    def write_in_a_new_session(
        table: sa.Table,
        rows: pa.Table,
    ) -> None:
        with engine.new_session() as session:
            write(session, table, rows)

    def update_first_half(
        table: sa.Table,
    ) -> None:
        barrier.wait(timeout=60)
        engine.query(
            sa.update(table).where(table.c.id_lancamento <= 500).values(valor=table.c.valor + 1)
        )

    def run_together(
        *tasks: tuple,
    ) -> None:
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = [pool.submit(*task) for task in tasks]
            for future in futures:
                future.result()

    # Dois appenders, cada um numa sessão a mais.
    sessions = PROJECTED.to_metadata(sa.MetaData(), name="cad_escritores_sessoes")
    engine.create_table(sessions)
    run_together(
        (write_in_a_new_session, sessions, first), (write_in_a_new_session, sessions, second)
    )
    assert totals_of(engine, engine.prefix + sessions.name) == both

    # Dois appenders na sessão principal.
    main = PROJECTED.to_metadata(sa.MetaData(), name="cad_escritores_principal")
    engine.create_table(main)
    run_together((write, engine, main, first), (write, engine, main, second))
    assert totals_of(engine, engine.prefix + main.name) == both

    # Um appender numa sessão a mais e o UPDATE da principal, que soma 1 a 500 linhas de first.
    updated = PROJECTED.to_metadata(sa.MetaData(), name="cad_escritores_update")
    engine.create_table(updated)
    engine.append(updated, first)
    run_together((write_in_a_new_session, updated, second), (update_first_half, updated))
    assert totals_of(engine, engine.prefix + updated.name) == {
        "linhas": 2_000,
        "ids": 2_000,
        "soma": both_sum + 500,
    }


@pytest.mark.redshift
@pytest.mark.s3
def test_stream_literal_values_on_the_target(
    target: Target,
) -> None:
    """Valores com ``'`` e ``\\`` voltam iguais pelo ``stream`` e pelo ``query``; um ``select``
    sem linha dá o stream vazio com o esquema; a tabela temporária criada por ``query`` é lida
    pelo ``UNLOAD`` do ``stream`` seguinte; o ``row_desc`` de cada tipo do contrato e dos
    agregados."""
    engine = target.engine
    texts = ["d'agua", "barra \\ invertida", "50% certo", "comum"]
    rows = entry_rows(MONTHS[0], 1, 4, PROJECTED)
    rows = rows.set_column(rows.schema.get_field_index("codigo"), "codigo", pa.array(texts))
    engine.create_table(PROJECTED)
    engine.append(PROJECTED, rows)
    for text in texts:
        statement = sa.select(PROJECTED.c.id_lancamento, PROJECTED.c.codigo).where(
            PROJECTED.c.codigo == sa.bindparam("texto")
        )
        by_query = engine.query(statement, {"texto": text})
        with engine.stream(statement, {"texto": text}) as stream:
            by_stream = stream.read_all()
        assert by_query.num_rows == 1, text
        assert by_stream.equals(by_query), text
    like = sa.select(PROJECTED.c.id_lancamento).where(PROJECTED.c.codigo.like(sa.bindparam("p")))
    with engine.stream(like, {"p": "%'%"}) as stream:
        assert stream.read_all().column("id_lancamento").to_pylist() == [1]

    # O resultado vazio fica com as colunas.
    none = sa.select(PROJECTED).where(PROJECTED.c.id_lancamento < 0)
    with engine.stream(none) as stream:
        empty = stream.read_all()
    assert empty.num_rows == 0
    assert empty.schema.names == PROJECTED.c.keys()

    # O stream lê a tabela temporária da sessão.
    temporary = f"{engine.prefix}temporaria"
    engine.query(f"CREATE TEMP TABLE {temporary} AS SELECT 2 AS x")
    with engine.stream(f"SELECT x FROM {temporary}") as stream:
        assert stream.read_all().column("x").to_pylist() == [2]

    # Os tipos que o row_desc descreve.
    described = engine.query(
        'select count(*) as c_count, sum("preco") as c_sum, sum("valor") as c_sum_double, '
        f"'literal' as c_text, 1.5 as c_numeric "
        f"from {target.qualified('cad_lancamentos_projetados')}"
    )
    types_by_column = {field.name: str(field.type) for field in described.schema}
    record("redshift.engine.row_desc", types_by_column)
    assert described.column("c_count").to_pylist() == [4]
    assert pa.types.is_decimal(described.column("c_sum").type)


@pytest.mark.redshift
@pytest.mark.s3
def test_stream_and_query_agree_on_a_colon_inside_a_literal(
    target: Target,
) -> None:
    """Um texto com ``:nome`` dentro de um literal, e um valor do cliente com contrabarra antes de
    ``:``, dão a mesma linha pelo ``stream`` e pelo ``query``: o literal e o valor chegam intactos
    ao ``UNLOAD``."""
    engine = target.engine
    rows = entry_rows(MONTHS[0], 1, 2, PROJECTED)
    rows = rows.set_column(
        rows.schema.get_field_index("codigo"), "codigo", pa.array(["ref :x1", r"ref \:x2"])
    )
    engine.create_table(PROJECTED)
    engine.append(PROJECTED, rows)
    text = (
        'SELECT "id_lancamento" FROM "{prefix}cad_lancamentos_projetados" '
        "WHERE \"codigo\" = 'ref :x1'"
    )
    by_query = engine.query(text)
    with engine.stream(text) as stream:
        by_stream = stream.read_all()
    assert by_query.column("id_lancamento").to_pylist() == [1]
    assert by_stream.equals(by_query)

    # O valor do cliente passa pelo cursor no query e pelo literal no stream.
    by_value = (
        'SELECT "id_lancamento" FROM "{prefix}cad_lancamentos_projetados" WHERE "codigo" = :codigo'
    )
    params = {"codigo": r"ref \:x2"}
    by_query = engine.query(by_value, params)
    with engine.stream(by_value, params) as stream:
        by_stream = stream.read_all()
    assert by_query.column("id_lancamento").to_pylist() == [2]
    assert by_stream.equals(by_query)


@pytest.mark.redshift
@pytest.mark.s3
def test_small_append_copy_cost(
    target: Target,
) -> None:
    """O tempo de um ``append`` de 10 linhas pelo ``appender``, o ``COPY`` de um arquivo pequeno,
    como leitura, nunca como reprovação."""
    engine = target.engine
    accounts = account_rows([f"C{index}" for index in range(10)])
    engine.create_table(ACCOUNTS)
    # O melhor de três acréscimos na mesma tabela.
    elapsed = []
    for _ in range(3):
        started = time.perf_counter()
        engine.append(ACCOUNTS, accounts)
        elapsed.append(time.perf_counter() - started)
    record("redshift.engine.small_append", f"{min(elapsed):.2f} s")
