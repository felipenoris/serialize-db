"""O substituto local do S3 e do Redshift, que roda fora do ambiente alvo as suítes que só rodam
nele: ``proof_of_concept/test_s3.py``, ``test_redshift.py`` e ``test_redshift_transactions.py``.

``SERIALIZE_DB_TEST_EMULATOR`` o liga em ``conftest.py``. No início da sessão, ``start`` sobe o
servidor do moto numa porta livre de ``127.0.0.1``, tira do processo as variáveis que levariam
uma chamada à AWS, a um proxy ou ao Redshift de verdade, aponta as variáveis da AWS e as das
suítes para o substituto e cria o bucket da raiz. ``connect_redshift`` devolve ``connect()``: uma
conexão falsa do ``redshift_connector`` sobre um DuckDB em memória, que traduz o SQL do Redshift
que as suítes escrevem. ``stop`` encerra o moto no fim. O moto e o DuckDB guardam tudo em memória,
e nada é gravado em disco.

O substituto confere o código Python dos testes. Cada recusa e cada comportamento que ele imita é
uma leitura do ambiente alvo registrada em ``plan/POC.md``: a contrabarra como escape nos literais
de texto, o ``UNLOAD`` de um resultado vazio sem manifesto nem arquivo e o ``is_valid_json`` que
recusa ``SUPER``. No resto, o DuckDB responde do jeito dele. Os bloqueios entre transações, a
criptografia do bucket, a Data API, as credenciais do contêiner, o proxy e a comparação do ``NaN``
numa varredura de tabela, que no Redshift segue o IEEE e no DuckDB a regra do PostgreSQL, só o
ambiente alvo mostra.

Duas variáveis provocam falhas, para rodar lado a lado o código anterior e o corrigido de um
tratamento de falha:

- ``SERIALIZE_DB_TEST_EMULATOR_FAIL_SQL``: uma expressão regular; o comando que casa com ela
  recebe um erro do servidor, ``XX000``.
- ``SERIALIZE_DB_TEST_EMULATOR_NO_MANIFEST``: qualquer valor; o ``UNLOAD ... MANIFEST`` passa sem
  gravar o manifesto.
"""

from __future__ import annotations

import collections
import importlib.util
import io
import json
import os
import re
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field

import boto3
import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import redshift_connector

# Os nomes que o substituto dá à raiz S3, ao esquema e aos bancos do Redshift.
BUCKET = "emulador"
S3_ROOT = f"s3://{BUCKET}/raiz"
SCHEMA = "emulador"
DATABASE_NAME = "dev"
SHARE_DATABASE = "compartilhado"
REGION = "us-east-1"

# O que levaria uma chamada à AWS de verdade, a um proxy ou ao Redshift de verdade sai do processo.
# Um endpoint por serviço, como AWS_ENDPOINT_URL_S3, venceria o do substituto no boto3.
REMOVED_VARIABLES = (
    "AWS_PROFILE",
    "AWS_SESSION_TOKEN",
    "AWS_ENDPOINT_URL_S3",
    "AWS_ENDPOINT_URL_STS",
    "HTTP_PROXY",
    "HTTPS_PROXY",
    "http_proxy",
    "https_proxy",
    "SERIALIZE_DB_REDSHIFT_WORKGROUP",
    "SERIALIZE_DB_REDSHIFT_HOST",
    "SERIALIZE_DB_REDSHIFT_PORT",
    "SERIALIZE_DB_REDSHIFT_USER",
    "SERIALIZE_DB_REDSHIFT_PASSWORD",
    "SERIALIZE_DB_REDSHIFT_IAM_ROLE",
)

# As funções do Redshift que o DuckDB não tem com o mesmo nome, como macros temporárias de cada
# conexão, que valem qualquer que seja o search_path dela.
MACROS = (
    "CREATE OR REPLACE TEMP MACRO json_parse(x) AS json(x)",
    "CREATE OR REPLACE TEMP MACRO json_serialize(x) AS CAST(x AS VARCHAR)",
    "CREATE OR REPLACE TEMP MACRO json_typeof(x) AS lower(json_type(x))",
    "CREATE OR REPLACE TEMP MACRO json_size(x) AS strlen(CAST(x AS VARCHAR))",
    "CREATE OR REPLACE TEMP MACRO is_valid_json(x) AS CASE WHEN typeof(x) = 'JSON' "
    "THEN error('function is_valid_json(super) does not exist') ELSE json_valid(x) END",
    "CREATE OR REPLACE TEMP MACRO octet_length(x) AS strlen(CAST(x AS VARCHAR))",
    "CREATE OR REPLACE TEMP MACRO getdate() AS now()",
    "CREATE OR REPLACE TEMP MACRO has_schema_privilege(s, p) AS true",
    "CREATE OR REPLACE TEMP MACRO to_char(d, f) AS strftime(d, "
    "replace(replace(replace(f, 'YYYY', '%Y'), 'MM', '%m'), 'DD', '%d'))",
)

# As tabelas e as visões de sistema do Redshift que as suítes leem, temporárias de cada conexão. O
# banco do datashare informa isolamento UNKNOWN, como no ambiente alvo (plan/POC.md).
SYSTEM_TABLES = (
    "CREATE OR REPLACE TEMP TABLE stl_load_errors (err_reason VARCHAR, starttime TIMESTAMP)",
    "CREATE OR REPLACE TEMP TABLE sys_load_error_detail "
    "(error_message VARCHAR, start_time TIMESTAMP)",
    "CREATE OR REPLACE TEMP VIEW svv_redshift_databases AS SELECT * FROM (VALUES "
    f"('{DATABASE_NAME}', 'local', 'Snapshot Isolation'), "
    f"('{SHARE_DATABASE}', 'shared', 'UNKNOWN')) "
    "AS t(database_name, database_type, database_isolation_level)",
    f"CREATE OR REPLACE TEMP VIEW svv_all_schemas AS SELECT '{SHARE_DATABASE}' AS database_name, "
    "schema_name, 'shared' AS schema_type FROM information_schema.schemata",
    f"CREATE OR REPLACE TEMP VIEW svv_all_columns AS SELECT '{SHARE_DATABASE}' AS database_name, "
    "table_schema AS schema_name, table_name, column_name, data_type, "
    "character_maximum_length, numeric_precision, numeric_scale, ordinal_position "
    "FROM information_schema.columns",
)

# O OID de cada tipo do DuckDB no protocolo do Redshift; um tipo fora da lista sai como texto (25).
# O JSON do DuckDB faz as vezes do SUPER, de OID 4000.
OIDS = {
    "BIGINT": 20,
    "SMALLINT": 21,
    "INTEGER": 23,
    "BOOLEAN": 16,
    "FLOAT": 700,
    "DOUBLE": 701,
    "DATE": 1082,
    "TIMESTAMP": 1114,
    "TIMESTAMP WITH TIME ZONE": 1184,
    "VARCHAR": 1043,
    "JSON": 4000,
}
TEXT_OID = 25
NUMERIC_OID = 1700

# Os comandos que não devolvem linhas; num INSERT, UPDATE ou DELETE, o DuckDB devolve a contagem.
COMMANDS = {
    "CREATE", "DROP", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "ALTER", "SET", "BEGIN", "COMMIT",
    "ROLLBACK",
}

# Uma região citada do DuckDB, '...' ou "...", com a aspa dobrada como escape; as traduções mexem só
# no texto fora delas.
QUOTED = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")

# Uma região citada do Redshift: o literal de texto, onde a aspa dobrada é uma aspa e a contrabarra
# escapa o caractere seguinte, como no PostgreSQL 8.0, ou o nome entre aspas duplas.
REDSHIFT_QUOTED = re.compile(r"'(?:[^'\\]|''|\\.)*'|\"(?:[^\"]|\"\")*\"", re.DOTALL)

COPY_PATTERN = re.compile(
    r"COPY\s+(?P<table>[^\s(]+)\s*(?:\((?P<columns>[^)]*)\)\s*)?"
    r"FROM\s+'(?P<source>[^']+)'(?P<options>.*)$",
    re.IGNORECASE | re.DOTALL,
)
UNLOAD_PATTERN = re.compile(
    r"UNLOAD\s*\(\s*'(?P<select>(?:[^'\\]|''|\\.)*)'\s*\)\s*TO\s*'(?P<target>[^']+)'"
    r"(?P<options>.*)$",
    re.IGNORECASE | re.DOTALL,
)
CREATE_TABLE_PATTERN = re.compile(
    r"CREATE\s+(?:TEMP\s+|TEMPORARY\s+)?TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    r"(?P<name>[^\s(]+)\s*\((?P<body>.*)\)",
    re.IGNORECASE | re.DOTALL,
)


# ------------------------------------------------------------ o moto e o ambiente


def free_port() -> int:
    """Uma porta livre de ``127.0.0.1``, escolhida pelo sistema."""
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def wait_until_listening(process: subprocess.Popen, port: int, timeout: float = 30.0) -> None:
    """Espera o moto abrir a porta; o processo que sai antes, ou o prazo esgotado, é erro."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(f"o moto saiu com o código {process.returncode} antes de abrir a "
                               f"porta {port}")
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.2):
                return
        except OSError:
            time.sleep(0.05)
    process.kill()
    raise RuntimeError(f"o moto não abriu a porta {port} em {timeout:.0f} s")


def emulator_environment(endpoint: str) -> dict[str, str]:
    """As variáveis que apontam a sessão para o substituto.

    O boto3, o PyArrow e o delta-rs leem ``AWS_ENDPOINT_URL``, e ``Storage.duckdb_setup`` o leva
    ao secret do DuckDB; o delta-rs recusa um endpoint ``http`` sem ``AWS_ALLOW_HTTP``. O boto3 lê
    a região de ``AWS_DEFAULT_REGION``, que vence a do perfil do usuário, e o delta-rs, a de
    ``AWS_REGION``. O moto aceita qualquer credencial.
    """
    return {
        "AWS_ENDPOINT_URL": endpoint,
        "AWS_ALLOW_HTTP": "true",
        "AWS_ACCESS_KEY_ID": "emulador",
        "AWS_SECRET_ACCESS_KEY": "emulador",
        "AWS_REGION": REGION,
        "AWS_DEFAULT_REGION": REGION,
        "SERIALIZE_DB_TEST_S3_ROOT": S3_ROOT,
        "SERIALIZE_DB_TEST_REDSHIFT_SCHEMA": SCHEMA,
        "SERIALIZE_DB_REDSHIFT_DATABASE": DATABASE_NAME,
        "SERIALIZE_DB_REDSHIFT_SHARE_DATABASE": SHARE_DATABASE,
    }


def start() -> subprocess.Popen:
    """Sobe o moto, aponta o ambiente do processo para ele e cria o bucket da raiz S3.

    Devolve o processo do moto, que ``stop`` encerra no fim da sessão. A saída dele vai para o
    ``/dev/null``: o moto registra cada requisição, e um pipe sem leitor o travaria. O moto vem do
    grupo ``emulator`` do ``pyproject.toml``, fora do ``dev``.
    """
    if importlib.util.find_spec("moto") is None:
        raise RuntimeError("SERIALIZE_DB_TEST_EMULATOR precisa do moto, do grupo emulator: "
                           "uv run --group emulator pytest ...")
    port = free_port()
    process = subprocess.Popen(
        [sys.executable, "-m", "moto.server", "-H", "127.0.0.1", "-p", str(port)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    wait_until_listening(process, port)

    for name in REMOVED_VARIABLES:
        os.environ.pop(name, None)
    os.environ.update(emulator_environment(f"http://127.0.0.1:{port}"))

    boto3.client("s3").create_bucket(Bucket=BUCKET)
    return process


def stop(process: subprocess.Popen) -> None:
    """Encerra o moto; os objetos que ele guardava em memória somem com ele."""
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


# ------------------------------------------------------------ os objetos no moto


def bucket_and_key(uri: str) -> tuple[str, str]:
    """O bucket e a chave de ``s3://bucket/chave``."""
    bucket, _, key = uri.removeprefix("s3://").partition("/")
    return bucket, key


def read_object(uri: str) -> bytes:
    """O conteúdo de um objeto."""
    bucket, key = bucket_and_key(uri)
    return boto3.client("s3").get_object(Bucket=bucket, Key=key)["Body"].read()


def write_object(uri: str, body: bytes) -> None:
    """Grava um objeto."""
    bucket, key = bucket_and_key(uri)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=body)


def object_uris(prefix_uri: str) -> list[str]:
    """As URIs dos objetos sob um prefixo."""
    bucket, prefix = bucket_and_key(prefix_uri)
    paginator = boto3.client("s3").get_paginator("list_objects_v2")
    uris = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            uris.append(f"s3://{bucket}/{item['Key']}")
    return uris


# ------------------------------------------------------------ o Redshift falso


@dataclass
class Result:
    """O que um comando devolve ao cursor: a descrição das colunas, o ``row_desc`` do protocolo,
    as linhas e a contagem de linhas, -1 quando o comando não conta."""

    description: list[tuple] | None = None
    row_desc: list[dict] = field(default_factory=list)
    rows: list[list] = field(default_factory=list)
    rowcount: int = -1


@dataclass
class RedshiftDatabase:
    """O banco que faz as vezes do Redshift: o DuckDB em memória, as conexões abertas por pid e o
    que o DuckDB não guarda do DDL, o ``n`` de cada ``VARCHAR(n)`` e as colunas ``SUPER``, por
    tabela."""

    duckdb_connection: duckdb.DuckDBPyConnection
    connections: dict[int, Connection] = field(default_factory=dict)
    last_pid: int = 1000
    varchar_lengths: dict[str, dict[str, int]] = field(default_factory=dict)
    super_columns: dict[str, set[str]] = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)

    def register(self, connection: Connection) -> int:
        """Dá à conexão o próximo pid, o que ``pg_terminate_backend`` recebe."""
        with self.lock:
            self.last_pid += 1
            self.connections[self.last_pid] = connection
            return self.last_pid

    def unregister(self, pid: int) -> None:
        """Esquece a conexão fechada."""
        with self.lock:
            self.connections.pop(pid, None)

    def all_super_columns(self) -> set[str]:
        """As colunas ``SUPER`` de todas as tabelas: um ``select`` não diz de que tabela vem cada
        nome."""
        columns = set()
        for names in self.super_columns.values():
            columns.update(names)
        return columns


class Cursor:
    """O cursor do ``redshift_connector`` nas partes que as suítes usam.

    Como o driver, o ``execute`` guarda o resultado inteiro em ``_cached_rows``, o nome que a
    suíte Redshift lê, e a descrição das colunas em ``ps["row_desc"]``; os ``fetch`` tiram as
    linhas dessa fila.
    """

    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.paramstyle = "format"
        self.description: list[tuple] | None = None
        self.rowcount = -1
        self.ps: dict[str, list[dict]] = {"row_desc": []}
        self._cached_rows: collections.deque = collections.deque()

    def execute(self, operation: str, args: object = None) -> Cursor:
        """Roda o comando e guarda o resultado."""
        result = run_command(self.connection, operation, args, self.paramstyle)
        self.description = result.description
        self.rowcount = result.rowcount
        self.ps = {"row_desc": result.row_desc}
        self._cached_rows = collections.deque(result.rows)
        return self

    def fetchone(self) -> list | None:
        """A próxima linha, ou ``None`` no fim."""
        if not self._cached_rows:
            return None
        return self._cached_rows.popleft()

    def fetchmany(self, num: int | None = None) -> tuple:
        """Até ``num`` linhas, uma só sem ``num``, como o ``arraysize`` padrão do driver."""
        size = num or 1
        rows = []
        while self._cached_rows and len(rows) < size:
            rows.append(self._cached_rows.popleft())
        return tuple(rows)

    def fetchall(self) -> tuple:
        """As linhas que restam."""
        rows = tuple(self._cached_rows)
        self._cached_rows.clear()
        return rows

    def close(self) -> None:
        """Nada a liberar: o resultado já está na fila."""


class Connection:
    """A conexão do ``redshift_connector`` nas partes que as suítes usam: uma conexão do DuckDB ao
    banco do substituto, com um pid e com as macros e as tabelas de sistema dela."""

    def __init__(self, database: RedshiftDatabase) -> None:
        self.database = database
        self.duckdb_connection = database.duckdb_connection.cursor()
        for statement in (*MACROS, *SYSTEM_TABLES):
            self.duckdb_connection.execute(statement)
        self.pid = database.register(self)
        self.autocommit = False
        self.in_transaction = False
        # As linhas do último UNLOAD que passou, o que pg_last_unload_count() devolve.
        self.last_unload_count = 0

    def cursor(self) -> Cursor:
        """Um cursor novo nesta conexão."""
        return Cursor(self)

    def commit(self) -> None:
        """Confirma a transação aberta."""
        self.end_transaction("COMMIT")

    def rollback(self) -> None:
        """Desfaz a transação aberta."""
        self.end_transaction("ROLLBACK")

    def end_transaction(self, command: str) -> None:
        """Confirma ou desfaz a transação aberta por ``BEGIN``; sem ela, nada a fazer."""
        if not self.in_transaction:
            return
        self.duckdb_connection.execute(command)
        self.in_transaction = False

    def close(self) -> None:
        """Fecha a conexão do DuckDB e libera o pid."""
        self.database.unregister(self.pid)
        self.duckdb_connection.close()


def new_database() -> RedshiftDatabase:
    """Um DuckDB em memória com o esquema do substituto."""
    connection = duckdb.connect()
    connection.execute(f"CREATE SCHEMA {SCHEMA}")
    return RedshiftDatabase(connection)


# O banco é um só no processo, e cada connect() abre uma conexão nele.
DATABASE = new_database()


def connect() -> Connection:
    """Uma conexão nova ao Redshift do substituto, no lugar de ``redshift_connector.connect``."""
    return Connection(DATABASE)


# ------------------------------------------------------------ a execução de um comando


def server_error(message: str, code: str = "XX000") -> redshift_connector.ProgrammingError:
    """O erro do driver com o dicionário de campos que o servidor manda: severidade, SQLSTATE e
    mensagem."""
    return redshift_connector.ProgrammingError({"S": "ERROR", "C": code, "M": message})


def first_line(error: Exception) -> str:
    """A primeira linha da mensagem de um erro do DuckDB."""
    lines = str(error).splitlines()
    return lines[0] if lines else type(error).__name__


def run_command(connection: Connection, operation: str, args: object, paramstyle: str) -> Result:
    """Roda um comando do Redshift: a falha provocada, os comandos de sessão, o ``COPY``, o
    ``UNLOAD`` e, no resto, o SQL traduzido para o DuckDB."""
    text = operation.strip().rstrip(";").strip()
    words = text.split(None, 1)
    first_word = words[0].upper() if words else ""

    raise_provoked_failure(text)
    answered = session_command(connection, text)
    if answered is not None:
        return answered
    if first_word == "COPY":
        return copy(connection, text)
    if first_word == "UNLOAD":
        return unload(connection, text)
    if first_word == "CREATE":
        remember_ddl(connection.database, without_physical_clauses(text))
    return run_in_duckdb(connection, text, args, paramstyle, first_word)


def raise_provoked_failure(text: str) -> None:
    """O erro do servidor para o comando que casa com ``SERIALIZE_DB_TEST_EMULATOR_FAIL_SQL``."""
    pattern = os.environ.get("SERIALIZE_DB_TEST_EMULATOR_FAIL_SQL")
    if pattern and re.search(pattern, text, re.IGNORECASE | re.DOTALL):
        raise server_error(f"falha provocada pelo substituto: {pattern}")


def session_command(connection: Connection, text: str) -> Result | None:
    """Os comandos de sessão, que o substituto responde sem o SQL traduzido; outro comando dá
    ``None``.

    ``USE``, ``LOCK`` e ``SET statement_timeout`` passam sem efeito, ``SET search_path`` vale para
    a conexão do DuckDB, ``pg_backend_pid`` e ``pg_terminate_backend`` usam os pids do substituto,
    e ``pg_last_unload_count`` dá as linhas do último ``UNLOAD`` da conexão.
    """
    if re.match(r"(?:USE|LOCK)\b|SET\s+statement_timeout\b", text, re.IGNORECASE):
        return Result()

    search_path = re.fullmatch(r"SET\s+search_path\s+TO\s+(.+)", text, re.IGNORECASE)
    if search_path:
        schema = search_path.group(1).strip()
        connection.duckdb_connection.execute(f"SET search_path = '{schema}'")
        return Result()

    if re.fullmatch(r"SELECT\s+pg_backend_pid\(\)", text, re.IGNORECASE):
        return single_value("pg_backend_pid", OIDS["INTEGER"], connection.pid)

    if re.fullmatch(r"SELECT\s+pg_last_unload_count\(\)", text, re.IGNORECASE):
        return single_value("pg_last_unload_count", OIDS["BIGINT"], connection.last_unload_count)

    terminate = re.fullmatch(r"SELECT\s+pg_terminate_backend\((\d+)\)", text, re.IGNORECASE)
    if terminate:
        target = connection.database.connections.get(int(terminate.group(1)))
        # A sessão presa num comando do DuckDB recebe a interrupção, como o fim do backend.
        if target is not None:
            target.duckdb_connection.interrupt()
        return single_value("pg_terminate_backend", OIDS["BOOLEAN"], target is not None)
    return None


def single_value(name: str, oid: int, value: object) -> Result:
    """O resultado de uma linha e uma coluna."""
    description = [(name, oid, None, None, None, None, None)]
    row_desc = [{"label": name.encode(), "type_oid": oid, "type_modifier": -1}]
    return Result(description, row_desc, [[value]], 1)


def run_in_duckdb(connection: Connection, text: str, args: object, paramstyle: str,
                  first_word: str) -> Result:
    """O comando traduzido, rodado na conexão do DuckDB; o erro do DuckDB volta como erro do
    servidor."""
    translated = to_duckdb(text, connection.database.all_super_columns())
    translated, parameters = duckdb_parameters(translated, args, paramstyle)
    try:
        cursor = connection.duckdb_connection.execute(translated, parameters)
        if first_word in COMMANDS:
            result = command_result(cursor)
        else:
            result = query_result(cursor)
    except duckdb.Error as error:
        raise server_error(first_line(error)) from None

    # A transação aberta por BEGIN, que o rollback da limpeza desfaz.
    if first_word == "BEGIN":
        connection.in_transaction = True
    if first_word in ("COMMIT", "ROLLBACK"):
        connection.in_transaction = False
    return result


def command_result(cursor: duckdb.DuckDBPyConnection) -> Result:
    """A contagem de linhas de um ``INSERT``, ``UPDATE`` ou ``DELETE``, a única linha que o
    DuckDB devolve; -1 nos outros comandos, como o driver."""
    rows = cursor.fetchall() if cursor.description else []
    if rows and isinstance(rows[0][0], int):
        return Result(rowcount=rows[0][0])
    return Result()


def query_result(cursor: duckdb.DuckDBPyConnection) -> Result:
    """As linhas e a descrição das colunas de uma consulta, como o driver as guarda."""
    description = []
    row_desc = []
    for column in cursor.description:
        name = column[0]
        oid, modifier = oid_and_modifier(str(column[1]))
        description.append((name, oid, None, None, None, None, None))
        row_desc.append({"label": name.encode(), "type_oid": oid, "type_modifier": modifier})
    rows = []
    for row in cursor.fetchall():
        rows.append(list(row))
    return Result(description, row_desc, rows, len(rows))


def oid_and_modifier(type_name: str) -> tuple[int, int]:
    """O OID e o ``type_modifier`` do tipo de uma coluna do DuckDB, como o ``row_desc`` do driver
    os traz: ``DECIMAL(p, s)`` é ``NUMERIC``, com ``((p << 16) | s) + 4``, e os outros não têm
    modificador, -1."""
    upper = type_name.upper()
    decimal = re.fullmatch(r"DECIMAL\((\d+),\s*(\d+)\)", upper)
    if decimal:
        precision = int(decimal.group(1))
        scale = int(decimal.group(2))
        return NUMERIC_OID, ((precision << 16) | scale) + 4
    return OIDS.get(upper, TEXT_OID), -1


# ------------------------------------------------------------ a tradução do SQL


def outside_quotes(text: str, rewrite: Callable[[str], str]) -> str:
    """Aplica ``rewrite`` a cada trecho de ``text`` fora das regiões citadas."""
    pieces = []
    position = 0
    for match in QUOTED.finditer(text):
        pieces.append(rewrite(text[position:match.start()]))
        pieces.append(match.group(0))
        position = match.end()
    pieces.append(rewrite(text[position:]))
    return "".join(pieces)


def without_physical_clauses(text: str) -> str:
    """O DDL sem ``DISTSTYLE``, ``DISTKEY``, ``SORTKEY`` e ``ENCODE``, que o DuckDB não tem.

    As cláusulas citam colunas entre aspas, e a troca vale para o texto inteiro.
    """
    text = re.sub(r"\bDISTSTYLE\s+\w+", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\b(?:COMPOUND\s+)?SORTKEY\s*\([^)]*\)", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\bDISTKEY\s*\([^)]*\)", "", text, flags=re.IGNORECASE)
    return re.sub(r"\bENCODE\s+\w+", "", text, flags=re.IGNORECASE)


def literal_value(body: str) -> str:
    """O valor de um literal de texto do Redshift, dado o que fica entre as aspas: a aspa dobrada é
    uma aspa, e a contrabarra dá o caractere seguinte. As sequências ``\\n``, ``\\t`` e a octal,
    que as suítes não escrevem, ficam de fora."""
    characters = []
    position = 0
    while position < len(body):
        character = body[position]
        # A aspa dobrada e a contrabarra valem pelo caractere seguinte.
        if character in ("'", "\\"):
            position += 1
            character = body[position]
        characters.append(character)
        position += 1
    return "".join(characters)


def duckdb_region(match: re.Match) -> str:
    """Uma região citada do Redshift no DuckDB: o literal de texto com o mesmo valor, sem escape de
    contrabarra, e o nome entre aspas duplas como está."""
    region = match.group(0)
    if region.startswith('"'):
        return region
    value = literal_value(region[1:-1])
    escaped = value.replace("'", "''")
    return f"'{escaped}'"


def to_duckdb(text: str, super_columns: set[str]) -> str:
    """O SQL do Redshift no dialeto do DuckDB: sem as cláusulas físicas do DDL, cada literal de
    texto com o valor que o Redshift lê nele e, fora das regiões citadas, ``SUPER`` como ``JSON`` e
    o caminho por ponto numa coluna ``SUPER`` (``meta.sistema``) como ``json_extract``."""

    def rewrite(part: str) -> str:
        part = re.sub(r"\bSUPER\b", "JSON", part, flags=re.IGNORECASE)
        for column in sorted(super_columns):
            path = rf"\b{re.escape(column)}\.(\w+)"
            part = re.sub(path, rf"json_extract({column}, '$.\1')", part)
        return part

    duckdb_text = REDSHIFT_QUOTED.sub(duckdb_region, without_physical_clauses(text))
    return outside_quotes(duckdb_text, rewrite)


def named_to_dollar(part: str) -> str:
    """Os marcadores ``:nome`` do estilo ``named`` como ``$nome``, o estilo do DuckDB."""
    return re.sub(r"(?<![:\w]):(\w+)", r"$\1", part)


def format_to_question_mark(part: str) -> str:
    """Os marcadores ``%s`` do estilo ``format`` como ``?``, e o ``%%`` como ``%``."""
    return part.replace("%s", "?").replace("%%", "%")


def duckdb_parameters(text: str, args: object, paramstyle: str) -> tuple[str, object]:
    """O texto com os marcadores do driver no estilo do DuckDB, e os valores.

    Sem valores, o texto vai como está, e um ``%%`` chega ao DuckDB como o driver o manda ao
    servidor.
    """
    if args is None:
        return text, None
    if paramstyle == "named":
        return outside_quotes(text, named_to_dollar), dict(args)
    return outside_quotes(text, format_to_question_mark), list(args)


def split_top_level(body: str) -> list[str]:
    """As definições de coluna de um ``CREATE TABLE``, separadas pelas vírgulas de fora dos
    parênteses."""
    parts = []
    depth = 0
    current = []
    for character in body:
        if character == "(":
            depth += 1
        if character == ")":
            depth -= 1
        if character == "," and depth == 0:
            parts.append("".join(current))
            current = []
            continue
        current.append(character)
    parts.append("".join(current))
    return parts


def table_key(name: str) -> str:
    """O nome da tabela sem o esquema e sem aspas, em minúsculas: a chave do que o substituto
    guarda do DDL."""
    return name.split(".")[-1].strip('"').lower()


def remember_ddl(database: RedshiftDatabase, text: str) -> None:
    """Guarda o ``n`` de cada ``VARCHAR(n)`` e as colunas ``SUPER`` de um ``CREATE TABLE``, que o
    ``COPY`` confere."""
    match = CREATE_TABLE_PATTERN.match(text)
    if match is None:
        return
    lengths = {}
    supers = set()
    for definition in split_top_level(match.group("body")):
        parts = definition.strip().split(None, 1)
        if len(parts) < 2:
            continue
        column = parts[0].strip('"').lower()
        kind = parts[1]
        varchar = re.search(r"(?:VARCHAR|CHARACTER VARYING)\s*\((\d+)\)", kind, re.IGNORECASE)
        if varchar:
            lengths[column] = int(varchar.group(1))
        if re.search(r"\bSUPER\b", kind, re.IGNORECASE):
            supers.add(column)
    table = table_key(match.group("name"))
    database.varchar_lengths[table] = lengths
    database.super_columns[table] = supers


def option_words(options: str) -> str:
    """As opções de um ``COPY`` ou ``UNLOAD`` em maiúsculas, com os valores citados esvaziados."""
    return QUOTED.sub("''", options).upper()


# ------------------------------------------------------------ o COPY


def copy(connection: Connection, text: str) -> Result:
    """O ``COPY`` de um Parquet, dos arquivos de um manifesto ou de um arquivo JSON com um objeto
    por linha, com as recusas que o ambiente alvo mostrou."""
    match = COPY_PATTERN.match(text)
    if match is None:
        raise server_error(f"COPY fora do que o substituto conhece: {text[:80]}")
    table = match.group("table")
    source = match.group("source")
    options = option_words(match.group("options"))
    target_columns = table_columns(connection, table)

    if "FORMAT JSON" in options or "FORMAT AS JSON" in options:
        return copy_json_lines(connection, table, target_columns, source)

    # O Redshift recusa TRUNCATECOLUMNS num COPY de Parquet (2026-09-21, plan/POC.md).
    if "TRUNCATECOLUMNS" in options:
        raise server_error("TRUNCATECOLUMNS argument is not supported for PARQUET based COPY",
                           "0A000")

    files = [source]
    if "MANIFEST" in options:
        manifest = json.loads(read_object(source))
        files = [entry["url"] for entry in manifest["entries"]]
    tables = []
    for uri in files:
        tables.append(pq.read_table(io.BytesIO(read_object(uri))))
    data = pa.concat_tables(tables)

    listed = None
    if match.group("columns"):
        listed = [name.strip().strip('"').lower() for name in match.group("columns").split(",")]
    fillrecord = "FILLRECORD" in options
    names = copy_column_names(target_columns, listed, data.num_columns, fillrecord=fillrecord)
    serialize_to_json = "SERIALIZETOJSON" in options
    check_copy(connection, table, names, data, serialize_to_json=serialize_to_json)

    loaded = pa.table(data.columns[:len(names)], names=names)
    insert_arrow(connection, table, loaded)
    return Result(rowcount=loaded.num_rows)


def table_columns(connection: Connection, table: str) -> list[str]:
    """As colunas da tabela, em ordem e em minúsculas."""
    cursor = connection.duckdb_connection.execute(f"SELECT * FROM {table} LIMIT 0")
    return [column[0].lower() for column in cursor.description]


def copy_column_names(target: list[str], listed: list[str] | None, file_columns: int, *,
                      fillrecord: bool) -> list[str]:
    """As colunas da tabela que recebem as do arquivo, por posição, com as recusas do Redshift
    (2026-09-21, ``plan/POC.md``): a lista de colunas com outra contagem, e o arquivo com menos
    colunas que a tabela sem ``FILLRECORD``."""
    if listed is not None:
        if len(listed) != file_columns:
            raise server_error("Spectrum Scan Error. Unmatched number of columns between the "
                               "column list and the file")
        return listed
    fewer_with_fillrecord = fillrecord and file_columns < len(target)
    if file_columns == len(target) or fewer_with_fillrecord:
        return target[:file_columns]
    raise server_error(
        "Spectrum Scan Error. error: Spectrum Scan Error code: 15007 context: Unmatched number "
        f"of columns between table and file. Table columns: {len(target)}, Data columns: "
        f"{file_columns}")


def check_copy(connection: Connection, table: str, names: list[str], data: pa.Table, *,
               serialize_to_json: bool) -> None:
    """As recusas do ``COPY`` que o ambiente alvo mostrou (2026-09-21, ``plan/POC.md``): o texto
    acima do ``VARCHAR(n)``, com o motivo nas tabelas de erro de carga, a coluna ``SUPER`` sem
    ``SERIALIZETOJSON`` e o texto acima de 65.535 bytes numa coluna ``SUPER``."""
    key = table_key(table)
    lengths = connection.database.varchar_lengths.get(key, {})
    supers = connection.database.super_columns.get(key, set())
    for index, name in enumerate(names):
        texts = [value for value in data.column(index).to_pylist() if isinstance(value, str)]
        longest = max((len(value.encode()) for value in texts), default=0)
        if name in supers and not serialize_to_json:
            raise server_error("SUPER column in COPY query requires SERIALIZETOJSON option")
        if name in supers and longest > 65535:
            raise server_error("1224 String value exceeds the max size of 65535 bytes")
        if name in lengths and longest > lengths[name]:
            record_load_error(connection, "String length exceeds DDL length")
            raise server_error("Load into table failed. Check 'stl_load_errors' system table for "
                               "details.")


def record_load_error(connection: Connection, reason: str) -> None:
    """Grava o motivo de uma carga recusada nas duas tabelas de erro que a suíte lê."""
    database = connection.duckdb_connection
    database.execute("INSERT INTO stl_load_errors VALUES (?, now())", [reason])
    database.execute("INSERT INTO sys_load_error_detail VALUES (?, now())", [reason])


def insert_arrow(connection: Connection, table: str, data: pa.Table) -> None:
    """Insere as colunas de ``data`` nas colunas de mesmo nome da tabela."""
    database = connection.duckdb_connection
    names = ", ".join(f'"{name}"' for name in data.column_names)
    database.register("substituto_copia", data)
    try:
        database.execute(f"INSERT INTO {table} ({names}) SELECT {names} FROM substituto_copia")
    except duckdb.Error as error:
        raise server_error(first_line(error)) from None
    finally:
        database.unregister("substituto_copia")


def copy_json_lines(connection: Connection, table: str, target: list[str],
                    source: str) -> Result:
    """O ``COPY ... FORMAT JSON 'auto'``: cada linha um objeto, cada chave na coluna de mesmo
    nome, e a coluna ``SUPER`` com o valor serializado."""
    supers = connection.database.super_columns.get(table_key(table), set())
    count = 0
    for line in read_object(source).decode().splitlines():
        if not line.strip():
            continue
        document = json.loads(line)
        names = [name for name in target if name in document]
        values = []
        for name in names:
            value = document[name]
            if name in supers:
                value = json.dumps(value)
            values.append(value)
        quoted = ", ".join(f'"{name}"' for name in names)
        placeholders = ", ".join("?" for _ in names)
        connection.duckdb_connection.execute(
            f"INSERT INTO {table} ({quoted}) VALUES ({placeholders})", values)
        count += 1
    return Result(rowcount=count)


# ------------------------------------------------------------ o UNLOAD


def unload(connection: Connection, text: str) -> Result:
    """O ``UNLOAD`` para Parquet, com ou sem ``PARTITION BY``, e o manifesto, com as recusas que o
    ambiente alvo mostrou (2026-09-21, ``plan/POC.md``): o ``LIMIT`` externo, e o destino com
    arquivos sem ``ALLOWOVERWRITE``. O ``select`` é o valor do literal, com a contrabarra como
    escape, e o resultado vazio não grava arquivo nem manifesto (leituras de 2026-09-23)."""
    match = UNLOAD_PATTERN.match(text)
    if match is None:
        raise server_error(f"UNLOAD fora do que o substituto conhece: {text[:80]}")
    select = literal_value(match.group("select"))
    target = match.group("target")
    options = option_words(match.group("options"))

    if re.search(r"\blimit\s+\d+\s*$", select, re.IGNORECASE):
        raise server_error("Limit clause is not supported in the outermost select of an UNLOAD "
                           "statement", "0A000")
    if "ALLOWOVERWRITE" not in options and object_uris(target):
        raise server_error(
            "Specified unload destination on S3 is not empty. Consider using a different "
            "bucket / prefix, manually removing the target files in S3, or using the "
            "ALLOWOVERWRITE option.")

    translated = to_duckdb(select, connection.database.all_super_columns())
    try:
        data = connection.duckdb_connection.execute(translated).to_arrow_table()
    except duckdb.Error as error:
        raise server_error(first_line(error)) from None

    # A contagem de pg_last_unload_count(); o resultado vazio para aqui, sem arquivo nem manifesto.
    connection.last_unload_count = data.num_rows
    if data.num_rows == 0:
        return Result()

    files = unload_files(as_unloaded(data), target, options)
    entries = write_unloaded_files(files)
    wants_manifest = "MANIFEST" in options
    if wants_manifest and not os.environ.get("SERIALIZE_DB_TEST_EMULATOR_NO_MANIFEST"):
        schema = files[0][1].schema
        manifest = unload_manifest(entries, schema, verbose="VERBOSE" in options)
        write_object(f"{target}manifest", manifest)
    return Result()


def as_unloaded(data: pa.Table) -> pa.Table:
    """A tabela como o ``UNLOAD`` a grava: toda coluna anulável, inclusive as ``NOT NULL`` da
    origem, e o texto em ``string``."""
    fields = []
    for column in data.schema:
        kind = column.type
        if kind == pa.large_string():
            kind = pa.string()
        fields.append(pa.field(column.name, kind, nullable=True))
    return data.cast(pa.schema(fields))


def unload_files(data: pa.Table, target: str, options: str) -> list[tuple[str, pa.Table]]:
    """Os arquivos do ``UNLOAD`` e as linhas de cada um: um por valor da coluna de
    ``PARTITION BY``, em ``<coluna>=<valor>/`` e sem a coluna, ou um só, ``000.parquet`` com
    ``PARALLEL OFF``."""
    partition = re.search(r"PARTITION BY\s*\(\s*(\w+)\s*\)", options)
    if partition is None:
        name = "000.parquet" if "PARALLEL OFF" in options else "0000_part_00.parquet"
        return [(f"{target}{name}", data)]

    column = partition.group(1).lower()
    files = []
    for value in sorted(set(data.column(column).to_pylist()), key=str):
        rows = data.filter(pc.equal(data.column(column), value)).drop_columns([column])
        files.append((f"{target}{column}={value}/0000_part_00.parquet", rows))
    return files


def write_unloaded_files(files: list[tuple[str, pa.Table]]) -> list[dict]:
    """Grava cada arquivo com o ``TIMESTAMP`` em ``INT96``, como o ``UNLOAD`` do ambiente alvo, e
    devolve as entradas do manifesto."""
    entries = []
    for uri, rows in files:
        buffer = io.BytesIO()
        pq.write_table(rows, buffer, use_deprecated_int96_timestamps=True)
        body = buffer.getvalue()
        write_object(uri, body)
        meta = {"content_length": len(body), "record_count": rows.num_rows}
        entries.append({"url": uri, "meta": meta})
    return entries


def unload_manifest(entries: list[dict], schema: pa.Schema, *, verbose: bool) -> bytes:
    """O manifesto do ``UNLOAD``: as entradas e, com ``VERBOSE``, o esquema e os totais."""
    manifest: dict[str, object] = {"entries": entries}
    if verbose:
        elements = []
        for column in schema:
            elements.append({"name": column.name, "type": {"base": str(column.type)}})
        manifest["schema"] = {"elements": elements}
        manifest["meta"] = {
            "content_length": sum(entry["meta"]["content_length"] for entry in entries),
            "record_count": sum(entry["meta"]["record_count"] for entry in entries),
        }
    return json.dumps(manifest).encode()
