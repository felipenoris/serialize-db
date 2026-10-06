"""O motor Redshift: o sandbox da execução nas tabelas ``exec_<id>_*`` do esquema, numa sessão sob
um lock.

A conexão vem de ``RedshiftConfig``: a credencial temporária do workgroup serverless
(``GetWorkgroup`` e ``GetCredentials``) ou o par informado, ``redshift_connector.connect`` sem
``timeout`` e com ``max_prepared_statements=0``, o ``USE`` no banco do datashare e o ``search_path``
no esquema. O motor guarda uma conexão, a sessão da execução, e um ``threading.RLock`` que toda
primitiva toma pelo tempo do seu comando; ``session()`` dá a conexão crua ao bloco, com o lock
tomado e reentrante na mesma thread, e ``new_session()`` abre outra conexão, com o seu lock. Uma
conexão derrubada pelo servidor é reaberta uma vez por comando, fora de transação, e o comando é
repetido; a carga de cada partição de ``ingest`` e de ``pinned_delta`` roda numa transação, para o
``COPY`` e o ``INSERT`` nunca se repetirem.

As primitivas:

- ``ingest`` carrega as partições pedidas da versão fixada em ``exec_<id>_<tabela>``, cada
  partição numa transação, por um ``COPY ... MANIFEST`` por lista de colunas dos arquivos, com a
  lista, numa staging sem a coluna de partição e um ``INSERT`` com o valor dela;
  ``pinned_delta`` carrega a versão fixada em ``exec_<id>_<tabela>_versao_<versão>`` e a devolve
  como origem de consulta;
- ``stream`` roda ``UNLOAD ... PARALLEL OFF`` para ``stream/<uuid>/`` sob o ``staging_prefix``
  (``<ambiente>/staging/<execution_id>/`` no sandbox, ``<id do leitor>/`` sob o ``unload_to`` do
  leitor Redshift) na thread de quem chama e lê os arquivos numa thread auxiliar, dois lotes à
  frente do cliente; ``query`` devolve a ``pa.Table`` do cursor, montada por colunas;
- ``create_table`` cria a tabela vazia do modelo em ``exec_<id>_<tabela>``, pelo DDL, anotada para
  o ``DROP`` do ``cleanup``;
- ``appender`` grava os lotes num Parquet do ``staging/`` numa thread auxiliar e, no ``close``,
  roda o ``COPY`` na tabela, com a lista das colunas do arquivo, numa transação; ``append`` é a
  forma por tabela;
- ``audit`` roda as verificações de ``serialize_db.audit`` e monta o ``AuditReport``;
- ``export_partition`` leva uma partição ao Delta pelo registro dos arquivos do ``UNLOAD``, e pela
  troca para ``publish_partition`` na partição com ``Double`` não finito.

O ``COPY`` e o ``UNLOAD`` levam a cláusula de credenciais de ``IAM_ROLE`` ou das credenciais da
sessão ``boto3``, montada por comando; o texto que a carrega passa por ``mask`` antes de qualquer
log ou exceção. Um statement Core é compilado pela cópia prefixada de ``sql.prefixed`` pelo dialeto
do Redshift com ``paramstyle="named"``; um texto pronto troca o sentinela ``{prefix}`` pelo prefixo
do motor (``exec_<id>_`` no sandbox, ``<ambiente>_`` no leitor Redshift) e passa por ``sql.bind``.

Exemplo:

.. code-block:: python

    from serialize_db.engine.redshift import RedshiftConfig, RedshiftEngine

    config = RedshiftConfig(workgroup="controladoria-wg", share_database="datalake_rw_shared",
                            schema="sbx_aco_decon", region="sa-east-1")
    staging = "prd/staging/exec-2026-09-05"
    with RedshiftEngine(config, "exec-2026-09-05", storage, staging) as engine:
        engine.ingest(Lancamento.__table__, uri, 143, partitions=["2026-07-31", "2026-08-31"])
        engine.query(sa.select(sa.func.count()).select_from(Lancamento.__table__))
"""

from __future__ import annotations

import contextlib
import dataclasses
import json
import logging
import os
import queue
import re
import threading
import uuid
from collections.abc import Collection, Iterable, Iterator, Mapping, Sequence

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import redshift_connector
import sqlalchemy as sa
from redshift_connector.utils.oids import RedshiftOID, get_datatype_name
from sqlalchemy.sql import quoted_name
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

from serialize_db import audit, delta, sql
from serialize_db.audit import AuditReport, CheckResult, KeyScope
from serialize_db.engine import batches_of, checked_batches, take
from serialize_db.engine.duckdb import environment_limits
from serialize_db.errors import ContractError, SandboxError
from serialize_db.schema import (
    arrow_schema,
    cast,
    check_partition_value,
    ddl,
    literal,
    quoted,
    sequential_key,
    sql_type,
    table_options,
)
from serialize_db.storage import Storage

__all__ = [
    "RedshiftConfig",
    "RedshiftEngine",
    "mask",
    "sandbox_prefix",
    "schema_from_row_description",
]

log = logging.getLogger("serialize_db.engine.redshift")

# O compilador dos statements Core: com o estilo named o dialeto não dobra o % dos literais, e o
# marcador :nome é o que o redshift_connector lê com cursor.paramstyle = "named".
_NAMED = RedshiftDialect_redshift_connector(paramstyle="named")

# O teto de um identificador do Redshift, em bytes, e o espaço que o prefixo deixa ao nome da
# tabela e aos sufixos _staging, _versao e _carga.
_IDENTIFIER_BYTES = 127
_TABLE_NAME_BYTES = 63

# As linhas até as quais o UNLOAD da exportação grava em série, num arquivo só (PARALLEL OFF): acima
# delas o UNLOAD roda em paralelo, que pode fragmentar por slice. No ambiente alvo, em 2026-10-05,
# o UNLOAD da partição ordenada pela sort_key, de 1.000.000 a 33.239.719 linhas de cad_lancamentos,
# levou o mesmo tempo nos dois modos (de 2,0 s a 50,3 s, razão de 0,98 a 0,99) e gravou um arquivo
# só em ambos (probes/operacao/probe_unload_parallel.py).
_PARALLEL_OFF_ROWS = 5_000_000

# A cláusula de credenciais que nunca vai a log: o valor de cada chave sai como ***.
_CREDENTIAL = re.compile(r"(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)\s+'[^']*'")

# Uma região citada, '...' ou "...", com a aspa dobrada como escape: a gramática com que sql.bind
# separa o texto citado dos marcadores :nome.
_QUOTED = re.compile(r"'(?:[^']|'')*'|\"(?:[^\"]|\"\")*\"")

# A contrabarra antes de ":" que o compilador do SQLAlchemy tira de um sa.text() depois de
# renderizar os valores, com o padrão do BIND_PARAMS_ESC dele: "\:nome" sem ":" nem letra depois.
_UNESCAPED_BY_THE_COMPILER = re.compile(r"\\(?=:[\w$]*(?![:\w$]))")

# O tipo Arrow de cada OID do resultado; o NUMERIC vem à parte, com a precisão e a escala do
# type_modifier. O texto, o CHAR e o SUPER saem em string, o tipo do JSON no motor DuckDB.
_ARROW_BY_OID = {
    int(RedshiftOID.BOOLEAN): pa.bool_(),
    int(RedshiftOID.SMALLINT): pa.int16(),
    int(RedshiftOID.INTEGER): pa.int32(),
    int(RedshiftOID.BIGINT): pa.int64(),
    int(RedshiftOID.REAL): pa.float32(),
    int(RedshiftOID.FLOAT): pa.float64(),
    int(RedshiftOID.CHAR): pa.string(),
    int(RedshiftOID.BPCHAR): pa.string(),
    int(RedshiftOID.VARCHAR): pa.string(),
    int(RedshiftOID.TEXT): pa.string(),
    int(RedshiftOID.NAME): pa.string(),  # os identificadores do catálogo: current_database()
    int(RedshiftOID.UNKNOWN): pa.string(),
    int(RedshiftOID.SUPER): pa.string(),
    int(RedshiftOID.DATE): pa.date32(),
    int(RedshiftOID.TIMESTAMP): pa.timestamp("us"),
    int(RedshiftOID.TIMESTAMPTZ): pa.timestamp("us", "UTC"),
}
_NUMERIC_OID = int(RedshiftOID.NUMERIC)

_END = object()


# ---------------------------------------------------------------- a configuração e a conexão


def _variable(
    environ: Mapping[str, str],
    name: str,
) -> str | None:
    """``SERIALIZE_DB_REDSHIFT_<name>``, com a variável vazia lida como ausente."""
    return environ.get(f"SERIALIZE_DB_REDSHIFT_{name}") or None


@dataclasses.dataclass(frozen=True, kw_only=True)
class RedshiftConfig:
    """A configuração do motor Redshift e da publicação.

    Exemplo:

    .. code-block:: python

        RedshiftConfig(workgroup="controladoria-wg", database="dev",
                       share_database="datalake_rw_shared", schema="sbx_aco_decon",
                       region="sa-east-1")
        RedshiftConfig.from_environment()   # as variáveis SERIALIZE_DB_REDSHIFT_*
    """

    workgroup: str | None = None
    """O workgroup serverless: o endereço por ``GetWorkgroup`` e a credencial temporária por
    ``GetCredentials``."""
    database: str = "dev"
    """O banco da conexão."""
    share_database: str | None = None
    """O banco do datashare que guarda o esquema, quando não é o da conexão: a sessão roda
    ``USE`` nele."""
    schema: str = "public"
    """O esquema das tabelas do sandbox e das publicadas."""
    iam_role: str | None = None
    """O papel do ``COPY`` e do ``UNLOAD``, um ARN ou ``default``; sem ele, as credenciais da
    sessão ``boto3``."""
    host: str | None = None
    """O endereço do par informado, ou o que substitui o do workgroup."""
    port: int = 5439
    """A porta de ``host``; sem ``host``, vale a do endereço do workgroup."""
    user: str | None = None
    """O usuário do par informado, usado com ``host`` e ``password``; sem os três, a conexão
    usa a credencial temporária do workgroup."""
    password: str | None = None
    """A senha do par informado, usada com ``host`` e ``user``."""
    region: str | None = None
    """A região das APIs do Redshift serverless e da sessão ``boto3``."""

    @staticmethod
    def from_environment(
        environ: Mapping[str, str] = os.environ,
    ) -> RedshiftConfig:
        """A configuração das variáveis ``SERIALIZE_DB_REDSHIFT_*`` (``WORKGROUP``, ``DATABASE``,
        ``SHARE_DATABASE``, ``SCHEMA``, ``IAM_ROLE``, ``HOST``, ``PORT``, ``USER``, ``PASSWORD``) e
        da região em ``AWS_REGION`` ou ``AWS_DEFAULT_REGION``.

        Exemplo:

        .. code-block:: python

            config = RedshiftConfig.from_environment()
            config.schema   # "sbx_aco_decon" com SERIALIZE_DB_REDSHIFT_SCHEMA=sbx_aco_decon

        :param environ: as variáveis lidas; o padrão é ``os.environ``. A variável vazia conta
            como ausente.
        :return: a configuração; a variável ausente deixa o padrão do campo.
        :raises ContractError: ``SERIALIZE_DB_REDSHIFT_PORT`` que não é um número inteiro.
        """
        port = _variable(environ, "PORT") or "5439"
        try:
            port_number = int(port)
        except ValueError:
            raise ContractError(
                f"SERIALIZE_DB_REDSHIFT_PORT={port!r}: a porta é um número inteiro, como 5439"
            ) from None
        return RedshiftConfig(
            workgroup=_variable(environ, "WORKGROUP"),
            database=_variable(environ, "DATABASE") or "dev",
            share_database=_variable(environ, "SHARE_DATABASE"),
            schema=_variable(environ, "SCHEMA") or "public",
            iam_role=_variable(environ, "IAM_ROLE"),
            host=_variable(environ, "HOST"),
            port=port_number,
            user=_variable(environ, "USER"),
            password=_variable(environ, "PASSWORD"),
            region=environ.get("AWS_REGION") or environ.get("AWS_DEFAULT_REGION") or None,
        )


def _workgroup_login(
    config: RedshiftConfig,
) -> dict[str, object]:
    """O endereço e o par usuário e senha do workgroup: ``GetWorkgroup`` e
    ``GetCredentials(durationSeconds=3600)``, o caminho de
    ``target_env_examples/redshift_native.py``."""
    serverless = boto3.client("redshift-serverless", region_name=config.region)
    endpoint = serverless.get_workgroup(workgroupName=config.workgroup)["workgroup"]["endpoint"]
    credentials = serverless.get_credentials(
        workgroupName=config.workgroup, dbName=config.database, durationSeconds=3600
    )
    return {
        "host": config.host or endpoint["address"],
        "port": config.port if config.host else int(endpoint["port"]),
        "user": credentials["dbUser"],
        "password": credentials["dbPassword"],
    }


def login_of(
    config: RedshiftConfig,
) -> dict[str, object]:
    """Os argumentos de conexão: o par informado, ou a credencial temporária do workgroup;
    protegida."""
    if config.host and config.user and config.password:
        return {
            "host": config.host,
            "port": config.port,
            "user": config.user,
            "password": config.password,
        }
    if config.workgroup:
        return _workgroup_login(config)
    raise ContractError(
        "RedshiftConfig sem conexão: informe workgroup, ou host, user e password "
        "(SERIALIZE_DB_REDSHIFT_WORKGROUP, ou _HOST, _USER e _PASSWORD)"
    )


def driver_connect(
    login: Mapping[str, object],
) -> object:
    """``redshift_connector.connect`` com os argumentos; protegida, a porta que os testes trocam
    pelo substituto local."""
    return redshift_connector.connect(**login)


def connect(
    config: RedshiftConfig,
) -> object:
    """Uma conexão nova pela configuração: sem ``timeout``, com ``max_prepared_statements=0``, o
    autocommit ligado antes do primeiro comando, o ``USE`` no banco do datashare e o
    ``search_path`` no esquema; protegida, para o motor e a publicação.

    O autocommit vem antes do ``USE``: desligado, o driver abre uma transação no primeiro comando,
    e o primeiro erro do servidor abortaria tudo o que vem depois (25P02). O cache de prepared
    statements fica desligado, porque o datashare recusa com ``34510`` um statement preparado antes
    de um ``TRUNCATE``.

    Ver também ``serialize_db.reader.open_redshift``, a entrada pública das consultas ao Redshift
    fora de uma execução: ``serialize_db.reader.RedshiftReader.query`` devolve o resultado em
    Arrow, e ``serialize_db.reader.RedshiftReader.session`` dá a conexão crua que esta função abre,
    com o lock tomado pelo bloco. Numa execução, a consulta no sandbox é ``RedshiftEngine.query``,
    o ``run.sandbox.query`` do pipeline.

    Exemplo:

    .. code-block:: python

        from serialize_db.engine.redshift import RedshiftConfig, connect

        connection = connect(RedshiftConfig.from_environment())   # SERIALIZE_DB_REDSHIFT_*
        cursor = connection.cursor()
        cursor.execute("SELECT start_time, trim(status), left(trim(query_text), 80) "
                       "FROM sys_query_history ORDER BY start_time DESC LIMIT 5")
        for row in cursor.fetchall():
            print(*row, sep=" | ")
        connection.close()
    """
    login = dict(login_of(config))
    login["database"] = config.database
    login["max_prepared_statements"] = 0
    connection = driver_connect(login)
    connection.autocommit = True
    cursor = connection.cursor()
    if config.share_database:
        cursor.execute(f"USE {config.share_database}")
    cursor.execute(f"SET search_path TO {config.schema}")
    return connection


def credentials_clause(
    config: RedshiftConfig,
) -> str:
    """Como o ``COPY`` e o ``UNLOAD`` alcançam o S3: ``IAM_ROLE`` com o ARN ou ``default`` quando
    a configuração o informa, senão ``ACCESS_KEY_ID``, ``SECRET_ACCESS_KEY`` e ``SESSION_TOKEN``
    das credenciais congeladas da sessão ``boto3``, o caminho do ambiente alvo; protegida.

    O texto devolvido carrega segredo: ele entra só no comando, e o comando passa por ``mask``
    antes de qualquer log ou exceção.
    """
    if config.iam_role == "default":
        return "IAM_ROLE default"
    if config.iam_role:
        return f"IAM_ROLE {literal(config.iam_role)}"
    credentials = boto3.Session(region_name=config.region).get_credentials()
    if credentials is None:
        raise SandboxError(
            "sem credenciais da AWS para o COPY e o UNLOAD: o boto3 não achou "
            "papel, variáveis AWS_* nem perfil, e a configuração não tem iam_role"
        )
    frozen = credentials.get_frozen_credentials()
    clause = (
        f"ACCESS_KEY_ID {literal(frozen.access_key)} SECRET_ACCESS_KEY {literal(frozen.secret_key)}"
    )
    if frozen.token:
        clause += f" SESSION_TOKEN {literal(frozen.token)}"
    return clause


def mask(
    text: str,
) -> str:
    """O texto com o valor de cada cláusula de credencial trocado por ``***``.

    Exemplo:

    .. code-block:: python

        mask("COPY t FROM 's3://b/m' ACCESS_KEY_ID 'AKIA' SECRET_ACCESS_KEY 'x' FORMAT AS PARQUET")
        # "COPY t FROM 's3://b/m' ACCESS_KEY_ID '***' SECRET_ACCESS_KEY '***' FORMAT AS PARQUET"

    :param text: o texto que pode levar a cláusula de credenciais, como o comando de um ``COPY``
        ou de um ``UNLOAD``.
    :return: o texto mascarado; as cláusulas mascaradas são ``ACCESS_KEY_ID``,
        ``SECRET_ACCESS_KEY`` e ``SESSION_TOKEN``.
    """
    return _CREDENTIAL.sub(r"\1 '***'", text)


def sandbox_prefix(
    execution_id: str,
) -> str:
    """O prefixo das tabelas do sandbox: ``exec_<id>_``, com o identificador em ``[a-z0-9_]``.

    O prefixo deixa 63 bytes ao nome da tabela e aos sufixos, dentro dos 127 bytes de um
    identificador do Redshift.

    Exemplo:

    .. code-block:: python

        sandbox_prefix("exec-2026-09-05")   # "exec_exec_2026_09_05_"

    :param execution_id: o identificador da execução; vira minúsculo, e cada caractere fora de
        ``[a-z0-9_]`` vira ``_``.
    :return: o prefixo, que antecede o nome de cada tabela do sandbox no esquema.
    :raises ContractError: um identificador mais longo, que não deixa os 63 bytes.
    """
    normalized = re.sub(r"[^a-z0-9_]", "_", execution_id.lower())
    prefix = f"exec_{normalized}_"
    if len(prefix.encode("utf-8")) + _TABLE_NAME_BYTES > _IDENTIFIER_BYTES:
        raise ContractError(
            f"execution_id {execution_id!r}: o prefixo {prefix!r} não deixa "
            f"{_TABLE_NAME_BYTES} bytes ao nome da tabela dentro dos "
            f"{_IDENTIFIER_BYTES} de um identificador do Redshift"
        )
    return prefix


# ---------------------------------------------------------------- os erros do servidor


def _server_fields(
    error: BaseException,
) -> Mapping[str, object]:
    """Os campos do erro do servidor que o driver guarda (``S``, ``C``, ``M``), ou vazio."""
    fields = error.args[0] if error.args else None
    return fields if isinstance(fields, Mapping) else {}


def relation_missing(
    error: BaseException,
) -> bool:
    """Se o erro do servidor é a relação inexistente: a mensagem ``does not exist``, que o Redshift
    responde com o SQLSTATE ``XX000`` (``Relation <nome> does not exist in the database.``, leitura
    de 2026-09-24), ou o ``42P01`` do PostgreSQL; protegida: o nome livre no sandbox e a tabela de
    controle ausente."""
    fields = _server_fields(error)
    return fields.get("C") == "42P01" or "does not exist" in str(fields.get("M", ""))


def relation_exists(
    error: BaseException,
) -> bool:
    """Se o erro do servidor é a relação que já existe, o SQLSTATE ``42P07`` ou a mensagem que o
    diz; protegida: a tabela publicada que outra primeira publicação criou."""
    fields = _server_fields(error)
    return fields.get("C") == "42P07" or "already exists" in str(fields.get("M", ""))


def serialization_failure(
    error: BaseException,
) -> bool:
    """Se o erro do servidor é a violação de isolamento serializável, o ``1023`` que a segunda de
    duas transações sobre a mesma tabela recebe; protegida."""
    message = str(_server_fields(error).get("M", ""))
    return "1023" in message or "Serializable isolation violation" in message


# ---------------------------------------------------------------- o texto dos comandos


def _staging_column_ddl(
    column: sa.Column,
) -> str:
    """A coluna de uma staging: anulável, e o documento JSON em ``VARCHAR(65535)``, o texto que o
    ``COPY`` de Parquet carrega e o ``JSON_PARSE`` do ``INSERT`` leva a ``SUPER``."""
    if isinstance(column.type, sa.JSON):
        return f"{quoted(column.name)} VARCHAR(65535)"
    return f"{quoted(column.name)} {sql_type(column, 'redshift')}"


def staging_ddl(
    table: sa.Table,
    name: str,
    columns: Iterable[sa.Column],
    temporary: bool = False,
) -> str:
    """O ``CREATE TABLE`` de uma staging com as ``columns`` do contrato, anuláveis, o JSON em
    ``VARCHAR(65535)`` e sem cláusula física; protegida. ``name`` vai como está, qualificado ou
    entre aspas por quem chama."""
    lines = []
    for column in columns:
        lines.append("    " + _staging_column_ddl(column))
    keyword = "CREATE TEMP TABLE" if temporary else "CREATE TABLE"
    return f"{keyword} {name} (\n" + ",\n".join(lines) + "\n)"


def columns_without_partition(
    table: sa.Table,
) -> list[sa.Column]:
    """As colunas do contrato sem a de partição, as que um arquivo da tabela tem; protegida."""
    partition_by = table_options(table).partition_by
    return [column for column in table.columns if column.name != partition_by]


def insert_from_staging(
    target: str,
    staging: str,
    table: sa.Table,
    value: str | None,
) -> str:
    """O ``INSERT INTO <target> (<colunas>) SELECT ... FROM <staging>`` na ordem do contrato, com a
    coluna de partição preenchida por ``value`` quando ele é informado e ``JSON_PARSE`` nas colunas
    JSON, que na staging são texto; protegida."""
    partition_by = table_options(table).partition_by
    names = []
    selected = []
    for column in table.columns:
        names.append(quoted(column.name))
        if column.name == partition_by and value is not None:
            selected.append(literal(value))
        elif isinstance(column.type, sa.JSON):
            selected.append(f"JSON_PARSE({quoted(column.name)})")
        else:
            selected.append(quoted(column.name))
    return f"INSERT INTO {target} ({', '.join(names)})\nSELECT {', '.join(selected)} FROM {staging}"


def copy_text(
    target: str,
    source: str,
    credentials: str,
    manifest: bool,
    columns: Sequence[str] | None = None,
) -> str:
    """O ``COPY ... FORMAT AS PARQUET`` de um manifesto ou de um arquivo, com ``FILLRECORD`` (o
    arquivo anterior a uma coluna nova entra com ela nula) e sem ``COMPUPDATE``; protegida. Sem
    ``columns``, as colunas do arquivo entram nas da tabela por posição; com elas, na ordem do
    arquivo, cada uma entra na de mesmo nome, e a coluna da tabela fora da lista fica nula. O texto
    carrega a cláusula de credenciais."""
    options = "FORMAT AS PARQUET FILLRECORD"
    if manifest:
        options = "FORMAT AS PARQUET MANIFEST FILLRECORD"
    listed = ""
    if columns is not None:
        names = ", ".join(quoted(name) for name in columns)
        listed = f" ({names})"
    return f"COPY {target}{listed}\nFROM {literal(source)}\n{credentials}\n{options}"


def unload_text(
    select: str,
    destination: str,
    credentials: str,
    parallel: bool,
) -> str:
    """O ``UNLOAD`` do ``select`` em Parquet para ``destination/``, com manifesto verboso e, sem
    ``parallel``, em série (``PARALLEL OFF``), na ordem do ``ORDER BY``; protegida.

    O ``select`` entra como literal com a contrabarra e a aspa simples dobradas: o literal do
    ``UNLOAD`` trata a contrabarra como escape, e o texto que o dialeto já escapou chega intacto
    só assim (leituras de 2026-09-23). O texto carrega a cláusula de credenciais.
    """
    escaped = select.replace("\\", "\\\\").replace("'", "''")
    text = (
        f"UNLOAD ('{escaped}')\nTO {literal(destination.rstrip('/') + '/')}\n{credentials}\n"
        "FORMAT AS PARQUET MANIFEST VERBOSE"
    )
    if not parallel:
        text += " PARALLEL OFF"
    return text


# ---------------------------------------------------------------- o esquema de um resultado


def _datatype_name(
    oid: int,
) -> str:
    """O nome do tipo de um OID pelo driver, ou o próprio número fora da lista dele."""
    try:
        return get_datatype_name(oid)
    except ValueError:
        return str(oid)


def _label(
    column: Mapping[str, object],
) -> str:
    """O nome da coluna do ``row_desc``, que o driver guarda em bytes."""
    label = column["label"]
    return label.decode("utf-8") if isinstance(label, bytes) else str(label)


def _numeric_type(
    name: str,
    modifier: int,
) -> pa.DataType:
    """O ``decimal128(p, s)`` de um ``NUMERIC`` pelo ``type_modifier`` do driver: a escala em
    ``(modifier - 4) & 0xFFFF`` e a precisão em ``((modifier - 4) >> 16) & 0xFFFF``."""
    if modifier == -1:
        raise SandboxError(f"{name}: NUMERIC sem precisão e escala no type_modifier")
    precision = ((modifier - 4) >> 16) & 0xFFFF
    scale = (modifier - 4) & 0xFFFF
    return pa.decimal128(precision, scale)


def schema_from_row_description(
    row_desc: Sequence[Mapping[str, object]],
) -> pa.Schema:
    """O esquema Arrow de um resultado pelo ``row_desc`` do cursor.

    ``BOOLEAN`` em ``bool``; ``SMALLINT``, ``INTEGER`` e ``BIGINT`` em ``int16``, ``int32`` e
    ``int64``; ``REAL`` e ``FLOAT`` em ``float32`` e ``float64``; ``NUMERIC`` em
    ``decimal128(p, s)`` pelo ``type_modifier``; ``CHAR``, ``VARCHAR``, ``TEXT``, ``NAME``,
    ``UNKNOWN`` e ``SUPER`` em ``string``; ``DATE`` em ``date32``; ``TIMESTAMP`` em
    ``timestamp[us]``; ``TIMESTAMPTZ`` em ``timestamp[us, UTC]``.

    Exemplo:

    .. code-block:: python

        schema_from_row_description([
            {"label": b"valor", "type_oid": 1700, "type_modifier": 1179654}])
        # valor: decimal128(18, 2)

    :param row_desc: a descrição das colunas que o driver guarda em ``cursor.ps["row_desc"]``,
        com ``label``, ``type_oid`` e ``type_modifier`` de cada coluna.
    :return: o esquema, um campo por coluna, na ordem do resultado.
    :raises SandboxError: outro OID, e um ``NUMERIC`` sem modificador, com o nome da coluna na
        mensagem.
    """
    fields = []
    for column in row_desc:
        name = _label(column)
        oid = int(column["type_oid"])
        if oid == _NUMERIC_OID:
            fields.append(pa.field(name, _numeric_type(name, int(column["type_modifier"]))))
        elif oid in _ARROW_BY_OID:
            fields.append(pa.field(name, _ARROW_BY_OID[oid]))
        else:
            raise SandboxError(f"{name}: tipo {_datatype_name(oid)} (OID {oid}) fora do contrato")
    return pa.schema(fields)


def table_from_cursor(
    cursor: object,
) -> pa.Table:
    """A ``pa.Table`` do resultado inteiro de um cursor, montada por colunas (``zip(*rows)``), com o
    esquema de ``schema_from_row_description``; vazia num comando sem resultado; protegida."""
    if cursor.description is None:
        return pa.table({})
    schema = schema_from_row_description(cursor.ps["row_desc"])
    rows = cursor.fetchall()
    # O resultado sem linha recebe uma coluna vazia por campo, que zip(*rows) não daria.
    columns = list(zip(*rows)) if rows else [() for _ in schema]
    arrays = []
    for column, field in zip(columns, schema):
        arrays.append(pa.array(list(column), type=field.type))
    return pa.Table.from_arrays(arrays, schema=schema)


# ---------------------------------------------------------------- a compilação


def _escaped_colons(
    text: str,
) -> str:
    """O texto com cada ``:`` das regiões citadas escrito ``\\:``: o ``sa.text()`` lê ``:nome``
    como parâmetro também dentro das aspas, e o ``literal_binds`` o trocaria por ``NULL``; o
    compilador devolve o ``:`` sem a contrabarra."""

    def escape(
        region: re.Match,
    ) -> str:
        return region.group(0).replace(":", "\\:")

    return _QUOTED.sub(escape, text)


def _compiler_proof(
    value: object,
) -> object:
    """O valor com a contrabarra antes de ``:`` repetida, a que o compilador tira do literal
    depois que o dialeto dobra cada contrabarra; o valor que não é texto passa como está."""
    if not isinstance(value, str):
        return value
    return _UNESCAPED_BY_THE_COMPILER.sub(r"\\\\", value)


def _text_with_values(
    text: str,
    params: Mapping[str, object] | None,
    prefix: str,
) -> sa.sql.ClauseElement:
    """Um texto pronto como statement com cada ``bindparam`` tipado pelo valor, para o caminho dos
    literais: o sentinela vira o prefixo, ``sql.bind`` confere os marcadores, o ``:`` das
    regiões citadas vai escapado e a contrabarra antes de ``:`` num valor, repetida; uma lista
    entra expansível, no ``IN``."""
    prefixed_text = text.replace(sql.SENTINEL, prefix)
    bound_text, values = sql.bind(prefixed_text, dict(params or {}), "redshift")
    parameters = []
    for name, value in values.items():
        if isinstance(value, (list, tuple, set)):
            items = [_compiler_proof(item) for item in value]
            parameters.append(sa.bindparam(name, value=items, expanding=True))
        else:
            parameters.append(sa.bindparam(name, value=_compiler_proof(value)))
    return sa.text(_escaped_colons(bound_text)).bindparams(*parameters)


def compiled_for_cursor(
    statement_or_sql: sa.sql.ClauseElement | str,
    params: Mapping[str, object] | None,
    prefix: str,
) -> tuple[str, dict[str, object]]:
    """O texto com os marcadores ``:nome`` e o dicionário do driver, para o cursor: o statement
    compilado pela cópia prefixada, com ``render_postcompile`` expandindo o ``IN`` de lista, ou o
    texto com o sentinela trocado pelo prefixo por ``sql.bind``; protegida."""
    if isinstance(statement_or_sql, str):
        return sql.bind(
            statement_or_sql.replace(sql.SENTINEL, prefix), dict(params or {}), "redshift"
        )
    bound = sql.bound_statement(statement_or_sql, params, prefix)
    compiled = bound.compile(dialect=_NAMED, compile_kwargs={"render_postcompile": True})
    return str(compiled), dict(compiled.construct_params())


def literal_text(
    statement_or_sql: sa.sql.ClauseElement | str,
    params: Mapping[str, object] | None,
    prefix: str,
) -> str:
    """O texto com os valores do cliente como literais, o que entra no ``UNLOAD``, que não recebe
    parâmetro; protegida. O dialeto dobra a aspa simples e a contrabarra e mantém o ``%``, e as
    regiões citadas de um texto pronto passam intactas, ``:nome`` inclusive."""
    if isinstance(statement_or_sql, str):
        statement = _text_with_values(statement_or_sql, params, prefix)
    else:
        statement = sql.bound_statement(statement_or_sql, params, prefix)
    compiled = statement.compile(
        dialect=_NAMED, compile_kwargs={"literal_binds": True, "render_postcompile": True}
    )
    # O texto sai sem o espaço e o ponto e vírgula finais: ele entra numa subconsulta e no UNLOAD.
    return str(compiled).strip().rstrip(";")


# ---------------------------------------------------------------- o stream


def _put(
    sink: queue.Queue,
    item: object,
    stop: threading.Event,
) -> bool:
    """Põe o item na fila, esperando em fatias de 50 ms; ``False`` quando ``stop`` chega
    antes."""
    while not stop.is_set():
        try:
            sink.put(item, timeout=0.05)
            return True
        except queue.Full:
            continue
    return False


def _unloaded_batches(
    storage: Storage,
    paths: Sequence[str],
    batch_size: int,
) -> Iterator[pa.RecordBatch]:
    """Os lotes dos arquivos do ``UNLOAD``, arquivo a arquivo, lidos pelo ``Storage`` com o
    ``INT96`` em microssegundos."""
    for path in paths:
        footer = pq.ParquetFile(storage.open_input_file(path), coerce_int96_timestamp_unit="us")
        yield from footer.iter_batches(batch_size)


def _read_unloaded(
    storage: Storage,
    paths: Sequence[str],
    schema: pa.Schema,
    batch_size: int,
    sink: queue.Queue,
    stop: threading.Event,
) -> None:
    """A thread do stream: entrega à fila cada lote dos arquivos do ``UNLOAD`` no esquema do
    statement, e ``_END`` no fim; a exceção da leitura vai à fila."""
    try:
        for batch in _unloaded_batches(storage, paths, batch_size):
            if not _put(sink, batch.cast(schema), stop):
                return
        _put(sink, _END, stop)
    except Exception as error:  # noqa: BLE001 - o erro da leitura vai ao cliente
        _put(sink, error, stop)


class RedshiftStream:
    """Os lotes de uma consulta do motor Redshift, lidos dos arquivos do ``UNLOAD`` dela.

    A construção roda, sob o lock e na thread de quem chama, a leitura do esquema do resultado
    (``select * from (<texto>) as t limit 0``) e o ``UNLOAD ... PARALLEL OFF`` para
    ``stream/<uuid>/`` sob o ``staging_prefix`` do motor; sem manifesto, ``pg_last_unload_count()``
    na mesma sessão separa o resultado vazio, sem lote, da falta do manifesto, que sobe. Depois, uma
    thread lê os arquivos pelo ``Storage`` e entrega cada lote, no esquema do statement, a uma fila
    de dois lotes, enquanto o cliente trabalha no anterior. ``close`` apaga o prefixo do stream.
    ``__arrow_c_stream__`` entrega os lotes a ``write_deltalake`` e a
    ``RecordBatchReader.from_stream``.
    """

    def __init__(
        self,
        engine: RedshiftEngine,
        text: str,
        batch_size: int = 100_000,
    ) -> None:
        # O stop vem antes de tudo: o __del__ de uma construção que falhou o usa.
        self._stop = threading.Event()
        self._engine = engine
        self._prefix = engine.storage.join(engine.staging_prefix, "stream", uuid.uuid4().hex)
        self._queue: queue.Queue = queue.Queue(maxsize=2)
        self._thread: threading.Thread | None = None
        self._finished = False
        with engine.session():
            self.schema = engine.result_schema(text)
            engine.execute(
                unload_text(
                    text,
                    engine.storage.uri_of(self._prefix),
                    credentials_clause(engine.config),
                    parallel=False,
                )
            )
            paths = engine.unloaded_paths(self._prefix)
        self._thread = threading.Thread(
            target=_read_unloaded,
            args=(engine.storage, paths, self.schema, batch_size, self._queue, self._stop),
            daemon=True,
        )
        self._thread.start()

    def read_next_batch(self) -> pa.RecordBatch:
        """O próximo lote; ``StopIteration`` no fim, e o erro da leitura no lugar do lote em que
        ele aconteceu."""
        if self._finished:
            raise StopIteration
        item = take(self._queue, self._stop)
        if item is None or item is _END:
            self._finished = True
            raise StopIteration
        if isinstance(item, BaseException):
            self._finished = True
            raise item
        return item

    def __iter__(self) -> Iterator[pa.RecordBatch]:
        while True:
            try:
                yield self.read_next_batch()
            except StopIteration:
                return

    def read_all(self) -> pa.Table:
        """Os lotes que faltam numa ``pa.Table``."""
        return pa.Table.from_batches(list(self), schema=self.schema)

    def __arrow_c_stream__(
        self,
        requested_schema: object = None,
    ) -> object:
        reader = pa.RecordBatchReader.from_batches(self.schema, iter(self))
        return reader.__arrow_c_stream__(requested_schema)

    def close(self) -> None:
        """Para a leitura e apaga os arquivos do ``UNLOAD`` deste stream."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=10)
        storage = self._engine.storage
        storage.delete(storage.list_files(self._prefix))

    def __enter__(self) -> RedshiftStream:
        return self

    def __exit__(
        self,
        *exc: object,
    ) -> None:
        self.close()

    def __del__(self) -> None:
        # O stream abandonado: a thread para no lote seguinte; os arquivos saem no cleanup.
        self._stop.set()


# ---------------------------------------------------------------- o appender


@dataclasses.dataclass
class _ParquetSink:
    """O arquivo Parquet de um appender no ``staging/``, aberto no primeiro lote."""

    storage: Storage
    path: str
    stream: object | None = None
    writer: pq.ParquetWriter | None = None

    def write(
        self,
        batch: pa.RecordBatch,
    ) -> None:
        """Grava o lote como um grupo de linhas, abrindo o arquivo no primeiro."""
        if self.writer is None:
            self.stream = self.storage.open_output_stream(self.path)
            self.writer = pq.ParquetWriter(self.stream, batch.schema)
        self.writer.write_batch(batch)

    def close(self) -> None:
        """Fecha o arquivo, quando algum lote o abriu: no S3, é o fim do upload."""
        if self.writer is not None:
            self.writer.close()
            self.stream.close()


def _write_until_end(
    sink: _ParquetSink,
    source: queue.Queue,
    closed: threading.Event,
    outcome: dict[str, object],
) -> None:
    """Grava cada lote tirado da fila até o fim dela; a exceção que o cliente pôs na fila, e o
    appender abandonado, sobem daqui."""
    item = take(source, closed)
    while item is not _END:
        if item is None:
            raise RuntimeError("appender encerrado sem close")
        if isinstance(item, BaseException):
            raise item
        sink.write(item)
        outcome["rows"] += item.num_rows
        item = take(source, closed)


def _write_parquet(
    sink: _ParquetSink,
    source: queue.Queue,
    closed: threading.Event,
    outcome: dict[str, object],
) -> None:
    """A thread do appender: grava no arquivo os lotes da fila, sem a sessão.

    Um ``closed`` sem o fim da fila é um appender abandonado. Terminada com erro, a thread apaga o
    arquivo, que nenhum ``COPY`` vai ler.
    """
    try:
        _write_until_end(sink, source, closed, outcome)
    except BaseException as error:  # noqa: BLE001 - relançado em close, ou no write seguinte
        outcome["error"] = error
    finally:
        sink.close()
    if outcome["error"] is not None:
        sink.storage.delete([sink.path])


def _write_file_manifest(
    sink: _ParquetSink,
    manifest_path: str,
) -> str:
    """Grava o manifesto do ``COPY`` com o arquivo do appender como a única entrada, obrigatória,
    e devolve a URI dele.

    Sem manifesto, o ``COPY`` de Parquet lê o caminho como prefixo e, sem objeto que case, não
    carrega nada e não dá erro (leitura de 2026-09-28). Com a entrada ``mandatory``, o ``COPY``
    falha quando o arquivo falta, e a URL exata não alcança outro objeto do mesmo prefixo.
    """
    entry = {
        "url": sink.storage.uri_of(sink.path),
        "mandatory": True,
        "meta": {"content_length": sink.storage.size(sink.path)},
    }
    sink.storage.write_text(manifest_path, json.dumps({"entries": [entry]}))
    return sink.storage.uri_of(manifest_path)


def _json_columns(
    table: sa.Table,
) -> list[str]:
    """Os nomes das colunas JSON, ``SUPER`` no Redshift."""
    return [column.name for column in table.columns if isinstance(column.type, sa.JSON)]


class RedshiftAppender:
    """O acréscimo em lotes a uma tabela do sandbox do motor Redshift.

    A abertura confere a tabela, sob o lock, por ``select 1 from <esquema>.<nome> limit 0``, e
    recusa com ``SandboxError`` o nome livre, que ``create_table`` ou o ``ingest`` ocupam.
    ``write`` faz o ``cast`` do lote na thread do cliente e o põe numa fila limitada; uma thread
    auxiliar grava os lotes, um grupo de linhas cada, num Parquet de
    ``staging/<execution_id>/<tabela>/``, pelo ``Storage``. ``close`` grava ao lado do arquivo um
    manifesto com ele como a única entrada, obrigatória, e roda, sob o lock e numa transação, o
    ``COPY ... MANIFEST`` na tabela (por uma staging temporária e ``JSON_PARSE`` quando a tabela
    tem coluna JSON), com a lista das colunas do arquivo: a coluna anulável que o lote não trouxe
    fica nula, a ``NOT NULL`` ausente faz a carga falhar (no ambiente alvo em 2026-10-06, o
    ``COPY`` direto com ``42601`` e o ``INSERT`` da staging com ``XX000``), e o arquivo ausente
    faz o ``COPY`` falhar. A tabela não muda antes dele, e um erro não deixa linha. Uma exceção
    dentro do ``with``, um lote recusado pelo ``cast`` ou um appender abandonado apagam o arquivo
    sem inserir nada, e a segunda chamada de ``close`` não faz nada.
    """

    def __init__(
        self,
        engine: RedshiftEngine,
        table: sa.Table,
        queue_depth: int = 2,
    ) -> None:
        # O closed vem antes de tudo: o __del__ de uma abertura recusada o usa.
        self._closed = threading.Event()
        self._name = engine.prefix + table.name
        if not engine.name_in_use(self._name):
            raise SandboxError(
                f"{table.name}: a tabela não existe no sandbox; crie-a por create_table ou pelo "
                f"ingest"
            )
        self._engine = engine
        self._table = table
        self._schema: pa.Schema | None = None
        self._refused: BaseException | None = None
        name = uuid.uuid4().hex
        folder = engine.storage.join(engine.staging_prefix, table.name)
        self._sink = _ParquetSink(engine.storage, engine.storage.join(folder, f"{name}.parquet"))
        # O manifesto fica ao lado do arquivo, num nome que não começa pelo caminho dele.
        self._manifest_path = engine.storage.join(folder, f"{name}.manifest")
        self._queue: queue.Queue = queue.Queue(maxsize=queue_depth)
        self._outcome: dict[str, object] = {"rows": 0, "error": None}
        self._thread = threading.Thread(
            target=_write_parquet,
            args=(self._sink, self._queue, self._closed, self._outcome),
            daemon=True,
        )
        self._thread.start()

    @property
    def rows(self) -> int:
        """As linhas gravadas até agora."""
        return self._outcome["rows"]

    @property
    def error(self) -> BaseException | None:
        """O erro da thread auxiliar ou o lote recusado, quando houve."""
        return self._refused or self._outcome["error"]

    def _put(
        self,
        item: object,
    ) -> bool:
        """Põe o item na fila; ``False`` quando a thread já terminou."""
        while self._thread.is_alive():
            try:
                self._queue.put(item, timeout=0.05)
                return True
            except queue.Full:
                continue
        return False

    def _converted(
        self,
        batch: pa.RecordBatch,
    ) -> pa.RecordBatch:
        """O lote no contrato, com as colunas do primeiro lote; outro conjunto é
        ``ContractError``."""
        converted = cast(batch, self._table)
        if self._schema is None:
            self._schema = converted.schema
        elif not converted.schema.equals(self._schema):
            raise ContractError(
                f"{self._table.name}: o lote traz {converted.schema.names}, e o "
                f"primeiro trouxe {self._schema.names}"
            )
        return converted

    def write(
        self,
        data: pa.RecordBatch | pa.Table,
    ) -> None:
        """Converte os lotes pelo contrato, na thread do cliente, e os põe na fila; um lote recusado
        faz o appender não inserir nada."""
        for batch in checked_batches(data):
            try:
                converted = self._converted(batch)
            except ContractError as error:
                self._refused = error
                raise
            if not self._put(converted):
                raise self.error or RuntimeError(
                    "a thread do appender terminou antes do fim da fila"
                )

    def _copy_file(
        self,
        manifest: str,
    ) -> None:
        """O ``COPY`` do arquivo na tabela pelo manifesto: direto, ou por uma staging temporária
        com o JSON em texto e o ``INSERT`` com ``JSON_PARSE`` quando a tabela tem coluna JSON."""
        engine = self._engine
        credentials = credentials_clause(engine.config)
        target = engine.qualified(self._name)
        # O COPY de Parquet é posicional: a lista leva cada coluna do arquivo, as do primeiro
        # lote, à de mesmo nome; a coluna anulável que o lote não trouxe fica nula, e a NOT NULL
        # ausente faz a carga falhar.
        columns = self._schema.names
        if not _json_columns(self._table):
            engine.execute(copy_text(target, manifest, credentials, manifest=True, columns=columns))
            return
        staging = quoted(f"{self._name}_carga")
        engine.execute(staging_ddl(self._table, staging, self._table.columns, temporary=True))
        engine.execute(copy_text(staging, manifest, credentials, manifest=True, columns=columns))
        engine.execute(insert_from_staging(target, staging, self._table, None))
        engine.execute(f"DROP TABLE {staging}")

    def close(
        self,
        error: BaseException | None = None,
    ) -> None:
        """Carrega os lotes na tabela, numa transação sob o lock; com ``error`` ou um lote
        recusado, só apaga o arquivo. A segunda chamada não faz nada."""
        # O close explícito dentro do with é seguido pelo do __exit__, que não roda outro COPY.
        if self._closed.is_set():
            return
        failure = error or self._refused
        self._put(failure if failure is not None else _END)
        self._closed.set()
        self._thread.join()
        try:
            written = self._sink.writer is not None
            if failure is None and self._outcome["error"] is None and written:
                # O manifesto sai antes da transação, que segura o lock só no COPY.
                manifest = _write_file_manifest(self._sink, self._manifest_path)
                with self._engine.transaction():
                    self._copy_file(manifest)
        finally:
            self._engine.storage.delete([self._sink.path, self._manifest_path])
        if error is None and self.error is not None:
            raise self.error

    def __enter__(self) -> RedshiftAppender:
        return self

    def __exit__(
        self,
        exc_type: object,
        exc: BaseException | None,
        tb: object,
    ) -> None:
        self.close(error=exc)

    def __del__(self) -> None:
        self._closed.set()


# ---------------------------------------------------------------- o motor


@dataclasses.dataclass(frozen=True)
class _PinnedStaging:
    """A staging ``_versao_<versão>`` da versão fixada de uma tabela: ``pinned_delta`` a carrega
    na hora, e a auditoria só quando uma verificação que a cita roda."""

    table: sa.Table
    name: str
    uri: str
    version: int


class RedshiftEngine:
    """O sandbox Redshift de uma execução: as tabelas ``exec_<id>_*`` do esquema, uma sessão e um
    ``RLock``.

    Exemplo:

    .. code-block:: python

        engine = RedshiftEngine(RedshiftConfig.from_environment(), "exec-2026-09-05", storage,
                                "prd/staging/exec-2026-09-05")
        try:
            engine.ingest(Operacao.__table__, uri, 3)
            print(engine.query("SELECT count(*) FROM {prefix}cad_operacoes"))
        finally:
            engine.cleanup()
    """

    def __init__(
        self,
        config: RedshiftConfig,
        execution_id: str,
        storage: Storage | None,
        staging_prefix: str | None,
        parent: RedshiftEngine | None = None,
        prefix: str | None = None,
    ) -> None:
        """Abre a sessão da execução no esquema, pelo caminho de ``connect``.

        :param config: a configuração do Redshift.
        :param execution_id: o identificador da execução, na regra da partição; dá o prefixo
            ``exec_<id>_`` das tabelas do sandbox.
        :param storage: o armazenamento da raiz do banco, onde ficam o Delta e os arquivos que o
            ``COPY`` lê e o ``UNLOAD`` grava, ou, no leitor Redshift, o de ``unload_to``; ``None``
            só no leitor sem ``unload_to``, que roda ``query`` e nada que toque arquivos.
        :param staging_prefix: a pasta dos arquivos intermediários da execução, relativa à raiz,
            como ``<ambiente>/staging/<execution_id>``; ``None`` com ``storage`` ``None``.
        :param parent: o motor principal, de que esta sessão a mais depende, dado por
            ``new_session``; a sessão abre outra conexão, com o seu lock, vê as tabelas que a
            principal confirmou, e o seu ``cleanup`` fecha só essa conexão. ``None`` no motor
            principal.
        :param prefix: o prefixo das tabelas do esquema que os statements citam; ``None`` é
            ``sandbox_prefix(execution_id)``, o ``exec_<id>_`` do sandbox, e o leitor Redshift
            passa ``<ambiente>_``, o das tabelas publicadas.
        :raises ContractError: ``execution_id`` fora da regra da partição ou, sem ``prefix``,
            longo demais para o prefixo; ou a configuração sem conexão: sem ``workgroup``, e sem
            ``host``, ``user`` e ``password``.
        """
        self.config = config
        """A configuração da conexão, do ``COPY`` e do ``UNLOAD``."""
        self.execution_id = check_partition_value(execution_id)
        """O identificador da execução, na regra da partição (``schema.PARTITION_VALUE``)."""
        self.prefix = prefix if prefix is not None else sandbox_prefix(execution_id)
        """O prefixo das tabelas no esquema: ``exec_<id>_`` no sandbox, ``<ambiente>_`` no
        leitor Redshift."""
        self.storage = storage
        """O armazenamento da raiz do banco, ou o de ``unload_to`` no leitor Redshift; ``None`` no
        leitor sem ``unload_to``."""
        self.staging_prefix = storage.join(staging_prefix) if storage is not None else None
        """A pasta dos arquivos intermediários da execução, relativa à raiz, que o ``cleanup``
        esvazia; ``None`` sem armazenamento."""
        self._lock = threading.RLock()
        self._owner: int | None = None
        self._closed = False
        self._in_transaction = False
        self._parent = parent
        self._pending: dict[str, _PinnedStaging] = {}
        # A sessão a mais divide com a principal as tabelas a apagar e as stagings já carregadas.
        if parent is not None:
            self._created = parent._created
            self._loaded = parent._loaded
            self._loaded_lock = parent._loaded_lock
        else:
            self._created: list[str] = []
            self._loaded: set[str] = set()
            self._loaded_lock = threading.Lock()
        self._connection = connect(config)
        log.info(
            "sessão %s aberta no esquema %s com o prefixo %s",
            self.execution_id,
            config.schema,
            self.prefix,
        )

    # ------------------------------------------------------------ a sessão

    @contextlib.contextmanager
    def session(self) -> Iterator[object]:
        """A conexão crua com o lock tomado pelo bloco, reentrante na mesma thread: uma primitiva
        chamada dentro do bloco não trava.

        Exemplo:

        .. code-block:: python

            with engine.session():
                engine.execute("CREATE TEMP TABLE ids (id BIGINT)")
                engine.query("SELECT count(*) FROM ids")   # a primitiva dentro do bloco não trava

        :return: o gerenciador de contexto cujo ``with`` dá a conexão do ``redshift_connector``
            da sessão, com o autocommit ligado.
        """
        with self._lock:
            outer_owner = self._owner
            self._owner = threading.get_ident()
            try:
                yield self._connection
            finally:
                self._owner = outer_owner

    def holds_session(self) -> bool:
        """Se a thread que chama está dentro de ``session()``.

        Exemplo:

        .. code-block:: python

            with engine.session():
                engine.holds_session()   # True
            engine.holds_session()       # False

        :return: ``True`` dentro do bloco.
        """
        return self._owner == threading.get_ident()

    def new_session(self) -> RedshiftEngine:
        """Uma sessão a mais: outra conexão pelo caminho de ``connect``, com credencial própria, o
        ``USE`` e o ``search_path``, e o seu lock; vê as tabelas ``exec_<id>_*`` que a principal
        confirmou e não as temporárias dela.

        Exemplo:

        .. code-block:: python

            with engine.new_session() as session:
                session.ingest(Contrato.__table__, uri, 88)

        :return: o motor da sessão a mais, gerenciador de contexto; o ``cleanup`` dele fecha só
            essa sessão, a conexão dela.
        """
        return RedshiftEngine(
            self.config,
            self.execution_id,
            self.storage,
            self.staging_prefix,
            parent=self,
            prefix=self.prefix,
        )

    def _reconnect(self) -> None:
        """A conexão reaberta com credencial nova, no lugar da que o servidor derrubou."""
        # O close da conexão derrubada levanta o InterfaceError do driver ou o OSError do socket,
        # e a conexão já não serve.
        with contextlib.suppress(redshift_connector.Error, OSError):
            self._connection.close()
        self._connection = connect(self.config)

    def _run(
        self,
        text: str,
        params: Mapping[str, object] | None,
    ) -> object:
        """Um comando num cursor novo da conexão; o erro do servidor leva o comando mascarado numa
        nota."""
        cursor = self._connection.cursor()
        cursor.paramstyle = "named"
        try:
            cursor.execute(text, params)
        except redshift_connector.Error as error:
            error.add_note(f"comando: {mask(text)}")
            raise
        return cursor

    def execute(
        self,
        text: str,
        params: Mapping[str, object] | None = None,
    ) -> object:
        """Roda um comando na sessão, sob o lock.

        Uma conexão derrubada pelo servidor (``InterfaceError`` do driver) é reaberta uma vez, com
        credencial nova, e o comando é repetido, fora de transação. A reconexão perde a tabela
        temporária que o pipeline tenha criado na sessão, e o log ``serialize_db.engine.redshift``
        avisa da perda. O driver não diz se o servidor aplicou o comando derrubado: um comando que
        não pode entrar duas vezes, como o ``COPY`` e o ``INSERT`` da carga de uma partição, roda
        em ``transaction()``, onde a queda sobe sem repetição.

        Exemplo:

        .. code-block:: python

            name = engine.qualified(engine.prefix + "cad_lancamentos")
            engine.execute(f"SELECT count(*) FROM {name}").fetchone()   # [120]

        :param text: o comando no SQL do Redshift, com cada parâmetro marcado como
            ``:nome``.
        :param params: os valores dos marcadores, por nome; ``None`` sem marcador.
        :return: o cursor do comando, com o resultado a ler.
        :raises redshift_connector.InterfaceError: a conexão derrubada dentro de uma transação;
            o erro sobe, porque a transação se perdeu.
        :raises redshift_connector.Error: o erro do servidor, com o comando mascarado por
            ``mask`` numa nota.
        """
        with self.session():
            try:
                return self._run(text, params)
            except redshift_connector.InterfaceError as error:
                if self._in_transaction:
                    raise
                log.warning(
                    "sandbox %s: conexão derrubada (%s); reaberta com credencial nova, e "
                    "a tabela temporária da sessão, se havia, se perdeu",
                    self.execution_id,
                    error,
                )
                self._reconnect()
                return self._run(text, params)

    @contextlib.contextmanager
    def transaction(self) -> Iterator[None]:
        """``BEGIN`` e ``COMMIT`` em volta do bloco, sob o lock; uma exceção sai por ``ROLLBACK``.
        Protegida, para o appender e para a carga de cada partição de ``ingest`` e ``pinned_delta``.

        O ``COMMIT`` e o ``ROLLBACK`` rodam dentro da transação, sem a reconexão de ``execute``:
        numa conexão nova, eles não teriam transação a fechar.

        Exemplo:

        .. code-block:: python

            with engine.transaction():
                engine.execute(ddl(Projetado.__table__, "redshift", prefix=engine.prefix))
                engine.register_created(engine.prefix + "cad_lancamentos_projetados")

        :return: o gerenciador de contexto da transação; o ``with`` dá ``None``.
        :raises redshift_connector.InterfaceError: a conexão derrubada no bloco, que perde a
            transação, ou no ``COMMIT``, cujo resultado fica desconhecido; o comando seguinte,
            fora da transação, reabre a conexão.
        """
        with self.session():
            self.execute("BEGIN")
            self._in_transaction = True
            try:
                yield
            except BaseException:
                with contextlib.suppress(redshift_connector.Error):
                    self.execute("ROLLBACK")
                raise
            else:
                self.execute("COMMIT")
            finally:
                self._in_transaction = False

    def qualified(
        self,
        name: str,
    ) -> str:
        """O nome em duas partes que resolve depois do ``USE``.

        Exemplo:

        .. code-block:: python

            engine.qualified("exec_exec_42_cad_contas")
            # '"sbx_aco_decon"."exec_exec_42_cad_contas"'

        :param name: o nome da tabela no esquema, sem aspas.
        :return: ``"<esquema>"."<nome>"``, com o esquema da configuração.
        """
        return f"{quoted(self.config.schema)}.{quoted(name)}"

    def register_created(
        self,
        name: str,
    ) -> None:
        """Anota uma tabela do sandbox para o ``DROP`` do ``cleanup``; protegida.

        Exemplo:

        .. code-block:: python

            engine.register_created(engine.prefix + "cad_lancamentos_projetados")

        :param name: o nome da tabela no esquema, sem aspas.
        """
        self._created.append(name)

    def name_in_use(
        self,
        name: str,
    ) -> bool:
        """Se uma tabela com o nome existe no esquema, por ``select 1 ... limit 0`` sob o lock: o
        erro de relação inexistente é o nome livre.

        Exemplo:

        .. code-block:: python

            engine.name_in_use(engine.prefix + "cad_lancamentos")   # True depois do ingest

        :param name: o nome da tabela no esquema, sem aspas, com o prefixo ``exec_<id>_`` numa
            tabela do sandbox.
        :return: ``True`` quando a tabela existe.
        """
        try:
            self.execute(f"SELECT 1 FROM {self.qualified(name)} LIMIT 0")
        except redshift_connector.Error as error:
            if relation_missing(error):
                return False
            raise
        return True

    # ------------------------------------------------------------ a carga do Delta

    def _manifest_folder(
        self,
        table: sa.Table,
        value: str | None,
    ) -> str:
        """Uma pasta nova dos manifestos do ``COPY`` no ``staging/`` da execução."""
        tag = value if value is not None else "tabela"
        path = self.storage.join(self.staging_prefix, table.name, f"{tag}_{uuid.uuid4().hex[:8]}")
        return self.storage.uri_of(path)

    def _copy_partition(
        self,
        table: sa.Table,
        name: str,
        staging: str,
        uri: str,
        version: int,
        value: str | None,
    ) -> None:
        """Uma partição da versão fixada na tabela do sandbox, numa transação: os manifestos, um
        ``COPY`` de cada na staging vazia, com a lista das colunas dos arquivos dele, e o ``INSERT``
        com o valor da partição.

        A transação tira a repetição de ``execute``: o driver não diz se o servidor aplicou o
        comando derrubado, e o ``COPY`` ou o ``INSERT`` repetido carregaria a partição duas vezes;
        a queda sobe como ``InterfaceError``, e o servidor desfaz a transação.
        """
        partitions = [value] if value is not None else None
        manifests = delta.copy_manifest(
            uri, version, partitions, self._manifest_folder(table, value), self.storage
        )
        with self.transaction():
            self.execute(f"DELETE FROM {self.qualified(staging)}")
            # O COPY de Parquet é posicional: a lista leva cada coluna do arquivo à de mesmo nome,
            # e a coluna que o arquivo não tem fica nula.
            for manifest in manifests:
                credentials = credentials_clause(self.config)
                self.execute(
                    copy_text(
                        self.qualified(staging),
                        manifest.uri,
                        credentials,
                        manifest=True,
                        columns=manifest.columns,
                    )
                )
            self.execute(
                insert_from_staging(self.qualified(name), self.qualified(staging), table, value)
            )

    def _load_from_delta(
        self,
        table: sa.Table,
        name: str,
        uri: str,
        version: int,
        values: Sequence[str | None],
    ) -> None:
        """A tabela ``name`` criada com o DDL do modelo e carregada com as partições ``values`` da
        versão fixada, por uma staging sem a coluna de partição, apagada no fim."""
        staging = f"{name}_staging"
        # A cópia do modelo leva name sem o prefixo, que o ddl acrescenta.
        model = table.to_metadata(sa.MetaData(), name=name.removeprefix(self.prefix))
        self.execute(ddl(model, "redshift", prefix=self.prefix))
        self.register_created(name)
        self.execute(staging_ddl(table, self.qualified(staging), columns_without_partition(table)))
        self.register_created(staging)
        # A staging sai do registro só depois do DROP: a que ele não alcança fica para o cleanup.
        try:
            for value in values:
                self._copy_partition(table, name, staging, uri, version, value)
        finally:
            self.execute(f"DROP TABLE IF EXISTS {self.qualified(staging)}")
            self._created.remove(staging)

    def _partitions_to_load(
        self,
        table: sa.Table,
        uri: str,
        version: int,
        partitions: Sequence[str] | None,
    ) -> list[str | None]:
        """As partições com arquivo na versão fixada: as pedidas, com um valor repetido contado
        uma vez, ou todas."""
        partition_by = table_options(table).partition_by
        if partition_by is None and partitions is not None:
            raise ContractError(
                f"{table.name}: tabela sem partição recebeu partitions={partitions}"
            )
        available = delta.partition_values(
            delta.open_table(uri, self.storage, version), partition_by
        )
        if partitions is None:
            return available
        wanted = sorted({check_partition_value(value) for value in partitions})
        return [value for value in wanted if value in available]

    def ingest(
        self,
        table: sa.Table,
        uri: str,
        version: int | None,
        partitions: list[str] | None = None,
        materialize: bool = False,
    ) -> None:
        """Carrega as partições pedidas da versão fixada da tabela Delta na tabela
        ``exec_<id>_<tabela>`` do esquema: por partição, numa transação, um ``COPY ... MANIFEST
        FILLRECORD`` por lista de colunas dos arquivos, com a lista, numa staging sem a coluna de
        partição, e um ``INSERT`` com o valor dela.

        O ``COPY`` lê os arquivos da versão na pasta da tabela Delta, listados nos manifestos que o
        motor grava sob o ``staging_prefix``. Um commit na tabela depois da abertura não muda o que
        foi carregado. A transação de cada partição deixa a queda da conexão subir sem a repetição
        de ``execute``, que carregaria a partição duas vezes.

        Exemplo:

        .. code-block:: python

            engine.ingest(Lancamento.__table__, uri, 143, partitions=previous)

        :param table: a tabela do modelo, cujo nome a ingestão ocupa no sandbox, com o prefixo
            ``exec_<id>_``.
        :param uri: a URI da pasta da tabela Delta de origem.
        :param version: a versão fixada da tabela; ``None``, a tabela sem versão no Delta.
        :param partitions: os valores de partição a ler, com um valor repetido contado uma vez;
            ``None`` lê todas, e a lista vazia, nenhuma.
        :param materialize: não muda nada, porque o Redshift não lê o Delta no lugar, e a
            tabela é sempre carregada.
        :raises SandboxError: o nome ocupado no sandbox, ou a tabela que não existe no Delta,
            sem versão (``version=None``); e, sem ``iam_role``, a sessão ``boto3`` sem
            credenciais para o ``COPY``.
        :raises ContractError: ``partitions`` numa tabela sem partição, ou um valor fora da regra
            da partição.
        :raises redshift_connector.Error: o ``COPY`` recusado por uma coluna da versão que o
            modelo não tem, porque a lista de colunas do rodapé a nomeia e a staging, criada do
            modelo, não a tem; o modelo tem de acompanhar a tabela.
        :raises redshift_connector.InterfaceError: a conexão derrubada na transação de uma
            partição, que o servidor desfaz; a tabela fica criada, sem essa partição e sem as
            seguintes.
        """
        if version is None:
            raise SandboxError(f"{table.name}: sem versão fixada, a tabela não existe no Delta")
        name = self.prefix + table.name
        if self.name_in_use(name):
            raise SandboxError(f"{table.name}: o nome já está ocupado no sandbox")
        values = self._partitions_to_load(table, uri, version, partitions)
        self._load_from_delta(table, name, uri, version, values)

    def _source(
        self,
        table: sa.Table,
        name: str,
    ) -> sa.FromClause:
        """A tabela ``name`` como origem de consulta com as colunas do contrato."""
        columns = []
        for column in table.columns:
            columns.append(sa.column(quoted_name(column.name, quote=True), column.type))
        return sa.table(quoted_name(name, quote=True), *columns)

    def _ensure_loaded(
        self,
        staging: _PinnedStaging,
    ) -> None:
        """A staging ``_versao_<versão>`` carregada uma vez por execução, entre as sessões."""
        with self._loaded_lock:
            if staging.name in self._loaded:
                return
            values = delta.partition_values(
                delta.open_table(staging.uri, self.storage, staging.version),
                table_options(staging.table).partition_by,
            )
            self._load_from_delta(staging.table, staging.name, staging.uri, staging.version, values)
            self._loaded.add(staging.name)

    def pinned_delta(
        self,
        table: sa.Table,
        uri: str,
        version: int | None,
    ) -> sa.FromClause:
        """A versão fixada da tabela como origem de consulta, sem ocupar o nome do modelo no
        sandbox: a staging ``exec_<id>_<tabela>_versao_<versão>``, carregada uma vez por
        execução e por versão com todas as partições dela. Outra versão, depois de
        ``run.publish_delta`` avançar ``versions``, entra numa staging nova, e as duas ficam no
        esquema até o ``cleanup``.

        Exemplo:

        .. code-block:: python

            previous = engine.pinned_delta(Projetado.__table__, uri, 57)
            engine.query(sa.select(sa.func.max(previous.c.id_lancamento)))

        :param table: a tabela do modelo, que dá as colunas.
        :param uri: a URI da pasta da tabela Delta lida.
        :param version: a versão fixada.
        :return: o ``FromClause`` com as colunas do contrato, para os statements Core, sobre a
            staging.
        :raises SandboxError: numa tabela que ainda não existe, sem versão (``version=None``);
            e, sem ``iam_role``, a sessão ``boto3`` sem credenciais para o ``COPY``.
        :raises redshift_connector.InterfaceError: a conexão derrubada na transação de uma
            partição da carga, que o servidor desfaz; a staging fica criada, sem essa partição e
            sem as seguintes, até o ``cleanup``.
        """
        if version is None:
            raise SandboxError(f"{table.name}: sem versão fixada, a tabela ainda não existe")
        staging = self._pinned_staging(table, uri, version)
        self._ensure_loaded(staging)
        return self._source(table, staging.name)

    def _pinned_staging(
        self,
        table: sa.Table,
        uri: str,
        version: int,
    ) -> _PinnedStaging:
        """A staging da versão, com a versão no nome: cada versão pedida tem a sua."""
        name = f"{self.prefix}{table.name}_versao_{version}"
        return _PinnedStaging(table, name, uri, version)

    # ------------------------------------------------------------ consulta, stream e carga

    def query(
        self,
        statement_or_sql: sa.sql.ClauseElement | str,
        params: Mapping[str, object] | None = None,
    ) -> pa.Table:
        """O resultado inteiro como ``pa.Table``, sob o lock.

        Exemplo:

        .. code-block:: python

            engine.query(sa.select(tabela).where(tabela.c.data_str == sa.bindparam("p")),
                         {"p": "2026-08-31"})

        :param statement_or_sql: um statement Core sobre as tabelas do modelo, que o motor
            compila para o sandbox, ou um texto pronto no SQL do motor, com os parâmetros como
            ``:nome``; o sentinela ``{prefix}`` do texto vira o prefixo do motor (``prefix``).
        :param params: os valores dos parâmetros, por nome, dos ``bindparam`` sem valor do
            statement ou dos marcadores do texto.
        :return: a tabela do resultado, montada por colunas com o esquema do ``row_desc``; um
            comando sem resultado devolve a tabela vazia.
        :raises SqlError: os nomes de ``params`` não fecham com os parâmetros do statement ou do
            texto.
        :raises SandboxError: uma coluna do resultado num tipo fora do contrato.
        """
        text, arguments = compiled_for_cursor(statement_or_sql, params, self.prefix)
        cursor = self.execute(text, arguments or None)
        return table_from_cursor(cursor)

    def result_schema(
        self,
        text: str,
    ) -> pa.Schema:
        """O esquema do resultado de um texto, pelo ``row_desc`` de ``select * from (<texto>) as t
        limit 0``; protegida, para o stream.

        Exemplo:

        .. code-block:: python

            engine.result_schema("SELECT 1 AS um").names   # ["um"]

        :param text: a consulta no SQL do Redshift, sem marcador de parâmetro.
        :return: o esquema Arrow de ``schema_from_row_description``.
        :raises SandboxError: uma coluna do resultado num tipo fora do contrato.
        """
        cursor = self.execute(f"SELECT * FROM ({text}) AS t LIMIT 0")
        return schema_from_row_description(cursor.ps["row_desc"])

    def unloaded_paths(
        self,
        prefix: str,
    ) -> list[str]:
        """Os arquivos que o ``UNLOAD`` para ``prefix`` gravou, pelo manifesto; protegida.

        Exemplo:

        .. code-block:: python

            engine.unloaded_paths("prd/staging/exec-42/stream/ab12")
            # ["prd/staging/exec-42/stream/ab12/000.parquet"]

        :param prefix: o destino do ``UNLOAD``, relativo à raiz.
        :return: os caminhos, relativos à raiz; sem manifesto, a lista vazia quando
            ``pg_last_unload_count()`` na mesma sessão dá o resultado vazio (0).
        :raises FileNotFoundError: a falta do manifesto depois de um ``UNLOAD`` de alguma linha.
        """
        try:
            text, _ = self.storage.read_text(self.storage.join(prefix, "manifest"))
        except FileNotFoundError:
            count = self.execute("SELECT pg_last_unload_count()").fetchone()[0]
            if count == 0:
                return []
            raise FileNotFoundError(
                f"{self.storage.uri_of(prefix)}/manifest ausente depois de um "
                f"UNLOAD de {count} linha(s)"
            ) from None
        return [self.storage.relative(entry["url"]) for entry in json.loads(text)["entries"]]

    def stream(
        self,
        statement_or_sql: sa.sql.ClauseElement | str,
        params: Mapping[str, object] | None = None,
        batch_size: int = 100_000,
    ) -> RedshiftStream:
        """Os lotes da consulta lidos dos arquivos do ``UNLOAD`` dela, dois lotes à frente do
        cliente.

        Exemplo:

        .. code-block:: python

            with engine.stream(sa.select(tabela), batch_size=100_000) as stream:
                for batch in stream:
                    work(batch)

        :param statement_or_sql: um statement Core sobre as tabelas do modelo, que o motor
            compila para o sandbox, ou um texto pronto no SQL do motor, com os parâmetros como
            ``:nome``; o sentinela ``{prefix}`` do texto vira o prefixo do motor (``prefix``).
        :param params: os valores dos parâmetros, por nome, dos ``bindparam`` sem valor do
            statement ou dos marcadores do texto. Os valores do cliente entram como literais,
            porque o ``UNLOAD`` não recebe parâmetro.
        :param batch_size: o máximo de linhas de cada lote, lido de cada arquivo do ``UNLOAD``;
            o último lote de cada arquivo pode ser menor.
        :return: o ``BatchStream`` dos lotes, gerenciador de contexto; o ``close`` apaga os
            arquivos do ``UNLOAD``.
        :raises ContractError: o motor sem armazenamento (``storage`` ``None``), antes de
            qualquer comando no servidor: o leitor Redshift o recebe em ``unload_to``.
        :raises SqlError: os nomes de ``params`` não fecham com os parâmetros do statement ou do
            texto.
        :raises SandboxError: uma coluna do resultado num tipo fora do contrato; ou, sem
            ``iam_role``, a sessão ``boto3`` sem credenciais para o ``UNLOAD``.
        :raises FileNotFoundError: a falta do manifesto depois de um ``UNLOAD`` de alguma linha.
        """
        if self.storage is None:
            raise ContractError(
                "stream precisa de um armazenamento para os arquivos do UNLOAD, "
                "e o motor abriu sem ele: o leitor Redshift o recebe em unload_to"
            )
        return RedshiftStream(self, literal_text(statement_or_sql, params, self.prefix), batch_size)

    def create_table(
        self,
        table: sa.Table,
    ) -> None:
        """Cria a tabela vazia do modelo em ``exec_<id>_<tabela>``, pelo DDL do Redshift, sob o
        lock, anotada para o ``DROP`` do ``cleanup``.

        Exemplo:

        .. code-block:: python

            engine.create_table(Projetado.__table__)

        :param table: a tabela do modelo, cujo nome não pode estar ocupado no sandbox, com o
            prefixo ``exec_<id>_``.
        :raises SandboxError: o nome que o ``ingest`` ou outro ``create_table`` ocupou.
        """
        name = self.prefix + table.name
        if self.name_in_use(name):
            raise SandboxError(f"{table.name}: o nome já está ocupado no sandbox")
        self.execute(ddl(table, "redshift", prefix=self.prefix))
        self.register_created(name)

    def appender(
        self,
        table: sa.Table,
        queue_depth: int = 2,
    ) -> RedshiftAppender:
        """O gerenciador de contexto que grava lotes numa tabela do sandbox, criada pelo
        ``ingest`` ou por ``create_table``, e os carrega no ``close``.

        Dois appenders na mesma tabela, ao mesmo tempo, entram os dois com todas as suas linhas:
        em sessões de ``new_session()``, cada ``COPY`` roda na sua conexão, e o Redshift grava um
        depois do outro na tabela; na sessão principal, um ``close`` espera o outro sob o lock.
        Sob o isolamento de snapshot, o que a escrita por datashare exige, um appender e um
        ``UPDATE`` das linhas que a tabela já tinha, ao mesmo tempo, também entram os dois.

        Exemplo:

        .. code-block:: python

            with engine.appender(Projetado.__table__) as appender:
                appender.write(batch)

        :param table: a tabela do modelo, já criada no sandbox com o prefixo ``exec_<id>_``.
        :param queue_depth: os lotes convertidos que esperam a thread de gravação; com a fila
            cheia, o ``write`` bloqueia.
        :return: o ``Appender`` da tabela, que guarda os lotes num Parquet do ``staging/`` até o
            ``close`` e os carrega por ``COPY ... MANIFEST``.
        :raises SandboxError: a tabela que não existe no sandbox.
        """
        return RedshiftAppender(self, table, queue_depth)

    def append(
        self,
        table: sa.Table,
        data: pa.Table | pa.RecordBatch | pa.RecordBatchReader | Iterable[pa.RecordBatch],
    ) -> int:
        """Acrescenta os lotes a uma tabela do sandbox pelo ``appender``.

        Dois ``append`` na mesma tabela ao mesmo tempo, ou um ``append`` e um ``UPDATE`` dela,
        entram os dois, como no ``appender``.

        Exemplo:

        .. code-block:: python

            engine.append(Projetado.__table__, pa.Table.from_pandas(frame, preserve_index=False))

        :param table: a tabela do modelo, já criada no sandbox com o prefixo ``exec_<id>_``.
        :param data: uma ``pa.Table``, um ``pa.RecordBatch``, um ``pa.RecordBatchReader`` ou um
            iterável de ``pa.RecordBatch``.
        :return: as linhas acrescentadas.
        :raises ContractError: um DataFrame, com a conversão sem cópia na mensagem, ou outro
            tipo em ``data``; ou um lote que o ``cast`` recusa, e nada é inserido.
        :raises SandboxError: a tabela que não existe no sandbox; ou, sem ``iam_role``, a sessão
            ``boto3`` sem credenciais para o ``COPY``.
        """
        batches = batches_of(data)
        with self.appender(table) as appender:
            for batch in batches:
                appender.write(batch)
        return appender.rows

    # ------------------------------------------------------------ a auditoria

    def _pinned_max_key(
        self,
        table: sa.Table,
        uri: str,
        version: int,
    ) -> int | None:
        """O ``max_key`` da versão fixada na chave sequencial, sem ler dados; ``None`` numa
        tabela sem ela."""
        key = sequential_key(table)
        if key is None:
            return None
        return delta.max_key(delta.open_table(uri, self.storage, version), key.name)

    def _pending_source(
        self,
        table: sa.Table,
        uri: str,
        version: int,
    ) -> sa.FromClause:
        """A versão fixada como origem de consulta, carregada só quando uma verificação a cita."""
        staging = self._pinned_staging(table, uri, version)
        self._pending[staging.name] = staging
        return self._source(table, staging.name)

    def _referenced_sources(
        self,
        table: sa.Table,
        foreign_keys: bool,
        referenced: Mapping[str, tuple[str, int]] | None,
    ) -> dict[str, sa.FromClause]:
        """A origem da linha referenciada de cada chave estrangeira: a tabela do sandbox com o nome
        do modelo, ou a versão fixada informada em ``referenced``."""
        sources: dict[str, sa.FromClause] = {}
        if not foreign_keys:
            return sources
        for constraint in table.foreign_key_constraints:
            target = constraint.referred_table
            if self.name_in_use(self.prefix + target.name):
                sources[target.name] = target
            elif referenced is not None and target.name in referenced:
                uri, version = referenced[target.name]
                sources[target.name] = self._pending_source(target, uri, version)
        return sources

    def _text(
        self,
        statement: sa.sql.ClauseElement,
        table: sa.Table,
    ) -> str:
        """O texto do Redshift de uma verificação, sobre as tabelas do sandbox."""
        return sql.render(statement, "redshift", table.metadata, prefix=self.prefix)

    def _load_cited(
        self,
        text: str,
    ) -> None:
        """Carrega a staging ``_versao`` que o texto cita e ainda não foi carregada."""
        for name, staging in list(self._pending.items()):
            if quoted(name) in text:
                self._ensure_loaded(staging)

    def _rows_result(
        self,
        table: sa.Table,
        check: audit.Check,
        partitions: Sequence[str] | None,
    ) -> tuple[CheckResult, dict, dict]:
        """A verificação de linhas: o resultado, as leituras por partição e os não finitos."""
        text = self._text(check.statement, table)
        rows = self.query(text).to_pylist()
        totals, nonfinite = audit.readings_by_partition(rows, table)
        defects, failing = audit.failing_counters(check.counters, totals)
        sample = self._rows_sample(table, check, partitions, failing)
        return CheckResult(check.name, text, defects, sample, defects == 0), totals, nonfinite

    def _rows_sample(
        self,
        table: sa.Table,
        check: audit.Check,
        partitions: Sequence[str] | None,
        failing: list[str],
    ) -> pa.Table:
        """Até 20 linhas inteiras dos contadores reprovados, uma consulta por contador."""
        samples = []
        for label in failing:
            statement = audit.sample_statement(table, partitions, check.counters[label])
            samples.append(self.query(self._text(statement, table)))
        if not samples:
            return pa.table({})
        return pa.concat_tables(samples).slice(0, audit.SAMPLE_ROWS)

    def _check_result(
        self,
        table: sa.Table,
        check: audit.Check,
    ) -> CheckResult:
        """Uma verificação de chave ou de órfão; o ``skip_when`` verdadeiro a aprova sem rodá-la, e
        a staging ``_versao`` que ela cita é carregada só quando ela roda."""
        text = self._text(check.statement, table)
        if check.skip_when is not None:
            skipped = self.query(self._text(check.skip_when, table)).column(0)[0].as_py()
            if skipped is True:
                reason = "dispensada: o menor valor da execução passa do maior da versão fixada"
                return CheckResult(check.name, text, 0, pa.table({}), True, reason)
        self._load_cited(text)
        found = self.query(text)
        return CheckResult(
            check.name, text, found.num_rows, found.slice(0, audit.SAMPLE_ROWS), found.num_rows == 0
        )

    def audit(
        self,
        table: sa.Table,
        partitions: list[str] | None,
        uri: str | None = None,
        version: int | None = None,
        foreign_keys: bool = False,
        key_scope: KeyScope | None = None,
        referenced: Mapping[str, tuple[str, int]] | None = None,
    ) -> AuditReport:
        """Roda as verificações do contrato sobre a tabela do sandbox.

        Exemplo:

        .. code-block:: python

            report = engine.audit(Projetado.__table__, ["2026-08-31"], uri, 57)
            report.passed, report.nonfinite_columns

        :param table: a tabela do modelo, no sandbox.
        :param partitions: as partições da execução; ``None`` audita a tabela inteira do
            sandbox.
        :param uri: a URI da tabela fixada pela execução; com ``version``, dá a versão
            fixada, que as chaves fora da partição comparam, carregada na staging
            ``_versao`` só quando a junção roda, e o ``max_key`` do ``skip_when``.
        :param version: a versão fixada da tabela; sem ela, ou sem ``uri``, a verificação
            ``chave_<colunas>_tabela`` não roda.
        :param foreign_keys: ``True`` confere as chaves estrangeiras, contra a tabela
            referenciada do sandbox ou contra a versão de ``referenced``.
        :param key_scope: o escopo da unicidade; ``"partition"`` suprime a verificação
            ``chave_<colunas>_tabela``, e ``"table"`` a confere também na chave com a coluna
            de ``partition_source``.
        :param referenced: por tabela, a URI e a versão fixada da tabela referenciada que o
            sandbox não tem, para as chaves estrangeiras com ``foreign_keys=True``, carregada
            na staging ``_versao`` só quando a junção roda.
        :return: o ``AuditReport``; a reprovação não levanta aqui: ``passed`` é falso, e
            ``Execution.audit`` levanta ``AuditFailed``.
        :raises ContractError: um valor de ``partitions`` fora da regra da partição.
        :raises SandboxError: sem ``iam_role``, a sessão ``boto3`` sem credenciais para o
            ``COPY`` da staging ``_versao``.
        """
        pinned = None
        pinned_max_key = None
        if uri is not None and version is not None:
            pinned = self._pending_source(table, uri, version)
            pinned_max_key = self._pinned_max_key(table, uri, version)
        sources = self._referenced_sources(table, foreign_keys, referenced)
        found, not_run = audit.checks_and_not_run(
            table, partitions, foreign_keys, key_scope, pinned, sources, pinned_max_key
        )
        results = []
        totals: dict = {}
        nonfinite: dict = {}
        for check in found:
            if check.name == "linhas":
                result, totals, nonfinite = self._rows_result(table, check, partitions)
            else:
                result = self._check_result(table, check)
            results.append(result)
        return AuditReport(
            table=table.name,
            partitions=tuple(partitions) if partitions is not None else None,
            results=tuple(results),
            not_run=tuple(not_run),
            nonfinite_columns=nonfinite,
            totals=totals,
        )

    # ------------------------------------------------------------ a exportação

    def _partition_select(
        self,
        table: sa.Table,
        value: str | None,
    ) -> str:
        """O ``SELECT`` da partição no sandbox: as colunas do contrato na ordem dele, sem a de
        partição, o JSON serializado em texto, na ordem da ``sort_key``."""
        options = table_options(table)
        columns = []
        for column in columns_without_partition(table):
            name = quoted(column.name)
            if isinstance(column.type, sa.JSON):
                columns.append(f"JSON_SERIALIZE({name}) AS {name}")
            else:
                columns.append(name)
        text = f"SELECT {', '.join(columns)} FROM {self.qualified(self.prefix + table.name)}"
        if options.partition_by is not None:
            text += f" WHERE {quoted(options.partition_by)} = {literal(value)}"
        if options.sort_key:
            text += " ORDER BY " + ", ".join(quoted(name) for name in options.sort_key)
        return text

    def _count(
        self,
        table: sa.Table,
        value: str | None,
    ) -> int:
        """As linhas da partição no sandbox."""
        partition_by = table_options(table).partition_by
        text = f"SELECT count(*) FROM {self.qualified(self.prefix + table.name)}"
        if partition_by is not None:
            text += f" WHERE {quoted(partition_by)} = {literal(value)}"
        return int(self.execute(text).fetchone()[0])

    def _unload(
        self,
        table: sa.Table,
        value: str | None,
        prefix: str,
        count: int,
    ) -> list[str]:
        """O ``UNLOAD`` da partição para o prefixo, em série até ``_PARALLEL_OFF_ROWS`` linhas, e os
        arquivos gravados, relativos à raiz."""
        # O lock fica do UNLOAD à leitura do manifesto: o pg_last_unload_count() é o deste UNLOAD.
        with self.session():
            self.execute(
                unload_text(
                    self._partition_select(table, value),
                    self.storage.uri_of(prefix),
                    credentials_clause(self.config),
                    parallel=count > _PARALLEL_OFF_ROWS,
                )
            )
            return self.unloaded_paths(prefix)

    def _empty_file(
        self,
        table: sa.Table,
        prefix: str,
    ) -> str:
        """Um arquivo Parquet sem linha no prefixo, com as colunas do contrato sem a de partição: o
        ``UNLOAD`` de um resultado vazio não grava arquivo, e a partição vazia entra no log por
        ele."""
        fields = [
            field
            for field in arrow_schema(table)
            if field.name != table_options(table).partition_by
        ]
        path = self.storage.join(prefix, "vazio.parquet")
        with self.storage.open_output_stream(path) as sink:
            pq.write_table(pa.schema(fields).empty_table(), sink)
        return path

    def _registered_files(
        self,
        table: sa.Table,
        table_path: str,
        prefix: str,
        paths: Sequence[str],
    ) -> list[delta.RegisteredFile]:
        """Os arquivos do ``UNLOAD`` descritos pelo rodapé, com o caminho relativo à pasta da
        tabela."""
        if not paths:
            paths = [self._empty_file(table, prefix)]
        files = []
        for path in paths:
            footer = pq.ParquetFile(self.storage.open_input_file(path))
            files.append(
                delta.file_from_footer(
                    footer,
                    path.removeprefix(table_path + "/"),
                    self.storage.size(path),
                    footer.metadata.num_rows,
                    table,
                )
            )
        return files

    def _register(
        self,
        table: sa.Table,
        uri: str,
        value: str | None,
        metadata: Mapping[str, str],
        expected_rows: int | None,
        columns_without_min_max: Collection[str],
        count: int,
    ) -> int:
        """O registro: o ``UNLOAD`` para ``<uri>/<coluna>=<valor>/<execution_id>_<uuid>/`` e o
        commit dos arquivos como o Redshift os gravou."""
        # Um prefixo novo por chamada: o UNLOAD recusa o destino que já tem objetos.
        partition_by = table_options(table).partition_by
        table_path = self.storage.relative(uri)
        folder = f"{partition_by}={value}" if partition_by is not None else ""
        prefix = self.storage.join(table_path, folder, f"{self.execution_id}_{uuid.uuid4().hex}")
        paths = self._unload(table, value, prefix, count)
        files = self._registered_files(table, table_path, prefix, paths)
        expected = expected_rows if expected_rows is not None else count
        return delta.register_files(
            uri,
            table,
            files,
            value,
            metadata,
            self.storage,
            expected_rows=expected,
            columns_without_min_max=columns_without_min_max,
        )

    def _swap_reader(
        self,
        connection: object,
        table: sa.Table,
        value: str | None,
        paths: Sequence[str],
    ) -> pa.RecordBatchReader | pa.Table:
        """A partição de volta pelo leitor do DuckDB sobre os arquivos do ``UNLOAD``, com cada
        coluna no tipo do contrato e a de partição acrescentada com o valor; vazia sem arquivo."""
        if not paths:
            return arrow_schema(table).empty_table()
        partition_by = table_options(table).partition_by
        columns = []
        for column in table.columns:
            name = quoted(column.name)
            if column.name == partition_by:
                columns.append(f"{literal(value)} AS {name}")
            else:
                columns.append(f"CAST({name} AS {sql_type(column, 'duckdb')}) AS {name}")
        files = ", ".join(literal(self.storage.uri_of(path)) for path in paths)
        text = f"SELECT {', '.join(columns)} FROM read_parquet([{files}])"
        return cast(connection.execute(text).to_arrow_reader(100_000), table)

    def _swap(
        self,
        table: sa.Table,
        uri: str,
        value: str | None,
        metadata: Mapping[str, str],
        expected_rows: int | None,
        columns_without_min_max: Collection[str],
        count: int,
    ) -> int:
        """A troca: o ``UNLOAD`` para o ``staging/``, a partição de volta por
        ``publish_partition``, que grava sem mínimo e máximo as colunas com valor não finito, e a
        releitura por ``read_back``, como no registro."""
        partition_by = table_options(table).partition_by
        folder = f"{partition_by}={value}" if partition_by is not None else ""
        prefix = self.storage.join(self.staging_prefix, table.name, folder, uuid.uuid4().hex)
        log.warning(
            "%s %s: colunas Double com valor não finito %s; a partição sai por "
            "publish_partition, e os dados passam pela máquina local",
            table.name,
            delta.partition_label(value),
            sorted(columns_without_min_max),
        )
        paths = self._unload(table, value, prefix, count)
        # Uma conexão do DuckDB por partição, nos limites do ambiente: o close devolve a memória.
        connection = self.storage.duckdb_connect(config=environment_limits())
        try:
            reader = self._swap_reader(connection, table, value, paths)
            version = delta.publish_partition(
                uri,
                table,
                value,
                reader,
                metadata,
                self.storage,
                columns_without_min_max=columns_without_min_max,
            )
        finally:
            connection.close()
        # O log e os dois leitores contam as linhas da partição; a diferença desfaz o commit.
        expected = expected_rows if expected_rows is not None else count
        delta.read_back(uri, table, value, expected, self.storage)
        return version

    def export_partition(
        self,
        table: sa.Table,
        uri: str,
        value: str | None,
        metadata: Mapping[str, str],
        expected_rows: int | None = None,
        columns_without_min_max: Collection[str] = (),
    ) -> int:
        """Leva a partição do sandbox ao Delta.

        A partição sai por ``UNLOAD ... MANIFEST VERBOSE``, sem ``PARTITION BY``, para um prefixo
        novo por chamada na pasta da tabela Delta, ``<coluna>=<valor>/<execution_id>_<uuid>/``
        (sem ``<coluna>=<valor>/`` numa tabela sem partição), e os arquivos entram no log da
        tabela por ``register_files``, como o Redshift os gravou, com as conferências e a
        releitura. O ``UNLOAD`` corre em série (``PARALLEL OFF``), num arquivo só, até 5.000.000
        linhas, e em paralelo acima disso, o que pode dar um arquivo por slice; no ambiente alvo, a
        partição ordenada pela ``sort_key`` saiu num arquivo só nos dois modos, até 33.239.719
        linhas (2026-10-05). Com ``columns_without_min_max``, o ``UNLOAD`` vai a
        ``<tabela>/<coluna>=<valor>/<uuid>/`` sob o ``staging_prefix`` e a partição volta por
        ``publish_partition``, com um aviso no log ``serialize_db.engine.redshift``, porque o
        rodapé do ``UNLOAD`` deixa o ``NaN`` fora do máximo e o leitor podaria a linha (issue #59).

        Exemplo:

        .. code-block:: python

            engine.export_partition(Projetado.__table__, uri, "2026-08-31",
                                    delta.commit_metadata("exec-42", versions))

        :param table: a tabela do modelo, no sandbox.
        :param uri: a URI da pasta da tabela Delta de destino, sob a raiz do armazenamento.
        :param value: o valor da partição; ``None`` numa tabela sem partição, que sai inteira.
        :param metadata: os metadados do commit, de ``delta.commit_metadata``.
        :param expected_rows: a contagem da auditoria, que o registro confere nos arquivos antes
            do commit e os dois caminhos conferem na releitura depois dele; sem ela, a contagem do
            sandbox.
        :param columns_without_min_max: as colunas ``Double`` com valor não finito na partição,
            que saem sem mínimo e máximo.
        :return: a versão do commit.
        :raises ContractError: o valor fora da regra da partição, ``None`` numa tabela
            particionada, ou um valor numa tabela sem partição.
        :raises RegistrationRefused: uma conferência dos arquivos reprovou, sem commit, ou a
            releitura desfez o commit.
        :raises ExecutionConflict: outro commit na mesma partição a partir da mesma versão.
        :raises ValueError: ``uri`` fora da raiz do armazenamento, no registro dos arquivos.
        :raises SandboxError: sem ``iam_role``, a sessão ``boto3`` sem credenciais para o
            ``UNLOAD``; ou a memória que o processo ainda pode usar abaixo de 2 MiB, ou negativa,
            lida no registro antes do commit, sem nada gravado, e na troca dos arquivos pelo
            ``Double`` não finito, na abertura do DuckDB (``environment_limits``).
        :raises FileNotFoundError: a falta do manifesto depois de um ``UNLOAD`` de alguma linha.
        """
        # O valor conferido antes do UNLOAD: os arquivos de um valor recusado no registro
        # ficariam na pasta da tabela, fora do log.
        value = delta.checked_value(table, value)
        count = self._count(table, value)
        if columns_without_min_max:
            return self._swap(
                table, uri, value, metadata, expected_rows, columns_without_min_max, count
            )
        return self._register(
            table, uri, value, metadata, expected_rows, columns_without_min_max, count
        )

    # ------------------------------------------------------------ o encerramento

    def cleanup(self) -> None:
        """Apaga as tabelas ``exec_<id>_*`` que a execução criou, uma por comando, os objetos do
        ``staging_prefix`` (``<ambiente>/staging/<execution_id>/`` no sandbox, ``<id do leitor>/``
        sob o ``unload_to`` do leitor; nenhum sem armazenamento) e fecha a sessão; uma tabela que o
        ``DROP`` não alcança fica nomeada no log ``serialize_db.engine.redshift``. Numa sessão a
        mais, fecha só a conexão dela. A segunda chamada não faz nada.

        Exemplo:

        .. code-block:: python

            staging = "prd/staging/exec-2026-09-05"
            engine = RedshiftEngine(config, "exec-2026-09-05", storage, staging)
            try:
                engine.ingest(Operacao.__table__, uri, 3)
            finally:
                engine.cleanup()
        """
        if self._closed:
            return
        self._closed = True
        if self._parent is None:
            self._drop_created()
            if self.storage is not None:
                self.storage.delete(self.storage.list_files(self.staging_prefix))
        with self._lock:
            self._connection.close()

    def _drop_created(self) -> None:
        """Apaga as tabelas que a execução criou, uma por comando; a que o ``DROP`` não alcança
        fica nomeada no log."""
        for name in list(self._created):
            try:
                self.execute(f"DROP TABLE IF EXISTS {self.qualified(name)}")
            except redshift_connector.Error as error:
                log.warning("sandbox %s: %s não apagada (%s)", self.execution_id, name, error)

    def __enter__(self) -> RedshiftEngine:
        return self

    def __exit__(
        self,
        *exc: object,
    ) -> None:
        self.cleanup()
