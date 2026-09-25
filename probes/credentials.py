"""As credenciais que a biblioteca segura numa execução mais longa que a validade delas: cada
cliente aberto no início lê de novo, com os mesmos objetos, depois que a sua credencial expira.

Uso:

    .venv/bin/python probes/credentials.py <tabela> [--interval-minutes 5] [--margin-minutes 3]
        [--max-wait-minutes 90] [--wait-minutes N]

``<tabela>`` é a URI de uma tabela Delta, de preferência pequena, como
``s3://bucket/prefixo/prd/cad_contas``: cada leitura conta as linhas dela ou lê o seu menor
arquivo. Uma pasta local também serve, sem credencial a expirar, para conferir a própria sonda.

A credencial do contêiner expira em cerca de uma hora, e a senha que ``GetCredentials`` dá à
conexão Redshift da biblioteca vale 3.600 s (``durationSeconds`` de ``_workgroup_login``). A sonda
abre no início os clientes que uma execução segura, lê cada um a cada ``--interval-minutes`` e
espera passar a última das duas expirações, mais ``--margin-minutes``, com o teto de
``--max-wait-minutes``; ``--wait-minutes`` troca essa espera por um tempo fixo. Os clientes:

- ``delta-rs``: o ``DeltaTable`` de ``delta.open_table``, que a execução guarda, por
  ``update_incremental``, que lista o log, e pela primeira linha do dataset, que lê um arquivo;
- ``duckdb delta_scan`` e ``duckdb read_parquet``: a conexão de ``Storage.duckdb_connect``, com o
  secret ``credential_chain`` e ``REFRESH auto`` de ``duckdb_setup`` e o cache de arquivos externos
  desligado, pela extensão ``delta`` sobre a tabela e pelo ``httpfs`` sobre o menor arquivo, nessa
  ordem, cada contagem lida até o fim;
- ``pyarrow``: o ``S3FileSystem`` de ``Storage``, pelo rodapé do menor arquivo;
- ``boto3``: ``Storage.read_text`` do último commit do log, com um cliente novo por chamada na
  sessão padrão do ``boto3``;
- ``redshift``: a conexão de ``engine.redshift.connect``, por ``select 1``, que também a mantém
  longe do encerramento da sessão ociosa (3.600 s no serverless).

Cada rodada lê também três chaves, pela impressão digital: a que a cadeia do ``boto3`` resolve
naquele momento numa sessão nova, a do contêiner, com a sua expiração; a que o secret do DuckDB
guarda; e a que a cláusula de credenciais do ``COPY`` e do ``UNLOAD`` (``credentials_clause``) leva,
montada sem rodar comando. A troca da primeira diz quando o contêiner renovou a credencial, e as
outras duas dizem se o secret e a cláusula a acompanharam. Uma chave nunca aparece no relatório:
só os oito primeiros caracteres hexadecimais do ``sha256`` dela. A expiração é a que o ``boto3``
lê: no IMDS, que não é o caminho do contêiner, o botocore adia a expiração próxima para 12 a 20
minutos adiante, e a espera cresce junto.

Depois da espera, clientes novos repetem as leituras: é o controle que separa "o cliente segurado
não renovou a credencial" de "o ambiente perdeu o acesso".

Só leitura: a sonda lê o log e um arquivo de dados da tabela e roda ``select 1`` no Redshift, e
nada é criado, alterado ou apagado. A escrita do DuckDB no S3 (o ``COPY ... TO`` da exportação) e
um ``COPY`` mais longo que a credencial ficam fora. O relatório sai no terminal e em
``probes/output/credentials_<data-hora>.txt``, uma linha por rodada durante a espera; ``Ctrl-C``
encerra a espera, e o relatório fecha com o que foi lido.

Seções:

1. Configuração: a tabela, as variáveis que escolhem a credencial e o Redshift.
2. Os clientes: a credencial do contêiner, a abertura de cada cliente e a primeira rodada.
3. A espera: uma linha por rodada.
4. O controle: os clientes novos depois da espera.
5. A linha do tempo: a tabela das rodadas e as leituras que falharam.

Cada seção é uma função, na ordem acima, e ``checks`` emite as checagens (``CR-1`` a ``CR-11``);
``main`` as chama uma a uma. Códigos de saída: 0 quando toda checagem passou, 1 quando alguma
chamada falhou, 2 quando alguma checagem reprovou.

O Redshift vem das variáveis da biblioteca, ``SERIALIZE_DB_REDSHIFT_*`` (``WORKGROUP``,
``DATABASE``, ``SHARE_DATABASE``, ``SCHEMA``, ``IAM_ROLE``, ``HOST``, ``PORT``, ``USER``,
``PASSWORD``), e a região de ``AWS_REGION`` ou ``AWS_DEFAULT_REGION``; sem ``WORKGROUP`` nem
``HOST``, o cliente ``redshift`` fica de fora.
"""

from __future__ import annotations

import argparse
import contextlib
import dataclasses
import datetime
import functools
import hashlib
import re
import sys
import time
import urllib.parse
from collections.abc import Callable
from pathlib import Path
from typing import TypeVar

import boto3
import botocore.credentials
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import DeltaTable

from serialize_db import delta
from serialize_db.engine.redshift import RedshiftConfig, connect, credentials_clause
from serialize_db.schema import literal
from serialize_db.storage import Storage, prepare_environment

sys.path.insert(0, str(Path(__file__).resolve().parent))
from probelib import Report, describe_error, environment_rows  # noqa: E402

T = TypeVar("T")

# A validade da senha que a conexão Redshift da biblioteca pede a GetCredentials: o
# durationSeconds de engine.redshift._workgroup_login.
REDSHIFT_PASSWORD_SECONDS = 3600

# O nome do secret do S3 que Storage.duckdb_setup cria.
DUCKDB_SECRET = "serialize_db_s3"

# As variáveis que escolhem a credencial que as cadeias resolvem: as do contêiner, as que a
# trocariam por uma chave fixa, que não expira, e as da região e do endpoint.
CREDENTIAL_VARIABLES = (
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_PROFILE",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ENDPOINT_URL",
)

DEFAULT_INTERVAL_MINUTES = 5.0
DEFAULT_MARGIN_MINUTES = 3.0
DEFAULT_MAX_WAIT_MINUTES = 90.0

# Os clientes segurados, na ordem da linha do tempo.
DELTA_RS = "delta-rs"
DUCKDB_DELTA = "duckdb delta_scan"
DUCKDB_PARQUET = "duckdb read_parquet"
PYARROW = "pyarrow"
BOTO3 = "boto3"
REDSHIFT = "redshift"

# A checagem de cada cliente segurado.
CLIENT_CHECKS = {
    DELTA_RS: "CR-3",
    DUCKDB_DELTA: "CR-4",
    DUCKDB_PARQUET: "CR-5",
    PYARROW: "CR-6",
    BOTO3: "CR-7",
    REDSHIFT: "CR-8",
}


@dataclasses.dataclass(frozen=True)
class Reading:
    """Uma leitura de um cliente: o instante, se passou, o resultado ou o motivo da falha e a
    duração em segundos."""

    client: str
    at: datetime.datetime
    ok: bool
    detail: str
    seconds: float


@dataclasses.dataclass(frozen=True)
class Round:
    """Uma rodada: as leituras dos clientes e as chaves daquele momento, pela impressão digital;
    ``None`` onde a chave não existe ou não foi lida."""

    at: datetime.datetime
    readings: list[Reading]
    container_key: str | None
    container_expiry: datetime.datetime | None
    secret_key: str | None
    clause_key: str | None


@dataclasses.dataclass
class Held:
    """Os clientes abertos no início e segurados até o fim."""

    readers: dict[str, Callable[[], str]] = dataclasses.field(default_factory=dict)
    """A leitura de cada cliente, pelo nome, na ordem da linha do tempo."""
    duckdb: duckdb.DuckDBPyConnection | None = None
    """A conexão do DuckDB, de onde sai a chave guardada no secret."""
    redshift: object | None = None
    """A conexão do Redshift."""
    redshift_expiry: datetime.datetime | None = None
    """O instante mais tardio em que a senha da conexão Redshift expira: o fim da abertura mais
    3.600 s; ``None`` sem a credencial temporária do workgroup."""


# --------------------------------------------------------------------------------------------------
# Funções puras: as chaves, os instantes, a espera e os vereditos


def now() -> datetime.datetime:
    """O instante atual, em UTC."""
    return datetime.datetime.now(datetime.timezone.utc)


def clock(at: datetime.datetime | None) -> str:
    """O instante no fuso da máquina, como ``14:05:12``; ``-`` sem instante."""
    if at is None:
        return "-"
    return at.astimezone().strftime("%H:%M:%S")


def signed_minutes(at: datetime.datetime, reference: datetime.datetime | None) -> str:
    """Os minutos de ``at`` em relação a ``reference``, com sinal: ``-45 min`` antes, ``+2 min``
    depois; ``-`` sem referência."""
    if reference is None:
        return "-"
    minutes = (at - reference).total_seconds() / 60
    return f"{minutes:+.0f} min"


def fingerprint(key: str | None) -> str | None:
    """Os oito primeiros caracteres hexadecimais do ``sha256`` de uma chave, que a identificam no
    relatório sem mostrá-la; ``None`` sem chave."""
    if not key:
        return None
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:8]


def secret_key_id(secret_string: str) -> str | None:
    """O ``key_id`` que um secret do DuckDB guarda, lido do ``secret_string`` de
    ``duckdb_secrets()``, que o mostra sem redação; ``None`` sem ele."""
    found = re.search(r"(?:^|;)key_id=([^;]*)", secret_string)
    if found is None or not found.group(1):
        return None
    return found.group(1)


def clause_key_id(clause: str) -> str | None:
    """O ``ACCESS_KEY_ID`` de uma cláusula de credenciais do ``COPY`` e do ``UNLOAD``; ``None`` na
    cláusula ``IAM_ROLE``, que não leva chave."""
    found = re.search(r"ACCESS_KEY_ID '([^']*)'", clause)
    if found is None:
        return None
    return found.group(1)


def wait_deadline(start: datetime.datetime, expiries: list[datetime.datetime | None],
                  margin: datetime.timedelta, ceiling: datetime.timedelta,
                  fixed: datetime.timedelta | None) -> tuple[datetime.datetime, str]:
    """O fim da espera e o motivo dele: o tempo fixo, quando pedido; senão a última expiração mais
    a margem, limitada pelo teto; sem expiração, o próprio início."""
    if fixed is not None:
        return start + fixed, "o tempo fixo de --wait-minutes"
    known = [expiry for expiry in expiries if expiry is not None]
    if not known:
        return start, "nenhuma credencial com expiração: nada a esperar"
    last = max(known)
    target = last + margin
    if target > start + ceiling:
        return start + ceiling, (f"o teto de --max-wait-minutes, antes da expiração das "
                                 f"{clock(last)}")
    return target, f"a expiração das {clock(last)} mais a margem"


def held_verdict(readings: list[Reading],
                 expiry: datetime.datetime | None) -> tuple[str, str]:
    """O veredito de um cliente segurado pelas leituras feitas depois da expiração da credencial
    que ele resolveu na abertura: ``pass`` quando todas passaram, ``fail`` quando alguma falhou e
    ``note`` sem expiração conhecida ou sem leitura depois dela."""
    if expiry is None:
        return "note", "sem expiração conhecida: a credencial não expira, ou a tabela é local"
    before = [reading for reading in readings if reading.at <= expiry]
    after = [reading for reading in readings if reading.at > expiry]
    failed_before = sum(1 for reading in before if not reading.ok)
    earlier = f"; {failed_before} falha(s) antes da expiração" if failed_before else ""
    if not after:
        return "note", f"nenhuma leitura depois da expiração das {clock(expiry)}{earlier}"
    failed = [reading for reading in after if not reading.ok]
    if failed:
        first = failed[0]
        return "fail", (f"{len(failed)} de {len(after)} leitura(s) depois da expiração das "
                        f"{clock(expiry)} falharam, a primeira {signed_minutes(first.at, expiry)}: "
                        f"{first.detail}{earlier}")
    last = after[-1]
    return "pass", (f"{len(after)} leitura(s) depois da expiração das {clock(expiry)} passaram, "
                    f"a última {signed_minutes(last.at, expiry)}{earlier}")


def key_change(keys: list[tuple[datetime.datetime, str | None]]) -> datetime.datetime | None:
    """O instante da primeira rodada em que a chave difere da chave da primeira rodada; ``None``
    sem troca. ``keys`` traz o instante e a impressão digital da chave de cada rodada."""
    _at, initial = keys[0]
    for at, key in keys[1:]:
        if key is not None and key != initial:
            return at
    return None


def key_verdict(keys: list[tuple[datetime.datetime, str | None]],
                expiry: datetime.datetime | None) -> tuple[str, str]:
    """O veredito de uma chave montada a cada uso, como a da cláusula, pelas rodadas depois da
    expiração da chave do início: ``pass`` quando cada uma leva outra chave, ``fail`` quando
    alguma ainda leva a do início e ``note`` sem chave, sem expiração ou sem chave lida depois
    dela."""
    _at, initial = keys[0]
    if initial is None:
        return "note", "sem chave na primeira rodada"
    if expiry is None:
        return "note", "sem expiração conhecida: a chave não expira"
    # A rodada que não leu a chave não diz se ela trocou.
    after = [(at, key) for at, key in keys if at > expiry and key is not None]
    if not after:
        return "note", f"nenhuma chave lida depois da expiração das {clock(expiry)}"
    stale = [at for at, key in after if key == initial]
    if stale:
        return "fail", (f"a chave do início, expirada às {clock(expiry)}, continua em {len(stale)} "
                        f"de {len(after)} rodada(s) depois da expiração")
    changed = key_change(keys)
    return "pass", (f"a chave trocou às {clock(changed)} "
                    f"({signed_minutes(changed, expiry)} da expiração das {clock(expiry)})")


def record(report: Report, status: str, check_id: str, what: str, detail: str) -> None:
    """Registra uma checagem pelo veredito: ``pass``, ``fail`` ou ``note``."""
    if status == "pass":
        report.ok(check_id, what, detail)
    elif status == "fail":
        report.fail(check_id, what, detail)
    else:
        report.note(check_id, what, detail)


def cell(reading: Reading | None) -> str:
    """A célula de uma leitura na linha do tempo: ``ok``, ``FALHOU`` ou ``-``."""
    if reading is None:
        return "-"
    return "ok" if reading.ok else "FALHOU"


def reading_of(item: Round, client: str) -> Reading | None:
    """A leitura de um cliente numa rodada; ``None`` quando o cliente não leu nela."""
    for reading in item.readings:
        if reading.client == client:
            return reading
    return None


def round_line(item: Round, expiry: datetime.datetime | None) -> str:
    """A linha de uma rodada, impressa durante a espera."""
    readings = ", ".join(f"{reading.client} {cell(reading)}" for reading in item.readings)
    container = (f"contêiner {item.container_key or '-'} até {clock(item.container_expiry)}")
    keys = f"secret {item.secret_key or '-'}, cláusula {item.clause_key or '-'}"
    return (f"{clock(item.at)} ({signed_minutes(item.at, expiry)}): {readings}; {container}, "
            f"{keys}")


def timeline_rows(rounds: list[Round], clients: list[str],
                  expiry: datetime.datetime | None) -> list[list[str]]:
    """A tabela da linha do tempo: uma linha por rodada, uma coluna por cliente e as chaves."""
    header = ["HORA", "EXPIRAÇÃO", *clients, "CONTÊINER", "ATÉ", "SECRET", "CLÁUSULA"]
    rows = [header]
    for item in rounds:
        cells = [cell(reading_of(item, client)) for client in clients]
        rows.append([clock(item.at), signed_minutes(item.at, expiry), *cells,
                     item.container_key or "-", clock(item.container_expiry),
                     item.secret_key or "-", item.clause_key or "-"])
    return rows


# --------------------------------------------------------------------------------------------------
# As leituras de cada cliente


def container_credential() -> tuple[str | None, str | None, bool, datetime.datetime | None]:
    """O método, a impressão digital da chave, se há token e a expiração da credencial que a cadeia
    do ``boto3`` resolve agora, numa sessão nova; tudo vazio sem credencial."""
    found = boto3.Session().get_credentials()
    if found is None:
        return None, None, False, None
    frozen = found.get_frozen_credentials()
    expiry = None
    if isinstance(found, botocore.credentials.RefreshableCredentials):
        expiry = found._expiry_time
    return found.method, fingerprint(frozen.access_key), bool(frozen.token), expiry


def smallest_file(dt: DeltaTable) -> str:
    """O menor arquivo de dados da versão da tabela, relativo à pasta dela."""
    # get_add_actions devolve uma tabela arro3; pa.table a converte sem cópia.
    actions = pa.table(dt.get_add_actions(flatten=True)).to_pylist()
    smallest = min(actions, key=lambda action: action["size_bytes"])
    return urllib.parse.unquote(smallest["path"])


def read_delta_table(dt: DeltaTable) -> str:
    """O ``DeltaTable`` segurado: a listagem do log e a primeira linha do dataset."""
    dt.update_incremental()
    rows = dt.to_pyarrow_dataset().head(1).num_rows
    return f"versão {dt.version()}, {rows} linha lida"


def open_duckdb(storage: Storage) -> duckdb.DuckDBPyConnection:
    """A conexão de ``Storage.duckdb_connect`` com o cache de arquivos externos desligado, para
    cada leitura ir ao armazenamento."""
    connection = storage.duckdb_connect()
    connection.execute("SET enable_external_file_cache = false")
    return connection


def read_count(connection: duckdb.DuckDBPyConnection, source: str) -> str:
    """As linhas de uma função de leitura do DuckDB, como ``delta_scan('<uri>')``.

    O resultado é lido até o fim, o que encerra a consulta: o secret que o ``httpfs`` renova numa
    consulta só fica quando ela termina, e a consulta deixada aberta por ``fetchone`` é desfeita
    pela seguinte, com a renovação."""
    rows = connection.execute(f"SELECT count(*) FROM {source}").fetchall()[0][0]
    return f"{rows} linhas"


def duckdb_secret_key(connection: duckdb.DuckDBPyConnection) -> str | None:
    """A impressão digital da chave que o secret do S3 guarda; ``None`` sem o secret."""
    rows = connection.execute("SELECT secret_string FROM duckdb_secrets() WHERE name = ?",
                              [DUCKDB_SECRET]).fetchall()
    if not rows:
        return None
    return fingerprint(secret_key_id(rows[0][0]))


def read_footer(storage: Storage, path: str) -> str:
    """O rodapé de um arquivo pelo ``S3FileSystem`` do armazenamento."""
    with storage.open_input_file(path) as source:
        rows = pq.ParquetFile(source).metadata.num_rows
    return f"{rows} linhas no rodapé"


def read_log_commit(storage: Storage, path: str) -> str:
    """Um commit do log por ``Storage.read_text``, o ``boto3`` no S3."""
    text, _etag = storage.read_text(path)
    return f"{len(text)} bytes"


def read_redshift(connection: object) -> str:
    """``select 1`` na conexão do Redshift."""
    cursor = connection.cursor()
    cursor.execute("select 1")
    return f"select 1 = {cursor.fetchone()[0]}"


def read_once(client: str, action: Callable[[], str]) -> Reading:
    """Uma leitura: o resultado de ``action`` ou o motivo da falha, com o instante e a duração."""
    at = now()
    started = time.perf_counter()
    try:
        detail = action()
    except Exception as error:  # noqa: BLE001 - a falha de um cliente é a leitura que a sonda busca
        return Reading(client, at, False, describe_error(error), time.perf_counter() - started)
    return Reading(client, at, True, detail, time.perf_counter() - started)


def optional(action: Callable[[], T]) -> T | None:
    """O resultado de ``action``, ou ``None`` quando ela falha: uma chave é leitura de apoio, e a
    falha do cliente que a usa aparece na leitura dele."""
    try:
        return action()
    except Exception:  # noqa: BLE001 - a rodada segue sem a chave
        return None


def clause_key(config: RedshiftConfig) -> str | None:
    """A impressão digital da chave que a cláusula do ``COPY`` e do ``UNLOAD`` levaria agora."""
    return fingerprint(clause_key_id(credentials_clause(config)))


def read_round(held: Held, config: RedshiftConfig) -> Round:
    """Uma rodada: cada cliente segurado lê, e as três chaves são lidas no mesmo momento."""
    started = now()
    readings = [read_once(client, action) for client, action in held.readers.items()]

    # As chaves logo depois das leituras; a que falta fica None, e a rodada segue.
    credential = optional(container_credential)
    container_key, container_expiry = None, None
    if credential is not None:
        _method, container_key, _token, container_expiry = credential
    secret_key = None
    if held.duckdb is not None:
        secret_key = optional(functools.partial(duckdb_secret_key, held.duckdb))
    clause = optional(functools.partial(clause_key, config))
    return Round(started, readings, container_key, container_expiry, secret_key, clause)


# --------------------------------------------------------------------------------------------------
# As seções


def configuration_section(report: Report, storage: Storage, changed: dict[str, str],
                          arguments: argparse.Namespace) -> RedshiftConfig:
    """Seção 1: a tabela, as variáveis que escolhem a credencial e o Redshift da configuração."""
    report.h1("Configuração")
    report.value("TABELA", storage.uri)
    if changed:
        report.line(f"variáveis acertadas por prepare_environment: {', '.join(sorted(changed))}")
    report.table([["VARIÁVEL", "VALOR"], *environment_rows(CREDENTIAL_VARIABLES)])

    config = RedshiftConfig.from_environment()
    if config.host and config.user and config.password:
        path = f"o par informado em {config.host}, sem senha que expire"
    elif config.workgroup:
        path = (f"a credencial temporária do workgroup {config.workgroup}, "
                f"GetCredentials(durationSeconds={REDSHIFT_PASSWORD_SECONDS})")
    else:
        path = "sem SERIALIZE_DB_REDSHIFT_WORKGROUP nem _HOST: o cliente redshift fica de fora"
    report.line(f"Redshift: {path}")
    clause = f"IAM_ROLE {config.iam_role}" if config.iam_role else "a credencial de quem chama"
    report.line(f"cláusula do COPY e do UNLOAD: {clause}")
    report.line(f"espera: rodadas a cada {arguments.interval_minutes:g} min, margem de "
                f"{arguments.margin_minutes:g} min depois da última expiração, teto de "
                f"{arguments.max_wait_minutes:g} min")
    return config


def open_s3_clients(report: Report, storage: Storage, held: Held) -> tuple[str, str] | None:
    """Abre o ``DeltaTable``, a conexão do DuckDB, o ``S3FileSystem`` e o ``boto3``; devolve o menor
    arquivo e o último commit do log, relativos à tabela, ou ``None`` sem a tabela."""
    # A tabela dá o arquivo e o commit que os outros clientes leem; sem ela, nada abre.
    dt = report.call(f"delta.open_table({storage.uri!r}, storage)",
                     functools.partial(delta.open_table, storage.uri, storage),
                     render=lambda table: f"versão {table.version()}")
    if dt is None:
        return None
    sample = report.call("o menor arquivo da versão", functools.partial(smallest_file, dt),
                         render=str)
    if sample is None:
        return None
    commit = f"_delta_log/{dt.version():020d}.json"
    held.readers[DELTA_RS] = functools.partial(read_delta_table, dt)

    # O DuckDB lê a tabela pela extensão delta e o menor arquivo pelo httpfs, nessa ordem.
    connection = report.call("storage.duckdb_connect(), sem o cache de arquivos externos",
                             functools.partial(open_duckdb, storage), render=None)
    if connection is not None:
        held.duckdb = connection
        held.readers[DUCKDB_DELTA] = functools.partial(
            read_count, connection, f"delta_scan({literal(storage.uri)})")
        held.readers[DUCKDB_PARQUET] = functools.partial(
            read_count, connection, f"read_parquet({literal(storage.uri_of(sample))})")

    # O S3FileSystem do Storage, o mesmo em todas as rodadas, e o boto3, com um cliente por
    # chamada na sessão padrão.
    held.readers[PYARROW] = functools.partial(read_footer, storage, sample)
    held.readers[BOTO3] = functools.partial(read_log_commit, storage, commit)
    return sample, commit


def open_redshift(report: Report, config: RedshiftConfig, held: Held) -> None:
    """Abre a conexão do Redshift pelo caminho da biblioteca, com a expiração da senha."""
    if not (config.workgroup or config.host):
        return
    connection = report.call("engine.redshift.connect(config)",
                             functools.partial(connect, config), render=None)
    if connection is None:
        return
    held.redshift = connection
    held.readers[REDSHIFT] = functools.partial(read_redshift, connection)

    # Só a credencial temporária do workgroup expira; o par informado não.
    informed_pair = config.host and config.user and config.password
    if config.workgroup and not informed_pair:
        held.redshift_expiry = now() + datetime.timedelta(seconds=REDSHIFT_PASSWORD_SECONDS)
        report.line(f"senha do Redshift: expira até as {clock(held.redshift_expiry)}")


def clients_section(report: Report, storage: Storage,
                    config: RedshiftConfig) -> tuple[Held, list[Round], tuple[str, str] | None]:
    """Seção 2: a credencial do contêiner, a abertura de cada cliente e a primeira rodada."""
    report.h1("Os clientes")
    # A credencial do contêiner antes de abrir os clientes, que a resolvem cada um a seu modo.
    credential = report.call("boto3.Session().get_credentials()", container_credential,
                             render=describe_credential)
    if credential is not None and credential[3] is not None:
        report.line(f"a credencial expira às {clock(credential[3])} "
                    f"({signed_minutes(credential[3], now())} a partir de agora)")

    held = Held()
    files = open_s3_clients(report, storage, held)
    open_redshift(report, config, held)
    if not held.readers:
        return held, [], files

    # A primeira rodada, logo depois da abertura, é a referência das seguintes.
    first = read_round(held, config)
    report.h2("A primeira rodada")
    report.table([["CLIENTE", "RESULTADO", "SEGUNDOS", "DETALHE"],
                  *[[reading.client, cell(reading), f"{reading.seconds:.1f}", reading.detail]
                    for reading in first.readings]])
    report.line(f"chaves: contêiner {first.container_key or '-'} até "
                f"{clock(first.container_expiry)}, secret do DuckDB {first.secret_key or '-'}, "
                f"cláusula {first.clause_key or '-'}")
    return held, [first], files


def describe_credential(credential: tuple[str | None, str | None, bool,
                                          datetime.datetime | None]) -> str:
    """O texto da credencial do contêiner: método, chave, token e expiração."""
    method, key, token, expiry = credential
    if key is None:
        return "o boto3 não encontrou credencial"
    token_text = "presente" if token else "ausente"
    return (f"método {method}, chave {key}, SESSION_TOKEN {token_text}, "
            f"expira {clock(expiry)}")


def wait_section(report: Report, held: Held, config: RedshiftConfig, rounds: list[Round],
                 deadline: datetime.datetime, reason: str, interval: datetime.timedelta,
                 expiry: datetime.datetime | None) -> str:
    """Seção 3: uma rodada a cada ``interval`` até ``deadline``, impressa ao terminar; devolve
    como a espera terminou."""
    report.h1("A espera")
    report.line(f"até as {clock(deadline)} ({reason}), uma rodada a cada "
                f"{interval.total_seconds() / 60:g} min; Ctrl-C encerra a espera")
    try:
        while True:
            remaining = (deadline - now()).total_seconds()
            if remaining <= 0:
                return f"terminou às {clock(now())}"
            time.sleep(min(interval.total_seconds(), remaining))
            item = read_round(held, config)
            rounds.append(item)
            report.line(round_line(item, expiry))
    except KeyboardInterrupt:
        return f"interrompida pelo operador às {clock(now())}"


def read_fresh_delta_table(storage: Storage) -> str:
    """Um ``DeltaTable`` novo e a primeira linha do dataset."""
    dt = delta.open_table(storage.uri, storage)
    rows = dt.to_pyarrow_dataset().head(1).num_rows
    return f"versão {dt.version()}, {rows} linha lida"


def read_fresh_duckdb(storage: Storage, source: str) -> str:
    """Uma conexão nova do DuckDB, a contagem e o fechamento."""
    connection = open_duckdb(storage)
    try:
        return read_count(connection, source)
    finally:
        connection.close()


def read_fresh_redshift(config: RedshiftConfig) -> str:
    """Uma conexão nova do Redshift, com credencial nova, ``select 1`` e o fechamento."""
    connection = connect(config)
    try:
        return read_redshift(connection)
    finally:
        connection.close()


def control_section(report: Report, storage: Storage, sample: str, config: RedshiftConfig,
                    held: Held) -> list[Reading]:
    """Seção 4: os clientes novos, abertos depois da espera, com a credencial daquele momento."""
    report.h1("O controle")
    # Um Storage novo, para o S3FileSystem e o boto3 resolverem a credencial de agora.
    fresh_storage = Storage.for_uri(storage.uri)
    actions: dict[str, Callable[[], str]] = {
        DELTA_RS: functools.partial(read_fresh_delta_table, fresh_storage),
        DUCKDB_DELTA: functools.partial(read_fresh_duckdb, fresh_storage,
                                        f"delta_scan({literal(storage.uri)})"),
        DUCKDB_PARQUET: functools.partial(read_fresh_duckdb, fresh_storage,
                                          f"read_parquet({literal(storage.uri_of(sample))})"),
        PYARROW: functools.partial(read_footer, fresh_storage, sample),
    }
    if held.redshift is not None:
        actions[REDSHIFT] = functools.partial(read_fresh_redshift, config)
    readings = [read_once(client, action) for client, action in actions.items()]
    report.table([["CLIENTE NOVO", "RESULTADO", "SEGUNDOS", "DETALHE"],
                  *[[reading.client, cell(reading), f"{reading.seconds:.1f}", reading.detail]
                    for reading in readings]])
    return readings


def timeline_section(report: Report, rounds: list[Round], clients: list[str],
                     expiry: datetime.datetime | None) -> None:
    """Seção 5: a tabela das rodadas e as leituras que falharam, com o motivo."""
    report.h1("A linha do tempo")
    report.line(f"EXPIRAÇÃO conta a partir da expiração da credencial do contêiner lida no "
                f"início ({clock(expiry)}); as chaves são impressões digitais")
    report.table(timeline_rows(rounds, clients, expiry))

    # Cada falha com o motivo inteiro, que a tabela não mostra.
    failed = [reading for item in rounds for reading in item.readings if not reading.ok]
    if not failed:
        report.line("Nenhuma leitura falhou.")
        return
    report.h2("As leituras que falharam")
    for reading in failed:
        report.line(f"- {clock(reading.at)} {reading.client}: {reading.detail}")


def checks(report: Report, storage: Storage, rounds: list[Round], controls: list[Reading],
           held: Held, config: RedshiftConfig, ending: str) -> None:
    """As checagens: ``CR-1`` a credencial do contêiner, ``CR-2`` a espera, ``CR-3`` a ``CR-8``
    cada cliente segurado, ``CR-9`` a chave do secret do DuckDB, ``CR-10`` a cláusula e ``CR-11``
    o controle."""
    first = rounds[0]
    expiry = first.container_expiry if storage.is_s3 else None

    # CR-1: sem expiração a credencial é fixa (variáveis AWS_*), e a sonda não tem o que medir.
    if not storage.is_s3:
        report.note("CR-1", "credencial do contêiner", "tabela local: nenhuma credencial")
    elif first.container_key is None:
        report.fail("CR-1", "credencial do contêiner", "o boto3 não encontrou credencial")
    elif expiry is None:
        report.note("CR-1", "credencial do contêiner",
                    f"chave {first.container_key} sem expiração: a credencial é fixa")
    else:
        report.ok("CR-1", "credencial do contêiner",
                  f"chave {first.container_key}, expira às {clock(expiry)}")

    # CR-2: a espera precisa passar das duas expirações para os vereditos valerem.
    expiries = [value for value in (expiry, held.redshift_expiry) if value is not None]
    detail = f"a espera {ending}, com {len(rounds)} rodada(s)"
    if not expiries:
        report.note("CR-2", "espera além das expirações", f"{detail}; nenhuma expiração a passar")
    elif rounds[-1].at > max(expiries):
        report.ok("CR-2", "espera além das expirações", detail)
    else:
        report.note("CR-2", "espera além das expirações",
                    f"{detail}, antes da expiração das {clock(max(expiries))}")

    # CR-3 a CR-8: cada cliente segurado depois da expiração da sua credencial; o cliente novo
    # que também falha põe a falha na conta do ambiente.
    control_by_client = {reading.client: reading for reading in controls}
    for client, check_id in CLIENT_CHECKS.items():
        if client not in held.readers:
            report.note(check_id, client, "cliente não aberto")
            continue
        readings = [reading_of(item, client) for item in rounds]
        readings = [reading for reading in readings if reading is not None]
        client_expiry = held.redshift_expiry if client == REDSHIFT else expiry
        status, text = held_verdict(readings, client_expiry)
        control = control_by_client.get(client)
        if status == "fail" and control is not None and not control.ok:
            status = "note"
            text += "; o cliente novo também falhou, e a falha é do ambiente"
        record(report, status, check_id, client, text)

    # CR-9: a chave do secret do DuckDB é leitura, e o veredito do DuckDB está em CR-4 e CR-5.
    secret_changed = key_change([(item.at, item.secret_key) for item in rounds])
    if first.secret_key is None:
        report.note("CR-9", "chave do secret do DuckDB", "sem secret do S3")
    elif secret_changed is None:
        report.note("CR-9", "chave do secret do DuckDB",
                    f"a chave {first.secret_key} não trocou em {len(rounds)} rodada(s)")
    else:
        report.note("CR-9", "chave do secret do DuckDB",
                    f"a chave trocou às {clock(secret_changed)} "
                    f"({signed_minutes(secret_changed, expiry)} da expiração)")

    # CR-10: a cláusula monta a chave a cada comando, e depois da expiração leva outra.
    if config.iam_role:
        report.note("CR-10", "cláusula do COPY e do UNLOAD",
                    f"IAM_ROLE {config.iam_role}: o comando não leva chave")
    else:
        status, text = key_verdict([(item.at, item.clause_key) for item in rounds], expiry)
        record(report, status, "CR-10", "cláusula do COPY e do UNLOAD", text)

    # CR-11: os clientes novos leem; sem eles, uma falha dos segurados não diz de quem é.
    failed = [reading.client for reading in controls if not reading.ok]
    if failed:
        report.fail("CR-11", "clientes novos", f"falharam: {', '.join(failed)}")
    else:
        report.ok("CR-11", "clientes novos", f"{len(controls)} cliente(s) novo(s) leram")


def close_clients(held: Held) -> None:
    """Fecha as conexões seguradas; o erro de uma não impede o fechamento da outra."""
    if held.duckdb is not None:
        held.duckdb.close()
    if held.redshift is not None:
        # A conexão que o servidor derrubou levanta no fechamento.
        with contextlib.suppress(Exception):
            held.redshift.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Segura os clientes da biblioteca além da expiração das credenciais e lê "
        "cada um de novo.")
    parser.add_argument("table", help="a URI de uma tabela Delta, de preferência pequena")
    parser.add_argument("--interval-minutes", type=float, default=DEFAULT_INTERVAL_MINUTES,
                        help=f"minutos entre as rodadas, {DEFAULT_INTERVAL_MINUTES:g} por padrão")
    parser.add_argument("--margin-minutes", type=float, default=DEFAULT_MARGIN_MINUTES,
                        help="minutos de espera depois da última expiração, "
                        f"{DEFAULT_MARGIN_MINUTES:g} por padrão")
    parser.add_argument("--max-wait-minutes", type=float, default=DEFAULT_MAX_WAIT_MINUTES,
                        help=f"teto da espera, {DEFAULT_MAX_WAIT_MINUTES:g} minutos por padrão")
    parser.add_argument("--wait-minutes", type=float,
                        help="espera fixa, no lugar da que as expirações dão")
    return parser


def main(argv: list[str]) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv[1:])

    # As variáveis que o delta-rs e o botocore leem, acertadas como a biblioteca acerta.
    changed = prepare_environment()
    try:
        storage = Storage.for_uri(arguments.table)
    except ValueError as error:
        parser.error(str(error))
    report = Report("credentials", f"os clientes da biblioteca além da expiração das "
                    f"credenciais, sobre {storage.uri}")
    report.line("Só leitura: o log e um arquivo de dados da tabela e select 1 no Redshift; "
                "nada é criado, alterado ou apagado.")

    # A configuração e a abertura dos clientes; sem a tabela, o relatório fecha aqui.
    config = configuration_section(report, storage, changed, arguments)
    held, rounds, files = clients_section(report, storage, config)
    if files is None or not rounds:
        close_clients(held)
        return report.finish()
    sample, _commit = files

    # A espera vai além da expiração da credencial do contêiner e da senha do Redshift.
    expiry = rounds[0].container_expiry if storage.is_s3 else None
    fixed = None
    if arguments.wait_minutes is not None:
        fixed = datetime.timedelta(minutes=arguments.wait_minutes)
    deadline, reason = wait_deadline(
        now(), [expiry, held.redshift_expiry],
        datetime.timedelta(minutes=arguments.margin_minutes),
        datetime.timedelta(minutes=arguments.max_wait_minutes), fixed)
    ending = wait_section(report, held, config, rounds, deadline, reason,
                          datetime.timedelta(minutes=arguments.interval_minutes), expiry)

    # O controle, a linha do tempo e os vereditos, com as conexões fechadas no fim.
    controls = control_section(report, storage, sample, config, held)
    timeline_section(report, rounds, list(held.readers), expiry)
    checks(report, storage, rounds, controls, held, config, ending)
    close_clients(held)
    return report.finish()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
