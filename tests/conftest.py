"""Configuração compartilhada dos testes.

``tests/`` recebe os testes do pacote ``serialize_db`` e os das funções puras dos probes
(``test_probes.py``); ``tests/proof_of_concept/`` recebe as provas
de conceito e os testes das bibliotecas externas (delta-rs, DuckDB, PyArrow, SQLAlchemy, boto3,
redshift_connector), que também servem de material de estudo das APIs que a biblioteca usa. As
provas de conceito da camada Delta rodam sobre os dois tipos de armazenamento que a biblioteca
suporta: uma pasta local (``proof_of_concept/test_local.py``) e um bucket S3
(``proof_of_concept/test_s3.py``); a suíte Redshift (``proof_of_concept/test_redshift.py``) usa o
bucket para os arquivos e um esquema do Redshift para as tabelas.

Cada suíte escreve só onde o usuário autoriza pela variável de ambiente, e a variável é a
autorização: sem ela a suíte é pulada, com o motivo no relatório da sessão, e ``pytest`` sem
variável alguma não executa nenhum teste que grave arquivos ou crie tabelas. Com a autorização dada,
o que impede a escrita é falha: pasta local inexistente, raiz S3 sem credencial ou sem acesso,
Redshift sem conexão.

Variáveis de ambiente lidas:

- ``SERIALIZE_DB_TEST_LOCAL_ROOT``: pasta existente sob a qual a suíte local cria
  ``serialize-db-poc/<id>/``.
- ``SERIALIZE_DB_TEST_S3_ROOT``: raiz ``s3://bucket/prefixo`` sob a qual a suíte S3 cria
  ``serialize-db-poc/<id>/``.
- ``SERIALIZE_DB_TEST_REDSHIFT_SCHEMA``: esquema do Redshift onde a suíte cria as tabelas
  ``serialize_db_poc_<id>_*``. A conexão vem de ``SERIALIZE_DB_REDSHIFT_*`` (as variáveis de
  ``probes/redshift.py``) e o banco do datashare que guarda o esquema de
  ``SERIALIZE_DB_REDSHIFT_SHARE_DATABASE``: com ela, cada conexão roda ``USE <banco>`` e as tabelas
  são citadas por ``esquema.tabela``, como em ``examples/redshift_copy_unload.py``.
  ``SERIALIZE_DB_REDSHIFT_IAM_ROLE`` nomeia o papel do ``COPY`` e do ``UNLOAD``, ou a palavra
  ``default``; sem ela, os dois levam as credenciais da sessão ``boto3``, que é o caminho do
  ambiente alvo, onde o namespace não tem papel associado.
- ``SERIALIZE_DB_TEST_KEEP``: qualquer valor mantém os objetos, as pastas e as tabelas criados.
- ``SERIALIZE_DB_TEST_REPORT``: caminho de um arquivo JSON onde o relatório da sessão é
  gravado; a pasta é criada, e cada teste reprovado entra com a mensagem do erro.
  O relatório abre com a sessão (``session.``: início, plataforma, Python, versões, marcadores e,
  no fim, a contagem por resultado e a duração) e registra a limpeza de cada raiz
  (``local.cleanup``, ``s3.cleanup``), para dizer sozinho se a suíte passou e o que ficou.
- ``SERIALIZE_DB_DUCKDB_EXTENSIONS``: pasta de extensões do DuckDB, a única onde a suíte instala as
  que faltam; sem ela, ``.duckdb/`` na raiz do repositório quando existir (criada por
  ``prepare_offline.sh``), senão o padrão do DuckDB, e nada é instalado. A instalação automática
  do DuckDB, que no ``LOAD`` baixaria a extensão para ``~/.duckdb`` sem aviso, fica desligada, e o
  teste cuja extensão falta é pulado.

As três variáveis de autorização se somam: informadas juntas, ``pytest`` sem ``-m`` roda tudo, e
``-m local``, ``-m s3`` e ``-m redshift`` selecionam uma suíte. Com a autorização dada, a raiz S3
é sondada antes com tempos curtos (cerca de 11 s com um proxy que não responde, antes de o
delta-rs tentar). As suítes criam ``serialize-db-poc/<id>/`` sob a raiz ou tabelas
``serialize_db_poc_<id>_*`` no esquema, apagam tudo no fim da sessão e imprimem o relatório com os
fatos e as medições, com as chaves prefixadas pelo alvo (``local.``, ``s3.``, ``redshift.``) ou
pela biblioteca (``duckdb.``, ``sqlalchemy.``, ``pyarrow.``). Num bucket versionado, cada objeto
que a limpeza apaga vira versão não corrente, invisível à listagem e cobrada até uma regra
``NoncurrentVersionExpiration``; ``probes/bucket.py`` (``BK-14``) conta o acumulado. Fora das
raízes informadas, o que uma sessão grava é ``.pytest_cache/`` na raiz do repositório, do próprio
pytest.

O que cada suíte exige do ambiente: a suíte S3 precisa de credenciais da AWS que o ``boto3``
encontre (papel do contêiner ou da instância, variáveis ``AWS_*`` ou perfil), das permissões
``s3:ListBucket``, ``s3:GetObject``, ``s3:PutObject`` e ``s3:DeleteObject`` sob o prefixo (e as
de KMS quando o bucket usa SSE-KMS), e das extensões ``httpfs``, ``delta`` e ``aws`` do DuckDB. A
suíte local precisa só da extensão ``delta``. A suíte Redshift precisa de
``redshift-serverless:GetWorkgroup`` e ``GetCredentials`` no workgroup, do ``GRANT`` que deixa o
usuário criar tabelas no esquema, e de ``redshift-data:ExecuteStatement``, ``DescribeStatement`` e
``GetStatementResult`` para o teste da Data API, que é pulado sem workgroup. Como o ``COPY`` e o
``UNLOAD`` alcançam o S3 pelas credenciais de quem chama, a identidade da sessão precisa das mesmas
permissões de S3 da suíte S3 sob a raiz, a não ser que ``SERIALIZE_DB_REDSHIFT_IAM_ROLE`` nomeie
um papel associado ao namespace. A suíte copia a região do ``boto3`` para ``AWS_REGION``, num
sentido só: um ambiente com apenas ``AWS_REGION`` e sem ``~/.aws/config`` precisa de manutenção.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import re
import platform
import shutil
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import ClassVar

import pytest

# Fatos e medições coletados pelos testes; impressos no fim da sessão e gravados em JSON.
REPORT: dict[str, object] = {}

# As versões que o relatório cita: as que as medições dependem.
SESSION_PACKAGES = ("deltalake", "duckdb", "pyarrow", "boto3", "sqlalchemy", "pandas", "pytest")
SESSION = {"started": time.time()}


# O COPY e o UNLOAD levam as credenciais de quem chama no texto do comando; nada que carregue esse
# texto, nem o erro que o cita, entra num relatório feito para ser colado na conversa. As duas
# saídas do relatório, a impressa e o JSON, passam por mask_credentials, que alcança também o valor
# guardado dentro de um dicionário ou de uma lista; a aspa do valor pode chegar escapada por repr
# ou pelo JSON (\' ou \\').
CREDENTIAL_PATTERN = re.compile(
    r"(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN|CREDENTIALS)\s+\\*'[^']*'", re.IGNORECASE
)


def mask_credentials(text: str) -> str:
    """Troca por ``***`` o valor de toda cláusula de credencial num texto."""
    return CREDENTIAL_PATTERN.sub(r"\1 '***'", text)


def record(key: str, value: object) -> None:
    """Registra um fato ou uma medição no relatório da sessão; a impressão e o JSON do relatório
    mascaram as credenciais."""
    REPORT[key] = value


def now_utc() -> str:
    """O instante atual em UTC, em ISO 8601 com precisão de segundos, para o relatório."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def opened_partition_folders(connection: object, column: str) -> set[str]:
    """As pastas ``<coluna>=<valor>`` dos arquivos Parquet que o DuckDB abriu, lidas do log
    ``FileSystem`` desde o último ``CALL truncate_duckdb_logs()``.

    A conexão precisa de ``CALL enable_logging('FileSystem')`` antes da consulta medida; cada
    abertura de arquivo é uma mensagem com ``"op":"OPEN"`` e o caminho do arquivo.
    """
    messages = connection.execute(
        "SELECT message FROM duckdb_logs WHERE type = 'FileSystem'").fetchall()
    folders = set()
    for (message,) in messages:
        if '"op":"OPEN"' in message and ".parquet" in message:
            folders.add(re.search(rf"{column}=[0-9-]+", message).group(0))
    return folders


def pytest_sessionstart(session: pytest.Session) -> None:
    """Abre o relatório com a sessão: quando, onde, com que versões e com que seleção ela roda."""
    versions = []
    for name in SESSION_PACKAGES:
        try:
            versions.append(f"{name} {importlib.metadata.version(name)}")
        except importlib.metadata.PackageNotFoundError:
            versions.append(f"{name} ausente")
    SESSION["started"] = time.time()
    record("session.started_at", now_utc())
    record("session.platform", platform.platform())
    record("session.python", platform.python_version())
    record("session.packages", ", ".join(versions))
    record("session.markers", session.config.option.markexpr or "(todos)")


def duckdb_extension_directory() -> str | None:
    """Pasta de extensões do DuckDB configurada, ou ``None`` para o padrão do DuckDB."""
    configured = os.environ.get("SERIALIZE_DB_DUCKDB_EXTENSIONS")
    if configured:
        return configured

    local = Path(__file__).resolve().parent.parent / ".duckdb"
    return str(local) if local.is_dir() else None


def local_root() -> Path | None:
    """Raiz da suíte local, de ``SERIALIZE_DB_TEST_LOCAL_ROOT``, ou ``None`` quando não
    informada."""
    configured = os.environ.get("SERIALIZE_DB_TEST_LOCAL_ROOT")
    return Path(configured).expanduser().resolve() if configured else None


def s3_root() -> str | None:
    """Raiz da suíte S3, de ``SERIALIZE_DB_TEST_S3_ROOT``, ou ``None`` quando não informada."""
    configured = os.environ.get("SERIALIZE_DB_TEST_S3_ROOT")
    return configured.rstrip("/") if configured else None


def redshift_schema() -> str | None:
    """Esquema da suíte Redshift, de ``SERIALIZE_DB_TEST_REDSHIFT_SCHEMA``, ou ``None`` quando não
    informado."""
    return os.environ.get("SERIALIZE_DB_TEST_REDSHIFT_SCHEMA") or None


# Motivo registrado no relatório e em ``pytest -rs`` quando a autorização de uma suíte falta.
SKIP_REASONS = {
    "local": (
        "SERIALIZE_DB_TEST_LOCAL_ROOT não informada: a suíte local só escreve sob a pasta que ela "
        "indica"
    ),
    "s3": (
        "SERIALIZE_DB_TEST_S3_ROOT não informada: a suíte S3 só escreve sob o prefixo que ela "
        "indica"
    ),
    "redshift": (
        "SERIALIZE_DB_TEST_REDSHIFT_SCHEMA não informada: a suíte Redshift só cria tabelas no "
        "esquema que ela indica"
    ),
}

# Como autorizar cada suíte; impresso no fim da sessão quando ela foi pulada. Não é um erro: sem a
# variável a suíte não tem onde escrever, e o usuário decide se e onde ela escreve.
USAGE = {
    "local": (
        "SERIALIZE_DB_TEST_LOCAL_ROOT=/pasta/existente uv run pytest -m local",
        "grava só em serialize-db-poc/<id>/ sob essa pasta e a apaga no fim; nenhum acesso à AWS",
    ),
    "s3": (
        "SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo uv run pytest -m s3",
        "grava só em serialize-db-poc/<id>/ sob o prefixo e o apaga no fim; precisa de credenciais "
        "que o boto3 encontre e das extensões httpfs, delta e aws do DuckDB",
    ),
    "redshift": (
        "SERIALIZE_DB_TEST_REDSHIFT_SCHEMA=esquema SERIALIZE_DB_REDSHIFT_WORKGROUP=workgroup "
        "SERIALIZE_DB_REDSHIFT_DATABASE=banco "
        "SERIALIZE_DB_REDSHIFT_SHARE_DATABASE=banco_do_datashare "
        "SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo uv run pytest -m redshift",
        "cria só tabelas serialize_db_poc_<id>_* no esquema e as apaga no fim; com _WORKGROUP a "
        "credencial é temporária (redshift-serverless:GetWorkgroup e GetCredentials, "
        "examples/redshift_native.py), e _HOST com _USER e _PASSWORD é o par informado na mesma "
        "chamada; _SHARE_DATABASE quando o esquema vem de um datashare (USE); "
        "SERIALIZE_DB_REDSHIFT_IAM_ROLE para o COPY e o UNLOAD (sem ela, as credenciais de quem "
        "chama)",
    ),
}


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Pula cada suíte cuja autorização não foi informada; roda depois da seleção por ``-m``."""
    missing = {
        "local": local_root() is None,
        "s3": s3_root() is None,
        "redshift": redshift_schema() is None,
    }

    for marker, reason in SKIP_REASONS.items():
        # A marca, e não os keywords do item: o id de um parâmetro também entra nos keywords, e um
        # parâmetro chamado redshift pularia o caso com a suíte Redshift.
        selected = [item for item in items if item.get_closest_marker(marker) is not None]
        if not selected or not missing[marker]:
            continue

        record(f"{marker}.skipped", reason)
        for item in selected:
            item.add_marker(pytest.mark.skip(reason=reason))


@dataclass(kw_only=True)
class SessionRoot:
    """Raiz exclusiva de uma sessão de testes num tipo de armazenamento.

    As subclasses fixam ``name``, o prefixo das chaves do relatório, e resolvem URIs e listagens no
    seu armazenamento; os testes comuns aos dois tipos usam só esta interface. É a mesma divisão que
    ``serialize_db.storage`` faz na biblioteca (``plan/PLAN-STAGE-3.md``).
    """

    name: ClassVar[str]
    keep: bool = False

    @property
    def uri(self) -> str:
        raise NotImplementedError

    def child(self, name: str) -> str:
        """URI de ``name`` sob a raiz da sessão."""
        return f"{self.uri}/{name}"

    def data_files(self, table_uri: str) -> list[str]:
        """Arquivos Parquet de dados sob a pasta da tabela, sem os checkpoints de
        ``_delta_log/``."""
        raise NotImplementedError

    def record(self, key: str, value: object) -> None:
        """Registra ``key`` no relatório com o prefixo do armazenamento."""
        record(f"{self.name}.{key}", value)


@dataclass(kw_only=True)
class S3Location(SessionRoot):
    """Bucket e prefixo exclusivos da sessão."""

    name: ClassVar[str] = "s3"
    bucket: str
    prefix: str

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"

    def data_files(self, table_uri: str) -> list[str]:
        import boto3

        # A listagem é paginada pelo boto3; cada página traz até 1.000 chaves em ``Contents``.
        bucket, _, prefix = table_uri.removeprefix("s3://").partition("/")
        paginator = boto3.client("s3").get_paginator("list_objects_v2")
        keys = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix + "/"):
            keys.extend(item["Key"] for item in page.get("Contents", []))

        files = []
        for key in keys:
            in_log = "/_delta_log/" in key
            if key.endswith(".parquet") and not in_log:
                files.append(f"s3://{bucket}/{key}")
        return sorted(files)


@dataclass(kw_only=True)
class LocalLocation(SessionRoot):
    """Pasta exclusiva da sessão em disco local."""

    name: ClassVar[str] = "local"
    path: Path

    @property
    def uri(self) -> str:
        return str(self.path)

    def data_files(self, table_uri: str) -> list[str]:
        folder = Path(table_uri)
        files = []
        for file in folder.rglob("*.parquet"):
            in_log = "_delta_log" in file.relative_to(folder).parts
            if not in_log:
                files.append(str(file))
        return sorted(files)


def require_s3_access(root: str) -> None:
    """Reprova a sessão quando a raiz informada não está acessível.

    Confere as credenciais do ``boto3`` e lista um objeto sob ``<raiz>/serialize-db-poc/`` com
    tempos curtos: sem rede, o ``boto3`` esperaria 60 s por tentativa, e o delta-rs tem as próprias
    esperas.
    """
    import boto3
    import botocore.config

    bucket, _, prefix = root.removeprefix("s3://").partition("/")

    # Tempos curtos só para esta sondagem; os clientes dos testes usam os padrões do boto3.
    config = botocore.config.Config(
        connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 2, "mode": "standard"}
    )

    try:
        session = boto3.Session()
        # pytest.fail levanta Failed, que deriva de BaseException: o except abaixo não a captura.
        if session.get_credentials() is None:
            pytest.fail(
                f"{root} informada, mas o boto3 não encontrou credenciais "
                "(papel, variáveis AWS_* ou perfil)",
                pytrace=False,
            )

        client = session.client("s3", config=config)
        client.list_objects_v2(
            Bucket=bucket, Prefix=f"{prefix}/serialize-db-poc/".lstrip("/"), MaxKeys=1
        )
    except Exception as error:  # noqa: BLE001 - falha de rede, de credencial ou de permissão
        pytest.fail(
            f"{root} informada, mas sem acesso: {type(error).__name__}: {error}", pytrace=False
        )


def variable_state(value: str | None) -> str:
    """O estado de uma variável de ambiente para o relatório: ausente, vazia ou definida."""
    if value is None:
        return "ausente"
    if value == "":
        return "vazia"
    return "definida"


@pytest.fixture(scope="session")
def proxy_environment() -> Iterator[dict[str, str | None]]:
    """Exporta ``NO_PROXY`` a partir de ``no_proxy`` quando a maiúscula está ausente ou vazia.

    O cliente HTTP do delta-rs lê ``NO_PROXY`` e, só quando ela está ausente, ``no_proxy``; vazia,
    ela anula as exceções, e a chamada ao endpoint de credenciais do contêiner passa pelo proxy do
    espaço e falha com 403. Na biblioteca, ``serialize_db.storage.prepare_environment`` faz o
    mesmo, chamada por ``Database`` ao iniciar.
    """
    original = {name: os.environ.get(name) for name in ("NO_PROXY", "AWS_REGION")}
    record("environment.no_proxy_as_found", variable_state(original["NO_PROXY"]))

    # Vazia conta como ausente: ``get`` devolve "" e a condição a substitui.
    if not os.environ.get("NO_PROXY") and os.environ.get("no_proxy"):
        os.environ["NO_PROXY"] = os.environ["no_proxy"]

    # O delta-rs exige a região em AWS_REGION ou AWS_DEFAULT_REGION; o boto3 a infere do perfil.
    if not os.environ.get("AWS_REGION"):
        import boto3

        region = boto3.Session().region_name or os.environ.get("AWS_DEFAULT_REGION")
        if region:
            os.environ["AWS_REGION"] = region

    record("environment.no_proxy_exported", os.environ.get("NO_PROXY") != original["NO_PROXY"])
    record("environment.aws_region", os.environ.get("AWS_REGION"))

    yield original

    # Devolve o ambiente do processo ao estado em que a sessão o encontrou.
    for name, value in original.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


@pytest.fixture(scope="session")
def s3_location(proxy_environment: dict[str, str | None]) -> Iterator[S3Location]:
    """Prefixo ``serialize-db-poc/<id>/`` sob a raiz informada, apagado no fim da sessão."""
    import boto3

    root = s3_root()
    assert root, "SERIALIZE_DB_TEST_S3_ROOT não informada"
    require_s3_access(root)

    bucket, _, prefix = root.removeprefix("s3://").partition("/")
    session_id = uuid.uuid4().hex[:8]
    location = S3Location(
        bucket=bucket,
        prefix=f"{prefix}/serialize-db-poc/{session_id}".strip("/"),
        keep=bool(os.environ.get("SERIALIZE_DB_TEST_KEEP")),
    )
    record("s3.root", root)
    record("s3.session_prefix", location.uri)

    yield location

    if location.keep:
        record("s3.cleanup", f"mantida por SERIALIZE_DB_TEST_KEEP: {location.uri}")
        return

    # Limpeza: lista o prefixo da sessão página a página e apaga até 1.000 chaves por chamada. Com
    # Quiet, a resposta traz só as chaves recusadas, em Errors.
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    deleted = 0
    refused = []
    for page in paginator.paginate(Bucket=location.bucket, Prefix=location.prefix + "/"):
        keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if not keys:
            continue
        response = s3.delete_objects(
            Bucket=location.bucket, Delete={"Objects": keys, "Quiet": True}
        )
        errors = response.get("Errors", [])
        refused.extend(f"{error['Key']}: {error.get('Code')}" for error in errors)
        deleted += len(keys) - len(errors)
    # Num bucket versionado a exclusão só cria marcadores: os objetos viram versões não correntes,
    # cobradas até uma regra de ciclo de vida.
    versioned = ""
    if REPORT.get("s3.versioned"):
        versioned = (
            "; no bucket versionado cada um vira versão não corrente até uma regra "
            "NoncurrentVersionExpiration"
        )
    record("s3.cleanup", f"{deleted} objeto(s) apagado(s) sob {location.uri}{versioned}")
    if refused:
        record("s3.cleanup.refused", refused)


@pytest.fixture(scope="session")
def local_location() -> Iterator[LocalLocation]:
    """Pasta ``serialize-db-poc/<id>/`` sob a raiz informada, que precisa existir, apagada no fim da
    sessão."""
    root = local_root()
    assert root, "SERIALIZE_DB_TEST_LOCAL_ROOT não informada"

    if not root.is_dir():
        pytest.fail(
            f"SERIALIZE_DB_TEST_LOCAL_ROOT aponta para uma pasta inexistente: {root}", pytrace=False
        )

    location = LocalLocation(
        path=root / "serialize-db-poc" / uuid.uuid4().hex[:8],
        keep=bool(os.environ.get("SERIALIZE_DB_TEST_KEEP")),
    )
    location.path.mkdir(parents=True)
    record("local.root", str(root))
    record("local.session_folder", location.uri)

    yield location

    if location.keep:
        record("local.cleanup", f"mantida por SERIALIZE_DB_TEST_KEEP: {location.uri}")
        return
    shutil.rmtree(location.path, ignore_errors=True)
    if location.path.exists():
        record("local.cleanup", f"não apagada por inteiro: {location.uri}")
        return
    # A pasta comum às sessões sai quando fica vazia.
    parent = location.path.parent
    if not any(parent.iterdir()):
        parent.rmdir()
    record("local.cleanup", f"apagada: {location.uri}")


@dataclass(kw_only=True)
class RedshiftSession:
    """Conexão aberta pela suíte Redshift e o esquema onde ela cria as tabelas
    ``serialize_db_poc_<id>_*``."""

    connection: object
    method: str
    schema: str
    iam_role: str | None
    session_id: str
    share_database: str | None = None
    keep: bool = False
    created: list[str] = field(default_factory=list)

    def table(self, suffix: str) -> str:
        """Nome, sem o esquema, de uma tabela da sessão; registrado para a limpeza."""
        name = f"serialize_db_poc_{self.session_id}_{suffix}"
        self.created.append(name)
        return name

    def qualified(self, name: str) -> str:
        """O nome como a sessão o cita: ``esquema.tabela``, porque a conexão já rodou ``USE`` no
        banco do datashare."""
        return f"{self.schema}.{name}"

    def fully_qualified(self, name: str) -> str:
        """O nome em três partes, para quem está conectado a outro banco: a Data API, que abre a
        sessão dela."""
        parts = [part for part in (self.share_database, self.schema, name) if part]
        return ".".join(parts)

    def credentials_clause(self) -> str:
        """Como o ``COPY`` e o ``UNLOAD`` alcançam o S3: o papel IAM configurado, ou as credenciais
        de quem chama.

        Sem ``SERIALIZE_DB_REDSHIFT_IAM_ROLE``, o comando leva ``ACCESS_KEY_ID``,
        ``SECRET_ACCESS_KEY`` e ``SESSION_TOKEN`` da sessão ``boto3``
        (``examples/redshift_copy_unload.py``), porque o namespace do ambiente alvo não tem papel
        associado e sem papel associado nem um ARN explícito funciona. O texto devolvido carrega
        segredo: ele nunca é impresso, registrado no relatório nem gravado em arquivo.
        """
        if self.iam_role == "default":
            return "IAM_ROLE default"
        if self.iam_role:
            return f"IAM_ROLE '{self.iam_role}'"

        import boto3

        credentials = boto3.Session().get_credentials().get_frozen_credentials()
        clause = (
            f"ACCESS_KEY_ID '{credentials.access_key}'\n"
            f"SECRET_ACCESS_KEY '{credentials.secret_key}'"
        )
        if not credentials.token:
            return clause
        return f"{clause}\nSESSION_TOKEN '{credentials.token}'"

    def execute(self, sql: str, params: tuple | dict | None = None) -> list[tuple]:
        """Executa ``sql`` num cursor novo e devolve as linhas, ou uma lista vazia para um comando
        sem resultado."""
        cursor = self.connection.cursor()
        cursor.execute(sql, params)
        return cursor.fetchall() if cursor.description else []


def redshift_variable(name: str) -> str | None:
    """``SERIALIZE_DB_REDSHIFT_<name>``, com a variável vazia lida como ausente."""
    return os.environ.get(f"SERIALIZE_DB_REDSHIFT_{name}") or None


def prepare_redshift_session(connection: object) -> None:
    """Liga o autocommit e roda ``USE <banco>`` quando o esquema vem de um datashare: daí em diante,
    ``esquema.tabela`` basta.

    O autocommit vem antes do primeiro comando. Desligado, o ``redshift_connector`` emite
    ``begin transaction`` antes do primeiro ``execute``, e ligá-lo depois não fecha essa
    transação: a sessão inteira corre nela, e o primeiro erro do servidor (a visão de sistema
    negada a um usuário comum) aborta tudo o que vem depois, inclusive a limpeza, com
    ``25P02`` (``plan/POC.md``). Cada comando confirmado ao terminar é também o que o ``COPY``
    e o ``UNLOAD`` precisam para não ficarem presos numa transação aberta.

    O ``USE`` é o passo de ``examples/redshift_copy_unload.py``: sem ele, quem não está
    conectado ao banco compartilhado só cita objetos por nome em três partes, e o ``CREATE`` e
    o ``COPY`` não foram exercitados assim.
    """
    connection.autocommit = True
    share = redshift_variable("SHARE_DATABASE")
    if share:
        cursor = connection.cursor()
        cursor.execute(f"USE {share}")


def workgroup_login(database: str) -> dict[str, object]:
    """O endereço e o par usuário e senha do workgroup de ``SERIALIZE_DB_REDSHIFT_WORKGROUP``, nos
    argumentos de ``redshift_connector.connect``.

    O endereço vem de ``get_workgroup``, e ``_HOST`` e ``_PORT`` o substituem; o par vem de
    ``get_credentials``, pedido com ``durationSeconds=3600``. A região é a de ``AWS_REGION``, a de
    ``AWS_DEFAULT_REGION`` ou, sem as duas, a da sessão ``boto3``.
    """
    import boto3

    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region:
        region = boto3.Session().region_name

    workgroup = redshift_variable("WORKGROUP")
    serverless = boto3.client("redshift-serverless", region_name=region)
    endpoint = serverless.get_workgroup(workgroupName=workgroup)["workgroup"]["endpoint"]
    credentials = serverless.get_credentials(
        workgroupName=workgroup, dbName=database, durationSeconds=3600
    )
    return {
        "host": redshift_variable("HOST") or endpoint["address"],
        "port": int(redshift_variable("PORT") or endpoint["port"]),
        "user": credentials["dbUser"],
        "password": credentials["dbPassword"],
    }


def connect_redshift(*, statement_cache: bool = False) -> tuple[str, object]:
    """Abre a conexão pelas variáveis ``SERIALIZE_DB_REDSHIFT_*`` e devolve o método e a conexão.

    Com ``_WORKGROUP``, o endereço vem de ``get_workgroup`` e o par usuário e senha de
    ``get_credentials``: o caminho de ``examples/redshift_native.py``, executado no ambiente alvo.
    Com ``_HOST``, ``_USER`` e ``_PASSWORD``, o par informado entra na mesma chamada. O IAM interno
    do ``redshift_connector`` e o cluster provisionado não são caminhos da suíte: ninguém os
    executou no ambiente alvo, que não tem cluster. A mesma resolução de ``probes/redshift.py``.
    Cada chamada pede a sua credencial, que dura no máximo uma hora, e a credencial derivada da
    identidade IAM cria o usuário do banco quando ele ainda não existe.

    A conexão vai sem ``timeout``: no ``redshift_connector`` ele é o tempo limite do socket, para
    conectar e para ler, e um ``COPY`` ou um ``UNLOAD`` dura mais que qualquer espera razoável
    (``plan/POC.md``). Uma rede morta aparece como o tempo limite do sistema, não como um teste
    reprovado no meio de uma carga.

    A conexão vai com ``max_prepared_statements=0``. O ``redshift_connector`` guarda um prepared
    statement nomeado por texto de comando e o reaproveita no ``execute`` seguinte do mesmo texto,
    e só descarta os guardados quando o servidor confirma um ``ALTER``, ``CREATE``, ``DROP`` ou
    ``ROLLBACK`` (``core.py``, ``handle_COMMAND_COMPLETE``), nunca num ``TRUNCATE``. Numa tabela do
    datashare, o comando reexecutado depois de um ``TRUNCATE`` recebe ``34510``, ``Concurrent DDL
    committed ... between Prepare and Execute`` (``plan/POC.md``). Com zero, o driver prepara o
    statement sem nome logo antes de cada execução e não guarda nada; ``statement_cache=True``
    mantém o padrão do driver, para a leitura que reproduz o erro.
    """
    import redshift_connector

    database = redshift_variable("DATABASE")
    if not database:
        raise RuntimeError("SERIALIZE_DB_REDSHIFT_DATABASE não informada")

    # O par informado vem antes da credencial temporária do workgroup.
    host = redshift_variable("HOST")
    user = redshift_variable("USER")
    password = redshift_variable("PASSWORD")
    if host and user and password:
        method = "par informado"
        port = int(redshift_variable("PORT") or 5439)
        login = {"host": host, "port": port, "user": user, "password": password}
    elif redshift_variable("WORKGROUP"):
        method = "credencial temporária do workgroup"
        login = workgroup_login(database)
    else:
        raise RuntimeError(
            "faltam parâmetros: SERIALIZE_DB_REDSHIFT_WORKGROUP para a credencial temporária, "
            "ou _HOST, _USER e _PASSWORD"
        )

    options: dict[str, object] = {"database": database}
    if not statement_cache:
        options["max_prepared_statements"] = 0
    connection = redshift_connector.connect(**login, **options)
    prepare_redshift_session(connection)
    return method, connection


def drop_session_tables(session: RedshiftSession) -> None:
    """Apaga as tabelas ``serialize_db_poc_<id>_*`` que a sessão criou; cada falha vai ao relatório
    sem esconder o resultado dos testes."""
    # Uma transação abortada por um teste recusaria cada DROP com 25P02: a limpeza começa fora
    # dela.
    if session.connection.in_transaction:
        try:
            session.connection.rollback()
        except Exception as error:  # noqa: BLE001 - a limpeza não esconde o resultado dos testes
            record("redshift.cleanup.rollback", f"{type(error).__name__}: {error}")
    for name in session.created:
        try:
            session.execute(f"DROP TABLE IF EXISTS {session.qualified(name)}")
        except Exception as error:  # noqa: BLE001 - a limpeza não esconde o resultado dos testes
            record(f"redshift.cleanup.{name}", f"{type(error).__name__}: {error}")


@pytest.fixture(scope="session")
def redshift_session() -> Iterator[RedshiftSession]:
    """Conexão da suíte Redshift; as tabelas criadas são apagadas no fim da sessão."""
    schema = redshift_schema()
    assert schema, "SERIALIZE_DB_TEST_REDSHIFT_SCHEMA não informada"

    try:
        method, connection = connect_redshift()
    except Exception as error:  # noqa: BLE001 - configuração incompleta, rede ou credencial
        pytest.fail(
            "SERIALIZE_DB_TEST_REDSHIFT_SCHEMA informada, mas sem conexão: "
            f"{type(error).__name__}: {error}",
            pytrace=False,
        )

    session = RedshiftSession(
        connection=connection,
        method=method,
        schema=schema,
        iam_role=os.environ.get("SERIALIZE_DB_REDSHIFT_IAM_ROLE") or None,
        session_id=uuid.uuid4().hex[:8],
        share_database=os.environ.get("SERIALIZE_DB_REDSHIFT_SHARE_DATABASE") or None,
        keep=bool(os.environ.get("SERIALIZE_DB_TEST_KEEP")),
    )
    record("redshift.connection_method", method)
    record("redshift.schema", session.fully_qualified("<tabela>"))

    yield session

    if not session.keep:
        drop_session_tables(session)
    connection.close()


def failure_message(report: pytest.TestReport, limit: int = 300) -> str:
    """As primeiras linhas do erro de um teste reprovado, para o relatório: a exceção e a asserção
    que a explica."""
    crash = getattr(report.longrepr, "reprcrash", None)
    text = crash.message if crash is not None else str(report.longrepr)
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    return " | ".join(lines[:2])[:limit] if lines else report.outcome


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
    """Imprime o relatório da sessão, as instruções das suítes puladas e grava o JSON de
    ``SERIALIZE_DB_TEST_REPORT``."""
    # O resultado entra no relatório: sem ele o JSON não diz se a suíte passou nem se a limpeza
    # rodou.
    stats = terminalreporter.stats
    counts = []
    for name in ("passed", "failed", "error", "skipped"):
        counts.append(f"{len(stats.get(name, []))} {name}")
    record("session.outcome", ", ".join(counts))
    record("session.finished_at", now_utc())
    record("session.duration_s", round(time.time() - SESSION["started"], 1))

    # Cada teste reprovado entra com a mensagem do erro: uma contagem diz quantos falharam, não por
    # quê.
    for kind in ("failed", "error"):
        for report in stats.get(kind, []):
            record(f"{kind}.{report.nodeid}", failure_message(report))

    # As medições e os fatos coletados; as chaves ``<suíte>.skipped`` ficam fora, porque a seção
    # seguinte as explica.
    measurements = {key: value for key, value in REPORT.items() if not key.endswith(".skipped")}
    if measurements:
        terminalreporter.section("relatório da prova de conceito")
        width = max(len(key) for key in measurements)
        for key, value in measurements.items():
            terminalreporter.write_line(mask_credentials(f"{key.ljust(width)}  {value}"))

    # Instruções, não erro: cada suíte pulada mostra a variável que a autoriza e o que ela grava.
    skipped = [marker for marker in USAGE if f"{marker}.skipped" in REPORT]
    if skipped:
        terminalreporter.section("suítes não executadas: como autorizá-las", sep="-")
        terminalreporter.write_line(
            "Cada suíte grava só onde a sua variável de ambiente autoriza; sem a variável ela é "
            "pulada."
        )
        for marker in skipped:
            command, note = USAGE[marker]
            terminalreporter.write_line(f"  {marker}:")
            terminalreporter.write_line(f"    {command}")
            terminalreporter.write_line(f"    {note}.")
        terminalreporter.write_line(
            "  Na pasta preparada sem internet, `.venv/bin/python -m pytest` no lugar de "
            "`uv run pytest`;"
        )
        terminalreporter.write_line("  as variáveis estão descritas em README.md, seção Testes.")

    path = os.environ.get("SERIALIZE_DB_TEST_REPORT")
    if path:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        text = json.dumps(REPORT, ensure_ascii=False, indent=2, default=str)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(mask_credentials(text))
        terminalreporter.write_line(f"relatório gravado em {path}")
