"""Configuração compartilhada dos testes.

``tests/`` recebe os testes do pacote ``serialize_db``; ``tests/proof_of_concept/`` recebe as provas
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
  ``probes/redshift.py``), e o papel do ``COPY`` e do ``UNLOAD`` de ``SERIALIZE_DB_REDSHIFT_IAM_ROLE``
  (sem ela, ``IAM_ROLE default``).
- ``SERIALIZE_DB_TEST_KEEP``: qualquer valor mantém os objetos, as pastas e as tabelas criados.
- ``SERIALIZE_DB_TEST_REPORT``: caminho de um arquivo JSON onde o relatório da sessão é gravado.
- ``SERIALIZE_DB_DUCKDB_EXTENSIONS``: pasta de extensões do DuckDB, a única onde a suíte instala as
  que faltam; sem ela, ``.duckdb/`` na raiz do repositório quando existir (criada por
  ``prepare_offline.sh``), senão o padrão do DuckDB, e nada é instalado.
"""

from __future__ import annotations

import json
import os
import shutil
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import ClassVar

import pytest

# Fatos e medições coletados pelos testes; impressos no fim da sessão e gravados em JSON.
REPORT: dict[str, object] = {}


def record(key: str, value: object) -> None:
    """Registra um fato ou uma medição no relatório da sessão."""
    REPORT[key] = value


def duckdb_extension_directory() -> str | None:
    """Pasta de extensões do DuckDB configurada, ou ``None`` para o padrão do DuckDB."""
    configured = os.environ.get("SERIALIZE_DB_DUCKDB_EXTENSIONS")
    if configured:
        return configured

    local = Path(__file__).resolve().parent.parent / ".duckdb"
    return str(local) if local.is_dir() else None


def local_root() -> Path | None:
    """Raiz da suíte local, de ``SERIALIZE_DB_TEST_LOCAL_ROOT``, ou ``None`` quando não informada."""
    configured = os.environ.get("SERIALIZE_DB_TEST_LOCAL_ROOT")
    return Path(configured).expanduser().resolve() if configured else None


def s3_root() -> str | None:
    """Raiz da suíte S3, de ``SERIALIZE_DB_TEST_S3_ROOT``, ou ``None`` quando não informada."""
    configured = os.environ.get("SERIALIZE_DB_TEST_S3_ROOT")
    return configured.rstrip("/") if configured else None


def redshift_schema() -> str | None:
    """Esquema da suíte Redshift, de ``SERIALIZE_DB_TEST_REDSHIFT_SCHEMA``, ou ``None`` quando não informado."""
    return os.environ.get("SERIALIZE_DB_TEST_REDSHIFT_SCHEMA") or None


# Motivo registrado no relatório e em ``pytest -rs`` quando a autorização de uma suíte falta.
SKIP_REASONS = {
    "local": "SERIALIZE_DB_TEST_LOCAL_ROOT não informada: a suíte local só escreve sob a pasta que ela indica",
    "s3": "SERIALIZE_DB_TEST_S3_ROOT não informada: a suíte S3 só escreve sob o prefixo que ela indica",
    "redshift": "SERIALIZE_DB_TEST_REDSHIFT_SCHEMA não informada: a suíte Redshift só cria tabelas no esquema que ela indica",
}


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Pula cada suíte cuja autorização não foi informada; roda depois da seleção por ``-m``."""
    missing = {"local": local_root() is None, "s3": s3_root() is None, "redshift": redshift_schema() is None}

    for marker, reason in SKIP_REASONS.items():
        selected = [item for item in items if marker in item.keywords]
        if not selected or not missing[marker]:
            continue

        record(f"{marker}.skipped", reason)
        for item in selected:
            item.add_marker(pytest.mark.skip(reason=reason))


@dataclass(kw_only=True)
class Storage:
    """Raiz exclusiva de uma sessão de testes num tipo de armazenamento.

    As subclasses fixam ``name``, o prefixo das chaves do relatório, e resolvem URIs e listagens no
    seu armazenamento; os testes comuns aos dois tipos usam só esta interface. É a mesma divisão que
    ``serialize_db.storage`` faz na biblioteca (``docs/PLAN.md``, etapa 3).
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
        """Arquivos Parquet de dados sob a pasta da tabela, sem os checkpoints de ``_delta_log/``."""
        raise NotImplementedError

    def record(self, key: str, value: object) -> None:
        """Registra ``key`` no relatório com o prefixo do armazenamento."""
        record(f"{self.name}.{key}", value)


@dataclass(kw_only=True)
class S3Location(Storage):
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
        pages = boto3.client("s3").get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix + "/")
        keys = [item["Key"] for page in pages for item in page.get("Contents", [])]

        return sorted(f"s3://{bucket}/{key}" for key in keys if key.endswith(".parquet") and "/_delta_log/" not in key)


@dataclass(kw_only=True)
class LocalLocation(Storage):
    """Pasta exclusiva da sessão em disco local."""

    name: ClassVar[str] = "local"
    path: Path

    @property
    def uri(self) -> str:
        return str(self.path)

    def data_files(self, table_uri: str) -> list[str]:
        folder = Path(table_uri)
        return sorted(str(file) for file in folder.rglob("*.parquet") if "_delta_log" not in file.relative_to(folder).parts)


def require_s3_access(root: str) -> None:
    """Reprova a sessão quando a raiz informada não está acessível.

    Confere as credenciais do ``boto3`` e lista um objeto sob ``<raiz>/serialize-db-poc/`` com tempos
    curtos: sem rede, o ``boto3`` esperaria 60 s por tentativa, e o delta-rs tem as próprias esperas.
    """
    import boto3
    import botocore.config

    bucket, _, prefix = root.removeprefix("s3://").partition("/")

    # Tempos curtos só para esta sondagem; os clientes dos testes usam os padrões do boto3.
    config = botocore.config.Config(connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 2, "mode": "standard"})

    try:
        session = boto3.Session()
        if session.get_credentials() is None:
            pytest.fail(f"{root} informada, mas o boto3 não encontrou credenciais (papel, variáveis AWS_* ou perfil)", pytrace=False)

        client = session.client("s3", config=config)
        client.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/serialize-db-poc/".lstrip("/"), MaxKeys=1)
    except Exception as error:  # noqa: BLE001 - falha de rede, de credencial ou de permissão
        pytest.fail(f"{root} informada, mas sem acesso: {type(error).__name__}: {error}", pytrace=False)


@pytest.fixture(scope="session")
def proxy_environment() -> Iterator[dict[str, str | None]]:
    """Exporta ``NO_PROXY`` a partir de ``no_proxy`` quando só a minúscula existe.

    O cliente HTTP do delta-rs lê apenas a variável em maiúsculas; sem ela, a chamada ao endpoint de
    credenciais do contêiner passa pelo proxy do espaço e falha. A biblioteca fará o mesmo ao iniciar.
    """
    original = {name: os.environ.get(name) for name in ("NO_PROXY", "AWS_REGION")}

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
        return

    # Limpeza: lista o prefixo da sessão página a página e apaga até 1.000 chaves por chamada.
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=location.bucket, Prefix=location.prefix + "/"):
        keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=location.bucket, Delete={"Objects": keys, "Quiet": True})


@pytest.fixture(scope="session")
def local_location() -> Iterator[LocalLocation]:
    """Pasta ``serialize-db-poc/<id>/`` sob a raiz informada, que precisa existir, apagada no fim da sessão."""
    root = local_root()
    assert root, "SERIALIZE_DB_TEST_LOCAL_ROOT não informada"

    if not root.is_dir():
        pytest.fail(f"SERIALIZE_DB_TEST_LOCAL_ROOT aponta para uma pasta inexistente: {root}", pytrace=False)

    location = LocalLocation(
        path=root / "serialize-db-poc" / uuid.uuid4().hex[:8],
        keep=bool(os.environ.get("SERIALIZE_DB_TEST_KEEP")),
    )
    location.path.mkdir(parents=True)
    record("local.root", str(root))
    record("local.session_folder", location.uri)

    yield location

    if not location.keep:
        shutil.rmtree(location.path, ignore_errors=True)


@dataclass(kw_only=True)
class RedshiftSession:
    """Conexão aberta pela suíte Redshift e o esquema onde ela cria as tabelas ``serialize_db_poc_<id>_*``."""

    connection: object
    method: str
    schema: str
    iam_role: str
    session_id: str
    keep: bool = False
    created: list[str] = field(default_factory=list)

    def table(self, suffix: str) -> str:
        """Nome, sem o esquema, de uma tabela da sessão; registrado para a limpeza."""
        name = f"serialize_db_poc_{self.session_id}_{suffix}"
        self.created.append(name)
        return name

    def qualified(self, name: str) -> str:
        """``esquema.tabela``, como o SQL a cita."""
        return f"{self.schema}.{name}"

    def iam_role_clause(self) -> str:
        """A cláusula do ``COPY`` e do ``UNLOAD``: o papel configurado ou o padrão do cluster."""
        return "IAM_ROLE default" if self.iam_role == "default" else f"IAM_ROLE '{self.iam_role}'"

    def execute(self, sql: str, params: tuple | dict | None = None) -> list[tuple]:
        """Executa ``sql`` num cursor novo e devolve as linhas, ou uma lista vazia para um comando sem resultado."""
        cursor = self.connection.cursor()
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)

        return cursor.fetchall() if cursor.description else []


def connect_redshift() -> tuple[str, object]:
    """Abre a conexão pelas variáveis ``SERIALIZE_DB_REDSHIFT_*``: por senha, ou por IAM num workgroup ou num cluster.

    A mesma resolução de ``probes/redshift.py``; a autenticação por IAM pede a região e pode criar
    o usuário do banco.
    """
    import redshift_connector

    def variable(name: str) -> str | None:
        return os.environ.get(f"SERIALIZE_DB_REDSHIFT_{name}") or None

    database = variable("DATABASE")
    if not database:
        raise RuntimeError("SERIALIZE_DB_REDSHIFT_DATABASE não informada")

    common: dict[str, object] = {"database": database, "timeout": 10}
    region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    if not region:
        import boto3

        region = boto3.Session().region_name

    if variable("HOST") and variable("USER") and variable("PASSWORD"):
        connection = redshift_connector.connect(
            host=variable("HOST"), port=int(variable("PORT") or 5439), user=variable("USER"), password=variable("PASSWORD"), ssl=True, **common
        )
        return "senha", connection

    if variable("WORKGROUP"):
        connection = redshift_connector.connect(iam=True, is_serverless=True, serverless_work_group=variable("WORKGROUP"), region=region, **common)
        return "IAM serverless", connection

    if variable("CLUSTER"):
        connection = redshift_connector.connect(iam=True, cluster_identifier=variable("CLUSTER"), db_user=variable("USER"), region=region, **common)
        return "IAM cluster", connection

    raise RuntimeError("faltam parâmetros: host, usuário e senha, ou cluster ou workgroup para autenticação por IAM")


@pytest.fixture(scope="session")
def redshift_session() -> Iterator[RedshiftSession]:
    """Conexão da suíte Redshift; as tabelas criadas são apagadas no fim da sessão."""
    schema = redshift_schema()
    assert schema, "SERIALIZE_DB_TEST_REDSHIFT_SCHEMA não informada"

    try:
        method, connection = connect_redshift()
    except Exception as error:  # noqa: BLE001 - configuração incompleta, rede ou credencial
        pytest.fail(f"SERIALIZE_DB_TEST_REDSHIFT_SCHEMA informada, mas sem conexão: {type(error).__name__}: {error}", pytrace=False)

    # Cada comando é confirmado ao terminar; o COPY e o UNLOAD não ficam presos numa transação aberta.
    connection.autocommit = True

    session = RedshiftSession(
        connection=connection,
        method=method,
        schema=schema,
        iam_role=os.environ.get("SERIALIZE_DB_REDSHIFT_IAM_ROLE") or "default",
        session_id=uuid.uuid4().hex[:8],
        keep=bool(os.environ.get("SERIALIZE_DB_TEST_KEEP")),
    )
    record("redshift.connection_method", method)
    record("redshift.schema", schema)

    yield session

    if not session.keep:
        for name in session.created:
            try:
                session.execute(f"DROP TABLE IF EXISTS {session.qualified(name)}")
            except Exception as error:  # noqa: BLE001 - a limpeza não esconde o resultado dos testes
                record(f"redshift.cleanup.{name}", f"{type(error).__name__}: {error}")

    connection.close()


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
    """Imprime o relatório da sessão e o grava em JSON quando ``SERIALIZE_DB_TEST_REPORT`` aponta um arquivo."""
    if not REPORT:
        return

    terminalreporter.section("relatório da prova de conceito")
    width = max(len(key) for key in REPORT)
    for key, value in REPORT.items():
        terminalreporter.write_line(f"{key.ljust(width)}  {value}")

    path = os.environ.get("SERIALIZE_DB_TEST_REPORT")
    if path:
        with open(path, "w", encoding="utf-8") as handle:
            json.dump(REPORT, handle, ensure_ascii=False, indent=2, default=str)
        terminalreporter.write_line(f"relatório gravado em {path}")
