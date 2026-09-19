"""Configuração compartilhada dos testes.

As provas de conceito da camada Delta rodam sobre os dois tipos de armazenamento que a biblioteca
suporta: uma pasta local (``test_local_proof_of_concept.py``), disponível em qualquer ambiente, e um
bucket S3 (``test_s3_proof_of_concept.py``), que exige uma raiz configurada, credenciais da AWS e
acesso ao bucket. Os testes marcados com ``s3`` são pulados quando qualquer dessas condições falta, e
o motivo vai para o relatório da sessão. A raiz S3 vem, nesta ordem, de ``SERIALIZE_DB_TEST_S3_ROOT``
(``s3://bucket/prefixo``) ou de ``sagemaker_studio.Project().s3.root`` dentro de um espaço do
SageMaker Unified Studio.

Variáveis de ambiente lidas:

- ``SERIALIZE_DB_TEST_S3_ROOT``: raiz S3 sob a qual os testes criam ``serialize-db-poc/<id>/``.
- ``SERIALIZE_DB_TEST_LOCAL_ROOT``: pasta sob a qual os testes criam ``serialize-db-poc/<id>/``; sem
  ela, a pasta temporária da sessão do pytest.
- ``SERIALIZE_DB_TEST_KEEP``: qualquer valor mantém os objetos e as pastas criados depois da sessão.
- ``SERIALIZE_DB_TEST_REPORT``: caminho de um arquivo JSON onde o relatório da sessão é gravado.
- ``SERIALIZE_DB_DUCKDB_EXTENSIONS``: pasta de extensões do DuckDB; sem ela, ``.duckdb/`` na raiz do
  repositório quando existir (criada por ``prepare_offline.sh``), senão o padrão do DuckDB.
"""

from __future__ import annotations

import functools
import json
import os
import shutil
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
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


@dataclass(kw_only=True)
class Storage:
    """Raiz exclusiva de uma sessão de testes num tipo de armazenamento.

    As subclasses fixam ``name``, o prefixo das chaves do relatório, e resolvem URIs e listagens no
    seu armazenamento; os testes comuns aos dois tipos usam só esta interface.
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


@dataclass(frozen=True)
class S3Availability:
    """Raiz S3 utilizável pela sessão, ou o motivo de os testes ``s3`` serem pulados."""

    root: str | None = None
    reason: str | None = None


def _s3_root_from_environment() -> str | None:
    root = os.environ.get("SERIALIZE_DB_TEST_S3_ROOT")
    if root:
        return root.rstrip("/")
    try:
        from sagemaker_studio import Project  # só existe dentro do espaço do SageMaker
    except ImportError:
        return None
    try:
        return str(Project().s3.root).rstrip("/")
    except Exception:  # noqa: BLE001 - qualquer falha do SDK significa "sem raiz"
        return None


@functools.cache
def s3_availability() -> S3Availability:
    """Verifica uma vez por sessão a raiz, as credenciais e o acesso ao prefixo dos testes.

    A sondagem lista um objeto sob ``<raiz>/serialize-db-poc/`` com tempos curtos: sem rede, o
    ``boto3`` esperaria 60 s por tentativa.
    """
    root = _s3_root_from_environment()
    if not root:
        return S3Availability(reason="sem raiz S3: defina SERIALIZE_DB_TEST_S3_ROOT ou rode num espaço do SageMaker")
    import boto3
    import botocore.config

    bucket, _, prefix = root.removeprefix("s3://").partition("/")
    config = botocore.config.Config(connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 2, "mode": "standard"})
    try:
        session = boto3.Session()
        if session.get_credentials() is None:
            return S3Availability(reason="sem credenciais da AWS: o boto3 não encontrou papel, variáveis AWS_* nem perfil")
        client = session.client("s3", config=config)
        client.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/serialize-db-poc/".lstrip("/"), MaxKeys=1)
    except Exception as error:  # noqa: BLE001 - falha de rede, de credencial ou de permissão
        return S3Availability(reason=f"sem acesso a {root}: {type(error).__name__}: {error}")
    return S3Availability(root=root)


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Pula os testes ``s3`` quando falta raiz, credencial ou acesso; a sondagem só roda se algum foi selecionado."""
    if not any("s3" in item.keywords for item in items):
        return
    availability = s3_availability()
    if availability.root:
        return
    record("s3.skipped", availability.reason)
    skip = pytest.mark.skip(reason=availability.reason)
    for item in items:
        if "s3" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def proxy_environment() -> Iterator[dict[str, str | None]]:
    """Exporta ``NO_PROXY`` a partir de ``no_proxy`` quando só a minúscula existe.

    O cliente HTTP do delta-rs lê apenas a variável em maiúsculas; sem ela, a chamada ao endpoint de
    credenciais do contêiner passa pelo proxy do espaço e falha. A biblioteca fará o mesmo ao iniciar.
    """
    original = {name: os.environ.get(name) for name in ("NO_PROXY", "AWS_REGION")}
    if not os.environ.get("NO_PROXY") and os.environ.get("no_proxy"):
        os.environ["NO_PROXY"] = os.environ["no_proxy"]
    if not os.environ.get("AWS_REGION"):
        import boto3

        region = boto3.Session().region_name or os.environ.get("AWS_DEFAULT_REGION")
        if region:
            os.environ["AWS_REGION"] = region  # o delta-rs exige a região; o boto3 a infere
    record("environment.no_proxy_exported", os.environ.get("NO_PROXY") != original["NO_PROXY"])
    record("environment.aws_region", os.environ.get("AWS_REGION"))
    yield original
    for name, value in original.items():
        if value is None:
            os.environ.pop(name, None)
        else:
            os.environ[name] = value


@pytest.fixture(scope="session")
def s3_location(proxy_environment: dict[str, str | None]) -> Iterator[S3Location]:
    """Prefixo ``serialize-db-poc/<id>/`` sob a raiz configurada, apagado no fim da sessão."""
    import boto3

    root = s3_availability().root
    assert root, "raiz S3 não disponível"
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
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=location.bucket, Prefix=location.prefix + "/"):
        keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if keys:
            s3.delete_objects(Bucket=location.bucket, Delete={"Objects": keys, "Quiet": True})


@pytest.fixture(scope="session")
def local_location(tmp_path_factory: pytest.TempPathFactory) -> Iterator[LocalLocation]:
    """Pasta ``serialize-db-poc/<id>/`` sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` ou sob a pasta temporária do pytest, apagada no fim da sessão."""
    configured = os.environ.get("SERIALIZE_DB_TEST_LOCAL_ROOT")
    root = Path(configured).expanduser().resolve() if configured else tmp_path_factory.getbasetemp()
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


def pytest_terminal_summary(terminalreporter: pytest.TerminalReporter) -> None:
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
