"""Configuração compartilhada dos testes.

Os testes marcados com ``s3`` rodam contra um bucket real e ficam desativados quando nenhuma raiz
S3 está configurada. A raiz vem, nesta ordem, de ``SERIALIZE_DB_TEST_S3_ROOT`` (``s3://bucket/prefixo``)
ou de ``sagemaker_studio.Project().s3.root`` dentro de um espaço do SageMaker Unified Studio.

Variáveis de ambiente lidas:

- ``SERIALIZE_DB_TEST_S3_ROOT``: raiz sob a qual os testes criam ``serialize-db-poc/<id>/``.
- ``SERIALIZE_DB_TEST_KEEP``: qualquer valor mantém os objetos criados no S3 depois da sessão.
- ``SERIALIZE_DB_TEST_REPORT``: caminho de um arquivo JSON onde o relatório da sessão é gravado.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field

import pytest

# Fatos e medições coletados pelos testes; impressos no fim da sessão e gravados em JSON.
REPORT: dict[str, object] = {}


def record(key: str, value: object) -> None:
    """Registra um fato ou uma medição no relatório da sessão."""
    REPORT[key] = value


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


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    if _s3_root_from_environment():
        return
    skip = pytest.mark.skip(
        reason="sem raiz S3: defina SERIALIZE_DB_TEST_S3_ROOT ou rode dentro de um espaço do SageMaker"
    )
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


@dataclass
class S3Location:
    """Bucket e prefixo exclusivos desta sessão de testes."""

    bucket: str
    prefix: str
    keep: bool = False
    created_keys: set[str] = field(default_factory=set)

    @property
    def uri(self) -> str:
        return f"s3://{self.bucket}/{self.prefix}"

    def child(self, name: str) -> str:
        return f"{self.uri}/{name}"


@pytest.fixture(scope="session")
def s3_location(proxy_environment: dict[str, str | None]) -> Iterator[S3Location]:
    """Prefixo ``serialize-db-poc/<id>/`` sob a raiz configurada, apagado no fim da sessão."""
    import boto3

    root = _s3_root_from_environment()
    assert root, "raiz S3 não configurada"
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
