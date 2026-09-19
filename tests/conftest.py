"""Configuração compartilhada dos testes.

As provas de conceito da camada Delta rodam sobre os dois tipos de armazenamento que a biblioteca
suporta: uma pasta local (``test_local_proof_of_concept.py``) e um bucket S3
(``test_s3_proof_of_concept.py``). Cada suíte escreve só sob a raiz que o usuário informa na sua
variável de ambiente, e a variável é a autorização: sem ela a suíte é pulada, com o motivo no
relatório da sessão, e ``pytest`` sem variável alguma não executa nenhum teste que grave arquivos.
Com a raiz informada, o que impede a escrita é falha: pasta local inexistente, ou raiz S3 sem
credencial ou sem acesso.

Variáveis de ambiente lidas:

- ``SERIALIZE_DB_TEST_LOCAL_ROOT``: pasta existente sob a qual a suíte local cria
  ``serialize-db-poc/<id>/``.
- ``SERIALIZE_DB_TEST_S3_ROOT``: raiz ``s3://bucket/prefixo`` sob a qual a suíte S3 cria
  ``serialize-db-poc/<id>/``.
- ``SERIALIZE_DB_TEST_KEEP``: qualquer valor mantém os objetos e as pastas criados depois da sessão.
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


def local_root() -> Path | None:
    """Raiz da suíte local, de ``SERIALIZE_DB_TEST_LOCAL_ROOT``, ou ``None`` quando não informada."""
    configured = os.environ.get("SERIALIZE_DB_TEST_LOCAL_ROOT")
    return Path(configured).expanduser().resolve() if configured else None


def s3_root() -> str | None:
    """Raiz da suíte S3, de ``SERIALIZE_DB_TEST_S3_ROOT``, ou ``None`` quando não informada."""
    configured = os.environ.get("SERIALIZE_DB_TEST_S3_ROOT")
    return configured.rstrip("/") if configured else None


SKIP_REASONS = {
    "local": "SERIALIZE_DB_TEST_LOCAL_ROOT não informada: a suíte local só escreve sob a pasta que ela indica",
    "s3": "SERIALIZE_DB_TEST_S3_ROOT não informada: a suíte S3 só escreve sob o prefixo que ela indica",
}


@pytest.hookimpl(trylast=True)
def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    """Pula cada suíte cuja raiz não foi informada; roda depois da seleção por ``-m``."""
    missing = {"local": local_root() is None, "s3": s3_root() is None}
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


def require_s3_access(root: str) -> None:
    """Reprova a sessão quando a raiz informada não está acessível.

    Confere as credenciais do ``boto3`` e lista um objeto sob ``<raiz>/serialize-db-poc/`` com tempos
    curtos: sem rede, o ``boto3`` esperaria 60 s por tentativa, e o delta-rs tem as próprias esperas.
    """
    import boto3
    import botocore.config

    bucket, _, prefix = root.removeprefix("s3://").partition("/")
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
