"""``serialize_db.storage``: os dois armazenamentos pelo ``pyarrow.fs``.

Os testes sem gravar rodam sem variável: a construção por URI sem rede, os caminhos relativos, as
opções do delta-rs resolvidas a cada chamada e sem credencial, o ambiente que o delta-rs lê, e o
proxy, o secret do DuckDB e a recriação dele quando a chave troca, esta com a extensão ``httpfs``
na pasta de extensões. Os que gravam rodam sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador
``local``) e, com ``SERIALIZE_DB_TEST_S3_ROOT``, os mesmos no bucket (marcador ``s3``): a escrita
condicional do arquivo de controle, a listagem, a cópia e a exclusão, e a conexão do DuckDB com a
extensão ``delta`` da pasta configurada.
"""

from __future__ import annotations

import dataclasses
import os
import uuid

import botocore.credentials
import duckdb
import pyarrow.fs as pafs
import pytest

from conftest import duckdb_test_config, require_duckdb_extension
from serialize_db.errors import ConflictError
from serialize_db.storage import (
    Storage,
    _duckdb_secret_options,
    _proxy_settings,
    aws_credentials,
    prepare_environment,
    renew_duckdb_secret,
)

# As variáveis que Storage lê; cada teste sem gravar parte delas limpas.
AWS_VARIABLES = (
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ENDPOINT_URL",
    "AWS_SERVER_SIDE_ENCRYPTION",
    "AWS_SSE_KMS_KEY_ID",
    "AWS_SSE_BUCKET_KEY_ENABLED",
)


@pytest.fixture
def clean_aws(monkeypatch: pytest.MonkeyPatch) -> pytest.MonkeyPatch:
    """O ambiente sem as variáveis da AWS que ``Storage`` lê."""
    for name in AWS_VARIABLES:
        monkeypatch.delenv(name, raising=False)
    return monkeypatch


@pytest.fixture(params=[pytest.param("local", marks=pytest.mark.local),
                        pytest.param("s3", marks=pytest.mark.s3)])
def storage(request: pytest.FixtureRequest) -> Storage:
    """Uma raiz nova por teste, sob a pasta da sessão local ou sob o prefixo da sessão no bucket."""
    location = request.getfixturevalue(f"{request.param}_location")
    return Storage.for_uri(location.child(f"storage/{uuid.uuid4().hex[:8]}"))


def test_storage_for_uri(clean_aws: pytest.MonkeyPatch) -> None:
    """``s3://``, ``file://`` e caminho dão o sistema de arquivos certo e o caminho nele, sem rede;
    o S3 sem região e outro esquema são erro."""
    # O S3, com a região da variável.
    clean_aws.setenv("AWS_REGION", "sa-east-1")
    s3 = Storage.for_uri("s3://bucket/projeto/delta/")
    assert isinstance(s3.filesystem, pafs.S3FileSystem)
    assert s3.filesystem.region == "sa-east-1"
    assert s3.uri == "s3://bucket/projeto/delta"
    assert s3.path == "bucket/projeto/delta"
    assert s3.is_s3

    # O file:// e o caminho relativo, resolvidos para caminhos absolutos.
    local = Storage.for_uri("file:///tmp/serialize-db/delta")
    assert isinstance(local.filesystem, pafs.LocalFileSystem)
    assert not local.is_s3
    assert local.uri == local.path == os.path.realpath("/tmp/serialize-db/delta")
    assert Storage.for_uri("relativa/delta").uri == os.path.realpath("relativa/delta")

    # Outro esquema e o S3 sem região são erro.
    with pytest.raises(ValueError, match="esquema"):
        Storage.for_uri("gs://bucket/delta")
    clean_aws.delenv("AWS_REGION")
    with pytest.raises(ValueError, match="AWS_REGION"):
        Storage.for_uri("s3://bucket/delta")


def test_paths_relative_to_the_root(clean_aws: pytest.MonkeyPatch) -> None:
    """``join`` junta por ``/`` sem barras nas pontas; ``relative`` leva uma URI sob a raiz ao
    caminho relativo e recusa a de fora; ``uri_of`` faz a volta."""
    clean_aws.setenv("AWS_REGION", "sa-east-1")
    storage = Storage.for_uri("s3://bucket/delta")
    joined = storage.join("prd/", "/cad_operacoes", "", "_delta_log")
    assert joined == "prd/cad_operacoes/_delta_log"
    assert storage.relative("s3://bucket/delta/prd/cad_operacoes/") == "prd/cad_operacoes"
    assert storage.relative("s3://bucket/delta") == ""
    assert storage.uri_of("prd/cad_operacoes") == "s3://bucket/delta/prd/cad_operacoes"
    with pytest.raises(ValueError, match="fora da raiz"):
        storage.relative("s3://bucket/delta2/prd")


def test_storage_options_resolved_per_call(clean_aws: pytest.MonkeyPatch) -> None:
    """Cada chamada devolve um dicionário novo, com a região da variável, o retry e as chaves de
    SSE configuradas, e nenhuma credencial; a pasta local não precisa de opção."""
    clean_aws.setenv("AWS_DEFAULT_REGION", "sa-east-1")
    clean_aws.setenv("AWS_ACCESS_KEY_ID", "AKIAEXEMPLO")
    clean_aws.setenv("AWS_SECRET_ACCESS_KEY", "segredo")
    storage = Storage.for_uri("s3://bucket/delta")
    first = storage.storage_options()
    assert first == {"AWS_REGION": "sa-east-1", "max_retries": "3", "retry_timeout": "10s"}

    # A chamada seguinte relê as variáveis.
    clean_aws.setenv("AWS_REGION", "us-west-2")
    clean_aws.setenv("AWS_SERVER_SIDE_ENCRYPTION", "aws:kms")
    second = storage.storage_options()
    assert second is not first
    assert second["AWS_REGION"] == "us-west-2"
    assert second["aws_server_side_encryption"] == "aws:kms"
    credentials = ("access", "secret", "token", "session")
    for key in second:
        for word in credentials:
            assert word not in key.lower(), key
    assert Storage.for_uri("/tmp/delta").storage_options() == {}


def test_prepare_environment() -> None:
    """``NO_PROXY`` sai de ``no_proxy`` quando ausente ou vazia, a região vai nos dois sentidos, e
    a segunda chamada não muda nada."""
    environ = {"no_proxy": "169.254.170.2,localhost", "AWS_REGION": "us-west-2"}
    assert prepare_environment(environ) == {"NO_PROXY": "169.254.170.2,localhost",
                                            "AWS_DEFAULT_REGION": "us-west-2"}
    assert prepare_environment(environ) == {}

    # NO_PROXY vazia, como o shell da extensão do Claude Code no espaço a deixa, conta como ausente.
    environ["NO_PROXY"] = ""
    assert prepare_environment(environ) == {"NO_PROXY": "169.254.170.2,localhost"}
    assert prepare_environment({"AWS_DEFAULT_REGION": "sa-east-1"}) == {"AWS_REGION": "sa-east-1"}


def test_duckdb_proxy_settings_without_credentials_in_the_address() -> None:
    """O endereço do proxy vai sem as credenciais, que o DuckDB recusa embutidas; o usuário e a
    senha vêm de ``username`` e ``password`` ou do próprio endereço, sem URL-encode."""
    assert _proxy_settings({}) == {}
    assert _proxy_settings({"HTTP_PROXY": "http://proxy:3128"}) == {"http_proxy": "proxy:3128"}
    embedded = _proxy_settings({"HTTP_PROXY": "http://ana:p%40ss@proxy:3128"})
    assert embedded == {"http_proxy": "proxy:3128", "http_proxy_username": "ana",
                        "http_proxy_password": "p@ss"}
    separate = _proxy_settings({"HTTP_PROXY": "proxy:3128", "username": "bia", "password": "x"})
    assert separate == {"http_proxy": "proxy:3128", "http_proxy_username": "bia",
                        "http_proxy_password": "x"}


def test_duckdb_secret_options_for_an_endpoint(clean_aws: pytest.MonkeyPatch) -> None:
    """Sem ``AWS_ENDPOINT_URL``, só a região; com ele, o endereço sem o esquema e o endereço por
    caminho, e ``USE_SSL false`` só num endpoint ``http``."""
    clean_aws.setenv("AWS_REGION", "sa-east-1")
    region = ["REGION 'sa-east-1'"]
    assert _duckdb_secret_options() == region

    # O endpoint http de um serviço compatível num IP, como o moto do substituto local.
    clean_aws.setenv("AWS_ENDPOINT_URL", "http://127.0.0.1:5055")
    http = [*region, "ENDPOINT '127.0.0.1:5055'", "URL_STYLE 'path'", "USE_SSL false"]
    assert _duckdb_secret_options() == http

    # O endpoint https, com SSL.
    clean_aws.setenv("AWS_ENDPOINT_URL", "https://minio.exemplo:9000")
    https = [*region, "ENDPOINT 'minio.exemplo:9000'", "URL_STYLE 'path'"]
    assert _duckdb_secret_options() == https


@dataclasses.dataclass
class RecordingConnection:
    """Uma conexão do DuckDB que guarda o texto de cada comando que passa por ela."""

    connection: duckdb.DuckDBPyConnection
    texts: list[str] = dataclasses.field(default_factory=list)

    def execute(self, text: str,
                parameters: list[object] | None = None) -> duckdb.DuckDBPyConnection:
        self.texts.append(text)
        return self.connection.execute(text, parameters)


def stored_secret(connection: duckdb.DuckDBPyConnection) -> str:
    """O ``secret_string`` do secret do S3, que mostra a chave e mascara o segredo e o token."""
    rows = connection.execute(
        "SELECT secret_string FROM duckdb_secrets() WHERE name = 'serialize_db_s3'").fetchall()
    assert len(rows) == 1
    return rows[0][0]


def test_renew_duckdb_secret_follows_the_key(clean_aws: pytest.MonkeyPatch) -> None:
    """O secret é criado sem um anterior, fica com a mesma chave e é recriado quando a chave da
    credencial troca, com a região do ambiente e sem mostrar o segredo nem o token; a chave, o
    segredo e o token ficam fora do texto dos comandos."""
    require_duckdb_extension("httpfs")
    clean_aws.setenv("AWS_REGION", "sa-east-1")
    credentials = botocore.credentials.Credentials("AKIAPRIMEIRA", "segredo-um", "token-um")
    with duckdb.connect(config=duckdb_test_config()) as connection:
        connection.execute("LOAD httpfs")
        recorder = RecordingConnection(connection)
        created = renew_duckdb_secret(recorder, credentials)
        kept = renew_duckdb_secret(recorder, credentials)
        first = stored_secret(connection)

        # A chave que a credencial passa a dar, como a do contêiner depois da troca.
        credentials.access_key = "AKIASEGUNDA"
        credentials.secret_key = "segredo-dois"
        renewed = renew_duckdb_secret(recorder, credentials)
        second = stored_secret(connection)
    assert created is True
    assert kept is False
    assert ";key_id=AKIAPRIMEIRA;" in first
    assert renewed is True
    assert ";key_id=AKIASEGUNDA;" in second
    assert ";region=sa-east-1;" in second
    assert "segredo-dois" not in second
    assert "token-um" not in second
    commands = " ".join(recorder.texts)
    assert "CREATE OR REPLACE SECRET" in commands
    for value in ("AKIAPRIMEIRA", "segredo-um", "token-um", "AKIASEGUNDA", "segredo-dois"):
        assert value not in commands


def test_create_text_and_write_text_if_match(storage: Storage) -> None:
    """A segunda ``create_text`` e o ``if_match`` velho são ``ConflictError`` sem gravar; o
    conteúdo final é o da escrita que venceu, e o arquivo ausente é ``FileNotFoundError``."""
    path = storage.join("prd", "_serialize_db", "snapshots.json")
    with pytest.raises(FileNotFoundError):
        storage.read_text(path)

    # A criação condicional.
    first = storage.create_text(path, '{"snapshots": {}}')
    with pytest.raises(ConflictError):
        storage.create_text(path, "{}")
    text, fingerprint = storage.read_text(path)
    assert (text, fingerprint) == ('{"snapshots": {}}', first)

    # A escrita condicional pela impressão digital lida.
    second = storage.write_text(path, '{"snapshots": {"2026T3": {}}}', if_match=fingerprint)
    with pytest.raises(ConflictError):
        storage.write_text(path, "{}", if_match=fingerprint)
    assert storage.read_text(path) == ('{"snapshots": {"2026T3": {}}}', second)


def test_list_copy_delete(storage: Storage) -> None:
    """``list_files`` desce as pastas, filtra pelo sufixo e exclui ``_delta_log/``; ``copy``
    preserva os bytes, também acima do limiar multipart do ``boto3``; ``delete`` de um caminho
    ausente não falha."""
    for name in ("t/p=a/1.parquet", "t/p=b/2.parquet", "t/_delta_log/00.checkpoint.parquet",
                 "t/_delta_log/00000000000000000000.json"):
        storage.write_text(name, name)
    assert storage.list_files("t", ".parquet") == ["t/p=a/1.parquet", "t/p=b/2.parquet"]
    assert storage.list_files("ausente") == []

    # A cópia de um arquivo pequeno, e o tamanho do ausente.
    storage.copy("t/p=a/1.parquet", "copia/p=a/1.parquet")
    assert storage.read_text("copia/p=a/1.parquet")[0] == "t/p=a/1.parquet"
    assert storage.size("copia/p=a/1.parquet") == len("t/p=a/1.parquet")
    assert storage.exists("copia/p=a/1.parquet")
    assert storage.size("ausente.parquet") is None

    # Acima do limiar multipart do boto3, 8 MiB, a cópia no S3 é um UploadPartCopy em duas partes.
    large = 9 * 1024 * 1024
    storage.write_text("t/p=c/grande.parquet", "x" * large)
    storage.copy("t/p=c/grande.parquet", "copia/p=c/grande.parquet")
    assert storage.size("copia/p=c/grande.parquet") == large

    # A exclusão, com um caminho ausente.
    storage.delete(["copia/p=a/1.parquet", "copia/ausente.parquet"])
    assert not storage.exists("copia/p=a/1.parquet")


def test_duckdb_connect_loads_delta(storage: Storage) -> None:
    """A conexão sai com a extensão ``delta`` da pasta configurada, sem instalação automática; no
    S3, com o secret na chave que a cadeia do ``boto3`` resolve."""
    with storage.duckdb_connect() as connection:
        loaded = connection.execute(
            "SELECT extension_name FROM duckdb_extensions() WHERE loaded ORDER BY 1").fetchall()
        autoinstall = connection.execute(
            "SELECT current_setting('autoinstall_known_extensions')").fetchone()[0]
        secrets = connection.execute("SELECT name FROM duckdb_secrets()").fetchall()
        secret = stored_secret(connection) if storage.is_s3 else None
    assert ("delta",) in loaded
    assert autoinstall is False
    assert secrets == ([("serialize_db_s3",)] if storage.is_s3 else [])
    if secret is not None:
        key = aws_credentials().get_frozen_credentials().access_key
        assert f";key_id={key};" in secret
