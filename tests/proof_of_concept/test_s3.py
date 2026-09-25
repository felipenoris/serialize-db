"""Prova de conceito da camada Delta no S3, executável em qualquer ambiente com um bucket.

Cada teste responde a um item da etapa 0 (``plan/PLAN-STAGE-0.md``): as credenciais que o delta-rs
encontra, a escrita e a leitura no bucket, o put condicional, o ``vacuum`` e o tempo do
``delta_scan``. Os testes comuns aos dois armazenamentos vêm de ``poc_delta.py``; os deste módulo
cobrem o que só existe no S3: a origem das credenciais, a cadeia de credenciais do delta-rs e a
forma da reserva que a biblioteca não usa, o put condicional, a criptografia dos arquivos, o cache
de arquivos externos do DuckDB sobre o que ele leu do bucket e as chamadas do ``boto3`` que a
biblioteca usa (listar, copiar, apagar). As medições vão para o
relatório impresso no fim da sessão (``conftest.py``). A suíte escreve só sob a raiz informada em
``SERIALIZE_DB_TEST_S3_ROOT``: sem ela é pulada, e com ela falta de credencial ou de acesso ao
bucket é falha.

As extensões ``httpfs`` e ``delta`` do DuckDB vêm da pasta de extensões (``.duckdb/`` do
repositório, preparada por ``prepare_offline.sh``, ou a pasta padrão do DuckDB), e nada é baixado
sem pedido: sem uma delas os testes que a usam são pulados, com ou sem internet, e com
``SERIALIZE_DB_DUCKDB_EXTENSIONS`` a suíte a instala nessa pasta.
"""

from __future__ import annotations

import os
import re
import subprocess
import sys

import boto3
import botocore.exceptions
import duckdb
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from conftest import S3Location, create_duckdb_s3_secret, record
from poc_delta import DeltaProofOfConcept, connect_duckdb, write_sample_table
from probelib import hide_credentials

pytestmark = pytest.mark.s3

# Os códigos de cor ANSI e a linha que começa pelo nome de uma exceção, como ``OSError: ...``.
ANSI_COLOR = re.compile(r"\x1b\[[0-9;]*m")
EXCEPTION_LINE = re.compile(r"^[A-Za-z_][\w.]*(Error|Exception)\b")


def environment_with(changes: dict[str, str | None]) -> dict[str, str]:
    """O ambiente do processo com as mudanças aplicadas: ``None`` remove a variável."""
    environment = dict(os.environ)
    for key, value in changes.items():
        if value is None:
            environment.pop(key, None)
        else:
            environment[key] = value
    return environment


def last_error(stderr: str) -> str:
    """A exceção final de um subprocesso numa linha: da última linha ``Tipo: mensagem`` até o fim,
    sem códigos de cor."""
    lines = ANSI_COLOR.sub("", stderr).strip().splitlines()
    if not lines:
        return "(sem saída de erro)"

    # A última linha que começa pelo nome de uma exceção; sem nenhuma, a última linha.
    start = len(lines) - 1
    for index, line in enumerate(lines):
        if EXCEPTION_LINE.match(line):
            start = index

    # Da linha escolhida até o fim, cada linha com os espaços reduzidos a um.
    tail = []
    for line in lines[start:]:
        tail.append(" ".join(line.split()))
    return " ".join(tail)


@pytest.fixture(scope="session")
def storage(s3_location: S3Location) -> S3Location:
    """Raiz da sessão no bucket."""
    return s3_location


@pytest.fixture(scope="session")
def table_uri(storage: S3Location) -> str:
    """Tabela ``operacoes`` gravada pela cadeia de credenciais padrão."""
    return write_sample_table(storage)


@pytest.fixture(scope="session")
def duckdb_connection(storage: S3Location) -> duckdb.DuckDBPyConnection:
    """Conexão com ``httpfs`` e ``delta`` carregadas e um secret S3 com a chave da credencial
    do ``boto3``."""
    # storage roda antes do secret: pelo s3_location, proxy_environment exporta AWS_REGION e
    # NO_PROXY, e require_s3_access confere o acesso à raiz.
    connection = connect_duckdb(("httpfs", "delta"))

    # O secret leva a chave que a cadeia do boto3 resolve agora (ambiente, contêiner, perfil,
    # IMDS), com as opções de Storage.duckdb_setup: a região e o endpoint de AWS_ENDPOINT_URL.
    create_duckdb_s3_secret(connection)

    return connection


class TestS3ProofOfConcept(DeltaProofOfConcept):
    """Os testes comuns sobre o bucket mais os próprios do S3."""

    # storage roda antes do boto3: pelo s3_location, proxy_environment exporta AWS_REGION e
    # NO_PROXY, e require_s3_access confere o acesso à raiz.
    @pytest.mark.usefixtures("storage")
    def test_boto3_credential_source(self) -> None:
        """Registra de onde o ``boto3`` obtém as credenciais e qual identidade assume."""
        session = boto3.Session()

        # get_credentials percorre a cadeia (variáveis, contêiner, perfil, IMDS) e diz qual método
        # respondeu.
        credentials = session.get_credentials()
        assert credentials is not None, "boto3 não encontrou credenciais"

        # O que se lê sem rede vai ao relatório antes do STS, que pode não responder.
        record("credentials.boto3_method", credentials.method)
        record(
            "credentials.container_relative_uri",
            bool(os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")),
        )
        record("environment.lowercase_no_proxy", bool(os.environ.get("no_proxy")))

        # O endereço do proxy entra no relatório sem o usuário e a senha embutidos: o relatório é
        # impresso e colado na conversa.
        proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
        record("environment.https_proxy", hide_credentials(proxy) if proxy else proxy)

        # O STS devolve o ARN do papel assumido; num ambiente só com endpoint VPC do S3 esta chamada
        # não responde.
        identity = session.client("sts").get_caller_identity()
        record("credentials.identity_arn", identity["Arn"])

    def test_delta_rs_credential_chain(
        self, storage: S3Location, proxy_environment: dict[str, str | None]
    ) -> None:
        """Quais variantes do ambiente deixam o delta-rs abrir a tabela pela cadeia padrão.

        Cada variante roda num subprocesso com o ambiente alterado; o relatório mostra o resultado
        de cada uma. Ao menos a variante com ``NO_PROXY`` exportado (a que a biblioteca usa) precisa
        passar. No espaço do SageMaker, ``no_proxy_absent`` passa e ``no_proxy_empty`` falha com
        403: o cliente HTTP do delta-rs lê ``NO_PROXY`` e, só quando ela está ausente, ``no_proxy``.
        """
        uri = storage.child("credential_probe")
        write_deltalake(uri, pa.table({"id": pa.array([1], pa.int64())}), mode="overwrite")

        # O programa do subprocesso só abre a tabela; o que muda entre as variantes é o ambiente.
        # ``as_found`` devolve NO_PROXY ao valor que a sessão encontrou (ausente, vazia ou
        # definida).
        probe = f"from deltalake import DeltaTable; print(DeltaTable({uri!r}).version())"
        variants: dict[str, dict[str, str | None]] = {
            "as_found": {"NO_PROXY": proxy_environment["NO_PROXY"]},
            "no_proxy_exported": {},
            "no_proxy_absent": {"NO_PROXY": None},
            "no_proxy_empty": {"NO_PROXY": ""},
            "proxy_unset": {
                name: None for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy")
            },
        }

        results: dict[str, str] = {}
        for name, changes in variants.items():
            completed = subprocess.run(
                [sys.executable, "-c", probe],
                env=environment_with(changes),
                capture_output=True,
                text=True,
                timeout=120,
            )
            results[name] = (
                "ok" if completed.returncode == 0 else last_error(completed.stderr)[:160]
            )
            record(f"credentials.delta_rs.{name}", results[name])

        assert results["no_proxy_exported"] == "ok", results

    def test_delta_rs_storage_options_fallback(self, storage: S3Location) -> None:
        """As credenciais temporárias do ``boto3`` em ``storage_options`` abrem a tabela sem a
        cadeia padrão.

        A biblioteca não as usa: ``storage_options`` nunca leva credencial, e o teste mede a forma
        para o dia em que um ambiente quebrar a cadeia.
        """
        # get_frozen_credentials fixa o trio chave, segredo e token no instante da chamada.
        frozen = boto3.Session().get_credentials().get_frozen_credentials()
        options = {
            "AWS_ACCESS_KEY_ID": frozen.access_key,
            "AWS_SECRET_ACCESS_KEY": frozen.secret_key,
            "AWS_REGION": os.environ["AWS_REGION"],
        }
        if frozen.token:
            options["AWS_SESSION_TOKEN"] = frozen.token

        # storage_options vai para o cliente de objetos do delta-rs, sem passar pela cadeia padrão.
        uri = storage.child("storage_options_probe")
        write_deltalake(
            uri,
            pa.table({"id": pa.array([1], pa.int64())}),
            mode="overwrite",
            storage_options=options,
        )
        assert DeltaTable(uri, storage_options=options).version() == 0

    def test_conditional_put(self, storage: S3Location) -> None:
        """``If-None-Match`` e ``If-Match`` no bucket: a primitiva do commit do Delta."""
        s3 = boto3.client("s3")
        key = f"{storage.prefix}/conditional.txt"

        # IfNoneMatch='*' cria o objeto só se ele não existe; a repetição devolve 412
        # PreconditionFailed.
        first = s3.put_object(Bucket=storage.bucket, Key=key, Body=b"v1", IfNoneMatch="*")
        with pytest.raises(botocore.exceptions.ClientError) as duplicate:
            s3.put_object(Bucket=storage.bucket, Key=key, Body=b"v2", IfNoneMatch="*")
        assert duplicate.value.response["Error"]["Code"] == "PreconditionFailed"

        # IfMatch=<etag> substitui só se a versão atual é a esperada; um ETag velho devolve o mesmo
        # 412.
        s3.put_object(Bucket=storage.bucket, Key=key, Body=b"v3", IfMatch=first["ETag"])
        with pytest.raises(botocore.exceptions.ClientError) as stale:
            s3.put_object(Bucket=storage.bucket, Key=key, Body=b"v4", IfMatch=first["ETag"])
        assert stale.value.response["Error"]["Code"] == "PreconditionFailed"

        # head_object lê os metadados sem baixar o corpo: criptografia, chave KMS e versionamento.
        head = s3.head_object(Bucket=storage.bucket, Key=key)
        storage.record("server_side_encryption", head.get("ServerSideEncryption"))
        storage.record("sse_kms_key_id", head.get("SSEKMSKeyId"))
        storage.record("bucket_key_enabled", head.get("BucketKeyEnabled"))
        storage.record("versioned", "VersionId" in head)

    def test_data_file_encryption(self, storage: S3Location, table_uri: str) -> None:
        """Os arquivos do delta-rs recebem a criptografia padrão do bucket sem opção alguma: a mesma
        de um objeto que o ``boto3`` grava sem opção."""
        s3 = boto3.client("s3")
        key = DeltaTable(table_uri).file_uris()[0].removeprefix(f"s3://{storage.bucket}/")
        data_file = s3.head_object(Bucket=storage.bucket, Key=key)

        # O objeto de referência leva a criptografia padrão do bucket, qualquer que ela seja.
        reference_key = f"{storage.prefix}/criptografia_padrao.txt"
        s3.put_object(Bucket=storage.bucket, Key=reference_key, Body=b"referencia")
        reference = s3.head_object(Bucket=storage.bucket, Key=reference_key)

        record("delta.data_file_encryption", data_file.get("ServerSideEncryption"))
        record("s3.default_encryption", reference.get("ServerSideEncryption"))
        assert data_file.get("ServerSideEncryption") == reference.get("ServerSideEncryption")
        assert data_file.get("SSEKMSKeyId") == reference.get("SSEKMSKeyId")

    def test_external_file_cache_serves_the_second_read(
        self, storage: S3Location, table_uri: str
    ) -> None:
        """O cache de arquivos externos do DuckDB, ligado por padrão, guarda os blocos que o
        ``delta_scan`` leu do S3, e a segunda leitura na mesma instância não acrescenta nada a ele.

        No moto, a primeira leitura de um arquivo fez 3 ``GET`` dele, a segunda nenhum, e a leitura
        com o cache desligado de novo 3 (sonda de 2026-09-24): um melhor de N no mesmo processo mede
        o cache, e ``probes/duckdb_threads.py`` o desliga em cada configuração.
        """
        # Uma instância nova, para o cache começar vazio; o de duckdb_connection é da sessão.
        connection = connect_duckdb(("httpfs", "delta"))
        create_duckdb_s3_secret(connection)
        cache = "SELECT count(*), coalesce(sum(nr_bytes), 0) FROM duckdb_external_file_cache()"
        read = f"SELECT count(*), max(valor) FROM delta_scan('{table_uri}')"
        try:
            enabled = connection.execute(
                "SELECT current_setting('enable_external_file_cache')").fetchone()[0]
            connection.execute(read).fetchall()
            after_first = connection.execute(cache).fetchone()
            connection.execute(read).fetchall()
            after_second = connection.execute(cache).fetchone()
        finally:
            connection.close()
        record("duckdb.external_file_cache", {
            "padrao": enabled, "primeira_leitura": after_first, "segunda_leitura": after_second,
        })
        assert enabled is True
        assert after_first[1] > 0, after_first
        assert after_second == after_first, (after_first, after_second)

    def test_boto3_list_copy_delete(self, storage: S3Location, table_uri: str) -> None:
        """Listar, copiar e apagar objetos: o que ``export_snapshot(mode="copy")`` e a limpeza fazem
        no S3."""
        s3 = boto3.client("s3")
        source_prefix = table_uri.removeprefix(f"s3://{storage.bucket}/")
        target_prefix = f"{storage.prefix}/exported"

        # O paginador entrega as chaves em páginas de até 1.000; o filtro deixa só os arquivos de
        # dados.
        pages = s3.get_paginator("list_objects_v2").paginate(
            Bucket=storage.bucket, Prefix=source_prefix + "/"
        )
        contents = []
        for page in pages:
            contents.extend(page.get("Contents", []))
        data_keys = [item["Key"] for item in contents if item["Key"].endswith(".parquet")]

        # A listagem confere com os arquivos que o log do Delta referencia, uma fonte independente
        # do paginador: nenhum vacuum nem overwrite deixou arquivo fora do snapshot.
        referenced = []
        for uri in DeltaTable(table_uri).file_uris():
            referenced.append(uri.removeprefix(f"s3://{storage.bucket}/"))
        assert sorted(data_keys) == sorted(referenced)

        # copy_object copia dentro do serviço, sem baixar os dados; o layout mes=.../ é preservado.
        for key in data_keys:
            relative = key.removeprefix(source_prefix + "/")
            s3.copy_object(
                Bucket=storage.bucket,
                Key=f"{target_prefix}/{relative}",
                CopySource={"Bucket": storage.bucket, "Key": key},
            )

        copied = s3.list_objects_v2(Bucket=storage.bucket, Prefix=target_prefix + "/")
        copied_keys = [{"Key": item["Key"]} for item in copied.get("Contents", [])]
        assert len(copied_keys) == len(data_keys)

        # delete_objects apaga até 1.000 chaves por chamada; Quiet omite as chaves apagadas da
        # resposta.
        s3.delete_objects(
            Bucket=storage.bucket, Delete={"Objects": copied_keys, "Quiet": True}
        )
        remaining = s3.list_objects_v2(Bucket=storage.bucket, Prefix=target_prefix + "/")
        assert "Contents" not in remaining
