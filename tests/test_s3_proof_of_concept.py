"""Prova de conceito da camada Delta no S3, executável em qualquer ambiente com um bucket.

Cada teste responde a um item da etapa 0 em ``docs/estrategia.md``: as credenciais que o delta-rs
encontra, a escrita e a leitura no bucket, o put condicional, o ``vacuum`` e o tempo do ``delta_scan``.
As medições vão para o relatório impresso no fim da sessão (``conftest.py``).

Num ambiente sem internet, as extensões ``httpfs``, ``delta`` e ``aws`` do DuckDB precisam estar na
pasta de extensões (``.duckdb/`` do repositório, preparada por ``tests/prepare_offline.sh``, ou a
pasta padrão do DuckDB); sem isso os testes que as usam são pulados.
"""

from __future__ import annotations

import datetime as dt
import decimal
import os
import re
import subprocess
import sys
import time
from collections.abc import Callable

import boto3
import botocore.exceptions
import duckdb
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from conftest import REPORT, S3Location, duckdb_extension_directory, record

pytestmark = pytest.mark.s3

ROWS = 300_000
APPENDED_ROWS = 10
MONTHS = ("2026-01", "2026-02")


def timed(label: str, action: Callable[[], object], repeat: int = 1) -> object:
    """Executa ``action`` ``repeat`` vezes e registra o tempo total no relatório."""
    started = time.perf_counter()
    result = None
    for _ in range(repeat):
        result = action()
    record(f"timing.{label}", f"{time.perf_counter() - started:.3f} s")
    return result


@pytest.fixture(scope="session")
def sample_table() -> pa.Table:
    """Amostra de ``operacoes`` com os tipos do contrato: inteiros, decimal, timestamp e texto."""
    half = ROWS // 2
    start = dt.datetime(2026, 1, 1)
    return pa.table(
        {
            "id_operacao": pa.array(range(ROWS), pa.int64()),
            "mes": pa.array([MONTHS[0]] * half + [MONTHS[1]] * (ROWS - half), pa.string()),
            "data_ref": pa.array([start + dt.timedelta(minutes=i) for i in range(ROWS)], pa.timestamp("us")),
            "id_cliente": pa.array([i % 1000 for i in range(ROWS)], pa.int32()),
            "valor": pa.array([decimal.Decimal(i) / 100 for i in range(ROWS)], pa.decimal128(18, 2)),
            "descricao": pa.array([f"operacao {i}" for i in range(ROWS)], pa.string()),
        }
    )


@pytest.fixture(scope="session")
def table_uri(s3_location: S3Location, sample_table: pa.Table) -> str:
    """Tabela Delta gravada pela cadeia de credenciais padrão: ``overwrite`` particionado e um ``append``."""
    uri = s3_location.child("operacoes")
    timed("write_deltalake.overwrite", lambda: write_deltalake(uri, sample_table, mode="overwrite", partition_by=["mes"]))
    timed("write_deltalake.append", lambda: write_deltalake(uri, sample_table.slice(0, APPENDED_ROWS), mode="append"))
    return uri


@pytest.fixture(scope="session")
def duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Conexão com ``httpfs`` e ``delta`` carregadas e um secret S3 pela cadeia de credenciais."""
    directory = duckdb_extension_directory()
    connection = duckdb.connect(config={"extension_directory": directory} if directory else {})
    record("duckdb.extension_directory", directory or "(padrão)")
    for extension in ("httpfs", "delta", "aws"):
        try:
            connection.execute(f"LOAD {extension}")
        except duckdb.Error:
            try:
                connection.execute(f"INSTALL {extension}; LOAD {extension}")
            except duckdb.Error as error:
                pytest.skip(f"extensão {extension} do DuckDB indisponível sem internet: {error}")
    region = os.environ.get("AWS_REGION", "")
    connection.execute(f"CREATE SECRET poc (TYPE s3, PROVIDER credential_chain, REGION '{region}')")
    record("duckdb.version", duckdb.__version__)
    record("duckdb.threads", connection.execute("SELECT current_setting('threads')").fetchone()[0])
    return connection


def test_boto3_credential_source() -> None:
    """Registra de onde o ``boto3`` obtém as credenciais e qual identidade assume."""
    session = boto3.Session()
    credentials = session.get_credentials()
    assert credentials is not None, "boto3 não encontrou credenciais"
    identity = session.client("sts").get_caller_identity()
    record("credentials.boto3_method", credentials.method)
    record("credentials.identity_arn", identity["Arn"])
    record("credentials.container_relative_uri", bool(os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")))
    record("environment.lowercase_no_proxy", bool(os.environ.get("no_proxy")))
    record("environment.https_proxy", os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy"))


def test_delta_rs_credential_chain(s3_location: S3Location) -> None:
    """Quais variantes do ambiente deixam o delta-rs abrir a tabela pela cadeia padrão.

    Cada variante roda num subprocesso com o ambiente alterado; o relatório mostra o resultado de
    cada uma. Ao menos a variante com ``NO_PROXY`` exportado (a que a biblioteca usa) precisa passar.
    """
    uri = s3_location.child("credential_probe")
    write_deltalake(uri, pa.table({"id": pa.array([1], pa.int64())}), mode="overwrite")
    probe = f"from deltalake import DeltaTable; print(DeltaTable({uri!r}).version())"
    variants: dict[str, dict[str, str | None]] = {
        "as_found": {"NO_PROXY": None} if os.environ.get("no_proxy") else {},
        "no_proxy_exported": {},
        "proxy_unset": {name: None for name in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy")},
    }
    results: dict[str, str] = {}
    for name, changes in variants.items():
        environment = dict(os.environ)
        for key, value in changes.items():
            if value is None:
                environment.pop(key, None)
            else:
                environment[key] = value
        completed = subprocess.run([sys.executable, "-c", probe], env=environment, capture_output=True, text=True, timeout=120)
        results[name] = "ok" if completed.returncode == 0 else completed.stderr.strip().splitlines()[-1][:160]
        record(f"credentials.delta_rs.{name}", results[name])
    assert results["no_proxy_exported"] == "ok", results


def test_delta_rs_storage_options_fallback(s3_location: S3Location) -> None:
    """As credenciais temporárias do ``boto3`` em ``storage_options`` são a reserva da biblioteca."""
    frozen = boto3.Session().get_credentials().get_frozen_credentials()
    options = {
        "AWS_ACCESS_KEY_ID": frozen.access_key,
        "AWS_SECRET_ACCESS_KEY": frozen.secret_key,
        "AWS_REGION": os.environ["AWS_REGION"],
    }
    if frozen.token:
        options["AWS_SESSION_TOKEN"] = frozen.token
    uri = s3_location.child("storage_options_probe")
    write_deltalake(uri, pa.table({"id": pa.array([1], pa.int64())}), mode="overwrite", storage_options=options)
    assert DeltaTable(uri, storage_options=options).version() == 0


def test_conditional_put(s3_location: S3Location) -> None:
    """``If-None-Match`` e ``If-Match`` no bucket: a primitiva do commit do Delta."""
    s3 = boto3.client("s3")
    key = f"{s3_location.prefix}/conditional.txt"
    first = s3.put_object(Bucket=s3_location.bucket, Key=key, Body=b"v1", IfNoneMatch="*")
    with pytest.raises(botocore.exceptions.ClientError) as duplicate:
        s3.put_object(Bucket=s3_location.bucket, Key=key, Body=b"v2", IfNoneMatch="*")
    assert duplicate.value.response["Error"]["Code"] == "PreconditionFailed"
    s3.put_object(Bucket=s3_location.bucket, Key=key, Body=b"v3", IfMatch=first["ETag"])
    with pytest.raises(botocore.exceptions.ClientError) as stale:
        s3.put_object(Bucket=s3_location.bucket, Key=key, Body=b"v4", IfMatch=first["ETag"])
    assert stale.value.response["Error"]["Code"] == "PreconditionFailed"
    head = s3.head_object(Bucket=s3_location.bucket, Key=key)
    record("s3.server_side_encryption", head.get("ServerSideEncryption"))
    record("s3.sse_kms_key_id", head.get("SSEKMSKeyId"))
    record("s3.bucket_key_enabled", head.get("BucketKeyEnabled"))
    record("s3.versioned", "VersionId" in head)


def test_write_and_open(table_uri: str) -> None:
    """A tabela gravada tem os dois commits, um arquivo por mês mais o do ``append`` e o protocolo esperado."""
    table = DeltaTable(table_uri)
    assert table.version() == 1
    assert len(table.file_uris()) == len(MONTHS) + 1
    protocol = table.protocol()
    assert protocol.min_reader_version == 3 and protocol.min_writer_version == 7
    assert "timestampNtz" in (protocol.writer_features or [])
    history = table.history(2)
    assert [entry["operation"] for entry in history] == ["WRITE", "WRITE"]
    record("delta.client_version", history[0].get("clientVersion"))
    rows = timed("delta_rs.to_pyarrow_table", lambda: table.to_pyarrow_table().num_rows)
    assert rows == ROWS + APPENDED_ROWS


def test_data_file_encryption(table_uri: str, s3_location: S3Location) -> None:
    """Os arquivos do delta-rs recebem a criptografia padrão do bucket sem opção alguma."""
    key = DeltaTable(table_uri).file_uris()[0].removeprefix(f"s3://{s3_location.bucket}/")
    head = boto3.client("s3").head_object(Bucket=s3_location.bucket, Key=key)
    record("delta.data_file_encryption", head.get("ServerSideEncryption"))
    assert head.get("ServerSideEncryption") in (None, "AES256", "aws:kms", "aws:kms:dsse")


def test_delta_scan_reads_types(duckdb_connection: duckdb.DuckDBPyConnection, table_uri: str) -> None:
    """``delta_scan`` com secret ``credential_chain`` lê a tabela com os tipos do contrato."""
    described = duckdb_connection.execute(f"DESCRIBE SELECT * FROM delta_scan('{table_uri}')").fetchall()
    types = {name: kind for name, kind, *_ in described}
    assert types == {
        "id_operacao": "BIGINT",
        "mes": "VARCHAR",
        "data_ref": "TIMESTAMP",
        "id_cliente": "INTEGER",
        "valor": "DECIMAL(18,2)",
        "descricao": "VARCHAR",
    }
    query = f"SELECT count(*), sum(valor) FROM delta_scan('{table_uri}')"
    count, total = timed("delta_scan.aggregate_first", lambda: duckdb_connection.execute(query).fetchone())
    timed("delta_scan.aggregate_again", lambda: duckdb_connection.execute(query).fetchone())
    assert count == ROWS + APPENDED_ROWS
    expected = sum(decimal.Decimal(i) / 100 for i in range(ROWS)) + sum(decimal.Decimal(i) / 100 for i in range(APPENDED_ROWS))
    assert total == expected


def test_delta_scan_prunes_partitions(duckdb_connection: duckdb.DuckDBPyConnection, table_uri: str) -> None:
    """O filtro por ``mes`` lê só os arquivos da partição."""
    query = f"SELECT count(*) FROM delta_scan('{table_uri}') WHERE mes = '{MONTHS[1]}'"
    count = timed("delta_scan.aggregate_one_month", lambda: duckdb_connection.execute(query).fetchone()[0])
    assert count == ROWS - ROWS // 2
    plan = duckdb_connection.execute(f"EXPLAIN ANALYZE {query}").fetchone()[1]
    scanned = re.search(r"Scanning Files: (\d+)/(\d+)", plan)
    assert scanned, "o plano não informa os arquivos lidos"
    record("delta_scan.files_scanned_one_month", f"{scanned.group(1)}/{scanned.group(2)}")
    assert int(scanned.group(1)) == 1


def test_scan_timings(duckdb_connection: duckdb.DuckDBPyConnection, table_uri: str) -> None:
    """Consultas pontuais: ``delta_scan``, ``ATTACH`` fixado, ``read_parquet`` e tabela materializada.

    A regra de ingestão da biblioteca (materializar o que é consultado mais de uma vez) depende de a
    tabela materializada ser mais rápida que o ``delta_scan`` repetido.
    """
    con = duckdb_connection
    repeat = 20
    point = f"WHERE id_operacao = {ROWS // 2}"
    con.execute(f"CREATE VIEW scan AS SELECT * FROM delta_scan('{table_uri}')")
    con.execute(f"ATTACH '{table_uri}' AS pinned (TYPE delta, PIN_SNAPSHOT true)")
    parquet = f"read_parquet('{table_uri}/*/*.parquet', hive_partitioning = true)"

    def run(sql: str) -> Callable[[], object]:
        return lambda: con.execute(sql).fetchall()

    scan = timed("point_queries.delta_scan", run(f"SELECT * FROM scan {point}"), repeat)
    timed("point_queries.delta_scan_with_month", run(f"SELECT * FROM scan {point} AND mes = '{MONTHS[1]}'"), repeat)
    timed("point_queries.attach_pinned", run(f"SELECT * FROM pinned {point}"), repeat)
    timed("point_queries.read_parquet", run(f"SELECT * FROM {parquet} {point}"), repeat)
    timed("aggregate.read_parquet", run(f"SELECT count(*), sum(valor) FROM {parquet}"))
    timed("materialize.create_table_as", run("CREATE TABLE materialized AS SELECT * FROM scan"))
    timed("aggregate.materialized", run("SELECT count(*), sum(valor) FROM materialized"))
    materialized = timed("point_queries.materialized", run(f"SELECT * FROM materialized {point}"), repeat)
    assert len(scan) == 1 and scan == materialized

    def seconds(label: str) -> float:
        return float(str(REPORT[f"timing.{label}"]).split()[0])

    assert seconds("point_queries.materialized") < seconds("point_queries.delta_scan")


def test_vacuum_deletes_files(s3_location: S3Location, sample_table: pa.Table) -> None:
    """``vacuum(dry_run=False)`` exerce ``DeleteObject`` e remove os arquivos substituídos."""
    uri = s3_location.child("vacuum_probe")
    small = sample_table.slice(0, 1000)
    write_deltalake(uri, small, mode="overwrite", partition_by=["mes"])
    write_deltalake(uri, small, mode="overwrite", partition_by=["mes"])
    table = DeltaTable(uri)
    removed = timed("vacuum.delete", lambda: table.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False))
    assert len(removed) == 1
    bucket, _, prefix = uri.removeprefix("s3://").partition("/")
    listed = boto3.client("s3").list_objects_v2(Bucket=bucket, Prefix=prefix + "/")
    data_files = [item["Key"] for item in listed.get("Contents", []) if item["Key"].endswith(".parquet")]
    assert len(data_files) == 1
