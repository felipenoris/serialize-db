"""Prova de conceito no Redshift: os itens da etapa 0 de ``docs/PLAN.md`` que esperam uma conexão.

A suíte cria tabelas ``serialize_db_poc_<id>_*`` no esquema de ``SERIALIZE_DB_TEST_REDSHIFT_SCHEMA``
e arquivos sob ``SERIALIZE_DB_TEST_S3_ROOT``; sem uma das duas é pulada, e com elas a falta de
conexão é falha. Os testes exercitam o ``redshift_connector`` (sessão, ``paramstyle`` nomeado), o
DDL compilado pelo SQLAlchemy, o ``COPY ... MANIFEST`` de arquivos gravados pelo delta-rs (o
``DECIMAL`` em ``INT64``, o ``timestamp_ntz``, a lista de colunas e o ``FILLRECORD``), o ``VARCHAR``
excedido, o ``SUPER`` e o ``UNLOAD ... PARTITION BY`` registrado no Delta e lido pelo DuckDB. Os
resultados que a documentação não fixa vão para o relatório da sessão em vez de virarem asserções.

A suíte foi escrita antes de o projeto ter uma conexão Redshift e ainda não rodou contra um cluster.
"""

from __future__ import annotations

import datetime as dt
import decimal
import io
import json
import os
import time

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa
from deltalake import DeltaTable, write_deltalake
from deltalake.transaction import AddAction
from sqlalchemy.schema import CreateTable
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

from conftest import RedshiftSession, S3Location, record
from delta import MONTHS, ROWS, connect_duckdb, sample_table

pytestmark = [pytest.mark.redshift, pytest.mark.s3]

REDSHIFT = RedshiftDialect_redshift_connector()


@pytest.fixture(scope="session")
def duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Conexão com ``httpfs``, ``delta`` e ``aws`` e um secret S3 pela cadeia de credenciais."""
    connection = connect_duckdb(("httpfs", "delta", "aws"))
    region = os.environ.get("AWS_REGION", "")
    connection.execute(f"CREATE SECRET poc (TYPE s3, PROVIDER credential_chain, REGION '{region}')")

    return connection


def contract_table(name: str, schema: str, *, month: bool = False, extra: bool = False, text: bool = False) -> sa.Table:
    """O ``Table`` de ``operacoes`` no esquema da sessão, na ordem das colunas dos arquivos do delta-rs.

    ``month`` acrescenta ``mes`` no fim (a tabela final), ``extra`` uma coluna nova no fim (a
    evolução de esquema) e ``text`` uma coluna ``Text`` (o tipo que o Redshift guarda como VARCHAR(256)).
    """
    columns = [
        sa.Column("id_operacao", sa.BigInteger, nullable=False),
        sa.Column("data_ref", sa.DateTime, nullable=False),
        sa.Column("id_cliente", sa.Integer, nullable=False),
        sa.Column("valor", sa.Numeric(18, 2), nullable=False),
        sa.Column("descricao", sa.String(200)),
    ]
    if month:
        columns.append(sa.Column("mes", sa.String(7), nullable=False))
    if extra:
        columns.append(sa.Column("canal", sa.String(20)))
    if text:
        columns.append(sa.Column("observacao", sa.Text))

    return sa.Table(name, sa.MetaData(schema=schema), *columns)


def ddl(table: sa.Table) -> str:
    """O ``CREATE TABLE`` compilado pelo dialeto do Redshift."""
    return str(CreateTable(table).compile(dialect=REDSHIFT))


def write_manifest(location: S3Location, key_suffix: str, table: DeltaTable, month: str | None = None) -> str:
    """Grava o manifesto do ``COPY`` com os arquivos do snapshot (de um mês, quando informado) e devolve a URI."""
    actions = table.get_add_actions(flatten=True)
    rows = zip(actions.column("path").to_pylist(), actions.column("size_bytes").to_pylist(), actions.column("partition.mes").to_pylist())

    # Cada entrada leva a URL do arquivo e o content_length, obrigatório para arquivos Parquet.
    entries = [
        {"url": f"{table.table_uri}/{path}", "mandatory": True, "meta": {"content_length": size}}
        for path, size, partition in rows
        if month is None or partition == month
    ]

    key = f"{location.prefix}/{key_suffix}"
    boto3.client("s3").put_object(Bucket=location.bucket, Key=key, Body=json.dumps({"entries": entries}).encode())

    return f"s3://{location.bucket}/{key}"


def outcome(action: object) -> str:
    """``ok`` quando a chamada passa; senão o tipo e a primeira linha do erro, para o relatório."""
    try:
        action()
        return "ok"
    except Exception as error:  # noqa: BLE001 - o resultado é registrado, não propagado
        return f"{type(error).__name__}: {str(error).splitlines()[0][:200]}"


def test_session_and_named_parameters(redshift_session: RedshiftSession) -> None:
    """A sessão responde; o cursor aceita ``paramstyle = "named"`` e o esquema dá ``CREATE``."""
    session = redshift_session

    version, user, schema = session.execute("select version(), current_user, current_schema()")[0]
    record("redshift.version", version)
    record("redshift.current_user", user)
    record("redshift.current_schema", schema)

    # Os parâmetros nomeados são o estilo que o texto gerado por dialeto usa (:mes).
    cursor = session.connection.cursor()
    cursor.paramstyle = "named"
    cursor.execute("select :mes as mes", {"mes": "2026-08"})
    assert cursor.fetchone()[0] == "2026-08"

    assert session.execute("select has_schema_privilege(%s, 'CREATE')", (session.schema,))[0][0] is True


def test_sqlalchemy_ddl_creates_table(redshift_session: RedshiftSession) -> None:
    """O DDL do SQLAlchemy cria a tabela no esquema; ``information_schema`` mostra o que o Redshift guardou."""
    session = redshift_session
    name = session.table("ddl")
    session.execute(ddl(contract_table(name, session.schema, month=True, text=True)))

    columns = session.execute(
        "select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale "
        "from information_schema.columns where table_schema = %s and table_name = %s order by ordinal_position",
        (session.schema, name),
    )
    assert [column[0] for column in columns] == ["id_operacao", "data_ref", "id_cliente", "valor", "descricao", "mes", "observacao"]

    by_name = {column[0]: column[1:] for column in columns}
    assert by_name["valor"][2:] == (18, 2)
    record("redshift.ddl.text_column", by_name["observacao"])  # esperado: VARCHAR(256)


def test_copy_manifest_from_delta_files(redshift_session: RedshiftSession, s3_location: S3Location) -> None:
    """Os arquivos do delta-rs entram por ``COPY ... MANIFEST`` numa staging sem ``mes``, e cada mês recebe o valor no ``INSERT``."""
    session = redshift_session
    two_months = sample_table().slice(ROWS // 2 - 500, 1000)

    # 1. O delta-rs grava a tabela no bucket: DECIMAL(18,2) como INT64 e timestamp_ntz nos arquivos.
    uri = s3_location.child("redshift/operacoes")
    write_deltalake(uri, two_months, mode="overwrite", partition_by=["mes"])
    table = DeltaTable(uri)

    # 2. A staging tem as colunas do arquivo; a tabela final acrescenta mes.
    staging = session.table("staging")
    target = session.table("operacoes")
    session.execute(ddl(contract_table(staging, session.schema)))
    session.execute(ddl(contract_table(target, session.schema, month=True)))

    # 3. Um manifesto por mês, COPY na staging e INSERT com o valor da partição.
    for month in MONTHS:
        manifest = write_manifest(s3_location, f"redshift/manifest_{month}.json", table, month)
        session.execute(f"TRUNCATE {session.qualified(staging)}")
        session.execute(f"COPY {session.qualified(staging)} FROM '{manifest}' {session.iam_role_clause()} FORMAT AS PARQUET MANIFEST")
        session.execute(f"INSERT INTO {session.qualified(target)} SELECT s.*, '{month}' FROM {session.qualified(staging)} s")

    # 4. Contagem, soma do DECIMAL e o timestamp mínimo batem com a amostra.
    rows = session.execute(f"select mes, count(*), sum(valor), min(data_ref) from {session.qualified(target)} group by mes order by mes")
    assert [(row[0], row[1]) for row in rows] == [(MONTHS[0], 500), (MONTHS[1], 500)]

    expected_sum = sum(two_months.column("valor").to_pylist(), decimal.Decimal(0))
    assert sum(row[2] for row in rows) == expected_sum
    assert rows[0][3] == two_months.column("data_ref")[0].as_py()
    record("redshift.copy.decimal_int64_and_timestamp_ntz", "ok")


def test_copy_column_list_and_fillrecord(redshift_session: RedshiftSession, s3_location: S3Location) -> None:
    """Um arquivo anterior a uma coluna nova: o que o ``COPY`` aceita, lista de colunas, ``FILLRECORD`` ou nenhum."""
    session = redshift_session
    uri = s3_location.child("redshift/operacoes_antigas")
    write_deltalake(uri, sample_table().slice(0, 100), mode="overwrite", partition_by=["mes"])
    manifest = write_manifest(s3_location, "redshift/manifest_antigo.json", DeltaTable(uri))

    target = session.table("evoluida")
    session.execute(ddl(contract_table(target, session.schema, extra=True)))
    qualified = session.qualified(target)
    role = session.iam_role_clause()

    attempts = {
        "positional": f"COPY {qualified} FROM '{manifest}' {role} FORMAT AS PARQUET MANIFEST",
        "column_list": f"COPY {qualified} (id_operacao, data_ref, id_cliente, valor, descricao) FROM '{manifest}' {role} FORMAT AS PARQUET MANIFEST",
        "fillrecord": f"COPY {qualified} FROM '{manifest}' {role} FORMAT AS PARQUET MANIFEST FILLRECORD",
    }

    results = {}
    for label, sql in attempts.items():
        session.execute(f"TRUNCATE {qualified}")
        results[label] = outcome(lambda sql=sql: session.execute(sql))
        if results[label] == "ok":
            loaded, nulls = session.execute(f"select count(*), count(*) - count(canal) from {qualified}")[0]
            results[label] = f"ok: {loaded} linhas, canal nulo em {nulls}"
        record(f"redshift.copy.{label}", results[label])

    assert any(result.startswith("ok") for result in results.values()), results


def test_copy_varchar_overflow(redshift_session: RedshiftSession, s3_location: S3Location) -> None:
    """Uma string acima do ``VARCHAR`` de destino: o ``COPY`` trunca ou aborta, e o motivo fica em ``stl_load_errors``."""
    session = redshift_session
    long_text = sample_table().slice(0, 10).set_column(5, "descricao", pa.array(["x" * 300] * 10, pa.string()))

    uri = s3_location.child("redshift/texto_longo")
    write_deltalake(uri, long_text, mode="overwrite", partition_by=["mes"])
    manifest = write_manifest(s3_location, "redshift/manifest_texto_longo.json", DeltaTable(uri))

    target = session.table("texto_longo")
    session.execute(ddl(contract_table(target, session.schema)))
    result = outcome(lambda: session.execute(f"COPY {session.qualified(target)} FROM '{manifest}' {session.iam_role_clause()} FORMAT AS PARQUET MANIFEST"))

    if result == "ok":
        result = f"ok: comprimento gravado = {session.execute(f'select max(len(descricao)) from {session.qualified(target)}')[0][0]}"
    else:
        errors = session.execute("select err_reason from stl_load_errors order by starttime desc limit 1")
        result = f"{result}; stl_load_errors: {errors[0][0].strip() if errors else '(vazio)'}"
    record("redshift.copy.varchar_overflow", result)


def test_super_and_json_parse(redshift_session: RedshiftSession) -> None:
    """``SUPER`` recebe texto por ``JSON_PARSE`` e devolve campos por caminho; ``JSON_SERIALIZE`` volta ao texto."""
    session = redshift_session
    name = session.table("eventos")
    session.execute(f"CREATE TABLE {session.qualified(name)} (id BIGINT NOT NULL, meta SUPER)")
    session.execute(f"INSERT INTO {session.qualified(name)} VALUES (1, JSON_PARSE(%s))", ('{"sistema": "A", "ativo": true}',))

    rows = session.execute(f"select meta.sistema, JSON_SERIALIZE(meta) from {session.qualified(name)}")
    record("redshift.super.path_and_serialize", rows[0])
    assert json.loads(rows[0][1])["sistema"] == "A"


def test_unload_partition_by_and_register(redshift_session: RedshiftSession, s3_location: S3Location, duckdb_connection: duckdb.DuckDBPyConnection) -> None:
    """``UNLOAD ... PARTITION BY (mes) MANIFEST VERBOSE`` grava o layout do Delta; os arquivos entram por ``AddAction`` e o DuckDB os lê."""
    session = redshift_session
    name = session.table("unload")
    table = contract_table(name, session.schema, month=True)
    session.execute(ddl(table))

    # Seis linhas por um INSERT de várias linhas, compilado com os valores embutidos.
    rows = [
        {
            "id_operacao": i,
            "data_ref": dt.datetime(2026, 1 + i % 2, 1, 12, 0),
            "id_cliente": i % 3,
            "valor": decimal.Decimal(i) / 100,
            "descricao": f"linha {i}",
            "mes": MONTHS[i % 2],
        }
        for i in range(6)
    ]
    session.execute(str(sa.insert(table).values(rows).compile(dialect=REDSHIFT, compile_kwargs={"literal_binds": True})))

    # O UNLOAD grava mes=.../ sem a coluna dentro do arquivo e um manifesto com content_length e record_count.
    destination = s3_location.child("redshift/unload/operacoes")
    select = f"select id_operacao, data_ref, id_cliente, valor, descricao, mes from {session.qualified(name)}"
    session.execute(
        f"UNLOAD ('{select}') TO '{destination}/' {session.iam_role_clause()} FORMAT AS PARQUET PARTITION BY (mes) MANIFEST VERBOSE ALLOWOVERWRITE"
    )

    s3 = boto3.client("s3")
    prefix = destination.removeprefix(f"s3://{s3_location.bucket}/")
    manifest = json.loads(s3.get_object(Bucket=s3_location.bucket, Key=f"{prefix}/manifest")["Body"].read())
    entries = manifest["entries"]
    assert sum(entry["meta"]["record_count"] for entry in entries) == 6
    record("redshift.unload.manifest_schema", manifest.get("schema"))

    # O rodapé de um arquivo: os tipos físicos, a obrigatoriedade e as estatísticas que a AddAction usa.
    body = s3.get_object(Bucket=s3_location.bucket, Key=entries[0]["url"].removeprefix(f"s3://{s3_location.bucket}/"))["Body"].read()
    parquet = pq.ParquetFile(io.BytesIO(body))
    record("redshift.unload.physical_types", {parquet.schema.column(i).name: parquet.schema.column(i).physical_type for i in range(len(parquet.schema))})
    record("redshift.unload.schema", " ".join(str(parquet.schema).split()))
    statistics = parquet.metadata.row_group(0).column(0).statistics
    record("redshift.unload.has_min_max", bool(statistics and statistics.has_min_max))

    # O registro no Delta: a tabela nasce do esquema do contrato, e cada arquivo do manifesto vira uma AddAction.
    schema = pa.schema(
        [
            pa.field("id_operacao", pa.int64(), nullable=False),
            pa.field("data_ref", pa.timestamp("us"), nullable=False),
            pa.field("id_cliente", pa.int32(), nullable=False),
            pa.field("valor", pa.decimal128(18, 2), nullable=False),
            pa.field("descricao", pa.string()),
            pa.field("mes", pa.string(), nullable=False),
        ]
    )
    delta = DeltaTable.create(destination, schema, partition_by=["mes"])
    actions = []
    for entry in entries:
        relative = entry["url"].removeprefix(f"{destination}/")
        month = relative.split("/")[0].removeprefix("mes=")
        actions.append(
            AddAction(
                path=relative,
                size=entry["meta"]["content_length"],
                partition_values={"mes": month},
                modification_time=int(time.time() * 1000),
                data_change=True,
                stats=json.dumps({"numRecords": entry["meta"]["record_count"], "minValues": {}, "maxValues": {}, "nullCount": {}}),
            )
        )
    delta.create_write_transaction(actions, mode="append", schema=delta.schema(), partition_by=["mes"])

    record("redshift.unload.delta_rs_read", outcome(lambda: DeltaTable(destination).to_pyarrow_table()))
    assert duckdb_connection.execute(f"SELECT count(*) FROM delta_scan('{destination}')").fetchone()[0] == 6
