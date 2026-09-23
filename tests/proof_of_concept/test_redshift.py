"""Prova de conceito no Redshift: os itens de ``plan/PLAN-STAGE-0.md`` que esperam uma conexão.

A suíte cria tabelas ``serialize_db_poc_<id>_*`` no esquema de ``SERIALIZE_DB_TEST_REDSHIFT_SCHEMA``
e arquivos sob ``SERIALIZE_DB_TEST_S3_ROOT``; sem uma das duas é pulada, e com elas a falta de
conexão é falha. A conexão é a de ``examples/redshift_native.py``, o caminho executado no ambiente
alvo: endereço e credencial temporária do workgroup serverless. Com
``SERIALIZE_DB_REDSHIFT_SHARE_DATABASE``, cada conexão roda ``USE <banco>`` e as tabelas são citadas
por ``esquema.tabela``, como em ``examples/redshift_copy_unload.py``; o nome em três partes fica
para quem está conectado a outro banco, como a Data API. O ``COPY`` e o ``UNLOAD`` levam o papel IAM
configurado ou, sem ele, as credenciais da sessão ``boto3``, porque o namespace do ambiente alvo não
tem papel associado.

Os testes exercitam o ``redshift_connector`` (sessão, ``paramstyle`` nomeado), o banco do esquema e
o ida e volta depois do ``USE``, o DDL compilado pelo SQLAlchemy, o ``COPY ... MANIFEST`` de
arquivos gravados pelo delta-rs (o ``DECIMAL`` em ``INT64``, o ``timestamp_ntz``, a lista de colunas
e o ``FILLRECORD``), o ``VARCHAR`` excedido, o ``SUPER``, o ``UNLOAD ... PARTITION BY`` registrado
no Delta e lido pelo DuckDB, a Data API pelo ciclo de ``examples/redshift_data_api.py``, e o
``COPY`` e o ``UNLOAD`` de duas tabelas em paralelo, uma conexão por tabela, o caminho de
``publish_redshift`` da etapa 8. As leituras que as decisões da etapa 5 de 2026-09-23 esperam vêm no
fim: o ``UNLOAD`` sem ``PARTITION BY`` para a pasta Hive, o ``stream`` por ``UNLOAD`` com os valores
como literais e os seus casos de borda, o ``row_desc`` de cada tipo, o custo de uma carga pequena
por ``COPY``, o rodapé do ``UNLOAD`` com ``NaN`` (issue #59), e o texto da auditoria da etapa 4 sob
``search_path`` no esquema do datashare, com a comparação do ``NaN``. Os resultados que a
documentação não fixa vão para o relatório da sessão; os que duas execuções limpas no ambiente alvo
leram iguais são asserções. O que cada execução no ambiente alvo leu está em ``plan/POC.md``.
"""

from __future__ import annotations

import datetime as dt
import decimal
import functools
import io
import json
import os
import time
import uuid
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote

import boto3
import botocore.exceptions
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import redshift_connector
import sqlalchemy as sa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import DeltaError
from deltalake.transaction import AddAction
from redshift_connector.utils.oids import get_datatype_name
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy.sql.visitors import iterate
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

from client_model import Base as ClientBase
from conftest import RedshiftSession, S3Location, connect_redshift, describe_error, record
from poc_delta import MONTHS, ROWS, connect_duckdb, sample_table
from serialize_db import audit, schema, sql

pytestmark = [pytest.mark.redshift, pytest.mark.s3]

REDSHIFT = RedshiftDialect_redshift_connector()
# O compilador do stream e do query da etapa 5: com paramstyle "named", o % dos literais não sai
# dobrado, e o padrão "format" o dobra (sonda local de 2026-09-23, plan/POC.md).
REDSHIFT_NAMED = RedshiftDialect_redshift_connector(paramstyle="named")

# Os erros que uma leitura registra em vez de reprovar: os do servidor, pelo driver, e os dos
# clientes do S3, do Delta e do Arrow. Um erro do próprio teste, como um TypeError, sobe e reprova.
SERVICE_ERRORS = (
    redshift_connector.Error,
    botocore.exceptions.ClientError,
    DeltaError,
    pa.ArrowException,
    OSError,
)


@pytest.fixture(scope="session")
def duckdb_connection(s3_location: S3Location) -> Iterator[duckdb.DuckDBPyConnection]:
    """Conexão com ``httpfs``, ``delta`` e ``aws`` e um secret S3 pela cadeia de credenciais,
    fechada no fim da sessão."""
    # s3_location roda antes do secret: pelo proxy_environment, AWS_REGION e NO_PROXY estão no
    # ambiente, e require_s3_access conferiu o acesso à raiz.
    connection = connect_duckdb(("httpfs", "delta", "aws"))
    region = os.environ.get("AWS_REGION", "")
    connection.execute(f"CREATE SECRET poc (TYPE s3, PROVIDER credential_chain, REGION '{region}')")

    yield connection
    connection.close()


def contract_table(name: str, schema_name: str, *, month: bool = False, extra: bool = False,
                   text: bool = False) -> sa.Table:
    """O ``Table`` de ``operacoes`` no esquema da sessão, na ordem das colunas dos arquivos do
    delta-rs.

    ``schema_name`` é o esquema da sessão: depois do ``USE``, ``esquema.tabela`` resolve no banco do
    datashare. O ``quoted_name`` com ``quote=False`` deixa o nome sem aspas no texto; num
    ``banco.esquema``, o nome em três partes de uma sessão sem ``USE``, é ele que mantém o ponto
    fora das aspas, porque o esquema com ponto em texto simples compila ``"banco.esquema".tabela``.

    ``month`` acrescenta ``mes`` no fim (a tabela final), ``extra`` uma coluna nova no fim (a
    evolução de esquema) e ``text`` uma coluna ``Text`` (o tipo que o Redshift guarda como
    VARCHAR(256)).
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

    return sa.Table(name, sa.MetaData(schema=quoted_name(schema_name, False)), *columns)


def ddl(table: sa.Table) -> str:
    """O ``CREATE TABLE`` compilado pelo dialeto do Redshift."""
    return str(CreateTable(table).compile(dialect=REDSHIFT))


def write_manifest(location: S3Location, key_suffix: str, table: DeltaTable,
                   month: str | None = None) -> str:
    """Grava o manifesto do ``COPY`` com os arquivos do snapshot (de um mês, quando informado) e
    devolve a URI.

    A URL de cada entrada é a pasta da tabela mais o ``path`` da ação ``add``.
    ``DeltaTable.table_uri`` termina em barra, que sai antes da junção: uma chave S3 com ``//`` é
    outra chave, e o ``COPY`` responde ``File not found`` (ambiente alvo, 2026-09-21). O ``path``
    pode vir codificado como URL (o protocolo Delta o permite; o delta-rs 1.6.4 grava
    ``mes=2026-01/...`` sem codificar), e a chave do objeto é a forma decodificada.
    """
    actions = table.get_add_actions(flatten=True)
    rows = zip(
        actions.column("path").to_pylist(),
        actions.column("size_bytes").to_pylist(),
        actions.column("partition.mes").to_pylist(),
    )

    # Cada entrada leva a URL do arquivo e o content_length, obrigatório para arquivos Parquet.
    base = table.table_uri.rstrip("/")
    entries = []
    for path, size, partition in rows:
        if month is not None and partition != month:
            continue
        entries.append(
            {"url": f"{base}/{unquote(path)}", "mandatory": True, "meta": {"content_length": size}}
        )
    record(
        f"redshift.copy.manifest.{key_suffix.rsplit('/', 1)[-1].removesuffix('.json')}",
        entries[0]["url"] if entries else "(vazio)",
    )

    key = f"{location.prefix}/{key_suffix}"
    boto3.client("s3").put_object(
        Bucket=location.bucket, Key=key, Body=json.dumps({"entries": entries}).encode()
    )

    return f"s3://{location.bucket}/{key}"


def outcome(action: Callable[[], object],
            errors: tuple[type[Exception], ...] = SERVICE_ERRORS) -> str:
    """``ok`` quando a chamada passa; num dos ``errors``, o tipo e a mensagem do erro, para o
    relatório. Outro erro sobe."""
    try:
        action()
        return "ok"
    except errors as error:
        return describe_error(error)


def reading(action: Callable[[], object],
            errors: tuple[type[Exception], ...] = SERVICE_ERRORS) -> object:
    """O valor da chamada, ou, num dos ``errors``, o tipo e a mensagem do erro: uma leitura que o
    ambiente decide vai para o relatório. Outro erro sobe."""
    try:
        return action()
    except errors as error:
        return describe_error(error)


def first_row(session: RedshiftSession, text: str, params: tuple | None = None) -> list[object]:
    """A primeira linha do resultado de ``text``."""
    return session.execute(text, params)[0]


def first_value(session: RedshiftSession, text: str, params: tuple | None = None) -> object:
    """A primeira coluna da primeira linha do resultado de ``text``."""
    return session.execute(text, params)[0][0]


def rows_as_tuples(session: RedshiftSession, text: str) -> list[tuple]:
    """As linhas do resultado de ``text``, cada uma como tupla."""
    return [tuple(row) for row in session.execute(text)]


def test_session_and_named_parameters(redshift_session: RedshiftSession) -> None:
    """A sessão responde; o cursor aceita ``paramstyle = "named"``; o que ``has_schema_privilege``
    diz do esquema é leitura."""
    session = redshift_session

    version, user, current_schema = session.execute(
        "select version(), current_user, current_schema()"
    )[0]
    record("redshift.version", version)
    record("redshift.current_user", user)
    record("redshift.current_schema", current_schema)

    # Os parâmetros nomeados são o estilo que o texto gerado por dialeto usa (:mes).
    cursor = session.connection.cursor()
    cursor.paramstyle = "named"
    cursor.execute("select :mes as mes", {"mes": "2026-08"})
    assert cursor.fetchone()[0] == "2026-08"

    # has_schema_privilege responde false pelo esquema do datashare depois do USE (leitura RS-5,
    # plan/POC.md): fica como leitura, e a prova do privilégio é o CREATE TABLE do ida e volta.
    privilege = "select has_schema_privilege(%s, 'CREATE')"
    record(
        "redshift.has_schema_privilege_create",
        reading(functools.partial(first_value, session, privilege, (session.schema,))),
    )


def test_cursor_fetchmany_feeds_record_batches(redshift_session: RedshiftSession) -> None:
    """``fetchmany`` entrega o resultado em fatias, e cada fatia vira um ``RecordBatch`` com o
    esquema do statement: o caminho do cursor, que no motor Redshift é o do ``query``."""
    arrow_schema = pa.schema(
        [("n", pa.int64()), ("valor", pa.decimal128(18, 2)), ("dia", pa.date32())]
    )
    cursor = redshift_session.connection.cursor()
    cursor.execute(
        "select n, cast(n * 0.25 as decimal(18, 2)) as valor, date '2026-08-01' + n as dia "
        "from (select 0 as n union all select 1 union all select 2 union all select 3 "
        "union all select 4) t order by n"
    )
    names = [column[0] for column in cursor.description]

    # O driver lê o resultado inteiro no execute: handle_messages só devolve em READY_FOR_QUERY,
    # cada DATA_ROW vai para cursor._cached_rows, e fetchmany fatia essa fila (redshift_connector
    # 2.1.16, core.py e cursor.py). A fila tinha as 5 linhas antes do primeiro fetchmany no ambiente
    # alvo (2026-09-21, 13:35 e 13:39): o stream do motor vai sempre por UNLOAD (decisão de
    # 2026-09-23).
    record("redshift.driver.rows_cached_after_execute", len(cursor._cached_rows))
    assert len(cursor._cached_rows) == 5

    batches = []
    rows = cursor.fetchmany(2)
    while rows:
        batches.append(
            pa.RecordBatch.from_pylist(
                [dict(zip(names, row)) for row in rows], schema=arrow_schema
            )
        )
        rows = cursor.fetchmany(2)
    assert [batch.num_rows for batch in batches] == [2, 2, 1]

    table = pa.Table.from_batches(batches)
    assert table.column("valor").to_pylist() == [
        decimal.Decimal(f"{k * 0.25:.2f}") for k in range(5)
    ]
    assert table.column("dia")[4].as_py() == dt.date(2026, 8, 5)


def test_schema_location_and_use_of_the_share_database(redshift_session: RedshiftSession) -> None:
    """Em que banco está o esquema do projeto, o que o ``USE`` mudou na sessão, e o ida e volta por
    ``esquema.tabela``."""
    session = redshift_session

    # 0. O USE de connect_redshift faz o nome em duas partes resolver no banco do datashare, e o
    # passo 4 é a prova. current_database() continua a responder o banco da conexão depois do USE
    # (ambiente alvo, 2026-09-21, plan/POC.md), então o valor é leitura, não asserção.
    record("redshift.current_database", session.execute("select current_database()")[0][0])

    # 1. Os bancos que a sessão enxerga: o tipo diz local ou shared, e o isolamento precisa ser de
    # snapshot no banco que recebe escrita vinda de outro warehouse.
    databases = session.execute(
        "select database_name, database_type, database_isolation_level "
        "from svv_redshift_databases order by 1"
    )
    record("redshift.databases", "; ".join(f"{row[0]}={row[1]}/{row[2]}" for row in databases))

    # 2. O banco do esquema: svv_all_schemas atravessa os bancos, pg_namespace só enxerga o local.
    places = session.execute(
        "select database_name, schema_name, schema_type from svv_all_schemas "
        "where schema_name = %s",
        (session.schema,),
    )
    record("redshift.schema_location", "; ".join(f"{row[0]}.{row[1]}={row[2]}" for row in places))
    assert places, f"{session.schema} não aparece em svv_all_schemas: a sessão não o enxerga"

    # 3. Os outros dois requisitos da escrita num datashare, que só a leitura fixa: o patch (186, ou
    # 1.0.78890 no serverless) e os slices do consumidor (64 ou mais). stv_slices é negada a um
    # usuário comum no ambiente alvo (42501): com o autocommit, a recusa não alcança o passo 4.
    record(
        "redshift.slices",
        reading(functools.partial(first_value, session, "select count(*) from stv_slices")),
    )

    # 4. O ida e volta pelo nome que a biblioteca escreve: criar, inserir e ler.
    name = session.table("nome_da_sessao")
    session.execute(
        f"CREATE TABLE {session.qualified(name)} (id BIGINT NOT NULL, texto VARCHAR(20))"
    )
    session.execute(f"INSERT INTO {session.qualified(name)} VALUES (1, 'a')")
    row = session.execute(f"select id, texto from {session.qualified(name)}")[0]
    assert (row[0], row[1]) == (1, "a")
    record("redshift.table_name", session.qualified(name))
    record("redshift.fully_qualified_name", session.fully_qualified(name))


def test_sqlalchemy_ddl_creates_table(redshift_session: RedshiftSession) -> None:
    """O DDL do SQLAlchemy cria a tabela no esquema; o cursor descreve as colunas, e as visões de
    catálogo são leitura."""
    session = redshift_session
    name = session.table("ddl")
    session.execute(ddl(contract_table(name, session.schema, month=True, text=True)))

    # A prova de que a tabela existe com as colunas do contrato vem do próprio cursor, que descreve
    # o resultado de um select vazio: nenhuma visão de catálogo no caminho.
    cursor = session.connection.cursor()
    cursor.execute(f"select * from {session.qualified(name)} limit 0")
    assert [column[0] for column in cursor.description] == [
        "id_operacao", "data_ref", "id_cliente", "valor", "descricao", "mes", "observacao",
    ]

    # information_schema.columns respondeu vazio depois do USE no ambiente alvo (2026-09-21): ela
    # enxerga só o banco da conexão, como has_schema_privilege. svv_all_columns cruza os bancos; o
    # que ela guarda de cada coluna (observacao em VARCHAR(256), valor em numeric 18, 2) é leitura.
    local = session.execute(
        "select column_name from information_schema.columns "
        "where table_schema = %s and table_name = %s",
        (session.schema, name),
    )
    record("redshift.ddl.information_schema_rows_after_use", len(local))
    database = session.share_database or session.execute("select current_database()")[0][0]
    columns_query = (
        "select column_name, data_type, character_maximum_length, numeric_precision, "
        "numeric_scale from svv_all_columns "
        "where database_name = %s and schema_name = %s and table_name = %s "
        "order by ordinal_position"
    )
    columns_params = (database, session.schema, name)
    record(
        "redshift.ddl.svv_all_columns",
        reading(functools.partial(session.execute, columns_query, columns_params)),
    )


def test_copy_manifest_from_delta_files(
    redshift_session: RedshiftSession, s3_location: S3Location
) -> None:
    """Os arquivos do delta-rs entram por ``COPY ... MANIFEST`` numa staging sem ``mes``, e cada mês
    recebe o valor no ``INSERT``."""
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
        session.execute(
            f"COPY {session.qualified(staging)} FROM '{manifest}' {session.credentials_clause()} "
            "FORMAT AS PARQUET MANIFEST"
        )
        session.execute(
            f"INSERT INTO {session.qualified(target)} SELECT s.*, '{month}' "
            f"FROM {session.qualified(staging)} s"
        )

    # 4. Contagem, soma do DECIMAL e o timestamp mínimo batem com a amostra.
    rows = session.execute(
        f"select mes, count(*), sum(valor), min(data_ref) from {session.qualified(target)} "
        "group by mes order by mes"
    )
    assert [(row[0], row[1]) for row in rows] == [(MONTHS[0], 500), (MONTHS[1], 500)]

    expected_sum = sum(two_months.column("valor").to_pylist(), decimal.Decimal(0))
    assert sum(row[2] for row in rows) == expected_sum
    assert rows[0][3] == two_months.column("data_ref")[0].as_py()
    record("redshift.copy.decimal_int64_and_timestamp_ntz", "ok")


def test_copy_column_list_and_fillrecord(
    redshift_session: RedshiftSession, s3_location: S3Location
) -> None:
    """Um arquivo anterior a uma coluna nova: o que o ``COPY`` aceita, lista de colunas,
    ``FILLRECORD`` ou nenhum."""
    session = redshift_session
    uri = s3_location.child("redshift/operacoes_antigas")
    write_deltalake(uri, sample_table().slice(0, 100), mode="overwrite", partition_by=["mes"])
    manifest = write_manifest(s3_location, "redshift/manifest_antigo.json", DeltaTable(uri))

    target = session.table("evoluida")
    session.execute(ddl(contract_table(target, session.schema, extra=True)))
    qualified = session.qualified(target)
    credentials = session.credentials_clause()

    attempts = {
        "positional": (
            f"COPY {qualified} FROM '{manifest}' {credentials} FORMAT AS PARQUET MANIFEST"
        ),
        "column_list": (
            f"COPY {qualified} (id_operacao, data_ref, id_cliente, valor, descricao) "
            f"FROM '{manifest}' {credentials} FORMAT AS PARQUET MANIFEST"
        ),
        "fillrecord": (
            f"COPY {qualified} FROM '{manifest}' {credentials} "
            "FORMAT AS PARQUET MANIFEST FILLRECORD"
        ),
    }

    # O COPY é registrado antes da contagem: a contagem repete um comando depois de um TRUNCATE, e
    # uma recusa dela não apaga o resultado do COPY. A conexão da sessão prepara cada comando logo
    # antes de o executar (max_prepared_statements=0), sem o prepared statement guardado que o
    # datashare recusa com 34510 depois de um TRUNCATE.
    results = {}
    for label, command in attempts.items():
        session.execute(f"TRUNCATE {qualified}")
        results[label] = outcome(functools.partial(session.execute, command))
        record(f"redshift.copy.{label}", results[label])
        if results[label] == "ok":
            loaded, nulls = session.execute(
                f"select count(*), count(*) - count(canal) from {qualified}"
            )[0]
            results[label] = f"ok: {loaded} linhas, canal nulo em {nulls}"
            record(f"redshift.copy.{label}", results[label])

    # Lido igual em 2026-09-21 às 13:35 e às 13:39 (plan/POC.md): o posicional reprova por contagem
    # de colunas (Spectrum Scan Error 15007, Unmatched number of columns), e a lista de colunas e o
    # FILLRECORD carregam as 100 linhas com a coluna nova nula.
    assert results["positional"].startswith("ProgrammingError"), results
    assert results["column_list"] == "ok: 100 linhas, canal nulo em 100", results
    assert results["fillrecord"] == "ok: 100 linhas, canal nulo em 100", results


def test_repeated_statement_after_truncate_and_the_driver_cache(
    redshift_session: RedshiftSession
) -> None:
    """Um comando repetido depois de um ``TRUNCATE`` na mesma conexão: sem o cache de prepared
    statements do driver ele passa; com o cache, o que o datashare responde é leitura.

    O ``redshift_connector`` guarda um prepared statement nomeado por texto de comando, o
    reaproveita no ``execute`` seguinte com ``Bind`` e ``Execute`` sem novo ``Parse``, e só descarta
    os guardados depois de ``ALTER``, ``CREATE``, ``DROP`` e ``ROLLBACK``. O datashare recusa a
    repetição guardada depois de um ``TRUNCATE`` com ``[Data Sharing] Error Code 34510: Concurrent
    DDL committed on <tabela> between Prepare and Execute``, e por isso a conexão da sessão vai com
    ``max_prepared_statements=0``. A segunda conexão deste teste mantém o padrão do driver; no
    ambiente alvo, em 2026-09-21 às 13:35 e às 13:39 (``plan/POC.md``), ela leu ``34510`` na
    repetição e de novo na segunda repetição (a entrada guardada fica), ``ok`` depois de um
    ``ALTER`` (que o driver reconhece) e ``ok`` numa tabela temporária do banco da conexão (a
    recusa é do datashare).
    """
    session = redshift_session
    name = session.table("cache")
    qualified = session.qualified(name)
    session.execute(f"CREATE TABLE {qualified} (id BIGINT)")
    session.execute(f"INSERT INTO {qualified} VALUES (1)")
    count = f"select count(*) from {qualified}"

    # 1. A conexão da sessão, sem cache: o mesmo texto antes e depois do TRUNCATE.
    assert session.execute(count)[0][0] == 1
    session.execute(f"TRUNCATE {qualified}")
    assert session.execute(count)[0][0] == 0

    # 2. Uma conexão com o cache do driver: a segunda execução do count reaproveita o statement
    # preparado antes do TRUNCATE.
    _, connection = connect_redshift(statement_cache=True)
    try:
        cursor = connection.cursor()
        cursor.execute(f"INSERT INTO {qualified} VALUES (2)")
        cursor.execute(count)
        assert cursor.fetchone()[0] == 1
        cursor.execute(f"TRUNCATE {qualified}")
        record(
            "redshift.driver.cached_statement_after_truncate",
            outcome(functools.partial(cursor.execute, count)),
        )
        record(
            "redshift.driver.cached_statement_repeated",
            outcome(functools.partial(cursor.execute, count)),
        )
        cursor.execute(f"ALTER TABLE {qualified} ADD COLUMN extra INTEGER")
        record(
            "redshift.driver.cached_statement_after_alter",
            outcome(functools.partial(cursor.execute, count)),
        )

        def local_temp_table() -> None:
            local = f"serialize_db_poc_{session.session_id}_cache_local"
            cursor.execute(f"CREATE TEMP TABLE {local} (id BIGINT)")
            cursor.execute(f"select count(*) from {local}")
            cursor.execute(f"TRUNCATE {local}")
            cursor.execute(f"select count(*) from {local}")

        record(
            "redshift.driver.cached_statement_after_truncate_local_temp",
            outcome(local_temp_table),
        )
    finally:
        connection.close()


def test_copy_varchar_overflow(redshift_session: RedshiftSession, s3_location: S3Location) -> None:
    """Uma string acima do ``VARCHAR`` de destino: o ``COPY`` trunca ou aborta, e o motivo fica em
    ``stl_load_errors``."""
    session = redshift_session
    long_text = sample_table().slice(0, 10).set_column(
        5, "descricao", pa.array(["x" * 300] * 10, pa.string())
    )

    uri = s3_location.child("redshift/texto_longo")
    write_deltalake(uri, long_text, mode="overwrite", partition_by=["mes"])
    manifest = write_manifest(s3_location, "redshift/manifest_texto_longo.json", DeltaTable(uri))

    target = session.table("texto_longo")
    session.execute(ddl(contract_table(target, session.schema)))

    def copy_manifest() -> None:
        """O ``COPY`` do manifesto na tabela de destino."""
        session.execute(
            f"COPY {session.qualified(target)} FROM '{manifest}' {session.credentials_clause()} "
            "FORMAT AS PARQUET MANIFEST"
        )

    def copy_manifest_truncating() -> None:
        """O mesmo ``COPY`` com ``TRUNCATECOLUMNS``."""
        session.execute(
            f"COPY {session.qualified(target)} FROM '{manifest}' {session.credentials_clause()} "
            "FORMAT AS PARQUET MANIFEST TRUNCATECOLUMNS"
        )

    result = outcome(copy_manifest)

    if result == "ok":
        result = f"ok: comprimento gravado = {
            session.execute(f'select max(len(descricao)) from {session.qualified(target)}')[0][0]
        }"
    else:
        # O diagnóstico está numa das duas visões; qual delas o ambiente deixa ler é o que RS-12 lê.
        errors = outcome(
            functools.partial(
                session.execute,
                "select err_reason from stl_load_errors order by starttime desc limit 1",
            )
        )
        if errors == "ok":
            rows = session.execute(
                "select err_reason from stl_load_errors order by starttime desc limit 1"
            )
            result = f"{result}; stl_load_errors: {rows[0][0].strip() if rows else '(vazio)'}"
        else:
            rows = session.execute(
                "select error_message from sys_load_error_detail order by start_time desc limit 1"
            )
            result = f"{result}; sys_load_error_detail: {rows[0][0].strip() if rows else '(vazio)'}"
    record("redshift.copy.varchar_overflow", result)
    # O COPY aborta em vez de truncar (2026-09-21, quatro execuções): a auditoria de tamanho da
    # etapa 4 é a barreira, e um COPY que passasse a truncar seria regressão.
    assert not result.startswith("ok"), result

    # TRUNCATECOLUMNS não é aceito com Parquet: 0A000, "TRUNCATECOLUMNS argument is not supported
    # for PARQUET based COPY" (2026-09-21, 13:35 e 13:39). Fica como leitura.
    record("redshift.copy.varchar_overflow_truncatecolumns", outcome(copy_manifest_truncating))


def test_super_and_json_parse(redshift_session: RedshiftSession, s3_location: S3Location) -> None:
    """``SUPER`` recebe texto por ``JSON_PARSE`` e devolve campos por caminho; um documento acima de
    65.535 bytes por ``INSERT`` e por ``COPY`` é leitura."""
    session = redshift_session
    name = session.table("eventos")
    session.execute(f"CREATE TABLE {session.qualified(name)} (id BIGINT NOT NULL, meta SUPER)")
    session.execute(
        f"INSERT INTO {session.qualified(name)} VALUES (1, JSON_PARSE(%s))",
        ('{"sistema": "A", "ativo": true}',),
    )

    rows = session.execute(
        f"select meta.sistema, JSON_SERIALIZE(meta) from {session.qualified(name)} where id = 1"
    )
    record("redshift.super.path_and_serialize", rows[0])
    assert json.loads(rows[0][1])["sistema"] == "A"

    # 2. Um documento acima de 65.535 bytes, o teto do VARCHAR e da staging com JSON_PARSE: se SUPER
    # o recebe por INSERT, a pergunta que resta é o COPY direto. json_size mede o documento
    # guardado.
    document = json.dumps({"itens": [{"k": i, "texto": "x" * 60} for i in range(1000)]})
    assert len(document) > 65535
    qualified = session.qualified(name)
    inserted = outcome(
        functools.partial(
            session.execute, f"INSERT INTO {qualified} VALUES (2, JSON_PARSE(%s))", (document,)
        )
    )
    if inserted == "ok":
        size_query = f"select json_size(meta) from {qualified} where id = 2"
        inserted = f"ok: json_size = {reading(functools.partial(first_value, session, size_query))}"
    record("redshift.super.insert_above_65535", inserted)

    # 3. O mesmo documento por COPY direto de um Parquet com a coluna em texto, sem staging. O
    # Redshift recusa sem SERIALIZETOJSON ("SUPER column in COPY query requires SERIALIZETOJSON
    # option") e, com a cláusula, recusa a string acima do teto ("1224 String value exceeds the max
    # size of 65535 bytes"), lido em 2026-09-21 às 13:35 e às 13:39: um Parquet com o documento em
    # texto não leva um documento grande a SUPER. As duas ficam como leitura.
    buffer = io.BytesIO()
    pq.write_table(
        pa.table({"id": pa.array([3], pa.int64()), "meta": pa.array([document], pa.string())}),
        buffer,
    )
    key = f"{s3_location.prefix}/redshift/super/documento.parquet"
    s3 = boto3.client("s3")
    s3.put_object(Bucket=s3_location.bucket, Key=key, Body=buffer.getvalue())
    source = f"s3://{s3_location.bucket}/{key}"

    def copy_parquet() -> None:
        """O ``COPY`` direto do Parquet, sem ``SERIALIZETOJSON``."""
        session.execute(
            f"COPY {qualified} FROM '{source}' {session.credentials_clause()} FORMAT AS PARQUET"
        )

    def copy_parquet_serializing() -> None:
        """O mesmo ``COPY`` com ``SERIALIZETOJSON``."""
        session.execute(
            f"COPY {qualified} FROM '{source}' {session.credentials_clause()} "
            "FORMAT AS PARQUET SERIALIZETOJSON"
        )

    record("redshift.super.copy_parquet_string_above_65535", outcome(copy_parquet))
    copied = outcome(copy_parquet_serializing)
    if copied == "ok":
        type_query = f"select json_typeof(meta), json_size(meta) from {qualified} where id = 3"
        copied = f"ok: {reading(functools.partial(first_row, session, type_query))}"
        # Uma string SUPER volta a documento por JSON_PARSE do texto, que passa pelo VARCHAR e pelo
        # teto dele.
        parse_query = f"select json_size(JSON_PARSE(meta::varchar)) from {qualified} where id = 3"
        record(
            "redshift.super.parse_of_super_string_above_65535",
            reading(functools.partial(first_value, session, parse_query)),
        )
    record("redshift.super.copy_parquet_serializetojson", copied)

    # 4. O documento como objeto num arquivo JSON de uma linha, por COPY ... FORMAT JSON 'auto': o
    # caminho da documentação para um documento grande numa coluna SUPER, que carregou o objeto de
    # 80.901 bytes em 2026-09-21 (13:35 e 13:39). É o caminho dos documentos acima do teto do
    # VARCHAR, se a etapa 8 o adotar (plan/OPEN_QUESTIONS.md).
    key = f"{s3_location.prefix}/redshift/super/documento.json"
    s3.put_object(
        Bucket=s3_location.bucket,
        Key=key,
        Body=json.dumps({"id": 4, "meta": json.loads(document)}).encode(),
    )

    def copy_json() -> None:
        """O ``COPY ... FORMAT JSON 'auto'`` do arquivo JSON de uma linha."""
        session.execute(
            f"COPY {qualified} FROM 's3://{s3_location.bucket}/{key}' "
            f"{session.credentials_clause()} FORMAT JSON 'auto'"
        )

    loaded = outcome(copy_json)
    if loaded == "ok":
        kind, size = session.execute(
            f"select json_typeof(meta), json_size(meta) from {qualified} where id = 4"
        )[0]
        loaded = f"ok: {kind}, json_size = {size}"
        assert kind == "object" and size > 65535, loaded
    record("redshift.super.copy_json_auto_above_65535", loaded)
    assert loaded.startswith("ok"), loaded


def operation_rows() -> list[dict[str, object]]:
    """As seis linhas de ``operacoes`` que os testes do ``UNLOAD`` inserem, três por mês."""
    return [
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


def read_object(location: S3Location, uri: str) -> bytes:
    """O conteúdo do objeto ``uri`` no bucket da sessão."""
    key = uri.removeprefix(f"s3://{location.bucket}/")
    return boto3.client("s3").get_object(Bucket=location.bucket, Key=key)["Body"].read()


def register_unloaded(destination: str, entries: list[dict]) -> None:
    """Cria em ``destination`` a tabela Delta de ``operacoes`` particionada por ``mes`` e registra
    cada arquivo do manifesto do ``UNLOAD`` numa ``AddAction``, sem estatísticas de coluna."""
    arrow_schema = pa.schema(
        [
            pa.field("id_operacao", pa.int64(), nullable=False),
            pa.field("data_ref", pa.timestamp("us"), nullable=False),
            pa.field("id_cliente", pa.int32(), nullable=False),
            pa.field("valor", pa.decimal128(18, 2), nullable=False),
            pa.field("descricao", pa.string()),
            pa.field("mes", pa.string(), nullable=False),
        ]
    )
    delta = DeltaTable.create(destination, arrow_schema, partition_by=["mes"])
    actions = []
    for entry in entries:
        relative = entry["url"].removeprefix(f"{destination}/")
        month = relative.split("/")[0].removeprefix("mes=")
        stats = {
            "numRecords": entry["meta"]["record_count"],
            "minValues": {},
            "maxValues": {},
            "nullCount": {},
        }
        actions.append(
            AddAction(
                path=relative,
                size=entry["meta"]["content_length"],
                partition_values={"mes": month},
                modification_time=int(time.time() * 1000),
                data_change=True,
                stats=json.dumps(stats),
            )
        )
    delta.create_write_transaction(
        actions, mode="append", schema=delta.schema(), partition_by=["mes"]
    )


def test_unload_partition_by_and_register(
    redshift_session: RedshiftSession, s3_location: S3Location,
    duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """``UNLOAD ... PARTITION BY (mes) MANIFEST VERBOSE`` grava o layout do Delta; os arquivos
    entram por ``AddAction`` e o DuckDB os lê."""
    session = redshift_session
    name = session.table("unload")
    table = contract_table(name, session.schema, month=True)
    session.execute(ddl(table))

    # Seis linhas por um INSERT de várias linhas, compilado com os valores embutidos.
    rows = operation_rows()
    session.execute(
        str(
            sa.insert(table)
            .values(rows)
            .compile(dialect=REDSHIFT, compile_kwargs={"literal_binds": True})
        )
    )

    # O UNLOAD grava mes=.../ sem a coluna dentro do arquivo e um manifesto com content_length e
    # record_count.
    destination = s3_location.child("redshift/unload/operacoes")
    select = (
        "select id_operacao, data_ref, id_cliente, valor, descricao, mes "
        f"from {session.qualified(name)}"
    )

    def unload_partitioned() -> None:
        """O ``UNLOAD`` por ``PARTITION BY (mes)``, com manifesto verboso e ``ALLOWOVERWRITE``."""
        session.execute(
            f"UNLOAD ('{select}') TO '{destination}/' {session.credentials_clause()} "
            "FORMAT AS PARQUET PARTITION BY (mes) MANIFEST VERBOSE ALLOWOVERWRITE"
        )

    unload = outcome(unload_partitioned)
    record("redshift.unload.partition_by", unload)

    # PARTITION BY MANIFEST VERBOSE, que a documentação não lista, passou no ambiente alvo em
    # 2026-09-21 (examples/redshift_manifest.py): a recusa é regressão e reprova.
    assert unload == "ok", unload

    manifest = json.loads(read_object(s3_location, f"{destination}/manifest"))
    entries = manifest["entries"]
    assert sum(entry["meta"]["record_count"] for entry in entries) == 6
    record("redshift.unload.manifest_schema", manifest.get("schema"))
    record(
        "redshift.unload.files",
        [entry["url"].removeprefix(f"{destination}/") for entry in entries],
    )

    # O rodapé de um arquivo: os tipos físicos, a obrigatoriedade e as estatísticas que a AddAction
    # usa.
    body = read_object(s3_location, entries[0]["url"])
    parquet = pq.ParquetFile(io.BytesIO(body))
    physical = {
        parquet.schema.column(i).name: parquet.schema.column(i).physical_type
        for i in range(len(parquet.schema))
    }
    record("redshift.unload.physical_types", physical)
    record("redshift.unload.schema", " ".join(str(parquet.schema).split()))
    statistics = parquet.metadata.row_group(0).column(0).statistics
    record("redshift.unload.has_min_max", bool(statistics and statistics.has_min_max))

    # Medido no ambiente alvo em 2026-09-21 (plan/POC.md): TIMESTAMP sai em INT96, obsoleto no
    # formato e sem estatística, e DECIMAL(18,2) em FIXED_LEN_BYTE_ARRAY, como o PyArrow grava e não
    # como grava o delta-rs. Toda coluna sai optional, inclusive as NOT NULL da origem.
    assert physical["data_ref"] == "INT96", physical
    assert physical["valor"] == "FIXED_LEN_BYTE_ARRAY", physical
    assert statistics and statistics.has_min_max, "o UNLOAD deixou de gravar mínimo e máximo"

    # O registro no Delta: a tabela nasce do esquema do contrato, e cada arquivo do manifesto vira
    # uma AddAction.
    register_unloaded(destination, entries)

    # data_ref está declarada timestamp[us] na tabela Delta e INT96 no arquivo: os dois leitores
    # convertem e devolvem os valores intactos (sondagem de 2026-09-21, plan/POC.md).
    def read_by_delta_rs() -> None:
        """A tabela registrada, lida inteira pelo delta-rs."""
        DeltaTable(destination).to_pyarrow_table()

    record("redshift.unload.delta_rs_read", outcome(read_by_delta_rs))
    assert duckdb_connection.execute(
        f"SELECT count(*) FROM delta_scan('{destination}')"
    ).fetchone()[0] == 6

    # Onde o UNLOAD recusa gravar sem ALLOWOVERWRITE: o mesmo prefixo, um prefixo pai com arquivos
    # abaixo, e um subprefixo novo e vazio dentro de uma pasta com arquivos, que é o destino novo
    # por tentativa da etapa 5 (test_unload_to_a_hive_prefix_and_register). Leituras.
    parent = s3_location.child("redshift/unload")
    for label, target in (
        ("same_prefix", f"{destination}/"),
        ("parent_prefix", f"{parent}/"),
        ("new_subprefix", f"{destination}/segunda/"),
    ):
        command = (
            f"UNLOAD ('{select}') TO '{target}' {session.credentials_clause()} "
            "FORMAT AS PARQUET PARTITION BY (mes)"
        )
        record(
            f"redshift.unload.destination.{label}",
            outcome(functools.partial(session.execute, command)),
        )


def cell_value(cell: dict) -> object:
    """O valor de uma célula da Data API: o único item do dicionário, ou ``None`` com ``isNull``."""
    if cell.get("isNull"):
        return None
    return next(iter(cell.values()))


def test_data_api_runs_the_statement_and_pages_the_result(
    redshift_session: RedshiftSession
) -> None:
    """A Data API executa por HTTPS, assíncrona: cada célula é um dicionário de um item, e
    ``DECIMAL`` volta como texto.

    É o ciclo de ``examples/redshift_data_api.py``. O que ele prova é que existe caminho sem a porta
    5439; o que ele mostra é por que a troca de dados da biblioteca não passa por aqui.
    """
    workgroup = os.environ.get("SERIALIZE_DB_REDSHIFT_WORKGROUP")
    if not workgroup:
        pytest.skip("a Data API precisa de SERIALIZE_DB_REDSHIFT_WORKGROUP")

    session = redshift_session
    name = session.table("data_api")
    session.execute(
        f"CREATE TABLE {session.qualified(name)} "
        "(id BIGINT NOT NULL, valor DECIMAL(18,2), texto VARCHAR(20))"
    )
    session.execute(
        f"INSERT INTO {session.qualified(name)} VALUES (1, 10.25, 'a'), (2, NULL, NULL)"
    )

    parameters = {
        "Database": os.environ["SERIALIZE_DB_REDSHIFT_DATABASE"],
        "WorkgroupName": workgroup,
    }
    client = boto3.client(
        "redshift-data", region_name=os.environ.get("AWS_REGION") or boto3.Session().region_name
    )

    # 1. Dispara: a chamada volta na hora, com o identificador do statement. A Data API abre a
    # sessão dela no banco de Database, sem o USE desta conexão: nome em três partes.
    statement = client.execute_statement(
        Sql=f"select id, valor, texto from {session.fully_qualified(name)} order by id",
        **parameters,
    )["Id"]

    # 2. Espera o estado final; sem espera não há resultado para pedir.
    deadline = time.perf_counter() + 60
    while True:
        described = client.describe_statement(Id=statement)
        if described["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            break
        assert time.perf_counter() < deadline, f"statement em {described['Status']} após 60 s"
        time.sleep(0.5)
    assert described["Status"] == "FINISHED", described.get("Error")

    # 3. Pagina o resultado: uma célula por dicionário de um item, e isNull no lugar do valor.
    columns: list[str] = []
    rows: list[list[object]] = []
    for page in client.get_paginator("get_statement_result").paginate(Id=statement):
        columns = columns or [column["name"] for column in page["ColumnMetadata"]]
        for record_cells in page["Records"]:
            rows.append([cell_value(cell) for cell in record_cells])

    assert columns == ["id", "valor", "texto"]
    assert [row[0] for row in rows] == [1, 2]
    assert rows[1][1] is None and rows[1][2] is None
    record("redshift.data_api.decimal_cell", repr(rows[0][1]))  # esperado: texto, não Decimal
    record("redshift.data_api.duration_ms", described.get("Duration", 0) // 1_000_000)


def test_parallel_copy_and_unload_on_two_connections(
    redshift_session: RedshiftSession, s3_location: S3Location
) -> None:
    """Duas tabelas carregadas por ``COPY ... MANIFEST`` e descarregadas por ``UNLOAD`` em paralelo,
    uma conexão por tabela.

    O ``redshift_connector`` declara ``threadsafety`` 1: uma conexão não serve a duas threads ao
    mesmo tempo, e cada tarefa abre a sua pela mesma resolução de ``connect_redshift``, que pede
    uma credencial temporária por conexão. É o caminho de ``publish_redshift`` da etapa 8, uma
    conexão por tabela; o motor da etapa 5 guarda uma sessão só por execução, sob um
    ``threading.RLock``, e os comandos do pipeline correm nela em série.
    """
    session = redshift_session
    two_months = sample_table().slice(ROWS // 2 - 500, 1000)
    uris = [s3_location.child(f"redshift/paralelo_{k}") for k in range(2)]
    for uri in uris:
        write_deltalake(uri, two_months, mode="overwrite", partition_by=["mes"])
    manifests = [
        write_manifest(s3_location, f"redshift/manifest_paralelo_{k}.json", DeltaTable(uri))
        for k, uri in enumerate(uris)
    ]
    targets = [session.table(f"paralelo_{k}") for k in range(2)]
    for target in targets:
        session.execute(ddl(contract_table(target, session.schema)))

    def run_on_own_connection(*commands: str) -> list:
        """Roda os comandos em ordem numa conexão própria e devolve as linhas do último."""
        _, connection = connect_redshift()
        try:
            cursor = connection.cursor()
            for command in commands:
                cursor.execute(command)
            return cursor.fetchall() if cursor.description else []
        finally:
            connection.close()

    def copy_into_target(k: int) -> int:
        """O ``COPY`` do manifesto ``k`` na tabela ``k``, numa conexão própria; devolve a
        contagem."""
        qualified = session.qualified(targets[k])
        command = (
            f"COPY {qualified} FROM '{manifests[k]}' "
            f"{session.credentials_clause()} FORMAT AS PARQUET MANIFEST"
        )
        rows = run_on_own_connection(command, f"select count(*) from {qualified}")
        return rows[0][0]

    # 1. Dois COPY em paralelo, em tabelas distintas: cada um numa conexão, limitados pelas slots do
    # WLM.
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=2) as pool:
        counts = list(pool.map(copy_into_target, range(2)))
    record("redshift.parallel.copy_two_tables", f"{time.perf_counter() - started:.1f} s")
    assert counts == [1000, 1000]

    # 2. Dois UNLOAD em paralelo, para prefixos distintos.
    destinations = [s3_location.child(f"redshift/unload_paralelo_{k}") for k in range(2)]

    def unload_target(k: int) -> None:
        """O ``UNLOAD`` da tabela ``k`` para o prefixo ``k``, numa conexão própria."""
        command = (
            f"UNLOAD ('select * from {session.qualified(targets[k])}') TO '{destinations[k]}/' "
            f"{session.credentials_clause()} FORMAT PARQUET"
        )
        run_on_own_connection(command)

    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(pool.map(unload_target, range(2)))
    record("redshift.parallel.unload_two_tables", f"{time.perf_counter() - started:.1f} s")
    for destination in destinations:
        files = s3_location.data_files(destination)
        assert files and sum(pq.read_metadata(file).num_rows for file in files) == 1000


# ---------------------------------------------------------------- as leituras da etapa 5


def unload_text(select: str, destination: str, credentials: str) -> str:
    """O ``UNLOAD`` da etapa 5: o ``select`` em Parquet para ``destination``, com manifesto verboso
    e ``PARALLEL OFF``.

    O ``select`` entra como literal, com as aspas simples dobradas, e ``PARALLEL OFF`` grava em
    série, na ordem do ``ORDER BY`` (``plan/redshift.md``). O texto devolvido carrega a cláusula de
    credenciais: ele vai só para ``session.execute``, nunca para o relatório.
    """
    escaped = select.replace("'", "''")
    return (
        f"UNLOAD ('{escaped}') TO '{destination}/' {credentials} "
        "FORMAT AS PARQUET MANIFEST VERBOSE PARALLEL OFF"
    )


def read_manifest(location: S3Location, destination: str) -> dict | None:
    """O manifesto que o ``UNLOAD`` gravou em ``<destination>/manifest``, ou ``None`` quando ele não
    existe."""
    s3 = boto3.client("s3")
    key = f"{destination.removeprefix(f's3://{location.bucket}/')}/manifest"
    try:
        body = s3.get_object(Bucket=location.bucket, Key=key)["Body"].read()
    except s3.exceptions.NoSuchKey:
        return None
    return json.loads(body)


def unloaded_manifest(location: S3Location, destination: str) -> dict:
    """O manifesto de um ``UNLOAD ... MANIFEST`` que passou; a falta dele reprova."""
    manifest = read_manifest(location, destination)
    assert manifest is not None, f"o UNLOAD passou e não gravou o manifesto em {destination}"
    return manifest


def read_unloaded(location: S3Location, entries: list[dict]) -> pa.Table:
    """As linhas dos arquivos do manifesto, na ordem das entradas, com o ``INT96`` lido em
    microssegundos."""
    tables = []
    for entry in entries:
        body = read_object(location, entry["url"])
        tables.append(pq.read_table(io.BytesIO(body), coerce_int96_timestamp_unit="us"))
    return pa.concat_tables(tables)


def unbound_parameters(statement: sa.sql.ClauseElement) -> list[str]:
    """Os ``bindparam`` do statement ainda sem valor: o guarda antes do ``literal_binds``.

    Sob ``literal_binds``, ``compiled.binds`` sai vazio e o ``bindparam`` sem valor vira ``NULL``
    calado, até num ``IN`` de lista (sonda local de 2026-09-23); o ``required`` de cada
    ``BindParameter`` do statement é o estado que marca a falta.
    """
    names = []
    for element in iterate(statement):
        if isinstance(element, sa.BindParameter) and element.required:
            names.append(element.key)
    return names


def footer_statistics(parquet: pq.ParquetFile, column: str) -> list[dict]:
    """O mínimo e o máximo do rodapé de ``column`` em cada grupo de linhas, em texto (``repr``) para
    o relatório."""
    index = parquet.schema_arrow.get_field_index(column)
    groups = []
    for group in range(parquet.metadata.num_row_groups):
        statistics = parquet.metadata.row_group(group).column(index).statistics
        if statistics is None or not statistics.has_min_max:
            groups.append({"has_min_max": False})
        else:
            groups.append(
                {"has_min_max": True, "min": repr(statistics.min), "max": repr(statistics.max)}
            )
    return groups


def test_unload_to_a_hive_prefix_and_register(
    redshift_session: RedshiftSession, s3_location: S3Location,
    duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """O ``UNLOAD`` sem ``PARTITION BY``, com a coluna de partição fora do ``select``, para
    ``mes=<valor>/<execution_id>_<uuid>/`` na pasta da tabela: o destino de ``export_partition`` da
    etapa 5.

    A decisão do usuário de 2026-09-23 tirou o ``PARTITION BY`` e fez o destino novo por partição e
    por tentativa. Os arquivos entram no Delta por ``AddAction`` com o caminho abaixo da pasta
    Hive, e a sonda local de 2026-09-23 leu esse caminho no delta-rs e no ``delta_scan``
    (``plan/POC.md``). O ``=`` no prefixo do ``UNLOAD``, o ``schema.elements`` do manifesto sem a
    coluna de partição e o segundo ``UNLOAD`` no mesmo destino são leituras do ambiente alvo.
    """
    session = redshift_session
    name = session.table("unload_hive")
    table = contract_table(name, session.schema, month=True)
    session.execute(ddl(table))
    rows = operation_rows()
    session.execute(
        str(
            sa.insert(table)
            .values(rows)
            .compile(dialect=REDSHIFT_NAMED, compile_kwargs={"literal_binds": True})
        )
    )

    # 1. Um UNLOAD por partição, sem mes no select, para um prefixo novo dentro de mes=<valor>/.
    destination = s3_location.child("redshift/unload_hive/operacoes")
    columns = "id_operacao, data_ref, id_cliente, valor, descricao"
    prefixes = {}
    entries = []
    for month in MONTHS:
        prefix = f"{destination}/mes={month}/exec_poc_{uuid.uuid4().hex[:8]}"
        prefixes[month] = prefix
        select = (
            f"select {columns} from {session.qualified(name)} where mes = '{month}' "
            "order by id_operacao"
        )
        unload = outcome(
            functools.partial(
                session.execute, unload_text(select, prefix, session.credentials_clause())
            )
        )
        record(f"redshift.unload_hive.unload.{month}", unload)
        assert unload == "ok", unload

        manifest = unloaded_manifest(s3_location, prefix)
        listed = [
            element.get("name") for element in (manifest.get("schema") or {}).get("elements", [])
        ]
        record(f"redshift.unload_hive.manifest_columns.{month}", listed)
        record(
            f"redshift.unload_hive.files.{month}",
            [entry["url"].removeprefix(f"{destination}/") for entry in manifest["entries"]],
        )
        entries.extend(manifest["entries"])
    assert sum(entry["meta"]["record_count"] for entry in entries) == 6

    # 2. O registro: cada arquivo vira uma AddAction com o caminho relativo à pasta da tabela, e a
    # partição vem do primeiro segmento, mes=<valor>.
    register_unloaded(destination, entries)

    # 3. A releitura pelos dois leitores, por partição.
    assert DeltaTable(destination).to_pyarrow_table().num_rows == 6
    counts = duckdb_connection.execute(
        f"SELECT mes, count(*) FROM delta_scan('{destination}') GROUP BY mes ORDER BY mes"
    ).fetchall()
    record("redshift.unload_hive.delta_scan_counts", counts)
    assert counts == [(MONTHS[0], 3), (MONTHS[1], 3)]

    # 4. O destino novo por tentativa: o mesmo prefixo de novo, sem ALLOWOVERWRITE, e outro uuid na
    # mesma pasta de partição. Leituras: a reexecução com o mesmo execution_id depende da segunda.
    select = f"select {columns} from {session.qualified(name)} where mes = '{MONTHS[0]}'"
    retry = f"{destination}/mes={MONTHS[0]}/exec_poc_{uuid.uuid4().hex[:8]}"
    for label, target in (("same_attempt", prefixes[MONTHS[0]]), ("new_attempt", retry)):
        record(
            f"redshift.unload_hive.destination.{label}",
            outcome(
                functools.partial(
                    session.execute, unload_text(select, target, session.credentials_clause())
                )
            ),
        )


def test_stream_by_unload_with_literal_values(
    redshift_session: RedshiftSession, s3_location: S3Location
) -> None:
    """O ``stream`` da etapa 5 por ``UNLOAD``: os valores do cliente entram no texto como literais,
    e as linhas lidas dos arquivos são comparadas com as do ``query``, que leva os mesmos valores
    como parâmetros do driver.

    O texto do ``UNLOAD`` é um literal e não recebe parâmetro. O dialeto com ``paramstyle="named"``
    dobra a aspa simples, mantém o ``%`` e dobra a contrabarra, o escape do PostgreSQL (sonda local
    de 2026-09-23, ``plan/POC.md``). Cada caso roda por três caminhos, que separam as hipóteses: os
    parâmetros do driver, o texto com os literais direto no cursor e o mesmo texto dentro do
    ``UNLOAD``. As comparações são leituras até duas execuções limpas no ambiente alvo.
    """
    session = redshift_session
    name = session.table("stream")
    table = sa.Table(
        name,
        sa.MetaData(schema=quoted_name(session.schema, False)),
        sa.Column("id", sa.BigInteger, nullable=False),
        sa.Column("texto", sa.String(40)),
        sa.Column("dia", sa.Date),
        sa.Column("carimbo", sa.DateTime),
        sa.Column("valor", sa.Numeric(18, 2)),
        sa.Column("taxa", sa.Float),
    )
    session.execute(ddl(table))

    # As linhas entram por parâmetro do driver, sem escape no texto.
    texts = ["d'agua", "barra \\ invertida", "50% certo", "comum"]
    for k, text in enumerate(texts):
        values = (
            k,
            text,
            dt.date(2026, 8, 28 + k),
            dt.datetime(2026, 8, 28 + k, 12, 0, 0, 123456),
            decimal.Decimal(k) + decimal.Decimal("0.25"),
            k / 10,
        )
        session.execute(
            f"INSERT INTO {session.qualified(name)} VALUES (%s, %s, %s, %s, %s, %s)", values
        )

    base = sa.select(
        table.c.id, table.c.texto, table.c.dia, table.c.carimbo, table.c.valor, table.c.taxa
    ).order_by(table.c.id)
    by_text = base.where(table.c.texto == sa.bindparam("texto"))

    # O guarda: sem o valor, o bindparam fica marcado, e o literal_binds o renderizaria NULL calado.
    assert unbound_parameters(by_text) == ["texto"]

    cases = [
        ("aspa", by_text, {"texto": "d'agua"}),
        ("contrabarra", by_text, {"texto": "barra \\ invertida"}),
        ("porcentagem", by_text, {"texto": "50% certo"}),
        ("like", base.where(table.c.texto.like(sa.bindparam("padrao"))), {"padrao": "%'%"}),
        (
            "data_numero_lista",
            base.where(table.c.dia >= sa.bindparam("dia"))
            .where(table.c.valor > sa.bindparam("valor"))
            .where(table.c.id.in_(sa.bindparam("ids", expanding=True))),
            {"dia": dt.date(2026, 8, 29), "valor": decimal.Decimal("1.00"), "ids": [1, 2, 3]},
        ),
        (
            "carimbo_taxa",
            base.where(table.c.carimbo > sa.bindparam("carimbo"))
            .where(table.c.taxa < sa.bindparam("taxa")),
            {"carimbo": dt.datetime(2026, 8, 28, 12, 0, 0, 123456), "taxa": 0.25},
        ),
    ]
    for label, statement, params in cases:
        bound = statement.params(**params)
        assert unbound_parameters(bound) == [], label

        # 1. O query: os valores como parâmetros do driver, no estilo "named".
        compiled = bound.compile(
            dialect=REDSHIFT_NAMED, compile_kwargs={"render_postcompile": True}
        )
        cursor = session.connection.cursor()
        cursor.paramstyle = "named"
        cursor.execute(str(compiled), compiled.construct_params())
        by_cursor = [tuple(row) for row in cursor.fetchall()]
        assert by_cursor, f"{label}: o query não achou linha, e a comparação não mediria nada"

        # 2. O mesmo statement com os valores como literais, direto no cursor e dentro do UNLOAD.
        literal = str(
            bound.compile(
                dialect=REDSHIFT_NAMED,
                compile_kwargs={"literal_binds": True, "render_postcompile": True},
            )
        )
        direct = reading(functools.partial(rows_as_tuples, session, literal))
        destination = s3_location.child(f"redshift/stream/{label}_{uuid.uuid4().hex[:8]}")
        unload = outcome(
            functools.partial(
                session.execute, unload_text(literal, destination, session.credentials_clause())
            )
        )
        by_unload: object = unload
        if unload == "ok":
            entries = unloaded_manifest(s3_location, destination)["entries"]
            by_unload = []
            if entries:
                unloaded_rows = read_unloaded(s3_location, entries).to_pylist()
                by_unload = [tuple(row.values()) for row in unloaded_rows]

        record(
            f"redshift.stream.{label}",
            {
                "linhas": len(by_cursor),
                "literal_no_cursor": "igual" if direct == by_cursor else repr(direct),
                "unload": "igual" if by_unload == by_cursor else repr(by_unload),
                "texto": literal.splitlines()[-1].strip(),
            },
        )


def test_unload_limit_empty_result_temp_table_and_super(
    redshift_session: RedshiftSession, s3_location: S3Location
) -> None:
    """Os casos de borda do ``stream`` por ``UNLOAD`` da etapa 5, todos leitura: o ``LIMIT`` no
    ``select`` externo, o resultado vazio, a tabela temporária da sessão e a coluna ``SUPER`` no
    Parquet.

    A documentação recusa o ``LIMIT`` externo (``plan/redshift.md``), e a mensagem é a leitura. O
    que o ``UNLOAD`` grava para um resultado vazio decide de onde sai o esquema do lote vazio.
    """
    session = redshift_session
    name = session.table("borda")
    qualified = session.qualified(name)
    session.execute(f"CREATE TABLE {qualified} (id BIGINT NOT NULL, meta SUPER)")
    session.execute(
        f"INSERT INTO {qualified} VALUES (1, JSON_PARSE(%s)), (2, JSON_PARSE(%s))",
        ('{"a": 1}', '{"b": [1, 2]}'),
    )
    s3 = boto3.client("s3")

    # 1. O LIMIT no select externo.
    limited = s3_location.child(f"redshift/borda/limite_{uuid.uuid4().hex[:8]}")
    record(
        "redshift.stream.outer_limit",
        outcome(
            functools.partial(
                session.execute,
                unload_text(
                    f"select id from {qualified} order by id limit 1",
                    limited,
                    session.credentials_clause(),
                ),
            )
        ),
    )

    # 2. O resultado vazio: o comando, o manifesto e os objetos que ficam no prefixo.
    empty = s3_location.child(f"redshift/borda/vazio_{uuid.uuid4().hex[:8]}")
    record(
        "redshift.stream.empty.unload",
        outcome(
            functools.partial(
                session.execute,
                unload_text(
                    f"select id from {qualified} where id < 0",
                    empty,
                    session.credentials_clause(),
                ),
            )
        ),
    )
    manifest = read_manifest(s3_location, empty)
    record(
        "redshift.stream.empty.manifest",
        None if manifest is None
        else {"entries": len(manifest["entries"]), "schema": manifest.get("schema")},
    )
    listing = s3.list_objects_v2(
        Bucket=s3_location.bucket,
        Prefix=empty.removeprefix(f"s3://{s3_location.bucket}/") + "/",
    )
    objects = listing.get("Contents", [])
    record(
        "redshift.stream.empty.objects",
        [(item["Key"].rsplit("/", 1)[-1], item["Size"]) for item in objects],
    )
    for item in objects:
        if item["Key"].endswith(".parquet"):
            body = s3.get_object(Bucket=s3_location.bucket, Key=item["Key"])["Body"].read()
            record(
                "redshift.stream.empty.file_schema",
                " ".join(str(pq.read_schema(io.BytesIO(body))).split()),
            )

    # 3. A tabela temporária da sessão, lida pelo UNLOAD na mesma sessão. Ela morre com a sessão, e
    # a limpeza da suíte não a apaga.
    temporary = f"serialize_db_poc_{session.session_id}_temporaria"
    session.execute(f"CREATE TEMP TABLE {temporary} AS SELECT id FROM {qualified}")
    from_temporary = s3_location.child(f"redshift/borda/temporaria_{uuid.uuid4().hex[:8]}")
    unloaded = outcome(
        functools.partial(
            session.execute,
            unload_text(
                f"select id from {temporary} order by id",
                from_temporary,
                session.credentials_clause(),
            ),
        )
    )
    if unloaded == "ok":
        manifest = unloaded_manifest(s3_location, from_temporary)
        unloaded_count = sum(entry["meta"]["record_count"] for entry in manifest["entries"])
        unloaded = f"ok: {unloaded_count} linhas"
    record("redshift.stream.temp_table", unloaded)

    # 4. A coluna SUPER no Parquet do UNLOAD: o tipo lido e os valores, em texto para o relatório.
    super_prefix = s3_location.child(f"redshift/borda/super_{uuid.uuid4().hex[:8]}")
    unloaded = outcome(
        functools.partial(
            session.execute,
            unload_text(
                f"select id, meta from {qualified} order by id",
                super_prefix,
                session.credentials_clause(),
            ),
        )
    )
    record("redshift.stream.super.unload", unloaded)
    if unloaded == "ok":
        manifest = unloaded_manifest(s3_location, super_prefix)
        super_rows = read_unloaded(s3_location, manifest["entries"])
        record("redshift.stream.super.type", str(super_rows.schema.field("meta").type))
        record(
            "redshift.stream.super.values",
            [repr(value) for value in super_rows.column("meta").to_pylist()],
        )


def test_row_description_oids_and_type_modifier(redshift_session: RedshiftSession) -> None:
    """O OID e o ``type_modifier`` de cada coluna de um resultado: a tabela de
    ``schema_from_row_description`` da etapa 5.

    O ``cursor.description`` do ``redshift_connector`` 2.1.16 devolve só o nome e o OID; o
    ``type_modifier`` fica em ``cursor.ps["row_desc"]``, e o driver o usa para decodificar o
    ``NUMERIC`` binário, com a escala ``(type_modifier - 4) & 0xFFFF`` (leitura do código de
    2026-09-23, ``plan/POC.md``). O que o servidor manda para cada tipo do contrato, para ``SUPER``,
    para os agregados e para os literais é leitura.
    """
    session = redshift_session
    name = session.table("tipos")
    qualified = session.qualified(name)
    session.execute(
        f"CREATE TABLE {qualified} (c_bigint BIGINT, c_integer INTEGER, c_smallint SMALLINT, "
        "c_double DOUBLE PRECISION, c_real REAL, c_decimal DECIMAL(18, 2), c_varchar VARCHAR(40), "
        "c_char CHAR(2), c_date DATE, c_timestamp TIMESTAMP, c_timestamptz TIMESTAMPTZ, "
        "c_boolean BOOLEAN, c_super SUPER)"
    )
    session.execute(
        f"INSERT INTO {qualified} VALUES (1, 2, 3, 1.5, 2.5, 10.25, 'texto', 'AB', '2026-08-31', "
        "'2026-08-31 12:00:00', '2026-08-31 12:00:00+00', true, JSON_PARSE(%s))",
        ('{"a": 1}',),
    )

    selects = {
        "colunas": f"select * from {qualified}",
        "expressoes": (
            "select count(*) as c_count, sum(c_decimal) as c_sum_decimal, "
            "avg(c_decimal) as c_avg_decimal, sum(c_double) as c_sum_double, "
            "cast(sum(c_decimal) as decimal(38, 6)) as c_decimal_38_6, "
            f"'literal' as c_text_literal, 1.5 as c_numeric_literal from {qualified}"
        ),
    }
    for label, query in selects.items():
        cursor = session.connection.cursor()
        cursor.execute(query)
        row = cursor.fetchone()
        row_desc = cursor.ps["row_desc"]

        # description e row_desc descrevem as mesmas colunas, com o mesmo OID
        # (Cursor._getDescription).
        assert [column[1] for column in cursor.description] == [
            field["type_oid"] for field in row_desc
        ]

        columns = []
        for field, value in zip(row_desc, row):
            modifier = field["type_modifier"]
            column = {
                "nome": field["label"].decode(),
                "oid": field["type_oid"],
                # ValueError num OID fora do enum
                "tipo": reading(
                    functools.partial(get_datatype_name, field["type_oid"]), errors=(ValueError,)
                ),
                "type_modifier": modifier,
                "python": type(value).__name__,
            }
            if field["type_oid"] == 1700 and modifier != -1:
                column["precisao_escala"] = [
                    ((modifier - 4) >> 16) & 0xFFFF, (modifier - 4) & 0xFFFF
                ]
            columns.append(column)
        record(f"redshift.row_desc.{label}", columns)


def test_small_load_copy_cost(redshift_session: RedshiftSession, s3_location: S3Location) -> None:
    """O custo fixo de carregar 10 linhas, como leitura: o Parquet no S3 mais ``COPY``, o caminho do
    ``load`` da etapa 5, contra o ``INSERT`` de várias linhas que saiu dele.

    A decisão do usuário de 2026-09-23 levou o ``load`` sempre pelo ``loader``; é esta leitura que
    traria o ``INSERT`` de volta. O melhor de três de cada, na mesma tabela e com as mesmas linhas,
    e o tempo do ``COPY`` inclui a gravação do arquivo no S3, como no ``loader``.
    """
    session = redshift_session
    name = session.table("carga_pequena")
    qualified = session.qualified(name)
    table = sa.Table(
        name,
        sa.MetaData(schema=quoted_name(session.schema, False)),
        sa.Column("id", sa.BigInteger, nullable=False),
        sa.Column("texto", sa.String(20)),
        sa.Column("valor", sa.Float),
    )
    session.execute(ddl(table))
    rows = [{"id": k, "texto": f"linha {k}", "valor": k / 10} for k in range(10)]
    arrow_schema = pa.schema(
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("texto", pa.string()),
            pa.field("valor", pa.float64()),
        ]
    )
    data = pa.Table.from_pylist(rows, schema=arrow_schema)
    key = f"{s3_location.prefix}/redshift/carga_pequena/lote.parquet"
    s3 = boto3.client("s3")

    def copy_rows() -> None:
        """Grava o arquivo no S3 e roda o ``COPY``, como o ``close`` do ``loader``."""
        buffer = io.BytesIO()
        pq.write_table(data, buffer)
        s3.put_object(Bucket=s3_location.bucket, Key=key, Body=buffer.getvalue())
        session.execute(
            f"COPY {qualified} FROM 's3://{s3_location.bucket}/{key}' "
            f"{session.credentials_clause()} FORMAT AS PARQUET"
        )

    insert = str(
        sa.insert(table)
        .values(rows)
        .compile(dialect=REDSHIFT_NAMED, compile_kwargs={"literal_binds": True})
    )

    best = {}
    for label, action in (
        ("copy", copy_rows),
        ("insert", functools.partial(session.execute, insert)),
    ):
        elapsed = []
        for _ in range(3):
            session.execute(f"TRUNCATE {qualified}")
            started = time.perf_counter()
            action()
            elapsed.append(time.perf_counter() - started)
            assert session.execute(f"select count(*) from {qualified}")[0][0] == 10
        best[label] = min(elapsed)
    record("redshift.load.small_copy", f"{best['copy']:.2f} s")
    record("redshift.load.small_insert", f"{best['insert']:.2f} s")


def test_unload_footer_statistics_with_nan(
    redshift_session: RedshiftSession, s3_location: S3Location,
    duckdb_connection: duckdb.DuckDBPyConnection,
) -> None:
    """O mínimo e o máximo que o ``UNLOAD`` grava no rodapé de uma coluna ``DOUBLE PRECISION`` com
    ``NaN`` e com os infinitos, e o que o leitor Parquet do DuckDB poda com eles, tudo leitura
    (issue #59).

    O Redshift aceita ``NaN``, ``Infinity`` e ``-Infinity`` em ``DOUBLE PRECISION``. O leitor
    Parquet do DuckDB poda o grupo de linhas pelo máximo do rodapé e perde a linha do ``NaN`` quando
    o máximo a deixa de fora (duckdb/duckdb#25521, ``plan/POC.md``). A biblioteca registra sem
    mínimo e máximo a coluna ``Double`` com valor não finito (``plan/PLAN-STAGE-3.md``), mas o
    rodapé do arquivo registrado é o do ``UNLOAD``. O ``NaN`` vai no início, no meio e no fim do
    grupo de linhas, pela ordem de ``posicao``.
    """
    session = redshift_session
    name = session.table("nao_finitos")
    qualified = session.qualified(name)
    session.execute(
        f"CREATE TABLE {qualified} "
        "(caso VARCHAR(10) NOT NULL, posicao INTEGER NOT NULL, valor DOUBLE PRECISION)"
    )
    session.execute(
        f"INSERT INTO {qualified} VALUES ('nan', 1, 1.0), "
        "('nan', 2, CAST('NaN' AS DOUBLE PRECISION)), ('nan', 3, 3.0), "
        "('infinito', 1, 1.0), ('infinito', 2, CAST('Infinity' AS DOUBLE PRECISION)), "
        "('infinito', 3, CAST('-Infinity' AS DOUBLE PRECISION))"
    )

    orders = {
        "nan_inicio": ("nan", "case when posicao = 2 then 0 else 1 end, posicao"),
        "nan_meio": ("nan", "posicao"),
        "nan_fim": ("nan", "case when posicao = 2 then 1 else 0 end, posicao"),
        "infinitos": ("infinito", "posicao"),
    }
    for label, (case, order) in orders.items():
        destination = s3_location.child(f"redshift/nao_finitos/{label}_{uuid.uuid4().hex[:8]}")
        select = f"select posicao, valor from {qualified} where caso = '{case}' order by {order}"
        unloaded = outcome(
            functools.partial(
                session.execute, unload_text(select, destination, session.credentials_clause())
            )
        )
        if unloaded != "ok":
            record(f"redshift.unload_nan.{label}", unloaded)
            continue

        manifest = unloaded_manifest(s3_location, destination)
        url = manifest["entries"][0]["url"]
        body = read_object(s3_location, url)
        parquet = pq.ParquetFile(io.BytesIO(body))

        # O DuckDB ordena o NaN acima de todo número: valor > 3 conta a linha do NaN, e 0 é a linha
        # perdida pela poda de um rodapé com máximo 3.0 (a leitura de 2026-09-23); nos infinitos,
        # conta o Infinity.
        above_three = duckdb_connection.execute(
            f"SELECT count(*) FROM read_parquet('{url}') WHERE valor > 3"
        ).fetchone()[0]
        record(
            f"redshift.unload_nan.{label}",
            {
                "ordem": [repr(value) for value in parquet.read().column("valor").to_pylist()],
                "arquivos": len(manifest["entries"]),
                "rodape": footer_statistics(parquet, "valor"),
                "duckdb_valor_maior_que_3": above_three,
            },
        )


def finite_text(expression: str) -> str:
    """O ``is_finite`` da auditoria para o Redshift, aplicado a ``expression``: o texto que o motor
    roda."""
    return str(audit.is_finite(sa.literal_column(expression)).compile(dialect=REDSHIFT_NAMED))


def as_text(rows: list) -> list[list[str]]:
    """As linhas de um resultado em texto, para o relatório."""
    return [[str(value) for value in row] for row in rows]


def test_audit_sql_under_search_path_and_nan_comparison(redshift_session: RedshiftSession) -> None:
    """O texto da auditoria da etapa 4 pelo caminho do motor da etapa 5, e como o Redshift compara o
    ``NaN``, tudo leitura.

    O ``ddl`` da etapa 1 e o ``audit_sql`` citam as tabelas sem esquema, e o motor conta com o
    ``search_path`` no esquema do datashare depois do ``USE``, que nunca rodou no ambiente alvo; o
    caso roda numa conexão própria, com o ``SET search_path`` do ``connect`` da etapa 5. O
    ``is_finite`` do Redshift é ``x NOT IN ('NaN'::float8, ...)``, que supõe o ``NaN`` igual a si
    mesmo, como no PostgreSQL; pelo IEEE, o ``NaN`` passaria por finito, ``naofinito_valor``
    contaria só o infinito e a soma levaria o ``NaN`` ao ``CAST`` para ``NUMERIC(38, 6)``. A tabela
    ``auditoria`` tem defeitos plantados e o esperado de cada contador ao lado da leitura; os textos
    do modelo cliente rodam sobre as tabelas vazias, e cobrem ``to_char``, ``octet_length``, ``~`` e
    ``count(CASE WHEN ...)``, que também nunca rodaram lá (``plan/OPEN_QUESTIONS.md``).
    """
    session = redshift_session
    prefix = f"serialize_db_poc_{session.session_id}_"
    _, connection = connect_redshift()
    try:
        cursor = connection.cursor()

        def run(text: str) -> list:
            """Executa ``text`` na conexão do caso e devolve as linhas, ou uma lista vazia."""
            cursor.execute(text)
            return cursor.fetchall() if cursor.description else []

        def run_as_text(text: str) -> list[list[str]]:
            """As linhas de ``text`` em texto, para o relatório."""
            return as_text(run(text))

        # 1. A comparação do NaN, que separa o PostgreSQL do IEEE, e o CAST que a soma evita.
        comparisons = {
            "nan_igual_nan": "select 'NaN'::float8 = 'NaN'::float8",
            "finito.nan": "select " + finite_text("'NaN'::float8"),
            "finito.infinito": "select " + finite_text("'Infinity'::float8"),
            "finito.menos_infinito": "select " + finite_text("'-Infinity'::float8"),
            "finito.numero": "select " + finite_text("1.5::float8"),
            "finito.nulo": "select " + finite_text("cast(null as float8)"),
            "cast_nan_numeric": "select cast('NaN'::float8 as numeric(38, 6))",
            "soma_com_nan": (
                "select sum(v) from (select 1.0::float8 as v union all select 'NaN'::float8) as t"
            ),
        }
        for label, text in comparisons.items():
            record(f"redshift.audit.{label}", reading(functools.partial(run_as_text, text)))

        # 2. O search_path no esquema do datashare: o que o connect da etapa 5 roda depois do USE.
        record(
            "redshift.audit.search_path",
            outcome(functools.partial(run, f"SET search_path TO {session.schema}")),
        )
        record(
            "redshift.audit.current_schema",
            reading(functools.partial(run_as_text, "select current_schema()")),
        )

        # 3. A tabela com defeitos plantados, criada pelo ddl da etapa 1 com o nome sem esquema.
        metadata = sa.MetaData()
        model = sa.Table(
            "auditoria",
            metadata,
            sa.Column("id", sa.BigInteger, primary_key=True),
            sa.Column("data", sa.Date, nullable=False),
            sa.Column("data_str", sa.String(10), nullable=False),
            sa.Column("nome", sa.String(20)),
            sa.Column("valor", sa.Double),
            sa.Column("preco", sa.Numeric(18, 2)),
            sa.Column("meta", sa.JSON),
            info={"serialize_db": {"partition_by": ["data_str"], "partition_source": "data"}},
        )
        name = session.table("auditoria")
        created = outcome(functools.partial(run, schema.ddl(model, "redshift", prefix=prefix)))
        record("redshift.audit.create_unqualified", created)
        assert created == "ok", created
        # O nome sem esquema caiu no esquema do datashare: o nome em duas partes acha a tabela.
        assert session.execute(f"select count(*) from {session.qualified(name)}")[0][0] == 0

        # Na partição 2026-08-31: o id 2 repetido, o NaN e o infinito em valor, e a linha 4 com data
        # fora da partição; a linha 5 está em outra partição, fora do escopo.
        run(
            f'INSERT INTO "{name}" VALUES '
            "(1, '2026-08-31', '2026-08-31', 'a', 1.5, 10.25, JSON_PARSE('{\"a\": 1}')), "
            "(2, '2026-08-31', '2026-08-31', 'b', CAST('NaN' AS DOUBLE PRECISION), 1.00, "
            "JSON_PARSE('{\"b\": 2}')), "
            "(2, '2026-08-31', '2026-08-31', 'c', CAST('Infinity' AS DOUBLE PRECISION), 2.00, "
            "NULL), "
            "(4, '2026-08-30', '2026-08-31', 'd', 3.0, 3.00, JSON_PARSE('{\"c\": 3}')), "
            "(5, '2026-08-30', '2026-08-30', 'e', 4.0, 4.00, NULL)"
        )
        expected = {
            "linhas": "4", "particao_data_str": "1", "naofinito_valor": "2",
            "total_valor": "4.500000", "total_preco": "16.250000", "json_meta": "0",
            "texto_nome": "0", "valor_data_str": "0",
        }

        # 4. O texto de cada verificação, como o motor o roda; a de linhas também medida a medida,
        # porque uma medida que o Redshift recusa derruba a consulta inteira.
        partitions = ["2026-08-31"]
        texts = audit.audit_sql(model, "redshift", partitions, prefix=prefix)
        for check, text in texts.items():
            record(f"redshift.audit.planted.{check}", reading(functools.partial(run_as_text, text)))
        rows_check = audit.checks(model, partitions)[0]
        measures = {}
        for column in rows_check.statement.selected_columns:
            if column.name == "data_str":
                continue
            single = sa.select(column).select_from(model).where(rows_check.statement.whereclause)
            value = reading(
                functools.partial(run_as_text, sql.render(single, "redshift", metadata, prefix))
            )
            measures[column.name] = value[0][0] if isinstance(value, list) else value
        record("redshift.audit.planted.measures", measures)
        record("redshift.audit.planted.expected", expected)
        matches = {}
        for label, value in expected.items():
            matches[label] = measures.get(label) == value
        record("redshift.audit.planted.matches", matches)
        # A amostra da reprovação de particao_data_str: a linha 4.
        sample = sql.render(
            audit.sample_statement(model, partitions, rows_check.counters["particao_data_str"]),
            "redshift",
            metadata,
            prefix,
        )
        record("redshift.audit.planted.sample", reading(functools.partial(run_as_text, sample)))

        # 5. Os textos do modelo cliente sobre as tabelas vazias, criadas pelo ddl da etapa 1.
        results = {}
        for table in ClientBase.metadata.sorted_tables:
            session.table(table.name)
            outcome_of_ddl = outcome(
                functools.partial(run, schema.ddl(table, "redshift", prefix=prefix))
            )
            if outcome_of_ddl != "ok":
                results[table.name] = {"ddl": outcome_of_ddl}
                continue
            table_partitions = partitions if schema.table_options(table).partition_by else None
            results[table.name] = {}
            for check, text in audit.audit_sql(
                table, "redshift", table_partitions, prefix=prefix
            ).items():
                results[table.name][check] = outcome(functools.partial(run, text))
        record("redshift.audit.client_model", results)
    finally:
        connection.close()
