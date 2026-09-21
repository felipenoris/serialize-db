"""Prova de conceito no Redshift: os itens de ``docs/PLAN-STAGE-0.md`` que esperam uma conexão.

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
e o ``FILLRECORD``), o ``VARCHAR`` excedido, o ``SUPER``, o ``UNLOAD ... PARTITION BY`` registrado no
Delta e lido pelo DuckDB, a Data API pelo ciclo de ``examples/redshift_data_api.py``, e o ``COPY`` e
o ``UNLOAD`` de duas tabelas em paralelo, uma conexão por thread. Os resultados que a documentação
não fixa vão para o relatório da sessão em vez de virarem asserções.

A primeira execução no ambiente alvo, em 2026-09-21, passou um teste e reprovou dez: a sessão
inteira correu numa transação aberta antes do ``USE``, que a visão ``stv_slices`` negada abortou, e
cada comando seguinte recebeu ``25P02``. A segunda, no mesmo dia, passou sete e reprovou quatro: a
URL do manifesto do ``COPY`` levava uma barra dobrada, e ``information_schema.columns`` não enxerga o
esquema do datashare depois do ``USE``. A terceira e a quarta, às 12:08 e 12:10 UTC, passaram dez e
reprovaram um: o ``select count(*)`` repetido depois do terceiro ``TRUNCATE`` de
``test_copy_column_list_and_fillrecord`` recebeu ``34510``, ``Concurrent DDL committed ... between
Prepare and Execute``, porque o ``redshift_connector`` reaproveita o prepared statement nomeado e
não o descarta num ``TRUNCATE``; a conexão vai com ``max_prepared_statements=0`` desde então. A
quinta e a sexta, às 13:35 e às 13:39 UTC, passaram os doze testes, e a etapa 0 fechou com elas: o
que as duas leram igual virou asserção (``docs/POC.md``).
"""

from __future__ import annotations

import datetime as dt
import decimal
import io
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor
from urllib.parse import unquote

import boto3
import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa
from deltalake import DeltaTable, write_deltalake
from deltalake.transaction import AddAction
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql.elements import quoted_name
from sqlalchemy_redshift.dialect import RedshiftDialect_redshift_connector

from conftest import RedshiftSession, S3Location, connect_redshift, record
from poc_delta import MONTHS, ROWS, connect_duckdb, sample_table

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

    ``schema`` é ``banco.esquema`` quando o esquema vem de um datashare, e o ``quoted_name`` com
    ``quote=False`` é o que mantém o ponto fora das aspas: um esquema com ponto em texto simples
    compila ``"banco.esquema".tabela``, que o Redshift lê como um esquema de nome esquisito.

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

    return sa.Table(name, sa.MetaData(schema=quoted_name(schema, False)), *columns)


def ddl(table: sa.Table) -> str:
    """O ``CREATE TABLE`` compilado pelo dialeto do Redshift."""
    return str(CreateTable(table).compile(dialect=REDSHIFT))


def write_manifest(location: S3Location, key_suffix: str, table: DeltaTable, month: str | None = None) -> str:
    """Grava o manifesto do ``COPY`` com os arquivos do snapshot (de um mês, quando informado) e devolve a URI.

    A URL de cada entrada é a pasta da tabela mais o ``path`` da ação ``add``. ``DeltaTable.table_uri``
    termina em barra, e a barra dobrada fez o ``COPY`` do ambiente alvo responder ``File not found``
    em 2026-09-21: uma chave S3 com ``//`` é outra chave. O ``path`` pode vir codificado como URL
    (o protocolo Delta o permite; o delta-rs 1.6.4 grava ``mes=2026-01/...`` sem codificar), e a
    chave do objeto é a forma decodificada.
    """
    actions = table.get_add_actions(flatten=True)
    rows = zip(actions.column("path").to_pylist(), actions.column("size_bytes").to_pylist(), actions.column("partition.mes").to_pylist())

    # Cada entrada leva a URL do arquivo e o content_length, obrigatório para arquivos Parquet.
    base = table.table_uri.rstrip("/")
    entries = [
        {"url": f"{base}/{unquote(path)}", "mandatory": True, "meta": {"content_length": size}}
        for path, size, partition in rows
        if month is None or partition == month
    ]
    record(f"redshift.copy.manifest.{key_suffix.rsplit('/', 1)[-1].removesuffix('.json')}", entries[0]["url"] if entries else "(vazio)")

    key = f"{location.prefix}/{key_suffix}"
    boto3.client("s3").put_object(Bucket=location.bucket, Key=key, Body=json.dumps({"entries": entries}).encode())

    return f"s3://{location.bucket}/{key}"


def describe(error: Exception) -> str:
    """O tipo e a mensagem de um erro para o relatório; num erro do ``redshift_connector``, o SQLSTATE, o campo ``M`` e o detalhe ``D`` numa linha só."""
    detail = error.args[0] if error.args else None
    if isinstance(detail, dict) and "M" in detail:
        parts = [str(detail.get("C", "")), str(detail["M"])]
        if detail.get("D"):
            parts.append(" ".join(word for word in str(detail["D"]).split() if set(word) != {"-"}))
        return f"{type(error).__name__}: {' '.join(part for part in parts if part)}"[:400]
    return f"{type(error).__name__}: {str(error).splitlines()[0][:200]}"


def outcome(action: object) -> str:
    """``ok`` quando a chamada passa; senão o tipo e a mensagem do erro, para o relatório."""
    try:
        action()
        return "ok"
    except Exception as error:  # noqa: BLE001 - o resultado é registrado, não propagado
        return describe(error)


def reading(action: object) -> object:
    """O valor da chamada, ou o tipo e a mensagem do erro; uma leitura que o ambiente decide vai para o relatório."""
    try:
        return action()
    except Exception as error:  # noqa: BLE001 - a leitura é registrada, não propagada
        return describe(error)


def test_session_and_named_parameters(redshift_session: RedshiftSession) -> None:
    """A sessão responde; o cursor aceita ``paramstyle = "named"``; o que ``has_schema_privilege`` diz do esquema é leitura."""
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

    # O que has_schema_privilege responde pelo esquema do datashare depois do USE é a leitura RS-5,
    # ainda sem resposta no ambiente alvo; a prova do privilégio é o CREATE TABLE do ida e volta.
    record("redshift.has_schema_privilege_create", reading(lambda: session.execute("select has_schema_privilege(%s, 'CREATE')", (session.schema,))[0][0]))


def test_cursor_fetchmany_feeds_record_batches(redshift_session: RedshiftSession) -> None:
    """``fetchmany`` entrega o resultado em fatias, e cada fatia vira um ``RecordBatch`` com o esquema do statement: o caminho de ``stream`` no motor Redshift."""
    schema = pa.schema([("n", pa.int64()), ("valor", pa.decimal128(18, 2)), ("dia", pa.date32())])
    cursor = redshift_session.connection.cursor()
    cursor.execute(
        "select n, cast(n * 0.25 as decimal(18, 2)) as valor, date '2026-08-01' + n as dia "
        "from (select 0 as n union all select 1 union all select 2 union all select 3 union all select 4) t order by n"
    )
    names = [column[0] for column in cursor.description]

    # O driver lê o resultado inteiro no execute: handle_messages só devolve em READY_FOR_QUERY, cada
    # DATA_ROW vai para cursor._cached_rows, e fetchmany fatia essa fila (redshift_connector 2.1.16,
    # core.py e cursor.py). A fila tinha as 5 linhas antes do primeiro fetchmany no ambiente alvo
    # (2026-09-21, 13:35 e 13:39): o stream do motor limita a memória só por UNLOAD.
    record("redshift.driver.rows_cached_after_execute", len(cursor._cached_rows))
    assert len(cursor._cached_rows) == 5

    batches = []
    while rows := cursor.fetchmany(2):
        batches.append(pa.RecordBatch.from_pylist([dict(zip(names, row)) for row in rows], schema=schema))
    assert [batch.num_rows for batch in batches] == [2, 2, 1]

    table = pa.Table.from_batches(batches)
    assert table.column("valor").to_pylist() == [decimal.Decimal(f"{k * 0.25:.2f}") for k in range(5)]
    assert table.column("dia")[4].as_py() == dt.date(2026, 8, 5)


def test_schema_location_and_use_of_the_share_database(redshift_session: RedshiftSession) -> None:
    """Em que banco está o esquema do projeto, o que o ``USE`` mudou na sessão, e o ida e volta por ``esquema.tabela``."""
    session = redshift_session

    # 0. O USE de connect_redshift faz o nome em duas partes resolver no banco do datashare, e o passo 4
    # é a prova. current_database() continua a responder o banco da conexão depois do USE (ambiente
    # alvo, 2026-09-21, docs/POC.md), então o valor é leitura, não asserção.
    record("redshift.current_database", session.execute("select current_database()")[0][0])

    # 1. Os bancos que a sessão enxerga: o tipo diz local ou shared, e o isolamento precisa ser de
    # snapshot no banco que recebe escrita vinda de outro warehouse.
    databases = session.execute("select database_name, database_type, database_isolation_level from svv_redshift_databases order by 1")
    record("redshift.databases", "; ".join(f"{row[0]}={row[1]}/{row[2]}" for row in databases))

    # 2. O banco do esquema: svv_all_schemas atravessa os bancos, pg_namespace só enxerga o local.
    places = session.execute("select database_name, schema_name, schema_type from svv_all_schemas where schema_name = %s", (session.schema,))
    record("redshift.schema_location", "; ".join(f"{row[0]}.{row[1]}={row[2]}" for row in places))
    assert places, f"{session.schema} não aparece em svv_all_schemas: a sessão não o enxerga"

    # 3. Os outros dois requisitos da escrita num datashare, que só a leitura fixa: o patch (186, ou
    # 1.0.78890 no serverless) e os slices do consumidor (64 ou mais). stv_slices é negada a um
    # usuário comum no ambiente alvo (42501): com o autocommit, a recusa não alcança o passo 4.
    record("redshift.slices", reading(lambda: session.execute("select count(*) from stv_slices")[0][0]))

    # 4. O ida e volta pelo nome que a biblioteca escreve: criar, inserir e ler.
    name = session.table("nome_da_sessao")
    session.execute(f"CREATE TABLE {session.qualified(name)} (id BIGINT NOT NULL, texto VARCHAR(20))")
    session.execute(f"INSERT INTO {session.qualified(name)} VALUES (1, 'a')")
    row = session.execute(f"select id, texto from {session.qualified(name)}")[0]
    assert (row[0], row[1]) == (1, "a")
    record("redshift.table_name", session.qualified(name))
    record("redshift.fully_qualified_name", session.fully_qualified(name))


def test_sqlalchemy_ddl_creates_table(redshift_session: RedshiftSession) -> None:
    """O DDL do SQLAlchemy cria a tabela no esquema; o cursor descreve as colunas, e as visões de catálogo são leitura."""
    session = redshift_session
    name = session.table("ddl")
    session.execute(ddl(contract_table(name, session.schema_prefix(), month=True, text=True)))

    # A prova de que a tabela existe com as colunas do contrato vem do próprio cursor, que descreve o
    # resultado de um select vazio: nenhuma visão de catálogo no caminho.
    cursor = session.connection.cursor()
    cursor.execute(f"select * from {session.qualified(name)} limit 0")
    assert [column[0] for column in cursor.description] == ["id_operacao", "data_ref", "id_cliente", "valor", "descricao", "mes", "observacao"]

    # information_schema.columns respondeu vazio depois do USE no ambiente alvo (2026-09-21): ela
    # enxerga só o banco da conexão, como has_schema_privilege. svv_all_columns cruza os bancos; o que
    # ela guarda de cada coluna (observacao em VARCHAR(256), valor em numeric 18, 2) é leitura.
    local = session.execute("select column_name from information_schema.columns where table_schema = %s and table_name = %s", (session.schema, name))
    record("redshift.ddl.information_schema_rows_after_use", len(local))
    database = session.share_database or session.execute("select current_database()")[0][0]
    record(
        "redshift.ddl.svv_all_columns",
        reading(
            lambda: session.execute(
                "select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale "
                "from svv_all_columns where database_name = %s and schema_name = %s and table_name = %s order by ordinal_position",
                (database, session.schema, name),
            )
        ),
    )


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
    session.execute(ddl(contract_table(staging, session.schema_prefix())))
    session.execute(ddl(contract_table(target, session.schema_prefix(), month=True)))

    # 3. Um manifesto por mês, COPY na staging e INSERT com o valor da partição.
    for month in MONTHS:
        manifest = write_manifest(s3_location, f"redshift/manifest_{month}.json", table, month)
        session.execute(f"TRUNCATE {session.qualified(staging)}")
        session.execute(f"COPY {session.qualified(staging)} FROM '{manifest}' {session.credentials_clause()} FORMAT AS PARQUET MANIFEST")
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
    session.execute(ddl(contract_table(target, session.schema_prefix(), extra=True)))
    qualified = session.qualified(target)
    credentials = session.credentials_clause()

    attempts = {
        "positional": f"COPY {qualified} FROM '{manifest}' {credentials} FORMAT AS PARQUET MANIFEST",
        "column_list": f"COPY {qualified} (id_operacao, data_ref, id_cliente, valor, descricao) FROM '{manifest}' {credentials} FORMAT AS PARQUET MANIFEST",
        "fillrecord": f"COPY {qualified} FROM '{manifest}' {credentials} FORMAT AS PARQUET MANIFEST FILLRECORD",
    }

    # O COPY é registrado antes da contagem, que é um comando repetido depois de um TRUNCATE: nas
    # execuções de 2026-09-21 às 12:08 e 12:10 a terceira volta recebeu 34510 dela, com o resultado do
    # FILLRECORD perdido (docs/POC.md). A conexão da sessão prepara cada comando logo antes de o executar.
    results = {}
    for label, sql in attempts.items():
        session.execute(f"TRUNCATE {qualified}")
        results[label] = outcome(lambda sql=sql: session.execute(sql))
        record(f"redshift.copy.{label}", results[label])
        if results[label] == "ok":
            loaded, nulls = session.execute(f"select count(*), count(*) - count(canal) from {qualified}")[0]
            results[label] = f"ok: {loaded} linhas, canal nulo em {nulls}"
            record(f"redshift.copy.{label}", results[label])

    # Lido igual em 2026-09-21 às 13:35 e às 13:39 (docs/POC.md): o posicional reprova por contagem de
    # colunas (Spectrum Scan Error 15007, Unmatched number of columns), e a lista de colunas e o
    # FILLRECORD carregam as 100 linhas com a coluna nova nula.
    assert results["positional"].startswith("ProgrammingError"), results
    assert results["column_list"] == "ok: 100 linhas, canal nulo em 100", results
    assert results["fillrecord"] == "ok: 100 linhas, canal nulo em 100", results


def test_repeated_statement_after_truncate_and_the_driver_cache(redshift_session: RedshiftSession) -> None:
    """Um comando repetido depois de um ``TRUNCATE`` na mesma conexão: sem o cache de prepared statements do driver ele passa; com o cache, o que o datashare responde é leitura.

    O ``redshift_connector`` guarda um prepared statement nomeado por texto de comando, o reaproveita
    no ``execute`` seguinte com ``Bind`` e ``Execute`` sem novo ``Parse``, e só descarta os guardados
    depois de ``ALTER``, ``CREATE``, ``DROP`` e ``ROLLBACK``. Nas execuções de 2026-09-21 às 12:08 e
    12:10 no ambiente alvo, o ``select count(*)`` de ``test_copy_column_list_and_fillrecord``
    reprovou na terceira volta com ``[Data Sharing] Error Code 34510: Concurrent DDL committed on
    <tabela> between Prepare and Execute`` (``docs/POC.md``): o ``TRUNCATE`` da volta é o DDL. A
    conexão da sessão vai com ``max_prepared_statements=0`` desde então, e passou aqui às 13:35 e às
    13:39 do mesmo dia; a segunda conexão deste teste mantém o padrão do driver e registra o que o
    Redshift responde: ``34510`` na repetição e de novo na segunda repetição (a entrada guardada
    fica), ``ok`` depois de um ``ALTER`` (que o driver reconhece) e ``ok`` numa tabela temporária do
    banco da conexão (a recusa é do datashare).
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
        record("redshift.driver.cached_statement_after_truncate", outcome(lambda: cursor.execute(count)))
        record("redshift.driver.cached_statement_repeated", outcome(lambda: cursor.execute(count)))
        cursor.execute(f"ALTER TABLE {qualified} ADD COLUMN extra INTEGER")
        record("redshift.driver.cached_statement_after_alter", outcome(lambda: cursor.execute(count)))

        def local_temp_table() -> None:
            local = f"serialize_db_poc_{session.session_id}_cache_local"
            cursor.execute(f"CREATE TEMP TABLE {local} (id BIGINT)")
            cursor.execute(f"select count(*) from {local}")
            cursor.execute(f"TRUNCATE {local}")
            cursor.execute(f"select count(*) from {local}")

        record("redshift.driver.cached_statement_after_truncate_local_temp", outcome(local_temp_table))
    finally:
        connection.close()


def test_copy_varchar_overflow(redshift_session: RedshiftSession, s3_location: S3Location) -> None:
    """Uma string acima do ``VARCHAR`` de destino: o ``COPY`` trunca ou aborta, e o motivo fica em ``stl_load_errors``."""
    session = redshift_session
    long_text = sample_table().slice(0, 10).set_column(5, "descricao", pa.array(["x" * 300] * 10, pa.string()))

    uri = s3_location.child("redshift/texto_longo")
    write_deltalake(uri, long_text, mode="overwrite", partition_by=["mes"])
    manifest = write_manifest(s3_location, "redshift/manifest_texto_longo.json", DeltaTable(uri))

    target = session.table("texto_longo")
    session.execute(ddl(contract_table(target, session.schema_prefix())))
    result = outcome(lambda: session.execute(f"COPY {session.qualified(target)} FROM '{manifest}' {session.credentials_clause()} FORMAT AS PARQUET MANIFEST"))

    if result == "ok":
        result = f"ok: comprimento gravado = {session.execute(f'select max(len(descricao)) from {session.qualified(target)}')[0][0]}"
    else:
        # O diagnóstico está numa das duas visões; qual delas o ambiente deixa ler é o que RS-12 lê.
        errors = outcome(lambda: session.execute("select err_reason from stl_load_errors order by starttime desc limit 1"))
        if errors == "ok":
            rows = session.execute("select err_reason from stl_load_errors order by starttime desc limit 1")
            result = f"{result}; stl_load_errors: {rows[0][0].strip() if rows else '(vazio)'}"
        else:
            rows = session.execute("select error_message from sys_load_error_detail order by start_time desc limit 1")
            result = f"{result}; sys_load_error_detail: {rows[0][0].strip() if rows else '(vazio)'}"
    record("redshift.copy.varchar_overflow", result)
    # O COPY aborta em vez de truncar (2026-09-21, quatro execuções): a auditoria de tamanho da etapa 4
    # é a barreira, e um COPY que passasse a truncar seria regressão.
    assert not result.startswith("ok"), result

    # TRUNCATECOLUMNS não é aceito com Parquet: 0A000, "TRUNCATECOLUMNS argument is not supported for
    # PARQUET based COPY" (2026-09-21, 13:35 e 13:39). Fica como leitura.
    record(
        "redshift.copy.varchar_overflow_truncatecolumns",
        outcome(lambda: session.execute(f"COPY {session.qualified(target)} FROM '{manifest}' {session.credentials_clause()} FORMAT AS PARQUET MANIFEST TRUNCATECOLUMNS")),
    )


def test_super_and_json_parse(redshift_session: RedshiftSession, s3_location: S3Location) -> None:
    """``SUPER`` recebe texto por ``JSON_PARSE`` e devolve campos por caminho; um documento acima de 65.535 bytes por ``INSERT`` e por ``COPY`` é leitura."""
    session = redshift_session
    name = session.table("eventos")
    session.execute(f"CREATE TABLE {session.qualified(name)} (id BIGINT NOT NULL, meta SUPER)")
    session.execute(f"INSERT INTO {session.qualified(name)} VALUES (1, JSON_PARSE(%s))", ('{"sistema": "A", "ativo": true}',))

    rows = session.execute(f"select meta.sistema, JSON_SERIALIZE(meta) from {session.qualified(name)} where id = 1")
    record("redshift.super.path_and_serialize", rows[0])
    assert json.loads(rows[0][1])["sistema"] == "A"

    # 2. Um documento acima de 65.535 bytes, o teto do VARCHAR e da staging com JSON_PARSE: se SUPER o
    # recebe por INSERT, a pergunta que resta é o COPY direto. json_size mede o documento guardado.
    document = json.dumps({"itens": [{"k": i, "texto": "x" * 60} for i in range(1000)]})
    assert len(document) > 65535
    qualified = session.qualified(name)
    inserted = outcome(lambda: session.execute(f"INSERT INTO {qualified} VALUES (2, JSON_PARSE(%s))", (document,)))
    if inserted == "ok":
        inserted = f"ok: json_size = {reading(lambda: session.execute(f'select json_size(meta) from {qualified} where id = 2')[0][0])}"
    record("redshift.super.insert_above_65535", inserted)

    # 3. O mesmo documento por COPY direto de um Parquet com a coluna em texto, sem staging. O Redshift
    # recusa sem SERIALIZETOJSON ("SUPER column in COPY query requires SERIALIZETOJSON option") e, com
    # a cláusula, recusa a string acima do teto ("1224 String value exceeds the max size of 65535
    # bytes"), lido em 2026-09-21 às 13:35 e às 13:39: um Parquet com o documento em texto não leva um
    # documento grande a SUPER. As duas ficam como leitura.
    buffer = io.BytesIO()
    pq.write_table(pa.table({"id": pa.array([3], pa.int64()), "meta": pa.array([document], pa.string())}), buffer)
    key = f"{s3_location.prefix}/redshift/super/documento.parquet"
    s3 = boto3.client("s3")
    s3.put_object(Bucket=s3_location.bucket, Key=key, Body=buffer.getvalue())
    source = f"s3://{s3_location.bucket}/{key}"
    record("redshift.super.copy_parquet_string_above_65535", outcome(lambda: session.execute(f"COPY {qualified} FROM '{source}' {session.credentials_clause()} FORMAT AS PARQUET")))
    copied = outcome(lambda: session.execute(f"COPY {qualified} FROM '{source}' {session.credentials_clause()} FORMAT AS PARQUET SERIALIZETOJSON"))
    if copied == "ok":
        copied = f"ok: {reading(lambda: session.execute(f'select json_typeof(meta), json_size(meta) from {qualified} where id = 3')[0])}"
        # Uma string SUPER volta a documento por JSON_PARSE do texto, que passa pelo VARCHAR e pelo teto dele.
        record("redshift.super.parse_of_super_string_above_65535", reading(lambda: session.execute(f"select json_size(JSON_PARSE(meta::varchar)) from {qualified} where id = 3")[0][0]))
    record("redshift.super.copy_parquet_serializetojson", copied)

    # 4. O documento como objeto num arquivo JSON de uma linha, por COPY ... FORMAT JSON 'auto': o
    # caminho da documentação para um documento grande numa coluna SUPER, que carregou o objeto de
    # 80.901 bytes em 2026-09-21 (13:35 e 13:39). É o caminho dos documentos acima do teto do VARCHAR,
    # se a etapa 8 o adotar (docs/OPEN_QUESTIONS.md).
    key = f"{s3_location.prefix}/redshift/super/documento.json"
    s3.put_object(Bucket=s3_location.bucket, Key=key, Body=json.dumps({"id": 4, "meta": json.loads(document)}).encode())
    loaded = outcome(lambda: session.execute(f"COPY {qualified} FROM 's3://{s3_location.bucket}/{key}' {session.credentials_clause()} FORMAT JSON 'auto'"))
    if loaded == "ok":
        kind, size = session.execute(f"select json_typeof(meta), json_size(meta) from {qualified} where id = 4")[0]
        loaded = f"ok: {kind}, json_size = {size}"
        assert kind == "object" and size > 65535, loaded
    record("redshift.super.copy_json_auto_above_65535", loaded)
    assert loaded.startswith("ok"), loaded


def test_unload_partition_by_and_register(redshift_session: RedshiftSession, s3_location: S3Location, duckdb_connection: duckdb.DuckDBPyConnection) -> None:
    """``UNLOAD ... PARTITION BY (mes) MANIFEST VERBOSE`` grava o layout do Delta; os arquivos entram por ``AddAction`` e o DuckDB os lê."""
    session = redshift_session
    name = session.table("unload")
    table = contract_table(name, session.schema_prefix(), month=True)
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
    unload = outcome(
        lambda: session.execute(
            f"UNLOAD ('{select}') TO '{destination}/' {session.credentials_clause()} FORMAT AS PARQUET PARTITION BY (mes) MANIFEST VERBOSE ALLOWOVERWRITE"
        )
    )
    record("redshift.unload.partition_by", unload)

    # PARTITION BY MANIFEST VERBOSE, que a documentação não lista, passou no ambiente alvo em
    # 2026-09-21 (examples/redshift_manifest.py). O pulo continua para o ambiente que recusar, mas
    # lá ele é regressão, não pergunta em aberto.
    if unload != "ok" and session.share_database:
        pytest.skip(f"UNLOAD ... PARTITION BY recusado no datashare {session.share_database}: {unload}")
    assert unload == "ok", unload

    s3 = boto3.client("s3")
    prefix = destination.removeprefix(f"s3://{s3_location.bucket}/")
    manifest = json.loads(s3.get_object(Bucket=s3_location.bucket, Key=f"{prefix}/manifest")["Body"].read())
    entries = manifest["entries"]
    assert sum(entry["meta"]["record_count"] for entry in entries) == 6
    record("redshift.unload.manifest_schema", manifest.get("schema"))
    record("redshift.unload.files", [entry["url"].removeprefix(f"{destination}/") for entry in entries])

    # O rodapé de um arquivo: os tipos físicos, a obrigatoriedade e as estatísticas que a AddAction usa.
    body = s3.get_object(Bucket=s3_location.bucket, Key=entries[0]["url"].removeprefix(f"s3://{s3_location.bucket}/"))["Body"].read()
    parquet = pq.ParquetFile(io.BytesIO(body))
    record("redshift.unload.physical_types", {parquet.schema.column(i).name: parquet.schema.column(i).physical_type for i in range(len(parquet.schema))})
    record("redshift.unload.schema", " ".join(str(parquet.schema).split()))
    statistics = parquet.metadata.row_group(0).column(0).statistics
    record("redshift.unload.has_min_max", bool(statistics and statistics.has_min_max))

    # Medido no ambiente alvo em 2026-09-21 (docs/POC.md): TIMESTAMP sai em INT96, obsoleto no
    # formato e sem estatística, e DECIMAL(18,2) em FIXED_LEN_BYTE_ARRAY, como o PyArrow grava e não
    # como grava o delta-rs. Toda coluna sai optional, inclusive as NOT NULL da origem.
    physical = {parquet.schema.column(i).name: parquet.schema.column(i).physical_type for i in range(len(parquet.schema))}
    assert physical["data_ref"] == "INT96", physical
    assert physical["valor"] == "FIXED_LEN_BYTE_ARRAY", physical
    assert statistics and statistics.has_min_max, "o UNLOAD deixou de gravar mínimo e máximo"

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

    # data_ref está declarada timestamp[us] na tabela Delta e INT96 no arquivo: os dois leitores
    # convertem e devolvem os valores intactos (sondagem de 2026-09-21, docs/POC.md).
    record("redshift.unload.delta_rs_read", outcome(lambda: DeltaTable(destination).to_pyarrow_table()))
    assert duckdb_connection.execute(f"SELECT count(*) FROM delta_scan('{destination}')").fetchone()[0] == 6

    # Onde o UNLOAD recusa gravar sem ALLOWOVERWRITE: o mesmo prefixo, um prefixo pai com arquivos
    # abaixo, e um subprefixo novo e vazio dentro de uma pasta com arquivos, que é o <uri>/<execution_id>/
    # da etapa 5. Leituras: a etapa 5 fixa o destino pelo que elas disserem.
    parent = s3_location.child("redshift/unload")
    for label, target in (("same_prefix", f"{destination}/"), ("parent_prefix", f"{parent}/"), ("new_subprefix", f"{destination}/segunda/")):
        result = outcome(lambda target=target: session.execute(f"UNLOAD ('{select}') TO '{target}' {session.credentials_clause()} FORMAT AS PARQUET PARTITION BY (mes)"))
        record(f"redshift.unload.destination.{label}", result)


def test_data_api_runs_the_statement_and_pages_the_result(redshift_session: RedshiftSession) -> None:
    """A Data API executa por HTTPS, assíncrona: cada célula é um dicionário de um item, e ``DECIMAL`` volta como texto.

    É o ciclo de ``examples/redshift_data_api.py``. O que ele prova é que existe caminho sem a porta
    5439; o que ele mostra é por que a troca de dados da biblioteca não passa por aqui.
    """
    workgroup = os.environ.get("SERIALIZE_DB_REDSHIFT_WORKGROUP")
    if not workgroup:
        pytest.skip("a Data API precisa de SERIALIZE_DB_REDSHIFT_WORKGROUP")

    session = redshift_session
    name = session.table("data_api")
    session.execute(f"CREATE TABLE {session.qualified(name)} (id BIGINT NOT NULL, valor DECIMAL(18,2), texto VARCHAR(20))")
    session.execute(f"INSERT INTO {session.qualified(name)} VALUES (1, 10.25, 'a'), (2, NULL, NULL)")

    parameters = {"Database": os.environ["SERIALIZE_DB_REDSHIFT_DATABASE"], "WorkgroupName": workgroup}
    client = boto3.client("redshift-data", region_name=os.environ.get("AWS_REGION") or boto3.Session().region_name)

    # 1. Dispara: a chamada volta na hora, com o identificador do statement.
    # A Data API abre a sessão dela no banco de Database, sem o USE desta conexão: nome em três partes.
    statement = client.execute_statement(Sql=f"select id, valor, texto from {session.fully_qualified(name)} order by id", **parameters)["Id"]

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
        rows.extend([None if cell.get("isNull") else next(iter(cell.values())) for cell in raw] for raw in page["Records"])

    assert columns == ["id", "valor", "texto"]
    assert [row[0] for row in rows] == [1, 2]
    assert rows[1][1] is None and rows[1][2] is None
    record("redshift.data_api.decimal_cell", repr(rows[0][1]))  # esperado: texto, não Decimal
    record("redshift.data_api.duration_ms", described.get("Duration", 0) // 1_000_000)


def test_parallel_copy_and_unload_on_two_connections(redshift_session: RedshiftSession, s3_location: S3Location) -> None:
    """Duas tabelas carregadas por ``COPY ... MANIFEST`` e descarregadas por ``UNLOAD`` em paralelo, uma conexão por thread.

    O ``redshift_connector`` declara ``threadsafety`` 1: a conexão da sessão não é compartilhada
    entre threads, e cada tarefa abre a sua pela mesma resolução de ``connect_redshift``, que pede
    uma credencial temporária por conexão. O motor da biblioteca guarda essa conexão num
    ``threading.local``.
    """
    session = redshift_session
    two_months = sample_table().slice(ROWS // 2 - 500, 1000)
    uris = [s3_location.child(f"redshift/paralelo_{k}") for k in range(2)]
    for uri in uris:
        write_deltalake(uri, two_months, mode="overwrite", partition_by=["mes"])
    manifests = [write_manifest(s3_location, f"redshift/manifest_paralelo_{k}.json", DeltaTable(uri)) for k, uri in enumerate(uris)]
    targets = [session.table(f"paralelo_{k}") for k in range(2)]
    for target in targets:
        session.execute(ddl(contract_table(target, session.schema_prefix())))

    def on_own_connection(sql: str, count_from: str | None = None) -> int:
        _, connection = connect_redshift()
        try:
            cursor = connection.cursor()
            cursor.execute(sql)
            if count_from is None:
                return 0
            cursor.execute(f"select count(*) from {count_from}")
            return cursor.fetchone()[0]
        finally:
            connection.close()

    # 1. Dois COPY em paralelo, em tabelas distintas: cada um numa conexão, limitados pelas slots do WLM.
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=2) as pool:
        counts = list(
            pool.map(
                lambda k: on_own_connection(
                    f"COPY {session.qualified(targets[k])} FROM '{manifests[k]}' {session.credentials_clause()} FORMAT AS PARQUET MANIFEST",
                    session.qualified(targets[k]),
                ),
                range(2),
            )
        )
    record("redshift.parallel.copy_two_tables", f"{time.perf_counter() - started:.1f} s")
    assert counts == [1000, 1000]

    # 2. Dois UNLOAD em paralelo, para prefixos distintos.
    destinations = [s3_location.child(f"redshift/unload_paralelo_{k}") for k in range(2)]
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=2) as pool:
        list(
            pool.map(
                lambda k: on_own_connection(
                    f"UNLOAD ('select * from {session.qualified(targets[k])}') TO '{destinations[k]}/' {session.credentials_clause()} FORMAT PARQUET"
                ),
                range(2),
            )
        )
    record("redshift.parallel.unload_two_tables", f"{time.perf_counter() - started:.1f} s")
    for destination in destinations:
        files = s3_location.data_files(destination)
        assert files and sum(pq.read_metadata(file).num_rows for file in files) == 1000
