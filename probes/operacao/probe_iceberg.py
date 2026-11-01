"""Sonda do Glue como catálogo de tabelas Iceberg e Delta, lidas pelo Athena e pelo Redshift.

A sonda faz as leituras do ambiente alvo que a avaliação do suporte a Iceberg de 2026-10-31 pede
antes da implementação (``.claude/memory/decisions.md``, "The target probed before Iceberg is
implemented (2026-11-01)"), sem pacote novo: o Glue e o Athena pelo ``boto3`` e o Redshift pela
conexão de ``serialize_db.engine.redshift.connect``. Ela:

1. cria no Glue o banco ``serialize_db_operacao_iceberg_<id>``, com o local na raiz de trabalho, e
   imprime o ``CreateTableDefaultPermissions`` que ele recebeu;
2. cria nele a tabela ``sonda_versao`` e a atualiza duas vezes pelo ``UpdateTable``, as duas com o
   ``VersionId`` lido antes da primeira;
3. cria pelo Athena a tabela Iceberg ``lancamentos``, particionada por ``data_base``, com o local
   na raiz de trabalho, grava duas linhas, apaga uma e lê os arquivos da tabela em ``$files``, as
   linhas e o registro dela no Glue;
4. com ``--delta``, registra pelo Athena a tabela Delta da URI como ``delta_tabela``, só pelo
   local, e conta as linhas dela pelo Athena e pelo ``delta_scan`` do DuckDB;
5. no Redshift, lê ``data_catalog_auto_mount``, os esquemas de ``awsdatacatalog``, as linhas de
   ``lancamentos`` e, com ``--delta``, a contagem de ``delta_tabela``, e tenta um ``INSERT`` em
   ``lancamentos``; com a leitura de ``lancamentos`` recusada e
   ``SERIALIZE_DB_REDSHIFT_SHARE_DATABASE`` definida, repete as três primeiras numa sessão sem o
   ``USE`` do banco do datashare.

Checagens:

- o banco criado no Glue;
- o segundo ``UpdateTable`` recusado com ``ConcurrentModificationException``;
- o ``DELETE`` do Athena com um arquivo de deleção (``content`` 1) em ``$files``, e uma linha na
  tabela;
- com ``--delta``, a contagem do Athena igual à do ``delta_scan``;
- ``lancamentos`` lida pelo Redshift com uma linha, na sessão da biblioteca;
- o banco da sonda apagado do Glue no fim.

Leituras: o ``CreateTableDefaultPermissions``, o registro de cada tabela no Glue (o tipo, o local,
o ``VersionId``, ``IsRegisteredWithLakeFormation`` e os parâmetros, com o ``metadata_location`` do
Iceberg), a configuração do workgroup, o local do resultado, os bytes lidos e o tempo de cada
consulta do Athena, e cada resposta do Redshift, o ``INSERT`` incluído, que a documentação recusa
à identidade federada.

A sonda grava sob a raiz de trabalho, no S3, e no banco novo do Glue, que ela apaga no fim pelo
``DeleteTable`` e pelo ``DeleteDatabase`` do Glue, nunca pelo ``DROP`` do Athena: o Glue não toca
os arquivos de uma tabela, e a pasta da tabela Delta, que a sonda só lê, fica intacta.
``SERIALIZE_DB_TEST_KEEP`` mantém o banco e a raiz. O Athena grava o resultado de cada consulta
no local do workgroup quando ele impõe a própria configuração (``EnforceWorkGroupConfiguration``),
e a sonda imprime esse local sem apagá-lo. Sem ``--workgroup``, a sonda toma o único workgroup
habilitado do Athena SQL fora do ``primary``; com nenhum ou com mais de um, ou com a raiz das
suítes fora do S3, para com o código 2 antes de criar o banco.

Chamadas: ``glue:CreateDatabase``, ``GetDatabase``, ``CreateTable``, ``GetTable``, ``GetTables``,
``UpdateTable``, ``DeleteTable``, ``DeleteDatabase``; ``athena:ListWorkGroups``, ``GetWorkGroup``,
``StartQueryExecution``, ``GetQueryExecution``, ``GetQueryResults``, ``StopQueryExecution``; as
leituras e as escritas do S3 sob a raiz de trabalho e no local de resultados do Athena, e as
leituras da tabela Delta; ``redshift-serverless:GetWorkgroup`` e ``GetCredentials`` e as
consultas no Redshift.

Exemplo:

.. code-block:: shell

    export SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo
    export SERIALIZE_DB_REDSHIFT_WORKGROUP=workgroup SERIALIZE_DB_REDSHIFT_SCHEMA=esquema
    .venv/bin/python probes/operacao/probe_iceberg.py --delta s3://bucket/base/prd/cad_contas
"""

from __future__ import annotations

import argparse
import dataclasses
import os
import sys
import time
from typing import Any

import boto3
import botocore.config
import botocore.exceptions
import duckdb
import operation_lib as lib
import redshift_connector

from serialize_db.engine import redshift
from serialize_db.engine.redshift import RedshiftConfig
from serialize_db.errors import ContractError
from serialize_db.resources import environment_limits
from serialize_db.schema import literal
from serialize_db.storage import Storage

VERSION_TABLE = "sonda_versao"
ICEBERG_TABLE = "lancamentos"
DELTA_TABLE = "delta_tabela"
CATALOG = "awsdatacatalog"
# A espera de cada consulta do Athena, lida a cada segundo; passado o prazo, a sonda a para.
ATHENA_TIMEOUT_SECONDS = 600
ATHENA_POLL_SECONDS = 1.0
# As esperas curtas do probelib.short_config: sem rede, o padrão do boto3 espera 60 s por
# tentativa.
BOTO_CONFIG = botocore.config.Config(
    connect_timeout=5,
    read_timeout=30,
    retries={"total_max_attempts": 2, "mode": "standard"},
)
AWS_ERRORS = (botocore.exceptions.BotoCoreError, botocore.exceptions.ClientError)
REDSHIFT_ERRORS = (redshift_connector.Error, OSError)


@dataclasses.dataclass
class AthenaResult:
    """Uma consulta do Athena encerrada: o estado, o motivo de uma falha e as linhas, sem a de
    cabeçalho."""

    state: str
    reason: str
    rows: list[list[str | None]]


@dataclasses.dataclass
class Answer:
    """A resposta de um comando no Redshift: as linhas, ou ``None`` e a mensagem do erro."""

    rows: list[tuple] | None
    error: str = ""


# ---------------------------------------------------------------- o que a sonda decide do que lê


def describe(
    error: Exception,
) -> str:
    """O erro numa linha, com a classe."""
    return f"{type(error).__name__}: {error}"


def error_code(
    error: Exception,
) -> str:
    """O código de um erro do serviço (``ClientError``) ou a classe de um erro local."""
    if isinstance(error, botocore.exceptions.ClientError):
        return error.response.get("Error", {}).get("Code", type(error).__name__)
    return type(error).__name__


def result_rows(
    result_set: dict,
) -> list[list[str | None]]:
    """As linhas de um ``ResultSet`` do ``GetQueryResults`` como texto, sem a de cabeçalho, com
    ``None`` no valor nulo, que o Athena devolve sem ``VarCharValue``."""
    labels = []
    for column in result_set.get("ResultSetMetadata", {}).get("ColumnInfo", []):
        labels.append(column.get("Label"))
    rows = []
    for row in result_set.get("Rows", []):
        values = []
        for datum in row.get("Data", []):
            values.append(datum.get("VarCharValue"))
        rows.append(values)
    # A primeira linha do resultado de um SELECT repete os nomes das colunas.
    if rows and rows[0] == labels:
        rows = rows[1:]
    return rows


def sql_workgroups(
    summaries: list[dict],
) -> list[str]:
    """Os workgroups habilitados do Athena SQL fora do ``primary``, pelos resumos do
    ``ListWorkGroups``; um workgroup do Spark tem o motor ``PySpark engine version 3``."""
    names = []
    for summary in summaries:
        engine = summary.get("EngineVersion", {}).get("EffectiveEngineVersion", "")
        if summary.get("State") != "ENABLED" or summary.get("Name") == "primary":
            continue
        if engine.startswith("Athena"):
            names.append(summary["Name"])
    return names


def permissions_reading(
    database: dict,
) -> str:
    """O que o ``CreateTableDefaultPermissions`` do banco, na resposta do ``GetDatabase``, diz de
    quem governa as tabelas novas."""
    permissions = database.get("CreateTableDefaultPermissions")
    if permissions is None:
        return "o Glue não devolveu CreateTableDefaultPermissions"
    for permission in permissions:
        principal = permission.get("Principal", {}).get("DataLakePrincipalIdentifier")
        if principal == "IAM_ALLOWED_PRINCIPALS":
            return "as tabelas novas ficam só com as permissões do IAM (IAM_ALLOWED_PRINCIPALS)"
    return "o Lake Formation governa as tabelas novas (sem IAM_ALLOWED_PRINCIPALS)"


def version_lock_problems(
    first: str | None,
    second: str | None,
) -> list[str]:
    """Os problemas da trava do ``VersionId``, pelo código de erro de cada ``UpdateTable``, ou
    ``None`` quando aceito: o primeiro aceito e o segundo, com o ``VersionId`` velho, recusado com
    ``ConcurrentModificationException``."""
    problems = []
    if first is not None:
        problems.append(f"o primeiro UpdateTable recusado com {first}")
    if second is None:
        problems.append(
            "o segundo UpdateTable, com o VersionId velho, aceito: o Glue não serializa os commits"
        )
    elif second != "ConcurrentModificationException":
        problems.append(f"o segundo UpdateTable recusado com {second}")
    return problems


def delete_file_problems(
    files: list[list[str | None]],
    rows: list[list[str | None]],
) -> list[str]:
    """Os problemas da tabela do Athena depois do ``DELETE``: ``files``, as linhas de ``$files``
    com ``content``, ``file_path`` e ``record_count``, sem arquivo de deleção (``content`` 1), e
    ``rows``, as linhas da tabela, diferentes da única de ``id_lancamento`` 1."""
    problems = []
    contents = []
    for file in files:
        contents.append(file[0])
    if "1" not in contents:
        problems.append(f"nenhum arquivo de deleção (content 1) em $files: content {contents}")
    ids = []
    for row in rows:
        ids.append(row[0])
    if ids != ["1"]:
        problems.append(f"a tabela com id_lancamento {ids}, não só 1")
    return problems


def redshift_select_problems(
    answer: Answer,
) -> list[str]:
    """Os problemas da leitura de ``lancamentos`` pelo Redshift: a leitura recusada, ou linhas
    diferentes da única de ``id_lancamento`` 1, que a deleção do Athena deixa."""
    if answer.rows is None:
        return [f"não lida: {answer.error}"]
    ids = []
    for row in answer.rows:
        ids.append(row[0])
    if ids != [1]:
        return [f"lida com id_lancamento {ids}, não só 1"]
    return []


# ---------------------------------------------------------------- o Athena e o Redshift


def wait_for_query(
    athena: Any,
    query_id: str,
) -> dict:
    """O ``QueryExecution`` da consulta encerrada, lido a cada ``ATHENA_POLL_SECONDS``; passado
    ``ATHENA_TIMEOUT_SECONDS``, a sonda para a consulta e devolve o estado lido logo depois."""
    deadline = time.monotonic() + ATHENA_TIMEOUT_SECONDS
    execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
    while execution["Status"]["State"] in ("QUEUED", "RUNNING"):
        if time.monotonic() > deadline:
            athena.stop_query_execution(QueryExecutionId=query_id)
            print(f"   consulta parada depois de {ATHENA_TIMEOUT_SECONDS} s")
            return athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
        time.sleep(ATHENA_POLL_SECONDS)
        execution = athena.get_query_execution(QueryExecutionId=query_id)["QueryExecution"]
    return execution


def run_athena(
    athena: Any,
    workgroup: str,
    output: str,
    sql: str,
) -> AthenaResult:
    """Roda ``sql`` no workgroup do Athena e espera o fim; imprime o comando, o estado, o tempo,
    os bytes lidos, o local do resultado e as linhas. ``output`` é o local de resultados pedido,
    que o workgroup troca pelo dele quando impõe a própria configuração; a recusa do serviço sai
    como ``FAILED``."""
    print(f"athena> {sql}")
    started = time.perf_counter()
    try:
        query_id = athena.start_query_execution(
            QueryString=sql,
            WorkGroup=workgroup,
            ResultConfiguration={"OutputLocation": output},
        )["QueryExecutionId"]
        execution = wait_for_query(athena, query_id)
        status = execution["Status"]
        rows = []
        if status["State"] == "SUCCEEDED":
            response = athena.get_query_results(QueryExecutionId=query_id, MaxResults=1000)
            rows = result_rows(response["ResultSet"])
    except AWS_ERRORS as error:
        print(f"   recusada: {describe(error)}")
        return AthenaResult("FAILED", describe(error), [])

    # O desfecho, com o local onde o Athena gravou o resultado.
    seconds = time.perf_counter() - started
    scanned = execution.get("Statistics", {}).get("DataScannedInBytes")
    location = execution.get("ResultConfiguration", {}).get("OutputLocation")
    print(
        f"   {status['State']} em {seconds:.1f} s, {scanned} bytes lidos, resultado em {location}"
    )
    reason = status.get("StateChangeReason", "")
    if reason:
        print(f"   motivo: {reason}")
    for row in rows:
        print("   |", " | ".join(str(value) for value in row))
    return AthenaResult(status["State"], reason, rows)


def print_workgroup(
    athena: Any,
    workgroup: str,
) -> None:
    """Imprime a configuração do workgroup que decide onde ficam os resultados: o motor,
    ``EnforceWorkGroupConfiguration`` e o ``OutputLocation``; a leitura negada é impressa."""
    try:
        found = athena.get_work_group(WorkGroup=workgroup)["WorkGroup"]
    except AWS_ERRORS as error:
        print(f"athena.get_work_group({workgroup}): {describe(error)}")
        return
    configuration = found.get("Configuration", {})
    engine = configuration.get("EngineVersion", {}).get("EffectiveEngineVersion")
    enforced = configuration.get("EnforceWorkGroupConfiguration")
    location = configuration.get("ResultConfiguration", {}).get("OutputLocation")
    print(
        f"workgroup {workgroup}: {engine}, EnforceWorkGroupConfiguration {enforced}, "
        f"OutputLocation {location}"
    )


def choose_workgroup(
    athena: Any,
) -> str | None:
    """O único workgroup habilitado do Athena SQL fora do ``primary``, pelo ``ListWorkGroups``;
    com nenhum, com mais de um ou com a lista negada, ``None``."""
    try:
        summaries = athena.list_work_groups()["WorkGroups"]
    except AWS_ERRORS as error:
        print(f"athena.list_work_groups(): {describe(error)}")
        return None
    names = sql_workgroups(summaries)
    print(f"workgroups habilitados do Athena SQL fora do primary: {names}")
    if len(names) == 1:
        return names[0]
    return None


def print_glue_table(
    glue: Any,
    database: str,
    name: str,
) -> None:
    """Imprime o registro da tabela no Glue: o tipo, o local, o ``VersionId``,
    ``IsRegisteredWithLakeFormation`` e os parâmetros, onde fica o ``metadata_location`` do
    Iceberg; a leitura negada é impressa."""
    try:
        table = glue.get_table(DatabaseName=database, Name=name)["Table"]
    except AWS_ERRORS as error:
        print(f"glue.get_table({name}): {describe(error)}")
        return
    location = table.get("StorageDescriptor", {}).get("Location")
    print(
        f"glue {name}: TableType {table.get('TableType')}, local {location}, VersionId "
        f"{table.get('VersionId')}, IsRegisteredWithLakeFormation "
        f"{table.get('IsRegisteredWithLakeFormation')}"
    )
    for key, value in sorted(table.get("Parameters", {}).items()):
        print(f"   {key} = {value}")


def redshift_statement(
    cursor: Any,
    sql: str,
) -> Answer:
    """Roda ``sql`` no cursor do Redshift e devolve as linhas, ou o erro; imprime o comando e o
    desfecho."""
    print(f"redshift> {sql}")
    try:
        cursor.execute(sql)
        rows = []
        if cursor.description:
            for row in cursor.fetchall():
                rows.append(tuple(row))
    except REDSHIFT_ERRORS as error:
        print(f"   erro: {describe(error)}")
        return Answer(None, describe(error))
    for row in rows:
        print("   |", row)
    print(f"   ok, {len(rows)} linha(s)")
    return Answer(rows)


def open_session(
    config: RedshiftConfig,
    label: str,
) -> Any:
    """A conexão de ``serialize_db.engine.redshift.connect`` pela configuração, ou ``None`` com o
    erro impresso; ``label`` nomeia a sessão na saída."""
    print(f"sessão {label}")
    try:
        return redshift.connect(config)
    except (ContractError, *AWS_ERRORS, *REDSHIFT_ERRORS) as error:
        print(f"   conexão recusada: {describe(error)}")
        return None


def close_session(
    connection: Any,
) -> None:
    """Fecha a conexão; a que caiu antes é impressa."""
    try:
        connection.close()
    except REDSHIFT_ERRORS as error:
        print(f"   fechamento: {describe(error)}")


def delta_count(
    uri: str,
) -> int:
    """As linhas da tabela Delta pelo ``delta_scan`` do DuckDB, numa conexão com os limites do
    ambiente."""
    connection = Storage.for_uri(uri).duckdb_connect(config=environment_limits())
    try:
        query = f"SELECT count(*) FROM delta_scan({literal(uri)})"
        return int(connection.execute(query).fetchone()[0])
    finally:
        connection.close()


# ---------------------------------------------------------------- as seções


def create_glue_database(
    glue: Any,
    database: str,
    location: str,
) -> bool:
    """Seção 1, o banco novo no Glue: o ``CreateDatabase`` com o local na raiz de trabalho e o
    ``CreateTableDefaultPermissions`` que o ``GetDatabase`` devolve; devolve se a sonda criou o
    banco, que só então ela apaga no fim. Checagem: o banco criado."""
    print(f"\n# 1. O banco novo no Glue: {database}, local {location}")
    problems = []
    created = False
    try:
        glue.create_database(DatabaseInput={"Name": database, "LocationUri": location})
        created = True
        found = glue.get_database(Name=database)["Database"]
        print("CreateTableDefaultPermissions:", found.get("CreateTableDefaultPermissions"))
        print("leitura:", permissions_reading(found))
    except AWS_ERRORS as error:
        problems.append(describe(error))
    lib.check("o banco novo criado no Glue", problems)
    return created


def version_lock(
    glue: Any,
    database: str,
) -> None:
    """Seção 2, a trava do ``VersionId``: a tabela ``sonda_versao`` atualizada duas vezes pelo
    ``UpdateTable`` com o ``VersionId`` lido antes da primeira, e o estado final. Checagem: o
    primeiro aceito e o segundo recusado com ``ConcurrentModificationException``."""
    print("\n# 2. A trava do VersionId do UpdateTable")
    title = "o segundo UpdateTable com o VersionId velho recusado (ConcurrentModificationException)"
    table = {"Name": VERSION_TABLE, "TableType": "EXTERNAL_TABLE", "Parameters": {"passo": "0"}}
    try:
        glue.create_table(DatabaseName=database, TableInput=table)
        found = glue.get_table(DatabaseName=database, Name=VERSION_TABLE)["Table"]
    except AWS_ERRORS as error:
        lib.check(title, [f"a tabela {VERSION_TABLE} não criada: {describe(error)}"])
        return
    version = found.get("VersionId")
    print(f"{VERSION_TABLE} criada com VersionId {version}")

    # Os dois passos levam o mesmo VersionId, o de antes do primeiro: o segundo é o escritor que
    # leu a tabela antes do commit de outro.
    outcomes = []
    for step in ("1", "2"):
        table["Parameters"] = {"passo": step}
        try:
            glue.update_table(DatabaseName=database, TableInput=table, VersionId=version)
        except AWS_ERRORS as error:
            outcomes.append(error_code(error))
            print(f"UpdateTable do passo {step} com VersionId {version}: {describe(error)}")
            continue
        outcomes.append(None)
        print(f"UpdateTable do passo {step} com VersionId {version}: aceito")

    # O estado final diz qual passo ficou na tabela.
    try:
        final = glue.get_table(DatabaseName=database, Name=VERSION_TABLE)["Table"]
        passo = final.get("Parameters", {}).get("passo")
        print(f"depois: VersionId {final.get('VersionId')}, passo {passo}")
    except AWS_ERRORS as error:
        print(f"glue.get_table({VERSION_TABLE}): {describe(error)}")
    lib.check(title, version_lock_problems(outcomes[0], outcomes[1]))


def athena_iceberg(
    athena: Any,
    glue: Any,
    database: str,
    workgroup: str,
    root: str,
) -> bool:
    """Seção 3, a tabela Iceberg do Athena: o ``CREATE TABLE`` com o local na raiz de trabalho, o
    ``INSERT`` de duas linhas, o ``DELETE`` de uma, ``$files``, as linhas e o registro no Glue;
    devolve se a tabela foi criada. Checagem: um arquivo de deleção e uma linha."""
    print("\n# 3. A tabela Iceberg do Athena com uma deleção")
    title = f"o DELETE do Athena com um arquivo de deleção em $files e uma linha em {ICEBERG_TABLE}"
    output = f"{root}/athena/"
    name = f"{database}.{ICEBERG_TABLE}"
    location = f"{root}/iceberg/{ICEBERG_TABLE}/"
    create = (
        f"CREATE TABLE {name} (id_lancamento bigint, data_base string, valor decimal(18, 2)) "
        f"PARTITIONED BY (data_base) LOCATION '{location}' "
        "TBLPROPERTIES ('table_type' = 'ICEBERG')"
    )
    result = run_athena(athena, workgroup, output, create)
    if result.state != "SUCCEEDED":
        lib.check(title, [f"CREATE TABLE {result.state}: {result.reason}"])
        return False

    # As duas linhas e a deleção de uma, cada comando um commit da tabela.
    changes = [
        f"INSERT INTO {name} VALUES (1, '2026-08-31', 1.00), (2, '2026-08-31', 2.00)",
        f"DELETE FROM {name} WHERE id_lancamento = 2",
    ]
    for sql in changes:
        result = run_athena(athena, workgroup, output, sql)
        if result.state != "SUCCEEDED":
            lib.check(title, [f"{sql.split()[0]} {result.state}: {result.reason}"])
            return True

    # Os arquivos que o snapshot atual registra, as linhas e o registro no Glue.
    files_sql = (
        f'SELECT content, file_path, record_count FROM "{database}"."{ICEBERG_TABLE}$files" '
        "ORDER BY content, file_path"
    )
    files = run_athena(athena, workgroup, output, files_sql)
    rows_sql = f"SELECT id_lancamento, data_base, valor FROM {name} ORDER BY id_lancamento"
    rows = run_athena(athena, workgroup, output, rows_sql)
    print_glue_table(glue, database, ICEBERG_TABLE)
    problems = []
    for read in (files, rows):
        if read.state != "SUCCEEDED":
            problems.append(f"SELECT {read.state}: {read.reason}")
    if not problems:
        problems = delete_file_problems(files.rows, rows.rows)
    lib.check(title, problems)
    return True


def athena_delta(
    athena: Any,
    glue: Any,
    database: str,
    workgroup: str,
    root: str,
    uri: str,
) -> bool:
    """Seção 4, a tabela Delta lida pelo Athena: o ``CREATE EXTERNAL TABLE`` só pelo local da
    tabela de ``uri``, a contagem pelo Athena e pelo ``delta_scan`` e o registro no Glue; devolve
    se a tabela foi registrada. Checagem: as duas contagens iguais."""
    print(f"\n# 4. A tabela Delta lida pelo Athena: {uri}")
    title = f"{DELTA_TABLE} com as mesmas linhas pelo Athena e pelo delta_scan"
    output = f"{root}/athena/"
    name = f"{database}.{DELTA_TABLE}"
    create = (
        f"CREATE EXTERNAL TABLE {name} LOCATION '{uri}/' TBLPROPERTIES ('table_type' = 'DELTA')"
    )
    result = run_athena(athena, workgroup, output, create)
    if result.state != "SUCCEEDED":
        lib.check(title, [f"CREATE EXTERNAL TABLE {result.state}: {result.reason}"])
        return False
    counted = run_athena(athena, workgroup, output, f"SELECT count(*) FROM {name}")
    print_glue_table(glue, database, DELTA_TABLE)

    # A contagem da biblioteca, pelo leitor que a ingestão usa.
    try:
        expected = delta_count(uri)
    except duckdb.Error as error:
        lib.check(title, [f"delta_scan: {describe(error)}"])
        return True
    print(f"delta_scan: {expected} linhas")
    problems = []
    if counted.state != "SUCCEEDED":
        problems.append(f"SELECT count(*) {counted.state}: {counted.reason}")
    elif counted.rows != [[str(expected)]]:
        problems.append(f"o Athena contou {counted.rows}, o delta_scan {expected}")
    lib.check(title, problems)
    return True


def redshift_catalog(
    database: str,
    with_iceberg: bool,
    with_delta: bool,
) -> None:
    """Seção 5, o Redshift pelo ``awsdatacatalog``, na sessão da biblioteca: o
    ``data_catalog_auto_mount``, os esquemas do catálogo, as linhas de ``lancamentos``, a
    contagem de ``delta_tabela`` e o ``INSERT``, este uma leitura; com a leitura recusada e o
    banco do datashare na configuração, as três primeiras numa sessão sem o ``USE``. Checagem:
    ``lancamentos`` com uma linha."""
    print(f"\n# 5. O Redshift pelo {CATALOG}")
    title = f"{ICEBERG_TABLE} lida pelo Redshift no {CATALOG} com uma linha, a deleção aplicada"
    iceberg = f'"{CATALOG}"."{database}"."{ICEBERG_TABLE}"'
    readings = [
        "SHOW data_catalog_auto_mount",
        "SELECT database_name, schema_name, schema_type FROM svv_all_schemas "
        f"WHERE database_name = '{CATALOG}' ORDER BY schema_name",
    ]
    select = f"SELECT id_lancamento, data_base, valor FROM {iceberg} ORDER BY id_lancamento"
    insert_sql = f"INSERT INTO {iceberg} VALUES (3, '2026-08-31', 3.00)"
    delta_table = f'"{CATALOG}"."{database}"."{DELTA_TABLE}"'
    config = RedshiftConfig.from_environment()
    connection = open_session(config, "da biblioteca, a de serialize_db.engine.redshift.connect")
    if connection is None:
        lib.check(title, ["a sessão não abriu"])
        return

    # As leituras na sessão da biblioteca; o INSERT vem por último, depois de toda leitura.
    answer = Answer(None, "a tabela Iceberg não foi criada no Athena")
    try:
        cursor = connection.cursor()
        for sql in readings:
            redshift_statement(cursor, sql)
        if with_iceberg:
            answer = redshift_statement(cursor, select)
        if with_delta:
            redshift_statement(cursor, f"SELECT count(*) FROM {delta_table}")
        if with_iceberg:
            insert = redshift_statement(cursor, insert_sql)
            if insert.rows is None:
                print("leitura: o Redshift recusou o INSERT no Iceberg")
            else:
                print("leitura: o Redshift gravou no Iceberg; a tabela depois do INSERT:")
                redshift_statement(cursor, select)
    finally:
        close_session(connection)

    # Uma sessão sem o USE separa a recusa do catálogo da do banco do datashare.
    if with_iceberg and answer.rows is None and config.share_database:
        plain = dataclasses.replace(config, share_database=None)
        connection = open_session(plain, f"sem o USE de {config.share_database}")
        if connection is not None:
            try:
                cursor = connection.cursor()
                for sql in [*readings, select]:
                    redshift_statement(cursor, sql)
            finally:
                close_session(connection)
    lib.check(title, redshift_select_problems(answer))


def delete_glue_database(
    glue: Any,
    database: str,
) -> None:
    """Apaga as tabelas do banco da sonda e o banco pelo Glue, que não toca os arquivos das
    tabelas; ``SERIALIZE_DB_TEST_KEEP`` os mantém. Checagem: o banco apagado."""
    print(f"\n# Limpeza do banco {database}")
    if os.environ.get(lib.KEEP_VARIABLE):
        print(f"banco mantido por {lib.KEEP_VARIABLE}: {database}")
        return
    problems = []
    try:
        names = []
        for page in glue.get_paginator("get_tables").paginate(DatabaseName=database):
            for table in page["TableList"]:
                names.append(table["Name"])
        for name in names:
            glue.delete_table(DatabaseName=database, Name=name)
        glue.delete_database(Name=database)
        print(f"banco apagado, com as tabelas {names}")
    except AWS_ERRORS as error:
        problems.append(describe(error))
    lib.check(f"o banco {database} apagado do Glue", problems)


def parse_arguments() -> argparse.Namespace:
    """A linha de comando: ``delta``, a URI ``s3://`` de uma tabela Delta que o Athena e o
    Redshift leem, e ``workgroup``, o workgroup do Athena; o uso errado sai com o código 2."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--delta",
        metavar="URI",
        help="uma tabela Delta no S3, que a sonda só lê, como a de cad_contas da base",
    )
    parser.add_argument(
        "--workgroup",
        help="o workgroup do Athena; sem ele, o único do Athena SQL habilitado fora do primary",
    )
    options = parser.parse_args()
    if options.delta is not None and not options.delta.startswith("s3://"):
        parser.error(f"--delta {options.delta}: o Athena lê só do S3")
    if options.delta is not None:
        options.delta = options.delta.rstrip("/")
    return options


def main() -> None:
    """As seções na ordem, com o relatório no terminal e em ``probes/output/``; o banco do Glue
    sai no fim, também quando uma seção para numa exceção."""
    options = parse_arguments()
    db = lib.work_database("iceberg")
    if not db.storage.is_s3:
        message = "o Athena e o Glue leem do S3: a sonda pede SERIALIZE_DB_TEST_S3_ROOT"
        print(message, file=sys.stderr)
        sys.exit(2)
    root = db.storage.uri
    database = f"serialize_db_operacao_iceberg_{root.rsplit('-', 1)[-1]}"
    glue = boto3.client("glue", config=BOTO_CONFIG)
    athena = boto3.client("athena", config=BOTO_CONFIG)

    # O workgroup antes do banco: sem ele, a sonda para sem ter criado nada.
    workgroup = options.workgroup or choose_workgroup(athena)
    if workgroup is None:
        print("a sonda pede --workgroup: nenhum ou mais de um workgroup candidato", file=sys.stderr)
        sys.exit(2)
    print_workgroup(athena, workgroup)

    # Sem o banco, nenhuma seção roda: finish imprime a checagem reprovada e encerra.
    if not create_glue_database(glue, database, f"{root}/"):
        lib.finish(db)
    try:
        version_lock(glue, database)
        with_iceberg = athena_iceberg(athena, glue, database, workgroup, root)
        with_delta = False
        if options.delta is not None:
            with_delta = athena_delta(athena, glue, database, workgroup, root, options.delta)
        redshift_catalog(database, with_iceberg, with_delta)
    finally:
        delete_glue_database(glue, database)
    lib.finish(db)


if __name__ == "__main__":
    lib.run(main)
