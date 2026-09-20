"""O Redshift do projeto visto de dentro: como o ambiente o expõe, se responde e o que a sessão enxerga.

Uso:

    .venv/bin/python probes/redshift.py [s3://bucket/prefixo]

A raiz S3, pelo argumento ou por ``SERIALIZE_DB_TEST_S3_ROOT``, serve à simulação do papel do
``COPY`` e do ``UNLOAD`` sobre ela.

Só leitura: conecta e consulta visões de sistema, sem criar, alterar ou apagar nada no banco. Uma
credencial derivada da identidade IAM (``GetCredentials``, ``GetClusterCredentials``) cria o usuário
do banco quando ele ainda não existe; é o único efeito colateral possível, e a checagem RS-4 o aponta. O relatório sai no terminal e em
``probes/output/redshift_<data-hora>.txt``. Seções:

1. Configuração: as variáveis ``SERIALIZE_DB_REDSHIFT_*`` e a conexão Redshift do projeto
   (``sagemaker_studio``), de onde saem host, porta, banco, cluster ou workgroup.
2. APIs: ``redshift:DescribeClusters``, ``redshift-serverless:ListWorkgroups``, ``GetWorkgroup`` e
   ``GetNamespace``, com o papel IAM padrão que o ``COPY`` e o ``UNLOAD`` usam, o roteamento VPC e os
   nós; a Data API, pelo ciclo completo de ``examples/redshift_data_api.py`` com ``select 1``, como
   caminho alternativo quando a porta 5439 está fechada.
3. Rede: DNS dos endpoints regionais e TCP até o host.
4. Sessão: a credencial temporária do workgroup (``examples/redshift_native.py``), senha ou IAM do
   ``redshift_connector``; versão, usuário, banco, ``search_path``, esquemas, os bancos visíveis e em
   qual deles está o esquema do projeto, se o consumidor pode escrever num banco de datashare,
   privilégios no esquema e no banco (``CREATE``, ``TEMP``), tabelas com o prefixo da biblioteca,
   ``SUPER``, as configurações da sessão, ``stl_load_errors`` (o diagnóstico de um ``COPY``
   reprovado) e os esquemas externos.
5. Papel do ``COPY`` e do ``UNLOAD`` sobre a raiz S3, pela simulação de política do IAM.

Cada seção é uma função, na ordem acima, que documenta as checagens que emite (``RS-1`` a
``RS-17``). A seção 1 devolve um ``Target`` com os parâmetros de conexão reunidos; as seguintes o
recebem, e a seção 2 acrescenta a ele os papéis IAM padrão que a seção 5 simula. ``main`` chama as
seções uma a uma, e uma seção que quebra não cala as outras.

Variáveis: ``SERIALIZE_DB_REDSHIFT_HOST``, ``_PORT`` (5439), ``_DATABASE`` (banco da conexão),
``_SHARE_DATABASE`` (banco do datashare que guarda o esquema do projeto, quando não é o da conexão),
``_USER``, ``_PASSWORD``, ``_CLUSTER`` (identificador do cluster, autenticação IAM), ``_WORKGROUP``
(serverless, autenticação IAM), ``_SCHEMA`` (esquema do projeto), ``_IAM_ROLE`` (o papel do ``COPY``
e do ``UNLOAD`` na suíte) e ``_CONNECTION`` (nome da conexão do projeto; sem ela, a única conexão
Redshift). Sem variável nenhuma e sem conexão no projeto, nada é conectado. Códigos de saída: 0
checagens ok, 1 alguma chamada falhou, 2 alguma checagem reprovou.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import probelib  # noqa: E402
from probelib import (  # noqa: E402
    Report,
    connection_rows,
    describe_error,
    dns_rows,
    environment_rows,
    find_values,
    pretty,
    region,
    short_config,
    tabulate,
    tcp_open,
)

# Os sufixos das variáveis SERIALIZE_DB_REDSHIFT_*, na ordem em que a tabela os mostra.
VARIABLES = ("HOST", "PORT", "DATABASE", "SHARE_DATABASE", "USER", "PASSWORD", "CLUSTER", "WORKGROUP", "SCHEMA", "IAM_ROLE", "CONNECTION")

# As APIs do Redshift cujo endpoint regional a seção de rede resolve; sem endpoint VPC, dependem da internet.
SERVICES = ("redshift", "redshift-serverless", "redshift-data")

# O prefixo das tabelas que a biblioteca cria no esquema do projeto.
TABLE_PREFIX = "serialize_db"

# As chaves, em snake_case e em camelCase, com que a conexão do projeto guarda os parâmetros de conexão.
CONNECTION_KEYS = (
    "host", "port", "database_name", "databaseName", "workgroup_name", "workgroupName", "cluster_name", "clusterName",
    "cluster_identifier", "clusterIdentifier", "db_user", "dbUser", "username", "password", "jdbc_url", "jdbcUrl",
    "secret_arn", "secretArn",
)

# As ações que o COPY e o UNLOAD exigem do papel sobre a raiz S3.
COPY_ACTIONS = ["s3:ListBucket", "s3:GetObject", "s3:PutObject"]

# O patch mínimo da escrita em datashare (patch 186), por tipo de consumidor.
DATASHARE_WRITE_VERSION = {"serverless": (1, 0, 78890), "provisionado": (1, 0, 78881)}

# Os slices mínimos de um consumidor que escreve num datashare.
DATASHARE_WRITE_SLICES = 64

# O nível de isolamento que o banco precisa ter para receber escrita de outro warehouse.
DATASHARE_WRITE_ISOLATION = "snapshot"


def variable(name: str) -> str | None:
    """O valor de ``SERIALIZE_DB_REDSHIFT_<name>``, ou ``None`` quando ausente ou vazio."""
    return os.environ.get(f"SERIALIZE_DB_REDSHIFT_{name}") or None


@dataclass
class Target:
    """Parâmetros de conexão reunidos das variáveis e da conexão do projeto.

    ``source`` diz de onde vieram (``variáveis``, ``conexão <nome> do projeto`` ou ``nada``); ``roles`` recebe
    os papéis IAM padrão que a seção das APIs encontra, para a simulação do ``COPY``. ``database`` é o
    banco da conexão e ``share_database`` o banco que guarda o esquema do projeto quando ele vem de um
    datashare, caso em que toda tabela é citada por nome em três partes.
    """

    host: str | None = None
    port: int = 5439
    database: str | None = None
    share_database: str | None = None
    user: str | None = None
    password: str | None = None
    cluster: str | None = None
    workgroup: str | None = None
    schema: str | None = None
    source: str = "nada"
    roles: list[str] = field(default_factory=list)

    def qualified(self, name: str) -> str:
        """O nome da tabela como o SQL a cita: três partes quando o esquema está num banco de datashare."""
        parts = [part for part in (self.share_database, self.schema, name) if part]
        return ".".join(parts)

    def schema_database(self) -> str | None:
        """O banco que guarda o esquema do projeto: o do datashare, ou o da conexão."""
        return self.share_database or self.database


# ---------------------------------------------------------------------------------------------------------------
# Seção 1: a configuração


def target_from_connection(report: Report, target: Target, chosen: dict) -> None:
    """Preenche ``target`` com os dados da conexão Redshift do projeto, quando as variáveis não o fizeram."""
    # Os dados da conexão chegam como dicionário (probelib.PROJECT_PROBE); as chaves variam entre snake_case e
    # camelCase, e a URL JDBC traz host, porta e banco quando os campos diretos faltam.
    found = find_values(chosen, CONNECTION_KEYS)
    jdbc = re.match(r"jdbc:redshift\w*://([^:/]+):(\d+)/([^?;]+)", str(found.get("jdbc_url") or found.get("jdbcUrl") or ""))
    if target.source != "nada":
        return

    endpoint = (chosen.get("physical_endpoints") or [{}])[0]
    target.host = endpoint.get("host") or found.get("host") or (jdbc.group(1) if jdbc else None)
    target.port = int(endpoint.get("port") or found.get("port") or (jdbc.group(2) if jdbc else target.port))
    target.database = target.database or found.get("database_name") or found.get("databaseName") or (jdbc.group(3) if jdbc else None)
    target.workgroup = found.get("workgroup_name") or found.get("workgroupName")
    target.cluster = found.get("cluster_name") or found.get("clusterName") or found.get("cluster_identifier") or found.get("clusterIdentifier")
    target.user = target.user or found.get("db_user") or found.get("dbUser") or found.get("username")
    target.password = target.password or found.get("password")
    target.source = f"conexão {chosen.get('name')} do projeto"

    # A conexão criada no Studio guarda usuário e senha num secret; ler o secret é uma leitura, e a senha não é impressa.
    secret = found.get("secret_arn") or found.get("secretArn")
    if secret and not target.password:
        import boto3

        credentials = report.call(
            f"secretsmanager.get_secret_value(SecretId={secret!r})",
            lambda: json.loads(boto3.client("secretsmanager", region_name=region(), config=short_config()).get_secret_value(SecretId=secret)["SecretString"]),
            render=lambda loaded: f"chaves: {', '.join(sorted(loaded))}",
        )
        if credentials:
            target.user = credentials.get("username") or credentials.get("user") or target.user
            target.password = credentials.get("password") or target.password


def configuration(report: Report) -> Target:
    """Seção 1: ``RS-1``, de onde vem a conexão: as variáveis, a conexão Redshift do projeto, ou nada."""
    report.h1("Configuração")
    report.table([["variável", "valor"], *environment_rows([f"SERIALIZE_DB_REDSHIFT_{name}" for name in VARIABLES])])

    # As variáveis têm precedência: com host, cluster ou workgroup nelas, a conexão do projeto é só listada.
    target = Target(
        host=variable("HOST"),
        port=int(variable("PORT") or 5439),
        database=variable("DATABASE"),
        share_database=variable("SHARE_DATABASE"),
        user=variable("USER"),
        password=variable("PASSWORD"),
        cluster=variable("CLUSTER"),
        workgroup=variable("WORKGROUP"),
        schema=variable("SCHEMA"),
    )
    if target.host or target.cluster or target.workgroup:
        target.source = "variáveis"

    # As conexões do projeto, uma linha cada; a conexão Redshift escolhida é a de SERIALIZE_DB_REDSHIFT_CONNECTION ou a única.
    result = report.call("sagemaker_studio.Project().connections", probelib.project_snapshot, render=lambda found: f"lido com {found[1]}")
    if result is not None:
        data, _ = result
        connections = data.get("connections", [])
        report.table([["conexão", "tipo", "endpoint", "detalhe"], *connection_rows(connections)] if connections else [["(nenhuma conexão no projeto)"]])

        redshift = [item for item in connections if "REDSHIFT" in str(item.get("type", "")).upper()]
        wanted = variable("CONNECTION")
        if wanted:
            chosen = next((item for item in redshift if item.get("name") == wanted), None)
        else:
            chosen = redshift[0] if len(redshift) == 1 else None

        if chosen:
            report.line(pretty(chosen, limit=80))
            target_from_connection(report, target, chosen)
        elif len(redshift) > 1:
            report.line(f"{len(redshift)} conexões Redshift no projeto; informe SERIALIZE_DB_REDSHIFT_CONNECTION")
        elif not redshift:
            report.line("nenhuma conexão Redshift no projeto")

    for name in ("host", "port", "database", "share_database", "user", "cluster", "workgroup", "schema"):
        report.value(f"REDSHIFT_{name.upper()}", getattr(target, name))

    # O nome que a biblioteca escreve no SQL; com o esquema num datashare, são três partes.
    if target.schema:
        report.value("REDSHIFT_TABLE_NAME", target.qualified("<tabela>"))

    if target.source == "nada":
        report.note("RS-1", "conexão configurada", "nada: informe SERIALIZE_DB_REDSHIFT_* ou crie a conexão Redshift no projeto")
    else:
        report.ok("RS-1", "conexão configurada", f"por {target.source}")
    return target


# ---------------------------------------------------------------------------------------------------------------
# Seção 2: as APIs


def cluster_rows(clusters: dict) -> list[list[object]]:
    """Uma linha por cluster provisionado, com o papel IAM padrão do COPY e o roteamento VPC."""
    rows: list[list[object]] = [["cluster", "estado", "endpoint", "banco", "versão", "nós", "papel IAM padrão", "papéis IAM", "vpc", "roteamento VPC", "público", "criptografado"]]
    for cluster in clusters.get("Clusters", []):
        endpoint = cluster.get("Endpoint") or {}
        rows.append([
            cluster.get("ClusterIdentifier"),
            cluster.get("ClusterStatus"),
            f"{endpoint.get('Address')}:{endpoint.get('Port')}",
            cluster.get("DBName"),
            cluster.get("ClusterVersion"),
            f"{cluster.get('NumberOfNodes')} x {cluster.get('NodeType')}",
            cluster.get("DefaultIamRoleArn") or "-",
            ", ".join(role.get("IamRoleArn", "") for role in cluster.get("IamRoles", [])) or "-",
            cluster.get("VpcId"),
            cluster.get("EnhancedVpcRouting"),
            cluster.get("PubliclyAccessible"),
            cluster.get("Encrypted"),
        ])
    return rows


def workgroup_rows(workgroups: dict) -> list[list[object]]:
    """Uma linha por workgroup serverless, com o namespace, a capacidade e o roteamento VPC."""
    rows: list[list[object]] = [["workgroup", "estado", "endpoint", "namespace", "capacidade", "público", "roteamento VPC"]]
    for workgroup in workgroups.get("workgroups", []):
        endpoint = workgroup.get("endpoint") or {}
        rows.append([
            workgroup.get("workgroupName"),
            workgroup.get("status"),
            f"{endpoint.get('address')}:{endpoint.get('port')}",
            workgroup.get("namespaceName"),
            workgroup.get("baseCapacity"),
            workgroup.get("publiclyAccessible"),
            workgroup.get("enhancedVpcRouting"),
        ])
    return rows


def data_api_row(record: list[dict]) -> list[object]:
    """Uma linha da Data API: cada célula é um dicionário de um item, e ``isNull`` vira ``None``."""
    return [None if cell.get("isNull") else next(iter(cell.values())) for cell in record]


def data_api_select(client: object, parameters: dict, sql: str, timeout: float = 30.0) -> tuple[list[str], list[list[object]], int]:
    """O ciclo de ``examples/redshift_data_api.py``: dispara, espera o fim e pagina o resultado.

    Devolve as colunas, as linhas e a duração em milissegundos. Um estado final diferente de
    ``FINISHED`` e a espera estourada viram ``RuntimeError``, para a chamada entrar na seção final
    do relatório com o motivo.
    """
    statement = client.execute_statement(Sql=sql, **parameters)["Id"]

    deadline = time.monotonic() + timeout
    while True:
        described = client.describe_statement(Id=statement)
        if described["Status"] in ("FINISHED", "FAILED", "ABORTED"):
            break
        if time.monotonic() > deadline:
            raise RuntimeError(f"statement {statement} ainda em {described['Status']} após {timeout:.0f} s")
        time.sleep(0.5)

    if described["Status"] != "FINISHED":
        raise RuntimeError(f"{described['Status']}: {described.get('Error', 'erro sem detalhe')}")

    columns: list[str] = []
    rows: list[list[object]] = []
    for page in client.get_paginator("get_statement_result").paginate(Id=statement):
        columns = columns or [column["name"] for column in page["ColumnMetadata"]]
        rows.extend(data_api_row(record) for record in page["Records"])

    return columns, rows, described.get("Duration", 0) // 1_000_000


def apis(report: Report, target: Target) -> None:
    """Seção 2: ``RS-2`` (as APIs respondem), ``RS-6`` (papel IAM padrão para ``COPY`` e ``UNLOAD``) e ``RS-10`` (Data API)."""
    import boto3

    report.h1("APIs do Redshift")
    resolved = region()
    config = short_config()

    # Os clusters provisionados, com o papel IAM padrão de cada um.
    clusters = report.call("redshift.describe_clusters()", lambda: boto3.client("redshift", region_name=resolved, config=config).describe_clusters(), render=None)
    clusters_reason = report.last_reason if clusters is None else None
    if clusters is not None:
        rows = cluster_rows(clusters)
        report.table(rows if len(rows) > 1 else [["(nenhum cluster provisionado visível)"]])

    # Os workgroups serverless e, para cada um, o namespace, que guarda o banco e o papel IAM padrão.
    workgroups = report.call("redshift-serverless.list_workgroups()", lambda: boto3.client("redshift-serverless", region_name=resolved, config=config).list_workgroups(), render=None)
    workgroups_reason = report.last_reason if workgroups is None else None
    namespaces: dict[str, dict] = {}

    def read_namespace(name: str | None) -> None:
        """Lê o namespace de um workgroup: é ele que guarda o banco e o papel IAM padrão que RS-6 cobra."""
        if not name or name in namespaces:
            return
        namespace = report.call(
            f"redshift-serverless.get_namespace(namespaceName={name!r})",
            lambda: boto3.client("redshift-serverless", region_name=resolved, config=config).get_namespace(namespaceName=name)["namespace"],
            render=None,
        )
        if namespace:
            namespaces[name] = namespace
            report.table([
                ["namespace", "banco", "papel IAM padrão", "papéis IAM", "chave KMS"],
                [namespace.get("namespaceName"), namespace.get("dbName"), namespace.get("defaultIamRoleArn") or "-", ", ".join(namespace.get("iamRoles", [])) or "-", namespace.get("kmsKeyId") or "-"],
            ])

    if workgroups is not None:
        rows = workgroup_rows(workgroups)
        report.table(rows if len(rows) > 1 else [["(nenhum workgroup serverless visível)"]])
        for workgroup in workgroups.get("workgroups", []):
            read_namespace(workgroup.get("namespaceName"))

    # O workgroup configurado é lido direto: ListWorkgroups pode estar negada, e GetWorkgroup dá o
    # endereço e a porta que a conexão usa (examples/redshift_native.py, passo 1).
    if target.workgroup and not any(item.get("workgroupName") == target.workgroup for item in (workgroups or {}).get("workgroups", [])):
        found = report.call(
            f"redshift-serverless.get_workgroup(workgroupName={target.workgroup!r})",
            lambda: boto3.client("redshift-serverless", region_name=resolved, config=config).get_workgroup(workgroupName=target.workgroup)["workgroup"],
            render=None,
        )
        if found:
            report.table(workgroup_rows({"workgroups": [found]}))
            read_namespace(found.get("namespaceName"))
            endpoint = found.get("endpoint") or {}
            target.host = target.host or endpoint.get("address")
            target.port = int(endpoint.get("port") or target.port)

    # O endereço do workgroup listado também preenche o alvo, quando as variáveis não trouxeram host.
    for item in (workgroups or {}).get("workgroups", []):
        if item.get("workgroupName") == target.workgroup and not target.host:
            endpoint = item.get("endpoint") or {}
            target.host = endpoint.get("address")
            target.port = int(endpoint.get("port") or target.port)

    # RS-2: cada API que falhou é nomeada com o motivo; a autenticação por IAM depende delas.
    missing = [f"{api}: {why}" for api, why in (("redshift", clusters_reason), ("redshift-serverless", workgroups_reason)) if why]
    if not missing:
        report.ok("RS-2", "APIs do Redshift", "responderam")
    else:
        report.note("RS-2", "APIs do Redshift", "; ".join(missing) + "; a autenticação por IAM depende delas; ver a seção final")

    # RS-6: sem papel padrão, o COPY precisa de IAM_ROLE explícito; sem cluster nem workgroup, nada a ler.
    roles = [cluster.get("DefaultIamRoleArn") for cluster in (clusters or {}).get("Clusters", [])] + [namespace.get("defaultIamRoleArn") for namespace in namespaces.values()]
    roles = [role for role in roles if role]
    target.roles = roles
    if roles:
        report.ok("RS-6", "papel IAM padrão para COPY e UNLOAD", ", ".join(roles))
    elif (clusters or {}).get("Clusters") or (workgroups or {}).get("workgroups"):
        report.fail("RS-6", "papel IAM padrão para COPY e UNLOAD", "nenhum cluster ou workgroup tem papel padrão: o COPY precisará de IAM_ROLE explícito")
    elif not missing:
        report.note("RS-6", "papel IAM padrão para COPY e UNLOAD", "nenhum cluster ou workgroup visível: nada a ler")
    else:
        report.note("RS-6", "papel IAM padrão para COPY e UNLOAD", "não lido: " + "; ".join(missing))

    # Sem configuração, os nomes visíveis dizem ao leitor o que informar para conectar por IAM.
    if target.source == "nada":
        names = [cluster.get("ClusterIdentifier") for cluster in (clusters or {}).get("Clusters", [])] + [workgroup.get("workgroupName") for workgroup in (workgroups or {}).get("workgroups", [])]
        if names:
            report.line(f"para conectar por IAM, informe SERIALIZE_DB_REDSHIFT_CLUSTER ou _WORKGROUP com um de: {', '.join(str(name) for name in names)}, e SERIALIZE_DB_REDSHIFT_DATABASE")

    # RS-10: a Data API executa SQL por HTTPS, sem a porta 5439: o caminho de reserva se a rede fechar a
    # porta. O ciclo é o de examples/redshift_data_api.py, com select 1, que não lê dado nenhum do banco.
    if target.database and (target.workgroup or (target.cluster and target.user)):
        data = boto3.client("redshift-data", region_name=resolved, config=config)
        parameters = {"Database": target.database}
        if target.workgroup:
            parameters["WorkgroupName"] = target.workgroup
        else:
            parameters.update({"ClusterIdentifier": target.cluster, "DbUser": target.user})

        executed = report.call(
            f"redshift-data: execute_statement + describe_statement + get_statement_result ({parameters}) select 1",
            lambda: data_api_select(data, parameters, "select 1"),
            render=lambda found: f"{found[0]} = {found[1]} em {found[2]} ms",
        )
        report.note("RS-10", "Data API", "ciclo completo responde: caminho alternativo por HTTPS, sem a porta 5439" if executed is not None else f"{report.last_reason}; ver a seção final")
    else:
        report.note("RS-10", "Data API", "não testada: precisa de SERIALIZE_DB_REDSHIFT_DATABASE e de _WORKGROUP, ou de _CLUSTER com _USER")


# ---------------------------------------------------------------------------------------------------------------
# Seção 3: a rede


def network(report: Report, target: Target) -> None:
    """Seção 3: ``RS-14`` (as APIs têm endpoint VPC) e ``RS-3`` (TCP até o host)."""
    report.h1("Rede")
    resolved = region()
    names = [f"{service}.{resolved}.amazonaws.com" for service in SERVICES] if resolved else []
    if target.host:
        names.append(target.host)
    rows, private = dns_rows(names)
    report.table([["nome", "endereços", "tipo"], *rows])

    # RS-14: sem internet, as APIs só respondem por endpoint VPC de interface; a porta 5439 do cluster fica dentro da VPC.
    api_names = names[: len(SERVICES)]
    public_names = [name for name in api_names if not private.get(name)]
    if api_names and not public_names:
        report.ok("RS-14", "APIs do Redshift sem internet", "endpoints VPC de interface: a credencial temporária do workgroup e a Data API funcionam sem internet")
    elif api_names:
        report.note("RS-14", "APIs do Redshift sem internet", f"sem endpoint VPC: {', '.join(public_names)}; sem internet ou proxy, a credencial temporária (GetCredentials, GetClusterCredentials) e a Data API não respondem, e resta a conexão por senha na porta 5439")

    # RS-3: a porta do host abre; sem host conhecido, o endereço vem das APIs da seção 2.
    if target.host:
        opened = report.call(f"tcp {target.host}:{target.port}", lambda: tcp_open(target.host or "", target.port, 5), render=lambda seconds: f"conectou em {seconds:.2f} s")
        if opened is None:
            report.fail("RS-3", "host por TCP", f"{target.host}:{target.port} sem conexão; ver a seção final")
        else:
            report.ok("RS-3", "host por TCP", f"{target.host}:{target.port}")
    else:
        report.note("RS-3", "host por TCP", "sem host conhecido; o endereço vem de GetWorkgroup ou de DescribeClusters (seção 2)")


# ---------------------------------------------------------------------------------------------------------------
# Seção 4: a sessão


def render_rows(found: tuple[list[str], list[tuple]]) -> str:
    """O resultado de uma consulta como tabela alinhada, ou ``(nenhuma linha)``."""
    columns, rows = found
    return tabulate([columns, *[[str(value) for value in row] for row in rows]]) if rows else "(nenhuma linha)"


def column_value(columns: list[str], row: tuple, name: str) -> object | None:
    """O valor de uma coluna pelo nome, ou ``None`` quando a visão de sistema não tem essa coluna."""
    return row[columns.index(name)] if name in columns else None


def matching_rows(found: tuple[list[str], list[tuple]], **wanted: str) -> list[tuple]:
    """As linhas cujas colunas nomeadas casam com os valores dados, sem diferenciar maiúsculas.

    O filtro é feito aqui, e não no ``where`` da consulta, porque o nome das colunas varia entre as
    visões ``svv_all_*`` e uma coluna ausente derrubaria a consulta inteira.
    """
    columns, rows = found
    return [
        row
        for row in rows
        if all(str(column_value(columns, row, name) or "").lower() == value.lower() for name, value in wanted.items())
    ]


def version_tuple(text: str) -> tuple[int, ...] | None:
    """Os três números do patch em ``version()`` (``Redshift 1.0.78890``), ou ``None`` quando não estão lá."""
    found = re.search(r"Redshift (\d+)\.(\d+)\.(\d+)", text)
    return tuple(int(part) for part in found.groups()) if found else None


def credential_summary(credentials: dict) -> str:
    """O usuário e a expiração de uma credencial temporária; a senha nunca entra no relatório."""
    return f"dbUser={credentials.get('dbUser')} expira {credentials.get('expiration')}"


def datashare_write_verdict(version: tuple[int, ...] | None, kind: str, isolation: object | None, slices: int | None) -> tuple[str, str]:
    """Se o consumidor pode escrever no banco do datashare: patch mínimo, isolamento de snapshot e slices.

    Devolve ``("ok" | "fail" | "note", texto)``, um requisito por trecho. Um requisito que a sessão
    não leu entra como ``não lido`` e não reprova sozinho: leitura negada não é requisito reprovado.
    """
    minimum = DATASHARE_WRITE_VERSION.get(kind, DATASHARE_WRITE_VERSION["serverless"])
    checks: list[tuple[str, bool | None]] = [
        (
            f"patch {'.'.join(str(part) for part in version)} contra {'.'.join(str(part) for part in minimum)} ({kind})" if version else "patch",
            None if version is None else version >= minimum,
        ),
        (
            f"isolamento {isolation}" if isolation else "isolamento",
            None if isolation is None else DATASHARE_WRITE_ISOLATION in str(isolation).lower(),
        ),
        (
            f"{slices} slices contra {DATASHARE_WRITE_SLICES}" if slices is not None else "slices",
            None if slices is None else slices >= DATASHARE_WRITE_SLICES,
        ),
    ]
    text = "; ".join(f"{label}: {'atende' if ok else 'não atende' if ok is False else 'não lido'}" for label, ok in checks)
    if any(ok is False for _, ok in checks):
        return "fail", text
    if any(ok is None for _, ok in checks):
        return "note", text
    return "ok", text


def session(report: Report, target: Target) -> None:
    """Seção 4: ``RS-15`` (credencial temporária do workgroup), ``RS-4`` (a sessão abre), ``RS-16`` (o banco do
    esquema do projeto), ``RS-17`` (escrita num banco de datashare), ``RS-5`` (privilégios no esquema),
    ``RS-8`` (tabelas da biblioteca), ``RS-7`` (``SUPER``), ``RS-9`` (privilégios no banco), ``RS-12``
    (diagnóstico do ``COPY``) e ``RS-13`` (esquemas externos)."""
    report.h1("Sessão")
    if target.source == "nada":
        report.line("sem configuração, nada a conectar (RS-1)")
        report.note("RS-4", "sessão", "sem configuração, nada a conectar")
        return
    if not target.database:
        report.line("sem banco: informe SERIALIZE_DB_REDSHIFT_DATABASE")
        report.fail("RS-4", "sessão", "sem banco: informe SERIALIZE_DB_REDSHIFT_DATABASE")
        return
    import boto3
    import redshift_connector

    resolved = region()

    # RS-15: a credencial temporária do workgroup, o passo 2 de examples/redshift_native.py. O usuário
    # sai da identidade IAM (IAMR:<papel>), entra em PUBLIC e a senha dura no máximo uma hora; sem
    # durationSeconds seriam 900 segundos. A senha fica fora do relatório.
    temporary = False
    if not target.workgroup:
        report.note("RS-15", "credencial temporária do workgroup", "sem workgroup serverless: a credencial temporária é do serverless")
    elif target.user and target.password:
        report.note("RS-15", "credencial temporária do workgroup", "não pedida: usuário e senha vieram das variáveis ou da conexão do projeto")
    else:
        credentials = report.call(
            f"redshift-serverless.get_credentials(workgroupName={target.workgroup!r}, dbName={target.database!r}, durationSeconds=3600)",
            lambda: boto3.client("redshift-serverless", region_name=resolved, config=short_config()).get_credentials(
                workgroupName=target.workgroup, dbName=target.database, durationSeconds=3600
            ),
            render=credential_summary,
        )
        if credentials:
            target.user, target.password, temporary = credentials["dbUser"], credentials["dbPassword"], True
            report.ok("RS-15", "credencial temporária do workgroup", credential_summary(credentials))
        else:
            report.fail("RS-15", "credencial temporária do workgroup", f"{report.last_reason}; resta a conexão por senha ou o IAM do redshift_connector")

    # RS-4: a mesma resolução de tests/conftest.py. A credencial temporária com o endereço do
    # workgroup é o caminho testado no ambiente alvo; o IAM do redshift_connector, que pede a mesma
    # API por dentro, fica de reserva para quando ela não responde ou o endereço é desconhecido.
    def connect() -> tuple[str, object]:
        common = {"database": target.database, "timeout": 10}
        if target.host and target.user and target.password:
            label = "credencial temporária do workgroup" if temporary else "senha"
            return label, redshift_connector.connect(host=target.host, port=target.port, user=target.user, password=target.password, ssl=True, **common)
        if target.workgroup:
            return "IAM do redshift_connector (serverless)", redshift_connector.connect(iam=True, is_serverless=True, serverless_work_group=target.workgroup, region=resolved, **common)
        if target.cluster:
            return "IAM do redshift_connector (cluster)", redshift_connector.connect(iam=True, cluster_identifier=target.cluster, db_user=target.user, region=resolved, **common)
        raise RuntimeError("faltam parâmetros: host, usuário e senha, ou cluster ou workgroup para autenticação por IAM")

    result = report.call("redshift_connector.connect(...)", connect, render=lambda found: f"conectou por {found[0]}")
    if result is None:
        report.fail("RS-4", "sessão", "conexão falhou; ver a seção final")
        return
    method, connection = result
    connection.autocommit = True
    created_user = method.startswith("IAM") or method.startswith("credencial")
    report.ok("RS-4", "sessão", f"aberta por {method}" + ("; a credencial derivada da identidade IAM pode ter criado o usuário do banco" if created_user else ""))

    def query(sql: str, params: tuple = ()) -> tuple[list[str], list[tuple]]:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        return [column[0] for column in cursor.description or []], rows

    # A versão: o patch (1.0.NNNNN) diz quais recursos existem: MERGE, SUPER, o COPY de Parquet com
    # FILLRECORD e a escrita num banco de datashare (RS-17).
    version = report.call("select version()", lambda: query("select version()"), render=render_rows)
    patch = version_tuple(str(version[1][0][0])) if version and version[1] else None
    if version and version[1]:
        report.value("REDSHIFT_VERSION", ".".join(str(part) for part in patch) if patch else str(version[1][0][0])[:80])

    report.call("select current_user, current_database(), current_schema()", lambda: query("select current_user, current_database(), current_schema()"), render=render_rows)
    report.call("show search_path", lambda: query("show search_path"), render=render_rows)
    report.call(
        "esquemas visíveis",
        lambda: query("select nspname, pg_get_userbyid(nspowner) as owner from pg_namespace where nspname not like 'pg_%%' and nspname <> 'information_schema' order by 1"),
        render=render_rows,
    )

    # RS-16: os bancos que a sessão enxerga e em qual deles está o esquema do projeto. Um banco de tipo
    # shared vem de datashare, e as suas tabelas só são citadas por nome em três partes.
    databases = report.call("svv_redshift_databases", lambda: query("select * from svv_redshift_databases order by 1"), render=render_rows)
    schema = target.schema
    schema_kind: str | None = None
    schema_database: str | None = None
    if not schema:
        report.note("RS-16", "banco do esquema do projeto", "sem SERIALIZE_DB_REDSHIFT_SCHEMA")
    else:
        found = report.call(
            f"svv_all_schemas, linhas do esquema {schema!r}",
            lambda: query("select * from svv_all_schemas"),
            render=lambda loaded: render_rows((loaded[0], matching_rows(loaded, schema_name=schema))),
        )
        places = [
            (str(column_value(found[0], row, "database_name")), str(column_value(found[0], row, "schema_type") or "?"))
            for row in (matching_rows(found, schema_name=schema) if found else [])
        ]
        listed = ", ".join(f"{database} ({kind})" for database, kind in places)
        if found is None:
            report.note("RS-16", "banco do esquema do projeto", f"svv_all_schemas: {report.last_reason}")
        elif not places:
            report.fail("RS-16", "banco do esquema do projeto", f"{schema} não aparece em svv_all_schemas: a sessão não o enxerga")
        else:
            schema_database, schema_kind = places[0]
            if len(places) > 1:
                report.note("RS-16", "banco do esquema do projeto", f"{schema} aparece em mais de um banco: {listed}; SERIALIZE_DB_REDSHIFT_SHARE_DATABASE decide qual")
            elif target.share_database and schema_database.lower() != target.share_database.lower():
                report.fail("RS-16", "banco do esquema do projeto", f"SERIALIZE_DB_REDSHIFT_SHARE_DATABASE diz {target.share_database} e a leitura diz {listed}")
            elif schema_kind.lower() == "shared":
                report.ok("RS-16", "banco do esquema do projeto", f"{listed}: nome em três partes")
            else:
                report.ok("RS-16", "banco do esquema do projeto", f"{listed}: nome em duas partes")

            # Sem a variável, o alvo toma o banco da leitura, para o nome em três partes sair certo daqui para baixo.
            if schema_kind.lower() == "shared" and not target.share_database:
                target.share_database = schema_database
                report.line(f"SERIALIZE_DB_REDSHIFT_SHARE_DATABASE ausente; a leitura diz {schema_database}")
            report.value("REDSHIFT_TABLE_NAME", target.qualified("<tabela>"))

    # RS-17: escrever num banco de datashare exige o patch 186 (1.0.78890 no serverless, 1.0.78881 no
    # provisionado), isolamento de snapshot no banco que recebe a escrita e 64 slices ou mais no
    # consumidor. Sem um deles, o CREATE, o COPY e o INSERT da publicação são recusados.
    if not schema_kind:
        report.note("RS-17", "escrita no banco do datashare", "banco do esquema não lido (RS-16)")
    elif schema_kind.lower() != "shared":
        report.note("RS-17", "escrita no banco do datashare", f"o esquema do projeto é {schema_kind} em {schema_database}: as regras do datashare não se aplicam")
    else:
        rows = matching_rows(databases, database_name=schema_database) if databases and schema_database else []
        isolation = column_value(databases[0], rows[0], "database_isolation_level") if databases and rows else None
        slices = report.call("select count(*) from stv_slices", lambda: query("select count(*) from stv_slices"), render=render_rows)
        status, verdict = datashare_write_verdict(
            patch,
            "serverless" if target.workgroup else "provisionado",
            isolation,
            slices[1][0][0] if slices and slices[1] else None,
        )
        getattr(report, status)("RS-17", "escrita no banco do datashare", verdict)

    # RS-5 e RS-8: no esquema do projeto, USAGE e CREATE, e quantas tabelas já têm o prefixo da
    # biblioteca. has_schema_privilege e svv_table_info só enxergam o banco local: num esquema de
    # datashare quem concede é o produtor, e a lista de tabelas vem de svv_all_tables.
    if not schema:
        report.note("RS-5", "privilégios no esquema", "sem SERIALIZE_DB_REDSHIFT_SCHEMA")
        report.note("RS-8", "tabelas com o prefixo da biblioteca no esquema", "sem SERIALIZE_DB_REDSHIFT_SCHEMA")
    else:
        if schema_kind and schema_kind.lower() == "shared":
            report.note(
                "RS-5",
                "privilégios no esquema",
                f"{schema} vem do datashare {schema_database}: has_schema_privilege só alcança o banco local, e quem concede USAGE e CREATE é o produtor; o primeiro CREATE da suíte é o teste",
            )
            found = report.call(
                f"svv_all_tables, linhas de {schema_database}.{schema}",
                lambda: query("select * from svv_all_tables"),
                render=lambda loaded: render_rows((loaded[0], matching_rows(loaded, database_name=schema_database or "", schema_name=schema))),
            )
            names = [
                str(column_value(found[0], row, "table_name"))
                for row in (matching_rows(found, database_name=schema_database or "", schema_name=schema) if found else [])
            ]
        else:
            privileges = report.call(
                f"has_schema_privilege({schema!r}, USAGE | CREATE)",
                lambda: query("select has_schema_privilege(%s, 'USAGE') as usage, has_schema_privilege(%s, 'CREATE') as create", (schema, schema)),
                render=render_rows,
            )
            if privileges and privileges[1]:
                usage, create = privileges[1][0]
                if usage and create:
                    report.ok("RS-5", "privilégios no esquema", f"{schema}: USAGE e CREATE")
                else:
                    report.fail("RS-5", "privilégios no esquema", f"{schema}: USAGE={usage}, CREATE={create}")
            else:
                report.fail("RS-5", "privilégios no esquema", f"{schema}: não lidos; ver a seção final")

            found = report.call(
                f"svv_table_info do esquema {schema!r}",
                lambda: query('select "table", tbl_rows, size as size_mb, diststyle, sortkey1 from svv_table_info where schema = %s order by 1', (schema,)),
                render=render_rows,
            )
            names = [str(row[0]) for row in found[1]] if found else []

        where = f"{schema_database}.{schema}" if schema_kind and schema_kind.lower() == "shared" else schema
        if found is None:
            report.note("RS-8", "tabelas com o prefixo da biblioteca no esquema", f"não lidas: {report.last_reason}")
        else:
            mine = [name for name in names if name.startswith(TABLE_PREFIX)]
            report.note("RS-8", "tabelas com o prefixo da biblioteca no esquema", f"{len(mine)} de {len(names)} tabelas em {where}")

    # RS-7: o tipo SUPER e JSON_PARSE, que a coluna JSON do contrato usa no Redshift.
    parsed = report.call("select json_parse(...)  (SUPER)", lambda: query("select json_parse(%s) as super_value", ('{"a": 1}',)), render=render_rows)
    if parsed is not None:
        report.ok("RS-7", "SUPER e JSON_PARSE", "disponíveis")
    else:
        report.note("RS-7", "SUPER e JSON_PARSE", "falhou; ver a seção final")

    # As configurações que mudam o comportamento do SQL gerado: datas, fuso, tempo limite e sensibilidade a maiúsculas.
    report.call(
        "pg_settings (datestyle, timezone, statement_timeout, search_path, enable_case_sensitive_identifier)",
        lambda: query("select name, setting from pg_settings where name in ('datestyle', 'timezone', 'statement_timeout', 'search_path', 'enable_case_sensitive_identifier', 'wlm_query_slot_count') order by 1"),
        render=render_rows,
    )
    report.call("pg_user do usuário atual", lambda: query("select usename, usesuper, usecreatedb from pg_user where usename = current_user"), render=render_rows)

    # RS-9: CREATE no banco permite um esquema externo; TEMP permite a staging temporária.
    privileges = report.call(
        "has_database_privilege(current_database(), CREATE | TEMP)",
        lambda: query("select has_database_privilege(current_database(), 'CREATE') as create_db, has_database_privilege(current_database(), 'TEMP') as temp_db"),
        render=render_rows,
    )
    if privileges and privileges[1]:
        create_db, temp_db = privileges[1][0]
        report.note("RS-9", "privilégios no banco", f"CREATE={create_db} (esquema externo {'possível' if create_db else 'impossível'}), TEMP={temp_db} (staging temporária {'possível' if temp_db else 'impossível'})")
    else:
        report.note("RS-9", "privilégios no banco", "não lidos; ver a seção final")

    # RS-12: um COPY reprovado explica o motivo em stl_load_errors (ou sys_load_error_detail); sem leitura, o diagnóstico
    # depende do administrador.
    errors = report.call("stl_load_errors dos últimos 30 dias", lambda: query("select count(*) from stl_load_errors where starttime > dateadd(day, -30, getdate())"), render=render_rows)
    if errors is not None:
        report.note("RS-12", "diagnóstico do COPY", f"stl_load_errors legível: {errors[1][0][0]} erro(s) de carga em 30 dias")
    else:
        detail = report.call("sys_load_error_detail dos últimos 30 dias", lambda: query("select count(*) from sys_load_error_detail where start_time > dateadd(day, -30, getdate())"), render=render_rows)
        report.note("RS-12", "diagnóstico do COPY", "sys_load_error_detail legível" if detail is not None else "nem stl_load_errors nem sys_load_error_detail legíveis: o motivo de um COPY reprovado virá do administrador")

    # RS-13: a biblioteca não usa esquemas externos; a contagem mostra se o Glue chegou ao Redshift.
    external = report.call("svv_external_schemas", lambda: query("select count(*) from svv_external_schemas"), render=render_rows)
    if external is not None:
        report.note("RS-13", "esquemas externos (Spectrum)", f"{external[1][0][0]} no banco; a biblioteca não os usa, e a contagem mostra se o Glue chegou ao Redshift")

    connection.close()


# ---------------------------------------------------------------------------------------------------------------
# Seção 5: o papel do COPY


def copy_role(report: Report, target: Target) -> None:
    """Seção 5: ``RS-11``, se o papel padrão do ``COPY`` e do ``UNLOAD`` alcança a raiz S3 informada, pela simulação de política do IAM."""
    import boto3

    report.h1("Papel do COPY e do UNLOAD sobre a raiz S3")
    root = (sys.argv[1] if len(sys.argv) > 1 else os.environ.get("SERIALIZE_DB_TEST_S3_ROOT", "")).rstrip("/")
    if not root.startswith("s3://"):
        report.line("sem raiz: informe s3://bucket/prefixo como argumento ou em SERIALIZE_DB_TEST_S3_ROOT")
        report.note("RS-11", "papel do COPY sobre a raiz", "sem raiz: informe s3://bucket/prefixo como argumento ou em SERIALIZE_DB_TEST_S3_ROOT")
        return
    if not target.roles:
        report.line("sem papel padrão conhecido (RS-6): nada a simular")
        report.note("RS-11", "papel do COPY sobre a raiz", "sem papel padrão conhecido (RS-6): o COPY precisará de IAM_ROLE explícito, e a suíte Redshift é o teste")
        return

    # Uma simulação por papel: ListBucket no bucket, GetObject e PutObject sob a raiz.
    bucket, _, prefix = root.removeprefix("s3://").partition("/")
    resources = [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/{prefix}/*" if prefix else f"arn:aws:s3:::{bucket}/*"]
    iam = boto3.client("iam", config=short_config())

    def render_decisions(found: dict) -> str:
        return tabulate([["ação", "decisão"], *[[item["EvalActionName"], item["EvalDecision"]] for item in found.get("EvaluationResults", [])]])

    for role in target.roles:
        found = report.call(
            f"iam.simulate_principal_policy({role}: ListBucket, GetObject, PutObject em {root})",
            lambda r=role: iam.simulate_principal_policy(PolicySourceArn=r, ActionNames=COPY_ACTIONS, ResourceArns=resources),
            render=render_decisions,
        )
        if found is None:
            report.note("RS-11", "papel do COPY sobre a raiz", f"{role}: simulação {report.last_reason}; o primeiro COPY da suíte Redshift é o teste")
            continue
        denied = [item["EvalActionName"] for item in found.get("EvaluationResults", []) if item["EvalDecision"] != "allowed"]
        if denied:
            report.fail("RS-11", "papel do COPY sobre a raiz", f"{role}: {', '.join(denied)} negadas sob {root}")
        else:
            report.ok("RS-11", "papel do COPY sobre a raiz", f"{role}: ListBucket, GetObject e PutObject sob {root}")


# ---------------------------------------------------------------------------------------------------------------
# main


def main() -> int:
    report = Report("redshift", "o Redshift do projeto visto de dentro")
    target = Target()
    for section in (configuration, apis, network, session, copy_role):
        try:
            # A seção 1 produz o Target; as demais o recebem.
            result = section(report) if section is configuration else section(report, target)
            if section is configuration and isinstance(result, Target):
                target = result
        except Exception as error:  # noqa: BLE001 - uma seção interrompida não cala as outras
            report.line(f"!! seção {section.__name__} interrompida: {describe_error(error)}")
            report.failures.append((f"seção {section.__name__}", describe_error(error)))
    return report.finish()


if __name__ == "__main__":
    sys.exit(main())
