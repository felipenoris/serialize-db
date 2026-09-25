"""O Redshift do projeto visto de dentro: como o ambiente o expõe, se responde e o que a sessão vê.

Uso:

    .venv/bin/python probes/redshift.py [s3://bucket/prefixo]

A raiz S3, pelo argumento, por ``SERIALIZE_DB_ROOT`` ou por ``SERIALIZE_DB_TEST_S3_ROOT``
(``probelib.s3_root``), serve à simulação de quem alcança o S3 no ``COPY`` e no ``UNLOAD``.

O probe só lê: conecta, consulta visões de sistema e troca o banco da sessão com ``USE``, sem criar,
alterar ou apagar nada no banco. A credencial temporária do workgroup (``GetCredentials``) cria o
usuário do banco quando ele ainda não existe; é o único efeito colateral possível, e a checagem
``RS-4`` o aponta. O relatório sai no terminal e em ``probes/output/redshift_<data-hora>.txt``.

A conexão repete ``examples/redshift_native.py``, executado no ambiente alvo em 2026-09-20:
``redshift-serverless:GetWorkgroup`` dá o endereço e a porta, ``GetCredentials`` dá o par usuário e
senha derivado da identidade IAM, e ``redshift_connector.connect`` abre a sessão com esse par. Um
par informado em ``_USER`` e ``_PASSWORD``, ou lido do secret da conexão do projeto, entra na mesma
chamada. O IAM interno do ``redshift_connector`` e o cluster provisionado não são caminhos deste
probe: ninguém os executou no ambiente alvo, que não tem cluster.

Seções:

1. Configuração: as variáveis ``SERIALIZE_DB_REDSHIFT_*`` e a conexão Redshift do projeto
   (``sagemaker_studio``), de onde saem host, porta, banco e workgroup.
2. APIs: ``redshift-serverless:GetWorkgroup`` do workgroup configurado, ``ListWorkgroups`` e
   ``GetNamespace``, com o papel IAM padrão e os associados; ``redshift:DescribeClusters`` como
   fotografia, porque o ambiente alvo não tem cluster; a Data API, pelo ciclo completo de
   ``examples/redshift_data_api.py`` com ``select 1``, o caminho alternativo quando a porta 5439
   está fechada.
3. Rede: DNS dos endpoints regionais e TCP até o host.
4. Sessão: a credencial temporária do workgroup; versão, usuário, banco, ``search_path``, esquemas,
   ``SUPER``, as configurações da sessão, privilégios no banco da conexão (``CREATE``, ``TEMP``),
   ``sys_load_error_detail`` (o diagnóstico de um ``COPY`` reprovado) e os esquemas externos; os
   bancos visíveis e em qual deles está o esquema do projeto, os requisitos da escrita num banco de
   datashare; o ``USE`` nesse banco (``examples/redshift_copy_unload.py``) e, depois dele, os
   privilégios no esquema e as tabelas com o prefixo da biblioteca.
5. Quem alcança o S3 no ``COPY`` e no ``UNLOAD``: as credenciais de quem chama, ou o papel de
   ``SERIALIZE_DB_REDSHIFT_IAM_ROLE``, e o alcance sobre a raiz pela simulação de política do IAM.

Cada seção é uma função, na ordem acima, que documenta as checagens que emite (``RS-1`` a
``RS-19``). A seção 1 devolve um ``Target`` com os parâmetros de conexão reunidos; as seguintes o
recebem, e a seção 2 acrescenta a ele os papéis IAM do namespace que a seção 5 consulta. ``main``
chama as seções uma a uma, e uma seção que quebra não cala as outras.

As leituras que descrevem o banco da conexão e a sessão vêm antes do ``USE``; depois dele o banco da
sessão é o do datashare, e só o que precisa desse banco roda lá.

Variáveis, todas com o prefixo do projeto: ``SERIALIZE_DB_REDSHIFT_WORKGROUP`` (o workgroup
serverless: endereço por ``GetWorkgroup`` e credencial por ``GetCredentials``), ``_DATABASE`` (banco
da conexão), ``_SHARE_DATABASE`` (banco do datashare que guarda o esquema do projeto, quando não é o
da conexão: a sessão roda ``USE`` nele), ``_SCHEMA`` (esquema do projeto), ``_HOST``, ``_PORT``
(5439), ``_USER`` e ``_PASSWORD`` (o par informado, na mesma chamada), ``_IAM_ROLE`` (o papel do
``COPY`` e do ``UNLOAD`` na suíte, ou ``default``; sem ela, as credenciais de quem chama) e
``_CONNECTION`` (nome da conexão do projeto; sem ela, a única conexão Redshift). Sem variável
nenhuma e sem conexão no projeto, nada é conectado. Códigos de saída: 0 checagens ok, 1 alguma
chamada falhou, 2 alguma checagem reprovou.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import NamedTuple

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

# Os sufixos das variáveis SERIALIZE_DB_REDSHIFT_*, na ordem em que a tabela os mostra: primeiro os
# do caminho executado.
VARIABLES = ("WORKGROUP", "DATABASE", "SHARE_DATABASE", "SCHEMA", "HOST", "PORT", "USER", "PASSWORD", "IAM_ROLE", "CONNECTION")

# As APIs do Redshift cujo endpoint regional a seção de rede resolve; sem endpoint VPC, elas
# dependem da internet.
SERVICES = ("redshift", "redshift-serverless", "redshift-data")

# O prefixo das tabelas que a biblioteca cria no esquema do projeto.
TABLE_PREFIX = "serialize_db"

# As chaves, em snake_case e em camelCase, com que a conexão do projeto guarda seus parâmetros.
CONNECTION_KEYS = (
    "host", "port", "database_name", "databaseName", "workgroup_name", "workgroupName", "db_user", "dbUser", "username",
    "password", "jdbc_url", "jdbcUrl", "secret_arn", "secretArn",
)

# As ações que o COPY e o UNLOAD exigem de quem alcança a raiz S3.
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

    ``source`` diz de onde vieram (``variáveis``, ``conexão <nome> do projeto`` ou ``nada``).
    ``database`` é o banco da conexão e ``share_database`` o banco que guarda o esquema do projeto
    quando ele vem de um datashare: a sessão roda ``USE`` nele e cita ``esquema.tabela``, e o nome
    em três partes fica para uma sessão aberta em outro banco, como a da Data API. ``iam_role`` é o
    papel de ``SERIALIZE_DB_REDSHIFT_IAM_ROLE``; ``namespace_roles`` recebe da seção das APIs os
    papéis IAM padrão e os associados ao namespace, ou fica ``None`` quando ela não os leu, para a
    seção 5 dizer quem alcança o S3.
    """

    host: str | None = None
    port: int = 5439
    database: str | None = None
    share_database: str | None = None
    user: str | None = None
    password: str | None = None
    workgroup: str | None = None
    schema: str | None = None
    iam_role: str | None = None
    source: str = "nada"
    namespace_roles: tuple[list[str], list[str]] | None = None

    def qualified(self, name: str) -> str:
        """O nome como a sessão o cita depois do ``USE``.

        É ``esquema.tabela``, ou só a tabela quando o alvo não tem esquema.
        """
        return ".".join(part for part in (self.schema, name) if part)

    def fully_qualified(self, name: str) -> str:
        """O nome em três partes de uma sessão aberta em outro banco: ``banco.esquema.tabela``."""
        return ".".join(part for part in (self.schema_database(), self.schema, name) if part)

    def schema_database(self) -> str | None:
        """O banco que guarda o esquema do projeto: o do datashare, ou o da conexão."""
        return self.share_database or self.database


# --------------------------------------------------------------------------------------------------
# Seção 1: a configuração


def target_from_connection(report: Report, target: Target, chosen: dict) -> None:
    """Preenche ``target`` com os dados da conexão Redshift do projeto.

    Nada muda quando as variáveis já preencheram o alvo (``target.source`` diferente de ``nada``).
    """
    # Os dados da conexão chegam como dicionário (probelib.PROJECT_PROBE); as chaves variam entre
    # snake_case e camelCase, e a URL JDBC traz host, porta e banco quando os campos diretos faltam.
    found = find_values(chosen, CONNECTION_KEYS)
    jdbc = re.match(r"jdbc:redshift\w*://([^:/]+):(\d+)/([^?;]+)", str(found.get("jdbc_url") or found.get("jdbcUrl") or ""))
    if target.source != "nada":
        return

    # Cada campo toma a primeira fonte que o traz: a variável, o endpoint, o campo direto e a URL
    # JDBC; a porta da variável, ou 5439, só vale sem as outras três.
    endpoint = (chosen.get("physical_endpoints") or [{}])[0]
    target.host = endpoint.get("host") or found.get("host") or (jdbc.group(1) if jdbc else None)
    target.port = int(endpoint.get("port") or found.get("port") or (jdbc.group(2) if jdbc else target.port))
    target.database = target.database or found.get("database_name") or found.get("databaseName") or (jdbc.group(3) if jdbc else None)
    target.workgroup = found.get("workgroup_name") or found.get("workgroupName")
    target.user = target.user or found.get("db_user") or found.get("dbUser") or found.get("username")
    target.password = target.password or found.get("password")
    target.source = f"conexão {chosen.get('name')} do projeto"

    # A conexão criada no Studio guarda usuário e senha num secret; o probe lê o secret e imprime só
    # as chaves, nunca a senha.
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
    """Seção 1, a configuração.

    Checagens: ``RS-1`` (a origem da conexão: as variáveis, a conexão Redshift do projeto ou nada).
    """
    report.h1("Configuração")
    report.table([["variável", "valor"], *environment_rows([f"SERIALIZE_DB_REDSHIFT_{name}" for name in VARIABLES])])

    # As variáveis têm precedência: com workgroup ou host nelas, a conexão do projeto é só listada.
    target = Target(
        host=variable("HOST"),
        port=int(variable("PORT") or 5439),
        database=variable("DATABASE"),
        share_database=variable("SHARE_DATABASE"),
        user=variable("USER"),
        password=variable("PASSWORD"),
        workgroup=variable("WORKGROUP"),
        schema=variable("SCHEMA"),
        iam_role=variable("IAM_ROLE"),
    )
    if target.host or target.workgroup:
        target.source = "variáveis"

    # As conexões do projeto, uma linha cada, e a conexão Redshift escolhida: a de
    # SERIALIZE_DB_REDSHIFT_CONNECTION ou a única. Fora de um espaço do Studio, o pacote
    # sagemaker_studio não existe, e a falha é leitura.
    result = report.call("sagemaker_studio.Project().connections", probelib.project_snapshot, render=lambda found: f"lido com {found[1]}", expected=True)
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

    for name in ("workgroup", "host", "port", "database", "share_database", "user", "schema", "iam_role"):
        report.value(f"REDSHIFT_{name.upper()}", getattr(target, name))

    # O nome que a biblioteca escreve no SQL depois do USE, e o nome em três partes de uma sessão
    # aberta em outro banco.
    if target.schema:
        report.value("REDSHIFT_TABLE_NAME", target.qualified("<tabela>"))
        if target.schema_database():
            report.value("REDSHIFT_TABLE_FULL_NAME", target.fully_qualified("<tabela>"))

    # RS-1: a conexão vem das variáveis ou da conexão do projeto; sem as duas, a checagem é leitura.
    if target.source == "nada":
        report.note("RS-1", "conexão configurada", "nada: informe SERIALIZE_DB_REDSHIFT_* ou crie a conexão Redshift no projeto")
    else:
        report.ok("RS-1", "conexão configurada", f"por {target.source}")
    return target


# --------------------------------------------------------------------------------------------------
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


def iam_roles(clusters: dict | None, namespaces: dict[str, dict]) -> tuple[list[str], list[str]]:
    """Os papéis IAM que ``IAM_ROLE`` aceitaria: os padrão e os apenas associados, sem repetição.

    Um papel só serve ao ``COPY`` quando está associado ao cluster ou ao namespace, e o padrão é o
    que ``IAM_ROLE default`` usa. Sem nenhum associado, nem um ARN explícito funciona, e ``RS-6``
    aponta esse caso.
    """
    # Os papéis de cada cluster provisionado e de cada namespace serverless.
    defaults, attached = [], []
    for cluster in (clusters or {}).get("Clusters", []):
        defaults.append(cluster.get("DefaultIamRoleArn"))
        attached += [role.get("IamRoleArn") for role in cluster.get("IamRoles", [])]
    for namespace in namespaces.values():
        defaults.append(namespace.get("defaultIamRoleArn"))
        attached += list(namespace.get("iamRoles", []))

    # Os papéis padrão saem da lista dos associados, e cada lista perde os vazios e as repetições.
    defaults = [role for role in defaults if role]
    attached = [role for role in attached if role and role not in defaults]
    return list(dict.fromkeys(defaults)), list(dict.fromkeys(attached))


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

    # A espera consulta o estado a cada 0,5 s, até um estado final ou o fim do prazo.
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

    # As páginas do resultado; os nomes das colunas vêm da primeira.
    columns: list[str] = []
    rows: list[list[object]] = []
    for page in client.get_paginator("get_statement_result").paginate(Id=statement):
        columns = columns or [column["name"] for column in page["ColumnMetadata"]]
        rows.extend(data_api_row(record) for record in page["Records"])

    return columns, rows, described.get("Duration", 0) // 1_000_000


def apis(report: Report, target: Target) -> None:
    """Seção 2, as APIs.

    Checagens: ``RS-2`` (a API do serverless responde), ``RS-6`` (papel IAM do namespace para
    ``COPY`` e ``UNLOAD``) e ``RS-10`` (Data API).
    """
    import boto3

    report.h1("APIs do Redshift")
    resolved = region()
    config = short_config()
    serverless = boto3.client("redshift-serverless", region_name=resolved, config=config)

    # Os clusters provisionados são fotografia, e a negação é leitura: o ambiente alvo não tem
    # nenhum, e a biblioteca só chama a API do serverless.
    clusters = report.call("redshift.describe_clusters()", lambda: boto3.client("redshift", region_name=resolved, config=config).describe_clusters(), render=None, expected=True)
    if clusters is not None:
        rows = cluster_rows(clusters)
        report.table(rows if len(rows) > 1 else [["(nenhum cluster provisionado visível)"]])

    namespaces: dict[str, dict] = {}

    def read_namespace(name: str | None) -> None:
        """Lê e mostra o namespace de um workgroup, uma vez por nome.

        O namespace guarda o banco e os papéis IAM que ``RS-6`` separa.
        """
        if not name or name in namespaces:
            return
        namespace = report.call(
            f"redshift-serverless.get_namespace(namespaceName={name!r})",
            lambda: serverless.get_namespace(namespaceName=name)["namespace"],
            render=None,
        )
        if namespace:
            namespaces[name] = namespace
            report.table([
                ["namespace", "banco", "papel IAM padrão", "papéis IAM", "chave KMS"],
                [namespace.get("namespaceName"), namespace.get("dbName"), namespace.get("defaultIamRoleArn") or "-", ", ".join(namespace.get("iamRoles", [])) or "-", namespace.get("kmsKeyId") or "-"],
            ])

    # GetWorkgroup é o passo 1 de examples/redshift_native.py: dá o endereço e a porta da conexão, e
    # a biblioteca depende dele. ListWorkgroups é a fotografia da conta, que a biblioteca não chama;
    # sem workgroup configurado, ela diz ao leitor o que informar.
    workgroup = None
    workgroup_reason: str | None = None
    if target.workgroup:
        workgroup = report.call(
            f"redshift-serverless.get_workgroup(workgroupName={target.workgroup!r})",
            lambda: serverless.get_workgroup(workgroupName=target.workgroup)["workgroup"],
            render=None,
        )
        workgroup_reason = report.last_reason if workgroup is None else None
        if workgroup:
            report.table(workgroup_rows({"workgroups": [workgroup]}))
            read_namespace(workgroup.get("namespaceName"))
            endpoint = workgroup.get("endpoint") or {}
            target.host = target.host or endpoint.get("address")
            target.port = int(endpoint.get("port") or target.port)

    workgroups = report.call("redshift-serverless.list_workgroups()", serverless.list_workgroups, render=None, expected=bool(target.workgroup))
    list_reason = report.last_reason if workgroups is None else None
    if workgroups is not None:
        rows = workgroup_rows(workgroups)
        report.table(rows if len(rows) > 1 else [["(nenhum workgroup serverless visível)"]])
        for item in workgroups.get("workgroups", []):
            read_namespace(item.get("namespaceName"))

    # RS-2: a conexão depende de GetWorkgroup (e de GetCredentials, RS-15); sem workgroup
    # configurado, ListWorkgroups diz o que há para informar.
    if target.workgroup:
        if workgroup is not None:
            report.ok("RS-2", "API do Redshift serverless", f"GetWorkgroup respondeu por {target.workgroup}")
        else:
            report.fail("RS-2", "API do Redshift serverless", f"GetWorkgroup: {workgroup_reason}; sem ele não há endereço nem credencial temporária; ver a seção final")
    elif workgroups is not None:
        report.ok("RS-2", "API do Redshift serverless", "ListWorkgroups respondeu")
    else:
        report.note("RS-2", "API do Redshift serverless", f"ListWorkgroups: {list_reason}; informe SERIALIZE_DB_REDSHIFT_WORKGROUP para GetWorkgroup ser lido")

    # RS-6: IAM_ROLE só aceita papel associado ao cluster ou ao namespace; o padrão é o que IAM_ROLE
    # default usa. A biblioteca só emite IAM_ROLE com SERIALIZE_DB_REDSHIFT_IAM_ROLE: sem ela, o
    # COPY e o UNLOAD levam as credenciais de quem chama (RS-18), o caminho de
    # examples/redshift_copy_unload.py, o único possível quando o namespace não tem papel nenhum.
    defaults, attached = iam_roles(clusters, namespaces)
    if clusters is not None or namespaces:
        target.namespace_roles = (defaults, attached)
    library = "a biblioteca só o usa com SERIALIZE_DB_REDSHIFT_IAM_ROLE; sem ela, as credenciais de quem chama (RS-18)"
    if defaults:
        report.ok("RS-6", "papel IAM para COPY e UNLOAD", f"padrão: {', '.join(defaults)}" + (f"; associados: {', '.join(attached)}" if attached else "") + f"; {library}")
    elif attached:
        report.note("RS-6", "papel IAM para COPY e UNLOAD", f"sem papel padrão; associados: {', '.join(attached)}; IAM_ROLE default não resolve, e {library}")
    elif target.namespace_roles is not None:
        report.note("RS-6", "papel IAM para COPY e UNLOAD", "nenhum papel associado ao namespace: IAM_ROLE não funciona nem com ARN explícito, e o COPY e o UNLOAD levam as credenciais de quem chama (RS-18, RS-11)")
    else:
        report.note("RS-6", "papel IAM para COPY e UNLOAD", "namespace não lido: nada a dizer sobre IAM_ROLE; sem a variável, o COPY leva as credenciais de quem chama")

    # Sem configuração, os nomes visíveis dizem ao leitor o que informar para conectar.
    if target.source == "nada":
        names = [item.get("workgroupName") for item in (workgroups or {}).get("workgroups", [])]
        if names:
            report.line(f"para conectar, informe SERIALIZE_DB_REDSHIFT_WORKGROUP com um de: {', '.join(str(name) for name in names)}, e SERIALIZE_DB_REDSHIFT_DATABASE")

    # RS-10: a Data API executa SQL por HTTPS, sem a porta 5439: o caminho de reserva se a rede
    # fechar a porta. O ciclo é o de examples/redshift_data_api.py, com select 1, que não lê dado
    # nenhum do banco.
    if target.database and target.workgroup:
        data = boto3.client("redshift-data", region_name=resolved, config=config)
        parameters = {"Database": target.database, "WorkgroupName": target.workgroup}
        executed = report.call(
            f"redshift-data: execute_statement + describe_statement + get_statement_result ({parameters}) select 1",
            lambda: data_api_select(data, parameters, "select 1"),
            render=lambda found: f"{found[0]} = {found[1]} em {found[2]} ms",
        )
        report.note("RS-10", "Data API", "ciclo completo responde: caminho alternativo por HTTPS, sem a porta 5439" if executed is not None else f"{report.last_reason}; ver a seção final")
    else:
        report.note("RS-10", "Data API", "não testada: precisa de SERIALIZE_DB_REDSHIFT_DATABASE e de _WORKGROUP")


# --------------------------------------------------------------------------------------------------
# Seção 3: a rede


def network(report: Report, target: Target) -> None:
    """Seção 3: ``RS-14`` (as APIs têm endpoint VPC) e ``RS-3`` (TCP até o host)."""
    report.h1("Rede")

    # Os endpoints regionais das APIs e o host, resolvidos pelo DNS.
    resolved = region()
    names = [f"{service}.{resolved}.amazonaws.com" for service in SERVICES] if resolved else []
    if target.host:
        names.append(target.host)
    rows, private = dns_rows(names)
    report.table([["nome", "endereços", "tipo"], *rows])

    # RS-14: sem internet, as APIs só respondem por endpoint VPC de interface; a porta 5439 do
    # workgroup fica dentro da VPC.
    api_names = names[: len(SERVICES)]
    public_names = [name for name in api_names if not private.get(name)]
    if api_names and not public_names:
        report.ok("RS-14", "APIs do Redshift sem internet", "endpoints VPC de interface: a credencial temporária do workgroup e a Data API funcionam sem internet")
    elif api_names:
        report.note("RS-14", "APIs do Redshift sem internet", f"sem endpoint VPC: {', '.join(public_names)}; sem internet ou proxy, GetWorkgroup, GetCredentials e a Data API não respondem, e resta o par informado em _USER e _PASSWORD na porta 5439")

    # RS-3: a porta do host abre; sem host conhecido, o endereço vem de GetWorkgroup na seção 2.
    if target.host:
        opened = report.call(f"tcp {target.host}:{target.port}", lambda: tcp_open(target.host or "", target.port, 5), render=lambda seconds: f"conectou em {seconds:.2f} s")
        if opened is None:
            report.fail("RS-3", "host por TCP", f"{target.host}:{target.port} sem conexão; ver a seção final")
        else:
            report.ok("RS-3", "host por TCP", f"{target.host}:{target.port}")
    else:
        report.note("RS-3", "host por TCP", "sem host conhecido; o endereço vem de GetWorkgroup (seção 2) ou de SERIALIZE_DB_REDSHIFT_HOST")


# --------------------------------------------------------------------------------------------------
# Seção 4: a sessão


def render_rows(found: tuple[list[str], list[tuple]]) -> str:
    """O resultado de uma consulta como tabela alinhada, ou ``(nenhuma linha)``."""
    columns, rows = found
    return tabulate([columns, *[[str(value) for value in row] for row in rows]]) if rows else "(nenhuma linha)"


def first_value(found: tuple[list[str], list[tuple]]) -> object | None:
    """O primeiro valor da primeira linha, ou ``None`` quando a consulta não devolveu linha.

    Um ``count(*)`` de ``sys_load_error_detail`` voltou sem linha no ambiente alvo (2026-09-23).
    """
    _, rows = found
    return rows[0][0] if rows and rows[0] else None


def count_text(found: tuple[list[str], list[tuple]], unit: str) -> str:
    """A contagem de uma consulta ``count(*)`` com a unidade, ou a falta da linha como leitura."""
    count = first_value(found)
    if count is None:
        return "a contagem não devolveu linha"
    return f"{count} {unit}"


def column_value(columns: list[str], row: tuple, name: str) -> object | None:
    """O valor de uma coluna pelo nome, ou ``None`` quando a visão de sistema não a tem."""
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
    """Os três números do patch em ``version()`` (``Redshift 1.0.78890``), ou ``None`` sem eles."""
    found = re.search(r"Redshift (\d+)\.(\d+)\.(\d+)", text)
    return tuple(int(part) for part in found.groups()) if found else None


def credential_summary(credentials: dict) -> str:
    """O usuário e a expiração de uma credencial temporária; a senha nunca entra no relatório."""
    return f"dbUser={credentials.get('dbUser')} expira {credentials.get('expiration')}"


def datashare_write_verdict(version: tuple[int, ...] | None, kind: str, isolation: object | None, slices: int | None) -> tuple[str, str]:
    """O veredito da escrita no banco do datashare: patch mínimo, isolamento de snapshot e slices.

    Devolve ``("ok" | "fail" | "note", texto)``, um requisito por trecho. Um requisito que a sessão
    não leu entra como ``não lido`` e não reprova sozinho. O isolamento exigido é o do banco que
    recebe a escrita, que fica no produtor: num banco de datashare a coluna vem ``UNKNOWN`` (leitura
    de 2026-09-20), e esse valor conta como ausência de leitura, não como isolamento serializável.
    """
    minimum = DATASHARE_WRITE_VERSION.get(kind, DATASHARE_WRITE_VERSION["serverless"])
    known_isolation = None if isolation is None or str(isolation).strip().lower() in ("", "unknown") else str(isolation)

    # Os requisitos, com o texto e o resultado: True atende, False não atende, None não lido.
    checks: list[tuple[str, bool | None]] = [
        (
            f"patch {'.'.join(str(part) for part in version)} contra {'.'.join(str(part) for part in minimum)} ({kind})" if version else "patch",
            None if version is None else version >= minimum,
        ),
        (
            f"isolamento {known_isolation}" if known_isolation else f"isolamento ({isolation or 'sem leitura'}, do banco do produtor)",
            None if known_isolation is None else DATASHARE_WRITE_ISOLATION in known_isolation.lower(),
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
        return "note", text + "; o que a sessão não lê, o CREATE TABLE da suíte testa"
    return "ok", text


def session(report: Report, target: Target) -> None:
    """Seção 4, a sessão.

    Checagens: ``RS-15`` (credencial temporária do workgroup), ``RS-4`` (a sessão abre), ``RS-7``
    (``SUPER``), ``RS-9`` (privilégios no banco da conexão), ``RS-12`` (diagnóstico do ``COPY``),
    ``RS-13`` (esquemas externos), ``RS-16`` (o banco do esquema do projeto), ``RS-17`` (escrita num
    banco de datashare), ``RS-19`` (``USE`` no banco do datashare), ``RS-5`` (privilégios no
    esquema) e ``RS-8`` (tabelas da biblioteca).

    As leituras do banco da conexão vêm antes do ``USE``, que troca o banco da sessão.
    """
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

    # RS-15: a credencial temporária do workgroup, o passo 2 de examples/redshift_native.py. O
    # usuário sai da identidade IAM (IAMR:<papel>), entra em PUBLIC e a senha dura no máximo uma
    # hora; sem durationSeconds seriam 900 segundos. A senha fica fora do relatório.
    temporary = False
    if target.user and target.password:
        report.note("RS-15", "credencial temporária do workgroup", "não pedida: usuário e senha vieram das variáveis ou da conexão do projeto")
    elif not target.workgroup:
        report.note("RS-15", "credencial temporária do workgroup", "sem SERIALIZE_DB_REDSHIFT_WORKGROUP: a credencial temporária é do workgroup serverless, e sem ela a sessão precisa do par em _USER e _PASSWORD")
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
            report.fail("RS-15", "credencial temporária do workgroup", f"{report.last_reason}; sem ela a sessão precisa do par em _USER e _PASSWORD")

    # RS-4: o passo 3 de examples/redshift_native.py, a mesma chamada de tests/conftest.py, com o
    # par da credencial temporária ou o informado. O timeout do redshift_connector vale para
    # conectar e para ler: 10 s abortaram sys_load_error_detail no ambiente alvo (2026-09-20) e a
    # conexão não voltou a servir, e o primeiro comando de uma sessão lá custou 10,8 s (o USE de
    # 2026-09-21); 30 s bastam para as visões de sistema que o probe lê.
    def connect() -> tuple[str, object]:
        if not (target.host and target.user and target.password):
            raise RuntimeError("faltam parâmetros: o endereço vem de GetWorkgroup e o par de GetCredentials (RS-15), ou de _HOST, _USER e _PASSWORD")
        label = "credencial temporária do workgroup" if temporary else "par informado"
        return label, redshift_connector.connect(host=target.host, port=target.port, database=target.database, user=target.user, password=target.password, timeout=30)

    result = report.call("redshift_connector.connect(...)", connect, render=lambda found: f"conectou por {found[0]}")
    if result is None:
        report.fail("RS-4", "sessão", "conexão falhou; ver a seção final")
        return
    method, connection = result
    connection.autocommit = True
    report.ok("RS-4", "sessão", f"aberta por {method}" + ("; a credencial derivada da identidade IAM pode ter criado o usuário do banco" if temporary else ""))

    # Um tempo limite de leitura fecha o socket do redshift_connector, e toda consulta seguinte
    # devolveria "cannot read from timed out object": a primeira perda é registrada, e as demais
    # leituras dizem que a conexão caiu, em vez de repetir esse erro.
    lost: list[str] = []

    def query(sql: str, params: tuple = ()) -> tuple[list[str], list[tuple]]:
        if lost:
            raise RuntimeError(f"conexão perdida em {lost[0]}; as leituras seguintes não rodam")
        cursor = connection.cursor()
        try:
            cursor.execute(sql, params)
            rows = cursor.fetchall() if cursor.description else []  # O USE não devolve linhas.
        except OSError:  # TimeoutError é OSError: o socket não volta a servir.
            lost.append(" ".join(sql.split())[:60])
            raise
        return [column[0] for column in cursor.description or []], rows

    # A versão: o patch (1.0.NNNNN) diz quais recursos existem: MERGE, SUPER, o COPY de Parquet com
    # FILLRECORD e a escrita num banco de datashare (RS-17).
    version = report.call("select version()", lambda: query("select version()"), render=render_rows)
    patch = version_tuple(str(version[1][0][0])) if version and version[1] else None
    if version and version[1]:
        report.value("REDSHIFT_VERSION", ".".join(str(part) for part in patch) if patch else str(version[1][0][0])[:80])

    # Quem é a sessão: usuário, banco e esquema correntes, search_path e os esquemas visíveis.
    report.call("select current_user, current_database(), current_schema()", lambda: query("select current_user, current_database(), current_schema()"), render=render_rows)
    report.call("show search_path", lambda: query("show search_path"), render=render_rows)
    report.call(
        "esquemas visíveis",
        lambda: query("select nspname, pg_get_userbyid(nspowner) as owner from pg_namespace where nspname not like 'pg_%%' and nspname <> 'information_schema' order by 1"),
        render=render_rows,
    )

    # RS-7: o tipo SUPER e JSON_PARSE, que a coluna JSON do contrato usa no Redshift.
    parsed = report.call("select json_parse(...)  (SUPER)", lambda: query("select json_parse(%s) as super_value", ('{"a": 1}',)), render=render_rows)
    if parsed is not None:
        report.ok("RS-7", "SUPER e JSON_PARSE", "disponíveis")
    else:
        report.note("RS-7", "SUPER e JSON_PARSE", "falhou; ver a seção final")

    # As configurações que mudam o comportamento do SQL gerado (datas, fuso, tempo limite,
    # search_path e sensibilidade a maiúsculas) e wlm_query_slot_count.
    report.call(
        "pg_settings (datestyle, timezone, statement_timeout, search_path, enable_case_sensitive_identifier)",
        lambda: query("select name, setting from pg_settings where name in ('datestyle', 'timezone', 'statement_timeout', 'search_path', 'enable_case_sensitive_identifier', 'wlm_query_slot_count') order by 1"),
        render=render_rows,
    )
    # pg_settings do serverless não trouxe timezone nem enable_case_sensitive_identifier (leitura de
    # 2026-09-20); SHOW responde pelas duas, e o probe lê a segunda, que decide como os
    # identificadores são citados.
    report.call("show enable_case_sensitive_identifier", lambda: query("show enable_case_sensitive_identifier"), render=render_rows, expected=True)
    report.call("pg_user do usuário atual", lambda: query("select usename, usesuper, usecreatedb from pg_user where usename = current_user"), render=render_rows)

    # RS-9: no banco da conexão, antes do USE: CREATE permite um esquema externo, TEMP permite a
    # staging temporária, que é uma das moradas possíveis do sandbox de execução.
    privileges = report.call(
        "has_database_privilege(current_database(), CREATE | TEMP)",
        lambda: query("select has_database_privilege(current_database(), 'CREATE') as create_db, has_database_privilege(current_database(), 'TEMP') as temp_db"),
        render=render_rows,
    )
    if privileges and privileges[1]:
        create_db, temp_db = privileges[1][0]
        report.note("RS-9", "privilégios no banco da conexão", f"{target.database}: CREATE={create_db} (esquema externo {'possível' if create_db else 'impossível'}), TEMP={temp_db} (staging temporária {'possível' if temp_db else 'impossível'})")
    else:
        report.note("RS-9", "privilégios no banco da conexão", "não lidos; ver a seção final")

    # RS-12: um COPY reprovado explica o motivo em sys_load_error_detail, a visão que respondeu no
    # ambiente alvo (1,5 s, 2026-09-20) e cobre o serverless; stl_load_errors cobre só clusters
    # provisionados e é negada a um usuário comum, então fica de reserva.
    detail = report.call("sys_load_error_detail dos últimos 30 dias", lambda: query("select count(*) from sys_load_error_detail where start_time > dateadd(day, -30, getdate())"), render=render_rows, expected=True)
    if detail is not None:
        report.note("RS-12", "diagnóstico do COPY", f"sys_load_error_detail legível: {count_text(detail, 'erro(s) de carga em 30 dias')}")
    else:
        errors = report.call("stl_load_errors dos últimos 30 dias", lambda: query("select count(*) from stl_load_errors where starttime > dateadd(day, -30, getdate())"), render=render_rows, expected=True)
        if errors is not None:
            report.note("RS-12", "diagnóstico do COPY", f"stl_load_errors legível: {count_text(errors, 'erro(s) de carga em 30 dias')}")
        else:
            report.note("RS-12", "diagnóstico do COPY", "nem sys_load_error_detail nem stl_load_errors legíveis: o motivo de um COPY reprovado virá do administrador")

    # RS-13: a biblioteca não usa esquemas externos; a contagem mostra se o Glue chegou ao Redshift.
    external = report.call("svv_external_schemas", lambda: query("select count(*) from svv_external_schemas"), render=render_rows, expected=True)
    if external is not None:
        report.note("RS-13", "esquemas externos (Spectrum)", f"{count_text(external, 'no banco')}; a biblioteca não os usa, e a contagem mostra se o Glue chegou ao Redshift")
    else:
        report.note("RS-13", "esquemas externos (Spectrum)", f"não lidos: {report.last_reason}")

    # RS-16: os bancos que a sessão enxerga e em qual deles está o esquema do projeto. Um banco de
    # tipo shared vem de datashare: a sessão roda USE nele (RS-19) e cita esquema.tabela; o nome em
    # três partes fica para uma sessão aberta em outro banco, como a da Data API.
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
            # Sem a variável, o alvo toma o banco da leitura, para o USE e o nome em três partes
            # saírem certos.
            if schema_kind.lower() == "shared" and not target.share_database:
                target.share_database = schema_database
                report.line(f"SERIALIZE_DB_REDSHIFT_SHARE_DATABASE ausente; a leitura diz {schema_database}")
            if len(places) > 1:
                report.note("RS-16", "banco do esquema do projeto", f"{schema} aparece em mais de um banco: {listed}; SERIALIZE_DB_REDSHIFT_SHARE_DATABASE decide qual")
            elif target.share_database and schema_database.lower() != target.share_database.lower():
                report.fail("RS-16", "banco do esquema do projeto", f"SERIALIZE_DB_REDSHIFT_SHARE_DATABASE diz {target.share_database} e a leitura diz {listed}")
            elif schema_kind.lower() == "shared":
                report.ok("RS-16", "banco do esquema do projeto", f"{listed}: USE nele e {target.qualified('<tabela>')} na sessão (RS-19); {target.fully_qualified('<tabela>')} de outro banco, como a Data API")
            else:
                report.ok("RS-16", "banco do esquema do projeto", f"{listed}: {target.qualified('<tabela>')} no banco da conexão, sem USE")
            report.value("REDSHIFT_TABLE_NAME", target.qualified("<tabela>"))
            report.value("REDSHIFT_TABLE_FULL_NAME", target.fully_qualified("<tabela>"))

    # RS-17: escrever num banco de datashare exige o patch 186 (1.0.78890 no serverless, 1.0.78881
    # no provisionado), isolamento de snapshot no banco que recebe a escrita e 64 slices ou mais no
    # consumidor. Sem um deles, o CREATE, o COPY e o INSERT da publicação são recusados. O que a
    # sessão não lê (stv_slices é negada a um usuário comum, e o isolamento do produtor vem UNKNOWN)
    # não reprova: o CREATE TABLE da suíte é o teste, e passou no ambiente alvo em 2026-09-20.
    if not schema_kind:
        report.note("RS-17", "escrita no banco do datashare", "banco do esquema não lido (RS-16)")
    elif schema_kind.lower() != "shared":
        report.note("RS-17", "escrita no banco do datashare", f"o esquema do projeto é {schema_kind} em {schema_database}: as regras do datashare não se aplicam")
    else:
        rows = matching_rows(databases, database_name=schema_database) if databases and schema_database else []
        isolation = column_value(databases[0], rows[0], "database_isolation_level") if databases and rows else None
        slices = report.call("select count(*) from stv_slices", lambda: query("select count(*) from stv_slices"), render=render_rows, expected=True)
        status, verdict = datashare_write_verdict(
            patch,
            "serverless" if target.workgroup else "provisionado",
            isolation,
            slices[1][0][0] if slices and slices[1] else None,
        )
        getattr(report, status)("RS-17", "escrita no banco do datashare", verdict)

    # RS-19: depois do USE, esquema.tabela resolve no banco do datashare, que é como o CREATE, o
    # COPY e o UNLOAD passaram (examples/redshift_copy_unload.py e redshift_manifest.py, este com os
    # dois comandos de manifesto em 2026-09-21) e como tests/conftest.py abre cada conexão. A prova
    # da troca é resolver um nome em duas partes de uma tabela que svv_all_tables lista no esquema:
    # current_database() continuou respondendo o banco da conexão depois do USE (ambiente alvo,
    # 2026-09-21), então ele é leitura, não critério. Nada é criado, alterado nem apagado.
    share = target.share_database
    used_share = False
    if not schema:
        report.note("RS-19", "USE no banco do datashare", "sem SERIALIZE_DB_REDSHIFT_SCHEMA")
    elif not share or share.lower() == (target.database or "").lower():
        report.note("RS-19", "USE no banco do datashare", f"o esquema está no banco da conexão ({target.database}): sem USE")
    else:
        current = report.call(f"use {share}; select current_database()", lambda: (query(f"USE {share}"), query("select current_database()"))[1], render=render_rows)
        landed = str(current[1][0][0]) if current and current[1] else None
        if current is None:
            report.fail("RS-19", "USE no banco do datashare", f"USE {share}: {report.last_reason}; ver a seção final")
        else:
            report.value("REDSHIFT_CURRENT_DATABASE", landed)
            listed = report.call(
                f"svv_all_tables, uma tabela de {share}.{schema} para resolver por nome em duas partes",
                lambda: query(
                    "select table_name from svv_all_tables where lower(database_name) = lower(%s) and lower(schema_name) = lower(%s) order by 1 limit 1",
                    (share, schema),
                ),
                render=render_rows,
            )
            probe_table = str(listed[1][0][0]) if listed and listed[1] else None
            if probe_table is None:
                report.note("RS-19", "USE no banco do datashare", f"USE {share} aceito e current_database() em {landed}; sem tabela em {schema} para provar a resolução de {target.qualified('<tabela>')}, o primeiro CREATE da suíte é o teste")
            elif report.call(f"select 1 from {target.qualified(probe_table)} where false (resolução depois do USE)", lambda: query(f"select 1 from {target.qualified(probe_table)} where false")) is not None:
                used_share = True
                report.ok("RS-19", "USE no banco do datashare", f"{target.qualified(probe_table)} resolveu depois do USE {share}; current_database() respondeu {landed}, que não reflete a troca")
            else:
                report.fail("RS-19", "USE no banco do datashare", f"{target.qualified(probe_table)} não resolveu depois do USE {share}: {report.last_reason}; a biblioteca depende da troca")

    # RS-5 e RS-8: USAGE e CREATE no esquema, e as tabelas com o prefixo da biblioteca.
    # has_schema_privilege e svv_table_info enxergam o banco da sessão, e num esquema compartilhado
    # só depois do USE: lá, no ambiente alvo em 2026-09-23, a primeira respondeu USAGE e CREATE
    # falsos onde o CREATE TABLE dos exemplos passou, e a segunda foi negada (42501). Uma resposta
    # negativa lá é leitura, e svv_all_tables, que cruza bancos, é a lista provada.
    shared = bool(schema_kind) and schema_kind.lower() == "shared"
    if not schema:
        report.note("RS-5", "privilégios no esquema", "sem SERIALIZE_DB_REDSHIFT_SCHEMA")
        report.note("RS-8", "tabelas com o prefixo da biblioteca no esquema", "sem SERIALIZE_DB_REDSHIFT_SCHEMA")
    else:
        if shared and not used_share:
            report.note("RS-5", "privilégios no esquema", f"{schema} vem do datashare {schema_database}: sem o USE (RS-19), has_schema_privilege não o alcança; quem concede USAGE e CREATE é o produtor, e o primeiro CREATE da suíte é o teste")
        else:
            privileges = report.call(
                f"has_schema_privilege({schema!r}, USAGE | CREATE)" + (" depois do USE" if used_share else ""),
                lambda: query("select has_schema_privilege(%s, 'USAGE') as usage, has_schema_privilege(%s, 'CREATE') as create", (schema, schema)),
                render=render_rows,
                expected=shared,
            )
            if privileges and privileges[1]:
                usage, create = privileges[1][0]
                if usage and create:
                    report.ok("RS-5", "privilégios no esquema", f"{schema}: USAGE e CREATE" + (" depois do USE" if used_share else ""))
                elif shared:
                    report.note("RS-5", "privilégios no esquema", f"{schema}: USAGE={usage}, CREATE={create} depois do USE; num esquema compartilhado quem concede é o produtor, e o CREATE da suíte é o teste")
                else:
                    report.fail("RS-5", "privilégios no esquema", f"{schema}: USAGE={usage}, CREATE={create}")
            elif shared:
                report.note("RS-5", "privilégios no esquema", f"{schema}: has_schema_privilege não respondeu depois do USE ({report.last_reason}); quem concede é o produtor, e o CREATE da suíte é o teste")
            else:
                report.fail("RS-5", "privilégios no esquema", f"{schema}: não lidos; ver a seção final")

        if shared:
            found = report.call(
                f"svv_all_tables, linhas de {schema_database}.{schema}",
                lambda: query("select * from svv_all_tables"),
                render=lambda loaded: render_rows((loaded[0], matching_rows(loaded, database_name=schema_database or "", schema_name=schema))),
            )
            names = [
                str(column_value(found[0], row, "table_name"))
                for row in (matching_rows(found, database_name=schema_database or "", schema_name=schema) if found else [])
            ]
            where = f"{schema_database}.{schema}"
            # Depois do USE, svv_table_info é leitura a mais: as linhas e o tamanho, que
            # svv_all_tables não traz, e se ela alcança o esquema.
            if used_share:
                report.call(
                    f"svv_table_info do esquema {schema!r} depois do USE",
                    lambda: query('select "table", tbl_rows, size as size_mb, diststyle, sortkey1 from svv_table_info where schema = %s order by 1', (schema,)),
                    render=render_rows,
                    expected=True,
                )
        else:
            found = report.call(
                f"svv_table_info do esquema {schema!r}",
                lambda: query('select "table", tbl_rows, size as size_mb, diststyle, sortkey1 from svv_table_info where schema = %s order by 1', (schema,)),
                render=render_rows,
            )
            names = [str(row[0]) for row in found[1]] if found else []
            where = schema

        if found is None:
            report.note("RS-8", "tabelas com o prefixo da biblioteca no esquema", f"não lidas: {report.last_reason}")
        else:
            mine = [name for name in names if name.startswith(TABLE_PREFIX)]
            report.note("RS-8", "tabelas com o prefixo da biblioteca no esquema", f"{len(mine)} de {len(names)} tabelas em {where}")

    connection.close()


# --------------------------------------------------------------------------------------------------
# Seção 5: quem alcança o S3


class Principal(NamedTuple):
    """Uma identidade que a seção 5 simula.

    ``label`` é o rótulo, ``arn`` o ARN, ``blocker`` o que a impede antes do S3 e ``doubt`` o que
    ficou sem leitura.
    """

    label: str
    arn: str
    blocker: str | None = None
    doubt: str | None = None


def caller_credentials() -> tuple[object, object | None]:
    """As credenciais congeladas da sessão ``boto3`` e a expiração que o provedor expõe.

    A expiração é ``None`` quando o provedor não a expõe; sem credenciais, levanta ``RuntimeError``.
    """
    import boto3

    found = boto3.Session().get_credentials()
    if found is None:
        raise RuntimeError("o boto3 não encontrou credenciais")
    return found.get_frozen_credentials(), getattr(found, "_expiry_time", None)


def credential_text(access_key: str, token: str | None, expiry: object | None, now: datetime) -> str:
    """O que o relatório diz das credenciais de quem chama.

    São o prefixo da chave, se há ``SESSION_TOKEN`` e quando expiram; o segredo nunca entra.
    """
    text = f"chave {access_key[:4]}…, SESSION_TOKEN {'presente' if token else 'ausente'}"
    if isinstance(expiry, datetime) and expiry.tzinfo is not None:
        minutes = (expiry - now).total_seconds() / 60
        text += f", expira {expiry} ({'daqui a' if minutes >= 0 else 'há'} {abs(minutes):.0f} min)"
    return text


def copy_principals(iam_role: str | None, namespace_roles: tuple[list[str], list[str]] | None, caller_arn: str | None) -> tuple[list[Principal], str | None]:
    """Quem alcança o S3 no ``COPY`` e no ``UNLOAD``, ou o motivo de não haver ninguém a simular.

    Sem ``SERIALIZE_DB_REDSHIFT_IAM_ROLE``, o comando leva as credenciais de quem chama, e a
    identidade simulada é a do STS, como papel. Com ``default``, é o papel padrão do namespace; com
    um ARN, é ele, e um ARN que o namespace não tem ganha o bloqueio, porque o Redshift só aceita
    papel associado; um namespace que a seção 2 não leu deixa a dúvida.
    """
    if not iam_role:
        if caller_arn:
            return [Principal("credenciais de quem chama", probelib.principal_arn(caller_arn))], None
        return [], "sem SERIALIZE_DB_REDSHIFT_IAM_ROLE e sem identidade do STS: nada a simular"

    defaults, attached = namespace_roles or ([], [])
    if iam_role.lower() == "default":
        if defaults:
            return [Principal("IAM_ROLE default", defaults[0])], None
        if namespace_roles is None:
            return [], "SERIALIZE_DB_REDSHIFT_IAM_ROLE=default, e o papel padrão do namespace não foi lido (RS-6): nada a simular"
        return [], "SERIALIZE_DB_REDSHIFT_IAM_ROLE=default sem papel padrão no namespace (RS-6): o COPY recusaria IAM_ROLE default"

    blocker = doubt = None
    if namespace_roles is None:
        doubt = "a associação ao namespace não foi lida (RS-6), e o Redshift só aceita papel associado"
    elif iam_role not in defaults and iam_role not in attached:
        blocker = "não está associado ao namespace (RS-6): o Redshift o recusa mesmo com permissão no S3"
    return [Principal("SERIALIZE_DB_REDSHIFT_IAM_ROLE", iam_role, blocker, doubt)], None


def copy_role(report: Report, target: Target) -> None:
    """Seção 5, quem alcança o S3.

    Checagens: ``RS-18`` (as credenciais de quem chama) e ``RS-11`` (se quem vai alcançar o S3 no
    ``COPY`` e no ``UNLOAD`` tem permissão sob a raiz, pela simulação de política do IAM).
    """
    import boto3

    report.h1("Quem alcança o S3 no COPY e no UNLOAD")

    # RS-18: sem SERIALIZE_DB_REDSHIFT_IAM_ROLE, o COPY e o UNLOAD levam as credenciais da sessão no
    # texto do comando (examples/redshift_copy_unload.py). Elas expiram, e um comando montado antes
    # da renovação falha: a biblioteca as pede a cada comando, e a expiração diz quanto um COPY
    # pode durar.
    frozen = report.call(
        "boto3.Session().get_credentials()",
        caller_credentials,
        render=lambda found: credential_text(found[0].access_key, found[0].token, found[1], datetime.now(timezone.utc)),
    )
    if frozen is None:
        report.fail("RS-18", "credenciais de quem chama", f"{report.last_reason}: sem elas o COPY e o UNLOAD só rodam com IAM_ROLE de um papel associado")
    else:
        credentials, expiry = frozen
        text = credential_text(credentials.access_key, credentials.token, expiry, datetime.now(timezone.utc))
        if not credentials.token:
            report.note("RS-18", "credenciais de quem chama", f"{text}: sem SESSION_TOKEN o COPY dispensa a cláusula, e elas não expiram sozinhas")
        else:
            report.ok("RS-18", "credenciais de quem chama", f"{text}: o COPY e o UNLOAD as levam no texto do comando, que nunca vai para log")

    # A raiz S3 que a simulação avalia; sem uma raiz s3://, RS-11 é leitura.
    root, source = probelib.s3_root(sys.argv)
    if not root.startswith("s3://"):
        report.line(probelib.NO_ROOT)
        report.note("RS-11", "alcance do COPY sobre a raiz", probelib.NO_ROOT)
        return
    report.value("S3_ROOT", f"{root} (por {source})")

    # Quem a simulação avalia é quem a biblioteca vai mandar ao S3: a identidade da sessão, ou o
    # papel que SERIALIZE_DB_REDSHIFT_IAM_ROLE nomeia. Os demais papéis do namespace ficam em RS-6.
    caller_arn = None
    if not target.iam_role:
        caller = report.call(
            "sts.get_caller_identity()",
            lambda: boto3.client("sts", config=short_config(5, 10, 1)).get_caller_identity(),
            render=lambda found: found.get("Arn", ""),
        )
        caller_arn = caller.get("Arn") if caller else None
    principals, reason = copy_principals(target.iam_role, target.namespace_roles, caller_arn)
    if not principals:
        detail = f"{reason}; STS: {report.last_reason}" if caller_arn is None and not target.iam_role else str(reason)
        report.note("RS-11", "alcance do COPY sobre a raiz", detail)
        return

    # Uma simulação por identidade: ListBucket no bucket, GetObject e PutObject sob a raiz.
    bucket, _, prefix = root.removeprefix("s3://").partition("/")
    resources = [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/{prefix}/*" if prefix else f"arn:aws:s3:::{bucket}/*"]
    iam = boto3.client("iam", config=short_config(2, 5, 1))

    # O IAM não tem endpoint VPC em todo ambiente; sem o teste, cada simulação esperaria o tempo
    # limite em cada endereço resolvido.
    alcance, leitura = probelib.endpoint_reachable(iam)
    report.line(f"alcance do IAM: {leitura}\n")
    if not alcance:
        report.note("RS-11", "alcance do COPY sobre a raiz", f"simulação sem chamada: o IAM não respondeu ao teste TCP ({leitura}); o primeiro COPY da suíte Redshift é o teste")
        return

    def render_decisions(found: dict) -> str:
        """As decisões da simulação como tabela: a ação e a decisão."""
        return tabulate([["ação", "decisão"], *[[item["EvalActionName"], item["EvalDecision"]] for item in found.get("EvaluationResults", [])]])

    # RS-11: uma ação negada ou um bloqueio reprovam; uma dúvida ou a simulação que falha ficam
    # como leitura.
    for principal in principals:
        who = f"{principal.label} ({principal.arn})"
        if principal.blocker:
            report.line(f"{who}: {principal.blocker}")
        found = report.call(
            f"iam.simulate_principal_policy({principal.arn}: ListBucket, GetObject, PutObject em {root})",
            lambda arn=principal.arn: iam.simulate_principal_policy(PolicySourceArn=arn, ActionNames=COPY_ACTIONS, ResourceArns=resources),
            render=render_decisions,
        )
        if found is None:
            report.note("RS-11", "alcance do COPY sobre a raiz", f"{who}: simulação {report.last_reason}; o primeiro COPY da suíte Redshift é o teste" + (f"; {principal.blocker}" if principal.blocker else ""))
            continue
        denied = [item["EvalActionName"] for item in found.get("EvaluationResults", []) if item["EvalDecision"] != "allowed"]
        if denied:
            report.fail("RS-11", "alcance do COPY sobre a raiz", f"{who}: {', '.join(denied)} negadas sob {root}")
        elif principal.blocker:
            report.fail("RS-11", "alcance do COPY sobre a raiz", f"{who}: ListBucket, GetObject e PutObject sob {root}, mas {principal.blocker}")
        elif principal.doubt:
            report.note("RS-11", "alcance do COPY sobre a raiz", f"{who}: ListBucket, GetObject e PutObject sob {root}; {principal.doubt}")
        else:
            report.ok("RS-11", "alcance do COPY sobre a raiz", f"{who}: ListBucket, GetObject e PutObject sob {root}")


# --------------------------------------------------------------------------------------------------
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
