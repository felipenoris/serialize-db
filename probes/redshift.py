"""O Redshift do projeto visto de dentro: como o ambiente o expõe, se responde e o que a sessão enxerga.

Uso:

    .venv/bin/python probes/redshift.py [s3://bucket/prefixo]

A raiz S3, pelo argumento ou por ``SERIALIZE_DB_TEST_S3_ROOT``, serve à simulação do papel do
``COPY`` e do ``UNLOAD`` sobre ela.

Só leitura: conecta e consulta visões de sistema, sem criar, alterar ou apagar nada no banco. Uma
autenticação por IAM com criação automática de usuário deixa esse usuário no cluster; é o único
efeito colateral possível, e a checagem RS-4 o aponta. O relatório sai no terminal e em
``probes/output/redshift_<data-hora>.txt``. Seções:

1. Configuração: as variáveis ``SERIALIZE_DB_REDSHIFT_*`` e a conexão Redshift do projeto
   (``sagemaker_studio``), de onde saem host, porta, banco, cluster ou workgroup.
2. APIs: ``redshift:DescribeClusters``, ``redshift-serverless:ListWorkgroups`` e ``GetNamespace``,
   com o papel IAM padrão que o ``COPY`` e o ``UNLOAD`` usam, o roteamento VPC e os nós; a Data API
   (``redshift-data:ListDatabases``) como caminho alternativo quando a porta 5439 está fechada.
3. Rede: DNS dos endpoints regionais e TCP até o host.
4. Sessão: ``redshift_connector`` com senha ou com IAM; versão, usuário, banco, ``search_path``,
   esquemas, privilégios no esquema do projeto e no banco (``CREATE``, ``TEMP``), tabelas com o
   prefixo da biblioteca, ``SUPER``, as configurações da sessão, ``stl_load_errors`` (o diagnóstico
   de um ``COPY`` reprovado) e os esquemas externos.
5. Papel do ``COPY`` e do ``UNLOAD`` sobre a raiz S3, pela simulação de política do IAM.

Variáveis: ``SERIALIZE_DB_REDSHIFT_HOST``, ``_PORT`` (5439), ``_DATABASE``, ``_USER``, ``_PASSWORD``,
``_CLUSTER`` (identificador do cluster, autenticação IAM), ``_WORKGROUP`` (serverless, autenticação
IAM), ``_SCHEMA`` (esquema do projeto) e ``_CONNECTION`` (nome da conexão do projeto; sem ela, a
única conexão Redshift). Sem variável nenhuma e sem conexão no projeto, nada é conectado. Códigos de
saída: 0 checagens ok, 1 alguma chamada falhou, 2 alguma checagem reprovou.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import probelib  # noqa: E402
from probelib import Report, describe_error, environment_rows, find_values, pretty, region, resolve, short_config, tabulate, tcp_open  # noqa: E402

VARIABLES = ("HOST", "PORT", "DATABASE", "USER", "PASSWORD", "CLUSTER", "WORKGROUP", "SCHEMA", "CONNECTION")
SERVICES = ("redshift", "redshift-serverless", "redshift-data")
TABLE_PREFIX = "serialize_db"


def variable(name: str) -> str | None:
    return os.environ.get(f"SERIALIZE_DB_REDSHIFT_{name}") or None


@dataclass
class Target:
    """Parâmetros de conexão reunidos das variáveis e da conexão do projeto."""

    host: str | None = None
    port: int = 5439
    database: str | None = None
    user: str | None = None
    password: str | None = None
    cluster: str | None = None
    workgroup: str | None = None
    schema: str | None = None
    source: str = "nada"
    roles: list[str] = field(default_factory=list)


def configuration(report: Report) -> Target:
    report.h1("Configuração")
    report.table([["variável", "valor"], *environment_rows([f"SERIALIZE_DB_REDSHIFT_{name}" for name in VARIABLES])])
    target = Target(
        host=variable("HOST"),
        port=int(variable("PORT") or 5439),
        database=variable("DATABASE"),
        user=variable("USER"),
        password=variable("PASSWORD"),
        cluster=variable("CLUSTER"),
        workgroup=variable("WORKGROUP"),
        schema=variable("SCHEMA"),
    )
    if target.host or target.cluster or target.workgroup:
        target.source = "variáveis"
    result = report.call("sagemaker_studio.Project().connections", probelib.project_snapshot, render=lambda found: f"lido com {found[1]}")
    if result is not None:
        data, _ = result
        connections = data.get("connections", [])
        report.table([["conexão", "tipo", "endpoints"], *[[item.get("name"), item.get("type"), "; ".join(f"{endpoint.get('host')}:{endpoint.get('port')}" for endpoint in item.get("physical_endpoints", [])) or "-"] for item in connections]])
        redshift = [item for item in connections if "REDSHIFT" in str(item.get("type", "")).upper()]
        wanted = variable("CONNECTION")
        if wanted:
            chosen = next((item for item in redshift if item.get("name") == wanted), None)
        else:
            chosen = redshift[0] if len(redshift) == 1 else None
        if chosen:
            report.line(pretty(chosen, limit=80))
            found = find_values(chosen, ("host", "port", "database_name", "databaseName", "workgroupName", "clusterName", "db_user", "dbUser", "username"))
            if target.source == "nada":
                endpoint = (chosen.get("physical_endpoints") or [{}])[0]
                target.host = endpoint.get("host") or found.get("host")
                target.port = int(endpoint.get("port") or found.get("port") or target.port)
                target.database = target.database or found.get("database_name") or found.get("databaseName")
                target.workgroup = found.get("workgroupName")
                target.cluster = found.get("clusterName")
                target.user = target.user or found.get("db_user") or found.get("dbUser") or found.get("username")
                target.source = f"conexão {chosen.get('name')} do projeto"
        elif len(redshift) > 1:
            report.line(f"{len(redshift)} conexões Redshift no projeto; informe SERIALIZE_DB_REDSHIFT_CONNECTION")
        elif not redshift:
            report.line("nenhuma conexão Redshift no projeto")
    for name in ("host", "port", "database", "user", "cluster", "workgroup", "schema"):
        report.value(f"REDSHIFT_{name.upper()}", getattr(target, name))
    if target.source == "nada":
        report.note("RS-1", "conexão configurada", "nada: informe SERIALIZE_DB_REDSHIFT_* ou crie a conexão Redshift no projeto")
    else:
        report.ok("RS-1", "conexão configurada", f"por {target.source}")
    return target


def apis(report: Report, target: Target) -> None:
    import boto3

    report.h1("APIs do Redshift")
    resolved = region()
    config = short_config()
    clusters = report.call("redshift.describe_clusters()", lambda: boto3.client("redshift", region_name=resolved, config=config).describe_clusters(), render=None)
    if clusters is not None:
        rows = [["cluster", "estado", "endpoint", "banco", "versão", "nós", "papel IAM padrão", "papéis IAM", "vpc", "roteamento VPC", "público", "criptografado"]]
        for cluster in clusters.get("Clusters", []):
            endpoint = cluster.get("Endpoint") or {}
            rows.append([
                cluster.get("ClusterIdentifier"), cluster.get("ClusterStatus"), f"{endpoint.get('Address')}:{endpoint.get('Port')}", cluster.get("DBName"),
                cluster.get("ClusterVersion"), f"{cluster.get('NumberOfNodes')} x {cluster.get('NodeType')}", cluster.get("DefaultIamRoleArn") or "-",
                ", ".join(role.get("IamRoleArn", "") for role in cluster.get("IamRoles", [])) or "-",
                cluster.get("VpcId"), cluster.get("EnhancedVpcRouting"), cluster.get("PubliclyAccessible"), cluster.get("Encrypted"),
            ])
        report.table(rows if len(rows) > 1 else [["(nenhum cluster provisionado visível)"]])
    workgroups = report.call("redshift-serverless.list_workgroups()", lambda: boto3.client("redshift-serverless", region_name=resolved, config=config).list_workgroups(), render=None)
    namespaces: dict[str, dict] = {}
    if workgroups is not None:
        rows = [["workgroup", "estado", "endpoint", "namespace", "capacidade", "público", "roteamento VPC"]]
        for workgroup in workgroups.get("workgroups", []):
            endpoint = workgroup.get("endpoint") or {}
            rows.append([workgroup.get("workgroupName"), workgroup.get("status"), f"{endpoint.get('address')}:{endpoint.get('port')}", workgroup.get("namespaceName"), workgroup.get("baseCapacity"), workgroup.get("publiclyAccessible"), workgroup.get("enhancedVpcRouting")])
        report.table(rows if len(rows) > 1 else [["(nenhum workgroup serverless visível)"]])
        for workgroup in workgroups.get("workgroups", []):
            name = workgroup.get("namespaceName")
            namespace = report.call(f"redshift-serverless.get_namespace(namespaceName={name!r})", lambda n=name: boto3.client("redshift-serverless", region_name=resolved, config=config).get_namespace(namespaceName=n)["namespace"], render=None)
            if namespace:
                namespaces[name] = namespace
                report.table([["namespace", "banco", "papel IAM padrão", "papéis IAM", "chave KMS"], [namespace.get("namespaceName"), namespace.get("dbName"), namespace.get("defaultIamRoleArn") or "-", ", ".join(namespace.get("iamRoles", [])) or "-", namespace.get("kmsKeyId") or "-"]])
    answered = clusters is not None or workgroups is not None
    if answered:
        report.ok("RS-2", "APIs do Redshift", "responderam")
    else:
        report.note("RS-2", "APIs do Redshift", "sem resposta ou negadas: a autenticação por IAM depende delas; ver a seção final")
    roles = [cluster.get("DefaultIamRoleArn") for cluster in (clusters or {}).get("Clusters", [])] + [namespace.get("defaultIamRoleArn") for namespace in namespaces.values()]
    roles = [role for role in roles if role]
    target.roles = roles
    if roles:
        report.ok("RS-6", "papel IAM padrão para COPY e UNLOAD", ", ".join(roles))
    elif answered and ((clusters or {}).get("Clusters") or (workgroups or {}).get("workgroups")):
        report.fail("RS-6", "papel IAM padrão para COPY e UNLOAD", "nenhum cluster ou workgroup tem papel padrão: o COPY precisará de IAM_ROLE explícito")
    else:
        report.note("RS-6", "papel IAM padrão para COPY e UNLOAD", "não lido")
    if target.source == "nada":
        names = [cluster.get("ClusterIdentifier") for cluster in (clusters or {}).get("Clusters", [])] + [workgroup.get("workgroupName") for workgroup in (workgroups or {}).get("workgroups", [])]
        if names:
            report.line(f"para conectar por IAM, informe SERIALIZE_DB_REDSHIFT_CLUSTER ou _WORKGROUP com um de: {', '.join(str(name) for name in names)}, e SERIALIZE_DB_REDSHIFT_DATABASE")
    # A Data API executa SQL por HTTPS, sem a porta 5439: o caminho de reserva se a rede fechar a porta.
    if target.database and (target.workgroup or (target.cluster and target.user)):
        data = boto3.client("redshift-data", region_name=resolved, config=config)
        if target.workgroup:
            call = lambda: data.list_databases(WorkgroupName=target.workgroup, Database=target.database)  # noqa: E731
        else:
            call = lambda: data.list_databases(ClusterIdentifier=target.cluster, Database=target.database, DbUser=target.user)  # noqa: E731
        listed = report.call("redshift-data.list_databases(...)", call, render=lambda found: ", ".join(found.get("Databases", [])) or "(nenhum banco listado)")
        report.note("RS-10", "Data API", "responde: caminho alternativo por HTTPS, sem a porta 5439" if listed is not None else "sem resposta ou negada; ver a seção final")
    else:
        report.note("RS-10", "Data API", "não testada: precisa de SERIALIZE_DB_REDSHIFT_DATABASE e de _WORKGROUP, ou de _CLUSTER com _USER")


def network(report: Report, target: Target) -> None:
    report.h1("Rede")
    resolved = region()
    names = [f"{service}.{resolved}.amazonaws.com" for service in SERVICES] if resolved else []
    if target.host:
        names.append(target.host)
    rows = [["nome", "endereços", "tipo"]]
    for name in names:
        try:
            addresses, private = resolve(name)
            rows.append([name, ", ".join(addresses[:4]), "privado: endpoint VPC de interface com DNS privado" if private else "público"])
        except OSError as error:
            rows.append([name, f"não resolve: {error}", "-"])
            report.failures.append((f"dns {name}", describe_error(error)))
    report.table(rows)
    if target.host:
        opened = report.call(f"tcp {target.host}:{target.port}", lambda: tcp_open(target.host or "", target.port, 5), render=lambda seconds: f"conectou em {seconds:.2f} s")
        if opened is None:
            report.fail("RS-3", "host por TCP", f"{target.host}:{target.port} sem conexão; ver a seção final")
        else:
            report.ok("RS-3", "host por TCP", f"{target.host}:{target.port}")
    else:
        report.note("RS-3", "host por TCP", "sem host conhecido; a autenticação por IAM resolve o host pela API")


def session(report: Report, target: Target) -> None:
    report.h1("Sessão")
    if target.source == "nada":
        report.note("RS-4", "sessão", "sem configuração, nada a conectar")
        return
    if not target.database:
        report.fail("RS-4", "sessão", "sem banco: informe SERIALIZE_DB_REDSHIFT_DATABASE")
        return
    import redshift_connector

    resolved = region()

    def connect() -> tuple[str, object]:
        common = {"database": target.database, "timeout": 10}
        if target.host and target.user and target.password:
            return "senha", redshift_connector.connect(host=target.host, port=target.port, user=target.user, password=target.password, ssl=True, **common)
        if target.workgroup:
            return "IAM serverless", redshift_connector.connect(iam=True, is_serverless=True, serverless_work_group=target.workgroup, region=resolved, **common)
        if target.cluster:
            return "IAM cluster", redshift_connector.connect(iam=True, cluster_identifier=target.cluster, db_user=target.user, region=resolved, **common)
        raise RuntimeError("faltam parâmetros: host, usuário e senha, ou cluster ou workgroup para autenticação por IAM")

    result = report.call("redshift_connector.connect(...)", connect, render=lambda found: f"conectou por {found[0]}")
    if result is None:
        report.fail("RS-4", "sessão", "conexão falhou; ver a seção final")
        return
    method, connection = result
    connection.autocommit = True
    report.ok("RS-4", "sessão", f"aberta por {method}" + ("; a autenticação por IAM pode ter criado o usuário do banco" if method.startswith("IAM") else ""))

    def query(sql: str, params: tuple = ()) -> tuple[list[str], list[tuple]]:
        cursor = connection.cursor()
        cursor.execute(sql, params)
        rows = cursor.fetchall()
        return [column[0] for column in cursor.description or []], rows

    def render(found: tuple[list[str], list[tuple]]) -> str:
        columns, rows = found
        return tabulate([columns, *[[str(value) for value in row] for row in rows]]) if rows else "(nenhuma linha)"

    version = report.call("select version()", lambda: query("select version()"), render=render)
    if version and version[1]:
        # O patch (1.0.NNNNN) diz quais recursos existem: MERGE, SUPER, o COPY de Parquet com FILLRECORD.
        import re

        patch = re.search(r"Redshift (\d+\.\d+\.\d+)", str(version[1][0][0]))
        report.value("REDSHIFT_VERSION", patch.group(1) if patch else str(version[1][0][0])[:80])
    report.call("select current_user, current_database(), current_schema()", lambda: query("select current_user, current_database(), current_schema()"), render=render)
    report.call("show search_path", lambda: query("show search_path"), render=render)
    report.call("esquemas visíveis", lambda: query("select nspname, pg_get_userbyid(nspowner) as owner from pg_namespace where nspname not like 'pg_%%' and nspname <> 'information_schema' order by 1"), render=render)
    schema = target.schema
    if schema:
        privileges = report.call(f"has_schema_privilege({schema!r}, USAGE | CREATE)", lambda: query("select has_schema_privilege(%s, 'USAGE') as usage, has_schema_privilege(%s, 'CREATE') as create", (schema, schema)), render=render)
        if privileges and privileges[1]:
            usage, create = privileges[1][0]
            if usage and create:
                report.ok("RS-5", "privilégios no esquema", f"{schema}: USAGE e CREATE")
            else:
                report.fail("RS-5", "privilégios no esquema", f"{schema}: USAGE={usage}, CREATE={create}")
        else:
            report.fail("RS-5", "privilégios no esquema", f"{schema}: não lidos; ver a seção final")
        tables = report.call(f"svv_table_info do esquema {schema!r}", lambda: query('select "table", tbl_rows, size as size_mb, diststyle, sortkey1 from svv_table_info where schema = %s order by 1', (schema,)), render=render)
        if tables is not None:
            mine = [row for row in tables[1] if str(row[0]).startswith(TABLE_PREFIX)]
            report.note("RS-8", "tabelas com o prefixo da biblioteca no esquema", f"{len(mine)} de {len(tables[1])} tabelas em {schema}")
    else:
        report.note("RS-5", "privilégios no esquema", "sem SERIALIZE_DB_REDSHIFT_SCHEMA")
    parsed = report.call("select json_parse(...)  (SUPER)", lambda: query("select json_parse(%s) as super_value", ('{"a": 1}',)), render=render)
    if parsed is not None:
        report.ok("RS-7", "SUPER e JSON_PARSE", "disponíveis")
    else:
        report.note("RS-7", "SUPER e JSON_PARSE", "falhou; ver a seção final")
    # As configurações que mudam o comportamento do SQL gerado: datas, fuso, tempo limite e sensibilidade a maiúsculas.
    report.call(
        "pg_settings (datestyle, timezone, statement_timeout, search_path, enable_case_sensitive_identifier)",
        lambda: query("select name, setting from pg_settings where name in ('datestyle', 'timezone', 'statement_timeout', 'search_path', 'enable_case_sensitive_identifier', 'wlm_query_slot_count') order by 1"),
        render=render,
    )
    report.call("pg_user do usuário atual", lambda: query("select usename, usesuper, usecreatedb from pg_user where usename = current_user"), render=render)
    privileges = report.call(
        "has_database_privilege(current_database(), CREATE | TEMP)",
        lambda: query("select has_database_privilege(current_database(), 'CREATE') as create_db, has_database_privilege(current_database(), 'TEMP') as temp_db"),
        render=render,
    )
    if privileges and privileges[1]:
        create_db, temp_db = privileges[1][0]
        report.note("RS-9", "privilégios no banco", f"CREATE={create_db} (esquema externo {'possível' if create_db else 'impossível'}), TEMP={temp_db} (staging temporária {'possível' if temp_db else 'impossível'})")
    else:
        report.note("RS-9", "privilégios no banco", "não lidos; ver a seção final")
    # Um COPY reprovado explica o motivo em stl_load_errors (ou sys_load_error_detail); sem leitura, o diagnóstico depende do administrador.
    errors = report.call("stl_load_errors dos últimos 30 dias", lambda: query("select count(*) from stl_load_errors where starttime > dateadd(day, -30, getdate())"), render=render)
    if errors is not None:
        report.note("RS-12", "diagnóstico do COPY", f"stl_load_errors legível: {errors[1][0][0]} erro(s) de carga em 30 dias")
    else:
        detail = report.call("sys_load_error_detail dos últimos 30 dias", lambda: query("select count(*) from sys_load_error_detail where start_time > dateadd(day, -30, getdate())"), render=render)
        report.note("RS-12", "diagnóstico do COPY", "sys_load_error_detail legível" if detail is not None else "nem stl_load_errors nem sys_load_error_detail legíveis: o motivo de um COPY reprovado virá do administrador")
    external = report.call("svv_external_schemas", lambda: query("select count(*) from svv_external_schemas"), render=render)
    if external is not None:
        report.note("RS-13", "esquemas externos (Spectrum)", f"{external[1][0][0]} no banco; a biblioteca não os usa, e a contagem mostra se o Glue chegou ao Redshift")
    connection.close()


def copy_role(report: Report, target: Target) -> None:
    """Se o papel padrão do ``COPY`` e do ``UNLOAD`` alcança a raiz S3 informada, pela simulação de política do IAM."""
    import boto3

    report.h1("Papel do COPY e do UNLOAD sobre a raiz S3")
    root = (sys.argv[1] if len(sys.argv) > 1 else os.environ.get("SERIALIZE_DB_TEST_S3_ROOT", "")).rstrip("/")
    if not root.startswith("s3://"):
        report.note("RS-11", "papel do COPY sobre a raiz", "sem raiz: informe s3://bucket/prefixo como argumento ou em SERIALIZE_DB_TEST_S3_ROOT")
        return
    if not target.roles:
        report.note("RS-11", "papel do COPY sobre a raiz", "sem papel padrão conhecido (RS-6): o COPY precisará de IAM_ROLE explícito, e a suíte Redshift é o teste")
        return
    bucket, _, prefix = root.removeprefix("s3://").partition("/")
    resources = [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/{prefix}/*" if prefix else f"arn:aws:s3:::{bucket}/*"]
    iam = boto3.client("iam", config=short_config())
    for role in target.roles:
        found = report.call(
            f"iam.simulate_principal_policy({role}: ListBucket, GetObject, PutObject em {root})",
            lambda r=role: iam.simulate_principal_policy(PolicySourceArn=r, ActionNames=["s3:ListBucket", "s3:GetObject", "s3:PutObject"], ResourceArns=resources),
            render=lambda found: tabulate([["ação", "decisão"], *[[item["EvalActionName"], item["EvalDecision"]] for item in found.get("EvaluationResults", [])]]),
        )
        if found is None:
            report.note("RS-11", "papel do COPY sobre a raiz", f"{role}: simulação negada ou sem resposta; o primeiro COPY da suíte Redshift é o teste")
            continue
        denied = [item["EvalActionName"] for item in found.get("EvaluationResults", []) if item["EvalDecision"] != "allowed"]
        if denied:
            report.fail("RS-11", "papel do COPY sobre a raiz", f"{role}: {', '.join(denied)} negadas sob {root}")
        else:
            report.ok("RS-11", "papel do COPY sobre a raiz", f"{role}: ListBucket, GetObject e PutObject sob {root}")


def main() -> int:
    report = Report("redshift", "o Redshift do projeto visto de dentro")
    target = Target()
    for section in (configuration, apis, network, session, copy_role):
        try:
            result = section(report) if section is configuration else section(report, target)
            if section is configuration and isinstance(result, Target):
                target = result
        except Exception as error:  # noqa: BLE001 - uma seção interrompida não cala as outras
            report.line(f"!! seção {section.__name__} interrompida: {describe_error(error)}")
            report.failures.append((f"seção {section.__name__}", describe_error(error)))
    return report.finish()


if __name__ == "__main__":
    sys.exit(main())
