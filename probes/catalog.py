"""Os serviços de catálogo alcançáveis do espaço: Glue, Athena, Lake Formation e S3 Tables.

A decisão em ``docs/estrategia.md`` dispensa serviço de catálogo; este probe mede o gatilho de
reavaliação registrado lá: se o Glue ou o S3 Tables passarem a existir e a responder ao papel do
projeto, o DuckLake e o Iceberg com catálogo voltam à mesa.

Uso:

    .venv/bin/python probes/catalog.py

Só leitura. O relatório sai no terminal e em ``probes/output/catalog_<data-hora>.txt``. Seções:

1. Rede: DNS dos endpoints regionais (IP privado indica endpoint VPC de interface com DNS privado).
2. Glue: bancos, tabelas com formato (Iceberg, Parquet, Delta) e catálogos federados (Lakehouse).
3. Athena: workgroups e o local de resultados do primário.
4. Lake Formation: locais registrados.
5. S3 Tables: table buckets.

Cada seção é uma função, na ordem acima, que documenta as checagens que emite (``CT-1`` a
``CT-6``); ``main`` as chama uma a uma, e uma seção que quebra não cala as outras. Uma negação é
leitura (``note``): o papel do projeto não ter o serviço é a resposta esperada hoje.

Chamadas: ``glue:GetDatabases``, ``GetTables``, ``GetCatalogs``; ``athena:ListWorkGroups``,
``GetWorkGroup``; ``lakeformation:ListResources``; ``s3tables:ListTableBuckets``. Nada é criado.
Códigos de saída: 0 checagens ok, 1 alguma chamada falhou, 2 alguma checagem reprovou.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from probelib import Report, describe_error, dns_rows, pretty, region, short_config  # noqa: E402

# Os serviços cujo endpoint regional a seção de rede resolve.
SERVICES = ("glue", "athena", "lakeformation", "s3tables")

# Quantos bancos do Glue têm as tabelas listadas, e quantos workgroups do Athena são detalhados.
MAX_DATABASES = 5
MAX_WORKGROUPS = 3


def summary(items: list[dict], keys: tuple[str, ...]) -> str:
    """JSON legível de uma lista de respostas, só com as chaves pedidas."""
    return pretty([{key: item.get(key) for key in keys} for item in items])


def network(report: Report, resolved: str | None) -> None:
    """Seção 1, rede: o DNS de cada serviço, como leitura."""
    report.h1("Rede")
    if not resolved:
        report.line("sem região, sem endpoints a resolver")
        return
    rows, _ = dns_rows([f"{service}.{resolved}.amazonaws.com" for service in SERVICES])
    report.table([["nome", "endereços", "tipo"], *rows])


def table_format(table: dict) -> str:
    """O formato de uma tabela do Glue: Iceberg, Delta ou Parquet pelos parâmetros e pelo descritor; senão a classificação."""
    parameters = table.get("Parameters", {})
    descriptor = table.get("StorageDescriptor", {})
    if parameters.get("table_type", "").upper() == "ICEBERG":
        return "Iceberg"
    if parameters.get("spark.sql.sources.provider", "").lower() == "delta" or "delta" in descriptor.get("Location", "").lower():
        return "Delta"
    if "parquet" in descriptor.get("InputFormat", "").lower() or parameters.get("classification", "").lower() == "parquet":
        return "Parquet"
    return parameters.get("classification") or table.get("TableType") or "-"


def glue(report: Report, resolved: str | None) -> None:
    """Seção 2, Glue: ``CT-1`` (responde), ``CT-2`` (tabelas por formato) e ``CT-6`` (catálogos federados)."""
    import boto3

    report.h1("Glue")
    client = boto3.client("glue", region_name=resolved, config=short_config())

    # CT-1: os bancos visíveis ao papel; a negação é a leitura esperada sem catálogo habilitado.
    databases = report.call(
        "glue.get_databases(MaxResults=50)",
        lambda: client.get_databases(MaxResults=50).get("DatabaseList", []),
        render=lambda found: summary(found, ("Name", "LocationUri", "CatalogId", "CreateTime")),
    )
    if databases is None:
        report.note("CT-1", "Glue", f"não lido: {report.last_reason}; sem catálogo Glue para o papel do projeto")
        report.note("CT-2", "tabelas Iceberg no Glue", "não lidas")
    else:
        report.ok("CT-1", "Glue", f"respondeu com {len(databases)} banco(s)")

        # CT-2: as tabelas dos primeiros bancos, com o formato; uma tabela Iceberg é o gatilho de reavaliação.
        formats: dict[str, int] = {}
        rows = [["banco", "tabela", "formato", "local"]]
        for database in databases[:MAX_DATABASES]:
            name = database["Name"]
            tables = report.call(
                f"glue.get_tables(DatabaseName={name!r}, MaxResults=50)",
                lambda n=name: client.get_tables(DatabaseName=n, MaxResults=50).get("TableList", []),
                render=lambda found: f"{len(found)} tabela(s)",
            )
            for table in tables or []:
                kind = table_format(table)
                formats[kind] = formats.get(kind, 0) + 1
                rows.append([name, table.get("Name"), kind, table.get("StorageDescriptor", {}).get("Location", "-")])
        report.table(rows if len(rows) > 1 else [["(nenhuma tabela nos primeiros bancos)"]])
        report.note("CT-2", "tabelas por formato no Glue", ", ".join(f"{kind}: {count}" for kind, count in sorted(formats.items())) or "nenhuma")

    # CT-6: um catálogo federado (Lakehouse) ligaria o Glue ao Redshift ou a outro catálogo.
    catalogs = report.call(
        "glue.get_catalogs()",
        lambda: client.get_catalogs().get("CatalogList", []),
        render=lambda found: summary(found, ("Name", "CatalogId", "CatalogType", "ResourceArn")),
    )
    if catalogs is None:
        report.note("CT-6", "catálogos federados do Glue (Lakehouse)", "não lidos")
    else:
        report.note("CT-6", "catálogos federados do Glue (Lakehouse)", ", ".join(f"{item.get('Name')} ({item.get('CatalogType')})" for item in catalogs) or "nenhum")


def athena(report: Report, resolved: str | None) -> None:
    """Seção 3, Athena: ``CT-3`` (responde) e o local de resultados dos primeiros workgroups."""
    import boto3

    report.h1("Athena")
    client = boto3.client("athena", region_name=resolved, config=short_config())

    groups = report.call(
        "athena.list_work_groups()",
        lambda: client.list_work_groups().get("WorkGroups", []),
        render=lambda found: summary(found, ("Name", "State", "EngineVersion")),
    )
    if groups is None:
        report.note("CT-3", "Athena", f"não lido: {report.last_reason}")
        return
    report.ok("CT-3", "Athena", f"respondeu com {len(groups)} workgroup(s)")

    # O local de resultados de cada workgroup diz onde uma consulta do Athena gravaria; o GetWorkGroup pode ser negado.
    def render_group(found: dict) -> str:
        configuration = found.get("Configuration", {})
        return pretty({key: configuration.get(key) for key in ("ResultConfiguration", "EnforceWorkGroupConfiguration", "EngineVersion", "BytesScannedCutoffPerQuery")})

    for group in groups[:MAX_WORKGROUPS]:
        name = group.get("Name")
        details = report.call(
            f"athena.get_work_group(WorkGroup={name!r})",
            lambda n=name: client.get_work_group(WorkGroup=n).get("WorkGroup", {}),
            render=render_group,
        )
        if details is not None:
            location = details.get("Configuration", {}).get("ResultConfiguration", {}).get("OutputLocation")
            report.value(f"ATHENA_RESULTS_{name}", location or "(sem local de resultados)")


def lake_formation(report: Report, resolved: str | None) -> None:
    """Seção 4, Lake Formation: ``CT-4``, os locais registrados, ou a negação."""
    import boto3

    report.h1("Lake Formation")
    client = boto3.client("lakeformation", region_name=resolved, config=short_config())
    resources = report.call(
        "lakeformation.list_resources()",
        lambda: client.list_resources().get("ResourceInfoList", []),
        render=lambda found: summary(found, ("ResourceArn", "RoleArn", "HybridAccessEnabled", "LastModified")),
    )
    if resources is None:
        report.note("CT-4", "Lake Formation", f"não lido: {report.last_reason}")
    else:
        report.note("CT-4", "Lake Formation", f"{len(resources)} local(is) registrado(s)")


def s3_tables(report: Report, resolved: str | None) -> None:
    """Seção 5, S3 Tables: ``CT-5``, os table buckets, ou a negação; um table bucket é o gatilho de reavaliação."""
    import boto3

    report.h1("S3 Tables")
    buckets = report.call(
        "s3tables.list_table_buckets()",
        lambda: boto3.client("s3tables", region_name=resolved, config=short_config()).list_table_buckets().get("tableBuckets", []),
        render=lambda found: summary(found, ("name", "arn", "createdAt")),
    )
    if buckets is None:
        report.note("CT-5", "S3 Tables", f"não lido: {report.last_reason}")
    else:
        report.note("CT-5", "S3 Tables", f"{len(buckets)} table bucket(s)")


def main() -> int:
    report = Report("catalog", "os serviços de catálogo alcançáveis do espaço")
    resolved = region()
    report.value("REGION", resolved)
    for section in (network, glue, athena, lake_formation, s3_tables):
        try:
            section(report, resolved)
        except Exception as error:  # noqa: BLE001 - uma seção interrompida não cala as outras
            report.line(f"!! seção {section.__name__} interrompida: {describe_error(error)}")
            report.failures.append((f"seção {section.__name__}", describe_error(error)))
    return report.finish()


if __name__ == "__main__":
    sys.exit(main())
