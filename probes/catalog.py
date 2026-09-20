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

Chamadas: ``glue:GetDatabases``, ``GetTables``, ``GetCatalogs``; ``athena:ListWorkGroups``,
``GetWorkGroup``; ``lakeformation:ListResources``; ``s3tables:ListTableBuckets``. Nada é criado.
Códigos de saída: 0 checagens ok, 1 alguma chamada falhou, 2 alguma checagem reprovou.
"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from probelib import Report, describe_error, pretty, region, resolve, short_config  # noqa: E402

SERVICES = ("glue", "athena", "lakeformation", "s3tables")
MAX_DATABASES = 5


def network(report: Report, resolved: str | None) -> None:
    report.h1("Rede")
    if not resolved:
        report.line("sem região, sem endpoints a resolver")
        return
    rows = [["nome", "endereços", "tipo"]]
    for service in SERVICES:
        name = f"{service}.{resolved}.amazonaws.com"
        try:
            addresses, private = resolve(name)
            rows.append([name, ", ".join(addresses[:4]), "privado: endpoint VPC de interface com DNS privado" if private else "público"])
        except OSError as error:
            rows.append([name, f"não resolve: {error}", "-"])
            report.failures.append((f"dns {name}", describe_error(error)))
    report.table(rows)


def table_format(table: dict) -> str:
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
    import boto3

    report.h1("Glue")
    client = boto3.client("glue", region_name=resolved, config=short_config())
    databases = report.call("glue.get_databases(MaxResults=50)", lambda: client.get_databases(MaxResults=50).get("DatabaseList", []), render=lambda found: pretty([{key: item.get(key) for key in ("Name", "LocationUri", "CatalogId", "CreateTime")} for item in found]))
    if databases is None:
        report.note("CT-1", "Glue", "sem resposta ou negado: sem catálogo Glue para o papel do projeto")
        report.note("CT-2", "tabelas Iceberg no Glue", "não lidas")
    else:
        report.ok("CT-1", "Glue", f"respondeu com {len(databases)} banco(s)")
        formats: dict[str, int] = {}
        rows = [["banco", "tabela", "formato", "local"]]
        for database in databases[:MAX_DATABASES]:
            name = database["Name"]
            tables = report.call(f"glue.get_tables(DatabaseName={name!r}, MaxResults=50)", lambda n=name: client.get_tables(DatabaseName=n, MaxResults=50).get("TableList", []), render=lambda found: f"{len(found)} tabela(s)")
            for table in tables or []:
                kind = table_format(table)
                formats[kind] = formats.get(kind, 0) + 1
                rows.append([name, table.get("Name"), kind, table.get("StorageDescriptor", {}).get("Location", "-")])
        report.table(rows if len(rows) > 1 else [["(nenhuma tabela nos primeiros bancos)"]])
        report.note("CT-2", "tabelas por formato no Glue", ", ".join(f"{kind}: {count}" for kind, count in sorted(formats.items())) or "nenhuma")
    catalogs = report.call("glue.get_catalogs()", lambda: client.get_catalogs().get("CatalogList", []), render=lambda found: pretty([{key: item.get(key) for key in ("Name", "CatalogId", "CatalogType", "ResourceArn")} for item in found]))
    if catalogs is None:
        report.note("CT-6", "catálogos federados do Glue (Lakehouse)", "não lidos")
    else:
        report.note("CT-6", "catálogos federados do Glue (Lakehouse)", ", ".join(f"{item.get('Name')} ({item.get('CatalogType')})" for item in catalogs) or "nenhum")


def athena(report: Report, resolved: str | None) -> None:
    import boto3

    report.h1("Athena")
    client = boto3.client("athena", region_name=resolved, config=short_config())
    groups = report.call("athena.list_work_groups()", lambda: client.list_work_groups().get("WorkGroups", []), render=lambda found: pretty([{key: item.get(key) for key in ("Name", "State", "EngineVersion")} for item in found]))
    if groups is None:
        report.note("CT-3", "Athena", "sem resposta ou negado")
        return
    report.ok("CT-3", "Athena", f"respondeu com {len(groups)} workgroup(s)")
    for group in groups[:3]:
        name = group.get("Name")
        details = report.call(f"athena.get_work_group(WorkGroup={name!r})", lambda n=name: client.get_work_group(WorkGroup=n).get("WorkGroup", {}), render=lambda found: pretty({key: found.get("Configuration", {}).get(key) for key in ("ResultConfiguration", "EnforceWorkGroupConfiguration", "EngineVersion", "BytesScannedCutoffPerQuery")}))
        if details is not None:
            location = details.get("Configuration", {}).get("ResultConfiguration", {}).get("OutputLocation")
            report.value(f"ATHENA_RESULTS_{name}", location or "(sem local de resultados)")


def lake_formation(report: Report, resolved: str | None) -> None:
    import boto3

    report.h1("Lake Formation")
    client = boto3.client("lakeformation", region_name=resolved, config=short_config())
    resources = report.call("lakeformation.list_resources()", lambda: client.list_resources().get("ResourceInfoList", []), render=lambda found: pretty([{key: item.get(key) for key in ("ResourceArn", "RoleArn", "HybridAccessEnabled", "LastModified")} for item in found]))
    if resources is None:
        report.note("CT-4", "Lake Formation", "sem resposta ou negado")
    else:
        report.note("CT-4", "Lake Formation", f"{len(resources)} local(is) registrado(s)")


def s3_tables(report: Report, resolved: str | None) -> None:
    import boto3

    report.h1("S3 Tables")
    buckets = report.call("s3tables.list_table_buckets()", lambda: boto3.client("s3tables", region_name=resolved, config=short_config()).list_table_buckets().get("tableBuckets", []), render=lambda found: pretty([{key: item.get(key) for key in ("name", "arn", "createdAt")} for item in found]))
    if buckets is None:
        report.note("CT-5", "S3 Tables", "sem resposta, negado ou sem o serviço neste boto3")
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
