"""O bucket do projeto sob a raiz da biblioteca: configuração, criptografia, ciclo de vida e o que já existe.

Uso:

    .venv/bin/python probes/bucket.py s3://bucket/prefixo

Só leitura. O relatório sai no terminal e em ``probes/output/bucket_<data-hora>.txt``. Seções:

1. Bucket: região, versionamento, criptografia padrão, Object Lock, bloqueio de acesso público e
   propriedade de objetos. Cada leitura pode ser negada ao papel do projeto; a negação é o fato.
2. Ciclo de vida: as regras e se alguma expiração alcança a raiz, porque uma tabela Delta não
   tolera expiração sob a sua pasta.
3. Inventário sob a raiz: objetos e bytes por pasta de primeiro nível, tabelas Delta (pastas com
   ``_delta_log``), classes de armazenamento, objeto mais recente e a criptografia de uma amostra.

Chamadas: ``s3:HeadBucket``, ``GetBucketLocation``, ``GetBucketVersioning``, ``GetBucketEncryption``,
``GetObjectLockConfiguration``, ``GetPublicAccessBlock``, ``GetBucketOwnershipControls``,
``GetBucketLifecycleConfiguration``, ``ListBucket`` e ``HeadObject``. Nada é gravado. Códigos de
saída: 0 checagens ok, 1 alguma chamada falhou, 2 alguma checagem reprovou.
"""

from __future__ import annotations

import os
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from probelib import Report, answered, describe_error, pretty, region, resolve, short_config  # noqa: E402

MAX_PAGES = 20  # 20.000 objetos
MAX_SECONDS = 30


def bucket_settings(report: Report, client, bucket: str, resolved: str | None) -> None:
    report.h1("Bucket")
    head = report.call(f"s3.head_bucket(Bucket={bucket!r})", lambda: client.head_bucket(Bucket=bucket), render=lambda found: pretty(found.get("ResponseMetadata", {}).get("HTTPHeaders", {})))
    if head is None:
        report.fail("BK-1", "bucket acessível", f"{bucket}: head_bucket falhou; ver a seção final")
    else:
        report.ok("BK-1", "bucket acessível", bucket)
        bucket_region = head.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("x-amz-bucket-region")
        report.value("BUCKET_REGION", bucket_region)
        if bucket_region and resolved and bucket_region != resolved:
            report.fail("BK-2", "região do bucket", f"{bucket_region}, e o boto3 resolve {resolved}")
        elif bucket_region:
            report.ok("BK-2", "região do bucket", bucket_region)
        else:
            report.note("BK-2", "região do bucket", "cabeçalho x-amz-bucket-region ausente")
    try:
        addresses, private = resolve(f"{bucket}.s3.{resolved}.amazonaws.com") if resolved else ([], False)
        report.line(f"DNS {bucket}.s3.{resolved}.amazonaws.com: {', '.join(addresses[:4])} ({'privado: endpoint VPC de interface' if private else 'público: gateway endpoint ou internet'})\n")
    except OSError as error:
        report.failures.append(("dns do bucket", describe_error(error)))
    versioning = report.call("s3.get_bucket_versioning()", lambda: client.get_bucket_versioning(Bucket=bucket))
    if versioning is not None:
        report.note("BK-4", "versionamento", versioning.get("Status", "desligado") + "; o Delta não precisa dele, e versões antigas custam")
    else:
        report.note("BK-4", "versionamento", "não lido")
    encryption = report.call("s3.get_bucket_encryption()", lambda: client.get_bucket_encryption(Bucket=bucket))
    if encryption is not None:
        rules = encryption.get("ServerSideEncryptionConfiguration", {}).get("Rules", [])
        default = rules[0].get("ApplyServerSideEncryptionByDefault", {}) if rules else {}
        report.note("BK-5", "criptografia padrão", f"{default.get('SSEAlgorithm', 'nenhuma')} {default.get('KMSMasterKeyID', '')}".strip() + f"; bucket key {rules[0].get('BucketKeyEnabled') if rules else '-'}")
    report.call("s3.get_object_lock_configuration()", lambda: client.get_object_lock_configuration(Bucket=bucket))
    report.call("s3.get_public_access_block()", lambda: client.get_public_access_block(Bucket=bucket))
    report.call("s3.get_bucket_ownership_controls()", lambda: client.get_bucket_ownership_controls(Bucket=bucket))


def lifecycle(report: Report, client, bucket: str, prefix: str) -> None:
    report.h1("Ciclo de vida")
    import botocore.exceptions

    def rules() -> list[dict]:
        try:
            return client.get_bucket_lifecycle_configuration(Bucket=bucket).get("Rules", [])
        except botocore.exceptions.ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchLifecycleConfiguration":
                return []
            raise

    found = report.call("s3.get_bucket_lifecycle_configuration()", rules)
    if found is None:
        report.note("BK-3", "expiração sob a raiz", "regras não lidas (negado ou sem resposta); confirme com quem administra o bucket")
        return
    reaching = []
    for rule in found:
        if rule.get("Status") != "Enabled":
            continue
        rule_prefix = rule.get("Prefix") or rule.get("Filter", {}).get("Prefix") or rule.get("Filter", {}).get("And", {}).get("Prefix") or ""
        expires = any(key in rule for key in ("Expiration", "NoncurrentVersionExpiration"))
        overlaps = prefix.startswith(rule_prefix) or rule_prefix.startswith(prefix)
        if expires and overlaps:
            reaching.append(f"{rule.get('ID', '(sem id)')} (prefixo {rule_prefix!r})")
    if reaching:
        report.fail("BK-3", "expiração sob a raiz", "; ".join(reaching) + ": uma regra de expiração apagaria arquivos de tabelas Delta")
    else:
        report.ok("BK-3", "expiração sob a raiz", f"nenhuma das {len(found)} regras expira objetos sob {prefix or '(raiz do bucket)'}")


def inventory(report: Report, client, bucket: str, prefix: str) -> None:
    report.h1("Inventário sob a raiz")
    listing_prefix = f"{prefix}/" if prefix else ""
    report.value("LISTING_PREFIX", listing_prefix or "(raiz do bucket)")
    totals: Counter[str] = Counter()
    counts: Counter[str] = Counter()
    classes: Counter[str] = Counter()
    delta_tables: set[str] = set()
    newest = None
    first_key = None
    objects = 0
    truncated = False

    def scan() -> str:
        nonlocal newest, first_key, objects, truncated
        started = time.perf_counter()
        paginator = client.get_paginator("list_objects_v2")
        for page_number, page in enumerate(paginator.paginate(Bucket=bucket, Prefix=listing_prefix, PaginationConfig={"PageSize": 1000}), start=1):
            for item in page.get("Contents", []):
                key = item["Key"]
                relative = key[len(listing_prefix):]
                folder = relative.split("/", 1)[0] if "/" in relative else "(arquivos na raiz)"
                totals[folder] += item.get("Size", 0)
                counts[folder] += 1
                classes[item.get("StorageClass", "STANDARD")] += 1
                objects += 1
                first_key = first_key or key
                if newest is None or item["LastModified"] > newest[0]:
                    newest = (item["LastModified"], key)
                if "/_delta_log/" in key:
                    delta_tables.add(key.split("/_delta_log/", 1)[0])
            if page_number >= MAX_PAGES or time.perf_counter() - started > MAX_SECONDS:
                truncated = page.get("IsTruncated", False)
                break
        return f"{objects} objetos" + (" (listagem interrompida no limite)" if truncated else "")

    if report.call(f"s3.list_objects_v2(Bucket={bucket!r}, Prefix={listing_prefix!r})", scan, render=str) is None:
        report.fail("BK-6", "listagem sob a raiz", "falhou; ver a seção final")
        return
    rows = [["pasta", "objetos", "bytes", "GiB"]]
    for folder, size in totals.most_common(30):
        rows.append([folder, counts[folder], size, f"{size / 2**30:.3f}"])
    rows.append(["total", objects, sum(totals.values()), f"{sum(totals.values()) / 2**30:.3f}"])
    report.table(rows)
    report.table([["classe de armazenamento", "objetos"], *[[name, count] for name, count in classes.most_common()]])
    if newest:
        report.line(f"objeto mais recente: {newest[1]} em {newest[0]}\n")
    report.table([["tabela Delta (pasta com _delta_log)"], *[[table] for table in sorted(delta_tables)[:50]]] if delta_tables else [["(nenhuma pasta com _delta_log sob a raiz)"]])
    report.note("BK-6", "tabelas Delta sob a raiz", f"{len(delta_tables)}" + (" (listagem truncada)" if truncated else ""))
    if first_key:
        head = report.call(f"s3.head_object(Key={first_key!r})", lambda: client.head_object(Bucket=bucket, Key=first_key), render=lambda found: pretty({key: found.get(key) for key in ("ServerSideEncryption", "SSEKMSKeyId", "BucketKeyEnabled", "StorageClass", "ContentLength", "LastModified", "VersionId")}))
        if head is not None:
            report.note("BK-7", "criptografia de uma amostra", f"{head.get('ServerSideEncryption', 'nenhuma')} {head.get('SSEKMSKeyId', '')}".strip() + f"; bucket key {head.get('BucketKeyEnabled', '-')}; versionado {'VersionId' in head}")


def main(argv: list[str]) -> int:
    root = (argv[1] if len(argv) > 1 else os.environ.get("SERIALIZE_DB_TEST_S3_ROOT", "")).rstrip("/")
    if not root.startswith("s3://"):
        print("uso: .venv/bin/python probes/bucket.py s3://bucket/prefixo", file=sys.stderr)
        return 2
    import boto3

    bucket, _, prefix = root.removeprefix("s3://").partition("/")
    report = Report("bucket", f"o bucket {bucket} sob {prefix or '(raiz)'}")
    report.value("BUCKET", bucket)
    report.value("PREFIX", prefix)
    resolved = region()
    report.value("REGION", resolved)
    client = boto3.client("s3", region_name=resolved, config=short_config())
    for section, arguments in ((bucket_settings, (client, bucket, resolved)), (lifecycle, (client, bucket, prefix)), (inventory, (client, bucket, prefix))):
        try:
            section(report, *arguments)
        except Exception as error:  # noqa: BLE001 - uma seção interrompida não cala as outras
            report.line(f"!! seção {section.__name__} interrompida: {describe_error(error)}")
            report.failures.append((f"seção {section.__name__}", describe_error(error)))
    return report.finish()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
