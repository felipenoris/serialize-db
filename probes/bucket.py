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
4. Permissões do papel sob a raiz, pela simulação de política do IAM: ``ListBucket`` no bucket,
   ``GetObject``, ``PutObject``, ``DeleteObject`` e ``AbortMultipartUpload`` sob o prefixo, e as ações
   do KMS sobre a chave padrão. A simulação lê as políticas do IAM, não a política da chave.
5. A chave KMS padrão do bucket: estado e gestor, quando o bucket usa SSE-KMS.
6. A política do bucket, com os ``Deny`` condicionados (criptografia, TLS) que valem para o delta-rs e
   o DuckDB, e os uploads multipart incompletos sob a raiz.

Chamadas: ``s3:HeadBucket``, ``GetBucketLocation``, ``GetBucketVersioning``, ``GetBucketEncryption``,
``GetObjectLockConfiguration``, ``GetPublicAccessBlock``, ``GetBucketOwnershipControls``,
``GetBucketLifecycleConfiguration``, ``ListBucket``, ``HeadObject``, ``GetBucketPolicy`` e
``ListMultipartUploads``; ``sts:GetCallerIdentity``; ``iam:SimulatePrincipalPolicy``;
``kms:DescribeKey``. Nada é gravado. Códigos de saída: 0 checagens ok, 1 alguma chamada falhou,
2 alguma checagem reprovou.
"""

from __future__ import annotations

import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from probelib import Report, answered, describe_error, pretty, region, resolve, short_config, tabulate  # noqa: E402

MAX_PAGES = 20  # 20.000 objetos
MAX_SECONDS = 30

# O que a biblioteca faz no S3: listar o prefixo, ler, gravar e apagar objetos, abortar um multipart interrompido.
BUCKET_ACTIONS = ("s3:ListBucket",)
OBJECT_ACTIONS = ("s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload")
KMS_ACTIONS = ("kms:GenerateDataKey", "kms:Decrypt")


def bucket_settings(report: Report, client, bucket: str, resolved: str | None) -> str | None:
    """Configuração do bucket; devolve a chave KMS padrão, quando há, para as seções seguintes."""
    report.h1("Bucket")
    kms_key: str | None = None
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
        kms_key = default.get("KMSMasterKeyID") or None
    report.call("s3.get_object_lock_configuration()", lambda: client.get_object_lock_configuration(Bucket=bucket))
    report.call("s3.get_public_access_block()", lambda: client.get_public_access_block(Bucket=bucket))
    report.call("s3.get_bucket_ownership_controls()", lambda: client.get_bucket_ownership_controls(Bucket=bucket))
    return kms_key


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


def principal_arn(caller_arn: str) -> str:
    """O ARN que a simulação de política aceita: o papel por trás de um assumed-role, ou o próprio usuário."""
    if ":assumed-role/" in caller_arn:
        account = caller_arn.split(":")[4]
        role = caller_arn.split(":assumed-role/")[1].split("/")[0]
        return f"arn:aws:iam::{account}:role/{role}"
    return caller_arn


def decisions(found: dict) -> str:
    return tabulate([["ação", "decisão"], *[[item["EvalActionName"], item["EvalDecision"]] for item in found.get("EvaluationResults", [])]])


def permissions(report: Report, bucket: str, prefix: str, resolved: str | None, kms_key: str | None) -> None:
    """O que o papel pode fazer sob a raiz, pela simulação de política do IAM; sem ela, a suíte S3 é o teste."""
    import boto3

    report.h1("Permissões do papel sob a raiz")
    session = boto3.Session()
    caller = report.call("sts.get_caller_identity()", lambda: session.client("sts", region_name=resolved, config=short_config(5, 10, 1)).get_caller_identity())
    if not caller:
        report.note("BK-8", "permissões sob a raiz", "identidade não lida: sem o STS não há simulação; a suíte S3 (SERIALIZE_DB_TEST_S3_ROOT) é o teste")
        return
    principal = principal_arn(caller["Arn"])
    report.value("PRINCIPAL", principal)
    object_arn = f"arn:aws:s3:::{bucket}/{prefix}/*" if prefix else f"arn:aws:s3:::{bucket}/*"
    iam = session.client("iam", config=short_config())
    results: dict[str, str] = {}
    for actions, resources in ((BUCKET_ACTIONS, [f"arn:aws:s3:::{bucket}"]), (OBJECT_ACTIONS, [object_arn])):
        found = report.call(
            f"iam.simulate_principal_policy({', '.join(actions)} em {resources[0]})",
            lambda a=actions, r=resources: iam.simulate_principal_policy(PolicySourceArn=principal, ActionNames=list(a), ResourceArns=r),
            render=decisions,
        )
        if found is None:
            report.note("BK-8", "permissões sob a raiz", "iam:SimulatePrincipalPolicy negado ou sem resposta: a suíte S3 (SERIALIZE_DB_TEST_S3_ROOT) é o teste")
            return
        results.update({item["EvalActionName"]: item["EvalDecision"] for item in found.get("EvaluationResults", [])})
    if kms_key and kms_key.startswith("arn:"):
        found = report.call(
            f"iam.simulate_principal_policy({', '.join(KMS_ACTIONS)} em {kms_key})",
            lambda: iam.simulate_principal_policy(PolicySourceArn=principal, ActionNames=list(KMS_ACTIONS), ResourceArns=[kms_key]),
            render=decisions,
        )
        if found is not None:
            results.update({item["EvalActionName"]: item["EvalDecision"] for item in found.get("EvaluationResults", [])})
    elif kms_key:
        report.line(f"chave KMS {kms_key} sem ARN: a simulação das ações do KMS precisa do ARN da chave\n")
    denied = [action for action, decision in results.items() if decision != "allowed"]
    if denied:
        report.fail("BK-8", "permissões sob a raiz", f"negadas pela simulação: {', '.join(denied)}; a política da chave KMS não entra na simulação")
    else:
        report.ok("BK-8", "permissões sob a raiz", f"permitidas: {', '.join(results)}")


def kms_key_section(report: Report, resolved: str | None, kms_key: str | None) -> None:
    """A chave que criptografa cada objeto gravado; uma chave desabilitada reprova toda escrita."""
    import boto3

    report.h1("Chave KMS padrão do bucket")
    if not kms_key:
        report.note("BK-9", "chave KMS", "o bucket não usa SSE-KMS por padrão, ou a criptografia não foi lida")
        return
    described = report.call(
        f"kms.describe_key(KeyId={kms_key!r})",
        lambda: boto3.client("kms", region_name=resolved, config=short_config()).describe_key(KeyId=kms_key)["KeyMetadata"],
        render=lambda meta: pretty({key: meta.get(key) for key in ("Arn", "KeyState", "KeyManager", "Origin", "KeySpec", "Enabled")}),
    )
    if described is None:
        report.note("BK-9", "chave KMS", "describe_key negado ou sem resposta: a escrita da suíte S3 diz se a chave serve")
    elif described.get("KeyState") == "Enabled":
        report.ok("BK-9", "chave KMS", f"{described.get('Arn')} habilitada, gerida por {described.get('KeyManager')}")
    else:
        report.fail("BK-9", "chave KMS", f"estado {described.get('KeyState')}: toda escrita com SSE-KMS falharia")


def policy_and_uploads(report: Report, client, bucket: str, prefix: str) -> None:
    """A política do bucket e os uploads incompletos sob a raiz."""
    report.h1("Política do bucket e uploads incompletos")
    policy = report.call("s3.get_bucket_policy()", lambda: json.loads(client.get_bucket_policy(Bucket=bucket)["Policy"]), render=lambda found: pretty(found, limit=60))
    if policy is None:
        report.note("BK-10", "política do bucket", "sem política, ou leitura negada (NoSuchBucketPolicy ou AccessDenied na seção final)")
    else:
        statements = policy.get("Statement", [])
        denies = [item for item in statements if item.get("Effect") == "Deny"]
        conditions = sorted({key for item in denies for block in item.get("Condition", {}).values() for key in block})
        detail = f"{len(statements)} declaração(ões), {len(denies)} Deny" + (f" com condições {', '.join(conditions)}" if conditions else "")
        report.note("BK-10", "política do bucket", detail + "; um Deny condicionado a cabeçalho de criptografia ou a TLS vale para o delta-rs e o DuckDB também")
    uploads = report.call(
        "s3.list_multipart_uploads(Prefix=raiz)",
        lambda: client.list_multipart_uploads(Bucket=bucket, Prefix=f"{prefix}/" if prefix else "", MaxUploads=100),
        render=lambda found: f"{len(found.get('Uploads', []))} upload(s) em andamento",
    )
    if uploads is not None:
        count = len(uploads.get("Uploads", []))
        report.note("BK-11", "uploads multipart incompletos sob a raiz", f"{count}: sobras de escritas interrompidas custam até uma regra AbortIncompleteMultipartUpload" if count else "nenhum")


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
    kms_key: str | None = None
    try:
        kms_key = bucket_settings(report, client, bucket, resolved)
    except Exception as error:  # noqa: BLE001 - uma seção interrompida não cala as outras
        report.line(f"!! seção bucket_settings interrompida: {describe_error(error)}")
        report.failures.append(("seção bucket_settings", describe_error(error)))
    sections = (
        (lifecycle, (client, bucket, prefix)),
        (inventory, (client, bucket, prefix)),
        (permissions, (bucket, prefix, resolved, kms_key)),
        (kms_key_section, (resolved, kms_key)),
        (policy_and_uploads, (client, bucket, prefix)),
    )
    for section, arguments in sections:
        try:
            section(report, *arguments)
        except Exception as error:  # noqa: BLE001 - uma seção interrompida não cala as outras
            report.line(f"!! seção {section.__name__} interrompida: {describe_error(error)}")
            report.failures.append((f"seção {section.__name__}", describe_error(error)))
    return report.finish()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
