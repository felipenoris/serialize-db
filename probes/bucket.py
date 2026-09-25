"""O bucket do projeto sob a raiz: configuração, criptografia, ciclo de vida e o que já existe.

Uso:

    .venv/bin/python probes/bucket.py s3://bucket/prefixo

O probe só lê. O relatório sai no terminal e em ``probes/output/bucket_<data-hora>.txt``. Seções:

1. Bucket: região, versionamento, criptografia padrão, Object Lock, bloqueio de acesso público e
   propriedade de objetos. Cada leitura pode ser negada ao papel do projeto; a negação é o fato.
2. Ciclo de vida: as regras e se alguma expiração alcança a raiz, porque uma tabela Delta não
   tolera expiração sob a sua pasta.
3. Inventário sob a raiz: objetos e bytes por pasta de primeiro nível, tabelas Delta (pastas com
   ``_delta_log``, com arquivos de dados, bytes, commits no log, checkpoints e último objeto),
   sessões da suíte S3 (``serialize-db-poc/<id>``), classes de armazenamento, objeto mais recente,
   a criptografia de uma amostra e o que o versionamento acumulou sob a raiz: versões não correntes
   e marcadores de exclusão, invisíveis à listagem comum.
4. Permissões do papel sob a raiz, pela simulação de política do IAM: ``ListBucket`` no bucket,
   ``GetObject``, ``PutObject``, ``DeleteObject`` e ``AbortMultipartUpload`` sob o prefixo, e as
   ações do KMS sobre a chave padrão. A simulação lê as políticas do IAM, não a política da chave.
5. A chave KMS padrão do bucket: estado e gestor, quando o bucket usa SSE-KMS.
6. A política do bucket, com os ``Deny`` condicionados (criptografia, TLS) que valem para o
   delta-rs e o DuckDB, e os uploads multipart incompletos sob a raiz.

Cada seção é uma função, na ordem acima, que documenta as checagens que emite (``BK-1`` a
``BK-14``); ``main`` as chama com ``guarded``, e uma seção que quebra não cala as outras. A checagem
``BK-4`` (versionamento) sai depois do inventário, porque a amostra do inventário a decide quando a
API é negada.

Chamadas: ``s3:HeadBucket``, ``GetBucketVersioning``, ``GetBucketEncryption``,
``GetObjectLockConfiguration``, ``GetPublicAccessBlock``, ``GetBucketOwnershipControls``,
``GetBucketLifecycleConfiguration``, ``ListBucket``, ``ListBucketVersions``, ``HeadObject``,
``GetBucketPolicy`` e ``ListMultipartUploads``; ``sts:GetCallerIdentity``;
``iam:SimulatePrincipalPolicy``; ``kms:DescribeKey``. Códigos de saída: 0 checagens ok, 1 alguma
chamada falhou, 2 alguma checagem reprovou.
"""

from __future__ import annotations

import json
import os
import re
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))
import probelib  # noqa: E402
from probelib import Report, describe_error, dns_rows, error_code, pretty, region, short_config, tabulate  # noqa: E402

# Limites das listagens: uma raiz com o banco inteiro pode ter centenas de milhares de objetos.
MAX_PAGES = 20  # As 20 páginas de 1.000 somam até 20.000 objetos.
MAX_SECONDS = 30

# serialize-db-poc/<id>: o id de sessão que tests/conftest.py gera (oito dígitos hexadecimais).
SESSION_ID = re.compile(r"^[0-9a-f]{8}$")

# O que a biblioteca faz no S3: listar o prefixo, ler, gravar e apagar objetos, abortar um
# multipart interrompido.
BUCKET_ACTIONS = ("s3:ListBucket",)
OBJECT_ACTIONS = ("s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:AbortMultipartUpload")
KMS_ACTIONS = ("kms:GenerateDataKey", "kms:Decrypt")

# Os cabeçalhos do HeadBucket e os campos do HeadObject que o relatório mostra.
HEAD_BUCKET_HEADERS = ("x-amz-bucket-region", "x-amz-bucket-arn", "x-amz-access-point-alias")
HEAD_OBJECT_FIELDS = ("ServerSideEncryption", "SSEKMSKeyId", "BucketKeyEnabled", "StorageClass", "ContentLength", "LastModified", "VersionId")

# Uma entrada da listagem: chave, tamanho e data de modificação.
Entry = tuple[str, int, Any]


# --------------------------------------------------------------------------------------------------
# Seção 1: o bucket


def render_head_bucket(found: dict) -> str:
    """Os cabeçalhos úteis do HeadBucket: região, ARN e se o nome é alias de access point."""
    headers = found.get("ResponseMetadata", {}).get("HTTPHeaders", {})
    return pretty({key: value for key, value in headers.items() if key in HEAD_BUCKET_HEADERS})


def bucket_settings(report: Report, client, bucket: str, resolved: str | None) -> tuple[str | None, str | None, str]:
    """Seção 1, o bucket.

    Checagens: ``BK-1`` (acessível), ``BK-2`` (região), ``BK-5`` (criptografia) e ``BK-12``
    (Object Lock). Devolve a chave KMS padrão, o estado do versionamento (``None`` quando não lido)
    e o motivo, para ``BK-4`` e para a seção da chave.
    """
    report.h1("Bucket")
    kms_key: str | None = None

    # BK-1 e BK-2: o bucket responde ao HeadBucket, e a região dele é a que o boto3 resolve.
    head = report.call(f"s3.head_bucket(Bucket={bucket!r})", lambda: client.head_bucket(Bucket=bucket), render=render_head_bucket)
    if head is None:
        report.fail("BK-1", "bucket acessível", f"{bucket}: {report.last_reason}; ver a seção final")
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

    # O nome virtual do bucket resolve para IP público mesmo atrás de um gateway endpoint, e o
    # rótulo da linha o diz.
    if resolved:
        rows, _ = dns_rows([f"{bucket}.s3.{resolved}.amazonaws.com"])
        report.line(f"DNS {rows[0][0]}: {rows[0][1]} ({rows[0][2]})\n")

    # BK-4 sai depois do inventário: com a API negada, a amostra com VersionId prova o
    # versionamento.
    versioning = report.call("s3.get_bucket_versioning()", lambda: client.get_bucket_versioning(Bucket=bucket))
    status = versioning.get("Status", "desligado") if versioning is not None else None
    versioning_reason = report.last_reason if versioning is None else "lido"

    # BK-5: a criptografia padrão diz se toda escrita passa pelo KMS, e por qual chave.
    encryption = report.call("s3.get_bucket_encryption()", lambda: client.get_bucket_encryption(Bucket=bucket))
    if encryption is not None:
        rules = encryption.get("ServerSideEncryptionConfiguration", {}).get("Rules", [])
        default = rules[0].get("ApplyServerSideEncryptionByDefault", {}) if rules else {}
        algorithm = f"{default.get('SSEAlgorithm', 'nenhuma')} {default.get('KMSMasterKeyID', '')}".strip()
        report.note("BK-5", "criptografia padrão", f"{algorithm}; bucket key {rules[0].get('BucketKeyEnabled') if rules else '-'}")
        kms_key = default.get("KMSMasterKeyID") or None

    # BK-12: sem Object Lock o serviço responde ObjectLockConfigurationNotFoundError; a ausência é
    # uma leitura, não uma chamada falhada. Uma versão retida não pode ser apagada, e o vacuum só
    # deixa marcadores até o fim da retenção.
    def object_lock() -> dict:
        try:
            return client.get_object_lock_configuration(Bucket=bucket).get("ObjectLockConfiguration", {})
        except Exception as error:
            if error_code(error) == "ObjectLockConfigurationNotFoundError":
                return {}
            raise

    lock = report.call("s3.get_object_lock_configuration()", object_lock, render=lambda found: pretty(found) if found else "(sem Object Lock)")
    if lock is None:
        report.note("BK-12", "Object Lock", f"não lido: {report.last_reason}")
    elif lock.get("ObjectLockEnabled") != "Enabled":
        report.note("BK-12", "Object Lock", "desativado")
    else:
        retention = lock.get("Rule", {}).get("DefaultRetention", {})
        amount = retention.get("Days") or retention.get("Years")
        unit = ("dia" if retention.get("Days") else "ano") + ("" if amount == 1 else "s")
        period = f"retenção padrão {retention.get('Mode')} por {amount} {unit}" if retention else "sem retenção padrão"
        report.note("BK-12", "Object Lock", f"ativo, {period}; uma versão retida não pode ser apagada, e o vacuum só deixa marcadores de exclusão até o fim da retenção")

    # O bloqueio de acesso público e a propriedade de objetos são leituras sem checagem; a
    # biblioteca não depende deles.
    report.call("s3.get_public_access_block()", lambda: client.get_public_access_block(Bucket=bucket))
    report.call("s3.get_bucket_ownership_controls()", lambda: client.get_bucket_ownership_controls(Bucket=bucket))

    return kms_key, status, versioning_reason


def versioning_check(report: Report, status: str | None, why: str, sample: dict | None) -> None:
    """``BK-4``, o versionamento, pela API ou, com ela negada, pela amostra do inventário."""
    consequence = (
        "cada DeleteObject do vacuum deixa uma versão não corrente, que só uma regra NoncurrentVersionExpiration "
        "remove (BK-14 conta o acumulado); confirme a regra com quem administra o bucket"
    )
    version_id = (sample or {}).get("VersionId")

    if status == "Enabled":
        report.note("BK-4", "versionamento", f"ativo pela API: {consequence}")
    elif status is not None:
        report.note("BK-4", "versionamento", f"{status}; o Delta não precisa dele")
    elif version_id and version_id != "null":
        # Um bucket sem versionamento devolve VersionId "null" ou nenhum; qualquer outro valor prova
        # o versionamento.
        report.note("BK-4", "versionamento", f"pela API, {why}; a amostra tem VersionId: ativo; {consequence}")
    else:
        report.note("BK-4", "versionamento", f"não lido: pela API, {why}, e nenhuma amostra com VersionId")


# --------------------------------------------------------------------------------------------------
# Seção 2: o ciclo de vida


def lifecycle(report: Report, client, bucket: str, prefix: str) -> None:
    """Seção 2: ``BK-3``, se alguma regra de expiração habilitada alcança a raiz."""
    report.h1("Ciclo de vida")
    import botocore.exceptions

    # Sem configuração o serviço responde NoSuchLifecycleConfiguration: a ausência de regras é uma
    # leitura.
    def rules() -> list[dict]:
        try:
            return client.get_bucket_lifecycle_configuration(Bucket=bucket).get("Rules", [])
        except botocore.exceptions.ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchLifecycleConfiguration":
                return []
            raise

    found = report.call("s3.get_bucket_lifecycle_configuration()", rules)
    if found is None:
        report.note("BK-3", "expiração sob a raiz", f"regras não lidas: {report.last_reason}; confirme com quem administra o bucket")
        return

    # BK-3: uma regra habilitada que expira objetos reprova quando o prefixo dela contém a raiz ou
    # está contido nela.
    reaching = []
    for rule in found:
        if rule.get("Status") != "Enabled":
            continue
        filter_block = rule.get("Filter", {})
        rule_prefix = rule.get("Prefix") or filter_block.get("Prefix") or filter_block.get("And", {}).get("Prefix") or ""
        expires = any(key in rule for key in ("Expiration", "NoncurrentVersionExpiration"))
        overlaps = prefix.startswith(rule_prefix) or rule_prefix.startswith(prefix)
        if expires and overlaps:
            reaching.append(f"{rule.get('ID', '(sem id)')} (prefixo {rule_prefix!r})")

    if reaching:
        report.fail("BK-3", "expiração sob a raiz", "; ".join(reaching) + ": uma regra de expiração apagaria arquivos de tabelas Delta")
    else:
        report.ok("BK-3", "expiração sob a raiz", f"nenhuma das {len(found)} regras expira objetos sob {prefix or '(raiz do bucket)'}")


# --------------------------------------------------------------------------------------------------
# Seção 3: o inventário


def delta_table_rows(entries: list[Entry], roots: list[str], listing_prefix: str) -> list[list[object]]:
    """Uma linha por tabela Delta, com o estado que a listagem mostra.

    As colunas são arquivos de dados, bytes, commits no log (``NNN.json``), checkpoints e último
    objeto.
    """
    known = set(roots)
    details: dict[str, dict[str, Any]] = {root: {"files": 0, "bytes": 0, "commits": 0, "checkpoints": 0, "newest": None} for root in roots}

    for key, size, modified in entries:
        # A tabela de um objeto é a pasta mais funda, acima dele, que tem _delta_log.
        parts = key.split("/")
        parents = ("/".join(parts[:depth]) for depth in range(len(parts) - 1, 0, -1))
        root = next((candidate for candidate in parents if candidate in known), None)
        if root is None:
            continue

        found = details[root]
        relative = key[len(root) + 1:]
        if relative.startswith("_delta_log/"):
            # No log, NNNNNNNNNNNNNNNNNNNN.json é um commit e NNN.checkpoint.parquet um checkpoint;
            # _last_checkpoint não conta.
            name = relative.split("/", 1)[1]
            if name.endswith(".json") and name[:-5].isdigit():
                found["commits"] += 1
            elif ".checkpoint" in name and name.endswith(".parquet"):
                found["checkpoints"] += 1
        elif key.endswith(".parquet"):
            found["files"] += 1
            found["bytes"] += size
        if found["newest"] is None or modified > found["newest"]:
            found["newest"] = modified

    # A tabela mostra as 50 primeiras em ordem de caminho, cada uma pelo caminho relativo ao
    # prefixo da listagem.
    rows: list[list[object]] = [["tabela Delta sob a raiz", "arquivos de dados", "bytes", "commits no log", "checkpoints", "último objeto"]]
    for root in roots[:50]:
        found = details[root]
        rows.append([root[len(listing_prefix):] or root, found["files"], found["bytes"], found["commits"], found["checkpoints"], str(found["newest"])])
    return rows


def session_rows(entries: list[Entry], listing_prefix: str) -> list[list[object]]:
    """Uma linha por sessão da suíte S3 (``serialize-db-poc/<id>/``) que ainda existe sob a raiz.

    A mais recente vem primeiro, e a tabela mostra até 20 sessões.
    """
    sessions: dict[str, dict[str, Any]] = {}
    for key, size, modified in entries:
        # Só conta um objeto dentro de serialize-db-poc/<id>/, com o id de sessão da suíte.
        parts = key[len(listing_prefix):].split("/")
        if len(parts) < 3 or parts[0] != "serialize-db-poc" or not SESSION_ID.match(parts[1]):
            continue

        found = sessions.setdefault(parts[1], {"objects": 0, "bytes": 0, "newest": None})
        found["objects"] += 1
        found["bytes"] += size
        if found["newest"] is None or modified > found["newest"]:
            found["newest"] = modified

    rows: list[list[object]] = [["sessão da suíte S3", "objetos", "bytes", "último objeto"]]
    for session_id, found in sorted(sessions.items(), key=lambda item: item[1]["newest"], reverse=True)[:20]:
        rows.append([session_id, found["objects"], found["bytes"], str(found["newest"])])
    return rows


def object_versions(report: Report, client, bucket: str, listing_prefix: str) -> None:
    """``BK-14``: o que o versionamento acumulou sob a raiz.

    As versões não correntes e os marcadores de exclusão são invisíveis a ``list_objects_v2`` e
    cobrados até uma regra removê-los.
    """
    counts = {"current": 0, "noncurrent": 0, "noncurrent_bytes": 0, "markers": 0}
    truncated = False

    def scan() -> str:
        nonlocal truncated
        started = time.perf_counter()
        paginator = client.get_paginator("list_object_versions")
        pages = paginator.paginate(Bucket=bucket, Prefix=listing_prefix, PaginationConfig={"PageSize": 1000})
        for page_number, page in enumerate(pages, start=1):
            # Cada página traz as versões (a corrente tem IsLatest) e, à parte, os marcadores de
            # exclusão.
            for item in page.get("Versions", []):
                if item.get("IsLatest"):
                    counts["current"] += 1
                else:
                    counts["noncurrent"] += 1
                    counts["noncurrent_bytes"] += item.get("Size", 0)
            counts["markers"] += len(page.get("DeleteMarkers", []))
            if page_number >= MAX_PAGES or time.perf_counter() - started > MAX_SECONDS:
                truncated = page.get("IsTruncated", False)
                break
        summary = f"{counts['current']} versões correntes, {counts['noncurrent']} não correntes, {counts['markers']} marcadores de exclusão"
        return summary + (" (listagem interrompida no limite)" if truncated else "")

    # BK-14 é leitura em todo caso: o que o versionamento acumulou, a ausência de acúmulo e a
    # listagem negada.
    listed = report.call("s3.list_object_versions(Prefix=raiz)", scan, render=str)
    limit = " (listagem interrompida no limite)" if truncated else ""
    if listed is None:
        report.note("BK-14", "versões não correntes sob a raiz", f"não lidas: {report.last_reason}; num bucket versionado cada exclusão deixa uma versão não corrente, que só uma regra NoncurrentVersionExpiration remove")
    elif counts["noncurrent"] or counts["markers"]:
        report.note("BK-14", "versões não correntes sob a raiz", f"{counts['noncurrent']} ({counts['noncurrent_bytes']} bytes) e {counts['markers']} marcadores de exclusão, invisíveis à listagem comum e cobrados até uma regra NoncurrentVersionExpiration os remover{limit}")
    else:
        report.note("BK-14", "versões não correntes sob a raiz", f"nenhuma, nem marcador de exclusão{limit}")


def render_head_object(found: dict) -> str:
    """Os campos do HeadObject que interessam.

    São a criptografia, a chave, o bucket key, a classe, o tamanho, a data e o ``VersionId``.
    """
    return pretty({key: found.get(key) for key in HEAD_OBJECT_FIELDS})


def inventory(report: Report, client, bucket: str, prefix: str) -> dict[str, Any]:
    """Seção 3, o inventário sob a raiz.

    Checagens: ``BK-6`` (tabelas Delta), ``BK-13`` (sessões da suíte), ``BK-7`` (amostra) e
    ``BK-14`` (versões). Devolve o que a listagem provou: ``listed`` e, quando houve amostra,
    ``sample`` com o ``head_object`` dela; ``BK-4`` e ``BK-8`` usam os dois.
    """
    report.h1("Inventário sob a raiz")
    listing_prefix = f"{prefix}/" if prefix else ""
    report.value("LISTING_PREFIX", listing_prefix or "(raiz do bucket)")

    # O que a listagem acumula: bytes e objetos por pasta, classes de armazenamento, as entradas e
    # o objeto mais recente.
    totals: Counter[str] = Counter()
    counts: Counter[str] = Counter()
    classes: Counter[str] = Counter()
    entries: list[Entry] = []
    newest = None
    first_key = None
    objects = 0
    truncated = False

    def scan() -> str:
        nonlocal newest, first_key, objects, truncated
        started = time.perf_counter()
        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(Bucket=bucket, Prefix=listing_prefix, PaginationConfig={"PageSize": 1000})
        for page_number, page in enumerate(pages, start=1):
            for item in page.get("Contents", []):
                key = item["Key"]
                relative = key[len(listing_prefix):]
                # A chave igual ao prefixo, ou terminada em "/", é o marcador de pasta que o
                # console e o s3fs criam.
                if not relative or key.endswith("/"):
                    folder = "(marcador de pasta)"
                elif "/" in relative:
                    folder = relative.split("/", 1)[0]
                else:
                    folder = "(arquivos na raiz)"
                totals[folder] += item.get("Size", 0)
                counts[folder] += 1
                classes[item.get("StorageClass", "STANDARD")] += 1
                objects += 1
                first_key = first_key or key
                if newest is None or item["LastModified"] > newest[0]:
                    newest = (item["LastModified"], key)
                entries.append((key, item.get("Size", 0), item["LastModified"]))
            if page_number >= MAX_PAGES or time.perf_counter() - started > MAX_SECONDS:
                truncated = page.get("IsTruncated", False)
                break
        return f"{objects} objetos" + (" (listagem interrompida no limite)" if truncated else "")

    # BK-6 reprova quando a listagem falha: sem ListBucket sob a raiz, nada na biblioteca funciona.
    proven: dict[str, Any] = {"listed": False, "sample": None}
    if report.call(f"s3.list_objects_v2(Bucket={bucket!r}, Prefix={listing_prefix!r})", scan, render=str) is None:
        report.fail("BK-6", "listagem sob a raiz", f"{report.last_reason}; ver a seção final")
        return proven
    proven["listed"] = True

    # Objetos e bytes por pasta de primeiro nível, classes de armazenamento e o objeto mais recente.
    rows = [["pasta", "objetos", "bytes", "GiB"]]
    for folder, size in totals.most_common(30):
        rows.append([folder, counts[folder], size, f"{size / 2**30:.3f}"])
    rows.append(["total", objects, sum(totals.values()), f"{sum(totals.values()) / 2**30:.3f}"])
    report.table(rows)
    report.table([["classe de armazenamento", "objetos"], *[[name, count] for name, count in classes.most_common()]])
    if newest:
        report.line(f"objeto mais recente: {newest[1]} em {newest[0]}\n")

    # BK-6: as tabelas Delta são as pastas com _delta_log; a tabela mostra o estado de cada uma.
    roots = sorted({key.split("/_delta_log/", 1)[0] for key, _, _ in entries if "/_delta_log/" in key})
    report.table(delta_table_rows(entries, roots, listing_prefix) if roots else [["(nenhuma pasta com _delta_log sob a raiz)"]])
    report.note("BK-6", "tabelas Delta sob a raiz", f"{len(roots)}" + (" (listagem truncada)" if truncated else ""))

    # BK-13: a suíte apaga a pasta da sua sessão ao terminar; uma que fica é mantida, interrompida
    # ou em andamento.
    sessions = session_rows(entries, listing_prefix)
    if len(sessions) > 1:
        report.table(sessions)
        total_bytes = sum(int(row[2]) for row in sessions[1:])
        report.note("BK-13", "sessões da suíte S3 sob a raiz", f"{len(sessions) - 1}, {total_bytes} bytes, a mais recente com objeto de {sessions[1][3]}; a suíte apaga a sua ao terminar, salvo SERIALIZE_DB_TEST_KEEP: uma pasta que fica é de sessão mantida, interrompida ou ainda em andamento")
    else:
        report.note("BK-13", "sessões da suíte S3 sob a raiz", "nenhuma")

    # BK-7: o HeadObject de uma amostra diz a criptografia aplicada e, pelo VersionId, o
    # versionamento (BK-4).
    if first_key:
        head = report.call(f"s3.head_object(Key={first_key!r})", lambda: client.head_object(Bucket=bucket, Key=first_key), render=render_head_object)
        if head is not None:
            proven["sample"] = head
            algorithm = f"{head.get('ServerSideEncryption', 'nenhuma')} {head.get('SSEKMSKeyId', '')}".strip()
            report.note("BK-7", "criptografia de uma amostra", f"{algorithm}; bucket key {head.get('BucketKeyEnabled', '-')}; versionado {'VersionId' in head}")

    object_versions(report, client, bucket, listing_prefix)
    return proven


# --------------------------------------------------------------------------------------------------
# Seção 4: as permissões do papel


def decisions(found: dict) -> str:
    """A tabela ação, decisão de uma simulação de política."""
    return tabulate([["ação", "decisão"], *[[item["EvalActionName"], item["EvalDecision"]] for item in found.get("EvaluationResults", [])]])


def permissions(report: Report, bucket: str, prefix: str, resolved: str | None, kms_key: str | None, proven: dict[str, Any]) -> None:
    """Seção 4: ``BK-8``, o que o papel pode fazer sob a raiz, pela simulação de política do IAM.

    Sem a simulação (negada ao papel do projeto, ou com o IAM sem resposta ao teste TCP), a
    checagem registra o que esta execução provou e aponta a suíte S3 como o teste de ``PutObject``
    e ``DeleteObject``.
    """
    import boto3

    report.h1("Permissões do papel sob a raiz")

    # A simulação precisa do ARN do papel, obtido da identidade do STS.
    session = boto3.Session()
    caller = report.call("sts.get_caller_identity()", lambda: session.client("sts", region_name=resolved, config=short_config(5, 10, 1)).get_caller_identity())
    if not caller:
        report.note("BK-8", "permissões sob a raiz", "identidade não lida: sem o STS não há simulação; a suíte S3 (SERIALIZE_DB_TEST_S3_ROOT) é o teste")
        return
    principal = probelib.principal_arn(caller["Arn"])
    report.value("PRINCIPAL", principal)

    # Uma simulação para o bucket (ListBucket) e outra para os objetos sob a raiz.
    object_arn = f"arn:aws:s3:::{bucket}/{prefix}/*" if prefix else f"arn:aws:s3:::{bucket}/*"
    iam = session.client("iam", config=short_config(2, 5, 1))

    def without_simulation(detail: str) -> None:
        """A nota no lugar da simulação: o que esta execução provou e o que só a suíte S3 prova."""
        proofs = (("ListBucket sob a raiz", proven.get("listed")), ("HeadObject de uma amostra", proven.get("sample") is not None))
        shown = [name for name, done in proofs if done]
        report.note("BK-8", "permissões sob a raiz", f"{detail}; nesta execução passaram: {', '.join(shown) or 'nenhuma leitura'}; PutObject e DeleteObject só a suíte S3 (SERIALIZE_DB_TEST_S3_ROOT) prova")

    # O IAM não tem endpoint VPC em todo ambiente; sem o teste TCP, cada simulação esperaria o tempo
    # limite em cada endereço que o nome resolve.
    alcance, leitura = probelib.endpoint_reachable(iam)
    report.line(f"alcance do IAM: {leitura}\n")
    if not alcance:
        without_simulation("iam:SimulatePrincipalPolicy sem chamada: o IAM não respondeu ao teste TCP")
        return

    results: dict[str, str] = {}
    for actions, resources in ((BUCKET_ACTIONS, [f"arn:aws:s3:::{bucket}"]), (OBJECT_ACTIONS, [object_arn])):
        found = report.call(
            f"iam.simulate_principal_policy({', '.join(actions)} em {resources[0]})",
            lambda a=actions, r=resources: iam.simulate_principal_policy(PolicySourceArn=principal, ActionNames=list(a), ResourceArns=r),
            render=decisions,
        )
        if found is None:
            without_simulation(f"iam:SimulatePrincipalPolicy {report.last_reason}")
            return
        results.update({item["EvalActionName"]: item["EvalDecision"] for item in found.get("EvaluationResults", [])})

    # As ações do KMS sobre a chave padrão, quando o bucket usa SSE-KMS; a simulação exige o ARN da
    # chave.
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

    # BK-8: toda ação simulada precisa sair allowed; a simulação não lê a política da chave KMS.
    denied = [action for action, decision in results.items() if decision != "allowed"]
    if denied:
        report.fail("BK-8", "permissões sob a raiz", f"negadas pela simulação: {', '.join(denied)}; a política da chave KMS não entra na simulação")
    else:
        report.ok("BK-8", "permissões sob a raiz", f"permitidas: {', '.join(results)}")


# --------------------------------------------------------------------------------------------------
# Seção 5: a chave KMS


def render_key(meta: dict) -> str:
    """Os campos da chave que decidem se a escrita funciona.

    São o ARN, o estado, o gestor, a origem, o tipo e se ela está habilitada.
    """
    return pretty({key: meta.get(key) for key in ("Arn", "KeyState", "KeyManager", "Origin", "KeySpec", "Enabled")})


def kms_key_section(report: Report, resolved: str | None, kms_key: str | None) -> None:
    """Seção 5: ``BK-9``, a chave KMS que criptografa cada objeto gravado.

    Uma chave desabilitada reprova toda escrita.
    """
    import boto3

    report.h1("Chave KMS padrão do bucket")
    if not kms_key:
        report.note("BK-9", "chave KMS", "o bucket não usa SSE-KMS por padrão, ou a criptografia não foi lida")
        return

    # O KMS resolve para vários endereços; sem endpoint VPC, cada um consome o connect_timeout da
    # chamada.
    kms = boto3.client("kms", region_name=resolved, config=short_config(2, 5, 1))
    alcance, leitura = probelib.endpoint_reachable(kms)
    report.line(f"alcance do KMS: {leitura}\n")
    if not alcance:
        report.note("BK-9", "chave KMS", f"describe_key sem chamada: o KMS não respondeu ao teste TCP ({leitura}); a escrita da suíte S3 diz se a chave serve")
        return

    # BK-9: a chave habilitada passa e a de outro estado reprova; a leitura negada deixa a prova
    # para a escrita da suíte S3.
    described = report.call(
        f"kms.describe_key(KeyId={kms_key!r})",
        lambda: kms.describe_key(KeyId=kms_key)["KeyMetadata"],
        render=render_key,
    )
    if described is None:
        report.note("BK-9", "chave KMS", f"describe_key {report.last_reason}: a escrita da suíte S3 diz se a chave serve")
    elif described.get("KeyState") == "Enabled":
        report.ok("BK-9", "chave KMS", f"{described.get('Arn')} habilitada, gerida por {described.get('KeyManager')}")
    else:
        report.fail("BK-9", "chave KMS", f"estado {described.get('KeyState')}: toda escrita com SSE-KMS falharia")


# --------------------------------------------------------------------------------------------------
# Seção 6: a política do bucket e os uploads incompletos


def policy_and_uploads(report: Report, client, bucket: str, prefix: str) -> None:
    """Seção 6, a política do bucket e os uploads incompletos.

    Checagens: ``BK-10`` (a política do bucket e seus ``Deny``) e ``BK-11`` (uploads multipart
    incompletos).
    """
    report.h1("Política do bucket e uploads incompletos")
    import botocore.exceptions

    # Sem política o serviço responde NoSuchBucketPolicy: a ausência é uma leitura.
    def document() -> dict:
        try:
            return json.loads(client.get_bucket_policy(Bucket=bucket)["Policy"])
        except botocore.exceptions.ClientError as error:
            if error.response.get("Error", {}).get("Code") == "NoSuchBucketPolicy":
                return {}
            raise

    # BK-10: um Deny condicionado a cabeçalho de criptografia ou a TLS vale para o delta-rs e o
    # DuckDB também.
    policy = report.call("s3.get_bucket_policy()", document, render=lambda found: pretty(found, limit=60) if found else "(o bucket não tem política)")
    if policy is None:
        report.note("BK-10", "política do bucket", f"não lida: {report.last_reason}; um Deny condicionado a cabeçalho de criptografia ou a TLS valeria para o delta-rs e o DuckDB")
    elif not policy:
        report.note("BK-10", "política do bucket", "nenhuma")
    else:
        statements = policy.get("Statement", [])
        denies = [item for item in statements if item.get("Effect") == "Deny"]
        conditions = sorted({key for item in denies for block in item.get("Condition", {}).values() for key in block})
        detail = f"{len(statements)} declaração(ões), {len(denies)} Deny" + (f" com condições {', '.join(conditions)}" if conditions else "")
        report.note("BK-10", "política do bucket", detail + "; um Deny condicionado a cabeçalho de criptografia ou a TLS vale para o delta-rs e o DuckDB também")

    # BK-11: as sobras de escritas interrompidas custam até uma regra
    # AbortIncompleteMultipartUpload limpá-las.
    uploads = report.call(
        "s3.list_multipart_uploads(Prefix=raiz)",
        lambda: client.list_multipart_uploads(Bucket=bucket, Prefix=f"{prefix}/" if prefix else "", MaxUploads=100),
        render=lambda found: f"{len(found.get('Uploads', []))} upload(s) em andamento",
    )
    if uploads is not None:
        count = len(uploads.get("Uploads", []))
        report.note("BK-11", "uploads multipart incompletos sob a raiz", f"{count}: sobras de escritas interrompidas custam até uma regra AbortIncompleteMultipartUpload" if count else "nenhum")
    else:
        report.note("BK-11", "uploads multipart incompletos sob a raiz", f"não lidos: {report.last_reason}; as sobras de escritas interrompidas só uma regra AbortIncompleteMultipartUpload limpa")


# --------------------------------------------------------------------------------------------------
# main


def main(argv: list[str]) -> int:
    root, source = probelib.s3_root(argv)
    if not root.startswith("s3://"):
        print(f"uso: .venv/bin/python probes/bucket.py s3://bucket/prefixo\n{probelib.NO_ROOT}", file=sys.stderr)
        return 2
    import boto3

    bucket, _, prefix = root.removeprefix("s3://").partition("/")
    report = Report("bucket", f"o bucket {bucket} sob {prefix or '(raiz)'}")
    report.value("BUCKET", bucket)
    report.value("PREFIX", prefix)
    resolved = region()
    report.value("REGION", resolved)
    client = boto3.client("s3", region_name=resolved, config=short_config())

    def guarded(section, *arguments):
        # Uma seção interrompida não cala as outras; o que ela devolveria fica no valor padrão do
        # chamador.
        try:
            return section(report, *arguments)
        except Exception as error:  # noqa: BLE001 - toda falha é diagnóstico
            report.line(f"!! seção {section.__name__} interrompida: {describe_error(error)}")
            report.failures.append((f"seção {section.__name__}", describe_error(error)))
            return None

    kms_key, versioning, why = guarded(bucket_settings, client, bucket, resolved) or (None, None, "interrompida")
    guarded(lifecycle, client, bucket, prefix)
    proven: dict[str, Any] = guarded(inventory, client, bucket, prefix) or {"listed": False, "sample": None}
    guarded(versioning_check, versioning, why, proven.get("sample"))
    guarded(permissions, bucket, prefix, resolved, kms_key, proven)
    guarded(kms_key_section, resolved, kms_key)
    guarded(policy_and_uploads, client, bucket, prefix)
    return report.finish()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
