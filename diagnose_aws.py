"""Diagnóstico, só de leitura, do acesso à AWS que a suíte S3 exige.

Uso, no ambiente destino, com a raiz que a suíte usaria:

    .venv/bin/python diagnose_aws.py s3://bucket/prefixo

Cada verificação imprime uma linha com o resultado, o detalhe e o tempo, sempre com timeouts curtos:
variáveis de ambiente, região como o ``boto3`` e o delta-rs a resolvem, DNS dos endpoints, credenciais,
listagem de ``<raiz>/serialize-db-poc/`` pelo ``boto3``, pelo delta-rs e pelo DuckDB, e o STS. O
resumo final diz se a suíte precisa de manutenção para o ambiente: região que o ``boto3`` não lê, STS
inalcançável, endpoint VPC de interface sem DNS privado. Nada é gravado no bucket: as chamadas são
listagens e leituras de metadado.
"""

from __future__ import annotations

import ipaddress
import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Callable
from pathlib import Path

PROBE_TIMEOUT = 60  # segundos de espera por subprocesso do delta-rs e do DuckDB
PROXY_VARIABLES = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "NO_PROXY", "no_proxy")


def report(status: str, label: str, detail: str, started: float | None = None) -> None:
    """Imprime uma linha ``[status] rótulo: detalhe (tempo)``."""
    elapsed = f" ({time.perf_counter() - started:.1f} s)" if started is not None else ""
    print(f"[{status:^5}] {label}: {detail}{elapsed}")


def describe(error: BaseException) -> str:
    """Erro do ``boto3`` numa linha, dizendo se o serviço respondeu ou se não houve resposta."""
    import botocore.exceptions

    answered = isinstance(error, botocore.exceptions.ClientError)
    prefix = "o serviço respondeu com erro" if answered else "sem resposta"
    return f"{prefix}: {type(error).__name__}: {' '.join(str(error).split())[:200]}"


def show_environment() -> None:
    print("== variáveis de ambiente")
    for name in ("AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", *PROXY_VARIABLES):
        print(f"  {name} = {os.environ.get(name, '(ausente)')}")
    for name in (
        "AWS_ACCESS_KEY_ID",
        "AWS_SESSION_TOKEN",
        "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
        "AWS_CONTAINER_CREDENTIALS_FULL_URI",
        "AWS_WEB_IDENTITY_TOKEN_FILE",
    ):
        print(f"  {name} {'definida' if os.environ.get(name) else '(ausente)'}")
    aws_folder = Path.home() / ".aws"
    for file in ("config", "credentials"):
        print(f"  ~/.aws/{file} {'existe' if (aws_folder / file).is_file() else '(ausente)'}")


def resolve_regions() -> tuple[str | None, str | None]:
    """Região como o ``boto3`` a resolve e como o delta-rs a resolve; cada um lê variáveis diferentes."""
    import boto3

    boto3_region = boto3.Session().region_name
    delta_region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")
    print("== região")
    report("ok" if boto3_region else "falha", "boto3", boto3_region or "nenhuma: o boto3 lê AWS_DEFAULT_REGION ou o perfil, não AWS_REGION, e sem região usa o endpoint global s3.amazonaws.com")
    report("ok" if delta_region else "falha", "delta-rs", delta_region or "nenhuma: sem AWS_REGION nem AWS_DEFAULT_REGION o delta-rs consulta o IMDS e cai em us-east-1")
    return boto3_region, delta_region


def check_dns(bucket: str, region: str | None) -> None:
    print("== DNS")
    names = ["s3.amazonaws.com"]
    if region:
        names += [f"s3.{region}.amazonaws.com", f"{bucket}.s3.{region}.amazonaws.com", f"sts.{region}.amazonaws.com"]
    for name in names:
        started = time.perf_counter()
        try:
            addresses = sorted({info[4][0] for info in socket.getaddrinfo(name, 443, type=socket.SOCK_STREAM)})
        except OSError as error:
            report("falha", f"DNS {name}", str(error), started)
            continue
        private = all(ipaddress.ip_address(address).is_private for address in addresses)
        kind = "IP privado: endpoint VPC de interface com DNS privado" if private else "IP público: gateway endpoint ou internet"
        report("ok", f"DNS {name}", f"{', '.join(addresses)} ({kind})", started)


def check_boto3_credentials() -> bool:
    import boto3

    print("== credenciais")
    started = time.perf_counter()
    credentials = boto3.Session().get_credentials()
    if credentials is None:
        report("falha", "boto3", "nenhuma encontrada (papel, variáveis AWS_* ou perfil)", started)
        return False
    report("ok", "boto3", f"método {credentials.method}", started)
    return True


def check_s3_boto3(bucket: str, prefix: str, region: str | None, label: str) -> bool:
    """Lista um objeto sob ``<raiz>/serialize-db-poc/`` como a suíte faz, com a região dada."""
    import boto3
    import botocore.config

    config = botocore.config.Config(connect_timeout=5, read_timeout=15, retries={"total_max_attempts": 2, "mode": "standard"})
    client = boto3.client("s3", region_name=region, config=config)
    started = time.perf_counter()
    try:
        client.list_objects_v2(Bucket=bucket, Prefix=f"{prefix}/serialize-db-poc/".lstrip("/"), MaxKeys=1)
    except Exception as error:  # noqa: BLE001 - qualquer falha é o diagnóstico
        report("falha", label, f"{client.meta.endpoint_url}: {describe(error)}", started)
        return False
    report("ok", label, client.meta.endpoint_url, started)
    return True


def check_sts(region: str | None) -> bool:
    """Devolve se o STS respondeu, com identidade ou com erro; só a falta de resposta é ``False``."""
    import boto3
    import botocore.config
    import botocore.exceptions

    config = botocore.config.Config(connect_timeout=5, read_timeout=10, retries={"total_max_attempts": 1, "mode": "standard"})
    started = time.perf_counter()
    try:
        client = boto3.client("sts", region_name=region, config=config)
        identity = client.get_caller_identity()
    except Exception as error:  # noqa: BLE001 - qualquer falha é o diagnóstico
        report("falha", "STS", describe(error), started)
        return isinstance(error, botocore.exceptions.ClientError)
    report("ok", "STS", f"{client.meta.endpoint_url}: {identity['Arn']}", started)
    return True


def run_probe(label: str, code: str, arguments: list[str], on_success: Callable[[str], str]) -> bool:
    """Roda ``code`` num subprocesso com espera limitada, porque o delta-rs e o DuckDB têm esperas próprias."""
    started = time.perf_counter()
    try:
        completed = subprocess.run([sys.executable, "-c", code, *arguments], capture_output=True, text=True, timeout=PROBE_TIMEOUT)
    except subprocess.TimeoutExpired:
        report("falha", label, f"sem resposta em {PROBE_TIMEOUT} s", started)
        return False
    if completed.returncode != 0:
        last_line = (completed.stderr.strip().splitlines() or ["(sem saída de erro)"])[-1]
        report("falha", label, last_line[:300], started)
        return False

    report("ok", label, on_success(completed.stdout.strip()), started)
    return True


DELTA_PROBE = """
import json, re, sys
from deltalake import DeltaTable
uri, options = sys.argv[1], json.loads(sys.argv[2])
# Sem rede, o padrão do delta-rs (10 tentativas com espera crescente) passa de um minuto; assim são 10 s.
options.update({"timeout": "15s", "connect_timeout": "5s", "max_retries": "1", "retry_timeout": "10s"})
try:
    print(DeltaTable.is_deltatable(uri, storage_options=options))
except Exception as error:
    message = re.sub(r"\x1b\[[0-9;]*m", "", " ".join(str(error).split()))
    print(type(error).__name__ + ": " + message, file=sys.stderr)
    sys.exit(1)
"""


def check_delta_rs(root: str, options: dict[str, str], label: str) -> bool:
    """``is_deltatable`` lista ``_delta_log/`` de uma tabela sob a raiz: credenciais, região e endpoint do delta-rs, sem escrever."""
    uri = f"{root}/serialize-db-poc/_diagnostico"
    return run_probe(label, DELTA_PROBE, [uri, json.dumps(options)], lambda out: f"listou o prefixo (tabela existe: {out})")


DUCKDB_PROBE = """
import sys, duckdb
root, region, directory, endpoint = sys.argv[1:5]
config = {"autoinstall_known_extensions": False, "autoload_known_extensions": False}
if directory:
    config["extension_directory"] = directory
try:
    connection = duckdb.connect(config=config)
    for extension in ("httpfs", "aws", "delta"):
        connection.execute(f"LOAD {extension}")
    connection.execute("SET http_timeout = 10000")
    connection.execute("SET http_retries = 1")
    secret = f"CREATE SECRET diag (TYPE s3, PROVIDER credential_chain, REGION '{region}'" + (f", ENDPOINT '{endpoint}'" if endpoint else "") + ")"
    connection.execute(secret)
    print(connection.execute(f"SELECT count(*) FROM glob('{root}/serialize-db-poc/*')").fetchone()[0])
except Exception as error:
    print(type(error).__name__ + ": " + " ".join(str(error).split()), file=sys.stderr)
    sys.exit(1)
"""


def duckdb_extension_directory() -> str:
    configured = os.environ.get("SERIALIZE_DB_DUCKDB_EXTENSIONS")
    if configured:
        return configured
    local = Path(__file__).resolve().parent / ".duckdb"
    return str(local) if local.is_dir() else ""


def check_duckdb(root: str, region: str | None, endpoint: str) -> bool:
    """Carrega ``httpfs``, ``aws`` e ``delta`` da pasta de extensões e lista o prefixo com um secret ``credential_chain``."""
    arguments = [root, region or "", duckdb_extension_directory(), endpoint]
    return run_probe("DuckDB", DUCKDB_PROBE, arguments, lambda out: f"listou o prefixo ({out} entradas) com extensões de {duckdb_extension_directory() or '(padrão)'}")


def main(argv: list[str]) -> int:
    root = (argv[1] if len(argv) > 1 else os.environ.get("SERIALIZE_DB_TEST_S3_ROOT", "")).rstrip("/")
    if not root.startswith("s3://"):
        print("uso: .venv/bin/python diagnose_aws.py s3://bucket/prefixo", file=sys.stderr)
        return 2
    bucket, _, prefix = root.removeprefix("s3://").partition("/")
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL") or ""
    endpoint_host = endpoint_url.removeprefix("https://").removeprefix("http://").rstrip("/")

    show_environment()
    boto3_region, delta_region = resolve_regions()
    region = boto3_region or delta_region
    check_dns(bucket, region)
    results: dict[str, bool] = {}
    if not check_boto3_credentials():
        print("== resumo")
        print("  sem credenciais, nenhum cliente vai adiante; o diagnóstico para aqui")
        return 1

    print("== S3")
    results["s3_boto3_suite"] = check_s3_boto3(bucket, prefix, None, "boto3 como a suíte (região do boto3)")
    if not boto3_region and delta_region:
        results["s3_boto3_region"] = check_s3_boto3(bucket, prefix, delta_region, f"boto3 com region_name={delta_region}")
    results["delta_rs"] = check_delta_rs(root, {}, "delta-rs como a suíte")
    if not delta_region and boto3_region:
        results["delta_rs_region"] = check_delta_rs(root, {"AWS_REGION": boto3_region}, f"delta-rs com AWS_REGION={boto3_region}")
    results["duckdb"] = check_duckdb(root, region, endpoint_host)
    print("== STS")
    results["sts"] = check_sts(region)

    print("== resumo")
    if boto3_region:
        print(f"  região: {boto3_region}, resolvida pelo boto3" + ("" if delta_region else "; a suíte a copia para AWS_REGION, que o delta-rs exige") + " (sem manutenção)")
    elif delta_region:
        print("  região: só AWS_REGION, que o boto3 ignora; a suíte precisa exportar AWS_DEFAULT_REGION a partir dela (manutenção necessária)")
    else:
        print("  região: nenhuma; defina AWS_DEFAULT_REGION antes da suíte, porque atrás de endpoint VPC o endpoint global é inalcançável")
    print("  STS: " + ("respondeu (sem manutenção)" if results["sts"] else "sem resposta; test_boto3_credential_source falharia (manutenção necessária)"))
    proxies = [name for name in PROXY_VARIABLES if os.environ.get(name)]
    print("  proxy: " + (f"variáveis {', '.join(proxies)}; a suíte copia no_proxy para NO_PROXY" if proxies else "sem variáveis; nada a fazer"))
    print("  endpoint: " + (f"{endpoint_url} em AWS_ENDPOINT_URL; a suíte não o passa ao DuckDB (manutenção necessária)" if endpoint_url else "sem AWS_ENDPOINT_URL; os nomes s3.<região>.amazonaws.com precisam resolver (ver DNS acima)"))
    core = ("s3_boto3_suite", "delta_rs", "duckdb")
    print("  suíte S3 como está: " + ("os três clientes listaram o prefixo" if all(results[key] for key in core) else "algum cliente falhou; ver as linhas acima"))
    return 0 if all(results[key] for key in core) and results["sts"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
