"""Diagnóstico, só de leitura, do acesso à AWS que a suíte S3 exige.

Uso, no ambiente destino, com a raiz que a suíte usaria:

    .venv/bin/python probes/diagnose_aws.py s3://bucket/prefixo

O resultado sai no terminal e em ``probes/output/diagnose_aws_<data-hora>.txt``, pasta fora do git,
para ser colado na conversa com o assistente. Cada verificação imprime uma linha com o resultado, o
detalhe e o tempo, sempre com timeouts curtos. As verificações são as variáveis de ambiente, a
região como o ``boto3`` e o delta-rs a resolvem, o DNS dos endpoints, as credenciais, a listagem de
``<raiz>/serialize-db-poc/`` pelo ``boto3``, pelo delta-rs e pelo DuckDB, e o STS.

O delta-rs roda com o ambiente como encontrado e, quando ``NO_PROXY`` está ausente ou vazia ao lado
de ``no_proxy``, de novo com ``NO_PROXY`` exportada de ``no_proxy``, que é o que a suíte faz antes
de abrir a tabela: o cliente HTTP do delta-rs lê ``NO_PROXY`` e, só quando ela está ausente,
``no_proxy``, e uma ``NO_PROXY`` vazia manda a chamada ao endpoint de credenciais pelo proxy. O
DuckDB roda com o endereço do proxy separado das credenciais, porque recusa o endereço com elas
embutidas e não tem exceção equivalente a ``NO_PROXY``. O resumo final diz se a suíte precisa de
manutenção para o ambiente: região que o ``boto3`` não lê, STS inalcançável, endpoint VPC de
interface sem DNS privado. Nada é gravado no bucket: as chamadas são listagens e leituras de
metadado.

O formato é próprio, sem o ``Report`` de ``probelib.py``: uma linha ``[status] rótulo: detalhe
(tempo)`` por verificação, agrupadas por ``== título``. O arquivo se organiza na ordem do
relatório: as funções ``show_*`` imprimem o contexto, as ``check_*`` fazem uma verificação cada e
devolvem se passou, o delta-rs e o DuckDB rodam em subprocessos (``DELTA_PROBE``,
``DUCKDB_PROBE``) com espera limitada, e ``diagnose`` encadeia tudo e escreve o resumo.

Os fatos que o resumo usa:

- Região. O botocore lê ``AWS_DEFAULT_REGION`` ou o perfil, não ``AWS_REGION``, e sem região usa o
  endpoint global ``s3.amazonaws.com``, que um endpoint VPC regional não atende. O delta-rs lê
  ``AWS_REGION`` e ``AWS_DEFAULT_REGION``; sem nenhuma, consulta o IMDS e cai em ``us-east-1``. A
  suíte copia a região do ``boto3`` para ``AWS_REGION``, num sentido só: um ambiente com apenas
  ``AWS_REGION`` e sem ``~/.aws/config`` precisa de manutenção.
- STS. ``test_boto3_credential_source`` chama ``get_caller_identity``; um ambiente só com endpoint
  VPC do S3 não alcança o STS, e o teste falharia depois dos 60 s por tentativa e 5 tentativas do
  botocore. O diagnóstico distingue "o serviço respondeu com erro" de "sem resposta": só o segundo
  pede manutenção.
- Proxy. Nada na suíte exige proxy; sem as variáveis, nada a fazer. Com elas, das duas linhas do
  delta-rs, a segunda, com ``NO_PROXY`` exportada de ``no_proxy``, é a que vale para a suíte.
- Endpoint. Com ``AWS_ENDPOINT_URL``, o ``boto3``, o delta-rs e o secret do DuckDB da suíte, pelas
  opções de ``Storage.duckdb_setup``, o usam.

Sem rede, o diagnóstico leva um minuto e meio: o ``boto3`` desiste em 11 s, o delta-rs em 10 s
(``max_retries`` e ``retry_timeout`` em ``storage_options``) e o DuckDB no teto de 60 s do
subprocesso.
"""

from __future__ import annotations

import ipaddress
import json
import os
import platform
import socket
import subprocess
import sys
import time
from collections.abc import Callable, Mapping
from pathlib import Path

import probelib

# A espera, em segundos, de cada subprocesso do delta-rs e do DuckDB, que têm esperas próprias e
# longas sem rede.
PROBE_TIMEOUT = 60

# As variáveis de proxy nas duas grafias: o delta-rs lê NO_PROXY e, só quando ela está ausente,
# no_proxy.
PROXY_VARIABLES = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "NO_PROXY", "no_proxy")

# As variáveis mostradas com valor, e as mostradas só como presença (credenciais).
SHOWN_VARIABLES = ("AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE", "AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3", *PROXY_VARIABLES)
PRESENCE_VARIABLES = (
    "AWS_ACCESS_KEY_ID",
    "AWS_SESSION_TOKEN",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
    "AWS_CONTAINER_CREDENTIALS_FULL_URI",
    "AWS_WEB_IDENTITY_TOKEN_FILE",
)


class Tee:
    """Escreve ao mesmo tempo no terminal e no arquivo de saída, linha a linha."""

    def __init__(self, path: Path) -> None:
        self.file = path.open("w", encoding="utf-8")
        self.terminal = sys.stdout

    def write(self, text: str) -> int:
        self.terminal.write(text)
        self.file.write(text)
        self.file.flush()
        return len(text)

    def flush(self) -> None:
        self.terminal.flush()
        self.file.flush()


# --------------------------------------------------------------------------------------------------
# O formato das linhas


def report(status: str, label: str, detail: str, started: float | None = None) -> None:
    """Imprime uma linha ``[status] rótulo: detalhe (tempo)``."""
    elapsed = f" ({time.perf_counter() - started:.1f} s)" if started is not None else ""
    print(f"[{status:^5}] {label}: {detail}{elapsed}")


def describe(error: BaseException) -> str:
    """Erro do ``boto3`` numa linha, dizendo se o serviço respondeu ou se não houve resposta."""
    import botocore.exceptions

    # Só a falta de resposta pede manutenção da rede; um erro de credencial ou de permissão é uma
    # resposta.
    answered = isinstance(error, botocore.exceptions.ClientError)
    prefix = "o serviço respondeu com erro" if answered else "sem resposta"
    return f"{prefix}: {type(error).__name__}: {' '.join(str(error).split())[:200]}"


# --------------------------------------------------------------------------------------------------
# O contexto: versões, variáveis e região


def show_versions(root: str) -> None:
    """O cabeçalho: a raiz, a data, a plataforma e as versões das bibliotecas que a suíte usa."""
    import boto3
    import botocore
    import deltalake
    import duckdb

    print(f"== diagnóstico de {root} em {time.strftime('%Y-%m-%d %H:%M:%S %z')}")
    print(f"  {platform.platform()}; python {platform.python_version()}")
    print(f"  boto3 {boto3.__version__}; botocore {botocore.__version__}; deltalake {deltalake.__version__}; duckdb {duckdb.__version__}")


def show_environment() -> None:
    """As variáveis que o boto3 e o delta-rs leem; as credenciais só como presença."""
    print("== variáveis de ambiente")
    for name in SHOWN_VARIABLES:
        value = os.environ.get(name)
        # A minúscula igual à maiúscula sai uma vez, a vazia sai como (vazia), e o endereço de
        # proxy sai sem as credenciais embutidas.
        if value is not None and name != name.upper() and value == os.environ.get(name.upper()):
            value = f"(igual a {name.upper()})"
        elif value == "":
            value = "(vazia)"
        elif value is not None and "proxy" in name.lower():
            value = probelib.hide_credentials(value)
        print(f"  {name} = {value if value is not None else '(ausente)'}")

    for name in PRESENCE_VARIABLES:
        print(f"  {name} {'definida' if os.environ.get(name) else '(ausente)'}")

    # Os arquivos de configuração do boto3 em ~/.aws: só a existência, sem ler o conteúdo.
    aws_folder = Path.home() / ".aws"
    for file in ("config", "credentials"):
        print(f"  ~/.aws/{file} {'existe' if (aws_folder / file).is_file() else '(ausente)'}")


def no_proxy_state(environ: Mapping[str, str]) -> str:
    """``NO_PROXY`` como o ambiente a tem: ``ausente``, ``vazia`` ou ``definida``.

    Vazia e ausente diferem para o delta-rs.
    """
    value = environ.get("NO_PROXY")
    if value is None:
        return "ausente"
    return "vazia" if value == "" else "definida"


def suite_environment(environ: Mapping[str, str]) -> dict[str, str]:
    """As variáveis que a suíte exporta antes de abrir a tabela.

    A suíte exporta ``NO_PROXY`` de ``no_proxy`` quando a maiúscula está ausente ou vazia. O cliente
    HTTP do delta-rs lê ``NO_PROXY`` e, só quando ela está ausente, ``no_proxy``; vazia, ela anula
    as exceções, e a chamada ao endpoint de credenciais do contêiner vai pelo proxy.
    """
    if not environ.get("NO_PROXY") and environ.get("no_proxy"):
        return {"NO_PROXY": environ["no_proxy"]}
    return {}


def resolve_regions() -> tuple[str | None, str | None]:
    """A região como o ``boto3`` e como o delta-rs a resolvem; cada um lê variáveis diferentes."""
    import boto3

    # O botocore lê AWS_DEFAULT_REGION ou o perfil; o delta-rs lê AWS_REGION e AWS_DEFAULT_REGION.
    boto3_region = boto3.Session().region_name
    delta_region = os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")

    print("== região")
    report("ok" if boto3_region else "falha", "boto3", boto3_region or "nenhuma: o boto3 lê AWS_DEFAULT_REGION ou o perfil, não AWS_REGION, e sem região usa o endpoint global s3.amazonaws.com")
    report("ok" if delta_region else "falha", "delta-rs", delta_region or "nenhuma: sem AWS_REGION nem AWS_DEFAULT_REGION o delta-rs consulta o IMDS e cai em us-east-1")
    return boto3_region, delta_region


# --------------------------------------------------------------------------------------------------
# As verificações: DNS, credenciais, S3 por cada cliente, STS


def check_dns(bucket: str, region: str | None) -> None:
    """O DNS dos endpoints do S3 e do STS.

    Um IP privado indica endpoint VPC de interface com DNS privado.
    """
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

        # Só o S3 e o DynamoDB têm gateway endpoint; os demais nomes públicos dependem da internet
        # ou do proxy.
        private = all(ipaddress.ip_address(address).is_private for address in addresses)
        gateway = name.split(".")[0] in ("s3", "dynamodb") or name.split(".")[1:2] in (["s3"], ["dynamodb"])
        if private:
            kind = "IP privado: endpoint VPC de interface com DNS privado"
        elif gateway:
            kind = "IP público: gateway endpoint ou internet"
        else:
            kind = "IP público: só pela internet ou pelo proxy"
        report("ok", f"DNS {name}", f"{', '.join(addresses)} ({kind})", started)


def check_boto3_credentials() -> bool:
    """Se o ``boto3`` encontra credenciais, e por qual método."""
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

    # Esperas curtas: sem rede, o padrão do boto3 é 60 s por tentativa.
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
    """Se o STS respondeu, com identidade ou com erro; só a falta de resposta é ``False``."""
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


# --------------------------------------------------------------------------------------------------
# O delta-rs e o DuckDB, cada um num subprocesso com espera limitada


def run_probe(label: str, code: str, arguments: list[str], on_success: Callable[[str], str], environment: Mapping[str, str] | None = None) -> bool:
    """Roda ``code`` num subprocesso com espera limitada e imprime a linha da verificação.

    A espera é limitada porque o delta-rs e o DuckDB têm esperas próprias. ``environment``
    substitui o ambiente do processo. Devolve se o subprocesso terminou sem erro.
    """
    started = time.perf_counter()
    try:
        completed = subprocess.run(
            [sys.executable, "-c", code, *arguments], capture_output=True, text=True, timeout=PROBE_TIMEOUT, env=dict(environment) if environment is not None else None
        )
    except subprocess.TimeoutExpired:
        report("falha", label, f"sem resposta em {PROBE_TIMEOUT} s", started)
        return False

    # O subprocesso imprime o erro numa linha em stderr; a última linha é o diagnóstico.
    if completed.returncode != 0:
        last_line = (completed.stderr.strip().splitlines() or ["(sem saída de erro)"])[-1]
        report("falha", label, last_line[:300], started)
        return False

    report("ok", label, on_success(completed.stdout.strip()), started)
    return True


# O programa do delta-rs: is_deltatable sobre uma tabela sob a raiz, com esperas curtas e o erro
# numa linha em stderr.
DELTA_PROBE = r"""
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


def check_delta_rs(root: str, options: dict[str, str], label: str, environment: Mapping[str, str] | None = None) -> bool:
    """Lista ``_delta_log/`` de uma tabela sob a raiz pelo ``is_deltatable`` do delta-rs.

    A listagem exercita as credenciais, a região e o endpoint do delta-rs, sem escrever.
    """
    uri = f"{root}/serialize-db-poc/_diagnostico"
    return run_probe(label, DELTA_PROBE, [uri, json.dumps(options)], lambda out: f"listou o prefixo (tabela existe: {out})", environment)


# O programa do DuckDB: carrega as extensões da pasta configurada, sem instalação automática, e
# lista o prefixo com credential_chain. O subprocesso lê o proxy do ambiente que herdou, por
# probelib, e a senha não passa pela linha de comando.
DUCKDB_PROBE = r"""
import sys, duckdb
root, region, directory, endpoint, probes = sys.argv[1:6]
sys.path.insert(0, probes)
import probelib
config = {"autoinstall_known_extensions": False, "autoload_known_extensions": False}
if directory:
    config["extension_directory"] = directory
try:
    connection = duckdb.connect(config=config)
    for extension in ("httpfs", "aws", "delta"):
        connection.execute(f"LOAD {extension}")
    for setting, value in probelib.duckdb_proxy().settings.items():
        connection.execute(f"SET {setting} = ?", [value])
    connection.execute("SET http_timeout = 10000")
    connection.execute("SET http_retries = 1")
    secret = f"CREATE SECRET diag (TYPE s3, PROVIDER credential_chain, REGION '{region}'" + (f", ENDPOINT '{endpoint}'" if endpoint else "") + ")"
    connection.execute(secret)
    # ** desce às subpastas; um * só não cruza "/", e as sessões da suíte ficam em serialize-db-poc/<id>/.
    print(connection.execute(f"SELECT count(*) FROM glob('{root}/serialize-db-poc/**')").fetchone()[0])
except Exception as error:
    print(type(error).__name__ + ": " + " ".join(str(error).split()), file=sys.stderr)
    sys.exit(1)
"""


def duckdb_extension_directory() -> str:
    """A pasta de extensões, com a regra da suíte.

    A pasta é ``SERIALIZE_DB_DUCKDB_EXTENSIONS``, senão ``.duckdb/`` do repositório, senão vazia.
    """
    configured = os.environ.get("SERIALIZE_DB_DUCKDB_EXTENSIONS")
    if configured:
        return configured
    local = Path(__file__).resolve().parent.parent / ".duckdb"
    return str(local) if local.is_dir() else ""


def check_duckdb(root: str, region: str | None, endpoint: str) -> bool:
    """Carrega ``httpfs``, ``aws`` e ``delta`` da pasta de extensões e lista o prefixo.

    A listagem usa um secret ``credential_chain``.
    """
    proxy = probelib.duckdb_proxy()
    arguments = [root, region or "", duckdb_extension_directory(), endpoint, str(Path(__file__).resolve().parent)]
    return run_probe(
        "DuckDB",
        DUCKDB_PROBE,
        arguments,
        lambda out: f"listou serialize-db-poc/ e subpastas ({out} objetos) com extensões de {duckdb_extension_directory() or '(padrão)'} e proxy {proxy.reading}",
    )


# --------------------------------------------------------------------------------------------------
# O diagnóstico e o resumo


def main(argv: list[str]) -> int:
    root, _ = probelib.s3_root(argv)
    if not root.startswith("s3://"):
        print(f"uso: .venv/bin/python probes/diagnose_aws.py s3://bucket/prefixo\n{probelib.NO_ROOT}", file=sys.stderr)
        return 2

    # O stdout vai ao terminal e ao arquivo até o fim do diagnóstico.
    output = Path(__file__).resolve().parent / "output"
    output.mkdir(exist_ok=True)
    path = output / f"diagnose_aws_{time.strftime('%Y%m%d-%H%M%S')}.txt"
    sys.stdout = Tee(path)
    try:
        return diagnose(root)
    finally:
        sys.stdout = sys.__stdout__
        print(f"resultado gravado em {path}")


def diagnose(root: str) -> int:
    """As verificações na ordem do relatório e o resumo.

    Devolve 0 quando os três clientes e o STS responderam, senão 1.
    """
    bucket, _, prefix = root.removeprefix("s3://").partition("/")
    # O endpoint de AWS_ENDPOINT_URL_S3 ou de AWS_ENDPOINT_URL, e o host dele sem o esquema, que o
    # secret do DuckDB recebe.
    endpoint_url = os.environ.get("AWS_ENDPOINT_URL_S3") or os.environ.get("AWS_ENDPOINT_URL") or ""
    endpoint_host = endpoint_url.removeprefix("https://").removeprefix("http://").rstrip("/")

    show_versions(root)
    show_environment()
    boto3_region, delta_region = resolve_regions()
    region = boto3_region or delta_region
    check_dns(bucket, region)

    # Sem credenciais nenhum cliente vai adiante.
    results: dict[str, bool] = {}
    if not check_boto3_credentials():
        print("== resumo")
        print("  sem credenciais, nenhum cliente vai adiante; o diagnóstico para aqui")
        return 1

    # Cada cliente lista o prefixo como a suíte faz; quando só um dos dois lê a região, a variante
    # com a região do outro mostra se a manutenção (copiar a região entre as variáveis) resolveria.
    print("== S3")
    results["s3_boto3_suite"] = check_s3_boto3(bucket, prefix, None, "boto3 como a suíte (região do boto3)")
    if not boto3_region and delta_region:
        results["s3_boto3_region"] = check_s3_boto3(bucket, prefix, delta_region, f"boto3 com region_name={delta_region}")
    # O delta-rs como o ambiente está e, quando a suíte exportaria NO_PROXY, de novo como a suíte o
    # deixa.
    results["delta_rs_as_found"] = check_delta_rs(root, {}, "delta-rs como encontrado")
    changes = suite_environment(os.environ)
    environment = {**os.environ, **changes}
    if changes:
        results["delta_rs"] = check_delta_rs(root, {}, "delta-rs como a suíte (NO_PROXY exportada de no_proxy)", environment)
    else:
        results["delta_rs"] = results["delta_rs_as_found"]
    if not delta_region and boto3_region:
        results["delta_rs_region"] = check_delta_rs(root, {"AWS_REGION": boto3_region}, f"delta-rs com AWS_REGION={boto3_region}", environment)
    results["duckdb"] = check_duckdb(root, region, endpoint_host)

    print("== STS")
    results["sts"] = check_sts(region)

    # O resumo: a manutenção de que a suíte precisa para rodar neste ambiente.
    print("== resumo")
    if boto3_region:
        print(f"  região: {boto3_region}, resolvida pelo boto3" + ("" if delta_region else "; a suíte a copia para AWS_REGION, que o delta-rs exige") + " (sem manutenção)")
    elif delta_region:
        print("  região: só AWS_REGION, que o boto3 ignora; a suíte precisa exportar AWS_DEFAULT_REGION a partir dela (manutenção necessária)")
    else:
        print("  região: nenhuma; defina AWS_DEFAULT_REGION antes da suíte, porque atrás de endpoint VPC o endpoint global é inalcançável")
    print("  STS: " + ("respondeu (sem manutenção)" if results["sts"] else "sem resposta; test_boto3_credential_source falharia (manutenção necessária)"))
    proxies = [name for name in PROXY_VARIABLES if os.environ.get(name)]
    if not proxies:
        print("  proxy: sem variáveis; nada a fazer")
    elif changes:
        outcome = "com ela o delta-rs listou" if results["delta_rs"] else "e mesmo assim o delta-rs falhou (manutenção necessária)"
        print(f"  proxy: variáveis {', '.join(proxies)}; NO_PROXY {no_proxy_state(os.environ)}: a suíte a exporta de no_proxy, {outcome}")
    else:
        print(f"  proxy: variáveis {', '.join(proxies)}; NO_PROXY {no_proxy_state(os.environ)}, a suíte não a altera")
    print("  endpoint: " + (f"{endpoint_url} em AWS_ENDPOINT_URL; a suíte não o passa ao DuckDB (manutenção necessária)" if endpoint_url else "sem AWS_ENDPOINT_URL; os nomes s3.<região>.amazonaws.com precisam resolver (ver DNS acima)"))
    # A suíte S3 como está depende dos três clientes; o código de saída 0 exige também o STS.
    core = ("s3_boto3_suite", "delta_rs", "duckdb")
    print("  suíte S3 como está: " + ("os três clientes listaram o prefixo" if all(results[key] for key in core) else "algum cliente falhou; ver as linhas acima"))
    return 0 if all(results[key] for key in core) and results["sts"] else 1


if __name__ == "__main__":
    sys.exit(main(sys.argv))
