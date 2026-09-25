"""O espaço do SageMaker Unified Studio: identidade, projeto, rede, máquina, Python e DuckDB.

Uso, em qualquer ambiente:

    .venv/bin/python probes/space.py

O probe só lê. O relatório sai no terminal e em ``probes/output/space_<data-hora>.txt``. Seções:

1. Identidade e credenciais: o método do ``boto3``, a região como o ``boto3`` a resolve, o endpoint
   de credenciais do contêiner, o IMDS e o STS.
2. Projeto: o que ``sagemaker_studio.Project()`` devolve (papel, chave KMS, raiz S3, conexões),
   lido neste interpretador ou no do sistema, porque o pacote não entra no venv.
3. Rede: variáveis de proxy, DNS dos endpoints regionais (IP privado indica endpoint VPC de
   interface com DNS privado), TCP até o S3 regional e até o proxy, e a internet por HTTPS.
4. Máquina: CPUs, memória, disco, o limite de arquivos abertos, a pasta compartilhada e os
   comandos disponíveis.
5. Python e pacotes: este interpretador e o do sistema, com as versões dos pacotes do projeto,
   conferidas contra as dependências de execução e o grupo ``dev`` de ``pyproject.toml``.
6. DuckDB: versão, plataforma, threads, memória, o proxy da sessão e as extensões que carregam da
   pasta configurada, com a instalação automática desligada.

Cada seção é uma função, na ordem acima (``identity``, ``project``, ``network``, ``machine``,
``python_packages``, ``duckdb_section``), que documenta as checagens que emite (``SP-1`` a
``SP-10``); ``main`` as chama uma a uma, e uma seção que quebra não cala as outras.

Chamadas: ``sts:GetCallerIdentity`` e as que ``sagemaker_studio`` faz para ler o projeto. Códigos
de saída: 0 checagens ok, 1 alguma chamada falhou, 2 alguma checagem reprovou.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import platform
import re
import shutil
import sys
import tempfile
import tomllib
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import probelib  # noqa: E402
from probelib import (  # noqa: E402
    Report,
    connection_rows,
    describe_error,
    dns_rows,
    environment_rows,
    pretty,
    region,
    run_python,
    short_config,
    tcp_open,
    tcp_probe,
)

# Os pacotes lidos em cada interpretador: os fixados pelo projeto (as dependências de execução e o
# grupo dev de pyproject.toml), os que a suíte de estudo usa, os opcionais que o grupo dev não
# traz (ADBC para leitura do Redshift, o pdoc do grupo docs) e os que o espaço já traz.
PACKAGES = (
    "deltalake", "duckdb", "pyarrow", "boto3", "botocore", "redshift_connector", "sqlalchemy", "duckdb_engine",
    "sqlalchemy_redshift", "pandas", "sqlglot", "adbc_driver_postgresql", "pdoc", "sagemaker_studio", "awswrangler", "pytest",
)

# Os serviços cujo endpoint regional é resolvido na seção de rede: os que a biblioteca e os probes
# chamam.
ENDPOINT_SERVICES = ("s3", "sts", "redshift", "redshift-serverless", "redshift-data", "glue", "athena", "kms", "secretsmanager", "sagemaker", "datazone")

# As extensões do DuckDB que a biblioteca carrega; as duas últimas vêm embutidas no binário.
EXTENSIONS = ("httpfs", "delta", "aws", "parquet", "json")

# As variáveis da cadeia de credenciais do boto3, na ordem em que a tabela as mostra.
CREDENTIAL_VARIABLES = (
    "AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE", "AWS_ACCESS_KEY_ID", "AWS_SESSION_TOKEN",
    "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "AWS_WEB_IDENTITY_TOKEN_FILE", "AWS_ROLE_ARN",
)

# Os comandos que o trabalho no espaço usa; ausência é leitura, não falha.
TOOLS = ("uv", "git", "gh", "aws", "duckdb")

# O programa que lê as versões dos pacotes no interpretador do sistema; recebe os nomes como
# argumentos.
VERSIONS_PROBE = r"""
import importlib.metadata, json, sys
out = {"version": sys.version.split()[0]}
for name in sys.argv[1:]:
    out[name] = None
    for candidate in (name, name.replace("_", "-"), name.replace("-", "_")):
        try:
            out[name] = importlib.metadata.version(candidate)
            break
        except importlib.metadata.PackageNotFoundError:
            pass
print(json.dumps(out))
"""


def package_version(name: str) -> str | None:
    """A versão instalada de ``name`` neste interpretador, ou ``None`` quando ausente.

    O nome vale com ``_`` ou com ``-``.
    """
    for candidate in (name, name.replace("_", "-"), name.replace("-", "_")):
        try:
            return importlib.metadata.version(candidate)
        except importlib.metadata.PackageNotFoundError:
            pass
    return None


def pinned_requirements() -> dict[str, str | None]:
    """Os pacotes das dependências de execução e do grupo ``dev`` de ``pyproject.toml``.

    A chave é o nome de importação e o valor, a versão quando ela é ``==``.
    """
    with open(probelib.REPO_ROOT / "pyproject.toml", "rb") as handle:
        pyproject = tomllib.load(handle)
    entries = pyproject.get("project", {}).get("dependencies", []) + pyproject.get("dependency-groups", {}).get("dev", [])

    # "deltalake==1.6.4" vira {"deltalake": "1.6.4"}; "boto3" vira {"boto3": None}; nomes com "-"
    # viram "_".
    found: dict[str, str | None] = {}
    for entry in entries:
        match = re.match(r"\s*([A-Za-z0-9_.-]+)\s*(?:==\s*([^\s;,]+))?", entry) if isinstance(entry, str) else None
        if match:
            found[match.group(1).lower().replace("-", "_")] = match.group(2)
    return found


def identity(report: Report) -> None:
    """Seção 1, identidade e credenciais.

    Checagens: ``SP-1`` (credenciais), ``SP-2`` (região) e ``SP-3`` (STS).
    """
    import boto3

    report.h1("Identidade e credenciais")
    report.table([["variável", "valor"], *environment_rows(CREDENTIAL_VARIABLES)])

    # Os arquivos de configuração do boto3 em ~/.aws: só a existência, sem ler o conteúdo.
    aws_folder = Path.home() / ".aws"
    report.table([
        ["arquivo", "estado"],
        *[[f"~/.aws/{name}", "existe" if (aws_folder / name).is_file() else "ausente"] for name in ("config", "credentials")],
    ])

    # SP-1: o boto3 encontra credenciais, e por qual método (variáveis, perfil, contêiner ou
    # instância).
    session = boto3.Session()
    credentials = report.call(
        "boto3.Session().get_credentials()",
        session.get_credentials,
        render=lambda found: f"método {found.method}" if found else "nenhuma",
    )
    if credentials:
        report.ok("SP-1", "credenciais do boto3", f"método {credentials.method}")

        # As credenciais temporárias expiram; o boto3 e o delta-rs renovam as do contêiner, e uma
        # execução longa depende disso. O mesmo instante lido em duas execuções seguidas diz quanto
        # dura cada emissão.
        expiry = getattr(credentials, "_expiry_time", None)
        if expiry and getattr(expiry, "tzinfo", None):
            minutes = (expiry - datetime.now(timezone.utc)).total_seconds() / 60
            when = f"daqui a {minutes:.0f} min" if minutes >= 0 else f"expirada há {-minutes:.0f} min"
            report.value("CREDENTIAL_EXPIRY", f"{expiry} ({when})")
        else:
            report.value("CREDENTIAL_EXPIRY", str(expiry) if expiry else "(sem expiração exposta: estáticas, ou renovadas pelo provedor)")
    else:
        report.fail("SP-1", "credenciais do boto3", "nenhuma encontrada: papel, variáveis AWS_* ou perfil")

    # SP-2: o botocore lê AWS_DEFAULT_REGION ou o perfil; AWS_REGION sozinha só serve ao delta-rs.
    resolved = region()
    report.value("REGION", resolved)
    if session.region_name:
        report.ok("SP-2", "região do boto3", session.region_name)
    elif resolved:
        report.fail("SP-2", "região do boto3", f"nenhuma; só {resolved} em AWS_REGION, que o botocore ignora: defina AWS_DEFAULT_REGION")
    else:
        report.fail("SP-2", "região", "nenhuma variável nem perfil a define")

    # Os dois endereços link-local são leituras: o espaço bloqueia o IMDS, e o endpoint do contêiner
    # só existe com a variável.
    if os.environ.get("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"):
        report.call("tcp 169.254.170.2:80 (endpoint de credenciais do contêiner)", lambda: tcp_probe("169.254.170.2", 80, 2), render=str)
    report.call("tcp 169.254.169.254:80 (IMDS)", lambda: tcp_probe("169.254.169.254", 80, 2), render=str)

    # SP-3: o STS responde com a identidade; a falta de resposta é leitura, porque o destino pode
    # não ter o endpoint.
    caller = report.call(
        "sts.get_caller_identity()",
        lambda: session.client("sts", region_name=resolved, config=short_config(5, 10, 1)).get_caller_identity(),
    )
    if caller:
        report.ok("SP-3", "identidade pelo STS", caller["Arn"])
    else:
        report.note("SP-3", "identidade pelo STS", "sem resposta ou erro; ver a seção final")


def project(report: Report) -> None:
    """Seção 2, o projeto do SageMaker Unified Studio.

    Checagens: ``SP-4`` (projeto lido) e ``SP-5`` (conexão Redshift).
    """
    report.h1("Projeto do SageMaker Unified Studio")

    # A leitura roda sagemaker_studio no primeiro interpretador que tem o pacote
    # (probelib.PROJECT_PROBE).
    result = report.call(
        "sagemaker_studio.Project() (neste interpretador ou no do sistema)",
        probelib.project_snapshot,
        render=lambda found: f"lido com {found[1]}",
    )
    if result is None:
        report.note("SP-4", "projeto do SageMaker", "não lido: fora de um espaço ou sem o pacote sagemaker_studio")
        report.note("SP-5", "conexão Redshift no projeto", "não lida")
        return

    data, _ = result
    for key in ("name", "id", "domain_id", "iam_role", "kms_key_arn", "s3_root"):
        report.value(f"PROJECT_{key.upper()}", data.get(key))

    # Uma linha por conexão; os dados completos só das conexões Redshift, que a etapa 5 usa.
    connections = data.get("connections", [])
    if connections:
        report.table([["conexão", "tipo", "endpoint", "detalhe"], *connection_rows(connections)])
    else:
        report.table([["(nenhuma conexão no projeto)"]])
    for item in connections:
        if "REDSHIFT" in str(item.get("type", "")).upper():
            report.line(f"conexão {item.get('name')}:\n{pretty(item, limit=80)}\n")

    # SP-4: o projeto foi lido; SP-5: a conexão Redshift do projeto, o atalho para as variáveis da
    # etapa 5.
    summary = ", ".join(f"{item.get('name')} ({item.get('type')})" for item in connections) or "nenhuma"
    report.ok("SP-4", "projeto do SageMaker", f"{data.get('name')}; conexões: {summary}")

    redshift = [item.get("name") for item in connections if "REDSHIFT" in str(item.get("type", "")).upper()]
    if redshift:
        report.ok("SP-5", "conexão Redshift no projeto", ", ".join(redshift))
    else:
        report.note("SP-5", "conexão Redshift no projeto", "nenhuma: a etapa 5 conecta pelas variáveis SERIALIZE_DB_REDSHIFT_*, e a conexão do projeto é só um atalho para elas")


def network(report: Report) -> None:
    """Seção 3, rede: proxy, DNS, ``SP-6`` (TCP até o S3 regional) e ``SP-7`` (internet).

    ``SP-7`` é leitura, nunca reprovação.
    """
    report.h1("Rede")
    report.table([["variável", "valor"], *environment_rows(probelib.PROXY_VARIABLES)])

    # O DNS: um IP privado indica endpoint VPC de interface; pypi.org e github.com dizem se há DNS
    # para a internet.
    resolved = region()
    names = [f"{service}.{resolved}.amazonaws.com" for service in ENDPOINT_SERVICES] if resolved else []
    names += ["s3.amazonaws.com", "pypi.org", "github.com"]
    rows, _ = dns_rows(names)
    report.table([["nome", "endereços", "tipo"], *rows])

    # SP-6: a porta 443 do S3 regional abre; sem ela nada na biblioteca funciona.
    if resolved:
        host = f"s3.{resolved}.amazonaws.com"
        opened = report.call(f"tcp {host}:443", lambda: tcp_open(host, 443, 5), render=lambda seconds: f"conectou em {seconds:.2f} s")
        if opened is None:
            report.fail("SP-6", "S3 regional por TCP", f"{host} não respondeu em 5 s")
        else:
            report.ok("SP-6", "S3 regional por TCP", host)
    else:
        report.note("SP-6", "S3 regional por TCP", "sem região, sem host")

    # O proxy, quando configurado, é sondado por TCP; a resposta é leitura.
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if proxy:
        parsed = urllib.parse.urlparse(proxy if "://" in proxy else f"http://{proxy}")
        if parsed.hostname:
            port = parsed.port or 3128
            report.call(f"tcp {parsed.hostname}:{port} (proxy)", lambda: tcp_probe(parsed.hostname or "", port, 5), render=str)

    # SP-7: a internet é uma leitura, não uma chamada que falha: o ambiente destino não a tem.
    def internet() -> str:
        request = urllib.request.Request("https://pypi.org/simple/", method="HEAD")
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return f"HTTP {response.status}"
        except Exception as error:  # noqa: BLE001 - o erro é a leitura
            return f"sem resposta: {describe_error(error)}"

    answer = report.call("HEAD https://pypi.org/simple/ (pelo proxy do ambiente, se houver)", internet, render=str)
    report.note("SP-7", "internet", "alcançável" if answer and answer.startswith("HTTP") else "inalcançável: esperado no ambiente destino")


def mount_state(path: Path, mounts: str = "/proc/mounts") -> str:
    """O estado de ``path``: ``ausente``, ``montada`` com tipo e modo, ou ``existe, sem montagem``.

    O tipo e o modo (``rw`` ou ``ro``) vêm de ``/proc/mounts``. A função segue o link simbólico
    antes de perguntar, porque ``os.path.ismount`` responde False a um link.
    """
    if not path.exists():
        return "ausente"

    real = Path(os.path.realpath(path))
    origin = f" (link para {real})" if real != path else ""

    # Cada linha de /proc/mounts traz dispositivo, ponto de montagem, tipo e as opções separadas
    # por vírgula.
    kinds: dict[str, str] = {}
    try:
        with open(mounts, encoding="utf-8") as handle:
            for entry in handle:
                fields = entry.split()
                if len(fields) >= 4:
                    mode = next((option for option in fields[3].split(",") if option in ("rw", "ro")), None)
                    kinds[fields[1]] = f"tipo {fields[2]}" + (f", {mode}" if mode else "")
    except OSError:
        pass

    if str(real) in kinds:
        return f"montada, {kinds[str(real)]}{origin}"
    if os.path.ismount(real):
        return f"montada{origin}"
    return f"existe, sem montagem{origin}"


def machine(report: Report) -> None:
    """Seção 4, a máquina, só com leituras e sem checagem.

    A tabela traz as CPUs, a memória, o disco, o limite de arquivos abertos, ``~/shared`` e os
    comandos.
    """
    report.h1("Máquina")
    rows: list[list[object]] = [["item", "valor"], ["plataforma", platform.platform()], ["cpus", os.cpu_count()]]

    # A memória física: o número de páginas vezes o tamanho de cada uma.
    try:
        rows.append(["memória", f"{os.sysconf('SC_PAGE_SIZE') * os.sysconf('SC_PHYS_PAGES') / 2**30:.1f} GiB"])
    except (ValueError, OSError, AttributeError):
        rows.append(["memória", "não lida"])

    # O sandbox do DuckDB e a pasta de transbordo ficam na pasta temporária; o espaço livre dela
    # limita a execução.
    disks = (
        ("repositório", probelib.REPO_ROOT),
        ("HOME", Path.home()),
        ("/tmp", Path("/tmp")),
        ("pasta temporária do Python", Path(tempfile.gettempdir())),
    )
    for label, path in disks:
        usage = shutil.disk_usage(path)
        rows.append([f"disco livre em {label}", f"{usage.free / 2**30:.1f} GiB de {usage.total / 2**30:.1f} GiB ({path})"])

    # O DuckDB abre um descritor por arquivo Parquet lido; um limite baixo derruba uma consulta
    # grande.
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        rows.append(["arquivos abertos por processo (ulimit -n)", f"{soft} (máximo {hard}); o DuckDB abre um descritor por arquivo Parquet lido"])
    except (ImportError, ValueError, OSError):
        rows.append(["arquivos abertos por processo", "não lido"])

    rows.append(["~/shared", mount_state(Path.home() / "shared")])
    for tool in TOOLS:
        rows.append([f"comando {tool}", shutil.which(tool) or "ausente"])
    report.table(rows)


def python_packages(report: Report) -> None:
    """Seção 5, Python e pacotes: ``SP-8`` e ``SP-9``.

    ``SP-8`` confere o Python 3.13; ``SP-9``, as dependências de execução e o grupo ``dev`` de
    ``pyproject.toml``.
    """
    report.h1("Python e pacotes")

    # Uma coluna por interpretador: este, e cada candidato do sistema lido por subprocesso
    # (VERSIONS_PROBE).
    columns: list[tuple[str, dict[str, str | None]]] = []
    here: dict[str, str | None] = {"version": platform.python_version()}
    for name in PACKAGES:
        here[name] = package_version(name)
    columns.append((sys.executable, here))

    for executable in probelib.python_candidates()[1:]:
        completed = report.call(
            f"{executable} (versões dos pacotes)",
            lambda exe=executable: run_python(VERSIONS_PROBE, list(PACKAGES), 60, exe),
            render=lambda done: done.stdout.strip()[:400] if done.returncode == 0 else f"falhou: {done.stderr.strip()[-200:]}",
        )
        if completed is not None and completed.returncode == 0:
            columns.append((executable, json.loads(completed.stdout)))

    rows = [["pacote", *[label for label, _ in columns]], ["python", *[str(data.get("version")) for _, data in columns]]]
    for name in PACKAGES:
        rows.append([name, *[data.get(name) or "ausente" for _, data in columns]])
    report.table(rows)

    # SP-8: o projeto fixa Python 3.13.
    if here["version"].startswith("3.13."):
        report.ok("SP-8", "Python 3.13 neste interpretador", str(here["version"]))
    else:
        report.fail("SP-8", "Python 3.13 neste interpretador", f"{here['version']}: o projeto fixa 3.13")

    # SP-9: as dependências de execução e o grupo dev de pyproject.toml são a referência: cada
    # pacote presente, e na versão fixada quando ela é ==.
    requirements = pinned_requirements()
    installed = {name: here[name] if name in here else package_version(name) for name in requirements}
    wrong = [
        f"{name} ausente" if installed[name] is None else f"{name} {installed[name]} (esperado {version})"
        for name, version in requirements.items()
        if installed[name] is None or (version and installed[name] != version)
    ]
    if wrong:
        report.fail("SP-9", "dependências e grupo dev do pyproject neste interpretador", "; ".join(wrong) + "; rode uv sync --group dev, ou prepare_offline.sh de novo, na pasta do projeto")
    else:
        report.ok("SP-9", "dependências e grupo dev do pyproject neste interpretador", ", ".join(f"{name} {installed[name]}" for name in requirements))


def duckdb_section(report: Report) -> None:
    """Seção 6, DuckDB: a configuração da conexão, o proxy e ``SP-10`` (as extensões carregam).

    O proxy entra com o endereço separado das credenciais, e as extensões carregam da pasta
    configurada.
    """
    import duckdb

    report.h1("DuckDB")

    # A pasta de extensões segue a regra da suíte: SERIALIZE_DB_DUCKDB_EXTENSIONS, senão .duckdb/
    # do repositório.
    local = probelib.REPO_ROOT / ".duckdb"
    directory = os.environ.get("SERIALIZE_DB_DUCKDB_EXTENSIONS") or (str(local) if local.is_dir() else None)
    report.value("DUCKDB_EXTENSION_DIRECTORY", directory or "(padrão do DuckDB)")

    # A instalação automática fica desligada: o LOAD de uma extensão conhecida baixaria a extensão
    # sem aviso.
    config: dict[str, object] = {"autoinstall_known_extensions": False, "autoload_known_extensions": False}
    if directory:
        config["extension_directory"] = directory
    connection = duckdb.connect(config=config)

    # O DuckDB recusa o endereço de proxy com as credenciais embutidas; aqui elas vão em
    # configurações à parte.
    proxy = probelib.duckdb_proxy()
    for setting, value in proxy.settings.items():
        connection.execute(f"SET {setting} = ?", [value])
    report.value("DUCKDB_HTTP_PROXY", proxy.reading)

    # A versão, a plataforma e as configurações que a conexão aplicou.
    rows: list[list[object]] = [["item", "valor"], ["versão", duckdb.__version__], ["plataforma", connection.execute("PRAGMA platform").fetchone()[0]]]
    for setting in ("threads", "memory_limit", "temp_directory", "extension_directory", "autoinstall_known_extensions", "autoload_known_extensions", "http_proxy"):
        rows.append([setting, connection.execute(f"SELECT current_setting('{setting}')").fetchone()[0]])
    report.table(rows)

    # SP-10: cada extensão carrega; o erro do LOAD é a leitura.
    rows = [["extensão", "carregou", "detalhe"]]
    missing = []
    for extension in EXTENSIONS:
        try:
            connection.execute(f"LOAD {extension}")
            rows.append([extension, "sim", ""])
        except duckdb.Error as error:
            rows.append([extension, "não", str(error).splitlines()[0][:120]])
            missing.append(extension)
    report.table(rows)

    def render_extensions(found: list[tuple]) -> str:
        """As extensões como tabela: nome, instalada, carregada, caminho e versão."""
        return probelib.tabulate([["extensão", "instalada", "carregada", "caminho", "versão"], *[[str(cell) for cell in row] for row in found]])

    report.call(
        "duckdb_extensions()",
        lambda: connection.execute(
            "SELECT extension_name, installed, loaded, install_path, extension_version FROM duckdb_extensions() "
            "WHERE extension_name IN ('httpfs', 'delta', 'aws', 'parquet', 'json') ORDER BY 1"
        ).fetchall(),
        render=render_extensions,
    )
    if missing:
        report.fail("SP-10", "extensões do DuckDB", f"não carregam: {', '.join(missing)}; rode prepare_offline.sh ou informe SERIALIZE_DB_DUCKDB_EXTENSIONS")
    else:
        report.ok("SP-10", "extensões do DuckDB", ", ".join(EXTENSIONS) + f" de {directory or 'pasta padrão'}")


def main() -> int:
    report = Report("space", "o espaço do SageMaker Unified Studio visto de dentro")
    for section in (identity, project, network, machine, python_packages, duckdb_section):
        try:
            section(report)
        except Exception as error:  # noqa: BLE001 - uma seção interrompida não cala as outras
            report.line(f"!! seção {section.__name__} interrompida: {describe_error(error)}")
            report.failures.append((f"seção {section.__name__}", describe_error(error)))
    return report.finish()


if __name__ == "__main__":
    sys.exit(main())
