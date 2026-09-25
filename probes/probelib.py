"""Formato comum dos probes: seções, chamadas ecoadas, checagens e a seção final de falhas.

Um probe fotografa o ambiente sem alterá-lo. O relatório sai no terminal e em
``probes/output/<nome>_<data-hora>.txt``, pasta fora do git, para ser colado na conversa com o
assistente. Ele traz um cabeçalho com data, plataforma e interpretador, seções numeradas,
identificadores reaproveitados como ``NOME=valor``, tabelas alinhadas e a tabela de checagens,
``fail`` primeiro, depois ``note``, depois ``pass``.

Cada chamada é ecoada acima do seu resultado ou do seu erro, e o erro também vai para a seção
final "Chamadas que falharam", para um bloco vazio nunca significar "negado". A chamada marcada
``expected`` é a exceção: a falha dela é leitura (a visão negada a um usuário comum), sai como
``-- SEM RESULTADO`` e fica fora da seção final. Códigos de saída: 0 quando toda checagem passou,
1 quando alguma chamada falhou, 2 quando alguma checagem reprovou.

O arquivo se organiza assim:

- as constantes de caminhos e de variáveis;
- ``Tee``, que duplica a saída no terminal e no arquivo;
- as esperas curtas do ``boto3`` (``short_config``) e a classificação dos seus erros
  (``answered``, ``unanswered``, ``error_code``, ``describe_error``, ``reason``): "o serviço
  respondeu com erro" e "sem resposta" são vereditos diferentes, e só o segundo pede manutenção da
  rede;
- o ARN da simulação de política (``principal_arn``), a raiz S3 (``s3_root``, ``NO_ROOT``) e a
  região (``region``);
- o proxy do DuckDB (``DuckDBProxy``, ``split_proxy``, ``hide_credentials``, ``duckdb_proxy``), que
  recusa o endereço com as credenciais embutidas e precisa delas em configurações à parte;
- as leituras de rede (``resolve``, ``tcp_open``, ``tcp_probe``, ``endpoint_reachable``,
  ``public_label``, ``dns_rows``), que nunca contam como chamada falhada;
- a formatação (``mask``, ``pretty``, ``tabulate``, ``environment_rows``), com os segredos
  mascarados, e ``run_python``, o subprocesso com espera limitada;
- ``Report``, o relatório em construção;
- a leitura do projeto do SageMaker Unified Studio por ``sagemaker_studio`` num subprocesso
  (``PROJECT_PROBE``, ``python_candidates``, ``project_snapshot``) e as funções que tornam as
  conexões legíveis (``find_values``, ``connection_rows``).
"""

from __future__ import annotations

import ipaddress
import json
import os
import platform
import re
import socket
import subprocess
import sys
import time
import urllib.parse
from collections.abc import Callable, Iterable, Mapping
from pathlib import Path
from typing import Any, NamedTuple, TypeVar

T = TypeVar("T")

# A pasta onde o relatório é gravado, fora do git, e a raiz do repositório, de onde se leem
# pyproject.toml e .duckdb/.
OUTPUT_DIR = Path(__file__).resolve().parent / "output"
REPO_ROOT = Path(__file__).resolve().parent.parent

# Uma chave com um destes nomes tem o valor trocado por *** na saída; uma variável com um deles só
# mostra presença.
SECRET_PATTERN = re.compile(r"secret|password|token|credential|private", re.IGNORECASE)

# As variáveis de proxy nas duas grafias. O delta-rs lê NO_PROXY e, só quando ela está ausente,
# no_proxy; uma NO_PROXY vazia anula as exceções.
PROXY_VARIABLES = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "NO_PROXY", "no_proxy")

# A única variável de proxy que o DuckDB lê, nesta grafia, para HTTP e para HTTPS, na hora do
# pedido; as demais não têm efeito sobre ele.
DUCKDB_PROXY_VARIABLE = "HTTP_PROXY"

# Os serviços que têm gateway endpoint.
GATEWAY_SERVICES = ("s3", "dynamodb")


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

    def close(self) -> None:
        self.file.close()


# --------------------------------------------------------------------------------------------------
# Erros do boto3: o serviço respondeu, não respondeu, ou o erro é local


def short_config(connect: float = 5, read: float = 15, attempts: int = 2) -> Any:
    """Um ``botocore.config.Config`` com esperas curtas.

    Sem rede, o padrão do boto3 espera 60 s por tentativa.
    """
    import botocore.config

    return botocore.config.Config(
        connect_timeout=connect,
        read_timeout=read,
        retries={"total_max_attempts": attempts, "mode": "standard"},
    )


def answered(error: BaseException) -> bool:
    """Se o serviço respondeu (erro de credencial ou de permissão) em vez de não haver resposta."""
    try:
        import botocore.exceptions
    except ImportError:
        return False

    return isinstance(error, botocore.exceptions.ClientError)


def unanswered(error: BaseException) -> bool:
    """Se o boto3 não obteve resposta (rede, proxy, tempo esgotado).

    Um erro local, como a credencial ausente, não conta.
    """
    try:
        import botocore.exceptions
    except ImportError:
        return False

    if not isinstance(error, botocore.exceptions.BotoCoreError):
        return False

    # O botocore não tem uma classe única para "sem resposta"; o nome da exceção diz a causa.
    name = type(error).__name__
    return any(word in name for word in ("Timeout", "Connection", "Endpoint", "SSL", "Proxy"))


def error_code(error: BaseException) -> str | None:
    """Código de erro do serviço numa ``ClientError`` do botocore; ``None`` para os demais erros."""
    response = getattr(error, "response", None)
    code = response.get("Error", {}).get("Code") if isinstance(response, dict) else None
    return str(code) if code else None


def describe_error(error: BaseException) -> str:
    """O erro numa linha, com o prefixo que diz se o serviço respondeu com erro ou não respondeu.

    Um erro local, ou de outra biblioteca que não o boto3, sai sem prefixo.
    """
    # O texto sem as cores do terminal nem quebras de linha, cortado para caber na seção final.
    text = re.sub(r"\x1b\[[0-9;]*m", "", " ".join(str(error).split()))[:300]

    if answered(error):
        return f"o serviço respondeu com erro: {type(error).__name__}: {text}"
    if unanswered(error):
        return f"sem resposta: {type(error).__name__}: {text}"
    return f"{type(error).__name__}: {text}"


def reason(error: BaseException) -> str:
    """O motivo curto de uma falha, para a checagem que a interpreta.

    O motivo é ``negado``, outro erro do serviço, ``sem resposta`` ou ``erro local``.
    """
    code = error_code(error)
    if code:
        denied = any(word in code for word in ("AccessDenied", "Unauthorized", "Forbidden", "NotAuthorized"))
        return f"negado ({code})" if denied else f"o serviço respondeu {code}"
    if answered(error):
        return f"o serviço respondeu com erro ({type(error).__name__})"
    if unanswered(error):
        return f"sem resposta ({type(error).__name__})"
    return f"erro local ({type(error).__name__})"


def principal_arn(caller_arn: str) -> str:
    """O ARN que a simulação de política aceita.

    Um assumed-role vira o ARN do papel por trás dele; qualquer outro ARN volta como veio.
    """
    if ":assumed-role/" in caller_arn:
        account = caller_arn.split(":")[4]
        role = caller_arn.split(":assumed-role/")[1].split("/")[0]
        return f"arn:aws:iam::{account}:role/{role}"
    return caller_arn


def s3_root(argv: list[str]) -> tuple[str, str]:
    """A raiz ``s3://bucket/prefixo`` que um probe fotografa, e de onde ela veio.

    A ordem é o argumento da linha de comando, ``SERIALIZE_DB_ROOT`` (a raiz da biblioteca, onde ela
    escreveria) e ``SERIALIZE_DB_TEST_S3_ROOT`` (a autorização da suíte S3, que costuma apontar para
    o mesmo lugar). Uma ``SERIALIZE_DB_ROOT`` de pasta local é ignorada, porque estes probes leem
    S3. Devolve ``("", "nada")`` quando nenhuma das três diz onde olhar.
    """
    candidates = [
        (argv[1] if len(argv) > 1 else "", "argumento"),
        (os.environ.get("SERIALIZE_DB_ROOT", ""), "SERIALIZE_DB_ROOT"),
        (os.environ.get("SERIALIZE_DB_TEST_S3_ROOT", ""), "SERIALIZE_DB_TEST_S3_ROOT"),
    ]
    for value, source in candidates:
        if value.startswith("s3://"):
            return value.rstrip("/"), source
        # Um argumento que não é s3:// volta como veio: o probe o aponta, em vez de ignorá-lo.
        if value and source == "argumento":
            return value.rstrip("/"), source

    return "", "nada"


# A frase que um probe imprime quando nenhuma das três origens diz onde olhar.
NO_ROOT = "sem raiz: informe s3://bucket/prefixo como argumento, em SERIALIZE_DB_ROOT ou em SERIALIZE_DB_TEST_S3_ROOT"


def region() -> str | None:
    """Região como o boto3 a resolve, senão ``AWS_REGION`` ou ``AWS_DEFAULT_REGION``."""
    import boto3

    return boto3.Session().region_name or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")


# --------------------------------------------------------------------------------------------------
# O proxy do DuckDB: o endereço de um lado, o usuário e a senha do outro


class DuckDBProxy(NamedTuple):
    """As configurações de proxy de uma sessão do DuckDB e a leitura que vai ao relatório."""

    settings: dict[str, str]
    reading: str


def split_proxy(url: str) -> tuple[str, str, str]:
    """Divide o ``url`` do proxy em endereço sem credenciais, usuário e senha, com URL-decode.

    O endereço volta vazio quando não há host ou a porta não é um número; o esquema é opcional.
    """
    try:
        # Sem "//", o urlsplit leria o host como esquema e o resto como caminho.
        parts = urllib.parse.urlsplit(url if "//" in url else "//" + url, scheme="http")
        port = parts.port
    except ValueError:
        return "", "", ""
    if not parts.hostname:
        return "", "", ""
    address = parts.hostname + (f":{port}" if port else "")
    return address, urllib.parse.unquote(parts.username or ""), urllib.parse.unquote(parts.password or "")


def hide_credentials(url: str) -> str:
    """``url`` com o usuário e a senha embutidos trocados por ``***``, para o relatório."""
    head, separator, tail = url.rpartition("@")
    if not separator:
        return url
    scheme, mark, _ = head.partition("//")
    return f"{scheme}{mark}***@{tail}"


def duckdb_proxy(environ: Mapping[str, str] | None = None) -> DuckDBProxy:
    """As configurações de proxy a aplicar numa sessão do DuckDB, lidas do ambiente.

    O DuckDB recusa o endereço com as credenciais embutidas ("Failed to parse http_proxy ... into a
    host and port"), que é como um proxy corporativo costuma aparecer no ambiente, e o erro atinge
    tanto o download de uma extensão quanto o acesso ao S3 pelo ``httpfs``. O endereço vai sem elas
    em ``http_proxy``, e o usuário e a senha, sem URL-encode, em ``http_proxy_username`` e
    ``http_proxy_password``, lidos das variáveis ``username`` e ``password`` e, sem elas, do próprio
    endereço com URL-decode.

    Só ``HTTP_PROXY`` é lida, a mesma variável e a mesma grafia que o DuckDB lê: as configurações
    não têm exceção equivalente a ``NO_PROXY``, e tirar o endereço de outra variável mandaria ao
    proxy o tráfego que o DuckDB, sem elas, manda direto. As demais grafias presentes entram na
    leitura.
    """
    environ = os.environ if environ is None else environ

    url = (environ.get(DUCKDB_PROXY_VARIABLE) or "").strip()
    if not url:
        ignored = [name for name in ("http_proxy", "HTTPS_PROXY", "https_proxy") if environ.get(name)]
        if ignored:
            return DuckDBProxy({}, f"sem {DUCKDB_PROXY_VARIABLE}; o DuckDB ignora {', '.join(ignored)}")
        return DuckDBProxy({}, "sem proxy no ambiente")

    address, user, secret = split_proxy(url)
    if not address:
        return DuckDBProxy({}, f"{DUCKDB_PROXY_VARIABLE} sem host: {hide_credentials(url)}")

    settings = {"http_proxy": address}
    user = environ.get("username") or user
    secret = environ.get("password") or secret
    if user:
        settings["http_proxy_username"] = user
        settings["http_proxy_password"] = secret
    return DuckDBProxy(settings, f"{address}, {'com usuário e senha' if user else 'sem credenciais'}")


# --------------------------------------------------------------------------------------------------
# Rede: DNS e TCP como leituras, nunca como chamadas falhadas


def resolve(name: str, port: int = 443) -> tuple[list[str], bool]:
    """Os endereços IP de ``name`` e se todos são privados.

    Todos privados indicam endpoint VPC de interface com DNS privado.
    """
    addresses = sorted({info[4][0] for info in socket.getaddrinfo(name, port, type=socket.SOCK_STREAM)})
    private = bool(addresses) and all(ipaddress.ip_address(address).is_private for address in addresses)
    return addresses, private


def tcp_open(host: str, port: int, timeout: float = 5) -> float:
    """Tempo, em segundos, para abrir uma conexão TCP; levanta a exceção do socket quando falha."""
    started = time.perf_counter()
    with socket.create_connection((host, port), timeout=timeout):
        return time.perf_counter() - started


def tcp_probe(host: str, port: int, timeout: float = 5) -> str:
    """Abre uma conexão TCP como leitura: ``conectou em N s`` ou ``não conectou: erro``.

    O erro do socket vira o texto da leitura, sem exceção.
    """
    try:
        return f"conectou em {tcp_open(host, port, timeout):.2f} s"
    except OSError as error:
        return f"não conectou: {error}"


def endpoint_reachable(client: Any, timeout: float = 2) -> tuple[bool, str]:
    """Se o endpoint de um cliente boto3 aceita conexão TCP em ``timeout``, e a leitura que o diz.

    Um serviço sem endpoint VPC e sem internet gasta ``connect_timeout`` em cada endereço que o nome
    resolve, vezes as tentativas: no ambiente alvo, em 2026-09-21, ``simulate_principal_policy``
    esperou 10 s e ``describe_key`` 80 s por nada, com ``short_config()`` em 5 s e duas tentativas.
    O teste vai a um endereço só, e custa ``timeout`` uma vez. Com proxy configurado, a conexão
    direta não responde pelo alcance, e a chamada é feita.
    """
    if any(os.environ.get(name) for name in PROXY_VARIABLES if "NO_PROXY" not in name.upper()):
        return True, "proxy configurado: a conexão direta não responde pelo alcance"

    parts = urllib.parse.urlsplit(client.meta.endpoint_url)
    if not parts.hostname:
        return True, f"endpoint sem host: {client.meta.endpoint_url}"

    port = parts.port or (80 if parts.scheme == "http" else 443)
    try:
        addresses, _ = resolve(parts.hostname, port)
    except OSError as error:
        return False, f"{parts.hostname} não resolve: {error}"

    # O endereço, e não o nome: com o nome, socket.create_connection tentaria cada endereço que ele
    # resolve, com o tempo limite em cada um.
    where = parts.hostname if addresses[0] == parts.hostname else f"{parts.hostname} ({addresses[0]})"
    reading = tcp_probe(addresses[0], port, timeout)
    return reading.startswith("conectou"), f"{where}:{port} {reading}"


def public_label(name: str) -> str:
    """O tipo de um nome que resolve para IP público.

    Só o S3 e o DynamoDB têm gateway endpoint; os demais dependem da internet ou do proxy.
    """
    # O serviço é o primeiro rótulo (s3.us-west-2...) ou o segundo (bucket.s3.us-west-2...).
    labels = name.lower().split(".")
    gateway = labels[0] in GATEWAY_SERVICES or (len(labels) > 1 and labels[1] in GATEWAY_SERVICES)
    return "público: gateway endpoint ou internet" if gateway else "público: só pela internet ou pelo proxy"


def dns_rows(names: Iterable[str]) -> tuple[list[list[str]], dict[str, bool | None]]:
    """As linhas da tabela de DNS e, por nome, se ele resolveu para IP privado.

    Um nome que não resolve fica com ``None`` e é uma leitura, não uma chamada falhada: sem
    internet, ``pypi.org`` não resolve.
    """
    rows: list[list[str]] = []
    private_by_name: dict[str, bool | None] = {}

    for name in names:
        try:
            addresses, private = resolve(name)
        except OSError as error:
            rows.append([name, f"não resolve: {error}", "-"])
            private_by_name[name] = None
            continue

        # Até quatro endereços por linha; um endpoint regional do S3 resolve para oito.
        shown = ", ".join(addresses[:4]) + (" ..." if len(addresses) > 4 else "")
        kind = "privado: endpoint VPC de interface com DNS privado" if private else public_label(name)
        rows.append([name, shown, kind])
        private_by_name[name] = private

    return rows, private_by_name


# --------------------------------------------------------------------------------------------------
# Formatação: JSON legível, tabelas alinhadas e segredos mascarados


def mask(data: Any) -> Any:
    """Cópia de ``data`` com os valores das chaves que parecem segredo trocados por ``***``."""
    if isinstance(data, dict):
        return {
            key: ("***" if isinstance(key, str) and SECRET_PATTERN.search(key) and value else mask(value))
            for key, value in data.items()
        }
    if isinstance(data, (list, tuple)):
        return [mask(value) for value in data]
    return data


def pretty(data: Any, limit: int = 120) -> str:
    """O JSON legível de uma resposta, sem ``ResponseMetadata`` e sem segredos.

    O texto é cortado em ``limit`` linhas.
    """
    if isinstance(data, dict):
        data = {key: value for key, value in data.items() if key != "ResponseMetadata"}

    lines = json.dumps(mask(data), indent=2, ensure_ascii=False, default=str).splitlines()
    if len(lines) > limit:
        lines = lines[:limit] + [f"... ({len(lines) - limit} linhas omitidas)"]
    return "\n".join(lines)


def tabulate(rows: Iterable[Iterable[Any] | str]) -> str:
    """Alinha linhas como ``column -t -s $'\\t'``, com ``-`` na célula vazia."""
    # Cada linha vira uma lista de células; uma linha dada como texto é dividida por tabulação.
    split: list[list[str]] = []
    for row in rows:
        fields = row.split("\t") if isinstance(row, str) else [str(cell) for cell in row]
        fields = [field if field != "" else "-" for field in fields]
        if fields:
            split.append(fields)

    # A largura de cada coluna é a da célula mais larga; a última coluna não recebe preenchimento.
    widths: dict[int, int] = {}
    for fields in split:
        for index, cell in enumerate(fields):
            widths[index] = max(widths.get(index, 0), len(cell))

    return "\n".join(
        "".join(cell.ljust(widths[index] + 2) for index, cell in enumerate(fields[:-1])) + fields[-1]
        for fields in split
    )


def run_python(code: str, arguments: list[str], timeout: float, executable: str | None = None) -> subprocess.CompletedProcess[str]:
    """Roda ``code`` num subprocesso Python com espera limitada."""
    return subprocess.run(
        [executable or sys.executable, "-c", code, *arguments],
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def environment_rows(names: Iterable[str]) -> list[list[str]]:
    """Linhas ``nome, valor`` das variáveis; as que parecem segredo mostram só presença."""
    names = list(names)
    rows = []
    for name in names:
        value = os.environ.get(name)
        if value is None:
            rows.append([name, "(ausente)"])
        elif value == "":
            # Uma variável vazia não é ausente: um cliente que lê NO_PROXY antes de no_proxy fica
            # sem exceção alguma.
            rows.append([name, "(vazia)"])
        elif name != name.upper() and name.upper() in names and value == os.environ.get(name.upper()):
            # A minúscula igual à maiúscula (no_proxy e NO_PROXY) sai uma vez: a lista de exceções
            # tem 1.500 caracteres.
            rows.append([name, f"(igual a {name.upper()})"])
        elif SECRET_PATTERN.search(name) or name in ("AWS_ACCESS_KEY_ID",):
            rows.append([name, "definida"])
        else:
            # O endereço de proxy costuma trazer o usuário e a senha embutidos, e o relatório é
            # colado na conversa.
            rows.append([name, hide_credentials(value) if "proxy" in name.lower() else value])
    return rows


# --------------------------------------------------------------------------------------------------
# O relatório


class Report:
    """Um relatório em construção: escreve no terminal e no arquivo, acumula checagens e falhas.

    Um probe cria um ``Report``, abre seções com ``h1``, ecoa cada chamada com ``call``, registra
    checagens com ``ok``, ``fail`` e ``note`` e termina com ``finish``, que imprime a tabela de
    checagens, a seção final de falhas e devolve o código de saída.
    """

    def __init__(self, name: str, subject: str) -> None:
        OUTPUT_DIR.mkdir(exist_ok=True)
        self.path = OUTPUT_DIR / f"{name}_{time.strftime('%Y%m%d-%H%M%S')}.txt"
        self.tee = Tee(self.path)
        self.checks: list[tuple[str, str, str, str]] = []
        self.failures: list[tuple[str, str]] = []
        # O motivo curto da última chamada que falhou, para a checagem que a interpreta.
        self.last_reason = "sem falha"
        self.section_number = 0

        # Tudo o que o probe imprime vai para o terminal e para o arquivo; finish() desfaz o desvio.
        sys.stdout = self.tee
        print("=" * 80)
        print(f"{name}: {subject}")
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S %z')}; {platform.platform()}; python {platform.python_version()}")
        print(f"interpretador {sys.executable}")
        print("=" * 80)

    def h1(self, title: str) -> None:
        """Abre uma seção numerada."""
        self.section_number += 1
        print(f"\n\n{'#' * 80}\n# {self.section_number}. {title}\n{'#' * 80}\n")

    def h2(self, title: str) -> None:
        """Abre uma subseção, sem número."""
        print(f"\n--- {title} ---\n")

    def line(self, text: str = "") -> None:
        """Uma linha de texto livre."""
        print(text)

    def value(self, name: str, value: object) -> None:
        """Um identificador reaproveitado, como ``BUCKET=nome``, para refazer uma chamada à mão."""
        print(f"{name}={value}")

    def table(self, rows: Iterable[Iterable[Any] | str]) -> None:
        """Uma tabela alinhada, a primeira linha como cabeçalho, e uma linha em branco depois."""
        print(tabulate(rows))
        print()

    def call(self, label: str, action: Callable[[], T], render: Callable[[Any], str] | None = pretty, expected: bool = False) -> T | None:
        """Ecoa ``label``, executa ``action`` e imprime o resultado ou o erro.

        Devolve o resultado de ``action``, ou ``None`` quando ela levantou exceção; nesse caso a
        falha vai para a seção final, e ``last_reason`` guarda o motivo curto para a checagem que a
        interpreta. ``render`` transforma o resultado em texto (``pretty`` por padrão; ``None`` não
        imprime nada).

        ``expected=True`` marca a chamada cuja falha é leitura, não defeito: a visão de sistema
        negada a um usuário comum, o pacote ausente fora de um espaço. Ela aparece no lugar e deixa
        ``last_reason`` para a checagem que a interpreta, mas fica fora da seção final e do código
        de saída, que existem para o que precisa de manutenção.
        """
        print(f"$ {label}")
        started = time.perf_counter()
        try:
            result = action()
        except Exception as error:  # noqa: BLE001 - toda falha é diagnóstico
            detail = describe_error(error)
            self.last_reason = reason(error)
            marker = "-- SEM RESULTADO" if expected else "!! FALHOU"
            print(f"{marker} ({time.perf_counter() - started:.1f} s): {detail}\n")
            if not expected:
                self.failures.append((label, detail))
            return None

        elapsed = time.perf_counter() - started
        if render is not None:
            text = render(result)
            print(text if text else "(resultado vazio: a chamada passou e não devolveu nada)")
        print(f"({elapsed:.1f} s)\n")
        return result

    def ok(self, check_id: str, what: str, detail: str) -> None:
        """Checagem aprovada."""
        self.checks.append(("pass", check_id, what, detail))

    def fail(self, check_id: str, what: str, detail: str) -> None:
        """Checagem reprovada: algo que impede a biblioteca; leva o código de saída a 2."""
        self.checks.append(("fail", check_id, what, detail))

    def note(self, check_id: str, what: str, detail: str) -> None:
        """Leitura registrada sem veredito: o ausente, o negado, o que só a próxima etapa decide."""
        self.checks.append(("note", check_id, what, detail))

    def finish(self) -> int:
        """Imprime as checagens e as chamadas que falharam e devolve o código de saída.

        O arquivo do relatório é fechado no fim.
        """
        # A tabela de checagens: fail primeiro, depois note, depois pass.
        self.h1("Checagens")
        rows: list[list[str]] = [["RESULTADO", "ID", "O QUE", "DETALHE"]]
        for kind in ("fail", "note", "pass"):
            rows += [list(check) for check in self.checks if check[0] == kind]
        self.table(rows)

        self.h1("Chamadas que falharam")
        if self.failures:
            print("Cada entrada é uma chamada cujo resultado falta acima; um bloco vazio em outro lugar significa que a chamada passou e não devolveu nada.\n")
            for label, detail in self.failures:
                print(f"- {label}\n  {detail}")
        else:
            print("Nenhuma. Toda chamada deste relatório passou.")

        # O código de saída: 2 com alguma checagem reprovada, senão 1 com alguma chamada falhada,
        # senão 0.
        failed_checks = sum(1 for check in self.checks if check[0] == "fail")
        code = 2 if failed_checks else 1 if self.failures else 0
        print(f"\ncódigo de saída {code}: {failed_checks} checagem(ns) reprovada(s), {len(self.failures)} chamada(s) falhada(s)")

        # O sys.stdout volta ao que o construtor desviou: no probe, o terminal; no pytest, o
        # capture.
        sys.stdout = self.tee.terminal
        self.tee.close()
        print(f"resultado gravado em {self.path}")
        return code


# --------------------------------------------------------------------------------------------------
# O projeto do SageMaker Unified Studio, lido por sagemaker_studio num subprocesso

# O programa que lê o projeto, rodado em cada interpretador candidato: o pacote sagemaker_studio
# não entra no venv do projeto, porque arrasta versões sem fixação. A saída é um JSON com o projeto
# e uma linha por conexão.
PROJECT_PROBE = r"""
import ast, json, sys
from sagemaker_studio import Project
project = Project()


def endpoint(item):
    return {name: getattr(item, name, None) for name in ("host", "port", "protocol", "aws_region", "aws_account_id", "access_role", "glue_connection_name", "stage")}


def parse_repr(text):
    # ConnectionData(a='x',b={'k': 1}) vira {'a': 'x', 'b': {'k': 1}}: a repr do pacote é uma chamada com literais.
    try:
        node = ast.parse(text.strip(), mode="eval").body
    except SyntaxError:
        return {"repr": text}
    if not isinstance(node, ast.Call):
        return {"repr": text}
    parsed = {}
    for keyword in node.keywords:
        try:
            parsed[keyword.arg] = ast.literal_eval(keyword.value)
        except (ValueError, SyntaxError):
            parsed[keyword.arg] = ast.unparse(keyword.value)
    return parsed


def as_dict(data):
    # Os dados da conexão como dicionário, para banco, workgroup e secret serem localizáveis por chave.
    if data is None or isinstance(data, dict):
        return data
    for name in ("to_dict", "model_dump", "dict"):
        method = getattr(data, name, None)
        if callable(method):
            try:
                result = method()
            except Exception:
                continue
            if isinstance(result, dict):
                return result
    fields = getattr(data, "__dict__", None)
    if isinstance(fields, dict) and fields:
        inner = [value for value in fields.values() if isinstance(value, dict)]
        if len(fields) == 1 and inner:
            return inner[0]
        return {key.lstrip("_"): value for key, value in fields.items()}
    return parse_repr(repr(data))


connections = getattr(project, "connections", [])
connections = connections() if callable(connections) else connections
rows = []
for connection in connections or []:
    row = {name: getattr(connection, name, None) for name in ("name", "type", "id", "iam_role")}
    row["type"] = str(row["type"])
    row["physical_endpoints"] = [endpoint(item) for item in (getattr(connection, "physical_endpoints", None) or [])]
    try:
        row["data"] = as_dict(connection.data)
    except Exception as error:
        row["data_error"] = f"{type(error).__name__}: {error}"
    rows.append(row)
s3 = getattr(project, "s3", None)
print(json.dumps({
    "name": getattr(project, "name", None), "id": getattr(project, "id", None), "domain_id": getattr(project, "domain_id", None),
    "iam_role": getattr(project, "iam_role", None), "kms_key_arn": getattr(project, "kms_key_arn", None),
    "user_id": getattr(project, "user_id", None), "s3_root": getattr(s3, "root", None), "connections": rows,
}, default=str))
"""


def python_candidates() -> list[str]:
    """Os interpretadores onde ``sagemaker_studio`` pode existir, sem repetição e sem os ausentes.

    São este, o do sistema do espaço (``/opt/conda/bin/python``) e o ``python3`` do caminho padrão
    do sistema (``os.defpath``).
    """
    import shutil

    candidates = [sys.executable, "/opt/conda/bin/python", shutil.which("python3", path=os.defpath) or ""]
    seen: list[str] = []
    for candidate in candidates:
        if candidate and Path(candidate).exists() and candidate not in seen:
            seen.append(candidate)
    return seen


def project_snapshot(timeout: float = 90) -> tuple[dict[str, Any], str]:
    """Lê o projeto do SageMaker Unified Studio com ``sagemaker_studio``.

    A leitura roda ``PROJECT_PROBE`` em cada interpretador de ``python_candidates`` até o primeiro
    que a completa. Devolve os dados e o interpretador usado; levanta ``RuntimeError`` quando nenhum
    interpretador tem o pacote ou quando a leitura falha em todos (fora de um espaço, por exemplo),
    com a última linha do erro de cada um.
    """
    errors = []
    for executable in python_candidates():
        completed = run_python(PROJECT_PROBE, [], timeout, executable)
        if completed.returncode == 0:
            return json.loads(completed.stdout), executable

        last_line = (completed.stderr.strip().splitlines() or ["(sem saída de erro)"])[-1]
        errors.append(f"{executable}: {last_line[:200]}")

    raise RuntimeError("; ".join(errors))


def find_values(data: Any, names: Iterable[str]) -> dict[str, Any]:
    """O primeiro valor não vazio de cada chave de ``names`` numa estrutura aninhada.

    A busca desce por dicionários, listas e tuplas, em qualquer profundidade.
    """
    wanted = set(names)
    found: dict[str, Any] = {}

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            for key, value in node.items():
                if key in wanted and key not in found and value not in (None, ""):
                    found[key] = value
                walk(value)
        elif isinstance(node, (list, tuple)):
            for value in node:
                walk(value)

    walk(data)
    return found


# O dado que distingue uma conexão na tabela, na ordem de preferência: URI S3, workgroup, banco,
# URL JDBC, host e versão do Glue.
CONNECTION_DETAILS = ("s3_uri", "workgroup_name", "database_name", "jdbc_url", "host", "glue_version")


def connection_rows(connections: Iterable[dict[str, Any]]) -> list[list[str]]:
    """Uma linha por conexão do projeto: nome, tipo, endpoint e o dado que a distingue.

    O dado é o primeiro de ``CONNECTION_DETAILS`` que a conexão traz, como a URI S3, o workgroup ou
    o banco.
    """
    rows: list[list[str]] = []
    for item in connections:
        # Os endpoints com host, como host:porta separados por ponto e vírgula.
        endpoints = item.get("physical_endpoints") or []
        shown = "; ".join(f"{endpoint.get('host')}:{endpoint.get('port')}" for endpoint in endpoints if endpoint.get("host")) or "-"

        # O detalhe vem dos dados da conexão; sem eles, do nome da conexão Glue do endpoint; sem
        # nada, do erro da leitura.
        data = item.get("data") if isinstance(item.get("data"), dict) else {}
        detail = next((f"{key}={data[key]}" for key in CONNECTION_DETAILS if data.get(key)), None)
        detail = detail or next(
            (f"glue_connection_name={endpoint['glue_connection_name']}" for endpoint in endpoints if endpoint.get("glue_connection_name")),
            None,
        )
        rows.append([str(item.get("name")), str(item.get("type")), shown, detail or item.get("data_error") or "-"])
    return rows
