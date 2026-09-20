"""Formato comum dos probes: relatório em seções, chamadas ecoadas, checagens e a seção final de falhas.

Um probe fotografa o ambiente sem alterá-lo. O relatório sai no terminal e em
``probes/output/<nome>_<data-hora>.txt``, pasta fora do git, para ser colado na conversa com o
assistente: um cabeçalho com data, plataforma e interpretador; seções numeradas; cada chamada
ecoada acima do seu resultado ou do seu erro, que também vai para a seção final "Chamadas que
falharam", para um bloco vazio nunca significar "negado"; identificadores reaproveitados como
``NOME=valor``; tabelas alinhadas; e a tabela de checagens, ``fail`` primeiro, depois ``note``,
depois ``pass``. Códigos de saída: 0 quando toda checagem passou, 1 quando alguma chamada
falhou, 2 quando alguma checagem reprovou.
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
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any, TypeVar

T = TypeVar("T")

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
REPO_ROOT = Path(__file__).resolve().parent.parent
SECRET_PATTERN = re.compile(r"secret|password|token|credential|private", re.IGNORECASE)
PROXY_VARIABLES = ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "NO_PROXY", "no_proxy")


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


def short_config(connect: float = 5, read: float = 15, attempts: int = 2) -> Any:
    """``botocore.config.Config`` com esperas curtas: sem rede, o padrão do boto3 é 60 s por tentativa."""
    import botocore.config

    return botocore.config.Config(connect_timeout=connect, read_timeout=read, retries={"total_max_attempts": attempts, "mode": "standard"})


def answered(error: BaseException) -> bool:
    """Se o serviço respondeu (erro de credencial ou de permissão) em vez de não haver resposta."""
    try:
        import botocore.exceptions
    except ImportError:
        return False
    return isinstance(error, botocore.exceptions.ClientError)


def describe_error(error: BaseException) -> str:
    """Erro numa linha; para o boto3, diz se o serviço respondeu com erro ou se não houve resposta."""
    text = re.sub(r"\x1b\[[0-9;]*m", "", " ".join(str(error).split()))[:300]
    if answered(error):
        return f"o serviço respondeu com erro: {type(error).__name__}: {text}"
    try:
        import botocore.exceptions

        if isinstance(error, botocore.exceptions.BotoCoreError):
            return f"sem resposta: {type(error).__name__}: {text}"
    except ImportError:
        pass
    return f"{type(error).__name__}: {text}"


def region() -> str | None:
    """Região como o boto3 a resolve, senão ``AWS_REGION`` ou ``AWS_DEFAULT_REGION``."""
    import boto3

    return boto3.Session().region_name or os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")


def resolve(name: str, port: int = 443) -> tuple[list[str], bool]:
    """Endereços IP de ``name`` e se todos são privados (endpoint VPC de interface com DNS privado)."""
    addresses = sorted({info[4][0] for info in socket.getaddrinfo(name, port, type=socket.SOCK_STREAM)})
    return addresses, bool(addresses) and all(ipaddress.ip_address(address).is_private for address in addresses)


def tcp_open(host: str, port: int, timeout: float = 5) -> float:
    """Tempo, em segundos, para abrir uma conexão TCP; levanta a exceção do socket quando falha."""
    started = time.perf_counter()
    with socket.create_connection((host, port), timeout=timeout):
        return time.perf_counter() - started


def mask(data: Any) -> Any:
    """Cópia de ``data`` com os valores das chaves que parecem segredo trocados por ``***``."""
    if isinstance(data, dict):
        return {key: ("***" if isinstance(key, str) and SECRET_PATTERN.search(key) and value else mask(value)) for key, value in data.items()}
    if isinstance(data, (list, tuple)):
        return [mask(value) for value in data]
    return data


def pretty(data: Any, limit: int = 120) -> str:
    """JSON legível de uma resposta, sem ``ResponseMetadata`` e sem segredos, cortado em ``limit`` linhas."""
    if isinstance(data, dict):
        data = {key: value for key, value in data.items() if key != "ResponseMetadata"}
    lines = json.dumps(mask(data), indent=2, ensure_ascii=False, default=str).splitlines()
    if len(lines) > limit:
        lines = lines[:limit] + [f"... ({len(lines) - limit} linhas omitidas)"]
    return "\n".join(lines)


def tabulate(rows: Iterable[Iterable[Any] | str]) -> str:
    """Alinha linhas como ``column -t -s $'\\t'``, com ``-`` na célula vazia."""
    split: list[list[str]] = []
    for row in rows:
        fields = row.split("\t") if isinstance(row, str) else [str(cell) for cell in row]
        fields = [field if field != "" else "-" for field in fields]
        if fields:
            split.append(fields)
    widths: dict[int, int] = {}
    for fields in split:
        for index, cell in enumerate(fields):
            widths[index] = max(widths.get(index, 0), len(cell))
    return "\n".join("".join(cell.ljust(widths[index] + 2) for index, cell in enumerate(fields[:-1])) + fields[-1] for fields in split)


def run_python(code: str, arguments: list[str], timeout: float, executable: str | None = None) -> subprocess.CompletedProcess[str]:
    """Roda ``code`` num subprocesso Python com espera limitada."""
    return subprocess.run([executable or sys.executable, "-c", code, *arguments], capture_output=True, text=True, timeout=timeout)


def environment_rows(names: Iterable[str]) -> list[list[str]]:
    """Linhas ``nome, valor`` das variáveis; as que parecem segredo mostram só presença."""
    rows = []
    for name in names:
        value = os.environ.get(name)
        if value is None:
            rows.append([name, "(ausente)"])
        elif SECRET_PATTERN.search(name) or name in ("AWS_ACCESS_KEY_ID",):
            rows.append([name, "definida"])
        else:
            rows.append([name, value])
    return rows


class Report:
    """Um relatório em construção: escreve no terminal e no arquivo, acumula checagens e falhas."""

    def __init__(self, name: str, subject: str) -> None:
        OUTPUT_DIR.mkdir(exist_ok=True)
        self.path = OUTPUT_DIR / f"{name}_{time.strftime('%Y%m%d-%H%M%S')}.txt"
        self.tee = Tee(self.path)
        self.checks: list[tuple[str, str, str, str]] = []
        self.failures: list[tuple[str, str]] = []
        self.section_number = 0
        sys.stdout = self.tee
        print("=" * 80)
        print(f"{name}: {subject}")
        print(f"{time.strftime('%Y-%m-%d %H:%M:%S %z')}; {platform.platform()}; python {platform.python_version()}")
        print(f"interpretador {sys.executable}")
        print("=" * 80)

    def h1(self, title: str) -> None:
        self.section_number += 1
        print(f"\n\n{'#' * 80}\n# {self.section_number}. {title}\n{'#' * 80}\n")

    def h2(self, title: str) -> None:
        print(f"\n--- {title} ---\n")

    def line(self, text: str = "") -> None:
        print(text)

    def value(self, name: str, value: object) -> None:
        """Identificador reaproveitado, como ``BUCKET=nome``, para o leitor refazer uma chamada à mão."""
        print(f"{name}={value}")

    def table(self, rows: Iterable[Iterable[Any] | str]) -> None:
        print(tabulate(rows))
        print()

    def call(self, label: str, action: Callable[[], T], render: Callable[[Any], str] | None = pretty) -> T | None:
        """Ecoa ``label``, executa ``action`` e imprime o resultado ou o erro; a falha vai para a seção final."""
        print(f"$ {label}")
        started = time.perf_counter()
        try:
            result = action()
        except Exception as error:  # noqa: BLE001 - toda falha é diagnóstico
            detail = describe_error(error)
            print(f"!! FALHOU ({time.perf_counter() - started:.1f} s): {detail}\n")
            self.failures.append((label, detail))
            return None
        elapsed = time.perf_counter() - started
        if render is not None:
            text = render(result)
            print(text if text else "(resultado vazio: a chamada passou e não devolveu nada)")
        print(f"({elapsed:.1f} s)\n")
        return result

    def ok(self, check_id: str, what: str, detail: str) -> None:
        self.checks.append(("pass", check_id, what, detail))

    def fail(self, check_id: str, what: str, detail: str) -> None:
        self.checks.append(("fail", check_id, what, detail))

    def note(self, check_id: str, what: str, detail: str) -> None:
        self.checks.append(("note", check_id, what, detail))

    def finish(self) -> int:
        """Imprime as checagens e as chamadas que falharam, fecha o arquivo e devolve o código de saída."""
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
        failed_checks = sum(1 for check in self.checks if check[0] == "fail")
        code = 2 if failed_checks else 1 if self.failures else 0
        print(f"\ncódigo de saída {code}: {failed_checks} checagem(ns) reprovada(s), {len(self.failures)} chamada(s) falhada(s)")
        sys.stdout = sys.__stdout__
        self.tee.close()
        print(f"resultado gravado em {self.path}")
        return code


PROJECT_PROBE = r"""
import json, sys
from sagemaker_studio import Project
project = Project()


def endpoint(item):
    return {name: getattr(item, name, None) for name in ("host", "port", "protocol", "aws_region", "aws_account_id", "access_role", "glue_connection_name", "stage")}


connections = getattr(project, "connections", [])
connections = connections() if callable(connections) else connections
rows = []
for connection in connections or []:
    row = {name: getattr(connection, name, None) for name in ("name", "type", "id", "iam_role")}
    row["type"] = str(row["type"])
    row["physical_endpoints"] = [endpoint(item) for item in (getattr(connection, "physical_endpoints", None) or [])]
    try:
        row["data"] = connection.data
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
    """Interpretadores onde ``sagemaker_studio`` pode existir: este, o do sistema do espaço e o ``python3`` do PATH."""
    import shutil

    candidates = [sys.executable, "/opt/conda/bin/python", shutil.which("python3", path=os.defpath) or ""]
    seen: list[str] = []
    for candidate in candidates:
        if candidate and Path(candidate).exists() and candidate not in seen:
            seen.append(candidate)
    return seen


def project_snapshot(timeout: float = 90) -> tuple[dict[str, Any], str]:
    """Lê o projeto do SageMaker Unified Studio com ``sagemaker_studio`` no primeiro interpretador que o tem.

    Devolve os dados e o interpretador usado; levanta ``RuntimeError`` quando nenhum interpretador tem o
    pacote ou quando a leitura falha em todos (fora de um espaço, por exemplo).
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
    """Primeiro valor de cada chave em ``names`` numa estrutura aninhada, em qualquer profundidade."""
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
