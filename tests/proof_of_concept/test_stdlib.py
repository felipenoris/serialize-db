"""A biblioteca padrão do Python nos papéis que as etapas (``docs/PLAN-STAGE-<n>.md``) lhe dão.

Sem gravar arquivo: a aritmética de meses (``datetime``, ``calendar``), os identificadores de
execução e de sandbox (``uuid``, ``re``), o protocolo do motor e as configurações
(``typing.Protocol``, ``dataclasses``), o gerenciador de contexto que descarta o sandbox mesmo com a
auditoria reprovada (``contextlib``), o ponto de entrada ``modulo:funcao`` (``importlib``), a linha
de comando (``argparse``), o log da execução (``logging``), o ambiente normalizado (``os.environ``
com ``monkeypatch``), o arquivo de controle e os metadados de commit (``json``), as URIs dos dois
armazenamentos (``urllib.parse``, ``pathlib``), o agrupamento das ações do log por mês
(``itertools``, ``collections``), os totais da auditoria (``decimal``) e o diff dos arquivos gerados
(``difflib``). Sob a raiz local (marcador ``local``): a criação exclusiva, a substituição atômica e
a impressão digital de um arquivo (``os``, ``tempfile``, ``hashlib``, ``shutil``).
"""

from __future__ import annotations

import argparse
import calendar
import collections
import contextlib
import dataclasses
import datetime as dt
import decimal
import difflib
import hashlib
import importlib
import itertools
import json
import logging
import os
import re
import shutil
import tempfile
import time
import urllib.parse
import uuid
from collections.abc import Callable, Iterator, MutableMapping
from pathlib import Path, PurePosixPath
from typing import Protocol, runtime_checkable

import pytest

from conftest import LocalLocation


def previous_months(month: str, count: int) -> list[str]:
    """Os ``count`` meses até ``month``, inclusive, como ``YYYY-MM`` em ordem crescente."""
    last = dt.date.fromisoformat(f"{month}-01")
    months = []

    for _ in range(count):
        months.append(last.strftime("%Y-%m"))
        last = (last - dt.timedelta(days=1)).replace(day=1)  # o dia anterior ao dia 1 cai no mês anterior

    return sorted(months)


def test_month_arithmetic() -> None:
    """``run.previous_months(n)`` e o mês seguinte vêm de ``date`` e ``calendar``, sem biblioteca externa."""
    assert previous_months("2026-08", 12) == [f"2025-{m:02d}" for m in range(9, 13)] + [f"2026-{m:02d}" for m in range(1, 9)]
    assert previous_months("2026-01", 2) == ["2025-12", "2026-01"]

    # O mês seguinte, com a virada do ano; monthrange dá o dia da semana do dia 1 e a quantidade de dias.
    year, month = 2026, 12
    following = dt.date(year + month // 12, month % 12 + 1, 1)
    assert following.strftime("%Y-%m") == "2027-01"
    assert calendar.monthrange(2026, 2) == (6, 28)

    # O mês de uma data, como a auditoria confere mes = strftime(data_ref, '%Y-%m').
    assert dt.datetime(2026, 8, 31, 23, 59).strftime("%Y-%m") == "2026-08"


def sandbox_prefix(execution_id: str) -> str:
    """``exec_<id>_`` com o identificador reduzido a ``[a-z0-9_]``, dentro dos 127 bytes de um identificador do Redshift."""
    normalized = re.sub(r"[^a-z0-9_]", "_", execution_id.lower())
    prefix = f"exec_{normalized}_"

    longest_table = 63  # a maior tabela do modelo cabe depois do prefixo
    if len(prefix.encode()) + longest_table > 127:
        raise ValueError(f"identificador longo demais para o Redshift: {execution_id}")

    return prefix


def test_execution_identifiers() -> None:
    """Identificadores únicos e nomes válidos nos dois motores: ``uuid``, ``re`` e o instante em UTC."""
    generated = uuid.uuid4().hex[:8]
    assert re.fullmatch(r"[0-9a-f]{8}", generated)

    assert sandbox_prefix("exec-2026-09-05") == "exec_exec_2026_09_05_"
    assert sandbox_prefix("Correção/Agosto") == "exec_corre__o_agosto_"
    with pytest.raises(ValueError):
        sandbox_prefix("x" * 70)

    # published_at e os carimbos do log: sempre em UTC, com o fuso explícito.
    stamp = dt.datetime.now(dt.timezone.utc)
    assert stamp.tzinfo is dt.timezone.utc
    assert stamp.isoformat(timespec="seconds").endswith("+00:00")


@runtime_checkable
class Engine(Protocol):
    """A interface que os dois motores implementam; a execução só depende dela."""

    def ingest(self, table: str, uri: str, version: int, months: list[str] | None = None, materialize: bool = False) -> None: ...

    def cleanup(self) -> None: ...


@dataclasses.dataclass(frozen=True, kw_only=True)
class DuckDBConfig:
    """A configuração do motor DuckDB; imutável e com padrões explícitos."""

    database: str | None = None
    threads: int | None = None
    memory_limit: str | None = None
    extension_directory: str | None = None
    extensions: tuple[str, ...] = ("delta",)


class FakeEngine:
    """Um motor de mentira que só registra o que a execução lhe pediu."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, int]] = []
        self.cleaned = False

    def ingest(self, table: str, uri: str, version: int, months: list[str] | None = None, materialize: bool = False) -> None:
        self.calls.append((table, uri, version))

    def cleanup(self) -> None:
        self.cleaned = True


def test_engine_protocol_and_config_dataclass() -> None:
    """``Protocol`` verifica a interface pela forma; ``dataclass`` dá configuração imutável, comparável e copiável."""
    assert isinstance(FakeEngine(), Engine)

    class Incomplete:
        def ingest(self, *args: object) -> None: ...

    assert not isinstance(Incomplete(), Engine)  # sem cleanup não é um Engine

    config = DuckDBConfig(threads=2, memory_limit="4GB")
    assert config.extensions == ("delta",)
    assert dataclasses.replace(config, threads=4).threads == 4 and config.threads == 2
    assert dataclasses.asdict(config)["memory_limit"] == "4GB"
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.threads = 8  # type: ignore[misc]


class AuditFailed(Exception):
    """A auditoria reprovou; a execução encerra sem tocar o Delta."""


@contextlib.contextmanager
def execution(engine: FakeEngine) -> Iterator[FakeEngine]:
    """O ciclo de uma execução: abre, entrega o motor ao pipeline e descarta o sandbox aconteça o que acontecer."""
    engine.ingest("cad_lancamentos", "/dados/prod/cad_lancamentos", 143)
    try:
        yield engine
    finally:
        engine.cleanup()


def test_context_manager_cleans_up_on_failure() -> None:
    """``contextlib`` garante o ``cleanup`` na saída normal e na exceção; ``ExitStack`` empilha vários recursos."""
    engine = FakeEngine()
    with execution(engine) as run:
        assert run.calls == [("cad_lancamentos", "/dados/prod/cad_lancamentos", 143)]
    assert engine.cleaned

    failed = FakeEngine()
    with pytest.raises(AuditFailed, match="chave duplicada"):
        with execution(failed):
            raise AuditFailed("chave duplicada em 2026-08")
    assert failed.cleaned  # o sandbox foi descartado mesmo assim

    # Vários recursos abertos em sequência e fechados na ordem inversa.
    engines = [FakeEngine(), FakeEngine()]
    with contextlib.ExitStack() as stack:
        for candidate in engines:
            stack.enter_context(execution(candidate))
    assert all(candidate.cleaned for candidate in engines)

    # suppress ignora uma exceção esperada, como a pasta de transbordo que já não existe.
    with contextlib.suppress(FileNotFoundError):
        os.remove("/caminho/que/nao/existe")


def load_entry_point(spec: str) -> Callable[..., object]:
    """O ponto de entrada ``modulo:funcao`` de ``serialize-db run``, resolvido por ``importlib``."""
    module_name, separator, attribute = spec.partition(":")
    if not separator or not attribute:
        raise ValueError(f"esperado modulo:funcao, recebido {spec!r}")

    return getattr(importlib.import_module(module_name), attribute)


def test_entry_point_by_import_string() -> None:
    """A função do pipeline é carregada pelo nome do módulo e do atributo; os erros distinguem módulo e função."""
    assert load_entry_point("json:dumps") is json.dumps

    with pytest.raises(ValueError):
        load_entry_point("json")
    with pytest.raises(ModuleNotFoundError):
        load_entry_point("pipeline_inexistente:main")
    with pytest.raises(AttributeError):
        load_entry_point("json:inexistente")


def month_argument(text: str) -> str:
    """Valida ``YYYY-MM`` na linha de comando; ``argparse`` transforma a exceção em mensagem e saída 2."""
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", text):
        raise argparse.ArgumentTypeError(f"mês inválido: {text!r} (esperado YYYY-MM)")

    return text


def build_parser(environ: MutableMapping[str, str]) -> argparse.ArgumentParser:
    """``serialize-db run`` e ``serialize-db schema``, com as variáveis de ambiente como padrão dos argumentos."""
    parser = argparse.ArgumentParser(prog="serialize-db")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="executa o pipeline de um mês")
    run.add_argument("--root", default=environ.get("SERIALIZE_DB_ROOT"), required="SERIALIZE_DB_ROOT" not in environ)
    run.add_argument("--environment", default=environ.get("SERIALIZE_DB_ENVIRONMENT", "dev"))
    run.add_argument("--engine", choices=["duckdb", "redshift"], default=environ.get("SERIALIZE_DB_ENGINE", "duckdb"))
    run.add_argument("--month", type=month_argument, required=True)
    run.add_argument("--execution-id", default=None)
    run.add_argument("pipeline", help="modulo:funcao que recebe a execução aberta")

    schema = commands.add_parser("schema", help="gera ou confere os arquivos de esquema")
    schema.add_argument("action", choices=["write", "check"])
    schema.add_argument("--directory", default="schema")

    return parser


def test_command_line_parsing() -> None:
    """``argparse`` com subcomandos, escolhas, validação de tipo e padrões vindos do ambiente."""
    parser = build_parser({"SERIALIZE_DB_ROOT": "s3://bucket/projeto/delta"})

    args = parser.parse_args(["run", "--month", "2026-08", "pipeline:main"])
    assert (args.command, args.root, args.environment, args.engine) == ("run", "s3://bucket/projeto/delta", "dev", "duckdb")
    assert args.month == "2026-08" and args.pipeline == "pipeline:main" and args.execution_id is None

    check = parser.parse_args(["schema", "check", "--directory", "esquemas"])
    assert (check.command, check.action, check.directory) == ("schema", "check", "esquemas")

    # Erros de uso saem com código 2 e a mensagem do argparse, sem traceback.
    with pytest.raises(SystemExit) as exit_info:
        parser.parse_args(["run", "--month", "2026-13", "pipeline:main"])
    assert exit_info.value.code == 2
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "--engine", "spark", "--month", "2026-08", "pipeline:main"])

    # Sem a variável, --root passa a ser obrigatório.
    with pytest.raises(SystemExit):
        build_parser({}).parse_args(["run", "--month", "2026-08", "pipeline:main"])


def test_execution_log(caplog: pytest.LogCaptureFixture) -> None:
    """O resumo da execução vai para o ``logging`` padrão, com os campos formatados na hora e os tempos por passo."""
    logger = logging.getLogger("serialize_db.execution")

    with caplog.at_level(logging.INFO, logger="serialize_db.execution"):
        started = time.perf_counter()
        logger.info("execução %s aberta: mês %s, versões lidas %s", "exec-2026-09-05", "2026-08", {"cad_lancamentos": 143})
        logger.info("passo %s concluído em %.3f s", "ingest", time.perf_counter() - started)
        logger.warning("auditoria reprovada: %s", "chave duplicada em 2026-08")
        logger.info("encerrada", extra={"execution_id": "exec-2026-09-05"})

    assert [record.levelname for record in caplog.records] == ["INFO", "INFO", "WARNING", "INFO"]
    assert "versões lidas {'cad_lancamentos': 143}" in caplog.records[0].getMessage()
    assert caplog.records[-1].execution_id == "exec-2026-09-05"  # type: ignore[attr-defined]


def prepare_environment(environ: MutableMapping[str, str]) -> dict[str, str]:
    """Exporta ``NO_PROXY`` de ``no_proxy`` e copia a região entre ``AWS_REGION`` e ``AWS_DEFAULT_REGION``; devolve o que mudou."""
    changes: dict[str, str] = {}

    # Ausente ou vazia: o delta-rs lê NO_PROXY antes de no_proxy, e vazia ela anula as exceções.
    if not environ.get("NO_PROXY") and environ.get("no_proxy"):
        environ["NO_PROXY"] = changes["NO_PROXY"] = environ["no_proxy"]

    region = environ.get("AWS_REGION") or environ.get("AWS_DEFAULT_REGION")
    if region:
        for name in ("AWS_REGION", "AWS_DEFAULT_REGION"):
            if environ.get(name) != region:
                environ[name] = changes[name] = region

    return changes


def test_prepare_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """``os.environ`` é o dicionário do processo; ``monkeypatch`` o altera e devolve ao fim do teste."""
    monkeypatch.delenv("NO_PROXY", raising=False)
    monkeypatch.delenv("AWS_DEFAULT_REGION", raising=False)
    monkeypatch.setenv("no_proxy", "169.254.169.254,localhost")
    monkeypatch.setenv("AWS_REGION", "us-west-2")

    changes = prepare_environment(os.environ)
    assert changes == {"NO_PROXY": "169.254.169.254,localhost", "AWS_DEFAULT_REGION": "us-west-2"}
    assert os.environ["NO_PROXY"] == "169.254.169.254,localhost" and os.environ["AWS_DEFAULT_REGION"] == "us-west-2"

    assert prepare_environment(os.environ) == {}  # a segunda chamada não muda nada

    # NO_PROXY vazia (o shell da extensão do Claude Code no espaço a deixa assim) é substituída como a ausente.
    monkeypatch.setenv("NO_PROXY", "")
    assert prepare_environment(os.environ) == {"NO_PROXY": "169.254.169.254,localhost"}


def test_json_control_file_and_commit_metadata() -> None:
    """``json`` grava ``snapshots.json`` e o texto de ``serialize_db_input_versions``; ``Decimal`` precisa de ``default``."""
    control = {"snapshots": {"2026T3": {"cad_lancamentos": 143, "cad_contratos": 88}}}
    text = json.dumps(control, indent=2, sort_keys=True, ensure_ascii=False)
    assert json.loads(text) == control
    assert text.splitlines()[0] == "{"

    # Os metadados de commit são texto: o dicionário das versões lidas vira uma string JSON dentro do JSON.
    metadata = {
        "serialize_db_execution_id": "exec-2026-09-05",
        "serialize_db_input_versions": json.dumps({"cad_lancamentos": 143}, sort_keys=True),
    }
    assert json.loads(metadata["serialize_db_input_versions"]) == {"cad_lancamentos": 143}

    with pytest.raises(TypeError):
        json.dumps({"valor": decimal.Decimal("1.50")})
    assert json.loads(json.dumps({"valor": decimal.Decimal("1.50")}, default=str))["valor"] == "1.50"

    stamp = dt.datetime(2026, 9, 5, 12, 0, tzinfo=dt.timezone.utc)
    assert dt.datetime.fromisoformat(json.loads(json.dumps(stamp.isoformat()))) == stamp


def test_storage_uris() -> None:
    """``urllib.parse`` separa bucket e prefixo; ``PurePosixPath`` junta chaves; ``Path`` cuida da pasta local."""
    parts = urllib.parse.urlparse("s3://awsds-sandbox/dzd/projeto/dev/prod/cad_lancamentos")
    assert parts.scheme == "s3" and parts.netloc == "awsds-sandbox"
    assert parts.path.lstrip("/") == "dzd/projeto/dev/prod/cad_lancamentos"

    # As chaves do S3 usam / independentemente do sistema; PurePosixPath as compõe sem tocar o disco.
    key = PurePosixPath("dzd/projeto/dev") / "prod" / "cad_lancamentos" / "_delta_log"
    assert str(key) == "dzd/projeto/dev/prod/cad_lancamentos/_delta_log"
    assert key.relative_to("dzd/projeto/dev").parts == ("prod", "cad_lancamentos", "_delta_log")

    # A pasta local: caminho absoluto ou file://, os dois aceitos pelo delta-rs.
    local = Path("/dados/prod/cad_lancamentos")
    assert urllib.parse.urlparse(str(local)).scheme == "" and local.is_absolute()
    assert local.as_uri() == "file:///dados/prod/cad_lancamentos"
    assert Path(urllib.parse.urlparse("file:///dados/prod").path) == Path("/dados/prod")


def test_group_log_actions_by_month() -> None:
    """``itertools.groupby`` e ``collections`` agrupam as ações ``add`` por mês; a diferença de conjuntos dá ``version_diff``."""
    actions = [("mes=2026-01/a.parquet", 100, "2026-01"), ("mes=2026-02/b.parquet", 300, "2026-02"), ("mes=2026-01/c.parquet", 50, "2026-01")]

    # groupby exige a entrada ordenada pela chave.
    by_month = {month: [path for path, _, _ in group] for month, group in itertools.groupby(sorted(actions, key=lambda a: a[2]), key=lambda a: a[2])}
    assert by_month == {"2026-01": ["mes=2026-01/a.parquet", "mes=2026-01/c.parquet"], "2026-02": ["mes=2026-02/b.parquet"]}

    bytes_by_month: collections.Counter[str] = collections.Counter()
    for _, size, month in actions:
        bytes_by_month[month] += size
    assert bytes_by_month == {"2026-01": 150, "2026-02": 300}

    files: collections.defaultdict[str, list[str]] = collections.defaultdict(list)
    for path, _, month in actions:
        files[month].append(path)
    assert len(files["2026-03"]) == 0  # um mês sem arquivos existe com lista vazia

    # version_diff: os meses dos arquivos que a versão nova tem e a publicada não tinha.
    published = {"mes=2026-01/a.parquet", "mes=2026-02/b.parquet"}
    current = {"mes=2026-01/a.parquet", "mes=2026-02/d.parquet"}
    assert {path.split("/")[0].removeprefix("mes=") for path in current - published} == {"2026-02"}


def test_decimal_totals() -> None:
    """``Decimal`` soma sem o erro binário do ``float``; ``quantize`` fixa duas casas com o arredondamento escolhido."""
    assert sum([decimal.Decimal("0.10")] * 3, decimal.Decimal(0)) == decimal.Decimal("0.30")
    assert sum([0.1] * 3) != 0.3
    assert decimal.Decimal(0.1) != decimal.Decimal("0.1")  # o float carrega o erro para dentro do Decimal

    cents = decimal.Decimal("0.01")
    assert decimal.Decimal("2.665").quantize(cents) == decimal.Decimal("2.66")  # ROUND_HALF_EVEN, o padrão
    assert decimal.Decimal("2.665").quantize(cents, rounding=decimal.ROUND_HALF_UP) == decimal.Decimal("2.67")

    # O maior valor de DECIMAL(18, 2) tem 18 dígitos; um a mais não cabe no contrato.
    limit = decimal.Decimal("9999999999999999.99")
    assert len(limit.as_tuple().digits) == 18
    assert len((limit + cents).as_tuple().digits) == 19


def test_generated_files_diff() -> None:
    """``difflib`` mostra a linha que mudou entre o arquivo versionado e o regenerado; iguais dão diff vazio."""
    versioned = "CREATE TABLE cad_operacoes (\n  id_operacao BIGINT NOT NULL,\n  valor NUMERIC(18, 2) NOT NULL\n)\n"
    regenerated = versioned.replace("  valor NUMERIC(18, 2) NOT NULL\n", "  valor NUMERIC(18, 2) NOT NULL,\n  canal VARCHAR(20)\n")

    diff = list(
        difflib.unified_diff(
            versioned.splitlines(keepends=True), regenerated.splitlines(keepends=True), fromfile="schema/cad_operacoes.duckdb.sql", tofile="gerado"
        )
    )
    assert diff[0].startswith("--- schema/cad_operacoes.duckdb.sql")
    assert any(line.startswith("+  canal VARCHAR(20)") for line in diff)

    assert list(difflib.unified_diff(versioned.splitlines(), versioned.splitlines())) == []


@pytest.mark.local
def test_exclusive_create_atomic_replace_and_fingerprint(local_location: LocalLocation) -> None:
    """Em disco, ``O_EXCL`` cria só se não existe, ``os.replace`` troca de uma vez, e um hash faz as vezes do ETag."""
    folder = Path(local_location.child("stdlib"))
    folder.mkdir(exist_ok=True)
    control = folder / "snapshots.json"

    # A criação exclusiva é a primitiva do commit em disco do delta-rs e do IfNoneMatch no S3.
    descriptor = os.open(control, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    os.write(descriptor, b'{"snapshots": {}}')
    os.close(descriptor)
    with pytest.raises(FileExistsError):
        os.open(control, os.O_WRONLY | os.O_CREAT | os.O_EXCL)

    # A impressão digital do conteúdo atual, conferida antes de substituir: o IfMatch da pasta local.
    def fingerprint(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def replace_if_match(path: Path, text: str, expected: str) -> None:
        if fingerprint(path) != expected:
            raise RuntimeError("o arquivo mudou desde a leitura")
        with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False, encoding="utf-8") as handle:
            handle.write(text)
            temporary = Path(handle.name)
        os.replace(temporary, path)  # atômico na mesma pasta

    before = fingerprint(control)
    replace_if_match(control, '{"snapshots": {"2026T3": {"cad_lancamentos": 143}}}', before)
    with pytest.raises(RuntimeError):
        replace_if_match(control, "{}", before)  # a impressão digital antiga não vale mais
    assert json.loads(control.read_text(encoding="utf-8"))["snapshots"]["2026T3"]["cad_lancamentos"] == 143

    # copy2 preserva tamanho e data de modificação; rglob lista; rmtree apaga a pasta inteira.
    copy = folder / "copia" / "snapshots.json"
    copy.parent.mkdir()
    shutil.copy2(control, copy)
    assert copy.stat().st_size == control.stat().st_size and int(copy.stat().st_mtime) == int(control.stat().st_mtime)
    assert sorted(path.name for path in folder.rglob("*.json")) == ["snapshots.json", "snapshots.json"]
    shutil.rmtree(folder / "copia")
    assert not copy.exists()

    # A pasta do sandbox: criada e apagada pelo gerenciador de contexto.
    with tempfile.TemporaryDirectory(dir=folder) as temporary:
        database = Path(temporary) / "exec.duckdb"
        database.write_bytes(b"")
        assert database.exists()
    assert not Path(temporary).exists()
