"""A biblioteca padrão do Python nos papéis que as etapas (``plan/PLAN-STAGE-<n>.md``) lhe dão.

Sem gravar arquivo: a ordem dos valores de partição e o valor derivado de uma data (``datetime``),
os identificadores de execução e de sandbox (``uuid``, ``re``), o protocolo e a configuração de um
motor de exemplo (``typing.Protocol``, ``dataclasses``), o atributo derivado de um dataclass
congelado (``functools.cached_property``), o gerenciador de contexto que descarta o sandbox mesmo
com a auditoria reprovada (``contextlib``), o ponto de entrada ``modulo:funcao``
(``pkgutil.resolve_name``), a linha de comando (``argparse``), o log da execução (``logging``), o
ambiente normalizado (``os.environ`` com ``monkeypatch``), o arquivo de controle e os metadados de
commit (``json``), as URIs dos dois armazenamentos (``urllib.parse``, ``pathlib``), as ações do log
agrupadas por partição (``json``, ``itertools``, ``collections``), os totais da auditoria
(``decimal``) e o diff dos arquivos gerados (``difflib``). Sob a raiz local (marcador ``local``): a
criação exclusiva, a substituição atômica e a impressão digital de um arquivo (``os``,
``tempfile``, ``hashlib``, ``shutil``).
"""

from __future__ import annotations

import argparse
import collections
import contextlib
import dataclasses
import datetime as dt
import decimal
import difflib
import functools
import hashlib
import itertools
import json
import logging
import os
import pkgutil
import re
import shutil
import tempfile
import time
import urllib.parse
import uuid
from collections.abc import Iterator, Mapping
from pathlib import Path, PurePosixPath
from typing import Protocol, runtime_checkable

import pytest

from conftest import LocalLocation
from serialize_db import schema
from serialize_db.errors import AuditFailed
from serialize_db.storage import prepare_environment


def test_partition_values_in_text_order() -> None:
    """``run.previous_partitions(table, n)`` ordena os valores de partição como texto, que em
    ``AAAA-MM-DD`` é a ordem do calendário; a auditoria deriva o valor de uma data por
    ``strftime``."""
    # Os fins de mês da base de origem, não contíguos e fora de ordem.
    values = ["2026-06-30", "2026-01-31", "2026-09-30", "2026-03-31", "2026-02-28"]

    # Em AAAA-MM-DD a ordem de texto é a do calendário; em DD-MM-AAAA não é. A biblioteca só ordena
    # texto, e o calendário é do cliente.
    assert sorted(values) == sorted(values, key=dt.date.fromisoformat)
    assert sorted(["31-01-2026", "28-02-2026"]) == ["28-02-2026", "31-01-2026"]

    # Com partition_source declarado, a auditoria confere a coluna de partição contra
    # strftime(<coluna de data>, '%Y-%m-%d'); a hora de um timestamp não entra no valor.
    assert dt.date(2026, 8, 31).strftime("%Y-%m-%d") == "2026-08-31"
    assert dt.datetime(2026, 8, 31, 23, 59).strftime("%Y-%m-%d") == "2026-08-31"


def sandbox_prefix(execution_id: str) -> str:
    """``exec_<id>_`` com o identificador reduzido a ``[a-z0-9_]``, dentro dos 127 bytes de um
    identificador do Redshift."""
    normalized = re.sub(r"[^a-z0-9_]", "_", execution_id.lower())
    prefix = f"exec_{normalized}_"

    longest_table = 63  # a maior tabela do modelo cabe depois do prefixo
    if len(prefix.encode()) + longest_table > 127:
        raise ValueError(f"identificador longo demais para o Redshift: {execution_id}")

    return prefix


def test_execution_identifiers() -> None:
    """Identificadores únicos e nomes válidos nos dois motores: ``uuid``, ``re`` e o instante em
    UTC."""
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
class ExampleEngine(Protocol):
    """Um protocolo de exemplo com dois métodos de um motor; o da biblioteca é
    ``serialize_db.engine.Engine``."""

    def ingest(
        self,
        table: str,
        uri: str,
        version: int,
        partitions: list[str] | None = None,
        materialize: bool = False,
    ) -> None: ...

    def cleanup(self) -> None: ...


@dataclasses.dataclass(frozen=True, kw_only=True)
class ExampleConfig:
    """Uma configuração de exemplo, imutável e com padrões explícitos."""

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

    def ingest(
        self,
        table: str,
        uri: str,
        version: int,
        partitions: list[str] | None = None,
        materialize: bool = False,
    ) -> None:
        self.calls.append((table, uri, version))

    def cleanup(self) -> None:
        self.cleaned = True


def test_engine_protocol_and_config_dataclass() -> None:
    """``Protocol`` verifica a interface pela forma; ``dataclass`` dá configuração imutável,
    comparável e copiável."""
    assert isinstance(FakeEngine(), ExampleEngine)

    class Incomplete:
        def ingest(self, *args: object) -> None: ...

    assert not isinstance(Incomplete(), ExampleEngine)  # sem cleanup não é um ExampleEngine

    config = ExampleConfig(threads=2, memory_limit="4GB")
    assert config.extensions == ("delta",)
    assert dataclasses.replace(config, threads=4).threads == 4
    assert config.threads == 2
    assert dataclasses.asdict(config)["memory_limit"] == "4GB"
    with pytest.raises(dataclasses.FrozenInstanceError):
        config.threads = 8  # type: ignore[misc]


def test_frozen_dataclass_derives_an_attribute_by_cached_property() -> None:
    """Um campo ``init=False`` de um dataclass congelado não aceita a atribuição do
    ``__post_init__``; ``functools.cached_property`` cria o atributo derivado no primeiro uso e
    deixa os campos congelados: é o ``storage`` de ``Database``."""

    @dataclasses.dataclass(frozen=True)
    class WithField:
        root: str
        storage: str = dataclasses.field(init=False)

        def __post_init__(self) -> None:
            self.storage = f"Storage({self.root})"

    with pytest.raises(dataclasses.FrozenInstanceError, match="storage"):
        WithField("s3://bucket/projeto")

    built: list[str] = []

    @dataclasses.dataclass(frozen=True)
    class Database:
        root: str

        @functools.cached_property
        def storage(self) -> str:
            built.append(self.root)
            return f"Storage({self.root})"

    # O atributo nasce uma vez, no primeiro uso; igualdade e hash seguem só os campos.
    db = Database("s3://bucket/projeto")
    assert db.storage is db.storage
    assert built == ["s3://bucket/projeto"]
    assert db == Database("s3://bucket/projeto")
    assert hash(db) == hash(Database("s3://bucket/projeto"))
    with pytest.raises(dataclasses.FrozenInstanceError):
        db.root = "outra"  # type: ignore[misc]


@contextlib.contextmanager
def execution(engine: FakeEngine) -> Iterator[FakeEngine]:
    """O ciclo de uma execução: abre, entrega o motor ao pipeline e descarta o sandbox aconteça o
    que acontecer."""
    # A abertura fica dentro do try: uma ingestão que falha também descarta o sandbox.
    try:
        engine.ingest("cad_lancamentos", "/dados/prod/cad_lancamentos", 143)
        yield engine
    finally:
        engine.cleanup()


def test_context_manager_cleans_up_on_failure() -> None:
    """``contextlib`` garante o ``cleanup`` na saída normal e na exceção; ``ExitStack`` empilha
    vários recursos."""
    engine = FakeEngine()
    with execution(engine) as run:
        assert run.calls == [("cad_lancamentos", "/dados/prod/cad_lancamentos", 143)]
    assert engine.cleaned

    # AuditFailed é a exceção da biblioteca para a auditoria reprovada.
    failed = FakeEngine()
    with pytest.raises(AuditFailed, match="chave duplicada"):
        with execution(failed):
            raise AuditFailed("chave duplicada em 2026-08-31")
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


def test_entry_point_by_import_string() -> None:
    """``pkgutil.resolve_name``, o que ``serialize-db`` chama, carrega a função do pipeline pelo
    nome do módulo e do atributo e segue atributos por ponto; os erros distinguem módulo e função, e
    sem ``:`` o nome inteiro é o módulo."""
    assert pkgutil.resolve_name("json:dumps") is json.dumps
    assert pkgutil.resolve_name("json:decoder.JSONDecoder") is json.decoder.JSONDecoder

    # Sem ":" o nome vale pelo módulo, sem erro; serialize-db confere o formato modulo:atributo
    # antes de chamar resolve_name.
    assert pkgutil.resolve_name("json") is json
    with pytest.raises(ModuleNotFoundError):
        pkgutil.resolve_name("pipeline_inexistente:main")
    with pytest.raises(AttributeError):
        pkgutil.resolve_name("json:inexistente")


def partition_argument(text: str) -> str:
    """Confere o valor de partição da linha de comando; ``argparse`` transforma a exceção em
    mensagem e saída 2.

    A partição segue ``schema.PARTITION_VALUE``: letra ou dígito no início, depois letras, dígitos,
    ``_``, ``.`` e ``-``, os caracteres que o nome da pasta ``<coluna>=<valor>`` guarda sem
    codificar.
    """
    if not re.fullmatch(schema.PARTITION_VALUE, text):
        message = f"partição {text!r} fora da regra {schema.PARTITION_VALUE}"
        raise argparse.ArgumentTypeError(message)

    return text


def build_parser(environ: Mapping[str, str]) -> argparse.ArgumentParser:
    """Um parser na forma de ``serialize-db run`` e ``serialize-db schema write|check``, com as
    variáveis de ambiente como padrão dos argumentos; a variável vazia conta como ausente."""
    parser = argparse.ArgumentParser(prog="serialize-db")
    commands = parser.add_subparsers(dest="command", required=True)

    run = commands.add_parser("run", help="executa o pipeline de uma partição")
    root = environ.get("SERIALIZE_DB_ROOT")
    run.add_argument("--root", default=root, required=not root)
    run.add_argument("--environment", default=environ.get("SERIALIZE_DB_ENVIRONMENT") or "dev")
    run.add_argument(
        "--engine",
        choices=["duckdb", "redshift"],
        default=environ.get("SERIALIZE_DB_ENGINE") or "duckdb",
    )
    run.add_argument(
        "--export-mode",
        choices=["register", "rewrite"],
        default=environ.get("SERIALIZE_DB_EXPORT_MODE") or "register",
    )
    run.add_argument("--partition", type=partition_argument, required=True)
    run.add_argument("--execution-id", default=None)
    run.add_argument(
        "--metadata", required=True, help="modulo:atributo com o MetaData dos modelos do pipeline"
    )
    run.add_argument("pipeline", help="modulo:funcao que recebe a execução aberta")

    # schema write e schema check: um subcomando dentro de outro, cada um com os mesmos argumentos.
    schema_command = commands.add_parser("schema", help="os arquivos de esquema dos modelos")
    actions = schema_command.add_subparsers(dest="action", required=True)
    for action in ("write", "check"):
        action_parser = actions.add_parser(action)
        action_parser.add_argument(
            "--metadata", required=True, help="modulo:atributo com o MetaData dos modelos"
        )
        action_parser.add_argument("directory", help="a pasta dos arquivos de esquema")

    return parser


def test_command_line_parsing() -> None:
    """``argparse`` com subcomandos aninhados, escolhas, validação de tipo e padrões vindos do
    ambiente."""
    parser = build_parser({"SERIALIZE_DB_ROOT": "s3://bucket/projeto/delta"})
    metadata = "pipeline.models:Base.metadata"

    run_argv = ["run", "--partition", "2026-08-31", "--metadata", metadata, "pipeline:main"]
    args = parser.parse_args(run_argv)
    assert (args.command, args.root, args.environment, args.engine, args.export_mode) == (
        "run",
        "s3://bucket/projeto/delta",
        "dev",
        "duckdb",
        "register",
    )
    assert args.partition == "2026-08-31"
    assert args.pipeline == "pipeline:main"
    assert args.execution_id is None

    check = parser.parse_args(["schema", "check", "--metadata", metadata, "esquemas"])
    assert (check.command, check.action, check.metadata, check.directory) == (
        "schema",
        "check",
        metadata,
        "esquemas",
    )

    # Erros de uso saem com código 2 e a mensagem do argparse, sem traceback: a partição com /, o
    # motor fora das escolhas e o schema sem --metadata.
    slash_partition = ["run", "--partition", "2026/08/31", "--metadata", "m:a", "pipeline:main"]
    with pytest.raises(SystemExit) as exit_info:
        parser.parse_args(slash_partition)
    assert exit_info.value.code == 2
    valid_run = ["--partition", "2026-08-31", "--metadata", "m:a", "pipeline:main"]
    with pytest.raises(SystemExit):
        parser.parse_args(["run", "--engine", "spark", *valid_run])
    with pytest.raises(SystemExit):
        parser.parse_args(["schema", "check", "esquemas"])

    # Sem a variável, --root passa a ser obrigatório; vazia, também (required=not root).
    with pytest.raises(SystemExit):
        build_parser({}).parse_args(["run", *valid_run])
    with pytest.raises(SystemExit):
        build_parser({"SERIALIZE_DB_ROOT": ""}).parse_args(["run", *valid_run])


def test_execution_log(caplog: pytest.LogCaptureFixture) -> None:
    """O resumo da execução vai para o ``logging`` padrão, com os campos formatados na hora e os
    tempos por passo."""
    logger = logging.getLogger("serialize_db.execution")

    with caplog.at_level(logging.INFO, logger="serialize_db.execution"):
        started = time.perf_counter()
        logger.info(
            "execução %s aberta: partição %s, versões lidas %s",
            "exec-2026-09-05",
            "2026-08-31",
            {"cad_lancamentos": 143},
        )
        logger.info("passo %s concluído em %.3f s", "ingest", time.perf_counter() - started)
        logger.warning("auditoria reprovada: %s", "chave duplicada em 2026-08-31")
        logger.info("encerrada", extra={"execution_id": "exec-2026-09-05"})

    assert [record.levelname for record in caplog.records] == ["INFO", "INFO", "WARNING", "INFO"]
    assert "versões lidas {'cad_lancamentos': 143}" in caplog.records[0].getMessage()
    assert caplog.records[-1].execution_id == "exec-2026-09-05"  # type: ignore[attr-defined]


def test_prepare_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    """``os.environ`` é o dicionário do processo; ``monkeypatch`` o altera e devolve ao fim do
    teste, e ``prepare_environment`` grava nele."""
    # delenv de uma variável ausente não guarda nada, e o que prepare_environment gravasse nela
    # ficaria no processo depois do teste; o setenv antes do delenv guarda o estado anterior.
    for name in ("NO_PROXY", "AWS_DEFAULT_REGION"):
        monkeypatch.setenv(name, "")
        monkeypatch.delenv(name)
    monkeypatch.setenv("no_proxy", "169.254.169.254,localhost")
    monkeypatch.setenv("AWS_REGION", "us-west-2")

    changes = prepare_environment(os.environ)
    assert changes == {"NO_PROXY": "169.254.169.254,localhost", "AWS_DEFAULT_REGION": "us-west-2"}
    assert os.environ["NO_PROXY"] == "169.254.169.254,localhost"
    assert os.environ["AWS_DEFAULT_REGION"] == "us-west-2"


def test_json_control_file_and_commit_metadata() -> None:
    """``json`` grava ``snapshots.json`` e o texto de ``serialize_db_input_versions``; ``Decimal``
    precisa de ``default``."""
    control = {"snapshots": {"2026T3": {"cad_lancamentos": 143, "cad_contratos": 88}}}
    text = json.dumps(control, indent=2, sort_keys=True, ensure_ascii=False)
    assert json.loads(text) == control
    assert text.splitlines()[0] == "{"

    # Os metadados de commit são texto: o dicionário das versões lidas vira uma string JSON dentro
    # do JSON.
    metadata = {
        "serialize_db_execution_id": "exec-2026-09-05",
        "serialize_db_input_versions": json.dumps({"cad_lancamentos": 143}, sort_keys=True),
    }
    assert json.loads(metadata["serialize_db_input_versions"]) == {"cad_lancamentos": 143}

    with pytest.raises(TypeError):
        json.dumps({"valor": decimal.Decimal("1.50")})
    serialized = json.dumps({"valor": decimal.Decimal("1.50")}, default=str)
    assert json.loads(serialized)["valor"] == "1.50"

    stamp = dt.datetime(2026, 9, 5, 12, 0, tzinfo=dt.timezone.utc)
    stamp_text = json.dumps(stamp.isoformat())
    assert dt.datetime.fromisoformat(json.loads(stamp_text)) == stamp


def test_storage_uris() -> None:
    """``urllib.parse`` separa bucket e prefixo; ``PurePosixPath`` junta chaves; ``Path`` cuida da
    pasta local."""
    parts = urllib.parse.urlparse("s3://awsds-sandbox/dzd/projeto/dev/prod/cad_lancamentos")
    assert parts.scheme == "s3"
    assert parts.netloc == "awsds-sandbox"
    assert parts.path.lstrip("/") == "dzd/projeto/dev/prod/cad_lancamentos"

    # As chaves do S3 usam / independentemente do sistema; PurePosixPath as compõe sem tocar o
    # disco.
    key = PurePosixPath("dzd/projeto/dev") / "prod" / "cad_lancamentos" / "_delta_log"
    assert str(key) == "dzd/projeto/dev/prod/cad_lancamentos/_delta_log"
    assert key.relative_to("dzd/projeto/dev").parts == ("prod", "cad_lancamentos", "_delta_log")

    # A pasta local: caminho absoluto ou file://, os dois aceitos pelo delta-rs.
    local = Path("/dados/prod/cad_lancamentos")
    assert urllib.parse.urlparse(str(local)).scheme == ""
    assert local.is_absolute()
    assert local.as_uri() == "file:///dados/prod/cad_lancamentos"
    assert Path(urllib.parse.urlparse("file:///dados/prod").path) == Path("/dados/prod")


def partition_of(file_action: dict) -> str:
    """O valor de partição de uma ação ``add`` ou ``remove`` do log."""
    return file_action["partitionValues"]["data_str"]


def test_group_log_actions_by_partition() -> None:
    """``json`` lê as ações do log, uma por linha; ``itertools.groupby`` e ``collections`` as
    agrupam por partição."""
    # Dois commits: a substituição da partição 2026-08-31 e a compactação da 2026-07-31, que troca
    # arquivos sem mudar dados e grava dataChange falso.
    lines = [
        '{"commitInfo": {"operation": "WRITE"}}',
        '{"remove": {"path": "data_str=2026-08-31/a.parquet", '
        '"partitionValues": {"data_str": "2026-08-31"}, "dataChange": true}}',
        '{"add": {"path": "data_str=2026-08-31/b.parquet", '
        '"partitionValues": {"data_str": "2026-08-31"}, "size": 300, "dataChange": true}}',
        '{"commitInfo": {"operation": "OPTIMIZE"}}',
        '{"remove": {"path": "data_str=2026-07-31/c.parquet", '
        '"partitionValues": {"data_str": "2026-07-31"}, "dataChange": false}}',
        '{"remove": {"path": "data_str=2026-07-31/d.parquet", '
        '"partitionValues": {"data_str": "2026-07-31"}, "dataChange": false}}',
        '{"add": {"path": "data_str=2026-07-31/e.parquet", '
        '"partitionValues": {"data_str": "2026-07-31"}, "size": 150, "dataChange": false}}',
        '{"add": {"path": "data_str=2026-07-31/f.parquet", '
        '"partitionValues": {"data_str": "2026-07-31"}, "size": 50, "dataChange": false}}',
    ]
    actions = [json.loads(line) for line in lines]
    adds = [action["add"] for action in actions if "add" in action]

    # groupby exige a entrada ordenada pela chave.
    files_by_partition: dict[str, list[str]] = {}
    for value, group in itertools.groupby(sorted(adds, key=partition_of), key=partition_of):
        files_by_partition[value] = [add["path"] for add in group]
    assert files_by_partition == {
        "2026-07-31": ["data_str=2026-07-31/e.parquet", "data_str=2026-07-31/f.parquet"],
        "2026-08-31": ["data_str=2026-08-31/b.parquet"],
    }

    bytes_by_partition: collections.Counter[str] = collections.Counter()
    for add in adds:
        bytes_by_partition[partition_of(add)] += add["size"]
    assert bytes_by_partition == {"2026-07-31": 200, "2026-08-31": 300}

    removed: collections.defaultdict[str, list[str]] = collections.defaultdict(list)
    for action in actions:
        if "remove" in action:
            removed[partition_of(action["remove"])].append(action["remove"]["path"])
    assert len(removed["2026-06-30"]) == 0  # uma partição sem arquivos existe com lista vazia


def test_decimal_totals() -> None:
    """``Decimal`` soma sem o erro binário do ``float``; ``quantize`` fixa duas casas com o
    arredondamento escolhido."""
    assert sum([decimal.Decimal("0.10")] * 3, decimal.Decimal(0)) == decimal.Decimal("0.30")
    assert sum([0.1] * 3) != 0.3
    # O float carrega o erro para dentro do Decimal.
    assert decimal.Decimal(0.1) != decimal.Decimal("0.1")

    cents = decimal.Decimal("0.01")
    # ROUND_HALF_EVEN, o padrão.
    assert decimal.Decimal("2.665").quantize(cents) == decimal.Decimal("2.66")
    half_up = decimal.Decimal("2.665").quantize(cents, rounding=decimal.ROUND_HALF_UP)
    assert half_up == decimal.Decimal("2.67")

    # O maior valor de DECIMAL(18, 2) tem 18 dígitos; um a mais não cabe no contrato.
    limit = decimal.Decimal("9999999999999999.99")
    assert len(limit.as_tuple().digits) == 18
    assert len((limit + cents).as_tuple().digits) == 19


def test_generated_files_diff() -> None:
    """``difflib`` mostra a linha que mudou entre o arquivo versionado e o regenerado; iguais dão
    diff vazio."""
    versioned = (
        "CREATE TABLE cad_operacoes (\n"
        "  id_operacao BIGINT NOT NULL,\n"
        "  valor NUMERIC(18, 2) NOT NULL\n"
        ")\n"
    )
    regenerated = versioned.replace(
        "  valor NUMERIC(18, 2) NOT NULL\n",
        "  valor NUMERIC(18, 2) NOT NULL,\n  canal VARCHAR(20)\n",
    )

    diff = list(
        difflib.unified_diff(
            versioned.splitlines(keepends=True),
            regenerated.splitlines(keepends=True),
            fromfile="schema/cad_operacoes.duckdb.sql",
            tofile="gerado",
        )
    )
    assert diff[0].startswith("--- schema/cad_operacoes.duckdb.sql")
    assert any(line.startswith("+  canal VARCHAR(20)") for line in diff)

    assert list(difflib.unified_diff(versioned.splitlines(), versioned.splitlines())) == []


@pytest.mark.local
def test_exclusive_create_atomic_replace_and_fingerprint(local_location: LocalLocation) -> None:
    """Em disco, ``O_EXCL`` cria só se não existe, ``os.replace`` troca de uma vez, e um hash faz as
    vezes do ETag."""
    folder = Path(local_location.child("stdlib"))
    folder.mkdir(exist_ok=True)
    control = folder / "snapshots.json"

    # A criação exclusiva é a primitiva do commit em disco do delta-rs e do IfNoneMatch no S3.
    descriptor = os.open(control, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
    os.write(descriptor, b'{"snapshots": {}}')
    os.close(descriptor)
    with pytest.raises(FileExistsError):
        os.open(control, os.O_WRONLY | os.O_CREAT | os.O_EXCL)

    # A impressão digital do conteúdo atual, conferida antes de substituir: o IfMatch da pasta
    # local.
    def fingerprint(path: Path) -> str:
        return hashlib.sha256(path.read_bytes()).hexdigest()

    def replace_if_match(path: Path, text: str, expected: str) -> None:
        if fingerprint(path) != expected:
            raise RuntimeError("o arquivo mudou desde a leitura")
        with tempfile.NamedTemporaryFile(
            "w", dir=path.parent, delete=False, encoding="utf-8"
        ) as handle:
            handle.write(text)
            temporary = Path(handle.name)
        os.replace(temporary, path)  # atômico na mesma pasta

    before = fingerprint(control)
    replace_if_match(control, '{"snapshots": {"2026T3": {"cad_lancamentos": 143}}}', before)
    with pytest.raises(RuntimeError):
        replace_if_match(control, "{}", before)  # a impressão digital antiga não vale mais
    snapshots = json.loads(control.read_text(encoding="utf-8"))["snapshots"]
    assert snapshots["2026T3"]["cad_lancamentos"] == 143

    # copy2 preserva tamanho e data de modificação; rglob lista; rmtree apaga a pasta inteira.
    copy = folder / "copia" / "snapshots.json"
    copy.parent.mkdir()
    shutil.copy2(control, copy)
    assert copy.stat().st_size == control.stat().st_size
    assert int(copy.stat().st_mtime) == int(control.stat().st_mtime)
    json_files = sorted(path.name for path in folder.rglob("*.json"))
    assert json_files == ["snapshots.json", "snapshots.json"]
    shutil.rmtree(folder / "copia")
    assert not copy.exists()

    # A pasta do sandbox: criada e apagada pelo gerenciador de contexto.
    with tempfile.TemporaryDirectory(dir=folder) as temporary:
        database = Path(temporary) / "exec.duckdb"
        database.write_bytes(b"")
        assert database.exists()
    assert not Path(temporary).exists()
