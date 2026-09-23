"""A linha de comando ``serialize-db``.

Cada subcomando entra com a etapa que entrega a primitiva por trás dele: ``schema`` é o da
etapa 1, ``sql`` o da etapa 2, e ``run`` e ``audit`` os da etapa 6. ``schema write`` grava os
arquivos de esquema dos modelos e ``schema check`` compara os versionados com a geração nova, sem
gravar; ``sql write`` grava o texto SQL de cada statement do pipeline em cada motor e ``sql check``
o compara com a geração nova. ``run`` abre uma execução e entrega a ``modulo:funcao`` do pipeline;
``audit`` imprime o texto das verificações de uma tabela (``--sql``) ou roda a auditoria sobre a
versão publicada. Os modelos chegam por ``--metadata modulo:atributo``, o caminho importável do
``MetaData`` do cliente, e os statements por ``--statements modulo:atributo``, o caminho importável
do dicionário ``{nome: statement}`` do pipeline. ``--root``, ``--environment``, ``--engine`` e
``--export-mode`` têm por padrão ``SERIALIZE_DB_ROOT``, ``SERIALIZE_DB_ENVIRONMENT`` (``dev``),
``SERIALIZE_DB_ENGINE`` (``duckdb``) e ``SERIALIZE_DB_EXPORT_MODE`` (``register``).

Exemplo:

.. code-block:: shell

    serialize-db schema write --metadata pipeline.models:Base.metadata schema/
    serialize-db schema check --metadata pipeline.models:Base.metadata schema/
    serialize-db sql write --metadata pipeline.models:Base.metadata --statements pipeline.queries:STATEMENTS sql/
    serialize-db sql check --metadata pipeline.models:Base.metadata --statements pipeline.queries:STATEMENTS sql/
    serialize-db run --root s3://bucket/delta --environment prod --partition 2026-08-31 \\
        --metadata pipeline.models:Base.metadata pipeline.mensal:main
    serialize-db audit --metadata pipeline.models:Base.metadata --table cad_lancamentos --sql

O código de saída é 0 quando o comando termina; 1 quando ``check`` encontra diferença, com o diff
impresso, e quando a auditoria reprova; 2 no erro de uso e no conflito de execução.
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys
import uuid
from collections.abc import Callable

import sqlalchemy as sa

from serialize_db import audit, delta, schema, sql
from serialize_db.audit import AuditReport
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.errors import AuditFailed, ContractError, ExecutionConflict
from serialize_db.execution import Database, Execution

__all__ = ["main"]


def _resolve_attribute(spec: str) -> object:
    """O objeto de ``modulo:atributo``: importa o módulo e segue os atributos por ponto.

    Exemplo:

    .. code-block:: python

        _resolve_attribute("pipeline.models:Base.metadata")
    """
    module_name, _, attribute_path = spec.partition(":")
    if not module_name or not attribute_path:
        raise argparse.ArgumentTypeError(f"esperado modulo:atributo, recebido {spec!r}")
    try:
        target = importlib.import_module(module_name)
        for name in attribute_path.split("."):
            target = getattr(target, name)
    except (ImportError, AttributeError) as error:
        raise argparse.ArgumentTypeError(f"{spec}: {error}") from None
    return target


def _resolve_metadata(spec: str) -> sa.MetaData:
    """O ``MetaData`` de ``modulo:atributo``, para ``--metadata``."""
    target = _resolve_attribute(spec)
    if not isinstance(target, sa.MetaData):
        raise argparse.ArgumentTypeError(f"{spec} não é um MetaData: {type(target).__name__}")
    return target


def _resolve_statements(spec: str) -> dict[str, sa.sql.ClauseElement]:
    """O dicionário ``{nome: statement}`` de ``modulo:atributo``, para ``--statements``."""
    target = _resolve_attribute(spec)
    if not isinstance(target, dict):
        raise argparse.ArgumentTypeError(f"{spec} não é um dicionário: {type(target).__name__}")
    for name, statement in target.items():
        if not isinstance(statement, sa.sql.ClauseElement):
            raise argparse.ArgumentTypeError(
                f"{spec}[{name!r}] não é um statement: {type(statement).__name__}")
    return target


def _resolve_function(spec: str) -> Callable[..., object]:
    """A função de ``modulo:funcao``, o pipeline que ``run`` chama com a execução aberta."""
    target = _resolve_attribute(spec)
    if not callable(target):
        raise argparse.ArgumentTypeError(f"{spec} não é uma função: {type(target).__name__}")
    return target


def _name_argument(text: str) -> str:
    """Um valor de partição, um ``execution_id`` ou um ambiente, pela regra da partição."""
    try:
        return schema.check_partition_value(text)
    except ContractError as error:
        raise argparse.ArgumentTypeError(str(error)) from None


def _add_run_parser(commands: argparse._SubParsersAction) -> None:
    """``serialize-db run``, com as variáveis ``SERIALIZE_DB_*`` como padrão."""
    root = os.environ.get("SERIALIZE_DB_ROOT")
    run = commands.add_parser("run", help="executa o pipeline de uma partição")
    run.add_argument("--root", default=root, required=not root,
                     help="a raiz do banco, pasta local ou s3://bucket/prefixo")
    run.add_argument("--environment", type=_name_argument,
                     default=os.environ.get("SERIALIZE_DB_ENVIRONMENT") or "dev")
    run.add_argument("--engine", choices=["duckdb", "redshift"],
                     default=os.environ.get("SERIALIZE_DB_ENGINE") or "duckdb")
    run.add_argument("--export-mode", choices=["register", "rewrite"], default=None,
                     help="o modo dos publish que não informam o seu; padrão SERIALIZE_DB_EXPORT_MODE")
    run.add_argument("--partition", type=_name_argument, required=True)
    run.add_argument("--execution-id", type=_name_argument, default=None)
    run.add_argument("--metadata", required=True, type=_resolve_metadata,
                     help="modulo:atributo com o MetaData dos modelos do pipeline")
    run.add_argument("pipeline", type=_resolve_function,
                     help="modulo:funcao que recebe a execução aberta")


def _add_audit_parser(commands: argparse._SubParsersAction) -> None:
    """``serialize-db audit``: o texto das verificações ou a auditoria da versão publicada."""
    audit_command = commands.add_parser("audit", help="a auditoria de uma tabela")
    audit_command.add_argument("--metadata", required=True, type=_resolve_metadata,
                               help="modulo:atributo com o MetaData dos modelos")
    audit_command.add_argument("--table", required=True)
    audit_command.add_argument("--partitions", nargs="+", type=_name_argument, default=None)
    audit_command.add_argument("--foreign-keys", action="store_true")
    audit_command.add_argument("--key-scope", choices=["partition", "table"], default=None)
    audit_command.add_argument("--engine", choices=["duckdb", "redshift"],
                               default=os.environ.get("SERIALIZE_DB_ENGINE") or "duckdb",
                               help="o dialeto do texto e o motor da auditoria")
    audit_command.add_argument("--sql", action="store_true",
                               help="imprime o texto das verificações, sem conexão nem armazenamento")
    audit_command.add_argument("--root", default=os.environ.get("SERIALIZE_DB_ROOT"))
    audit_command.add_argument("--environment", type=_name_argument,
                               default=os.environ.get("SERIALIZE_DB_ENVIRONMENT") or "dev")


def _build_parser() -> argparse.ArgumentParser:
    """O parser da linha de comando, com um subcomando por primitiva."""
    parser = argparse.ArgumentParser(prog="serialize-db")
    commands = parser.add_subparsers(dest="command", required=True)
    _add_run_parser(commands)
    _add_audit_parser(commands)

    schema_command = commands.add_parser("schema", help="os arquivos de esquema dos modelos")
    schema_actions = schema_command.add_subparsers(dest="action", required=True)
    for action, help_text in (("write", "grava os arquivos"), ("check", "compara sem gravar")):
        action_parser = schema_actions.add_parser(action, help=help_text)
        action_parser.add_argument("--metadata", required=True, type=_resolve_metadata,
                                   help="modulo:atributo com o MetaData dos modelos")
        action_parser.add_argument("directory", help="a pasta dos arquivos de esquema")

    sql_command = commands.add_parser("sql", help="o texto SQL dos statements em cada motor")
    sql_actions = sql_command.add_subparsers(dest="action", required=True)
    for action, help_text in (("write", "grava os arquivos"), ("check", "compara sem gravar")):
        action_parser = sql_actions.add_parser(action, help=help_text)
        action_parser.add_argument("--metadata", required=True, type=_resolve_metadata,
                                   help="modulo:atributo com o MetaData dos modelos")
        action_parser.add_argument("--statements", required=True, type=_resolve_statements,
                                   help="modulo:atributo com o dicionário {nome: statement}")
        action_parser.add_argument("directory", help="a pasta dos arquivos de texto SQL")
    return parser


def _print_diff(diff: list[str], directory: str, kind: str) -> int:
    """Imprime o diff dos arquivos versionados contra a geração nova; 1 quando há diferença."""
    for line in diff:
        print(line)
    if diff:
        return 1
    print(f"{directory}: arquivos de {kind} atualizados")
    return 0


def _schema_write(args: argparse.Namespace) -> int:
    """Grava os arquivos de esquema e imprime os caminhos."""
    for path in schema.write_schema_files(args.metadata, args.directory):
        print(path)
    return 0


def _schema_check(args: argparse.Namespace) -> int:
    """Compara os arquivos de esquema versionados com a geração nova."""
    diff = schema.check_schema_files(args.metadata, args.directory)
    return _print_diff(diff, args.directory, "esquema")


def _sql_write(args: argparse.Namespace) -> int:
    """Grava os arquivos de texto SQL e imprime os caminhos."""
    for path in sql.write_sql_files(args.statements, args.metadata, args.directory):
        print(path)
    return 0


def _sql_check(args: argparse.Namespace) -> int:
    """Compara os arquivos de texto SQL versionados com a geração nova."""
    diff = sql.check_sql_files(args.statements, args.metadata, args.directory)
    return _print_diff(diff, args.directory, "texto SQL")


def _run(args: argparse.Namespace) -> int:
    """Abre a execução, entrega-a ao pipeline e devolve o código pelo resultado: 1 na auditoria
    reprovada, 2 no conflito e no motor que não serve."""
    try:
        execution = Execution(Database(args.root, args.environment, args.metadata), args.engine,
                              args.partition, args.execution_id, args.export_mode)
    except ContractError as error:
        print(f"serialize-db run: {error}", file=sys.stderr)
        return 2
    try:
        with execution as run:
            args.pipeline(run)
    except AuditFailed as error:
        print(f"serialize-db run: auditoria reprovada: {error}", file=sys.stderr)
        return 1
    except ExecutionConflict as error:
        print(f"serialize-db run: conflito: {error}", file=sys.stderr)
        return 2
    return 0


def _print_report(report: AuditReport) -> None:
    """O relatório da auditoria: o veredito de cada verificação, as amostras e as leituras."""
    for result in report.results:
        verdict = "aprovada" if result.passed else f"reprovada, {result.defects} defeito(s)"
        print(f"{result.name}: {verdict}{' (' + result.reason + ')' if result.reason else ''}")
        for row in result.sample.to_pylist():
            print(f"    {row}")
    for reason in report.not_run:
        print(f"não rodou: {reason}")
    for value, totals in report.totals.items():
        print(f"partição {value}: {totals}")


def _audit_published(args: argparse.Namespace, table: sa.Table) -> int:
    """A auditoria da versão publicada, num sandbox DuckDB próprio."""
    db = Database(args.root, args.environment, args.metadata)
    uri = db.uri(table)
    if not delta.table_exists(uri, db.storage):
        print(f"serialize-db audit: {table.name} não existe em {uri}", file=sys.stderr)
        return 2
    version = delta.open_table(uri, db.storage).version()
    referenced = {}
    for constraint in table.foreign_key_constraints:
        target = db.uri(constraint.referred_table)
        if delta.table_exists(target, db.storage):
            referenced[constraint.referred_table.name] = (target, delta.open_table(target, db.storage).version())
    with DuckDBEngine(DuckDBConfig(), f"auditoria-{uuid.uuid4().hex[:8]}", db.storage) as engine:
        engine.ingest(table, uri, version, args.partitions, materialize=True)
        report = engine.audit(table, args.partitions, uri, version, args.foreign_keys,
                              args.key_scope, referenced)
    print(f"{table.name} na versão {version}:")
    _print_report(report)
    return 0 if report.passed else 1


def _audit(args: argparse.Namespace) -> int:
    """``--sql`` imprime o texto das verificações; sem ele, a auditoria da versão publicada."""
    table = args.metadata.tables.get(args.table)
    if table is None:
        print(f"serialize-db audit: a tabela {args.table} não está nos modelos", file=sys.stderr)
        return 2
    if args.sql:
        texts = audit.audit_sql(table, args.engine, args.partitions, args.foreign_keys,
                                args.key_scope)
        for name, text in texts.items():
            print(f"-- {name}\n{text}\n")
        return 0
    if not args.root:
        print("serialize-db audit: sem --sql, informe --root ou SERIALIZE_DB_ROOT", file=sys.stderr)
        return 2
    if args.engine != "duckdb":
        print("serialize-db audit: o motor redshift é a etapa 5, ainda não implementada",
              file=sys.stderr)
        return 2
    return _audit_published(args, table)


# A função de cada par (comando, ação); cada etapa acrescenta o seu par aqui.
_HANDLERS = {
    ("schema", "write"): _schema_write,
    ("schema", "check"): _schema_check,
    ("sql", "write"): _sql_write,
    ("sql", "check"): _sql_check,
    ("run", None): _run,
    ("audit", None): _audit,
}


def main(argv: list[str] | None = None) -> int:
    """Executa a linha de comando e devolve o código de saída."""
    args = _build_parser().parse_args(argv)
    if args.command in ("run", "audit"):
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    return _HANDLERS[(args.command, getattr(args, "action", None))](args)


if __name__ == "__main__":
    sys.exit(main())
