"""A linha de comando ``serialize-db``.

Cada subcomando entra com a etapa que entrega a primitiva por trás dele: ``schema`` é o da
etapa 1 e ``sql`` o da etapa 2. ``schema write`` grava os arquivos de esquema dos modelos e
``schema check`` compara os versionados com a geração nova, sem gravar; ``sql write`` grava o texto
SQL de cada statement do pipeline em cada motor e ``sql check`` o compara com a geração nova. Os
modelos chegam por ``--metadata modulo:atributo``, o caminho importável do ``MetaData`` do
cliente, e os statements por ``--statements modulo:atributo``, o caminho importável do dicionário
``{nome: statement}`` do pipeline.

Exemplo:

    serialize-db schema write --metadata pipeline.models:Base.metadata schema/
    serialize-db schema check --metadata pipeline.models:Base.metadata schema/
    serialize-db sql write --metadata pipeline.models:Base.metadata --statements pipeline.queries:STATEMENTS sql/
    serialize-db sql check --metadata pipeline.models:Base.metadata --statements pipeline.queries:STATEMENTS sql/

O código de saída é 0 quando o comando termina; 1 quando ``check`` encontra diferença, com o diff
impresso; 2 no erro de uso.
"""

from __future__ import annotations

import argparse
import importlib
import sys

import sqlalchemy as sa

from serialize_db import schema, sql

__all__ = ["main"]


def _resolve_attribute(spec: str) -> object:
    """O objeto de ``modulo:atributo``: importa o módulo e segue os atributos por ponto.

    Exemplo:

        _resolve_attribute("pipeline.models:Base.metadata")
    """
    module_name, _, attribute_path = spec.partition(":")
    if not module_name or not attribute_path:
        raise argparse.ArgumentTypeError(f"esperado modulo:atributo, recebido {spec!r}")
    target = importlib.import_module(module_name)
    for name in attribute_path.split("."):
        target = getattr(target, name)
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


def _build_parser() -> argparse.ArgumentParser:
    """O parser da linha de comando, com um subcomando por primitiva e ``write`` e ``check`` em cada um."""
    parser = argparse.ArgumentParser(prog="serialize-db")
    commands = parser.add_subparsers(dest="command", required=True)

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


# A função de cada par (comando, ação); cada etapa acrescenta o seu par aqui.
_HANDLERS = {
    ("schema", "write"): _schema_write,
    ("schema", "check"): _schema_check,
    ("sql", "write"): _sql_write,
    ("sql", "check"): _sql_check,
}


def main(argv: list[str] | None = None) -> int:
    """Executa a linha de comando e devolve o código de saída."""
    args = _build_parser().parse_args(argv)
    return _HANDLERS[(args.command, args.action)](args)


if __name__ == "__main__":
    sys.exit(main())
