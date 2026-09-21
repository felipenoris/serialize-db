"""A linha de comando ``serialize-db``.

Cada subcomando entra com a etapa que entrega a primitiva por trás dele; ``schema`` é o da
etapa 1: ``schema write`` grava os arquivos de esquema dos modelos e ``schema check`` compara os
versionados com a geração nova, sem gravar. Os modelos chegam por ``--metadata modulo:atributo``,
o caminho importável do ``MetaData`` do cliente.

Exemplo:

    serialize-db schema write --metadata pipeline.models:Base.metadata schema/
    serialize-db schema check --metadata pipeline.models:Base.metadata schema/

O código de saída é 0 quando o comando termina; 1 quando ``schema check`` encontra diferença, com
o diff impresso; 2 no erro de uso.
"""

from __future__ import annotations

import argparse
import importlib
import sys

import sqlalchemy as sa

from serialize_db import schema

__all__ = ["main"]


def _resolve_metadata(spec: str) -> sa.MetaData:
    """O ``MetaData`` de ``modulo:atributo``: importa o módulo e segue os atributos por ponto.

    Exemplo:

        _resolve_metadata("pipeline.models:Base.metadata")
    """
    module_name, _, attribute_path = spec.partition(":")
    if not module_name or not attribute_path:
        raise argparse.ArgumentTypeError(f"esperado modulo:atributo, recebido {spec!r}")
    target = importlib.import_module(module_name)
    for name in attribute_path.split("."):
        target = getattr(target, name)
    if not isinstance(target, sa.MetaData):
        raise argparse.ArgumentTypeError(f"{spec} não é um MetaData: {type(target).__name__}")
    return target


def _build_parser() -> argparse.ArgumentParser:
    """O parser da linha de comando, com um subcomando por primitiva."""
    parser = argparse.ArgumentParser(prog="serialize-db")
    commands = parser.add_subparsers(dest="command", required=True)

    schema_command = commands.add_parser("schema", help="os arquivos de esquema dos modelos")
    actions = schema_command.add_subparsers(dest="action", required=True)
    for action, help_text in (("write", "grava os arquivos"), ("check", "compara sem gravar")):
        action_parser = actions.add_parser(action, help=help_text)
        action_parser.add_argument("--metadata", required=True, type=_resolve_metadata,
                                   help="modulo:atributo com o MetaData dos modelos")
        action_parser.add_argument("directory", help="a pasta dos arquivos de esquema")
    return parser


def _schema_write(args: argparse.Namespace) -> int:
    """Grava os arquivos de esquema e imprime os caminhos."""
    for path in schema.write_schema_files(args.metadata, args.directory):
        print(path)
    return 0


def _schema_check(args: argparse.Namespace) -> int:
    """Imprime o diff dos arquivos versionados contra a geração nova; 1 quando há diferença."""
    diff = schema.check_schema_files(args.metadata, args.directory)
    for line in diff:
        print(line)
    if diff:
        return 1
    print(f"{args.directory}: arquivos de esquema atualizados")
    return 0


def main(argv: list[str] | None = None) -> int:
    """Executa a linha de comando e devolve o código de saída."""
    args = _build_parser().parse_args(argv)
    if args.command == "schema" and args.action == "write":
        return _schema_write(args)
    return _schema_check(args)


if __name__ == "__main__":
    sys.exit(main())
