"""A linha de comando ``serialize-db``.

Cada subcomando entra com a etapa que entrega a primitiva por trás dele: ``schema`` é o da etapa 1,
``sql`` o da etapa 2, ``run`` e ``audit`` os da etapa 6, ``load`` o da etapa 7, ``publish`` o da
etapa 8, ``snapshot``, ``vacuum``, ``compact``, ``archive``, ``export`` e ``history`` os da etapa
9 e ``channel`` o da etapa 10, com o runbook abaixo, seguido das opções de cada subcomando.
``schema write`` grava os arquivos
de esquema dos modelos e ``schema check`` compara os versionados com a geração nova, sem gravar;
``sql write`` grava o texto SQL de cada statement do pipeline em cada motor e ``sql check`` o
compara com a geração nova. ``run`` abre uma execução e entrega a ``modulo:funcao`` do pipeline;
``audit`` imprime o texto das verificações de uma tabela (``--sql``) ou roda a auditoria sobre a
versão publicada, no motor de ``--engine``;
``load`` faz a carga inicial da base Parquet de origem (``--source``) nas tabelas Delta do
ambiente, as sem partição antes das particionadas, e confere contagem e somas por partição;
``publish`` publica no Redshift o snapshot de ``--snapshot`` ou do canal de ``--channel``
(``current`` é a versão atual de cada tabela), mostra o estado da publicação (``--status``), cria
a tabela de controle (``--init``) ou despublica (``--unpublish``). ``snapshot`` grava a versão
atual de cada tabela do ambiente no arquivo de controle; ``channel`` aponta um canal do ambiente
para um snapshot, o ``default`` que o leitor Delta lê sem argumento, ou lista os canais;
``vacuum`` lista, ou apaga com
``--apply``, os arquivos fora da retenção e das versões dos snapshots; ``compact`` junta os
arquivos pequenos das partições de uma tabela; ``archive`` copia as tabelas de um snapshot para
``arquivo/<nome>/`` e move a entrada para ``archived``; ``export`` grava as pastas Parquet de uma
versão de uma tabela, sem o log; ``history`` lista os commits de uma tabela com os metadados da
biblioteca. Os modelos chegam por ``--metadata modulo:atributo``, o caminho importável do
``MetaData`` do cliente, e os statements por ``--statements modulo:atributo``, o caminho importável
do dicionário ``{nome: statement}`` do pipeline. ``--root``, ``--environment`` e ``--engine`` têm
por padrão ``SERIALIZE_DB_ROOT``, ``SERIALIZE_DB_ENVIRONMENT`` (``dsv``) e ``SERIALIZE_DB_ENGINE``
(``duckdb``); a configuração do Redshift vem das variáveis ``SERIALIZE_DB_REDSHIFT_*``.

Exemplo:

.. code-block:: shell

    serialize-db schema write --metadata pipeline.models:Base.metadata schema/
    serialize-db schema check --metadata pipeline.models:Base.metadata schema/
    serialize-db sql write --metadata pipeline.models:Base.metadata \\
        --statements pipeline.queries:STATEMENTS sql/
    serialize-db sql check --metadata pipeline.models:Base.metadata \\
        --statements pipeline.queries:STATEMENTS sql/
    serialize-db run --root s3://bucket/delta --environment prd --partition 2026-08-31 \\
        --metadata pipeline.models:Base.metadata pipeline.mensal:main
    serialize-db audit --metadata pipeline.models:Base.metadata --table cad_lancamentos --sql
    serialize-db load --root s3://bucket/delta --environment prd \\
        --metadata pipeline.models:Base.metadata --source s3://bucket/db_projetado
    serialize-db publish --init
    serialize-db publish --root s3://bucket/delta --environment prd \\
        --metadata pipeline.models:Base.metadata --channel default
    serialize-db publish --root s3://bucket/delta --environment prd \\
        --metadata pipeline.models:Base.metadata --snapshot 2026T2 --tables cad_lancamentos
    serialize-db publish --root s3://bucket/delta --environment prd \\
        --metadata pipeline.models:Base.metadata --status
    serialize-db snapshot --root s3://bucket/delta --environment prd \\
        --metadata pipeline.models:Base.metadata --name 2026T3
    serialize-db channel --root s3://bucket/delta --environment prd \\
        --metadata pipeline.models:Base.metadata --name default --snapshot 2026T3
    serialize-db vacuum --root s3://bucket/delta --environment prd \\
        --metadata pipeline.models:Base.metadata --apply
    serialize-db history --root s3://bucket/delta --environment prd \\
        --metadata pipeline.models:Base.metadata --table cad_lancamentos

O código de saída é 0 quando o comando termina; 1 quando ``check`` encontra diferença, com o diff
impresso, quando a auditoria reprova e quando a carga acha uma partição fora do contrato ou uma
diferença de contagem ou soma; 2 no erro de uso, na configuração do Redshift sem conexão, na
tabela fora do modelo ou sem Delta, na partição acima do ``String(n)`` da coluna de partição, no
``execution_id`` longo demais para o prefixo do sandbox do Redshift, no ``ContractError`` do
pipeline, no conflito da execução e da carga, na publicação sem a tabela de controle, sem
``--snapshot`` nem ``--channel``, do snapshot ou do canal ausente, do snapshot arquivado ou da
tabela fora do snapshot, no modelo fora do contrato e na origem ausente ou fora dos armazenamentos
da carga, no snapshot repetido ou ausente, no canal sem ``--name`` e ``--snapshot`` juntos, no
canal ``current``, na compactação depois de um snapshot na versão atual, no arquivo do snapshot de
um canal e no destino da exportação não vazio ou fora da raiz.

.. include:: ../../docs/operacao.md
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
import pkgutil
import sys
import time
import uuid
from collections.abc import Callable
from typing import TYPE_CHECKING

import sqlalchemy as sa

from serialize_db import audit, delta, load, schema, sql
from serialize_db.audit import AuditReport
from serialize_db.engine import Engine
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.errors import (
    AuditFailed,
    ConflictError,
    ContractError,
    ExecutionConflict,
    PublicationError,
)
from serialize_db.execution import Database, Execution
from serialize_db.load import LoadReport
from serialize_db.resources import peak_rss_mb
from serialize_db.storage import Storage

if TYPE_CHECKING:
    from serialize_db.publication import PublicationStatus

__all__ = ["main"]


def _resolve_attribute(spec: str) -> object:
    """O objeto de ``modulo:atributo``, pelo ``pkgutil.resolve_name`` da biblioteca padrão: importa
    o módulo e segue os atributos por ponto.

    Exemplo:

    .. code-block:: python

        _resolve_attribute("pipeline.models:Base.metadata")
    """
    module_name, _, attribute_path = spec.partition(":")
    if not module_name or not attribute_path:
        raise argparse.ArgumentTypeError(f"esperado modulo:atributo, recebido {spec!r}")
    try:
        return pkgutil.resolve_name(spec)
    except (ImportError, AttributeError) as error:
        raise argparse.ArgumentTypeError(f"{spec}: {error}") from None


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
    """Um valor de partição, um ``execution_id``, um ambiente ou o nome de um snapshot ou de um
    canal, pela regra da partição."""
    try:
        return schema.check_partition_value(text)
    except ContractError as error:
        raise argparse.ArgumentTypeError(str(error)) from None


def _environment_default() -> str:
    """O padrão de ``--environment`` em todo subcomando: ``SERIALIZE_DB_ENVIRONMENT``, com a
    variável vazia lida como ausente, ou ``dsv``."""
    return os.environ.get("SERIALIZE_DB_ENVIRONMENT") or "dsv"


def _add_database_arguments(parser: argparse.ArgumentParser) -> None:
    """``--metadata``, ``--root`` e ``--environment`` obrigatórios, com os padrões
    ``SERIALIZE_DB_*``: os de ``run``, de ``load`` e das rotinas de operação."""
    root = os.environ.get("SERIALIZE_DB_ROOT")
    parser.add_argument("--metadata", required=True, type=_resolve_metadata,
                        help="o MetaData dos modelos, como pipeline.models:Base.metadata")
    parser.add_argument("--root", default=root, required=not root,
                        help="a raiz das tabelas Delta, pasta local ou s3://bucket/prefixo; "
                             "padrão SERIALIZE_DB_ROOT")
    parser.add_argument("--environment", type=_name_argument, default=_environment_default())


def _add_run_parser(commands: argparse._SubParsersAction) -> None:
    """``serialize-db run``, com as variáveis ``SERIALIZE_DB_*`` como padrão."""
    run = commands.add_parser("run", help="executa o pipeline de uma partição")
    _add_database_arguments(run)
    run.add_argument("--engine", choices=["duckdb", "redshift"],
                     default=os.environ.get("SERIALIZE_DB_ENGINE") or "duckdb")
    run.add_argument("--partition", type=_name_argument, required=True)
    run.add_argument("--execution-id", type=_name_argument, default=None)
    run.add_argument("pipeline", type=_resolve_function,
                     help="modulo:funcao que recebe a execução aberta")
    run.set_defaults(handler=_run)


def _add_publish_parser(commands: argparse._SubParsersAction) -> None:
    """``serialize-db publish``: a publicação no Redshift de um snapshot, pelo nome ou pelo canal,
    o estado, a tabela de controle e a despublicação; a conexão vem de
    ``SERIALIZE_DB_REDSHIFT_*``."""
    publish = commands.add_parser("publish", help="a publicação no Redshift de um snapshot")
    publish.add_argument("--metadata", type=_resolve_metadata, default=None,
                         help="modulo:atributo com o MetaData dos modelos; dispensado por --init")
    publish.add_argument("--root", default=os.environ.get("SERIALIZE_DB_ROOT"))
    publish.add_argument("--environment", type=_name_argument, default=_environment_default())
    which = publish.add_mutually_exclusive_group()
    which.add_argument("--snapshot", type=_name_argument, default=None,
                       help="publica as versões deste snapshot do arquivo de controle")
    which.add_argument("--channel", type=_name_argument, default=None,
                       help="publica o snapshot deste canal; current é a versão atual de cada "
                            "tabela")
    publish.add_argument("--tables", nargs="+", default=None,
                         help="as tabelas a publicar ou despublicar; sem ela, todas do modelo")
    publish.add_argument("--max-workers", type=int, default=1)
    publish.add_argument("--execution-id", type=_name_argument, default=None,
                         help="o identificador gravado na linha de controle; sem ele, "
                              "publicacao-<AAAA-MM-DD>-<uuid8>")
    publish.add_argument("--init", action="store_true",
                         help="cria a tabela de controle serialize_db_publications, uma vez")
    publish.add_argument("--status", action="store_true",
                         help="mostra a versão publicada e a atual de cada tabela")
    publish.add_argument("--unpublish", action="store_true",
                         help="despublica as tabelas: DROP TABLE e a linha de controle")
    publish.set_defaults(handler=_publish)


def _add_load_parser(commands: argparse._SubParsersAction) -> None:
    """``load``: a carga inicial da base Parquet de origem nas tabelas Delta do ambiente."""
    load_command = commands.add_parser("load", help="a carga inicial da base Parquet de origem")
    _add_database_arguments(load_command)
    load_command.add_argument("--source", required=True,
                              help="a raiz da base Parquet de origem, pasta local ou "
                                   "s3://bucket/prefixo")
    load_command.add_argument("--tables", nargs="+", default=None,
                              help="só estas tabelas do modelo; sem elas, todas, as sem partição "
                                   "antes das particionadas")
    load_command.add_argument("--partitions", nargs="+", type=_name_argument, default=None,
                              help="só estas partições; as tabelas sem partição ficam de fora")
    load_command.set_defaults(handler=_load)


def _add_operation_parsers(commands: argparse._SubParsersAction) -> None:
    """Os subcomandos da operação: ``snapshot``, ``channel``, ``vacuum``, ``compact``,
    ``archive``, ``export`` e ``history``."""
    snapshot = commands.add_parser("snapshot",
                                   help="o snapshot do banco com a versão atual de cada tabela")
    _add_database_arguments(snapshot)
    snapshot.add_argument("--name", required=True, type=_name_argument)
    snapshot.set_defaults(handler=_snapshot)

    channel = commands.add_parser("channel",
                                  help="aponta um canal do ambiente para um snapshot, ou lista "
                                       "os canais")
    _add_database_arguments(channel)
    channel.add_argument("--name", type=_name_argument, default=None,
                         help="o canal, como default; com --snapshot")
    channel.add_argument("--snapshot", type=_name_argument, default=None,
                         help="o snapshot que o canal passa a apontar, presente em snapshots")
    channel.set_defaults(handler=_channel)

    vacuum = commands.add_parser("vacuum",
                                 help="os arquivos fora da retenção e das versões dos snapshots")
    _add_database_arguments(vacuum)
    vacuum.add_argument("--apply", action="store_true", help="apaga os arquivos listados")
    vacuum.add_argument("--full", action="store_true", help="inclui os arquivos órfãos")
    vacuum.add_argument("--retention-hours", type=int, default=9600,
                        help="a retenção em horas; padrão 9600, os 400 dias")
    vacuum.set_defaults(handler=_vacuum)

    compact_command = commands.add_parser("compact",
                                          help="junta os arquivos pequenos das partições")
    _add_database_arguments(compact_command)
    compact_command.add_argument("--table", required=True)
    compact_command.add_argument("--partitions", nargs="+", type=_name_argument, default=None)
    compact_command.set_defaults(handler=_compact)

    archive = commands.add_parser("archive",
                                  help="copia as tabelas de um snapshot para arquivo/<nome>/")
    _add_database_arguments(archive)
    archive.add_argument("--name", required=True, type=_name_argument)
    archive.set_defaults(handler=_archive)

    export = commands.add_parser("export",
                                 help="as pastas Parquet de uma versão da tabela, sem o log")
    _add_database_arguments(export)
    export.add_argument("--table", required=True)
    export.add_argument("--destination", required=True,
                        help="a URI da pasta de destino, vazia e sob a raiz")
    export.add_argument("--version", type=int, default=None, help="a versão; padrão a atual")
    export.add_argument("--mode", choices=["copy", "rewrite"], default="copy")
    export.set_defaults(handler=_export)

    history = commands.add_parser("history",
                                  help="os commits de uma tabela com os metadados da biblioteca")
    _add_database_arguments(history)
    history.add_argument("--table", required=True)
    history.set_defaults(handler=_history)


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
                               help="imprime o texto das verificações, sem conexão nem "
                                    "armazenamento")
    audit_command.add_argument("--root", default=os.environ.get("SERIALIZE_DB_ROOT"))
    audit_command.add_argument("--environment", type=_name_argument,
                               default=_environment_default())
    audit_command.set_defaults(handler=_audit)


def _build_parser() -> argparse.ArgumentParser:
    """O parser da linha de comando, com um subcomando por primitiva."""
    parser = argparse.ArgumentParser(prog="serialize-db")
    commands = parser.add_subparsers(dest="command", required=True)
    _add_run_parser(commands)
    _add_audit_parser(commands)
    _add_publish_parser(commands)
    _add_load_parser(commands)
    _add_operation_parsers(commands)

    schema_command = commands.add_parser("schema", help="os arquivos de esquema dos modelos")
    schema_actions = schema_command.add_subparsers(dest="action", required=True)
    for action, handler, help_text in (("write", _schema_write, "grava os arquivos"),
                                       ("check", _schema_check, "compara sem gravar")):
        action_parser = schema_actions.add_parser(action, help=help_text)
        action_parser.add_argument("--metadata", required=True, type=_resolve_metadata,
                                   help="modulo:atributo com o MetaData dos modelos")
        action_parser.add_argument("directory", help="a pasta dos arquivos de esquema")
        action_parser.set_defaults(handler=handler)

    sql_command = commands.add_parser("sql", help="o texto SQL dos statements em cada motor")
    sql_actions = sql_command.add_subparsers(dest="action", required=True)
    for action, handler, help_text in (("write", _sql_write, "grava os arquivos"),
                                       ("check", _sql_check, "compara sem gravar")):
        action_parser = sql_actions.add_parser(action, help=help_text)
        action_parser.add_argument("--metadata", required=True, type=_resolve_metadata,
                                   help="modulo:atributo com o MetaData dos modelos")
        action_parser.add_argument("--statements", required=True, type=_resolve_statements,
                                   help="modulo:atributo com o dicionário {nome: statement}")
        action_parser.add_argument("directory", help="a pasta dos arquivos de texto SQL")
        action_parser.set_defaults(handler=handler)
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
    reprovada, 2 no conflito e no ``ContractError`` da abertura ou do pipeline, como o motor que
    não serve, a configuração do Redshift sem conexão e o ``execution_id`` longo demais para o
    prefixo do sandbox."""
    redshift = None
    if args.engine == "redshift":
        # O módulo do Redshift entra só com o motor: importar o pacote não carrega o driver.
        from serialize_db.engine.redshift import RedshiftConfig

        redshift = RedshiftConfig.from_environment()
    try:
        execution = Execution(Database(args.root, args.environment, args.metadata), args.engine,
                              args.partition, args.execution_id, redshift=redshift)
        # A entrada do with abre o motor, que confere a conexão e o prefixo do sandbox.
        with execution as run:
            args.pipeline(run)
    except ContractError as error:
        print(f"serialize-db run: {error}", file=sys.stderr)
        return 2
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
        if result.reason:
            verdict += f" ({result.reason})"
        print(f"{result.name}: {verdict}")
        for row in result.sample.to_pylist():
            print(f"    {row}")
    for reason in report.not_run:
        print(f"não rodou: {reason}")
    for value, totals in report.totals.items():
        print(f"partição {value}: {totals}")


def _audit_engine(args: argparse.Namespace, db: Database, execution_id: str) -> Engine:
    """O sandbox próprio da auditoria: o motor de ``--engine``."""
    if args.engine == "redshift":
        # O módulo do Redshift entra só com o motor: importar o pacote não carrega o driver.
        from serialize_db.engine.redshift import RedshiftConfig, RedshiftEngine

        return RedshiftEngine(RedshiftConfig.from_environment(), execution_id, db.storage,
                              db.staging_prefix(execution_id))
    return DuckDBEngine(DuckDBConfig(), execution_id, db.storage)


def _audit_published(args: argparse.Namespace, table: sa.Table) -> int:
    """A auditoria da versão publicada, num sandbox próprio do motor de ``--engine``: 1 na
    auditoria reprovada, 2 na tabela sem Delta e na configuração do Redshift sem conexão."""
    db = Database(args.root, args.environment, args.metadata)
    uri = db.uri(table)
    if not delta.table_exists(uri, db.storage):
        print(f"serialize-db audit: {table.name} não existe em {uri}", file=sys.stderr)
        return 2
    version = delta.open_table(uri, db.storage).version()
    # A versão atual de cada tabela referenciada que existe, para as chaves estrangeiras.
    referenced = {}
    for constraint in table.foreign_key_constraints:
        target = db.uri(constraint.referred_table)
        if delta.table_exists(target, db.storage):
            target_version = delta.open_table(target, db.storage).version()
            referenced[constraint.referred_table.name] = (target, target_version)
    # O motor Redshift confere a conexão ao nascer, e a configuração sem ela é erro de uso.
    try:
        sandbox = _audit_engine(args, db, f"auditoria-{uuid.uuid4().hex[:8]}")
    except ContractError as error:
        print(f"serialize-db audit: {error}", file=sys.stderr)
        return 2
    with sandbox as engine:
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
    return _audit_published(args, table)


def _selected_tables(metadata: sa.MetaData, names: list[str] | None) -> list[sa.Table]:
    """As tabelas de ``--tables`` no modelo, ou todas; um nome fora do modelo é erro de uso."""
    if names is None:
        return list(metadata.sorted_tables)
    tables = []
    for name in names:
        table = metadata.tables.get(name)
        if table is None:
            raise argparse.ArgumentTypeError(f"a tabela {name} não está nos modelos")
        tables.append(table)
    return tables


def _publish(args: argparse.Namespace) -> int:
    """``--init`` cria a tabela de controle; ``--status`` mostra o estado; ``--unpublish``
    despublica; sem os três, publica as tabelas no snapshot de ``--snapshot`` ou do canal de
    ``--channel``, um dos dois obrigatório. 2 no erro de uso, na configuração do Redshift sem
    conexão, sem a tabela de controle, no snapshot ou no canal ausente, no snapshot arquivado, na
    tabela fora do snapshot e no conflito."""
    # Os módulos do Redshift entram só no publish: importar o pacote não carrega o driver.
    from serialize_db import publication
    from serialize_db.engine.redshift import RedshiftConfig

    chosen = args.snapshot is not None or args.channel is not None
    if (args.init or args.status or args.unpublish) and chosen:
        print("serialize-db publish: --init, --status e --unpublish não recebem --snapshot nem "
              "--channel", file=sys.stderr)
        return 2
    if not (args.init or args.status or args.unpublish) and not chosen:
        print("serialize-db publish: informe --snapshot <nome> ou --channel <nome> (default, "
              "current)", file=sys.stderr)
        return 2
    config = RedshiftConfig.from_environment()
    if args.init:
        try:
            publication.create_publications_table(config)
        except ContractError as error:
            print(f"serialize-db publish: {error}", file=sys.stderr)
            return 2
        print(f"{config.schema}.{publication.CONTROL_TABLE} criada")
        return 0
    if not args.root or args.metadata is None:
        print("serialize-db publish: informe --metadata e --root ou SERIALIZE_DB_ROOT",
              file=sys.stderr)
        return 2
    db = Database(args.root, args.environment, args.metadata)
    try:
        tables = _selected_tables(args.metadata, args.tables)
        if args.status:
            _print_statuses(publication.publication_status(db, config))
        elif args.unpublish:
            _print_unpublished(publication.unpublish_redshift(db, config, tables))
        else:
            versions = _versions_to_publish(db, args, tables)
            execution_id = _publication_id(args.execution_id)
            _print_published(publication.publish_redshift(db, config, tables, execution_id,
                                                          args.max_workers, versions))
    except (argparse.ArgumentTypeError, PublicationError, ContractError) as error:
        print(f"serialize-db publish: {error}", file=sys.stderr)
        return 2
    except ExecutionConflict as error:
        print(f"serialize-db publish: conflito: {error}", file=sys.stderr)
        return 2
    return 0


def _versions_to_publish(db: Database, args: argparse.Namespace,
                         tables: list[sa.Table]) -> dict[str, int]:
    """As versões que ``publish`` grava: as do snapshot de ``--snapshot`` ou do canal de
    ``--channel``, e a versão atual de cada tabela do ambiente com ``--channel current``; a
    tabela pedida sem versão é ``PublicationError`` com a origem."""
    if args.channel == delta.CURRENT_CHANNEL:
        versions = _current_versions(db)
        source = f"da versão atual do ambiente {db.environment}"
    else:
        control, _ = delta.read_snapshots(db.storage, db.environment)
        name = args.snapshot or delta.channel_snapshot(control, args.channel)
        versions = delta.snapshot_versions(control, name)
        source = f"do snapshot {name}"
    missing = [table.name for table in tables if table.name not in versions]
    if missing:
        raise PublicationError(f"{', '.join(missing)}: fora {source}; --tables deixa de fora a "
                               "tabela sem versão")
    return versions


def _publication_id(execution_id: str | None) -> str:
    """O ``--execution-id`` da publicação, ou ``publicacao-<AAAA-MM-DD>-<uuid8>`` sem ele, com a
    data em UTC."""
    if execution_id:
        return execution_id
    today = datetime.datetime.now(datetime.timezone.utc).date().isoformat()
    return f"publicacao-{today}-{uuid.uuid4().hex[:8]}"


def _print_statuses(statuses: list[PublicationStatus]) -> None:
    """Uma linha por tabela: a versão publicada, a atual e as partições pendentes."""
    for status in statuses:
        print(f"{status.table}: publicada {status.published_version}, atual "
              f"{status.current_version}, pendentes {list(status.pending_partitions)}")


def _print_published(versions: dict[str, int]) -> None:
    """Uma linha por tabela publicada: a versão do Delta que ela tem agora."""
    for name, version in versions.items():
        print(f"{name}: versão {version}")


def _print_unpublished(versions: dict[str, int | None]) -> None:
    """Uma linha por tabela despublicada: a versão que estava publicada."""
    for name, version in versions.items():
        if version is None:
            print(f"{name}: não estava publicada")
        else:
            print(f"{name}: {version}")


def _print_load_report(report: LoadReport, loaded: list[str | None]) -> None:
    """As linhas de uma tabela da carga: as partições gravadas agora, cada diferença, o veredito,
    as conversões de tipo e o que ficou fora do padrão."""
    written = f"{report.table}: {len(loaded)} partição(ões) gravada(s)"
    if loaded:
        written += ": " + ", ".join(str(value) for value in loaded)
    print(written)
    for partition in report.partitions:
        if not partition.matches:
            print(f"  DIFERENÇA em {partition.value}: origem {partition.source_rows} linhas "
                  f"{dict(partition.source_sums)} não finitos {dict(partition.source_nonfinite)}, "
                  f"Delta {partition.delta_rows} linhas {dict(partition.delta_sums)} não finitos "
                  f"{dict(partition.delta_nonfinite)}")
    verdict = "contagens e somas iguais" if report.matches else "com diferenças"
    print(f"  {len(report.partitions)} partição(ões) conferida(s), {verdict}")
    if report.conversions:
        print(f"  conversões: {', '.join(report.conversions)}")
    for entry in report.skipped:
        print(f"  fora do padrão: {entry}")


def _load(args: argparse.Namespace) -> int:
    """A carga inicial de cada tabela pedida, na ordem da carga, e o relatório de cada uma: 1 na
    partição fora do contrato e na diferença de contagem ou soma, 2 no modelo fora do contrato,
    na tabela fora do modelo, na origem ausente ou fora dos armazenamentos da biblioteca e no
    conflito."""
    problems = schema.check_models(args.metadata)
    if problems:
        print("serialize-db load: modelo fora do contrato:", *problems, sep="\n  ",
              file=sys.stderr)
        return 2
    # A origem num esquema que a biblioteca não lê, ou no S3 sem região, é erro de uso.
    try:
        Storage.for_uri(args.source)
    except ValueError as error:
        print(f"serialize-db load: {error}", file=sys.stderr)
        return 2
    db = Database(args.root, args.environment, args.metadata)
    matches = True
    try:
        tables = load.load_order(_selected_tables(args.metadata, args.tables))
        for table in tables:
            loaded = load.initial_load(db, table, args.source, args.partitions)
            report = load.load_report(db, table, args.source)
            _print_load_report(report, loaded)
            matches = matches and report.matches
    except (argparse.ArgumentTypeError, FileNotFoundError) as error:
        print(f"serialize-db load: {error}", file=sys.stderr)
        return 2
    except ContractError as error:
        print(f"serialize-db load: {error}", file=sys.stderr)
        return 1
    except ExecutionConflict as error:
        print(f"serialize-db load: conflito: {error}", file=sys.stderr)
        return 2
    outside = load.entries_outside_the_model(args.source, args.metadata)
    if outside:
        print(f"fora do modelo: {', '.join(outside)}")
    verdict = "contagens e somas iguais" if matches else "com diferenças"
    print(f"{len(tables)} tabela(s) conferida(s), {verdict}")
    return 0 if matches else 1


# ---------------------------------------------------------------- a operação


def _existing_tables(db: Database) -> dict[str, tuple[sa.Table, str]]:
    """As tabelas do modelo que existem no ambiente: ``{nome: (tabela, URI)}``."""
    found = {}
    for table in db.tables():
        uri = db.uri(table)
        if delta.table_exists(uri, db.storage):
            found[table.name] = (table, uri)
    return found


def _existing_table(args: argparse.Namespace, db: Database,
                    command: str) -> tuple[sa.Table, str] | None:
    """A tabela de ``--table`` no modelo e a URI dela no ambiente, ou ``None`` com a mensagem
    impressa quando ela não está no modelo ou não tem Delta."""
    table = args.metadata.tables.get(args.table)
    if table is None:
        print(f"serialize-db {command}: a tabela {args.table} não está nos modelos",
              file=sys.stderr)
        return None
    uri = db.uri(table)
    if not delta.table_exists(uri, db.storage):
        print(f"serialize-db {command}: {table.name} não existe em {uri}", file=sys.stderr)
        return None
    return table, uri


def _current_versions(db: Database) -> dict[str, int]:
    """A versão atual de cada tabela do modelo que existe no ambiente: a entrada de ``snapshot``
    e as versões do canal ``current`` da publicação."""
    versions = {}
    for name, (_, uri) in _existing_tables(db).items():
        versions[name] = delta.open_table(uri, db.storage).version()
    return versions


def _snapshot(args: argparse.Namespace) -> int:
    """A entrada do snapshot com a versão atual de cada tabela do ambiente; 2 no nome repetido e
    no conflito de escrita."""
    db = Database(args.root, args.environment, args.metadata)
    versions = _current_versions(db)
    try:
        delta.snapshot(db.storage, db.environment, args.name, versions)
    except (ValueError, ConflictError) as error:
        print(f"serialize-db snapshot: {error}", file=sys.stderr)
        return 2
    for name, version in sorted(versions.items()):
        print(f"{name}: versão {version}")
    print(f"snapshot {args.name} gravado com {len(versions)} tabela(s)")
    return 0


def _channel(args: argparse.Namespace) -> int:
    """Com ``--name`` e ``--snapshot``, aponta o canal e imprime o snapshot anterior e o novo; sem
    os dois, lista os canais do ambiente. 2 com um só dos dois, no canal ``current``, no snapshot
    ausente ou arquivado e no conflito de escrita."""
    if (args.name is None) != (args.snapshot is None):
        print("serialize-db channel: informe --name e --snapshot juntos, ou nenhum dos dois para "
              "listar os canais", file=sys.stderr)
        return 2
    db = Database(args.root, args.environment, args.metadata)
    control, _ = delta.read_snapshots(db.storage, db.environment)
    channels = control.get("channels", {})
    if args.name is None:
        if not channels:
            print(f"nenhum canal em {db.environment}")
        for name, snapshot in sorted(channels.items()):
            print(f"{name}: {snapshot}")
        return 0
    previous = channels.get(args.name, "(nenhum)")
    try:
        # ContractError, do canal current, é um ValueError.
        delta.set_channel(db.storage, db.environment, args.name, args.snapshot)
    except (ValueError, ConflictError) as error:
        print(f"serialize-db channel: {error}", file=sys.stderr)
        return 2
    print(f"{args.name}: {previous} -> {args.snapshot}")
    return 0


def _vacuum(args: argparse.Namespace) -> int:
    """A lista, ou a exclusão com ``--apply``, dos arquivos de cada tabela fora da retenção e das
    versões dos snapshots."""
    db = Database(args.root, args.environment, args.metadata)
    control, _ = delta.read_snapshots(db.storage, db.environment)
    verb = "apagado(s)" if args.apply else "a apagar"
    for name, (_, uri) in _existing_tables(db).items():
        listed = delta.vacuum_keeping_snapshots(uri, control, name, db.storage,
                                                args.retention_hours, args.apply, args.full)
        print(f"{name}: {len(listed)} arquivo(s) {verb}")
        for path in listed:
            print(f"    {path}")
    return 0


def _measure(started: float) -> str:
    """O tempo desde ``started`` e o pico de memória residente do processo, no formato do script
    de migração: a medida da rotina na tabela, para dimensionar a máquina."""
    seconds = time.perf_counter() - started
    return f"em {seconds:.1f} s; RSS máximo do processo {peak_rss_mb():.0f} MB"


def _compact(args: argparse.Namespace) -> int:
    """A compactação das partições pedidas, com o tempo e o pico de RSS impressos; 2 na tabela
    fora do modelo ou sem Delta, na tabela particionada sem ``--partitions`` e no snapshot na
    versão atual da tabela."""
    db = Database(args.root, args.environment, args.metadata)
    found = _existing_table(args, db, "compact")
    if found is None:
        return 2
    table, uri = found
    if schema.table_options(table).partition_by is not None and not args.partitions:
        print("serialize-db compact: informe --partitions numa tabela particionada",
              file=sys.stderr)
        return 2
    current = delta.open_table(uri, db.storage).version()
    control, _ = delta.read_snapshots(db.storage, db.environment)
    for name, versions in control["snapshots"].items():
        if versions.get(table.name) == current:
            print(f"serialize-db compact: o snapshot {name} está na versão atual {current} de "
                  f"{table.name}; compacte antes de um snapshot", file=sys.stderr)
            return 2
    started = time.perf_counter()
    metrics = delta.compact(uri, table, args.partitions or [], db.storage)
    print(f"{table.name}: {metrics['numFilesAdded']} arquivo(s) gravado(s), "
          f"{metrics['numFilesRemoved']} removido(s), {_measure(started)}")
    return 0


def _archive(args: argparse.Namespace) -> int:
    """A cópia de cada tabela do snapshot para ``arquivo/<nome>/``, com o tempo e o pico de RSS
    impressos por tabela, e a entrada movida para ``archived``; 2 no snapshot ausente de
    ``snapshots`` ou apontado por um canal, na tabela do snapshot que já não existe na raiz e no
    conflito de escrita. A repetição depois de uma interrupção continua a cópia: ``deep_copy``
    pula as partições já registradas no arquivo."""
    db = Database(args.root, args.environment, args.metadata)
    storage = db.storage
    control, _ = delta.read_snapshots(storage, db.environment)
    entry = control["snapshots"].get(args.name)
    if entry is None:
        print(f"serialize-db archive: o snapshot {args.name} não está em snapshots",
              file=sys.stderr)
        return 2
    # Os canais conferidos antes da cópia: archive_snapshot só os confere depois dela.
    pointing = delta.channels_pointing(control, args.name)
    if pointing:
        print(f"serialize-db archive: o snapshot {args.name} é o do canal {', '.join(pointing)}; "
              "mova o canal antes (serialize-db channel)", file=sys.stderr)
        return 2
    pending = []
    for name, version in sorted(entry.items()):
        source = storage.uri_of(storage.join(db.environment, name))
        destination = storage.uri_of(storage.join(db.archive_prefix(args.name), name))
        if not delta.table_exists(source, storage):
            print(f"serialize-db archive: {name} não existe em {source}", file=sys.stderr)
            return 2
        pending.append((name, version, source, destination))
    # Toda tabela conferida antes da primeira cópia: um snapshot com uma tabela sumida não deixa
    # um arquivo pela metade.
    for name, version, source, destination in pending:
        started = time.perf_counter()
        copied = delta.deep_copy(source, version, destination, storage)
        print(f"{name}: versão {version} copiada para {destination}, versão {copied} no arquivo, "
              f"{_measure(started)}")
    try:
        delta.archive_snapshot(storage, db.environment, args.name)
    except (ValueError, ConflictError) as error:
        print(f"serialize-db archive: {error}", file=sys.stderr)
        return 2
    print(f"snapshot {args.name} movido para archived")
    return 0


def _export(args: argparse.Namespace) -> int:
    """A exportação de uma versão da tabela para pastas Parquet sem o log, com o tempo e o pico
    de RSS impressos; 2 na tabela fora do modelo ou sem Delta e no destino fora da raiz ou não
    vazio."""
    db = Database(args.root, args.environment, args.metadata)
    found = _existing_table(args, db, "export")
    if found is None:
        return 2
    table, uri = found
    try:
        relative = db.storage.relative(args.destination)
    except ValueError as error:
        print(f"serialize-db export: {error}", file=sys.stderr)
        return 2
    if db.storage.list_files(relative):
        print(f"serialize-db export: destino não vazio: {args.destination}", file=sys.stderr)
        return 2
    started = time.perf_counter()
    files = delta.export_snapshot(uri, table, args.destination, db.storage, args.version,
                                  args.mode)
    version = "" if args.version is None else f" da versão {args.version}"
    print(f"{table.name}: {len(files)} arquivo(s) em {args.destination}{version}, "
          f"{_measure(started)}")
    return 0


def _history(args: argparse.Namespace) -> int:
    """Uma linha por commit da tabela, do mais recente ao mais antigo: a versão, a operação, o
    instante em UTC e os metadados da biblioteca que o commit tem."""
    db = Database(args.root, args.environment, args.metadata)
    found = _existing_table(args, db, "history")
    if found is None:
        return 2
    _, uri = found
    for entry in delta.history(uri, db.storage):
        print(_history_line(entry))
    return 0


def _history_line(entry: dict) -> str:
    """A linha de um commit: a versão, a operação, o instante em UTC e os metadados da biblioteca
    que ele tem, na ordem de ``delta.history``."""
    instant = entry["timestamp"].isoformat(timespec="seconds")
    line = f"{entry['version']} {entry['operation']} {instant}"
    for key, value in entry.items():
        if key.startswith("serialize_db_"):
            line += f" {key}={value}"
    return line


def main(argv: list[str] | None = None) -> int:
    """Executa a linha de comando.

    Cada subcomando guarda a sua função em ``handler`` (``set_defaults`` do ``argparse``). Fora de
    ``schema`` e ``sql``, o log vai ao stderr a partir do nível ``INFO``.

    Exemplo:

    .. code-block:: python

        main(["schema", "check", "--metadata", "pipeline.models:Base.metadata", "schema/"])
        # 0 com os arquivos atualizados; 1 com o diff impresso

    :param argv: os argumentos, sem o nome do programa; ``None`` lê ``sys.argv``.
    :return: o código de saída: 0 quando o comando termina, 1 e 2 nos casos que a documentação
        do módulo lista.
    :raises SystemExit: o erro de uso que o ``argparse`` detecta, com o código 2, e ``--help``,
        com o código 0.
    """
    args = _build_parser().parse_args(argv)
    if args.command not in ("schema", "sql"):
        logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    return args.handler(args)


if __name__ == "__main__":
    sys.exit(main())
