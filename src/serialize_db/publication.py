"""A publicação para clientes no Redshift: as tabelas ``<ambiente>_<tabela>`` do esquema,
derivadas do Delta, e a tabela de controle ``serialize_db_publications``.

A tabela de controle é uma só para todos os ambientes, criada uma vez no esquema pelo usuário
(``create_publications_table`` ou ``serialize-db publish --init``); nenhum caminho do pipeline a
cria, e ``publish_redshift`` recusa publicar sem ela, antes de qualquer escrita. A publicação de
cada tabela é uma transação, numa conexão própria: ``BEGIN``; a leitura da linha de controle, que
identifica a versão publicada e fixa o snapshot da transação; sem linha, a primeira publicação, com
a tabela publicada criada e todas as partições; com linha, ``version_diff`` entre a versão lida e a
pedida, a menor e a maior delas, o que também volta a tabela a um snapshot anterior ao publicado;
por partição, ``DELETE`` da partição e, quando a versão pedida a tem, ``COPY ... MANIFEST`` numa
staging temporária e ``INSERT ... SELECT`` com o valor; e por último o ``INSERT`` da linha de
controle, ou o ``UPDATE`` dela condicionado à versão lida, cujas 0 linhas, como o ``1023`` e a
tabela publicada que outra primeira publicação criou, saem como ``ExecutionConflict``. As tabelas
correm num pool, uma conexão por tabela. ``unpublish_redshift`` apaga a tabela publicada e a linha
de controle numa transação, e ``publication_status`` compara a versão publicada com a atual. As
versões vêm de um snapshot do arquivo de controle, por ``serialize-db publish --snapshot`` ou
``--channel``, ou são as atuais.

O ``COPY`` leva a cláusula de credenciais do motor Redshift, montada uma vez por tabela, antes
dos comandos da transação; nenhum texto que a carregue vai a log. Todo comando cita a tabela
publicada e a de controle por nome em duas partes, depois do ``USE`` que a conexão roda, e a
staging temporária pelo nome só, sem ``COMPUPDATE`` e sem ``TRUNCATE``, o que um datashare
aceita.

Exemplo:

.. code-block:: python

    from serialize_db import publication
    from serialize_db.engine.redshift import RedshiftConfig

    config = RedshiftConfig.from_environment()
    publication.create_publications_table(config)          # uma vez, pelo usuário
    publication.publish_redshift(db, config, [Lancamento.__table__], "exec-2026-09-05")
    publication.publication_status(db, config)
"""

from __future__ import annotations

import dataclasses
import functools
import logging
import re
import time
from collections.abc import Mapping, Sequence
from typing import TYPE_CHECKING

import redshift_connector
import sqlalchemy as sa

from serialize_db import delta
from serialize_db._pool import run_in_pool
from serialize_db.engine.redshift import (
    RedshiftConfig,
    columns_without_partition,
    connect,
    copy_text,
    credentials_clause,
    insert_from_staging,
    mask,
    relation_exists,
    relation_missing,
    serialization_failure,
    staging_ddl,
)
from serialize_db.errors import ExecutionConflict, PublicationError
from serialize_db.resources import peak_rss_mb
from serialize_db.schema import (
    column_ddl,
    literal,
    quoted,
    redshift_options,
    sql_type,
    table_options,
)
from serialize_db.storage import Storage

if TYPE_CHECKING:
    from serialize_db.execution import Database

__all__ = [
    "CONTROL_TABLE",
    "PublicationStatus",
    "PublishedColumn",
    "control_ddl",
    "control_read",
    "create_publications_table",
    "publication_statements",
    "publication_status",
    "publish_redshift",
    "published_ddl",
    "reconcile_published",
    "unpublication_statements",
    "unpublish_redshift",
]

log = logging.getLogger("serialize_db.publication")

CONTROL_TABLE = "serialize_db_publications"
"""A tabela de controle: ``table_name``, ``delta_version``, ``execution_id`` e ``published_at``."""

# A instrução de inicialização, na mensagem de PublicationError.
_INIT = "crie-a uma vez com serialize-db publish --init (create_publications_table)"

# O tipo de svv_all_columns e o do DDL do contrato, cada um numa família, para o diff das
# tabelas publicadas; o texto, o CHAR e o NUMERIC levam a largura, ou a precisão e a escala.
_TYPE_FAMILIES = {
    "bigint": "bigint",
    "smallint": "smallint",
    "integer": "integer",
    "int": "integer",
    "boolean": "boolean",
    "double precision": "double",
    "real": "real",
    "date": "date",
    "timestamp": "timestamp",
    "timestamp without time zone": "timestamp",
    "timestamptz": "timestamptz",
    "timestamp with time zone": "timestamptz",
    "varchar": "varchar",
    "character varying": "varchar",
    "char": "char",
    "character": "char",
    "decimal": "decimal",
    "numeric": "decimal",
    "super": "super",
}


@dataclasses.dataclass(frozen=True)
class PublicationStatus:
    """A situação da publicação de uma tabela, para o operador.

    Exemplo:

    .. code-block:: python

        publication_status(db, config)[0]
        # PublicationStatus(table="prd_cad_lancamentos", published_version=57,
        #                   current_version=58, pending_partitions=("2026-08-31",))
    """

    table: str
    """``<ambiente>_<tabela>``."""
    published_version: int | None
    """A versão do Delta na tabela de controle; ``None`` quando nunca publicada."""
    current_version: int
    """A versão atual do Delta."""
    pending_partitions: tuple[str | None, ...]
    """As partições que a próxima publicação troca; ``(None,)`` na tabela sem partição que ela troca
    inteira."""


@dataclasses.dataclass(frozen=True)
class PublishedColumn:
    """Uma coluna de uma tabela publicada, como ``svv_all_columns`` a lista.

    Exemplo:

    .. code-block:: python

        PublishedColumn("preco", "numeric", None, 18, 2)
    """

    name: str
    """O nome da coluna, o ``column_name`` do catálogo."""
    data_type: str
    """O tipo, o ``data_type`` do catálogo, como ``bigint``, ``numeric`` ou
    ``character varying``."""
    length: int | None
    """A largura do texto, o ``character_maximum_length``; ``None`` quando o catálogo não a traz."""
    precision: int | None
    """A precisão numérica, o ``numeric_precision``; ``None`` quando o catálogo não a traz."""
    scale: int | None
    """A escala numérica, o ``numeric_scale``; ``None`` quando o catálogo não a traz."""


# ---------------------------------------------------------------- o texto dos comandos


def _published_name(environment: str, table: sa.Table) -> str:
    """``<ambiente>_<tabela>``, o nome da tabela publicada."""
    return f"{environment}_{table.name}"


def _qualified(schema: str, name: str) -> str:
    """``"<esquema>"."<nome>"``."""
    return f"{quoted(schema)}.{quoted(name)}"


def control_ddl(schema: str) -> str:
    """O ``CREATE TABLE`` da tabela de controle, sem ``IF NOT EXISTS``: só o usuário o roda, uma
    vez, e a segunda chamada falha com a mensagem do servidor.

    Exemplo:

    .. code-block:: python

        control_ddl("sbx_aco_decon")
        # CREATE TABLE "sbx_aco_decon"."serialize_db_publications" (table_name VARCHAR(127), ...)

    :param schema: o esquema do Redshift, o ``schema`` de ``RedshiftConfig``.
    :return: o texto do comando, com a tabela qualificada pelo esquema.
    """
    return (f"CREATE TABLE {_qualified(schema, CONTROL_TABLE)} (table_name VARCHAR(127), "
            "delta_version BIGINT, execution_id VARCHAR(127), published_at TIMESTAMP)")


def control_read(schema: str, environment: str, table: sa.Table) -> str:
    """O ``select`` da linha de controle da tabela, o primeiro comando da transação.

    Exemplo:

    .. code-block:: python

        control_read("sbx_aco_decon", "prd", Lancamento.__table__)
        # SELECT delta_version FROM "sbx_aco_decon"."serialize_db_publications"
        # WHERE table_name = 'prd_cad_lancamentos'

    :param schema: o esquema do Redshift, o ``schema`` de ``RedshiftConfig``.
    :param environment: o ambiente, que prefixa o nome da tabela publicada,
        ``<ambiente>_<tabela>``.
    :param table: a tabela do modelo.
    :return: o texto da consulta, que devolve ``delta_version`` numa linha, ou nenhuma linha
        antes da primeira publicação.
    """
    return (f"SELECT delta_version FROM {_qualified(schema, CONTROL_TABLE)} "
            f"WHERE table_name = {literal(_published_name(environment, table))}")


def published_ddl(schema: str, environment: str, table: sa.Table) -> str:
    """O ``CREATE TABLE`` da tabela publicada: o DDL do contrato com a chave primária
    informativa e as cláusulas físicas do modelo.

    Exemplo:

    .. code-block:: python

        print(published_ddl("sbx_aco_decon", "prd", Lancamento.__table__))
        # CREATE TABLE "sbx_aco_decon"."prd_cad_lancamentos" (
        #     "id_lancamento" BIGINT NOT NULL,
        #     ...
        #     PRIMARY KEY ("id_lancamento")
        # ) SORTKEY ("data_base", "id_lancamento")

    :param schema: o esquema do Redshift, o ``schema`` de ``RedshiftConfig``.
    :param environment: o ambiente, que prefixa o nome da tabela publicada,
        ``<ambiente>_<tabela>``.
    :param table: a tabela do modelo.
    :return: o texto do comando, com a tabela qualificada pelo esquema.
    :raises ContractError: a tabela com um tipo de coluna fora do contrato ou com mais de uma
        coluna de partição.
    """
    lines = []
    for column in table.columns:
        lines.append("    " + column_ddl(column, "redshift"))
    key = [quoted(column.name) for column in table.primary_key.columns]
    if key:
        lines.append(f"    PRIMARY KEY ({', '.join(key)})")
    name = _qualified(schema, _published_name(environment, table))
    return (f"CREATE TABLE {name} (\n" + ",\n".join(lines) + "\n)"
            + redshift_options(table_options(table)))


def _partition_delete(published: str, table: sa.Table, value: str | None) -> str:
    """O ``DELETE`` da partição na tabela publicada; a tabela inteira sem partição."""
    partition_by = table_options(table).partition_by
    if partition_by is None:
        return f"DELETE FROM {published}"
    return f"DELETE FROM {published} WHERE {quoted(partition_by)} = {literal(value)}"


def publication_statements(schema: str, environment: str, table: sa.Table,
                           partitions: Sequence[str | None],
                           manifests: Mapping[str | None, str], delta_version: int,
                           published_version: int | None, execution_id: str,
                           credentials: str) -> list[str]:
    """Os comandos da transação depois da leitura da linha de controle, um por ``execute``, sem
    ``BEGIN`` e ``COMMIT``: na primeira publicação o ``CREATE TABLE`` da tabela publicada; a
    staging temporária sem a coluna de partição; por partição, o ``DELETE`` dela e, quando o
    manifesto existe, o ``DELETE`` da staging, o ``COPY ... MANIFEST FILLRECORD`` e o
    ``INSERT ... SELECT`` com o valor e ``JSON_PARSE``; o ``DROP`` da staging; e por último a
    linha de controle, pelo ``INSERT`` ou pelo ``UPDATE`` condicionado à versão lida.

    Exemplo:

    .. code-block:: python

        publication_statements("sbx_aco_decon", "prd", Lancamento.__table__, ["2026-08-31"],
                               {"2026-08-31": "s3://bucket/prd/publicacao/exec-42/...manifest"},
                               58, 57, "exec-42", "IAM_ROLE default")

    :param schema: o esquema do Redshift, o ``schema`` de ``RedshiftConfig``.
    :param environment: o ambiente, que prefixa o nome da tabela publicada,
        ``<ambiente>_<tabela>``.
    :param table: a tabela do modelo.
    :param partitions: as partições que a publicação troca, na ordem dos comandos; ``None`` é a
        tabela inteira, sem partição.
    :param manifests: a URI do manifesto do ``COPY`` de cada partição com arquivo na versão, pelo
        valor; uma partição sem manifesto foi removida no Delta e recebe só o ``DELETE``.
    :param delta_version: a versão do Delta publicada, que a linha de controle grava.
    :param published_version: a versão lida na linha de controle; ``None`` na primeira
        publicação.
    :param execution_id: a execução que publica, que a linha de controle grava.
    :param credentials: a cláusula de credenciais, como ``IAM_ROLE default``, que os textos do
        ``COPY`` carregam.
    :return: os comandos, na ordem da transação.
    :raises ContractError: a tabela com um tipo de coluna fora do contrato ou com mais de uma
        coluna de partição.
    """
    name = _published_name(environment, table)
    published = _qualified(schema, name)
    staging = quoted(f"{name}_staging")
    statements = []
    if published_version is None:
        statements.append(published_ddl(schema, environment, table))
    statements.append(staging_ddl(table, staging, columns_without_partition(table),
                                  temporary=True))
    for value in partitions:
        statements.append(_partition_delete(published, table, value))
        if value not in manifests:
            continue
        statements.append(f"DELETE FROM {staging}")
        statements.append(copy_text(staging, manifests[value], credentials, manifest=True))
        statements.append(insert_from_staging(published, staging, table, value))
    statements.append(f"DROP TABLE {staging}")
    control = _qualified(schema, CONTROL_TABLE)
    if published_version is None:
        statements.append(f"INSERT INTO {control} VALUES ({literal(name)}, {int(delta_version)}, "
                          f"{literal(execution_id)}, getdate())")
    else:
        statements.append(f"UPDATE {control} SET delta_version = {int(delta_version)}, "
                          f"execution_id = {literal(execution_id)}, published_at = getdate() "
                          f"WHERE table_name = {literal(name)} "
                          f"AND delta_version = {int(published_version)}")
    return statements


def unpublication_statements(schema: str, environment: str, table: sa.Table,
                             published_version: int) -> list[str]:
    """Os comandos da despublicação depois da leitura da linha de controle: o ``DROP TABLE`` da
    tabela publicada e o ``DELETE`` da linha, condicionado à versão lida.

    Exemplo:

    .. code-block:: python

        unpublication_statements("sbx_aco_decon", "prd", Lancamento.__table__, 58)

    :param schema: o esquema do Redshift, o ``schema`` de ``RedshiftConfig``.
    :param environment: o ambiente, que prefixa o nome da tabela publicada,
        ``<ambiente>_<tabela>``.
    :param table: a tabela do modelo.
    :param published_version: a versão lida na linha de controle.
    :return: os comandos, na ordem da transação.
    """
    name = _published_name(environment, table)
    return [
        f"DROP TABLE {_qualified(schema, name)}",
        f"DELETE FROM {_qualified(schema, CONTROL_TABLE)} WHERE table_name = {literal(name)} "
        f"AND delta_version = {int(published_version)}",
    ]


# ---------------------------------------------------------------- a reconciliação


def _expected_column(column: sa.Column) -> PublishedColumn:
    """A coluna do contrato como ``svv_all_columns`` a listaria, na família do tipo."""
    # O texto do tipo é o nome, com um número entre parênteses (a largura) ou dois (a precisão e
    # a escala): "BIGINT", "VARCHAR(20)", "DECIMAL(18, 2)".
    text = sql_type(column, "redshift")
    match = re.fullmatch(r"([A-Z ]+?)(?:\((\d+)(?:, (\d+))?\))?", text)
    type_name, first_number, second_number = match.groups()
    family = _TYPE_FAMILIES[type_name.lower()]
    if family in ("varchar", "char"):
        return PublishedColumn(column.name, family, _optional_int(first_number), None, None)
    if family == "decimal":
        return PublishedColumn(column.name, family, None, _optional_int(first_number),
                               _optional_int(second_number))
    return PublishedColumn(column.name, family, None, None, None)


def _optional_int(value: object) -> int | None:
    """O inteiro de um número lido como texto ou do catálogo; ``None`` quando não há valor."""
    if value is None:
        return None
    return int(value)


def _in_family(column: PublishedColumn) -> PublishedColumn:
    """A coluna publicada com o tipo na família, para a comparação."""
    family = _TYPE_FAMILIES.get(column.data_type.lower(), column.data_type.lower())
    if family in ("varchar", "char"):
        return dataclasses.replace(column, data_type=family, precision=None, scale=None)
    if family == "decimal":
        return dataclasses.replace(column, data_type=family, length=None)
    return PublishedColumn(column.name, family, None, None, None)


def reconcile_published(schema: str, environment: str, table: sa.Table,
                        columns: Sequence[PublishedColumn]) -> tuple[list[str], list[str]]:
    """O diff entre o contrato e a tabela publicada.

    Aditivo: a coluna anulável nova. Destrutivo: a coluna removida, a coluna ``NOT NULL`` nova, o
    tipo que mudou e a largura de ``VARCHAR(n)``, a precisão ou a escala que mudaram, porque o
    ``ALTER COLUMN ... TYPE`` é recusado no esquema do datashare.

    Exemplo:

    .. code-block:: python

        statements, destructive = reconcile_published("sbx_aco_decon", "prd",
                                                      Lancamento.__table__, columns)

    :param schema: o esquema do Redshift, o ``schema`` de ``RedshiftConfig``.
    :param environment: o ambiente, que prefixa o nome da tabela publicada,
        ``<ambiente>_<tabela>``.
    :param table: a tabela do modelo.
    :param columns: as colunas da tabela publicada como ``svv_all_columns`` as lista, com nome,
        tipo, largura, precisão e escala, em ``PublishedColumn``.
    :return: os ``ALTER TABLE ... ADD COLUMN`` do diff aditivo, no fim da tabela, e uma frase por
        diferença destrutiva, que despublica a tabela para a transação da mesma publicação
        recriá-la.
    :raises ContractError: a tabela com um tipo de coluna fora do contrato.
    """
    published = _qualified(schema, _published_name(environment, table))
    existing = {column.name: _in_family(column) for column in columns}
    statements = []
    destructive = []
    for column in table.columns:
        expected = _expected_column(column)
        is_new = column.name not in existing
        if is_new and column.nullable:
            statements.append(f"ALTER TABLE {published} ADD COLUMN "
                              f"{column_ddl(column, 'redshift')}")
        elif is_new:
            destructive.append(f"{column.name}: coluna NOT NULL nova")
        elif existing[column.name] != expected:
            destructive.append(f"{column.name}: {existing[column.name]} na tabela publicada, "
                               f"{expected} no modelo")
    for name in existing:
        if name not in table.c:
            destructive.append(f"{name}: removida do modelo")
    return statements, destructive


# ---------------------------------------------------------------- a conexão


class _Connection:
    """Uma conexão da publicação: um cursor por comando, o erro do servidor com o comando
    mascarado numa nota, e o ``BEGIN``, o ``COMMIT`` e o ``ROLLBACK`` explícitos."""

    def __init__(self, config: RedshiftConfig) -> None:
        self.config = config
        self.connection = connect(config)

    def execute(self, text: str) -> object:
        """Roda um comando num cursor novo e o devolve."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(text)
        except redshift_connector.Error as error:
            error.add_note(f"comando: {mask(text)}")
            raise
        return cursor

    def rows(self, text: str) -> list:
        """As linhas de uma consulta."""
        return list(self.execute(text).fetchall())

    def rollback(self) -> None:
        """O ``ROLLBACK``; o erro dele não esconde o que o causou."""
        try:
            self.execute("ROLLBACK")
        except redshift_connector.Error as error:
            log.warning("ROLLBACK recusado: %s", error)

    def close(self) -> None:
        self.connection.close()


def _check_control_table(connection: _Connection, schema: str) -> None:
    """A tabela de controle existe, por ``select 1 ... limit 0`` fora de transação; sem ela,
    ``PublicationError`` com a instrução de inicialização."""
    try:
        connection.execute(f"SELECT 1 FROM {_qualified(schema, CONTROL_TABLE)} LIMIT 0")
    except redshift_connector.Error as error:
        if relation_missing(error):
            raise PublicationError(
                f"a tabela de controle {schema}.{CONTROL_TABLE} não existe; {_INIT}") from None
        raise


def _read_control(connection: _Connection, schema: str, environment: str,
                  table: sa.Table) -> int | None:
    """A versão publicada da tabela na linha de controle, ou ``None`` sem linha."""
    rows = connection.rows(control_read(schema, environment, table))
    return int(rows[0][0]) if rows else None


def _published_columns(connection: _Connection, config: RedshiftConfig, environment: str,
                       table: sa.Table) -> list[PublishedColumn]:
    """As colunas da tabela publicada em ``svv_all_columns``, na ordem; vazia quando a tabela
    não existe."""
    name = _published_name(environment, table)
    try:
        connection.execute(f"SELECT 1 FROM {_qualified(config.schema, name)} LIMIT 0")
    except redshift_connector.Error as error:
        if relation_missing(error):
            return []
        raise
    # svv_all_columns cruza os bancos: information_schema.columns não lista a tabela depois do USE.
    rows = connection.rows(
        "SELECT column_name, data_type, character_maximum_length, numeric_precision, "
        f"numeric_scale FROM svv_all_columns WHERE schema_name = {literal(config.schema)} "
        f"AND table_name = {literal(name)} ORDER BY ordinal_position")
    columns = []
    for column_name, data_type, length, precision, scale in rows:
        columns.append(PublishedColumn(str(column_name), str(data_type), _optional_int(length),
                                       _optional_int(precision), _optional_int(scale)))
    if not columns:
        log.warning("%s existe, e svv_all_columns não lista as colunas dela: a reconciliação "
                    "não rodou", name)
    return columns


# ---------------------------------------------------------------- a publicação


def _conflict(error: BaseException, table: sa.Table) -> ExecutionConflict | None:
    """O ``ExecutionConflict`` de um erro do servidor que é conflito entre publicações: o
    ``1023`` e a tabela publicada que outra primeira publicação criou; ``None`` nos demais."""
    if serialization_failure(error):
        return ExecutionConflict(f"{table.name}: outra publicação gravou a tabela ao mesmo tempo "
                                 f"(1023): {error}")
    if relation_exists(error):
        return ExecutionConflict(f"{table.name}: outra primeira publicação criou a tabela "
                                 f"publicada: {error}")
    return None


def _unpublish_table(connection: _Connection, config: RedshiftConfig, environment: str,
                     table: sa.Table) -> int | None:
    """A transação da despublicação: a linha lida, o ``DROP TABLE`` e o ``DELETE`` da linha;
    devolve a versão que estava publicada, ou ``None`` sem linha."""
    connection.execute("BEGIN")
    try:
        published = _read_control(connection, config.schema, environment, table)
        if published is None:
            connection.rollback()
            return None
        drop, delete_control = unpublication_statements(config.schema, environment, table,
                                                        published)
        connection.execute(drop)
        deleted = connection.execute(delete_control)
        if deleted.rowcount == 0:
            raise ExecutionConflict(f"{table.name}: a linha de controle mudou desde a leitura "
                                    f"(versão lida {published})")
        connection.execute("COMMIT")
    except redshift_connector.Error as error:
        connection.rollback()
        conflict = _conflict(error, table)
        if conflict is not None:
            raise conflict from error
        raise
    except BaseException:
        connection.rollback()
        raise
    return published


def _reconcile(connection: _Connection, config: RedshiftConfig, environment: str,
               table: sa.Table) -> None:
    """A tabela publicada reconciliada com o contrato: as colunas novas acrescentadas, fora de
    transação; um diff destrutivo despublica a tabela, e a transação da mesma publicação a
    recria."""
    columns = _published_columns(connection, config, environment, table)
    if not columns:
        return
    statements, destructive = reconcile_published(config.schema, environment, table, columns)
    if destructive:
        log.warning("%s: diff destrutivo na tabela publicada (%s); a tabela é despublicada e "
                    "recriada com todas as partições", table.name, "; ".join(destructive))
        _unpublish_table(connection, config, environment, table)
        return
    for statement in statements:
        connection.execute(statement)


def _partitions_to_publish(uri: str, table: sa.Table, published: int | None, version: int,
                           storage: Storage) -> tuple[list[str | None], list[str | None]]:
    """As partições que a publicação troca e, entre elas, as que têm arquivo na versão: todas na
    primeira publicação, as de ``version_diff`` entre a menor e a maior das duas versões nas
    seguintes, o que serve à volta a uma versão anterior à publicada."""
    partition_by = table_options(table).partition_by
    available = delta.partition_values(delta.open_table(uri, storage, version), partition_by)
    if published is None:
        return list(available), list(available)
    changed = sorted(delta.version_diff(uri, min(published, version), max(published, version),
                                        table, storage), key=str)
    return changed, [value for value in changed if value in available]


def _write_manifests(db: Database, table: sa.Table, values: Sequence[str | None],
                     execution_id: str, version: int) -> dict[str | None, str]:
    """Um manifesto do ``COPY`` por partição com arquivo, em
    ``<ambiente>/publicacao/<execution_id>/<tabela>/<valor>.manifest``."""
    storage = db.storage
    uri = db.uri(table)
    manifests = {}
    for value in values:
        tag = value if value is not None else "tabela"
        path = storage.join(db.publication_prefix(execution_id), table.name, f"{tag}.manifest")
        partitions = [value] if value is not None else None
        manifests[value] = delta.copy_manifest(uri, version, partitions, storage.uri_of(path),
                                               storage)
    return manifests


def _run_publication(connection: _Connection, table: sa.Table, statements: Sequence[str],
                     published: int | None) -> None:
    """Os comandos da transação, um por ``execute``; o último grava a linha de controle, e o
    ``UPDATE`` dela que não afeta linha é ``ExecutionConflict``."""
    *changes, control_statement = statements
    for statement in changes:
        connection.execute(statement)
    control = connection.execute(control_statement)
    if published is not None and control.rowcount == 0:
        raise ExecutionConflict(f"{table.name}: a linha de controle mudou desde a leitura "
                                f"(versão lida {published})")


def _publication_transaction(connection: _Connection, db: Database, config: RedshiftConfig,
                             table: sa.Table, execution_id: str,
                             version: int) -> list[str | None] | None:
    """A transação da publicação de uma tabela: a linha de controle lida, as partições trocadas e
    a linha gravada; devolve as partições trocadas, ou ``None`` quando a versão já está
    publicada e nada muda."""
    environment = db.environment
    connection.execute("BEGIN")
    try:
        published = _read_control(connection, config.schema, environment, table)
        if published == version:
            connection.rollback()
            return None
        changed, with_files = _partitions_to_publish(db.uri(table), table, published, version,
                                                     db.storage)
        manifests = _write_manifests(db, table, with_files, execution_id, version)
        statements = publication_statements(config.schema, environment, table, changed,
                                            manifests, version, published, execution_id,
                                            credentials_clause(config))
        _run_publication(connection, table, statements, published)
        connection.execute("COMMIT")
    except redshift_connector.Error as error:
        connection.rollback()
        conflict = _conflict(error, table)
        if conflict is not None:
            raise conflict from error
        raise
    except BaseException:
        connection.rollback()
        raise
    return changed


def _publish_table(db: Database, config: RedshiftConfig, table: sa.Table, execution_id: str,
                   version: int) -> int:
    """A publicação de uma tabela, numa conexão própria: a reconciliação e a transação, com o
    tempo e o pico de RSS do processo na linha de log da tabela publicada."""
    started = time.perf_counter()
    connection = _Connection(config)
    try:
        _reconcile(connection, config, db.environment, table)
        changed = _publication_transaction(connection, db, config, table, execution_id, version)
    finally:
        connection.close()
    if changed is None:
        log.info("%s: a versão %s já está publicada", table.name, version)
        return version
    log.info("%s publicada na versão %s: partições %s, em %.1f s; RSS máximo do processo "
             "%.0f MB", table.name, version, changed, time.perf_counter() - started,
             peak_rss_mb())
    return version


def _version_to_publish(db: Database, table: sa.Table,
                        versions: Mapping[str, int] | None) -> int:
    """A versão do Delta que a publicação grava: a de ``versions``, ou a atual sem ``versions``; a
    tabela sem versão, fora de ``versions`` ou inexistente no ambiente, é ``PublicationError``."""
    if versions is not None:
        version = versions.get(table.name)
        if version is None:
            raise PublicationError(f"{table.name}: a tabela está fora das versões pedidas, e não "
                                   "há o que publicar")
        return version
    uri = db.uri(table)
    if not delta.table_exists(uri, db.storage):
        raise PublicationError(f"{table.name}: a tabela não existe no ambiente {db.environment}, "
                               "e não há o que publicar")
    return delta.open_table(uri, db.storage).version()


def create_publications_table(config: RedshiftConfig) -> None:
    """Cria a tabela de controle no esquema, uma vez, pelo usuário.

    Exemplo:

    .. code-block:: python

        create_publications_table(RedshiftConfig.from_environment())

    :param config: a configuração do Redshift, com a conexão e o esquema.
    :raises redshift_connector.Error: a segunda chamada falha com a mensagem do servidor, porque a
        tabela já existe.
    :raises ContractError: ``config`` sem ``workgroup`` e sem ``host``, ``user`` e ``password``.
    """
    connection = _Connection(config)
    try:
        connection.execute(control_ddl(config.schema))
    finally:
        connection.close()


def publish_redshift(db: Database, config: RedshiftConfig, tables: Sequence[sa.Table],
                     execution_id: str, max_workers: int = 1,
                     versions: Mapping[str, int] | None = None) -> dict[str, int]:
    """Publica as tabelas no Redshift.

    Confere a tabela de controle antes de tudo; depois, por tabela, numa conexão própria do pool: a
    reconciliação da tabela publicada que já existe e a transação da publicação. Uma tabela cuja
    versão publicada é a pedida não muda; uma versão anterior à publicada volta a tabela a ela,
    trocando as partições alteradas entre as duas, e a partição que só a versão publicada tem sai
    pelo ``DELETE``. Na primeira falha nada novo começa, o que está em curso termina, e a exceção
    leva o resultado de cada tabela numa nota. Cada tabela publicada vai ao log com as partições,
    o tempo e o pico de memória residente do processo.

    Exemplo:

    .. code-block:: python

        publish_redshift(db, config, [Lancamento.__table__], "exec-2026-09-05")
        # {"cad_lancamentos": 58}

    :param db: o banco, com a raiz Delta e o ambiente, que prefixa o nome das tabelas publicadas.
    :param config: a configuração do Redshift, com a conexão, o esquema e o ``iam_role`` do
        ``COPY``.
    :param tables: as tabelas do modelo a publicar.
    :param execution_id: a execução que publica, que a linha de controle grava; os manifestos do
        ``COPY`` ficam em ``<ambiente>/publicacao/<execution_id>/``, sob a raiz.
    :param max_workers: o tamanho do pool, quantas tabelas publicam ao mesmo tempo; o padrão 1
        publica uma por vez.
    :param versions: a versão do Delta por nome de tabela, as de um snapshot do arquivo de
        controle (``delta.snapshot_versions``); ``None`` publica a versão atual de cada tabela.
    :return: ``{tabela: versão publicada}``.
    :raises PublicationError: antes de qualquer escrita, sem a tabela de controle, ou com uma
        tabela sem versão: fora de ``versions`` ou, sem ``versions``, fora do Delta do ambiente.
    :raises ExecutionConflict: o ``UPDATE`` da linha de controle sem linha (ou o ``DELETE`` dela,
        na despublicação de um diff destrutivo), o ``1023`` e a tabela publicada que outra
        primeira publicação criou, sem repetição.
    :raises LogUnavailable: um arquivo do log entre a versão publicada e a pedida não existe.
    :raises SandboxError: sem ``iam_role`` em ``config`` e sem credenciais da AWS na sessão
        ``boto3``, para o ``COPY``.
    :raises ContractError: ``config`` sem ``workgroup`` e sem ``host``, ``user`` e ``password``.
    """
    connection = _Connection(config)
    try:
        _check_control_table(connection, config.schema)
    finally:
        connection.close()
    tasks = []
    for table in tables:
        version = _version_to_publish(db, table, versions)
        task = functools.partial(_publish_table, db, config, table, execution_id, version)
        tasks.append((table.name, task))
    return run_in_pool(tasks, max_workers)


def unpublish_redshift(db: Database, config: RedshiftConfig,
                       tables: Sequence[sa.Table]) -> dict[str, int | None]:
    """Despublica as tabelas: por tabela, uma transação com o ``DROP TABLE`` da tabela publicada
    e o ``DELETE`` da linha de controle. O Delta fica intacto.

    Exemplo:

    .. code-block:: python

        unpublish_redshift(db, config, [Lancamento.__table__])   # {"cad_lancamentos": 58}

    :param db: o banco, com a raiz Delta e o ambiente, que prefixa o nome das tabelas publicadas.
    :param config: a configuração do Redshift, com a conexão e o esquema.
    :param tables: as tabelas do modelo a despublicar.
    :return: ``{tabela: versão que estava publicada}``, com ``None`` na tabela que não estava.
    :raises PublicationError: antes de qualquer escrita, sem a tabela de controle.
    :raises ExecutionConflict: o ``DELETE`` da linha de controle sem linha e o ``1023``, sem
        repetição.
    :raises ContractError: ``config`` sem ``workgroup`` e sem ``host``, ``user`` e ``password``.
    """
    connection = _Connection(config)
    try:
        _check_control_table(connection, config.schema)
        results = {}
        for table in tables:
            results[table.name] = _unpublish_table(connection, config, db.environment, table)
        return results
    finally:
        connection.close()


def publication_status(db: Database, config: RedshiftConfig) -> list[PublicationStatus]:
    """A versão publicada contra a atual de cada tabela do ambiente que existe no Delta, com as
    partições pendentes.

    Exemplo:

    .. code-block:: python

        for status in publication_status(db, config):
            print(status.table, status.published_version, status.current_version)

    :param db: o banco, com a raiz Delta e o ambiente, que prefixa o nome das tabelas publicadas.
    :param config: a configuração do Redshift, com a conexão e o esquema.
    :return: uma ``PublicationStatus`` por tabela, na ordem de ``db.tables()``; as partições
        pendentes são todas na tabela nunca publicada, as de ``version_diff`` nas outras.
    :raises PublicationError: sem a tabela de controle.
    :raises LogUnavailable: um arquivo do log entre a versão publicada e a atual não existe.
    :raises ContractError: ``config`` sem ``workgroup`` e sem ``host``, ``user`` e ``password``.
    """
    connection = _Connection(config)
    try:
        _check_control_table(connection, config.schema)
        existing = [table for table in db.tables() if delta.table_exists(db.uri(table), db.storage)]
        return [_table_status(connection, db, config, table) for table in existing]
    finally:
        connection.close()


def _table_status(connection: _Connection, db: Database, config: RedshiftConfig,
                  table: sa.Table) -> PublicationStatus:
    """A situação da publicação de uma tabela que existe no Delta: as partições pendentes são
    todas na tabela nunca publicada, e as de ``version_diff`` na publicada numa versão antiga."""
    uri = db.uri(table)
    current = delta.open_table(uri, db.storage).version()
    published = _read_control(connection, config.schema, db.environment, table)
    pending: list[str | None] = []
    if published is None or published < current:
        pending, _ = _partitions_to_publish(uri, table, published, current, db.storage)
    return PublicationStatus(_published_name(db.environment, table), published, current,
                             tuple(pending))
