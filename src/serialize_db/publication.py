"""[public] A publicação para clientes no Redshift: as tabelas ``<ambiente>_<tabela>`` do esquema,
derivadas do Delta, e a tabela de controle ``serialize_db_publications``.

A tabela de controle guarda a versão publicada de cada tabela. Ela é uma só para todos os
ambientes, e o usuário a cria uma vez no esquema, por ``create_publications_table`` ou por
``serialize-db publish_redshift --init``. O pipeline não a cria, e ``publish_redshift`` recusa
publicar sem ela, antes de qualquer escrita. As versões a publicar vêm de um snapshot do arquivo
de controle (a entrada com a versão de cada tabela do ambiente num instante), por
``serialize-db publish_redshift`` com ``--snapshot`` ou ``--channel``, ou são as versões atuais.

Cada tabela publica numa conexão própria, e as tabelas correm num pool. Antes da transação, e
fora de transação, a tabela publicada que já existe é reconciliada com o contrato. As colunas
novas são acrescentadas. Um diff destrutivo, como uma coluna removida ou um tipo que mudou,
despublica a tabela, e a transação a recria com todas as partições. A transação roda, em ordem:

1. ``BEGIN``.
2. A leitura da linha de controle, a linha da tabela na tabela de controle, que identifica a
   versão publicada e fixa o snapshot da transação. Sem linha, é a primeira publicação: a
   transação cria a tabela publicada e carrega todas as partições. Com linha, ``version_diff``
   entre a menor e a maior das duas versões, a lida e a pedida, dá as partições a trocar, o que
   também volta a tabela a um snapshot anterior ao publicado.
3. Por partição, o ``DELETE`` da partição e, quando a versão pedida a tem, a carga numa staging
   temporária (uma tabela de passagem, apagada no fim da transação). A carga usa um
   ``COPY ... MANIFEST`` por lista de colunas dos arquivos, com a lista, e é seguida do
   ``INSERT ... SELECT`` com o valor da partição. O manifesto é o arquivo JSON com a lista dos
   arquivos que o ``COPY`` carrega.
4. Por último, o ``INSERT`` da linha de controle, ou o ``UPDATE`` dela condicionado à versão lida.

As 0 linhas desse ``UPDATE``, o ``1023`` e a tabela publicada que outra primeira publicação criou
saem como ``ExecutionConflict``. Os manifestos do ``COPY`` ficam em
``<raiz>/<ambiente>/publicacao/<execution_id>/<tabela>/<valor>/``, e a publicação não os apaga.
``unpublish_redshift`` apaga a tabela publicada e a linha de controle numa transação.
``publication_status`` compara a versão publicada com a atual.

Cada ``COPY`` leva a cláusula de credenciais do motor Redshift, montada logo antes do seu
``execute`` com a chave que a cadeia de credenciais do ``boto3`` serve naquele momento. Nenhum
texto que carregue a cláusula vai a log. Todo comando cita a tabela publicada e a de controle por
nome em duas partes, ``"<esquema>"."<nome>"``, depois do ``USE`` que a conexão roda. A staging
temporária é citada pelo nome só, sem ``COMPUPDATE`` e sem ``TRUNCATE``. Um datashare aceita os
comandos nessa forma.

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
    "create_publications_table",
    "publication_status",
    "publish_redshift",
    "unpublish_redshift",
]

log = logging.getLogger("serialize_db.publication")
# O tempo da abertura de cada conexão e o de cada comando, em DEBUG, num logger à parte, que se liga
# sem o resto do log da publicação.
command_log = logging.getLogger("serialize_db.publication.commands")

CONTROL_TABLE = "serialize_db_publications"
"""[public] O nome da tabela de controle. As colunas dela: ``table_name``, ``delta_version``,
``execution_id`` e ``published_at``."""

# A instrução de inicialização, na mensagem de PublicationError.
_INIT = "crie-a uma vez com serialize-db publish_redshift --init (create_publications_table)"

# O lugar da cláusula de credenciais nos textos do COPY, que _run_publication preenche logo
# antes do execute de cada um.
_CREDENTIALS_MARKER = "<credenciais>"

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
    """[public] A situação da publicação de uma tabela, para o operador: a versão publicada contra a
    atual e as partições pendentes.

    Exemplo:

    .. code-block:: python

        publication_status(db, config)[0]
        # PublicationStatus(table="prd_cad_lancamentos", published_version=57,
        #                   current_version=58, pending_partitions=("2026-08-31",))
    """

    table: str
    """[public] O nome da tabela publicada, ``<ambiente>_<tabela>``."""
    published_version: int | None
    """[public] A versão do Delta que a tabela de controle registra, ou ``None`` na tabela nunca
    publicada."""
    current_version: int
    """[public] A versão atual do Delta."""
    pending_partitions: tuple[str | None, ...]
    """[public] As partições que a próxima publicação troca. Na tabela sem partição, que ela troca
    inteira, ``(None,)``."""


@dataclasses.dataclass(frozen=True)
class _PublishedColumn:
    """[private] Uma coluna de uma tabela publicada, como o catálogo ``svv_all_columns`` a lista.
    Serve a ``_reconcile_published``.

    Exemplo:

    .. code-block:: python

        _PublishedColumn("preco", "numeric", None, 18, 2)
    """

    name: str
    """[private] O nome da coluna, o ``column_name`` do catálogo."""
    data_type: str
    """[private] O tipo, o ``data_type`` do catálogo, como ``bigint``, ``numeric`` ou
    ``character varying``."""
    length: int | None
    """[private] A largura do texto, o ``character_maximum_length`` do catálogo, ou ``None`` quando
    o catálogo não a traz."""
    precision: int | None
    """[private] A precisão numérica, o ``numeric_precision`` do catálogo, ou ``None`` quando o
    catálogo não a traz."""
    scale: int | None
    """[private] A escala numérica, o ``numeric_scale`` do catálogo, ou ``None`` quando o catálogo
    não a traz."""


# ---------------------------------------------------------------- o texto dos comandos


def _published_name(
    environment: str,
    table: sa.Table,
) -> str:
    """[private] O nome da tabela publicada, ``<ambiente>_<tabela>``."""
    return f"{environment}_{table.name}"


def _qualified(
    schema: str,
    name: str,
) -> str:
    """[private] O nome em duas partes, ``"<esquema>"."<nome>"``, cada parte entre as aspas de
    ``quoted``."""
    return f"{quoted(schema)}.{quoted(name)}"


def _control_ddl(
    schema: str,
) -> str:
    """[private] O ``CREATE TABLE`` da tabela de controle, para ``create_publications_table``. O
    comando vai sem ``IF NOT EXISTS``: só o usuário o roda, uma vez, e a segunda chamada falha com
    a mensagem do servidor.

    Exemplo:

    .. code-block:: python

        _control_ddl("sbx_aco_decon")
        # CREATE TABLE "sbx_aco_decon"."serialize_db_publications" (table_name VARCHAR(127), ...)

    :param schema: o esquema do Redshift, o ``schema`` de ``RedshiftConfig``.
    :return: o texto do comando, com a tabela qualificada pelo esquema.
    """
    return (
        f"CREATE TABLE {_qualified(schema, CONTROL_TABLE)} (table_name VARCHAR(127), "
        "delta_version BIGINT, execution_id VARCHAR(127), published_at TIMESTAMP)"
    )


def _control_read(
    schema: str,
    environment: str,
    table: sa.Table,
) -> str:
    """[private] O ``select`` da linha de controle da tabela. ``publish_redshift`` e
    ``unpublish_redshift`` o rodam como o primeiro comando da transação; ``publication_status`` o
    roda fora de transação.

    Exemplo:

    .. code-block:: python

        _control_read("sbx_aco_decon", "prd", Lancamento.__table__)
        # SELECT delta_version FROM "sbx_aco_decon"."serialize_db_publications"
        # WHERE table_name = 'prd_cad_lancamentos'

    :param schema: o esquema do Redshift, o ``schema`` de ``RedshiftConfig``.
    :param environment: o ambiente, que prefixa o nome da tabela publicada,
        ``<ambiente>_<tabela>``.
    :param table: a tabela do modelo.
    :return: o texto da consulta, que devolve ``delta_version`` numa linha, ou nenhuma linha
        antes da primeira publicação.
    """
    return (
        f"SELECT delta_version FROM {_qualified(schema, CONTROL_TABLE)} "
        f"WHERE table_name = {literal(_published_name(environment, table))}"
    )


def _published_ddl(
    schema: str,
    environment: str,
    table: sa.Table,
) -> str:
    """[private] O ``CREATE TABLE`` da tabela publicada, para ``_publication_statements``: o DDL do
    contrato, a chave primária, que o Redshift trata como informativa, e as cláusulas físicas do
    modelo.

    Exemplo:

    .. code-block:: python

        print(_published_ddl("sbx_aco_decon", "prd", Lancamento.__table__))
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
    :raises serialize_db.errors.ContractError: a tabela com um tipo de coluna fora do contrato ou
        com mais de uma coluna de partição.
    """
    lines = []
    for column in table.columns:
        lines.append("    " + column_ddl(column, "redshift"))
    key = [quoted(column.name) for column in table.primary_key.columns]
    if key:
        lines.append(f"    PRIMARY KEY ({', '.join(key)})")
    name = _qualified(schema, _published_name(environment, table))
    return (
        f"CREATE TABLE {name} (\n"
        + ",\n".join(lines)
        + "\n)"
        + redshift_options(table_options(table))
    )


def _partition_delete(
    published: str,
    table: sa.Table,
    value: str | None,
) -> str:
    """[private] O ``DELETE`` da partição na tabela publicada. Numa tabela sem partição, o
    ``DELETE`` da tabela inteira."""
    partition_by = table_options(table).partition_by
    if partition_by is None:
        return f"DELETE FROM {published}"
    return f"DELETE FROM {published} WHERE {quoted(partition_by)} = {literal(value)}"


def _publication_statements(
    schema: str,
    environment: str,
    table: sa.Table,
    partitions: Sequence[str | None],
    manifests: Mapping[str | None, Sequence[delta.CopyManifest]],
    delta_version: int,
    published_version: int | None,
    execution_id: str,
    credentials: str,
) -> list[str]:
    """[private] Os comandos da transação depois da leitura da linha de controle, um por
    ``execute``, sem ``BEGIN`` e ``COMMIT``; para ``publish_redshift``.

    Na ordem:

    - na primeira publicação, o ``CREATE TABLE`` da tabela publicada;
    - a staging temporária, sem a coluna de partição;
    - por partição, o ``DELETE`` dela e, quando ela tem manifestos, o ``DELETE`` da staging, um
      ``COPY ... MANIFEST FILLRECORD`` por manifesto, com a lista das colunas dos arquivos dele, e
      o ``INSERT ... SELECT`` com o valor e ``JSON_PARSE``;
    - o ``DROP`` da staging;
    - por último, a linha de controle, pelo ``INSERT`` ou pelo ``UPDATE`` condicionado à versão
      lida.

    Exemplo:

    .. code-block:: python

        manifest = delta.CopyManifest("s3://bucket/prd/publicacao/exec-42/.../1.manifest",
                                      ("id_lancamento", "id_conta", "data_base", "valor"))
        _publication_statements("sbx_aco_decon", "prd", Lancamento.__table__, ["2026-08-31"],
                                {"2026-08-31": [manifest]}, 58, 57, "exec-42", "IAM_ROLE default")

    :param schema: o esquema do Redshift, o ``schema`` de ``RedshiftConfig``.
    :param environment: o ambiente, que prefixa o nome da tabela publicada,
        ``<ambiente>_<tabela>``.
    :param table: a tabela do modelo.
    :param partitions: as partições que a publicação troca, na ordem dos comandos. ``None`` é a
        tabela inteira, sem partição.
    :param manifests: os manifestos do ``COPY`` de cada partição com arquivo na versão, pelo
        valor, como ``delta.copy_manifest`` os devolve. Uma partição sem manifesto foi removida no
        Delta e recebe só o ``DELETE``.
    :param delta_version: a versão do Delta publicada, que a linha de controle grava.
    :param published_version: a versão lida na linha de controle, ou ``None`` na primeira
        publicação.
    :param execution_id: a execução que publica, que a linha de controle grava.
    :param credentials: a cláusula de credenciais, como ``IAM_ROLE default``, que os textos do
        ``COPY`` carregam.
    :return: os comandos, na ordem da transação.
    :raises serialize_db.errors.ContractError: a tabela com um tipo de coluna fora do contrato ou
        com mais de uma coluna de partição.
    """
    name = _published_name(environment, table)
    published = _qualified(schema, name)
    staging = quoted(f"{name}_staging")
    statements = []
    if published_version is None:
        statements.append(_published_ddl(schema, environment, table))
    statements.append(staging_ddl(table, staging, columns_without_partition(table), temporary=True))
    for value in partitions:
        statements.append(_partition_delete(published, table, value))
        if value not in manifests:
            continue
        statements.append(f"DELETE FROM {staging}")
        for manifest in manifests[value]:
            statements.append(
                copy_text(
                    staging, manifest.uri, credentials, manifest=True, columns=manifest.columns
                )
            )
        statements.append(insert_from_staging(published, staging, table, value))
    statements.append(f"DROP TABLE {staging}")
    control = _qualified(schema, CONTROL_TABLE)
    if published_version is None:
        statements.append(
            f"INSERT INTO {control} VALUES ({literal(name)}, {int(delta_version)}, "
            f"{literal(execution_id)}, getdate())"
        )
    else:
        statements.append(
            f"UPDATE {control} SET delta_version = {int(delta_version)}, "
            f"execution_id = {literal(execution_id)}, published_at = getdate() "
            f"WHERE table_name = {literal(name)} "
            f"AND delta_version = {int(published_version)}"
        )
    return statements


def _unpublication_statements(
    schema: str,
    environment: str,
    table: sa.Table,
    published_version: int,
) -> list[str]:
    """[private] Os comandos da despublicação depois da leitura da linha de controle: o
    ``DROP TABLE`` da tabela publicada e o ``DELETE`` da linha, condicionado à versão lida. Servem a
    ``unpublish_redshift`` e a ``publish_redshift``, que despublica a tabela publicada num diff
    destrutivo antes de recriá-la.

    Exemplo:

    .. code-block:: python

        _unpublication_statements("sbx_aco_decon", "prd", Lancamento.__table__, 58)

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


def _expected_column(
    column: sa.Column,
) -> _PublishedColumn:
    """[private] A coluna do contrato na forma em que ``svv_all_columns`` a listaria, com o tipo na
    família de ``_TYPE_FAMILIES``."""
    # O texto do tipo é o nome, com um número entre parênteses (a largura) ou dois (a precisão e
    # a escala): "BIGINT", "VARCHAR(20)", "DECIMAL(18, 2)".
    text = sql_type(column, "redshift")
    match = re.fullmatch(r"([A-Z ]+?)(?:\((\d+)(?:, (\d+))?\))?", text)
    type_name, first_number, second_number = match.groups()
    family = _TYPE_FAMILIES[type_name.lower()]
    if family in ("varchar", "char"):
        return _PublishedColumn(column.name, family, _optional_int(first_number), None, None)
    if family == "decimal":
        return _PublishedColumn(
            column.name, family, None, _optional_int(first_number), _optional_int(second_number)
        )
    return _PublishedColumn(column.name, family, None, None, None)


def _optional_int(
    value: object,
) -> int | None:
    """[private] O inteiro de um número lido como texto ou do catálogo, ou ``None`` quando não há
    valor."""
    if value is None:
        return None
    return int(value)


def _in_family(
    column: _PublishedColumn,
) -> _PublishedColumn:
    """[private] A coluna publicada com o tipo levado à família de ``_TYPE_FAMILIES``, para a
    comparação com a coluna do contrato."""
    family = _TYPE_FAMILIES.get(column.data_type.lower(), column.data_type.lower())
    if family in ("varchar", "char"):
        return dataclasses.replace(column, data_type=family, precision=None, scale=None)
    if family == "decimal":
        return dataclasses.replace(column, data_type=family, length=None)
    return _PublishedColumn(column.name, family, None, None, None)


def _reconcile_published(
    schema: str,
    environment: str,
    table: sa.Table,
    columns: Sequence[_PublishedColumn],
) -> tuple[list[str], list[str]]:
    """[private] O diff entre o contrato e a tabela publicada; para ``publish_redshift``.

    O diff aditivo é a coluna anulável nova. O diff destrutivo é a coluna removida, a coluna
    ``NOT NULL`` nova, o tipo que mudou e a largura de ``VARCHAR(n)``, a precisão ou a escala que
    mudaram. Essas mudanças são destrutivas porque o ``ALTER COLUMN ... TYPE`` é recusado no
    esquema do datashare.

    Exemplo:

    .. code-block:: python

        statements, destructive = _reconcile_published("sbx_aco_decon", "prd",
                                                       Lancamento.__table__, columns)

    :param schema: o esquema do Redshift, o ``schema`` de ``RedshiftConfig``.
    :param environment: o ambiente, que prefixa o nome da tabela publicada,
        ``<ambiente>_<tabela>``.
    :param table: a tabela do modelo.
    :param columns: as colunas da tabela publicada como ``svv_all_columns`` as lista, com nome,
        tipo, largura, precisão e escala, em ``_PublishedColumn``.
    :return: os ``ALTER TABLE ... ADD COLUMN`` do diff aditivo, que acrescentam cada coluna no fim
        da tabela, e uma frase por diferença destrutiva. Qualquer diferença destrutiva despublica a
        tabela, e a transação da mesma publicação a recria.
    :raises serialize_db.errors.ContractError: a tabela com um tipo de coluna fora do contrato.
    """
    published = _qualified(schema, _published_name(environment, table))
    existing = {column.name: _in_family(column) for column in columns}
    statements = []
    destructive = []
    for column in table.columns:
        expected = _expected_column(column)
        is_new = column.name not in existing
        if is_new and column.nullable:
            statements.append(
                f"ALTER TABLE {published} ADD COLUMN {column_ddl(column, 'redshift')}"
            )
        elif is_new:
            destructive.append(f"{column.name}: coluna NOT NULL nova")
        elif existing[column.name] != expected:
            destructive.append(
                f"{column.name}: {existing[column.name]} na tabela publicada, {expected} no modelo"
            )
    for name in existing:
        if name not in table.c:
            destructive.append(f"{name}: removida do modelo")
    return statements, destructive


# ---------------------------------------------------------------- a conexão


class _Connection:
    """[private] Uma conexão da publicação, aberta por ``connect`` do motor Redshift: um cursor por
    comando, o erro do servidor com o comando mascarado numa nota, e o ``BEGIN``, o ``COMMIT`` e o
    ``ROLLBACK`` explícitos. O log ``serialize_db.publication.commands`` dá em ``DEBUG`` o tempo
    da abertura da conexão e o de cada comando, com o texto mascarado por ``mask``."""

    def __init__(
        self,
        config: RedshiftConfig,
    ) -> None:
        self.config = config
        started = time.perf_counter()
        self.connection = connect(config)
        command_log.debug("%s em %.3f s", "conexão", time.perf_counter() - started)

    def execute(
        self,
        text: str,
    ) -> object:
        """[private] Roda um comando num cursor novo e devolve o cursor."""
        cursor = self.connection.cursor()
        started = time.perf_counter()
        try:
            cursor.execute(text)
        except redshift_connector.Error as error:
            error.add_note(f"comando: {mask(text)}")
            raise
        finally:
            command_log.debug("%s em %.3f s", mask(text), time.perf_counter() - started)
        return cursor

    def rows(
        self,
        text: str,
    ) -> list:
        """[private] As linhas de uma consulta."""
        return list(self.execute(text).fetchall())

    def rollback(self) -> None:
        """[private] Roda o ``ROLLBACK``. Um erro do próprio ``ROLLBACK`` vai ao log como aviso e
        não esconde o erro que o causou."""
        try:
            self.execute("ROLLBACK")
        except redshift_connector.Error as error:
            log.warning("ROLLBACK recusado: %s", error)

    def close(self) -> None:
        self.connection.close()


def _check_control_table(
    connection: _Connection,
    schema: str,
) -> None:
    """[private] Confere que a tabela de controle existe, por ``select 1 ... limit 0`` fora de
    transação. Sem ela, ``PublicationError`` com a instrução de inicialização."""
    try:
        connection.execute(f"SELECT 1 FROM {_qualified(schema, CONTROL_TABLE)} LIMIT 0")
    except redshift_connector.Error as error:
        if relation_missing(error):
            raise PublicationError(
                f"a tabela de controle {schema}.{CONTROL_TABLE} não existe; {_INIT}"
            ) from None
        raise


def _read_control(
    connection: _Connection,
    schema: str,
    environment: str,
    table: sa.Table,
) -> int | None:
    """[private] A versão publicada da tabela na linha de controle, ou ``None`` sem linha."""
    rows = connection.rows(_control_read(schema, environment, table))
    return int(rows[0][0]) if rows else None


def _published_columns(
    connection: _Connection,
    config: RedshiftConfig,
    environment: str,
    table: sa.Table,
) -> list[_PublishedColumn]:
    """[private] As colunas da tabela publicada, lidas em ``svv_all_columns`` na ordem das posições.
    A lista é vazia quando a tabela não existe. Quando a tabela existe e o catálogo não lista as
    colunas, a lista também é vazia, e o log ``serialize_db.publication`` avisa que a
    reconciliação não rodou."""
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
        f"AND table_name = {literal(name)} ORDER BY ordinal_position"
    )
    columns = []
    for column_name, data_type, length, precision, scale in rows:
        columns.append(
            _PublishedColumn(
                str(column_name),
                str(data_type),
                _optional_int(length),
                _optional_int(precision),
                _optional_int(scale),
            )
        )
    if not columns:
        log.warning(
            "%s existe, e svv_all_columns não lista as colunas dela: a reconciliação não rodou",
            name,
        )
    return columns


# ---------------------------------------------------------------- a publicação


def _conflict(
    error: BaseException,
    table: sa.Table,
) -> ExecutionConflict | None:
    """[private] O ``ExecutionConflict`` de um erro do servidor que é conflito entre publicações: o
    ``1023``, a violação de isolamento serializável, e a tabela publicada que outra primeira
    publicação criou. ``None`` nos demais erros."""
    if serialization_failure(error):
        return ExecutionConflict(
            f"{table.name}: outra publicação gravou a tabela ao mesmo tempo (1023): {error}"
        )
    if relation_exists(error):
        return ExecutionConflict(
            f"{table.name}: outra primeira publicação criou a tabela publicada: {error}"
        )
    return None


def _unpublish_table(
    connection: _Connection,
    config: RedshiftConfig,
    environment: str,
    table: sa.Table,
) -> int | None:
    """[private] A transação da despublicação de uma tabela: lê a linha de controle, roda o
    ``DROP TABLE`` e o ``DELETE`` da linha. Devolve a versão que estava publicada, ou ``None`` sem
    linha. O ``DELETE`` que não acha a linha é ``ExecutionConflict``."""
    connection.execute("BEGIN")
    try:
        published = _read_control(connection, config.schema, environment, table)
        if published is None:
            connection.rollback()
            return None
        drop, delete_control = _unpublication_statements(
            config.schema, environment, table, published
        )
        connection.execute(drop)
        deleted = connection.execute(delete_control)
        if deleted.rowcount == 0:
            raise ExecutionConflict(
                f"{table.name}: a linha de controle mudou desde a leitura (versão lida {published})"
            )
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


def _reconcile(
    connection: _Connection,
    config: RedshiftConfig,
    environment: str,
    table: sa.Table,
) -> None:
    """[private] Reconcilia a tabela publicada com o contrato, fora de transação: acrescenta as
    colunas novas. Num diff destrutivo, despublica a tabela, com um aviso no log, e a transação da
    mesma publicação a recria. Sem tabela publicada, não faz nada."""
    columns = _published_columns(connection, config, environment, table)
    if not columns:
        return
    statements, destructive = _reconcile_published(config.schema, environment, table, columns)
    if destructive:
        log.warning(
            "%s: diff destrutivo na tabela publicada (%s); a tabela é despublicada e "
            "recriada com todas as partições",
            table.name,
            "; ".join(destructive),
        )
        _unpublish_table(connection, config, environment, table)
        return
    for statement in statements:
        connection.execute(statement)


def _partitions_to_publish(
    uri: str,
    table: sa.Table,
    published: int | None,
    version: int,
    storage: Storage,
) -> tuple[list[str | None], list[str | None]]:
    """[private] As partições que a publicação troca e, entre elas, as que têm arquivo na versão
    pedida. Na primeira publicação, todas. Nas seguintes, as de ``version_diff`` entre a menor e a
    maior das duas versões, o que também serve à volta a uma versão anterior à publicada."""
    partition_by = table_options(table).partition_by
    available = delta.partition_values(delta.open_table(uri, storage, version), partition_by)
    if published is None:
        return list(available), list(available)
    changed = sorted(
        delta.version_diff(uri, min(published, version), max(published, version), table, storage),
        key=str,
    )
    return changed, [value for value in changed if value in available]


def _write_manifests(
    delta_db: Database,
    table: sa.Table,
    values: Sequence[str | None],
    execution_id: str,
    version: int,
) -> dict[str | None, list[delta.CopyManifest]]:
    """[private] Grava os manifestos do ``COPY`` de cada partição com arquivo, um por lista de
    colunas dos arquivos, em ``<ambiente>/publicacao/<execution_id>/<tabela>/<valor>/``, e os
    devolve pelo valor da partição. Na tabela sem partição, a última pasta se chama ``tabela``."""
    storage = delta_db.storage
    uri = delta_db.uri(table)
    manifests = {}
    for value in values:
        tag = value if value is not None else "tabela"
        folder = storage.join(delta_db.publication_prefix(execution_id), table.name, tag)
        partitions = [value] if value is not None else None
        manifests[value] = delta.copy_manifest(
            uri, version, partitions, storage.uri_of(folder), storage
        )
    return manifests


def _run_publication(
    connection: _Connection,
    config: RedshiftConfig,
    table: sa.Table,
    statements: Sequence[str],
    published: int | None,
) -> None:
    """[private] Roda os comandos da transação, um por ``execute``. Cada ``COPY`` recebe a cláusula
    de credenciais montada logo antes dele. O último comando grava a linha de controle; o
    ``UPDATE`` dela que não afeta linha é ``ExecutionConflict``."""
    *changes, control_statement = statements
    for statement in changes:
        # Uma cláusula nova por COPY: a credencial temporária da cadeia vence em até uma hora, e
        # a transação de uma tabela grande pode durar mais que isso.
        if _CREDENTIALS_MARKER in statement:
            credentials = credentials_clause(config)
            statement = statement.replace(_CREDENTIALS_MARKER, credentials)
        connection.execute(statement)
    control = connection.execute(control_statement)
    if published is not None and control.rowcount == 0:
        raise ExecutionConflict(
            f"{table.name}: a linha de controle mudou desde a leitura (versão lida {published})"
        )


def _publication_transaction(
    connection: _Connection,
    delta_db: Database,
    config: RedshiftConfig,
    table: sa.Table,
    execution_id: str,
    version: int,
) -> list[str | None] | None:
    """[private] A transação da publicação de uma tabela: lê a linha de controle, troca as
    partições e grava a linha. Devolve as partições trocadas, ou ``None`` quando a versão pedida
    já está publicada e nada muda."""
    environment = delta_db.environment
    connection.execute("BEGIN")
    try:
        published = _read_control(connection, config.schema, environment, table)
        if published == version:
            connection.rollback()
            return None
        changed, with_files = _partitions_to_publish(
            delta_db.uri(table), table, published, version, delta_db.storage
        )
        manifests = _write_manifests(delta_db, table, with_files, execution_id, version)
        statements = _publication_statements(
            config.schema,
            environment,
            table,
            changed,
            manifests,
            version,
            published,
            execution_id,
            _CREDENTIALS_MARKER,
        )
        _run_publication(connection, config, table, statements, published)
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


def _publish_table(
    delta_db: Database,
    config: RedshiftConfig,
    table: sa.Table,
    execution_id: str,
    version: int,
) -> int:
    """[private] Publica uma tabela numa conexão própria: a reconciliação e depois a transação. A
    linha de log da tabela publicada traz as partições, o tempo e o pico de RSS do processo."""
    started = time.perf_counter()
    connection = _Connection(config)
    try:
        _reconcile(connection, config, delta_db.environment, table)
        changed = _publication_transaction(
            connection, delta_db, config, table, execution_id, version
        )
    finally:
        connection.close()
    if changed is None:
        log.info("%s: a versão %s já está publicada", table.name, version)
        return version
    log.info(
        "%s publicada na versão %s: partições %s, em %.1f s; RSS máximo do processo %.0f MB",
        table.name,
        version,
        changed,
        time.perf_counter() - started,
        peak_rss_mb(),
    )
    return version


def _version_to_publish(
    delta_db: Database,
    table: sa.Table,
    versions: Mapping[str, int] | None,
) -> int:
    """[private] A versão do Delta que a publicação grava: a de ``versions`` ou, sem ``versions``, a
    atual. A tabela sem versão, fora de ``versions`` ou inexistente no ambiente, é
    ``PublicationError``."""
    if versions is not None:
        version = versions.get(table.name)
        if version is None:
            raise PublicationError(
                f"{table.name}: a tabela está fora das versões pedidas, e não há o que publicar"
            )
        return version
    uri = delta_db.uri(table)
    if not delta.table_exists(uri, delta_db.storage):
        raise PublicationError(
            f"{table.name}: a tabela não existe no ambiente {delta_db.environment}, "
            "e não há o que publicar"
        )
    return delta.open_table(uri, delta_db.storage).version()


def create_publications_table(
    config: RedshiftConfig,
) -> None:
    """[public] Cria a tabela de controle ``serialize_db_publications`` no esquema, uma vez, pelo
    usuário.

    ``serialize-db publish_redshift --init`` faz o mesmo na linha de comando.

    Exemplo:

    .. code-block:: python

        create_publications_table(RedshiftConfig.from_environment())

    :param config: a configuração do Redshift, com a conexão e o esquema.
    :raises redshift_connector.Error: a segunda chamada falha com a mensagem do servidor, porque a
        tabela já existe.
    :raises serialize_db.errors.ContractError: ``config`` sem conexão possível, isto é, sem
        ``workgroup`` e sem ``host``, ``user`` e ``password``.
    """
    connection = _Connection(config)
    try:
        connection.execute(_control_ddl(config.schema))
    finally:
        connection.close()


def publish_redshift(
    delta_db: Database,
    config: RedshiftConfig,
    tables: Sequence[sa.Table],
    execution_id: str,
    max_workers: int = 1,
    versions: Mapping[str, int] | None = None,
) -> dict[str, int]:
    """[public] Publica as tabelas Delta do ambiente nas tabelas ``<ambiente>_<tabela>`` do esquema
    do Redshift.

    A função confere a tabela de controle antes de tudo. Depois, por tabela, numa conexão própria
    do pool, roda a reconciliação da tabela publicada que já existe e a transação da publicação.
    Uma tabela cuja versão publicada é a pedida não muda. Uma versão anterior à publicada volta a
    tabela a ela: as partições alteradas entre as duas versões são trocadas, e a partição que só a
    versão publicada tem sai pelo ``DELETE``. Na primeira falha nada novo começa, o que está em
    curso termina, e a exceção leva o resultado de cada tabela numa nota. Cada tabela publicada vai
    ao log ``serialize_db.publication`` com as partições, o tempo e o pico de memória residente do
    processo.

    Exemplo:

    .. code-block:: python

        publish_redshift(db, config, [Lancamento.__table__], "exec-2026-09-05")
        # {"cad_lancamentos": 58}

    :param delta_db: o banco Delta de origem, com a raiz e o ambiente, que prefixa o nome das
        tabelas publicadas.
    :param config: a configuração do Redshift de destino, com a conexão, o esquema das tabelas
        publicadas e o ``iam_role`` do ``COPY``.
    :param tables: as tabelas do modelo a publicar.
    :param execution_id: a execução que publica, que a linha de controle grava. Os manifestos do
        ``COPY`` ficam em ``<ambiente>/publicacao/<execution_id>/``, sob a raiz, e a publicação
        não os apaga.
    :param max_workers: o tamanho do pool, quantas tabelas publicam ao mesmo tempo. O padrão 1
        publica uma por vez. Com mais de um, a primeira publicação de uma tabela pode esperar
        cerca de 20 s no ``INSERT`` da linha de controle. A espera apareceu em 14 das 27
        repetições das sondas de 2026-10-05 a 2026-10-14, só nas publicações cuja transação criou
        a tabela. O usuário aceitou a espera em 2026-10-10 (``docs/index.md``, seção
        "Multithreading").
    :param versions: a versão do Delta por nome de tabela, as de um snapshot do arquivo de
        controle (``delta.snapshot_versions``). ``None`` publica a versão atual de cada tabela.
    :return: ``{tabela: versão publicada}``.
    :raises serialize_db.errors.PublicationError: antes de qualquer escrita, sem a tabela de
        controle ou com uma tabela sem versão. A tabela sem versão é a que está fora de
        ``versions`` ou, sem ``versions``, fora do Delta do ambiente.
    :raises serialize_db.errors.ExecutionConflict: o ``UPDATE`` da linha de controle que não acha a
        linha (ou o ``DELETE`` dela, na despublicação de um diff destrutivo), o ``1023`` e a tabela
        publicada que outra primeira publicação criou. A função não tenta de novo.
    :raises serialize_db.errors.LogUnavailable: um arquivo do log entre a versão publicada e a
        pedida não existe.
    :raises serialize_db.errors.SandboxError: sem ``iam_role`` em ``config`` e sem credenciais da
        AWS na sessão ``boto3``, para o ``COPY``.
    :raises serialize_db.errors.ContractError: ``config`` sem conexão possível, isto é, sem
        ``workgroup`` e sem ``host``, ``user`` e ``password``.
    """
    connection = _Connection(config)
    try:
        _check_control_table(connection, config.schema)
    finally:
        connection.close()
    tasks = []
    for table in tables:
        version = _version_to_publish(delta_db, table, versions)
        task = functools.partial(_publish_table, delta_db, config, table, execution_id, version)
        tasks.append((table.name, task))
    return run_in_pool(tasks, max_workers)


def unpublish_redshift(
    db: Database,
    config: RedshiftConfig,
    tables: Sequence[sa.Table],
) -> dict[str, int | None]:
    """[public] Despublica as tabelas do Redshift. Por tabela, uma transação roda o ``DROP TABLE``
    da tabela publicada e o ``DELETE`` da linha de controle. O Delta fica intacto.

    As tabelas são despublicadas uma por vez, numa só conexão.

    Exemplo:

    .. code-block:: python

        unpublish_redshift(db, config, [Lancamento.__table__])   # {"cad_lancamentos": 58}

    :param db: o banco, com a raiz Delta e o ambiente, que prefixa o nome das tabelas publicadas.
    :param config: a configuração do Redshift, com a conexão e o esquema.
    :param tables: as tabelas do modelo a despublicar.
    :return: ``{tabela: versão que estava publicada}``, com ``None`` na tabela que não estava.
    :raises serialize_db.errors.PublicationError: antes de qualquer escrita, sem a tabela de
        controle.
    :raises serialize_db.errors.ExecutionConflict: o ``DELETE`` da linha de controle que não acha a
        linha e o ``1023``. A função não tenta de novo.
    :raises serialize_db.errors.ContractError: ``config`` sem conexão possível, isto é, sem
        ``workgroup`` e sem ``host``, ``user`` e ``password``.
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


def publication_status(
    delta_db: Database,
    config: RedshiftConfig,
) -> list[PublicationStatus]:
    """[public] A situação da publicação de cada tabela do ambiente que existe no Delta: a versão
    publicada no Redshift contra a atual no Delta, com as partições pendentes.

    Exemplo:

    .. code-block:: python

        for status in publication_status(db, config):
            print(status.table, status.published_version, status.current_version)

    :param delta_db: o banco Delta, com a raiz e o ambiente, que prefixa o nome das tabelas
        publicadas.
    :param config: a configuração do Redshift, com a conexão e o esquema.
    :return: uma ``PublicationStatus`` por tabela, na ordem de ``delta_db.tables()``. As
        partições pendentes são todas na tabela nunca publicada e as de ``version_diff`` nas
        outras.
    :raises serialize_db.errors.PublicationError: sem a tabela de controle.
    :raises serialize_db.errors.LogUnavailable: um arquivo do log entre a versão publicada e a atual
        não existe.
    :raises serialize_db.errors.ContractError: ``config`` sem conexão possível, isto é, sem
        ``workgroup`` e sem ``host``, ``user`` e ``password``.
    """
    connection = _Connection(config)
    try:
        _check_control_table(connection, config.schema)
        existing = [
            table
            for table in delta_db.tables()
            if delta.table_exists(delta_db.uri(table), delta_db.storage)
        ]
        return [_table_status(connection, delta_db, config, table) for table in existing]
    finally:
        connection.close()


def _table_status(
    connection: _Connection,
    delta_db: Database,
    config: RedshiftConfig,
    table: sa.Table,
) -> PublicationStatus:
    """[private] A situação da publicação de uma tabela que existe no Delta. As partições pendentes
    são todas na tabela nunca publicada, e as de ``version_diff`` na tabela publicada numa versão
    anterior à atual. Nas outras, nenhuma."""
    uri = delta_db.uri(table)
    current = delta.open_table(uri, delta_db.storage).version()
    published = _read_control(connection, config.schema, delta_db.environment, table)
    pending: list[str | None] = []
    if published is None or published < current:
        pending, _ = _partitions_to_publish(uri, table, published, current, delta_db.storage)
    return PublicationStatus(
        _published_name(delta_db.environment, table), published, current, tuple(pending)
    )
