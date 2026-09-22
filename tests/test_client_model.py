"""O modelo cliente contra o modelo de referência: a cópia difere do original só pelas correções.

A cópia é ``tests/client_model/``; o original, ``tests/reference_model/``; as correções estão em
``plan/PLAN-STAGE-1.md``.
Cada teste confere uma correção e que nada mais mudou: as tabelas e as colunas na mesma ordem,
com a coluna de partição no fim das quatro tabelas particionadas; ``BigInteger`` só nas chaves e
nas colunas que as referenciam, e ``String(n)`` só onde havia ``String``; a nulidade; as chaves
primárias, únicas e estrangeiras, sem ``DEFERRABLE`` e sem ``autoincrement``, com os dois índices
únicos compostos do original como ``UniqueConstraint``, porque as chaves estrangeiras compostas os
apontam; os comentários; e ``Table.info["serialize_db"]`` com a partição que a base tem. O modelo
inteiro é criado por ``create_all`` num ``sqlalchemy.Connection`` do DuckDB criado fora da
biblioteca.
"""

from __future__ import annotations

import sqlalchemy as sa

import source_db_projetado as source
from client_model import Base as ClientBase
from reference_model.model_db_projetado import Base as ReferenceBase


def reference_table(name: str) -> sa.Table:
    """A tabela ``name`` do modelo de referência."""
    return ReferenceBase.metadata.tables[name]


def partition_column(name: str) -> str | None:
    """A coluna de partição da tabela ``name`` na base; ``None`` numa tabela sem partição."""
    if name not in source.PARTITIONS:
        return None
    return source.PARTITIONS[name].column


def references_a_primary_key(column: sa.Column) -> bool:
    """Se a coluna é chave primária ou aponta, por chave estrangeira simples, para uma."""
    if column.primary_key:
        return True
    for foreign_key in column.foreign_keys:
        if foreign_key.column.primary_key:
            return True
    return False


def unique_constraints(table: sa.Table) -> set[tuple[str, ...]]:
    """As ``UniqueConstraint`` da tabela, como tuplas de colunas."""
    keys = set()
    for constraint in table.constraints:
        if isinstance(constraint, sa.UniqueConstraint):
            keys.add(tuple(column.name for column in constraint.columns))
    return keys


def unique_indexes(table: sa.Table) -> set[tuple[str, ...]]:
    """Os índices únicos da tabela, como tuplas de colunas."""
    keys = set()
    for index in table.indexes:
        if index.unique:
            keys.add(tuple(column.name for column in index.columns))
    return keys


def unique_keys(table: sa.Table) -> set[tuple[str, ...]]:
    """As ``UniqueConstraint`` e os índices únicos da tabela, como tuplas de colunas."""
    return unique_constraints(table) | unique_indexes(table)


def foreign_keys(table: sa.Table) -> set[tuple[tuple[str, ...], str, tuple[str, ...]]]:
    """As chaves estrangeiras da tabela: colunas, tabela referenciada e colunas referenciadas."""
    keys = set()
    for constraint in table.foreign_key_constraints:
        columns = tuple(element.parent.name for element in constraint.elements)
        referenced = tuple(element.column.name for element in constraint.elements)
        keys.add((columns, constraint.referred_table.name, referenced))
    return keys


def test_the_tables_and_columns_are_the_references_with_the_partition_column_last() -> None:
    """As 12 tabelas do original, cada coluna na mesma posição, e a coluna de partição no fim."""
    assert sorted(ClientBase.metadata.tables) == sorted(ReferenceBase.metadata.tables)
    for name, client in ClientBase.metadata.tables.items():
        expected = [column.name for column in reference_table(name).columns]
        if partition_column(name) is not None:
            expected.append(partition_column(name))
        assert [column.name for column in client.columns] == expected, name


def test_types_and_nullability_change_only_where_the_plan_says() -> None:
    """``Integer`` vira ``BigInteger`` só nas chaves e ``String`` ganha ``n``; o resto fica igual.

    A nulidade de cada coluna é a do original.
    """
    for name, client in ClientBase.metadata.tables.items():
        for reference_column in reference_table(name).columns:
            column = client.c[reference_column.name]
            assert column.nullable == reference_column.nullable, (name, column.name)
            if isinstance(reference_column.type, sa.Integer):
                if references_a_primary_key(reference_column):
                    assert type(column.type) is sa.BigInteger, (name, column.name)
                else:
                    assert type(column.type) is sa.Integer, (name, column.name)
            elif isinstance(reference_column.type, sa.String):
                assert type(column.type) is sa.String, (name, column.name)
                assert column.type.length, (name, column.name)
            else:
                assert type(column.type) is type(reference_column.type), (name, column.name)


# A chave estrangeira do original que saiu do modelo cliente: o destino não é único, porque o
# contrato está em N operações (decisão do usuário de 2026-09-21).
REMOVED_FOREIGN_KEY = (("data", "sistema", "contrato"), "rel_contrato_operacao", ("data", "sistema", "contrato"))


def test_keys_are_the_references_without_deferrable_and_without_autoincrement() -> None:
    """As chaves do original sem ``DEFERRABLE`` nem ``autoincrement``, menos a que apontava para
    colunas não únicas; nenhum índice, porque os únicos viraram ``UniqueConstraint``."""
    assert REMOVED_FOREIGN_KEY in foreign_keys(reference_table("cad_contratos"))
    for name, client in ClientBase.metadata.tables.items():
        reference = reference_table(name)
        client_primary = [column.name for column in client.primary_key.columns]
        reference_primary = [column.name for column in reference.primary_key.columns]
        assert client_primary == reference_primary, name
        assert unique_keys(client) == unique_keys(reference), name
        assert foreign_keys(client) == foreign_keys(reference) - {REMOVED_FOREIGN_KEY}, name
        for constraint in client.foreign_key_constraints:
            assert constraint.deferrable is None, (name, constraint.name)
            assert constraint.initially is None, (name, constraint.name)
        for column in client.primary_key.columns:
            assert column.autoincrement is False, (name, column.name)
        assert not client.indexes, name


# Os índices únicos do original que viraram UniqueConstraint na cópia: as chaves estrangeiras
# compostas apontam estas colunas, e o DuckDB e o Redshift exigem chave primária ou UNIQUE no alvo
# (decisão do usuário de 2026-09-22).
UNIQUE_CONSTRAINTS_FROM_INDEXES = {
    "cad_operacoes": ("data", "operacao"),
    "cad_contratos": ("data", "sistema", "contrato"),
}


def test_the_composite_foreign_key_targets_are_unique_constraints() -> None:
    """Os dois índices únicos do original são ``UniqueConstraint`` na cópia, e o modelo inteiro é
    criado por ``create_all`` num ``sqlalchemy.Connection`` do DuckDB criado fora da biblioteca,
    que recusava o índice único como alvo de chave estrangeira (leitura de 2026-09-22)."""
    for name, columns in UNIQUE_CONSTRAINTS_FROM_INDEXES.items():
        assert columns in unique_indexes(reference_table(name)), name
        assert columns in unique_constraints(ClientBase.metadata.tables[name]), name
    engine = sa.create_engine("duckdb:///:memory:")
    with engine.begin() as connection:
        ClientBase.metadata.create_all(connection)
        rows = connection.execute(sa.text("SELECT table_name FROM duckdb_tables()")).fetchall()
    engine.dispose()
    assert sorted(row[0] for row in rows) == sorted(ClientBase.metadata.tables)


def test_every_table_and_column_has_a_comment() -> None:
    """O comentário de cada tabela e de cada coluna, que o DDL e o esquema Arrow carregam."""
    for name, table in ClientBase.metadata.tables.items():
        assert table.comment, name
        for column in table.columns:
            assert column.comment, (name, column.name)


def test_the_partitioned_tables_declare_the_partition_the_base_has() -> None:
    """A partição declarada é a da base.

    ``partition_by`` e ``partition_source`` como a leitura mostrou, a coluna ``String(10)``
    obrigatória no fim, e a ``sort_key`` com colunas da tabela.
    """
    for name, table in ClientBase.metadata.tables.items():
        options = table.info.get("serialize_db", {})
        if partition_column(name) is None:
            assert "partition_by" not in options, name
            continue
        partition = source.PARTITIONS[name]
        assert options["partition_by"] == [partition.column], name
        assert options["partition_source"] == partition.source, name
        last = list(table.columns)[-1]
        assert last.name == partition.column, name
        assert last.type.length == 10 and not last.nullable, name
        assert set(options["sort_key"]) <= set(table.columns.keys()), name
