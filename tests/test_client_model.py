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

REFERENCE_TABLES = ReferenceBase.metadata.tables


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


def expected_type(reference_column: sa.Column) -> type:
    """O tipo que a cópia declara para a coluna do original: ``BigInteger`` na chave inteira e na
    coluna que aponta para uma, e o mesmo tipo nas demais."""
    is_integer = isinstance(reference_column.type, sa.Integer)
    if is_integer and references_a_primary_key(reference_column):
        return sa.BigInteger
    return type(reference_column.type)


def column_pairs() -> list[tuple[str, sa.Column, sa.Column]]:
    """Cada coluna do original ao lado da mesma coluna na cópia, com o nome da tabela."""
    pairs = []
    for name, client_table in ClientBase.metadata.tables.items():
        for reference_column in REFERENCE_TABLES[name].columns:
            pairs.append((name, reference_column, client_table.c[reference_column.name]))
    return pairs


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
    assert sorted(ClientBase.metadata.tables) == sorted(REFERENCE_TABLES)
    for name, client_table in ClientBase.metadata.tables.items():
        expected = [column.name for column in REFERENCE_TABLES[name].columns]
        partition = partition_column(name)
        if partition is not None:
            expected.append(partition)
        assert [column.name for column in client_table.columns] == expected, name


def test_types_and_nullability_change_only_where_the_plan_says() -> None:
    """``Integer`` vira ``BigInteger`` só nas chaves e ``String`` ganha ``n``; o resto fica igual.

    A nulidade de cada coluna é a do original.
    """
    for name, reference_column, client_column in column_pairs():
        where = (name, client_column.name)
        assert client_column.nullable == reference_column.nullable, where
        assert type(client_column.type) is expected_type(reference_column), where
        if isinstance(reference_column.type, sa.String):
            assert client_column.type.length, where


# A chave estrangeira do original que o modelo cliente não tem: o destino não é único, porque o
# contrato está em N operações.
REMOVED_FOREIGN_KEY = (
    ("data", "sistema", "contrato"),
    "rel_contrato_operacao",
    ("data", "sistema", "contrato"),
)


def test_keys_are_the_references_without_deferrable_and_without_autoincrement() -> None:
    """As chaves do original sem ``DEFERRABLE`` nem ``autoincrement``, menos a que apontava para
    colunas não únicas; nenhum índice, porque a cópia declara os únicos como
    ``UniqueConstraint``."""
    assert REMOVED_FOREIGN_KEY in foreign_keys(REFERENCE_TABLES["cad_contratos"])
    for name, client_table in ClientBase.metadata.tables.items():
        reference_table = REFERENCE_TABLES[name]
        client_primary = [column.name for column in client_table.primary_key.columns]
        reference_primary = [column.name for column in reference_table.primary_key.columns]
        assert client_primary == reference_primary, name
        assert unique_keys(client_table) == unique_keys(reference_table), name
        kept_foreign_keys = foreign_keys(reference_table) - {REMOVED_FOREIGN_KEY}
        assert foreign_keys(client_table) == kept_foreign_keys, name
        for constraint in client_table.foreign_key_constraints:
            assert constraint.deferrable is None, (name, constraint.name)
            assert constraint.initially is None, (name, constraint.name)
        for column in client_table.primary_key.columns:
            assert column.autoincrement is False, (name, column.name)
        assert not client_table.indexes, name


# Os índices únicos do original que a cópia declara como UniqueConstraint: as chaves estrangeiras
# compostas apontam estas colunas, e o DuckDB e o Redshift exigem chave primária ou UNIQUE no alvo.
UNIQUE_CONSTRAINTS_FROM_INDEXES = {
    "cad_operacoes": ("data", "operacao"),
    "cad_contratos": ("data", "sistema", "contrato"),
}


def test_the_composite_foreign_key_targets_are_unique_constraints() -> None:
    """Os dois índices únicos do original são ``UniqueConstraint`` na cópia, e o modelo inteiro é
    criado por ``create_all`` num ``sqlalchemy.Connection`` do DuckDB criado fora da biblioteca,
    que recusa o índice único como alvo de chave estrangeira."""
    for name, columns in UNIQUE_CONSTRAINTS_FROM_INDEXES.items():
        assert columns in unique_indexes(REFERENCE_TABLES[name]), name
        assert columns in unique_constraints(ClientBase.metadata.tables[name]), name

    # O DuckDB cria o modelo inteiro.
    engine = sa.create_engine("duckdb:///:memory:")
    query = sa.text("SELECT table_name FROM duckdb_tables()")
    with engine.begin() as connection:
        ClientBase.metadata.create_all(connection)
        created = connection.execute(query).scalars().all()
    engine.dispose()
    assert sorted(created) == sorted(ClientBase.metadata.tables)


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
        last_column = list(table.columns)[-1]
        assert last_column.name == partition.column, name
        assert last_column.type.length == 10, name
        assert not last_column.nullable, name
        assert set(options["sort_key"]) <= set(table.columns.keys()), name
