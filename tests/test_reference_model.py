"""O modelo de referência contra a leitura da base de origem.

``tests/reference_model/`` é o modelo SQLAlchemy da base original em Parquet particionado, que fica
como está. ``probes/parquet_source.py`` leu a base de
desenvolvimento em 2026-09-20 e a de produção em 2026-09-21 com a mesma seção 3, que
``source_db_projetado.SCHEMAS`` transcreve e ``test_source_db_projetado.py`` confere nos arquivos
gravados; aqui o modelo é lido pelo SQLAlchemy e comparado com ela coluna a coluna, e as chaves que
``source_db_projetado`` transcreve são conferidas contra as do modelo. Os módulos do modelo
importam ``lib_base_contabil`` e ``lib_base_gerencial``, a biblioteca do pipeline;
``tests/lib_base_contabil.py`` e ``tests/lib_base_gerencial.py`` fazem esses nomes apontarem para os
arquivos do modelo.
"""

from __future__ import annotations

import pyarrow as pa
import sqlalchemy as sa

import source_db_projetado as source
from reference_model.model_db_projetado import Base as ReferenceBase

# O tipo Arrow que cada tipo SQLAlchemy do modelo tem nos arquivos: a tabela de tipos de
# plan/schema.md, com o timestamp gravado em INT96 e lido como nanossegundos.
ARROW_TYPES = (
    (sa.Integer, pa.int32()),
    (sa.String, pa.string()),
    (sa.Date, pa.date32()),
    (sa.Double, pa.float64()),
    (sa.Boolean, pa.bool_()),
    (sa.DateTime, pa.timestamp("ns")),
)


def arrow_type_in_files(column: sa.Column) -> pa.DataType:
    """O tipo Arrow da coluna do modelo nos arquivos, pela tabela ``ARROW_TYPES``."""
    for sa_type, arrow in ARROW_TYPES:
        if isinstance(column.type, sa_type):
            return arrow
    raise AssertionError(
        f"{column.table.name}.{column.name}: o tipo {column.type!r} não está em ARROW_TYPES")


def column_fields() -> list[tuple[str, sa.Column, pa.Field]]:
    """Cada coluna do modelo ao lado do campo lido nos arquivos, com o nome da tabela, em ordem de
    tabela."""
    fields = []
    for name, table in sorted(ReferenceBase.metadata.tables.items()):
        read = source.SCHEMAS[name]
        for column in table.columns:
            fields.append((name, column, read.field(column.name)))
    return fields


def declared_keys(table: sa.Table) -> list[list[str]]:
    """A chave primária, as ``UniqueConstraint`` e os índices únicos da tabela, como listas de
    colunas."""
    keys = [[column.name for column in table.primary_key.columns]]
    for constraint in table.constraints:
        if isinstance(constraint, sa.UniqueConstraint):
            keys.append([column.name for column in constraint.columns])
    for index in table.indexes:
        if index.unique:
            keys.append([column.name for column in index.columns])
    return keys


def test_the_model_tables_are_the_read_tables_column_by_column() -> None:
    """As 12 tabelas do modelo existem nos arquivos com as mesmas colunas, na mesma ordem e nos
    mesmos tipos."""
    tables = ReferenceBase.metadata.tables
    assert sorted(tables) == sorted(set(source.SCHEMAS) - set(source.OUTSIDE_MODEL))
    for name, table in sorted(tables.items()):
        assert [column.name for column in table.columns] == source.SCHEMAS[name].names, name
    for name, column, field in column_fields():
        assert arrow_type_in_files(column) == field.type, (name, column.name)


def test_the_nullability_differs_only_in_the_columns_the_fixture_lists() -> None:
    """A nulidade difere só nas sete colunas de ``cad_contratos``."""
    nullable_in_the_files = []
    for name, column, field in column_fields():
        if column.nullable != field.nullable:
            # Só o arquivo anulável e o modelo NOT NULL; o modelo prevalece.
            assert field.nullable, (name, column.name)
            assert not column.nullable, (name, column.name)
            nullable_in_the_files.append((name, column.name))
    expected = []
    for table_name, column_names in source.MODEL_NOT_NULL_DECLARED_NULLABLE.items():
        for column_name in column_names:
            expected.append((table_name, column_name))
    assert nullable_in_the_files == expected


def test_the_unique_keys_transcribed_for_the_fixture_are_the_models() -> None:
    """A chave primária, as restrições de unicidade e os índices únicos de cada tabela são os
    transcritos."""
    for name, table in ReferenceBase.metadata.tables.items():
        assert sorted(declared_keys(table)) == sorted(source.UNIQUE_KEYS[name]), name


def test_the_foreign_keys_transcribed_for_the_fixture_are_the_models() -> None:
    """As chaves estrangeiras de cada tabela são as transcritas."""
    foreign_keys = set()
    for table in ReferenceBase.metadata.tables.values():
        for constraint in table.foreign_key_constraints:
            columns = tuple(element.parent.name for element in constraint.elements)
            referenced = tuple(element.column.name for element in constraint.elements)
            foreign_keys.add((table.name, columns, constraint.referred_table.name, referenced))
    transcribed = set()
    for child, columns, parent, referenced in source.FOREIGN_KEYS:
        transcribed.add((child, tuple(columns), parent, tuple(referenced)))
    assert foreign_keys == transcribed
