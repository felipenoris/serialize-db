"""O modelo de referência contra a leitura da base de origem.

``tests/reference_model/`` é o modelo SQLAlchemy da base original em Parquet particionado, que fica
como está (decisão do usuário de 2026-09-21). ``probes/parquet_source.py`` leu a base de
desenvolvimento em 2026-09-20 e a de produção em 2026-09-21 com a mesma seção 3, que
``source_db_projetado.SCHEMAS`` transcreve e ``test_source_db_projetado.py`` confere nos arquivos
gravados; aqui o modelo é lido pelo SQLAlchemy e comparado com ela coluna a coluna, e as chaves que
``test_source_db_projetado.py`` transcreve são conferidas contra as do modelo. Os módulos do modelo
importam ``lib_base_contabil`` e ``lib_base_gerencial``, a biblioteca do pipeline;
``tests/lib_base_contabil.py`` e ``tests/lib_base_gerencial.py`` fazem esses nomes apontarem para os
arquivos do modelo.
"""

from __future__ import annotations

import pyarrow as pa
import sqlalchemy as sa

import source_db_projetado as source
from reference_model.model_db_projetado import Base
from test_source_db_projetado import FOREIGN_KEYS, MODEL_NOT_NULL_DECLARED_NULLABLE, UNIQUE_KEYS

# O tipo Arrow que cada tipo SQLAlchemy do modelo tem nos arquivos: a tabela de tipos de docs/schema.md, com o
# timestamp gravado em INT96 e lido como nanossegundos.
ARROW_TYPES = (
    (sa.Integer, pa.int32()),
    (sa.String, pa.string()),
    (sa.Date, pa.date32()),
    (sa.Double, pa.float64()),
    (sa.Boolean, pa.bool_()),
    (sa.DateTime, pa.timestamp("ns")),
)


def arrow_type(column: sa.Column) -> pa.DataType:
    return next(arrow for sa_type, arrow in ARROW_TYPES if isinstance(column.type, sa_type))


def test_the_model_tables_are_the_read_tables_column_by_column() -> None:
    """As 12 tabelas do modelo existem nos arquivos com as mesmas colunas, na mesma ordem e nos mesmos tipos; a nulidade difere só nas sete colunas de ``cad_contratos``."""
    assert sorted(Base.metadata.tables) == sorted(set(source.SCHEMAS) - set(source.OUTSIDE_MODEL))
    nullable_in_the_files = []
    for name, table in sorted(Base.metadata.tables.items()):
        read = source.SCHEMAS[name]
        assert [column.name for column in table.columns] == read.names, name
        for column in table.columns:
            field = read.field(column.name)
            assert arrow_type(column) == field.type, (name, column.name)
            if column.nullable != field.nullable:
                # Só o arquivo anulável e o modelo NOT NULL; o modelo prevalece (decisão de 2026-09-20).
                assert field.nullable and not column.nullable, (name, column.name)
                nullable_in_the_files.append((name, column.name))
    expected = []
    for table, columns in MODEL_NOT_NULL_DECLARED_NULLABLE.items():
        for column in columns:
            expected.append((table, column))
    assert nullable_in_the_files == expected


def test_the_keys_transcribed_for_the_fixture_are_the_models() -> None:
    """A chave primária, as restrições de unicidade, os índices únicos e as chaves estrangeiras de cada tabela são os transcritos."""
    for name, table in Base.metadata.tables.items():
        keys = [[column.name for column in table.primary_key.columns]]
        keys += [[column.name for column in constraint.columns] for constraint in table.constraints if isinstance(constraint, sa.UniqueConstraint)]
        keys += [[column.name for column in index.columns] for index in table.indexes if index.unique]
        assert sorted(keys) == sorted(UNIQUE_KEYS[name]), name

    foreign_keys = set()
    for table in Base.metadata.tables.values():
        for constraint in table.foreign_key_constraints:
            columns = tuple(element.parent.name for element in constraint.elements)
            referenced = tuple(element.column.name for element in constraint.elements)
            foreign_keys.add((table.name, columns, constraint.referred_table.name, referenced))
    transcribed = set()
    for child, columns, parent, referenced in FOREIGN_KEYS:
        transcribed.add((child, tuple(columns), parent, tuple(referenced)))
    assert foreign_keys == transcribed
