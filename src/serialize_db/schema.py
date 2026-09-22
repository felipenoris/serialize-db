"""O esquema a partir dos modelos SQLAlchemy: Arrow, Delta, DDL por motor, cast e conferência.

O módulo lê os modelos declarativos do cliente (``Base.metadata``) e deriva deles o que a
biblioteca precisa saber sobre cada tabela: o esquema Arrow do contrato (``arrow_schema``), o
esquema Delta (``delta_schema``), as opções físicas de ``Table.info["serialize_db"]``
(``table_options``), o nome de cada tipo no motor (``sql_type``), o texto do ``CREATE TABLE`` do
DuckDB e do Redshift (``ddl``), a conversão de um lote de dados para o contrato (``cast``), a lista
de violações do contrato nos modelos (``check_models``) e os arquivos de esquema que o cliente
versiona (``schema_files``, ``write_schema_files``, ``check_schema_files``).

A tabela de tipos do contrato está na página principal da documentação. Todo identificador que o
módulo emite vai entre aspas duplas, porque nomes como ``to`` e ``timestamp`` são palavras
reservadas dos motores.

Exemplo, com um modelo mínimo:

.. code-block:: python

    from datetime import date

    import pyarrow as pa
    import sqlalchemy as sa
    from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

    from serialize_db import schema


    class Base(DeclarativeBase):
        pass


    class Operacao(Base):
        __tablename__ = "cad_operacoes"
        __table_args__ = {
            "comment": "Operações por data-base",
            "info": {"serialize_db": {"partition_by": ["data_str"],
                                      "partition_source": "data",
                                      "sort_key": ["data", "id_operacao"]}},
        }
        id_operacao: Mapped[int] = mapped_column(
            sa.BigInteger, primary_key=True, autoincrement=False, comment="Identificador")
        data: Mapped[date] = mapped_column(sa.Date, comment="Data-base")
        valor: Mapped[float] = mapped_column(sa.Double, comment="Valor")
        data_str: Mapped[str] = mapped_column(sa.String(10), comment="Partição AAAA-MM-DD")


    table = Operacao.__table__
    schema.check_models(Base.metadata)      # [] quando o modelo obedece ao contrato
    schema.arrow_schema(table)              # id_operacao: int64 not null, data: date32[day], ...
    print(schema.ddl(table, "duckdb"))      # CREATE TABLE "cad_operacoes" (
    batch = pa.RecordBatch.from_pydict({"id_operacao": [1], "data": [date(2026, 8, 31)],
                                        "valor": [10.5], "data_str": ["2026-08-31"]})
    schema.cast(batch, table)               # o lote no esquema do contrato
"""

from __future__ import annotations

import dataclasses
import difflib
import json
import os
from collections.abc import Iterable, Iterator
from typing import Literal

import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sa
from deltalake import Schema as DeltaSchema

from serialize_db.errors import ContractError

__all__ = [
    "Dialect",
    "TableOptions",
    "arrow_schema",
    "arrow_type",
    "cast",
    "check_models",
    "check_schema_files",
    "column_ddl",
    "ddl",
    "delta_schema",
    "quoted",
    "schema_files",
    "sql_type",
    "table_options",
    "write_schema_files",
]

Dialect = Literal["duckdb", "redshift"]
"""Os motores para os quais o módulo gera texto SQL."""

# A tabela de tipos do contrato. A ordem importa: BigInteger e SmallInteger derivam de Integer, e
# Text de String, então os derivados vêm antes.
_ARROW_TYPES: tuple[tuple[type, pa.DataType], ...] = (
    (sa.BigInteger, pa.int64()),
    (sa.SmallInteger, pa.int16()),
    (sa.Integer, pa.int32()),
    (sa.Boolean, pa.bool_()),
    (sa.Double, pa.float64()),
    (sa.Date, pa.date32()),
    (sa.Text, pa.string()),
    (sa.Uuid, pa.string()),
    (sa.JSON, pa.string()),
    (sa.String, pa.string()),
)

# O nome de cada tipo sem parâmetro em cada motor; Numeric, String e DateTime saem de sql_type.
# A ordem importa como em _ARROW_TYPES: sql_type devolve o primeiro tipo que casa por
# isinstance, e BigInteger e SmallInteger derivam de Integer.
_SQL_TYPES: dict[str, dict[type, str]] = {
    "duckdb": {
        sa.BigInteger: "BIGINT",
        sa.SmallInteger: "SMALLINT",
        sa.Integer: "INTEGER",
        sa.Boolean: "BOOLEAN",
        sa.Double: "DOUBLE",
        sa.Date: "DATE",
        sa.Text: "VARCHAR",
        sa.Uuid: "VARCHAR(36)",
        sa.JSON: "JSON",
    },
    "redshift": {
        sa.BigInteger: "BIGINT",
        sa.SmallInteger: "SMALLINT",
        sa.Integer: "INTEGER",
        sa.Boolean: "BOOLEAN",
        sa.Double: "DOUBLE PRECISION",
        sa.Date: "DATE",
        sa.Text: "VARCHAR(65535)",
        sa.Uuid: "VARCHAR(36)",
        sa.JSON: "SUPER",
    },
}

# O teto do VARCHAR no Redshift, em bytes: o limite de uma coluna Text, que não declara n
# (decisão do usuário de 2026-09-21).
_TEXT_LIMIT = 65535


# ---------------------------------------------------------------- o esquema Arrow e Delta


def arrow_type(column: sa.Column) -> pa.DataType:
    """O tipo Arrow da coluna, pela tabela de tipos do contrato.

    ``Numeric(p, s)`` vira ``decimal128(p, s)``; ``DateTime`` vira ``timestamp[us]``, com
    ``tz="UTC"`` quando o tipo tem fuso; os demais seguem a tabela. Um tipo fora dela (``Float``,
    ``LargeBinary``, ``ARRAY``, ``Interval``) é ``ContractError``, com a tabela e a coluna.

    Exemplo:

    .. code-block:: python

        arrow_type(Operacao.__table__.c.id_operacao)   # int64
        arrow_type(Operacao.__table__.c.data)          # date32[day]
    """
    kind = column.type
    # Numeric leva precisão e escala; Float e Double derivam de Numeric e ficam fora deste ramo.
    if isinstance(kind, sa.Numeric) and not isinstance(kind, sa.Float):
        return pa.decimal128(kind.precision or 18, kind.scale or 0)
    if isinstance(kind, sa.DateTime):
        timezone = "UTC" if kind.timezone else None
        return pa.timestamp("us", tz=timezone)
    for sa_type, arrow in _ARROW_TYPES:
        if isinstance(kind, sa_type):
            return arrow
    raise ContractError(f"{column.table.name}.{column.name}: tipo fora do contrato: {kind!r}")


def _arrow_field(column: sa.Column, position: int) -> pa.Field:
    """O campo Arrow da coluna: tipo, nulidade, ``PARQUET:field_id`` pela posição, comentário."""
    metadata = {"PARQUET:field_id": str(position)}
    if column.comment:
        metadata["comment"] = column.comment
    return pa.field(column.name, arrow_type(column), nullable=column.nullable, metadata=metadata)


def arrow_schema(table: sa.Table) -> pa.Schema:
    """O esquema Arrow da tabela: um campo por coluna, na ordem do modelo.

    Cada campo leva a nulidade, ``PARQUET:field_id`` pela posição e o comentário da coluna em
    ``metadata``; o esquema leva o nome da tabela em ``serialize_db_table``. Um campo JSON é
    ``string``, sem a extensão ``arrow.json``.

    Exemplo:

    .. code-block:: python

        schema = arrow_schema(Operacao.__table__)
        schema.field("id_operacao").nullable        # False
        schema.field("valor").metadata[b"comment"]  # b"Valor"
    """
    fields = []
    for position, column in enumerate(table.columns, start=1):
        fields.append(_arrow_field(column, position))
    return pa.schema(fields, metadata={"serialize_db_table": table.name})


def delta_schema(table: sa.Table) -> DeltaSchema:
    """O esquema Delta da tabela, derivado do Arrow pelo delta-rs.

    ``DateTime`` sem fuso vira ``timestamp_ntz`` e com fuso ``timestamp``; ``Numeric(p, s)`` vira
    ``decimal(p,s)``; JSON e UUID viram ``string``; os comentários ficam nos campos. O
    ``PARQUET:field_id`` do esquema Arrow fica de fora: com ele no esquema Delta
    (``parquet.field.id``), o ``delta_scan`` do DuckDB lê toda coluna como nula, qualquer que
    seja o escritor do arquivo (leitura de 2026-09-21).

    Exemplo:

    .. code-block:: python

        delta_schema(Operacao.__table__).to_json()   # {"type": "struct", "fields": [...]}
    """
    fields = []
    for field in arrow_schema(table):
        metadata = dict(field.metadata)
        metadata.pop(b"PARQUET:field_id")
        fields.append(field.with_metadata(metadata))
    return DeltaSchema.from_arrow(pa.schema(fields))


# ---------------------------------------------------------------- as opções físicas


@dataclasses.dataclass(frozen=True)
class TableOptions:
    """O que ``Table.info["serialize_db"]`` declara para uma tabela, com os padrões da biblioteca.

    Exemplo:

    .. code-block:: python

        options = table_options(Operacao.__table__)
        options.partition_by, options.partition_source   # ("data_str", "data")
        options.keys                                      # (("id_operacao",),)
    """

    partition_by: str | None
    """A coluna de partição, ``String(10)`` em ``AAAA-MM-DD``; ``None`` sem partição."""
    partition_source: str | None
    """A coluna de data de que a coluna de partição deriva."""
    sort_key: tuple[str, ...]
    """As colunas da ``SORTKEY`` do Redshift e da ordenação dos arquivos."""
    redshift: dict[str, str]
    """``diststyle`` e ``distkey`` do Redshift; vazio é ``AUTO``."""
    keys: tuple[tuple[str, ...], ...]
    """As chaves que a auditoria confere: a chave primária, as únicas e os índices únicos do
    modelo, mais ``keys["add"]``, menos ``keys["drop"]``."""


def _column_names(columns: Iterable[sa.Column]) -> tuple[str, ...]:
    """Os nomes de uma coleção de colunas, na ordem dela."""
    return tuple(column.name for column in columns)


def _declared_keys(table: sa.Table) -> list[tuple[str, ...]]:
    """A chave primária, as ``UniqueConstraint`` e os índices únicos do modelo."""
    keys = []
    if table.primary_key.columns:
        keys.append(_column_names(table.primary_key.columns))
    for constraint in table.constraints:
        if isinstance(constraint, sa.UniqueConstraint):
            keys.append(_column_names(constraint.columns))
    for index in table.indexes:
        if index.unique:
            keys.append(_column_names(index.columns))
    return keys


def _adjusted_keys(keys: list[tuple[str, ...]], info: dict) -> tuple[tuple[str, ...], ...]:
    """As chaves do modelo mais ``keys["add"]`` e menos ``keys["drop"]`` de ``Table.info``."""
    adjustments = info.get("keys", {})
    declared = list(keys)
    for key in adjustments.get("add", []):
        declared.append(tuple(key))
    dropped = [tuple(key) for key in adjustments.get("drop", [])]
    kept = []
    for key in declared:
        if key not in dropped:
            kept.append(key)
    return tuple(kept)


def table_options(table: sa.Table) -> TableOptions:
    """As opções físicas da tabela, lidas de ``Table.info["serialize_db"]``.

    Sem ``partition_by`` a tabela não tem partição; mais de uma coluna de partição é
    ``ContractError``. ``keys`` reúne a chave primária, as ``UniqueConstraint`` e os índices
    únicos, ajustados por ``keys["add"]`` e ``keys["drop"]``, sempre listas de colunas.

    Exemplo:

    .. code-block:: python

        table_options(Operacao.__table__).sort_key   # ("data", "id_operacao")
    """
    info = table.info.get("serialize_db", {})
    partition = info.get("partition_by") or []
    if len(partition) > 1:
        raise ContractError(
            f"{table.name}: uma coluna de partição no máximo, recebidas {partition}")
    partition_by = partition[0] if partition else None
    return TableOptions(
        partition_by=partition_by,
        partition_source=info.get("partition_source"),
        sort_key=tuple(info.get("sort_key", [])),
        redshift=dict(info.get("redshift", {})),
        keys=_adjusted_keys(_declared_keys(table), info),
    )


# ---------------------------------------------------------------- o DDL por motor


def sql_type(column: sa.Column, dialect: Dialect) -> str:
    """O nome do tipo da coluna no motor, pela tabela de tipos do contrato.

    ``DECIMAL(p, s)`` para ``Numeric``; ``VARCHAR(n)`` para ``String(n)`` nos dois motores (o
    DuckDB aceita e ignora o comprimento; o Redshift o aplica em bytes); ``VARCHAR`` e
    ``VARCHAR(65535)`` para ``Text``; ``VARCHAR(36)`` para ``Uuid``; ``JSON`` e ``SUPER`` para
    ``JSON``; ``DOUBLE`` e ``DOUBLE PRECISION`` para ``Double``; ``TIMESTAMP`` ou ``TIMESTAMPTZ``
    para ``DateTime``. Um tipo fora do contrato é ``ContractError``.

    Exemplo:

    .. code-block:: python

        sql_type(Operacao.__table__.c.valor, "redshift")   # "DOUBLE PRECISION"
    """
    kind = column.type
    arrow = arrow_type(column)      # recusa o tipo fora do contrato antes de qualquer texto
    if pa.types.is_decimal(arrow):
        return f"DECIMAL({arrow.precision}, {arrow.scale})"
    if isinstance(kind, sa.DateTime):
        return "TIMESTAMPTZ" if kind.timezone else "TIMESTAMP"
    for sa_type, text in _SQL_TYPES[dialect].items():
        if isinstance(kind, sa_type):
            return text
    # String(n): o DuckDB aceita e ignora o comprimento, o Redshift o aplica em bytes.
    return f"VARCHAR({kind.length})" if kind.length else "VARCHAR"


def quoted(name: str) -> str:
    """O identificador entre aspas duplas.

    Todo nome de tabela e de coluna que a biblioteca emite vai entre aspas: ``to`` e
    ``timestamp`` são palavras reservadas dos motores, e os nomes do contrato são minúsculos, que
    os dois motores leem igual com ou sem aspas.

    Exemplo:

    .. code-block:: python

        quoted("to")   # '"to"'
    """
    return f'"{name}"'


def column_ddl(column: sa.Column, dialect: Dialect) -> str:
    """A linha da coluna no ``CREATE TABLE``: nome entre aspas, tipo e ``NOT NULL``.

    Exemplo:

    .. code-block:: python

        column_ddl(Operacao.__table__.c.id_operacao, "duckdb")   # '"id_operacao" BIGINT NOT NULL'
    """
    text = f"{quoted(column.name)} {sql_type(column, dialect)}"
    if not column.nullable:
        text += " NOT NULL"
    return text


def _redshift_options(options: TableOptions) -> str:
    """As cláusulas físicas do Redshift depois do parêntese: DISTSTYLE, DISTKEY e SORTKEY."""
    clauses = []
    if options.redshift.get("diststyle"):
        clauses.append(f"DISTSTYLE {options.redshift['diststyle']}")
    if options.redshift.get("distkey"):
        clauses.append(f"DISTKEY ({quoted(options.redshift['distkey'])})")
    if options.sort_key:
        names = ", ".join(quoted(name) for name in options.sort_key)
        clauses.append(f"SORTKEY ({names})")
    return " " + " ".join(clauses) if clauses else ""


def ddl(table: sa.Table, dialect: Dialect, prefix: str = "") -> str:
    """O ``CREATE TABLE`` da tabela no motor, gerado como texto.

    Colunas, tipos e ``NOT NULL``, todo identificador entre aspas; ``DISTSTYLE``, ``DISTKEY`` e
    ``SORTKEY`` no Redshift, de ``table_options``. Sem chave, ``DEFERRABLE``, ``Identity``,
    ``CHECK``, ``DEFAULT`` nem comentário: as chaves são da auditoria, e o comentário vai no
    esquema Delta. ``prefix`` renomeia a tabela para o sandbox, dentro das aspas.

    Exemplo:

    .. code-block:: python

        print(ddl(Operacao.__table__, "redshift", prefix="exec_42_"))
        # CREATE TABLE "exec_42_cad_operacoes" (
        #     "id_operacao" BIGINT NOT NULL,
        #     ...
        # ) SORTKEY ("data", "id_operacao")
    """
    lines = []
    for column in table.columns:
        lines.append("    " + column_ddl(column, dialect))
    text = f"CREATE TABLE {quoted(prefix + table.name)} (\n" + ",\n".join(lines) + "\n)"
    if dialect == "redshift":
        text += _redshift_options(table_options(table))
    return text


# ---------------------------------------------------------------- o cast por lote


def _contract_fields(
    data: pa.Table | pa.RecordBatch, contract: pa.Schema, table: str
) -> list[pa.Field]:
    """Os campos do contrato presentes nos dados, na ordem do contrato; nenhum é erro."""
    present = []
    for field in contract:
        if field.name in data.schema.names:
            present.append(field)
    if not present:
        raise ContractError(f"{table}: nenhuma coluna do contrato em {data.schema.names}")
    return present


def _refuse_double_out_of_scale(
    column: pa.Array | pa.ChunkedArray, field: pa.Field, table: str
) -> None:
    """Um double numa coluna Numeric entra só quando ``pc.round`` o devolve igual."""
    rounded = pc.round(column, field.type.scale)
    # min_count=0: a tabela vazia de reader.schema.empty_table() passa; sem ele, pc.all dá nulo.
    if not pc.all(pc.equal(rounded, column), min_count=0).as_py():
        raise ContractError(f"{table}.{field.name}: double fora da escala {field.type.scale}; "
                            "arredonde no cliente antes de chamar")


def _refuse_timestamp_with_time(
    column: pa.Array | pa.ChunkedArray, field: pa.Field, table: str
) -> None:
    """Um timestamp numa coluna Date entra só quando a ida e volta o devolve igual."""
    round_trip = column.cast(field.type).cast(column.type)
    if not pc.all(pc.equal(round_trip, column), min_count=0).as_py():
        raise ContractError(f"{table}.{field.name}: timestamp com hora numa coluna Date; "
                            "trunque no cliente")


def _refuse_nested_json(column: pa.Array | pa.ChunkedArray, field: pa.Field, table: str) -> None:
    """Um documento JSON chega serializado; struct, list e map são recusados."""
    if pa.types.is_nested(column.type):
        raise ContractError(f"{table}.{field.name}: documento JSON como {column.type}; "
                            "serialize com json.dumps antes de chamar")


def _longest_text(column: pa.Array | pa.ChunkedArray) -> int:
    """O maior valor da coluna em bytes; 0 numa coluna vazia ou só de nulos."""
    return pc.max(pc.binary_length(column)).as_py() or 0


def _refuse_text_above_length(
    column: pa.Array | pa.ChunkedArray, field: pa.Field, table: str, limit: int
) -> None:
    """Texto acima de String(n), medido em bytes como o VARCHAR(n) do Redshift."""
    longest = _longest_text(column)
    if longest > limit:
        raise ContractError(f"{table}.{field.name}: texto de {longest} bytes acima de "
                            f"String({limit}) em bytes; corte o valor ou aumente o comprimento")


def _refuse_text_above_varchar(
    column: pa.Array | pa.ChunkedArray, field: pa.Field, table: str
) -> None:
    """Texto acima do teto do VARCHAR do Redshift numa coluna Text, que não declara n."""
    longest = _longest_text(column)
    if longest > _TEXT_LIMIT:
        raise ContractError(f"{table}.{field.name}: texto de {longest} bytes acima do teto de "
                            f"{_TEXT_LIMIT} bytes do VARCHAR do Redshift; corte o valor")


def _refuse_silent_losses(
    column: pa.Array | pa.ChunkedArray,
    field: pa.Field,
    kind: sa.types.TypeEngine,
    table: str,
) -> None:
    """As perdas que ``cast(safe=True)`` não acusa, recusadas antes da conversão."""
    if pa.types.is_floating(column.type) and pa.types.is_decimal(field.type):
        _refuse_double_out_of_scale(column, field, table)
    if pa.types.is_timestamp(column.type) and pa.types.is_date(field.type):
        _refuse_timestamp_with_time(column, field, table)
    if isinstance(kind, sa.JSON):
        _refuse_nested_json(column, field, table)
    if not pa.types.is_string(column.type):
        return
    # Text é subclasse de String e vem antes: o limite dela é o teto do VARCHAR do Redshift,
    # qualquer que seja o comprimento declarado, porque sql_type ignora o comprimento de Text.
    if isinstance(kind, sa.Text):
        _refuse_text_above_varchar(column, field, table)
    elif isinstance(kind, sa.String) and kind.length:
        _refuse_text_above_length(column, field, table, kind.length)


def _contract_column(
    data: pa.Table | pa.RecordBatch, field: pa.Field, table: sa.Table
) -> pa.Array | pa.ChunkedArray:
    """A coluna dos dados no tipo do contrato; as perdas que ``safe=True`` não acusa vêm antes."""
    column = data.column(field.name)
    _refuse_silent_losses(column, field, table.c[field.name].type, table.name)
    if pa.types.is_integer(column.type) and pa.types.is_decimal(field.type):
        # O cast direto de int64 para decimal128(p, s) pede precisão p + 3; o desvio é seguro.
        column = column.cast(pa.decimal128(field.type.precision + 3, field.type.scale))
    try:
        # safe=True recusa escala perdida, nanossegundo não nulo e estouro.
        return column.cast(field.type, safe=True)
    except (pa.ArrowInvalid, ValueError) as error:
        raise ContractError(f"{table.name}.{field.name}: {error}") from None


def _contract_arrays(
    data: pa.Table | pa.RecordBatch, table: sa.Table
) -> tuple[list[pa.Array | pa.ChunkedArray], pa.Schema]:
    """As colunas do contrato presentes, convertidas, e o esquema delas."""
    contract = arrow_schema(table)
    fields = _contract_fields(data, contract, table.name)
    arrays = []
    for field in fields:
        arrays.append(_contract_column(data, field, table))
    return arrays, pa.schema(fields, metadata=contract.metadata)


def _cast_batch(batch: pa.RecordBatch, table: sa.Table) -> pa.RecordBatch:
    """O lote no esquema do contrato; nulo em coluna NOT NULL é recusado pelo cast do esquema."""
    arrays, schema = _contract_arrays(batch, table)
    try:
        return pa.RecordBatch.from_arrays(arrays, schema=schema).cast(schema, safe=True)
    except (pa.ArrowInvalid, ValueError) as error:
        raise ContractError(f"{table.name}: {error}") from None


def _cast_table(data: pa.Table, table: sa.Table) -> pa.Table:
    """A tabela no esquema do contrato, pelo mesmo caminho do lote."""
    arrays, schema = _contract_arrays(data, table)
    try:
        return pa.Table.from_arrays(arrays, schema=schema).cast(schema, safe=True)
    except (pa.ArrowInvalid, ValueError) as error:
        raise ContractError(f"{table.name}: {error}") from None


def _cast_batches(reader: pa.RecordBatchReader, table: sa.Table) -> Iterator[pa.RecordBatch]:
    """Os lotes do leitor convertidos um a um, para o leitor de saída."""
    for batch in reader:
        yield _cast_batch(batch, table)


def _cast_reader(reader: pa.RecordBatchReader, table: sa.Table) -> pa.RecordBatchReader:
    """O leitor que converte lote a lote, com o esquema derivado do esquema do leitor."""
    schema = _cast_table(reader.schema.empty_table(), table).schema
    return pa.RecordBatchReader.from_batches(schema, _cast_batches(reader, table))


def cast(
    data: pa.Table | pa.RecordBatch | pa.RecordBatchReader, table: sa.Table
) -> pa.Table | pa.RecordBatch | pa.RecordBatchReader:
    """Os dados no esquema do contrato da tabela, devolvidos no mesmo tipo em que chegaram.

    Só as colunas do contrato presentes entram, na ordem do contrato; as ausentes ficam para quem
    grava. Cada coluna é convertida com ``safe=True`` (``large_string`` para ``string``,
    timestamps a microssegundos, inteiro em ``Numeric``), e as perdas que o cast seguro não acusa
    são recusadas antes: ``double`` fora da escala de um ``Numeric``, ``timestamp`` com hora numa
    coluna ``Date``, documento JSON como ``struct``, texto acima de ``String(n)`` em bytes e texto
    acima de 65.535 bytes numa coluna ``Text``, o teto do ``VARCHAR`` do Redshift. Nulo
    em coluna ``NOT NULL``, escala perdida, nanossegundo não nulo, estouro de inteiro e um lote sem
    coluna alguma do contrato também são ``ContractError``, com a tabela, a coluna e a instrução
    ao cliente na mensagem. Um ``RecordBatchReader`` sai como leitor que converte lote a lote.

    Exemplo:

    .. code-block:: python

        batch = pa.RecordBatch.from_pydict({"valor": [10.5], "id_operacao": [1],
                                            "data": [date(2026, 8, 31)],
                                            "data_str": ["2026-08-31"], "extra": [0]})
        cast(batch, Operacao.__table__).schema.names
        # ["id_operacao", "data", "valor", "data_str"]
    """
    if isinstance(data, pa.RecordBatchReader):
        return _cast_reader(data, table)
    if isinstance(data, pa.RecordBatch):
        return _cast_batch(data, table)
    return _cast_table(data, table)


# ---------------------------------------------------------------- a conferência dos modelos


def _column_problems(column: sa.Column) -> list[str]:
    """As violações de uma coluna: tipo, autoincrement, Identity, String sem comprimento."""
    table = column.table.name
    problems = []
    try:
        arrow_type(column)
    except ContractError as error:
        problems.append(str(error))
    # O autoincrement padrão é a string "auto", e um dialeto emitiria SERIAL por ele.
    integer_key = column.primary_key and isinstance(column.type, sa.Integer)
    if integer_key and column.autoincrement in ("auto", True):
        problems.append(f"{table}.{column.name}: chave inteira com autoincrement; "
                        "declare autoincrement=False")
    if column.identity is not None:
        problems.append(f"{table}.{column.name}: Identity fora do contrato")
    if type(column.type) is sa.String and not column.type.length:
        problems.append(f"{table}.{column.name}: String sem comprimento; "
                        "declare String(n) ou Text")
    return problems


def _key_problems(table: sa.Table, options: TableOptions) -> list[str]:
    """As violações das chaves: DEFERRABLE e a tabela sem chave alguma."""
    problems = []
    for constraint in table.foreign_key_constraints:
        if constraint.deferrable or constraint.initially:
            columns = list(_column_names(constraint.columns))
            problems.append(f"{table.name}: chave estrangeira DEFERRABLE em {columns}")
    if not options.keys:
        problems.append(f"{table.name}: sem chave primária e sem keys")
    return problems


def _partition_problems(table: sa.Table, options: TableOptions) -> list[str]:
    """As violações da partição: a coluna ausente ou fora de String(10), a origem ausente."""
    if not options.partition_by:
        return []
    problems = []
    column = table.c.get(options.partition_by)
    if column is None:
        problems.append(
            f"{table.name}: partition_by aponta {options.partition_by}, que a tabela não tem")
    elif not (isinstance(column.type, sa.String) and column.type.length == 10):
        problems.append(
            f"{table.name}.{options.partition_by}: coluna de partição fora de String(10)")
    if not options.partition_source or options.partition_source not in table.c:
        problems.append(f"{table.name}: partition_by sem partition_source válido")
    return problems


def check_models(metadata: sa.MetaData) -> list[str]:
    """As violações do contrato nos modelos, um texto por violação; vazia nos modelos corretos.

    As regras: tipo fora da tabela de tipos; ``autoincrement`` numa chave inteira (o padrão
    ``"auto"`` inclusive); ``Identity``; ``String`` sem comprimento; chave estrangeira
    ``DEFERRABLE``; ``partition_by`` sem a coluna, com a coluna fora de ``String(10)`` ou sem
    ``partition_source``; tabela sem chave primária e sem ``keys``. O comentário de tabela e de
    coluna é opcional (decisão do usuário de 2026-09-21); o da coluna, quando existe, vai para o
    esquema Arrow e para o Delta.

    Exemplo:

    .. code-block:: python

        assert check_models(Base.metadata) == []
    """
    problems = []
    for table in metadata.sorted_tables:
        for column in table.columns:
            problems.extend(_column_problems(column))
        options = table_options(table)
        problems.extend(_key_problems(table, options))
        problems.extend(_partition_problems(table, options))
    return problems


# ---------------------------------------------------------------- os arquivos de esquema


def _delta_schema_json(table: sa.Table) -> str:
    """O esquema Delta em JSON canônico: chaves ordenadas e indentado, uma linha por chave.

    O ``to_json()`` do delta-rs serializa os metadados de cada campo em ordem arbitrária, que muda
    a cada geração; o JSON canônico é o que o cliente versiona e compara.
    """
    document = json.loads(delta_schema(table).to_json())
    return json.dumps(document, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def _versioned_text(path: str) -> str:
    """O conteúdo do arquivo versionado, ou vazio quando ele não existe."""
    if not os.path.exists(path):
        return ""
    with open(path, encoding="utf-8") as handle:
        return handle.read()


def schema_files(metadata: sa.MetaData) -> dict[str, str]:
    """Os arquivos de esquema de cada tabela, em memória: o conteúdo por nome de arquivo.

    ``<tabela>.delta.json`` é o esquema Delta em JSON canônico, ``<tabela>.duckdb.sql`` e
    ``<tabela>.redshift.sql`` são o ``CREATE TABLE`` de cada motor; todo texto termina em ``\\n``.
    O cliente os versiona no repositório do pipeline, e o diff contra a geração nova mostra o que
    uma mudança de modelo altera em cada motor.

    Exemplo:

    .. code-block:: python

        sorted(schema_files(Base.metadata))
        # ["cad_operacoes.delta.json", "cad_operacoes.duckdb.sql", "cad_operacoes.redshift.sql"]
    """
    files = {}
    for table in metadata.sorted_tables:
        files[f"{table.name}.delta.json"] = _delta_schema_json(table)
        files[f"{table.name}.duckdb.sql"] = ddl(table, "duckdb") + "\n"
        files[f"{table.name}.redshift.sql"] = ddl(table, "redshift") + "\n"
    return files


def write_schema_files(metadata: sa.MetaData, directory: str) -> list[str]:
    """Grava ``schema_files`` em ``directory``, criada se preciso, e devolve os caminhos gravados.

    Exemplo:

    .. code-block:: python

        write_schema_files(Base.metadata, "schema")   # ["schema/cad_operacoes.delta.json", ...]
    """
    os.makedirs(directory, exist_ok=True)
    written = []
    for name, content in sorted(schema_files(metadata).items()):
        path = os.path.join(directory, name)
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(content)
        written.append(path)
    return written


def check_schema_files(metadata: sa.MetaData, directory: str) -> list[str]:
    """O diff unificado dos arquivos versionados em ``directory`` contra a geração nova.

    Vazio quando nada mudou; um arquivo ausente aparece inteiro como acrescentado. Nada é gravado.

    Exemplo:

    .. code-block:: python

        check_schema_files(Base.metadata, "schema")   # [] quando os arquivos estão atualizados
    """
    diff = []
    for name, content in sorted(schema_files(metadata).items()):
        path = os.path.join(directory, name)
        diff.extend(difflib.unified_diff(
            _versioned_text(path).splitlines(), content.splitlines(),
            fromfile=path, tofile=f"{path} (gerado)", lineterm=""))
    return diff
