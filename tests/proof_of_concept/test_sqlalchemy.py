"""O SQLAlchemy no papel que a biblioteca lhe dá: modelos declarativos como contrato e Core para mover dados.

Sem gravar arquivo algum, os testes mostram o ``Table`` que um modelo declarativo expõe, o DDL
compilado para o DuckDB e para o Redshift (com as opções físicas lidas de ``Table.info``), o
``create_all`` num DuckDB em memória, os statements Core de ``insert`` e ``select`` executados pelo
``duckdb_engine``, o caminho por Arrow na conexão bruta, a reflexão, a precisão do ``Numeric`` pelo
dialeto contra o caminho Arrow, o ``pandas.read_sql``, o texto SQL gerado por dialeto com parâmetro
e prefixo (``docs/sqlalchemy.md``) e a compilação de DML para o Redshift, que não exige um cluster.
Nenhuma classe ORM é instanciada: a biblioteca usa os modelos como metadados e o Core como gerador
de SQL.
"""

from __future__ import annotations

import datetime as dt
import decimal
import difflib
import json
import re
import warnings
from collections.abc import Iterator

import duckdb
import duckdb_engine
import pandas as pd
import pyarrow as pa
import pytest
import sqlalchemy as sa
from deltalake import Schema as DeltaSchema
from sqlalchemy.exc import SAWarning
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.visitors import replacement_traverse
from sqlalchemy_redshift.dialect import SUPER, RedshiftDialect_redshift_connector

from conftest import record


class Base(DeclarativeBase):
    """A base declarativa: cada subclasse mapeada registra um ``Table`` em ``Base.metadata``."""


class Operacao(Base):
    """O modelo de exemplo, com os tipos do contrato e as opções físicas em ``Table.info``."""

    __tablename__ = "cad_operacoes"
    __table_args__ = {
        "comment": "Operações do mês",
        "info": {
            "serialize_db": {
                "partition_by": ["mes"],
                "sort_key": ["data_ref", "id_operacao"],
                "redshift": {"diststyle": "KEY", "distkey": "id_cliente"},
            }
        },
    }

    # autoincrement=False: a chave é gerada no cliente; com o padrão, o duckdb_engine emitiria SERIAL.
    id_operacao: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False, comment="Identificador da operação")
    data_ref: Mapped[dt.date] = mapped_column(sa.Date, nullable=False, comment="Data de referência")
    id_cliente: Mapped[int] = mapped_column(sa.BigInteger, nullable=False, comment="Chave do cliente")
    valor: Mapped[decimal.Decimal] = mapped_column(sa.Numeric(18, 2), nullable=False, comment="Valor em reais")
    descricao: Mapped[str | None] = mapped_column(sa.String(200), comment="Texto livre")

    # JSON no DuckDB e SUPER no Redshift, pelo mesmo atributo: with_variant troca o tipo por dialeto.
    meta: Mapped[dict | None] = mapped_column(sa.JSON().with_variant(SUPER(), "redshift"), comment="Documento sem esquema fixo")
    mes: Mapped[str] = mapped_column(sa.String(7), nullable=False, comment="Partição YYYY-MM")


class Cliente(Base):
    """A dimensão do exemplo, para o join."""

    __tablename__ = "cad_clientes"

    id_cliente: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    nome: Mapped[str] = mapped_column(sa.String(200), nullable=False)
    mes: Mapped[str] = mapped_column(sa.String(7), nullable=False)


# Dialetos avulsos, sem engine: bastam para compilar DDL e DML. paramstyle="named" evita a dobra do %
# nos literais quando o texto é gerado com literal_binds.
DIALECTS = {
    "duckdb": duckdb_engine.Dialect(paramstyle="named"),
    "redshift": RedshiftDialect_redshift_connector(paramstyle="named"),
}

OPERACOES = [
    {"id_operacao": 1, "data_ref": dt.date(2026, 8, 1), "id_cliente": 7, "valor": decimal.Decimal("150.00"), "descricao": "a", "meta": {"canal": "app"}, "mes": "2026-08"},
    {"id_operacao": 2, "data_ref": dt.date(2026, 8, 2), "id_cliente": 7, "valor": decimal.Decimal("50.00"), "descricao": None, "meta": None, "mes": "2026-08"},
    {"id_operacao": 3, "data_ref": dt.date(2026, 7, 1), "id_cliente": 9, "valor": decimal.Decimal("200.00"), "descricao": "c", "meta": None, "mes": "2026-07"},
    {"id_operacao": 4, "data_ref": dt.date(2026, 8, 3), "id_cliente": 9, "valor": decimal.Decimal("120.00"), "descricao": "d", "meta": None, "mes": "2026-08"},
]
CLIENTES = [
    {"id_cliente": 7, "nome": "Alfa", "mes": "2026-08"},
    {"id_cliente": 9, "nome": "Beta", "mes": "2026-08"},
]


@compiles(CreateTable, "redshift")
def create_table_with_physical_options(element: CreateTable, compiler: sa.sql.compiler.DDLCompiler, **kw: object) -> str:
    """Acrescenta ao ``CREATE TABLE`` do Redshift as opções físicas guardadas em ``Table.info["serialize_db"]``.

    É a alternativa aos argumentos ``redshift_diststyle``, ``redshift_distkey`` e ``redshift_sortkey``
    do ``sqlalchemy-redshift``, que só existem com o dialeto instalado; ``info`` mantém o modelo neutro.
    """
    text = compiler.visit_create_table(element, **kw).rstrip()
    options = element.element.info.get("serialize_db", {})
    clauses = []

    redshift = options.get("redshift", {})
    if redshift.get("diststyle"):
        clauses.append(f"DISTSTYLE {redshift['diststyle']}")
    if redshift.get("distkey"):
        clauses.append(f"DISTKEY ({redshift['distkey']})")
    if options.get("sort_key"):
        clauses.append(f"SORTKEY ({', '.join(options['sort_key'])})")

    return f"{text} {' '.join(clauses)}\n\n" if clauses else f"{text}\n\n"


def normalized(sql: str) -> str:
    """O texto com espaços e quebras de linha reduzidos a um espaço, para comparações."""
    return " ".join(sql.split())


@pytest.fixture
def engine() -> Iterator[sa.Engine]:
    """Um DuckDB em memória com as tabelas do modelo criadas; cada teste recebe o seu."""
    engine = sa.create_engine("duckdb:///:memory:")
    Base.metadata.create_all(engine)

    yield engine

    engine.dispose()


def load_sample(engine: sa.Engine) -> None:
    """Carrega as linhas de exemplo por um único ``INSERT`` de várias linhas por tabela."""
    with engine.begin() as connection:
        connection.execute(sa.insert(Operacao).values(OPERACOES))
        connection.execute(sa.insert(Cliente).values(CLIENTES))


def test_declarative_model_exposes_table() -> None:
    """O modelo declarativo é um ``Table`` em ``Base.metadata``: colunas, tipos, nulidade, chave, comentários e ``info``."""
    table = Operacao.__table__

    assert isinstance(table, sa.Table) and table.name == "cad_operacoes"
    assert Base.metadata.tables["cad_operacoes"] is table
    assert [column.name for column in table.columns] == ["id_operacao", "data_ref", "id_cliente", "valor", "descricao", "meta", "mes"]
    assert [column.name for column in table.primary_key.columns] == ["id_operacao"]

    # Mapped[str | None] torna a coluna anulável; nullable=False e Mapped[str] a tornam obrigatória.
    assert table.c.descricao.nullable and not table.c.valor.nullable

    valor = table.c.valor.type
    assert isinstance(valor, sa.Numeric) and (valor.precision, valor.scale) == (18, 2)
    assert isinstance(table.c.meta.type, sa.JSON)  # with_variant preserva o tipo genérico

    assert table.info["serialize_db"]["partition_by"] == ["mes"]
    assert table.comment == "Operações do mês" and table.c.id_cliente.comment == "Chave do cliente"


def test_ddl_per_dialect() -> None:
    """O mesmo ``Table`` compila o ``CREATE TABLE`` de cada motor; o Redshift recebe as opções físicas de ``info``."""
    duckdb_ddl = normalized(str(CreateTable(Operacao.__table__).compile(dialect=DIALECTS["duckdb"])))
    redshift_ddl = normalized(str(CreateTable(Operacao.__table__).compile(dialect=DIALECTS["redshift"])))

    assert "valor NUMERIC(18, 2) NOT NULL" in duckdb_ddl
    assert "meta JSON" in duckdb_ddl
    assert "PRIMARY KEY (id_operacao)" in duckdb_ddl

    assert "meta SUPER" in redshift_ddl
    assert redshift_ddl.endswith("DISTSTYLE KEY DISTKEY (id_cliente) SORTKEY (data_ref, id_operacao)")

    # Text vira TEXT no Redshift, que o banco guarda como VARCHAR(256); o contrato usa String(65535).
    text_table = sa.Table("observacoes", sa.MetaData(), sa.Column("texto", sa.Text))
    assert "texto TEXT" in normalized(str(CreateTable(text_table).compile(dialect=DIALECTS["redshift"])))


def test_create_all_and_reflection(engine: sa.Engine) -> None:
    """``create_all`` cria as tabelas no DuckDB; a reflexão devolve colunas, tipos e comentários, não a chave."""
    inspector = sa.inspect(engine)
    assert {"cad_operacoes", "cad_clientes"} <= set(inspector.get_table_names())

    columns = {column["name"]: column for column in inspector.get_columns("cad_operacoes")}
    assert (columns["valor"]["type"].precision, columns["valor"]["type"].scale) == (18, 2)
    assert columns["descricao"]["type"].length is None  # o catálogo guarda VARCHAR sem comprimento
    assert columns["id_cliente"]["comment"] == "Chave do cliente"
    assert columns["descricao"]["nullable"] and not columns["valor"]["nullable"]

    # A chave primária existe no catálogo, mas o duckdb_engine não a reflete.
    assert inspector.get_pk_constraint("cad_operacoes")["constrained_columns"] == []

    # Uma chave Integer de uma coluna com o autoincrement padrão sai como SERIAL, que o DuckDB não tem.
    metadata = sa.MetaData()
    sa.Table("serial_probe", metadata, sa.Column("id", sa.Integer, primary_key=True))
    with pytest.raises(sa.exc.DBAPIError, match="SERIAL"):
        metadata.create_all(engine)


def test_core_insert_and_select(engine: sa.Engine) -> None:
    """``insert(...).values(lista)`` é um único comando; ``select`` com join e agregação devolve ``Decimal``."""
    load_sample(engine)

    query = (
        sa.select(Cliente.nome, sa.func.sum(Operacao.valor).label("total"))
        .join_from(Operacao, Cliente, Operacao.id_cliente == Cliente.id_cliente)
        .where(Operacao.mes == "2026-08")
        .group_by(Cliente.nome)
        .order_by(Cliente.nome)
    )

    with engine.connect() as connection:
        rows = connection.execute(query).all()
    assert rows == [("Alfa", decimal.Decimal("200.00")), ("Beta", decimal.Decimal("120.00"))]

    # O dialeto ligado ao engine compila com $1, $2 (numeric_dollar) e lista os parâmetros em positiontup.
    compiled = query.compile(dialect=engine.dialect)
    assert "$1" in str(compiled) and list(compiled.positiontup) == ["mes_1"]

    # A coluna JSON devolve o dict gravado.
    with engine.connect() as connection:
        meta = connection.execute(sa.select(Operacao.meta).where(Operacao.id_operacao == 1)).scalar_one()
    assert meta == {"canal": "app"}


def test_arrow_path_on_raw_connection(engine: sa.Engine) -> None:
    """O SQL compilado pelo SQLAlchemy roda na conexão DuckDB por trás do engine, com Arrow na saída e na entrada."""
    load_sample(engine)
    query = sa.select(Operacao).where(Operacao.mes == "2026-08").order_by(Operacao.id_operacao)

    with engine.connect() as connection:
        # O texto e os parâmetros vêm do compilador; a conexão bruta os executa e devolve Arrow.
        compiled = query.compile(dialect=engine.dialect)
        parameters = [compiled.params[name] for name in (compiled.positiontup or [])]
        raw = connection.connection.dbapi_connection
        table = raw.execute(str(compiled), parameters).to_arrow_table()

    assert table.num_rows == 3
    assert table.schema.field("valor").type == pa.decimal128(18, 2)

    # Ingestão: a tabela Arrow registrada na conexão bruta entra por INSERT ... BY NAME, sem executemany.
    incoming = pa.table(
        {
            "id_operacao": pa.array([5], pa.int64()),
            "data_ref": pa.array([dt.date(2026, 8, 4)], pa.date32()),
            "id_cliente": pa.array([7], pa.int64()),
            "valor": pa.array([decimal.Decimal("1.00")], pa.decimal128(18, 2)),
            "mes": pa.array(["2026-08"], pa.string()),
        }
    )
    with engine.begin() as connection:
        raw = connection.connection.dbapi_connection
        raw.register("entrada", incoming)
        connection.execute(sa.text("INSERT INTO cad_operacoes BY NAME SELECT * FROM entrada"))
        raw.unregister("entrada")

    with engine.connect() as connection:
        assert connection.execute(sa.select(sa.func.count()).select_from(Operacao)).scalar_one() == 5


def test_numeric_precision_dialect_versus_arrow(engine: sa.Engine) -> None:
    """Pelo dialeto, ``Numeric`` passa por ``float``; pelo Arrow, ``decimal128(18, 2)`` guarda os 18 dígitos."""
    exact = decimal.Decimal("1234567890123.45")  # 15 dígitos significativos
    rounded = decimal.Decimal("123456789012345.67")  # 17 dígitos

    with engine.begin() as connection:
        connection.execute(
            sa.insert(Operacao).values(
                [
                    {"id_operacao": 1, "data_ref": dt.date(2026, 8, 1), "id_cliente": 7, "valor": exact, "mes": "2026-08"},
                    {"id_operacao": 2, "data_ref": dt.date(2026, 8, 1), "id_cliente": 7, "valor": rounded, "mes": "2026-08"},
                ]
            )
        )

    with engine.connect() as connection:
        read = dict(connection.execute(sa.select(Operacao.id_operacao, Operacao.valor)).all())
    assert read[1] == exact
    assert read[2] != rounded
    record("sqlalchemy.numeric_17_digits_through_dialect", f"{rounded} -> {read[2]}")

    # Pelo Arrow o valor de 18 dígitos entra e sai íntegro.
    big = decimal.Decimal("1234567890123456.78")
    incoming = pa.table(
        {
            "id_operacao": pa.array([3], pa.int64()),
            "data_ref": pa.array([dt.date(2026, 8, 1)], pa.date32()),
            "id_cliente": pa.array([7], pa.int64()),
            "valor": pa.array([big], pa.decimal128(18, 2)),
            "mes": pa.array(["2026-08"], pa.string()),
        }
    )
    with engine.begin() as connection:
        raw = connection.connection.dbapi_connection
        raw.register("entrada", incoming)
        connection.execute(sa.text("INSERT INTO cad_operacoes BY NAME SELECT * FROM entrada"))
        stored = raw.execute("SELECT valor FROM cad_operacoes WHERE id_operacao = 3").fetchone()[0]
        raw.unregister("entrada")

    assert stored == big


def test_pandas_read_sql_keeps_decimal_only_with_coerce_float_off(engine: sa.Engine) -> None:
    """``pandas.read_sql`` converte ``Decimal`` em ``float`` por padrão; ``coerce_float=False`` preserva os objetos."""
    load_sample(engine)
    query = sa.select(Operacao.id_operacao, Operacao.data_ref, Operacao.valor).order_by(Operacao.id_operacao)

    default = pd.read_sql(query, engine)
    assert default["valor"].dtype == "float64"

    kept = pd.read_sql(query, engine, coerce_float=False)
    assert isinstance(kept["valor"].iloc[0], decimal.Decimal)
    assert isinstance(kept["data_ref"].iloc[0], dt.date)


def param(name: str, type_: sa.types.TypeEngine | None = None) -> sa.ColumnElement:
    """Parâmetro de execução: o texto ``:nome`` atravessa ``literal_binds`` e é resolvido na execução."""
    return sa.literal_column(f":{name}", type_=type_)


def prefixed(statement: sa.Select, metadata: sa.MetaData, prefix: str = "{prefix}") -> sa.Select:
    """Troca cada tabela do contrato pela cópia com o prefixo do sandbox; o sentinela sai sem aspas."""
    copies = {
        table: table.to_metadata(sa.MetaData(), name=quoted_name(f"{prefix}{table.name}", quote=False)) for table in metadata.tables.values()
    }

    def replace(element: object) -> object:
        if isinstance(element, sa.Table):
            return copies.get(element)
        if isinstance(element, sa.Column) and element.table in copies:
            return copies[element.table].c[element.name]
        return None

    return replacement_traverse(statement, {}, replace)


def render(statement: sa.Select, dialect: str, metadata: sa.MetaData, prefix: str = "{prefix}") -> str:
    """Texto do dialeto com as constantes embutidas; um ``bindparam`` sem valor viraria ``NULL``, então é erro."""
    with warnings.catch_warnings():
        warnings.simplefilter("error", SAWarning)
        compiled = prefixed(statement, metadata, prefix).compile(dialect=DIALECTS[dialect], compile_kwargs={"literal_binds": True})

    return str(compiled)


def test_generated_sql_text_per_dialect() -> None:
    """O statement Core vira texto de cada dialeto com ``:mes``, o prefixo do sandbox e os literais intactos."""
    operations, clients = Operacao.__table__, Cliente.__table__
    query = (
        sa.select(clients.c.nome, sa.func.sum(operations.c.valor).label("total"))
        .join_from(operations, clients, operations.c.id_cliente == clients.c.id_cliente)
        .where(operations.c.mes == param("mes", sa.String(7)), operations.c.valor > decimal.Decimal("100.00"), clients.c.nome.like("A%"))
        .group_by(clients.c.nome)
        .order_by(clients.c.nome)
    )

    texts = {dialect: render(query, dialect, Base.metadata) for dialect in DIALECTS}
    assert texts["duckdb"] == texts["redshift"]
    assert "{prefix}cad_operacoes" in texts["duckdb"] and ":mes" in texts["duckdb"] and "LIKE 'A%'" in texts["duckdb"]

    # Um bindparam sem valor renderizaria NULL com um aviso; render o transforma em erro.
    with pytest.raises(SAWarning):
        render(sa.select(operations.c.id_cliente).where(operations.c.mes == sa.bindparam("mes")), "duckdb", Base.metadata)

    # Com o dialeto avulso no paramstyle padrão (pyformat), o mesmo literal sairia dobrado: LIKE 'A%%'.
    doubled = str(query.compile(dialect=duckdb_engine.Dialect(), compile_kwargs={"literal_binds": True}))
    assert "LIKE 'A%%'" in doubled

    # O texto roda no DuckDB com o prefixo do sandbox no lugar do sentinela e :mes reescrito como $mes.
    con = duckdb.connect()
    con.execute("CREATE TABLE exec_42_cad_operacoes (id_operacao BIGINT, id_cliente BIGINT, valor DECIMAL(18,2), mes VARCHAR)")
    con.execute("CREATE TABLE exec_42_cad_clientes (id_cliente BIGINT, nome VARCHAR, mes VARCHAR)")
    con.execute("INSERT INTO exec_42_cad_operacoes VALUES (1, 7, 150.00, '2026-08'), (2, 7, 50.00, '2026-08'), (3, 9, 200.00, '2026-07'), (4, 9, 120.00, '2026-08')")
    con.execute("INSERT INTO exec_42_cad_clientes VALUES (7, 'Alfa', '2026-08'), (9, 'Beta', '2026-08')")

    params = {"mes": "2026-08"}
    sql = re.sub(rf"(?<!:):({'|'.join(params)})\b", r"$\1", render(query, "duckdb", Base.metadata, prefix="exec_42_"))
    assert con.execute(sql, params).fetchall() == [("Alfa", decimal.Decimal("150.00"))]


def test_redshift_dialect_compiles_dml() -> None:
    """O DML compila para o Redshift sem cluster: um ``INSERT`` de várias linhas com ``%s`` e o ``select`` do contrato."""
    dialect = RedshiftDialect_redshift_connector()

    insert = sa.insert(Operacao.__table__).values(OPERACOES[:2])
    text = str(insert.compile(dialect=dialect))
    assert text.startswith("INSERT INTO cad_operacoes") and text.count("(%s, ") == 2  # um comando, duas linhas
    assert text.count("json_parse(%s)") == 2  # o parâmetro da coluna SUPER entra por json_parse

    # UPDATE ... FROM e DELETE ... USING, que o Redshift aceita, compilam pelo Core.
    operations, clients = Operacao.__table__, Cliente.__table__
    update = sa.update(operations).where(operations.c.id_cliente == clients.c.id_cliente).values(descricao=clients.c.nome)
    assert normalized(str(update.compile(dialect=dialect))).startswith("UPDATE cad_operacoes SET descricao=cad_clientes.nome FROM cad_clientes")

    delete = sa.delete(operations).where(operations.c.id_cliente == clients.c.id_cliente)
    assert "USING cad_clientes" in normalized(str(delete.compile(dialect=dialect)))


ARROW_TYPES: dict[type, pa.DataType] = {
    sa.SmallInteger: pa.int16(),
    sa.Integer: pa.int32(),
    sa.BigInteger: pa.int64(),
    sa.Boolean: pa.bool_(),
    sa.Double: pa.float64(),
    sa.Date: pa.date32(),
    sa.String: pa.string(),
    sa.Text: pa.string(),
    sa.Uuid: pa.string(),
    sa.JSON: pa.string(),
}


def arrow_type(column: sa.Column) -> pa.DataType:
    """O tipo Arrow de uma coluna do contrato; ``Numeric`` e ``DateTime`` carregam parâmetros, os demais vêm da tabela."""
    kind = column.type
    if isinstance(kind, sa.Numeric) and not isinstance(kind, sa.Float):
        return pa.decimal128(kind.precision or 18, kind.scale or 0)
    if isinstance(kind, sa.DateTime):
        return pa.timestamp("us", tz="UTC" if kind.timezone else None)

    # A ordem importa: BigInteger e SmallInteger derivam de Integer, e Text de String.
    for sa_type in (sa.BigInteger, sa.SmallInteger, sa.Integer, sa.Boolean, sa.Double, sa.Date, sa.Text, sa.Uuid, sa.JSON, sa.String):
        if isinstance(kind, sa_type):
            return ARROW_TYPES[sa_type]

    raise TypeError(f"{column.name}: tipo fora do contrato: {kind!r}")


def arrow_schema(table: sa.Table) -> pa.Schema:
    """O esquema Arrow do ``Table``: tipos do contrato, nulidade, comentário e ``PARQUET:field_id`` por campo."""
    fields = []
    for index, column in enumerate(table.columns, start=1):
        metadata = {"PARQUET:field_id": str(index)}
        if column.comment:
            metadata["comment"] = column.comment
        fields.append(pa.field(column.name, arrow_type(column), nullable=column.nullable, metadata=metadata))

    return pa.schema(fields, metadata={"serialize_db_table": table.name})


def test_arrow_and_delta_schema_from_table() -> None:
    """Do ``Table`` saem o esquema Arrow e, dele, o esquema Delta em JSON: a etapa 1 sem gravar nada."""
    schema = arrow_schema(Operacao.__table__)

    assert schema.field("id_operacao").type == pa.int64() and not schema.field("id_operacao").nullable
    assert schema.field("valor").type == pa.decimal128(18, 2)
    assert schema.field("data_ref").type == pa.date32()
    assert schema.field("descricao").nullable and schema.field("meta").type == pa.string()
    assert schema.field("id_cliente").metadata[b"comment"] == b"Chave do cliente"
    assert schema.field("mes").metadata[b"PARQUET:field_id"] == b"7"

    # O delta-rs deriva o esquema Delta do Arrow; to_json é o conteúdo de schema/<tabela>.delta.json.
    delta = DeltaSchema.from_arrow(schema)
    fields = {field["name"]: field for field in json.loads(delta.to_json())["fields"]}
    assert fields["id_operacao"]["type"] == "long" and fields["valor"]["type"] == "decimal(18,2)"
    assert fields["data_ref"]["type"] == "date" and fields["meta"]["type"] == "string"
    assert fields["id_cliente"]["metadata"]["comment"] == "Chave do cliente"
    assert fields["id_operacao"]["nullable"] is False

    # Um DateTime sem fuso vira timestamp_ntz; com fuso, timestamp.
    stamped = sa.Table("carimbos", sa.MetaData(), sa.Column("local", sa.DateTime), sa.Column("utc", sa.DateTime(timezone=True)))
    kinds = {field["name"]: field["type"] for field in json.loads(DeltaSchema.from_arrow(arrow_schema(stamped)).to_json())["fields"]}
    assert kinds == {"local": "timestamp_ntz", "utc": "timestamp"}


def test_sandbox_copy_of_table_and_schema_files_diff() -> None:
    """``to_metadata`` dá a cópia com prefixo e esquema para o sandbox; os arquivos gerados são comparados por ``difflib``."""
    table = Operacao.__table__

    # A cópia renomeada e qualificada é o que o motor Redshift cria por execução.
    sandbox = table.to_metadata(sa.MetaData(schema="projeto"), name="exec_42_cad_operacoes")
    ddl = normalized(str(CreateTable(sandbox).compile(dialect=DIALECTS["redshift"])))
    assert ddl.startswith("CREATE TABLE projeto.exec_42_cad_operacoes (")
    assert sandbox.c.valor.type.scale == 2 and sandbox.info == table.info

    # schema/<tabela>.<dialeto>.sql versionado contra o regenerado depois de uma coluna nova.
    def schema_files(source: sa.Table) -> dict[str, str]:
        return {
            f"{source.name}.duckdb.sql": str(CreateTable(source).compile(dialect=DIALECTS["duckdb"])),
            f"{source.name}.redshift.sql": str(CreateTable(source).compile(dialect=DIALECTS["redshift"])),
            f"{source.name}.delta.json": DeltaSchema.from_arrow(arrow_schema(source)).to_json(),
        }

    versioned = schema_files(table)
    evolved = table.to_metadata(sa.MetaData())
    evolved.append_column(sa.Column("canal", sa.String(20), comment="Origem do lançamento"))
    regenerated = schema_files(evolved)

    assert set(versioned) == set(regenerated)
    diff = list(difflib.unified_diff(versioned["cad_operacoes.duckdb.sql"].splitlines(), regenerated["cad_operacoes.duckdb.sql"].splitlines(), lineterm=""))
    assert any(line.startswith("+") and "canal VARCHAR(20)" in line for line in diff)
    assert "canal" in regenerated["cad_operacoes.delta.json"] and "canal" not in versioned["cad_operacoes.delta.json"]
