"""O SQLAlchemy no papel que a biblioteca lhe dá: modelos declarativos como contrato e Core para
mover dados.

Sem gravar arquivo algum, os testes mostram o ``Table`` que um modelo declarativo expõe, o DDL
compilado para o DuckDB e para o Redshift (com as opções físicas de ``Table.info`` acrescentadas
por uma função comum, sem regra ``@compiles``), o ``create_all`` num DuckDB em memória, os
statements Core de ``insert`` e ``select`` executados pelo ``duckdb_engine``, o caminho por Arrow
na conexão bruta, a reflexão, a precisão do ``Numeric`` pelo dialeto contra o caminho Arrow, o
``pandas.read_sql``, os comportamentos do compilador que dão forma ao ``render`` da etapa 2 (o
``bindparam`` sem valor sob ``literal_binds``, ``compiled.binds``, o ``%`` dobrado, a citação só
das palavras reservadas), o ``IN`` de lista no caminho dos motores (``statement.params`` com
``render_postcompile``, e o estilo ``qmark``), o registro de uma ``GenericFunction`` em ``sa.func``
para o processo inteiro, o nome em três partes, que só serve a uma sessão aberta em outro banco, a
compilação de DML para o Redshift, que não exige um cluster, o esquema Delta que
``Schema.from_arrow`` deriva de um esquema Arrow e a cópia do sandbox por ``to_metadata``.
Nenhuma classe ORM é instanciada: a biblioteca usa os modelos como metadados e o Core como gerador
de SQL.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
from collections.abc import Iterator

import duckdb
import duckdb_engine
import pandas as pd
import pyarrow as pa
import pytest
import sqlalchemy as sa
from deltalake import Schema as DeltaSchema
from sqlalchemy.exc import InvalidRequestError, SAWarning
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.schema import CreateTable
from sqlalchemy.sql import quoted_name
from sqlalchemy.sql.functions import Function, FunctionElement, GenericFunction
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

    # autoincrement=False: a chave é gerada no cliente; com o padrão, o duckdb_engine emitiria
    # SERIAL.
    id_operacao: Mapped[int] = mapped_column(
        sa.BigInteger, primary_key=True, autoincrement=False, comment="Identificador da operação")
    data_ref: Mapped[dt.date] = mapped_column(sa.Date, nullable=False, comment="Data de referência")
    id_cliente: Mapped[int] = mapped_column(
        sa.BigInteger, nullable=False, comment="Chave do cliente")
    valor: Mapped[decimal.Decimal] = mapped_column(
        sa.Numeric(18, 2), nullable=False, comment="Valor em reais")
    descricao: Mapped[str | None] = mapped_column(sa.String(200), comment="Texto livre")

    # JSON no DuckDB e SUPER no Redshift, pelo mesmo atributo: with_variant troca o tipo por
    # dialeto.
    meta: Mapped[dict | None] = mapped_column(
        sa.JSON().with_variant(SUPER(), "redshift"), comment="Documento sem esquema fixo")
    mes: Mapped[str] = mapped_column(sa.String(7), nullable=False, comment="Partição YYYY-MM")


class Cliente(Base):
    """A dimensão do exemplo, para o join."""

    __tablename__ = "cad_clientes"

    id_cliente: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    nome: Mapped[str] = mapped_column(sa.String(200), nullable=False)
    mes: Mapped[str] = mapped_column(sa.String(7), nullable=False)


# Dialetos avulsos, sem engine: bastam para compilar DDL e DML. paramstyle="named" evita a dobra do
# % nos literais quando o texto é gerado com literal_binds.
DIALECTS = {
    "duckdb": duckdb_engine.Dialect(paramstyle="named"),
    "redshift": RedshiftDialect_redshift_connector(paramstyle="named"),
}

OPERACOES = [
    {"id_operacao": 1, "data_ref": dt.date(2026, 8, 1), "id_cliente": 7,
     "valor": decimal.Decimal("150.00"), "descricao": "a", "meta": {"canal": "app"},
     "mes": "2026-08"},
    {"id_operacao": 2, "data_ref": dt.date(2026, 8, 2), "id_cliente": 7,
     "valor": decimal.Decimal("50.00"), "descricao": None, "meta": None,
     "mes": "2026-08"},
    {"id_operacao": 3, "data_ref": dt.date(2026, 7, 1), "id_cliente": 9,
     "valor": decimal.Decimal("200.00"), "descricao": "c", "meta": None,
     "mes": "2026-07"},
    {"id_operacao": 4, "data_ref": dt.date(2026, 8, 3), "id_cliente": 9,
     "valor": decimal.Decimal("120.00"), "descricao": "d", "meta": None,
     "mes": "2026-08"},
]
CLIENTES = [
    {"id_cliente": 7, "nome": "Alfa", "mes": "2026-08"},
    {"id_cliente": 9, "nome": "Beta", "mes": "2026-08"},
]


def redshift_ddl(table: sa.Table) -> str:
    """O ``CREATE TABLE`` do Redshift compilado pelo dialeto, com as opções físicas de
    ``Table.info["serialize_db"]`` no fim.

    As opções ficam em ``info``, e não nos argumentos ``redshift_diststyle``, ``redshift_distkey`` e
    ``redshift_sortkey`` do ``sqlalchemy-redshift``, que só existem com o dialeto instalado: o
    modelo fica neutro. É uma função chamada por quem quer as opções, e não uma regra
    ``@compiles(CreateTable, "redshift")``, que mudaria a compilação de todo ``CreateTable`` do
    Redshift no processo, a das outras suítes inclusive. O DDL da biblioteca não passa pelo dialeto
    (``serialize_db.schema.ddl``).
    """
    text = str(CreateTable(table).compile(dialect=DIALECTS["redshift"])).rstrip()
    options = table.info.get("serialize_db", {})
    redshift = options.get("redshift", {})

    # As cláusulas físicas vêm depois do parêntese que fecha a lista de colunas.
    parts = [text]
    if redshift.get("diststyle"):
        parts.append(f"DISTSTYLE {redshift['diststyle']}")
    if redshift.get("distkey"):
        parts.append(f"DISTKEY ({redshift['distkey']})")
    if options.get("sort_key"):
        parts.append(f"SORTKEY ({', '.join(options['sort_key'])})")
    return " ".join(parts)


def normalized(sql: str) -> str:
    """O texto com espaços e quebras de linha reduzidos a um espaço, para comparações."""
    return " ".join(sql.split())


def literal_text(statement: sa.sql.ClauseElement) -> str:
    """O texto do DuckDB com as constantes embutidas (``literal_binds``), numa linha só."""
    compiled = statement.compile(dialect=DIALECTS["duckdb"], compile_kwargs={"literal_binds": True})
    return normalized(str(compiled))


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


def insert_by_arrow(engine: sa.Engine, id_operacao: int, data_ref: dt.date,
                    valor: decimal.Decimal) -> None:
    """Insere uma operação do cliente 7 em 2026-08 por uma tabela Arrow registrada na conexão
    bruta, com ``INSERT ... BY NAME`` e sem ``executemany``."""
    incoming = pa.table(
        {
            "id_operacao": pa.array([id_operacao], pa.int64()),
            "data_ref": pa.array([data_ref], pa.date32()),
            "id_cliente": pa.array([7], pa.int64()),
            "valor": pa.array([valor], pa.decimal128(18, 2)),
            "mes": pa.array(["2026-08"], pa.string()),
        }
    )
    with engine.begin() as connection:
        raw = connection.connection.dbapi_connection
        raw.register("entrada", incoming)
        connection.execute(sa.text("INSERT INTO cad_operacoes BY NAME SELECT * FROM entrada"))
        raw.unregister("entrada")


def test_declarative_model_exposes_table() -> None:
    """O modelo declarativo é um ``Table`` em ``Base.metadata``: colunas, tipos, nulidade, chave,
    comentários e ``info``."""
    table = Operacao.__table__

    assert isinstance(table, sa.Table)
    assert table.name == "cad_operacoes"
    assert Base.metadata.tables["cad_operacoes"] is table
    names = [column.name for column in table.columns]
    assert names == ["id_operacao", "data_ref", "id_cliente", "valor", "descricao", "meta", "mes"]
    assert [column.name for column in table.primary_key.columns] == ["id_operacao"]

    # Mapped[str | None] torna a coluna anulável; nullable=False e Mapped[str] a tornam obrigatória.
    assert table.c.descricao.nullable
    assert not table.c.valor.nullable

    valor = table.c.valor.type
    assert isinstance(valor, sa.Numeric)
    assert (valor.precision, valor.scale) == (18, 2)
    assert isinstance(table.c.meta.type, sa.JSON)  # with_variant preserva o tipo genérico

    assert table.info["serialize_db"]["partition_by"] == ["mes"]
    assert table.comment == "Operações do mês"
    assert table.c.id_cliente.comment == "Chave do cliente"


def test_ddl_per_dialect() -> None:
    """O mesmo ``Table`` compila o ``CREATE TABLE`` de cada motor; ``redshift_ddl`` acrescenta as
    opções físicas de ``info``."""
    duckdb_ddl = CreateTable(Operacao.__table__).compile(dialect=DIALECTS["duckdb"])
    duckdb_text = normalized(str(duckdb_ddl))
    redshift_text = normalized(redshift_ddl(Operacao.__table__))

    assert "valor NUMERIC(18, 2) NOT NULL" in duckdb_text
    assert "meta JSON" in duckdb_text
    assert "PRIMARY KEY (id_operacao)" in duckdb_text

    assert "meta SUPER" in redshift_text
    assert redshift_text.endswith(
        "DISTSTYLE KEY DISTKEY (id_cliente) SORTKEY (data_ref, id_operacao)")

    # O dialeto sozinho não lê info: o CREATE TABLE compilado sem a função termina na lista de
    # colunas.
    plain_ddl = CreateTable(Operacao.__table__).compile(dialect=DIALECTS["redshift"])
    plain = normalized(str(plain_ddl))
    assert plain.endswith(")")
    assert "DISTSTYLE" not in plain

    # O dialeto compila Text como TEXT, que o Redshift guarda como VARCHAR(256); o DDL da
    # biblioteca, gerado sem o dialeto, emite VARCHAR(65535) para Text.
    text_table = sa.Table("observacoes", sa.MetaData(), sa.Column("texto", sa.Text))
    assert "texto TEXT" in normalized(redshift_ddl(text_table))


def test_create_all_and_reflection(engine: sa.Engine) -> None:
    """``create_all`` cria as tabelas no DuckDB; a reflexão devolve colunas, tipos e comentários,
    não a chave."""
    inspector = sa.inspect(engine)
    assert {"cad_operacoes", "cad_clientes"} <= set(inspector.get_table_names())

    columns = {column["name"]: column for column in inspector.get_columns("cad_operacoes")}
    assert (columns["valor"]["type"].precision, columns["valor"]["type"].scale) == (18, 2)
    assert columns["descricao"]["type"].length is None  # o catálogo guarda VARCHAR sem comprimento
    assert columns["id_cliente"]["comment"] == "Chave do cliente"
    assert columns["descricao"]["nullable"]
    assert not columns["valor"]["nullable"]

    # A chave primária existe no catálogo, mas o duckdb_engine não a reflete.
    assert inspector.get_pk_constraint("cad_operacoes")["constrained_columns"] == []

    # Uma chave Integer de uma coluna com o autoincrement padrão sai como SERIAL, que o DuckDB não
    # tem.
    metadata = sa.MetaData()
    sa.Table("serial_probe", metadata, sa.Column("id", sa.Integer, primary_key=True))
    with pytest.raises(sa.exc.DBAPIError, match="SERIAL"):
        metadata.create_all(engine)


def test_core_insert_and_select(engine: sa.Engine) -> None:
    """``insert(...).values(lista)`` é um único comando; ``select`` com join e agregação devolve
    ``Decimal``."""
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

    # O dialeto ligado ao engine compila com $1, $2 (numeric_dollar) e lista os parâmetros em
    # positiontup.
    compiled = query.compile(dialect=engine.dialect)
    assert "$1" in str(compiled)
    assert list(compiled.positiontup) == ["mes_1"]

    # A coluna JSON devolve o dict gravado.
    first_meta = sa.select(Operacao.meta).where(Operacao.id_operacao == 1)
    with engine.connect() as connection:
        meta = connection.execute(first_meta).scalar_one()
    assert meta == {"canal": "app"}


def test_arrow_path_on_raw_connection(engine: sa.Engine) -> None:
    """O SQL compilado pelo SQLAlchemy roda na conexão DuckDB por trás do engine, com Arrow na saída
    e na entrada."""
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

    # Ingestão: a tabela Arrow registrada na conexão bruta entra por INSERT ... BY NAME, sem
    # executemany.
    insert_by_arrow(engine, 5, dt.date(2026, 8, 4), decimal.Decimal("1.00"))

    count = sa.select(sa.func.count()).select_from(Operacao)
    with engine.connect() as connection:
        assert connection.execute(count).scalar_one() == 5


def test_numeric_precision_dialect_versus_arrow(engine: sa.Engine) -> None:
    """Pelo dialeto, ``Numeric`` passa por ``float``; pelo Arrow, ``decimal128(18, 2)`` guarda os 18
    dígitos."""
    exact = decimal.Decimal("1234567890123.45")  # 15 dígitos significativos
    rounded = decimal.Decimal("123456789012345.67")  # 17 dígitos
    common = {"data_ref": dt.date(2026, 8, 1), "id_cliente": 7, "mes": "2026-08"}

    with engine.begin() as connection:
        connection.execute(
            sa.insert(Operacao).values(
                [
                    {"id_operacao": 1, "valor": exact, **common},
                    {"id_operacao": 2, "valor": rounded, **common},
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
    insert_by_arrow(engine, 3, dt.date(2026, 8, 1), big)
    with engine.connect() as connection:
        raw = connection.connection.dbapi_connection
        stored = raw.execute("SELECT valor FROM cad_operacoes WHERE id_operacao = 3").fetchone()[0]

    assert stored == big


def test_pandas_read_sql_keeps_decimal_only_with_coerce_float_off(engine: sa.Engine) -> None:
    """``pandas.read_sql`` converte ``Decimal`` em ``float`` por padrão; ``coerce_float=False``
    preserva os objetos."""
    load_sample(engine)
    query = (
        sa.select(Operacao.id_operacao, Operacao.data_ref, Operacao.valor)
        .order_by(Operacao.id_operacao)
    )

    default = pd.read_sql(query, engine)
    assert default["valor"].dtype == "float64"

    kept = pd.read_sql(query, engine, coerce_float=False)
    assert isinstance(kept["valor"].iloc[0], decimal.Decimal)
    assert isinstance(kept["data_ref"].iloc[0], dt.date)


def test_literal_binds_renders_a_bindparam_without_value_as_null(
        recwarn: pytest.WarningsRecorder) -> None:
    """Sob ``literal_binds``, o ``bindparam`` sem valor sai ``NULL`` sem erro, e o ``SAWarning`` só
    aparece numa comparação por ``=``.

    No ``LIKE``, no ``coalesce``, no ``VALUES`` de um ``INSERT`` e no ``text()`` o ``NULL`` sai
    calado, e o texto gerado filtraria ou gravaria nulo sem que ninguém percebesse. Por isso o
    ``render`` da [etapa 2](../../plan/PLAN-STAGE-2.md) não se apoia no aviso e lê
    ``compiled.binds`` (o teste seguinte).
    """
    operations = Operacao.__table__
    like_statement = sa.select(operations.c.id_operacao).where(
        operations.c.descricao.like(sa.bindparam("padrao")))
    month = sa.bindparam("mes", type_=sa.String(7))
    coalesce_statement = sa.select(sa.func.coalesce(operations.c.mes, month))
    insert_statement = sa.insert(operations).values(id_operacao=1, mes=sa.bindparam("mes"))
    text_statement = sa.select(operations.c.id_operacao).where(sa.text("mes = :mes"))

    # O fim do texto esperado de cada statement: o parâmetro virou NULL.
    silent = {
        "WHERE cad_operacoes.descricao LIKE NULL": like_statement,
        "SELECT coalesce(cad_operacoes.mes, NULL) AS coalesce_1 FROM cad_operacoes":
            coalesce_statement,
        "INSERT INTO cad_operacoes (id_operacao, mes) VALUES (1, NULL)": insert_statement,
        "WHERE mes = NULL": text_statement,
    }
    for ending, statement in silent.items():
        assert literal_text(statement).endswith(ending)
    assert [warning for warning in recwarn if issubclass(warning.category, SAWarning)] == []

    # Só a comparação por = avisa.
    compared = sa.select(operations.c.id_operacao).where(operations.c.mes == sa.bindparam("mes"))
    assert literal_text(compared).endswith("WHERE cad_operacoes.mes = NULL")
    assert "rendering literal NULL" in str(recwarn.pop(SAWarning).message)


def test_compiled_binds_marks_the_bindparam_without_value_as_required() -> None:
    """Sem ``literal_binds``, ``compiled.binds`` marca ``required`` todo ``bindparam`` sem valor, e
    um ``literal_column(":nome")`` no lugar dele atravessa ``literal_binds`` como texto.

    São as duas peças do ``render`` da [etapa 2](../../plan/PLAN-STAGE-2.md): ``required`` diz quais
    nós trocar, e ``replacement_traverse`` os troca numa cópia do statement.
    """
    operations = Operacao.__table__
    query = sa.select(operations.c.id_operacao).where(
        operations.c.mes == sa.bindparam("mes"),
        operations.c.descricao.like(sa.bindparam("padrao")),
        operations.c.id_cliente == sa.bindparam("cliente", value=7),
        operations.c.valor > decimal.Decimal("100.00"),
    )
    compiled = query.compile(dialect=DIALECTS["duckdb"])

    # Só os dois sem valor são obrigatórios: o bindparam com valor e a constante, que entra com um
    # nome anônimo, não são. construct_params junta os valores do chamador aos que o statement tem.
    required = sorted(name for name, bind in compiled.binds.items() if bind.required)
    assert required == ["mes", "padrao"]
    values = {"mes": "2026-08", "padrao": "A%"}
    constructed = compiled.construct_params(values)
    assert constructed == {**values, "cliente": 7, "valor_1": decimal.Decimal("100.00")}

    # replacement_traverse chama a função em cada nó e deixa o nó como está quando ela devolve None.
    def placeholder(element: sa.sql.ClauseElement) -> sa.sql.ClauseElement | None:
        if isinstance(element, sa.BindParameter) and element.required:
            return sa.literal_column(f":{element.key}", type_=element.type)
        return None

    copy = replacement_traverse(query, {}, placeholder)
    assert literal_text(copy).endswith(
        "WHERE cad_operacoes.mes = :mes AND cad_operacoes.descricao LIKE :padrao "
        "AND cad_operacoes.id_cliente = 7 AND cad_operacoes.valor > 100.00"
    )

    # O statement original continua com os parâmetros.
    assert normalized(str(compiled)).endswith(
        "WHERE cad_operacoes.mes = :mes AND cad_operacoes.descricao LIKE :padrao "
        "AND cad_operacoes.id_cliente = :cliente AND cad_operacoes.valor > :valor_1"
    )


def test_default_paramstyle_doubles_the_percent_in_literals() -> None:
    """Um dialeto avulso compila no ``paramstyle`` do driver, e sob ``literal_binds`` os estilos com
    ``%`` dobram o ``%`` dos literais; ``paramstyle="named"`` não dobra.

    O texto dobrado é o comando que o DBAPI recebe e desdobra, e fora dele é SQL errado: por isso o
    ``render`` da [etapa 2](../../plan/PLAN-STAGE-2.md) compila com ``paramstyle="named"``.
    """
    operations = Operacao.__table__
    query = sa.select(operations.c.id_operacao).where(
        operations.c.descricao.like("A%"),
        sa.func.strftime(operations.c.data_ref, "%Y-%m") == "2026-08",
    )

    defaults = {"duckdb": duckdb_engine.Dialect(), "redshift": RedshiftDialect_redshift_connector()}
    paramstyles = {name: dialect.paramstyle for name, dialect in defaults.items()}
    assert paramstyles == {"duckdb": "pyformat", "redshift": "format"}
    for dialect in defaults.values():
        text = str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
        assert "LIKE 'A%%'" in text
        assert "'%%Y-%%m'" in text

    for dialect in DIALECTS.values():
        text = str(query.compile(dialect=dialect, compile_kwargs={"literal_binds": True}))
        assert "LIKE 'A%'" in text
        assert "'%Y-%m'" in text


def test_dialects_quote_only_their_reserved_words() -> None:
    """Cada dialeto cita só as palavras que ele reserva, ``to`` nos dois e ``timestamp`` só no
    Redshift; ``quoted_name(quote=True)`` cita todo nome.

    É por isso que a cópia prefixada do ``render`` leva ``quote=True`` em toda tabela e coluna do
    contrato, como o DDL da etapa 1, com o sentinela ``{prefix}`` dentro das aspas. O texto
    compilado deixa um espaço antes de cada quebra de linha, que o ``render`` apara.
    """
    contracts = sa.Table(
        "cad_contratos", sa.MetaData(),
        sa.Column("to", sa.Date), sa.Column("timestamp", sa.DateTime),
        sa.Column("numero", sa.String(20)),
    )
    query = sa.select(contracts)
    duckdb_text = normalized(str(query.compile(dialect=DIALECTS["duckdb"])))
    redshift_text = normalized(str(query.compile(dialect=DIALECTS["redshift"])))
    assert duckdb_text == (
        'SELECT cad_contratos."to", cad_contratos.timestamp, cad_contratos.numero '
        "FROM cad_contratos"
    )
    assert redshift_text == (
        'SELECT cad_contratos."to", cad_contratos."timestamp", cad_contratos.numero '
        "FROM cad_contratos"
    )
    assert " \nFROM" in str(query.compile(dialect=DIALECTS["duckdb"]))

    # A cópia com todo nome em quoted_name(quote=True): o mesmo texto nos dois dialetos.
    columns = []
    for column in contracts.columns:
        columns.append(sa.Column(quoted_name(column.name, quote=True), column.type))
    copy = sa.Table(quoted_name("{prefix}cad_contratos", quote=True), sa.MetaData(), *columns)
    expected = (
        'SELECT "{prefix}cad_contratos"."to", "{prefix}cad_contratos"."timestamp", '
        '"{prefix}cad_contratos"."numero" FROM "{prefix}cad_contratos"'
    )
    for dialect in DIALECTS.values():
        assert normalized(str(sa.select(copy).compile(dialect=dialect))) == expected


def test_in_list_needs_render_postcompile_on_the_engine_path() -> None:
    """Sem ``literal_binds``, o ``IN`` de uma lista compila como ``__[POSTCOMPILE_...]``, que o
    DuckDB recusa; ``statement.params`` e ``render_postcompile`` expandem a lista e o ``bindparam``
    expansível do cliente.

    É o caminho padrão dos motores das etapas 4 e 5, o statement do cliente compilado com os
    parâmetros dele, e o cliente filtra partições por ``in_``. ``params`` dá os valores antes da
    compilação e ignora um nome que o statement não tem; o valor que falta é
    ``InvalidRequestError`` na compilação. O estilo ``qmark``, o do driver do DuckDB, roda com a
    lista de ``positiontup``, sem reescrever marcador algum.
    """
    operations = Operacao.__table__
    connection = duckdb.connect()
    connection.execute(
        "CREATE TABLE cad_operacoes (id_operacao BIGINT, id_cliente BIGINT, mes VARCHAR)")
    connection.execute(
        "INSERT INTO cad_operacoes VALUES (1, 7, '2026-06'), (2, 7, '2026-07'), (3, 9, '2026-08')")
    in_list = sa.select(operations.c.id_operacao).where(
        operations.c.mes.in_(["2026-07", "2026-08"]))

    # Sem render_postcompile: o marcador fica no texto, e o DuckDB não o lê.
    compiled = in_list.compile(dialect=DIALECTS["duckdb"])
    text = normalized(str(compiled))
    assert "IN (__[POSTCOMPILE_mes_1])" in text
    with pytest.raises(duckdb.InvalidInputException, match="Parameter argument/count mismatch"):
        connection.execute(text, compiled.construct_params())

    # Os parâmetros do cliente entram por params, e render_postcompile expande as listas.
    statement = sa.select(operations.c.id_operacao).where(
        operations.c.mes.in_(sa.bindparam("meses", expanding=True)),
        operations.c.id_cliente == sa.bindparam("cliente"),
    )
    qmark = duckdb_engine.Dialect(paramstyle="qmark")
    postcompile = {"render_postcompile": True}
    bound = statement.params(meses=["2026-06", "2026-07"], cliente=7, sobra=1)
    compiled = bound.compile(dialect=qmark, compile_kwargs=postcompile)
    values = compiled.construct_params()
    arguments = [values[name] for name in compiled.positiontup]
    expanded = normalized(str(compiled))
    assert expanded.endswith("WHERE cad_operacoes.mes IN (?, ?) AND cad_operacoes.id_cliente = ?")
    assert arguments == ["2026-06", "2026-07", 7]  # o nome que sobra não aparece
    assert connection.execute(str(compiled), arguments).fetchall() == [(1,), (2,)]

    # O valor que falta é recusado na compilação.
    missing = statement.params(meses=["2026-06"])
    with pytest.raises(InvalidRequestError,
                       match="A value is required for bind parameter 'cliente'"):
        missing.compile(dialect=qmark, compile_kwargs=postcompile)
    connection.close()


def test_generic_function_subclass_registers_in_sa_func_for_the_whole_process() -> None:
    """Uma subclasse de ``GenericFunction`` se registra em ``sa.func`` pelo nome, para o processo
    inteiro; uma de ``FunctionElement``, com ``@compiles`` por dialeto, compila igual sem registro.

    Um ``json_valid`` da auditoria como ``GenericFunction``, com ``@compiles`` para o Redshift,
    faria o ``sa.func.json_valid`` do próprio cliente sair pela regra da auditoria no Redshift
    depois do import da biblioteca. Os nomes das classes abaixo são únicos, para o registro que a
    primeira deixa não alcançar outro teste.
    """
    column = sa.column("meta", sa.String)
    assert type(sa.func.serialize_db_sonda_len(column)) is Function

    # N801: o nome da classe é o nome da função SQL.
    class serialize_db_sonda_len(GenericFunction):  # noqa: N801
        type = sa.Integer()
        inherit_cache = True

    assert type(sa.func.serialize_db_sonda_len(column)) is serialize_db_sonda_len

    # A subclasse de FunctionElement, como o month_of de plan/sqlalchemy.md: uma regra por dialeto,
    # e o sa.func intacto.
    # N801: o nome da classe segue o da função SQL.
    class serialize_db_sonda_bytes(FunctionElement):  # noqa: N801
        type = sa.Integer()
        name = "serialize_db_sonda_bytes"
        inherit_cache = True

    @compiles(serialize_db_sonda_bytes, "duckdb")
    def _duckdb_bytes(element: FunctionElement, compiler: sa.sql.compiler.SQLCompiler,
                      **kw: object) -> str:
        return f"strlen({compiler.process(element.clauses, **kw)})"

    @compiles(serialize_db_sonda_bytes, "redshift")
    def _redshift_bytes(element: FunctionElement, compiler: sa.sql.compiler.SQLCompiler,
                        **kw: object) -> str:
        return f"octet_length({compiler.process(element.clauses, **kw)})"

    duckdb_text = str(serialize_db_sonda_bytes(column).compile(dialect=DIALECTS["duckdb"]))
    redshift_text = str(serialize_db_sonda_bytes(column).compile(dialect=DIALECTS["redshift"]))
    assert duckdb_text == "strlen(meta)"
    assert redshift_text == "octet_length(meta)"
    assert type(sa.func.serialize_db_sonda_bytes(column)) is Function


def test_three_part_name_needs_quoted_name_without_quotes() -> None:
    """Um esquema ``banco.esquema``, o nome em três partes, só sai sem aspas com
    ``quoted_name(quote=False)``.

    O ``IdentifierPreparer`` cita qualquer identificador com caractere fora do permitido, e o ponto
    é um deles: o esquema em texto simples vira um nome só, entre aspas. O motor Redshift da
    [etapa 5](../../plan/PLAN-STAGE-5.md) roda ``USE`` no banco do datashare e cita
    ``esquema.tabela``; o nome em três partes serve a uma sessão aberta em outro banco, como a da
    Data API.
    """
    dialect = DIALECTS["redshift"]

    def table(schema: str) -> sa.Table:
        return sa.Table("operacoes", sa.MetaData(schema=schema),
                        sa.Column("id_operacao", sa.BigInteger, nullable=False))

    # O esquema com ponto em texto simples: um identificador só, entre aspas.
    plain = normalized(redshift_ddl(table("datalake_rw_shared.sbx_aco_decon")))
    assert plain.startswith('CREATE TABLE "datalake_rw_shared.sbx_aco_decon".operacoes')

    # Com quote=False o ponto atravessa, no DDL e no DML.
    name = quoted_name("datalake_rw_shared.sbx_aco_decon", False)
    ddl = normalized(redshift_ddl(table(name)))
    assert ddl.startswith("CREATE TABLE datalake_rw_shared.sbx_aco_decon.operacoes")
    select_text = normalized(str(sa.select(table(name)).compile(dialect=dialect)))
    assert "FROM datalake_rw_shared.sbx_aco_decon.operacoes" in select_text
    insert = sa.insert(table(name)).values([{"id_operacao": 1}])
    compiled_insert = insert.compile(dialect=dialect, compile_kwargs={"literal_binds": True})
    assert normalized(str(compiled_insert)).startswith(
        "INSERT INTO datalake_rw_shared.sbx_aco_decon.operacoes")


def test_redshift_dialect_compiles_dml() -> None:
    """O DML compila para o Redshift sem cluster: um ``INSERT`` de várias linhas com ``%s`` e o
    ``select`` do contrato."""
    dialect = RedshiftDialect_redshift_connector()

    insert = sa.insert(Operacao.__table__).values(OPERACOES[:2])
    text = str(insert.compile(dialect=dialect))
    assert text.startswith("INSERT INTO cad_operacoes")
    assert text.count("(%s, ") == 2  # um comando, duas linhas
    assert text.count("json_parse(%s)") == 2  # o parâmetro da coluna SUPER entra por json_parse

    # UPDATE ... FROM e DELETE ... USING, que o Redshift aceita, compilam pelo Core.
    operations = Operacao.__table__
    clients = Cliente.__table__
    update = (
        sa.update(operations)
        .where(operations.c.id_cliente == clients.c.id_cliente)
        .values(descricao=clients.c.nome)
    )
    update_text = normalized(str(update.compile(dialect=dialect)))
    assert update_text.startswith(
        "UPDATE cad_operacoes SET descricao=cad_clientes.nome FROM cad_clientes")

    delete = sa.delete(operations).where(operations.c.id_cliente == clients.c.id_cliente)
    assert "USING cad_clientes" in normalized(str(delete.compile(dialect=dialect)))


def test_arrow_and_delta_schema_from_table() -> None:
    """``Schema.from_arrow`` leva ao esquema Delta os tipos, a nulidade e o comentário de cada campo
    Arrow, e o ``PARQUET:field_id`` como ``parquet.field.id`` inteiro.

    O esquema Arrow é montado à mão com colunas de ``Operacao``. Com o ``parquet.field.id`` no
    esquema Delta, o ``delta_scan`` do DuckDB lê toda coluna como nula, e o esquema Delta do
    contrato sai sem ele: ``serialize_db.schema.arrow_schema`` e ``delta_schema``
    (``tests/test_schema.py``).
    """
    schema = pa.schema(
        [
            pa.field("id_operacao", pa.int64(), nullable=False,
                     metadata={"PARQUET:field_id": "1"}),
            pa.field("data_ref", pa.date32(), metadata={"PARQUET:field_id": "2"}),
            pa.field("id_cliente", pa.int64(),
                     metadata={"PARQUET:field_id": "3", "comment": "Chave do cliente"}),
            pa.field("valor", pa.decimal128(18, 2), metadata={"PARQUET:field_id": "4"}),
            pa.field("meta", pa.string(), metadata={"PARQUET:field_id": "6"}),
            pa.field("mes", pa.string(), nullable=False, metadata={"PARQUET:field_id": "7"}),
        ]
    )

    # Schema.from_arrow leva o PARQUET:field_id do Arrow ao esquema Delta, como parquet.field.id
    # inteiro.
    carried = {}
    for field in json.loads(DeltaSchema.from_arrow(schema).to_json())["fields"]:
        carried[field["name"]] = field
    assert carried["mes"]["metadata"]["parquet.field.id"] == 7
    assert carried["id_operacao"]["type"] == "long"
    assert carried["valor"]["type"] == "decimal(18,2)"
    assert carried["data_ref"]["type"] == "date"
    assert carried["meta"]["type"] == "string"
    assert carried["id_cliente"]["metadata"]["comment"] == "Chave do cliente"
    assert carried["id_operacao"]["nullable"] is False

    # Um timestamp sem fuso vira timestamp_ntz; com fuso, timestamp.
    stamped = pa.schema([("local", pa.timestamp("us")), ("utc", pa.timestamp("us", tz="UTC"))])
    kinds = {}
    for field in json.loads(DeltaSchema.from_arrow(stamped).to_json())["fields"]:
        kinds[field["name"]] = field["type"]
    assert kinds == {"local": "timestamp_ntz", "utc": "timestamp"}


def test_sandbox_copy_of_table_and_schema_files_diff() -> None:
    """``to_metadata`` dá a cópia com prefixo e esquema para o sandbox, com as opções físicas de
    ``info``.

    Os arquivos de esquema gerados e o diff contra os versionados são de ``serialize_db.schema``
    (``tests/test_schema.py``).
    """
    table = Operacao.__table__

    # A cópia renomeada no esquema: esquema.tabela, o nome em duas partes que o motor Redshift cita
    # depois do USE. info vem junto, e com ele as opções físicas.
    sandbox = table.to_metadata(sa.MetaData(schema="projeto"), name="exec_42_cad_operacoes")
    ddl = normalized(redshift_ddl(sandbox))
    assert ddl.startswith("CREATE TABLE projeto.exec_42_cad_operacoes (")
    assert ddl.endswith("SORTKEY (data_ref, id_operacao)")
    assert sandbox.c.valor.type.scale == 2
    assert sandbox.info == table.info
