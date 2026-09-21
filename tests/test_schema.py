"""``serialize_db.schema``: esquema Arrow e Delta, opções físicas, DDL, cast e conferência.

Os testes correm sobre três modelos: ``TUDO``, uma tabela de teste com todos os tipos do
contrato e as colunas ``to`` e ``timestamp``, palavras reservadas dos motores; o modelo cliente de
``tests/client_model/``, que obedece ao contrato e tem os arquivos de esquema versionados em
``tests/client_model/schema/``; e o modelo de referência de ``tests/reference_model/``, o modelo
com defeitos que ``check_models`` lista. Nada é gravado, exceto o teste marcado ``local``, que
grava os arquivos de esquema sob ``SERIALIZE_DB_TEST_LOCAL_ROOT``; o DDL executa num DuckDB em
memória.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json
from pathlib import Path

import duckdb
import pyarrow as pa
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from client_model import Base as ClientBase
from reference_model.model_db_projetado import Base as ReferenceBase
from serialize_db import schema
from serialize_db.cli import main
from serialize_db.errors import ContractError

SCHEMA_DIRECTORY = Path(__file__).parent / "client_model" / "schema"

# A tabela de teste: todos os tipos do contrato, a chave única por índice, a partição e as
# cláusulas físicas em Table.info, e as colunas `to` e `timestamp`.
TUDO = sa.Table(
    "tudo",
    sa.MetaData(),
    sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=False, comment="Identificador"),
    sa.Column("data", sa.Date, nullable=False, comment="Data"),
    sa.Column("nome", sa.String(100), nullable=False, comment="Nome"),
    sa.Column("to", sa.String(2), nullable=False, comment="Reservada nos dois motores"),
    sa.Column("valor", sa.Numeric(18, 2), nullable=False, comment="Valor"),
    sa.Column("spread", sa.Double, comment="Spread"),
    sa.Column("parcelas", sa.SmallInteger, comment="Parcelas"),
    sa.Column("sistema", sa.Integer, nullable=False, comment="Sistema"),
    sa.Column("ativa", sa.Boolean, nullable=False, comment="Se está ativa"),
    sa.Column("observacao", sa.Text, comment="Texto longo"),
    sa.Column("meta", sa.JSON, comment="Documento JSON serializado"),
    sa.Column("timestamp", sa.DateTime, nullable=False, comment="Reservada no Redshift"),
    sa.Column("carimbo_utc", sa.DateTime(timezone=True), comment="Gravação em UTC"),
    sa.Column("chave", sa.Uuid, comment="UUID como texto"),
    sa.Column("data_str", sa.String(10), nullable=False, comment="Partição AAAA-MM-DD de data"),
    sa.Index("ix_tudo_data_nome", "data", "nome", unique=True),
    comment="Todos os tipos do contrato",
    info={"serialize_db": {
        "partition_by": ["data_str"], "partition_source": "data", "sort_key": ["data", "nome"],
        "redshift": {"diststyle": "KEY", "distkey": "id"},
    }},
)

EXPECTED_ARROW = {
    "id": pa.int64(), "data": pa.date32(), "nome": pa.string(), "to": pa.string(),
    "valor": pa.decimal128(18, 2), "spread": pa.float64(), "parcelas": pa.int16(),
    "sistema": pa.int32(), "ativa": pa.bool_(), "observacao": pa.string(), "meta": pa.string(),
    "timestamp": pa.timestamp("us"), "carimbo_utc": pa.timestamp("us", tz="UTC"),
    "chave": pa.string(), "data_str": pa.string(),
}

EXPECTED_DUCKDB_DDL = """CREATE TABLE "tudo" (
    "id" BIGINT NOT NULL,
    "data" DATE NOT NULL,
    "nome" VARCHAR(100) NOT NULL,
    "to" VARCHAR(2) NOT NULL,
    "valor" DECIMAL(18, 2) NOT NULL,
    "spread" DOUBLE,
    "parcelas" SMALLINT,
    "sistema" INTEGER NOT NULL,
    "ativa" BOOLEAN NOT NULL,
    "observacao" VARCHAR,
    "meta" JSON,
    "timestamp" TIMESTAMP NOT NULL,
    "carimbo_utc" TIMESTAMPTZ,
    "chave" VARCHAR(36),
    "data_str" VARCHAR(10) NOT NULL
)"""

EXPECTED_REDSHIFT_DDL = """CREATE TABLE "tudo" (
    "id" BIGINT NOT NULL,
    "data" DATE NOT NULL,
    "nome" VARCHAR(100) NOT NULL,
    "to" VARCHAR(2) NOT NULL,
    "valor" DECIMAL(18, 2) NOT NULL,
    "spread" DOUBLE PRECISION,
    "parcelas" SMALLINT,
    "sistema" INTEGER NOT NULL,
    "ativa" BOOLEAN NOT NULL,
    "observacao" VARCHAR(65535),
    "meta" SUPER,
    "timestamp" TIMESTAMP NOT NULL,
    "carimbo_utc" TIMESTAMPTZ,
    "chave" VARCHAR(36),
    "data_str" VARCHAR(10) NOT NULL
) DISTSTYLE KEY DISTKEY ("id") SORTKEY ("data", "nome")"""

# A tabela mudada para os testes do diff e da linha de comando: `cad_operacoes` com uma coluna a
# mais no fim; `changed` é importável como `test_schema:changed` pela linha de comando.
changed = sa.MetaData()
CHANGED_TABLE = ClientBase.metadata.tables["cad_operacoes"].to_metadata(changed)
CHANGED_TABLE.append_column(sa.Column("canal", sa.String(20), comment="Origem do lançamento"))


class RuimBase(DeclarativeBase):
    pass


class Ruim(RuimBase):
    """Uma tabela com cada defeito que ``check_models`` reprova."""

    __tablename__ = "ruim"
    __table_args__ = {"info": {"serialize_db": {"partition_by": ["mes"]}}}
    id: Mapped[int] = mapped_column(sa.Integer, primary_key=True)
    id_tudo: Mapped[int] = mapped_column(
        sa.BigInteger,
        sa.ForeignKey("tudo.id", deferrable=True, initially="DEFERRED"),
        comment="Referência",
    )
    mes: Mapped[str] = mapped_column(sa.String(7), comment="Mês")
    nome: Mapped[str] = mapped_column(sa.String, comment="Nome sem comprimento")
    peso: Mapped[bytes] = mapped_column(sa.LargeBinary, comment="Fora do contrato")


# A tabela referenciada por Ruim, no mesmo MetaData, para a chave estrangeira resolver.
TUDO.to_metadata(RuimBase.metadata)


def batch_of_tudo(**columns) -> pa.RecordBatch:
    """Um lote com as colunas informadas, para os testes de ``cast``."""
    return pa.RecordBatch.from_pydict(columns)


# ---------------------------------------------------------------- o esquema Arrow e Delta


def test_arrow_schema_maps_every_contract_type() -> None:
    """Cada tipo do contrato no Arrow esperado, com a nulidade, o comentário e o field_id."""
    arrow = schema.arrow_schema(TUDO)
    assert {field.name: field.type for field in arrow} == EXPECTED_ARROW
    assert not arrow.field("id").nullable and arrow.field("spread").nullable
    assert arrow.field("valor").metadata == {b"PARQUET:field_id": b"5", b"comment": b"Valor"}
    assert arrow.metadata == {b"serialize_db_table": b"tudo"}


@pytest.mark.parametrize(
    "kind", [sa.Float, sa.LargeBinary, sa.ARRAY(sa.Integer), sa.Interval], ids=str
)
def test_arrow_schema_refuses_foreign_types(kind: sa.types.TypeEngine) -> None:
    """Um tipo fora da tabela de tipos é ``ContractError`` com a tabela e a coluna."""
    table = sa.Table("estranha", sa.MetaData(), sa.Column("campo", kind, comment="Fora"))
    with pytest.raises(ContractError, match="estranha.campo: tipo fora do contrato"):
        schema.arrow_schema(table)


def test_delta_schema_json_matches_versioned_file() -> None:
    """O esquema Delta de cada tabela do modelo cliente é o `<tabela>.delta.json` versionado.

    O `to_json()` do delta-rs ordena os metadados de cada campo de forma arbitrária, então a
    comparação é do documento, e o arquivo versionado é o JSON canônico de `schema_files`.
    """
    files = schema.schema_files(ClientBase.metadata)
    for table in ClientBase.metadata.sorted_tables:
        versioned = (SCHEMA_DIRECTORY / f"{table.name}.delta.json").read_text(encoding="utf-8")
        generated = json.loads(schema.delta_schema(table).to_json())
        assert json.loads(versioned) == generated, table.name
        assert files[f"{table.name}.delta.json"] == versioned, table.name
    fields = {f["name"]: f for f in json.loads(schema.delta_schema(TUDO).to_json())["fields"]}
    assert fields["timestamp"]["type"] == "timestamp_ntz"
    assert fields["carimbo_utc"]["type"] == "timestamp"
    assert fields["valor"]["type"] == "decimal(18,2)"
    assert fields["valor"]["metadata"]["comment"] == "Valor"


def test_delta_schema_carries_no_field_id() -> None:
    """O `PARQUET:field_id` do Arrow não passa ao esquema Delta.

    Com `parquet.field.id` nos campos do esquema Delta, o `delta_scan` do DuckDB lê toda coluna
    como nula, qualquer que seja o escritor do arquivo (leitura de 2026-09-21); o comentário fica.
    """
    for table in ClientBase.metadata.sorted_tables:
        for field in json.loads(schema.delta_schema(table).to_json())["fields"]:
            assert "parquet.field.id" not in field["metadata"], (table.name, field["name"])
            assert field["metadata"]["comment"], (table.name, field["name"])
    assert schema.arrow_schema(TUDO).field("id").metadata[b"PARQUET:field_id"] == b"1"


# ---------------------------------------------------------------- as opções físicas


def test_table_options_defaults_and_keys() -> None:
    """Os padrões sem `info`, as chaves do modelo e de `keys`, e a partição de uma coluna só."""
    plain = sa.Table("simples", sa.MetaData(), sa.Column("id", sa.BigInteger, primary_key=True))
    options = schema.table_options(plain)
    assert options.partition_by is None and options.partition_source is None
    assert options.sort_key == () and options.redshift == {}
    assert options.keys == (("id",),)

    # O índice único de cad_contratos e a UniqueConstraint de cad_aliquotas são chaves.
    contratos = schema.table_options(ClientBase.metadata.tables["cad_contratos"])
    assert contratos.keys == (("id_contrato",), ("data", "sistema", "contrato"))
    aliquotas = schema.table_options(ClientBase.metadata.tables["cad_aliquotas"])
    assert aliquotas.keys == (("id",), ("id_conta_origem", "id_conta_destino"))

    adjusted = TUDO.to_metadata(sa.MetaData())
    adjusted.info = {"serialize_db": {"keys": {"add": [["nome"]], "drop": [["data", "nome"]]}}}
    assert schema.table_options(adjusted).keys == (("id",), ("nome",))

    two = sa.Table("duas", sa.MetaData(), sa.Column("id", sa.BigInteger, primary_key=True),
                   info={"serialize_db": {"partition_by": ["a", "b"]}})
    with pytest.raises(ContractError, match="duas: uma coluna de partição no máximo"):
        schema.table_options(two)


# ---------------------------------------------------------------- o DDL por motor


def test_sql_type_per_dialect() -> None:
    """Cada tipo do contrato no texto de cada motor."""
    duckdb_types = {c.name: schema.sql_type(c, "duckdb") for c in TUDO.columns}
    redshift_types = {c.name: schema.sql_type(c, "redshift") for c in TUDO.columns}
    assert duckdb_types["valor"] == redshift_types["valor"] == "DECIMAL(18, 2)"
    assert duckdb_types["nome"] == redshift_types["nome"] == "VARCHAR(100)"
    assert duckdb_types["observacao"] == "VARCHAR"
    assert redshift_types["observacao"] == "VARCHAR(65535)"
    assert duckdb_types["chave"] == redshift_types["chave"] == "VARCHAR(36)"
    assert (duckdb_types["meta"], redshift_types["meta"]) == ("JSON", "SUPER")
    assert (duckdb_types["spread"], redshift_types["spread"]) == ("DOUBLE", "DOUBLE PRECISION")
    assert duckdb_types["timestamp"] == redshift_types["timestamp"] == "TIMESTAMP"
    assert duckdb_types["carimbo_utc"] == redshift_types["carimbo_utc"] == "TIMESTAMPTZ"
    assert (duckdb_types["parcelas"], duckdb_types["sistema"], duckdb_types["id"]) == (
        "SMALLINT", "INTEGER", "BIGINT")


def test_ddl_per_dialect() -> None:
    """O texto de cada motor, linha a linha; as cláusulas físicas só no Redshift, entre aspas."""
    assert schema.ddl(TUDO, "duckdb") == EXPECTED_DUCKDB_DDL
    assert schema.ddl(TUDO, "redshift") == EXPECTED_REDSHIFT_DDL
    for dialect in ("duckdb", "redshift"):
        text = schema.ddl(TUDO, dialect)
        for absent in ("PRIMARY KEY", "UNIQUE", "REFERENCES", "DEFERRABLE", "SERIAL",
                       "IDENTITY", "CHECK", "COMMENT"):
            assert absent not in text, (dialect, absent)


def test_ddl_quotes_every_identifier() -> None:
    """Todo nome de tabela e de coluna entre aspas, `"to"` e `"timestamp"` inclusive."""
    for dialect in ("duckdb", "redshift"):
        contratos = schema.ddl(ClientBase.metadata.tables["cad_contratos"], dialect)
        lancamentos = schema.ddl(ClientBase.metadata.tables["cad_lancamentos"], dialect)
        assert '    "to" VARCHAR(2) NOT NULL' in contratos
        assert '    "timestamp" TIMESTAMP NOT NULL' in lancamentos
        for table in ClientBase.metadata.sorted_tables:
            lines = schema.ddl(table, dialect).splitlines()
            assert lines[0] == f'CREATE TABLE "{table.name}" ('
            for line in lines[1:-1]:
                assert line.startswith('    "'), (dialect, table.name, line)


def test_ddl_runs_in_duckdb_memory() -> None:
    """O DDL das 12 tabelas do modelo cliente e o da tabela de teste executam no DuckDB."""
    connection = duckdb.connect()
    for table in ClientBase.metadata.sorted_tables:
        connection.execute(schema.ddl(table, "duckdb"))
    connection.execute(schema.ddl(TUDO, "duckdb"))
    created = connection.execute("SELECT count(*) FROM information_schema.tables").fetchone()
    assert created == (13,)
    types = dict(connection.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = 'tudo'").fetchall())
    assert types["valor"] == "DECIMAL(18,2)"
    assert types["carimbo_utc"] == "TIMESTAMP WITH TIME ZONE"
    assert types["meta"] == "JSON"
    assert types["to"] == "VARCHAR" and types["timestamp"] == "TIMESTAMP"


def test_ddl_prefix_inside_the_quotes() -> None:
    """O prefixo do sandbox e o sentinela `{prefix}` saem dentro das aspas; o sentinela executa."""
    for dialect in ("duckdb", "redshift"):
        sandbox = schema.ddl(TUDO, dialect, prefix="exec_42_")
        assert sandbox.startswith('CREATE TABLE "exec_42_tudo" (')
        sentinel = schema.ddl(TUDO, dialect, prefix="{prefix}")
        assert sentinel.startswith('CREATE TABLE "{prefix}tudo" (')
    connection = duckdb.connect()
    connection.execute(schema.ddl(TUDO, "duckdb", prefix="{prefix}"))
    assert connection.execute("SELECT table_name FROM information_schema.tables").fetchone() == (
        "{prefix}tudo",)


# ---------------------------------------------------------------- o cast por lote

ACCEPTED_BATCH = pa.RecordBatch.from_pydict({
    "nome": pa.array(["a", "b"], pa.large_string()),
    "id": pa.array([1, 2], pa.int32()),
    "valor": pa.array([10, 20], pa.int64()),
    "data": pa.array([dt.datetime(2026, 8, 31), dt.datetime(2026, 8, 31)], pa.timestamp("ns")),
    "data_str": ["2026-08-31", "2026-08-31"],
    "extra": [1, 2],
})


@pytest.mark.parametrize("kind", ["batch", "table"])
def test_cast_reorders_and_normalizes(kind: str) -> None:
    """Colunas fora de ordem, `large_string`, nanossegundo zero e inteiro em Numeric entram."""
    data = ACCEPTED_BATCH if kind == "batch" else pa.Table.from_batches([ACCEPTED_BATCH])
    done = schema.cast(data, TUDO)
    assert type(done) is type(data)
    assert done.schema.names == ["id", "data", "nome", "valor", "data_str"]
    assert done.schema.field("id").type == pa.int64()
    assert done.schema.field("data").type == pa.date32()
    assert done.schema.field("nome").type == pa.string()
    assert done.column("valor").to_pylist() == [decimal.Decimal("10.00"), decimal.Decimal("20.00")]
    assert done.schema.metadata == {b"serialize_db_table": b"tudo"}


def test_cast_reader_converts_batch_by_batch() -> None:
    """Um leitor de dois lotes sai como leitor no contrato; a tabela vazia passa."""
    reader = pa.RecordBatchReader.from_batches(ACCEPTED_BATCH.schema, [ACCEPTED_BATCH] * 2)
    done = schema.cast(reader, TUDO)
    assert isinstance(done, pa.RecordBatchReader)
    assert done.schema.names == ["id", "data", "nome", "valor", "data_str"]
    assert done.read_all().num_rows == 4
    empty = schema.cast(ACCEPTED_BATCH.schema.empty_table(), TUDO)
    assert empty.num_rows == 0 and empty.schema.names == done.schema.names


REFUSED_BATCHES = {
    "nulo em NOT NULL": (batch_of_tudo(id=pa.array([1, None], pa.int64())), "id"),
    "double fora da escala": (batch_of_tudo(valor=pa.array([1.236])), "valor"),
    "hora numa coluna Date": (
        batch_of_tudo(data=pa.array([dt.datetime(2026, 8, 31, 12)], pa.timestamp("us"))), "data"),
    "struct em JSON": (batch_of_tudo(meta=pa.array([{"k": 1}])), "meta"),
    "texto acima de String(100)": (batch_of_tudo(nome=pa.array(["x" * 101])), "nome"),
    "texto acima de String(2) em bytes": (batch_of_tudo(to=pa.array(["ãã"])), "to"),
    "escala perdida": (
        batch_of_tudo(valor=pa.array([decimal.Decimal("1.234")], pa.decimal128(20, 3))), "valor"),
    "nanossegundo não nulo": (
        batch_of_tudo(timestamp=pa.array([1], pa.timestamp("ns"))), "timestamp"),
    "estouro de inteiro": (batch_of_tudo(parcelas=pa.array([40000], pa.int32())), "parcelas"),
    "nenhuma coluna do contrato": (batch_of_tudo(extra=[1]), "extra"),
}


@pytest.mark.parametrize("case", sorted(REFUSED_BATCHES))
def test_cast_refuses_each_loss(case: str) -> None:
    """Cada perda é `ContractError` com a tabela e a coluna na mensagem."""
    batch, column = REFUSED_BATCHES[case]
    with pytest.raises(ContractError) as error:
        schema.cast(batch, TUDO)
    assert str(error.value).startswith("tudo")
    assert column in str(error.value)


def test_cast_keeps_doubles_of_the_reference_model() -> None:
    """Uma coluna `Double` entra como chega, sem arredondamento; `2.675` é um double exato."""
    lancamentos = ClientBase.metadata.tables["cad_lancamentos"]
    batch = pa.RecordBatch.from_pydict({"valor": pa.array([2.675, 11846195394.62801])})
    done = schema.cast(batch, lancamentos)
    assert done.schema.field("valor").type == pa.float64()
    assert done.column("valor").to_pylist() == [2.675, 11846195394.62801]
    exact = schema.cast(batch_of_tudo(valor=pa.array([1.25, 2.5])), TUDO)
    assert exact.column("valor").to_pylist() == [decimal.Decimal("1.25"), decimal.Decimal("2.50")]
    accepted = schema.cast(batch_of_tudo(timestamp=pa.array([1000], pa.timestamp("ns"))), TUDO)
    assert accepted.schema.field("timestamp").type == pa.timestamp("us")


# ---------------------------------------------------------------- a conferência dos modelos


def test_check_models_finds_each_violation() -> None:
    """Uma tabela com cada defeito produz uma violação por defeito."""
    problems = [p for p in schema.check_models(RuimBase.metadata) if p.startswith("ruim")]
    assert problems == [
        "ruim: tabela sem comentário",
        "ruim.id: chave inteira com autoincrement; declare autoincrement=False",
        "ruim.id: coluna sem comentário",
        "ruim.nome: String sem comprimento; declare String(n) ou Text",
        "ruim.peso: tipo fora do contrato: LargeBinary()",
        "ruim: chave estrangeira DEFERRABLE em ['id_tudo']",
        "ruim.mes: coluna de partição fora de String(10)",
        "ruim: partition_by sem partition_source válido",
    ]
    assert [p for p in schema.check_models(RuimBase.metadata) if p.startswith("tudo")] == []


def test_check_models_lists_the_reference_model_defects() -> None:
    """O modelo de referência produz autoincrement, DEFERRABLE, String sem n e comentários."""
    problems = schema.check_models(ReferenceBase.metadata)
    counts = {
        "autoincrement": sum("chave inteira com autoincrement" in p for p in problems),
        "deferrable": sum("chave estrangeira DEFERRABLE" in p for p in problems),
        "string": sum("String sem comprimento" in p for p in problems),
        "table_comment": sum(p.endswith(": tabela sem comentário") for p in problems),
        "column_comment": sum(p.endswith(": coluna sem comentário") for p in problems),
    }
    tables = list(ReferenceBase.metadata.tables.values())
    columns = [column for table in tables for column in table.columns]
    strings = [c for c in columns if type(c.type) is sa.String and not c.type.length]
    foreign_keys = [fk for table in tables for fk in table.foreign_key_constraints]
    deferrable = [fk for fk in foreign_keys if fk.deferrable]
    assert counts == {
        "autoincrement": len(tables),
        "deferrable": len(deferrable),
        "string": len(strings),
        "table_comment": len(tables),
        "column_comment": len(columns),
    }
    assert sum(counts.values()) == len(problems)
    assert (len(tables), len(foreign_keys), len(deferrable), len(strings)) == (12, 14, 12, 20)


def test_client_model_is_clean() -> None:
    """O modelo cliente obedece ao contrato."""
    assert schema.check_models(ClientBase.metadata) == []


# ---------------------------------------------------------------- os arquivos de esquema


def test_schema_files_match_versioned() -> None:
    """Os arquivos versionados em `tests/client_model/schema/` são a geração nova."""
    files = schema.schema_files(ClientBase.metadata)
    assert len(files) == 36
    assert sorted(path.name for path in SCHEMA_DIRECTORY.iterdir()) == sorted(files)
    assert schema.check_schema_files(ClientBase.metadata, str(SCHEMA_DIRECTORY)) == []


def test_check_schema_files_reports_a_changed_model() -> None:
    """Uma coluna acrescentada aparece no diff dos três formatos."""
    diff = schema.check_schema_files(changed, str(SCHEMA_DIRECTORY))
    added = [line for line in diff if line.startswith("+") and "canal" in line]
    assert len(added) == 3
    assert '+    "canal" VARCHAR(20)' in added
    assert any(line.startswith("+++ ") and line.endswith("(gerado)") for line in diff)


@pytest.mark.local
def test_write_schema_files(local_location) -> None:
    """Os arquivos gravados sob a raiz local, com os nomes previstos, e o `check` vazio depois."""
    directory = local_location.child("schema")
    written = schema.write_schema_files(ClientBase.metadata, directory)
    assert len(written) == 36
    names = sorted(Path(path).name for path in written)
    assert names == sorted(schema.schema_files(ClientBase.metadata))
    assert schema.check_schema_files(ClientBase.metadata, directory) == []


def test_cli_schema_check_reads_the_versioned_files(capsys: pytest.CaptureFixture) -> None:
    """`schema check` sai com 0 sem diff, 1 com o diff impresso e 2 sem `--metadata`."""
    assert main(["schema", "check", "--metadata", "client_model:Base.metadata",
                 str(SCHEMA_DIRECTORY)]) == 0
    assert "atualizados" in capsys.readouterr().out

    directory = str(SCHEMA_DIRECTORY)
    assert main(["schema", "check", "--metadata", "test_schema:changed", directory]) == 1
    assert '+    "canal" VARCHAR(20)' in capsys.readouterr().out

    with pytest.raises(SystemExit) as exit_code:
        main(["schema", "check", str(SCHEMA_DIRECTORY)])
    assert exit_code.value.code == 2
    with pytest.raises(SystemExit) as exit_code:
        main(["schema", "check", "--metadata", "client_model:Base", str(SCHEMA_DIRECTORY)])
    assert exit_code.value.code == 2
