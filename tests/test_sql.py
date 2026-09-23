"""``serialize_db.sql``: parâmetro, prefixo, texto por motor, ``bind``, tabelas referenciadas e arquivos.

Os testes correm sobre o statement de ``plan/sqlalchemy.md`` (o parâmetro, um ``%`` e um ``:``
em literais, duas tabelas do contrato), sobre os quatro statements do pipeline fictício de
``tests/client_model/statements.py``, com os arquivos versionados em ``tests/client_model/sql/``,
e sobre uma tabela cujos identificadores carregam ``:`` e ``'``. Nada é gravado, exceto o teste
marcado ``local``, que grava os arquivos de texto SQL sob ``SERIALIZE_DB_TEST_LOCAL_ROOT``; o
texto executa num DuckDB em memória sobre o DDL da etapa 1, e o statement com ``bindparam`` num
``sqlalchemy.Connection`` do ``duckdb-engine`` criado fora da biblioteca.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest
import sqlalchemy as sa
import sqlglot
from sqlglot.errors import TokenError

from client_model import Base as ClientBase
from client_model.statements import STATEMENTS
from serialize_db import schema, sql
from serialize_db.cli import main
from serialize_db.errors import SqlError

SQL_DIRECTORY = Path(__file__).parent / "client_model" / "sql"
PARTITION = {"data_base_str": "2026-08-31"}

# O statement de plan/sqlalchemy.md: o parâmetro, o % e o : em literais, duas tabelas do contrato.
METADATA = sa.MetaData()
ENTRIES = sa.Table(
    "cad_lancamentos", METADATA,
    sa.Column("id_lancamento", sa.BigInteger), sa.Column("id_conta", sa.BigInteger),
    sa.Column("valor", sa.Double), sa.Column("area", sa.String(50)),
    sa.Column("data_base_str", sa.String(10)),
)
ACCOUNTS = sa.Table(
    "cad_contas", METADATA,
    sa.Column("id_conta", sa.BigInteger), sa.Column("numero", sa.String(30)),
)
TOTAL_BY_ACCOUNT = (
    sa.select(ACCOUNTS.c.numero, sa.func.sum(ENTRIES.c.valor).label("total"))
    .join_from(ENTRIES, ACCOUNTS, ENTRIES.c.id_conta == ACCOUNTS.c.id_conta)
    .where(ENTRIES.c.data_base_str == sa.bindparam("data_base_str", type_=sa.String(10)),
           ENTRIES.c.area.like("TI:%"), ACCOUNTS.c.numero != "1:2")
    .group_by(ACCOUNTS.c.numero)
    .order_by(ACCOUNTS.c.numero)
)
EXPECTED_TEXT = """\
SELECT "{prefix}cad_contas"."numero", sum("{prefix}cad_lancamentos"."valor") AS total
FROM "{prefix}cad_lancamentos" JOIN "{prefix}cad_contas" ON "{prefix}cad_lancamentos"."id_conta" = "{prefix}cad_contas"."id_conta"
WHERE "{prefix}cad_lancamentos"."data_base_str" = :data_base_str AND "{prefix}cad_lancamentos"."area" LIKE 'TI:%' AND "{prefix}cad_contas"."numero" != '1:2' GROUP BY "{prefix}cad_contas"."numero" ORDER BY "{prefix}cad_contas"."numero"\
"""

# Os statements mudados para os testes do diff e da linha de comando: `saldos_por_conta` com uma
# coluna a mais; `changed` é importável como `test_sql:changed` pela linha de comando.
changed = dict(STATEMENTS)
changed["saldos_por_conta"] = STATEMENTS["saldos_por_conta"].add_columns(
    sa.func.count().label("lancamentos"))


def draft_sandbox(prefix: str) -> duckdb.DuckDBPyConnection:
    """Um DuckDB em memória com as duas tabelas do statement de teste, criadas pelo DDL da etapa 1
    com o prefixo, e as linhas do rascunho de ``plan/PLAN-STAGE-2.md``."""
    connection = duckdb.connect()
    for table in (ENTRIES, ACCOUNTS):
        connection.execute(schema.ddl(table, "duckdb", prefix=prefix))
    connection.execute(
        f'INSERT INTO "{prefix}cad_lancamentos" VALUES (1, 7, 150, \'TI:infra\', \'2026-08-31\'), '
        "(2, 7, 50, 'RH', '2026-08-31'), (3, 9, 200, 'TI:dados', '2026-07-31')")
    connection.execute(f'INSERT INTO "{prefix}cad_contas" VALUES (7, \'1.1\'), (9, \'1:2\')')
    return connection


def client_sandbox(prefix: str) -> duckdb.DuckDBPyConnection:
    """Um DuckDB em memória com as 12 tabelas do modelo cliente, prefixadas, e um lançamento."""
    connection = duckdb.connect()
    for table in ClientBase.metadata.sorted_tables:
        connection.execute(schema.ddl(table, "duckdb", prefix=prefix))
    connection.execute(
        f'INSERT INTO "{prefix}cad_lancamentos" ("id_lancamento", "id_veiculo", "id_conta", "data", '
        '"valor", "timestamp", "id_mensuracao", "data_base", "data_base_str") VALUES '
        "(1, 5, 7, DATE '2026-09-30', 150, TIMESTAMP '2026-08-31 12:00:00', 1, "
        "DATE '2026-08-31', '2026-08-31')")
    return connection


# ---------------------------------------------------------------- o texto por motor


def test_render_embeds_constants_and_keeps_parameters() -> None:
    """Constantes embutidas, `:nome` preservado, `%` simples, todo nome do contrato entre aspas com
    o sentinela dentro delas, e o mesmo texto nos dois motores para os statements portáveis."""
    assert sql.render(TOTAL_BY_ACCOUNT, "duckdb", METADATA) == EXPECTED_TEXT
    assert sql.render(TOTAL_BY_ACCOUNT, "redshift", METADATA) == EXPECTED_TEXT
    assert sql.render(TOTAL_BY_ACCOUNT, "duckdb", METADATA, prefix="exec_42_").startswith(
        'SELECT "exec_42_cad_contas"."numero"')

    # As colunas `to` e `timestamp` e o CAST do modelo cliente saem iguais nos dois motores.
    for dialect in ("duckdb", "redshift"):
        text = sql.render(STATEMENTS["lancamentos_por_contrato"], dialect, ClientBase.metadata)
        assert '"{prefix}cad_contratos"."to"' in text
        assert '"{prefix}cad_lancamentos"."timestamp"' in text
        assert 'CAST("{prefix}cad_lancamentos"."valor" AS NUMERIC(18, 2)) AS valor_centavos' in text
    for name, statement in STATEMENTS.items():
        duckdb_text = sql.render(statement, "duckdb", ClientBase.metadata)
        assert duckdb_text == sql.render(statement, "redshift", ClientBase.metadata), name
        for line in duckdb_text.splitlines():
            assert line == line.rstrip(), (name, line)


@pytest.mark.parametrize("prefix", ["", "exec_42_"])
def test_rendered_text_runs_in_duckdb(prefix: str) -> None:
    """O texto com o prefixo informado passa por `bind` e roda num DuckDB em memória com `$nome`."""
    text, values = sql.bind(
        sql.render(TOTAL_BY_ACCOUNT, "duckdb", METADATA, prefix=prefix), PARTITION, "duckdb")
    assert "$data_base_str" in text and ":data_base_str" not in text
    assert "LIKE 'TI:%'" in text and "!= '1:2'" in text
    assert draft_sandbox(prefix).execute(text, values).fetchall() == [("1.1", 150.0)]

    # Cada statement do pipeline fictício roda sobre o DDL das 12 tabelas do modelo cliente; o
    # INSERT cria o veículo que o lançamento cita e, repetido, não o cria de novo.
    connection = client_sandbox(prefix)
    for name, statement in STATEMENTS.items():
        text, values = sql.bind(
            sql.render(statement, "duckdb", ClientBase.metadata, prefix=prefix), PARTITION, "duckdb")
        connection.execute(text, values)
    text, values = sql.bind(
        sql.render(STATEMENTS["veiculos_novos"], "duckdb", ClientBase.metadata, prefix=prefix),
        PARTITION, "duckdb")
    connection.execute(text, values)
    vehicles = connection.execute(f'SELECT * FROM "{prefix}dom_veiculos"').fetchall()
    assert vehicles == [(5, "veículo 5")]


def test_render_writes_a_bindparam_without_value_as_placeholder() -> None:
    """Um `bindparam` sem valor sai como `:nome`, num `text()` inclusive, e o statement original
    fica intacto; com valor, é constante; um nome fora de `[a-z_][a-z0-9_]*` é `SqlError`."""
    statement = sa.select(ENTRIES.c.id_conta).where(ENTRIES.c.area == sa.bindparam("area"))
    assert sql.render(statement, "duckdb", METADATA, prefix="").endswith('"area" = :area')
    assert "area" in statement.compile().binds
    fragment = sa.select(ENTRIES.c.id_conta).where(sa.text('"area" = :area'))
    assert sql.render(fragment, "redshift", METADATA, prefix="").endswith('"area" = :area')
    with_value = sa.select(ENTRIES.c.id_conta).where(
        ENTRIES.c.area == sa.bindparam("area", value="RH"))
    assert sql.render(with_value, "duckdb", METADATA).endswith("\"area\" = 'RH'")
    invalid = sa.select(ENTRIES.c.id_conta).where(ENTRIES.c.area == sa.bindparam("Data Base"))
    with pytest.raises(SqlError, match="nome de parâmetro inválido: 'Data Base'"):
        sql.render(invalid, "duckdb", METADATA)


def test_statement_with_bindparam_runs_on_a_client_connection() -> None:
    """O statement escrito com `bindparam` roda num `sqlalchemy.Connection` criado fora da
    biblioteca, com o dicionário de parâmetros: um statement serve ao `Connection` do cliente, aos
    motores e aos arquivos."""
    engine = sa.create_engine("duckdb:///:memory:")
    with engine.begin() as connection:
        METADATA.create_all(connection)
        connection.execute(sa.insert(ENTRIES), [
            {"id_lancamento": 1, "id_conta": 7, "valor": 150, "area": "TI:infra",
             "data_base_str": "2026-08-31"},
            {"id_lancamento": 2, "id_conta": 7, "valor": 50, "area": "RH",
             "data_base_str": "2026-08-31"},
            {"id_lancamento": 3, "id_conta": 9, "valor": 200, "area": "TI:dados",
             "data_base_str": "2026-07-31"},
        ])
        connection.execute(sa.insert(ACCOUNTS), [
            {"id_conta": 7, "numero": "1.1"}, {"id_conta": 9, "numero": "1:2"}])
        assert connection.execute(TOTAL_BY_ACCOUNT, PARTITION).fetchall() == [("1.1", 150.0)]
    engine.dispose()


# ---------------------------------------------------------------- o bind


def test_bind_leaves_quoted_literals_and_casts_alone() -> None:
    """`'TI:%'`, `'12:30'` e `valor::DECIMAL(18, 2)` intactos; só o `:nome` do dicionário muda."""
    text = "SELECT valor::DECIMAL(18, 2), '12:30' FROM t WHERE k = :k AND area LIKE 'TI:%'"
    params = {"k": 1}
    for style, marker in (("duckdb", "$"), ("redshift", ":")):
        bound, values = sql.bind(text, params, style)
        assert bound == text.replace(" :k", f" {marker}k")
        assert values == params and values is not params


def test_bind_leaves_quoted_identifiers_alone() -> None:
    """As colunas `taxa :base`, `:base` e `preco d'agua` saem intactas entre aspas duplas, e o
    `:nome` fora das aspas é o único trocado."""
    quoted_metadata = sa.MetaData()
    table = sa.Table(
        "t", quoted_metadata,
        sa.Column("taxa :base", sa.String(10)), sa.Column(":base", sa.String(10)),
        sa.Column("preco d'agua", sa.Double), sa.Column("data_str", sa.String(10)),
    )
    query = (
        sa.select(table.c["taxa :base"], table.c[":base"], table.c["preco d'agua"])
        .where(table.c.data_str == sa.bindparam("data_str"))
    )
    text, values = sql.bind(
        sql.render(query, "duckdb", quoted_metadata, prefix=""), {"data_str": "2026-08-31"}, "duckdb")
    assert text.splitlines() == [
        'SELECT "t"."taxa :base", "t".":base", "t"."preco d\'agua"',
        'FROM "t"',
        'WHERE "t"."data_str" = $data_str',
    ]
    assert values == {"data_str": "2026-08-31"}


def test_bind_refuses_missing_and_extra_parameters() -> None:
    """Faltante e sobrando são `SqlError` com os dois conjuntos na mensagem."""
    with pytest.raises(SqlError, match=r"texto \['a', 'b'\] e do dicionário \['a'\]"):
        sql.bind("SELECT 1 WHERE x = :a AND y = :b", {"a": 1}, "duckdb")
    with pytest.raises(SqlError, match=r"texto \['a'\] e do dicionário \['a', 'b'\]"):
        sql.bind("SELECT 1 WHERE x = :a", {"a": 1, "b": 2}, "redshift")


# ---------------------------------------------------------------- o prefixo e as tabelas


def test_prefixed_replaces_every_contract_table() -> None:
    """Tabelas e colunas trocadas em `select`, `insert ... from_select` e `join`; o original intacto."""
    insert = sa.insert(ACCOUNTS).from_select(
        ["id_conta", "numero"], sa.select(ENTRIES.c.id_conta, ENTRIES.c.area).distinct())
    for statement in (TOTAL_BY_ACCOUNT, insert):
        before = sql.referenced_tables(statement)
        copy = sql.prefixed(statement, METADATA, "exec_42_")
        assert sql.referenced_tables(copy) == {f"exec_42_{name}" for name in before}
        assert sql.referenced_tables(statement) == before
        assert "exec_42_" not in str(statement)
    text = " ".join(sql.render(insert, "duckdb", METADATA, prefix="exec_42_").split())
    assert text == (
        'INSERT INTO "exec_42_cad_contas" ("id_conta", "numero") SELECT DISTINCT '
        '"exec_42_cad_lancamentos"."id_conta", "exec_42_cad_lancamentos"."area" '
        'FROM "exec_42_cad_lancamentos"')
    # O alvo do INSERT do pipeline fictício e a subconsulta do NOT EXISTS também são trocados.
    copy = sql.prefixed(STATEMENTS["veiculos_novos"], ClientBase.metadata, "exec_42_")
    assert sql.referenced_tables(copy) == {"exec_42_cad_lancamentos", "exec_42_dom_veiculos"}


def test_referenced_tables_from_core_and_text() -> None:
    """`find_tables` e o sentinela dão o mesmo conjunto para o mesmo comando."""
    assert sql.referenced_tables(TOTAL_BY_ACCOUNT) == {"cad_contas", "cad_lancamentos"}
    assert sql.referenced_tables(STATEMENTS["veiculos_novos"]) == {"cad_lancamentos", "dom_veiculos"}
    for name, statement in STATEMENTS.items():
        text = sql.render(statement, "duckdb", ClientBase.metadata)
        assert sql.referenced_tables(text) == sql.referenced_tables(statement), name
    assert sql.referenced_tables(sql.render(TOTAL_BY_ACCOUNT, "duckdb", METADATA, prefix="")) == set()


# ---------------------------------------------------------------- os arquivos gerados


def test_sql_files_match_versioned() -> None:
    """Os arquivos versionados em `tests/client_model/sql/` são a geração nova, com o sentinela."""
    files = sql.sql_files(STATEMENTS, ClientBase.metadata)
    assert len(files) == 8
    assert sorted(path.name for path in SQL_DIRECTORY.iterdir()) == sorted(files)
    for name, content in files.items():
        assert (SQL_DIRECTORY / name).read_text(encoding="utf-8") == content, name
        assert content.endswith("\n") and "{prefix}" in content, name
    assert sql.check_sql_files(STATEMENTS, ClientBase.metadata, str(SQL_DIRECTORY)) == []


def test_check_sql_files_reports_a_changed_statement() -> None:
    """Uma coluna acrescentada ao statement aparece no diff dos dois motores."""
    diff = sql.check_sql_files(changed, ClientBase.metadata, str(SQL_DIRECTORY))
    added = [line for line in diff if line.startswith("+") and "count(*) AS lancamentos" in line]
    assert len(added) == 2
    assert any(line.startswith("+++ ") and line.endswith("(gerado)") for line in diff)


def test_read_sql_fills_the_sentinel() -> None:
    """`read_sql` troca o sentinela pelo prefixo informado; o sentinela que sobra é `SqlError` no `bind`."""
    plain = sql.read_sql(str(SQL_DIRECTORY), "saldos_por_conta", "duckdb", prefix="")
    assert 'FROM "cad_lancamentos" JOIN "cad_contas"' in plain
    sandbox = sql.read_sql(str(SQL_DIRECTORY), "saldos_por_conta", "redshift", prefix="exec_42_")
    assert 'FROM "exec_42_cad_lancamentos" JOIN "exec_42_cad_contas"' in sandbox
    assert "{prefix}" not in sandbox
    bound, _ = sql.bind(sandbox, PARTITION, "redshift")
    assert '"data_base_str" = :data_base_str' in bound

    versioned = (SQL_DIRECTORY / "saldos_por_conta.duckdb.sql").read_text(encoding="utf-8")
    with pytest.raises(SqlError, match="ainda traz o sentinela"):
        sql.bind(versioned, PARTITION, "duckdb")


@pytest.mark.local
def test_write_sql_files(local_location) -> None:
    """Os arquivos gravados sob a raiz local, com os nomes previstos, e o `check` vazio depois."""
    directory = local_location.child("sql")
    written = sql.write_sql_files(STATEMENTS, ClientBase.metadata, directory)
    assert len(written) == 8
    names = sorted(Path(path).name for path in written)
    assert names == sorted(sql.sql_files(STATEMENTS, ClientBase.metadata))
    assert sql.check_sql_files(STATEMENTS, ClientBase.metadata, directory) == []
    text = sql.read_sql(directory, "veiculos_novos", "duckdb", prefix="exec_42_")
    assert text.startswith('INSERT INTO "exec_42_dom_veiculos" ("id_veiculo", "nome")')


def test_cli_sql_check_reads_the_versioned_files(capsys: pytest.CaptureFixture) -> None:
    """`sql check` sai com 0 sem diff, 1 com o diff impresso e 2 sem `--statements`."""
    directory = str(SQL_DIRECTORY)
    assert main(["sql", "check", "--metadata", "client_model:Base.metadata",
                 "--statements", "client_model.statements:STATEMENTS", directory]) == 0
    assert "atualizados" in capsys.readouterr().out

    assert main(["sql", "check", "--metadata", "client_model:Base.metadata",
                 "--statements", "test_sql:changed", directory]) == 1
    assert "count(*) AS lancamentos" in capsys.readouterr().out

    with pytest.raises(SystemExit) as exit_code:
        main(["sql", "check", "--metadata", "client_model:Base.metadata", directory])
    assert exit_code.value.code == 2
    with pytest.raises(SystemExit) as exit_code:
        main(["sql", "check", "--metadata", "client_model:Base.metadata",
              "--statements", "client_model:Base", directory])
    assert exit_code.value.code == 2


# ---------------------------------------------------------------- o texto do Redshift analisável


def test_redshift_text_parses_with_sqlglot() -> None:
    """O texto do Redshift de cada statement analisa pelo `sqlglot`, o arquivo versionado com o
    sentinela inclusive, porque o sentinela fica dentro das aspas de um identificador; uma aspa
    desbalanceada é recusada. O teste não diz o que o Redshift suporta."""
    for name, statement in STATEMENTS.items():
        versioned = (SQL_DIRECTORY / f"{name}.redshift.sql").read_text(encoding="utf-8")
        assert type(sqlglot.parse_one(versioned, dialect="redshift")).__name__ in (
            "Select", "Insert"), name
        plain = sql.render(statement, "redshift", ClientBase.metadata, prefix="")
        assert type(sqlglot.parse_one(plain, dialect="redshift")).__name__ in (
            "Select", "Insert"), name
    with pytest.raises(TokenError):
        sqlglot.parse_one("SELECT 1 WHERE area = 'TI", dialect="redshift")
