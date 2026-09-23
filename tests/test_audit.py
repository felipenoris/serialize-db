"""``serialize_db.audit``: as verificações do contrato e o texto delas por dialeto, sem gravar.

Os testes conferem o texto de cada verificação do modelo cliente nos dois dialetos, com as funções
de cada motor e sem a cláusula ``FILTER``, que o Redshift não tem; o texto do DuckDB rodando num
DuckDB em memória sobre o DDL da etapa 1; o escopo da chave pela coluna de partição e pela de
``partition_source``, com ``key_scope`` e o ``skip_when`` da chave primária inteira; as chaves
estrangeiras só com ``foreign_keys=True``; e a recusa do valor de partição fora da regra.
"""

from __future__ import annotations

import datetime as dt

import duckdb
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from client_model import Base as ClientBase
from serialize_db import audit, schema
from serialize_db.errors import ContractError

CLIENT_TABLES = ClientBase.metadata.tables


class Base(DeclarativeBase):
    pass


class Evento(Base):
    """Uma tabela com uma coluna de cada tipo que muda de função entre os motores."""

    __tablename__ = "cad_eventos"
    __table_args__ = {"info": {"serialize_db": {"partition_by": ["data_str"], "partition_source": "data"}}}
    id_evento: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    data: Mapped[dt.date] = mapped_column(sa.Date)
    valor: Mapped[float] = mapped_column(sa.Double)
    documento: Mapped[dict | None] = mapped_column(sa.JSON)
    nota: Mapped[str | None] = mapped_column(sa.Text)
    data_str: Mapped[str] = mapped_column(sa.String(10))


def published_source(table: sa.Table) -> sa.FromClause:
    """Uma origem com as colunas da tabela, no papel da versão publicada."""
    return sa.table("publicada", *[sa.column(column.name, column.type) for column in table.columns]).alias("publicado")


def partitions_of(table: sa.Table) -> list[str] | None:
    """Uma partição para a tabela particionada, nenhuma para a sem partição."""
    return ["2026-08-31"] if schema.table_options(table).partition_by else None


def test_audit_sql_per_dialect() -> None:
    """Cada verificação do modelo cliente renderiza nos dois dialetos, com as funções de cada motor e
    sem ``FILTER`` no Redshift; o texto do DuckDB roda sobre o DDL da etapa 1."""
    connection = duckdb.connect()
    for table in ClientBase.metadata.sorted_tables:
        connection.execute(schema.ddl(table, "duckdb"))
    for table in ClientBase.metadata.sorted_tables:
        duckdb_texts = audit.audit_sql(table, "duckdb", partitions_of(table), prefix="")
        redshift_texts = audit.audit_sql(table, "redshift", partitions_of(table), prefix="exec_42_")
        assert duckdb_texts.keys() == redshift_texts.keys()
        assert all("FILTER" not in text for text in redshift_texts.values())
        assert all('"exec_42_' in text for text in redshift_texts.values())
        for text in duckdb_texts.values():
            connection.execute(text).fetchall()
    connection.close()

    rows = {dialect: audit.audit_sql(Evento.__table__, dialect, ["2026-08-31"], prefix="")["linhas"] for dialect in ("duckdb", "redshift")}
    assert "strftime(\"cad_eventos\".\"data\", '%Y-%m-%d')" in rows["duckdb"]
    assert "to_char(\"cad_eventos\".\"data\", 'YYYY-MM-DD')" in rows["redshift"]
    assert "json_valid(" in rows["duckdb"] and "is_valid_json(" in rows["redshift"]
    assert "strlen(\"cad_eventos\".\"nota\") > 65535" in rows["duckdb"] and "octet_length(" in rows["redshift"]
    assert "regexp_full_match(\"cad_eventos\".\"data_str\", '[0-9A-Za-z][0-9A-Za-z_.-]*')" in rows["duckdb"]
    assert "~ '^[0-9A-Za-z][0-9A-Za-z_.-]*$'" in rows["redshift"]
    assert "isfinite(\"cad_eventos\".\"valor\")" in rows["duckdb"] and "'NaN'::float8" in rows["redshift"]
    assert "count(CASE WHEN" in rows["duckdb"] and "AS NUMERIC(38, 6)" in rows["duckdb"]

    # As funções da auditoria não se registram em sa.func: o cliente continua com as suas.
    assert type(sa.func.json_valid(sa.column("x"))) is sa.sql.functions.Function


def test_key_scope_follows_the_partition_column() -> None:
    """A chave com a coluna de ``partition_source`` fica nas partições da execução; a chave primária
    ganha a consulta contra a versão publicada, com o ``skip_when`` do ``max_key``;
    ``key_scope="partition"`` a suprime e registra, ``"table"`` a estende à chave da origem."""
    contracts = CLIENT_TABLES["cad_contratos"]
    published = published_source(contracts)

    found, not_run = audit.checks_and_not_run(contracts, ["2026-08-31"], False, None, published, None, 500)
    names = [check.name for check in found]
    assert names == ["linhas", "chave_id_contrato", "chave_id_contrato_publicada", "chave_data_sistema_contrato"]
    against = found[2]
    assert against.skip_when is not None and "min(" in str(against.skip_when) and not_run == []

    found, not_run = audit.checks_and_not_run(contracts, ["2026-08-31"], False, "partition", published, None, 500)
    assert "chave_id_contrato_publicada" not in [check.name for check in found]
    assert not_run == ["chave_id_contrato_publicada (key_scope=partition)"]

    found, _ = audit.checks_and_not_run(contracts, ["2026-08-31"], False, "table", published, None, None)
    assert [check.name for check in found if check.name.endswith("_publicada")] == [
        "chave_id_contrato_publicada", "chave_data_sistema_contrato_publicada"]
    assert all(check.skip_when is None for check in found)  # sem published_max_key

    # Sem versão publicada, a tabela nova: a consulta não roda, e o relatório diz por quê.
    _, not_run = audit.checks_and_not_run(contracts, ["2026-08-31"], False, None, None, None, None)
    assert not_run == ["chave_id_contrato_publicada (sem versão publicada)"]

    # A tabela sem partição, que a execução substitui inteira, e a auditoria da tabela inteira não
    # comparam com as demais partições.
    accounts = CLIENT_TABLES["cad_contas"]
    assert [check.name for check in audit.checks(accounts, None, published=published_source(accounts))] == [
        "linhas", "chave_id_conta", "chave_numero"]
    assert "chave_id_contrato_publicada" not in [check.name for check in audit.checks(contracts, None, published=published)]


def test_foreign_key_check_only_on_request() -> None:
    """Sem ``foreign_keys=True`` cada chave estrangeira entra em ``not_run``; com ele, o anti-join contra
    a origem de ``referenced``, e a tabela referenciada sem origem fica em ``not_run`` com o motivo."""
    entries = CLIENT_TABLES["cad_lancamentos"]
    found, not_run = audit.checks_and_not_run(entries, ["2026-08-31"], False, None, None, None, None)
    assert not [check for check in found if check.name.startswith("orfao_")]
    assert all(reason.endswith("(sem foreign_keys=True)") for reason in not_run if reason.startswith("orfao_"))

    referenced = {"cad_contas": CLIENT_TABLES["cad_contas"], "dom_veiculos": CLIENT_TABLES["dom_veiculos"]}
    found, not_run = audit.checks_and_not_run(entries, ["2026-08-31"], True, None, None, referenced, None)
    orphans = [check.name for check in found if check.name.startswith("orfao_")]
    assert orphans == ["orfao_id_conta", "orfao_id_veiculo"]
    assert "orfao_id_mensuracao (dom_mensuracoes fora do sandbox e sem versão fixada)" in not_run
    text = audit.audit_sql(entries, "duckdb", ["2026-08-31"], foreign_keys=True, referenced=referenced, prefix="")["orfao_id_conta"]
    assert "NOT (EXISTS (SELECT 1" in text and '"cad_contas"."id_conta" = "cad_lancamentos"."id_conta"' in text


def test_partition_values_follow_the_rule() -> None:
    """Um valor de partição fora da regra é recusado antes de qualquer texto."""
    with pytest.raises(ContractError, match="regra da partição"):
        audit.checks(CLIENT_TABLES["cad_lancamentos"], ["2026-08-31'; DROP TABLE x; --"])
