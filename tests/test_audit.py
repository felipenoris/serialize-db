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

# A partição da execução em todos os testes.
PARTITIONS = ["2026-08-31"]


class EventoBase(DeclarativeBase):
    pass


class Evento(EventoBase):
    """Uma tabela com uma coluna de cada tipo que muda de função entre os motores."""

    __tablename__ = "cad_eventos"
    __table_args__ = {
        "info": {"serialize_db": {"partition_by": ["data_str"], "partition_source": "data"}},
    }
    id_evento: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    data: Mapped[dt.date] = mapped_column(sa.Date)
    valor: Mapped[float] = mapped_column(sa.Double)
    documento: Mapped[dict | None] = mapped_column(sa.JSON)
    nota: Mapped[str | None] = mapped_column(sa.Text)
    data_str: Mapped[str] = mapped_column(sa.String(10))


# Os trechos do texto da verificação de linhas de Evento em cada motor: as funções que mudam entre
# o DuckDB e o Redshift.
FUNCTION_TEXTS = {
    "duckdb": [
        """strftime("cad_eventos"."data", '%Y-%m-%d')""",
        "json_valid(",
        """strlen("cad_eventos"."nota") > 65535""",
        """strlen(CAST("cad_eventos"."documento" AS VARCHAR)) > 65535""",
        """regexp_full_match("cad_eventos"."data_str", '[0-9A-Za-z][0-9A-Za-z_.-]*')""",
        """isfinite("cad_eventos"."valor")""",
        "count(CASE WHEN",
        "AS NUMERIC(38, 6)",
    ],
    "redshift": [
        """to_char("cad_eventos"."data", 'YYYY-MM-DD')""",
        "is_valid_json(",
        "octet_length(",
        """json_size("cad_eventos"."documento") > 65535""",
        "~ '^[0-9A-Za-z][0-9A-Za-z_.-]*$'",
        "'NaN'::float8",
    ],
}


def published_source(table: sa.Table) -> sa.FromClause:
    """Uma origem com as colunas da tabela, no papel da versão publicada."""
    columns = [sa.column(column.name, column.type) for column in table.columns]
    return sa.table("publicada", *columns).alias("publicado")


def partitions_of(table: sa.Table) -> list[str] | None:
    """Uma partição para a tabela particionada, nenhuma para a sem partição."""
    return PARTITIONS if schema.table_options(table).partition_by else None


def names_of(found: list[audit.Check]) -> list[str]:
    """Os nomes das verificações, na ordem."""
    return [check.name for check in found]


def test_audit_sql_per_dialect() -> None:
    """Cada verificação do modelo cliente renderiza nos dois dialetos, com as funções de cada motor
    e sem ``FILTER`` no Redshift; o texto do DuckDB roda sobre o DDL da etapa 1."""
    # Cada verificação do modelo cliente nos dois dialetos, e o texto do DuckDB rodando sobre o DDL.
    connection = duckdb.connect()
    for table in ClientBase.metadata.sorted_tables:
        connection.execute(schema.ddl(table, "duckdb"))
    for table in ClientBase.metadata.sorted_tables:
        partitions = partitions_of(table)
        duckdb_texts = audit.audit_sql(table, "duckdb", partitions, prefix="")
        redshift_texts = audit.audit_sql(table, "redshift", partitions, prefix="exec_42_")
        assert duckdb_texts.keys() == redshift_texts.keys()
        for name, text in redshift_texts.items():
            assert "FILTER" not in text, (table.name, name)
            assert '"exec_42_' in text, (table.name, name)
        for text in duckdb_texts.values():
            connection.execute(text).fetchall()
    connection.close()

    # As funções de cada motor no texto da verificação de linhas de Evento.
    for dialect, fragments in FUNCTION_TEXTS.items():
        texts = audit.audit_sql(Evento.__table__, dialect, PARTITIONS, prefix="")
        rows_text = texts["linhas"]
        for fragment in fragments:
            assert fragment in rows_text, (dialect, fragment)

    # As funções da auditoria não se registram em sa.func: o cliente continua com as suas.
    assert type(sa.func.json_valid(sa.column("x"))) is sa.sql.functions.Function


def test_key_scope_follows_the_partition_column() -> None:
    """A chave com a coluna de ``partition_source`` fica nas partições da execução; a chave primária
    ganha a consulta contra a versão publicada, com o ``skip_when`` do ``max_key``;
    ``key_scope="partition"`` a suprime e registra, ``"table"`` a estende à chave da origem."""
    contracts = CLIENT_TABLES["cad_contratos"]
    published = published_source(contracts)

    # O escopo padrão: a chave primária contra a versão publicada, com o skip_when do max_key.
    found, not_run = audit.checks_and_not_run(
        contracts, PARTITIONS, foreign_keys=False, key_scope=None, published=published,
        referenced=None, published_max_key=500)
    assert names_of(found) == [
        "linhas",
        "chave_id_contrato",
        "chave_id_contrato_publicada",
        "chave_data_sistema_contrato",
    ]
    checks_by_name = {check.name: check for check in found}
    skip_when = checks_by_name["chave_id_contrato_publicada"].skip_when
    assert skip_when is not None
    assert "min(" in str(skip_when)
    assert not_run == []

    # key_scope="partition": a consulta contra a versão publicada sai e fica registrada.
    found, not_run = audit.checks_and_not_run(
        contracts, PARTITIONS, foreign_keys=False, key_scope="partition", published=published,
        referenced=None, published_max_key=500)
    assert "chave_id_contrato_publicada" not in names_of(found)
    assert not_run == ["chave_id_contrato_publicada (key_scope=partition)"]

    # key_scope="table": a consulta se estende à chave com a coluna de partition_source; sem
    # published_max_key, nenhuma verificação tem skip_when.
    found, _ = audit.checks_and_not_run(
        contracts, PARTITIONS, foreign_keys=False, key_scope="table", published=published,
        referenced=None, published_max_key=None)
    against_published = [name for name in names_of(found) if name.endswith("_publicada")]
    assert against_published == [
        "chave_id_contrato_publicada",
        "chave_data_sistema_contrato_publicada",
    ]
    for check in found:
        assert check.skip_when is None, check.name

    # Sem versão publicada, a tabela nova: a consulta não roda, e o relatório diz por quê.
    _, not_run = audit.checks_and_not_run(
        contracts, PARTITIONS, foreign_keys=False, key_scope=None, published=None,
        referenced=None, published_max_key=None)
    assert not_run == ["chave_id_contrato_publicada (sem versão publicada)"]

    # A tabela sem partição, que a execução substitui inteira, não compara com as demais partições.
    accounts = CLIENT_TABLES["cad_contas"]
    account_checks = audit.checks(accounts, None, published=published_source(accounts))
    assert names_of(account_checks) == ["linhas", "chave_id_conta", "chave_numero"]

    # A auditoria da tabela inteira também não compara com as demais partições.
    whole_table_checks = audit.checks(contracts, None, published=published)
    assert "chave_id_contrato_publicada" not in names_of(whole_table_checks)


def test_foreign_key_check_only_on_request() -> None:
    """Sem ``foreign_keys=True`` cada chave estrangeira entra em ``not_run``; com ele, o anti-join
    contra a origem de ``referenced``, e a tabela referenciada sem origem fica em ``not_run`` com o
    motivo."""
    entries = CLIENT_TABLES["cad_lancamentos"]

    # Sem foreign_keys=True: nenhuma verificação de órfão, e cada chave estrangeira em not_run.
    found, not_run = audit.checks_and_not_run(
        entries, PARTITIONS, foreign_keys=False, key_scope=None, published=None,
        referenced=None, published_max_key=None)
    assert not [check for check in found if check.name.startswith("orfao_")]
    orphan_reasons = [reason for reason in not_run if reason.startswith("orfao_")]
    assert orphan_reasons == [
        "orfao_data_base_sistema_contrato (sem foreign_keys=True)",
        "orfao_id_conta (sem foreign_keys=True)",
        "orfao_id_mensuracao (sem foreign_keys=True)",
        "orfao_id_negocio (sem foreign_keys=True)",
        "orfao_id_segmento (sem foreign_keys=True)",
        "orfao_id_veiculo (sem foreign_keys=True)",
    ]

    # Com foreign_keys=True: o anti-join contra as tabelas de referenced, e o motivo da que falta.
    referenced = {
        "cad_contas": CLIENT_TABLES["cad_contas"],
        "dom_veiculos": CLIENT_TABLES["dom_veiculos"],
    }
    found, not_run = audit.checks_and_not_run(
        entries, PARTITIONS, foreign_keys=True, key_scope=None, published=None,
        referenced=referenced, published_max_key=None)
    orphans = [check.name for check in found if check.name.startswith("orfao_")]
    assert orphans == ["orfao_id_conta", "orfao_id_veiculo"]
    assert "orfao_id_mensuracao (dom_mensuracoes fora do sandbox e sem versão fixada)" in not_run

    # O texto do anti-join no DuckDB.
    texts = audit.audit_sql(entries, "duckdb", PARTITIONS, foreign_keys=True,
                            referenced=referenced, prefix="")
    orphan_text = texts["orfao_id_conta"]
    assert "NOT (EXISTS (SELECT 1" in orphan_text
    assert '"cad_contas"."id_conta" = "cad_lancamentos"."id_conta"' in orphan_text


def test_partition_values_follow_the_rule() -> None:
    """Um valor de partição fora da regra é recusado antes de qualquer texto."""
    with pytest.raises(ContractError, match="regra da partição"):
        audit.checks(CLIENT_TABLES["cad_lancamentos"], ["2026-08-31'; DROP TABLE x; --"])
