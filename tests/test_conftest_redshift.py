"""O ``conftest`` da suíte Redshift sem conexão: a ordem entre o autocommit e o ``USE``, e o cache de prepared statements desligado.

Na primeira execução no ambiente alvo (2026-09-21) o ``USE`` correu antes de o autocommit ser
ligado, o ``redshift_connector`` já tinha emitido ``begin transaction``, e a sessão inteira ficou
numa transação que o primeiro erro abortou (``docs/POC.md``). Na terceira e na quarta, o comando
repetido depois de um ``TRUNCATE`` reaproveitou o prepared statement guardado pelo driver e o
datashare o recusou com ``34510``; a conexão passou a ir com ``max_prepared_statements=0``. Os testes
trocam o ``redshift_connector`` por um módulo fabricado que registra os argumentos da conexão e, a
cada comando, o estado do autocommit no momento.
"""

from __future__ import annotations

import sys
import types

import pytest

from conftest import connect_redshift


class FakeCursor:
    """Um cursor que registra cada comando com o autocommit vigente na hora."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def execute(self, sql: str, params: object = None) -> None:
        self.connection.statements.append((sql, self.connection.autocommit))


class FakeConnection:
    """Uma conexão do ``redshift_connector`` fabricada: nasce com o autocommit desligado, como a real."""

    def __init__(self, **kwargs: object) -> None:
        self.kwargs = kwargs
        self.autocommit = False
        self.statements: list[tuple[str, bool]] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)


def test_connect_redshift_turns_autocommit_on_before_the_use(monkeypatch: pytest.MonkeyPatch) -> None:
    """O ``USE`` é o primeiro comando da sessão e já corre com o autocommit ligado: nenhuma transação fica aberta."""
    monkeypatch.setitem(sys.modules, "redshift_connector", types.SimpleNamespace(connect=lambda **kwargs: FakeConnection(**kwargs)))
    for name, value in {"DATABASE": "dev", "HOST": "host", "USER": "usuario", "PASSWORD": "senha", "SHARE_DATABASE": "compartilhado"}.items():
        monkeypatch.setenv(f"SERIALIZE_DB_REDSHIFT_{name}", value)
    monkeypatch.delenv("SERIALIZE_DB_REDSHIFT_WORKGROUP", raising=False)
    monkeypatch.delenv("SERIALIZE_DB_REDSHIFT_PORT", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "sa-east-1")

    method, connection = connect_redshift()

    assert method == "par informado"
    assert connection.kwargs == {"host": "host", "port": 5439, "user": "usuario", "password": "senha", "database": "dev", "max_prepared_statements": 0}
    assert connection.statements == [("USE compartilhado", True)]
    assert connection.autocommit is True


def test_connect_redshift_with_statement_cache_keeps_the_driver_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """``statement_cache=True`` deixa o cache do driver como vem: é a conexão que reproduz o ``34510`` na suíte."""
    monkeypatch.setitem(sys.modules, "redshift_connector", types.SimpleNamespace(connect=lambda **kwargs: FakeConnection(**kwargs)))
    for name, value in {"DATABASE": "dev", "HOST": "host", "USER": "usuario", "PASSWORD": "senha"}.items():
        monkeypatch.setenv(f"SERIALIZE_DB_REDSHIFT_{name}", value)
    for name in ("WORKGROUP", "SHARE_DATABASE", "PORT"):
        monkeypatch.delenv(f"SERIALIZE_DB_REDSHIFT_{name}", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "sa-east-1")

    _, connection = connect_redshift(statement_cache=True)

    assert "max_prepared_statements" not in connection.kwargs
    assert connection.autocommit is True


def test_connect_redshift_without_share_database_runs_no_use(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sem ``SERIALIZE_DB_REDSHIFT_SHARE_DATABASE`` a sessão fica no banco da conexão, ainda com o autocommit ligado."""
    monkeypatch.setitem(sys.modules, "redshift_connector", types.SimpleNamespace(connect=lambda **kwargs: FakeConnection(**kwargs)))
    for name, value in {"DATABASE": "dev", "HOST": "host", "USER": "usuario", "PASSWORD": "senha"}.items():
        monkeypatch.setenv(f"SERIALIZE_DB_REDSHIFT_{name}", value)
    for name in ("WORKGROUP", "SHARE_DATABASE"):
        monkeypatch.delenv(f"SERIALIZE_DB_REDSHIFT_{name}", raising=False)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "sa-east-1")

    _, connection = connect_redshift()

    assert connection.statements == []
    assert connection.autocommit is True
