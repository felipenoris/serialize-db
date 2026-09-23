"""O ``conftest`` da suíte Redshift sem conexão: a ordem entre o autocommit e o ``USE``, e o cache de prepared statements desligado.

O ``USE`` corre com o autocommit já ligado: com ele desligado, o ``redshift_connector`` abre uma
transação antes do primeiro comando, e o primeiro erro do servidor aborta a sessão inteira. A
conexão vai com ``max_prepared_statements=0``, porque o datashare recusa com ``34510`` o prepared
statement que o driver reaproveita depois de um ``TRUNCATE`` (``plan/POC.md``). Os testes trocam o
``redshift_connector`` por um módulo fabricado que registra os argumentos da conexão e, a cada
comando, o estado do autocommit no momento.
"""

from __future__ import annotations

import sys
import types

import pytest

from conftest import connect_redshift

# As variáveis SERIALIZE_DB_REDSHIFT_* que connect_redshift lê.
REDSHIFT_VARIABLES = ("DATABASE", "HOST", "PORT", "USER", "PASSWORD", "WORKGROUP", "SHARE_DATABASE")


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


def use_fake_driver(monkeypatch: pytest.MonkeyPatch, variables: dict[str, str]) -> None:
    """Troca o ``redshift_connector`` pelo fabricado e deixa no ambiente só as ``SERIALIZE_DB_REDSHIFT_*`` de ``variables``.

    As variáveis do ambiente de quem roda a suíte saem, para o teste ler só as que declara; a região
    fica definida, para ``connect_redshift`` não perguntar ao ``boto3``.
    """
    monkeypatch.setitem(sys.modules, "redshift_connector", types.SimpleNamespace(connect=FakeConnection))
    for name in REDSHIFT_VARIABLES:
        monkeypatch.delenv(f"SERIALIZE_DB_REDSHIFT_{name}", raising=False)
    for name, value in variables.items():
        monkeypatch.setenv(f"SERIALIZE_DB_REDSHIFT_{name}", value)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "sa-east-1")


def test_connect_redshift_turns_autocommit_on_before_the_use(monkeypatch: pytest.MonkeyPatch) -> None:
    """O ``USE`` é o primeiro comando da sessão e já corre com o autocommit ligado: nenhuma transação fica aberta."""
    use_fake_driver(monkeypatch, {"DATABASE": "dev", "HOST": "host", "USER": "usuario", "PASSWORD": "senha", "SHARE_DATABASE": "compartilhado"})

    method, connection = connect_redshift()

    assert method == "par informado"
    assert connection.kwargs == {"host": "host", "port": 5439, "user": "usuario", "password": "senha", "database": "dev", "max_prepared_statements": 0}
    assert connection.statements == [("USE compartilhado", True)]
    assert connection.autocommit is True


def test_connect_redshift_with_statement_cache_keeps_the_driver_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """``statement_cache=True`` deixa o cache do driver como vem: é a conexão que reproduz o ``34510`` na suíte."""
    use_fake_driver(monkeypatch, {"DATABASE": "dev", "HOST": "host", "USER": "usuario", "PASSWORD": "senha"})

    _, connection = connect_redshift(statement_cache=True)

    assert "max_prepared_statements" not in connection.kwargs
    assert connection.autocommit is True


def test_connect_redshift_without_share_database_runs_no_use(monkeypatch: pytest.MonkeyPatch) -> None:
    """Sem ``SERIALIZE_DB_REDSHIFT_SHARE_DATABASE`` a sessão fica no banco da conexão, ainda com o autocommit ligado."""
    use_fake_driver(monkeypatch, {"DATABASE": "dev", "HOST": "host", "USER": "usuario", "PASSWORD": "senha"})

    _, connection = connect_redshift()

    assert connection.statements == []
    assert connection.autocommit is True
