"""O ``conftest`` da suíte Redshift sem conexão: a ordem entre o autocommit e o ``USE``, o cache
de prepared statements desligado, a cláusula de credenciais fora da mensagem de um teste reprovado
e as variáveis da dica que autoriza a suíte.

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

from conftest import USAGE, connect_redshift, failure_message, mask_credentials

# As variáveis SERIALIZE_DB_REDSHIFT_* que connect_redshift lê.
REDSHIFT_VARIABLES = ("DATABASE", "HOST", "PORT", "USER", "PASSWORD", "WORKGROUP", "SHARE_DATABASE")

# As variáveis da conexão pelo par informado, sem SHARE_DATABASE e sem WORKGROUP.
INFORMED_PAIR_VARIABLES = {
    "DATABASE": "dev",
    "HOST": "host",
    "USER": "usuario",
    "PASSWORD": "senha",
}


class FakeCursor:
    """Um cursor que registra cada comando com o autocommit vigente na hora."""

    def __init__(self, connection: FakeConnection) -> None:
        self.connection = connection

    def execute(self, sql: str) -> None:
        self.connection.statements.append((sql, self.connection.autocommit))


class FakeConnection:
    """Uma conexão do ``redshift_connector`` fabricada: nasce com o autocommit desligado, como a
    real."""

    def __init__(self, **arguments: object) -> None:
        self.arguments = arguments
        self.autocommit = False
        self.statements: list[tuple[str, bool]] = []

    def cursor(self) -> FakeCursor:
        return FakeCursor(self)


def use_fake_driver(monkeypatch: pytest.MonkeyPatch, variables: dict[str, str]) -> None:
    """Troca o ``redshift_connector`` pelo fabricado e deixa no ambiente só as
    ``SERIALIZE_DB_REDSHIFT_*`` de ``variables``.

    As variáveis do ambiente de quem roda a suíte saem, para o teste ler só as que declara, e o
    substituto local fica desligado, porque o teste confere o caminho do driver; a região fica
    definida, para ``connect_redshift`` não perguntar ao ``boto3``.
    """
    monkeypatch.setitem(sys.modules, "redshift_connector",
                        types.SimpleNamespace(connect=FakeConnection))
    monkeypatch.delenv("SERIALIZE_DB_TEST_EMULATOR", raising=False)
    for name in REDSHIFT_VARIABLES:
        monkeypatch.delenv(f"SERIALIZE_DB_REDSHIFT_{name}", raising=False)
    for name, value in variables.items():
        monkeypatch.setenv(f"SERIALIZE_DB_REDSHIFT_{name}", value)
    monkeypatch.setenv("AWS_DEFAULT_REGION", "sa-east-1")


def test_connect_redshift_turns_autocommit_on_before_the_use(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """O ``USE`` é o primeiro comando da sessão e já corre com o autocommit ligado: nenhuma
    transação fica aberta."""
    use_fake_driver(monkeypatch, {**INFORMED_PAIR_VARIABLES, "SHARE_DATABASE": "compartilhado"})

    method, connection = connect_redshift()

    assert method == "par informado"
    assert connection.arguments == {
        "host": "host",
        "port": 5439,
        "user": "usuario",
        "password": "senha",
        "database": "dev",
        "max_prepared_statements": 0,
    }
    assert connection.statements == [("USE compartilhado", True)]
    assert connection.autocommit is True


def test_connect_redshift_with_statement_cache_keeps_the_driver_default(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """``statement_cache=True`` deixa o cache do driver como vem: é a conexão que reproduz o
    ``34510`` na suíte."""
    use_fake_driver(monkeypatch, INFORMED_PAIR_VARIABLES)

    _, connection = connect_redshift(statement_cache=True)

    assert "max_prepared_statements" not in connection.arguments
    assert connection.autocommit is True


def test_connect_redshift_without_share_database_runs_no_use(
        monkeypatch: pytest.MonkeyPatch) -> None:
    """Sem ``SERIALIZE_DB_REDSHIFT_SHARE_DATABASE`` a sessão fica no banco da conexão, ainda com o
    autocommit ligado."""
    use_fake_driver(monkeypatch, INFORMED_PAIR_VARIABLES)

    _, connection = connect_redshift()

    assert connection.statements == []
    assert connection.autocommit is True


def test_failure_message_masks_the_credentials_before_the_cut() -> None:
    """A mensagem de um teste reprovado passa pela máscara antes do corte em 300 caracteres: um
    corte no meio do valor tiraria a aspa final que ``mask_credentials`` exige, e a impressão e o
    JSON do relatório levariam o começo do segredo."""
    command = ("COPY t FROM 's3://b/m' ACCESS_KEY_ID 'AKIAEXEMPLO' "
               f"SECRET_ACCESS_KEY 'segredo{'0' * 400}' FORMAT AS PARQUET")
    crash = types.SimpleNamespace(message=f"ProgrammingError: {command}")
    report = types.SimpleNamespace(longrepr=types.SimpleNamespace(reprcrash=crash),
                                   outcome="failed")

    recorded = failure_message(report)

    # O relatório passa o valor registrado por mask_credentials de novo na impressão e no JSON.
    assert "segredo" not in mask_credentials(recorded)
    assert "SECRET_ACCESS_KEY '***'" in recorded
    assert len(recorded) <= 300


def test_redshift_usage_names_the_local_root() -> None:
    """A dica da suíte Redshift traz ``SERIALIZE_DB_TEST_LOCAL_ROOT``: os casos da publicação e do
    leitor que também são ``local`` são pulados sem ela."""
    command, note = USAGE["redshift"]
    assert "SERIALIZE_DB_TEST_LOCAL_ROOT=" in command
    assert "SERIALIZE_DB_TEST_LOCAL_ROOT" in note
