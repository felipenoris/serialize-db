"""Testes das funções puras dos probes, sem rede e sem AWS.

Os probes (``probes/``) leem o ambiente; as decisões que eles tomam sobre o que leram são funções
puras, testadas aqui com respostas fabricadas: a classificação dos erros do ``boto3``, os rótulos
de DNS, as tabelas e os segredos mascarados, o código de saída do relatório, o inventário do bucket
(tabelas Delta, sessões da suíte, versões não correntes), o versionamento pela amostra, o Object
Lock, o ciclo de vida, a montagem de ``~/shared``, o formato das tabelas do Glue e os parâmetros da
conexão Redshift. Um
``Report`` grava em ``probes/output/``; ``make_report`` o aponta para a pasta temporária do teste e
devolve ``sys.stdout`` ao pytest no fim. Nenhum teste grava fora de ``tmp_path``, e o do
``parquet_source.py`` não abre arquivo algum: as suas funções recebem colunas e rodapés fabricados.
"""

from __future__ import annotations

import contextlib
import io
import datetime as dt
import os
import socket
import sys
import types
from pathlib import Path

import botocore.exceptions
import pytest

PROBES = Path(__file__).resolve().parent.parent / "probes"
sys.path.insert(0, str(PROBES))

import bucket  # noqa: E402
import catalog  # noqa: E402
import diagnose_aws  # noqa: E402
import parquet_source  # noqa: E402
import probelib  # noqa: E402
import redshift  # noqa: E402
import space  # noqa: E402

NOW = dt.datetime(2026, 9, 20, 3, 44, tzinfo=dt.timezone.utc)
MINUTE = dt.timedelta(minutes=1)


def client_error(code: str, operation: str = "Operation") -> botocore.exceptions.ClientError:
    """Uma ``ClientError`` do botocore com o código dado, como o serviço a devolveria."""
    return botocore.exceptions.ClientError({"Error": {"Code": code, "Message": "mensagem"}}, operation)


@contextlib.contextmanager
def make_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Um ``Report`` que grava em ``tmp_path``; no fim, fecha o arquivo e devolve ``sys.stdout`` ao pytest."""
    monkeypatch.setattr(probelib, "OUTPUT_DIR", tmp_path)
    stdout = sys.stdout
    report = probelib.Report("teste", "funções puras")
    try:
        yield report
    finally:
        if not report.tee.file.closed:
            report.tee.close()
        sys.stdout = stdout


def checks(report: probelib.Report, check_id: str) -> list[str]:
    """Os detalhes das checagens com o id dado, na ordem em que foram registradas."""
    return [check[3] for check in report.checks if check[1] == check_id]


class FakePaginator:
    """Um paginador do boto3 que devolve páginas prontas, ou levanta a exceção colocada no lugar de uma página."""

    def __init__(self, pages: list) -> None:
        self.pages = pages

    def paginate(self, **kwargs):
        for page in self.pages:
            if isinstance(page, Exception):
                raise page
            yield page


class FakeS3:
    """Um cliente S3 cujas respostas são dicionários prontos; uma exceção no lugar da resposta é levantada."""

    def __init__(self, **responses) -> None:
        self.responses = responses

    def __getattr__(self, name: str):
        if name not in self.responses:
            raise AttributeError(name)

        def call(**kwargs):
            response = self.responses[name]
            if isinstance(response, Exception):
                raise response
            return response

        return call

    def get_paginator(self, name: str) -> FakePaginator:
        return FakePaginator(self.responses[name])


# ---------------------------------------------------------------------------------------------------------------
# probelib: a classificação dos erros, os rótulos de rede, a formatação e o relatório


def test_reason_distinguishes_denied_answered_unanswered_and_local() -> None:
    """``reason`` é o motivo curto que a checagem imprime: negado, outro erro do serviço, sem resposta ou erro local."""
    assert probelib.reason(client_error("AccessDenied")) == "negado (AccessDenied)"
    assert probelib.reason(client_error("AccessDeniedException")) == "negado (AccessDeniedException)"
    assert probelib.reason(client_error("InvalidAccessKeyId")) == "o serviço respondeu InvalidAccessKeyId"

    # Sem resposta: uma exceção do botocore cujo nome diz rede, endpoint ou tempo esgotado.
    unanswered = botocore.exceptions.EndpointConnectionError(endpoint_url="https://s3.exemplo")
    assert probelib.reason(unanswered) == "sem resposta (EndpointConnectionError)"
    assert probelib.describe_error(unanswered).startswith("sem resposta: EndpointConnectionError:")

    assert probelib.reason(ValueError("x")) == "erro local (ValueError)"
    assert probelib.describe_error(ValueError("x")) == "ValueError: x"
    assert probelib.describe_error(client_error("AccessDenied")).startswith("o serviço respondeu com erro: ClientError:")

    assert probelib.error_code(client_error("NoSuchBucket")) == "NoSuchBucket"
    assert probelib.error_code(ValueError("x")) is None


def test_public_label_marks_gateway_endpoint_only_for_s3_and_dynamodb() -> None:
    """Só o S3 e o DynamoDB têm gateway endpoint; um IP público de outro serviço depende da internet ou do proxy."""
    gateway = "público: gateway endpoint ou internet"
    proxy = "público: só pela internet ou pelo proxy"
    assert probelib.public_label("s3.us-west-2.amazonaws.com") == gateway
    assert probelib.public_label("s3.amazonaws.com") == gateway
    assert probelib.public_label("meu-bucket.s3.us-west-2.amazonaws.com") == gateway
    assert probelib.public_label("dynamodb.us-west-2.amazonaws.com") == gateway
    assert probelib.public_label("redshift.us-west-2.amazonaws.com") == proxy
    assert probelib.public_label("redshift-serverless.us-west-2.amazonaws.com") == proxy
    assert probelib.public_label("sagemaker.us-west-2.amazonaws.com") == proxy
    assert probelib.public_label("pypi.org") == proxy


def fake_client(url: str) -> types.SimpleNamespace:
    """Um cliente boto3 fabricado: ``endpoint_reachable`` lê só ``meta.endpoint_url``."""
    return types.SimpleNamespace(meta=types.SimpleNamespace(endpoint_url=url))


def test_endpoint_reachable_skips_the_call_when_the_port_does_not_answer(monkeypatch: pytest.MonkeyPatch) -> None:
    """A porta que aceita conexão é alcance; a recusada dispensa a chamada, e com proxy o teste direto não decide."""
    for name in probelib.PROXY_VARIABLES:
        monkeypatch.delenv(name, raising=False)

    with socket.create_server(("127.0.0.1", 0)) as server:
        port = server.getsockname()[1]
        reachable, reading = probelib.endpoint_reachable(fake_client(f"https://127.0.0.1:{port}"))
        assert reachable
        assert reading.startswith(f"127.0.0.1:{port} conectou em ")

    # Fora do bloco a porta está fechada: o sistema recusa a conexão, sem esperar o tempo limite.
    reachable, reading = probelib.endpoint_reachable(fake_client(f"https://127.0.0.1:{port}"), timeout=0.5)
    assert not reachable
    assert "não conectou" in reading

    # Um endereço sem esquema não tem host para testar, e a chamada é feita.
    reachable, reading = probelib.endpoint_reachable(fake_client("iam.amazonaws.com"))
    assert reachable
    assert "endpoint sem host" in reading

    # Um nome que não resolve é leitura, não chamada falhada.
    reachable, reading = probelib.endpoint_reachable(fake_client("https://nao.existe.invalid"), timeout=0.5)
    assert not reachable
    assert "não resolve" in reading

    # Com proxy, a conexão direta não responde pelo alcance: a chamada é feita.
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy:3128")
    reachable, reading = probelib.endpoint_reachable(fake_client(f"https://127.0.0.1:{port}"), timeout=0.5)
    assert reachable
    assert "proxy configurado" in reading


def test_tabulate_aligns_columns_and_fills_empty_cells() -> None:
    """``tabulate`` alinha como ``column -t``: duas colunas de espaço entre células, ``-`` na célula vazia."""
    text = probelib.tabulate([["nome", "valor"], ["a", ""], ["abc", "1"], "x\ty"])
    assert text.splitlines() == ["nome  valor", "a     -", "abc   1", "x     y"]


def test_mask_and_pretty_hide_secrets_and_response_metadata() -> None:
    """``pretty`` mostra JSON legível sem ``ResponseMetadata`` e com os valores de chaves que parecem segredo mascarados."""
    data = {"password": "s", "secretArn": "arn", "nested": {"token": "t", "name": "n"}, "ResponseMetadata": {"HTTPStatusCode": 200}}
    masked = probelib.mask(data)
    assert masked["password"] == "***" and masked["secretArn"] == "***" and masked["nested"] == {"token": "***", "name": "n"}

    text = probelib.pretty(data)
    assert "ResponseMetadata" not in text and '"name": "n"' in text and "***" in text and '"s"' not in text

    # Acima do limite de linhas, o resto vira uma contagem.
    long_text = probelib.pretty({f"k{index}": index for index in range(50)}, limit=10)
    assert long_text.splitlines()[-1] == "... (42 linhas omitidas)"


def test_environment_rows_show_presence_for_secrets_and_collapse_equal_twins(monkeypatch: pytest.MonkeyPatch) -> None:
    """A minúscula igual à maiúscula sai uma vez; uma variável que parece segredo mostra só presença; a ausente, ``(ausente)``."""
    monkeypatch.setenv("NO_PROXY", "a,b")
    monkeypatch.setenv("no_proxy", "a,b")
    monkeypatch.setenv("HTTPS_PROXY", "http://proxy:3128")
    monkeypatch.setenv("https_proxy", "http://outro:3128")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "x")
    monkeypatch.delenv("AWS_PROFILE", raising=False)
    monkeypatch.setenv("HTTP_PROXY", "")

    rows = dict(probelib.environment_rows(["NO_PROXY", "no_proxy", "HTTPS_PROXY", "https_proxy", "AWS_SECRET_ACCESS_KEY", "AWS_PROFILE", "HTTP_PROXY"]))
    assert rows["no_proxy"] == "(igual a NO_PROXY)"
    assert rows["https_proxy"] == "http://outro:3128"
    assert rows["AWS_SECRET_ACCESS_KEY"] == "definida"
    assert rows["AWS_PROFILE"] == "(ausente)"
    assert rows["HTTP_PROXY"] == "(vazia)"


def test_duckdb_proxy_splits_the_address_from_the_credentials() -> None:
    """O endereço vai sem as credenciais, e o usuário e a senha em configurações à parte, sem URL-encode."""
    proxy = probelib.duckdb_proxy({"HTTP_PROXY": "http://usuario:se%40nha@proxy01.exemplo.net:8080"})
    assert proxy.settings == {"http_proxy": "proxy01.exemplo.net:8080", "http_proxy_username": "usuario", "http_proxy_password": "se@nha"}
    assert proxy.reading == "proxy01.exemplo.net:8080, com usuário e senha"

    # username e password ganham das credenciais embutidas no endereço.
    proxy = probelib.duckdb_proxy({"HTTP_PROXY": "http://outro:errada@proxy01.exemplo.net:8080", "username": "usuario", "password": "se@nha"})
    assert proxy.settings["http_proxy_username"] == "usuario"
    assert proxy.settings["http_proxy_password"] == "se@nha"

    # Sem credenciais em lugar algum, só o endereço; o esquema é opcional e a porta, também.
    assert probelib.duckdb_proxy({"HTTP_PROXY": "proxy01.exemplo.net:8080"}).settings == {"http_proxy": "proxy01.exemplo.net:8080"}
    assert probelib.duckdb_proxy({"HTTP_PROXY": "http://proxy01.exemplo.net"}).settings == {"http_proxy": "proxy01.exemplo.net"}


def test_duckdb_proxy_reads_only_the_variable_duckdb_reads() -> None:
    """Sem ``HTTP_PROXY`` nada é configurado: as demais grafias, que o DuckDB ignora, entram na leitura."""
    assert probelib.duckdb_proxy({}) == probelib.DuckDBProxy({}, "sem proxy no ambiente")

    proxy = probelib.duckdb_proxy({"http_proxy": "http://p:3128", "https_proxy": "http://p:3128", "HTTP_PROXY": ""})
    assert proxy.settings == {}
    assert proxy.reading == "sem HTTP_PROXY; o DuckDB ignora http_proxy, https_proxy"

    # Um endereço sem host, ou com porta que não é número, vira leitura; a senha não aparece nela.
    for url in ("http://", "http://usuario:se%40nha@:8080", "http://usuario:se%40nha@proxy01.exemplo.net:porta"):
        proxy = probelib.duckdb_proxy({"HTTP_PROXY": url})
        assert proxy.settings == {}
        assert proxy.reading.startswith("HTTP_PROXY sem host: ")
        assert "se%40nha" not in proxy.reading


def test_hide_credentials_keeps_the_address_and_drops_the_userinfo(monkeypatch: pytest.MonkeyPatch) -> None:
    """O relatório é colado na conversa: o endereço fica legível e o usuário e a senha somem."""
    assert probelib.hide_credentials("http://usuario:se%40nha@proxy01.exemplo.net:8080") == "http://***@proxy01.exemplo.net:8080"
    assert probelib.hide_credentials("proxy01.exemplo.net:8080") == "proxy01.exemplo.net:8080"
    assert probelib.hide_credentials("") == ""

    # environment_rows e o relatório do diagnose_aws usam a mesma regra em toda variável de proxy.
    monkeypatch.setenv("HTTP_PROXY", "http://usuario:se%40nha@proxy01.exemplo.net:8080")
    monkeypatch.setenv("AWS_REGION", "us-west-2")
    assert dict(probelib.environment_rows(["HTTP_PROXY", "AWS_REGION"])) == {
        "HTTP_PROXY": "http://***@proxy01.exemplo.net:8080",
        "AWS_REGION": "us-west-2",
    }
    printed = io.StringIO()
    with contextlib.redirect_stdout(printed):
        diagnose_aws.show_environment()
    assert "http://***@proxy01.exemplo.net:8080" in printed.getvalue()
    assert "se%40nha" not in printed.getvalue()


def test_find_values_and_connection_rows() -> None:
    """``find_values`` acha a primeira ocorrência de cada chave em qualquer profundidade; ``connection_rows`` resume cada conexão."""
    data = {"data": {"inner": {"host": "h", "port": 5439, "password": ""}}, "host": "ignorado"}
    assert probelib.find_values(data, ("host", "port", "password")) == {"host": "h", "port": 5439}

    connections = [
        {"name": "default.s3_shared", "type": "S3", "data": {"s3_uri": "s3://b/p/"}},
        {"name": "project.redshift", "type": "REDSHIFT", "physical_endpoints": [{"host": "wg.redshift", "port": 5439}], "data": {"database_name": "dev"}},
        {"name": "quebrada", "type": "SPARK", "data_error": "TypeError: x"},
    ]
    rows = probelib.connection_rows(connections)
    assert rows[0] == ["default.s3_shared", "S3", "-", "s3_uri=s3://b/p/"]
    assert rows[1] == ["project.redshift", "REDSHIFT", "wg.redshift:5439", "database_name=dev"]
    assert rows[2] == ["quebrada", "SPARK", "-", "TypeError: x"]


@pytest.mark.parametrize(
    ("kinds", "failing", "expected"),
    [(["pass"], False, 0), (["pass"], True, 1), (["fail", "pass"], False, 2), (["fail"], True, 2)],
)
def test_report_exit_code(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, kinds: list[str], failing: bool, expected: int) -> None:
    """O código de saída: 2 com checagem reprovada, senão 1 com chamada falhada, senão 0."""
    with make_report(tmp_path, monkeypatch) as report:
        for kind in kinds:
            getattr(report, "ok" if kind == "pass" else kind)("T-1", "o que", "detalhe")
        if failing:
            assert report.call("falha()", lambda: (_ for _ in ()).throw(client_error("AccessDenied"))) is None
            assert report.last_reason == "negado (AccessDenied)"
        assert report.finish() == expected
        assert report.path.read_text(encoding="utf-8").count("# ") >= 2


def test_report_call_returns_the_result_and_records_the_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``call`` devolve o resultado da ação; na exceção, devolve ``None``, guarda o motivo e registra a falha na seção final."""
    with make_report(tmp_path, monkeypatch) as report:
        assert report.call("soma", lambda: 1 + 1, render=str) == 2
        assert report.failures == []

        def denied():
            raise client_error("AccessDenied", "GetBucketPolicy")

        assert report.call("s3.get_bucket_policy()", denied) is None
        assert report.last_reason == "negado (AccessDenied)"
        assert report.failures[0][0] == "s3.get_bucket_policy()"
        assert report.failures[0][1].startswith("o serviço respondeu com erro: ClientError:")


# ---------------------------------------------------------------------------------------------------------------
# bucket.py: o inventário, o versionamento, o Object Lock, o ciclo de vida e as permissões


PREFIX = "dzd/proj/shared/serialize-db-tests/"
ENTRIES = [
    (PREFIX + "serialize-db-poc/8b9e9976/operacoes/_delta_log/00000000000000000000.json", 1000, NOW),
    (PREFIX + "serialize-db-poc/8b9e9976/operacoes/_delta_log/00000000000000000001.json", 1000, NOW + MINUTE),
    (PREFIX + "serialize-db-poc/8b9e9976/operacoes/_delta_log/00000000000000000010.checkpoint.parquet", 5000, NOW),
    (PREFIX + "serialize-db-poc/8b9e9976/operacoes/_delta_log/_last_checkpoint", 50, NOW),
    (PREFIX + "serialize-db-poc/8b9e9976/operacoes/mes=2026-01/part-0.parquet", 100000, NOW),
    (PREFIX + "serialize-db-poc/8b9e9976/operacoes/mes=2026-02/part-0.parquet", 200000, NOW + 2 * MINUTE),
    (PREFIX + "serialize-db-poc/8b9e9976/conditional.txt", 5, NOW),
    (PREFIX + "serialize-db-poc/deadbeef/x/_delta_log/00000000000000000000.json", 10, NOW - 60 * MINUTE),
    (PREFIX + "serialize-db-poc/deadbeef/x/part.parquet", 10, NOW - 60 * MINUTE),
    (PREFIX + "serialize-db-poc/notasession/y.parquet", 1, NOW),
    (PREFIX + "outra/tabela/_delta_log/00000000000000000000.json", 1, NOW),
    (PREFIX + "outra/tabela/data.parquet", 7, NOW),
    (PREFIX, 0, NOW),
]


def test_delta_table_rows_count_files_commits_and_checkpoints() -> None:
    """Cada tabela Delta (pasta com ``_delta_log``) tem os arquivos de dados, bytes, commits, checkpoints e o último objeto."""
    roots = sorted({key.split("/_delta_log/", 1)[0] for key, _, _ in ENTRIES if "/_delta_log/" in key})
    rows = bucket.delta_table_rows(ENTRIES, roots, PREFIX)
    by_name = {row[0]: row[1:] for row in rows[1:]}
    assert by_name["serialize-db-poc/8b9e9976/operacoes"] == [2, 300000, 2, 1, str(NOW + 2 * MINUTE)]
    assert by_name["outra/tabela"] == [1, 7, 1, 0, str(NOW)]
    assert by_name["serialize-db-poc/deadbeef/x"] == [1, 10, 1, 0, str(NOW - 60 * MINUTE)]


def test_session_rows_list_the_suite_sessions_newest_first() -> None:
    """Só ``serialize-db-poc/<oito dígitos hexadecimais>/`` é sessão da suíte; a mais recente vem primeiro."""
    rows = bucket.session_rows(ENTRIES, PREFIX)
    assert [row[0] for row in rows[1:]] == ["8b9e9976", "deadbeef"]
    assert rows[1][1:] == [7, 307055, str(NOW + 2 * MINUTE)]


def test_object_versions_reads_denied_accumulated_and_clean(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``BK-14``: negado com o motivo; versões não correntes e marcadores contados; ou nada acumulado."""
    with make_report(tmp_path, monkeypatch) as report:
        bucket.object_versions(report, FakeS3(list_object_versions=[client_error("AccessDenied")]), "b", PREFIX)
        page = {"Versions": [{"IsLatest": True, "Size": 10}, {"IsLatest": False, "Size": 20}, {"IsLatest": False, "Size": 30}], "DeleteMarkers": [{}, {}]}
        bucket.object_versions(report, FakeS3(list_object_versions=[page]), "b", PREFIX)
        bucket.object_versions(report, FakeS3(list_object_versions=[{"Versions": [{"IsLatest": True, "Size": 10}]}]), "b", PREFIX)

        notes = checks(report, "BK-14")
        assert notes[0].startswith("não lidas: negado (AccessDenied)")
        assert notes[1].startswith("2 (50 bytes) e 2 marcadores de exclusão")
        assert notes[2] == "nenhuma, nem marcador de exclusão"


def test_versioning_check_uses_the_api_or_the_sample(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``BK-4``: pela API quando lida; com ela negada, um VersionId na amostra prova o versionamento."""
    with make_report(tmp_path, monkeypatch) as report:
        bucket.versioning_check(report, "Enabled", "lido", None)
        bucket.versioning_check(report, "Suspended", "lido", None)
        bucket.versioning_check(report, None, "negado (AccessDenied)", {"VersionId": "ORjAs2CA"})
        bucket.versioning_check(report, None, "negado (AccessDenied)", {"VersionId": "null"})

        notes = checks(report, "BK-4")
        assert notes[0].startswith("ativo pela API:")
        assert notes[1] == "Suspended; o Delta não precisa dele"
        assert notes[2].startswith("pela API, negado (AccessDenied); a amostra tem VersionId: ativo;")
        assert notes[3] == "não lido: pela API, negado (AccessDenied), e nenhuma amostra com VersionId"


@pytest.mark.parametrize(
    ("lock", "expected"),
    [
        (client_error("AccessDenied"), "não lido: negado (AccessDenied)"),
        (client_error("ObjectLockConfigurationNotFoundError"), "desativado"),
        ({"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled"}}, "ativo, sem retenção padrão;"),
        ({"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled", "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": 30}}}}, "ativo, retenção padrão GOVERNANCE por 30 dias;"),
        ({"ObjectLockConfiguration": {"ObjectLockEnabled": "Enabled", "Rule": {"DefaultRetention": {"Mode": "COMPLIANCE", "Years": 1}}}}, "ativo, retenção padrão COMPLIANCE por 1 ano;"),
    ],
)
def test_bucket_settings_interpret_object_lock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, lock, expected: str) -> None:
    """``BK-12``: a ausência de Object Lock é uma leitura (``desativado``), a negação traz o motivo, e a retenção padrão sai por extenso."""
    client = FakeS3(
        head_bucket={"ResponseMetadata": {"HTTPHeaders": {"x-amz-bucket-region": "us-west-2"}}},
        get_bucket_versioning=client_error("AccessDenied"),
        get_bucket_encryption={"ServerSideEncryptionConfiguration": {"Rules": [{"ApplyServerSideEncryptionByDefault": {"SSEAlgorithm": "aws:kms", "KMSMasterKeyID": "arn:aws:kms:us-west-2:1:key/k"}, "BucketKeyEnabled": True}]}},
        get_object_lock_configuration=lock,
        get_public_access_block={},
        get_bucket_ownership_controls={},
    )
    with make_report(tmp_path, monkeypatch) as report:
        kms_key, status, why = bucket.bucket_settings(report, client, "b", None)
        assert (kms_key, status, why) == ("arn:aws:kms:us-west-2:1:key/k", None, "negado (AccessDenied)")
        assert checks(report, "BK-1") == ["b"] and checks(report, "BK-2") == ["us-west-2"]
        assert checks(report, "BK-5") == ["aws:kms arn:aws:kms:us-west-2:1:key/k; bucket key True"]
        assert checks(report, "BK-12")[0].startswith(expected)


def test_lifecycle_flags_an_enabled_expiration_that_reaches_the_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """``BK-3``: só uma regra habilitada, com expiração, cujo prefixo contém a raiz ou está contido nela, reprova."""
    reaching = {"ID": "expira", "Status": "Enabled", "Filter": {"Prefix": "dzd/"}, "Expiration": {"Days": 30}}
    disabled = {"ID": "parada", "Status": "Disabled", "Prefix": "dzd/", "Expiration": {"Days": 30}}
    elsewhere = {"ID": "outra", "Status": "Enabled", "Filter": {"Prefix": "logs/"}, "NoncurrentVersionExpiration": {"NoncurrentDays": 7}}
    with make_report(tmp_path, monkeypatch) as report:
        bucket.lifecycle(report, FakeS3(get_bucket_lifecycle_configuration={"Rules": [reaching, disabled, elsewhere]}), "b", "dzd/proj")
        bucket.lifecycle(report, FakeS3(get_bucket_lifecycle_configuration={"Rules": [disabled, elsewhere]}), "b", "dzd/proj")
        bucket.lifecycle(report, FakeS3(get_bucket_lifecycle_configuration=client_error("NoSuchLifecycleConfiguration")), "b", "dzd/proj")
        bucket.lifecycle(report, FakeS3(get_bucket_lifecycle_configuration=client_error("AccessDenied")), "b", "dzd/proj")

        kinds = [check[0] for check in report.checks if check[1] == "BK-3"]
        notes = checks(report, "BK-3")
        assert kinds == ["fail", "pass", "pass", "note"]
        assert notes[0].startswith("expira (prefixo 'dzd/'):")
        assert notes[1] == "nenhuma das 2 regras expira objetos sob dzd/proj"
        assert notes[2] == "nenhuma das 0 regras expira objetos sob dzd/proj"
        assert notes[3].startswith("regras não lidas: negado (AccessDenied)")


def test_principal_arn_turns_an_assumed_role_into_the_role() -> None:
    """A simulação de política aceita o papel, não a sessão assumida."""
    assumed = "arn:aws:sts::892278726726:assumed-role/datazone_usr_role_x/SageMaker"
    assert probelib.principal_arn(assumed) == "arn:aws:iam::892278726726:role/datazone_usr_role_x"
    assert probelib.principal_arn("arn:aws:iam::1:user/eu") == "arn:aws:iam::1:user/eu"


# ---------------------------------------------------------------------------------------------------------------
# space.py, catalog.py, redshift.py e diagnose_aws.py


def test_mount_state_follows_the_link_and_reads_proc_mounts(tmp_path: Path) -> None:
    """``~/shared`` é um link para a montagem; o estado vem do caminho real em ``/proc/mounts``, com o tipo e ``rw`` ou ``ro``."""
    real = Path(os.path.realpath(tmp_path / "shared"))
    real.mkdir()
    link = tmp_path / "link"
    link.symlink_to(real)
    mounts = tmp_path / "mounts"

    mounts.write_text(f"s3fs {real} fuse.s3fs rw,nosuid,nodev,relatime,user_id=1000,allow_other 0 0\nproc /proc proc rw 0 0\n")
    assert space.mount_state(link, str(mounts)) == f"montada, tipo fuse.s3fs, rw (link para {real})"

    mounts.write_text(f"s3fs {real} fuse.s3fs ro,nosuid 0 0\n")
    assert space.mount_state(real, str(mounts)) == "montada, tipo fuse.s3fs, ro"

    assert space.mount_state(tmp_path / "nothere", str(mounts)) == "ausente"
    assert space.mount_state(real, str(tmp_path / "nomounts")) == "existe, sem montagem"


def test_pinned_requirements_read_the_pinned_versions_of_pyproject() -> None:
    """``SP-9`` compara o venv com as dependências de execução e o grupo ``dev`` de ``pyproject.toml``: nome de
    importação e versão quando fixada por ``==``; os dialetos são de execução desde a etapa 2."""
    requirements = space.pinned_requirements()
    assert requirements["deltalake"] == "1.6.4" and requirements["duckdb"] == "1.5.5"
    assert requirements["sqlalchemy_redshift"] == "1.0.0" and requirements["duckdb_engine"] == "0.17.0"
    assert "redshift_connector" in requirements and requirements["sqlglot"] == "30.18.0"
    assert requirements["boto3"] is None


def test_table_format_recognizes_iceberg_delta_and_parquet() -> None:
    """O formato de uma tabela do Glue vem dos parâmetros (``table_type``, provedor Spark, classificação) ou do descritor."""
    assert catalog.table_format({"Parameters": {"table_type": "ICEBERG"}}) == "Iceberg"
    assert catalog.table_format({"Parameters": {"spark.sql.sources.provider": "delta"}}) == "Delta"
    assert catalog.table_format({"StorageDescriptor": {"Location": "s3://b/delta/t/"}}) == "Delta"
    assert catalog.table_format({"StorageDescriptor": {"InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"}}) == "Parquet"
    assert catalog.table_format({"Parameters": {"classification": "csv"}}) == "csv"
    assert catalog.table_format({"TableType": "VIRTUAL_VIEW"}) == "VIRTUAL_VIEW"
    assert catalog.table_format({}) == "-"


def test_target_from_connection_fills_host_port_and_database(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Os parâmetros da conexão do projeto: o endpoint físico, os dados em camelCase e a URL JDBC preenchem o alvo."""
    chosen = {
        "name": "project.redshift",
        "physical_endpoints": [{"host": "wg.redshift.amazonaws.com", "port": 5439}],
        "data": {"databaseName": "dev", "workgroupName": "wg", "jdbcUrl": "jdbc:redshift://outro:5439/ignorado"},
    }
    with make_report(tmp_path, monkeypatch) as report:
        target = redshift.Target()
        redshift.target_from_connection(report, target, chosen)
        assert (target.host, target.port, target.database, target.workgroup) == ("wg.redshift.amazonaws.com", 5439, "dev", "wg")
        assert target.source == "conexão project.redshift do projeto"

        # Sem endpoint físico, host, porta e banco vêm da URL JDBC.
        target = redshift.Target()
        redshift.target_from_connection(report, target, {"name": "c", "data": {"jdbc_url": "jdbc:redshift://h:5440/db?ssl=true"}})
        assert (target.host, target.port, target.database) == ("h", 5440, "db")

        # As variáveis têm precedência: um alvo vindo delas não é alterado.
        target = redshift.Target(host="das-variaveis", source="variáveis")
        redshift.target_from_connection(report, target, chosen)
        assert target.host == "das-variaveis"


def test_cluster_and_workgroup_rows_have_one_row_per_resource() -> None:
    """As tabelas das APIs: uma linha por cluster e por workgroup, com o papel IAM padrão e o roteamento VPC."""
    clusters = {"Clusters": [{"ClusterIdentifier": "c1", "ClusterStatus": "available", "Endpoint": {"Address": "a", "Port": 5439}, "DBName": "dev", "NumberOfNodes": 2, "NodeType": "ra3", "DefaultIamRoleArn": "arn:role", "IamRoles": [{"IamRoleArn": "arn:role"}]}]}
    rows = redshift.cluster_rows(clusters)
    assert len(rows) == 2 and rows[1][0] == "c1" and rows[1][2] == "a:5439" and rows[1][6] == "arn:role"

    workgroups = {"workgroups": [{"workgroupName": "wg", "status": "AVAILABLE", "endpoint": {"address": "b", "port": 5439}, "namespaceName": "ns", "baseCapacity": 8}]}
    rows = redshift.workgroup_rows(workgroups)
    assert len(rows) == 2 and rows[1][:4] == ["wg", "AVAILABLE", "b:5439", "ns"]


def test_qualified_is_the_session_name_and_fully_qualified_the_three_part_one() -> None:
    """O nome da tabela no SQL: ``esquema.tabela`` na sessão, que rodou ``USE``; três partes só de outro banco, como a Data API."""
    target = redshift.Target(database="dev", share_database="datalake_rw_shared", schema="sbx_aco_decon")
    assert target.qualified("t") == "sbx_aco_decon.t"
    assert target.fully_qualified("t") == "datalake_rw_shared.sbx_aco_decon.t"

    # Sem datashare, o banco do nome em três partes é o da conexão; sem esquema, só a tabela.
    assert redshift.Target(database="dev", schema="publico").fully_qualified("t") == "dev.publico.t"
    assert redshift.Target(database="dev").qualified("t") == "t"

    # O banco do esquema é o do datashare quando há um, e o da conexão quando não há.
    assert redshift.Target(database="dev", share_database="share").schema_database() == "share"
    assert redshift.Target(database="dev").schema_database() == "dev"


def test_copy_principals_follow_the_iam_role_variable() -> None:
    """``RS-11`` simula quem vai alcançar o S3: quem chama sem a variável, o papel que ela nomeia com ela."""
    roles = (["arn:padrao"], ["arn:associado"])
    caller = "arn:aws:sts::1:assumed-role/papel/sessao"

    # Sem SERIALIZE_DB_REDSHIFT_IAM_ROLE, o COPY leva as credenciais de quem chama: a identidade do STS, como papel.
    principals, reason = redshift.copy_principals(None, roles, caller)
    assert [(p.label, p.arn, p.blocker, p.doubt) for p in principals] == [("credenciais de quem chama", "arn:aws:iam::1:role/papel", None, None)]
    assert reason is None
    assert redshift.copy_principals(None, roles, None) == ([], "sem SERIALIZE_DB_REDSHIFT_IAM_ROLE e sem identidade do STS: nada a simular")

    # default é o papel padrão do namespace; sem padrão, o COPY o recusaria; sem leitura, nada a simular.
    assert redshift.copy_principals("default", roles, caller)[0][0].arn == "arn:padrao"
    principals, reason = redshift.copy_principals("default", ([], ["arn:associado"]), caller)
    assert principals == [] and "sem papel padrão" in reason
    assert "não foi lido" in redshift.copy_principals("default", None, caller)[1]

    # Um ARN associado passa limpo; um ARN que o namespace não tem ganha o bloqueio; um namespace não lido, a dúvida.
    clean = redshift.copy_principals("arn:associado", roles, caller)[0][0]
    assert (clean.blocker, clean.doubt) == (None, None)
    assert "não está associado" in redshift.copy_principals("arn:outro", roles, caller)[0][0].blocker
    assert "não foi lida" in redshift.copy_principals("arn:outro", None, caller)[0][0].doubt


def test_credential_text_shows_the_key_prefix_the_token_and_the_expiry() -> None:
    """``RS-18``: o prefixo da chave, se há ``SESSION_TOKEN`` e quando as credenciais expiram; o segredo nunca entra."""
    now = dt.datetime(2026, 9, 20, 20, 0, tzinfo=dt.timezone.utc)
    text = redshift.credential_text("AKIAEXEMPLO", "token", now + dt.timedelta(minutes=42), now)
    assert text == "chave AKIA…, SESSION_TOKEN presente, expira 2026-09-20 20:42:00+00:00 (daqui a 42 min)"
    assert "EXEMPLO" not in text
    assert redshift.credential_text("AKIAEXEMPLO", None, None, now) == "chave AKIA…, SESSION_TOKEN ausente"
    assert "(há 5 min)" in redshift.credential_text("AKIAEXEMPLO", "token", now - dt.timedelta(minutes=5), now)


def test_column_value_and_matching_rows_read_by_column_name() -> None:
    """As visões ``svv_all_*`` são lidas pelo nome da coluna; uma coluna ausente vale ``None`` e não derruba a leitura."""
    found = (
        ["database_name", "schema_name", "schema_type"],
        [("dev", "public", "local"), ("datalake_rw_shared", "sbx_aco_decon", "shared"), ("dev", "sbx_aco_decon", "local")],
    )
    assert redshift.column_value(found[0], found[1][1], "schema_type") == "shared"
    assert redshift.column_value(found[0], found[1][1], "coluna_que_a_visao_nao_tem") is None

    # O filtro casa sem diferenciar maiúsculas e aceita mais de uma coluna.
    assert len(redshift.matching_rows(found, schema_name="SBX_ACO_DECON")) == 2
    assert redshift.matching_rows(found, database_name="datalake_rw_shared", schema_name="sbx_aco_decon") == [found[1][1]]
    assert redshift.matching_rows(found, schema_name="ausente") == []


def test_version_tuple_reads_the_patch_from_the_version_string() -> None:
    """O patch sai de ``version()`` como tupla, que a comparação de RS-17 usa."""
    assert redshift.version_tuple("PostgreSQL 8.0.2 on ..., Redshift 1.0.78890") == (1, 0, 78890)
    assert redshift.version_tuple("sem patch") is None


def test_datashare_write_verdict_separates_unmet_from_unread() -> None:
    """Um requisito não atendido reprova; um requisito não lido vira ``note``, porque leitura negada não é requisito reprovado."""
    status, text = redshift.datashare_write_verdict((1, 0, 78890), "serverless", "Snapshot Isolation", 128)
    assert status == "ok" and "atende" in text and "não atende" not in text

    # Patch anterior ao 186: a escrita no datashare é recusada pelo servidor.
    status, text = redshift.datashare_write_verdict((1, 0, 70000), "serverless", "Snapshot Isolation", 128)
    assert status == "fail" and "não atende" in text

    # Isolamento serializável e slices de menos também reprovam.
    assert redshift.datashare_write_verdict((1, 0, 78890), "serverless", "Serializable", 128)[0] == "fail"
    assert redshift.datashare_write_verdict((1, 0, 78890), "serverless", "Snapshot Isolation", 32)[0] == "fail"

    # O que não foi lido não reprova sozinho, e o texto diz qual requisito ficou sem leitura.
    status, text = redshift.datashare_write_verdict(None, "serverless", None, None)
    assert status == "note" and text.count("não lido") == 3

    # O isolamento de um banco de datashare vem UNKNOWN (leitura do ambiente alvo, 2026-09-20): é
    # ausência de leitura, porque o banco que recebe a escrita fica no produtor, e não reprovação.
    status, text = redshift.datashare_write_verdict((1, 0, 436211), "serverless", "UNKNOWN", None)
    assert status == "note" and "isolamento (UNKNOWN, do banco do produtor): não lido" in text

    # O provisionado tem patch mínimo próprio: o mesmo número reprova num e passa no outro.
    assert redshift.datashare_write_verdict((1, 0, 78885), "serverless", "Snapshot Isolation", 64)[0] == "fail"
    assert redshift.datashare_write_verdict((1, 0, 78885), "provisionado", "Snapshot Isolation", 64)[0] == "ok"


def test_data_api_row_decodes_cells_of_one_item() -> None:
    """Cada célula da Data API é um dicionário de um item; ``isNull`` vira ``None``, e o resto vem como está."""
    record = [{"stringValue": "a"}, {"longValue": 1}, {"isNull": True}, {"doubleValue": 1.5}]
    assert redshift.data_api_row(record) == ["a", 1, None, 1.5]


def test_credential_summary_keeps_the_password_out_of_the_report() -> None:
    """O resumo da credencial temporária mostra usuário e expiração; a senha nunca entra no relatório."""
    summary = redshift.credential_summary({"dbUser": "IAMR:papel", "dbPassword": "segredo", "expiration": "2026-09-20 05:00"})
    assert "IAMR:papel" in summary and "2026-09-20 05:00" in summary and "segredo" not in summary


def test_s3_root_prefers_the_argument_then_the_library_root(monkeypatch: pytest.MonkeyPatch) -> None:
    """A raiz que um probe fotografa: o argumento, depois ``SERIALIZE_DB_ROOT``, depois a autorização da suíte."""
    monkeypatch.delenv("SERIALIZE_DB_ROOT", raising=False)
    monkeypatch.delenv("SERIALIZE_DB_TEST_S3_ROOT", raising=False)
    assert probelib.s3_root(["probe.py", "s3://bucket/prefixo/"]) == ("s3://bucket/prefixo", "argumento")
    assert probelib.s3_root(["probe.py"]) == ("", "nada")

    # A raiz da biblioteca vem antes da autorização da suíte, que é só conveniência.
    monkeypatch.setenv("SERIALIZE_DB_ROOT", "s3://bucket/biblioteca")
    monkeypatch.setenv("SERIALIZE_DB_TEST_S3_ROOT", "s3://bucket/suite")
    assert probelib.s3_root(["probe.py"]) == ("s3://bucket/biblioteca", "SERIALIZE_DB_ROOT")
    assert probelib.s3_root(["probe.py", "s3://outro/raiz"]) == ("s3://outro/raiz", "argumento")

    # Uma raiz local da biblioteca não serve a estes probes, que leem S3: a autorização da suíte responde.
    monkeypatch.setenv("SERIALIZE_DB_ROOT", "/pasta/local")
    assert probelib.s3_root(["probe.py"]) == ("s3://bucket/suite", "SERIALIZE_DB_TEST_S3_ROOT")

    # Um argumento que não é s3:// volta como veio, para o probe dizer o que está errado.
    assert probelib.s3_root(["probe.py", "/pasta"]) == ("/pasta", "argumento")


def test_iam_roles_separate_the_default_from_the_ones_merely_attached() -> None:
    """``RS-6``: o papel padrão serve a ``IAM_ROLE default``; um associado serve por ARN; sem nenhum, o ``COPY`` não alcança o S3."""
    clusters = {"Clusters": [{"DefaultIamRoleArn": "arn:padrao", "IamRoles": [{"IamRoleArn": "arn:padrao"}, {"IamRoleArn": "arn:outro"}]}]}
    assert redshift.iam_roles(clusters, {}) == (["arn:padrao"], ["arn:outro"])

    # O namespace serverless do ambiente alvo: sem padrão e sem associado.
    assert redshift.iam_roles(None, {"ns": {"defaultIamRoleArn": None, "iamRoles": []}}) == ([], [])

    # Associado sem padrão: IAM_ROLE default não resolve, e o ARN precisa ser informado.
    assert redshift.iam_roles(None, {"ns": {"iamRoles": ["arn:associado"]}}) == ([], ["arn:associado"])


def test_diagnose_suite_environment_exports_no_proxy_when_absent_or_empty() -> None:
    """O diagnóstico repete o delta-rs com o que a suíte exporta: ``NO_PROXY`` de ``no_proxy`` quando ausente ou vazia, e nada nos demais casos."""
    assert diagnose_aws.suite_environment({"no_proxy": "169.254.170.2,localhost"}) == {"NO_PROXY": "169.254.170.2,localhost"}
    assert diagnose_aws.suite_environment({"NO_PROXY": "", "no_proxy": "169.254.170.2"}) == {"NO_PROXY": "169.254.170.2"}
    assert diagnose_aws.suite_environment({"NO_PROXY": "a", "no_proxy": "b"}) == {}
    assert diagnose_aws.suite_environment({"NO_PROXY": ""}) == {}
    assert diagnose_aws.suite_environment({}) == {}

    assert diagnose_aws.no_proxy_state({}) == "ausente"
    assert diagnose_aws.no_proxy_state({"NO_PROXY": ""}) == "vazia"
    assert diagnose_aws.no_proxy_state({"NO_PROXY": "a"}) == "definida"


def test_diagnose_describe_says_whether_the_service_answered() -> None:
    """No diagnóstico da suíte S3, só a falta de resposta pede manutenção da rede."""
    assert diagnose_aws.describe(client_error("AccessDenied")).startswith("o serviço respondeu com erro: ClientError:")
    assert diagnose_aws.describe(botocore.exceptions.EndpointConnectionError(endpoint_url="x")).startswith("sem resposta: EndpointConnectionError:")


# ---------------------------------------------------------------------------------------------------------------
# parquet_source.py: o caminho da partição, o esquema como chave, a diferença entre esquemas e a soma do rodapé


def column(name: str, arrow_type: str = "int64", nullable: bool = True, physical: str = "INT64", logical: str = "None") -> parquet_source.Column:
    """Uma coluna fabricada, como o probe a monta do esquema Arrow e da folha do esquema Parquet."""
    return parquet_source.Column(name=name, arrow_type=arrow_type, nullable=nullable, physical=physical, logical=logical, converted="NONE", field_id="-")


def reading(path: str, statistics: dict | None = None, columns: list | None = None) -> parquet_source.FileReading:
    """O rodapé fabricado de um arquivo, com a partição derivada do caminho como o probe faz."""
    keys, kind = parquet_source.partition_of(path)
    return parquet_source.FileReading(
        path=path,
        size=10,
        rows=sum(entry.get("rows", 0) for entry in (statistics or {}).values()),
        row_groups=1,
        created_by="parquet-cpp-arrow",
        format_version="2.6",
        columns=columns or [column("id")],
        footer={},
        partition=keys,
        partition_kind=kind,
        compression=("SNAPPY",),
        encodings=("PLAIN",),
        statistics=statistics or {},
    )


def test_partition_of_reads_hive_nameless_and_root() -> None:
    """O caminho diz a partição: Hive dá nome e valor, pastas sem ``=`` dão níveis numerados, a raiz não dá nada."""
    assert parquet_source.partition_of("mes=2026-08/part-0.parquet") == ((("mes", "2026-08"),), "hive")
    assert parquet_source.partition_of("ano=2026/mes=08/x.parquet") == ((("ano", "2026"), ("mes", "08")), "hive")
    assert parquet_source.partition_of("2026/08/dados.parquet") == ((("nível 1", "2026"), ("nível 2", "08")), "pastas sem nome")
    assert parquet_source.partition_of("tabela.parquet") == ((), "sem partição")
    # Um nível misto conta como sem nome: o probe não adivinha qual metade nomeia a coluna.
    assert parquet_source.partition_of("2026/mes=08/x.parquet")[1] == "pastas sem nome"


def test_logical_label_shortens_timestamp_and_decimal() -> None:
    """O tipo lógico sai curto, guardando o fuso e a unidade, que decidem entre ``timestamp`` e ``timestamp_ntz``."""
    verbose = "Timestamp(isAdjustedToUTC=false, timeUnit=microseconds, is_from_converted_type=false, force_set_converted_type=false)"
    assert parquet_source.logical_label(verbose) == "Timestamp(us, utc=false)"
    assert parquet_source.logical_label(verbose.replace("false, timeUnit=microseconds", "true, timeUnit=milliseconds")) == "Timestamp(ms, utc=true)"
    assert parquet_source.logical_label("Decimal(precision=18, scale=2)") == "Decimal(18,2)"
    assert parquet_source.logical_label("Int(bitWidth=32, isSigned=true)") == "Int(32, com sinal=true)"
    assert parquet_source.logical_label("String") == "String"


def test_schema_key_separates_files_by_name_type_and_nullability() -> None:
    """Dois arquivos entram no mesmo grupo só quando nome, tipo, nulidade e tipo físico coincidem na mesma ordem."""
    base = [column("id"), column("valor", "decimal128(18, 2)", physical="FIXED_LEN_BYTE_ARRAY", logical="Decimal(18,2)")]
    assert parquet_source.schema_key(base) == parquet_source.schema_key(list(base))
    assert parquet_source.schema_key(base) != parquet_source.schema_key(list(reversed(base)))
    assert parquet_source.schema_key(base) != parquet_source.schema_key([column("id", nullable=False), base[1]])


def test_schema_difference_names_missing_extra_retyped_and_reordered() -> None:
    """A divergência é dita coluna a coluna: a que falta, a que sobra, a que mudou de tipo e a ordem trocada."""
    reference = [column("id"), column("valor", "double", physical="DOUBLE"), column("mes", "string", physical="BYTE_ARRAY", logical="String")]

    missing = parquet_source.schema_difference(reference, reference[:2])
    assert ["mes", "ausente", "string no majoritário", "-"] in missing

    extra = parquet_source.schema_difference(reference, [*reference, column("id_execucao", "string", physical="BYTE_ARRAY", logical="String")])
    assert extra == [["id_execucao", "a mais", "-", "string"]]

    retyped = parquet_source.schema_difference(reference, [reference[0], column("valor", "decimal128(18, 2)", physical="FIXED_LEN_BYTE_ARRAY", logical="Decimal(18,2)"), reference[2]])
    assert retyped[0] == ["valor", "tipo", "double", "decimal128(18, 2)"]
    assert retyped[1] == ["valor", "tipo físico", "DOUBLE/None", "FIXED_LEN_BYTE_ARRAY/Decimal(18,2)"]

    nullability = parquet_source.schema_difference(reference, [column("id", nullable=False), reference[1], reference[2]])
    assert nullability == [["id", "nulidade", "nulo", "não nulo"]]

    # Mesmas colunas e mesmos tipos em outra ordem: uma leitura posicional, como o COPY do Redshift, quebra.
    reordered = parquet_source.schema_difference(reference, [reference[1], reference[0], reference[2]])
    assert reordered[0][:2] == ["(todas)", "ordem"]

    assert parquet_source.schema_difference(reference, list(reference)) == []


def test_merge_statistics_sums_rows_and_keeps_the_extremes() -> None:
    """O rodapé de vários arquivos vira uma entrada por coluna: linhas e nulos somam, mínimo e máximo são os extremos."""
    merged = parquet_source.merge_statistics(
        [
            reading("mes=2026-07/a.parquet", {"valor": {"rows": 100, "nulls": 3, "min": 5, "max": 50, "distinct": 40}}),
            reading("mes=2026-08/b.parquet", {"valor": {"rows": 200, "nulls": 0, "min": 1, "max": 30, "distinct": 70}}),
        ]
    )
    assert merged["valor"]["rows"] == 300
    assert merged["valor"]["nulls"] == 3 and merged["valor"]["nulls_known"]
    assert (merged["valor"]["min"], merged["valor"]["max"]) == (1, 50)
    # O maior visto num arquivo é um piso da cardinalidade da tabela; a soma contaria duas vezes o valor repetido.
    assert merged["valor"]["distinct"] == 70
    assert merged["valor"]["without"] == 0


def test_better_extreme_keeps_the_first_of_two_types_that_do_not_compare() -> None:
    """O extremo sai do valor que compara; um tipo que não compara com o atual mantém o lido antes."""
    assert parquet_source.better_extreme("min", 5, None) is True
    assert parquet_source.better_extreme("min", 1, 5) is True
    assert parquet_source.better_extreme("min", 9, 5) is False
    assert parquet_source.better_extreme("max", 50, 30) is True
    assert parquet_source.better_extreme("max", 10, 30) is False

    # bytes ao lado de str, que o Parquet permite entre arquivos e entre row groups.
    assert parquet_source.better_extreme("min", b"a", "a") is False
    assert parquet_source.better_extreme("max", b"z", "a") is False


def test_merge_statistics_counts_the_files_without_min_and_max() -> None:
    """Um arquivo sem estatística de mínimo e máximo é contado, para o leitor saber que a faixa é parcial."""
    merged = parquet_source.merge_statistics(
        [
            reading("a.parquet", {"descricao": {"rows": 10, "nulls": None, "min": None, "max": None, "distinct": None}}),
            reading("b.parquet", {"descricao": {"rows": 10, "nulls": 2, "min": "a", "max": "z", "distinct": None}}),
        ]
    )
    assert merged["descricao"]["without"] == 1
    assert (merged["descricao"]["min"], merged["descricao"]["max"]) == ("a", "z")
    assert merged["descricao"]["nulls"] == 2

    # Sem nulo conhecido em arquivo algum, a contagem fica sem verdicto em vez de sair como zero.
    unknown = parquet_source.merge_statistics([reading("a.parquet", {"x": {"rows": 10, "nulls": None, "min": 1, "max": 2, "distinct": None}})])
    assert unknown["x"]["nulls_known"] is False


def test_measure_text_batch_counts_bytes_and_characters() -> None:
    """A medida acumula linhas e nulos e guarda o maior texto em bytes, a medida do String(n)."""
    import pyarrow as pa

    measured = {"nome": {"rows": 0, "nulls": 0, "bytes": 0, "chars": 0}}
    batch = pa.RecordBatch.from_pydict({"nome": ["ação", None, "ab"]})
    parquet_source.measure_text_batch(measured, batch)
    # "ação" tem 4 caracteres e 6 bytes em UTF-8: o ç e o ã levam dois cada.
    assert measured["nome"] == {"rows": 3, "nulls": 1, "bytes": 6, "chars": 4}

    parquet_source.measure_text_batch(measured, pa.RecordBatch.from_pydict({"nome": ["x" * 9]}))
    assert measured["nome"] == {"rows": 4, "nulls": 1, "bytes": 9, "chars": 9}

    # Um lote só de nulos soma as linhas e não mexe no maior texto.
    parquet_source.measure_text_batch(
        measured, pa.RecordBatch.from_pydict({"nome": pa.array([None], pa.string())})
    )
    assert measured["nome"] == {"rows": 5, "nulls": 2, "bytes": 9, "chars": 9}


def test_format_value_decodes_bytes_and_cuts_long_text() -> None:
    """Um valor de estatística cabe na célula: bytes viram texto, o ilegível vira hexadecimal, o longo é cortado."""
    assert parquet_source.format_value(None) == "-"
    assert parquet_source.format_value(b"2026-08") == "2026-08"
    assert parquet_source.format_value(b"\xff\xfe").startswith("0x")
    assert parquet_source.format_value("linha\nquebrada") == "linha\\nquebrada"
    assert parquet_source.format_value("x" * 100, limit=10) == "x" * 9 + "…"


def test_partition_values_lists_each_value_once_in_order() -> None:
    """Os valores de cada coluna de partição saem sem repetição, na ordem em que os arquivos os trouxeram."""
    values = parquet_source.partition_values(
        [reading("mes=2026-07/a.parquet"), reading("mes=2026-07/b.parquet"), reading("mes=2026-06/c.parquet")]
    )
    assert values == {"mes": ["2026-07", "2026-06"]}


def test_text_columns_lists_the_text_columns_once() -> None:
    """As colunas de texto saem na ordem do primeiro arquivo que as traz, sem repetição."""
    primeiro = reading("a.parquet", columns=[column("id"), column("nome", arrow_type="string"), column("data", arrow_type="date32[day]")])
    segundo = reading("b.parquet", columns=[column("nome", arrow_type="string"), column("meta", arrow_type="large_string")])
    assert parquet_source.text_columns([primeiro, segundo]) == ["nome", "meta"]
    assert parquet_source.text_columns([reading("c.parquet", columns=[column("id")])]) == []


def test_parse_reads_the_root_and_the_options() -> None:
    """A linha de comando: a raiz é obrigatória, ``--sample`` e ``--files`` pedem número, ``--text-bytes`` é uma chave, e o resto é uso errado."""
    assert parquet_source.parse(["probe", "/base"]) == ("/base", 0, parquet_source.DEFAULT_FILE_ROWS, False)
    assert parquet_source.parse(["probe", "/base", "--sample", "500"]) == ("/base", 500, parquet_source.DEFAULT_FILE_ROWS, False)
    assert parquet_source.parse(["probe", "--files", "5", "s3://bucket/prefixo"]) == ("s3://bucket/prefixo", 0, 5, False)
    assert parquet_source.parse(["probe", "/base", "--text-bytes"]) == ("/base", 0, parquet_source.DEFAULT_FILE_ROWS, True)
    assert parquet_source.parse(["probe"]) is None
    assert parquet_source.parse(["probe", "/base", "--sample"]) is None
    assert parquet_source.parse(["probe", "/base", "--sample", "x"]) is None
    assert parquet_source.parse(["probe", "/base", "/outra"]) is None
    assert parquet_source.parse(["probe", "/base", "--desconhecida"]) is None
