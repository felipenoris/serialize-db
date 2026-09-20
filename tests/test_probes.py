"""Testes das funções puras dos probes, sem rede e sem AWS.

Os probes (``probes/``) leem o ambiente; as decisões que eles tomam sobre o que leram (classificar um
erro do ``boto3``, rotular um IP, montar as tabelas do inventário do bucket, interpretar o Object
Lock ou a montagem de ``~/shared``) são funções puras, testadas aqui com respostas fabricadas. Um
``Report`` grava em ``probes/output/``; ``make_report`` o aponta para a pasta temporária do teste e
devolve ``sys.stdout`` ao pytest no fim. Nenhum teste grava fora de ``tmp_path``.
"""

from __future__ import annotations

import contextlib
import datetime as dt
import os
import sys
from pathlib import Path

import botocore.exceptions
import pytest

PROBES = Path(__file__).resolve().parent.parent / "probes"
sys.path.insert(0, str(PROBES))

import bucket  # noqa: E402
import catalog  # noqa: E402
import diagnose_aws  # noqa: E402
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
    assert bucket.principal_arn(assumed) == "arn:aws:iam::892278726726:role/datazone_usr_role_x"
    assert bucket.principal_arn("arn:aws:iam::1:user/eu") == "arn:aws:iam::1:user/eu"


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


def test_dev_requirements_read_the_pinned_versions_of_pyproject() -> None:
    """``SP-9`` compara o venv com o grupo ``dev`` de ``pyproject.toml``: nome de importação e versão quando fixada por ``==``."""
    requirements = space.dev_requirements()
    assert requirements["deltalake"] == "1.6.4" and requirements["duckdb"] == "1.5.5"
    assert "redshift_connector" in requirements and "sqlalchemy_redshift" in requirements
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
