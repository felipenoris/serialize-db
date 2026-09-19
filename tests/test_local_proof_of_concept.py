"""Prova de conceito da camada Delta numa pasta local, o segundo armazenamento da biblioteca.

A pasta vem de ``SERIALIZE_DB_TEST_LOCAL_ROOT`` ou da pasta temporária do pytest, e nada aqui toca a
AWS: a suíte roda em qualquer ambiente e valida o Python, o delta-rs, o DuckDB e as extensões antes
da suíte no S3. Os testes comuns aos dois armazenamentos vêm de ``delta_proof_of_concept.py``; os
deste módulo cobrem o que só faz sentido em disco: a primitiva do commit atômico e o conflito entre
escritores, os caminhos relativos do log com a realocação da pasta, e a abertura sem variáveis AWS.

Só a extensão ``delta`` do DuckDB é necessária; sem ela e sem internet, os testes que a usam são
pulados.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pyarrow.compute as pc
import pytest
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError

from conftest import LocalLocation, duckdb_extension_directory
from delta_proof_of_concept import (
    APPENDED_ROWS,
    MONTHS,
    ROWS,
    DeltaProofOfConcept,
    connect_duckdb,
    sample_table,
    timed,
    write_sample_table,
)

pytestmark = pytest.mark.local


@pytest.fixture(scope="session")
def storage(local_location: LocalLocation) -> LocalLocation:
    """Raiz da sessão em disco."""
    return local_location


@pytest.fixture(scope="session")
def table_uri(storage: LocalLocation) -> str:
    """Tabela ``operacoes`` gravada na pasta da sessão."""
    return write_sample_table(storage)


@pytest.fixture(scope="session")
def duckdb_connection() -> duckdb.DuckDBPyConnection:
    """Conexão com a extensão ``delta`` carregada; uma pasta local dispensa ``httpfs``, ``aws`` e secrets."""
    return connect_duckdb(("delta",))


class TestLocalProofOfConcept(DeltaProofOfConcept):
    """Os testes comuns sobre a pasta mais os próprios do disco local."""

    def test_commit_is_atomic_on_disk(self, storage: LocalLocation) -> None:
        """O commit em disco cria o arquivo do log só se ele não existe, o equivalente do ``If-None-Match`` no S3.

        Dois escritores abertos na mesma versão: o segundo ``overwrite`` do mesmo mês falha com
        ``CommitFailedError``; ``overwrite`` de meses diferentes e ``append`` mais ``append`` comitam os dois.
        """
        uri = storage.child("commit_probe")
        small = sample_table().slice(ROWS // 2 - 500, 1000)  # 500 linhas de cada mês
        write_deltalake(uri, small, mode="overwrite", partition_by=["mes"])
        with pytest.raises(FileExistsError):
            open(Path(uri) / "_delta_log" / "00000000000000000000.json", "x").close()
        by_month = {month: small.filter(pc.field("mes") == month) for month in MONTHS}

        def overwrite(table: DeltaTable, month: str) -> None:
            write_deltalake(table, by_month[month], mode="overwrite", predicate=f"mes = '{month}'")

        first, second = DeltaTable(uri), DeltaTable(uri)
        overwrite(first, MONTHS[0])
        with pytest.raises(CommitFailedError):
            overwrite(second, MONTHS[0])
        assert DeltaTable(uri).version() == 1

        first, second = DeltaTable(uri), DeltaTable(uri)
        overwrite(first, MONTHS[0])
        overwrite(second, MONTHS[1])
        assert DeltaTable(uri).version() == 3

        first, second = DeltaTable(uri), DeltaTable(uri)
        write_deltalake(first, small.slice(0, 10), mode="append")
        write_deltalake(second, small.slice(0, 10), mode="append")
        table = DeltaTable(uri)
        assert table.version() == 5
        assert table.to_pyarrow_table().num_rows == small.num_rows + 20

    def test_folder_relocates(self, storage: LocalLocation, duckdb_connection: duckdb.DuckDBPyConnection, table_uri: str) -> None:
        """O log guarda caminhos relativos, e a pasta copiada abre na mesma versão pelo delta-rs e pelo DuckDB.

        É a propriedade que leva um banco entre pastas, e entre a pasta e o S3, sem reescrever metadado.
        """
        paths = DeltaTable(table_uri).get_add_actions().column("path").to_pylist()
        assert paths and not any(path.startswith(("/", "file:")) for path in paths)
        copy = Path(storage.child("relocated")) / "operacoes"
        timed(storage, "copytree", lambda: shutil.copytree(table_uri, copy))
        moved = DeltaTable(str(copy))
        assert moved.version() == 1
        assert moved.to_pyarrow_table().num_rows == ROWS + APPENDED_ROWS
        assert DeltaTable(copy.as_uri()).version() == 1  # a URI file:// equivale ao caminho
        count = duckdb_connection.execute(f"SELECT count(*) FROM delta_scan('{copy}')").fetchone()[0]
        assert count == ROWS + APPENDED_ROWS

    @pytest.mark.usefixtures("duckdb_connection")
    def test_opens_without_aws_environment(self, table_uri: str, tmp_path: Path) -> None:
        """Sem variáveis ``AWS_*``, sem proxy e sem ``~/.aws``, o delta-rs e o DuckDB abrem a tabela.

        O subprocesso recebe a pasta de extensões do DuckDB resolvida aqui, porque o ``HOME`` vazio
        esconde a pasta padrão.
        """
        directory = duckdb_extension_directory() or str(Path.home() / ".duckdb" / "extensions")
        probe = "\n".join(
            [
                "import duckdb",
                "from deltalake import DeltaTable",
                f"print(DeltaTable({table_uri!r}).version())",
                f"connection = duckdb.connect(config={{'extension_directory': {directory!r}}})",
                "connection.execute('LOAD delta')",
                f"print(connection.execute(\"SELECT count(*) FROM delta_scan('{table_uri}')\").fetchone()[0])",
            ]
        )
        proxies = {"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY"}
        environment = {
            name: value for name, value in os.environ.items() if not name.startswith("AWS_") and name.upper() not in proxies
        }
        environment["HOME"] = str(tmp_path)
        completed = subprocess.run([sys.executable, "-c", probe], env=environment, capture_output=True, text=True, timeout=120)
        assert completed.returncode == 0, completed.stderr
        assert completed.stdout.split() == ["1", str(ROWS + APPENDED_ROWS)]
