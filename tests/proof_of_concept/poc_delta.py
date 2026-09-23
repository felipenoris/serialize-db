"""Prova de conceito da camada Delta comum aos dois tipos de armazenamento.

``DeltaProofOfConcept`` reúne os testes que valem tanto para um bucket S3 quanto para uma pasta
local: a escrita e a leitura pelo delta-rs, o ``delta_scan`` com os tipos do contrato e a poda de
partição, os tempos de consulta e o ``vacuum``. ``test_local.py`` acrescenta o que só faz sentido
em disco (o commit atômico e o conflito entre escritores, a realocação da pasta, a abertura sem
variáveis ``AWS_*``) e ``test_s3.py`` o que só existe no bucket (a origem das credenciais, a cadeia
do delta-rs e a forma da reserva que a biblioteca não usa, o put condicional, a criptografia, listar, copiar e apagar pelo
``boto3``). ``test_s3.py`` e ``test_local.py`` herdam a classe, fornecem as fixtures ``storage`` (a raiz
da sessão), ``table_uri`` (a tabela ``operacoes`` gravada por ``write_sample_table``) e
``duckdb_connection`` (a conexão com as extensões daquele armazenamento) e acrescentam os testes
próprios do seu armazenamento. As medições vão para o relatório da sessão com o prefixo do
armazenamento, como ``local.timing.delta_scan.aggregate_first``.

O módulo também é o material comum dos outros testes da pasta: ``sample_table`` é a amostra com os
tipos do contrato, ``connect_duckdb`` abre o DuckDB com a regra de extensões do projeto, e
``run_in_threads`` roda ações em paralelo e mede o tempo até a última terminar.
"""

from __future__ import annotations

import datetime as dt
import decimal
import functools
import os
import re
import time
from collections.abc import Callable, Iterable
from concurrent.futures import ThreadPoolExecutor

import duckdb
import pyarrow as pa
import pytest
from deltalake import DeltaTable, write_deltalake

from conftest import REPORT, Storage, duckdb_extension_directory, record

ROWS = 300_000
APPENDED_ROWS = 10
MONTHS = ("2026-01", "2026-02")


def timed(storage: Storage, label: str, action: Callable[[], object], repeat: int = 1) -> object:
    """Executa ``action`` ``repeat`` vezes e registra o tempo total em ``timing.<label>`` do armazenamento."""
    started = time.perf_counter()
    result = None

    for _ in range(repeat):
        result = action()

    storage.record(f"timing.{label}", f"{time.perf_counter() - started:.3f} s")
    return result


def run_in_threads(actions: list[Callable[[], object]]) -> float:
    """Roda as ações em threads, uma por ação, e devolve o tempo até a última terminar.

    A exceção de uma ação sobe no chamador por ``future.result()``: numa ``threading.Thread`` ela
    só seria impressa, e o teste falharia depois com um sintoma sem relação.
    """
    started = time.perf_counter()
    with ThreadPoolExecutor(max_workers=len(actions)) as pool:
        futures = [pool.submit(action) for action in actions]
        for future in futures:
            future.result()
    return time.perf_counter() - started


def seconds(storage: Storage, label: str) -> float:
    """Tempo registrado por ``timed`` para ``label``, em segundos."""
    return float(str(REPORT[f"{storage.name}.timing.{label}"]).split()[0])


@functools.cache
def sample_table() -> pa.Table:
    """Amostra de ``operacoes`` com os tipos do contrato: inteiros, decimal, timestamp e texto.

    A primeira metade das linhas leva ``mes`` 2026-01 e a segunda 2026-02; ``mes`` é a coluna de
    partição, e o valor não deriva de ``data_ref``, que avança um minuto por linha.
    """
    half = ROWS // 2
    start = dt.datetime(2026, 1, 1)

    return pa.table(
        {
            "id_operacao": pa.array(range(ROWS), pa.int64()),
            "mes": pa.array([MONTHS[0]] * half + [MONTHS[1]] * (ROWS - half), pa.string()),
            "data_ref": pa.array([start + dt.timedelta(minutes=i) for i in range(ROWS)], pa.timestamp("us")),
            "id_cliente": pa.array([i % 1000 for i in range(ROWS)], pa.int32()),
            "valor": pa.array([decimal.Decimal(i) / 100 for i in range(ROWS)], pa.decimal128(18, 2)),
            "descricao": pa.array([f"operacao {i}" for i in range(ROWS)], pa.string()),
        }
    )


def write_sample_table(storage: Storage) -> str:
    """Grava ``operacoes`` sob a raiz da sessão, ``overwrite`` particionado por ``mes`` e um ``append``, e devolve a URI."""
    uri = storage.child("operacoes")
    sample = sample_table()

    # Versão 0: a tabela inteira, um arquivo por partição. Versão 1: dez linhas a mais, num commit condicional.
    timed(storage, "write_deltalake.overwrite", lambda: write_deltalake(uri, sample, mode="overwrite", partition_by=["mes"]))
    timed(storage, "write_deltalake.append", lambda: write_deltalake(uri, sample.slice(0, APPENDED_ROWS), mode="append"))

    return uri


def connect_duckdb(extensions: Iterable[str]) -> duckdb.DuckDBPyConnection:
    """Conexão com ``extensions`` carregadas da pasta de extensões.

    A suíte instala uma extensão que falta só na pasta informada em ``SERIALIZE_DB_DUCKDB_EXTENSIONS``,
    e uma instalação que falha ali é falha; sem a variável, a extensão que falta pula o teste. A
    instalação automática do DuckDB fica desligada: com ela, o ``LOAD`` de uma extensão conhecida a
    baixaria para a pasta de extensões sem aviso.
    """
    directory = duckdb_extension_directory()

    # As opções de configuração entram na abertura da conexão; ``extension_directory`` só existe aqui.
    config: dict[str, object] = {"autoinstall_known_extensions": False, "autoload_known_extensions": False}
    if directory:
        config["extension_directory"] = directory

    connection = duckdb.connect(config=config)
    record("duckdb.extension_directory", directory or "(padrão)")

    may_install = bool(os.environ.get("SERIALIZE_DB_DUCKDB_EXTENSIONS"))
    for extension in extensions:
        try:
            connection.execute(f"LOAD {extension}")
        except duckdb.Error as error:
            if not may_install:
                pytest.skip(
                    f"extensão {extension} do DuckDB não instalada em {directory or '(padrão)'}: informe "
                    f"SERIALIZE_DB_DUCKDB_EXTENSIONS para a suíte instalá-la, ou rode prepare_offline.sh "
                    f"({str(error).splitlines()[0]})"
                )
            connection.execute(f"INSTALL {extension}; LOAD {extension}")

    record("duckdb.version", duckdb.__version__)
    record("duckdb.threads", connection.execute("SELECT current_setting('threads')").fetchone()[0])

    return connection


class DeltaProofOfConcept:
    """Testes que valem para os dois armazenamentos; a subclasse é coletada com as fixtures do seu módulo."""

    def test_write_and_open(self, storage: Storage, table_uri: str) -> None:
        """A tabela gravada tem os dois commits, um arquivo por mês mais o do ``append`` e o protocolo esperado."""
        table = DeltaTable(table_uri)

        # Duas versões: o overwrite (0) e o append (1); um arquivo por partição mais o do append.
        assert table.version() == 1
        assert len(table.file_uris()) == len(MONTHS) + 1

        # A coluna timestamp sem fuso exige o recurso timestampNtz, que eleva o protocolo a leitor 3 e escritor 7.
        protocol = table.protocol()
        assert protocol.min_reader_version == 3 and protocol.min_writer_version == 7
        assert "timestampNtz" in (protocol.writer_features or [])

        # O histórico vem do mais recente para o mais antigo; os dois commits são operações WRITE.
        history = table.history(2)
        assert [entry["operation"] for entry in history] == ["WRITE", "WRITE"]
        record("delta.client_version", history[0].get("clientVersion"))

        # Leitura pelo delta-rs: a tabela inteira em Arrow.
        rows = timed(storage, "delta_rs.to_pyarrow_table", lambda: table.to_pyarrow_table().num_rows)
        assert rows == ROWS + APPENDED_ROWS

    def test_delta_scan_reads_types(self, storage: Storage, duckdb_connection: duckdb.DuckDBPyConnection, table_uri: str) -> None:
        """``delta_scan`` lê a tabela com os tipos do contrato e soma o ``DECIMAL`` sem perda."""
        # DESCRIBE mostra o tipo que o DuckDB atribui a cada coluna lida do Delta.
        described = duckdb_connection.execute(f"DESCRIBE SELECT * FROM delta_scan('{table_uri}')").fetchall()
        types = {name: kind for name, kind, *_ in described}
        assert types == {
            "id_operacao": "BIGINT",
            "mes": "VARCHAR",
            "data_ref": "TIMESTAMP",
            "id_cliente": "INTEGER",
            "valor": "DECIMAL(18,2)",
            "descricao": "VARCHAR",
        }

        # A mesma agregação duas vezes: a primeira leitura paga o log e os arquivos, a segunda mede o cache.
        query = f"SELECT count(*), sum(valor) FROM delta_scan('{table_uri}')"
        count, total = timed(storage, "delta_scan.aggregate_first", lambda: duckdb_connection.execute(query).fetchone())
        timed(storage, "delta_scan.aggregate_again", lambda: duckdb_connection.execute(query).fetchone())

        assert count == ROWS + APPENDED_ROWS
        expected = sum(decimal.Decimal(i) / 100 for i in range(ROWS)) + sum(decimal.Decimal(i) / 100 for i in range(APPENDED_ROWS))
        assert total == expected

    def test_delta_scan_prunes_partitions(self, storage: Storage, duckdb_connection: duckdb.DuckDBPyConnection, table_uri: str) -> None:
        """O filtro por ``mes`` lê só os arquivos da partição."""
        query = f"SELECT count(*) FROM delta_scan('{table_uri}') WHERE mes = '{MONTHS[1]}'"
        count = timed(storage, "delta_scan.aggregate_one_month", lambda: duckdb_connection.execute(query).fetchone()[0])
        assert count == ROWS - ROWS // 2

        # EXPLAIN ANALYZE devolve o plano executado; "Scanning Files: lidos/total" mostra a poda por partição.
        plan = duckdb_connection.execute(f"EXPLAIN ANALYZE {query}").fetchone()[1]
        scanned = re.search(r"Scanning Files: (\d+)/(\d+)", plan)
        assert scanned, "o plano não informa os arquivos lidos"

        storage.record("delta_scan.files_scanned_one_month", f"{scanned.group(1)}/{scanned.group(2)}")
        assert int(scanned.group(1)) == 1

    def test_scan_timings(self, storage: Storage, duckdb_connection: duckdb.DuckDBPyConnection, table_uri: str) -> None:
        """Consultas pontuais: ``delta_scan``, ``ATTACH`` fixado, ``read_parquet`` e tabela materializada.

        A regra de ingestão da biblioteca (materializar o que é consultado mais de uma vez) depende de a
        tabela materializada ser mais rápida que o ``delta_scan`` repetido.
        """
        con = duckdb_connection
        repeat = 20
        point = f"WHERE id_operacao = {ROWS // 2}"

        # Os quatro caminhos de leitura: view sobre delta_scan, tabela anexada com snapshot fixado,
        # os arquivos Parquet lidos diretamente (sem o log) e uma tabela materializada no DuckDB.
        con.execute(f"CREATE VIEW scan AS SELECT * FROM delta_scan('{table_uri}')")
        con.execute(f"ATTACH '{table_uri}' AS pinned (TYPE delta, PIN_SNAPSHOT true)")
        parquet = f"read_parquet('{table_uri}/*/*.parquet', hive_partitioning = true)"

        def run(sql: str) -> Callable[[], object]:
            return lambda: con.execute(sql).fetchall()

        scan = timed(storage, "point_queries.delta_scan", run(f"SELECT * FROM scan {point}"), repeat)
        timed(storage, "point_queries.delta_scan_with_month", run(f"SELECT * FROM scan {point} AND mes = '{MONTHS[1]}'"), repeat)
        timed(storage, "point_queries.attach_pinned", run(f"SELECT * FROM pinned {point}"), repeat)
        timed(storage, "point_queries.read_parquet", run(f"SELECT * FROM {parquet} {point}"), repeat)
        timed(storage, "aggregate.read_parquet", run(f"SELECT count(*), sum(valor) FROM {parquet}"))

        timed(storage, "materialize.create_table_as", run("CREATE TABLE materialized AS SELECT * FROM scan"))
        timed(storage, "aggregate.materialized", run("SELECT count(*), sum(valor) FROM materialized"))
        materialized = timed(storage, "point_queries.materialized", run(f"SELECT * FROM materialized {point}"), repeat)

        assert len(scan) == 1 and scan == materialized
        assert seconds(storage, "point_queries.materialized") < seconds(storage, "point_queries.delta_scan")

    def test_vacuum_deletes_files(self, storage: Storage) -> None:
        """``vacuum(dry_run=False)`` remove do armazenamento os arquivos substituídos."""
        uri = storage.child("vacuum_probe")
        small = sample_table().slice(0, 1000)

        # Dois overwrites da mesma tabela: o segundo deixa o arquivo do primeiro fora do snapshot, mas em disco.
        write_deltalake(uri, small, mode="overwrite", partition_by=["mes"])
        write_deltalake(uri, small, mode="overwrite", partition_by=["mes"])
        assert len(storage.data_files(uri)) == 2

        # A retenção padrão (168 h) é desligada só para o teste; em produção o vacuum usa keep_versions.
        table = DeltaTable(uri)
        removed = timed(storage, "vacuum.delete", lambda: table.vacuum(retention_hours=0, enforce_retention_duration=False, dry_run=False))

        assert len(removed) == 1
        assert len(storage.data_files(uri)) == 1
