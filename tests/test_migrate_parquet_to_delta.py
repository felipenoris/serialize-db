"""``scripts/migrate_parquet_to_delta.py``: a migração adiantada, sobre o pacote, na base fictícia.

Os testes escrevem sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``): a base de
``tests/source_db_projetado.py`` numa pasta da sessão e as tabelas Delta em outras, uma raiz por
teste, com a pasta temporária do processo apontada para a pasta do teste, onde o motor DuckDB de
cada chamada de ``initial_load`` abre o banco. Eles conferem a linha de comando sobre a base
inteira, duas vezes, com o ambiente, cada tabela e o que ficou fora do modelo no relatório JSON; o
relatório parcial de uma carga interrompida numa partição fora do contrato; e a recusa de um modelo
que viola o contrato, sem ler a origem. A carga em si e o relatório de contagens e somas são de
``serialize_db.load``, cobertos por ``tests/test_load.py``.
"""

from __future__ import annotations

import datetime
import json
import shutil
import tempfile
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import migrate_parquet_to_delta as migrate
import source_db_projetado as source
from client_model import Base
from conftest import LocalLocation

pytestmark = pytest.mark.local

TABLES = Base.metadata.tables
OUTSIDE_MODEL = ["alembic_version", "meta_update_status", "schema.json"]


@pytest.fixture(scope="module")
def base(local_location: LocalLocation) -> source.SourceBase:
    """A base fictícia gravada uma vez por módulo sob a pasta da sessão."""
    return source.write_source(Path(local_location.child("migracao-origem")))


@pytest.fixture
def folder(local_location: LocalLocation, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Uma pasta nova por teste sob a raiz da sessão, que também recebe a pasta temporária do
    processo."""
    path = Path(local_location.child(f"migracao/{uuid.uuid4().hex[:8]}"))
    path.mkdir(parents=True)
    monkeypatch.setattr(tempfile, "tempdir", str(path))
    return path


def rewrite_first_chunk(folder: Path, column: str, value: object) -> None:
    """Regrava ``chunk_0.parquet`` de ``folder`` com ``value`` na primeira linha de ``column``, no
    layout da origem."""
    path = folder / "chunk_0.parquet"
    chunk = pq.read_table(path)
    values = chunk.column(column).to_pylist()
    values[0] = value
    field = chunk.schema.field(column)
    index = chunk.schema.get_field_index(column)
    altered = chunk.set_column(index, field, pa.array(values, field.type))
    pq.write_table(altered, path, version="1.0", use_dictionary=False,
                   use_deprecated_int96_timestamps=True)


def test_main_migrates_the_whole_base(base: source.SourceBase, folder: Path,
                                      capsys: pytest.CaptureFixture) -> None:
    """A linha de comando sobre a base inteira: as tabelas sem partição antes das particionadas,
    cada partição com linhas, tempo e pico, o relatório em JSON com as 12 tabelas iguais, o
    ambiente e o que ficou fora do modelo, saída 0; a segunda execução não grava nada."""
    root = str(folder / "delta")
    report_path = folder / "relatorio-migracao.json"
    arguments = ["--metadata", "client_model:Base.metadata", "--source", str(base.root),
                 "--root", root, "--environment", "prod", "--report", str(report_path)]
    assert migrate.main(arguments) == 0
    printed = capsys.readouterr().out
    first_line = printed.splitlines()[0]
    assert " CPUs, " in first_line
    assert "memory_limit" in first_line
    assert "fora do modelo: alembic_version, meta_update_status, schema.json" in printed
    assert "12 tabelas conferidas, contagens e somas iguais" in printed
    assert Path(root, "prod", "cad_lancamentos", "_delta_log").is_dir()

    # O relatório: o ambiente, a ordem, as contagens e as partições.
    document = json.loads(report_path.read_text())
    environment = document["environment"]
    assert environment["cpus"] > 0
    assert environment["memory_total_mb"] > 0
    assert {"threads", "memory_limit"} <= set(environment["duckdb_limits"])
    assert environment["arguments"]["environment"] == "prod"
    table_order = [table["table"] for table in document["tables"]]
    assert table_order[-4:] == ["cad_operacoes", "rel_contrato_operacao", "cad_contratos",
                                "cad_lancamentos"]
    assert len(document["tables"]) == len(TABLES)
    assert all(table["matches"] for table in document["tables"])
    assert document["outside_model"] == OUTSIDE_MODEL
    source_rows_by_table = {}
    for table in document["tables"]:
        partition_rows = [partition["source_rows"] for partition in table["partitions"]]
        source_rows_by_table[table["table"]] = sum(partition_rows)
    assert source_rows_by_table == {name: base.rows[name] for name in TABLES}
    for table in document["tables"]:
        assert table["loaded"], table["table"]
        for partition in table["loaded"]:
            assert partition["rows"] > 0, table["table"]
            assert partition["seconds"] > 0, table["table"]
            assert partition["peak_rss_mb"] > 0, table["table"]
    tables_by_name = {table["table"]: table for table in document["tables"]}
    entries = tables_by_name["cad_lancamentos"]
    loaded_values = [partition["value"] for partition in entries["loaded"]]
    assert loaded_values == list(source.PARTITION_VALUES)
    assert "timestamp: INT96 -> timestamp[us]" in entries["conversions"]

    # A segunda execução não grava nada.
    assert migrate.main(arguments) == 0
    document = json.loads(report_path.read_text())
    assert all(table["loaded"] == [] for table in document["tables"])
    assert "in_progress" not in document


def test_report_keeps_the_progress_of_an_interrupted_load(base: source.SourceBase, folder: Path,
                                                          capsys: pytest.CaptureFixture) -> None:
    """Uma carga interrompida em 2026-03-31 deixa no relatório a tabela da vez em ``in_progress``,
    com as partições já gravadas: o JSON é regravado a cada partição."""
    # Uma cópia de cad_operacoes com a partição 2026-03-31 fora do contrato.
    source_root = folder / "origem"
    shutil.copytree(base.root / "cad_operacoes", source_root / "cad_operacoes")
    rewrite_first_chunk(source_root / "cad_operacoes" / "data_str=2026-03-31", "data",
                        datetime.date(2026, 2, 28))
    report_path = folder / "relatorio.json"
    arguments = ["--metadata", "client_model:Base.metadata", "--source", str(source_root),
                 "--root", str(folder / "delta"), "--tables", "cad_operacoes",
                 "--report", str(report_path)]
    assert migrate.main(arguments) == 1
    assert "ContractError: cad_operacoes partição 2026-03-31" in capsys.readouterr().err

    # O relatório parcial.
    document = json.loads(report_path.read_text())
    progress = document["in_progress"]
    assert progress["table"] == "cad_operacoes"
    assert [load["value"] for load in progress["loaded"]] == ["2026-01-31", "2026-02-28"]
    assert document["tables"] == []
    assert document["environment"]["arguments"]["tables"] == ["cad_operacoes"]


def test_main_refuses_a_model_with_violations(base: source.SourceBase, folder: Path,
                                              capsys: pytest.CaptureFixture) -> None:
    """O modelo de referência viola o contrato: saída 2 com a lista, sem ler a origem; uma
    tabela fora do modelo é erro de uso."""
    never_written = folder / "nunca-gravada"
    arguments = ["--metadata", "reference_model.model_db_projetado:Base.metadata",
                 "--source", str(base.root), "--root", str(never_written)]
    assert migrate.main(arguments) == 2
    assert "modelo fora do contrato" in capsys.readouterr().err
    assert not never_written.exists()

    # A tabela fora do modelo.
    with pytest.raises(SystemExit) as refusal:
        migrate.main(["--metadata", "client_model:Base.metadata", "--source", str(base.root),
                      "--root", str(never_written), "--tables", "nada"])
    assert refusal.value.code == 2
    assert not never_written.exists()
