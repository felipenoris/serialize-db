"""``scripts/migrate_parquet_to_delta.py``: a migração adiantada sobre a base fictícia.

Os testes escrevem sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``): a base de
``tests/source_db_projetado.py`` numa pasta da sessão e as tabelas Delta em outras, uma raiz por
teste. Eles conferem a descoberta das partições e do que fica fora do modelo; a consulta que leva
a partição ao contrato, com ``to`` entre aspas; a carga de cada partição uma vez só, a
retomada depois de uma interrupção e o filtro de partições; os dois modos com o mesmo
relatório e os tipos
do contrato nos arquivos gravados; a ordem da ``sort_key``; as recusas sem commit (valor da
coluna de origem fora do caminho, nulo em coluna ``NOT NULL``, texto acima de ``String(n)``), nos
dois modos; o relatório que acusa uma linha apagada; e a linha de comando sobre a base inteira,
duas vezes. A extensão ``delta`` do DuckDB precisa estar na pasta de extensões
(``SERIALIZE_DB_DUCKDB_EXTENSIONS``, senão ``.duckdb/`` na raiz do repositório).
"""

from __future__ import annotations

import datetime as dt
import json
import re
import shutil
import sys
import uuid
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable

import source_db_projetado as source
from client_model import Base
from serialize_db import schema
from serialize_db.errors import ContractError

# O script vive em scripts/, fora do pacote e de tests/: a pasta entra no caminho de importação.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))
import migrate_parquet_to_delta as migrate  # noqa: E402

pytestmark = pytest.mark.local

TABLES = Base.metadata.tables
OUTSIDE_MODEL = ["alembic_version", "meta_update_status", "schema.json"]
PARTITION_VALUES = list(source.PARTITION_VALUES)


def settings(
    mode: str = "register", sort: bool = True, partitions: tuple[str, ...] = ()
) -> migrate.Settings:
    """As configurações de uma execução local, sem S3."""
    return migrate.Settings(mode=mode, sort=sort, partitions=partitions, storage_options={})


@pytest.fixture(scope="module")
def base(local_location) -> source.SourceBase:
    """A base fictícia gravada uma vez por módulo sob a pasta da sessão."""
    return source.write_source(Path(local_location.child("migracao-origem")))


@pytest.fixture(scope="module")
def origin(base: source.SourceBase) -> migrate.Location:
    return migrate.open_location(str(base.root))


@pytest.fixture(scope="module")
def con():
    connection = migrate.connect_duckdb(uses_s3=False, region=None)
    yield connection
    connection.close()


@pytest.fixture
def root(local_location) -> migrate.Location:
    """Uma raiz Delta nova por teste."""
    return migrate.open_location(local_location.child(f"delta-{uuid.uuid4().hex[:8]}"))


def loaded_values(loaded: list[migrate.PartitionLoad]) -> list[str | None]:
    return [load.value for load in loaded]


def physical_types(path: str) -> dict[str, str]:
    """O tipo físico de cada coluna do arquivo Parquet."""
    parquet_schema = pq.ParquetFile(path).schema
    return {
        parquet_schema.column(i).name: parquet_schema.column(i).physical_type
        for i in range(len(parquet_schema))
    }


def test_discover_partitions_and_the_entries_outside_the_pattern(
    base: source.SourceBase, origin: migrate.Location
) -> None:
    """As quatro tabelas particionadas dão os valores do caminho, as oito sem partição dão
    ``None``, e o que a
    raiz tem fora do modelo e o que a pasta da tabela tem fora do padrão vão para as listas."""
    for name, table in TABLES.items():
        options = schema.table_options(table)
        found, skipped = migrate.discover_partitions(origin.child(name), options)
        assert skipped == [], name
        if options.partition_by is None:
            assert list(found) == [None] and found[None].uri == f"{origin.uri}/{name}", name
            continue
        assert list(found) == PARTITION_VALUES, name
        assert (
            found["2026-02-28"].uri == f"{origin.uri}/{name}/{options.partition_by}=2026-02-28"
        ), name
    assert migrate.entries_outside_the_model(origin, Base.metadata) == OUTSIDE_MODEL

    stray = base.root / "cad_operacoes" / "notas.txt"
    stray.write_text("fora do padrão")
    found, skipped = migrate.discover_partitions(
        origin.child("cad_operacoes"), schema.table_options(TABLES["cad_operacoes"])
    )
    assert list(found) == PARTITION_VALUES and skipped == ["notas.txt"]
    stray.unlink()


def test_partition_query_casts_to_the_contract(
    base: source.SourceBase, origin: migrate.Location, con
) -> None:
    """O esquema Arrow do ``SELECT`` é o do contrato: as chaves em ``int64``, o ``timestamp`` em
    microssegundos, a coluna de partição no fim com o valor do caminho; a consulta de
    ``cad_contratos`` roda com ``to`` entre aspas."""
    table = TABLES["cad_lancamentos"]
    query = migrate.partition_query(
        origin.child("cad_lancamentos").child("data_base_str=2026-01-31"), table, "2026-01-31"
    )
    read = con.execute(query).to_arrow_reader().schema
    contract = schema.arrow_schema(table)
    assert read.names == contract.names
    assert [str(field.type) for field in read] == [str(field.type) for field in contract]
    assert (
        str(read.field("id_lancamento").type) == "int64"
        and str(read.field("timestamp").type) == "timestamp[us]"
    )
    assert con.execute(f"SELECT DISTINCT data_base_str FROM ({query})").fetchall() == [
        ("2026-01-31",)
    ]

    contratos = migrate.partition_query(
        origin.child("cad_contratos").child("data_str=2026-02-28"),
        TABLES["cad_contratos"],
        "2026-02-28",
    )
    assert (
        con.execute(f"SELECT count(*) FROM ({contratos})").fetchone()[0]
        == base.partition_rows["cad_contratos"]["2026-02-28"]
    )


def test_initial_load_loads_every_partition_once(
    base: source.SourceBase, origin: migrate.Location, con, root: migrate.Location
) -> None:
    """A primeira passagem grava toda partição num commit cada, com o nome, a partição e as
    retenções no log; a
    segunda não grava nada; uma tabela sem partição carrega uma vez, com o valor ``None``."""
    table = TABLES["cad_contratos"]
    loaded, skipped = migrate.initial_load(con, table, origin, root, settings())
    assert loaded_values(loaded) == PARTITION_VALUES and skipped == []
    assert [load.rows for load in loaded] == [
        base.partition_rows["cad_contratos"][value] for value in PARTITION_VALUES
    ]
    assert all(load.seconds > 0 and load.peak_rss_mb > 0 for load in loaded)
    delta = DeltaTable(root.child("cad_contratos").uri)
    assert delta.version() == len(PARTITION_VALUES)
    assert delta.metadata().name == "cad_contratos" and delta.metadata().partition_columns == [
        "data_str"
    ]
    assert delta.metadata().description == "Contratos por data-base"
    assert delta.metadata().configuration == migrate.RETENTION

    assert migrate.initial_load(con, table, origin, root, settings()) == ([], [])
    assert DeltaTable(root.child("cad_contratos").uri).version() == len(PARTITION_VALUES)

    loaded, _ = migrate.initial_load(con, TABLES["cad_contas"], origin, root, settings())
    assert [(load.value, load.rows) for load in loaded] == [(None, base.rows["cad_contas"])]
    assert migrate.initial_load(con, TABLES["cad_contas"], origin, root, settings())[0] == []
    assert DeltaTable(root.child("cad_contas").uri).version() == 1


def test_partition_filter_loads_only_the_listed_values(
    origin: migrate.Location, con, root: migrate.Location
) -> None:
    """``--partitions`` filtra as partições encontradas, deixa as tabelas sem partição de fora,
    e a passagem
    seguinte sem o filtro carrega só o que falta."""
    only = settings(partitions=("2026-02-28",))
    loaded, _ = migrate.initial_load(con, TABLES["cad_operacoes"], origin, root, only)
    assert loaded_values(loaded) == ["2026-02-28"]
    assert migrate.initial_load(con, TABLES["cad_contas"], origin, root, only)[0] == []

    loaded, _ = migrate.initial_load(con, TABLES["cad_operacoes"], origin, root, settings())
    assert loaded_values(loaded) == ["2026-01-31", "2026-03-31", "2026-06-30"]


def test_interrupted_load_resumes(
    origin: migrate.Location, con, root: migrate.Location, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Uma exceção na terceira partição deixa duas no log; a chamada seguinte carrega só as
    duas restantes."""
    table = TABLES["rel_contrato_operacao"]
    original = migrate.register_partition
    attempts: list[str | None] = []

    def failing_on_the_third(*arguments):
        attempts.append(arguments[4])
        if len(attempts) == 3:
            raise RuntimeError("interrompida")
        return original(*arguments)

    monkeypatch.setattr(migrate, "register_partition", failing_on_the_third)
    with pytest.raises(RuntimeError, match="interrompida"):
        migrate.initial_load(con, table, origin, root, settings())
    uri = root.child("rel_contrato_operacao").uri
    assert attempts == PARTITION_VALUES[:3] and DeltaTable(uri).version() == 2

    monkeypatch.undo()
    loaded, _ = migrate.initial_load(con, table, origin, root, settings())
    assert loaded_values(loaded) == PARTITION_VALUES[2:]
    assert DeltaTable(uri).version() == 4


def test_both_modes_give_the_same_report_and_the_contract_types(
    base: source.SourceBase, origin: migrate.Location, con, local_location
) -> None:
    """``register`` e ``rewrite`` sobre ``cad_lancamentos`` dão o mesmo relatório, o Delta lê
    ``int64`` e ``timestamp[us]``, e o arquivo do ``COPY`` tem ``INT64`` onde a origem tinha
    ``INT32`` e ``INT96``."""
    table = TABLES["cad_lancamentos"]
    reports = {}
    roots = {}
    for mode in ("register", "rewrite"):
        roots[mode] = migrate.open_location(local_location.child(f"delta-modo-{mode}"))
        loaded, skipped = migrate.initial_load(
            con, table, origin, roots[mode], settings(mode=mode)
        )
        reports[mode] = migrate.load_report(con, table, origin, roots[mode], loaded, skipped)
        assert reports[mode].matches, mode
        assert loaded_values(reports[mode].loaded) == PARTITION_VALUES, mode
        delta_schema = pa.schema(DeltaTable(roots[mode].child("cad_lancamentos").uri).schema())
        assert str(delta_schema.field("id_lancamento").type) == "int64", mode
        assert str(delta_schema.field("timestamp").type) == "timestamp[us]", mode

    assert reports["register"].partitions == reports["rewrite"].partitions
    report = reports["register"]
    assert [partition.value for partition in report.partitions] == PARTITION_VALUES
    assert [partition.source_rows for partition in report.partitions] == [
        base.partition_rows["cad_lancamentos"][value] for value in PARTITION_VALUES
    ]
    assert all(partition.source_sums.keys() == {"valor"} for partition in report.partitions)
    assert report.conversions == (
        "id_lancamento: int32 -> int64",
        "id_veiculo: int32 -> int64",
        "id_conta: int32 -> int64",
        "timestamp: INT96 -> timestamp[us]",
        "id_mensuracao: int32 -> int64",
        "id_segmento: int32 -> int64",
        "id_negocio: int32 -> int64",
    )

    written = sorted(DeltaTable(roots["register"].child("cad_lancamentos").uri).file_uris())
    assert len(written) == len(PARTITION_VALUES) and all(
        "/carga_inicial_" in path for path in written
    )
    types = physical_types(written[0])
    assert types["id_lancamento"] == "INT64" and types["timestamp"] == "INT64"
    assert "data_base_str" not in types


def test_rows_are_written_in_sort_key_order(
    base: source.SourceBase, origin: migrate.Location, con, root: migrate.Location
) -> None:
    """As linhas da partição saem na ordem da ``sort_key`` do modelo, que não é a da origem."""
    table = TABLES["cad_lancamentos"]
    key = list(schema.table_options(table).sort_key)
    assert key == ["data_base", "id_mensuracao", "id_veiculo", "id_conta"]
    migrate.initial_load(con, table, origin, root, settings(partitions=("2026-01-31",)))

    (written,) = DeltaTable(root.child("cad_lancamentos").uri).file_uris()
    rows = pq.read_table(written, columns=key).to_pylist()
    keys = [tuple(row[name] for name in key) for row in rows]
    assert len(keys) == base.partition_rows["cad_lancamentos"]["2026-01-31"]
    assert keys == sorted(keys)

    chunks = sorted((base.root / "cad_lancamentos" / "data_base_str=2026-01-31").glob("*.parquet"))
    original = pa.concat_tables(
        [pq.read_table(chunk, columns=key) for chunk in chunks]
    ).to_pylist()
    assert [tuple(row[name] for name in key) for row in original] != keys


def off_the_path(chunk: pa.Table) -> pa.Table:
    """A primeira linha com ``data`` fora do valor do caminho da partição."""
    values = chunk.column("data").to_pylist()
    values[0] = dt.date(2026, 3, 31)
    return chunk.set_column(
        chunk.schema.get_field_index("data"), "data", pa.array(values, pa.date32())
    )


def with_a_null(chunk: pa.Table) -> pa.Table:
    """A primeira linha com ``sistema`` nulo: ``NOT NULL`` no modelo, anulável nos arquivos."""
    values = chunk.column("sistema").to_pylist()
    values[0] = None
    return chunk.set_column(
        chunk.schema.get_field_index("sistema"), "sistema", pa.array(values, pa.int32())
    )


def above_the_length(chunk: pa.Table) -> pa.Table:
    """A primeira linha com ``to`` de três bytes, coluna ``String(2)`` no modelo."""
    values = chunk.column("to").to_pylist()
    values[0] = "ABC"
    return chunk.set_column(
        chunk.schema.get_field_index("to"), "to", pa.array(values, pa.string())
    )


def source_with_defect(
    local_location, base: source.SourceBase, table: str, alter: Callable[[pa.Table], pa.Table]
) -> migrate.Location:
    """Uma origem nova só com a partição 2026-02-28 da tabela, o primeiro chunk alterado por
    ``alter``."""
    folder_name = f"{source.PARTITIONS[table].column}=2026-02-28"
    new_root = Path(local_location.child(f"origem-{uuid.uuid4().hex[:8]}"))
    destination = new_root / table / folder_name
    shutil.copytree(base.root / table / folder_name, destination)
    first = destination / "chunk_0.parquet"
    altered = alter(pq.read_table(first))
    pq.write_table(
        altered, first, version="1.0", use_dictionary=False, use_deprecated_int96_timestamps=True
    )
    return migrate.open_location(str(new_root))


@pytest.mark.parametrize(
    ("table", "alter", "fragment"),
    [
        ("cad_operacoes", off_the_path, "1 linhas com data diferente de 2026-02-28"),
        ("cad_contratos", with_a_null, "1 nulos na coluna NOT NULL sistema"),
        ("cad_contratos", above_the_length, "1 textos acima de String(2) em to"),
    ],
)
@pytest.mark.parametrize("mode", ["register", "rewrite"])
def test_a_partition_off_the_contract_is_refused_without_commit(
    base: source.SourceBase,
    con,
    local_location,
    table: str,
    alter: Callable,
    fragment: str,
    mode: str,
) -> None:
    """Cada defeito é ``ContractError`` com a tabela, a partição e a coluna, antes de qualquer
    gravação: a versão
    fica em 0 e a pasta da tabela não tem arquivo Parquet, nos dois modos."""
    origin = source_with_defect(local_location, base, table, alter)
    root = migrate.open_location(local_location.child(f"delta-{uuid.uuid4().hex[:8]}"))
    with pytest.raises(ContractError, match=re.escape(fragment)) as refusal:
        migrate.initial_load(con, TABLES[table], origin, root, settings(mode=mode))
    assert str(refusal.value).startswith(f"{table} partição 2026-02-28: ")
    uri = root.child(table).uri
    assert DeltaTable(uri).version() == 0
    assert list(Path(uri).rglob("*.parquet")) == []


def test_load_report_matches_and_detects_a_deleted_row(
    base: source.SourceBase, origin: migrate.Location, con, root: migrate.Location
) -> None:
    """O relatório confere contagem e somas por partição, ``None`` numa tabela sem partição, e
    uma linha apagada
    do Delta aparece como diferença na partição dela."""
    for name in ("cad_aliquotas", "cad_operacoes"):
        loaded, skipped = migrate.initial_load(con, TABLES[name], origin, root, settings())
        report = migrate.load_report(con, TABLES[name], origin, root, loaded, skipped)
        assert (
            report.matches
            and report.skipped == ()
            and loaded_values(report.loaded) == loaded_values(loaded)
        ), name

    aliquotas = migrate.load_report(con, TABLES["cad_aliquotas"], origin, root, [], [])
    assert [partition.value for partition in aliquotas.partitions] == [None]
    assert aliquotas.partitions[0].source_rows == base.rows["cad_aliquotas"]
    assert aliquotas.partitions[0].source_sums.keys() == {"fator"}
    assert aliquotas.conversions == (
        "id: int32 -> int64",
        "id_conta_origem: int32 -> int64",
        "id_conta_destino: int32 -> int64",
    )

    uri = root.child("cad_operacoes").uri
    first = con.execute(
        f"SELECT min(id_operacao) FROM delta_scan('{uri}') WHERE data_str = '2026-03-31'"
    ).fetchone()[0]
    DeltaTable(uri).delete(f"data_str = '2026-03-31' AND id_operacao = {first}")
    report = migrate.load_report(con, TABLES["cad_operacoes"], origin, root, [], [])
    assert not report.matches
    differing = [partition for partition in report.partitions if not partition.matches]
    assert [partition.value for partition in differing] == ["2026-03-31"]
    assert differing[0].delta_rows == differing[0].source_rows - 1


def test_main_migrates_the_whole_base(
    base: source.SourceBase, local_location, capsys: pytest.CaptureFixture
) -> None:
    """A linha de comando sobre a base inteira: as tabelas sem partição antes das particionadas,
    o relatório em JSON com as 12 tabelas iguais e o que ficou fora do modelo, saída 0; a
    segunda execução não grava nada."""
    root = local_location.child(f"delta-{uuid.uuid4().hex[:8]}")
    report_path = Path(local_location.child("relatorio-migracao.json"))
    argv = [
        "--metadata",
        "client_model:Base.metadata",
        "--source",
        str(base.root),
        "--root",
        root,
        "--report",
        str(report_path),
    ]
    assert migrate.main(argv) == 0
    out = capsys.readouterr().out
    assert out.startswith("DuckDB ")
    assert "fora do modelo: alembic_version, meta_update_status, schema.json" in out
    assert "12 tabelas conferidas, contagens e somas iguais" in out

    document = json.loads(report_path.read_text())
    assert [table["table"] for table in document["tables"]][-4:] == [
        "cad_operacoes",
        "rel_contrato_operacao",
        "cad_contratos",
        "cad_lancamentos",
    ]
    assert len(document["tables"]) == 12 and all(table["matches"] for table in document["tables"])
    assert document["outside_model"] == OUTSIDE_MODEL
    rows = {
        table["table"]: sum(partition["source_rows"] for partition in table["partitions"])
        for table in document["tables"]
    }
    assert rows == {name: base.rows[name] for name in rows}
    assert all(table["loaded"] for table in document["tables"])

    assert migrate.main(argv) == 0
    document = json.loads(report_path.read_text())
    assert all(table["loaded"] == [] for table in document["tables"])


def test_main_refuses_a_model_with_violations(
    base: source.SourceBase, local_location, capsys: pytest.CaptureFixture
) -> None:
    """O modelo de referência viola o contrato: saída 2 com a lista, sem ler a origem."""
    argv = [
        "--metadata",
        "reference_model.model_db_projetado:Base.metadata",
        "--source",
        str(base.root),
        "--root",
        local_location.child("nunca-gravada"),
    ]
    assert migrate.main(argv) == 2
    assert "modelo fora do contrato" in capsys.readouterr().err
