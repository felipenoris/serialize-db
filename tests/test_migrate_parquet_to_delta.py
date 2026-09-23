"""``scripts/migrate_parquet_to_delta.py``: a migração adiantada sobre a base fictícia.

Os testes escrevem sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``): a base de
``tests/source_db_projetado.py`` numa pasta da sessão e as tabelas Delta em outras, uma raiz por
teste. Eles conferem a descoberta das partições e do que fica fora do modelo; a consulta que leva a
partição ao contrato, com ``to`` entre aspas; a carga de cada partição uma vez só, a retomada depois
de uma interrupção e o filtro de partições; os dois modos com o mesmo relatório e os tipos do
contrato nos arquivos gravados; a ordem da ``sort_key``; as recusas sem commit (valor da coluna de
origem fora do caminho, nulo em coluna ``NOT NULL``, texto acima de ``String(n)``), nos dois modos;
as estatísticas registradas, de inteiro, data, ``Double`` e texto; a coluna ``Double`` com ``NaN``
ou infinito sem mínimo e máximo na partição dela, nos dois modos, com o relatório que soma só os
finitos; o relatório que acusa uma linha apagada; a linha de comando sobre a base inteira, duas
vezes, com o ambiente no relatório; e a medição das variantes, cada uma num processo novo, com o
pico do processo filho abaixo do processo do teste, também na segunda execução do mesmo comando e
com a variante que falha registrada. A extensão ``delta`` do DuckDB precisa estar na pasta de
extensões (``SERIALIZE_DB_DUCKDB_EXTENSIONS``, senão ``.duckdb/`` na raiz do repositório).
"""

from __future__ import annotations

import datetime as dt
import json
import re
import shutil
import uuid
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
from deltalake import DeltaTable

import migrate_parquet_to_delta as migrate
import source_db_projetado as source
from client_model import Base
from conftest import LocalLocation
from serialize_db import schema
from serialize_db.errors import ContractError

pytestmark = pytest.mark.local

TABLES = Base.metadata.tables
OUTSIDE_MODEL = ["alembic_version", "meta_update_status", "schema.json"]
PARTITION_VALUES = list(source.PARTITION_VALUES)


def settings(mode: str = "register", partitions: tuple[str, ...] = ()) -> migrate.Settings:
    """As configurações de uma execução local, sem S3, com as linhas na ordem da ``sort_key``."""
    return migrate.Settings(mode=mode, sort=True, partitions=partitions, storage_options={})


def unique_child(local_location: LocalLocation, prefix: str) -> str:
    """Uma pasta nova sob a pasta da sessão: ``prefix``, um hífen e oito dígitos hexadecimais."""
    return local_location.child(f"{prefix}-{uuid.uuid4().hex[:8]}")


@pytest.fixture(scope="module")
def base(local_location: LocalLocation) -> source.SourceBase:
    """A base fictícia gravada uma vez por módulo sob a pasta da sessão."""
    return source.write_source(Path(local_location.child("migracao-origem")))


@pytest.fixture(scope="module")
def origin(base: source.SourceBase) -> migrate.Location:
    """A raiz da base fictícia, aberta como o script abre a origem."""
    return migrate.open_location(str(base.root))


@pytest.fixture(scope="module")
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    """A conexão DuckDB do script, sem S3, compartilhada pelo módulo."""
    connection = migrate.connect_duckdb(uses_s3=False, region=None)
    yield connection
    connection.close()


@pytest.fixture
def root(local_location: LocalLocation) -> migrate.Location:
    """Uma raiz Delta nova por teste."""
    return migrate.open_location(unique_child(local_location, "delta"))


def loaded_values(loaded: list[migrate.PartitionLoad]) -> list[str | None]:
    """Os valores de partição das cargas, na ordem em que foram gravadas."""
    return [load.value for load in loaded]


def key_values(row: dict[str, object], key: list[str]) -> tuple[object, ...]:
    """Os valores das colunas de ``key`` numa linha, na ordem de ``key``."""
    return tuple(row[name] for name in key)


def physical_types(path: str) -> dict[str, str]:
    """O tipo físico de cada coluna do arquivo Parquet."""
    parquet_schema = pq.ParquetFile(path).schema
    return {
        parquet_schema.column(index).name: parquet_schema.column(index).physical_type
        for index in range(len(parquet_schema))
    }


def test_discover_partitions_and_the_entries_outside_the_model(origin: migrate.Location) -> None:
    """As quatro tabelas particionadas dão os valores do caminho, as oito sem partição dão
    ``None``, e o que a raiz tem fora do modelo vai para a lista."""
    for name, table in TABLES.items():
        options = schema.table_options(table)
        found, skipped = migrate.discover_partitions(origin.child(name), options)
        assert skipped == [], name
        if options.partition_by is None:
            assert list(found) == [None], name
            assert found[None].uri == f"{origin.uri}/{name}", name
            continue
        assert list(found) == PARTITION_VALUES, name
        expected_uri = f"{origin.uri}/{name}/{options.partition_by}=2026-02-28"
        assert found["2026-02-28"].uri == expected_uri, name
    assert migrate.entries_outside_the_model(origin, Base.metadata) == OUTSIDE_MODEL


def test_discover_partitions_skips_an_entry_outside_the_pattern(
    base: source.SourceBase, origin: migrate.Location
) -> None:
    """O que a pasta da tabela tem fora do padrão ``<coluna>=<valor>`` vai para a lista."""
    # A base é do módulo inteiro: o arquivo acrescentado sai mesmo se a asserção falhar.
    stray = base.root / "cad_operacoes" / "notas.txt"
    stray.write_text("fora do padrão")
    try:
        found, skipped = migrate.discover_partitions(
            origin.child("cad_operacoes"), schema.table_options(TABLES["cad_operacoes"])
        )
    finally:
        stray.unlink()
    assert list(found) == PARTITION_VALUES
    assert skipped == ["notas.txt"]


def test_discover_partitions_accepts_a_value_that_is_not_a_date(
    base: source.SourceBase, origin: migrate.Location
) -> None:
    """A partição é texto: uma pasta cujo valor não é data também é achada."""
    # A base é do módulo inteiro: a pasta acrescentada sai mesmo se a asserção falhar.
    quarter = base.root / "cad_operacoes" / "data_str=2026-Q1"
    quarter.mkdir()
    try:
        found, skipped = migrate.discover_partitions(
            origin.child("cad_operacoes"), schema.table_options(TABLES["cad_operacoes"])
        )
    finally:
        quarter.rmdir()
    assert "2026-Q1" in found
    assert skipped == []


def test_partition_query_casts_to_the_contract(
    base: source.SourceBase, origin: migrate.Location, con: duckdb.DuckDBPyConnection
) -> None:
    """O esquema Arrow do ``SELECT`` é o do contrato: as chaves em ``int64``, o ``timestamp`` em
    microssegundos, a coluna de partição no fim com o valor do caminho; a consulta de
    ``cad_contratos`` roda com ``to`` entre aspas."""
    table = TABLES["cad_lancamentos"]
    folder = origin.child("cad_lancamentos").child("data_base_str=2026-01-31")
    query = migrate.partition_query(folder, table, "2026-01-31")
    query_schema = con.execute(query).to_arrow_reader().schema
    contract_schema = schema.arrow_schema(table)
    assert query_schema.names == contract_schema.names
    query_types = [str(field.type) for field in query_schema]
    contract_types = [str(field.type) for field in contract_schema]
    assert query_types == contract_types
    assert str(query_schema.field("id_lancamento").type) == "int64"
    assert str(query_schema.field("timestamp").type) == "timestamp[us]"
    path_values = con.execute(f"SELECT DISTINCT data_base_str FROM ({query})").fetchall()
    assert path_values == [("2026-01-31",)]

    contracts_query = migrate.partition_query(
        origin.child("cad_contratos").child("data_str=2026-02-28"),
        TABLES["cad_contratos"],
        "2026-02-28",
    )
    counted = con.execute(f"SELECT count(*) FROM ({contracts_query})").fetchone()[0]
    assert counted == base.partition_rows["cad_contratos"]["2026-02-28"]


def test_initial_load_loads_every_partition_once(
    base: source.SourceBase,
    origin: migrate.Location,
    con: duckdb.DuckDBPyConnection,
    root: migrate.Location,
) -> None:
    """A primeira passagem grava toda partição num commit cada, com o nome, a partição e as
    retenções no log; a segunda não grava nada; uma tabela sem partição carrega uma vez, com o
    valor ``None``."""
    table = TABLES["cad_contratos"]
    loaded, skipped = migrate.initial_load(con, table, origin, root, settings())
    assert loaded_values(loaded) == PARTITION_VALUES
    assert skipped == []
    assert [load.rows for load in loaded] == [
        base.partition_rows["cad_contratos"][value] for value in PARTITION_VALUES
    ]
    assert all(load.seconds > 0 for load in loaded)
    assert all(load.peak_rss_mb > 0 for load in loaded)
    delta = DeltaTable(root.child("cad_contratos").uri)
    metadata = delta.metadata()
    assert delta.version() == len(PARTITION_VALUES)
    assert metadata.name == "cad_contratos"
    assert metadata.partition_columns == ["data_str"]
    assert metadata.description == table.comment
    assert metadata.configuration == migrate.RETENTION

    # A segunda passagem não grava nada.
    loaded, skipped = migrate.initial_load(con, table, origin, root, settings())
    assert loaded == []
    assert skipped == []
    assert DeltaTable(root.child("cad_contratos").uri).version() == len(PARTITION_VALUES)

    # Uma tabela sem partição carrega uma vez, com o valor None.
    accounts = TABLES["cad_contas"]
    loaded, _ = migrate.initial_load(con, accounts, origin, root, settings())
    assert [(load.value, load.rows) for load in loaded] == [(None, base.rows["cad_contas"])]
    loaded, _ = migrate.initial_load(con, accounts, origin, root, settings())
    assert loaded == []
    assert DeltaTable(root.child("cad_contas").uri).version() == 1


def test_partition_filter_loads_only_the_listed_values(
    origin: migrate.Location, con: duckdb.DuckDBPyConnection, root: migrate.Location
) -> None:
    """``--partitions`` filtra as partições encontradas, deixa as tabelas sem partição de fora, e
    a passagem seguinte sem o filtro carrega só o que falta."""
    only = settings(partitions=("2026-02-28",))
    loaded, _ = migrate.initial_load(con, TABLES["cad_operacoes"], origin, root, only)
    assert loaded_values(loaded) == ["2026-02-28"]
    loaded, _ = migrate.initial_load(con, TABLES["cad_contas"], origin, root, only)
    assert loaded == []

    loaded, _ = migrate.initial_load(con, TABLES["cad_operacoes"], origin, root, settings())
    assert loaded_values(loaded) == ["2026-01-31", "2026-03-31", "2026-06-30"]


def test_interrupted_load_resumes(
    origin: migrate.Location,
    con: duckdb.DuckDBPyConnection,
    root: migrate.Location,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Uma exceção na terceira partição deixa duas no log; a chamada seguinte carrega só as
    duas restantes."""
    table = TABLES["rel_contrato_operacao"]
    original = migrate.register_partition
    attempts: list[str | None] = []

    def failing_on_the_third(*arguments: object) -> int:
        """O ``register_partition`` do script, com uma exceção na terceira partição."""
        # Os argumentos de register_partition; o quinto é o valor da partição.
        attempts.append(arguments[4])
        if len(attempts) == 3:
            raise RuntimeError("interrompida")
        return original(*arguments)

    monkeypatch.setattr(migrate, "register_partition", failing_on_the_third)
    with pytest.raises(RuntimeError, match="interrompida"):
        migrate.initial_load(con, table, origin, root, settings())
    uri = root.child("rel_contrato_operacao").uri
    assert attempts == PARTITION_VALUES[:3]
    assert DeltaTable(uri).version() == 2

    monkeypatch.undo()
    loaded, _ = migrate.initial_load(con, table, origin, root, settings())
    assert loaded_values(loaded) == PARTITION_VALUES[2:]
    assert DeltaTable(uri).version() == 4


def test_both_modes_give_the_same_report_and_the_contract_types(
    base: source.SourceBase,
    origin: migrate.Location,
    con: duckdb.DuckDBPyConnection,
    local_location: LocalLocation,
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
    assert len(written) == len(PARTITION_VALUES)
    assert all("/carga_inicial_" in path for path in written)
    types = physical_types(written[0])
    assert types["id_lancamento"] == "INT64"
    assert types["timestamp"] == "INT64"
    assert "data_base_str" not in types


def test_rows_are_written_in_sort_key_order(
    base: source.SourceBase,
    origin: migrate.Location,
    con: duckdb.DuckDBPyConnection,
    root: migrate.Location,
) -> None:
    """As linhas da partição saem na ordem da ``sort_key`` do modelo, que não é a da origem."""
    table = TABLES["cad_lancamentos"]
    sort_columns = list(schema.table_options(table).sort_key)
    assert sort_columns == ["data_base", "id_mensuracao", "id_veiculo", "id_conta"]
    migrate.initial_load(con, table, origin, root, settings(partitions=("2026-01-31",)))

    (written,) = DeltaTable(root.child("cad_lancamentos").uri).file_uris()
    rows = pq.read_table(written, columns=sort_columns).to_pylist()
    written_keys = [key_values(row, sort_columns) for row in rows]
    assert len(written_keys) == base.partition_rows["cad_lancamentos"]["2026-01-31"]
    assert written_keys == sorted(written_keys)

    partition_folder = base.root / "cad_lancamentos" / "data_base_str=2026-01-31"
    chunks = sorted(partition_folder.glob("*.parquet"))
    source_rows = pa.concat_tables(
        [pq.read_table(chunk, columns=sort_columns) for chunk in chunks]
    ).to_pylist()
    assert [key_values(row, sort_columns) for row in source_rows] != written_keys


def test_registered_stats_carry_the_four_exact_types(
    base: source.SourceBase,
    origin: migrate.Location,
    con: duckdb.DuckDBPyConnection,
    root: migrate.Location,
) -> None:
    """A ação registrada leva mínimo e máximo de inteiro, data, ``Double`` e texto, e deixa o
    ``timestamp`` de fora; os valores batem com os do arquivo."""
    # decimal e timestamp ficam sem extremos: o log os guarda como número JSON, e um máximo abaixo
    # do valor real poda o arquivo que tem a linha (plan/POC.md, 2026-09-22).
    assert migrate.stat_converter(pa.decimal128(18, 2)) is None
    assert migrate.stat_converter(pa.timestamp("us")) is None

    table = TABLES["cad_lancamentos"]
    migrate.initial_load(con, table, origin, root, settings(partitions=("2026-01-31",)))

    uri = root.child("cad_lancamentos").uri
    log = Path(uri, "_delta_log", "00000000000000000001.json").read_text()
    actions = [json.loads(line) for line in log.splitlines()]
    (add,) = [action["add"] for action in actions if "add" in action]
    stats = json.loads(add["stats"])
    partition_by = schema.table_options(table).partition_by

    # As colunas com extremos saem do contrato: as que stat_converter transcreve, menos a de
    # partição, que no Delta fica só no caminho.
    expected = set()
    for field in schema.arrow_schema(table):
        has_bounds = migrate.stat_converter(field.type) is not None
        if has_bounds and field.name != partition_by:
            expected.add(field.name)
    # Uma coluna só de nulos não tem mínimo nem máximo no RETURN_STATS, e fica de fora dos dois.
    all_null = {name for name, nulls in stats["nullCount"].items() if nulls == stats["numRecords"]}
    assert all_null == {"meta"}
    assert set(stats["minValues"]) == expected - all_null
    assert stats["minValues"].keys() == stats["maxValues"].keys()
    assert "timestamp" in stats["nullCount"]
    assert "timestamp" not in stats["minValues"]
    assert stats["numRecords"] == base.partition_rows["cad_lancamentos"]["2026-01-31"]

    # Os extremos registrados são os do arquivo, no tipo que o log guarda.
    (written,) = DeltaTable(uri).file_uris()
    rows = pq.read_table(written, columns=["id_lancamento", "valor", "data", "area"])
    assert stats["maxValues"]["id_lancamento"] == pc.max(rows.column("id_lancamento")).as_py()
    assert stats["minValues"]["valor"] == pc.min(rows.column("valor")).as_py()
    assert stats["maxValues"]["data"] == str(pc.max(rows.column("data")).as_py())
    assert stats["maxValues"]["area"] == pc.max(rows.column("area")).as_py()
    assert isinstance(stats["maxValues"]["valor"], float)
    assert isinstance(stats["maxValues"]["id_lancamento"], int)


def replace_first_value(chunk: pa.Table, column: str, value: object) -> pa.Table:
    """A tabela com ``value`` na primeira linha de ``column``, no campo do arquivo: mesmo tipo e
    mesma nulidade."""
    values = chunk.column(column).to_pylist()
    values[0] = value
    field = chunk.schema.field(column)
    index = chunk.schema.get_field_index(column)
    return chunk.set_column(index, field, pa.array(values, field.type))


def rewrite_first_chunk(folder: Path, column: str, value: object) -> None:
    """Regrava ``chunk_0.parquet`` de ``folder`` com ``value`` na primeira linha de ``column``, no
    layout da origem."""
    path = folder / "chunk_0.parquet"
    altered = replace_first_value(pq.read_table(path), column, value)
    pq.write_table(
        altered, path, version="1.0", use_dictionary=False, use_deprecated_int96_timestamps=True
    )


def source_with_defect(
    local_location: LocalLocation,
    base: source.SourceBase,
    table: str,
    column: str,
    value: object,
) -> migrate.Location:
    """Uma origem nova só com a partição 2026-02-28 da tabela, com ``value`` na primeira linha de
    ``column`` do primeiro chunk."""
    folder_name = f"{source.PARTITIONS[table].column}=2026-02-28"
    new_root = Path(unique_child(local_location, "origem"))
    destination = new_root / table / folder_name
    shutil.copytree(base.root / table / folder_name, destination)
    rewrite_first_chunk(destination, column, value)
    return migrate.open_location(str(new_root))


@pytest.mark.parametrize(
    ("table", "column", "value", "fragment"),
    [
        pytest.param(
            "cad_operacoes",
            "data",
            dt.date(2026, 3, 31),
            "1 linhas com data diferente de 2026-02-28",
            id="off_the_path",
        ),
        pytest.param(
            "cad_contratos", "sistema", None, "1 nulos na coluna NOT NULL sistema", id="with_a_null"
        ),
        pytest.param(
            "cad_contratos", "to", "ABC", "1 textos acima de String(2) em to", id="above_the_length"
        ),
    ],
)
@pytest.mark.parametrize("mode", ["register", "rewrite"])
def test_a_partition_off_the_contract_is_refused_without_commit(
    base: source.SourceBase,
    con: duckdb.DuckDBPyConnection,
    local_location: LocalLocation,
    root: migrate.Location,
    table: str,
    column: str,
    value: object,
    fragment: str,
    mode: str,
) -> None:
    """Cada defeito é ``ContractError`` com a tabela, a partição e a coluna, antes de qualquer
    gravação: a versão fica em 0 e a pasta da tabela não tem arquivo Parquet, nos dois modos.

    Os defeitos, na primeira linha: ``data`` fora do valor do caminho da partição; ``sistema``
    nulo, ``NOT NULL`` no modelo e anulável nos arquivos; ``to`` de três bytes, coluna
    ``String(2)`` no modelo.
    """
    origin = source_with_defect(local_location, base, table, column, value)
    with pytest.raises(ContractError, match=re.escape(fragment)) as refusal:
        migrate.initial_load(con, TABLES[table], origin, root, settings(mode=mode))
    assert str(refusal.value).startswith(f"{table} partição 2026-02-28: ")
    uri = root.child(table).uri
    assert DeltaTable(uri).version() == 0
    assert list(Path(uri).rglob("*.parquet")) == []


def source_with_nonfinite(
    local_location: LocalLocation, base: source.SourceBase
) -> migrate.Location:
    """Uma origem nova com ``cad_lancamentos`` inteira, um ``NaN`` no primeiro chunk da partição
    2026-02-28 e um infinito no da 2026-03-31."""
    new_root = Path(unique_child(local_location, "origem"))
    shutil.copytree(base.root / "cad_lancamentos", new_root / "cad_lancamentos")
    partition_column = source.PARTITIONS["cad_lancamentos"].column
    nonfinite_values = {"2026-02-28": float("nan"), "2026-03-31": float("inf")}
    for value, number in nonfinite_values.items():
        folder = new_root / "cad_lancamentos" / f"{partition_column}={value}"
        rewrite_first_chunk(folder, "valor", number)
    return migrate.open_location(str(new_root))


def has_min_max(path: str, column: str) -> bool:
    """Verdadeiro quando algum grupo de linhas do arquivo tem mínimo e máximo de ``column``."""
    parquet_file = pq.ParquetFile(path)
    index = parquet_file.schema_arrow.get_field_index(column)
    for row_group_index in range(parquet_file.metadata.num_row_groups):
        statistics = parquet_file.metadata.row_group(row_group_index).column(index).statistics
        if statistics is not None and statistics.has_min_max:
            return True
    return False


@pytest.mark.parametrize("mode", ["register", "rewrite"])
def test_nonfinite_double_leaves_min_max_out_of_its_partition(
    base: source.SourceBase,
    con: duckdb.DuckDBPyConnection,
    local_location: LocalLocation,
    root: migrate.Location,
    mode: str,
) -> None:
    """As partições com ``NaN`` ou infinito em ``valor`` gravam a coluna sem mínimo e máximo no
    log, e as outras ficam com os dois; o relatório soma só os finitos e conta os não finitos nos
    dois lados; e o ``delta_scan`` devolve as duas linhas num filtro acima de todo número finito,
    porque o DuckDB ordena o ``NaN`` e o infinito acima deles (issue #59)."""
    origin = source_with_nonfinite(local_location, base)
    table = TABLES["cad_lancamentos"]
    loaded, skipped = migrate.initial_load(con, table, origin, root, settings(mode=mode))
    nonfinite_partitions = {"2026-02-28", "2026-03-31"}
    for load in loaded:
        expected = ("valor",) if load.value in nonfinite_partitions else ()
        assert load.nonfinite_columns == expected, load.value

    # O log: as partições dos não finitos sem o mínimo e o máximo de valor, as outras com eles.
    uri = root.child("cad_lancamentos").uri
    partition_by = schema.table_options(table).partition_by
    add_actions = pa.table(DeltaTable(uri).get_add_actions(flatten=True)).to_pylist()
    for action in add_actions:
        value = action[f"partition.{partition_by}"]
        has_bounds = value not in nonfinite_partitions
        assert (action["min.valor"] is not None) == has_bounds, value
        assert (action["max.valor"] is not None) == has_bounds, value

    # O rodapé: o arquivo da partição do NaN sai sem mínimo e máximo de valor nos dois modos (o
    # COPY do DuckDB já omite os dois no grupo com NaN), e o de uma partição finita, com eles.
    files = DeltaTable(uri).file_uris()
    nan_file = next(path for path in files if f"{partition_by}=2026-02-28" in path)
    finite_file = next(path for path in files if f"{partition_by}=2026-01-31" in path)
    assert not has_min_max(nan_file, "valor")
    assert has_min_max(finite_file, "valor")

    # O relatório não falha no CAST para DECIMAL e confere os não finitos dos dois lados.
    report = migrate.load_report(con, table, origin, root, loaded, skipped)
    assert report.matches
    for partition in report.partitions:
        count = 1 if partition.value in nonfinite_partitions else 0
        assert partition.source_nonfinite == {"valor": count}, partition.value
        assert partition.delta_nonfinite == {"valor": count}, partition.value

    query = f"SELECT count(*) FROM delta_scan('{uri}') WHERE valor > 1e300"
    assert con.execute(query).fetchone()[0] == 2


def test_load_report_matches_and_detects_a_deleted_row(
    base: source.SourceBase,
    origin: migrate.Location,
    con: duckdb.DuckDBPyConnection,
    root: migrate.Location,
) -> None:
    """O relatório confere contagem e somas por partição, ``None`` numa tabela sem partição, e uma
    linha apagada do Delta aparece como diferença na partição dela."""
    for name in ("cad_aliquotas", "cad_operacoes"):
        loaded, skipped = migrate.initial_load(con, TABLES[name], origin, root, settings())
        report = migrate.load_report(con, TABLES[name], origin, root, loaded, skipped)
        assert report.matches, name
        assert report.skipped == (), name
        assert loaded_values(report.loaded) == loaded_values(loaded), name

    rates_report = migrate.load_report(con, TABLES["cad_aliquotas"], origin, root, [], [])
    assert [partition.value for partition in rates_report.partitions] == [None]
    assert rates_report.partitions[0].source_rows == base.rows["cad_aliquotas"]
    assert rates_report.partitions[0].source_sums.keys() == {"fator"}
    assert rates_report.conversions == (
        "id: int32 -> int64",
        "id_conta_origem: int32 -> int64",
        "id_conta_destino: int32 -> int64",
    )

    uri = root.child("cad_operacoes").uri
    deleted_id = con.execute(
        f"SELECT min(id_operacao) FROM delta_scan('{uri}') WHERE data_str = '2026-03-31'"
    ).fetchone()[0]
    DeltaTable(uri).delete(f"data_str = '2026-03-31' AND id_operacao = {deleted_id}")
    report = migrate.load_report(con, TABLES["cad_operacoes"], origin, root, [], [])
    assert not report.matches
    differing = [partition for partition in report.partitions if not partition.matches]
    assert [partition.value for partition in differing] == ["2026-03-31"]
    assert differing[0].delta_rows == differing[0].source_rows - 1


def test_main_migrates_the_whole_base(
    base: source.SourceBase, local_location: LocalLocation, capsys: pytest.CaptureFixture
) -> None:
    """A linha de comando sobre a base inteira: as tabelas sem partição antes das particionadas,
    o relatório em JSON com as 12 tabelas iguais e o que ficou fora do modelo, saída 0; a
    segunda execução não grava nada."""
    root = unique_child(local_location, "delta")
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
        "--no-measure",
    ]
    assert migrate.main(argv) == 0
    out = capsys.readouterr().out
    assert out.startswith("DuckDB ")
    assert "fora do modelo: alembic_version, meta_update_status, schema.json" in out
    assert "12 tabelas conferidas, contagens e somas iguais" in out

    document = json.loads(report_path.read_text())
    environment = document["environment"]
    assert environment["cpus"] > 0 and environment["memory_total_mb"] > 0
    assert {"threads", "memory_limit"} <= set(environment["duckdb_settings"])
    assert environment["arguments"]["measure"] is False
    assert all(table["measurements"] == [] for table in document["tables"])
    table_order = [table["table"] for table in document["tables"]]
    assert table_order[-4:] == [
        "cad_operacoes",
        "rel_contrato_operacao",
        "cad_contratos",
        "cad_lancamentos",
    ]
    assert len(document["tables"]) == len(TABLES)
    assert all(table["matches"] for table in document["tables"])
    assert document["outside_model"] == OUTSIDE_MODEL
    rows = {}
    for table in document["tables"]:
        rows[table["table"]] = sum(partition["source_rows"] for partition in table["partitions"])
    assert rows == {name: base.rows[name] for name in TABLES}
    assert all(table["loaded"] for table in document["tables"])

    assert migrate.main(argv) == 0
    document = json.loads(report_path.read_text())
    assert all(table["loaded"] == [] for table in document["tables"])


def test_measurement_runs_every_variant_even_with_the_partition_in_the_log(
    base: source.SourceBase, local_location: LocalLocation, capsys: pytest.CaptureFixture
) -> None:
    """A medição grava a partição pedida nas quatro variantes, cada uma num processo novo, antes
    da carga e de novo quando a partição já está no log, como na segunda execução do mesmo
    comando; a tabela descartável sai da raiz. As outras partições ficam fora do Delta, e o
    relatório as acusa: saída 1."""
    root = unique_child(local_location, "delta")
    report_path = Path(unique_child(local_location, "relatorio-medicao") + ".json")
    value = PARTITION_VALUES[0]
    argv = [
        "--metadata",
        "client_model:Base.metadata",
        "--source",
        str(base.root),
        "--root",
        root,
        "--tables",
        "cad_contratos",
        "--partitions",
        value,
        "--report",
        str(report_path),
    ]
    # O processo do teste passa de 512 MB, uma página tocada a cada 4 KB: um pico medido nele, ou
    # num filho que herdasse o pico dele, passaria disso.
    ballast = bytearray(512 * 2**20)
    ballast[::4096] = b"\x01" * (len(ballast) // 4096)
    for run in range(2):
        assert migrate.main(argv) == 1
        assert f"medição {value} rewrite sem ordem" in capsys.readouterr().out
        table = json.loads(report_path.read_text())["tables"][0]
        assert len(table["loaded"]) == (1 if run == 0 else 0)

        # Cada variante gravou as linhas da partição, uma vez, sem erro.
        partitions = {partition["value"]: partition for partition in table["partitions"]}
        measurements = table["measurements"]
        variants = [(measurement["mode"], measurement["sort"]) for measurement in measurements]
        assert variants == list(migrate.VARIANTS)
        for measurement in measurements:
            assert measurement["error"] is None
            assert measurement["value"] == value
            assert measurement["rows"] == partitions[value]["source_rows"]
            assert 0 < measurement["base_mb"] <= measurement["peak_mb"] < 512
            assert measurement["files"] >= 1 and measurement["bytes"] > 0
        assert not Path(root, "_medicao_cad_contratos").exists()
    del ballast


def test_a_failed_variant_enters_the_measurement_with_its_error(
    origin: migrate.Location, root: migrate.Location
) -> None:
    """A variante que falha no processo filho entra na medição com o erro, e as seguintes rodam;
    a tabela descartável de cada uma sai da pasta. Uma pasta de partição ausente faz o
    ``read_parquet`` falhar nas quatro."""
    table = TABLES["cad_contratos"]
    missing = origin.child(table.name).child("data_str=2099-12-31")
    scratch = root.child("_medicao_cad_contratos")
    measurements = migrate.measure_partition(
        table, missing, scratch, "2099-12-31", settings(), uses_s3=False, region=None
    )
    assert [(measurement.mode, measurement.sort) for measurement in measurements] == list(
        migrate.VARIANTS
    )
    for measurement in measurements:
        assert measurement.error.startswith("IOException: "), measurement.error
        assert measurement.rows is None
    assert not Path(scratch.path).exists() or not any(Path(scratch.path).iterdir())


def test_main_refuses_a_model_with_violations(
    base: source.SourceBase, local_location: LocalLocation, capsys: pytest.CaptureFixture
) -> None:
    """O modelo de referência viola o contrato: saída 2 com a lista, sem ler a origem."""
    never_written = local_location.child("nunca-gravada")
    argv = [
        "--metadata",
        "reference_model.model_db_projetado:Base.metadata",
        "--source",
        str(base.root),
        "--root",
        never_written,
    ]
    assert migrate.main(argv) == 2
    assert "modelo fora do contrato" in capsys.readouterr().err
    assert not Path(never_written).exists()
