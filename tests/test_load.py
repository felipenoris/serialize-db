"""``serialize_db.load``: a carga inicial da base Parquet de origem sobre a base fictícia.

Os testes escrevem sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``): a base de
``tests/source_db_projetado.py`` numa pasta da sessão e as tabelas Delta em outras, uma raiz por
teste, com o motor DuckDB da carga na pasta do teste. Eles conferem a descoberta das partições e do
que fica fora do padrão e fora do modelo; a consulta que leva a partição ao contrato, com ``to``
entre aspas; a carga de cada partição uma vez só, a retomada depois de uma interrupção e o filtro
de partições; os tipos do contrato nos arquivos gravados; a ordem da ``sort_key``; as recusas sem
commit (valor da coluna de origem fora do caminho, texto acima de ``String(n)``, um nulo em cada
uma das sete colunas ``NOT NULL`` de ``cad_contratos`` declaradas anuláveis nos arquivos); a coluna
``Double`` com ``NaN`` ou infinito sem mínimo e máximo na partição dela, com o relatório que soma só
os finitos; o relatório que acusa uma linha apagada; a auditoria de chave estrangeira que registra
o órfão sem barrar a carga; e ``serialize-db load`` sobre a base inteira, duas vezes.
"""

from __future__ import annotations

import datetime
import re
import shutil
import tempfile
import uuid
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import source_db_projetado as source
from client_model import Base
from conftest import LocalLocation
from serialize_db import cli, delta, load
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.errors import ContractError
from serialize_db.execution import Database
from serialize_db.schema import arrow_schema, table_options

pytestmark = pytest.mark.local

TABLES = Base.metadata.tables
OUTSIDE_MODEL = ("alembic_version", "meta_update_status", "schema.json")
PARTITION_VALUES = list(source.PARTITION_VALUES)


@pytest.fixture(scope="module")
def base(local_location: LocalLocation) -> source.SourceBase:
    """A base fictícia gravada uma vez por módulo sob a pasta da sessão."""
    return source.write_source(Path(local_location.child("carga-origem")))


@pytest.fixture
def folder(local_location: LocalLocation) -> Path:
    """Uma pasta nova por teste sob a raiz da sessão."""
    path = Path(local_location.child(f"carga/{uuid.uuid4().hex[:8]}"))
    path.mkdir(parents=True)
    return path


@pytest.fixture
def db(folder: Path) -> Database:
    """O banco do teste, numa raiz Delta nova, no ambiente ``prd``."""
    return Database(str(folder / "delta"), "prd", Base.metadata)


@pytest.fixture
def config(folder: Path) -> DuckDBConfig:
    """O motor DuckDB da carga com o banco e o transbordo na pasta do teste."""
    return DuckDBConfig(temp_directory=str(folder / "sandbox"))


def origin_of(base: source.SourceBase) -> str:
    """A raiz da base fictícia como a carga a recebe."""
    return str(base.root)


def add_actions(db: Database, name: str) -> list[dict]:
    """As ações ``add`` da versão atual da tabela, achatadas."""
    dt = delta.open_table(db.uri(TABLES[name]), db.storage)
    return pa.table(dt.get_add_actions(flatten=True)).to_pylist()


def physical_types(path: str) -> dict[str, str]:
    """O tipo físico de cada coluna do arquivo Parquet."""
    parquet_schema = pq.ParquetFile(path).schema
    types = {}
    for index in range(len(parquet_schema)):
        column = parquet_schema.column(index)
        types[column.name] = column.physical_type
    return types


def key_values(row: dict[str, object], key: list[str]) -> tuple[object, ...]:
    """Os valores das colunas de ``key`` numa linha, na ordem de ``key``."""
    return tuple(row[name] for name in key)


# ---------------------------------------------------------------- a origem


def test_discover_partitions_and_skipped_entries(base: source.SourceBase, folder: Path) -> None:
    """As quatro tabelas particionadas dão os valores do caminho, as oito sem partição dão
    ``None``; um arquivo solto e uma pasta com valor fora da regra da partição vão para a lista,
    uma pasta com valor que não é data é partição; a raiz tem três entradas fora do modelo; a
    pasta da tabela ausente é ``FileNotFoundError``."""
    origin = origin_of(base)
    partitioned = []
    for name, table in TABLES.items():
        found, skipped = load.discover_partitions(origin, table)
        assert skipped == (), name
        partition_by = table_options(table).partition_by
        if partition_by is None:
            assert found == {None: f"{base.root}/{name}"}, name
            continue
        partitioned.append(name)
        assert list(found) == PARTITION_VALUES, name
        assert found["2026-02-28"] == f"{base.root}/{name}/{partition_by}=2026-02-28", name
    assert len(partitioned) == 4
    assert len(TABLES) == 12

    # A base é do módulo inteiro: o que o teste acrescenta sai mesmo se a asserção falhar.
    operations = base.root / "cad_operacoes"
    stray = operations / "notas.txt"
    off_the_rule = operations / "data_str=2026 Q1"
    quarter = operations / "data_str=2026-Q1"
    stray.write_text("fora do padrão")
    off_the_rule.mkdir()
    quarter.mkdir()
    try:
        found, skipped = load.discover_partitions(origin, TABLES["cad_operacoes"])
    finally:
        stray.unlink()
        off_the_rule.rmdir()
        quarter.rmdir()
    assert list(found) == [*PARTITION_VALUES, "2026-Q1"]
    assert skipped == ("data_str=2026 Q1", "notas.txt")

    # As entradas da origem fora do modelo, e a tabela sem pasta.
    assert load.entries_outside_the_model(origin, Base.metadata) == OUTSIDE_MODEL
    with pytest.raises(FileNotFoundError, match="cad_contas"):
        load.discover_partitions(str(folder / "vazia"), TABLES["cad_contas"])


def test_load_order_puts_unpartitioned_tables_first() -> None:
    """As tabelas sem partição vêm antes das particionadas, cada grupo na ordem dada."""
    ordered = [table.name for table in load.load_order(list(TABLES.values()))]
    assert ordered[-4:] == ["cad_operacoes", "rel_contrato_operacao", "cad_contratos",
                            "cad_lancamentos"]
    assert all(table_options(TABLES[name]).partition_by is None for name in ordered[:-4])


def test_partition_query_casts_to_the_contract(base: source.SourceBase, db: Database,
                                               config: DuckDBConfig) -> None:
    """O esquema Arrow do ``SELECT`` é o do contrato: as chaves em ``int64``, o ``timestamp`` em
    microssegundos, a coluna de partição no fim com o valor do caminho; a consulta de
    ``cad_contratos`` roda com ``to`` entre aspas."""
    table = TABLES["cad_lancamentos"]
    folder_uri = f"{base.root}/cad_lancamentos/data_base_str=2026-01-31"
    query = load.partition_query(folder_uri, table, "2026-01-31")
    contracts_query = load.partition_query(f"{base.root}/cad_contratos/data_str=2026-02-28",
                                           TABLES["cad_contratos"], "2026-02-28")
    with DuckDBEngine(config, "consulta", db.storage) as engine, engine.session() as connection:
        query_schema = connection.execute(query).to_arrow_table().schema
        path_values = connection.execute(f"SELECT DISTINCT data_base_str FROM ({query})").fetchall()
        counted = connection.execute(f"SELECT count(*) FROM ({contracts_query})").fetchone()[0]
    contract_schema = arrow_schema(table)
    assert query_schema.names == contract_schema.names
    query_types = [str(field.type) for field in query_schema]
    contract_types = [str(field.type) for field in contract_schema]
    assert query_types == contract_types
    assert str(query_schema.field("id_lancamento").type) == "int64"
    assert str(query_schema.field("timestamp").type) == "timestamp[us]"
    assert path_values == [("2026-01-31",)]
    assert counted == base.partition_rows["cad_contratos"]["2026-02-28"]


# ---------------------------------------------------------------- a carga


def test_initial_load_loads_every_partition_once(base: source.SourceBase, db: Database,
                                                 config: DuckDBConfig) -> None:
    """A primeira passagem grava toda partição num commit cada, com o nome, a partição, as
    retenções e o ``execution_id`` da carga no log; a segunda não grava nada; uma tabela sem
    partição carrega uma vez, com o valor ``None``; ``partitions`` filtra as partições
    encontradas, deixa as tabelas sem partição de fora, e a passagem seguinte carrega o resto."""
    origin = origin_of(base)
    table = TABLES["cad_contratos"]
    assert load.initial_load(db, table, origin, config=config) == PARTITION_VALUES
    dt = delta.open_table(db.uri(table), db.storage)
    assert dt.version() == len(PARTITION_VALUES)
    metadata = dt.metadata()
    assert metadata.name == "cad_contratos"
    assert metadata.partition_columns == ["data_str"]
    assert metadata.description == table.comment
    assert metadata.configuration == delta.RETENTION
    assert dt.history(limit=1)[0]["serialize_db_execution_id"].startswith("carga-")
    rows_by_value = {action["partition.data_str"]: action["num_records"]
                     for action in add_actions(db, "cad_contratos")}
    assert rows_by_value == base.partition_rows["cad_contratos"]

    # A segunda carga não grava nada.
    assert load.initial_load(db, table, origin, config=config) == []
    assert delta.open_table(db.uri(table), db.storage).version() == len(PARTITION_VALUES)

    # A tabela sem partição carrega uma vez, com o valor None.
    accounts = TABLES["cad_contas"]
    assert load.initial_load(db, accounts, origin, config=config) == [None]
    assert load.initial_load(db, accounts, origin, config=config) == []
    assert delta.open_table(db.uri(accounts), db.storage).version() == 1
    assert add_actions(db, "cad_contas")[0]["num_records"] == base.rows["cad_contas"]

    # partitions filtra as partições e deixa de fora a tabela sem partição; a passagem seguinte
    # carrega o resto.
    operations = TABLES["cad_operacoes"]
    only = ["2026-02-28"]
    assert load.initial_load(db, operations, origin, partitions=only, config=config) == only
    rates = TABLES["cad_aliquotas"]
    assert load.initial_load(db, rates, origin, partitions=only, config=config) == []
    rest = load.initial_load(db, operations, origin, config=config)
    assert rest == ["2026-01-31", "2026-03-31", "2026-06-30"]


def test_interrupted_load_resumes(base: source.SourceBase, db: Database, config: DuckDBConfig,
                                  monkeypatch: pytest.MonkeyPatch) -> None:
    """Uma exceção no registro da terceira partição deixa duas no log; a chamada seguinte carrega
    só as duas restantes."""
    origin = origin_of(base)
    table = TABLES["rel_contrato_operacao"]
    original = delta.register_files
    attempts: list[str | None] = []

    def failing_on_the_third(uri: str, registered: object, files: list, value: str | None,
                             *arguments: object, **options: object) -> int:
        attempts.append(value)
        if len(attempts) == 3:
            raise RuntimeError("interrompida")
        return original(uri, registered, files, value, *arguments, **options)

    monkeypatch.setattr(delta, "register_files", failing_on_the_third)
    with pytest.raises(RuntimeError, match="interrompida"):
        load.initial_load(db, table, origin, config=config)
    assert attempts == PARTITION_VALUES[:3]
    assert delta.open_table(db.uri(table), db.storage).version() == 2

    monkeypatch.undo()
    assert load.initial_load(db, table, origin, config=config) == PARTITION_VALUES[2:]
    assert delta.open_table(db.uri(table), db.storage).version() == 4


def test_keys_are_int64_and_timestamps_are_microseconds(base: source.SourceBase, db: Database,
                                                        config: DuckDBConfig) -> None:
    """O Delta lê ``int64`` e ``timestamp[us]``, e o arquivo gravado tem ``INT64`` onde a origem
    tinha ``INT32`` e ``INT96``, sem a coluna de partição, com o ``execution_id`` no nome."""
    table = TABLES["cad_lancamentos"]
    load.initial_load(db, table, origin_of(base), partitions=["2026-02-28"], config=config)
    dt = delta.open_table(db.uri(table), db.storage)
    delta_schema = pa.schema(dt.schema())
    assert str(delta_schema.field("id_lancamento").type) == "int64"
    assert str(delta_schema.field("timestamp").type) == "timestamp[us]"

    # O arquivo gravado: o execution_id no nome e os tipos físicos contra os da origem.
    (written,) = dt.file_uris()
    assert re.search(r"/data_base_str=2026-02-28/carga-[0-9a-f]{8}_[0-9a-f]{32}\.parquet$",
                     written)
    types = physical_types(written)
    assert types["id_lancamento"] == "INT64"
    assert types["timestamp"] == "INT64"
    assert "data_base_str" not in types
    source_types = physical_types(
        str(base.root / "cad_lancamentos" / "data_base_str=2026-02-28" / "chunk_0.parquet"))
    assert (source_types["id_lancamento"], source_types["timestamp"]) == ("INT32", "INT96")


def test_rows_are_written_in_sort_key_order(base: source.SourceBase, db: Database,
                                            config: DuckDBConfig) -> None:
    """As linhas da partição saem na ordem da ``sort_key`` do modelo, que não é a da origem."""
    table = TABLES["cad_lancamentos"]
    sort_columns = list(table_options(table).sort_key)
    assert sort_columns == ["data_base", "id_mensuracao", "id_veiculo", "id_conta"]
    load.initial_load(db, table, origin_of(base), partitions=["2026-01-31"], config=config)

    (written,) = delta.open_table(db.uri(table), db.storage).file_uris()
    rows = pq.read_table(written, columns=sort_columns).to_pylist()
    written_keys = [key_values(row, sort_columns) for row in rows]
    assert len(written_keys) == base.partition_rows["cad_lancamentos"]["2026-01-31"]
    assert written_keys == sorted(written_keys)

    # A origem tem as mesmas linhas em outra ordem.
    partition_folder = base.root / "cad_lancamentos" / "data_base_str=2026-01-31"
    chunks = sorted(partition_folder.glob("*.parquet"))
    source_rows = pa.concat_tables(
        [pq.read_table(chunk, columns=sort_columns) for chunk in chunks]).to_pylist()
    assert [key_values(row, sort_columns) for row in source_rows] != written_keys


# ---------------------------------------------------------------- as recusas


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
    pq.write_table(altered, path, version="1.0", use_dictionary=False,
                   use_deprecated_int96_timestamps=True)


def source_with_defect(folder: Path, base: source.SourceBase, table: str, column: str,
                       value: object) -> str:
    """Uma origem nova só com a partição 2026-02-28 da tabela, com ``value`` na primeira linha de
    ``column`` do primeiro chunk."""
    folder_name = f"{source.PARTITIONS[table].column}=2026-02-28"
    new_root = folder / "origem"
    destination = new_root / table / folder_name
    shutil.copytree(base.root / table / folder_name, destination)
    rewrite_first_chunk(destination, column, value)
    return str(new_root)


def assert_refused_without_commit(db: Database, config: DuckDBConfig, origin: str, table: str,
                                  fragment: str) -> None:
    """A carga é ``ContractError`` com a tabela, a partição e ``fragment``, antes de qualquer
    gravação: a versão fica em 0 e a pasta da tabela não tem arquivo Parquet."""
    with pytest.raises(ContractError, match=re.escape(fragment)) as refusal:
        load.initial_load(db, TABLES[table], origin, config=config)
    assert str(refusal.value).startswith(f"{table} partição 2026-02-28: ")
    uri = db.uri(TABLES[table])
    assert delta.open_table(uri, db.storage).version() == 0
    assert list(Path(uri).rglob("*.parquet")) == []


@pytest.mark.parametrize(
    ("table", "column", "value", "fragment"),
    [
        pytest.param("cad_operacoes", "data", datetime.date(2026, 3, 31),
                     "1 linhas com data diferente de 2026-02-28", id="off_the_path"),
        pytest.param("cad_contratos", "to", "ABC", "1 textos acima de String(2) em to",
                     id="above_the_length"),
    ],
)
def test_source_value_different_from_the_path_aborts(
    base: source.SourceBase, db: Database, config: DuckDBConfig, folder: Path, table: str,
    column: str, value: object, fragment: str,
) -> None:
    """``data`` fora do valor do caminho da partição, e ``to`` de três bytes numa coluna
    ``String(2)``, abortam a partição sem commit."""
    origin = source_with_defect(folder, base, table, column, value)
    assert_refused_without_commit(db, config, origin, table, fragment)


@pytest.mark.parametrize("column", source.MODEL_NOT_NULL_DECLARED_NULLABLE["cad_contratos"])
def test_null_in_not_null_column_is_refused(base: source.SourceBase, db: Database,
                                            config: DuckDBConfig, folder: Path,
                                            column: str) -> None:
    """Um nulo plantado em cada uma das sete colunas de ``cad_contratos`` que o modelo declara
    ``NOT NULL`` e os arquivos declaram anuláveis é recusado com a coluna e a partição."""
    assert not TABLES["cad_contratos"].c[column].nullable
    origin = source_with_defect(folder, base, "cad_contratos", column, None)
    assert_refused_without_commit(db, config, origin, "cad_contratos",
                                  f"1 nulos na coluna NOT NULL {column}")


# ---------------------------------------------------------------- o Double não finito


def source_with_nonfinite(folder: Path, base: source.SourceBase) -> str:
    """Uma origem nova com ``cad_lancamentos`` inteira, um ``NaN`` no primeiro chunk da partição
    2026-02-28 e um infinito no da 2026-03-31."""
    new_root = folder / "origem"
    shutil.copytree(base.root / "cad_lancamentos", new_root / "cad_lancamentos")
    partition_column = source.PARTITIONS["cad_lancamentos"].column
    nonfinite_values = {"2026-02-28": float("nan"), "2026-03-31": float("inf")}
    for value, number in nonfinite_values.items():
        rewrite_first_chunk(new_root / "cad_lancamentos" / f"{partition_column}={value}",
                            "valor", number)
    return str(new_root)


def has_min_max(path: str, column: str) -> bool:
    """Verdadeiro quando algum grupo de linhas do arquivo tem mínimo e máximo de ``column``."""
    parquet_file = pq.ParquetFile(path)
    index = parquet_file.schema_arrow.get_field_index(column)
    for row_group_index in range(parquet_file.metadata.num_row_groups):
        statistics = parquet_file.metadata.row_group(row_group_index).column(index).statistics
        if statistics is not None and statistics.has_min_max:
            return True
    return False


def test_nonfinite_double_leaves_min_max_out_of_its_partition(
    base: source.SourceBase, db: Database, config: DuckDBConfig, folder: Path
) -> None:
    """As partições com ``NaN`` ou infinito em ``valor`` gravam a coluna sem mínimo e máximo no
    log, e as outras ficam com os dois; o relatório soma só os finitos e conta os não finitos nos
    dois lados; e o ``delta_scan`` devolve as duas linhas num filtro acima de todo número finito,
    porque o DuckDB ordena o ``NaN`` e o infinito acima deles (issue #59)."""
    origin = source_with_nonfinite(folder, base)
    table = TABLES["cad_lancamentos"]
    assert load.initial_load(db, table, origin, config=config) == PARTITION_VALUES
    nonfinite_partitions = {"2026-02-28", "2026-03-31"}

    # O log: as partições dos não finitos sem o mínimo e o máximo de valor, as outras com eles.
    for action in add_actions(db, "cad_lancamentos"):
        value = action["partition.data_base_str"]
        has_bounds = value not in nonfinite_partitions
        assert (action["min.valor"] is not None) == has_bounds, value
        assert (action["max.valor"] is not None) == has_bounds, value

    # O rodapé: o arquivo da partição do NaN sai sem mínimo e máximo de valor (o COPY do DuckDB
    # omite os dois no grupo com NaN), e o de uma partição finita, com eles.
    uri = db.uri(table)
    files = delta.open_table(uri, db.storage).file_uris()
    nan_file = next(path for path in files if "data_base_str=2026-02-28" in path)
    finite_file = next(path for path in files if "data_base_str=2026-01-31" in path)
    assert not has_min_max(nan_file, "valor")
    assert has_min_max(finite_file, "valor")

    # O relatório não falha no CAST para DECIMAL e confere os não finitos dos dois lados.
    report = load.load_report(db, table, origin, config=config)
    assert report.matches
    for partition in report.partitions:
        count = 1 if partition.value in nonfinite_partitions else 0
        assert partition.source_nonfinite == {"valor": count}, partition.value
        assert partition.delta_nonfinite == {"valor": count}, partition.value

    # O delta_scan devolve as duas linhas não finitas num filtro por intervalo.
    with db.storage.duckdb_connect() as connection:
        query = f"SELECT count(*) FROM delta_scan('{uri}') WHERE valor > 1e300"
        assert connection.execute(query).fetchone()[0] == 2


# ---------------------------------------------------------------- o relatório e a auditoria


def test_load_report_matches_and_detects_a_difference(base: source.SourceBase, db: Database,
                                                      config: DuckDBConfig) -> None:
    """O relatório confere contagem e somas por partição, ``None`` numa tabela sem partição, com
    as conversões de tipo; uma linha apagada do Delta aparece como diferença na partição dela."""
    origin = origin_of(base)
    for name in ("cad_aliquotas", "cad_operacoes"):
        load.initial_load(db, TABLES[name], origin, config=config)
        report = load.load_report(db, TABLES[name], origin, config=config)
        assert report.matches, name
        assert report.skipped == (), name

    # A tabela sem partição, com as conversões; a tabela não carregada não confere.
    rates_report = load.load_report(db, TABLES["cad_aliquotas"], origin, config=config)
    assert [partition.value for partition in rates_report.partitions] == [None]
    assert rates_report.partitions[0].source_rows == base.rows["cad_aliquotas"]
    assert rates_report.partitions[0].source_sums.keys() == {"fator"}
    assert rates_report.conversions == ("id: int32 -> int64", "id_conta_origem: int32 -> int64",
                                        "id_conta_destino: int32 -> int64")
    entries_report = load.load_report(db, TABLES["cad_lancamentos"], origin, config=config)
    assert not entries_report.matches
    assert all(partition.delta_rows is None for partition in entries_report.partitions)
    assert "timestamp: INT96 -> timestamp[us]" in entries_report.conversions

    # Uma linha apagada do Delta aparece como diferença na partição dela.
    uri = db.uri(TABLES["cad_operacoes"])
    with db.storage.duckdb_connect() as connection:
        deleted_id = connection.execute(
            f"SELECT min(id_operacao) FROM delta_scan('{uri}') WHERE data_str = '2026-03-31'"
        ).fetchone()[0]
    delta.open_table(uri, db.storage).delete(
        f"data_str = '2026-03-31' AND id_operacao = {deleted_id}")
    report = load.load_report(db, TABLES["cad_operacoes"], origin, config=config)
    assert not report.matches
    differing = [partition for partition in report.partitions if not partition.matches]
    assert [partition.value for partition in differing] == ["2026-03-31"]
    assert differing[0].delta_rows == differing[0].source_rows - 1


def test_foreign_key_orphans_are_reported_not_blocking(base: source.SourceBase, db: Database,
                                                       config: DuckDBConfig) -> None:
    """A carga da base inteira não confere chave estrangeira: uma conta apagada depois da carga
    deixa lançamentos órfãos, que a auditoria de chave estrangeira do motor DuckDB registra em
    ``orfao_id_conta`` sobre a versão fixada de ``cad_contas``, e as outras chaves passam."""
    origin = origin_of(base)
    for table in load.load_order(db.tables()):
        load.initial_load(db, table, origin, config=config)
        assert load.load_report(db, table, origin, config=config).matches, table.name

    # Uma conta apagada depois da carga deixa lançamentos órfãos.
    entries = TABLES["cad_lancamentos"]
    entries_uri = db.uri(entries)
    accounts_uri = db.uri(TABLES["cad_contas"])
    with db.storage.duckdb_connect() as connection:
        orphaned_account = connection.execute(
            f"SELECT min(id_conta) FROM delta_scan('{entries_uri}') "
            "WHERE data_base_str = '2026-01-31'").fetchone()[0]
    delta.open_table(accounts_uri, db.storage).delete(f"id_conta = {orphaned_account}")

    # A auditoria das chaves estrangeiras sobre a versão fixada de cada tabela referenciada.
    referenced = {}
    for constraint in entries.foreign_key_constraints:
        target_uri = db.uri(constraint.referred_table)
        target_version = delta.open_table(target_uri, db.storage).version()
        referenced[constraint.referred_table.name] = (target_uri, target_version)
    version = delta.open_table(entries_uri, db.storage).version()
    with DuckDBEngine(config, "auditoria", db.storage) as engine:
        engine.ingest(entries, entries_uri, version, ["2026-01-31"])
        report = engine.audit(entries, ["2026-01-31"], entries_uri, version, foreign_keys=True,
                              referenced=referenced)
    assert not report.passed
    by_name = {result.name: result for result in report.results}
    assert by_name["orfao_id_conta"].defects >= 1
    orphan_checks = [name for name in by_name if name.startswith("orfao_")]
    other_orphans = [name for name in orphan_checks if name != "orfao_id_conta"]
    assert other_orphans
    assert all(by_name[name].passed for name in other_orphans)


# ---------------------------------------------------------------- a linha de comando


def test_cli_load_loads_the_base_and_reports(base: source.SourceBase, folder: Path,
                                             monkeypatch: pytest.MonkeyPatch,
                                             capsys: pytest.CaptureFixture) -> None:
    """``serialize-db load`` sobre a base inteira: as tabelas sem partição antes das
    particionadas, as três entradas fora do modelo, saída 0; a segunda execução não grava nada;
    1 na partição fora do contrato e no relatório com diferença; 2 no modelo fora do contrato, na
    tabela fora do modelo e na origem ausente, sem traceback."""
    monkeypatch.setattr(tempfile, "tempdir", str(folder))
    monkeypatch.delenv("SERIALIZE_DB_ROOT", raising=False)
    root = str(folder / "delta")
    common = ["load", "--metadata", "client_model:Base.metadata", "--source", origin_of(base),
              "--environment", "prd"]
    assert cli.main([*common, "--root", root]) == 0
    printed = capsys.readouterr().out
    assert "fora do modelo: alembic_version, meta_update_status, schema.json" in printed
    assert "12 tabela(s) conferida(s), contagens e somas iguais" in printed
    unpartitioned_line = printed.index("cad_contas: 1 partição(ões) gravada(s): None")
    partitioned_line = printed.index("cad_operacoes: 4 partição(ões) gravada(s): 2026-01-31")
    assert unpartitioned_line < partitioned_line
    assert "conversões: id_lancamento: int32 -> int64" in printed
    assert Path(root, "prd", "cad_lancamentos", "_delta_log").is_dir()

    # A segunda execução não grava nada.
    assert cli.main([*common, "--root", root]) == 0
    printed = capsys.readouterr().out
    assert printed.count("0 partição(ões) gravada(s)") == 12

    # Uma partição só de uma tabela: as outras faltam no Delta, e o relatório acusa.
    partial_root = str(folder / "parcial")
    partial = [*common, "--root", partial_root, "--tables", "cad_contratos",
               "--partitions", "2026-02-28"]
    assert cli.main(partial) == 1
    printed = capsys.readouterr().out
    assert "cad_contratos: 1 partição(ões) gravada(s): 2026-02-28" in printed
    assert "DIFERENÇA em 2026-01-31: origem 38 linhas" in printed
    assert "1 tabela(s) conferida(s), com diferenças" in printed

    # A partição fora do contrato: saída 1 com a mensagem, sem traceback.
    off_the_path = datetime.date(2026, 3, 31)
    defective = source_with_defect(folder, base, "cad_operacoes", "data", off_the_path)
    refused = ["load", "--metadata", "client_model:Base.metadata", "--source", defective,
               "--root", str(folder / "recusada"), "--tables", "cad_operacoes"]
    assert cli.main(refused) == 1
    printed_errors = capsys.readouterr().err
    refusal = "serialize-db load: cad_operacoes partição 2026-02-28: 1 linhas com data"
    assert refusal in printed_errors

    # Os erros de uso: o modelo de referência viola o contrato, a tabela fora do modelo e a
    # origem sem a pasta da tabela.
    reference = ["load", "--metadata", "reference_model.model_db_projetado:Base.metadata",
                 "--source", origin_of(base), "--root", str(folder / "nunca-gravada")]
    assert cli.main(reference) == 2
    assert "modelo fora do contrato" in capsys.readouterr().err
    assert not Path(folder, "nunca-gravada").exists()
    assert cli.main([*common, "--root", root, "--tables", "nada"]) == 2
    assert "a tabela nada não está nos modelos" in capsys.readouterr().err
    assert cli.main([*common[:3], "--source", str(folder / "vazia"), "--root", root]) == 2
    printed_errors = capsys.readouterr().err
    assert "a pasta da tabela não existe na origem" in printed_errors
    assert "Traceback" not in printed_errors
