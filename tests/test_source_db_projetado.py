"""A base Parquet de origem fictícia reproduz a estrutura que o probe leu nas duas bases reais.

``source_db_projetado.write_source`` grava a base uma vez por módulo sob a pasta da sessão da suíte
local, e os testes são ``local``: pulados sem ``SERIALIZE_DB_TEST_LOCAL_ROOT``. Cada teste confere
nos arquivos gravados um aspecto das leituras de ``probes/parquet_source.py`` sobre
``db_projetado`` (a base de desenvolvimento em 2026-09-20 e a de produção em 2026-09-21, idênticas
na seção 3): as tabelas e o arquivo solto na raiz, o esquema de cada arquivo no vocabulário do
relatório (tipo Arrow, nulidade, tipo físico, lógico e convertido), as partições, o layout físico,
os valores que a carga inicial tem de tratar, a leitura pelos dois leitores da biblioteca, o
controle de esquema da biblioteca anterior e a consistência da base com o modelo de referência. O
esquema esperado é a seção 3 do relatório, transcrita; a saída do probe fica em ``probes/output/``,
fora do git, e a transcrição é o que o teste guarda dela. A base é o material de
``tests/test_migrate_parquet_to_delta.py``, o teste da migração adiantada da carga inicial
(``plan/PLAN-STAGE-7.md``).
"""

from __future__ import annotations

import datetime
import json
import re
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

import parquet_source
import source_db_projetado as source
from conftest import LocalLocation

pytestmark = pytest.mark.local

# A seção 3 do relatório do probe sobre a base real, como ele a imprimiu: por tabela, coluna, tipo
# Arrow, nulidade, tipo físico, tipo lógico, tipo convertido e field_id.
OBSERVED_SCHEMAS = """
alembic_version
coluna       tipo Arrow  nulidade  físico      lógico  convertido  field_id
version_num  string      NÃO NULO  BYTE_ARRAY  String  UTF8        -

cad_aliquotas
coluna                tipo Arrow   nulidade  físico  lógico  convertido  field_id
id                    int32        NÃO NULO  INT32   None    NONE        -
id_conta_origem       int32        NÃO NULO  INT32   None    NONE        -
id_conta_destino      int32        NÃO NULO  INT32   None    NONE        -
data_fim_validade     date32[day]  nulo      INT32   Date    DATE        -
data_inicio_validade  date32[day]  NÃO NULO  INT32   Date    DATE        -
fator                 double       NÃO NULO  DOUBLE  None    NONE        -

cad_contas
coluna               tipo Arrow  nulidade  físico      lógico  convertido  field_id
id_conta             int32       NÃO NULO  INT32       None    NONE        -
nome                 string      NÃO NULO  BYTE_ARRAY  String  UTF8        -
numero               string      NÃO NULO  BYTE_ARRAY  String  UTF8        -
permite_lancamentos  bool        NÃO NULO  BOOLEAN     None    NONE        -

cad_contratos
coluna                     tipo Arrow   nulidade  físico      lógico  convertido  field_id
id_contrato                int32        NÃO NULO  INT32       None    NONE        -
data                       date32[day]  NÃO NULO  INT32       Date    DATE        -
sistema                    int32        nulo      INT32       None    NONE        -
contrato                   string       NÃO NULO  BYTE_ARRAY  String  UTF8        -
legado                     bool         NÃO NULO  BOOLEAN     None    NONE        -
um                         int32        nulo      INT32       None    NONE        -
to                         string       nulo      BYTE_ARRAY  String  UTF8        -
fonte                      int32        nulo      INT32       None    NONE        -
fonte_familia              string       nulo      BYTE_ARRAY  String  UTF8        -
estagio                    int32        nulo      INT32       None    NONE        -
taxa_juros_fixos           double       nulo      DOUBLE      None    NONE        -
data_assinatura            date32[day]  nulo      INT32       Date    DATE        -
data_primeira_amortizacao  date32[day]  nulo      INT32       Date    DATE        -
data_ultima_amortizacao    date32[day]  nulo      INT32       Date    DATE        -

cad_lancamentos
coluna         tipo Arrow     nulidade  físico      lógico  convertido  field_id
id_lancamento  int32          NÃO NULO  INT32       None    NONE        -
id_veiculo     int32          NÃO NULO  INT32       None    NONE        -
id_conta       int32          NÃO NULO  INT32       None    NONE        -
data           date32[day]    NÃO NULO  INT32       Date    DATE        -
valor          double         NÃO NULO  DOUBLE      None    NONE        -
meta           string         nulo      BYTE_ARRAY  String  UTF8        -
timestamp      timestamp[ns]  NÃO NULO  INT96       None    NONE        -
id_mensuracao  int32          NÃO NULO  INT32       None    NONE        -
id_segmento    int32          nulo      INT32       None    NONE        -
id_negocio     int32          nulo      INT32       None    NONE        -
data_base      date32[day]    NÃO NULO  INT32       Date    DATE        -
sistema        int32          nulo      INT32       None    NONE        -
contrato       string         nulo      BYTE_ARRAY  String  UTF8        -
area           string         nulo      BYTE_ARRAY  String  UTF8        -

cad_operacoes
coluna                  tipo Arrow   nulidade  físico      lógico  convertido  field_id
id_operacao             int32        NÃO NULO  INT32       None    NONE        -
data                    date32[day]  NÃO NULO  INT32       Date    DATE        -
operacao                string       NÃO NULO  BYTE_ARRAY  String  UTF8        -
legado                  bool         NÃO NULO  BOOLEAN     None    NONE        -
area                    string       nulo      BYTE_ARRAY  String  UTF8        -
departamento            string       nulo      BYTE_ARRAY  String  UTF8        -
spread_basico           double       nulo      DOUBLE      None    NONE        -
spread_risco            double       nulo      DOUBLE      None    NONE        -
spread_total            double       nulo      DOUBLE      None    NONE        -
taxa_total              double       nulo      DOUBLE      None    NONE        -
taxa_bndes              double       nulo      DOUBLE      None    NONE        -
custo_adicional         double       nulo      DOUBLE      None    NONE        -
instrumento_financeiro  string       nulo      BYTE_ARRAY  String  UTF8        -

dom_hierarquias_contas
coluna         tipo Arrow  nulidade  físico      lógico  convertido  field_id
id_hierarquia  int32       NÃO NULO  INT32       None    NONE        -
nome           string      NÃO NULO  BYTE_ARRAY  String  UTF8        -
descricao      string      nulo      BYTE_ARRAY  String  UTF8        -

dom_mensuracoes
coluna         tipo Arrow  nulidade  físico      lógico  convertido  field_id
id_mensuracao  int32       NÃO NULO  INT32       None    NONE        -
nome           string      NÃO NULO  BYTE_ARRAY  String  UTF8        -

dom_negocios
coluna       tipo Arrow  nulidade  físico      lógico  convertido  field_id
id_negocio   int32       NÃO NULO  INT32       None    NONE        -
id_segmento  int32       NÃO NULO  INT32       None    NONE        -
nome         string      NÃO NULO  BYTE_ARRAY  String  UTF8        -

dom_segmentos
coluna       tipo Arrow  nulidade  físico      lógico  convertido  field_id
id_segmento  int32       NÃO NULO  INT32       None    NONE        -
nome         string      NÃO NULO  BYTE_ARRAY  String  UTF8        -

dom_veiculos
coluna      tipo Arrow  nulidade  físico      lógico  convertido  field_id
id_veiculo  int32       NÃO NULO  INT32       None    NONE        -
nome        string      NÃO NULO  BYTE_ARRAY  String  UTF8        -

meta_update_status
coluna            tipo Arrow     nulidade  físico      lógico  convertido  field_id
id_update_status  int32          NÃO NULO  INT32       None    NONE        -
table_name        string         NÃO NULO  BYTE_ARRAY  String  UTF8        -
partition         string         nulo      BYTE_ARRAY  String  UTF8        -
timestamp         timestamp[ns]  NÃO NULO  INT96       None    NONE        -

rel_contas_hierarquias
coluna                   tipo Arrow  nulidade  físico  lógico  convertido  field_id
id_rel_conta_hierarquia  int32       NÃO NULO  INT32   None    NONE        -
id_hierarquia            int32       NÃO NULO  INT32   None    NONE        -
id_parent                int32       NÃO NULO  INT32   None    NONE        -
id_child                 int32       NÃO NULO  INT32   None    NONE        -

rel_contrato_operacao
coluna                    tipo Arrow   nulidade  físico      lógico  convertido  field_id
id_rel_contrato_operacao  int32        NÃO NULO  INT32       None    NONE        -
data                      date32[day]  NÃO NULO  INT32       Date    DATE        -
operacao                  string       NÃO NULO  BYTE_ARRAY  String  UTF8        -
sistema                   int32        NÃO NULO  INT32       None    NONE        -
contrato                  string       NÃO NULO  BYTE_ARRAY  String  UTF8        -
fator_rateio              double       NÃO NULO  DOUBLE      None    NONE        -
"""

# A seção 5: a coluna de partição de cada tabela particionada. A leitura tinha três datas em
# cad_contratos, cad_operacoes e rel_contrato_operacao (2026-02-28, 2026-03-31 e 2026-06-30) e
# quatro em cad_lancamentos; a base fictícia dá as quatro a todas, para que cada data_base tenha os
# seus contratos.
OBSERVED_PARTITION_COLUMNS = {
    "cad_contratos": "data_str",
    "cad_lancamentos": "data_base_str",
    "cad_operacoes": "data_str",
    "rel_contrato_operacao": "data_str",
}
PARTITION_VALUES = ["2026-01-31", "2026-02-28", "2026-03-31", "2026-06-30"]
OBSERVED_TABLES = [
    "alembic_version",
    "cad_aliquotas",
    "cad_contas",
    "cad_contratos",
    "cad_lancamentos",
    "cad_operacoes",
    "dom_hierarquias_contas",
    "dom_mensuracoes",
    "dom_negocios",
    "dom_segmentos",
    "dom_veiculos",
    "meta_update_status",
    "rel_contas_hierarquias",
    "rel_contrato_operacao",
]
# As tabelas do modelo sem partição, uma linha cada no registro das cargas: as cinco dom_*,
# cad_contas, cad_aliquotas e rel_contas_hierarquias.
UNPARTITIONED_MODEL_TABLE_COUNT = 8
# As partições das tabelas particionadas: uma linha de meta_update_status por partição.
PARTITION_COUNT = len(OBSERVED_PARTITION_COLUMNS) * len(PARTITION_VALUES)

# Os tipos do controle de esquema da biblioteca anterior e o tipo Arrow que os arquivos têm.
SQL_TYPES = {
    "INTEGER": "int32",
    "VARCHAR": "string",
    "DATE": "date32[day]",
    "BOOLEAN": "bool",
    "DOUBLE_PRECISION": "double",
    "TIMESTAMP": "timestamp[ns]",
}

# As opções do read_parquet que leem o valor do caminho Hive como texto.
HIVE_AS_TEXT = "hive_partitioning=true, hive_types_autocast=false"


def parse_observed(text: str) -> dict[str, list[tuple[str, ...]]]:
    """Os blocos da seção 3: uma tupla por coluna, com as sete células da tabela do relatório."""
    schemas: dict[str, list[tuple[str, ...]]] = {}
    # Um bloco por tabela, separado por linha em branco: o nome, o cabeçalho e uma linha por coluna.
    for block in text.strip().split("\n\n"):
        lines = block.splitlines()
        table = lines[0].strip()
        rows = []
        for line in lines[2:]:
            cells = re.split(r"\s{2,}", line.strip())
            rows.append(tuple(cells))
        schemas[table] = rows
    return schemas


def read_as_the_probe(path: Path) -> list[tuple[str, ...]]:
    """As sete células do relatório para cada coluna de um arquivo, lidas do rodapé por
    ``parquet_source.footer_columns``, a leitura do probe."""
    rows = []
    for column in parquet_source.footer_columns(pq.ParquetFile(path)):
        nullability = "nulo" if column.nullable else "NÃO NULO"
        rows.append(
            (
                column.name,
                column.arrow_type,
                nullability,
                column.physical,
                column.logical,
                column.converted,
                column.field_id,
            )
        )
    return rows


def files_under(folder: Path) -> list[Path]:
    """Todos os arquivos sob ``folder``, em qualquer profundidade, em ordem de caminho."""
    return sorted(path for path in folder.rglob("*") if path.is_file())


def chunk_number(path: Path) -> int:
    """O número ``n`` de um arquivo ``chunk_<n>.parquet``."""
    return int(path.stem.removeprefix("chunk_"))


def footer_keys(path: Path) -> set[bytes]:
    """As chaves dos metadados do esquema no rodapé do arquivo."""
    return set(pq.read_schema(path).metadata or {})


def read_partitioned(base: source.SourceBase, table: str) -> pa.Table:
    """A tabela particionada inteira, lida pelo PyArrow com a partição Hive."""
    return ds.dataset(base.root / table, format="parquet", partitioning="hive").to_table()


def connect_with_views(base: source.SourceBase) -> duckdb.DuckDBPyConnection:
    """Uma conexão em memória com uma view por tabela sobre os arquivos gravados; a coluna de
    partição vem do caminho como texto."""
    connection = duckdb.connect()
    for table in OBSERVED_TABLES:
        if table in OBSERVED_PARTITION_COLUMNS:
            reader = f"read_parquet('{base.root / table}/*/*.parquet', {HIVE_AS_TEXT})"
        else:
            reader = f"read_parquet('{base.root / table}/*.parquet')"
        connection.execute(f"CREATE VIEW {table} AS SELECT * FROM {reader}")
    return connection


@pytest.fixture(scope="module")
def base(local_location: LocalLocation) -> source.SourceBase:
    """A base gravada uma vez por módulo, sob a pasta da sessão da suíte local."""
    return source.write_source(Path(local_location.child("base-ficticia")))


def test_root_has_the_table_folders_and_the_loose_file(base: source.SourceBase) -> None:
    """14 pastas de tabela, só arquivos Parquet dentro delas, e ``schema.json`` como o único arquivo
    solto na raiz."""
    folders = sorted(path.name for path in base.root.iterdir() if path.is_dir())
    loose = sorted(path.name for path in base.root.iterdir() if path.is_file())
    assert folders == OBSERVED_TABLES
    assert loose == ["schema.json"]

    for table in OBSERVED_TABLES:
        files = files_under(base.root / table)
        assert files, table
        assert all(path.suffix == ".parquet" for path in files), table
        assert files == sorted(base.files[table]), table
        file_rows = sum(pq.read_metadata(path).num_rows for path in files)
        assert base.rows[table] == file_rows, table

    # As tabelas fora do modelo de referência estão na origem e a carga as pula: a revisão do
    # Alembic e o registro das cargas, uma linha por tabela sem partição e uma por partição das
    # demais.
    assert set(source.OUTSIDE_MODEL) < set(OBSERVED_TABLES)
    assert base.rows["alembic_version"] == 1
    status_rows = UNPARTITIONED_MODEL_TABLE_COUNT + PARTITION_COUNT
    assert base.rows["meta_update_status"] == status_rows


def test_every_file_has_the_schema_the_probe_reported(base: source.SourceBase) -> None:
    """Cada arquivo de cada tabela repete a seção 3 do relatório: nome, tipo Arrow, nulidade,
    físico, lógico, convertido, sem field_id."""
    observed = parse_observed(OBSERVED_SCHEMAS)
    assert sorted(observed) == OBSERVED_TABLES

    # Cada arquivo lido pelo probe.
    for table, columns in observed.items():
        for path in base.files[table]:
            assert read_as_the_probe(path) == columns, path


def test_transcription_has_the_type_counts_of_the_report() -> None:
    """A transcrição da seção 3 tem os seis tipos da base nas contagens do relatório, 78 colunas ao
    todo: int32 30, string 23, date32 10, double 10, bool 3, timestamp[ns] 2."""
    columns_per_type: dict[str, int] = {}
    for columns in parse_observed(OBSERVED_SCHEMAS).values():
        for column in columns:
            arrow_type = column[1]
            columns_per_type[arrow_type] = columns_per_type.get(arrow_type, 0) + 1
    assert columns_per_type == {
        "int32": 30,
        "string": 23,
        "date32[day]": 10,
        "double": 10,
        "bool": 3,
        "timestamp[ns]": 2,
    }


def assert_partition_files(folder: Path, partition: source.Partition, value: str) -> None:
    """Os arquivos da partição ``value`` não trazem a coluna do caminho e têm ``value`` na coluna de
    origem em toda linha."""
    expected = [datetime.date.fromisoformat(value)]
    for path in files_under(folder / f"{partition.column}={value}"):
        data = pq.read_table(path)
        assert partition.column not in data.column_names, path
        assert data.column(partition.source).unique().to_pylist() == expected, path


def test_partitions_live_in_the_path_and_equal_the_source_column(base: source.SourceBase) -> None:
    """Hive por ``data_str`` ou ``data_base_str``, valor ausente do arquivo e igual a ``data`` ou
    ``data_base`` em toda linha."""
    for table, column in OBSERVED_PARTITION_COLUMNS.items():
        folder = base.root / table
        partition_folders = sorted(path.name for path in folder.iterdir())
        assert partition_folders == [f"{column}={value}" for value in PARTITION_VALUES], table
        loose_files = [path for path in folder.iterdir() if path.is_file()]
        assert not loose_files, f"{table}: arquivo fora das partições"

        partition = source.PARTITIONS[table]
        assert partition.column == column, table
        assert list(partition.values) == PARTITION_VALUES, table
        for value in PARTITION_VALUES:
            assert_partition_files(folder, partition, value)

    # As tabelas sem partição têm um único chunk_0.parquet na raiz da tabela.
    for table in OBSERVED_TABLES:
        if table in OBSERVED_PARTITION_COLUMNS:
            continue
        names = [path.name for path in files_under(base.root / table)]
        assert names == ["chunk_0.parquet"], table


def test_chunks_are_numbered_from_zero_without_padding(base: source.SourceBase) -> None:
    """``chunk_<n>.parquet`` de 0 em diante por partição, o último com até ``CHUNK_ROWS`` linhas, e
    ``chunk_10`` antes de ``chunk_2`` na ordem alfabética."""
    for table in OBSERVED_PARTITION_COLUMNS:
        for partition_folder in sorted((base.root / table).iterdir()):
            files = files_under(partition_folder)
            numbers = sorted(chunk_number(path) for path in files)
            assert numbers == list(range(len(files))), partition_folder
            rows_by_chunk = {chunk_number(path): pq.read_metadata(path).num_rows for path in files}
            # Todo chunk antes do último tem CHUNK_ROWS linhas; o último, de 1 a CHUNK_ROWS.
            full_chunks = numbers[:-1]
            last_chunk = numbers[-1]
            for number in full_chunks:
                assert rows_by_chunk[number] == source.CHUNK_ROWS, (partition_folder, number)
            assert 0 < rows_by_chunk[last_chunk] <= source.CHUNK_ROWS, partition_folder

    # A ordem alfabética põe chunk_10 antes de chunk_2.
    january = base.root / "cad_lancamentos" / "data_base_str=2026-01-31"
    names = sorted(path.name for path in january.iterdir())
    assert "chunk_11.parquet" in names
    assert names.index("chunk_10.parquet") < names.index("chunk_2.parquet")
    february_relations = base.root / "rel_contrato_operacao" / "data_str=2026-02-28"
    assert pq.read_metadata(february_relations / "chunk_2.parquet").num_rows == 2


def assert_column_chunks(path: Path, row_group: pq.RowGroupMetaData) -> None:
    """Cada coluna do row group em SNAPPY, só com PLAIN e RLE, e com estatística fora do INT96."""
    for index in range(row_group.num_columns):
        chunk = row_group.column(index)
        where = (path, chunk.path_in_schema)
        assert chunk.compression == "SNAPPY", where
        assert set(chunk.encodings) <= {"PLAIN", "RLE"}, where
        if chunk.physical_type == "INT96":
            assert chunk.statistics is None, where
            continue
        # Fora do INT96 toda coluna tem estatística: mínimo e máximo, ou só nulos.
        statistics = chunk.statistics
        assert statistics is not None, where
        assert statistics.has_min_max or statistics.null_count == row_group.num_rows, where


def assert_file_layout(table: str, path: Path) -> None:
    """Um row group, formato 1.0, gravado pelo parquet-cpp-arrow, e a chave ``pandas`` no rodapé só
    onde a origem a gravou."""
    metadata = pq.read_metadata(path)
    assert metadata.num_row_groups == 1, path
    assert metadata.format_version == "1.0", path
    assert metadata.created_by.startswith("parquet-cpp-arrow"), path

    # O valor da partição vem da pasta <coluna>=<valor>.
    value = None
    if table in OBSERVED_PARTITION_COLUMNS:
        value = path.parent.name.removeprefix(f"{OBSERVED_PARTITION_COLUMNS[table]}=")
    expected_keys = {b"pandas"} if source.written_by_pandas(table, value) else set()
    assert footer_keys(path) == expected_keys, path
    assert_column_chunks(path, metadata.row_group(0))


def test_physical_layout_matches_the_reading(base: source.SourceBase) -> None:
    """Um row group por arquivo, SNAPPY, PLAIN e RLE, formato 1.0, parquet-cpp-arrow, a chave
    ``pandas`` em parte dos arquivos e INT96 sem estatística."""
    for table in OBSERVED_TABLES:
        for path in base.files[table]:
            assert_file_layout(table, path)

    # As duas tabelas fora do modelo saem sem chave, e cada tabela particionada tem arquivos com e
    # sem ela, como na produção.
    for table in source.OUTSIDE_MODEL:
        assert not footer_keys(base.files[table][0]), table
    for table in OBSERVED_PARTITION_COLUMNS:
        files_with_key = [path for path in base.files[table] if b"pandas" in footer_keys(path)]
        assert 0 < len(files_with_key) < len(base.files[table]), table


def test_cad_lancamentos_values_reproduce_what_the_initial_load_handles(
    base: source.SourceBase,
) -> None:
    """Os valores de ``cad_lancamentos`` que a carga inicial tem de tratar, um por regra de
    ``plan/PLAN-STAGE-7.md``."""
    entries = read_partitioned(base, "cad_lancamentos")

    # ``valor`` é double com três casas na leitura de desenvolvimento (cinco no extremo da
    # produção); o par extremo está presente.
    amounts = entries.column("valor")
    assert amounts.type == pa.float64()
    rounded_to_cents = pc.round(amounts, 2)
    more_than_two_decimals = pc.sum(pc.not_equal(rounded_to_cents, amounts)).as_py()
    assert more_than_two_decimals > entries.num_rows // 2
    assert pc.max(amounts).as_py() == source.EXTREME_AMOUNT
    assert pc.min(amounts).as_py() == -source.EXTREME_AMOUNT

    # O ``timestamp`` chega em nanossegundos (INT96) com a parte sub-microssegundo zerada.
    timestamp = entries.column("timestamp")
    assert timestamp.type == pa.timestamp("ns")
    truncated = pc.floor_temporal(timestamp, unit="microsecond")
    assert pc.all(pc.equal(truncated, timestamp)).as_py()

    # Ids ``int32`` esparsos até 1.113.599.996 (952.517.158 na produção), pouco acima da metade do
    # tipo; a migração os leva a ``int64``.
    entry_ids = entries.column("id_lancamento")
    largest_id = pc.max(entry_ids).as_py()
    assert largest_id == source.MAX_ENTRY_ID
    assert largest_id < 2**31
    assert len(entry_ids.unique()) == entries.num_rows

    # ``data`` é o mês projetado, sempre depois de ``data_base`` e até 2026-12-31; ``meta`` é sempre
    # nula.
    projected_after_base = pc.greater(entries.column("data"), entries.column("data_base"))
    assert pc.all(projected_after_base).as_py()
    assert pc.max(entries.column("data")).as_py() == source.PROJECTION_HORIZON
    assert entries.column("meta").null_count == entries.num_rows

    # O lançamento sem contrato tem ``sistema`` e ``contrato`` nulos juntos.
    system_is_null = pc.is_null(entries.column("sistema"))
    contract_is_null = pc.is_null(entries.column("contrato"))
    assert pc.all(pc.equal(system_is_null, contract_is_null)).as_py()
    assert 0 < entries.column("contrato").null_count < entries.num_rows


def test_cad_contratos_values_reproduce_what_the_initial_load_handles(
    base: source.SourceBase,
) -> None:
    """Os valores de ``cad_contratos`` que a carga inicial tem de tratar, um por regra de
    ``plan/PLAN-STAGE-7.md``."""
    contracts = read_partitioned(base, "cad_contratos")

    # Cada ``data_base`` de ``cad_lancamentos`` tem os seus contratos: as quatro datas.
    assert set(contracts.column("data_str").to_pylist()) == set(PARTITION_VALUES)

    # ``data_assinatura`` tem nulo em mais da metade das linhas.
    assert contracts.column("data_assinatura").null_count > contracts.num_rows // 2


def test_unpartitioned_values_reproduce_what_the_initial_load_handles(
    base: source.SourceBase,
) -> None:
    """Os valores das tabelas sem partição que a carga inicial tem de tratar, um por regra de
    ``plan/PLAN-STAGE-7.md``."""
    # ``fator`` com cinco casas e ``data_fim_validade`` toda nula.
    rates = pq.read_table(base.files["cad_aliquotas"][0])
    assert 0.59895 in rates.column("fator").to_pylist()
    assert rates.column("data_fim_validade").null_count == rates.num_rows

    # O registro das cargas com a partição em JSON.
    status = pq.read_table(base.files["meta_update_status"][0])
    partition_texts = status.column("partition").to_pylist()
    partitions = [json.loads(text) for text in partition_texts if text is not None]
    assert len(partitions) == PARTITION_COUNT
    assert status.column("partition").null_count == UNPARTITIONED_MODEL_TABLE_COUNT
    assert {"data_base": {"__type__": "date", "value": "2026-01-31"}} in partitions

    # A revisão do Alembic.
    alembic = pq.read_table(base.files["alembic_version"][0])
    assert alembic.column("version_num").to_pylist() == [source.ALEMBIC_REVISION]


def test_the_base_satisfies_the_reference_model(base: source.SourceBase) -> None:
    """Toda chave do modelo é única, toda chave estrangeira tem a linha referenciada, e as colunas
    NOT NULL do modelo não têm nulo."""
    connection = connect_with_views(base)
    for table, keys in source.UNIQUE_KEYS.items():
        for columns in keys:
            key = ", ".join(f'"{column}"' for column in columns)
            duplicated = connection.execute(
                "WITH chaves_repetidas AS "
                f"(SELECT {key} FROM {table} GROUP BY ALL HAVING count(*) > 1) "
                "SELECT count(*) FROM chaves_repetidas"
            ).fetchone()[0]
            assert duplicated == 0, (table, columns)

    # Anti-join por chave estrangeira; uma linha com componente nulo não é conferida, como no SQL.
    for child, columns, parent, referenced in source.FOREIGN_KEYS:
        column_pairs = zip(columns, referenced, strict=True)
        condition = " AND ".join(
            f'filha."{child_column}" = referenciada."{parent_column}"'
            for child_column, parent_column in column_pairs
        )
        not_null = " AND ".join(f'filha."{child_column}" IS NOT NULL' for child_column in columns)
        orphans = connection.execute(
            f"SELECT count(*) FROM {child} filha WHERE {not_null} "
            f"AND NOT EXISTS (SELECT 1 FROM {parent} referenciada WHERE {condition})"
        ).fetchone()[0]
        assert orphans == 0, (child, columns, parent)

    # As sete colunas de ``cad_contratos`` anuláveis nos arquivos e ``NOT NULL`` no modelo não têm
    # nulo.
    for table, columns in source.MODEL_NOT_NULL_DECLARED_NULLABLE.items():
        for column in columns:
            nulls = connection.execute(
                f'SELECT count(*) FILTER (WHERE "{column}" IS NULL) FROM {table}'
            ).fetchone()[0]
            assert nulls == 0, (table, column)


def test_rel_contrato_operacao_apportions_each_contract_among_its_operations(
    base: source.SourceBase,
) -> None:
    """A relação N para N: toda operação tem contratos, um contrato está em mais de uma operação, e
    ``fator_rateio`` soma 1 por contrato."""
    connection = connect_with_views(base)
    sums = connection.execute(
        "SELECT sum(fator_rateio) FROM rel_contrato_operacao GROUP BY data, sistema, contrato"
    ).fetchall()
    totals = [row[0] for row in sums]
    assert totals
    assert all(total == 1.0 for total in totals)

    # Toda operação está na relação (o modelo só declara a chave estrangeira no sentido contrário,
    # dos contratos).
    without_contract = connection.execute(
        "SELECT count(*) FROM cad_operacoes operacoes WHERE NOT EXISTS "
        "(SELECT 1 FROM rel_contrato_operacao relacao "
        "WHERE relacao.data = operacoes.data AND relacao.operacao = operacoes.operacao)"
    ).fetchone()[0]
    assert without_contract == 0

    # Alguma operação tem mais de um contrato, e algum contrato está em mais de uma operação.
    per_operation = connection.execute(
        "WITH contratos_por_operacao AS "
        "(SELECT count(*) AS linhas FROM rel_contrato_operacao GROUP BY data, operacao) "
        "SELECT max(linhas) FROM contratos_por_operacao"
    ).fetchone()[0]
    per_contract = connection.execute(
        "WITH operacoes_por_contrato AS "
        "(SELECT count(*) AS linhas FROM rel_contrato_operacao GROUP BY data, sistema, contrato) "
        "SELECT max(linhas) FROM operacoes_por_contrato"
    ).fetchone()[0]
    assert per_operation > 1
    assert per_contract > 1

    # O par (data, operacao, sistema, contrato) não se repete.
    repeated = connection.execute(
        "WITH pares_repetidos AS "
        "(SELECT data, operacao, sistema, contrato FROM rel_contrato_operacao "
        "GROUP BY ALL HAVING count(*) > 1) "
        "SELECT count(*) FROM pares_repetidos"
    ).fetchone()[0]
    assert repeated == 0


def path_to_root(parent_of: dict[int, int], account: int) -> list[int]:
    """As contas da subida de ``account`` pelos pais, até a que não tem pai; um ciclo reprova."""
    path = [account]
    while path[-1] in parent_of:
        parent = parent_of[path[-1]]
        assert parent not in path, ("ciclo", path)
        path.append(parent)
    return path


def test_rel_contas_hierarquias_is_a_tree_of_accounts(base: source.SourceBase) -> None:
    """A hierarquia 1 é uma árvore de contas: nenhuma conta é pai de si mesma, cada conta tem um
    pai só, há uma raiz, e a subida pelos pais leva toda conta até ela sem ciclo."""
    hierarchy = pq.read_table(base.files["rel_contas_hierarquias"][0])
    parents = hierarchy.column("id_parent").to_pylist()
    children = hierarchy.column("id_child").to_pylist()
    parent_of = dict(zip(children, parents, strict=True))

    # Nenhuma conta é pai de si mesma, e cada filho aparece numa relação só.
    for child, parent in parent_of.items():
        assert child != parent, child
    assert len(parent_of) == hierarchy.num_rows

    # As contagens da leitura de desenvolvimento: 93 filhos e 32 pais distintos.
    assert len(parent_of) == 93
    assert len(set(parents)) == 32

    # Uma raiz só, o pai que não é filho de ninguém, e toda conta sobe até ela.
    roots = set(parents) - set(children)
    assert len(roots) == 1
    for child in children:
        assert path_to_root(parent_of, child)[-1] in roots, child


def test_schema_json_is_the_previous_library_control_and_matches_the_files(
    base: source.SourceBase,
) -> None:
    """O ``schema.json`` real, a reflexão do SQLAlchemy do banco anterior: as colunas, os tipos e a
    nulidade são os dos arquivos."""
    control = json.loads((base.root / "schema.json").read_text(encoding="utf-8"))
    assert sorted(control) == OBSERVED_TABLES
    for table, entry in control.items():
        schema = pq.read_schema(base.files[table][0])
        assert [column["name"] for column in entry["columns"]] == schema.names, table
        for column in entry["columns"]:
            field = schema.field(column["name"])
            where = (table, column["name"])
            assert SQL_TYPES[column["type"]] == str(field.type), where
            assert column["nullable"] == field.nullable, where

    # As chaves estrangeiras compostas do modelo de referência não existiam no banco anterior.
    assert control["cad_contratos"]["foreign_keys"] == []
    assert control["rel_contrato_operacao"]["foreign_keys"] == []
    entry_keys = control["cad_lancamentos"]["foreign_keys"]
    referenced_tables = {key["ref_table"] for key in entry_keys}
    assert referenced_tables == {
        "dom_negocios",
        "dom_veiculos",
        "dom_segmentos",
        "cad_contas",
        "dom_mensuracoes",
    }


def test_both_readers_see_the_partition_column_from_the_path(base: source.SourceBase) -> None:
    """O DuckDB e o PyArrow leem a pasta com partição Hive: as mesmas linhas, e a coluna do caminho
    tipada por cada leitor."""
    connection = duckdb.connect()
    for table, column in OBSERVED_PARTITION_COLUMNS.items():
        glob = f"{base.root / table}/*/*.parquet"
        # O DuckDB converte o valor do caminho para DATE por padrão; só com
        # hive_types_autocast=false ele fica VARCHAR.
        typed = connection.execute(
            f"SELECT typeof({column}) FROM read_parquet('{glob}', hive_partitioning=true) LIMIT 1"
        ).fetchone()
        as_text = connection.execute(
            f"SELECT typeof({column}) FROM read_parquet('{glob}', {HIVE_AS_TEXT}) LIMIT 1"
        ).fetchone()
        assert typed == ("DATE",), table
        assert as_text == ("VARCHAR",), table
        counted = connection.execute(
            f"SELECT {column}::VARCHAR, count(*) "
            f"FROM read_parquet('{glob}', hive_partitioning=true) GROUP BY 1"
        ).fetchall()
        assert dict(counted) == base.partition_rows[table], table

        # O PyArrow lê o valor do caminho como texto.
        dataset = ds.dataset(base.root / table, format="parquet", partitioning="hive")
        assert dataset.schema.field(column).type == pa.string(), table
        assert dataset.count_rows() == base.rows[table], table
        expected_names = set(source.SCHEMAS[table].names) | {column}
        assert set(dataset.schema.names) == expected_names, table
