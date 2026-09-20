"""A base Parquet de origem fictícia reproduz a estrutura que o probe leu na base de desenvolvimento.

``source_db_projetado.write_source`` grava a base sob a pasta temporária do pytest, e cada teste
confere nos arquivos gravados um aspecto da leitura de 2026-09-20 (``probes/parquet_source.py``
sobre ``db_projetado``, ambiente de desenvolvimento): as tabelas e o arquivo solto na raiz, o
esquema de cada arquivo no vocabulário do relatório (tipo Arrow, nulidade, tipo físico, lógico e
convertido), as partições, o layout físico, os valores que a carga inicial tem de tratar e a leitura
pelos dois leitores da biblioteca. O esquema esperado é a seção 3 do relatório, transcrita; a saída
do probe fica em ``probes/output/``, fora do git, e a transcrição é o que o teste guarda dela. A
base é o material do teste da carga inicial (``docs/PLAN-STAGE-7.md``), ainda por escrever.
"""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

import source_db_projetado as source

# A seção 3 do relatório do probe sobre a base real, como ele a imprimiu: por tabela, coluna, tipo Arrow,
# nulidade, tipo físico, tipo lógico, tipo convertido e field_id.
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

# A seção 5: a coluna de partição de cada tabela particionada e os seus valores, fins de mês não contíguos.
OBSERVED_PARTITIONS = {
    "cad_contratos": ("data_str", ["2026-02-28", "2026-03-31", "2026-06-30"]),
    "cad_lancamentos": ("data_base_str", ["2026-01-31", "2026-02-28", "2026-03-31", "2026-06-30"]),
    "cad_operacoes": ("data_str", ["2026-02-28", "2026-03-31", "2026-06-30"]),
    "rel_contrato_operacao": ("data_str", ["2026-02-28", "2026-03-31", "2026-06-30"]),
}
OBSERVED_TABLES = sorted(OBSERVED_PARTITIONS) + [
    "alembic_version",
    "cad_aliquotas",
    "cad_contas",
    "dom_hierarquias_contas",
    "dom_mensuracoes",
    "dom_negocios",
    "dom_segmentos",
    "dom_veiculos",
    "meta_update_status",
    "rel_contas_hierarquias",
]


def parse_observed(text: str) -> dict[str, list[tuple[str, ...]]]:
    """Os blocos da seção 3: uma tupla por coluna, com as sete células da tabela do relatório."""
    schemas: dict[str, list[tuple[str, ...]]] = {}
    table = None
    for line in text.strip().splitlines():
        if not line.strip():
            table = None
        elif table is None:
            table = line.strip()
            schemas[table] = []
        elif not line.startswith("coluna"):
            schemas[table].append(tuple(re.split(r"\s{2,}", line.strip())))
    return schemas


def read_as_the_probe(path: Path) -> list[tuple[str, ...]]:
    """As sete células do relatório para cada coluna de um arquivo, lidas do rodapé como o probe as lê."""
    parquet = pq.ParquetFile(path)
    leaves = {parquet.schema.column(i).path: parquet.schema.column(i) for i in range(len(parquet.schema))}
    rows = []
    for field in parquet.schema_arrow:
        leaf = leaves[field.name]
        field_id = (field.metadata or {}).get(b"PARQUET:field_id", b"").decode() or "-"
        rows.append((field.name, str(field.type), "nulo" if field.nullable else "NÃO NULO", leaf.physical_type, str(leaf.logical_type), str(leaf.converted_type), field_id))
    return rows


def parquet_files(folder: Path) -> list[Path]:
    return sorted(path for path in folder.rglob("*") if path.is_file())


@pytest.fixture(scope="module")
def base(tmp_path_factory: pytest.TempPathFactory) -> source.SourceBase:
    """A base gravada uma vez por módulo, sob a pasta temporária do pytest."""
    return source.write_source(tmp_path_factory.mktemp("db_projetado"))


def test_root_has_the_table_folders_and_the_loose_file(base: source.SourceBase) -> None:
    """14 pastas de tabela, só arquivos Parquet dentro delas, e ``schema.json`` como o único arquivo solto na raiz."""
    folders = sorted(path.name for path in base.root.iterdir() if path.is_dir())
    loose = sorted(path.name for path in base.root.iterdir() if path.is_file())
    assert folders == sorted(OBSERVED_TABLES)
    assert loose == ["schema.json"]
    assert json.loads((base.root / "schema.json").read_text(encoding="utf-8"))["tabelas"].keys() == set(OBSERVED_TABLES)

    for table in OBSERVED_TABLES:
        files = parquet_files(base.root / table)
        assert files and all(path.suffix == ".parquet" for path in files), table
        assert files == sorted(base.files[table]), table
        assert base.rows[table] == sum(pq.read_metadata(path).num_rows for path in files), table

    # As tabelas fora do modelo de referência estão na origem e a carga as pula.
    assert set(source.OUTSIDE_MODEL) < set(OBSERVED_TABLES)
    assert base.rows["alembic_version"] == 1 and base.rows["meta_update_status"] == 21


def test_every_file_has_the_schema_the_probe_reported(base: source.SourceBase) -> None:
    """Cada arquivo de cada tabela repete a seção 3 do relatório: nome, tipo Arrow, nulidade, físico, lógico, convertido, sem field_id."""
    observed = parse_observed(OBSERVED_SCHEMAS)
    assert sorted(observed) == sorted(OBSERVED_TABLES)

    for table, columns in observed.items():
        for path in base.files[table]:
            assert read_as_the_probe(path) == columns, path

    # Os seis tipos da base, nas contagens da seção 3: int32 30, string 23, date32 10, double 10, bool 3, timestamp[ns] 2.
    counter: dict[str, int] = {}
    for columns in observed.values():
        for column in columns:
            counter[column[1]] = counter.get(column[1], 0) + 1
    assert counter == {"int32": 30, "string": 23, "date32[day]": 10, "double": 10, "bool": 3, "timestamp[ns]": 2}
    assert sum(counter.values()) == 78


def test_partitions_live_in_the_path_and_equal_the_source_column(base: source.SourceBase) -> None:
    """Hive por ``data_str`` ou ``data_base_str``, valor ausente do arquivo e igual a ``data`` ou ``data_base`` em toda linha."""
    for table, (column, values) in OBSERVED_PARTITIONS.items():
        folder = base.root / table
        partitions = sorted(path.name for path in folder.iterdir())
        assert partitions == [f"{column}={value}" for value in values], table
        assert not any(path.is_file() for path in folder.iterdir()), f"{table}: arquivo fora das partições"

        partition = source.PARTITIONS[table]
        assert (partition.column, list(partition.values)) == (column, values)
        for value in values:
            for path in parquet_files(folder / f"{column}={value}"):
                data = pq.read_table(path)
                assert column not in data.column_names, path
                assert data.column(partition.source).unique().to_pylist() == [dt.date.fromisoformat(value)], path

    # As tabelas sem partição têm um único chunk_0.parquet na raiz da tabela.
    for table in OBSERVED_TABLES:
        if table not in OBSERVED_PARTITIONS:
            assert [path.name for path in parquet_files(base.root / table)] == ["chunk_0.parquet"], table


def test_chunks_are_numbered_from_zero_without_padding(base: source.SourceBase) -> None:
    """``chunk_<n>.parquet`` de 0 em diante por partição, o último menor que ``CHUNK_ROWS``, e ``chunk_10`` antes de ``chunk_2`` na ordem alfabética."""
    for table in OBSERVED_PARTITIONS:
        for partition in sorted((base.root / table).iterdir()):
            files = parquet_files(partition)
            numbers = sorted(int(path.stem.removeprefix("chunk_")) for path in files)
            assert numbers == list(range(len(files))), partition
            rows = {int(path.stem.removeprefix("chunk_")): pq.read_metadata(path).num_rows for path in files}
            assert all(rows[number] == source.CHUNK_ROWS for number in numbers[:-1]), partition
            assert 0 < rows[numbers[-1]] <= source.CHUNK_ROWS, partition

    january = base.root / "cad_lancamentos" / "data_base_str=2026-01-31"
    names = sorted(path.name for path in january.iterdir())
    assert "chunk_11.parquet" in names
    assert names.index("chunk_10.parquet") < names.index("chunk_2.parquet")
    assert pq.read_metadata(base.root / "rel_contrato_operacao" / "data_str=2026-02-28" / "chunk_1.parquet").num_rows == 2


def test_physical_layout_matches_the_reading(base: source.SourceBase) -> None:
    """Um row group por arquivo, SNAPPY, PLAIN e RLE, formato 1.0, parquet-cpp-arrow, a chave ``pandas`` e INT96 sem estatística."""
    for table in OBSERVED_TABLES:
        for path in base.files[table]:
            metadata = pq.read_metadata(path)
            assert metadata.num_row_groups == 1, path
            assert metadata.format_version == "1.0", path
            assert metadata.created_by.startswith("parquet-cpp-arrow"), path
            assert set(pq.read_schema(path).metadata) == {b"pandas"}, path
            group = metadata.row_group(0)
            for index in range(group.num_columns):
                chunk = group.column(index)
                assert chunk.compression == "SNAPPY", (path, chunk.path_in_schema)
                assert set(chunk.encodings) <= {"PLAIN", "RLE"}, (path, chunk.path_in_schema)
                if chunk.physical_type == "INT96":
                    assert chunk.statistics is None, (path, chunk.path_in_schema)
                else:
                    assert chunk.statistics is not None and chunk.statistics.has_min_max or chunk.statistics.null_count == group.num_rows, (path, chunk.path_in_schema)


def test_values_reproduce_what_the_initial_load_handles(base: source.SourceBase) -> None:
    """Os valores que a carga inicial tem de tratar, um por regra de ``docs/PLAN-STAGE-7.md``."""
    lancamentos = ds.dataset(base.root / "cad_lancamentos", format="parquet", partitioning="hive").to_table()
    contratos = ds.dataset(base.root / "cad_contratos", format="parquet", partitioning="hive").to_table()

    # ``valor`` tem três casas: arredondar a duas muda a maioria das linhas, e o par extremo está presente.
    valor = lancamentos.column("valor")
    assert pc.sum(pc.not_equal(pc.round(valor, 2), valor)).as_py() > lancamentos.num_rows // 2
    assert (pc.max(valor).as_py(), pc.min(valor).as_py()) == (source.EXTREME_VALOR, -source.EXTREME_VALOR)

    # O ``timestamp`` chega em nanossegundos (INT96) com a parte sub-microssegundo zerada.
    timestamp = lancamentos.column("timestamp")
    assert timestamp.type == pa.timestamp("ns")
    assert pc.all(pc.equal(pc.floor_temporal(timestamp, unit="microsecond"), timestamp)).as_py()

    # Ids ``int32`` esparsos até 1.113.599.996, pouco acima da metade do tipo.
    assert pc.max(lancamentos.column("id_lancamento")).as_py() == source.MAX_ID_LANCAMENTO < 2**31
    assert len(lancamentos.column("id_lancamento").unique()) == lancamentos.num_rows

    # ``data`` é o mês projetado, sempre depois de ``data_base`` e até 2026-12-31; ``meta`` é sempre nula.
    assert pc.all(pc.greater(lancamentos.column("data"), lancamentos.column("data_base"))).as_py()
    assert pc.max(lancamentos.column("data")).as_py() == source.PROJECTION_HORIZON
    assert lancamentos.column("meta").null_count == lancamentos.num_rows

    # O contrato sem cadastro e os lançamentos de 2026-01-31, data que ``cad_contratos`` não tem.
    assert source.ORPHAN_CONTRATO in set(lancamentos.column("contrato").to_pylist())
    assert source.ORPHAN_CONTRATO not in set(contratos.column("contrato").to_pylist())
    assert "2026-01-31" not in set(contratos.column("data_str").to_pylist())

    # As sete colunas de ``cad_contratos`` anuláveis nos arquivos e ``NOT NULL`` no modelo não têm nulo; ``data_assinatura`` tem em mais da metade.
    for column in ("sistema", "um", "to", "fonte", "taxa_juros_fixos", "data_primeira_amortizacao", "data_ultima_amortizacao"):
        assert contratos.column(column).null_count == 0, column
    assert contratos.column("data_assinatura").null_count > contratos.num_rows // 2

    # ``fator`` com cinco casas, ``data_fim_validade`` toda nula, e o registro das cargas com a partição em JSON.
    aliquotas = pq.read_table(base.files["cad_aliquotas"][0])
    assert 0.59895 in aliquotas.column("fator").to_pylist()
    assert aliquotas.column("data_fim_validade").null_count == 15
    status = pq.read_table(base.files["meta_update_status"][0])
    partitions = [json.loads(text) for text in status.column("partition").to_pylist() if text is not None]
    assert len(partitions) == 13 and status.column("partition").null_count == 8
    assert {"data": {"__type__": "date", "value": "2026-02-28"}} in partitions
    assert pq.read_table(base.files["alembic_version"][0]).column("version_num").to_pylist() == [source.ALEMBIC_REVISION]


def test_both_readers_see_the_partition_column_from_the_path(base: source.SourceBase) -> None:
    """O DuckDB e o PyArrow leem a pasta com partição Hive: as mesmas linhas, e a coluna do caminho tipada por cada leitor."""
    connection = duckdb.connect()
    for table, (column, values) in OBSERVED_PARTITIONS.items():
        glob = f"{base.root / table}/*/*.parquet"
        # O DuckDB converte o valor do caminho para DATE por padrão; só com hive_types_autocast=false ele fica VARCHAR.
        typed = connection.execute(f"SELECT typeof({column}) FROM read_parquet('{glob}', hive_partitioning=true) LIMIT 1").fetchone()
        as_text = connection.execute(f"SELECT typeof({column}) FROM read_parquet('{glob}', hive_partitioning=true, hive_types_autocast=false) LIMIT 1").fetchone()
        assert (typed, as_text) == (("DATE",), ("VARCHAR",)), table
        counted = dict(connection.execute(f"SELECT {column}::VARCHAR, count(*) FROM read_parquet('{glob}', hive_partitioning=true) GROUP BY 1").fetchall())
        assert counted == base.partition_rows[table], table

        dataset = ds.dataset(base.root / table, format="parquet", partitioning="hive")
        assert dataset.schema.field(column).type == pa.string(), table
        assert dataset.count_rows() == base.rows[table], table
        assert set(dataset.schema.names) == set(source.SCHEMAS[table].names) | {column}, table
