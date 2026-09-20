"""O PyArrow como forma canônica dos dados: esquema com metadados, cast seguro, leitores em lote e Parquet.

Sem gravar arquivo: o esquema com nulidade, comentários e ``PARQUET:field_id`` nos metadados de campo,
``Table.from_pylist`` (que exige dicionários), o cast seguro que recusa perda de dados e o
``RecordBatchReader`` consumido uma vez. Sob a raiz local (marcador ``local``): o ``ParquetWriter``
lote a lote com um row group por lote e o rodapé lido de volta, o mesmo conteúdo gravado pelo DuckDB
para comparar os tipos físicos, e o dataset particionado ao estilo Hive.
"""

from __future__ import annotations

import datetime as dt
import decimal
import re
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pytest

from conftest import LocalLocation, record
from poc_delta import MONTHS, ROWS, sample_table


def contract_schema() -> pa.Schema:
    """Esquema com nulidade, comentário e ``PARQUET:field_id`` por campo e metadados da tabela."""
    return pa.schema(
        [
            pa.field("id_operacao", pa.int64(), nullable=False, metadata={"PARQUET:field_id": "1", "comment": "Identificador"}),
            pa.field("data_ref", pa.timestamp("us"), nullable=False, metadata={"PARQUET:field_id": "2"}),
            pa.field("valor", pa.decimal128(18, 2), nullable=False, metadata={"PARQUET:field_id": "3"}),
            pa.field("descricao", pa.string(), metadata={"PARQUET:field_id": "4"}),
        ],
        metadata={"serialize_db_version": "1"},
    )


def test_schema_metadata_and_from_pylist() -> None:
    """Metadados de campo e de esquema são bytes; ``from_pylist`` quer dicionários, e tuplas viram nulos sem erro."""
    schema = contract_schema()
    assert schema.metadata == {b"serialize_db_version": b"1"}
    assert schema.field("id_operacao").metadata[b"comment"] == b"Identificador"
    assert not schema.field("id_operacao").nullable and schema.field("descricao").nullable

    row = {"id_operacao": 1, "data_ref": dt.datetime(2026, 1, 1), "valor": decimal.Decimal("1.50"), "descricao": None}
    table = pa.Table.from_pylist([row], schema=schema)
    assert table.num_rows == 1 and table.schema.equals(schema, check_metadata=True)

    # A mesma linha como tupla: nenhuma chave casa com um nome, e cada coluna recebe nulo.
    tuples = pa.Table.from_pylist([tuple(row.values())], schema=schema)
    assert tuples.num_rows == 1 and all(column.null_count == 1 for column in tuples.columns)

    # O Arrow não valida a nulidade declarada: nulo num campo nullable=False entra; a biblioteca confere null_count.
    violated = pa.Table.from_pylist([{"id_operacao": None}], schema=pa.schema([pa.field("id_operacao", pa.int64(), nullable=False)]))
    assert violated.column("id_operacao").null_count == 1


def test_safe_cast_refuses_data_loss() -> None:
    """O cast seguro (padrão) recusa perda de precisão, estouro e perda de resolução; comprimento de texto é conferido à parte."""
    target = pa.schema([("valor", pa.decimal128(18, 2))])
    wide = pa.table({"valor": pa.array([decimal.Decimal("1.2345")], pa.decimal128(20, 4))})
    with pytest.raises(pa.ArrowInvalid):
        wide.cast(target)
    assert wide.cast(target, safe=False).num_rows == 1

    with pytest.raises(pa.ArrowInvalid):
        pa.array([2**40], pa.int64()).cast(pa.int32())

    # Nanossegundos (o padrão do pandas) só cabem em microssegundos quando a parte perdida é zero.
    with pytest.raises(pa.ArrowInvalid):
        pa.array([1], pa.timestamp("ns")).cast(pa.timestamp("us"))
    assert pa.array([1000], pa.timestamp("ns")).cast(pa.timestamp("us"))[0].as_py() == dt.datetime(1970, 1, 1, 0, 0, 0, 1)

    # O tipo string não tem comprimento: String(200) do contrato é verificado com utf8_length.
    lengths = pc.utf8_length(pa.array(["ok", "x" * 201]))
    assert pc.max(lengths).as_py() == 201


def test_record_batch_reader_is_consumed_once() -> None:
    """O ``RecordBatchReader`` entrega cada lote uma vez, de uma lista ou de um gerador, sem materializar o todo."""
    batches = [pa.RecordBatch.from_pydict({"id": pa.array(range(i * 10, i * 10 + 10), pa.int64())}) for i in range(3)]

    reader = pa.RecordBatchReader.from_batches(batches[0].schema, batches)
    assert [batch.num_rows for batch in reader] == [10, 10, 10]
    assert reader.read_all().num_rows == 0  # já consumido

    def generate():
        for batch in batches:
            yield batch

    reader = pa.RecordBatchReader.from_batches(batches[0].schema, generate())
    assert reader.schema == batches[0].schema
    assert reader.read_all().num_rows == 30


@pytest.mark.local
def test_parquet_writer_row_groups_and_footer(local_location: LocalLocation) -> None:
    """``ParquetWriter`` grava um row group por lote; o rodapé traz tipos físicos, obrigatoriedade, ids e estatísticas."""
    folder = Path(local_location.child("pyarrow"))
    folder.mkdir(exist_ok=True)

    # O esquema do contrato: não anulável exceto descricao, com field_id em cada campo.
    sample = sample_table().slice(0, 30_000)
    fields = [
        pa.field(field.name, field.type, nullable=field.name == "descricao", metadata={"PARQUET:field_id": str(index + 1)})
        for index, field in enumerate(sample.schema)
    ]
    schema = pa.schema(fields)
    table = sample.cast(schema)

    path = folder / "operacoes.parquet"
    with pq.ParquetWriter(path, schema) as writer:
        for batch in table.to_batches(max_chunksize=10_000):
            writer.write_batch(batch)

    metadata = pq.read_metadata(path)
    assert metadata.num_row_groups == 3 and metadata.num_rows == 30_000

    # O PyArrow grava DECIMAL(18,2) como FIXED_LEN_BYTE_ARRAY e mantém mínimo e máximo por coluna.
    valor = metadata.row_group(0).column(schema.get_field_index("valor"))
    assert valor.physical_type == "FIXED_LEN_BYTE_ARRAY" and valor.statistics.has_min_max
    record("pyarrow.parquet_valor_statistics_row_group_0", f"min={valor.statistics.min} max={valor.statistics.max}")

    parquet_schema = str(pq.ParquetFile(path).schema)
    assert "required int64 field_id=1 id_operacao" in parquet_schema
    assert "optional binary field_id=6 descricao" in parquet_schema
    assert pq.read_schema(path).field("valor").metadata[b"PARQUET:field_id"] == b"5"

    # O DuckDB grava o mesmo conteúdo com DECIMAL como INT64 e toda coluna optional.
    con = duckdb.connect()
    con.register("t", table)
    duck_path = folder / "operacoes_duckdb.parquet"
    con.execute(f"COPY (SELECT * FROM t) TO '{duck_path}' (FORMAT parquet)")
    con.close()

    duck_valor = pq.read_metadata(duck_path).row_group(0).column(schema.get_field_index("valor"))
    assert duck_valor.physical_type == "INT64"
    assert re.search(r"optional int64( field_id=-?\d+)? id_operacao", str(pq.ParquetFile(duck_path).schema))


@pytest.mark.local
def test_hive_partitioned_dataset(local_location: LocalLocation) -> None:
    """``write_to_dataset`` grava ``mes=.../`` sem a coluna no arquivo; ``dataset`` a lê de volta e poda pelo filtro."""
    root = Path(local_location.child("pyarrow/dataset"))
    two_months = sample_table().slice(ROWS // 2 - 1000, 2000)

    pq.write_to_dataset(two_months, root, partition_cols=["mes"])
    folders = sorted(path.name for path in root.iterdir() if path.is_dir())
    assert folders == [f"mes={MONTHS[0]}", f"mes={MONTHS[1]}"]

    file = next((root / folders[0]).glob("*.parquet"))
    assert "mes" not in pq.read_schema(file).names

    dataset = ds.dataset(root, partitioning="hive")
    assert dataset.schema.field("mes").type == pa.string()
    assert dataset.to_table(filter=ds.field("mes") == MONTHS[1]).num_rows == 1000


@pytest.mark.local
def test_parquet_streaming_read_filters_and_pandas(local_location: LocalLocation) -> None:
    """``iter_batches`` lê por lotes sem carregar o arquivo; ``filters`` e ``columns`` reduzem a leitura; pandas recebe tipos Arrow."""
    folder = Path(local_location.child("pyarrow"))
    folder.mkdir(exist_ok=True)
    path = folder / "leitura.parquet"

    two_months = sample_table().slice(ROWS // 2 - 1000, 2000)
    pq.write_table(two_months, path, row_group_size=500)

    # Cada lote é um RecordBatch; o arquivo inteiro nunca fica na memória de uma vez.
    parquet = pq.ParquetFile(path)
    assert parquet.metadata.num_row_groups == 4
    assert [batch.num_rows for batch in parquet.iter_batches(batch_size=800)] == [800, 800, 400]

    # filters é empurrado aos row groups pelas estatísticas; columns lê só as colunas pedidas.
    february = pq.read_table(path, filters=[("mes", "=", MONTHS[1])], columns=["id_operacao", "valor"])
    assert february.num_rows == 1000 and february.column_names == ["id_operacao", "valor"]

    # A conversão para pandas com types_mapper preserva decimal e date como tipos Arrow do pandas.
    frame = february.to_pandas(types_mapper=pd.ArrowDtype)
    assert str(frame["valor"].dtype) == "decimal128(18, 2)[pyarrow]"
    assert frame["valor"].iloc[0] == two_months.column("valor")[1000].as_py()
