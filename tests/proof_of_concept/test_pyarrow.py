"""O PyArrow como forma canônica dos dados: esquema com metadados, cast seguro, leitores em lote e Parquet.

Sem gravar arquivo: o esquema com nulidade, comentários e ``PARQUET:field_id`` nos metadados de campo,
``Table.from_pylist`` (que exige dicionários), o cast seguro que recusa perda de dados, o
``RecordBatchReader`` consumido uma vez, o ``RecordBatch`` como unidade da fronteira (o ``cast`` do
lote, ``to_batches`` e ``from_batches`` sem cópia, o ciclo do lote com o pandas) e o leitor de
``from_batches`` que não confere os lotes contra o esquema declarado. Sob a raiz local (marcador ``local``): o ``ParquetWriter``
lote a lote com um row group por lote e o rodapé lido de volta, o mesmo conteúdo gravado pelo DuckDB
para comparar os tipos físicos, e o dataset particionado ao estilo Hive.
"""

from __future__ import annotations

import datetime as dt
import decimal
import gc
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


def addresses(column: pa.ChunkedArray | pa.Array) -> set[int]:
    """Os endereços dos buffers de uma coluna: iguais dos dois lados quando a conversão não copiou."""
    chunks = column.chunks if isinstance(column, pa.ChunkedArray) else [column]
    return {buffer.address for chunk in chunks for buffer in chunk.buffers() if buffer is not None}


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

    # double para decimal arredonda o valor binário exato (2,675 é 2,67499...) sem acusar a perda, mesmo com
    # safe=True; o valor representável na escala é o que pc.round devolve igual, e pc.round difere do cast em 2,675.
    floats = pa.array([1.236, 2.675, 1234.56, 0.29])
    assert [str(value) for value in floats.cast(pa.decimal128(18, 2))] == ["1.24", "2.67", "1234.56", "0.29"]
    assert pc.equal(pc.round(floats, 2), floats).to_pylist() == [False, False, True, True]
    assert [str(value) for value in pc.round(floats, 2)] == ["1.24", "2.68", "1234.56", "0.29"]

    # timestamp para date descarta a hora sem erro; a ida e volta acusa.
    stamps = pa.array([dt.datetime(2026, 8, 1, 12, 30), dt.datetime(2026, 8, 2)], pa.timestamp("us"))
    assert pc.equal(stamps.cast(pa.date32()).cast(pa.timestamp("us")), stamps).to_pylist() == [False, True]

    # Table.cast recusa nulo em campo não anulável e exige os mesmos nomes na mesma ordem; inteiro em
    # decimal128(18, 2) pede precisão 21 e passa por (21, 2).
    with pytest.raises(ValueError, match="non-nullable"):
        pa.table({"n": pa.array([1, None], pa.int64())}).cast(pa.schema([pa.field("n", pa.int64(), nullable=False)]))
    with pytest.raises(ValueError, match="field names"):
        pa.table({"b": [1], "a": [2]}).cast(pa.schema([("a", pa.int64()), ("b", pa.int64())]))
    with pytest.raises(pa.ArrowInvalid):
        pa.array([1]).cast(pa.decimal128(18, 2))
    assert pa.array([1]).cast(pa.decimal128(21, 2)).cast(pa.decimal128(18, 2)).to_pylist() == [decimal.Decimal("1.00")]

    # Uma coluna de dicts vira struct com a união das chaves, e um dict não entra em string: JSON chega serializado.
    documents = pa.Table.from_pandas(pd.DataFrame({"atributos": [{"k": 1}, {"k": 2, "x": "y"}]}), preserve_index=False)
    assert documents.column("atributos").to_pylist() == [{"k": 1, "x": None}, {"k": 2, "x": "y"}]
    with pytest.raises(pa.ArrowTypeError):
        pa.array([{"k": 1}], pa.string())


def test_arrow_table_round_trips_through_pandas_without_copy() -> None:
    """``to_pandas(types_mapper=pd.ArrowDtype)`` e ``from_pandas`` compartilham os buffers e mantêm os tipos do contrato; o backend numpy os perde."""
    schema = pa.schema([
        pa.field("id_operacao", pa.int64(), nullable=False),
        pa.field("data_ref", pa.date32(), nullable=False),
        pa.field("valor", pa.decimal128(18, 2), nullable=False),
        pa.field("descricao", pa.string()),
    ])
    table = pa.table({
        "id_operacao": [1, 2],
        "data_ref": [dt.date(2026, 8, 1), dt.date(2026, 8, 2)],
        "valor": [decimal.Decimal("10.50"), decimal.Decimal("99999.99")],
        "descricao": ["a", None],
    }, schema=schema)

    # A ida com ArrowDtype não copia: os buffers do DataFrame são os da tabela, e os tipos são os do contrato.
    frame = table.to_pandas(types_mapper=pd.ArrowDtype)
    assert [str(dtype) for dtype in frame.dtypes] == ["int64[pyarrow]", "date32[day][pyarrow]", "decimal128(18, 2)[pyarrow]", "string[pyarrow]"]
    assert addresses(frame["valor"].array._pa_array) == addresses(table.column("valor"))

    # A volta também não copia; todo campo volta anulável, e o cast seguro devolve o esquema do contrato.
    back = pa.Table.from_pandas(frame, preserve_index=False)
    assert back.schema.types == schema.types and all(field.nullable for field in back.schema)
    assert addresses(back.column("valor")) == addresses(table.column("valor"))
    assert back.cast(schema).schema == schema

    # O backend numpy troca decimal e date por objetos Python e um inteiro com nulo por float64.
    default = table.to_pandas()
    assert [str(dtype) for dtype in default.dtypes] == ["int64", "object", "object", "str"]
    assert isinstance(default["valor"].iloc[0], decimal.Decimal)
    assert str(pa.table({"n": pa.array([1, None], pa.int64())}).to_pandas()["n"].dtype) == "float64"

    # A aritmética sobre decimal128[pyarrow] fica em decimal; um float no meio leva a double.
    assert str((frame["valor"] * decimal.Decimal("1.1")).dtype) == "decimal128(21, 3)[pyarrow]"
    assert str((frame["valor"] * 1.1).dtype) == "double[pyarrow]"
    assert frame["valor"].sum() == decimal.Decimal("100010.49")


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


def test_record_batch_cast_and_conversions_share_buffers() -> None:
    """``RecordBatch.cast`` recusa o que ``Table.cast`` recusa; ``to_batches``, ``from_batches`` e o ciclo do lote com o pandas por ``ArrowDtype`` compartilham os buffers."""
    schema = pa.schema([pa.field("n", pa.int64(), nullable=False), pa.field("valor", pa.decimal128(18, 2))])
    batch = pa.RecordBatch.from_pydict({"n": pa.array([1, None], pa.int64()), "valor": pa.array([decimal.Decimal("1.5000"), None], pa.decimal128(20, 4))})
    with pytest.raises(ValueError, match="non-nullable"):
        batch.cast(schema)
    with pytest.raises(ValueError, match="field names"):
        pa.RecordBatch.from_pydict({"b": [1], "a": [2]}).cast(pa.schema([("a", pa.int64()), ("b", pa.int64())]))
    wide = pa.RecordBatch.from_pydict({"valor": pa.array([decimal.Decimal("1.2345")], pa.decimal128(20, 4))})
    narrow = pa.schema([("valor", pa.decimal128(18, 2))])
    with pytest.raises(pa.ArrowInvalid):
        wide.cast(narrow)
    assert wide.cast(narrow, safe=False).column("valor").to_pylist() == [decimal.Decimal("1.23")]

    # to_batches fatia a tabela sem copiar, e from_batches a remonta sobre os mesmos buffers.
    table = sample_table()
    batches = table.to_batches(max_chunksize=100_000)
    assert [batch.num_rows for batch in batches] == [100_000] * 3
    assert addresses(batches[0].column("valor")) <= addresses(table.column("valor"))
    rebuilt = pa.Table.from_batches(batches)
    assert rebuilt.equals(table) and addresses(rebuilt.column("valor")) == addresses(table.column("valor"))

    # Um lote vai ao pandas e volta sem cópia, com os tipos do contrato, como a tabela inteira.
    frame = batches[0].to_pandas(types_mapper=pd.ArrowDtype)
    assert str(frame["valor"].dtype) == "decimal128(18, 2)[pyarrow]"
    assert addresses(frame["valor"].array._pa_array) == addresses(batches[0].column("valor"))
    returned = pa.RecordBatch.from_pandas(frame, preserve_index=False)
    assert returned.schema.types == batches[0].schema.types
    assert addresses(returned.column("valor")) == addresses(batches[0].column("valor"))


def test_record_batch_reader_from_batches_trusts_the_batches() -> None:
    """``from_batches`` não confere cada lote contra o esquema declarado: ``read_next_batch`` devolve o lote como veio, e só ``read_all`` acusa.

    ``close`` não chega ao gerador, que só termina quando o leitor é descartado; um objeto com
    ``__arrow_c_stream__`` é um leitor para ``from_stream`` (e para o ``register`` do DuckDB, em
    ``test_duckdb.py``).
    """
    declared = pa.schema([("id", pa.int64()), ("valor", pa.float64())])
    swapped = pa.RecordBatch.from_pydict({"valor": pa.array([1.0]), "id": pa.array([1], pa.int64())})

    reader = pa.RecordBatchReader.from_batches(declared, [swapped])
    assert reader.schema.names == ["id", "valor"]
    assert reader.read_next_batch().schema.names == ["valor", "id"]
    with pytest.raises(pa.ArrowInvalid, match="Schema at index 0 was different"):
        pa.RecordBatchReader.from_batches(declared, [swapped]).read_all()

    # close() não encerra o gerador: a leitura seguinte ainda entrega, e o finally roda no descarte.
    events: list[str] = []

    def generate():
        try:
            for k in range(3):
                events.append(f"lote {k}")
                yield pa.RecordBatch.from_pydict({"id": pa.array([k], pa.int64()), "valor": pa.array([0.0])})
        finally:
            events.append("finally")

    reader = pa.RecordBatchReader.from_batches(declared, generate())
    reader.read_next_batch()
    reader.close()
    assert events == ["lote 0"]
    assert reader.read_next_batch().num_rows == 1 and events == ["lote 0", "lote 1"]
    del reader
    gc.collect()
    assert events == ["lote 0", "lote 1", "finally"]

    class Stream:
        def __init__(self, source: pa.RecordBatchReader) -> None:
            self._source = source

        def __arrow_c_stream__(self, requested_schema: object = None) -> object:
            return self._source.__arrow_c_stream__(requested_schema)

    assert pa.RecordBatchReader.from_stream(Stream(pa.RecordBatchReader.from_batches(declared, generate()))).read_all().num_rows == 3


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
