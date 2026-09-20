"""O DuckDB como sandbox: a API Python que a biblioteca usa.

Sem gravar arquivo: a configuração da conexão, Arrow na entrada e na saída, o leitor invalidado pelo
comando seguinte, o tipo ``DECIMAL`` inferido de uma amostra do pandas contra o fixado pelo esquema
Arrow, JSON, e o custo do ``executemany`` contra a carga por Arrow. Sob a raiz local (marcador
``local``): ``COPY ... TO`` com ``RETURN_STATS`` e o esquema físico do Parquet gravado, o ``COPY``
particionado por mês e um banco em arquivo com pasta de transbordo. O ``delta_scan`` está em
``delta.py``.
"""

from __future__ import annotations

import decimal
import time
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pytest

from conftest import LocalLocation, record
from delta import MONTHS, ROWS, sample_table


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    """Banco em memória com a configuração padrão; as extensões embutidas (json, parquet) dispensam LOAD."""
    connection = duckdb.connect()

    yield connection

    connection.close()


def test_connection_config() -> None:
    """As opções entram na abertura da conexão ou por ``SET``; ``duckdb_settings()`` mostra o valor em vigor."""
    connection = duckdb.connect(config={"threads": 2, "memory_limit": "512MB", "preserve_insertion_order": False})

    settings = dict(
        connection.execute(
            "SELECT name, value FROM duckdb_settings() WHERE name IN ('threads', 'memory_limit', 'preserve_insertion_order')"
        ).fetchall()
    )
    assert settings["threads"] == "2" and settings["preserve_insertion_order"] == "false"
    record("duckdb.memory_limit_512MB_as_reported", settings["memory_limit"])

    # O mesmo ajuste por comando, depois de aberta a conexão.
    connection.execute("SET threads = 1")
    assert connection.execute("SELECT current_setting('threads')").fetchone()[0] == 1

    connection.close()


def test_arrow_in_and_out(con: duckdb.DuckDBPyConnection) -> None:
    """Uma tabela Arrow registrada é consultada sem cópia; o resultado sai como tabela Arrow ou como leitor em lotes."""
    sample = sample_table().slice(0, 1000)

    # register expõe a tabela Arrow como uma view; CREATE TABLE AS a materializa no banco.
    con.register("entrada", sample)
    con.execute("CREATE TABLE operacoes AS SELECT * FROM entrada")

    # INSERT ... BY NAME casa as colunas pelo nome, em qualquer ordem.
    con.execute(
        "INSERT INTO operacoes BY NAME SELECT valor, id_operacao + 1000 AS id_operacao, mes, data_ref, id_cliente, descricao FROM entrada"
    )
    assert con.execute("SELECT count(*) FROM operacoes").fetchone()[0] == 2000

    # to_arrow_table materializa o resultado; os tipos do DuckDB voltam como tipos Arrow.
    table = con.execute("SELECT * FROM operacoes ORDER BY id_operacao").to_arrow_table()
    assert table.num_rows == 2000
    assert table.schema.field("valor").type == pa.decimal128(18, 2)
    assert table.schema.field("data_ref").type == pa.timestamp("us")

    # to_arrow_reader entrega o resultado em lotes, sem materializar tudo.
    reader = con.execute("SELECT id_operacao FROM operacoes").to_arrow_reader(500)
    batches = list(reader)
    assert sum(batch.num_rows for batch in batches) == 2000
    record("duckdb.arrow_reader_batches_for_2000_rows_batch_500", len(batches))


def test_arrow_reader_is_invalidated_by_the_next_command(con: duckdb.DuckDBPyConnection) -> None:
    """O leitor pertence à consulta em curso: outro comando na mesma conexão o esvazia, sem erro.

    O leitor precisa ser consumido antes do próximo comando; o que sobrar some em silêncio.
    """
    con.execute("CREATE TABLE numeros AS SELECT range AS id FROM range(100000)")

    reader = con.execute("SELECT id FROM numeros").to_arrow_reader(2048)
    first = reader.read_next_batch()
    assert 0 < first.num_rows < 100000

    # Um comando qualquer na mesma conexão encerra a consulta que alimentava o leitor.
    con.execute("SELECT 1").fetchall()
    rest = reader.read_all()
    assert rest.num_rows == 0
    record("duckdb.arrow_reader_rows_after_next_command", rest.num_rows)


def test_decimal_from_pandas_sample_versus_arrow_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Uma coluna ``object`` de ``Decimal`` recebe o tipo dos valores presentes; o esquema Arrow fixa ``DECIMAL(18,2)``."""
    values = [decimal.Decimal("12345.67")] * 1500 + [decimal.Decimal("123456789.01")]
    frame = pd.DataFrame({"valor": values})

    # O DuckDB infere o tipo dos valores do DataFrame, não do contrato: um DECIMAL do tamanho dos dados de hoje.
    con.register("amostra", frame)
    inferred = con.execute("DESCRIBE SELECT valor FROM amostra").fetchone()[1]
    record("duckdb.decimal_inferred_from_sample", inferred)
    assert inferred.startswith("DECIMAL(") and inferred != "DECIMAL(18,2)"

    try:
        con.execute("CREATE TABLE inferida AS SELECT * FROM amostra")
        outcome = f"aceita: max = {con.execute('SELECT max(valor) FROM inferida').fetchone()[0]}"
    except duckdb.Error as error:
        outcome = f"{type(error).__name__}: {str(error).splitlines()[0][:120]}"
    record("duckdb.decimal_inferred_materialize", outcome)

    # A conversão para Arrow com o esquema do contrato fixa o tipo antes de o DuckDB ver os dados.
    fixed = pa.Table.from_pandas(frame, schema=pa.schema([("valor", pa.decimal128(18, 2))]), preserve_index=False)
    con.register("contrato", fixed)
    assert con.execute("DESCRIBE SELECT valor FROM contrato").fetchone()[1] == "DECIMAL(18,2)"
    assert con.execute("SELECT max(valor) FROM contrato").fetchone()[0] == decimal.Decimal("123456789.01")


def test_json_column(con: duckdb.DuckDBPyConnection) -> None:
    """A coluna ``JSON`` valida na entrada e responde a ``->>``, ``json_extract`` e ``json_valid``."""
    con.execute("CREATE TABLE eventos (id BIGINT, meta JSON)")
    con.execute("""INSERT INTO eventos VALUES (1, '{"sistema": "A", "ativo": true}'), (2, NULL)""")

    assert con.execute("SELECT meta->>'$.sistema', json_extract(meta, '$.ativo') FROM eventos WHERE id = 1").fetchone() == ("A", "true")
    assert con.execute("""SELECT json_valid('{"a": 1}'), json_valid('{a}')""").fetchone() == (True, False)

    # Texto inválido é recusado no cast para JSON; a auditoria da biblioteca confere json_valid antes.
    with pytest.raises(duckdb.Error, match="Malformed JSON"):
        con.execute("SELECT '{a}'::JSON").fetchall()

    # Na saída em Arrow, JSON vira string: o tipo é do motor, não dos dados.
    table = con.execute("SELECT meta FROM eventos").to_arrow_table()
    record("duckdb.json_column_arrow_type", str(table.schema.field("meta").type))


def test_executemany_versus_arrow(con: duckdb.DuckDBPyConnection) -> None:
    """``executemany`` faz uma ida por linha; a tabela Arrow entra num comando só."""
    rows = 5000
    con.execute("CREATE TABLE lote (id BIGINT, valor DECIMAL(18,2))")

    started = time.perf_counter()
    con.executemany("INSERT INTO lote VALUES (?, ?)", [(i, decimal.Decimal(i) / 100) for i in range(rows)])
    by_row = time.perf_counter() - started

    con.register(
        "lote_arrow",
        pa.table({"id": pa.array(range(rows), pa.int64()), "valor": pa.array([decimal.Decimal(i) / 100 for i in range(rows)], pa.decimal128(18, 2))}),
    )
    started = time.perf_counter()
    con.execute("INSERT INTO lote SELECT * FROM lote_arrow")
    by_arrow = time.perf_counter() - started

    record("duckdb.timing.executemany_5000_rows", f"{by_row:.3f} s")
    record("duckdb.timing.arrow_insert_5000_rows", f"{by_arrow:.3f} s")
    assert con.execute("SELECT count(*) FROM lote").fetchone()[0] == 2 * rows
    assert by_arrow < by_row


@pytest.mark.local
def test_copy_to_parquet_with_return_stats(local_location: LocalLocation) -> None:
    """``COPY ... TO`` grava o Parquet e devolve contagem, tamanho e estatísticas por coluna, sem reabrir o arquivo."""
    folder = Path(local_location.child("duckdb"))
    folder.mkdir(exist_ok=True)
    path = folder / "amostra.parquet"

    con = duckdb.connect()
    con.register("amostra", sample_table().slice(0, 10_000))

    # RETURN_STATS devolve uma linha com o que a AddAction do Delta precisa.
    cursor = con.execute(f"COPY (SELECT * FROM amostra) TO '{path}' (FORMAT parquet, RETURN_STATS)")
    stats = dict(zip([column[0] for column in cursor.description], cursor.fetchone()))
    assert stats["count"] == 10_000
    assert stats["file_size_bytes"] == path.stat().st_size

    per_column = {name.strip('"'): values for name, values in stats["column_statistics"].items()}
    assert per_column["valor"]["null_count"] == "0" and per_column["valor"]["max"] == "99.99"

    # O esquema físico do arquivo: DECIMAL(18,2) como INT64, timestamp como INT64, toda coluna optional.
    schema = con.execute(f"SELECT name, type, repetition_type FROM parquet_schema('{path}')").fetchall()
    physical = {name: kind for name, kind, _ in schema}
    repetition = {name: kind for name, _, kind in schema}
    assert physical["valor"] == "INT64" and physical["data_ref"] == "INT64"
    assert repetition["id_operacao"] == "OPTIONAL"

    assert con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0] == 10_000
    con.close()


@pytest.mark.local
def test_copy_partition_by_month(local_location: LocalLocation) -> None:
    """``PARTITION_BY (mes)`` grava ``mes=.../data_0.parquet`` sem a coluna dentro do arquivo; a leitura Hive a restaura."""
    out = Path(local_location.child("duckdb/particionado"))

    con = duckdb.connect()
    con.register("amostra", sample_table().slice(ROWS // 2 - 1000, 2000))  # 1.000 linhas de cada mês
    con.execute(f"COPY (SELECT * FROM amostra) TO '{out}' (FORMAT parquet, PARTITION_BY (mes))")

    files = sorted(str(file.relative_to(out)) for file in out.rglob("*.parquet"))
    assert files == [f"mes={MONTHS[0]}/data_0.parquet", f"mes={MONTHS[1]}/data_0.parquet"]

    # O arquivo não tem a coluna de partição, a convenção do Delta. read_parquet a devolve mesmo num
    # arquivo só, porque detecta o layout Hive no caminho; parquet_schema mostra o que está gravado.
    columns = [row[0] for row in con.execute(f"SELECT name FROM parquet_schema('{out / files[0]}') WHERE name <> 'duckdb_schema'").fetchall()]
    assert columns == ["id_operacao", "data_ref", "id_cliente", "valor", "descricao"]
    assert "mes" in [row[0] for row in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{out / files[0]}')").fetchall()]

    count = con.execute(f"SELECT count(*) FROM read_parquet('{out}/*/*.parquet', hive_partitioning = true) WHERE mes = '{MONTHS[1]}'").fetchone()[0]
    assert count == 1000
    con.close()


@pytest.mark.local
def test_database_file_and_temp_directory(local_location: LocalLocation) -> None:
    """O sandbox em arquivo sobrevive ao fechamento e reabre só de leitura; a pasta de transbordo é configurável."""
    folder = Path(local_location.child("duckdb"))
    folder.mkdir(exist_ok=True)
    database = folder / "sandbox.duckdb"

    con = duckdb.connect(str(database), config={"temp_directory": str(folder / "spill")})
    con.register("amostra", sample_table().slice(0, 1000))
    con.execute("CREATE TABLE trabalho AS SELECT * FROM amostra")
    assert con.execute("SELECT current_setting('temp_directory')").fetchone()[0] == str(folder / "spill")
    con.close()

    assert database.exists()
    again = duckdb.connect(str(database), read_only=True)
    assert again.execute("SELECT count(*) FROM trabalho").fetchone()[0] == 1000
    again.close()
