"""O DuckDB como sandbox: a API Python que a biblioteca usa.

Sem gravar arquivo: a configuração da conexão, Arrow na entrada e na saída, o leitor invalidado pelo
comando seguinte e preservado num cursor próprio, a consulta em streaming (o primeiro lote antes do
fim, a memória de um lote, medida em subprocesso), o ``INSERT`` alimentado por um leitor sobre um
gerador Python (um comando só, e os lotes que o leitor não confere), o tipo ``DECIMAL`` inferido de
uma amostra do pandas contra o fixado pelo esquema Arrow, JSON, o custo do ``executemany`` contra a
carga por Arrow, as consultas da auditoria, a soma de controle que o ``NaN`` derruba e a que o
``DECIMAL(38, 6)`` torna independente das threads, o ``interrupt()`` chamado de outra thread e o
``cursor()`` aberto com uma consulta em curso. Sob a raiz local (marcador ``local``): ``COPY ...
TO`` com ``RETURN_STATS`` e o esquema físico do Parquet gravado, o ``RETURN_STATS`` com ``NaN``,
infinito e texto longo, o ``COPY`` particionado por mês e um banco em arquivo com pasta de
transbordo. O ``delta_scan`` está em ``poc_delta.py``.
"""

from __future__ import annotations

import decimal
import json
import subprocess
import sys
import threading
import time
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pytest

from conftest import LocalLocation, record
from poc_delta import MONTHS, ROWS, StreamOnly, sample_table


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


def test_arrow_reader_on_its_own_cursor_survives_commands_on_another(con: duckdb.DuckDBPyConnection) -> None:
    """O leitor preso a um cursor entrega o snapshot da sua consulta enquanto outro cursor insere na mesma tabela, cria, altera e apaga tabelas.

    É o que isola o ``stream`` da etapa 4 dos comandos que a thread do cliente roda no cursor dela.
    O cursor fechado no meio da leitura e o custo de abrir um cursor são leituras do relatório.
    """
    con.execute("CREATE TABLE numeros AS SELECT range AS id FROM range(1_000_000)")
    reading, writing = con.cursor(), con.cursor()
    reader = reading.execute("SELECT id FROM numeros").to_arrow_reader(100_000)
    first = reader.read_next_batch()

    writing.execute("INSERT INTO numeros SELECT range + 1_000_000 FROM range(10)")
    writing.execute("CREATE TABLE outra AS SELECT 1 AS x")
    writing.execute("ALTER TABLE numeros ADD COLUMN y INTEGER")
    writing.execute("DROP TABLE outra")
    assert first.num_rows + reader.read_all().num_rows == 1_000_000  # o snapshot da consulta, sem as dez linhas
    assert writing.execute("SELECT count(*) FROM numeros").fetchone()[0] == 1_000_010

    reader = reading.execute("SELECT id FROM numeros").to_arrow_reader(100_000)
    reader.read_next_batch()
    reading.close()
    record("duckdb.arrow_reader_rows_after_its_cursor_closed", reader.read_all().num_rows)
    started = time.perf_counter()
    for _ in range(100):
        con.cursor().close()
    record("duckdb.cursor_open_and_close_100", f"{(time.perf_counter() - started) * 1e3:.1f} ms")
    writing.close()


# Roda num subprocesso, um cenário por chamada: a memória máxima do processo depende só do cenário.
MEMORY_PROBE = r"""
import json, resource, sys, time
import duckdb
scenario, rows = sys.argv[1], int(sys.argv[2])
con = duckdb.connect(config={"threads": 2})
sql = f"SELECT range AS id, range % 97 AS m, 'x' || (range % 1000) AS s FROM range({rows})"
started = time.perf_counter()
if scenario == "table":
    n = con.execute(sql).to_arrow_table().num_rows
else:
    n = sum(batch.num_rows for batch in con.execute(sql).to_arrow_reader(100_000))
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1e6 if sys.platform == "darwin" else 1e3)
print(json.dumps({"rows": n, "seconds": round(time.perf_counter() - started, 3), "peak_mb": round(peak)}))
"""


def test_streaming_query_starts_before_the_end_and_bounds_memory() -> None:
    """Sem ``ORDER BY`` o primeiro lote chega antes de a consulta terminar, e o processo fica no tamanho de um lote; com ``ORDER BY`` a ordenação inteira precede o primeiro lote.

    A memória é medida num subprocesso por cenário (``ru_maxrss``): a tabela inteira contra o leitor
    em lotes, sobre as mesmas linhas.
    """
    rows = 10_000_000
    con = duckdb.connect(config={"threads": 2})
    sql = f"SELECT range AS id, range % 97 AS m, 'x' || (range % 1000) AS s FROM range({rows})"

    def timing(query: str) -> tuple[float, float, float, int]:
        started = time.perf_counter()
        reader = con.execute(query).to_arrow_reader(100_000)
        executed = time.perf_counter()
        first = reader.read_next_batch()
        first_batch = time.perf_counter()
        count = first.num_rows + sum(batch.num_rows for batch in reader)
        return executed - started, first_batch - executed, time.perf_counter() - started, count

    executed, first, total, count = timing(sql)
    assert count == rows and executed + first < total / 5
    record("duckdb.stream_10M_rows_unordered", f"execute {executed:.3f} s, primeiro lote {first:.3f} s, total {total:.3f} s")

    executed, first, total, count = timing(sql + " ORDER BY m, id")
    assert count == rows and executed > total / 2
    record("duckdb.stream_10M_rows_ordered", f"execute {executed:.3f} s, primeiro lote {first:.3f} s, total {total:.3f} s")
    con.close()

    peaks = {}
    for scenario in ("table", "stream"):
        completed = subprocess.run([sys.executable, "-c", MEMORY_PROBE, scenario, str(rows)], capture_output=True, text=True, check=True)
        peaks[scenario] = json.loads(completed.stdout)
    record("duckdb.peak_rss_10M_rows", {scenario: f"{reading['peak_mb']} MB em {reading['seconds']} s" for scenario, reading in peaks.items()})
    assert peaks["stream"]["rows"] == peaks["table"]["rows"] == rows
    assert peaks["stream"]["peak_mb"] < peaks["table"]["peak_mb"] / 2


# O stream da sessão única: o leitor inteiro gravado num arquivo Arrow IPC com LZ4, e o arquivo lido
# lote a lote. Roda num subprocesso, como MEMORY_PROBE, para a memória máxima ser só dele.
SPOOL_PROBE = r"""
import json, os, resource, sys, threading, time
import duckdb, pyarrow as pa
rows, folder = int(sys.argv[1]), sys.argv[2]
con = duckdb.connect(config={"threads": 2})
sql = f"SELECT range AS id, range % 97 AS m, 'x' || (range % 1000) AS s FROM range({rows})"
path = os.path.join(folder, "transbordo.arrow")
options = pa.ipc.IpcWriteOptions(compression="lz4")
condition = threading.Condition()
progress = {"written": 0, "done": False, "query_seconds": None}

def produce():
    reader = con.execute(sql).to_arrow_reader(100_000)
    with pa.OSFile(path, "wb") as sink, pa.ipc.new_stream(sink, reader.schema, options=options) as writer:
        for batch in reader:
            writer.write_batch(batch)
            with condition:
                progress["written"] += 1
                condition.notify_all()
    with condition:
        progress["done"], progress["query_seconds"] = True, time.perf_counter() - started
        condition.notify_all()

started = time.perf_counter()
threading.Thread(target=produce).start()
with condition:
    condition.wait_for(lambda: progress["written"] > 0 or progress["done"])
first = time.perf_counter() - started
n = read = 0
with pa.OSFile(path, "rb") as source:
    reader = pa.ipc.open_stream(source)
    while True:
        with condition:
            condition.wait_for(lambda: progress["written"] > read or progress["done"])
            if progress["written"] == read:
                break
        n += reader.read_next_batch().num_rows
        read += 1
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1e6 if sys.platform == "darwin" else 1e3)
print(json.dumps({"rows": n, "seconds": round(time.perf_counter() - started, 3), "first_batch_seconds": round(first, 3),
                  "query_seconds": round(progress["query_seconds"], 3), "file_mb": round(os.path.getsize(path) / 1e6), "peak_mb": round(peak)}))
"""


@pytest.mark.local
def test_spooled_stream_bounds_memory(local_location: LocalLocation) -> None:
    """O resultado gravado lote a lote num arquivo Arrow IPC com LZ4 por uma thread, e lido lote a lote enquanto ela grava, mantém o processo no tamanho de um lote, como o leitor direto.

    É o ``stream`` da sessão única (``test_parallel.py``): a thread roda a consulta sob o lock e grava
    cada lote assim que o DuckDB o entrega, e o cliente lê cada lote gravado sem a sessão. O tempo
    até o primeiro lote, o tempo da consulta, o tamanho do arquivo e a memória máxima vão para o
    relatório; a asserção é a mesma do leitor direto, menos da metade da memória da tabela inteira.
    """
    rows = 10_000_000
    folder = Path(local_location.child("transbordo_memoria"))
    folder.mkdir()
    completed = subprocess.run([sys.executable, "-c", SPOOL_PROBE, str(rows), str(folder)], capture_output=True, text=True, check=True)
    spooled = json.loads(completed.stdout)
    table = json.loads(subprocess.run([sys.executable, "-c", MEMORY_PROBE, "table", str(rows)], capture_output=True, text=True, check=True).stdout)
    record(
        "duckdb.spooled_stream_10M_rows",
        f"{spooled['peak_mb']} MB em {spooled['seconds']} s (primeiro lote em {spooled['first_batch_seconds']} s, consulta em {spooled['query_seconds']} s, "
        f"{spooled['file_mb']} MB de arquivo); a tabela inteira, {table['peak_mb']} MB",
    )
    assert spooled["rows"] == table["rows"] == rows
    assert spooled["peak_mb"] < table["peak_mb"] / 2


def test_insert_from_a_reader_is_one_statement_and_trusts_the_batches(con: duckdb.DuckDBPyConnection) -> None:
    """Um ``INSERT ... SELECT`` de um ``RecordBatchReader`` sobre um gerador Python é um comando só: a falha do gerador deixa a tabela como estava.

    O ``arrow_scan`` puxa o fluxo por uma thread de leitura antecipada do Arrow, que chama o gerador
    em outra thread, puxa lotes além do que o comando consumiu e continua depois de o comando falhar:
    o buffer foge da fila de quem alimenta o gerador, e é por isso que o ``Loader`` de
    ``test_parallel.py`` insere lote a lote em vez de entregar um gerador ao DuckDB. O leitor não
    confere os lotes contra o esquema declarado, e o ``arrow_scan`` os lê pelo esquema declarado: um
    lote com as colunas em outra ordem entra com os bytes trocados, sem erro, e o ``cast`` de cada
    lote para o esquema, antes do leitor, é a barreira. A nulidade do esquema Arrow não é conferida; a
    coluna ``NOT NULL`` do DuckDB é. Um objeto com ``__arrow_c_stream__`` entra por ``register`` como
    um leitor.
    """
    schema = pa.schema([("id", pa.int64()), ("valor", pa.float64())])
    con.execute("CREATE TABLE destino (id BIGINT, valor DOUBLE)")
    threads: set[int] = set()
    delivered: list[int] = []

    def generate(count: int, fail_at: int | None = None) -> Iterator[pa.RecordBatch]:
        for k in range(count):
            threads.add(threading.get_ident())
            if k == fail_at:
                raise RuntimeError(f"falha do cliente no lote {k}")
            delivered.append(k)
            yield pa.RecordBatch.from_pydict({"id": pa.array(range(k * 1000, (k + 1) * 1000), pa.int64()), "valor": pa.array([float(k)] * 1000)})

    con.register("entrada", pa.RecordBatchReader.from_batches(schema, generate(50)))
    con.execute("INSERT INTO destino BY NAME SELECT * FROM entrada")
    con.unregister("entrada")
    assert con.execute("SELECT count(*), sum(valor) FROM destino").fetchone() == (50_000, 1_225_000.0)
    record("duckdb.generator_pulled_by", "a thread do chamador" if threads == {threading.get_ident()} else f"outra thread ({len(threads)}, a leitura antecipada do Arrow)")

    # O gerador falha no lote 20: o comando falha inteiro, com a mensagem do cliente, e a contagem não muda.
    delivered.clear()
    con.register("entrada", pa.RecordBatchReader.from_batches(schema, generate(50, fail_at=20)))
    with pytest.raises(duckdb.InvalidInputException, match="falha do cliente no lote 20"):
        con.execute("INSERT INTO destino BY NAME SELECT * FROM entrada")
    con.unregister("entrada")
    assert con.execute("SELECT count(*) FROM destino").fetchone()[0] == 50_000 and len(delivered) == 20

    # O comando falha no primeiro lote (valor fora do INTEGER), e a leitura antecipada segue puxando o gerador.
    con.execute("CREATE TABLE estreita (id INTEGER, valor DOUBLE)")
    delivered.clear()

    def generate_wide(count: int) -> Iterator[pa.RecordBatch]:
        for k in range(count):
            delivered.append(k)
            yield pa.RecordBatch.from_pydict({"id": pa.array([2**40] * 1000, pa.int64()), "valor": pa.array([0.0] * 1000)})

    con.register("entrada", pa.RecordBatchReader.from_batches(schema, generate_wide(50)))
    with pytest.raises(duckdb.ConversionException):
        con.execute("INSERT INTO estreita BY NAME SELECT * FROM entrada")
    at_failure = len(delivered)
    time.sleep(0.5)
    con.unregister("entrada")
    assert len(delivered) > 1
    record("duckdb.batches_pulled_ahead_of_a_failed_insert", f"{at_failure} na falha, {len(delivered)} meio segundo depois")

    # Os lotes que o leitor não confere: a ordem trocada corrompe em silêncio; a coluna a mais falha.
    con.execute("DELETE FROM destino")
    swapped = pa.RecordBatch.from_pydict({"valor": pa.array([1.0]), "id": pa.array([1], pa.int64())})
    con.register("entrada", pa.RecordBatchReader.from_batches(schema, [swapped]))
    con.execute("INSERT INTO destino BY NAME SELECT * FROM entrada")
    con.unregister("entrada")
    corrupted = con.execute("SELECT id, valor FROM destino").fetchall()
    assert len(corrupted) == 1 and corrupted != [(1, 1.0)]
    record("duckdb.batch_with_swapped_columns_read_as", str(corrupted))
    extra = pa.RecordBatch.from_pydict({"id": pa.array([1], pa.int64()), "valor": pa.array([1.0]), "x": ["z"]})
    con.register("entrada", pa.RecordBatchReader.from_batches(schema, [extra]))
    with pytest.raises(duckdb.InvalidInputException, match="3 children, expected 2"):
        con.execute("INSERT INTO destino BY NAME SELECT * FROM entrada")
    con.unregister("entrada")

    # A nulidade declarada no esquema do leitor não é conferida; a coluna NOT NULL do DuckDB é.
    strict = pa.schema([pa.field("id", pa.int64(), nullable=False), ("valor", pa.float64())])
    with_null = pa.RecordBatch.from_pydict({"id": pa.array([1, None], pa.int64()), "valor": pa.array([1.0, 2.0])})
    con.execute("DELETE FROM destino")
    con.register("entrada", pa.RecordBatchReader.from_batches(strict, [with_null]))
    con.execute("INSERT INTO destino BY NAME SELECT * FROM entrada")
    con.unregister("entrada")
    assert con.execute("SELECT count(*) FILTER (WHERE id IS NULL) FROM destino").fetchone()[0] == 1
    con.execute("CREATE TABLE estrito (id BIGINT NOT NULL, valor DOUBLE)")
    con.register("entrada", pa.RecordBatchReader.from_batches(schema, [with_null]))
    with pytest.raises(duckdb.ConstraintException, match="NOT NULL constraint failed"):
        con.execute("INSERT INTO estrito BY NAME SELECT * FROM entrada")
    con.unregister("entrada")

    # Um objeto que só expõe __arrow_c_stream__ entra por register como um leitor.
    con.register("entrada", StreamOnly(pa.RecordBatchReader.from_batches(schema, generate(3))))
    con.execute("INSERT INTO estrito BY NAME SELECT * FROM entrada")
    con.unregister("entrada")
    assert con.execute("SELECT count(*) FROM estrito").fetchone()[0] == 3000


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
def test_return_stats_leave_nan_out_and_bound_long_text(local_location: LocalLocation) -> None:
    """O ``RETURN_STATS`` de um ``DOUBLE`` com ``NaN`` marca ``has_nan`` e dá o máximo sem ele; o infinito sai ``inf``; o texto longo sai truncado em 256 caracteres, com o máximo arredondado para cima, e o texto multibyte longo sai sem mínimo e máximo.

    ``register_files`` da [etapa 3](../../plan/PLAN-STAGE-3.md) transcreve o ``Double`` por
    ``float``: o ``NaN`` fica fora do máximo registrado, e ``float("inf")`` vira ``Infinity`` no JSON
    do log (``test_deltalake.py::test_nan_statistics_hide_rows_from_delta_scan``). No texto, o
    máximo truncado continua acima de todo valor do arquivo, e a poda não perde linha.
    """
    folder = Path(local_location.child("duckdb/estatisticas"))
    folder.mkdir(parents=True)
    con = duckdb.connect()

    def column_stats(values_sql: str, column: str) -> dict[str, str]:
        con.execute(f"CREATE OR REPLACE TABLE t AS SELECT * FROM (VALUES {values_sql}) v({column})")
        row = con.execute(f"COPY t TO '{folder / 'f.parquet'}' (FORMAT parquet, RETURN_STATS)").fetchone()
        return row[4][f'"{column}"']

    # NaN: o máximo é o maior número, e has_nan avisa. O infinito entra como texto 'inf'.
    with_nan = column_stats("(1.5), ('nan'::DOUBLE), (2.0)", "valor")
    assert (with_nan["has_nan"], with_nan["min"], with_nan["max"]) == ("true", "1.5", "2.0")
    with_infinity = column_stats("(1.5), ('inf'::DOUBLE)", "valor")
    assert (with_infinity["has_nan"], with_infinity["max"]) == ("false", "inf")
    assert json.dumps({"max": float(with_infinity["max"])}) == '{"max": Infinity}'  # não é JSON válido

    # Texto: 255 caracteres e o último incrementado, um limite superior do valor real.
    long_z = "a" * 300 + "z"
    long_text = column_stats(f"('{'a' * 300 + 'b'}'), ('{long_z}')", "texto")
    assert long_text["max"] == "a" * 255 + "b" and long_text["max"] > long_z
    assert long_text["min"] == "a" * 256
    multibyte = column_stats(f"('{'ç' * 200 + 'z'}'), ('{'ç' * 10}')", "texto")
    assert "min" not in multibyte and "max" not in multibyte
    con.close()


def test_control_total_fails_on_nan_and_infinity(con: duckdb.DuckDBPyConnection) -> None:
    """A soma de controle da auditoria, ``sum(CAST(valor AS DECIMAL(38, 6)))``, falha com ``ConversionException`` num ``NaN`` ou num infinito, também sob ``FILTER (WHERE isfinite(valor))``; um ``CASE`` com ``isfinite`` soma só os finitos.

    Um ``Double`` não finito derrubaria a verificação ``linhas`` inteira da etapa 4 em vez de
    aparecer como contagem. O ``FILTER`` do agregado não evita o erro, porque o ``CAST`` é avaliado
    em toda linha antes dele.
    """
    con.execute("CREATE TABLE t AS SELECT * FROM (VALUES (1, 1.5), (2, 'nan'::DOUBLE), (3, 'inf'::DOUBLE)) v(id, valor)")
    for special in (2, 3):
        with pytest.raises(duckdb.ConversionException, match="to DECIMAL\\(38,6\\)"):
            con.execute(f"SELECT sum(CAST(valor AS DECIMAL(38, 6))) FROM t WHERE id = {special}").fetchall()
    with pytest.raises(duckdb.ConversionException):
        con.execute("SELECT sum(CAST(valor AS DECIMAL(38, 6))) FILTER (WHERE isfinite(valor)) FROM t").fetchall()
    finite = con.execute(
        "SELECT sum(CASE WHEN isfinite(valor) THEN CAST(valor AS DECIMAL(38, 6)) END), count(*) FILTER (WHERE NOT isfinite(valor)) FROM t"
    ).fetchone()
    assert finite == (decimal.Decimal("1.500000"), 2)


def test_control_total_by_decimal_does_not_depend_on_threads(con: duckdb.DuckDBPyConnection) -> None:
    """A soma de controle por ``DECIMAL(38, 6)`` dá o mesmo valor com qualquer número de threads; a soma em ``DOUBLE`` depende da ordem, e as suas somas são leituras do relatório.

    É o motivo do ``CAST`` na soma de controle da auditoria: sem ele, duas execuções sobre as
    mesmas linhas podem discordar nas últimas casas.
    """
    con.execute(
        "CREATE TABLE t AS SELECT (hash(range) % 2400000000000)::DOUBLE / 100 - 11846195394.62 AS valor FROM range(5_000_000)"
    )
    as_double = []
    as_decimal = []
    for threads in (1, 2, 4):
        con.execute(f"SET threads = {threads}")
        as_double.append(con.execute("SELECT sum(valor) FROM t").fetchone()[0])
        as_decimal.append(con.execute("SELECT sum(CAST(valor AS DECIMAL(38, 6))) FROM t").fetchone()[0])
    assert len(set(as_decimal)) == 1
    record("duckdb.control_total_double_by_threads", ", ".join(repr(total) for total in as_double))


def test_interrupt_stops_a_blocking_query_from_another_thread() -> None:
    """``interrupt()``, chamado de outra thread, para em milissegundos uma consulta presa num operador bloqueante; a conexão continua usável, um ``interrupt()`` ocioso não afeta o comando seguinte, e o de uma conexão não para a consulta de um cursor dela.

    O ``close`` de um ``stream`` da etapa 4 só confere o pedido de parada entre lotes; uma ordenação
    não entrega lote algum antes de terminar. O tempo até parar é leitura do relatório.
    """
    con = duckdb.connect(config={"threads": 2})
    con.execute("CREATE TABLE t AS SELECT range AS id, hash(range) AS h FROM range(20_000_000)")
    outcome: dict[str, object] = {}

    def sort_all(connection: duckdb.DuckDBPyConnection) -> None:
        try:
            connection.execute("SELECT id FROM t ORDER BY h").to_arrow_table()
            outcome["error"] = None
        except duckdb.Error as error:
            outcome["error"] = error

    worker = threading.Thread(target=sort_all, args=(con,))
    worker.start()
    time.sleep(0.2)
    asked = time.perf_counter()
    con.interrupt()
    worker.join(timeout=10)
    stopped = time.perf_counter() - asked
    assert isinstance(outcome["error"], duckdb.InterruptException)
    record("duckdb.interrupt_stop", f"{stopped * 1e3:.1f} ms depois do pedido")

    # A conexão continua usável, e um interrupt sem consulta em curso não afeta o comando seguinte.
    assert con.execute("SELECT count(*) FROM t").fetchone()[0] == 20_000_000
    con.interrupt()
    assert con.execute("SELECT 42").fetchone()[0] == 42

    # O interrupt pertence à conexão: a consulta num cursor dela, que é outra conexão, termina.
    cursor = con.cursor()
    outcome.clear()
    worker = threading.Thread(target=sort_all, args=(cursor,))
    worker.start()
    time.sleep(0.2)
    con.interrupt()
    worker.join(timeout=30)
    assert outcome["error"] is None
    cursor.close()
    con.close()


def test_cursor_opens_while_the_connection_runs_a_query() -> None:
    """``cursor()`` volta na hora com uma consulta em curso na conexão, de outra thread, e o cursor novo consulta o mesmo banco.

    ``new_session()`` da etapa 4 e um cursor próprio não precisam do lock da sessão para nascer: a
    sessão a mais pedida durante um comando longo da principal não espera por ele.
    """
    con = duckdb.connect(config={"threads": 2})
    con.execute("CREATE TABLE t AS SELECT range AS id, hash(range) AS h FROM range(20_000_000)")
    finished = threading.Event()

    def sort_all() -> None:
        con.execute("SELECT id FROM t ORDER BY h").to_arrow_table()
        finished.set()

    worker = threading.Thread(target=sort_all)
    worker.start()
    time.sleep(0.2)
    started = time.perf_counter()
    cursor = con.cursor()
    opened = time.perf_counter() - started
    running = not finished.is_set()
    assert cursor.execute("SELECT count(*) FROM t").fetchone()[0] == 20_000_000
    worker.join(timeout=30)
    assert running and opened < 0.1
    record("duckdb.cursor_during_a_query", f"{opened * 1e3:.2f} ms com a consulta da conexão em curso")
    cursor.close()
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


def test_audit_queries(con: duckdb.DuckDBPyConnection) -> None:
    """As consultas da auditoria acham cada defeito de uma partição: chave repetida, nulo, partição diferente da data de origem, JSON inválido, texto acima de ``String(200)`` em bytes."""
    con.execute("CREATE TABLE lancamentos (id BIGINT, data_base DATE, data_base_str VARCHAR, valor DECIMAL(18,2), meta VARCHAR, descricao VARCHAR)")
    con.execute(
        """
        INSERT INTO lancamentos VALUES
            (1, '2026-08-31', '2026-08-31', 10.00, '{"ok": true}', 'a'),
            (1, '2026-08-31', '2026-08-31', 20.00, NULL, 'b'),
            (2, '2026-08-31', '2026-08-31', NULL, '{invalido', repeat('x', 200)),
            (3, '2026-07-31', '2026-08-31', 5.00, NULL, repeat('ç', 101))
        """
    )

    duplicates = con.execute("SELECT id, count(*) FROM lancamentos GROUP BY id HAVING count(*) > 1").fetchall()
    assert duplicates == [(1, 2)]

    # O String(n) do contrato é medido em bytes, a medida do VARCHAR(n) do Redshift: strlen conta
    # bytes, length conta caracteres, e 'ç' ocupa dois bytes. O octet_length do DuckDB só aceita BLOB.
    assert con.execute("SELECT strlen(repeat('ç', 101)), length(repeat('ç', 101))").fetchone() == (202, 101)
    with pytest.raises(duckdb.BinderException, match="octet_length"):
        con.execute("SELECT octet_length(descricao) FROM lancamentos")

    # count(*) FILTER conta os defeitos numa passagem só; o total de controle acompanha. A partição
    # é conferida contra a data de origem por strftime(data, '%Y-%m-%d') quando o modelo declara
    # partition_source.
    row = con.execute(
        """
        SELECT count(*) FILTER (WHERE valor IS NULL),
               count(*) FILTER (WHERE data_base_str <> strftime(data_base, '%Y-%m-%d')),
               count(*) FILTER (WHERE meta IS NOT NULL AND NOT json_valid(meta)),
               count(*) FILTER (WHERE strlen(descricao) > 200),
               sum(valor)
        FROM lancamentos
        """
    ).fetchone()
    assert row == (1, 1, 1, 1, decimal.Decimal("35.00"))
