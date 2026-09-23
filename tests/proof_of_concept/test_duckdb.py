"""O DuckDB como sandbox: a API Python que a biblioteca usa.

Sem gravar arquivo: a configuração da conexão, Arrow na entrada e na saída, o leitor invalidado pelo
comando seguinte e preservado num cursor próprio, a tabela temporária vista pelas threads da conexão
e não por um cursor dela, a tabela Arrow registrada vista só pela conexão que a registrou, a
consulta em streaming (o primeiro lote antes do fim, a memória de um lote, medida em subprocesso),
o ``INSERT`` alimentado por um leitor sobre um gerador Python (um comando só, a leitura antecipada
que segue puxando o gerador depois da falha, os lotes que o leitor não confere e o objeto que só
expõe ``__arrow_c_stream__``), o tipo ``DECIMAL`` inferido de uma amostra do pandas contra o fixado
pelo esquema Arrow, JSON, o custo do ``executemany`` contra a carga por Arrow, as consultas da
auditoria, a soma de controle que o ``NaN`` derruba e a que o ``DECIMAL(38, 6)`` torna independente
das threads, o ``interrupt()`` chamado de outra thread e o ``cursor()`` aberto com uma consulta em
curso. Sob a raiz local (marcador ``local``): ``COPY ... TO`` com ``RETURN_STATS`` e o esquema
físico do Parquet gravado, o ``RETURN_STATS`` com ``NaN``, infinito e texto longo, o ``has_nan``
que só vê o último grupo de linhas, a poda do leitor Parquet pelo rodapé do pyarrow num grupo com
``NaN``, o stream transbordado num arquivo Arrow IPC, o ``COPY`` particionado por mês e um banco em
arquivo com pasta de transbordo. O ``delta_scan`` está em ``poc_delta.py``.
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
import pyarrow.parquet as pq
import pytest

from conftest import LocalLocation, record
from poc_delta import MONTHS, ROWS, StreamOnly, run_in_threads, sample_table


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    """Banco em memória com a configuração padrão; as extensões embutidas (json, parquet) dispensam
    LOAD."""
    connection = duckdb.connect()

    yield connection

    connection.close()


def return_stats(cursor: duckdb.DuckDBPyConnection) -> dict[str, object]:
    """A linha do ``RETURN_STATS`` de um ``COPY``, pelo nome de cada coluna."""
    names = [column[0] for column in cursor.description]
    return dict(zip(names, cursor.fetchone()))


def test_connection_config() -> None:
    """As opções entram na abertura da conexão ou por ``SET``; ``duckdb_settings()`` mostra o valor
    em vigor."""
    config = {"threads": 2, "memory_limit": "512MB", "preserve_insertion_order": False}
    connection = duckdb.connect(config=config)

    settings = dict(
        connection.execute(
            "SELECT name, value FROM duckdb_settings() "
            "WHERE name IN ('threads', 'memory_limit', 'preserve_insertion_order')"
        ).fetchall()
    )
    assert settings["threads"] == "2"
    assert settings["preserve_insertion_order"] == "false"
    record("duckdb.memory_limit_512MB_as_reported", settings["memory_limit"])

    # O mesmo ajuste por comando, depois de aberta a conexão.
    connection.execute("SET threads = 1")
    assert connection.execute("SELECT current_setting('threads')").fetchone()[0] == 1

    connection.close()


def test_arrow_in_and_out(con: duckdb.DuckDBPyConnection) -> None:
    """Uma tabela Arrow registrada é consultada sem cópia; o resultado sai como tabela Arrow ou como
    leitor em lotes."""
    sample = sample_table().slice(0, 1000)

    # register expõe a tabela Arrow como uma view; CREATE TABLE AS a materializa no banco.
    con.register("entrada", sample)
    con.execute("CREATE TABLE operacoes AS SELECT * FROM entrada")

    # INSERT ... BY NAME casa as colunas pelo nome, em qualquer ordem.
    con.execute(
        "INSERT INTO operacoes BY NAME SELECT valor, id_operacao + 1000 AS id_operacao, mes, "
        "data_ref, id_cliente, descricao FROM entrada"
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


def test_arrow_reader_on_its_own_cursor_survives_commands_on_another(
        con: duckdb.DuckDBPyConnection) -> None:
    """O leitor preso a um cursor entrega o snapshot da sua consulta enquanto outro cursor insere na
    mesma tabela, cria, altera e apaga tabelas.

    É o que isola o ``stream`` da etapa 4 dos comandos que a thread do cliente roda no cursor dela.
    O cursor fechado no meio da leitura e o custo de abrir um cursor são leituras do relatório.
    """
    con.execute("CREATE TABLE numeros AS SELECT range AS id FROM range(1_000_000)")
    reading = con.cursor()
    writing = con.cursor()
    reader = reading.execute("SELECT id FROM numeros").to_arrow_reader(100_000)
    first = reader.read_next_batch()

    writing.execute("INSERT INTO numeros SELECT range + 1_000_000 FROM range(10)")
    writing.execute("CREATE TABLE outra AS SELECT 1 AS x")
    writing.execute("ALTER TABLE numeros ADD COLUMN y INTEGER")
    writing.execute("DROP TABLE outra")
    rest = reader.read_all()
    assert first.num_rows + rest.num_rows == 1_000_000  # o snapshot da consulta, sem as dez linhas
    assert writing.execute("SELECT count(*) FROM numeros").fetchone()[0] == 1_000_010

    reader = reading.execute("SELECT id FROM numeros").to_arrow_reader(100_000)
    reader.read_next_batch()
    reading.close()
    record("duckdb.arrow_reader_rows_after_its_cursor_closed", reader.read_all().num_rows)
    started = time.perf_counter()
    for _ in range(100):
        con.cursor().close()
    elapsed_ms = (time.perf_counter() - started) * 1e3
    record("duckdb.cursor_open_and_close_100", f"{elapsed_ms:.1f} ms")
    writing.close()


def test_temp_table_is_seen_by_the_threads_of_its_connection_not_by_a_cursor(
        con: duckdb.DuckDBPyConnection) -> None:
    """A tabela temporária é da conexão: outra thread que usa a mesma conexão a lê, e um cursor
    dela, que é outra conexão ao mesmo banco, vê a tabela confirmada e não a temporária.

    É o que faz a tabela temporária da sessão única valer para toda thread da execução, e a sessão
    a mais de ``new_session``, um cursor, não a ver.
    """
    con.execute("CREATE TABLE confirmada AS SELECT range AS id FROM range(10)")
    con.execute("CREATE TEMP TABLE temporaria AS SELECT range AS id FROM range(10)")
    seen: dict[str, int] = {}

    def read_temporary() -> None:
        seen["outra_thread"] = con.execute("SELECT count(*) FROM temporaria").fetchone()[0]

    run_in_threads([read_temporary])
    assert seen == {"outra_thread": 10}

    cursor = con.cursor()
    assert cursor.execute("SELECT count(*) FROM confirmada").fetchone()[0] == 10
    with pytest.raises(duckdb.CatalogException, match="temporaria"):
        cursor.execute("SELECT count(*) FROM temporaria")
    cursor.close()


def test_registered_arrow_table_is_seen_only_by_its_connection(
        con: duckdb.DuckDBPyConnection) -> None:
    """O registro de uma tabela Arrow pertence à conexão: um cursor dela, que é outra conexão, não a
    vê."""
    con.register("somente_aqui", pa.table({"x": [1, 2, 3]}))
    cursor = con.cursor()
    with pytest.raises(duckdb.CatalogException, match="somente_aqui"):
        cursor.execute("SELECT count(*) FROM somente_aqui")
    cursor.close()
    assert con.execute("SELECT count(*) FROM somente_aqui").fetchone()[0] == 3
    con.unregister("somente_aqui")


# A consulta das medições em subprocesso e do primeiro lote: a tabela inteira, o leitor em lotes e o
# stream transbordado leem o mesmo texto, e a comparação entre eles é justa.
STREAM_SQL = "SELECT range AS id, range % 97 AS m, 'x' || (range % 1000) AS s FROM range({rows})"

# Roda num subprocesso, um cenário por chamada: a memória máxima do processo depende só do cenário.
MEMORY_PROBE = r"""
import json, resource, sys, time
import duckdb
scenario = sys.argv[1]
sql = sys.argv[2]
con = duckdb.connect(config={"threads": 2})
started = time.perf_counter()
if scenario == "table":
    rows = con.execute(sql).to_arrow_table().num_rows
else:
    rows = sum(batch.num_rows for batch in con.execute(sql).to_arrow_reader(100_000))
scale = 1e6 if sys.platform == "darwin" else 1e3
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / scale
seconds = round(time.perf_counter() - started, 3)
print(json.dumps({"rows": rows, "seconds": seconds, "peak_mb": round(peak)}))
"""


def run_probe(script: str, *arguments: str) -> dict[str, object]:
    """Roda ``script`` num Python novo com ``arguments`` e devolve o JSON que ele imprime."""
    completed = subprocess.run([sys.executable, "-c", script, *arguments],
                               capture_output=True, text=True, check=True)
    return json.loads(completed.stdout)


def test_streaming_query_starts_before_the_end() -> None:
    """Sem ``ORDER BY`` o primeiro lote chega antes de a consulta terminar; com ``ORDER BY`` a
    ordenação inteira precede o primeiro lote."""
    rows = 10_000_000
    con = duckdb.connect(config={"threads": 2})
    sql = STREAM_SQL.format(rows=rows)

    def timing(query: str) -> tuple[float, float, float, int]:
        """Os segundos do ``execute``, do primeiro lote e do total, e as linhas lidas."""
        started = time.perf_counter()
        reader = con.execute(query).to_arrow_reader(100_000)
        executed = time.perf_counter()
        first = reader.read_next_batch()
        first_batch = time.perf_counter()
        count = first.num_rows + sum(batch.num_rows for batch in reader)
        execute_seconds = executed - started
        first_batch_seconds = first_batch - executed
        total_seconds = time.perf_counter() - started
        return execute_seconds, first_batch_seconds, total_seconds, count

    def describe(executed: float, first: float, total: float) -> str:
        """Os três tempos de ``timing`` no texto do relatório."""
        return f"execute {executed:.3f} s, primeiro lote {first:.3f} s, total {total:.3f} s"

    executed, first, total, count = timing(sql)
    assert count == rows
    assert executed + first < total / 5
    record("duckdb.stream_10M_rows_unordered", describe(executed, first, total))

    executed, first, total, count = timing(sql + " ORDER BY m, id")
    assert count == rows
    assert executed > total / 2
    record("duckdb.stream_10M_rows_ordered", describe(executed, first, total))
    con.close()


def test_streaming_query_bounds_memory() -> None:
    """Sem ``ORDER BY`` o processo fica no tamanho de um lote.

    A memória é medida num subprocesso por cenário (``ru_maxrss``): a tabela inteira contra o leitor
    em lotes, sobre as mesmas linhas.
    """
    rows = 10_000_000
    sql = STREAM_SQL.format(rows=rows)
    peaks = {}
    for scenario in ("table", "stream"):
        peaks[scenario] = run_probe(MEMORY_PROBE, scenario, sql)
    readings = {}
    for scenario, reading in peaks.items():
        readings[scenario] = f"{reading['peak_mb']} MB em {reading['seconds']} s"
    record("duckdb.peak_rss_10M_rows", readings)
    assert peaks["stream"]["rows"] == peaks["table"]["rows"] == rows
    assert peaks["stream"]["peak_mb"] < peaks["table"]["peak_mb"] / 2


# O stream da sessão única: o leitor inteiro gravado num arquivo Arrow IPC com LZ4, e o arquivo lido
# lote a lote. Roda num subprocesso, como MEMORY_PROBE, para a memória máxima ser só dele.
SPOOL_PROBE = r"""
import json, os, resource, sys, threading, time
import duckdb, pyarrow as pa
folder = sys.argv[1]
sql = sys.argv[2]
con = duckdb.connect(config={"threads": 2})
path = os.path.join(folder, "transbordo.arrow")
options = pa.ipc.IpcWriteOptions(compression="lz4")
condition = threading.Condition()
progress = {"written": 0, "done": False, "query_seconds": None}
started = time.perf_counter()

def produce() -> None:
    reader = con.execute(sql).to_arrow_reader(100_000)
    with (pa.OSFile(path, "wb") as sink,
          pa.ipc.new_stream(sink, reader.schema, options=options) as writer):
        for batch in reader:
            writer.write_batch(batch)
            with condition:
                progress["written"] += 1
                condition.notify_all()
    with condition:
        progress["done"] = True
        progress["query_seconds"] = time.perf_counter() - started
        condition.notify_all()

threading.Thread(target=produce).start()
with condition:
    condition.wait_for(lambda: progress["written"] > 0 or progress["done"])
first = time.perf_counter() - started
rows_read = 0
batches_read = 0
with pa.OSFile(path, "rb") as source:
    reader = pa.ipc.open_stream(source)
    while True:
        with condition:
            condition.wait_for(lambda: progress["written"] > batches_read or progress["done"])
            if progress["written"] == batches_read:
                break
        rows_read += reader.read_next_batch().num_rows
        batches_read += 1
scale = 1e6 if sys.platform == "darwin" else 1e3
peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / scale
print(json.dumps({
    "rows": rows_read,
    "seconds": round(time.perf_counter() - started, 3),
    "first_batch_seconds": round(first, 3),
    "query_seconds": round(progress["query_seconds"], 3),
    "file_mb": round(os.path.getsize(path) / 1e6),
    "peak_mb": round(peak),
}))
"""


@pytest.mark.local
def test_spooled_stream_bounds_memory(local_location: LocalLocation) -> None:
    """O resultado gravado lote a lote num arquivo Arrow IPC com LZ4 por uma thread, e lido lote a
    lote enquanto ela grava, mantém o processo no tamanho de um lote, como o leitor direto.

    É o caminho do arquivo do ``stream`` híbrido (``serialize_db.engine.duckdb``), o que ele toma
    depois do orçamento de memória: a thread roda a consulta sob o lock e grava cada lote assim que
    o DuckDB o entrega, e o cliente lê cada lote gravado sem a sessão. O tempo até o primeiro lote,
    o tempo da consulta, o tamanho do arquivo e a memória máxima vão para o relatório; a asserção é
    a mesma do leitor direto, menos da metade da memória da tabela inteira.
    """
    rows = 10_000_000
    sql = STREAM_SQL.format(rows=rows)
    folder = Path(local_location.child("transbordo_memoria"))
    folder.mkdir()
    spooled = run_probe(SPOOL_PROBE, str(folder), sql)
    table = run_probe(MEMORY_PROBE, "table", sql)
    record(
        "duckdb.spooled_stream_10M_rows",
        f"{spooled['peak_mb']} MB em {spooled['seconds']} s (primeiro lote em "
        f"{spooled['first_batch_seconds']} s, consulta em {spooled['query_seconds']} s, "
        f"{spooled['file_mb']} MB de arquivo); a tabela inteira, {table['peak_mb']} MB",
    )
    assert spooled["rows"] == table["rows"] == rows
    assert spooled["peak_mb"] < table["peak_mb"] / 2


# O esquema dos leitores dos testes de INSERT: um inteiro e um DOUBLE.
ID_AND_VALOR = pa.schema([("id", pa.int64()), ("valor", pa.float64())])


def batch_of_thousand(k: int) -> pa.RecordBatch:
    """O lote ``k``: os ids de ``k * 1000`` a ``(k + 1) * 1000 - 1`` e ``valor`` igual a ``k``."""
    ids = pa.array(range(k * 1000, (k + 1) * 1000), pa.int64())
    values = pa.array([float(k)] * 1000)
    return pa.RecordBatch.from_pydict({"id": ids, "valor": values})


def insert_from_reader(con: duckdb.DuckDBPyConnection, reader: object, table: str) -> None:
    """Registra ``reader`` como ``entrada``, roda ``INSERT INTO <table> BY NAME`` sobre ele e o
    desregistra, também quando o comando falha."""
    con.register("entrada", reader)
    try:
        con.execute(f"INSERT INTO {table} BY NAME SELECT * FROM entrada")
    finally:
        con.unregister("entrada")


def test_insert_from_a_generator_reader_is_one_statement(con: duckdb.DuckDBPyConnection) -> None:
    """Um ``INSERT ... SELECT`` de um ``RecordBatchReader`` sobre um gerador Python é um comando só:
    a falha do gerador deixa a tabela como estava.

    O ``arrow_scan`` puxa o fluxo por uma thread de leitura antecipada do Arrow, que chama o gerador
    em outra thread; a thread que chamou o gerador vai para o relatório.
    """
    con.execute("CREATE TABLE destino (id BIGINT, valor DOUBLE)")
    threads: set[int] = set()
    delivered: list[int] = []

    def generate(count: int, fail_at: int | None = None) -> Iterator[pa.RecordBatch]:
        for k in range(count):
            threads.add(threading.get_ident())
            if k == fail_at:
                raise RuntimeError(f"falha do cliente no lote {k}")
            delivered.append(k)
            yield batch_of_thousand(k)

    reader = pa.RecordBatchReader.from_batches(ID_AND_VALOR, generate(50))
    insert_from_reader(con, reader, "destino")
    totals = con.execute("SELECT count(*), sum(valor) FROM destino").fetchone()
    assert totals == (50_000, 1_225_000.0)
    if threads == {threading.get_ident()}:
        puller = "a thread do chamador"
    else:
        puller = f"outra thread ({len(threads)}, a leitura antecipada do Arrow)"
    record("duckdb.generator_pulled_by", puller)

    # O gerador falha no lote 20: o comando falha inteiro, com a mensagem do cliente, e a contagem
    # não muda.
    delivered.clear()
    failing = pa.RecordBatchReader.from_batches(ID_AND_VALOR, generate(50, fail_at=20))
    with pytest.raises(duckdb.InvalidInputException, match="falha do cliente no lote 20"):
        insert_from_reader(con, failing, "destino")
    assert con.execute("SELECT count(*) FROM destino").fetchone()[0] == 50_000
    assert len(delivered) == 20


def test_read_ahead_keeps_pulling_the_generator_after_a_failed_insert(
        con: duckdb.DuckDBPyConnection) -> None:
    """A leitura antecipada do Arrow, que alimenta o ``arrow_scan``, puxa lotes além do que o
    comando consumiu e continua depois de o comando falhar.

    O buffer foge da fila de quem alimenta o gerador, e é por isso que o ``loader`` do motor
    (``serialize_db.engine.duckdb``) grava os lotes num arquivo Arrow IPC e roda um único ``INSERT``
    sobre o leitor nativo do arquivo, sem entregar um gerador ao DuckDB. As contagens na falha e
    meio segundo depois vão para o relatório.
    """
    # O comando falha no primeiro lote (valor fora do INTEGER), e a leitura antecipada segue
    # puxando o gerador.
    con.execute("CREATE TABLE estreita (id INTEGER, valor DOUBLE)")
    delivered: list[int] = []

    def generate_wide(count: int) -> Iterator[pa.RecordBatch]:
        for k in range(count):
            delivered.append(k)
            ids = pa.array([2**40] * 1000, pa.int64())
            yield pa.RecordBatch.from_pydict({"id": ids, "valor": pa.array([0.0] * 1000)})

    con.register("entrada", pa.RecordBatchReader.from_batches(ID_AND_VALOR, generate_wide(50)))
    with pytest.raises(duckdb.ConversionException):
        con.execute("INSERT INTO estreita BY NAME SELECT * FROM entrada")
    at_failure = len(delivered)
    time.sleep(0.5)
    con.unregister("entrada")
    assert len(delivered) > 1
    record("duckdb.batches_pulled_ahead_of_a_failed_insert",
           f"{at_failure} na falha, {len(delivered)} meio segundo depois")


def test_insert_from_a_reader_trusts_the_batches(con: duckdb.DuckDBPyConnection) -> None:
    """O leitor não confere os lotes contra o esquema declarado, e o ``arrow_scan`` os lê pelo
    esquema declarado: um lote com as colunas em outra ordem entra com os bytes trocados, sem erro,
    e um lote com uma coluna a mais falha.

    O ``cast`` de cada lote para o esquema, antes do leitor, é a barreira. A nulidade do esquema
    Arrow não é conferida; a coluna ``NOT NULL`` do DuckDB é.
    """
    con.execute("CREATE TABLE destino (id BIGINT, valor DOUBLE)")

    # A ordem trocada corrompe em silêncio.
    swapped = pa.RecordBatch.from_pydict(
        {"valor": pa.array([1.0]), "id": pa.array([1], pa.int64())})
    insert_from_reader(con, pa.RecordBatchReader.from_batches(ID_AND_VALOR, [swapped]), "destino")
    corrupted = con.execute("SELECT id, valor FROM destino").fetchall()
    assert len(corrupted) == 1
    assert corrupted != [(1, 1.0)]
    record("duckdb.batch_with_swapped_columns_read_as", str(corrupted))

    # A coluna a mais falha.
    extra = pa.RecordBatch.from_pydict(
        {"id": pa.array([1], pa.int64()), "valor": pa.array([1.0]), "x": ["z"]})
    with_extra = pa.RecordBatchReader.from_batches(ID_AND_VALOR, [extra])
    with pytest.raises(duckdb.InvalidInputException, match="3 children, expected 2"):
        insert_from_reader(con, with_extra, "destino")

    # A nulidade declarada no esquema do leitor não é conferida; a coluna NOT NULL do DuckDB é.
    strict = pa.schema([pa.field("id", pa.int64(), nullable=False), ("valor", pa.float64())])
    with_null = pa.RecordBatch.from_pydict(
        {"id": pa.array([1, None], pa.int64()), "valor": pa.array([1.0, 2.0])})
    con.execute("DELETE FROM destino")
    insert_from_reader(con, pa.RecordBatchReader.from_batches(strict, [with_null]), "destino")
    nulls = con.execute("SELECT count(*) FILTER (WHERE id IS NULL) FROM destino").fetchone()[0]
    assert nulls == 1
    con.execute("CREATE TABLE estrito (id BIGINT NOT NULL, valor DOUBLE)")
    null_into_strict = pa.RecordBatchReader.from_batches(ID_AND_VALOR, [with_null])
    with pytest.raises(duckdb.ConstraintException, match="NOT NULL constraint failed"):
        insert_from_reader(con, null_into_strict, "estrito")


def test_register_takes_an_object_with_only_arrow_c_stream(con: duckdb.DuckDBPyConnection) -> None:
    """Um objeto que só expõe ``__arrow_c_stream__`` entra por ``register`` como um leitor."""
    con.execute("CREATE TABLE estrito (id BIGINT NOT NULL, valor DOUBLE)")
    batches = (batch_of_thousand(k) for k in range(3))
    stream = StreamOnly(pa.RecordBatchReader.from_batches(ID_AND_VALOR, batches))
    insert_from_reader(con, stream, "estrito")
    assert con.execute("SELECT count(*) FROM estrito").fetchone()[0] == 3000


def test_decimal_from_pandas_sample_versus_arrow_schema(con: duckdb.DuckDBPyConnection) -> None:
    """Uma coluna ``object`` de ``Decimal`` recebe o tipo dos valores presentes; o esquema Arrow
    fixa ``DECIMAL(18,2)``."""
    values = [decimal.Decimal("12345.67")] * 1500 + [decimal.Decimal("123456789.01")]
    frame = pd.DataFrame({"valor": values})

    # O DuckDB infere o tipo dos valores do DataFrame, não do contrato: um DECIMAL do tamanho dos
    # dados de hoje.
    con.register("amostra", frame)
    inferred = con.execute("DESCRIBE SELECT valor FROM amostra").fetchone()[1]
    record("duckdb.decimal_inferred_from_sample", inferred)
    assert inferred.startswith("DECIMAL(")
    assert inferred != "DECIMAL(18,2)"

    try:
        con.execute("CREATE TABLE inferida AS SELECT * FROM amostra")
        maximum = con.execute("SELECT max(valor) FROM inferida").fetchone()[0]
        outcome = f"aceita: max = {maximum}"
    except duckdb.Error as error:
        outcome = f"{type(error).__name__}: {str(error).splitlines()[0][:120]}"
    record("duckdb.decimal_inferred_materialize", outcome)

    # A conversão para Arrow com o esquema do contrato fixa o tipo antes de o DuckDB ver os dados.
    contract = pa.schema([("valor", pa.decimal128(18, 2))])
    fixed = pa.Table.from_pandas(frame, schema=contract, preserve_index=False)
    con.register("contrato", fixed)
    assert con.execute("DESCRIBE SELECT valor FROM contrato").fetchone()[1] == "DECIMAL(18,2)"
    maximum = con.execute("SELECT max(valor) FROM contrato").fetchone()[0]
    assert maximum == decimal.Decimal("123456789.01")


def test_json_column(con: duckdb.DuckDBPyConnection) -> None:
    """A coluna ``JSON`` valida na entrada e responde a ``->>``, ``json_extract`` e
    ``json_valid``."""
    con.execute("CREATE TABLE eventos (id BIGINT, meta JSON)")
    con.execute("""INSERT INTO eventos VALUES (1, '{"sistema": "A", "ativo": true}'), (2, NULL)""")

    extracted = con.execute(
        "SELECT meta->>'$.sistema', json_extract(meta, '$.ativo') FROM eventos WHERE id = 1"
    ).fetchone()
    assert extracted == ("A", "true")
    validity = con.execute("""SELECT json_valid('{"a": 1}'), json_valid('{a}')""").fetchone()
    assert validity == (True, False)

    # Texto inválido é recusado no cast para JSON; a auditoria da biblioteca confere json_valid
    # antes.
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
    values = [decimal.Decimal(i) / 100 for i in range(rows)]
    con.executemany("INSERT INTO lote VALUES (?, ?)", list(zip(range(rows), values)))
    by_row = time.perf_counter() - started

    ids = pa.array(range(rows), pa.int64())
    decimals = pa.array(values, pa.decimal128(18, 2))
    con.register("lote_arrow", pa.table({"id": ids, "valor": decimals}))
    started = time.perf_counter()
    con.execute("INSERT INTO lote SELECT * FROM lote_arrow")
    by_arrow = time.perf_counter() - started

    record("duckdb.timing.executemany_5000_rows", f"{by_row:.3f} s")
    record("duckdb.timing.arrow_insert_5000_rows", f"{by_arrow:.3f} s")
    assert con.execute("SELECT count(*) FROM lote").fetchone()[0] == 2 * rows
    assert by_arrow < by_row


@pytest.mark.local
def test_copy_to_parquet_with_return_stats(local_location: LocalLocation) -> None:
    """``COPY ... TO`` grava o Parquet e devolve contagem, tamanho e estatísticas por coluna, sem
    reabrir o arquivo."""
    folder = Path(local_location.child("duckdb"))
    folder.mkdir(exist_ok=True)
    path = folder / "amostra.parquet"

    con = duckdb.connect()
    con.register("amostra", sample_table().slice(0, 10_000))

    # RETURN_STATS devolve uma linha com o que a AddAction do Delta precisa.
    cursor = con.execute(f"COPY (SELECT * FROM amostra) TO '{path}' (FORMAT parquet, RETURN_STATS)")
    stats = return_stats(cursor)
    assert stats["count"] == 10_000
    assert stats["file_size_bytes"] == path.stat().st_size

    per_column = {name.strip('"'): values for name, values in stats["column_statistics"].items()}
    assert per_column["valor"]["null_count"] == "0"
    assert per_column["valor"]["max"] == "99.99"

    # O esquema físico do arquivo: DECIMAL(18,2) como INT64, timestamp como INT64, toda coluna
    # optional.
    schema = con.execute(
        f"SELECT name, type, repetition_type FROM parquet_schema('{path}')").fetchall()
    physical = {name: kind for name, kind, _ in schema}
    repetition = {name: kind for name, _, kind in schema}
    assert physical["valor"] == "INT64"
    assert physical["data_ref"] == "INT64"
    assert repetition["id_operacao"] == "OPTIONAL"

    assert con.execute(f"SELECT count(*) FROM read_parquet('{path}')").fetchone()[0] == 10_000
    con.close()


@pytest.mark.local
def test_return_stats_leave_nan_out_and_bound_long_text(local_location: LocalLocation) -> None:
    """O ``RETURN_STATS`` de um ``DOUBLE`` com ``NaN`` marca ``has_nan`` e dá o máximo sem ele.

    O infinito sai ``inf``; o texto longo sai truncado em 256 caracteres, com o máximo arredondado
    para cima, e o texto multibyte longo sai sem mínimo e máximo.

    Transcrito por ``float``, o ``NaN`` ficaria fora do máximo registrado, e ``float("inf")``
    viraria ``Infinity`` no JSON do log
    (``test_deltalake.py::test_nan_statistics_hide_rows_from_delta_scan``): ``register_files`` da
    [etapa 3](../../plan/PLAN-STAGE-3.md) deixa fora do log o mínimo e o máximo não finitos e os das
    colunas de ``columns_without_min_max`` (issue #59). No texto, o máximo truncado continua acima
    de todo valor do arquivo, e a poda não perde linha.
    """
    folder = Path(local_location.child("duckdb/estatisticas"))
    folder.mkdir(parents=True)
    target = folder / "f.parquet"
    con = duckdb.connect()

    def column_stats(values_sql: str, column: str) -> dict[str, str]:
        """As estatísticas de ``column`` no ``RETURN_STATS`` de um arquivo com ``values_sql``."""
        con.execute(f"CREATE OR REPLACE TABLE t AS SELECT * FROM (VALUES {values_sql}) v({column})")
        cursor = con.execute(f"COPY t TO '{target}' (FORMAT parquet, RETURN_STATS)")
        return return_stats(cursor)["column_statistics"][f'"{column}"']

    # NaN: o máximo é o maior número, e has_nan avisa. O infinito entra como texto 'inf'.
    with_nan = column_stats("(1.5), ('nan'::DOUBLE), (2.0)", "valor")
    assert (with_nan["has_nan"], with_nan["min"], with_nan["max"]) == ("true", "1.5", "2.0")
    with_infinity = column_stats("(1.5), ('inf'::DOUBLE)", "valor")
    assert (with_infinity["has_nan"], with_infinity["max"]) == ("false", "inf")
    dumped = json.dumps({"max": float(with_infinity["max"])})
    assert dumped == '{"max": Infinity}'  # não é JSON válido

    # Texto: 255 caracteres e o último incrementado, um limite superior do valor real.
    long_b = "a" * 300 + "b"
    long_z = "a" * 300 + "z"
    long_text = column_stats(f"('{long_b}'), ('{long_z}')", "texto")
    assert long_text["max"] == "a" * 255 + "b"
    assert long_text["max"] > long_z
    assert long_text["min"] == "a" * 256
    multibyte_long = "ç" * 200 + "z"
    multibyte_short = "ç" * 10
    multibyte = column_stats(f"('{multibyte_long}'), ('{multibyte_short}')", "texto")
    assert "min" not in multibyte
    assert "max" not in multibyte
    con.close()


@pytest.mark.local
def test_return_stats_has_nan_follows_only_the_last_row_group(
        local_location: LocalLocation) -> None:
    """O ``has_nan`` do ``RETURN_STATS`` num arquivo de dois grupos de linhas só vê o ``NaN`` do
    último grupo; o rodapé omite o mínimo e o máximo de todo grupo com ``NaN``.

    Um ``NaN`` só no primeiro grupo sai ``has_nan`` falso, com o mínimo e o máximo dos números do
    arquivo inteiro: ``register_files`` não decide pelo ``has_nan`` se a coluna tem ``NaN`` (issue
    #59).
    """
    folder = Path(local_location.child("duckdb/has_nan"))
    folder.mkdir(parents=True)
    con = duckdb.connect()
    outcomes = {}
    for name, nan_rows in (("primeiro_grupo", "7"), ("ultimo_grupo", "3000"), ("nenhum", "-1")):
        file = folder / f"{name}.parquet"
        source = (
            f"SELECT i AS id, CASE WHEN i IN ({nan_rows}) THEN 'nan'::DOUBLE ELSE i / 1000 END "
            "AS valor FROM range(4096) r(i)"
        )
        options = "FORMAT parquet, ROW_GROUP_SIZE 2048, RETURN_STATS"
        cursor = con.execute(f"COPY ({source}) TO '{file}' ({options})")
        valor = return_stats(cursor)["column_statistics"]['"valor"']
        footer = con.execute(
            f"SELECT stats_min_value, stats_max_value FROM parquet_metadata('{file}') "
            "WHERE path_in_schema = 'valor' ORDER BY row_group_id"
        ).fetchall()
        outcomes[name] = (valor["has_nan"], footer)

    assert outcomes["primeiro_grupo"] == ("false", [(None, None), ("2.048", "4.095")])
    assert outcomes["ultimo_grupo"] == ("true", [("0.0", "2.047"), (None, None)])
    assert outcomes["nenhum"] == ("false", [("0.0", "2.047"), ("2.048", "4.095")])
    con.close()


@pytest.mark.local
def test_parquet_reader_prunes_the_nan_row_group_by_the_arrow_footer(
        local_location: LocalLocation) -> None:
    """O leitor Parquet do DuckDB poda pelo máximo do rodapé um grupo de linhas com ``NaN`` gravado
    pelo pyarrow, e perde a linha que ele mesmo ordena acima de todo número; o arquivo do próprio
    DuckDB e a tabela nativa a devolvem.

    O pyarrow segue a especificação do Parquet, que deixa o ``NaN`` fora do mínimo e do máximo; o
    DuckDB grava o grupo com ``NaN`` sem mínimo e máximo, e a tabela nativa guarda o ``NaN`` como
    máximo. É a issue duckdb/duckdb#25521; o delta-rs grava como o pyarrow
    (``test_deltalake.py::test_float_statistics_off_keep_the_nan_row``).
    """
    folder = Path(local_location.child("duckdb/poda_nan"))
    folder.mkdir(parents=True)
    ids = pa.array([1, 2, 3, 4, 5, 6], pa.int64())
    values = pa.array([1.5, float("nan"), 2.0, 4.0, 5.0, 6.0])
    table = pa.table({"id": ids, "valor": values})
    arrow_path = folder / "pyarrow.parquet"
    duckdb_path = folder / "duckdb.parquet"
    con = duckdb.connect()
    con.register("origem", table)
    con.execute("CREATE TABLE nativa AS SELECT * FROM origem")
    pq.write_table(table, arrow_path, row_group_size=3)
    con.execute(f"COPY nativa TO '{duckdb_path}' (FORMAT parquet)")

    def count(source: str, predicate: str = "valor > 3") -> int:
        return con.execute(f"SELECT count(*) FROM {source} WHERE {predicate}").fetchone()[0]

    arrow_file = f"read_parquet('{arrow_path}')"
    maximums = con.execute(
        f"SELECT stats_max_value FROM parquet_metadata('{arrow_path}') "
        "WHERE path_in_schema = 'valor' ORDER BY row_group_id"
    ).fetchall()
    assert maximums == [("2.0",), ("6.0",)]
    # NaN > 3 no DuckDB: 4 linhas sem poda, 3 com o grupo do NaN podado pelo máximo 2.0.
    assert count(arrow_file) == 3
    assert count(arrow_file, "valor + 0 > 3") == 4  # a expressão não desce ao leitor como filtro
    assert count(f"read_parquet('{duckdb_path}')") == 4
    assert count("nativa") == 4
    con.close()


def test_control_total_fails_on_nan_and_infinity(con: duckdb.DuckDBPyConnection) -> None:
    """A soma de controle da auditoria falha com ``ConversionException`` num ``NaN`` ou infinito.

    A soma é ``sum(CAST(valor AS DECIMAL(38, 6)))``, e falha também sob
    ``FILTER (WHERE isfinite(valor))``; um ``CASE`` com ``isfinite`` soma só os finitos.

    Um ``Double`` não finito derrubaria a verificação ``linhas`` inteira da etapa 4 em vez de
    aparecer como contagem. O ``FILTER`` do agregado não evita o erro, porque o ``CAST`` é avaliado
    em toda linha antes dele.
    """
    con.execute(
        "CREATE TABLE t AS SELECT * FROM "
        "(VALUES (1, 1.5), (2, 'nan'::DOUBLE), (3, 'inf'::DOUBLE)) v(id, valor)"
    )
    for special in (2, 3):
        query = f"SELECT sum(CAST(valor AS DECIMAL(38, 6))) FROM t WHERE id = {special}"
        with pytest.raises(duckdb.ConversionException, match="to DECIMAL\\(38,6\\)"):
            con.execute(query).fetchall()
    filtered = "SELECT sum(CAST(valor AS DECIMAL(38, 6))) FILTER (WHERE isfinite(valor)) FROM t"
    with pytest.raises(duckdb.ConversionException):
        con.execute(filtered).fetchall()
    finite = con.execute(
        "SELECT sum(CASE WHEN isfinite(valor) THEN CAST(valor AS DECIMAL(38, 6)) END), "
        "count(*) FILTER (WHERE NOT isfinite(valor)) FROM t"
    ).fetchone()
    assert finite == (decimal.Decimal("1.500000"), 2)


def test_control_total_by_decimal_does_not_depend_on_threads(
        con: duckdb.DuckDBPyConnection) -> None:
    """A soma de controle por ``DECIMAL(38, 6)`` dá o mesmo valor com qualquer número de threads; a
    soma em ``DOUBLE`` depende da ordem, e as suas somas são leituras do relatório.

    É o motivo do ``CAST`` na soma de controle da auditoria: sem ele, duas execuções sobre as
    mesmas linhas podem discordar nas últimas casas.
    """
    con.execute(
        "CREATE TABLE t AS SELECT (hash(range) % 2400000000000)::DOUBLE / 100 - 11846195394.62 "
        "AS valor FROM range(5_000_000)"
    )
    as_double = []
    as_decimal = []
    for threads in (1, 2, 4):
        con.execute(f"SET threads = {threads}")
        as_double.append(con.execute("SELECT sum(valor) FROM t").fetchone()[0])
        decimal_total = con.execute("SELECT sum(CAST(valor AS DECIMAL(38, 6))) FROM t").fetchone()
        as_decimal.append(decimal_total[0])
    assert len(set(as_decimal)) == 1
    record("duckdb.control_total_double_by_threads", ", ".join(repr(total) for total in as_double))


def test_interrupt_stops_a_blocking_query_from_another_thread() -> None:
    """``interrupt()``, de outra thread, para em milissegundos uma consulta num operador bloqueante.

    A conexão continua usável, um ``interrupt()`` ocioso não afeta o comando seguinte, e o de uma
    conexão não para a consulta de um cursor dela.

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
    """``cursor()`` volta na hora com uma consulta em curso na conexão, de outra thread, e o cursor
    novo consulta o mesmo banco.

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
    assert running
    assert opened < 0.1
    record("duckdb.cursor_during_a_query",
           f"{opened * 1e3:.2f} ms com a consulta da conexão em curso")
    cursor.close()
    con.close()


@pytest.mark.local
def test_copy_partition_by_month(local_location: LocalLocation) -> None:
    """``PARTITION_BY (mes)`` grava ``mes=.../data_0.parquet`` sem a coluna dentro do arquivo; a
    leitura Hive a restaura."""
    out = Path(local_location.child("duckdb/particionado"))

    con = duckdb.connect()
    # 1.000 linhas de cada mês.
    con.register("amostra", sample_table().slice(ROWS // 2 - 1000, 2000))
    con.execute(f"COPY (SELECT * FROM amostra) TO '{out}' (FORMAT parquet, PARTITION_BY (mes))")

    files = sorted(str(file.relative_to(out)) for file in out.rglob("*.parquet"))
    assert files == [f"mes={MONTHS[0]}/data_0.parquet", f"mes={MONTHS[1]}/data_0.parquet"]

    # O arquivo não tem a coluna de partição, a convenção do Delta. read_parquet a devolve mesmo num
    # arquivo só, porque detecta o layout Hive no caminho; parquet_schema mostra o que está gravado.
    first_file = out / files[0]
    stored = con.execute(
        f"SELECT name FROM parquet_schema('{first_file}') WHERE name <> 'duckdb_schema'"
    ).fetchall()
    columns = [row[0] for row in stored]
    assert columns == ["id_operacao", "data_ref", "id_cliente", "valor", "descricao"]
    described = con.execute(f"DESCRIBE SELECT * FROM read_parquet('{first_file}')").fetchall()
    assert "mes" in [row[0] for row in described]

    hive = f"read_parquet('{out}/*/*.parquet', hive_partitioning = true)"
    count = con.execute(f"SELECT count(*) FROM {hive} WHERE mes = '{MONTHS[1]}'").fetchone()[0]
    assert count == 1000
    con.close()


@pytest.mark.local
def test_database_file_and_temp_directory(local_location: LocalLocation) -> None:
    """O sandbox em arquivo sobrevive ao fechamento e reabre só de leitura; a pasta de transbordo é
    configurável."""
    folder = Path(local_location.child("duckdb"))
    folder.mkdir(exist_ok=True)
    database = folder / "sandbox.duckdb"
    spill = str(folder / "spill")

    con = duckdb.connect(str(database), config={"temp_directory": spill})
    con.register("amostra", sample_table().slice(0, 1000))
    con.execute("CREATE TABLE trabalho AS SELECT * FROM amostra")
    assert con.execute("SELECT current_setting('temp_directory')").fetchone()[0] == spill
    con.close()

    assert database.exists()
    again = duckdb.connect(str(database), read_only=True)
    assert again.execute("SELECT count(*) FROM trabalho").fetchone()[0] == 1000
    again.close()


def test_audit_queries(con: duckdb.DuckDBPyConnection) -> None:
    """As consultas da auditoria acham cada defeito de uma partição.

    Os defeitos: chave repetida, nulo, partição diferente da data de origem, JSON inválido, texto
    acima de ``String(200)`` em bytes.
    """
    con.execute(
        "CREATE TABLE lancamentos (id BIGINT, data_base DATE, data_base_str VARCHAR, "
        "valor DECIMAL(18,2), meta VARCHAR, descricao VARCHAR)"
    )
    con.execute(
        """
        INSERT INTO lancamentos VALUES
            (1, '2026-08-31', '2026-08-31', 10.00, '{"ok": true}', 'a'),
            (1, '2026-08-31', '2026-08-31', 20.00, NULL, 'b'),
            (2, '2026-08-31', '2026-08-31', NULL, '{invalido', repeat('x', 200)),
            (3, '2026-07-31', '2026-08-31', 5.00, NULL, repeat('ç', 101))
        """
    )

    duplicates = con.execute(
        "SELECT id, count(*) FROM lancamentos GROUP BY id HAVING count(*) > 1").fetchall()
    assert duplicates == [(1, 2)]

    # O String(n) do contrato é medido em bytes, a medida do VARCHAR(n) do Redshift: strlen conta
    # bytes, length conta caracteres, e 'ç' ocupa dois bytes. O octet_length do DuckDB só aceita
    # BLOB.
    lengths = con.execute("SELECT strlen(repeat('ç', 101)), length(repeat('ç', 101))").fetchone()
    assert lengths == (202, 101)
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
