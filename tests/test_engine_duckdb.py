"""``serialize_db.engine.duckdb``: o motor DuckDB sobre um Delta local criado no teste.

Os testes gravam sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``): a raiz Delta, o banco do
sandbox e a pasta de transbordo ficam numa pasta nova por teste. O modelo é o de ``Lancamento``,
particionado por ``data_base_str`` com a origem ``data_base``, com uma chave estrangeira para
``Conta``, e ``Projetado``, a tabela que o pipeline grava, com as mesmas colunas.

Eles conferem a configuração e a sessão única, a sessão a mais, a ingestão presa à versão e a poda
por intervalo, os parâmetros do statement, a versão publicada, o stream (o primeiro lote com a
consulta rodando, o orçamento, o cancelamento, os erros), o loader (a transação no ``close``, o nome
ocupado, a ordem do exemplo mensal), as formas por tabela, o ciclo com o pandas, a auditoria (cada
defeito, a dispensa da junção, a amostra, os não finitos, o órfão), a exportação nos dois modos e o
pipeline de exemplo num banco em arquivo. A extensão ``delta`` do DuckDB precisa estar na pasta de
extensões (``SERIALIZE_DB_DUCKDB_EXTENSIONS``, senão ``.duckdb/`` na raiz do repositório).
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import decimal
import json
import re
import threading
import time
import uuid
from collections.abc import Iterator
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from conftest import LocalLocation, record
from serialize_db import delta, schema
from serialize_db.engine import Engine
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine, DuckDBStream
from serialize_db.errors import ContractError, RegistrationRefused, SandboxError, SqlError
from serialize_db.storage import Storage

pytestmark = pytest.mark.local


class Base(DeclarativeBase):
    pass


class Conta(Base):
    __tablename__ = "cad_contas"
    id_conta: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    numero: Mapped[str] = mapped_column(sa.String(20), unique=True)


LANCAMENTO_INFO = {"serialize_db": {"partition_by": ["data_base_str"], "partition_source": "data_base",
                                    "sort_key": ["data_base", "id_lancamento"]}}


class Lancamento(Base):
    __tablename__ = "cad_lancamentos"
    __table_args__ = {"info": LANCAMENTO_INFO}
    id_lancamento: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    id_conta: Mapped[int] = mapped_column(sa.BigInteger, sa.ForeignKey("cad_contas.id_conta"))
    data_base: Mapped[dt.date] = mapped_column(sa.Date)
    valor: Mapped[float] = mapped_column(sa.Double)
    preco: Mapped[decimal.Decimal | None] = mapped_column(sa.Numeric(18, 2))
    area: Mapped[str | None] = mapped_column(sa.String(10))
    meta: Mapped[dict | None] = mapped_column(sa.JSON)
    to: Mapped[str | None] = mapped_column(sa.String(2))
    codigo: Mapped[str] = mapped_column(sa.String(20))
    data_base_str: Mapped[str] = mapped_column(sa.String(10))


class Projetado(Base):
    __tablename__ = "cad_lancamentos_projetados"
    __table_args__ = (sa.UniqueConstraint("codigo"), {"info": LANCAMENTO_INFO})
    id_lancamento: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    id_conta: Mapped[int] = mapped_column(sa.BigInteger)
    data_base: Mapped[dt.date] = mapped_column(sa.Date)
    valor: Mapped[float] = mapped_column(sa.Double)
    preco: Mapped[decimal.Decimal | None] = mapped_column(sa.Numeric(18, 2))
    area: Mapped[str | None] = mapped_column(sa.String(10))
    meta: Mapped[dict | None] = mapped_column(sa.JSON)
    to: Mapped[str | None] = mapped_column(sa.String(2))
    codigo: Mapped[str] = mapped_column(sa.String(20))
    data_base_str: Mapped[str] = mapped_column(sa.String(10))


ENTRIES = Lancamento.__table__
PROJECTED = Projetado.__table__
ACCOUNTS = Conta.__table__
METADATA = delta.commit_metadata("exec-2026-09-05", {"cad_contas": 1})
MONTHS = ["2026-03-31", "2026-04-30", "2026-05-31", "2026-06-30", "2026-07-31", "2026-08-31"]


@dataclasses.dataclass
class Setup:
    """A raiz Delta e o motor de um teste."""

    storage: Storage
    engine: DuckDBEngine
    folder: Path

    def uri(self, table: sa.Table) -> str:
        return self.storage.uri_of(f"prod/{table.name}")


@pytest.fixture
def setup(local_location: LocalLocation) -> Iterator[Setup]:
    """Uma pasta nova com a raiz Delta e a pasta do sandbox, e o motor sobre ela."""
    folder = Path(local_location.child(f"engine/{uuid.uuid4().hex[:8]}"))
    storage = Storage.for_uri(str(folder / "delta"))
    engine = DuckDBEngine(DuckDBConfig(threads=2, temp_directory=str(folder / "sandbox")), "exec-2026-09-05", storage)
    yield Setup(storage, engine, folder)
    engine.cleanup()


def entries(value: str, start: int, count: int, table: sa.Table = ENTRIES, valor: list[float] | None = None,
            area: str | None = "TI") -> pa.Table:
    """``count`` lançamentos da partição ``value`` com ids a partir de ``start``, no contrato."""
    ids = list(range(start, start + count))
    data = pa.table({
        "id_lancamento": pa.array(ids, pa.int64()),
        "id_conta": pa.array([1 + k % 3 for k in ids], pa.int64()),
        "data_base": pa.array([dt.date.fromisoformat(value)] * count, pa.date32()),
        "valor": pa.array(valor if valor is not None else [k / 4 for k in ids], pa.float64()),
        "preco": pa.array([decimal.Decimal(k) / 100 for k in ids], pa.decimal128(18, 2)),
        "area": pa.array([area] * count, pa.string()),
        "meta": pa.array([json.dumps({"k": k}) for k in ids]),
        "to": pa.array(["SP"] * count),
        "codigo": pa.array([f"L{k:06d}" for k in ids]),
        "data_base_str": pa.array([value] * count),
    })
    return schema.cast(data, table)


def published_table(setup: Setup, table: sa.Table, months: list[str], rows: int = 100) -> int:
    """A tabela Delta com ``rows`` linhas por mês; devolve a última versão."""
    uri = setup.uri(table)
    delta.create_table(uri, table, setup.storage)
    version = 0
    for index, month in enumerate(months):
        version = delta.publish_partition(uri, table, month, entries(month, 1 + index * rows, rows, table), METADATA, setup.storage)
    return version


def count_of(engine: DuckDBEngine, name: str) -> int:
    return engine.query(f'SELECT count(*) AS n FROM "{name}"').column("n")[0].as_py()


def exists(engine: DuckDBEngine, name: str) -> bool:
    return engine.name_in_use(name)


# ---------------------------------------------------------------- a sessão


def test_engine_config_and_single_session(setup: Setup, caplog: pytest.LogCaptureFixture) -> None:
    """As configurações pedidas, o ``memory_limit`` no padrão do DuckDB e o banco em arquivo dentro da
    pasta; três threads usam a mesma sessão, a tabela temporária de uma vale para as outras, uma
    primitiva dentro de ``session()`` não trava, e ``cleanup`` apaga o banco e o transbordo."""
    engine = setup.engine
    assert isinstance(engine, Engine)
    rows = engine.query("SELECT name, value FROM duckdb_settings() WHERE name IN ('threads', 'preserve_insertion_order')").to_pylist()
    assert {row["name"]: row["value"] for row in rows} == {"threads": "2", "preserve_insertion_order": "false"}
    database = Path(setup.folder / "sandbox" / "exec-2026-09-05.duckdb")
    assert database.exists()

    def create(k: int) -> None:
        engine.query(f"CREATE TABLE t{k} AS SELECT {k} AS k")

    threads = [threading.Thread(target=create, args=(k,)) for k in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert [count_of(engine, f"t{k}") for k in range(3)] == [1, 1, 1]

    engine.query("CREATE TEMP TABLE temporaria AS SELECT range AS id FROM range(10)")
    seen = {}
    worker = threading.Thread(target=lambda: seen.update(n=count_of(engine, "temporaria")))
    worker.start()
    worker.join()
    assert seen == {"n": 10}
    with engine.session() as connection:
        connection.execute("CREATE TABLE dentro AS SELECT 1 AS x")
        assert count_of(engine, "dentro") == 1 and engine.holds_session()

    other = DuckDBEngine(DuckDBConfig(temp_directory=str(setup.folder / "outro")), "exec-2026-09-06", setup.storage)
    limit = other.query("SELECT current_setting('memory_limit') AS m").column("m")[0].as_py()
    other.cleanup()
    assert limit.endswith("iB") and not (setup.folder / "outro" / "exec-2026-09-06.duckdb").exists()
    engine.cleanup()
    engine.cleanup()  # a segunda chamada não faz nada
    assert not database.exists() and not (setup.folder / "sandbox" / "exec-2026-09-05_transbordo").exists()
    with pytest.raises(duckdb.ConnectionException):
        engine.query("SELECT 1")


def test_temporary_folder_is_created_and_removed(local_location: LocalLocation) -> None:
    """Sem ``temp_directory``, o motor cria uma pasta nova e a apaga inteira no ``cleanup``."""
    storage = Storage.for_uri(local_location.child("engine_tmp_delta"))
    engine = DuckDBEngine(DuckDBConfig(), "exec-tmp", storage)
    folder = Path(engine._folder)
    assert folder.is_dir() and (folder / "exec-tmp.duckdb").exists()
    engine.cleanup()
    assert not folder.exists()


def test_new_session_runs_beside_the_main_one(setup: Setup) -> None:
    """A sessão a mais vê o que a principal confirmou e não a temporária dela, roda enquanto a principal
    está num bloco ``session()``, e a principal vê o que ela confirma; o ``with`` fecha só o cursor."""
    engine = setup.engine
    engine.query("CREATE TABLE confirmada AS SELECT range AS id FROM range(10)")
    engine.query("CREATE TEMP TABLE temporaria AS SELECT 1 AS x")
    with engine.new_session() as other:
        assert count_of(other, "confirmada") == 10
        with pytest.raises(duckdb.CatalogException, match="temporaria"):
            other.query("SELECT * FROM temporaria")
        seen = {}
        worker = threading.Thread(target=lambda: seen.update(n=count_of(other, "confirmada")))
        with engine.session():
            worker.start()
            worker.join(timeout=10)
        assert seen == {"n": 10}
        other.query("CREATE TABLE da_outra AS SELECT 1 AS x")
    assert count_of(engine, "da_outra") == 1
    assert Path(setup.folder / "sandbox" / "exec-2026-09-05.duckdb").exists()


# ---------------------------------------------------------------- a leitura do Delta


def test_ingest_pins_the_version(setup: Setup) -> None:
    """Um commit depois da abertura não aparece na view nem na tabela materializada; o nome ocupado é
    ``SandboxError``, e a versão ausente também."""
    version = published_table(setup, ENTRIES, MONTHS[:2])
    engine = setup.engine
    materialized = sa.Table("cad_lancamentos_materializada", sa.MetaData(), *[column._copy() for column in ENTRIES.columns], info=ENTRIES.info)
    engine.ingest(ENTRIES, setup.uri(ENTRIES), version)
    engine.ingest(materialized, setup.uri(ENTRIES), version, materialize=True)
    delta.publish_partition(setup.uri(ENTRIES), ENTRIES, MONTHS[2], entries(MONTHS[2], 500, 50), METADATA, setup.storage)
    assert count_of(engine, "cad_lancamentos") == 200 and count_of(engine, "cad_lancamentos_materializada") == 200
    with pytest.raises(SandboxError, match="ocupado"):
        engine.ingest(ENTRIES, setup.uri(ENTRIES), version)
    with pytest.raises(SandboxError, match="sem versão"):
        engine.ingest(PROJECTED, setup.uri(PROJECTED), None)


def test_ingest_opens_only_the_range_of_partitions(setup: Setup) -> None:
    """A ingestão de partições contíguas e de uma lista salteada abre só os arquivos do intervalo, pelo
    log ``FileSystem`` do DuckDB, e traz só as linhas pedidas."""
    version = published_table(setup, ENTRIES, MONTHS, rows=10)
    engine = setup.engine
    opened_by_case = {}
    for name, wanted in (("contiguas", MONTHS[1:3]), ("salteadas", [MONTHS[1], MONTHS[3]])):
        with engine.session() as connection:
            connection.execute("CALL enable_logging('FileSystem')")
            connection.execute("CALL truncate_duckdb_logs()")
        table = sa.Table(f"cad_lancamentos_{name}", sa.MetaData(), *[column._copy() for column in ENTRIES.columns], info=ENTRIES.info)
        engine.ingest(table, setup.uri(ENTRIES), version, partitions=wanted, materialize=True)
        with engine.session() as connection:
            messages = connection.execute("SELECT message FROM duckdb_logs WHERE type = 'FileSystem'").fetchall()
            connection.execute("CALL disable_logging()")
        opened = set()
        for (message,) in messages:
            if '"op":"OPEN"' in message and ".parquet" in message:
                opened.add(re.search(r"data_base_str=[0-9-]+", message).group(0))
        opened_by_case[name] = opened
        partitions = engine.query(f'SELECT DISTINCT data_base_str FROM "{table.name}" ORDER BY 1').column(0).to_pylist()
        assert partitions == sorted(wanted)
    assert opened_by_case["contiguas"] == {f"data_base_str={month}" for month in MONTHS[1:3]}
    assert opened_by_case["salteadas"] == {f"data_base_str={month}" for month in MONTHS[1:4]}


def test_published_reads_the_pinned_version(setup: Setup) -> None:
    """``published`` lê a versão fixada sem criar objeto no sandbox, e o ``loader`` da mesma tabela
    fica com o nome do modelo; sem versão, ``SandboxError``."""
    version = published_table(setup, PROJECTED, MONTHS[:2])
    engine = setup.engine
    source = engine.published(PROJECTED, setup.uri(PROJECTED), version)
    delta.publish_partition(setup.uri(PROJECTED), PROJECTED, MONTHS[2], entries(MONTHS[2], 900, 5, PROJECTED), METADATA, setup.storage)
    statement = sa.select(sa.func.count().label("n"), sa.func.max(source.c.id_lancamento).label("topo"))
    assert engine.query(statement).to_pylist() == [{"n": 200, "topo": 200}]
    assert not exists(engine, PROJECTED.name)
    engine.load(PROJECTED, entries(MONTHS[5], 1000, 3, PROJECTED))
    assert count_of(engine, PROJECTED.name) == 3
    with pytest.raises(SandboxError, match="ainda não existe"):
        engine.published(PROJECTED, setup.uri(PROJECTED), None)


# ---------------------------------------------------------------- consulta e parâmetros


def test_statement_parameters_expand_in_lists(setup: Setup) -> None:
    """Um statement com ``IN`` de lista, ``NOT IN`` e ``bindparam`` expansível roda por ``query`` e por
    ``stream``; um nome a mais ou a menos é ``SqlError`` antes de rodar."""
    engine = setup.engine
    engine.load(ENTRIES, entries(MONTHS[0], 1, 30))
    statement = (
        sa.select(ENTRIES.c.id_lancamento)
        .where(ENTRIES.c.id_conta.in_([1, 2]), ENTRIES.c.id_lancamento.notin_([1, 4]),
               ENTRIES.c.area.in_(sa.bindparam("areas", expanding=True)),
               ENTRIES.c.data_base_str == sa.bindparam("particao"))
        .order_by(ENTRIES.c.id_lancamento)
    )
    params = {"areas": ["TI", "RH"], "particao": MONTHS[0]}
    by_query = engine.query(statement, params).column(0).to_pylist()
    with engine.stream(statement, params, batch_size=5) as stream:
        by_stream = stream.read_all().column(0).to_pylist()
    expected = [k for k in range(1, 31) if 1 + k % 3 in (1, 2) and k not in (1, 4)]
    assert by_query == by_stream == expected
    with pytest.raises(SqlError, match="não fecham"):
        engine.query(statement, {"areas": ["TI"]})
    with pytest.raises(SqlError, match="não fecham"):
        engine.query(statement, {**params, "sobra": 1})


def test_query_keeps_percent_literals(setup: Setup) -> None:
    """``LIKE 'A%'`` num statement e num texto pronto chega ao DuckDB como está, e o texto pronto
    recebe os parâmetros por ``:nome``."""
    engine = setup.engine
    engine.load(ENTRIES, pa.concat_tables([entries(MONTHS[0], 1, 3, area="ABC"), entries(MONTHS[0], 10, 2, area="XYZ")]))
    statement = sa.select(sa.func.count().label("n")).where(ENTRIES.c.area.like("A%"))
    assert engine.query(statement).column("n")[0].as_py() == 3
    text = 'SELECT count(*) AS n FROM "cad_lancamentos" WHERE "area" LIKE \'X%\' AND "id_lancamento" > :minimo'
    assert engine.query(text, {"minimo": 10}).column("n")[0].as_py() == 1


# ---------------------------------------------------------------- o stream


def test_stream_delivers_each_batch_while_the_query_runs(setup: Setup) -> None:
    """O primeiro lote com a consulta rodando, sem arquivo com o cliente acompanhando; um
    comando no meio da leitura roda depois da consulta; a tabela temporária e o stream dentro de
    ``session()``; o abandono para a consulta e apaga o arquivo; o erro chega na construção ou na
    leitura seguinte ao último lote."""
    engine = setup.engine
    engine.query("CREATE TABLE numeros AS SELECT range AS id, 'x' || range AS s FROM range(3_000_000)")
    spool = Path(engine._spool_folder)
    started = time.perf_counter()
    first_batch = None
    query_running = None
    seen = 0
    total = 0
    # Com preserve_insertion_order = false, a ordem sem ORDER BY é a do DuckDB: a soma confere as linhas.
    with engine.stream("SELECT id, s FROM numeros WHERE id % 3 <> 0") as stream:
        for batch in stream:
            if first_batch is None:
                first_batch = time.perf_counter() - started
                query_running = stream._thread.is_alive()
            seen += batch.num_rows
            total += pc.sum(batch.column("id")).as_py()
        assert stream._spool.spilled == 0
    assert seen == 2_000_000 and total == sum(k for k in range(3_000_000) if k % 3 != 0)
    assert list(spool.iterdir()) == []
    record("engine.stream.first_batch", f"{first_batch:.3f} s, com a consulta rodando: {query_running}")

    with DuckDBStream(engine, "SELECT id FROM numeros", [], budget=10_000) as stream:
        stream.read_next_batch()
        with engine.session() as connection:
            connection.execute("SELECT 1").fetchall()
            assert stream._spool.done

    engine.query("CREATE TEMP TABLE pares AS SELECT id FROM numeros WHERE id % 2 = 0")
    with engine.session(), engine.stream("SELECT count(*) AS n FROM pares") as inside:
        assert inside._thread is None and inside.read_all().column("n").to_pylist() == [1_500_000]
    with engine.stream("SELECT id FROM numeros WHERE id < 5000", batch_size=1000) as source:
        assert pa.RecordBatchReader.from_stream(source).read_all().num_rows == 5000

    abandoned = DuckDBStream(engine, "SELECT id FROM numeros", [], batch_size=1000, budget=10_000)
    abandoned.read_next_batch()
    thread = abandoned._thread
    del abandoned
    thread.join(timeout=5)
    assert not thread.is_alive() and list(spool.iterdir()) == []

    with pytest.raises(duckdb.CatalogException, match="nao_existe"):
        engine.stream("SELECT * FROM nao_existe")
    failing = "SELECT CAST(CASE WHEN id = 2_900_000 THEN 'x' ELSE CAST(id AS VARCHAR) END AS INTEGER) AS n FROM numeros"
    for budget in (64 * 2**20, 10_000):
        with pytest.raises((duckdb.ConversionException, OSError), match="Could not convert string 'x' to INT32"):
            with DuckDBStream(engine, failing, [], batch_size=100_000, budget=budget) as stream:
                for _ in stream:
                    pass
        assert list(spool.iterdir()) == []


def test_stream_spills_after_the_budget_and_keeps_the_order(setup: Setup) -> None:
    """Com o cliente lento e um orçamento de dois lotes, a memória para no orçamento, o resto vai para o
    arquivo, as linhas saem todas e na ordem, e o ``close`` apaga o arquivo."""
    engine = setup.engine
    engine.query("CREATE TABLE numeros AS SELECT range AS id, 'x' || range AS s FROM range(3_000_000)")
    budget = 2 * 1_900_000
    ids = []
    peak = 0
    with DuckDBStream(engine, "SELECT id, s FROM numeros ORDER BY id", [], budget=budget) as stream:
        for batch in stream:
            time.sleep(0.01)
            peak = max(peak, stream._spool.memory_bytes)
            ids.extend(batch.column("id").to_pylist())
        spilled = stream._spool.spilled
    assert ids == list(range(3_000_000)) and spilled > 0 and peak <= budget
    assert list(Path(engine._spool_folder).iterdir()) == []


def test_close_and_cleanup_interrupt_the_running_query(setup: Setup) -> None:
    """O ``close`` depois do primeiro lote de uma varredura longa cancela a consulta, e a sessão
    continua usável; o ``cleanup`` cancela a ordenação de um stream que outra thread ainda constrói."""
    engine = setup.engine
    engine.query("CREATE TABLE numeros AS SELECT range AS id FROM range(20_000_000)")
    # Com preserve_insertion_order = false, o ajuste do motor, esta consulta entrega o primeiro lote só
    # no fim (1,093 s de 1,093 s com duas threads e o md5 simples, leitura de 2026-09-23); com a
    # ordem preservada, o primeiro lote costuma chegar antes e a varredura continua, o intervalo em
    # que o close a cancela. Com a máquina carregada, o primeiro lote pode sair no fim e não há o que
    # cancelar: o erro fica nulo, e o que vale nos dois casos é a thread terminada e a sessão livre.
    with engine.session() as connection:
        connection.execute("SET preserve_insertion_order = true")
    stream = engine.stream("SELECT id FROM numeros WHERE id < 150000 OR md5(md5(id::VARCHAR)) = 'x'")
    stream.read_next_batch()
    started = time.perf_counter()
    stream.close()
    closed = time.perf_counter() - started
    error = stream._spool.error
    record("engine.stream.close_interrupts", f"{closed:.3f} s, erro {type(error).__name__}")
    assert error is None or "INTERRUPT" in str(error).upper()
    assert not stream._thread.is_alive() and closed < 1.0
    assert count_of(engine, "numeros") == 20_000_000

    outcome = {}

    def construct() -> None:
        try:
            engine.stream("SELECT id FROM numeros ORDER BY hash(id)")
            outcome["error"] = None
        except (duckdb.Error, OSError) as error:
            outcome["error"] = error

    worker = threading.Thread(target=construct)
    worker.start()
    time.sleep(0.2)
    engine.cleanup()
    worker.join(timeout=30)
    assert "INTERRUPT" in str(outcome["error"]).upper()


# ---------------------------------------------------------------- o loader


def test_loader_creates_and_inserts_in_one_transaction_on_close(setup: Setup) -> None:
    """Nada existe antes do ``close``; a exceção do cliente, o lote recusado pelo ``cast`` e o erro do
    ``INSERT`` não deixam tabela; ``rows`` conta as linhas; o loader sem lote cria a tabela vazia."""
    engine = setup.engine
    visible = []
    with engine.loader(PROJECTED) as loader:
        for k in range(5):
            loader.write(entries(MONTHS[0], 1 + k * 100, 100, PROJECTED))
            visible.append(exists(engine, PROJECTED.name))
    assert loader.rows == 500 and set(visible) == {False} and count_of(engine, PROJECTED.name) == 500
    assert list(Path(engine._spool_folder).iterdir()) == []

    other = sa.Table("cad_segunda", sa.MetaData(), *[column._copy() for column in PROJECTED.columns])
    with pytest.raises(ValueError, match="falha do cliente"):
        with engine.loader(other) as loader:
            loader.write(entries(MONTHS[0], 1, 10, PROJECTED))
            raise ValueError("falha do cliente")
    assert not exists(engine, "cad_segunda")

    with pytest.raises(ContractError, match="texto de"):
        with engine.loader(other) as loader:
            loader.write(entries(MONTHS[0], 1, 10, PROJECTED))
            long_area = entries(MONTHS[0], 11, 1, PROJECTED)
            loader.write(long_area.set_column(long_area.schema.get_field_index("area"), "area", pa.array(["x" * 11])))
    assert not exists(engine, "cad_segunda")

    # O lote sem a coluna NOT NULL valor passa pelo cast e falha no INSERT; o ROLLBACK desfaz o CREATE.
    with pytest.raises(duckdb.ConstraintException):
        with engine.loader(other) as loader:
            loader.write(entries(MONTHS[0], 1, 10, PROJECTED).drop_columns(["valor"]))
    assert not exists(engine, "cad_segunda")

    with engine.loader(other) as loader:
        pass
    assert exists(engine, "cad_segunda") and count_of(engine, "cad_segunda") == 0


def test_loader_opened_after_a_stream_does_not_wait_for_its_query(setup: Setup) -> None:
    """Com ``stream`` e depois ``loader`` no mesmo ``with``, o primeiro lote chega com a consulta
    rodando, e a tabela tem todas as linhas no fim."""
    engine = setup.engine
    engine.query(
        "CREATE TABLE fonte AS SELECT range AS id_lancamento, 1 AS id_conta, DATE '2026-08-31' AS data_base, "
        "range / 4 AS valor, 'L' || range AS codigo, '2026-08-31' AS data_base_str FROM range(3_000_000)")
    query_running = None
    with engine.stream("SELECT * FROM fonte") as stream, engine.loader(PROJECTED) as loader:
        for batch in stream:
            if query_running is None:
                query_running = stream._thread.is_alive()
            loader.write(batch)
    assert query_running and count_of(engine, PROJECTED.name) == 3_000_000


def test_loader_refuses_a_name_in_use(setup: Setup) -> None:
    """O ``loader`` sobre a view do ``ingest``, sobre a tabela materializada e sobre a tabela de um
    ``loader`` anterior levanta ``SandboxError`` antes do primeiro lote, e o objeto não muda."""
    version = published_table(setup, ENTRIES, MONTHS[:1], rows=10)
    engine = setup.engine
    engine.ingest(ENTRIES, setup.uri(ENTRIES), version)
    materialized = sa.Table("cad_materializada", sa.MetaData(), *[column._copy() for column in ENTRIES.columns], info=ENTRIES.info)
    engine.ingest(materialized, setup.uri(ENTRIES), version, materialize=True)
    engine.load(PROJECTED, entries(MONTHS[0], 1, 5, PROJECTED))
    for table in (ENTRIES, materialized, PROJECTED):
        with pytest.raises(SandboxError, match="run.published"):
            engine.loader(table)
    assert [count_of(engine, name) for name in ("cad_lancamentos", "cad_materializada", PROJECTED.name)] == [10, 10, 5]


def test_query_and_load_match_stream_and_loader(setup: Setup) -> None:
    """``query`` é ``stream(...).read_all()``; ``load`` de ``pa.Table``, lote, leitor e iterável dá o
    mesmo resultado; o DataFrame é recusado com a mensagem que aponta ``from_pandas``."""
    engine = setup.engine
    data = entries(MONTHS[0], 1, 1000, PROJECTED)
    forms = {
        "tabela": data,
        "lote": data.combine_chunks().to_batches()[0],
        "leitor": pa.RecordBatchReader.from_batches(data.schema, data.to_batches(max_chunksize=100)),
        "iteravel": data.to_batches(max_chunksize=300),
    }
    totals = {}
    for name, form in forms.items():
        table = sa.Table(f"forma_{name}", sa.MetaData(), *[column._copy() for column in PROJECTED.columns])
        assert engine.load(table, form) == 1000
        totals[name] = engine.query(f'SELECT count(*) AS n, sum(preco) AS p FROM "forma_{name}"').to_pylist()
    assert len({json.dumps(value, default=str) for value in totals.values()}) == 1
    statement = sa.select(sa.text("*")).select_from(sa.table("forma_tabela")).order_by(sa.text("id_lancamento"))
    with engine.stream(statement) as stream:
        assert stream.read_all().equals(engine.query(statement))
    with pytest.raises(ContractError, match="from_pandas"):
        engine.load(PROJECTED, data.to_pandas())
    with pytest.raises(ContractError, match="from_pandas"):
        engine.load(PROJECTED, "texto")


def test_pandas_round_trip_keeps_contract_types(setup: Setup) -> None:
    """``to_pandas(types_mapper=pd.ArrowDtype)`` e ``from_pandas`` mantêm ``decimal128(18, 2)`` e
    ``date32`` no caminho ``query``, lógica em pandas e ``load``."""
    engine = setup.engine
    engine.load(ENTRIES, entries(MONTHS[0], 1, 100))
    frame = engine.query(sa.select(ENTRIES)).to_pandas(types_mapper=pd.ArrowDtype)
    assert str(frame["preco"].dtype) == "decimal128(18, 2)[pyarrow]" and str(frame["data_base"].dtype) == "date32[day][pyarrow]"
    frame["id_lancamento"] = frame["id_lancamento"] + 1000
    engine.load(PROJECTED, pa.Table.from_pandas(frame, preserve_index=False))
    loaded = engine.query(sa.select(PROJECTED))
    assert loaded.schema.field("preco").type == pa.decimal128(18, 2) and loaded.schema.field("data_base").type == pa.date32()
    assert engine.query('SELECT sum(preco) AS p FROM "cad_lancamentos_projetados"').column("p")[0].as_py() == sum(
        decimal.Decimal(k) / 100 for k in range(1, 101))


# ---------------------------------------------------------------- a auditoria


def test_audit_finds_each_defect(setup: Setup) -> None:
    """Nulo, texto acima do ``String(n)`` em bytes e dentro dele em caracteres, partição fora da origem,
    valor de partição fora da regra, JSON inválido, chave repetida na partição e contra a publicada, e
    chave única repetida contra a publicada."""
    version = published_table(setup, PROJECTED, MONTHS[:2], rows=10)
    engine = setup.engine
    good = entries(MONTHS[2], 100, 6, PROJECTED)
    batch = good.to_pylist()
    batch[0]["id_lancamento"] = batch[1]["id_lancamento"]                     # repetida na partição
    batch[2]["id_lancamento"] = 3                                             # repetida contra a publicada
    batch[3]["area"] = "ação ação"                                            # 9 caracteres, 13 bytes
    batch[4]["data_base"] = dt.date(2026, 7, 31)                              # fora da origem da partição
    batch[5]["meta"] = "{nao json"                                          # JSON inválido
    # A coluna JSON do DuckDB recusa o texto inválido na carga: a tabela nasce por CTAS, com meta em
    # VARCHAR, como uma tabela que o pipeline criasse por SQL.
    with engine.session() as connection:
        connection.register("dados", pa.Table.from_pylist(batch, schema=good.schema))
        connection.execute('CREATE TABLE "cad_lancamentos_projetados" AS SELECT * FROM dados')
        connection.unregister("dados")
        # O valor de partição fora da regra entra por SQL: o cast não confere a regra, a auditoria confere.
        connection.execute('INSERT INTO "cad_lancamentos_projetados" BY NAME SELECT 999 AS id_lancamento, 9 AS id_conta, '
                           "DATE '2026-05-31' AS data_base, 1.0 AS valor, 'L999' AS codigo, 'd''agua' AS data_base_str")
    report = engine.audit(PROJECTED, [MONTHS[2]], setup.uri(PROJECTED), version)
    results = {result.name: result for result in report.results}
    rows = report.totals[MONTHS[2]]
    assert (rows["texto_area"], rows["particao_data_base_str"], rows["json_meta"]) == (1, 1, 1)
    assert results["chave_id_lancamento"].defects == 1
    assert results["chave_id_lancamento_publicada"].sample.column(0).to_pylist() == [3]
    assert results["chave_codigo_publicada"].defects == 0
    assert not report.passed and not results["linhas"].passed

    whole = engine.audit(PROJECTED, None)
    assert whole.totals["d'agua"]["valor_data_base_str"] == 1
    engine.query('INSERT INTO "cad_lancamentos_projetados" BY NAME SELECT 1000 AS id_lancamento, 9 AS id_conta, '
                 "DATE '2026-05-31' AS data_base, 1.0 AS valor, 'L1000' AS codigo, NULL AS data_base_str")
    assert engine.audit(PROJECTED, None).totals[None]["nulo_data_base_str"] == 1


def test_audit_unique_key_against_the_published_version(setup: Setup) -> None:
    """A chave única sem a coluna de partição, repetida entre a execução e a versão publicada, reprova
    pela consulta contra ``published``."""
    uri = setup.uri(PROJECTED)
    delta.create_table(uri, PROJECTED, setup.storage)
    published = entries(MONTHS[0], 1, 3, PROJECTED)
    version = delta.publish_partition(uri, PROJECTED, MONTHS[0], published, METADATA, setup.storage)
    repeated = entries(MONTHS[1], 100, 1, PROJECTED).to_pylist()[0]
    repeated["codigo"] = published.column("codigo")[0].as_py()
    setup.engine.load(PROJECTED, pa.Table.from_pylist([repeated], schema=published.schema))
    report = setup.engine.audit(PROJECTED, [MONTHS[1]], uri, version)
    results = {result.name: result for result in report.results}
    assert results["chave_codigo_publicada"].sample.column("codigo").to_pylist() == ["L000001"]
    assert results["linhas"].passed and not report.passed


def test_audit_skips_the_published_join_above_max_key(setup: Setup) -> None:
    """Com as chaves da execução acima do ``max_key`` da versão fixada, a junção não roda e o relatório
    diz por quê; com uma chave abaixo dele, a junção roda e acha a repetição."""
    version = published_table(setup, PROJECTED, MONTHS[:2], rows=10)
    engine = setup.engine
    engine.load(PROJECTED, entries(MONTHS[2], 1000, 5, PROJECTED))
    report = engine.audit(PROJECTED, [MONTHS[2]], setup.uri(PROJECTED), version)
    skipped = {result.name: result for result in report.results}["chave_id_lancamento_publicada"]
    assert skipped.passed and skipped.reason.startswith("dispensada") and report.passed

    engine.query('DROP TABLE "cad_lancamentos_projetados"')
    engine.load(PROJECTED, entries(MONTHS[2], 15, 5, PROJECTED))
    run = {result.name: result for result in engine.audit(PROJECTED, [MONTHS[2]], setup.uri(PROJECTED), version).results}
    assert run["chave_id_lancamento_publicada"].reason == "" and run["chave_id_lancamento_publicada"].defects == 5


def test_audit_report_samples_failing_rows(setup: Setup) -> None:
    """Até 20 linhas inteiras por verificação reprovada; a de linhas busca as suas por contador, e a
    aprovada não traz amostra; ``sql()`` junta os textos; os não finitos entram sem reprovar."""
    engine = setup.engine
    data = entries(MONTHS[0], 1, 30, PROJECTED, valor=[float("nan")] + [1.0] * 29).to_pylist()
    for row in data:
        row["area"] = "x" * 11 if row["id_lancamento"] <= 25 else row["area"]
    with engine.session() as connection:
        connection.register("dados", pa.Table.from_pylist(data))
        connection.execute(schema.ddl(PROJECTED, "duckdb"))
        connection.execute('INSERT INTO "cad_lancamentos_projetados" BY NAME SELECT * FROM dados')
        connection.unregister("dados")
    report = engine.audit(PROJECTED, [MONTHS[0]])
    rows = {result.name: result for result in report.results}["linhas"]
    assert rows.defects == 25 and rows.sample.num_rows == 20 and set(rows.sample.column_names) >= {"id_lancamento", "area"}
    assert {result.name: result for result in report.results}["chave_id_lancamento"].sample.num_rows == 0
    assert report.nonfinite_columns == {MONTHS[0]: ("valor",)} and report.totals[MONTHS[0]]["naofinito_valor"] == 1
    assert "-- linhas\nSELECT" in report.sql() and "-- chave_id_lancamento" in report.sql()


def test_audit_orphan_against_a_referenced_table_outside_the_sandbox(setup: Setup) -> None:
    """Com ``foreign_keys=True``, a tabela referenciada fora do sandbox entra pela versão fixada de
    ``referenced``, e o órfão aparece; sem o argumento, a verificação fica em ``not_run``."""
    accounts_uri = setup.uri(ACCOUNTS)
    delta.create_table(accounts_uri, ACCOUNTS, setup.storage)
    accounts = schema.cast(pa.table({"id_conta": pa.array([1, 2], pa.int64()), "numero": ["A", "B"]}), ACCOUNTS)
    accounts_version = delta.publish_partition(accounts_uri, ACCOUNTS, None, accounts, METADATA, setup.storage)
    engine = setup.engine
    engine.load(ENTRIES, entries(MONTHS[0], 1, 6))  # id_conta 1, 2 e 3: a conta 3 não existe
    report = engine.audit(ENTRIES, [MONTHS[0]], foreign_keys=True, referenced={"cad_contas": (accounts_uri, accounts_version)})
    orphans = {result.name: result for result in report.results}["orfao_id_conta"]
    assert orphans.sample.column("id_conta").to_pylist() == [3] and not report.passed
    assert "orfao_id_conta (sem foreign_keys=True)" in engine.audit(ENTRIES, [MONTHS[0]]).not_run


# ---------------------------------------------------------------- a exportação


def test_export_partition_modes_produce_the_same_partition(setup: Setup) -> None:
    """``register`` e ``rewrite`` sobre a mesma partição dão as mesmas linhas e somas; o ``register``
    poda pela estatística, sem o mínimo e o máximo da coluna com ``NaN``; ``expected_rows`` diferente
    recusa nos dois modos."""
    engine = setup.engine
    data = entries(MONTHS[1], 1, 1000, PROJECTED, valor=[float("nan")] + [k / 4 for k in range(2, 1001)])
    engine.load(PROJECTED, data)
    uris = {mode: setup.storage.uri_of(f"prod/{mode}/{PROJECTED.name}") for mode in ("register", "rewrite")}
    for mode, uri in uris.items():
        delta.create_table(uri, PROJECTED, setup.storage)
        version = engine.export_partition(PROJECTED, uri, MONTHS[1], METADATA, mode, expected_rows=1000,
                                          columns_without_min_max=["valor"])
        assert version == 1
    totals = [
        scan(setup, f"SELECT count(*), sum(preco), sum(id_lancamento), count(*) FILTER (WHERE isnan(valor)) FROM delta_scan('{uri}')")
        for uri in uris.values()
    ]
    assert totals[0] == totals[1] == [(1000, decimal.Decimal("5005.00"), 500500, 1)]
    for uri in uris.values():
        stats = pa.table(delta.open_table(uri, setup.storage).get_add_actions(flatten=True))
        assert stats.column("max.valor").null_count == stats.num_rows
        assert stats.column("max.id_lancamento").to_pylist() == [1000]
    plan = scan(setup, f"EXPLAIN ANALYZE SELECT count(*) FROM delta_scan('{uris['register']}') WHERE id_lancamento > 5000")[0][1]
    assert re.search(r"Scanning Files: 0/1", plan)
    files = setup.storage.list_files(setup.storage.relative(uris["register"]), ".parquet")
    assert re.fullmatch(rf"prod/register/{PROJECTED.name}/data_base_str={MONTHS[1]}/exec-2026-09-05_[0-9a-f]{{32}}\.parquet", files[0])
    for mode, uri in uris.items():
        with pytest.raises(RegistrationRefused):
            engine.export_partition(PROJECTED, uri, MONTHS[1], METADATA, mode, expected_rows=999)
        assert delta.open_table(uri, setup.storage).version() == 1


def scan(setup: Setup, text: str) -> list[tuple]:
    """As linhas de uma consulta numa conexão DuckDB própria, fora do motor."""
    connection = setup.storage.duckdb_connect()
    try:
        return connection.execute(text).fetchall()
    finally:
        connection.close()


def test_example_pipeline_in_a_file_backed_database(setup: Setup) -> None:
    """O pipeline de exemplo: seis partições materializadas e a dimensão em view, um ``select`` com
    ``join`` em lotes para o ``loader``, auditoria com a versão publicada, exportação; ``cleanup``
    apaga o arquivo do banco."""
    engine = setup.engine
    entries_version = published_table(setup, ENTRIES, MONTHS, rows=200)
    accounts_uri = setup.uri(ACCOUNTS)
    delta.create_table(accounts_uri, ACCOUNTS, setup.storage)
    accounts = schema.cast(pa.table({"id_conta": pa.array([1, 2, 3], pa.int64()), "numero": ["A", "B", "C"]}), ACCOUNTS)
    accounts_version = delta.publish_partition(accounts_uri, ACCOUNTS, None, accounts, METADATA, setup.storage)
    projected_uri = setup.uri(PROJECTED)
    delta.create_table(projected_uri, PROJECTED, setup.storage)

    engine.ingest(ENTRIES, setup.uri(ENTRIES), entries_version, partitions=MONTHS, materialize=True)
    engine.ingest(ACCOUNTS, accounts_uri, accounts_version)
    statement = (
        sa.select(ENTRIES)
        .join_from(ENTRIES, ACCOUNTS, ENTRIES.c.id_conta == ACCOUNTS.c.id_conta)
        .where(ENTRIES.c.data_base_str == sa.bindparam("particao"), ACCOUNTS.c.numero != "C")
    )
    with engine.stream(statement, {"particao": MONTHS[-1]}, batch_size=50) as stream, engine.loader(PROJECTED) as loader:
        for batch in stream:
            loader.write(batch)
    report = engine.audit(PROJECTED, [MONTHS[-1]], projected_uri, 0, foreign_keys=True)
    assert report.passed, report.results
    version = engine.export_partition(PROJECTED, projected_uri, MONTHS[-1], METADATA, "register", report.rows(MONTHS[-1]),
                                      report.nonfinite_columns.get(MONTHS[-1], ()))
    assert version == 1
    written = scan(setup, f"SELECT count(*) FROM delta_scan('{projected_uri}')")[0][0]
    assert written == report.rows(MONTHS[-1]) == 200 - len([k for k in range(1001, 1201) if 1 + k % 3 == 3])
    database = Path(setup.folder / "sandbox" / "exec-2026-09-05.duckdb")
    assert database.exists()
    engine.cleanup()
    assert not database.exists()
