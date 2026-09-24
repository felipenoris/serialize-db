"""``serialize_db.engine.duckdb``: o motor DuckDB sobre um Delta local criado no teste.

Os testes gravam sob ``SERIALIZE_DB_TEST_LOCAL_ROOT`` (marcador ``local``): a raiz Delta, o banco do
sandbox e a pasta de transbordo ficam numa pasta nova por teste. O modelo é o de ``Lancamento``,
particionado por ``data_base_str`` com a origem ``data_base``, com uma chave estrangeira para
``Conta``, e ``Projetado``, a tabela que o pipeline grava, com as mesmas colunas.

Eles conferem a configuração com os limites lidos do ambiente e a sessão única, a sessão a mais, a
ingestão presa à versão e a poda por intervalo, os parâmetros do statement, a versão publicada, o
stream (o primeiro lote com a consulta rodando, o orçamento, o cancelamento, os erros), o loader (a
transação no ``close``, o loader abandonado, o nome ocupado, a ordem do exemplo mensal, o pipeline
de três estágios, a leitura durante uma carga esquecida), as formas por tabela, o ciclo com o
pandas, a auditoria (cada defeito, a dispensa da junção, a amostra, os não finitos, o órfão), a
exportação pelo registro do arquivo do ``COPY`` e o pipeline de exemplo num banco em arquivo. A
extensão ``delta`` do DuckDB precisa estar na pasta de extensões
(``SERIALIZE_DB_DUCKDB_EXTENSIONS``, senão ``.duckdb/`` na raiz do repositório).
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import decimal
import json
import re
import tempfile
import threading
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pytest
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from conftest import LocalLocation, opened_partition_folders, record
from serialize_db import delta, resources, schema
from serialize_db.audit import AuditReport, CheckResult
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


LANCAMENTO_INFO = {"serialize_db": {"partition_by": ["data_base_str"],
                                    "partition_source": "data_base",
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
EXECUTION_ID = "exec-2026-09-05"
METADATA = delta.commit_metadata(EXECUTION_ID, {"cad_contas": 1})
MONTHS = ["2026-03-31", "2026-04-30", "2026-05-31", "2026-06-30", "2026-07-31", "2026-08-31"]


@dataclasses.dataclass
class Setup:
    """A raiz Delta e o motor de um teste."""

    storage: Storage
    engine: DuckDBEngine
    folder: Path

    def uri(self, table: sa.Table) -> str:
        """A pasta da tabela no ambiente ``prod``."""
        return self.storage.uri_of(f"prod/{table.name}")

    def database_file(self) -> Path:
        """O banco em arquivo do sandbox, que o ``cleanup`` apaga."""
        return self.folder / "sandbox" / f"{EXECUTION_ID}.duckdb"


@pytest.fixture
def setup(local_location: LocalLocation) -> Iterator[Setup]:
    """Uma pasta nova com a raiz Delta e a pasta do sandbox, e o motor sobre ela."""
    folder = Path(local_location.child(f"engine/{uuid.uuid4().hex[:8]}"))
    storage = Storage.for_uri(str(folder / "delta"))
    config = DuckDBConfig(threads=2, temp_directory=str(folder / "sandbox"))
    engine = DuckDBEngine(config, EXECUTION_ID, storage)
    yield Setup(storage, engine, folder)
    engine.cleanup()


def account_of(entry_id: int) -> int:
    """A conta do lançamento ``entry_id`` em ``entries``: 1, 2 e 3 em rodízio."""
    return 1 + entry_id % 3


def entries(value: str, start: int, count: int, table: sa.Table = ENTRIES,
            valor: list[float] | None = None, area: str | None = "TI") -> pa.Table:
    """``count`` lançamentos da partição ``value`` com ids a partir de ``start``, no contrato."""
    ids = list(range(start, start + count))
    data = pa.table({
        "id_lancamento": pa.array(ids, pa.int64()),
        "id_conta": pa.array([account_of(k) for k in ids], pa.int64()),
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
    """A tabela Delta com ``rows`` linhas por mês e ids contíguos a partir de 1; devolve a última
    versão."""
    uri = setup.uri(table)
    delta.create_table(uri, table, setup.storage)
    version = 0
    for index, month in enumerate(months):
        data = entries(month, 1 + index * rows, rows, table)
        version = delta.publish_partition(uri, table, month, data, METADATA, setup.storage)
    return version


def published_accounts(setup: Setup, numbers: list[str]) -> int:
    """``cad_contas`` publicada com uma conta por número, ids a partir de 1; devolve a versão."""
    uri = setup.uri(ACCOUNTS)
    delta.create_table(uri, ACCOUNTS, setup.storage)
    ids = pa.array(range(1, len(numbers) + 1), pa.int64())
    accounts = schema.cast(pa.table({"id_conta": ids, "numero": numbers}), ACCOUNTS)
    return delta.publish_partition(uri, ACCOUNTS, None, accounts, METADATA, setup.storage)


def count_of(engine: DuckDBEngine, name: str) -> int:
    """As linhas da tabela ou view ``name`` do sandbox."""
    return engine.query(f'SELECT count(*) AS n FROM "{name}"').column("n")[0].as_py()


def spool_files(engine: DuckDBEngine) -> list[Path]:
    """Os arquivos na pasta de transbordo do motor."""
    return list(Path(engine._spool_folder).iterdir())


def scan(storage: Storage, text: str) -> list[tuple]:
    """As linhas de uma consulta numa conexão DuckDB própria, fora do motor."""
    with storage.duckdb_connect() as connection:
        return connection.execute(text).fetchall()


def result_of(report: AuditReport, name: str) -> CheckResult:
    """A verificação ``name`` do relatório."""
    for result in report.results:
        if result.name == name:
            return result
    raise KeyError(name)


# ---------------------------------------------------------------- a sessão


def test_engine_config_and_single_session(setup: Setup, monkeypatch: pytest.MonkeyPatch) -> None:
    """As configurações pedidas, os limites lidos do ambiente quando a configuração os omite e o
    banco em arquivo dentro da pasta; três threads usam a mesma sessão, a tabela temporária de uma
    vale para as outras, uma primitiva dentro de ``session()`` não trava, e ``cleanup`` apaga o
    banco e o transbordo."""
    engine = setup.engine
    assert isinstance(engine, Engine)
    settings_query = ("SELECT name, value FROM duckdb_settings() "
                      "WHERE name IN ('threads', 'preserve_insertion_order')")
    rows = engine.query(settings_query).to_pylist()
    settings = {row["name"]: row["value"] for row in rows}
    assert settings == {"threads": "2", "preserve_insertion_order": "false"}
    database = setup.database_file()
    assert database.exists()

    # Três threads usam a mesma sessão.
    def create(k: int) -> None:
        engine.query(f"CREATE TABLE t{k} AS SELECT {k} AS k")

    threads = [threading.Thread(target=create, args=(k,)) for k in range(3)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert [count_of(engine, f"t{k}") for k in range(3)] == [1, 1, 1]

    # A tabela temporária criada numa thread vale para as outras.
    engine.query("CREATE TEMP TABLE temporaria AS SELECT range AS id FROM range(10)")
    with ThreadPoolExecutor(max_workers=1) as pool:
        counted = pool.submit(count_of, engine, "temporaria").result(timeout=10)
    assert counted == 10

    # Uma primitiva dentro de session() não trava.
    with engine.session() as connection:
        connection.execute("CREATE TABLE dentro AS SELECT 1 AS x")
        assert count_of(engine, "dentro") == 1
        assert engine.holds_session()

    # Os limites lidos do ambiente, noutro motor: metade dos 2 GiB disponíveis e a cota de uma
    # CPU, num /proc e num cgroup fabricados; o memory_limit informado fica no lugar do lido.
    machine = setup.folder / "maquina"
    fabricated = {
        "proc/meminfo": "MemAvailable:   2097152 kB\n",
        "proc/self/cgroup": "0::/\n",
        "cgroup/cpu.max": "100000 100000\n",
    }
    for relative, text in fabricated.items():
        (machine / relative).parent.mkdir(parents=True, exist_ok=True)
        (machine / relative).write_text(text)
    monkeypatch.setattr(resources, "_PROC", machine / "proc")
    monkeypatch.setattr(resources, "_CGROUP_ROOT", machine / "cgroup")
    limits_query = "SELECT current_setting('memory_limit') AS m, current_setting('threads') AS t"
    for memory_limit, expected in [(None, "1.0 GiB"), ("768MiB", "768.0 MiB")]:
        other_config = DuckDBConfig(memory_limit=memory_limit,
                                    temp_directory=str(setup.folder / "outro"))
        other = DuckDBEngine(other_config, "exec-2026-09-06", setup.storage)
        limits = other.query(limits_query).to_pylist()[0]
        other.cleanup()
        assert limits == {"m": expected, "t": 1}
    assert not (setup.folder / "outro" / "exec-2026-09-06.duckdb").exists()

    # O cleanup apaga o banco e o transbordo e fecha a sessão.
    engine.cleanup()
    engine.cleanup()  # a segunda chamada não faz nada
    assert not database.exists()
    assert not (setup.folder / "sandbox" / f"{EXECUTION_ID}_transbordo").exists()
    with pytest.raises(duckdb.ConnectionException):
        engine.query("SELECT 1")


def test_temporary_folder_is_created_and_removed(local_location: LocalLocation,
                                                 monkeypatch: pytest.MonkeyPatch) -> None:
    """Sem ``temp_directory``, o motor cria uma pasta nova e a apaga inteira no ``cleanup``."""
    # A pasta temporária do processo aponta para a raiz local: a suíte só grava sob ela.
    temporary = Path(local_location.child(f"engine_tmp/{uuid.uuid4().hex[:8]}"))
    temporary.mkdir(parents=True)
    monkeypatch.setattr(tempfile, "tempdir", str(temporary))
    storage = Storage.for_uri(local_location.child("engine_tmp_delta"))
    with DuckDBEngine(DuckDBConfig(), "exec-tmp", storage) as engine:
        folder = Path(engine._folder)
        assert folder.parent == temporary
        assert folder.is_dir()
        assert (folder / "exec-tmp.duckdb").exists()
    assert not folder.exists()


def test_new_session_runs_beside_the_main_one(setup: Setup) -> None:
    """A sessão a mais vê o que a principal confirmou e não a temporária dela, roda enquanto a
    principal está num bloco ``session()``, e a principal vê o que ela confirma; o ``with`` fecha só
    o cursor."""
    engine = setup.engine
    engine.query("CREATE TABLE confirmada AS SELECT range AS id FROM range(10)")
    engine.query("CREATE TEMP TABLE temporaria AS SELECT 1 AS x")
    with engine.new_session() as other:
        assert count_of(other, "confirmada") == 10
        with pytest.raises(duckdb.CatalogException, match="temporaria"):
            other.query("SELECT * FROM temporaria")
        with ThreadPoolExecutor(max_workers=1) as pool, engine.session():
            counted = pool.submit(count_of, other, "confirmada").result(timeout=10)
        assert counted == 10
        other.query("CREATE TABLE da_outra AS SELECT 1 AS x")
    assert count_of(engine, "da_outra") == 1
    assert setup.database_file().exists()


# ---------------------------------------------------------------- a leitura do Delta


def test_ingest_pins_the_version(setup: Setup) -> None:
    """Um commit depois da abertura não aparece na view nem na tabela materializada; o nome ocupado
    é ``SandboxError``, e a versão ausente também."""
    version = published_table(setup, ENTRIES, MONTHS[:2])
    engine = setup.engine
    uri = setup.uri(ENTRIES)
    materialized = ENTRIES.to_metadata(sa.MetaData(), name="cad_lancamentos_materializada")
    engine.ingest(ENTRIES, uri, version)
    engine.ingest(materialized, uri, version, materialize=True)
    later = entries(MONTHS[2], 500, 50)
    delta.publish_partition(uri, ENTRIES, MONTHS[2], later, METADATA, setup.storage)
    assert count_of(engine, "cad_lancamentos") == 200
    assert count_of(engine, "cad_lancamentos_materializada") == 200
    with pytest.raises(SandboxError, match="ocupado"):
        engine.ingest(ENTRIES, uri, version)
    with pytest.raises(SandboxError, match="sem versão"):
        engine.ingest(PROJECTED, setup.uri(PROJECTED), None)


@pytest.mark.parametrize(("name", "wanted", "opened_months"), [
    ("contiguas", MONTHS[1:3], MONTHS[1:3]),
    ("salteadas", [MONTHS[1], MONTHS[3]], MONTHS[1:4]),
], ids=["contiguas", "salteadas"])
def test_ingest_opens_only_the_range_of_partitions(setup: Setup, name: str, wanted: list[str],
                                                   opened_months: list[str]) -> None:
    """A ingestão de partições contíguas e de uma lista salteada abre só os arquivos do intervalo,
    pelo log ``FileSystem`` do DuckDB, e traz só as linhas pedidas."""
    version = published_table(setup, ENTRIES, MONTHS, rows=10)
    engine = setup.engine
    table = ENTRIES.to_metadata(sa.MetaData(), name=f"cad_lancamentos_{name}")
    with engine.session() as connection:
        connection.execute("CALL enable_logging('FileSystem')")
        connection.execute("CALL truncate_duckdb_logs()")
    engine.ingest(table, setup.uri(ENTRIES), version, partitions=wanted, materialize=True)
    with engine.session() as connection:
        opened = opened_partition_folders(connection, "data_base_str")
        connection.execute("CALL disable_logging()")
    partitions = engine.query(f'SELECT DISTINCT data_base_str FROM "{table.name}" ORDER BY 1')
    assert partitions.column(0).to_pylist() == sorted(wanted)
    assert opened == {f"data_base_str={month}" for month in opened_months}


def test_published_reads_the_pinned_version(setup: Setup) -> None:
    """``published`` lê a versão fixada sem criar objeto no sandbox, e o ``loader`` da mesma tabela
    fica com o nome do modelo; sem versão, ``SandboxError``."""
    version = published_table(setup, PROJECTED, MONTHS[:2])
    engine = setup.engine
    uri = setup.uri(PROJECTED)
    source = engine.published(PROJECTED, uri, version)
    later = entries(MONTHS[2], 900, 5, PROJECTED)
    delta.publish_partition(uri, PROJECTED, MONTHS[2], later, METADATA, setup.storage)
    top = sa.func.max(source.c.id_lancamento).label("topo")
    statement = sa.select(sa.func.count().label("n"), top)
    assert engine.query(statement).to_pylist() == [{"n": 200, "topo": 200}]
    assert not engine.name_in_use(PROJECTED.name)
    engine.load(PROJECTED, entries(MONTHS[5], 1000, 3, PROJECTED))
    assert count_of(engine, PROJECTED.name) == 3
    with pytest.raises(SandboxError, match="ainda não existe"):
        engine.published(PROJECTED, uri, None)


# ---------------------------------------------------------------- consulta e parâmetros


def test_statement_parameters_expand_in_lists(setup: Setup) -> None:
    """Um statement com ``IN`` de lista, ``NOT IN`` e ``bindparam`` expansível roda por ``query`` e
    por ``stream``; um nome a mais ou a menos é ``SqlError`` antes de rodar."""
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
    expected = []
    for entry_id in range(1, 31):
        if account_of(entry_id) in (1, 2) and entry_id not in (1, 4):
            expected.append(entry_id)
    assert by_query == by_stream == expected
    with pytest.raises(SqlError, match="não fecham"):
        engine.query(statement, {"areas": ["TI"]})
    with pytest.raises(SqlError, match="não fecham"):
        engine.query(statement, {**params, "sobra": 1})


def test_query_keeps_percent_literals(setup: Setup) -> None:
    """``LIKE 'A%'`` num statement e num texto pronto chega ao DuckDB como está, e o texto pronto
    recebe os parâmetros por ``:nome``."""
    engine = setup.engine
    abc = entries(MONTHS[0], 1, 3, area="ABC")
    xyz = entries(MONTHS[0], 10, 2, area="XYZ")
    engine.load(ENTRIES, pa.concat_tables([abc, xyz]))
    statement = sa.select(sa.func.count().label("n")).where(ENTRIES.c.area.like("A%"))
    assert engine.query(statement).column("n")[0].as_py() == 3
    text = ('SELECT count(*) AS n FROM "cad_lancamentos" '
            'WHERE "area" LIKE \'X%\' AND "id_lancamento" > :minimo')
    assert engine.query(text, {"minimo": 10}).column("n")[0].as_py() == 1


# ---------------------------------------------------------------- o stream


def create_numbers(engine: DuckDBEngine) -> None:
    """Cria ``numeros`` no sandbox: ``id`` de 0 a 2.999.999 e o texto ``s``."""
    engine.query("CREATE TABLE numeros AS "
                 "SELECT range AS id, 'x' || range AS s FROM range(3_000_000)")


def test_stream_delivers_each_batch_while_the_query_runs(setup: Setup) -> None:
    """O primeiro lote com a consulta rodando, sem arquivo com o cliente acompanhando; um
    comando no meio da leitura roda depois da consulta; a tabela temporária e o stream dentro de
    ``session()``; o abandono para a consulta e apaga o arquivo; o erro chega na construção ou na
    leitura seguinte ao último lote."""
    engine = setup.engine
    create_numbers(engine)

    # O primeiro lote chega com a consulta rodando, e nenhum lote vai para o arquivo. Com
    # preserve_insertion_order = false, a ordem sem ORDER BY é a do DuckDB: a soma confere as
    # linhas.
    started = time.perf_counter()
    with engine.stream("SELECT id, s FROM numeros WHERE id % 3 <> 0") as stream:
        first = stream.read_next_batch()
        first_batch = time.perf_counter() - started
        query_running = stream._thread.is_alive()
        seen = first.num_rows
        total = pc.sum(first.column("id")).as_py()
        for batch in stream:
            seen += batch.num_rows
            total += pc.sum(batch.column("id")).as_py()
        assert stream._spool.spilled == 0
    assert seen == 2_000_000
    assert total == sum(k for k in range(3_000_000) if k % 3 != 0)
    assert spool_files(engine) == []
    reading = f"{first_batch:.3f} s, com a consulta rodando: {query_running}"
    record("engine.stream.first_batch", reading)

    # Um comando no meio da leitura roda depois da consulta.
    with DuckDBStream(engine, "SELECT id FROM numeros", [], budget=10_000) as stream:
        stream.read_next_batch()
        with engine.session() as connection:
            connection.execute("SELECT 1").fetchall()
            assert stream._spool.done

    # A tabela temporária e o stream dentro de session(): a consulta roda na thread do bloco.
    engine.query("CREATE TEMP TABLE pares AS SELECT id FROM numeros WHERE id % 2 = 0")
    with engine.session(), engine.stream("SELECT count(*) AS n FROM pares") as inside:
        assert inside._thread is None
        assert inside.read_all().column("n").to_pylist() == [1_500_000]

    # O stream lido por RecordBatchReader.from_stream, pela interface __arrow_c_stream__.
    with engine.stream("SELECT id FROM numeros WHERE id < 5000", batch_size=1000) as source:
        assert pa.RecordBatchReader.from_stream(source).read_all().num_rows == 5000

    # O stream abandonado para a consulta e apaga o arquivo.
    abandoned = DuckDBStream(engine, "SELECT id FROM numeros", [], batch_size=1000, budget=10_000)
    abandoned.read_next_batch()
    thread = abandoned._thread
    del abandoned
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert spool_files(engine) == []

    # O erro chega na construção, quando a consulta não entrega lote algum, ou na leitura seguinte
    # ao último lote, com os lotes na memória ou no arquivo. A falha na linha 2.900.000 chegou como
    # OSError do leitor Arrow, depois de 25 ou 26 lotes, nos dois orçamentos (seis leituras de
    # 2026-09-23); o teste aceita também ConversionException, a classe do DuckDB para o mesmo erro.
    with pytest.raises(duckdb.CatalogException, match="nao_existe"):
        engine.stream("SELECT * FROM nao_existe")
    failing = ("SELECT CAST(CASE WHEN id = 2_900_000 THEN 'x' ELSE CAST(id AS VARCHAR) END "
               "AS INTEGER) AS n FROM numeros")
    for budget in (64 * 2**20, 10_000):
        with (pytest.raises((duckdb.ConversionException, OSError),
                            match="Could not convert string 'x' to INT32"),
              DuckDBStream(engine, failing, [], batch_size=100_000, budget=budget) as stream):
            stream.read_all()
        assert spool_files(engine) == []


def test_stream_spills_after_the_budget_and_keeps_the_order(setup: Setup) -> None:
    """Com o cliente lento e um orçamento de dois lotes, a memória para no orçamento, o resto vai
    para o arquivo, as linhas saem todas e na ordem, e o ``close`` apaga o arquivo."""
    engine = setup.engine
    create_numbers(engine)
    # Um lote de 100.000 linhas de (BIGINT, VARCHAR curto) tem 1,8 MB: o orçamento guarda dois.
    budget = 2 * 1_900_000
    ids = []
    peak = 0
    with DuckDBStream(engine, "SELECT id, s FROM numeros ORDER BY id", [], budget=budget) as stream:
        for batch in stream:
            time.sleep(0.01)
            peak = max(peak, stream._spool.memory_bytes)
            ids.extend(batch.column("id").to_pylist())
        spilled = stream._spool.spilled
    assert ids == list(range(3_000_000))
    assert spilled > 0
    assert peak <= budget
    assert spool_files(engine) == []

    # Os lotes que foram ao arquivo e o pico em memória vão para o relatório.
    record("engine.stream.spilled_batches",
           f"{spilled} de 30 lotes no arquivo, com {peak / 1e6:.1f} MB de pico em memória")


def sorting(observer: DuckDBEngine) -> bool:
    """Se alguma consulta do banco ordena agora: memória ``ORDER_BY`` em ``duckdb_memory()``."""
    query = ("SELECT count(*) AS n FROM duckdb_memory() "
             "WHERE tag = 'ORDER_BY' AND memory_usage_bytes > 0")
    return observer.query(query).column("n")[0].as_py() > 0


def wait_for_the_sort(engine: DuckDBEngine) -> None:
    """Espera, com prazo de 10 s, uma ordenação no banco do motor, lida numa sessão a mais."""
    deadline = time.monotonic() + 10
    with engine.new_session() as observer:
        while not sorting(observer) and time.monotonic() < deadline:
            time.sleep(0.01)
        assert sorting(observer), "a ordenação não começou em 10 s"


def test_close_and_cleanup_interrupt_the_running_query(setup: Setup) -> None:
    """O ``close`` depois do primeiro lote de uma varredura longa cancela a consulta, e a sessão
    continua usável; o ``cleanup`` cancela a ordenação de um stream que outra thread ainda
    constrói."""
    engine = setup.engine
    engine.query("CREATE TABLE numeros AS SELECT range AS id FROM range(20_000_000)")
    # Com preserve_insertion_order = false, o ajuste do motor, esta consulta entrega o primeiro lote
    # só no fim (1,093 s de 1,093 s com duas threads e o md5 simples, leitura de 2026-09-23); com a
    # ordem preservada, o primeiro lote costuma chegar antes e a varredura continua, o intervalo em
    # que o close a cancela. Com a máquina carregada, o primeiro lote pode sair no fim e não há o
    # que cancelar: o erro fica nulo, e o que vale nos dois casos é a thread terminada e a sessão
    # livre.
    with engine.session() as connection:
        connection.execute("SET preserve_insertion_order = true")
    scan_query = "SELECT id FROM numeros WHERE id < 150000 OR md5(md5(id::VARCHAR)) = 'x'"
    stream = engine.stream(scan_query)
    stream.read_next_batch()
    started = time.perf_counter()
    stream.close()
    closed = time.perf_counter() - started
    error = stream._spool.error
    record("engine.stream.close_interrupts", f"{closed:.3f} s, erro {type(error).__name__}")
    assert error is None or "INTERRUPT" in str(error).upper()
    assert not stream._thread.is_alive()
    assert closed < 1.0
    assert count_of(engine, "numeros") == 20_000_000

    # O cleanup cancela a ordenação de um stream que outra thread ainda constrói: a thread
    # principal espera a ordenação começar antes de chamar o cleanup, e o tempo do cleanup vai para
    # o relatório.
    with ThreadPoolExecutor(max_workers=1) as pool:
        construction = pool.submit(engine.stream, "SELECT id FROM numeros ORDER BY hash(id)")
        wait_for_the_sort(engine)
        started = time.perf_counter()
        engine.cleanup()
        cleaned = time.perf_counter() - started
        error = construction.exception(timeout=30)
    assert "INTERRUPT" in str(error).upper()
    record("engine.cleanup_interrupts", f"{cleaned:.3f} s do cleanup com a ordenação em curso")


# ---------------------------------------------------------------- o loader


def test_loader_creates_and_inserts_in_one_transaction_on_close(setup: Setup) -> None:
    """Nada existe antes do ``close``; a exceção do cliente, o lote recusado pelo ``cast`` e o erro
    do ``INSERT`` não deixam tabela; ``rows`` conta as linhas; o loader sem lote cria a tabela
    vazia."""
    engine = setup.engine
    visible = []
    with engine.loader(PROJECTED) as loader:
        for k in range(5):
            loader.write(entries(MONTHS[0], 1 + k * 100, 100, PROJECTED))
            visible.append(engine.name_in_use(PROJECTED.name))
    assert loader.rows == 500
    assert set(visible) == {False}
    assert count_of(engine, PROJECTED.name) == 500
    assert spool_files(engine) == []

    # A exceção do cliente não deixa tabela.
    other = PROJECTED.to_metadata(sa.MetaData(), name="cad_segunda")
    with pytest.raises(ValueError, match="falha do cliente"):
        with engine.loader(other) as loader:
            loader.write(entries(MONTHS[0], 1, 10, PROJECTED))
            raise ValueError("falha do cliente")
    assert not engine.name_in_use("cad_segunda")

    # O lote recusado pelo cast não deixa tabela.
    long_area = entries(MONTHS[0], 11, 1, PROJECTED)
    area_index = long_area.schema.get_field_index("area")
    long_area = long_area.set_column(area_index, "area", pa.array(["x" * 11]))
    with pytest.raises(ContractError, match="texto de"):
        with engine.loader(other) as loader:
            loader.write(entries(MONTHS[0], 1, 10, PROJECTED))
            loader.write(long_area)
    assert not engine.name_in_use("cad_segunda")

    # O lote sem a coluna NOT NULL valor passa pelo cast e falha no INSERT; o ROLLBACK desfaz o
    # CREATE.
    with pytest.raises(duckdb.ConstraintException):
        with engine.loader(other) as loader:
            loader.write(entries(MONTHS[0], 1, 10, PROJECTED).drop_columns(["valor"]))
    assert not engine.name_in_use("cad_segunda")

    # O loader abandonado sem close: a thread termina e apaga o arquivo, e nada é criado.
    abandoned = engine.loader(other)
    abandoned.write(entries(MONTHS[0], 1, 10, PROJECTED))
    thread = abandoned._thread
    del abandoned
    thread.join(timeout=2)
    assert not thread.is_alive()
    assert spool_files(engine) == []
    assert not engine.name_in_use("cad_segunda")

    # O loader sem lote cria a tabela vazia.
    with engine.loader(other) as loader:
        pass
    assert engine.name_in_use("cad_segunda")
    assert count_of(engine, "cad_segunda") == 0


def test_loader_opened_after_a_stream_does_not_wait_for_its_query(setup: Setup) -> None:
    """Com ``stream`` e depois ``loader`` no mesmo ``with``, o primeiro lote chega com a consulta
    rodando, e a tabela tem todas as linhas no fim."""
    engine = setup.engine
    engine.query(
        "CREATE TABLE fonte AS SELECT range AS id_lancamento, 1 AS id_conta, "
        "DATE '2026-08-31' AS data_base, range / 4 AS valor, 'L' || range AS codigo, "
        "'2026-08-31' AS data_base_str FROM range(3_000_000)")
    with engine.stream("SELECT * FROM fonte") as stream, engine.loader(PROJECTED) as loader:
        first = stream.read_next_batch()
        query_running = stream._thread.is_alive()
        loader.write(first)
        for batch in stream:
            loader.write(batch)
    assert query_running
    assert count_of(engine, PROJECTED.name) == 3_000_000


def pipeline_table(name: str) -> sa.Table:
    """A saída do pipeline de três estágios: as colunas da fonte e o ``dobro`` do valor."""
    return sa.Table(
        name, sa.MetaData(),
        sa.Column("id", sa.BigInteger),
        sa.Column("valor", sa.Numeric(18, 2)),
        sa.Column("s", sa.String(20)),
        sa.Column("dobro", sa.Double),
    )


def test_three_stage_pipeline_overlaps_read_work_and_write(setup: Setup) -> None:
    """Leitura por ``stream``, trabalho do cliente por lote e escrita por ``loader`` numa sessão
    única, sobre um banco em arquivo, produzem as mesmas linhas que a versão por lote sem threads e
    que a versão por ``pa.Table``; os tempos são leituras do relatório."""
    engine = setup.engine
    engine.query(
        "CREATE TABLE fonte AS SELECT range AS id, "
        "CAST(((range * 7) % 1000) / 100.0 AS DECIMAL(18, 2)) AS valor, "
        "'x' || range AS s FROM range(3_000_000)")
    target = pa.schema([("id", pa.int64()), ("valor", pa.decimal128(18, 2)), ("s", pa.string()),
                        ("dobro", pa.float64())])
    engine.query(schema.ddl(pipeline_table("sequencial"), "duckdb"))
    sql = "SELECT id, valor, s FROM fonte"

    def work(data: pa.RecordBatch | pa.Table) -> pa.RecordBatch | pa.Table:
        """O trabalho do cliente: o ``dobro`` do valor, calculado no pandas."""
        frame = data.to_pandas(types_mapper=pd.ArrowDtype)
        frame["dobro"] = (frame["valor"] * 2).astype("double[pyarrow]")
        if isinstance(data, pa.Table):
            return pa.Table.from_pandas(frame, preserve_index=False).cast(target)
        return pa.RecordBatch.from_pandas(frame, preserve_index=False).cast(target)

    def insert(name: str, data: pa.RecordBatch) -> None:
        """Um ``INSERT ... BY NAME`` do lote na tabela ``name``, sob o lock da sessão."""
        with engine.session() as connection:
            connection.register("lote", data)
            connection.execute(f"INSERT INTO {name} BY NAME SELECT * FROM lote")
            connection.unregister("lote")

    timings: dict[str, float] = {}

    # A tabela inteira: to_arrow_table, o trabalho de uma vez, a carga de uma vez.
    started = time.perf_counter()
    engine.load(pipeline_table("por_tabela"), work(engine.query(sql)))
    timings["por_tabela"] = time.perf_counter() - started

    # Por lote, sem threads: dentro de session, o stream roda a consulta inteira na thread do
    # cliente, e o trabalho e um INSERT por lote entram no mesmo bloco.
    started = time.perf_counter()
    with engine.session(), engine.stream(sql, batch_size=200_000) as stream:
        for batch in stream:
            insert("sequencial", work(batch))
    timings["sequencial"] = time.perf_counter() - started

    # Encadeado, na ordem do exemplo mensal: a consulta na thread do stream, o trabalho na thread
    # do cliente, a escrita do arquivo na thread do loader, e o CREATE e o INSERT únicos no close.
    started = time.perf_counter()
    with (engine.stream(sql, batch_size=200_000) as stream,
          engine.loader(pipeline_table("encadeado")) as loader):
        for batch in stream:
            loader.write(work(batch))
    timings["encadeado"] = time.perf_counter() - started

    # As três versões dão as mesmas linhas e somas; os tempos vão para o relatório.
    totals = {}
    for name in ("por_tabela", "sequencial", "encadeado"):
        query = (f"SELECT count(*) AS n, sum(valor) AS v, sum(dobro::DECIMAL(18, 2)) AS d "
                 f"FROM {name}")
        totals[name] = engine.query(query).to_pylist()[0]
    assert totals["por_tabela"] == totals["sequencial"] == totals["encadeado"]
    assert totals["encadeado"]["n"] == 3_000_000
    readings = []
    for name, seconds in timings.items():
        readings.append(f"{name} {seconds:.3f} s")
    record("engine.timing.three_stage_pipeline_3M_rows", ", ".join(readings))


def test_loader_refuses_a_name_in_use(setup: Setup) -> None:
    """O ``loader`` sobre a view do ``ingest``, sobre a tabela materializada e sobre a tabela de um
    ``loader`` anterior levanta ``SandboxError`` antes do primeiro lote, e o objeto não muda."""
    version = published_table(setup, ENTRIES, MONTHS[:1], rows=10)
    engine = setup.engine
    engine.ingest(ENTRIES, setup.uri(ENTRIES), version)
    materialized = ENTRIES.to_metadata(sa.MetaData(), name="cad_materializada")
    engine.ingest(materialized, setup.uri(ENTRIES), version, materialize=True)
    engine.load(PROJECTED, entries(MONTHS[0], 1, 5, PROJECTED))
    for table in (ENTRIES, materialized, PROJECTED):
        with pytest.raises(SandboxError, match="run.published"):
            engine.loader(table)
    names = ("cad_lancamentos", "cad_materializada", PROJECTED.name)
    assert [count_of(engine, name) for name in names] == [10, 10, 5]


def test_read_during_a_forgotten_load_fails_instead_of_reading_old_rows(setup: Setup) -> None:
    """Um ``load`` disparado numa thread sem ``result()`` não deixa a leitura ver dado velho: a
    tabela só nasce no ``close`` do ``loader``, e o nome ocupado é recusado, então a leitura antes
    da carga falha com ``CatalogException`` na sessão principal e numa sessão a mais."""
    engine = setup.engine
    missing = f"{PROJECTED.name} does not exist"
    opened = threading.Event()
    release = threading.Event()

    def load() -> None:
        with engine.loader(PROJECTED) as loader:
            loader.write(entries(MONTHS[0], 1, 1000, PROJECTED))
            opened.set()
            assert release.wait(timeout=10)

    with ThreadPoolExecutor(max_workers=1) as pool:
        future = pool.submit(load)  # o cliente esquece o result()
        assert opened.wait(timeout=10)

        # A carga em voo: a leitura falha nas duas sessões, em vez de ver uma tabela vazia ou
        # anterior.
        with pytest.raises(duckdb.CatalogException, match=missing):
            count_of(engine, PROJECTED.name)
        with engine.new_session() as other:
            with pytest.raises(duckdb.CatalogException, match=missing):
                count_of(other, PROJECTED.name)
        release.set()
        future.result()

    assert count_of(engine, PROJECTED.name) == 1000

    # Uma segunda carga no mesmo nome é recusada: nenhuma tabela do sandbox tem estado anterior a
    # ler.
    with pytest.raises(SandboxError, match=PROJECTED.name):
        engine.loader(PROJECTED)


def test_query_and_load_match_stream_and_loader(setup: Setup) -> None:
    """``query`` é ``stream(...).read_all()``; ``load`` de ``pa.Table``, lote, leitor e iterável dá
    o mesmo resultado; o DataFrame é recusado com a mensagem que aponta ``from_pandas``."""
    engine = setup.engine
    data = entries(MONTHS[0], 1, 1000, PROJECTED)
    reader = pa.RecordBatchReader.from_batches(data.schema, data.to_batches(max_chunksize=100))
    forms = {
        "tabela": data,
        "lote": data.combine_chunks().to_batches()[0],
        "leitor": reader,
        "iteravel": data.to_batches(max_chunksize=300),
    }
    totals = {}
    for name, form in forms.items():
        table = PROJECTED.to_metadata(sa.MetaData(), name=f"forma_{name}")
        assert engine.load(table, form) == 1000
        sums = f'SELECT count(*) AS n, sum(preco) AS p FROM "forma_{name}"'
        totals[name] = engine.query(sums).to_pylist()
    for name, total in totals.items():
        assert total == totals["tabela"], name

    # query é stream(...).read_all().
    ordered = sa.text("id_lancamento")
    statement = sa.select(sa.text("*")).select_from(sa.table("forma_tabela")).order_by(ordered)
    with engine.stream(statement) as stream:
        streamed = stream.read_all()
        assert streamed.equals(engine.query(statement))

    # O DataFrame e o texto são recusados com a mensagem que aponta from_pandas.
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
    assert str(frame["preco"].dtype) == "decimal128(18, 2)[pyarrow]"
    assert str(frame["data_base"].dtype) == "date32[day][pyarrow]"
    frame["id_lancamento"] = frame["id_lancamento"] + 1000
    engine.load(PROJECTED, pa.Table.from_pandas(frame, preserve_index=False))
    loaded = engine.query(sa.select(PROJECTED))
    assert loaded.schema.field("preco").type == pa.decimal128(18, 2)
    assert loaded.schema.field("data_base").type == pa.date32()
    total = engine.query('SELECT sum(preco) AS p FROM "cad_lancamentos_projetados"')
    expected = sum(decimal.Decimal(k) / 100 for k in range(1, 101))
    assert total.column("p")[0].as_py() == expected


# ---------------------------------------------------------------- a auditoria


def test_audit_finds_each_defect(setup: Setup) -> None:
    """Nulo, texto acima do ``String(n)`` em bytes e dentro dele em caracteres, partição fora da
    origem, valor de partição fora da regra, JSON inválido, documento JSON acima de 65.535 bytes,
    chave repetida na partição e contra a publicada, e chave única repetida contra a publicada."""
    version = published_table(setup, PROJECTED, MONTHS[:2], rows=10)
    engine = setup.engine
    good = entries(MONTHS[2], 100, 7, PROJECTED)
    batch = good.to_pylist()
    batch[0]["id_lancamento"] = batch[1]["id_lancamento"]  # repetida na partição
    batch[2]["id_lancamento"] = 3  # repetida contra a publicada
    batch[3]["area"] = "ação ação"  # 9 caracteres, 13 bytes
    batch[4]["data_base"] = dt.date(2026, 7, 31)  # fora da origem da partição
    batch[5]["meta"] = "{nao json"  # JSON inválido
    batch[6]["meta"] = json.dumps({"k": "x" * 65527})  # 65.536 bytes
    # A coluna JSON do DuckDB recusa o texto inválido na carga: a tabela nasce por CTAS, com meta em
    # VARCHAR, como uma tabela que o pipeline criasse por SQL.
    with engine.session() as connection:
        connection.register("dados", pa.Table.from_pylist(batch, schema=good.schema))
        connection.execute('CREATE TABLE "cad_lancamentos_projetados" AS SELECT * FROM dados')
        connection.unregister("dados")
        # O valor de partição fora da regra entra por SQL: o cast não confere a regra, a auditoria
        # confere.
        connection.execute(
            'INSERT INTO "cad_lancamentos_projetados" BY NAME SELECT 999 AS id_lancamento, '
            "9 AS id_conta, DATE '2026-05-31' AS data_base, 1.0 AS valor, 'L999' AS codigo, "
            "'d''agua' AS data_base_str")
    report = engine.audit(PROJECTED, [MONTHS[2]], setup.uri(PROJECTED), version)
    counters = report.totals[MONTHS[2]]
    assert counters["texto_area"] == 1
    assert counters["particao_data_base_str"] == 1
    assert counters["json_meta"] == 1
    assert counters["texto_meta"] == 1
    assert result_of(report, "chave_id_lancamento").defects == 1
    repeated = result_of(report, "chave_id_lancamento_publicada")
    assert repeated.sample.column(0).to_pylist() == [3]
    assert result_of(report, "chave_codigo_publicada").defects == 0
    assert not report.passed
    assert not result_of(report, "linhas").passed

    # A auditoria da tabela inteira conta por valor de partição, o fora da regra e o nulo inclusive.
    whole = engine.audit(PROJECTED, None)
    assert whole.totals["d'agua"]["valor_data_base_str"] == 1
    engine.query(
        'INSERT INTO "cad_lancamentos_projetados" BY NAME SELECT 1000 AS id_lancamento, '
        "9 AS id_conta, DATE '2026-05-31' AS data_base, 1.0 AS valor, 'L1000' AS codigo, "
        "NULL AS data_base_str")
    assert engine.audit(PROJECTED, None).totals[None]["nulo_data_base_str"] == 1


def test_audit_unique_key_against_the_published_version(setup: Setup) -> None:
    """A chave única sem a coluna de partição, repetida entre a execução e a versão publicada,
    reprova pela consulta contra ``published``."""
    uri = setup.uri(PROJECTED)
    delta.create_table(uri, PROJECTED, setup.storage)
    published = entries(MONTHS[0], 1, 3, PROJECTED)
    version = delta.publish_partition(uri, PROJECTED, MONTHS[0], published, METADATA, setup.storage)
    repeated = entries(MONTHS[1], 100, 1, PROJECTED).to_pylist()[0]
    repeated["codigo"] = published.column("codigo")[0].as_py()
    setup.engine.load(PROJECTED, pa.Table.from_pylist([repeated], schema=published.schema))
    report = setup.engine.audit(PROJECTED, [MONTHS[1]], uri, version)
    sample = result_of(report, "chave_codigo_publicada").sample
    assert sample.column("codigo").to_pylist() == ["L000001"]
    assert result_of(report, "linhas").passed
    assert not report.passed


def test_audit_skips_the_published_join_above_max_key(setup: Setup) -> None:
    """Com as chaves da execução acima do ``max_key`` da versão fixada, a junção não roda e o
    relatório diz por quê; com uma chave abaixo dele, a junção roda e acha a repetição."""
    version = published_table(setup, PROJECTED, MONTHS[:2], rows=10)
    engine = setup.engine
    uri = setup.uri(PROJECTED)
    engine.load(PROJECTED, entries(MONTHS[2], 1000, 5, PROJECTED))
    report = engine.audit(PROJECTED, [MONTHS[2]], uri, version)
    skipped = result_of(report, "chave_id_lancamento_publicada")
    assert skipped.passed
    assert skipped.reason.startswith("dispensada")
    assert report.passed

    # Com chaves abaixo do max_key, a junção roda e acha as cinco repetições.
    engine.query('DROP TABLE "cad_lancamentos_projetados"')
    engine.load(PROJECTED, entries(MONTHS[2], 15, 5, PROJECTED))
    joined_report = engine.audit(PROJECTED, [MONTHS[2]], uri, version)
    joined = result_of(joined_report, "chave_id_lancamento_publicada")
    assert joined.reason == ""
    assert joined.defects == 5


def test_audit_report_samples_failing_rows(setup: Setup) -> None:
    """Até 20 linhas inteiras por verificação reprovada; a de linhas busca as suas por contador, e a
    aprovada não traz amostra; ``sql()`` junta os textos; os não finitos entram sem reprovar."""
    engine = setup.engine
    with_nan = [float("nan")] + [1.0] * 29
    data = entries(MONTHS[0], 1, 30, PROJECTED, valor=with_nan).to_pylist()
    for row in data:
        if row["id_lancamento"] <= 25:
            row["area"] = "x" * 11
    with engine.session() as connection:
        connection.register("dados", pa.Table.from_pylist(data))
        connection.execute(schema.ddl(PROJECTED, "duckdb"))
        connection.execute('INSERT INTO "cad_lancamentos_projetados" BY NAME SELECT * FROM dados')
        connection.unregister("dados")
    report = engine.audit(PROJECTED, [MONTHS[0]])
    rows_check = result_of(report, "linhas")
    assert rows_check.defects == 25
    assert rows_check.sample.num_rows == 20
    assert set(rows_check.sample.column_names) >= {"id_lancamento", "area"}
    assert result_of(report, "chave_id_lancamento").sample.num_rows == 0
    assert report.nonfinite_columns == {MONTHS[0]: ("valor",)}
    assert report.totals[MONTHS[0]]["naofinito_valor"] == 1
    assert "-- linhas\nSELECT" in report.sql()
    assert "-- chave_id_lancamento" in report.sql()


def test_audit_orphan_against_a_referenced_table_outside_the_sandbox(setup: Setup) -> None:
    """Com ``foreign_keys=True``, a tabela referenciada fora do sandbox entra pela versão fixada de
    ``referenced``, e o órfão aparece; sem o argumento, a verificação fica em ``not_run``."""
    accounts_version = published_accounts(setup, ["A", "B"])
    engine = setup.engine
    engine.load(ENTRIES, entries(MONTHS[0], 1, 6))  # id_conta 1, 2 e 3: a conta 3 não existe
    referenced = {"cad_contas": (setup.uri(ACCOUNTS), accounts_version)}
    report = engine.audit(ENTRIES, [MONTHS[0]], foreign_keys=True, referenced=referenced)
    orphans = result_of(report, "orfao_id_conta")
    assert orphans.sample.column("id_conta").to_pylist() == [3]
    assert not report.passed
    without_foreign_keys = engine.audit(ENTRIES, [MONTHS[0]])
    assert "orfao_id_conta (sem foreign_keys=True)" in without_foreign_keys.not_run


# ---------------------------------------------------------------- a exportação


def exported_totals(storage: Storage, uri: str) -> list[tuple]:
    """Linhas, somas de ``preco`` e de ``id_lancamento`` e linhas com ``NaN`` em ``valor`` da tabela
    Delta."""
    query = ("SELECT count(*), sum(preco), sum(id_lancamento), "
             "count(*) FILTER (WHERE isnan(valor)) "
             f"FROM delta_scan('{uri}')")
    return scan(storage, query)


def test_export_partition_registers_the_copy_file(setup: Setup) -> None:
    """A partição sai pelo ``COPY ... RETURN_STATS`` e entra no log com as linhas e as somas do
    sandbox, sem o mínimo e o máximo da coluna com ``NaN``; o Delta poda pela estatística da
    chave, o arquivo leva o ``execution_id`` no nome, e ``expected_rows`` diferente recusa sem
    commit."""
    engine = setup.engine
    with_nan = [float("nan")] + [k / 4 for k in range(2, 1001)]
    engine.load(PROJECTED, entries(MONTHS[1], 1, 1000, PROJECTED, valor=with_nan))
    uri = setup.uri(PROJECTED)
    delta.create_table(uri, PROJECTED, setup.storage)
    version = engine.export_partition(PROJECTED, uri, MONTHS[1], METADATA, expected_rows=1000,
                                      columns_without_min_max=["valor"])
    assert version == 1
    assert exported_totals(setup.storage, uri) == [(1000, decimal.Decimal("5005.00"), 500500, 1)]
    stats = pa.table(delta.open_table(uri, setup.storage).get_add_actions(flatten=True))
    assert stats.column("max.valor").null_count == stats.num_rows
    assert stats.column("max.id_lancamento").to_pylist() == [1000]

    # A poda pela estatística da chave, e o arquivo com o execution_id no nome.
    query = (f"EXPLAIN ANALYZE SELECT count(*) FROM delta_scan('{uri}') "
             "WHERE id_lancamento > 5000")
    # O EXPLAIN ANALYZE devolve uma linha, com o plano na segunda coluna.
    plan = scan(setup.storage, query)[0][1]
    assert "Scanning Files: 0/1" in plan
    files = setup.storage.list_files(setup.storage.relative(uri), ".parquet")
    folder = f"prod/{PROJECTED.name}/data_base_str={MONTHS[1]}/"
    assert files[0].startswith(folder)
    pattern = re.escape(EXECUTION_ID) + r"_[0-9a-f]{32}\.parquet"
    assert re.fullmatch(pattern, files[0].removeprefix(folder))

    # expected_rows diferente recusa, sem commit.
    with pytest.raises(RegistrationRefused):
        engine.export_partition(PROJECTED, uri, MONTHS[1], METADATA, expected_rows=999)
    assert delta.open_table(uri, setup.storage).version() == 1


def test_example_pipeline_in_a_file_backed_database(setup: Setup) -> None:
    """O pipeline de exemplo: seis partições materializadas e a dimensão em view, um ``select`` com
    ``join`` em lotes para o ``loader``, auditoria com a versão publicada, exportação; ``cleanup``
    apaga o arquivo do banco."""
    engine = setup.engine
    entries_version = published_table(setup, ENTRIES, MONTHS, rows=200)
    accounts_version = published_accounts(setup, ["A", "B", "C"])
    projected_uri = setup.uri(PROJECTED)
    delta.create_table(projected_uri, PROJECTED, setup.storage)

    engine.ingest(ENTRIES, setup.uri(ENTRIES), entries_version, partitions=MONTHS, materialize=True)
    engine.ingest(ACCOUNTS, setup.uri(ACCOUNTS), accounts_version)
    statement = (
        sa.select(ENTRIES)
        .join_from(ENTRIES, ACCOUNTS, ENTRIES.c.id_conta == ACCOUNTS.c.id_conta)
        .where(ENTRIES.c.data_base_str == sa.bindparam("particao"), ACCOUNTS.c.numero != "C")
    )
    params = {"particao": MONTHS[-1]}
    with (engine.stream(statement, params, batch_size=50) as stream,
          engine.loader(PROJECTED) as loader):
        for batch in stream:
            loader.write(batch)
    report = engine.audit(PROJECTED, [MONTHS[-1]], projected_uri, 0, foreign_keys=True)
    assert report.passed, report.results
    expected_rows = report.rows(MONTHS[-1])
    nonfinite = report.nonfinite_columns.get(MONTHS[-1], ())
    version = engine.export_partition(PROJECTED, projected_uri, MONTHS[-1], METADATA,
                                      expected_rows=expected_rows,
                                      columns_without_min_max=nonfinite)
    assert version == 1

    # O join deixa de fora os lançamentos da conta 3, a de número "C".
    written = scan(setup.storage, f"SELECT count(*) FROM delta_scan('{projected_uri}')")[0][0]
    kept = [entry_id for entry_id in range(1001, 1201) if account_of(entry_id) != 3]
    assert written == len(kept)
    assert report.rows(MONTHS[-1]) == len(kept)
    database = setup.database_file()
    assert database.exists()
    engine.cleanup()
    assert not database.exists()
