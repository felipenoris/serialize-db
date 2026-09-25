"""O motor Redshift sob concorrência, pelo pytest com as fixtures das suítes: ``ingest`` e
``query``, quatro ``stream`` ao mesmo tempo com ``query`` ao lado, dois ``loader`` em threads, a
auditoria e ``export_partition`` lidos pelo dataset do delta-rs e pelo ``delta_scan``, duas
sessões a mais em threads, ``publish_redshift`` com dois workers e o leitor publicado consultado e
transmitido por duas threads. No ambiente alvo roda contra o Redshift e o S3 reais, num ambiente
``poc<id>`` próprio, cujas tabelas publicadas e linhas de controle saem no fim; na máquina de
quem desenvolve, contra o substituto de ``tests/emulator.py``.

.. code-block:: shell

    PYTHONPATH=tests .venv/bin/python -m pytest -p conftest -m redshift -s \\
        probes/consistencia/probe_redshift_test.py
    SERIALIZE_DB_TEST_EMULATOR=1 PYTHONPATH=tests .venv/bin/python -m pytest -p conftest \\
        -m redshift -s probes/consistencia/probe_redshift_test.py
"""
from __future__ import annotations

import dataclasses
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor

import pyarrow as pa
import pyarrow.compute as pc
import pytest
import redshift_connector
import sqlalchemy as sa

from consistency_lib import compare
from conftest import S3Location, redshift_config
from lancamentos_model import ACCOUNTS, ENTRIES, MONTHS, PROJECTED, Base, account_rows, entry_rows
from serialize_db import delta, publication
from serialize_db.engine import redshift
from serialize_db.engine.redshift import RedshiftConfig, RedshiftEngine
from serialize_db.execution import Database
from serialize_db.reader import open_redshift

ROWS = 2000
JSON_COLUMNS = ("meta",)


@dataclasses.dataclass
class Target:
    """O banco numa raiz nova do bucket, a configuração e o ambiente ``poc<id>`` da sonda."""

    db: Database
    config: RedshiftConfig
    unload_to: str

    def published(self, table: sa.Table) -> str:
        return f'"{self.config.schema}"."{self.db.environment}_{table.name}"'

    def control(self) -> str:
        return f'"{self.config.schema}"."{publication.CONTROL_TABLE}"'


def execute_all(config: RedshiftConfig, texts: list[str]) -> None:
    """Os comandos numa conexão própria, em ordem."""
    connection = redshift.connect(config)
    try:
        cursor = connection.cursor()
        for text in texts:
            cursor.execute(text)
    finally:
        connection.close()


def relation_exists(config: RedshiftConfig, qualified: str) -> bool:
    """Se a tabela existe, por ``select 1 ... limit 0``."""
    try:
        execute_all(config, [f"SELECT 1 FROM {qualified} LIMIT 0"])
    except redshift_connector.Error as error:
        if redshift.relation_missing(error):
            return False
        raise
    return True


@pytest.fixture
def target(s3_location: S3Location, redshift_driver: None) -> Iterator[Target]:
    """O banco da sonda num ambiente ``poc<id>`` e a tabela de controle, criada quando não existe
    e apagada só nesse caso; as tabelas publicadas e as linhas de controle do ambiente saem no
    fim."""
    config = redshift_config()
    environment = f"poc{uuid.uuid4().hex[:8]}"
    db = Database(s3_location.child(f"consistencia/{environment}"), environment, Base.metadata)
    created = not relation_exists(config, f'"{config.schema}"."{publication.CONTROL_TABLE}"')
    if created:
        publication.create_publications_table(config)
    target = Target(db, config, s3_location.child(f"consistencia/{environment}-unload"))
    yield target
    cleanup = [f"DROP TABLE IF EXISTS {target.published(table)}"
               for table in Base.metadata.sorted_tables]
    if created:
        cleanup.append(f"DROP TABLE {target.control()}")
    else:
        cleanup.append(f"DELETE FROM {target.control()} WHERE table_name LIKE '{environment}_%'")
    execute_all(config, cleanup)


def count_of(engine: RedshiftEngine, name: str) -> int:
    found = engine.query(f"SELECT count(*) AS n FROM {engine.qualified(name)}")
    return found.column("n")[0].as_py()


def seed(db: Database) -> tuple[dict[str, pa.Table], int, pa.Table, int]:
    """As duas partições dos lançamentos e as contas, publicadas pelo escritor do delta-rs."""
    seeds = {}
    uri = db.uri(ENTRIES)
    delta.create_table(uri, ENTRIES, db.storage)
    version = 0
    for index, month in enumerate(MONTHS):
        seeds[month] = entry_rows(month, 1 + index * ROWS, ROWS)
        version = delta.publish_partition(uri, ENTRIES, month, seeds[month], {}, db.storage)
    accounts_uri = db.uri(ACCOUNTS)
    delta.create_table(accounts_uri, ACCOUNTS, db.storage)
    accounts = account_rows(["A", "B", "C"])
    accounts_version = delta.publish_partition(accounts_uri, ACCOUNTS, None, accounts, {},
                                               db.storage)
    return seeds, version, accounts, accounts_version


def concurrent_streams(engine: RedshiftEngine, seeds: dict[str, pa.Table]) -> list[str]:
    """Quatro streams ao mesmo tempo, cada um de uma partição, com uma consulta no meio."""
    def stream_month(index: int) -> list[str]:
        month = MONTHS[index % 2]
        statement = sa.select(ENTRIES).where(ENTRIES.c.data_base_str == month)
        with engine.stream(statement, batch_size=300 + 100 * index) as stream:
            found = pa.Table.from_batches(list(stream))
        count = engine.query(sa.select(sa.func.count()).select_from(ENTRIES)).column(0)[0].as_py()
        problems = compare(seeds[month], found, "id_lancamento", f"stream {index}", JSON_COLUMNS)
        if count != 2 * ROWS:
            problems.append(f"query ao lado dos streams: {count}")
        return problems

    problems = []
    with ThreadPoolExecutor(max_workers=4) as pool:
        for result in pool.map(stream_month, range(4)):
            problems += result
    return problems


def concurrent_loaders(engine: RedshiftEngine, seeds: dict[str, pa.Table],
                       second: sa.Table) -> tuple[list[str], dict[str, pa.Table]]:
    """Dois loaders em threads, cada um do seu stream, com os ids deslocados; devolve os
    problemas e o que cada tabela deve conter."""
    def pipeline(table: sa.Table, month: str, offset: int) -> int:
        statement = sa.select(ENTRIES).where(ENTRIES.c.data_base_str == month)
        with engine.stream(statement, batch_size=500) as stream, engine.loader(table) as loader:
            for batch in stream:
                ids = pc.add(batch.column("id_lancamento"), offset)
                loader.write(batch.set_column(0, "id_lancamento", ids))
        return loader.rows

    jobs = ((PROJECTED, MONTHS[0], 10_000), (second, MONTHS[1], 20_000))
    expected = {}
    for table, month, offset in jobs:
        shifted = pc.add(seeds[month].column("id_lancamento"), offset)
        expected[table.name] = seeds[month].set_column(0, "id_lancamento", shifted)
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(pipeline, *job) for job in jobs]
        rows = [future.result() for future in futures]
    problems = []
    if rows != [ROWS, ROWS]:
        problems.append(f"linhas dos loaders {rows}")
    for table, _, _ in jobs:
        found = engine.query(sa.select(table))
        problems += compare(expected[table.name], found, "id_lancamento",
                            f"carregada {table.name}", JSON_COLUMNS)
    return problems, expected


def export_projected(engine: RedshiftEngine, db: Database, expected: pa.Table,
                     readings: list[str]) -> tuple[list[str], int]:
    """A auditoria e a exportação da projeção, lidas pelo dataset e pelo ``delta_scan``."""
    uri = db.uri(PROJECTED)
    delta.create_table(uri, PROJECTED, db.storage)
    report = engine.audit(PROJECTED, [MONTHS[0]], uri, 0)
    problems = []
    if not report.passed:
        problems.append(f"auditoria {[r.name for r in report.results if not r.passed]}")
    metadata = delta.commit_metadata(engine.execution_id, {}, None)
    version = engine.export_partition(PROJECTED, uri, MONTHS[0], metadata,
                                      expected_rows=report.rows(MONTHS[0]))
    arrow = delta.open_table(uri, db.storage).to_pyarrow_table()
    problems += compare(expected, arrow, "id_lancamento", "export dataset", JSON_COLUMNS)
    with db.storage.duckdb_connect() as connection:
        scan = connection.execute(f"SELECT * FROM delta_scan('{uri}')").to_arrow_table()
    raw_types = []
    for field in scan.schema:
        if str(field.type) != str(arrow.schema.field(field.name).type):
            raw_types.append((field.name, str(field.type)))
    readings.append(f"tipos do delta_scan diferentes do dataset: {raw_types}")
    differences = compare(expected, scan, "id_lancamento", "export delta_scan", JSON_COLUMNS)
    problems += [p for p in differences if " no tipo " not in p]
    return problems, version


def extra_sessions(engine: RedshiftEngine, accounts_uri: str, accounts_version: int) -> list[str]:
    """Duas sessões a mais em threads: uma ingere as contas, a outra consulta; a ingestão fica
    visível na sessão principal."""
    def ingest_accounts() -> None:
        with engine.new_session() as session:
            session.ingest(ACCOUNTS, accounts_uri, accounts_version)

    def query_entries() -> int:
        with engine.new_session() as session:
            statement = sa.select(sa.func.count()).select_from(ENTRIES)
            return session.query(statement).column(0)[0].as_py()

    problems = []
    with ThreadPoolExecutor(max_workers=2) as pool:
        ingest_future = pool.submit(ingest_accounts)
        query_future = pool.submit(query_entries)
        ingest_future.result()
        if query_future.result() != 2 * ROWS:
            problems.append(f"query na sessão a mais {query_future.result()}")
    if count_of(engine, f"{engine.prefix}cad_contas") != 3:
        problems.append("as contas ingeridas na sessão a mais não aparecem na principal")
    return problems


def publish_and_read(target: Target, execution_id: str, expected: pa.Table,
                     accounts: pa.Table, versions: dict[str, int],
                     readings: list[str]) -> list[str]:
    """``publish_redshift`` com dois workers e o leitor publicado, consultado e transmitido por
    duas threads."""
    problems = []
    published = publication.publish_redshift(target.db, target.config, [PROJECTED, ACCOUNTS],
                                             execution_id, max_workers=2)
    if published != versions:
        problems.append(f"publicadas {published}, esperadas {versions}")
    reader = open_redshift(Base.metadata, target.db.environment, target.config,
                           unload_to=target.unload_to)
    try:
        def read_published(index: int) -> list[str]:
            found = reader.query(sa.select(PROJECTED))
            out = compare(expected, found, "id_lancamento", f"leitor query {index}", JSON_COLUMNS)
            with reader.stream(sa.select(ACCOUNTS), batch_size=2) as stream:
                streamed = pa.Table.from_batches(list(stream))
            out += compare(accounts, streamed, "id_conta", f"leitor stream {index}")
            return out

        with ThreadPoolExecutor(max_workers=2) as pool:
            for result in pool.map(read_published, range(2)):
                problems += result
    finally:
        reader.close()
    status = publication.publication_status(target.db, target.config)
    readings.append(f"estado da publicação: {[(s.table, s.published_version) for s in status]}")
    return problems


@pytest.mark.redshift
@pytest.mark.s3
def test_redshift_consistency(target: Target) -> None:
    """A sonda inteira, na ordem do cabeçalho; imprime as leituras e os problemas, e falha com
    qualquer problema."""
    db = target.db
    execution_id = f"exec-{uuid.uuid4().hex[:8]}"
    engine = RedshiftEngine(target.config, execution_id, db.storage,
                            f"{db.environment}/staging/{execution_id}")
    problems: list[str] = []
    readings: list[str] = []
    try:
        seeds, version, accounts, accounts_version = seed(db)
        engine.ingest(ENTRIES, db.uri(ENTRIES), version)
        by_query = engine.query(sa.select(ENTRIES).order_by(ENTRIES.c.id_lancamento))
        expected_all = pa.concat_tables(list(seeds.values()))
        problems += compare(expected_all, by_query, "id_lancamento", "query", JSON_COLUMNS)
        problems += concurrent_streams(engine, seeds)
        second = PROJECTED.to_metadata(sa.MetaData(), name="cad_projetados_2")
        loader_problems, expected_loaded = concurrent_loaders(engine, seeds, second)
        problems += loader_problems
        export_problems, exported = export_projected(engine, db, expected_loaded[PROJECTED.name],
                                                     readings)
        problems += export_problems
        problems += extra_sessions(engine, db.uri(ACCOUNTS), accounts_version)
        problems += publish_and_read(target, execution_id, expected_loaded[PROJECTED.name],
                                     accounts, {PROJECTED.name: exported,
                                                ACCOUNTS.name: accounts_version}, readings)
    finally:
        engine.cleanup()
    print("\nLEITURAS:")
    for line in readings:
        print("  ", line)
    print("PROBLEMAS:" if problems else "PROBLEMAS: nenhum")
    for line in problems:
        print("  -", line)
    assert not problems
