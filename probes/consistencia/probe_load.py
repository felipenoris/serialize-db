"""A carga inicial da base Parquet fictícia de ``tests/source_db_projetado.py`` em Delta por
``initial_load``, tabela a tabela na ordem de ``load_order``, com o conteúdo de cada tabela
comparado valor a valor entre os arquivos de origem, lidos por ``read_parquet`` e levados ao
contrato, e a tabela Delta lida pelo dataset do delta-rs e pelo ``delta_scan``; a segunda
passagem sem commit e ``load_report`` fechando.

.. code-block:: shell

    SERIALIZE_DB_TEST_LOCAL_ROOT=$HOME/serialize-db-local \\
        .venv/bin/python probes/consistencia/probe_load.py
"""
from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sa

# A base fictícia e o modelo do cliente vivem em tests/, que a sonda põe no caminho.
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "tests"))
import source_db_projetado as source  # noqa: E402
from client_model import Base  # noqa: E402
from consistency_lib import compare, finish, print_notes, probe_folder, report  # noqa: E402
from consistency_lib import to_contract  # noqa: E402
from serialize_db import delta, load  # noqa: E402
from serialize_db.engine.duckdb import DuckDBConfig  # noqa: E402
from serialize_db.execution import Database  # noqa: E402
from serialize_db.schema import table_options  # noqa: E402

NOTES: set[str] = set()
KEY_COLUMN = "_chave"


def read_source(connection: object, folder: Path, table: sa.Table) -> pa.Table:
    """Os arquivos de origem de uma tabela, com a coluna de partição Hive tomada do caminho,
    levados ao contrato."""
    partition_by = table_options(table).partition_by
    table_folder = folder / table.name
    if partition_by is None:
        text = f"SELECT * FROM read_parquet('{table_folder}/*.parquet', union_by_name=true)"
    else:
        text = (f"SELECT * FROM read_parquet('{table_folder}/*/*.parquet', "
                f"hive_partitioning=true, union_by_name=true)")
    return to_contract(connection.execute(text).to_arrow_table(), table, NOTES)


def read_scan(connection: object, db: Database, table: sa.Table) -> pa.Table:
    text = f"SELECT * FROM delta_scan('{db.uri(table)}')"
    return to_contract(connection.execute(text).to_arrow_table(), table, NOTES)


def read_arrow(db: Database, table: sa.Table) -> pa.Table:
    found = delta.open_table(db.uri(table), db.storage).to_pyarrow_table()
    return to_contract(found, table, NOTES)


def with_key(table: sa.Table, data: pa.Table) -> tuple[pa.Table, str]:
    """A tabela com a coluna de ordenação da comparação: a chave única quando é uma coluna, ou
    a chave composta unida num texto em ``_chave``."""
    key = list(table_options(table).keys[0])
    if len(key) == 1:
        return data, key[0]
    parts = [pc.cast(data.column(name), pa.string()) for name in key]
    joined = pc.binary_join_element_wise(*parts, "|")
    return data.append_column(KEY_COLUMN, joined), KEY_COLUMN


def check_table(connection: object, db: Database, root: str, config: DuckDBConfig,
                source_folder: Path, table: sa.Table) -> list[str]:
    """Uma tabela: a carga, a segunda passagem, os dois leitores contra a origem e o relatório."""
    loaded = load.initial_load(db, table, root, config=config)
    again = load.initial_load(db, table, root, config=config)
    problems = []
    if again:
        problems.append(f"a segunda passagem carregou {again}")
    expected, key = with_key(table, read_source(connection, source_folder, table))
    readers = (("dataset", read_arrow(db, table)), ("delta_scan", read_scan(connection, db, table)))
    for reader_name, found in readers:
        found, _ = with_key(table, found)
        differences = compare(expected, found, key=key, label=f"{table.name} {reader_name}")
        problems += [p for p in differences if KEY_COLUMN not in p]
    report_rows = load.load_report(db, table, root, config=config)
    if not report_rows.matches:
        problems.append(f"load_report não fecha: {report_rows}")
    print(f"   {table.name}: {expected.num_rows} linhas, carga {loaded}")
    return problems


def main() -> None:
    folder = probe_folder("carga")
    base = source.write_source(folder / "origem")
    db = Database(str(folder / "delta"), "prd", Base.metadata)
    config = DuckDBConfig(temp_directory=str(folder / "sandbox"))
    tables = load.load_order(list(Base.metadata.tables.values()))
    print("tabelas:", [t.name for t in tables])
    connection = db.storage.duckdb_connect()
    all_problems = []
    try:
        for table in tables:
            problems = check_table(connection, db, str(base.root), config, folder / "origem",
                                   table)
            report(f"L {table.name}", problems)
            all_problems += problems
    finally:
        connection.close()
    report("L a carga inicial igual à origem pelos dois leitores", all_problems)
    print_notes(NOTES)
    finish(folder)


if __name__ == "__main__":
    main()
