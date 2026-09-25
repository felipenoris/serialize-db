"""Os tipos pela fronteira do motor DuckDB: 2.000 linhas de ``cad_tudo``, toda coluna do contrato
com os seus valores de borda, por ``load``, ``query``, ``export_partition`` e
``publish_partition``, lidas pelo dataset do delta-rs, pelo ``delta_scan`` e pelo arquivo Parquet
registrado; o mínimo e o máximo do log contra os dados; um ``stream`` com o transbordo forçado
num ``loader`` de outra sessão; e as estatísticas exatas do log de ``cad_simples``, com a poda dos
dois leitores nos extremos como leitura.

.. code-block:: shell

    SERIALIZE_DB_TEST_LOCAL_ROOT=$HOME/serialize-db-local \\
        .venv/bin/python probes/consistencia/probe_types.py
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import sqlalchemy as sa

from consistency_lib import (DOUBLES, SIMPLES, TEXTS, TUDO, compare, edge_rows, finish,
                             known_zero_sign, print_notes, probe_folder, report, to_contract)
from serialize_db import delta, schema
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine, DuckDBStream
from serialize_db.storage import Storage

FIRST_PARTITION = "2026-08-31"
SECOND_PARTITION = "2026-09-30"
ROWS = 2000
METADATA = delta.commit_metadata("exec-tipos", {})
NOTES: set[str] = set()


def report_known(title: str, problems: list[str]) -> None:
    """A checagem sem a diferença conhecida do sinal do zero, impressa como leitura."""
    rest, known = known_zero_sign(problems)
    report(title, rest)
    if known:
        print(f"   diferenças conhecidas do sinal do zero: {len(known)}; {known[0]}")


def check_loader_and_query(engine: DuckDBEngine, data: pa.Table) -> None:
    """Seção 1: ``load`` e ``query`` por statement e por texto devolvem as linhas carregadas."""
    engine.load(TUDO, data)
    by_statement = engine.query(sa.select(TUDO))
    print("   esquema Arrow do sandbox:", by_statement.schema.types)
    report("1 load e query por statement",
           compare(data, to_contract(by_statement, TUDO, NOTES), label="query"))
    by_text = engine.query('SELECT * FROM "cad_tudo"')
    report("1b load e query por texto",
           compare(data, to_contract(by_text, TUDO, NOTES), label="query-texto"))


def short(value: object) -> str:
    """O ``repr`` de um valor com no máximo 40 caracteres, para os textos longos do log."""
    text = repr(value)
    return text if len(text) <= 40 else text[:37] + "..."


def log_bounds_data(actions: list[dict], data: pa.Table) -> list[str]:
    """As estatísticas do log limitam os dados: o ``nullCount`` igual, e o mínimo e o máximo
    fora dos extremos dos dados finitos; a coluna sem estatística é impressa."""
    problems = []
    contract = schema.arrow_schema(TUDO)
    for action in actions:
        for name in contract.names:
            if name == "data_str":
                continue
            column = data.column(name)
            nulls = action.get(f"null_count.{name}")
            if nulls is not None and nulls != column.null_count:
                problems.append(f"{name}: nullCount {nulls} no log, {column.null_count} nos dados")
            low, high = action.get(f"min.{name}"), action.get(f"max.{name}")
            if low is None and high is None:
                print(f"   (sem mínimo e máximo no log para {name})")
                continue
            finite = column
            if pa.types.is_floating(column.type):
                finite = pc.filter(column, pc.and_(pc.is_finite(column), pc.is_valid(column)))
            extremes = pc.min_max(finite)
            real_low, real_high = extremes["min"].as_py(), extremes["max"].as_py()
            if pa.types.is_date(column.type) and isinstance(low, str):
                real_low, real_high = real_low.isoformat(), real_high.isoformat()
            print(f"   {name}: log {short(low)}..{short(high)}, "
                  f"dados {short(real_low)}..{short(real_high)}")
            if low > real_low:
                problems.append(f"{name}: mínimo do log {low!r} acima do mínimo {real_low!r}")
            if high < real_high:
                problems.append(f"{name}: máximo do log {high!r} abaixo do máximo {real_high!r}")
    return problems


def check_export(engine: DuckDBEngine, config: DuckDBConfig, storage: Storage, uri: str,
                 folder: Path, data: pa.Table) -> int:
    """Seção 2: ``export_partition`` pelo ``COPY`` do DuckDB, lido pelo dataset do delta-rs, pelo
    ``delta_scan`` de outra sessão e pelo arquivo Parquet registrado; as estatísticas do log."""
    delta.create_table(uri, TUDO, storage)
    version = engine.export_partition(TUDO, uri, FIRST_PARTITION, METADATA, expected_rows=ROWS,
                                      columns_without_min_max=["valor"])
    print("   versão exportada:", version)
    table = delta.open_table(uri, storage)
    by_dataset = table.to_pyarrow_table()
    report_known("2a export_partition pelo dataset do delta-rs",
                 compare(data, to_contract(by_dataset, TUDO, NOTES), label="delta-rs"))
    with DuckDBEngine(config, "exec-tipos-scan", storage) as other:
        other.ingest(TUDO, uri, version)
        by_scan = other.query(sa.select(TUDO))
        print("   esquema Arrow do delta_scan:", by_scan.schema.types)
        report_known("2b export_partition pelo delta_scan",
                     compare(data, to_contract(by_scan, TUDO, NOTES), label="delta_scan"))
    actions = pa.table(table.get_add_actions(flatten=True)).to_pylist()
    file_path = folder / "delta" / "prd" / TUDO.name / actions[0]["path"]
    direct = pq.read_table(file_path)
    print("   esquema do arquivo Parquet:", direct.schema.types)
    direct = direct.append_column("data_str", pa.array([FIRST_PARTITION] * direct.num_rows))
    report_known("2c export_partition pelo arquivo Parquet",
                 compare(data, to_contract(direct, TUDO, NOTES), label="parquet"))
    report("2d as estatísticas do log limitam os dados", log_bounds_data(actions, data))
    return version


def check_publish(config: DuckDBConfig, storage: Storage, uri: str, data: pa.Table) -> None:
    """Seção 3: ``publish_partition`` pelo escritor do delta-rs, noutra partição, lido pelo
    dataset e pelo ``delta_scan``."""
    version = delta.publish_partition(uri, TUDO, SECOND_PARTITION, data, METADATA, storage,
                                      columns_without_min_max=["valor"])
    table = delta.open_table(uri, storage)
    by_dataset = table.to_pyarrow_table(filters=[("data_str", "=", SECOND_PARTITION)])
    report("3a publish_partition pelo dataset do delta-rs",
           compare(data, to_contract(by_dataset, TUDO, NOTES), label="delta-rs"))
    with DuckDBEngine(config, "exec-tipos-scan2", storage) as other:
        other.ingest(TUDO, uri, version, partitions=[SECOND_PARTITION])
        by_scan = other.query(sa.select(TUDO))
        report("3b publish_partition pelo delta_scan",
               compare(data, to_contract(by_scan, TUDO, NOTES), label="delta_scan"))


def check_spilled_stream(engine: DuckDBEngine, config: DuckDBConfig, storage: Storage,
                         data: pa.Table) -> None:
    """Seção 4: um ``stream`` de 300 linhas por lote com 10.000 bytes de orçamento, o transbordo
    forçado, num ``loader`` de outra sessão."""
    with DuckDBEngine(config, "exec-tipos-segunda", storage) as second:
        stream = DuckDBStream(engine, 'SELECT * FROM "cad_tudo"', [], batch_size=300,
                              budget=10_000)
        with stream, second.loader(TUDO) as loader:
            batches = 0
            for batch in stream:
                loader.write(batch)
                batches += 1
            # O contador de lotes transbordados é do transbordo do stream: uma leitura.
            print(f"   lotes {batches}, transbordados {stream._spool.spilled}")
        by_second = second.query(sa.select(TUDO))
        report("4 stream com transbordo para o loader de outra sessão",
               compare(data, to_contract(by_second, TUDO, NOTES), label="segunda"))


def check_whole_table(storage: Storage, uri: str, expected: pa.Table) -> None:
    """Seção 5: as duas partições, de escritores diferentes, lidas juntas pelo dataset."""
    found = delta.open_table(uri, storage).to_pyarrow_table()
    report_known("5 a tabela inteira pelo dataset do delta-rs",
                 compare(expected, to_contract(found, TUDO, NOTES), label="tudo"))


def simple_rows() -> pa.Table:
    """400 linhas de ``cad_simples`` só com valores finitos e textos sem ``NUL``, para o mínimo e
    o máximo exatos do log."""
    finite = [d for d in DOUBLES if d is not None and d == d and abs(d) != float("inf")]
    names = [t.replace("\x00", "0")[:20] for t in TEXTS] + ["ÿÿ", "a" * 20, "Ω"]
    count = 400
    table = pa.table({
        "id": pa.array(range(1, count + 1), pa.int64()),
        "nome": pa.array([names[i % len(names)] for i in range(count)]),
        "valor": pa.array([finite[i % len(finite)] for i in range(count)], pa.float64()),
    })
    return schema.cast(table, SIMPLES)


def print_pruning(storage: Storage, uri: str, simple: pa.Table) -> None:
    """A poda nos extremos como leitura: as contagens do ``delta_scan`` e do dataset com filtros
    iguais ao mínimo e ao máximo, contra as contagens nos dados."""
    extremes = pc.min_max(simple.column("valor"))
    low, high = extremes["min"].as_py(), extremes["max"].as_py()
    filters = [f"valor = {high!r}", f"valor = {low!r}", f"valor >= {high!r}", "nome = 'ÿÿ'",
               "nome = ''"]
    connection = storage.duckdb_connect()
    try:
        for clause in filters:
            text = f"SELECT count(*) FROM delta_scan('{uri}') WHERE {clause}"
            print(f"   delta_scan {clause}: {connection.execute(text).fetchone()[0]}")
    finally:
        connection.close()
    dataset = delta.open_table(uri, storage).to_pyarrow_dataset()
    print(f"   dataset valor = {high!r}: {dataset.count_rows(filter=ds.field('valor') == high)}, "
          f"valor = {low!r}: {dataset.count_rows(filter=ds.field('valor') == low)}, "
          f"nome = 'ÿÿ': {dataset.count_rows(filter=ds.field('nome') == 'ÿÿ')}, "
          f"nome = '': {dataset.count_rows(filter=ds.field('nome') == '')}")
    print(f"   dados valor = {high!r}: {pc.sum(pc.equal(simple.column('valor'), high)).as_py()}, "
          f"valor = {low!r}: {pc.sum(pc.equal(simple.column('valor'), low)).as_py()}, "
          f"nome = 'ÿÿ': {pc.sum(pc.equal(simple.column('nome'), 'ÿÿ')).as_py()}, "
          f"nome = '': {pc.sum(pc.equal(simple.column('nome'), '')).as_py()}")


def check_exact_stats(engine: DuckDBEngine, storage: Storage) -> None:
    """Seção 6: numa tabela só de valores finitos, o mínimo e o máximo do log gravado pelo
    ``COPY`` são exatamente os extremos dos dados, e a tabela volta igual pelo dataset."""
    simple = simple_rows()
    uri = storage.uri_of("prd/cad_simples")
    delta.create_table(uri, SIMPLES, storage)
    engine.load(SIMPLES, simple)
    engine.export_partition(SIMPLES, uri, None, METADATA, expected_rows=simple.num_rows)
    action = pa.table(delta.open_table(uri, storage).get_add_actions(flatten=True)).to_pylist()[0]
    problems = []
    for name in ("valor", "nome", "id"):
        extremes = pc.min_max(simple.column(name))
        low, high = extremes["min"].as_py(), extremes["max"].as_py()
        logged_low, logged_high = action.get(f"min.{name}"), action.get(f"max.{name}")
        print(f"   {name}: dados {low!r}..{high!r}, log {logged_low!r}..{logged_high!r}")
        if logged_low != low or logged_high != high:
            problems.append(f"{name}: log {logged_low!r}..{logged_high!r} difere dos dados "
                            f"{low!r}..{high!r}")
    report("6 o mínimo e o máximo exatos do log nos tipos exatos", problems)
    found = delta.open_table(uri, storage).to_pyarrow_table()
    report_known("6b cad_simples de volta pelo dataset do delta-rs",
                 compare(simple, to_contract(found, SIMPLES, NOTES), label="simples"))
    print_pruning(storage, uri, simple)


def main() -> None:
    folder = probe_folder("tipos")
    storage = Storage.for_uri(str(folder / "delta"))
    uri = storage.uri_of("prd/cad_tudo")
    config = DuckDBConfig(temp_directory=str(folder / "sandbox"))
    engine = DuckDBEngine(config, "exec-tipos", storage)
    data = edge_rows(FIRST_PARTITION, 1, ROWS)
    second_data = edge_rows(SECOND_PARTITION, 5001, ROWS)
    print(f"linhas {data.num_rows}, colunas {data.schema.names}")
    try:
        check_loader_and_query(engine, data)
        check_export(engine, config, storage, uri, folder, data)
        check_publish(config, storage, uri, second_data)
        check_spilled_stream(engine, config, storage, data)
        check_whole_table(storage, uri, pa.concat_tables([data, second_data]))
        check_exact_stats(engine, storage)
    finally:
        engine.cleanup()
    print_notes(NOTES)
    finish(folder)


if __name__ == "__main__":
    main()
