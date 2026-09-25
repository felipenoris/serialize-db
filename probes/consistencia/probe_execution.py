"""O ciclo da ``Execution`` sobre o motor DuckDB: quatro partições de 20.000 linhas com valores
de borda; numa execução, o pipeline em threads, uma por partição, cada uma com o seu ``stream``
da entrada escrevendo em dois ``loader`` com ids de ``next_ids``, a auditoria e dois ``publish``
ao mesmo tempo; um leitor ``current`` aberto e consultado enquanto outra execução ingere,
lê ``published`` e publica de novo, com os canais ``default`` e ``current``; e duas execuções
abertas na mesma versão publicando a mesma partição em threads.

.. code-block:: shell

    SERIALIZE_DB_TEST_LOCAL_ROOT=$HOME/serialize-db-local \\
        .venv/bin/python probes/consistencia/probe_execution.py
"""
from __future__ import annotations

import datetime
import decimal
import json
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from consistency_lib import (DOCS, DOUBLES, PRICES, STAMPS, TEXTS, compare, finish,
                             known_zero_sign, pick, probe_folder, report)
from serialize_db import delta, schema
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.errors import ExecutionConflict
from serialize_db.execution import Database, Execution

INFO = {"serialize_db": {"partition_by": ["data_str"], "partition_source": "data",
                         "sort_key": ["data", "id"]}}


class Base(DeclarativeBase):
    pass


class Colunas:
    """As colunas da entrada e das projeções."""

    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    data: Mapped[datetime.date] = mapped_column(sa.Date)
    valor: Mapped[float] = mapped_column(sa.Double)
    preco: Mapped[decimal.Decimal | None] = mapped_column(sa.Numeric(18, 2))
    texto: Mapped[str | None] = mapped_column(sa.String(50))
    flag: Mapped[bool | None] = mapped_column(sa.Boolean)
    carimbo: Mapped[datetime.datetime | None] = mapped_column(sa.DateTime)
    doc: Mapped[dict | None] = mapped_column(sa.JSON)
    data_str: Mapped[str] = mapped_column(sa.String(10))


class Entrada(Colunas, Base):
    __tablename__ = "cad_entradas"
    __table_args__ = {"info": INFO}


class Projetado(Colunas, Base):
    __tablename__ = "cad_projetados"
    __table_args__ = {"info": INFO}
    id_entrada: Mapped[int] = mapped_column(sa.BigInteger)


class Projetado2(Colunas, Base):
    __tablename__ = "cad_projetados_2"
    __table_args__ = {"info": INFO}
    id_entrada: Mapped[int] = mapped_column(sa.BigInteger)


class Cadastro(Base):
    __tablename__ = "cad_cadastro"
    id: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    nome: Mapped[str | None] = mapped_column(sa.String(50))
    valor: Mapped[float | None] = mapped_column(sa.Double)


ENTRADA = Entrada.__table__
PROJ = Projetado.__table__
PROJ2 = Projetado2.__table__
CADASTRO = Cadastro.__table__
MONTHS = ["2026-05-31", "2026-06-30", "2026-07-31", "2026-08-31"]
NEW_MONTH = "2026-09-30"
ROWS = 20_000
# A soma de controle da auditoria falha de 1e32 em diante (plan/OPEN_QUESTIONS.md): esses
# valores ficam fora dos dados do pipeline.
FINITE_OR_NAN = [d for d in DOUBLES if d is not None and abs(d) < 1e30]
AUGUST = MONTHS[3]


def entrada_rows(month: str, start: int, count: int) -> pa.Table:
    """``count`` linhas de ``cad_entradas`` na partição ``month`` com valores de borda."""
    ids = list(range(start, start + count))
    day = datetime.date.fromisoformat(month)
    table = pa.table({
        "id": pa.array(ids, pa.int64()),
        "data": pa.array([day] * count, pa.date32()),
        "valor": pa.array([pick(FINITE_OR_NAN, i) for i in ids], pa.float64()),
        "preco": pa.array([pick(PRICES, i) for i in ids], pa.decimal128(18, 2)),
        "texto": pa.array([f"{pick(TEXTS, i)}|{i}"[:50] if i % 7 else None for i in ids],
                          pa.string()),
        "flag": pa.array([pick([True, False, None], i) for i in ids], pa.bool_()),
        "carimbo": pa.array([pick(STAMPS, i) for i in ids], pa.timestamp("us")),
        "doc": pa.array([pick(DOCS, i) for i in ids], pa.string()),
        "data_str": pa.array([month] * count, pa.string()),
    })
    return schema.cast(table, ENTRADA)


def projected_expected(entrada: pa.Table) -> pa.Table:
    """As linhas que o pipeline projeta de ``entrada``: toda coluna copiada e o id da entrada em
    ``id_entrada``; o ``id`` fica de fora, porque vem de ``next_ids``."""
    return entrada.append_column("id_entrada", entrada.column("id")).drop_columns(["id"])


def engine_for(db: Database, folder: Path, execution_id: str) -> DuckDBEngine:
    """O motor DuckDB da execução, com os limites do ambiente e o sandbox na pasta da sonda."""
    config = DuckDBConfig(temp_directory=str(folder / f"sandbox_{execution_id}"))
    return DuckDBEngine(config, execution_id, db.storage)


def reader_config(folder: Path, name: str) -> DuckDBConfig:
    return DuckDBConfig(temp_directory=str(folder / f"leitor_{name}"))


def report_known(title: str, problems: list[str]) -> None:
    """A checagem sem a diferença conhecida do sinal do zero, impressa como leitura."""
    rest, known = known_zero_sign(problems)
    report(title, rest)
    if known:
        print(f"   diferenças conhecidas do sinal do zero: {len(known)}; {known[0]}")


def pipeline(run: Execution, months: list[str], outputs: list[sa.Table],
             loaders: dict[str, object]) -> dict[str, int]:
    """O pipeline do cliente: uma thread por partição lê a entrada por ``stream`` e escreve
    cada lote em todo ``loader`` de saída com ids de ``next_ids``; devolve as linhas por mês."""
    written = {}

    def project(month: str) -> None:
        statement = sa.select(ENTRADA).where(ENTRADA.c.data_str == month)
        rows = 0
        with run.sandbox.stream(statement, batch_size=3000) as stream:
            for batch in stream:
                for table in outputs:
                    ids = pa.array(list(run.next_ids(table, batch.num_rows)), pa.int64())
                    others = [name for name in batch.column_names if name != "id"]
                    arrays = [ids] + [batch.column(name) for name in others]
                    arrays.append(batch.column("id"))
                    names = ["id"] + others + ["id_entrada"]
                    loaders[table.name].write(pa.RecordBatch.from_arrays(arrays, names=names))
                rows += batch.num_rows
        written[month] = rows

    with ThreadPoolExecutor(max_workers=len(months)) as pool:
        list(pool.map(project, months))
    return written


def read_current(db: Database, folder: Path, table: sa.Table,
                 month: str | None = None) -> pa.Table:
    """A tabela pelo leitor Delta do canal ``current``, uma partição ou todas."""
    with db.open_delta(channel="current", config=reader_config(folder, "current")) as reader:
        statement = sa.select(table)
        if month is not None:
            statement = statement.where(table.c.data_str == month)
        return reader.query(statement)


def read_arrow(db: Database, table: sa.Table, month: str | None = None,
               version: int | None = None) -> pa.Table:
    """A tabela pelo dataset do delta-rs, uma partição ou todas."""
    found = delta.open_table(db.uri(table), db.storage, version).to_pyarrow_table()
    if month is not None:
        found = found.filter(pc.field("data_str") == month)
    return found


def seed(db: Database) -> dict[str, pa.Table]:
    """As quatro partições da entrada publicadas pelo escritor do delta-rs."""
    seeds = {}
    delta.create_table(db.uri(ENTRADA), ENTRADA, db.storage)
    for index, month in enumerate(MONTHS):
        seeds[month] = entrada_rows(month, 1 + index * ROWS, ROWS)
        delta.publish_partition(db.uri(ENTRADA), ENTRADA, month, seeds[month], {}, db.storage)
    print("semente: cad_entradas na versão",
          delta.open_table(db.uri(ENTRADA), db.storage).version())
    return seeds


def cadastro_rows() -> pa.Table:
    table = pa.table({
        "id": pa.array(range(1, 1001), pa.int64()),
        "nome": pa.array([f"{pick(TEXTS, i)}|{i}"[:50] for i in range(1, 1001)], pa.string()),
        "valor": pa.array([pick(FINITE_OR_NAN, i) for i in range(1, 1001)], pa.float64()),
    })
    return schema.cast(table, CADASTRO)


def check_execution_a(db: Database, folder: Path, cadastro: pa.Table) -> None:
    """Seção A: a execução ``exec-a`` ingere a entrada, carrega o cadastro, roda o pipeline nas
    quatro partições em threads, audita e publica em duas threads: as duas tabelas particionadas
    com ``max_workers=2`` e o cadastro."""
    problems = []
    with Execution(db, engine_for(db, folder, "exec-a"), AUGUST, "exec-a") as run:
        run.snapshot("t1")
        expected_versions = {"cad_entradas": 4, "cad_projetados": None,
                             "cad_projetados_2": None, "cad_cadastro": None}
        if run.versions != expected_versions:
            problems.append(f"A: versões {run.versions}")
        run.ingest(ENTRADA)
        previous = run.previous_partitions(ENTRADA, 3)
        if previous != MONTHS[1:]:
            problems.append(f"A: previous_partitions {previous}")
        run.sandbox.load(CADASTRO, cadastro)
        with run.sandbox.loader(PROJ) as loader, run.sandbox.loader(PROJ2) as loader2:
            written = pipeline(run, MONTHS, [PROJ, PROJ2],
                               {PROJ.name: loader, PROJ2.name: loader2})
        if any(rows != ROWS for rows in written.values()):
            problems.append(f"A: linhas do pipeline {written}")
        for table in (PROJ, PROJ2):
            run.audit(table, MONTHS)
        run.audit(CADASTRO, None)
        results = {}

        def publish_partitioned() -> None:
            results["p"] = run.publish(PROJ, PROJ2, partitions=MONTHS, max_workers=2)

        def publish_cadastro() -> None:
            results["c"] = run.publish(CADASTRO)

        threads = [threading.Thread(target=publish_partitioned),
                   threading.Thread(target=publish_cadastro)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        if results.get("p") != {PROJ.name: 4, PROJ2.name: 4}:
            problems.append(f"A: publish das particionadas {results.get('p')}")
        if results.get("c") != {CADASTRO.name: 1}:
            problems.append(f"A: publish do cadastro {results.get('c')}")
        expected_versions = {"cad_entradas": 4, "cad_projetados": 4, "cad_projetados_2": 4,
                             "cad_cadastro": 1}
        if run.versions != expected_versions:
            problems.append(f"A: versões depois do publish {run.versions}")
    report("A a execução exec-a rodou", problems)


def check_published_a(db: Database, folder: Path, seeds: dict[str, pa.Table],
                      cadastro: pa.Table) -> dict[tuple[str, str], pa.Table]:
    """Seção A2: as tabelas publicadas contra as sementes pelo dataset e pelo leitor, os ids de
    1 a 80.000 sem repetição, os metadados dos commits e a entrada do snapshot ``t1``."""
    problems = []
    expected_a = {}
    for table in (PROJ, PROJ2):
        all_ids = []
        for month in MONTHS:
            expected = projected_expected(seeds[month])
            expected_a[(table.name, month)] = expected
            arrow = read_arrow(db, table, month)
            current = read_current(db, folder, table, month)
            problems += compare(expected, arrow, key="id_entrada",
                                label=f"{table.name} {month} dataset")
            problems += compare(expected, current, key="id_entrada",
                                label=f"{table.name} {month} leitor")
            all_ids += arrow.column("id").to_pylist()
        if sorted(all_ids) != list(range(1, 4 * ROWS + 1)):
            problems.append(f"{table.name}: ids fora de 1..{4 * ROWS}: {len(set(all_ids))} "
                            f"distintos, mínimo {min(all_ids)}, máximo {max(all_ids)}")
    problems += compare(cadastro, read_arrow(db, CADASTRO), label="cadastro dataset")
    problems += compare(cadastro, read_current(db, folder, CADASTRO), label="cadastro leitor")
    # O delta-rs devolve os metadados do commit como textos: input_versions é um JSON.
    for entry in delta.history(db.uri(PROJ), db.storage):
        if entry.get("version", 0) == 0:
            continue
        input_versions = json.loads(entry.get("serialize_db_input_versions", "{}"))
        if entry.get("serialize_db_execution_id") != "exec-a" \
                or entry.get("serialize_db_snapshot") != "t1" \
                or input_versions != {"cad_entradas": 4}:
            problems.append(f"metadados do commit: {entry}")
    control, _ = delta.read_snapshots(db.storage, "prd")
    expected_snapshot = {"cad_entradas": 4, "cad_projetados": 4, "cad_projetados_2": 4,
                         "cad_cadastro": 1}
    if control.get("snapshots", {}).get("t1") != expected_snapshot:
        problems.append(f"snapshot t1: {control}")
    report_known("A2 os dados publicados iguais às sementes", problems)
    return expected_a


def count_projected(reader: object) -> int:
    return reader.query(sa.select(sa.func.count()).select_from(PROJ)).column(0)[0].as_py()


def check_pinned_reader_b(db: Database, folder: Path, seeds: dict[str, pa.Table],
                          expected_a: dict[tuple[str, str], pa.Table]) -> None:
    """Seção B: um leitor ``current`` aberto antes de ``exec-b`` e consultado a cada 50 ms
    enquanto ela ingere com ``materialize=True``, lê ``published``, projeta agosto de novo e
    setembro e publica; depois os canais ``current`` e ``default`` (em ``t1``)."""
    problems = []
    seeds[NEW_MONTH] = entrada_rows(NEW_MONTH, 1 + 4 * ROWS, ROWS)
    delta.publish_partition(db.uri(ENTRADA), ENTRADA, NEW_MONTH, seeds[NEW_MONTH], {},
                            db.storage)
    pinned = db.open_delta(channel="current", config=reader_config(folder, "preso"))
    expected_versions = {"cad_entradas": 5, "cad_projetados": 4, "cad_projetados_2": 4,
                         "cad_cadastro": 1}
    if pinned.versions != expected_versions:
        problems.append(f"B: versões do leitor preso {pinned.versions}")
    counts_seen = set()
    stop = threading.Event()

    def poll_pinned() -> None:
        while not stop.is_set():
            counts_seen.add(count_projected(pinned))
            time.sleep(0.05)

    poller = threading.Thread(target=poll_pinned)
    poller.start()
    months_b = [AUGUST, NEW_MONTH]
    try:
        with Execution(db, engine_for(db, folder, "exec-b"), NEW_MONTH, "exec-b") as run:
            # A tabela que o pipeline escreve é lida por published, nunca ingerida.
            run.ingest(ENTRADA, CADASTRO, materialize=True)
            previous = run.published(PROJ)
            max_id = run.sandbox.query(sa.select(sa.func.max(previous.c.id))).column(0)[0].as_py()
            first = run.next_ids(PROJ, 0).start
            if max_id != 4 * ROWS or first != 4 * ROWS + 1:
                problems.append(f"B: id máximo {max_id}, primeiro next_id {first}")
            with run.sandbox.loader(PROJ) as loader:
                pipeline(run, months_b, [PROJ], {PROJ.name: loader})
            run.audit(PROJ, months_b)
            versions = run.publish(PROJ, partitions=months_b)
            if versions != {PROJ.name: 6}:
                problems.append(f"B: publish {versions}")
    finally:
        stop.set()
        poller.join()
    if counts_seen != {4 * ROWS}:
        problems.append(f"B: o leitor preso viu as contagens {counts_seen}")
    after = count_projected(pinned)
    if after != 4 * ROWS:
        problems.append(f"B: o leitor preso depois do publish leu {after}")
    pinned.close()
    changed = delta.version_diff(db.uri(PROJ), 4, 6, PROJ, db.storage)
    if changed != set(months_b):
        problems.append(f"B: version_diff {changed}")
    # O canal current: 100.000 linhas, agosto com ids novos e setembro; o canal default: t1.
    current_all = read_arrow(db, PROJ)
    if current_all.num_rows != 5 * ROWS:
        problems.append(f"B: linhas em current {current_all.num_rows}")
    for month in months_b:
        found = read_current(db, folder, PROJ, month)
        problems += compare(projected_expected(seeds[month]), found, key="id_entrada",
                            label=f"B {month}")
        ids = found.column("id").to_pylist()
        if min(ids) <= 4 * ROWS:
            problems.append(f"B {month}: ids abaixo de {4 * ROWS + 1}: mínimo {min(ids)}")
    all_ids = current_all.column("id").to_pylist()
    if len(set(all_ids)) != len(all_ids):
        problems.append("B: ids repetidos em current")
    with db.open_delta(config=reader_config(folder, "default")) as reader:
        if reader.snapshot != "t1" or reader.versions.get(PROJ.name) != 4:
            problems.append(f"B: canal default {reader.snapshot} {reader.versions}")
        found = reader.query(sa.select(PROJ).where(PROJ.c.data_str == AUGUST))
        problems += compare(expected_a[(PROJ.name, AUGUST)], found, key="id_entrada",
                            label="B canal default agosto")
        ids = found.column("id").to_pylist()
        if max(ids) > 4 * ROWS:
            problems.append(f"B canal default: ids de agosto acima de {4 * ROWS}")
    report_known("B o leitor preso enquanto exec-b escreve; os canais", problems)


def check_race_c(db: Database, folder: Path, seeds: dict[str, pa.Table]) -> None:
    """Seção C: ``exec-c`` e ``exec-d`` abertas na mesma versão publicam setembro em threads:
    uma commita e a outra recebe ``ExecutionConflict``; a partição fica com um arquivo só, o da
    vencedora, e o arquivo do ``COPY`` da perdedora é o órfão fora do log."""
    problems = []
    outcomes: dict[str, object] = {}
    with ExitStack() as stack:
        runs = {}
        for name in ("exec-c", "exec-d"):
            runs[name] = stack.enter_context(Execution(db, engine_for(db, folder, name),
                                                       NEW_MONTH, name))
        for name, run in runs.items():
            run.ingest(ENTRADA)
            with run.sandbox.loader(PROJ) as loader:
                pipeline(run, [NEW_MONTH], [PROJ], {PROJ.name: loader})
            run.audit(PROJ, [NEW_MONTH])

        def publish(name: str) -> None:
            try:
                outcomes[name] = runs[name].publish(PROJ, partitions=[NEW_MONTH])
            except ExecutionConflict as error:
                outcomes[name] = error

        threads = [threading.Thread(target=publish, args=(name,)) for name in runs]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
    winners = [name for name, outcome in outcomes.items() if isinstance(outcome, dict)]
    losers = [name for name, outcome in outcomes.items()
              if isinstance(outcome, ExecutionConflict)]
    print(f"   resultados: {outcomes}")
    if len(winners) != 1 or len(losers) != 1:
        problems.append(f"C: vencedoras {winners}, perdedoras {losers}")
    table = delta.open_table(db.uri(PROJ), db.storage)
    if table.version() != 7:
        problems.append(f"C: versão {table.version()}")
    actions = pa.table(table.get_add_actions(flatten=True))
    september = actions.filter(pc.field("partition.data_str") == NEW_MONTH)
    if september.num_rows != 1:
        problems.append(f"C: arquivos de setembro {september.num_rows}")
    paths = set(september.column("path").to_pylist())
    if winners and not all(winners[0] in path for path in paths):
        problems.append(f"C: o arquivo de setembro não é da vencedora: {paths}")
    partition_folder = folder / "delta" / "prd" / PROJ.name / f"data_str={NEW_MONTH}"
    on_disk = {f"data_str={NEW_MONTH}/{p.name}" for p in partition_folder.glob("*.parquet")}
    print(f"   arquivos na pasta da partição {len(on_disk)}, órfãos {sorted(on_disk - paths)}")
    found = read_current(db, folder, PROJ, NEW_MONTH)
    problems += compare(projected_expected(seeds[NEW_MONTH]), found, key="id_entrada",
                        label="C setembro")
    all_ids = read_arrow(db, PROJ).column("id").to_pylist()
    if len(set(all_ids)) != len(all_ids):
        problems.append("C: ids repetidos")
    report_known("C duas execuções disputando a mesma partição", problems)


def main() -> None:
    folder = probe_folder("execucao")
    db = Database(str(folder / "delta"), "prd", Base.metadata)
    seeds = seed(db)
    cadastro = cadastro_rows()
    check_execution_a(db, folder, cadastro)
    expected_a = check_published_a(db, folder, seeds, cadastro)
    delta.set_channel(db.storage, "prd", "default", "t1")
    check_pinned_reader_b(db, folder, seeds, expected_a)
    check_race_c(db, folder, seeds)
    versions = {t.name: delta.open_table(db.uri(t), db.storage).version() for t in db.tables()}
    print("versões finais:", versions)
    finish(folder)


if __name__ == "__main__":
    main()
