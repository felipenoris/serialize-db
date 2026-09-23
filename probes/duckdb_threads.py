"""O ``threads`` do DuckDB na ingestão das tabelas Delta: o padrão, um por núcleo, contra valores
acima dos núcleos, e a sessão a mais por tabela de ``run.ingest``.

O DuckDB lê arquivo remoto com E/S síncrona, uma requisição HTTP por thread, e a documentação
recomenda ``threads`` de 2 a 5 vezes os núcleos para essa leitura (``plan/duckdb.md``). Este probe
mede, sobre as tabelas Delta que a migração adiantada grava (``scripts/migrate_parquet_to_delta.py``),
a ingestão pelo motor DuckDB do pacote com cada valor de ``threads``: é a medição que decide o padrão
de ``DuckDBConfig.threads`` numa raiz no S3 (etapa 4) e o que a sessão a mais por tabela de
``run.ingest`` ganha no ambiente alvo (etapa 6).

Uso:

    PYTHONPATH=tests .venv/bin/python probes/duckdb_threads.py <raiz> [--metadata MÓDULO:ATRIBUTO]
        [--tables TABELA ...] [--partition AAAA-MM-DD] [--threads N ...] [--repetitions N]

``<raiz>`` é a pasta com uma tabela Delta por subpasta, local ou ``s3://bucket/prefixo``: o
``--root`` da migração. ``--metadata`` é o modelo, ``client_model:Base.metadata`` por padrão, com
``tests`` no ``PYTHONPATH``. ``--tables`` são as tabelas medidas, a primeira a grande; por padrão
``cad_lancamentos``, ``cad_contratos``, ``cad_operacoes`` e ``rel_contrato_operacao``.
``--partition`` é a partição lida em cada tabela particionada, por padrão a mais recente comum a
elas; uma tabela sem partição é lida inteira. ``--threads`` são os valores medidos, por padrão o
padrão do DuckDB vezes 1 a 5. ``--repetitions`` é quantas vezes cada medida roda, 3 por padrão; o
relatório dá cada repetição e a melhor.

Cada configuração roda num processo novo (``spawn``), num motor ``DuckDBEngine`` novo, com o seu
pico de memória (``VmHWM`` no Linux) ao lado da base depois das importações e da conexão:

- ``materializada``: a partição da primeira tabela por ``ingest(..., materialize=True)``, o
  ``CREATE TABLE AS`` sobre ``delta_scan``, apagada depois de cada repetição;
- ``agregada``: a mesma partição pela view de ``ingest``, lida inteira por
  ``SELECT count(*), max(COLUMNS(*))``, sem gravar;
- ``em série``: as tabelas materializadas uma depois da outra na sessão principal;
- ``sessões a mais``: cada tabela materializada numa sessão a mais (``new_session()``), todas
  juntas, como ``run.ingest`` de várias tabelas.

Só leitura sob a raiz: as tabelas são lidas pelo ``delta_scan`` e pelo log do delta-rs, e nada é
criado, alterado ou apagado nela. O banco do DuckDB de cada configuração fica na pasta temporária do
motor, que o ``cleanup`` apaga no fim dela, e o relatório sai no terminal e em
``probes/output/duckdb_threads_<data-hora>.txt``; o andamento sai no terminal, fora do relatório.

Seções:

1. As tabelas: a versão, as partições e, na partição medida, os arquivos, os bytes e as linhas do log.
2. A máquina: os núcleos, a memória, o disco da pasta temporária, o padrão do DuckDB e os valores medidos.
3. Uma tabela: ``materializada`` e ``agregada`` por valor de ``threads``.
4. Várias tabelas: ``em série`` e ``sessões a mais`` por valor de ``threads``.

Cada seção é uma função, na ordem acima, que documenta as checagens que emite (``DT-1`` a ``DT-6``);
``main`` as chama uma a uma. Códigos de saída: 0 quando toda checagem passou, 1 quando alguma
leitura falhou, 2 quando alguma checagem reprovou.
"""

from __future__ import annotations

import argparse
import dataclasses
import importlib
import importlib.metadata
import multiprocessing
import os
import re
import resource
import shutil
import sys
import tempfile
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from concurrent.futures.process import BrokenProcessPool
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import sqlalchemy as sa
from deltalake.exceptions import DeltaError

from serialize_db import delta
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.schema import quoted, table_options
from serialize_db.storage import Storage, prepare_environment

sys.path.insert(0, str(Path(__file__).resolve().parent))
from probelib import Report, tabulate  # noqa: E402

# As tabelas medidas por padrão: a grande primeiro, depois as que o pipeline mensal lê com ela na
# mesma partição.
DEFAULT_TABLES = ("cad_lancamentos", "cad_contratos", "cad_operacoes", "rel_contrato_operacao")
DEFAULT_METADATA = "client_model:Base.metadata"
DEFAULT_REPETITIONS = 3

# O padrão do DuckDB, um por núcleo, vezes cada um: a faixa de 2 a 5 vezes os núcleos que a
# documentação recomenda para arquivo remoto, ao lado do padrão.
MULTIPLIERS = (1, 2, 3, 4, 5)

# As medidas de cada seção, na ordem do relatório.
SINGLE_TABLE_SCENARIOS = ("materializada", "agregada")
MANY_TABLES_SCENARIOS = ("em série", "sessões a mais")

# O execution_id do motor de cada medição; a regra da partição o aceita.
EXECUTION_ID = "sonda-threads"

# As falhas de uma medição que entram no relatório sem parar o probe: o processo filho morto (a
# falta de memória, por exemplo), o erro do DuckDB, do delta-rs e do armazenamento.
MEASUREMENT_ERRORS = (BrokenProcessPool, duckdb.Error, DeltaError, OSError, MemoryError)


@dataclasses.dataclass(frozen=True)
class TableInput:
    """Uma tabela medida: o modelo, a URI da tabela Delta, a versão lida na abertura, a lista de
    partições da ingestão (``None`` numa tabela sem partição) e o que o log diz dessa leitura."""

    table: sa.Table
    uri: str
    version: int
    partitions: list[str] | None
    files: int
    bytes: int
    rows: int


@dataclasses.dataclass
class Measurement:
    """Uma configuração medida num processo novo: o cenário, as threads pedidas e as que o DuckDB
    aplicou, o tempo de cada repetição, a memória e as linhas lidas na primeira repetição."""

    scenario: str
    threads: int
    seconds: list[float] = dataclasses.field(default_factory=list)
    threads_read: int | None = None
    base_mb: float = 0.0
    peak_mb: float = 0.0
    rows: int = 0
    error: str = ""


# ---------------------------------------------------------------------------------------------------------------
# Funções puras: os valores medidos, a partição, os totais do log e a formatação


def thread_values(default_threads: int, requested: list[int] | None) -> list[int]:
    """Os valores de ``threads`` medidos, em ordem e sem repetição: os pedidos, ou o padrão do
    DuckDB vezes 1 a 5."""
    if requested:
        return sorted(set(requested))
    return [default_threads * multiplier for multiplier in MULTIPLIERS]


def latest_common_partition(values_by_table: dict[str, set[str]]) -> str | None:
    """A partição mais recente presente em todas as tabelas particionadas, ou ``None`` quando elas
    não têm partição em comum."""
    common: set[str] | None = None
    for values in values_by_table.values():
        if common is None:
            common = set(values)
        else:
            common = common & values
    if not common:
        return None
    return max(common)


def partition_totals(actions: pa.Table, column: str | None, value: str | None) -> tuple[int, int, int]:
    """Os arquivos, os bytes e as linhas das ações ``add`` da partição, ou de todas numa tabela sem
    partição; as linhas vêm do ``numRecords`` de cada ação."""
    if column is not None:
        actions = actions.filter(pc.equal(actions.column(f"partition.{column}"), value))
    total_bytes = pc.sum(actions.column("size_bytes")).as_py() or 0
    total_rows = pc.sum(actions.column("num_records")).as_py() or 0
    return actions.num_rows, total_bytes, total_rows


def short_error(error: str, limit: int = 100) -> str:
    """A primeira linha do erro, cortada em ``limit`` caracteres, para caber numa célula."""
    first_line = error.splitlines()[0] if error else ""
    if len(first_line) <= limit:
        return first_line
    return first_line[:limit - 3] + "..."


def best(seconds: list[float]) -> float | None:
    """O menor tempo das repetições, ou ``None`` sem repetição."""
    if not seconds:
        return None
    return min(seconds)


def speedup(reference: float | None, value: float | None) -> str:
    """Quantas vezes ``value`` é mais rápido que ``reference``, como ``1.80x``; ``-`` sem um dos
    dois."""
    if not reference or not value:
        return "-"
    return f"{reference / value:.2f}x"


def expected_rows(scenario: str, inputs: list[TableInput]) -> int:
    """As linhas que o log diz que o cenário lê: a primeira tabela nas medidas de uma tabela, a
    soma de todas nas de várias."""
    if scenario in SINGLE_TABLE_SCENARIOS:
        return inputs[0].rows
    return sum(item.rows for item in inputs)


def measurement_rows(measurements: list[Measurement], inputs: list[TableInput]) -> list[list[str]]:
    """As linhas da tabela de uma seção: por cenário e valor de ``threads``, as threads aplicadas,
    a melhor repetição, cada repetição, a razão sobre o primeiro valor do cenário, a memória e as
    linhas lidas contra as do log."""
    rows = [["CENÁRIO", "THREADS", "APLICADAS", "MELHOR S", "REPETIÇÕES S", "RAZÃO", "BASE MB", "PICO MB",
             "LINHAS LIDAS", "LINHAS DO LOG"]]
    reference: dict[str, float | None] = {}
    for measurement in measurements:
        fastest = best(measurement.seconds)
        # A razão compara com o primeiro valor medido do cenário, o padrão do DuckDB quando ele é medido.
        if measurement.scenario not in reference:
            reference[measurement.scenario] = fastest
        if measurement.error:
            rows.append([measurement.scenario, str(measurement.threads), "-", "-",
                         short_error(measurement.error), "-", "-", "-", "-",
                         str(expected_rows(measurement.scenario, inputs))])
            continue
        repetitions = ", ".join(f"{value:.3f}" for value in measurement.seconds)
        rows.append([
            measurement.scenario,
            str(measurement.threads),
            str(measurement.threads_read),
            f"{fastest:.3f}",
            repetitions,
            speedup(reference[measurement.scenario], fastest),
            f"{measurement.base_mb:.0f}",
            f"{measurement.peak_mb:.0f}",
            str(measurement.rows),
            str(expected_rows(measurement.scenario, inputs)),
        ])
    return rows


def fastest_configuration(measurements: list[Measurement], scenario: str) -> tuple[int, float] | None:
    """O valor de ``threads`` com a melhor repetição do cenário, e o tempo dela; ``None`` quando
    nenhuma configuração do cenário terminou."""
    found: tuple[int, float] | None = None
    for measurement in measurements:
        fastest = best(measurement.seconds)
        if measurement.scenario != scenario or measurement.error or fastest is None:
            continue
        if found is None or fastest < found[1]:
            found = (measurement.threads, fastest)
    return found


# ---------------------------------------------------------------------------------------------------------------
# A medição, num processo novo


def peak_rss_mb() -> float:
    """O pico de memória residente do próprio processo até agora, em MB.

    No Linux, o ``VmHWM`` de ``/proc/self/status``, em KB: o ``ru_maxrss`` de um processo novo
    começa no pico do processo pai. No macOS, o ``ru_maxrss``, em bytes.
    """
    if sys.platform == "darwin":
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / (1024 * 1024)
    status = Path("/proc/self/status").read_text()
    kilobytes = re.search(r"^VmHWM:\s+(\d+)", status, re.MULTILINE).group(1)
    return int(kilobytes) / 1024


def drop_tables(engine: DuckDBEngine, inputs: list[TableInput]) -> None:
    """Apaga as tabelas materializadas das entradas, para a repetição seguinte recriá-las."""
    with engine.session() as connection:
        for item in inputs:
            connection.execute(f"DROP TABLE {quoted(item.table.name)}")


def count_rows(engine: DuckDBEngine, inputs: list[TableInput]) -> int:
    """As linhas das tabelas materializadas das entradas, somadas."""
    total = 0
    with engine.session() as connection:
        for item in inputs:
            total += connection.execute(f"SELECT count(*) FROM {quoted(item.table.name)}").fetchone()[0]
    return total


def time_materialized(engine: DuckDBEngine, inputs: list[TableInput], repetitions: int) -> tuple[list[float], int]:
    """A primeira tabela materializada, apagada depois de cada repetição; as linhas saem da
    primeira."""
    item = inputs[0]
    seconds = []
    rows = 0
    for repetition in range(repetitions):
        started = time.perf_counter()
        engine.ingest(item.table, item.uri, item.version, item.partitions, materialize=True)
        seconds.append(time.perf_counter() - started)
        if repetition == 0:
            rows = count_rows(engine, [item])
        drop_tables(engine, [item])
    return seconds, rows


def time_aggregated(engine: DuckDBEngine, inputs: list[TableInput], repetitions: int) -> tuple[list[float], int]:
    """A primeira tabela pela view de ``ingest``, lida inteira por ``max(COLUMNS(*))``, sem gravar."""
    item = inputs[0]
    engine.ingest(item.table, item.uri, item.version, item.partitions)
    text = f"SELECT count(*), max(COLUMNS(*)) FROM {quoted(item.table.name)}"
    seconds = []
    rows = 0
    for _ in range(repetitions):
        started = time.perf_counter()
        with engine.session() as connection:
            result = connection.execute(text).fetchone()
        seconds.append(time.perf_counter() - started)
        rows = result[0]
    return seconds, rows


def time_in_series(engine: DuckDBEngine, inputs: list[TableInput], repetitions: int) -> tuple[list[float], int]:
    """As tabelas materializadas uma depois da outra na sessão principal."""
    seconds = []
    rows = 0
    for repetition in range(repetitions):
        started = time.perf_counter()
        for item in inputs:
            engine.ingest(item.table, item.uri, item.version, item.partitions, materialize=True)
        seconds.append(time.perf_counter() - started)
        if repetition == 0:
            rows = count_rows(engine, inputs)
        drop_tables(engine, inputs)
    return seconds, rows


def ingest_in_new_session(engine: DuckDBEngine, item: TableInput) -> None:
    """Uma tabela materializada numa sessão a mais, fechada no fim, como ``run.ingest`` faz."""
    with engine.new_session() as session:
        session.ingest(item.table, item.uri, item.version, item.partitions, materialize=True)


def time_in_new_sessions(engine: DuckDBEngine, inputs: list[TableInput], repetitions: int) -> tuple[list[float], int]:
    """Cada tabela numa sessão a mais, todas juntas num pool de uma thread por tabela, como
    ``run.ingest`` de várias tabelas."""
    seconds = []
    rows = 0
    for repetition in range(repetitions):
        started = time.perf_counter()
        with ThreadPoolExecutor(max_workers=len(inputs)) as pool:
            futures = [pool.submit(ingest_in_new_session, engine, item) for item in inputs]
            for future in futures:
                future.result()
        seconds.append(time.perf_counter() - started)
        if repetition == 0:
            rows = count_rows(engine, inputs)
        drop_tables(engine, inputs)
    return seconds, rows


# Cada cenário e a função que o mede.
SCENARIOS = {
    "materializada": time_materialized,
    "agregada": time_aggregated,
    "em série": time_in_series,
    "sessões a mais": time_in_new_sessions,
}


def measure(scenario: str, threads: int, inputs: list[TableInput], repetitions: int, root: str) -> Measurement:
    """Mede um cenário num motor novo com ``threads``; roda num processo novo, e a base é a memória
    dele depois das importações e da conexão."""
    engine = DuckDBEngine(DuckDBConfig(threads=threads), EXECUTION_ID, Storage.for_uri(root))
    try:
        base = peak_rss_mb()
        with engine.session() as connection:
            applied = connection.execute("SELECT current_setting('threads')").fetchone()[0]
        seconds, rows = SCENARIOS[scenario](engine, inputs, repetitions)
        return Measurement(scenario, threads, seconds, int(applied), base, peak_rss_mb(), rows)
    finally:
        engine.cleanup()


def run_measurement(scenario: str, threads: int, inputs: list[TableInput], repetitions: int, root: str) -> Measurement:
    """Roda ``measure`` num processo novo, pelo ``spawn``, para o pico de memória ser só da
    configuração; a configuração que falha entra com o erro."""
    context = multiprocessing.get_context("spawn")
    try:
        with ProcessPoolExecutor(max_workers=1, mp_context=context) as executor:
            return executor.submit(measure, scenario, threads, inputs, repetitions, root).result()
    except MEASUREMENT_ERRORS as error:
        return Measurement(scenario, threads, error=f"{type(error).__name__}: {error}")


def run_scenarios(scenarios: tuple[str, ...], values: list[int], inputs: list[TableInput], repetitions: int, root: str) -> list[Measurement]:
    """Cada cenário com cada valor de ``threads``, um processo por configuração; o andamento sai no
    terminal."""
    measurements = []
    for scenario in scenarios:
        for threads in values:
            print(f"medindo {scenario} com {threads} threads", file=sys.stderr)
            measurement = run_measurement(scenario, threads, inputs, repetitions, root)
            outcome = measurement.error or f"melhor {best(measurement.seconds):.3f} s"
            print(f"  {scenario}, {threads} threads: {outcome}", file=sys.stderr)
            measurements.append(measurement)
    return measurements


# ---------------------------------------------------------------------------------------------------------------
# As seções do relatório


def resolve_metadata(spec: str) -> sa.MetaData:
    """O ``MetaData`` de ``módulo:atributo``, como ``client_model:Base.metadata``."""
    module_name, _, attribute = spec.partition(":")
    if not module_name or not attribute:
        raise argparse.ArgumentTypeError(f"esperado módulo:atributo, recebido {spec!r}")
    target = importlib.import_module(module_name)
    for name in attribute.split("."):
        target = getattr(target, name)
    if not isinstance(target, sa.MetaData):
        raise argparse.ArgumentTypeError(f"{spec} não é um sqlalchemy.MetaData")
    return target


def read_log(storage: Storage, table: sa.Table) -> tuple[int, pa.Table]:
    """A versão atual da tabela Delta ``<raiz>/<tabela>`` e as ações ``add`` dela."""
    opened = delta.open_table(storage.uri_of(table.name), storage)
    return opened.version(), pa.table(opened.get_add_actions(flatten=True))


def tables_section(report: Report, storage: Storage, tables: list[sa.Table], requested: str | None) -> list[TableInput] | None:
    """Seção 1: cada tabela aberta pelo log, a versão, a coluna e a quantidade de partições, e os
    arquivos, os bytes e as linhas da partição medida; checagem ``DT-1``. Devolve as entradas das
    medições, ou ``None`` quando uma tabela ou a partição falta."""
    report.h1("As tabelas")
    logs: dict[str, tuple[int, pa.Table]] = {}
    for table in tables:
        read = report.call(f"ler o log de {storage.uri_of(table.name)}", lambda: read_log(storage, table),
                           render=lambda result: f"versão {result[0]}, {result[1].num_rows} arquivo(s)")
        if read is None:
            report.fail("DT-1", "tabelas e partição", f"{table.name} não abriu: {report.last_reason}")
            return None
        logs[table.name] = read

    # A partição medida: a pedida, ou a mais recente comum às tabelas particionadas.
    values_by_table: dict[str, set[str]] = {}
    for table in tables:
        column = table_options(table).partition_by
        if column is not None:
            actions = logs[table.name][1]
            values_by_table[table.name] = set(actions.column(f"partition.{column}").to_pylist())
    partition = requested or latest_common_partition(values_by_table)
    missing = [name for name, values in values_by_table.items() if partition not in values]
    if values_by_table and (partition is None or missing):
        report.fail("DT-1", "tabelas e partição", f"partição {partition} ausente em {', '.join(missing) or 'todas'}")
        return None
    report.value("PARTICAO", partition)

    # O que o log diz de cada leitura: é o que as linhas lidas nas medições devem repetir.
    inputs = []
    rows = [["TABELA", "VERSÃO", "COLUNA DE PARTIÇÃO", "PARTIÇÕES", "ARQUIVOS LIDOS", "MB LIDOS", "LINHAS LIDAS"]]
    for table in tables:
        version, actions = logs[table.name]
        column = table_options(table).partition_by
        value = partition if column is not None else None
        files, size, lines = partition_totals(actions, column, value)
        partitions = [partition] if column is not None else None
        inputs.append(TableInput(table, storage.uri_of(table.name), version, partitions, files, size, lines))
        count = len(values_by_table.get(table.name, ())) or "-"
        rows.append([table.name, str(version), column or "(sem partição)", str(count), str(files), f"{size / 2**20:.0f}", str(lines)])
    report.table(rows)
    report.ok("DT-1", "tabelas e partição", f"{len(tables)} tabela(s) na partição {partition}")
    return inputs


def machine_section(report: Report, requested: list[int] | None, repetitions: int) -> list[int]:
    """Seção 2: os núcleos, a memória, o disco da pasta temporária do motor, o padrão do DuckDB e
    os valores medidos; nenhuma checagem. Devolve os valores de ``threads``."""
    report.h1("A máquina e o DuckDB")
    connection = duckdb.connect()
    default_threads, memory_limit = connection.execute(
        "SELECT current_setting('threads'), current_setting('memory_limit')").fetchone()
    connection.close()
    values = thread_values(int(default_threads), requested)

    memory_mb = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES") / 2**20
    temporary = tempfile.gettempdir()
    free_gib = shutil.disk_usage(temporary).free / 2**30
    packages = ", ".join(f"{name} {importlib.metadata.version(name)}" for name in ("duckdb", "deltalake", "pyarrow"))
    report.table([
        ["LEITURA", "VALOR"],
        ["núcleos (os.cpu_count)", str(os.cpu_count())],
        ["memória", f"{memory_mb:.0f} MB"],
        ["pasta temporária do motor", f"{temporary}, {free_gib:.1f} GiB livres"],
        ["pacotes", packages],
        ["threads padrão do DuckDB", str(default_threads)],
        ["memory_limit padrão do DuckDB", str(memory_limit)],
        ["threads medidas", ", ".join(str(value) for value in values)],
        ["repetições por configuração", str(repetitions)],
    ])
    return values


def single_table_section(report: Report, values: list[int], inputs: list[TableInput], repetitions: int, root: str) -> list[Measurement]:
    """Seção 3: a partição da primeira tabela ``materializada`` e ``agregada`` com cada valor de
    ``threads``; checagem ``DT-5`` e as medições para ``DT-2`` a ``DT-4``."""
    report.h1(f"Uma tabela: {inputs[0].table.name}")
    report.line("materializada: ingest(..., materialize=True), o CREATE TABLE AS sobre delta_scan, apagada a cada repetição.")
    report.line("agregada: a view de ingest lida inteira por SELECT count(*), max(COLUMNS(*)), sem gravar.")
    report.line("RAZÃO: a melhor repetição do primeiro valor de threads dividida pela desta linha; acima de 1 é mais rápido.\n")
    measurements = run_scenarios(SINGLE_TABLE_SCENARIOS, values, inputs, repetitions, root)
    report.table(measurement_rows(measurements, inputs))

    # A leitura que decide o padrão: o valor de threads mais rápido de cada cenário contra o primeiro.
    readings = []
    for scenario in SINGLE_TABLE_SCENARIOS:
        found = fastest_configuration(measurements, scenario)
        first = fastest_configuration([item for item in measurements if item.threads == values[0]], scenario)
        if found is None or first is None:
            readings.append(f"{scenario}: sem medição completa")
            continue
        readings.append(f"{scenario}: melhor com threads={found[0]}, {found[1]:.3f} s, "
                        f"{speedup(first[1], found[1])} sobre threads={values[0]}")
    report.note("DT-5", "threads de uma tabela", "; ".join(readings))
    return measurements


def many_tables_section(report: Report, values: list[int], inputs: list[TableInput], repetitions: int, root: str) -> list[Measurement]:
    """Seção 4: as tabelas ``em série`` na sessão principal e em ``sessões a mais`` com cada valor
    de ``threads``; checagem ``DT-6`` e as medições para ``DT-2`` a ``DT-4``."""
    report.h1("Várias tabelas: " + ", ".join(item.table.name for item in inputs))
    report.line("em série: cada tabela materializada depois da outra na sessão principal.")
    report.line("sessões a mais: cada tabela materializada numa sessão a mais, todas juntas, como run.ingest.\n")
    measurements = run_scenarios(MANY_TABLES_SCENARIOS, values, inputs, repetitions, root)
    report.table(measurement_rows(measurements, inputs))

    # A leitura das sessões a mais: com cada valor de threads, o tempo em série sobre o das sessões.
    readings = []
    for threads in values:
        same = [item for item in measurements if item.threads == threads]
        series = fastest_configuration(same, "em série")
        sessions = fastest_configuration(same, "sessões a mais")
        if series is None or sessions is None:
            readings.append(f"threads={threads}: sem medição completa")
            continue
        readings.append(f"threads={threads}: {speedup(series[1], sessions[1])}")
    report.note("DT-6", "sessões a mais sobre em série",
                "o tempo em série dividido pelo das sessões a mais; " + "; ".join(readings))
    return measurements


def measurement_checks(report: Report, measurements: list[Measurement], inputs: list[TableInput]) -> None:
    """As checagens de todas as medições: ``DT-2`` as linhas lidas contra as do log, ``DT-3`` as
    threads que o DuckDB aplicou, ``DT-4`` as configurações que falharam."""
    finished = [item for item in measurements if not item.error]

    # DT-2: a ingestão leu as linhas que o log diz ter; uma diferença é leitura perdida ou a mais.
    wrong_rows = [f"{item.scenario}/{item.threads}: {item.rows} contra {expected_rows(item.scenario, inputs)}"
                  for item in finished if item.rows != expected_rows(item.scenario, inputs)]
    if wrong_rows:
        report.fail("DT-2", "linhas lidas", "; ".join(wrong_rows))
    else:
        report.ok("DT-2", "linhas lidas", f"{len(finished)} configuração(ões) leram as linhas do log")

    # DT-3: o motor abriu a conexão com o valor pedido; outro valor invalida a linha da tabela.
    wrong_threads = [f"{item.scenario}/{item.threads}: {item.threads_read}" for item in finished if item.threads_read != item.threads]
    if wrong_threads:
        report.fail("DT-3", "threads aplicadas", "; ".join(wrong_threads))
    else:
        report.ok("DT-3", "threads aplicadas", "o DuckDB aplicou cada valor pedido")

    # DT-4: uma configuração que falhou, como a falta de memória com muitas threads, é leitura; ela
    # também vai para a seção final, porque a linha dela falta na tabela.
    failed = [item for item in measurements if item.error]
    for item in failed:
        report.failures.append((f"{item.scenario} com threads={item.threads}", item.error))
    if failed:
        details = [f"{item.scenario}/{item.threads}: {short_error(item.error)}" for item in failed]
        report.note("DT-4", "configurações medidas", "; ".join(details))
    else:
        report.ok("DT-4", "configurações medidas", f"{len(measurements)} configuração(ões) terminaram")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Mede a ingestão das tabelas Delta pelo motor DuckDB com cada valor de threads, "
        "numa tabela e em várias, em série e em sessões a mais.")
    parser.add_argument("root", help="a pasta das tabelas Delta, local ou s3://bucket/prefixo: o --root da migração")
    parser.add_argument("--metadata", default=DEFAULT_METADATA, help=f"o MetaData do modelo, {DEFAULT_METADATA} por padrão")
    parser.add_argument("--tables", nargs="+", default=list(DEFAULT_TABLES), metavar="TABELA", help="as tabelas medidas, a primeira a grande")
    parser.add_argument("--partition", metavar="AAAA-MM-DD", help="a partição lida; por padrão a mais recente comum às tabelas")
    parser.add_argument("--threads", nargs="+", type=int, metavar="N", help="os valores medidos; por padrão o padrão do DuckDB vezes 1 a 5")
    parser.add_argument("--repetitions", type=int, default=DEFAULT_REPETITIONS, help=f"repetições por configuração, {DEFAULT_REPETITIONS} por padrão")
    return parser


def main(argv: list[str]) -> int:
    parser = build_parser()
    arguments = parser.parse_args(argv[1:])
    metadata = resolve_metadata(arguments.metadata)
    unknown = [name for name in arguments.tables if name not in metadata.tables]
    if unknown:
        parser.error(f"tabelas fora do modelo: {', '.join(unknown)}")
    tables = [metadata.tables[name] for name in arguments.tables]

    # As variáveis que o delta-rs e o botocore leem, acertadas como a biblioteca acerta na abertura.
    changed = prepare_environment()
    storage = Storage.for_uri(arguments.root)
    report = Report("duckdb_threads", f"o threads do DuckDB na ingestão das tabelas Delta em {storage.uri}")
    report.line("Só leitura sob a raiz: nada é criado, alterado ou apagado nela; o banco de cada configuração fica na pasta temporária do motor e sai no cleanup.")
    if changed:
        report.line(f"variáveis acertadas por prepare_environment: {', '.join(sorted(changed))}")

    inputs = tables_section(report, storage, tables, arguments.partition)
    if inputs is None:
        return report.finish()
    values = machine_section(report, arguments.threads, arguments.repetitions)
    measurements = single_table_section(report, values, inputs, arguments.repetitions, storage.uri)
    measurements += many_tables_section(report, values, inputs, arguments.repetitions, storage.uri)
    measurement_checks(report, measurements, inputs)
    return report.finish()


if __name__ == "__main__":
    sys.exit(main(sys.argv))
