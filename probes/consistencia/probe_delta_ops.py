"""As rotinas Delta sobre partições de três escritores (``publish_partition`` pelo delta-rs, o
``export_partition`` do motor DuckDB e dois ``write_deltalake(mode="append")`` numa partição de
dois arquivos): ``compact``, ``deep_copy`` e a sua retomada, ``export_snapshot`` por cópia e por
reescrita, ``rewrite`` com uma coluna renomeada, ``vacuum_keeping_snapshots`` prendendo um
snapshot, a restauração de ``read_back`` e a escrita condicional do arquivo de controle por oito
threads, esta como leitura do achado conhecido.

.. code-block:: shell

    SERIALIZE_DB_TEST_LOCAL_ROOT=$HOME/serialize-db-local \\
        .venv/bin/python probes/consistencia/probe_delta_ops.py
"""
from __future__ import annotations

import json
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
import sqlalchemy as sa
from deltalake import write_deltalake

from consistency_lib import (TUDO, compare, edge_rows, finish, known_zero_sign, print_notes,
                             probe_folder, report, same, to_contract)
from serialize_db import delta, schema
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.errors import ConflictError, RegistrationRefused
from serialize_db.storage import Storage

MONTHS = ["2026-06-30", "2026-07-31", "2026-08-31"]
NOTES: set[str] = set()


def report_known(title: str, problems: list[str]) -> None:
    """A checagem sem a diferença conhecida do sinal do zero, impressa como leitura."""
    rest, known = known_zero_sign(problems)
    report(title, rest)
    if known:
        print(f"   diferenças conhecidas do sinal do zero: {len(known)}; {known[0]}")


def read_delta_scan(storage: Storage, uri: str, version: int | None = None) -> pa.Table:
    """A tabela inteira pelo ``delta_scan`` do DuckDB, na versão dada ou na atual."""
    connection = storage.duckdb_connect()
    try:
        clause = f"delta_scan('{uri}')"
        if version is not None:
            clause = f"delta_scan('{uri}', version := {version})"
        return connection.execute(f"SELECT * FROM {clause}").to_arrow_table()
    finally:
        connection.close()


def read_arrow(storage: Storage, uri: str, version: int | None = None) -> pa.Table:
    """A tabela inteira pelo dataset do delta-rs, na versão dada ou na atual."""
    return delta.open_table(uri, storage, version).to_pyarrow_table()


def check_all(storage: Storage, label: str, uri: str, expected_by_month: dict[str, pa.Table],
              version: int | None = None) -> list[str]:
    """Os dois leitores contra as partições esperadas."""
    problems = []
    readers = (("dataset", read_arrow), ("delta_scan", read_delta_scan))
    for reader_name, reader in readers:
        found = reader(storage, uri, version)
        for month, expected in expected_by_month.items():
            part = found.filter(pc.field("data_str") == month)
            problems += compare(expected, to_contract(part, TUDO, NOTES), key="id",
                                label=f"{label} {reader_name} {month}")
        total = sum(t.num_rows for t in expected_by_month.values())
        if found.num_rows != total:
            problems.append(f"{label} {reader_name}: {found.num_rows} linhas, esperadas {total}")
    return problems


def three_writers(storage: Storage, uri: str, folder: Path) -> dict[str, pa.Table]:
    """Seção W: três escritores numa tabela, uma partição cada, lidos iguais pelos dois
    leitores; devolve as partições esperadas."""
    expected: dict[str, pa.Table] = {}
    expected[MONTHS[0]] = edge_rows(MONTHS[0], 1, 3000)
    delta.publish_partition(uri, TUDO, MONTHS[0], expected[MONTHS[0]], {}, storage)
    expected[MONTHS[1]] = edge_rows(MONTHS[1], 3001, 3000)
    config = DuckDBConfig(temp_directory=str(folder / "sandbox"))
    with DuckDBEngine(config, "exec-delta", storage) as engine:
        engine.load(TUDO, expected[MONTHS[1]])
        engine.export_partition(TUDO, uri, MONTHS[1], {}, expected_rows=3000,
                                columns_without_min_max=["valor"])
    first = edge_rows(MONTHS[2], 6001, 1500)
    second = edge_rows(MONTHS[2], 7501, 1500)
    write_deltalake(uri, first, mode="append")
    write_deltalake(uri, second, mode="append")
    expected[MONTHS[2]] = pa.concat_tables([first, second])
    table = delta.open_table(uri, storage)
    actions = pa.table(table.get_add_actions(flatten=True))
    files_by_month = {}
    for month in expected:
        files_by_month[month] = actions.filter(pc.field("partition.data_str") == month).num_rows
    print(f"   três escritores: versão {table.version()}, arquivos por partição "
          f"{files_by_month}")
    report_known("W três escritores lidos iguais pelos dois leitores",
                 check_all(storage, "escritores", uri, expected))
    return expected


def below(a: object, b: object) -> bool:
    """Se ``a`` vem antes de ``b``, comparando textos como textos."""
    if isinstance(b, str):
        return str(a) < str(b)
    return a < b


def check_compact(storage: Storage, uri: str, expected: dict[str, pa.Table],
                  version_before: int) -> int:
    """Seção K: ``compact`` da partição de dois arquivos: as métricas, ``version_diff`` sem a
    compactação, os dados iguais e as estatísticas do arquivo novo limitando os dados nas
    colunas exatas; devolve a versão depois."""
    metrics = delta.compact(uri, TUDO, [MONTHS[2]], storage)
    after = delta.open_table(uri, storage).version()
    problems = []
    if metrics.get("numFilesRemoved") != 2 or metrics.get("numFilesAdded") != 1:
        problems.append(f"métricas do compact {metrics}")
    changed = delta.version_diff(uri, version_before, after, TUDO, storage)
    if changed:
        problems.append(f"version_diff conta a compactação: {changed}")
    problems += known_zero_sign(check_all(storage, "compact", uri, expected))[0]
    actions = pa.table(delta.open_table(uri, storage).get_add_actions(flatten=True))
    compacted = actions.filter(pc.field("partition.data_str") == MONTHS[2]).to_pylist()[0]
    for name in ("id", "inteiro", "texto", "data"):
        column = expected[MONTHS[2]].column(name)
        low, high = pc.min(column).as_py(), pc.max(column).as_py()
        logged_low, logged_high = compacted.get(f"min.{name}"), compacted.get(f"max.{name}")
        if logged_low is None or logged_high is None:
            problems.append(f"compact: {name} sem mínimo e máximo no log")
            continue
        if below(low, logged_low):
            problems.append(f"compact: min.{name} {logged_low} acima dos dados {low}")
        if below(logged_high, high):
            problems.append(f"compact: max.{name} {logged_high} abaixo dos dados {high}")
    print(f"   estatísticas de valor no arquivo compactado: mínimo {compacted.get('min.valor')}, "
          f"máximo {compacted.get('max.valor')}, nulos {compacted.get('null_count.valor')} "
          f"(os dados têm NaN, inf e -inf)")
    report("K compact da partição de dois arquivos", problems)
    return after


def check_deep_copy(storage: Storage, uri: str, copy_uri: str, version: int,
                    expected: dict[str, pa.Table]) -> None:
    """Seção D: ``deep_copy`` igual à origem pelos dois leitores, com as estatísticas do log
    iguais arquivo a arquivo, e a repetição sem commit novo."""
    copy_version = delta.deep_copy(uri, version, copy_uri, storage)
    problems = known_zero_sign(check_all(storage, "cópia", copy_uri, expected))[0]
    again = delta.deep_copy(uri, version, copy_uri, storage)
    if again != copy_version:
        problems.append(f"a retomada do deep_copy mudou a versão: {copy_version} -> {again}")
    source_actions = pa.table(delta.open_table(uri, storage).get_add_actions(flatten=True))
    copy_actions = pa.table(delta.open_table(copy_uri, storage).get_add_actions(flatten=True))
    by_path_source = {a["path"]: a for a in source_actions.to_pylist()}
    by_path_copy = {a["path"]: a for a in copy_actions.to_pylist()}
    if set(by_path_source) != set(by_path_copy):
        problems.append(f"caminhos diferentes: {set(by_path_source) ^ set(by_path_copy)}")
    dropped = set()
    for path, action in by_path_source.items():
        other = by_path_copy.get(path, {})
        for key, value in action.items():
            if key == "modification_time":
                continue
            copied = other.get(key)
            # A cópia registra o mínimo e o máximo só das colunas de tipo exato, a regra de
            # register_files: a estatística que ela deixa de fora é leitura.
            if copied is None and value is not None and key.startswith(("min.", "max.")):
                dropped.add(key)
                continue
            if not same(value, copied):
                problems.append(f"deep_copy {path}: {key} {value!r} -> {copied!r}")
    print(f"   estatísticas da origem que a cópia não registra: {sorted(dropped)}")
    report("D deep_copy igual à origem com as estatísticas; a repetição sem commit", problems)


def read_export(folder: Path) -> pa.Table:
    """Todo arquivo Parquet sob as pastas ``<coluna>=<valor>/`` da exportação, com a coluna de
    partição tomada do caminho quando o arquivo não a traz, cada um levado ao contrato porque
    guarda o esquema do seu escritor."""
    parts = []
    for file in sorted(folder.rglob("*.parquet")):
        table = pq.read_table(file)
        value = file.parent.name.split("=", 1)[1]
        if "data_str" not in table.column_names:
            partition = pa.array([value] * table.num_rows, pa.string())
            table = table.append_column("data_str", partition)
        parts.append(to_contract(table, TUDO, NOTES))
    return pa.concat_tables(parts)


def check_export_snapshot(storage: Storage, uri: str, folder: Path,
                          expected: dict[str, pa.Table]) -> None:
    """Seção E: ``export_snapshot`` por cópia e por reescrita, os arquivos lidos direto."""
    problems = []
    for mode in ("copy", "rewrite"):
        destination = storage.uri_of(storage.join("prd", "exportacao", mode))
        files = delta.export_snapshot(uri, TUDO, destination, storage, mode=mode)
        found = read_export(folder / "delta" / "prd" / "exportacao" / mode)
        for month, expected_part in expected.items():
            part = found.filter(pc.field("data_str") == month)
            problems += known_zero_sign(compare(expected_part, part, key="id",
                                                label=f"export {mode} {month}"))[0]
        print(f"   export {mode}: {len(files)} arquivos")
    report("E export_snapshot por cópia e por reescrita", problems)


def renamed_table() -> sa.Table:
    """``cad_tudo`` com ``texto`` renomeada para ``texto2``, sem a restrição única."""
    columns = []
    for column in TUDO.columns:
        if column.name == "texto":
            columns.append(sa.Column("texto2", column.type, nullable=column.nullable))
        else:
            columns.append(sa.Column(column.name, column.type, primary_key=column.primary_key,
                                     nullable=column.nullable))
    return sa.Table("cad_tudo", sa.MetaData(), *columns, info=dict(TUDO.info))


def check_rewrite(storage: Storage, uri: str, expected: dict[str, pa.Table]) -> int:
    """Seção R: ``rewrite`` com a coluna renomeada, igual pelos dois leitores, e a versão
    anterior ainda legível com o esquema antigo; devolve a versão antes da reescrita."""
    renamed = renamed_table()
    version_before = delta.open_table(uri, storage).version()
    rewritten = delta.rewrite(uri, renamed, storage, expressions={"texto2": '"texto"'})
    expected_renamed = {}
    for month, table in expected.items():
        names = ["texto2" if name == "texto" else name for name in table.column_names]
        expected_renamed[month] = table.rename_columns(names)
    problems = []
    readers = (("dataset", read_arrow), ("delta_scan", read_delta_scan))
    for reader_name, reader in readers:
        found = reader(storage, uri)
        for month, expected_part in expected_renamed.items():
            part = found.filter(pc.field("data_str") == month).select(expected_part.column_names)
            part = schema.cast(part, renamed)
            problems += known_zero_sign(compare(expected_part, part, key="id",
                                                label=f"rewrite {reader_name} {month}"))[0]
    old = read_arrow(storage, uri, version_before)
    if "texto" not in old.column_names or old.num_rows != 9000:
        problems.append(f"rewrite: a versão antiga com {old.column_names}, {old.num_rows} linhas")
    changed = delta.version_diff(uri, version_before, rewritten, renamed, storage)
    if changed != set(expected):
        problems.append(f"rewrite: version_diff {changed}")
    report("R rewrite com uma coluna renomeada; a versão anterior legível", problems)
    return version_before


def check_vacuum(storage: Storage, uri: str, expected: dict[str, pa.Table],
                 kept_version: int) -> None:
    """Seção V: ``vacuum_keeping_snapshots`` com retenção zero apaga só os arquivos fora do
    snapshot, e a versão presa segue legível."""
    problems = []
    delta.snapshot(storage, "prd", "s1", {TUDO.name: kept_version})
    control, _ = delta.read_snapshots(storage, "prd")
    listed = delta.vacuum_keeping_snapshots(uri, control, TUDO.name, storage, retention_hours=0)
    kept_table = delta.open_table(uri, storage, kept_version)
    kept_paths = set(pa.table(kept_table.get_add_actions(flatten=True)).column("path").to_pylist())
    overlap = kept_paths & set(listed)
    if overlap:
        problems.append(f"o vacuum apagaria arquivos da versão do snapshot: {sorted(overlap)[:3]}")
    deleted = delta.vacuum_keeping_snapshots(uri, control, TUDO.name, storage, retention_hours=0,
                                             apply=True)
    print(f"   vacuum apagou {len(deleted)} arquivos (os dois da partição compactada)")
    problems += known_zero_sign(check_all(storage, "depois do vacuum", uri, expected,
                                          kept_version))[0]
    without = delta.vacuum_keeping_snapshots(uri, {"snapshots": {}}, TUDO.name, storage,
                                             retention_hours=0)
    if not (set(without) & kept_paths):
        problems.append("sem o snapshot, o vacuum não lista nenhum arquivo da versão antiga")
    report("V vacuum prendendo a versão do snapshot", problems)


def check_read_back(storage: Storage, copy_uri: str) -> None:
    """Seção B: ``read_back`` devolve ``None`` na contagem certa e, na errada, recusa e restaura
    a versão anterior."""
    problems = []
    before = delta.open_table(copy_uri, storage).version()
    try:
        delta.read_back(copy_uri, TUDO, MONTHS[0], 3000, storage)
    except RegistrationRefused as error:
        problems.append(f"read_back recusou a contagem certa: {error}")
    if delta.open_table(copy_uri, storage).version() != before:
        problems.append("read_back com a contagem certa mudou a versão")
    try:
        delta.read_back(copy_uri, TUDO, MONTHS[0], 2999, storage)
        problems.append("read_back aceitou a contagem errada")
    except RegistrationRefused as error:
        print(f"   read_back recusou como esperado: {str(error)[:120]}...")
    restored = delta.open_table(copy_uri, storage)
    if restored.version() != before + 1:
        problems.append(f"versão restaurada {restored.version()}, esperada {before + 1}")
    print(f"   a cópia depois da restauração: {restored.to_pyarrow_table().num_rows} linhas "
          f"(o último commit de partição desfeito)")
    report("B read_back devolve None na contagem certa e restaura na errada", problems)


def check_conditional_writes(storage: Storage) -> None:
    """Seção C: oito threads somando 50 cada no arquivo de controle por ``read_text`` e
    ``write_text(if_match=...)``, com nova tentativa no ``ConflictError``; as atualizações
    perdidas na pasta local são o achado conhecido de ``plan/OPEN_QUESTIONS.md``, leitura."""
    path = storage.join("prd", "_serialize_db", "contador.json")
    storage.create_text(path, json.dumps({"n": 0}))
    conflicts = 0
    lock = threading.Lock()

    def increment(index: int) -> None:
        nonlocal conflicts
        for _ in range(50):
            while True:
                text, fingerprint = storage.read_text(path)
                value = json.loads(text)
                value["n"] += 1
                try:
                    storage.write_text(path, json.dumps(value), if_match=fingerprint)
                    break
                except ConflictError:
                    with lock:
                        conflicts += 1

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(increment, range(8)))
    final = json.loads(storage.read_text(path)[0])["n"]
    print(f"   escritas condicionais: final {final} de 400, conflitos vistos {conflicts}")
    if final != 400:
        print(f"   achado conhecido: {400 - final} atualizações perdidas na pasta local")
    problems = [] if 0 < final <= 400 else [f"contador final {final}"]
    report("C a escrita condicional do arquivo de controle por oito threads (leitura)", problems)


def main() -> None:
    folder = probe_folder("delta")
    storage = Storage.for_uri(str(folder / "delta"))
    uri = storage.uri_of(storage.join("prd", TUDO.name))
    delta.create_table(uri, TUDO, storage)
    expected = three_writers(storage, uri, folder)
    version_three_writers = delta.open_table(uri, storage).version()
    after_compact = check_compact(storage, uri, expected, version_three_writers)
    copy_uri = storage.uri_of(storage.join("prd", "arquivo", "s1", TUDO.name))
    check_deep_copy(storage, uri, copy_uri, after_compact, expected)
    check_export_snapshot(storage, uri, folder, expected)
    version_before_rewrite = check_rewrite(storage, uri, expected)
    check_vacuum(storage, uri, expected, version_before_rewrite)
    check_read_back(storage, copy_uri)
    check_conditional_writes(storage)
    print_notes(NOTES)
    finish(folder)


if __name__ == "__main__":
    main()
