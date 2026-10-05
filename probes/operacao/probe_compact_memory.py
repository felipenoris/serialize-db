"""Sonda do ``compact`` numa partição de vários arquivos e da memória dele.

``.claude/memory/OPEN_QUESTIONS.md`` ("A memória da compactação" e "A operação no ambiente alvo")
espera a compactação de uma partição com mais de um arquivo, que a carga não grava, e a memória do
``optimize.compact`` do delta-rs, que roda fora do ``memory_limit`` do DuckDB. A sonda carrega a
primeira partição de ``cad_lancamentos`` na origem, reparte o arquivo dela em cerca de 32 arquivos
pelo ``COPY ... FILE_SIZE_BYTES`` do DuckDB, como o ``UNLOAD`` em paralelo do Redshift fragmenta
por slice, registra os arquivos no lugar dele e roda ``serialize-db compact`` na partição.

Checagens:

- a partição repartida em mais de um arquivo, todos abaixo do tamanho alvo de 100 MB;
- o ``compact`` sai com 0 e junta arquivos;
- a partição com menos arquivos depois dele, e as mesmas linhas e soma de ``id_lancamento`` pelo
  ``delta_scan``.

Leituras: o tempo e o pico de RSS que ``compact`` imprime, a memória disponível antes dele, os
tamanhos dos arquivos antes e depois e a operação do commit em ``history``.

Numa partição acima de cerca de 1,6 GB, cada um dos 32 arquivos passa de 50 MB, a metade do
tamanho alvo, nenhum par cabe junto nele, e o ``compact`` não grava nada.

Exemplo:

.. code-block:: shell

    export SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo
    .venv/bin/python probes/operacao/probe_compact_memory.py s3://bucket/origem/db_projetado
"""

from __future__ import annotations

import re
import sys
import uuid

import operation_lib as lib
import pyarrow as pa

from serialize_db import delta
from serialize_db.execution import Database
from serialize_db.resources import available_memory, environment_limits
from serialize_db.schema import literal, quoted

USAGE = "uso: .venv/bin/python probes/operacao/probe_compact_memory.py <origem>"

# Os arquivos da partição repartida, a fragmentação por slice de um UNLOAD em paralelo (32
# arquivos para 500.000 linhas no ambiente alvo em 2026-09-21), e o tamanho alvo do delta-rs sem
# a propriedade delta.targetFileSize.
PARTS = 32
TARGET_BYTES = 100 * 2**20


def logged_sizes(
    uri: str,
    db: Database,
    value: str,
) -> list[int]:
    """Os tamanhos dos arquivos que a versão atual registra na partição, em bytes."""
    actions = pa.table(delta.open_table(uri, db.storage).get_add_actions(flatten=True))
    sizes = []
    values = actions.column(f"partition.{lib.PARTITION_BY}").to_pylist()
    for size, partition in zip(actions.column("size_bytes").to_pylist(), values):
        if partition == value:
            sizes.append(size)
    return sizes


def describe_sizes(
    sizes: list[int],
) -> str:
    """Os arquivos, o total, o menor e o maior, em MB."""
    if not sizes:
        return "nenhum arquivo"
    return (
        f"{len(sizes)} arquivo(s), {sum(sizes) / 2**20:.1f} MB, de {min(sizes) / 2**20:.1f} "
        f"a {max(sizes) / 2**20:.1f} MB"
    )


def split_partition(
    db: Database,
    uri: str,
    value: str,
) -> list[delta.RegisteredFile]:
    """Reparte a partição em cerca de ``PARTS`` arquivos pelo ``COPY ... FILE_SIZE_BYTES`` do
    DuckDB, numa pasta nova dentro dela, e os registra no lugar do arquivo da carga."""
    storage = db.storage
    part_bytes = max(sum(logged_sizes(uri, db, value)) // PARTS, 1)
    folder = f"{uri}/{lib.PARTITION_BY}={value}/partes-{uuid.uuid4().hex[:8]}"
    names = []
    for column in lib.TABLE.columns:
        if column.name != lib.PARTITION_BY:
            names.append(quoted(column.name))
    select = (
        f"SELECT {', '.join(names)} FROM delta_scan({literal(uri)}) "
        f"WHERE {quoted(lib.PARTITION_BY)} = {literal(value)}"
    )
    copy = (
        f"COPY ({select}) TO {literal(folder)} "
        f"(FORMAT parquet, FILE_SIZE_BYTES {part_bytes}, RETURN_STATS)"
    )
    # Uma conexão com os limites do ambiente, fechada antes do compact: o close devolve a memória.
    connection = storage.duckdb_connect(config=environment_limits())
    try:
        cursor = connection.execute(copy)
        columns = [column[0] for column in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    finally:
        connection.close()
    files = [delta.file_from_return_stats(row, lib.TABLE, uri) for row in rows]
    expected = sum(file.rows for file in files)
    metadata = delta.commit_metadata(f"operacao-{uuid.uuid4().hex[:8]}", {})
    delta.register_files(uri, lib.TABLE, files, value, metadata, storage, expected_rows=expected)
    return files


def check_compact(
    compact: lib.Finished,
) -> None:
    """O ``compact`` saiu com 0 e juntou arquivos."""
    problems = []
    if compact.code != 0:
        problems.append(f"saída {compact.code}")
    pattern = re.compile(r": (\d+) arquivo\(s\) gravado\(s\), (\d+) removido\(s\)")
    removed = None
    for line in compact.lines:
        match = pattern.search(line)
        if match is not None:
            removed = int(match.group(2))
    if removed is None:
        problems.append("sem a linha dos arquivos gravados e removidos")
    elif removed < 2:
        problems.append(f"{removed} arquivo(s) removido(s)")
    lib.check("o compact junta os arquivos da partição", problems)


def main() -> None:
    """O ``compact`` da partição repartida, com o relatório no terminal e em
    ``probes/output/``."""
    if len(sys.argv) != 2:
        print(USAGE, file=sys.stderr)
        sys.exit(2)
    source = sys.argv[1]
    db = lib.work_database("compact")
    storage = db.storage
    value = lib.source_partitions(source, 1)[0]
    lib.load_partitions(db, source, [value])
    uri = lib.table_uri(db, lib.TABLE.name)
    totals = lib.delta_totals(storage, uri)
    print(f"carregada: {describe_sizes(logged_sizes(uri, db, value))}; {totals[0]} linhas")

    # A partição repartida no lugar do arquivo da carga.
    files = split_partition(db, uri, value)
    split_sizes = logged_sizes(uri, db, value)
    print(f"repartida: {describe_sizes(split_sizes)}")
    problems = []
    if len(files) < 2 or len(split_sizes) != len(files):
        problems.append(f"{len(files)} arquivo(s) gravado(s), {len(split_sizes)} no log")
    for size in split_sizes:
        if size >= TARGET_BYTES:
            problems.append(f"um arquivo de {size / 2**20:.1f} MB, acima do tamanho alvo")
    lib.check("a partição repartida em arquivos abaixo do tamanho alvo", problems)

    # O compact num processo próprio, que imprime o tempo e o pico de RSS dele.
    print(f"memória disponível antes do compact: {available_memory() / 2**30:.1f} GiB")
    compact = lib.run_cli(
        lib.cli_arguments(db, "compact", "--table", lib.TABLE.name, "--partitions", value)
    )
    check_compact(compact)

    compacted_sizes = logged_sizes(uri, db, value)
    print(f"compactada: {describe_sizes(compacted_sizes)}")
    latest = delta.history(uri, storage)[0]
    print(f"último commit: versão {latest['version']}, {latest['operation']}")
    problems = []
    if len(compacted_sizes) >= len(split_sizes):
        problems.append(f"{len(compacted_sizes)} arquivo(s), antes {len(split_sizes)}")
    after = lib.delta_totals(storage, uri)
    if after != totals:
        problems.append(f"linhas e soma {after}, antes {totals}")
    lib.check("a partição com menos arquivos e as mesmas linhas e soma", problems)
    lib.finish(db)


if __name__ == "__main__":
    lib.run(main)
