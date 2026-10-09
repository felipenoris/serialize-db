"""Sonda do ``compact`` numa partição de vários arquivos e da memória dele.

A sonda mede a compactação de uma partição com mais de um arquivo, que a carga não grava, e a
memória do ``optimize.compact`` do delta-rs, que roda fora do ``memory_limit`` do DuckDB; a leitura
do ambiente alvo de 2026-10-05 está em ``docs/operacao.md``, seção "Compactação". Ela carrega a
primeira partição de ``cad_lancamentos`` na origem, reparte o arquivo dela pelo
``COPY ... FILE_SIZE_BYTES`` do DuckDB em arquivos de até 1/32 do tamanho dela, como o ``UNLOAD``
em paralelo do Redshift fragmenta por slice, registra os arquivos no lugar dele e roda
``serialize-db compact`` na partição. No ambiente alvo, os 551,5 MB da partição saíram em 64
arquivos de 5,3 a 16,7 MB com 8 threads do DuckDB (2026-10-05), em 40 de 11,6 a 16,7 MB com 2
(2026-10-07) e em 50 de 5,3 a 19,5 MB com 4 (2026-10-09). ``--ignore-partitions`` deixa partições
da origem fora das primeiras que a sonda toma, como o do ``serialize-db import``.

Checagens:

- a partição repartida em mais de um arquivo, todos abaixo do tamanho alvo de 100 MB;
- o ``compact`` sai com 0 e junta arquivos;
- a partição com menos arquivos depois dele, e as mesmas linhas e soma de ``id_lancamento`` pelo
  ``delta_scan``.

Leituras: o tempo e o pico de RSS que ``compact`` imprime, a memória disponível antes dele, os
tamanhos dos arquivos antes e depois e a operação do commit em ``history``.

Numa partição acima de cerca de 1,6 GB, os arquivos de 1/32 do tamanho dela passam de 50 MB, a
metade do tamanho alvo, e não cabem dois a dois nele.

Exemplo:

.. code-block:: shell

    export SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo
    .venv/bin/python probes/operacao/probe_compact_memory.py s3://bucket/origem/db_projetado
"""

from __future__ import annotations

import re
import uuid

import operation_lib as lib
import pyarrow as pa

from serialize_db import delta
from serialize_db.execution import Database
from serialize_db.resources import available_memory, environment_limits
from serialize_db.schema import literal, quoted

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
    options = lib.parse_arguments(__doc__.splitlines()[0])
    source = options.source
    db = lib.work_database("compact")
    storage = db.storage
    value = lib.source_partitions(source, 1, options.ignore_partitions)[0]
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
