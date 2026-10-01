"""Sonda do ``vacuum --full`` sobre os arquivos fora do log.

``plan/OPEN_QUESTIONS.md`` ("A operação no ambiente alvo") espera a leitura no alvo do
``vacuum --full`` de um arquivo que nenhuma versão do log referencia, como o da carga parada de
2026-09-28. A sonda carrega a primeira partição de ``cad_lancamentos`` na origem e copia o arquivo
dela para dois órfãos: um na pasta da partição, como o do ``COPY`` da carga, e outro num prefixo
dentro dela, como o do ``UNLOAD`` da exportação Redshift. Depois roda ``serialize-db vacuum
--full`` com a retenção padrão, com ``--retention-hours 0`` e com ``--apply``.

Checagens:

- com a retenção padrão de 9600 horas, nada listado: os órfãos são recentes;
- com ``--retention-hours 0``, os dois órfãos listados, e só eles;
- com ``--apply``, os dois apagados, o arquivo do log mantido e as mesmas linhas e soma de
  ``id_lancamento`` pelo ``delta_scan``.

Leituras: a versão da tabela antes e depois de cada ``vacuum``.

Exemplo:

.. code-block:: shell

    export SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo
    .venv/bin/python probes/operacao/probe_vacuum_orphans.py s3://bucket/origem/db_projetado
"""

from __future__ import annotations

import sys
import uuid

import operation_lib as lib

from serialize_db import delta
from serialize_db.execution import Database

USAGE = "uso: .venv/bin/python probes/operacao/probe_vacuum_orphans.py <origem>"


def listed_paths(
    finished: lib.Finished,
) -> list[str]:
    """Os arquivos que ``serialize-db vacuum`` imprimiu para a tabela, um por linha recuada
    depois da linha dela."""
    paths = []
    inside = False
    for line in finished.lines:
        if line.startswith(f"{lib.TABLE.name}: ") and "arquivo(s)" in line:
            inside = True
            continue
        if inside and line.startswith("    "):
            paths.append(line.strip())
            continue
        inside = False
    return paths


def run_vacuum(
    db: Database,
    *options: str,
) -> list[str] | None:
    """``serialize-db vacuum --full`` com as opções, com a versão da tabela depois impressa;
    devolve os arquivos listados, ou ``None`` numa saída diferente de 0."""
    finished = lib.run_cli(lib.cli_arguments(db, "vacuum", "--full", *options))
    uri = lib.table_uri(db, lib.TABLE.name)
    print(f"  versão da tabela depois: {delta.open_table(uri, db.storage).version()}")
    if finished.code != 0:
        return None
    return listed_paths(finished)


def check_listed(
    title: str,
    listed: list[str] | None,
    expected: list[str],
) -> None:
    """A lista de um ``vacuum`` igual à esperada."""
    problems = []
    if listed is None:
        problems.append("o comando falhou")
    elif sorted(listed) != sorted(expected):
        problems.append(f"listados {sorted(listed)}, esperados {sorted(expected)}")
    lib.check(title, problems)


def main() -> None:
    """O ``vacuum --full`` dos órfãos, com o relatório no terminal e em ``probes/output/``."""
    if len(sys.argv) != 2:
        print(USAGE, file=sys.stderr)
        sys.exit(2)
    source = sys.argv[1]
    db = lib.work_database("vacuum")
    storage = db.storage
    value = lib.source_partitions(source, 1)[0]
    lib.load_partitions(db, source, [value])
    uri = lib.table_uri(db, lib.TABLE.name)
    table_path = storage.relative(uri)
    registered = lib.logged_files(uri, storage)[value]
    totals = lib.delta_totals(storage, uri)
    print(
        f"no log: {registered}; {totals[0]} linhas; versão "
        f"{delta.open_table(uri, storage).version()}"
    )

    # Os órfãos: cópias do arquivo registrado na pasta da partição e num prefixo dentro dela.
    partition_folder = f"{lib.PARTITION_BY}={value}"
    orphans = [
        f"{partition_folder}/orfao-{uuid.uuid4().hex[:8]}.parquet",
        f"{partition_folder}/orfao-{uuid.uuid4().hex[:8]}/0000_part_00.parquet",
    ]
    for orphan in orphans:
        storage.copy(storage.join(table_path, registered[0]), storage.join(table_path, orphan))
        print(f"órfão gravado: {orphan}")

    listed = run_vacuum(db)
    check_listed("com a retenção padrão, nada listado", listed, [])
    listed = run_vacuum(db, "--retention-hours", "0")
    check_listed("com --retention-hours 0, os dois órfãos listados", listed, orphans)
    deleted = run_vacuum(db, "--retention-hours", "0", "--apply")
    check_listed("com --apply, os dois órfãos apagados", deleted, orphans)

    problems = []
    remaining = lib.folder_files(uri, storage)
    if sorted(remaining) != sorted(registered):
        problems.append(f"na pasta: {sorted(remaining)}; no log: {sorted(registered)}")
    after = lib.delta_totals(storage, uri)
    if after != totals:
        problems.append(f"linhas e soma {after}, antes {totals}")
    lib.check("o arquivo do log mantido, com as mesmas linhas e soma", problems)
    lib.finish(db)


if __name__ == "__main__":
    lib.run(main)
