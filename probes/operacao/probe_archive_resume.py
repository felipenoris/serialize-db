"""Sonda do ``archive`` interrompido no meio da cópia e continuado pelo mesmo comando.

``.claude/memory/OPEN_QUESTIONS.md`` ("A operação no ambiente alvo") registra que só o substituto
exercitou a continuação de uma cópia interrompida. A sonda carrega as três primeiras partições de
``cad_lancamentos`` na origem, grava um snapshot delas, roda ``serialize-db archive`` e o encerra
por ``SIGKILL`` logo depois da linha do log que dá a primeira partição copiada, na ordem do log da
tabela, com a seguinte em cópia; depois repete o comando.

Checagens:

- o processo encerrado com uma partição ao menos na cópia, outra fora dela e o snapshot ainda em
  ``snapshots``;
- a repetição sai com 0, pula as partições que a cópia já registrava e copia as outras;
- a cópia com os arquivos da versão do snapshot, por partição, uma versão por partição e nenhum
  arquivo fora do log;
- o snapshot em ``archived`` e fora de ``snapshots``.

Leituras: o tempo e o pico de RSS que ``archive`` imprime por tabela, e o tempo de cada partição
no log.

Exemplo:

.. code-block:: shell

    export SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo
    .venv/bin/python probes/operacao/probe_archive_resume.py s3://bucket/origem/db_projetado
"""

from __future__ import annotations

import sys
import uuid

import operation_lib as lib

from serialize_db import delta
from serialize_db.execution import Database

USAGE = "uso: .venv/bin/python probes/operacao/probe_archive_resume.py <origem>"


def check_killed(
    killed: lib.Finished,
    after_kill: dict[str, list[str]],
    values: list[str],
    in_snapshots: bool,
) -> None:
    """O sinal chegou com uma partição ao menos na cópia, outra fora dela e o snapshot ainda em
    ``snapshots``."""
    problems = []
    if not killed.killed:
        problems.append(f"o processo terminou sem o sinal, com a saída {killed.code}")
    if not after_kill:
        problems.append("nenhuma partição na cópia depois do sinal")
    if len(after_kill) >= len(values):
        problems.append("todas as partições na cópia: o sinal chegou tarde")
    if not in_snapshots:
        problems.append("o snapshot saiu de snapshots antes do fim da cópia")
    lib.check("o processo encerrado no meio da cópia", problems)


def check_rerun(
    rerun: lib.Finished,
    after_kill: dict[str, list[str]],
    values: list[str],
    name: str,
) -> None:
    """A repetição pulou as partições registradas na cópia e copiou as outras."""
    problems = []
    if rerun.code != 0:
        problems.append(f"saída {rerun.code}")
    for value in values:
        expected = "já no destino" if value in after_kill else "copiada"
        if not any(f"{value} {expected}" in line for line in rerun.lines):
            problems.append(f"sem a linha de {value} {expected}")
    if f"snapshot {name} movido para archived" not in rerun.lines:
        problems.append("sem a linha do snapshot movido para archived")
    lib.check("a repetição pula as partições já copiadas e copia as outras", problems)


def check_copy(
    db: Database,
    source_uri: str,
    archive_uri: str,
    values: list[str],
) -> None:
    """A cópia com os arquivos da versão do snapshot, uma versão por partição e nenhum arquivo
    fora do log."""
    storage = db.storage
    problems = []
    expected = lib.logged_files(source_uri, storage)
    copied = lib.logged_files(archive_uri, storage)
    for value in values:
        if sorted(copied.get(value, [])) != sorted(expected.get(value, [])):
            problems.append(
                f"{value}: {copied.get(value)} na cópia, {expected.get(value)} na origem"
            )
    version = delta.open_table(archive_uri, storage).version()
    if version != len(values):
        problems.append(f"a cópia na versão {version}, esperada {len(values)}")
    for path in lib.orphans(archive_uri, storage):
        problems.append(f"{path}: na pasta da cópia, fora do log")
    lib.check("a cópia com os arquivos da versão do snapshot", problems)


def main() -> None:
    """O ``archive`` interrompido e continuado, com o relatório no terminal e em
    ``probes/output/``."""
    if len(sys.argv) != 2:
        print(USAGE, file=sys.stderr)
        sys.exit(2)
    source = sys.argv[1]
    db = lib.work_database("archive")
    storage = db.storage
    values = lib.source_partitions(source, 3)
    lib.load_partitions(db, source, values)

    # O snapshot que o archive copia; o nome segue a regra de um nome do banco.
    name = f"operacao-{uuid.uuid4().hex[:8]}"
    snapshot = lib.run_cli(lib.cli_arguments(db, "snapshot", "--name", name))
    if snapshot.code != 0:
        lib.check("o snapshot gravado", [f"saída {snapshot.code}"])
        lib.finish(db)
    source_uri = lib.table_uri(db, lib.TABLE.name)
    archive_uri = storage.uri_of(storage.join(db.archive_prefix(name), lib.TABLE.name))

    # A primeira execução, encerrada logo depois da linha da primeira partição copiada, que
    # deep_copy escreve depois do commit dela.
    arguments = lib.cli_arguments(db, "archive", "--name", name)
    killed = lib.run_cli_killed(arguments, " copiada, ", lib.no_wait)
    after_kill = lib.logged_files(archive_uri, storage)
    control, _ = delta.read_snapshots(storage, db.environment)
    print(
        f"depois do sinal: {sorted(after_kill)} na cópia; "
        f"{len(lib.folder_files(archive_uri, storage))} arquivo(s) na pasta dela"
    )
    check_killed(killed, after_kill, values, name in control["snapshots"])

    # A repetição do mesmo comando continua a cópia.
    rerun = lib.run_cli(arguments)
    check_rerun(rerun, after_kill, values, name)
    check_copy(db, source_uri, archive_uri, values)

    control, _ = delta.read_snapshots(storage, db.environment)
    problems = []
    if name not in control.get("archived", {}):
        problems.append("fora de archived")
    if name in control["snapshots"]:
        problems.append("ainda em snapshots")
    lib.check("o snapshot em archived", problems)
    lib.finish(db)


if __name__ == "__main__":
    lib.run(main)
