"""Sonda da carga inicial parada no meio e retomada pelo mesmo comando.

``plan/OPEN_QUESTIONS.md`` ("A operação no ambiente alvo") espera a leitura no alvo do que
``tests/test_load.py`` cobre na pasta local. A sonda roda ``serialize-db load`` das três primeiras
partições de ``cad_lancamentos`` na origem e o encerra por ``SIGKILL``, como o kernel sem memória,
quando o arquivo da segunda partição aparece na pasta dela, entre o ``COPY`` e o commit; depois
repete o comando.

Checagens:

- o processo encerrado com a primeira partição no log e a terceira fora dele;
- a repetição grava as partições que o log não tinha, e só elas, sem diferença nas pedidas;
- o log com um arquivo por partição pedida;
- os arquivos fora do log todos da execução encerrada.

Leituras: o momento do sinal, se a segunda partição chegou ao log antes dele, a duração da
repetição, os arquivos fora do log, que ``probe_vacuum_orphans.py`` mostra como apagar, e a pasta
temporária do motor DuckDB que o processo encerrado deixou, com o tamanho, que a sonda apaga. A
repetição sai com o código 1 quando a origem tem partições fora do pedido: o relatório de
``serialize-db load`` soma a origem inteira e mostra cada uma como diferença.

Exemplo:

.. code-block:: shell

    export SERIALIZE_DB_TEST_S3_ROOT=s3://bucket/prefixo
    .venv/bin/python probes/operacao/probe_load_resume.py s3://bucket/origem/db_projetado
"""
from __future__ import annotations

import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import operation_lib as lib

from serialize_db import delta

USAGE = "uso: .venv/bin/python probes/operacao/probe_load_resume.py <origem>"


def execution_of(path: str) -> str:
    """O ``execution_id`` no nome de um arquivo da carga, ``<execution_id>_<uuid>.parquet``."""
    return path.rsplit("/", 1)[-1].split("_", 1)[0]


def duckdb_leftovers(execution_ids: set[str]) -> list[Path]:
    """As pastas temporárias do motor DuckDB das execuções em ``tempfile.gettempdir()``: o
    ``cleanup`` do motor as apaga, e o processo encerrado por ``SIGKILL`` não o roda."""
    found = []
    for folder in sorted(Path(tempfile.gettempdir()).glob("serialize_db_*")):
        for execution_id in execution_ids:
            if (folder / f"{execution_id}.duckdb").exists():
                found.append(folder)
    return found


def folder_megabytes(folder: Path) -> float:
    """O tamanho dos arquivos sob a pasta, em MB."""
    total = 0
    for path in folder.rglob("*"):
        if path.is_file():
            total += path.stat().st_size
    return total / 2**20


def report_leftovers(execution_ids: set[str]) -> None:
    """Imprime e apaga as pastas temporárias que a execução encerrada deixou."""
    leftovers = duckdb_leftovers(execution_ids)
    if not leftovers:
        print(f"pasta temporária do DuckDB deixada em {tempfile.gettempdir()}: nenhuma")
    for folder in leftovers:
        print(f"pasta temporária do DuckDB deixada: {folder}, {folder_megabytes(folder):.1f} MB; "
              "apagada pela sonda")
        shutil.rmtree(folder)


def check_killed(killed: lib.Finished, after_kill: dict[str, list[str]],
                 values: list[str]) -> None:
    """O sinal chegou com a primeira partição no log e a terceira fora dele."""
    problems = []
    if not killed.killed:
        problems.append(f"o processo terminou sem o sinal, com a saída {killed.code}")
    if values[0] not in after_kill:
        problems.append(f"{values[0]} fora do log depois do sinal")
    if values[2] in after_kill:
        problems.append(f"{values[2]} já no log: o sinal chegou tarde")
    lib.check("o processo encerrado entre a primeira e a terceira partição", problems)


def check_rerun(rerun: lib.Finished, missing: list[str], values: list[str]) -> None:
    """A repetição gravou as partições ausentes do log, e só elas, sem diferença nas pedidas."""
    problems = []
    written = f"{lib.TABLE.name}: {len(missing)} partição(ões) gravada(s)"
    if missing:
        written += ": " + ", ".join(missing)
    if written not in rerun.lines:
        problems.append(f"sem a linha {written!r}")
    for line in rerun.lines:
        for value in values:
            if line.strip().startswith(f"DIFERENÇA em {value}:"):
                problems.append(line.strip())
    if rerun.code == 2:
        problems.append("a repetição saiu com o código 2")
    lib.check("a repetição grava só as partições ausentes do log, sem diferença nas pedidas",
              problems)


def main() -> None:
    """A carga parada e retomada, com o relatório no terminal e em ``probes/output/``."""
    if len(sys.argv) != 2:
        print(USAGE, file=sys.stderr)
        sys.exit(2)
    source = sys.argv[1]
    db = lib.work_database("carga")
    storage = db.storage
    values = lib.source_partitions(source, 3)
    uri = lib.table_uri(db, lib.TABLE.name)
    arguments = lib.cli_arguments(db, "load", "--source", source, "--tables", lib.TABLE.name,
                                  "--partitions", *values)
    second_folder = storage.join(db.environment, lib.TABLE.name,
                                 f"{lib.PARTITION_BY}={values[1]}")

    # O sinal espera o arquivo da segunda partição, depois da linha da primeira no log.
    def wait_second_file(process: subprocess.Popen) -> str:
        return lib.wait_for_file(storage, second_folder, process)

    print(f"partições pedidas: {', '.join(values)}")
    # A linha do log da primeira partição gravada, depois do commit e da releitura dela.
    trigger = f"{lib.TABLE.name} {delta.partition_label(values[0])}: "
    killed = lib.run_cli_killed(arguments, trigger, wait_second_file)
    after_kill = lib.logged_files(uri, storage)
    written = lib.folder_files(uri, storage)
    killed_ids = {execution_of(path) for path in written}
    print(f"depois do sinal: {sorted(after_kill)} no log; {len(written)} arquivo(s) na pasta, "
          f"da execução {', '.join(sorted(killed_ids))}")
    arrived = "chegou ao log" if values[1] in after_kill else "ficou fora do log"
    print(f"a segunda partição, {values[1]}, {arrived} antes do sinal")
    check_killed(killed, after_kill, values)
    report_leftovers(killed_ids)

    # A repetição do mesmo comando grava o que falta no log.
    missing = [value for value in values if value not in after_kill]
    rerun = lib.run_cli(arguments)
    check_rerun(rerun, missing, values)

    final = lib.logged_files(uri, storage)
    problems = []
    for value in values:
        paths = final.get(value, [])
        if len(paths) != 1:
            problems.append(f"{value}: {len(paths)} arquivo(s) no log")
        for path in paths:
            print(f"no log: {path}")
    lib.check("o log com um arquivo por partição pedida", problems)

    outside = lib.orphans(uri, storage)
    for path in outside:
        print(f"fora do log: {path}")
    problems = []
    for path in outside:
        if execution_of(path) not in killed_ids:
            problems.append(f"{path}: de outra execução")
    lib.check("os arquivos fora do log todos da execução encerrada", problems)
    lib.finish(db)


if __name__ == "__main__":
    lib.run(main)
