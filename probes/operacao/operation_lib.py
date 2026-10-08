"""Biblioteca comum das sondas da operação: a raiz de trabalho, o relatório, ``serialize-db``
num processo filho, encerrado por ``SIGKILL`` num ponto marcado, os arquivos de uma tabela Delta
e a limpeza.

Uma sonda da operação roda uma rotina de ``serialize-db`` no ambiente alvo sobre as primeiras
partições de ``cad_lancamentos`` da base de origem, em ordem de nome e fora das de
``--ignore-partitions``, que ela só lê. Ela grava só sob
``<raiz>/serialize-db-operacao/<sonda>-<id>/``, com a raiz de ``SERIALIZE_DB_TEST_S3_ROOT`` ou, sem
ela, de ``SERIALIZE_DB_TEST_LOCAL_ROOT``, e apaga a pasta no fim, também quando para numa exceção
(``SERIALIZE_DB_TEST_KEEP`` a mantém). ``probe_published_base.py`` e ``probe_readers.py`` são a
exceção: rodam sobre a própria base publicada, sem raiz de trabalho, com ``open_report`` e
``finish_on_base``, e só a primeira grava nela. A saída e os erros vão ao terminal e a
``probes/output/operacao_<sonda>_<data-hora>.txt``. Código de saída: 0 quando toda checagem passou,
1 quando alguma reprovou ou a sonda parou numa exceção, 2 no uso errado da linha de comando, sem
raiz de trabalho ou com a origem sem as partições.
"""

from __future__ import annotations

import argparse
import dataclasses
import importlib.metadata
import logging
import os
import platform
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import TextIO

import pyarrow as pa
import pyarrow.fs as pafs

# O modelo do cliente vive em tests/, que a sonda põe no caminho; o processo filho o recebe pelo
# PYTHONPATH.
TESTS_DIR = Path(__file__).resolve().parents[2] / "tests"
sys.path.insert(0, str(TESTS_DIR))
from client_model import Base  # noqa: E402

from serialize_db import delta, parquet_import  # noqa: E402
from serialize_db.execution import Database  # noqa: E402
from serialize_db.resources import available_cpus, available_memory  # noqa: E402
from serialize_db.resources import environment_limits  # noqa: E402
from serialize_db.schema import literal  # noqa: E402
from serialize_db.storage import Storage  # noqa: E402

OUTPUT_DIR = Path(__file__).resolve().parents[1] / "output"
KEEP_VARIABLE = "SERIALIZE_DB_TEST_KEEP"
PACKAGES = ("deltalake", "duckdb", "pyarrow", "boto3", "redshift-connector")
ENVIRONMENT = "prd"
METADATA = "client_model:Base.metadata"
TABLE = Base.metadata.tables["cad_lancamentos"]
PARTITION_BY = "data_base_str"
POLL_SECONDS = 0.2

FAILED: list[str] = []
PASSED: list[str] = []
# O banco da sonda, que work_database anota para a limpeza de run.
DATABASES: list[Database] = []


class Tee:
    """Escreve ao mesmo tempo num fluxo do terminal e no arquivo de saída."""

    def __init__(
        self,
        file: TextIO,
        terminal: TextIO,
    ) -> None:
        self.file = file
        self.terminal = terminal

    def write(
        self,
        text: str,
    ) -> int:
        self.terminal.write(text)
        self.file.write(text)
        self.file.flush()
        return len(text)

    def flush(self) -> None:
        self.terminal.flush()
        self.file.flush()


@dataclasses.dataclass
class Finished:
    """Um ``serialize-db`` encerrado: o código de saída, as linhas da saída e a duração."""

    code: int
    lines: list[str]
    seconds: float
    killed: bool = False


# ---------------------------------------------------------------- a raiz e o relatório


def _suite_root() -> str:
    """A raiz das suítes: a do S3 ou, sem ela, a pasta local, que precisa existir; sem nenhuma, a
    sonda para com o código 2."""
    s3_root = os.environ.get("SERIALIZE_DB_TEST_S3_ROOT")
    if s3_root:
        return s3_root
    local_root = os.environ.get("SERIALIZE_DB_TEST_LOCAL_ROOT")
    if local_root and Path(local_root).is_dir():
        return local_root
    print(
        "SERIALIZE_DB_TEST_S3_ROOT ausente e SERIALIZE_DB_TEST_LOCAL_ROOT ausente ou sem pasta: "
        "a sonda grava só sob a raiz das suítes.",
        file=sys.stderr,
    )
    sys.exit(2)


def work_database(
    name: str,
) -> Database:
    """O banco da sonda ``name`` numa pasta nova, ``<raiz>/serialize-db-operacao/<name>-<id>``,
    no ambiente ``prd`` dela, com o arquivo de saída aberto por ``open_report``."""
    root = f"{_suite_root().rstrip('/')}/serialize-db-operacao/{name}-{uuid.uuid4().hex[:8]}"
    output = open_report(name)
    print(f"raiz de trabalho: {root}; ambiente {ENVIRONMENT}; saída: {output}")
    db = Database(root, ENVIRONMENT, Base.metadata)
    DATABASES.append(db)
    return db


def open_report(
    name: str,
) -> Path:
    """Abre o arquivo de saída da sonda ``name``, que recebe também os erros e o log da
    biblioteca, e imprime o cabeçalho com a máquina e as versões; devolve o caminho do
    arquivo."""
    OUTPUT_DIR.mkdir(exist_ok=True)
    output = OUTPUT_DIR / f"operacao_{name}_{time.strftime('%Y%m%d-%H%M%S')}.txt"
    report = output.open("w", encoding="utf-8")
    sys.stdout = Tee(report, sys.stdout)
    sys.stderr = Tee(report, sys.stderr)
    # O log da biblioteca vai à mesma saída a partir de INFO, com o instante de cada linha; o dos
    # outros pacotes, como o botocore, só a partir de WARNING.
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.WARNING,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    logging.getLogger("serialize_db").setLevel(logging.INFO)
    versions = []
    for package in PACKAGES:
        versions.append(f"{package} {importlib.metadata.version(package)}")
    print(
        f"{time.strftime('%Y-%m-%d %H:%M:%S %z')}; {platform.platform()}; "
        f"python {platform.python_version()}; {', '.join(versions)}"
    )
    print(f"máquina: {available_cpus()} CPUs, {available_memory() / 2**30:.1f} GiB disponíveis")
    return output


def parse_arguments(
    description: str,
) -> argparse.Namespace:
    """A linha de comando das sondas que leem a origem: ``source``, a raiz da base Parquet de
    origem, local ou ``s3://``, e ``ignore_partitions``, os valores de partição que a sonda não
    toma, vazio sem ``--ignore-partitions``, como o do ``serialize-db import``; o uso errado sai
    com o código 2."""
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument("source", metavar="origem", help="a raiz da base Parquet de origem")
    parser.add_argument(
        "--ignore-partitions",
        nargs="+",
        metavar="AAAA-MM-DD",
        default=(),
        help="deixa estas partições fora das primeiras que a sonda toma de cad_lancamentos",
    )
    return parser.parse_args()


def source_partitions(
    source: str,
    count: int,
    ignored: Sequence[str] = (),
) -> list[str]:
    """Os ``count`` primeiros valores de partição de ``cad_lancamentos`` na origem, em ordem de
    nome e fora de ``ignored``, com uma linha impressa por partição ignorada que a origem tem;
    com menos, a sonda para com o código 2."""
    kept, skipped = parquet_import.split_ignored_partitions(source, TABLE, ignored)
    for value in skipped:
        print(f"{TABLE.name}: partição {value} ignorada, em --ignore-partitions")
    values = kept[:count]
    if len(values) < count:
        print(
            f"{source}: {TABLE.name} tem {len(kept)} partição(ões) fora das ignoradas, a sonda "
            f"pede {count}",
            file=sys.stderr,
        )
        sys.exit(2)
    return values


def load_partitions(
    db: Database,
    source: str,
    values: list[str],
) -> None:
    """A carga das partições pela biblioteca, sem o relatório de ``serialize-db import``, que soma a
    origem inteira."""
    started = time.perf_counter()
    loaded = parquet_import.import_table(db, TABLE, source, values)
    print(f"carga de {', '.join(loaded)} em {time.perf_counter() - started:.1f} s")


def check(
    title: str,
    problems: list[str],
) -> None:
    """Imprime a checagem ``title`` como ``OK`` ou ``PROBLEMAS`` com a lista, e a registra para o
    código de saída."""
    print(f"== {title}: {'OK' if not problems else 'PROBLEMAS'}")
    for problem in problems:
        print("   -", problem)
    if problems:
        FAILED.append(title)
    else:
        PASSED.append(title)


def delete_root(
    db: Database,
) -> None:
    """Apaga a raiz de trabalho, com o número de objetos e o tamanho impressos, a menos que
    ``SERIALIZE_DB_TEST_KEEP`` a mantenha."""
    storage = db.storage
    if os.environ.get(KEEP_VARIABLE):
        print(f"raiz mantida por {KEEP_VARIABLE}: {storage.uri}")
        return
    if not storage.exists(""):
        return
    # O número de objetos antes da exclusão: num bucket versionado cada um vira versão não
    # corrente, cobrada até uma regra NoncurrentVersionExpiration.
    selector = pafs.FileSelector(storage.path, recursive=True)
    files = []
    for info in storage.filesystem.get_file_info(selector):
        if info.type == pafs.FileType.File:
            files.append(info)
    megabytes = sum(info.size for info in files) / 2**20
    storage.filesystem.delete_dir(storage.path)
    print(f"raiz apagada: {len(files)} objeto(s), {megabytes:.0f} MB")


def _print_checks() -> None:
    """Imprime o resumo das checagens, com as reprovadas."""
    print(f"checagens: {len(PASSED) + len(FAILED)}, reprovadas: {len(FAILED)}")
    for title in FAILED:
        print("   reprovada:", title)


def finish(
    db: Database,
) -> None:
    """Imprime o resumo das checagens, apaga a raiz de trabalho e encerra com o código 1 quando
    alguma checagem reprovou."""
    _print_checks()
    delete_root(db)
    sys.stdout.flush()
    sys.exit(1 if FAILED else 0)


def finish_on_base() -> None:
    """Imprime o resumo das checagens e encerra com o código 1 quando alguma reprovou, sem raiz de
    trabalho a apagar: o fim das sondas sobre a própria base."""
    _print_checks()
    sys.stdout.flush()
    sys.exit(1 if FAILED else 0)


def run(
    main: Callable[[], None],
) -> None:
    """Roda a sonda; numa exceção, apaga a raiz de trabalho antes de ela subir com o traceback,
    que o arquivo de saída recebe pelo stderr."""
    try:
        main()
    except Exception:
        for db in DATABASES:
            delete_root(db)
        raise


# ---------------------------------------------------------------- serialize-db num processo filho


def cli_arguments(
    db: Database,
    command: str,
    *options: str,
) -> list[str]:
    """Os argumentos de um subcomando de operação sobre o banco da sonda."""
    return [
        command,
        "--root",
        db.storage.uri,
        "--environment",
        db.environment,
        "--metadata",
        METADATA,
        *options,
    ]


def _child_environment() -> dict[str, str]:
    """O ambiente do processo filho, com ``tests/`` no ``PYTHONPATH`` para ``--metadata``."""
    environment = dict(os.environ)
    paths = [str(TESTS_DIR)]
    if environment.get("PYTHONPATH"):
        paths.append(environment["PYTHONPATH"])
    environment["PYTHONPATH"] = os.pathsep.join(paths)
    return environment


def _start(
    arguments: list[str],
) -> subprocess.Popen:
    """``serialize-db`` do ambiente virtual da sonda, com o stderr, onde vai o log, junto do
    stdout."""
    print("$ serialize-db " + " ".join(arguments))
    command = [str(Path(sys.executable).with_name("serialize-db")), *arguments]
    return subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        env=_child_environment(),
    )


def run_cli(
    arguments: list[str],
) -> Finished:
    """Roda ``serialize-db`` até o fim, ecoando cada linha da saída."""
    started = time.perf_counter()
    process = _start(arguments)
    lines = []
    for line in process.stdout:
        lines.append(line.rstrip("\n"))
        print("  |", lines[-1])
    code = process.wait()
    seconds = time.perf_counter() - started
    print(f"  saída {code} em {seconds:.1f} s")
    return Finished(code, lines, seconds)


def run_cli_killed(
    arguments: list[str],
    trigger: str,
    before_kill: Callable[[subprocess.Popen], str],
) -> Finished:
    """Roda ``serialize-db`` e o encerra por ``SIGKILL``, como o kernel sem memória, sem que o
    processo rode um ``finally``: depois da primeira linha da saída que contém ``trigger`` e da
    volta de ``before_kill``, que devolve o que esperou. Sem a linha, ou com o processo terminado
    durante a espera, não há sinal."""
    started = time.perf_counter()
    process = _start(arguments)
    lines: list[str] = []
    seen = threading.Event()
    triggered = []

    # Uma thread lê a saída enquanto a principal espera: o processo nunca para num pipe cheio.
    def read_output() -> None:
        for line in process.stdout:
            lines.append(line.rstrip("\n"))
            print("  |", lines[-1])
            if trigger in lines[-1] and not triggered:
                triggered.append(time.perf_counter() - started)
                seen.set()
        seen.set()

    reader = threading.Thread(target=read_output)
    reader.start()
    seen.wait()
    killed = False
    if triggered and process.poll() is None:
        waited = before_kill(process)
        if process.poll() is None:
            process.send_signal(signal.SIGKILL)
            killed = True
            print(f"  SIGKILL depois {waited}, {time.perf_counter() - started:.1f} s do início")
    reader.join()
    code = process.wait()
    seconds = time.perf_counter() - started
    print(f"  saída {code} em {seconds:.1f} s")
    return Finished(code, lines, seconds, killed)


def wait_for_file(
    storage: Storage,
    folder: str,
    process: subprocess.Popen,
) -> str:
    """Espera um arquivo ``.parquet`` na pasta, listada a cada ``POLL_SECONDS``, enquanto o
    processo roda; devolve o que viu."""
    started = time.perf_counter()
    while process.poll() is None:
        files = storage.list_files(folder, ".parquet")
        if files:
            waited = time.perf_counter() - started
            return f"de {files[0].rsplit('/', 1)[-1]} aparecer em {waited:.1f} s"
        time.sleep(POLL_SECONDS)
    return "do fim do processo"


def no_wait(
    process: subprocess.Popen,
) -> str:
    """O sinal logo depois da linha marcada."""
    return "da linha marcada"


# ---------------------------------------------------------------- os arquivos de uma tabela


def table_uri(
    db: Database,
    *parts: str,
) -> str:
    """A URI de uma pasta sob o ambiente, como a de ``cad_lancamentos``."""
    return db.storage.uri_of(db.storage.join(db.environment, *parts))


def logged_files(
    uri: str,
    storage: Storage,
) -> dict[str, list[str]]:
    """Os arquivos que a versão atual da tabela registra, por valor de partição, relativos à pasta
    dela; a tabela ausente dá o dicionário vazio."""
    if not delta.table_exists(uri, storage):
        return {}
    actions = pa.table(delta.open_table(uri, storage).get_add_actions(flatten=True))
    by_value: dict[str, list[str]] = {}
    if actions.num_rows == 0:
        return by_value
    values = actions.column(f"partition.{PARTITION_BY}").to_pylist()
    for path, value in zip(actions.column("path").to_pylist(), values):
        by_value.setdefault(value, []).append(path)
    return by_value


def folder_files(
    uri: str,
    storage: Storage,
) -> list[str]:
    """Os arquivos ``.parquet`` sob a pasta da tabela, fora de ``_delta_log/``, relativos a ela."""
    table_path = storage.relative(uri)
    files = []
    for path in storage.list_files(table_path, ".parquet"):
        files.append(path.removeprefix(table_path + "/"))
    return files


def orphans(
    uri: str,
    storage: Storage,
) -> list[str]:
    """Os arquivos da pasta da tabela que a versão atual não registra."""
    registered = set()
    for paths in logged_files(uri, storage).values():
        registered.update(paths)
    return [path for path in folder_files(uri, storage) if path not in registered]


def delta_totals(
    storage: Storage,
    uri: str,
) -> tuple[int, int]:
    """As linhas da tabela e a soma de ``id_lancamento`` pelo ``delta_scan`` do DuckDB, numa
    conexão com os limites do ambiente."""
    connection = storage.duckdb_connect(config=environment_limits())
    try:
        query = f'SELECT count(*), sum("id_lancamento") FROM delta_scan({literal(uri)})'
        rows, total = connection.execute(query).fetchone()
    finally:
        connection.close()
    return int(rows), int(total or 0)
