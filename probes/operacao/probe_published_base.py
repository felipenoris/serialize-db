"""Sonda da base publicada no Redshift: o ``EXPLAIN`` do join típico e a volta a um snapshot
anterior ao publicado, com o tempo e o pico de RSS por tabela.

A sonda roda depois da publicação da base inteira pelo canal ``default``, com cada tabela
``prd_*`` na versão do snapshot do canal, e recebe a raiz da base, a do ``--root`` da publicação.
Ela:

1. lê o ``EXPLAIN`` do join de ``prd_cad_lancamentos`` com ``prd_cad_contas`` por ``id_conta``,
   cujos rótulos ``DS_BCAST_INNER`` ou ``DS_DIST_BOTH`` pedem a chave de distribuição (decisão do
   usuário de 2026-09-23, ``.claude/memory/decisions.md``);
2. refaz, com as mesmas linhas, a primeira partição de ``cad_lancamentos`` em ordem de nome, numa
   execução do motor DuckDB marcada com o snapshot ``refeito-<execution_id>``: ``ingest``,
   ``audit`` e ``publish_delta``, o passo 1 de "Refazer um snapshot" de ``docs/operacao.md``;
3. aponta o canal ``default`` para o snapshot novo e publica a base pelo canal, a ida;
4. devolve o canal ao snapshot de antes e publica de novo, a volta a um snapshot anterior ao
   publicado, que troca a partição refeita pelos arquivos da versão do snapshot.

Os passos 3 e 4 rodam ``serialize-db channel`` e ``serialize-db publish_redshift --channel default
--max-workers 4`` num processo filho, como o operador os roda; a volta roda também quando a ida
falha.

Checagens:

- o plano do join com algum rótulo ``DS_*``; a recusa do ``EXPLAIN`` a reprova, e a sonda segue;
- a ida com cada tabela publicada na versão do snapshot novo, e só a partição refeita trocada;
- a volta com cada tabela publicada na versão do snapshot de antes, e só a partição refeita
  trocada;
- ``prd_cad_lancamentos`` com as linhas e a soma de ``id_lancamento`` de antes, depois da ida e
  depois da volta;
- o canal ``default`` no snapshot de antes, no fim.

Leituras: o plano e os rótulos ``DS_*``; as linhas da partição e o tempo da execução que a refaz;
na ida e na volta, por tabela trocada, a versão, as partições, o tempo e o pico de RSS do processo
da publicação.

A sonda grava na própria base, e não sob a raiz das suítes: a versão nova de ``cad_lancamentos``,
o snapshot ``refeito-<execution_id>`` e os manifestos do ``COPY`` em ``prd/publicacao/`` ficam; o
canal ``default`` e as tabelas ``prd_*`` voltam ao estado de antes. Com a raiz fora do S3, sem o
canal ``default`` ou com uma tabela, publicada ou atual, fora da versão do snapshot dele, ela para
com o código 2 antes de gravar: a execução da partição refeita levaria ao snapshot novo a versão
atual de cada tabela.

Exemplo:

.. code-block:: shell

    export SERIALIZE_DB_REDSHIFT_WORKGROUP=workgroup SERIALIZE_DB_REDSHIFT_SCHEMA=esquema
    .venv/bin/python probes/operacao/probe_published_base.py s3://bucket/prefixo
"""

from __future__ import annotations

import argparse
import re
import sys
import time
import uuid

import operation_lib as lib
import pyarrow as pa
import pyarrow.compute as pc
import redshift_connector
from deltalake import DeltaTable

from serialize_db import delta, publication
from serialize_db.engine import redshift
from serialize_db.engine.redshift import RedshiftConfig
from serialize_db.errors import ContractError
from serialize_db.execution import Database, Execution
from serialize_db.resources import peak_rss_mb

CHANNEL = "default"
# Os rótulos do plano que pedem a chave de distribuição, pela decisão do usuário de 2026-09-23.
REDISTRIBUTING = ("DS_BCAST_INNER", "DS_DIST_BOTH")
# A linha que a publicação grava no log serialize_db.publication por tabela trocada.
PUBLISHED_LINE = re.compile(
    r"serialize_db\.publication: (?P<table>\w+) publicada na versão (?P<version>\d+): "
    r"partições (?P<partitions>.+), em (?P<seconds>[\d.]+) s; "
    r"RSS máximo do processo (?P<rss>\d+) MB"
)


def parse_arguments() -> argparse.Namespace:
    """A linha de comando: ``root``, a raiz da base publicada; o uso errado sai com o código 2."""
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("root", metavar="raiz", help="a raiz da base, a do --root da publicação")
    return parser.parse_args()


def published_name(
    config: RedshiftConfig,
    table: str,
) -> str:
    """O nome qualificado da tabela publicada ``prd_<table>`` no esquema."""
    return f'"{config.schema}"."{lib.ENVIRONMENT}_{table}"'


def query_rows(
    config: RedshiftConfig,
    text: str,
) -> list[tuple]:
    """As linhas de uma consulta numa conexão própria."""
    connection = redshift.connect(config)
    try:
        cursor = connection.cursor()
        cursor.execute(text)
        return [tuple(row) for row in cursor.fetchall()]
    finally:
        connection.close()


def table_statuses(
    db: Database,
    config: RedshiftConfig,
) -> dict[str, publication.PublicationStatus]:
    """A situação da publicação de cada tabela do ambiente, pelo nome da tabela do modelo."""
    statuses = {}
    for status in publication.publication_status(db, config):
        statuses[status.table.removeprefix(f"{db.environment}_")] = status
    return statuses


def published_problems(
    statuses: dict[str, publication.PublicationStatus],
    expected: dict[str, int],
) -> list[str]:
    """As tabelas cuja versão publicada não é a de ``expected``."""
    problems = []
    for table, version in sorted(expected.items()):
        published = statuses[table].published_version if table in statuses else None
        if published != version:
            problems.append(f"{table}: publicada na versão {published}, não {version}")
    return problems


def current_problems(
    statuses: dict[str, publication.PublicationStatus],
    expected: dict[str, int],
) -> list[str]:
    """As tabelas cuja versão atual do Delta não é a de ``expected``."""
    problems = []
    for table, version in sorted(expected.items()):
        current = statuses[table].current_version if table in statuses else None
        if current != version:
            problems.append(f"{table}: versão atual {current}, não {version}")
    return problems


def channel_state(
    db: Database,
    config: RedshiftConfig,
) -> tuple[str, dict[str, int]]:
    """O snapshot do canal ``default`` e as versões dele; sem o canal, ou com uma tabela publicada
    ou atual fora da versão do snapshot, a sonda para com o código 2."""
    control, _ = delta.read_snapshots(db.storage, db.environment)
    try:
        snapshot = delta.channel_snapshot(control, CHANNEL)
    except ContractError as error:
        print(f"{error}; a sonda roda depois da publicação pelo canal", file=sys.stderr)
        sys.exit(2)
    versions = delta.snapshot_versions(control, snapshot)
    statuses = table_statuses(db, config)
    problems = published_problems(statuses, versions) + current_problems(statuses, versions)
    if problems:
        print(
            f"a base não está no snapshot {snapshot} do canal {CHANNEL}: {'; '.join(problems)}; "
            "a sonda roda depois da publicação pelo canal, antes de outra escrita na base",
            file=sys.stderr,
        )
        sys.exit(2)
    return snapshot, versions


def published_totals(
    config: RedshiftConfig,
) -> tuple[int, int]:
    """As linhas e a soma de ``id_lancamento`` de ``prd_cad_lancamentos``."""
    table = published_name(config, lib.TABLE.name)
    rows, total = query_rows(config, f'SELECT count(*), sum("id_lancamento") FROM {table}')[0]
    return int(rows), int(total)


def read_join_plan(
    config: RedshiftConfig,
) -> list[str]:
    """O ``EXPLAIN`` do join de ``prd_cad_lancamentos`` com ``prd_cad_contas`` por ``id_conta``,
    com o plano e os rótulos ``DS_*`` impressos; devolve os problemas da checagem, a recusa do
    ``EXPLAIN`` entre eles, para a sonda seguir à partição refeita."""
    entries = published_name(config, lib.TABLE.name)
    accounts = published_name(config, "cad_contas")
    join = f'{entries} l JOIN {accounts} c ON l."id_conta" = c."id_conta"'
    try:
        plan = query_rows(config, f"EXPLAIN SELECT count(*) FROM {join}")
    except redshift_connector.Error as error:
        return [f"EXPLAIN recusado: {type(error).__name__}: {error}"]
    print("EXPLAIN do join de cad_lancamentos com cad_contas por id_conta:")
    lines = []
    for row in plan:
        lines.append(str(row[0]))
        print("  ", lines[-1])
    labels = sorted(set(re.findall(r"DS_\w+", "\n".join(lines))))
    redistributing = [label for label in labels if label in REDISTRIBUTING]
    print(f"rótulos do join: {labels}; os que pedem a chave de distribuição: {redistributing}")
    if not labels:
        return ["o plano não traz rótulo DS_*"]
    return []


def partition_rows(
    dt: DeltaTable,
    value: str,
) -> int:
    """As linhas da partição ``value`` pelo ``numRecords`` dos arquivos que a versão registra."""
    actions = pa.table(dt.get_add_actions(flatten=True))
    in_partition = pc.equal(actions.column(f"partition.{lib.PARTITION_BY}"), value)
    return pc.sum(actions.filter(in_partition).column("num_records")).as_py()


def redo_partition(
    db: Database,
    value: str,
) -> str:
    """Refaz a partição ``value`` de ``cad_lancamentos`` com as mesmas linhas, numa execução do
    motor DuckDB marcada com o snapshot ``refeito-<execution_id>``; devolve o nome do
    snapshot."""
    execution_id = f"operacao-{uuid.uuid4().hex[:8]}"
    snapshot = f"refeito-{execution_id}"
    started = time.perf_counter()
    with Execution(db, "duckdb", partition=value, execution_id=execution_id) as run:
        run.snapshot(snapshot)
        run.ingest(lib.TABLE, partitions=[value])
        run.audit(lib.TABLE, [value])
        run.publish_delta(lib.TABLE, partitions=[value])
    print(
        f"partição {value} refeita e snapshot {snapshot} gravado em "
        f"{time.perf_counter() - started:.1f} s; RSS máximo do processo {peak_rss_mb():.0f} MB"
    )
    return snapshot


def publish_channel(
    db: Database,
    snapshot: str,
) -> lib.Finished:
    """Aponta o canal ``default`` para o snapshot e publica a base pelo canal, cada comando num
    processo filho; devolve o fim da publicação. O canal que não se move para a sonda numa
    exceção, antes de publicar."""
    moved = lib.run_cli(lib.cli_arguments(db, "channel", "--name", CHANNEL, "--snapshot", snapshot))
    if moved.code != 0:
        raise RuntimeError(f"serialize-db channel saiu com o código {moved.code}")
    return lib.run_cli(
        lib.cli_arguments(db, "publish_redshift", "--channel", CHANNEL, "--max-workers", "4")
    )


def published_tables(
    finished: lib.Finished,
) -> dict[str, dict[str, str]]:
    """As tabelas que a publicação trocou, pelas linhas do log ``serialize_db.publication``: a
    versão, as partições, o tempo e o pico de RSS do processo."""
    tables = {}
    for line in finished.lines:
        match = PUBLISHED_LINE.search(line)
        if match is not None:
            tables[match.group("table")] = match.groupdict()
    return tables


def publication_problems(
    db: Database,
    config: RedshiftConfig,
    finished: lib.Finished,
    expected: dict[str, int],
    value: str,
    label: str,
) -> list[str]:
    """Os problemas de uma publicação pelo canal: a saída diferente de 0, uma tabela fora da
    versão de ``expected`` e uma troca além da partição ``value`` de ``cad_lancamentos``; imprime
    a leitura de cada tabela trocada."""
    problems = []
    if finished.code != 0:
        problems.append(f"publish_redshift saiu com o código {finished.code}")
    problems.extend(published_problems(table_statuses(db, config), expected))
    swapped = published_tables(finished)
    for table, fields in swapped.items():
        print(
            f"{label}: {table} na versão {fields['version']}, partições {fields['partitions']}, "
            f"em {fields['seconds']} s; RSS máximo do processo {fields['rss']} MB"
        )
    if sorted(swapped) != [lib.TABLE.name]:
        problems.append(f"tabelas trocadas: {sorted(swapped)}, não [{lib.TABLE.name!r}]")
    elif swapped[lib.TABLE.name]["partitions"] != str([value]):
        problems.append(f"partições trocadas: {swapped[lib.TABLE.name]['partitions']}")
    return problems


def totals_problems(
    config: RedshiftConfig,
    before: tuple[int, int],
    label: str,
) -> list[str]:
    """As linhas e a soma de ``prd_cad_lancamentos`` diferentes das de antes."""
    totals = published_totals(config)
    print(f"{label}: prd_cad_lancamentos com {totals[0]} linhas, soma de id_lancamento {totals[1]}")
    if totals != before:
        return [f"{totals} depois da {label}, {before} antes"]
    return []


def main() -> None:
    """O ``EXPLAIN``, a partição refeita, a ida e a volta, com o relatório no terminal e em
    ``probes/output/``."""
    options = parse_arguments()
    output = lib.open_report("base_publicada")
    db = Database(options.root, lib.ENVIRONMENT, lib.Base.metadata)
    print(f"base: {db.storage.uri}; ambiente {db.environment}; saída: {output}")
    if not db.storage.is_s3:
        print("a publicação lê do S3: a sonda pede a raiz da base no S3", file=sys.stderr)
        sys.exit(2)
    try:
        config = RedshiftConfig.from_environment()
    except ContractError as error:
        print(error, file=sys.stderr)
        sys.exit(2)

    # O estado de antes: o snapshot do canal, publicado em cada tabela, e os totais.
    snapshot, versions = channel_state(db, config)
    before = published_totals(config)
    print(f"canal {CHANNEL}: {snapshot}, versões {versions}")
    print(f"antes: prd_cad_lancamentos com {before[0]} linhas, soma de id_lancamento {before[1]}")
    lib.check("o plano do join com algum rótulo DS_*", read_join_plan(config))

    # A partição refeita numa versão nova, com o snapshot dela.
    dt = delta.open_table(db.uri(lib.TABLE), db.storage)
    value = delta.partition_values(dt, lib.PARTITION_BY)[0]
    print(f"partição refeita: {value}, {partition_rows(dt, value)} linhas na versão {dt.version()}")
    redone = redo_partition(db, value)
    control, _ = delta.read_snapshots(db.storage, db.environment)
    redone_versions = delta.snapshot_versions(control, redone)

    # A ida ao snapshot novo e a volta ao de antes, que roda também quando a ida falha.
    try:
        forward = publish_channel(db, redone)
        problems = publication_problems(db, config, forward, redone_versions, value, "ida")
        problems.extend(totals_problems(config, before, "ida"))
        lib.check("a ida no snapshot novo, só com a partição refeita trocada", problems)
    finally:
        back = publish_channel(db, snapshot)
    problems = publication_problems(db, config, back, versions, value, "volta")
    problems.extend(totals_problems(config, before, "volta"))
    lib.check("a volta ao snapshot de antes, só com a partição refeita trocada", problems)

    # O canal no snapshot de antes.
    control, _ = delta.read_snapshots(db.storage, db.environment)
    pointed = delta.channel_snapshot(control, CHANNEL)
    channel_problems = [] if pointed == snapshot else [f"canal {CHANNEL} em {pointed}"]
    lib.check(f"o canal {CHANNEL} no snapshot {snapshot}, no fim", channel_problems)
    lib.finish_on_base()


if __name__ == "__main__":
    lib.run(main)
