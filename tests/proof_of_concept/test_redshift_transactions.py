"""Transações simultâneas no Redshift: duas publicações da etapa 8 que se cruzam no esquema do
datashare.

A publicação troca as partições de uma tabela e grava a sua linha em ``serialize_db_publications``
numa transação só, e essa tabela de controle é a única que dois ambientes escrevem ao mesmo tempo
(``plan/PLAN.md``, premissas). A documentação do Redshift diz o que esperar (``plan/redshift.md``,
seção "Transações concorrentes"): ``DELETE`` e ``UPDATE`` tomam o lock da tabela e o segundo espera
o primeiro terminar; sob isolamento de snapshot, dois escritores de linhas distintas confirmam, e
sob o serializável o segundo recebe ``1023``; o snapshot de uma transação nasce no primeiro
``SELECT``, DML ou DDL dela; o ``LOCK`` no início força a ordem, mas não está na lista de comandos
que a escrita por datashare aceita. O banco do datashare informou isolamento ``UNKNOWN`` no ambiente
alvo, e a escrita por datashare exige snapshot no banco do produtor. Estes testes medem o que o
esquema do datashare faz.

Cada cenário de concorrência abre duas conexões próprias, A e B, pela resolução de
``connect_redshift``, e segue a sequência da etapa 8 com ``INSERT ... VALUES`` no lugar do ``COPY``,
que toma o mesmo lock. A roda
até o ponto de conflito e segura a transação aberta; B roda numa thread, e o relatório registra se B
esperou, em que comando, o desfecho de cada comando dos dois e o estado final das tabelas. O
``COMMIT`` de A solta B. Nenhum desfecho do Redshift vira asserção antes de uma execução no ambiente
alvo; cada teste confere só que o estado final bate com os desfechos lidos, isto é, que a transação
confirmada deixou as suas linhas e a abortada nenhuma. Uma thread que não termina no prazo depois do
``COMMIT`` de A é registrada como presa, e a sessão dela é encerrada por ``pg_terminate_backend``.

Os cenários: escritas em tabelas distintas (a ingestão em paralelo e ``publish_redshift`` com uma
conexão por tabela); dev e prod publicando ao mesmo tempo, com linhas distintas da tabela de
controle; duas publicações da mesma tabela e partição, com a staging de nome fixo da etapa 8; o
``LOCK`` da tabela de controle no início da transação; e a linha de controle gravada por um
``UPDATE`` condicionado à versão lida, como primeiro comando. Dois cenários de uma conexão só
leem a staging temporária da publicação, cheia dentro da transação e antes do ``BEGIN``: a
escrita de uma transação vai para um banco só no datashare, e a documentação não diz em que banco
fica a tabela temporária criada depois do ``USE``. As tabelas são
``serialize_db_poc_<id>_*`` no esquema de ``SERIALIZE_DB_TEST_REDSHIFT_SCHEMA``, apagadas no fim da
sessão pela fixture ``redshift_session``; nada vai ao S3.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass

import pytest
import redshift_connector

from conftest import RedshiftSession, connect_redshift, describe_error, record

pytestmark = [pytest.mark.redshift]

# Quanto a thread de B roda antes de o relatório a chamar de bloqueada, e quanto ela tem para
# terminar depois do COMMIT de A. O teto por comando é a rede de segurança de uma espera sem fim.
BLOCKED_AFTER = 10.0
FINISH_WITHIN = 120.0
STATEMENT_TIMEOUT_MS = 180_000
PARTITION = "2026-08-31"


@dataclass
class Step:
    """Um comando de um participante e o seu desfecho: o tempo, o erro do servidor, as linhas
    afetadas e o resultado."""

    label: str
    sql: str
    seconds: float | None = None
    error: str | None = None
    rowcount: int | None = None
    result: list[tuple] | None = None

    def describe(self) -> str:
        """O desfecho numa linha: ``rótulo: 0.4 s, linhas afetadas: 2`` ou
        ``rótulo: erro em 12.1 s: ...``."""
        if self.seconds is None:
            return f"{self.label}: não terminou"
        if self.error is not None:
            return f"{self.label}: erro em {self.seconds:.1f} s: {self.error}"
        text = f"{self.label}: {self.seconds:.1f} s"
        if self.rowcount is not None and self.rowcount >= 0:
            text += f", linhas afetadas: {self.rowcount}"
        if self.result:
            text += f", {self.result}"
        return text


class Participant:
    """Uma das duas conexões de um cenário e os comandos que ela rodou, em ordem.

    A conexão vem de ``connect_redshift``, com o autocommit ligado e o ``USE`` no banco do
    datashare, e a transação é o ``BEGIN`` explícito da etapa 8. O primeiro erro encerra a
    sequência, porque numa transação abortada cada comando seguinte receberia ``25P02``. O
    participante é um gerenciador de contexto, que fecha a conexão na saída do bloco.
    """

    def __init__(self, name: str) -> None:
        self.name = name
        _, self.connection = connect_redshift()
        self.steps: list[Step] = []
        self.failed = False

        # O SET e o SELECT correm antes do cenário: o primeiro comando de uma sessão custou 10,8 s
        # no ambiente alvo (o USE de 2026-09-21), e nenhum comando do cenário paga esse custo. O pid
        # é o que pg_terminate_backend recebe se a thread ficar presa.
        timeout = self.execute(
            Step("statement_timeout", f"SET statement_timeout TO {STATEMENT_TIMEOUT_MS}")
        )
        record(f"redshift.transactions.statement_timeout.{name}", timeout.describe())
        pid = self.execute(Step("pid", "SELECT pg_backend_pid()"))
        record(f"redshift.transactions.pid.{name}", pid.describe())
        self.pid = pid.result[0][0] if pid.result else None

    def execute(self, step: Step) -> Step:
        """Roda um comando e preenche o desfecho dele, sem levantar o erro do servidor."""
        started = time.perf_counter()
        cursor = self.connection.cursor()
        try:
            cursor.execute(step.sql)
            step.rowcount = cursor.rowcount
            step.result = cursor.fetchall() if cursor.description else None
        except redshift_connector.Error as error:
            # O erro do servidor é a leitura; um erro do próprio teste sobe.
            step.error = describe_error(error)
        step.seconds = time.perf_counter() - started
        return step

    def run(self, steps: list[Step]) -> None:
        """Roda os comandos em ordem, até o fim ou até o primeiro erro."""
        for step in steps:
            if self.failed:
                return
            self.steps.append(step)
            if self.execute(step).error is not None:
                self.failed = True

    def end_transaction(self) -> None:
        """``COMMIT`` quando a sequência correu sem erro; senão ``ROLLBACK``, que solta os
        bloqueios da transação abortada antes de o cenário esperar pelo outro participante."""
        if self.failed:
            self.steps.append(self.execute(Step("ROLLBACK", "ROLLBACK")))
            return
        self.run([commit()])

    def start(self, steps: list[Step]) -> threading.Thread:
        """Roda os comandos numa thread, para o cenário ver se eles esperam pela transação do
        outro."""
        thread = threading.Thread(target=self.run, args=(steps,), daemon=True)
        thread.start()
        return thread

    def current(self) -> str:
        """O rótulo do comando em curso, ou do último que rodou."""
        return self.steps[-1].label if self.steps else "nenhum"

    @property
    def committed(self) -> bool:
        """Verdadeiro quando o ``COMMIT`` rodou sem erro e nenhum comando antes dele falhou."""
        commits = [step for step in self.steps if step.label == "COMMIT"]
        return not self.failed and bool(commits) and commits[-1].error is None

    def report(self, prefix: str) -> None:
        """Registra no relatório cada comando do participante, em ordem."""
        record(f"{prefix}.{self.name}", " | ".join(step.describe() for step in self.steps))

    def close(self) -> None:
        """Fecha a conexão; o fim da sessão desfaz a transação que ficou aberta."""
        self.connection.close()

    def __enter__(self) -> Participant:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()


@dataclass(frozen=True)
class Tables:
    """A tabela de controle e a tabela de dados de cada ambiente num cenário, com os nomes como a
    sessão os cita."""

    control: str
    prod: str
    dev: str


def create_tables(session: RedshiftSession, scenario: str) -> Tables:
    """A tabela de controle de ``serialize_db_publications`` com prod e dev na versão 1, e uma
    tabela de dados por ambiente com a partição publicada."""
    control = session.qualified(session.table(f"{scenario}_controle"))
    session.execute(
        f"CREATE TABLE {control} (table_name VARCHAR(127) NOT NULL, delta_version BIGINT NOT NULL, "
        "execution_id VARCHAR(127) NOT NULL, published_at TIMESTAMP NOT NULL)"
    )
    session.execute(
        f"INSERT INTO {control} VALUES ('prod_x', 1, 'exec-0', getdate()), "
        "('dev_x', 1, 'exec-0', getdate())"
    )

    names = []
    for environment in ("prod", "dev"):
        name = session.qualified(session.table(f"{scenario}_{environment}_x"))
        session.execute(
            f"CREATE TABLE {name} (id BIGINT NOT NULL, execution_id VARCHAR(127) NOT NULL, "
            "data_str VARCHAR(10) NOT NULL)"
        )
        session.execute(
            f"INSERT INTO {name} VALUES (1, 'exec-0', '{PARTITION}'), (2, 'exec-0', '{PARTITION}')"
        )
        names.append(name)

    return Tables(control=control, prod=names[0], dev=names[1])


def begin() -> Step:
    return Step("BEGIN", "BEGIN")


def commit() -> Step:
    return Step("COMMIT", "COMMIT")


def replace_partition(table: str, execution_id: str) -> list[Step]:
    """A troca da partição da etapa 8, com ``INSERT`` no lugar do ``COPY`` na staging: o mesmo lock
    de escrita."""
    return [
        Step("apaga a partição", f"DELETE FROM {table} WHERE data_str = '{PARTITION}'"),
        Step(
            "insere a partição",
            f"INSERT INTO {table} VALUES (1, '{execution_id}', '{PARTITION}'), "
            f"(2, '{execution_id}', '{PARTITION}')",
        ),
    ]


def write_control_row(control: str, table_name: str, version: int, execution_id: str) -> list[Step]:
    """A linha de controle como a etapa 8 a grava, no fim da transação: ``DELETE`` e ``INSERT``."""
    return [
        Step(
            "apaga a linha de controle",
            f"DELETE FROM {control} WHERE table_name = '{table_name}'",
        ),
        Step(
            "insere a linha de controle",
            f"INSERT INTO {control} VALUES ('{table_name}', {version}, '{execution_id}', "
            "getdate())",
        ),
    ]


def waits(thread: threading.Thread) -> bool:
    """Verdadeiro quando a thread ainda roda depois de ``BLOCKED_AFTER`` segundos: o comando dela
    espera pela outra transação."""
    thread.join(BLOCKED_AFTER)
    return thread.is_alive()


def finish(thread: threading.Thread, participant: Participant, session: RedshiftSession,
           prefix: str) -> None:
    """Espera a thread do participante; presa depois do prazo, a sessão dela é encerrada, e o fato
    vai ao relatório."""
    thread.join(FINISH_WITHIN)
    if not thread.is_alive():
        return
    record(
        f"{prefix}.presa",
        f"{participant.name} em '{participant.current()}' depois de {FINISH_WITHIN:.0f} s",
    )
    # Sem o pid, lido na abertura, a sessão presa não tem como ser encerrada.
    if participant.pid is None:
        record(f"{prefix}.encerramento", f"{participant.name} sem pid: sessão não encerrada")
        return
    session.execute(f"SELECT pg_terminate_backend({participant.pid})")
    thread.join(FINISH_WITHIN)


def control_rows(session: RedshiftSession, tables: Tables) -> dict[str, tuple[int, str]]:
    """A versão e a execução de cada linha de controle."""
    rows = session.execute(f"SELECT table_name, delta_version, execution_id FROM {tables.control}")
    return {name: (version, execution_id) for name, version, execution_id in rows}


def partition_origin(session: RedshiftSession, table: str) -> list[str]:
    """As execuções que gravaram as linhas da partição; uma só quando a troca foi atômica."""
    rows = session.execute(
        f"SELECT DISTINCT execution_id FROM {table} WHERE data_str = '{PARTITION}'"
    )
    return sorted(row[0] for row in rows)


def expected_control_row(a: Participant, b: Participant) -> tuple[int, str]:
    """A linha de controle da última transação confirmada: a versão 3 de B, a 2 de A, ou a 1 de
    ``exec-0`` quando nenhuma confirmou."""
    expected = (1, "exec-0")
    if a.committed:
        expected = (2, "exec-a")
    if b.committed:
        expected = (3, "exec-b")
    return expected


def run_scenario(
    session: RedshiftSession, prefix: str, a_steps: list[Step], b_steps: list[Step],
    b_thread_steps: list[Step],
) -> tuple[Participant, Participant]:
    """Roda um cenário e devolve os dois participantes.

    A roda ``a_steps`` e segura a transação aberta; B roda ``b_steps`` e, numa thread,
    ``b_thread_steps``. O relatório registra se B espera A e em que comando, e o fim da transação
    de A, o ``COMMIT`` ou o ``ROLLBACK`` de uma sequência que falhou, solta B.
    """
    with Participant("A") as a, Participant("B") as b:
        a.run(a_steps)
        b.run(b_steps)
        thread = b.start(b_thread_steps)
        b_waits = waits(thread)
        record(f"{prefix}.b_espera_a", f"{b_waits} ({b.current()})")
        a.end_transaction()
        finish(thread, b, session, prefix)
    a.report(prefix)
    b.report(prefix)
    return a, b


def test_isolation_level_readings(redshift_session: RedshiftSession) -> None:
    """O nível de isolamento que o Redshift informa para os bancos que a sessão vê: leitura, nunca
    asserção."""
    session = redshift_session
    rows = session.execute(
        "SELECT database_name, database_type, database_isolation_level "
        "FROM svv_redshift_databases ORDER BY database_name"
    )
    record("redshift.transactions.isolation.svv_redshift_databases", rows)

    # A visão STV pode ser negada a um usuário comum, como stv_slices (42501): a negação também é
    # leitura.
    try:
        record(
            "redshift.transactions.isolation.stv_db_isolation_level",
            session.execute("SELECT * FROM stv_db_isolation_level"),
        )
    except redshift_connector.Error as error:
        # A negação é a leitura.
        record("redshift.transactions.isolation.stv_db_isolation_level", describe_error(error))


def test_writes_to_distinct_tables(redshift_session: RedshiftSession) -> None:
    """A e B escrevem, cada um numa transação, em tabelas distintas: a ingestão em paralelo e
    ``publish_redshift`` com uma conexão por tabela."""
    session = redshift_session
    tables = create_tables(session, "distintas")
    prefix = "redshift.transactions.distintas"
    a, b = run_scenario(
        session,
        prefix,
        [begin(), *replace_partition(tables.prod, "exec-a")],
        [begin()],
        [*replace_partition(tables.dev, "exec-b"), commit()],
    )

    assert partition_origin(session, tables.prod) == (["exec-a"] if a.committed else ["exec-0"])
    assert partition_origin(session, tables.dev) == (["exec-b"] if b.committed else ["exec-0"])


def test_two_environments_write_distinct_control_rows(redshift_session: RedshiftSession) -> None:
    """prod e dev publicam ao mesmo tempo: tabelas de dados distintas e linhas distintas da tabela
    de controle.

    A sequência é a da etapa 8: a versão publicada lida antes da transação, a partição trocada e a
    linha de controle gravada no fim. B troca a sua partição antes do ``COMMIT`` de A, então o
    snapshot de B é anterior a ele; o ``DELETE`` da linha de controle de B é o comando que pode
    esperar pelo lock que A tomou na tabela de controle.
    """
    session = redshift_session
    tables = create_tables(session, "ambientes")
    prefix = "redshift.transactions.ambientes"
    a, b = run_scenario(
        session,
        prefix,
        [
            begin(),
            *replace_partition(tables.prod, "exec-a"),
            *write_control_row(tables.control, "prod_x", 2, "exec-a"),
        ],
        [begin(), *replace_partition(tables.dev, "exec-b")],
        [*write_control_row(tables.control, "dev_x", 2, "exec-b"), commit()],
    )
    rows = control_rows(session, tables)
    record(f"{prefix}.controle", rows)

    # O estado final bate com os desfechos: cada ambiente tem a partição e a linha de quem
    # confirmou.
    assert rows["prod_x"] == ((2, "exec-a") if a.committed else (1, "exec-0"))
    assert rows["dev_x"] == ((2, "exec-b") if b.committed else (1, "exec-0"))
    assert partition_origin(session, tables.prod) == (["exec-a"] if a.committed else ["exec-0"])
    assert partition_origin(session, tables.dev) == (["exec-b"] if b.committed else ["exec-0"])


def test_two_publications_of_the_same_table(redshift_session: RedshiftSession) -> None:
    """Duas publicações de prod da mesma tabela e partição, com a staging de nome fixo da etapa 8.

    O ``CREATE TABLE`` da staging abre o snapshot de cada transação. B cria a mesma staging e troca
    a mesma partição enquanto A segura a sua transação aberta: o relatório diz onde B espera (o
    nome da staging ou o lock da tabela publicada) e se B, com o snapshot anterior ao ``COMMIT`` de
    A, confirma ou é abortado. B publica a versão 3 e A a versão 2.
    """
    session = redshift_session
    tables = create_tables(session, "mesma")
    staging = session.qualified(session.table("mesma_prod_x_staging"))
    prefix = "redshift.transactions.mesma_tabela"

    def publication(execution_id: str, version: int) -> list[Step]:
        return [
            Step(
                "cria a staging",
                f"CREATE TABLE {staging} (id BIGINT, execution_id VARCHAR(127))",
            ),
            *replace_partition(tables.prod, execution_id),
            *write_control_row(tables.control, "prod_x", version, execution_id),
            Step("apaga a staging", f"DROP TABLE {staging}"),
        ]

    a, b = run_scenario(
        session,
        prefix,
        [begin(), *publication("exec-a", 2)],
        [begin()],
        [*publication("exec-b", 3), commit()],
    )
    rows = control_rows(session, tables)
    record(f"{prefix}.controle", rows)
    record(f"{prefix}.particao", partition_origin(session, tables.prod))

    # A última transação confirmada é a dona da partição e da linha de controle.
    expected = expected_control_row(a, b)
    assert rows["prod_x"] == expected
    assert partition_origin(session, tables.prod) == [expected[1]]


def fill_temporary_staging(staging: str, execution_id: str) -> list[Step]:
    """A staging temporária da sessão, sem a coluna de partição, cheia por ``INSERT`` no lugar do
    ``COPY``."""
    return [
        Step(
            "cria a staging temporária",
            f"CREATE TEMP TABLE {staging} (id BIGINT, execution_id VARCHAR(127))",
        ),
        Step(
            "enche a staging",
            f"INSERT INTO {staging} VALUES (1, '{execution_id}'), (2, '{execution_id}')",
        ),
    ]


def swap_from_staging(tables: Tables, staging: str, execution_id: str, version: int) -> list[Step]:
    """A troca da partição de prod a partir da staging e a linha de controle, como a etapa 8 as
    grava."""
    return [
        Step("apaga a partição", f"DELETE FROM {tables.prod} WHERE data_str = '{PARTITION}'"),
        Step(
            "insere a partição da staging",
            f"INSERT INTO {tables.prod} SELECT id, execution_id, '{PARTITION}' FROM {staging}",
        ),
        *write_control_row(tables.control, "prod_x", version, execution_id),
    ]


def assert_publication_matches(session: RedshiftSession, tables: Tables,
                               publisher: Participant) -> None:
    """O estado final bate com o desfecho: a partição e a linha de controle de ``exec-a`` quando a
    transação confirmou, as de ``exec-0`` quando não."""
    expected = (2, "exec-a") if publisher.committed else (1, "exec-0")
    assert control_rows(session, tables)["prod_x"] == expected
    assert partition_origin(session, tables.prod) == [expected[1]]


def test_temporary_staging_filled_inside_the_transaction(
    redshift_session: RedshiftSession,
) -> None:
    """A publicação com a staging temporária criada e cheia dentro da transação.

    Se a tabela temporária contar como o banco local, a transação escreve em dois bancos, e o
    comando que a regra do datashare recusa, ou o ``COMMIT``, aparece no relatório.
    """
    session = redshift_session
    tables = create_tables(session, "temporaria_dentro")
    staging = f"serialize_db_poc_{session.session_id}_temporaria_dentro"
    prefix = "redshift.transactions.temporaria_dentro"
    with Participant("A") as a:
        a.run([
            begin(),
            *fill_temporary_staging(staging, "exec-a"),
            *swap_from_staging(tables, staging, "exec-a", 2),
        ])
        a.end_transaction()
    a.report(prefix)
    record(f"{prefix}.confirmada", a.committed)

    assert_publication_matches(session, tables, a)


def test_temporary_staging_filled_before_the_transaction(
    redshift_session: RedshiftSession,
) -> None:
    """A publicação com a staging temporária cheia antes do ``BEGIN``, em autocommit.

    A transação só escreve no banco do datashare, e a carga da staging, o ``COPY`` da etapa 8, fica
    fora dela e dos locks que ela segura.
    """
    session = redshift_session
    tables = create_tables(session, "temporaria_antes")
    staging = f"serialize_db_poc_{session.session_id}_temporaria_antes"
    prefix = "redshift.transactions.temporaria_antes"
    with Participant("A") as a:
        a.run(fill_temporary_staging(staging, "exec-a"))
        a.run([begin(), *swap_from_staging(tables, staging, "exec-a", 2)])
        a.end_transaction()
    a.report(prefix)
    record(f"{prefix}.confirmada", a.committed)

    assert_publication_matches(session, tables, a)


def test_lock_on_the_control_table(redshift_session: RedshiftSession) -> None:
    """O ``LOCK`` da tabela de controle no início das duas transações, a forma documentada de forçar
    a ordem.

    O ``LOCK`` não está entre os comandos que a escrita por datashare aceita, e a recusa também é a
    leitura. Aceito, B espera no ``LOCK`` até o ``COMMIT`` de A, e o snapshot de B nasce depois
    dele: a versão que B lê dentro da transação é a que A gravou.
    """
    session = redshift_session
    tables = create_tables(session, "lock")
    prefix = "redshift.transactions.lock"
    with Participant("A") as a, Participant("B") as b:
        a.run([begin(), Step("LOCK", f"LOCK {tables.control}")])
        if a.failed:
            a.report(prefix)
            assert control_rows(session, tables)["prod_x"] == (1, "exec-0")
            return
        a.run(
            [
                *replace_partition(tables.prod, "exec-a"),
                *write_control_row(tables.control, "prod_x", 2, "exec-a"),
            ]
        )
        b.run([begin()])
        thread = b.start(
            [
                Step("LOCK", f"LOCK {tables.control}"),
                Step(
                    "lê a versão publicada",
                    f"SELECT delta_version FROM {tables.control} WHERE table_name = 'prod_x'",
                ),
                *replace_partition(tables.prod, "exec-b"),
                *write_control_row(tables.control, "prod_x", 3, "exec-b"),
                commit(),
            ]
        )
        b_waits = waits(thread)
        record(f"{prefix}.b_espera_a", f"{b_waits} ({b.current()})")
        a.end_transaction()
        finish(thread, b, session, prefix)
    a.report(prefix)
    b.report(prefix)
    rows = control_rows(session, tables)
    record(f"{prefix}.controle", rows)

    expected = expected_control_row(a, b)
    assert rows["prod_x"] == expected


def test_conditional_update_of_the_control_row(redshift_session: RedshiftSession) -> None:
    """A linha de controle gravada primeiro, por um ``UPDATE`` condicionado à versão lida antes da
    transação.

    As duas publicações leram a versão 1. O ``UPDATE`` de B espera o lock que o de A tomou; depois
    do ``COMMIT`` de A, o snapshot de B nasce no próprio ``UPDATE``, e o relatório diz se B
    atualizou 0 linhas (viu a versão 2) ou recebeu um erro de serialização. Em qualquer dos dois, a
    publicação de B saberia que outra publicou antes e desfaria a transação.
    """
    session = redshift_session
    tables = create_tables(session, "condicional")
    prefix = "redshift.transactions.condicional"

    def update_if_unchanged(execution_id: str, version: int) -> Step:
        return Step(
            "atualiza a linha de controle se a versão é 1",
            f"UPDATE {tables.control} SET delta_version = {version}, "
            f"execution_id = '{execution_id}', published_at = getdate() "
            "WHERE table_name = 'prod_x' AND delta_version = 1",
        )

    a, b = run_scenario(
        session,
        prefix,
        [begin(), update_if_unchanged("exec-a", 2)],
        [begin()],
        [update_if_unchanged("exec-b", 3)],
    )
    rows = control_rows(session, tables)
    record(f"{prefix}.controle", rows)

    # B nunca confirma aqui: o fechamento da conexão desfaz a transação dele.
    assert rows["prod_x"] == ((2, "exec-a") if a.committed else (1, "exec-0"))
