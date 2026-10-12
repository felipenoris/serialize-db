"""[private] O pool de threads que a execução, a publicação e o leitor usam para tratar várias
tabelas ao mesmo tempo, uma tarefa por tabela.

Uma tarefa só começa quando há um worker livre e nenhuma tarefa falhou. Na primeira falha, as
tarefas em curso terminam, as que não começaram ficam canceladas, e a exceção sobe com o
resultado de cada tarefa numa nota. A nota existe porque os commits das tarefas que terminaram
ficam: o Delta não tem transação entre tabelas.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

__all__: list[str] = []

# Uma tarefa do pool: o nome da tabela e a função que a processa.
Task = tuple[str, Callable[[], object]]


def _outcome(
    future: Future,
) -> str:
    """[private] O resultado de uma tarefa terminada ou cancelada, como texto para a nota da
    exceção: ``cancelada``, ``concluída`` ou ``falhou`` com o tipo e a mensagem do erro."""
    if future.cancelled():
        return "cancelada"
    error = future.exception()
    if error is None:
        return "concluída"
    return f"falhou: {type(error).__name__}: {error}"


def _outcomes_note(
    outcomes: Mapping[str, str],
) -> str:
    """[private] A nota da exceção com o resultado de cada tabela, em ordem de nome. A nota mostra o
    que ficou feito, porque os commits feitos ficam: o Delta não tem transação entre tabelas."""
    lines = []
    for name, outcome in sorted(outcomes.items()):
        lines.append(f"{name}: {outcome}")
    return "resultado por tabela: " + "; ".join(lines)


def _outcomes_of(
    futures: Mapping[Future, str],
) -> dict[str, str]:
    """[private] O resultado de cada tarefa terminada, pelo nome da tabela."""
    outcomes = {}
    for future, name in futures.items():
        outcomes[name] = _outcome(future)
    return outcomes


@dataclasses.dataclass
class _PoolState:
    """[private] O estado do pool das tabelas durante ``run_in_pool``: as tarefas em curso, as
    terminadas e a primeira falha. Uma tarefa começa só com um worker livre e nenhuma falha;
    então, na primeira falha, o que está em curso termina e o que não começou fica de fora."""

    pool: ThreadPoolExecutor
    workers: int
    running: dict[Future, str] = dataclasses.field(default_factory=dict)
    finished: dict[Future, str] = dataclasses.field(default_factory=dict)
    failure: BaseException | None = None

    def start(
        self,
        waiting: list[Task],
    ) -> None:
        """[private] Tira de ``waiting`` e submete ao pool as tarefas que cabem nos workers livres,
        enquanto não houve falha."""
        while waiting and self.failure is None and len(self.running) < self.workers:
            name, action = waiting.pop(0)
            future = self.pool.submit(action)
            self.running[future] = name

    def collect(self) -> None:
        """[private] Espera ao menos uma tarefa em curso terminar, move as terminadas para
        ``finished`` e guarda a primeira falha em ``failure``."""
        done, _ = wait(self.running, return_when=FIRST_COMPLETED)
        for future in done:
            self.finished[future] = self.running.pop(future)
            if future.exception() is not None and self.failure is None:
                self.failure = future.exception()


def run_in_pool(
    tasks: list[Task],
    max_workers: int,
) -> dict[str, object]:
    """[protected] Roda as tarefas num pool de ``max_workers`` threads e devolve o resultado de
    cada uma pelo nome da tabela; para ``serialize_db.execution``, ``serialize_db.publication`` e
    ``serialize_db.reader``.

    Cada tarefa é um par com o nome da tabela e a função sem argumentos que a processa. Uma tarefa
    só começa quando há um worker livre e nenhuma tarefa falhou. Na primeira falha, as tarefas em
    curso terminam, as que não começaram ficam canceladas, e a exceção sobe com o resultado de
    cada tarefa numa nota. Com um worker por tarefa, todas começam juntas e todas terminam.
    """
    waiting = list(tasks)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        state = _PoolState(pool, max_workers)
        state.start(waiting)
        while state.running:
            state.collect()
            state.start(waiting)
    outcomes = _outcomes_of(state.finished)
    # As tarefas que sobraram em waiting nunca começaram e entram na nota como canceladas.
    for name, _ in waiting:
        outcomes[name] = "cancelada"
    if state.failure is not None:
        state.failure.add_note(_outcomes_note(outcomes))
        raise state.failure
    results = {}
    for future, name in state.finished.items():
        results[name] = future.result()
    return results
