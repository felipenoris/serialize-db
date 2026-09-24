"""Privado: o pool de tarefas por tabela que a execução e a publicação usam.

Uma tarefa começa só com um worker livre e nenhuma falha: na primeira falha, as tarefas em curso
terminam, as que não começaram ficam canceladas, e a exceção sobe com o resultado de cada tarefa
numa nota, porque os commits feitos ficam.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Callable, Mapping
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait

# Uma tarefa do pool: o nome da tabela e a função que a processa.
Task = tuple[str, Callable[[], object]]


def _outcome(future: Future) -> str:
    """O resultado de uma tarefa terminada ou cancelada, para a nota da exceção."""
    if future.cancelled():
        return "cancelada"
    error = future.exception()
    if error is None:
        return "concluída"
    return f"falhou: {type(error).__name__}: {error}"


def _raise_with_outcomes(outcomes: Mapping[str, str], error: BaseException) -> None:
    """Relança a exceção com o resultado de cada tabela numa nota: os commits feitos ficam, porque o
    Delta não tem transação entre tabelas."""
    lines = []
    for name, outcome in sorted(outcomes.items()):
        lines.append(f"{name}: {outcome}")
    error.add_note("resultado por tabela: " + "; ".join(lines))
    raise error


def _outcomes_of(futures: Mapping[Future, str]) -> dict[str, str]:
    """O resultado de cada tarefa terminada, pelo nome da tabela."""
    outcomes = {}
    for future, name in futures.items():
        outcomes[name] = _outcome(future)
    return outcomes


@dataclasses.dataclass
class _PoolState:
    """O estado do pool das tabelas: uma tarefa começa só com um worker livre e nenhuma falha,
    então, na primeira falha, o que está em curso termina e o que não começou fica de fora."""

    pool: ThreadPoolExecutor
    workers: int
    running: dict[Future, str] = dataclasses.field(default_factory=dict)
    finished: dict[Future, str] = dataclasses.field(default_factory=dict)
    failure: BaseException | None = None

    def start(self, waiting: list[Task]) -> None:
        """Começa as tarefas que cabem nos workers livres, enquanto não houve falha."""
        while waiting and self.failure is None and len(self.running) < self.workers:
            name, action = waiting.pop(0)
            self.running[self.pool.submit(action)] = name

    def collect(self) -> None:
        """Espera a próxima tarefa terminar e guarda a primeira falha."""
        done, _ = wait(self.running, return_when=FIRST_COMPLETED)
        for future in done:
            self.finished[future] = self.running.pop(future)
            if future.exception() is not None and self.failure is None:
                self.failure = future.exception()


def run_in_pool(tasks: list[Task], max_workers: int) -> dict[str, object]:
    """Roda as tarefas num pool de ``max_workers`` e devolve o resultado de cada uma pelo nome.

    Uma tarefa começa só com um worker livre e nenhuma falha: na primeira falha, as tarefas em
    curso terminam, as que não começaram ficam canceladas, e a exceção sobe com o resultado de cada
    tarefa numa nota. Com um worker por tarefa, todas começam juntas e todas terminam.
    """
    waiting = list(tasks)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        state = _PoolState(pool, max_workers)
        state.start(waiting)
        while state.running:
            state.collect()
            state.start(waiting)
    outcomes = _outcomes_of(state.finished)
    for name, _ in waiting:
        outcomes[name] = "cancelada"
    if state.failure is not None:
        _raise_with_outcomes(outcomes, state.failure)
    results = {}
    for future, name in state.finished.items():
        results[name] = future.result()
    return results
