"""Os motores da execução e a interface comum a eles.

``Engine`` é o protocolo que ``serialize_db.execution`` usa, com a mesma forma nos dois motores:
``serialize_db.engine.duckdb`` (etapa 4) e o motor Redshift (etapa 5). Cada motor guarda uma sessão
por execução sob um ``threading.RLock`` que as primitivas tomam e soltam; ``session()`` dá a
conexão crua com o lock tomado pelo bloco, reentrante na mesma thread, e ``new_session()`` abre uma
sessão a mais sobre o mesmo banco, para o que roda em paralelo.

Os dados cruzam a fronteira em lotes Arrow: ``stream`` devolve um ``BatchStream`` de
``pa.RecordBatch``, e ``loader`` recebe lotes por ``write``; ``query`` devolve a ``pa.Table`` que
``stream`` montaria, e ``load`` entrega ao ``loader`` os lotes de uma ``pa.Table``, de um
``RecordBatch``, de um ``RecordBatchReader`` ou de um iterável.

Exemplo, com o motor DuckDB:

.. code-block:: python

    from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine

    with DuckDBEngine(DuckDBConfig(), "exec-2026-09-05", storage) as engine:
        engine.ingest(Operacao.__table__, uri, version, partitions=["2026-08-31"])
        with engine.stream(sa.select(Operacao)) as stream, \\
                engine.loader(Projetada.__table__) as loader:
            for batch in stream:
                loader.write(project(batch))
"""

from __future__ import annotations

import contextlib
from collections.abc import Collection, Iterable, Iterator, Mapping
from typing import Literal, Protocol, runtime_checkable

import pyarrow as pa
import sqlalchemy as sa

from serialize_db.audit import AuditReport, KeyScope

__all__ = ["BatchStream", "Engine", "ExportMode", "Loader"]

ExportMode = Literal["register", "rewrite"]
"""Como uma partição que o motor gravou entra no Delta: ``register`` registra no log o arquivo que
o motor gravou, depois das conferências da etapa 3; ``rewrite`` grava pelo ``write_deltalake``."""


class BatchStream(Protocol):
    """Os lotes de uma consulta, lidos na ordem dela enquanto ela roda."""

    schema: pa.Schema

    def read_next_batch(self) -> pa.RecordBatch: ...
    def __iter__(self) -> Iterator[pa.RecordBatch]: ...
    def read_all(self) -> pa.Table: ...
    def close(self) -> None: ...
    def __enter__(self) -> BatchStream: ...
    def __exit__(self, *exc: object) -> None: ...
    def __arrow_c_stream__(self, requested_schema: object = None) -> object: ...


class Loader(Protocol):
    """A carga em lotes de uma tabela nova do sandbox, criada no ``close``."""

    rows: int

    def write(self, data: pa.RecordBatch | pa.Table) -> None: ...
    def close(self) -> None: ...
    def __enter__(self) -> Loader: ...
    def __exit__(self, *exc: object) -> None: ...


@runtime_checkable
class Engine(Protocol):
    """A interface dos dois motores; ``Execution`` só depende dela."""

    execution_id: str

    def session(self) -> contextlib.AbstractContextManager[object]: ...
    def new_session(self) -> Engine: ...
    def __enter__(self) -> Engine: ...
    def __exit__(self, *exc: object) -> None: ...
    def ingest(self, table: sa.Table, uri: str, version: int, partitions: list[str] | None = None,
               materialize: bool = False) -> None: ...
    def published(self, table: sa.Table, uri: str, version: int | None) -> sa.FromClause: ...
    def stream(self, statement_or_sql: sa.sql.ClauseElement | str,
               params: Mapping[str, object] | None = None,
               batch_size: int = 100_000) -> BatchStream: ...
    def query(self, statement_or_sql: sa.sql.ClauseElement | str,
              params: Mapping[str, object] | None = None) -> pa.Table: ...
    def loader(self, table: sa.Table, queue_depth: int = 2) -> Loader: ...
    def load(
        self, table: sa.Table,
        data: pa.Table | pa.RecordBatch | pa.RecordBatchReader | Iterable[pa.RecordBatch],
    ) -> int: ...
    def audit(self, table: sa.Table, partitions: list[str] | None, uri: str | None = None,
              version: int | None = None, foreign_keys: bool = False,
              key_scope: KeyScope | None = None,
              referenced: Mapping[str, tuple[str, int]] | None = None) -> AuditReport: ...
    def export_partition(self, table: sa.Table, uri: str, value: str | None,
                         metadata: Mapping[str, str], mode: ExportMode,
                         expected_rows: int | None = None,
                         columns_without_min_max: Collection[str] = ()) -> int: ...
    def cleanup(self) -> None: ...
