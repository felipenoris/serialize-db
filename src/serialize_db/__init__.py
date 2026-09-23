"""Banco analítico em tabelas Delta Lake, com o DuckDB e o Redshift como motores.

.. include:: ../../docs/index.md
"""

from serialize_db import audit, cli, delta, engine, errors, execution, schema, sql, storage
from serialize_db.cli import main
from serialize_db.execution import Database, Execution

__all__ = [
    "Database",
    "Execution",
    "audit",
    "cli",
    "delta",
    "engine",
    "errors",
    "execution",
    "main",
    "schema",
    "sql",
    "storage",
]
