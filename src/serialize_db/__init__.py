"""Banco analítico em tabelas Delta Lake, com o DuckDB e o Redshift como motores.

.. include:: ../../docs/index.md
"""

from serialize_db import cli, delta, errors, schema, sql, storage
from serialize_db.cli import main

__all__ = ["cli", "delta", "errors", "main", "schema", "sql", "storage"]
