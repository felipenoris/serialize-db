"""serialize-db: banco analítico em tabelas Delta Lake, com DuckDB e Redshift como motores.

Os modelos SQLAlchemy do cliente são o contrato de esquema; ``serialize_db.schema`` deriva deles o
esquema Arrow e Delta, o DDL de cada motor, a conversão dos lotes de dados e a conferência dos
modelos. ``serialize_db.errors`` guarda as exceções, e ``serialize_db.cli`` a linha de comando.
"""

from serialize_db.cli import main

__all__ = ["main"]
