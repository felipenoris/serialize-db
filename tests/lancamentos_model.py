"""O modelo das suítes do motor Redshift e da publicação: ``Conta``, ``Lancamento`` e
``Projetado``, e as linhas de uma partição no contrato.

``Lancamento`` é particionado por ``data_base_str`` com a origem ``data_base``, tem uma chave
estrangeira para ``Conta``, uma coluna JSON, uma ``DateTime``, uma ``Numeric(18, 2)`` e a coluna
``to``, palavra reservada; ``Projetado`` é a tabela que o pipeline grava, com as mesmas colunas e
a unicidade de ``codigo``. ``tests/test_engine_redshift.py`` e ``tests/test_publication.py`` o
importam.
"""

from __future__ import annotations

import datetime as dt
import decimal
import json

import pyarrow as pa
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from serialize_db import schema


class Base(DeclarativeBase):
    pass


class Conta(Base):
    __tablename__ = "cad_contas"
    id_conta: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    numero: Mapped[str] = mapped_column(sa.String(20), unique=True)


LANCAMENTO_INFO = {"serialize_db": {"partition_by": ["data_base_str"],
                                    "partition_source": "data_base",
                                    "sort_key": ["data_base", "id_lancamento"]}}


class Lancamento(Base):
    __tablename__ = "cad_lancamentos"
    __table_args__ = {"info": LANCAMENTO_INFO}
    id_lancamento: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    id_conta: Mapped[int] = mapped_column(sa.BigInteger, sa.ForeignKey("cad_contas.id_conta"))
    data_base: Mapped[dt.date] = mapped_column(sa.Date)
    carimbo: Mapped[dt.datetime | None] = mapped_column(sa.DateTime)
    valor: Mapped[float] = mapped_column(sa.Double)
    preco: Mapped[decimal.Decimal | None] = mapped_column(sa.Numeric(18, 2))
    area: Mapped[str | None] = mapped_column(sa.String(10))
    meta: Mapped[dict | None] = mapped_column(sa.JSON)
    to: Mapped[str | None] = mapped_column(sa.String(2))
    codigo: Mapped[str] = mapped_column(sa.String(20))
    data_base_str: Mapped[str] = mapped_column(sa.String(10))


class Projetado(Base):
    __tablename__ = "cad_lancamentos_projetados"
    __table_args__ = (sa.UniqueConstraint("codigo"), {"info": LANCAMENTO_INFO})
    id_lancamento: Mapped[int] = mapped_column(sa.BigInteger, primary_key=True, autoincrement=False)
    id_conta: Mapped[int] = mapped_column(sa.BigInteger)
    data_base: Mapped[dt.date] = mapped_column(sa.Date)
    carimbo: Mapped[dt.datetime | None] = mapped_column(sa.DateTime)
    valor: Mapped[float] = mapped_column(sa.Double)
    preco: Mapped[decimal.Decimal | None] = mapped_column(sa.Numeric(18, 2))
    area: Mapped[str | None] = mapped_column(sa.String(10))
    meta: Mapped[dict | None] = mapped_column(sa.JSON)
    to: Mapped[str | None] = mapped_column(sa.String(2))
    codigo: Mapped[str] = mapped_column(sa.String(20))
    data_base_str: Mapped[str] = mapped_column(sa.String(10))


ENTRIES = Lancamento.__table__
PROJECTED = Projetado.__table__
ACCOUNTS = Conta.__table__
MONTHS = ["2026-07-31", "2026-08-31"]


def account_of(entry_id: int) -> int:
    """A conta do lançamento ``entry_id``: 1, 2 e 3 em rodízio."""
    return 1 + entry_id % 3


def entries(value: str, start: int, count: int, table: sa.Table = ENTRIES,
            valor: list[float] | None = None) -> pa.Table:
    """``count`` lançamentos da partição ``value`` com ids a partir de ``start``, no contrato."""
    ids = list(range(start, start + count))
    day = dt.date.fromisoformat(value)
    data = pa.table({
        "id_lancamento": pa.array(ids, pa.int64()),
        "id_conta": pa.array([account_of(k) for k in ids], pa.int64()),
        "data_base": pa.array([day] * count, pa.date32()),
        "carimbo": pa.array([dt.datetime(day.year, day.month, day.day, 12, 0, 0, k % 1000)
                             for k in ids], pa.timestamp("us")),
        "valor": pa.array(valor if valor is not None else [k / 4 for k in ids], pa.float64()),
        "preco": pa.array([decimal.Decimal(k) / 100 for k in ids], pa.decimal128(18, 2)),
        "area": pa.array(["TI"] * count, pa.string()),
        "meta": pa.array([json.dumps({"k": k}) for k in ids]),
        "to": pa.array(["SP"] * count),
        "codigo": pa.array([f"L{k:06d}" for k in ids]),
        "data_base_str": pa.array([value] * count),
    })
    return schema.cast(data, table)


def accounts(numbers: list[str]) -> pa.Table:
    """Uma conta por número, ids a partir de 1, no contrato."""
    ids = pa.array(range(1, len(numbers) + 1), pa.int64())
    return schema.cast(pa.table({"id_conta": ids, "numero": numbers}), ACCOUNTS)
