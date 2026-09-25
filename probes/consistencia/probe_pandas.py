"""A fronteira do pandas que um cliente atravessa: um ``DataFrame`` com os tipos padrão do
pandas 3 (``Int16``, ``Int32``, ``boolean``, ``float64``, ``Decimal`` em ``object``,
``datetime64[us]``, ``datetime64[us, UTC]``, ``str``) por ``pa.Table.from_pandas`` e
``schema.cast`` no ``load``, de volta por ``query`` e ``to_pandas``; o ``NaN`` que vira nulo em
``from_pandas`` e as recusas de ``cast`` são leituras.

.. code-block:: shell

    SERIALIZE_DB_TEST_LOCAL_ROOT=$HOME/serialize-db-local \\
        .venv/bin/python probes/consistencia/probe_pandas.py
"""
from __future__ import annotations

import datetime
import decimal
import json
import uuid

import pandas as pd
import pyarrow as pa
import sqlalchemy as sa

from consistency_lib import (TUDO, compare, finish, print_notes, probe_folder, report,
                             to_contract)
from serialize_db import schema
from serialize_db.engine.duckdb import DuckDBConfig, DuckDBEngine
from serialize_db.errors import ContractError
from serialize_db.storage import Storage

COUNT = 8
NOTES: set[str] = set()
FLOATS = [0.0, -0.0, float("nan"), None, 1.5, float("inf"), float("-inf"), 2.675]


def client_frame() -> pd.DataFrame:
    """O ``DataFrame`` do cliente, uma linha por valor de borda de cada tipo."""
    stamps = ["2026-09-25 12:00:00.123456", None, "1970-01-01", "2262-04-11 23:47:16.854775",
              "2000-02-29 23:59:59.999999", "1900-01-01", "2026-01-01", "2026-12-31"]
    stamps_tz = ["2026-09-25 12:00:00.123456+00:00", None, "1970-01-01T00:00:00+00:00",
                 "2026-09-25 09:00:00-03:00", "2000-02-29 23:59:59.999999+00:00",
                 "1900-01-01T00:00:00+00:00", "2026-01-01T00:00:00+00:00",
                 "2026-12-31T00:00:00+00:00"]
    return pd.DataFrame({
        "id": pd.Series(range(1, COUNT + 1), dtype="int64"),
        "pequeno": pd.Series([1, None, -32768, 32767, 0, None, 5, 6], dtype="Int16"),
        "inteiro": pd.Series([1, None, -2147483648, 2147483647, 0, None, 5, 6], dtype="Int32"),
        "flag": pd.Series([True, None, False, True, None, False, True, None], dtype="boolean"),
        "valor": pd.Series(FLOATS, dtype="float64"),
        "preco": [decimal.Decimal("1.10"), None, decimal.Decimal("-0.01"),
                  decimal.Decimal("9999999999999999.99"), decimal.Decimal("0"),
                  decimal.Decimal("1"), decimal.Decimal("2.5"), decimal.Decimal("3.25")],
        "grande": [decimal.Decimal("1.0000000001"), None, decimal.Decimal("-1"),
                   decimal.Decimal("0.5"), decimal.Decimal("7"), decimal.Decimal("8"),
                   decimal.Decimal("9"), decimal.Decimal("10")],
        "data": [datetime.date(2026, 8, 31)] * COUNT,
        "carimbo": pd.to_datetime(stamps, format="ISO8601"),
        "carimbo_tz": pd.to_datetime(stamps_tz, utc=True, format="ISO8601"),
        "texto": pd.Series(["a", None, "ção", "😀", "z" * 50, "", "quote'", "back\\slash"],
                           dtype="str"),
        "longo": pd.Series(["L" * 3000, None, "", "x", "y", "z", "w", "v"], dtype="str"),
        "ident": [str(uuid.UUID(int=i)) if i % 2 else None for i in range(COUNT)],
        "doc": [json.dumps({"k": i}) if i % 3 else None for i in range(COUNT)],
        "data_str": ["2026-08-31"] * COUNT,
    })


def dtypes_of(frame: pd.DataFrame) -> dict[str, str]:
    return {name: str(dtype) for name, dtype in frame.dtypes.items()}


def check_round_trip(engine: DuckDBEngine, frame: pd.DataFrame) -> None:
    """Seção P: o ``DataFrame`` por ``from_pandas`` e ``cast`` no ``load``, de volta por
    ``query`` igual ao que entrou, e o ``to_pandas`` com e sem ``types_mapper`` como leitura."""
    print("   dtypes do pandas:", dtypes_of(frame))
    table = pa.Table.from_pandas(frame, preserve_index=False)
    print("   tipos de from_pandas:", {f.name: str(f.type) for f in table.schema})
    cast = schema.cast(table, TUDO)
    changed = {}
    for field in cast.schema:
        raw = str(table.schema.field(field.name).type)
        if raw != str(field.type):
            changed[field.name] = f"{raw} -> {field.type}"
    print("   tipos mudados por cast:", changed)
    values_in = cast.column("valor").to_pylist()
    print("   valor depois de from_pandas e cast:", values_in)
    print(f"   leitura: o NaN virou nulo: {values_in[2] is None}; o -0.0 ficou: "
          f"{str(values_in[1]) == '-0.0'}")
    direct = pa.array(FLOATS, pa.float64()).to_pylist()
    print(f"   leitura: pa.array direto guarda o NaN: {direct[2] != direct[2]}")
    engine.load(TUDO, cast)
    found = engine.query(sa.select(TUDO).order_by(TUDO.c.id))
    problems = compare(cast, to_contract(found, TUDO, NOTES), key="id", label="query")
    with_arrow_types = found.to_pandas(types_mapper=pd.ArrowDtype)
    print("   dtypes de to_pandas(types_mapper=pd.ArrowDtype):", dtypes_of(with_arrow_types))
    print("   valor:", with_arrow_types["valor"].tolist())
    print("   pequeno:", with_arrow_types["pequeno"].tolist())
    plain = found.to_pandas()
    print("   dtypes de to_pandas():", dtypes_of(plain))
    print("   valor:", plain["valor"].tolist())
    print("   pequeno:", plain["pequeno"].tolist())
    print("   flag:", plain["flag"].tolist())
    print("   carimbo:", plain["carimbo"].tolist()[:3])
    report("P a ida e volta pelo pandas: query igual ao cast da entrada", problems)


def print_refusals(frame: pd.DataFrame) -> None:
    """As recusas de ``cast`` na fronteira, como leitura: nanossegundos, escala a mais, instante
    sem fuso em coluna com fuso, float em ``Numeric``, texto acima do limite."""
    cases = {
        "nanossegundos": frame.assign(
            carimbo=pd.to_datetime(["2026-09-25 12:00:00.123456789"] * COUNT)),
        "escala 3 em Numeric(18, 2)": frame.assign(preco=[decimal.Decimal("1.123")] * COUNT),
        "sem fuso na coluna com fuso": frame.assign(
            carimbo_tz=pd.to_datetime(["2026-09-25 12:00:00"] * COUNT)),
        "float 1.125 em preco": frame.assign(preco=[1.125] * COUNT),
        "float 1.10 em preco": frame.assign(preco=[1.10] * COUNT),
        "51 bytes em String(50)": frame.assign(texto=["z" * 51] * COUNT),
        "nanossegundos zerados": frame.assign(
            carimbo=pd.to_datetime(["2026-09-25 12:00:00.123456000"] * COUNT)),
    }
    for label, variant in cases.items():
        try:
            result = schema.cast(pa.Table.from_pandas(variant, preserve_index=False), TUDO)
            print(f"   {label}: aceito; preco={result.column('preco')[0]} "
                  f"carimbo={result.column('carimbo')[0]}")
        except ContractError as error:
            print(f"   {label}: ContractError: {str(error)[:90]}")


def main() -> None:
    folder = probe_folder("pandas")
    storage = Storage.for_uri(str(folder / "delta"))
    config = DuckDBConfig(temp_directory=str(folder / "sandbox"))
    engine = DuckDBEngine(config, "exec-pandas", storage)
    frame = client_frame()
    try:
        check_round_trip(engine, frame)
    finally:
        engine.cleanup()
    print_refusals(frame)
    print_notes(NOTES)
    finish(folder)


if __name__ == "__main__":
    main()
