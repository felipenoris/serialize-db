# Types across Parquet, Arrow, Delta and the engines

Read before `cast`, the schema mapping of stage 1, a Parquet footer check or a change in what a writer produces. Each fact ends with the `docs/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

## What each writer produces

- The DuckDB Parquet writer marks every column `optional`, even `NOT NULL`, writes `DECIMAL(18, 2)`
  as `INT64` and writes no page index; PyArrow writes `required` and `FIXED_LEN_BYTE_ARRAY(8)`.
  `docs/parquet.md`
- delta-rs, DuckLake and DuckDB all write `DECIMAL(18, 2)` as `INT64`; PyArrow writes
  `FIXED_LEN_BYTE_ARRAY`. The pending Redshift `COPY` test covers all three. `docs/estrategia.md`
- delta-rs maps the contract types from Arrow as `short`, `integer`, `long`, `boolean`, `double`,
  `decimal(p,s)`, `string`, `date`, `timestamp_ntz` (naive) and `timestamp` (UTC); a naive timestamp
  column raises the protocol to reader 3 / writer 7 with the `timestampNtz` feature, which DuckDB
  reads as `TIMESTAMP`. `docs/schema.md`, `docs/estrategia.md`

## The type contract

- A JSON field is `sa.JSON().with_variant(SUPER(), "redshift")` in the model (DDL `JSON` on DuckDB,
  `SUPER` on Redshift), `string` in Arrow (user decision of 2026-09-20: the `arrow.json` extension
  dtype has no `.str` kernels in pandas and no engine returns it; DuckDB validates on load into a
  `JSON` column with `Malformed JSON`), `string` in Delta (the extension name kept in field metadata
  when an Arrow schema carries it), `JSON` logical type in Parquet written by PyArrow or
  DuckDB and `String` when written by delta-rs; DuckDB reads `delta_scan` JSON as `VARCHAR` and
  validates only on `::JSON`; Arrow and Delta never validate. `docs/schema.md`, `docs/delta.md`

## PyArrow casts and pandas conversions

- `to_pandas(types_mapper=pd.ArrowDtype)` on 300,000 rows took 2.3 ms with every buffer shared, and
  `from_pandas` of that DataFrame 0.6 ms, also shared, with every field back
  nullable; the default `to_pandas()` took 37.6 ms with `Decimal` and `date` objects and int-with-null
  as `float64`. `safe=True` does not report two losses: `double` to `decimal128(18, 2)` rounds the
  exact binary value (`2.675` gives `2.67`; `pc.round` gives `2.68`) and `timestamp` to `date32` drops
  the time; `pc.equal(pc.round(x, 2), x)` finds the representable doubles (1.1 ms per 300,000) and
  the round trip finds the timestamps with a time. `Table.cast` refuses nulls in a non-nullable field
  and wants the same names in the same order; `int64` to `decimal128(18, 2)` needs the detour through
  `(21, 2)`; a `dict` column infers `struct` with the union of keys; pandas 3 `str` gives
  `large_string`. `docs/PLAN.md`, `tests/proof_of_concept/test_pyarrow.py`

## Building batches from rows (2026-09-21)

- A `RecordBatch` built by columns from `fetchmany` tuples (`zip(*rows)`, `pa.array(column,
  type=field.type)`) took 0.03 s for 200,000 rows in four columns against 0.10 s for
  `RecordBatch.from_pylist` of dicts, after the first call paid the lazy import (0.16 s and 0.24 s);
  the Redshift `stream` builds by columns. `RecordBatch.from_arrays(columns, schema=...)` casts each
  column to the schema type and raises `ArrowInvalid` on a lossy decimal rescale, so `cast` wraps it.
  `duckdb_engine` DDL spells `NUMERIC(18, 2)`, `DOUBLE PRECISION` and `TEXT`, which DuckDB records as
  `DECIMAL(18,2)`, `DOUBLE` and `VARCHAR`. A column's default `autoincrement` is the string `"auto"`.
  `docs/POC.md`, `docs/PLAN-STAGE-1.md`, `docs/PLAN-STAGE-5.md`
- `pc.all` over an empty array returns null, so a check written as `not pc.all(...).as_py()` refuses
  an empty column: the reader path of `cast` derives its output schema from
  `reader.schema.empty_table()` and failed on it until the two checks got `min_count=0`, which makes
  the empty column pass (2026-09-21). `docs/PLAN-STAGE-1.md`, `docs/POC.md`
