# Types across Parquet, Arrow, Delta and the engines

Read before `cast`, the schema mapping of stage 1, a Parquet footer check or a change in what a writer produces. Each fact ends with the `plan/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

## What each writer produces

- The DuckDB Parquet writer marks every column `optional`, even `NOT NULL`, writes `DECIMAL(18, 2)`
  as `INT64` and writes no page index; PyArrow writes `required` and `FIXED_LEN_BYTE_ARRAY(8)`.
  `plan/parquet.md`
- delta-rs, DuckLake and DuckDB all write `DECIMAL(18, 2)` as `INT64`; PyArrow writes
  `FIXED_LEN_BYTE_ARRAY`. The pending Redshift `COPY` test covers all three. `plan/estrategia.md`
- delta-rs maps the contract types from Arrow as `short`, `integer`, `long`, `boolean`, `double`,
  `decimal(p,s)`, `string`, `date`, `timestamp_ntz` (naive) and `timestamp` (UTC); a naive timestamp
  column raises the protocol to reader 3 / writer 7 with the `timestampNtz` feature, which DuckDB
  reads as `TIMESTAMP`. `plan/schema.md`, `plan/estrategia.md`
- In a row group holding a `NaN`, DuckDB writes the float column without min and max; pyarrow and
  delta-rs write both without the `NaN` (2026-09-23). The Parquet spec asks for the latter
  (PARQUET-1222, parquet-format 2.10.0, 2022) and, since PARQUET-2249 (2026-05-26), for a
  `nan_count` even when zero; a reader without `nan_count` must assume `NaN` may be present and
  ignore min and max in a search the `NaN` satisfies. PARQUET-1246 (2018, parquet-mr 1.10.0) was a
  Java reader fix that ignores a min or max that is itself `NaN`, not a rule to omit statistics.
  `plan/POC.md`, `plan/delta.md`
- DuckDB 1.5.5 `COPY ... (FORMAT parquet)` writes format 1.0, SNAPPY and `PLAIN`: `DECIMAL(18, 2)`
  as `INT64` and `DECIMAL(38, 6)` as `FIXED_LEN_BYTE_ARRAY`, `TIMESTAMP` as `INT64` µs, `DATE` as
  `INT32`, `VARCHAR` as `BYTE_ARRAY` `String`, `BOOLEAN`, `DOUBLE`, and `JSON` as `BYTE_ARRAY`
  with the `JSON` logical type, which pyarrow reads as `extension<arrow.json>`; the engine's
  `export_partition` casts a `sa.JSON` column to DuckDB `JSON`, `register_files` accepts the file
  (physical type only), and `delta_scan` reads it as `VARCHAR` text, delta-rs as `string` (probe
  of 2026-09-24). The Redshift `COPY` of a DuckDB-written file has not run in the target.
  `plan/POC.md`, `plan/PLAN-STAGE-8.md`, `plan/OPEN_QUESTIONS.md`

## The type contract

- A JSON field is `sa.JSON().with_variant(SUPER(), "redshift")` in the model (DDL `JSON` on DuckDB,
  `SUPER` on Redshift), `string` in Arrow (user decision of 2026-09-20: the `arrow.json` extension
  dtype has no `.str` kernels in pandas and no engine returns it; DuckDB validates on load into a
  `JSON` column with `Malformed JSON`), `string` in Delta (the extension name kept in field metadata
  when an Arrow schema carries it), `JSON` logical type in Parquet written by PyArrow or
  DuckDB and `String` when written by delta-rs; DuckDB reads `delta_scan` JSON as `VARCHAR` and
  validates only on `::JSON`; Arrow and Delta never validate. `plan/schema.md`, `plan/delta.md`

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
  `large_string`. `plan/PLAN.md`, `tests/proof_of_concept/test_pyarrow.py`

## Building batches from rows (2026-09-21)

- A `RecordBatch` built by columns from `fetchmany` tuples (`zip(*rows)`, `pa.array(column,
  type=field.type)`) took 0.03 s for 200,000 rows in four columns against 0.10 s for
  `RecordBatch.from_pylist` of dicts, after the first call paid the lazy import (0.16 s and 0.24 s);
  the Redshift `stream` builds by columns. `RecordBatch.from_arrays(columns, schema=...)` casts each
  column to the schema type and raises `ArrowInvalid` on a lossy decimal rescale, so `cast` wraps it.
  `duckdb_engine` DDL spells `NUMERIC(18, 2)`, `DOUBLE PRECISION` and `TEXT`, which DuckDB records as
  `DECIMAL(18,2)`, `DOUBLE` and `VARCHAR`. A column's default `autoincrement` is the string `"auto"`.
  `plan/POC.md`, `plan/PLAN-STAGE-1.md`, `plan/PLAN-STAGE-5.md`
- `pc.all` over an empty array returns null, so a check written as `not pc.all(...).as_py()` refuses
  an empty column: the reader path of `cast` derives its output schema from
  `reader.schema.empty_table()` and failed on it until the two checks got `min_count=0`, which makes
  the empty column pass (2026-09-21). `plan/PLAN-STAGE-1.md`, `plan/POC.md`

## Cast input types and errors (2026-09-22)

- `pa.types.is_string` is false for `large_string` (the type of the pandas 3 `str` after
  `from_pandas`), `string_view` and dictionary; `pc.binary_length` has no kernel for `string_view`
  or dictionary. `cast` measures text on the column already converted to `string`.
- A cast with no kernel (`struct` to `int32`, `list` or `bool` to `date32`, `date32` to `int64`)
  raises `ArrowNotImplementedError`, a `NotImplementedError`, not a `ValueError`; `ArrowInvalid`
  is a `ValueError`, and a null in a `nullable=False` field raises a plain `ValueError` from
  `RecordBatch.cast`.
- Integer to `decimal128(p, s)` needs `p` to hold the whole integer type (19 digits plus the scale
  for `int64`, 10 for `int32`) regardless of the values; `cast` goes through `decimal128(38, s)`
  and the second cast checks each value against `p` (`1000` into `(5, 2)`: `Decimal value does not
  fit in precision 5`).
- `timestamp[us, tz=...]` to naive `timestamp[us]` passes with `safe=True` and keeps the UTC
  instant as wall time; naive to tz-aware assumes UTC. `cast` accepts both today (pending decision
  in `plan/OPEN_QUESTIONS.md`). `plan/POC.md`

## Footer statistics and the JSON logical type read by pyarrow 25 (2026-09-24)

- `pyarrow._parquet.Statistics` in 25.0.1 has `has_min_max`, `min`, `max`, `null_count` and no
  `is_min_value_exact`/`is_max_value_exact`, so a string's min and max cannot be told exact from
  truncated; date statistics come back as `datetime.date`, strings as `str`. A Parquet `JSON`
  logical type column reads as `pa.json_(pa.string())` (`extension<arrow.json>`), and
  `RecordBatch.cast` to a `string` field converts it directly; DuckDB's `to_arrow_table()` of a
  `JSON` column gives a plain `string`, and pyarrow writes it as a plain `String`, so the stand-in's
  `UNLOAD` file of a `SUPER` column lacks the logical type the target writes. `plan/POC.md`

## The type table read against the code (2026-09-25)

- `arrow_type` and `sql_type` match by `isinstance`: `Unicode` and `CHAR(n)` (emitted as
  `VARCHAR(n)`) take the `String(n)` row; `Numeric()` is `decimal128(18, 0)`. `Enum` derives from
  `String` and `Numeric(39, s)` made pyarrow raise `ValueError` out of `check_models`; since the
  user's decision of 2026-09-25 `arrow_type` refuses both with `ContractError`, which
  `check_models` lists. `Time` and `REAL` are refused. `plan/POC.md`
- PyArrow's own cast turns the `arrow.uuid` that `pa.array` and `from_pandas` infer from
  `uuid.UUID` into a `string` of the 16 raw bytes: random UUIDs fail with `Invalid UTF8 payload`
  and an all-ASCII one enters as 16 characters. PyArrow 25.0.1 has `pa.UuidType`, no
  `pa.types.is_uuid` and no hex function; `cast(pa.binary(16))` gives the raw bytes of an `Array`
  or a `ChunkedArray`. Since the user's decision of 2026-09-25, `cast` writes the canonical text by
  `bytes.hex` (1.0 s per million values against 3.7 s by `str(uuid.UUID)`) and `cast` and the audit
  refuse `Uuid` text above 36 bytes. A DuckDB native `UUID` column (a table made by SQL with
  `gen_random_uuid()`) refuses `strlen` and `octet_length` with a Binder Error, so the audit
  measures `strlen(CAST(x AS VARCHAR))`; its `COPY` writes `FIXED_LEN_BYTE_ARRAY` with the `UUID`
  logical type (read back as `arrow.uuid`), and a query's Arrow output gives `string`.
  `plan/POC.md`
- `Numeric(10, 12)` and `Numeric(38, -1)` pass `check_models`, and `delta_schema` raises delta-rs's
  generic `Exception`; pending user decision in `plan/OPEN_QUESTIONS.md`. `plan/POC.md`
- A `DateTime` column makes delta-rs create the table at reader 3 / writer 7 with `timestampNtz`;
  without it, 1 / 2. `delta_scan` gives `VARCHAR` for `JSON` (the DDL tables say `JSON`), and
  DuckDB's Arrow output labels `TIMESTAMPTZ` with the session `TimeZone`. DuckDB and delta-rs write
  decimals as `INT32` up to 9 digits, `INT64` up to 18, `FIXED_LEN_BYTE_ARRAY` above. `plan/POC.md`
