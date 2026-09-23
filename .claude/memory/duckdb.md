# DuckDB

Read before code on `engine.duckdb`, `storage.duckdb_setup`, a probe that opens DuckDB, or a query over Parquet or Delta from DuckDB. Each fact ends with the `plan/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

## Ingestion and results

- Arrow is the ingestion path for both engines: 300,000 rows into DuckDB from an Arrow table took
  0.008 s, against 5.2 s for 50,000 rows through `executemany`. `plan/duckdb.md`
- A pandas `object` column of `Decimal` gets its DuckDB type from the values present, not from the
  contract (`plan/duckdb.md` reports a 1,000-value sample giving `DECIMAL(7, 2)`; on 2026-09-19 a
  5,001-row column with the large value last gave `DECIMAL(11,2)` and materialized); casting to Arrow
  with the contract schema first fixes the type. `plan/duckdb.md`, `tests/proof_of_concept/test_duckdb.py`
- `pandas.read_sql` turns `Decimal` into `float` unless `coerce_float=False`, and
  `dtype_backend="pyarrow"` returns `double` for `DECIMAL` and `string` for `DATE`; only the Arrow
  path preserves `decimal128(18, 2)` and `date32`. `plan/duckdb.md`
- `COPY ... FROM` in DuckDB is positional and silently casts convertible types, so type
  enforcement belongs on the metadata, before the load. `plan/parquet.md`
- Constraints cost on load and do not help queries in either engine: a DuckDB load of 300,000 rows
  went from 0.008 s to 0.073 s with a composite primary key, and Redshift keys are informational.
  `plan/schema.md`, `plan/duckdb.md`
- A foreign key needs a primary key or `UNIQUE` constraint on the referenced columns, in the same
  order: `FOREIGN KEY (b, a) REFERENCES alvo (b, a)` against `UNIQUE (a, b)` fails with
  `Binder Error: ... does not have a primary key or unique constraint on the columns b,a`, and a
  unique index as target with `there is no primary key or unique constraint for referenced table`
  (DuckDB 1.5.5, 2026-09-22); `check_models` enforces the rule. `plan/POC.md`, `plan/schema.md`
- `COPY ... (FORMAT parquet, RETURN_STATS)` returns `filename`, `count`, `file_size_bytes`,
  `footer_size_bytes`, `column_statistics` and `partition_keys`; the statistics come keyed by the
  quoted column name, with `column_size_bytes`, `min`, `max`, `null_count`, `num_values` as text,
  plus `has_nan` on a float column holding one. A `DOUBLE` round-trips through `float`, a `VARCHAR`
  is not truncated (200 characters came back whole), and an all-null column carries no `min`/`max`.
  `plan/POC.md`, `scripts/migrate_parquet_to_delta.py`

## Proxy

- DuckDB has `http_proxy`, `http_proxy_username` and `http_proxy_password` and nothing like
  `NO_PROXY`; it reads only the uppercase `HTTP_PROXY` (the lowercase spelling alone does
  nothing), parses it at request time, not at `SET` time, and refuses an address with the
  credentials inside it (`Failed to parse http_proxy ... into a host and port`), which is how a
  corporate proxy usually reaches the environment. `SET http_proxy` overrides the variable and
  takes `host:port` or `http://host:port`; the password goes in without URL-encode and reaches
  the proxy as `Proxy-Authorization: Basic`. The error covers every DuckDB HTTP call, an `httpfs` S3
  `glob` included, not only an extension download. `probelib.duckdb_proxy` does the split for the
  probes and for `prepare_offline.sh`, reading only `HTTP_PROXY`, because taking the address from a
  spelling DuckDB ignores would send through the proxy the traffic that goes direct today; stage 3
  does the same in `duckdb_setup`. `plan/POC.md`

## Python API

- DuckDB 1.5.5 Python API: `.arrow()` returns a `RecordBatchReader`, and `fetch_record_batch()` /
  `fetch_arrow_table()` are deprecated in favour of `to_arrow_reader()` / `to_arrow_table()`; the
  reader returns no further rows, without error, after any other command on the same connection. `pa.Table.from_pylist` wants
  dicts: tuples give an all-null table without error. Only the field metadata key
  `PARQUET:field_id` produces Parquet field ids. DuckDB cannot read `BYTE_STREAM_SPLIT` on a
  DECIMAL column written by PyArrow. `DeltaTable.alter.add_columns` needs `deltalake.schema.Field`,
  not `pyarrow.Field`. A pandas `dict` column enters a DuckDB `JSON` column without a cast.
  `plan/duckdb.md`, `plan/parquet.md`, `plan/redshift.md`, `plan/sqlalchemy.md`
- A DuckDB listing glob over S3 crosses `/` only with `**`: `*` does not, which gave a count of 0 beside boto3's 16
  in `probes/diagnose_aws.py` before the fix (2026-09-20). `probes/README.md`
## Catalog names and limits

- `CREATE TABLE IF NOT EXISTS <name>` guards nothing but the name: over a view it passes and creates
  nothing, and the later `INSERT ... BY NAME` fails with `Catalog Error: <name> is not an table`;
  over a table with other columns it also passes, and `INSERT ... BY NAME` either fails on a missing
  column or fills an extra column with null in silence. `con.register("lote", ...)` occupies a view
  name, listed by `duckdb_views()` and typed `VIEW` by `information_schema.tables`, against
  `BASE TABLE` for tables. A table from `CREATE TABLE AS SELECT` carries no `NOT NULL` (2026-09-22).
  `plan/PLAN-STAGE-4.md`, `plan/POC.md`
- `memory_limit` takes only a value with a unit: `'60%'` and `'60'` are refused with
  `Parser Error: Unknown unit for memory`, `'4.5GiB'` passes. The default is 80% of the memory
  DuckDB detects (14.3 GiB where `os.sysconf` reads 18.0 GiB; 6.1 GiB of the target's 7.6 GiB), so a
  fraction of the machine is Python's arithmetic (2026-09-22). `plan/duckdb.md`
- A `CREATE TEMP TABLE` belongs to the connection that created it: `con.cursor()` is a new
  connection and gets `Catalog Error` on it, a second `connect(path)` in the same process cannot
  see it either, and the same connection object used from another thread can; `duckdb_tables()`
  lists it under catalog `temp`, schema `main`, `temporary` true, only in that connection
  (2026-09-22, DuckDB 1.5.5; the docs: session scoped, only the creating connection, in memory
  with spill to `temp_directory`). The engine keeps one connection per execution under a lock
  (user decision of 2026-09-22), so a temporary table the pipeline creates serves every later
  command; the library's own sandbox tables stay regular, and `schema.ddl(..., temporary=True)`
  exists at the user's request. `plan/POC.md`, `plan/PLAN-STAGE-1.md`, `plan/PLAN-STAGE-4.md`
