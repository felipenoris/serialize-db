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
