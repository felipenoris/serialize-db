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
- `interrupt()` called from another thread stops a query stuck in a blocking operator in about
  2 ms with `InterruptException`, and the connection stays usable; an idle `interrupt()` does not
  affect the next command, and one connection's `interrupt()` does not stop a query on its
  `cursor()`. `cursor()` returns in 0.04 ms while the connection runs a query in another thread.
  The DB-API style `qmark` compiled by `duckdb_engine.Dialect(paramstyle="qmark")` runs with the
  list built from `compiled.positiontup`, and a `?` inside a quoted literal passes intact
  (2026-09-23). An `interrupt()` that reaches a read of the Arrow reader surfaces as `OSError:
  INTERRUPT Error: Interrupted!`, one that reaches `execute` as `duckdb.InterruptException`.
  `plan/POC.md`, `tests/proof_of_concept/test_duckdb.py`, `test_sqlalchemy.py`, `test_parallel.py`
- `COPY ... (RETURN_STATS)` on a `DOUBLE` with `NaN` gives the largest number as the maximum and a
  `has_nan` that sees only the last row group: 4,096 rows in two groups of 2,048 gave `false` with
  the `NaN` only in the first group (2026-09-23), while the footer omits min and max of every group
  holding a `NaN`. Infinity comes as the text `inf`. Long text comes truncated to 256 characters,
  the maximum as 255 characters with the last one incremented, above the real value; long
  multibyte text comes without minimum and maximum. `sum(CAST(x AS DECIMAL(38, 6)))` fails with
  `ConversionException` on `NaN` and infinity, also under `FILTER (WHERE isfinite(x))`, because the
  cast runs before the aggregate filter; `CASE WHEN isfinite(x) THEN CAST(...) END` works
  (2026-09-23). `plan/POC.md`, `tests/proof_of_concept/test_duckdb.py`
- `sum(x)` over `DOUBLE` depends on the order and the thread count: 20,000,000 values up to
  1.2e10 with cents gave five results for 1, 2, 4, 8 and 11 threads, the farthest 13,409 from the
  exact sum, and ascending and descending order differed on one thread; `sum(CAST(x AS
  DECIMAL(38, 6)))` gave the exact value on all five, and `fsum` a double near it (2026-09-23).
  `plan/POC.md`, `tests/proof_of_concept/test_duckdb.py`

- `preserve_insertion_order = false`, the engine's setting, makes a selective scan hand out its
  first batch only at the end: `WHERE id < 150000 OR md5(id::VARCHAR) = 'x'` over 20,000,000 rows
  gave the first batch at 1.093 s of 1.093 s with two threads, against 0.373 s of 1.108 s with the
  order kept; a one-partition filter (1/12, contiguous) gave it in 2 to 3 ms in every setting, and a
  17,000,000-row `CREATE TABLE AS` took 0.533 s and 802 MB above base without the order against
  0.908 s and 876 MB with it (2026-09-23). A query without `ORDER BY` comes in arbitrary order.
  `to_arrow_table()` of a `CREATE` or `INSERT` gives a `Count` table, of `SET` or `DROP` a `Success`
  table; `cursor()` of a cursor opens another connection to the database; a file database keeps a
  `.wal` beside it while open. `plan/POC.md`, `plan/PLAN-STAGE-4.md`

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
- `threads` belongs to the database instance, not the connection (2026-09-23): `duckdb_settings()`
  gives `threads` and `external_threads` scope `GLOBAL`, `SET SESSION threads` fails with `option
  "threads" cannot be set locally`, and `SET threads` changes the pool at runtime for every cursor.
  The thread that calls each connection also executes its query beside the pool: with `threads = 1`,
  four cursors in four Python threads scanned 100,000,000 rows in 0.639 s against 0.608 s for one;
  with `threads` at the 11 cores, 0.313 s against 0.079 s, their time in series, and 22 threads
  changed nothing. `range()` generates rows on one thread whatever `threads` says. The docs
  ("How to Tune Workloads") parallelize by 122,880-row row groups and advise 2 to 5 times the cores
  for remote files, whose I/O is synchronous, one HTTP request per thread. `plan/duckdb.md`,
  `tests/proof_of_concept/test_concurrency.py`
- DuckDB's Parquet reader prunes a row group by the footer maximum that pyarrow
  (`parquet-cpp-arrow 25.0.1`) and delta-rs (`parquet-rs 59.3.0`) write without the `NaN`, and loses
  the row DuckDB orders above every number: `read_parquet ... WHERE valor > 3` gave 3 rows of 4,
  and `valor + 0 > 3`, which does not reach the reader, gave 4 (2026-09-23). It is
  duckdb/duckdb#25521 (open, `reproduced`, also `!=` losing and `<=` adding the row). DuckDB's own
  writer omits min and max of a group with `NaN`, and its native storage keeps `NaN` as the segment
  maximum; both answer 4. The documented deviation is from IEEE 754, not from Parquet: `NaN` equals
  `NaN` and is greater than every float. `plan/POC.md`, `plan/delta.md`,
  `tests/proof_of_concept/test_duckdb.py`
- DuckDB 1.5.5 has `enable_external_file_cache` on by default, `GLOBAL` in scope (a `SET` on the
  connection holds for its cursors), and `duckdb_external_file_cache()` lists what it holds: on the
  moto stand-in the first `delta_scan` of a file made 3 `GET` requests of the Parquet file, the
  second and third none, and a read with the cache off 3 again (2026-09-24). A best of three in one
  process measures the cache, not S3: the threads probe of 2026-09-23 read the 393 MB partition in
  4.1 s with 4 threads and 1.9 s to 2.1 s with 8 to 20 in the first repetition, and 1.16 s in every
  later one. Materializing it into the engine's file database took 12.7 s with 4 threads and got
  slower above the cores, with the peak growing from 1,590 MB to 3,125 MB. `plan/POC.md`,
  `probes/duckdb_threads.py`
- A sorted write goes past `memory_limit`: in a 4 vCPU and 16,095 MB container (default limit
  10.6 GiB), a synthetic 52,654,607-row partition with the `cad_lancamentos` source schema took
  50.2 s and 11,966 MB in the migration's `register` sorted variant, 14.0 s and 4,517 MB unsorted,
  and the sorted load 35.1 s with 12,250 MB; a plain sorted `COPY` of it peaked at 10,196 MB under a
  12.3 GiB limit and 7,432 MB under 6 GiB, both in about 17 s (2026-09-24). `plan/POC.md`,
  `plan/PLAN-STAGE-7.md`
- Memory and threads against the environment (2026-09-24): DuckDB reads the cgroup memory limit
  (`memory.limit_in_bytes` in v1, `memory.max` in v2) and the CPU quota (`cpu.cfs_quota_us` over
  `cpu.cfs_period_us`, or `cpu.max`, rounded up) since 1.3 (duckdb/duckdb#16608, merged
  2025-03-14); 1.1.3 read the host's memory in a container (duckdb/duckdb#15080). In the session
  container, with a cgroup v1 limit of 14,345,912,320 bytes in the process folder, DuckDB 1.5.5
  defaulted to `memory_limit` 10.6 GiB (80% of it) and 4 threads. The 1.4 out-of-memory guide
  separates DuckDB's `OutOfMemoryException` (`failed to pin block of size ...`) from the process
  killed by the OS (`Killed`), and for the latter sets `memory_limit` at 50% to 60% of memory,
  because some operations bypass the buffer manager; ART indexes are not buffer-managed, and
  `list()` and `string_agg()` do not spill. The environment guide asks at least 125 MB per thread
  and 1 to 4 GB per thread (1 to 2 for aggregations, 3 to 4 for joins). The sort rewritten in 1.4
  (blog of 2025-09-24) spills sorted runs page by page into the spillable page layout of the hash
  join and aggregation and merges them k-way. DuckDB returns a connection's memory to the OS only on
  `close`: a sorted `CREATE TABLE AS` of 20,000,000 rows from a local Parquet file left RSS at
  1,188 MB (from 191 MB), still 1,188 MB after `DROP TABLE`, and 208 MB after `close`; the external
  file cache held 1,042 entries and 271,906 bytes after reading that local file. `memory_limit`
  takes `'6771MiB'` and `'7516192768B'`, shown as `6.6 GiB` and `7.0 GiB`; `'768MiB'` shows as
  `768.0 MiB`. `plan/duckdb.md`, `plan/POC.md`, `src/serialize_db/resources.py`
