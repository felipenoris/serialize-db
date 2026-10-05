# DuckDB

Read before code on `engine.duckdb`, `storage.duckdb_setup`, a probe that opens DuckDB, or a query over Parquet or Delta from DuckDB. Each fact ends with the `plan/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

## Ingestion and results

- Arrow is the ingestion path for both engines: 300,000 rows into DuckDB from an Arrow table took
  0.008 s, against 5.2 s for 50,000 rows through `executemany`. `docs/tecnologias.md` (DuckDB)
- A pandas `object` column of `Decimal` gets its DuckDB type from the values present, not from the
  contract (the DuckDB section of `docs/tecnologias.md` reports a 1,000-value sample giving `DECIMAL(7, 2)`; on 2026-09-19 a
  5,001-row column with the large value last gave `DECIMAL(11,2)` and materialized); casting to Arrow
  with the contract schema first fixes the type. `docs/tecnologias.md` (DuckDB), `tests/proof_of_concept/test_duckdb.py`
- `pandas.read_sql` turns `Decimal` into `float` unless `coerce_float=False`, and
  `dtype_backend="pyarrow"` returns `double` for `DECIMAL` and `string` for `DATE`; only the Arrow
  path preserves `decimal128(18, 2)` and `date32`. `docs/tecnologias.md` (DuckDB)
- `COPY ... FROM` in DuckDB is positional and silently casts convertible types, so type
  enforcement belongs on the metadata, before the load. `docs/tecnologias.md` (Parquet)
- Constraints cost on load and do not help queries in either engine: a DuckDB load of 300,000 rows
  went from 0.008 s to 0.073 s with a composite primary key, and Redshift keys are informational.
  `plan/schema.md`, `docs/tecnologias.md` (DuckDB)
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
- On Windows, the `filename` of a `COPY ... PARTITION_BY ... RETURN_STATS` joins the partition
  folders to the destination with `\` (`D:/.../reescrita\data_str=2026-07-31\data_0.parquet`,
  DuckDB 1.5.5, 2026-10-01), while a `COPY` without partitions returns the destination as given;
  `delta._return_stats_path` turns `\` into `/` for the registration and the export.
  `plan/POC.md`

- A view over `delta_scan(uri, version := v)` binds at `CREATE VIEW`: it read the log (one file
  open, 8.7 ms, no Parquet) and a missing version failed there with `IOException` (`LogSegment
  end version 4 not the same as the specified end version 9`). A filter through the view prunes
  as on `delta_scan` itself: `=`, `BETWEEN` and a one-value `IN` open only their partition
  folders, a two-value `IN` opens every folder, and the `IN` beside the range opens the range.
  An old version exposes its own columns: a column added later is `BinderException`. `BEGIN`,
  `DROP VIEW`, `CREATE TABLE ... AS SELECT` swaps a view for a table, the `ROLLBACK` brings the
  view back, and two swaps in parallel cursors of one connection ran without conflict (DuckDB
  1.5.5, local folder, 2026-09-24). `plan/POC.md`, `plan/PLAN-STAGE-10.md`

- A view whose definition holds the `ingest` partition filter (`BETWEEN` from the lowest to the
  highest value beside the `IN`) prunes like `ingest`: with no client filter it opened the
  range's folders; with `data = '2026-08-31'`, only that folder; a value inside the range but
  outside the `IN` opened its folder and returned 0 rows, and a value outside the range opened
  none. `CREATE TABLE ... AS SELECT *` over the view opened the range's folders (DuckDB 1.5.5,
  deltalake 1.6.4, a four-partition table written by `write_deltalake`, local folder,
  2026-09-24). `plan/POC.md`

- `COPY ... PARTITION_BY` keeps at most `partitioned_write_max_open_files` files open (100 in
  DuckDB 1.5.5) and opens a new file for a partition whose file it closed: with the partitions'
  rows interleaved, 150 partitions of 20,000 rows gave up to 8, 16 and 25 files per partition with
  4, 16 and 32 threads, while 3 partitions of 5,000,000 rows and 40 of 1,000,000 gave one file each;
  `delta.rewrite` over 150 partitions of three files each wrote 254 files with 4 threads and 222
  with 16, one partition in three files (2026-09-25). `plan/POC.md`, `plan/PLAN-STAGE-9.md`

- `COPY ... (FORMAT parquet)` writes a `DOUBLE` column with a dictionary (`PLAIN_DICTIONARY` in
  the footer) whose key treats `-0.0` and `0.0` as equal: every zero of the row group comes out
  with the sign of the first one written (2,000 alternating rows by `id` all `0.0`, by `id DESC`
  all `-0.0`); 4 rows go `PLAIN` and keep the sign, as does `DICTIONARY_SIZE_LIMIT 0`;
  `write_deltalake`, `pq.write_table` and Arrow IPC keep it (DuckDB 1.5.5, 2026-09-25).
  `plan/POC.md`, `.claude/memory/OPEN_QUESTIONS.md`
- `CAST(x AS NUMERIC(38, 6))` fails with `ConversionException` from `1e32` up (`1e31` passes),
  and `sum` of that decimal overflows with `OutOfRangeException` past about 1e32 (20 rows of
  `1e31`, 20,000 of `1e28`): the audit's control total of a `Double` dies on such values
  (2026-09-25). `plan/POC.md`, `.claude/memory/OPEN_QUESTIONS.md`
- A `TIMESTAMPTZ` result reaches Arrow as `timestamp[us, tz=<session TimeZone>]`: `Etc/UTC` in
  this container (no `TZ`), `America/Sao_Paulo` after `SET TimeZone`, the same instant shown in
  that zone; the Parquet file `COPY` writes carries `tz=UTC` either way (2026-09-25).
  `docs/index.md`, `plan/POC.md`

- DuckDB 1.5.5 has no nested transaction: `BEGIN TRANSACTION` inside an open one is refused and
  aborts it, and the `COMMIT` of the aborted transaction returned without error and without its
  work; after a `ROLLBACK`, the `COMMIT` fails with `cannot commit - no transaction is active`, which
  is what the engine's materialized `ingest` gives a client transaction since 2026-09-28. A
  transaction's snapshot is fixed at its first command that reads or changes the database, not at
  `BEGIN`: after a bare `BEGIN` or a `SELECT 1` it still sees a table another cursor creates, after
  a read it does not; a `CatalogException` inside a transaction does not abort it (2026-09-28).
  `plan/PLAN-STAGE-4.md`, `plan/POC.md`
- `read_parquet('<tabela>/*/*.parquet', hive_partitioning = true)` fails with `Hive partition
  mismatch` when the table folder holds a subfolder outside the Hive pattern, such as `backup/`,
  and a folder such as `data_str=2026 Q1` enters as a partition; `import_report` reads the folders
  `discover_partitions` returns, one by one, since 2026-09-28. `plan/PLAN-STAGE-7.md`

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
  `docs/tecnologias.md` (DuckDB, Parquet, Redshift, SQLAlchemy)
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
- A query error in the middle of a stream can reach the Arrow reader as `OSError: INTERRUPT Error:
  Interrupted!` with no `interrupt()` call: `test_stream_delivers_each_batch_while_the_query_runs`
  failed so in 3 of 21 runs on 2026-09-28 (4 vCPUs, `duckdb` 1.5.5), once in the budget of 10,000
  bytes with a pytest plugin reading no engine `interrupt()`. In the source of revision
  `d8cdaa33fd`, `Executor::PushError` sets `context.interrupted` to stop the other tasks, and
  `SimpleBufferedData::ExecuteTaskInternal` throws `InterruptException` on that flag before asking
  the executor for the stored error [inferred]; 210 plain-DuckDB reads and 240 engine reads of the
  same query never reproduced it. `ClientContext::ExecuteTaskInternal` swaps a worker's interrupt
  for the stored error, and the buffered-data check runs before that call, outside the swap. The
  user chose (2026-09-28) one thread for the test's error case and a warning in the `stream`
  docstrings. `plan/POC.md`, `plan/PLAN-STAGE-4.md`
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
- A secret the `httpfs` refreshes on an expired key lives in the transaction of the query that
  refreshed it: `fetchall()` ends the query and keeps the new key; after `fetchone()` the query
  stays open, the next statement on the connection rolls it back with the refresh, and
  `duckdb_secrets()` shows the old key again (DuckDB 1.5.5, four rounds per variant, with and
  without a parameter in the secrets query, 2026-09-25). `plan/POC.md`
- A secret belongs to the database instance: the cursors of `cursor()` see the one another
  recreates, and eight cursors running `CREATE OR REPLACE SECRET` at once, unlocked, got `Catalog
  write-write conflict on alter with "serialize_db_s3"` in 1,249 of 1,600 attempts; the engine
  shares one secret lock among the sessions of a database. For a secret with an explicit key
  (`provider=config`), `duckdb_secrets()` shows `key_id` unredacted and `secret` and
  `session_token` redacted (DuckDB 1.5.5, 2026-09-25). `plan/POC.md`, `plan/PLAN-STAGE-4.md`

## Catalog names and limits

- `CREATE TABLE IF NOT EXISTS <name>` guards nothing but the name: over a view it passes and creates
  nothing, and the later `INSERT ... BY NAME` fails with `Catalog Error: <name> is not an table`;
  over a table with other columns it also passes, and `INSERT ... BY NAME` either fails on a missing
  column or fills an extra column with null in silence. `con.register("lote", ...)` occupies a view
  name, listed by `duckdb_views()` and typed `VIEW` by `information_schema.tables`, against
  `BASE TABLE` for tables. A table from `CREATE TABLE AS SELECT` carries no `NOT NULL` (2026-09-22).
  `plan/PLAN-STAGE-4.md`, `plan/POC.md`
- `duckdb_views()` lists the catalog's internal views, 47 in DuckDB 1.5.5, all in the `system`
  database (`information_schema.columns`, `information_schema.tables`, `pg_catalog.pg_class`,
  `main.sqlite_master`); `duckdb_tables()` lists no internal table. A name check over the two
  functions filters `NOT internal`, or a model table named `columns` reads as occupied
  (2026-09-28). `plan/POC.md`
- `memory_limit` takes only a value with a unit: `'60%'` and `'60'` are refused with
  `Parser Error: Unknown unit for memory`, `'4.5GiB'` passes. The default is 80% of the memory
  DuckDB detects (14.3 GiB where `os.sysconf` reads 18.0 GiB; 6.1 GiB of the target's 7.6 GiB), so a
  fraction of the machine is Python's arithmetic (2026-09-22). `docs/tecnologias.md` (DuckDB)
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
  for remote files, whose I/O is synchronous, one HTTP request per thread. `docs/tecnologias.md` (DuckDB),
  `tests/proof_of_concept/test_concurrency.py`
- DuckDB's Parquet reader prunes a row group by the footer maximum that pyarrow
  (`parquet-cpp-arrow 25.0.1`) and delta-rs (`parquet-rs 59.3.0`) write without the `NaN`, and loses
  the row DuckDB orders above every number: `read_parquet ... WHERE valor > 3` gave 3 rows of 4,
  and `valor + 0 > 3`, which does not reach the reader, gave 4 (2026-09-23). It is
  duckdb/duckdb#25521 (open, `reproduced`, also `!=` losing and `<=` adding the row). DuckDB's own
  writer omits min and max of a group with `NaN`, and its native storage keeps `NaN` as the segment
  maximum; both answer 4. The documented deviation is from IEEE 754, not from Parquet: `NaN` equals
  `NaN` and is greater than every float. `plan/POC.md`, `docs/tecnologias.md` (Delta Lake),
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
  `768.0 MiB`. `docs/tecnologias.md` (DuckDB), `plan/POC.md`, `src/serialize_db/resources.py`

- Threads against the machine (target, 2026-09-24, 16 vCPUs of 8 physical cores, external file
  cache off, partition 2026-06-30 of `cad_lancamentos`, 542 MB, 32,218,190 rows): materializing
  into the file database took 7.8 s with 8 threads, 7.0 s with 16, 11.1 s with 32 and 14.2 s to
  17.1 s with 48 to 80, the process peak from 1,880 MB to 6,427 MB; the aggregated S3 read 2.03 s
  with 8, 1.19 s with 16, 0.88 s with 32 and 0.84 s to 0.89 s with 48 to 80; the four tables 17.5 s
  in series and 9.3 s in extra sessions with 16 threads (1.89x), worse with more. `threads` stays
  at the process's CPUs. A sorted `COPY` of the `cad_lancamentos` partitions with 16 threads was
  faster than the unsorted one (10.6 s against 12.5 s for 52,654,607 rows) at the cost of memory
  (15,126 MB against 7,540 MB), with files of the same size, unlike the other tables; with 4 vCPUs
  the sorted synthetic partition took 3.6 times the unsorted. `plan/POC.md`, `plan/PLAN-STAGE-4.md`
- Threads against the machine (target, 2026-09-27, 8 vCPUs with two threads per physical core,
  external file cache off, partition 2026-07-31 of `cad_lancamentos`, 2,331 MB, 141,933,948 rows,
  run beside `probes/credentials.py`): materializing took 52.7 s with 4 threads, 37.2 s with 8,
  34.2 s with 16, 35.4 s with 24 and 36.4 s to 37.8 s with 32 and 40, the process peak from 3,341 MB
  to 6,362 MB (3,879 MB with 8, 4,937 MB with 16); every 16-thread repetition beat every 8-thread
  one, the best by 9% and the median by 5%. The aggregated S3 read took 7.7 s with 8 and 4.0 s with
  24 (1.91x). The four tables (166,708,072 rows) were fastest in series with 8 threads (49.0 s, 5%
  slower with 16) and in extra sessions with 16 (39.1 s, 1.08x over 8, peak 5,326 MB against
  4,584 MB); extra sessions beat the series by 1.09x to 1.44x, against 1.89x on 2026-09-24, when
  `cad_lancamentos` held a smaller share of the rows. `threads` stays at the process's CPUs (user
  instruction of 2026-09-24). `plan/POC.md`, `plan/PLAN-STAGE-4.md`
- Threads against the machine (target, 2026-09-29, the same machine and partition, the
  materialization by the model's DDL and `INSERT ... BY NAME` from `delta_scan` since 2026-09-28,
  the consistency probes running beside the 4- and 8-thread repetitions [inferred from the clock]):
  materializing took 55.3 s with 4 threads, 39.6 s with 8, 35.3 s with 16, 34.9 s with 24 and 36.3 s
  with 32 and 40, the process peak from 3,401 MB to 6,338 MB (3,955 MB with 8, 4,950 MB with 16), 6%
  more than the `CREATE TABLE AS` with 8 threads and 3% with 16; 16 threads beat 8 by 1.12x. The
  aggregated S3 read took 7.7 s with 8 and 4.0 s with 32 (1.92x). The four tables were fastest in
  series with 8 threads (48.8 s, 8% slower with 16) and tied in extra sessions (40.4 s with 8 and
  16, 40.0 s to 42.6 s with 24 to 40); extra sessions beat the series by 1.11x to 1.52x. `threads`
  stays at the process's CPUs. `plan/POC.md`, `plan/PLAN-STAGE-4.md`
