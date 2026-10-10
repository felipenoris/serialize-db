# Threads, the GIL and the batch boundary

Read before `stream`, `appender`, `max_workers`, any helper thread, or a change in how batches cross the library's boundary. A fact that a file of the repository details ends with that file, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

- `duckdb` and `redshift_connector` declare DB-API `threadsafety` 1: threads share the module, never a
  connection. DuckDB, delta-rs and PyArrow release the GIL during native work (a Python loop in
  another thread kept 94% to 101% of its rate, 2026-09-20, macOS), so `ThreadPoolExecutor` gives real
  parallelism without Rust: two `write_deltalake` into distinct tables took 0.10 s in two threads
  against 0.21 s in sequence. A DuckDB connection shared by two threads hands one thread the other's
  result without error; `con.cursor()` per thread shares the database, two in-memory `connect()` are
  separate databases, a second `connect(path)` with another config or `read_only` raises
  `ConnectionException`, and `threads` belongs to the instance, not the cursor. Delta readers keep
  the version they loaded while an `append` commits. A native call that releases and reacquires the GIL
  beside a thread running pure Python waits the switch interval per reacquisition: 200 `os.stat` took
  0.3 s against 0.2 ms alone (0.035 s with `sys.setswitchinterval(0.0005)`), and the lazy
  `import pyarrow.dataset` inside the first `pq.read_table` took 15 s against 0.19 s; import at
  startup and keep hot pure-Python loops out of the library's threads (decisions of 2026-09-20,
  `decisions.md`). `tests/proof_of_concept/test_concurrency.py`, `test_parallel.py`
- Rust already enters through the `deltalake` wheel and DuckDB's `delta` extension; an extension of
  the project's own (PyO3, `pyo3-arrow`) pays only when a profile shows a hot Python
  loop that neither SQL nor Polars expresses, contract-by-contract projection rules for example,
  or when a log store beyond what delta-rs offers is needed (assessment of 2026-09-19).
- The client boundary by batches (2026-09-20, macOS arm64, DuckDB 1.5.5 with `threads = 2`): a
  `to_arrow_reader` on its own `cursor()` delivers its query's snapshot while other cursors insert
  into the same table and change the catalog, and closing that cursor mid-stream did not stop it;
  100 `cursor()` plus `close()` took 0.4 ms. 20,000,000 rows: `to_arrow_table` 0.59 s at 567 MB of
  process, `to_arrow_reader(100_000)` 0.54 s at 83 MB, first batch in 3 ms without `ORDER BY` and
  after the whole sort (2.4 s) with it. A prefetch thread hid the client's per-batch work: 1.25x
  with pandas work, 1.55x with a pure-Python loop. Writes of 6,000,000 rows in 100,000-row batches:
  one `INSERT` over a queue-fed reader 0.20 s, one `INSERT` per batch in one transaction on a helper
  thread 0.41 s (about 3.5 ms per statement, nothing visible before `commit`, `rollback` on failure
  leaves 0 rows). DuckDB's `arrow_scan` pulls a registered Python stream through an Arrow readahead
  thread (`BackgroundGenerator`) that ran the generator on another thread, had pulled 5 to 15
  batches when the `INSERT` failed on the first and reached 10 to 20 after the failure.
  `RecordBatchReader.from_batches` does not check its batches: swapped columns entered DuckDB as
  `(4607182418800017408, 5e-324)` for `(1, 1.0)`; `read_all` raises `Schema at index 0 was
  different`; `RecordBatch.cast` refuses what `Table.cast` refuses; `RecordBatchReader.close()` does
  not close a generator-backed reader. `to_batches`/`from_batches`/`RecordBatch.to_pandas(ArrowDtype)`/
  `RecordBatch.from_pandas` share buffers (0.04 ms, 0.003 ms, 1.4 ms, 0.5 ms). A mid-read query
  error reaches Python as `OSError` with DuckDB's message. Objects with `__arrow_c_stream__` are
  accepted by `from_stream`, DuckDB `register` and `write_deltalake`. `docs/tecnologias.md`
  (DuckDB), `tests/proof_of_concept/test_duckdb.py`, `test_pyarrow.py`, `test_parallel.py`
- Both engines keep one session per execution under a `threading.RLock` (user decision of
  2026-09-22, first for Redshift, then for DuckDB the same day, so a temporary table serves every
  later command on both engines); `session()` hands the raw connection to the client with the lock
  held for the block, reentrant in the same thread, and the client never touches the lock. No lock
  is held while client code runs: the session-bound part of each primitive runs in the calling
  thread and never waits for the client. DuckDB `stream` consumes the whole `to_arrow_reader` into
  an Arrow IPC file with LZ4 under the lock (the next command on the connection would empty the
  reader) and the helper reads the file outside the session; `loader` writes an IPC file outside
  the session and runs one `INSERT ... BY NAME` over the file's native reader in `close`. Redshift
  `stream` runs `execute` under the lock in the calling thread (the driver materializes the result)
  and the helper slices `fetchmany`; an `execute` on the helper thread would deadlock against a
  client holding `session()`. Measured on 2026-09-22: the three-stage pipeline over 3,000,000 rows
  on a file database took 0.400 s on the single session against 0.565 s with a cursor per stream
  and loader (in memory, 0.135 s against 0.112 s); Parquet as the intermediate file cost 0.699 s;
  LZ4 IPC for 20,000,000 rows is 162 MB against 478 MB uncompressed; 10,000,000 rows streamed
  through the file peaked at 106 MB against 83 MB direct and 322 MB for the whole table. What the
  single session gives up: four 150,000-row tables ingested by `delta_scan` took 0.061 s in series
  against 0.017 s in four cursors, so `ingest` lost `max_workers`; S3 is unmeasured.
  `publish_redshift` keeps a connection per table, outside the sandbox session. Since then the
  Redshift `stream` goes through `UNLOAD ... PARALLEL OFF`, with the files read by a helper thread,
  and `query` through the cursor (user decision of 2026-09-23), and the DuckDB `loader` is the
  `appender` (user decision of 2026-09-28), with the same IPC file and `INSERT ... BY NAME` at
  `close`. `tests/proof_of_concept/test_parallel.py`
- The DuckDB `stream` writes each batch while the query runs (2026-09-23): a helper thread takes the
  session lock, pulls `to_arrow_reader`, writes each batch to the Arrow IPC spool and counts the
  batches written under a `threading.Condition`; the client reads each written batch in its own
  thread, with timed waits that check the stop event. The IPC stream writer puts the schema in the
  file only with the first batch or at close (`Tried reading schema message, was null or length 0`
  before that), so the stream waits for the first batch or the end before opening the reader. The
  engine records the thread inside `session()`; a `stream` opened there runs the query in the
  calling thread, because a helper would wait for the block. Best of three over 20,000,000 rows on a
  file database, 13,333,333 rows out, `threads = 2`: first batch 0.003 s with a cursor per stream,
  0.472 s with the whole result spooled first, 0.005 s writing batch by batch; with 5 ms of client
  work per batch, 0.842 s, 1.330 s and 0.939 s; the batch-by-batch spool costs about 0.18 s of file
  writing in the query's path. 10,000,000 rows peaked at 94 MB. The loader through its file beat a
  cursor with an `INSERT` per batch while the client produced fast (0.747 s against 1.101 s) and tied
  when client work dominated. `new_session()` is `cursor()` on DuckDB: it sees tables the main
  session committed, refuses its temporary tables with `CatalogException`, and runs while the main
  session is held; four 150,000-row tables entered by `delta_scan` in 0.017 s in four extra
  sessions against 0.066 s in series. `tests/proof_of_concept/test_parallel.py`,
  `tests/proof_of_concept/test_duckdb.py`
- The 2026-09-23 review measured alternatives to the single-session `stream` and `loader` (macOS,
  11 cores, file database, best of three, `threads = 2`, 100,000-row batches). `stream` over
  13,333,333 rows (134 batches), total without work / 5 ms pure Python per batch / pandas: current
  LZ4 spool 0.598 / 0.996 / 0.629 s, uncompressed spool 0.436 / 0.862 / 0.466 s (486 MB against
  178 MB), unbounded memory queue 0.394 / 0.694 / 0.404 s (527 MB peak when the client lags), hybrid
  (memory up to 256 MiB, LZ4 file after) 0.407 / 0.709 / 0.418 s, own cursor 0.399 / 0.673 / 0.408 s,
  sequential 0.399 / 1.068 / 0.555 s. LZ4 on the query path costs 0.2 s and holds the lock that
  long; with pure-Python client work the helper waits the GIL switch interval per reacquisition
  (`sys.setswitchinterval(0.0005)` took the current design to 0.788 s). `loader` over 6,000,000 rows:
  the current IPC LZ4 spool plus one `INSERT` (0.753 s, the `INSERT` 0.660 s at 2 threads and 0.346 s
  at 8) beat raw IPC (0.697 s, 263 MB), Parquet spools (1.18 to 1.29 s), an own cursor inserting per
  batch into a hidden table renamed at close (1.223 s, about 20 ms per batch) and per-batch inserts
  in one transaction (1.327 s). In the three-stage pipeline over 20,000,000 rows, the `CREATE TABLE`
  the stage 4 `loader` runs under the lock at open waited for the whole query of the `stream`
  opened before it: first batch 0.811 s against 0.006 s with the table created at `close`, total
  3.120 s against 2.761 s. `cursor()` returned in 0.04 ms while the connection ran a query, so
  `new_session()` needs no session lock; `interrupt()` from another thread stopped a blocking sort
  in 2 ms and left the connection usable, and one connection's `interrupt()` does not stop a query
  on its cursor. Four 8,000,000-row Delta tables ingested in 1.629 s in extra sessions against
  3.498 s in series with `threads = 2` (1.037 against 1.382 s with 11), peak memory 373 to 514 MB and
  803 to 917 MB; part of the gain is the calling threads added to the pool, which the target's
  2 vCPUs lack. The proposals (table created at close, hybrid stream, `interrupt()`) await the user
  in `.claude/memory/OPEN_QUESTIONS.md`. `tests/proof_of_concept/test_duckdb.py`
- The hybrid `stream` (user decision of 2026-09-23, implemented in `serialize_db.engine.duckdb`)
  keeps batches in a deque while their bytes fit a 64 MiB budget and writes the first batch that
  does not fit, and every later one, to the LZ4 spool; the client drains the deque before reading
  the file, so the order holds. The spool file is born mid-query, after `__del__` may have unlinked
  the path: the first version left an orphan file in three of six runs, and the producer now unlinks
  the file it created when it stops on `stop`. The producer marks the end under the session lock, so
  a command that runs after the stream sees it finished (the same arrangement the `interrupt()`
  proposal needs). Against the current design over 13,333,333 rows at `threads = 2`: 0.622 → 0.411 s
  without work, 0.965 → 0.673 s with 5 ms pure Python per batch, 0.641 → 0.411 s with pandas, and
  1.086 → 0.908 s with a lagging client (96 batches spilled, 297 MB peak; 256 MiB gave 0.839 s at
  522 MB).
- The stage 4 sketches (2026-09-23; `test_parallel.py` held them until the review of the same day
  retired them, user decision, and `DuckDBStream` and `DuckDBLoader` implemented them; the loader
  is `DuckDBAppender` since 2026-09-28, and `create_table` creates the table):
  `BatchStream.close` sets `stop` and,
  under the spool's condition while the query has not marked its end, calls the connection's
  `interrupt()`, which never reaches another command because the producer marks the end under the
  session lock; `SandboxEngine.cleanup` interrupts before taking the lock; `new_session` creates the
  cursor without the lock; `Loader` checks the name through `name_in_use` on a cursor and runs
  `BEGIN`, the `CREATE TABLE`, the `INSERT` and `COMMIT` at `close`, rolling back on failure. The
  close after the first batch of a long scan took 7 to 10 ms, the cleanup of a sort 2 ms, the
  three-stage pipeline 0.365 to 0.370 s against 0.406 to 0.436 s before; with `stream` then `loader`
  in one `with`, the first batch arrives while the query runs. Five runs of the suite were green;
  the orphan-file race and a `__del__` reading a field the failed `__init__` never set appeared only
  on repetition.
- A read racing a `load` fired in a thread and forgotten without `result()` fails with
  `CatalogException: Table with name ... does not exist!` on the main session and on an extra
  session, never reads old rows: the reference `Loader` creates the table only at `close` and
  refuses a taken name, so no sandbox table has a previous state (probe and
  `test_engine_duckdb.py::test_read_during_a_forgotten_load_fails_instead_of_reading_old_rows`, six
  green runs, 2026-09-23, macOS). The table barrier left the plan for that reason. The property is
  lost since 2026-09-28 (user decision, PR #103): the table exists before the `append`, from
  `create_table` or `ingest(materialize=True)`, and a read during an `append` in flight sees the
  table without the new rows
  (`test_engine_duckdb.py::test_read_during_an_append_in_flight_sees_the_table_without_the_new_rows`,
  `.claude/memory/decisions.md`).
- Two writers on one sandbox table entered in both engines in the target on 2026-09-29
  (`probes/consistencia/probe_append_test.py`, `-m redshift` in 41.4 s with 20,000 rows per writer,
  `-m local` in 11.6 s with 200,000): two `append` at once in extra sessions, two on the main
  session in threads (in series under the lock) and an `append` beside an `UPDATE` of the rows
  already written left every row, no repeated id and the sum of `valor`; Redshift refused no pair
  with `1023`, under the snapshot isolation datashare writes require. In the probe's section D,
  DuckDB's `create_table` entered during a 5,000,000-row `stream` in 0.010 s on an extra session and
  0.006 s on the main one, where the main session's `CREATE TABLE` waited for the whole query
  (1.569 s to 1.896 s) locally before PR #103. Since the user's decision of 2026-09-29
  (`decisions.md`), the docstrings of `appender` and `append`, in the protocol and both engines, say
  both writers enter, and `test_two_writers_on_the_same_table_both_enter` checks it in both engines,
  the `close` calls released together by a barrier after the `write`. A scratch pytest plugin that
  refused a `close` with another in flight, dropped its rows, or deleted the table's rows before
  inserting failed it, but the delete passed the extra-sessions section, where both `DELETE`s ran
  together and saw the empty table; on Redshift the refusal and the drop wrap the whole `close`,
  because `_copy_file` already runs under the transaction's lock. The test passed in the target on
  2026-09-29 at 13:31, on DuckDB in the `-m "not redshift"` session with the local root and on
  Redshift in the four sessions that collect it.

- A pool that receives every task at once cannot promise that nothing new starts after the first
  failure: with one worker, the worker took the third table before the main loop saw the second
  one fail, and `shutdown(cancel_futures=True)` came too late; the sketch that was in
  `test_parallel.py` passed only because each task slept 0.5 s. `Execution.publish_delta` submits a
  table only when a worker is free and no failure arrived, and the failure goes up with its own type
  and each table's outcome in a note (`add_note`) (2026-09-23); `run.ingest` uses the same pool
  function (`_run_in_pool`, `serialize_db._pool.run_in_pool` since 2026-09-24) with one worker per
  table, so every table starts at once and all finish.
  Under load, DuckDB can hand a stream's first batch only at the
  end of the query (4.531 s in a three-process reproducer, with the second batch already in memory),
  so a test that closes a stream "mid-query" asserts the thread ended and the session is free, with
  the error null or the interrupt's.
- `max_workers=0` reaches `ThreadPoolExecutor` in `serialize_db._pool.run_in_pool`, whose
  `ValueError: max_workers must be greater than 0` goes up from `run.publish_delta` and
  `publish_redshift`, and `serialize-db publish_redshift --max-workers 0`, a plain `int` option,
  ends in a traceback (code review of 2026-10-01, no decision asked; the `ValueError` read again
  on 2026-10-07). `src/serialize_db/cli.py`
- The memory probes of `tests/proof_of_concept/test_duckdb.py` on Linux x86_64 (2026-09-23, 4 vCPUs,
  Python 3.13.12, DuckDB 1.5.5, PyArrow 25.0.1): a new process's `ru_maxrss` starts at its parent's
  peak on Linux, so the probes read `VmHWM` from `/proc/self/status` there and `ru_maxrss` on macOS,
  import PyArrow before the base and count each scenario from the base after the imports and the
  connection, 92 MB on Linux (Python 9 MB, `import duckdb` 42 MB, the connection 2 MB,
  `import pyarrow` 39 MB, PyArrow's default pool `mimalloc`). 10,000,000 rows: the whole table
  335 MB (242 to 243 MB over the base), the direct reader 102 MB (9 to 10 MB), the spool 130 to
  138 MB (37 to 45 MB, 81 MB of file); PyArrow imported by `to_arrow_reader` mid-query left the
  reader at 103 MB over a 53 MB base. `tests/proof_of_concept/test_duckdb.py`

- `python_rate_during` (the GIL helper of `tests/proof_of_concept/test_concurrency.py`) waits for
  the counter thread's first iteration before timing the action: without the wait, 200 `os.stat`
  finished in 5.4 ms before the thread got the GIL, `beside` came out below `shorter` and
  `test_gil_reacquisition_waits_the_switch_interval` failed once in four sessions on 2026-09-24
  (usually 0.08 s to 0.75 s beside the loop); five runs passed after the wait. It failed again in
  local whole-suite sessions on 2026-09-25 (0.011 s beside the loop against 0.018 s with the
  shorter interval), on 2026-10-03 and on 2026-10-07 (0.005 s against 0.006 s; alone, three runs
  at 0.278 s to 0.444 s against 0.006 s to 0.024 s), and passed alone and in the other sessions.
  In the target the `-m "not redshift"` session of `suite_alvo.sh` runs it, and it passed in the 14
  sessions with a report from 2026-09-24 to 2026-10-09, at 0.381 s to 0.893 s beside the loop
  against 0.012 s to 0.086 s with the shorter interval (`concurrency.gil.os_stat_200` of
  `suite_s3.json`), 0.381 s against 0.012 s on 2 vCPUs on 2026-10-07 and 0.773 s against 0.086 s
  on 4 vCPUs on 2026-10-09; the item is in `.claude/memory/OPEN_QUESTIONS.md`.
- `Storage.write_text(if_match=...)` on a local folder is not atomic between threads either:
  `_replace_local` reads the fingerprint and `os.replace`s without a lock, and eight threads
  adding 50 each with a retry on `ConflictError` kept 107 of 400 (204 conflicts seen,
  2026-09-25), and in the target machine's local folder 79 of 400 (175 conflicts) on 2026-09-26,
  88 (204) on 2026-09-27, 65 (84) on 2026-09-29, 67 (108) on 2026-10-05, 87 (165) on
  2026-10-07 and 81 (151) on 2026-10-09; S3's `IfMatch` is server-side. `.claude/memory/OPEN_QUESTIONS.md`
- `Execution.publish_delta` checks `version_diff` from the pinned version before `reconcile` and
  `export_partition`, and `register_files` (`publish_partition` too) opens the table anew right
  before the commit, so a data commit by another execution on the same partition between the check
  and that open passes without `ExecutionConflict` and the later commit replaces the partition
  silently (2026-09-25: the race of `probes/consistencia/probe_execution.py` under load, then its
  section D deterministically; section D again in the target on 2026-09-26, where section C's race
  took the `ExecutionConflict` path). delta-rs 1.6.6 opened at the pinned version refuses the commit
  after an overwrite of the same partition or a schema change, and passes after another partition, a
  compaction of the same partition or a metadata-only commit. `.claude/memory/OPEN_QUESTIONS.md`
- The threaded APIs against their serial form, 2026-10-04, this container (Linux, 4 vCPUs, 16 GB,
  DuckDB 1.5.5, PyArrow 25.0.1, deltalake 1.6.6, pandas 3.0.6), each measure in a new process, best
  of three, 10,000,000 rows of six columns in a DuckDB file database, 100,000-row batches: `stream`
  beat `query` plus the loop 1.12-1.14x with no client work, 1.56-1.65x with pandas per batch,
  1.70-1.73x with 5 ms of pure Python and 1.90-1.91x with a 5 ms sleep, at 118-134 MB of peak
  against 686-700 MB and the first batch at 0.02 s against 0.8 s. The raw `to_arrow_reader` inside
  `session()` gained 1.01-1.17x, the 5 ms sleep included (1.304 s against 1.321 s), so DuckDB makes
  the next batch only when the client pulls it [inferred] and the gain is the library's helper
  thread; `stream` opened inside `session()` in the same thread lost (0.55-0.69x, first batch at 1.2
  s, the whole query through the spool file). `appender` against all the work then `append`: 1.09x,
  1.52x, 1.13x and 1.19x; `stream` and `appender` in one `with`: 1.07x, 1.47x, 1.20x and 1.24x, with
  40% to 45% less peak. 200 small queries from four threads: 0.377 s on the main session against
  0.375 s serial, 0.170 s with one `new_session()` per thread (2.20x), as SQL text; as Core
  statements, about 2 ms more each in Python (the compile path, no cache) and only 1.20-1.25x; four
  big aggregations, 1.12x. In one warm process the serial `appender` path ran faster (fewer page
  faults) and its pandas gain fell to 1.07-1.23x. Four 5,000,000-row tables in a local Delta folder:
  `run.ingest(*tables)` 1.30x and `reader.materialize(*tables)` 1.56x over one call per table, with
  a 39% to 46% higher peak; `publish_delta(max_workers=4)` 1.02x. A peak read in-process after
  earlier measures misses the memory the process kept: one query read +418 MB first and +205 MB on
  each repeat, and +365 MB with the Arrow pool's `release_unused()` and glibc's `malloc_trim(0)`
  before `/proc/self/clear_refs`. `probes/operacao/probe_parallel_gain.py` repeated the measures in
  the target on 2026-10-05 (next entry). `docs/index.md`
- The threaded APIs against their serial form in the target, 2026-10-05
  (`probes/operacao/probe_parallel_gain.py`, 8 vCPUs, 13.0 GiB available, DuckDB limits 8 threads
  and 6,665 MiB, four 5,000,000-row Delta tables on S3 prepared in 26.4 s, the Redshift serverless
  workgroup, best of three in one process, the peak over the base after `clear_refs`). DuckDB:
  `stream` 1.26x with no work (0.364 s, +2 MB, against 0.459 s, +313 MB) and 1.83x with pandas
  (0.389 s against 0.712 s); `appender` 1.00x and 1.07x; both in one `with` 1.03x and 1.40x
  (2.143 s, +207 MB, against 3.009 s, +628 MB); 200 small Core-statement queries 0.526 s serial,
  0.541 s from four threads on the main session, 0.348 s with one `new_session()` each (1.51x);
  `run.ingest` of the four 2.39x (4.005 s, +1,035 MB, against 9.564 s, +916 MB), `materialize` of
  the four in the Delta reader 2.44x (4.139 s, +991 MB, against 10.097 s, +896 MB) and
  `publish_delta(max_workers=4)` 2.04x (8.951 s against 18.221 s), where the local folder had read
  1.02x. Redshift: `run.ingest` of the four 2.59x (12.536 s against 32.509 s); `stream`, `appender`
  and both 1.01x, 1.07x and 1.04x with no work and 1.00x, 0.96x and 1.00x with pandas, since the
  `UNLOAD` runs whole before the first batch and the `COPY` after the loop, the threaded `stream`
  and both at +79 MB to +129 MB against +243 MB to +457 MB serial, the `appender` at +84 MB against
  +43 MB with no work and +28 MB against +231 MB with pandas; 80 small Core-statement queries
  6.858 s serial (about 86 ms each), 6.746 s on the main session, 2.181 s with one extra session
  each, its opening timed (3.14x); `publish_delta(max_workers=4)` 1.39x (19.131 s against 26.681 s);
  and `publish_redshift` with four workers against one 0.95x (31.521 s against 29.987 s, the
  unpublishing out of the time). The runbook's base publication uses `--max-workers 4` since
  2026-09-26 and has no serial reading in the target. `docs/index.md` ("Multithreading")
- The threaded APIs against their serial form in the target again, 2026-10-07
  (`probes/operacao/probe_parallel_gain.py`, 2 vCPUs, 5.8 GiB available, DuckDB limits 2 threads
  and 2,913 MiB, the four tables prepared in 48.0 s, the same method). DuckDB: `stream` 1.29x with
  no work (0.696 s, +4 MB, against 0.899 s, +340 MB) and 1.44x with pandas (0.886 s against
  1.276 s); `appender` 1.01x and 1.03x; both in one `with` 1.08x (4.582 s, +196 MB, against
  4.952 s, +636 MB) and 1.13x (4.788 s, +342 MB, against 5.430 s, +743 MB); 200 small queries
  0.830 s serial, 0.829 s on the main session, 0.650 s with one `new_session()` each (1.28x);
  `run.ingest` of the four 1.15x (14.727 s, +717 MB, against 16.987 s, +601 MB), `materialize`
  1.15x (14.772 s, +714 MB, against 16.942 s, +604 MB) and `publish_delta(max_workers=4)` 1.71x
  (15.695 s against 26.902 s): with 2 CPUs the serial DuckDB work takes 1.5 to 2 times the 8-vCPU
  time and the pools gain half as much. Redshift: `run.ingest` of the four 2.75x (11.500 s
  against 31.608 s); `stream`, `appender` and both 1.04x, 1.02x and 1.04x with no work and 1.02x,
  1.17x and 1.05x with pandas, the threaded forms at +24 MB to +106 MB against +44 MB to +504 MB
  serial; 80 small queries 5.541 s serial (about 69 ms each), 5.298 s on the main session,
  1.967 s with one extra session each (2.82x); `publish_delta(max_workers=4)` 1.48x (19.951 s
  against 29.608 s); and `publish_redshift` with four workers against one 2.35x (11.819 s against
  27.757 s), where 2026-10-05 had read 0.95x with the serial form at 29.987 s: the 31.521 s of
  that day's parallel form has no cause measured. `docs/index.md` ("Multithreading")
- The same probe again on 2026-10-07 from 18:55 UTC (2 vCPUs, 5.9 GiB available, DuckDB limits
  2 threads and 3,013 MiB, the tables prepared in 49.4 s, `main` at `b6acfa8`). DuckDB: `stream`
  1.25x with no work and 1.42x with pandas, `appender` 1.02x and 1.07x, both 1.07x and 1.12x,
  200 small queries 1.38x with one `new_session()` each, `run.ingest` 1.21x (14.517 s against
  17.616 s), `materialize` 1.16x and `publish_delta(max_workers=4)` 1.61x (16.860 s against
  27.073 s). Redshift: `run.ingest` 2.68x (12.232 s against 32.831 s), `stream` 1.03x,
  `appender` 1.04x and 1.11x, both 1.11x and 1.07x, 80 small queries 3.824 s serial (about 48 ms
  each, 69 ms at 15:48), 3.774 s on the main session and 1.855 s with one extra session each
  (2.06x), `publish_delta(max_workers=4)` 1.43x (19.712 s against 28.188 s) and
  `publish_redshift` with four workers 2.40x (11.133 s against 26.750 s), its third repetition at
  33.135 s, above every serial repetition (26.750 s, 27.448 s and 29.068 s): the four-connection
  publication varied three times between repetitions of one run, like the 31.521 s of
  2026-10-05, with no server reading to name the cause. `docs/index.md` ("Multithreading")
- The same probe on 2026-10-09 from 05:42 UTC (4 vCPUs, 13.3 GiB available, DuckDB limits
  4 threads and 6,794 MiB, the tables prepared in 30.9 s, `main` at `1a18fd5`). DuckDB: `stream`
  1.29x with no work (0.460 s, +4 MB, against 0.595 s, +311 MB) and 1.68x with pandas (0.592 s
  against 0.992 s), `appender` 1.02x and 1.03x, both 1.08x and 1.23x, 200 small queries 0.756 s
  serial, 0.785 s on the main session and 0.492 s with one `new_session()` each (1.54x),
  `run.ingest` 1.57x (7.058 s, +864 MB, against 11.077 s, +692 MB), `materialize` 1.39x
  (7.136 s against 9.896 s) and `publish_delta(max_workers=4)` 1.81x (11.059 s against
  19.963 s): the pools gained between their 2-vCPU and 8-vCPU gains. Redshift: `run.ingest` 2.84x
  (11.973 s against 33.998 s), `stream`, `appender` and both 1.04x, 1.00x and 1.07x with no work
  and 1.04x, 1.06x and 1.07x with pandas, 80 small queries 3.808 s serial (about 48 ms each, its
  first repetition 5.565 s), 3.727 s on the main session and 1.944 s with one extra session each
  (1.96x), `publish_delta(max_workers=4)` 1.37x (19.057 s against 26.058 s) and
  `publish_redshift` with four workers 2.03x (13.813 s against 28.083 s), its other two
  repetitions at 32.850 s and 32.314 s. Across the four runs of 2026-10-05 to 2026-10-09 the 12
  four-connection repetitions fell in two groups with nothing between: 6 at 11.133 s to 13.813 s
  (the three of 15:48 on 2026-10-07, two of 18:55 and one of 2026-10-09) and 6 at 31.521 s to
  33.148 s (the three of 2026-10-05, one of 18:55 on 2026-10-07 and two of 2026-10-09), against
  26.750 s to 32.179 s for the 12 serial ones, and the cause is unread. `docs/index.md`
  ("Multithreading")
- The same probe in the second battery of 2026-10-09, from 22:09 UTC (8 vCPUs, 12.9 GiB
  available, DuckDB limits 8 threads and 6,595 MiB, the tables prepared in 25.9 s, `main` at
  `c503462`). DuckDB: `stream` 1.33x with no work (0.344 s, +4 MB, against 0.458 s, +312 MB) and
  1.84x with pandas (0.381 s against 0.702 s), `appender` 1.00x and 1.23x, both 1.10x and 1.21x,
  200 small queries 0.525 s serial, 0.534 s on the main session and 0.350 s with one
  `new_session()` each (1.50x), `run.ingest` 2.45x (4.192 s, +1,088 MB, against 10.290 s,
  +917 MB), `materialize` 2.42x (3.897 s against 9.416 s) and `publish_delta(max_workers=4)` 2.08x
  (8.209 s against 17.044 s), as on 8 vCPUs on 2026-10-05. Redshift: `run.ingest` 2.68x
  (12.443 s against 33.405 s), `stream`, `appender` and both 1.01x, 1.10x and 1.06x with no work
  and 1.05x, 1.10x and 1.04x with pandas, 80 small queries 3.494 s serial, 3.332 s on the main
  session and 1.918 s with one extra session each (1.82x), `publish_delta(max_workers=4)` 1.34x
  (18.761 s against 25.162 s) and `publish_redshift` with four workers 2.01x (13.782 s against
  27.657 s), its other two repetitions at 36.110 s and 34.213 s. The per-command times of the log
  `serialize_db.publication.commands` (PR #148), read for the first time, place the difference in
  the control row's `INSERT`, the last statement before each `COMMIT`: the four summed 5.42 s, at
  most 2.98 s, in the 13.782 s repetition, and 70.03 s and 65.94 s, at most 24.87 s and 23.40 s,
  in the slow ones, so at least three of the four took over 9 s there, while the `COPY`s summed
  12.60 s to 13.80 s, the data `INSERT`s 12.68 s to 14.63 s and the `COMMIT`s 3.14 s to 3.84 s,
  at most 1.49 s, in all three. One table at a time, the control `INSERT`s summed 1.34 s to
  3.77 s, at most 0.49 s to 2.80 s. Each transaction holds the control table's write lock from its
  `INSERT` to its `COMMIT` (`docs/tecnologias.md`, Redshift, "Transações concorrentes"), and no
  `COMMIT` took more than 1.49 s, so what made the `INSERT`s wait about 20 s is unread
  (`.claude/memory/OPEN_QUESTIONS.md`). Across the five runs of 2026-10-05 to 2026-10-09 the 15
  four-connection repetitions fell in the two groups, 7 at 11.133 s to 13.813 s and 8 at 31.521 s
  to 36.110 s, against 26.750 s to 32.179 s for the 15 serial ones. `docs/index.md`
  ("Multithreading")
- The same probe in the battery of 2026-10-10, from 04:14 UTC (8 vCPUs, 13.1 GiB available, DuckDB
  limits 8 threads and 6,695 MiB, the tables prepared in 25.6 s, `main` at `e38432f`). DuckDB:
  `stream` 1.30x with no work (0.373 s, +3 MB, against 0.484 s, +314 MB) and 1.89x with pandas
  (0.410 s against 0.774 s), `appender` 1.01x and 1.07x, both 1.14x and 1.22x, 200 small queries
  0.575 s serial, 0.597 s on the main session and 0.390 s with one `new_session()` each (1.48x),
  `run.ingest` 2.61x (3.923 s, +1,115 MB, against 10.255 s, +909 MB), `materialize` 2.33x
  (4.117 s against 9.612 s) and `publish_delta(max_workers=4)` 2.04x (8.140 s against 16.585 s).
  Redshift: `run.ingest` 2.68x (13.780 s against 36.866 s), `stream`, `appender` and both 1.01x,
  1.09x and 1.05x with no work and 1.01x, 1.14x and 1.03x with pandas, 80 small queries 4.153 s
  serial, 4.116 s on the main session and 1.789 s with one extra session each (2.32x),
  `publish_delta(max_workers=4)` 1.36x (18.305 s against 24.864 s) and `publish_redshift` with
  four workers 2.46x (12.033 s against 29.561 s), its other two repetitions at 12.537 s and
  33.214 s against 30.266 s and 29.722 s serial; without the unpublish between the measures, each
  measure swapping the published version (the `UPDATE` of the control row, no `CREATE TABLE`), the
  three four-worker repetitions took 11.380 s, 11.970 s and 12.658 s against 35.471 s, 27.579 s
  and 33.607 s serial (2.42x, best against best). The readings PR #151 added place the wait: in the
  33.214 s repetition the control `INSERT`s of `cad_paralelo_c` and `cad_paralelo_d` started at
  11.674 s and 11.799 s, while `cad_paralelo_a`'s ran (from 9.552 s, 3.214 s long) and as
  `cad_paralelo_b`'s `COMMIT` (2.260 s long) ended, and took 21.228 s and 20.251 s, their
  `COMMIT`s 0.311 s and 0.344 s, the data `INSERT`s before them 4.892 s and 4.894 s against 2.5 s
  to 3.6 s in the other transactions of the run. `sys_query_history` answered for 52 of 52 control
  writes: those two `INSERT`s had `planning_time` 21.070 s and 20.073 s, `lock_wait_time` 0.000 s,
  `queue_time` 0.000 s and `execution_time` 0.012 s and 0.011 s (`elapsed_time` 21.224 s and
  20.247 s), and the `INSERT`s of the same two tables in the publication that prepared the
  no-unpublish measures (outside the measures, 31.3 s for the four tables) had 20.067 s and
  21.020 s of planning with 0.000 s and 0.061 s of lock wait; the other 24 `INSERT`s and the 24
  `UPDATE`s planned in 0.040 s to 2.053 s, waited for a lock 0.000 s to 0.132 s, queued 0.000 s
  and executed in 0.011 s to 0.800 s. The fifth session's `svv_transactions` answered every poll
  (11 to 33 readings per measure) and saw no pending lock in any measure with unpublish, the slow
  one included (31 readings over 32.5 s); the one pending lock of the run was in the third
  no-unpublish four-worker repetition, at +12.0 s: `cad_paralelo_b` waiting for the
  `PgXenWriteLock` on relation `723112` that `cad_paralelo_c` held, after `cad_paralelo_a` held it
  at +10.9 s [inferred: the control table, the one relation the four share], its `UPDATE` 2.086 s
  long with 0.108 s of lock wait. So the ~20 s is the server planning the control row's `INSERT`,
  not a lock wait nor a queue, and the lock on the control table costs about 0.1 s; the four long
  plannings came in publications that created the tables (the `CREATE` before the `COPY`, the
  `INSERT` of the control row), in two of the four transactions each time, and none of the 48
  other writes planned over 2.1 s [3 repetitions per variant]. What the planner waits for is
  unread; the page on datashare writes (`REFERENCES.md`) says Redshift does not support accessing
  a datashare object that had a concurrent DDL between the `Prepare` and the `Execute` of the
  access, and the `CREATE TABLE` of the other three transactions, uncommitted in the same schema
  while the `INSERT` is planned, is the one DDL in the window [hypothesis]. Across the six runs of
  2026-10-05 to 2026-10-10 the 18 four-connection repetitions with unpublish fell in the two
  groups, 9 at 11.133 s to 13.813 s and 9 at 31.521 s to 36.110 s, against 26.750 s to 32.179 s
  for the 18 serial ones. The user chose to extend the probe (`decisions.md`, 2026-10-10): its
  `publicacao` section publishes the four tables by its own transactions in three statement
  orders, the publication's, the control row right after the `CREATE TABLE` and the tables
  created before the transaction, and the next battery reads the planning of each `INSERT`.
  `docs/index.md` ("Multithreading")
- The same probe in the second battery of 2026-10-10, from 18:26 UTC (8 vCPUs, 13.1 GiB
  available, DuckDB limits 8 threads and 6,683 MiB, the tables prepared in 23.1 s, `main` at
  `f8eba34`). DuckDB: `stream` 1.34x with no work (0.323 s, +3 MB, against 0.432 s, +311 MB) and
  2.06x with pandas (0.348 s against 0.717 s), `appender` 1.06x and 1.08x, both 1.22x and 1.19x,
  200 small queries 0.507 s serial, 0.519 s on the main session and 0.337 s with one
  `new_session()` each (1.50x), `run.ingest` 2.50x (3.783 s, +1,104 MB, against 9.473 s, +901 MB),
  `materialize` 2.49x (3.756 s against 9.343 s) and `publish_delta(max_workers=4)` 2.03x (7.995 s
  against 16.219 s). Redshift: `run.ingest` 2.50x (11.933 s against 29.840 s), `stream`, `appender`
  and both 1.01x, 1.04x and 1.07x with no work and 1.03x, 0.98x and 1.14x with pandas, 80 small
  queries 3.720 s serial, 3.748 s on the main session and 1.798 s with one extra session each
  (2.07x), `publish_delta(max_workers=4)` 1.32x (18.585 s against 24.611 s) and `publish_redshift`
  with four workers 2.31x (10.520 s against 24.341 s), its other two repetitions at 10.759 s and
  31.302 s against 27.373 s and 25.616 s serial; without the unpublish between the measures the
  three four-worker repetitions took 10.965 s, 11.083 s and 10.768 s against 30.732 s, 27.386 s
  and 30.820 s serial (2.54x, best against best). In the slow repetition the control `INSERT`s of
  `cad_paralelo_a` and `cad_paralelo_d` started at 9.373 s and 9.553 s, while `cad_paralelo_b`'s
  ran (from 9.125 s, 0.413 s long) and its `COMMIT` followed (from 9.538 s, 0.529 s long), and
  took 21.536 s and 20.395 s: `planning_time` 20.959 s and 20.061 s, `lock_wait_time` 0.104 s and
  0.000 s, no queue, `execution_time` 0.011 s and 0.012 s; both ran on past `cad_paralelo_b`'s
  `COMMIT`, ended within 1 s of each other, and `cad_paralelo_a`'s ended 0.36 s after
  `cad_paralelo_d`'s `COMMIT`. `sys_query_history` answered for 88 of 88 control writes, and the
  fifth session saw no pending lock in the slow repetition (29 readings over 31 s).
  The `publicacao` section's first publication of the four tables by the probe's own transactions
  (the user's choice of 2026-10-10, `decisions.md`), three repetitions per order:
  - the publication's order (`CREATE TABLE`, the load, the control row's `INSERT`): 9.461 s,
    9.710 s and 9.427 s, the 12 `INSERT`s 0.200 s to 1.310 s long, no wait [3 repetitions];
  - the control row right after the `CREATE TABLE`, before the staging and the `COPY`: 23.069 s,
    22.798 s and 23.358 s, 2.4 times the publication's order, in every repetition. The four
    transactions serialized on the control table: the first `INSERT` of each repetition took
    0.210 s to 0.344 s and the other three waited for the previous transaction's `COMMIT`, 5.340 s
    to 5.804 s, 10.785 s to 11.147 s and 16.232 s to 16.870 s, each ending 0.14 s to 0.34 s after
    the `COMMIT` before it; `sys_query_history` booked the waits as `planning_time` (4.995 s to
    16.481 s) with `lock_wait_time` 0.086 s to 0.132 s, and the fifth session saw, in the second
    repetition only, the `PgXenWriteLock` on relation `738123` held by one pid after the other
    (+2.1 s to +6.5 s, +7.5 s to +11.8 s, +12.9 s to +17.2 s, +18.3 s to +22.6 s) and pending for
    one reading, and `nenhum lock pendente` in the other two, whose timelines show the same
    serialization;
  - the tables created and committed before the transactions: 10.234 s, 9.741 s and 9.135 s, the
    12 `INSERT`s 0.204 s to 1.416 s long, no wait [3 repetitions].
  So the control row's write takes a write lock on the control table that the transaction holds
  until its `COMMIT`; `sys_query_history` counts the wait for it as `planning_time`, and
  `svv_transactions` shows it in some readings only. The publication's order, with the control
  row last, holds the lock from the `INSERT` to the end of the `COMMIT`, 0.5 s to 1.7 s in the
  probe's repetitions, and the ~20 s wait is not that lock: the two waiters ran on past the
  holder's `COMMIT`, and the first `INSERT` of each repetition with the control row before the
  `COPY` planned in 0.21 s to 0.34 s with the other three transactions' `CREATE TABLE`
  uncommitted, which refutes the hypothesis that the planner waits on those as such. What
  separates the slow repetitions is still the `CREATE TABLE` inside the concurrent transactions:
  across the seven runs of 2026-10-05 to 2026-10-10, 10 of the 21 four-connection repetitions of
  the library's publication with unpublish, a `CREATE TABLE` per transaction, took 31.302 s to
  36.110 s and 11 took 10.520 s to 13.813 s, against 24.341 s to 32.179 s for the 21 serial ones,
  and none of the 9 without it, the 6 no-unpublish repetitions (`UPDATE`) and the 3 with the
  tables created before, waited [inferred: at the slow rate of 10 in 21, nine fast repetitions in
  a row have a chance near 0.3%]. What the two waiters wait for in those ~20 s is unread. In the
  four publications of the whole base since 2026-10-09 no table paid the wait (`redshift.md`). The
  choice between accepting the wait and creating the tables before the load transactions is the
  user's (`OPEN_QUESTIONS.md`, "A espera da linha de controle na publicação em paralelo").
  `docs/index.md` ("Multithreading"), `docs/tecnologias.md` (Redshift, "Transações concorrentes")
