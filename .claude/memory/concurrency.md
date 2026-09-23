# Threads, the GIL and the batch boundary

Read before `stream`, `loader`, `max_workers`, any helper thread, or a change in how batches cross the library's boundary. Each fact ends with the `plan/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

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
  startup and keep hot pure-Python loops out of the library's threads. `plan/PLAN.md`, section "A troca de dados com o
  código cliente", records the decisions of 2026-09-20. `tests/proof_of_concept/test_concurrency.py`, `test_parallel.py`
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
  accepted by `from_stream`, DuckDB `register` and `write_deltalake`. `plan/PLAN.md`, `plan/POC.md`,
  `plan/duckdb.md`, `tests/proof_of_concept/test_duckdb.py`, `test_pyarrow.py`, `test_parallel.py`
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
  `publish_redshift` keeps a connection per table, outside the sandbox session. `plan/PLAN.md`,
  `plan/PLAN-STAGE-4.md`, `plan/PLAN-STAGE-5.md`, `plan/POC.md`, `tests/proof_of_concept/test_parallel.py`
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
  sessions against 0.066 s in series. `plan/PLAN.md`, `plan/PLAN-STAGE-4.md`, `plan/POC.md`,
  `tests/proof_of_concept/test_parallel.py`, `tests/proof_of_concept/test_duckdb.py`
