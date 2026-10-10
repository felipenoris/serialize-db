# Redshift

Read before code on `engine.redshift`, `serialize_db.publication`, the Redshift suite or `probes/redshift.py`; the scripts that fixed the target are in the Claude project's library, `/mnt/project-files/target_env_examples/`. A fact that a file of the repository details ends with that file, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

## The target

- The target's Redshift is serverless (`controladoria-wg`, `sa-east-1`), and the connection is the
  workgroup's temporary credential: `GetWorkgroup` for the endpoint, `GetCredentials` for a user
  `IAMR:<role>` and a password lasting 900 s by default and 3600 s at most, then
  `redshift_connector.connect`. No stored password, and the user is created in the database and put
  in `PUBLIC`. The Data API is the HTTPS path: async (`ExecuteStatement`, `DescribeStatement`,
  `GetStatementResult`), one-item dicts per cell, `DECIMAL` and timestamps as text, 500 MB per
  result, 24 h retention, 200 KB per statement, a session that dies with the statement unless
  `SessionKeepAliveSeconds` keeps it. It stays out of the library (user decision of 2026-09-20).
  `redshift_connector.connect(timeout=)` is the socket timeout for connecting and reading, applied
  once: 10 s killed `sys_load_error_detail` in the target and the socket never served again; the
  probe uses 30 s over system views, the suite and the library connect without one because a `COPY`
  outlives any read timeout; `ssl=True` is the default. The driver's internal IAM (`iam=True`) and
  `GetClusterCredentials` are out: nobody ran them in the target, which has no cluster.
  `/mnt/project-files/target_env_examples/`, `docs/tecnologias.md` (Redshift)
- The target's Redshift, read on 2026-09-20 (the reports left `plan/readings/` on 2026-09-23 and
  stay in git history): workgroup `controladoria-wg`,
  namespace `controladoria-ns`, account `<conta>`, base capacity 8, no provisioned cluster; the
  three Redshift APIs and the workgroup host resolve to private IPs, so the temporary credential and
  the Data API work without internet; version `1.0.436211`; `datalake_rw_shared` is `shared` from the
  datashare `controladoria_rw_datashare` (producer account `<conta do produtor>`) with isolation `UNKNOWN`,
  and `sbx_aco_decon` exists only there. The namespace has no IAM role, default or attached, so
  `IAM_ROLE` is unusable and `COPY`/`UNLOAD` carry the caller's credentials.
  `has_database_privilege(dev, CREATE)` is false and `TEMP` true, so the execution sandbox is either
  a temporary table or the datashare itself, and no external schema reads the Delta in place:
  Redshift loads it only through `COPY ... MANIFEST`, as the diagnosis of 2026-09-13 in PR #2 had
  found. `stv_slices` and `stl_load_errors` are denied
  to a regular user (42501) while `sys_load_error_detail` answers; `pg_settings` on serverless lists
  neither `timezone` nor `enable_case_sensitive_identifier`, which `SHOW` returns.
  `.claude/memory/OPEN_QUESTIONS.md`
- The cost of one full `SUITE_ALVO.md` run, estimated on 2026-10-07: only the workgroup's compute
  counts, at the public on-demand price of US$ 0.5976 per RPU-hour in `sa-east-1` (Price List API,
  publication 2026-09-11; the one-year reservation without upfront is US$ 0.466), so US$ 4.78 per
  active hour at the base 8 RPU, billed per second with a 60 s minimum. The blocks that reach
  Redshift kept it active about 88 minutes on 2 vCPUs: the six pytest sessions 43.9 min (03:16:37
  to 04:00:30 on 2026-10-07; 35.7 min on 8 vCPUs on 2026-10-05), the Redshift sections of
  `probe_parallel_gain.py` about 21 min, `probe_unload_parallel.py` 12.4 min (its rerun of 18:41
  on 2026-10-07; 12.3 min on 8 vCPUs on 2026-10-05), the publication block about 6 min, the
  consistency probes 2.3 min, and one minimum each for `probes/redshift.py` and the reader: about
  11.7 RPU-hours, US$ 6.98; `credentials.py`, absent from both batteries, adds about 15 isolated
  `select 1` minimums, US$ 1.20. The section "Sonda da base publicada", which `SUITE_ALVO.md` gained
  on 2026-10-08, is outside the estimate: `probe_published_base.py` keeps Redshift active for the
  `EXPLAIN` and two publications by the channel, each swapping one partition of `cad_lancamentos`,
  and its first run, on 2026-10-09, took 168 s, 96.9 s of them in the two publications. The
  estimate counts each block's wall time as active, because the AWS pages do not say how the
  minimum covers consecutive commands, and assumes no scaling above 8 RPU, which is billed at the
  same rate; the probe never read the workgroup's max capacity.
  `SYS_SERVERLESS_USAGE` (`charged_seconds` per minute, 7 days) is visible only to superusers,
  which the project's user is not; system-table queries are billed and autonomics are not.
  Managed storage costs US$ 0.043 per GB-month, charged to the producer's namespace [inferred].
  `REFERENCES.md` (Amazon Redshift)

## The datashare, COPY and UNLOAD

- The project schema `sbx_aco_decon` lives in the datashare database `datalake_rw_shared` (user
  decision of 2026-09-20), and writing into it works, proved that day by
  `target_env_examples/redshift_copy_unload.py`. `USE <database>` switches the session's database,
  after which `schema.table` is enough; the three-part name binds only a session connected
  elsewhere, such as the Data API. `CREATE TABLE`, `COPY` of a Parquet prefix, `SELECT` and `UNLOAD`
  all passed, and `COPY`/`UNLOAD` reach S3 through the caller's `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY`
  and `SESSION_TOKEN` instead of `IAM_ROLE`, which unblocks a namespace with no attached role; the
  statement carries a secret and never reaches a log, the suite report or a file
  (`tests/conftest.py` masks every credential clause). A datashare write also needs patch 186
  (`1.0.78890` serverless), snapshot isolation on the producer's database and 64 slices; it accepts
  `CREATE`/`DROP`/`SHOW TABLE`, CTAS, `ALTER TABLE ADD`/`DROP COLUMN`, `RENAME`, `TRUNCATE`
  (transactional there), `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE` and `COPY` with no
  `COMPUPDATE` clause (the Parquet `COPY` rejects it; the columns' encoding comes from the DDL or
  `ENCODE AUTO`, and an `ANALYZE COMPRESSION` on a real sample is what settles it), writes one
  database per transaction and creates no views.
  `svv_all_schemas`, `svv_all_tables` and `svv_redshift_databases` cross databases;
  `has_schema_privilege` and `svv_table_info` see only the session's. `COPY ... MANIFEST` and
  `UNLOAD ... PARTITION BY ... MANIFEST VERBOSE` passed there on 2026-09-21 over 500,000 rows (4.6 s
  and 0.8 s), and the session's first statement cost 10.8 s. The `UNLOAD` writes `TIMESTAMP` as `INT96` and `DECIMAL(18,2)` as
  `FIXED_LEN_BYTE_ARRAY(8)`, where the library writes `INT64` for both; every column comes out
  `optional`, min and max are present except on the `INT96`, and it fragments by slice (32 files for
  500,000 rows, which `PARALLEL OFF` or compaction undoes). An `INT96` file registered in a
  `timestamp_ntz` table reads back as `timestamp[us]` in delta-rs and in `delta_scan`, values
  intact. `docs/tecnologias.md` (Redshift)
- The `UNLOAD` read on 2026-09-23, twice: the `select` is a literal that treats the backslash as an
  escape (the docs escape a quote as `\'`), so the text goes in with the backslash and the quote
  doubled; with only the quote doubled, a literal with a backslash reached the inner `select`
  unpaired and matched no row. An empty result passes and writes no file and no manifest;
  `pg_last_unload_count()` (docs read that day) gives the rows of the session's last completed
  `UNLOAD`, 0 when none completed or it failed while unloading. The outer `LIMIT` is `42601 Limit
  clause is not supported`; a session temporary table is readable by the `UNLOAD` of the same
  session; without `PARTITION BY`, a prefix with `=` works and the verbose manifest's
  `schema.elements` lists the `select`'s columns only; a `SUPER` column comes out with the Parquet
  `JSON` type, read by pyarrow as `extension<arrow.json>` holding each value's JSON text, which
  `schema.cast` turns into `string`. In a row group with `NaN`, the footer's min and max of a
  `DOUBLE PRECISION` leave the `NaN` out, as pyarrow does, and DuckDB's `read_parquet` pruned the
  group (0 rows for `valor > 3`), wherever the `NaN` sat; with the infinities, the footer holds
  `-inf` and `inf`. `docs/tecnologias.md` (Redshift)
- The export's `UNLOAD` threshold read in the target on 2026-10-05
  (`probes/operacao/probe_unload_parallel.py`): the partition 2026-01-31 of `cad_lancamentos`
  (33,239,719 rows) went into the sandbox by the engine's `ingest` in 37.5 s, and tables of 1, 5, 10
  and 20 million of its rows by `CREATE TABLE AS ... LIMIT`; the export's `SELECT` (no `PARTITION
  BY`, `ORDER BY` the sort key) wrote one file in both modes in every repetition, 17.4 MB, 86.4 MB,
  172.1 MB, 338.6 MB to 339.0 MB and 559.0 MB, the best times 2.0 s, 8.3 s, 16.2 s, 31.9 s and
  50.3 s with `PARALLEL OFF` against 2.1 s, 8.4 s, 16.3 s, 32.6 s and 50.7 s in parallel (0.98 to
  0.99), the footers read in 0.03 s to 0.10 s. The 32 files of 2026-09-21 came from an `UNLOAD ...
  PARTITION BY` without `ORDER BY` of a `DISTSTYLE KEY` table; which of the three differences gives
  one file is not separated. `serialize_db.engine.redshift` (`_PARALLEL_OFF_ROWS`),
  `docs/tecnologias.md` (Redshift)
- The same probe on 2026-10-07 from 18:41 UTC (`--ignore-partitions 2025-09-30`, 2 vCPUs,
  `environments.md`): the partition went into the sandbox in 37.9 s, both modes wrote one file in
  every repetition, 17.3 MB, 86.3 MB, 172.1 MB, 339.1 MB and 559.1 MB, the best times 2.1 s,
  8.5 s, 16.4 s, 32.2 s and 50.4 s with `PARALLEL OFF` against 2.1 s, 8.4 s, 16.4 s, 32.8 s and
  51.0 s in parallel (0.98 to 1.01), the footers read in 0.03 s to 0.08 s: within 0.5 s of
  2026-10-05 with 8 vCPUs, the `UNLOAD` time is the server's.
- The same probe on 2026-10-09 from 05:29 UTC (4 vCPUs, `environments.md`): the partition went
  into the sandbox in 39.9 s, both modes wrote one file in every repetition, 17.4 MB, 86.4 MB,
  172.1 MB, 338.9 MB and 559.1 MB, the best times 2.1 s, 8.4 s, 16.2 s, 32.1 s and 50.4 s with
  `PARALLEL OFF` against 2.1 s, 8.4 s, 16.5 s, 32.5 s and 51.1 s in parallel (0.98 to 0.99), the
  footers read in 0.04 s to 0.08 s.
- The same probe in the second battery of 2026-10-09, from 21:56 UTC (8 vCPUs,
  `environments.md`): the partition went into the sandbox in 37.2 s, both modes wrote one file in
  every repetition, 17.4 MB, 86.3 MB, 172.0 MB, 339.0 MB and 558.9 MB, the best times 2.1 s, 8.3 s,
  16.3 s, 32.2 s and 49.9 s with `PARALLEL OFF` against 2.1 s, 8.5 s, 16.4 s, 32.9 s and 51.2 s in
  parallel (0.97 to 1.00), the footers read in 0.04 s to 0.07 s.
- The result description read on 2026-09-23: OIDs 20, 23, 21, 701, 700, 1700, 1043, 1042, 1082,
  1114, 1184, 16 and 4000 for `BIGINT`, `INTEGER`, `SMALLINT`, `DOUBLE PRECISION`, `REAL`,
  `DECIMAL`, `VARCHAR`, `CHAR`, `DATE`, `TIMESTAMP`, `TIMESTAMPTZ`, `BOOLEAN` and `SUPER`;
  `type_modifier` 1,179,654 for `DECIMAL(18, 2)`, `n + 4` for `VARCHAR(n)` and `CHAR(n)`,
  16,384,000 for `SUPER`, -1 elsewhere; `sum` and `avg` of `DECIMAL(18, 2)` come out
  `NUMERIC(38, 2)`, a text literal `VARCHAR`, `1.5` `NUMERIC(2, 1)`; `SUPER` reaches Python as
  `str`. A 10-row load took 0.91 s and 0.97 s by `COPY` against 0.53 s and 0.55 s by a multi-row
  `INSERT` (best of three).

## The driver

- With `redshift_connector`, `executemany` makes one round trip per row and the dialect does not
  rewrite it into a multi-row `VALUES`: bulk loads go through Parquet on S3 and `COPY`, small batches
  through `insert(Modelo).values(lista)`. `docs/tecnologias.md` (Redshift)
- With autocommit off, `redshift_connector` issues `begin transaction` before the first `execute`
  of a cursor, and setting `autocommit = True` afterwards does not close the open transaction: a
  session that ran `USE` before switching autocommit on stayed in one transaction, the denied
  `stv_slices` read aborted it, and every later statement, including the cleanup's `DROP`s, failed
  with 25P02 (target, 2026-09-21). The suite and the library set autocommit right after `connect`,
  before the `USE`. `select version()` comes back with a trailing NUL byte, `current_schema()` is
  null after the `USE`, and `svv_redshift_databases` reports `datalake_rw_shared` as `shared` with
  isolation `UNKNOWN` and `dev` as `local` with `Snapshot Isolation`.
- `redshift_connector` 2.1.16 keeps a named prepared statement per SQL text (`Connection.execute`,
  key `(operation, params)`, cache per paramstyle and pid), reuses it with `Bind` and `Execute` and
  no new `Parse`, and closes and clears the cache only when a `CommandComplete` starts with `ALTER`,
  `CREATE`, `DROP` or `ROLLBACK` (`handle_COMMAND_COMPLETE`); `TRUNCATE` is not in the list. In the
  datashare, an `Execute` of a statement parsed before a `TRUNCATE` of its table answered `XX000`
  `[Data Sharing] Error Code 34510: Concurrent DDL committed on <db>.<schema>.<table> between
  Prepare and Execute` (routine `relocalize_data_sharing_cached_rtes`) in the runs of 12:08 and
  12:10 UTC, third round of `test_copy_column_list_and_fillrecord`. `max_prepared_statements=0`
  makes the driver use the unnamed statement, parsed right before every execute, and cache nothing
  (`get_statement_name_bin`; the cache insertion runs only above zero). The suite and the library
  connect with it; `connect_redshift(statement_cache=True)` keeps the driver default for the
  reading that reproduces the error. `docs/tecnologias.md` (Redshift)
- The driver materializes a result in `execute`: `EXECUTE_MSG` asks the portal for all rows,
  `handle_messages` returns only at `READY_FOR_QUERY`, each `DATA_ROW` lands in
  `cursor._cached_rows`, and `fetchmany` is `islice` over `Cursor.__next__`, which pops that deque.
  `stream` on Redshift always goes through `UNLOAD` and `query` through the cursor (user decision of
  2026-09-23); the suite read 5 rows in the queue before the first `fetchmany` (2026-09-21, 13:35
  and 13:39), now an assertion. `docs/tecnologias.md` (Redshift)
- `cursor.description` of `redshift_connector` 2.1.16 is `(name, oid, None, None, None, None,
  None)` per column (`Cursor._getDescription`); the `type_modifier` of each column is in
  `cursor.ps["row_desc"]`, stored by `Connection.handle_ROW_DESCRIPTION`, and the driver itself
  decodes the binary `NUMERIC` with scale `(type_modifier - 4) & 0xFFFF`
  (`Cursor.truncated_row_desc`); precision is `((type_modifier - 4) >> 16) & 0xFFFF`. `RedshiftOID`
  lists `REAL` 700, `BPCHAR` 1042, `TEXT` 25, `UNKNOWN` 705 and `SUPER` 4000, which the driver reads
  as text (code reading of 2026-09-23). `docs/tecnologias.md` (Redshift)
- The literal text of the `stream` by `UNLOAD` (local probe of 2026-09-23): the
  `RedshiftDialect_redshift_connector` default `paramstyle` (`format`) doubles `%` inside literals
  and `named` does not, and `redshift_connector` sends a statement executed without parameters
  unconverted (`has_bind_parameters`), so the engine compiles with `named`; both styles double the
  single quote and the backslash (`_backslash_escapes`); `compiled.binds` is empty under
  `literal_binds`, where a valueless `IN` list renders `IN (NULL)`, so the guard walks the statement
  (`sqlalchemy.sql.visitors.iterate`) for `BindParameter.required`; `text().params()` refuses to
  render (`CompileError ... with datatype NULL`) and `sa.bindparam(name, value=value, expanding=...)`
  types each value. The `UNLOAD` literal also treats the backslash as an escape, so the `select`
  goes in with the backslash and the quote doubled again (target reading of 2026-09-23, below);
  the six cases of `test_stream_by_unload_with_literal_values` wait for the next run with that
  escape.
- The stage 5 readings of `tests/proof_of_concept/test_redshift.py` (seven tests after
  `test_parallel_copy_and_unload_on_two_connections`) ran in the target on 2026-09-23, twice; what
  is left for the next run is in `.claude/memory/OPEN_QUESTIONS.md`.
- The stage 1 `ddl` and the stage 4 `audit_sql` cite tables without a schema, so on Redshift the
  engine relies on `SET search_path TO <schema>` after `USE`, which passed on the datashare schema
  on 2026-09-23; one refused measure fails the whole rows check, so
  `test_audit_sql_under_search_path_and_nan_comparison` also runs each measure alone beside the
  expected counters, and the client model's texts on empty tables.
  `.claude/memory/OPEN_QUESTIONS.md`

## The reading of 2026-09-21

- `USE datalake_rw_shared` makes two-part names resolve in the datashare, and `current_database()`
  keeps answering `dev` afterwards (probe `RS-19` of 2026-09-21, user confirmation the same day): the
  switch is confirmed by resolving a name (the library runs no confirmation command, and its first
  two-part statement confirms it; only `create_publications_table`, run once by the user, creates
  `<schema>.serialize_db_publications`, user decision of 2026-09-23; the probe selects from a table
  `svv_all_tables` lists), never by that function, and the suite records the function's
  value as a reading. `has_schema_privilege('sbx_aco_decon', 'CREATE')` after the `USE` answered
  `false` without error in the suite of 2026-09-21 (one reading), in the schema where `CREATE TABLE`
  works: the function does not prove the privilege on a datashare schema, the `CREATE` does;
  `svv_table_info` after the `USE` answered `permission denied for relation svv_table_info` (42501)
  to the project's role (probe `RS-8`, 2026-09-23), so the publication needs another source for the
  assigned distribution (the `EXPLAIN` of a typical join, `.claude/memory/OPEN_QUESTIONS.md`). Other
  readings: `enable_case_sensitive_identifier` off, `datestyle` `ISO, MDY`, `statement_timeout` 0,
  `wlm_query_slot_count` 1, `sys_load_error_detail` answered 0 in 2.4 s; the Data API `select 1` stayed
  `PICKED` for 30 s (23 ms the day before); `iam.simulate_principal_policy` times out in the target
  (no IAM endpoint), so the first `COPY` proves the permission.
- Second suite run in the target (2026-09-21 11:28 UTC, 7 passed, 4 failed; report not kept, the
  clean runs repeat its readings): `information_schema.columns` is empty for
  the datashare schema after the `USE` (local database only, like `has_schema_privilege`, which
  answered `false` again); `svv_all_columns` crosses databases, and `cursor.description` of a
  `select ... limit 0` describes a table without any catalog view. `COPY ... MANIFEST` answers
  `Spectrum Scan Error: File not found` for a URL with `//` (an S3 key with a double slash is
  another key) and reports the `=` of a Hive folder as `%3D`; `DeltaTable.table_uri` ends with a
  slash, and delta-rs 1.6.4 stores and returns `mes=2026-01/...` unencoded, also for an `AddAction`
  registered with the raw path. The verbose `UNLOAD` manifest's `schema.elements` lists the
  partition column (`mes`, `character varying`, `max_length` 7) that the files do not have. The
  Data API answered in 444 ms: the 30 s `PICKED` was transient. `docs/tecnologias.md` (Redshift)
- Third and fourth runs (2026-09-21 12:08 and 12:10 UTC, 10 passed and 1 failed each; reports not
  kept, the clean runs repeat their readings; identical reading by reading): `COPY ... FORMAT AS PARQUET MANIFEST` loads `DECIMAL(18,2)` as `INT64` and
  `timestamp_ntz` as `INT64` µs (sum and min checked); a five-column file into a six-column table
  fails positionally with `Spectrum Scan Error` 15007 `Unmatched number of columns`, loads with a
  column list (100 rows, the missing column null), and `FILLRECORD` is accepted with its row count
  unread; a 300-byte string into `VARCHAR(200)` aborts the `COPY` with 15007 and
  `sys_load_error_detail` says `The length of the data column descricao is longer than the length
  defined in the table. Table: 200, Data: 300` (`stl_load_errors` still denied); `SUPER` takes an
  80,901-byte document by `INSERT ... JSON_PARSE(%s)` (`json_size` 80901) and the `COPY` of a
  Parquet string column into `SUPER` needs `SERIALIZETOJSON` (`SUPER column in COPY query requires
  SERIALIZETOJSON option`); `UNLOAD ... PARTITION BY` names files `mes=<v>/<slice>_part_<nn>.parquet`
  with the slice varying between runs (`0064`, `0000`), and without `ALLOWOVERWRITE` checks the
  destination as a prefix (same prefix and parent prefix refused with `Specified unload destination
  on S3 is not empty`, a new subprefix under a folder with files accepted), so stage 5 unloads
  without `PARTITION BY` to a prefix new per partition and per attempt,
  `<uri>/<coluna>=<valor>/<execution_id>_<uuid>/` (user decision of 2026-09-23); two parallel `COPY` 4.5 s and 3.6 s, two parallel `UNLOAD` 1.9 s
  and 1.5 s; Data API 610 ms and 177 ms; `has_schema_privilege` `false` four times.
- Fifth and sixth runs (2026-09-21 13:35 and 13:39 UTC, 12 passed each, the two clean runs the proof
  of concept required; the two JSON reports left `plan/readings/` on 2026-09-23, in git history, and
  the folder then held the 2026-09-23 runs): with
  `max_prepared_statements=0` the same `select count(*)` passes before and after a `TRUNCATE`; with
  the driver's cache the repeat after the `TRUNCATE` and a second repeat both get 34510 (the stale
  entry stays), the repeat after an `ALTER TABLE ... ADD COLUMN` passes, and the same sequence on a
  `CREATE TEMP TABLE` made after the `USE` passes (the refusal is the datashare's).
  `COPY ... FILLRECORD` loads the five-column file into the six-column table with the new column
  null (100 rows), like the column list; `TRUNCATECOLUMNS` is refused for Parquet (`0A000`
  `TRUNCATECOLUMNS argument is not supported for PARQUET based COPY`); `COPY ... FORMAT AS PARQUET
  SERIALIZETOJSON` of an 80,901-byte string into `SUPER` fails with `1224 String value exceeds the
  max size of 65535 bytes`, so a Parquet string never carries a document above 65,535 bytes into
  `SUPER`; `COPY ... FORMAT JSON 'auto'` of a one-line JSON file with the document as an object loads
  it (`json_typeof` `object`, `json_size` 80901). Data API 270 ms and 240 ms; parallel `COPY` 4.3 s
  and 3.8 s, `UNLOAD` 1.6 s and 1.5 s; `has_schema_privilege` `false` six times. Proposals awaiting
  the user: `FILLRECORD` on every library `COPY`, and the 65,535-byte ceiling of the JSON field
  checked by the audit.

## Sessions and temporary tables

- Redshift Serverless ends by `SESSION TIMEOUT` a session idle for more than 3,600 s and a
  transaction left open and inactive for more than 21,600 s, and stops a query above 86,399 s;
  `ALTER USER ... SESSION TIMEOUT` sets the limit per user, 60 s to 20 days, new sessions only, and
  needs a superuser or the `ALTER USER` privilege; `stv_sessions` shows the session's limit. Every
  query is billed activity, a keepalive included, with a 60 s minimum (AWS docs read on
  2026-09-22). A temporary table lives in a session-specific schema that comes first in the
  `search_path`, may carry the name of a permanent table and shadows it until the permanent one is
  schema-qualified, gets `RAW` encoding by default unless a column says `ENCODE`, and is absent
  from `svv_table_info`. `redshift_connector.Cursor.execute` delegates to `Connection.execute`, so
  every cursor of a connection is the same session. `docs/tecnologias.md` (Redshift)
- The Redshift engine keeps one session per execution under a `threading.RLock` (user decision of
  2026-09-22), the `exec_<id>_*` tables stay permanent in the datashare schema, and the user
  reverted the temporary-table proposal the same day; a temporary table the pipeline creates in
  the session is lost when the engine reconnects.
- Concurrent transactions, from the AWS docs read on 2026-09-23: a transaction's snapshot starts at
  its first `SELECT`, DML, `CREATE`/`DROP`/`ALTER`/`TRUNCATE TABLE`, not at `BEGIN`; `SNAPSHOT` is
  the default level of new clusters and workgroups, and under it two `UPDATE`s of distinct rows of
  one table both commit, while under `SERIALIZABLE` the second gets `ERROR:1023 DETAIL: Serializable
  isolation violation on table`; concurrent `DELETE`/`UPDATE` on one table wait for the first to
  finish on both levels, and concurrent `COPY`/`INSERT` run together under snapshot until both must
  write; locks leave only at the transaction's end, and `INSERT`/`COPY` followed by an exclusive
  statement on the same table can deadlock under snapshot. `LOCK` takes `ACCESS EXCLUSIVE` until the
  end and is the documented way to force order, but it is not in the list of statements a datashare
  consumer may write with; datashare writes require snapshot isolation on the producer's database,
  and `svv_redshift_databases` reported `datalake_rw_shared` as `UNKNOWN`. `1018 Relation does not
  exist` is a transaction reading a table another created after its snapshot.
  `tests/proof_of_concept/test_redshift_transactions.py` read in the datashare schema twice on
  2026-09-23 (A holds its transaction 10 s while B runs in a thread): writes to distinct tables do
  not wait; distinct control rows both commit, B's `DELETE` of its row waiting for A's `COMMIT`;
  B's `CREATE TABLE` of the fixed-name staging A created waits for A's `COMMIT`, and B's `DELETE`
  of the partition A replaced then gets `1023 Serializable isolation violation`; `LOCK` is refused
  (`0A000 Operation is not supported through datashares`); the `UPDATE` of the control row
  conditioned on the version read, as the first statement, waits for A's `COMMIT` and affects 0
  rows. `stv_db_isolation_level` is denied (42501). The stage 8 transaction reads the control row
  first and writes it last, by `INSERT` or by the `UPDATE` conditioned on the version read, and the
  unpublish flow deletes it with the published table (user decision of 2026-09-23,
  `decisions.md`). `docs/tecnologias.md` (Redshift)
- An extra session (`new_session()`, 2026-09-23) is another connection with its own temporary
  credential and `USE`: it sees the `exec_<id>_*` tables the main session committed and not its
  temporary tables; `run.ingest` of more than one table opens one per table. The suite's two
  parallel `COPY`s, each opening its connection inside the task, took 4.3 s and 3.8 s in the target
  on 2026-09-21.

- Redshift's `COUNT` has no `FILTER (WHERE ...)` clause (`COUNT( * | expression )` in the docs, read
  2026-09-23), so the audit counts with `count(CASE WHEN <defect> THEN 1 END)` on both engines.
  The audit text ran in the target on 2026-09-23, on a connection with `SET search_path` to the
  datashare schema after the `USE` (it passed, the unqualified `CREATE TABLE` landed there, and
  `current_schema()` stayed null): `count(CASE WHEN ...)`, `to_char`, `octet_length` and `~` passed;
  `is_valid_json(super)` does not exist (42883), and it took the whole rows check down; and Redshift
  compares `NaN` two ways, equal to itself on constants (`'NaN'::float8 = 'NaN'::float8` true, as
  PostgreSQL) and unequal to everything in a table scan (IEEE), so `x NOT IN ('NaN'::float8, ...)`
  counted 1 of 2 non-finite values and let the `NaN` reach the `CAST`, refused with `NaN input
  (scale float to decimal)`; the `CAST` of a constant `NaN` to `NUMERIC(38, 6)` is `22P02`, and a
  `sum` with a `NaN` is `nan`. The text now compiles `is_finite` as `(x > '-Infinity'::float8 AND
  x < 'Infinity'::float8)`, false for `NaN` under both rules, and `json_valid` as `true`, because
  the JSON column is `SUPER`; the new text waits for the next suite run.
  `.claude/memory/OPEN_QUESTIONS.md`
- The suite runs of 2026-09-23 at 22:56 and 23:01 UTC (30 passed each, readings equal but ids and
  times): the stream with literals gave the same rows by the three paths in the six cases, the
  backslash doubled in the `UNLOAD` literal; the empty `UNLOAD` passed without manifest or object
  and `pg_last_unload_count()` read 0, the temporary table's `UNLOAD` 2; `ALTER TABLE ... ALTER
  COLUMN ... TYPE VARCHAR(10)` was refused on the share with `0A000 Operation is not supported
  through datashares`, on a common and on a key column, and ten characters were refused with
  `22001`; the role ran `EXPLAIN` on the share (`XN Hash Join DS_DIST_ALL_NONE` between two small
  tables); both temporary stagings committed, filled inside the transaction and before the `BEGIN`;
  in the publication that reads the control row first, the second transaction read version 1,
  waited 10.1 s and 10.7 s at the partition `DELETE` and got `1023`. With the strict infinity
  comparison the audit's sum left the `NaN` out (4.500000), but its negation did not count the
  `NaN` either (`naofinito_valor` 1 of 2), and in the scan neither `valor = 'NaN'::float8` nor
  `valor <> valor` was true (0 and 0): the count became `count(x) - count(CASE WHEN finite ...)`,
  and `nan_na_tabela_detalhe` reads each comparison, the negation, `is null` and the text of the
  `NaN` row. The `redshift.py` run of 22:49 got no row from `select count(*) from
  sys_load_error_detail where start_time > ...` (12.8 s) and stopped the session section on
  `IndexError` before `RS-5`, `RS-8`, `RS-12`, `RS-13`, `RS-16`, `RS-17` and `RS-19`.

- The suite runs of 2026-09-24 at 01:46 and 01:49 UTC (30 passed each): `naofinito_valor` counted
  2 of 2 by `count(x) - count(CASE WHEN finite ...)`, every measure matched the expected, and on
  the `NaN` row `valor > '-Infinity'::float8`, `valor < 'Infinity'::float8`, `NOT (valor >
  '-Infinity'::float8)` and `(valor > '-Infinity'::float8) IS NULL` were all false, the text
  `NaN`; both are assertions now, the row only in the target, because the stand-in's DuckDB reads
  `NaN > -inf` as true. `redshift.py` at 01:41 read the whole session section: 25 load errors in
  30 days in `sys_load_error_detail` (1.5 s), no external schema, two tables in the schema, none
  with the library prefix.

## The engine and the publication as implemented (2026-09-24)

- `serialize_db.engine.redshift` and `serialize_db.publication` were written on 2026-09-24 and
  ran only on the stand-in; no command of theirs reached the target. What the code assumes about
  the target and the first target run reads: a missing relation answers SQLSTATE `42P01` or a
  message with `does not exist` (`relation_missing`, used by `name_in_use` and the control table
  check); an existing one `42P07` or `already exists`; the `1023` text of a serializable
  violation; `svv_all_columns.data_type` spelled `character varying`, `numeric`, `timestamp
  without time zone`, `double precision`, `super` (the publication compares by type family, so
  `varchar`, `decimal` and `timestamp` also match); the manifest of an `UNLOAD` to a prefix with
  `=` keeps the `=` unencoded and names the `PARALLEL OFF` file `000.parquet` (target reading of
  2026-09-24, `redshift.unload_hive.files`). `.claude/memory/OPEN_QUESTIONS.md`
- The first target run of the engine suite (2026-09-24, 05:10 and 05:12 UTC) passed five of the
  six cases: `COPY ... MANIFEST FILLRECORD` of a delta-rs file, the `UNLOAD` stream equal to the
  cursor `query`, the loader's `CREATE TABLE` rolled back by a failed `COPY`, the audit with the
  `_publicado` staging, the export by registering the `UNLOAD` file (its JSON column read as text
  by delta-rs), the `NaN` swap, two ingests in two sessions, and a 10-row `load` at 1.5 s.
  `select current_database()` is described as `name` (OID 19, `type_size` 128), the catalog
  identifier type, which `schema_from_row_description` refused; `NAME` maps to `string` since,
  and the stand-in describes `current_database()` with OID 19. Its value stays `dev` after the
  `USE` (reading of 2026-09-21, recalled by the user on 2026-09-24): the test records it and
  never asserts it. The publication suite did not run:
  the target's `SERIALIZE_DB_TEST_LOCAL_ROOT` folder was missing.
- The battery of 2026-09-24 at 12:38 (the folder created) passed the six engine cases (13:01,
  13:03) and the eight publication cases (13:05, 13:08) twice each. Readings: a missing relation
  answers SQLSTATE `XX000` with `Relation <name> does not exist in the database.`, not `42P01`
  (`relation_missing` matched by the message; the stand-in imitates the target's form since);
  `svv_all_columns` spells the published table `bigint`, `date`, `timestamp without time zone`,
  `double precision`, `numeric` (18, 2), `character varying` with the width and `super`, and the
  reconciliation found no diff; the Redshift `COPY ... MANIFEST` loaded the DuckDB-written files
  (`DECIMAL` in `INT64`, `TIMESTAMP` in `INT64` microseconds, `DATE` in `INT32`, the JSON column
  through the `VARCHAR(65535)` staging); the concurrent publication surfaced as
  `ExecutionConflict` carrying the `1023`; a failed `COPY` is a `ProgrammingError` with nothing
  published; two published tables at `AUTO` join with `DS_DIST_ALL_NONE`; the `UNLOAD` file's
  `SUPER` column registered by `export_partition` reads as `VARCHAR` text through `delta_scan`; a
  10-row `load` took 1.66 s to 2.06 s. `/mnt/project-files/readings/`
- The publication of the whole base (2026-09-24, the 16:51 battery, from `main` with #73):
  `publish --init` created `sbx_aco_decon.serialize_db_publications`, `--tables cad_contas`
  published version 1, `--max-workers 4` skipped it ("a versão 1 já está publicada") and
  published the other 11 tables with every partition, `cad_lancamentos` at version 4 with
  141,901,795 rows through `COPY ... MANIFEST` over the files the load's DuckDB `COPY` wrote, and
  `--status` read the 12 tables as `prod_<table>` with published equal to current and no pending
  partition; the CLI printed no duration then, so nothing of the publication's time was read;
  since the user's decision of the same day each published table's log line carries the
  partitions, the time and the process's peak RSS.
- The publication of the whole base by channel (2026-09-26, 8 vCPUs, the root loaded that day):
  `publish --init` created the control table; `snapshot --name carga-2026-09-25` (12 tables,
  `cad_lancamentos` at version 5) and `channel --name default --snapshot carga-2026-09-25`;
  `--tables cad_contas --channel default` published version 1 in 3.5 s (peak 247 MB) and
  `--max-workers 4 --channel default` the other 11: the unpartitioned tables 3.5 s to 4.8 s,
  `cad_contratos` 31.2 s, `cad_operacoes` 53.1 s, `rel_contrato_operacao` 56.5 s and
  `cad_lancamentos` (5 partitions, 283,835,743 rows) 295.1 s, the process peaking at 266 MB. On
  2026-09-24 at 23:25 (16 vCPUs) the same tables took 21.5 s, 32.5 s, 40.3 s and 153.9 s
  (141,901,795 rows) at 273 MB: the time follows the rows, about 0.96 and 0.92 million rows per
  second, and the peak does not. `--status` read the 12 `prd_<table>` with published equal to
  current. `--channel current` and `--snapshot carga-2026-09-25 --tables cad_contas` answered
  `a versão <n> já está publicada` for every table, because the snapshot is the current version:
  a revert over the base needs a commit after the snapshot.
- The whole base published again by channel (2026-09-27, 8 vCPUs, the same base), the first time
  with the credentials clause built for each `COPY`: `--init` created the control table again;
  `cad_contas` 3.4 s at 249 MB, the unpartitioned tables 3.4 s to 4.0 s, `cad_contratos` 37.9 s,
  `cad_operacoes` 60.5 s, `rel_contrato_operacao` 66.1 s and `cad_lancamentos` 328.5 s at 270 MB,
  11% to 21% slower than on 2026-09-26 with the cause unmeasured (the credential read takes under
  0.05 s and `cad_lancamentos` runs five `COPY`). The publication suite passed its 8 cases twice on
  the same code.
- A positional `COPY` cannot load a subset of a file's columns (the column list must match the
  file's count, reading of 2026-09-21), so the audit's staging of the pinned version carries every
  contract column and is the same `exec_<id>_<tabela>_versao_<versão>` as `pinned_delta()`, loaded
  once per execution and version, and only when a check that cites it runs. The stream's schema
  comes from the `row_desc` of `select * from (<texto>) as t limit 0`, and each batch of the
  `UNLOAD` file is cast to it (`INT96` coerced to microseconds, `SUPER` as text).
- The appender of a table with a JSON column loads through a `CREATE TEMP TABLE` staging with the
  JSON in `VARCHAR(65535)` and `INSERT ... JSON_PARSE`, because the Parquet `COPY` into `SUPER`
  needs `SERIALIZETOJSON`, never read on a small string; the export serializes the column with
  `JSON_SERIALIZE` so the `UNLOAD` file carries text.
- The review of 2026-09-28 fixed on the stand-in: the appender's `COPY`, direct and through the
  `_carga` staging, lists the file's columns, the first batch's (`COPY <alvo> ("a", "b") FROM ...
  FORMAT AS PARQUET FILLRECORD`), because the positional Parquet `COPY` put a nullable middle column
  the batch lacked into the next column's values (`largura` into `altura`, no error); the target
  read the column list and `FILLRECORD` each alone on 2026-09-21, not together, and
  `test_appender_loads_a_batch_without_a_middle_column` read the combination passing in the target
  on 2026-09-28 at 20:14 and at 23:09. The text path (`literal_text`, the `stream` of a ready text)
  writes each `:` of a quoted region as `\:` before `sa.text()`, which read `':b'` as a bind and
  rendered `'a NULL'` (`query` `[1]`, `stream` `[]`), and repeats a client value's backslash before
  `:`, because the compiler's `BIND_PARAMS_ESC` also acts on the rendered literals (`r"ref \:x2"`
  reached the `UNLOAD` as `ref :x2`). `transaction()` runs `COMMIT` and `ROLLBACK` inside the
  transaction: an `InterfaceError` at `COMMIT` rises with the outcome unknown, where the old code
  reconnected and repeated `COMMIT` outside a transaction, which returned success. A second
  `RedshiftAppender.close` does nothing (it ran another `COPY` of the deleted file).
- The user's decision of 2026-09-28 extended the column list to every `COPY` from Delta (`ingest`,
  `pinned_delta`, publication): `delta.copy_manifest` reads each file's footer once, in series,
  groups the files by their column-name tuple and writes `1.manifest`, `2.manifest` in a folder,
  one `COPY <staging> (<cols>) ... MANIFEST FILLRECORD` each, because the files written before a
  middle column lack it, delta-rs writes a new column at the end of its files, and a reordered
  model shifted values without error on the stand-in (`novo` got `a1`, `a` got `b1`, `b` null).
  `get_add_actions` lists the newest commit first, so the actions are sorted by `path`. The new
  `redshift` cases of `tests/test_engine_redshift.py` and `tests/test_publication.py` read the list
  with `FILLRECORD` passing in the target on 2026-09-28 at 20:14 and at 23:09, and reading the
  footers cost the base's publication 0.5% to 2.0% per partitioned table on 2026-09-29, with one
  file per partition.
- A Parquet `COPY` without `MANIFEST` reads its path as a key prefix, and a prefix that matches no
  object loaded nothing without error in the target on 2026-09-28 (inferred from
  `test_appender_copies_the_file_at_close` failing with `DID NOT RAISE` four times; until PR #104
  its double called itself, and the `RecursionError` passed as the expected error, so that `COPY`
  never ran in the target before). The documentation: `mandatory: true` makes the `COPY` terminate
  when an entry's file is missing, and a missing or malformed manifest fails the `COPY`. The
  user's decision of 2026-09-28 ("Manifesto") puts the `appender`'s `COPY` on a manifest written at
  `close` beside the file, `<uuid>.manifest`, with the file as its only entry, `mandatory: true`
  and `content_length` from `Storage.size`; `copy_text(manifest=False)` has no caller in `src/`
  since. The stand-in reads a path without `MANIFEST` as a prefix (`object_uris`), returns an empty
  result without error when nothing matches, and fails a missing manifest with its own message and a
  missing mandatory entry with the target's (below); on it, the old code fails as in the target, and
  `mandatory: false` fails both the local and the target test. In the target, on 2026-09-28 at
  23:09, the missing mandatory file failed the `COPY` with `Spectrum Scan Error: File not found` and
  SQLSTATE `XX000` in the four runs, loading no row (`redshift.engine.copy_missing_mandatory_file`);
  the stand-in gives the same message and SQLSTATE since the user's decision of 2026-09-29
  (`decisions.md`), and the test still only records it.
- The Redshift audit's refused ingest printed in the target on 2026-09-28 as `Cannot insert a NULL
  value into column valor` (code 8007) and `Invalid input` (code 8001, `JSON_PARSE() error:
  End-of-input inside object or array: {`), both `XX000`, with the driver's whole dict, the
  server's source path included. `RS-12` counted 85 load errors in 30 days that day (no row on
  2026-09-26 and 2026-09-27, 25 on 2026-09-24).
- The stand-in maps DuckDB's `TransactionContext Error: Conflict on tuple deletion!` to the
  `1023` message, catalog `does not exist` to `XX000` with the target's `Relation <name> does not
  exist in the database.` (since 2026-09-24; `42P01` before) and `already exists` to `42P07`, and lists
  `svv_all_columns` from the DDL it remembers, in Redshift's spelling; the concurrent publication
  test pauses every connection of the second publication after its control-row read through the
  `driver_connect` seam. `tests/emulator.py`
- A sandbox table outlived its run in the target: the `svv_all_tables` listing `RS-8` prints showed
  on 2026-09-29 at 13:31 `exec_poc_faa78dd7_cad_append_0` among 17 tables, where the listings of
  2026-09-27 and 2026-09-28 held only `teste` and `teste3`, and `RS-19` resolved it by name after
  the `USE`. `RS-8` itself counts only names starting with `serialize_db` (`TABLE_PREFIX`: the
  control table and the Redshift suite's tables), so its 1 of 17 was `serialize_db_publications`,
  and the sandbox's `exec_` tables never enter its count. `sys_query_history`, read by the library's
  identity at 16:43, shows execution `poc-faa78dd7` from 00:29:01 to 00:29:06 UTC: the main session
  created only that table, two extra sessions each created and dropped a temporary
  `exec_poc_faa78dd7_cad_append_0_carga` (the first round's two appends), and at 00:29:06 the main
  session ran `DROP TABLE IF EXISTS "sbx_aco_decon"."exec_poc_faa78dd7_cad_append_0"` with `status`
  `success`. No other table of the execution was created, so the run stopped before round 1, and its
  output is in no report; the passing `-m redshift` run of 00:32 is another execution and left
  nothing. `sys_transaction_history`, which says whether a transaction committed, is denied to the
  library's identity (`42501`). The hypothesis (inferred, put to the user): a Ctrl+C mid-query left
  that query's response pending on the main connection; `redshift_connector` reads until the first
  `ReadyForQuery` of each phase (Parse, Describe and Sync, then Bind, Execute and Sync), so the
  cleanup's `DROP` took the old response as its own, and the connection closed before Redshift
  committed the `DROP`. A second read-only script (the three sessions' statements from 00:28:30 to
  00:29:30 without the `COPY` text, `sys_session_history` and the table's count) went to the user
  and never ran: at 17:00 the user attributed the table to a run they had interrupted and dropped it
  by hand, and the listing of 17:04 no longer held it nor any `exec_` table of the sessions of 13:31
  to 14:13. `sys_query_history` lists DDL and `UTILITY` rows with `status` (`failed`, `success`,
  ...), `error_message`, `lock_wait_time`, `elapsed_time` and `query_text` (up to 4,000 characters),
  a regular user seeing only their own rows; `sys_session_history` gives each session's `status`,
  `start_time` and `end_time`. `cleanup` names a table its `DROP` misses only in `log.warning`; on
  the stand-in, the probe run with `-o log_cli=true --log-cli-level=WARNING` dropped every table
  without a warning. The schema also keeps the base publication of 2026-09-29 (the 12 `prd_*` tables
  and `serialize_db_publications`), so `redshift.engine.control_table_present` read `True` at 13:31.
- `redshift_connector` 2.1.17 does not mark a connection when an exception leaves
  `Connection.handle_messages` in the middle of a response, and each later command on it reads the
  previous command's response, without error (2026-09-29, a fake PostgreSQL wire server in the
  scratchpad, the driver connected as the engine connects it: `max_prepared_statements=0`,
  autocommit on). A SIGINT 0.5 s into a 2 s `SELECT 'lento'` raised `KeyboardInterrupt`; the next
  `SELECT` waited for the slow one and returned `()`, a `DROP TABLE IF EXISTS` returned on its own
  `Parse` response with its execution response unread, and the server ran every command in order and
  got the `close`'s `Terminate` after the last. The engine reuses the connection in `_run`, and the
  user decided on 2026-09-29 to leave it so (`decisions.md`).
- The reconnect of `execute` repeats a command the server may have applied: the driver's
  `InterfaceError` on a closed socket says nothing about the command's outcome, and on the
  stand-in a `COPY` or `INSERT` passed to the server before the drop ran twice (240 rows of a
  120-row partition, 2026-10-04). Since 2026-10-04 `_copy_partition` runs each partition's
  `DELETE`, `COPY` and `INSERT` in `transaction()`, where the drop rises as `InterfaceError`
  after the `ROLLBACK` and the server discards the transaction; the appender already loaded
  inside one, and the publication's `_Connection` never retries.
- The driver raises `InterfaceError` (`BrokenPipe: server socket closed. ...`) only when a read
  returns no bytes; an `ErrorResponse` becomes `ProgrammingError` (`28000` `InterfaceError`, `23505`
  `IntegrityError`); `_send_message` turns only `ValueError("write to closed file")` and
  `AttributeError` into `InterfaceError`, and the socket `flush` that sends each command catches
  only `AttributeError`, so its `OSError` (`BrokenPipeError`, `ConnectionResetError`) passes
  `Cursor.execute` unconverted (redshift_connector 2.1.17, read in its code on 2026-10-09). The
  target showed it the same day, a `BrokenPipeError` on the staging's `DROP` after a dropped `COPY`
  (below), and since then `RedshiftEngine._run` turns an `OSError` from `cursor.execute` into the
  drop's `InterfaceError`, with the original as `__cause__` (`decisions.md`). Since 2026-10-09
  `tests/test_engine_redshift.py` ends an engine session with `pg_terminate_backend` from the parent
  engine, idle, inside `transaction()` and inside a client's `BEGIN`
  (`redshift.engine.terminated_session`, keys `ociosa`, `transacao` and `transacao_do_cliente`) and
  during the `COPY` of `ingest` over a 300,000-row partition
  (`redshift.engine.terminated_during_copy`), and records the error chain, whether the next command
  reconnects and what the server kept; the second battery of 2026-10-09 ran them (below). The
  driver's `in_transaction` changes only in `handle_READY_FOR_QUERY` (`core.py:1493`), so a drop
  leaves it as it was before the failed command; the engine reads it to refuse the primitives and to
  stop the reconnect inside a client transaction, and marks the dropped connection
  (`_connection_dropped`) so that the next command reconnects (read in the driver's code on
  2026-10-09). The stand-in imitates the driver's `InterfaceError` with the open transaction rolled
  back and `in_transaction` left as it was, and with `SERIALIZE_DB_TEST_EMULATOR_BROKEN_PIPE` the
  send's `BrokenPipeError` from the second command sent on a terminated session.
- The battery of 2026-10-05 (16:38 to 20:09 UTC, `environments.md`) read the Redshift version
  `1.0.434008`, against `1.0.436211` in every battery from 2026-09-20 to 2026-09-30, a lower number
  the readings do not explain. Between the end of the battery of 2026-09-30 (15:42 UTC, the control
  table last read present at 15:34) and 16:38 UTC on 2026-10-05, the 12 `prd_*` tables and
  `serialize_db_publications` left the schema: `RS-8` read 0 of 3 tables (`teste`, `teste3` and
  `teste_query_editor`), `RS-19` resolved `teste` after the `USE`,
  `redshift.engine.control_table_present` read `False`, and `publish_redshift --init` created the
  control table again before the base publication; the user dropped every table before the
  battery (statement of 2026-10-05). The `COPY ...
  SERIALIZETOJSON` of the 80,901-byte string into `SUPER` failed with the `1224` message now ending
  in `(Hint: set enable_large_strings_opt_in parameter.)`; `RS-12` counted 56 load errors in 30
  days; the suite's `UNLOAD` named its files `0064_part_00`; the Data API answered in 509 ms and
  515 ms. The threaded APIs of the engine and the publication were measured against their serial
  form the same day (`concurrency.md`).
- The battery of 2026-10-06 (14:24 to 15:40 UTC, `environments.md`) read the same version
  `1.0.434008` and ran the three cases of PR #139 four times each (the engine and publication
  suites twice, the whole Redshift suite twice), with the same reading every time. A `COPY` whose
  column list leaves out a `NOT NULL` column without `DEFAULT` is refused with `42601 NOT NULL
  column without DEFAULT must be included in column list`, and the appender's staging route for a
  table with JSON, whose staging accepts null, fails in the `INSERT` with `XX000 Cannot insert a
  NULL value into column codigo`, no row in either table
  (`test_appender_refuses_a_batch_without_a_not_null_column`). A `COPY` list naming a column the
  staging does not have is refused with `42703 column "extra" of relation "t_rslocal_..." does not
  exist`, the relation named by an internal identifier that carries the producer account
  [inferred], masked here (`test_ingest_refuses_a_delta_column_outside_the_model`). The `COPY ...
  MANIFEST FILLRECORD` of a partition `compact` rewrote in ZSTD loaded its 15 rows beside a
  Snappy partition (`test_publication_loads_a_compacted_partition`). `RS-12` counted 48 load
  errors in 30 days.
- The battery of 2026-10-07 (03:03 UTC on, `environments.md`) read the same version `1.0.434008`;
  `RS-8` read 0 of 3 tables again, the control table and `prd_cad_contas` of 2026-10-06 gone
  before the battery, and `RS-12`'s count of `sys_load_error_detail` returned no row (the quirk of
  2026-09-24, `lessons.md`). The three cases of PR #139 read the same four times. The whole base
  was published again by channel, on 2 vCPUs, after `--init` created the control table:
  `cad_contas` 3.8 s at 243 MB, the unpartitioned tables 2.9 s to 4.4 s, `cad_contratos` 38.9 s,
  `cad_operacoes` 52.3 s, `rel_contrato_operacao` 65.9 s and `cad_lancamentos` (5 partitions,
  283,835,693 rows) 300.2 s at 274 MB, 0.95 million rows per second, against 324.9 s at 285 MB
  with 8 vCPUs on 2026-10-05: the publication's time is the server's `COPY`, not the machine's.
  `--status` read the 12 `prd_<table>` with published equal to current, and `--channel current`
  and `--snapshot carga-2026-09-25 --tables cad_contas` answered `já está publicada` for every
  table. The Data API answered in 461 ms and 167 ms.
- The battery of 2026-10-09 (01:06 to 06:11 UTC, `environments.md`) read the same version
  `1.0.434008`, `RS-8` read 0 of 3 tables again, and `RS-12` counted 36 load errors in 30 days. The
  three engine cases of PR #145 read the same in the four sessions that run them, the engine suite
  and the whole Redshift suite twice each:
  - `test_append_inside_a_client_transaction_is_read`: the server took the `BEGIN` of
    `transaction()` inside the client's open transaction, and later the client's `ROLLBACK`
    outside any transaction, without an error or a notice, the only notice the `COPY`'s `INFO`
    (`Load into table '<prefix>cad_medidas' completed, 2 record(s) loaded successfully.`); the
    `append` loaded its 2 rows, and the ids 1, 2 and 3 stayed, the client's row committed by the
    engine's `COMMIT`. The stand-in refuses the nested `BEGIN`, as DuckDB does. Since 2026-10-09
    the engine refuses `append`, `appender`, `ingest` and `pinned_delta` inside a client
    transaction, read from the driver's `in_transaction` (`decisions.md`), and the case, renamed
    `test_primitives_refuse_a_transaction_the_client_opened_on_the_target`, asserts the four
    refusals and that the client's `ROLLBACK` leaves no row; the docstrings of `session()` and
    `transaction()` say so.
  - `test_nonfinite_double_constant_is_read`: the text of `sql.render` (`SELECT nan AS valor`, and
    the same with `inf` and `-inf`) and the `stream` of the client's value, which `literal_text`
    writes the same way, were refused with `42703 column "nan" does not exist` (`column "inf" does
    not exist` for `inf` and `-inf`), and the driver's parameter in `query` returned `nan`, `inf`
    and `-inf`. Since 2026-10-09 both texts write `'NaN'::float8`, `'Infinity'::float8` and
    `'-Infinity'::float8` (`.claude/memory/sqlalchemy.md`), and the case, renamed
    `test_nonfinite_double_reaches_the_server_by_every_text_path`, asserts that every path returns
    the value.
  - `test_zero_sign_through_copy_query_and_unload_is_read`: of the 1,000 alternating zeros of the
    appender's file and the 2 `INSERT` constants, the 500 and the 1 negative zeros kept the sign in
    the server (the text `-0` and a negative `atan2(valor, -1)`), through the cursor of `query` and
    through the `UNLOAD` file of `stream`.

  The whole base was published by channel on 4 vCPUs after `--init` created the control table:
  `cad_contas` 6.3 s at 248 MB, `cad_contratos` 43.5 s, `cad_operacoes` 58.2 s,
  `rel_contrato_operacao` 77.5 s and `cad_lancamentos` (5 partitions, 283,835,836 rows) 320.9 s at
  280 MB, 0.88 million rows per second, the command 341 s; `--status`, `--channel current` and
  `--snapshot carga-2026-09-25 --tables cad_contas` read as on 2026-10-07. The Data API answered
  in 430 ms and 187 ms.
- `probes/operacao/probe_published_base.py` ran in the target for the first time on 2026-10-09
  (05:12 UTC, 168 s, every check passing), over the base just published by the channel `default`
  at `carga-2026-09-25`. The `EXPLAIN` of the join of `prd_cad_lancamentos` (283,835,836 rows) with
  `prd_cad_contas` (101) on `id_conta` read `XN Hash Join DS_DIST_ALL_NONE`, the inner
  `prd_cad_contas` hashed from a table in `ALL`, with no `DS_BCAST_INNER` or `DS_DIST_BOTH`, so the
  published tables stay `DISTSTYLE AUTO` with no `DISTKEY` (`decisions.md`). The redo of the first
  partition, 2026-01-31 (33,239,719 rows), in a DuckDB execution marked with the snapshot
  `refeito-operacao-18a392d7` took 46.4 s at a process peak of 7,522 MB (`ingest` 0.136 s, `audit`
  12.135 s, `publish_delta` 30.693 s), `cad_lancamentos` at version 6; with the channel on it, the
  publication by channel swapped only that partition, version 6, in 42.8 s at 261 MB (47.6 s for
  the command), and with the channel back on `carga-2026-09-25` it swapped it back, version 5, in
  44.3 s at 258 MB (49.3 s), the other 11 tables `já está publicada` both ways. `prd_cad_lancamentos`
  kept 283,835,836 rows and the `id_lancamento` sum 274,392,499,994,978,967 before the swap, after
  it and after the return, and the channel ended on `carga-2026-09-25`.
- The second battery of 2026-10-09 (18:34 to 22:35 UTC, `environments.md`) read the version
  `1.0.477953`, the first change since the `1.0.434008` of 2026-10-05, the error messages naming the
  package `RedshiftPADB-1.0.377953.0` (`1.0.334008.0` before); `RS-8` read 0 of 3 tables, `RS-12`'s
  count returned no row, and the Data API answered in 427 ms and 558 ms. The four sessions that run
  the engine cases, the engine suite and the whole Redshift suite twice each, read:
  - `test_primitives_refuse_a_transaction_the_client_opened_on_the_target` passed in all four, the
    refusal of PR #149 read in the target.
  - `test_nonfinite_double_reaches_the_server_by_every_text_path`: `sql.render` wrote
    `SELECT 'NaN'::float8 AS valor`, and the same with `'Infinity'::float8` and
    `'-Infinity'::float8`, and the rendered text through `query`, the `stream` and the driver's
    parameter returned `nan`, `inf` and `-inf` (PR #148).
  - `test_session_terminated_by_the_server_is_read` (`redshift.engine.terminated_session`): in the
    idle session the next command reconnected on a new pid; inside `transaction()` and inside the
    client's `BEGIN` the command raised the driver's `InterfaceError` (`BrokenPipe: server socket
    closed. ...`), and the next command, outside, reconnected; 12 of 12 cases.
  - `test_session_terminated_during_the_ingest_copy_is_read`
    (`redshift.engine.terminated_during_copy`): `pg_terminate_backend` ran 1.675 s to 2.080 s after
    the start, right after the `COPY` was sent, which raised the `InterfaceError` 0.34 s to 0.67 s
    after it was sent, and the `ROLLBACK` of `transaction()` raised it again. In 3 of the 4
    sessions the next command, the `DROP TABLE IF EXISTS` of the staging that `ingest` runs after
    the transaction, failed in the driver's send with `BrokenPipeError: [Errno 32] Broken pipe`, an
    `OSError` that `execute` did not catch then: `ingest` raised the `BrokenPipeError`, with the
    `COPY`'s `InterfaceError` in its context, the staging stayed, and the next command on the
    session, `SELECT pg_backend_pid()`, failed the same way, the session stuck on the dead
    connection. In the fourth (`engine_redshift_2.json`) the `DROP` got the `InterfaceError`, the
    engine reconnected and dropped the staging, and the next command ran on a new pid. The sandbox
    table kept 0 rows in all four. What makes the send fail is unread: in `terminated_session`
    the command, the `ROLLBACK` and the next command all wrote to the closed connection and read
    its end, while after the `COPY` the second write failed in 3 of 4 sessions [inferred: a reset
    from the server side arriving before the write, a matter of timing]. The user chose the fix
    the same day (`decisions.md`), and the case now asserts that the staging leaves and the session
    goes on; `.claude/memory/OPEN_QUESTIONS.md` ("A reconexão do motor Redshift no alvo").

  The whole base was published by channel on 8 vCPUs after `--init` created the control table:
  `cad_contas` 3.2 s at 251 MB, the unpartitioned tables 3.2 s to 4.3 s, `cad_contratos` 55.0 s,
  `cad_operacoes` 71.5 s, `rel_contrato_operacao` 88.3 s and `cad_lancamentos` (6 partitions,
  424,598,150 rows) 487.8 s at 286 MB, 0.87 million rows per second, the command 506 s. The three
  mid-size tables, with the partitions of 01:06, took 10.8 s to 13.3 s more than then, a cause the
  battery does not read: the command prints no per-command time (the `DEBUG` log
  `serialize_db.publication.commands`). `--status`, `--channel current` and
  `--snapshot carga-2026-09-25 --tables cad_contas` read as on 2026-10-07.
- `probes/operacao/probe_published_base.py` in the second battery of 2026-10-09 (21:45 UTC, 154 s,
  every check passing), over the base just published at `carga-2026-09-25`: the `EXPLAIN` of the
  join of `prd_cad_lancamentos` (424,598,150 rows) with `prd_cad_contas` (101) on `id_conta` read
  `XN Hash Join DS_DIST_ALL_NONE` again. The redo of 2026-01-31 (33,239,719 rows) took 32.6 s at a
  process peak of 8,162 MB, `cad_lancamentos` at version 7; with the channel on it, the
  publication by channel swapped only that partition, version 7, in 44.3 s at 262 MB (48.5 s for
  the command), and with the channel back on `carga-2026-09-25` it swapped it back, version 6, in
  46.7 s at 262 MB (50.8 s), the other 11 tables `já está publicada` both ways.
  `prd_cad_lancamentos` kept 424,598,150 rows and the `id_lancamento` sum
  538,418,094,708,709,063 before the swap, after it and after the return, and the channel ended on
  `carga-2026-09-25`.
