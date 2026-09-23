# Redshift

Read before code on `engine.redshift`, the publication of stage 8, the Redshift suite or `probes/redshift.py`; the scripts that fixed the target are in `examples/`. Each fact ends with the `plan/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

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
  `examples/`, `plan/redshift.md`, `plan/POC.md`
- The target's Redshift, read on 2026-09-20 (the reports left `plan/readings/` on 2026-09-23 and
  stay in git history): workgroup `controladoria-wg`,
  namespace `controladoria-ns`, account 138071776059, base capacity 8, no provisioned cluster; the
  three Redshift APIs and the workgroup host resolve to private IPs, so the temporary credential and
  the Data API work without internet; version `1.0.436211`; `datalake_rw_shared` is `shared` from the
  datashare `controladoria_rw_datashare` (producer account 390403891846) with isolation `UNKNOWN`,
  and `sbx_aco_decon` exists only there. The namespace has no IAM role, default or attached, so
  `IAM_ROLE` is unusable and `COPY`/`UNLOAD` carry the caller's credentials.
  `has_database_privilege(dev, CREATE)` is false and `TEMP` true, so the execution sandbox is either
  a temporary table or the datashare itself. `stv_slices` and `stl_load_errors` are denied
  to a regular user (42501) while `sys_load_error_detail` answers; `pg_settings` on serverless lists
  neither `timezone` nor `enable_case_sensitive_identifier`, which `SHOW` returns. `plan/POC.md`,
  `plan/OPEN_QUESTIONS.md`

## The datashare, COPY and UNLOAD

- The project schema `sbx_aco_decon` lives in the datashare database `datalake_rw_shared` (user
  decision of 2026-09-20), and writing into it works, proved that day by
  `examples/redshift_copy_unload.py`. `USE <database>` switches the session's database, after which
  `schema.table` is enough; the three-part name binds only a session connected elsewhere, such as the
  Data API. `CREATE TABLE`, `COPY` of a Parquet prefix, `SELECT` and `UNLOAD` all passed, and
  `COPY`/`UNLOAD` reach S3 through the caller's `ACCESS_KEY_ID`, `SECRET_ACCESS_KEY` and
  `SESSION_TOKEN` instead of `IAM_ROLE`, which unblocks a namespace with no attached role; the
  statement carries a secret and never reaches a log, the suite report or a file
  (`tests/conftest.py` masks every credential clause). A datashare write also needs patch 186
  (`1.0.78890` serverless), snapshot isolation on the producer's database and 64 slices; it accepts
  `CREATE`/`DROP`/`SHOW TABLE`, CTAS, `ALTER TABLE ADD`/`DROP COLUMN`, `RENAME`, `TRUNCATE`
  (transactional there), `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `MERGE` and `COPY` with no
  `COMPUPDATE` clause (the Parquet `COPY` rejects it), writes one database per transaction and creates no views. `svv_all_schemas`,
  `svv_all_tables` and `svv_redshift_databases` cross databases; `has_schema_privilege` and
  `svv_table_info` see only the session's. `COPY ... MANIFEST` and
  `UNLOAD ... PARTITION BY ... MANIFEST VERBOSE` passed there on 2026-09-21 over 500,000 rows (4.6 s
  and 0.8 s), and the session's first statement cost 10.8 s. The `UNLOAD` writes `TIMESTAMP` as `INT96` and `DECIMAL(18,2)` as
  `FIXED_LEN_BYTE_ARRAY(8)`, where the library writes `INT64` for both; every column comes out
  `optional`, min and max are present except on the `INT96`, and it fragments by slice (32 files for
  500,000 rows, which `PARALLEL OFF` or compaction undoes). An `INT96` file registered in a
  `timestamp_ntz` table reads back as `timestamp[us]` in delta-rs and in `delta_scan`, values
  intact. `plan/POC.md`, `plan/redshift.md`
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
  `-inf` and `inf`. `plan/POC.md`, `plan/redshift.md`, `plan/PLAN-STAGE-5.md`
- The result description read on 2026-09-23: OIDs 20, 23, 21, 701, 700, 1700, 1043, 1042, 1082,
  1114, 1184, 16 and 4000 for `BIGINT`, `INTEGER`, `SMALLINT`, `DOUBLE PRECISION`, `REAL`,
  `DECIMAL`, `VARCHAR`, `CHAR`, `DATE`, `TIMESTAMP`, `TIMESTAMPTZ`, `BOOLEAN` and `SUPER`;
  `type_modifier` 1,179,654 for `DECIMAL(18, 2)`, `n + 4` for `VARCHAR(n)` and `CHAR(n)`,
  16,384,000 for `SUPER`, -1 elsewhere; `sum` and `avg` of `DECIMAL(18, 2)` come out
  `NUMERIC(38, 2)`, a text literal `VARCHAR`, `1.5` `NUMERIC(2, 1)`; `SUPER` reaches Python as
  `str`. A 10-row load took 0.91 s and 0.97 s by `COPY` against 0.53 s and 0.55 s by a multi-row
  `INSERT` (best of three). `plan/POC.md`, `plan/PLAN-STAGE-5.md`

## The driver

- With `redshift_connector`, `executemany` makes one round trip per row and the dialect does not
  rewrite it into a multi-row `VALUES`: bulk loads go through Parquet on S3 and `COPY`, small batches
  through `insert(Modelo).values(lista)`. `plan/redshift.md`
- With autocommit off, `redshift_connector` issues `begin transaction` before the first `execute`
  of a cursor, and setting `autocommit = True` afterwards does not close the open transaction: a
  session that ran `USE` before switching autocommit on stayed in one transaction, the denied
  `stv_slices` read aborted it, and every later statement, including the cleanup's `DROP`s, failed
  with 25P02 (target, 2026-09-21). The suite and the library set autocommit right after `connect`,
  before the `USE`. `select version()` comes back with a trailing NUL byte, `current_schema()` is
  null after the `USE`, and `svv_redshift_databases` reports `datalake_rw_shared` as `shared` with
  isolation `UNKNOWN` and `dev` as `local` with `Snapshot Isolation`. `plan/POC.md`
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
  reading that reproduces the error. `plan/redshift.md`, `plan/POC.md`
- The driver materializes a result in `execute`: `EXECUTE_MSG` asks the portal for all rows,
  `handle_messages` returns only at `READY_FOR_QUERY`, each `DATA_ROW` lands in
  `cursor._cached_rows`, and `fetchmany` is `islice` over `Cursor.__next__`, which pops that deque.
  `stream` on Redshift always goes through `UNLOAD` and `query` through the cursor (user decision of
  2026-09-23); the suite read 5 rows in the queue before the first `fetchmany` (2026-09-21, 13:35
  and 13:39), now an assertion. `plan/redshift.md`, `plan/PLAN-STAGE-5.md`
- `cursor.description` of `redshift_connector` 2.1.16 is `(name, oid, None, None, None, None,
  None)` per column (`Cursor._getDescription`); the `type_modifier` of each column is in
  `cursor.ps["row_desc"]`, stored by `Connection.handle_ROW_DESCRIPTION`, and the driver itself
  decodes the binary `NUMERIC` with scale `(type_modifier - 4) & 0xFFFF`
  (`Cursor.truncated_row_desc`); precision is `((type_modifier - 4) >> 16) & 0xFFFF`. `RedshiftOID`
  lists `REAL` 700, `BPCHAR` 1042, `TEXT` 25, `UNKNOWN` 705 and `SUPER` 4000, which the driver reads
  as text (code reading of 2026-09-23). `plan/redshift.md`, `plan/PLAN-STAGE-5.md`
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
  escape. `plan/PLAN-STAGE-5.md`, `plan/POC.md`
- The stage 5 readings of `tests/proof_of_concept/test_redshift.py` (seven tests after
  `test_parallel_copy_and_unload_on_two_connections`) ran in the target on 2026-09-23, twice; what
  is left for the next run is in `plan/OPEN_QUESTIONS.md`. `plan/POC.md`
- The stage 1 `ddl` and the stage 4 `audit_sql` cite tables without a schema, so on Redshift the
  engine relies on `SET search_path TO <schema>` after `USE`, which passed on the datashare schema
  on 2026-09-23; one refused measure fails the whole rows check, so
  `test_audit_sql_under_search_path_and_nan_comparison` also runs each measure alone beside the
  expected counters, and the client model's texts on empty tables. `plan/OPEN_QUESTIONS.md`,
  `plan/POC.md`

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
  to the project's role (probe `RS-8`, 2026-09-23), so stage 8 needs another source for the assigned
  distribution (`plan/PLAN-STAGE-8.md`). Other
  readings: `enable_case_sensitive_identifier` off, `datestyle` `ISO, MDY`, `statement_timeout` 0,
  `wlm_query_slot_count` 1, `sys_load_error_detail` answered 0 in 2.4 s; the Data API `select 1` stayed
  `PICKED` for 30 s (23 ms the day before); `iam.simulate_principal_policy` times out in the target
  (no IAM endpoint), so the first `COPY` proves the permission. `plan/POC.md`, `plan/PLAN-STAGE-5.md`
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
  Data API answered in 444 ms: the 30 s `PICKED` was transient. `plan/POC.md`, `plan/redshift.md`
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
  and 1.5 s; Data API 610 ms and 177 ms; `has_schema_privilege` `false` four times. `plan/POC.md`
- Fifth and sixth runs (2026-09-21 13:35 and 13:39 UTC, 12 passed each, the two clean runs stage 0
  required; the two JSON reports left `plan/readings/` on 2026-09-23, in git history, and the
  folder holds the 2026-09-23 runs): with
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
  checked by the audit. `plan/POC.md`, `plan/PLAN-STAGE-8.md`

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
  every cursor of a connection is the same session. `plan/redshift.md`
- The Redshift engine keeps one session per execution under a `threading.Lock` (user decision of
  2026-09-22), the `exec_<id>_*` tables stay permanent in the datashare schema, and the user
  reverted the temporary-table proposal the same day; a temporary table the pipeline creates in
  the session is lost when the engine reconnects. `plan/PLAN-STAGE-5.md`
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
  `decisions.md`). `plan/redshift.md`, `plan/POC.md`, `plan/PLAN-STAGE-8.md`
- An extra session (`new_session()`, 2026-09-23) is another connection with its own temporary
  credential and `USE`: it sees the `exec_<id>_*` tables the main session committed and not its
  temporary tables; `run.ingest` of more than one table opens one per table. The suite's two
  parallel `COPY`s, each opening its connection inside the task, took 4.3 s and 3.8 s in the target
  on 2026-09-21. `plan/PLAN-STAGE-5.md`

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
  the JSON column is `SUPER`; the new text waits for the next suite run. `plan/PLAN-STAGE-4.md`,
  `plan/POC.md`, `plan/OPEN_QUESTIONS.md`
