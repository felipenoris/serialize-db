# Redshift

Read before code on `engine.redshift`, the publication of stage 8, the Redshift suite or `probes/redshift.py`; the scripts that fixed the target are in `examples/`. Each fact ends with the `docs/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

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
  `examples/`, `docs/redshift.md`, `docs/POC.md`
- The target's Redshift, read on 2026-09-20 (`docs/readings/`): workgroup `controladoria-wg`,
  namespace `controladoria-ns`, account 138071776059, base capacity 8, no provisioned cluster; the
  three Redshift APIs and the workgroup host resolve to private IPs, so the temporary credential and
  the Data API work without internet; version `1.0.436211`; `datalake_rw_shared` is `shared` from the
  datashare `controladoria_rw_datashare` (producer account 390403891846) with isolation `UNKNOWN`,
  and `sbx_aco_decon` exists only there. The namespace has no IAM role, default or attached, so
  `IAM_ROLE` is unusable and `COPY`/`UNLOAD` carry the caller's credentials.
  `has_database_privilege(dev, CREATE)` is false and `TEMP` true, so the execution sandbox is either
  a temporary table or the datashare itself. `stv_slices` and `stl_load_errors` are denied
  to a regular user (42501) while `sys_load_error_detail` answers; `pg_settings` on serverless lists
  neither `timezone` nor `enable_case_sensitive_identifier`, which `SHOW` returns. `docs/POC.md`,
  `docs/OPEN_QUESTIONS.md`

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
  intact. `docs/POC.md`, `docs/redshift.md`

## The driver

- With `redshift_connector`, `executemany` makes one round trip per row and the dialect does not
  rewrite it into a multi-row `VALUES`: bulk loads go through Parquet on S3 and `COPY`, small batches
  through `insert(Modelo).values(lista)`. `docs/redshift.md`
- With autocommit off, `redshift_connector` issues `begin transaction` before the first `execute`
  of a cursor, and setting `autocommit = True` afterwards does not close the open transaction: a
  session that ran `USE` before switching autocommit on stayed in one transaction, the denied
  `stv_slices` read aborted it, and every later statement, including the cleanup's `DROP`s, failed
  with 25P02 (target, 2026-09-21). The suite and the library set autocommit right after `connect`,
  before the `USE`. `select version()` comes back with a trailing NUL byte, `current_schema()` is
  null after the `USE`, and `svv_redshift_databases` reports `datalake_rw_shared` as `shared` with
  isolation `UNKNOWN` and `dev` as `local` with `Snapshot Isolation`. `docs/POC.md`

## The reading of 2026-09-21

- `USE datalake_rw_shared` makes two-part names resolve in the datashare, and `current_database()`
  keeps answering `dev` afterwards (probe `RS-19` of 2026-09-21, user confirmation the same day): the
  switch is confirmed by resolving a name (the library creates
  `<schema>.serialize_db_publications` with `IF NOT EXISTS` right after the `USE`; the probe selects
  from a table `svv_all_tables` lists), never by that function, and the suite records the function's
  value as a reading. `has_schema_privilege` and `svv_table_info` after the `USE` remain unread. Other
  readings: `enable_case_sensitive_identifier` off, `datestyle` `ISO, MDY`, `statement_timeout` 0,
  `wlm_query_slot_count` 1, `sys_load_error_detail` answered 0 in 2.4 s; the Data API `select 1` stayed
  `PICKED` for 30 s (23 ms the day before); `iam.simulate_principal_policy` times out in the target
  (no IAM endpoint), so the first `COPY` proves the permission. `docs/POC.md`, `docs/PLAN-STAGE-5.md`
