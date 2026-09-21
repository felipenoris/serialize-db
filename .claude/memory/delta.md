# Delta Lake and delta-rs

Read before code that touches `serialize_db.delta`, a Delta table or the `deltalake` package. Each fact ends with the `docs/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

## delta-rs behavior

- `deltalake` is the Delta project's native Rust implementation, not the reference one:
  `delta-spark` (JVM, `import delta`, 4.4.0 of 2026-08-20) gets protocol features first. delta-rs
  1.6.4 reads but does not write deletion vectors (issue 4512 open), has no `rename_column` (drop is
  PR 4732, column mapping on write is issue 3936), no identity columns and no symlink manifest, while
  its S3 conditional-put commits need no DynamoDB, which delta-spark still documents. Redshift `COPY`
  reads the raw files, so deletion vectors and column mapping stay off with any writer; DuckDB reads
  through `delta-kernel-rs`, and delta-rs `main` pins the fork `buoyant_kernel`. `docs/delta.md`
- `DeltaTable.create` takes the contract schema with nullability, column comments in field
  metadata, partition columns and table properties; `mode="ignore"` makes it idempotent. Time travel
  reads a version with that version's schema, `restore` re-commits an older version, and `vacuum`
  refuses a retention below the table's deleted-file retention (168 h by default) unless
  `enforce_retention_duration=False`. Alembic has no role with Delta as the source of truth: a
  reconcile step applies additive schema diffs and refuses destructive ones. `docs/estrategia.md`
- delta-rs `alter.add_columns` accepts a `nullable=False` column on a table with data and leaves it
  null in every row; `append` casts incoming data to the table type (int32, double and string into
  `long` accepted, decimal(20,4) into decimal(18,2) refused) without changing the table; type changes
  need `mode="overwrite"` with `schema_mode="overwrite"`. The reconcile step must refuse NOT NULL
  additions, and the Arrow cast must enforce types before writing. `docs/estrategia.md`
- Two delta-rs writers on the same version: append plus append both commit; overwrite of the same
  month fails with `CommitFailedError`; overwrites of different months both commit. App transactions
  (`Transaction(app_id, version)`) are recorded but not enforced: a repeated write with the same
  version was accepted. `docs/delta.md`
- `add`/`remove` paths are relative to the table folder: a copied folder opened at the same version
  with the same rows in delta-rs and DuckDB, history and time travel intact. Never register files
  by absolute URI; Iceberg manifests store absolute paths. `docs/delta.md`
- Delta data files do not contain the partition column (it lives in the `add` action), so a
  partition key must derive from a column in the file for Redshift `COPY`; DuckLake keeps identity
  and source columns inside the files. `docs/estrategia.md`
- `DeltaTable.create_write_transaction` with `AddAction` registers Parquet files written by others
  (the `UNLOAD` path) and checks nothing (2026-09-21): a missing path, false statistics and a file
  without a `NOT NULL` column all commit, and the readers obey the action: false min/max make
  delta-rs, `delta_scan` and DataFusion prune the file that holds the rows, a false `numRecords` is
  DataFusion's `count(*)`, the missing column reads null where `write_deltalake` refuses, a value
  that does not cast fails only when its column is read, and `optimize.compact` rewrites registered
  `INT96`/`FIXED_LEN_BYTE_ARRAY` files as `INT64` with statistics. `ducklake_add_data_files` does
  the same registration for DuckLake but failed on a table partitioned by `year()`/`month()`.
  `docs/POC.md`, `docs/estrategia.md`
- Renaming or dropping a column with delta-rs 1.6.4 is a rewrite of every live file in one commit
  (`remove` of all, `add` of all, `metaData`); `dt.alter` has no `rename_column` or `drop_columns`.
  `write_deltalake(reader, mode="overwrite", schema_mode="overwrite")` held 1,140 MB RSS for 135 MB
  of Parquet and 1,960 MB for 269 MB; DuckDB `COPY ... PARTITION_BY ... RETURN_STATS` plus
  `create_write_transaction(mode="overwrite", schema=new)` gave the same commit at a flat 600 MB.
  `schema_mode="overwrite"` with a `predicate` is accepted and switches the schema of the whole
  table, so the other months read the renamed column as null; the library refuses the combination.
  `docs/delta.md`
- delta-rs log cleanup is automatic at checkpoint time and removes log files older than
  `delta.logRetentionDuration` (30 days by default): with `interval 0 days` version 0 became
  unreadable after five commits. `vacuum(keep_versions=[...])` preserves the files of chosen
  versions (database snapshots) while removing those of intermediate versions; `full=True` also
  lists orphan files. A deep copy of a version is `write_deltalake(destino,
  DeltaTable(uri, version=v).to_pyarrow_dataset().scanner().to_reader())`. `docs/delta.md`
- The Delta table folder keeps every data file ever written until `vacuum`: after 20 commits, 20
  files on disk for 14 in the snapshot, and a raw `read_parquet` of the folder returned 330,000 rows
  too many; the `**` glob also reads the checkpoints in `_delta_log/`. Going back to Parquet folders
  by month is either copying the files `get_add_actions()` lists (0.014 s for 14 files; each file
  keeps the schema of its write, so only readers that match by name fill the added columns) or
  rewriting with DuckDB `COPY (SELECT * FROM delta_scan(...)) TO ... (PARTITION_BY (mes))`, which
  gave one `data_0.parquet` per month with 11 threads and keeps the partition column out of the
  files unless `WRITE_PARTITION_COLUMNS true`. `docs/delta.md`

## Performance measured

- On local disk, 3,000,000 rows in 12 files: `delta_scan` aggregates in 0.010 s against 0.006 s for
  `read_parquet` and 0.007 s for a materialized table; 20 point queries took 0.05 s through
  `delta_scan` and under 0.01 s on a table. On S3 from the SageMaker space (300,010 rows, 3 files):
  `delta_scan` aggregates in 0.3 s (0.77 s cold) against 0.06 s for `read_parquet` and 0.002 s on a
  materialized table, which took 0.29 s to build; 20 point queries took 6.0 s through `delta_scan`,
  3.1 s through `ATTACH ... PIN_SNAPSHOT`, 1.3 s through `read_parquet` and 0.013 s on a table.
  Each `delta_scan` rereads the log: materialize every table queried more than once. `docs/delta.md`

## DuckDB access to Delta

- DuckDB `INSERT INTO` an attached Delta table works (commitInfo shows `UNKNOWN`) but writes the
  partition column inside the file, unlike delta-rs; mixing writers breaks positional `COPY`.
  `COPY ... (RETURN_STATS)` plus `AddAction` with stats gives files that both readers prune
  (`Scanning Files: 0/12`). `docs/delta.md`

## The library's use of Delta

- A database snapshot is the library's `{table: version}` set, marked on demand; the user renamed
  it from `fechamento` on 2026-09-19, and the periodicity (quarterly in the examples) is the
  process's choice. Commit key `serialize_db_snapshot`, control file `_serialize_db/snapshots.json`
  at the environment root, written with `IfMatch`, reconstructible from `history()` only while the
  log lasts because `commitInfo` is not in checkpoints. The library stores no schema, file list or
  statistics; the DDL of an old snapshot comes from that version's Delta schema, not from the
  current model. `docs/serialize-db.md`, `docs/delta.md`

## Alternatives assessed

- DuckLake inlines inserts of up to 10 rows into the catalog by default (`DATA_INLINING_ROW_LIMIT`),
  producing no Parquet file (a 10-row insert wrote nothing to the data path); publishing to Redshift
  requires inlining off or a flush. `docs/estrategia.md`
- A DuckLake catalog file served over HTTP attaches read-only with `ATTACH 'ducklake:http://...'`;
  a SQLite catalog on S3 is unsupported by design; without PostgreSQL the model is one writer at a
  time, with the catalog file moved by the library. `docs/estrategia.md`
- PyIceberg 0.12.0 with a SQLite `sql` catalog writes Iceberg without a service: hidden partition by
  `month(data_ref)`, `overwrite` with a filter, rename and add columns, `add_files`; the catalog
  holds one row per table in a 20 KB file. DuckDB `iceberg_scan` needs the `metadata.json` path,
  because PyIceberg writes no `version-hint.text` and names metadata `<N>-<uuid>.metadata.json`.
  Partition transforms on write need the `pyiceberg-core` extra. `docs/estrategia.md`
