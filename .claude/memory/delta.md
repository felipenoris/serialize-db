# Delta Lake and delta-rs

Read before code that touches `serialize_db.delta`, a Delta table or the `deltalake` package. Each fact ends with the `plan/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

## delta-rs behavior

- `deltalake` is the Delta project's native Rust implementation, not the reference one:
  `delta-spark` (JVM, `import delta`, 4.4.0 of 2026-08-20) gets protocol features first. delta-rs
  1.6.4 reads but does not write deletion vectors (issue 4512 open), has no `rename_column` (drop is
  PR 4732, column mapping on write is issue 3936), no identity columns and no symlink manifest, while
  its S3 conditional-put commits need no DynamoDB, which delta-spark still documents. Redshift `COPY`
  reads the raw files, so deletion vectors and column mapping stay off with any writer; DuckDB reads
  through `delta-kernel-rs`, and delta-rs `main` pins the fork `buoyant_kernel`. `plan/delta.md`
- `create_write_transaction` does not refresh the `DeltaTable` object it is called on, and does not
  need it refreshed: three commits in a row on one object (delta-rs 1.6.4, local folder, one
  partition each, `mode="overwrite"` with `partition_filters`) left the object at version 0 with 0
  entries in `file_uris()`, while the log on storage went 1, 2, 3 with one file each. Each commit
  resolves the version against the log on storage. `scripts/migrate_parquet_to_delta.py` reopened
  the table after every partition until the code review of 2026-09-21 measured this and removed the
  reopen; what the in-memory object holds matters to `get_add_actions`, which the load reads once
  before the loop. `plan/POC.md`
- `DeltaTable.create` takes the contract schema with nullability, column comments in field
  metadata, partition columns and table properties; `mode="ignore"` makes it idempotent. Time travel
  reads a version with that version's schema, `restore` re-commits an older version, and `vacuum`
  refuses a retention below the table's deleted-file retention (168 h by default) unless
  `enforce_retention_duration=False`. Alembic has no role with Delta as the source of truth: a
  reconcile step applies additive schema diffs and refuses destructive ones. `plan/estrategia.md`
- delta-rs `alter.add_columns` accepts a `nullable=False` column on a table with data and leaves it
  null in every row; `append` casts incoming data to the table type (int32, double and string into
  `long` accepted, decimal(20,4) into decimal(18,2) refused) without changing the table; type changes
  need `mode="overwrite"` with `schema_mode="overwrite"`. The reconcile step must refuse NOT NULL
  additions, and the Arrow cast must enforce types before writing. `plan/estrategia.md`
- Two delta-rs writers on the same version: append plus append both commit; overwrite of the same
  month fails with `CommitFailedError`; overwrites of different months both commit. App transactions
  (`Transaction(app_id, version)`) are recorded but not enforced: a repeated write with the same
  version was accepted. `plan/delta.md`
- `add`/`remove` paths are relative to the table folder: a copied folder opened at the same version
  with the same rows in delta-rs and DuckDB, history and time travel intact. Never register files
  by absolute URI; Iceberg manifests store absolute paths. `plan/delta.md`
- deltalake 1.6.4 percent-encodes the partition value in the folder name (`p=a%3Ab` for `a:b`,
  `p=d%27agua` for `d'agua`, `p=a%C3%A7%C3%A3o` for `ação`) and the `add` path encodes the folder
  again (`p=a%253Ab`); a value of letters, digits, `_`, `.` and `-` comes out unchanged in both. The
  predicate `p = 'd'agua'` fails with `Unterminated string literal` (2026-09-23,
  `test_deltalake.py::test_partition_value_is_percent_encoded_in_the_folder_and_the_log`). This is
  why the partition value and the `execution_id` follow `[0-9A-Za-z][0-9A-Za-z_.-]*`.
  `plan/delta.md`, `plan/PLAN-STAGE-6.md`
- Delta data files do not contain the partition column (it lives in the `add` action), so a
  partition key must derive from a column in the file for Redshift `COPY`; DuckLake keeps identity
  and source columns inside the files. `plan/estrategia.md`
- `DeltaTable.create_write_transaction` with `AddAction` registers Parquet files written by others
  (the `UNLOAD` path) and checks nothing (2026-09-21): a missing path, false statistics and a file
  without a `NOT NULL` column all commit, and the readers obey the action: false min/max make
  delta-rs, `delta_scan` and DataFusion prune the file that holds the rows, a false `numRecords` is
  DataFusion's `count(*)`, the missing column reads null where `write_deltalake` refuses, a value
  that does not cast fails only when its column is read, and `optimize.compact` rewrites registered
  `INT96`/`FIXED_LEN_BYTE_ARRAY` files as `INT64` with statistics. `ducklake_add_data_files` does
  the same registration for DuckLake but failed on a table partitioned by `year()`/`month()`.
  `plan/POC.md`, `plan/estrategia.md`
- Renaming or dropping a column with delta-rs 1.6.4 is a rewrite of every live file in one commit
  (`remove` of all, `add` of all, `metaData`); `dt.alter` has no `rename_column` or `drop_columns`.
  `write_deltalake(reader, mode="overwrite", schema_mode="overwrite")` held 1,140 MB RSS for 135 MB
  of Parquet and 1,960 MB for 269 MB; DuckDB `COPY ... PARTITION_BY ... RETURN_STATS` plus
  `create_write_transaction(mode="overwrite", schema=new)` gave the same commit at a flat 600 MB.
  `schema_mode="overwrite"` with a `predicate` is accepted and switches the schema of the whole
  table, so the other months read the renamed column as null; the library refuses the combination.
  `plan/delta.md`
- delta-rs log cleanup is automatic at checkpoint time and removes log files older than
  `delta.logRetentionDuration` (30 days by default): with `interval 0 days` version 0 became
  unreadable after five commits. `vacuum(keep_versions=[...])` preserves the files of chosen
  versions (database snapshots) while removing those of intermediate versions; `full=True` also
  lists orphan files. A deep copy of a version is `write_deltalake(destino,
  DeltaTable(uri, version=v).to_pyarrow_dataset().scanner().to_reader())`. `plan/delta.md`
- The Delta table folder keeps every data file ever written until `vacuum`: after 20 commits, 20
  files on disk for 14 in the snapshot, and a raw `read_parquet` of the folder returned 330,000 rows
  too many; the `**` glob also reads the checkpoints in `_delta_log/`. Going back to Parquet folders
  by month is either copying the files `get_add_actions()` lists (0.014 s for 14 files; each file
  keeps the schema of its write, so only readers that match by name fill the added columns) or
  rewriting with DuckDB `COPY (SELECT * FROM delta_scan(...)) TO ... (PARTITION_BY (mes))`, which
  gave one `data_0.parquet` per month with 11 threads and keeps the partition column out of the
  files unless `WRITE_PARTITION_COLUMNS true`. `plan/delta.md`
- The writer's own `minValues`/`maxValues` are JSON numbers, so a wide `decimal` loses rows:
  `decimal(18,2)` holding `123456789012345.21` was written as `123456789012345.2`, and
  `WHERE valor = 123456789012345.21` returned 0 rows in delta-rs and in `delta_scan` with the row in
  the file. `timestamp[us]` is truncated to milliseconds (`...59.999999` becomes `...59.999`) and
  both readers still found the row; `float64` and `string` are exact, and `NaN` stays out of min and
  max. The defect is the writer's, so `publish_partition` carries it too; the client model has no
  `Numeric` column. `test_deltalake.py::test_written_stats_lose_the_row_on_decimal`, `plan/POC.md`
- A table's `description`, `name` and column comments survive `write_deltalake(mode="overwrite")`
  with and without a predicate; `alter.set_table_description` and `alter.set_column_metadata` change
  them in a commit of `commitInfo` and `metaData` alone.
  `test_deltalake.py::test_description_and_comments_survive_overwrite`, `plan/POC.md`
- Two `create_write_transaction(mode="overwrite", partition_filters=...)` of the same partition from
  the same read version: the second raises `CommitFailedError` (`a concurrent transaction deleted
  data this operation read`); the method returns `None` and leaves the calling object at the read
  version. Both writers keep `NaN` out of a `double` maximum, so `delta_scan` answers a range filter
  by the pruning (`valor > 3` 0 rows with the file pruned, `valor >= 2` 2 rows with `NaN` inside,
  because DuckDB orders `NaN` above every number); delta-rs writes `null` for an infinite extreme,
  and `float("inf")` from `RETURN_STATS` would put `Infinity`, invalid JSON, in the log
  (2026-09-23). `plan/POC.md`, `plan/PLAN-STAGE-3.md`, `tests/proof_of_concept/test_deltalake.py`
- The Delta log and the Parquet footer follow different `NaN` conventions (read 2026-09-23). The
  protocol keeps file statistics in the JSON `stats` of the `add` action, `maxValues` being the
  largest valid value, with no `NaN` count; delta-kernel-rs writes `NaN` as the maximum
  (`default-engine/src/stats.rs`), and Delta Spark drops the float min and max it collects from the
  footer of writers that leave `NaN` out (PR #7101, 2026-06-27,
  `collectStats.skipFloatingPointFromFooter` on by default), keeping parquet-mr's, which records
  `NaN` as the maximum. delta-rs copies the footer maximum without the `NaN` into the log, and no
  delta-rs issue covers it. delta-rs honors `delta.dataSkippingStatsColumns` when writing the log,
  while `delta_scan` uses whatever statistics the log carries; the footer keeps the maximum without
  the `NaN`, and `delta_scan` still loses the row by the row-group pruning of DuckDB's Parquet
  reader. `ColumnProperties(statistics_enabled="NONE")` in `WriterProperties(column_properties=...)`
  removes the column's min and max from the footer and the log (its `nullCount` too), and
  `delta_scan` finds the row. `plan/POC.md`, `plan/delta.md`, `plan/PLAN-STAGE-3.md`,
  `tests/proof_of_concept/test_deltalake.py`
- `writer_properties` applies to one `write_deltalake` call, so each partition written by
  `mode="overwrite", predicate=...` keeps or drops the float statistics on its own: a partition
  written with `statistics_enabled="NONE"` on `valor` and one written with the default gave
  `delta_scan ... WHERE valor > 3` the `NaN` row of the first and never opened the file of the
  second, pruned by its maximum 2.5 (2026-09-23). Statistics live per file, one `add` action each,
  with `partitionValues`; the table keeps no aggregate. `plan/POC.md`, `plan/PLAN-STAGE-3.md`,
  `tests/proof_of_concept/test_deltalake.py`

- Read while implementing stage 3 (2026-09-23): `pa.schema(dt.schema())` returns every contract
  type of `arrow_schema(table)` unchanged, with nullability; `alter.set_column_metadata` merges the
  new keys into the field's metadata instead of replacing it; a `Field` from `delta_schema` enters
  `alter.add_columns` with its comment; three `alter` calls on one object committed three versions
  and advanced the object; `ColumnProperties(statistics_enabled="NONE")` leaves the column's
  footer `statistics` as `None`, not `has_min_max` false; the reader of
  `to_pyarrow_dataset().scanner().to_reader()` carries the version's nullability and comments, so
  `write_deltalake(..., mode="error", name=..., description=..., configuration=...)` makes a deep
  copy at version 0 with all three; `get_add_actions(flatten=True)` types `min.<col>`/`max.<col>` by
  the column (`date32`, `decimal128`, `timestamp[us]`) and `partition.<col>` as `string not null`.
  `plan/POC.md`, `plan/PLAN-STAGE-3.md`

- `write_deltalake` splits a partition at the default target file size, 104,857,600 bytes:
  20,000,000 rows of a `BigInteger` and a `Double` from a reader of 100,000-row batches gave files
  of 104,884,565, 104,890,931 and 104,884,007 bytes and one of 16,285,616, and `optimize.compact`
  of that partition committed nothing, since no two files fit the target. `optimize.compact(
  writer_properties=WriterProperties(column_properties={"valor": ColumnProperties(
  statistics_enabled="NONE")}))` writes the merged file without the min, max and `nullCount` of
  `valor` in the log and without its footer statistics, keeping the other columns'. `deltalake`
  1.6.4 is yanked on PyPI with the reason "Issue: #4784": `uv` warns and still installs the
  pinned version (2026-09-25). delta-rs #4784 is a `MERGE` on a table with the change data feed
  on inserting an all-null row for each row a `when_not_matched_insert` predicate rejects,
  affecting 1.6.4 and 1.6.5 (read by the documentation review on 2026-09-25); the package uses
  neither `MERGE` nor the change data feed. `plan/POC.md`, `plan/PLAN-STAGE-9.md`,
  `plan/OPEN_QUESTIONS.md`

- `to_pyarrow_dataset()` gives each fragment a `partition_expression` built from the file's log
  statistics, and a null min or max becomes `column >= null` or `column <= null`, so PyArrow skips
  the file for any filter on that column (`filestats_to_expression_next` in `python/src/lib.rs`,
  read on delta-rs `main` on 2026-09-25); a `nullCount` of 0 adds `is_valid`, and only a
  `nullCount` between zero and the file's rows adds `or is_null`. On 2026-09-25, a file registered
  by the DuckDB engine, whose log has no min and max for `Numeric(18, 2)`, `DateTime` and
  `Boolean`, read 0 rows for `valor > 1`, `quando > '2025-12-31'` and `legado = true` through the
  dataset, and 0 for `valor > 1` through `to_pyarrow_table` and `to_pandas` with `filters`, against
  2, 2 and 1 through `delta_scan`; DuckDB over the dataset registered by `con.register` read 2 for
  `valor > 1` and 0 for the timestamp filter. A `Double` written with
  `ColumnProperties(statistics_enabled="NONE")` read 0 for `valor IS NULL`, `valor > 0` and
  `valor = 1.0`, and all 5 rows for `valor IS NOT NULL`. In a table partitioned by `particao`
  with that `valor`, `particao == 'a'`, `particao == 'b'` and `id > 3` read the right 2, 3 and 2
  rows through the dataset, and `valor > 1.5` read 0 of 2: the loss is only in a filter on the
  column itself, and `_arrow_reading` in `delta.py` filters the dataset only by the partition
  column. `delta.dataSkippingStatsColumns` without the affected columns leaves them out of the
  guarantee, and the engine file's filters read 2, 2 and 1. `plan/POC.md`,
  `plan/OPEN_QUESTIONS.md`, `tests/proof_of_concept/test_deltalake.py`

## Performance measured

- On local disk, 3,000,000 rows in 12 files: `delta_scan` aggregates in 0.010 s against 0.006 s for
  `read_parquet` and 0.007 s for a materialized table; 20 point queries took 0.05 s through
  `delta_scan` and under 0.01 s on a table. On S3 from the SageMaker space (300,010 rows, 3 files):
  `delta_scan` aggregates in 0.3 s (0.77 s cold) against 0.06 s for `read_parquet` and 0.002 s on a
  materialized table, which took 0.29 s to build; 20 point queries took 6.0 s through `delta_scan`,
  3.1 s through `ATTACH ... PIN_SNAPSHOT`, 1.3 s through `read_parquet` and 0.013 s on a table.
  Each `delta_scan` rereads the log: materialize every table queried more than once. `plan/delta.md`

## DuckDB access to Delta

- DuckDB `INSERT INTO` an attached Delta table works (commitInfo shows `UNKNOWN`) but writes the
  partition column inside the file, unlike delta-rs; mixing writers breaks positional `COPY`.
  `COPY ... (RETURN_STATS)` plus `AddAction` with stats gives files that both readers prune
  (`Scanning Files: 0/12`). `plan/delta.md`
- A Delta schema whose fields carry `parquet.field.id` metadata (what `DeltaSchema.from_arrow`
  keeps from an Arrow `PARQUET:field_id`) makes `delta_scan` read every column as null: for a
  DuckDB `COPY` file, a delta-rs file with the ids and one without, while delta-rs and
  `read_parquet` read the values; without the key the same three files read correctly
  (2026-09-21, DuckDB 1.5.5, delta extension `45c4087`). `serialize_db.schema.delta_schema` drops
  the key before `from_arrow`, and the versioned `.delta.json` files carry no ids.
  `plan/POC.md`, `plan/PLAN-STAGE-1.md`
- `delta_scan` prunes partition files by `=`, by a one-value `IN`, by `BETWEEN` and by `>=`, and
  opens every file for an `IN` of two or more values and for an `OR` of equalities; a range added
  beside the `IN` prunes to the range, and `EXPLAIN ANALYZE` of that form fails with
  `InternalException: ... total_files inconsistent!` while the query and a `CREATE TABLE AS` run.
  Read pruning through `CALL enable_logging('FileSystem')` and the `OPEN` messages of
  `duckdb_logs` (2026-09-23, DuckDB 1.5.5). `plan/delta.md`, `plan/PLAN-STAGE-4.md`,
  `tests/proof_of_concept/test_deltalake.py`

## The library's use of Delta

- A database snapshot is the library's `{table: version}` set, marked on demand; the user renamed
  it from `fechamento` on 2026-09-19, and the periodicity (quarterly in the examples) is the
  process's choice. Commit key `serialize_db_snapshot`, control file `_serialize_db/snapshots.json`
  at the environment root, written with `IfMatch`, reconstructible from `history()` only while the
  log lasts because `commitInfo` is not in checkpoints. The library stores no schema, file list or
  statistics; the DDL of an old snapshot comes from that version's Delta schema, not from the
  current model. `plan/serialize-db.md`, `plan/delta.md`

- The archive copies instead of rewriting (2026-09-24, user decision): `deep_copy` creates the
  destination with `DeltaTable.create` from the version's own schema, name, description and
  configuration, copies every file the version's log lists with `Storage.copy` to the same
  relative path, and commits one `overwrite` per partition with `AddAction`s rebuilt from
  `get_add_actions(flatten=False)` (`path`, `size_bytes`, `num_records`, the `null_count`, `min`,
  `max` and `partition` structs), keeping min and max only for the exact types as `register_files`
  does; the read-back is the row count by both readers against the sum of the actions, because an
  archived version may carry a schema older than the current model. `history()` entries carry the
  commit's custom metadata as top-level keys, newest first. Since 2026-09-24 `deep_copy` resumes
  an interrupted copy: with a table at the destination it skips the partitions whose files the
  destination already registers (no commit), copies the others, and refuses with
  `RegistrationRefused` a destination registering a file the version does not list; the repeat
  over a complete copy returns the same version, and `serialize-db archive` calls it for every
  table instead of skipping an existing destination, which had let a half-copied table pass as
  archived. In the target on 2026-09-24 at 16:51 the whole `archive` copied the 21 files of the
  12 tables (one commit per partition, the partitions in the reverse order of the load) and moved
  the entry; the resume path ran only on the stand-in. Since the user's decision of the same day
  `deep_copy` logs each partition's copy time, and `serialize-db archive` prints the time and the
  peak RSS per table. `plan/PLAN-STAGE-9.md`, `plan/POC.md`

## Alternatives assessed

- DuckLake inlines inserts of up to 10 rows into the catalog by default (`DATA_INLINING_ROW_LIMIT`),
  producing no Parquet file (a 10-row insert wrote nothing to the data path); publishing to Redshift
  requires inlining off or a flush. `plan/estrategia.md`
- A DuckLake catalog file served over HTTP attaches read-only with `ATTACH 'ducklake:http://...'`;
  a SQLite catalog on S3 is unsupported by design; without PostgreSQL the model is one writer at a
  time, with the catalog file moved by the library. `plan/estrategia.md`
- PyIceberg 0.12.0 with a SQLite `sql` catalog writes Iceberg without a service: hidden partition by
  `month(data_ref)`, `overwrite` with a filter, rename and add columns, `add_files`; the catalog
  holds one row per table in a 20 KB file. DuckDB `iceberg_scan` needs the `metadata.json` path,
  because PyIceberg writes no `version-hint.text` and names metadata `<N>-<uuid>.metadata.json`.
  Partition transforms on write need the `pyiceberg-core` extra. `plan/estrategia.md`

## Drafts of 2026-09-21

- `optimize.compact` commits `add` and `remove` actions with `dataChange: false` and their
  `partitionValues`; a partition with a single file is not compacted and no commit is written, so the
  version does not move. `version_diff` reads `_delta_log/<v>.json` and skips `dataChange: false`.
  `alter.add_columns` takes the Delta type object of a field (`Field(name, <PrimitiveType>)`), not its
  string. `get_add_actions(flatten=True)` is an `arro3` table: convert with `pa.table(...)` before
  `pc.max`. `vacuum` within the 400-day retention lists nothing even with intermediate versions;
  `keep_versions` only matters past the retention, and `vacuum` writes two commits (`VACUUM START`,
  `VACUUM END`) without library metadata. A `publish` that compared versions by equality would abort
  after a `vacuum`, `compact` or `reconcile`: `Execution.publish` compares by `version_diff`.
  `plan/POC.md`, `plan/PLAN-STAGE-3.md`, `plan/PLAN-STAGE-6.md`
- `Schema.to_json()` serializes each field's `metadata` map in arbitrary order (two consecutive
  generations of the same model differed) and writes `PARQUET:field_id` as an integer
  `parquet.field.id`; `schema_files` dumps the parsed document with `sort_keys=True` and
  `indent=2`, and the versioned `.delta.json` is that canonical text (2026-09-21). `plan/POC.md`,
  `plan/PLAN-STAGE-1.md`
- `write_deltalake(dt, ...)` with the `DeltaTable` object leaves `dt.version()` at its own commit,
  even with another writer's commit in between (1, then 3 with 2 written by another object);
  `create_write_transaction` does not update the object, and `DeltaTable(uri).version()` after a
  write returns the log's latest version, possibly another writer's (2026-09-22).
  `plan/POC.md`, `plan/PLAN-STAGE-3.md`
- `deep_copy` (stage 3) is `write_deltalake` over the reader of the whole table at a version, so
  its memory grows with the table outside DuckDB's limit: by the 2026-09-23 measurement (1,140 MB
  for 12,000,000 rows, 1,960 MB for 24,000,000) a copy of `cad_lancamentos` (141,901,795 rows) would
  need about 11 GB, unmeasured; the user decided on 2026-09-24 that stage 9 replaces its body by
  `Storage.copy` of each partition's files and `register_files` on a table made by `create_table`,
  one commit per partition, no data through the machine, files identical to the source (the
  DuckDB `COPY` path stays for `export --mode rewrite` and compaction). `optimize.compact` runs in
  delta-rs too, with its default parallel tasks, memory unmeasured.
  `plan/PLAN-STAGE-9.md`, `plan/OPEN_QUESTIONS.md`

## The registration of UNLOAD files (2026-09-24)

- `create_write_transaction` with an empty action list, in an `overwrite` of a partition, raises
  `IndexError` (`deltalake` 1.6.4): `register_files` needs at least one file, so the Redshift
  engine writes an empty Parquet file with the contract schema minus the partition column for an
  empty partition and registers it. `file_from_footer` (protected) reads `null_count` for every
  contract column and min and max for integer, `Double` and date columns only: pyarrow 25.0.1
  exposes no `is_min_value_exact`, and a string's footer min and max may be truncated.
  `partition_values` (protected) lists the partitions with files in a version. `plan/POC.md`,
  `plan/PLAN-STAGE-5.md`
