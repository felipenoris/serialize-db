# User Instructions

Do not edit this section. You're free to edit all other sections of this file.

These are the instructions written by the user.

## Project Goals

The main goal is to build a library that takes as input a database stored as a set of Parquet files, ingests the necessary files into DuckDB or Redshift while enforcing a schema, runs a pipeline on that data, and exports the updated database back to Parquet files, ideally incrementally. Only the data required by the pipeline is ingested, since the full database may not fit on the local machine (in RAM or on disk). DuckDB or Redshift is used as a sandbox to generate new data and as a tool to run complex queries.

- The source of truth for this database is the Parquet files.

- The set of Parquet files conforms to a schema.

- A core feature is the ability to import and export the database in Parquet format, ideally incrementally.

- Use the SQLAlchemy ORM to model the database schema in Python.

- Support schema migration. Alembic is an option.

- The pipeline runs on DuckDB or Redshift. Both engines must be supported.

- Data will be published to Redshift for clients.

- The project has access to a single Redshift schema. A prefix on table names could implement a namespace that separates production, development and pipeline execution.

- Parquet, DuckDB and Redshift are columnar, with little emphasis on table constraints:

  - Parquet: holds the data of a single table, with no concept of primary keys, foreign keys or autoincrement columns.

  - DuckDB: enforces constraints, at a performance cost.

  - Redshift: constraints are informational only.

## References

- <https://duckdb.org/docs/lts/guides/performance/schema#constraints>
- <https://docs.aws.amazon.com/redshift/latest/dg/t_Defining_constraints.html>
- <https://github.com/felipenoris/etl-cookbook-tutorial>

# Guidelines

## Project

- This repo is a Python library.

- Use `uv` to manage the Python version and the dependencies.

- Use Python 3.13.

- Use `pytest` to write tests.

- Use [`pdoc`](https://pdoc.dev/) to generate the documentation as static HTML.

## Tools installed in the current environment

- `uv`

- `gh`

- `cargo`

- `git`

## git

When you need to commit in this repo, do it on a new branch prefixed with `claude/` and open a PR. While a PR you opened is still open, every new commit goes to that PR's branch; do not open another branch or PR. Submit your commits incrementally. The user will merge when needed. After the merge, sync the local copy of the repo to the `main` branch (sometimes the user will do that for you).

## AWS target environment

- SageMaker Unified Studio (Ubuntu Linux, amd64)

- S3 (project bucket)

- Redshift: read and write permissions on a single schema

## Language convention (important)

All prose in this repo is **Brazilian Portuguese (pt-BR)**: README files, code comments, docstrings, printed output, test messages, and shell-script comments. When editing or adding content, keep writing in pt-BR to match.

Database identifiers (schema names, table names, column names) are in Brazilian Portuguese (pt-BR).

Code identifiers (variable and function names) are in English.

This `CLAUDE.md` is in English, for AI-assistant tooling.

## `REFERENCES.md`

Record in this file every website you visit when searching for information.

## Chat

Chat with the user in Brazilian Portuguese. Tag your answers with [guess] or [uncertain] when you are not confident.

Challenge the user with questions when they propose something bad or wrong, or when you have a better idea.

Be direct and succinct.

## Writing style

These rules apply to every text in the repository: Markdown, code comments, and this file. The test for
a sentence is whether the reader does something with it. Facts, identifiers, measurements and their
dates, commands, quoted output and a log entry's provenance are never cut; the words around them are.

1. **A heading names its subject**, in the file's own vocabulary: "Account tree", "The identities the
   battery runs as". Not a label that needs the body to be understood ("The map"), and not a
   rhetorical tail ("and why each one is the one it is").
2. **No counts in headings or titles.** "The two identities", "Four rules for reading this picture",
   "ONE MATCHER, TWO INPUTS" go stale the day an item is added, and the number is not the information.
   Write "The identities", "Rules for reading this picture", "The DNS Firewall coverage matcher".
3. **Every sentence has a subject, headings included.** "True now, and expected to change" becomes
   "Readings a later stage changes".
4. **Lead with the subject**, then its status: "The WireGuard Elastic IP is not allocated here; it is
   transferred from Sandbox." Never a preamble that reveals the subject at its end ("WHAT IS NOT HERE,
   AND THE ABSENCE IS THE STEP AFTER THIS ONE: the WireGuard Elastic IP").
5. **No capitals for emphasis.** The plain sentence carries the same fact. Bold marks the one term a
   reader scans for, never a whole sentence.
6. **No revision artifacts in headings or prose.** Edit dates, "(revised 2026-08-17, by the user)",
   "row four, inverted", "REWRITTEN 2026-09-06", "the list grew one entry at step 3" belong to git
   history. A date stays when it dates a measurement.
7. **A code comment states what the code does and the constraint it obeys.** It does not narrate how the
   file got here, argue that something is "expected rather than a finding", or cite a lesson in place of
   the fact. A comment the reader of the code does not need is deleted.
8. **Delete what carries no information**: rhetorical connectives ("and that is the point", "which is
   the whole reason", "said out loud"), a second phrasing of the same fact, and a justification another
   document already records. One short sentence beats a chain of clauses joined by dashes.
9. **Shorter is the goal; a fact removed is a defect.** When a cut would drop a measurement, an
   identifier or a verdict, keep the sentence.

## `prompts.md`

Never read the file `prompts.md`.

# Claude Memory

Use this section to store your memory for this project. Keep this file within a size budget of 50 KB.

## Repository index

Read the file listed here before researching its subject again. Each document names the pages it
came from, and `REFERENCES.md` collects every URL consulted so far, grouped by subject: Parquet
format, Redshift, DuckDB, PyArrow, SQLAlchemy, pandas, PyIceberg, Glue Data Catalog, Athena, Lake
Formation, SageMaker Unified Studio, S3, Python packages, Delta Lake, DuckLake, Hudi, SQL tooling
(SQLGlot, SQLMesh, dbt, Ibis, dlt), data-contract tools, Rust/PyO3 and consulted repositories. New
research appends to the matching group.

| File | Subject |
| --- | --- |
| `README.md` | `uv init --python 3.13`, `uv sync --group dev`, the test layout (`tests/` for the package, `tests/proof_of_concept/` for the proofs of concept and the study suites of the external libraries) with the rule that each authorization variable enables the writes under it and the variables of the three targets, the probes, the delta-rs credentials and proxy note, and the offline recipe (`prepare_offline.sh`, `.tar.gz` transfer, `.venv/bin/python -m pytest`). |
| `prepare_offline.sh` | Makes the project folder self-contained for the target without internet: managed Python in `.python/`, the package and every `pyproject.toml` group in `.venv/` (`uv sync --all-groups`), DuckDB extensions in `.duckdb/`, all links relative. **Review it whenever a dependency is added**: Python packages come in through `uv sync`; a new DuckDB extension, a Python version change or another runtime asset is added by hand, and the user reruns it before packing. It runs on any platform and stops when `.python/` has no interpreter; only a folder prepared on Linux x86_64 serves the SageMaker space. |
| `probes/` | Read-only scripts that photograph the environment (`.venv/bin/python probes/<script>.py`; the report also goes to `probes/output/`, ignored by git, for pasting into the conversation), indexed by `probes/README.md` in the shape of the `aws/` scripts of felipenoris/AWS-DataScience: `space.py` (the space from inside), `bucket.py` (the bucket under the root), `diagnose_aws.py` (the access the S3 suite needs and its maintenance verdict; own format), `redshift.py` (`SERIALIZE_DB_REDSHIFT_*` or the project connection, clusters and workgroups with the default IAM role for `COPY`, the session), `catalog.py` (the re-evaluation trigger: Glue, Athena, Lake Formation, S3 Tables), over `probelib.py`. `sagemaker-studio` stays out of the project: it drags unpinned `deltalake`, `duckdb` and `pandas` (a test install downgraded duckdb to 1.5.1); the probes import it from the system interpreter. |
| `docs/guia.md` | ETL practices the pipeline follows: immutable monthly partitions with idempotent replacement, write-audit-publish, the schema contract, and the open question about committing metadata atomically on S3. |
| `docs/schema.md` | DDL from the ORM models, `Table.info["serialize_db"]` (`partition_by`, `sort_key`, `redshift`), constraint policy per backend, the type table from SQLAlchemy to Arrow, Delta, DuckDB and Redshift, SQL portability between the engines, and the JSON field per layer. |
| `docs/parquet.md` | Parquet file layout and every metadata structure, inspection with DuckDB and with PyArrow, partitioning, query optimization by layer, and import and export in DuckDB and in Redshift. |
| `docs/duckdb.md` | DuckDB as the execution sandbox. |
| `docs/redshift.md` | Redshift as the publication database and the second execution engine; it opens with the diagnostic queries for a session. |
| `docs/sqlalchemy.md` | SQLAlchemy as the schema contract: metadata, reflection, deferrable constraints, Core and ORM for DDL and DML, server-generated keys, SQL generation per dialect (`compile`, dialect objects and paramstyles, `literal_binds`, `render_postcompile`, `create_mock_engine`, `echo`), the `Numeric` float conversion, what each dialect and Parquet support, the verdict per part, the recommendation without the compatibility premise (own contract with Arrow as canonical form, hand-written SQL validated by SQLGlot) and the gradual replacement of runtime compilation by generated SQL text (`param`, `prefixed`, `render`, `write_sql_files`, `execute`). |
| `docs/delta.md` | Delta Lake as the source of truth: folder layout and log actions, Delta versus Iceberg, the implementations (delta-spark, delta-rs, Delta Kernel) and the delta-rs gaps, S3 requirements, types and JSON, table creation from the model, schema evolution with the measured rename/drop rewrite and what replaces Alembic, transactions, conflicts and restore, DML, ingestion and export back to Parquet folders by month, pipeline steps, DuckDB and Redshift access, performance measurements, relocation and SQLAlchemy support. |
| `docs/PLAN.md` | The plan (pt-BR): the decisions with the premises behind them, the state of the project (repository, verified proof of concept, environment readings, pending items), the rules every stage obeys, the package layout with dependencies, configuration and test policy, the stages 0 to 9 with the primitives of each module (`schema`, `sql`, `storage`, `delta`, `engine.duckdb`, `engine.redshift`, `execution`, `load`, `cli`), the monthly pipeline with the `Execution` API, and the order of work. |
| `docs/estrategia.md` | Rationale and comparisons only: the premises, table layers without a catalog service (Delta via delta-rs, DuckLake, Iceberg without a catalog, Hudi, hand-rolled manifests) against the requirements, the Redshift path by `COPY ... MANIFEST`, the SQL layer options (SQLAlchemy Core, SQLGlot, SQLMesh, dbt, Ibis, dlt), contract and audit tools, why Alembic leaves, the Rust/PyO3 assessment, why each layer was chosen or rejected, and the maturity assessment of Delta against Iceberg with the re-evaluation trigger. |
| `docs/serialize-db.md` | The library's modeling: features, own metadata (commit keys, `_serialize_db/snapshots.json`, `serialize_db_publications`) and the flow of each use case; the primitives live in `docs/PLAN.md`. |
| `src/serialize_db/model/` | Declarative ORM models of the accounting, management and projection tables. |

`docs/duckdb.md`, `docs/redshift.md` and `docs/delta.md` share a section order: data organization and
the differences from PostgreSQL, supported types with `DECIMAL` and JSON, DDL,
`SELECT`/`INSERT`/`UPDATE`/`DELETE`, ingestion, export to Parquet, performance recommendations,
SQLAlchemy support, references. `docs/delta.md` adds schema evolution, transactions, the pipeline,
DuckDB and Redshift access, and relocation.

## Lessons learned

Process lessons from the sessions so far, kept so the same mistake is not paid twice; the
technical facts stay in the list below and in the study suites. A lesson is added at the end of a
unit of work when a mistake cost a retry or a verification changed the plan, with its date.

- **A documented behavior becomes a test assertion only after a probe reproduces it**
  (2026-09-19). Three claims taken from `docs/duckdb.md` and `docs/delta.md` failed as assertions:
  the DuckDB Arrow reader returns zero rows after another command instead of raising;
  `read_parquet` on a single file under `mes=.../` adds `mes` by Hive auto-detection, so a file's
  real columns come from `parquet_schema`; the `DECIMAL` inferred from a pandas column came from all
  the values, not a sample. Run a ten-line probe first, assert the observed value, and record the
  rest in the session report.
- **A claim of "writes nothing" or "needs nothing" is verified in a stripped environment**
  (2026-09-19). The `LOAD` of a known DuckDB extension downloaded it into `~/.duckdb`, found only
  by running with an empty `HOME`; the same kind of run found the dangling `.venv/bin/python`.
  Strip `AWS_*` and the proxy variables, point proxies at a closed port, give the subprocess an
  empty `HOME`.
- **A script that resolves paths by pattern runs on both platforms before it is trusted**
  (2026-09-19). `prepare_offline.sh` had a Linux-only glob and left the macOS venv broken without
  an error; now it stops when the pattern matches nothing. macOS has no `timeout` command: time a
  subprocess from Python.
- **Nothing unpinned enters the project venv** (2026-09-19). Installing `sagemaker-studio`
  downgraded duckdb to 1.5.1. Try a package in a scratch venv, read the versions after any install
  (`probes/space.py` SP-9 does it), and restore with `uv sync --group dev`.
- **The pytest layout has no `__init__.py`.** The root `tests/conftest.py` is imported as
  `conftest` and its folder lands on `sys.path`, so `from conftest import ...` and
  `from delta import ...` work inside `tests/proof_of_concept/`; test-module basenames must stay
  unique across `tests/` and `tests/proof_of_concept/` (a future `tests/test_local.py` would collide
  with the proof of concept).
- **Before a commit**: `uv run pytest` with no variables and again with
  `SERIALIZE_DB_TEST_LOCAL_ROOT` set to the scratchpad, both green; `py_compile` on an edited probe;
  `git status` clean of stray files.
- **Git and shell traps.** After `git mv`, `git add` of the old path aborts a `&&` chain; add the
  new path. In zsh, `--include=*.md` needs quotes. A Bash result above about 50 KB is saved to a
  file instead of shown; read a long document in `sed -n` ranges.
- **A subprocess probe prints its own one-line error.** A DuckDB error inside a subprocess showed
  only `^`; the probe catches the exception and prints `Type: message` to stderr. Probe code held
  in a Python string is a raw string, or `\[` raises a `SyntaxWarning`. "The service answered with
  an error" and "no response" are different verdicts: only the second means the network needs
  maintenance.
- **Restructuring a document is a scripted splice followed by checks** (2026-09-19). Split on
  heading markers with a Python script, then list the headings, grep for references to the removed
  sections and for stale identifiers across the repository, and test every `](...md)` link target.
  Trimming this file used the same idea: list the backticked spans and numbers of the old text
  missing from the new one and review each; a fact removed is a defect.
- **A change the user did not ask for is named in the report.** Moving the primitives out of
  `docs/serialize-db.md` followed from the plan request and was flagged as such; the `Text` rule
  proposed in `docs/PLAN.md` is marked as awaiting the user's confirmation.
- **API details learned by running live in `tests/proof_of_concept/`, not here**:
  `schema_mode="merge"` for an append with fewer columns than the evolved table, the normalized
  `CHECK` expression, `filters=` instead of the deprecated `partitions=`, `pa.schema(dt.schema())`
  through the PyCapsule interface, `partition.mes` and `size_bytes` in
  `get_add_actions(flatten=True)`, SUPER binds rendered as `json_parse(%s)`. Read the study suite
  of a library before writing code against its API.

## What the documents establish

Each fact is detailed in the file named at the end of its line.

- Arrow is the ingestion path for both engines: 300,000 rows into DuckDB from an Arrow table took
  0.008 s, against 5.2 s for 50,000 rows through `executemany`. `docs/duckdb.md`
- A pandas `object` column of `Decimal` gets its DuckDB type from the values present, not from the
  contract (`docs/duckdb.md` reports a 1,000-value sample giving `DECIMAL(7, 2)`; on 2026-09-19 a
  5,001-row column with the large value last gave `DECIMAL(11,2)` and materialized); casting to Arrow
  with the contract schema first fixes the type. `docs/duckdb.md`, `tests/proof_of_concept/test_duckdb.py`
- `pandas.read_sql` turns `Decimal` into `float` unless `coerce_float=False`, and
  `dtype_backend="pyarrow"` returns `double` for `DECIMAL` and `string` for `DATE`; only the Arrow
  path preserves `decimal128(18, 2)` and `date32`. `docs/duckdb.md`
- With `redshift_connector`, `executemany` makes one round trip per row and the dialect does not
  rewrite it into a multi-row `VALUES`: bulk loads go through Parquet on S3 and `COPY`, small batches
  through `insert(Modelo).values(lista)`. `docs/redshift.md`
- The generic `Identity()` disappears from the Redshift DDL, and DuckDB rejects it; business keys
  generated on the client avoid the problem on both. `docs/sqlalchemy.md`
- A `@compiles(CreateTable, "redshift")` hook reading `Table.info` produced the same
  `DISTSTYLE KEY DISTKEY (...) SORTKEY (...)` as the `redshift_*` arguments. Without the dialect
  installed, `redshift_*` arguments are accepted with a `Can't validate argument` warning; with it,
  unknown ones raise `ArgumentError`. `docs/redshift.md`
- `duckdb_engine` reflects columns, types and comments, not primary keys or indexes; Python-scope
  variables are invisible to queries through the engine, so a DataFrame needs `register` on the raw
  connection. `docs/duckdb.md`
- The DuckDB Parquet writer marks every column `optional`, even `NOT NULL`, writes `DECIMAL(18, 2)`
  as `INT64` and writes no page index; PyArrow writes `required` and `FIXED_LEN_BYTE_ARRAY(8)`.
  `docs/parquet.md`
- `COPY ... FROM` in DuckDB is positional and silently casts convertible types, so type
  enforcement belongs on the metadata, before the load. `docs/parquet.md`
- Constraints cost on load and do not help queries in either engine: a DuckDB load of 300,000 rows
  went from 0.008 s to 0.073 s with a composite primary key, and Redshift keys are informational.
  `docs/schema.md`, `docs/duckdb.md`
- S3 `PutObject` accepts `IfNoneMatch='*'` (since 2024-08-20) and `IfMatch=<etag>` (since
  2024-11-25); failures return 412, conflicts 409. This is the primitive a table format needs for
  atomic commits, and it answers the open question in `docs/guia.md`. `docs/estrategia.md`
- `deltalake` 1.6.0 (2026-05-19) removed the DynamoDB lock store; S3 conditional put is the default
  commit mode, and the delta-rs docs page on S3 locking is stale. The writer does not read
  `~/.aws/config`. In the SageMaker space it finds the container credentials through the default
  chain; early in the 2026-09-19 session the credential call failed with 403 as found and passed with
  `NO_PROXY` exported, minutes later the environment as found passed with every interpreter and
  version, and the cause was not isolated. The library exports `NO_PROXY` as a precaution and keeps
  `storage_options` with the `boto3` credentials as the fallback. `docs/delta.md`, `docs/estrategia.md`
- Delta data files do not contain the partition column (it lives in the `add` action), so a
  partition key must derive from a column in the file for Redshift `COPY`; DuckLake keeps identity
  and source columns inside the files. `docs/estrategia.md`
- delta-rs, DuckLake and DuckDB all write `DECIMAL(18, 2)` as `INT64`; PyArrow writes
  `FIXED_LEN_BYTE_ARRAY`. The pending Redshift `COPY` test covers all three. `docs/estrategia.md`
- DuckLake inlines inserts of up to 10 rows into the catalog by default (`DATA_INLINING_ROW_LIMIT`),
  producing no Parquet file (a 10-row insert wrote nothing to the data path); publishing to Redshift
  requires inlining off or a flush. `docs/estrategia.md`
- A DuckLake catalog file served over HTTP attaches read-only with `ATTACH 'ducklake:http://...'`;
  a SQLite catalog on S3 is unsupported by design; without PostgreSQL the model is one writer at a
  time, with the catalog file moved by the library. `docs/estrategia.md`
- `DeltaTable.create_write_transaction` with `AddAction` registers Parquet files written by others
  (the `UNLOAD` path); `ducklake_add_data_files` does the same for DuckLake but failed on a table
  partitioned by `year()`/`month()`. `docs/estrategia.md`
- SQLGlot transpiles function names and syntax between DuckDB and Redshift but passes through
  constructs the target lacks (`INSERT ... BY NAME`, `list_aggregate`) and turned DuckDB
  `VARCHAR(200)` into Redshift `VARCHAR(MAX)`; Redshift integration tests remain necessary.
  `docs/estrategia.md`
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
- DuckDB `INSERT INTO` an attached Delta table works (commitInfo shows `UNKNOWN`) but writes the
  partition column inside the file, unlike delta-rs; mixing writers breaks positional `COPY`.
  `COPY ... (RETURN_STATS)` plus `AddAction` with stats gives files that both readers prune
  (`Scanning Files: 0/12`). `docs/delta.md`
- On local disk, 3,000,000 rows in 12 files: `delta_scan` aggregates in 0.010 s against 0.006 s for
  `read_parquet` and 0.007 s for a materialized table; 20 point queries took 0.05 s through
  `delta_scan` and under 0.01 s on a table. On S3 from the SageMaker space (300,010 rows, 3 files):
  `delta_scan` aggregates in 0.3 s (0.77 s cold) against 0.06 s for `read_parquet` and 0.002 s on a
  materialized table, which took 0.29 s to build; 20 point queries took 6.0 s through `delta_scan`,
  3.1 s through `ATTACH ... PIN_SNAPSHOT`, 1.3 s through `read_parquet` and 0.013 s on a table.
  Each `delta_scan` rereads the log: materialize every table queried more than once. `docs/delta.md`
- delta-rs log cleanup is automatic at checkpoint time and removes log files older than
  `delta.logRetentionDuration` (30 days by default): with `interval 0 days` version 0 became
  unreadable after five commits. `vacuum(keep_versions=[...])` preserves the files of chosen
  versions (database snapshots) while removing those of intermediate versions; `full=True` also
  lists orphan files. A deep copy of a version is `write_deltalake(destino,
  DeltaTable(uri, version=v).to_pyarrow_dataset().scanner().to_reader())`. `docs/delta.md`
- A JSON field is `sa.JSON().with_variant(SUPER(), "redshift")` in the model (DDL `JSON` on DuckDB,
  `SUPER` on Redshift), `pa.json_(pa.string())` or `string` in Arrow, `string` in Delta (the
  extension name kept in field metadata), `JSON` logical type in Parquet written by PyArrow or
  DuckDB and `String` when written by delta-rs; DuckDB reads `delta_scan` JSON as `VARCHAR` and
  validates only on `::JSON`; Arrow and Delta never validate. `docs/schema.md`, `docs/delta.md`
- S3 needs for Delta: `ListBucket` (prefix), `GetObject`, `PutObject` (commits use
  `If-None-Match: *`, no extra IAM action; `object_store` defaults `aws_conditional_put` to
  `etag`), `DeleteObject` for vacuum, KMS actions only with SSE-KMS; no lifecycle expiration under
  table prefixes; versioning and Object Lock unnecessary. SSE keys in `storage_options`:
  `aws_server_side_encryption`, `aws_sse_kms_key_id`, `aws_sse_bucket_key_enabled`. `docs/delta.md`
- DuckDB 1.5.5 Python API: `.arrow()` returns a `RecordBatchReader`, and `fetch_record_batch()` /
  `fetch_arrow_table()` are deprecated in favour of `to_arrow_reader()` / `to_arrow_table()`; the
  reader returns no further rows, without error, after any other command on the same connection. `pa.Table.from_pylist` wants
  dicts: tuples give an all-null table without error. Only the field metadata key
  `PARQUET:field_id` produces Parquet field ids. DuckDB cannot read `BYTE_STREAM_SPLIT` on a
  DECIMAL column written by PyArrow. `DeltaTable.alter.add_columns` needs `deltalake.schema.Field`,
  not `pyarrow.Field`. A pandas `dict` column enters a DuckDB `JSON` column without a cast.
  `docs/duckdb.md`, `docs/parquet.md`, `docs/redshift.md`, `docs/sqlalchemy.md`
- PyIceberg 0.12.0 with a SQLite `sql` catalog writes Iceberg without a service: hidden partition by
  `month(data_ref)`, `overwrite` with a filter, rename and add columns, `add_files`; the catalog
  holds one row per table in a 20 KB file. DuckDB `iceberg_scan` needs the `metadata.json` path,
  because PyIceberg writes no `version-hint.text` and names metadata `<N>-<uuid>.metadata.json`.
  Partition transforms on write need the `pyiceberg-core` extra. `docs/estrategia.md`
- delta-rs maps the contract types from Arrow as `short`, `integer`, `long`, `boolean`, `double`,
  `decimal(p,s)`, `string`, `date`, `timestamp_ntz` (naive) and `timestamp` (UTC); a naive timestamp
  column raises the protocol to reader 3 / writer 7 with the `timestampNtz` feature, which DuckDB
  reads as `TIMESTAMP`. `docs/schema.md`, `docs/estrategia.md`
- `compile()` needs the right dialect object: `duckdb_engine.Dialect()` renders `%(name)s`, the
  dialect of a created engine renders `$1` (`numeric_dollar`, set when the engine loads the DBAPI)
  and `RedshiftDialect_redshift_connector()` renders `%s` with `positiontup`. The Redshift
  `CREATE TABLE` compiler drops `CHECK`, but `AddConstraint` and `CREATE INDEX` still render, so
  `ddl_if(dialect="duckdb")` gates them; `create_mock_engine` dumps the whole `create_all` sequence
  with `checkfirst=False`. `docs/sqlalchemy.md`
- Both dialects declare `supports_native_decimal = False`: `Numeric` parameters go through
  `to_float` and results through a float-formatting `DecimalResultProcessor`. Through duckdb_engine,
  `Numeric(18, 2)` is exact up to 15 significant digits, 16 digits lose the last cent, 17 round to
  10^15 and 18 fail the `INSERT`; the Arrow path keeps 18 digits and `literal_binds` text keeps the
  decimal. `docs/sqlalchemy.md`
- `deltalake` is the Delta project's native Rust implementation, not the reference one:
  `delta-spark` (JVM, `import delta`, 4.4.0 of 2026-08-20) gets protocol features first. delta-rs
  1.6.4 reads but does not write deletion vectors (issue 4512 open), has no `rename_column` (drop is
  PR 4732, column mapping on write is issue 3936), no identity columns and no symlink manifest, while
  its S3 conditional-put commits need no DynamoDB, which delta-spark still documents. Redshift `COPY`
  reads the raw files, so deletion vectors and column mapping stay off with any writer; DuckDB reads
  through `delta-kernel-rs`, and delta-rs `main` pins the fork `buoyant_kernel`. `docs/delta.md`
- The Delta table folder keeps every data file ever written until `vacuum`: after 20 commits, 20
  files on disk for 14 in the snapshot, and a raw `read_parquet` of the folder returned 330,000 rows
  too many; the `**` glob also reads the checkpoints in `_delta_log/`. Going back to Parquet folders
  by month is either copying the files `get_add_actions()` lists (0.014 s for 14 files; each file
  keeps the schema of its write, so only readers that match by name fill the added columns) or
  rewriting with DuckDB `COPY (SELECT * FROM delta_scan(...)) TO ... (PARTITION_BY (mes))`, which
  gave one `data_0.parquet` per month with 11 threads and keeps the partition column out of the
  files unless `WRITE_PARTITION_COLUMNS true`. `docs/delta.md`
- A database snapshot is the library's `{table: version}` set, marked on demand; the user renamed
  it from `fechamento` on 2026-09-19, and the periodicity (quarterly in the examples) is the
  process's choice. Commit key `serialize_db_snapshot`, control file `_serialize_db/snapshots.json`
  at the environment root, written with `IfMatch`, reconstructible from `history()` only while the
  log lasts because `commitInfo` is not in checkpoints. The library stores no schema, file list or
  statistics; the DDL of an old snapshot comes from that version's Delta schema, not from the
  current model. `docs/serialize-db.md`, `docs/delta.md`
- Renaming or dropping a column with delta-rs 1.6.4 is a rewrite of every live file in one commit
  (`remove` of all, `add` of all, `metaData`); `dt.alter` has no `rename_column` or `drop_columns`.
  `write_deltalake(reader, mode="overwrite", schema_mode="overwrite")` held 1,140 MB RSS for 135 MB
  of Parquet and 1,960 MB for 269 MB; DuckDB `COPY ... PARTITION_BY ... RETURN_STATS` plus
  `create_write_transaction(mode="overwrite", schema=new)` gave the same commit at a flat 600 MB.
  `schema_mode="overwrite"` with a `predicate` is accepted and switches the schema of the whole
  table, so the other months read the renamed column as null; the library refuses the combination.
  `docs/delta.md`
- `literal_column(":mes")` survives `literal_binds` in both dialects and is the execution parameter
  of generated SQL; `bindparam("mes")` without a value and `text("mes = :mes")` render `mes = NULL`
  with a `SAWarning` instead of failing. Stand-alone dialect instances (`pyformat`, `format`) double
  `%` in literals under `literal_binds` (`LIKE 'A%'` becomes `'A%%'`); `Dialect(paramstyle="named")`
  does not. A table name with `{` is quoted unless `quoted_name(quote=False)`;
  `replacement_traverse` swaps the contract tables of a finished statement for prefixed copies.
  DuckDB runs the text with `$name` and a dict; `redshift_connector` with
  `cursor.paramstyle = "named"`. The Redshift dialect derives from `PGDialect` and compiles
  `DISTINCT ON`, `ON CONFLICT DO NOTHING` and array subscripts without error. `docs/sqlalchemy.md`
- botocore 1.43.98 reads `AWS_DEFAULT_REGION` or the profile, never `AWS_REGION`, and without a
  region uses the global endpoint `s3.amazonaws.com`, which a regional VPC endpoint does not serve;
  delta-rs reads both variables and without either queries IMDS and falls back to `us-east-1`. The S3
  suite copies the `boto3` region into `AWS_REGION` one way only, so an environment with only
  `AWS_REGION` and no `~/.aws/config` needs maintenance; `test_boto3_credential_source` needs STS
  (60 s per attempt, 5 attempts by default). On a dead network delta-rs gives up in 10 s with
  `max_retries=1` and `retry_timeout=10s` in `storage_options` (59 s without). `README.md`

## The pipeline outside this repository

Facts stated by the user, not visible in the code: the pipeline is mostly Python logic; SQLAlchemy
is used only for the declarative models (DDL) and for Core `select` and `insert` statements that
move DataFrames, never for ORM instances; no catalog service is enabled, which excludes Iceberg on
Glue (Iceberg with a SQLite catalog file moved by the library is the documented alternative if Glue
or S3 Tables may be enabled later); development and production runs write separate tables; renaming
or dropping columns is rare. The decision in `docs/PLAN.md`, with the rationale in `docs/estrategia.md`, follows from them: Delta Lake
through `deltalake` as the table layer, SQLAlchemy kept as contract metadata and Core, DataFrames
moved through Arrow, SQLMesh, dbt and DuckLake not adopted. SQLAlchemy is in the project for
compatibility with that code (user statement of 2026-09-19); the same day the user decided that
runtime compilation by the dialect is replaced gradually by generated SQL text per dialect, one
database interaction at a time, so SQLAlchemy ends in the models and in generation and
`duckdb_engine` and `sqlalchemy-redshift` leave the runtime dependencies.

## Naming decisions applied to the documents

The convention was applied to every example in `docs/` on 2026-09-19. ORM model classes and column
mixins (`Operacao`, `Lancamento`, `Rastreio`) keep Portuguese names: they are data-model artifacts,
like tables and columns. Python variables, functions, parameters, modules, the proposed API
(`Database`, `Execution`, `ingest`, `audit`, `publish`) and the keys of `Table.info["serialize_db"]`
(`partition_by`, `sort_key`, `redshift`) are English. The library's own metadata is English (user
decisions of 2026-09-19). A key that lives under `_serialize_db/` carries no prefix (`snapshots` in
`_serialize_db/snapshots.json`); everything else the library writes carries the `serialize_db_`
prefix: the commit keys `serialize_db_execution_id`, `serialize_db_input_versions` and
`serialize_db_snapshot`, the Parquet footer keys `serialize_db_version` and
`serialize_db_execution_id`, and the Redshift control table
`serialize_db_publications(table_name, delta_version, execution_id, published_at)`, whose columns
stay unprefixed because the table name is the namespace. `snapshot` is an accepted loanword in
prose. Data tables and columns stay Portuguese, including the `id_execucao` column of the `Rastreio`
mixin in `docs/sqlalchemy.md`. SQL placeholders in prose (`COPY (consulta) TO ...`) and staging
table names (`staging_<tabela>`) count as database identifiers.

## Where the work stands

The state of the project, the decisions, the stages with their primitives and the order of work are
in `docs/PLAN.md` (pt-BR, 2026-09-19); read it before planning a session. What the plan does not
record: PRs #3 to #12 were merged by the user, who syncs `main`; PR #13 (branch
`claude/diagnostico-aws`) is open and carries the probes, the plan, the test layout and the study
suites, and every new commit goes there until it merges; `gh` is authenticated in the space since
2026-09-19. The next session starts stage 1 (`serialize_db.schema`) and stage 2 (`serialize_db.sql`)
on local folders.

Every Python block in `docs/` ran in the session scratchpad through `uv run --no-project
--python 3.13 --with "deltalake==1.6.4" --with "duckdb==1.5.5" --with "pyarrow==25.0.1" ...` with
the versions listed under the environment section; the scripts were not kept, the documents are the
record, and the Redshift statements were compiled only. New examples are checked against the
identifier convention by tokenizing the Python blocks: only `NAME` tokens are candidates, and
strings, attribute access after `.`, `name: Mapped[...]` annotations and keyword arguments of
`.values(...)` and of model constructors are column names. The Portuguese names left after that
check are columns, ORM classes and the loanwords `sandbox` and `staging`.

## The state of the code

`pyproject.toml` declares no runtime dependencies; the `dev` group pins pytest, deltalake 1.6.4,
DuckDB 1.5.5, PyArrow 25.0.1, boto3, redshift-connector, SQLAlchemy 2.0.54, duckdb-engine 0.17.0,
sqlalchemy-redshift 1.0.0 and pandas 3.0.6 (the last four added on 2026-09-19 for the study suites;
`prepare_offline.sh` must be rerun). The models in `src/serialize_db/model/` and their defects are
listed in `docs/PLAN.md` under the state of the repository; the `DEFERRABLE` and `SERIAL` behavior of
each dialect is in `docs/sqlalchemy.md` and `docs/duckdb.md`.

Test layout (user decision of 2026-09-19): `tests/` holds the package tests, none yet, and
`tests/conftest.py` the authorization rule and the fixtures of the three targets;
`tests/proof_of_concept/` holds the proof of concept of the Delta layer on both storages (`delta.py`,
`test_local.py`, `test_s3.py`), the study suites of the external libraries (`test_sqlalchemy.py`,
`test_duckdb.py`, `test_pyarrow.py`, `test_deltalake.py`), commented step by step because they are
also the learning material for future maintainers, and `test_redshift.py`, the stage-0 Redshift
items, written before any connection existed and never run against a cluster. User rule of
2026-09-19: each authorization variable (`SERIALIZE_DB_TEST_LOCAL_ROOT`, `SERIALIZE_DB_TEST_S3_ROOT`,
`SERIALIZE_DB_TEST_REDSHIFT_SCHEMA`) enables the writes under it, so `uv run pytest` with no
variables runs only the tests that write nothing, and with the variable set whatever prevents the
write fails the session (local root missing; S3 root without `boto3` credentials or failing a
short-timeout listing, 11 s with a silent proxy; Redshift without a connection); the S3 root is never
inferred from the SageMaker project; DuckDB extensions are installed only into
`SERIALIZE_DB_DUCKDB_EXTENSIONS`, with `autoinstall_known_extensions` off because `LOAD` of a known
extension otherwise downloads it into `~/.duckdb` without notice. Verified on 2026-09-19 on macOS: no
variables, 18 passed and 39 skipped; local root, 39 passed and 18 skipped. The S3 suite passed in the
space the same day (10 passed in 27 s) before the boto3 list, copy and delete test was added.

The suites exist so the same proof of concept runs in the target environment, which has no internet;
the local suite validates the prepared folder there without S3. Verified on 2026-09-19: the archive
extracted at another path and run with dead proxies and an empty `HOME`, 10 passed; on macOS the
script had left `.venv/bin/python` pointing nowhere (Linux-only glob), and after the fix of PR #12 the
local suite passed there through `.venv/bin/python -m pytest` with dead proxies. The folder travels
as `.tar.gz`, never zip, to keep links and permissions; `uv sync` ignores `UV_VENV_RELOCATABLE`, and
the `.venv/bin/*` scripts keep absolute shebangs, hence `python -m pytest`.

## Environment of the measurements

The examples in `docs/parquet.md`, `docs/duckdb.md` and `docs/sqlalchemy.md` ran on 2026-09-18 with
Python 3.13, DuckDB 1.5.5, PyArrow 25.0.1, pandas 3.0.6, polars 1.44.2, SQLAlchemy 2.0.54,
duckdb_engine 0.17.0, sqlalchemy-redshift 1.0.0 and redshift_connector 2.1.16, on a sample of
300,000 rows of `operacoes` with the columns `id_operacao`, `data_ref`, `id_cliente`, `valor` and
`descricao`; the Redshift statements were compiled only. The S3 proof of concept ran on 2026-09-19
inside the SageMaker Unified Studio space with Python 3.13.15, deltalake 1.6.4, DuckDB 1.5.5 and
PyArrow 25.0.1 through `UV_PYTHON_DOWNLOADS=automatic uv run --no-project --python 3.13 --with ...`
and `NO_PROXY="$no_proxy"` exported; the same scripts also passed on the space's system Python
3.12.13 with deltalake 1.5.0 and DuckDB 1.5.4, and then as the pytest suite (10 passed in 27 s); the
scratch objects were deleted afterwards. The local proof of concept in `docs/estrategia.md` ran on
2026-09-19 on macOS arm64 with Python 3.13, deltalake 1.6.4, DuckDB 1.5.5 with the `delta`,
`ducklake` and `iceberg` extensions (ducklake `d8a1881e`, metadata version 1.0), PyIceberg 0.12.0
with the `sql-sqlite` and `pyiceberg-core` extras, PyArrow 25.0.1 and SQLGlot 30.18.0, through
`uv run --with` in the scratchpad, with 11 DuckDB threads and the files in the page cache; nothing
ran against S3 or Redshift, and the Python examples added to every document on 2026-09-19 ran under
the same pinned versions.

## The SageMaker Unified Studio environment, as observed on 2026-09-19

- Domain `dzd-d8yrvx1ko7im6o`, project `eighth-experimentation` (`avhvbqn37ty7m8`), account
  892278726726, region `us-west-2`, space `my-code-v4` (Code Editor, 4 vCPUs). The `sagemaker_studio`
  package's `Project()` gives `iam_role`, `kms_key_arn`, `s3.root` and `connections`.
- Credentials: the container endpoint (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`; `boto3` reports
  `container-role`) assumes `datazone_usr_role_avhvbqn37ty7m8_5hkjdsy3umpi1c`. `~/.aws/config` has a
  `default` profile with `credential_source = EcsContainer` and a `DomainExecutionRoleCreds` profile.
- Project bucket `awsds-sandbox-smus-projects`, prefix `dzd-d8yrvx1ko7im6o/avhvbqn37ty7m8/`: `dev/`
  is the working area (`s3.root`), `shared/` is the s3fs mount at `$HOME/shared`. SSE-KMS by default
  with bucket key and the project KMS key. The role cannot `ListAllMyBuckets` or
  `GetBucketVersioning`. A second S3 connection, `sandbox-lake.s3`, points at
  `s3://awsds-sandbox-lake/sso-group-data-scientists/` through S3 Access Grants.
- Connections: S3, Athena, Glue Spark, Spark Connect, Lakehouse and workflows. No Redshift
  connection, cluster or serverless workgroup.
- Network: outbound HTTP goes through `proxy.awsds.internal:3128`; only lower-case `no_proxy` is
  set. `uv` reaches PyPI through the proxy but downloads Python only with
  `UV_PYTHON_DOWNLOADS=automatic`; `uv sync` needs it to fetch Python 3.13, the venv lands in `.venv`
  (ignored), and `uv run` warns that `VIRTUAL_ENV=/opt/conda` is ignored, which is harmless. System
  Python is 3.12.13 with boto3, awswrangler, deltalake 1.5.0, DuckDB 1.5.4, PyArrow 21.0.0 and
  redshift_connector 2.1.10 preinstalled.
- `gh` is installed and authenticated as the user, and `git push` over HTTPS works through it.

## Questions the official documentation does not answer

The documents mark these as pending the proof of concept. All of them need Redshift, which the
project does not have yet; the S3 questions were answered on 2026-09-19.

- Whether `COPY` from Parquet accepts a column list: `awswrangler` emits
  `COPY tabela (colunas) ... FORMAT AS PARQUET`, while the `COPY` reference describes the column list
  only for flat files.
- The correspondence between Parquet physical types and Redshift columns in `COPY`, in particular
  `TIMESTAMP` as `INT64` in microseconds and the physical type of `DECIMAL`.
- What `COPY` from Parquet does when a string exceeds the target `VARCHAR`: truncate or abort.
- Whether Redshift Spectrum maps plain Parquet columns by name or by position.
- The physical types `UNLOAD` writes for `TIMESTAMP` and `DECIMAL`, whether its columns are
  required, and whether it writes minimum and maximum statistics; the three affect the `AddAction`
  that registers `UNLOAD` files in the Delta log (`register_file` in `docs/delta.md`).
- Whether `FILLRECORD` lets `COPY` from Parquet load older files that lack columns appended later
  to the schema, which both Delta and DuckLake produce on evolution.

One gap needs a decision rather than a test: `sqlalchemy-redshift` compiles `Text` as `TEXT`, which
Redshift stores as `VARCHAR(256)`, so the `VARCHAR(65535)` of the contract needs `String(65535)` or a
`@compiles(Text, "redshift")` rule.
