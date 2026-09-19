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

All prose in this repo is **Brazilian Portuguese (pt-BR)**: README files, code comments, docstrings, printed output, test messages, and shell-script comments. When editing or adding content, keep writing in pt-BR to match. Identifiers (variable and function names) are in English.

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
(SQLGlot, SQLMesh, dbt, Ibis, dlt), data-contract tools and Rust/PyO3. New research appends to the
matching group.

| File | Subject |
| --- | --- |
| `README.md` | Initialization with `uv init --python 3.13`. |
| `docs/guia.md` | ETL practices the pipeline follows: immutable monthly partitions with idempotent replacement, write-audit-publish, the schema contract, and the open question about committing metadata atomically on S3. |
| `docs/schema.md` | DDL generated from the ORM models, physical options carried in `Table.info`, constraint policy per backend, the type table that maps SQLAlchemy to Arrow, Iceberg, DuckDB and Redshift, and SQL portability between the two engines. |
| `docs/parquet.md` | Parquet file layout and every metadata structure (`FileMetaData`, schema, row group, `ColumnMetaData`, page index, Bloom filters, page headers, key-value pairs, size and geospatial statistics, encryption, summary files), inspection with DuckDB and with PyArrow, partitioning, query optimization by layer, and import and export in DuckDB and in Redshift. |
| `docs/duckdb.md` | DuckDB as the execution sandbox. |
| `docs/redshift.md` | Redshift as the publication database and the second execution engine. It opens with the diagnostic queries for a session. |
| `docs/sqlalchemy.md` | SQLAlchemy as the schema contract: metadata, reflection and customization, deferrable constraints, Core statements, the ORM for DDL and for DML, keys generated on the server, and what the Redshift dialect, the DuckDB dialect and Parquet files each support. |
| `docs/delta.md` | Delta Lake as the source of truth: table folder layout and log actions, Delta versus Iceberg (where the current-version pointer lives), supported types, table creation from the SQLAlchemy model, schema evolution rules and what replaces Alembic, transactions, conflicts and restore, DML through delta-rs, ingestion and export, the pipeline steps, DuckDB and Redshift access, performance measurements, relocation of the whole folder (relative paths) and SQLAlchemy support. |
| `docs/estrategia.md` | Table layer over Parquet without a catalog service (Delta Lake via delta-rs, DuckLake, Iceberg without a catalog, Hudi, hand-rolled manifests) compared against the project's requirements, the Redshift path by `COPY ... MANIFEST` and its rules, the SQL layer options (SQLAlchemy Core, SQLGlot, SQLMesh, dbt, Ibis, dlt), contract and audit tools, the Rust/PyO3 assessment, the recommendation (Delta Lake plus SQLAlchemy Core), the decisions taken with the user and the proof of concept still pending on S3 and Redshift. It records the local proof of concept of 2026-09-19. |
| `src/serialize_db/model/` | Declarative ORM models of the accounting, management and projection tables. |

`docs/duckdb.md`, `docs/redshift.md` and `docs/delta.md` share a section order: data organization and
the differences from PostgreSQL, supported types with `DECIMAL` and JSON, DDL,
`SELECT`/`INSERT`/`UPDATE`/`DELETE`, ingestion, export to Parquet, performance recommendations,
SQLAlchemy support, references. `docs/delta.md` adds schema evolution, transactions, the pipeline,
DuckDB and Redshift access, and relocation.

## What the documents establish

Each fact below is detailed in the file named at the end of its line.

- Arrow is the ingestion path for both engines. Loading 300,000 rows into DuckDB from an Arrow table
  took 0.008 s, against 5.2 s for 50,000 rows through `executemany`. `docs/duckdb.md`
- A pandas column of `object` holding `Decimal` gets its DuckDB type from a sample of 1,000 values:
  the sample column became `DECIMAL(7, 2)`, which a larger value in a later batch would reject.
  Casting to Arrow with the contract schema first fixes the type. `docs/duckdb.md`
- `pandas.read_sql` turns `Decimal` into `float` unless `coerce_float=False`, and
  `dtype_backend="pyarrow"` returns `double` for `DECIMAL` and `string` for `DATE`. Only the Arrow
  path preserves `decimal128(18, 2)` and `date32`. `docs/duckdb.md`
- With `redshift_connector`, `executemany` makes one round trip per row, and the dialect does not
  rewrite it into a multi-row `VALUES`. Bulk loads go through Parquet on S3 and `COPY`; small batches
  go through `insert(Modelo).values(lista)`. `docs/redshift.md`
- The generic `Identity()` disappears from the Redshift DDL, and DuckDB rejects it outright. Business
  keys generated on the client avoid the problem on both. `docs/sqlalchemy.md`
- A `@compiles(CreateTable, "redshift")` hook that reads `Table.info` produced the same
  `DISTSTYLE KEY DISTKEY (...) SORTKEY (...)` as the `redshift_*` arguments. Without the dialect
  installed, `redshift_*` arguments are accepted with a `Can't validate argument` warning; with it
  installed, unknown ones raise `ArgumentError`. `docs/redshift.md`
- `duckdb_engine` reflects columns, types and comments, but not primary keys or indexes; variables
  in the Python scope are invisible to queries issued through the engine, so a DataFrame needs
  `register` on the raw connection. `docs/duckdb.md`
- The DuckDB Parquet writer marks every column `optional`, even `NOT NULL`, writes `DECIMAL(18, 2)`
  as `INT64` and writes no page index. PyArrow writes `required` and `FIXED_LEN_BYTE_ARRAY(8)`.
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
  commit mode. The delta-rs docs page on S3 locking is stale. The writer does not read
  `~/.aws/config`; credentials on SageMaker are a proof-of-concept item. `docs/estrategia.md`
- Delta data files do not contain the partition column (it lives in the `add` action), so a
  partition key must derive from a column in the file for Redshift `COPY`. DuckLake keeps identity
  and source columns inside the files. `docs/estrategia.md`
- delta-rs, DuckLake and DuckDB all write `DECIMAL(18, 2)` as `INT64`; PyArrow writes
  `FIXED_LEN_BYTE_ARRAY`. The pending Redshift `COPY` test covers all three. `docs/estrategia.md`
- DuckLake inlines inserts of up to 10 rows into the catalog by default (`DATA_INLINING_ROW_LIMIT`),
  producing no Parquet file; a 10-row insert in the proof of concept wrote nothing to the data path.
  Publishing to Redshift requires inlining off or a flush. `docs/estrategia.md`
- A DuckLake catalog file served over HTTP attaches read-only with `ATTACH 'ducklake:http://...'`;
  a SQLite catalog on S3 is unsupported by design. Without PostgreSQL the model is one writer at a
  time, with the catalog file moved by the library. `docs/estrategia.md`
- `DeltaTable.create_write_transaction` with `AddAction` registers Parquet files written by others
  (the `UNLOAD` path); `ducklake_add_data_files` does the same for DuckLake, but failed on a table
  partitioned by `year()`/`month()` in the proof of concept. `docs/estrategia.md`
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
  null in every row, and `append` casts incoming data to the table type (int32, double and string
  into `long` were accepted; decimal(20,4) into decimal(18,2) refused) without changing the table;
  type changes need `mode="overwrite"` with `schema_mode="overwrite"`. The reconcile step must refuse
  NOT NULL additions and the Arrow cast must enforce types before writing. `docs/estrategia.md`
- Two delta-rs writers on the same version: append plus append both commit; overwrite of the same
  month fails with `CommitFailedError`; overwrites of different months both commit. App
  transactions (`Transaction(app_id, version)`) are recorded but not enforced: a repeated write with
  the same version was accepted. `docs/delta.md`
- `add`/`remove` paths are relative to the table folder: a copied folder opened at the same version
  with the same rows in delta-rs and DuckDB, history and time travel intact. Never register files
  by absolute URI. Iceberg manifests store absolute paths. `docs/delta.md`
- DuckDB `INSERT INTO` an attached Delta table works (commitInfo shows `UNKNOWN`) but writes the
  partition column inside the file, unlike delta-rs; mixing writers breaks positional `COPY`.
  `COPY ... (RETURN_STATS)` plus `AddAction` with stats gives files that both readers prune
  (`Scanning Files: 0/12`). `docs/delta.md`
- On local disk, 3,000,000 rows in 12 files: `delta_scan` aggregates in 0.010 s against 0.006 s for
  `read_parquet` and 0.007 s for a materialized table; 20 point queries took 0.05 s through
  `delta_scan` and under 0.01 s on a table. Materialize only tables queried repeatedly; S3 numbers
  are pending. `docs/delta.md`
- delta-rs log cleanup is automatic at checkpoint time and removes log files older than
  `delta.logRetentionDuration` (30 days by default): with `interval 0 days` version 0 became
  unreadable after five commits. `vacuum(keep_versions=[...])` preserves the files of chosen
  versions (quarterly closings) while removing those of intermediate versions; `full=True` also
  lists orphan files. A deep copy of a version is `write_deltalake(destino,
  DeltaTable(uri, version=v).to_pyarrow_dataset().scanner().to_reader())`. `docs/delta.md`
- PyIceberg 0.12.0 with a SQLite `sql` catalog writes Iceberg without a service: hidden partition by
  `month(data_ref)`, `overwrite` with a filter, rename and add columns, `add_files`; the catalog
  holds one row per table in a 20 KB file. DuckDB `iceberg_scan` needs the `metadata.json` path,
  because PyIceberg writes no `version-hint.text` and names metadata `<N>-<uuid>.metadata.json`.
  Partition transforms on write need the `pyiceberg-core` extra. `docs/estrategia.md`
- delta-rs maps the contract types from Arrow as `short`, `integer`, `long`, `boolean`, `double`,
  `decimal(p,s)`, `string`, `date`, `timestamp_ntz` (naive) and `timestamp` (UTC); a naive timestamp
  column raises the protocol to reader 3 / writer 7 with the `timestampNtz` feature, which DuckDB
  reads as `TIMESTAMP`. `docs/schema.md`, `docs/estrategia.md`

## The pipeline outside this repository

Facts stated by the user, not visible in the code: the pipeline is mostly Python logic; SQLAlchemy is
used only for the declarative models (DDL) and for Core `select` and `insert` statements that move
DataFrames, never for ORM instances; no catalog service is enabled, which excludes Iceberg on Glue
(Iceberg with a SQLite catalog file moved by the library is the documented alternative if Glue or
S3 Tables may be enabled later); development and production runs write separate tables; renaming or dropping columns is
rare. The decision recorded in `docs/estrategia.md` follows from them: Delta Lake through `deltalake`
as the table layer, SQLAlchemy kept as contract metadata and Core, DataFrames moved through Arrow,
SQLMesh, dbt and DuckLake not adopted.

## The state of the code

`src/serialize_db/model/` holds three modules of declarative models: `model_base_contabil.py`
(`dom_veiculos`, `dom_hierarquias_contas`, `cad_contas`, `rel_contas_hierarquias`,
`cad_lancamentos`), `model_base_gerencial.py` (`dom_mensuracoes`, `dom_segmentos`, `dom_negocios`,
`cad_operacoes`, `rel_contrato_operacao`, `cad_contratos`, `cad_lancamentos`) and
`model_db_projetado.py` (`cad_contratos`, `cad_aliquotas`).

Two of them fail to import: `model_base_gerencial.py` runs `from lib_base_contabil import Base` and
`model_db_projetado.py` runs `from lib_base_gerencial import Base`, module names the repository does
not have. Only `model_base_contabil.py` declares `Base`. The models carry no dialect options and no
`info` dictionaries, which `docs/schema.md` proposes, and they use `Double` where the contract
expects `Numeric(18, 2)`. Their single-column integer primary keys keep the default `autoincrement`,
which duckdb_engine renders as `SERIAL` and DuckDB rejects with `Type with name SERIAL does not
exist!`; `autoincrement=False` avoids it. Foreign keys are declared `deferrable=True, initially='DEFERRED'`, which
both dialects render as `DEFERRABLE INITIALLY DEFERRED`: DuckDB rejects that DDL with
`Constraint not implemented!`, and the Redshift `CREATE TABLE` syntax has no such clause
(`docs/sqlalchemy.md`, section on deferrable constraints).

`REFERENCES.md` links `docs/plano-de-implementacao.md`, which is not in the repository.

## Environment of the measurements

The examples in `docs/parquet.md`, `docs/duckdb.md` and `docs/sqlalchemy.md` ran on 2026-09-18 with
Python 3.13, DuckDB 1.5.5, PyArrow 25.0.1, pandas 3.0.6, polars 1.44.2, SQLAlchemy 2.0.54,
duckdb_engine 0.17.0, sqlalchemy-redshift 1.0.0 and redshift_connector 2.1.16. The sample is 300,000
rows of `operacoes` with the columns `id_operacao`, `data_ref`, `id_cliente`, `valor` and
`descricao`. The Redshift statements were compiled only; nothing ran against a cluster.

The proof of concept in `docs/estrategia.md` ran on 2026-09-19 on macOS arm64 with Python 3.13,
deltalake 1.6.4, DuckDB 1.5.5 with the `delta`, `ducklake` and `iceberg` extensions (ducklake
`d8a1881e`, metadata version 1.0), PyIceberg 0.12.0 with the `sql-sqlite` and `pyiceberg-core`
extras, PyArrow 25.0.1 and SQLGlot 30.18.0, through `uv run --with` in the scratchpad, with 11 DuckDB
threads and the files in the page cache. Nothing ran against S3 or Redshift.

## Questions the official documentation does not answer

The documents mark these as pending the proof of concept.

- Whether `COPY` from Parquet accepts a column list. `awswrangler` emits
  `COPY tabela (colunas) ... FORMAT AS PARQUET`, while the `COPY` reference describes the column list
  only for flat files.
- The correspondence between Parquet physical types and Redshift columns in `COPY`, in particular
  `TIMESTAMP` as `INT64` in microseconds and the physical type of `DECIMAL`.
- What `COPY` from Parquet does when a string exceeds the target `VARCHAR`: truncate or abort.
- Whether Redshift Spectrum maps plain Parquet columns by name or by position.
- The physical types `UNLOAD` writes for `TIMESTAMP` and `DECIMAL`, whether its columns are
  required, and whether it writes minimum and maximum statistics. The three affect `add_files`.
- Whether `FILLRECORD` lets `COPY` from Parquet load older files that lack columns appended later
  to the schema, which both Delta and DuckLake produce on evolution.
- Which credential sources the delta-rs writer finds inside a SageMaker Unified Studio space.

One gap is already understood and needs a decision rather than a test: `sqlalchemy-redshift` compiles
`Text` as `TEXT`, which Redshift stores as `VARCHAR(256)`, so the `VARCHAR(65535)` of the contract
needs `String(65535)` or a `@compiles(Text, "redshift")` rule.
