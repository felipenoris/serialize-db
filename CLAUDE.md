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

## The documents that track the work

The documents under `docs/` that record where the project stands are updated by the unit of work
that changes what they describe, in the same commit:

- **`docs/CURRENT_STATE.md`** holds the state of the implementation: the situation of each stage and
  what each artifact of the repository contains. Update it when a stage advances, a module is
  written, a suite's pass and skip counts change, or an artifact appears or leaves.
- **`docs/POC.md`** holds what the proofs of concept, the test suites and the probes showed, with
  the date of each measurement. Add the finding when a probe runs, a suite runs in a new
  environment, or an experiment answers a question. A finding that contradicts `docs/PLAN.md` or a
  stage file triggers the revision of that file in the same unit of work, and the report names the
  files the revision changed.
- **`docs/OPEN_QUESTIONS.md`** holds the pending items, the doubts and the decisions waiting on the
  user. Add an item when the work finds a question it cannot close; remove it when the answer lands
  in the document that owns it, and name that document in the report.

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

## `secrets/`

Never read the contents of the `secrets` folder.

# Claude Memory

Use this section to store your memory for this project. Keep this file within a size budget of 75 KB.

## How this memory is organized

This file is loaded whole into every session, so it holds what every session needs: the index of the
repository and of the memory files, the working rules, the naming conventions and where the work stands.
The facts and the stories live in `.claude/memory/`, one file per theme, read on demand with the Read
tool and never imported with `@`, which would load them all. Each theme file opens with when to read it,
and each fact ends with the `docs/` file that details it. A fact found in a session is appended to its
theme file; this file changes when a rule, a convention, an index entry or the state of the work changes.
The budget above is never a reason to drop a fact: what does not fit here goes to a theme file.

| Memory file | Read it before |
| --- | --- |
| `.claude/memory/decisions.md` | Planning or implementing any stage: what the user stated and decided, with dates (the pipeline outside this repo, the batch boundary, the partition unit, the Redshift target, `export_mode`, the test layout). |
| `.claude/memory/lessons.md` | Adding a lesson, or when the reason behind a working rule matters: the dated stories. |
| `.claude/memory/delta.md` | Code on `serialize_db.delta` or the `deltalake` package: delta-rs behavior, `create_write_transaction`, schema evolution, conflicts, vacuum and log retention, performance, the alternatives assessed. |
| `.claude/memory/duckdb.md` | Code on `engine.duckdb`, `storage.duckdb_setup` or a probe that opens DuckDB: Arrow in and out, `COPY`, proxy, Python API. |
| `.claude/memory/redshift.md` | Code on `engine.redshift`, the publication, the Redshift suite or `probes/redshift.py`: the target, the datashare rules, `COPY` and `UNLOAD`, the driver, the readings of 2026-09-20. |
| `.claude/memory/sqlalchemy.md` | Stages 1 and 2, a DDL rule per dialect or generated SQL text: dialects, compilation, `Numeric`, SQLGlot. |
| `.claude/memory/parquet-arrow-types.md` | `cast`, the schema mapping or a Parquet footer check: what each writer produces, the type contract, PyArrow casts and pandas conversions. |
| `.claude/memory/aws-s3.md` | `serialize_db.storage`, the S3 suite or a probe that reaches AWS: conditional put, IAM needs, credentials, region and proxy per client. |
| `.claude/memory/concurrency.md` | `stream`, `loader`, `max_workers` or any helper thread: the GIL, DB-API thread safety, the batch boundary measurements. |
| `.claude/memory/source-base.md` | Stage 7 or `tests/source_db_projetado.py`: the dev base read on 2026-09-20 and the fixture. |
| `.claude/memory/environments.md` | Running in the SageMaker space or the target, preparing the offline folder, dating a measurement: the lab, the target, the venv, the environments of the measurements. |

## Repository index

Read the file listed here before researching its subject again. Each document names the pages it
came from, and `REFERENCES.md` collects every URL consulted so far, grouped by subject: Parquet
format, Redshift, DuckDB, PyArrow, SQLAlchemy, pandas, PyIceberg, Glue Data Catalog, Athena, Lake
Formation, SageMaker Unified Studio, S3, Python packages, Delta Lake, DuckLake, Hudi, SQL tooling
(SQLGlot, SQLMesh, dbt, Ibis, dlt), data-contract tools, Rust/PyO3 and consulted repositories. New
research appends to the matching group.

| File | Subject |
| --- | --- |
| `README.md` | `uv sync --group dev`, the test layout (`tests/` for the package, `tests/proof_of_concept/` for the proofs of concept and the study suites) with one command per suite, the rule that each authorization variable enables the writes under it and the variables of the three targets, the probes, the delta-rs credentials and proxy note, and why the folder is prepared for the target without internet, with the procedure in the `prepare_offline.sh` header. |
| `prepare_offline.sh` | Makes the project folder self-contained for the target without internet: managed Python in `.python/`, every `pyproject.toml` group in `.venv/` (`uv sync --all-groups`), DuckDB extensions in `.duckdb/`, all links relative. **Rerun it whenever a dependency is added**; a new DuckDB extension, a Python version change or another runtime asset is added by hand. Only a folder prepared on Linux x86_64 serves the SageMaker space; the header is the operating procedure, and the extensions block configures the DuckDB proxy through `probelib.duckdb_proxy`. |
| `examples/` | The scripts the user ran in the target, kept as run: `redshift_native.py`, `redshift_data_api.py`, `redshift_copy_unload.py` (2026-09-20) and `redshift_manifest.py` (2026-09-21, the prerequisites of `export_partition`); `examples/README.md` says what each one fixes. The probe, the suite and stage 5 repeat their calls, and the target they fix is in `.claude/memory/redshift.md`. |
| `probes/` | Read-only scripts that photograph the environment (`.venv/bin/python probes/<script>.py`; the report also lands in `probes/output/`, ignored by git, for pasting into the conversation). `probes/README.md` indexes them, details every check and fixes a probe's structure: `space.py`, `bucket.py`, `diagnose_aws.py`, `redshift.py` (`RS-1` to `RS-19`; the connection repeats `examples/redshift_native.py`), `catalog.py` and `parquet_source.py` (`--sample N`), over `probelib.py` (`duckdb_proxy`, `hide_credentials`, `report.last_reason`; DNS, TCP and internet results are readings, never failed calls). `tests/test_probes.py` covers the pure helpers with fabricated responses, no network. |
| `docs/readings/` | The probe reports backing a statement in `docs/POC.md`, kept as they came out, indexed by `docs/readings/README.md`. |
| `docs/guia.md` | ETL practices the pipeline follows: immutable partitions with idempotent replacement, write-audit-publish, the schema contract, and the open question about committing metadata atomically on S3. |
| `docs/schema.md` | DDL from the ORM models, `Table.info["serialize_db"]` (`partition_by`, `sort_key`, `redshift`), constraint policy per backend, the type table from SQLAlchemy to Arrow, Delta, DuckDB and Redshift, SQL portability between the engines, and the JSON field per layer. |
| `docs/parquet.md` | Parquet file layout and every metadata structure, inspection with DuckDB and with PyArrow, partitioning, query optimization by layer, and import and export in DuckDB and in Redshift. |
| `docs/duckdb.md` | DuckDB as the execution sandbox. |
| `docs/redshift.md` | Redshift as the publication database and the second execution engine; it opens with the diagnostic queries for a session and holds the manifest conversions between the Delta log and `COPY`/`UNLOAD`. |
| `docs/sqlalchemy.md` | SQLAlchemy as the schema contract: metadata, reflection, deferrable constraints, Core and ORM for DDL and DML, server-generated keys, SQL generation per dialect (`compile`, dialect objects and paramstyles, `literal_binds`, `render_postcompile`, `create_mock_engine`, `echo`), the `Numeric` float conversion, the verdict per part, the recommendation without the compatibility premise and the gradual replacement of runtime compilation by generated SQL text (`param`, `prefixed`, `render`, `write_sql_files`, `execute`). |
| `docs/delta.md` | Delta Lake as the source of truth: folder layout and log actions, Delta versus Iceberg, the implementations and the delta-rs gaps, S3 requirements, types and JSON, table creation from the model, schema evolution with the measured rename/drop rewrite, transactions, conflicts and restore, DML, ingestion and export, pipeline steps, DuckDB and Redshift access, performance, relocation and SQLAlchemy support. |
| `docs/PLAN.md` | The plan (pt-BR): the decisions with the premises behind them, the streaming `pa.RecordBatch` boundary with client code and its measured hazards, the rules every stage obeys, the package layout with dependencies, configuration and test policy, the table of stages 0 to 9 with delivery and acceptance criterion, the monthly pipeline with the `Execution` API, and the order of work. |
| `docs/PLAN-STAGE-0.md` to `docs/PLAN-STAGE-9.md` | One file per stage, indexed in `docs/PLAN.md`: the module and its primitives with signature and behavior (`schema`, `sql`, `storage` and `delta`, `audit` and `engine.duckdb`, `engine.redshift`, `execution` and `cli`, `load`, the `publication` module, the operation routines), the tests, the dependencies and the proofs of concept that exercise each API, and per stage the sections `Interface` (runnable signature stubs), `Estratégia de implementação`, `Pré-requisitos e pós-condições`, `Testes por caso`, `Rascunhos executados` (the code that ran on 2026-09-21 and its output, the reference for the implementation) and `Decisões pendentes` (mirrored in `docs/OPEN_QUESTIONS.md`); stage 0 holds the Redshift items of the proof of concept and the probes that precede any stage on AWS. |
| `docs/CURRENT_STATE.md` | Where the implementation stands (pt-BR): the situation of each stage, and the repository artifact by artifact, including the reference model's defects and each suite's last pass and skip counts. |
| `docs/POC.md` | What each run showed (pt-BR), with the date of each measurement and its consequence in the plan. |
| `docs/OPEN_QUESTIONS.md` | What has no answer yet (pt-BR): one item per pending question, with the run or the decision that will close it; a closed item leaves the file when its answer lands in the owning document. |
| `docs/estrategia.md` | Rationale and comparisons only: the premises, table layers without a catalog service against the requirements, the Redshift path by `COPY ... MANIFEST`, the SQL layer options, contract and audit tools, why Alembic leaves, the Rust/PyO3 assessment, why each layer was chosen or rejected, and Delta against Iceberg with the re-evaluation trigger. |
| `docs/serialize-db.md` | The library's modeling: features, own metadata (commit keys, `_serialize_db/snapshots.json`, `serialize_db_publications`), the flow of each use case, and the parallelism section (what the library guarantees, parallel reads and writes per technology, the client's `Future` dependencies, `next_ids`, pure-Python work beside the library's threads); the primitives live in `docs/PLAN-STAGE-<n>.md`. |
| `tests/model/` | The reference model: the declarative ORM models the tests hand to the package API as a client library would hand its own; the package holds no model. |
| `tests/source_db_projetado.py` | The fictitious Parquet source base `db_projetado`, reproducing the structure `probes/parquet_source.py` read in the dev base (`.claude/memory/source-base.md`), checked by `tests/test_source_db_projetado.py`; the material of the stage 7 test. |

`docs/duckdb.md`, `docs/redshift.md` and `docs/delta.md` share a section order: data organization and
the differences from PostgreSQL, supported types with `DECIMAL` and JSON, DDL,
`SELECT`/`INSERT`/`UPDATE`/`DELETE`, ingestion, export to Parquet, performance recommendations,
SQLAlchemy support, references. `docs/delta.md` adds schema evolution, transactions, the pipeline,
DuckDB and Redshift access, and relocation.

## Working rules

Each rule is distilled from a lesson; the story behind it, with its date, is in `.claude/memory/lessons.md`.
A new lesson adds its story there and its rule here, in the same commit.

- **A documented behavior becomes a test assertion only after a probe reproduces it**: run a ten-line
  probe first, assert the observed value, record the rest in the session report (2026-09-19).
- **A claim of "writes nothing" or "needs nothing" is verified in a stripped environment**: no `AWS_*`
  and no proxy variables, proxies pointed at a closed port, an empty `HOME` for the subprocess (2026-09-19).
- **A script that resolves paths by pattern runs on both platforms before it is trusted**; it stops when
  the pattern matches nothing. macOS has no `timeout` command: time a subprocess from Python (2026-09-19).
- **Nothing unpinned enters the project venv**: try a package in a scratch venv, read the versions after
  any install (`probes/space.py` SP-9), restore with `uv sync --group dev` (2026-09-19).
- **The pytest layout has no `__init__.py`**: `tests/conftest.py` is imported as `conftest` and its folder
  lands on `sys.path`, so `from conftest import ...` and `from poc_delta import ...` work inside
  `tests/proof_of_concept/`; test-module basenames stay unique across the two folders.
- **Before a commit**: `uv run pytest` with no variables and again with `SERIALIZE_DB_TEST_LOCAL_ROOT` set
  to the scratchpad, both green; `py_compile` on an edited probe; every `](...md)` link target checked;
  `git status` clean of stray files; this file under budget; `git branch --show-current` a `claude/`
  branch whose PR is still open (`gh pr view --json state`), because the user merges and syncs `main`
  between turns, and a merged PR means a new branch (2026-09-21).
- **Git and shell traps**: after `git mv`, add the new path; in zsh, quote `--include=*.md`, run a command
  held in a variable as `${=command}`, and never pass `====` to `echo` (it is `=command` expansion); a Bash
  result above about 50 KB goes to a file; read long documents in `sed -n` ranges.
- **A subprocess probe prints its own one-line error** (`Type: message` to stderr); probe code held in a
  Python string is a raw string. "The service answered with an error" and "no response" are different
  verdicts: only the second means the network needs maintenance.
- **Restructuring a document is a scripted splice followed by checks**: list the headings, grep for
  references to the removed sections and for stale identifiers, test every link target; when trimming or
  moving memory, list the backticked spans and numbers of the old text missing from the new one, because
  a fact removed is a defect (2026-09-19).
- **A change the user did not ask for is named in the report**, and a rule proposed without confirmation
  is marked as awaiting it.
- **API details learned by running live go to `tests/proof_of_concept/` and to the theme file**, not
  here; read the study suite of a library before writing code against its API.
- **A probe's first real run tests its parsing and its verdicts**: serialize data as data, make expected
  conditions readings, derive lists from the source of truth, give every check a branch for the denied
  call, read a label against each item it covers, make two readings of one thing agree or explain the
  difference, have a report record its own outcome (2026-09-20).
- **A variable set to the empty string is not absent**: render `(vazia)` apart from `(ausente)`, restore a
  variable to the value found instead of removing it, run the failing case and the fix side by side
  (2026-09-20).
- **A probe's inputs must separate the hypotheses**: pick values whose outcomes differ per hypothesis
  before wording a rule (`1.236` and `2.675` told truncation from rounding, 2026-09-20).
- **A probe's helper thread stops in `finally`; a speedup is a reading**, the assertion is the loop rate
  (2026-09-20).
- **A section moved between files carries its deixis**: grep the moved block for "abaixo", "acima",
  "este", "desta" and "seção", and turn each reference that now crosses files into a link (2026-09-20).
- **A read-only claim over the user's data is proved by a copy and a `diff -r`**, plus a grep for `open(`,
  `write`, `mkdir`, `unlink`, `remove` and `rmtree`; a fixture carries the defects the tool must find, one
  per table (2026-09-20).
- **A fixture that reproduces a reading is verified by the reading instrument**, and asserted in its
  vocabulary (2026-09-20).
- **The writer's own control file explains a difference before a hypothesis does**: ask for the metadata
  beside the data before listing hypotheses (2026-09-20).
- **A documented phrase is read two ways until something runs**: write the plainest reading, mark the
  other in `docs/OPEN_QUESTIONS.md`, grep the repository before calling a question open, and grep tables
  and lists when a fact changes (2026-09-20).
- **A probe's verdict is a hypothesis until the environment answers, and one repetition separates the
  transient from the permanent**: read a value the reading cannot produce as unread, treat an expected
  absence as a reading (`Report.call(expected=True)`), stop the section when the connection dies, run the
  probe twice before writing a consequence into a plan (2026-09-20).
- **A script the user ran in the target outranks a plan written without one**: keep it verbatim in
  `examples/`, with its literal values, and make the probe and the suite repeat its calls, the joins of
  URIs included: `DeltaTable.table_uri` ends with a slash, the example's base string did not, and the
  `//` cost a run in the target (2026-09-20, 2026-09-21).
- **Each tool in a script reads the proxy its own way**: `uv` accepts `http://user:password@host:port`,
  DuckDB refuses it and reads only `HTTP_PROXY`; the probe is a local socket that logs the request and
  answers 407, fed a password with a character URL-encoding changes (2026-09-20).
- **A generator handed to native code is pulled by a thread the library does not own**: insert batch by
  batch in a transaction, never block without a timeout inside a generator a native reader pulls, keep
  helper threads free of references to their owner; `to_pyarrow_table()` right before exit hangs the
  process, `to_pyarrow_dataset()` does not (2026-09-20).
- **A reader built by `from_batches` trusts its batches**: cast every batch to the declared schema before
  it enters a reader (2026-09-20).
- **A session-state change inside a probe splits its readings**: every reading of the pre-change state
  before the change, the change confirmed by a query, and after it only what needs the new state
  (2026-09-20).
- **A run answers more than the question it was written for**: read every line a successful run prints
  against the documents, not only the answer (2026-09-21).
- **A trust-the-caller API is probed field by field before its guards are designed**: register one wrong
  field at a time, read through every reader the pipeline uses, put the cases in the study suite before
  the check enters the plan (2026-09-21).
- **A memory that hits its ceiling is split, not trimmed**: a fact goes to its theme file in
  `.claude/memory/`, and this file changes only when a rule, a convention or an index entry changes
  (2026-09-21).
- **A state change is confirmed by the effect the caller depends on**, never by a system function
  believed to report it: `current_database()` stayed `dev` after a `USE` that made two-part names
  resolve, and the check built on it failed a working environment (2026-09-21).
- **`secrets/` stays unread unless the user names a path inside it**: the probe reports of
  2026-09-21 were read from `secrets/probes-aws-bn/` on request, only that folder, and nothing from
  them is copied into git; the facts go to `docs/POC.md` (2026-09-21).
- **A shared connection's transaction mode is set before its first statement, and a denied reading
  never shares a transaction with what follows**: `redshift_connector` begins a transaction before the
  first `execute` when autocommit is off and leaves it open when autocommit is switched on later; one
  denied system view then aborts every later statement and the cleanup with 25P02. A report that counts
  failures records their messages and creates its own folder (2026-09-21).
- **A statement repeated on one connection is prepared again after any DDL, and a driver's cache is
  read in its source before a retry is designed**: `redshift_connector` reuses a named prepared
  statement per SQL text and clears its cache only on `ALTER`, `CREATE`, `DROP` and `ROLLBACK`; a
  `TRUNCATE` between the `Parse` and a later `Execute` got 34510 from the datashare in two runs. The
  suite and the library connect with `max_prepared_statements=0`, and a loop that repeats a statement
  records each outcome before the next step that can fail (2026-09-21).
- **A plan revision reads every stage against the decisions memory**: a sentence written before a
  decision survives in another section (the `pc.round` of the initial load, contradicting the
  `Double` decision of 2026-09-20, found only by the full review of 2026-09-21); grep the plan for the
  old rule's vocabulary when a decision lands.

## Naming conventions

The convention was applied to every example in `docs/` on 2026-09-19. ORM model classes and column
mixins (`Operacao`, `Lancamento`, `Rastreio`) keep Portuguese names: they are data-model artifacts,
like tables and columns. Python variables, functions, parameters, modules, the proposed API
(`Database`, `Execution`, `ingest`, `audit`, `publish`) and the keys of `Table.info["serialize_db"]`
(`partition_by`, `partition_source`, `sort_key`, `redshift`) are English. The library's own metadata is English (user
decisions of 2026-09-19). A key under `_serialize_db/` carries no prefix (`snapshots` in
`_serialize_db/snapshots.json`); everything else the library writes carries the `serialize_db_`
prefix: the commit keys `serialize_db_execution_id`, `serialize_db_input_versions` and
`serialize_db_snapshot`, the Parquet footer keys `serialize_db_version` and
`serialize_db_execution_id`, and the Redshift control table
`serialize_db_publications(table_name, delta_version, execution_id, published_at)`, whose columns
stay unprefixed because the table name is the namespace. `snapshot` is an accepted loanword in
prose. Every variable the project requires starts with `SERIALIZE_DB_` (`SERIALIZE_DB_ROOT` and the
library's own, `SERIALIZE_DB_REDSHIFT_*`, `SERIALIZE_DB_TEST_*`, `SERIALIZE_DB_DUCKDB_EXTENSIONS`);
the unprefixed ones the probes and the suites read are third-party standards (`AWS_REGION`,
`AWS_DEFAULT_REGION`, `AWS_ENDPOINT_URL*`, `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, the proxy
variables in both spellings), read by the libraries that define them, and renaming them would break
those libraries. The S3 root a probe photographs comes from `probelib.s3_root`: the argument, then
`SERIALIZE_DB_ROOT`, then `SERIALIZE_DB_TEST_S3_ROOT` (user question of 2026-09-20; `S3_TMP_PATH` is
the space's, not the project's, and nothing in the repository reads it). Data tables and columns stay Portuguese, including the `id_execucao` column of the `Rastreio`
mixin in `docs/sqlalchemy.md`. SQL placeholders in prose (`COPY (consulta) TO ...`) and staging
table names (`staging_<tabela>`) count as database identifiers.

Every Python block in `docs/` ran in the session scratchpad through `uv run --no-project
--python 3.13 --with "deltalake==1.6.4" --with "duckdb==1.5.5" --with "pyarrow==25.0.1" ...`; the
scripts were not kept, the documents are the record. New examples are checked against the
identifier convention by tokenizing the Python blocks: only `NAME` tokens are candidates, and
strings, attribute access after `.`, `name: Mapped[...]` annotations and keyword arguments of
`.values(...)` and of model constructors are column names. The Portuguese names left after that
check are columns, ORM classes and the loanwords `sandbox` and `staging`.

## Where the work stands

Read `docs/PLAN.md`, the stage files, `docs/CURRENT_STATE.md`, `docs/POC.md` and
`docs/OPEN_QUESTIONS.md` before planning a session. The next session starts stage 1
(`serialize_db.schema`, with `serialize_db.errors`) and stage 2 (`serialize_db.sql`) on local
folders, from the `Interface` and `Rascunhos executados` sections of `docs/PLAN-STAGE-1.md` and
`docs/PLAN-STAGE-2.md`; the pending API decisions are listed per stage in `docs/OPEN_QUESTIONS.md`.
The plan's unit is the partition (`publish_partition`, `partitions=`, `Execution(partition=...)`),
never the month. The probe, `tests/conftest.py` and the Redshift suite follow the scripts in
`examples/`, and `docs/PLAN-STAGE-5.md` and `docs/PLAN-STAGE-8.md` carry their consequences. The
five probes ran in the target on 2026-09-21 (reports in `secrets/probes-aws-bn/`, outside git,
interpreted in `docs/POC.md`): no proxy, S3 by gateway endpoint, IAM and KMS unreachable, 2 vCPUs
and 7.6 GiB; `RS-19` failed on the wrong criterion, `current_database()` does not reflect the `USE`
(user confirmation), the probe now resolves a two-part name, `RS-5` was read `false` by the suite (`has_schema_privilege`
does not prove the privilege on the datashare schema; the `CREATE` does), and `RS-8` remains unread.
The Redshift suite ran six times in the target on 2026-09-21 (10:50: 1 passed, one transaction
opened before the `USE`, fixed in `tests/conftest.py`; 11:28: 7 passed, the manifest URL's double
slash and `information_schema` blind to the datashare, fixed in the suite; 12:08 and 12:10: 10
passed, the count repeated after a `TRUNCATE` refused with 34510 because of the driver's prepared
statement cache, now off; 13:35 and 13:39: all 12 passed, the two clean runs stage 0 required).
Stage 0 is closed: the `COPY` questions are answered (types, column list and `FILLRECORD`,
positional count, `VARCHAR` aborts and `TRUNCATECOLUMNS` refused, parallel), the `UNLOAD`
destination is checked as a prefix (stage 5 unloads to `<uri>/<execution_id>/<valor>/`), a Parquet
string above 65,535 bytes never reaches `SUPER` by `COPY` while `FORMAT JSON 'auto'` and
`INSERT ... JSON_PARSE` load it, and the readings the two clean runs repeated are assertions. `RS-8`
(`svv_table_info` after the `USE`) stays with the probe; no stage depends on it. Two proposals await
the user in `docs/PLAN-STAGE-8.md`: `FILLRECORD` on every library `COPY`, and the 65,535-byte
ceiling of the JSON field checked by the audit.

The client boundary's reference sketches `BatchStream` and `Loader` are in
`tests/proof_of_concept/test_parallel.py`; the Redshift driver materializes a result in `execute`
(`docs/redshift.md`), so `stream` bounds memory only through `UNLOAD`.
