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

## Repository index

Read the file listed here before researching its subject again. Each document names the pages it
came from, and `REFERENCES.md` collects every URL consulted so far, grouped by subject: Parquet
format, Redshift, DuckDB, PyArrow, SQLAlchemy, pandas, PyIceberg, Glue Data Catalog, Athena, Lake
Formation, SageMaker Unified Studio, S3, Python packages, Delta Lake, DuckLake, Hudi, SQL tooling
(SQLGlot, SQLMesh, dbt, Ibis, dlt), data-contract tools, Rust/PyO3 and consulted repositories. New
research appends to the matching group.

| File | Subject |
| --- | --- |
| `README.md` | `uv sync --group dev`, the test layout (`tests/` for the package, `tests/proof_of_concept/` for the proofs of concept and the study suites of the external libraries) with one command per suite, the rule that each authorization variable enables the writes under it and the variables of the three targets, the probes, the delta-rs credentials and proxy note, and why the folder is prepared for the target without internet, with the procedure itself in the `prepare_offline.sh` header. |
| `prepare_offline.sh` | Makes the project folder self-contained for the target without internet: managed Python in `.python/`, the package and every `pyproject.toml` group in `.venv/` (`uv sync --all-groups`), DuckDB extensions in `.duckdb/`, all links relative. **Review it whenever a dependency is added**: Python packages come in through `uv sync`; a new DuckDB extension, a Python version change or another runtime asset is added by hand, and the user reruns it before packing. It runs on any platform and stops when `.python/` has no interpreter; only a folder prepared on Linux x86_64 serves the SageMaker space. Its header is the operating procedure: how to run it, the proxy variables and the pack and unpack commands. The extensions block configures the DuckDB proxy through `probelib.duckdb_proxy`, the probes' own function, because DuckDB refuses an address with the credentials inside it. |
| `examples/` | The scripts the user ran in the target, kept as run: `redshift_native.py` (`GetWorkgroup` for the endpoint, `GetCredentials` for a temporary user and password, `redshift_connector.connect`), `redshift_data_api.py` (the async Data API cycle) and `redshift_copy_unload.py` (`USE` into the datashare database, `CREATE TABLE`, `COPY` of a Parquet prefix and `UNLOAD`, all with the caller's credentials in the statement text). They fix the target: region `sa-east-1`, workgroup `controladoria-wg`, connection database `dev`, schema `sbx_aco_decon` in the datashare database `datalake_rw_shared`, which the session enters with `USE`. `redshift_manifest.py` ran on 2026-09-21: a `cad_contratos` partition converted to Delta, then `COPY ... MANIFEST` and `UNLOAD ... PARTITION BY ... MANIFEST VERBOSE` on a datashare table, the prerequisites of `export_partition`, both accepted. The probe, the suite and stage 5 repeat what ran. |
| `probes/` | Read-only scripts that photograph the environment (`.venv/bin/python probes/<script>.py`; the report also lands in `probes/output/`, ignored by git, for pasting into the conversation). `probes/README.md` indexes them, details every check and fixes a probe's structure — `space.py` (the space: credentials and expiry, region, connections, network, the machine, the `~/shared` mount, the pinned versions, DuckDB extensions), `bucket.py` (the S3 root: settings, lifecycle, inventory, the role's permissions, versioning, KMS, Object Lock), `diagnose_aws.py` (the access the S3 suite needs and its maintenance verdict, own format; DuckDB listing globs with `**`, because `*` does not cross `/`), `redshift.py` (`SERIALIZE_DB_REDSHIFT_*` or the project connection parsed as a dict; the connection repeats `examples/redshift_native.py`, a given user/password pair the only alternative; `RS-1` to `RS-19`, from the configuration to the IAM simulation of the identity the library sends to S3, with the session readings before the `USE` into the datashare database, checked by `current_database()` as `RS-19`, and the schema readings after it), `catalog.py` (the re-evaluation trigger: Glue, Athena, Lake Formation, S3 Tables) and `parquet_source.py` (the initial load's source base, one folder per table, everything read from the footers and the divergent files named; `--sample N` reads what the footer does not hold, and without it no data page is read). Over `probelib.py`: DNS, TCP and internet results are readings, never failed calls, and a failed call leaves `report.last_reason` for the check that interprets it; `duckdb_proxy` splits the proxy address from the credentials for every DuckDB session (`prepare_offline.sh` uses it too) and `hide_credentials` keeps the embedded password out of a report meant to be pasted into the conversation; the probes import `sagemaker-studio` from the system interpreter (see the unpinned-packages lesson); `tests/test_probes.py` covers the pure helpers with fabricated responses, no network. |
| `docs/readings/` | The probe reports backing a statement in `docs/POC.md`, kept as they came out, indexed by `docs/readings/README.md`: the target Redshift readings of 2026-09-20. |
| `docs/guia.md` | ETL practices the pipeline follows: immutable monthly partitions with idempotent replacement, write-audit-publish, the schema contract, and the open question about committing metadata atomically on S3. |
| `docs/schema.md` | DDL from the ORM models, `Table.info["serialize_db"]` (`partition_by`, `sort_key`, `redshift`), constraint policy per backend, the type table from SQLAlchemy to Arrow, Delta, DuckDB and Redshift, SQL portability between the engines, and the JSON field per layer. |
| `docs/parquet.md` | Parquet file layout and every metadata structure, inspection with DuckDB and with PyArrow, partitioning, query optimization by layer, and import and export in DuckDB and in Redshift. |
| `docs/duckdb.md` | DuckDB as the execution sandbox. |
| `docs/redshift.md` | Redshift as the publication database and the second execution engine; it opens with the diagnostic queries for a session. |
| `docs/sqlalchemy.md` | SQLAlchemy as the schema contract: metadata, reflection, deferrable constraints, Core and ORM for DDL and DML, server-generated keys, SQL generation per dialect (`compile`, dialect objects and paramstyles, `literal_binds`, `render_postcompile`, `create_mock_engine`, `echo`), the `Numeric` float conversion, what each dialect and Parquet support, the verdict per part, the recommendation without the compatibility premise (own contract with Arrow as canonical form, hand-written SQL validated by SQLGlot) and the gradual replacement of runtime compilation by generated SQL text (`param`, `prefixed`, `render`, `write_sql_files`, `execute`). |
| `docs/delta.md` | Delta Lake as the source of truth: folder layout and log actions, Delta versus Iceberg, the implementations (delta-spark, delta-rs, Delta Kernel) and the delta-rs gaps, S3 requirements, types and JSON, table creation from the model, schema evolution with the measured rename/drop rewrite and what replaces Alembic, transactions, conflicts and restore, DML, ingestion and export back to Parquet folders by month, pipeline steps, DuckDB and Redshift access, performance measurements, relocation and SQLAlchemy support. |
| `docs/PLAN.md` | The plan (pt-BR): the decisions with the premises behind them, the streaming `pa.RecordBatch` boundary with client code (`stream` and `loader`, `pa.Table` as the convenience) with its measured hazards, the measured pandas conversion, the rules every stage obeys, the package layout with dependencies, configuration and test policy, the table of stages 0 to 9 with delivery and acceptance criterion and the index of the stage files, the monthly pipeline with the `Execution` API, and the order of work. |
| `docs/PLAN-STAGE-0.md` to `docs/PLAN-STAGE-9.md` | One file per stage, indexed in `docs/PLAN.md`: the module and its primitives with signature and behavior (`schema`, `sql`, `storage` and `delta`, `audit` and `engine.duckdb`, `engine.redshift`, `execution` and `cli`, `load`, the Redshift publication, the operation routines), the tests, the dependencies and the proofs of concept that exercise each API; stage 0 holds the Redshift items of the proof of concept and the probes that precede any stage on AWS. |
| `docs/CURRENT_STATE.md` | Where the implementation stands (pt-BR): the situation of each stage, and the repository artifact by artifact, including the reference model's defects and each suite's last pass and skip counts. |
| `docs/POC.md` | What each run showed (pt-BR): the S3 proof of concept in the SageMaker space with its timings, the local one, the delta-rs credential variants and the isolated `NO_PROXY` 403, and the probe readings of the space, each with its consequence in the plan. |
| `docs/OPEN_QUESTIONS.md` | What has no answer yet (pt-BR): one item per pending question, with the run or the decision that will close it, plus the Redshift questions the official documentation does not answer; a closed item leaves the file when its answer lands in the owning document. |
| `docs/estrategia.md` | Rationale and comparisons only: the premises, table layers without a catalog service (Delta via delta-rs, DuckLake, Iceberg without a catalog, Hudi, hand-rolled manifests) against the requirements, the Redshift path by `COPY ... MANIFEST`, the SQL layer options (SQLAlchemy Core, SQLGlot, SQLMesh, dbt, Ibis, dlt), contract and audit tools, why Alembic leaves, the Rust/PyO3 assessment, why each layer was chosen or rejected, and the maturity assessment of Delta against Iceberg with the re-evaluation trigger. |
| `docs/serialize-db.md` | The library's modeling: features, own metadata (commit keys, `_serialize_db/snapshots.json`, `serialize_db_publications`), the flow of each use case, and the parallelism section (what the library guarantees, parallel reads and writes per technology, the client's `Future` dependencies, `next_ids`, pure-Python work beside the library's threads); the primitives live in `docs/PLAN-STAGE-<n>.md`. |
| `tests/model/` | The reference model: the declarative ORM models of the accounting, management and projection tables, moved out of the package on 2026-09-20. The tests hand it to the package API as a client library would hand its own models; the package holds no model. |
| `tests/source_db_projetado.py` | The fictitious Parquet source base `db_projetado`, reproducing the structure `probes/parquet_source.py` read in the dev base on 2026-09-20, which the source-base fact below states in full: the 14 tables with the read columns, types and nullability (12 match the reference model; `alembic_version` and `meta_update_status` are outside it), the Hive partitions, the `chunk_<n>` files, the `INT96` timestamps, and the previous library's real `schema.json` at the root (`source_db_projetado_schema.json`). The values the load handles (`valor` with three decimals, `fator` with five, `id_lancamento` up to 1,113,599,996). The data is consistent with the reference model (unique keys, every foreign key satisfied, the four dates in every partitioned table, the N×N `rel_contrato_operacao` with dyadic `fator_rateio` summing to 1 per operation). `write_source(root)` returns the files and row counts; `tests/test_source_db_projetado.py` checks the written files against the transcribed section 3 of the report, the model's keys and the schema control. The material of the stage 7 test. |

`docs/duckdb.md`, `docs/redshift.md` and `docs/delta.md` share a section order: data organization and
the differences from PostgreSQL, supported types with `DECIMAL` and JSON, DDL,
`SELECT`/`INSERT`/`UPDATE`/`DELETE`, ingestion, export to Parquet, performance recommendations,
SQLAlchemy support, references. `docs/delta.md` adds schema evolution, transactions, the pipeline,
DuckDB and Redshift access, and relocation.

## Lessons learned

Process lessons from the sessions so far; the technical facts stay in the list below and in the
study suites. A lesson is added at the end of a
unit of work when a mistake cost a retry or a verification changed the plan, with its date.

- **A documented behavior becomes a test assertion only after a probe reproduces it**
  (2026-09-19). Three claims taken from `docs/duckdb.md` and `docs/delta.md` failed as assertions:
  the DuckDB Arrow reader returns zero rows after another command instead of raising;
  `read_parquet` on a single file under `mes=.../` adds `mes` by Hive auto-detection, so a file's
  real columns come from `parquet_schema`; the `DECIMAL` inferred from a pandas column came from all
  the values, not a sample; the log cleanup with `delta.logRetentionDuration = interval 0 days` did
  not happen after six commits and a checkpoint. Run a ten-line probe first, assert the observed
  value, and record the rest in the session report.
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
  `from poc_delta import ...` work inside `tests/proof_of_concept/`; test-module basenames must stay
  unique across `tests/` and `tests/proof_of_concept/`.
- **Before a commit**: `uv run pytest` with no variables and again with
  `SERIALIZE_DB_TEST_LOCAL_ROOT` set to the scratchpad, both green; `py_compile` on an edited probe;
  `git status` clean of stray files.
- **Git and shell traps.** After `git mv`, add the new path, not the old one. In zsh, quote
  `--include=*.md`, and an unquoted `$command` runs as one word (a probe loop silently ran nothing on
  2026-09-19): use `${=command}`. A Bash result above about 50 KB goes to a file; read long documents
  in `sed -n` ranges.
- **A subprocess probe prints its own one-line error.** A DuckDB error inside a subprocess showed
  only `^`; the probe catches the exception and prints `Type: message` to stderr. Probe code held
  in a Python string is a raw string, or `\[` raises a `SyntaxWarning`. "The service answered with
  an error" and "no response" are different verdicts: only the second means the network needs
  maintenance.
- **Restructuring a document is a scripted splice followed by checks** (2026-09-19). Split on
  heading markers with a Python script, then list the headings, grep for references to the removed
  sections and for stale identifiers across the repository, and test every `](...md)` link target.
  Trimming this file: list the backticked spans and numbers of the old text missing from the new
  one; a fact removed is a defect.
- **A change the user did not ask for is named in the report.** Moving the primitives out of
  `docs/serialize-db.md` followed from the plan request and was flagged as such; the `Text` rule
  proposed in `docs/OPEN_QUESTIONS.md` is marked as awaiting the user's confirmation.
- **API details learned by running live in `tests/proof_of_concept/`, not here**:
  `schema_mode="merge"` for an append with fewer columns than the evolved table, the normalized
  `CHECK` expression, `filters=` instead of the deprecated `partitions=`, `pa.schema(dt.schema())`
  through the PyCapsule interface, `partition.mes` and `size_bytes` in
  `get_add_actions(flatten=True)`, SUPER binds rendered as `json_parse(%s)`. Read the study suite
  of a library before writing code against its API.
- **A probe's first real run tests its parsing and its verdicts** (2026-09-20). Three lab runs
  found: connection data serialized as a repr string, unreadable by `find_values`; DNS, TCP and
  internet readings counted as failed calls (exit 1 in the target, where they always fail); a pin
  list drifted from `pyproject.toml`; `BK-11` without a branch for a denied call; the S3
  gateway-endpoint label applied to every public name; a mount test a symlink defeats; a DuckDB
  count of 0 beside boto3's 16 (non-recursive glob); a test report silent on its outcome and
  cleanup. Serialize data as data, make expected conditions readings, derive lists from the source
  of truth, give every check a branch for the denied call, read a label against each item it
  covers, make two readings of one thing agree or explain the difference, and have a report record
  its own outcome.
- **A variable set to the empty string is not absent** (2026-09-20). `os.environ.get` is falsy for
  both, so the suite's `as_found` variant removed `NO_PROXY` and passed while the probe found it
  empty and got 403. Render `(vazia)` apart from `(ausente)`, restore a variable to the value found
  instead of removing it, and run the failing case and the fix side by side.
- **A probe's inputs must separate the hypotheses** (2026-09-20). `1.234` cast to two decimals gives
  `1.23` under truncation and under rounding, and the first reading said "truncates"; only `1.236`
  (`1.24`) and `2.675` (`2.67`) showed the cast rounds the binary value, which changed the `cast` rule
  from refusing every `double` to refusing the ones outside the scale. Pick values whose outcomes
  differ per hypothesis before wording a rule.
- **A probe's helper thread stops in `finally`; a speedup is a reading** (2026-09-20). The first
  GIL probe raised on a column name (`range(n)` exposes `range`, not `i`), skipped `stop.set()` and
  spun at 100% CPU until killed. A two-thread speedup depends on the free cores and on the DuckDB
  `threads` of the instance, so it is recorded; the assertion is the loop rate near zero when the GIL
  is held.
- **A section moved between files carries its deixis with it** (2026-09-20). The backticked-span
  and number check passes on "este plano", "Cada etapa abaixo" and "após as correções desta
  sessão", because the words survive the move; only reading the moved text finds them, and the
  second of the three had been stale since the previous split. After moving a block, grep it for
  "abaixo", "acima", "este", "desta" and "seção", and turn each reference that now crosses files
  into a link.
- **A read-only claim over the user's data is proved by a copy and a `diff -r`** (2026-09-20).
  Reading the probe's code for write calls proves only what the reader thought to look for. Copy the
  fixture, run against the copy's source, diff the two afterwards, and grep the probe for `open(`,
  `write`, `mkdir`, `unlink`, `remove` and `rmtree` as the second reading. Build the fixture with the
  defects the tool must find, one per table: a column added in one month, a type changed between
  partitions, a partition column also written inside the file, a mixed partition depth, a folder
  without a Parquet file, a file with no rows. A fixture without defects exercises no verdict, and
  that first run also found four rendering defects in the report itself.
- **A fixture that reproduces a reading is verified by the reading instrument** (2026-09-20).
  `tests/source_db_projetado.py` was checked by running `probes/parquet_source.py` on it and diffing
  section 3 against the real report (identical), and the test transcribes that section as the
  expected value. A hand-written footer check failed first: `FileMetaData.metadata` carries
  `ARROW:schema`, which the Arrow schema metadata the probe reads does not. Read the file the way
  the instrument reads it, and assert in its vocabulary.
- **The writer's own control file explains a difference before a hypothesis does** (2026-09-20).
  Seven `cad_contratos` columns nullable in the files and `NOT NULL` in the model came from the
  previous library's `schema.json`, the SQLAlchemy reflection of the source database, which also
  lacked the model's composite foreign keys; one reply from the user closed two open questions.
  Ask for the metadata beside the data before listing hypotheses about it.
- **A documented phrase is read two ways until something runs, and the answer may already be in the
  repo** (2026-09-20). "COPY without COMPUPDATE", in the datashare write list, became "emit
  `COMPUPDATE OFF`" in the suite and in two stage files; the script that ran in the target carries
  no `COMPUPDATE` clause, and passed. The question left open after it was answered two documents
  away, by this repo's own table of Parquet `COPY` rules: that `COPY` rejects the parameter. Write
  the plainest reading, mark the other in `docs/OPEN_QUESTIONS.md`, and grep the repository before
  calling a question open. A changed fact is then grepped in tables and lists too: `README.md`
  promised `IAM_ROLE default` two PRs after its prose said the caller's credentials.
- **A probe's verdict is a hypothesis until the environment answers, and one repetition separates
  the transient from the permanent** (2026-09-20). The first Redshift run in the target failed
  `RS-17` on an isolation level of `UNKNOWN`, which is the consumer not seeing the producer's
  database, not serializable isolation; it declared `svv_external_schemas` unreadable when a 10 s
  read timeout had actually killed the socket, and every later query inherited
  `cannot read from timed out object`; and it counted five expected absences (the package missing
  outside a space, system views denied to a regular user) as failed calls, which is what the exit
  code is for. The repetition six minutes later answered `sys_load_error_detail` in 1.5 s and
  `svv_external_schemas` too, which turned "the administrator will have to read the load errors"
  into "the view is readable". Read a value the reading cannot produce as unread, treat an expected
  absence as a reading (`Report.call(expected=True)`), stop the section when the connection dies,
  and run the probe twice before writing a consequence into a plan.
- **A script the user ran in the target outranks a plan written without one** (2026-09-20). Two
  connection scripts from the target replaced the plan's default (password) with the workgroup's
  temporary credential and moved the Data API out of the library. Keep such a script verbatim in
  `examples/`, with its literal values, and make the probe and the suite repeat its calls instead of
  a variant nobody executed.
- **Each tool in a script reads the proxy its own way** (2026-09-20). `prepare_offline.sh` got
  through `uv sync` and died on the DuckDB `INSTALL` with the same `HTTP_PROXY`: `uv` accepts
  `http://user:password@host:port`, DuckDB refuses it and reads only the uppercase spelling.
  The probe that settles it is a socket on `127.0.0.1` that logs the request and answers 407:
  it shows the address parsed, which host the request went to, and whether
  `Proxy-Authorization` carried the credentials — none of which the error message says. Feed it
  a password with a character that URL-encoding changes (`se@nha` as `se%40nha`), so a decoded
  and an undecoded credential give different base64.

- **A generator handed to native code is pulled by a thread the library does not own** (2026-09-20).
  The loader sketch fed one `INSERT` from a queue-backed `RecordBatchReader`; when the `INSERT`
  failed, the process hung at exit inside Arrow's thread-pool destructor, because DuckDB's
  `arrow_scan` pulls the stream through an Arrow readahead thread that was still blocked in the
  generator's `queue.get()`, and, with a timeout on the wait, still calling Python while the
  interpreter finalized (`PyThread_hang_thread`). A `sample` of the native stacks found it; a `faulthandler`
  dump did not fire, the hang being after the script's last line. Insert
  batch by batch in a transaction instead, never block without a timeout inside a generator a
  native reader pulls, and keep helper threads free of references to their owner so an abandoned
  object is collected. The same destructor hangs with no generator: `DeltaTable.to_pyarrow_table()`
  leaves an Acero task in flight, so a process exiting right after it never returns, which
  `to_pyarrow_dataset()` or half a second of other work avoids (`docs/POC.md`).
- **A reader built by `from_batches` trusts its batches** (2026-09-20). A batch with the columns in
  another order went through `RecordBatchReader.from_batches(schema, ...)` and DuckDB's `arrow_scan`
  without an error and came out with the bytes swapped. Cast every batch to the declared schema
  before it enters a reader; a probe that feeds a deliberately wrong batch is the check.
- **A session-state change inside a probe splits its readings** (2026-09-20). `USE` landed mid-section in
  the Redshift probe, and `has_database_privilege(current_database())` after it silently read the
  datashare database instead of the connection's, and nothing failed. Put every reading of the pre-change state before
  the change, confirm the change with a query (`current_database()`), and run after it only what
  needs the new state.
- **A run answers more than the question it was written for** (2026-09-21).
  `examples/redshift_manifest.py` existed to close two yes/no questions in the target, and both came
  back yes; what changed six documents was the footer it printed on the way: `TIMESTAMP` in `INT96`, the type the contract removes from the source and the `UNLOAD`
  puts back, `DECIMAL` in `FIXED_LEN_BYTE_ARRAY` where the library writes `INT64`, and 32 files for one
  partition. Read every line a successful run prints against the documents, not only the answer. The `INT96` read like a broken type
  contract until a ten-line probe registered such a file in a `timestamp_ntz` table and both
  readers returned `timestamp[us]` intact: a scare became one recorded cost, the statistics the
  `INT96` does not carry.
- **A trust-the-caller API is probed field by field before its guards are designed** (2026-09-21).
  The critique of `create_write_transaction` was reasoned; the probe that registered one wrong
  field per table found what the reasoning missed: a file without a `NOT NULL` column reads null in
  both readers, a castable type mismatch is cast at read, DataFusion answers `count(*)` from
  `numRecords`, and the reader trusts even `size`. Register one wrong field at a time, read through
  every reader the pipeline uses, and put the cases in the study suite before the check enters the
  plan.

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
  commit mode, and the delta-rs docs page on S3 locking is stale. In the SageMaker space the writer
  finds the container credentials through the default chain, which also consults the `default`
  profile of `~/.aws/config` (`credential_source = EcsContainer`, per the `aws_config::profile`
  warning) but not its `region` (without the variables it went to `us-east-1`). Its HTTP client reads `HTTP_PROXY`/`HTTPS_PROXY` in both spellings but `no_proxy` only
  when `NO_PROXY` is absent: an empty `NO_PROXY`, what a shell opened by the Claude Code extension
  has, sends the credential call through the proxy, which answers 403; absent, exported from
  `no_proxy` or `169.254.170.2` alone passes (isolated 2026-09-20; the 2026-09-19 403 was this). The
  library exports `NO_PROXY` from `no_proxy` when absent or empty and keeps `storage_options` with
  the `boto3` credentials as the fallback. `docs/delta.md`, `docs/estrategia.md`
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
  (the `UNLOAD` path) and checks nothing (2026-09-21): a missing path, false statistics and a file
  without a `NOT NULL` column all commit, and the readers obey the action: false min/max make
  delta-rs, `delta_scan` and DataFusion prune the file that holds the rows, a false `numRecords` is
  DataFusion's `count(*)`, the missing column reads null where `write_deltalake` refuses, a value
  that does not cast fails only when its column is read, and `optimize.compact` rewrites registered
  `INT96`/`FIXED_LEN_BYTE_ARRAY` files as `INT64` with statistics. `ducklake_add_data_files` does
  the same registration for DuckLake but failed on a table partitioned by `year()`/`month()`.
  `docs/POC.md`, `docs/estrategia.md`
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
  `SUPER` on Redshift), `string` in Arrow (user decision of 2026-09-20: the `arrow.json` extension
  dtype has no `.str` kernels in pandas and no engine returns it; DuckDB validates on load into a
  `JSON` column with `Malformed JSON`), `string` in Delta (the extension name kept in field metadata
  when an Arrow schema carries it), `JSON` logical type in Parquet written by PyArrow or
  DuckDB and `String` when written by delta-rs; DuckDB reads `delta_scan` JSON as `VARCHAR` and
  validates only on `::JSON`; Arrow and Delta never validate. `docs/schema.md`, `docs/delta.md`
- S3 needs for Delta: `ListBucket` (prefix), `GetObject`, `PutObject` (commits use
  `If-None-Match: *`, no extra IAM action; `object_store` defaults `aws_conditional_put` to
  `etag`), `DeleteObject` for vacuum, KMS actions only with SSE-KMS; no lifecycle expiration under
  table prefixes; versioning and Object Lock unnecessary. SSE keys in `storage_options`:
  `aws_server_side_encryption`, `aws_sse_kms_key_id`, `aws_sse_bucket_key_enabled`. `docs/delta.md`
- DuckDB has `http_proxy`, `http_proxy_username` and `http_proxy_password` and nothing like
  `NO_PROXY`; it reads only the uppercase `HTTP_PROXY` (the lowercase spelling alone does
  nothing), parses it at request time, not at `SET` time, and refuses an address with the
  credentials inside it (`Failed to parse http_proxy ... into a host and port`), which is how a
  corporate proxy usually reaches the environment. `SET http_proxy` overrides the variable and
  takes `host:port` or `http://host:port`; the password goes in without URL-encode and reaches
  the proxy as `Proxy-Authorization: Basic`. The error covers every DuckDB HTTP call, an `httpfs` S3
  `glob` included, not only an extension download. `probelib.duckdb_proxy` does the split for the
  probes and for `prepare_offline.sh`, reading only `HTTP_PROXY`, because taking the address from a
  spelling DuckDB ignores would send through the proxy the traffic that goes direct today; stage 3
  does the same in `duckdb_setup`. `docs/POC.md`
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
- `to_pandas(types_mapper=pd.ArrowDtype)` on 300,000 rows took 2.3 ms with every buffer shared, and
  `from_pandas` of that DataFrame 0.6 ms, also shared, with every field back
  nullable; the default `to_pandas()` took 37.6 ms with `Decimal` and `date` objects and int-with-null
  as `float64`. `safe=True` does not report two losses: `double` to `decimal128(18, 2)` rounds the
  exact binary value (`2.675` gives `2.67`; `pc.round` gives `2.68`) and `timestamp` to `date32` drops
  the time; `pc.equal(pc.round(x, 2), x)` finds the representable doubles (1.1 ms per 300,000) and
  the round trip finds the timestamps with a time. `Table.cast` refuses nulls in a non-nullable field
  and wants the same names in the same order; `int64` to `decimal128(18, 2)` needs the detour through
  `(21, 2)`; a `dict` column infers `struct` with the union of keys; pandas 3 `str` gives
  `large_string`. `docs/PLAN.md`, `tests/proof_of_concept/test_pyarrow.py`
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
  startup and keep hot pure-Python loops out of the library's threads. `docs/PLAN.md`, section "A troca de dados com o
  código cliente", records the decisions of 2026-09-20. `tests/proof_of_concept/test_concurrency.py`, `test_parallel.py`
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
- A SQLAlchemy schema with a dot is one quoted identifier (`"db.schema".tabela`);
  `MetaData(schema=quoted_name("db.schema", False))` renders the three-part name in DDL, `select`
  and `insert` (2026-09-20, Redshift dialect). `docs/sqlalchemy.md`,
  `tests/proof_of_concept/test_sqlalchemy.py`
- The source base (dev, 2026-09-20): 14 tables, 205 files, 3.76 GB, 187,340,644 rows, written by
  pandas through parquet-cpp-arrow with format 1.0, no dictionary, `INT96` timestamps and the
  `pandas` footer key; Hive partitions `data_str=<YYYY-MM-DD>` (`data_base_str` for
  `cad_lancamentos`) whose value is a month end, lives only in the path and equals `data` or
  `data_base` in every row; up to 36 `chunk_<n>.parquet` files of 1,000,000 rows and one row group
  per partition, not zero-padded. PyArrow reads `INT96` as `timestamp[ns]` without min/max;
  `coerce_int96_timestamp_unit="us"` and DuckDB's `TIMESTAMP` truncate silently and the safe cast
  refuses a non-zero sub-microsecond part; DuckDB `hive_partitioning=true` casts `data_str` to
  `DATE` unless `hive_types_autocast=false`, and a PyArrow dataset keeps it `string`. The 12 model
  tables match the files in columns, order, types and nullability except seven `cad_contratos`
  columns nullable in the files and `NOT NULL` in the model with no null in the data. `docs/POC.md`,
  `docs/PLAN-STAGE-7.md`, `tests/source_db_projetado.py`

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
  accepted by `from_stream`, DuckDB `register` and `write_deltalake`. `docs/PLAN.md`, `docs/POC.md`,
  `docs/duckdb.md`, `tests/proof_of_concept/test_duckdb.py`, `test_pyarrow.py`, `test_parallel.py`

## The pipeline outside this repository

Facts stated by the user, not visible in the code: the pipeline is mostly Python logic; SQLAlchemy
is used only for the declarative models (DDL) and for Core `select` and `insert` statements that
move tables, never for ORM instances; no catalog service is enabled, which excludes Iceberg on
Glue (Iceberg with a SQLite catalog file moved by the library is the documented alternative if Glue
or S3 Tables may be enabled later); development and production runs write separate tables; renaming
or dropping columns is rare. The decision in `docs/PLAN.md`, with the rationale in `docs/estrategia.md`, follows from them: Delta Lake
through `deltalake` as the table layer, SQLAlchemy kept as contract metadata and Core, SQLMesh, dbt and
DuckLake not adopted. SQLAlchemy is in the project for
compatibility with that code (user statement of 2026-09-19); the same day the user decided that
runtime compilation by the dialect is replaced gradually by generated SQL text per dialect, one
database interaction at a time, so SQLAlchemy ends in the models and in generation and
`duckdb_engine` and `sqlalchemy-redshift` leave the runtime dependencies. On 2026-09-20 the user fixed
the exchange type with client code as streaming `pa.RecordBatch` in both directions (`stream` reads,
`loader` writes), with `pa.Table` accepted and returned by `query`, `execute` and `load` only as a
convenience over the same batch API, so the client works on the current batch while the library
reads the next and writes the previous; never an ORM instance, a row list or a DataFrame. The
pipelines run pandas with the pyarrow backend (user statement of 2026-09-20), so
`types_mapper=pd.ArrowDtype` is their native form, and the rule rests on the conversion being cheap,
which the probes of that day measured for the table and for the batch (`docs/PLAN.md`, section "A
troca de dados com o código cliente"). The same day the user moved the
models to `tests/model/` as the reference model: the tests hand it to the package API as a client
library would, and the package holds no model. After the source base was read (2026-09-20) the user
decided: partition by date as text `AAAA-MM-DD` like the reference base, the column and its date
source declared by the client's model (`partition_by`, `partition_source`), so the library's unit is
the partition and never the month; every numeric column stays `Double`, with no rounding and no
fixed-precision `Numeric` (the package supports `Numeric`, and moving `valor` to `Numeric(18, 2)` is
a future improvement); integer keys become `int64` in the Delta; `INT96` timestamps become `INT64`
and their precision does not matter; nullability follows the model until the migration proves it
problematic; the dev base's orphans are ignored and the test base is consistent, with the N×N
`rel_contrato_operacao` whose `fator_rateio` sums to 1 per operation; `alembic_version` and
`meta_update_status` are ignored; `schema.json` at the source root is the previous library's schema
control in SQLAlchemy-reflection form, not Arrow. On 2026-09-20 the user also fixed the Redshift
target: the library's tables live in `datalake_rw_shared.sbx_aco_decon`, the datashare database, so
the connection runs `USE` there and the datashare write rules apply; and the Data API is not a connection
path of the library, only a probe check, a suite test and an example. On 2026-09-21 the user asked
for a flag on how a partition an engine wrote enters the Delta, on both engines and the initial
load: `export_mode="register"` registers the engine's file (`UNLOAD`, DuckDB
`COPY ... (RETURN_STATS)`) after the checks of `docs/PLAN-STAGE-3.md`, `"rewrite"` writes by
`write_deltalake`; the `cad_lancamentos` measurement decides the default, `"register"` until then.

## Naming decisions applied to the documents

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

## Where the work stands

Read `docs/PLAN.md`, the stage files, `docs/CURRENT_STATE.md`, `docs/POC.md` and
`docs/OPEN_QUESTIONS.md` before planning a session. The next session starts stage 1
(`serialize_db.schema`) and stage 2 (`serialize_db.sql`) on local folders. The plan's unit is the
partition (`publish_partition`, `partitions=`, `Execution(partition=...)`), never the month. The
probe, `tests/conftest.py` and the Redshift suite follow the scripts in `examples/`, and
`docs/PLAN-STAGE-5.md` and `docs/PLAN-STAGE-8.md` carry their consequences. The next
`probes/redshift.py` run in the target checks the `USE` (`RS-19`) and what `has_schema_privilege`
answers after it (`docs/OPEN_QUESTIONS.md`).

The client boundary's reference sketches `BatchStream` and `Loader` are in
`tests/proof_of_concept/test_parallel.py`, and the Redshift `fetchmany` question in
`docs/OPEN_QUESTIONS.md`.

Every Python block in `docs/` ran in the session scratchpad through `uv run --no-project
--python 3.13 --with "deltalake==1.6.4" --with "duckdb==1.5.5" --with "pyarrow==25.0.1" ...`; the
scripts were not kept, the documents are the record. New examples are checked against the
identifier convention by tokenizing the Python blocks: only `NAME` tokens are candidates, and
strings, attribute access after `.`, `name: Mapped[...]` annotations and keyword arguments of
`.values(...)` and of model constructors are column names. The Portuguese names left after that
check are columns, ORM classes and the loanwords `sandbox` and `staging`.

## The state of the code

`pyproject.toml` declares no runtime dependencies and pins the `dev` group (SQLAlchemy, duckdb-engine,
sqlalchemy-redshift and pandas were added on 2026-09-19 for the study suites; `prepare_offline.sh`
must be rerun). The reference model in `tests/model/` and its defects are
listed in `docs/CURRENT_STATE.md` under the repository; the `DEFERRABLE` and `SERIAL` behavior of
each dialect is in `docs/sqlalchemy.md` and `docs/duckdb.md`.

Test layout (user decision of 2026-09-19): `tests/` holds the package tests (`test_source_db_projetado.py`
over `source_db_projetado.py`, the fictitious source base of 2026-09-20), `tests/model/`,
`tests/test_probes.py` and `tests/conftest.py`; `tests/proof_of_concept/` holds the Delta proof of concept on
both storages, the study suites (commented step by step as learning material, listed per stage in
`docs/PLAN-STAGE-<n>.md`) and `test_redshift.py`, never run against a cluster. Files, authorization variables and
last-run counts: the `tests/` row of the repository table in `docs/CURRENT_STATE.md` and `README.md`. The 11 s
listing failure behind a silent proxy is in `README.md`; `test_delta_rs_credential_chain` runs five variants.

The suites exist so the same proof of concept runs in the target, without internet; the local suite
validates the prepared folder there (verified 2026-09-19: extracted at another path with dead proxies
and an empty `HOME`, 10 passed; on macOS after the glob fix of PR #12). `uv sync` ignores
`UV_VENV_RELOCATABLE` and the `.venv/bin/*` scripts keep absolute shebangs, hence
`.venv/bin/python -m pytest` and the `.tar.gz`, never zip, that keeps links and permissions.

## Environment of the measurements

Each document dates its measurements and pins their versions in its opening lines: `docs/parquet.md`,
`docs/duckdb.md` and `docs/sqlalchemy.md` on 2026-09-18, over 300,000 rows of `operacoes`
(`poc_delta.sample_table`), the Redshift statements compiled only; `docs/estrategia.md` on 2026-09-19;
the S3 proof of concept of 2026-09-19 (Python 3.13.15) in `docs/POC.md`. What they do not say: the
local proof of concept ran on macOS arm64 through `uv run --with` in the scratchpad, with 11 DuckDB
threads and the files in the page cache, nothing against S3 or Redshift; the Python examples added
to every document on 2026-09-19 ran under the same pinned versions.

## The SageMaker Unified Studio environment, as observed on 2026-09-19

- Domain `dzd-d8yrvx1ko7im6o`, project `eighth-experimentation` (`avhvbqn37ty7m8`), account
  892278726726, region `us-west-2`, space `my-code-v4` (Code Editor, 4 vCPUs). The `sagemaker_studio`
  package's `Project()` gives `iam_role`, `kms_key_arn`, `s3.root` and `connections`.
- Credentials: the container endpoint (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`; `boto3` reports
  `container-role`) assumes `datazone_usr_role_avhvbqn37ty7m8_5hkjdsy3umpi1c`. `~/.aws/config` has a
  `default` profile with `credential_source = EcsContainer` and a `DomainExecutionRoleCreds` profile.
- Project bucket `awsds-sandbox-smus-projects`, prefix `dzd-d8yrvx1ko7im6o/avhvbqn37ty7m8/`: `dev/`
  is the working area (`s3.root`), `shared/` is the s3fs mount at `$HOME/shared`. The role cannot
  `ListAllMyBuckets`. A second S3 connection, `sandbox-lake.s3`, points at
  `s3://awsds-sandbox-lake/sso-group-data-scientists/` through S3 Access Grants.
- Connections: S3, Athena, Glue Spark, Spark Connect, Lakehouse and workflows. No Redshift
  connection, cluster or serverless workgroup.
- Network: outbound HTTP goes through `proxy.awsds.internal:3128`; `no_proxy` is always set;
  `NO_PROXY` equals it in a Code Editor terminal and is empty in a Claude Code extension shell. `uv` reaches PyPI through the proxy but downloads Python only with
  `UV_PYTHON_DOWNLOADS=automatic`; `uv sync` needs it to fetch Python 3.13, and `uv run` warns that `VIRTUAL_ENV=/opt/conda` is ignored (harmless). System
  Python is 3.12.13 with boto3, awswrangler, deltalake 1.5.0, DuckDB 1.5.4, PyArrow 21.0.0 and
  redshift_connector 2.1.10 preinstalled.
- `gh`, installed and authenticated as the user on 2026-09-19, pushes over HTTPS; the 03:23 UTC
  probe run of 2026-09-20 found no `gh` on the PATH and the 03:44 and 04:40 runs found `/usr/bin/gh`
  (with `/usr/local/bin/aws` and no `duckdb` CLI), so check for it before relying on it.
- Probe readings of 2026-09-20 in the same space (four runs, the last at 04:40 UTC) are recorded in
  `docs/POC.md`, which also holds the lab-only readings (IMDS, the `~/shared` mount, Athena, the
  container credential's lifetime). This lab is not the target: the target has Redshift and no
  internet (user statement of 2026-09-20).
