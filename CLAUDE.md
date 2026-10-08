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

- Since the programs using this package will run on scalable AWS compute of our choosing, let's optimize for parallel processing.

## Language convention (important)

All prose in this repo is **Brazilian Portuguese (pt-BR)**: README files, code comments, docstrings, printed output, test messages, and shell-script comments. When editing or adding content, keep writing in pt-BR to match.

Database identifiers (schema names, table names, column names) are in Brazilian Portuguese (pt-BR).

Code identifiers (variable and function names) are in English.

This `CLAUDE.md` is in English, for AI-assistant tooling.

## `REFERENCES.md`

Record in this file every website you visit when searching for information.

## The documents that track the work

`.claude/memory/OPEN_QUESTIONS.md` is the only planning document the work keeps updated: it holds
the pending items, the doubts and the decisions waiting on the user, in pt-BR. Add an item when the
work finds a question it cannot close; remove it when the answer lands where it stays (the
docstring or test of the code it decides, `.claude/memory/decisions.md` or a theme file of
`.claude/memory/`), and name that place in the report. The repository has no `plan/` folder.

A new measurement goes, with its date, to the docstring or test of the code it justifies, to the
PR description and to its theme file in `.claude/memory/`. The plan that built the library, its
stage files and the measurements up to 2026-10-05 are in the Claude project's library, `plan/`
("Repository index"): `OPEN_QUESTIONS.md` and the dated entries of the memory cite them as
`POC.md`, `estrategia.md`, `plan/<file>` or "etapa N" (`PLAN-STAGE-N.md`), and a new text names
the code, the test or the memory file that holds the fact instead.

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

## Python Code Style

### Core principle

- When writing Python code, use [PEP 8 Style](https://peps.python.org/pep-0008/).
- Use docstrings to explain function interface and purpose.
- Add code examples when writing docstrings for public interface.
- Add comments for each logical block of code, explaining what you're doing.

IMPORTANT: prioritize code that is easy for humans to read and review over short or "clever" code.
Test: would a junior data scientist on the team understand this snippet in a single read? If not, simplify.

### Simplicity
- Write the minimum code that solves the requested problem. Nothing speculative.
- Do not add abstractions, classes, parameters, or configuration that were not requested.
- Prefer functions over classes. Use classes only when there is real state; prefer `dataclass` over inheritance hierarchies.
- Do not handle errors for scenarios that cannot happen.
- If the solution got long, look for a more direct version before delivering, or break it into subfunctions.

### Structure
- Short functions with a single responsibility and a name that describes what they do.
- Use early returns instead of nested `if`s; at most 2 levels of nesting.
- Break long expressions into intermediate variables with descriptive names.
- Prefer separate functions over one generic function controlled by boolean flags.
- Do not use bare `except:` or generic `except Exception` without re-raising.
- A function with at least one parameter declares one parameter per line, with a trailing comma, and
  `) -> T:` on its own line; `ruff format` keeps the layout through the trailing comma. `self`
  and `cls` do not count: a method with only `self` or `cls` stays on one line, like a function
  without parameters (user decisions of 2026-10-01).

### Constructs to avoid
- Comprehensions with more than one `for` or a complex condition → use an explicit loop.
- `lambda` beyond trivial expressions → use a named `def`.
- Nested ternaries → use `if/elif/else`.
- Walrus operator (`:=`), `functools.reduce`, `map`/`filter` with lambda.
- Metaprogramming: metaclasses, custom decorators, dynamic `getattr`/`setattr`, monkey-patching.
- `from module import *`.
- One-liners that do several things at once.

### Names, types, and comments
- Descriptive names instead of abbreviations (`balance_by_account`, not `bba` or `df2`).
- Type hints on all function signatures.
- Short docstring on public functions: what it does, inputs, and output.
- Comments explain *why*, not *what*. Do not comment the obvious.

### Docstrings

The standard every public function, method and class follows (user decision of 2026-09-24), which
`pdoc` renders with `--docformat restructuredtext`:

- **Layout.** A summary line saying what the function does or returns, the behavior as prose, an
  `Exemplo:` block opened by `.. code-block:: python` (or `shell`) on the public interface, and the
  fields last, after a blank line.
- **Fields.** `:param name:` for every parameter pdoc shows, in signature order, except `self` and
  `cls` (`*parts` is `:param parts:`); `:return:` when the annotation is not `None`, and for a
  context manager what `with` gives; one `:raises Exc:` per exception the function raises
  deliberately, directly or through the package's own helpers, named as the module imports it. A
  third-party exception that only propagates stays out, unless the reader acts on it (`duckdb.Error`
  in `import_table`, `Storage.duckdb_connect` and `DuckDBEngine`, `redshift_connector.Error` in
  `RedshiftEngine.execute` and `create_publications_table`). pdoc reads only `param`, `return` and
  `raises`, and ignores `:returns:` and `:raise:`.
- **Field text.** pt-BR, starting lowercase and ending with a period: what the argument is, its
  unit, its accepted values and what `None` or the default means; continuation lines indent four
  spaces. The first line of a `:param` or `:raises` field holds no other colon, a quoted `s3://` or
  `:memory:` included, because pdoc 16 reads the name up to the last colon of that line.
- **One place per fact.** A sentence about one argument, the return or one exception lives in its
  field, not in the prose too; the prose keeps what the function does as a whole, and a moved
  sentence keeps every identifier, number and date.
- **Constructors and attributes.** A dataclass documents its constructor by one docstring per field,
  a string right after the field; a regular class documents its arguments in the `__init__`
  docstring, and each public `self.x` gets a docstring right after the assignment. A
  `typing.Protocol` says in its class docstring that the `(*args, **kwargs)` pdoc shows is the
  `__init__` of `typing.Protocol`.
- **Repeated interfaces.** The methods of `Engine` carry the full field list again in `DuckDBEngine`
  and `RedshiftEngine`, with the same wording for the shared meaning and each engine's own detail.
- **Model-level errors.** A `ContractError` that only a model outside the contract raises is listed
  where the function derives text or a schema from the model (`serialize_db.schema`,
  `published_ddl`, `publication_statements`, `reconcile_published`, `partition_query`), and left out
  of the functions that orchestrate, because `check_models` refuses such a model first.
- **Checks.** Every line stays within 100 characters, and the built site is read as the working
  rules ask: a misread field shows as a bold name holding a colon, which
  `grep -o '<strong>[^<]*:[^<]*</strong>'` over the built HTML finds.

### Data (pandas / SQL)
- Prefer vectorized operations over loops and `.apply()`.
- Never use `inplace=True`; reassign the result (`df = df.dropna()`).
- Method chaining: one method per line, wrapped in parentheses, up to ~5 steps; beyond that, split into steps with named variables.
- Use `.loc` with explicit column names; avoid `.iloc` without justification.
- In SQL, use CTEs with descriptive names instead of nested subqueries.

### When editing existing code
- Follow the style of the surrounding code, even if you would prefer another.
- Change only what the task requires; do not refactor unrelated code.
- If you notice improvements outside the scope, mention them instead of changing them.

## `secrets/`

Never read the contents of the `secrets` folder.

# Claude Memory

Use this section to store your memory for this project. Keep this file within a size budget of 75 KB.

## How this memory is organized

This file is loaded whole into every session, so it holds what every session needs: the index of the
repository and of the memory files, the working rules, the naming conventions and where the work stands.
The facts, the stories and the pending items live in `.claude/memory/`, one file per theme, read on
demand with the Read tool and never imported with `@`, which would load them all. Each theme file
opens with when to read it, and a fact that a file of the repository details ends with that file.
A fact found in a session is appended to its theme file; this file changes when a rule, a
convention, an index entry or the state of the work changes. The budget above is never a reason to
drop a fact: what does not fit here goes to a theme file.

The stage numbers of the plan name the parts of the package it built, and the dated entries of the
memory and of `OPEN_QUESTIONS.md` still use them: 0 is the proof of concept
(`tests/proof_of_concept/`), 1 `schema`, 2 `sql`, 3 `storage` and `delta`, 4 `audit`, the `engine`
protocol and `engine.duckdb`, 5 `engine.redshift`, 6 `execution`, 7 `parquet_import`, 8
`publication`, 9 the operation subcommands of `cli` (`snapshot`, `vacuum`, `compact`, `archive`,
`export`, `history`) with `docs/operacao.md`, and 10 `reader` and the snapshot channels.

| Memory file | Read it before |
| --- | --- |
| `.claude/memory/decisions.md` | Changing an interface or a behavior, or answering a "why": what the user stated and decided, with dates (the pipeline outside this repo, the batch boundary, the partition unit, the Redshift target, `export_mode`, the test layout, the read access). |
| `.claude/memory/OPEN_QUESTIONS.md` | Planning a session, or when the work finds a question it cannot close: what has no answer yet (pt-BR), one item per pending question, with the run or the decision that will close it; a closed item leaves the file when its answer lands in the owning document. |
| `.claude/memory/lessons.md` | Adding a lesson, or when the reason behind a working rule matters: the dated stories. |
| `.claude/memory/delta.md` | Code on `serialize_db.delta` or the `deltalake` package: delta-rs behavior, `create_write_transaction`, schema evolution, conflicts, vacuum and log retention, performance, the alternatives assessed. |
| `.claude/memory/duckdb.md` | Code on `engine.duckdb`, `storage.duckdb_setup` or a probe that opens DuckDB: Arrow in and out, `COPY`, proxy, Python API. |
| `.claude/memory/redshift.md` | Code on `engine.redshift`, the publication, the Redshift suite or `probes/redshift.py`: the target, the datashare rules, `COPY` and `UNLOAD`, the driver, the readings of 2026-09-20. |
| `.claude/memory/sqlalchemy.md` | `serialize_db.schema`, `serialize_db.sql`, a DDL rule per dialect or generated SQL text: dialects, compilation, `Numeric`, SQLGlot. |
| `.claude/memory/parquet-arrow-types.md` | `cast`, the schema mapping or a Parquet footer check: what each writer produces, the type contract, PyArrow casts and pandas conversions. |
| `.claude/memory/aws-s3.md` | `serialize_db.storage`, the S3 suite or a probe that reaches AWS: conditional put, IAM needs, credentials, region and proxy per client. |
| `.claude/memory/concurrency.md` | `stream`, `appender`, `max_workers`, any helper thread: the GIL, DB-API thread safety, the batch boundary measurements. |
| `.claude/memory/source-base.md` | `serialize_db.parquet_import`, `tests/source_db_projetado.py` or `tests/reference_model/`: the dsv base read on 2026-09-20, the production base read on 2026-09-21, the fixture and the reference model against them, and the early migration's reports from the target, kept only here. |
| `.claude/memory/environments.md` | Running in the SageMaker space or the target, preparing the offline folder, dating a measurement: the lab, the target, the venv, the environments of the measurements. |

## Repository index

Read the file listed here before researching its subject again. Each document names the pages it
came from, and `REFERENCES.md` collects every URL consulted so far, grouped by subject: reference
projects, Parquet format, Redshift, DuckDB, PyArrow, SQLAlchemy, pandas, PostgreSQL, PyIceberg, Glue
Data Catalog, Athena, Lake Formation, SageMaker Unified Studio, S3, the Python standard library,
Linux and EC2, Windows, Python packages, Delta Lake, DuckLake, Hudi, Apache Hive, SQL tooling
(SQLGlot, SQLMesh, dbt, Ibis, dlt), data-contract tools, Rust/PyO3, the sources list and consulted
repositories. New research appends to the matching group.

| File | Subject |
| --- | --- |
| `README.md` | `uv sync` (with the `dev` group by default), the `pdoc` build, and only the commands: the package tests (the GitHub workflow's command) and the Ruff check (`uv run ruff check`, `uv run ruff format --check`), the stand-in run of the target-only suites (`SERIALIZE_DB_TEST_EMULATOR`, `uv run --group emulator`, local only), the AWS tests (`SERIALIZE_DB_TEST_REDSHIFT_SCHEMA` switches the Redshift suite on, `-m "not redshift"` off), the variables table, the probe commands and the migration script's commands; the two workflow badges open it. What each test does lives in the header of its file, and the suites' writes, permissions and report in the header of `tests/conftest.py`. |
| `docs/` | The package documentation for `pdoc`: `docs/index.md` is the main page, included by the docstring of `src/serialize_db/__init__.py`, with the client's installation, how the package works, the tutorial, the threads (the threaded APIs, their gain over serial measured on 2026-10-04, the usage rules), the retention of removed files (the 400 days, the versioned bucket's `NoncurrentVersionExpiration`, how to change them) and the type mapping table; `docs/operacao.md` is the operation runbook followed by the options of each `serialize-db` subcommand, included by the docstring of `src/serialize_db/cli.py`; `uv run pdoc serialize_db --docformat restructuredtext -o site` builds it. Docstring examples open with `.. code-block:: python` (or `shell`), the only form pdoc highlights, and the docstring standard is "Python Code Style", section "Docstrings". `docs/tecnologias.md` is the technology review, included by the docstring of `src/serialize_db/technology_review.py`: one section per technology, Parquet, Delta Lake, DuckDB, Redshift and SQLAlchemy, with how each works, what the library does with it, the dated measurements and the readings of the target. |
| `.github/` | `tests.yml`, on Ubuntu and Windows, imports every module after `uv sync --no-dev`, then runs `ruff check` and `ruff format --check`, installs the DuckDB `delta` and `httpfs` extensions into `.duckdb/` and runs `tests/` without `tests/proof_of_concept/` and `tests/test_probes.py` on push to `main`, on pull request and by hand; `docs.yml` publishes the `pdoc` site to <https://felipenoris.github.io/serialize-db/> on push to `main` (the repository's Pages source must be "GitHub Actions"). |
| `prepare_offline.sh` | Makes the project folder self-contained for the target without internet: managed Python in `.python/`, every `pyproject.toml` group and extra in `.venv/` (`uv sync --all-groups --all-extras`), DuckDB extensions in `.duckdb/`, all links relative. **Rerun it whenever a dependency is added**; a new DuckDB extension, a Python version change or another runtime asset is added by hand. Only a folder prepared on Linux x86_64 serves the SageMaker space; the header is the operating procedure, and the extensions block configures the DuckDB proxy through `probelib.duckdb_proxy`. |
| `probes/` | Read-only scripts that photograph the environment (`.venv/bin/python probes/<script>.py`; the report also lands in `probes/output/`, ignored by git, for pasting into the conversation). `probes/README.md` indexes them, details every check and fixes a probe's structure: `space.py`, `bucket.py`, `diagnose_aws.py`, `redshift.py` (`RS-1` to `RS-19`; the connection repeats `target_env_examples/redshift_native.py`), `catalog.py`, `parquet_source.py` (`--sample N`), `duckdb_threads.py` (the `threads` of the DuckDB engine ingesting the migrated Delta tables, each configuration in a new process with the external file cache off) and `credentials.py` (the library's clients held past the credential's expiry, about an hour), over `probelib.py` (`duckdb_proxy`, `hide_credentials`, `report.last_reason`; DNS, TCP and internet results are readings, never failed calls). `tests/test_probes.py` covers the pure helpers with fabricated responses, with no network but one DNS lookup; its cases that write a report or fabricated files are `local`. `consistencia/` holds consistency probes of 2026-09-25 and `probe_append_test.py` (2026-09-28), under `SERIALIZE_DB_TEST_LOCAL_ROOT` (pytest ones in `poc<id>`), and `operacao/` (2026-09-30) runs `serialize-db` routines over the source's first partitions under a suite root, `probe_parallel_gain.py` (2026-10-04) times the threaded APIs against serial, and `probe_published_base.py` (2026-10-08) reads the join `EXPLAIN` and the revert to an older snapshot on the published base itself, which it writes; commands in `SUITE.md` and `probes/README.md`. |
| `scripts/` | `migrate_parquet_to_delta.py`, the operator's tool for the initial load, thin over `serialize_db.parquet_import`: per table in `import_order`, `import_table` partition by partition with rows, time and the process's peak RSS printed, `import_report` and `entries_outside_the_model`; `--report` writes the JSON with the machine, the versions, the DuckDB limits read from the environment and the arguments, rewritten after each committed partition with the current table in `in_progress`; `--environment` names the folder under the root (`<root>/<environment>/<table>`); local folder or `s3://`. The commands are in `README.md`, the behavior in the script's header, the tests in `tests/test_migrate_parquet_to_delta.py`. |
| `src/serialize_db/` | The package: `errors.py`, `schema.py`, `sql.py`, `storage.py` and `delta.py` (`history`, `archive_snapshot`, the copy-and-register `deep_copy`, `set_channel`, `channel_snapshot`, `snapshot_versions` and the protected `channels_pointing`), `audit.py`, `engine/__init__.py` (the `Engine` protocol and the protected helpers both engines use), `engine/duckdb.py` and `resources.py` (the CPUs and memory the process may use, cgroup-aware, behind `environment_limits`, and `peak_rss_mb`, the process's peak RSS the script and the routines print), `engine/redshift.py` (`RedshiftConfig`, `RedshiftEngine`, `sandbox_prefix`, `schema_from_row_description`, `mask`, and the protected `connect`, `credentials_clause`, `staging_ddl`, `insert_from_staging`, `copy_text`, `unload_text` and error readers the publication reuses; `driver_connect` is the seam the tests replace with the stand-in), `execution.py` (`Database`, with `open_delta` and `open_redshift`, and `Execution`), `reader.py` (`DeltaReader`, `RedshiftReader`, `open_redshift`), `parquet_import.py` (`discover_partitions`, `partition_query`, `import_table` on a DuckDB engine per call with the partition check before the `COPY ... RETURN_STATS` and `register_files`, `import_report`, `import_order`, `check_requested_partitions`, the protected `entries_outside_the_model` and `split_ignored_partitions`), `publication.py` (the control table, the per-table transaction, unpublish, `reconcile_published`, `publication_status`), `technology_review.py` (the docstring-only module that includes `docs/tecnologias.md`), `_pool.py` (private: the per-table task pool of `execution`, `publication` and `reader`), `_files.py` (private: writing and diffing the generated files of `schema` and `sql`) and `cli.py` (`serialize-db schema\|sql write\|check`, `run`, `audit`, `import`, `publish_redshift` by `--snapshot` or `--channel`, `snapshot`, `vacuum`, `compact`, `archive`, `export`, `history` and `channel`, only `main` public). What each does is in the docstrings; `pyproject.toml` pins the runtime dependencies, all the public API uses (`redshift-connector==2.1.17` included, imported only when a Redshift path enters), and the groups, which never reach a client, and holds `[tool.ruff]`. |
| `tests/reference_model/` | The reference model: the SQLAlchemy model of the original partitioned Parquet base, kept as it is; it matches both readings of the source base (`tests/test_reference_model.py`, with `tests/lib_base_contabil.py` and `tests/lib_base_gerencial.py` standing in for the pipeline's modules it imports). The corrected copy is the client model in `tests/client_model/`. |
| `tests/client_model/` | The client model: the corrected copy of `tests/reference_model/` that the tests hand to the package API as a client library would, the corrections listed in its `__init__.py` and checked by `tests/test_client_model.py`; `statements.py` holds the fictitious pipeline's Core statements (`STATEMENTS`), `schema/` and `sql/` the generated files. |
| `tests/emulator.py` | The local stand-in of S3 and Redshift for the target-only suites: with `SERIALIZE_DB_TEST_EMULATOR`, `tests/conftest.py` starts the moto server (`moto[s3]` and `flask` in the `emulator` group, out of `dev`) as a subprocess before collection, points the AWS variables and the suites' roots at it, and gives `connect_redshift` a fake `redshift_connector` connection over an in-memory DuckDB that translates the suites' Redshift SQL and imitates the refusals and behaviors read in the target (the backslash escape in literals, the empty `UNLOAD` writing nothing, `pg_last_unload_count()`, `is_valid_json` refusing `SUPER`, `ALTER COLUMN ... TYPE` refused on the share, the missing relation as `XX000` with the target's `Relation <name> does not exist in the database.` and the existing one as `42P07`, DuckDB's transaction conflict as the `1023` message, `svv_all_columns` from the remembered DDL in Redshift's spelling); `SERIALIZE_DB_TEST_EMULATOR_FAIL_SQL` and `SERIALIZE_DB_TEST_EMULATOR_NO_MANIFEST` provoke failures. It checks the tests' code, not the target's behavior; the command is in `README.md`. `conftest.redshift_config()` and the `redshift_driver` fixture give the engine, publication and reader suites (`tests/test_engine_redshift.py`, `tests/test_publication.py`, `tests/test_reader.py`, over the model of `tests/lancamentos_model.py`) the stand-in connection through `driver_connect`. |
| `tests/source_db_projetado.py` | The fictitious Parquet source base `db_projetado`, reproducing the structure `probes/parquet_source.py` read in the dsv base and in the production base (`.claude/memory/source-base.md`), checked by `tests/test_source_db_projetado.py` (all `local`); the material of `tests/test_parquet_import.py`, `tests/test_migrate_parquet_to_delta.py` and `tests/test_pipeline.py`. It also holds the reference model's keys (`UNIQUE_KEYS`, `FOREIGN_KEYS`, `MODEL_NOT_NULL_DECLARED_NULLABLE`), which `tests/test_reference_model.py` checks against the model and the fixture satisfies. |

The probe and suite reports a pending item still consults are in the Claude project's library,
`/mnt/project-files/readings/`, indexed by its `README.md`; `.claude/memory/decisions.md`, "The
readings folder", lists them.

The scripts the user ran in the target are in the library's `target_env_examples/` (git history
keeps them in `examples/`), kept as run but for the masked identifiers in the bucket path:
`redshift_native.py`, `redshift_data_api.py`, `redshift_copy_unload.py` (2026-09-20) and
`redshift_manifest.py` (2026-09-21, the prerequisites of `export_partition`); its `README.md` says
what each one fixes. The probe, the Redshift suite and `engine.redshift` repeat their calls, and
the target they fix is in `.claude/memory/redshift.md`.

The plan's documents, frozen on 2026-10-05, are in the library's `plan/`
(`/mnt/project-files/plan/`, indexed by its `README.md`; git history keeps them up to `c97d658`):
`PLAN.md`, `PLAN-STAGE-0.md` to `PLAN-STAGE-10.md`, `CURRENT_STATE.md`, `POC.md` (what each run
showed, with the date of each measurement), `estrategia.md` (the premises and the comparisons
behind each layer), `guia.md`, `schema.md` and `serialize-db.md`. Only the project's cloud sessions
read the library; elsewhere, git history has the same files.

The DuckDB, Redshift and Delta Lake sections of `docs/tecnologias.md` share a section order: data
organization and the differences from PostgreSQL, supported types with `DECIMAL` and JSON, DDL,
`SELECT`/`INSERT`/`UPDATE`/`DELETE`, ingestion, export to Parquet, performance recommendations,
SQLAlchemy support, references. The Delta Lake section adds schema evolution, transactions, the
pipeline, DuckDB and Redshift access, and relocation.

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
  any install (`probes/space.py` SP-9), restore with `uv sync --all-groups`, because `--group dev`
  removes the `docs`, `emulator` and `interactive` groups another session in the folder may use
  (2026-09-19, 2026-09-23).
- **The pytest layout has no `__init__.py`**: `tests/conftest.py` is imported as `conftest` and its folder
  lands on `sys.path`, so `from conftest import ...` and `from poc_delta import ...` work inside
  `tests/proof_of_concept/`; `pythonpath = ["scripts", "probes", "probes/operacao"]` in
  `pyproject.toml` puts the migration script, the probes and the operation probes' library on
  `sys.path` too, so module basenames stay unique across `tests/`, `tests/proof_of_concept/`,
  `scripts/`, `probes/` and `probes/operacao/`.
- **Before a commit**: `uv run pytest` with no variables and again with `SERIALIZE_DB_TEST_LOCAL_ROOT` set
  to the scratchpad, both green and both with the `AWS_*` variables removed (`env -u AWS_ACCESS_KEY_ID
  -u AWS_SECRET_ACCESS_KEY AWS_EC2_METADATA_DISABLED=true`), because this container exports a
  credential and the GitHub runner has none (2026-09-24); before a push that touches what the target-only suites cover
  (`storage` and `delta` on S3, `audit`, `engine.redshift`, `publication` and `reader` on Redshift,
  `tests/conftest.py`, `tests/emulator.py`, the suites themselves), the target-only suites on the
  stand-in with the local root,
  `SERIALIZE_DB_TEST_EMULATOR=1 SERIALIZE_DB_TEST_LOCAL_ROOT=... uv run --group emulator pytest ...`
  as in `README.md`, which the GitHub workflow does not run (2026-09-23);
  `py_compile` on an edited probe; `uv run ruff check` and `uv run ruff format --check` clean,
  since the whole repository follows `ruff format` (2026-10-01); every `](...md)`
  link target checked;
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
  other in `.claude/memory/OPEN_QUESTIONS.md`, grep the repository before calling a question open,
  and grep tables and lists when a fact changes (2026-09-20).
- **A probe's verdict is a hypothesis until the environment answers, and one repetition separates the
  transient from the permanent**: read a value the reading cannot produce as unread, treat an expected
  absence as a reading (`Report.call(expected=True)`), stop the section when the connection dies, run the
  probe twice before writing a consequence into a document (2026-09-20).
- **The environment's sensitive identifiers are masked in every file but `SUITE.md`**: the
  accounts (`<conta>`, `<conta do produtor>`), the users and personal folders (`<usuário>`), the
  role id (`<id do papel>`) and the SSO suffix (`<id>`), the DataZone ids (`dzd-<domínio>`,
  `<projeto>`, `<ambiente>`), the Glue database id (`glue_db_<id>`), the KMS key (`<chave>`), the
  producer namespace (`<namespace>`) and the private addresses (`10.x.x.x`); the lab's account and
  DataZone ids are `<conta do laboratório>`, `dzd-<domínio do laboratório>`,
  `<projeto do laboratório>` and `<ambiente do laboratório>`; a reading, a memory fact or an
  example is masked before it enters git, and git history keeps the old values (2026-09-23).
- **A script the user ran in the target outranks a design written without one**: keep it verbatim in
  the library's `target_env_examples/`, with its literal values but the masked identifiers, and make
  the probe and the suite repeat its calls, the joins of URIs included: `DeltaTable.table_uri` ends
  with a slash, the example's base string did not, and the `//` cost a run in the target
  (2026-09-20, 2026-09-21).
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
  the check enters the code (2026-09-21).
- **A memory that hits its ceiling is split, not trimmed**: a fact goes to its theme file in
  `.claude/memory/`, and this file changes only when a rule, a convention or an index entry changes
  (2026-09-21).
- **A state change is confirmed by the effect the caller depends on**, never by a system function
  believed to report it: `current_database()` stayed `dev` after a `USE` that made two-part names
  resolve, and the check built on it failed a working environment (2026-09-21).
- **`secrets/` stays unread unless the user names a path inside it**: the probe reports of
  2026-09-21 were read from `secrets/probes-aws-bn/` on request, only that folder, and nothing from
  them is copied into git; the facts go where a new measurement goes (2026-09-21).
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
- **A document revision reads every section against the decisions memory**: a sentence written
  before a decision survives in another section (the `pc.round` of the initial load, contradicting
  the `Double` decision of 2026-09-20, found only by the full review of 2026-09-21); grep the
  documents for the old rule's vocabulary after the decision lands, not while researching it, and
  read with it the tables that index names (a table kept `memory_limit` explícito and missed
  `SandboxError` in the exception list on 2026-09-22).
- **Every identifier the library emits is double-quoted, and a new model's names are read against
  `duckdb_keywords()` and the Redshift reserved list**: `to` and `timestamp` are client-model columns
  and reserved words, and the first `schema` draft never met them because its example model was safe
  (2026-09-21).
- **A draft calls every input kind its signature accepts**: `cast` promised three kinds and ran on
  one; the reader path hid `pc.all` returning null on an empty column (2026-09-21).
- **A generated file is generated twice and diffed before a test asserts it equal to its versioned
  copy**: delta-rs `Schema.to_json()` orders field metadata arbitrarily, and the first generation
  looked fine (2026-09-21).
- **`uv` resolution needs every configured index reachable**: the corporate index that sat in
  `pyproject.toml` failed outside the corporate network, and the user removed it on 2026-09-21; an
  index the workflows cannot reach stays out of the repository, and an empty `UV_CONFIG_FILE` is
  the way to ignore a `[tool.uv]` section when one must (2026-09-21).
- **A validated instrument is refactored against its own report**: run the probe over the fixture
  base (`tests/source_db_projetado.py` writes it to the scratchpad; `probes/parquet_source.py`
  takes a local folder) before and after, and diff the two reports; one that needs the target
  environment to run is not restructured, the item goes to `.claude/memory/OPEN_QUESTIONS.md`
  (2026-09-21).
- **A schema decision is read back through every reader the pipeline uses, with a file from each
  writer**: `parquet.field.id` in the Delta schema made `delta_scan` read every column as null for
  every writer, and only the migration script's report, the first `delta_scan` over a table created
  by `delta_schema`, found it (2026-09-21).
- **Control flow never rides on process-wide state**: `warnings.catch_warnings` swaps the
  interpreter's filter (Python 3.13, no `context_aware_warnings`) while the engines call `render`
  from helper threads; read the compiled statement's `binds` instead of catching the warning
  (2026-09-21).
- **A comparison between two designs runs both under the same conditions**: the first probe of
  the single session ran it on a file database against the cursor sketches in memory and read six
  times slower; on the same database it was faster. Time every alternative best of three, on the
  same database, data and batch size (2026-09-22).
- **A conversion rule is probed with every input type and parameter that reaches it**: `cast`
  measured text only on `string` input and missed the `large_string` of the pandas 3 `str`, and the
  integer detour `decimal128(p + 3, s)` held only for `Numeric(18, 2)`; measure on the converted
  column, and feed each input type the pandas paths produce (2026-09-22).
- **A verdict measured on a draft is measured again after a decision changes the draft's output**:
  the `sqlglot` `ParseError` on the sentinel was read on the bare `{prefix}cad_contas` of the
  `quote=False` draft, survived the `quote=True` decision of the same day into the documents, the
  decisions memory and a user decision, and the implementation found the quoted sentinel parses;
  grep the documents for a measurement's premise when a decision changes what a primitive emits
  (2026-09-22).
- **A library's warning is a reading, never a guard**: the valueless `bindparam` warns under
  `literal_binds` only in a `=` comparison and renders `NULL` silently in `LIKE`, `coalesce`,
  `VALUES`, a `select` column and `text()`; guard on the state the library exposes, `required` on
  each `BindParameter` of the statement (`compiled.binds` is empty under `literal_binds`, where a
  valueless `IN` list renders `IN (NULL)`, 2026-09-23), and probe every form the input takes before
  writing that a behavior warns (2026-09-22).
- **A requirement is measured in the user's own words before it is reported kept**: "the client
  works while the connection does I/O" was reported kept when only the spool file's reading
  overlapped the client, and the query ran whole before the first batch; time the overlap the
  requirement names, with the client work it names (first batch, total with work per batch), and
  report a weaker form as weaker (2026-09-23).
- **A primitive is measured in the documented usage, with every command its implementation runs**:
  the reference `Loader` wrote into tables the tests had created, and the DuckDB `loader`
  draft's `CREATE TABLE` at open waits for the whole query of a `stream` opened before it (first
  batch 0.811 s against 0.006 s); a sketch backs a requirement only after running every command
  the primitive's design lists, in the documented order (2026-09-23).
- **A claim that a type round-trips is probed with the type's special values**: `NaN`, the
  infinities, null, the empty and the longest value; the "exact" `Double` statistics lose the `NaN`
  row in `delta_scan`, and the audit's control total fails on it (2026-09-23).
- **A statement path is probed with every clause form the client writes**: the compile path met
  `IN` lists only after it was designed (`__[POSTCOMPILE_...]`), and `delta_scan` prunes by
  `=` and ranges but opens every file for a multi-value `IN`; read pruning through the files the
  engine opens (2026-09-23).
- **A concurrency test is repeated before it is trusted, and its failure paths are read**: the
  hybrid stream's orphan-file race showed in three of six runs and never in the first, and a
  `__del__` read a field a failed `__init__` never set; run such tests several times, read the
  warnings, and set every field a finalizer reads before the first line that can raise (2026-09-23).
- **A statistic is probed at every layer that prunes and in every row-group position, and a standard
  is read in its current text**: the first `NaN` probe read only the Delta log over one-row-group
  files, and missed DuckDB's Parquet reader pruning by the delta-rs and pyarrow footers and a
  `has_nan` that sees only the last row group; the question came from a 2018 Java ticket, while the
  spec changed in May 2026 (2026-09-23).
- **A statistic left out for safety is read through every reader of the format**: the registration's
  absent min and max only stop pruning in `delta_scan`, and the delta-rs dataset turns them into a
  guarantee that skips the file for any filter on the column (2026-09-25).
- **A path chosen by a quantity needs the quantity before the path runs**: the Redshift `stream`
  threshold between `fetchmany` and `UNLOAD` waited on a measurement, while the row count only
  existed after the `execute` that had already materialized the result; write down where the
  quantity is read before measuring a threshold (2026-09-23).
- **"Empty by construction" is read against the caller's loop and the rerun**: the `UNLOAD`
  destinations of `export_partition` were empty only for the first partition of an execution and
  its first attempt; end every write destination with a segment new per call (2026-09-23).
- **A checkout may be shared with another session**: read `git status -sb` and `git reflog -5`
  before creating a branch or committing, never the session's opening snapshot, because a branch
  switch moves every session in the folder; another session's open `claude/` PR is the open PR the
  git rule names; add files by path, and agree by message on the order of edits to files two
  sessions touch (2026-09-23).
- **A test only the target can run is first run against a local stand-in**: `tests/emulator.py`
  (`SERIALIZE_DB_TEST_EMULATOR`), DuckDB for Redshift and moto for S3, checks the test's own code
  before a target run is spent; the stand-in found a `NaN` case filtering `valor > 2`, which the
  footer's 3.0 maximum lets through, so the pruning loss it was written to show could never appear.
  A correction of a failure path is proved by provoking that failure in the stand-in, the old and
  the new code side by side: both ran green without it, and the old suites recorded a missing
  `UNLOAD` manifest as an empty result (2026-09-23).
- **The study suites keep only the external libraries' facts**: a draft of package code leaves
  them when its module lands, in the same unit of work, and its unique cases move to the package
  tests; a draft kept beside its module drifts and measures a setup the module never runs
  (2026-09-23).
- **Every behavior a docstring promises has an assertion that fails without it**: the abandoned
  loader's spool deletion was in the docstring and in its design, untested and false; run the new
  assertion against the old code before trusting it (2026-09-23).
- **A secret is masked where it leaves**: the report's printed lines and its JSON go through the
  mask, not the values that enter it, which may nest an error's `repr` in a dict (2026-09-23).
- **A hook that selects tests by suite reads the marker, never `item.keywords`**, which also holds
  every parameter id: a case with the id `redshift` was skipped as the Redshift suite (2026-09-23).
- **A test that runs a default which writes points the default at the authorized root first**:
  `DuckDBConfig()`'s `mkdtemp` wrote in the system temp folder from a `local` test (2026-09-23).
- **A compatibility the library claims is run against an instance of it**: `AWS_ENDPOINT_URL`
  reached boto3, PyArrow and delta-rs, while the DuckDB secret carried only the host and never
  reached an `http` or IP endpoint until the moto stand-in ran the package's `s3` tests
  (2026-09-23).
- **A memory reading in a child process reads the child's own peak and counts from its base**: on
  Linux a new process's `ru_maxrss` starts at its parent's peak, so read `VmHWM` from
  `/proc/self/status`, and compare what each scenario adds over the base after the imports, which
  changes with the platform (2026-09-23).
- **A difference the stand-in shows is read against the target's documentation before it is blamed
  on the stand-in**, and an output that can be empty is read with the command's own count: the
  `UNLOAD` literal escapes the backslash, a case matched no row, and the empty `UNLOAD` wrote no
  manifest, which the suite read as a failure (`pg_last_unload_count()` tells them apart,
  2026-09-23); and the stand-in reads an input as that documentation does: its `COPY` raised
  `NoSuchKey` on a missing key, the target read a key prefix and loaded nothing (2026-09-28).
- **A SQL predicate is probed on table rows as well as constants, and a function in generated SQL
  with the type the generated DDL gives its argument**: Redshift compared `NaN` as PostgreSQL on
  constants and as IEEE in a table scan, and refused `is_valid_json` on `SUPER` (2026-09-23).
- **A resource limit is read from the environment when the connection opens, never fixed in code
  nor left at a default sized to the whole machine**: the CPUs the process may use and half the
  memory still available (`environment_limits`), because DuckDB's 80% default let the kernel kill
  the `cad_lancamentos` migration; and a long-lived process closes its DuckDB connection per unit
  of work, because DuckDB returns memory only on `close` (2026-09-24).
- **The complement of a comparison with `NaN` is counted as total minus matches, never as the
  negation, and a count is also read without rows**: in a Redshift scan neither the strict
  infinity comparison nor its negation was true for `NaN`, and a `count(*)` of
  `sys_load_error_detail` returned no row and stopped a probe section (2026-09-24).
- **A repetition in the same process measures the caches the first run filled**: DuckDB's external
  file cache served the threads probe's second and third reads from memory; turn caches off or
  count requests at the source before a best of N stands for a remote read (2026-09-24).
- **A long-running script writes its report as it goes**: the migration process that ended in
  `cad_lancamentos` 2026-03-31 took the four-variant measurement with it (2026-09-24).
- **A credential renewal is designed per client that holds a copy**: delta-rs resolves the chain
  per call and `redshift_connector` reconnects per command, while a DuckDB secret stores the key
  at `CREATE SECRET`, and `REFRESH auto` renewed it only in `httpfs`, never in `delta_scan`; list
  every client that copies the credential, with the expiry the probe read beside the run's length
  (2026-09-24, 2026-09-25).
- **A test that uses a suite's fixture carries the suite's marker**: the publication's target
  cases reached `local_location` through their `target` fixture with only `redshift` and `s3`,
  so `pytest -m redshift` without the variable errors instead of skipping; `tests/conftest.py`
  refuses at collection a test whose fixture closure holds a suite fixture without its marker
  (`SUITE_FIXTURES`), and a new suite fixture enters that table (2026-09-24).
- **A catalog function's result type is outside the contract until a run reads it**:
  `current_database()` is `name` (OID 19), and its reading through the typed `query` failed
  the case before its last assertion; read a system function last in a case, or through the
  raw `row_desc` (2026-09-24).
- **A single-request S3 call on a large object is bounded by the SDK's low-speed limit, and a
  routine that copies many files is proved resumable**: pyarrow's `copy_file` is one
  `CopyObject`, which the AWS C++ SDK abandons after 3 s without a byte (`curlCode: 28`) while
  S3 copies server-side, so `Storage.copy` on S3 goes through boto3's managed transfer; and the
  new assertion run against the old code showed the rerun of `archive` moving the snapshot entry
  over a half-copied table (2026-09-24).
- **A routine the runbook asks the operator to size prints its own measure**: the first whole
  `archive` and the first publication of the base ran in the target with no duration and no
  memory reading, because the CLI printed none; `compact`, `archive`, `export` and the
  publication print the time and the peak RSS per table since (2026-09-24).
- **A documentation convention is checked in the rendered page as well as in the source**: build
  the pdoc site and read the fields it renders; pdoc 16 reads a `:param` or `:raises` name up to
  the last colon of the field's first line, and a script over the source missed it (2026-09-24).
- **A diff is split into commits from an empty index**: `git reset --soft` leaves the whole diff
  staged, and the next commit takes all of it; read `git diff --cached --stat` before each commit
  (2026-09-24).
- **A test double of a library function forwards `**options`**: before changing how a function is
  called, grep the tests for `monkeypatch.setattr(<module>, "<function>"` (2026-09-24).
- **One condition per `assert`**: `assert A and B` does not say which side failed, and an `or`
  beside an earlier assertion left the `--redshift` pipeline test without an engine check
  (2026-09-24).
- **A reading that runs statements on the connection it measures is part of the measurement**:
  the credentials probe's `fetchone()` left each `read_parquet` open, and the next statement
  rolled back the secret it had refreshed; consume every result, and when two instruments
  disagree, run variants that differ in one step (2026-09-25).
- **A test double calls the original through a reference saved before the swap, and a failure
  case matches its own message and asserts that its double ran**: `pytest.raises(Exception)`
  swallowed the `RecursionError` of a double that called itself and the `AttributeError` of one
  that never wrote its manifest, and a refusal case without `match` stayed green with its check
  removed (2026-09-28).
- **A double of a concurrent writer changes the bytes it writes**: the local fingerprint is the
  content's `sha256`, and a double that rewrote the same control file read as no change once
  `run.snapshot` read the file before the commits (2026-09-28).
- **A Parquet footer is read by each leaf's `path`, never its `name`**: the leaf of a column
  nested in a struct repeats the inner field's name, so `valor` inside `s` passed
  `register_files`, registered and read null (2026-10-01).

## Naming conventions

ORM model classes and column mixins (`Operacao`, `Lancamento`, `Rastreio`) keep Portuguese names:
they are data-model artifacts, like tables and columns. Python variables, functions, parameters,
modules, the API (`Database`, `Execution`, `ingest`, `audit`, `publish_delta`) and the keys of
`Table.info["serialize_db"]` (`partition_by`, `partition_source`, `sort_key`, `redshift`) are
English. The library's own metadata is English. Each module separates three levels of name: public,
the interface client code imports, which `pdoc` documents; protected, used by another module of the
library; private, used only inside its module. The module's `__all__` lists the public names and
only those, so a module with a protected name declares `__all__` and `pdoc` leaves the protected
one out; protected is unprefixed all the same, and private carries the `_` prefix. A package's
`__all__` also lists its public submodules, the only ones `pdoc` documents (`serialize_db.engine`
lists `duckdb` and `redshift`; `tests/test_package.py` checks every package). A key under
`_serialize_db/` carries no prefix (`snapshots` in `_serialize_db/snapshots.json`); everything else
the library writes carries the `serialize_db_` prefix: the commit keys `serialize_db_execution_id`,
`serialize_db_input_versions` and `serialize_db_snapshot`, and the Redshift control table
`serialize_db_publications(table_name, delta_version, execution_id, published_at)`, whose columns
stay unprefixed because the table name is the namespace. `snapshot` is an accepted loanword in
prose. The production environment is `prd` and the development one `dsv`, the names of the source
base's folders: examples, commands, docstrings, tests and the `--environment` default use them, and
the dated records of the runs before 2026-09-24 keep the `prod` and `dev` they ran with; `dev` stays
as the Redshift database of the connection and the dependency group. Every variable the project
requires starts with `SERIALIZE_DB_` (`SERIALIZE_DB_ROOT` and the library's own,
`SERIALIZE_DB_REDSHIFT_*`, `SERIALIZE_DB_TEST_*`, `SERIALIZE_DB_DUCKDB_EXTENSIONS`); the unprefixed
ones the probes and the suites read are third-party standards (`AWS_REGION`, `AWS_DEFAULT_REGION`,
`AWS_ENDPOINT_URL*`, `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, the proxy variables in both
spellings, and the proxy's `username` and `password`), read by the libraries that define them, and
renaming them would break those libraries. The S3 root a probe photographs comes from
`probelib.s3_root`: the argument, then `SERIALIZE_DB_ROOT`, then `SERIALIZE_DB_TEST_S3_ROOT`
(`S3_TMP_PATH` is the space's, not the project's, and nothing in the repository reads it). The
library reads no configuration file: `Database` and the command line take explicit arguments, with
the `SERIALIZE_DB_*` variables as their defaults, and the database root is always given, never
inferred from the space's project. Data tables and columns stay Portuguese, including the
`id_execucao` column of the `Rastreio` mixin in the SQLAlchemy section of `docs/tecnologias.md`.
SQL placeholders in prose (`COPY (consulta) TO ...`) and staging table names (`staging_<tabela>`)
count as database identifiers.

A Python example in the documentation is checked against the identifier convention by tokenizing
it: only `NAME` tokens are candidates, and strings, attribute access after `.`,
`name: Mapped[...]` annotations and keyword arguments of `.values(...)` and of model constructors
are column names. The Portuguese names left after that check are columns, ORM classes and the
loanwords `sandbox` and `staging`.

## Where the work stands

The plan's stages, 0 to 10, are implemented and merged: the package of "Repository index" is the
library, what each module does is in its docstrings and tests, `tests/test_pipeline.py` runs the
monthly pipeline end to end on DuckDB over the fixture base, and `docs/operacao.md` is the runbook.
What waits on the user, on a run in the target or on a later commit is in
`.claude/memory/OPEN_QUESTIONS.md`; the user's statements and decisions, with their dates, are in
`.claude/memory/decisions.md`, and the runs in the target are in `.claude/memory/environments.md`.
Two facts every change respects: the library's unit is the partition (`publish_partition`,
`partitions=`, `Execution(partition=...)`), a `String(n)` text column whose `AAAA-MM-DD` date is the
current base's case, never the month; and the Redshift target is `sbx_aco_decon` in the datashare
database `datalake_rw_shared`, reached by `USE` with the workgroup's temporary credential
(`.claude/memory/redshift.md`).
