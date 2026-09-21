# What the user stated and decided

Read before planning or implementing any stage, and whenever a "why" question comes up: these are facts stated by the user, not visible in the code, with their dates. The plan (`plan/PLAN.md`, pt-BR) records the decisions it rests on; this file keeps the statements behind them.

## The pipeline outside this repository

Facts stated by the user, not visible in the code: the pipeline is mostly Python logic; SQLAlchemy
is used only for the declarative models (DDL) and for Core `select` and `insert` statements that
move tables, never for ORM instances; no catalog service is enabled, which excludes Iceberg on
Glue (Iceberg with a SQLite catalog file moved by the library is the documented alternative if Glue
or S3 Tables may be enabled later); development and production runs write separate tables; renaming
or dropping columns is rare. The decision in `plan/PLAN.md`, with the rationale in `plan/estrategia.md`, follows from them: Delta Lake
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
which the probes of that day measured for the table and for the batch (`plan/PLAN.md`, section "A
troca de dados com o código cliente"). The same day the user moved the
models to `tests/model/` as the reference model: the tests hand it to the package API as a client
library would, and the package holds no model. On 2026-09-21 the user renamed it
`tests/reference_model/` and stated that it is the SQLAlchemy model of the original partitioned
Parquet base and that its files will not be changed: the corrections stage 1 planned go to a copy,
the client model, in `tests/client_model/` (proposed, awaiting confirmation), and the reference
model stays as the model whose defects `check_models` lists. The same day the user ran
`probes/parquet_source.py` in the target on the production base and committed the report to
`plan/readings/` (unlike the five probe reports of that day, kept in `secrets/`). Later that day
the user confirmed `tests/client_model/` as the corrected copy (it represents the data model the
client code presents to use the library) and accepted the path: the client model, then
`serialize_db.schema`, then `scripts/migrate_parquet_to_delta.py` (the stage 7 draft on stage 1,
`deltalake` and DuckDB), tested on the fixture and run in the target on a copy of the production
base the user can create there, before stages 2 to 6, with stage 7 absorbing the script; the
migration script lives in `scripts/`. After the source base was read (2026-09-20) the user
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
`COPY ... (RETURN_STATS)`) after the checks of `plan/PLAN-STAGE-3.md`, `"rewrite"` writes by
`write_deltalake`; the `cad_lancamentos` measurement decides the default, `"register"` until then.

## The test layout

Test layout (user decision of 2026-09-19): `tests/` holds the package tests (`test_source_db_projetado.py`
over `source_db_projetado.py`, the fictitious source base of 2026-09-20), `tests/reference_model/`
with `tests/lib_base_contabil.py` and `tests/lib_base_gerencial.py` beside it (the pipeline's module
names the model imports) and `tests/test_reference_model.py`, `tests/test_probes.py` and
`tests/conftest.py`; `tests/proof_of_concept/` holds the Delta proof of concept on
both storages, the study suites (commented step by step as learning material, listed per stage in
`plan/PLAN-STAGE-<n>.md`) and `test_redshift.py`, never run against a cluster. Files, authorization variables and
last-run counts: the `tests/` row of the repository table in `plan/CURRENT_STATE.md` and `README.md`. The 11 s
listing failure behind a silent proxy is in `README.md`; `test_delta_rs_credential_chain` runs five variants.

## The target's `USE` and the probe reports of 2026-09-21

On 2026-09-21 the user ran the five probes in the target and saved the reports in
`secrets/probes-aws-bn/`, asking for them to be read and propagated to the plan: the exception to the
rule that `secrets/` is never read, limited to that path; the facts are in `plan/POC.md` and the
reports stay outside git until the user decides to copy them to `plan/readings/`
(`plan/OPEN_QUESTIONS.md`). The same day the user confirmed the reading of `RS-19`: `USE
datalake_rw_shared` makes two-part names resolve in the datashare while `current_database()` keeps
answering `dev`, so the switch is confirmed by resolving a name, never by that function
(`plan/PLAN-STAGE-5.md`, `plan/redshift.md`).

## The stage 1 review

On 2026-09-21 the user accepted two proposals of the stage 1 review (`plan/PLAN-STAGE-1.md`): a
`String` without length is a `check_models` violation (`String(n)` or `Text`), and `ddl` generates
the `CREATE TABLE` text from the type table without the SQLAlchemy dialects (`sql_type`, `quoted`,
`column_ddl`; no `@compiles` hook; `duckdb-engine` and `sqlalchemy-redshift` stay in the `dev`
group). The second was accepted after two concerns were answered: a model change needs no `ALTER
TABLE` support beyond what stage 8 already plans, because the Delta reconciles by the delta-rs API
(stage 3), the sandboxes are recreated from the current DDL every execution, and the published
tables get `ALTER TABLE ADD COLUMN` as text under either design, since SQLAlchemy Core has no
`ALTER` (that was Alembic's, which left); and the dialects stay for the statements (`sql.render`,
stage 2), so the decision only keeps stage 1 and the migration script free of them, and stage 2
decides whether `render` runs at development or at runtime. A `String(n)` width change reaches only
the versioned `.redshift.sql`, never the Delta diff (`plan/PLAN-STAGE-8.md`, pending decision).

## The repository layout of 2026-09-21

On 2026-09-21 the user asked, with the stage 1 implementation: `docs/` renamed to `plan/` (the plan,
the readings and the study documents), a new `docs/` with the package documentation for `pdoc`
(a main page with how the package works, a tutorial and the type mapping table that was in
`schema.md`), a GitHub workflow that runs only the package tests (never the proofs of concept, the
examples or the probes; DuckDB and local files, no S3), a GitHub workflow that publishes the `pdoc`
site, and a README whose test and probe sections hold only the final commands, split into the
package tests of the GitHub workflow and the AWS tests with a flag for Redshift, with the prose on
what each test does moved to the header of each test file. `plan/CURRENT_STATE.md`, `README.md`

Later on 2026-09-21, after merging PR #46, the user removed the corporate index from
`pyproject.toml` (the `UV_CONFIG_FILE` workaround left the workflows), took `tests/test_probes.py`
out of the GitHub tests workflow, and asked for a README instruction that builds the static HTML
documentation into a folder the user names. The docs workflow fails until the user enables Pages
with the source "GitHub Actions". `plan/CURRENT_STATE.md`, `README.md`
