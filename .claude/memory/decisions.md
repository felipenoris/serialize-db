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
`rel_contrato_operacao` whose `fator_rateio` sums to 1 per contract (per operation until the user's measurement of 2026-09-21 on the production base corrected the direction); `alembic_version` and
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

Still on 2026-09-21, the user asked for the two workflow badges in the `README.md` and for the
DuckDB DDL beside the Redshift one on the documentation's main page. The repository's Pages now has
the source "GitHub Actions", and the docs workflow's manual run of 18:24 UTC published
<https://felipenoris.github.io/serialize-db/>. The same day the user set the three levels of name
every module separates: public, the interface client code imports, which `pdoc` documents;
protected, not public interface but used by another module of the library; private, used only
inside its module. The private one carries the `_` prefix, the other two are unprefixed, and the
module's `__all__` lists the public names and only those, so `pdoc` documents the public interface
and nothing else.
`plan/PLAN.md`, `plan/CURRENT_STATE.md`, `CLAUDE.md`

On 2026-09-21 the user decided the `sort_key` of each partitioned table of the client model:
`data, sistema, contrato` in `cad_contratos`, `data, operacao` in `cad_operacoes`, `data, sistema,
contrato, operacao` in `rel_contrato_operacao` and `data_base, id_mensuracao, id_veiculo, id_conta`
in `cad_lancamentos`, after the orientation of that day: the key is the compound `SORTKEY` of the
published Redshift table and the `ORDER BY` of every partition written to the Delta; its first
column is the partition source, constant inside the partition and the pruning column of the whole
published table; a later column prunes while the earlier ones form long runs, so a high-cardinality
column prunes only right after the partition; `id_mensuracao` and `id_veiculo` have one value each
in the base today, and the user kept them. The user did not take the proposed primary-key
tiebreaker. The migration does not freeze the key the way it freezes the types: the Delta log does
not store it, changing it later reorders the partitions one rewrites, and Redshift has
`ALTER TABLE ... ALTER COMPOUND SORTKEY`. `plan/PLAN-STAGE-1.md`, `plan/PLAN-STAGE-7.md`

On 2026-09-21 the user decided how `sa.Text` reaches Redshift, the first of the seven pending
decisions of stage 1: `sql_type` keeps writing `VARCHAR(65535)`, the engine's ceiling, instead of
forbidding `Text` and requiring `String(n)` in the models (`sqlalchemy-redshift` would compile
`TEXT`, which Redshift stores as `VARCHAR(256)`), and `cast` now measures a `Text` column against
65,535 bytes. A value above it is refused at ingestion by `_refuse_text_above_varchar`, beside
`_refuse_text_above_length` for `String(n)`; both measure bytes, and without the check the value
would only abort the publication `COPY` (`Spectrum Scan Error` 15007, read on 2026-09-21), after
the data was already in the Delta. No column of the client model or of the reference model is
`Text`: the rule guards future models. `plan/PLAN-STAGE-1.md`, `plan/schema.md`, `docs/index.md`

On 2026-09-21 the user kept the byte measure for `String(n)`, the second pending decision of stage
1: `cast` measures with `pc.binary_length`, the measure Redshift applies to `VARCHAR(n)` (a UTF-8
character takes up to 4 bytes), so the contract refuses at ingestion what the publication `COPY`
would abort. The alternatives offered and declined were measuring characters (`pc.utf8_length`,
which reads as the model's author reads `n`, but moves the failure to the `COPY`) and measuring
characters with `VARCHAR(4n)` in the Redshift DDL. The audit of stage 4 (`octet_length` in
Redshift, `strlen` in DuckDB) and `scripts/migrate_parquet_to_delta.py` already measure bytes. The
same day `probes/parquet_source.py` gained the byte maximum beside the character length in its
sample section, because the client model's lengths came from a reading in characters: in the
fictitious base, `cad_contas.nome` reads 45 characters and 47 bytes.
`plan/PLAN-STAGE-1.md`, `plan/schema.md`, `probes/README.md`

On 2026-09-21 the user made the table and column comment optional, the third pending decision of
stage 1: `check_models` no longer reports a table or a column without `comment`. The alternatives
offered and declined were keeping the rule as it was and keeping it with the columns of a table
grouped into one violation. The numbers behind the choice: the reference model, which has no
comment at all, produced 129 violations, 85 of them about comments, so the 44 structural defects
were drowned. The client model keeps its 12 table comments and its 77 column comments. The column
comment still reaches the Arrow schema and the Delta schema; the table comment now has no consumer,
and whether `create_table` passes it as the Delta table's `description` is an open item of stage 3.
`plan/PLAN-STAGE-1.md`, `plan/PLAN-STAGE-3.md`, `docs/index.md`

On 2026-09-21, asked to confirm the `String(n)` lengths of the client model (the fourth pending
decision of stage 1), the user asked for the whole base to be measured first, instead of confirming
the lengths or widening the two with no margin. The lengths had come from the probe's 5,000-row
sample per table, in characters; two columns sit at zero margin (`cad_contratos.to` with
`String(2)` over the domain `01`..`ZT`, and `cad_contratos.fonte_familia` with `String(3)` over
`BND`..`FMM`), and `cad_lancamentos.meta` is 100% null over its 141,901,795 rows, so its
`String(255)` rests on nothing. `probes/parquet_source.py <raiz> --text-bytes` now reads the text
columns of every file and reports, per column, the longest value in bytes and in characters
(section 9); the run in the target closes the decision. `plan/PLAN-STAGE-1.md`, `probes/README.md`

On 2026-09-21 the user replaced the decision of the same day to measure the whole base before
fixing the `String(n)` lengths of the client model: the lengths are the model owner's, reviewed
directly in the code, and no run in the target gates stage 1. The `--text-bytes` section of
`probes/parquet_source.py` stays as an available reading of the base, not as a pending task. The
same day the user left the Redshift distribution at `AUTO`: the client model declares no `redshift`
key, and `svv_table_info` read after the first publication (stage 8, `diststyle`, `sortkey1`,
`tbl_rows`, `skew_rows`, the denied view being a reading too) says whether an explicit `distkey`
pays, which would then enter by `ALTER TABLE`. What weighed: nobody has measured how the clients
query, the AWS documentation recommends `AUTO`, and the distribution is reversible, unlike the
types the migration freezes. `plan/PLAN-STAGE-1.md`, `plan/PLAN-STAGE-8.md`

On 2026-09-21 the user closed the review of the client model's comments the same way as the
lengths: they are a first draft, and the model's owner revises them directly in the code, so the
item leaves `plan/OPEN_QUESTIONS.md`. The only pending decision of stage 1 left is the foreign key
`cad_contratos` declares to `rel_contrato_operacao`. `plan/PLAN-STAGE-1.md`

On 2026-09-21 the user removed the foreign key `cad_contratos` declared to `rel_contrato_operacao`,
the last pending decision of stage 1, and declined declaring `(data, sistema, contrato, operacao)`
as a unique key of `rel_contrato_operacao`. The key was copied from the original model and pointed
at columns that are not unique — the contract is apportioned among its N operations — so no engine
would accept it, and the alternatives offered were inverting it (the direction the unique index
`ix_contratos_data_sistema_contrato` sustains), declaring both, or keeping it. With it gone, the
audit of stage 4 checks nothing between the two tables, and `rel_contrato_operacao` keeps
`id_rel_contrato_operacao` as its only key. The query the client runs is contract → operations,
which the `sort_key` `data, sistema, contrato, operacao` already serves; the direction of a foreign
key never bore on it. `plan/PLAN-STAGE-1.md`, `tests/client_model/`

## The generated SQL text of stage 2

On 2026-09-21 the user decided how `bind` reads the generated text: quotes delimit literal content,
single and double alike, so `bind` rewrites a `:nome` marker only outside every quoted region, and
the regex gains the `"(?:[^"]|"")*"` alternative beside the one for `'...'`. The argument is that
the marker is the library's own, put in by `render` only where a value goes, so skipping quoted
regions can never miss a real marker. Two measurements of the same day showed what the draft's
expression did without it: a column named `taxa :base` came out as `"taxa $base"` with an invented
parameter `base`, and one named `preco d'agua` opened a false literal region that swallowed the next
`:nome` (`SqlError: parâmetros do texto [] e do dicionário ['data_str'] não fecham`). No identifier
of the contract carries `:` or `'`, so the two expressions agree on today's text. `bind` substitutes
no value into the SQL: it rewrites the marker's style and hands the dictionary to the driver.
`plan/PLAN-STAGE-2.md`

The same day the user decided who fills the `{prefix}` sentinel of a saved SQL file: the functions
that handle the file take a `prefix` argument, and the obligation to state it propagates to their
callers. `read_sql(directory, name, dialect, prefix)` replaces the sentinel and `prefix` is
required; `sql_files` always writes the sentinel, because the versioned file serves any target; no
other primitive fills it. The question came from the user reading `plan/PLAN-STAGE-4.md`, whose
`stream` row said the engine emptied `{prefix}` "por `sql.bind`" while the stage 2 signature
`bind(sql, params, style)` has no prefix; stages 4 and 5 now say the ready text arrives with the
prefix already replaced. `plan/PLAN-STAGE-2.md`, `plan/PLAN-STAGE-4.md`, `plan/PLAN-STAGE-5.md`


## The code review of 2026-09-21

The user refused the three proposals the review left open, all on the same day.
`probes/redshift.py::session` (310 lines, four levels, eleven checks on one connection) **stays as
it is**, and so do `apis` and `copy_role`: the open item left `plan/OPEN_QUESTIONS.md` with the
answer. `_resolve_metadata` **stays private in `serialize_db.cli`**, duplicated by
`scripts/migrate_parquet_to_delta.py`, which the stage 7 `initial_load` absorbs anyway. **No linter
enters the project**: `ruff` in the `dev` group and in the workflow was refused, so the style rules
of `CLAUDE.md` stay checked by reading, and the line width stays what each file uses (100 in the
package and in the script, wider in the probes and in the proofs of concept). A later session
proposes none of the three again.

On 2026-09-21 the user kept `duckdb-engine` and `sqlalchemy-redshift` as the compilers of
`render` in stage 2, after the review measured the alternative (SQLAlchemy's own `postgresql`
dialect with a fully quoted table copy compiles the same text, quotes every identifier and runs in
DuckDB). Consequence recorded in `plan/PLAN.md`: the two packages leave the `dev` group and enter
the runtime dependencies in the commit that writes `serialize_db.sql`, because the engines of
stages 4 and 5 call `render` at run time; `prepare_offline.sh` runs again then.
`plan/PLAN-STAGE-2.md`, `plan/PLAN.md`

The same day the user accepted the recommendation that followed from keeping the dialects: the
prefixed copy `render` compiles is built with `quoted_name(quote=True)` on the table name and on
every column, so the DML quotes every contract identifier like the DDL of stage 1, with the
`{prefix}` sentinel inside the quotes and no dependence on either dialect's reserved-word list;
labels and the rest of the statement stay quoted as the dialect requires, because they are the
client's. The rule of `plan/PLAN.md` stays as written. `plan/PLAN-STAGE-2.md`

On 2026-09-22 the user put `sqlglot` in the tests: `sqlglot==30.18.0` (the version of the scratch
trial of 2026-09-21) enters the `dev` group, and `test_redshift_text_parses_with_sqlglot` stops
being optional. The decision was taken believing the versioned file with the sentinel does not
parse, a reading of the `quote=False` draft of 2026-09-21; the implementation of 2026-09-22 showed
the quoted sentinel parses, and the test parses the versioned file of every statement and the
text with the prefix empty. Stage 2 has no decision awaiting the user. `plan/PLAN-STAGE-2.md`,
`plan/POC.md`, `pyproject.toml`

## The decisions of stage 4

On 2026-09-22 the user took the four decisions of stage 4. The `loader` refuses a name already taken
in the sandbox with `SandboxError`, instead of creating with `IF NOT EXISTS` and appending: a loop
over partitions keeps one loader open. `memory_limit` stays at DuckDB's default, 80% of the machine,
and the engine only records in the log the value DuckDB chose, beside the free space of
`temp_directory`; the first real run measures what is left for the client's pandas, which DuckDB's
limit does not cover. The database is a file at `<temp_directory>/<execution_id>.duckdb`, with
`temp_directory` omitted becoming a new `tempfile.mkdtemp` folder that `cleanup` deletes, and
`:memory:` only through `DuckDBConfig(database=":memory:")`. The `AuditReport` carries up to 20 whole
rows of sample per failed check, in the log, and the `linhas` check, a `count(*) FILTER` per column,
fetches its sample in a second query per counter above zero. The refusal brought two decisions of
the same day: `published(table, uri, version)` in both engines' protocol and `run.published(table)`
in `Execution` — the pinned version as a query source that occupies no name in the sandbox, which is
also what the audit's keys outside the partition already needed — and `SandboxError` as the stage's
exception in `serialize_db.errors`. Stage 4 has no decision awaiting the user.
`plan/PLAN-STAGE-4.md`, `plan/PLAN-STAGE-5.md`, `plan/PLAN-STAGE-6.md`, `plan/PLAN.md`,
`plan/serialize-db.md`, `plan/POC.md`

## The decisions of stage 3

Four decisions, all before any code. `create_table` passes the table comment as the Delta
`description`, and `reconcile` syncs the description and the column comments with the model through
`set_table_description` and `set_column_metadata`: a probe of the same day showed the three survive
every `overwrite`, so the only question left was drift. `storage_options` carries no credential at
all: the delta-rs default chain renews them inside the `DeltaTable` an execution holds, while a
frozen `boto3` trio would expire in about an hour mid-execution and would travel in a dictionary a
log or an exception prints; `test_delta_rs_storage_options_fallback` keeps measuring the shape for
the day an environment breaks the chain. `register_files` registers min and max of the integer,
date, `Double` and `String` columns, the four that transcribe exactly, and leaves `decimal` and
`timestamp` out. `version_diff` refuses a cleaned log with `LogUnavailable`, naming the full
publication, instead of falling back to the set difference of `get_add_actions`: `create_table`
pins `delta.logRetentionDuration` at 3650 days, and the cleanup that would remove the file also
makes the published version unreadable. Stage 3 has no decision awaiting the user.
`plan/PLAN-STAGE-3.md`, `plan/POC.md`

## The sandbox tables and the DDL flag of 2026-09-22

On 2026-09-22 the user proposed, in `CLAUDE.md`, temporary tables to separate the pipeline's
execution from the published data in the single Redshift schema, with prefixes for the
environments, and reverted the sentence the same day after the analysis: the sandboxes stay with
regular tables, `exec_<id>_*` in the datashare schema for Redshift and the models' names in the
throwaway file database for DuckDB, as the plan already had. What weighed: a DuckDB temporary
table belongs to the connection that created it while the engine gives a cursor per thread, stream
and loader (probe of the same day); a Redshift temporary table lives in the session, which the
serverless workgroup ends after 3,600 s idle, cannot be inspected from outside or after a failed
audit, and gets `RAW` encoding by default. The user asked for
`ddl(table, dialect, prefix="", temporary=False)` all the same, with `temporary=True` emitting
`CREATE TEMP TABLE` and no use inside the plan. `plan/PLAN-STAGE-1.md`, `plan/POC.md`

The same day the user removed the per-thread connection from the Redshift engine (stage 5): the
engine keeps one session per execution and a `threading.Lock`, every command takes it, `stream`
executes under it and its helper thread only slices `fetchmany` (the driver materializes the
result in `execute`), `loader` writes the Parquet outside it and runs the `COPY` under it;
`ingest(max_workers)` serializes on that engine, `publish_redshift` keeps a connection per table,
and a temporary table the pipeline creates in the session serves the next commands and is lost
when the engine reconnects. The DuckDB engine keeps its cursor per thread, stream and loader.
`plan/PLAN.md`, `plan/PLAN-STAGE-5.md`, `plan/PLAN-STAGE-6.md`, `plan/serialize-db.md`

The same day the user generalized the partition column: any text column `String(n)` partitions,
`partition_source` is optional and the derivation `strftime('%Y-%m-%d')` is checked by the audit
and the initial load only when the model declares it, and the `AAAA-MM-DD` date is the current
base's case, not the contract; the value must serve as a folder name and a literal (no `/`, `=`,
space or empty), `previous_partitions` returns the text order, and `Execution` validates the
value against those rules instead of `AAAA-MM-DD`. `check_models` refuses a partition column
without length, a `partition_source` the table lacks and one without `partition_by`. A non-text
partition type stays out: five SQL templates and the log's `partition_values` treat the value as
quoted text. `plan/PLAN.md`, `plan/PLAN-STAGE-1.md`, `plan/PLAN-STAGE-4.md`,
`plan/PLAN-STAGE-6.md`, `plan/PLAN-STAGE-7.md`, `docs/index.md`
