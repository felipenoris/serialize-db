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
`duckdb_engine` and `sqlalchemy-redshift` leave the runtime dependencies (superseded: on 2026-09-21 the two dialects stayed runtime dependencies as `render`'s compilers, and on 2026-09-22 the user made the Core statement submitted to the engine the default path and the generated text the optional migration path out of SQLAlchemy). On 2026-09-20 the user fixed
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
reports stay outside git (the user decided on 2026-09-23 not to copy them to `plan/readings/`). The same day the user confirmed the reading of `RS-19`: `USE
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
when the engine reconnects. The DuckDB engine keeps its cursor per thread, stream and loader
(superseded later the same day: both engines keep a single session under a lock, section "The
review of 2026-09-22 and the single session on both engines").
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

The same day the user made the Core statement submitted to the engine the default path
(`run.sandbox.query`, `execute` and `stream` compile the prefixed copy with the client's
parameters and run it on the raw connection) and the generated SQL text the optional migration
path out of SQLAlchemy, not the core feature; the wording in `plan/PLAN.md` follows the proposal
the user accepted, with one requirement the user stated: the library never demands a model that a
`sqlalchemy.Connection` created outside the library would refuse, because the client may submit
the same statements there. The user accepted the two proposals from the probes of that day, both
the same day: `render` maps a valueless `bindparam` to `:nome` by `replacement_traverse` and
`param` left the module, so one statement serves the client's `Connection`, the engines and the
files (`plan/PLAN-STAGE-2.md`); and `UniqueConstraint` replaced the two unique indexes the client
model's composite foreign keys reference, with the `check_models` rule that a foreign key targets
the referred table's primary key or a `UniqueConstraint` in the same column order, because DuckDB
refuses a unique index as target and a swapped order, and Redshift documents the same requirement
(`plan/PLAN-STAGE-1.md`, `plan/schema.md`). The
migration ran successfully in the target; its reports exist and are not available yet, so the
`export_mode` default still waits for their numbers. `plan/PLAN.md`, `plan/PLAN-STAGE-2.md`,
`plan/OPEN_QUESTIONS.md`, `plan/POC.md`

## The review of 2026-09-22 and the single session on both engines

Later on 2026-09-22, answering the code review, the user accepted the rewrite of the environment
premise in `plan/PLAN.md` (dev and prod never touch each other's tables; inside an environment one
execution at a time, with the log ordering commits and `publish` aborting the second with
`ExecutionConflict`; the shared `serialize_db_publications` as the exception) and the two stage 3
proposals: `expressions` in `rewrite` (the old name in a rename, the value of a new `NOT NULL`
column) and `Storage` over `pyarrow.fs` (a dataclass with the URI, the filesystem and the path, no
class per storage, `boto3` only for the conditional write of `_serialize_db/snapshots.json`). The
same day the user asked for one behavior on both engines: a single session per execution protected
by a lock, so temporary tables are portable, keeping the streaming requirement (the client works on
the current batch while the connection does I/O) and an API that handles the lock for the client.
The probes of the same day showed it feasible with intermediate files (`.claude/memory/concurrency.md`),
and the plan now has `session()` on both engines, `stream` and `loader` through Arrow IPC files on
DuckDB, and `ingest` without `max_workers`. The user stated that the target may have any number of
vCPUs and that the library must explore parallelism to scale; the 2 vCPUs read on 2026-09-21 are a
reading, not a design premise. `plan/PLAN.md`, `plan/PLAN-STAGE-3.md` to `plan/PLAN-STAGE-6.md`,
`plan/serialize-db.md`, `plan/POC.md`

## The answers of 2026-09-23: extra sessions, parallel ingest and the audit text

On 2026-09-23 the user asked for a probe of simultaneous Redshift transactions around
`serialize_db_publications`; it is `tests/proof_of_concept/test_redshift_transactions.py`
(`-m redshift`, five scenarios and the isolation readings), waiting for a run in the target, and
its result decides whether the stage 8 transaction stays as it is. The user asked whether the
client could open several sessions for parallel reads, accepting no ordering between calls and no
shared temporary tables: the plan adopted `new_session()` on both engines, an engine over another
connection (a DuckDB `cursor()`, a Redshift connection with its own temporary credential and
`USE`) with its own lock; the name is the assistant's proposal, named in the report. The user
suggested that the engines' initial ingestion, one table at a time by nature, always run in
parallel: `run.ingest` of more than one table runs each in its own extra session, all at once, and
returns when all finish. The user asked whether the first batch waited for the whole query in the
earlier design and whether that design was more efficient; the measurement answered yes for
`stream` and no for `loader`, and the assistant changed `stream` so a helper thread runs the query
under the lock and writes each batch to the spool file while the client reads the batches already
written (`prefetch` left the signature), named in the report as a change the user did not ask for.
The user asked to record the `export_mode` revision trigger in the plan: the target's migration
report with the `cad_lancamentos` partition in both modes sets the default and decides, per engine
and for the initial load, whether the other mode leaves stages 4, 5 and 7. The user accepted
freezing the generated SQL text path where it served only diffs: `audit_files`,
`write_audit_files` and `serialize-db audit --write` left the plan, and `audit_sql` with `--sql`
stay for debugging. `plan/PLAN.md`, `plan/PLAN-STAGE-4.md` to `plan/PLAN-STAGE-8.md`,
`plan/serialize-db.md`, `plan/redshift.md`, `plan/POC.md`, `plan/OPEN_QUESTIONS.md`

## The review of stages 3 and 4 of 2026-09-23

The user answered the review's proposals the same day. Accepted, now written in
`plan/PLAN-STAGE-4.md`, `plan/PLAN-STAGE-5.md` and `plan/PLAN-STAGE-6.md`: the DuckDB engine compiles
Core statements with `duckdb_engine.Dialect(paramstyle="qmark")` and the positional list of
`positiontup`, with no marker rewrite (the text path keeps `bind`); the audit checks a key that
includes the `partition_source` column only in the execution's partitions, and gives the
single-column integer primary key a `skip_when` (`min(key) > published_max_key`, the pinned
version's `max_key`) that skips the join with the other partitions, and on Redshift the key staging;
and the engine interface: `query(statement_or_sql, params=None)` replaces `query(statement,
**params)` and `execute` (the assistant's reading of "`execute` como a mesma primitiva de `query`
sobre texto", named in the report), `export_partition` takes the `mode` resolved by `Execution`, and
the engine never reads `SERIALIZE_DB_EXPORT_MODE`, `audit` and `audit_sql` take `checks`' named
arguments instead of `**options`, `checks` has no `prefix`, and `export_partition` in `rewrite` mode
hands the DuckDB reader to `write_deltalake` under the lock, without `stream`. The user asked
whether removing the `CAST` from the control total would solve the non-finite `Double`: it would
stop the crash, but `sum(double)` changed with the thread count (five results for 1 to 11 threads,
the farthest 13,409 from the exact sum over 20,000,000 values) while the `DECIMAL(38, 6)` sum stayed
exact, and the pruning defect involves no `CAST`; the decision stays pending in stage 1. The user
asked for the context of the `loader` creating its table at `close` and of `interrupt()`, and for a
reference implementation of the hybrid `stream` before deciding; the three stay pending in
`plan/OPEN_QUESTIONS.md`. `plan/PLAN-STAGE-1.md`, `plan/PLAN-STAGE-4.md`, `plan/POC.md`

The same day the user answered the rest. The non-finite `Double`: `cast` does not refuse `NaN` or
infinity; the pruning defect is GitHub issue #59 (opened at the user's request) and stays in
`plan/OPEN_QUESTIONS.md`, with the `register_files` statistics for a `has_nan` column pending in
stage 3 and the audit counting non-finite values as a reading, without failing. Accepted and
implemented in the sketches of `tests/proof_of_concept/test_parallel.py`, the stage 4 reference: the
`loader` checks the name at open on a cursor of its own, without the session lock, and creates the
table at `close` in one transaction with the `INSERT`; the hybrid `stream` (batches in memory up to
64 MiB, the LZ4 spool file after), which the user asked to see as code before approving; and
`interrupt()` in the stream's `close` and in the engine's `cleanup`. Stage 4 has no decision
awaiting the user. `plan/PLAN-STAGE-1.md`, `plan/PLAN-STAGE-3.md`, `plan/PLAN-STAGE-4.md`,
`plan/PLAN.md`, `plan/POC.md`

After merging PR #58 the user read PARQUET-1246 as the Parquet spec recommending no min and max for
a column holding `NaN`, noted that DuckDB documents its own `NaN` convention, and proposed that the
library follow the Parquet storage standard whatever DuckDB does (2026-09-23). The sources showed a
different spec rule (min and max without the `NaN` plus `nan_count`), a Delta log outside the
Parquet spec, and DuckDB losing the row through both layers; the recommendation awaiting the user is
no `Double` min and max in the footer or the log. `plan/POC.md`, `plan/PLAN-STAGE-3.md`,
`plan/OPEN_QUESTIONS.md`

## The decisions of stage 6

On 2026-09-23 the user took the four pending decisions of stage 6. `serialize-db run` keeps
`--metadata modulo:atributo`, like `schema` and `sql`, instead of a conventional attribute of the
pipeline module. On `next_ids` the user stated that it serves only sequential keys, which are a
single column; for a table whose primary key has several columns the client decides the ids
directly and does not call `next_ids`. So `next_ids` takes the single-column integer primary key
and no `id_column` option exists; raising `ContractError` on another key is the assistant's
reading, named in the report. The table barrier left the plan and stage 5's Redshift `loader` was
aligned with stage 4 (refuse a taken name at open, `CREATE TABLE` and `COPY` in one transaction at
`close`), after the probe of the same day showed that a read racing a forgotten `load` fails with
`CatalogException` instead of reading old rows; checking the name on Redshift by
`select 1 from <esquema>.<nome> limit 0` is the assistant's proposal. The partition value and the
`execution_id` follow the allowlist `[0-9A-Za-z][0-9A-Za-z_.-]*` (full match) instead of the list
of refused characters: the single quote broke the `publish_partition` predicate, and delta-rs
percent-encodes `:`, `%`, `#`, `'` and accents in the folder name, which the `register` mode's
`COPY` writes unencoded. Validating every value of `partitions` in `ingest`, `audit` and `publish`
is the assistant's extension, named in the report. Stage 6 has no decision awaiting the user.
`plan/PLAN-STAGE-4.md`, `plan/PLAN-STAGE-5.md`, `plan/PLAN-STAGE-6.md`, `plan/PLAN.md`,
`plan/POC.md`, `plan/OPEN_QUESTIONS.md`

Later on 2026-09-23 the user asked for the stage 6 interface to be corrected against its table of
primitives: `Database(root, environment, metadata)` without `storage_options`, which stage 3
resolves per call; `Execution(..., export_mode=None)` as the default mode of the execution's
`publish` calls, filled by `serialize-db run --export-mode`, resolved in the order `publish`
argument, `Execution` argument, `SERIALIZE_DB_EXPORT_MODE`, `"register"`; and `--export-mode` in the
`run` row. The assistant also aligned `uri` without a trailing slash, `partitions=None` in
`publish`, `delta.snapshot(storage, environment, name, versions)`, the `serialize-db audit`
options and `AuditFailed` imported from `serialize_db.errors`, and made `storage` a
`functools.cached_property` after a probe showed the frozen dataclass refusing the `init=False`
field in `__post_init__`; each was named in the report. `plan/PLAN-STAGE-6.md`,
`plan/PLAN-STAGE-5.md`, `plan/POC.md`

## The decisions of stage 5

On 2026-09-23 the user took the four pending decisions of stage 5, after the assistant's review found
defects behind three of them. The control table was the user's own proposal: neither `connect` nor
`publish_redshift` creates `serialize_db_publications`; `publish_redshift` checks at its start that
the table exists and stops with an error when it does not; a dedicated initialization, run
explicitly by the user once, creates it, so no pipeline code path runs its `CREATE TABLE`. The
assistant's choices around it, named in the report: `create_publications_table(config)`,
`serialize-db publish --init`, the new `PublicationError`, a plain `CREATE TABLE` without `IF NOT
EXISTS`, the check by `select 1 ... limit 0` outside a transaction, `publication_status` making the
same check, the pipeline's opening no longer reading the table (`plan/PLAN.md`, step 1), a
runbook row in stage 9, and `connect` running no command to confirm the `USE` (the first two-part
statement confirms it). The user accepted the other three proposals as made. `stream` always goes
through `UNLOAD ... PARALLEL OFF` to `staging/<execution_id>/stream/<uuid>/` and `query` always
through the cursor, because the engine cannot know a result's size before the `execute` that
materializes it; the client's values enter the `UNLOAD` as literals, guarded by `required` in
`compiled.binds`, and the `'` and `\` escape is probed in the target first. `load` always goes
through the `loader`, and the multi-row `INSERT` left. `schema_from_row_description` reads the
`NUMERIC` precision and scale from the `type_modifier` in the driver's private
`cursor.ps["row_desc"]`, `numeric_types` left, and the `redshift` extra pins
`redshift-connector==2.1.16`. The export has no `PARTITION BY` and writes to a prefix new per
partition and per attempt, `<uri>/<coluna>=<valor>/<execution_id>_<uuid>/` in `register` and
`staging/<execution_id>/<tabela>/<coluna>=<valor>/<uuid>/` in `rewrite` (the `rewrite` path is the
assistant's). Stage 5 has no decision awaiting the user; the readings the next suite run in the
target makes for these decisions are in `plan/OPEN_QUESTIONS.md`. `plan/PLAN-STAGE-5.md`,
`plan/PLAN-STAGE-8.md`, `plan/PLAN.md`, `plan/POC.md`, `plan/OPEN_QUESTIONS.md`

The user then chose the per-partition version of the #59 rule (2026-09-23): a `Double` column with a
non-finite value (`NaN` or infinity) in a partition is written without min and max, in the Parquet
footer (`ColumnProperties(statistics_enabled="NONE")` in `publish_partition`) and in the Delta log
(`register_files`); the other partitions keep both. The list comes from the audit's non-finite count
per partition and column (`AuditReport.nonfinite_columns`, the row query grouped by the partition
column), which `run.publish` passes as `columns_without_min_max` to `export_partition` of both
engines; with `audit=False` every `Double` column goes in the list. The trigger is any non-finite
value, so the audit count serves as it is and the infinite-extreme special case of `register_files`
goes. `initial_load` counts non-finite values in its partition check query, and its `load_report`
sums only finite values. Open: the footer the Redshift `UNLOAD` writes for a row group with `NaN`.
`plan/PLAN-STAGE-3.md`, `plan/PLAN-STAGE-4.md`, `plan/PLAN-STAGE-5.md`, `plan/PLAN-STAGE-6.md`,
`plan/PLAN-STAGE-7.md`, `plan/OPEN_QUESTIONS.md`

## The implementation of stages 3, 4 and 6

On 2026-09-23 the user approved aligning stage 6 with the issue #59 decision (`publish` hands the
audit's `nonfinite_columns` to `export_partition`) and asked for stages 3, 4 and 6 to be implemented
in full, reviewing the following stages after each one with what it taught. The three landed on PR
#61 the same day, with a review commit after each. The assistant's choices, named in the report:
`Storage` gained `relative`, `uri_of`, `size`, `ensure_folder`, `open_input_file` and
`duckdb_connect`, and `storage_options` passes `max_retries=3` and `retry_timeout=10s` without
`timeout`; `register_files` also refuses a file holding the partition column, columns out of the
contract order and a null in a `NOT NULL` column by the footer's null count, and `read_back`
compares the log's key bounds with what the readers read; `register_files` also drops a non-finite
float extreme that reaches it outside `columns_without_min_max`, a guard against the invalid-JSON
`Infinity` beside the issue #59 rule, which removed that special case from the plan; `rewrite` counts the non-finite `Double`
per partition; the audit counts with `count(CASE WHEN ...)`, because Redshift's `COUNT` has no
`FILTER`, and the engine's `audit` takes `referenced`; the publication pool hands a table to a free
worker only, and a failure goes up with each table's outcome in a note; `Execution` refuses the
`"redshift"` engine until stage 5, `publish_redshift` waits for stage 8, and the snapshot is
written only when the execution ends without error; the two stream-cancellation tests assert the
thread ended and the session is free, with the error null or the interrupt's. `plan/PLAN-STAGE-3.md`,
`plan/PLAN-STAGE-4.md`, `plan/PLAN-STAGE-6.md`, `plan/POC.md`


## The code review of 2026-09-23

On 2026-09-23 the user asked for a review of `src/` and `tests/` against the repository's code rules
(PEP 8, readability for a junior reader, no over-engineering, abstractions at the right level) and
for the corrections. The same day the user decided to retire the drafts of package code from the
study suites: the sketches `SandboxEngine`, `BatchStream` and `Loader` of
`tests/proof_of_concept/test_parallel.py`, which `DuckDBStream` repeated line for line and which had
already diverged from the engine (the sketch's `load` registered and ran `CREATE TABLE AS`), their
seven twin tests, the copies of `arrow_schema` and `delta_schema` in `test_sqlalchemy.py`,
`RangeAllocator` in `test_concurrency.py`, and the models in `test_stdlib.py` that still stated the
old partition rule. The study suites keep only the facts of the external libraries; the unique
cases moved to `tests/test_engine_duckdb.py` and `test_duckdb.py`. The assistant's choices, named in
the report: `schema` gained the protected helpers three modules repeated (`literal`,
`sequential_key`, `double_columns`, `foreign_keys_by_columns`, `TEXT_LIMIT`); `run.ingest` and
`run.publish` share `_run_in_pool`; the CLI dispatches by `set_defaults(handler=...)` and resolves
`modulo:atributo` by `pkgutil.resolve_name`; a test cited by name in `plan/` was never split or
renamed, only restructured in blocks; the abandoned `DuckDBLoader` deletes its spool file, as its
docstring and stage 4 said; `tests/conftest.py` skips by marker, masks credentials at output time
and names its root class `SessionRoot`; the target-only suites changed only in form. Reported, not
changed: `Database.__post_init__` writing `os.environ` in the tests. The user answered the report
the same day: code the plan assigns to a later stage stays, even without a caller (the three
`Database` prefixes of stages 5, 8 and 9, `staging_prefix`, `publication_prefix` and
`archive_prefix`, and the `sandbox_prefix` draft of stage 5 in `test_stdlib.py`); in the real base
`id_parent` and `id_child` always differ, and `rel_contas_hierarquias` implements a tree of
accounting accounts, which the fixture now builds and `tests/test_source_db_projetado.py` checks.
`plan/PLAN-STAGE-4.md`, `plan/CURRENT_STATE.md`, `plan/POC.md`, `plan/OPEN_QUESTIONS.md`

The user asked on 2026-09-23 to build the local stand-in and apply the behavior corrections of the
target-only suites, then to version the stand-in: `tests/emulator.py`, switched on by
`SERIALIZE_DB_TEST_EMULATOR`, with moto pinned in the `dev` group (in the `emulator` group since the
answers to the pending decisions, below). `plan/POC.md`,
`plan/CURRENT_STATE.md`

## The early migration's reports

On 2026-09-23 the user handed over the JSON reports of the early migration run in the target,
written by the script version before the issue #59 rule, and asked that they not be recorded in the
repository, allowing the memory to keep the findings it needs: the reports stay outside git, the
findings live in `source-base.md`, and `plan/` is not updated, so `plan/CURRENT_STATE.md`,
`plan/OPEN_QUESTIONS.md` and `plan/POC.md` still describe the reports as unavailable and still list
the issue #59 item on the migrated tables. The user offered to run the latest script again in the
target; the assistant's analysis of the same day found that only the `cad_lancamentos` partition in
`rewrite` and with `--no-sort` needs the rerun. The user declined the separate runs the assistant
proposed and asked instead that the script produce the measurement when the `SUITE.md` commands
run again, every table with the same parameters: the script measures, before each partitioned
table's load, every requested partition in the four write variants, each in a new process
(`spawn`) with its own peak (`VmHWM` on Linux) and a scratch table under `<root>/_medicao_<table>/`,
also when the partition is already in the log, and the report carries the machine; `--no-measure`
turns it off (the assistant's design, named in the report). `source-base.md`,
`plan/PLAN-STAGE-7.md`, `plan/OPEN_QUESTIONS.md`

## The answers to the pending decisions

On 2026-09-23 the user went through the pending decisions of the plan and answered them. `cast`
refuses a tz-aware `timestamp` in a `DateTime` column without time zone and the reverse
(`ContractError`; the same 12:00 UTC came out 12:00 from the PyArrow cast and 09:00 from the DuckDB
`CAST` in `America/Sao_Paulo`); another zone into a tz-aware column keeps the instant. Stage 8:
`FILLRECORD` on every `COPY` of the library; the JSON document capped at 65,535 bytes in the
contract, refused by `cast` and counted by the audit (`texto_<coluna>`, `json_size` on Redshift),
with no path for larger documents until a table needs one; a `VARCHAR(n)` width change is a
destructive diff (recreate and reload), detected through `svv_all_columns`, and
`ALTER COLUMN ... TYPE` enters only after `test_alter_column_type_on_the_share` reads it on the
datashare. Stage 9: the runbook is `docs/operacao.md`, published by pdoc through the
`serialize_db.cli` docstring; the monthly `vacuum` keeps the 400-day retention, and the user asked for
a pdoc section on it and on how to change it (`docs/index.md`, "Retenção dos arquivos removidos").
The probe reports of 2026-09-21 in `secrets/` stay out of git, and the Redshift reports left
`plan/readings/` (a report stays only while a pending stage consults it; the production base reading
stays). The local stand-in stays out of the GitHub workflow: it runs locally before a push that
touches what the target-only suites cover, from the `emulator` dependency group (moto, flask), kept
out of `dev`. Still open after the same conversation: the publication staging (temporary or regular;
the user asked where the one-database-per-transaction rule comes from: the AWS page "Considerations
for data sharing reads and writes", never measured in the target), the entry of an archived snapshot
(the assistant proposed moving it to a sibling `archived` key), the pytest temporary folder (the
user asked for context), and the source of the distribution reading, which the probe of the same
day found denied in `svv_table_info`. `plan/PLAN-STAGE-1.md`, `plan/PLAN-STAGE-8.md`, `plan/PLAN-STAGE-9.md`,
`plan/OPEN_QUESTIONS.md`, `docs/index.md`, `README.md`

The user answered the three questions left open the same day. The publication staging: the two
temporary-staging cases go into `tests/proof_of_concept/test_redshift_transactions.py`, one filling
the temporary table inside the transaction and one before the `BEGIN` (outside the transaction, which
then writes only to the datashare database and holds its locks shorter), and the target run decides:
temporary when either passes, a regular staging named per execution when neither does. The archived
snapshot's entry moves from `snapshots` to a sibling `archived` key of the control file in the same
conditional write, and `snapshot` refuses a name present in either key (both enter
`serialize_db.delta` with stage 9's `archive`). The pytest temporary folder: the tests that wrote
there, `tests/test_source_db_projetado.py` and the 17 writing cases of `tests/test_probes.py`, are
`local` and write under `SERIALIZE_DB_TEST_LOCAL_ROOT`, so the premise that `pytest` without a
variable writes nothing holds (checked in a stripped environment). `plan/PLAN-STAGE-8.md`,
`plan/PLAN-STAGE-9.md`, `plan/POC.md`, `plan/CURRENT_STATE.md`

The same day the user chose the source of the stage 8 distribution reading, after `probes/redshift.py`
(`RS-8`) found `svv_table_info` denied to the role after the `USE` (42501): the published tables stay
`DISTSTYLE AUTO`, and the reading after the first publication is the `EXPLAIN` of a typical join
between published tables (`cad_lancamentos` with `cad_contas` on `id_conta`); an explicit `DISTKEY`
enters by `ALTER TABLE ... ALTER DISTKEY` only when the plan shows `DS_BCAST_INNER` or `DS_DIST_BOTH`.
Rejected: `SHOW TABLE`, which probably shows only `DISTSTYLE AUTO`, and asking the administrator for
the view. `tests/proof_of_concept/test_redshift.py::test_explain_of_a_join_on_the_share` reads whether
the role may run `EXPLAIN` on the datashare. `plan/PLAN-STAGE-1.md`, `plan/PLAN-STAGE-8.md`,
`plan/OPEN_QUESTIONS.md`

## The readings folder

On 2026-09-23 the user decided that `plan/readings/` holds the reports of 2026-09-23, the three
suite sessions and the five probes, in place of the older ones, with the environment's sensitive
identifiers masked: the accounts, the database user and the personal folder, the role id and the
SSO role suffix, the DataZone domain, project and environment ids, the KMS key, the datashare
producer's namespace and the private addresses, each replaced by a placeholder the folder's
`README.md` lists. The source-base reading of 2026-09-21 stays, masked, because no newer reading
replaces it; the probe reports of 2026-09-21 stay out of git, and the older readings live in git
history. The rule of the same day decides when a report leaves: once `plan/POC.md` and the stage
file hold what it showed. The same identifiers remained elsewhere in the repository (`plan/POC.md`, the memory,
`SUITE.md`, `examples/`, `tests/test_probes.py`) until the user's answer of the same day, in the
section on the publication flow. `plan/readings/README.md`

## The Redshift suite runs of 2026-09-23 and the stage 8 transaction

On 2026-09-23 the user ran the three target-only suites from `main` (S3 at 18:48 UTC, Redshift at
18:52 and 18:55) and approved registering their readings in `plan/` and fixing the tests in the
open PR: the `UNLOAD` literal doubles the backslash as well as the quote, the stream test reads
`pg_last_unload_count()` to tell an empty result from a missing manifest and records an empty
`UNLOAD` instead of stopping, and the Redshift audit text compiles `is_finite` as the strict
comparison with the infinities and `json_valid` as `true` on the `SUPER` column (the assistant's
fixes, named in the report). For the stage 8 transaction the assistant offered (a) keeping the
`DELETE`/`INSERT` of the control row and repeating the transaction on `1023`, or (b) opening the
transaction with `UPDATE serialize_db_publications ... WHERE table_name = <t> AND delta_version =
<version read>`, where 0 affected rows raises `ExecutionConflict` without a retry and a `1023` that
still escapes from any command raises `ExecutionConflict` too; the user chose (b), and replaced it
later the same day with the flow of the next section, as the answers on the first publication and
on the two stage 5 proposals came. `plan/PLAN-STAGE-8.md`, `plan/PLAN-STAGE-5.md`,
`plan/PLAN-STAGE-4.md`, `plan/POC.md`, `plan/OPEN_QUESTIONS.md`

## The publication flow, the unpublish flow and the stage 5 answers of 2026-09-23

Asked how the first publication of a table writes its control row, the user redesigned the stage 8
transaction (2026-09-23): open the transaction, read the table's control row, identify the previous
version, `INSERT` the row when there is none, and when there is one check the version and
`UPDATE` it; and create an unpublish flow, similar but with `DELETE`. The assistant's reading,
which the user confirmed the same day: the read is the transaction's first statement and fixes its snapshot; the
check compares the version read with the Delta version the execution publishes (equal: nothing to
publish, `ROLLBACK`; higher: `ExecutionConflict`; lower: `version_diff` between them); the control
row is written last, after the partitions, because its `INSERT` or `UPDATE` holds the control
table's lock until the end and the tables publish in parallel; the `UPDATE` stays conditioned on the
version read, and its zero rows, a `1023` and the failed `CREATE TABLE` of a published table another
first publication created are `ExecutionConflict`, without a retry. The unpublish flow reads the row
the same way and, when present, runs `DROP TABLE` on the published table and `DELETE` of the row,
conditioned on the version read, in one transaction (`unpublish_redshift`,
`serialize-db publish --unpublish`); a destructive schema diff unpublishes and republishes. The
concurrent behavior of the new sequence is a reading of
`test_redshift_transactions.py::test_control_row_read_first_and_written_last`, added the same day.
On stage 5 the user approved exporting by `rewrite` a partition with a non-finite `Double` whatever
the `mode`, with a warning in the execution log (`log.warning`, the user's choice over
`warnings.warn`) when the mode asked for `register`; approved the empty text stream's schema from
`schema_from_row_description` of `select * from (<texto>) as t limit 0`; and kept `load` through the
`loader` after the `COPY` cost reading. The user also asked to mask the environment's sensitive
identifiers everywhere in the repository except `SUITE.md`: `plan/POC.md`, the memory, the bucket
path of `examples/redshift_copy_unload.py` and `examples/redshift_manifest.py`, and the lab
account in a fabricated ARN of `tests/test_probes.py`, which took the documentation's
`123456789012`, carry the placeholders of `plan/readings/README.md` (the lab's got their own:
`<conta do laboratório>`, `dzd-<domínio do laboratório>`, `<projeto do laboratório>`); git
history keeps the old values. `plan/PLAN-STAGE-8.md`,
`plan/PLAN-STAGE-5.md`, `plan/OPEN_QUESTIONS.md`

## The battery of 2026-09-23 at 22:49 and the parallel-processing instruction

On 2026-09-23 the user added to the user section of `CLAUDE.md` that the programs using the package
run on scalable AWS compute of the user's choosing, and that the plan optimizes for parallel
processing. The same evening the user ran every `SUITE.md` command from `main` with #67, on a
4 vCPU and 16 GB machine, with new roots under the personal folder (`.../shared/<usuário>/serialize-db/`),
handed over the reports and offered to rerun the whole battery on a more powerful machine if that
yields more information. The user also confirmed the assistant's reading of the stage 8
publication (the section above). From the results the assistant changed the Redshift audit count
of non-finite values to `count(x) - count(finite)`, fixed the `redshift.py` crash on a `count(*)`
without rows, made the threads probe turn off DuckDB's external file cache and the migration script
rewrite its report after each step, turned the readings two clean runs confirmed into assertions
and made the stand-in refuse `ALTER COLUMN ... TYPE`; it proposed, awaiting the user, `register` as
the default `export_mode` with `rewrite` only for the stage 5 non-finite fallback, the load sorted
by `sort_key`, and the temporary staging filled inside the transaction. The raw migration reports
stayed out of git, as the first run's did; their numbers entered `plan/POC.md`.
`plan/POC.md`, `plan/OPEN_QUESTIONS.md`, `plan/PLAN-STAGE-7.md`, `plan/PLAN-STAGE-8.md`

## The environment-derived DuckDB limits and the approvals of 2026-09-24

On 2026-09-24 the user pointed to <https://duckdb.org/2024/07/09/memory-management>, asked for more
recent articles, said they had had to set `memory_limit` in other projects to avoid running out of
memory, and instructed: the target may have a different memory size, so no specific memory or
thread value is fixed in code; the limits are chosen from what the environment has available. This
replaces the stage 4 decision of 2026-09-22 that left `memory_limit` at DuckDB's default. The
assistant added `serialize_db.resources` (`available_cpus`: affinity capped by the cgroup CPU quota,
rounded up; `available_memory`: the smallest of physical memory, `MemAvailable` and the cgroup room,
v1 and v2, the tightest ancestor winning) and `engine.duckdb.environment_limits` (`threads` = the
CPUs, `memory_limit` = half the memory still available, in MiB), which the engine applies when
`DuckDBConfig` omits them and the migration script applies at every connection, one per table. The
half is the assistant's choice from DuckDB's out-of-memory guide (50% to 60% when the OS kills the
process) and the measured 13% to 21% overshoot; the `cad_lancamentos` rerun confirms it
(`plan/OPEN_QUESTIONS.md`). The same day the user asked the threads probe for a round at half the
machine's cores beside the 1x to 5x rounds; said the terminal output of the `cad_lancamentos` run
was not kept but `Killed` showed several times in that part, the kernel killing the process for
lack of RAM; approved the three proposals (`register` as the default in stages 4, 5 and 7 with
`rewrite` only in the stage 5 swap for a non-finite `Double` partition, the load sorted by
`sort_key`, the stage 8 temporary staging filled inside the transaction after the control row
read); and merged PR #68. Whether `rewrite` leaves stages 4 and 7, with the `export_mode` flag and
its tests, was put to the user (proposed: remove it). `plan/PLAN-STAGE-4.md`, `plan/PLAN-STAGE-7.md`,
`plan/PLAN-STAGE-8.md`, `plan/POC.md`, `plan/OPEN_QUESTIONS.md`

## The rewrite leaves stages 4 and 7, and the engine page enters the pdoc site (2026-09-24)

On 2026-09-24 the user answered "sim, tire o rewrite das etapas 4 e 7" and asked for the pdoc fix,
after merging PR #69. The DuckDB engine's `export_partition` registers the `COPY` file only; the
`ExportMode` type, the `mode` argument of the `Engine` protocol, `Execution(export_mode=...)`,
`publish(export_mode=...)`, `serialize-db run --export-mode` and `SERIALIZE_DB_EXPORT_MODE` left
with their tests; `delta.publish_partition` stays for the stage 5 swap of a partition with a
non-finite `Double`, which now always logs its warning because no mode asks for the registration.
The migration script lost `--mode` and the `rewrite` measurement variants: it loads by the
registration and measures each partition with and without the sort (`SORT_VARIANTS`); the plan
text had proposed keeping the `rewrite` variants until stage 7 absorbs the script, and the
assistant removed them to match the user's answer. `serialize_db.engine.__all__` lists `duckdb`,
because pdoc documents only the submodules a package's `__all__` names, and the site had no engine
page since stage 4; `tests/test_package.py` checks every package. `plan/PLAN-STAGE-4.md`,
`plan/PLAN-STAGE-5.md`, `plan/PLAN-STAGE-6.md`, `plan/PLAN-STAGE-7.md`, `plan/PLAN.md`

## The battery of 2026-09-24 at 01:41 on a 16 vCPU machine

On 2026-09-24 the user reran every `SUITE.md` command from `main` (fa734eb, with #69) on a 16 vCPU
and 31,159 MB machine, handed over the reports and asked to analyze them and propagate the
revisions to the plan and the code. The assistant fixed `DuckDBConfig.threads` at the process's
CPUs by the probe's reading, as the plan had assigned to that run, closed the open items on the
`cad_lancamentos` migration, the half-memory fraction, the threads, the non-finite `Double` and the
Redshift audit text, and turned the audit readings into assertions; the migration reports stay out
of git. `plan/POC.md`, `plan/OPEN_QUESTIONS.md`, `plan/PLAN-STAGE-4.md`, `plan/PLAN-STAGE-7.md`

## REFRESH auto on the DuckDB secret and the archive by copy and registration (2026-09-24)

On 2026-09-24 the user asked for the pending decisions to be explained and approved both
recommendations: the DuckDB `credential_chain` secret of `storage.duckdb_setup` and of the
migration script is created with `REFRESH auto`, because it stores the credential resolved at
`CREATE SECRET` and the container's expires in about an hour (implemented the same day; a
connection crossing the rotation is still unmeasured); and stage 9's `archive` copies each
partition's files of the snapshot version with `Storage.copy` and registers them with
`register_files` on a table made by `create_table`, one commit per partition, in place of
`deep_copy` by `write_deltalake` over the whole table, whose memory grows with the table outside
DuckDB's limit (the DuckDB `COPY` rewrite stays with `export --mode rewrite` and compaction). The
`deep_copy` code changes when stage 9 starts. `plan/PLAN-STAGE-3.md`, `plan/PLAN-STAGE-9.md`,
`plan/OPEN_QUESTIONS.md`

## The implementation of stages 5 and 8 (2026-09-24)

On 2026-09-24 the user asked to implement stages 5 and 8 and answered three questions before the
work started: the publication's connection is an explicit `RedshiftConfig` given by the client,
`Execution(..., redshift=RedshiftConfig(...))`, and `publish_redshift` without it refuses with
`PublicationError` (the assistant's reading, stated in the report: the `"redshift"` engine
without the argument reads the `SERIALIZE_DB_REDSHIFT_*` variables and keeps that configuration
on the execution; `serialize-db run --engine redshift` passes it, and `--redshift` passes it to a
DuckDB execution); the commands of the new `redshift`-marked suites go in the report, and the user
adds them to `SUITE.md`; and the `redshift` extra pins `redshift-connector==2.1.17`, the version
installed in the development venv, whose source has the same `ps["row_desc"]` and `type_modifier`
read in 2.1.16. The assistant's choices, named in the report: the audit's `_publicado` staging
with every contract column, loaded only when the join runs (a positional `COPY` cannot load only
the key columns); the `INSERT` from the staging with an explicit column list; the loader's
temporary staging with `JSON_PARSE` for a table with a JSON column; the stream schema always from
the `row_desc` of `select * from (...) limit 0`; `PARALLEL OFF` up to 5,000,000 rows, unmeasured;
string min and max left out of the log for `UNLOAD` files; an empty partition registered through
an empty file the engine writes; and the stage 8 suite publishing in a `poc<id>` environment,
creating the control table only when absent and dropping it only in that case. `plan/PLAN-STAGE-5.md`,
`plan/PLAN-STAGE-6.md`, `plan/PLAN-STAGE-8.md`, `plan/OPEN_QUESTIONS.md`, `plan/POC.md`

## Stage 7 over the package and the measurement out of the script (2026-09-24)

After the first target battery of stages 5 and 8 (05:10 UTC), the user asked to analyze the
results, review the code, propagate the corrections to the plan, implement the remaining stages
where possible, review code, documentation and tests, and review the migration script, "which could
be based on this package". Two questions were answered before the work: the script uses the
package as `serialize_db.load` with the plan's primitives (`discover_partitions`,
`partition_query`, `initial_load`, `load_report`) plus `serialize-db load`, and the script stays
thin, keeping only what the plan leaves out of the package (the `--report` JSON with the machine
and the per-partition progress); and the measurement of the sorted and unsorted variants leaves
the script, because the 2026-09-24 battery measured the four `cad_lancamentos` partitions in both
variants and the `sort_key` order is decided (`plan/POC.md` keeps the numbers). The assistant's
choices, named in the report: the partition check before the `COPY` (no orphan file on a
contract refusal), `config` on `initial_load` and `load_report` for the tests' engine folder,
`--tables` instead of `--table` on `serialize-db load`, the root layout
`<root>/<environment>/<table>` of `Database` (the script gains `--environment`), one `initial_load`
call per partition in the script for the progress JSON, and the foreign-key audit left to
`serialize-db audit --foreign-keys` after the load. `plan/PLAN-STAGE-7.md`, `plan/CURRENT_STATE.md`,
`plan/OPEN_QUESTIONS.md`, `plan/POC.md`

## The routines print their measure (2026-09-24)

After the 16:51 battery ran the whole `archive` and the publication of the base with no duration
printed, the assistant proposed that `archive`, `publish`, `export` and `compact` print the time
and the peak RSS per table, like the migration script, and the user answered "Pode implementar a
duração e o pico de RSS por tabela." The assistant's choices, named in the report: the measure on
the CLI's printed line for `compact`, `archive` and `export` (`em <s> s; RSS máximo do processo
<MB> MB`, the script's format), on the per-table log line of `publish_redshift` for the
publication, whose CLI line is a summary printed at the end; `peak_rss_mb` in
`serialize_db.resources`, read from `_PROC / "self/status"` so the tests fabricate it, with the
script importing it; and, beyond the ask, the copy time of each partition on `deep_copy`'s log
line. `plan/PLAN-STAGE-8.md`, `plan/PLAN-STAGE-9.md`, `plan/OPEN_QUESTIONS.md`, `plan/POC.md`

## The arguments, returns and exceptions in the docstrings (2026-09-24)

On 2026-09-24 the user asked for a review of the project documentation covering every argument
of every function the pdoc site shows, and answered before the work: the arguments as reST field
lists at the end of the docstring (`:param name:`, which pdoc renders under the English heading
"Parameters"), with `:return:` and `:raises Exc:` for the return value and the exceptions; the
methods of the `Engine` protocol carry the full list again in `DuckDBEngine` and
`RedshiftEngine`, so each page reads alone; the dataclass constructors are documented by one
docstring per field, the existing pattern, with the missing ones completed, not by `:param` in the
class docstring; the options of each `serialize-db` subcommand go to `docs/operacao.md`; the
review covers `docs/index.md` and `docs/operacao.md` against the code; and no test enforces the
fields. The assistant's choices, named in the report: the public instance attributes of the
regular classes (`Execution`, `RedshiftEngine`) got docstrings as the dataclass fields did, and the
protocols' `(*args, **kwargs)` signature, the `__init__` of `typing.Protocol`, is explained in the
class docstring instead of documented; the argument paragraphs of the `Execution`, `DuckDBEngine`
and `RedshiftEngine` class docstrings moved into the `__init__` fields; a `ContractError` that
only a model outside the contract raises is listed where the function derives text or a schema
from the model (`serialize_db.schema`, `published_ddl`, `publication_statements`,
`reconcile_published`, `partition_query`) and left out of the functions that orchestrate; and a
third-party exception that only propagates stays out, except `duckdb.Error` where a failed
conversion or a missing extension surfaces (`initial_load`, `Storage.duckdb_connect`,
`DuckDBEngine`). The code divergences the review found wait on the user in
`plan/OPEN_QUESTIONS.md`. The same day the user asked for the standard in `CLAUDE.md`, which holds
it in "Python Code Style", section "Docstrings". `docs/operacao.md`, `docs/index.md`,
`plan/POC.md`

## The code review of 2026-09-24

On 2026-09-24 the user asked for a review of `src/` and `tests/` against the code rules and the
docstring standard of `CLAUDE.md`, with the refactors applied, and answered the assistant's
questions before the work: the tests reviewed are the package's (the `test_*.py` files of `tests/`,
`conftest.py`, `emulator.py`, `client_model/`, `lancamentos_model.py`, `source_db_projetado.py`),
with `tests/proof_of_concept/` and `tests/reference_model/` out; public names may be renamed, with
`docs/`, `plan/`, `README.md` and `SUITE.md` updated in the same commit; the refactor stays at the
function level, every module keeping its place; and a defect the review finds is fixed with an
assertion that fails on the old code and named in the report when small, and goes to
`plan/OPEN_QUESTIONS.md` when large. Under the last answer the review closed two pending items:
`check_models` lists a table with two partition columns instead of raising, and an empty
`SERIALIZE_DB_ENVIRONMENT` means `dev` in every subcommand, as it did in `run`, `audit` and
`publish`. The assistant's choices, named in the report: `Storage.create_text` in place of
`write_text(if_none_match=True)`; `bind(sql, params, dialect)`; `sql.bound_statement`, the helpers
the two engines repeated in `serialize_db.engine` (`batches_of`, `checked_batches`, `take`,
`ARROW_ONLY`) and the rows check reading in `audit` (`readings_by_partition`,
`failing_counters`); an `Exemplo:` block in every public function, method and exception; and
`import datetime` in place of `import datetime as dt` in the tests, where `dt` names a
`DeltaTable` as in `src/`. Kept on purpose: the name `dt` for a `DeltaTable`, the two loader
classes, which share their shape but not their storage, and the pytest `monkeypatch` of the
`driver_connect` seam. In the tests the review split every `assert A and B`, passed optional
arguments by name (`version=`, `limit=`, `prefix=`, `partitions=`, `seconds=`), renamed the row
builders of `tests/lancamentos_model.py` and `tests/test_engine_duckdb.py` to `entry_rows` and
`account_rows`, split the `--redshift` pipeline of `tests/test_execution.py` in two so each checks
its engine, and made `tests/test_source_db_projetado.py` read each file through
`parquet_source.footer_columns`. Kept on purpose in the tests: the `valor` keyword of the row
builders, a column name like a `.values(...)` keyword; the duplication across test modules (the
`storage` and `folder` fixtures, the model-key helpers of `test_client_model.py`,
`test_reference_model.py` and `test_schema.py`, the two `rewrite_first_chunk`, the `exit_code`
helpers), because a shared helper would move code between modules; and the `monkeypatch` of
`delta.open_table`, `delta.read_snapshots` and `Storage.copy`, the only way to put a commit or a
failure between two steps. `plan/PLAN-STAGE-1.md`, `plan/PLAN-STAGE-3.md`, `plan/PLAN-STAGE-9.md`,
`docs/operacao.md`, `plan/POC.md`

## The read access of stage 10 (2026-09-24)

The user asked to use the package also to mediate access to the base: a client that has the
model submits a SQLAlchemy `select` and gets an Arrow result, easily converted to pandas, from
the Delta base (a DuckDB database like the pipeline's, with some tables materialized and the
others views over the Delta files) and from the base published in Redshift. The assistant
proposed, and the user accepted, a reader object opened from `Database` and implemented in
`serialize_db.reader`, named `open_delta` and `open_redshift`; neither `Execution` (partition,
`execution_id`, snapshot, publication) nor a SQLAlchemy `Connection` (rows, and no
`cad_lancamentos` to `prod_cad_lancamentos` mapping); the same `query`, `stream` and `session()`
as the engines, `materialize` only on Delta. The user's answers of the same day: the two
sources serve different teams, and clients with read-only access to the published Redshift
base give their own `UNLOAD` destination; the latest snapshot is the Delta reader's default,
and another mode must read the current version; a partial materialization (a subset of
partitions under the model's name) is allowed. The assistant's proposals awaiting the user:
`created_at` in a sibling key of the snapshot control file, because the entries carry no date
and `_write_control` sorts the keys, so nothing tells which snapshot is the latest (A);
`db.open_delta(current=True)` for the current version, against `db.open_delta_current()` under
the boolean-flag rule (B); `serialize_db.reader.open_redshift(metadata, environment, config,
unload_to)` for the client without the Delta root, beside `Database.open_redshift(config,
unload_to=None)` (C); and reading an archived snapshot from its copy in
`arquivo/<name>/<table>` at the copy's current version, against refusing the name (D), because
the target's only snapshot, `carga-2026-09-24`, moved to `archived` in the 16:51 battery.
`plan/PLAN-STAGE-10.md`, `plan/OPEN_QUESTIONS.md`, `plan/POC.md`
