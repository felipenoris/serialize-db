# Lessons learned, with their stories

Read a story when the reason behind a rule in `CLAUDE.md` matters, or before adding a lesson. Process lessons from the sessions so far, each with the mistake that cost a retry or the verification that changed the plan, and its date; the rule distilled from each one lives in the "Working rules" section of `CLAUDE.md`, and the technical facts live in the theme files beside this one and in the study suites. A new lesson is appended here at the end of the unit of work, and its rule is added to `CLAUDE.md` in the same commit.

- **A documented behavior becomes a test assertion only after a probe reproduces it**
  (2026-09-19). Three claims taken from `plan/duckdb.md` and `plan/delta.md` failed as assertions:
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
  `plan/serialize-db.md` followed from the plan request and was flagged as such; the `Text` rule
  proposed in `plan/OPEN_QUESTIONS.md` is marked as awaiting the user's confirmation.
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
  the plainest reading, mark the other in `plan/OPEN_QUESTIONS.md`, and grep the repository before
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
  `examples/`, with its literal values (the environment's identifiers masked since 2026-09-23), and make the probe and the suite repeat its calls instead of
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
  `to_pyarrow_dataset()` or half a second of other work avoids (`plan/POC.md`).
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
- **A memory that hits its ceiling is split, not trimmed** (2026-09-21). Four budget overruns in one
  session were paid with compressions of a file loaded whole into every session, and the user asked for
  the split: `CLAUDE.md` keeps the index, the working rules, the conventions and where the work stands,
  and `.claude/memory/<theme>.md` keeps the facts and the stories, read on demand. A fact found in a
  session goes to its theme file, and `CLAUDE.md` changes only when a rule or an index entry changes.
  The same session found a zsh trap: `====` as an `echo` argument is `=command` expansion and fails with
  "not found", so a separator is quoted or made of hyphens.
- **A state change is confirmed by the effect the caller depends on** (2026-09-21). `RS-19`
  confirmed the `USE` by `select current_database()`, which stayed `dev` in the target while the
  examples' `CREATE`, `COPY` and `UNLOAD` by two-part name had passed after the same `USE`; the probe
  failed a working environment and gated `RS-5` and `RS-8` behind the wrong verdict. The user
  confirmed the reading. The check now selects from a table `svv_all_tables` lists, the library's
  `connect` creates the control table with `IF NOT EXISTS` as its confirmation, and the function's
  value is a reading.
- **`secrets/` is read only when the user names a path inside it** (2026-09-21). The probe reports
  of the target landed in `secrets/probes-aws-bn/` and the user asked for their analysis: the folder
  was read, nothing else under `secrets/`, the facts went to `plan/POC.md`, and copying the reports
  into `plan/readings/` was left to the user.
- **A plan revision reads every stage against the decisions memory** (2026-09-21). The full review
  found `plan/PLAN.md` saying the initial load rounds `Double` by `pc.round(x, 2)` two sections away
  from the premise that says the opposite (decision of 2026-09-20), `plan/guia.md` still calling the
  month the unit of write, a `Database` example without the `MetaData` the primitives need, and
  signatures (`publish_partition`, `export_partition`) missing the arguments their callers must pass.
  Grep the plan for the old rule's vocabulary when a decision lands, and read the API tables against
  the flows that call them. It happened again on 2026-09-22, with the stage 4 decisions: the grep for
  `memory_limit` ran while researching the decision, not after it landed, so `plan/PLAN.md` kept "o
  motor DuckDB nasce em arquivo, com `memory_limit` explícito" and the stage's acceptance criterion
  kept "o pipeline de exemplo roda em memória"; the `serialize_db.errors` row of the same table, which
  indexes the library's exceptions, missed `SandboxError`. The grep runs after the decision, and the
  tables that index names are read with it.
- **A shared connection's transaction mode is set before its first statement, and a report that
  counts failures records their messages** (2026-09-21). The first run of the Redshift suite in the
  target passed 1 test and failed 10. `connect_redshift` ran `USE` before the fixture switched
  autocommit on; `redshift_connector` had already issued `begin transaction` (it does so before the
  first `execute` when autocommit is off, and switching autocommit on later leaves that transaction
  open), so the whole session ran in one transaction. The denied `stv_slices` read (42501) aborted
  it, and every later statement, the `CREATE` of eight tests and the eleven `DROP` of the cleanup,
  failed with 25P02. The JSON said "10 failed" and nothing else, and the first attempt lost the JSON
  because the report folder did not exist. Autocommit now comes before the `USE`, the cleanup rolls
  back first, the report records each failure's message and creates its folder, and a fake driver
  fixes the order in `tests/test_conftest_redshift.py`. The terminal output the user pasted supplied
  the two facts the JSON lacked: the passing test was `fetchmany`, and the first test failed on
  `has_schema_privilege` answering `false` after the `USE`.
- **The branch is read before every commit** (2026-09-21). The user merged PR #38 and synced the
  local checkout to `main` between two turns; the next two commits (`8fa25a3`, `dd5e19c`) ran
  `git add -A && git commit && git push` on `main`, reached `origin/main` without a PR, and
  `gh pr edit` rewrote the description of a merged PR. Before each commit: `git branch --show-current`
  and `gh pr view --json state,headRefName`; a merged PR means a new `claude/` branch, and a `push`
  never goes to `main`.
- **The suite repeats the example's calls, joins of URIs included** (2026-09-21).
  `examples/redshift_manifest.py` built each manifest URL from its own base string; the suite built
  it from `DeltaTable.table_uri`, which ends with a slash, and every `COPY ... MANIFEST` of the
  second run in the target failed with `File not found` on `…/operacoes//mes=…`, a key that does
  not exist. A local probe showed the trailing slash and the unencoded `=` before the fix; the suite
  now strips the slash, decodes the path and records the first URL of each manifest, so the next
  report shows the exact key it asked for.
- **A statement repeated on one connection is prepared again after any DDL, and a driver's cache is
  read in its source before a retry is designed** (2026-09-21). The third and fourth runs of the
  Redshift suite in the target passed 10 and failed 1, the same test both times:
  `test_copy_column_list_and_fillrecord` repeats `TRUNCATE`, `COPY` and `select count(*)` per
  attempt, and the third round's count got `34510`, `Concurrent DDL committed ... between Prepare
  and Execute`. Reading `redshift_connector/core.py` gave the cause: a named prepared statement per
  SQL text, reused with `Bind` and `Execute`, and a cache cleared only on `ALTER`, `CREATE`, `DROP`
  and `ROLLBACK`; the count was parsed in the second round and the third round's `TRUNCATE`
  invalidated it in the datashare. `max_prepared_statements=0` makes the driver parse the unnamed
  statement before every execute. The `FILLRECORD` reading was lost because the count came before
  `record`: a loop that repeats a statement records each outcome before the next step that can
  fail. The same source reading answered the `fetchmany` question: `execute` reads every row into
  `cursor._cached_rows`.
- **A model's identifiers are read against the engines' keyword lists before SQL text is generated
  bare** (2026-09-21). The stage 1 draft rendered its DDL on an example model whose column names were
  all safe, and the plan sent the `{prefix}` sentinel out without quotes on purpose. The client model
  has `to` in `cad_contratos` and `timestamp` in `cad_lancamentos`; `duckdb_keywords()` classifies
  `to` as `reserved` (`CREATE TABLE t (to VARCHAR(2))` is a parser error) and Redshift reserves both,
  which the user's `examples/redshift_manifest.py` had already noted with `"to"` between quotes. The
  review of stage 1 found it by querying `duckdb_keywords()` with every table and column name of the
  model; the rule is that every identifier the library emits is double-quoted, and a new model's names
  are read against the two keyword lists.
- **A draft calls every input kind its signature accepts** (2026-09-21). `cast` promised
  `pa.Table`, `pa.RecordBatch` and `RecordBatchReader`, and the draft that ran on 2026-09-21 called it
  only with a batch. The reader path derives its output schema from `reader.schema.empty_table()`, and
  `pc.all` over that empty column returns null, so the first call with a reader was refused as a
  timestamp with a time of day; `min_count=0` fixed it. The rewrite in the module's shape exercised
  the three kinds and found the defect the dense draft had hidden.
- **A generated file is generated twice and diffed before a test asserts it equal to its versioned
  copy** (2026-09-21). `schema_files` wrote `tests/client_model/schema/*.delta.json` from delta-rs
  `Schema.to_json()`, the files looked right, and the test that regenerates and compares failed on
  the first run: delta-rs serializes each field's metadata map in arbitrary order, and two
  generations of the same model differed (`{"parquet.field.id":1,"comment":...}` against
  `{"comment":...,"parquet.field.id":2}`). The canonical dump (`json.dumps` with `sort_keys=True`)
  is what the client versions.
- **`uv` resolution needs every configured index reachable** (2026-09-21). `pyproject.toml` carries
  the corporate GitLab index as an extra `[[tool.uv.index]]`; outside the corporate network `uv add`
  and `uv lock` fail with a DNS error on it, even though every package comes from PyPI. An empty
  configuration file (`UV_CONFIG_FILE`, or a `uv.toml` beside `pyproject.toml`) makes `uv` ignore
  the whole `[tool.uv]` section; the workflows used `.github/uv-ci.toml`, and the local lock was made
  the same way, with the index block restored afterwards. The user removed the index from
  `pyproject.toml` later that day, and the workaround left with it.
- **A schema decision is read back through every reader the pipeline uses, with a file from each
  writer** (2026-09-21). Stage 1's `delta_schema` passed the Arrow schema with `PARQUET:field_id`
  to `DeltaSchema.from_arrow`, which keeps it as `parquet.field.id` in the Delta field metadata;
  the stage 1 draft read the Delta schema only through delta-rs, and the proofs of concept
  registered files on tables created by `write_deltalake` from schemas without ids. The migration
  script's report was the first `delta_scan` over a table created by `delta_schema`: every column
  came back null, with the DuckDB `COPY` file and with delta-rs files with and without ids, while
  delta-rs and `read_parquet` read the values; a schema without `parquet.field.id` read the same
  three files correctly (DuckDB 1.5.5, delta extension `45c4087`). `delta_schema` now drops the
  key, the versioned `.delta.json` files lost it, and `tests/test_schema.py` asserts its absence.
- **A validated instrument is refactored against its own report** (2026-09-21). The code review
  found `probes/parquet_source.py::read_footer` at 81 lines and six levels of nesting, and
  `text_lengths` at five: the deepest functions in the repository, in a probe that had already read
  the development base and the production base. The probe takes a local folder, so
  `tests/source_db_projetado.py` wrote the fixture base to the scratchpad and the probe ran over it
  with `--sample 5 --text-bytes` before and after the split, 758 lines of report each time,
  identical but for the timestamp and the output path. The fixture carries what the report needs to
  exercise: `INT96` without statistics, Hive partitions, the `pandas` footer key in part of the
  files and non-ASCII text columns. `probes/redshift.py::session` got no such refactor: it needs the
  target environment to run, and the user refused the split the same day
  (`.claude/memory/decisions.md`).
- **Control flow never rides on process-wide state** (2026-09-21). The stage 2 draft turned the
  `SAWarning` of a `bindparam` rendered as `NULL` into an exception with `warnings.catch_warnings`,
  which swaps the interpreter's warning filter for the duration of the block; the `warnings`
  documentation calls it unsafe in a concurrent program below Python 3.14's
  `context_aware_warnings`, the project runs 3.13, and the engines of stages 4 and 5 call `render`
  from helper threads. The review read the compiled statement instead: without `literal_binds`,
  `compiled.binds` lists the parameter with `required=True`; with it, the list is empty and the
  text carries `NULL`. Two compilations, no global state. `plan/PLAN-STAGE-2.md`, `plan/POC.md`
- **A verdict measured on a draft is measured again after a decision changes the draft's output**
  (2026-09-22). The SQLGlot trial of 2026-09-21 read a `ParseError` on the generated text with the
  sentinel, when the draft's prefixed copy used `quote=False` and the sentinel stood bare
  (`{prefix}cad_contas`). The `quote=True` decision of the same day put the sentinel inside the
  quotes of an identifier, and the verdict survived unchanged into `plan/PLAN-STAGE-2.md`,
  `plan/POC.md`, the decisions memory and the user's decision of 2026-09-22 to parse only the text
  with the prefix empty. The first run of `tests/test_sql.py` parsed the versioned files with the
  sentinel. The test now parses the versioned file of every statement, and the three documents
  were revised in the same commit. `plan/POC.md`
- **A conversion rule is probed with every input type and parameter that reaches it** (2026-09-22).
  `cast` measured text only when the input column was `string` (`pa.types.is_string`), which is
  false for `large_string`, the type `pa.Table.from_pandas` gives the pandas 3 `str`, and for
  `string_view` and dictionary; a pipeline in pandas with the numpy backend would have passed text
  above `String(n)` to the Redshift `COPY`, which the byte measure existed to prevent. The same
  review found the integer detour `decimal128(p + 3, s)` right only for `Numeric(18, 2)`: the cast
  needs the precision of the whole integer type (19 digits plus the scale for `int64`), and
  `p + 3 = 21` was a coincidence of the one case measured. Both rules were written from a single
  measurement and asserted on it. The fix measures text after the conversion to the contract type,
  and the tests feed each input type the pandas paths produce. `plan/POC.md`, `plan/PLAN-STAGE-1.md`
- **A comparison between two designs runs both under the same conditions** (2026-09-22). The first
  probe of the single session put the new design on a file-backed DuckDB database, the stage 4
  default, against the cursor sketches of `test_parallel.py` on an in-memory one, and read the
  three-stage pipeline six times slower (0.699 s against 0.110 s). Profiling the parts showed the
  `INSERT` into a file database dominating; on the same database kind the single session with
  Arrow IPC files was faster on a file (0.400 s against 0.565 s) and 20% slower in memory. A
  timing that decides a design is taken best of three, on the same database, the same data and the
  same batch size, for every alternative. `plan/POC.md`
- **A library's warning is a reading, never a guard** (2026-09-22). `plan/sqlalchemy.md` said since
  2026-09-19 that a valueless `bindparam` and `text("mes = :mes")` both render `mes = NULL` under
  `literal_binds` with a `SAWarning`, and the stage 2 draft of 2026-09-21 turned that warning into
  an error. The review of the study suites probed seven forms: the warning fires only for the two
  `=` comparisons (`coluna = :mes`, `upper(coluna) = upper(:mes)`), and `LIKE`, `coalesce`, a
  `select` column, the `VALUES` of an `INSERT` and `text()` render `NULL` silently, while
  `compiled.binds` marks the parameter `required` in all seven. The warning-based guard would have
  let five of seven through, and the claim about `text()` had been generalized from the one case
  the draft ran. `render` already read `compiled.binds` for thread safety; the documents now give
  the second reason. `plan/POC.md`, `plan/sqlalchemy.md`, `plan/PLAN-STAGE-2.md`
- **A requirement is measured in the user's own words before it is reported kept** (2026-09-23).
  On 2026-09-22 the user asked that the client work on the next or previous batch while the
  connection does I/O, and the single-session design was reported as keeping the requirement:
  the client's work overlapped the reading of the spool file, but the query ran whole before the
  first batch, so the connection's I/O never overlapped the client. The pipeline measurement had
  no client work in it, and the design read faster than the cursors. The user's next question
  ("did the first batch arrive differently before?") exposed it; timing the first batch and the
  total with 2 ms and 5 ms of client work per batch gave 0.472 s against 0.003 s and 1.330 s
  against 0.842 s, and the stream now writes each batch while the query runs (0.005 s and
  0.939 s). A requirement that names what overlaps with what is timed on exactly that overlap,
  with the work it names, and a design that keeps it in a weaker form says so in the report.
  `plan/POC.md`
- **A primitive is measured in the documented usage, with every command its implementation runs**
  (2026-09-23). The reference `Loader` of `test_parallel.py` wrote into tables the tests created
  beforehand, so the three-stage pipeline measured on 2026-09-22 and 2026-09-23 never ran the
  `CREATE TABLE` that the stage 4 `loader` runs under the session lock at open. In the order of the
  plan's own monthly example, `with stream(...), loader(...)`, that command waits for the whole
  query of the stream: over 20,000,000 rows the first batch came at 0.811 s instead of 0.006 s,
  and the requirement reported kept on 2026-09-23 held only with the loader opened first. A sketch
  that stands in for a primitive runs every command the primitive's plan lists, in the order the
  documented usage opens them, before its timings back a requirement. `plan/POC.md`
- **A claim that a type round-trips is probed with the type's special values** (2026-09-22,
  2026-09-23). The stage 3 decision registered min and max of the four types "that transcribe
  exactly", measured with finite doubles; `NaN` stays out of the maximum in both Delta writers,
  and `delta_scan` then answers a range filter by the pruning, while infinity becomes `Infinity`,
  invalid JSON, through `float`. The same `NaN` makes the audit's control total fail with
  `ConversionException`, a check nobody had fed a special value. Before calling a type exact or
  safe, feed it `NaN`, the infinities, null, the empty value and the longest value.
  `plan/POC.md`, `plan/PLAN-STAGE-3.md`
- **A statement path is probed with every clause form the plan writes** (2026-09-23). The engines'
  compile path was probed on 2026-09-22 with `=` and `LIKE`, and an `IN` list compiles there as
  `__[POSTCOMPILE_...]`, which DuckDB refuses; the `delta_scan` pruning was read on `=` and
  `BETWEEN`, and the stage 4 `ingest` wrote `IN`, which opens every file. Grep the stage files for
  each clause form that reaches a path (`IN`, `NOT IN`, `OR`, expanding parameters, table
  functions) and probe each one, reading pruning through the files the engine opens.
  `plan/POC.md`, `plan/PLAN-STAGE-4.md`
- **A concurrency test is repeated before it is trusted, and its failure paths are read** (2026-09-23).
  The hybrid `stream` passed its eight cases on the first run; repeated six times, it left the spool
  file of an abandoned stream in three runs, because the file is born mid-query and can appear after
  `__del__` unlinked the path. The `Loader` that refuses an occupied name at open printed an
  `AttributeError` from `__del__`, which read an event the failed `__init__` never created, only as
  a warning in the suite's output. Run a new test of threads or finalizers several times in a row,
  read the warnings the run prints, and give every field a finalizer reads a value before the first
  line of `__init__` that can raise. `plan/POC.md`
- **A statistic is probed at every layer that prunes and in every position of the file, and a
  standard is read in its current text** (2026-09-23). The first `NaN` probe read only the Delta log
  over one-row-group files, and missed that DuckDB's Parquet reader prunes by the footer of the
  delta-rs and pyarrow writers, so removing the log statistics alone keeps the defect, and that
  `has_nan` sees only the last row group, which sank the proposal built on it. The question itself
  came from PARQUET-1246, a Java reader fix of 2018, while the spec's rule dates from 2022 and
  changed in May 2026. Put the special value in the first, a middle and the last row group, read it
  through the log and through `read_parquet`, and quote the spec file at its current commit.
  `plan/POC.md`
- **A path chosen by a quantity needs the quantity before the path runs** (2026-09-23). The stage 5
  plan switched `stream` from `fetchmany` to `UNLOAD` above a row threshold and waited on a
  measurement in the target to fix it; the row count exists only after the `execute` that already
  materialized the whole result in the driver, so no threshold could ever be applied, and the
  measurement would have sized a rule that cannot run. Before measuring a threshold, write down
  where the quantity is read and whether that happens before the choice. `plan/PLAN-STAGE-5.md`
- **"Empty by construction" is read against the caller's loop and the rerun** (2026-09-23). The
  stage 5 `UNLOAD` destinations were empty only on the first call: `rewrite` sent every partition to
  `staging/<execution_id>/<tabela>/`, which the first partition of `run.publish`'s loop fills, and
  `register` reused `<uri>/<execution_id>/<valor>/` on the rerun with the same `execution_id` that
  stage 6 supports, while the DuckDB `register` already carried a `uuid` for that rerun. Read a
  uniqueness claim against the loop that calls the primitive and against the rerun, and end every
  write destination with a segment new per call. `plan/PLAN-STAGE-5.md`
- **A checkout shared with another session is read from git before a branch or a commit**
  (2026-09-23). Two sessions worked in the same folder: while one discussed the stage 6 decisions,
  the other created `claude/nan-estatisticas-parquet-delta`, committed and opened PR #60. The first
  ran `git checkout -b claude/decisoes-etapa-6` trusting its opening snapshot, which said `main`, so
  the new branch started from the other session's commit, and the switch moved the other
  session's `HEAD` too: a commit of it in those two minutes would have landed on the wrong branch.
  `git status -sb` and `git reflog` showed what had happened; the empty branch was deleted, and the
  commits went to the open PR's branch, as the git rule asks. Three sessions then edited the same
  plan files in turn, each one waiting for the previous one's message, adding files by path and
  leaving alone the files another session had modified. Read `git status -sb` and `git reflog -5`
  before creating a branch or committing, and agree by message on the order of edits to shared
  files. `CLAUDE.md`
- **A test only the target can run is first run against a local stand-in** (2026-09-23). The six
  stage 5 readings for the Redshift suite can run only in the target, where a run is rare. A
  throwaway emulator (DuckDB in place of Redshift, with `UNLOAD` and `COPY` translated to
  `read_parquet` and `pyarrow`, and a folder in place of S3, through a fake `boto3` client) ran
  them before the commit and found a case that could never show what it was written for: the `NaN`
  footer case filtered `valor > 2`, which a footer maximum of 3.0 lets through, while the pruning
  loss of issue #59 appears only above the finite maximum (`valor > 3`). The same preparation found
  that the plan's guard for the literal path, `compiled.binds`, is empty under `literal_binds`,
  where a valueless `IN` list renders `IN (NULL)`: the guard walks the statement for
  `BindParameter.required`. Run a target-only test against a local stand-in first, and give the
  stand-in the target's contract where the test reads it (the emulator's first `description` and
  `row_desc` disagreed, a defect of the emulator, not of the test). `plan/POC.md`,
  `plan/PLAN-STAGE-5.md`

- **A shared venv is restored with every group** (2026-09-23). While implementing stage 3 in a
  checkout two other sessions used, the assistant pinned `boto3` in the runtime dependencies and ran
  `uv sync --group dev`, the restore command the venv rule named; the sync removed the 29 packages
  of the `docs` and `interactive` groups (pdoc, ipykernel) that the folder's venv carried, and
  `uv sync --all-groups` put them back. The rule now names `--all-groups`, what
  `prepare_offline.sh` runs.

- **A draft of package code leaves the study suites when its module lands** (2026-09-23). Stage 4
  was implemented from the sketches `SandboxEngine`, `BatchStream` and `Loader` of
  `tests/proof_of_concept/test_parallel.py`, and the sketches stayed. By the code review of the same
  day, `BatchStream` differed from `DuckDBStream` only in docstrings and wrapping, while
  `SandboxEngine.load` had drifted (`register` plus `CREATE TABLE AS`, where the engine goes through
  the loader), `SandboxEngine` kept `preserve_insertion_order` on, which the engine turns off, so
  seven twin tests timed a setup the engine never runs, and `test_stdlib.py` still stated the old
  partition rule. The user retired the drafts; the unique cases moved to the package tests.
  `CLAUDE.md`, `plan/PLAN-STAGE-4.md`
- **A promised behavior without a failing assertion can be false** (2026-09-23). The `DuckDBLoader`
  docstring and stage 4 said an abandoned loader deletes its spool file; the case moved from the
  sketch asserted only that the thread ended and no table appeared, and a direct check showed the
  file stayed until `cleanup`. The writer thread now deletes the file when it ends with an error,
  and the new assertion was run against the old code, where it fails. `CLAUDE.md`, `plan/POC.md`
- **A mask at the entry misses what nests** (2026-09-23). `record` masked the credential clauses
  of a string value; the Redshift suite recorded a dict whose `"unload"` entry was the `repr` of the
  `UNLOAD` outcome, an error text that can quote the command. The mask moved to the two outputs, the
  printed lines and the JSON text, and a session with fake credentials in a dict, a list and a
  string printed none of them. `CLAUDE.md`, `tests/conftest.py`
- **A parameter id is a pytest keyword** (2026-09-23). The skip hook selected a suite's tests by
  `marker in item.keywords`; the review parametrized a DDL test by dialect, and its `[redshift]` case
  was skipped as part of the Redshift suite, without a word. The hook reads
  `item.get_closest_marker(marker)`, and the selection of the 453 collected tests did not change.
  `CLAUDE.md`, `tests/conftest.py`
- **A default path that writes is a write** (2026-09-23). `test_temporary_folder_is_created_and_removed`
  checked the engine's own `mkdtemp` folder, created in the system temp directory, outside the root
  `SERIALIZE_DB_TEST_LOCAL_ROOT` authorizes; the test points `tempfile.tempdir` at the local root
  first. The same review found `test_stdlib.py` leaving `NO_PROXY` and `AWS_DEFAULT_REGION` in the
  process, because `monkeypatch.delenv` on an absent variable records nothing to restore.
  `CLAUDE.md`, `plan/POC.md`
- **Planned code is not speculative code** (2026-09-23). The code review reported the three
  `Database` prefixes of stages 5, 8 and 9 (`staging_prefix`, `publication_prefix`,
  `archive_prefix`) as code without a caller, against the style rule "nothing speculative", and
  offered to remove them. The user decided that code the plan assigns to a later stage stays: the
  rule covers code no stage plans. `CLAUDE.md`, `.claude/memory/decisions.md`
- **A correction of a failure path is proved by provoking the failure** (2026-09-23). The review's
  corrections to the S3 and Redshift suites change what happens when the target fails, and the
  stand-in (the moto 5.2.3 server for S3 and STS, DuckDB for Redshift) ran green before and after
  them: 35 passed, 1 skipped. Switches in the stand-in provoked each failure (no manifest after an
  `UNLOAD` that passed, `PARTITION BY` refused, A's `INSERT` refused, `pg_backend_pid` refused), and
  the old code ran beside the new one from a worktree: the old code recorded the missing manifest
  as an empty result and as `ok: sem manifesto linhas`, skipped the refused `PARTITION BY`, left A's
  aborted transaction open until its connection closed, and raised `TypeError` on the refused
  `pg_backend_pid`. A green run proves only the path without failure. `CLAUDE.md`, `plan/POC.md`
- **A compatibility the library claims is run against an instance of it** (2026-09-23).
  `storage.py` read `AWS_ENDPOINT_URL` "for an S3-compatible service" and passed it to boto3,
  PyArrow and delta-rs, while the DuckDB secret carried only the host: with DuckDB's default
  virtual-host style and TLS, it never reached an `http` endpoint or one at an IP, and no test ran
  it against such a service. The moto stand-in found it when the package's `s3` tests read through
  `Storage.duckdb_connect`; the secret now adds `URL_STYLE 'path'` and, for `http`,
  `USE_SSL false`. `CLAUDE.md`, `plan/POC.md`

- **A memory reading in a child process reads the child's own peak and counts from its base**
  (2026-09-23). The two memory tests of `tests/proof_of_concept/test_duckdb.py` passed on macOS and
  failed in a Linux x86_64 container under pytest: in the full session every scenario read the same
  peak as the whole table (935 MB, 1,117 MB with the local root; 1,120 MB in the target the same
  day), and the two tests alone read the direct reader at 195 MB and the spool at 199 MB against
  362 MB for the whole table, with the ceiling at half of it. On Linux a new process's `ru_maxrss`
  starts at its parent's peak (a child of a 524 MB parent read 524 MB, its `VmHWM` 9 MB), and the
  probe's base before any query was 161 MB under a parent that had imported the test module: the
  reader's reading was pytest's peak. Read through `VmHWM`, the process base after importing DuckDB
  and PyArrow and connecting is 92 MB, more than half of the 168 MB ceiling, so the spool passed
  with as little as 23 MB to spare; counted from the base, the table adds 243 MB, the reader 10 MB
  and the spool 37 to 45 MB. The macOS runs never exercised the parent's peak. `CLAUDE.md`,
  `plan/POC.md`
- **A difference the stand-in shows is read against the target's documentation before it is blamed
  on the stand-in** (2026-09-23). The first stand-in run of the stream case printed different rows
  for the backslash through the literal text and through the `UNLOAD`, and the difference went into
  `plan/POC.md` as DuckDB not treating the backslash as an escape. In the target the `UNLOAD` passed
  and wrote nothing: its literal treats the backslash as an escape, as the `UNLOAD` pages show by
  escaping a quote with `\'`, so the backslash the dialect had doubled reached the inner `select`
  unpaired and the filter matched no row. The suite asserted the manifest of every `UNLOAD` that
  passed, and an empty result writes none, so the test stopped there twice and lost four cases. The
  stand-in now reads Redshift literals with the backslash escape and writes nothing for an empty
  `UNLOAD`, the old code fails in it with the target's message, and the helper tells an empty
  result from a missing manifest by `pg_last_unload_count()`. `CLAUDE.md`, `plan/POC.md`
- **A predicate is probed on table rows as well as on constants, and a function with the type the
  generated DDL gives its argument** (2026-09-23). The Redshift audit's `is_finite`,
  `x NOT IN ('NaN'::float8, ...)`, was false for `NaN` on constants in the target and let the `NaN`
  of the planted table through: Redshift compared `NaN` as PostgreSQL on constants and as IEEE in
  the scan, so the count saw 1 of 2 non-finite values and the control total hit `NaN input (scale
  float to decimal)`. The same run refused `is_valid_json` on the `SUPER` column that the stage 1
  DDL gives a JSON column (42883), which `plan/POC.md` had marked [uncertain] without a probe of
  that type; the refusal took down every measure of the rows check. `CLAUDE.md`, `plan/POC.md`
- **A repetition in the same process measures the caches the first run filled** (2026-09-24). The
  threads probe took the best of three repetitions per configuration, each in a new process but the
  three in one: DuckDB's external file cache served repetitions two and three from memory, and the
  S3 read, 4.1 s against 1.9 s, showed only in the first. The moto request log settled it (3 `GET`,
  then none). Turn the cache off, or count requests at the source, before a best of N stands for a
  remote read.
- **A long-running script writes its report as it goes** (2026-09-24). The migration wrote its JSON
  at the end, and the process that ended in the largest partition of `cad_lancamentos` took with it
  the four-variant measurement the plan waited on. The report is now rewritten after each
  measurement and each committed partition.
- **The complement of a comparison with `NaN` is counted as total minus matches, never as the
  negation** (2026-09-24). In a Redshift table scan, the strict comparison with the infinities was
  not true for `NaN`, so the sum left it out, and its negation was not true either, so the count
  missed it; `count(x) - count(CASE WHEN finite THEN 1 END)` holds whatever the engine does with
  the comparison. A probe that reads a count also reads a count without rows: `count(*)` on
  `sys_load_error_detail` came back empty and stopped a probe section.
- **A default sized to the whole machine is wrong for a process that shares it** (2026-09-24). The
  migration kept DuckDB's `memory_limit` default, 80% of the memory DuckDB detects, in a parent
  process whose connection lived across all tables while measurement children opened their own
  80% instances; DuckDB's RSS passes its limit by 13% to 21% in a sorted `COPY` and stays at its
  peak until the connection closes (1,188 MB after `DROP TABLE`, 208 MB after `close`), and the
  kernel killed the `cad_lancamentos` load on a 15,786 MB machine. The user had met the same in
  other projects and asked for limits read from the environment; the engine and the script now
  take half the memory still available and the process's CPUs at each opening, and the script
  opens one connection per table.
- **A credential renewal is designed per client that holds a copy** (2026-09-24). The one-hour
  credentials item covered delta-rs, which resolves the chain per call, and the Redshift driver,
  which reconnects per command, and the review of the 2026-09-23 22:49 readings found the third
  copy: DuckDB's `credential_chain` secret, created by `storage.duckdb_setup` and by the migration
  script, stores `key_id`, `secret` and `session_token` at `CREATE SECRET` (`duckdb_secrets()`),
  while `RS-18` read the caller's credential expiring 46 minutes later and the script's load
  connection lives from the end of the measurement to the load report. DuckDB 1.5.5 accepts
  `REFRESH auto`, which the aws extension docs prescribe for credentials that expire; the user
  approved it on 2026-09-24. The target read on 2026-09-25 that only `httpfs` triggers it, never
  `delta_scan`, and the user chose that day the secret with `boto3`'s key, recreated by the DuckDB
  engine at each session entry.

- **A helper thread's contention is confirmed before the timed call starts** (2026-09-24). The GIL
  measurement started its busy thread and timed 200 `os.stat` at once; in one of four sessions the
  stats finished in 5.4 ms before the thread got the GIL, and the ratio assertion failed. The
  thread now sets an event on its first iteration and the caller waits for it (the rule
  "A concurrency test is repeated before it is trusted" caught it on the repetition).

- **A test over a fake connection runs where no credential exists** (2026-09-24). The six `local`
  cases of `tests/test_engine_redshift.py` that run `COPY` or `UNLOAD` through `FakeConnection`
  built the engine on a `RedshiftConfig` without `iam_role`, so `credentials_clause` asked `boto3`
  for a credential; the session's container exports `AWS_ACCESS_KEY_ID` and
  `AWS_SECRET_ACCESS_KEY`, the three pre-commit rounds passed, and the GitHub runner, with none,
  failed them with `SandboxError` in the first run of PR #71. The test config now carries
  `iam_role="default"`, the `boto3` path is exercised only by `test_credentials_clause_and_mask`
  with variables it sets itself, and the pre-commit rounds run with the `AWS_*` variables
  removed (the rule "Before a commit").

- **A test that uses a suite's fixture carries the suite's marker** (2026-09-24). The eight
  target cases of `tests/test_publication.py` reached `local_location` through their `target`
  fixture, which builds the DuckDB engine's `temp_directory` under the local root, and carried
  only `redshift` and `s3`: `pytest -m redshift` without the variable errors instead of skipping,
  and the first target run, whose `SERIALIZE_DB_TEST_LOCAL_ROOT` pointed at a folder the machine
  did not have, lost all eight in 1.2 s while the engine suite ran. The stand-in rounds had the
  variable set and never showed it. The cases carry `local` now, and `tests/conftest.py` refuses
  at collection a test whose fixture closure holds a suite fixture without its marker
  (`SUITE_FIXTURES`, `pytest_itemcollected`, `UsageError`), proved by a throwaway test.
- **A catalog function's result type is outside the contract until a run reads it** (2026-09-24).
  `test_connect_uses_share_database` recorded `current_database()` through the typed `query`, and
  the target described the column as `name` (OID 19), which the type map lacked: the reading
  failed the case before its last assertion, in both rounds. `NAME` maps to `string` now, the
  stand-in imitates the OID, and the case records the SQLSTATE of the missing relation too, which
  `name_in_use` had accepted without saying whether `42P01` or the message matched.
- **A single-request S3 call on a large object is bounded by the SDK's low-speed limit, and a
  routine that copies many files is proved resumable** (2026-09-24). The first `serialize-db
  archive` in the target copied three tables and died in `cad_lancamentos`: `Storage.copy` was
  pyarrow's `copy_file`, one `CopyObject`, and the AWS C++ SDK abandoned it after 3 s without a
  byte while S3 copied the 32-million-row file server-side (`curlCode: 28`); the suites never met
  it because their files are small, and the larger 2026-03-31 file had copied moments before, so
  the limit is about S3's response latency, not the size alone. The fix routes S3 copies through
  boto3's managed transfer (`UploadPartCopy` in 8 MiB parts). Running the new resume test against
  the old code showed a second defect the failure had exposed: `archive` skipped any destination
  table that existed, so the rerun would have printed "já no arquivo" for the half-copied
  `cad_lancamentos` and moved the snapshot entry to `archived`; `deep_copy` now skips only the
  partitions the destination registers and refuses a destination holding files outside the
  version.
- **A routine the runbook asks the operator to size prints its own measure** (2026-09-24). The
  runbook told the operator to compare the machine's memory with the routine's measure on the
  largest table, and the 16:51 battery ran the whole `archive` (21 files) and the publication of
  the 12 tables in the target without a single duration or memory reading, because only the
  migration script printed them. The user approved the proposal the same day: `compact`,
  `archive` and `export` print the time and the process's peak RSS per table in the script's
  format, the publication carries them on each table's log line, `deep_copy` logs each
  partition's copy time, and `peak_rss_mb` moved from the script to `serialize_db.resources`.
  The reading in the target is still pending.
- **A documentation convention is checked in the rendered page as well as in the source**
  (2026-09-24). The `:param`/`:return:`/`:raises` convention was checked by a script over the
  docstrings (a field per argument, every old code span kept, the code unchanged), and the model
  module written by hand passed it with broken fields: pdoc 16 reads a `:param` or `:raises`
  name up to the last colon of the field's first line, so `:raises ValueError: a URI fora da
  raiz: as primitivas` rendered a bold "ValueError: a URI fora da raiz", and ``:memory:`` broke
  another field. An agent reading the built HTML found them, with 19 more in the engine files;
  the checker flags a second colon on a field's first line since.
- **`git reset --soft` leaves the whole diff in the index** (2026-09-24). To fold two document
  edits into the review's local commits, the assistant reset them with `--soft`, added a group of
  paths and committed: the commit took every staged file, the next two commits found nothing to
  add, and the split had to be redone after `git reset --mixed`. A diff is split into commits from
  an empty index, and `git diff --cached --stat` is read before each commit.
- **A test double of a library function takes the arguments the callers may pass by name**
  (2026-09-24). The review turned the optional arguments of `delta.register_files` into keywords
  at the call sites, and the fake of `test_interrupted_load_resumes`, which took `*arguments`,
  failed with `TypeError` in the local suite. Before changing how a function is called, grep the
  tests for `monkeypatch.setattr(<module>, "<function>"` and make each double forward `**options`.
- **An assertion that joins conditions hides one of them** (2026-09-24). The `--redshift`
  pipeline of `tests/test_execution.py` asserted `isinstance(run.sandbox, RedshiftEngine) or
  run.redshift is not None` right after asserting `run.redshift` a `RedshiftConfig`, so the engine
  of either option went unchecked; a review agent found it. The review split every
  `assert A and B` of the package tests as well, whose failure does not say which side broke.
- **A reading that runs statements on the connection it measures is part of the measurement**
  (2026-09-25). The first version of `probes/credentials.py` read each DuckDB count with
  `fetchone()`, which left the `read_parquet` query open; the next statement rolled it back
  with the secret it had refreshed, and the probe read the key never changing, while a
  script reading with `fetchall()` read the renewal. Four variants run side by side, each
  changing one step (`fetchone` or `fetchall`, the secrets query with or without a parameter),
  isolated the cause. When two instruments disagree, run variants that differ in one step, and
  consume every result the measured connection returns.
- **A statistic left out for safety is read back through every reader of the format**
  (2026-09-25). Stage 3 left the min and max of `decimal` and `timestamp` out of the log, because
  a wrong bound prunes the file that holds the row, and wrote that an absent statistic only stops
  pruning; no probe read an absent one through the delta-rs dataset. Verifying a `nullCount`
  finding of the comment review, a filter through `DeltaTable.to_pyarrow_dataset()` read 0 rows
  on a column without min and max: delta-rs turns each absent bound into the guarantee
  `column >= null`, and PyArrow skips the file. The package filters that reader only by the
  partition column, whose guarantee is the partition value, so no suite saw it. Read an omitted
  statistic through `delta_scan`, the delta-rs dataset and `read_parquet`, with a filter on the
  column itself. `plan/POC.md`, `plan/OPEN_QUESTIONS.md`
