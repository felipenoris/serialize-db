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
  the flows that call them.
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
