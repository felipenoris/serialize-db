# The environments

Read before running anything in the SageMaker space or the target, preparing the offline folder, or dating a measurement. The target Redshift's readings are in `redshift.md`; the lab's probe readings of 2026-09-20 are in `plan/POC.md`.

## The SageMaker Unified Studio lab, as observed on 2026-09-19

- Domain `dzd-<domínio do laboratório>`, project `eighth-experimentation` (`<projeto do laboratório>`), account
  `<conta do laboratório>`, region `us-west-2`, space `my-code-v4` (Code Editor, 4 vCPUs). The `sagemaker_studio`
  package's `Project()` gives `iam_role`, `kms_key_arn`, `s3.root` and `connections`.
- Credentials: the container endpoint (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`; `boto3` reports
  `container-role`) assumes `datazone_usr_role_<projeto do laboratório>_<ambiente do laboratório>`. `~/.aws/config` has a
  `default` profile with `credential_source = EcsContainer` and a `DomainExecutionRoleCreds` profile.
- Project bucket `awsds-sandbox-smus-projects`, prefix `dzd-<domínio do laboratório>/<projeto do laboratório>/`: `dev/`
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
  `plan/POC.md`, which also holds the lab-only readings (IMDS, the `~/shared` mount, Athena, the
  container credential's lifetime). This lab is not the target: the target has Redshift and no
  internet (user statement of 2026-09-20).
- The probes import `sagemaker-studio` from the system interpreter, because nothing unpinned enters the project venv
  (`lessons.md`).

## The target

The target is a sandbox, not production (user statement of 2026-09-23): the source files under
`databases/prd/db_projetado` are a copy of the production base, and the S3 bucket and the Redshift
schema are sandbox resources; `prd` in the path names the base copied, not the environment.
Redshift serverless `controladoria-wg` in `sa-east-1`, account `<conta>`, with no internet: the readings of
2026-09-20 and 2026-09-21 are in `redshift.md` and `plan/POC.md`, and the project library's
`readings/` (`/mnt/project-files/readings/`, `plan/readings/` until 2026-10-04) holds the masked
reports a pending stage consults and the source-base reading of 2026-09-21. The five probes of 2026-09-21
(03:47 to 03:51 UTC, Linux x86_64, Python 3.13.15, the project venv; reports in `secrets/probes-aws-bn/`,
outside git; interpreted in `plan/POC.md`) read the machine and the network: 2 vCPUs, 7.6 GiB, 29.8 GiB
free of 37.0 GiB on one disk serving `HOME`, `/tmp` and the repository, `ulimit -n` 65536; DuckDB
1.5.5 `linux_amd64` with 2 threads, `memory_limit` 6.1 GiB, `temp_directory` `.tmp`, extensions loaded
from the prepared `.duckdb/`; `uv`, `git`, `aws` and `duckdb` on the PATH, no `gh`, no `~/shared`, no
`~/.aws/config`; system Python 3.12.14 (`/opt/conda`) with deltalake 1.6.3, DuckDB 1.5.1, PyArrow
21.0.0 and `sagemaker_studio` 1.1.32 failing with `ProfileNotFound (DomainExecutionRoleCreds)`. No
proxy variable in any spelling and no internet (`Network is unreachable`); S3 names resolve public and
connect (gateway endpoint); interface endpoints for STS, the three Redshift APIs and the workgroup
host, Glue, Athena, Secrets Manager and DataZone; KMS (80 s), IAM (10 s), SageMaker, Lake Formation
(30 s) and S3 Tables (31 s) resolve public and time out. Container credentials (`container-role`)
lasting about an hour; `AWS_REGION` and `AWS_DEFAULT_REGION` both `sa-east-1`. Bucket
`bndes-aco-models-<conta>`: SSE-KMS with bucket key, SSE-C blocked, versioned by the sample, the
role denied the bucket-level reads as in the lab; the test root held one folder marker and no Delta
table. Glue has one database with one Parquet table, Athena three workgroups; Lake Formation and S3
Tables unreachable, so the re-evaluation trigger did not fire.

The five probes ran again on 2026-09-23 (19:18 to 19:19 UTC, from `main`, Python 3.13.15, the venv
prepared again with the `dev` group and moto 5.2.3): the space's instance changed to 4 vCPUs and
15.4 GiB, 29.7 GiB free of 37.0 GiB, and DuckDB 1.5.5 defaults to 4 threads and a `memory_limit`
of 12.3 GiB; the network, the credentials and the bucket read as on 2026-09-21. The IAM and KMS
reach tests failed their 2 s TCP test and skipped the calls, Lake Formation timed out in 60.6 s
and S3 Tables in 30.2 s, and `svv_table_info` is denied to the role after the `USE` (42501).
The early migration's reports show a process peak of 19,595 MB, more than this instance's RAM,
so the migration ran on a larger instance (`source-base.md`). `plan/POC.md`

The battery of 2026-09-24 at 12:38 UTC (from `main` with #72) ran on 16 vCPUs and 31,383 MB with
28,061 MB available, the same network and credentials (the caller's credential expiring in 36
minutes, the workgroup's in an hour), Lake Formation and S3 Tables timing out in 60.3 s and
30.4 s, and 907 non-current versions (32,966,477 bytes) with 859 delete markers under the test
root (`BK-14`). `catalog.py`, `bucket.py` and `space.py` exit with 1 because failed calls count,
all of them expected readings; no check failed. `plan/POC.md`

The battery of 2026-09-24 at 16:51 UTC (from `main` with #73, the root loaded anew) ran on the
same machine, 16 vCPUs and 31,383 MB with 28,074 MB available (Python 3.13.15, DuckDB 1.5.5,
deltalake 1.6.4, pyarrow 25.0.1, `sa-east-1`), and finished the whole `SUITE.md` flow: the load,
the audit, `history`, `snapshot`, `vacuum`, `archive` and the publication of the whole base;
`export`, `compact` and the threads probe did not run. `plan/POC.md`

The first target run of 2026-09-24 at 05:10 passed five of the six engine cases (`COPY ...
MANIFEST`, `UNLOAD`, the loader, the audit, the export by registration, the `NaN` swap) and failed
`current_database()`, described as `name` (OID 19), mapped to `string` since; the publication cases
did not run because the target's local root folder was missing (they carry `local` now). The battery
of 2026-09-24 at 12:38 passed the six engine cases and the eight publication cases twice each: the
Redshift `COPY` of DuckDB-written files, the `svv_all_columns` spelling, the `1023` through the
library as `ExecutionConflict`, the `EXPLAIN` of the join (`DS_DIST_ALL_NONE`), the missing relation
as `XX000` with `Relation <name> does not exist in the database.` (the stand-in imitates it since)
and the JSON column of the `UNLOAD` file as `VARCHAR` through `delta_scan`; the `PARALLEL OFF`
threshold of 5,000,000 rows stays an unmeasured choice. The load through the package ran in the
target on 2026-09-24 at 14:16 in the root layout `<root>/<environment>/<table>`: the 12 tables
matched, `cad_lancamentos` peaked at 16,198 MB under a 14,030 MiB limit, and the audit with
`--foreign-keys` read the known 989,852 orphans of 2026-01-31. In the target on 2026-09-24
`history`, `snapshot` and `vacuum` ran, and `archive` died in pyarrow's single `CopyObject` of a
`cad_lancamentos` file (the AWS SDK's 3-second low-speed limit): `Storage.copy` on S3 is boto3's
managed copy since, `deep_copy` resumes an interrupted copy and `archive` no longer skips a table
present in the archive. The battery of 2026-09-24 at 16:51, on the root loaded anew
(`cad_lancamentos` 19.9 s to 31.8 s per partition, peak 16,355 MB), ran the whole flow: `archive`
copied the 21 files of the 12 tables through the managed transfer and moved the entry, and
`serialize-db publish` (`--init`, `--tables cad_contas`, `--max-workers 4`, `--status`) put the 12
tables in Redshift as `prod_<table>`, `cad_lancamentos` at version 4 with 141,901,795 rows. The
battery of 2026-09-24 at 23:25, on a new root in `prd`, passed every suite case (S3 481, Redshift 44
twice, engine 6 and publication 8 twice each) and read them: `archive` of `cad_lancamentos` 11.1 s,
its publication 153.9 s at a 273 MB peak, the first `export` 8.8 s by copy and 17.3 s at 5,425 MB by
`--mode rewrite`; `compact` ran only on a one-file partition, which does not commit, so a real
compaction stays unread there (`plan/POC.md`).

The three target-only suites ran in the target on 2026-09-23 from `main`, at 18:48 and again at
22:53 (S3, 445 passed with the `VmHWM` memory measurements) and 22:56 and 23:01 (Redshift, 30
passed each): the stream with literals, the empty `UNLOAD` with `pg_last_unload_count()` 0, the
temporary table and the `ALTER COLUMN ... TYPE` refused on the share are assertions now. The
stage 8 transaction the user decided (read the control row first, `INSERT` or check and
`UPDATE` it last; the unpublish flow with `DROP TABLE` and `DELETE`) read `1023` for the second
of two publications, and both temporary stagings committed. The Redshift audit's strict
comparison left `NaN` out of the sum but its negation did not count it, so the non-finite count
is `count(x) - count(finite)` and waits for two runs, as does the reading
`nan_na_tabela_detalhe`; the stand-in `tests/emulator.py` has no locks, no bucket encryption, no
Data API and none of Redshift's `NaN` scan behavior. `probes/duckdb_threads.py` ran at 23:21, but
DuckDB's external file cache served its later repetitions from memory; it now turns the cache off,
measures half the CPUs too (user request of 2026-09-24), and its run of 2026-09-24 at 02:02 on
16 vCPUs fixed the default at the process's CPUs. `plan/POC.md`

The battery of 2026-09-25 at 17:25 to 19:37 UTC (from `main` of the day, the folder prepared
again: deltalake 1.6.6, boto3 1.43.102, `redshift_connector` 2.1.17 and sqlglot 30.19.0 by
`SP-9`) ran on 8 vCPUs and 15,505 MB (Python 3.13.15, DuckDB 1.5.5, pyarrow 25.0.1), DuckDB
defaulting to 8 threads and a 12.1 GiB `memory_limit`, and `environment_limits` giving 8 threads
and 6,227 MiB in the load (half of 12,454 MB available); the caller's credential expiring in 52
minutes (`RS-18`), 45 load errors in 30 days (`RS-12`), 2,980 non-current versions (86,695,363
bytes) and 2,788 delete markers under the test root (`BK-14`), Lake Formation and S3 Tables
timing out in 60.7 s and 30.3 s. Every suite case passed; `duckdb_threads.py` stopped at `DT-1`
because `SUITE.md` passed the root without `prd`, and `compact` refused the partition because a
snapshot pointed at the current version. `plan/POC.md`

The first part of the battery of 2026-09-26, the five probes and the seven pytest sessions from
15:14 to 15:47 UTC (from `main` of 2026-09-25 at 22:35 or later, inferred from the 576 cases
collected and `SP-9`), ran on 8 vCPUs and 15.3 GiB (Python 3.13.15, DuckDB 1.5.5, deltalake
1.6.6, pyarrow 25.0.1, boto3 1.43.102, `redshift_connector` 2.1.17), DuckDB defaulting to 8
threads and a 12.2 GiB `memory_limit`; the caller's credential expiring in 45 minutes (`RS-18`),
the `RS-12` count returning no row in 15.1 s, 5,269 non-current versions (159,538,248 bytes) and
4,883 delete markers under the test root (`BK-14`), Lake Formation and S3 Tables timing out in
60.1 s and 30.6 s. Every suite case passed: S3 531 in 219.7 s, the DuckDB secret's stale-key case
among them, Redshift 45, engine 6 and publication 8 twice each. The migration block followed from
15:55 on the same machine, read as 15,617 MB with 12,768 MB available, `environment_limits` giving
8 threads and 6,384 MiB; the process peaked at 9,161 MB loading the new 141,933,948-row partition
of `cad_lancamentos` (`source-base.md`). The publication block followed on the same machine, the
whole base by `--channel default` (`cad_lancamentos` 295.1 s at a 266 MB peak), then
`probes/credentials.py` from 16:15:59 to 17:19:00 with no read failing, the reader block (the
Delta reader's 12 views in 0.582 s) and the export block (`cad_lancamentos` by copy 15.6 s at
258 MB, by `--mode rewrite` 55.0 s at 6,989 MB; `compact` refused by the snapshot at the current
version); the consistency probes ran from 18:30:31 with every check passing, the Redshift one in
53.6 s, and `duckdb_threads.py` had not been sent by 18:35 UTC. `plan/POC.md`

The battery of 2026-09-27 from 15:58 UTC (from `main` of 15:47 UTC with the corrected probes,
inferred from the 541 cases of the S3 session) ran on 8 vCPUs and 15,617 MB (Python 3.13.15, DuckDB
1.5.5, deltalake 1.6.6, pyarrow 25.0.1, boto3 1.43.102, `redshift_connector` 2.1.17), DuckDB
defaulting to 8 threads and 12.2 GiB. The five probes differed from 2026-09-26 only in what changes
per run and in the corrected lines (the `pg_settings` label now naming `wlm_query_slot_count`, the
expiry read by `credential_expiry`); `RS-12` again returned no row, `BK-14` counted 7,619
non-current versions (233,165,927 bytes) and 7,030 delete markers, and Lake Formation timed out in
30.1 s over 3 DNS addresses. Every suite case passed: S3 541 in 226.7 s, Redshift 45, engine 6 and
publication 8 twice each. The load from 16:34 (12,547 MB available, 8 threads and 6,273 MiB) read
the source of 2026-09-26 unchanged in 496.8 s, peak 8,625 MB; the whole base was published by
`--channel default` (`cad_lancamentos` 328.5 s at 270 MB), the Delta reader opened in 0.571 s,
`export` took 14.7 s by copy and 56.7 s at 6,938 MB by rewrite, and `compact` refused; the
consistency probes ran from 17:33:03 with every check passing, `probes/credentials.py` from 17:35:23
to 18:38:25 with no read failing, and `probes/duckdb_threads.py` from 17:35:54 beside it, complete
(`duckdb.md`). `plan/POC.md`

The battery of 2026-09-28 from 20:14 UTC (from `main` with PR #104, inferred from the 662 cases
collected) ran only the "Probes e Testes - BN" block and the migration block, on 8 vCPUs and
15.1 GiB (the same versions as 2026-09-27), DuckDB defaulting to 8 threads and 12.1 GiB. The probes
differed from 2026-09-27 in what changes per run and in `RS-12` (85 load errors in 30 days),
`BK-14` (8,822 non-current versions, 270,369,639 bytes, and 8,127 delete markers) and `SP-10`
(passing with `httpfs`, `delta`, `parquet` and `json`, no `aws` listed). S3 passed 610 in 274.1 s;
Redshift 51 of 52 and engine 9 of 10 twice each, `test_appender_copies_the_file_at_close` failing
every time (`redshift.md`); publication 10 twice. The load from 21:14:56 (12,515 MB available,
6,257 MiB) stopped at `cad_lancamentos` 2026-07-31 on `RegistrationRefused` (`source-base.md`).
`plan/POC.md`

The battery of 2026-09-28 from 23:09 UTC (from `main` with PR #105, inferred from the report key
`redshift.engine.copy_missing_mandatory_file`, which only its test writes) ran the
"Probes e Testes - BN" block on 8 vCPUs and 15,505 MB (the same versions), DuckDB defaulting to 8
threads and 12.1 GiB, and on 2026-09-29 from 00:01 the migration, publication, read access and
export blocks, the consistency probes with the two-writer probe, `duckdb_threads.py` and
`credentials.py`. Every case and check passed: S3 610 in 271.4 s, Redshift 52, engine 10 and
publication 10 twice each, the `appender`'s missing mandatory file failing its `COPY`
(`redshift.md`). The probes differed from 20:14 only in `RS-12` (93 load errors in 30 days), `BK-14`
(10,301 non-current versions, 307,133,320 bytes, and 9,504 delete markers) and the container
credential's 32 minutes left. The load (12,042 MB available, 6,021 MiB) passed whole on the source
of 2026-09-27 in 514.3 s, peak 8,734 MB (`source-base.md`); the whole base was published by
`--channel default` (`cad_lancamentos` 335.2 s at 286 MB, 0.5% to 2.0% more per partitioned table
than on 2026-09-27, the footer reading), the Delta reader opened in 0.556 s, `export` took 15.6 s by
copy and 58.4 s at 6,902 MB by rewrite, and `compact` refused; `probe_append_test.py` passed in both
engines (`concurrency.md`), `credentials.py` ran from 00:31:00 to 01:34:02 with no read failing, and
`duckdb_threads.py` from 00:27:53 (`duckdb.md`). `plan/POC.md`

The battery of 2026-09-29 from 13:31 UTC (from `main` with PR #106, inferred from the sessions
collecting `test_two_writers_on_the_same_table_both_enter`) ran only the "Probes e Testes - BN"
block on 8 vCPUs and 15.1 GiB (the same versions). Every case and check passed: S3 611 in 281.8 s,
Redshift 53 twice (687.1 s, 613.5 s), engine 11 twice (188.0 s, 167.7 s) and publication 10 twice
(214.5 s, 209.4 s), each session but the publication's with one case more than at 23:09, the
two-writers test (`concurrency.md`). The probes differed from 23:09 in `RS-8` (1 of 17 tables with
the `serialize_db` prefix, `serialize_db_publications`, and in its listing a sandbox table of an
append probe run that stopped at 00:29, `redshift.md`), `RS-12` (88 load errors in 30 days) and
`BK-14`, whose listing stopped at its 20,000-entry limit (10,434 non-current versions, 283,570,979
bytes, and 9,565 delete markers, now a floor). `plan/POC.md`

The battery of 2026-09-29 from 17:04 UTC repeated the "Probes e Testes - BN" block with the same
versions, after the user dropped the leftover sandbox table by hand. Every case and check passed: S3
611 in 275.7 s, Redshift 53 twice (631.5 s, 625.4 s), engine 11 twice (165.6 s, 180.0 s) and
publication 10 twice (210.4 s, 204.9 s). The probes differed from 13:31 in the `RS-8` listing (16
tables, no `exec_` table), `RS-12` (100 load errors in 30 days) and `BK-14` (10,481 non-current
versions, 283,880,162 bytes, and 9,518 delete markers, the listing again at its limit).
`plan/POC.md`

The battery of 2026-09-30 from 14:58 UTC (from `main` with PRs #109, #110 and #111, inferred from
the sessions' counts) repeated the "Probes e Testes - BN" block on 8 vCPUs and 15.3 GiB with the
same versions. Every case and check passed: S3 614 in 274.9 s (the three `ConflictError` cases of
`tests/test_storage.py` new), Redshift 54 twice (748.9 s, 674.3 s), engine 11 twice (168.4 s,
169.1 s) and publication 11 twice (248.1 s, 236.7 s), the Redshift and publication sessions with
`test_redo_a_snapshot_and_revert_by_the_channel`, its first target run. The probes differed from
17:04 in `RS-12` (112 load errors in 30 days) and `BK-14` (10,460 non-current versions, 283,526,691
bytes, and 9,539 delete markers, the listing again at its limit); the `RS-8` listing held the same
16 tables, no `exec_` table. `plan/POC.md`

## The prepared folder and the venv

On 2026-09-19 `pyproject.toml` declared no runtime dependencies and pinned the `dev` group
(SQLAlchemy, duckdb-engine, sqlalchemy-redshift and pandas were added that day for the study
suites). The runtime dependencies are pinned in `[project]` since the package code, with
`redshift-connector==2.1.17` among them since 2026-09-25 (PR #95), and `prepare_offline.sh` must be
rerun whenever one is added.
DuckDB keeps its extensions per version, `.duckdb/v<version>/linux_amd64/`: DuckDB 1.5.6 installs
`delta` 6059958 and `httpfs` 4bc690d, where 1.5.5 installs `45c4087` and `827222f`, so a folder
prepared for one version has no extension for another until `prepare_offline.sh` runs again. The
pin went to 1.5.6 on 2026-10-03 and back to 1.5.5 on 2026-10-04: the package's clients install the
extensions from the PyPI wheels `duckdb-extension-delta` and `duckdb-extension-httpfs` (user
statement of 2026-10-04; the wheel's README installs one with `duckdb_extensions.import_extension`),
each version requiring the same exact `duckdb`, and the newest was 1.5.5, of 2026-08-10; with
`duckdb==1.5.6`, `uv pip compile` resolved the unpinned wheels to 1.0.3 without an error. A DuckDB
bump waits for those wheels. `plan/POC.md`, `plan/OPEN_QUESTIONS.md`

The suites exist so the same proof of concept runs in the target, without internet; the local suite
validates the prepared folder there (verified 2026-09-19: extracted at another path with dead proxies
and an empty `HOME`, 10 passed; on macOS after the glob fix of PR #12). `uv sync` ignores
`UV_VENV_RELOCATABLE` and the `.venv/bin/*` scripts keep absolute shebangs, hence
`.venv/bin/python -m pytest` and the `.tar.gz`, never zip, that keeps links and permissions.

## The environments of the measurements

Each document dates its measurements and pins their versions in its opening lines: `plan/parquet.md`,
`plan/duckdb.md` and `plan/sqlalchemy.md` on 2026-09-18, over 300,000 rows of `operacoes`
(`poc_delta.sample_table`), the Redshift statements compiled only; `plan/estrategia.md` on 2026-09-19;
the S3 proof of concept of 2026-09-19 (Python 3.13.15) in `plan/POC.md`. What they do not say: the
local proof of concept ran on macOS arm64 through `uv run --with` in the scratchpad, with 11 DuckDB
threads and the files in the page cache, nothing against S3 or Redshift; the Python examples added
to every document on 2026-09-19 ran under the same pinned versions.

## GitHub Actions (2026-09-21)

- The workflows in `.github/workflows/` run on `ubuntu-latest` with `astral-sh/setup-uv` pinned
  to an exact tag (the repository has no `v10` major tag; the first run failed on `@v10`) and
  Python 3.13. `tests.yml` sets `SERIALIZE_DB_TEST_LOCAL_ROOT` to a folder under the workspace and
  runs `tests/` without `tests/proof_of_concept/` and `tests/test_probes.py`; `docs.yml` builds
  the `pdoc` site and deploys it to GitHub Pages (`actions/configure-pages`,
  `upload-pages-artifact`, `deploy-pages`), which needs the Pages source set to "GitHub Actions"
  in the repository settings: without it `configure-pages` fails with `Get Pages site failed`
  (404), as it did twice on `main` on 2026-09-21. The tests workflow ran twice on PR #46 that day:
  the first run failed at `Set up job` (`Unable to resolve action astral-sh/setup-uv@v10`, the
  repository tags major versions only up to `v7`), the second passed the 105 package tests in
  about a minute with `@v10.2.0`. The corporate index left `pyproject.toml` the same day, and the
  `UV_CONFIG_FILE` workaround left the workflows with it. `plan/CURRENT_STATE.md`, `README.md`

The battery of 2026-09-23 at 22:49 to 23:36 UTC ran on the same kind of machine (4 vCPUs, 15,786 MB,
DuckDB `memory_limit` 12.3 GiB) with the venv without the `emulator` group (`SP-9`) and new roots
under `.../shared/<usuário>/serialize-db/`: `serialize-db-tests` for the suites and probes and
`delta/db_projetado` for the migration. The session container of this repository's cloud sessions
is also 4 vCPUs and 16,095 MB (30 GiB free), where DuckDB picks a 10.6 GiB `memory_limit`: local
reproductions of target memory behavior run on a comparable machine. `plan/POC.md`

The session container (2026-09-24) is cgroup v1 for memory: `/proc/self/cgroup` puts `memory` in a
folder of its own (`/process_api/<id>/claude-code-bash`) with `memory.limit_in_bytes`
14,345,912,320, the mount root unlimited, and a `0::/` v2 line with no controller; `MemTotal`
16,481,980 kB, no CPU quota, one thread per core (`lscpu`, `smt/control` `notsupported`).
`available_memory()` reads 14,197,641,216 bytes there (the cgroup room) and `environment_limits()`
gives 4 threads and 6,761 MiB. `plan/POC.md`
Shared memory counts in the cgroup's file cache and cannot be reclaimed: 256 MiB of shared `mmap`
in that container (no swap) raised `total_cache` and `total_shmem` by 256 MiB each, the old room
(limit − usage + cache) fell 2 MiB and the room since 2026-09-28 (minus `total_shmem` in v1,
`shmem` in v2) fell 274 MiB, as the usage rose 274 MiB; the kernel's cgroup-v2 text says `file`
includes tmpfs and shared memory. `plan/PLAN-STAGE-4.md`, `plan/POC.md`

The battery of 2026-09-24 at 01:41 to 02:19 UTC ran on a 16 vCPU (two per physical core) and
31,159 MB instance, DuckDB defaulting to 16 threads and a 24.3 GiB `memory_limit`,
`environment_limits` giving 16 threads and 13.1 to 13.6 GiB (half of the 27 to 28 GB available at
each opening), 29.7 GiB free of 37.0 GiB, the same roots under `.../shared/<usuário>/serialize-db/`
and `main` with #69; the whole migration finished there, `cad_lancamentos` peaking at 16,430 MB
under a 13.4 GiB limit with the sort faster than none at 16 threads (`source-base.md`). The threads
probe with the cache off fixed `threads` at the process's CPUs: materialization best there, worse
at half and at double, the S3 read 1.4x faster at triple (`duckdb.md`); the audit's non-finite
count and the `NaN` row became assertions (`redshift.md`). `plan/POC.md`

A new cloud session container (2026-09-24) starts without `.duckdb/`: the package tests with
`SERIALIZE_DB_TEST_LOCAL_ROOT` failed on the missing `delta` extension until the GitHub
workflow's command installed it (`duckdb.connect(config={'extension_directory': '.duckdb'})
.execute('INSTALL delta')`), and the stand-in's `s3` cases also need `httpfs` there. With
`delta`, `httpfs` and `aws`, the library's extensions of 2026-09-24, the local root gave 429
passed and 94 skipped, and the stand-in with the local root 522 passed and 1 skipped; since the
secret by `boto3`'s key of 2026-09-25 nothing loads `aws`, and on 2026-09-28 the stand-in passed
with `delta` and `httpfs` only. `README.md`

## The target batteries of 2026-09-25 and 2026-09-26

The battery of 2026-09-25 in the target (8 vCPUs, 15,505 MB, deltalake 1.6.6) passed every suite
case (S3 512, Redshift 45 twice, engine 6 and publication 8 twice each, with the stage 10 reader
and the publication by channel), loaded the base in 219.4 s and opened the Delta reader's 12 views
in 0.645 s. `probes/credentials.py` read there delta-rs, `S3FileSystem` and `boto3` renewing the
container credential, which rotates about every 30.6 minutes, the open Redshift connection
outliving its password, and DuckDB's `delta_scan` failing once after the key its secret holds
expired, since only `httpfs` triggers `REFRESH auto`. The user chose the same day the secret with
`boto3`'s key, which the DuckDB engine recreates at each session entry when it changes; the battery
of 2026-09-26 passed every suite case again (S3 531, the stale-key case among them), loaded the
source's new month 2026-07-31 (`.claude/memory/source-base.md`), published the whole base by
channel and read, in the probe, the engine's secret renewed before each expiry. `plan/POC.md`

## The Windows runner (2026-10-01)

- `tests.yml` runs on `ubuntu-latest` and `windows-latest` (Windows-2025Server-10.0.26100-SP0,
  Python 3.13.15, the workspace at `D:\a\serialize-db\serialize-db`), every step in Git Bash
  (`defaults.run.shell: bash`). The user ran a client project on Windows, where
  `import serialize_db` failed on `import resource`; Windows has neither `resource` nor
  `os.sysconf`, and `resources.py` reads `GlobalMemoryStatusEx` and `K32GetProcessMemoryInfo`
  through `ctypes` there. The third run passed 354 cases with 75 skipped. `plan/POC.md`
- What differs on Windows: PyArrow's `LocalFileSystem` lists with `/` (`D:/a/...`), so the local
  root goes through `as_posix()`; DuckDB's partitioned `COPY ... RETURN_STATS` joins the partition
  folders with `\` (`duckdb.md`); `Path.from_uri("file:///tmp/x")` raises `URI is not absolute`; a
  new file's mode reads 0o666; `os.path.join` joins with `\`; and `os.environ` uppercases the
  name, so the proxy's `username` variable reads the login's `USERNAME`
  (`plan/OPEN_QUESTIONS.md`). The job log is the only reading: on 2026-10-01 its download URL
  was refused by this container's proxy (403), and `get_job_logs` with `tail_lines` returns the
  end of it; on 2026-10-04, `get_job_logs` with `return_content: false` gave a `logs_url` that
  `curl -sS -o <file>` downloaded whole, once the job had finished (404 while it ran).
- On 2026-10-04 the user's `uv run python -m pytest` on Windows 11 (Python 3.13.3) stopped at
  collection: `tests/test_probes.py` imports `probes/duckdb_threads.py`, which imported
  `resource` at the top. The workflow never runs `tests/test_probes.py` nor
  `tests/proof_of_concept/` (decision of 2026-09-21), so only a whole run shows such errors; a
  temporary `tests.yml` on the fix branch (push trigger on the branch, the whole suite without
  variables and with the local root, `--continue-on-collection-errors -rfE`) read them on the
  runner. Also on Windows: the variable name is case-insensitive (`no_proxy` is `NO_PROXY`), a
  Linux path without a drive is not absolute, `read_text()` decodes in cp1252, and a URI built
  from the `\` root fails `Storage.relative` and the `RETURN_STATS` registration, which compare
  with `/`. After the fixes: 252 passed and 441 skipped without variables, 568 and 125 with the
  local root. `plan/POC.md`
