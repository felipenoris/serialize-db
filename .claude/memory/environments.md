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
2026-09-20 and 2026-09-21 are in `redshift.md` and `plan/POC.md`, and `plan/readings/` holds the
masked reports of 2026-09-23 and the source-base reading of 2026-09-21. The five probes of 2026-09-21
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

## The prepared folder and the venv

`pyproject.toml` declares no runtime dependencies and pins the `dev` group (SQLAlchemy, duckdb-engine,
sqlalchemy-redshift and pandas were added on 2026-09-19 for the study suites; `prepare_offline.sh`
must be rerun).

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

The battery of 2026-09-24 at 01:41 to 02:19 UTC ran on a 16 vCPU (two per physical core) and
31,159 MB instance, DuckDB defaulting to 16 threads and a 24.3 GiB `memory_limit`,
`environment_limits` giving 16 threads and 13.1 to 13.6 GiB (half of the 27 to 28 GB available at
each opening), 29.7 GiB free of 37.0 GiB, the same roots under `.../shared/<usuário>/serialize-db/`
and `main` with #69; the whole migration finished there, `cad_lancamentos` peaking at 16,430 MB.
`plan/POC.md`

A new cloud session container (2026-09-24) starts without `.duckdb/`: the package tests with
`SERIALIZE_DB_TEST_LOCAL_ROOT` failed on the missing `delta` extension until the GitHub
workflow's command installed it (`duckdb.connect(config={'extension_directory': '.duckdb'})
.execute('INSTALL delta')`), and the stand-in's `s3` cases also need `httpfs` and `aws` there.
With the three, the local root gave 429 passed and 94 skipped, and the stand-in with the local
root 522 passed and 1 skipped. `README.md`
