# The environments

Read before running anything in the SageMaker space or the target, preparing the offline folder, or dating a measurement. The target Redshift's readings are in `redshift.md`; the lab's probe readings of 2026-09-20 are in `plan/POC.md`.

## The SageMaker Unified Studio lab, as observed on 2026-09-19

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
  `plan/POC.md`, which also holds the lab-only readings (IMDS, the `~/shared` mount, Athena, the
  container credential's lifetime). This lab is not the target: the target has Redshift and no
  internet (user statement of 2026-09-20).
- The probes import `sagemaker-studio` from the system interpreter, because nothing unpinned enters the project venv
  (`lessons.md`).

## The target

Redshift serverless `controladoria-wg` in `sa-east-1`, account 138071776059, with no internet: the readings of
2026-09-20 and 2026-09-21 are in `redshift.md` and `plan/readings/`. The five probes of 2026-09-21
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
`bndes-aco-models-138071776059`: SSE-KMS with bucket key, SSE-C blocked, versioned by the sample, the
role denied the bucket-level reads as in the lab; the test root held one folder marker and no Delta
table. Glue has one database with one Parquet table, Athena three workgroups; Lake Formation and S3
Tables unreachable, so the re-evaluation trigger did not fire.

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
