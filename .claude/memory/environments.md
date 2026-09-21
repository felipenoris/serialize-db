# The environments

Read before running anything in the SageMaker space or the target, preparing the offline folder, or dating a measurement. The target Redshift's readings are in `redshift.md`; the lab's probe readings of 2026-09-20 are in `docs/POC.md`.

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
  `docs/POC.md`, which also holds the lab-only readings (IMDS, the `~/shared` mount, Athena, the
  container credential's lifetime). This lab is not the target: the target has Redshift and no
  internet (user statement of 2026-09-20).
- The probes import `sagemaker-studio` from the system interpreter, because nothing unpinned enters the project venv
  (`lessons.md`).

## The target

Redshift serverless `controladoria-wg` in `sa-east-1`, account 138071776059, with no internet: the readings of
2026-09-20 and 2026-09-21 are in `redshift.md` and `docs/readings/`.

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

Each document dates its measurements and pins their versions in its opening lines: `docs/parquet.md`,
`docs/duckdb.md` and `docs/sqlalchemy.md` on 2026-09-18, over 300,000 rows of `operacoes`
(`poc_delta.sample_table`), the Redshift statements compiled only; `docs/estrategia.md` on 2026-09-19;
the S3 proof of concept of 2026-09-19 (Python 3.13.15) in `docs/POC.md`. What they do not say: the
local proof of concept ran on macOS arm64 through `uv run --with` in the scratchpad, with 11 DuckDB
threads and the files in the page cache, nothing against S3 or Redshift; the Python examples added
to every document on 2026-09-19 ran under the same pinned versions.
