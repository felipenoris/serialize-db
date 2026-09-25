# S3, credentials, region and proxy

Read before `serialize_db.storage`, the S3 suite, `prepare_offline.sh` or a probe that reaches AWS. Each fact ends with the `plan/` file that details it, and `tests/proof_of_concept/` holds the API details as assertions. A fact found in a session is appended here, under the heading it belongs to.

## S3 primitives and IAM needs

- S3 `PutObject` accepts `IfNoneMatch='*'` (since 2024-08-20) and `IfMatch=<etag>` (since
  2024-11-25); failures return 412, conflicts 409. This is the primitive a table format needs for
  atomic commits, and it answers the open question in `plan/guia.md`. `plan/estrategia.md`
- S3 needs for Delta: `ListBucket` (prefix), `GetObject`, `PutObject` (commits use
  `If-None-Match: *`, no extra IAM action; `object_store` defaults `aws_conditional_put` to
  `etag`), `DeleteObject` for vacuum, KMS actions only with SSE-KMS; no lifecycle expiration under
  table prefixes; versioning and Object Lock unnecessary. SSE keys in `storage_options`:
  `aws_server_side_encryption`, `aws_sse_kms_key_id`, `aws_sse_bucket_key_enabled`. `plan/delta.md`
- A `CopyObject` of a large object can outlast the AWS C++ SDK's low-speed limit:
  `S3FileSystem.copy_file` (pyarrow 25.0.1) is one `CopyObject`, S3 copies server-side before it
  answers, and the SDK gives up after 3 s without a byte (`AWS Error NETWORK_CONNECTION during
  CopyObject operation: curlCode: 28, Timeout was reached; Details: Operation too slow. Less than
  1 bytes/sec transferred the last 3 seconds`), which killed the target's `archive` on the
  32,218,190-row file of `cad_lancamentos` 2026-06-30 on 2026-09-24 after three smaller tables
  had copied; pyarrow exposes no option for that limit. `Storage.copy` on S3 is boto3's managed
  `copy` since (`CopyObject` up to 8 MiB, `UploadPartCopy` in 8 MiB parts above, 10 threads,
  botocore's retries per part); moto 5.2.3 serves `UploadPartCopy`, and `test_list_copy_delete`
  copies a 9 MiB object in both roots. The rerun of 2026-09-24 at 16:51, on the root loaded anew,
  copied the 21 files of the 12 tables through the managed transfer, the four `cad_lancamentos`
  files included, with no error and no duration printed. `plan/POC.md`, `plan/PLAN-STAGE-3.md`

## Credentials, region and proxy in the clients

- `deltalake` 1.6.0 (2026-05-19) removed the DynamoDB lock store; S3 conditional put is the default
  commit mode, and the delta-rs docs page on S3 locking is stale. In the SageMaker space the writer
  finds the container credentials through the default chain, which also consults the `default`
  profile of `~/.aws/config` (`credential_source = EcsContainer`, per the `aws_config::profile`
  warning) but not its `region` (without the variables it went to `us-east-1`). Its HTTP client reads `HTTP_PROXY`/`HTTPS_PROXY` in both spellings but `no_proxy` only
  when `NO_PROXY` is absent: an empty `NO_PROXY`, what a shell opened by the Claude Code extension
  has, sends the credential call through the proxy, which answers 403; absent, exported from
  `no_proxy` or `169.254.170.2` alone passes (isolated 2026-09-20; the 2026-09-19 403 was this). The
  library exports `NO_PROXY` from `no_proxy` when absent or empty and keeps `storage_options` with
  the `boto3` credentials as the fallback. `plan/delta.md`, `plan/estrategia.md`
- botocore 1.43.98 reads `AWS_DEFAULT_REGION` or the profile, never `AWS_REGION`, and without a
  region uses the global endpoint `s3.amazonaws.com`, which a regional VPC endpoint does not serve;
  delta-rs reads both variables and without either queries IMDS and falls back to `us-east-1`. The S3
  suite copies the `boto3` region into `AWS_REGION` one way only, so an environment with only
  `AWS_REGION` and no `~/.aws/config` needs maintenance; `test_boto3_credential_source` needs STS
  (60 s per attempt, 5 attempts by default). On a dead network delta-rs gives up in 10 s with
  `max_retries=1` and `retry_timeout=10s` in `storage_options` (59 s without). `README.md`
- `pyarrow.fs.FileSystem.from_uri("s3://<bucket>/...")` without a region looks the bucket's region
  up over the network at construction (0.49 s and `OSError: Bucket ... not found` in a stripped
  environment); `?region=...` in the URI or `S3FileSystem(region=...)` builds in 0.00 s without
  network, so stage 3 builds the filesystem with the environment's region (2026-09-23).
  `plan/POC.md`, `plan/PLAN-STAGE-3.md`

- The delta-rs retry bound, read on 2026-09-23 in a stripped subprocess: against an unroutable
  endpoint (`http://10.255.255.1:9`) `is_deltatable` gave up in 57.0 s with the defaults and in
  10.3 s with `retry_timeout=10s`, whether `max_retries` was 1 or 3; against a closed local port in
  2.4, 0.3 and 0.6 s. `serialize_db.storage` passes `max_retries=3` and `retry_timeout=10s`: the
  timeout is the ceiling, and the retries cover a passing S3 error. `plan/POC.md`,
  `plan/PLAN-STAGE-3.md`
- DuckDB 1.5.5's `credential_chain` secret stores `key_id`, `secret` and `session_token` resolved
  at `CREATE SECRET` (`duckdb_secrets()` shows them, redacted), and nothing renews them; `REFRESH
  auto` is accepted with and without `CHAIN`, adds `refresh_info={'refresh': auto, ...}` to the
  secret, and the aws extension docs say some endpoints need periodic refreshing, which
  `REFRESH auto` requests and `CHAIN 'sts'` and `'web_identity'` switch on by themselves (probe of
  2026-09-24; when the refresh runs, the page does not say). `storage.duckdb_setup` and the
  migration script created the secret with `REFRESH auto` (user decision of 2026-09-24) until the
  target read on 2026-09-25 that only `httpfs` triggers it (below). `plan/POC.md`,
  `plan/OPEN_QUESTIONS.md`, `plan/PLAN-STAGE-3.md`
- In the stand-in of 2026-09-25 (moto behind a proxy answering `400 ExpiredToken` to a key past
  its 70 s lifetime, a local IMDS issuing a new key every 40 s, because delta-rs ignored
  `AWS_CONTAINER_CREDENTIALS_FULL_URI` and 169.254.170.2 does not exist in the container):
  DuckDB 1.5.5's `delta_scan` fails once the key stored in the secret expires (`DeltaKernel
  ObjectStoreError (8)`, the delta-kernel object_store reading the log with the secret's key)
  and so does `glob` (`HTTPException`, HTTP 400), neither refreshing; `read_parquet` gets the 400
  on `HEAD`, refreshes the secret through the aws extension's chain (AWS C++ SDK 1.11.702) and
  retries, and `delta_scan`, `glob` and `COPY ... TO` then work with the new key. delta-rs
  (object_store 0.13.2) refreshed by itself. `S3FileSystem` and boto3 failed for IMDS reasons
  the target does not share: botocore's IMDS fetcher pushes a near expiry 12 to 20 minutes
  ahead (`ec2_credential_refresh_window` 10 min plus 2 to 10 random), and the AWS C++ SDK
  1.11.800 of PyArrow reloaded about every five minutes. `plan/POC.md`, `plan/OPEN_QUESTIONS.md`
- In the target on 2026-09-25 (`probes/credentials.py`, 18:33 to 19:36 UTC, 14 rounds 5 minutes
  apart over `<root>/prd/cad_contas`, `plan/readings/credentials-2026-09-25-1833.txt`), the
  `boto3` chain served a new container key about every 30.6 minutes (the first expiring at
  19:19:10, the next ones at 19:49:57 and 20:20:31, switched between 18:48:54 and 18:53:55 and
  between 19:18:59 and 19:24:00): if each key lasts an hour, the key a client gets has 29 to 60
  minutes left [inferred]. The open `DeltaTable`, `S3FileSystem`, `Storage.read_text` and the
  open Redshift connection, past its `GetCredentials` password expiry at 19:33:52, kept reading.
  The DuckDB secret kept the opening key until it expired: at 19:24:00, 5 minutes after,
  `delta_scan` failed once (`DeltaKernel ObjectStoreError (8)`, `Generic S3 error` on
  `_delta_log/_last_checkpoint`), the same round's `read_parquet` read, the secret moved to the
  new key, and `delta_scan` read in the three later rounds. `credentials_clause` followed the
  container key from 18:53:55 on (a new `boto3` session per call). botocore refreshes a held
  container credential 15 minutes (advisory) to 10 minutes (mandatory) before its expiry.
  `plan/POC.md`, `plan/OPEN_QUESTIONS.md`
- The user chose on 2026-09-25 (card "Chave boto3") the DuckDB secret built from the key of the
  `boto3` credential (`storage.aws_credentials`; `KEY_ID ?`, `SECRET ?` and `SESSION_TOKEN ?` as
  command parameters, since a DuckDB syntax error repeats the command's line; without the `aws`
  extension), which the DuckDB engine recreates at the entry of each session when the key
  changed (`renew_duckdb_secret`, comparing the `key_id` that `duckdb_secrets()` shows). In the
  stand-in of that day, with a container credentials endpoint (`AWS_CONTAINER_CREDENTIALS_FULL_URI`,
  a key every 40 s, each valid 70 s) and a local IMDS for delta-rs (`AWS_METADATA_ENDPOINT`), the
  old engine failed from 72 s and the new one read every round; `probes/credentials.py`, which
  reads DuckDB through the engine since, failed `CR-4` on the old code and passed on the new.
  With `FULL_URI`, `S3FileSystem` and `boto3` renewed, unlike the IMDS stand-in. The target run
  of the probe is pending. `plan/POC.md`, `plan/PLAN-STAGE-3.md`, `plan/OPEN_QUESTIONS.md`

## The target's network, read on 2026-09-21

- No proxy variable, no internet; S3 through the gateway endpoint (public IPs, port 443 connects);
  STS by interface endpoint; IAM (`iam.amazonaws.com`) and KMS unreachable (10 s and 80 s timeouts
  in `bucket.py` and `redshift.py`). `diagnose_aws.py` had boto3, delta-rs as found and DuckDB list
  the test prefix with both region variables set: the S3 suite needs no maintenance there, and the
  library never calls IAM or KMS (SSE-KMS is applied by S3; the first write proves the permission).
  `plan/POC.md`, `plan/PLAN.md`

## The local stand-in for S3

- `tests/emulator.py`, switched on by `SERIALIZE_DB_TEST_EMULATOR` in `tests/conftest.py`, starts
  the moto 5.2.3 server as `python -m moto.server -H 127.0.0.1 -p <free port>` from the `dev` group
  (`moto[s3]==5.2.3`, `flask==3.1.3`, `flask-cors==6.0.5`: 28 packages, against 61 for
  `moto[server]`, which pulls `cfn-lint`, `docker` and `sympy`); the throwaway stand-in ran
  `uvx --from 'moto[server]==5.2.3' moto_server` outside the venv. Moto answered S3 and STS for
  `test_s3.py`, the Redshift suites and the package's `s3` tests on 2026-09-23: the conditional
  `PutObject` (`IfNoneMatch='*'`, `IfMatch`) returns 412, and `HeadObject` returns no
  `ServerSideEncryption`. Its output goes to `/dev/null`, because it logs every request and a pipe
  without a reader would block it. `plan/POC.md`
- Each client reaches it its own way: `boto3` and `pyarrow` read `AWS_ENDPOINT_URL`, delta-rs also
  needs `AWS_ALLOW_HTTP=true`, and DuckDB 1.5.5 ignores the variable: its secret needs `ENDPOINT`
  without the scheme, `URL_STYLE 'path'` (without it the bucket becomes a subdomain of the IP) and
  `USE_SSL false` for an `http` endpoint (without it, an SSL error), which
  `serialize_db.storage._duckdb_secret_options` now builds. With only `AWS_REGION` set, `boto3`
  1.43.98 took the region of the user's profile, and a `CreateBucket` without
  `LocationConstraint` got `IllegalLocationConstraintException`: the stand-in sets
  `AWS_DEFAULT_REGION` too. With keys in
  `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`, `boto3` reports the credential method `env`.
  `plan/POC.md`, `plan/PLAN-STAGE-3.md`
