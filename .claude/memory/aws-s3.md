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

## The target's network, read on 2026-09-21

- No proxy variable, no internet; S3 through the gateway endpoint (public IPs, port 443 connects);
  STS by interface endpoint; IAM (`iam.amazonaws.com`) and KMS unreachable (10 s and 80 s timeouts
  in `bucket.py` and `redshift.py`). `diagnose_aws.py` had boto3, delta-rs as found and DuckDB list
  the test prefix with both region variables set: the S3 suite needs no maintenance there, and the
  library never calls IAM or KMS (SSE-KMS is applied by S3; the first write proves the permission).
  `plan/POC.md`, `plan/PLAN.md`
