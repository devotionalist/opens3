> Language: [English](./s3-compat.md) · [中文](../s3-compat.md)

# S3 compatibility matrix

Compared against common AWS S3 / `aws-cli` capabilities. Status:

- **Yes**: Implemented with automated coverage (`tests/s3_compat/run.sh`, `e2e_prod_hardening.sh`, or unit tests)
- **Partial**: Basic implementation; semantics incomplete
- **No**: Not implemented yet

| API / capability | Status | Notes |
|------------------|--------|-------|
| CreateBucket / DeleteBucket / ListBuckets | Yes | Supports `x-amz-bucket-object-lock-enabled` |
| PutObject / GetObject / HeadObject / DeleteObject | Yes | |
| ListObjectsV2 | Yes | `prefix` / `delimiter` / `max-keys` / `continuation-token` |
| CopyObject | Yes | `x-amz-copy-source` |
| DeleteObjects | Yes | `POST ?delete`; respects Object Lock |
| Multipart Create/UploadPart/Complete/Abort | Yes | Abort GCs part blobs |
| ListParts | Yes | `GET ?uploadId=` |
| ListMultipartUploads | Yes | `GET /{bucket}?uploads` |
| SigV4 | Yes | Header + **query presign** (`X-Amz-*`) |
| Bucket Policy | Yes | Persisted to meta KV |
| Bucket / Object ACL | Yes | Canned ACL: `private` / `public-read` / `public-read-write` / `authenticated-read` |
| Bucket Lifecycle | Partial | Expire Days; persisted; no Transition |
| Versioning | Partial | Toggle + ListObjectVersions + versionId Get/Delete; version copies under `.versions/` |
| Range GET | Yes | `Range: bytes=` → 206 |
| CORS | Yes | Put/GetBucketCors |
| Tagging | Yes | Bucket/Object tagging |
| Object Lock | Yes | Config R/W + COMPLIANCE/GOVERNANCE delete/modify enforcement + legal hold; `x-amz-bypass-governance-retention` |
| SSE-S3 (`AES256`) | Yes | Response headers; at-rest encryption when combined with Filer `--encrypt` |
| SSE-KMS | Partial | Local KMS (`--kms-key` / `OPENS3_KMS_KEY`); not cloud-vendor KMS |

## Run compatibility smoke tests

```bash
bash tests/s3_compat/run.sh
bash tests/e2e_prod_hardening.sh   # JWT / Lock / ACL / ListMPU / TLS
```

If `aws` CLI is present locally, `s3_compat` also runs `mb/cp` (`--no-sign-request`).
