# S3 兼容矩阵

对照 AWS S3 / `aws-cli` 常用能力。状态：

- **是**：已实现并有自动化覆盖（`tests/s3_compat/run.sh`、`e2e_prod_hardening.sh` 或单元测试）
- **部分**：有基础实现，语义不完整
- **否**：尚未实现

| API / 能力 | 状态 | 说明 |
|------------|------|------|
| CreateBucket / DeleteBucket / ListBuckets | 是 | 支持 `x-amz-bucket-object-lock-enabled` |
| PutObject / GetObject / HeadObject / DeleteObject | 是 | |
| ListObjectsV2 | 是 | `prefix` / `delimiter` / `max-keys` / `continuation-token` |
| CopyObject | 是 | `x-amz-copy-source` |
| DeleteObjects | 是 | `POST ?delete`；尊重 Object Lock |
| Multipart Create/UploadPart/Complete/Abort | 是 | Abort 会 GC part blob |
| ListParts | 是 | `GET ?uploadId=` |
| ListMultipartUploads | 是 | `GET /{bucket}?uploads` |
| SigV4 | 是 | Header + **query 预签名**（`X-Amz-*`） |
| Bucket Policy | 是 | 持久化到 meta KV |
| Bucket / Object ACL | 是 | Canned ACL：`private` / `public-read` / `public-read-write` / `authenticated-read` |
| Bucket Lifecycle | 部分 | Expire Days；持久化；无 Transition |
| Versioning | 部分 | 开关 + ListObjectVersions + versionId Get/Delete；版本副本存 `.versions/` |
| Range GET | 是 | `Range: bytes=` → 206 |
| CORS | 是 | Put/GetBucketCors |
| Tagging | 是 | Bucket/Object tagging |
| Object Lock | 是 | 配置存取 + COMPLIANCE/GOVERNANCE 删改执法 + legal hold；`x-amz-bypass-governance-retention` |
| SSE-S3 (`AES256`) | 是 | 响应头；与 Filer `--encrypt` 联用时落盘加密 |
| SSE-KMS | 部分 | 本地 KMS（`--kms-key` / `OPENS3_KMS_KEY`）；非云厂商 KMS |

## 跑兼容冒烟

```bash
bash tests/s3_compat/run.sh
bash tests/e2e_prod_hardening.sh   # JWT / Lock / ACL / ListMPU / TLS
```

若本机有 `aws` CLI，`s3_compat` 会额外跑 `mb/cp`（`--no-sign-request`）。
