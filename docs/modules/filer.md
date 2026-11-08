> 语言: [中文](./filer.md) · [English](../en/modules/filer.md)

# Filer（`opens3-filer`）

## 作用

**统一网关**：S3 API、POSIX 路径 HTTP / WebDAV、Admin UI 共用一个 `Server`、一个 listener，进程内共享 `FilerStore` + `BlobClient`。生产与 Helm 以 `opens3 s3`（Protocols::ALL）启动本 crate。

## 位置

`crates/opens3-filer/src/`（扁平，无子目录）

| 文件 | 作用 |
|------|------|
| `server.rs` | 融合后的 `Server`：`put_bytes` / `serve`、协议开关 |
| `http_filer.rs` | `/entries`、`/dav`、`/store` 等路径路由 |
| `http_s3.rs` | S3 API + Admin UI 路由 |
| `client.rs` | `BlobClient`（Clerk assign/lookup + Volume IO；禁用自动跟随 307，自行 pin leader） |
| `auth.rs` / `policy.rs` / … | SigV4、策略、生命周期等 |

## Clerk 跟随

与 Volume 相同：`--clerk` / `--clerk-peers`（或 `OPENS3_CLERK_PEERS`）。非 leader 返回 307 + `leader` URL 时，Filer/S3 会改写内存中的 Clerk base，后续请求直打 leader，避免对 Service VIP 无限 redirect。

## 默认端口

`8333`。

## 写路径

```text
put_bytes(path, data)
  → 可选 encrypt
  → BlobClient.write_chunked
  → FilerStore insert/update EntryMeta
  → EventBus PUT
```

## 依赖

`opens3-store`、`opens3-common`。
