> Language: [English](./filer.md) · [中文](../../zh/modules/filer.md)

# Filer (`opens3-filer`)

## Role

**Unified gateway**: S3 API, POSIX-path HTTP / WebDAV, and Admin UI share one `Server`, one listener, and in-process `FilerStore` + `BlobClient`. Production and Helm start this crate via `opens3 s3` (Protocols::ALL).

## Location

`crates/opens3-filer/src/` (flat; no subdirectories)

| File | Role |
|------|------|
| `server.rs` | Unified `Server`: `put_bytes` / `serve`, protocol switches |
| `http_filer.rs` | Path routes: `/entries`, `/dav`, `/store`, etc. |
| `http_s3.rs` | S3 API + Admin UI routes |
| `client.rs` | `BlobClient` (Clerk assign/lookup + Volume IO; no auto-follow 307 — pins leader) |
| `auth.rs` / `policy.rs` / … | SigV4, policies, lifecycle, etc. |

## Clerk follow

Same as Volume: `--clerk` / `--clerk-peers` (or `OPENS3_CLERK_PEERS`). On a non-leader 307 + `leader` URL, Filer/S3 rewrites the in-memory Clerk base so later calls hit the leader instead of redirecting forever through a Service VIP.

## Default port

`8333`.

## Write path

```text
put_bytes(path, data)
  → optional encrypt
  → BlobClient.write_chunked
  → FilerStore insert/update EntryMeta
  → EventBus PUT
```

## Dependencies

`opens3-store`, `opens3-common`.
