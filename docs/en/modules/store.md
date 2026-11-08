> Language: [English](./store.md) · [中文](../../modules/store.md)

# Store (`opens3-store`)

## Role

**Metadata storage abstraction** for Filer / S3: path-level `EntryMeta` and namespace KV (IAM, multipart, lifecycle, versioning, etc.). Object bytes do **not** enter Store—only FileId / chunk pointers.

## Location

`crates/opens3-store/src/`

| File | Role |
|------|------|
| `lib.rs` | `FilerStore` trait |
| `redb_store.rs` | Single-node embedded (default); K8s needs RWO |
| `memory.rs` | Tests / non-persistent |
| `postgres.rs` | Shared metadata across instances (Postgres) |
| `mysql.rs` | Shared metadata across instances (MySQL / TiDB) |
| `http_store.rs` | Proxy metadata ops via remote Filer HTTP |

## `FilerStore` interface

**Path API**

- `insert_entry` / `update_entry` / `find_entry` / `delete_entry`  
- `list_directory` / `list_by_prefix`  

**KV API** (`ns` + `key`)

- `kv_put` / `kv_get` / `kv_delete` / `kv_list`  
- Used for multipart uploadId, bucket policy, lifecycle, versioning, etc.  

## Backend guidance

| Backend | Fit |
|---------|-----|
| **redb** | Single Filer/S3 replica; minimal dependencies |
| **memory** | Unit tests, temporary demos |
| **postgres** | Shared metadata across multiple S3/Filer (see [multi-filer](../multi-filer.md)) |
| **mysql** / **tidb** | MySQL or TiDB (MySQL protocol); CLI names are equivalent |
| **http** | S3 process keeps no local meta; forwards to a standalone Filer |

## Design notes

- Trait is fully `async` + `Send + Sync`, suited to axum sharing `Arc<dyn FilerStore>`.  
- Path and KV use separate tables/trees so object listing and multipart state do not interfere.  
- Does **not** implement distributed locks; multi-writer setups need Postgres transaction semantics or single-writer deployment.

## Dependencies

`opens3-common` (`EntryMeta`); used by `opens3-filer` (including S3) / CLI.
