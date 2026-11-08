> Language: [English](./README.md) · [中文](../../modules/README.md)

# Module docs

Per-crate notes on **role, boundaries, internal structure, key APIs / dependencies**. For system-level read/write paths, see [Architecture overview](../architecture.md).

| Module | Crate | One-liner |
|--------|-------|-----------|
| [CLI](cli.md) | `opens3` | Process entry: clerk / volume / s3 (unified Filer) / filer / server / shell |
| [Common](common.md) | `opens3-common` | Shared domain types and utilities (no network services) |
| [Proto](proto.md) | `opens3-proto` | gRPC contracts |
| [Clerk](clerk.md) | `opens3-clerk` | Cluster control plane: topology, assignment, Raft, repair |
| [Volume](volume.md) | `opens3-volume` | Data plane: Needle storage and replication |
| → [Needle](needle.md) | (inside volume) | `.dat` / `.idx` byte layout and read/write semantics |
| [Store](store.md) | `opens3-store` | Filer metadata storage abstraction |
| [Filer](filer.md) | `opens3-filer` | Unified gateway: S3 + HTTP/WebDAV + Admin |
| [S3](s3.md) | `opens3-filer` (`http_s3`) | S3 API routing and auth (same crate) |
| [Clients & gateways](../gateways.md) | — | S3 SDK / FUSE / HTTP / WebDAV access |

## Design principles

1. **Control plane / data plane separation**: Clerk does not forward object byte streams.  
2. **Volume as the allocation unit**: Scale by adding Volume Servers + new volumes, not by rewriting old objects.  
3. **Hot replicas, cold EC**: The Put path uses synchronous replication; EC is a post-seal capacity optimization.  
4. **Unified Filer**: S3 and path APIs share one process and one port; metadata is pluggable (Postgres / MySQL·TiDB / redb / HTTP).  
5. **Shared types sink down**: FileId, replication codes, and EntryMeta live only in `opens3-common` to avoid circular dependencies.  
