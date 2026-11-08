> Language: [English](./common.md) · [中文](../../zh/modules/common.md)

# Common (`opens3-common`)

## Role

Shared domain layer with **no I/O services**: every runtime crate depends on it so FileId / replication / topology / error / metrics conventions stay consistent.

## Location

`crates/opens3-common/src/`

| Module | Role |
|--------|------|
| `fileid` | FileId encode/parse: `{volumeId},{fileKey}{cookie}` |
| `replication` | Three-digit replica code `XYZ` → required replica count and topology constraints |
| `topology` | `DataCenter` / `Rack` / `TopologyLocation` / `NodeId` |
| `types` | `VolumeMeta`, `VolumeState`, `EntryMeta`, `ChunkRef`, default volume size / chunk size |
| `error` | Unified `Error` / `Result` (including HTTP/S3 status mapping) |
| `util` | Small helpers: MD5, path join, etc. |
| `jwt` | Optional JWT verification helpers |
| `metrics` | Shared Prometheus `REGISTRY` and Clerk/Volume/Filer metric definitions |

## Key constants (`types`)

| Constant | Default | Meaning |
|----------|---------|---------|
| `DEFAULT_VOLUME_SIZE` | 30 GiB | Soft upper bound per volume |
| `DEFAULT_CHUNK_SIZE` | 8 MiB | Filer large-file chunk size |
| `NEEDLE_HEADER_SIZE` | 16 | Needle header length |
| `NEEDLE_CHECKSUM_SIZE` | 4 | CRC32 |

## Volume state machine

```text
Writable ──(full/read-only)──► ReadOnly ──(seal)──► Sealed ──(EC)──► EcEncoded
```

## Design notes

- **Pure library**: does not listen on ports; metrics register into the process-level `REGISTRY` and are exported by each service's `/metrics`.  
- **Serialization-friendly**: core structs carry `serde` for Raft payloads / HTTP JSON.  
- Prefer placing new cross-component concepts in this crate to avoid master↔volume↔filer circular dependencies.

## Dependents

Almost every crate; **do not** depend back on master/volume/filer/s3.
