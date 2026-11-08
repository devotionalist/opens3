> Language: [English](./architecture.md) · [中文](../zh/architecture.md)

# Architecture

OpenS3 separates the control plane from the data plane: hot paths use volume-level synchronous replicas; cold volumes can use asynchronous erasure coding (EC). Code is split by crate with clear responsibilities and independent deployability.

## System overview

```text
  Clients (S3 SDK / FUSE / HTTP / WebDAV...)
                 │
                 ▼
           opens3 filer          single process / single port (default 8333)
        S3 · /entries · /dav · Admin
                 │
         ┌───────┴────────┐
         ▼                ▼
      FilerStore       Volume×N
   Postgres / MySQL    needle .dat/.idx
         │                ▲
         └────── Clerk ───┘
              assign / lookup (+ Raft)
```

**Production topology**: Clerk (Raft) + Volume + **unified Filer** (S3 and path APIs in the same process). Helm deploys a single `opens3-filer` Deployment; the `opens3-s3` Service is only a DNS-compatible alias. See [Clients and gateways](gateways.md) and [Kubernetes](kubernetes.md).

Clerk is **not on the read hot path**: after the Filer obtains `volumeId → servers`, it talks to Volume directly; lookup results can be cached.

### Inter-node protocols

| Path | Protocol | Notes |
|------|----------|-------|
| Filer → Volume (needle read/write/delete) | **gRPC** (HTTP port +10000) | `WriteNeedle` / `ReadNeedle` / `DeleteNeedle` |
| Volume → Volume (synchronous replication) | **gRPC** `ReplicateNeedle` | Parallel |
| Filer / Volume → Clerk | HTTP | `assign` / `lookup` / `heartbeat` |
| Client → Filer | HTTP (S3 / WebDAV / entries) | External protocols unchanged |

## Crate / module map

| Crate | Role | Docs |
|-------|------|------|
| [`opens3`](modules/cli.md) | CLI binary: spawn subprocesses / all-in-one / shell | [CLI](modules/cli.md) |
| [`opens3-common`](modules/common.md) | Shared types: FileId, replication codes, topology, errors, metrics | [common](modules/common.md) |
| [`opens3-proto`](modules/proto.md) | gRPC protobuf (Clerk / Volume service definitions) | [proto](modules/proto.md) |
| [`opens3-clerk`](modules/clerk.md) | Topology, assign/lookup, Raft, heal/balance | [Clerk](modules/clerk.md) |
| [`opens3-volume`](modules/volume.md) | Needle storage, replication, compact, EC | [Volume](modules/volume.md) |
| [`opens3-store`](modules/store.md) | Filer metadata backend abstraction and implementations | [Store](modules/store.md) |
| [`opens3-filer`](modules/filer.md) | Unified gateway: S3 + HTTP/WebDAV + Admin | [Filer](modules/filer.md) / [S3](modules/s3.md) |

Dependency direction (runtime components must not reverse-depend):

```text
opens3 (CLI)
  ├── opens3-clerk ─┐
  ├── opens3-volume ──┼── opens3-common
  └── opens3-filer  ──┤         ▲
        └── opens3-store ───────┘
            (http_s3: S3 API / SigV4 / Admin)
opens3-proto ── (tonic-generated, for volume gRPC)
```

Entry index: [Module docs](modules/README.md).

## Component responsibilities (runtime)

| Component | Responsibility | Default port |
|-----------|----------------|--------------|
| **Clerk** | Topology, volume `assign`, lookup, Raft HA, heal/balance scheduling | 9333 |
| **Volume** | Needle append storage, sync replication, compact, volume EC, import/replicate | 8080 (gRPC = port+10000) |
| **Filer** | Unified gateway: S3 API, `/entries`, `/dav`, Admin UI; in-process FilerStore | 8333 |

CLI: `opens3 s3` starts **Protocols::ALL** (S3 + path APIs); `opens3 filer` is path APIs only (dev/compat). Production and Helm use `opens3 s3` (container args) for the unified Filer.

### Boundaries (does / does not)

| Component | Does | Does not |
|-----------|------|----------|
| Clerk | Volume-level metadata, leader election, replication scheduling | Store object bytes, proxy GET/PUT body |
| Volume | Local `.dat/.idx`, sync replicas, EC | Path semantics, IAM, bucket policy |
| Store | `EntryMeta` + KV | Object data, Raft |
| Filer | Path↔FileId, chunking, encryption, S3/HTTP/WebDAV | Own disk volume files |

## Data model

- **FileId**: `{volumeId},{fileKeyHex}{cookieHex}`; cookie prevents guessing  
- **Needle**: One blob in a volume (header + payload + CRC32)  
- **Volume**: Default ~30GiB append file; states `Writable → ReadOnly/Sealed → EcEncoded`  
- **EntryMeta**: Filer path metadata (single FileId or chunks list)  
- **Replication code** (three digits): `XYZ`  
  - `X`: Cross-DC replica count  
  - `Y`: Same DC, cross rack  
  - `Z`: Same rack, other nodes  
  - Examples: `000` single replica, `001` 2 copies same rack, `010` 2 copies cross rack  

## Write path (small objects via Filer)

1. Filer → Clerk `assign(collection, replication)` → FileId + primary Volume URL + replica locations  
2. PUT needle to primary Volume; primary **fan-outs synchronously in parallel** to all replicas; any failure fails the whole write  
3. FilerStore writes path → FileId / chunks  

Large files are split into **8MiB** chunks by default; each chunk has its own FileId; too many chunks can be packed into a meta-chunk.

## Read path

1. FilerStore looks up metadata to get FileId(s)  
2. Clerk lookup (or cache) yields Volume addresses  
3. GET from any healthy replica; on CRC failure try another replica  

## Horizontal scaling

1. Start a new Volume Server; it heartbeats to Clerk  
2. Clerk **creates new volumes** on idle nodes  
3. **New writes** land on new volumes; **old volumes are not moved by default**  
4. When relocation is needed, run `volume-balance` / `heal` (see [Operations](operations.md))  
5. Filer is stateless: scale `filer.replicas` (shared Postgres / MySQL)  

Unlike MinIO “add Server Pool”, OpenS3 scales by **adding disks/Volume nodes + new volumes**.

## Consistency and availability

| Path | Model |
|------|-------|
| Multi-replica needle write | Synchronous replication; success only when all succeed |
| Clerk metadata | Raft leader election; `assign` / heal / balance on leader only |
| Filer metadata | Shared Postgres / MySQL (multi-replica); single-node dev can use redb |
| Read | Any healthy replica; can fall back to another location |

## Asynchronous EC

After a volume is sealed, Volume can Reed-Solomon-split `.dat` into data+parity shards (`/admin/ec/{id}`). Hot-path Put does **not** do synchronous EC.

## Security (production)

| Capability | Behavior |
|------------|----------|
| HTTPS | `--tls-cert` / `--tls-key` (or `OPENS3_TLS_*`) terminates with rustls in-process; Ingress termination also fine |
| Volume JWT | Clerk `--jwt-secret` issues on `/dir/assign`; Volume validates Bearer on write/delete; Filer forwards with the same secret |
| S3 Auth | SigV4 header + query presign; Bucket Policy ∩ canned ACL |
| Object Lock | COMPLIANCE / GOVERNANCE retention and legal hold enforcement |
| SSE | `AES256` / local `aws:kms` (`OPENS3_KMS_KEY`) + Filer `--encrypt` |

## Observability

Each component exposes `GET /metrics` (Prometheus). The Filer exposes both `opens3_s3_*` and `opens3_filer_*` on the same port. Metric tables, Grafana, and alerts: [Monitoring](monitoring.md).

## Related docs

- [Module overview](modules/README.md)  
- [Deployment](deployment.md) / [Kubernetes](kubernetes.md)  
- [Operations](operations.md) / [Troubleshooting](troubleshooting.md)  
