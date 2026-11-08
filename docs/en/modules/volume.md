> Language: [English](./volume.md) · [中文](../../zh/modules/volume.md)

# Volume (`opens3-volume`)

## Role

Cluster **data plane**: stores objects on local disk as a **Needle append log**, and provides HTTP (and gRPC) read/write, **synchronous replication**, compact, volume-level **Reed-Solomon EC**, and volume import/export (for heal).

## Location

`crates/opens3-volume/src/`

| File | Role |
|------|------|
| `needle.rs` | Per-volume `.dat` / `.idx`: append write, mmap-load idx, batched flush, soft delete, compact |
| `store.rs` | Multi-volume management: `VolumeStore`, create volume, seal, compact, free disk |
| `ec.rs` | Sealed-volume EC encoding (data + parity shards) |
| `server.rs` | HTTP routes, heartbeat loop, replication, metrics, gRPC adapters |

## Default ports

- HTTP: `8080` (management plane: heartbeat coordination, compact/EC/heal admin APIs)  
- gRPC: `port + 10000` (e.g. `18080`) — **sole protocol** for gateway and replica **needle hot paths**

## Disk layout

```text
<data-dir>/
  {volumeId}.dat     # append needle data
  {volumeId}.idx     # append log: fileKey → offset/size/cookie/deleted (later entry wins for same key)
  ec/                # EC shard output directory (optional)
```

### Needle / Idx

Byte-level layout, field meanings, and read/write semantics: **[Needle data structures](needle.md)**.

Summary:

```text
.dat: [cookie|size|flags|pad|ttl][file_key|payload][crc32]   # header 16B; CRC covers payload only
.idx: [file_key|offset|size|cookie|deleted|pad]              # fixed 28B; later entry wins for same key
```

- **Append-write** idx; startup **mmap** rebuilds in-memory `HashMap`  
- **pread** reads `.dat` (does not move the write cursor); volume-level `RwLock`  
- Batched flush: default every 64 ops; `OPENS3_VOLUME_IDX_FLUSH_EVERY` / `OPENS3_VOLUME_SYNC`  
- **compact**: rewrite a clean `.dat` + compact `.idx`

## Write path (gRPC)

1. Clerk `assign` picks among writable volumes by **allocation strategy** (Admin-configurable): `random` / `round_robin` / `free_disk` / `least_size`  
2. Gateway `WriteNeedle` (with `replica_urls` = remaining locations)  
3. Primary node local `store.write`  
4. Primary **parallel gRPC** `ReplicateNeedle` fan-out to all replicas  
5. Replicas only persist locally and do not forward further; all must succeed before return; failures roll back the primary replica  

## Read / delete (gRPC)

- `ReadNeedle`: seek `.dat` via idx; verify cookie/CRC  
- `DeleteNeedle`: mark deleted (space reclaimed by compact)

## Ops APIs

| Path | Role |
|------|------|
| `POST /admin/create_volume/{id}` | Create empty volume |
| `POST /admin/seal/{id}` | Seal (stop writes) |
| `POST /admin/compact/{id}` | Compact; drop deleted needles |
| `POST /admin/ec/{id}` | EC encode |
| `POST /admin/replicate_volume/{id}?targets=&max_bps=` | Copy whole volume to targets (heal; `max_bps` average rate limit, 0 = unlimited) |
| `POST /admin/import_volume_bin/{id}` | Binary import `.dat` (header `X-OpenS3-Idx-B64`) |
| `GET /admin/volume_fp/{id}` | Volume fingerprint (restart alignment) |
| `POST /admin/import_volume` | Receive whole-volume dat/idx |
| `GET /status` / `/metrics` | Status and metrics |

## Heartbeat

Every 5s, `POST /cluster/heartbeat` to Clerk with local volume stats and `free_disk_bytes`; on failure set `volume_heartbeat_fail=1`.

## Design notes

- **Append write, sequential I/O**: small-file friendly, high throughput.  
- **Sync replication completes in the Volume layer**: Filer hits the primary URL once.  
- **No more writes after volume full / seal**; new writes get a different volume from Clerk.  
- EC is decoupled from hot writes so the Put path does not do RS encoding.

## Metrics (selected)

`volume_count`, `max_volumes`, `free_disk_bytes`, `needle_ops_total`, `needle_bytes_total`, `replicate_total`, `compact_total`, `ec_total`.

## Dependencies

`opens3-common`, `opens3-proto`; outbound HTTP to Clerk, outbound gRPC to peer Volumes.
