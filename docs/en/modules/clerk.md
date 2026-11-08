> Language: [English](./clerk.md) · [中文](../../modules/clerk.md)

# Clerk (`opens3-clerk`)

## Role

Cluster **control plane**: maintains Volume Server topology and volume metadata; owns **FileId allocation (assign)**, **volume location (lookup)**, **Raft HA**, and **under-replica heal / capacity balance** scheduling.  
**Does not store object bytes** and does not proxy read/write traffic.

## Location

`crates/opens3-clerk/src/`

| File | Role |
|------|------|
| `server.rs` | HTTP API, `/metrics`, periodic gauges / heal / topology propose |
| `cluster.rs` | In-memory cluster state: servers, volumes, assign, lookup, heartbeat volume creation |
| `raft/` | openraft: append-only WAL log (`openraft-log.wal`) + state machine / snapshot |
| `heal.rs` | Under-replica detection and replication plans; balance migration plans and execution |

Root causes and remediation when Clerk repeatedly CrashLoops / fails Ready on K8s: [Troubleshooting · Master repeated failure RCA](../troubleshooting.md#master-repeated-failure-rca).

## Default port

`9333` (HTTP). Ready: `/ready` (has a leader or known leader); live: `/health`.

## Core responsibilities

1. **Heartbeat**: Volume reports `node_id`, topology, and local volume stats; Clerk merges `VolumeMeta.locations` and, when capacity is not full, returns `volume_ids_to_create`.  
2. **Assign**: **leader only**; randomly picks one among **all writable data volumes** matching the replica code, then takes that volume's locations and generates `volumeId + fileKey + cookie` (with many volumes, PUTs spread across volumes/nodes).  
3. **Lookup**: `volumeId → locations[]` (any node can serve reads).  
4. **Raft**: `assign` and critical mutations go through propose; multi-Clerk uses `peers` + `advertise-url`.  
5. **Heal / Balance**: leader periodically `plan_heal` → calls Volume `replicate_volume`; `balance` migrates volumes off hot nodes.

## Key HTTP APIs

| Path | Description |
|------|-------------|
| `GET/POST /dir/assign` | Allocate FileId |
| `GET /dir/lookup/{volume_id}` | Look up volume locations |
| `POST /cluster/heartbeat` | Volume heartbeat |
| `GET /cluster/status` | Cluster JSON |
| `POST /cluster/heal` / `balance` | Run heal/balance immediately |
| `GET`/`POST /cluster/heal-bandwidth` | heal/balance push bandwidth cap (`max_mbps` / `max_bps`, 0 = unlimited) |
| Volume `boot_id` via heartbeat | On change, fingerprint realign; nodes not `write_ready` are excluded from assign |
| `POST /raft/*` | Raft RPC |
| `GET /metrics` | Prometheus |

## Design notes

- **Leader writes, followers redirect**: assign/heal/balance on a non-leader return a leader hint.  
- **Stale heartbeat pruning**: Volume Servers unseen for too long are removed from topology, triggering under-replicated.  
- **Auto volume creation**: on heartbeat, if a node's local volume count is `< max_volume_count`, Clerk allocates new volume ids for the node to create.  
- **Online semantics**: new nodes without bootstrap default to **offline** (Web/STS scale-out); Helm-declared ordinals bootstrapped default to **online**, and values may set `replicaOf`. Admin can online/offline at any time.  
- **Online/offline**: `POST /vol/online`, `POST /cluster/server/online`; offline volumes and offline nodes do not take new writes.  

- The Raft directory must be durable (PVC; avoid long-lived emptyDir); state machine and snapshots must persist with the log, or restarts re-apply the full log and can OOM.  
- K8s must set a stable `advertise-url`; multi-Clerk STS should use `podManagementPolicy: Parallel`.  
- **Raft voter IDs are always derived from `advertise-url` / peer URL** (`node_id_from_url`), decoupled from `--ip`; `--ip` is display-only. Never mix `--ip`-name hashes and URL hashes into membership (this previously caused ghost voters and leadership-change panics).  
- **Log physical layer is an append-only WAL** (`openraft-log.wal`): hot path only appends records; truncate/purge write logical markers; rewrite compact when holes grow. Legacy `openraft-log.json` full snapshots migrate automatically on startup.

## Failure drills

Hardened Clerk-specific drills (local processes, no K8s dependency):

```bash
bash tests/e2e_clerk_chaos.sh
```

| Phase | Injection | Expectation |
|-------|-----------|-------------|
| scale-out | Bring up 0→1→2 under a 3-voter config | Leader available at 2/3; assign available after 3/3 |
| scale-in | Permanently stop one follower | 2/3 still assign; restart restores 3 |
| node-crash | Kill Leader / Follower, restart on same disk | Assign after leadership change; restarted node rejoins |
| disk-corrupt | Corrupt follower `openraft-log.wal` | Bad node fails to start; survivors keep assigning |
| disk-wipe | Clear follower `raft-dir` then restart | No split-brain; survivors keep assigning |
| majority-loss | Leave only one former follower | Cannot elect leader; assign unavailable |

Lighter leadership-change / snapshot cases: `tests/e2e_clerk_failover.sh`, `tests/e2e_raft_snapshot.sh`.

## Metrics (selected)

`opens3_clerk_is_leader`, `raft_term`, `under_replicated_volumes`, `assign_total`, `lookup_total`, `heal_*`, `balance_*`. See [Monitoring](../monitoring.md).

## Dependencies

`opens3-common`; HTTP client calls Volume to execute heal/balance.
