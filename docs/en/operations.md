> Language: [English](./operations.md) · [中文](../zh/operations.md)

# Operations handbook

## Cluster status

```bash
opens3 shell cluster-status --clerk http://CLERK:9333
opens3 shell raft-status --clerk http://CLERK:9333
opens3 shell volume-list --clerk http://CLERK:9333
curl -s http://CLERK:9333/cluster/status | jq .
```

Watch fields: `is_leader`, `term`, `volume_server_count`, `servers[].last_seen`, and each volume `state`.

## Health & readiness

| URL | Meaning |
|-----|---------|
| `GET /health` (clerk / volume / filer) | Process liveness |
| `GET /ready` (clerk) | Leader is known; safe to take traffic |
| `GET /metrics` | Prometheus text metrics |

Full metrics tables, Grafana dashboards, alert rules, and ServiceMonitor: **[Monitoring guide](monitoring.md)**.

Clerk: `opens3_clerk_is_leader`, `opens3_clerk_under_replicated_volumes`, `opens3_clerk_assign_total`  
Volume: `opens3_volume_needle_ops_total`, `opens3_volume_heartbeat_fail`  
Filer: `opens3_s3_requests_total`, `opens3_filer_ops_total`  

## Under-replication repair (Heal)

Clerk scans periodically; you can also trigger manually:

```bash
opens3 shell heal --clerk http://CLERK:9333
# or
curl -X POST http://CLERK:9333/cluster/heal
```

Flow: compare desired replica count vs `locations` → source Volume `POST /admin/replicate_volume/{id}?targets=…&max_bps=…` → target `import_volume_bin` writes `.dat/.idx` and reopens.

### Heal / Balance bandwidth limit

Full-volume push can saturate NICs. Cluster-wide rate limit (Raft-persisted; `0` = unlimited):

```bash
# View
curl http://CLERK:9333/cluster/heal-bandwidth

# Set to 50 MB/s (max_bps also accepted)
curl -X POST http://CLERK:9333/cluster/heal-bandwidth \
  -H 'Content-Type: application/json' \
  -d '{"max_mbps":50}'

# Clear limit
curl -X POST http://CLERK:9333/cluster/heal-bandwidth \
  -H 'Content-Type: application/json' \
  -d '{"max_mbps":0}'
```

The Admin topology Heal panel can also change the “bandwidth limit”. The limit applies to auto heal, manual Heal, place-replicas, and Balance; Volume pushes in chunks at an average rate.

## Crash / partial-write recovery

When Volume opens a volume at startup: scan `.dat`, keep every needle with a **valid CRC**, and truncate the file only to the end of the last complete record (not wipe the whole volume). On object read, if a location fails CRC/connect, Filer automatically tries other replicas from lookup.

### Restart fingerprint catch-up (degraded write, background sync)

Each Volume process start gets a new `boot_id`. When Clerk sees a change and the node hosts multi-replica volumes:

1. That node gets `write_ready=false`: local Put/ReplicateNeedle returns 503; it **does not appear in assign location lists**  
2. **Surviving replicas keep accepting writes** (degraded: temporarily one fewer sync replica); the cluster avoids long write stalls from full-volume copies of large gaps  
3. Background: pull `GET /admin/volume_fp/{id}` from peers, overwrite the restarted node from the larger replica (subject to heal bandwidth; if the source grows during copy, up to 5 catch-up rounds)  
4. After catch-up, the node becomes `write_ready=true` and rejoins sync fan-out  

Single-replica / empty nodes become `write_ready` immediately after start. Reads may still hit any location (including catch-up nodes; failover on failure).

## Capacity balance (Balance)

```bash
opens3 shell volume-balance --clerk http://CLERK:9333
curl -X POST http://CLERK:9333/cluster/balance
```

Copy volume replicas from nodes with clearly more volumes to nodes with fewer (**copy, do not delete the source**; confirm manually before shrinking).

## Volume seal & EC

```bash
# On a Volume node
curl -X POST http://VOLUME:8080/admin/seal/{volumeId}
curl -X POST 'http://VOLUME:8080/admin/ec/{volumeId}?data_shards=4&parity_shards=2'

# Clerk marks
curl -X POST http://CLERK:9333/vol/seal/{volumeId}
curl -X POST http://CLERK:9333/vol/ec/{volumeId}
```

EC artifacts land in `{dir}/ec/{id}.ec.*`.

## Compact (reclaim deleted needles)

```bash
curl -X POST http://VOLUME:8080/admin/compact/{volumeId}
```

Returns `before` / `after` byte counts. Prefer off-peak on volumes with high delete rates.

## Direct blob debugging (bypass S3)

```bash
opens3 shell assign --clerk http://CLERK:9333 --replication 001
# get fid and url
curl -X PUT "http://VOLUME:8080/{fid}?locations=http://v1:8080,http://v2:8080" --data-binary @file
curl "http://VOLUME:8080/{fid}" -o out
```

## Logging

```bash
RUST_LOG=info,opens3_clerk=debug,opens3_volume=debug opens3 server --dir ./data
```

## Backup recommendations

| Data | Path | Advice |
|------|------|--------|
| Object data | Volume `--dir` | Rely on replicas/EC; cold-backup whole disks if needed |
| Filer metadata | `meta.redb` | Periodic snapshots; corruption orphans objects |
| Raft | `raft_dir` | 3-replica Clerk; never delete logs from a single node |

## Admin UI

Open `http://S3:8333/admin/ui` in a browser; it polls `/admin/api/status`.
