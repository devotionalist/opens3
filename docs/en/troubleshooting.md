> Language: [English](./troubleshooting.md) · [中文](../zh/troubleshooting.md)

# Troubleshooting

## Symptom quick reference

| Symptom | Likely cause | Action |
|---------|--------------|--------|
| `assign` fails / No writable volume | No Volume heartbeat; volume full; not on leader | Check `cluster-status`; confirm VS online; hit the leader |
| S3 403 AccessDenied | Auth enabled without signing; Bucket Policy Deny | Set AK/SK; check policy |
| S3 500 on object write | Volume unreachable; volume read-only/sealed | `volume-list`; check Volume logs; heal |
| Mid-read checksum mismatch | Disk bitrot | Read from another replica; replicate from healthy replica |
| Master 307/redirect | Current node is not leader | Follow `leader` URL in JSON; Filer/S3/Volume pin automatically — do not auto-follow redirects on the VIP |
| Filer/S3 writes stuck on 307 / timeout | Old client auto-follows Service VIP | Upgrade and set `--clerk-peers`; confirm pin to a concrete Clerk Pod URL |
| Master `CrashLoopBackOff` / `OOMKilled` | Raft log growth + full replay on restart | See [Master repeated failure RCA](#master-repeated-failure-rca) below |
| Master `Ready 0/1` / dual Leader | Raft dir on emptyDir or lost PVC | Enable `clerk.persistence`; do not cold-start multi-replica on empty disks |
| Master STS won't start (multi-replica) | `OrderedReady` waiting forever for quorum | Chart already uses `podManagementPolicy: Parallel` |
| Multi-Filer metadata corruption | Multiple processes sharing one redb | Switch to shared Postgres / MySQL |
| Old data “lost” after scale-out | Wrong node queried; stale lookup cache | lookup volume; clear client cache |

## Confirm the leader

```bash
curl -s http://10.0.0.1:9333/raft/status
curl -s http://10.0.0.2:9333/raft/status
opens3 shell raft-status --master http://10.0.0.1:9333
```

Only the node with `role=Leader` should receive heartbeat/assign.

## Master repeated failure RCA

Local / Colima k3s development clusters saw repeated `opens3-master-0` unavailability in 2026-11 (`Ready 0/1`, `CrashLoopBackOff`). The timeline below summarizes the **root-cause chain** for future comparison.

### Symptom mapping

| Order | Symptom | Direct evidence | Root cause |
|-------|---------|-----------------|------------|
| A | Long `Ready 0/1` after Pod restart; occasional dual Leader / Candidate | Raft dir on `emptyDir`; after restart `last_log_id=None`, re-`Initialize` | **Raft state not persisted** |
| B | All 3 Masters Pending / waiting on each other on cold start | STS default `OrderedReady`: `-0` waits for peers to become Leader, peers wait for `-0` Ready | **Startup policy deadlocks with Raft quorum** |
| C | `CrashLoopBackOff`, `Last State: OOMKilled`, `Exit Code: 137` | Startup log `re-apply ... T1-N0-3315`; killed ~10s after `master listening`; limit was 1–2Gi | **Uncompacted full-topology Raft log fully re-applied on restart → OOM** |

These are often treated as “Master is unstable again,” but they are **different layers** stacked together.

### Root cause A: Raft on emptyDir

- If Master's `--raft-dir` mounts `emptyDir`, every Pod recreate is a **brand-new node**.
- Single replica: repeated bootstrap; multi-replica: each node campaigns with empty logs → **dual Leader / split-brain feel / Ready fails for a long time**.
- **Fix**: Helm `clerk.persistence.enabled=true` (PVC `raft-opens3-master-N`). `values-dev.yaml` already forces this on.

### Root cause B: OrderedReady × Raft cold start

- Three voters must be online together for quorum; `OrderedReady` starts Pods serially, so `-0` never gets `-1/-2`.
- **Fix**: `deploy/helm/opens3/templates/clerk.yaml` uses `podManagementPolicy: Parallel`.

### Root cause C: Topology snapshot blowing up Raft (primary OOM cause)

Mechanism (before the fix):

1. Leader **every 3 seconds** `client_write`s the **entire** `ClusterSnapshot` into Raft (each log entry is full topology JSON).
2. Volume heartbeats auto-create volumes (old default `maxVolumes=32`, now `1`) so topology keeps growing; log count and per-entry size inflate together.
3. openraft default `SnapshotPolicy::LogsSinceLast(5000)`: a few hours of a dev cluster with ~3k entries **never triggered compaction**.
4. State machine / Raft snapshot lived **only in memory**; disk had only log files. On restart `last_applied=None`, the engine `re-apply`s the whole log.
5. Thousands of “full topology deserialize + import” peaks exceed the memory limit → **OOMKilled (137)** → kubelet `CrashLoopBackOff`.

Typical log signature:

```text
openraft ... re-apply ... T1-N0-0 ... T1-N0-3315
opens3_master ... master listening on 0.0.0.0:9333
# Process then disappears; kubectl describe: Last State OOMKilled, Exit Code 137
```

**Shipped fixes** (requires rebuilding the image):

| Change | Location | Effect |
|--------|----------|--------|
| Topology propose every 30s + fingerprint dedupe | `server.rs` | Greatly reduces pointless write amplification |
| `SnapshotPolicy::LogsSinceLast(32)` + periodic `trigger().snapshot()` | `raft/mod.rs` / `server.rs` | Actively compact logs |
| Persist `openraft-sm.json` / `openraft-snapshot.bin` | `raft/store.rs` | Restart from `last_applied`; avoid full re-apply |
| Default `volume.maxVolumes: 1` | `values.yaml` / CLI | Only 1 data volume pre-created per data node by default |

**Emergency for already-bloated PVCs** (dev clusters can accept losing control-plane state):

```bash
kubectl -n opens3 scale sts/opens3-master --replicas=0
kubectl -n opens3 delete pvc raft-opens3-master-0
# After deploying an image that includes the fixes above
kubectl -n opens3 scale sts/opens3-master --replicas=1
```

In production, first try raising the memory limit and confirming the new build can create snapshots / purge; blindly deleting PVCs drops Master-side topology and Raft term info (Volume disk data remains; topology must be rebuilt via heartbeat).

### Post-restart health checks

```bash
kubectl -n opens3 get pod opens3-master-0
kubectl -n opens3 describe pod opens3-master-0 | grep -E 'OOM|Exit Code|Restart'
kubectl -n opens3 logs opens3-master-0 | grep -E 're-apply|last_applied|build_snapshot|OOM'
kubectl -n opens3 exec opens3-master-0 -- ls -lah /data/raft/
# Expect all of: openraft-log.wal (or pre-migration openraft-log.json), openraft-sm.json, openraft-snapshot.bin
curl -s http://127.0.0.1:9333/cluster/status   # role=Leader / is_leader=true
```

Healthy restart logs should look like: `last_applied=T…` and **no** thousands-of-index `re-apply` spam; periodic `build_snapshot` should appear afterward.

### Ops constraints (avoid recurrence)

1. **Master Raft directory must be a PVC**; do not use emptyDir in multi-replica or restart-prone environments.  
2. **Do not** run old images that “write full topology every few seconds” for long on uncompacted old PVCs.  
3. Raise `volume.maxVolumes` only when you need more pre-created data volumes; keep the default of 1 volume per node to avoid topology/Raft bloat.  
4. Watch `opens3_master_raft_commit_index` and `openraft-log.wal` size on the PVC; growth without shrink and never compacting means snapshot/purge is broken.  
5. After single-replica Master scale-down / PVC wipe, old multi-replica volume data and topology may disagree—converge with heartbeat + heal, rather than starting an empty Master set and expecting automatic alignment.

## Recovering after Volume goes offline

1. Fix disk/process; bring it up with the **same** `--node-id` and `--dir`  
2. Wait for heartbeat  
3. `opens3 shell heal` to repair missing replicas  
4. Spot-check key objects with S3 Get  

## Fault drill scripts

The repo provides:

```bash
bash tests/e2e_fault.sh
```

Coverage: start cluster → write → add second Volume → heal/balance → still readable.

Unit / fault-oriented tests:

```bash
cargo test -p opens3-volume --test fault_replica_import
cargo test -p opens3-topology --test raft_persistence
cargo test -p opens3-common jwt
```

## Common ops pitfalls

1. **Putting Volume primary storage on NFS**: poor latency and lock semantics; cold backup only  
2. **`replication=001` with only one Volume process**: nowhere for the second replica; write topology degrades to single replica  
3. **Changing DC/rack labels without rebuilding volumes**: existing volume locations are not auto-corrected; need heal/balance  
4. **Expecting writes to the same volume after seal**: Sealed volumes reject writes; use a new volume  

## Gathering support info

When filing an issue, include:

```bash
opens3 --version
curl -s http://MASTER:9333/cluster/status
curl -s http://MASTER:9333/raft/status
# Relevant log snippets with RUST_LOG=debug
```
