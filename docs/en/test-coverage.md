> Language: [English](./test-coverage.md) · [中文](../zh/test-coverage.md)

# Test coverage report

Inventory of automated tests in the repo, what they cover, and the latest full-run results.

| Item | Value |
|------|-------|
| Report date | 2026-11-02 |
| Command baseline | `cargo test --workspace` + `tests/e2e_*.sh` + `tests/s3_compat/run.sh` |
| Summary | **Cargo 16 passed / 0 failed**; **E2E 8/8 passed** (watch fixed-port conflicts when running serially; see below) |

Quick re-run:

```bash
cargo test --workspace
for t in e2e_fault e2e_kill_replica e2e_clerk_failover e2e_clerk_chaos e2e_seal_ec \
         e2e_multi_s3 e2e_raft_snapshot e2e_prod_hardening; do
  bash "tests/${t}.sh" || exit 1
done
bash tests/s3_compat/run.sh
```

---

## 1. Cargo unit / integration tests

`cargo test --workspace` (report date): **all passed**.

### 1.1 `opens3-common` (4)

| Test | File | Coverage | Result |
|------|------|----------|--------|
| `fileid::tests::roundtrip` | `fileid.rs` | FileId encode/decode | Pass |
| `jwt::tests::roundtrip_jwt` | `jwt.rs` | Volume JWT issue/verify | Pass |
| `metrics::tests::master_metrics_encode` | `metrics.rs` | Prometheus text export | Pass |
| `integration_tests::fileid_and_replication` | `lib.rs` | FileId + Replication `010` | Pass |

### 1.2 `opens3-s3` (4)

| Test | File | Coverage | Result |
|------|------|----------|--------|
| `policy::tests::deny_beats_allow` | `policy.rs` | IAM Deny beats Allow | Pass |
| `lock::tests::parse_lock_xml` | `lock.rs` | Object Lock XML → config | Pass |
| `lock::tests::compliance_blocks_delete` | `lock.rs` | COMPLIANCE retention blocks delete | Pass |
| `lock::tests::governance_bypass` | `lock.rs` | GOVERNANCE + bypass header | Pass |

### 1.3 `opens3-clerk` (2)

| Test | File | Coverage | Result |
|------|------|----------|--------|
| `heal_plans_missing_replica` | `tests/heal_plan.rs` | Heal plan for missing replica | Pass |
| `leader_propose_survives_restart` | `tests/raft_persistence.rs` | Raft log persistence across restart | Pass |

### 1.4 `opens3-volume` (5)

| Test | File | Coverage | Result |
|------|------|----------|--------|
| `write_read_delete_compact` | `tests/needle_roundtrip.rs` | Needle write/read/delete + compact | Pass |
| `encode_and_reconstruct` | `tests/ec_roundtrip.rs` | Reed-Solomon EC encode/decode | Pass |
| `second_write_fails_when_volume_full` | `tests/volume_full.rs` | Volume full rejects writes | Pass |
| `bitrot_detected_on_checksum_mismatch` | `tests/fault_replica_import.rs` | CRC/checksum mismatch detection | Pass |
| `recover_volume_from_exported_bytes` | `tests/fault_replica_import.rs` | Recover volume from exported bytes | Pass |

### 1.5 `opens3-filer` (1)

| Test | File | Coverage | Result |
|------|------|----------|--------|
| `read_blob_falls_back_to_second_replica` | `tests/read_failover.rs` | Fail over read to second replica | Pass |

### 1.6 Crates without automated unit tests

| Crate | Notes |
|-------|-------|
| `opens3` (CLI) | Covered by E2E scripts for startup and shell subcommands |
| `opens3-proto` | Generated code; covered indirectly via Volume gRPC |
| `opens3-store` | Covered indirectly via Filer/S3 E2E (redb / HttpStore) |

---

## 2. End-to-end scripts (`tests/`)

Per-script re-run results on the report date. `e2e_kill_replica` failed once when run serially right after `e2e_fault` due to fixed-port occupancy; after cleaning processes and re-running alone it **passed**.

| Script | Scenario | Result |
|--------|----------|--------|
| `e2e_fault.sh` | all-in-one read/write; second Volume + heal/balance; metrics/raft | **Pass** |
| `e2e_kill_replica.sh` | Kill standby during `replication=001` write → PUT fail-closed (503) | **Pass** |
| `e2e_clerk_failover.sh` | 3 Clerk Raft; kill leader; new leader can assign | **Pass** |
| `e2e_clerk_chaos.sh` | Clerk scale / kill primary-standby / log corruption / empty disk / lose quorum | **Pass** |
| `e2e_raft_snapshot.sh` | ClusterSnapshot sync; topology still usable after leader change | **Pass** |
| `e2e_seal_ec.sh` | write → seal → EC; read-back length check | **Pass** |
| `e2e_multi_s3.sh` | Dual S3 + shared Filer HttpStore cross read/write | **Pass** |
| `e2e_prod_hardening.sh` | JWT write path, Object Lock, ACL, ListMPU, HTTPS listen | **Pass** |
| `s3_compat/run.sh` | ListObjectsV2 pagination, DeleteObjects, MPU, policy/lifecycle; optional aws-cli | **Pass** |

### 2.1 `e2e_prod_hardening` sub-assertions

| Assertion | Result |
|-----------|--------|
| assign response includes `jwt`; write without Bearer → 401; with Bearer → success | Pass |
| Object Lock COMPLIANCE: delete object → 403 | Pass |
| Bucket canned ACL `public-read` GetAcl includes AllUsers | Pass |
| After CreateMultipartUpload, `GET ?uploads` lists UploadId | Pass |
| Clerk `--tls-cert/--tls-key` + `curl -k https://…/health` | Pass |

### 2.2 `s3_compat` sub-assertions

| Assertion | Result |
|-----------|--------|
| Put/GetObject | Pass |
| ListObjectsV2 `max-keys` + continuation-token | Pass |
| DeleteObjects XML | Pass |
| Multipart initiate / UploadPart / Complete | Pass |
| Put/Get Bucket Policy, Lifecycle | Pass |
| aws-cli `mb`/`cp` (when `aws` is present) | Pass |

---

## 3. Capability × test mapping

| Capability area | Primary coverage | Gaps / notes |
|-----------------|------------------|--------------|
| FileId / replication codes | common unit tests | — |
| Needle persistence / compact | volume needle | — |
| Volume full | volume_full | — |
| EC encode/decode / seal path | ec_roundtrip + e2e_seal_ec | No dedicated E2E for remote shard rebuild |
| Read failover | filer read_failover | — |
| Write replica fail-closed | e2e_kill_replica | — |
| Heal / Balance | heal_plan + e2e_fault | No dedicated assertion for balance “move then delete source” |
| Raft persistence / leader change | raft_persistence + failover + snapshot | — |
| Multi-S3 shared meta | e2e_multi_s3 | No Postgres-meta E2E |
| S3 basic APIs | s3_compat | — |
| Object Lock / ACL / ListMPU / JWT / TLS | e2e_prod_hardening + lock unit tests | No dedicated presigned SigV4 script |
| SSE-S3/KMS headers | Code path | **No automated assertion** |
| WebDAV `/dav` | Documented | **No automated assertion** |
| gRPC Volume TLS | — | **Not covered** (HTTP TLS tested) |

---

## 4. Statistics

| Category | Count | Passed this report |
|----------|-------|--------------------|
| Named Cargo tests | 16 | 16 |
| E2E / compat scripts | 8 | 8 |
| **Total** | **24** | **24** |

---

## 5. Known run caveats

1. Several E2Es use **fixed ports** (e.g. 39333/38080). Clear leftovers before serial runs: `pkill -f 'opens3 '`, or stagger execution.
2. `e2e_prod_hardening` needs local `openssl`, `python3`, `curl`.
3. The aws-cli section of `s3_compat` is optional; without `aws`, remaining curl assertions still count as pass.
4. Clippy warning in `opens3-volume` `ec.rs` unused assignment **does not affect test results**.

## Related docs

- [Development and testing](development.md) — how to run locally
- [S3 compatibility matrix](s3-compat.md) — API semantics status
- [Troubleshooting](troubleshooting.md) — manual fault drills
