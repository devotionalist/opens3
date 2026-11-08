> 语言: [English](../en/test-coverage.md) · [中文](./test-coverage.md)

# 测试覆盖报告

记录仓库内自动化测试清单、覆盖能力，以及最近一次全量跑通结果。

| 项 | 值 |
|----|----|
| 报告日期 | 2026-11-02 |
| 命令基线 | `cargo test --workspace` + `tests/e2e_*.sh` + `tests/s3_compat/run.sh` |
| 总览 | **Cargo 16 passed / 0 failed**；**E2E 8/8 passed**（串行跑时注意固定端口冲突，见下文） |

快速复跑：

```bash
cargo test --workspace
for t in e2e_fault e2e_kill_replica e2e_clerk_failover e2e_clerk_chaos e2e_seal_ec \
         e2e_multi_s3 e2e_raft_snapshot e2e_prod_hardening; do
  bash "tests/${t}.sh" || exit 1
done
bash tests/s3_compat/run.sh
```

---

## 1. Cargo 单元 / 集成测试

`cargo test --workspace`（本报告日）：**全部通过**。

### 1.1 `opens3-common`（4）

| 测试 | 文件 | 覆盖 | 结果 |
|------|------|------|------|
| `fileid::tests::roundtrip` | `fileid.rs` | FileId 编解码 | 通过 |
| `jwt::tests::roundtrip_jwt` | `jwt.rs` | Volume JWT 签发/校验 | 通过 |
| `metrics::tests::master_metrics_encode` | `metrics.rs` | Prometheus 文本导出 | 通过 |
| `integration_tests::fileid_and_replication` | `lib.rs` | FileId + Replication `010` | 通过 |

### 1.2 `opens3-s3`（4）

| 测试 | 文件 | 覆盖 | 结果 |
|------|------|------|------|
| `policy::tests::deny_beats_allow` | `policy.rs` | IAM Deny 优先于 Allow | 通过 |
| `lock::tests::parse_lock_xml` | `lock.rs` | Object Lock XML → 配置 | 通过 |
| `lock::tests::compliance_blocks_delete` | `lock.rs` | COMPLIANCE 保留期删拒绝 | 通过 |
| `lock::tests::governance_bypass` | `lock.rs` | GOVERNANCE + bypass 头 | 通过 |

### 1.3 `opens3-clerk`（2）

| 测试 | 文件 | 覆盖 | 结果 |
|------|------|------|------|
| `heal_plans_missing_replica` | `tests/heal_plan.rs` | 缺副本 heal 规划 | 通过 |
| `leader_propose_survives_restart` | `tests/raft_persistence.rs` | Raft 日志持久化与重启 | 通过 |

### 1.4 `opens3-volume`（5）

| 测试 | 文件 | 覆盖 | 结果 |
|------|------|------|------|
| `write_read_delete_compact` | `tests/needle_roundtrip.rs` | Needle 写读删 + compact | 通过 |
| `encode_and_reconstruct` | `tests/ec_roundtrip.rs` | Reed-Solomon EC 编解码 | 通过 |
| `second_write_fails_when_volume_full` | `tests/volume_full.rs` | 卷满拒绝写入 | 通过 |
| `bitrot_detected_on_checksum_mismatch` | `tests/fault_replica_import.rs` | CRC/校验失败检出 | 通过 |
| `recover_volume_from_exported_bytes` | `tests/fault_replica_import.rs` | 导出字节恢复卷 | 通过 |

### 1.5 `opens3-filer`（1）

| 测试 | 文件 | 覆盖 | 结果 |
|------|------|------|------|
| `read_blob_falls_back_to_second_replica` | `tests/read_failover.rs` | 主副本失败读切备 | 通过 |

### 1.6 无自动化单测的 crate

| Crate | 说明 |
|-------|------|
| `opens3`（CLI） | 由 E2E 脚本覆盖启动与 shell 子命令 |
| `opens3-proto` | 生成代码；经 Volume gRPC 间接覆盖 |
| `opens3-store` | 经 Filer/S3 E2E（redb / HttpStore）间接覆盖 |

---

## 2. 端到端脚本（`tests/`）

本报告日逐项复跑结果如下。`e2e_kill_replica` 在紧接 `e2e_fault` 串行时曾因固定端口占用失败；清理进程后单独复跑 **通过**。

| 脚本 | 覆盖场景 | 结果 |
|------|----------|------|
| `e2e_fault.sh` | all-in-one 写读；第二 Volume + heal/balance；metrics/raft | **通过** |
| `e2e_kill_replica.sh` | `replication=001` 写时杀备副本 → PUT fail-closed（503） | **通过** |
| `e2e_clerk_failover.sh` | 3 Clerk Raft；杀 leader；新主可 assign | **通过** |
| `e2e_clerk_chaos.sh` | Clerk 扩缩容 / 杀主备 / 日志损坏 / 空盘 / 丢多数派 | **通过** |
| `e2e_raft_snapshot.sh` | ClusterSnapshot 同步；切主后拓扑仍可用 | **通过** |
| `e2e_seal_ec.sh` | 写 → seal → EC；读回长度校验 | **通过** |
| `e2e_multi_s3.sh` | 双 S3 + 共享 Filer HttpStore 交叉读写 | **通过** |
| `e2e_prod_hardening.sh` | JWT 写路径、Object Lock、ACL、ListMPU、HTTPS 监听 | **通过** |
| `s3_compat/run.sh` | ListObjectsV2 分页、DeleteObjects、MPU、policy/lifecycle；可选 aws-cli | **通过** |

### 2.1 `e2e_prod_hardening` 子断言

| 断言 | 结果 |
|------|------|
| assign 响应含 `jwt`；无 Bearer 写 → 401；有 Bearer → 成功 | 通过 |
| Object Lock COMPLIANCE：删对象 → 403 | 通过 |
| Bucket canned ACL `public-read` GetAcl 含 AllUsers | 通过 |
| CreateMultipartUpload 后 `GET ?uploads` 列出 UploadId | 通过 |
| Clerk `--tls-cert/--tls-key` + `curl -k https://…/health` | 通过 |

### 2.2 `s3_compat` 子断言

| 断言 | 结果 |
|------|------|
| Put/GetObject | 通过 |
| ListObjectsV2 `max-keys` + continuation-token | 通过 |
| DeleteObjects XML | 通过 |
| Multipart initiate / UploadPart / Complete | 通过 |
| Put/Get Bucket Policy、Lifecycle | 通过 |
| aws-cli `mb`/`cp`（本机有 `aws` 时） | 通过 |

---

## 3. 能力 × 测试映射

| 能力域 | 主要覆盖 | 缺口 / 备注 |
|--------|----------|-------------|
| FileId / 副本码 | common 单测 | — |
| Needle 持久化 / compact | volume needle | — |
| 卷满 | volume_full | — |
| EC 编解码 / seal 路径 | ec_roundtrip + e2e_seal_ec | 缺 shard 远程重建无独立 E2E |
| 读故障切换 | filer read_failover | — |
| 写副本 fail-closed | e2e_kill_replica | — |
| Heal / Balance | heal_plan + e2e_fault | balance「搬迁删源」无专用断言 |
| Raft 持久化 / 切主 | raft_persistence + failover + snapshot | — |
| 多 S3 共享 meta | e2e_multi_s3 | Postgres meta 无 E2E |
| S3 基础 API | s3_compat | — |
| Object Lock / ACL / ListMPU / JWT / TLS | e2e_prod_hardening + lock 单测 | 预签名 SigV4 无独立脚本 |
| SSE-S3/KMS 头 | 代码路径 | **无自动化断言** |
| WebDAV `/dav` | 文档指引 | **无自动化断言** |
| gRPC Volume TLS | — | **未覆盖**（HTTP TLS 已测） |

---

## 4. 统计

| 类别 | 数量 | 本报告通过 |
|------|------|------------|
| Cargo 命名测试 | 16 | 16 |
| E2E / 兼容脚本 | 8 | 8 |
| **合计** | **24** | **24** |

---

## 5. 已知跑法注意

1. 多个 E2E 使用**固定端口**（如 39333/38080）。串行跑前先清残留：`pkill -f 'opens3 '`，或错开执行。
2. `e2e_prod_hardening` 依赖本机 `openssl`、`python3`、`curl`。
3. `s3_compat` 的 aws-cli 段为可选；无 `aws` 时其余 curl 断言仍算通过。
4. Clippy 告警 `opens3-volume` `ec.rs` unused assignment **不影响测试结果**。

## 相关文档

- [开发测试](development.md) — 如何本地跑
- [S3 兼容矩阵](s3-compat.md) — API 语义状态
- [故障排查](troubleshooting.md) — 手工故障演练
