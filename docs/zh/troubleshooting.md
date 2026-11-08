> 语言: [English](../en/troubleshooting.md) · [中文](./troubleshooting.md)

# 故障排查

## 症状速查

| 现象 | 可能原因 | 处理 |
|------|----------|------|
| `assign` 失败 / No writable volume | 无 Volume 心跳；卷已满；不在 leader | 查 `cluster-status`；确认 VS 在线；打到 leader |
| S3 403 AccessDenied | 开了鉴权未签名；Bucket Policy Deny | 配齐 AK/SK；检查 policy |
| S3 500 写对象 | Volume 不可达；卷只读/已密封 | `volume-list`；看 Volume 日志；heal |
| 读到一半 checksum mismatch | 磁盘 bitrot | 换副本读；从健康副本 replicate |
| Master 307/redirect | 当前节点非 leader | 跟随 JSON 里 `leader` URL；Filer/S3/Volume 会自动 pin，勿对 VIP 开 HTTP 自动跟随 |
| Filer/S3 写路径一直 307 / 超时 | 旧版客户端自动 follow Service VIP | 升级并用 `--clerk-peers`；确认 pin 到具体 Clerk Pod URL |
| Master `CrashLoopBackOff` / `OOMKilled` | Raft 日志膨胀 + 重启全量重放 | 见下方 [Master 反复故障 RCA](#master-反复故障-rca) |
| Master `Ready 0/1` / 双 Leader | Raft 目录用了 emptyDir 或 PVC 丢失 | 启用 `clerk.persistence`；勿冷启多副本空盘 |
| Master STS 起不来（多副本） | `OrderedReady` 等不到 quorum | Chart 已用 `podManagementPolicy: Parallel` |
| 多 Filer 元数据错乱 | 多进程抢同一 redb | 改用共享 Postgres / MySQL |
| 扩容后旧数据「丢了」 | 查错节点；lookup 缓存 | lookup volume；清客户端缓存 |

## 确认 Leader

```bash
curl -s http://10.0.0.1:9333/raft/status
curl -s http://10.0.0.2:9333/raft/status
opens3 shell raft-status --master http://10.0.0.1:9333
```

只有 `role=Leader` 的节点应接收 heartbeat/assign。

## Master 反复故障 RCA

本地 / Colima k3s 开发集群在 2026-11 连续多次出现 `opens3-master-0` 不可用（`Ready 0/1`、`CrashLoopBackOff`）。以下按时间线归纳**根因链条**，便于以后对照。

### 现象对照

| 次序 | 症状 | 直接证据 | 根因 |
|------|------|----------|------|
| A | Pod 重启后长时间 `Ready 0/1`，偶发双 Leader / Candidate | Raft 目录在 `emptyDir`；重启后 `last_log_id=None`，重新 `Initialize` | **Raft 状态未持久化** |
| B | 3 Master 冷启全部 Pending / 互等 | STS 默认 `OrderedReady`：`-0` 要等 peers 成 Leader，peers 又等 `-0` Ready | **启动策略与 Raft quorum 死锁** |
| C | `CrashLoopBackOff`，`Last State: OOMKilled`，`Exit Code: 137` | 启动日志 `re-apply ... T1-N0-3315`，听完 `master listening` 约 10s 被杀；limit 曾 1–2Gi | **未压缩的全量拓扑 Raft 日志在重启时整段重放 → OOM** |

三者常被当成「Master 又不稳了」，实际是**不同层**的问题叠在一起。

### 根因 A：Raft 落在 emptyDir

- Master 的 `--raft-dir` 若挂 `emptyDir`，每次 Pod 重建等价于**全新节点**。
- 单副本时表现为反复 bootstrap；多副本时各节点带着空日志竞选 → **双 Leader / 脑裂感 / Ready 长期失败**。
- **处理**：Helm `clerk.persistence.enabled=true`（PVC `raft-opens3-master-N`）。`values-dev.yaml` 已强制开启。

### 根因 B：OrderedReady × Raft 冷启

- 三投票人需要同时在线才能凑 quorum；`OrderedReady` 串行起 Pod，`-0` 永远等不到 `-1/-2`。
- **处理**：`deploy/helm/opens3/templates/clerk.yaml` 使用 `podManagementPolicy: Parallel`。

### 根因 C：拓扑快照写爆 Raft（OOM 主因）

机制（修复前）：

1. Leader **每 3 秒**把**整份** `ClusterSnapshot` `client_write` 进 Raft（每条日志是完整拓扑 JSON）。
2. Volume 心跳自动建卷（旧默认 `maxVolumes=32`，现已改为 `1`）让拓扑持续变大，日志条数与单条体积同步膨胀。
3. openraft 默认 `SnapshotPolicy::LogsSinceLast(5000)`：开发集群跑数小时只有约 3k 条时**从未触发压缩**。
4. 状态机 / Raft snapshot **只在内存**；磁盘仅有日志文件。重启时 `last_applied=None`，引擎对整段日志做 `re-apply`。
5. 数千次「全量拓扑反序列化 + 导入」峰值内存超过 limit → **OOMKilled (137)** → kubelet `CrashLoopBackOff`。

典型日志特征：

```text
openraft ... re-apply ... T1-N0-0 ... T1-N0-3315
opens3_master ... master listening on 0.0.0.0:9333
# 随后进程消失；kubectl describe: Last State OOMKilled, Exit Code 137
```

**已落地修复**（需重建镜像）：

| 改动 | 位置 | 作用 |
|------|------|------|
| 拓扑 propose 改为 30s + fingerprint 去重 | `server.rs` | 大幅减少无意义写放大 |
| `SnapshotPolicy::LogsSinceLast(32)` + 周期 `trigger().snapshot()` | `raft/mod.rs` / `server.rs` | 主动压缩日志 |
| 持久化 `openraft-sm.json` / `openraft-snapshot.bin` | `raft/store.rs` | 重启从 `last_applied` 恢复，避免全量 re-apply |
| 默认 `volume.maxVolumes: 1` | `values.yaml` / CLI | 每数据节点默认只预建 1 个数据卷 |

**已膨胀 PVC 的急救**（开发集群可接受丢控制面状态）：

```bash
kubectl -n opens3 scale sts/opens3-master --replicas=0
kubectl -n opens3 delete pvc raft-opens3-master-0
# 部署含上述修复的镜像后
kubectl -n opens3 scale sts/opens3-master --replicas=1
```

生产环境应先尝试升高 memory limit 并确认新版本能建成 snapshot / purge；盲目删 PVC 会丢掉 Master 侧拓扑与 Raft 任期信息（Volume 盘上数据仍在，需靠心跳重建拓扑）。

### 重启后健康检查

```bash
kubectl -n opens3 get pod opens3-master-0
kubectl -n opens3 describe pod opens3-master-0 | grep -E 'OOM|Exit Code|Restart'
kubectl -n opens3 logs opens3-master-0 | grep -E 're-apply|last_applied|build_snapshot|OOM'
kubectl -n opens3 exec opens3-master-0 -- ls -lah /data/raft/
# 期望同时存在：openraft-log.wal（或迁移前的 openraft-log.json）、openraft-sm.json、openraft-snapshot.bin
curl -s http://127.0.0.1:9333/cluster/status   # role=Leader / is_leader=true
```

健康重启日志应类似：`last_applied=T…` 且 **没有** 长达数千 index 的 `re-apply` 刷屏；随后可见周期 `build_snapshot`。

### 运维约束（避免复发）

1. **Master Raft 目录必须 PVC**；不要在多副本或会重启的环境用 emptyDir。  
2. **不要**在未压缩的旧 PVC 上长期跑「每秒写全量拓扑」的旧镜像。  
3. 需要预建更多数据卷时再提高 `volume.maxVolumes`；默认保持每节点 1 卷，避免拓扑/Raft 膨胀。  
4. 观察 `opens3_master_raft_commit_index` 与 PVC 上 `openraft-log.wal` 体积；只涨不缩且从不 compact 说明 snapshot/purge 失效。  
5. 单副本 Master 缩容 / 清 PVC 后，旧多副本 volume 数据与拓扑可能不一致，应用心跳 + heal 收敛，而不是再起一套空 Master 指望自动对齐。

## Volume 掉线后恢复

1. 修复磁盘/进程，用**相同** `--node-id` 与 `--dir` 拉起  
2. 等待 heartbeat  
3. `opens3 shell heal` 补缺失副本  
4. 用 S3 Get 抽检关键对象  

## 故障演练脚本

仓库提供：

```bash
bash tests/e2e_fault.sh
```

覆盖：启动集群 → 写入 → 加第二 Volume → heal/balance → 仍可读。

单元/故障向测试：

```bash
cargo test -p opens3-volume --test fault_replica_import
cargo test -p opens3-topology --test raft_persistence
cargo test -p opens3-common jwt
```

## 常见运维误区

1. **把 Volume 放在 NFS 上当主存储**：延迟与锁语义差，仅建议冷备  
2. **replication=001 但只有一个 Volume 进程**：第二副本无处可放，写入拓扑降级为单副本  
3. **改 DC/rack 标签不重建卷**：已有卷 locations 不会自动纠正，需 heal/balance  
4. **密封后又继续期望写入同一卷**：Sealed 卷拒绝写入，应使用新卷  

## 获取支持信息

提交 issue 时请附带：

```bash
opens3 --version
curl -s http://MASTER:9333/cluster/status
curl -s http://MASTER:9333/raft/status
RUST_LOG=debug 下的相关日志片段
```
