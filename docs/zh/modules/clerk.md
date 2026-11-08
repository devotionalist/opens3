> 语言: [English](../../en/modules/clerk.md) · [中文](./clerk.md)

# Clerk（`opens3-clerk`）

## 作用

集群 **控制面**：维护 Volume Server 拓扑与卷元数据，负责 **FileId 分配（assign）**、**卷定位（lookup）**、**Raft 高可用**，以及 **缺副本 heal / 容量 balance** 调度。  
**不存储对象字节**，也不代理读写流量。

## 位置

`crates/opens3-clerk/src/`

| 文件 | 作用 |
|------|------|
| `server.rs` | HTTP API、`/metrics`、周期性 gauge / heal / 拓扑 propose |
| `cluster.rs` | 内存集群状态：servers、volumes、assign、lookup、心跳建卷 |
| `raft/` | openraft：append-only WAL 日志（`openraft-log.wal`）+ 状态机 / snapshot |
| `heal.rs` | 欠副本检测、复制计划；balance 迁移计划与执行 |

Clerk 在 K8s 上反复 CrashLoop / Ready 失败的根因与处置见 [故障排查 · Clerk RCA](../troubleshooting.md#clerk-反复故障-rca)。

## 默认端口

`9333`（HTTP）。就绪：`/ready`（有 leader 或已知 leader）；存活：`/health`。

## 核心职责

1. **Heartbeat**：Volume 上报 `node_id`、拓扑、本地卷统计；Clerk 合并 `VolumeMeta.locations`，并在容量未满时下发 `volume_ids_to_create`。  
2. **Assign**：仅 **leader**；在匹配副本码的**全部可写数据卷**中随机选一个，再取该卷 locations，生成 `volumeId + fileKey + cookie`（多 volume 同时读写时 PUT 会打散到各卷/节点）。  
3. **Lookup**：`volumeId → locations[]`（任意节点可读）。  
4. **Raft**：`assign`、关键变更等走 propose；多 Clerk 用 `peers` + `advertise-url`。  
5. **Heal / Balance**：leader 周期性 `plan_heal` → 调 Volume `replicate_volume`；`balance` 搬迁过热节点上的卷。

## 关键 HTTP API

| 路径 | 说明 |
|------|------|
| `GET/POST /dir/assign` | 分配 FileId |
| `GET /dir/lookup/{volume_id}` | 查卷位置 |
| `POST /cluster/heartbeat` | Volume 心跳 |
| `GET /cluster/status` | 集群 JSON |
| `POST /cluster/heal` / `balance` | 立即执行修复/均衡 |
| `GET`/`POST /cluster/heal-bandwidth` | heal/balance 推送带宽上限（`max_mbps` / `max_bps`，0=不限） |
| Volume `boot_id` via heartbeat | 变化则指纹对齐；未 `write_ready` 的节点不参与 assign |
| `POST /raft/*` | Raft RPC |
| `GET /metrics` | Prometheus |

## 设计要点

- **Leader 写、Follower 重定向**：非 leader 的 assign/heal/balance 返回 leader 提示。  
- **过期心跳裁剪**：长时间未见的 Volume Server 从拓扑摘除，触发 under-replicated。  
- **自动建卷**：heartbeat 时若节点本地卷数 `< max_volume_count`，Clerk 分配新 volume id 让节点创建。  
- **上线语义**：无 bootstrap 的新节点默认 **offline**（Web/STS 扩容）；Helm 声明的 ordinal 经 bootstrap 默认可 **online**，也可在 values 里指定 `replicaOf`。Admin 仍可随时上下线。  
- **上线/下线**：`POST /vol/online`、`POST /cluster/server/online`；离线卷与离线节点不承接新写入。  

- Raft 目录需持久化（PVC，禁止长期 emptyDir）；状态机与 snapshot 须随日志一起落盘，否则重启会全量 re-apply 日志导致 OOM。  
- K8s 必须配置稳定 `advertise-url`；多 Clerk STS 使用 `podManagementPolicy: Parallel`。  
- **Raft voter ID 一律由 `advertise-url` / peer URL 派生**（`node_id_from_url`），与 `--ip` 解耦；`--ip` 仅作展示名。禁止再把 `--ip` 名哈希和 URL 哈希同时写进 membership（曾导致幽灵 voter、切主 panic）。  
- **日志物理层为 append-only WAL**（`openraft-log.wal`）：热路径只追加 record；truncate/purge 写逻辑标记；空洞多时 rewrite compact。旧版 `openraft-log.json` 全量快照会在启动时自动迁移。

## 故障演练

加强版 Clerk 专用演练（本地进程，不依赖 K8s）：

```bash
bash tests/e2e_clerk_chaos.sh
```

| 阶段 | 注入 | 期望 |
|------|------|------|
| scale-out | 3-voter 配置下按 0→1→2 拉起 | 2/3 起可出 Leader；3/3 后 assign 可用 |
| scale-in | 永久停一个 follower | 2/3 仍可 assign；再拉起可回到 3 |
| node-crash | 杀 Leader / Follower，原盘重启 | 切主后 assign；重启节点重新入组 |
| disk-corrupt | 破坏 follower 的 `openraft-log.wal` | 坏节点起不来；幸存者继续 assign |
| disk-wipe | 清空 follower `raft-dir` 后重启 | 不脑裂；幸存者继续 assign |
| majority-loss | 只留一个原 follower | 无法选主；assign 不可用 |

更轻量的切主 / 快照用例见 `tests/e2e_clerk_failover.sh`、`tests/e2e_raft_snapshot.sh`。

## 指标（节选）

`opens3_clerk_is_leader`、`raft_term`、`under_replicated_volumes`、`assign_total`、`lookup_total`、`heal_*`、`balance_*`。详见 [监控](../monitoring.md)。

## 依赖

`opens3-common`；HTTP 客户端调用 Volume 执行 heal/balance。
