> 语言: [English](../en/architecture.md) · [中文](./architecture.md)

# 架构

OpenS3 采用控制面与数据面分离：热路径用卷级同步副本，冷卷可异步纠删码（EC）。代码按 crate 拆分，职责清晰、可独立部署。

## 系统总览

```text
  Clients（S3 SDK / FUSE / HTTP / WebDAV…）
                 │
                 ▼
           opens3 filer          单进程 / 单端口（默认 8333）
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

**生产拓扑**：Clerk（Raft）+ Volume + **统一 Filer**（S3 与路径 API 同进程）。Helm 只部署一个 `opens3-filer` Deployment；`opens3-s3` Service 仅为 DNS 兼容别名。详见 [客户端与网关](gateways.md)、[Kubernetes](kubernetes.md)。

Clerk **不在读热路径**：Filer 拿到 `volumeId → servers` 后直连 Volume；lookup 结果可缓存。

### 节点间协议

| 路径 | 协议 | 说明 |
|------|------|------|
| Filer → Volume（needle 读写删） | **gRPC**（HTTP 端口 +10000） | `WriteNeedle` / `ReadNeedle` / `DeleteNeedle` |
| Volume → Volume（同步副本） | **gRPC** `ReplicateNeedle` | 并行 |
| Filer / Volume → Clerk | HTTP | `assign` / `lookup` / `heartbeat` |
| 客户端 → Filer | HTTP（S3 / WebDAV / entries） | 对外协议不变 |

## Crate / 模块地图

| Crate | 角色 | 文档 |
|-------|------|------|
| [`opens3`](modules/cli.md) | CLI 二进制：启动子进程 / all-in-one / shell | [CLI](modules/cli.md) |
| [`opens3-common`](modules/common.md) | 共享类型：FileId、副本码、拓扑、错误、metrics | [common](modules/common.md) |
| [`opens3-proto`](modules/proto.md) | gRPC protobuf（Clerk / Volume 服务定义） | [proto](modules/proto.md) |
| [`opens3-clerk`](modules/clerk.md) | 拓扑、assign/lookup、Raft、heal/balance | [Clerk](modules/clerk.md) |
| [`opens3-volume`](modules/volume.md) | Needle 存储、复制、compact、EC | [Volume](modules/volume.md) |
| [`opens3-store`](modules/store.md) | Filer 元数据后端抽象与实现 | [Store](modules/store.md) |
| [`opens3-filer`](modules/filer.md) | 统一网关：S3 + HTTP/WebDAV + Admin | [Filer](modules/filer.md) / [S3](modules/s3.md) |

依赖方向（不允许反向依赖运行时组件）：

```text
opens3 (CLI)
  ├── opens3-clerk ─┐
  ├── opens3-volume ──┼── opens3-common
  └── opens3-filer  ──┤         ▲
        └── opens3-store ───────┘
            （http_s3：S3 API / SigV4 / Admin）
opens3-proto ──（tonic 生成，供 volume gRPC）
```

入口索引：[模块文档](modules/README.md)。

## 组件职责（运行时）

| 组件 | 职责 | 默认端口 |
|------|------|----------|
| **Clerk** | 拓扑、卷分配 `assign`、lookup、Raft HA、heal/balance 调度 | 9333 |
| **Volume** | Needle 追加存储、同步复制、compact、卷 EC、import/replicate | 8080（gRPC = port+10000） |
| **Filer** | 统一网关：S3 API、`/entries`、`/dav`、Admin UI；进程内 FilerStore | 8333 |

CLI：`opens3 s3` 启动 **Protocols::ALL**（S3 + 路径 API）；`opens3 filer` 仅路径 API（开发/兼容）。生产与 Helm 使用 `opens3 s3`（容器 args）跑统一 Filer。

### 边界（做什么 / 不做什么）

| 组件 | 做 | 不做 |
|------|----|------|
| Clerk | 卷级元数据、选主、调度复制 | 存对象字节、代理 GET/PUT body |
| Volume | 本地 `.dat/.idx`、同步副本、EC | 路径语义、IAM、桶策略 |
| Store | `EntryMeta` + KV | 对象数据、Raft |
| Filer | 路径↔FileId、分片、加密、S3/HTTP/WebDAV | 自管磁盘卷文件 |

## 数据模型

- **FileId**：`{volumeId},{fileKeyHex}{cookieHex}`，cookie 防猜解  
- **Needle**：卷内一条 blob（header + payload + CRC32）  
- **Volume**：默认约 30GiB 追加文件；状态 `Writable → ReadOnly/Sealed → EcEncoded`  
- **EntryMeta**：Filer 路径元数据（单 FileId 或 chunks 列表）  
- **副本码**（三位）：`XYZ`  
  - `X`：跨 DC 副本数  
  - `Y`：同 DC 跨 rack  
  - `Z`：同 rack 其他节点  
  - 例：`000` 单副本，`001` 同 rack 共 2 份，`010` 跨 rack 共 2 份  

## 写路径（小文件经 Filer）

1. Filer → Clerk `assign(collection, replication)` → FileId + 主 Volume URL + 副本 locations  
2. PUT needle 到主 Volume；主节点向全部副本 **并行同步扇出**，失败则整体失败  
3. FilerStore 写入路径 → FileId / chunks  

大文件按默认 **8MiB** 切 chunk，每 chunk 独立 FileId；过多 chunk 可打成 meta-chunk。

## 读路径

1. FilerStore 查元数据得 FileId(s)  
2. Clerk lookup（或缓存）得 Volume 地址  
3. 任选健康副本 GET；CRC 失败换副本  

## 横向扩容

1. 启动新 Volume Server，向 Clerk heartbeat  
2. Clerk 在空闲节点上 **创建新 volume**  
3. **新写入**落到新卷；**旧卷默认不搬迁**  
4. 需要腾挪时执行 `volume-balance` / `heal`（见 [运维](operations.md)）  
5. Filer 无状态：扩 `filer.replicas`（共享 Postgres / MySQL）  

与 MinIO「加 Server Pool」不同：OpenS3 以 **加盘/加 Volume 节点 + 新卷** 扩容。

## 一致性与可用性

| 路径 | 模型 |
|------|------|
| Needle 多副本写 | 同步复制；全部成功才返回成功 |
| Clerk 元数据 | Raft 选主；`assign` / heal / balance 仅 leader |
| Filer 元数据 | 共享 Postgres / MySQL（多副本）；单机开发可用 redb |
| 读 | 任意健康副本；可降级换 location |

## 异步 EC

密封卷后，Volume 可将 `.dat` 按 Reed-Solomon 切成 data+parity shard（`/admin/ec/{id}`）。热路径 Put **不做**同步 EC。

## 安全（生产）

| 能力 | 行为 |
|------|------|
| HTTPS | `--tls-cert` / `--tls-key`（或 `OPENS3_TLS_*`）进程内 rustls 终止；亦可 Ingress 终止 |
| Volume JWT | Clerk `--jwt-secret` 在 `/dir/assign` 签发；Volume 写/删校验 Bearer；Filer 同 secret 转发 |
| S3 Auth | SigV4 header + query 预签名；Bucket Policy ∩ canned ACL |
| Object Lock | COMPLIANCE / GOVERNANCE 保留期与 legal hold 执法 |
| SSE | `AES256` / 本地 `aws:kms`（`OPENS3_KMS_KEY`）+ Filer `--encrypt` |

## 观测

各组件 `GET /metrics`（Prometheus）。Filer 同端口同时暴露 `opens3_s3_*` 与 `opens3_filer_*`。指标表、Grafana、告警见 [监控](monitoring.md)。

## 相关文档

- [模块总览](modules/README.md)  
- [部署](deployment.md) / [Kubernetes](kubernetes.md)  
- [运维](operations.md) / [故障排查](troubleshooting.md)  
