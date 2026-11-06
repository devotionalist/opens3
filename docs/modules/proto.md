# Proto（`opens3-proto`）

## 作用

用 Protobuf + tonic 定义 **ClerkService** / **VolumeService** 的 gRPC 契约，生成 Rust 类型与服务 trait。Volume needle 热路径（网关读写、同步副本）走 **gRPC**；Clerk 控制面仍以 HTTP JSON 为主。

## 位置

- IDL：`proto/opens3.proto`  
- Crate：`crates/opens3-proto`（`tonic::include_proto!("opens3")`）

## 服务概览

### ClerkService

| RPC | 作用 |
|-----|------|
| `Heartbeat` | Volume 上报拓扑与本地卷；返回待创建 volume id |
| `Assign` | 分配 FileId + 写入位置 |
| `LookupVolume` | volumeId → locations |
| `ListVolumes` / `SealVolume` / `RegisterEcShards` | 管理面 |
| `GetClusterInfo` | 集群摘要 |

### VolumeService

| RPC | 作用 |
|-----|------|
| `WriteNeedle` / `ReadNeedle` / `DeleteNeedle` | 对象字节 |
| `ReplicateNeedle` | 副本写入 |
| `CompactVolume` / `EncodeEc` | 维护 |
| `Health` | 探活 |

## 设计要点

- 消息里的 `Location` 与 HTTP JSON 字段对齐（`url` / `public_url` / DC / rack / `node_id`）。  
- HTTP 与 gRPC **共用**底层 `VolumeStore` / `ClusterState`，避免两套业务逻辑。  
- 改 IDL 后需重新 `cargo build -p opens3-proto`（build.rs 生成）。

## 依赖

`tonic` / `prost`；被 `opens3-volume` 引用。
