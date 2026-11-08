> 语言: [中文](./volume.md) · [English](../en/modules/volume.md)

# Volume（`opens3-volume`）

## 作用

集群 **数据面**：在本地磁盘上以 **Needle 追加日志** 形式存对象，提供 HTTP（及 gRPC）读写、**同步复制**、compact、卷级 **Reed-Solomon EC**、卷导入/导出（heal 用）。

## 位置

`crates/opens3-volume/src/`

| 文件 | 作用 |
|------|------|
| `needle.rs` | 单卷 `.dat` / `.idx`：追加写、mmap 加载 idx、批量刷盘、软删、compact |
| `store.rs` | 多卷管理：`VolumeStore`、建卷、seal、compact、磁盘余量 |
| `ec.rs` | 密封卷 EC 编码（data + parity shard） |
| `server.rs` | HTTP 路由、心跳循环、复制、metrics、gRPC 适配 |

## 默认端口

- HTTP：`8080`（管理面：heartbeat 协作、compact/EC/heal 管理 API）  
- gRPC：`port + 10000`（如 `18080`）— 网关与副本 **needle 热路径唯一协议**

## 磁盘布局

```text
<data-dir>/
  {volumeId}.dat     # 追加 needle 数据
  {volumeId}.idx     # 追加日志：fileKey → offset/size/cookie/deleted（同 key 后者覆盖）
  ec/                # EC shard 输出目录（可选）
```

### Needle / Idx

字节级布局、字段含义与读写语义见 **[Needle 数据结构](needle.md)**。

摘要：

```text
.dat: [cookie|size|flags|pad|ttl][file_key|payload][crc32]   # header 16B；CRC 仅覆盖 payload
.idx: [file_key|offset|size|cookie|deleted|pad]              # 定长 28B，同 key 后者覆盖
```

- **追加写** idx；启动 **mmap** 重建内存 `HashMap`  
- **pread** 读 `.dat`（不移动写游标）；卷级 `RwLock`  
- 批量刷盘：默认每 64 次；`OPENS3_VOLUME_IDX_FLUSH_EVERY` / `OPENS3_VOLUME_SYNC`  
- **compact**：重写干净 `.dat` + 紧凑 `.idx`

## 写路径（gRPC）

1. Clerk `assign` 按 **分配策略** 在可写卷中选型（Admin 可改）：`random` / `round_robin` / `free_disk` / `least_size`  
2. 网关 `WriteNeedle`（带 `replica_urls` = 其余 location）  
3. 主节点本地 `store.write`  
4. 主节点 **并行 gRPC** `ReplicateNeedle` 扇出到全部副本  
5. 副本只落盘、不再二次转发；全部成功才返回，失败则回滚主副本  

## 读 / 删（gRPC）

- `ReadNeedle`：按 idx seek `.dat`，校验 cookie/CRC  
- `DeleteNeedle`：标记 deleted（空间靠 compact 回收）

## 运维 API

| 路径 | 作用 |
|------|------|
| `POST /admin/create_volume/{id}` | 建空卷 |
| `POST /admin/seal/{id}` | 密封（停止写入） |
| `POST /admin/compact/{id}` | 压缩剔除已删 needle |
| `POST /admin/ec/{id}` | EC 编码 |
| `POST /admin/replicate_volume/{id}?targets=&max_bps=` | 整卷复制到目标（heal；`max_bps` 平均限速，0=不限） |
| `POST /admin/import_volume_bin/{id}` | 二进制导入 `.dat`（header `X-OpenS3-Idx-B64`） |
| `GET /admin/volume_fp/{id}` | 卷指纹（重启对齐用） |
| `POST /admin/import_volume` | 接收整卷 dat/idx |
| `GET /status` / `/metrics` | 状态与指标 |

## 心跳

每 5s 向 Clerk `POST /cluster/heartbeat`，上报本地卷统计与 `free_disk_bytes`；失败置 `volume_heartbeat_fail=1`。

## 设计要点

- **追加写、顺序 I/O**：小文件友好，吞吐高。  
- **同步复制在 Volume 层完成**：Filer 只打一次主 URL。  
- **卷满 / seal 后不再写入**；新写入靠 Clerk 换卷。  
- EC 与热写解耦，避免 Put 路径做 RS 编码。

## 指标（节选）

`volume_count`、`max_volumes`、`free_disk_bytes`、`needle_ops_total`、`needle_bytes_total`、`replicate_total`、`compact_total`、`ec_total`。

## 依赖

`opens3-common`、`opens3-proto`；出站 HTTP 访问 Clerk，出站 gRPC 访问 peer Volume。
