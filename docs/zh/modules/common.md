> 语言: [English](../../en/modules/common.md) · [中文](./common.md)

# Common（`opens3-common`）

## 作用

**无 I/O 服务**的共享领域层：所有运行时 crate 依赖它，保证 FileId / 副本 / 拓扑 / 错误 / 指标约定一致。

## 位置

`crates/opens3-common/src/`

| 模块 | 作用 |
|------|------|
| `fileid` | FileId 编码/解析：`{volumeId},{fileKey}{cookie}` |
| `replication` | 三位副本码 `XYZ` → 所需副本数与拓扑约束 |
| `topology` | `DataCenter` / `Rack` / `TopologyLocation` / `NodeId` |
| `types` | `VolumeMeta`、`VolumeState`、`EntryMeta`、`ChunkRef`、默认卷大小/分片大小 |
| `error` | 统一 `Error` / `Result`（含 HTTP/S3 状态映射） |
| `util` | MD5、路径拼接等小工具 |
| `jwt` | 可选 JWT 校验辅助 |
| `metrics` | 共享 Prometheus `REGISTRY` 与 Clerk/Volume/Filer 指标定义 |

## 关键常量（`types`）

| 常量 | 默认 | 含义 |
|------|------|------|
| `DEFAULT_VOLUME_SIZE` | 30 GiB | 单卷软上限 |
| `DEFAULT_CHUNK_SIZE` | 8 MiB | Filer 大文件分片 |
| `NEEDLE_HEADER_SIZE` | 16 | Needle 头长度 |
| `NEEDLE_CHECKSUM_SIZE` | 4 | CRC32 |

## Volume 状态机

```text
Writable ──(满/只读)──► ReadOnly ──(seal)──► Sealed ──(EC)──► EcEncoded
```

## 设计要点

- **纯库**：不监听端口；metrics 注册到进程级 `REGISTRY`，由各服务的 `/metrics` 导出。  
- **序列化友好**：核心结构带 `serde`，便于 Raft payload / HTTP JSON。  
- 新增跨组件概念时优先落在本 crate，避免 master↔volume↔filer 循环依赖。

## 依赖方

几乎所有 crate；**不要**反向依赖 master/volume/filer/s3。
