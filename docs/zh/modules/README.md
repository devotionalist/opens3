> 语言: [中文](./README.md) · [English](../en/modules/README.md)

# 模块文档

按 crate 说明 **作用、边界、内部结构、关键 API / 依赖**。系统级读写路径见 [架构总览](../architecture.md)。

| 模块 | Crate | 一句话 |
|------|-------|--------|
| [CLI](cli.md) | `opens3` | 进程入口：clerk / volume / s3（统一 Filer）/ filer / server / shell |
| [Common](common.md) | `opens3-common` | 共享领域类型与工具（无网络服务） |
| [Proto](proto.md) | `opens3-proto` | gRPC 契约 |
| [Clerk](clerk.md) | `opens3-clerk` | 集群控制面：拓扑、分配、Raft、修复 |
| [Volume](volume.md) | `opens3-volume` | 数据面：Needle 存储与复制 |
| → [Needle](needle.md) | （volume 内部） | `.dat` / `.idx` 字节布局与读写语义 |
| [Store](store.md) | `opens3-store` | Filer 元数据存储抽象 |
| [Filer](filer.md) | `opens3-filer` | 统一网关：S3 + HTTP/WebDAV + Admin |
| [S3](s3.md) | `opens3-filer`（`http_s3`） | S3 API 路由与鉴权（同 crate） |
| [客户端与网关](../gateways.md) | — | S3 SDK / FUSE / HTTP / WebDAV 接入 |

## 设计原则

1. **控制面 / 数据面分离**：Clerk 不转发对象字节流。  
2. **卷为分配单位**：扩容靠新 Volume Server + 新卷，而非重写旧对象。  
3. **热副本、冷 EC**：Put 路径同步复制；EC 是密封后的容量优化。  
4. **统一 Filer**：S3 与路径 API 同进程同端口；元数据可插拔（Postgres / MySQL·TiDB / redb / HTTP）。  
5. **共享类型下沉**：FileId、副本码、EntryMeta 只在 `opens3-common`，避免循环依赖。  
