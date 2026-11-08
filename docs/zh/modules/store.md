> 语言: [English](../../en/modules/store.md) · [中文](./store.md)

# Store（`opens3-store`）

## 作用

Filer / S3 的 **元数据存储抽象**：路径级 `EntryMeta` 与命名空间 KV（IAM、multipart、lifecycle、versioning 等）。对象字节 **不** 进 Store，只存 FileId / chunks 指针。

## 位置

`crates/opens3-store/src/`

| 文件 | 作用 |
|------|------|
| `lib.rs` | `FilerStore` trait |
| `redb_store.rs` | 单机嵌入式（默认）；K8s 需 RWO |
| `memory.rs` | 测试 / 无持久化 |
| `postgres.rs` | 多实例共享元数据（Postgres） |
| `mysql.rs` | 多实例共享元数据（MySQL / TiDB） |
| `http_store.rs` | 经远程 Filer HTTP 代理元数据操作 |

## `FilerStore` 接口

**路径 API**

- `insert_entry` / `update_entry` / `find_entry` / `delete_entry`  
- `list_directory` / `list_by_prefix`  

**KV API**（`ns` + `key`）

- `kv_put` / `kv_get` / `kv_delete` / `kv_list`  
- 用于 multipart uploadId、bucket policy、lifecycle、versioning 等  

## 选型建议

| 后端 | 适用 |
|------|------|
| **redb** | 单 Filer/S3 副本；最低依赖 |
| **memory** | 单元测试、临时演示 |
| **postgres** | 多 S3/Filer 共享元数据（见 [multi-filer](../multi-filer.md)） |
| **mysql** / **tidb** | MySQL 或 TiDB（MySQL 协议）；CLI 两个名字等价 |
| **http** | S3 进程不落本地 meta，转发到独立 Filer |

## 设计要点

- Trait 全 `async` + `Send + Sync`，便于 axum 共享 `Arc<dyn FilerStore>`。  
- 路径与 KV 分表/分树，避免对象 listing 与 multipart 状态互相干扰。  
- **不** 实现分布式锁；多写者需依赖 Postgres 事务语义或单写者部署。

## 依赖

`opens3-common`（`EntryMeta`）；被 `opens3-filer`（含 S3）/ CLI 使用。
