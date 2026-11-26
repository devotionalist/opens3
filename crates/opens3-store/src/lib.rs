// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Filer 元数据存储抽象与各后端实现（memory / redb / Postgres / MySQL / HTTP）。

pub mod http_store;
pub mod memory;
pub mod mysql;
pub mod postgres;
pub mod redb_store;

use async_trait::async_trait;
use opens3_common::types::EntryMeta;
use opens3_common::Result;

/// Filer 路径条目与 KV 元数据的统一存储接口；各后端可互换。
#[async_trait]
pub trait FilerStore: Send + Sync {
    /// 插入条目；路径已存在时应返回 AlreadyExists。
    async fn insert_entry(&self, path: &str, entry: &EntryMeta) -> Result<()>;
    /// 覆盖更新（或 upsert）条目元数据。
    async fn update_entry(&self, path: &str, entry: &EntryMeta) -> Result<()>;
    /// 按绝对路径查找；不存在返回 Ok(None)。
    async fn find_entry(&self, path: &str) -> Result<Option<EntryMeta>>;
    /// 删除条目；返回是否原先存在。
    async fn delete_entry(&self, path: &str) -> Result<bool>;
    /// 列出目录下一层子项（不含深层后代）。
    async fn list_directory(&self, dir: &str) -> Result<Vec<EntryMeta>>;
    /// 按路径前缀扫描，最多返回 limit 条（含目录）。
    async fn list_by_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, EntryMeta)>>;

    // —— KV：IAM / 配置 / multipart 等非路径命名空间 ——
    /// 写入命名空间 ns 下的 key。
    async fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()>;
    /// 读取 KV；不存在返回 Ok(None)。
    async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>>;
    /// 删除 KV（幂等）。
    async fn kv_delete(&self, ns: &str, key: &str) -> Result<()>;
    /// 列出 ns 下 key 前缀匹配的项。
    async fn kv_list(&self, ns: &str, prefix: &str) -> Result<Vec<(String, Vec<u8>)>>;
}

pub use http_store::HttpStore;
pub use memory::MemoryStore;
pub use mysql::MysqlStore;
pub use postgres::PostgresStore;
pub use redb_store::RedbStore;
