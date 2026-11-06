// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
pub mod http_store;
pub mod memory;
pub mod mysql;
pub mod postgres;
pub mod redb_store;

use async_trait::async_trait;
use opens3_common::types::EntryMeta;
use opens3_common::Result;

#[async_trait]
pub trait FilerStore: Send + Sync {
    async fn insert_entry(&self, path: &str, entry: &EntryMeta) -> Result<()>;
    async fn update_entry(&self, path: &str, entry: &EntryMeta) -> Result<()>;
    async fn find_entry(&self, path: &str) -> Result<Option<EntryMeta>>;
    async fn delete_entry(&self, path: &str) -> Result<bool>;
    async fn list_directory(&self, dir: &str) -> Result<Vec<EntryMeta>>;
    async fn list_by_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, EntryMeta)>>;

    // KV helpers for IAM / config / multipart
    async fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()>;
    async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>>;
    async fn kv_delete(&self, ns: &str, key: &str) -> Result<()>;
    async fn kv_list(&self, ns: &str, prefix: &str) -> Result<Vec<(String, Vec<u8>)>>;
}

pub use http_store::HttpStore;
pub use memory::MemoryStore;
pub use mysql::MysqlStore;
pub use postgres::PostgresStore;
pub use redb_store::RedbStore;
