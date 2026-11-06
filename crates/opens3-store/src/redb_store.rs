// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use crate::FilerStore;
use async_trait::async_trait;
use opens3_common::types::EntryMeta;
use opens3_common::util::{base_name, parent_path};
use opens3_common::{Error, Result};
use redb::{Database, ReadableTable, TableDefinition};
use std::path::Path;
use std::sync::Arc;

const ENTRIES: TableDefinition<&str, &[u8]> = TableDefinition::new("entries");
const KV: TableDefinition<&str, &[u8]> = TableDefinition::new("kv");

#[derive(Clone)]
pub struct RedbStore {
    db: Arc<Database>,
}

impl RedbStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let db = Database::create(path.as_ref())
            .map_err(|e| Error::Internal(format!("redb open: {e}")))?;
        let store = Self { db: Arc::new(db) };
        store.init()?;
        Ok(store)
    }

    fn init(&self) -> Result<()> {
        let txn = self
            .db
            .begin_write()
            .map_err(|e| Error::Internal(e.to_string()))?;
        {
            let _ = txn.open_table(ENTRIES).map_err(|e| Error::Internal(e.to_string()))?;
            let _ = txn.open_table(KV).map_err(|e| Error::Internal(e.to_string()))?;
        }
        txn.commit().map_err(|e| Error::Internal(e.to_string()))?;
        Ok(())
    }

    fn normalize(path: &str) -> String {
        let p = if path.is_empty() || path == "/" {
            "/".to_string()
        } else if path.starts_with('/') {
            path.trim_end_matches('/').to_string()
        } else {
            format!("/{}", path.trim_end_matches('/'))
        };
        if p.is_empty() {
            "/".into()
        } else {
            p
        }
    }

    fn kv_key(ns: &str, key: &str) -> String {
        format!("{ns}/{key}")
    }
}

#[async_trait]
impl FilerStore for RedbStore {
    async fn insert_entry(&self, path: &str, entry: &EntryMeta) -> Result<()> {
        let path = Self::normalize(path);
        let db = self.db.clone();
        let entry = entry.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write().map_err(|e| Error::Internal(e.to_string()))?;
            {
                let mut table = txn
                    .open_table(ENTRIES)
                    .map_err(|e| Error::Internal(e.to_string()))?;
                if table
                    .get(path.as_str())
                    .map_err(|e| Error::Internal(e.to_string()))?
                    .is_some()
                {
                    return Err(Error::AlreadyExists(path));
                }
                // ensure parents
                let mut parent = parent_path(&path).to_string();
                loop {
                    if parent == "/" {
                        if table
                            .get("/")
                            .map_err(|e| Error::Internal(e.to_string()))?
                            .is_none()
                        {
                            let meta = EntryMeta::directory("/");
                            let bytes = serde_json::to_vec(&meta)
                                .map_err(|e| Error::Internal(e.to_string()))?;
                            table
                                .insert("/", bytes.as_slice())
                                .map_err(|e| Error::Internal(e.to_string()))?;
                        }
                        break;
                    }
                    if table
                        .get(parent.as_str())
                        .map_err(|e| Error::Internal(e.to_string()))?
                        .is_none()
                    {
                        let meta = EntryMeta::directory(base_name(&parent));
                        let bytes = serde_json::to_vec(&meta)
                            .map_err(|e| Error::Internal(e.to_string()))?;
                        table
                            .insert(parent.as_str(), bytes.as_slice())
                            .map_err(|e| Error::Internal(e.to_string()))?;
                    }
                    parent = parent_path(&parent).to_string();
                }
                let bytes =
                    serde_json::to_vec(&entry).map_err(|e| Error::Internal(e.to_string()))?;
                table
                    .insert(path.as_str(), bytes.as_slice())
                    .map_err(|e| Error::Internal(e.to_string()))?;
            }
            txn.commit().map_err(|e| Error::Internal(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Internal(e.to_string()))?
    }

    async fn update_entry(&self, path: &str, entry: &EntryMeta) -> Result<()> {
        let path = Self::normalize(path);
        let db = self.db.clone();
        let entry = entry.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write().map_err(|e| Error::Internal(e.to_string()))?;
            {
                let mut table = txn
                    .open_table(ENTRIES)
                    .map_err(|e| Error::Internal(e.to_string()))?;
                let bytes =
                    serde_json::to_vec(&entry).map_err(|e| Error::Internal(e.to_string()))?;
                table
                    .insert(path.as_str(), bytes.as_slice())
                    .map_err(|e| Error::Internal(e.to_string()))?;
            }
            txn.commit().map_err(|e| Error::Internal(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Internal(e.to_string()))?
    }

    async fn find_entry(&self, path: &str) -> Result<Option<EntryMeta>> {
        let path = Self::normalize(path);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(|e| Error::Internal(e.to_string()))?;
            let table = txn
                .open_table(ENTRIES)
                .map_err(|e| Error::Internal(e.to_string()))?;
            match table
                .get(path.as_str())
                .map_err(|e| Error::Internal(e.to_string()))?
            {
                Some(v) => {
                    let meta: EntryMeta = serde_json::from_slice(v.value())
                        .map_err(|e| Error::Internal(e.to_string()))?;
                    Ok(Some(meta))
                }
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| Error::Internal(e.to_string()))?
    }

    async fn delete_entry(&self, path: &str) -> Result<bool> {
        let path = Self::normalize(path);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write().map_err(|e| Error::Internal(e.to_string()))?;
            let existed;
            {
                let mut table = txn
                    .open_table(ENTRIES)
                    .map_err(|e| Error::Internal(e.to_string()))?;
                existed = table
                    .remove(path.as_str())
                    .map_err(|e| Error::Internal(e.to_string()))?
                    .is_some();
            }
            txn.commit().map_err(|e| Error::Internal(e.to_string()))?;
            Ok(existed)
        })
        .await
        .map_err(|e| Error::Internal(e.to_string()))?
    }

    async fn list_directory(&self, dir: &str) -> Result<Vec<EntryMeta>> {
        let dir = Self::normalize(dir);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(|e| Error::Internal(e.to_string()))?;
            let table = txn
                .open_table(ENTRIES)
                .map_err(|e| Error::Internal(e.to_string()))?;
            let mut out = Vec::new();
            let iter = table.iter().map_err(|e| Error::Internal(e.to_string()))?;
            for item in iter {
                let (k, v) = item.map_err(|e| Error::Internal(e.to_string()))?;
                let path = k.value().to_string();
                if path == dir {
                    continue;
                }
                if parent_path(&path) == dir {
                    let mut meta: EntryMeta = serde_json::from_slice(v.value())
                        .map_err(|e| Error::Internal(e.to_string()))?;
                    meta.name = base_name(&path).to_string();
                    out.push(meta);
                }
            }
            out.sort_by(|a, b| a.name.cmp(&b.name));
            Ok(out)
        })
        .await
        .map_err(|e| Error::Internal(e.to_string()))?
    }

    async fn list_by_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, EntryMeta)>> {
        let prefix = if prefix.is_empty() {
            "/".to_string()
        } else {
            Self::normalize(prefix)
        };
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(|e| Error::Internal(e.to_string()))?;
            let table = txn
                .open_table(ENTRIES)
                .map_err(|e| Error::Internal(e.to_string()))?;
            let mut out = Vec::new();
            let iter = table.iter().map_err(|e| Error::Internal(e.to_string()))?;
            for item in iter {
                let (k, v) = item.map_err(|e| Error::Internal(e.to_string()))?;
                let path = k.value().to_string();
                if prefix != "/" && !path.starts_with(&prefix) {
                    continue;
                }
                let meta: EntryMeta = serde_json::from_slice(v.value())
                    .map_err(|e| Error::Internal(e.to_string()))?;
                // Include directories so empty folders (UI / S3 prefix markers) are visible.
                if path == "/" {
                    continue;
                }
                out.push((path, meta));
                if out.len() >= limit {
                    break;
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::Internal(e.to_string()))?
    }

    async fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()> {
        let k = Self::kv_key(ns, key);
        let db = self.db.clone();
        let value = value.to_vec();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write().map_err(|e| Error::Internal(e.to_string()))?;
            {
                let mut table = txn
                    .open_table(KV)
                    .map_err(|e| Error::Internal(e.to_string()))?;
                table
                    .insert(k.as_str(), value.as_slice())
                    .map_err(|e| Error::Internal(e.to_string()))?;
            }
            txn.commit().map_err(|e| Error::Internal(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Internal(e.to_string()))?
    }

    async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let k = Self::kv_key(ns, key);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(|e| Error::Internal(e.to_string()))?;
            let table = txn
                .open_table(KV)
                .map_err(|e| Error::Internal(e.to_string()))?;
            match table
                .get(k.as_str())
                .map_err(|e| Error::Internal(e.to_string()))?
            {
                Some(v) => Ok(Some(v.value().to_vec())),
                None => Ok(None),
            }
        })
        .await
        .map_err(|e| Error::Internal(e.to_string()))?
    }

    async fn kv_delete(&self, ns: &str, key: &str) -> Result<()> {
        let k = Self::kv_key(ns, key);
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_write().map_err(|e| Error::Internal(e.to_string()))?;
            {
                let mut table = txn
                    .open_table(KV)
                    .map_err(|e| Error::Internal(e.to_string()))?;
                let _ = table
                    .remove(k.as_str())
                    .map_err(|e| Error::Internal(e.to_string()))?;
            }
            txn.commit().map_err(|e| Error::Internal(e.to_string()))?;
            Ok(())
        })
        .await
        .map_err(|e| Error::Internal(e.to_string()))?
    }

    async fn kv_list(&self, ns: &str, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let pfx = Self::kv_key(ns, prefix);
        let ns_owned = ns.to_string();
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let txn = db.begin_read().map_err(|e| Error::Internal(e.to_string()))?;
            let table = txn
                .open_table(KV)
                .map_err(|e| Error::Internal(e.to_string()))?;
            let mut out = Vec::new();
            let iter = table.iter().map_err(|e| Error::Internal(e.to_string()))?;
            for item in iter {
                let (k, v) = item.map_err(|e| Error::Internal(e.to_string()))?;
                let key = k.value();
                if key.starts_with(&pfx) {
                    let short = key
                        .strip_prefix(&format!("{ns_owned}/"))
                        .unwrap_or(key)
                        .to_string();
                    out.push((short, v.value().to_vec()));
                }
            }
            Ok(out)
        })
        .await
        .map_err(|e| Error::Internal(e.to_string()))?
    }
}
