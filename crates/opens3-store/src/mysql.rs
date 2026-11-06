// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! MySQL / TiDB-backed FilerStore for multi-instance Filer/S3.
//!
//! TiDB speaks the MySQL protocol; use the same `--meta-backend mysql|tidb`
//! with a `mysql://` URL (TiDB default port 4000).

use crate::FilerStore;
use async_trait::async_trait;
use opens3_common::types::EntryMeta;
use opens3_common::util::{base_name, join_path, parent_path};
use opens3_common::{Error, Result};
use sqlx::mysql::MySqlPoolOptions;
use sqlx::{MySqlPool, Row};

#[derive(Clone)]
pub struct MysqlStore {
    pool: MySqlPool,
}

impl MysqlStore {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = MySqlPoolOptions::new()
            .max_connections(16)
            .connect(database_url)
            .await
            .map_err(|e| Error::Internal(format!("mysql/tidb connect: {e}")))?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<()> {
        // sqlx MySQL does not run multi-statement batches by default.
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS opens3_entries (
              path VARCHAR(2048) NOT NULL,
              meta JSON NOT NULL,
              PRIMARY KEY (path(768))
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Internal(format!("mysql migrate: {e}")))?;

        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS opens3_kv (
              ns VARCHAR(255) NOT NULL,
              `key` VARCHAR(512) NOT NULL,
              value LONGBLOB NOT NULL,
              PRIMARY KEY (ns, `key`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Internal(format!("mysql migrate: {e}")))?;
        Ok(())
    }
}

fn normalize(path: &str) -> String {
    if path.is_empty() {
        return "/".into();
    }
    let p = if path.starts_with('/') {
        path.trim_end_matches('/').to_string().replace("//", "/")
    } else {
        join_path("/", path)
    };
    if p.is_empty() {
        "/".into()
    } else {
        p
    }
}

#[async_trait]
impl FilerStore for MysqlStore {
    async fn insert_entry(&self, path: &str, entry: &EntryMeta) -> Result<()> {
        let path = normalize(path);
        // ensure parents
        let mut parent = parent_path(&path).to_string();
        while parent != "/" {
            let dir = EntryMeta::directory(base_name(&parent));
            let meta = serde_json::to_value(&dir)
                .map_err(|e| Error::Internal(e.to_string()))?;
            // INSERT IGNORE ≈ Postgres ON CONFLICT DO NOTHING
            sqlx::query(
                "INSERT IGNORE INTO opens3_entries(path, meta) VALUES (?, ?)",
            )
            .bind(&parent)
            .bind(meta)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
            parent = parent_path(&parent).to_string();
        }
        let root = EntryMeta::directory("/");
        let root_meta =
            serde_json::to_value(&root).map_err(|e| Error::Internal(e.to_string()))?;
        sqlx::query("INSERT IGNORE INTO opens3_entries(path, meta) VALUES ('/', ?)")
            .bind(root_meta)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;

        let meta = serde_json::to_value(entry).map_err(|e| Error::Internal(e.to_string()))?;
        let res = sqlx::query("INSERT IGNORE INTO opens3_entries(path, meta) VALUES (?, ?)")
            .bind(&path)
            .bind(meta)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        if res.rows_affected() == 0 {
            return Err(Error::AlreadyExists(path));
        }
        Ok(())
    }

    async fn update_entry(&self, path: &str, entry: &EntryMeta) -> Result<()> {
        let path = normalize(path);
        let meta = serde_json::to_value(entry).map_err(|e| Error::Internal(e.to_string()))?;
        sqlx::query(
            "INSERT INTO opens3_entries(path, meta) VALUES (?, ?)
             ON DUPLICATE KEY UPDATE meta = VALUES(meta)",
        )
        .bind(&path)
        .bind(meta)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(())
    }

    async fn find_entry(&self, path: &str) -> Result<Option<EntryMeta>> {
        let path = normalize(path);
        let row = sqlx::query("SELECT meta FROM opens3_entries WHERE path = ?")
            .bind(&path)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        match row {
            Some(r) => {
                let v: serde_json::Value = r.get("meta");
                let e: EntryMeta =
                    serde_json::from_value(v).map_err(|e| Error::Internal(e.to_string()))?;
                Ok(Some(e))
            }
            None => Ok(None),
        }
    }

    async fn delete_entry(&self, path: &str) -> Result<bool> {
        let path = normalize(path);
        let res = sqlx::query("DELETE FROM opens3_entries WHERE path = ?")
            .bind(&path)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(res.rows_affected() > 0)
    }

    async fn list_directory(&self, dir: &str) -> Result<Vec<EntryMeta>> {
        let dir = normalize(dir);
        let rows = sqlx::query(
            "SELECT path, meta FROM opens3_entries WHERE path LIKE CONCAT(?, '/%')",
        )
        .bind(&dir)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
        let mut out = Vec::new();
        for r in rows {
            let p: String = r.get("path");
            if parent_path(&p) != dir {
                continue;
            }
            let v: serde_json::Value = r.get("meta");
            let mut e: EntryMeta =
                serde_json::from_value(v).map_err(|e| Error::Internal(e.to_string()))?;
            e.name = base_name(&p).to_string();
            out.push(e);
        }
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn list_by_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, EntryMeta)>> {
        let prefix = if prefix.is_empty() {
            "/".to_string()
        } else {
            normalize(prefix)
        };
        let rows = sqlx::query(
            "SELECT path, meta FROM opens3_entries WHERE path LIKE CONCAT(?, '%') ORDER BY path LIMIT ?",
        )
        .bind(&prefix)
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
        let mut out = Vec::new();
        for r in rows {
            let p: String = r.get("path");
            let v: serde_json::Value = r.get("meta");
            let e: EntryMeta =
                serde_json::from_value(v).map_err(|e| Error::Internal(e.to_string()))?;
            if p == "/" {
                continue;
            }
            out.push((p, e));
        }
        Ok(out)
    }

    async fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()> {
        sqlx::query(
            "INSERT INTO opens3_kv(ns, `key`, value) VALUES (?, ?, ?)
             ON DUPLICATE KEY UPDATE value = VALUES(value)",
        )
        .bind(ns)
        .bind(key)
        .bind(value)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(())
    }

    async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let row = sqlx::query("SELECT value FROM opens3_kv WHERE ns = ? AND `key` = ?")
            .bind(ns)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(row.map(|r| r.get::<Vec<u8>, _>("value")))
    }

    async fn kv_delete(&self, ns: &str, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM opens3_kv WHERE ns = ? AND `key` = ?")
            .bind(ns)
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(())
    }

    async fn kv_list(&self, ns: &str, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let rows = sqlx::query(
            "SELECT `key`, value FROM opens3_kv WHERE ns = ? AND `key` LIKE CONCAT(?, '%') ORDER BY `key`",
        )
        .bind(ns)
        .bind(prefix)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<String, _>("key"), r.get::<Vec<u8>, _>("value")))
            .collect())
    }
}
