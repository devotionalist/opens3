// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Shared Postgres-backed FilerStore for multi-instance Filer/S3.

use crate::FilerStore;
use async_trait::async_trait;
use opens3_common::types::EntryMeta;
use opens3_common::util::{base_name, join_path, parent_path};
use opens3_common::{Error, Result};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};

#[derive(Clone)]
pub struct PostgresStore {
    pool: PgPool,
}

impl PostgresStore {
    pub async fn connect(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(16)
            .connect(database_url)
            .await
            .map_err(|e| Error::Internal(format!("postgres connect: {e}")))?;
        let store = Self { pool };
        store.migrate().await?;
        Ok(store)
    }

    async fn migrate(&self) -> Result<()> {
        // sqlx Postgres rejects multi-statement batches in a single prepared query.
        for stmt in [
            r#"
            CREATE TABLE IF NOT EXISTS opens3_entries (
              path TEXT PRIMARY KEY,
              meta JSONB NOT NULL
            )
            "#,
            r#"
            CREATE TABLE IF NOT EXISTS opens3_kv (
              ns TEXT NOT NULL,
              key TEXT NOT NULL,
              value BYTEA NOT NULL,
              PRIMARY KEY (ns, key)
            )
            "#,
            r#"
            CREATE INDEX IF NOT EXISTS opens3_entries_path_prefix
              ON opens3_entries (path text_pattern_ops)
            "#,
        ] {
            sqlx::query(stmt)
                .execute(&self.pool)
                .await
                .map_err(|e| Error::Internal(format!("postgres migrate: {e}")))?;
        }
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
impl FilerStore for PostgresStore {
    async fn insert_entry(&self, path: &str, entry: &EntryMeta) -> Result<()> {
        let path = normalize(path);
        // ensure parents
        let mut parent = parent_path(&path).to_string();
        while parent != "/" {
            let dir = EntryMeta::directory(base_name(&parent));
            let meta = serde_json::to_value(&dir)
                .map_err(|e| Error::Internal(e.to_string()))?;
            sqlx::query(
                "INSERT INTO opens3_entries(path, meta) VALUES ($1, $2) ON CONFLICT (path) DO NOTHING",
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
        sqlx::query(
            "INSERT INTO opens3_entries(path, meta) VALUES ('/', $1) ON CONFLICT (path) DO NOTHING",
        )
        .bind(root_meta)
        .execute(&self.pool)
        .await
        .map_err(|e| Error::Internal(e.to_string()))?;

        let meta = serde_json::to_value(entry).map_err(|e| Error::Internal(e.to_string()))?;
        let res = sqlx::query(
            "INSERT INTO opens3_entries(path, meta) VALUES ($1, $2) ON CONFLICT (path) DO NOTHING",
        )
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
            "INSERT INTO opens3_entries(path, meta) VALUES ($1, $2)
             ON CONFLICT (path) DO UPDATE SET meta = EXCLUDED.meta",
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
        let row = sqlx::query("SELECT meta FROM opens3_entries WHERE path = $1")
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
        let res = sqlx::query("DELETE FROM opens3_entries WHERE path = $1")
            .bind(&path)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(res.rows_affected() > 0)
    }

    async fn list_directory(&self, dir: &str) -> Result<Vec<EntryMeta>> {
        let dir = normalize(dir);
        let rows = sqlx::query("SELECT path, meta FROM opens3_entries WHERE path LIKE $1 || '/%'")
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
            // Always derive display name from path (legacy parents stored full path as name).
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
            "SELECT path, meta FROM opens3_entries WHERE path LIKE $1 || '%' ORDER BY path LIMIT $2",
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
            "INSERT INTO opens3_kv(ns, key, value) VALUES ($1, $2, $3)
             ON CONFLICT (ns, key) DO UPDATE SET value = EXCLUDED.value",
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
        let row = sqlx::query("SELECT value FROM opens3_kv WHERE ns = $1 AND key = $2")
            .bind(ns)
            .bind(key)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(row.map(|r| r.get::<Vec<u8>, _>("value")))
    }

    async fn kv_delete(&self, ns: &str, key: &str) -> Result<()> {
        sqlx::query("DELETE FROM opens3_kv WHERE ns = $1 AND key = $2")
            .bind(ns)
            .bind(key)
            .execute(&self.pool)
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(())
    }

    async fn kv_list(&self, ns: &str, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let rows = sqlx::query(
            "SELECT key, value FROM opens3_kv WHERE ns = $1 AND key LIKE $2 || '%' ORDER BY key",
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
