// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Remote FilerStore over Filer HTTP `/store/*` API — multiple S3 gateways share one Filer.

use crate::FilerStore;
use async_trait::async_trait;
use opens3_common::types::EntryMeta;
use opens3_common::{Error, Result};
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct HttpStore {
    base: String,
    http: reqwest::Client,
}

impl HttpStore {
    pub fn new(filer_url: impl Into<String>) -> Self {
        Self {
            base: filer_url.into().trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }
}

#[derive(Serialize, Deserialize)]
struct EntryBody {
    entry: EntryMeta,
}

#[derive(Deserialize)]
struct ListPrefixResp {
    items: Vec<ListItem>,
}

#[derive(Deserialize)]
struct ListItem {
    path: String,
    entry: EntryMeta,
}

#[derive(Deserialize)]
struct ListDirResp {
    entries: Vec<EntryMeta>,
}

#[derive(Deserialize)]
struct KvListResp {
    items: Vec<KvItem>,
}

#[derive(Deserialize)]
struct KvItem {
    key: String,
    value_b64: String,
}

#[async_trait]
impl FilerStore for HttpStore {
    async fn insert_entry(&self, path: &str, entry: &EntryMeta) -> Result<()> {
        let url = format!("{}/store/entry{}", self.base, path_encode(path));
        let resp = self
            .http
            .put(&url)
            .query(&[("mode", "insert")])
            .json(&EntryBody {
                entry: entry.clone(),
            })
            .send()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        if resp.status().as_u16() == 409 {
            return Err(Error::AlreadyExists(path.to_string()));
        }
        if !resp.status().is_success() {
            return Err(Error::Internal(resp.text().await.unwrap_or_default()));
        }
        Ok(())
    }

    async fn update_entry(&self, path: &str, entry: &EntryMeta) -> Result<()> {
        let url = format!("{}/store/entry{}", self.base, path_encode(path));
        let resp = self
            .http
            .put(&url)
            .query(&[("mode", "update")])
            .json(&EntryBody {
                entry: entry.clone(),
            })
            .send()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(Error::Internal(resp.text().await.unwrap_or_default()));
        }
        Ok(())
    }

    async fn find_entry(&self, path: &str) -> Result<Option<EntryMeta>> {
        let url = format!("{}/store/entry{}", self.base, path_encode(path));
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(Error::Internal(resp.text().await.unwrap_or_default()));
        }
        let body: EntryBody = resp
            .json()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(Some(body.entry))
    }

    async fn delete_entry(&self, path: &str) -> Result<bool> {
        let url = format!("{}/store/entry{}", self.base, path_encode(path));
        let resp = self
            .http
            .delete(&url)
            .send()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(resp.status().is_success())
    }

    async fn list_directory(&self, dir: &str) -> Result<Vec<EntryMeta>> {
        let url = format!("{}/store/list{}", self.base, path_encode(dir));
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(Error::Internal(resp.text().await.unwrap_or_default()));
        }
        let body: ListDirResp = resp
            .json()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(body.entries)
    }

    async fn list_by_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, EntryMeta)>> {
        let url = format!("{}/store/prefix", self.base);
        let resp = self
            .http
            .get(&url)
            .query(&[("prefix", prefix), ("limit", &limit.to_string())])
            .send()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(Error::Internal(resp.text().await.unwrap_or_default()));
        }
        let body: ListPrefixResp = resp
            .json()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(body
            .items
            .into_iter()
            .map(|i| (i.path, i.entry))
            .collect())
    }

    async fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()> {
        let url = format!("{}/store/kv/{ns}/{key}", self.base);
        let resp = self
            .http
            .put(&url)
            .header("content-type", "application/octet-stream")
            .body(value.to_vec())
            .send()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(Error::Internal(resp.text().await.unwrap_or_default()));
        }
        Ok(())
    }

    async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>> {
        let url = format!("{}/store/kv/{ns}/{key}", self.base);
        let resp = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(Error::Internal(resp.text().await.unwrap_or_default()));
        }
        Ok(Some(
            resp.bytes()
                .await
                .map(|b| b.to_vec())
                .map_err(|e| Error::Internal(e.to_string()))?,
        ))
    }

    async fn kv_delete(&self, ns: &str, key: &str) -> Result<()> {
        let url = format!("{}/store/kv/{ns}/{key}", self.base);
        let _ = self.http.delete(&url).send().await;
        Ok(())
    }

    async fn kv_list(&self, ns: &str, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        use base64::Engine;
        let url = format!("{}/store/kv/{ns}", self.base);
        let resp = self
            .http
            .get(&url)
            .query(&[("prefix", prefix)])
            .send()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        if !resp.status().is_success() {
            return Err(Error::Internal(resp.text().await.unwrap_or_default()));
        }
        let body: KvListResp = resp
            .json()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        let mut out = Vec::new();
        for i in body.items {
            let v = base64::engine::general_purpose::STANDARD
                .decode(i.value_b64)
                .map_err(|e| Error::Internal(e.to_string()))?;
            out.push((i.key, v));
        }
        Ok(out)
    }
}

fn path_encode(path: &str) -> String {
    if path.is_empty() || path == "/" {
        "/".into()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    }
}
