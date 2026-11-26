// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 进程内内存 FilerStore；单测 / `--memory` 用，重启即丢。

use crate::FilerStore;
use async_trait::async_trait;
use opens3_common::types::EntryMeta;
use opens3_common::util::{base_name, join_path, parent_path};
use opens3_common::{Error, Result};
use parking_lot::RwLock;
use std::collections::BTreeMap;
use std::sync::Arc;

/// 基于 BTreeMap 的内存元数据存储。
#[derive(Clone, Default)]
pub struct MemoryStore {
    entries: Arc<RwLock<BTreeMap<String, EntryMeta>>>, // 路径 → EntryMeta
    kv: Arc<RwLock<BTreeMap<String, Vec<u8>>>>,        // ns\0key → 原始字节
}

impl MemoryStore {
    /// 空库实例。
    pub fn new() -> Self {
        Self::default()
    }

    /// KV 复合键：用 NUL 分隔 ns 与 key，避免路径分隔符冲突。
    fn kv_key(ns: &str, key: &str) -> String {
        format!("{ns}\0{key}")
    }
}

#[async_trait]
impl FilerStore for MemoryStore {
    async fn insert_entry(&self, path: &str, entry: &EntryMeta) -> Result<()> {
        let path = normalize(path);
        let mut g = self.entries.write();
        if g.contains_key(&path) {
            return Err(Error::AlreadyExists(path));
        }
        // 自下而上补齐缺失的父目录
        let mut parent = parent_path(&path).to_string();
        while parent != "/" {
            if !g.contains_key(&parent) {
                g.insert(parent.clone(), EntryMeta::directory(base_name(&parent)));
            }
            parent = parent_path(&parent).to_string();
        }
        if !g.contains_key("/") {
            g.insert("/".into(), EntryMeta::directory("/"));
        }
        g.insert(path, entry.clone());
        Ok(())
    }

    async fn update_entry(&self, path: &str, entry: &EntryMeta) -> Result<()> {
        let path = normalize(path);
        let mut g = self.entries.write();
        g.insert(path, entry.clone());
        Ok(())
    }

    async fn find_entry(&self, path: &str) -> Result<Option<EntryMeta>> {
        let path = normalize(path);
        Ok(self.entries.read().get(&path).cloned())
    }

    async fn delete_entry(&self, path: &str) -> Result<bool> {
        let path = normalize(path);
        Ok(self.entries.write().remove(&path).is_some())
    }

    async fn list_directory(&self, dir: &str) -> Result<Vec<EntryMeta>> {
        let dir = normalize(dir);
        let g = self.entries.read();
        let mut out = Vec::new();
        for (p, e) in g.iter() {
            if p == &dir {
                continue; // 跳过目录自身
            }
            // 仅直接子项：parent(path) == dir
            if parent_path(p) == dir {
                let mut e = e.clone();
                e.name = base_name(p).to_string(); // 展示名取路径末段
                out.push(e);
            }
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
        let g = self.entries.read();
        let mut out = Vec::new();
        // BTree 有序扫描：前缀不匹配即可提前结束
        for (p, e) in g.range(prefix.clone()..) {
            if !p.starts_with(&prefix) && prefix != "/" {
                break;
            }
            if p == "/" {
                continue;
            }
            out.push((p.clone(), e.clone()));
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    async fn kv_put(&self, ns: &str, key: &str, value: &[u8]) -> Result<()> {
        self.kv
            .write()
            .insert(Self::kv_key(ns, key), value.to_vec());
        Ok(())
    }

    async fn kv_get(&self, ns: &str, key: &str) -> Result<Option<Vec<u8>>> {
        Ok(self.kv.read().get(&Self::kv_key(ns, key)).cloned())
    }

    async fn kv_delete(&self, ns: &str, key: &str) -> Result<()> {
        self.kv.write().remove(&Self::kv_key(ns, key));
        Ok(())
    }

    async fn kv_list(&self, ns: &str, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let pfx = Self::kv_key(ns, prefix);
        let g = self.kv.read();
        Ok(g
            .range(pfx.clone()..)
            .filter(|(k, _)| k.starts_with(&pfx) || k.starts_with(&format!("{ns}\0")))
            .filter(|(k, _)| k.starts_with(&format!("{ns}\0{prefix}")))
            .map(|(k, v)| {
                // 剥掉 ns\0 前缀，只返回业务 key
                let key = k.split_once('\0').map(|(_, k)| k.to_string()).unwrap_or_default();
                (key, v.clone())
            })
            .collect())
    }
}

/// 规范为以 `/` 开头、无尾斜杠的绝对路径；空串 → `/`。
fn normalize(path: &str) -> String {
    if path.is_empty() {
        return "/".into();
    }
    if path.starts_with('/') {
        path.trim_end_matches('/').to_string().replace("//", "/")
    } else {
        join_path("/", path)
    }
    .pipe(|s| if s.is_empty() { "/".into() } else { s })
}

/// 本地 pipe 辅助，便于在 normalize 末尾做空串兜底。
trait Pipe: Sized {
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R;
}

impl<T> Pipe for T {
    fn pipe<F, R>(self, f: F) -> R
    where
        F: FnOnce(Self) -> R,
    {
        f(self)
    }
}
