// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use opens3_common::fileid::FileId;
use opens3_common::types::{ChunkRef, DEFAULT_CHUNK_SIZE};
use opens3_common::{volume_grpc_endpoint, util, Error, Result};
use opens3_proto::volume_service_client::VolumeServiceClient;
use opens3_proto::{DeleteNeedleRequest, ReadNeedleRequest, WriteNeedleRequest};
use parking_lot::RwLock;
use serde::Deserialize;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::Channel;

#[derive(Clone)]
pub struct BlobClient {
    /// Active Clerk base URL. Updated when a non-leader returns 307 + leader_url
    /// so subsequent calls stick to the Raft leader instead of redirecting forever.
    clerk: Arc<RwLock<String>>,
    /// Extra Clerk URLs to try after failover (same idea as Volume `--clerk-peers`).
    clerk_peers: Vec<String>,
    /// Clerk HTTP client: redirects disabled so we can pin the leader ourselves.
    pub http: reqwest::Client,
    /// Separate client for volume HTTP reads (may follow redirects).
    volume_http: reqwest::Client,
    pub default_replication: String,
    pub chunk_size: u64,
    /// When set, mint Bearer tokens for volume deletes (same secret as clerk/volume).
    pub jwt_secret: Option<String>,
    /// Cached gRPC channels keyed by endpoint (`http://host:18080`).
    grpc: Arc<dashmap::DashMap<String, Channel>>,
}

#[derive(Debug, Deserialize)]
pub struct AssignResp {
    // 分配响应
    pub fid: String, // 文件ID
    pub url: String, // URL
    #[serde(default, rename = "publicUrl")]
    pub public_url: String, // 公共URL
    #[serde(default)]
    pub locations: Vec<Loc>, // 位置
    /// Write token from clerk when volume JWT is enabled
    #[serde(default)]
    pub jwt: Option<String>, // 写入令牌
}

#[derive(Debug, Deserialize)]
struct Loc {
    // 位置
    url: String,
    #[serde(default, rename = "publicUrl")]
    pub public_url: String, // 公共URL
}

#[derive(Debug, Deserialize)]
struct LookupResp {
    // 查找响应
    #[serde(default)]
    pub locations: Vec<Loc>, // 位置
}

impl BlobClient {
    pub fn new(clerk: impl Into<String>, replication: impl Into<String>) -> Self {
        Self::new_with_peers(clerk, replication, Vec::<String>::new())
    }

    pub fn new_with_peers(
        clerk: impl Into<String>,
        replication: impl Into<String>,
        peers: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        let clerk = clerk.into();
        let mut clerk_peers: Vec<String> = peers
            .into_iter()
            .map(|p| p.into())
            .filter(|p| !p.trim().is_empty())
            .collect();
        if !clerk_peers
            .iter()
            .any(|p| p.trim_end_matches('/') == clerk.trim_end_matches('/'))
        {
            clerk_peers.insert(0, clerk.clone());
        }
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self {
            clerk: Arc::new(RwLock::new(clerk)),
            clerk_peers,
            http,
            volume_http: reqwest::Client::new(),
            default_replication: replication.into(),
            chunk_size: DEFAULT_CHUNK_SIZE,
            jwt_secret: None,
            grpc: Arc::new(dashmap::DashMap::new()),
        }
    }

    pub fn with_jwt_secret(mut self, secret: Option<String>) -> Self {
        self.jwt_secret = secret;
        self
    }

    /// Current Clerk base URL (may track the Raft leader after redirects).
    pub fn clerk_url(&self) -> String {
        self.clerk.read().clone()
    }

    fn remember_clerk(&self, url: &str) {
        let u = url.trim_end_matches('/').to_string();
        if u.is_empty() {
            return;
        }
        let mut g = self.clerk.write();
        if g.trim_end_matches('/') != u {
            tracing::info!(old = %*g, new = %u, "filer pinned clerk leader URL");
            *g = u;
        }
    }

    fn clerk_candidates(&self) -> Vec<String> {
        let cur = self.clerk.read().clone();
        let mut out = vec![cur];
        for p in &self.clerk_peers {
            if !out
                .iter()
                .any(|x| x.trim_end_matches('/') == p.trim_end_matches('/'))
            {
                out.push(p.clone());
            }
        }
        out
    }

    fn extract_leader(v: &serde_json::Value) -> Option<String> {
        v.get("leader")
            .or_else(|| v.get("leader_url"))
            .and_then(|x| x.as_str())
            .map(|s| s.trim_end_matches('/').to_string())
            .filter(|s| !s.is_empty())
    }

    /// GET a Clerk path, follow one leader redirect, and pin the leader URL.
    pub async fn clerk_get(&self, path_and_query: &str) -> Result<reqwest::Response> {
        let pq = path_and_query.trim_start_matches('/');
        let mut last_err = Error::Internal("no clerk reachable".into());
        for base in self.clerk_candidates() {
            let url = format!("{}/{}", base.trim_end_matches('/'), pq);
            match self.http.get(&url).send().await {
                Ok(resp) if resp.status().as_u16() == 307 || resp.status().as_u16() == 308 => {
                    let v: serde_json::Value = resp.json().await.unwrap_or_default();
                    let Some(lead) = Self::extract_leader(&v) else {
                        last_err = Error::Internal("clerk redirect without leader".into());
                        continue;
                    };
                    self.remember_clerk(&lead);
                    let follow = format!("{lead}/{pq}");
                    match self.http.get(&follow).send().await {
                        Ok(r) if r.status().is_success() => {
                            self.remember_clerk(&lead);
                            return Ok(r);
                        }
                        Ok(r) => {
                            last_err = Error::Internal(format!(
                                "clerk leader {}: {}",
                                lead,
                                r.status()
                            ));
                        }
                        Err(e) => last_err = Error::Internal(e.to_string()),
                    }
                }
                Ok(resp) if resp.status().is_success() => {
                    self.remember_clerk(&base);
                    return Ok(resp);
                }
                Ok(resp) => {
                    last_err = Error::Internal(format!("clerk {}: {}", url, resp.status()));
                }
                Err(e) => last_err = Error::Internal(e.to_string()),
            }
        }
        Err(last_err)
    }

    /// POST JSON to Clerk with the same leader-pinning behavior as [`clerk_get`].
    pub async fn clerk_post_json(
        &self,
        path: &str,
        body: &serde_json::Value,
    ) -> Result<reqwest::Response> {
        let pq = path.trim_start_matches('/');
        let mut last_err = Error::Internal("no clerk reachable".into());
        for base in self.clerk_candidates() {
            let url = format!("{}/{}", base.trim_end_matches('/'), pq);
            match self.http.post(&url).json(body).send().await {
                Ok(resp) if resp.status().as_u16() == 307 || resp.status().as_u16() == 308 => {
                    let v: serde_json::Value = resp.json().await.unwrap_or_default();
                    let Some(lead) = Self::extract_leader(&v) else {
                        last_err = Error::Internal("clerk redirect without leader".into());
                        continue;
                    };
                    self.remember_clerk(&lead);
                    let follow = format!("{lead}/{pq}");
                    match self.http.post(&follow).json(body).send().await {
                        Ok(r) if r.status().is_success() || r.status().is_client_error() => {
                            self.remember_clerk(&lead);
                            return Ok(r);
                        }
                        Ok(r) => {
                            last_err = Error::Internal(format!(
                                "clerk leader {}: {}",
                                lead,
                                r.status()
                            ));
                        }
                        Err(e) => last_err = Error::Internal(e.to_string()),
                    }
                }
                Ok(resp) if resp.status().is_success() || resp.status().is_client_error() => {
                    self.remember_clerk(&base);
                    return Ok(resp);
                }
                Ok(resp) => {
                    last_err = Error::Internal(format!("clerk {}: {}", url, resp.status()));
                }
                Err(e) => last_err = Error::Internal(e.to_string()),
            }
        }
        Err(last_err)
    }

    async fn grpc_client(&self, http_url: &str) -> Result<VolumeServiceClient<Channel>> {
        let ep = volume_grpc_endpoint(http_url)
            .ok_or_else(|| Error::Internal(format!("bad volume url {http_url}")))?;
        if let Some(ch) = self.grpc.get(&ep) {
            return Ok(VolumeServiceClient::new(ch.clone()));
        }
        let ch = Channel::from_shared(ep.clone())
            .map_err(|e| Error::Internal(e.to_string()))?
            .connect_timeout(Duration::from_secs(3))
            .timeout(Duration::from_secs(60))
            .connect()
            .await
            .map_err(|e| Error::Internal(format!("grpc connect {ep}: {e}")))?;
        self.grpc.insert(ep, ch.clone());
        Ok(VolumeServiceClient::new(ch))
    }

    /// Assign a new file to a volume and return its URL/locations.
    pub async fn assign(
        &self,
        collection: &str,
        replication: Option<&str>,
        ttl: u64, // 过期时间
    ) -> Result<AssignResp> {
        let repl = replication.unwrap_or(&self.default_replication);
        let pq = format!(
            "dir/assign?collection={}&replication={}&ttl={}",
            urlencoding_encode(collection),
            repl,
            ttl
        );
        let resp = self.clerk_get(&pq).await?;
        if !resp.status().is_success() {
            return Err(Error::Internal(format!(
                "assign failed: {}",
                resp.text().await.unwrap_or_default()
            )));
        }
        resp.json()
            .await
            .map_err(|e| Error::Internal(e.to_string()))
    }

    /// Lookup the URLs for a volume by its ID.
    pub async fn lookup_urls(&self, volume_id: u32) -> Result<Vec<String>> {
        let resp = self.clerk_get(&format!("dir/lookup/{volume_id}")).await?;
        if !resp.status().is_success() {
            return Err(Error::NotFound(format!("volume {volume_id}")));
        }
        let body: LookupResp = resp
            .json()
            .await
            .map_err(|e| Error::Internal(e.to_string()))?;
        Ok(body
            .locations
            .into_iter()
            .map(|l| {
                if !l.public_url.is_empty() {
                    l.public_url
                } else {
                    l.url
                }
            })
            .filter(|u| !u.is_empty())
            .collect())
    }

    fn loc_urls(a: &AssignResp) -> Vec<String> {
        if a.locations.is_empty() {
            vec![if !a.public_url.is_empty() {
                a.public_url.clone()
            } else {
                a.url.clone()
            }]
        } else {
            a.locations
                .iter()
                .map(|l| {
                    if !l.public_url.is_empty() {
                        l.public_url.clone()
                    } else {
                        l.url.clone()
                    }
                })
                .collect()
        }
    }

    /// Write a blob to a volume and return its ID/ETag.
    pub async fn write_blob(
        &self,
        collection: &str,
        data: &[u8], // 数据
        replication: Option<&str>, // 复制策略
        ttl: u64,                  // 过期时间
    ) -> Result<(String, String)> {
        let a = self.assign(collection, replication, ttl).await?; // 分配
        let locs = Self::loc_urls(&a);
        let primary = locs.first().cloned().unwrap_or(a.url.clone());
        let file_id: FileId = a.fid.parse()?;
        let replica_urls: Vec<String> = locs.iter().skip(1).cloned().collect();

        // gRPC only — primary sync-replicates via replica_urls.
        let mut client = self.grpc_client(&primary).await?;
        let mut req = tonic::Request::new(WriteNeedleRequest {
            volume_id: file_id.volume_id,
            file_key: file_id.file_key,
            cookie: file_id.cookie,
            data: data.to_vec(),
            ttl_seconds: ttl,
            is_replication: false,
            replica_urls,
        });
        if let Some(tok) = a.jwt.as_deref() {
            if let Ok(v) = format!("Bearer {tok}").parse() {
                req.metadata_mut().insert("authorization", v);
            }
        }
        let resp = client
            .write_needle(req)
            .await
            .map_err(|e| Error::Internal(format!("grpc write {primary}: {e}")))?;
        let r = resp.into_inner();
        let etag = if r.etag.is_empty() {
            util::md5_hex(data)
        } else {
            r.etag
        };
        Ok((a.fid, etag))
    }

    pub async fn read_blob(&self, fid: &str) -> Result<Vec<u8>> {
        let file_id: FileId = fid.parse()?;
        let urls = self.lookup_urls(file_id.volume_id).await?;
        if urls.is_empty() {
            return Err(Error::NotFound(fid.to_string()));
        }
        let mut last_err = Error::NotFound(fid.to_string());
        for (i, base) in urls.iter().enumerate() {
            match self.read_blob_at(base, &file_id).await {
                Ok(data) => {
                    if i > 0 {
                        tracing::warn!(
                            fid,
                            location = %base,
                            attempt = i + 1,
                            "read succeeded after trying earlier location(s)"
                        );
                    }
                    return Ok(data);
                }
                Err(e) => {
                    tracing::warn!(
                        fid,
                        location = %base,
                        error = %e,
                        "read failed; trying next location"
                    );
                    last_err = e;
                }
            }
        }
        Err(last_err)
    }

    /// Read one location: gRPC first, then HTTP `GET /{fid}` (CRC checked on volume).
    async fn read_blob_at(&self, base: &str, file_id: &FileId) -> Result<Vec<u8>> {
        let fid = file_id.fid();
        match self.grpc_client(base).await {
            Ok(mut client) => {
                let req = tonic::Request::new(ReadNeedleRequest {
                    volume_id: file_id.volume_id,
                    file_key: file_id.file_key,
                    cookie: file_id.cookie,
                });
                match client.read_needle(req).await {
                    Ok(resp) => return Ok(resp.into_inner().data),
                    Err(e) => {
                        // Fall through to HTTP for connect/version quirks; still failover.
                        tracing::debug!(location = %base, error = %e, "grpc read failed, try http");
                    }
                }
            }
            Err(e) => {
                tracing::debug!(location = %base, error = %e, "grpc connect failed, try http");
            }
        }

        let url = format!("{}/{}", base.trim_end_matches('/'), fid);
        let resp = self
            .volume_http
            .get(&url)
            .send()
            .await
            .map_err(|e| Error::Internal(format!("http read {url}: {e}")))?;
        if !resp.status().is_success() {
            return Err(Error::Internal(format!(
                "http read {url}: {}",
                resp.status()
            )));
        }
        resp.bytes()
            .await
            .map(|b| b.to_vec())
            .map_err(|e| Error::Internal(format!("http read body {url}: {e}")))
    }

    pub async fn delete_blob(&self, fid: &str) -> Result<()> {
        let file_id: FileId = fid.parse()?;
        let urls = self.lookup_urls(file_id.volume_id).await?;
        let delete_tok = self.jwt_secret.as_deref().and_then(|secret| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let claims = opens3_common::jwt::VolumeClaims {
                fid: fid.to_string(),
                exp: now + 600,
                op: "write".into(),
            };
            opens3_common::jwt::sign(secret, &claims).ok()
        });
        let mut last_err: Option<Error> = None;
        let mut any_ok = false;
        for base in urls {
            let mut client = match self.grpc_client(&base).await {
                Ok(c) => c,
                Err(e) => {
                    last_err = Some(e);
                    continue;
                }
            };
            let mut req = tonic::Request::new(DeleteNeedleRequest {
                volume_id: file_id.volume_id,
                file_key: file_id.file_key,
                cookie: file_id.cookie,
            });
            if let Some(tok) = delete_tok.as_ref() {
                if let Ok(v) = format!("Bearer {tok}").parse() {
                    req.metadata_mut().insert("authorization", v);
                }
            }
            match client.delete_needle(req).await {
                Ok(_) => any_ok = true,
                Err(e) => {
                    last_err = Some(Error::Internal(format!("grpc delete {base}/{fid}: {e}")));
                }
            }
        }
        if any_ok {
            Ok(())
        } else {
            Err(last_err.unwrap_or_else(|| Error::NotFound(fid.to_string())))
        }
    }

    /// Write a chunked blob to a volume and return its ID/ETag.
    pub async fn write_chunked(
        &self,
        collection: &str,
        data: &[u8], // 数据
        replication: Option<&str>,
        ttl: u64,
    ) -> Result<(Vec<ChunkRef>, String, u64)> {
        if data.len() as u64 <= self.chunk_size {
            let (fid, etag) = self.write_blob(collection, data, replication, ttl).await?;
            return Ok((
                vec![ChunkRef {
                    file_id: fid.clone(),
                    offset: 0,
                    size: data.len() as u64,
                    etag: etag.clone(),
                }],
                etag,
                data.len() as u64,
            ));
        }
        let mut chunks = Vec::new();
        let mut offset = 0u64;
        let mut etag_hasher = md5::Md5::default();
        use md5::Digest;
        while offset < data.len() as u64 {
            let end = ((offset + self.chunk_size) as usize).min(data.len());
            let slice = &data[offset as usize..end];
            let (fid, etag) = self.write_blob(collection, slice, replication, ttl).await?;
            etag_hasher.update(slice);
            chunks.push(ChunkRef {
                file_id: fid,
                offset,
                size: slice.len() as u64,
                etag,
            });
            offset = end as u64;
        }
        let etag = hex::encode(etag_hasher.finalize());
        // meta-chunk if many chunks
        if chunks.len() > 1000 {
            let meta = serde_json::to_vec(&chunks).map_err(|e| Error::Internal(e.to_string()))?;
            let (fid, _) = self.write_blob(collection, &meta, replication, ttl).await?;
            return Ok((
                vec![ChunkRef {
                    file_id: fid,
                    offset: 0,
                    size: data.len() as u64,
                    etag: format!("meta:{etag}"),
                }],
                etag,
                data.len() as u64,
            ));
        }
        Ok((chunks, etag, data.len() as u64))
    }

    /// Read a chunked blob from a volume and return its data.
    pub async fn read_chunked(&self, chunks: &[ChunkRef], file_id: Option<&str>) -> Result<Vec<u8>> {
        if let Some(fid) = file_id {
            if chunks.is_empty() {
                return self.read_blob(fid).await;
            }
        }
        let owned: Vec<ChunkRef>;
        let chunks = if chunks.len() == 1 && chunks[0].etag.starts_with("meta:") {
            let meta = self.read_blob(&chunks[0].file_id).await?;
            owned = serde_json::from_slice(&meta).map_err(|e| Error::Internal(e.to_string()))?;
            owned.as_slice()
        } else {
            chunks
        };
        let mut out = Vec::new();
        for c in chunks {
            let data = self.read_blob(&c.file_id).await?;
            out.extend_from_slice(&data);
        }
        Ok(out)
    }
}

fn urlencoding_encode(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            _ => format!("%{:02X}", c as u8),
        })
        .collect()
}
