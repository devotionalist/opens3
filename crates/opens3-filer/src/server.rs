// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 统一网关：Filer 路径 API 与 S3 共用同一端口。

use crate::auth::AuthStore;
use crate::client::BlobClient;
use crate::encryption::{decrypt, encrypt, key_from_passphrase};
use crate::events::{EventBus, FilerEvent};
use crate::lifecycle::LifecycleConfig;
use crate::policy::PolicyStore;
use axum::body::Bytes;
use axum::Router;
use opens3_common::metrics::{encode_metrics, FilerMetrics, S3Metrics, REGISTRY};
use opens3_common::types::{AccessKey, EntryMeta};
use opens3_common::util::{base_name, join_path};
use opens3_store::FilerStore;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;
use tracing::info;

/// 启用的协议开关（Filer / S3）
#[derive(Clone, Copy, Debug)]
pub struct Protocols {
    pub filer: bool, // 是否挂载 Filer/WebDAV 路由
    pub s3: bool,    // 是否挂载 S3 兼容路由
}

impl Protocols {
    /// 仅 Filer
    pub const FILER: Self = Self {
        filer: true,
        s3: false,
    };
    /// Filer + S3
    pub const ALL: Self = Self {
        filer: true,
        s3: true,
    };
}

/// 网关服务核心状态：元数据存储、Blob 客户端、鉴权与策略等
#[derive(Clone)]
pub struct Server {
    pub store: Arc<dyn FilerStore>, // 元数据 / KV 存储
    pub blob: BlobClient,           // 向 Volume 读写 needle
    pub events: EventBus,           // PUT/DELETE 事件
    pub port: u16,                  // 监听端口
    pub encrypt_key: Option<[u8; 32]>, // SSE 数据加密密钥（可选）
    pub collection: String,         // 默认 collection（assign 用）
    pub filer_metrics: Arc<FilerMetrics>, // Filer 侧 Prometheus 指标
    pub auth: Arc<parking_lot::RwLock<AuthStore>>, // SigV4 AccessKey
    pub policies: Arc<parking_lot::RwLock<PolicyStore>>, // IAM / 桶策略
    pub lifecycles: Arc<parking_lot::RwLock<HashMap<String, LifecycleConfig>>>, // 桶 lifecycle
    pub s3_metrics: Arc<S3Metrics>, // S3 侧指标
    pub tls: Option<opens3_common::tls::TlsConfig>, // 可选 TLS
    pub kms_key: Option<[u8; 32]>,  // 简易 KMS 派生密钥
    pub protocols: Protocols,       // 路由协议开关
}

/// Axum 共享状态
#[derive(Clone)]
pub struct AppState {
    pub server: Server, // 网关实例
}

impl Server {
    /// 构造服务（无额外 Clerk peer）
    pub fn new_with_opts(
        store: Arc<dyn FilerStore>,
        clerk: String,
        replication: String,
        port: u16,
        encrypt_passphrase: Option<String>,
        jwt_secret: Option<String>,
        tls: Option<opens3_common::tls::TlsConfig>,
        access_key: Option<(String, String)>,
        kms_passphrase: Option<String>,
        protocols: Protocols,
    ) -> Self {
        Self::new_with_clerk_peers(
            store,
            clerk,
            Vec::<String>::new(),
            replication,
            port,
            encrypt_passphrase,
            jwt_secret,
            tls,
            access_key,
            kms_passphrase,
            protocols,
        )
    }

    /// 构造服务；可指定 Clerk peer 列表以便故障转移
    pub fn new_with_clerk_peers(
        store: Arc<dyn FilerStore>,
        clerk: String,
        clerk_peers: Vec<String>,
        replication: String,
        port: u16,
        encrypt_passphrase: Option<String>,
        jwt_secret: Option<String>,
        tls: Option<opens3_common::tls::TlsConfig>,
        access_key: Option<(String, String)>,
        kms_passphrase: Option<String>,
        protocols: Protocols,
    ) -> Self {
        // 有 access_key 时开启鉴权，并注入 root 身份
        let mut auth = AuthStore::new(access_key.is_some());
        if let Some((ak, sk)) = access_key {
            auth.upsert(AccessKey {
                access_key: ak,
                secret_key: sk,
                identity: "root".into(),
                actions: vec![
                    "Admin".into(),
                    "Read".into(),
                    "Write".into(),
                    "List".into(),
                ],
            });
        }
        let kms_key = kms_passphrase.map(|p| {
            use sha2::{Digest, Sha256};
            let mut out = [0u8; 32];
            out.copy_from_slice(&Sha256::digest(p.as_bytes())[..32]);
            out
        });
        Self {
            store,
            blob: BlobClient::new_with_peers(clerk, replication, clerk_peers)
                .with_jwt_secret(jwt_secret),
            events: EventBus::new(),
            port,
            encrypt_key: encrypt_passphrase.map(|p| key_from_passphrase(&p)),
            collection: String::new(),
            filer_metrics: FilerMetrics::register(&REGISTRY).expect("filer metrics"),
            auth: Arc::new(parking_lot::RwLock::new(auth)),
            policies: Arc::new(parking_lot::RwLock::new(PolicyStore::default())),
            lifecycles: Arc::new(parking_lot::RwLock::new(HashMap::new())),
            s3_metrics: S3Metrics::register(&REGISTRY).expect("s3 metrics"),
            tls,
            kms_key,
            protocols,
        }
    }

    /// 启动 HTTP(S) 服务；启用 S3 时加载持久化配置并跑 lifecycle 后台任务
    pub async fn serve(self) -> anyhow::Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.port).parse()?;
        if self.protocols.s3 {
            self.load_persisted_config().await;
            let life = self.clone();
            // 每 60s 扫描各桶 lifecycle
            tokio::spawn(async move {
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(60)).await;
                    let cfgs: Vec<_> = life
                        .lifecycles
                        .read()
                        .iter()
                        .map(|(b, c)| (b.clone(), c.clone()))
                        .collect();
                    for (bucket, cfg) in cfgs {
                        let store = life.store.clone();
                        let srv = life.clone();
                        let _ = crate::lifecycle::apply_bucket_lifecycle(
                            &store,
                            &bucket,
                            &cfg,
                            |path| {
                                let srv = srv.clone();
                                Box::pin(async move {
                                    let _ = srv.delete_path(&path).await;
                                })
                            },
                        )
                        .await;
                    }
                }
            });
        }

        let tls = self.tls.clone();
        let port = self.port;
        let protocols = self.protocols;
        let state = AppState { server: self };

        let mut app = Router::new()
            .route("/health", axum::routing::get(|| async { "ok" }))
            .route("/metrics", axum::routing::get(metrics_handler));

        if protocols.filer {
            app = app.merge(crate::http_filer::http_router());
        }
        if protocols.s3 {
            app = app.merge(crate::http_s3::http_router());
        }

        let app = app.with_state(state);
        info!(
            "server listening on {addr} tls={} filer={} s3={}",
            tls.is_some(),
            protocols.filer,
            protocols.s3
        );
        opens3_common::tls::serve_http(port, app, tls).await?;
        Ok(())
    }

    /// 从 KV 加载桶策略与 lifecycle 到内存
    async fn load_persisted_config(&self) {
        if let Ok(items) = self.store.kv_list("policy", "").await {
            let mut policies = self.policies.write();
            for (bucket, bytes) in items {
                if let Ok(doc) = serde_json::from_slice::<crate::policy::PolicyDocument>(&bytes) {
                    policies.bucket_policies.insert(bucket, doc);
                }
            }
        }
        if let Ok(items) = self.store.kv_list("lifecycle", "").await {
            let mut lives = self.lifecycles.write();
            for (bucket, bytes) in items {
                if let Ok(cfg) = serde_json::from_slice::<LifecycleConfig>(&bytes) {
                    lives.insert(bucket, cfg);
                }
            }
        }
    }

    /// 写入路径：分块落盘 + 更新元数据，并记录 Filer 指标
    pub async fn put_bytes(
        &self,
        path: &str,
        data: Bytes,
        ttl: u64,
    ) -> opens3_common::Result<EntryMeta> {
        let t0 = Instant::now();
        let nbytes = data.len() as u64;
        let result = self.put_bytes_inner(path, data, ttl).await;
        let label = if result.is_ok() { "ok" } else { "error" };
        self.filer_metrics
            .ops
            .with_label_values(&["put", label])
            .inc();
        if result.is_ok() {
            self.filer_metrics
                .bytes
                .with_label_values(&["in"])
                .inc_by(nbytes);
        }
        self.filer_metrics
            .request_duration
            .with_label_values(&["put"])
            .observe(t0.elapsed().as_secs_f64());
        result
    }

    /// put 核心：可选加密、chunk 写入 Volume、upsert EntryMeta、发 PUT 事件
    async fn put_bytes_inner(
        &self,
        path: &str,
        data: Bytes,
        ttl: u64,
    ) -> opens3_common::Result<EntryMeta> {
        let path = normalize_path(path);
        let mut payload = data.to_vec();
        let mut encrypted = false;
        if let Some(key) = &self.encrypt_key {
            payload = encrypt(key, &payload)?;
            encrypted = true;
        }
        let (chunks, etag, size) = self
            .blob
            .write_chunked(&self.collection, &payload, None, ttl)
            .await?;
        let name = base_name(&path).to_string();
        // 单 chunk 且非 meta 时用 file_id；多 chunk 走 chunks 列表
        let mut entry = if chunks.len() == 1 && !chunks[0].etag.starts_with("meta:") {
            EntryMeta::file(name, size, chunks[0].file_id.clone(), etag.clone())
        } else {
            let mut e = EntryMeta::file(name, size, String::new(), etag.clone());
            e.file_id = None;
            e.chunks = chunks;
            e
        };
        entry.encrypted = encrypted;
        entry.ttl_seconds = ttl;
        entry.size = data.len() as u64; // 对外 size 为明文长度
        if self.store.find_entry(&path).await?.is_some() {
            self.store.update_entry(&path, &entry).await?;
        } else {
            self.store.insert_entry(&path, &entry).await?;
        }
        self.events
            .publish(FilerEvent {
                event: "PUT".into(),
                path: path.clone(),
                size: entry.size,
                etag: entry.etag.clone(),
            })
            .await;
        Ok(entry)
    }

    /// 读取路径内容与元数据（含指标）
    pub async fn get_bytes(&self, path: &str) -> opens3_common::Result<(EntryMeta, Vec<u8>)> {
        let t0 = Instant::now();
        let result = self.get_bytes_inner(path).await;
        let label = if result.is_ok() { "ok" } else { "error" };
        self.filer_metrics
            .ops
            .with_label_values(&["get", label])
            .inc();
        if let Ok((_, ref data)) = result {
            self.filer_metrics
                .bytes
                .with_label_values(&["out"])
                .inc_by(data.len() as u64);
        }
        self.filer_metrics
            .request_duration
            .with_label_values(&["get"])
            .observe(t0.elapsed().as_secs_f64());
        result
    }

    /// get 核心：查元数据、读 chunk、按需解密
    async fn get_bytes_inner(&self, path: &str) -> opens3_common::Result<(EntryMeta, Vec<u8>)> {
        let path = normalize_path(path);
        let entry = self
            .store
            .find_entry(&path)
            .await?
            .ok_or_else(|| opens3_common::Error::NotFound(path.clone()))?;
        if entry.is_directory {
            return Err(opens3_common::Error::InvalidArgument(
                "is a directory".into(),
            ));
        }
        let mut data = self
            .blob
            .read_chunked(&entry.chunks, entry.file_id.as_deref())
            .await?;
        if entry.encrypted {
            if let Some(key) = &self.encrypt_key {
                data = decrypt(key, &data)?;
            }
        }
        Ok((entry, data))
    }

    /// 删除路径（含指标）
    pub async fn delete_path(&self, path: &str) -> opens3_common::Result<()> {
        let t0 = Instant::now();
        let result = self.delete_path_inner(path).await;
        let label = if result.is_ok() { "ok" } else { "error" };
        self.filer_metrics
            .ops
            .with_label_values(&["delete", label])
            .inc();
        self.filer_metrics
            .request_duration
            .with_label_values(&["delete"])
            .observe(t0.elapsed().as_secs_f64());
        result
    }

    /// delete 核心：删 Volume blob + 元数据，发 DELETE 事件；路径不存在视为成功
    async fn delete_path_inner(&self, path: &str) -> opens3_common::Result<()> {
        let path = normalize_path(path);
        if let Some(entry) = self.store.find_entry(&path).await? {
            if let Some(fid) = &entry.file_id {
                let _ = self.blob.delete_blob(fid).await;
            }
            for c in &entry.chunks {
                let _ = self.blob.delete_blob(&c.file_id).await;
            }
            self.store.delete_entry(&path).await?;
            self.events
                .publish(FilerEvent {
                    event: "DELETE".into(),
                    path: path.clone(),
                    size: 0,
                    etag: String::new(),
                })
                .await;
        }
        Ok(())
    }
}

/// 规范化路径：保证以 `/` 开头、去掉尾部 `/`（根除外）
fn normalize_path(path: &str) -> String {
    let p = if path.is_empty() || path == "/" {
        "/".to_string()
    } else if path.starts_with('/') {
        path.trim_end_matches('/').to_string()
    } else {
        join_path("/", path)
    };
    if p.is_empty() {
        "/".into()
    } else {
        p
    }
}

/// Prometheus `/metrics` 处理器
async fn metrics_handler() -> impl axum::response::IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        encode_metrics(&REGISTRY),
    )
}

/// 兼容别名：融合后的 [`Server`]
pub type FilerServer = Server;
/// 兼容别名：同 [`Server`]
pub type S3Server = Server;
/// 兼容别名：同 [`Server`]
pub type Gateway = Server;
