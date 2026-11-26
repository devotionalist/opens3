// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Volume 节点 HTTP + gRPC 服务：needle CRUD、vacuum、EC、卷复制与心跳。

use crate::ec::EcEncoder;
use crate::store::VolumeStore;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::Engine;
use opens3_common::fileid::FileId;
use opens3_common::metrics::{encode_metrics, VolumeMetrics, REGISTRY};
use opens3_common::Result as CommonResult;
use opens3_proto::volume_service_server::{VolumeService, VolumeServiceServer};
use opens3_proto::*;
use serde::Deserialize;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tonic::{Request, Response, Status};
use tracing::{info, warn};

/// Volume 节点运行时：本地存储、Clerk 心跳、复制与指标。
#[derive(Clone)]
pub struct VolumeServer {
    pub node_id: String, // 节点唯一 id
    pub store: VolumeStore, // 本地多卷存储
    /// Active clerk URL (may be updated when following redirects / peer list)
    /// 当前活跃 Clerk URL（跟随 redirect / peer 列表更新）
    pub clerk: Arc<parking_lot::RwLock<String>>,
    pub clerk_peers: Vec<String>, // Clerk 候选列表（含 leader 切换）
    pub public_url: String,       // 对外可达 URL
    pub data_center: String,      // 机房 / DC 拓扑
    pub rack: String,             // 机架拓扑
    pub ip: String,               // 绑定/上报 IP
    pub port: u16,                // HTTP 端口；gRPC 默认 port+10000
    pub max_volumes: u64,         // 本节点最大卷数
    pub http_client: reqwest::Client, // 心跳 / 复制 HTTP 客户端
    pub metrics: Arc<VolumeMetrics>,  // Prometheus 指标
    /// If set, write/delete require Authorization: Bearer <jwt>
    /// 若设置，写/删需 Bearer JWT
    pub jwt_secret: Option<String>,
    pub tls: Option<opens3_common::tls::TlsConfig>, // 可选 TLS
    /// Unique per process start; Clerk uses change to trigger fingerprint reconcile.
    /// 每次进程启动唯一；Clerk 借此触发指纹对齐
    pub boot_id: String,
    /// Cleared until Clerk heartbeat returns write_ready after restart align.
    /// 重启对齐完成前为 false，阻止写入
    pub write_ready: Arc<AtomicBool>,
    /// Deploy bootstrap: ask Clerk to mark this node online on first registration.
    /// 部署引导：首次注册时请求 Clerk 标为 online
    pub start_online: bool,
    /// Deploy bootstrap: ask Clerk to pair as replica of this node id (pod name).
    /// 部署引导：请求作为某节点的副本配对
    pub replica_of: Option<String>,
}

/// Axum 共享状态
#[derive(Clone)]
struct AppState {
    server: VolumeServer,
}

impl VolumeServer {
    /// 精简构造：无 peer / JWT / TLS / bootstrap
    pub fn new(
        node_id: String,
        data_dir: impl AsRef<std::path::Path>,
        clerk: String,
        ip: String,
        port: u16,
        data_center: String,
        rack: String,
        max_volumes: u64,
        max_volume_size: Option<u64>,
    ) -> CommonResult<Self> {
        Self::new_with_opts(
            node_id,
            data_dir,
            clerk,
            vec![],
            ip,
            port,
            data_center,
            rack,
            max_volumes,
            max_volume_size,
            None,
            None,
        )
    }

    /// 带 peer / JWT / TLS 的构造
    pub fn new_with_opts(
        node_id: String,
        data_dir: impl AsRef<std::path::Path>,
        clerk: String,
        clerk_peers: Vec<String>,
        ip: String,
        port: u16,
        data_center: String,
        rack: String,
        max_volumes: u64,
        max_volume_size: Option<u64>,
        jwt_secret: Option<String>,
        tls: Option<opens3_common::tls::TlsConfig>,
    ) -> CommonResult<Self> {
        Self::new_with_bootstrap(
            node_id,
            data_dir,
            clerk,
            clerk_peers,
            ip,
            port,
            data_center,
            rack,
            max_volumes,
            max_volume_size,
            jwt_secret,
            tls,
            false,
            None,
        )
    }

    /// 完整构造，含部署 bootstrap（start_online / replica_of）
    pub fn new_with_bootstrap(
        node_id: String,
        data_dir: impl AsRef<std::path::Path>,
        clerk: String,
        clerk_peers: Vec<String>,
        ip: String,
        port: u16,
        data_center: String,
        rack: String,
        max_volumes: u64,
        max_volume_size: Option<u64>,
        jwt_secret: Option<String>,
        tls: Option<opens3_common::tls::TlsConfig>,
        start_online: bool,
        replica_of: Option<String>,
    ) -> CommonResult<Self> {
        let store = VolumeStore::open(data_dir, max_volume_size)?;
        let scheme = opens3_common::tls::scheme(tls.as_ref());
        let public_url = format!("{scheme}://{ip}:{port}");
        let metrics = VolumeMetrics::register(&REGISTRY)?;
        metrics.max_volumes.set(max_volumes as i64);
        let mut peers = clerk_peers;
        // 确保主 clerk URL 在候选列表首位
        if !peers.iter().any(|p| p.trim_end_matches('/') == clerk.trim_end_matches('/')) {
            peers.insert(0, clerk.clone());
        }
        Ok(Self {
            node_id,
            store,
            clerk: Arc::new(parking_lot::RwLock::new(clerk)),
            clerk_peers: peers,
            public_url: public_url.clone(),
            data_center,
            rack,
            ip,
            port,
            max_volumes,
            http_client: reqwest::Client::new(),
            metrics,
            jwt_secret,
            tls,
            boot_id: uuid::Uuid::new_v4().to_string(),
            // Optimistic until first heartbeat; Clerk may clear via response.
            // 首心跳前为 false；Clerk 回 write_ready 后放开
            write_ready: Arc::new(AtomicBool::new(false)),
            start_online,
            replica_of: replica_of
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
        })
    }

    /// 写入门禁：指纹对齐完成前拒绝写
    fn ensure_write_ready(&self) -> Result<(), String> {
        if self.write_ready.load(Ordering::Relaxed) {
            Ok(())
        } else {
            Err("volume node reconciling fingerprints; writes blocked".into())
        }
    }

    /// 汇总各卷统计并刷新 Prometheus gauges
    fn refresh_gauges(&self) {
        let ids = self.store.list_ids();
        let mut size = 0u64;
        let mut files = 0u64;
        let mut deleted = 0u64;
        for id in &ids {
            if let Ok((s, f, d, read_only, sealed)) = self.store.volume_stats(*id) {
                size += s;
                files += f;
                deleted += d;
                let state = if sealed {
                    "sealed"
                } else if read_only {
                    "readonly"
                } else {
                    "writable"
                };
                let id_s = id.to_string();
                self.metrics
                    .detail_size
                    .with_label_values(&[&id_s, state])
                    .set(s as i64);
                self.metrics
                    .detail_files
                    .with_label_values(&[&id_s, state])
                    .set(f as i64);
            }
        }
        self.metrics.volume_count.set(ids.len() as i64);
        self.metrics.total_size_bytes.set(size as i64);
        self.metrics.total_files.set(files as i64);
        self.metrics.deleted_files.set(deleted as i64);
        self.metrics
            .free_disk_bytes
            .set(self.store.free_disk_bytes() as i64);
    }

    /// 启动 HTTP + gRPC，并周期心跳
    pub async fn serve(self) -> anyhow::Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.port).parse()?;
        // gRPC prefers port+10000; wrap into a high but valid range if that would overflow u16
        // gRPC 优先 port+10000；溢出则映射到高位合法端口
        let grpc_port = self.port.checked_add(10000).unwrap_or_else(|| {
            40000 + (self.port % 20000)
        });
        let grpc_addr: SocketAddr = format!("0.0.0.0:{grpc_port}").parse()?;

        let tls = self.tls.clone();
        let port = self.port;
        let node_id = self.node_id.clone();
        let has_tls = tls.is_some();
        let state = AppState {
            server: self.clone(),
        };
        let app = Router::new()
            .route("/status", get(status_handler))
            .route("/health", get(|| async { "ok" }))
            .route("/admin/create_volume/{id}", post(create_volume_handler))
            .route("/admin/seal/{id}", post(seal_handler))
            .route("/admin/compact/{id}", post(compact_handler))
            .route("/admin/vacuum/{id}/check", post(vacuum_check_handler))
            .route("/admin/vacuum/{id}/compact", post(vacuum_compact_handler))
            .route("/admin/vacuum/{id}/commit", post(vacuum_commit_handler))
            .route("/admin/vacuum/{id}/cleanup", post(vacuum_cleanup_handler))
            .route("/admin/ec/{id}", post(ec_handler))
            .route("/admin/replicate_volume/{id}", post(replicate_volume_handler))
            .route("/admin/import_volume", post(import_volume_handler))
            .route(
                "/admin/import_volume_bin/{id}",
                post(import_volume_bin_handler),
            )
            .route("/admin/volume_fp/{id}", get(volume_fp_handler))
            .route("/admin/drop_volume/{id}", post(drop_volume_handler))
            .route("/metrics", get(volume_metrics))
            .route("/{fid}", get(read_handler).put(write_handler).delete(delete_handler))
            .with_state(state);

        // 后台：每 5s 心跳 + 刷新 gauges
        let vs = self.clone();
        tokio::spawn(async move {
            loop {
                if let Err(e) = vs.heartbeat_once().await {
                    warn!("heartbeat failed: {e}");
                    vs.metrics.heartbeat_fail.set(1);
                } else {
                    vs.metrics.heartbeat_fail.set(0);
                }
                vs.refresh_gauges();
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        let grpc_svc = VolumeServiceServer::new(GrpcVolume(self.clone()));
        tokio::spawn(async move {
            info!("volume grpc listening on {grpc_addr}");
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(grpc_svc)
                .serve(grpc_addr)
                .await
            {
                warn!("volume grpc stopped: {e}");
            }
        });

        info!("volume http listening on {addr} node={node_id} tls={has_tls}");
        opens3_common::tls::serve_http(port, app, tls).await?;
        Ok(())
    }

    /// 向 Clerk 上报本节点卷指纹；跟随 307/308 切 leader
    async fn heartbeat_once(&self) -> anyhow::Result<()> {
        let mut volumes = Vec::new();
        for id in self.store.list_ids() {
            if let Ok((size, file_count, deleted_count, read_only, sealed)) =
                self.store.volume_stats(id)
            {
                let fingerprint = format!("{size}:{file_count}:{deleted_count}");
                volumes.push(serde_json::json!({
                    "id": id,
                    "size": size,
                    "file_count": file_count,
                    "deleted_count": deleted_count,
                    "read_only": read_only,
                    "sealed": sealed,
                    "fingerprint": fingerprint,
                }));
            }
        }
        let mut body = serde_json::json!({
            "node_id": self.node_id,
            "ip": self.ip,
            "port": self.port,
            "public_url": self.public_url,
            "data_center": self.data_center,
            "rack": self.rack,
            "max_volume_count": self.max_volumes,
            "free_disk_bytes": self.store.free_disk_bytes(),
            "boot_id": self.boot_id,
            "online": self.start_online,
            "volumes": volumes,
        });
        if let Some(src) = &self.replica_of {
            body.as_object_mut()
                .unwrap()
                .insert("replica_of".into(), serde_json::json!(src));
        }
        let mut last_err = None;
        let mut resp_opt = None;
        // 当前 clerk + peers，去重后逐个尝试
        let candidates = {
            let cur = self.clerk.read().clone();
            let mut c = vec![cur];
            for p in &self.clerk_peers {
                if !c.iter().any(|x| x.trim_end_matches('/') == p.trim_end_matches('/')) {
                    c.push(p.clone());
                }
            }
            c
        };
        for clerk_url in candidates {
            let url = format!("{}/cluster/heartbeat", clerk_url.trim_end_matches('/'));
            match self.http_client.post(&url).json(&body).send().await {
                Ok(resp) if resp.status().as_u16() == 307 || resp.status().as_u16() == 308 => {
                    // 非 leader：跟随 Location/leader 字段重试
                    let v: serde_json::Value = resp.json().await.unwrap_or_default();
                    if let Some(leader) = v.get("leader").and_then(|x| x.as_str()) {
                        let lead = leader.trim_end_matches('/').to_string();
                        if !lead.is_empty() {
                            *self.clerk.write() = lead.clone();
                            let follow = format!("{lead}/cluster/heartbeat");
                            match self.http_client.post(&follow).json(&body).send().await {
                                Ok(r) => {
                                    resp_opt = Some(r);
                                    break;
                                }
                                Err(e) => last_err = Some(e.to_string()),
                            }
                        }
                    }
                }
                Ok(resp) if resp.status().is_success() => {
                    *self.clerk.write() = clerk_url;
                    resp_opt = Some(resp);
                    break;
                }
                Ok(resp) => last_err = Some(format!("status {}", resp.status())),
                Err(e) => last_err = Some(e.to_string()),
            }
        }
        let resp = resp_opt.ok_or_else(|| {
            anyhow::anyhow!(last_err.unwrap_or_else(|| "no clerk reachable".into()))
        })?;
        if resp.status().is_success() {
            let v: serde_json::Value = resp.json().await.unwrap_or_default();
            // Clerk 可下发待创建卷 id
            if let Some(ids) = v.get("volume_ids_to_create").and_then(|x| x.as_array()) {
                for id in ids {
                    if let Some(id) = id.as_u64() {
                        let _ = self.store.create_volume(id as u32);
                    }
                }
            }
            if let Some(ready) = v.get("write_ready").and_then(|x| x.as_bool()) {
                self.write_ready.store(ready, Ordering::Relaxed);
            }
        }
        Ok(())
    }

    /// Primary fans out in parallel to all replica URLs.
    /// Targets write locally via ReplicateNeedle and must not fan out again.
    /// 主副本并行扇出到各 replica；目标端只本地写，不再二次扇出
    pub async fn replicate_to(&self, urls: &[String], fid: &FileId, data: &[u8], ttl: u64) -> bool {
        let self_url = self.public_url.trim_end_matches('/').to_string();
        let bearer = self.jwt_secret.as_deref().and_then(|secret| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let claims = opens3_common::jwt::VolumeClaims {
                fid: fid.fid(),
                exp: now + 600,
                op: "write".into(),
            };
            opens3_common::jwt::sign(secret, &claims).ok()
        });
        let targets: Vec<String> = urls
            .iter()
            .map(|u| u.trim_end_matches('/').to_string())
            .filter(|u| u != &self_url && !u.is_empty()) // 跳过自身
            .collect();
        if targets.is_empty() {
            return true;
        }
        let futs = targets.into_iter().map(|url| {
            let data = data.to_vec();
            let bearer = bearer.clone();
            let fid = *fid;
            async move {
                let Some(ep) = opens3_common::volume_grpc_endpoint(&url) else {
                    warn!("replicate: bad volume url {url}");
                    return false;
                };
                let mut client =
                    match opens3_proto::volume_service_client::VolumeServiceClient::connect(
                        ep.clone(),
                    )
                    .await
                    {
                        Ok(c) => c,
                        Err(e) => {
                            warn!("grpc connect {ep} failed: {e}");
                            return false;
                        }
                    };
                let mut req = tonic::Request::new(ReplicateNeedleRequest {
                    volume_id: fid.volume_id,
                    file_key: fid.file_key,
                    cookie: fid.cookie,
                    data,
                    ttl_seconds: ttl,
                });
                if let Some(tok) = bearer.as_ref() {
                    if let Ok(v) = format!("Bearer {tok}").parse() {
                        req.metadata_mut().insert("authorization", v);
                    }
                }
                match client.replicate_needle(req).await {
                    Ok(resp) => {
                        if resp.into_inner().ok {
                            true
                        } else {
                            warn!("grpc replicate to {ep} returned ok=false");
                            false
                        }
                    }
                    Err(e) => {
                        warn!("grpc replicate to {ep} failed: {e}");
                        false
                    }
                }
            }
        });
        let results = futures::future::join_all(futs).await;
        results.into_iter().all(|ok| ok)
    }
}

/// gRPC 元数据 JWT 校验（fid / op 须匹配或为 `*`）
fn grpc_check_jwt(server: &VolumeServer, meta: &tonic::metadata::MetadataMap, fid: &str, op: &str) -> std::result::Result<(), Status> {
    let Some(secret) = server.jwt_secret.as_deref() else {
        return Ok(()); // 未配置 JWT 则跳过
    };
    let auth = meta
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = auth
        .strip_prefix("Bearer ")
        .or_else(|| auth.strip_prefix("bearer "))
        .ok_or_else(|| Status::unauthenticated("missing bearer token"))?;
    let claims = opens3_common::jwt::verify(secret, token)
        .map_err(|e| Status::unauthenticated(e))?;
    if claims.fid != fid && claims.fid != "*" {
        return Err(Status::permission_denied("fid mismatch"));
    }
    if claims.op != op && claims.op != "*" {
        return Err(Status::permission_denied("op mismatch"));
    }
    Ok(())
}

/// GET /status — 节点概览
async fn status_handler(State(st): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "node_id": st.server.node_id,
        "public_url": st.server.public_url,
        "volumes": st.server.store.list_ids(),
        "free_disk_bytes": st.server.store.free_disk_bytes(),
        "boot_id": st.server.boot_id,
        "write_ready": st.server.write_ready.load(Ordering::Relaxed),
    }))
}

/// GET /admin/volume_fp/{id} — 单卷指纹
async fn volume_fp_handler(
    State(st): State<AppState>,
    Path(id): Path<u32>,
) -> impl IntoResponse {
    match st.server.store.volume_stats(id) {
        Ok((size, file_count, deleted_count, read_only, sealed)) => {
            let fingerprint = format!("{size}:{file_count}:{deleted_count}");
            Json(serde_json::json!({
                "id": id,
                "size": size,
                "file_count": file_count,
                "deleted_count": deleted_count,
                "read_only": read_only,
                "sealed": sealed,
                "fingerprint": fingerprint,
            }))
            .into_response()
        }
        Err(e) => (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::NOT_FOUND),
            e.to_string(),
        )
            .into_response(),
    }
}

/// PUT /{fid} 查询参数：TTL / 复制标记 / 副本 URL 列表
#[derive(Deserialize)]
struct WriteQuery {
    ttl: Option<u64>,             // 过期秒数
    replication: Option<String>,  // "1" = 已是复制写，勿再扇出
    locations: Option<String>,    // 逗号分隔副本 URL
}

/// HTTP Authorization Bearer JWT 校验
fn check_volume_jwt(st: &AppState, headers: &HeaderMap, fid: &str, op: &str) -> Result<(), String> {
    let Some(secret) = st.server.jwt_secret.as_deref() else {
        return Ok(());
    };
    let auth = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = auth
        .strip_prefix("Bearer ")
        .or_else(|| auth.strip_prefix("bearer "))
        .ok_or_else(|| "missing bearer token".to_string())?;
    let claims = opens3_common::jwt::verify(secret, token)?;
    if claims.fid != fid && claims.fid != "*" {
        return Err("fid mismatch".into());
    }
    if claims.op != op && claims.op != "*" {
        return Err("op mismatch".into());
    }
    Ok(())
}

/// PUT /{fid} — 写 needle；可选并行复制，失败则回滚主本
async fn write_handler(
    State(st): State<AppState>,
    Path(fid): Path<String>,
    Query(q): Query<WriteQuery>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    if let Err(e) = st.server.ensure_write_ready() {
        return (StatusCode::SERVICE_UNAVAILABLE, e).into_response();
    }
    if let Err(e) = check_volume_jwt(&st, &headers, &fid, "write") {
        return (StatusCode::UNAUTHORIZED, e).into_response();
    }
    let fid: FileId = match fid.parse() {
        Ok(f) => f,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    if let Err(e) = st.server.store.create_volume(fid.volume_id) {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    let t0 = Instant::now();
    match st.server.store.write(
        fid.volume_id,
        fid.file_key,
        fid.cookie,
        &body,
        q.ttl.unwrap_or(0) as u32,
    ) {
        Ok((size, etag)) => {
            st.server
                .metrics
                .needle_ops
                .with_label_values(&["write", "ok"])
                .inc();
            st.server
                .metrics
                .needle_bytes
                .with_label_values(&["write"])
                .inc_by(size);
            // replicate if locations provided and not already a replication write
            // 非复制入口且带 locations 时扇出；失败回滚主本
            if q.replication.as_deref() != Some("1") {
                if let Some(locs) = q.locations.as_ref() {
                    let urls: Vec<String> = locs
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect();
                    let ok = st
                        .server
                        .replicate_to(&urls, &fid, &body, q.ttl.unwrap_or(0))
                        .await;
                    st.server
                        .metrics
                        .replicate_total
                        .with_label_values(&[if ok { "ok" } else { "error" }])
                        .inc();
                    if !ok {
                        // rollback primary so client can retry cleanly
                        let _ = st.server.store.delete(fid.volume_id, fid.file_key, fid.cookie);
                        return (
                            StatusCode::SERVICE_UNAVAILABLE,
                            "replication failed".to_string(),
                        )
                            .into_response();
                    }
                }
            }
            st.server
                .metrics
                .request_duration
                .with_label_values(&["write"])
                .observe(t0.elapsed().as_secs_f64());
            Json(serde_json::json!({"size": size, "etag": etag, "fid": fid.fid()})).into_response()
        }
        Err(e) => {
            st.server
                .metrics
                .needle_ops
                .with_label_values(&["write", "error"])
                .inc();
            (
                StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
                .into_response()
        }
    }
}

/// GET /{fid} — 读 needle；本地失败则 EC 回退（4+2 或 2+1）
async fn read_handler(State(st): State<AppState>, Path(fid): Path<String>) -> impl IntoResponse {
    let fid: FileId = match fid.parse() {
        Ok(f) => f,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let t0 = Instant::now();
    // Prefer local dat; fall back to EC reconstruct (default 4+2 layout used by admin/ec)
    match st
        .server
        .store
        .read_with_ec_fallback(fid.volume_id, fid.file_key, fid.cookie, 4, 2)
        .or_else(|_| {
            st.server
                .store
                .read_with_ec_fallback(fid.volume_id, fid.file_key, fid.cookie, 2, 1)
        }) {
        Ok(data) => {
            st.server
                .metrics
                .needle_ops
                .with_label_values(&["read", "ok"])
                .inc();
            st.server
                .metrics
                .needle_bytes
                .with_label_values(&["read"])
                .inc_by(data.len() as u64);
            st.server
                .metrics
                .request_duration
                .with_label_values(&["read"])
                .observe(t0.elapsed().as_secs_f64());
            let etag = opens3_common::util::md5_hex(&data);
            (
                StatusCode::OK,
                [
                    (axum::http::header::ETAG, format!("\"{etag}\"")),
                    (
                        axum::http::header::CONTENT_TYPE,
                        "application/octet-stream".into(),
                    ),
                ],
                data,
            )
                .into_response()
        }
        Err(e) => {
            st.server
                .metrics
                .needle_ops
                .with_label_values(&["read", "error"])
                .inc();
            (
                StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
                .into_response()
        }
    }
}

/// DELETE /{fid} — 软删；接受 write 或 delete JWT
async fn delete_handler(
    State(st): State<AppState>,
    Path(fid): Path<String>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = st.server.ensure_write_ready() {
        return (StatusCode::SERVICE_UNAVAILABLE, e).into_response();
    }
    if let Err(e) = check_volume_jwt(&st, &headers, &fid, "delete") {
        // accept write tokens for delete as well (assign issues write)
        // assign 签发的是 write，删也接受
        if check_volume_jwt(&st, &headers, &fid, "write").is_err() {
            return (StatusCode::UNAUTHORIZED, e).into_response();
        }
    }
    let fid: FileId = match fid.parse() {
        Ok(f) => f,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let t0 = Instant::now();
    match st.server.store.delete(fid.volume_id, fid.file_key, fid.cookie) {
        Ok(()) => {
            st.server
                .metrics
                .needle_ops
                .with_label_values(&["delete", "ok"])
                .inc();
            st.server
                .metrics
                .request_duration
                .with_label_values(&["delete"])
                .observe(t0.elapsed().as_secs_f64());
            StatusCode::NO_CONTENT.into_response()
        }
        Err(e) => {
            st.server
                .metrics
                .needle_ops
                .with_label_values(&["delete", "error"])
                .inc();
            st.server
                .metrics
                .request_duration
                .with_label_values(&["delete"])
                .observe(t0.elapsed().as_secs_f64());
            (
                StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
                .into_response()
        }
    }
}

/// POST /admin/create_volume/{id}
async fn create_volume_handler(
    State(st): State<AppState>,
    Path(id): Path<u32>,
) -> impl IntoResponse {
    match st.server.store.create_volume(id) {
        Ok(()) => Json(serde_json::json!({"ok": true, "id": id})).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// 整卷复制查询：目标 URL 列表与可选限速
#[derive(Deserialize)]
struct ReplicateVolQuery {
    targets: String, // 逗号分隔目标 volume URL
    /// Average push rate limit in bytes/sec (`0` / omitted = unlimited).
    /// 平均推送速率上限（字节/秒）；0/缺省 = 不限
    #[serde(default)]
    max_bps: u64,
}

/// 按 max_bps 包装请求体（限速流或一次性 Body）
fn replicate_push_body(dat: Vec<u8>, max_bps: u64) -> reqwest::Body {
    if max_bps > 0 {
        reqwest::Body::wrap_stream(crate::throttle::rate_limited_byte_stream(dat, max_bps))
    } else {
        reqwest::Body::from(dat)
    }
}

/// POST /admin/replicate_volume/{id} — 将本卷 dat/idx 推到目标节点
async fn replicate_volume_handler(
    State(st): State<AppState>,
    Path(id): Path<u32>,
    Query(q): Query<ReplicateVolQuery>,
) -> impl IntoResponse {
    let dat = st.server.store.dir().join(format!("{id}.dat"));
    let idx = st.server.store.dir().join(format!("{id}.idx"));
    if !dat.exists() {
        return (StatusCode::NOT_FOUND, "volume dat missing").into_response();
    }
    let dat_bytes = match std::fs::read(&dat) {
        Ok(b) => b,
        Err(e) => return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    };
    let idx_b64 = base64::engine::general_purpose::STANDARD
        .encode(std::fs::read(&idx).unwrap_or_default());
    let mut results = Vec::new();
    for target in q.targets.split(',').filter(|s| !s.is_empty()) {
        let base = target.trim_end_matches('/');
        // 先确保对端有卷，再二进制导入
        let _ = st
            .server
            .http_client
            .post(format!("{base}/admin/create_volume/{id}"))
            .send()
            .await;
        let resp = st
            .server
            .http_client
            .post(format!("{base}/admin/import_volume_bin/{id}"))
            .header("x-opens3-idx-b64", &idx_b64)
            .header(axum::http::header::CONTENT_TYPE, "application/octet-stream")
            .body(replicate_push_body(dat_bytes.clone(), q.max_bps))
            .send()
            .await;
        match resp {
            Ok(r) => {
                results.push(serde_json::json!({"target": target, "status": r.status().as_u16()}))
            }
            Err(e) => results.push(serde_json::json!({"target": target, "error": e.to_string()})),
        }
    }
    Json(serde_json::json!({
        "ok": true,
        "max_bps": q.max_bps,
        "results": results
    }))
    .into_response()
}

/// JSON 导入体：base64 编码的 dat/idx
#[derive(Deserialize)]
struct ImportBody {
    volume_id: u32,
    dat: String, // base64(.dat)
    idx: String, // base64(.idx)
}

/// 落盘 dat/idx 后 reopen，供 JSON / 二进制导入共用
fn finish_volume_import(store: &VolumeStore, volume_id: u32, dat: &[u8], idx: &[u8]) -> impl IntoResponse {
    let dir = store.dir();
    if let Err(e) = std::fs::write(dir.join(format!("{volume_id}.dat")), dat) {
        return (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response();
    }
    let _ = std::fs::write(dir.join(format!("{volume_id}.idx")), idx);
    let _ = store.create_volume(volume_id);
    match opens3_volume_reopen(store, volume_id) {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// 从 `X-OpenS3-Idx-B64` 解码 idx；缺失则空 vec
fn decode_idx_b64_header(headers: &HeaderMap) -> Result<Vec<u8>, (StatusCode, String)> {
    let Some(v) = headers.get("x-opens3-idx-b64") else {
        return Ok(Vec::new());
    };
    let s = v
        .to_str()
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    base64::engine::general_purpose::STANDARD
        .decode(s.as_bytes())
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))
}

/// POST /admin/import_volume — JSON+base64 导入
async fn import_volume_handler(
    State(st): State<AppState>,
    Json(body): Json<ImportBody>,
) -> impl IntoResponse {
    let dat = match base64::engine::general_purpose::STANDARD.decode(body.dat.as_bytes()) {
        Ok(b) => b,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let idx = base64::engine::general_purpose::STANDARD
        .decode(body.idx.as_bytes())
        .unwrap_or_default();
    finish_volume_import(&st.server.store, body.volume_id, &dat, &idx).into_response()
}

/// Binary import used by heal/balance: raw `.dat` body + idx in `X-OpenS3-Idx-B64`.
/// heal/balance 用：原始 `.dat` body + 头里的 idx
async fn import_volume_bin_handler(
    State(st): State<AppState>,
    Path(id): Path<u32>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let idx = match decode_idx_b64_header(&headers) {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    finish_volume_import(&st.server.store, id, &body, &idx).into_response()
}

/// 导入后从磁盘重新打开卷
fn opens3_volume_reopen(store: &VolumeStore, id: u32) -> opens3_common::Result<()> {
    store.reopen_volume(id)
}

/// GET /metrics — Prometheus 文本
async fn volume_metrics(State(st): State<AppState>) -> impl IntoResponse {
    st.server.refresh_gauges();
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        encode_metrics(&REGISTRY),
    )
}

/// POST /admin/seal/{id}
async fn seal_handler(State(st): State<AppState>, Path(id): Path<u32>) -> impl IntoResponse {
    match st.server.store.seal(id) {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// POST /admin/drop_volume/{id} — 删除内存句柄与磁盘文件
async fn drop_volume_handler(State(st): State<AppState>, Path(id): Path<u32>) -> impl IntoResponse {
    match st.server.store.drop_volume_files(id) {
        Ok(()) => Json(serde_json::json!({"ok": true, "dropped": id})).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// POST /admin/compact/{id} — 一次性 vacuum
async fn compact_handler(State(st): State<AppState>, Path(id): Path<u32>) -> impl IntoResponse {
    match st.server.store.compact(id) {
        Ok((before, after)) => {
            st.server
                .metrics
                .compact_total
                .with_label_values(&["ok"])
                .inc();
            Json(serde_json::json!({"before": before, "after": after})).into_response()
        }
        Err(e) => {
            st.server
                .metrics
                .compact_total
                .with_label_values(&["error"])
                .inc();
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// POST /admin/vacuum/{id}/check
async fn vacuum_check_handler(State(st): State<AppState>, Path(id): Path<u32>) -> impl IntoResponse {
    match st.server.store.vacuum_check(id) {
        Ok(garbage_ratio) => {
            st.server
                .metrics
                .vacuum_total
                .with_label_values(&["check", "ok"])
                .inc();
            Json(serde_json::json!({"garbage_ratio": garbage_ratio})).into_response()
        }
        Err(e) => {
            st.server
                .metrics
                .vacuum_total
                .with_label_values(&["check", "error"])
                .inc();
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// POST /admin/vacuum/{id}/compact
async fn vacuum_compact_handler(
    State(st): State<AppState>,
    Path(id): Path<u32>,
) -> impl IntoResponse {
    match st.server.store.vacuum_compact(id) {
        Ok((before_size, processed_bytes)) => {
            st.server
                .metrics
                .vacuum_total
                .with_label_values(&["compact", "ok"])
                .inc();
            Json(serde_json::json!({
                "before_size": before_size,
                "processed_bytes": processed_bytes,
            }))
            .into_response()
        }
        Err(e) => {
            st.server
                .metrics
                .vacuum_total
                .with_label_values(&["compact", "error"])
                .inc();
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// POST /admin/vacuum/{id}/commit
async fn vacuum_commit_handler(
    State(st): State<AppState>,
    Path(id): Path<u32>,
) -> impl IntoResponse {
    match st.server.store.vacuum_commit(id) {
        Ok(volume_size) => {
            let is_read_only = st
                .server
                .store
                .volume_stats(id)
                .map(|(_, _, _, ro, _)| ro)
                .unwrap_or(false);
            st.server
                .metrics
                .vacuum_total
                .with_label_values(&["commit", "ok"])
                .inc();
            Json(serde_json::json!({
                "volume_size": volume_size,
                "is_read_only": is_read_only,
            }))
            .into_response()
        }
        Err(e) => {
            st.server
                .metrics
                .vacuum_total
                .with_label_values(&["commit", "error"])
                .inc();
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// POST /admin/vacuum/{id}/cleanup
async fn vacuum_cleanup_handler(
    State(st): State<AppState>,
    Path(id): Path<u32>,
) -> impl IntoResponse {
    match st.server.store.vacuum_cleanup(id) {
        Ok(()) => {
            st.server
                .metrics
                .vacuum_total
                .with_label_values(&["cleanup", "ok"])
                .inc();
            Json(serde_json::json!({"ok": true})).into_response()
        }
        Err(e) => {
            st.server
                .metrics
                .vacuum_total
                .with_label_values(&["cleanup", "error"])
                .inc();
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// EC 编码查询参数
#[derive(Deserialize)]
struct EcQuery {
    data_shards: Option<usize>,   // 默认 4
    parity_shards: Option<usize>, // 默认 2
}

/// POST /admin/ec/{id} — 编码后密封并删除明文 `.dat`（保留 idx）
async fn ec_handler(
    State(st): State<AppState>,
    Path(id): Path<u32>,
    Query(q): Query<EcQuery>,
) -> impl IntoResponse {
    let data_shards = q.data_shards.unwrap_or(4);
    let parity_shards = q.parity_shards.unwrap_or(2);
    let encoder = match EcEncoder::new(data_shards, parity_shards) {
        Ok(e) => e,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let dat = st.server.store.dir().join(format!("{id}.dat"));
    let out_dir = st.server.store.dir().join("ec");
    match encoder.encode_file(&dat, &out_dir, id) {
        Ok(paths) => {
            let _ = st.server.store.seal(id);
            // Drop plaintext .dat so subsequent reads exercise EC reconstruct path.
            // Index is kept for needle offsets.
            // 删明文 .dat，后续读走 EC 重建；idx 保留偏移
            let _ = std::fs::remove_file(&dat);
            st.server.store.invalidate(id);
            st.server.metrics.ec_total.with_label_values(&["ok"]).inc();
            Json(serde_json::json!({
                "ok": true,
                "shards": paths.iter().map(|p| p.display().to_string()).collect::<Vec<_>>(),
            }))
            .into_response()
        }
        Err(e) => {
            st.server
                .metrics
                .ec_total
                .with_label_values(&["error"])
                .inc();
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    }
}

/// gRPC VolumeService 适配器
struct GrpcVolume(VolumeServer);

#[tonic::async_trait]
impl VolumeService for GrpcVolume {
    /// 写 needle；非复制请求可扇出到 replica_urls
    async fn write_needle(
        &self,
        req: Request<WriteNeedleRequest>,
    ) -> std::result::Result<Response<WriteNeedleResponse>, Status> {
        let meta = req.metadata().clone();
        let r = req.into_inner();
        let fid = FileId {
            volume_id: r.volume_id,
            file_key: r.file_key,
            cookie: r.cookie,
        };
        grpc_check_jwt(&self.0, &meta, &fid.fid(), "write")?;
        // Replication fan-in must also wait until this node finished fingerprint align.
        if let Err(e) = self.0.ensure_write_ready() {
            return Err(Status::unavailable(e));
        }
        self.0
            .store
            .create_volume(r.volume_id)
            .map_err(|e| Status::internal(e.to_string()))?;
        let (size, etag) = self
            .0
            .store
            .write(
                r.volume_id,
                r.file_key,
                r.cookie,
                &r.data,
                r.ttl_seconds as u32,
            )
            .map_err(|e| Status::internal(e.to_string()))?;
        if !r.is_replication && !r.replica_urls.is_empty() {
            let ok = self
                .0
                .replicate_to(&r.replica_urls, &fid, &r.data, r.ttl_seconds)
                .await;
            self.0
                .metrics
                .replicate_total
                .with_label_values(&[if ok { "ok" } else { "error" }])
                .inc();
            if !ok {
                let _ = self.0.store.delete(r.volume_id, r.file_key, r.cookie);
                return Err(Status::unavailable("replication failed"));
            }
        }
        Ok(Response::new(WriteNeedleResponse {
            volume_id: r.volume_id,
            file_key: r.file_key,
            cookie: r.cookie,
            size,
            etag,
        }))
    }

    /// 读 needle（本地，无 EC 回退）
    async fn read_needle(
        &self,
        req: Request<ReadNeedleRequest>,
    ) -> std::result::Result<Response<ReadNeedleResponse>, Status> {
        let r = req.into_inner();
        let data = self
            .0
            .store
            .read(r.volume_id, r.file_key, r.cookie)
            .map_err(|e| Status::not_found(e.to_string()))?;
        let etag = opens3_common::util::md5_hex(&data);
        let size = data.len() as u64;
        Ok(Response::new(ReadNeedleResponse { data, etag, size }))
    }

    /// 软删 needle
    async fn delete_needle(
        &self,
        req: Request<DeleteNeedleRequest>,
    ) -> std::result::Result<Response<DeleteNeedleResponse>, Status> {
        let meta = req.metadata().clone();
        let r = req.into_inner();
        let fid = FileId {
            volume_id: r.volume_id,
            file_key: r.file_key,
            cookie: r.cookie,
        };
        grpc_check_jwt(&self.0, &meta, &fid.fid(), "write")?;
        if let Err(e) = self.0.ensure_write_ready() {
            return Err(Status::unavailable(e));
        }
        self.0
            .store
            .delete(r.volume_id, r.file_key, r.cookie)
            .map_err(|e| Status::not_found(e.to_string()))?;
        Ok(Response::new(DeleteNeedleResponse { ok: true }))
    }

    /// 副本侧写入入口：只本地写，不再扇出
    async fn replicate_needle(
        &self,
        req: Request<ReplicateNeedleRequest>,
    ) -> std::result::Result<Response<ReplicateNeedleResponse>, Status> {
        let meta = req.metadata().clone();
        let r = req.into_inner();
        let fid = FileId {
            volume_id: r.volume_id,
            file_key: r.file_key,
            cookie: r.cookie,
        };
        grpc_check_jwt(&self.0, &meta, &fid.fid(), "write")?;
        if let Err(e) = self.0.ensure_write_ready() {
            return Err(Status::unavailable(e));
        }
        self.0
            .store
            .create_volume(r.volume_id)
            .map_err(|e| Status::internal(e.to_string()))?;
        self.0
            .store
            .write(
                r.volume_id,
                r.file_key,
                r.cookie,
                &r.data,
                r.ttl_seconds as u32,
            )
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(ReplicateNeedleResponse { ok: true }))
    }

    /// 一次性 compact
    async fn compact_volume(
        &self,
        req: Request<CompactVolumeRequest>,
    ) -> std::result::Result<Response<CompactVolumeResponse>, Status> {
        let id = req.into_inner().volume_id;
        let (before_size, after_size) = self
            .0
            .store
            .compact(id)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(CompactVolumeResponse {
            before_size,
            after_size,
        }))
    }

    /// vacuum 垃圾比例检查
    async fn vacuum_volume_check(
        &self,
        req: Request<VacuumVolumeCheckRequest>,
    ) -> std::result::Result<Response<VacuumVolumeCheckResponse>, Status> {
        let id = req.into_inner().volume_id;
        let garbage_ratio = self
            .0
            .store
            .vacuum_check(id)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(VacuumVolumeCheckResponse { garbage_ratio }))
    }

    /// vacuum 压缩阶段
    async fn vacuum_volume_compact(
        &self,
        req: Request<VacuumVolumeCompactRequest>,
    ) -> std::result::Result<Response<VacuumVolumeCompactResponse>, Status> {
        let id = req.into_inner().volume_id;
        let (before_size, processed_bytes) = self
            .0
            .store
            .vacuum_compact(id)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(VacuumVolumeCompactResponse {
            before_size,
            processed_bytes,
        }))
    }

    /// vacuum 提交阶段
    async fn vacuum_volume_commit(
        &self,
        req: Request<VacuumVolumeCommitRequest>,
    ) -> std::result::Result<Response<VacuumVolumeCommitResponse>, Status> {
        let id = req.into_inner().volume_id;
        let volume_size = self
            .0
            .store
            .vacuum_commit(id)
            .map_err(|e| Status::internal(e.to_string()))?;
        let is_read_only = self
            .0
            .store
            .volume_stats(id)
            .map(|(_, _, _, ro, _)| ro)
            .unwrap_or(false);
        Ok(Response::new(VacuumVolumeCommitResponse {
            volume_size,
            is_read_only,
        }))
    }

    /// vacuum 清理阶段
    async fn vacuum_volume_cleanup(
        &self,
        req: Request<VacuumVolumeCleanupRequest>,
    ) -> std::result::Result<Response<VacuumVolumeCleanupResponse>, Status> {
        let id = req.into_inner().volume_id;
        self.0
            .store
            .vacuum_cleanup(id)
            .map_err(|e| Status::internal(e.to_string()))?;
        Ok(Response::new(VacuumVolumeCleanupResponse {}))
    }

    /// 对密封卷做 EC 编码并返回分片信息
    async fn encode_ec(
        &self,
        req: Request<EncodeEcRequest>,
    ) -> std::result::Result<Response<EncodeEcResponse>, Status> {
        let r = req.into_inner();
        let encoder = EcEncoder::new(r.data_shards as usize, r.parity_shards as usize)
            .map_err(|e| Status::invalid_argument(e.to_string()))?;
        let dat = self.0.store.dir().join(format!("{}.dat", r.volume_id));
        let out_dir = self.0.store.dir().join("ec");
        let paths = encoder
            .encode_file(&dat, &out_dir, r.volume_id)
            .map_err(|e| Status::internal(e.to_string()))?;
        let _ = self.0.store.seal(r.volume_id);
        let shards = paths
            .into_iter()
            .enumerate()
            .map(|(i, p)| EcShardInfo {
                volume_id: r.volume_id,
                shard_id: i as u32,
                url: p.display().to_string(),
                is_parity: i >= r.data_shards as usize,
            })
            .collect();
        Ok(Response::new(EncodeEcResponse { ok: true, shards }))
    }

    /// 健康检查
    async fn health(
        &self,
        _req: Request<HealthRequest>,
    ) -> std::result::Result<Response<HealthResponse>, Status> {
        Ok(Response::new(HealthResponse {
            ok: true,
            node_id: self.0.node_id.clone(),
            free_disk_bytes: self.0.store.free_disk_bytes(),
            volume_count: self.0.store.list_ids().len() as u32,
        }))
    }
}
