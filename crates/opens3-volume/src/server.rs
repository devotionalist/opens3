// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
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

#[derive(Clone)]
pub struct VolumeServer {
    pub node_id: String,
    pub store: VolumeStore,
    /// Active clerk URL (may be updated when following redirects / peer list)
    pub clerk: Arc<parking_lot::RwLock<String>>,
    pub clerk_peers: Vec<String>,
    pub public_url: String,
    pub data_center: String,
    pub rack: String,
    pub ip: String,
    pub port: u16,
    pub max_volumes: u64,
    pub http_client: reqwest::Client,
    pub metrics: Arc<VolumeMetrics>,
    /// If set, write/delete require Authorization: Bearer <jwt>
    pub jwt_secret: Option<String>,
    pub tls: Option<opens3_common::tls::TlsConfig>,
    /// Unique per process start; Clerk uses change to trigger fingerprint reconcile.
    pub boot_id: String,
    /// Cleared until Clerk heartbeat returns write_ready after restart align.
    pub write_ready: Arc<AtomicBool>,
    /// Deploy bootstrap: ask Clerk to mark this node online on first registration.
    pub start_online: bool,
    /// Deploy bootstrap: ask Clerk to pair as replica of this node id (pod name).
    pub replica_of: Option<String>,
}

#[derive(Clone)]
struct AppState {
    server: VolumeServer,
}

impl VolumeServer {
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
            write_ready: Arc::new(AtomicBool::new(false)),
            start_online,
            replica_of: replica_of
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty()),
        })
    }

    fn ensure_write_ready(&self) -> Result<(), String> {
        if self.write_ready.load(Ordering::Relaxed) {
            Ok(())
        } else {
            Err("volume node reconciling fingerprints; writes blocked".into())
        }
    }

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

    pub async fn serve(self) -> anyhow::Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.port).parse()?;
        // gRPC prefers port+10000; wrap into a high but valid range if that would overflow u16
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
            .filter(|u| u != &self_url && !u.is_empty())
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

fn grpc_check_jwt(server: &VolumeServer, meta: &tonic::metadata::MetadataMap, fid: &str, op: &str) -> std::result::Result<(), Status> {
    let Some(secret) = server.jwt_secret.as_deref() else {
        return Ok(());
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

#[derive(Deserialize)]
struct WriteQuery {
    ttl: Option<u64>,
    replication: Option<String>,
    locations: Option<String>,
}

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

async fn create_volume_handler(
    State(st): State<AppState>,
    Path(id): Path<u32>,
) -> impl IntoResponse {
    match st.server.store.create_volume(id) {
        Ok(()) => Json(serde_json::json!({"ok": true, "id": id})).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct ReplicateVolQuery {
    targets: String,
    /// Average push rate limit in bytes/sec (`0` / omitted = unlimited).
    #[serde(default)]
    max_bps: u64,
}

fn replicate_push_body(dat: Vec<u8>, max_bps: u64) -> reqwest::Body {
    if max_bps > 0 {
        reqwest::Body::wrap_stream(crate::throttle::rate_limited_byte_stream(dat, max_bps))
    } else {
        reqwest::Body::from(dat)
    }
}

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

#[derive(Deserialize)]
struct ImportBody {
    volume_id: u32,
    dat: String,
    idx: String,
}

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

fn opens3_volume_reopen(store: &VolumeStore, id: u32) -> opens3_common::Result<()> {
    store.reopen_volume(id)
}

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

async fn seal_handler(State(st): State<AppState>, Path(id): Path<u32>) -> impl IntoResponse {
    match st.server.store.seal(id) {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn drop_volume_handler(State(st): State<AppState>, Path(id): Path<u32>) -> impl IntoResponse {
    match st.server.store.drop_volume_files(id) {
        Ok(()) => Json(serde_json::json!({"ok": true, "dropped": id})).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

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

#[derive(Deserialize)]
struct EcQuery {
    data_shards: Option<usize>,
    parity_shards: Option<usize>,
}

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

struct GrpcVolume(VolumeServer);

#[tonic::async_trait]
impl VolumeService for GrpcVolume {
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
