// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Clerk HTTP 服务：集群 API、Raft RPC、heal/vacuum 后台循环。

use crate::cluster::{AssignResult, ClusterState, VolumeReport, VolumeServerInfo};
use crate::cluster::AssignPolicy;
use crate::heal::{
    execute_balance, execute_heal, plan_balance, plan_heal, plan_heal_filtered,
    reconcile_node_fingerprints, HealTracker,
};
use crate::vacuum::{
    execute_vacuum, plan_vacuum, plan_vacuum_filtered, VacuumConfig, VacuumTracker,
};
use crate::raft::{RaftLite, RaftRequest, TypeConfig};
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{header, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use opens3_common::metrics::{encode_metrics, ClerkMetrics, REGISTRY};
use opens3_common::replication::Replication;
use opens3_common::topology::TopologyLocation;
use serde::Deserialize;
use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use parking_lot::Mutex;
use tracing::info;

/// Clerk（Master）进程：拓扑、Raft、HTTP API 与后台编排任务。
#[derive(Clone)]
pub struct ClerkServer {
    pub cluster: ClusterState,                 // 内存拓扑
    pub raft: RaftLite,                        // 共识门面
    pub port: u16,                             // 监听端口
    pub peers: Vec<String>,                    // 配置的 peer URL
    pub http: reqwest::Client,                 // 访问 Volume 管理 API
    pub metrics: Arc<ClerkMetrics>,
    /// 与 Volume 共享；设置后 assign 响应附带写 JWT。
    pub jwt_secret: Option<String>,
    pub tls: Option<opens3_common::tls::TlsConfig>,
    /// 进行中的 heal 任务跟踪。
    pub heal: HealTracker,
    /// 进行中的 vacuum 任务跟踪（Leader）。
    pub vacuum: VacuumTracker,
    pub vacuum_config: VacuumConfig,
    /// 重启后正在跑指纹 reconcile 的节点集合。
    pub reconciling: Arc<Mutex<HashSet<String>>>,
}

/// Axum 共享状态。
#[derive(Clone)]
struct AppState {
    clerk: ClerkServer,
}

impl ClerkServer {
    /// 便捷构造（无 JWT / TLS）。
    pub async fn new(
        node_id: String,
        port: u16,
        peers: Vec<String>,
        default_replication: Replication,
        raft_dir: PathBuf,
        advertise_url: Option<String>,
    ) -> anyhow::Result<Self> {
        Self::new_with_opts(
            node_id,
            port,
            peers,
            default_replication,
            raft_dir,
            advertise_url,
            None,
            None,
        )
        .await
    }

    /// 完整构造：打开 Raft、注册指标、加载 vacuum 配置。
    pub async fn new_with_opts(
        node_id: String,
        port: u16,
        peers: Vec<String>,
        default_replication: Replication,
        raft_dir: PathBuf,
        advertise_url: Option<String>,
        jwt_secret: Option<String>,
        tls: Option<opens3_common::tls::TlsConfig>,
    ) -> anyhow::Result<Self> {
        let scheme = opens3_common::tls::scheme(tls.as_ref());
        let listen_url =
            advertise_url.unwrap_or_else(|| format!("{scheme}://127.0.0.1:{port}"));
        let cluster = ClusterState::new(default_replication);
        let raft =
            RaftLite::open(&raft_dir, node_id, listen_url, peers.clone(), cluster.clone()).await?;
        let metrics = ClerkMetrics::register(&REGISTRY)?;
        Ok(Self {
            cluster,
            raft,
            port,
            peers,
            http: reqwest::Client::new(),
            metrics,
            jwt_secret,
            tls,
            heal: HealTracker::new(),
            vacuum: VacuumTracker::new(),
            vacuum_config: VacuumConfig::from_env(),
            reconciling: Arc::new(Mutex::new(HashSet::new())),
        })
    }

    /// 刷新 Prometheus gauges（leader / 拓扑规模等）。
    fn refresh_gauges(&self) {
        let (vs, vc, size, files) = self.cluster.stats();
        self.metrics
            .is_leader
            .set(if self.raft.is_leader() { 1 } else { 0 });
        self.metrics.term.set(self.raft.term() as i64);
        self.metrics
            .commit_index
            .set(self.raft.commit_index() as i64);
        self.metrics.volume_servers.set(vs as i64);
        self.metrics.volumes.set(vc as i64);
        self.metrics.total_size_bytes.set(size as i64);
        self.metrics.total_files.set(files as i64);
        self.metrics
            .under_replicated_volumes
            .set(self.cluster.under_replicated_count() as i64);
    }

    /// 启动后台循环并监听 HTTP（可选 TLS）。
    pub async fn serve(self) -> anyhow::Result<()> {
        let addr: SocketAddr = format!("0.0.0.0:{}", self.port).parse()?;
        let cluster = self.cluster.clone();
        tokio::spawn(async move {
            loop {
                cluster.prune_stale(Duration::from_secs(30));
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });

        let heal_master = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                if !heal_master.raft.is_leader() {
                    continue;
                }
                let tasks = plan_heal(&heal_master.cluster);
                heal_master.metrics.heal_tasks.set(tasks.len() as i64);
                if tasks.is_empty() {
                    continue;
                }
                if let Err(_busy) = heal_master.heal.try_begin(&tasks) {
                    continue;
                }
                let max_bps = heal_master.cluster.heal_max_bps();
                let _ = execute_heal(
                    &heal_master.http,
                    &tasks,
                    Some(&heal_master.heal),
                    max_bps,
                )
                .await;
                heal_master
                    .metrics
                    .heal_runs_total
                    .with_label_values(&["ok"])
                    .inc();
            }
        });

        let vacuum_master = self.clone();
        tokio::spawn(async move {
            let interval = Duration::from_secs(vacuum_master.vacuum_config.interval_secs.max(5));
            loop {
                tokio::time::sleep(interval).await;
                if !vacuum_master.vacuum_config.enabled {
                    continue;
                }
                if !vacuum_master.raft.is_leader() {
                    continue;
                }
                let threshold = vacuum_master.vacuum_config.garbage_threshold;
                let tasks = plan_vacuum(&vacuum_master.cluster, threshold);
                vacuum_master
                    .metrics
                    .vacuum_tasks
                    .set(tasks.len() as i64);
                if tasks.is_empty() {
                    continue;
                }
                if let Err(_busy) = vacuum_master.vacuum.try_begin(&tasks, threshold) {
                    continue;
                }
                let _ = execute_vacuum(
                    &vacuum_master.http,
                    &tasks,
                    Some(&vacuum_master.vacuum),
                    threshold,
                )
                .await;
                vacuum_master
                    .metrics
                    .vacuum_runs_total
                    .with_label_values(&["ok"])
                    .inc();
            }
        });

        let gauge_master = self.clone();
        tokio::spawn(async move {
            loop {
                gauge_master.refresh_gauges();
                tokio::time::sleep(Duration::from_secs(5)).await;
            }
        });

        // Leader 周期性在拓扑变化时复制快照。
        // 不要每个 volume 心跳都 propose — 全量快照会淹没 Raft，
        // 且未压缩时重启重放会 OOM。
        let snap_master = self.clone();
        tokio::spawn(async move {
            let mut last_fp = String::new();
            loop {
                tokio::time::sleep(Duration::from_secs(30)).await;
                if !snap_master.raft.is_leader() {
                    continue;
                }
                if let Ok(Some(fp)) =
                    propose_cluster_snapshot_if_changed(&snap_master, &last_fp).await
                {
                    last_fp = fp;
                }
                // 压缩 Raft 日志，避免重启重放每一次拓扑写。
                snap_master.raft.trigger_snapshot().await;
            }
        });

        let tls = self.tls.clone();
        let port = self.port;
        let has_tls = tls.is_some();
        let state = AppState { clerk: self };
        let app = Router::new()
            .route("/health", get(|| async { "ok" }))
            .route("/ready", get(ready))
            .route("/metrics", get(metrics))
            .route("/cluster/status", get(cluster_status))
            .route("/cluster/heartbeat", post(heartbeat))
            .route("/dir/assign", get(assign).post(assign))
            .route("/dir/lookup/{volume_id}", get(lookup))
            .route("/vol/list", get(list_volumes))
            .route("/vol/seal/{volume_id}", post(seal_volume))
            .route("/vol/ec/{volume_id}", post(mark_ec))
            .route("/vol/online", post(set_volume_online))
            .route("/vol/replication", post(set_replication))
            .route("/vol/grow", post(grow_volumes))
            .route("/cluster/server/online", post(set_server_online))
            .route("/cluster/server/remove", post(remove_server))
            .route("/cluster/leader", get(leader))
            .route("/cluster/heal", post(heal_now).get(heal_status))
            .route("/cluster/heal/status", get(heal_status))
            .route("/cluster/vacuum", post(vacuum_now).get(vacuum_status))
            .route("/cluster/vacuum/status", get(vacuum_status))
            .route(
                "/cluster/heal-bandwidth",
                get(heal_bandwidth_get).post(heal_bandwidth_set),
            )
            .route("/cluster/place-replicas", post(place_replicas))
            .route("/cluster/replica-of", post(set_replica_of_api))
            .route("/cluster/balance", post(balance_now))
            .route(
                "/cluster/assign-policy",
                get(assign_policy_get).post(assign_policy_set),
            )
            .route("/raft/vote", post(raft_vote))
            .route("/raft/append", post(raft_append))
            .route("/raft/snapshot", post(raft_snapshot))
            .route("/raft/status", get(raft_status))
            .with_state(state);

        info!("clerk listening on {addr} tls={has_tls}");
        opens3_common::tls::serve_http(port, app, tls).await?;
        Ok(())
    }
}

/// 就绪探针：Raft 已进入可服务角色。
async fn ready(State(st): State<AppState>) -> impl IntoResponse {
    if st.clerk.raft.is_ready() {
        (StatusCode::OK, "ready")
    } else {
        (StatusCode::SERVICE_UNAVAILABLE, "electing")
    }
}

/// Prometheus metrics 端点。
async fn metrics(State(st): State<AppState>) -> impl IntoResponse {
    st.clerk.refresh_gauges();
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        encode_metrics(&REGISTRY),
    )
}

/// 集群概览 JSON（leader、规模、策略、节点列表）。
async fn cluster_status(State(st): State<AppState>) -> impl IntoResponse {
    let (vs, vc, size, files) = st.clerk.cluster.stats();
    let policy = st.clerk.cluster.assign_policy();
    Json(serde_json::json!({
        "leader": st.clerk.raft.leader(),
        "leader_url": st.clerk.raft.leader_url(),
        "is_leader": st.clerk.raft.is_leader(),
        "term": st.clerk.raft.term(),
        "role": st.clerk.raft.role_name(),
        "commit_index": st.clerk.raft.commit_index(),
        "peers": st.clerk.peers,
        "volume_server_count": vs,
        "volume_count": vc,
        "total_size": size,
        "total_files": files,
        "assign_policy": policy.as_str(),
        "assign_policy_label": policy.label_zh(),
        "heal_max_bps": st.clerk.cluster.heal_max_bps(),
        "servers": st.clerk.cluster.servers(),
    }))
}

/// assign 策略及可选列表的 JSON。
fn assign_policy_json(policy: AssignPolicy) -> serde_json::Value {
    serde_json::json!({
        "policy": policy.as_str(),
        "label": policy.label_zh(),
        "options": [
            {"id": "random", "label": "随机"},
            {"id": "round_robin", "label": "循环"},
            {"id": "free_disk", "label": "按磁盘空闲加权"},
            {"id": "least_size", "label": "优先较空卷"},
        ],
    })
}

/// GET 当前 assign 策略。
async fn assign_policy_get(State(st): State<AppState>) -> impl IntoResponse {
    Json(assign_policy_json(st.clerk.cluster.assign_policy()))
}

/// 设置 assign 策略请求体。
#[derive(Deserialize)]
struct AssignPolicyBody {
    policy: String, // random | round_robin | free_disk | least_size
}

/// POST 更新 assign 策略（仅 Leader；否则重定向）。
async fn assign_policy_set(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<AssignPolicyBody>,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    let policy = match body.policy.parse::<AssignPolicy>() {
        Ok(p) => p,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, e.to_string()).into_response();
        }
    };
    st.clerk.cluster.set_assign_policy(policy);
    let _ = propose_cluster_snapshot(&st.clerk).await;
    Json(assign_policy_json(policy)).into_response()
}

/// heal 带宽配置 JSON。
fn heal_bandwidth_json(max_bps: u64) -> serde_json::Value {
    serde_json::json!({
        "max_bps": max_bps,
        "max_mbps": max_bps as f64 / (1024.0 * 1024.0),
        "unlimited": max_bps == 0,
    })
}

/// GET heal/balance 限速。
async fn heal_bandwidth_get(State(st): State<AppState>) -> impl IntoResponse {
    Json(heal_bandwidth_json(st.clerk.cluster.heal_max_bps()))
}

/// 设置 heal 带宽请求体。
#[derive(Deserialize)]
struct HealBandwidthBody {
    /// 优先：兆字节/秒（`0` = 不限）。
    max_mbps: Option<f64>,
    /// 备选：字节/秒（`0` = 不限）。
    max_bps: Option<u64>,
}

/// 两者都给时优先 `max_mbps`；`0` 表示不限速。
fn parse_heal_max_bps(body: &HealBandwidthBody) -> Result<u64, String> {
    if let Some(mbps) = body.max_mbps {
        if mbps < 0.0 {
            return Err("max_mbps must be >= 0".into());
        }
        return Ok((mbps * 1024.0 * 1024.0).round() as u64);
    }
    body.max_bps
        .ok_or_else(|| "provide max_mbps or max_bps".into())
}

/// POST 更新 heal/balance 限速。
async fn heal_bandwidth_set(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<HealBandwidthBody>,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    let max_bps = match parse_heal_max_bps(&body) {
        Ok(v) => v,
        Err(msg) => return (StatusCode::BAD_REQUEST, msg).into_response(),
    };
    st.clerk.cluster.set_heal_max_bps(max_bps);
    let _ = propose_cluster_snapshot(&st.clerk).await;
    Json(heal_bandwidth_json(max_bps)).into_response()
}

/// 当前 Leader id / URL。
async fn leader(State(st): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "leader": st.clerk.raft.leader(),
        "leader_url": st.clerk.raft.leader_url(),
    }))
}

/// 本节点 Raft 状态摘要。
async fn raft_status(State(st): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "node_id": st.clerk.raft.node_name,
        "raft_id": st.clerk.raft.node_id,
        "role": st.clerk.raft.role_name(),
        "term": st.clerk.raft.term(),
        "leader": st.clerk.raft.leader(),
        "last_log_index": st.clerk.raft.last_log_index(),
        "commit_index": st.clerk.raft.commit_index(),
    }))
}

/// openraft Vote RPC。
async fn raft_vote(
    State(st): State<AppState>,
    Json(req): Json<VoteRequest<crate::raft::NodeId>>,
) -> impl IntoResponse {
    Json(st.clerk.raft.raft.vote(req).await)
}

/// openraft AppendEntries RPC。
async fn raft_append(
    State(st): State<AppState>,
    Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> impl IntoResponse {
    Json(st.clerk.raft.raft.append_entries(req).await)
}

/// openraft InstallSnapshot RPC。
async fn raft_snapshot(
    State(st): State<AppState>,
    Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> impl IntoResponse {
    Json(st.clerk.raft.raft.install_snapshot(req).await)
}

/// Volume 心跳请求体。
#[derive(Deserialize)]
struct HeartbeatBody {
    node_id: String,
    ip: String,
    port: u16,
    public_url: String,
    data_center: String,
    rack: String,
    max_volume_count: u64,
    free_disk_bytes: u64,
    /// 每次 Volume 进程启动唯一；变化触发指纹 reconcile。
    #[serde(default)]
    boot_id: String,
    /// Bootstrap 意图（Helm / CLI）。首次注册及仍 offline 且未配对时生效。
    #[serde(default)]
    online: Option<bool>,
    #[serde(default)]
    replica_of: Option<String>,
    #[serde(default)]
    volumes: Vec<VolumeReport>,
}

/// 非 Leader：307 重定向到 Leader 同源路径。
fn redirect_leader(st: &AppState, uri: &Uri) -> Response {
    let base = st.clerk.raft.leader_url();
    let base = base.trim_end_matches('/');
    let path = uri
        .path_and_query()
        .map(|pq| pq.as_str())
        .unwrap_or(uri.path());
    let location = format!("{base}{path}");
    (
        StatusCode::TEMPORARY_REDIRECT,
        [(header::LOCATION, location)],
        Json(serde_json::json!({
            "leader": base,
            "leader_id": st.clerk.raft.leader(),
        })),
    )
        .into_response()
}

/// Volume 心跳：更新拓扑、按需建卷、触发指纹追赶。
async fn heartbeat(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<HeartbeatBody>,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    let node_id = body.node_id.clone();
    let info = VolumeServerInfo {
        node_id: body.node_id.clone(),
        location: TopologyLocation {
            data_center: body.data_center,
            rack: body.rack,
            node_id: body.node_id,
            url: format!("http://{}:{}", body.ip, body.port),
            public_url: body.public_url,
        },
        max_volume_count: body.max_volume_count,
        free_disk_bytes: body.free_disk_bytes,
        last_seen: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        // 占位；真实 online/replica_of 由 cluster.heartbeat bootstrap 处理。
        online: body.online.unwrap_or(false),
        replica_of: body
            .replica_of
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty()),
        boot_id: if body.boot_id.is_empty() {
            uuid::Uuid::new_v4().to_string()
        } else {
            body.boot_id
        },
        write_ready: false,
    };
    let desired_online = body.online.unwrap_or(false);
    let desired_replica = info.replica_of.clone();
    let t0 = Instant::now();
    let outcome = st.clerk.cluster.heartbeat_with_bootstrap(
        info,
        body.volumes,
        desired_online,
        desired_replica,
    );
    // 仅在需要建卷时强制 Raft propose；其余由周期快照同步 follower。
    if !outcome.create.is_empty() {
        let _ = propose_cluster_snapshot(&st.clerk).await;
    }
    if outcome.needs_reconcile {
        let mut guard = st.clerk.reconciling.lock();
        if guard.insert(node_id.clone()) {
            drop(guard);
            let master = st.clerk.clone();
            let nid = node_id.clone();
            tokio::spawn(async move {
                let max_bps = master.cluster.heal_max_bps();
                reconcile_node_fingerprints(&master.http, &master.cluster, &nid, max_bps).await;
                master.reconciling.lock().remove(&nid);
            });
        }
    }
    st.clerk.raft.bump_heartbeat();
    st.clerk.metrics.heartbeat_total.with_label_values(&["ok"]).inc();
    st.clerk
        .metrics
        .request_duration
        .with_label_values(&["heartbeat"])
        .observe(t0.elapsed().as_secs_f64());
    Json(serde_json::json!({
        "volume_ids_to_create": outcome.create,
        "write_ready": outcome.write_ready,
        "needs_reconcile": outcome.needs_reconcile,
    }))
    .into_response()
}

/// `/dir/assign` 查询参数。
#[derive(Deserialize)]
struct AssignQuery {
    collection: Option<String>,
    replication: Option<String>, // XYZ 字符串
    count: Option<u32>,
    ttl: Option<u64>,            // 秒
}

/// 分配 FileId（仅 Leader）。
async fn assign(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(q): Query<AssignQuery>,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    let repl = match q.replication.as_deref() {
        Some(s) => match Replication::from_str(s) {
            Ok(r) => Some(r),
            Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        },
        None => None,
    };
    let t0 = Instant::now();
    match st.clerk.cluster.assign(
        q.collection.as_deref().unwrap_or(""),
        repl,
        q.count.unwrap_or(1),
        q.ttl.unwrap_or(0),
    ) {
        Ok(a) => {
            let _ = propose_cluster_snapshot(&st.clerk).await;
            st.clerk.metrics.assign_total.with_label_values(&["ok"]).inc();
            st.clerk
                .metrics
                .request_duration
                .with_label_values(&["assign"])
                .observe(t0.elapsed().as_secs_f64());
            Json(assign_json(&st.clerk, a)).into_response()
        }
        Err(e) => {
            st.clerk
                .metrics
                .assign_total
                .with_label_values(&["error"])
                .inc();
            st.clerk
                .metrics
                .request_duration
                .with_label_values(&["assign"])
                .observe(t0.elapsed().as_secs_f64());
            (
                StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                e.to_string(),
            )
                .into_response()
        }
    }
}

/// 将 AssignResult 编成客户端 JSON；有 jwt_secret 时附带写 token。
fn assign_json(st: &ClerkServer, a: AssignResult) -> serde_json::Value {
    let mut v = serde_json::json!({
        "fid": a.fid.fid(),
        "url": a.url,
        "publicUrl": a.public_url,
        "count": a.count,
        "locations": a.locations.iter().map(|l| serde_json::json!({
            "url": l.url,
            "publicUrl": l.public_url,
            "dataCenter": l.data_center,
            "rack": l.rack,
            "nodeId": l.node_id,
        })).collect::<Vec<_>>(),
    });
    if let Some(secret) = st.jwt_secret.as_deref() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let claims = opens3_common::jwt::VolumeClaims {
            fid: a.fid.fid(),
            exp: now + 3600,
            op: "write".into(),
        };
        if let Ok(tok) = opens3_common::jwt::sign(secret, &claims) {
            v["jwt"] = serde_json::Value::String(tok);
        }
    }
    v
}

/// 按 volume_id 查找位置。
async fn lookup(State(st): State<AppState>, Path(volume_id): Path<u32>) -> impl IntoResponse {
    let t0 = Instant::now();
    match st.clerk.cluster.lookup(volume_id) {
        Ok(v) => {
            st.clerk
                .metrics
                .lookup_total
                .with_label_values(&["ok"])
                .inc();
            st.clerk
                .metrics
                .request_duration
                .with_label_values(&["lookup"])
                .observe(t0.elapsed().as_secs_f64());
            Json(serde_json::json!({
                "volumeId": v.id,
                "locations": v.locations.iter().map(|l| serde_json::json!({
                    "url": l.url,
                    "publicUrl": l.public_url,
                })).collect::<Vec<_>>(),
                "ec": matches!(v.state, opens3_common::types::VolumeState::EcEncoded),
                "collection": v.collection,
                "replication": v.replication.as_str(),
                "state": v.state.as_str(),
            }))
            .into_response()
        }
        Err(e) => {
            st.clerk
                .metrics
                .lookup_total
                .with_label_values(&["error"])
                .inc();
            st.clerk
                .metrics
                .request_duration
                .with_label_values(&["lookup"])
                .observe(t0.elapsed().as_secs_f64());
            (StatusCode::NOT_FOUND, e.to_string()).into_response()
        }
    }
}

/// 单卷上下线请求体。
#[derive(Deserialize)]
struct OnlineVolumeBody {
    volume_id: u32,
    online: bool, // true=Writable，false=Offline
}

/// 单卷上下线。
async fn set_volume_online(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<OnlineVolumeBody>,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    match st.clerk.cluster.set_volume_online(body.volume_id, body.online) {
        Ok(state) => {
            let _ = propose_cluster_snapshot(&st.clerk).await;
            Json(serde_json::json!({
                "ok": true,
                "volume_id": body.volume_id,
                "online": body.online,
                "state": state.as_str(),
            }))
            .into_response()
        }
        Err(e) => (StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::BAD_REQUEST), e.to_string())
            .into_response(),
    }
}

/// 数据节点上下线请求体。
#[derive(Deserialize)]
struct OnlineServerBody {
    node_id: String,
    online: bool,
}

/// 数据节点上下线（卷跟随）。
async fn set_server_online(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<OnlineServerBody>,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    match st
        .clerk
        .cluster
        .set_server_online(&body.node_id, body.online)
    {
        Ok(()) => {
            let _ = propose_cluster_snapshot(&st.clerk).await;
            Json(serde_json::json!({
                "ok": true,
                "node_id": body.node_id,
                "online": body.online,
            }))
            .into_response()
        }
        Err(e) => (StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::BAD_REQUEST), e.to_string())
            .into_response(),
    }
}

/// 移除数据节点请求体。
#[derive(Deserialize)]
struct RemoveServerBody {
    node_id: String,
}

/// 从目录移除数据节点。
async fn remove_server(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<RemoveServerBody>,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    let node = body.node_id.trim().to_string();
    if node.is_empty() {
        return (StatusCode::BAD_REQUEST, "node_id required".to_string()).into_response();
    }
    match st.clerk.cluster.remove_server(&node) {
        Ok(()) => {
            let _ = propose_cluster_snapshot(&st.clerk).await;
            Json(serde_json::json!({ "ok": true, "node_id": node })).into_response()
        }
        Err(e) => (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::BAD_REQUEST),
            e.to_string(),
        )
            .into_response(),
    }
}

/// 列卷查询参数。
#[derive(Deserialize)]
struct ListQuery {
    collection: Option<String>,
}

/// 列出卷元数据。
async fn list_volumes(State(st): State<AppState>, Query(q): Query<ListQuery>) -> impl IntoResponse {
    let vols = st
        .clerk
        .cluster
        .list_volumes(q.collection.as_deref().unwrap_or(""));
    Json(serde_json::json!({ "volumes": vols }))
}

/// 封印卷。
async fn seal_volume(State(st): State<AppState>, Path(volume_id): Path<u32>) -> impl IntoResponse {
    match st.clerk.cluster.seal(volume_id) {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

/// 标记卷为 EC。
async fn mark_ec(State(st): State<AppState>, Path(volume_id): Path<u32>) -> impl IntoResponse {
    match st.clerk.cluster.mark_ec(volume_id) {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => (StatusCode::NOT_FOUND, e.to_string()).into_response(),
    }
}

/// 批量改复制策略请求体。
#[derive(Deserialize)]
struct SetReplicationBody {
    /// XYZ，如 "001" / "010" / "100" / "011"
    replication: String,
    /// 指定卷；空且带 from = 按当前策略批量
    #[serde(default)]
    volume_ids: Vec<u32>,
    /// 仅当前为此码的卷（如 "000"）
    #[serde(default)]
    from: Option<String>,
    /// 改策略后是否立刻 heal 补副本
    #[serde(default = "default_true")]
    heal: bool,
}

/// serde 缺省 true。
fn default_true() -> bool {
    true
}

/// 更新卷复制策略，可选触发 heal。
async fn set_replication(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<SetReplicationBody>,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    let repl = match Replication::from_str(&body.replication) {
        Ok(r) => r,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    if body.volume_ids.is_empty() && body.from.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            "provide volume_ids and/or from (e.g. from=000)".to_string(),
        )
            .into_response();
    }
    let updated = match st.clerk.cluster.set_replication_batch(
        &body.volume_ids,
        body.from.as_deref(),
        repl,
    ) {
        Ok(ids) => ids,
        Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    };
    let _ = propose_cluster_snapshot(&st.clerk).await;

    let heal_tasks = if body.heal && !updated.is_empty() {
        let tasks = plan_heal_filtered(&st.clerk.cluster, Some(&updated), None);
        st.clerk.metrics.heal_tasks.set(tasks.len() as i64);
        let tracker = match st.clerk.heal.try_begin(&tasks) {
            Ok(_) => Some(&st.clerk.heal),
            Err(_) => None,
        };
        let max_bps = st.clerk.cluster.heal_max_bps();
        Some(execute_heal(&st.clerk.http, &tasks, tracker, max_bps).await)
    } else {
        None
    };

    Json(serde_json::json!({
        "ok": true,
        "replication": repl.as_str(),
        "copies": repl.total_copies(),
        "updated": updated.len(),
        "volume_ids": updated,
        "heal_tasks": heal_tasks,
    }))
    .into_response()
}

/// 扩容空卷请求体。
#[derive(Deserialize)]
struct GrowBody {
    #[serde(default = "default_grow_count")]
    count: u32,
    #[serde(default)]
    collection: String,
    /// XYZ；省略则用集群默认
    replication: Option<String>,
}

/// grow 默认创建 1 个卷。
fn default_grow_count() -> u32 {
    1
}

/// Master 预留卷并请求各节点物化空文件。
async fn grow_volumes(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<GrowBody>,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    let repl = match body.replication.as_deref() {
        Some(s) => match Replication::from_str(s) {
            Ok(r) => r,
            Err(e) => return (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        },
        None => st.clerk.cluster.default_replication,
    };
    match st
        .clerk
        .cluster
        .grow_volumes(body.count, &body.collection, repl)
    {
        Ok(vols) => {
            let _ = propose_cluster_snapshot(&st.clerk).await;
            // 请求各 volume 节点物化空卷文件。
            let mut created_on = Vec::new();
            for v in &vols {
                for loc in &v.locations {
                    let url = format!(
                        "{}/admin/create_volume/{}",
                        loc.url.trim_end_matches('/'),
                        v.id
                    );
                    match st.clerk.http.post(&url).send().await {
                        Ok(r) if r.status().is_success() => {
                            created_on.push(serde_json::json!({
                                "volume_id": v.id,
                                "node": loc.node_id,
                                "ok": true,
                            }));
                        }
                        Ok(r) => {
                            created_on.push(serde_json::json!({
                                "volume_id": v.id,
                                "node": loc.node_id,
                                "ok": false,
                                "status": r.status().as_u16(),
                            }));
                        }
                        Err(e) => {
                            created_on.push(serde_json::json!({
                                "volume_id": v.id,
                                "node": loc.node_id,
                                "ok": false,
                                "error": e.to_string(),
                            }));
                        }
                    }
                }
            }
            Json(serde_json::json!({
                "ok": true,
                "replication": repl.as_str(),
                "copies": repl.total_copies(),
                "volumes": vols.iter().map(|v| serde_json::json!({
                    "id": v.id,
                    "locations": v.locations.iter().map(|l| &l.node_id).collect::<Vec<_>>(),
                })).collect::<Vec<_>>(),
                "materialize": created_on,
            }))
            .into_response()
        }
        Err(e) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
    }
}

/// 当前 heal 运行状态。
async fn heal_status(State(st): State<AppState>) -> impl IntoResponse {
    Json(st.clerk.heal.snapshot()).into_response()
}

/// vacuum 配置 + 运行状态。
async fn vacuum_status(State(st): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "config": {
            "enabled": st.clerk.vacuum_config.enabled,
            "interval_secs": st.clerk.vacuum_config.interval_secs,
            "garbage_threshold": st.clerk.vacuum_config.garbage_threshold,
            "max_parallel_volumes": st.clerk.vacuum_config.max_parallel_volumes,
        },
        "status": st.clerk.vacuum.snapshot(),
    }))
    .into_response()
}

/// 手动 vacuum 查询参数。
#[derive(Deserialize)]
struct VacuumQuery {
    threshold: Option<f64>,  // 覆盖默认垃圾阈值
    volume_id: Option<u32>,  // 仅处理该卷
}

/// 手动触发一轮 vacuum（异步后台）。
async fn vacuum_now(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Query(q): Query<VacuumQuery>,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    let threshold = q
        .threshold
        .unwrap_or(st.clerk.vacuum_config.garbage_threshold)
        .clamp(0.0, 1.0);
    let only = q.volume_id.map(|id| vec![id]);
    let tasks = plan_vacuum_filtered(
        &st.clerk.cluster,
        threshold,
        only.as_deref(),
    );
    st.clerk.metrics.vacuum_tasks.set(tasks.len() as i64);
    if tasks.is_empty() {
        return Json(serde_json::json!({
            "ok": true,
            "started": false,
            "tasks_total": 0,
            "message": "nothing to vacuum",
            "threshold": threshold,
            "status": st.clerk.vacuum.snapshot(),
        }))
        .into_response();
    }
    match st.clerk.vacuum.try_begin(&tasks, threshold) {
        Ok(run_id) => {
            let master = st.clerk.clone();
            let planned = tasks.clone();
            tokio::spawn(async move {
                let _ = execute_vacuum(
                    &master.http,
                    &planned,
                    Some(&master.vacuum),
                    threshold,
                )
                .await;
                master
                    .metrics
                    .vacuum_runs_total
                    .with_label_values(&["ok"])
                    .inc();
            });
            Json(serde_json::json!({
                "ok": true,
                "started": true,
                "run_id": run_id,
                "tasks_total": tasks.len(),
                "threshold": threshold,
                "status": st.clerk.vacuum.snapshot(),
            }))
            .into_response()
        }
        Err(busy) => Json(serde_json::json!({
            "ok": true,
            "started": false,
            "busy": true,
            "message": "vacuum already running",
            "status": busy,
        }))
        .into_response(),
    }
}

/// 手动触发一轮 heal（异步后台）。
async fn heal_now(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    let tasks = plan_heal(&st.clerk.cluster);
    st.clerk.metrics.heal_tasks.set(tasks.len() as i64);
    if tasks.is_empty() {
        return Json(serde_json::json!({
            "ok": true,
            "started": false,
            "tasks_total": 0,
            "message": "nothing to heal",
            "status": st.clerk.heal.snapshot(),
        }))
        .into_response();
    }
    match st.clerk.heal.try_begin(&tasks) {
        Ok(run_id) => {
            let master = st.clerk.clone();
            let planned = tasks.clone();
            let max_bps = st.clerk.cluster.heal_max_bps();
            tokio::spawn(async move {
                let _ = execute_heal(&master.http, &planned, Some(&master.heal), max_bps).await;
                master
                    .metrics
                    .heal_runs_total
                    .with_label_values(&["ok"])
                    .inc();
            });
            Json(serde_json::json!({
                "ok": true,
                "started": true,
                "run_id": run_id,
                "tasks_total": tasks.len(),
                "max_bps": max_bps,
                "status": st.clerk.heal.snapshot(),
            }))
            .into_response()
        }
        Err(busy) => Json(serde_json::json!({
            "ok": true,
            "started": false,
            "busy": true,
            "message": "heal already running",
            "status": busy,
        }))
        .into_response(),
    }
}

/// 放置副本请求体。
#[derive(Deserialize)]
struct PlaceReplicasBody {
    source_node_id: String,
    target_node_id: String,
    #[serde(default = "default_true")]
    heal: bool, // 是否立即 heal 到 target
}

/// 将 `source_node_id` 上的卷副本放到 `target_node_id`
///（必要时抬升 XYZ，再优先向 target heal）。
async fn place_replicas(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<PlaceReplicasBody>,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    let source = body.source_node_id.trim().to_string();
    let target = body.target_node_id.trim().to_string();
    if source.is_empty() || target.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "source_node_id and target_node_id required".to_string(),
        )
            .into_response();
    }
    let volume_ids = match st.clerk.cluster.prepare_place_replicas(&source, &target) {
        Ok(ids) => ids,
        Err(e) => {
            return (
                StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::BAD_REQUEST),
                e.to_string(),
            )
                .into_response();
        }
    };
    // 先持久化配对，使 Admin 在 heal 完成前就能显示「B 是 A 的副本」。
    if let Err(e) = st.clerk.cluster.set_replica_of(&target, Some(&source)) {
        return (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::BAD_REQUEST),
            e.to_string(),
        )
            .into_response();
    }
    let _ = propose_cluster_snapshot(&st.clerk).await;

    let target_loc = st
        .clerk
        .cluster
        .servers()
        .into_iter()
        .find(|s| s.node_id == target)
        .map(|s| s.location);

    let heal_tasks = if body.heal && !volume_ids.is_empty() {
        let prefer = vec![target.clone()];
        let tasks = plan_heal_filtered(&st.clerk.cluster, Some(&volume_ids), Some(&prefer));
        st.clerk.metrics.heal_tasks.set(tasks.len() as i64);
        let tracker = match st.clerk.heal.try_begin(&tasks) {
            Ok(_) => Some(st.clerk.heal.clone()),
            Err(_) => None,
        };
        let max_bps = st.clerk.cluster.heal_max_bps();
        let results = execute_heal(&st.clerk.http, &tasks, tracker.as_ref(), max_bps).await;
        if let Some(loc) = &target_loc {
            for t in &results {
                if t.status == "ok" {
                    let _ = st.clerk.cluster.add_volume_location(t.volume_id, loc.clone());
                }
            }
            let _ = propose_cluster_snapshot(&st.clerk).await;
        }
        st.clerk
            .metrics
            .heal_runs_total
            .with_label_values(&["ok"])
            .inc();
        Some(results)
    } else {
        None
    };

    Json(serde_json::json!({
        "ok": true,
        "source_node_id": source,
        "target_node_id": target,
        "volume_ids": volume_ids,
        "heal_tasks": heal_tasks,
    }))
    .into_response()
}

/// 设置 replica_of 请求体。
#[derive(Deserialize)]
struct ReplicaOfBody {
    node_id: String,
    /// 空 / 省略则清除配对（不改卷数据）。
    #[serde(default)]
    replica_of: Option<String>,
}

/// 设置或清除节点 replica_of 配对。
async fn set_replica_of_api(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
    Json(body): Json<ReplicaOfBody>,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    let node = body.node_id.trim().to_string();
    if node.is_empty() {
        return (StatusCode::BAD_REQUEST, "node_id required".to_string()).into_response();
    }
    let source = body
        .replica_of
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty());
    match st.clerk.cluster.set_replica_of(&node, source) {
        Ok(()) => {
            let _ = propose_cluster_snapshot(&st.clerk).await;
            Json(serde_json::json!({
                "ok": true,
                "node_id": node,
                "replica_of": source,
            }))
            .into_response()
        }
        Err(e) => (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::BAD_REQUEST),
            e.to_string(),
        )
            .into_response(),
    }
}

/// 规划并执行一轮卷均衡。
async fn balance_now(
    State(st): State<AppState>,
    OriginalUri(uri): OriginalUri,
) -> impl IntoResponse {
    if !st.clerk.raft.is_leader() {
        return redirect_leader(&st, &uri);
    }
    let plan = plan_balance(&st.clerk.cluster);
    st.clerk
        .metrics
        .balance_moves
        .set(plan.moves.len() as i64);
    let max_bps = st.clerk.cluster.heal_max_bps();
    let done = execute_balance(&st.clerk.http, &plan, max_bps).await;
    st.clerk
        .metrics
        .balance_runs_total
        .with_label_values(&["ok"])
        .inc();
    Json(serde_json::json!({"planned": plan, "done": done})).into_response()
}

/// 无条件尝试 propose（内部用空 last_fp 强制写）。
async fn propose_cluster_snapshot(master: &ClerkServer) -> anyhow::Result<()> {
    let _ = propose_cluster_snapshot_if_changed(master, "").await?;
    Ok(())
}

/// 仅当拓扑指纹变化时 propose 全量集群快照；写入新条目时返回 `Some(fp)`。
async fn propose_cluster_snapshot_if_changed(
    master: &ClerkServer,
    last_fp: &str,
) -> anyhow::Result<Option<String>> {
    let snap = master.cluster.export_snapshot();
    let fp = {
        use std::hash::{Hash, Hasher};
        let bytes = serde_json::to_vec(&snap)?;
        let mut h = std::collections::hash_map::DefaultHasher::new();
        bytes.hash(&mut h);
        format!("{:016x}", h.finish())
    };
    if fp == last_fp {
        return Ok(None);
    }
    master
        .raft
        .propose(RaftRequest::ClusterMutation {
            payload: serde_json::json!({"op":"snapshot","snapshot": snap}),
        })
        .await?;
    Ok(Some(fp))
}
