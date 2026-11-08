// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Admin cluster topology, traffic scrape, and ops (heal / balance / scale).

use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::Mutex;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::server::AppState;

#[derive(Clone, Debug)]
struct K8sPodInfo {
    name: String,
    component: String,
    ip: String,
    phase: String,
    ready: bool,
    restarting: bool,
    reason: String,
    /// Kubernetes worker node (spec.nodeName).
    node_name: String,
}

#[derive(Clone, serde::Serialize)]
pub struct TopoNode {
    pub id: String,
    pub kind: String,
    pub label: String,
    pub url: String,
    pub status: String,
    pub meta: serde_json::Value,
}

#[derive(Clone, serde::Serialize)]
pub struct TopoEdge {
    pub from: String,
    pub to: String,
    pub kind: String,
    pub label: String,
    pub bytes_per_sec: f64,
    pub ops_per_sec: f64,
    pub bytes_total: f64,
    pub ops_total: f64,
}

#[derive(Default)]
struct TrafficSample {
    at: Option<Instant>,
    counters: HashMap<String, f64>,
}

struct TrafficState {
    prev: TrafficSample,
    rates: HashMap<String, f64>,
}

fn traffic_state() -> std::sync::MutexGuard<'static, TrafficState> {
    static TRAFFIC: std::sync::OnceLock<Mutex<TrafficState>> = std::sync::OnceLock::new();
    TRAFFIC
        .get_or_init(|| {
            Mutex::new(TrafficState {
                prev: TrafficSample::default(),
                rates: HashMap::new(),
            })
        })
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

async fn fetch_text(client: &reqwest::Client, url: &str) -> Option<String> {
    client
        .get(url)
        .timeout(Duration::from_secs(2))
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .text()
        .await
        .ok()
}

fn parse_prom_sum(text: &str, metric: &str) -> f64 {
    let mut sum = 0.0;
    for line in text.lines() {
        if line.starts_with('#') {
            continue;
        }
        let Some((left, right)) = line.rsplit_once(' ') else {
            continue;
        };
        let name = left.split('{').next().unwrap_or(left);
        if name == metric {
            if let Ok(v) = right.parse::<f64>() {
                sum += v;
            }
        }
    }
    sum
}

fn update_rates(counters: HashMap<String, f64>) -> HashMap<String, f64> {
    let mut g = traffic_state();
    let now = Instant::now();
    let mut rates = g.rates.clone();
    if let Some(prev_at) = g.prev.at {
        let dt = now.duration_since(prev_at).as_secs_f64().max(0.001);
        for (k, v) in &counters {
            let prev = g.prev.counters.get(k).copied().unwrap_or(*v);
            rates.insert(k.clone(), (*v - prev).max(0.0) / dt);
        }
    }
    g.prev = TrafficSample {
        at: Some(now),
        counters,
    };
    g.rates = rates.clone();
    rates
}

async fn scrape_counters(
    client: &reqwest::Client,
    s3_port: u16,
    clerk_url: &str,
    volume_urls: &[(String, String)], // (node_id, url)
) -> HashMap<String, f64> {
    let mut counters = HashMap::new();
    if let Some(text) = fetch_text(client, &format!("http://127.0.0.1:{s3_port}/metrics")).await {
        counters.insert(
            "s3_bytes".into(),
            parse_prom_sum(&text, "opens3_s3_request_bytes_total"),
        );
        counters.insert(
            "s3_reqs".into(),
            parse_prom_sum(&text, "opens3_s3_requests_total"),
        );
        counters.insert(
            "filer_bytes".into(),
            parse_prom_sum(&text, "opens3_filer_bytes_total"),
        );
    }
    if let Some(text) = fetch_text(client, &format!("{clerk_url}/metrics")).await {
        counters.insert(
            "clerk_assign".into(),
            parse_prom_sum(&text, "opens3_clerk_assign_total"),
        );
        counters.insert(
            "clerk_lookup".into(),
            parse_prom_sum(&text, "opens3_clerk_lookup_total"),
        );
        counters.insert(
            "clerk_heartbeat".into(),
            parse_prom_sum(&text, "opens3_clerk_heartbeat_total"),
        );
    }
    for (node_id, url) in volume_urls {
        if url.is_empty() {
            continue;
        }
        if let Some(text) = fetch_text(client, &format!("{url}/metrics")).await {
            counters.insert(
                format!("vol_bytes:{node_id}"),
                parse_prom_sum(&text, "opens3_volume_needle_bytes_total"),
            );
            counters.insert(
                format!("vol_ops:{node_id}"),
                parse_prom_sum(&text, "opens3_volume_needle_ops_total"),
            );
        }
    }
    counters
}

fn in_kubernetes() -> bool {
    std::path::Path::new("/var/run/secrets/kubernetes.io/serviceaccount/token").exists()
        || std::env::var_os("KUBERNETES_SERVICE_HOST").is_some()
}

fn env_usize(key: &str, default: usize) -> usize {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn env_u16(key: &str, default: u16) -> u16 {
    std::env::var(key)
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(default)
}

fn pod_status(p: &K8sPodInfo) -> &'static str {
    if p.restarting || p.reason == "CrashLoopBackOff" || p.reason == "OOMKilled" {
        return "crashloop";
    }
    match p.phase.as_str() {
        "Pending" => "pending",
        "Failed" | "Unknown" => "down",
        "Succeeded" => "down",
        "Running" if p.ready => "up",
        "Running" => "not_ready",
        _ if p.ready => "up",
        _ => "down",
    }
}

async fn list_k8s_pods() -> Vec<K8sPodInfo> {
    if !in_kubernetes() {
        return Vec::new();
    }
    let token = match std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/token")
    {
        Ok(t) => t,
        Err(_) => return Vec::new(),
    };
    let ns = std::env::var("OPENS3_K8S_NAMESPACE").unwrap_or_else(|_| "opens3".into());
    let release = std::env::var("OPENS3_HELM_RELEASE").unwrap_or_else(|_| "opens3".into());
    let host = std::env::var("KUBERNETES_SERVICE_HOST").unwrap_or_else(|_| "kubernetes.default.svc".into());
    let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".into());
    let selector = format!(
        "app.kubernetes.io/instance={release},app.kubernetes.io/name=opens3"
    );
    let url = format!(
        "https://{host}:{port}/api/v1/namespaces/{ns}/pods?labelSelector={}",
        urlencoding_encode(&selector)
    );

    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(3));
    if let Ok(ca) = std::fs::read("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt") {
        if let Ok(cert) = reqwest::Certificate::from_pem(&ca) {
            builder = builder.add_root_certificate(cert);
        }
    } else {
        builder = builder.danger_accept_invalid_certs(true);
    }
    let Ok(client) = builder.build() else {
        return Vec::new();
    };
    let Ok(resp) = client
        .get(&url)
        .bearer_auth(token.trim())
        .send()
        .await
    else {
        return Vec::new();
    };
    if !resp.status().is_success() {
        return Vec::new();
    }
    let Ok(body) = resp.json::<serde_json::Value>().await else {
        return Vec::new();
    };
    let mut out = Vec::new();
    let Some(items) = body.get("items").and_then(|v| v.as_array()) else {
        return out;
    };
    for item in items {
        let name = item
            .pointer("/metadata/name")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if name.is_empty() {
            continue;
        }
        // Skip pods being deleted so topology matches `kubectl get pods` Ready set.
        if item.pointer("/metadata/deletionTimestamp").is_some() {
            continue;
        }
        let component = item
            .pointer("/metadata/labels/app.kubernetes.io~1component")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        if !matches!(
            component.as_str(),
            "clerk" | "volume" | "s3" | "filer" | "postgres"
        ) {
            continue;
        }
        let ip = item
            .pointer("/status/podIP")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let phase = item
            .pointer("/status/phase")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();
        let ready = item
            .pointer("/status/conditions")
            .and_then(|v| v.as_array())
            .map(|conds| {
                conds.iter().any(|c| {
                    c.get("type").and_then(|t| t.as_str()) == Some("Ready")
                        && c.get("status").and_then(|s| s.as_str()) == Some("True")
                })
            })
            .unwrap_or(false);
        let mut reason = String::new();
        let mut restarting = false;
        if let Some(statuses) = item
            .pointer("/status/containerStatuses")
            .and_then(|v| v.as_array())
        {
            if let Some(cs) = statuses.first() {
                if let Some(r) = cs
                    .pointer("/state/waiting/reason")
                    .and_then(|v| v.as_str())
                {
                    reason = r.to_string();
                    restarting = r == "CrashLoopBackOff" || r == "ImagePullBackOff";
                } else if let Some(r) = cs
                    .pointer("/lastState/terminated/reason")
                    .and_then(|v| v.as_str())
                {
                    if !ready {
                        reason = r.to_string();
                    }
                }
            }
        }
        let node_name = item
            .pointer("/spec/nodeName")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        out.push(K8sPodInfo {
            name,
            component,
            ip,
            phase,
            ready,
            restarting,
            reason,
            node_name,
        });
    }
    out.sort_by(|a, b| a.component.cmp(&b.component).then(a.name.cmp(&b.name)));
    out
}

fn urlencoding_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b'=' => out.push_str("%3D"),
            b',' => out.push_str("%2C"),
            b'/' => out.push_str("%2F"),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn k8s_sts_urls(kind: &str) -> Vec<(String, String)> {
    if !in_kubernetes() {
        return Vec::new();
    }
    let ns = std::env::var("OPENS3_K8S_NAMESPACE").unwrap_or_else(|_| "opens3".into());
    let release = std::env::var("OPENS3_HELM_RELEASE").unwrap_or_else(|_| "opens3".into());
    let (headless_env, port_env, replicas_env, default_port) = match kind {
        "clerk" => (
            "OPENS3_CLERK_HEADLESS",
            "OPENS3_CLERK_PORT",
            "OPENS3_CLERK_REPLICAS",
            9333u16,
        ),
        "volume" => (
            "OPENS3_VOLUME_HEADLESS",
            "OPENS3_VOLUME_PORT",
            "OPENS3_VOLUME_REPLICAS",
            8080u16,
        ),
        _ => return Vec::new(),
    };
    let headless = std::env::var(headless_env).unwrap_or_else(|_| format!("{release}-{kind}-hl"));
    let port = env_u16(port_env, default_port);
    let replicas = env_usize(replicas_env, 0);
    if replicas == 0 {
        return Vec::new();
    }
    (0..replicas)
        .map(|i| {
            let node_id = format!("{release}-{kind}-{i}");
            let url = format!("http://{node_id}.{headless}.{ns}.svc.cluster.local:{port}");
            (node_id, url)
        })
        .collect()
}

/// Resolve which clerk pod is Raft leader.
/// openraft exposes `leader` as a numeric NodeId (`"0"`) and `leader_url` as the advertise URL.
fn leader_pod_name(
    cluster: &Option<serde_json::Value>,
    clerks: &[(String, String)],
) -> Option<String> {
    let c = cluster.as_ref()?;

    if let Some(url) = c.get("leader_url").and_then(|v| v.as_str()) {
        if let Some((n, _)) = clerks.iter().find(|(n, _)| url.contains(n.as_str())) {
            return Some(n.clone());
        }
        // http://opens3-clerk-0.hl...:9333 → opens3-clerk-0
        let host = url
            .trim_start_matches("https://")
            .trim_start_matches("http://")
            .split('/')
            .next()
            .unwrap_or("")
            .split(':')
            .next()
            .unwrap_or("");
        let label = host.split('.').next().unwrap_or(host);
        if !label.is_empty() {
            if let Some((n, _)) = clerks.iter().find(|(n, _)| n == label) {
                return Some(n.clone());
            }
        }
    }

    let leader = c.get("leader")?;
    let leader_str = if let Some(s) = leader.as_str() {
        s.to_string()
    } else if let Some(n) = leader.as_u64() {
        n.to_string()
    } else if let Some(n) = leader.as_i64() {
        n.to_string()
    } else {
        return None;
    };

    // Numeric openraft NodeId → StatefulSet ordinal (`opens3-clerk-0`)
    if let Ok(ord) = leader_str.parse::<u64>() {
        if let Some((n, _)) = clerks.iter().find(|(n, _)| {
            n.rsplit('-')
                .next()
                .and_then(|s| s.parse::<u64>().ok())
                == Some(ord)
        }) {
            return Some(n.clone());
        }
    }

    let host = leader_str.split(':').next().unwrap_or(&leader_str);
    if let Some((n, _)) = clerks
        .iter()
        .find(|(n, _)| n == host || n.ends_with(host) || host.starts_with(n.as_str()))
    {
        return Some(n.clone());
    }
    None
}

fn pod_is_raft_leader(cluster: &Option<serde_json::Value>, pod_name: &str) -> bool {
    leader_pod_name(cluster, &[(pod_name.to_string(), String::new())])
        .as_deref()
        == Some(pod_name)
}

fn leader_clerk_id(cluster: &Option<serde_json::Value>, clerks: &[(String, String)]) -> String {
    if let Some(n) = leader_pod_name(cluster, clerks) {
        return format!("clerk-{n}");
    }
    clerks
        .first()
        .map(|(n, _)| format!("clerk-{n}"))
        .unwrap_or_else(|| "clerk".into())
}

pub(crate) async fn admin_topology(State(st): State<AppState>) -> impl IntoResponse {
    let client = reqwest::Client::new();
    let clerk_svc = st.server.blob.clerk_url().trim_end_matches('/').to_string();
    let s3_port = st.server.port;
    let self_pod = std::env::var("POD_NAME").unwrap_or_default();
    let k8s = in_kubernetes();
    let k8s_pods = list_k8s_pods().await;

    let mut cluster: Option<serde_json::Value> = None;
    let clerk_svc_ok = match fetch_text(&client, &format!("{clerk_svc}/cluster/status")).await {
        Some(body) => {
            cluster = serde_json::from_str(&body).ok();
            true
        }
        None => false,
    };

    let mut nodes = Vec::new();
    let mut edges = Vec::new();
    let mut s3_ids: Vec<String> = Vec::new();
    let mut clerk_pairs: Vec<(String, String)> = Vec::new(); // (name, url)
    let mut volume_urls: Vec<(String, String)> = Vec::new();
    let mut seen_volume = HashSet::new();
    let mut seen_clerk = HashSet::new();
    let mut seen_s3 = HashSet::new();
    // Collect Deployment pods first so we can assign stable ordinal IDs
    // (filer-0) that survive ReplicaSet rollouts — required for
    // persisting manual topology layout across refreshes.
    let mut filer_pods: Vec<&K8sPodInfo> = Vec::new();

    if !k8s_pods.is_empty() {
        let clerk_port = env_u16("OPENS3_CLERK_PORT", 9333);
        let volume_port = env_u16("OPENS3_VOLUME_PORT", 8080);
        let filer_port = env_u16("OPENS3_FILER_PORT", s3_port);
        let ns = std::env::var("OPENS3_K8S_NAMESPACE").unwrap_or_else(|_| "opens3".into());
        let release = std::env::var("OPENS3_HELM_RELEASE").unwrap_or_else(|_| "opens3".into());
        let clerk_hl = std::env::var("OPENS3_CLERK_HEADLESS")
            .unwrap_or_else(|_| format!("{release}-clerk-hl"));
        let volume_hl = std::env::var("OPENS3_VOLUME_HEADLESS")
            .unwrap_or_else(|_| format!("{release}-volume-hl"));

        for p in &k8s_pods {
            let status = pod_status(p).to_string();
            match p.component.as_str() {
                "clerk" => {
                    let url = format!(
                        "http://{}.{}.{ns}.svc.cluster.local:{clerk_port}",
                        p.name, clerk_hl
                    );
                    seen_clerk.insert(p.name.clone());
                    clerk_pairs.push((p.name.clone(), url.clone()));
                    let is_leader = pod_is_raft_leader(&cluster, &p.name);
                    let raft_role = if is_leader { "leader" } else { "follower" };
                    nodes.push(TopoNode {
                        id: format!("clerk-{}", p.name),
                        kind: "clerk".into(),
                        label: format!("{} · {raft_role}", p.name),
                        url,
                        status,
                        meta: serde_json::json!({
                            "role": "clerk",
                            "raft_role": raft_role,
                            "source": "k8s-api",
                            "pod": p.name,
                            "ip": p.ip,
                            "phase": p.phase,
                            "ready": p.ready,
                            "reason": p.reason,
                            "leader": is_leader,
                        }),
                    });
                }
                "volume" => {
                    let url = if p.ip.is_empty() {
                        format!(
                            "http://{}.{}.{ns}.svc.cluster.local:{volume_port}",
                            p.name, volume_hl
                        )
                    } else {
                        format!("http://{}:{volume_port}", p.ip)
                    };
                    seen_volume.insert(p.name.clone());
                    volume_urls.push((p.name.clone(), url.clone()));
                    nodes.push(TopoNode {
                        id: format!("volume-{}", p.name),
                        kind: "volume".into(),
                        label: format!("数据节点 · {}", p.name),
                        url,
                        status,
                        meta: serde_json::json!({
                            "role": "volume",
                            "source": "k8s-api",
                            "pod": p.name,
                            "ip": p.ip,
                            "phase": p.phase,
                            "ready": p.ready,
                            "reason": p.reason,
                            "kubernetes_node": p.node_name,
                            "volumes": [],
                            "volume_count": 0,
                        }),
                    });
                }
                // Unified filer (S3 + path API). Legacy charts may still label "s3".
                "filer" | "s3" => filer_pods.push(p),
                _ => {}
            }
        }

        filer_pods.sort_by(|a, b| a.name.cmp(&b.name));

        for (i, p) in filer_pods.into_iter().enumerate() {
            let status = pod_status(p).to_string();
            let url = if !p.ip.is_empty() {
                format!("http://{}:{filer_port}", p.ip)
            } else {
                std::env::var("OPENS3_FILER_URL")
                    .unwrap_or_else(|_| format!("http://127.0.0.1:{filer_port}"))
            };
            let id = format!("filer-{i}");
            seen_s3.insert(p.name.clone());
            // Same process serves S3 — client-s3 edges attach here.
            s3_ids.push(id.clone());
            let mine = !self_pod.is_empty() && self_pod == p.name;
            nodes.push(TopoNode {
                id,
                kind: "filer".into(),
                label: if mine {
                    format!("Filer · {} (this)", p.name)
                } else {
                    format!("Filer · {}", p.name)
                },
                url: url.trim_end_matches('/').to_string(),
                status,
                meta: serde_json::json!({
                    "role": "filer",
                    "source": "k8s-api",
                    "pod": p.name,
                    "ordinal": i,
                    "ip": p.ip,
                    "phase": p.phase,
                    "ready": p.ready,
                    "reason": p.reason,
                    "self": mine,
                    "auth_enabled": st.server.auth.read().enabled,
                    "protocols": ["s3", "http", "webdav"],
                    "shared_meta": std::env::var("DATABASE_URL").is_ok()
                        || !std::env::var("OPENS3_FILER_URL").unwrap_or_default().is_empty(),
                }),
            });
        }
    }

    // Non-k8s / API failure: single local filer (+ S3 on same port).
    if s3_ids.is_empty() && !nodes.iter().any(|n| n.kind == "filer") {
        let id = "filer-local".to_string();
        s3_ids.push(id.clone());
        nodes.push(TopoNode {
            id,
            kind: "filer".into(),
            label: format!("Filer :{s3_port}"),
            url: format!("http://127.0.0.1:{s3_port}"),
            status: "up".into(),
            meta: serde_json::json!({
                "role": "filer",
                "source": "local",
                "port": s3_port,
                "protocols": ["s3", "http", "webdav"],
                "auth_enabled": st.server.auth.read().enabled,
            }),
        });
    }

    if clerk_pairs.is_empty() {
        // Prefer STS DNS discovery so scaled clerks appear even without API access.
        let discovered = k8s_sts_urls("clerk");
        if discovered.is_empty() {
            clerk_pairs.push(("clerk".into(), clerk_svc.clone()));
            nodes.push(TopoNode {
                id: "clerk".into(),
                kind: "clerk".into(),
                label: "Clerk (control)".into(),
                url: clerk_svc.clone(),
                status: if clerk_svc_ok { "up" } else { "down" }.into(),
                meta: serde_json::json!({
                    "role": "clerk",
                    "source": "service",
                    "url": clerk_svc,
                }),
            });
        } else {
            for (name, url) in discovered {
                if !seen_clerk.insert(name.clone()) {
                    continue;
                }
                let healthy = fetch_text(&client, &format!("{url}/cluster/status")).await.is_some()
                    || fetch_text(&client, &format!("{url}/health")).await.is_some();
                clerk_pairs.push((name.clone(), url.clone()));
                nodes.push(TopoNode {
                    id: format!("clerk-{name}"),
                    kind: "clerk".into(),
                    label: format!("Clerk · {name}"),
                    url,
                    status: if healthy { "up" } else { "down" }.into(),
                    meta: serde_json::json!({
                        "role": "clerk",
                        "source": "k8s-dns",
                        "pod": name,
                    }),
                });
            }
        }
    }

    // Enrich volumes from clerk heartbeat (URLs / disk meta), without dropping k8s pods.
    if let Some(ref c) = cluster {
        if let Some(servers) = c.get("servers").and_then(|v| v.as_array()) {
            for (i, s) in servers.iter().enumerate() {
                let node_id = s
                    .get("node_id")
                    .and_then(|v| v.as_str())
                    .unwrap_or("volume")
                    .to_string();
                let url = s
                    .pointer("/location/public_url")
                    .or_else(|| s.pointer("/location/url"))
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .trim_end_matches('/')
                    .to_string();
                let last_seen = s.get("last_seen").and_then(|v| v.as_u64()).unwrap_or(0);
                let stale = now_secs().saturating_sub(last_seen) > 45;
                if let Some(existing) = nodes.iter_mut().find(|n| n.id == format!("volume-{node_id}")) {
                    if !url.is_empty() {
                        existing.url = url.clone();
                    }
                    if let Some(obj) = existing.meta.as_object_mut() {
                        obj.insert("free_disk_bytes".into(), s.get("free_disk_bytes").cloned().unwrap_or(serde_json::Value::Null));
                        obj.insert("last_seen".into(), serde_json::json!(last_seen));
                        obj.insert("heartbeat".into(), serde_json::json!(if stale { "stale" } else { "ok" }));
                        obj.insert(
                            "online".into(),
                            serde_json::json!(s.get("online").and_then(|v| v.as_bool()).unwrap_or(false)),
                        );
                        if let Some(r) = s.get("replica_of").and_then(|v| v.as_str()) {
                            if !r.is_empty() {
                                obj.insert("replica_of".into(), serde_json::json!(r));
                            }
                        }
                    }
                    if existing.status == "up" && stale {
                        existing.status = "stale".into();
                    }
                    if let Some((_, u)) = volume_urls.iter_mut().find(|(n, _)| n == &node_id) {
                        if !url.is_empty() {
                            *u = url;
                        }
                    }
                    continue;
                }
                // In Kubernetes, skip clerk-only ghosts after STS scale-in.
                if k8s_pods.iter().any(|p| p.component == "volume") {
                    continue;
                }
                if !seen_volume.insert(node_id.clone()) {
                    continue;
                }
                volume_urls.push((node_id.clone(), url.clone()));
                nodes.push(TopoNode {
                    id: format!("volume-{node_id}"),
                    kind: "volume".into(),
                    label: format!("Volume · {node_id}"),
                    url,
                    status: if stale { "stale" } else { "up" }.into(),
                    meta: {
                        let mut m = serde_json::json!({
                            "role": "volume",
                            "source": "clerk",
                            "index": i,
                            "last_seen": last_seen,
                            "free_disk_bytes": s.get("free_disk_bytes"),
                            "dc": s.pointer("/location/data_center"),
                            "rack": s.pointer("/location/rack"),
                            "online": s.get("online").and_then(|v| v.as_bool()).unwrap_or(false),
                        });
                        if let Some(r) = s.get("replica_of").and_then(|v| v.as_str()) {
                            if !r.is_empty() {
                                m.as_object_mut()
                                    .unwrap()
                                    .insert("replica_of".into(), serde_json::json!(r));
                            }
                        }
                        m
                    },
                });
            }
        }
    }

    for (node_id, url) in k8s_sts_urls("volume") {
        if !seen_volume.insert(node_id.clone()) {
            continue;
        }
        let healthy = fetch_text(&client, &format!("{url}/health")).await.is_some();
        volume_urls.push((node_id.clone(), url.clone()));
        nodes.push(TopoNode {
            id: format!("volume-{node_id}"),
            kind: "volume".into(),
            label: format!("Volume · {node_id}"),
            url,
            status: if healthy { "up" } else { "down" }.into(),
            meta: serde_json::json!({
                "role": "volume",
                "source": "k8s-dns",
            }),
        });
    }

    let leader_id = leader_clerk_id(&cluster, &clerk_pairs);

    // Re-label clerks now that the full peer set is known (openraft numeric leader id).
    if let Some(leader_pod) = leader_pod_name(&cluster, &clerk_pairs) {
        for n in nodes.iter_mut().filter(|n| n.kind == "clerk") {
            let is_leader = n.id == format!("clerk-{leader_pod}");
            let raft_role = if is_leader { "leader" } else { "follower" };
            if let Some(obj) = n.meta.as_object_mut() {
                obj.insert("leader".into(), serde_json::json!(is_leader));
                obj.insert("raft_role".into(), serde_json::json!(raft_role));
            }
            if let Some(pod) = n.meta.get("pod").and_then(|v| v.as_str()) {
                n.label = format!("{pod} · {raft_role}");
            } else {
                n.label = format!("{} · {raft_role}", n.label);
            }
        }
    } else {
        // Leader unknown: still mark non-leaders as follower once we have multiple clerks.
        if clerk_pairs.len() > 1 {
            for n in nodes.iter_mut().filter(|n| n.kind == "clerk") {
                if n.meta.get("leader").and_then(|v| v.as_bool()) == Some(true) {
                    continue;
                }
                if let Some(obj) = n.meta.as_object_mut() {
                    obj.insert("raft_role".into(), serde_json::json!("follower"));
                }
                if let Some(pod) = n.meta.get("pod").and_then(|v| v.as_str()) {
                    n.label = format!("{pod} · follower");
                }
            }
        }
    }

    // Raft replication edges between clerks (leader → each follower).
    for (name, _) in &clerk_pairs {
        let mid = format!("clerk-{name}");
        if mid == leader_id {
            continue;
        }
        edges.push(TopoEdge {
            from: leader_id.clone(),
            to: mid,
            kind: "raft".into(),
            label: "raft".into(),
            bytes_per_sec: 0.0,
            ops_per_sec: 0.0,
            bytes_total: 0.0,
            ops_total: 1.0, // keep line visible even with zero traffic
        });
    }

    // Double-scrape so one API call can return non-zero rates.
    let c1 = scrape_counters(&client, s3_port, &clerk_svc, &volume_urls).await;
    let _ = update_rates(c1);
    tokio::time::sleep(Duration::from_millis(400)).await;
    let counters = scrape_counters(&client, s3_port, &clerk_svc, &volume_urls).await;
    let rates = update_rates(counters.clone());
    let n_vol = volume_urls.len().max(1);
    let n_s3 = s3_ids.len().max(1) as f64;

    let s3_bytes_total = {
        let a = counters.get("s3_bytes").copied().unwrap_or(0.0);
        if a > 0.0 {
            a
        } else {
            counters.get("filer_bytes").copied().unwrap_or(0.0)
        }
    };
    let s3_bytes_rate = {
        let a = rates.get("s3_bytes").copied().unwrap_or(0.0);
        if a > 0.0 {
            a
        } else {
            rates.get("filer_bytes").copied().unwrap_or(0.0)
        }
    };

    let filer_ids: Vec<String> = nodes
        .iter()
        .filter(|n| n.kind == "filer")
        .map(|n| n.id.clone())
        .collect();
    // Fallback: env points at shared Filer even if pod discovery missed it.
    if filer_ids.is_empty() {
        if let Ok(url) = std::env::var("OPENS3_FILER_URL") {
            let url = url.trim_end_matches('/').to_string();
            if !url.is_empty() {
                nodes.push(TopoNode {
                    id: "filer".into(),
                    kind: "filer".into(),
                    label: "Filer (meta)".into(),
                    url: url.clone(),
                    status: if fetch_text(&client, &format!("{url}/health"))
                        .await
                        .is_some()
                    {
                        "up".into()
                    } else {
                        "down".into()
                    },
                    meta: serde_json::json!({
                        "role": "filer",
                        "source": "env",
                    }),
                });
            }
        }
    }
    let filer_ids: Vec<String> = nodes
        .iter()
        .filter(|n| n.kind == "filer")
        .map(|n| n.id.clone())
        .collect();

    // Shared metadata store (Postgres / MySQL / …) — gateways open it in-process.
    let mut metadb_ids: Vec<String> = Vec::new();
    let mut pg_pods: Vec<&K8sPodInfo> = k8s_pods
        .iter()
        .filter(|p| p.component == "postgres")
        .collect();
    pg_pods.sort_by(|a, b| a.name.cmp(&b.name));
    for (i, p) in pg_pods.into_iter().enumerate() {
        let id = format!("metadb-{i}");
        metadb_ids.push(id.clone());
        nodes.push(TopoNode {
            id,
            kind: "metadb".into(),
            label: format!("Postgres · {}", p.name),
            url: if p.ip.is_empty() {
                String::new()
            } else {
                format!("{}:5432", p.ip)
            },
            status: pod_status(p).to_string(),
            meta: serde_json::json!({
                "role": "metadb",
                "engine": "postgres",
                "source": "k8s-api",
                "pod": p.name,
                "ip": p.ip,
                "phase": p.phase,
                "ready": p.ready,
                "ordinal": i,
            }),
        });
    }
    if metadb_ids.is_empty() {
        if let Ok(db_url) = std::env::var("DATABASE_URL") {
            if !db_url.is_empty() {
                let backend = std::env::var("OPENS3_META_BACKEND")
                    .unwrap_or_else(|_| "postgres".into());
                let engine = if backend == "tidb" {
                    "mysql/tidb"
                } else {
                    backend.as_str()
                };
                let host = db_url
                    .split('@')
                    .nth(1)
                    .and_then(|s| s.split('/').next())
                    .unwrap_or("shared-db");
                metadb_ids.push("metadb".into());
                nodes.push(TopoNode {
                    id: "metadb".into(),
                    kind: "metadb".into(),
                    label: format!("{engine} · meta"),
                    url: host.to_string(),
                    status: "up".into(),
                    meta: serde_json::json!({
                        "role": "metadb",
                        "engine": engine,
                        "source": "env",
                        "host": host,
                    }),
                });
            }
        }
    }

    // Distinct client nodes per public entry (S3 vs Filer HTTP vs WebDAV).
    // FUSE via s3fs/goofys shares the S3 entry; davfs-style FUSE is WebDAV → Filer.
    let mut client_nodes: Vec<TopoNode> = Vec::new();
    if !s3_ids.is_empty() {
        client_nodes.push(TopoNode {
            id: "client-s3".into(),
            kind: "client".into(),
            label: "S3 客户端".into(),
            url: "".into(),
            status: "up".into(),
            meta: serde_json::json!({
                "entry": "s3",
                "protocols": ["s3", "aws-cli", "sdk"],
            }),
        });
        client_nodes.push(TopoNode {
            id: "client-fuse".into(),
            kind: "client".into(),
            label: "FUSE 客户端".into(),
            url: "".into(),
            status: "up".into(),
            meta: serde_json::json!({
                "entry": "fuse",
                "protocols": ["s3fs", "goofys"],
                "via": "s3",
            }),
        });
    }
    if !filer_ids.is_empty() {
        client_nodes.push(TopoNode {
            id: "client-http".into(),
            kind: "client".into(),
            label: "HTTP 客户端".into(),
            url: "".into(),
            status: "up".into(),
            meta: serde_json::json!({
                "entry": "http",
                "protocols": ["http", "curl", "rclone"],
                "via": "filer",
            }),
        });
        client_nodes.push(TopoNode {
            id: "client-webdav".into(),
            kind: "client".into(),
            label: "WebDAV 客户端".into(),
            url: "".into(),
            status: "up".into(),
            meta: serde_json::json!({
                "entry": "webdav",
                "protocols": ["webdav", "davfs", "finder"],
                "via": "filer",
            }),
        });
    }

    for sid in &s3_ids {
        // S3 SDK / CLI entry
        edges.push(TopoEdge {
            from: "client-s3".into(),
            to: sid.clone(),
            kind: "client".into(),
            label: "S3".into(),
            bytes_per_sec: s3_bytes_rate / n_s3,
            ops_per_sec: rates.get("s3_reqs").copied().unwrap_or(0.0) / n_s3,
            bytes_total: s3_bytes_total / n_s3,
            ops_total: counters.get("s3_reqs").copied().unwrap_or(0.0) / n_s3,
        });
        // FUSE (s3fs / goofys) also talks S3 protocol to the gateway
        edges.push(TopoEdge {
            from: "client-fuse".into(),
            to: sid.clone(),
            kind: "client".into(),
            label: "FUSE".into(),
            bytes_per_sec: 0.0,
            ops_per_sec: 0.0,
            bytes_total: 0.0,
            ops_total: 1.0,
        });
        for mid in &metadb_ids {
            edges.push(TopoEdge {
                from: sid.clone(),
                to: mid.clone(),
                kind: "meta".into(),
                label: "meta".into(),
                bytes_per_sec: 0.0,
                ops_per_sec: 0.0,
                bytes_total: 0.0,
                ops_total: 1.0,
            });
        }
        edges.push(TopoEdge {
            from: sid.clone(),
            to: leader_id.clone(),
            kind: "control".into(),
            label: "assign / lookup".into(),
            bytes_per_sec: 0.0,
            ops_per_sec: (rates.get("clerk_assign").copied().unwrap_or(0.0)
                + rates.get("clerk_lookup").copied().unwrap_or(0.0))
                / n_s3,
            bytes_total: 0.0,
            ops_total: (counters.get("clerk_assign").copied().unwrap_or(0.0)
                + counters.get("clerk_lookup").copied().unwrap_or(0.0))
                / n_s3,
        });
    }

    // Gateways write object bytes straight to Volume; both also open the shared
    // meta DB in-process (Postgres/MySQL) — no S3→Filer metadata hop.
    //   S3 SDK path  → S3  ↔ Volume (+ meta DB)
    //   HTTP/WebDAV  → Filer ↔ Volume (+ meta DB)
    // After /vol/list we drop data edges to nodes that are never a volume
    // primary (pure replica holders).
    // Unified filer: s3_ids aliases the same filer node ids — emit data edges once.
    let data_gws: Vec<&String> = if !filer_ids.is_empty() {
        filer_ids.iter().collect()
    } else {
        s3_ids.iter().collect()
    };
    let n_gw = data_gws.len().max(1) as f64;
    for (node_id, _) in &volume_urls {
        let vid = format!("volume-{node_id}");
        let vb = counters
            .get(&format!("vol_bytes:{node_id}"))
            .copied()
            .unwrap_or(0.0);
        let vo = counters
            .get(&format!("vol_ops:{node_id}"))
            .copied()
            .unwrap_or(0.0);
        let mut bps = rates
            .get(&format!("vol_bytes:{node_id}"))
            .copied()
            .unwrap_or(0.0);
        let ops = rates
            .get(&format!("vol_ops:{node_id}"))
            .copied()
            .unwrap_or(0.0);
        if bps <= 0.0 {
            bps = rates.get("filer_bytes").copied().unwrap_or(0.0) / n_vol as f64;
        }
        let bytes_total = if vb > 0.0 {
            vb
        } else {
            counters.get("filer_bytes").copied().unwrap_or(0.0) / n_vol as f64
        };
        for gw in &data_gws {
            edges.push(TopoEdge {
                from: (*gw).clone(),
                to: vid.clone(),
                kind: "data".into(),
                label: "data".into(),
                bytes_per_sec: bps / n_gw,
                ops_per_sec: ops / n_gw,
                bytes_total: bytes_total / n_gw,
                ops_total: vo / n_gw,
            });
        }
        edges.push(TopoEdge {
            from: vid,
            to: leader_id.clone(),
            kind: "control".into(),
            label: "heartbeat".into(),
            bytes_per_sec: 0.0,
            ops_per_sec: rates.get("clerk_heartbeat").copied().unwrap_or(0.0) / n_vol as f64,
            bytes_total: 0.0,
            ops_total: counters.get("clerk_heartbeat").copied().unwrap_or(0.0) / n_vol as f64,
        });
    }

    // Volume → Volume replica links + per-node data-volume inventory from /vol/list.
    let known_volume_ids: HashSet<String> = nodes
        .iter()
        .filter(|n| n.kind == "volume")
        .map(|n| n.id.clone())
        .collect();
    let mut replica_dirs: HashMap<(String, String), (u64, String)> = HashMap::new();
    let mut repl_by_code: HashMap<String, u64> = HashMap::new();
    let mut loc_hist: HashMap<u64, u64> = HashMap::new();
    let mut multi_location: u64 = 0;
    let mut under_replicated: u64 = 0;
    let mut over_replicated: u64 = 0;
    let mut volumes_by_node: HashMap<String, Vec<serde_json::Value>> = HashMap::new();
    let mut primary_nodes: HashSet<String> = HashSet::new();
    // Node ids (pod / heartbeat id) that participate in an explicit replica_of chain.
    // Location-derived edges must not mesh these — e.g. chain 0←1←2 must not draw 0→2
    // just because a volume happens to have copies on all three.
    let mut replica_of_nodes: HashSet<String> = HashSet::new();
    for n in nodes.iter().filter(|n| n.kind == "volume") {
        let Some(src) = n.meta.get("replica_of").and_then(|v| v.as_str()) else {
            continue;
        };
        if src.is_empty() {
            continue;
        }
        let nid = n
            .meta
            .get("pod")
            .and_then(|v| v.as_str())
            .unwrap_or_else(|| n.id.strip_prefix("volume-").unwrap_or(&n.id))
            .to_string();
        replica_of_nodes.insert(nid);
        replica_of_nodes.insert(src.to_string());
    }
    if let Some(body) = fetch_text(&client, &format!("{clerk_svc}/vol/list")).await {
        if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
            let vols = v
                .get("volumes")
                .and_then(|x| x.as_array())
                .cloned()
                .unwrap_or_default();
            for vol in vols {
                let code = replication_code_of(&vol);
                let want = replication_copies(&code);
                *repl_by_code.entry(code.clone()).or_default() += 1;
                let vol_id = vol.get("id").and_then(|x| x.as_u64()).unwrap_or(0);
                let size = vol.get("size").and_then(|x| x.as_u64()).unwrap_or(0);

                let mut locs: Vec<String> = Vec::new();
                let mut seen = HashSet::new();
                if let Some(arr) = vol.get("locations").and_then(|x| x.as_array()) {
                    for loc in arr {
                        let Some(n) = loc.get("node_id").and_then(|x| x.as_str()) else {
                            continue;
                        };
                        let id = format!("volume-{n}");
                        let state = vol
                            .get("state")
                            .and_then(|x| x.as_str())
                            .unwrap_or("offline");
                        volumes_by_node.entry(n.to_string()).or_default().push(
                            serde_json::json!({
                                "id": vol_id,
                                "replication": code,
                                "size": size,
                                "state": state,
                                "online": state != "offline",
                            }),
                        );
                        if !known_volume_ids.contains(&id) || !seen.insert(id.clone()) {
                            continue;
                        }
                        locs.push(id);
                    }
                }
                let have = locs.len() as u64;
                *loc_hist.entry(have).or_default() += 1;
                if have < want {
                    under_replicated += 1;
                } else if have > want {
                    over_replicated += 1;
                }
                if locs.len() >= 2 {
                    multi_location += 1;
                }
                if let Some(p) = locs.first().and_then(|id| id.strip_prefix("volume-")) {
                    primary_nodes.insert(p.to_string());
                }
                // Only draw replica edges for intentional multi-copy XYZ (not accidental
                // dual locations on 000 shards — those look like "nodes are linked").
                if locs.len() < 2 || want <= 1 {
                    continue;
                }
                // First listed location is primary/source for replication direction.
                let primary = locs[0].clone();
                for replica in locs.into_iter().skip(1) {
                    let Some(pn) = primary.strip_prefix("volume-") else {
                        continue;
                    };
                    let Some(rn) = replica.strip_prefix("volume-") else {
                        continue;
                    };
                    // Prefer explicit replica_of topology over location mesh.
                    if replica_of_nodes.contains(pn) || replica_of_nodes.contains(rn) {
                        continue;
                    }
                    let ent = replica_dirs
                        .entry((primary.clone(), replica))
                        .or_insert((0, code.clone()));
                    ent.0 += 1;
                }
            }
        }
    }
    // Hide S3 data edges only for nodes that never appear as a volume primary.
    // If inventory is empty (fresh cluster), fall back to hiding dedicated replica_of
    // targets so unpaired primaries still show write paths.
    let mut no_s3_data: HashSet<String> = HashSet::new();
    if !primary_nodes.is_empty() {
        for (node_id, _) in &volume_urls {
            if !primary_nodes.contains(node_id) {
                no_s3_data.insert(node_id.clone());
            }
        }
    } else {
        for n in nodes.iter().filter(|n| n.kind == "volume") {
            let has_src = n
                .meta
                .get("replica_of")
                .and_then(|v| v.as_str())
                .map(|s| !s.is_empty())
                .unwrap_or(false);
            if !has_src {
                continue;
            }
            let key = n
                .meta
                .get("pod")
                .and_then(|v| v.as_str())
                .unwrap_or_else(|| n.id.strip_prefix("volume-").unwrap_or(&n.id))
                .to_string();
            no_s3_data.insert(key);
        }
    }
    if !no_s3_data.is_empty() {
        edges.retain(|e| {
            if e.kind != "data" {
                return true;
            }
            let from_nid = e.from.strip_prefix("volume-");
            let to_nid = e.to.strip_prefix("volume-");
            match (from_nid, to_nid) {
                (Some(n), _) | (_, Some(n)) if no_s3_data.contains(n) => false,
                _ => true,
            }
        });
    }
    for n in nodes.iter_mut().filter(|n| n.kind == "volume") {
        let pod = n
            .meta
            .get("pod")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        // topo id is volume-<pod_or_node_id>; heartbeat node_id often equals pod name.
        let key = if !pod.is_empty() {
            pod
        } else {
            n.id.strip_prefix("volume-").unwrap_or(&n.id).to_string()
        };
        let mut list = volumes_by_node.remove(&key).unwrap_or_default();
        list.sort_by_key(|v| v.get("id").and_then(|x| x.as_u64()).unwrap_or(0));
        let count = list.len();
        if let Some(obj) = n.meta.as_object_mut() {
            obj.insert("volumes".into(), serde_json::Value::Array(list));
            obj.insert("volume_count".into(), serde_json::json!(count));
        }
    }
    let mut loc_dist: Vec<serde_json::Value> = loc_hist
        .into_iter()
        .map(|(copies, volumes)| serde_json::json!({ "actual_copies": copies, "volumes": volumes }))
        .collect();
    loc_dist.sort_by_key(|v| v.get("actual_copies").and_then(|x| x.as_u64()).unwrap_or(0));
    for ((a, b), (count, code)) in replica_dirs {
        edges.push(TopoEdge {
            from: a,
            to: b,
            kind: "replica".into(),
            label: format!("{code} ×{count}"),
            bytes_per_sec: 0.0,
            ops_per_sec: 0.0,
            bytes_total: 0.0,
            ops_total: count as f64,
        });
    }
    // Node-level "added as replica of X" intent (shown even when heal still pending).
    let mut seen_pair = HashSet::new();
    for n in nodes.iter().filter(|n| n.kind == "volume") {
        let Some(src) = n.meta.get("replica_of").and_then(|v| v.as_str()) else {
            continue;
        };
        if src.is_empty() {
            continue;
        }
        let from = format!("volume-{src}");
        let to = n.id.clone();
        if !known_volume_ids.contains(&from) || !known_volume_ids.contains(&to) {
            continue;
        }
        if !seen_pair.insert((from.clone(), to.clone())) {
            continue;
        }
        // Skip if a volume-location replica edge already covers this pair.
        if edges
            .iter()
            .any(|e| e.kind == "replica" && e.from == from && e.to == to)
        {
            continue;
        }
        edges.push(TopoEdge {
            from,
            to,
            kind: "replica".into(),
            label: "副本".into(),
            bytes_per_sec: 0.0,
            ops_per_sec: 0.0,
            bytes_total: 0.0,
            ops_total: 1.0,
        });
    }
    let mut repl_codes: Vec<serde_json::Value> = repl_by_code
        .into_iter()
        .map(|(code, count)| {
            let copies = replication_copies(&code);
            serde_json::json!({
                "code": code,
                "volumes": count,
                "copies": copies,
                "desc": replication_desc(&code),
            })
        })
        .collect();
    repl_codes.sort_by(|a, b| {
        a.get("code")
            .and_then(|v| v.as_str())
            .cmp(&b.get("code").and_then(|v| v.as_str()))
    });
    let replication_info = serde_json::json!({
        "multi_location_volumes": multi_location,
        "under_replicated": under_replicated,
        "over_replicated": over_replicated,
        "by_code": repl_codes,
        "actual_copies": loc_dist,
        "legend": [
            {"code": "XYZ", "desc": "X=跨DC额外份，Y=跨rack额外份，Z=同rack额外份；总份数=1+X+Y+Z"},
            {"code": "000", "desc": "单副本（无冗余）"},
            {"code": "001", "desc": "同 rack 共 2 份"},
            {"code": "010", "desc": "跨 rack 共 2 份"},
            {"code": "100", "desc": "跨 DC 共 2 份"},
        ],
    });

    // Filer public entries: HTTP /entries and WebDAV /dav (not used by S3 SDK path).
    for fid in &filer_ids {
        edges.push(TopoEdge {
            from: "client-http".into(),
            to: fid.clone(),
            kind: "client".into(),
            label: "HTTP".into(),
            bytes_per_sec: 0.0,
            ops_per_sec: 0.0,
            bytes_total: 0.0,
            ops_total: 1.0,
        });
        edges.push(TopoEdge {
            from: "client-webdav".into(),
            to: fid.clone(),
            kind: "client".into(),
            label: "WebDAV".into(),
            bytes_per_sec: 0.0,
            ops_per_sec: 0.0,
            bytes_total: 0.0,
            ops_total: 1.0,
        });
        for mid in &metadb_ids {
            edges.push(TopoEdge {
                from: fid.clone(),
                to: mid.clone(),
                kind: "meta".into(),
                label: "meta".into(),
                bytes_per_sec: 0.0,
                ops_per_sec: 0.0,
                bytes_total: 0.0,
                ops_total: 1.0,
            });
        }
        edges.push(TopoEdge {
            from: fid.clone(),
            to: leader_id.clone(),
            kind: "control".into(),
            label: "assign / lookup".into(),
            bytes_per_sec: 0.0,
            ops_per_sec: 0.0,
            bytes_total: 0.0,
            ops_total: 1.0,
        });
    }

    for (i, c) in client_nodes.into_iter().enumerate() {
        nodes.insert(i, c);
    }

    let clerk_n = nodes.iter().filter(|n| n.kind == "clerk").count();
    let volume_n = nodes.iter().filter(|n| n.kind == "volume").count();
    let s3_n = nodes.iter().filter(|n| n.kind == "s3").count();
    let filer_n = nodes.iter().filter(|n| n.kind == "filer").count();
    // Unified filer also serves S3; keep s3_nodes as gateway count for older UI.
    let gateway_n = if filer_n > 0 { filer_n } else { s3_n };
    let cluster_nodes = clerk_n + volume_n + gateway_n;
    let clerks_ready = nodes
        .iter()
        .filter(|n| n.kind == "clerk" && n.status == "up")
        .count();
    let data_volumes = cluster
        .as_ref()
        .and_then(|c| c.get("volume_count"))
        .and_then(|v| v.as_u64())
        .unwrap_or(0);

    Json(serde_json::json!({
        "generated_at": now_secs(),
        "clerk": clerk_svc,
        "kubernetes": k8s,
        "k8s_pods": k8s_pods.len(),
        "scale": scale_hints(),
        "summary": {
            "cluster_nodes": cluster_nodes,
            "clerk_nodes": clerk_n,
            "clerk_ready": clerks_ready,
            "volume_nodes": volume_n,
            "s3_nodes": gateway_n,
            "filer_nodes": gateway_n,
            "data_volumes": data_volumes,
            "clerk_up": clerk_svc_ok,
            "multi_replica_volumes": multi_location,
        },
        "replication": replication_info,
        "volumes_by_node": nodes
            .iter()
            .filter(|n| n.kind == "volume")
            .map(|n| {
                (
                    n.meta
                        .get("pod")
                        .and_then(|v| v.as_str())
                        .unwrap_or(n.id.as_str())
                        .to_string(),
                    n.meta.get("volumes").cloned().unwrap_or(serde_json::json!([])),
                )
            })
            .collect::<serde_json::Map<String, serde_json::Value>>(),
        "cluster": cluster,
        "nodes": nodes,
        "edges": edges,
    }))
}

fn replication_code_of(vol: &serde_json::Value) -> String {
    if let Some(s) = vol.get("replication").and_then(|v| v.as_str()) {
        return s.to_string();
    }
    if let Some(obj) = vol.get("replication").and_then(|v| v.as_object()) {
        let x = obj
            .get("diff_data_center")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let y = obj.get("diff_rack").and_then(|v| v.as_u64()).unwrap_or(0);
        let z = obj.get("same_rack").and_then(|v| v.as_u64()).unwrap_or(0);
        return format!("{x}{y}{z}");
    }
    "000".into()
}

fn replication_copies(code: &str) -> u64 {
    if code.len() != 3 || !code.bytes().all(|b| b.is_ascii_digit()) {
        return 1;
    }
    let b = code.as_bytes();
    1 + (b[0] - b'0') as u64 + (b[1] - b'0') as u64 + (b[2] - b'0') as u64
}

fn replication_desc(code: &str) -> String {
    match code {
        "000" => "单副本".into(),
        "001" => "同 rack 共 2 份".into(),
        "010" => "跨 rack 共 2 份".into(),
        "100" => "跨 DC 共 2 份".into(),
        "011" => "同 DC 共 3 份（跨 rack + 同 rack）".into(),
        _ => format!("总 {} 份（1+X+Y+Z）", replication_copies(code)),
    }
}

fn scale_hints() -> serde_json::Value {
    let ns = std::env::var("OPENS3_K8S_NAMESPACE").unwrap_or_else(|_| "opens3".into());
    let release = std::env::var("OPENS3_HELM_RELEASE").unwrap_or_else(|_| "opens3".into());
    serde_json::json!({
        "namespace": ns,
        "release": release,
        "resources": {
            "clerk": {"kind": "statefulset", "name": format!("{release}-clerk")},
            "volume": {"kind": "statefulset", "name": format!("{release}-volume")},
            "filer": {"kind": "deployment", "name": format!("{release}-filer")},
            "s3": {"kind": "deployment", "name": format!("{release}-filer")},
        }
    })
}

#[derive(Deserialize)]
pub struct ScaleReq {
    pub component: String,
    pub replicas: u32,
}

pub(crate) async fn admin_ops_scale(Json(req): Json<ScaleReq>) -> Response {
    let hints = scale_hints();
    // Clerk StatefulSet uses emptyDir for Raft; one-click scale/restart causes
    // split-brain (Ready 0/1 Candidate). Volume/S3 scale is safe.
    if req.component == "clerk" {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "不能一键扩缩 Clerk：Raft 元数据在 emptyDir 上，改副本会分叉导致 Ready 0/1。请只扩缩 volume / filer；Clerk 需按运维流程处理成员变更。",
            })),
        )
            .into_response();
    }
    let res = hints["resources"].get(&req.component).cloned();
    let Some(res) = res else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "ok": false,
                "error": "component must be volume|filer|s3",
            })),
        )
            .into_response();
    };
    let kind = res.get("kind").and_then(|v| v.as_str()).unwrap_or("");
    let name = res.get("name").and_then(|v| v.as_str()).unwrap_or("");
    let ns = hints
        .get("namespace")
        .and_then(|v| v.as_str())
        .unwrap_or("opens3");

    let kubectl = format!("kubectl -n {ns} scale {kind}/{name} --replicas={}", req.replicas);
    let helm = format!(
        "helm upgrade {release} deploy/helm/opens3 -n {ns} --reuse-values --set {comp}.replicas={n}",
        release = hints
            .get("release")
            .and_then(|v| v.as_str())
            .unwrap_or("opens3"),
        comp = req.component,
        n = req.replicas,
    );

    let attempt = tokio::task::spawn_blocking({
        let kind = kind.to_string();
        let name = name.to_string();
        let ns = ns.to_string();
        let replicas = req.replicas;
        let kubectl = kubectl.clone();
        move || {
            let out = std::process::Command::new("kubectl")
                .args([
                    "-n",
                    &ns,
                    "scale",
                    &format!("{kind}/{name}"),
                    &format!("--replicas={replicas}"),
                ])
                .output();
            match out {
                Ok(o) if o.status.success() => Ok(String::from_utf8_lossy(&o.stdout).to_string()),
                Ok(o) => Err(format!(
                    "{}{}",
                    String::from_utf8_lossy(&o.stderr),
                    String::from_utf8_lossy(&o.stdout)
                )),
                Err(e) => Err(format!("kubectl not available: {e} ({kubectl})")),
            }
        }
    })
    .await;

    match attempt {
        Ok(Ok(stdout)) => Json(serde_json::json!({
            "ok": true,
            "applied": true,
            "stdout": stdout,
            "kubectl": kubectl,
            "helm": helm,
        }))
        .into_response(),
        Ok(Err(err)) => Json(serde_json::json!({
            "ok": true,
            "applied": false,
            "error": err,
            "kubectl": kubectl,
            "helm": helm,
            "hint": "在有权限的环境执行上述命令，或为 Admin Pod 绑定 scale RBAC",
        }))
        .into_response(),
        Err(e) => Json(serde_json::json!({
            "ok": false,
            "applied": false,
            "error": e.to_string(),
            "kubectl": kubectl,
            "helm": helm,
        }))
        .into_response(),
    }
}

pub(crate) async fn admin_ops_heal(State(st): State<AppState>) -> Response {
    proxy_clerk_post(&st, "/cluster/heal").await
}

pub(crate) async fn admin_ops_assign_policy_get(State(st): State<AppState>) -> Response {
    proxy_clerk_get(&st, "/cluster/assign-policy").await
}

pub(crate) async fn admin_ops_assign_policy_set(
    State(st): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    proxy_clerk_post_json(&st, "/cluster/assign-policy", body).await
}

pub(crate) async fn admin_ops_heal_status(State(st): State<AppState>) -> Response {
    proxy_clerk_get(&st, "/cluster/heal/status").await
}

pub(crate) async fn admin_ops_heal_bandwidth_get(State(st): State<AppState>) -> Response {
    proxy_clerk_get(&st, "/cluster/heal-bandwidth").await
}

pub(crate) async fn admin_ops_heal_bandwidth_set(
    State(st): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    proxy_clerk_post_json(&st, "/cluster/heal-bandwidth", body).await
}

pub(crate) async fn admin_ops_balance(State(st): State<AppState>) -> Response {
    proxy_clerk_post(&st, "/cluster/balance").await
}

#[derive(Deserialize)]
pub struct CreateVolQuery {
    pub volume_url: String,
    pub id: u32,
}

pub(crate) async fn admin_ops_create_volume(Query(q): Query<CreateVolQuery>) -> Response {
    let url = format!(
        "{}/admin/create_volume/{}",
        q.volume_url.trim_end_matches('/'),
        q.id
    );
    let client = reqwest::Client::new();
    match client
        .post(&url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
    {
        Ok(r) => {
            let status =
                StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let body = r.text().await.unwrap_or_default();
            (status, body).into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
pub struct SetReplicationReq {
    pub replication: String,
    #[serde(default)]
    pub volume_ids: Vec<u32>,
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default = "admin_default_true")]
    pub heal: bool,
}

fn admin_default_true() -> bool {
    true
}

pub(crate) async fn admin_ops_set_replication(
    State(st): State<AppState>,
    Json(body): Json<SetReplicationReq>,
) -> Response {
    proxy_clerk_post_json(
        &st,
        "/vol/replication",
        serde_json::json!({
            "replication": body.replication,
            "volume_ids": body.volume_ids,
            "from": body.from,
            "heal": body.heal,
        }),
    )
    .await
}

#[derive(Deserialize)]
pub struct GrowVolReq {
    #[serde(default = "admin_default_grow")]
    pub count: u32,
    #[serde(default)]
    pub collection: String,
    pub replication: Option<String>,
}

fn admin_default_grow() -> u32 {
    1
}

pub(crate) async fn admin_ops_grow_volumes(
    State(st): State<AppState>,
    Json(body): Json<GrowVolReq>,
) -> Response {
    proxy_clerk_post_json(
        &st,
        "/vol/grow",
        serde_json::json!({
            "count": body.count,
            "collection": body.collection,
            "replication": body.replication,
        }),
    )
    .await
}

async fn proxy_clerk_get(st: &AppState, path: &str) -> Response {
    match st.server.blob.clerk_get(path).await {
        Ok(r) => {
            let status =
                StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let body = r.text().await.unwrap_or_default();
            (
                status,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                body,
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn proxy_clerk_post(st: &AppState, path: &str) -> Response {
    proxy_clerk_post_json(st, path, serde_json::json!({})).await
}

async fn proxy_clerk_post_json(st: &AppState, path: &str, body: serde_json::Value) -> Response {
    match st.server.blob.clerk_post_json(path, &body).await {
        Ok(r) => {
            let status =
                StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let text = r.text().await.unwrap_or_default();
            (
                status,
                [(axum::http::header::CONTENT_TYPE, "application/json")],
                text,
            )
                .into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
pub struct DeleteVolQuery {
    pub volume_url: String,
    pub id: u32,
}

/// Delete a data volume on a specific data node (drop local .dat/.idx).
pub(crate) async fn admin_ops_delete_volume(Query(q): Query<DeleteVolQuery>) -> Response {
    let url = format!(
        "{}/admin/drop_volume/{}",
        q.volume_url.trim_end_matches('/'),
        q.id
    );
    let client = reqwest::Client::new();
    match client
        .post(&url)
        .timeout(Duration::from_secs(10))
        .send()
        .await
    {
        Ok(r) => {
            let status =
                StatusCode::from_u16(r.status().as_u16()).unwrap_or(StatusCode::BAD_GATEWAY);
            let body = r.text().await.unwrap_or_default();
            (status, body).into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
pub struct SetVolumeOnlineReq {
    pub volume_id: u32,
    pub online: bool,
}

pub(crate) async fn admin_ops_set_volume_online(
    State(st): State<AppState>,
    Json(body): Json<SetVolumeOnlineReq>,
) -> Response {
    proxy_clerk_post_json(
        &st,
        "/vol/online",
        serde_json::json!({
            "volume_id": body.volume_id,
            "online": body.online,
        }),
    )
    .await
}

#[derive(Deserialize)]
pub struct SetNodeOnlineReq {
    pub node_id: String,
    pub online: bool,
}

pub(crate) async fn admin_ops_set_node_online(
    State(st): State<AppState>,
    Json(body): Json<SetNodeOnlineReq>,
) -> Response {
    proxy_clerk_post_json(
        &st,
        "/cluster/server/online",
        serde_json::json!({
            "node_id": body.node_id,
            "online": body.online,
        }),
    )
    .await
}

fn k8s_api_base() -> Option<(String, String, reqwest::Client)> {
    if !in_kubernetes() {
        return None;
    }
    let token = std::fs::read_to_string("/var/run/secrets/kubernetes.io/serviceaccount/token").ok()?;
    let host =
        std::env::var("KUBERNETES_SERVICE_HOST").unwrap_or_else(|_| "kubernetes.default.svc".into());
    let port = std::env::var("KUBERNETES_SERVICE_PORT").unwrap_or_else(|_| "443".into());
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(15));
    if let Ok(ca) = std::fs::read("/var/run/secrets/kubernetes.io/serviceaccount/ca.crt") {
        if let Ok(cert) = reqwest::Certificate::from_pem(&ca) {
            builder = builder.add_root_certificate(cert);
        }
    } else {
        builder = builder.danger_accept_invalid_certs(true);
    }
    let client = builder.build().ok()?;
    Some((format!("https://{host}:{port}"), token, client))
}

pub(crate) async fn admin_ops_k8s_nodes() -> Response {
    let Some((base, token, client)) = k8s_api_base() else {
        return Json(serde_json::json!({
            "ok": true,
            "nodes": [],
            "hint": "not running in Kubernetes or missing service account",
        }))
        .into_response();
    };
    let pods = list_k8s_pods().await;
    let mut vol_on_node: HashMap<String, u64> = HashMap::new();
    for p in pods.iter().filter(|p| p.component == "volume") {
        if !p.node_name.is_empty() {
            *vol_on_node.entry(p.node_name.clone()).or_default() += 1;
        }
    }
    let url = format!("{base}/api/v1/nodes");
    let Ok(resp) = client.get(&url).bearer_auth(token.trim()).send().await else {
        return (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"ok": false, "error": "list nodes failed"})),
        )
            .into_response();
    };
    if !resp.status().is_success() {
        let t = resp.text().await.unwrap_or_default();
        return (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"ok": false, "error": t})),
        )
            .into_response();
    }
    let Ok(body) = resp.json::<serde_json::Value>().await else {
        return (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"ok": false, "error": "bad nodes json"})),
        )
            .into_response();
    };
    let mut nodes = Vec::new();
    if let Some(items) = body.get("items").and_then(|v| v.as_array()) {
        for item in items {
            let name = item
                .pointer("/metadata/name")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            if name.is_empty() {
                continue;
            }
            let ready = item
                .pointer("/status/conditions")
                .and_then(|v| v.as_array())
                .map(|conds| {
                    conds.iter().any(|c| {
                        c.get("type").and_then(|t| t.as_str()) == Some("Ready")
                            && c.get("status").and_then(|s| s.as_str()) == Some("True")
                    })
                })
                .unwrap_or(false);
            let unschedulable = item
                .pointer("/spec/unschedulable")
                .and_then(|v| v.as_bool())
                .unwrap_or(false);
            nodes.push(serde_json::json!({
                "name": name,
                "ready": ready,
                "unschedulable": unschedulable,
                "data_nodes": vol_on_node.get(&name).copied().unwrap_or(0),
            }));
        }
    }
    nodes.sort_by(|a, b| {
        a.get("name")
            .and_then(|v| v.as_str())
            .cmp(&b.get("name").and_then(|v| v.as_str()))
    });
    Json(serde_json::json!({ "ok": true, "nodes": nodes })).into_response()
}

#[derive(Deserialize)]
pub struct AddDataNodeReq {
    pub kubernetes_node: String,
}

#[derive(Deserialize)]
pub struct PlaceReplicasReq {
    pub source_node_id: String,
    pub target_node_id: String,
    #[serde(default = "admin_default_true")]
    pub heal: bool,
}

#[derive(Deserialize)]
pub struct ClearReplicaOfReq {
    pub node_id: String,
}

/// Mark `target` as replica of `source` and optionally heal volumes onto target.
pub(crate) async fn admin_ops_place_replicas(
    State(st): State<AppState>,
    Json(body): Json<PlaceReplicasReq>,
) -> Response {
    proxy_clerk_post_json(
        &st,
        "/cluster/place-replicas",
        serde_json::json!({
            "source_node_id": body.source_node_id,
            "target_node_id": body.target_node_id,
            "heal": body.heal,
        }),
    )
    .await
}

/// Clear node-level replica_of pairing (does not delete volume copies).
pub(crate) async fn admin_ops_clear_replica_of(
    State(st): State<AppState>,
    Json(body): Json<ClearReplicaOfReq>,
) -> Response {
    proxy_clerk_post_json(
        &st,
        "/cluster/replica-of",
        serde_json::json!({
            "node_id": body.node_id,
            "replica_of": serde_json::Value::Null,
        }),
    )
    .await
}

async fn k8s_patch_node_unschedulable(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    name: &str,
    unschedulable: bool,
) -> Result<(), String> {
    let url = format!("{base}/api/v1/nodes/{name}");
    let body = serde_json::json!({ "spec": { "unschedulable": unschedulable } });
    let resp = client
        .patch(&url)
        .bearer_auth(token.trim())
        .header(
            reqwest::header::CONTENT_TYPE,
            "application/strategic-merge-patch+json",
        )
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if resp.status().is_success() {
        Ok(())
    } else {
        Err(resp.text().await.unwrap_or_else(|_| "patch node failed".into()))
    }
}

async fn k8s_scale_sts(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    ns: &str,
    name: &str,
    replicas: u32,
) -> Result<(), String> {
    let url = format!("{base}/apis/apps/v1/namespaces/{ns}/statefulsets/{name}/scale");
    let body = serde_json::json!({
        "apiVersion": "autoscaling/v1",
        "kind": "Scale",
        "metadata": { "name": name, "namespace": ns },
        "spec": { "replicas": replicas }
    });
    let resp = client
        .put(&url)
        .bearer_auth(token.trim())
        .json(&body)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if resp.status().is_success() {
        Ok(())
    } else {
        Err(resp.text().await.unwrap_or_else(|_| "scale failed".into()))
    }
}

async fn k8s_sts_replicas(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    ns: &str,
    name: &str,
) -> Result<u32, String> {
    let url = format!("{base}/apis/apps/v1/namespaces/{ns}/statefulsets/{name}");
    let resp = client
        .get(&url)
        .bearer_auth(token.trim())
        .send()
        .await
        .map_err(|e| e.to_string())?;
    if !resp.status().is_success() {
        return Err(resp.text().await.unwrap_or_else(|_| "get sts failed".into()));
    }
    let v: serde_json::Value = resp.json().await.map_err(|e| e.to_string())?;
    Ok(v.pointer("/spec/replicas")
        .and_then(|x| x.as_u64())
        .unwrap_or(0) as u32)
}

async fn k8s_pod_node(
    client: &reqwest::Client,
    base: &str,
    token: &str,
    ns: &str,
    pod: &str,
) -> Option<String> {
    let url = format!("{base}/api/v1/namespaces/{ns}/pods/{pod}");
    let resp = client
        .get(&url)
        .bearer_auth(token.trim())
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let v: serde_json::Value = resp.json().await.ok()?;
    v.pointer("/spec/nodeName")
        .and_then(|x| x.as_str())
        .map(|s| s.to_string())
}

/// Add one volume data-node (capacity only). Replica pairing is done via place-replicas on a node.
pub(crate) async fn admin_ops_add_data_node(Json(req): Json<AddDataNodeReq>) -> Response {
    let target = req.kubernetes_node.trim().to_string();
    if target.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"ok": false, "error": "kubernetes_node required"})),
        )
            .into_response();
    }
    let Some((base, token, client)) = k8s_api_base() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"ok": false, "error": "not in Kubernetes"})),
        )
            .into_response();
    };
    let ns = std::env::var("OPENS3_K8S_NAMESPACE").unwrap_or_else(|_| "opens3".into());
    let release = std::env::var("OPENS3_HELM_RELEASE").unwrap_or_else(|_| "opens3".into());
    let sts = format!("{release}-volume");

    let cur = match k8s_sts_replicas(&client, &base, &token, &ns, &sts).await {
        Ok(n) => n,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"ok": false, "error": e})),
            )
                .into_response();
        }
    };
    let next = cur + 1;
    let new_pod = format!("{sts}-{cur}");

    // List ready nodes to cordon (all except target).
    let nodes_resp = admin_ops_k8s_nodes().await;
    // Re-fetch via API for cordon list
    let mut cordoned: Vec<String> = Vec::new();
    let mut cordon_ok = true;
    let url = format!("{base}/api/v1/nodes");
    if let Ok(resp) = client.get(&url).bearer_auth(token.trim()).send().await {
        if let Ok(body) = resp.json::<serde_json::Value>().await {
            if let Some(items) = body.get("items").and_then(|v| v.as_array()) {
                for item in items {
                    let name = item
                        .pointer("/metadata/name")
                        .and_then(|v| v.as_str())
                        .unwrap_or("");
                    if name.is_empty() || name == target {
                        continue;
                    }
                    let already = item
                        .pointer("/spec/unschedulable")
                        .and_then(|v| v.as_bool())
                        .unwrap_or(false);
                    if already {
                        continue;
                    }
                    match k8s_patch_node_unschedulable(&client, &base, &token, name, true).await {
                        Ok(()) => cordoned.push(name.to_string()),
                        Err(_) => {
                            cordon_ok = false;
                            break;
                        }
                    }
                }
            }
        }
    } else {
        cordon_ok = false;
    }
    let _ = nodes_resp;

    let scale_res = k8s_scale_sts(&client, &base, &token, &ns, &sts, next).await;

    // Wait up to ~45s for pod to schedule.
    let mut scheduled_on = None;
    if scale_res.is_ok() {
        for _ in 0..30 {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if let Some(n) = k8s_pod_node(&client, &base, &token, &ns, &new_pod).await {
                scheduled_on = Some(n);
                break;
            }
        }
    }

    // Always uncordon what we cordoned.
    for name in &cordoned {
        let _ = k8s_patch_node_unschedulable(&client, &base, &token, name, false).await;
    }

    match scale_res {
        Ok(()) => Json(serde_json::json!({
            "ok": true,
            "applied": true,
            "pod": new_pod,
            "replicas": next,
            "requested_node": target,
            "scheduled_on": scheduled_on,
            "cordon_used": cordon_ok && !cordoned.is_empty(),
            "matched": scheduled_on.as_deref() == Some(target.as_str()),
        }))
        .into_response(),
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({
                "ok": false,
                "error": e,
                "cordoned_restored": true,
            })),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
pub struct DeleteDataNodeReq {
    /// When set, must be the current highest ordinal pod name (STS shrinks from the end).
    pub pod: Option<String>,
}

pub(crate) async fn admin_ops_delete_data_node(
    State(st): State<AppState>,
    Json(req): Json<DeleteDataNodeReq>,
) -> Response {
    let Some((base, token, client)) = k8s_api_base() else {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"ok": false, "error": "not in Kubernetes"})),
        )
            .into_response();
    };
    let ns = std::env::var("OPENS3_K8S_NAMESPACE").unwrap_or_else(|_| "opens3".into());
    let release = std::env::var("OPENS3_HELM_RELEASE").unwrap_or_else(|_| "opens3".into());
    let sts = format!("{release}-volume");
    let cur = match k8s_sts_replicas(&client, &base, &token, &ns, &sts).await {
        Ok(n) => n,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(serde_json::json!({"ok": false, "error": e})),
            )
                .into_response();
        }
    };
    if cur == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"ok": false, "error": "no data nodes"})),
        )
            .into_response();
    }
    let last_pod = format!("{sts}-{}", cur - 1);
    if let Some(p) = req.pod.as_deref() {
        if p != last_pod {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "ok": false,
                    "error": format!("StatefulSet 只能从末尾删除，请先删除末尾节点 {last_pod}"),
                    "last_pod": last_pod,
                })),
            )
                .into_response();
        }
    }
    // Must be offline on clerk before scale-in.
    if let Ok(resp) = st.server.blob.clerk_get("/cluster/status").await {
        if let Ok(v) = resp.json::<serde_json::Value>().await {
            if let Some(servers) = v.get("servers").and_then(|x| x.as_array()) {
                let online = servers
                    .iter()
                    .find(|s| s.get("node_id").and_then(|x| x.as_str()) == Some(last_pod.as_str()))
                    .and_then(|s| s.get("online").and_then(|x| x.as_bool()))
                    .unwrap_or(false);
                if online {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(serde_json::json!({
                            "ok": false,
                            "error": format!("{last_pod} 仍在线，请先下线后再删除"),
                            "node_id": last_pod,
                        })),
                    )
                        .into_response();
                }
            }
        }
    }
    let node = k8s_pod_node(&client, &base, &token, &ns, &last_pod).await;
    match k8s_scale_sts(&client, &base, &token, &ns, &sts, cur - 1).await {
        Ok(()) => {
            // Drop from clerk catalog so Admin topology does not keep a ghost.
            let _ = st
                .server
                .blob
                .clerk_post_json(
                    "/cluster/server/remove",
                    &serde_json::json!({ "node_id": last_pod }),
                )
                .await;
            Json(serde_json::json!({
                "ok": true,
                "applied": true,
                "removed_pod": last_pod,
                "kubernetes_node": node,
                "replicas": cur - 1,
            }))
            .into_response()
        }
        Err(e) => (
            StatusCode::BAD_GATEWAY,
            Json(serde_json::json!({"ok": false, "error": e})),
        )
            .into_response(),
    }
}

const TOPO_LAYOUT_NS: &str = "admin";
const TOPO_LAYOUT_KEY: &str = "topo_layout_v2";

/// GET saved Admin topology node positions (shared cluster-wide via Filer KV).
pub(crate) async fn admin_topo_layout_get(State(st): State<AppState>) -> Response {
    match st.server.store.kv_get(TOPO_LAYOUT_NS, TOPO_LAYOUT_KEY).await {
        Ok(Some(bytes)) => match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(v) => Json(v).into_response(),
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": e.to_string()})),
            )
                .into_response(),
        },
        Ok(None) => Json(serde_json::json!({ "positions": {} })).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"ok": false, "error": e.to_string()})),
        )
            .into_response(),
    }
}

/// PUT manual topology layout. Body: `{ "positions": { "node-id": { "x": 1, "y": 2 }, ... } }`.
pub(crate) async fn admin_topo_layout_put(
    State(st): State<AppState>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let positions = body
        .get("positions")
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    if !positions.is_object() {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({"ok": false, "error": "positions must be an object"})),
        )
            .into_response();
    }
    let payload = serde_json::json!({
        "positions": positions,
        "updated_at": chrono::Utc::now().to_rfc3339(),
    });
    let bytes = match serde_json::to_vec(&payload) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(serde_json::json!({"ok": false, "error": e.to_string()})),
            )
                .into_response();
        }
    };
    match st
        .server
                .store
        .kv_put(TOPO_LAYOUT_NS, TOPO_LAYOUT_KEY, &bytes)
        .await
    {
        Ok(()) => Json(serde_json::json!({"ok": true})).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"ok": false, "error": e.to_string()})),
        )
            .into_response(),
    }
}

/// DELETE clears the shared topology layout (Admin 「复位布局」).
pub(crate) async fn admin_topo_layout_delete(State(st): State<AppState>) -> Response {
    let empty = serde_json::json!({ "positions": {} });
    let bytes = serde_json::to_vec(&empty).unwrap_or_else(|_| b"{\"positions\":{}}".to_vec());
    match st
        .server
                .store
        .kv_put(TOPO_LAYOUT_NS, TOPO_LAYOUT_KEY, &bytes)
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({"ok": false, "error": e.to_string()})),
        )
            .into_response(),
    }
}
