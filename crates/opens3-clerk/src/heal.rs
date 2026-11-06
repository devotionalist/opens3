// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Replica healing and volume balance orchestration (master side).

use crate::cluster::{ClusterState, VolumeFingerprint};
use opens3_common::types::VolumeState;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

#[derive(Debug, Clone, Deserialize)]
struct VolumeFpResp {
    #[serde(default)]
    size: u64,
    #[serde(default)]
    file_count: u64,
    #[serde(default)]
    deleted_count: u64,
}

impl VolumeFpResp {
    fn fp(&self) -> VolumeFingerprint {
        VolumeFingerprint::new(self.size, self.file_count, self.deleted_count)
    }
}

async fn fetch_volume_fp(
    http: &reqwest::Client,
    base: &str,
    volume_id: u32,
) -> Option<VolumeFingerprint> {
    let url = format!(
        "{}/admin/volume_fp/{}",
        base.trim_end_matches('/'),
        volume_id
    );
    match http.get(&url).send().await {
        Ok(resp) if resp.status().is_success() => match resp.json::<VolumeFpResp>().await {
            Ok(body) => Some(body.fp()),
            Err(e) => {
                warn!("volume_fp parse {url}: {e}");
                None
            }
        },
        Ok(resp) => {
            warn!("volume_fp {url}: {}", resp.status());
            None
        }
        Err(e) => {
            warn!("volume_fp {url}: {e}");
            None
        }
    }
}

/// Max whole-volume catch-up rounds per volume while peers keep accepting writes.
const RECONCILE_CATCHUP_ROUNDS: usize = 5;

/// After a volume node restart: compare fingerprints with peer replicas and
/// whole-volume push from the largest replica onto the restarted node.
///
/// **Degraded writes**: the restarted node stays `write_ready=false` (out of
/// assign / sync fan-out) while catch-up runs; surviving replicas keep serving
/// writes so a large delta does not stall the cluster. When catch-up finishes
/// (or gives up), the node rejoins as `write_ready`.
pub async fn reconcile_node_fingerprints(
    http: &reqwest::Client,
    cluster: &ClusterState,
    node_id: &str,
    max_bps: u64,
) {
    let volumes = cluster.multi_copy_volumes_on(node_id);
    if volumes.is_empty() {
        cluster.set_write_ready(node_id, true);
        info!(node = node_id, "fingerprint reconcile: no multi-copy volumes; write_ready");
        return;
    }

    let servers = cluster.servers();
    let self_url = servers
        .iter()
        .find(|s| s.node_id == node_id)
        .map(|s| s.location.url.clone());
    let Some(self_url) = self_url else {
        warn!(node = node_id, "fingerprint reconcile: node url missing");
        cluster.set_write_ready(node_id, true);
        return;
    };

    let mut all_ok = true;
    for v in &volumes {
        let mut peer_samples: Vec<(String, String, VolumeFingerprint)> = Vec::new();
        for loc in &v.locations {
            if loc.node_id == node_id {
                continue;
            }
            let url = servers
                .iter()
                .find(|s| s.node_id == loc.node_id)
                .map(|s| s.location.url.clone())
                .unwrap_or_else(|| loc.url.clone());
            if let Some(fp) = fetch_volume_fp(http, &url, v.id).await {
                peer_samples.push((loc.node_id.clone(), url, fp));
            } else {
                warn!(
                    volume = v.id,
                    node = %loc.node_id,
                    "fingerprint reconcile: missing peer fp"
                );
                all_ok = false;
            }
        }
        let Some(mine0) = fetch_volume_fp(http, &self_url, v.id).await else {
            warn!(volume = v.id, "fingerprint reconcile: self fp missing");
            all_ok = false;
            continue;
        };
        if peer_samples.is_empty() {
            all_ok = false;
            continue;
        }

        // Source = largest peer (survivors kept writing during our downtime).
        peer_samples.sort_by(|a, b| {
            b.2.size
                .cmp(&a.2.size)
                .then(b.2.file_count.cmp(&a.2.file_count))
                .then(b.2.deleted_count.cmp(&a.2.deleted_count))
        });
        let (src_node, src_url, src_fp) = peer_samples[0].clone();
        if mine0.key() == src_fp.key() {
            info!(volume = v.id, "fingerprint reconcile: already matches peer");
            continue;
        }

        info!(
            volume = v.id,
            source = %src_node,
            self_fp = %mine0.key(),
            src_fp = %src_fp.key(),
            "fingerprint divergence; background catch-up (cluster keeps writing on peers)"
        );

        let mut synced = false;
        for round in 1..=RECONCILE_CATCHUP_ROUNDS {
            let url = replicate_volume_url(&src_url, v.id, &self_url, max_bps);
            match http.post(&url).send().await {
                Ok(resp) if resp.status().is_success() => {}
                Ok(resp) => {
                    warn!(
                        volume = v.id,
                        round,
                        "fingerprint heal failed: {}",
                        resp.status()
                    );
                    all_ok = false;
                    break;
                }
                Err(e) => {
                    warn!(volume = v.id, round, "fingerprint heal error: {e}");
                    all_ok = false;
                    break;
                }
            }
            // Peer may have grown while we copied — refresh and retry if still behind.
            let src_now = fetch_volume_fp(http, &src_url, v.id).await;
            let mine_now = fetch_volume_fp(http, &self_url, v.id).await;
            match (src_now, mine_now) {
                (Some(s), Some(m)) if s.key() == m.key() => {
                    info!(volume = v.id, round, "fingerprint catch-up matched");
                    synced = true;
                    break;
                }
                (Some(s), Some(m)) => {
                    info!(
                        volume = v.id,
                        round,
                        self_fp = %m.key(),
                        src_fp = %s.key(),
                        "peer grew during copy; another catch-up round"
                    );
                }
                _ => {
                    all_ok = false;
                    break;
                }
            }
        }
        if !synced {
            warn!(
                volume = v.id,
                "fingerprint catch-up exhausted rounds; rejoining anyway (may still lag)"
            );
            all_ok = false;
        }
    }

    // Rejoin write path. Cluster was never fully stopped — only this node was excluded.
    cluster.set_write_ready(node_id, true);
    if all_ok {
        info!(node = node_id, "fingerprint reconcile complete; write_ready");
    } else {
        warn!(
            node = node_id,
            "fingerprint reconcile finished with gaps; write_ready (rely on next restart/heal)"
        );
    }
}

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealTask {
    // 修复任务
    pub volume_id: u32,
    pub missing_urls: Vec<String>, // 缺失URL
    pub source_url: String,        // 源URL
    #[serde(default)]
    pub source_node: String,
    #[serde(default)]
    pub bytes_total: u64,
    pub status: String, // planned | queued | running | ok | failed
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealTaskView {
    pub volume_id: u32,
    pub source_url: String,
    pub source_node: String,
    pub targets: Vec<String>,
    pub status: String,
    pub progress_pct: f64,
    pub bytes_total: u64,
    pub bytes_copied: u64,
    pub current_target: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealStatus {
    pub running: bool,
    pub run_id: u64,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub current_volume_id: Option<u32>,
    pub current_source_node: Option<String>,
    pub current_source_url: Option<String>,
    pub current_target: Option<String>,
    pub tasks_total: usize,
    pub tasks_done: usize,
    pub overall_progress_pct: f64,
    pub tasks: Vec<HealTaskView>,
}

impl Default for HealStatus {
    fn default() -> Self {
        Self {
            running: false,
            run_id: 0,
            started_at: 0,
            finished_at: None,
            current_volume_id: None,
            current_source_node: None,
            current_source_url: None,
            current_target: None,
            tasks_total: 0,
            tasks_done: 0,
            overall_progress_pct: 0.0,
            tasks: Vec::new(),
        }
    }
}

/// In-memory heal job tracker (leader process).
#[derive(Clone, Default)]
pub struct HealTracker {
    inner: Arc<RwLock<HealStatus>>,
    next_run: Arc<AtomicU64>,
}

impl HealTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> HealStatus {
        self.inner.read().clone()
    }

    pub fn try_begin(&self, tasks: &[HealTask]) -> Result<u64, HealStatus> {
        let mut g = self.inner.write();
        if g.running {
            return Err(g.clone());
        }
        let run_id = self.next_run.fetch_add(1, Ordering::Relaxed) + 1;
        let views: Vec<HealTaskView> = tasks
            .iter()
            .map(|t| HealTaskView {
                volume_id: t.volume_id,
                source_url: t.source_url.clone(),
                source_node: t.source_node.clone(),
                targets: t.missing_urls.clone(),
                status: "queued".into(),
                progress_pct: 0.0,
                bytes_total: t.bytes_total,
                bytes_copied: 0,
                current_target: None,
                message: String::new(),
            })
            .collect();
        *g = HealStatus {
            running: true,
            run_id,
            started_at: now_secs(),
            finished_at: None,
            current_volume_id: None,
            current_source_node: None,
            current_source_url: None,
            current_target: None,
            tasks_total: views.len(),
            tasks_done: 0,
            overall_progress_pct: if views.is_empty() { 100.0 } else { 0.0 },
            tasks: views,
        };
        Ok(run_id)
    }

    fn recompute(g: &mut HealStatus) {
        let n = g.tasks.len().max(1) as f64;
        let sum: f64 = g.tasks.iter().map(|t| t.progress_pct).sum();
        g.overall_progress_pct = (sum / n).clamp(0.0, 100.0);
        g.tasks_done = g
            .tasks
            .iter()
            .filter(|t| t.status == "ok" || t.status == "failed")
            .count();
    }

    pub fn set_task_running(&self, idx: usize, target: Option<&str>) {
        let mut g = self.inner.write();
        let cur = g.tasks.get_mut(idx).map(|t| {
            t.status = "running".into();
            t.current_target = target.map(|s| s.to_string());
            t.message = target
                .map(|u| format!("copying → {u}"))
                .unwrap_or_else(|| "running".into());
            (
                t.volume_id,
                t.source_node.clone(),
                t.source_url.clone(),
                t.current_target.clone(),
            )
        });
        if let Some((vid, node, url, tgt)) = cur {
            g.current_volume_id = Some(vid);
            g.current_source_node = Some(node);
            g.current_source_url = Some(url);
            g.current_target = tgt;
        }
        Self::recompute(&mut g);
    }

    pub fn set_task_target_progress(&self, idx: usize, done_targets: usize, total_targets: usize) {
        let mut g = self.inner.write();
        if let Some(t) = g.tasks.get_mut(idx) {
            let total = total_targets.max(1) as f64;
            t.progress_pct = ((done_targets as f64) / total * 100.0).clamp(0.0, 99.0);
            if t.bytes_total > 0 && total_targets > 0 {
                t.bytes_copied = (t.bytes_total as f64 * (done_targets as f64) / total) as u64;
            }
        }
        Self::recompute(&mut g);
    }

    pub fn finish_task(&self, idx: usize, ok: bool, message: impl Into<String>) {
        let mut g = self.inner.write();
        if let Some(t) = g.tasks.get_mut(idx) {
            t.status = if ok { "ok" } else { "failed" }.into();
            t.progress_pct = if ok { 100.0 } else { t.progress_pct };
            if ok {
                t.bytes_copied = t.bytes_total;
            }
            t.current_target = None;
            t.message = message.into();
        }
        Self::recompute(&mut g);
    }

    pub fn finish_run(&self) {
        let mut g = self.inner.write();
        g.running = false;
        g.finished_at = Some(now_secs());
        g.current_volume_id = None;
        g.current_source_node = None;
        g.current_source_url = None;
        g.current_target = None;
        if g.tasks.is_empty() {
            g.overall_progress_pct = 100.0;
        } else {
            Self::recompute(&mut g);
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalancePlan {
    // 平衡计划
    pub moves: Vec<BalanceMove>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceMove {
    // 平衡移动
    pub volume_id: u32, // 卷ID
    pub from: String,   // 源URL
    pub to: String,     // 目标URL
}

pub fn plan_heal(cluster: &ClusterState) -> Vec<HealTask> {
    // 计划修复
    plan_heal_filtered(cluster, None, None) // 计划修复过滤
}

/// When `only_ids` is Some, only those volume ids are considered.
/// When `prefer_node_ids` is Some, missing replicas are placed only on those nodes
/// (in listed order) before falling back to any other free server.
pub fn plan_heal_filtered(
    // 计划修复过滤
    cluster: &ClusterState,            // 集群
    only_ids: Option<&[u32]>,          // 只考虑的卷ID
    prefer_node_ids: Option<&[String]>, // 优先考虑的节点ID
) -> Vec<HealTask> {
    // 计划修复过滤
    let mut tasks = Vec::new(); // 任务列表
    for v in cluster.list_volumes("") {
        if let Some(ids) = only_ids {
            if !ids.contains(&v.id) {
                // 如果卷ID不在只考虑的卷ID列表中
                continue;
            }
        }
        if matches!(v.state, VolumeState::EcEncoded) {
            continue;
        }
        let expected = v.replication.total_copies() as usize;
        if v.locations.len() >= expected {
            continue;
        }
        let Some(source) = v.locations.first().cloned() else {
            continue;
        };
        let holders: std::collections::HashSet<_> =
            v.locations.iter().map(|l| l.node_id.clone()).collect();
        let need = expected.saturating_sub(v.locations.len());
        let mut missing = Vec::new();
        let servers = cluster.servers();
        // Always use live server URLs (volume.locations.url can be stale after Pod restart).
        let live_url = |node_id: &str, fallback: &str| -> String {
            servers
                .iter()
                .find(|s| s.node_id == node_id)
                .map(|s| s.location.url.clone())
                .unwrap_or_else(|| fallback.to_string())
        };
        let source_url = live_url(&source.node_id, &source.url);
        // Prefer explicit targets first (e.g. newly added replica node).
        let pinned = prefer_node_ids.map(|p| !p.is_empty()).unwrap_or(false);
        if let Some(prefs) = prefer_node_ids {
            for pid in prefs {
                if missing.len() >= need {
                    break;
                }
                if holders.contains(pid) {
                    continue;
                }
                if let Some(s) = servers.iter().find(|s| &s.node_id == pid) {
                    missing.push(s.location.url.clone());
                }
            }
        }
        if !pinned {
            for s in &servers {
                if missing.len() >= need {
                    break;
                }
                if holders.contains(&s.node_id) {
                    continue;
                }
                if missing.iter().any(|u| u == &s.location.url) {
                    continue;
                }
                missing.push(s.location.url.clone());
            }
        }
        if missing.is_empty() {
            continue;
        }
        tasks.push(HealTask {
            volume_id: v.id,
            missing_urls: missing,
            source_url,
            source_node: source.node_id,
            bytes_total: v.size,
            status: "planned".into(),
        });
    }
    tasks
}

fn replicate_volume_url(source: &str, volume_id: u32, target: &str, max_bps: u64) -> String {
    let base = source.trim_end_matches('/');
    if max_bps > 0 {
        format!("{base}/admin/replicate_volume/{volume_id}?targets={target}&max_bps={max_bps}")
    } else {
        format!("{base}/admin/replicate_volume/{volume_id}?targets={target}")
    }
}

/// Run heal tasks while updating `tracker` (task list + current volume + progress).
/// `max_bps` is forwarded to volume `replicate_volume` (`0` = unlimited).
pub async fn execute_heal(
    http: &reqwest::Client,
    tasks: &[HealTask],
    tracker: Option<&HealTracker>,
    max_bps: u64,
) -> Vec<HealTask> {
    let mut out = Vec::new();
    for (idx, t) in tasks.iter().enumerate() {
        let mut t = t.clone();
        let targets = t.missing_urls.clone();
        let n_targets = targets.len().max(1);
        if let Some(tr) = tracker {
            tr.set_task_running(idx, targets.first().map(|s| s.as_str()));
        }
        let mut ok_all = true;
        let mut last_err = String::new();
        for (ti, target) in targets.iter().enumerate() {
            if let Some(tr) = tracker {
                tr.set_task_running(idx, Some(target.as_str()));
            }
            let url = replicate_volume_url(&t.source_url, t.volume_id, target, max_bps);
            match http.post(&url).send().await {
                Ok(resp) if resp.status().is_success() => {
                    if let Some(tr) = tracker {
                        tr.set_task_target_progress(idx, ti + 1, n_targets);
                    }
                }
                Ok(resp) => {
                    ok_all = false;
                    last_err = format!("failed: {}", resp.status());
                    warn!("heal volume {} -> {target}: {last_err}", t.volume_id);
                    break;
                }
                Err(e) => {
                    ok_all = false;
                    last_err = format!("error: {e}");
                    warn!("heal volume {} -> {target}: {last_err}", t.volume_id);
                    break;
                }
            }
        }
        if ok_all {
            t.status = "ok".into();
            info!("healed volume {} -> {:?}", t.volume_id, t.missing_urls);
            if let Some(tr) = tracker {
                tr.finish_task(idx, true, format!("replicated to {} target(s)", n_targets));
            }
        } else {
            t.status = last_err.clone();
            if let Some(tr) = tracker {
                tr.finish_task(idx, false, last_err);
            }
        }
        out.push(t);
    }
    if let Some(tr) = tracker {
        tr.finish_run();
    }
    out
}

//
// 计划平衡
pub fn plan_balance(cluster: &ClusterState) -> BalancePlan {
    // 计划平衡
    let servers = cluster.servers();
    if servers.len() < 2 {
        return BalancePlan { moves: vec![] };
    }
    // count volumes per node
    let mut counts: HashMap<String, usize> = HashMap::new();
    for s in &servers {
        counts.insert(s.node_id.clone(), 0);
    }
    let volumes = cluster.list_volumes("");
    for v in &volumes {
        for loc in &v.locations {
            *counts.entry(loc.node_id.clone()).or_default() += 1;
        }
    }
    let avg = (volumes.len().max(1)) / servers.len().max(1);
    let mut overloaded: Vec<_> = counts
        .iter()
        .filter(|(_, c)| **c > avg + 1)
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    overloaded.sort_by(|a, b| b.1.cmp(&a.1));
    let mut underloaded: Vec<_> = counts
        .iter()
        .filter(|(_, c)| **c < avg)
        .map(|(k, v)| (k.clone(), *v))
        .collect();
    underloaded.sort_by(|a, b| a.1.cmp(&b.1));

    let mut moves = Vec::new();
    for (from_id, _) in &overloaded {
        let Some(to) = underloaded.first() else {
            break;
        };
        let to_id = to.0.clone();
        // pick a writable volume primarily on from
        if let Some(v) = volumes.iter().find(|v| {
            v.locations
                .first()
                .map(|l| &l.node_id == from_id)
                .unwrap_or(false)
                && matches!(v.state, VolumeState::Writable | VolumeState::ReadOnly)
        }) {
            let from_url = v.locations[0].url.clone();
            let to_url = servers
                .iter()
                .find(|s| s.node_id == to_id)
                .map(|s| s.location.url.clone())
                .unwrap_or_default();
            if !to_url.is_empty() {
                moves.push(BalanceMove {
                    volume_id: v.id,
                    from: from_url,
                    to: to_url,
                });
                if let Some(u) = underloaded.first_mut() {
                    u.1 += 1;
                }
            }
        }
    }
    BalancePlan { moves }
}

//
// 执行平衡
pub async fn execute_balance(
    http: &reqwest::Client,
    plan: &BalancePlan,
    max_bps: u64,
) -> BalancePlan {
    let mut done = Vec::new();
    for m in &plan.moves {
        let url = replicate_volume_url(&m.from, m.volume_id, &m.to, max_bps);
        match http.post(&url).send().await {
            Ok(resp) if resp.status().is_success() => {
                // true move: drop source after successful replicate
                let drop_url = format!(
                    "{}/admin/drop_volume/{}",
                    m.from.trim_end_matches('/'),
                    m.volume_id
                );
                match http.post(&drop_url).send().await {
                    Ok(r) if r.status().is_success() => {
                        info!(
                            "balanced(move) volume {} {} -> {}",
                            m.volume_id, m.from, m.to
                        );
                        done.push(m.clone());
                    }
                    Ok(r) => warn!(
                        "balance {} copied but drop source failed: {}",
                        m.volume_id,
                        r.status()
                    ),
                    Err(e) => warn!("balance {} drop source error: {e}", m.volume_id),
                }
            }
            Ok(resp) => warn!("balance {} failed: {}", m.volume_id, resp.status()),
            Err(e) => warn!("balance {} error: {e}", m.volume_id),
        }
    }
    BalancePlan { moves: done }
}
