// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 副本修复与卷均衡编排（Master 侧）。

use crate::cluster::{ClusterState, VolumeFingerprint};
use opens3_common::types::VolumeState;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

/// Volume 节点 `/admin/volume_fp` 响应体。
#[derive(Debug, Clone, Deserialize)]
struct VolumeFpResp {
    #[serde(default)]
    size: u64,          // 卷字节大小
    #[serde(default)]
    file_count: u64,    // 存活文件数
    #[serde(default)]
    deleted_count: u64, // 软删文件数
}

impl VolumeFpResp {
    /// 转为集群侧指纹。
    fn fp(&self) -> VolumeFingerprint {
        VolumeFingerprint::new(self.size, self.file_count, self.deleted_count)
    }
}

/// 拉取某副本上指定卷的内容指纹；失败返回 None。
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

/// 对端持续写入时，单卷整卷追赶的最大轮数。
const RECONCILE_CATCHUP_ROUNDS: usize = 5;

/// Volume 节点重启后：与 peer 副本比对指纹，并从最大副本整卷推送到重启节点。
///
/// **降级写**：追赶期间重启节点保持 `write_ready=false`（不参与 assign / 同步扇出）；
/// 存活副本继续承接写入，避免大 diff 卡住集群。追赶结束（或放弃）后重新 `write_ready`。
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

        // 源 = 最大 peer（宕机期间存活副本持续写入）。
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
            // 拷贝期间 peer 可能又增长 — 刷新指纹，落后则再追一轮。
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

    // 重新加入写路径。集群从未完全停写 — 仅本节点被排除。
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

/// Unix 秒时间戳。
fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// 单条副本修复任务（计划 / 执行单元）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealTask {
    pub volume_id: u32,            // 待修复卷
    pub missing_urls: Vec<String>, // 缺失副本目标 URL
    pub source_url: String,        // 源副本 URL
    #[serde(default)]
    pub source_node: String,       // 源节点 id
    #[serde(default)]
    pub bytes_total: u64,          // 预估总字节（进度用）
    pub status: String,            // planned | queued | running | ok | failed
}

/// 对外展示的 heal 任务视图（含进度）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealTaskView {
    pub volume_id: u32,
    pub source_url: String,
    pub source_node: String,
    pub targets: Vec<String>,           // 目标 URL 列表
    pub status: String,                 // queued | running | ok | failed
    pub progress_pct: f64,              // 0..=100
    pub bytes_total: u64,
    pub bytes_copied: u64,              // 按目标完成比例估算
    pub current_target: Option<String>, // 当前正在拷贝的目标
    pub message: String,                // 状态说明
}

/// 一次 heal 运行的全局状态快照。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealStatus {
    pub running: bool,                           // 是否进行中
    pub run_id: u64,                             // 运行序号
    pub started_at: u64,                         // 开始 Unix 秒
    pub finished_at: Option<u64>,                // 结束 Unix 秒
    pub current_volume_id: Option<u32>,          // 当前卷
    pub current_source_node: Option<String>,
    pub current_source_url: Option<String>,
    pub current_target: Option<String>,
    pub tasks_total: usize,
    pub tasks_done: usize,                       // ok|failed 计数
    pub overall_progress_pct: f64,
    pub tasks: Vec<HealTaskView>,
}

impl Default for HealStatus {
    /// 空闲、无任务的默认状态。
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

/// 内存中的 heal 任务跟踪器（Leader 进程内）。
#[derive(Clone, Default)]
pub struct HealTracker {
    inner: Arc<RwLock<HealStatus>>,
    next_run: Arc<AtomicU64>, // 分配 run_id
}

impl HealTracker {
    /// 新建空跟踪器。
    pub fn new() -> Self {
        Self::default()
    }

    /// 当前状态快照。
    pub fn snapshot(&self) -> HealStatus {
        self.inner.read().clone()
    }

    /// 若空闲则开始新一轮；忙则返回当前状态 Err。
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

    /// 按各任务进度重算整体进度与完成数。
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

    /// 标记任务为 running，并更新当前目标。
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

    /// 按已完成目标数更新任务进度（封顶 99%，完成态由 finish_task 置 100）。
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

    /// 结束单个任务（ok / failed）。
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

    /// 结束整轮 heal 运行。
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

/// 卷均衡计划（若干次搬迁）。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalancePlan {
    pub moves: Vec<BalanceMove>,
}

/// 单次卷搬迁：从 from 复制到 to 后可 drop 源。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BalanceMove {
    pub volume_id: u32, // 卷 ID
    pub from: String,   // 源 URL
    pub to: String,     // 目标 URL
}

/// 规划全量欠副本 heal 任务。
pub fn plan_heal(cluster: &ClusterState) -> Vec<HealTask> {
    plan_heal_filtered(cluster, None, None)
}

/// 规划 heal 任务。
/// `only_ids` 非空时只考虑这些卷；`prefer_node_ids` 非空时优先把缺失副本放到所列节点
///（按顺序），且不再回落到其它空闲节点（pinned）。
pub fn plan_heal_filtered(
    cluster: &ClusterState,
    only_ids: Option<&[u32]>,
    prefer_node_ids: Option<&[String]>,
) -> Vec<HealTask> {
    let mut tasks = Vec::new();
    for v in cluster.list_volumes("") {
        if let Some(ids) = only_ids {
            if !ids.contains(&v.id) {
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
        // 始终用存活 server URL（Pod 重启后 volume.locations.url 可能过期）。
        let live_url = |node_id: &str, fallback: &str| -> String {
            servers
                .iter()
                .find(|s| s.node_id == node_id)
                .map(|s| s.location.url.clone())
                .unwrap_or_else(|| fallback.to_string())
        };
        let source_url = live_url(&source.node_id, &source.url);
        // 优先显式目标（如新加的 replica 节点）。
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

/// 构造 volume 侧 `replicate_volume` 请求 URL（可选限速）。
fn replicate_volume_url(source: &str, volume_id: u32, target: &str, max_bps: u64) -> String {
    let base = source.trim_end_matches('/');
    if max_bps > 0 {
        format!("{base}/admin/replicate_volume/{volume_id}?targets={target}&max_bps={max_bps}")
    } else {
        format!("{base}/admin/replicate_volume/{volume_id}?targets={target}")
    }
}

/// 执行 heal 任务并更新 `tracker`（任务列表 / 当前卷 / 进度）。
/// `max_bps` 转发到 volume `replicate_volume`（`0` = 不限速）。
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

/// 按每节点卷数相对均值规划搬迁（过载 -> 欠载）。
pub fn plan_balance(cluster: &ClusterState) -> BalancePlan {
    let servers = cluster.servers();
    if servers.len() < 2 {
        return BalancePlan { moves: vec![] };
    }
    // 统计每节点承载的卷副本数
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
        // 选一个主位置在 from 上的可读写卷
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

/// 执行均衡：复制成功后再 drop 源端卷。
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
                // 真搬迁：复制成功后再删源
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
