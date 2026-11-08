// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Clerk-side vacuum orchestration (SeaweedFS-style Check→Compact→Commit→Cleanup).

use crate::cluster::ClusterState;
use opens3_common::types::VolumeState;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{info, warn};

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Runtime knobs (env overrides at Clerk start).
#[derive(Debug, Clone)]
pub struct VacuumConfig {
    pub enabled: bool,
    pub interval_secs: u64,
    pub garbage_threshold: f64,
    pub max_parallel_volumes: usize,
}

impl Default for VacuumConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            interval_secs: 60,
            garbage_threshold: 0.3,
            max_parallel_volumes: 1,
        }
    }
}

impl VacuumConfig {
    pub fn from_env() -> Self {
        let mut c = Self::default();
        if let Ok(v) = std::env::var("OPENS3_VACUUM_ENABLED") {
            c.enabled = matches!(
                v.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            );
        }
        if let Ok(v) = std::env::var("OPENS3_VACUUM_INTERVAL_SECS") {
            if let Ok(n) = v.parse::<u64>() {
                c.interval_secs = n.max(5);
            }
        }
        if let Ok(v) = std::env::var("OPENS3_VACUUM_GARBAGE_THRESHOLD") {
            if let Ok(n) = v.parse::<f64>() {
                if (0.0..=1.0).contains(&n) {
                    c.garbage_threshold = n;
                }
            }
        }
        if let Ok(v) = std::env::var("OPENS3_VACUUM_MAX_PARALLEL") {
            if let Ok(n) = v.parse::<usize>() {
                c.max_parallel_volumes = n.max(1);
            }
        }
        c
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VacuumTask {
    pub volume_id: u32,
    pub locations: Vec<String>,
    pub estimated_garbage_ratio: f64,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VacuumTaskView {
    pub volume_id: u32,
    pub locations: Vec<String>,
    pub estimated_garbage_ratio: f64,
    pub status: String,
    pub progress_pct: f64,
    pub current_url: Option<String>,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VacuumStatus {
    pub running: bool,
    pub run_id: u64,
    pub started_at: u64,
    pub finished_at: Option<u64>,
    pub current_volume_id: Option<u32>,
    pub current_url: Option<String>,
    pub tasks_total: usize,
    pub tasks_done: usize,
    pub overall_progress_pct: f64,
    pub threshold: f64,
    pub tasks: Vec<VacuumTaskView>,
}

impl Default for VacuumStatus {
    fn default() -> Self {
        Self {
            running: false,
            run_id: 0,
            started_at: 0,
            finished_at: None,
            current_volume_id: None,
            current_url: None,
            tasks_total: 0,
            tasks_done: 0,
            overall_progress_pct: 0.0,
            threshold: 0.3,
            tasks: Vec::new(),
        }
    }
}

#[derive(Clone, Default)]
pub struct VacuumTracker {
    inner: Arc<RwLock<VacuumStatus>>,
    next_run: Arc<AtomicU64>,
}

impl VacuumTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn snapshot(&self) -> VacuumStatus {
        self.inner.read().clone()
    }

    pub fn try_begin(&self, tasks: &[VacuumTask], threshold: f64) -> Result<u64, VacuumStatus> {
        let mut g = self.inner.write();
        if g.running {
            return Err(g.clone());
        }
        let run_id = self.next_run.fetch_add(1, Ordering::Relaxed) + 1;
        let views: Vec<VacuumTaskView> = tasks
            .iter()
            .map(|t| VacuumTaskView {
                volume_id: t.volume_id,
                locations: t.locations.clone(),
                estimated_garbage_ratio: t.estimated_garbage_ratio,
                status: "queued".into(),
                progress_pct: 0.0,
                current_url: None,
                message: String::new(),
            })
            .collect();
        *g = VacuumStatus {
            running: true,
            run_id,
            started_at: now_secs(),
            finished_at: None,
            current_volume_id: None,
            current_url: None,
            tasks_total: views.len(),
            tasks_done: 0,
            overall_progress_pct: 0.0,
            threshold,
            tasks: views,
        };
        Ok(run_id)
    }

    fn set_task_running(&self, idx: usize, url: Option<&str>) {
        let mut g = self.inner.write();
        let volume_id = g.tasks.get(idx).map(|t| t.volume_id);
        if let Some(t) = g.tasks.get_mut(idx) {
            t.status = "running".into();
            t.current_url = url.map(|s| s.to_string());
        }
        g.current_volume_id = volume_id;
        g.current_url = url.map(|s| s.to_string());
    }

    fn set_task_progress(&self, idx: usize, locs_done: usize, locs_total: usize) {
        let mut g = self.inner.write();
        if let Some(t) = g.tasks.get_mut(idx) {
            let pct = if locs_total == 0 {
                100.0
            } else {
                (locs_done as f64 / locs_total as f64) * 100.0
            };
            t.progress_pct = pct;
        }
        let done = g.tasks.iter().filter(|t| t.status == "ok" || t.status == "failed" || t.status == "skipped").count();
        g.tasks_done = done;
        let n = g.tasks_total.max(1) as f64;
        let sum: f64 = g.tasks.iter().map(|t| t.progress_pct).sum();
        g.overall_progress_pct = sum / n;
    }

    fn finish_task(&self, idx: usize, status: &str, message: impl Into<String>) {
        let mut g = self.inner.write();
        if let Some(t) = g.tasks.get_mut(idx) {
            t.status = status.into();
            t.message = message.into();
            if status == "ok" || status == "skipped" {
                t.progress_pct = 100.0;
            }
            t.current_url = None;
        }
        let done = g
            .tasks
            .iter()
            .filter(|t| matches!(t.status.as_str(), "ok" | "failed" | "skipped"))
            .count();
        g.tasks_done = done;
        let n = g.tasks_total.max(1) as f64;
        let sum: f64 = g.tasks.iter().map(|t| t.progress_pct).sum();
        g.overall_progress_pct = sum / n;
    }

    fn finish_run(&self) {
        let mut g = self.inner.write();
        g.running = false;
        g.finished_at = Some(now_secs());
        g.current_volume_id = None;
        g.current_url = None;
        g.overall_progress_pct = 100.0;
    }
}

fn estimated_ratio(file_count: u64, deleted_count: u64) -> f64 {
    let total = file_count.saturating_add(deleted_count);
    if total == 0 {
        0.0
    } else {
        deleted_count as f64 / total as f64
    }
}

/// Plan volumes whose heartbeat stats suggest enough garbage.
pub fn plan_vacuum(cluster: &ClusterState, threshold: f64) -> Vec<VacuumTask> {
    plan_vacuum_filtered(cluster, threshold, None)
}

pub fn plan_vacuum_filtered(
    cluster: &ClusterState,
    threshold: f64,
    only_ids: Option<&[u32]>,
) -> Vec<VacuumTask> {
    let servers = cluster.servers();
    let live_url = |node_id: &str, fallback: &str| -> String {
        servers
            .iter()
            .find(|s| s.node_id == node_id)
            .map(|s| s.location.url.clone())
            .unwrap_or_else(|| fallback.to_string())
    };

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
        if v.locations.is_empty() {
            continue;
        }
        if v.deleted_count == 0 {
            continue;
        }
        let ratio = estimated_ratio(v.file_count, v.deleted_count);
        if ratio < threshold {
            continue;
        }
        let mut locations: Vec<String> = v
            .locations
            .iter()
            .map(|l| live_url(&l.node_id, &l.url))
            .collect();
        locations.sort();
        locations.dedup();
        tasks.push(VacuumTask {
            volume_id: v.id,
            locations,
            estimated_garbage_ratio: ratio,
            status: "planned".into(),
        });
    }
    tasks.sort_by(|a, b| {
        b.estimated_garbage_ratio
            .partial_cmp(&a.estimated_garbage_ratio)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    tasks
}

#[derive(Debug, Deserialize)]
struct CheckResp {
    #[serde(default)]
    garbage_ratio: f64,
}

async fn post_json(
    http: &reqwest::Client,
    url: &str,
) -> Result<reqwest::Response, String> {
    match http.post(url).send().await {
        Ok(resp) if resp.status().is_success() => Ok(resp),
        Ok(resp) => Err(format!("{} {}", resp.status(), url)),
        Err(e) => Err(format!("{e} ({url})")),
    }
}

async fn vacuum_one_replica(
    http: &reqwest::Client,
    base: &str,
    volume_id: u32,
) -> Result<(), String> {
    let base = base.trim_end_matches('/');
    post_json(http, &format!("{base}/admin/vacuum/{volume_id}/compact")).await?;
    post_json(http, &format!("{base}/admin/vacuum/{volume_id}/commit")).await?;
    post_json(http, &format!("{base}/admin/vacuum/{volume_id}/cleanup")).await?;
    Ok(())
}

/// Run vacuum tasks; updates tracker when provided.
pub async fn execute_vacuum(
    http: &reqwest::Client,
    tasks: &[VacuumTask],
    tracker: Option<&VacuumTracker>,
    threshold: f64,
) -> Vec<VacuumTask> {
    let mut out = Vec::new();
    for (idx, t) in tasks.iter().enumerate() {
        let mut t = t.clone();
        let n_locs = t.locations.len().max(1);
        if let Some(tr) = tracker {
            tr.set_task_running(idx, t.locations.first().map(|s| s.as_str()));
        }
        let mut ok_all = true;
        let mut skipped_all = true;
        let mut last_err = String::new();
        for (li, url) in t.locations.iter().enumerate() {
            if let Some(tr) = tracker {
                tr.set_task_running(idx, Some(url.as_str()));
            }
            // Re-check ratio via HTTP; vacuum_one_replica no-ops if below threshold.
            let before = http
                .post(format!(
                    "{}/admin/vacuum/{}/check",
                    url.trim_end_matches('/'),
                    t.volume_id
                ))
                .send()
                .await
                .ok()
                .and_then(|r| {
                    if r.status().is_success() {
                        Some(r)
                    } else {
                        None
                    }
                });
            let ratio = if let Some(r) = before {
                r.json::<CheckResp>().await.ok().map(|b| b.garbage_ratio)
            } else {
                None
            };
            if ratio.map(|r| r < threshold).unwrap_or(false) {
                if let Some(tr) = tracker {
                    tr.set_task_progress(idx, li + 1, n_locs);
                }
                continue;
            }
            skipped_all = false;
            match vacuum_one_replica(http, url, t.volume_id).await {
                Ok(()) => {
                    if let Some(tr) = tracker {
                        tr.set_task_progress(idx, li + 1, n_locs);
                    }
                }
                Err(e) => {
                    ok_all = false;
                    last_err = e;
                    warn!(
                        "vacuum volume {} @ {}: {last_err}",
                        t.volume_id, url
                    );
                    break;
                }
            }
        }
        if skipped_all && ok_all {
            t.status = "skipped".into();
            if let Some(tr) = tracker {
                tr.finish_task(idx, "skipped", "below threshold on all replicas");
            }
        } else if ok_all {
            t.status = "ok".into();
            if let Some(tr) = tracker {
                tr.finish_task(idx, "ok", "");
            }
            info!(
                "vacuumed volume {} on {} replica(s)",
                t.volume_id,
                t.locations.len()
            );
        } else {
            t.status = "failed".into();
            if let Some(tr) = tracker {
                tr.finish_task(idx, "failed", &last_err);
            }
        }
        out.push(t);
    }
    if let Some(tr) = tracker {
        tr.finish_run();
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn estimated_ratio_math() {
        assert!((estimated_ratio(70, 30) - 0.3).abs() < 1e-9);
        assert_eq!(estimated_ratio(0, 0), 0.0);
    }
}
