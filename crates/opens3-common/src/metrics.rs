// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Shared Prometheus registry and metric helpers for OpenS3 components.

use prometheus::{
    Encoder, Gauge, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

pub static REGISTRY: LazyLock<Registry> =
    LazyLock::new(|| Registry::new_custom(Some("opens3".to_string()), None).expect("registry"));

static PROCESS_REGISTERED: AtomicBool = AtomicBool::new(false);

fn opts(name: &str, help: &str) -> Opts {
    Opts::new(name, help)
}

/// Process-level metrics shared by every component (idempotent).
pub fn ensure_process_metrics(reg: &Registry) {
    if PROCESS_REGISTERED.swap(true, Ordering::SeqCst) {
        return;
    }
    let version = env!("CARGO_PKG_VERSION");
    let info = IntGaugeVec::new(
        opts(
            "build_info",
            "Build metadata (always 1); labels carry version/component",
        ),
        &["version", "component"],
    )
    .expect("build_info");
    // Component label filled by callers via set_component_info; seed a base series.
    info.with_label_values(&[version, "opens3"]).set(1);
    let _ = reg.register(Box::new(info));

    let start = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0);
    let start_gauge = Gauge::with_opts(opts(
        "process_start_time_seconds",
        "Unix timestamp when this process registered metrics",
    ))
    .expect("start_time");
    start_gauge.set(start);
    let _ = reg.register(Box::new(start_gauge));
}

/// Master-side gauges / counters
pub struct ClerkMetrics {
    pub is_leader: IntGauge,
    pub term: IntGauge,
    pub commit_index: IntGauge,
    pub volume_servers: IntGauge,
    pub volumes: IntGauge,
    pub total_size_bytes: IntGauge,
    pub total_files: IntGauge,
    pub under_replicated_volumes: IntGauge,
    pub assign_total: IntCounterVec,
    pub lookup_total: IntCounterVec,
    pub heartbeat_total: IntCounterVec,
    pub heal_tasks: IntGauge,
    pub heal_runs_total: IntCounterVec,
    pub vacuum_tasks: IntGauge,
    pub vacuum_runs_total: IntCounterVec,
    pub balance_moves: IntGauge,
    pub balance_runs_total: IntCounterVec,
    pub request_duration: HistogramVec,
}

impl ClerkMetrics {
    pub fn register(reg: &Registry) -> anyhow::Result<Arc<Self>> {
        ensure_process_metrics(reg);
        let m = Self {
            is_leader: IntGauge::with_opts(opts(
                "clerk_is_leader",
                "1 if this clerk is Raft leader",
            ))?,
            term: IntGauge::with_opts(opts("clerk_raft_term", "Current Raft term"))?,
            commit_index: IntGauge::with_opts(opts(
                "clerk_raft_commit_index",
                "Raft commit index",
            ))?,
            volume_servers: IntGauge::with_opts(opts(
                "clerk_volume_servers",
                "Registered volume servers",
            ))?,
            volumes: IntGauge::with_opts(opts("clerk_volumes", "Known volumes"))?,
            total_size_bytes: IntGauge::with_opts(opts(
                "clerk_total_size_bytes",
                "Sum of volume sizes",
            ))?,
            total_files: IntGauge::with_opts(opts("clerk_total_files", "Sum of file counts"))?,
            under_replicated_volumes: IntGauge::with_opts(opts(
                "clerk_under_replicated_volumes",
                "Volumes with fewer locations than replication requires",
            ))?,
            assign_total: IntCounterVec::new(
                opts("clerk_assign_total", "Assign requests"),
                &["result"],
            )?,
            lookup_total: IntCounterVec::new(
                opts("clerk_lookup_total", "Volume lookup requests"),
                &["result"],
            )?,
            heartbeat_total: IntCounterVec::new(
                opts("clerk_heartbeat_total", "Heartbeats received"),
                &["result"],
            )?,
            heal_tasks: IntGauge::with_opts(opts("clerk_heal_tasks", "Last heal task count"))?,
            heal_runs_total: IntCounterVec::new(
                opts("clerk_heal_runs_total", "Heal planner executions"),
                &["result"],
            )?,
            vacuum_tasks: IntGauge::with_opts(opts(
                "clerk_vacuum_tasks",
                "Last vacuum task count",
            ))?,
            vacuum_runs_total: IntCounterVec::new(
                opts("clerk_vacuum_runs_total", "Vacuum planner executions"),
                &["result"],
            )?,
            balance_moves: IntGauge::with_opts(opts(
                "clerk_balance_moves",
                "Last balance move count",
            ))?,
            balance_runs_total: IntCounterVec::new(
                opts("clerk_balance_runs_total", "Balance planner executions"),
                &["result"],
            )?,
            request_duration: HistogramVec::new(
                HistogramOpts::new("clerk_request_duration_seconds", "Clerk HTTP latency")
                    .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
                &["op"],
            )?,
        };
        for c in [
            Box::new(m.is_leader.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(m.term.clone()),
            Box::new(m.commit_index.clone()),
            Box::new(m.volume_servers.clone()),
            Box::new(m.volumes.clone()),
            Box::new(m.total_size_bytes.clone()),
            Box::new(m.total_files.clone()),
            Box::new(m.under_replicated_volumes.clone()),
            Box::new(m.assign_total.clone()),
            Box::new(m.lookup_total.clone()),
            Box::new(m.heartbeat_total.clone()),
            Box::new(m.heal_tasks.clone()),
            Box::new(m.heal_runs_total.clone()),
            Box::new(m.vacuum_tasks.clone()),
            Box::new(m.vacuum_runs_total.clone()),
            Box::new(m.balance_moves.clone()),
            Box::new(m.balance_runs_total.clone()),
            Box::new(m.request_duration.clone()),
        ] {
            reg.register(c)?;
        }
        Ok(Arc::new(m))
    }
}

/// Volume-side metrics
pub struct VolumeMetrics {
    pub volume_count: IntGauge,
    pub max_volumes: IntGauge,
    pub free_disk_bytes: IntGauge,
    pub total_size_bytes: IntGauge,
    pub total_files: IntGauge,
    pub deleted_files: IntGauge,
    pub needle_ops: IntCounterVec,
    pub needle_bytes: IntCounterVec,
    pub replicate_total: IntCounterVec,
    pub compact_total: IntCounterVec,
    pub vacuum_total: IntCounterVec,
    pub ec_total: IntCounterVec,
    pub heartbeat_fail: IntGauge,
    pub request_duration: HistogramVec,
    pub detail_size: IntGaugeVec,
    pub detail_files: IntGaugeVec,
}

impl VolumeMetrics {
    pub fn register(reg: &Registry) -> anyhow::Result<Arc<Self>> {
        ensure_process_metrics(reg);
        let m = Self {
            volume_count: IntGauge::with_opts(opts("volume_count", "Local volumes"))?,
            max_volumes: IntGauge::with_opts(opts(
                "volume_max_volumes",
                "Configured max local volumes",
            ))?,
            free_disk_bytes: IntGauge::with_opts(opts(
                "volume_free_disk_bytes",
                "Free disk space on volume data dir",
            ))?,
            total_size_bytes: IntGauge::with_opts(opts("volume_size_bytes", "Local data size"))?,
            total_files: IntGauge::with_opts(opts("volume_files", "Live needles"))?,
            deleted_files: IntGauge::with_opts(opts("volume_deleted_files", "Deleted needles"))?,
            needle_ops: IntCounterVec::new(
                opts("volume_needle_ops_total", "Needle operations"),
                &["op", "result"],
            )?,
            needle_bytes: IntCounterVec::new(
                opts("volume_needle_bytes_total", "Needle payload bytes"),
                &["op"],
            )?,
            replicate_total: IntCounterVec::new(
                opts("volume_replicate_total", "Volume/needle replication"),
                &["result"],
            )?,
            compact_total: IntCounterVec::new(
                opts("volume_compact_total", "Compact runs"),
                &["result"],
            )?,
            vacuum_total: IntCounterVec::new(
                opts("volume_vacuum_total", "Vacuum phase runs"),
                &["phase", "result"],
            )?,
            ec_total: IntCounterVec::new(opts("volume_ec_total", "EC encode runs"), &["result"])?,
            heartbeat_fail: IntGauge::with_opts(opts(
                "volume_heartbeat_fail",
                "1 if last heartbeat failed",
            ))?,
            request_duration: HistogramVec::new(
                HistogramOpts::new("volume_request_duration_seconds", "HTTP handler latency")
                    .buckets(vec![
                        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
                    ]),
                &["op"],
            )?,
            detail_size: IntGaugeVec::new(
                opts("volume_detail_size_bytes", "Per-volume size"),
                &["volume_id", "state"],
            )?,
            detail_files: IntGaugeVec::new(
                opts("volume_detail_files", "Per-volume live files"),
                &["volume_id", "state"],
            )?,
        };
        for c in [
            Box::new(m.volume_count.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(m.max_volumes.clone()),
            Box::new(m.free_disk_bytes.clone()),
            Box::new(m.total_size_bytes.clone()),
            Box::new(m.total_files.clone()),
            Box::new(m.deleted_files.clone()),
            Box::new(m.needle_ops.clone()),
            Box::new(m.needle_bytes.clone()),
            Box::new(m.replicate_total.clone()),
            Box::new(m.compact_total.clone()),
            Box::new(m.vacuum_total.clone()),
            Box::new(m.ec_total.clone()),
            Box::new(m.heartbeat_fail.clone()),
            Box::new(m.request_duration.clone()),
            Box::new(m.detail_size.clone()),
            Box::new(m.detail_files.clone()),
        ] {
            reg.register(c)?;
        }
        Ok(Arc::new(m))
    }
}

/// S3 gateway metrics
pub struct S3Metrics {
    pub requests: IntCounterVec,
    pub request_bytes: IntCounterVec,
    pub in_flight: IntGauge,
    pub errors: IntCounterVec,
    pub multipart_uploads: IntGauge,
    pub request_duration: HistogramVec,
    pub auth_fail: IntCounterVec,
}

impl S3Metrics {
    pub fn register(reg: &Registry) -> anyhow::Result<Arc<Self>> {
        ensure_process_metrics(reg);
        let m = Self {
            requests: IntCounterVec::new(
                opts("s3_requests_total", "S3 API requests"),
                &["method", "action", "status"],
            )?,
            request_bytes: IntCounterVec::new(
                opts("s3_request_bytes_total", "Request/response body bytes"),
                &["direction"],
            )?,
            in_flight: IntGauge::with_opts(opts("s3_requests_in_flight", "In-flight S3 requests"))?,
            errors: IntCounterVec::new(opts("s3_errors_total", "S3 errors by code"), &["code"])?,
            multipart_uploads: IntGauge::with_opts(opts(
                "s3_multipart_uploads",
                "Active multipart uploads",
            ))?,
            request_duration: HistogramVec::new(
                HistogramOpts::new("s3_request_duration_seconds", "S3 handler latency").buckets(
                    vec![0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
                ),
                &["action"],
            )?,
            auth_fail: IntCounterVec::new(
                opts("s3_auth_fail_total", "Authentication failures"),
                &["reason"],
            )?,
        };
        for c in [
            Box::new(m.requests.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(m.request_bytes.clone()),
            Box::new(m.in_flight.clone()),
            Box::new(m.errors.clone()),
            Box::new(m.multipart_uploads.clone()),
            Box::new(m.request_duration.clone()),
            Box::new(m.auth_fail.clone()),
        ] {
            reg.register(c)?;
        }
        Ok(Arc::new(m))
    }
}

/// Filer HTTP API metrics
pub struct FilerMetrics {
    pub ops: IntCounterVec,
    pub bytes: IntCounterVec,
    pub request_duration: HistogramVec,
    pub in_flight: IntGauge,
}

impl FilerMetrics {
    pub fn register(reg: &Registry) -> anyhow::Result<Arc<Self>> {
        ensure_process_metrics(reg);
        let m = Self {
            ops: IntCounterVec::new(
                opts("filer_ops_total", "Filer entry operations"),
                &["op", "result"],
            )?,
            bytes: IntCounterVec::new(
                opts("filer_bytes_total", "Filer payload bytes"),
                &["direction"],
            )?,
            request_duration: HistogramVec::new(
                HistogramOpts::new("filer_request_duration_seconds", "Filer handler latency")
                    .buckets(vec![
                        0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
                    ]),
                &["op"],
            )?,
            in_flight: IntGauge::with_opts(opts(
                "filer_requests_in_flight",
                "In-flight filer requests",
            ))?,
        };
        for c in [
            Box::new(m.ops.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(m.bytes.clone()),
            Box::new(m.request_duration.clone()),
            Box::new(m.in_flight.clone()),
        ] {
            reg.register(c)?;
        }
        Ok(Arc::new(m))
    }
}

pub fn encode_metrics(reg: &Registry) -> String {
    let families = reg.gather();
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    if encoder.encode(&families, &mut buf).is_err() {
        return String::new();
    }
    String::from_utf8_lossy(&buf).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn master_metrics_encode() {
        let reg = Registry::new_custom(Some("opens3".into()), None).unwrap();
        let m = ClerkMetrics::register(&reg).unwrap();
        m.assign_total.with_label_values(&["ok"]).inc();
        let text = encode_metrics(&reg);
        assert!(text.contains("opens3_clerk_assign_total"));
        assert!(text.contains("opens3_process_start_time_seconds"));
    }
}
