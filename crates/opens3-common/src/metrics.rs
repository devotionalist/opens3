// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 共享 Prometheus 注册表与各组件指标辅助。

use prometheus::{
    Encoder, Gauge, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock};
use std::time::{SystemTime, UNIX_EPOCH};

/// 全局默认 Registry（指标名前缀 `opens3`）
pub static REGISTRY: LazyLock<Registry> =
    LazyLock::new(|| Registry::new_custom(Some("opens3".to_string()), None).expect("registry"));

/// 进程级指标是否已注册（跨 register 调用幂等）
static PROCESS_REGISTERED: AtomicBool = AtomicBool::new(false);

/// 构造带名称与帮助文本的 Opts
fn opts(name: &str, help: &str) -> Opts {
    Opts::new(name, help)
}

/// 注册进程级指标（幂等；各组件 register 时先调用）
pub fn ensure_process_metrics(reg: &Registry) {
    if PROCESS_REGISTERED.swap(true, Ordering::SeqCst) {
        return; // 已注册则跳过
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
    // 组件 label 由调用方补充；先种一条基础序列
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

/// Clerk（master）侧 gauges / counters
pub struct ClerkMetrics {
    pub is_leader: IntGauge,                 // 本节点是否 Raft leader（1/0）
    pub term: IntGauge,                      // 当前 Raft term
    pub commit_index: IntGauge,              // Raft commit index
    pub volume_servers: IntGauge,            // 已登记 volume server 数
    pub volumes: IntGauge,                   // 已知卷数
    pub total_size_bytes: IntGauge,          // 各卷 size 之和
    pub total_files: IntGauge,               // 各卷 file_count 之和
    pub under_replicated_volumes: IntGauge,  // 副本不足的卷数
    pub assign_total: IntCounterVec,         // assign 请求计数（按 result）
    pub lookup_total: IntCounterVec,         // volume lookup 计数
    pub heartbeat_total: IntCounterVec,      // 收到的心跳计数
    pub heal_tasks: IntGauge,                // 最近一次 heal 任务数
    pub heal_runs_total: IntCounterVec,      // heal planner 执行次数
    pub vacuum_tasks: IntGauge,              // 最近一次 vacuum 任务数
    pub vacuum_runs_total: IntCounterVec,    // vacuum planner 执行次数
    pub balance_moves: IntGauge,             // 最近一次 balance 迁移数
    pub balance_runs_total: IntCounterVec,   // balance planner 执行次数
    pub request_duration: HistogramVec,      // Clerk HTTP 延迟（按 op）
}

impl ClerkMetrics {
    /// 向 Registry 注册全部 Clerk 指标并返回共享句柄
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
        // 逐个注册；任一项失败则整体返回错误
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

/// Volume 节点侧指标
pub struct VolumeMetrics {
    pub volume_count: IntGauge,          // 本机卷数量
    pub max_volumes: IntGauge,           // 配置的本机卷上限
    pub free_disk_bytes: IntGauge,       // 数据目录剩余磁盘
    pub total_size_bytes: IntGauge,      // 本机数据总大小
    pub total_files: IntGauge,           // 存活 needle 数
    pub deleted_files: IntGauge,         // 软删 needle 数
    pub needle_ops: IntCounterVec,       // Needle 操作计数（op, result）
    pub needle_bytes: IntCounterVec,     // Needle payload 字节（按 op）
    pub replicate_total: IntCounterVec,  // 卷/needle 复制次数
    pub compact_total: IntCounterVec,    // compact 运行次数
    pub vacuum_total: IntCounterVec,     // vacuum 各 phase 运行次数
    pub ec_total: IntCounterVec,         // EC 编码运行次数
    pub heartbeat_fail: IntGauge,        // 最近心跳是否失败（1/0）
    pub request_duration: HistogramVec,  // HTTP handler 延迟
    pub detail_size: IntGaugeVec,        // 按 volume_id/state 的大小
    pub detail_files: IntGaugeVec,       // 按 volume_id/state 的存活文件数
}

impl VolumeMetrics {
    /// 向 Registry 注册全部 Volume 指标并返回共享句柄
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

/// S3 网关指标
pub struct S3Metrics {
    pub requests: IntCounterVec,         // S3 API 请求（method/action/status）
    pub request_bytes: IntCounterVec,    // 请求/响应 body 字节（按 direction）
    pub in_flight: IntGauge,             // 进行中的 S3 请求数
    pub errors: IntCounterVec,           // 按 S3 错误码计数
    pub multipart_uploads: IntGauge,     // 活跃 multipart 上传数
    pub request_duration: HistogramVec,  // handler 延迟（按 action）
    pub auth_fail: IntCounterVec,        // 认证失败（按 reason）
}

impl S3Metrics {
    /// 向 Registry 注册全部 S3 指标并返回共享句柄
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

/// Filer HTTP API 指标
pub struct FilerMetrics {
    pub ops: IntCounterVec,              // 条目操作计数（op, result）
    pub bytes: IntCounterVec,            // payload 字节（按 direction）
    pub request_duration: HistogramVec,  // handler 延迟（按 op）
    pub in_flight: IntGauge,             // 进行中的 filer 请求数
}

impl FilerMetrics {
    /// 向 Registry 注册全部 Filer 指标并返回共享句柄
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

/// 将 Registry 编码为 Prometheus text exposition 格式
pub fn encode_metrics(reg: &Registry) -> String {
    let families = reg.gather();
    let encoder = TextEncoder::new();
    let mut buf = Vec::new();
    if encoder.encode(&families, &mut buf).is_err() {
        return String::new(); // 编码失败时返回空串，避免打挂 scrape
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
