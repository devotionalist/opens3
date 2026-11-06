# 监控指南

OpenS3 各组件暴露 **Prometheus 文本格式**指标：`GET /metrics`。

| 组件 | 默认地址 | 指标前缀 |
|------|----------|----------|
| Clerk | `:9333/metrics` | `opens3_clerk_*` |
| Volume | `:8080/metrics` | `opens3_volume_*` |
| Filer（统一网关） | `:8333/metrics` | `opens3_s3_*` + `opens3_filer_*` |

统一命名空间 `opens3`（序列化后为 `opens3_<name>`）。另有进程级指标：

| 指标 | 说明 |
|------|------|
| `opens3_build_info{version,component}` | 构建版本（恒为 1） |
| `opens3_process_start_time_seconds` | 进程注册 metrics 的 Unix 时间 |

## 快速查看

```bash
curl -s http://127.0.0.1:9333/metrics | head
curl -s http://127.0.0.1:8080/metrics | grep needle
curl -s http://127.0.0.1:8333/metrics | grep -E 's3_requests|filer_ops'
```

## 指标清单

### Clerk

| 指标 | 类型 | 说明 |
|------|------|------|
| `opens3_clerk_is_leader` | gauge | 本节点是否 leader（1/0） |
| `opens3_clerk_raft_term` | gauge | Raft term |
| `opens3_clerk_raft_commit_index` | gauge | 已提交日志序号 |
| `opens3_clerk_volume_servers` | gauge | 在线 Volume 数 |
| `opens3_clerk_volumes` | gauge | 已知卷数 |
| `opens3_clerk_total_size_bytes` | gauge | 卷大小合计 |
| `opens3_clerk_total_files` | gauge | 文件数合计 |
| `opens3_clerk_under_replicated_volumes` | gauge | 副本不足的卷 |
| `opens3_clerk_assign_total{result}` | counter | assign 成功/失败 |
| `opens3_clerk_lookup_total{result}` | counter | 卷查找 |
| `opens3_clerk_heartbeat_total{result}` | counter | heartbeat 次数 |
| `opens3_clerk_heal_tasks` | gauge | 最近 heal 任务数 |
| `opens3_clerk_heal_runs_total{result}` | counter | heal 规划执行 |
| `opens3_clerk_balance_moves` | gauge | 最近 balance 计划数 |
| `opens3_clerk_balance_runs_total{result}` | counter | balance 执行 |
| `opens3_clerk_request_duration_seconds{op}` | histogram | Clerk HTTP 延迟 |

### Volume

| 指标 | 类型 | 说明 |
|------|------|------|
| `opens3_volume_count` | gauge | 本地卷数量 |
| `opens3_volume_max_volumes` | gauge | 配置上限 |
| `opens3_volume_free_disk_bytes` | gauge | 数据目录剩余磁盘 |
| `opens3_volume_size_bytes` | gauge | 本地数据量 |
| `opens3_volume_files` / `opens3_volume_deleted_files` | gauge | 存活/已删 needle |
| `opens3_volume_detail_size_bytes{volume_id,state}` | gauge | 分卷大小 |
| `opens3_volume_detail_files{volume_id,state}` | gauge | 分卷文件数 |
| `opens3_volume_needle_ops_total{op,result}` | counter | read/write/delete |
| `opens3_volume_needle_bytes_total{op}` | counter | 读写字节 |
| `opens3_volume_replicate_total{result}` | counter | 复制 |
| `opens3_volume_compact_total{result}` | counter | compact |
| `opens3_volume_ec_total{result}` | counter | EC |
| `opens3_volume_heartbeat_fail` | gauge | 最近心跳是否失败 |
| `opens3_volume_request_duration_seconds{op}` | histogram | 处理延迟 |

### Filer

| 指标 | 类型 | 说明 |
|------|------|------|
| `opens3_filer_ops_total{op,result}` | counter | put/get/delete/list |
| `opens3_filer_bytes_total{direction}` | counter | 入/出站字节 |
| `opens3_filer_requests_in_flight` | gauge | HTTP 并发 |
| `opens3_filer_request_duration_seconds{op}` | histogram | 延迟 |

（统一 Filer 同端口同时暴露 `opens3_s3_*` 与 `opens3_filer_*`。）

### S3

| 指标 | 类型 | 说明 |
|------|------|------|
| `opens3_s3_requests_total{method,action,status}` | counter | API 请求（含 multipart action） |
| `opens3_s3_request_bytes_total{direction}` | counter | in/out 字节 |
| `opens3_s3_requests_in_flight` | gauge | 并发中请求 |
| `opens3_s3_errors_total{code}` | counter | 错误 |
| `opens3_s3_auth_fail_total{reason}` | counter | 鉴权失败 |
| `opens3_s3_multipart_uploads` | gauge | 进行中的分片上传 |
| `opens3_s3_request_duration_seconds{action}` | histogram | 延迟 |

## Kubernetes 采集

### Prometheus Operator（推荐）

```bash
helm upgrade opens3 deploy/helm/opens3 -n opens3 \
  --reuse-values \
  --set serviceMonitor.enabled=true \
  --set serviceMonitor.labels.release=kube-prometheus-stack \
  --set prometheusRule.enabled=true \
  --set prometheusRule.labels.release=kube-prometheus-stack \
  --set grafanaDashboard.enabled=true
```

- **ServiceMonitor**：Clerk / Volume / S3，默认 `interval=15s`、`scrapeTimeout=10s`
- **PrometheusRule**：内置告警（无 leader、欠副本、磁盘、assign 错误、S3 错误/延迟、复制失败等）
- **Grafana ConfigMap**：带 `grafana_dashboard: "1"`，可被 sidecar 自动导入

### 静态 scrape

参考 [`deploy/monitoring/prometheus-scrape.yaml`](../deploy/monitoring/prometheus-scrape.yaml)。Volume 用 `kubernetes_sd_configs` 抓每个 Pod。

## Grafana

导入 [`deploy/monitoring/grafana-dashboard.json`](../deploy/monitoring/grafana-dashboard.json)（uid: `opens3-overview`）。

面板：Leader/Raft、容量、欠副本、S3 QPS/延迟/字节/错误、Volume 吞吐/磁盘/填充率、Filer、heal/balance。

## 告警

完整规则：[`deploy/monitoring/alerts.yaml`](../deploy/monitoring/alerts.yaml)（与 Helm `PrometheusRule` 对齐）。

| 告警 | 级别 | 触发条件 |
|------|------|----------|
| OpenS3ClerkNoLeader | critical | 2m 无 leader |
| OpenS3UnderReplicatedVolumes | warning | 欠副本持续 10m |
| OpenS3VolumeHeartbeatFail | warning | 心跳失败 3m |
| OpenS3VolumeDiskLow | warning | 剩余磁盘 < 10Gi |
| OpenS3VolumeNearCapacity | warning | 卷数 > 90% 上限 |
| OpenS3AssignErrors | warning | assign 错误率 > 10% |
| OpenS3S3HighErrorRate | warning | S3 错误率 > 5% |
| OpenS3S3HighLatency | warning | S3 p99 > 2s |
| OpenS3S3AuthFailures | info | 鉴权失败速率 |
| OpenS3ReplicateFailures | warning | 复制失败 |
| OpenS3FilerErrors | warning | Filer 错误率 > 5% |

## 建议看板与 SLI

| SLI | 表达式思路 |
|-----|------------|
| 可用性 | `1 - errors/requests`（S3 5xx） |
| 延迟 | `histogram_quantile(0.99, …s3_request_duration…)` |
| 数据健康 | `under_replicated_volumes == 0` |
| 控制面 | `max(opens3_clerk_is_leader) == 1` |
| 容量 | `volume_count / volume_max_volumes`、`free_disk_bytes` |

## 本地联调

```bash
opens3 server --dir /tmp/o --max-volume-size-mb 64 &
sleep 2
curl -s localhost:9333/metrics | grep under_replicated
curl -s localhost:8333/metrics | grep -E 's3_requests|filer_ops|build_info'
for i in $(seq 1 20); do curl -s -X PUT localhost:8333/b/k$i -d "x$i" >/dev/null; done
curl -s localhost:8333/metrics | grep s3_requests_total
curl -s localhost:8080/metrics | grep volume_needle
```
