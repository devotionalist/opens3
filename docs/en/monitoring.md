> Language: [English](./monitoring.md) · [中文](../monitoring.md)

# Monitoring guide

OpenS3 components expose **Prometheus text-format** metrics at `GET /metrics`.

| Component | Default address | Metric prefix |
|-----------|-----------------|---------------|
| Clerk | `:9333/metrics` | `opens3_clerk_*` |
| Volume | `:8080/metrics` | `opens3_volume_*` |
| Filer (unified gateway) | `:8333/metrics` | `opens3_s3_*` + `opens3_filer_*` |

Unified namespace `opens3` (serialized as `opens3_<name>`). Process-level metrics also exist:

| Metric | Description |
|--------|-------------|
| `opens3_build_info{version,component}` | Build version (always 1) |
| `opens3_process_start_time_seconds` | Unix time when the process registered metrics |

## Quick peek

```bash
curl -s http://127.0.0.1:9333/metrics | head
curl -s http://127.0.0.1:8080/metrics | grep needle
curl -s http://127.0.0.1:8333/metrics | grep -E 's3_requests|filer_ops'
```

## Metric catalog

### Clerk

| Metric | Type | Description |
|--------|------|-------------|
| `opens3_clerk_is_leader` | gauge | Whether this node is leader (1/0) |
| `opens3_clerk_raft_term` | gauge | Raft term |
| `opens3_clerk_raft_commit_index` | gauge | Committed log index |
| `opens3_clerk_volume_servers` | gauge | Online Volume count |
| `opens3_clerk_volumes` | gauge | Known volume count |
| `opens3_clerk_total_size_bytes` | gauge | Sum of volume sizes |
| `opens3_clerk_total_files` | gauge | Sum of file counts |
| `opens3_clerk_under_replicated_volumes` | gauge | Under-replicated volumes |
| `opens3_clerk_assign_total{result}` | counter | assign success/failure |
| `opens3_clerk_lookup_total{result}` | counter | Volume lookups |
| `opens3_clerk_heartbeat_total{result}` | counter | heartbeat count |
| `opens3_clerk_heal_tasks` | gauge | Recent heal task count |
| `opens3_clerk_heal_runs_total{result}` | counter | heal planning runs |
| `opens3_clerk_balance_moves` | gauge | Recent balance plan count |
| `opens3_clerk_balance_runs_total{result}` | counter | balance runs |
| `opens3_clerk_request_duration_seconds{op}` | histogram | Clerk HTTP latency |

### Volume

| Metric | Type | Description |
|--------|------|-------------|
| `opens3_volume_count` | gauge | Local volume count |
| `opens3_volume_max_volumes` | gauge | Configured cap |
| `opens3_volume_free_disk_bytes` | gauge | Free disk under data dir |
| `opens3_volume_size_bytes` | gauge | Local data size |
| `opens3_volume_files` / `opens3_volume_deleted_files` | gauge | Live/deleted needles |
| `opens3_volume_detail_size_bytes{volume_id,state}` | gauge | Per-volume size |
| `opens3_volume_detail_files{volume_id,state}` | gauge | Per-volume file count |
| `opens3_volume_needle_ops_total{op,result}` | counter | read/write/delete |
| `opens3_volume_needle_bytes_total{op}` | counter | Read/write bytes |
| `opens3_volume_replicate_total{result}` | counter | Replication |
| `opens3_volume_compact_total{result}` | counter | compact |
| `opens3_volume_ec_total{result}` | counter | EC |
| `opens3_volume_heartbeat_fail` | gauge | Whether last heartbeat failed |
| `opens3_volume_request_duration_seconds{op}` | histogram | Handling latency |

### Filer

| Metric | Type | Description |
|--------|------|-------------|
| `opens3_filer_ops_total{op,result}` | counter | put/get/delete/list |
| `opens3_filer_bytes_total{direction}` | counter | Ingress/egress bytes |
| `opens3_filer_requests_in_flight` | gauge | Concurrent HTTP |
| `opens3_filer_request_duration_seconds{op}` | histogram | Latency |

(The unified Filer exposes both `opens3_s3_*` and `opens3_filer_*` on the same port.)

### S3

| Metric | Type | Description |
|--------|------|-------------|
| `opens3_s3_requests_total{method,action,status}` | counter | API requests (including multipart actions) |
| `opens3_s3_request_bytes_total{direction}` | counter | in/out bytes |
| `opens3_s3_requests_in_flight` | gauge | In-flight requests |
| `opens3_s3_errors_total{code}` | counter | Errors |
| `opens3_s3_auth_fail_total{reason}` | counter | Auth failures |
| `opens3_s3_multipart_uploads` | gauge | In-progress multipart uploads |
| `opens3_s3_request_duration_seconds{action}` | histogram | Latency |

## Kubernetes scraping

### Prometheus Operator (recommended)

```bash
helm upgrade opens3 deploy/helm/opens3 -n opens3 \
  --reuse-values \
  --set serviceMonitor.enabled=true \
  --set serviceMonitor.labels.release=kube-prometheus-stack \
  --set prometheusRule.enabled=true \
  --set prometheusRule.labels.release=kube-prometheus-stack \
  --set grafanaDashboard.enabled=true
```

- **ServiceMonitor**: Clerk / Volume / S3; default `interval=15s`, `scrapeTimeout=10s`
- **PrometheusRule**: built-in alerts (no leader, under-replication, disk, assign errors, S3 errors/latency, replicate failures, etc.)
- **Grafana ConfigMap**: labeled `grafana_dashboard: "1"` for sidecar auto-import

### Static scrape

See [`deploy/monitoring/prometheus-scrape.yaml`](../../deploy/monitoring/prometheus-scrape.yaml). Volume uses `kubernetes_sd_configs` to scrape each Pod.

## Grafana

Import [`deploy/monitoring/grafana-dashboard.json`](../../deploy/monitoring/grafana-dashboard.json) (uid: `opens3-overview`).

Panels: Leader/Raft, capacity, under-replication, S3 QPS/latency/bytes/errors, Volume throughput/disk/fill ratio, Filer, heal/balance.

## Alerts

Full rules: [`deploy/monitoring/alerts.yaml`](../../deploy/monitoring/alerts.yaml) (aligned with Helm `PrometheusRule`).

| Alert | Severity | Trigger |
|-------|----------|---------|
| OpenS3ClerkNoLeader | critical | No leader for 2m |
| OpenS3UnderReplicatedVolumes | warning | Under-replication for 10m |
| OpenS3VolumeHeartbeatFail | warning | Heartbeat failure for 3m |
| OpenS3VolumeDiskLow | warning | Free disk < 10Gi |
| OpenS3VolumeNearCapacity | warning | Volume count > 90% of cap |
| OpenS3AssignErrors | warning | assign error rate > 10% |
| OpenS3S3HighErrorRate | warning | S3 error rate > 5% |
| OpenS3S3HighLatency | warning | S3 p99 > 2s |
| OpenS3S3AuthFailures | info | Auth failure rate |
| OpenS3ReplicateFailures | warning | Replication failures |
| OpenS3FilerErrors | warning | Filer error rate > 5% |

## Suggested dashboards & SLIs

| SLI | Expression idea |
|-----|-----------------|
| Availability | `1 - errors/requests` (S3 5xx) |
| Latency | `histogram_quantile(0.99, …s3_request_duration…)` |
| Data health | `under_replicated_volumes == 0` |
| Control plane | `max(opens3_clerk_is_leader) == 1` |
| Capacity | `volume_count / volume_max_volumes`, `free_disk_bytes` |

## Local smoke

```bash
opens3 server --dir /tmp/o --max-volume-size-mb 64 &
sleep 2
curl -s localhost:9333/metrics | grep under_replicated
curl -s localhost:8333/metrics | grep -E 's3_requests|filer_ops|build_info'
for i in $(seq 1 20); do curl -s -X PUT localhost:8333/b/k$i -d "x$i" >/dev/null; done
curl -s localhost:8333/metrics | grep s3_requests_total
curl -s localhost:8080/metrics | grep volume_needle
```
