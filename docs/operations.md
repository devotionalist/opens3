> 语言: [中文](./operations.md) · [English](en/operations.md)

# 运维手册

## 集群状态

```bash
opens3 shell cluster-status --clerk http://CLERK:9333
opens3 shell raft-status --clerk http://CLERK:9333
opens3 shell volume-list --clerk http://CLERK:9333
curl -s http://CLERK:9333/cluster/status | jq .
```

关注字段：`is_leader`、`term`、`volume_server_count`、`servers[].last_seen`、各卷 `state`。

## 健康与就绪

| URL | 含义 |
|-----|------|
| `GET /health`（clerk / volume / filer） | 进程存活 |
| `GET /ready`（clerk） | 已有 leader 感知，可接入流量 |
| `GET /metrics` | Prometheus 文本指标 |

完整指标表、Grafana 看板、告警规则与 ServiceMonitor 见 **[监控指南](monitoring.md)**。

Clerk：`opens3_clerk_is_leader`、`opens3_clerk_under_replicated_volumes`、`opens3_clerk_assign_total`  
Volume：`opens3_volume_needle_ops_total`、`opens3_volume_heartbeat_fail`  
Filer：`opens3_s3_requests_total`、`opens3_filer_ops_total`  

## 缺副本修复（Heal）

Clerk 定期扫描；也可手动：

```bash
opens3 shell heal --clerk http://CLERK:9333
# 或
curl -X POST http://CLERK:9333/cluster/heal
```

流程：对比卷的期望副本数与 `locations` → 从源 Volume `POST /admin/replicate_volume/{id}?targets=…&max_bps=…` → 目标 `import_volume_bin` 写入 `.dat/.idx` 并 reopen。

### Heal / Balance 带宽上限

整卷推送可能打满网卡。集群级限速（Raft 持久化，`0` = 不限）：

```bash
# 查看
curl http://CLERK:9333/cluster/heal-bandwidth

# 设为 50 MB/s（也可用 max_bps）
curl -X POST http://CLERK:9333/cluster/heal-bandwidth \
  -H 'Content-Type: application/json' \
  -d '{"max_mbps":50}'

# 取消限速
curl -X POST http://CLERK:9333/cluster/heal-bandwidth \
  -H 'Content-Type: application/json' \
  -d '{"max_mbps":0}'
```

Admin 拓扑页 Heal 面板也可改「带宽上限」。限速对自动 heal、手动 Heal、place-replicas、Balance 一律生效；Volume 侧按平均速率分块推送。

## 掉电 / 半写恢复

Volume 启动打开卷时：扫描 `.dat`，保留所有 **CRC 完整** 的 needle，只把文件截到最后一条完整记录末尾（不是清空整卷）。Filer 读对象时若某 location CRC/连接失败，会自动试 lookup 返回的其他副本。

### 重启指纹对齐（降级写，后台追平）

Volume 进程每次启动带新 `boot_id`。Clerk 发现变化且该节点有多副本卷时：

1. 该节点 `write_ready=false`：本机 Put/ReplicateNeedle 返回 503；**不出现在 assign 的 location 列表里**  
2. **存活副本继续接写**（降级：暂时少一个同步副本），集群不会因大差距整卷拷贝而长时间停写  
3. 后台向同伴拉 `GET /admin/volume_fp/{id}`，以更大副本为源对重启节点整卷覆盖（受 heal 带宽上限约束；源在拷贝期间若继续增长会多轮追平，最多 5 轮）  
4. 追平结束后该节点 `write_ready=true`，重新加入同步扇出  

单副本 / 无卷节点启动后立即 `write_ready`。读仍可走任意 location（含正在追平的节点，失败则 failover）。

## 容量均衡（Balance）

```bash
opens3 shell volume-balance --clerk http://CLERK:9333
curl -X POST http://CLERK:9333/cluster/balance
```

从卷数明显偏多的节点，把卷副本复制到偏少节点（**复制而非删除源**；缩容前请人工确认）。

## 卷密封与 EC

```bash
# 在 Volume 节点
curl -X POST http://VOLUME:8080/admin/seal/{volumeId}
curl -X POST 'http://VOLUME:8080/admin/ec/{volumeId}?data_shards=4&parity_shards=2'

# Clerk 标记
curl -X POST http://CLERK:9333/vol/seal/{volumeId}
curl -X POST http://CLERK:9333/vol/ec/{volumeId}
```

EC 产物在 `{dir}/ec/{id}.ec.*`。

## Compact（回收删除针脚）

```bash
curl -X POST http://VOLUME:8080/admin/compact/{volumeId}
```

返回 `before` / `after` 字节数。建议在低峰对删除率高的卷执行。

## 直接 Blob 调试（绕过 S3）

```bash
opens3 shell assign --clerk http://CLERK:9333 --replication 001
# 得到 fid 与 url
curl -X PUT "http://VOLUME:8080/{fid}?locations=http://v1:8080,http://v2:8080" --data-binary @file
curl "http://VOLUME:8080/{fid}" -o out
```

## 日志

```bash
RUST_LOG=info,opens3_clerk=debug,opens3_volume=debug opens3 server --dir ./data
```

## 备份建议

| 数据 | 路径 | 建议 |
|------|------|------|
| 对象数据 | Volume `--dir` | 依赖副本/EC；可冷备整盘 |
| Filer 元数据 | `meta.redb` | 定期快照；损坏则对象成孤儿 |
| Raft | `raft_dir` | 三副本 Clerk；勿单点删日志 |

## Admin UI

浏览器打开 `http://S3:8333/admin/ui`，轮询 `/admin/api/status`。
