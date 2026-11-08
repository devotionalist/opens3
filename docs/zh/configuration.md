> 语言: [English](../en/configuration.md) · [中文](./configuration.md)

# 配置参考

OpenS3 以 **CLI 参数 + 环境变量** 配置（无独立必选配置文件）。

## 环境变量

| 变量 | 作用 |
|------|------|
| `OPENS3_ACCESS_KEY` | S3 Access Key（与 Secret 同时设置才启用 SigV4） |
| `OPENS3_SECRET_KEY` | S3 Secret Key |
| `OPENS3_VOLUME_JWT_SECRET` | Clerk/Volume/Filer/S3 共享；开启后 assign 签发 JWT，Volume 写/删强制 Bearer |
| `OPENS3_TLS_CERT` / `OPENS3_TLS_KEY` | PEM 证书与私钥；进程内 HTTPS |
| `OPENS3_KMS_KEY` | 本地 SSE-KMS 口令 |
| `OPENS3_CLERK_PEERS` | Volume 多 Clerk 跟随 |
| `OPENS3_META_BACKEND` | `redb` / `memory` / `postgres` / `mysql` / `tidb` / `http` |
| `DATABASE_URL` | Postgres meta |
| `RUST_LOG` | 日志级别，如 `info`、`opens3_volume=debug` |

## `opens3 server`（all-in-one）

| 参数 | 默认 | 说明 |
|------|------|------|
| `--dir` | `./data` | 数据根目录（volumes/filer/raft） |
| `--clerk-port` | `9333` | Clerk |
| `--volume-port` | `8080` | Volume HTTP |
| `--gateway-port` | `8333` | 统一 Filer（S3 + /entries + /dav） |
| `--default-replication` | `000` | 默认副本码 |
| `--max-volume-size-mb` | （内部默认约 30GiB） | 单卷上限，开发可调小 |
| `--access-key` / `--secret-key` | 无 | 也可不用 env |

## `opens3 clerk`

| 参数 | 默认 | 说明 |
|------|------|------|
| `--port` | `9333` | 监听端口 |
| `--ip` | `clerk-1` | 参与 node_id：`{ip}:{port}` |
| `--default-replication` | `000` | 默认副本 |
| `--peers` | 空 | 逗号分隔 Clerk URL，Raft 成员 |
| `--raft-dir` | `./data/raft` | 持久化 term/vote/log |
| `--advertise-url` | 空（默认 `http://127.0.0.1:port`） | Raft 对端可达地址；K8s 填 Pod DNS |
| `--jwt-secret` | 无 | 签发 Volume 写令牌 |
| `--tls-cert` / `--tls-key` | 无 | HTTPS |

## `opens3 volume`

| 参数 | 默认 | 说明 |
|------|------|------|
| `--port` | `8080` | HTTP；gRPC 为 `port+10000` |
| `--ip` | `127.0.0.1` | 上报给 Clerk 的地址 |
| `--dir` | `./data` | 卷目录（`.dat`/`.idx`） |
| `--clerk` | `http://127.0.0.1:9333` | Clerk |
| `--clerk-peers` | 空 | 多 Clerk 跟随（或 `OPENS3_CLERK_PEERS`） |
| `OPENS3_VOLUME_SYNC` | `data` | Volume 刷盘：`none` / `data` / `all` |
| `OPENS3_VOLUME_IDX_FLUSH_EVERY` | `64` | idx/dat 每 N 次变更 flush（`0`=仅显式/Drop） |
| `--data-center` | `dc1` | 拓扑 DC |
| `--rack` | `rack1` | 拓扑 rack |
| `--max-volumes` | `1` | 本节点可承载卷数上限；Clerk 心跳会自动预建到该上限 |
| `--max-volume-size-mb` | 无 | 覆盖默认卷大小 |
| `--node-id` | `{ip}:{port}` | 集群内唯一 |
| `--jwt-secret` | 无 | 校验写/删 Bearer |
| `--tls-cert` / `--tls-key` | 无 | HTTPS（`publicUrl` 随之变为 https） |

## `opens3 s3` / `opens3 filer`

| 参数 | 默认 | 说明 |
|------|------|------|
| `--port` | `8333` | 监听端口（`s3`=统一网关；`filer`=仅路径 API） |
| `--clerk` | `http://127.0.0.1:9333` | Clerk |
| `--clerk-peers` | 空 | 多 Clerk 候选（或 `OPENS3_CLERK_PEERS`）；收到 307 后会 pin 到 `leader` URL，避免反复 redirect |
| `--default-replication` | `000` | 写入时 assign 副本 |
| `--meta-dir` | `./filer` | redb 元数据目录（单机；多副本勿用） |
| `--memory` | false | 等价 `--meta-backend memory` |
| `--meta-backend` | `redb` | `redb` / `memory` / `postgres` / `mysql` / `tidb` / `http` |
| `--database-url` | 无 | Postgres / MySQL / TiDB URL（或 `DATABASE_URL`） |
| `--filer-url` | 无 | 兼容：HttpStore 远程 `/store`（仅 `s3`） |
| `--encrypt` | 无 | 口令派生 AES-256-GCM 静态加密 |
| `--jwt-secret` | 无 | 向 Volume 转发/签发删除令牌 |
| `--kms-key` | 无 | 本地 SSE-KMS（`s3`） |
| `--tls-cert` / `--tls-key` | 无 | HTTPS |

Helm 将 `filer.port` / `filer.credentials` / `filer.metaBackend` 注入统一 Filer Deployment。

多实例见 [multi-filer.md](multi-filer.md)。

## 磁盘布局

```text
{volume-dir}/
  {volumeId}.dat
  {volumeId}.idx
  ec/{volumeId}.ec.{shard}

{meta-dir}/
  …（FilerStore 实现相关）

{raft-dir}/
  openraft-log.wal / openraft-sm.json / openraft-snapshot.bin …
```
