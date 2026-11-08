# OpenS3

> Language: [中文](#opens3) · [English](#opens3-en)

用 Rust 实现的分布式对象存储：Clerk（Raft）分配卷、Volume 追加存储与同步副本、统一 Filer 网关（S3 + HTTP/WebDAV）与共享元数据。

**License:** [Apache License 2.0](LICENSE)

## 架构拓扑

Admin 在线拓扑（客户端 → Filer → Meta / Volume，Clerk Raft 在顶行）：

![OpenS3 topology](docs/images/topology.jpg)

部署后打开 `http://<filer>:8333/admin/ui` 可查看实时拓扑。更多说明见 [架构](docs/architecture.md) 与 [客户端与网关](docs/gateways.md)。

## 文档

| 语言 | 入口 |
|------|------|
| 中文 | **[docs/](docs/README.md)** |
| English | **[docs/en/](docs/en/README.md)** |

| 文档 | 中文 | English |
|------|------|---------|
| 快速开始 | [quickstart](docs/quickstart.md) | [EN](docs/en/quickstart.md) |
| 安装构建 | [install](docs/install.md) | [EN](docs/en/install.md) |
| 架构 | [architecture](docs/architecture.md) | [EN](docs/en/architecture.md) |
| 客户端与网关 | [gateways](docs/gateways.md) | [EN](docs/en/gateways.md) |
| 模块设计 | [modules](docs/modules/README.md) | [EN](docs/en/modules/README.md) |
| 部署 | [deployment](docs/deployment.md) | [EN](docs/en/deployment.md) |
| Kubernetes | [kubernetes](docs/kubernetes.md) | [EN](docs/en/kubernetes.md) |
| 配置参考 | [configuration](docs/configuration.md) | [EN](docs/en/configuration.md) |
| S3 使用 | [s3](docs/s3.md) | [EN](docs/en/s3.md) |
| 运维 | [operations](docs/operations.md) | [EN](docs/en/operations.md) |
| 监控 | [monitoring](docs/monitoring.md) | [EN](docs/en/monitoring.md) |
| 故障排查 | [troubleshooting](docs/troubleshooting.md) | [EN](docs/en/troubleshooting.md) |
| 开发测试 | [development](docs/development.md) | [EN](docs/en/development.md) |

## 快速体验

```bash
cargo build --release
./target/release/opens3 server --dir ./data --max-volume-size-mb 64

# S3:    http://127.0.0.1:8333
# Admin: http://127.0.0.1:8333/admin/ui
# Clerk: http://127.0.0.1:9333
```

```bash
curl -X PUT http://127.0.0.1:8333/demo
echo hello | curl -X PUT http://127.0.0.1:8333/demo/hello.txt --data-binary @-
curl http://127.0.0.1:8333/demo/hello.txt
```

生产请开启鉴权并分进程部署，参见 [部署](docs/deployment.md) 与 [S3 鉴权](docs/s3.md)。

## 特性摘要

- 热路径 **卷级多副本**（强一致写），冷卷 **异步 EC**
- Clerk **持久化 Raft**（RequestVote / AppendEntries）
- 缺副本 **heal**、容量 **balance**、Prometheus **metrics**
- S3：SigV4、Multipart、Versioning、Lifecycle、Bucket Policy
- Apache-2.0 开源

## 测试

```bash
cargo test --workspace
bash tests/e2e_fault.sh
bash tests/e2e_kill_replica.sh
bash tests/e2e_clerk_failover.sh
bash tests/e2e_seal_ec.sh
bash tests/e2e_multi_s3.sh
bash tests/s3_compat/run.sh
```

---

<a id="opens3-en"></a>

# OpenS3 (English)

Distributed object storage in Rust: Clerk (Raft) assigns volumes, Volume appends with sync replicas, unified Filer gateway (S3 + HTTP/WebDAV) with shared metadata.

**Docs:** [Chinese](docs/README.md) · [English](docs/en/README.md)

```bash
cargo build --release
./target/release/opens3 server --dir ./data --max-volume-size-mb 64
# S3 http://127.0.0.1:8333 · Admin /admin/ui · Clerk :9333
```

See [docs/en/](docs/en/README.md) for full English documentation.