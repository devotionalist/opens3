# OpenS3

用 Rust 实现的分布式对象存储：Clerk（Raft）分配卷、Volume 追加存储与同步副本、统一 Filer 网关（S3 + HTTP/WebDAV）与共享元数据。

**License:** [Apache License 2.0](LICENSE)

## 架构拓扑

Admin 在线拓扑（客户端 → Filer → Meta / Volume，Clerk Raft 在顶行）：

![OpenS3 topology](docs/images/topology.jpg)

部署后打开 `http://<filer>:8333/admin/ui` 可查看实时拓扑。更多说明见 [架构](docs/architecture.md) 与 [客户端与网关](docs/gateways.md)。

## 文档

完整使用说明见 **[docs/](docs/README.md)**：

| 文档 | 内容 |
|------|------|
| [快速开始](docs/quickstart.md) | 编译、一键启动、curl / aws-cli |
| [安装构建](docs/install.md) | 依赖与发布 |
| [架构](docs/architecture.md) | 系统图、读写路径、crate 地图 |
| [客户端与网关](docs/gateways.md) | S3 / Filer HTTP / WebDAV / FUSE |
| [模块设计](docs/modules/README.md) | Clerk / Volume / 统一 Filer 等职责 |
| [部署](docs/deployment.md) | 多 Volume、Clerk HA、systemd |
| [Kubernetes](docs/kubernetes.md) | Helm Chart / Kustomize 清单 |
| [配置参考](docs/configuration.md) | CLI / 环境变量 / 目录布局 |
| [S3 使用](docs/s3.md) | 鉴权、策略、生命周期、分片上传 |
| [运维](docs/operations.md) | heal、balance、metrics、EC |
| [监控](docs/monitoring.md) | Prometheus / Grafana / 告警 |
| [故障排查](docs/troubleshooting.md) | 症状表与演练 |
| [开发测试](docs/development.md) | 仓库结构与测试 |

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
