# OpenS3 文档

OpenS3 是用 Rust 实现的分布式对象存储：Clerk（Raft）分配卷、Volume 顺序追加与副本、统一 Filer 网关（S3 + HTTP/WebDAV）与共享元数据。

## 入门与运维

| 文档 | 说明 |
|------|------|
| [快速开始](quickstart.md) | 五分钟跑通 all-in-one 与 aws-cli |
| [安装构建](install.md) | 依赖、编译、发布产物 |
| [部署](deployment.md) | 单机、多 Volume、Clerk HA |
| [Kubernetes](kubernetes.md) | Helm / Kustomize / 扩容与验证 |
| [配置参考](configuration.md) | CLI 参数与环境变量 |
| [运维](operations.md) | heal、balance、metrics、EC、compact |
| [监控](monitoring.md) | Prometheus 指标、Grafana、告警、ServiceMonitor |

## 架构与模块

| 文档 | 说明 |
|------|------|
| [架构总览](architecture.md) | 系统图、数据模型、读写路径、crate 依赖 |
| [模块文档](modules/README.md) | 各 crate 职责与内部设计 |
| → [Clerk](modules/clerk.md) | 拓扑、assign、Raft、heal/balance |
| → [Volume](modules/volume.md) | Needle、复制、compact、EC |
| → [Needle](modules/needle.md) | `.dat` / `.idx` 字节布局 |
| → [Filer](modules/filer.md) | 统一网关：S3 + 路径 API、分片、加密 |
| → [S3](modules/s3.md) | S3 路由、SigV4、策略、生命周期（同 Filer crate） |
| → [Store](modules/store.md) | FilerStore：redb / postgres / mysql·tidb / http |
| → [Common](modules/common.md) / [Proto](modules/proto.md) / [CLI](modules/cli.md) | 共享类型、gRPC、入口 |
| [客户端与网关](gateways.md) | 协议入口与 Helm 统一 Filer |

## S3 与开发

| 文档 | 说明 |
|------|------|
| [S3 使用](s3.md) | API、鉴权、策略、生命周期、版本 |
| [S3 兼容矩阵](s3-compat.md) | API 覆盖与冒烟脚本 |
| [多实例元数据](multi-filer.md) | Postgres / MySQL 多 Filer 副本 |
| [开发测试](development.md) | 仓库结构、测试、贡献 |
| [测试覆盖报告](test-coverage.md) | 单测/E2E 清单与最近跑通结果 |

协议：[Apache License 2.0](../LICENSE)
