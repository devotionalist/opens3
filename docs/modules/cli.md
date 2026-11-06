# CLI（`opens3`）

## 作用

二进制入口 crate，把各服务 crate 组装成可运行进程。不实现存储逻辑，只做：

- 参数解析（`clap`）
- 选择元数据后端并构造统一 `Server`（Filer）
- 单组件启动或 **all-in-one**（`server`）同进程拉起 Clerk + Volume + Filer
- 简易 shell（assign / lookup 等运维命令）

## 位置

`crates/opens3/src/main.rs`

## 子命令

| 命令 | 作用 |
|------|------|
| `clerk` | 启动 Clerk（Raft 目录、peers、advertise-url） |
| `volume` | 启动 Volume（数据目录、拓扑标签、max-volumes） |
| `s3` | **统一 Filer**：S3 + `/entries` + `/dav` + Admin（Protocols::ALL） |
| `filer` | 仅路径 API / WebDAV（Protocols::FILER） |
| `server` | 本地开发：Clerk + Volume + 统一 Filer 同进程 |
| `shell` | 轻量运维：assign、lookup 等 |
| `fuse` | 占位（引导用 S3/WebDAV 挂载） |

## 设计要点

- **advertise-url**：K8s StatefulSet 下 Raft 必须用稳定 DNS，不能只用 `127.0.0.1`。  
- **meta-backend**：`redb` | `memory` | `postgres` | `mysql` | `tidb` | `http`（共享元数据见 [multi-filer](../multi-filer.md)）。  
- Helm / 生产用 `opens3 s3` 跑统一网关；`opens3 filer` 仅路径场景。  
- all-in-one 适合开发与冒烟；生产按组件分进程 / Pod 部署。

## 依赖

`opens3-clerk`、`opens3-volume`、`opens3-filer`、`opens3-store`、`opens3-common`
