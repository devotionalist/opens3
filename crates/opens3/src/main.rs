// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! OpenS3 统一 CLI：clerk / volume / filer / s3 / 一体机 / shell / fuse 入口。

use anyhow::Result;
use clap::{Parser, Subcommand};
use opens3_common::replication::Replication;
use opens3_clerk::ClerkServer;
use opens3_filer::{Protocols, Server};
use opens3_store::{HttpStore, MemoryStore, MysqlStore, PostgresStore, RedbStore};
use opens3_volume::VolumeServer;
use std::str::FromStr;
use std::sync::Arc;
use tracing::info;

/// 顶层命令行：子命令分发到各角色进程。
#[derive(Parser, Debug)]
#[command(name = "opens3", version, about = "OpenS3 distributed object storage")]
struct Cli {
    #[command(subcommand)]
    cmd: Commands, // 实际运行的子命令
}

/// 可启动的角色与运维辅助命令。
#[derive(Subcommand, Debug)]
enum Commands {
    /// 启动 Clerk（拓扑 / 分配 / Raft）
    Clerk {
        #[arg(long, default_value = "9333")]
        port: u16, // 监听端口
        #[arg(long, default_value = "clerk-1")]
        ip: String, // 节点标识或绑定 IP
        #[arg(long, default_value = "000")]
        default_replication: String, // 默认 XYZ 副本策略
        /// HA 用的逗号分隔 peer Clerk URL
        #[arg(long, default_value = "")]
        peers: String,
        #[arg(long, default_value = "./data/raft")]
        raft_dir: String, // Raft 持久化目录
        /// 对 Raft peers 通告的 URL（如 http://opens3-clerk-0.opens3-clerk-hl:9333）
        #[arg(long, default_value = "")]
        advertise_url: String,
        #[arg(long, env = "OPENS3_VOLUME_JWT_SECRET")]
        jwt_secret: Option<String>, // Volume JWT 密钥
        #[arg(long, env = "OPENS3_TLS_CERT")]
        tls_cert: Option<String>,
        #[arg(long, env = "OPENS3_TLS_KEY")]
        tls_key: Option<String>,
    },
    /// 启动 Volume（数据节点）
    Volume {
        #[arg(long, default_value = "8080")]
        port: u16,
        #[arg(long, default_value = "127.0.0.1")]
        ip: String,
        #[arg(long, default_value = "./data")]
        dir: String, // 本地 .dat/.idx 根目录
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String, // 主 Clerk URL
        /// 逗号分隔 Clerk URL，故障切换后跟随新 leader
        #[arg(long, default_value = "", env = "OPENS3_CLERK_PEERS")]
        clerk_peers: String,
        #[arg(long, default_value = "dc1")]
        data_center: String,
        #[arg(long, default_value = "rack1")]
        rack: String,
        /// 本节点可持有的最大数据卷数；心跳时 Clerk 可预创建到此上限
        #[arg(long, default_value = "1")]
        max_volumes: u64,
        #[arg(long)]
        max_volume_size_mb: Option<u64>, // 单卷大小上限（MB）
        #[arg(long, default_value = "")]
        node_id: String, // 空则用 ip:port
        /// 首次向 Clerk 注册时即上线（Helm 部署默认）
        #[arg(long, default_value_t = false, env = "OPENS3_VOLUME_START_ONLINE")]
        start_online: bool,
        /// 首次注册时作为该节点 id / pod 名的副本（仍可先离线）
        #[arg(long, default_value = "", env = "OPENS3_VOLUME_REPLICA_OF")]
        replica_of: String,
        /// 按 ordinal 的 bootstrap JSON 目录（`{ordinal}` → `{"online":true,"replicaOf":"..."}`）
        /// 缺文件时（Web/STS 扩容）保持离线，等 Admin 操作
        #[arg(long, default_value = "", env = "OPENS3_VOLUME_BOOTSTRAP_DIR")]
        bootstrap_dir: String,
        #[arg(long, env = "OPENS3_VOLUME_JWT_SECRET")]
        jwt_secret: Option<String>,
        #[arg(long, env = "OPENS3_TLS_CERT")]
        tls_cert: Option<String>,
        #[arg(long, env = "OPENS3_TLS_KEY")]
        tls_key: Option<String>,
    },
    /// 启动 Filer（路径 API / WebDAV；单网关端口）
    Filer {
        #[arg(long, default_value = "8333")]
        port: u16,
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
        /// 逗号分隔 Clerk URL；找存活节点 / 故障后钉住 Raft leader
        #[arg(long, default_value = "", env = "OPENS3_CLERK_PEERS")]
        clerk_peers: String,
        #[arg(long, default_value = "000")]
        default_replication: String,
        #[arg(long, default_value = "./filer")]
        meta_dir: String, // redb 等本地 meta 目录
        #[arg(long)]
        memory: bool, // true 时强制 memory 后端
        /// Meta 后端：redb | memory | postgres | mysql | tidb
        #[arg(long, default_value = "redb", env = "OPENS3_META_BACKEND")]
        meta_backend: String,
        /// Postgres / MySQL / TiDB URL（`postgres://` 或 `mysql://`）
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,
        #[arg(long)]
        encrypt: Option<String>, // 可选落盘加密密钥
        #[arg(long, env = "OPENS3_VOLUME_JWT_SECRET")]
        jwt_secret: Option<String>,
        #[arg(long, env = "OPENS3_TLS_CERT")]
        tls_cert: Option<String>,
        #[arg(long, env = "OPENS3_TLS_KEY")]
        tls_key: Option<String>,
    },
    /// 启动 S3 + Filer 网关（单端口）
    S3 {
        #[arg(long, default_value = "8333")]
        port: u16,
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
        /// 逗号分隔 Clerk URL；找存活节点 / 故障后钉住 Raft leader
        #[arg(long, default_value = "", env = "OPENS3_CLERK_PEERS")]
        clerk_peers: String,
        #[arg(long, default_value = "000")]
        default_replication: String,
        #[arg(long, default_value = "./filer")]
        meta_dir: String,
        #[arg(long)]
        memory: bool,
        /// Meta 后端：redb | memory | postgres | mysql | tidb | http
        #[arg(long, default_value = "redb", env = "OPENS3_META_BACKEND")]
        meta_backend: String,
        /// Postgres / MySQL / TiDB URL（`postgres://` 或 `mysql://`）
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,
        /// meta-backend=http 时，经远程 Filer `/store` 共享元数据
        #[arg(long, default_value = "")]
        filer_url: String,
        #[arg(long, env = "OPENS3_ACCESS_KEY")]
        access_key: Option<String>,
        #[arg(long, env = "OPENS3_SECRET_KEY")]
        secret_key: Option<String>,
        #[arg(long)]
        encrypt: Option<String>,
        /// SSE-KMS 本地口令
        #[arg(long, env = "OPENS3_KMS_KEY")]
        kms_key: Option<String>,
        #[arg(long, env = "OPENS3_VOLUME_JWT_SECRET")]
        jwt_secret: Option<String>,
        #[arg(long, env = "OPENS3_TLS_CERT")]
        tls_cert: Option<String>,
        #[arg(long, env = "OPENS3_TLS_KEY")]
        tls_key: Option<String>,
    },
    /// 单进程跑 clerk + volume + s3（开发用）
    Server {
        #[arg(long, default_value = "./data")]
        dir: String, // 数据 / filer / raft 父目录
        #[arg(long, default_value = "9333")]
        clerk_port: u16,
        #[arg(long, default_value = "8080")]
        volume_port: u16,
        #[arg(long, default_value = "8333")]
        gateway_port: u16,
        #[arg(long, default_value = "000")]
        default_replication: String,
        #[arg(long, env = "OPENS3_ACCESS_KEY")]
        access_key: Option<String>,
        #[arg(long, env = "OPENS3_SECRET_KEY")]
        secret_key: Option<String>,
        #[arg(long)]
        max_volume_size_mb: Option<u64>,
        #[arg(long, env = "OPENS3_VOLUME_JWT_SECRET")]
        jwt_secret: Option<String>,
        #[arg(long, env = "OPENS3_TLS_CERT")]
        tls_cert: Option<String>,
        #[arg(long, env = "OPENS3_TLS_KEY")]
        tls_key: Option<String>,
    },
    /// 简易运维 HTTP 辅助
    Shell {
        #[command(subcommand)]
        cmd: ShellCmd,
    },
    /// FUSE 挂载占位（优先 WebDAV；原生 FUSE 后续再开 feature）
    Fuse {
        #[arg(long, default_value = "http://127.0.0.1:8333")]
        filer: String, // Filer/网关根 URL
        #[arg(long)]
        mountpoint: String, // 本地挂载点
    },
}

/// `opens3 shell` 子命令：直打 Clerk HTTP。
#[derive(Subcommand, Debug)]
enum ShellCmd {
    /// 申请 file id / volume 位置
    Assign {
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
        #[arg(long, default_value = "")]
        collection: String,
        #[arg(long, default_value = "000")]
        replication: String,
    },
    /// 按 volume_id 查副本位置
    Lookup {
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
        volume_id: u32,
    },
    /// 集群状态
    ClusterStatus {
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
    },
    /// 列出卷
    VolumeList {
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
    },
    /// 触发卷平衡
    VolumeBalance {
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
    },
    /// 触发 heal
    Heal {
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
    },
    /// Raft 状态
    RaftStatus {
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Commands::Clerk {
            port,
            ip,
            default_replication,
            peers,
            raft_dir,
            advertise_url,
            jwt_secret,
            tls_cert,
            tls_key,
        } => {
            let repl = Replication::from_str(&default_replication)?;
            let peers: Vec<String> = peers
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            // Pod DNS 名（如 opens3-clerk-0）已唯一；裸 IP 才拼 :port
            let node_id = if ip.parse::<std::net::IpAddr>().is_ok() {
                format!("{ip}:{port}")
            } else {
                ip.clone()
            };
            let tls = opens3_common::tls::TlsConfig::from_opts(tls_cert, tls_key)?;
            let adv = if advertise_url.is_empty() {
                None
            } else {
                Some(advertise_url)
            };


            // 启动 Clerk 并阻塞服务
            ClerkServer::new_with_opts(
                node_id,
                port,
                peers,
                repl,
                raft_dir.into(),
                adv,
                jwt_secret,
                tls,
            )
            .await?
            .serve()
            .await?;
        }
        Commands::Volume {
            port,
            ip,
            dir,
            clerk,
            clerk_peers,
            data_center,
            rack,
            max_volumes,
            max_volume_size_mb,
            node_id,
            start_online,
            replica_of,
            bootstrap_dir,
            jwt_secret,
            tls_cert,
            tls_key,
        } => {
            let node_id = if node_id.is_empty() {
                format!("{ip}:{port}")
            } else {
                node_id
            };
            let max_size = max_volume_size_mb.map(|m| m * 1024 * 1024); // MB → 字节
            let peers: Vec<String> = clerk_peers
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let tls = opens3_common::tls::TlsConfig::from_opts(tls_cert, tls_key)?;
            let boot = load_volume_bootstrap(&node_id, &bootstrap_dir, start_online, &replica_of);
            VolumeServer::new_with_bootstrap(
                node_id,
                dir,
                clerk,
                peers,
                ip,
                port,
                data_center,
                rack,
                max_volumes,
                max_size,
                jwt_secret,
                tls,
                boot.start_online,
                boot.replica_of,
            )?
            .serve()
            .await?;
        }
        Commands::Filer {
            port,
            clerk,
            clerk_peers,
            default_replication,
            meta_dir,
            memory,
            meta_backend,
            database_url,
            encrypt,
            jwt_secret,
            tls_cert,
            tls_key,
        } => {
            // --memory 覆盖 meta-backend
            let backend = if memory {
                "memory".into()
            } else {
                meta_backend
            };
            let store = open_meta_store(&backend, &meta_dir, database_url.as_deref(), None).await?;
            let tls = opens3_common::tls::TlsConfig::from_opts(tls_cert, tls_key)?;
            let peers: Vec<String> = clerk_peers
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            Server::new_with_clerk_peers(
                store,
                clerk,
                peers,
                default_replication,
                port,
                encrypt,
                jwt_secret,
                tls,
                None,
                None,
                Protocols::FILER, // 仅 Filer/WebDAV，不含 S3
            )
            .serve()
            .await?;
        }
        Commands::S3 {
            port,
            clerk,
            clerk_peers,
            default_replication,
            meta_dir,
            memory,
            meta_backend,
            database_url,
            filer_url,
            access_key,
            secret_key,
            encrypt,
            kms_key,
            jwt_secret,
            tls_cert,
            tls_key,
        } => {
            // 优先级：--memory > --filer-url > --meta-backend
            let backend = if memory {
                "memory".into()
            } else if !filer_url.is_empty() {
                "http".into()
            } else {
                meta_backend
            };
            let store = open_meta_store(
                &backend,
                &meta_dir,
                database_url.as_deref(),
                if filer_url.is_empty() {
                    None
                } else {
                    Some(filer_url.as_str())
                },
            )
            .await?;
            let tls = opens3_common::tls::TlsConfig::from_opts(tls_cert, tls_key)?;
            // 必须成对提供 AK/SK，否则不加身份
            let ak = match (access_key, secret_key) {
                (Some(a), Some(s)) => Some((a, s)),
                _ => None,
            };
            let peers: Vec<String> = clerk_peers
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            Server::new_with_clerk_peers(
                store,
                clerk,
                peers,
                default_replication,
                port,
                encrypt,
                jwt_secret,
                tls,
                ak,
                kms_key,
                Protocols::ALL, // S3 + Filer
            )
            .serve()
            .await?;
        }
        Commands::Server {
            dir,
            clerk_port,
            volume_port,
            gateway_port,
            default_replication,
            access_key,
            secret_key,
            max_volume_size_mb,
            jwt_secret,
            tls_cert,
            tls_key,
        } => {
            // 一体机：先 Clerk，再 Volume，最后网关（同进程）
            let repl = Replication::from_str(&default_replication)?;
            let tls = opens3_common::tls::TlsConfig::from_opts(tls_cert, tls_key)?;
            let scheme = opens3_common::tls::scheme(tls.as_ref());
            let clerk_url = format!("{scheme}://127.0.0.1:{clerk_port}");
            let data_dir = format!("{dir}/volumes");
            let meta_dir = format!("{dir}/filer");
            std::fs::create_dir_all(&data_dir)?;
            std::fs::create_dir_all(&meta_dir)?;

            let raft_dir = format!("{dir}/raft");
            std::fs::create_dir_all(&raft_dir)?;
            let clerk = ClerkServer::new_with_opts(
                format!("127.0.0.1:{clerk_port}"),
                clerk_port,
                vec![],
                repl,
                raft_dir.into(),
                Some(format!("{scheme}://127.0.0.1:{clerk_port}")),
                jwt_secret.clone(),
                tls.clone(),
            )
            .await?;
            tokio::spawn(async move {
                if let Err(e) = clerk.serve().await {
                    tracing::error!("clerk: {e}");
                }
            });
            // 给 Raft / HTTP 一点启动时间
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;

            let max_size = max_volume_size_mb.map(|m| m * 1024 * 1024);
            let volume = VolumeServer::new_with_bootstrap(
                format!("127.0.0.1:{volume_port}"),
                data_dir,
                clerk_url.clone(),
                vec![],
                "127.0.0.1".into(),
                volume_port,
                "dc1".into(),
                "rack1".into(),
                16,
                max_size,
                jwt_secret.clone(),
                tls.clone(),
                true, // 一体机默认上线
                None,
            )?;
            tokio::spawn(async move {
                if let Err(e) = volume.serve().await {
                    tracing::error!("volume: {e}");
                }
            });
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;

            let store: Arc<dyn opens3_store::FilerStore> =
                Arc::new(RedbStore::open(format!("{meta_dir}/meta.redb"))?);
            let ak = match (access_key, secret_key) {
                (Some(a), Some(s)) => Some((a, s)),
                _ => None,
            };
            info!("OpenS3 all-in-one ready: clerk={clerk_port} volume={volume_port} gateway={gateway_port}");
            Server::new_with_opts(
                store,
                clerk_url,
                default_replication,
                gateway_port,
                None,
                jwt_secret,
                tls,
                ak,
                None,
                Protocols::ALL,
            )
            .serve()
            .await?;
        }
        Commands::Fuse { filer, mountpoint } => {
            let filer = filer.trim_end_matches('/');
            let dav = format!("{filer}/dav");
            std::fs::create_dir_all(&mountpoint)?;
            // 优先 davfs2（Filer WebDAV 网关）
            let davfs = ["mount.davfs", "davfs2"]
                .into_iter()
                .find(|b| std::process::Command::new("which").arg(b).output().ok().map(|o| o.status.success()).unwrap_or(false));
            if let Some(bin) = davfs {
                let status = std::process::Command::new(bin)
                    .args([&dav, &mountpoint])
                    .status()?;
                if status.success() {
                    println!("mounted WebDAV {dav} -> {mountpoint}");
                    return Ok(());
                }
            }
            // 未链接原生 FUSE 时给出替代挂载说明
            anyhow::bail!(
                "native kernel FUSE is not linked. Use a gateway mount:\n\n\
                 # WebDAV (Filer)\n\
                 mkdir -p {mountpoint}\n\
                 sudo mount -t davfs {dav} {mountpoint}\n\
                 # or: open {dav} in Cyberduck / Finder\n\n\
                 # S3 (stateless gateway)\n\
                 s3fs <bucket> {mountpoint} -o url=http://127.0.0.1:8333,use_path_request_style\n\
                 # or: goofys --endpoint http://127.0.0.1:8333 <bucket> {mountpoint}\n\n\
                 # Filer HTTP\n\
                 curl -T ./file {filer}/entries/path/file\n\
                 curl {filer}/entries/path/file\n\
                 curl {filer}/list/\n"
            );
        }
        Commands::Shell { cmd } => match cmd {
            ShellCmd::Assign {
                clerk,
                collection,
                replication,
            } => {
                let url = format!(
                    "{}/dir/assign?collection={collection}&replication={replication}",
                    clerk.trim_end_matches('/')
                );
                let body = reqwest::get(url).await?.text().await?;
                println!("{body}");
            }
            ShellCmd::Lookup { clerk, volume_id } => {
                let url = format!("{}/dir/lookup/{volume_id}", clerk.trim_end_matches('/'));
                let body = reqwest::get(url).await?.text().await?;
                println!("{body}");
            }
            ShellCmd::ClusterStatus { clerk } => {
                let url = format!("{}/cluster/status", clerk.trim_end_matches('/'));
                let body = reqwest::get(url).await?.text().await?;
                println!("{body}");
            }
            ShellCmd::VolumeList { clerk } => {
                let url = format!("{}/vol/list", clerk.trim_end_matches('/'));
                let body = reqwest::get(url).await?.text().await?;
                println!("{body}");
            }
            ShellCmd::VolumeBalance { clerk } => {
                let url = format!("{}/cluster/balance", clerk.trim_end_matches('/'));
                let body = reqwest::Client::new().post(url).send().await?.text().await?;
                println!("{body}");
            }
            ShellCmd::Heal { clerk } => {
                let url = format!("{}/cluster/heal", clerk.trim_end_matches('/'));
                let body = reqwest::Client::new().post(url).send().await?.text().await?;
                println!("{body}");
            }
            ShellCmd::RaftStatus { clerk } => {
                let url = format!("{}/raft/status", clerk.trim_end_matches('/'));
                let body = reqwest::get(url).await?.text().await?;
                println!("{body}");
            }
        },
    }
    Ok(())
}

/// 按 backend 名打开对应 FilerStore；默认落 redb。
async fn open_meta_store(
    backend: &str,
    meta_dir: &str,
    database_url: Option<&str>,
    filer_url: Option<&str>,
) -> Result<Arc<dyn opens3_store::FilerStore>> {
    match backend {
        "memory" => Ok(Arc::new(MemoryStore::new())),
        "postgres" => {
            let url = database_url.ok_or_else(|| {
                anyhow::anyhow!("postgres meta-backend requires --database-url / DATABASE_URL")
            })?;
            Ok(Arc::new(PostgresStore::connect(url).await?))
        }
        // TiDB 走 MySQL 协议；同一 MysqlStore + mysql://
        "mysql" | "tidb" => {
            let url = database_url.ok_or_else(|| {
                anyhow::anyhow!("{backend} meta-backend requires --database-url / DATABASE_URL")
            })?;
            Ok(Arc::new(MysqlStore::connect(url).await?))
        }
        "http" => {
            let url = filer_url.ok_or_else(|| {
                anyhow::anyhow!("http meta-backend requires --filer-url")
            })?;
            Ok(Arc::new(HttpStore::new(url)))
        }
        _ => {
            // redb 及其他未知名 → 本地文件
            std::fs::create_dir_all(meta_dir)?;
            Ok(Arc::new(RedbStore::open(format!("{meta_dir}/meta.redb"))?))
        }
    }
}

/// Volume 首次注册时的上线 / 副本配对意图。
struct VolumeBootstrap {
    start_online: bool,              // 是否立即 online
    replica_of: Option<String>,      // 作为谁的副本
}

/// 合并 CLI/env 与 Helm ConfigMap 中按 ordinal 的 bootstrap JSON。
///
/// 优先级：显式 `--start-online` / `--replica-of` 覆盖文件。若设置了
/// `bootstrap_dir` 但 ordinal 文件缺失（Web 扩容），强制离线以便 Admin UI 配对。
fn load_volume_bootstrap(
    node_id: &str,
    bootstrap_dir: &str,
    cli_start_online: bool,
    cli_replica_of: &str,
) -> VolumeBootstrap {
    let cli_replica = {
        let t = cli_replica_of.trim();
        if t.is_empty() {
            None
        } else {
            Some(t.to_string())
        }
    };

    let dir = bootstrap_dir.trim();
    if dir.is_empty() {
        // 无 bootstrap 目录：完全听 CLI
        return VolumeBootstrap {
            start_online: cli_start_online,
            replica_of: cli_replica,
        };
    }

    // StatefulSet pod 名末段作 ordinal，如 opens3-volume-2 → "2"
    let ordinal = node_id.rsplit('-').next().unwrap_or("");
    let path = std::path::Path::new(dir).join(ordinal);
    if !path.is_file() {
        tracing::info!(
            %node_id,
            ordinal,
            "volume bootstrap file missing — starting offline for Admin ops"
        );
        return VolumeBootstrap {
            start_online: false, // 缺文件 → 离线等 Admin
            replica_of: cli_replica,
        };
    }

    let mut start_online = cli_start_online;
    let mut replica_of = cli_replica;
    if let Ok(bytes) = std::fs::read(&path) {
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&bytes) {
            // CLI 未要求上线时，才读文件里的 online
            if !cli_start_online {
                if let Some(o) = v.get("online").and_then(|x| x.as_bool()) {
                    start_online = o;
                }
            }
            // CLI 未指定 replica 时，接受 replicaOf / replica_of
            if replica_of.is_none() {
                let src = v
                    .get("replicaOf")
                    .or_else(|| v.get("replica_of"))
                    .and_then(|x| x.as_str())
                    .map(str::trim)
                    .filter(|s| !s.is_empty())
                    .map(|s| s.to_string());
                replica_of = src;
            }
        }
    }
    VolumeBootstrap {
        start_online,
        replica_of,
    }
}
