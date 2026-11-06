// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
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

#[derive(Parser, Debug)]
#[command(name = "opens3", version, about = "OpenS3 distributed object storage")]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Start clerk server
    Clerk {
        #[arg(long, default_value = "9333")]
        port: u16,
        #[arg(long, default_value = "clerk-1")]
        ip: String,
        #[arg(long, default_value = "000")]
        default_replication: String,
        /// Comma-separated peer clerk URLs for HA
        #[arg(long, default_value = "")]
        peers: String,
        #[arg(long, default_value = "./data/raft")]
        raft_dir: String,
        /// Advertised URL for Raft peers (e.g. http://opens3-clerk-0.opens3-clerk-hl:9333)
        #[arg(long, default_value = "")]
        advertise_url: String,
        #[arg(long, env = "OPENS3_VOLUME_JWT_SECRET")]
        jwt_secret: Option<String>,
        #[arg(long, env = "OPENS3_TLS_CERT")]
        tls_cert: Option<String>,
        #[arg(long, env = "OPENS3_TLS_KEY")]
        tls_key: Option<String>,
    },
    /// Start volume server
    Volume {
        #[arg(long, default_value = "8080")]
        port: u16,
        #[arg(long, default_value = "127.0.0.1")]
        ip: String,
        #[arg(long, default_value = "./data")]
        dir: String,
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
        /// Comma-separated clerk URLs for leader follow after failover
        #[arg(long, default_value = "", env = "OPENS3_CLERK_PEERS")]
        clerk_peers: String,
        #[arg(long, default_value = "dc1")]
        data_center: String,
        #[arg(long, default_value = "rack1")]
        rack: String,
        /// Max data volumes this data node may hold; clerk auto-precreates up to this on heartbeat.
        #[arg(long, default_value = "1")]
        max_volumes: u64,
        #[arg(long)]
        max_volume_size_mb: Option<u64>,
        #[arg(long, default_value = "")]
        node_id: String,
        #[arg(long, env = "OPENS3_VOLUME_JWT_SECRET")]
        jwt_secret: Option<String>,
        #[arg(long, env = "OPENS3_TLS_CERT")]
        tls_cert: Option<String>,
        #[arg(long, env = "OPENS3_TLS_KEY")]
        tls_key: Option<String>,
    },
    /// Start filer (path API / WebDAV; single gateway port)
    Filer {
        #[arg(long, default_value = "8333")]
        port: u16,
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
        #[arg(long, default_value = "000")]
        default_replication: String,
        #[arg(long, default_value = "./filer")]
        meta_dir: String,
        #[arg(long)]
        memory: bool,
        /// Meta backend: redb | memory | postgres | mysql | tidb
        #[arg(long, default_value = "redb", env = "OPENS3_META_BACKEND")]
        meta_backend: String,
        /// Postgres / MySQL / TiDB URL (`postgres://` or `mysql://`)
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,
        #[arg(long)]
        encrypt: Option<String>,
        #[arg(long, env = "OPENS3_VOLUME_JWT_SECRET")]
        jwt_secret: Option<String>,
        #[arg(long, env = "OPENS3_TLS_CERT")]
        tls_cert: Option<String>,
        #[arg(long, env = "OPENS3_TLS_KEY")]
        tls_key: Option<String>,
    },
    /// Start S3 + Filer gateway (single port)
    S3 {
        #[arg(long, default_value = "8333")]
        port: u16,
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
        #[arg(long, default_value = "000")]
        default_replication: String,
        #[arg(long, default_value = "./filer")]
        meta_dir: String,
        #[arg(long)]
        memory: bool,
        /// Meta backend: redb | memory | postgres | mysql | tidb | http
        #[arg(long, default_value = "redb", env = "OPENS3_META_BACKEND")]
        meta_backend: String,
        /// Postgres / MySQL / TiDB URL (`postgres://` or `mysql://`)
        #[arg(long, env = "DATABASE_URL")]
        database_url: Option<String>,
        /// When meta-backend=http, share metadata via remote Filer `/store` API
        #[arg(long, default_value = "")]
        filer_url: String,
        #[arg(long, env = "OPENS3_ACCESS_KEY")]
        access_key: Option<String>,
        #[arg(long, env = "OPENS3_SECRET_KEY")]
        secret_key: Option<String>,
        #[arg(long)]
        encrypt: Option<String>,
        /// Local KMS passphrase for SSE-KMS headers
        #[arg(long, env = "OPENS3_KMS_KEY")]
        kms_key: Option<String>,
        #[arg(long, env = "OPENS3_VOLUME_JWT_SECRET")]
        jwt_secret: Option<String>,
        #[arg(long, env = "OPENS3_TLS_CERT")]
        tls_cert: Option<String>,
        #[arg(long, env = "OPENS3_TLS_KEY")]
        tls_key: Option<String>,
    },
    /// Run clerk + volume + s3 in one process (dev)
    Server {
        #[arg(long, default_value = "./data")]
        dir: String,
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
    /// Simple shell helpers
    Shell {
        #[command(subcommand)]
        cmd: ShellCmd,
    },
    /// FUSE mount placeholder (use S3/Filer HTTP; native FUSE is feature-gated later)
    Fuse {
        #[arg(long, default_value = "http://127.0.0.1:8333")]
        filer: String,
        #[arg(long)]
        mountpoint: String,
    },
}

#[derive(Subcommand, Debug)]
enum ShellCmd {
    Assign {
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
        #[arg(long, default_value = "")]
        collection: String,
        #[arg(long, default_value = "000")]
        replication: String,
    },
    Lookup {
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
        volume_id: u32,
    },
    ClusterStatus {
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
    },
    VolumeList {
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
    },
    VolumeBalance {
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
    },
    Heal {
        #[arg(long, default_value = "http://127.0.0.1:9333")]
        clerk: String,
    },
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
            // Pod DNS names (e.g. opens3-clerk-0) are already unique; only bare IPs need :port.
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


            // 创建主服务器
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
            jwt_secret,
            tls_cert,
            tls_key,
        } => {
            let node_id = if node_id.is_empty() {
                format!("{ip}:{port}")
            } else {
                node_id
            };
            let max_size = max_volume_size_mb.map(|m| m * 1024 * 1024);
            let peers: Vec<String> = clerk_peers
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            let tls = opens3_common::tls::TlsConfig::from_opts(tls_cert, tls_key)?;
            VolumeServer::new_with_opts(
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
            )?
            .serve()
            .await?;
        }
        Commands::Filer {
            port,
            clerk,
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
            let backend = if memory {
                "memory".into()
            } else {
                meta_backend
            };
            let store = open_meta_store(&backend, &meta_dir, database_url.as_deref(), None).await?;
            let tls = opens3_common::tls::TlsConfig::from_opts(tls_cert, tls_key)?;
            Server::new_with_opts(
                store,
                clerk,
                default_replication,
                port,
                encrypt,
                jwt_secret,
                tls,
                None,
                None,
                Protocols::FILER,
            )
            .serve()
            .await?;
        }
        Commands::S3 {
            port,
            clerk,
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
            let ak = match (access_key, secret_key) {
                (Some(a), Some(s)) => Some((a, s)),
                _ => None,
            };
            Server::new_with_opts(
                store,
                clerk,
                default_replication,
                port,
                encrypt,
                jwt_secret,
                tls,
                ak,
                kms_key,
                Protocols::ALL,
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
            tokio::time::sleep(std::time::Duration::from_millis(300)).await;

            let max_size = max_volume_size_mb.map(|m| m * 1024 * 1024);
            let volume = VolumeServer::new_with_opts(
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
            // Prefer davfs2 when available (Filer WebDAV gateway).
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
        // TiDB is MySQL-protocol compatible; same MysqlStore + mysql:// URL.
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
            std::fs::create_dir_all(meta_dir)?;
            Ok(Arc::new(RedbStore::open(format!("{meta_dir}/meta.redb"))?))
        }
    }
}
