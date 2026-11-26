// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 可选 HTTPS 监听辅助：master / volume / filer / s3 共用。

use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

/// TLS PEM 证书与私钥路径
#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_path: PathBuf, // 证书 PEM 路径
    pub key_path: PathBuf,  // 私钥 PEM 路径
}

impl TlsConfig {
    /// 由证书/私钥路径构造
    pub fn new(cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        Self {
            cert_path: cert.into(),
            key_path: key.into(),
        }
    }

    /// 从 CLI 选项解析：二者皆空 → None；缺一不可
    pub fn from_opts(cert: Option<String>, key: Option<String>) -> anyhow::Result<Option<Self>> {
        match (cert, key) {
            (None, None) => Ok(None),
            (Some(c), Some(k)) if !c.is_empty() && !k.is_empty() => Ok(Some(Self::new(c, k))),
            _ => anyhow::bail!("--tls-cert and --tls-key must be set together"),
        }
    }

    /// 启用 TLS 时的 URL scheme
    pub fn scheme(&self) -> &'static str {
        "https"
    }
}

/// 按是否配置 TLS 返回 `https` / `http`
pub fn scheme(tls: Option<&TlsConfig>) -> &'static str {
    if tls.is_some() {
        "https"
    } else {
        "http"
    }
}

/// 安装默认 rustls crypto provider（幂等）
fn ensure_crypto_provider() {
    // 可能同时链接 ring 与 aws-lc-rs（sqlx/reqwest），需显式选定
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// 绑定 `0.0.0.0:port` 服务 `app`；可选 rustls PEM 终结
pub async fn serve_http(
    port: u16,
    app: Router,
    tls: Option<TlsConfig>,
) -> anyhow::Result<()> {
    let addr: SocketAddr = format!("0.0.0.0:{port}").parse()?;
    match tls {
        None => {
            let listener = tokio::net::TcpListener::bind(addr).await?;
            axum::serve(listener, app).await?;
        }
        Some(tls) => {
            ensure_crypto_provider();
            let config = load_rustls(&tls.cert_path, &tls.key_path).await?;
            axum_server::bind_rustls(addr, config)
                .serve(app.into_make_service())
                .await?;
        }
    }
    Ok(())
}

/// 从 PEM 文件加载 RustlsConfig
async fn load_rustls(cert: &Path, key: &Path) -> anyhow::Result<RustlsConfig> {
    RustlsConfig::from_pem_file(cert, key)
        .await
        .map_err(|e| anyhow::anyhow!("load TLS cert/key: {e}"))
}
