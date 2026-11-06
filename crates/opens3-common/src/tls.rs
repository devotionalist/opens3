// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Optional HTTPS listener helper shared by master / volume / filer / s3.

use axum::Router;
use axum_server::tls_rustls::RustlsConfig;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct TlsConfig {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
}

impl TlsConfig {
    pub fn new(cert: impl Into<PathBuf>, key: impl Into<PathBuf>) -> Self {
        Self {
            cert_path: cert.into(),
            key_path: key.into(),
        }
    }

    pub fn from_opts(cert: Option<String>, key: Option<String>) -> anyhow::Result<Option<Self>> {
        match (cert, key) {
            (None, None) => Ok(None),
            (Some(c), Some(k)) if !c.is_empty() && !k.is_empty() => Ok(Some(Self::new(c, k))),
            _ => anyhow::bail!("--tls-cert and --tls-key must be set together"),
        }
    }

    pub fn scheme(&self) -> &'static str {
        "https"
    }
}

pub fn scheme(tls: Option<&TlsConfig>) -> &'static str {
    if tls.is_some() {
        "https"
    } else {
        "http"
    }
}

fn ensure_crypto_provider() {
    // Multiple rustls backends may be linked (ring + aws-lc-rs via sqlx/reqwest).
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Bind `0.0.0.0:port` and serve `app`, with optional rustls PEM termination.
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

async fn load_rustls(cert: &Path, key: &Path) -> anyhow::Result<RustlsConfig> {
    RustlsConfig::from_pem_file(cert, key)
        .await
        .map_err(|e| anyhow::anyhow!("load TLS cert/key: {e}"))
}
