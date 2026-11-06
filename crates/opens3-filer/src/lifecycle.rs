// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Bucket lifecycle: expire current / noncurrent objects by age.

use chrono::{Duration, Utc};
use opens3_common::types::EntryMeta;
use opens3_store::FilerStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LifecycleConfig {
    #[serde(rename = "Rule", default)]
    pub rules: Vec<LifecycleRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRule {
    #[serde(rename = "ID", default)]
    pub id: String,
    #[serde(rename = "Status", default = "default_enabled")]
    pub status: String,
    #[serde(rename = "Filter", default)]
    pub filter: LifecycleFilter,
    #[serde(rename = "Expiration", default)]
    pub expiration: Option<Expiration>,
    #[serde(rename = "NoncurrentVersionExpiration", default)]
    pub noncurrent: Option<NoncurrentExpiration>,
}

fn default_enabled() -> String {
    "Enabled".into()
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LifecycleFilter {
    #[serde(rename = "Prefix", default)]
    pub prefix: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Expiration {
    #[serde(rename = "Days")]
    pub days: Option<i64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NoncurrentExpiration {
    #[serde(rename = "NoncurrentDays")]
    pub noncurrent_days: Option<i64>,
}

pub async fn apply_bucket_lifecycle(
    store: &Arc<dyn FilerStore>,
    bucket: &str,
    cfg: &LifecycleConfig,
    delete_fn: impl Fn(String) -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
) -> usize {
    let mut deleted = 0usize;
    let prefix = format!("/buckets/{bucket}/");
    let listed = store.list_by_prefix(&prefix, 100_000).await.unwrap_or_default();
    let now = Utc::now();
    for (path, meta) in listed {
        let key = path.strip_prefix(&prefix).unwrap_or(&path);
        for rule in &cfg.rules {
            if !rule.status.eq_ignore_ascii_case("Enabled") {
                continue;
            }
            if !key.starts_with(&rule.filter.prefix) {
                continue;
            }
            if let Some(exp) = &rule.expiration {
                if let Some(days) = exp.days {
                    if now - meta.mtime > Duration::days(days) && should_expire(&meta) {
                        delete_fn(path.clone()).await;
                        deleted += 1;
                        info!("lifecycle expired {path} rule={}", rule.id);
                        break;
                    }
                }
            }
        }
    }
    deleted
}

fn should_expire(meta: &EntryMeta) -> bool {
    !meta.is_directory && !meta.delete_marker
}

pub fn parse_lifecycle_xml(xml: &str) -> LifecycleConfig {
    // Minimal XML scrape for Days/Prefix/Status — full XML parser optional
    let mut cfg = LifecycleConfig::default();
    if xml.contains("<Rule>") {
        let days = xml
            .split("<Days>")
            .nth(1)
            .and_then(|s| s.split("</Days>").next())
            .and_then(|s| s.parse().ok());
        let prefix = xml
            .split("<Prefix>")
            .nth(1)
            .and_then(|s| s.split("</Prefix>").next())
            .unwrap_or("")
            .to_string();
        let status = if xml.contains("<Status>Enabled</Status>") {
            "Enabled"
        } else {
            "Disabled"
        };
        cfg.rules.push(LifecycleRule {
            id: "rule1".into(),
            status: status.into(),
            filter: LifecycleFilter { prefix },
            expiration: Some(Expiration { days }),
            noncurrent: None,
        });
    }
    cfg
}
