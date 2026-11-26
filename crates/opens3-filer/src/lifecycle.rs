// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Bucket lifecycle：按天数过期当前 / 非当前对象。

use chrono::{Duration, Utc};
use opens3_common::types::EntryMeta;
use opens3_store::FilerStore;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::info;

/// Bucket lifecycle 配置（规则列表）
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LifecycleConfig {
    #[serde(rename = "Rule", default)]
    pub rules: Vec<LifecycleRule>, // 生命周期规则
}

/// 单条 lifecycle 规则
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LifecycleRule {
    #[serde(rename = "ID", default)]
    pub id: String, // 规则 ID
    #[serde(rename = "Status", default = "default_enabled")]
    pub status: String, // Enabled / Disabled
    #[serde(rename = "Filter", default)]
    pub filter: LifecycleFilter, // 前缀过滤
    #[serde(rename = "Expiration", default)]
    pub expiration: Option<Expiration>, // 当前版本过期
    #[serde(rename = "NoncurrentVersionExpiration", default)]
    pub noncurrent: Option<NoncurrentExpiration>, // 非当前版本过期（预留）
}

/// serde 默认状态为 Enabled
fn default_enabled() -> String {
    "Enabled".into()
}

/// lifecycle 过滤条件（目前仅 Prefix）
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LifecycleFilter {
    #[serde(rename = "Prefix", default)]
    pub prefix: String, // 对象 key 前缀
}

/// 当前对象过期：按天数
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Expiration {
    #[serde(rename = "Days")]
    pub days: Option<i64>, // 距 mtime 的天数阈值
}

/// 非当前版本过期（字段预留，apply 暂未使用）
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct NoncurrentExpiration {
    #[serde(rename = "NoncurrentDays")]
    pub noncurrent_days: Option<i64>, // 非当前版本保留天数
}

/// 对桶内对象应用 lifecycle：匹配 Enabled 规则后调用 `delete_fn`；返回删除次数
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
                continue; // 跳过未启用规则
            }
            if !key.starts_with(&rule.filter.prefix) {
                continue;
            }
            if let Some(exp) = &rule.expiration {
                if let Some(days) = exp.days {
                    // mtime 超龄且为可过期对象时删除
                    if now - meta.mtime > Duration::days(days) && should_expire(&meta) {
                        delete_fn(path.clone()).await;
                        deleted += 1;
                        info!("lifecycle expired {path} rule={}", rule.id);
                        break; // 一对象只匹配一条规则
                    }
                }
            }
        }
    }
    deleted
}

/// 目录与 delete marker 不过期
fn should_expire(meta: &EntryMeta) -> bool {
    !meta.is_directory && !meta.delete_marker
}

/// 最小 XML 刮取：解析单条 Rule 的 Days / Prefix / Status
pub fn parse_lifecycle_xml(xml: &str) -> LifecycleConfig {
    // 完整 XML 解析器可选；此处用字符串切分即可满足常用配置
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
