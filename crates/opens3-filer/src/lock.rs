// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Object Lock 配置与按对象保留期强制执行。

use chrono::{DateTime, Duration, Utc};
use opens3_common::types::EntryMeta;
use opens3_common::{Error, Result};
use serde::{Deserialize, Serialize};

/// 桶级 Object Lock 默认配置
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ObjectLockConfig {
    pub enabled: bool,          // 是否启用 Object Lock
    pub mode: Option<String>,   // GOVERNANCE | COMPLIANCE
    pub days: Option<i64>,      // 默认保留天数
    pub years: Option<i64>,     // 默认保留年数（优先 days）
}

impl ObjectLockConfig {
    /// 从 ObjectLockConfiguration XML 粗解析
    pub fn from_xml(body: &str) -> Self {
        let enabled = body.contains("Enabled") || body.contains("<ObjectLockEnabled>Enabled");
        let mode = if body.contains("COMPLIANCE") {
            Some("COMPLIANCE".into())
        } else if body.contains("GOVERNANCE") {
            Some("GOVERNANCE".into())
        } else {
            None
        };
        let days = extract_int(body, "Days");
        let years = extract_int(body, "Years");
        Self {
            enabled,
            mode,
            days,
            years,
        }
    }

    /// 序列化为 ObjectLockConfiguration XML
    pub fn to_xml(&self) -> String {
        if !self.enabled {
            return r#"<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <ObjectLockEnabled>Disabled</ObjectLockEnabled>
</ObjectLockConfiguration>"#
                .into();
        }
        let mut rule = String::new();
        if let Some(mode) = &self.mode {
            rule.push_str("<Rule><DefaultRetention>");
            rule.push_str(&format!("<Mode>{mode}</Mode>"));
            if let Some(d) = self.days {
                rule.push_str(&format!("<Days>{d}</Days>"));
            }
            if let Some(y) = self.years {
                rule.push_str(&format!("<Years>{y}</Years>"));
            }
            rule.push_str("</DefaultRetention></Rule>");
        }
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <ObjectLockEnabled>Enabled</ObjectLockEnabled>
  {rule}
</ObjectLockConfiguration>"#
        )
    }

    /// 按 days / years 计算默认 retain-until；都无则 None
    pub fn default_retain_until(&self) -> Option<DateTime<Utc>> {
        let now = Utc::now();
        if let Some(d) = self.days {
            return Some(now + Duration::days(d));
        }
        if let Some(y) = self.years {
            return Some(now + Duration::days(y * 365)); // 简化：按 365 天/年
        }
        None
    }
}

/// 从 XML 提取整型标签内容
fn extract_int(body: &str, tag: &str) -> Option<i64> {
    let open = format!("<{tag}>");
    let close = format!("</{tag}>");
    let s = body.find(&open)? + open.len();
    let e = body[s..].find(&close)? + s;
    body[s..e].trim().parse().ok()
}

/// 将桶默认保留写入 entry 的 user_metadata
pub fn apply_default_retention(entry: &mut EntryMeta, cfg: &ObjectLockConfig) {
    if !cfg.enabled {
        return;
    }
    if let Some(mode) = &cfg.mode {
        entry
            .user_metadata
            .insert("x-amz-object-lock-mode".into(), mode.clone());
    }
    if let Some(until) = cfg.default_retain_until() {
        entry.user_metadata.insert(
            "x-amz-object-lock-retain-until-date".into(),
            until.to_rfc3339(),
        );
    }
}

/// 将请求头中的 Object Lock 字段写入 entry metadata
pub fn apply_request_lock_headers(
    entry: &mut EntryMeta,
    headers: &std::collections::HashMap<String, String>,
) {
    if let Some(mode) = headers.get("x-amz-object-lock-mode") {
        entry
            .user_metadata
            .insert("x-amz-object-lock-mode".into(), mode.clone());
    }
    if let Some(until) = headers.get("x-amz-object-lock-retain-until-date") {
        entry.user_metadata.insert(
            "x-amz-object-lock-retain-until-date".into(),
            until.clone(),
        );
    }
    if let Some(hold) = headers.get("x-amz-object-lock-legal-hold") {
        entry
            .user_metadata
            .insert("x-amz-object-lock-legal-hold".into(), hold.clone());
    }
}

/// 删除/覆盖前检查：legal hold、COMPLIANCE/GOVERNANCE 保留期；返回 Err(ObjectLocked) 表示禁止
pub fn enforce_delete(
    entry: &EntryMeta,
    bypass_governance: bool,
) -> Result<()> {
    // 法律保留开启时一律禁止
    if entry
        .user_metadata
        .get("x-amz-object-lock-legal-hold")
        .map(|v| v.eq_ignore_ascii_case("ON"))
        .unwrap_or(false)
    {
        return Err(Error::ObjectLocked(
            "object has legal hold".into(),
        ));
    }
    let mode = entry
        .user_metadata
        .get("x-amz-object-lock-mode")
        .cloned()
        .unwrap_or_default();
    let until = entry
        .user_metadata
        .get("x-amz-object-lock-retain-until-date")
        .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
        .map(|d| d.with_timezone(&Utc));
    let Some(until) = until else {
        return Ok(()); // 无保留期则放行
    };
    if Utc::now() >= until {
        return Ok(()); // 已过期
    }
    if mode.eq_ignore_ascii_case("COMPLIANCE") {
        return Err(Error::ObjectLocked(format!(
            "compliance retention until {}",
            until.to_rfc3339()
        )));
    }
    // GOVERNANCE 可用 bypass 头绕过
    if mode.eq_ignore_ascii_case("GOVERNANCE") && !bypass_governance {
        return Err(Error::ObjectLocked(format!(
            "governance retention until {} (set x-amz-bypass-governance-retention)",
            until.to_rfc3339()
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::*;
    use opens3_common::types::EntryMeta;

    #[test]
    /// COMPLIANCE 即使 bypass 也不可删
    fn compliance_blocks_delete() {
        let mut e = EntryMeta::file("k", 1, "f".into(), "e".into());
        e.user_metadata
            .insert("x-amz-object-lock-mode".into(), "COMPLIANCE".into());
        e.user_metadata.insert(
            "x-amz-object-lock-retain-until-date".into(),
            (Utc::now() + Duration::days(7)).to_rfc3339(),
        );
        assert!(enforce_delete(&e, true).is_err());
    }

    #[test]
    /// GOVERNANCE 可通过 bypass_governance 放行
    fn governance_bypass() {
        let mut e = EntryMeta::file("k", 1, "f".into(), "e".into());
        e.user_metadata
            .insert("x-amz-object-lock-mode".into(), "GOVERNANCE".into());
        e.user_metadata.insert(
            "x-amz-object-lock-retain-until-date".into(),
            (Utc::now() + Duration::days(7)).to_rfc3339(),
        );
        assert!(enforce_delete(&e, false).is_err());
        assert!(enforce_delete(&e, true).is_ok());
    }

    #[test]
    /// 解析 ObjectLockConfiguration XML
    fn parse_lock_xml() {
        let cfg = ObjectLockConfig::from_xml(
            r#"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled>
            <Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>30</Days></DefaultRetention></Rule>
            </ObjectLockConfiguration>"#,
        );
        assert!(cfg.enabled);
        assert_eq!(cfg.mode.as_deref(), Some("COMPLIANCE"));
        assert_eq!(cfg.days, Some(30));
    }
}
