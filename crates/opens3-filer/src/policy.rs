// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! IAM / bucket policy 求值（Allow/Deny + Action/Resource）。

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// IAM 风格策略文档
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PolicyDocument {
    #[serde(rename = "Version", default = "default_version")]
    pub version: String, // 策略版本，默认 2012-10-17
    #[serde(rename = "Statement", default)]
    pub statement: Vec<Statement>, // 语句列表
}

/// serde 默认 Version
fn default_version() -> String {
    "2012-10-17".into()
}

/// 单条策略语句
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statement {
    #[serde(rename = "Sid", default)]
    pub sid: String, // 语句 ID（可选）
    #[serde(rename = "Effect")]
    pub effect: String, // Allow / Deny
    #[serde(rename = "Action", default)]
    pub action: ActionVal, // 动作（可单值或数组）
    #[serde(rename = "Resource", default)]
    pub resource: ActionVal, // 资源 ARN 模式
    #[serde(rename = "Principal", default)]
    pub principal: Option<serde_json::Value>, // 主体；缺省匹配任意
}

/// Action / Resource 的单值或列表（untagged）
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ActionVal {
    One(String),       // 单个模式
    Many(Vec<String>), // 多个模式
}

impl Default for ActionVal {
    /// 默认通配 `*`
    fn default() -> Self {
        Self::One("*".into())
    }
}

impl ActionVal {
    /// 统一展开为字符串切片列表
    fn list(&self) -> Vec<&str> {
        match self {
            ActionVal::One(s) => vec![s.as_str()],
            ActionVal::Many(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }
}

/// 简单 glob：`*` 全匹配，或仅支持后缀 `*` 前缀匹配
fn match_glob(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }
    pattern == value
}

/// 对多份策略求值：任意 Deny 优先；否则需至少一条 Allow
pub fn evaluate(
    policies: &[PolicyDocument],
    identity: &str,
    action: &str,
    resource: &str,
) -> bool {
    let mut allow = false;
    let mut deny = false;
    for pol in policies {
        for st in &pol.statement {
            let actions = st.action.list();
            let resources = st.resource.list();
            let action_ok = actions.iter().any(|a| match_glob(a, action));
            let resource_ok = resources.iter().any(|r| match_glob(r, resource));
            // Principal：缺省放行；支持字符串 / {AWS: ...}
            let principal_ok = match &st.principal {
                None => true,
                Some(serde_json::Value::String(s)) => s == "*" || s == identity,
                Some(serde_json::Value::Object(m)) => m
                    .get("AWS")
                    .map(|v| match v {
                        serde_json::Value::String(s) => s == "*" || s == identity,
                        serde_json::Value::Array(arr) => arr.iter().any(|x| {
                            x.as_str()
                                .map(|s| s == "*" || s == identity)
                                .unwrap_or(false)
                        }),
                        _ => false,
                    })
                    .unwrap_or(true),
                _ => true,
            };
            if !(action_ok && resource_ok && principal_ok) {
                continue;
            }
            if st.effect.eq_ignore_ascii_case("Deny") {
                deny = true;
            } else if st.effect.eq_ignore_ascii_case("Allow") {
                allow = true;
            }
        }
    }
    if deny {
        return false; // Deny 覆盖 Allow
    }
    allow
}

/// 身份策略 + 桶策略的内存存储
#[derive(Debug, Clone, Default)]
pub struct PolicyStore {
    /// identity -> IAM policies
    pub identity_policies: HashMap<String, Vec<PolicyDocument>>, // 身份附加策略
    /// bucket -> bucket policy
    pub bucket_policies: HashMap<String, PolicyDocument>, // 桶级策略
}

impl PolicyStore {
    /// 判断身份对 `bucket/key` 的 `action` 是否允许
    pub fn allowed(&self, identity: &str, action: &str, bucket: &str, key: &str) -> bool {
        if identity == "anonymous" || identity == "root" {
            // root 恒允许；anonymous 仅当桶策略显式允许
            if identity == "root" {
                return true;
            }
        }
        let resource = if key.is_empty() {
            format!("arn:aws:s3:::{bucket}")
        } else {
            format!("arn:aws:s3:::{bucket}/{key}")
        };
        let mut docs = Vec::new();
        if let Some(p) = self.identity_policies.get(identity) {
            docs.extend(p.iter().cloned());
        }
        if let Some(p) = self.bucket_policies.get(bucket) {
            docs.push(p.clone());
        }
        if docs.is_empty() {
            // 无策略时：非匿名已认证身份默认放行
            return identity != "anonymous";
        }
        evaluate(&docs, identity, action, &resource)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
    /// Deny 优先于 Allow
    fn deny_beats_allow() {
        let pol: PolicyDocument = serde_json::from_str(
            r#"{
            "Statement": [
              {"Effect":"Allow","Action":"s3:*","Resource":"arn:aws:s3:::b/*"},
              {"Effect":"Deny","Action":"s3:DeleteObject","Resource":"arn:aws:s3:::b/*"}
            ]
        }"#,
        )
        .unwrap();
        assert!(evaluate(&[pol.clone()], "u", "s3:GetObject", "arn:aws:s3:::b/x"));
        assert!(!evaluate(&[pol], "u", "s3:DeleteObject", "arn:aws:s3:::b/x"));
    }
}
