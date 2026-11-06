// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! IAM / bucket policy evaluation (Allow/Deny with Action/Resource).

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PolicyDocument {
    #[serde(rename = "Version", default = "default_version")]
    pub version: String,
    #[serde(rename = "Statement", default)]
    pub statement: Vec<Statement>,
}

fn default_version() -> String {
    "2012-10-17".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Statement {
    #[serde(rename = "Sid", default)]
    pub sid: String,
    #[serde(rename = "Effect")]
    pub effect: String,
    #[serde(rename = "Action", default)]
    pub action: ActionVal,
    #[serde(rename = "Resource", default)]
    pub resource: ActionVal,
    #[serde(rename = "Principal", default)]
    pub principal: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ActionVal {
    One(String),
    Many(Vec<String>),
}

impl Default for ActionVal {
    fn default() -> Self {
        Self::One("*".into())
    }
}

impl ActionVal {
    fn list(&self) -> Vec<&str> {
        match self {
            ActionVal::One(s) => vec![s.as_str()],
            ActionVal::Many(v) => v.iter().map(|s| s.as_str()).collect(),
        }
    }
}

fn match_glob(pattern: &str, value: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    if let Some(prefix) = pattern.strip_suffix('*') {
        return value.starts_with(prefix);
    }
    pattern == value
}

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
        return false;
    }
    allow
}

#[derive(Debug, Clone, Default)]
pub struct PolicyStore {
    /// identity -> IAM policies
    pub identity_policies: HashMap<String, Vec<PolicyDocument>>,
    /// bucket -> bucket policy
    pub bucket_policies: HashMap<String, PolicyDocument>,
}

impl PolicyStore {
    pub fn allowed(&self, identity: &str, action: &str, bucket: &str, key: &str) -> bool {
        if identity == "anonymous" || identity == "root" {
            // root always; anonymous only if bucket policy allows
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
            // no policies: allow authenticated identities with keys
            return identity != "anonymous";
        }
        evaluate(&docs, identity, action, &resource)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    #[test]
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
