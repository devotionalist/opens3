// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use hmac::{Hmac, Mac};
use opens3_common::types::AccessKey;
use opens3_common::{Error, Result};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

type HmacSha256 = Hmac<Sha256>;

#[derive(Clone, Default)]
pub struct AuthStore {
    keys: HashMap<String, AccessKey>,
    /// If empty, auth is disabled (dev mode)
    pub enabled: bool,
}

impl AuthStore {
    pub fn new(enabled: bool) -> Self {
        Self {
            keys: HashMap::new(),
            enabled,
        }
    }

    pub fn upsert(&mut self, key: AccessKey) {
        self.keys.insert(key.access_key.clone(), key);
    }

    pub fn get(&self, access_key: &str) -> Option<&AccessKey> {
        self.keys.get(access_key)
    }

    pub fn verify_sigv4(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &HashMap<String, String>,
        payload_hash: &str,
    ) -> Result<String> {
        if !self.enabled {
            // Dev / open mode: treat as privileged root so IAM policies do not block.
            return Ok("root".into());
        }
        if let Some(auth) = headers.get("authorization") {
            return self.verify_header_sigv4(method, path, query, headers, payload_hash, auth);
        }
        // Presigned URL: X-Amz-* query parameters
        self.verify_query_sigv4(method, path, query, headers, payload_hash)
    }

    fn verify_header_sigv4(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &HashMap<String, String>,
        payload_hash: &str,
        auth: &str,
    ) -> Result<String> {
        if !auth.starts_with("AWS4-HMAC-SHA256 ") {
            return Err(Error::Unauthorized("only SigV4 supported".into()));
        }
        let rest = &auth["AWS4-HMAC-SHA256 ".len()..];
        let mut credential = None;
        let mut signed_headers = None;
        let mut signature = None;
        for part in rest.split(',') {
            let part = part.trim();
            if let Some(v) = part.strip_prefix("Credential=") {
                credential = Some(v);
            } else if let Some(v) = part.strip_prefix("SignedHeaders=") {
                signed_headers = Some(v);
            } else if let Some(v) = part.strip_prefix("Signature=") {
                signature = Some(v);
            }
        }
        let credential =
            credential.ok_or_else(|| Error::Unauthorized("missing Credential".into()))?;
        let signed_headers =
            signed_headers.ok_or_else(|| Error::Unauthorized("missing SignedHeaders".into()))?;
        let signature = signature.ok_or_else(|| Error::Unauthorized("missing Signature".into()))?;

        let mut cred_parts = credential.split('/');
        let access_key = cred_parts
            .next()
            .ok_or_else(|| Error::Unauthorized("bad credential".into()))?;
        let date = cred_parts
            .next()
            .ok_or_else(|| Error::Unauthorized("bad credential".into()))?;
        let region = cred_parts.next().unwrap_or("us-east-1");
        let service = cred_parts.next().unwrap_or("s3");

        let key = self
            .get(access_key)
            .ok_or_else(|| Error::Unauthorized("unknown access key".into()))?;

        let amz_date = headers
            .get("x-amz-date")
            .cloned()
            .or_else(|| headers.get("date").cloned())
            .ok_or_else(|| Error::Unauthorized("missing x-amz-date".into()))?;

        let expected = compute_signature(
            &key.secret_key,
            method,
            path,
            query,
            headers,
            signed_headers,
            payload_hash,
            date,
            region,
            service,
            &amz_date,
        )?;
        if expected != signature {
            return Err(Error::Unauthorized("signature mismatch".into()));
        }
        Ok(key.identity.clone())
    }

    fn verify_query_sigv4(
        &self,
        method: &str,
        path: &str,
        query: &str,
        headers: &HashMap<String, String>,
        payload_hash: &str,
    ) -> Result<String> {
        let qmap = parse_query_map(query);
        let algo = qmap
            .get("X-Amz-Algorithm")
            .ok_or_else(|| Error::Unauthorized("missing Authorization or X-Amz-Algorithm".into()))?;
        if algo != "AWS4-HMAC-SHA256" {
            return Err(Error::Unauthorized("only SigV4 supported".into()));
        }
        let credential = qmap
            .get("X-Amz-Credential")
            .ok_or_else(|| Error::Unauthorized("missing X-Amz-Credential".into()))?;
        let amz_date = qmap
            .get("X-Amz-Date")
            .ok_or_else(|| Error::Unauthorized("missing X-Amz-Date".into()))?;
        let expires: u64 = qmap
            .get("X-Amz-Expires")
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let signed_headers = qmap
            .get("X-Amz-SignedHeaders")
            .cloned()
            .unwrap_or_else(|| "host".into());
        let signature = qmap
            .get("X-Amz-Signature")
            .ok_or_else(|| Error::Unauthorized("missing X-Amz-Signature".into()))?;

        // Expiry check
        if expires > 0 {
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(amz_date, "%Y%m%dT%H%M%SZ") {
                let start = dt.and_utc().timestamp() as u64;
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if now > start.saturating_add(expires) {
                    return Err(Error::Unauthorized("presigned URL expired".into()));
                }
            }
        }

        let mut cred_parts = credential.split('/');
        let access_key = cred_parts
            .next()
            .ok_or_else(|| Error::Unauthorized("bad credential".into()))?;
        let date = cred_parts
            .next()
            .ok_or_else(|| Error::Unauthorized("bad credential".into()))?;
        let region = cred_parts.next().unwrap_or("us-east-1");
        let service = cred_parts.next().unwrap_or("s3");

        let key = self
            .get(access_key)
            .ok_or_else(|| Error::Unauthorized("unknown access key".into()))?;

        // Canonical query excludes X-Amz-Signature
        let canon_q = canonical_query_excluding(query, "X-Amz-Signature");
        let ph = if payload_hash.is_empty() {
            "UNSIGNED-PAYLOAD"
        } else {
            payload_hash
        };
        let expected = compute_signature(
            &key.secret_key,
            method,
            path,
            &canon_q,
            headers,
            &signed_headers,
            ph,
            date,
            region,
            service,
            amz_date,
        )?;
        if expected != *signature {
            return Err(Error::Unauthorized("signature mismatch".into()));
        }
        Ok(key.identity.clone())
    }
}

fn compute_signature(
    secret_key: &str,
    method: &str,
    path: &str,
    query: &str,
    headers: &HashMap<String, String>,
    signed_headers: &str,
    payload_hash: &str,
    date: &str,
    region: &str,
    service: &str,
    amz_date: &str,
) -> Result<String> {
    let canonical_headers = {
        let mut pairs = Vec::new();
        for h in signed_headers.split(';') {
            let hv = headers
                .get(h)
                .or_else(|| headers.get(&h.to_ascii_lowercase()))
                .cloned()
                .unwrap_or_default();
            pairs.push(format!("{}:{}", h, hv.trim()));
        }
        pairs.join("\n") + "\n"
    };
    let canonical_request = format!(
        "{method}\n{path}\n{query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    );
    let canonical_hash = hex::encode(Sha256::digest(canonical_request.as_bytes()));
    let scope = format!("{date}/{region}/{service}/aws4_request");
    let string_to_sign = format!("AWS4-HMAC-SHA256\n{amz_date}\n{scope}\n{canonical_hash}");
    let signing_key = signing_key(secret_key, date, region, service)?;
    let mut mac =
        HmacSha256::new_from_slice(&signing_key).map_err(|e| Error::Internal(e.to_string()))?;
    mac.update(string_to_sign.as_bytes());
    Ok(hex::encode(mac.finalize().into_bytes()))
}

fn parse_query_map(query: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        m.insert(
            urlencoding_decode(k),
            urlencoding_decode(v),
        );
    }
    m
}

fn canonical_query_excluding(query: &str, exclude: &str) -> String {
    let mut pairs: Vec<(String, String)> = Vec::new();
    for part in query.split('&') {
        if part.is_empty() {
            continue;
        }
        let (k, v) = part.split_once('=').unwrap_or((part, ""));
        let kd = urlencoding_decode(k);
        if kd.eq_ignore_ascii_case(exclude) {
            continue;
        }
        pairs.push((kd, urlencoding_decode(v)));
    }
    pairs.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    pairs
        .into_iter()
        .map(|(k, v)| format!("{}={}", uri_encode(&k), uri_encode(&v)))
        .collect::<Vec<_>>()
        .join("&")
}

fn urlencoding_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(a), Some(b)) = (from_hex(bytes[i + 1]), from_hex(bytes[i + 2])) {
                out.push(a * 16 + b);
                i += 3;
                continue;
            }
        }
        if bytes[i] == b'+' {
            out.push(b' ');
        } else {
            out.push(bytes[i]);
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn from_hex(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

fn uri_encode(s: &str) -> String {
    s.chars()
        .map(|c| match c {
            'A'..='Z' | 'a'..='z' | '0'..='9' | '-' | '_' | '.' | '~' => c.to_string(),
            _ => format!("%{:02X}", c as u8),
        })
        .collect()
}

fn signing_key(secret: &str, date: &str, region: &str, service: &str) -> Result<Vec<u8>> {
    let k_date = hmac_sha256(format!("AWS4{secret}").as_bytes(), date.as_bytes())?;
    let k_region = hmac_sha256(&k_date, region.as_bytes())?;
    let k_service = hmac_sha256(&k_region, service.as_bytes())?;
    hmac_sha256(&k_service, b"aws4_request")
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> Result<Vec<u8>> {
    let mut mac =
        HmacSha256::new_from_slice(key).map_err(|e| Error::Internal(e.to_string()))?;
    mac.update(data);
    Ok(mac.finalize().into_bytes().to_vec())
}

pub fn payload_hash_from_headers(headers: &HashMap<String, String>, body: &[u8]) -> String {
    if let Some(h) = headers.get("x-amz-content-sha256") {
        if h != "UNSIGNED-PAYLOAD" && h != "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
            return h.clone();
        }
        if h == "UNSIGNED-PAYLOAD" {
            return "UNSIGNED-PAYLOAD".into();
        }
    }
    hex::encode(Sha256::digest(body))
}
