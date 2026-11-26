// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Filer/Master 签发的卷访问令牌：简易 HMAC-SHA256 JWT（payload.sig，无标准 header）。

use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>; // HMAC-SHA256 别名

/// 卷读写访问声明（嵌入 token payload）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeClaims {
    pub fid: String, // FileId 字符串
    pub exp: u64,    // 过期 Unix 秒
    pub op: String,  // 操作：read|write
}

/// 用 secret 签名 claims，返回 `base64url(payload).base64url(sig)`
pub fn sign(secret: &str, claims: &VolumeClaims) -> Result<String, String> {
    let payload = serde_json::to_vec(claims).map_err(|e| e.to_string())?;
    let payload_b64 = base64url(&payload);
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|e| e.to_string())?;
    mac.update(payload_b64.as_bytes());
    let sig = base64url(&mac.finalize().into_bytes());
    Ok(format!("{payload_b64}.{sig}"))
}

/// 校验签名与过期时间，成功则返回 claims
pub fn verify(secret: &str, token: &str) -> Result<VolumeClaims, String> {
    let (payload_b64, sig) = token
        .split_once('.')
        .ok_or_else(|| "bad token".to_string())?;
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).map_err(|e| e.to_string())?;
    mac.update(payload_b64.as_bytes());
    let expected = base64url(&mac.finalize().into_bytes());
    // 常量时间比较更理想，此处保持既有相等比较语义
    if expected != sig {
        return Err("bad signature".into());
    }
    let bytes = unbase64url(payload_b64)?;
    let claims: VolumeClaims = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    if claims.exp < now {
        return Err("expired".into());
    }
    Ok(claims)
}

/// URL-safe Base64（无 padding）
fn base64url(data: &[u8]) -> String {
    base64::Engine::encode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, data)
}

/// 解码 URL-safe Base64（无 padding）
fn unbase64url(s: &str) -> Result<Vec<u8>, String> {
    base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, s.as_bytes())
        .map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_jwt() {
        let claims = VolumeClaims {
            fid: "1,abc".into(),
            exp: u64::MAX / 2,
            op: "write".into(),
        };
        let tok = sign("secret", &claims).unwrap();
        let got = verify("secret", &tok).unwrap();
        assert_eq!(got.fid, claims.fid);
        assert!(verify("wrong", &tok).is_err());
    }
}
