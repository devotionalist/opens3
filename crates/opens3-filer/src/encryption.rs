// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! SSE-S3 风格 AES-256-GCM 信封加解密。

use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use opens3_common::{Error, Result};
use rand::RngCore;

/// 简单 SSE-S3 信封：`nonce(12) || ciphertext`；失败映射为 Internal
pub fn encrypt(key32: &[u8; 32], plaintext: &[u8]) -> Result<Vec<u8>> {
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key32));
    let mut nonce_bytes = [0u8; 12]; // GCM 标准 96-bit nonce
    rand::thread_rng().fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);
    let ct = cipher
        .encrypt(nonce, plaintext)
        .map_err(|e| Error::Internal(format!("encrypt: {e}")))?;
    let mut out = Vec::with_capacity(12 + ct.len());
    out.extend_from_slice(&nonce_bytes); // 前缀存放 nonce，解密时切分
    out.extend_from_slice(&ct);
    Ok(out)
}

/// 解密 `nonce || ciphertext`；长度不足 12 字节视为非法参数
pub fn decrypt(key32: &[u8; 32], data: &[u8]) -> Result<Vec<u8>> {
    if data.len() < 12 {
        return Err(Error::InvalidArgument("ciphertext too short".into()));
    }
    let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(key32));
    let nonce = Nonce::from_slice(&data[..12]);
    cipher
        .decrypt(nonce, &data[12..])
        .map_err(|e| Error::Internal(format!("decrypt: {e}")))
}

/// 口令经 SHA-256 派生为 32 字节密钥（非 PBKDF；仅作开发/简单场景）
pub fn key_from_passphrase(pass: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(pass.as_bytes());
    let out = h.finalize();
    let mut key = [0u8; 32];
    key.copy_from_slice(&out);
    key
}
