// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 哈希、随机 cookie、路径拼接等小工具。

use md5::{Digest, Md5};
use rand::Rng;
use sha2::Sha256;

/// 计算 MD5 并以小写 hex 返回（常作 ETag）
pub fn md5_hex(data: &[u8]) -> String {
    let mut h = Md5::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// 计算 SHA-256 并以小写 hex 返回
pub fn sha256_hex(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// 生成随机 Needle cookie（u32）
pub fn random_cookie() -> u32 {
    rand::thread_rng().gen()
}

/// CRC32（与 Needle 尾校验一致）
pub fn crc32(data: &[u8]) -> u32 {
    crc32fast::hash(data)
}

/// 拼接父路径与名称，保证以 `/` 分隔且根路径规范
pub fn join_path(parent: &str, name: &str) -> String {
    if parent.is_empty() || parent == "/" {
        if name.starts_with('/') {
            name.to_string()
        } else {
            format!("/{name}")
        }
    } else if parent.ends_with('/') {
        format!("{parent}{name}")
    } else {
        format!("{parent}/{name}")
    }
}

/// 取父目录路径；根或其子的父均为 `"/"`
pub fn parent_path(path: &str) -> &str {
    let path = path.trim_end_matches('/');
    match path.rfind('/') {
        Some(0) | None => "/",
        Some(i) => &path[..i],
    }
}

/// 取路径末段名（去掉尾部 `/`）
pub fn base_name(path: &str) -> &str {
    let path = path.trim_end_matches('/');
    match path.rfind('/') {
        Some(i) => &path[i + 1..],
        None => path,
    }
}
