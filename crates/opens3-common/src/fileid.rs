// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! FileId：`{volumeId},{fileKeyHex}{cookieHex8}` 的解析与格式化。

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// 文件定位符：卷 ID + file_key + cookie
///
/// 字符串形式：`{volumeId},{fileKey}{cookieHex}`（cookie 固定 8 位十六进制）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileId {
    pub volume_id: u32, // 所属卷 ID
    pub file_key: u64,  // 卷内 needle 键（变长 hex）
    pub cookie: u32,    // 防猜写 cookie（固定 8 hex）
}

impl FileId {
    /// 构造 FileId
    pub fn new(volume_id: u32, file_key: u64, cookie: u32) -> Self {
        Self {
            volume_id,
            file_key,
            cookie,
        }
    }

    /// 格式化为 fid 字符串（cookie 补零到 8 hex）
    pub fn fid(&self) -> String {
        format!("{},{:x}{:08x}", self.volume_id, self.file_key, self.cookie)
    }
}

impl fmt::Display for FileId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.fid())
    }
}

impl FromStr for FileId {
    type Err = Error;

    /// 解析 `{vol},{keyHex}{cookieHex8}`；cookie 取末 8 位 hex
    fn from_str(s: &str) -> Result<Self> {
        let (vol, rest) = s
            .split_once(',')
            .ok_or_else(|| Error::InvalidArgument(format!("bad fid: {s}")))?;
        let volume_id: u32 = vol
            .parse()
            .map_err(|_| Error::InvalidArgument(format!("bad volume id in fid: {s}")))?;
        // cookie 固定占末尾 8 个十六进制字符
        if rest.len() < 8 {
            return Err(Error::InvalidArgument(format!("bad fid cookie: {s}")));
        }
        let (key_hex, cookie_hex) = rest.split_at(rest.len() - 8);
        let file_key = u64::from_str_radix(key_hex, 16)
            .map_err(|_| Error::InvalidArgument(format!("bad file key in fid: {s}")))?;
        let cookie = u32::from_str_radix(cookie_hex, 16)
            .map_err(|_| Error::InvalidArgument(format!("bad cookie in fid: {s}")))?;
        Ok(Self {
            volume_id,
            file_key,
            cookie,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let f = FileId::new(3, 0xabc, 0x12345678);
        let s = f.fid();
        let p: FileId = s.parse().unwrap();
        assert_eq!(f, p);
    }
}
