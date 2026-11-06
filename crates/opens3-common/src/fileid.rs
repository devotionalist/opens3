// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

/// File id: `{volumeId},{fileKey}{cookieHex}`
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct FileId {
    pub volume_id: u32,
    pub file_key: u64,
    pub cookie: u32,
}

impl FileId {
    pub fn new(volume_id: u32, file_key: u64, cookie: u32) -> Self {
        Self {
            volume_id,
            file_key,
            cookie,
        }
    }

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

    fn from_str(s: &str) -> Result<Self> {
        let (vol, rest) = s
            .split_once(',')
            .ok_or_else(|| Error::InvalidArgument(format!("bad fid: {s}")))?;
        let volume_id: u32 = vol
            .parse()
            .map_err(|_| Error::InvalidArgument(format!("bad volume id in fid: {s}")))?;
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
