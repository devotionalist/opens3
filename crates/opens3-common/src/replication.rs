// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// Replication policy XYZ: cross-DC / cross-rack / same-rack extra copies.
/// X = other data centers, Y = other racks in same DC, Z = other servers in same rack
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Replication {
    // 复制策略
    pub diff_data_center: u8, // 不同的数据中心
    pub diff_rack: u8,        // 不同的机架
    pub same_rack: u8,        // 同一个机架
}

impl Replication {
    // 复制策略的操作
    pub const DEFAULT: Self = Self {
        // 默认的复制策略
        diff_data_center: 0,
        diff_rack: 0,
        same_rack: 0,
    };

    pub fn total_copies(&self) -> u8 {
        1 + self.diff_data_center + self.diff_rack + self.same_rack
    }

    pub fn as_str(&self) -> String {
        format!(
            "{}{}{}",
            self.diff_data_center, self.diff_rack, self.same_rack
        )
    }
}

impl Default for Replication {
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl FromStr for Replication {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        let s = s.trim();
        if s.len() != 3 || !s.chars().all(|c| c.is_ascii_digit()) {
            return Err(Error::InvalidArgument(format!(
                "replication must be 3 digits, got {s}"
            )));
        }
        let bytes = s.as_bytes();
        Ok(Self {
            diff_data_center: bytes[0] - b'0',
            diff_rack: bytes[1] - b'0',
            same_rack: bytes[2] - b'0',
        })
    }
}

impl std::fmt::Display for Replication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}
