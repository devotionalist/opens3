// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 复制策略 XYZ：跨 DC / 跨机架 / 同机架额外副本数。

use crate::error::{Error, Result};
use serde::{Deserialize, Serialize};
use std::str::FromStr;

/// 复制策略 XYZ：
/// - X = 其它数据中心副本数
/// - Y = 同 DC 其它机架副本数
/// - Z = 同机架其它节点副本数
///
/// 总副本数 = 1（主）+ X + Y + Z
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Replication {
    pub diff_data_center: u8, // 跨 DC 额外副本（X）
    pub diff_rack: u8,        // 同 DC 跨机架额外副本（Y）
    pub same_rack: u8,        // 同机架额外副本（Z）
}

impl Replication {
    /// 默认 000：仅单副本
    pub const DEFAULT: Self = Self {
        diff_data_center: 0,
        diff_rack: 0,
        same_rack: 0,
    };

    /// 总副本数（含主副本）
    pub fn total_copies(&self) -> u8 {
        1 + self.diff_data_center + self.diff_rack + self.same_rack
    }

    /// 格式化为三位数字串，如 `"010"`
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

    /// 解析三位数字字符串（每位 0–9）
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
