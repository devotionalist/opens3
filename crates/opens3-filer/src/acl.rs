// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 罐头 ACL 存储与判定（与 policy 取交集使用）。

use opens3_common::Result;
use serde::{Deserialize, Serialize};

/// S3 罐头 ACL 枚举（对应 x-amz-acl 常用取值）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CannedAcl {
    Private,                 // 仅 owner / root
    PublicRead,              // 匿名可读
    PublicReadWrite,         // 匿名可读写
    AuthenticatedRead,       // 已认证用户可读
    BucketOwnerFullControl,  // 桶所有者完全控制（写入侧等同 Private）
}

impl CannedAcl {
    /// 解析 ACL 字符串；未知值回落为 Private
    pub fn parse(s: &str) -> Self {
        match s {
            "public-read" => Self::PublicRead,
            "public-read-write" => Self::PublicReadWrite,
            "authenticated-read" => Self::AuthenticatedRead,
            "bucket-owner-full-control" => Self::BucketOwnerFullControl,
            _ => Self::Private,
        }
    }

    /// 序列化为 S3 标准 ACL 名称
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::PublicRead => "public-read",
            Self::PublicReadWrite => "public-read-write",
            Self::AuthenticatedRead => "authenticated-read",
            Self::BucketOwnerFullControl => "bucket-owner-full-control",
        }
    }

    /// 是否允许读取：`identity` 为调用方身份；`authenticated` 表示已登录
    pub fn allows_read(&self, identity: &str, authenticated: bool) -> bool {
        match self {
            Self::Private | Self::BucketOwnerFullControl => {
                identity == "root" || identity == "owner"
            }
            Self::PublicRead | Self::PublicReadWrite => true,
            Self::AuthenticatedRead => authenticated || identity == "root",
        }
    }

    /// 是否允许写入：仅 PublicReadWrite 对所有人开放，其余需 root/owner
    pub fn allows_write(&self, identity: &str) -> bool {
        match self {
            Self::PublicReadWrite => true,
            _ => identity == "root" || identity == "owner",
        }
    }
}

/// 按罐头 ACL 生成 AccessControlPolicy XML（owner 恒有 FULL_CONTROL）
pub fn acl_xml(owner: &str, canned: &CannedAcl) -> String {
    let mut grants = String::new();
    grants.push_str(&format!(
        "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>{owner}</ID><DisplayName>{owner}</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant>"
    ));
    match canned {
        CannedAcl::PublicRead | CannedAcl::PublicReadWrite => {
            // AllUsers READ
            grants.push_str(
                "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant>",
            );
        }
        CannedAcl::AuthenticatedRead => {
            // AuthenticatedUsers READ
            grants.push_str(
                "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AuthenticatedUsers</URI></Grantee><Permission>READ</Permission></Grant>",
            );
        }
        _ => {}
    }
    if *canned == CannedAcl::PublicReadWrite {
        // AllUsers WRITE
        grants.push_str(
            "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>WRITE</Permission></Grant>",
        );
    }
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<AccessControlPolicy xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner><ID>{owner}</ID><DisplayName>{owner}</DisplayName></Owner>
  <AccessControlList>{grants}</AccessControlList>
</AccessControlPolicy>"#
    )
}

/// 从 ACL XML 粗粒度反推罐头类型（按 AllUsers/AuthenticatedUsers + WRITE 启发式）
pub fn parse_canned_from_acl_xml(body: &str) -> CannedAcl {
    if body.contains("AllUsers") && body.contains("WRITE") {
        CannedAcl::PublicReadWrite
    } else if body.contains("AllUsers") {
        CannedAcl::PublicRead
    } else if body.contains("AuthenticatedUsers") {
        CannedAcl::AuthenticatedRead
    } else {
        CannedAcl::Private
    }
}

/// 持久化值：ACL 名称的 UTF-8 字节
pub fn store_value(canned: &CannedAcl) -> Vec<u8> {
    canned.as_str().as_bytes().to_vec()
}

/// 从持久化字节加载罐头 ACL
pub fn load_value(raw: &[u8]) -> CannedAcl {
    CannedAcl::parse(&String::from_utf8_lossy(raw))
}

#[allow(dead_code)]
/// 占位：消化 Result 的辅助（当前无实际逻辑）
pub fn check_ok(_r: Result<()>) {}
