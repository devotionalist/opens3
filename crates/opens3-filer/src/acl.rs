// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Canned ACL storage and evaluation (policy ∩ ACL).

use opens3_common::Result;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CannedAcl {
    Private,
    PublicRead,
    PublicReadWrite,
    AuthenticatedRead,
    BucketOwnerFullControl,
}

impl CannedAcl {
    pub fn parse(s: &str) -> Self {
        match s {
            "public-read" => Self::PublicRead,
            "public-read-write" => Self::PublicReadWrite,
            "authenticated-read" => Self::AuthenticatedRead,
            "bucket-owner-full-control" => Self::BucketOwnerFullControl,
            _ => Self::Private,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Private => "private",
            Self::PublicRead => "public-read",
            Self::PublicReadWrite => "public-read-write",
            Self::AuthenticatedRead => "authenticated-read",
            Self::BucketOwnerFullControl => "bucket-owner-full-control",
        }
    }

    pub fn allows_read(&self, identity: &str, authenticated: bool) -> bool {
        match self {
            Self::Private | Self::BucketOwnerFullControl => {
                identity == "root" || identity == "owner"
            }
            Self::PublicRead | Self::PublicReadWrite => true,
            Self::AuthenticatedRead => authenticated || identity == "root",
        }
    }

    pub fn allows_write(&self, identity: &str) -> bool {
        match self {
            Self::PublicReadWrite => true,
            _ => identity == "root" || identity == "owner",
        }
    }
}

pub fn acl_xml(owner: &str, canned: &CannedAcl) -> String {
    let mut grants = String::new();
    grants.push_str(&format!(
        "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"CanonicalUser\"><ID>{owner}</ID><DisplayName>{owner}</DisplayName></Grantee><Permission>FULL_CONTROL</Permission></Grant>"
    ));
    match canned {
        CannedAcl::PublicRead | CannedAcl::PublicReadWrite => {
            grants.push_str(
                "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AllUsers</URI></Grantee><Permission>READ</Permission></Grant>",
            );
        }
        CannedAcl::AuthenticatedRead => {
            grants.push_str(
                "<Grant><Grantee xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" xsi:type=\"Group\"><URI>http://acs.amazonaws.com/groups/global/AuthenticatedUsers</URI></Grantee><Permission>READ</Permission></Grant>",
            );
        }
        _ => {}
    }
    if *canned == CannedAcl::PublicReadWrite {
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

pub fn store_value(canned: &CannedAcl) -> Vec<u8> {
    canned.as_str().as_bytes().to_vec()
}

pub fn load_value(raw: &[u8]) -> CannedAcl {
    CannedAcl::parse(&String::from_utf8_lossy(raw))
}

#[allow(dead_code)]
pub fn check_ok(_r: Result<()>) {}
