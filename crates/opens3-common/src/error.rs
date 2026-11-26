// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 跨组件统一错误类型，并映射到 S3 错误码 / HTTP 状态。

use thiserror::Error;

/// 本 crate 及调用方常用的 `Result` 别名
pub type Result<T> = std::result::Result<T, Error>;

/// OpenS3 公共错误枚举
#[derive(Debug, Error)]
pub enum Error {
    /// 资源不存在（对象/卷/条目等）
    #[error("not found: {0}")]
    NotFound(String),

    /// 资源已存在（如 bucket 冲突）
    #[error("already exists: {0}")]
    AlreadyExists(String),

    /// 参数非法（含 FileId / 复制策略解析失败）
    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    /// 卷已满（参数为 volume id）
    #[error("volume full: {0}")]
    VolumeFull(u32),

    /// 卷只读（参数为 volume id）
    #[error("volume read only: {0}")]
    VolumeReadOnly(u32),

    /// 集群无可用可写卷
    #[error("no writable volume available")]
    NoWritableVolume,

    /// 副本同步 / 复制失败
    #[error("replication failed: {0}")]
    ReplicationFailed(String),

    /// Needle cookie 与请求不一致（防猜写）
    #[error("cookie mismatch")]
    CookieMismatch,

    /// 未认证 / 凭证无效
    #[error("unauthorized: {0}")]
    Unauthorized(String),

    /// 已认证但无权限
    #[error("forbidden: {0}")]
    Forbidden(String),

    /// 对象锁定（Object Lock）阻止操作
    #[error("object locked: {0}")]
    ObjectLocked(String),

    /// 底层 IO 错误
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// 内部逻辑错误
    #[error("internal: {0}")]
    Internal(String),

    /// 其它 anyhow 包装错误
    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl Error {
    /// 映射为 S3 XML/API 错误码字符串
    pub fn s3_code(&self) -> &'static str {
        match self {
            Error::NotFound(_) => "NoSuchKey",
            Error::AlreadyExists(_) => "BucketAlreadyExists",
            Error::InvalidArgument(_) => "InvalidArgument",
            Error::Unauthorized(_) => "AccessDenied",
            Error::Forbidden(_) => "AccessDenied",
            Error::ObjectLocked(_) => "AccessDenied",
            Error::CookieMismatch => "InvalidRequest",
            _ => "InternalError",
        }
    }

    /// 映射为 HTTP 状态码
    pub fn http_status(&self) -> u16 {
        match self {
            Error::NotFound(_) => 404,
            Error::AlreadyExists(_) => 409,
            Error::InvalidArgument(_) | Error::CookieMismatch => 400,
            Error::Unauthorized(_) => 401,
            Error::Forbidden(_) | Error::ObjectLocked(_) => 403,
            Error::VolumeFull(_) | Error::NoWritableVolume => 503,
            _ => 500,
        }
    }
}
