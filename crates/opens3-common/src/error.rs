// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("invalid argument: {0}")]
    InvalidArgument(String),

    #[error("volume full: {0}")]
    VolumeFull(u32),

    #[error("volume read only: {0}")]
    VolumeReadOnly(u32),

    #[error("no writable volume available")]
    NoWritableVolume,

    #[error("replication failed: {0}")]
    ReplicationFailed(String),

    #[error("cookie mismatch")]
    CookieMismatch,

    #[error("unauthorized: {0}")]
    Unauthorized(String),

    #[error("forbidden: {0}")]
    Forbidden(String),

    #[error("object locked: {0}")]
    ObjectLocked(String),

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("internal: {0}")]
    Internal(String),

    #[error("{0}")]
    Other(#[from] anyhow::Error),
}

impl Error {
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
