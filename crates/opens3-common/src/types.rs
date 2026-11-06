// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use crate::replication::Replication;
use crate::topology::TopologyLocation;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const DEFAULT_VOLUME_SIZE: u64 = 30 * 1024 * 1024 * 1024; // 30 GiB
pub const DEFAULT_CHUNK_SIZE: u64 = 8 * 1024 * 1024; // 8 MiB
pub const NEEDLE_HEADER_SIZE: usize = 16;
pub const NEEDLE_CHECKSUM_SIZE: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeState {
    /// Provisioned but not serving assign/write until admin brings it online.
    Offline,
    Writable,
    ReadOnly,
    Sealed,
    EcEncoded,
}

impl VolumeState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Offline => "offline",
            Self::Writable => "writable",
            Self::ReadOnly => "read_only",
            Self::Sealed => "sealed",
            Self::EcEncoded => "ec_encoded",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeMeta { // 卷的元数据
    pub id: u32, // 卷的ID
    pub collection: String, // 卷的集合
    pub replication: Replication, // 卷的复制策略
    pub size: u64, // 卷的大小
    pub file_count: u64, // 卷的文件数量
    pub deleted_count: u64, // 卷的删除数量
    pub state: VolumeState, // 卷的状态
    pub ttl_seconds: u64, // 卷的TTL时间
    pub locations: Vec<TopologyLocation>, // 卷的位置
    pub max_size: u64, // 卷的最大大小
}

impl VolumeMeta { // 卷的元数据的操作
    pub fn new(id: u32, collection: &str, replication: Replication) -> Self {
        Self {
            id,
            collection: collection.to_string(),
            replication,
            size: 0,
            file_count: 0,
            deleted_count: 0,
            // New volumes stay offline until ops explicitly online them.
            state: VolumeState::Offline,
            ttl_seconds: 0,
            locations: Vec::new(),
            max_size: DEFAULT_VOLUME_SIZE,
        }
    }

    pub fn is_writable(&self) -> bool {
        matches!(self.state, VolumeState::Writable) && self.size < self.max_size
    }

    pub fn is_online(&self) -> bool {
        !matches!(self.state, VolumeState::Offline)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    pub file_id: String,
    pub offset: u64,
    pub size: u64,
    pub etag: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryMeta {
    pub name: String,
    pub is_directory: bool,
    pub size: u64,
    pub mtime: DateTime<Utc>,
    pub crtime: DateTime<Utc>,
    pub mode: u32,
    pub mime: String,
    pub etag: String,
    /// Single-blob file id (small files)
    pub file_id: Option<String>,
    /// Chunked large file
    pub chunks: Vec<ChunkRef>,
    pub ttl_seconds: u64,
    pub encrypted: bool,
    pub version_id: Option<String>,
    pub delete_marker: bool,
    pub user_metadata: std::collections::HashMap<String, String>,
}

impl EntryMeta {
    pub fn directory(name: impl Into<String>) -> Self {
        let now = Utc::now();
        Self {
            name: name.into(),
            is_directory: true,
            size: 0,
            mtime: now,
            crtime: now,
            mode: 0o755,
            mime: "application/octet-stream".into(),
            etag: String::new(),
            file_id: None,
            chunks: Vec::new(),
            ttl_seconds: 0,
            encrypted: false,
            version_id: None,
            delete_marker: false,
            user_metadata: Default::default(),
        }
    }

    pub fn file(name: impl Into<String>, size: u64, file_id: String, etag: String) -> Self {
        let now = Utc::now();
        Self {
            name: name.into(),
            is_directory: false,
            size,
            mtime: now,
            crtime: now,
            mode: 0o644,
            mime: "application/octet-stream".into(),
            etag,
            file_id: Some(file_id),
            chunks: Vec::new(),
            ttl_seconds: 0,
            encrypted: false,
            version_id: None,
            delete_marker: false,
            user_metadata: Default::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessKey {
    pub access_key: String,
    pub secret_key: String,
    pub identity: String,
    pub actions: Vec<String>,
}
