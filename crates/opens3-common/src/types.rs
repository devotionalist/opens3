// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 卷 / 文件条目 / 访问密钥等跨组件共享的核心数据结构。

use crate::replication::Replication;
use crate::topology::TopologyLocation;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const DEFAULT_VOLUME_SIZE: u64 = 30 * 1024 * 1024 * 1024; // 默认卷容量上限：30 GiB
pub const DEFAULT_CHUNK_SIZE: u64 = 8 * 1024 * 1024; // 大文件分片默认大小：8 MiB
pub const NEEDLE_HEADER_SIZE: usize = 16; // Needle 磁盘头长度（字节）
pub const NEEDLE_CHECKSUM_SIZE: usize = 4; // Needle CRC32 尾校验长度（字节）

/// 卷生命周期状态（影响 assign / 读写 / EC）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VolumeState {
    /// 已分配但未上线；admin online 前不参与 assign/写
    Offline,
    /// 可写入（且未达 max_size）
    Writable,
    /// 只读；拒绝新写
    ReadOnly,
    /// 已封存；通常为 vacuum/EC 前的稳定态
    Sealed,
    /// 已做纠删码（EC）编码
    EcEncoded,
}

impl VolumeState {
    /// 序列化为 snake_case 字符串（与 serde 一致）
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

/// 卷的元数据（Clerk 视图 + 心跳汇总）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeMeta {
    pub id: u32,                              // 卷 ID
    pub collection: String,                   // 逻辑集合名（隔离命名空间）
    pub replication: Replication,             // XYZ 复制策略
    pub size: u64,                            // 当前占用字节数
    pub file_count: u64,                      // 存活 file_key 数
    pub deleted_count: u64,                   // 软删 file_key 数
    pub state: VolumeState,                   // 生命周期状态
    pub ttl_seconds: u64,                     // 卷级 TTL（秒；0 表示不限期）
    pub locations: Vec<TopologyLocation>,     // 副本所在拓扑位置
    pub max_size: u64,                        // 可写上限（字节）
}

impl VolumeMeta {
    /// 新建卷元数据：初始 Offline，容量用 [`DEFAULT_VOLUME_SIZE`]
    pub fn new(id: u32, collection: &str, replication: Replication) -> Self {
        Self {
            id,
            collection: collection.to_string(),
            replication,
            size: 0,
            file_count: 0,
            deleted_count: 0,
            // 新卷保持 Offline，需运维显式 online
            state: VolumeState::Offline,
            ttl_seconds: 0,
            locations: Vec::new(),
            max_size: DEFAULT_VOLUME_SIZE,
        }
    }

    /// 是否可写：状态为 Writable 且未达 max_size
    pub fn is_writable(&self) -> bool {
        matches!(self.state, VolumeState::Writable) && self.size < self.max_size
    }

    /// 是否已上线（非 Offline）
    pub fn is_online(&self) -> bool {
        !matches!(self.state, VolumeState::Offline)
    }
}

/// 大文件分片引用（EntryMeta.chunks）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkRef {
    pub file_id: String, // 分片对应的 FileId 字符串
    pub offset: u64,     // 在对象逻辑字节流中的起始偏移
    pub size: u64,       // 分片字节长度
    pub etag: String,    // 分片内容校验（通常 MD5 hex）
}

/// Filer 目录/文件条目元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EntryMeta {
    pub name: String,           // 条目名（路径末段）
    pub is_directory: bool,     // 是否目录
    pub size: u64,              // 对象逻辑大小（字节）
    pub mtime: DateTime<Utc>,   // 修改时间
    pub crtime: DateTime<Utc>,  // 创建时间
    pub mode: u32,              // Unix 权限位
    pub mime: String,           // Content-Type
    pub etag: String,           // 对象 ETag
    /// 小文件：单 blob 的 FileId
    pub file_id: Option<String>,
    /// 大文件：分片列表
    pub chunks: Vec<ChunkRef>,
    pub ttl_seconds: u64,       // 条目 TTL（秒；0 表示不限期）
    pub encrypted: bool,        // 是否客户端/服务端加密
    pub version_id: Option<String>, // S3 版本 ID（若启用）
    pub delete_marker: bool,    // 版本删除标记
    pub user_metadata: std::collections::HashMap<String, String>, // 用户自定义元数据
}

impl EntryMeta {
    /// 构造目录条目（mode 0755，空 etag）
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

    /// 构造单 blob 文件条目（mode 0644）
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

/// S3 风格访问密钥与授权动作
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessKey {
    pub access_key: String,   // Access Key ID
    pub secret_key: String,   // Secret Access Key
    pub identity: String,     // 身份标识（用户/主体）
    pub actions: Vec<String>, // 允许的动作列表（如 s3:GetObject）
}
