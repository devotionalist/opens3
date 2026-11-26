// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Volume 节点 crate：Needle 磁盘卷、EC、限速推送与 HTTP/gRPC 服务。

/// 纠删码（Reed-Solomon）编解码
pub mod ec;
/// Needle 卷磁盘格式与 vacuum
pub mod needle;
/// Volume HTTP / gRPC 服务端
pub mod server;
/// 多卷目录管理与并发读写
pub mod store;
/// heal/balance 推送平均速率限制
pub mod throttle;

pub use needle::{SyncMode, VolumeIoOpts, DEFAULT_IDX_FLUSH_EVERY, IDX_ENTRY_SIZE};
pub use server::VolumeServer;
pub use store::VolumeStore;
