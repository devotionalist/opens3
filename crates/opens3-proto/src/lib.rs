// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! OpenS3 gRPC / protobuf 生成代码入口；业务逻辑不在此 crate。

/// tonic 生成的 `opens3` 包（消息与服务定义）。
pub mod pb {
    tonic::include_proto!("opens3");
}

pub use pb::*;
