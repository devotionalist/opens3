// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! OpenS3 Clerk（Master）：拓扑、分配、heal/vacuum 编排与 Raft 共识入口。

pub mod cluster;
pub mod heal;
pub mod raft;
pub mod server;
pub mod vacuum;

pub use cluster::{
    AssignPolicy, ClusterSnapshot, ClusterState, HeartbeatOutcome, VolumeFingerprint,
};
pub use server::ClerkServer;
