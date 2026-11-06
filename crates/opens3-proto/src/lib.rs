// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
pub mod pb {
    tonic::include_proto!("opens3");
}

pub use pb::*;
