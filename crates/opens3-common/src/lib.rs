// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
pub mod error;
pub mod fileid;
pub mod grpc_endpoint;
pub mod jwt;
pub mod metrics;
pub mod replication;
pub mod tls;
pub mod topology;
pub mod types;
pub mod util;

#[cfg(test)]
mod integration_tests {
    use crate::fileid::FileId;
    use crate::replication::Replication;
    use std::str::FromStr;

    #[test]
    fn fileid_and_replication() {
        let f = FileId::new(9, 0xdead, 0xbeef0001);
        assert_eq!(f, f.fid().parse().unwrap());
        assert_eq!(Replication::from_str("010").unwrap().total_copies(), 2);
    }
}

pub use error::{Error, Result};
pub use fileid::FileId;
pub use grpc_endpoint::volume_grpc_endpoint;
pub use replication::Replication;
pub use topology::{DataCenter, NodeId, Rack, TopologyLocation};
pub use types::*;
