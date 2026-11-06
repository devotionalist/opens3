// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use serde::{Deserialize, Serialize};

pub type NodeId = String;
pub type DataCenter = String;
pub type Rack = String;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopologyLocation { // 拓扑位置
    pub data_center: DataCenter, // 数据中心
    pub rack: Rack, // 机架
    pub node_id: NodeId, // 节点ID
    pub url: String, // URL
    pub public_url: String, // 公共URL
}

impl TopologyLocation {
    pub fn new(
        data_center: impl Into<String>,
        rack: impl Into<String>,
        node_id: impl Into<String>,
        url: impl Into<String>,
    ) -> Self {
        let url = url.into();
        Self {
            data_center: data_center.into(),
            rack: rack.into(),
            node_id: node_id.into(),
            public_url: url.clone(),
            url,
        }
    }
}

impl Default for TopologyLocation {
    fn default() -> Self {
        Self {
            data_center: "dc1".into(),
            rack: "rack1".into(),
            node_id: "node1".into(),
            url: "http://127.0.0.1:8080".into(),
            public_url: "http://127.0.0.1:8080".into(),
        }
    }
}
