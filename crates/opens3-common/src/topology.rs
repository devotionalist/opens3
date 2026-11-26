// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 数据中心 / 机架 / 节点拓扑位置，用于复制放置与心跳登记。

use serde::{Deserialize, Serialize};

/// Volume / Clerk 节点唯一标识
pub type NodeId = String;
/// 数据中心名称
pub type DataCenter = String;
/// 机架名称
pub type Rack = String;

/// 拓扑位置：副本落点 + 对内/对外 URL
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopologyLocation {
    pub data_center: DataCenter, // 数据中心
    pub rack: Rack,              // 机架
    pub node_id: NodeId,         // 节点 ID
    pub url: String,             // 集群内访问 URL
    pub public_url: String,      // 对外/客户端可见 URL
}

impl TopologyLocation {
    /// 构造拓扑位置；`public_url` 默认与 `url` 相同
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
    /// 本地单机开发默认拓扑
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
