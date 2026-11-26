// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! openraft RPC 的 HTTP 传输层（reqwest 客户端）。

use openraft::error::{NetworkError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::BasicNode;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::{NodeId, TypeConfig};

/// Raft 网络工厂 / 共享 RPC 发送器（无状态）。
pub struct Network {}

impl Network {
    /// 向对端 `target_node` 的 `{addr}/{uri}` POST JSON，并解析响应。
    pub async fn send_rpc<Req, Resp, Err>(
        &self,
        target: NodeId,
        target_node: &BasicNode,
        uri: &str,
        req: Req,
    ) -> Result<Resp, RPCError<NodeId, BasicNode, Err>>
    where
        Req: Serialize,
        Err: std::error::Error + DeserializeOwned,
        Resp: DeserializeOwned,
    {
        let base = target_node.addr.trim_end_matches('/');
        let url = format!("{base}/{uri}");
        let client = reqwest::Client::new();
        let resp = client.post(&url).json(&req).send().await.map_err(|e| {
            // 连接失败标为 Unreachable，便于 openraft 区分瞬态网络错误
            if e.is_connect() {
                return RPCError::Unreachable(Unreachable::new(&e));
            }
            RPCError::Network(NetworkError::new(&e))
        })?;

        let res: Result<Resp, Err> = resp
            .json()
            .await
            .map_err(|e| RPCError::Network(NetworkError::new(&e)))?;

        res.map_err(|e| RPCError::RemoteError(RemoteError::new(target, e)))
    }
}

impl RaftNetworkFactory<TypeConfig> for Network {
    type Network = NetworkConnection;

    /// 为对端节点创建 RPC 客户端。
    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        NetworkConnection {
            owner: Network {},
            target,
            target_node: node.clone(),
        }
    }
}

/// 指向单一 Raft peer 的连接句柄（由工厂按目标节点创建）。
pub struct NetworkConnection {
    owner: Network,           // 复用发送逻辑
    target: NodeId,           // 对端 Raft NodeId
    target_node: BasicNode,   // 对端 advertise 地址等
}

impl RaftNetwork<TypeConfig> for NetworkConnection {
    /// AppendEntries RPC → `raft/append`。
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.owner
            .send_rpc(self.target.clone(), &self.target_node, "raft/append", req)
            .await
    }

    /// InstallSnapshot RPC → `raft/snapshot`。
    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        self.owner
            .send_rpc(self.target.clone(), &self.target_node, "raft/snapshot", req)
            .await
    }

    /// Vote RPC → `raft/vote`。
    async fn vote(
        &mut self,
        req: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        self.owner
            .send_rpc(self.target.clone(), &self.target_node, "raft/vote", req)
            .await
    }
}
