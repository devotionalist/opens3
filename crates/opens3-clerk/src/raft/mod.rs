// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Master 侧共识：基于 [openraft](https://docs.rs/openraft) 的 Raft 封装。

mod network;
mod store;
mod wal;

use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::Arc;

use openraft::error::InitializeError;
use openraft::BasicNode;
use openraft::Config;
use openraft::ServerState;
use openraft::SnapshotPolicy;
use serde::{Deserialize, Serialize};

pub use network::Network;
pub use store::{LogStore, StateMachineStore};

use crate::cluster::ClusterState;

/// Raft 节点数值 ID（由 advertise URL / 名称派生）。
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// OpenS3 Clerk 的 openraft 类型配置。
    pub TypeConfig:
        D = RaftRequest,
        R = RaftResponse,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

/// openraft 实例别名。
pub type Raft = openraft::Raft<TypeConfig>;

/// 经 Raft 复制的客户端请求。
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftRequest {
    /// 集群拓扑变更（当前为全量 snapshot 载荷）。
    ClusterMutation { payload: serde_json::Value },
}

/// 请求 apply 后的应答。
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RaftResponse {
    pub ok: bool, // 是否成功应用
}

/// 供 ClerkServer / HTTP 使用的薄封装门面。
#[derive(Clone)]
pub struct RaftLite {
    pub raft: Raft,           // openraft 核心
    pub node_id: NodeId,      // 本节点 Raft ID
    /// 人类可读 id（pod 名 / ip:port），供状态 API 展示。
    pub node_name: String,
    pub listen_url: String,   // 本节点 advertise URL
}

impl RaftLite {
    /// 打开/创建 Raft：加载 WAL、状态机，并按需 bootstrap 成员。
    pub async fn open(
        raft_dir: &Path,
        node_name: String,
        listen_url: String,
        peers: Vec<String>,
        cluster: ClusterState,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(raft_dir)?;
        // NodeId 必须与 membership（由 URL 派生）一致；若用 `--ip`/node_name
        // 而 peers 按 advertise-url 哈希，会产生幽灵 voter / 故障转移 panic。
        let node_id = node_id_from_url(&listen_url);
        let name_id = name_to_node_id(&node_name);
        if name_id != node_id {
            tracing::warn!(
                %node_name,
                %listen_url,
                name_id,
                raft_node_id = node_id,
                "clerk --ip/node name id differs from advertise-url id; using advertise-url for Raft"
            );
        }

        // 默认 LogsSinceLast(5000) 对我们写入速率几乎不触发；缩短窗口以免
        // 重启时重放数千条全量集群快照。
        let config = Config {
            heartbeat_interval: 500,       // ms
            election_timeout_min: 1500,    // ms
            election_timeout_max: 3000,    // ms
            snapshot_policy: SnapshotPolicy::LogsSinceLast(32),
            max_in_snapshot_log_to_keep: 16,
            ..Default::default()
        };
        let config = Arc::new(config.validate()?);

        let log_store = LogStore::<TypeConfig>::open(raft_dir)?;
        let state_machine = Arc::new(StateMachineStore::open(raft_dir, cluster)?);
        let network = Network {};

        let raft = Raft::new(
            node_id,
            config,
            network,
            log_store,
            state_machine,
        )
        .await?;

        let members = build_members(&listen_url, &peers)?;
        maybe_bootstrap(&raft, node_id, &peers, members).await?;

        Ok(Self {
            raft,
            node_id,
            node_name,
            listen_url,
        })
    }

    /// 当前是否为本集群 Leader。
    pub fn is_leader(&self) -> bool {
        let m = self.raft.metrics().borrow().clone();
        m.current_leader == Some(self.node_id) && m.state == ServerState::Leader
    }

    /// 当前 Leader 的 NodeId 字符串；未知则为空。
    pub fn leader(&self) -> String {
        self.raft
            .metrics()
            .borrow()
            .current_leader
            .map(|id| id.to_string())
            .unwrap_or_default()
    }

    /// Leader 的 advertise URL；未知时回退本节点 URL。
    pub fn leader_url(&self) -> String {
        let m = self.raft.metrics().borrow().clone();
        let Some(leader) = m.current_leader else {
            return self.listen_url.clone();
        };
        if leader == self.node_id {
            return self.listen_url.clone();
        }
        m.membership_config
            .membership()
            .get_node(&leader)
            .map(|n| n.addr.clone())
            .unwrap_or_else(|| self.listen_url.clone())
    }

    /// 当前任期号。
    pub fn term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
    }

    /// 已应用到状态机的最后日志 index。
    pub fn commit_index(&self) -> u64 {
        self.raft
            .metrics()
            .borrow()
            .last_applied
            .map(|id| id.index)
            .unwrap_or(0)
    }

    /// 本地已知的最后日志 index。
    pub fn last_log_index(&self) -> u64 {
        self.raft
            .metrics()
            .borrow()
            .last_log_index
            .unwrap_or(0)
    }

    /// 角色名：Leader / Follower / Candidate / Learner / Shutdown。
    pub fn role_name(&self) -> &'static str {
        match self.raft.metrics().borrow().state {
            ServerState::Leader => "Leader",
            ServerState::Follower => "Follower",
            ServerState::Candidate => "Candidate",
            ServerState::Learner => "Learner",
            ServerState::Shutdown => "Shutdown",
        }
    }

    /// 是否已进入可服务状态（非选主中的 Candidate）。
    pub fn is_ready(&self) -> bool {
        matches!(
            self.raft.metrics().borrow().state,
            ServerState::Leader | ServerState::Follower | ServerState::Learner
        )
    }

    /// 提交一条客户端写请求，返回日志 index。
    pub async fn propose(&self, req: RaftRequest) -> anyhow::Result<u64> {
        let resp = self
            .raft
            .client_write(req)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(resp.log_id.index)
    }

    /// 触发 openraft 构建快照并清理已压缩日志。
    pub async fn trigger_snapshot(&self) {
        let _ = self.raft.trigger().snapshot().await;
    }

    /// 空操作：选举超时由 openraft 自行管理。
    pub fn bump_heartbeat(&self) {}
}

/// 仅按 advertise URL（`node_id_from_url`）构建 voter 集；拒绝冲突的 id↔addr 绑定。
fn build_members(
    listen_url: &str,
    peers: &[String],
) -> anyhow::Result<BTreeMap<NodeId, BasicNode>> {
    let mut members = BTreeMap::new();
    insert_member(&mut members, listen_url)?;
    for url in peers {
        insert_member(&mut members, url)?;
    }
    for (id, node) in &members {
        let derived = node_id_from_url(&node.addr);
        if derived != *id {
            anyhow::bail!(
                "raft membership corrupt: id={id} addr={} derives id={derived}",
                node.addr
            );
        }
    }
    Ok(members)
}

/// 将单个 URL 插入成员表；同 id 不同地址或同地址不同 id 均报错。
fn insert_member(
    members: &mut BTreeMap<NodeId, BasicNode>,
    url: &str,
) -> anyhow::Result<()> {
    let url = url.trim().trim_end_matches('/');
    if url.is_empty() {
        return Ok(());
    }
    let id = node_id_from_url(url);
    let norm = normalize_raft_addr(url);

    if let Some(prev) = members.get(&id) {
        if normalize_raft_addr(&prev.addr) != norm {
            anyhow::bail!(
                "raft voter id {id} maps to both '{}' and '{url}' — check advertise-url / peers",
                prev.addr
            );
        }
        return Ok(());
    }
    if let Some((other_id, other)) = members
        .iter()
        .find(|(_, n)| normalize_raft_addr(&n.addr) == norm)
    {
        anyhow::bail!(
            "raft advertise '{url}' (id={id}) conflicts with existing id={other_id} addr={}",
            other.addr
        );
    }
    members.insert(id, BasicNode { addr: url.to_string() });
    Ok(())
}

/// 规范化地址以便比较（去 scheme / 尾斜杠，转小写）。
fn normalize_raft_addr(url: &str) -> String {
    url.trim()
        .trim_end_matches('/')
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .to_ascii_lowercase()
}

/// 优先 StatefulSet ordinal 0 做 bootstrap；否则取最小数值 id。
fn is_bootstrap_node(node_id: NodeId, members: &BTreeMap<NodeId, BasicNode>) -> bool {
    if members.len() == 1 {
        return true;
    }
    if node_id == 0 {
        return true;
    }
    members.keys().next().copied() == Some(node_id)
}

/// 若任一 peer 已有存活 Raft 日志 / Leader，则禁止再 `initialize()` 空集群
/// （master-0 emptyDir 重启后的常见脑裂场景）。
async fn peer_cluster_already_formed(peers: &[String]) -> bool {
    if peers.is_empty() {
        return false;
    }
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(2))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());
    for url in peers {
        let base = url.trim_end_matches('/');
        let endpoints = [
            format!("{base}/raft/status"),
            format!("{base}/cluster/status"),
        ];
        for ep in endpoints {
            let Ok(resp) = client.get(&ep).send().await else {
                continue;
            };
            if !resp.status().is_success() {
                continue;
            }
            let Ok(v) = resp.json::<serde_json::Value>().await else {
                continue;
            };
            let commit = v
                .get("commit_index")
                .and_then(|x| x.as_u64())
                .or_else(|| {
                    v.get("last_log_index")
                        .and_then(|x| x.as_u64())
                })
                .unwrap_or(0);
            let has_leader = v
                .get("leader")
                .map(|l| match l {
                    // openraft NodeId 0 是合法 Leader id
                    serde_json::Value::String(s) => !s.is_empty(),
                    serde_json::Value::Number(_) => true,
                    _ => false,
                })
                .unwrap_or(false);
            let is_leader = v
                .get("is_leader")
                .and_then(|x| x.as_bool())
                .unwrap_or(false);
            if commit > 0 || has_leader || is_leader {
                tracing::info!(%ep, commit, has_leader, is_leader, "peer already has raft state");
                return true;
            }
        }
    }
    false
}

/// Bootstrap 节点尝试 `initialize`；已初始化或 peer 已成团则跳过。
async fn maybe_bootstrap(
    raft: &Raft,
    node_id: NodeId,
    peers: &[String],
    members: BTreeMap<NodeId, BasicNode>,
) -> anyhow::Result<()> {
    if !is_bootstrap_node(node_id, &members) {
        tracing::info!(node_id, "openraft waiting for leader bootstrap");
        return Ok(());
    }
    // 环境变量强制 bootstrap（测试 / 显式恢复）。
    let force = std::env::var("OPENS3_RAFT_BOOTSTRAP")
        .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if !force && members.len() > 1 && peer_cluster_already_formed(peers).await {
        tracing::warn!(
            node_id,
            "skipping openraft initialize: peers already formed a cluster (avoid split-brain after empty restart)"
        );
        return Ok(());
    }
    match raft.initialize(members).await {
        Ok(()) => {
            tracing::info!(node_id, "openraft cluster initialized");
            Ok(())
        }
        Err(e) => match e {
            openraft::error::RaftError::APIError(InitializeError::NotAllowed { .. })
            | openraft::error::RaftError::APIError(InitializeError::NotInMembers { .. }) => {
                tracing::debug!(node_id, "openraft already initialized: {e}");
                Ok(())
            }
            other => Err(anyhow::anyhow!("openraft initialize: {other}")),
        },
    }
}

/// 将人类可读节点名映射为紧凑 Raft NodeId。
/// - `opens3-clerk-0` → `0`（StatefulSet ordinal）
/// - 其它 → 稳定哈希（避开低 ordinal 区间）
pub fn name_to_node_id(name: &str) -> NodeId {
    if let Some((_, suffix)) = name.rsplit_once('-') {
        if let Ok(n) = suffix.parse::<u64>() {
            // 避免与哈希 id 冲突：ordinal 保持较小。
            return n;
        }
    }
    let mut h = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut h);
    // 避开 StatefulSet 常用的低 ordinal 区间（0..1024）。
    1_000_000 + (h.finish() % 1_000_000_000)
}

/// 从 peer advertise URL 提取 Raft 节点 id。
/// - K8s DNS: `http://opens3-clerk-0.hl.ns.svc:9333` → `0`
/// - 裸 IP: `http://127.0.0.1:9333` → hash(`127.0.0.1:9333`)
pub fn node_id_from_url(url: &str) -> NodeId {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .unwrap_or(url);
    let hostport = rest.split('/').next().unwrap_or(rest);
    let hostport = hostport.split('@').last().unwrap_or(hostport);
    let host = hostport.split(':').next().unwrap_or(hostport);
    if host.parse::<std::net::IpAddr>().is_ok() {
        return name_to_node_id(hostport);
    }
    let label = host.split('.').next().unwrap_or(host);
    name_to_node_id(label)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_peer_urls() {
        assert_eq!(
            node_id_from_url(
                "http://opens3-clerk-1.opens3-clerk-hl.opens3.svc.cluster.local:9333"
            ),
            1
        );
        assert_eq!(
            node_id_from_url("http://127.0.0.1:9333"),
            name_to_node_id("127.0.0.1:9333")
        );
        assert_eq!(name_to_node_id("opens3-clerk-0"), 0);
    }

    #[test]
    fn membership_uses_advertise_url_not_pod_name() {
        // Regression: --ip opens3-clerk-0 + IP advertise must not also insert id=0.
        let listen = "http://127.0.0.1:58331";
        let peers = vec![
            "http://127.0.0.1:58331".into(),
            "http://127.0.0.1:58332".into(),
            "http://127.0.0.1:58333".into(),
        ];
        let members = build_members(listen, &peers).unwrap();
        assert_eq!(members.len(), 3, "expected 3 voters, got {members:?}");
        assert!(members.contains_key(&node_id_from_url(listen)));
        assert!(!members.contains_key(&name_to_node_id("opens3-clerk-0")));
    }

    #[test]
    fn membership_sts_dns_keeps_ordinals() {
        let listen = "http://opens3-clerk-0.opens3-clerk-hl.svc:9333";
        let peers = vec![
            "http://opens3-clerk-0.opens3-clerk-hl.svc:9333".into(),
            "http://opens3-clerk-1.opens3-clerk-hl.svc:9333".into(),
            "http://opens3-clerk-2.opens3-clerk-hl.svc:9333".into(),
        ];
        let members = build_members(listen, &peers).unwrap();
        assert_eq!(members.len(), 3);
        for id in 0..3 {
            assert!(members.contains_key(&id));
        }
        assert!(is_bootstrap_node(0, &members));
        assert!(!is_bootstrap_node(1, &members));
    }

    #[test]
    fn membership_rejects_same_id_different_addrs() {
        let listen = "http://opens3-clerk-0.a.svc:9333";
        let peers = vec![
            "http://opens3-clerk-0.a.svc:9333".into(),
            "http://opens3-clerk-0.b.svc:9333".into(),
        ];
        let err = build_members(listen, &peers).unwrap_err().to_string();
        assert!(
            err.contains("maps to both"),
            "unexpected err: {err}"
        );
    }
}
