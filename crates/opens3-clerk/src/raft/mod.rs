// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Master consensus via [openraft](https://docs.rs/openraft).

mod network;
mod store;

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

pub type NodeId = u64;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = RaftRequest,
        R = RaftResponse,
        NodeId = NodeId,
        Node = BasicNode,
        Entry = openraft::Entry<TypeConfig>,
        SnapshotData = std::io::Cursor<Vec<u8>>,
        AsyncRuntime = openraft::TokioRuntime,
);

pub type Raft = openraft::Raft<TypeConfig>;

/// Client request replicated through Raft.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum RaftRequest {
    ClusterMutation { payload: serde_json::Value },
}

/// Response returned after a request is applied.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct RaftResponse {
    pub ok: bool,
}

/// Thin façade kept for ClerkServer / HTTP handlers.
#[derive(Clone)]
pub struct RaftLite {
    pub raft: Raft,
    pub node_id: NodeId,
    /// Human-readable id (pod name / ip:port) for status APIs.
    pub node_name: String,
    pub listen_url: String,
}

impl RaftLite {
    pub async fn open(
        raft_dir: &Path,
        node_name: String,
        listen_url: String,
        peers: Vec<String>,
        cluster: ClusterState,
    ) -> anyhow::Result<Self> {
        std::fs::create_dir_all(raft_dir)?;
        let node_id = name_to_node_id(&node_name);

        // Default LogsSinceLast(5000) never fires for our write rate; keep a short
        // window so restart does not replay thousands of full cluster snapshots.
        let config = Config {
            heartbeat_interval: 500,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
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

        let members = build_members(&node_name, node_id, &listen_url, &peers);
        maybe_bootstrap(&raft, node_id, &peers, members).await?;

        Ok(Self {
            raft,
            node_id,
            node_name,
            listen_url,
        })
    }

    pub fn is_leader(&self) -> bool {
        let m = self.raft.metrics().borrow().clone();
        m.current_leader == Some(self.node_id) && m.state == ServerState::Leader
    }

    pub fn leader(&self) -> String {
        self.raft
            .metrics()
            .borrow()
            .current_leader
            .map(|id| id.to_string())
            .unwrap_or_default()
    }

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

    pub fn term(&self) -> u64 {
        self.raft.metrics().borrow().current_term
    }

    pub fn commit_index(&self) -> u64 {
        self.raft
            .metrics()
            .borrow()
            .last_applied
            .map(|id| id.index)
            .unwrap_or(0)
    }

    pub fn last_log_index(&self) -> u64 {
        self.raft
            .metrics()
            .borrow()
            .last_log_index
            .unwrap_or(0)
    }

    pub fn role_name(&self) -> &'static str {
        match self.raft.metrics().borrow().state {
            ServerState::Leader => "Leader",
            ServerState::Follower => "Follower",
            ServerState::Candidate => "Candidate",
            ServerState::Learner => "Learner",
            ServerState::Shutdown => "Shutdown",
        }
    }

    pub fn is_ready(&self) -> bool {
        matches!(
            self.raft.metrics().borrow().state,
            ServerState::Leader | ServerState::Follower | ServerState::Learner
        )
    }

    pub async fn propose(&self, req: RaftRequest) -> anyhow::Result<u64> {
        let resp = self
            .raft
            .client_write(req)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        Ok(resp.log_id.index)
    }

    /// Ask openraft to build a snapshot and purge compacted logs.
    pub async fn trigger_snapshot(&self) {
        let _ = self.raft.trigger().snapshot().await;
    }

    /// No-op: openraft manages election timeouts itself.
    pub fn bump_heartbeat(&self) {}
}

fn build_members(
    node_name: &str,
    node_id: NodeId,
    listen_url: &str,
    peers: &[String],
) -> BTreeMap<NodeId, BasicNode> {
    let mut members = BTreeMap::new();
    members.insert(
        node_id,
        BasicNode {
            addr: listen_url.to_string(),
        },
    );
    let _ = node_name;
    for url in peers {
        let id = node_id_from_url(url);
        members.insert(
            id,
            BasicNode {
                addr: url.clone(),
            },
        );
    }
    members
}

/// Prefer StatefulSet ordinal 0 as bootstrap; else smallest numeric id.
fn is_bootstrap_node(node_id: NodeId, members: &BTreeMap<NodeId, BasicNode>) -> bool {
    if members.len() == 1 {
        return true;
    }
    if node_id == 0 {
        return true;
    }
    members.keys().next().copied() == Some(node_id)
}

/// True if any peer already has a live Raft log / leader — then we must NOT
/// `initialize()` a competing empty cluster (common after emptyDir restart of master-0).
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
                    // openraft NodeId 0 is a valid leader id
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
    // Force bootstrap via env (tests / explicit recovery).
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

/// Map a human node name to a compact Raft NodeId.
/// - `opens3-clerk-0` → `0` (StatefulSet ordinal)
/// - anything else → stable hash
pub fn name_to_node_id(name: &str) -> NodeId {
    if let Some((_, suffix)) = name.rsplit_once('-') {
        if let Ok(n) = suffix.parse::<u64>() {
            // Avoid colliding with hashed ids: ordinals stay small.
            return n;
        }
    }
    let mut h = std::collections::hash_map::DefaultHasher::new();
    name.hash(&mut h);
    // Keep out of the low ordinal range used by StatefulSet (0..1024).
    1_000_000 + (h.finish() % 1_000_000_000)
}

/// Extract Raft node id from a peer advertise URL.
/// - K8s DNS: `http://opens3-clerk-0.hl.ns.svc:9333` → `0`
/// - Bare IP: `http://127.0.0.1:9333` → hash(`127.0.0.1:9333`)
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
}
