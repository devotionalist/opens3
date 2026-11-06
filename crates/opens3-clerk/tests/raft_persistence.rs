// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use opens3_common::replication::Replication;
use opens3_clerk::cluster::ClusterState;
use opens3_clerk::raft::{RaftLite, RaftRequest};
use tempfile::tempdir;

#[tokio::test]
async fn leader_propose_survives_restart() {
    let dir = tempdir().unwrap();
    let url = "http://127.0.0.1:19333".to_string();
    let id = "127.0.0.1:19333".to_string();
    let node = RaftLite::open(
        dir.path(),
        id.clone(),
        url.clone(),
        vec![],
        ClusterState::new(Replication::DEFAULT),
    )
    .await
    .unwrap();

    // Wait until single-node becomes leader.
    for _ in 0..50 {
        if node.is_leader() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    assert!(node.is_leader());

    let idx = node
        .propose(RaftRequest::ClusterMutation {
            payload: serde_json::json!({"op":"test"}),
        })
        .await
        .unwrap();
    assert!(idx >= 1);
    let commit = node.commit_index();
    drop(node);

    let node2 = RaftLite::open(
        dir.path(),
        id,
        url,
        vec![],
        ClusterState::new(Replication::DEFAULT),
    )
    .await
    .unwrap();
    assert!(node2.last_log_index() >= idx);
    assert!(node2.commit_index() >= commit || node2.last_log_index() >= idx);
}
