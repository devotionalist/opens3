// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Three-node openraft election + propose smoke test over real HTTP.
use axum::{routing::post, Json, Router};
use opens3_common::replication::Replication;
use opens3_clerk::cluster::ClusterState;
use opens3_clerk::raft::{RaftLite, RaftRequest, TypeConfig};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use std::net::SocketAddr;
use std::time::Duration;
use tempfile::tempdir;
use tokio::sync::oneshot;

async fn serve_node(raft: RaftLite, addr: SocketAddr) -> oneshot::Sender<()> {
    let r1 = raft.clone();
    let r2 = raft.clone();
    let r3 = raft.clone();
    let app = Router::new()
        .route(
            "/raft/vote",
            post(move |Json(req): Json<VoteRequest<opens3_clerk::raft::NodeId>>| {
                let r = r1.clone();
                async move { Json(r.raft.vote(req).await) }
            }),
        )
        .route(
            "/raft/append",
            post(move |Json(req): Json<AppendEntriesRequest<TypeConfig>>| {
                let r = r2.clone();
                async move { Json(r.raft.append_entries(req).await) }
            }),
        )
        .route(
            "/raft/snapshot",
            post(move |Json(req): Json<InstallSnapshotRequest<TypeConfig>>| {
                let r = r3.clone();
                async move { Json(r.raft.install_snapshot(req).await) }
            }),
        );
    let (tx, rx) = oneshot::channel::<()>();
    tokio::spawn(async move {
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await
            .ok();
    });
    tx
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_elects_one_leader_and_proposes() {
    let d0 = tempdir().unwrap();
    let d1 = tempdir().unwrap();
    let d2 = tempdir().unwrap();

    let l0 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l1 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let l2 = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a0 = l0.local_addr().unwrap();
    let a1 = l1.local_addr().unwrap();
    let a2 = l2.local_addr().unwrap();
    drop(l0);
    drop(l1);
    drop(l2);

    let u0 = format!("http://{a0}");
    let u1 = format!("http://{a1}");
    let u2 = format!("http://{a2}");
    let peers = vec![u0.clone(), u1.clone(), u2.clone()];

    // Node ids must match node_id_from_url(advertise) for IP-based peers.
    let id0 = format!("127.0.0.1:{}", a0.port());
    let id1 = format!("127.0.0.1:{}", a1.port());
    let id2 = format!("127.0.0.1:{}", a2.port());

    let cluster = || ClusterState::new(Replication::DEFAULT);
    let n0 = RaftLite::open(d0.path(), id0, u0, peers.clone(), cluster())
        .await
        .unwrap();
    let n1 = RaftLite::open(d1.path(), id1, u1, peers.clone(), cluster())
        .await
        .unwrap();
    let n2 = RaftLite::open(d2.path(), id2, u2, peers.clone(), cluster())
        .await
        .unwrap();

    let _s0 = serve_node(n0.clone(), a0).await;
    let _s1 = serve_node(n1.clone(), a1).await;
    let _s2 = serve_node(n2.clone(), a2).await;

    let nodes = [n0.clone(), n1.clone(), n2.clone()];
    let mut leader: Option<RaftLite> = None;
    for _ in 0..100 {
        tokio::time::sleep(Duration::from_millis(200)).await;
        let leaders: Vec<_> = nodes.iter().filter(|n| n.is_leader()).collect();
        if leaders.len() == 1 {
            leader = Some(leaders[0].clone());
            break;
        }
    }
    let leader = leader.expect("exactly one leader elected");
    assert_eq!(
        nodes.iter().filter(|n| n.is_leader()).count(),
        1,
        "split brain"
    );

    let idx = leader
        .propose(RaftRequest::ClusterMutation {
            payload: serde_json::json!({"op": "scale-test"}),
        })
        .await
        .expect("propose on leader");
    assert!(idx >= 1);

    tokio::time::sleep(Duration::from_millis(1500)).await;
    for n in &nodes {
        assert!(
            n.commit_index() >= idx,
            "{} commit {} < {idx}",
            n.node_id,
            n.commit_index()
        );
    }

    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert_eq!(nodes.iter().filter(|n| n.is_leader()).count(), 1);
}
