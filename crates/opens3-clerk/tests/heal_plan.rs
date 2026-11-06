// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use opens3_common::replication::Replication;
use opens3_common::topology::TopologyLocation;
use opens3_clerk::cluster::{ClusterState, VolumeServerInfo};
use opens3_clerk::heal::plan_heal;
use std::str::FromStr;

#[test]
fn heal_plans_missing_replica() {
    let repl = Replication::from_str("001").unwrap();
    let cluster = ClusterState::new(repl);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    for (id, url) in [("n1", "http://127.0.0.1:8080"), ("n2", "http://127.0.0.1:8081")] {
        cluster.heartbeat(
            VolumeServerInfo {
                node_id: id.into(),
                location: TopologyLocation::new("dc1", "rack1", id, url),
                max_volume_count: 0,
                free_disk_bytes: 100,
                last_seen: now,
                online: false,
                replica_of: None,
                boot_id: format!("boot-{id}"),
                write_ready: false,
            },
            vec![],
        );
        cluster.set_server_online(id, true).unwrap();
    }

    let grown = cluster.grow_volumes(1, "", repl).unwrap();
    assert_eq!(grown[0].locations.len(), 2);
    let vid = grown[0].id;
    // Online nodes → grown volumes start writable (no per-volume online).
    assert_eq!(grown[0].state, opens3_common::types::VolumeState::Writable);

    let a = cluster.assign("", Some(repl), 1, 0).unwrap();
    assert_eq!(a.fid.volume_id, vid);
    assert_eq!(a.locations.len(), 2);

    cluster.remove_volume_location(vid, "n2").unwrap();
    let tasks = plan_heal(&cluster);
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].volume_id, vid);
}
