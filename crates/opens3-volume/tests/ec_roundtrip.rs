// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use opens3_volume::ec::EcEncoder;
use std::fs;
use tempfile::tempdir;

#[test]
fn encode_and_reconstruct() {
    let dir = tempdir().unwrap();
    let src = dir.path().join("vol.dat");
    fs::write(&src, b"abcdefghijklmnopqrstuvwxyz0123456789").unwrap();
    let encoder = EcEncoder::new(2, 1).unwrap();
    let shards = encoder.encode_file(&src, dir.path(), 7).unwrap();
    assert_eq!(shards.len(), 3);
    // drop one data shard
    let mut paths: Vec<Option<_>> = shards.into_iter().map(Some).collect();
    paths[0] = None;
    let out = dir.path().join("rebuilt.dat");
    encoder.reconstruct(&paths, &out).unwrap();
    assert_eq!(fs::read(&out).unwrap(), fs::read(&src).unwrap());
}
