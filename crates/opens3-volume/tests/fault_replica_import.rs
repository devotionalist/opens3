// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Fault-oriented: volume import after "loss" of a replica disk.

use opens3_volume::store::VolumeStore;
use tempfile::tempdir;

#[test]
fn recover_volume_from_exported_bytes() {
    let primary = tempdir().unwrap();
    let replica = tempdir().unwrap();
    let store_a = VolumeStore::open(primary.path(), Some(4 * 1024 * 1024)).unwrap();
    store_a.create_volume(9).unwrap();
    store_a.write(9, 1, 42, b"critical-data", 0).unwrap();

    // simulate replica by copying files (what replicate_volume ships)
    std::fs::copy(
        primary.path().join("9.dat"),
        replica.path().join("9.dat"),
    )
    .unwrap();
    std::fs::copy(
        primary.path().join("9.idx"),
        replica.path().join("9.idx"),
    )
    .unwrap();

    // primary "dies"
    drop(store_a);
    std::fs::remove_file(primary.path().join("9.dat")).unwrap();

    // replica still serves
    let store_b = VolumeStore::open(replica.path(), Some(4 * 1024 * 1024)).unwrap();
    let data = store_b.read(9, 1, 42).unwrap();
    assert_eq!(data, b"critical-data");
}

#[test]
fn bitrot_detected_on_checksum_mismatch() {
    let dir = tempdir().unwrap();
    let store = VolumeStore::open(dir.path(), Some(4 * 1024 * 1024)).unwrap();
    store.create_volume(1).unwrap();
    store.write(1, 7, 1, b"abcdef", 0).unwrap();
    // corrupt dat file
    let dat = dir.path().join("1.dat");
    let mut bytes = std::fs::read(&dat).unwrap();
    if let Some(b) = bytes.last_mut() {
        *b ^= 0xff;
    }
    std::fs::write(&dat, bytes).unwrap();
    // reopen to clear caches
    let store2 = VolumeStore::open(dir.path(), Some(4 * 1024 * 1024)).unwrap();
    assert!(store2.read(1, 7, 1).is_err());
}
