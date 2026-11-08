// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use opens3_volume::store::VolumeStore;
use tempfile::tempdir;

#[test]
fn vacuum_check_compact_commit_cleanup() {
    let dir = tempdir().unwrap();
    let store = VolumeStore::open(dir.path(), Some(8 * 1024 * 1024)).unwrap();
    store.create_volume(1).unwrap();
    store.write(1, 1, 1, b"keep-me", 0).unwrap();
    store.write(1, 2, 2, b"delete-me", 0).unwrap();
    store.delete(1, 2, 2).unwrap();

    let ratio = store.vacuum_check(1).unwrap();
    assert!(ratio > 0.0);

    let (before, processed) = store.vacuum_compact(1).unwrap();
    assert!(before >= processed);
    // Old dat still serves reads while vacuuming.
    assert_eq!(store.read(1, 1, 1).unwrap(), b"keep-me");
    assert!(store.write(1, 3, 3, b"blocked", 0).is_err());

    assert!(dir.path().join("1.cpd").exists());
    assert!(dir.path().join("1.cpx").exists());

    let after = store.vacuum_commit(1).unwrap();
    assert!(after <= before);
    assert_eq!(store.read(1, 1, 1).unwrap(), b"keep-me");
    assert!(store.read(1, 2, 2).is_err());

    store.vacuum_cleanup(1).unwrap();
    assert!(!dir.path().join("1.cpd").exists());
    assert!(!dir.path().join("1.cpx").exists());
    assert!(!dir.path().join("1.cpc").exists());

    // Writable again.
    store.write(1, 4, 4, b"after", 0).unwrap();
    assert_eq!(store.read(1, 4, 4).unwrap(), b"after");
}

#[test]
fn vacuum_reconcile_completes_cpc_commit() {
    let dir = tempdir().unwrap();
    let store = VolumeStore::open(dir.path(), Some(8 * 1024 * 1024)).unwrap();
    store.create_volume(9).unwrap();
    store.write(9, 1, 1, b"alive", 0).unwrap();
    store.write(9, 2, 2, b"gone", 0).unwrap();
    store.delete(9, 2, 2).unwrap();
    let _ = store.vacuum_compact(9).unwrap();

    // Simulate crash after .cpc was written: finish marker then drop store without commit.
    std::fs::write(dir.path().join("9.cpc"), b"opens3-vacuum-commit\n").unwrap();
    drop(store);

    let store2 = VolumeStore::open(dir.path(), Some(8 * 1024 * 1024)).unwrap();
    assert_eq!(store2.read(9, 1, 1).unwrap(), b"alive");
    assert!(!dir.path().join("9.cpd").exists());
    assert!(!dir.path().join("9.cpc").exists());
}
