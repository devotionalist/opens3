// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use opens3_volume::store::VolumeStore;
use tempfile::tempdir;

#[test]
fn write_read_delete_compact() {
    let dir = tempdir().unwrap();
    let store = VolumeStore::open(dir.path(), Some(8 * 1024 * 1024)).unwrap();
    store.create_volume(1).unwrap();
    let (size, etag) = store.write(1, 42, 0xabcd, b"hello-needle", 0).unwrap();
    assert_eq!(size, 12);
    assert!(!etag.is_empty());
    let data = store.read(1, 42, 0xabcd).unwrap();
    assert_eq!(data, b"hello-needle");
    store.delete(1, 42, 0xabcd).unwrap();
    assert!(store.read(1, 42, 0xabcd).is_err());
    store.write(1, 43, 1, b"keep", 0).unwrap();
    let (before, after) = store.compact(1).unwrap();
    assert!(before >= after);
    assert_eq!(store.read(1, 43, 1).unwrap(), b"keep");
}
