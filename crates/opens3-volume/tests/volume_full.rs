// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use opens3_volume::store::VolumeStore;
use tempfile::tempdir;

#[test]
fn second_write_fails_when_volume_full() {
    let dir = tempdir().unwrap();
    // header(16)+key(8)+payload+crc(4) ≈ 28 + payload; cap at 48 so second write fails
    let store = VolumeStore::open(dir.path(), Some(48)).unwrap();
    store.create_volume(1).unwrap();
    store.write(1, 1, 1, b"aaaa", 0).unwrap();
    let err = store.write(1, 2, 1, b"bbbb", 0);
    assert!(err.is_err(), "expected VolumeFull, got {err:?}");
}
