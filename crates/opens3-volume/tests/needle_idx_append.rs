// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use opens3_volume::needle::{
    NeedleVolume, SyncMode, VolumeIoOpts, DEFAULT_IDX_FLUSH_EVERY, IDX_ENTRY_SIZE,
};
use opens3_volume::store::VolumeStore;
use std::sync::Arc;
use std::thread;
use tempfile::tempdir;

#[test]
fn append_idx_grows_not_rewrite_full_map() {
    let dir = tempdir().unwrap();
    let opts = VolumeIoOpts {
        idx_flush_every: 1,
        sync_mode: SyncMode::None,
    };
    let mut vol = NeedleVolume::create_with_opts(dir.path(), 1, 64 * 1024 * 1024, &opts).unwrap();

    for i in 0..10u64 {
        vol.write_needle(i, 1, format!("body-{i}").as_bytes(), 0)
            .unwrap();
    }
    let idx_len = std::fs::metadata(dir.path().join("1.idx")).unwrap().len();
    assert_eq!(idx_len, (10 * IDX_ENTRY_SIZE) as u64);

    vol.write_needle(0, 2, b"new", 0).unwrap();
    let idx_len2 = std::fs::metadata(dir.path().join("1.idx")).unwrap().len();
    assert_eq!(idx_len2, (11 * IDX_ENTRY_SIZE) as u64);
    assert_eq!(vol.read_needle(0, 2).unwrap(), b"new");
}

#[test]
fn batch_flush_and_reopen_recovers() {
    let dir = tempdir().unwrap();
    {
        let opts = VolumeIoOpts {
            sync_mode: SyncMode::None,
            ..Default::default()
        };
        let mut vol =
            NeedleVolume::create_with_opts(dir.path(), 7, 32 * 1024 * 1024, &opts).unwrap();
        assert_eq!(vol.idx_flush_every, DEFAULT_IDX_FLUSH_EVERY);
        for i in 0..100u64 {
            vol.write_needle(i, 9, &[i as u8], 0).unwrap();
        }
    }
    let vol = NeedleVolume::open(dir.path(), 7, 32 * 1024 * 1024).unwrap();
    assert_eq!(vol.file_count, 100);
    assert_eq!(vol.read_needle(42, 9).unwrap(), &[42]);
}

#[test]
fn delete_tombstone_survives_reopen() {
    let dir = tempdir().unwrap();
    {
        let opts = VolumeIoOpts {
            idx_flush_every: 1,
            sync_mode: SyncMode::None,
        };
        let mut vol =
            NeedleVolume::create_with_opts(dir.path(), 3, 8 * 1024 * 1024, &opts).unwrap();
        vol.write_needle(5, 1, b"x", 0).unwrap();
        vol.delete_needle(5, 1).unwrap();
    }
    let vol = NeedleVolume::open(dir.path(), 3, 8 * 1024 * 1024).unwrap();
    assert!(vol.read_needle(5, 1).is_err());
    assert_eq!(vol.file_count, 0);
    assert_eq!(vol.deleted_count, 1);
}

#[test]
fn store_compact_still_works() {
    let dir = tempdir().unwrap();
    let store = VolumeStore::open_with_opts(
        dir.path(),
        Some(8 * 1024 * 1024),
        VolumeIoOpts {
            sync_mode: SyncMode::None,
            ..Default::default()
        },
    )
    .unwrap();
    store.create_volume(1).unwrap();
    store.write(1, 1, 1, b"a", 0).unwrap();
    store.write(1, 2, 1, b"b", 0).unwrap();
    store.delete(1, 1, 1).unwrap();
    let (before, after) = store.compact(1).unwrap();
    assert!(before > after);
    assert_eq!(store.read(1, 2, 1).unwrap(), b"b");
    assert!(store.read(1, 1, 1).is_err());
}

#[test]
fn concurrent_shared_reads_via_pread() {
    let dir = tempdir().unwrap();
    let store = Arc::new(
        VolumeStore::open_with_opts(
            dir.path(),
            Some(32 * 1024 * 1024),
            VolumeIoOpts {
                idx_flush_every: 1,
                sync_mode: SyncMode::None,
            },
        )
        .unwrap(),
    );
    store.create_volume(1).unwrap();
    for i in 0..64u64 {
        store
            .write(1, i, 7, format!("payload-{i}").as_bytes(), 0)
            .unwrap();
    }

    let mut handles = Vec::new();
    for t in 0..8 {
        let store = Arc::clone(&store);
        handles.push(thread::spawn(move || {
            for i in 0..64u64 {
                let key = (i + t) % 64;
                let got = store.read(1, key, 7).unwrap();
                assert_eq!(got, format!("payload-{key}").as_bytes());
            }
        }));
    }
    for h in handles {
        h.join().unwrap();
    }
}

#[test]
fn sync_mode_parse() {
    assert_eq!(SyncMode::parse("none"), Some(SyncMode::None));
    assert_eq!(SyncMode::parse("DATA"), Some(SyncMode::Data));
    assert_eq!(SyncMode::parse("all"), Some(SyncMode::All));
    assert_eq!(SyncMode::parse("nope"), None);
}

#[test]
fn reopen_truncates_torn_dat_tail_keeps_complete_needles() {
    let dir = tempdir().unwrap();
    let dat = dir.path().join("9.dat");
    let good_len;
    {
        let opts = VolumeIoOpts {
            idx_flush_every: 1,
            sync_mode: SyncMode::None,
        };
        let mut vol =
            NeedleVolume::create_with_opts(dir.path(), 9, 8 * 1024 * 1024, &opts).unwrap();
        vol.write_needle(1, 1, b"alpha", 0).unwrap();
        vol.write_needle(2, 1, b"bravo", 0).unwrap();
        vol.flush().unwrap();
        good_len = std::fs::metadata(&dat).unwrap().len();
        // Simulate power loss mid-append: garbage / partial needle after last CRC-complete end.
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new().append(true).open(&dat).unwrap();
        f.write_all(&[0xAAu8; 17]).unwrap();
        f.write_all(b"not-a-needle").unwrap();
    }
    let torn_len = std::fs::metadata(&dat).unwrap().len();
    assert!(torn_len > good_len);

    let vol = NeedleVolume::open(dir.path(), 9, 8 * 1024 * 1024).unwrap();
    let after = std::fs::metadata(&dat).unwrap().len();
    assert_eq!(after, good_len, "must truncate only torn EOF, keep complete needles");
    assert_eq!(vol.read_needle(1, 1).unwrap(), b"alpha");
    assert_eq!(vol.read_needle(2, 1).unwrap(), b"bravo");
    assert_eq!(vol.file_count, 2);
}
