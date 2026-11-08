// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Append-only Raft log WAL (TiKV raft-engine inspired, single-group).
//!
//! Hot path only appends records. Truncate / purge append logical markers.
//! Physical reclaim happens via occasional rewrite compaction.

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};

use openraft::{Entry, LogId, RaftLogId, StorageError, StorageIOError, Vote};
use serde::{Deserialize, Serialize};

use super::{NodeId, TypeConfig};

const MAGIC: &[u8; 8] = b"O3RFWAL1";
const VERSION: u32 = 1;
const HEADER_LEN: u64 = 5;
const CRC_LEN: u64 = 4;
const FILE_HEADER_LEN: u64 = MAGIC.len() as u64 + 4;

const REC_ENTRY: u8 = 1;
const REC_VOTE: u8 = 2;
const REC_COMMITTED: u8 = 3;
const REC_TRUNCATE: u8 = 4;
const REC_PURGE: u8 = 5;

/// Approximate waste credited when a prior vote/committed record is superseded.
const META_WASTE_APPROX: u64 = 32;

/// Compact when wasted payload exceeds this absolute floor…
const COMPACT_WASTE_MIN: u64 = 64 * 1024;
/// …and waste is at least this multiple of live payload.
const COMPACT_WASTE_RATIO: u64 = 3;

#[derive(Debug, Default, Clone)]
pub struct WalState {
    pub last_purged_log_id: Option<LogId<NodeId>>,
    pub log: BTreeMap<u64, Entry<TypeConfig>>,
    pub committed: Option<LogId<NodeId>>,
    pub vote: Option<Vote<NodeId>>,
}

pub struct Wal {
    path: PathBuf,
    file: File,
    /// End offset of valid records (may be < file len after crash tear).
    len: u64,
    live_bytes: u64,
    waste_bytes: u64,
}

impl Wal {
    pub fn create_or_open(path: &Path) -> anyhow::Result<(Self, WalState)> {
        if path.exists() {
            Self::open_existing(path)
        } else {
            Ok((Self::create_new(path)?, WalState::default()))
        }
    }

    fn create_new(path: &Path) -> anyhow::Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.write_all(MAGIC)?;
        file.write_all(&VERSION.to_le_bytes())?;
        file.sync_all()?;
        Ok(Self {
            path: path.to_path_buf(),
            file,
            len: FILE_HEADER_LEN,
            live_bytes: 0,
            waste_bytes: 0,
        })
    }

    fn open_existing(path: &Path) -> anyhow::Result<(Self, WalState)> {
        let mut file = OpenOptions::new().read(true).write(true).open(path)?;
        let mut magic = [0u8; 8];
        file.read_exact(&mut magic)?;
        if &magic != MAGIC {
            anyhow::bail!("bad raft wal magic at {}", path.display());
        }
        let mut ver = [0u8; 4];
        file.read_exact(&mut ver)?;
        if u32::from_le_bytes(ver) != VERSION {
            anyhow::bail!("unsupported raft wal version at {}", path.display());
        }

        let mut state = WalState::default();
        let mut live = 0u64;
        let mut waste = 0u64;
        let mut pos = FILE_HEADER_LEN;
        let file_len = file.metadata()?.len();

        loop {
            if pos + HEADER_LEN > file_len {
                break; // torn header — stop
            }
            file.seek(SeekFrom::Start(pos))?;
            let mut hdr = [0u8; 5];
            if file.read_exact(&mut hdr).is_err() {
                break;
            }
            let rec_type = hdr[0];
            let payload_len = u32::from_le_bytes(hdr[1..5].try_into().unwrap()) as u64;
            let record_end = pos + HEADER_LEN + payload_len + CRC_LEN;
            if record_end > file_len {
                break; // torn record
            }
            let mut payload = vec![0u8; payload_len as usize];
            file.read_exact(&mut payload)?;
            let mut crc_buf = [0u8; 4];
            file.read_exact(&mut crc_buf)?;
            if crc32(&hdr, &payload) != u32::from_le_bytes(crc_buf) {
                // Corruption at end — truncate logically by stopping replay.
                tracing::warn!(
                    offset = pos,
                    "raft wal checksum mismatch; truncating trailing torn/corrupt records"
                );
                break;
            }

            apply_record(&mut state, rec_type, &payload, &mut live, &mut waste)?;
            pos = record_end;
        }

        // Drop torn tail so the next append overwrites it.
        file.set_len(pos)?;
        file.seek(SeekFrom::Start(pos))?;
        file.sync_all()?;

        Ok((
            Self {
                path: path.to_path_buf(),
                file,
                len: pos,
                live_bytes: live,
                waste_bytes: waste,
            },
            state,
        ))
    }

    /// Rewrite a brand-new WAL from an in-memory state (migration / compact).
    pub fn rewrite(path: &Path, state: &WalState) -> anyhow::Result<Self> {
        let tmp = path.with_extension("wal.tmp");
        {
            let mut wal = Self::create_new(&tmp)?;
            if let Some(v) = &state.vote {
                wal.append_vote(v)?;
            }
            if let Some(c) = &state.committed {
                wal.append_committed(Some(c))?;
            }
            if let Some(p) = &state.last_purged_log_id {
                wal.append_purge(p)?;
            }
            for ent in state.log.values() {
                wal.append_entry(ent)?;
            }
            wal.sync()?;
        }
        std::fs::rename(&tmp, path)?;
        let (wal, _) = Self::open_existing(path)?;
        Ok(wal)
    }

    pub fn append_entry(&mut self, entry: &Entry<TypeConfig>) -> Result<(), StorageError<NodeId>> {
        let payload =
            serde_json::to_vec(entry).map_err(|e| StorageIOError::write_logs(&e))?;
        self.write_record(REC_ENTRY, &payload)?;
        self.live_bytes += payload.len() as u64;
        Ok(())
    }

    pub fn append_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let payload = serde_json::to_vec(vote).map_err(|e| StorageIOError::write_logs(&e))?;
        self.waste_bytes = self.waste_bytes.saturating_add(META_WASTE_APPROX);
        self.write_record(REC_VOTE, &payload)
    }

    pub fn append_committed(
        &mut self,
        committed: Option<&LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let payload =
            serde_json::to_vec(&committed).map_err(|e| StorageIOError::write_logs(&e))?;
        self.waste_bytes = self.waste_bytes.saturating_add(META_WASTE_APPROX);
        self.write_record(REC_COMMITTED, &payload)
    }

    pub fn append_truncate(&mut self, from_index: u64) -> Result<(), StorageError<NodeId>> {
        self.write_record(REC_TRUNCATE, &from_index.to_le_bytes())
    }

    pub fn append_purge(&mut self, log_id: &LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let payload = serde_json::to_vec(log_id).map_err(|e| StorageIOError::write_logs(&e))?;
        self.write_record(REC_PURGE, &payload)
    }

    pub fn note_removed_waste(&mut self, removed_payload_approx: u64) {
        self.live_bytes = self.live_bytes.saturating_sub(removed_payload_approx);
        self.waste_bytes = self.waste_bytes.saturating_add(removed_payload_approx);
    }

    pub fn needs_compact(&self) -> bool {
        self.waste_bytes >= COMPACT_WASTE_MIN
            && self.waste_bytes >= self.live_bytes.saturating_mul(COMPACT_WASTE_RATIO)
    }

    pub fn compact(&mut self, state: &WalState) -> Result<(), StorageError<NodeId>> {
        let path = self.path.clone();
        *self = Wal::rewrite(&path, state).map_err(|e| {
            StorageIOError::write_logs(&std::io::Error::other(e.to_string()))
        })?;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<(), StorageError<NodeId>> {
        self.file.sync_data().map_err(io_err)
    }

    fn write_record(&mut self, rec_type: u8, payload: &[u8]) -> Result<(), StorageError<NodeId>> {
        if payload.len() > u32::MAX as usize {
            return Err(StorageIOError::write_logs(&std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "raft wal record too large",
            ))
            .into());
        }
        let mut hdr = [0u8; 5];
        hdr[0] = rec_type;
        hdr[1..5].copy_from_slice(&(payload.len() as u32).to_le_bytes());
        let crc = crc32(&hdr, payload);
        self.file.seek(SeekFrom::Start(self.len)).map_err(io_err)?;
        self.file.write_all(&hdr).map_err(io_err)?;
        self.file.write_all(payload).map_err(io_err)?;
        self.file.write_all(&crc.to_le_bytes()).map_err(io_err)?;
        self.len += HEADER_LEN + payload.len() as u64 + CRC_LEN;
        Ok(())
    }
}

fn io_err(e: std::io::Error) -> StorageError<NodeId> {
    StorageIOError::write_logs(&e).into()
}

fn crc32(hdr: &[u8], payload: &[u8]) -> u32 {
    let mut c = crc32fast::Hasher::new();
    c.update(hdr);
    c.update(payload);
    c.finalize()
}

fn apply_record(
    state: &mut WalState,
    rec_type: u8,
    payload: &[u8],
    live: &mut u64,
    waste: &mut u64,
) -> anyhow::Result<()> {
    match rec_type {
        REC_ENTRY => {
            let ent: Entry<TypeConfig> = serde_json::from_slice(payload)?;
            let idx = ent.get_log_id().index;
            if let Some(old) = state.log.insert(idx, ent) {
                *waste += approx_entry_bytes(&old);
            }
            *live += payload.len() as u64;
        }
        REC_VOTE => {
            state.vote = Some(serde_json::from_slice(payload)?);
            *waste += META_WASTE_APPROX;
        }
        REC_COMMITTED => {
            state.committed = serde_json::from_slice(payload)?;
            *waste += META_WASTE_APPROX;
        }
        REC_TRUNCATE => {
            if payload.len() != 8 {
                anyhow::bail!("truncate record bad len");
            }
            let from = u64::from_le_bytes(payload.try_into().unwrap());
            remove_log_range(state, from.., live, waste);
        }
        REC_PURGE => {
            let log_id: LogId<NodeId> = serde_json::from_slice(payload)?;
            state.last_purged_log_id = Some(log_id.clone());
            remove_log_range(state, ..=log_id.index, live, waste);
        }
        other => anyhow::bail!("unknown raft wal record type {other}"),
    }
    Ok(())
}

fn remove_log_range(
    state: &mut WalState,
    range: impl RangeBounds<u64>,
    live: &mut u64,
    waste: &mut u64,
) {
    let keys: Vec<u64> = state.log.range(range).map(|(k, _)| *k).collect();
    for k in keys {
        if let Some(e) = state.log.remove(&k) {
            let n = approx_entry_bytes(&e);
            *live = live.saturating_sub(n);
            *waste += n;
        }
    }
}

pub(crate) fn approx_entry_bytes(entry: &Entry<TypeConfig>) -> u64 {
    serde_json::to_vec(entry)
        .map(|v| v.len() as u64)
        .unwrap_or(64)
}

/// Legacy JSON snapshot used before the append-only WAL.
#[derive(Serialize, Deserialize)]
pub struct LegacyPersistedLog {
    pub vote: Option<Vote<NodeId>>,
    pub last_purged_log_id: Option<LogId<NodeId>>,
    pub committed: Option<LogId<NodeId>>,
    pub entries: Vec<Entry<TypeConfig>>,
}

pub fn migrate_legacy_json(dir: &Path) -> anyhow::Result<Option<WalState>> {
    let json_path = dir.join("openraft-log.json");
    if !json_path.exists() {
        return Ok(None);
    }
    let bytes = std::fs::read(&json_path)?;
    let p: LegacyPersistedLog = serde_json::from_slice(&bytes)?;
    let mut log = BTreeMap::new();
    for e in p.entries {
        log.insert(e.get_log_id().index, e);
    }
    let state = WalState {
        last_purged_log_id: p.last_purged_log_id,
        log,
        committed: p.committed,
        vote: p.vote,
    };
    let wal_path = dir.join("openraft-log.wal");
    let _wal = Wal::rewrite(&wal_path, &state)?;
    let bak = dir.join("openraft-log.json.bak");
    let _ = std::fs::rename(&json_path, &bak);
    tracing::info!(
        from = %json_path.display(),
        to = %wal_path.display(),
        "migrated raft log JSON snapshot to append-only WAL"
    );
    Ok(Some(state))
}

#[cfg(test)]
mod tests {
    use super::*;
    use openraft::EntryPayload;
    use tempfile::tempdir;

    fn blank_entry(index: u64) -> Entry<TypeConfig> {
        Entry {
            log_id: openraft::LogId::new(
                openraft::CommittedLeaderId::new(1, 1),
                index,
            ),
            payload: EntryPayload::Blank,
        }
    }

    #[test]
    fn append_and_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("openraft-log.wal");
        {
            let (mut wal, _) = Wal::create_or_open(&path).unwrap();
            wal.append_entry(&blank_entry(1)).unwrap();
            wal.append_entry(&blank_entry(2)).unwrap();
            wal.sync().unwrap();
        }
        let (_, state) = Wal::create_or_open(&path).unwrap();
        assert_eq!(state.log.len(), 2);
        assert!(state.log.contains_key(&1));
        assert!(state.log.contains_key(&2));
    }

    #[test]
    fn truncate_and_purge_recover() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("openraft-log.wal");
        {
            let (mut wal, mut st) = Wal::create_or_open(&path).unwrap();
            for i in 1..=5 {
                let e = blank_entry(i);
                wal.append_entry(&e).unwrap();
                st.log.insert(i, e);
            }
            wal.append_truncate(4).unwrap();
            let keys: Vec<_> = st.log.range(4..).map(|(k, _)| *k).collect();
            for k in keys {
                st.log.remove(&k);
            }
            let purge_id = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 2);
            wal.append_purge(&purge_id).unwrap();
            st.last_purged_log_id = Some(purge_id);
            let keys: Vec<_> = st.log.range(..=2).map(|(k, _)| *k).collect();
            for k in keys {
                st.log.remove(&k);
            }
            wal.sync().unwrap();
        }
        let (_, state) = Wal::create_or_open(&path).unwrap();
        assert_eq!(state.log.keys().copied().collect::<Vec<_>>(), vec![3]);
        assert_eq!(state.last_purged_log_id.unwrap().index, 2);
    }

    #[test]
    fn migrates_legacy_json() {
        let dir = tempdir().unwrap();
        let legacy = LegacyPersistedLog {
            vote: None,
            last_purged_log_id: None,
            committed: None,
            entries: vec![blank_entry(1), blank_entry(2)],
        };
        std::fs::write(
            dir.path().join("openraft-log.json"),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();
        let state = migrate_legacy_json(dir.path()).unwrap().unwrap();
        assert_eq!(state.log.len(), 2);
        assert!(dir.path().join("openraft-log.wal").exists());
        assert!(dir.path().join("openraft-log.json.bak").exists());
        assert!(!dir.path().join("openraft-log.json").exists());
    }

    #[test]
    fn compact_rewrites_live_only() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("openraft-log.wal");
        let (mut wal, mut st) = Wal::create_or_open(&path).unwrap();
        for i in 1..=10 {
            let e = blank_entry(i);
            wal.append_entry(&e).unwrap();
            st.log.insert(i, e);
        }
        let before = wal.len;
        let purge_id = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 8);
        wal.append_purge(&purge_id).unwrap();
        st.last_purged_log_id = Some(purge_id.clone());
        let keys: Vec<_> = st.log.range(..=8).map(|(k, _)| *k).collect();
        for k in keys {
            if let Some(e) = st.log.remove(&k) {
                wal.note_removed_waste(approx_entry_bytes(&e));
            }
        }
        // Force compact by faking waste accounting.
        wal.waste_bytes = COMPACT_WASTE_MIN;
        wal.live_bytes = 1;
        assert!(wal.needs_compact());
        wal.compact(&st).unwrap();
        assert!(wal.len < before);
        let (_, state) = Wal::create_or_open(&path).unwrap();
        assert_eq!(state.log.keys().copied().collect::<Vec<_>>(), vec![9, 10]);
    }
}
