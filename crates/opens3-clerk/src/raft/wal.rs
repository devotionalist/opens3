// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 追加写 Raft 日志 WAL（灵感来自 TiKV raft-engine，单 group）。
//!
//! 热路径只追加记录；truncate / purge 追加逻辑标记。
//! 物理回收靠偶发 rewrite compaction。

use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};

use openraft::{Entry, LogId, RaftLogId, StorageError, StorageIOError, Vote};
use serde::{Deserialize, Serialize};

use super::{NodeId, TypeConfig};

const MAGIC: &[u8; 8] = b"O3RFWAL1"; // 文件魔数
const VERSION: u32 = 1;                 // WAL 格式版本
const HEADER_LEN: u64 = 5;              // 记录头：type(1) + len(4)
const CRC_LEN: u64 = 4;                 // 每条记录尾 CRC32
const FILE_HEADER_LEN: u64 = MAGIC.len() as u64 + 4; // magic + version

const REC_ENTRY: u8 = 1;     // 日志条目
const REC_VOTE: u8 = 2;      // Vote
const REC_COMMITTED: u8 = 3; // committed LogId
const REC_TRUNCATE: u8 = 4;  // 自 index 起截断
const REC_PURGE: u8 = 5;     // 清理至 LogId

/// 旧 vote/committed 被覆盖时计入的近似浪费字节。
const META_WASTE_APPROX: u64 = 32;

/// 浪费字节超过该绝对下限才考虑 compact…
const COMPACT_WASTE_MIN: u64 = 64 * 1024;
/// …且浪费至少为存活载荷的该倍数。
const COMPACT_WASTE_RATIO: u64 = 3;

/// 从 WAL 回放得到的内存态（日志 + vote + committed）。
#[derive(Debug, Default, Clone)]
pub struct WalState {
    pub last_purged_log_id: Option<LogId<NodeId>>, // 已 purge 的最大 LogId
    pub log: BTreeMap<u64, Entry<TypeConfig>>,     // index → entry
    pub committed: Option<LogId<NodeId>>,          // 已提交点
    pub vote: Option<Vote<NodeId>>,                // 持久化投票
}

/// 打开的追加写 WAL 句柄。
pub struct Wal {
    path: PathBuf,
    file: File,
    /// 有效记录结束偏移（崩溃撕裂后可能 < 文件长度）。
    len: u64,
    live_bytes: u64,  // 存活条目近似字节
    waste_bytes: u64, // 已逻辑删除 / 被覆盖的浪费字节
}

impl Wal {
    /// 存在则打开并回放；否则新建空 WAL。
    pub fn create_or_open(path: &Path) -> anyhow::Result<(Self, WalState)> {
        if path.exists() {
            Self::open_existing(path)
        } else {
            Ok((Self::create_new(path)?, WalState::default()))
        }
    }

    /// 写入 magic/version 头，返回空句柄。
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

    /// 校验头并顺序回放记录；遇撕裂/CRC 失败则截断尾部。
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
                break; // 头被撕裂 — 停止回放
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
                break; // 记录被撕裂
            }
            let mut payload = vec![0u8; payload_len as usize];
            file.read_exact(&mut payload)?;
            let mut crc_buf = [0u8; 4];
            file.read_exact(&mut crc_buf)?;
            if crc32(&hdr, &payload) != u32::from_le_bytes(crc_buf) {
                // 尾部损坏：停止回放即逻辑截断。
                tracing::warn!(
                    offset = pos,
                    "raft wal checksum mismatch; truncating trailing torn/corrupt records"
                );
                break;
            }

            apply_record(&mut state, rec_type, &payload, &mut live, &mut waste)?;
            pos = record_end;
        }

        // 丢掉撕裂尾，使下次 append 覆盖。
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

    /// 由内存态重写全新 WAL（迁移 / compact）。
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

    /// 追加一条 Raft 日志条目。
    pub fn append_entry(&mut self, entry: &Entry<TypeConfig>) -> Result<(), StorageError<NodeId>> {
        let payload =
            serde_json::to_vec(entry).map_err(|e| StorageIOError::write_logs(&e))?;
        self.write_record(REC_ENTRY, &payload)?;
        self.live_bytes += payload.len() as u64;
        Ok(())
    }

    /// 追加 Vote；旧 vote 计入浪费。
    pub fn append_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let payload = serde_json::to_vec(vote).map_err(|e| StorageIOError::write_logs(&e))?;
        self.waste_bytes = self.waste_bytes.saturating_add(META_WASTE_APPROX);
        self.write_record(REC_VOTE, &payload)
    }

    /// 追加 committed LogId。
    pub fn append_committed(
        &mut self,
        committed: Option<&LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let payload =
            serde_json::to_vec(&committed).map_err(|e| StorageIOError::write_logs(&e))?;
        self.waste_bytes = self.waste_bytes.saturating_add(META_WASTE_APPROX);
        self.write_record(REC_COMMITTED, &payload)
    }

    /// 追加 truncate 标记（自 `from_index` 起丢弃）。
    pub fn append_truncate(&mut self, from_index: u64) -> Result<(), StorageError<NodeId>> {
        self.write_record(REC_TRUNCATE, &from_index.to_le_bytes())
    }

    /// 追加 purge 标记（清理至 `log_id`）。
    pub fn append_purge(&mut self, log_id: &LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let payload = serde_json::to_vec(log_id).map_err(|e| StorageIOError::write_logs(&e))?;
        self.write_record(REC_PURGE, &payload)
    }

    /// 内存删除条目后同步浪费计数（truncate/purge 路径）。
    pub fn note_removed_waste(&mut self, removed_payload_approx: u64) {
        self.live_bytes = self.live_bytes.saturating_sub(removed_payload_approx);
        self.waste_bytes = self.waste_bytes.saturating_add(removed_payload_approx);
    }

    /// 是否达到 compact 阈值（浪费绝对量 + 相对存活比）。
    pub fn needs_compact(&self) -> bool {
        self.waste_bytes >= COMPACT_WASTE_MIN
            && self.waste_bytes >= self.live_bytes.saturating_mul(COMPACT_WASTE_RATIO)
    }

    /// 用当前内存态 rewrite WAL，回收浪费空间。
    pub fn compact(&mut self, state: &WalState) -> Result<(), StorageError<NodeId>> {
        let path = self.path.clone();
        *self = Wal::rewrite(&path, state).map_err(|e| {
            StorageIOError::write_logs(&std::io::Error::other(e.to_string()))
        })?;
        Ok(())
    }

    /// fsync 数据（不强制元数据）。
    pub fn sync(&mut self) -> Result<(), StorageError<NodeId>> {
        self.file.sync_data().map_err(io_err)
    }

    /// 写入一条 `[type|len|payload|crc]` 记录并推进 `len`。
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

/// 将 IO 错误包装为 openraft StorageError。
fn io_err(e: std::io::Error) -> StorageError<NodeId> {
    StorageIOError::write_logs(&e).into()
}

/// 记录头 + payload 的 CRC32。
fn crc32(hdr: &[u8], payload: &[u8]) -> u32 {
    let mut c = crc32fast::Hasher::new();
    c.update(hdr);
    c.update(payload);
    c.finalize()
}

/// 将单条 WAL 记录应用到内存态，并更新 live/waste 计数。
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

/// 从内存日志删除区间内条目，并把字节从 live 挪到 waste。
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

/// 条目序列化后的近似字节数（compact 计量用）。
pub(crate) fn approx_entry_bytes(entry: &Entry<TypeConfig>) -> u64 {
    serde_json::to_vec(entry)
        .map(|v| v.len() as u64)
        .unwrap_or(64)
}

/// 追加写 WAL 之前的旧版 JSON 快照格式。
#[derive(Serialize, Deserialize)]
pub struct LegacyPersistedLog {
    pub vote: Option<Vote<NodeId>>,
    pub last_purged_log_id: Option<LogId<NodeId>>,
    pub committed: Option<LogId<NodeId>>,
    pub entries: Vec<Entry<TypeConfig>>,
}

/// 若存在 `openraft-log.json` 则迁到 WAL，并改名为 `.bak`。
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

    /// 构造 Blank 测试条目。
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
        // 伪造浪费计数以强制 compact。
        wal.waste_bytes = COMPACT_WASTE_MIN;
        wal.live_bytes = 1;
        assert!(wal.needs_compact());
        wal.compact(&st).unwrap();
        assert!(wal.len < before);
        let (_, state) = Wal::create_or_open(&path).unwrap();
        assert_eq!(state.log.keys().copied().collect::<Vec<_>>(), vec![9, 10]);
    }
}
