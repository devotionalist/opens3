// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Raft 日志 + 状态机存储。
//!
//! 日志持久化为追加写 WAL（`openraft-log.wal`）；状态机仍用原子 JSON/bin 快照。

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine, Snapshot};
use openraft::{
    Entry, EntryPayload, LogId, LogState, RaftLogId, RaftLogReader, RaftSnapshotBuilder,
    RaftTypeConfig, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use super::wal::{approx_entry_bytes, migrate_legacy_json, Wal, WalState};
use super::{NodeId, RaftRequest, RaftResponse, TypeConfig};
use crate::cluster::{ClusterSnapshot, ClusterState};

// ── 日志存储 ────────────────────────────────────────────────────────────────

/// openraft 日志存储（内存 + 可选 WAL）。
#[derive(Clone)]
pub struct LogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<LogStoreInner<C>>>,
}

/// 日志存储内部态。
struct LogStoreInner<C: RaftTypeConfig> {
    last_purged_log_id: Option<LogId<C::NodeId>>, // 已 purge 边界
    log: BTreeMap<u64, C::Entry>,                 // index → entry
    committed: Option<LogId<C::NodeId>>,          // 已提交点
    vote: Option<Vote<C::NodeId>>,                // 持久化投票
    wal: Option<Wal>,                             // 磁盘 WAL；测试可为空
}

impl Default for LogStoreInner<TypeConfig> {
    /// 空内存态（无 WAL）。
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
            wal: None,
        }
    }
}

impl LogStoreInner<TypeConfig> {
    /// 由 WAL 回放态构造内部结构。
    fn from_wal_state(state: WalState, wal: Option<Wal>) -> Self {
        Self {
            last_purged_log_id: state.last_purged_log_id,
            log: state.log,
            committed: state.committed,
            vote: state.vote,
            wal,
        }
    }

    /// 导出当前内存态供 WAL compact。
    fn snapshot_state(&self) -> WalState {
        WalState {
            last_purged_log_id: self.last_purged_log_id.clone(),
            log: self.log.clone(),
            committed: self.committed.clone(),
            vote: self.vote.clone(),
        }
    }

    /// 删除区间内日志，返回移除的近似字节数。
    fn remove_log_range(&mut self, range: impl RangeBounds<u64>) -> u64 {
        let keys: Vec<_> = self.log.range(range).map(|(k, _)| *k).collect();
        let mut removed = 0u64;
        for key in keys {
            if let Some(ent) = self.log.remove(&key) {
                removed += approx_entry_bytes(&ent);
            }
        }
        removed
    }

    /// 若 WAL 浪费达标则 rewrite compact。
    fn maybe_compact(&mut self) -> Result<(), StorageError<NodeId>> {
        if !self.wal.as_ref().is_some_and(|w| w.needs_compact()) {
            return Ok(());
        }
        let state = self.snapshot_state();
        if let Some(wal) = self.wal.as_mut() {
            wal.compact(&state)?;
        }
        Ok(())
    }
}

impl LogStore<TypeConfig> {
    /// 打开目录下 WAL；若无则先尝试迁移旧 JSON。
    pub fn open(dir: &Path) -> anyhow::Result<Self> {
        let wal_path = dir.join("openraft-log.wal");
        if !wal_path.exists() {
            let _ = migrate_legacy_json(dir)?;
        }
        let (wal, state) = Wal::create_or_open(&wal_path)?;
        Ok(Self {
            inner: Arc::new(Mutex::new(LogStoreInner::from_wal_state(state, Some(wal)))),
        })
    }
}

impl Default for LogStore<TypeConfig> {
    /// 仅内存 LogStore（测试用）。
    fn default() -> Self {
        Self {
            inner: Arc::new(Mutex::new(LogStoreInner::default())),
        }
    }
}

impl RaftLogReader<TypeConfig> for LogStore<TypeConfig> {
    /// 按 index 区间读取日志条目。
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        Ok(inner
            .log
            .range(range)
            .map(|(_, v)| v.clone())
            .collect())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore<TypeConfig> {
    type LogReader = Self;

    /// 返回 last_purged / last_log。
    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().await;
        let last_purged = inner.last_purged_log_id.clone();
        let last = inner
            .log
            .iter()
            .next_back()
            .map(|(_, ent)| ent.get_log_id().clone())
            .or_else(|| last_purged.clone());
        Ok(LogState {
            last_purged_log_id: last_purged,
            last_log_id: last,
        })
    }

    /// 持久化 committed 点到内存与 WAL。
    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.committed = committed.clone();
        if let Some(wal) = inner.wal.as_mut() {
            wal.append_committed(committed.as_ref())?;
            wal.sync()?;
        }
        Ok(())
    }

    /// 读取已保存的 committed。
    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().await.committed.clone())
    }

    /// 持久化 Vote。
    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.vote = Some(vote.clone());
        if let Some(wal) = inner.wal.as_mut() {
            wal.append_vote(vote)?;
            wal.sync()?;
        }
        Ok(())
    }

    /// 读取 Vote。
    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().await.vote.clone())
    }

    /// 追加日志条目并 fsync；完成后回调 LogFlushed。
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>>,
    {
        let mut inner = self.inner.lock().await;
        for entry in entries {
            if let Some(wal) = inner.wal.as_mut() {
                wal.append_entry(&entry)?;
            }
            inner.log.insert(entry.get_log_id().index, entry);
        }
        if let Some(wal) = inner.wal.as_mut() {
            wal.sync()?;
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    /// 自 `log_id.index` 起截断冲突日志。
    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().await;
        let removed = inner.remove_log_range(log_id.index..);
        if let Some(wal) = inner.wal.as_mut() {
            wal.append_truncate(log_id.index)?;
            wal.note_removed_waste(removed);
            wal.sync()?;
        }
        inner.maybe_compact()?;
        Ok(())
    }

    /// 清理已快照覆盖的旧日志（含 `log_id`）。
    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // purge 边界必须单调不减
        let mut inner = self.inner.lock().await;
        assert!(inner.last_purged_log_id.as_ref() <= Some(&log_id));
        inner.last_purged_log_id = Some(log_id.clone());
        let removed = inner.remove_log_range(..=log_id.index);
        if let Some(wal) = inner.wal.as_mut() {
            wal.append_purge(&log_id)?;
            wal.note_removed_waste(removed);
            wal.sync()?;
        }
        inner.maybe_compact()?;
        Ok(())
    }

    /// 返回可并发读的日志 reader（克隆句柄）。
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
}

// ── 状态机 ──────────────────────────────────────────────────────────────────

/// 内存中的 Raft 快照副本。
#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<NodeId, openraft::BasicNode>, // 快照元数据
    pub data: Vec<u8>,                                   // 序列化状态机字节
}

/// 持久化状态机内容。
#[derive(Serialize, Deserialize, Debug, Default, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<NodeId>>, // 最后应用日志
    pub last_membership: StoredMembership<NodeId, openraft::BasicNode>, // 成员配置
    /// 最近一次应用的集群快照载荷（用于重建 Raft snapshot）。
    pub cluster_json: Option<serde_json::Value>,
}

/// 集群拓扑状态机：apply Raft 日志并落盘。
pub struct StateMachineStore {
    pub state_machine: RwLock<StateMachineData>,
    snapshot_idx: AtomicU64, // 本地快照序号
    current_snapshot: RwLock<Option<StoredSnapshot>>,
    /// 与 ClerkServer HTTP 共享的实时拓扑。
    cluster: ClusterState,
    sm_path: Option<PathBuf>,   // openraft-sm.json
    snap_path: Option<PathBuf>, // openraft-snapshot.bin
}

impl StateMachineStore {
    /// 仅内存态（无落盘路径），供测试。
    pub fn new(cluster: ClusterState) -> Self {
        Self {
            state_machine: Default::default(),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
            cluster,
            sm_path: None,
            snap_path: None,
        }
    }

    /// 从目录加载状态机 JSON 与快照 bin。
    pub fn open(dir: &Path, cluster: ClusterState) -> anyhow::Result<Self> {
        let sm_path = dir.join("openraft-sm.json");
        let snap_path = dir.join("openraft-snapshot.bin");
        let mut store = Self {
            state_machine: Default::default(),
            snapshot_idx: AtomicU64::new(0),
            current_snapshot: RwLock::new(None),
            cluster,
            sm_path: Some(sm_path.clone()),
            snap_path: Some(snap_path.clone()),
        };
        if sm_path.exists() {
            let bytes = std::fs::read(&sm_path)?;
            let data: StateMachineData = serde_json::from_slice(&bytes)?;
            if let Some(ref payload) = data.cluster_json {
                store.apply_payload(payload);
            }
            store.state_machine = RwLock::new(data);
        }
        if snap_path.exists() {
            let bytes = std::fs::read(&snap_path)?;
            if let Ok(stored) = serde_json::from_slice::<PersistedSnapshot>(&bytes) {
                store.current_snapshot = RwLock::new(Some(StoredSnapshot {
                    meta: stored.meta,
                    data: stored.data,
                }));
            }
        }
        Ok(store)
    }

    /// 原子写状态机 JSON（tmp + rename）。
    fn persist_sm(data: &StateMachineData, path: &Path) -> Result<(), StorageError<NodeId>> {
        let bytes =
            serde_json::to_vec(data).map_err(|e| StorageIOError::write_state_machine(&e))?;
        let tmp = path.with_extension("json.tmp");
        std::fs::write(&tmp, bytes).map_err(|e| StorageIOError::write_state_machine(&e))?;
        std::fs::rename(&tmp, path).map_err(|e| StorageIOError::write_state_machine(&e))?;
        Ok(())
    }

    /// 原子写快照 bin（tmp + rename）。
    fn persist_snapshot(
        snap: &StoredSnapshot,
        path: &Path,
    ) -> Result<(), StorageError<NodeId>> {
        let p = PersistedSnapshot {
            meta: snap.meta.clone(),
            data: snap.data.clone(),
        };
        let bytes = serde_json::to_vec(&p).map_err(|e| StorageIOError::write_snapshot(None, &e))?;
        let tmp = path.with_extension("bin.tmp");
        std::fs::write(&tmp, bytes).map_err(|e| StorageIOError::write_snapshot(None, &e))?;
        std::fs::rename(&tmp, path).map_err(|e| StorageIOError::write_snapshot(None, &e))?;
        Ok(())
    }

    /// 将 ClusterMutation 载荷导入内存 ClusterState（仅 `op=snapshot`）。
    fn apply_payload(&self, payload: &serde_json::Value) {
        if payload.get("op").and_then(|v| v.as_str()) != Some("snapshot") {
            return;
        }
        if let Some(s) = payload.get("snapshot") {
            if let Ok(snap) = serde_json::from_value::<ClusterSnapshot>(s.clone()) {
                self.cluster.import_snapshot(snap);
            }
        }
    }
}

/// 磁盘上的快照封套（meta + data）。
#[derive(Serialize, Deserialize)]
struct PersistedSnapshot {
    meta: SnapshotMeta<NodeId, openraft::BasicNode>,
    data: Vec<u8>,
}

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    /// 序列化当前状态机构建 Raft 快照并落盘。
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<NodeId>> {
        let state_machine = self.state_machine.read().await;
        let data =
            serde_json::to_vec(&*state_machine).map_err(|e| StorageIOError::read_state_machine(&e))?;

        let last_applied_log = state_machine.last_applied_log;
        let last_membership = state_machine.last_membership.clone();

        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };
        if let Some(path) = &self.snap_path {
            StateMachineStore::persist_snapshot(&stored, path)?;
        }
        *current_snapshot = Some(stored);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    type SnapshotBuilder = Self;

    /// 返回 last_applied 与 membership。
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, openraft::BasicNode>), StorageError<NodeId>>
    {
        let sm = self.state_machine.read().await;
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    /// 顺序 apply 日志：Blank / ClusterMutation / Membership。
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<RaftResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
    {
        let mut res = Vec::new();
        let mut sm = self.state_machine.write().await;

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);

            match entry.payload {
                EntryPayload::Blank => res.push(RaftResponse { ok: true }),
                EntryPayload::Normal(ref req) => match req {
                    RaftRequest::ClusterMutation { payload } => {
                        self.apply_payload(payload);
                        sm.cluster_json = Some(payload.clone());
                        res.push(RaftResponse { ok: true });
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    sm.last_membership =
                        StoredMembership::new(Some(entry.log_id), mem.clone());
                    res.push(RaftResponse { ok: true });
                }
            }
        }
        if let Some(path) = &self.sm_path {
            StateMachineStore::persist_sm(&sm, path)?;
        }
        Ok(res)
    }

    /// 开始接收快照：返回可写缓冲区。
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    /// 安装完整快照：导入拓扑并持久化。
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let updated: StateMachineData = serde_json::from_slice(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        if let Some(ref payload) = updated.cluster_json {
            self.apply_payload(payload);
        }

        let mut state_machine = self.state_machine.write().await;
        *state_machine = updated;

        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        let stored = StoredSnapshot {
            meta: meta.clone(),
            data,
        };
        if let Some(path) = &self.snap_path {
            StateMachineStore::persist_snapshot(&stored, path)?;
        }
        if let Some(path) = &self.sm_path {
            let sm = self.state_machine.read().await;
            StateMachineStore::persist_sm(&sm, path)?;
        }
        *current_snapshot = Some(stored);
        Ok(())
    }

    /// 返回当前已构建的快照（若有）。
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<NodeId>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => Ok(Some(Snapshot {
                meta: snapshot.meta.clone(),
                snapshot: Box::new(Cursor::new(snapshot.data.clone())),
            })),
            None => Ok(None),
        }
    }

    /// 快照构建器即自身 Arc。
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }
}
