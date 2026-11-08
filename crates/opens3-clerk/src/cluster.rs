// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use opens3_common::fileid::FileId;
use opens3_common::replication::Replication;
use opens3_common::topology::TopologyLocation;
use opens3_common::types::{VolumeMeta, VolumeState};
use opens3_common::util;
use opens3_common::{Error, Result};
use parking_lot::RwLock;
use rand::Rng;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// How `assign` picks among writable volumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AssignPolicy {
    /// Uniform random among candidates (default).
    #[default]
    Random,
    /// Cycle through candidates in volume-id order.
    RoundRobin,
    /// Weighted random by primary node's `free_disk_bytes`.
    FreeDisk,
    /// Prefer the volume with the smallest current `size`.
    LeastSize,
}

impl AssignPolicy {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Random => "random",
            Self::RoundRobin => "round_robin",
            Self::FreeDisk => "free_disk",
            Self::LeastSize => "least_size",
        }
    }

    pub fn label_zh(self) -> &'static str {
        match self {
            Self::Random => "随机",
            Self::RoundRobin => "循环",
            Self::FreeDisk => "按磁盘空闲加权",
            Self::LeastSize => "优先较空卷",
        }
    }
}

impl FromStr for AssignPolicy {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "random" | "rand" => Ok(Self::Random),
            "round_robin" | "rr" | "roundrobin" | "cycle" => Ok(Self::RoundRobin),
            "free_disk" | "disk" | "weighted_disk" | "weight" => Ok(Self::FreeDisk),
            "least_size" | "least" | "smallest" => Ok(Self::LeastSize),
            other => Err(Error::InvalidArgument(format!(
                "unknown assign policy '{other}' (random|round_robin|free_disk|least_size)"
            ))),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeServerInfo { // 服务器信息
    pub node_id: String, // 节点ID
    pub location: TopologyLocation, // 位置
    pub max_volume_count: u64, // 最大卷的数量
    pub free_disk_bytes: u64, // 空闲的磁盘空间
    pub last_seen: u64, // 最后一次看到的时间
    /// Ops switch: new data nodes start offline and do not receive assigns until online.
    #[serde(default)]
    pub online: bool, // 是否在线
    /// When set, this data node was added as a replica of `replica_of` (source node_id / pod).
    #[serde(default)]
    pub replica_of: Option<String>,
    /// Process boot id from Volume heartbeat; change ⇒ restart ⇒ must re-reconcile.
    #[serde(default)]
    pub boot_id: String,
    /// After restart, false until fingerprint align with peer replicas finishes.
    /// Defaults true for old Raft snapshots so existing clusters keep serving.
    #[serde(default = "default_true")]
    pub write_ready: bool,
}

fn default_true() -> bool {
    true
}

/// Lightweight volume content fingerprint (`size:file_count:deleted_count`).
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeFingerprint {
    pub size: u64,
    pub file_count: u64,
    pub deleted_count: u64,
}

impl VolumeFingerprint {
    pub fn new(size: u64, file_count: u64, deleted_count: u64) -> Self {
        Self {
            size,
            file_count,
            deleted_count,
        }
    }

    pub fn key(&self) -> String {
        format!("{}:{}:{}", self.size, self.file_count, self.deleted_count)
    }
}

/// Result of applying a volume-server heartbeat.
#[derive(Debug, Clone)]
pub struct HeartbeatOutcome {
    pub create: Vec<u32>,
    /// True when `boot_id` changed (or first seen) — caller should run fingerprint reconcile.
    pub needs_reconcile: bool,
    pub write_ready: bool,
}

struct Inner {
    servers: HashMap<String, VolumeServerInfo>,
    volumes: HashMap<u32, VolumeMeta>,
    next_volume_id: u32,
    next_file_key: u64,
    volume_seq_by_server: HashMap<String, Vec<u32>>,
    assign_policy: AssignPolicy,
    /// Cursor for [`AssignPolicy::RoundRobin`].
    assign_rr_cursor: u64,
    /// Max bytes/sec for heal/balance volume push (`0` = unlimited).
    heal_max_bps: u64,
}

#[derive(Clone)]
pub struct ClusterState {  // 集群状态  
    inner: Arc<RwLock<Inner>>,  // 集群状态的内部实现  
    pub default_replication: Replication, // 默认的复制策略  
}

impl ClusterState { // 集群状态的操作
    pub fn new(default_replication: Replication) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                servers: HashMap::new(),
                volumes: HashMap::new(),
                next_volume_id: 1,
                next_file_key: 1,
                assign_policy: AssignPolicy::default(),
                assign_rr_cursor: 0,
                heal_max_bps: 0,
                volume_seq_by_server: HashMap::new(),
            })),
            default_replication,
        }
    }

    pub fn heartbeat(
        &self,
        mut info: VolumeServerInfo,
        reported: Vec<VolumeReport>,
    ) -> HeartbeatOutcome {
        self.heartbeat_with_bootstrap(info, reported, false, None)
    }

    /// Heartbeat with deploy bootstrap intents (`desired_online` / `desired_replica_of`).
    ///
    /// - First registration: apply bootstrap (replica waits offline until source exists).
    /// - Later: preserve ops online/replica_of; while still offline with no pairing,
    ///   keep trying `desired_replica_of`, then promote online if desired.
    pub fn heartbeat_with_bootstrap(
        &self,
        mut info: VolumeServerInfo,
        reported: Vec<VolumeReport>,
        desired_online: bool,
        desired_replica_of: Option<String>,
    ) -> HeartbeatOutcome {
        let mut g = self.inner.write();
        let node_id = info.node_id.clone();
        let prev = g.servers.get(&node_id).cloned();
        let prev_boot = prev.as_ref().map(|s| s.boot_id.as_str()).unwrap_or("");
        let restart = prev_boot.is_empty() || prev_boot != info.boot_id;
        let is_new = prev.is_none();

        if let Some(ref p) = prev {
            info.online = p.online;
            info.replica_of = p.replica_of.clone();
        } else {
            info.online = false;
            info.replica_of = None;
        }
        if restart {
            // Restart / first registration: block writes until fingerprint align.
            info.write_ready = false;
        } else {
            info.write_ready = prev.map(|s| s.write_ready).unwrap_or(false);
        }

        // Resolve bootstrap replica / online before insert so volumes follow node state.
        let desired_replica = desired_replica_of
            .as_ref()
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());
        if is_new {
            apply_bootstrap_locked(
                &mut g,
                &node_id,
                &mut info,
                desired_online,
                desired_replica.as_deref(),
            );
        } else if !info.online && info.replica_of.is_none() {
            // Waiting for source (or Web-offline node with Helm bootstrap still set).
            apply_bootstrap_locked(
                &mut g,
                &node_id,
                &mut info,
                desired_online,
                desired_replica.as_deref(),
            );
        }

        g.servers.insert(node_id.clone(), info);

        let srv_loc = g.servers.get(&node_id).map(|s| s.location.clone());
        let reported_ids: std::collections::HashSet<u32> =
            reported.iter().map(|r| r.id).collect();
        for r in reported {
            let entry = g.volumes.entry(r.id).or_insert_with(|| {
                VolumeMeta::new(r.id, &r.collection, self.default_replication)
            });
            entry.size = r.size;
            entry.file_count = r.file_count;
            entry.deleted_count = r.deleted_count;
            // Never auto-promote Offline → Writable from heartbeat; node online does.
            if r.sealed {
                entry.state = VolumeState::Sealed;
            } else if r.read_only && !matches!(entry.state, VolumeState::Offline) {
                entry.state = VolumeState::ReadOnly;
            }
            if let Some(loc) = &srv_loc {
                if let Some(existing) = entry.locations.iter_mut().find(|l| l.node_id == node_id) {
                    *existing = loc.clone();
                } else {
                    entry.locations.push(loc.clone());
                }
            }
            g.next_volume_id = g.next_volume_id.max(r.id + 1);
        }

        // Always refresh URLs for every volume hosted on this node — even when the
        // disk report is empty (Pod restart / emptyDir wipe). Otherwise assign keeps
        // handing out stale Pod IPs and S3 fails with grpc connect transport errors.
        if let Some(loc) = &srv_loc {
            for v in g.volumes.values_mut() {
                if let Some(existing) = v.locations.iter_mut().find(|l| l.node_id == node_id) {
                    *existing = loc.clone();
                }
            }
        }

        // auto-create volumes if under capacity
        let mut to_create = Vec::new();
        let srv = g.servers.get(&node_id).cloned();
        if let Some(srv) = srv {
            let owned = g
                .volumes
                .values()
                .filter(|v| v.locations.iter().any(|l| l.node_id == node_id))
                .count() as u64;
            if owned < srv.max_volume_count {
                let need = (srv.max_volume_count - owned).min(1);
                for _ in 0..need {
                    let id = g.next_volume_id;
                    g.next_volume_id += 1;
                    let mut meta = VolumeMeta::new(id, "", self.default_replication);
                    meta.locations.push(srv.location.clone());
                    // Volumes follow the node: already-online nodes get writable vols.
                    if srv.online {
                        meta.state = VolumeState::Writable;
                    }
                    g.volumes.insert(id, meta);
                    g.volume_seq_by_server
                        .entry(node_id.clone())
                        .or_default()
                        .push(id);
                    to_create.push(id);
                }
            }
            // Rematerialize topology volumes missing on disk after restart.
            for v in g.volumes.values() {
                if v.locations.iter().any(|l| l.node_id == node_id)
                    && !reported_ids.contains(&v.id)
                    && !to_create.contains(&v.id)
                {
                    to_create.push(v.id);
                }
            }
        }
        // Single-copy / empty: nothing to compare — mark write-ready immediately.
        let mut needs_reconcile = restart;
        if restart {
            let multi = g.volumes.values().any(|v| {
                v.locations.iter().any(|l| l.node_id == node_id) && v.locations.len() >= 2
            });
            if !multi {
                if let Some(s) = g.servers.get_mut(&node_id) {
                    s.write_ready = true;
                }
                needs_reconcile = false;
            }
        }
        let write_ready = g
            .servers
            .get(&node_id)
            .map(|s| s.write_ready)
            .unwrap_or(false);
        HeartbeatOutcome {
            create: to_create,
            needs_reconcile,
            write_ready,
        }
    }

    pub fn set_write_ready(&self, node_id: &str, ready: bool) -> bool {
        let mut g = self.inner.write();
        if let Some(s) = g.servers.get_mut(node_id) {
            s.write_ready = ready;
            return true;
        }
        false
    }

    pub fn is_write_ready(&self, node_id: &str) -> bool {
        self.inner
            .read()
            .servers
            .get(node_id)
            .map(|s| s.write_ready)
            .unwrap_or(false)
    }

    /// Volumes hosted on `node_id` that also have at least one other location.
    pub fn multi_copy_volumes_on(&self, node_id: &str) -> Vec<VolumeMeta> {
        self.inner
            .read()
            .volumes
            .values()
            .filter(|v| {
                v.locations.iter().any(|l| l.node_id == node_id) && v.locations.len() >= 2
            })
            .cloned()
            .collect()
    }

    pub fn assign(
        &self,
        collection: &str,
        replication: Option<Replication>,
        count: u32,
        ttl_seconds: u64,
    ) -> Result<AssignResult> {
        let repl = replication.unwrap_or(self.default_replication);
        let mut g = self.inner.write();

        // Degraded writes: volume is assignable if ≥1 location is online+write_ready.
        // Reconciling (restart) nodes are omitted from the returned location list so
        // catch-up can run in the background without stalling the cluster write path.
        let candidates: Vec<u32> = g
            .volumes
            .iter()
            .filter(|(_, v)| {
                v.is_writable()
                    && (collection.is_empty() || v.collection == collection || v.collection.is_empty())
                    && v.replication == repl
                    && v.locations.iter().any(|l| {
                        g.servers
                            .get(&l.node_id)
                            .map(|s| s.online && s.write_ready)
                            .unwrap_or(false)
                    })
            })
            .map(|(id, _)| *id)
            .collect();

        // Do not auto-create here: new volumes start offline and must be onlined by ops.
        let volume_id = Self::pick_assign_volume(&mut g, &candidates)?;

        // ensure collection set; top-up replica locations if under-replicated
        let topup = pick_locations(&g.servers, repl).unwrap_or_default();
        let meta = {
            let v = g
                .volumes
                .get_mut(&volume_id)
                .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))?;
            if v.collection.is_empty() && !collection.is_empty() {
                v.collection = collection.to_string();
            }
            v.ttl_seconds = ttl_seconds;
            if (v.locations.len() as u8) < repl.total_copies() {
                for loc in topup {
                    if !v.locations.iter().any(|l| l.node_id == loc.node_id) {
                        v.locations.push(loc);
                    }
                    if (v.locations.len() as u8) >= repl.total_copies() {
                        break;
                    }
                }
            }
            v.clone()
        };

        // Prefer live server URLs (heartbeat) over possibly stale volume.locations.
        let write_locs: Vec<_> = meta
            .locations
            .iter()
            .filter_map(|l| {
                let s = g.servers.get(&l.node_id)?;
                if s.online && s.write_ready {
                    Some(s.location.clone())
                } else {
                    None
                }
            })
            .collect();
        let primary = write_locs
            .first()
            .cloned()
            .ok_or_else(|| Error::NoWritableVolume)?;

        let file_key = g.next_file_key;
        g.next_file_key += count.max(1) as u64;
        let cookie = util::random_cookie();
        let fid = FileId::new(volume_id, file_key, cookie);

        Ok(AssignResult {
            fid,
            count: count.max(1),
            url: primary.url.clone(),
            public_url: primary.public_url.clone(),
            // Only write-ready replicas — sync fan-out skips nodes still catching up.
            locations: write_locs,
        })
    }

    pub fn lookup(&self, volume_id: u32) -> Result<VolumeMeta> {
        self.inner
            .read()
            .volumes
            .get(&volume_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))
    }

    pub fn list_volumes(&self, collection: &str) -> Vec<VolumeMeta> {
        self.inner
            .read()
            .volumes
            .values()
            .filter(|v| collection.is_empty() || v.collection == collection)
            .cloned()
            .collect()
    }

    pub fn seal(&self, volume_id: u32) -> Result<()> {
        let mut g = self.inner.write();
        let v = g
            .volumes
            .get_mut(&volume_id)
            .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))?;
        v.state = VolumeState::Sealed;
        Ok(())
    }

    /// Bring a data volume online (Offline → Writable) or take it offline for new writes.
    pub fn set_volume_online(&self, volume_id: u32, online: bool) -> Result<VolumeState> {
        let mut g = self.inner.write();
        let v = g
            .volumes
            .get_mut(&volume_id)
            .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))?;
        if online {
            match v.state {
                VolumeState::Offline | VolumeState::Writable => {
                    v.state = VolumeState::Writable;
                }
                VolumeState::ReadOnly | VolumeState::Sealed | VolumeState::EcEncoded => {
                    return Err(Error::InvalidArgument(format!(
                        "volume {volume_id} is {:?}; cannot online",
                        v.state
                    )));
                }
            }
        } else {
            match v.state {
                VolumeState::Offline | VolumeState::Writable => {
                    v.state = VolumeState::Offline;
                }
                VolumeState::ReadOnly | VolumeState::Sealed | VolumeState::EcEncoded => {
                    return Err(Error::InvalidArgument(format!(
                        "volume {volume_id} is {:?}; take offline only from writable/offline",
                        v.state
                    )));
                }
            }
        }
        Ok(v.state)
    }

    /// Record that `node_id` is a replica of `source` (or clear with None).
    /// Constraints:
    /// - a data node may be the replica of at most one other node (single source);
    /// - one source may have many replicas (fan-out: A←B and A←C);
    /// - chaining is allowed (A←B←C); cycles are rejected;
    /// - replica pairing can only change while the node is offline.
    pub fn set_replica_of(&self, node_id: &str, source: Option<&str>) -> Result<()> {
        let mut g = self.inner.write();
        if !g.servers.contains_key(node_id) {
            return Err(Error::NotFound(format!("data node {node_id}")));
        }
        let target_online = g.servers.get(node_id).map(|s| s.online).unwrap_or(false);
        let src = source
            .map(str::trim)
            .filter(|x| !x.is_empty())
            .map(|x| x.to_string());
        // Any change to replica_of while online is rejected (set or clear).
        if target_online {
            let cur = g.servers.get(node_id).and_then(|s| s.replica_of.clone());
            let next = src.clone();
            if cur != next {
                return Err(Error::InvalidArgument(format!(
                    "{node_id} is online; change replica_of only while offline"
                )));
            }
            return Ok(());
        }
        if let Some(ref src) = src {
            if src == node_id {
                return Err(Error::InvalidArgument(
                    "a data node cannot be a replica of itself".into(),
                ));
            }
            if !g.servers.contains_key(src) {
                return Err(Error::NotFound(format!("data node {src}")));
            }
            // Chaining OK; reject cycles (walk source's replica_of chain).
            let mut walk = src.clone();
            for _ in 0..=g.servers.len() {
                if walk == node_id {
                    return Err(Error::InvalidArgument(format!(
                        "replica chain cycle: {node_id} → {src} would loop"
                    )));
                }
                match g.servers.get(&walk).and_then(|s| s.replica_of.clone()) {
                    Some(next) => walk = next,
                    None => break,
                }
            }
            if let Some(cur) = g.servers.get(node_id).and_then(|s| s.replica_of.clone()) {
                if cur != *src {
                    return Err(Error::InvalidArgument(format!(
                        "{node_id} is already a replica of {cur}; clear first before switching"
                    )));
                }
                // same source — idempotent
                return Ok(());
            }
        }
        let s = g.servers.get_mut(node_id).unwrap();
        s.replica_of = src;
        Ok(())
    }

    /// Ops switch for a data node. Brings the node's data volumes online/offline with it;
    /// per-volume ops are not exposed — volumes follow the node.
    pub fn set_server_online(&self, node_id: &str, online: bool) -> Result<()> {
        let mut g = self.inner.write();
        let s = g
            .servers
            .get_mut(node_id)
            .ok_or_else(|| Error::NotFound(format!("data node {node_id}")))?;
        s.online = online;
        for v in g.volumes.values_mut() {
            if !v.locations.iter().any(|l| l.node_id == node_id) {
                continue;
            }
            if online {
                if matches!(v.state, VolumeState::Offline) {
                    v.state = VolumeState::Writable;
                }
            } else if matches!(v.state, VolumeState::Writable) {
                v.state = VolumeState::Offline;
            }
        }
        Ok(())
    }

    /// Update a volume's XYZ replication policy. Does not copy data; call heal
    /// afterwards to materialize missing replicas on other volume servers.
    pub fn set_replication(&self, volume_id: u32, repl: Replication) -> Result<VolumeMeta> {
        let mut g = self.inner.write();
        let v = g
            .volumes
            .get_mut(&volume_id)
            .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))?;
        if matches!(v.state, VolumeState::EcEncoded) {
            return Err(Error::InvalidArgument(format!(
                "volume {volume_id} is EC-encoded; cannot change replication"
            )));
        }
        v.replication = repl;
        Ok(v.clone())
    }

    /// Apply XYZ policy to many volumes. `from_code` when set only touches volumes
    /// whose current replication string matches (e.g. upgrade all `000` → `001`).
    /// Empty `volume_ids` means all volumes (optionally filtered by `from_code`).
    pub fn set_replication_batch(
        &self,
        volume_ids: &[u32],
        from_code: Option<&str>,
        repl: Replication,
    ) -> Result<Vec<u32>> {
        let mut g = self.inner.write();
        let ids: Vec<u32> = if volume_ids.is_empty() {
            g.volumes.keys().copied().collect()
        } else {
            volume_ids.to_vec()
        };
        let mut updated = Vec::new();
        for id in ids {
            let Some(v) = g.volumes.get_mut(&id) else {
                continue;
            };
            if matches!(v.state, VolumeState::EcEncoded) {
                continue;
            }
            if let Some(from) = from_code {
                if v.replication.as_str() != from {
                    continue;
                }
            }
            v.replication = repl;
            updated.push(id);
        }
        Ok(updated)
    }

    /// Create `count` empty volumes with the given XYZ placement plan (locations
    /// reserved on master). Physical files are created via volume heartbeat /
    /// replicate; this is distinct from node-local `/admin/create_volume`.
    pub fn grow_volumes(
        &self,
        count: u32,
        collection: &str,
        replication: Replication,
    ) -> Result<Vec<VolumeMeta>> {
        let n = count.max(1).min(64);
        let mut g = self.inner.write();
        let mut out = Vec::with_capacity(n as usize);
        for _ in 0..n {
            let locations = pick_locations(&g.servers, replication)?;
            let id = g.next_volume_id;
            g.next_volume_id += 1;
            let mut meta = VolumeMeta::new(id, collection, replication);
            meta.locations = locations;
            // Pick only online servers; new volumes on online nodes start writable.
            if meta.locations.iter().any(|l| {
                g.servers.get(&l.node_id).map(|s| s.online).unwrap_or(false)
            }) {
                meta.state = VolumeState::Writable;
            }
            for loc in &meta.locations {
                g.volume_seq_by_server
                    .entry(loc.node_id.clone())
                    .or_default()
                    .push(id);
            }
            g.volumes.insert(id, meta.clone());
            out.push(meta);
        }
        Ok(out)
    }

    pub fn mark_ec(&self, volume_id: u32) -> Result<()> {
        let mut g = self.inner.write();
        let v = g
            .volumes
            .get_mut(&volume_id)
            .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))?;
        v.state = VolumeState::EcEncoded;
        Ok(())
    }

    pub fn servers(&self) -> Vec<VolumeServerInfo> {
        self.inner.read().servers.values().cloned().collect()
    }

    /// Explicitly drop a data node (e.g. after STS scale-in). Removes its locations;
    /// volumes with no remaining locations are deleted from the catalog.
    pub fn remove_server(&self, node_id: &str) -> Result<()> {
        let mut g = self.inner.write();
        if g.servers.remove(node_id).is_none() {
            // Idempotent: already gone is OK for ops delete.
            return Ok(());
        }
        g.volume_seq_by_server.remove(node_id);
        let mut drop_ids = Vec::new();
        for (id, v) in g.volumes.iter_mut() {
            v.locations.retain(|l| l.node_id != node_id);
            if v.locations.is_empty() {
                drop_ids.push(*id);
            }
        }
        for id in drop_ids {
            g.volumes.remove(&id);
        }
        let live_vol_ids: std::collections::HashSet<u32> = g.volumes.keys().copied().collect();
        for ids in g.volume_seq_by_server.values_mut() {
            ids.retain(|id| live_vol_ids.contains(id));
        }
        g.volume_seq_by_server.retain(|_, ids| !ids.is_empty());
        Ok(())
    }

    pub fn prune_stale(&self, max_age: Duration) {
        let now = now_secs();
        let mut g = self.inner.write();
        g.servers
            .retain(|_, s| now.saturating_sub(s.last_seen) < max_age.as_secs());
        // Drop locations on dead data nodes; remove volumes that no longer have any location.
        // Otherwise Admin keeps showing ghost data volumes after scale-in / emptyDir recycle.
        let live: std::collections::HashSet<String> = g.servers.keys().cloned().collect();
        let mut drop_ids = Vec::new();
        for (id, v) in g.volumes.iter_mut() {
            v.locations.retain(|l| live.contains(&l.node_id));
            if v.locations.is_empty() {
                drop_ids.push(*id);
            }
        }
        for id in drop_ids {
            g.volumes.remove(&id);
        }
        let live_vol_ids: std::collections::HashSet<u32> = g.volumes.keys().copied().collect();
        g.volume_seq_by_server.retain(|nid, ids| {
            if !live.contains(nid) {
                return false;
            }
            ids.retain(|id| live_vol_ids.contains(id));
            !ids.is_empty()
        });
    }

    pub fn stats(&self) -> (usize, usize, u64, u64) {
        let g = self.inner.read();
        let vs = g.servers.len();
        let vc = g.volumes.len();
        let size = g.volumes.values().map(|v| v.size).sum();
        let files = g.volumes.values().map(|v| v.file_count).sum();
        (vs, vc, size, files)
    }

    /// Volumes whose location count is below replication.total_copies().
    pub fn under_replicated_count(&self) -> usize {
        self.inner
            .read()
            .volumes
            .values()
            .filter(|v| {
                !matches!(v.state, VolumeState::EcEncoded)
                    && (v.locations.len() as u8) < v.replication.total_copies()
            })
            .count()
    }

    pub fn assign_policy(&self) -> AssignPolicy {
        self.inner.read().assign_policy
    }

    pub fn set_assign_policy(&self, policy: AssignPolicy) {
        let mut g = self.inner.write();
        g.assign_policy = policy;
        if policy == AssignPolicy::RoundRobin {
            g.assign_rr_cursor = 0;
        }
    }

    /// Heal/balance push rate limit in bytes/sec (`0` = unlimited).
    pub fn heal_max_bps(&self) -> u64 {
        self.inner.read().heal_max_bps
    }

    pub fn set_heal_max_bps(&self, max_bps: u64) {
        self.inner.write().heal_max_bps = max_bps;
    }

    fn pick_assign_volume(g: &mut Inner, candidates: &[u32]) -> Result<u32> {
        if candidates.is_empty() {
            return Err(Error::NoWritableVolume);
        }
        let mut ids = candidates.to_vec();
        ids.sort_unstable();
        Ok(match g.assign_policy {
            AssignPolicy::Random => ids[rand::thread_rng().gen_range(0..ids.len())],
            AssignPolicy::RoundRobin => {
                let i = (g.assign_rr_cursor as usize) % ids.len();
                g.assign_rr_cursor = g.assign_rr_cursor.wrapping_add(1);
                ids[i]
            }
            AssignPolicy::FreeDisk => {
                let weights: Vec<u64> = ids
                    .iter()
                    .map(|id| {
                        g.volumes
                            .get(id)
                            .and_then(|v| v.locations.first())
                            .and_then(|l| g.servers.get(&l.node_id))
                            .map(|s| s.free_disk_bytes.max(1))
                            .unwrap_or(1)
                    })
                    .collect();
                let total: u64 = weights.iter().sum();
                if total == 0 {
                    ids[rand::thread_rng().gen_range(0..ids.len())]
                } else {
                    let mut tick = rand::thread_rng().gen_range(0..total);
                    let mut chosen = ids[0];
                    for (id, w) in ids.iter().zip(weights.iter()) {
                        if tick < *w {
                            chosen = *id;
                            break;
                        }
                        tick -= *w;
                    }
                    chosen
                }
            }
            AssignPolicy::LeastSize => {
                let mut best = ids[0];
                let mut best_size = g.volumes.get(&best).map(|v| v.size).unwrap_or(u64::MAX);
                for id in ids.iter().skip(1) {
                    let sz = g.volumes.get(id).map(|v| v.size).unwrap_or(u64::MAX);
                    if sz < best_size || (sz == best_size && rand::thread_rng().gen_bool(0.5)) {
                        best = *id;
                        best_size = sz;
                    }
                }
                best
            }
        })
    }

    pub fn export_snapshot(&self) -> ClusterSnapshot {
        let g = self.inner.read();
        ClusterSnapshot {
            servers: g.servers.values().cloned().collect(),
            volumes: g.volumes.values().cloned().collect(),
            next_volume_id: g.next_volume_id,
            next_file_key: g.next_file_key,
            assign_policy: g.assign_policy,
            assign_rr_cursor: g.assign_rr_cursor,
            heal_max_bps: g.heal_max_bps,
        }
    }

    pub fn import_snapshot(&self, snap: ClusterSnapshot) {
        let mut g = self.inner.write();
        g.servers.clear();
        for s in snap.servers {
            g.servers.insert(s.node_id.clone(), s);
        }
        g.volumes.clear();
        g.volume_seq_by_server.clear();
        for v in snap.volumes {
            for loc in &v.locations {
                g.volume_seq_by_server
                    .entry(loc.node_id.clone())
                    .or_default()
                    .push(v.id);
            }
            g.volumes.insert(v.id, v);
        }
        g.next_volume_id = snap.next_volume_id;
        g.next_file_key = snap.next_file_key;
        g.assign_policy = snap.assign_policy;
        g.assign_rr_cursor = snap.assign_rr_cursor;
        g.heal_max_bps = snap.heal_max_bps;
    }

    pub fn remove_volume_location(&self, volume_id: u32, node_id: &str) -> Result<()> {
        let mut g = self.inner.write();
        let v = g
            .volumes
            .get_mut(&volume_id)
            .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))?;
        v.locations.retain(|l| l.node_id != node_id);
        Ok(())
    }

    pub fn add_volume_location(
        &self,
        volume_id: u32,
        loc: TopologyLocation,
    ) -> Result<VolumeMeta> {
        let mut g = self.inner.write();
        let node_id = loc.node_id.clone();
        let v = g
            .volumes
            .get_mut(&volume_id)
            .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))?;
        if !v.locations.iter().any(|l| l.node_id == node_id) {
            v.locations.push(loc);
            drop(v);
            g.volume_seq_by_server
                .entry(node_id)
                .or_default()
                .push(volume_id);
        }
        g.volumes
            .get(&volume_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))
    }

    /// Prepare volumes on `source_node` to take an extra replica on `target_node`:
    /// bump XYZ when current copies are saturated, return ids that still lack target.
    pub fn prepare_place_replicas(
        &self,
        source_node: &str,
        target_node: &str,
    ) -> Result<Vec<u32>> {
        if source_node == target_node {
            return Err(Error::InvalidArgument(
                "source and target data nodes must differ".into(),
            ));
        }
        {
            let g = self.inner.read();
            if !g.servers.contains_key(source_node) {
                return Err(Error::NotFound(format!("data node {source_node}")));
            }
            if !g.servers.contains_key(target_node) {
                return Err(Error::NotFound(format!("data node {target_node}")));
            }
        }
        let mut g = self.inner.write();
        let mut need_heal = Vec::new();
        for v in g.volumes.values_mut() {
            if matches!(v.state, VolumeState::EcEncoded) {
                continue;
            }
            let on_source = v.locations.iter().any(|l| l.node_id == source_node);
            if !on_source {
                continue;
            }
            if v.locations.iter().any(|l| l.node_id == target_node) {
                continue;
            }
            // Need at least one more copy slot for the new replica node.
            let have = v.locations.len() as u8;
            if v.replication.total_copies() <= have {
                let mut r = v.replication;
                r.same_rack = r.same_rack.saturating_add(1);
                v.replication = r;
            }
            need_heal.push(v.id);
        }
        Ok(need_heal)
    }

    pub fn transfer_volume_primary(
        &self,
        volume_id: u32,
        from_node: &str,
        to_location: TopologyLocation,
    ) -> Result<()> {
        let mut g = self.inner.write();
        let v = g
            .volumes
            .get_mut(&volume_id)
            .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))?;
        v.locations.retain(|l| l.node_id != from_node);
        if !v.locations.iter().any(|l| l.node_id == to_location.node_id) {
            v.locations.insert(0, to_location);
        }
        Ok(())
    }
}

/// Apply Helm/CLI bootstrap intents while holding the cluster write lock.
fn apply_bootstrap_locked(
    g: &mut Inner,
    node_id: &str,
    info: &mut VolumeServerInfo,
    desired_online: bool,
    desired_replica: Option<&str>,
) {
    if let Some(src) = desired_replica {
        if src == node_id {
            return;
        }
        if !g.servers.contains_key(src) {
            // Source not registered yet — stay offline until it appears.
            return;
        }
        // Reject cycles (walk source chain).
        let mut walk = src.to_string();
        for _ in 0..=g.servers.len() {
            if walk == node_id {
                return;
            }
            match g.servers.get(&walk).and_then(|s| s.replica_of.clone()) {
                Some(next) => walk = next,
                None => break,
            }
        }
        info.replica_of = Some(src.to_string());
    }
    if desired_online && (desired_replica.is_none() || info.replica_of.is_some()) {
        info.online = true;
        promote_node_volumes_locked(g, node_id, true);
    }
}

fn promote_node_volumes_locked(g: &mut Inner, node_id: &str, online: bool) {
    for v in g.volumes.values_mut() {
        if !v.locations.iter().any(|l| l.node_id == node_id) {
            continue;
        }
        if online {
            if matches!(v.state, VolumeState::Offline) {
                v.state = VolumeState::Writable;
            }
        } else if matches!(v.state, VolumeState::Writable) {
            v.state = VolumeState::Offline;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSnapshot { // 集群快照
    pub servers: Vec<VolumeServerInfo>, // 服务器信息
    pub volumes: Vec<VolumeMeta>, // 卷的元数据
    pub next_volume_id: u32, // 下一个卷的ID
    pub next_file_key: u64, // 下一个文件的键
    #[serde(default)]
    pub assign_policy: AssignPolicy,
    #[serde(default)]
    pub assign_rr_cursor: u64,
    /// Max heal/balance push rate in bytes/sec; `0` = unlimited.
    #[serde(default)]
    pub heal_max_bps: u64,
}

#[derive(Debug, Clone)]
pub struct AssignResult { // 分配结果
    pub fid: FileId, // 文件ID
    pub count: u32, // 卷的数量
    pub url: String, // URL
    pub public_url: String, // 公共URL
    pub locations: Vec<TopologyLocation>, // 位置
}

#[derive(Debug, Clone, Deserialize)]
pub struct VolumeReport { // 卷的报告
    pub id: u32, // 卷的ID
    #[serde(default)]
    pub collection: String, // 卷的集合
    #[serde(default)]
    pub size: u64, // 卷的大小
    #[serde(default)]
    pub file_count: u64, // 卷的文件数量
    #[serde(default)]
    pub deleted_count: u64, // 卷的删除数量
    #[serde(default)]
    pub read_only: bool, // 卷的只读状态
    #[serde(default)]
    pub sealed: bool, // 卷的密封状态
    /// Optional; if empty Master derives from size/file_count/deleted_count.
    #[serde(default)]
    pub fingerprint: String,
}

fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn pick_locations(
    servers: &HashMap<String, VolumeServerInfo>,
    repl: Replication,
) -> Result<Vec<TopologyLocation>> {
    let mut list: Vec<_> = servers
        .values()
        .filter(|s| s.online && s.write_ready)
        .collect();
    if list.is_empty() {
        return Err(Error::NoWritableVolume);
    }
    // prefer more free space
    list.sort_by(|a, b| b.free_disk_bytes.cmp(&a.free_disk_bytes));

    let mut chosen: Vec<TopologyLocation> = Vec::new();
    let primary = list[0].location.clone();
    chosen.push(primary.clone());

    // same rack extras
    for s in &list {
        if chosen.len() as u8 >= repl.total_copies() {
            break;
        }
        if s.location.node_id == primary.node_id {
            continue;
        }
        if s.location.data_center == primary.data_center
            && s.location.rack == primary.rack
            && chosen.len() as u8 <= 1 + repl.same_rack
        {
            chosen.push(s.location.clone());
        }
    }
    // diff rack same dc
    for s in &list {
        if chosen.len() as u8 >= repl.total_copies() {
            break;
        }
        if s.location.data_center == primary.data_center
            && s.location.rack != primary.rack
            && !chosen.iter().any(|c| c.node_id == s.location.node_id)
        {
            chosen.push(s.location.clone());
        }
    }
    // diff dc
    for s in &list {
        if chosen.len() as u8 >= repl.total_copies() {
            break;
        }
        if s.location.data_center != primary.data_center
            && !chosen.iter().any(|c| c.node_id == s.location.node_id)
        {
            chosen.push(s.location.clone());
        }
    }
    // fill remaining with any
    for s in &list {
        if chosen.len() as u8 >= repl.total_copies() {
            break;
        }
        if !chosen.iter().any(|c| c.node_id == s.location.node_id) {
            chosen.push(s.location.clone());
        }
    }

    if chosen.is_empty() {
        return Err(Error::NoWritableVolume);
    }
    // shuffle secondary slightly
    let len = chosen.len();
    if len > 1 {
        let swap = rand::thread_rng().gen_range(1..len);
        chosen.swap(1.min(len - 1), swap);
    }
    Ok(chosen)
}

#[allow(dead_code)]
fn _instant_epoch(i: Instant) -> u64 {
    i.elapsed().as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;
    use opens3_common::replication::Replication;
    use opens3_common::topology::TopologyLocation;
    use std::collections::HashSet;
    use std::str::FromStr;

    fn loc(node: &str) -> TopologyLocation {
        TopologyLocation::new("dc1", "rack1", node, format!("http://{node}:8080"))
    }

    fn online_server(node: &str) -> VolumeServerInfo {
        VolumeServerInfo {
            node_id: node.into(),
            location: loc(node),
            max_volume_count: 8,
            free_disk_bytes: 1 << 30,
            last_seen: now_secs(),
            online: true,
            replica_of: None,
            boot_id: format!("boot-{node}"),
            write_ready: true,
        }
    }

    #[test]
    fn assign_spreads_across_writable_volumes() {
        let cs = ClusterState::new(Replication::DEFAULT);
        {
            let mut g = cs.inner.write();
            for (id, node) in [(1u32, "v0"), (2, "v1"), (3, "v2")] {
                g.servers.insert(node.into(), online_server(node));
                let mut meta = VolumeMeta::new(id, "", Replication::DEFAULT);
                meta.state = VolumeState::Writable;
                meta.locations = vec![loc(node)];
                g.volumes.insert(id, meta);
            }
        }
        let mut seen = HashSet::new();
        for _ in 0..80 {
            let a = cs.assign("", None, 1, 0).unwrap();
            seen.insert(a.fid.volume_id);
        }
        assert!(
            seen.len() >= 2,
            "expected PUTs to hit multiple volumes, got {seen:?}"
        );
    }

    #[test]
    fn assign_round_robin_cycles_volume_ids() {
        let cs = ClusterState::new(Replication::DEFAULT);
        {
            let mut g = cs.inner.write();
            for (id, node) in [(1u32, "v0"), (2, "v1"), (3, "v2")] {
                g.servers.insert(node.into(), online_server(node));
                let mut meta = VolumeMeta::new(id, "", Replication::DEFAULT);
                meta.state = VolumeState::Writable;
                meta.locations = vec![loc(node)];
                g.volumes.insert(id, meta);
            }
        }
        cs.set_assign_policy(AssignPolicy::RoundRobin);
        let got: Vec<_> = (0..6)
            .map(|_| cs.assign("", None, 1, 0).unwrap().fid.volume_id)
            .collect();
        assert_eq!(got, vec![1, 2, 3, 1, 2, 3]);
    }

    #[test]
    fn replica_of_single_source_and_fan_out() {
        let cs = ClusterState::new(Replication::DEFAULT);
        {
            let mut g = cs.inner.write();
            let mut offline = |id: &str| {
                let mut s = online_server(id);
                s.online = false;
                s
            };
            g.servers.insert("v0".into(), offline("v0"));
            g.servers.insert("v1".into(), offline("v1"));
            g.servers.insert("v2".into(), offline("v2"));
            g.servers.insert("v3".into(), offline("v3"));
        }
        cs.set_replica_of("v1", Some("v0")).unwrap();
        assert_eq!(
            cs.servers()
                .into_iter()
                .find(|s| s.node_id == "v1")
                .unwrap()
                .replica_of
                .as_deref(),
            Some("v0")
        );
        // cannot switch without clear
        let err = cs.set_replica_of("v1", Some("v2")).unwrap_err().to_string();
        assert!(err.contains("clear first"), "{err}");
        // fan-out: one source may have many replicas
        cs.set_replica_of("v2", Some("v0")).unwrap();
        assert_eq!(
            cs.servers()
                .into_iter()
                .find(|s| s.node_id == "v2")
                .unwrap()
                .replica_of
                .as_deref(),
            Some("v0")
        );
        // chaining allowed: v3 is replica of v1, which is replica of v0
        cs.set_replica_of("v3", Some("v1")).unwrap();
        assert_eq!(
            cs.servers()
                .into_iter()
                .find(|s| s.node_id == "v3")
                .unwrap()
                .replica_of
                .as_deref(),
            Some("v1")
        );
        // cycle rejected: v0 → v3 would close v0←v1←v3←v0
        let err = cs.set_replica_of("v0", Some("v3")).unwrap_err().to_string();
        assert!(err.contains("cycle"), "{err}");
    }

    #[test]
    fn replica_of_cannot_set_when_online() {
        let cs = ClusterState::new(Replication::DEFAULT);
        {
            let mut g = cs.inner.write();
            g.servers.insert("v0".into(), online_server("v0"));
            g.servers.insert("v1".into(), online_server("v1"));
        }
        let err = cs.set_replica_of("v1", Some("v0")).unwrap_err().to_string();
        assert!(err.contains("online"), "{err}");
        // offline → set OK; online locks pairing (clear also blocked)
        cs.set_server_online("v1", false).unwrap();
        cs.set_replica_of("v1", Some("v0")).unwrap();
        cs.set_server_online("v1", true).unwrap();
        let err = cs.set_replica_of("v1", None).unwrap_err().to_string();
        assert!(err.contains("online"), "{err}");
        cs.set_server_online("v1", false).unwrap();
        cs.set_replica_of("v1", None).unwrap();
    }

    #[test]
    fn prepare_place_replicas_bumps_xyz_and_lists_vols() {
        let cs = ClusterState::new(Replication::DEFAULT);
        {
            let mut g = cs.inner.write();
            g.servers.insert("v0".into(), online_server("v0"));
            g.servers.insert("v1".into(), online_server("v1"));
            let mut meta = VolumeMeta::new(1, "", Replication::DEFAULT);
            meta.state = VolumeState::Writable;
            meta.locations = vec![loc("v0")];
            g.volumes.insert(1, meta);
        }
        let ids = cs.prepare_place_replicas("v0", "v1").unwrap();
        assert_eq!(ids, vec![1]);
        let v = cs.lookup(1).unwrap();
        assert_eq!(v.replication.as_str(), "001");
        assert!(!v.locations.iter().any(|l| l.node_id == "v1"));
    }

    #[test]
    fn heartbeat_refreshes_volume_urls_even_when_disk_empty() {
        let cs = ClusterState::new(Replication::DEFAULT);
        {
            let mut g = cs.inner.write();
            let mut s = online_server("v0");
            s.location = TopologyLocation::new("dc1", "rack1", "v0", "http://10.0.0.1:8080");
            g.servers.insert("v0".into(), s);
            let mut meta = VolumeMeta::new(7, "", Replication::DEFAULT);
            meta.state = VolumeState::Writable;
            meta.locations = vec![TopologyLocation::new(
                "dc1",
                "rack1",
                "v0",
                "http://10.0.0.1:8080",
            )];
            g.volumes.insert(7, meta);
        }
        // Pod restarted with a new IP; disk empty so report has no volumes.
        let mut hb = online_server("v0");
        hb.boot_id = "boot-v0".into(); // same boot → no reconcile gate
        hb.location = TopologyLocation::new("dc1", "rack1", "v0", "http://10.0.0.99:8080");
        hb.max_volume_count = 1;
        let out = cs.heartbeat(hb, vec![]);
        assert!(
            out.create.contains(&7),
            "should rematerialize missing vol 7: {:?}",
            out.create
        );
        let v = cs.lookup(7).unwrap();
        assert_eq!(v.locations[0].url, "http://10.0.0.99:8080");
        let a = cs.assign("", None, 1, 0).unwrap();
        assert_eq!(a.url, "http://10.0.0.99:8080");
    }

    #[test]
    fn restart_clears_write_ready_when_multi_copy() {
        let cs = ClusterState::new(Replication::from_str("001").unwrap());
        {
            let mut g = cs.inner.write();
            g.servers.insert("v0".into(), online_server("v0"));
            g.servers.insert("v1".into(), online_server("v1"));
            let mut meta = VolumeMeta::new(1, "", Replication::from_str("001").unwrap());
            meta.state = VolumeState::Writable;
            meta.locations = vec![loc("v0"), loc("v1")];
            g.volumes.insert(1, meta);
        }
        assert!(cs.is_write_ready("v0"));
        let mut hb = online_server("v0");
        hb.boot_id = "boot-v0-restarted".into();
        hb.write_ready = false;
        hb.max_volume_count = 0;
        let out = cs.heartbeat(hb, vec![]);
        assert!(out.needs_reconcile);
        assert!(!out.write_ready);
        // Surviving replica still writable — assign must not stall the cluster.
        let a = cs.assign("", Some(Replication::from_str("001").unwrap()), 1, 0).unwrap();
        assert_eq!(a.locations.len(), 1);
        assert_eq!(a.locations[0].node_id, "v1");
        cs.set_write_ready("v0", true);
        let a2 = cs.assign("", Some(Replication::from_str("001").unwrap()), 1, 0).unwrap();
        assert_eq!(a2.locations.len(), 2);
    }

    #[test]
    fn new_volume_and_node_start_offline() {
        let cs = ClusterState::new(Replication::DEFAULT);
        let info = VolumeServerInfo {
            node_id: "v0".into(),
            location: loc("v0"),
            max_volume_count: 1,
            free_disk_bytes: 1 << 30,
            last_seen: now_secs(),
            online: false,
            replica_of: None,
            boot_id: "boot-v0".into(),
            write_ready: false,
        };
        let created = cs.heartbeat(info, vec![]).create;
        assert_eq!(created.len(), 1);
        let servers = cs.servers();
        assert!(!servers[0].online);
        let vols = cs.list_volumes("");
        assert_eq!(vols[0].state, VolumeState::Offline);
        assert!(cs.assign("", None, 1, 0).is_err());
        // Volumes follow the node: online node promotes its Offline volumes.
        cs.set_server_online("v0", true).unwrap();
        assert_eq!(cs.list_volumes("")[0].state, VolumeState::Writable);
        assert!(cs.assign("", None, 1, 0).is_ok());
    }

    #[test]
    fn bootstrap_start_online_joins_assign() {
        let cs = ClusterState::new(Replication::DEFAULT);
        let info = VolumeServerInfo {
            node_id: "v0".into(),
            location: loc("v0"),
            max_volume_count: 1,
            free_disk_bytes: 1 << 30,
            last_seen: now_secs(),
            online: false,
            replica_of: None,
            boot_id: "boot-v0".into(),
            write_ready: false,
        };
        cs.heartbeat_with_bootstrap(info, vec![], true, None);
        assert!(cs.servers()[0].online);
        assert_eq!(cs.list_volumes("")[0].state, VolumeState::Writable);
        assert!(cs.assign("", None, 1, 0).is_ok());
    }

    #[test]
    fn bootstrap_replica_waits_for_source_then_onlines() {
        let cs = ClusterState::new(Replication::DEFAULT);
        let mk = |id: &str| VolumeServerInfo {
            node_id: id.into(),
            location: loc(id),
            max_volume_count: 1,
            free_disk_bytes: 1 << 30,
            last_seen: now_secs(),
            online: false,
            replica_of: None,
            boot_id: format!("boot-{id}"),
            write_ready: false,
        };
        // Replica registers first — stays offline until source appears.
        cs.heartbeat_with_bootstrap(mk("v1"), vec![], true, Some("v0".into()));
        assert!(!cs.servers().iter().find(|s| s.node_id == "v1").unwrap().online);
        assert!(cs
            .servers()
            .iter()
            .find(|s| s.node_id == "v1")
            .unwrap()
            .replica_of
            .is_none());
        // Source comes online.
        cs.heartbeat_with_bootstrap(mk("v0"), vec![], true, None);
        assert!(cs.servers().iter().find(|s| s.node_id == "v0").unwrap().online);
        // Replica heartbeat retries bootstrap.
        cs.heartbeat_with_bootstrap(mk("v1"), vec![], true, Some("v0".into()));
        let v1 = cs.servers().into_iter().find(|s| s.node_id == "v1").unwrap();
        assert_eq!(v1.replica_of.as_deref(), Some("v0"));
        assert!(v1.online);
    }
}
