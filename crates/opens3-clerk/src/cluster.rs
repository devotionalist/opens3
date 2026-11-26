// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 集群拓扑与分配状态：VolumeServer / Volume 元数据、心跳、assign、副本策略。

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

/// `assign` 在可写卷中的选取策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum AssignPolicy {
    /// 候选中均匀随机（默认）。
    #[default]
    Random,
    /// 按 volume-id 顺序轮询。
    RoundRobin,
    /// 按主节点 `free_disk_bytes` 加权随机。
    FreeDisk,
    /// 优先当前 `size` 最小的卷。
    LeastSize,
}

impl AssignPolicy {
    /// 序列化用英文 id。
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Random => "random",
            Self::RoundRobin => "round_robin",
            Self::FreeDisk => "free_disk",
            Self::LeastSize => "least_size",
        }
    }

    /// 中文展示标签（Admin UI）。
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

    /// 解析策略名（含若干别名）。
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

/// Volume 数据节点在 Master 上的注册信息。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VolumeServerInfo {
    pub node_id: String,                // 节点 ID（pod / 逻辑名）
    pub location: TopologyLocation,     // DC/rack/url
    pub max_volume_count: u64,          // 可承载卷上限
    pub free_disk_bytes: u64,           // 空闲磁盘字节
    pub last_seen: u64,                 // 最近心跳 Unix 秒
    /// 运维开关：新节点默认 offline，上线前不接 assign。
    #[serde(default)]
    pub online: bool,
    /// 若设置，表示本节点是 `replica_of`（源 node_id）的副本。
    #[serde(default)]
    pub replica_of: Option<String>,
    /// Volume 进程 boot id；变化 ⇒ 重启 ⇒ 需重新指纹对齐。
    #[serde(default)]
    pub boot_id: String,
    /// 重启后为 false，直到与 peer 指纹对齐完成。
    /// 旧 Raft 快照默认 true，保证存量集群可继续服务。
    #[serde(default = "default_true")]
    pub write_ready: bool,
}

/// serde 缺省 true（兼容旧快照）。
fn default_true() -> bool {
    true
}

/// 轻量卷内容指纹（`size:file_count:deleted_count`）。
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct VolumeFingerprint {
    pub size: u64,          // 卷字节
    pub file_count: u64,    // 存活文件数
    pub deleted_count: u64, // 软删文件数
}

impl VolumeFingerprint {
    /// 构造指纹。
    pub fn new(size: u64, file_count: u64, deleted_count: u64) -> Self {
        Self {
            size,
            file_count,
            deleted_count,
        }
    }

    /// 比较用字符串键。
    pub fn key(&self) -> String {
        format!("{}:{}:{}", self.size, self.file_count, self.deleted_count)
    }
}

/// 应用一次 VolumeServer 心跳的结果。
#[derive(Debug, Clone)]
pub struct HeartbeatOutcome {
    pub create: Vec<u32>, // 需在该节点物化的卷 id
    /// `boot_id` 变化（或首次见到）时为 true — 调用方应跑指纹 reconcile。
    pub needs_reconcile: bool,
    pub write_ready: bool, // 心跳后该节点是否可写
}

/// ClusterState 内部可变状态（RwLock 保护）。
struct Inner {
    servers: HashMap<String, VolumeServerInfo>, // node_id → 节点
    volumes: HashMap<u32, VolumeMeta>,          // volume_id → 元数据
    next_volume_id: u32,                        // 下一可用卷 id
    next_file_key: u64,                         // 下一 file_key
    volume_seq_by_server: HashMap<String, Vec<u32>>, // 节点上卷创建顺序
    assign_policy: AssignPolicy,
    /// [`AssignPolicy::RoundRobin`] 游标。
    assign_rr_cursor: u64,
    /// heal/balance 推送限速字节/秒（`0` = 不限）。
    heal_max_bps: u64,
}

/// 集群拓扑内存态（可克隆共享；经 Raft 快照复制）。
#[derive(Clone)]
pub struct ClusterState {
    inner: Arc<RwLock<Inner>>,
    pub default_replication: Replication, // 默认 XYZ 复制策略
}

impl ClusterState {
    /// 以给定默认复制策略创建空集群。
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

    /// 普通心跳（无 bootstrap 意图）。
    pub fn heartbeat(
        &self,
        mut info: VolumeServerInfo,
        reported: Vec<VolumeReport>,
    ) -> HeartbeatOutcome {
        self.heartbeat_with_bootstrap(info, reported, false, None)
    }

    /// 带部署 bootstrap 意图的心跳（`desired_online` / `desired_replica_of`）。
    ///
    /// - 首次注册：应用 bootstrap（副本等源出现前保持 offline）。
    /// - 之后：保留运维 online/replica_of；仍 offline 且未配对时继续尝试
    ///   `desired_replica_of`，满足后再按需上线。
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
            // 重启 / 首次注册：对齐指纹前禁止写入。
            info.write_ready = false;
        } else {
            info.write_ready = prev.map(|s| s.write_ready).unwrap_or(false);
        }

        // 插入前解析 bootstrap 副本 / 上线，使卷状态跟随节点。
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
            // 心跳不自动 Offline→writable；由节点 online 提升。
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

        // 刷新本节点承载的所有卷 URL — 即使磁盘报告为空（Pod 重启 / emptyDir 清空）。
        // 否则 assign 会继续下发过期 Pod IP，S3 侧 grpc 连接失败。
        if let Some(loc) = &srv_loc {
            for v in g.volumes.values_mut() {
                if let Some(existing) = v.locations.iter_mut().find(|l| l.node_id == node_id) {
                    *existing = loc.clone();
                }
            }
        }

        // 未达容量上限时自动创建卷槽
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
                    // 卷跟随节点：已 online 的节点上新卷直接可写。
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
            // 重启后磁盘缺失的拓扑卷需重新物化。
            for v in g.volumes.values() {
                if v.locations.iter().any(|l| l.node_id == node_id)
                    && !reported_ids.contains(&v.id)
                    && !to_create.contains(&v.id)
                {
                    to_create.push(v.id);
                }
            }
        }
        // 单副本 / 无多副本卷：无需比对，立即 write-ready。
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

    /// 设置节点 write_ready；节点不存在返回 false。
    pub fn set_write_ready(&self, node_id: &str, ready: bool) -> bool {
        let mut g = self.inner.write();
        if let Some(s) = g.servers.get_mut(node_id) {
            s.write_ready = ready;
            return true;
        }
        false
    }

    /// 查询节点是否 write_ready（未知节点视为 false）。
    pub fn is_write_ready(&self, node_id: &str) -> bool {
        self.inner
            .read()
            .servers
            .get(node_id)
            .map(|s| s.write_ready)
            .unwrap_or(false)
    }

    /// 位于 `node_id` 且另有至少一个副本位置的卷。
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

    /// 分配 FileId：选可写卷、补齐副本位置、签发 cookie。
    pub fn assign(
        &self,
        collection: &str,
        replication: Option<Replication>,
        count: u32,
        ttl_seconds: u64,
    ) -> Result<AssignResult> {
        let repl = replication.unwrap_or(self.default_replication);
        let mut g = self.inner.write();

        // 降级写：至少一个 location online+write_ready 即可 assign。
        // 正在 reconcile（重启）的节点不放入返回 locations，后台追赶不堵写路径。
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

        // 此处不自动建卷：新卷默认 offline，须运维上线。
        let volume_id = Self::pick_assign_volume(&mut g, &candidates)?;

        // 补齐 collection；欠副本时 top-up 位置
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

        // 优先用心跳中的存活 URL，避免 volume.locations 过期。
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
            // 仅 write-ready 副本 — 同步扇出跳过仍在追赶的节点。
            locations: write_locs,
        })
    }

    /// 按 volume_id 查找元数据。
    pub fn lookup(&self, volume_id: u32) -> Result<VolumeMeta> {
        self.inner
            .read()
            .volumes
            .get(&volume_id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))
    }

    /// 列出卷；`collection` 空则全部。
    pub fn list_volumes(&self, collection: &str) -> Vec<VolumeMeta> {
        self.inner
            .read()
            .volumes
            .values()
            .filter(|v| collection.is_empty() || v.collection == collection)
            .cloned()
            .collect()
    }

    /// 将卷标记为 Sealed。
    pub fn seal(&self, volume_id: u32) -> Result<()> {
        let mut g = self.inner.write();
        let v = g
            .volumes
            .get_mut(&volume_id)
            .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))?;
        v.state = VolumeState::Sealed;
        Ok(())
    }

    /// 卷上线（Offline→Writable）或下线（禁止新写）。
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

    /// 记录 `node_id` 是 `source` 的副本（`None` 清除）。
    /// 约束：
    /// - 一节点至多一个源（单源）；
    /// - 一源可有多副本（扇出 A←B、A←C）；
    /// - 允许链式（A←B←C），拒绝环；
    /// - 仅 offline 时可改配对。
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
        // online 时拒绝任何 replica_of 变更（设置或清除）。
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
            // 链式允许；拒绝环（沿源的 replica_of 链走）。
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
                // 同源 — 幂等
                return Ok(());
            }
        }
        let s = g.servers.get_mut(node_id).unwrap();
        s.replica_of = src;
        Ok(())
    }

    /// 数据节点运维开关；其上数据卷随节点上下线（不单独暴露按卷运维）。
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

    /// 更新卷 XYZ 复制策略；不拷贝数据，需随后 heal 物化缺失副本。
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

    /// 批量应用 XYZ；`from_code` 非空时只改当前策略匹配的卷（如全部 `000`→`001`）。
    /// 空 `volume_ids` 表示全部（可再按 `from_code` 过滤）。
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

    /// 在 Master 上预留 `count` 个空卷及 XYZ 放置；物理文件靠心跳 / replicate 创建
    ///（不同于节点本地 `/admin/create_volume`）。
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
            // 仅挑 online 节点；其上新卷直接 Writable。
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

    /// 将卷标记为 EC 编码态。
    pub fn mark_ec(&self, volume_id: u32) -> Result<()> {
        let mut g = self.inner.write();
        let v = g
            .volumes
            .get_mut(&volume_id)
            .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))?;
        v.state = VolumeState::EcEncoded;
        Ok(())
    }

    /// 列出全部数据节点。
    pub fn servers(&self) -> Vec<VolumeServerInfo> {
        self.inner.read().servers.values().cloned().collect()
    }

    /// 显式移除数据节点（如 STS 缩容）。清其 location；无剩余位置的卷从目录删除。
    pub fn remove_server(&self, node_id: &str) -> Result<()> {
        let mut g = self.inner.write();
        if g.servers.remove(node_id).is_none() {
            // 幂等：已不存在对运维删除仍 OK。
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

    /// 剔除心跳超时节点，并清理幽灵卷 location。
    pub fn prune_stale(&self, max_age: Duration) {
        let now = now_secs();
        let mut g = self.inner.write();
        g.servers
            .retain(|_, s| now.saturating_sub(s.last_seen) < max_age.as_secs());
        // 去掉死节点上的 location；无位置的卷删除，避免缩容 / emptyDir 后 Admin 残留幽灵卷。
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

    /// 返回 (节点数, 卷数, 总字节, 总文件数)。
    pub fn stats(&self) -> (usize, usize, u64, u64) {
        let g = self.inner.read();
        let vs = g.servers.len();
        let vc = g.volumes.len();
        let size = g.volumes.values().map(|v| v.size).sum();
        let files = g.volumes.values().map(|v| v.file_count).sum();
        (vs, vc, size, files)
    }

    /// location 数低于 replication.total_copies() 的卷数量（不含 EC）。
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

    /// 当前 assign 策略。
    pub fn assign_policy(&self) -> AssignPolicy {
        self.inner.read().assign_policy
    }

    /// 设置 assign 策略；切到 RoundRobin 时重置游标。
    pub fn set_assign_policy(&self, policy: AssignPolicy) {
        let mut g = self.inner.write();
        g.assign_policy = policy;
        if policy == AssignPolicy::RoundRobin {
            g.assign_rr_cursor = 0;
        }
    }

    /// heal/balance 推送限速（字节/秒；`0` = 不限）。
    pub fn heal_max_bps(&self) -> u64 {
        self.inner.read().heal_max_bps
    }

    /// 设置 heal/balance 限速。
    pub fn set_heal_max_bps(&self, max_bps: u64) {
        self.inner.write().heal_max_bps = max_bps;
    }

    /// 按策略从候选卷中选一个 volume_id。
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

    /// 导出可 Raft 复制的集群快照。
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

    /// 用快照覆盖本地拓扑（follower apply / 启动恢复）。
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

    /// 从卷上移除某节点的 location。
    pub fn remove_volume_location(&self, volume_id: u32, node_id: &str) -> Result<()> {
        let mut g = self.inner.write();
        let v = g
            .volumes
            .get_mut(&volume_id)
            .ok_or_else(|| Error::NotFound(format!("volume {volume_id}")))?;
        v.locations.retain(|l| l.node_id != node_id);
        Ok(())
    }

    /// 为卷添加 location（幂等）；成功返回最新元数据。
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

    /// 为 `source_node` 上的卷在 `target_node` 准备额外副本：
    /// 副本已满时抬升 XYZ，返回仍缺 target 的卷 id。
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
            // 新副本节点至少需要一个额外 copy 槽。
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

    /// 转移卷主位置：去掉 from，并把 to 插到 locations 首位。
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

/// 持有写锁时应用 Helm/CLI bootstrap 意图。
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
            // 源尚未注册 — 保持 offline 直至出现。
            return;
        }
        // 拒绝环（沿源链走）。
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

/// 将节点上 Offline↔Writable 卷随节点上下线切换。
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

/// 可经 Raft 复制的集群全量快照。
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterSnapshot {
    pub servers: Vec<VolumeServerInfo>, // 数据节点
    pub volumes: Vec<VolumeMeta>,       // 卷元数据
    pub next_volume_id: u32,            // 下一卷 id
    pub next_file_key: u64,             // 下一 file_key
    #[serde(default)]
    pub assign_policy: AssignPolicy,
    #[serde(default)]
    pub assign_rr_cursor: u64,          // RoundRobin 游标
    /// heal/balance 推送限速字节/秒；`0` = 不限。
    #[serde(default)]
    pub heal_max_bps: u64,
}

/// `assign` 成功结果。
#[derive(Debug, Clone)]
pub struct AssignResult {
    pub fid: FileId,                           // 分配的 FileId
    pub count: u32,                            // 连续 key 数量
    pub url: String,                           // 主副本内部 URL
    pub public_url: String,                    // 主副本对外 URL
    pub locations: Vec<TopologyLocation>,      // 可写副本位置
}

/// Volume 心跳上报的单卷状态。
#[derive(Debug, Clone, Deserialize)]
pub struct VolumeReport {
    pub id: u32, // 卷 ID
    #[serde(default)]
    pub collection: String,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub file_count: u64,
    #[serde(default)]
    pub deleted_count: u64,
    #[serde(default)]
    pub read_only: bool,
    #[serde(default)]
    pub sealed: bool,
    /// 可选；空则 Master 由 size/file_count/deleted_count 推导。
    #[serde(default)]
    pub fingerprint: String,
}

/// Unix 秒。
fn now_secs() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// 按 XYZ 策略从 online+write_ready 节点挑选放置位置。
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
    // 优先空闲空间更大的节点
    list.sort_by(|a, b| b.free_disk_bytes.cmp(&a.free_disk_bytes));

    let mut chosen: Vec<TopologyLocation> = Vec::new();
    let primary = list[0].location.clone();
    chosen.push(primary.clone());

    // 同 rack 额外副本
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
    // 同 DC 不同 rack
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
    // 不同 DC
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
    // 仍不足则任意补齐
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
    // 轻微打乱次要副本顺序
    let len = chosen.len();
    if len > 1 {
        let swap = rand::thread_rng().gen_range(1..len);
        chosen.swap(1.min(len - 1), swap);
    }
    Ok(chosen)
}

/// 测试辅助：Instant 相对经过秒（保留占位）。
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

    /// 测试用拓扑位置。
    fn loc(node: &str) -> TopologyLocation {
        TopologyLocation::new("dc1", "rack1", node, format!("http://{node}:8080"))
    }

    /// 测试用已 online+write_ready 的节点。
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
