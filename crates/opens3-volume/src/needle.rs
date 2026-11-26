// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Needle 卷：append-only `.dat` + `.idx` 日志，支持批量 flush 与 vacuum 压缩。

use opens3_common::error::{Error, Result};
use opens3_common::types::{NEEDLE_CHECKSUM_SIZE, NEEDLE_HEADER_SIZE};
use opens3_common::util;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use tracing::warn;

/// On-disk needle layout:
/// [cookie u32][size u32][flags u8][pad 3][ttl u32][file_key u64][payload...][crc32 u32]
///
/// `.idx` is an **append-only log** of fixed 28-byte records (last entry for a
/// `file_key` wins). Writes buffer into memory and flush in batches; vacuum
/// rewrites live needles to `.cpd`/`.cpx`, then commits via rename (SeaweedFS-style).
///
/// 单条 `.idx` 记录字节数（固定 28B）
pub const IDX_ENTRY_SIZE: usize = 8 + 8 + 4 + 4 + 1 + 3;

/// Marker that [`NeedleVolume::vacuum_compact`] completed and awaits commit.
/// vacuum_compact 完成、等待 commit 的内存标记
#[derive(Debug, Clone)]
struct PendingVacuum {
    #[allow(dead_code)]
    before_size: u64, // compact 前 .dat 大小
    #[allow(dead_code)]
    new_size: u64, // 压缩后新大小
    #[allow(dead_code)]
    new_index: HashMap<u64, NeedleIndex>, // 压缩后的内存索引快照
}

/// Default: flush idx/dat every N mutations (1 = every op; higher = throughput).
/// 默认每 N 次写/删刷一次 idx/dat
pub const DEFAULT_IDX_FLUSH_EVERY: u32 = 64;

/// Durability vs throughput when flushing dat/idx.
/// dat/idx 刷盘的持久化强度
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SyncMode {
    /// `flush` only — fastest; risk losing the last batch on crash.
    /// 仅 flush，最快；崩溃可能丢最后一批
    None,
    /// `sync_data` — default: data durable, metadata may lag slightly.
    /// sync_data：数据落盘，元数据可能略滞后
    #[default]
    Data,
    /// `sync_all` — strongest durability, slowest.
    /// sync_all：最强持久化，最慢
    All,
}

impl SyncMode {
    /// 解析配置字符串：`none`/`data`/`all` 等别名
    pub fn parse(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "none" | "off" | "0" | "false" => Some(Self::None),
            "data" | "datasync" | "sync_data" => Some(Self::Data),
            "all" | "full" | "sync_all" => Some(Self::All),
            _ => None,
        }
    }

    /// 按模式对已打开文件做 fsync
    fn apply(self, file: &File) -> std::io::Result<()> {
        match self {
            Self::None => Ok(()),
            Self::Data => file.sync_data(),
            Self::All => file.sync_all(),
        }
    }
}

/// Tunables applied when opening/creating a volume.
/// 打开/创建卷时的 IO 可调参数
#[derive(Debug, Clone)]
pub struct VolumeIoOpts {
    pub idx_flush_every: u32, // 每 N 次变更 flush；0 = 仅显式 flush
    pub sync_mode: SyncMode,  // 刷盘持久化模式
}

impl Default for VolumeIoOpts {
    fn default() -> Self {
        Self {
            idx_flush_every: DEFAULT_IDX_FLUSH_EVERY,
            sync_mode: SyncMode::Data,
        }
    }
}

impl VolumeIoOpts {
    /// `OPENS3_VOLUME_IDX_FLUSH_EVERY`, `OPENS3_VOLUME_SYNC` (`none`|`data`|`all`).
    /// 从环境变量读取 IO 选项
    pub fn from_env() -> Self {
        let mut o = Self::default();
        if let Ok(v) = std::env::var("OPENS3_VOLUME_IDX_FLUSH_EVERY") {
            if let Ok(n) = v.parse::<u32>() {
                o.idx_flush_every = n;
            }
        }
        if let Ok(v) = std::env::var("OPENS3_VOLUME_SYNC") {
            if let Some(m) = SyncMode::parse(&v) {
                o.sync_mode = m;
            }
        }
        o
    }
}

/// 以读写方式打开文件；`create` 控制是否新建
fn open_rw(path: &Path, create: bool) -> std::io::Result<File> {
    OpenOptions::new()
        .create(create)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
}

/// 单条 needle 在 .idx / 内存中的定位信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeedleIndex {
    pub offset: u64,   // 在 .dat 中的起始偏移
    pub size: u32,     // body 长度（含 file_key）
    pub cookie: u32,   // 防伪 cookie，读写须匹配
    pub deleted: bool, // 软删标记；空间靠 vacuum 回收
}

/// 单个 Needle 卷：持有 `.dat`/`.idx` 句柄与内存索引
#[derive(Debug)]
pub struct NeedleVolume {
    pub id: u32,          // 卷 id
    dat_path: PathBuf,    // `.dat` 路径
    idx_path: PathBuf,    // `.idx` 路径
    dat: File,            // 数据文件句柄
    idx: File,            // 索引文件句柄
    /// Pending idx records not yet written to disk.
    /// 尚未落盘的 idx 记录缓冲
    idx_buf: Vec<u8>,
    dirty_ops: u32, // 自上次 flush 以来的写/删次数
    /// Flush every N write/delete ops. `0` = only explicit flush / compact / drop.
    /// 每 N 次写/删 flush；`0` 仅显式 flush / compact / drop
    pub idx_flush_every: u32,
    pub sync_mode: SyncMode,              // 刷盘模式
    pub index: HashMap<u64, NeedleIndex>, // file_key → 最新索引（后写覆盖）
    pub size: u64,                        // 逻辑 .dat 末尾（已校验完整）
    pub file_count: u64,                  // 存活 file_key 数
    pub deleted_count: u64,               // 软删 file_key 数
    pub read_only: bool,                  // 只读（含密封）
    pub sealed: bool,                     // 已密封，不再接受写入
    pub max_size: u64,                    // 卷容量上限（字节）
    /// True between vacuum_compact and vacuum_commit/cleanup; blocks writes/deletes.
    /// compact 与 commit/cleanup 之间为 true，阻止写/删
    pub vacuuming: bool,
    pending_vacuum: Option<PendingVacuum>, // 待提交的压缩结果
}

/// 定位读（pread）：不移动文件游标，读锁下安全
fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(buf, offset)
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0usize;
        while done < buf.len() {
            let n = file.seek_read(&mut buf[done..], offset + done as u64)?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "unexpected eof in pread",
                ));
            }
            done += n;
        }
        Ok(())
    }
    #[cfg(not(any(unix, windows)))]
    {
        let _ = (file, buf, offset);
        Err(std::io::Error::new(
            std::io::ErrorKind::Unsupported,
            "pread not supported on this platform",
        ))
    }
}

impl NeedleVolume {
    /// 用默认 IO 选项创建/打开卷
    pub fn create(dir: &Path, id: u32, max_size: u64) -> Result<Self> {
        Self::create_with_opts(dir, id, max_size, &VolumeIoOpts::default())
    }

    /// 创建卷文件并加载索引；顺带做 vacuum 崩溃恢复
    pub fn create_with_opts(dir: &Path, id: u32, max_size: u64, opts: &VolumeIoOpts) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        let dat_path = dir.join(format!("{id}.dat"));
        let idx_path = dir.join(format!("{id}.idx"));
        let mut vol = Self {
            id,
            dat: open_rw(&dat_path, true)?,
            idx: open_rw(&idx_path, true)?,
            dat_path,
            idx_path,
            idx_buf: Vec::with_capacity(IDX_ENTRY_SIZE * opts.idx_flush_every.max(1) as usize),
            dirty_ops: 0,
            idx_flush_every: opts.idx_flush_every,
            sync_mode: opts.sync_mode,
            index: Default::default(),
            size: 0,
            file_count: 0,
            deleted_count: 0,
            read_only: false,
            sealed: false,
            max_size,
            vacuuming: false,
            pending_vacuum: None,
        };
        vol.load_index()?;
        vol.reconcile_vacuum_files()?;
        Ok(vol)
    }

    /// vacuum 临时数据文件路径（`.cpd`）
    fn cpd_path(&self) -> PathBuf {
        self.dat_path.with_extension("cpd")
    }

    /// vacuum 临时索引路径（`.cpx`）
    fn cpx_path(&self) -> PathBuf {
        self.idx_path.with_extension("cpx")
    }

    /// vacuum 提交标记路径（`.cpc`，SeaweedFS 风格）
    fn cpc_path(&self) -> PathBuf {
        self.dat_path.with_extension("cpc")
    }

    /// 删除 vacuum 临时文件（忽略不存在）
    fn remove_vacuum_temps(cpd: &Path, cpx: &Path, cpc: &Path) {
        let _ = std::fs::remove_file(cpd);
        let _ = std::fs::remove_file(cpx);
        let _ = std::fs::remove_file(cpc);
    }

    /// Crash recovery: finish commit if `.cpc` present; else drop orphan temps.
    /// 崩溃恢复：有完整 `.cpc` 则继续 commit，否则丢弃孤儿临时文件
    fn reconcile_vacuum_files(&mut self) -> Result<()> {
        let cpd = self.cpd_path();
        let cpx = self.cpx_path();
        let cpc = self.cpc_path();
        let has_cpc = cpc.exists();
        let has_cpd = cpd.exists();
        let has_cpx = cpx.exists();
        if has_cpc && has_cpd && has_cpx {
            warn!(
                volume = self.id,
                "vacuum: completing interrupted commit from .cpc marker"
            );
            self.apply_vacuum_swap(&cpd, &cpx, &cpc)?;
            return Ok(());
        }
        if has_cpc || has_cpd || has_cpx {
            warn!(
                volume = self.id,
                "vacuum: discarding incomplete temp files (no durable .cpc)"
            );
            Self::remove_vacuum_temps(&cpd, &cpx, &cpc);
        }
        self.vacuuming = false;
        self.pending_vacuum = None;
        Ok(())
    }

    /// 将内存索引整表写入 `path`（截断覆盖）并 sync_data
    fn write_idx_snapshot_to(path: &Path, index: &HashMap<u64, NeedleIndex>) -> Result<()> {
        let mut buf = Vec::with_capacity(index.len() * IDX_ENTRY_SIZE);
        for (k, v) in index {
            Self::encode_idx_entry(*k, v, &mut buf);
        }
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)?;
        f.write_all(&buf)?;
        f.flush()?;
        f.sync_data()?;
        Ok(())
    }

    /// 用 `.cpd`/`.cpx` rename 替换正式文件，并重新加载索引
    fn apply_vacuum_swap(&mut self, cpd: &Path, cpx: &Path, cpc: &Path) -> Result<()> {
        // Ensure handles do not keep the old inode open across rename.
        // 关闭旧 inode 引用，避免 rename 后仍指向旧文件
        let _ = self.flush();
        self.dat = open_rw(&self.dat_path, false)?;
        self.idx = open_rw(&self.idx_path, false)?;
        std::fs::rename(cpd, &self.dat_path)?;
        std::fs::rename(cpx, &self.idx_path)?;
        let _ = std::fs::remove_file(cpc);
        self.dat = open_rw(&self.dat_path, false)?;
        self.idx = open_rw(&self.idx_path, false)?;
        self.idx_buf.clear();
        self.dirty_ops = 0;
        self.load_index()?;
        self.vacuuming = false;
        self.pending_vacuum = None;
        self.dat.seek(SeekFrom::End(0))?;
        Ok(())
    }

    /// Garbage ratio in `[0.0, 1.0]`: deleted / (live + deleted).
    /// 垃圾比例：deleted / (live + deleted)；按 key 计数，非字节占用
    pub fn vacuum_check(&self) -> f64 {
        let live = self.file_count;   // 存活 file_key 数
        let dead = self.deleted_count; // 软删 file_key 数
        // 空卷视为无垃圾，避免除零
        let total = live.saturating_add(dead);
        if total == 0 {
            0.0
        } else {
            dead as f64 / total as f64
        }
    }

    /// Copy live needles to `.cpd` / `.cpx`. Does not replace `.dat`/`.idx` yet.
    /// Blocks further writes until [`vacuum_commit`] or [`vacuum_cleanup`].
    /// 将存活 needle 拷到临时文件；提交前阻止写入
    pub fn vacuum_compact(&mut self) -> Result<(u64, u64)> {
        if self.vacuuming {
            return Err(Error::Internal(format!(
                "volume {} already vacuuming",
                self.id
            )));
        }
        self.flush()?;
        let before = self.size;
        let cpd = self.cpd_path();
        let cpx = self.cpx_path();
        let cpc = self.cpc_path();
        Self::remove_vacuum_temps(&cpd, &cpx, &cpc);

        let mut out = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&cpd)?;

        // 仅拷贝未软删条目
        let live: Vec<(u64, NeedleIndex)> = self
            .index
            .iter()
            .filter(|(_, v)| !v.deleted)
            .map(|(k, v)| (*k, v.clone()))
            .collect();

        let mut new_index = HashMap::with_capacity(live.len());
        let mut new_size = 0u64;
        for (file_key, old) in live {
            let Ok(data) = self.read_needle(file_key, old.cookie) else {
                continue; // 读失败则跳过该 key
            };
            let offset = new_size;
            let size = (8 + data.len()) as u32;
            let mut header = [0u8; NEEDLE_HEADER_SIZE];
            header[0..4].copy_from_slice(&old.cookie.to_le_bytes());
            header[4..8].copy_from_slice(&size.to_le_bytes());
            out.write_all(&header)?;
            out.write_all(&file_key.to_le_bytes())?;
            out.write_all(&data)?;
            out.write_all(&util::crc32(&data).to_le_bytes())?;
            new_size += NEEDLE_HEADER_SIZE as u64 + size as u64 + NEEDLE_CHECKSUM_SIZE as u64;
            new_index.insert(
                file_key,
                NeedleIndex {
                    offset,
                    size,
                    cookie: old.cookie,
                    deleted: false,
                },
            );
        }
        out.flush()?;
        self.sync_mode.apply(&out)?;
        drop(out);
        Self::write_idx_snapshot_to(&cpx, &new_index)?;

        self.vacuuming = true;
        self.pending_vacuum = Some(PendingVacuum {
            before_size: before,
            new_size,
            new_index,
        });
        Ok((before, new_size))
    }

    /// Atomically swap compacted files into place (writes `.cpc` marker first).
    /// 先写持久 `.cpc` 标记，再原子替换正式文件
    pub fn vacuum_commit(&mut self) -> Result<u64> {
        if self.pending_vacuum.is_none() {
            return Err(Error::InvalidArgument(format!(
                "volume {}: vacuum_commit without vacuum_compact",
                self.id
            )));
        }
        let cpd = self.cpd_path();
        let cpx = self.cpx_path();
        let cpc = self.cpc_path();
        if !cpd.exists() || !cpx.exists() {
            self.vacuuming = false;
            self.pending_vacuum = None;
            return Err(Error::Internal(format!(
                "volume {}: missing .cpd/.cpx for commit",
                self.id
            )));
        }

        // Durable commit marker before renames (SeaweedFS .cpc).
        // rename 前先落盘提交标记，崩溃可续跑
        {
            let mut m = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&cpc)?;
            m.write_all(b"opens3-vacuum-commit\n")?;
            m.flush()?;
            m.sync_all()?;
        }

        self.apply_vacuum_swap(&cpd, &cpx, &cpc)?;
        Ok(self.size)
    }

    /// Drop temp files and clear vacuuming (also after a failed/aborted compact).
    /// 清理临时文件；若 `.cpc` 齐全则向前完成提交而非丢弃
    pub fn vacuum_cleanup(&mut self) -> Result<()> {
        let cpd = self.cpd_path();
        let cpx = self.cpx_path();
        let cpc = self.cpc_path();
        if cpc.exists() && cpd.exists() && cpx.exists() {
            // Incomplete commit — finish forward rather than discard.
            return self.apply_vacuum_swap(&cpd, &cpx, &cpc).map(|_| ());
        }
        Self::remove_vacuum_temps(&cpd, &cpx, &cpc);
        self.vacuuming = false;
        self.pending_vacuum = None;
        Ok(())
    }

    /// 打开已有卷（实现上等同 create）
    pub fn open(dir: &Path, id: u32, max_size: u64) -> Result<Self> {
        Self::create(dir, id, max_size)
    }

    /// 带 IO 选项打开已有卷
    pub fn open_with_opts(dir: &Path, id: u32, max_size: u64, opts: &VolumeIoOpts) -> Result<Self> {
        Self::create_with_opts(dir, id, max_size, opts)
    }

    /// 编码一条固定长度 idx 记录到 `out`
    fn encode_idx_entry(file_key: u64, idx: &NeedleIndex, out: &mut Vec<u8>) {
        out.extend_from_slice(&file_key.to_le_bytes());
        out.extend_from_slice(&idx.offset.to_le_bytes());
        out.extend_from_slice(&idx.size.to_le_bytes());
        out.extend_from_slice(&idx.cookie.to_le_bytes());
        out.push(u8::from(idx.deleted));
        out.extend_from_slice(&[0, 0, 0]); // 3 字节对齐填充
    }

    /// 解码一条 idx 记录 → `(file_key, NeedleIndex)`
    fn decode_idx_entry(rec: &[u8]) -> (u64, NeedleIndex) {
        debug_assert!(rec.len() >= IDX_ENTRY_SIZE);
        let file_key = u64::from_le_bytes(rec[0..8].try_into().unwrap());
        let entry = NeedleIndex {
            offset: u64::from_le_bytes(rec[8..16].try_into().unwrap()),
            size: u32::from_le_bytes(rec[16..20].try_into().unwrap()),
            cookie: u32::from_le_bytes(rec[20..24].try_into().unwrap()),
            deleted: rec[24] != 0,
        };
        (file_key, entry)
    }

    /// 将 idx 字节流按记录灌入 HashMap（后写覆盖）
    fn apply_idx_bytes(index: &mut HashMap<u64, NeedleIndex>, data: &[u8]) {
        for rec in data.chunks_exact(IDX_ENTRY_SIZE) {
            let (file_key, entry) = Self::decode_idx_entry(rec);
            index.insert(file_key, entry);
        }
    }

    /// 由内存索引重算 file_count / deleted_count
    fn recount_stats(&mut self) {
        let (live, dead) = self.index.values().fold((0u64, 0u64), |(live, dead), e| {
            if e.deleted {
                (live, dead + 1)
            } else {
                (live + 1, dead)
            }
        });
        self.file_count = live;
        self.deleted_count = dead;
    }

    /// 从磁盘加载 idx，截断撕裂尾，必要时从 dat 重建
    fn load_index(&mut self) -> Result<()> {
        self.index.clear();

        let idx_len = std::fs::metadata(&self.idx_path).map(|m| m.len()).unwrap_or(0);
        // Drop a torn trailing idx record (not a multiple of 28B) after crash mid-write.
        // 崩溃半写：截掉非 28B 对齐的撕裂尾
        let idx_usable = (idx_len / IDX_ENTRY_SIZE as u64) * IDX_ENTRY_SIZE as u64;
        if idx_len > idx_usable {
            warn!(
                volume = self.id,
                idx_len,
                idx_usable,
                "truncating torn .idx tail"
            );
            self.idx.set_len(idx_usable)?;
            self.sync_mode.apply(&self.idx)?;
        }
        if idx_usable > 0 {
            let file = OpenOptions::new().read(true).open(&self.idx_path)?;
            // SAFETY: idx is not truncated while mapped; mapping is read-only.
            let mmap = unsafe { memmap2::Mmap::map(&file) }
                .map_err(|e| Error::Internal(format!("mmap idx: {e}")))?;
            Self::apply_idx_bytes(&mut self.index, &mmap[..idx_usable as usize]);
        }

        // Crash recovery: keep every CRC-complete needle; cut only the torn EOF tail.
        // 只砍 EOF 撕裂尾，保留所有 CRC 完整的 needle
        let good_end = self.scan_complete_dat_end(false)?;
        let dat_len = self.dat.metadata()?.len();
        if dat_len > good_end {
            warn!(
                volume = self.id,
                dat_len,
                good_end,
                torn = dat_len - good_end,
                "truncating .dat to last CRC-complete needle"
            );
            self.dat.set_len(good_end)?;
            self.sync_mode.apply(&self.dat)?;
        }

        if self.index.is_empty() && good_end > 0 {
            // idx 空但 dat 有数据 → 从 dat 重建索引
            let _ = self.scan_complete_dat_end(true)?;
            self.rewrite_index_snapshot()?;
        } else {
            let before = self.index.len();
            // 丢掉越界（指向撕裂区）的索引项
            self.index.retain(|_, e| {
                let total =
                    NEEDLE_HEADER_SIZE as u64 + e.size as u64 + NEEDLE_CHECKSUM_SIZE as u64;
                e.offset.saturating_add(total) <= good_end
            });
            if self.index.len() != before {
                self.rewrite_index_snapshot()?;
            }
        }

        self.recount_stats();
        self.size = good_end;
        self.dat.seek(SeekFrom::End(0))?;
        self.idx.seek(SeekFrom::End(0))?;
        Ok(())
    }

    /// Walk `.dat` from offset 0. Stops at the first incomplete or CRC-bad needle
    /// (append-only tear is always at EOF). Returns the end offset of the last
    /// fully verified needle — **not** a wipe of the whole file.
    /// When `fill_index` is true, rebuilds `self.index` from verified needles.
    /// 从 0 扫描 `.dat`，遇不完整/CRC 坏即停；返回最后完整 needle 末尾偏移
    fn scan_complete_dat_end(&mut self, fill_index: bool) -> Result<u64> {
        let file_len = self.dat.metadata()?.len();
        let min_needle = NEEDLE_HEADER_SIZE as u64 + 8 + NEEDLE_CHECKSUM_SIZE as u64;
        if fill_index {
            self.index.clear();
        }
        let mut offset = 0u64;
        while offset + min_needle <= file_len {
            self.dat.seek(SeekFrom::Start(offset))?;

            let mut header = [0u8; NEEDLE_HEADER_SIZE];
            if self.dat.read_exact(&mut header).is_err() {
                break;
            }
            let cookie = u32::from_le_bytes(header[0..4].try_into().unwrap());
            let size = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
            let total = NEEDLE_HEADER_SIZE as u64 + size as u64 + NEEDLE_CHECKSUM_SIZE as u64;
            if size < 8 || offset + total > file_len {
                break; // body 过短或越出 EOF → 撕裂
            }

            let mut body = vec![0u8; size];
            let mut crc_buf = [0u8; 4];
            if self.dat.read_exact(&mut body).is_err() || self.dat.read_exact(&mut crc_buf).is_err()
            {
                break;
            }

            let file_key = u64::from_le_bytes(body[0..8].try_into().unwrap());
            let crc = u32::from_le_bytes(crc_buf);
            if crc != util::crc32(&body[8..]) {
                break; // CRC 失败：后续视为不可信
            }

            if fill_index {
                self.index.insert(
                    file_key,
                    NeedleIndex {
                        offset,
                        size: size as u32,
                        cookie,
                        deleted: false,
                    },
                );
            }
            offset += total;
        }
        Ok(offset)
    }

    /// 从 `.dat` 全量重建索引并重写 `.idx` 快照
    fn rebuild_index_from_dat(&mut self) -> Result<()> {
        let end = self.scan_complete_dat_end(true)?;
        self.size = end;
        if self.dat.metadata()?.len() > end {
            self.dat.set_len(end)?;
            self.sync_mode.apply(&self.dat)?;
        }
        self.rewrite_index_snapshot()?;
        Ok(())
    }

    /// 暂存一条 idx 变更；达阈值则自动 flush
    fn stage_index_entry(&mut self, file_key: u64, entry: NeedleIndex) -> Result<()> {
        Self::encode_idx_entry(file_key, &entry, &mut self.idx_buf);
        self.index.insert(file_key, entry);
        self.dirty_ops = self.dirty_ops.saturating_add(1);
        let every = self.idx_flush_every;
        if every > 0 && self.dirty_ops >= every {
            self.flush()?;
        }
        Ok(())
    }

    /// Append buffered idx records, flush, and apply [`SyncMode`].
    /// 写出缓冲 idx，flush dat/idx，并按 SyncMode fsync
    pub fn flush(&mut self) -> Result<()> {
        if !self.idx_buf.is_empty() {
            self.idx.seek(SeekFrom::End(0))?;
            self.idx.write_all(&self.idx_buf)?;
            self.idx_buf.clear();
        }
        self.dat.flush()?;
        self.idx.flush()?;
        self.sync_mode.apply(&self.dat)?;
        self.sync_mode.apply(&self.idx)?;
        self.dirty_ops = 0;
        Ok(())
    }

    /// 将当前内存索引整表重写为 `.idx`（截断后写）
    fn rewrite_index_snapshot(&mut self) -> Result<()> {
        self.idx_buf.clear();
        self.dirty_ops = 0;

        let mut buf = Vec::with_capacity(self.index.len() * IDX_ENTRY_SIZE);
        for (file_key, idx) in &self.index {
            Self::encode_idx_entry(*file_key, idx, &mut buf);
        }

        self.idx.set_len(0)?;
        self.idx.seek(SeekFrom::Start(0))?;
        self.idx.write_all(&buf)?;
        self.idx.flush()?;
        self.sync_mode.apply(&self.idx)?;
        self.idx.seek(SeekFrom::End(0))?;
        Ok(())
    }

    /// 追加写入 needle；同 file_key 覆盖时旧条目计为软删。
    /// 返回 `(payload 字节数, md5 hex)`。
    pub fn write_needle(
        &mut self,
        file_key: u64,
        cookie: u32,
        data: &[u8],
        ttl_seconds: u32,
    ) -> Result<(u64, String)> {
        if self.read_only || self.sealed {
            return Err(Error::VolumeReadOnly(self.id));
        }
        if self.vacuuming {
            return Err(Error::Internal(format!(
                "volume {} is vacuuming",
                self.id
            )));
        }
        let need = NEEDLE_HEADER_SIZE as u64 + 8 + data.len() as u64 + NEEDLE_CHECKSUM_SIZE as u64;
        if self.size + need > self.max_size {
            return Err(Error::VolumeFull(self.id));
        }

        let offset = self.dat.seek(SeekFrom::End(0))?;
        let size = (8 + data.len()) as u32; // body = file_key + payload
        let mut header = [0u8; NEEDLE_HEADER_SIZE];
        header[0..4].copy_from_slice(&cookie.to_le_bytes());
        header[4..8].copy_from_slice(&size.to_le_bytes());
        header[8] = 0; // flags
        header[12..16].copy_from_slice(&ttl_seconds.to_le_bytes());
        self.dat.write_all(&header)?;
        self.dat.write_all(&file_key.to_le_bytes())?;
        self.dat.write_all(data)?;
        let crc = util::crc32(data);
        self.dat.write_all(&crc.to_le_bytes())?;

        let entry = NeedleIndex {
            offset,
            size,
            cookie,
            deleted: false,
        };

        if let Some(old) = self.index.get(&file_key) {
            if !old.deleted {
                self.deleted_count += 1; // 覆盖写：旧版本计软删
            }
        } else {
            self.file_count += 1; // 新 key
        }
        self.size = offset + NEEDLE_HEADER_SIZE as u64 + size as u64 + NEEDLE_CHECKSUM_SIZE as u64;
        self.stage_index_entry(file_key, entry)?;

        Ok((data.len() as u64, util::md5_hex(data)))
    }

    /// Positioned read (`pread`) — does not move the file cursor, safe under shared locks.
    /// 定位读；校验 cookie 与 CRC
    pub fn read_needle(&self, file_key: u64, cookie: u32) -> Result<Vec<u8>> {
        let idx = self
            .index
            .get(&file_key)
            .ok_or_else(|| Error::NotFound(format!("needle {file_key}")))?;
        if idx.deleted {
            return Err(Error::NotFound(format!("needle {file_key} deleted")));
        }
        if idx.cookie != cookie {
            return Err(Error::CookieMismatch);
        }

        let size = idx.size as usize;
        let mut buf = vec![0u8; NEEDLE_HEADER_SIZE + size + NEEDLE_CHECKSUM_SIZE];
        read_exact_at(&self.dat, &mut buf, idx.offset)
            .map_err(|e| Error::Internal(format!("pread needle: {e}")))?;

        let on_disk_size = u32::from_le_bytes(buf[4..8].try_into().unwrap()) as usize;
        if on_disk_size != size || size < 8 {
            return Err(Error::Internal("needle size mismatch".into()));
        }

        let body_start = NEEDLE_HEADER_SIZE;
        let body_end = body_start + size;
        let data = &buf[body_start + 8..body_end]; // 跳过内嵌 file_key
        let crc = u32::from_le_bytes(buf[body_end..body_end + NEEDLE_CHECKSUM_SIZE].try_into().unwrap());
        if crc != util::crc32(data) {
            return Err(Error::Internal("needle checksum mismatch".into()));
        }
        Ok(data.to_vec())
    }

    /// 软删：追加 tombstone idx，不立刻回收磁盘空间
    pub fn delete_needle(&mut self, file_key: u64, cookie: u32) -> Result<()> {
        if self.vacuuming {
            return Err(Error::Internal(format!(
                "volume {} is vacuuming",
                self.id
            )));
        }
        let existing = self
            .index
            .get(&file_key)
            .ok_or_else(|| Error::NotFound(format!("needle {file_key}")))?
            .clone();
        if existing.cookie != cookie {
            return Err(Error::CookieMismatch);
        }
        if existing.deleted {
            return Ok(()); // 幂等
        }
        let tomb = NeedleIndex {
            deleted: true,
            ..existing
        };
        self.deleted_count += 1;
        if self.file_count > 0 {
            self.file_count -= 1;
        }
        self.stage_index_entry(file_key, tomb)?;
        Ok(())
    }

    /// One-shot vacuum: compact → commit → cleanup (compatible with legacy callers).
    /// 一次性 vacuum：compact → commit → cleanup
    pub fn compact(&mut self) -> Result<(u64, u64)> {
        let (before, _) = self.vacuum_compact()?;
        match self.vacuum_commit() {
            Ok(after) => {
                let _ = self.vacuum_cleanup();
                Ok((before, after))
            }
            Err(e) => {
                let _ = self.vacuum_cleanup();
                Err(e)
            }
        }
    }

    /// 设置只读标志
    pub fn set_read_only(&mut self, v: bool) {
        self.read_only = v;
    }

    /// 密封：flush 后标记 sealed + read_only
    pub fn seal(&mut self) {
        let _ = self.flush();
        self.sealed = true;
        self.read_only = true;
    }
}

impl Drop for NeedleVolume {
    /// 析构时尽量 flush，降低缓冲丢失风险
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
