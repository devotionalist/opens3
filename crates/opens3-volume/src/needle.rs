// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
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
pub const IDX_ENTRY_SIZE: usize = 8 + 8 + 4 + 4 + 1 + 3;

/// Marker that [`NeedleVolume::vacuum_compact`] completed and awaits commit.
#[derive(Debug, Clone)]
struct PendingVacuum {
    #[allow(dead_code)]
    before_size: u64,
    #[allow(dead_code)]
    new_size: u64,
    #[allow(dead_code)]
    new_index: HashMap<u64, NeedleIndex>,
}

/// Default: flush idx/dat every N mutations (1 = every op; higher = throughput).
pub const DEFAULT_IDX_FLUSH_EVERY: u32 = 64;

/// Durability vs throughput when flushing dat/idx.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum SyncMode {
    /// `flush` only — fastest; risk losing the last batch on crash.
    None,
    /// `sync_data` — default: data durable, metadata may lag slightly.
    #[default]
    Data,
    /// `sync_all` — strongest durability, slowest.
    All,
}

impl SyncMode {  // 同步模式
    pub fn parse(s: &str) -> Option<Self> { // 解析同步模式
        match s.trim().to_ascii_lowercase().as_str() {
            "none" | "off" | "0" | "false" => Some(Self::None),
            "data" | "datasync" | "sync_data" => Some(Self::Data),
            "all" | "full" | "sync_all" => Some(Self::All),
            _ => None,
        }
    }

    fn apply(self, file: &File) -> std::io::Result<()> { // 应用同步模式
        match self {
            Self::None => Ok(()),
            Self::Data => file.sync_data(),
            Self::All => file.sync_all(),
        }
    }
}

/// Tunables applied when opening/creating a volume.
#[derive(Debug, Clone)]
pub struct VolumeIoOpts { // 卷IO选项
    pub idx_flush_every: u32, // 索引刷新频率
    pub sync_mode: SyncMode, // 同步模式
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

fn open_rw(path: &Path, create: bool) -> std::io::Result<File> { // 打开读写文件
    OpenOptions::new()
        .create(create)
        .read(true)
        .write(true)
        .truncate(false)
        .open(path)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NeedleIndex { // 针索引
    pub offset: u64,
    pub size: u32, // 大小
    pub cookie: u32, // 饼干
    pub deleted: bool, // 删除
}

#[derive(Debug)]
pub struct NeedleVolume { // 针卷
    pub id: u32,
    dat_path: PathBuf, // 数据路径
    idx_path: PathBuf, // 索引路径
    dat: File, // 数据文件
    idx: File, // 索引文件
    /// Pending idx records not yet written to disk.
    idx_buf: Vec<u8>,
    dirty_ops: u32, // 脏操作   
    /// Flush every N write/delete ops. `0` = only explicit flush / compact / drop.
    pub idx_flush_every: u32,
    pub sync_mode: SyncMode, // 同步模式
    pub index: HashMap<u64, NeedleIndex>, // 索引
    pub size: u64, // 大小
    pub file_count: u64, // 文件数量
    pub deleted_count: u64, // 删除数量
    pub read_only: bool, // 只读
    pub sealed: bool, // 密封
    pub max_size: u64, // 最大大小
    /// True between vacuum_compact and vacuum_commit/cleanup; blocks writes/deletes.
    pub vacuuming: bool,
    pending_vacuum: Option<PendingVacuum>,
}

fn read_exact_at(file: &File, buf: &mut [u8], offset: u64) -> std::io::Result<()> { // 读取精确位置 
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
    pub fn create(dir: &Path, id: u32, max_size: u64) -> Result<Self> { // 创建针卷
        Self::create_with_opts(dir, id, max_size, &VolumeIoOpts::default())
    }

    pub fn create_with_opts(dir: &Path, id: u32, max_size: u64, opts: &VolumeIoOpts) -> Result<Self> { // 创建针卷  
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

    fn cpd_path(&self) -> PathBuf {
        self.dat_path.with_extension("cpd")
    }

    fn cpx_path(&self) -> PathBuf {
        self.idx_path.with_extension("cpx")
    }

    fn cpc_path(&self) -> PathBuf {
        self.dat_path.with_extension("cpc")
    }

    fn remove_vacuum_temps(cpd: &Path, cpx: &Path, cpc: &Path) {
        let _ = std::fs::remove_file(cpd);
        let _ = std::fs::remove_file(cpx);
        let _ = std::fs::remove_file(cpc);
    }

    /// Crash recovery: finish commit if `.cpc` present; else drop orphan temps.
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

    fn apply_vacuum_swap(&mut self, cpd: &Path, cpx: &Path, cpc: &Path) -> Result<()> {
        // Ensure handles do not keep the old inode open across rename.
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
    pub fn vacuum_check(&self) -> f64 {
        let live = self.file_count;
        let dead = self.deleted_count;
        let total = live.saturating_add(dead);
        if total == 0 {
            0.0
        } else {
            dead as f64 / total as f64
        }
    }

    /// Copy live needles to `.cpd` / `.cpx`. Does not replace `.dat`/`.idx` yet.
    /// Blocks further writes until [`vacuum_commit`] or [`vacuum_cleanup`].
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
                continue;
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

    pub fn open(dir: &Path, id: u32, max_size: u64) -> Result<Self> {
        Self::create(dir, id, max_size)
    }

    pub fn open_with_opts(dir: &Path, id: u32, max_size: u64, opts: &VolumeIoOpts) -> Result<Self> {
        Self::create_with_opts(dir, id, max_size, opts)
    }

    fn encode_idx_entry(file_key: u64, idx: &NeedleIndex, out: &mut Vec<u8>) { // 编码索引条目
        out.extend_from_slice(&file_key.to_le_bytes());
        out.extend_from_slice(&idx.offset.to_le_bytes());
        out.extend_from_slice(&idx.size.to_le_bytes());
        out.extend_from_slice(&idx.cookie.to_le_bytes());
        out.push(u8::from(idx.deleted));
        out.extend_from_slice(&[0, 0, 0]);
    }

    fn decode_idx_entry(rec: &[u8]) -> (u64, NeedleIndex) { // 解码索引条目
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

    fn apply_idx_bytes(index: &mut HashMap<u64, NeedleIndex>, data: &[u8]) {
        for rec in data.chunks_exact(IDX_ENTRY_SIZE) {
            let (file_key, entry) = Self::decode_idx_entry(rec);
            index.insert(file_key, entry);
        }
    }

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

    fn load_index(&mut self) -> Result<()> {
        self.index.clear();

        let idx_len = std::fs::metadata(&self.idx_path).map(|m| m.len()).unwrap_or(0);
        // Drop a torn trailing idx record (not a multiple of 28B) after crash mid-write.
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
            let _ = self.scan_complete_dat_end(true)?;
            self.rewrite_index_snapshot()?;
        } else {
            let before = self.index.len();
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
                break;
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
                break;
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

    // 阶段索引条目
    fn stage_index_entry(&mut self, file_key: u64, entry: NeedleIndex) -> Result<()> { // 阶段索引条目
        Self::encode_idx_entry(file_key, &entry, &mut self.idx_buf); // 编码索引条目
        self.index.insert(file_key, entry);
        self.dirty_ops = self.dirty_ops.saturating_add(1); // 脏操作增加    
        let every = self.idx_flush_every; // 索引刷新频率
        if every > 0 && self.dirty_ops >= every {
            self.flush()?; // 刷新  
        }
        Ok(())
    }

    /// Append buffered idx records, flush, and apply [`SyncMode`].
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

    // 写入针
    pub fn write_needle(  //
        &mut self, // 针卷
        file_key: u64, // 文件键
        cookie: u32, // 饼干
        data: &[u8], // 数据
        ttl_seconds: u32, // 过期时间
    ) -> Result<(u64, String)> {
        if self.read_only || self.sealed { // 只读或密封
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
        let size = (8 + data.len()) as u32;
        let mut header = [0u8; NEEDLE_HEADER_SIZE];
        header[0..4].copy_from_slice(&cookie.to_le_bytes());
        header[4..8].copy_from_slice(&size.to_le_bytes());
        header[8] = 0;
        header[12..16].copy_from_slice(&ttl_seconds.to_le_bytes());
        self.dat.write_all(&header)?;
        self.dat.write_all(&file_key.to_le_bytes())?; // 写入文件键
        self.dat.write_all(data)?;
        let crc = util::crc32(data);
        self.dat.write_all(&crc.to_le_bytes())?; // 写入CRC

        let entry = NeedleIndex { // 创建索引条目
            offset,
            size,
            cookie,
            deleted: false,
        };

        if let Some(old) = self.index.get(&file_key) {// 如果文件键已存在
            if !old.deleted { // 如果文件键未删除
                self.deleted_count += 1; // 删除数量增加
            }
        } else { // 如果文件键不存在
            self.file_count += 1; // 文件数量增加
        }
        self.size = offset + NEEDLE_HEADER_SIZE as u64 + size as u64 + NEEDLE_CHECKSUM_SIZE as u64; // 大小
        self.stage_index_entry(file_key, entry)?; // 阶段索引条目

        Ok((data.len() as u64, util::md5_hex(data))) // 返回数据长度和MD5
    }

    /// Positioned read (`pread`) — does not move the file cursor, safe under shared locks.
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
        let data = &buf[body_start + 8..body_end];
        let crc = u32::from_le_bytes(buf[body_end..body_end + NEEDLE_CHECKSUM_SIZE].try_into().unwrap());
        if crc != util::crc32(data) {
            return Err(Error::Internal("needle checksum mismatch".into()));
        }
        Ok(data.to_vec())
    }

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
            return Ok(());
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

    pub fn set_read_only(&mut self, v: bool) {
        self.read_only = v;
    }

    pub fn seal(&mut self) {
        let _ = self.flush();
        self.sealed = true;
        self.read_only = true;
    }
}

impl Drop for NeedleVolume {
    fn drop(&mut self) {
        let _ = self.flush();
    }
}
