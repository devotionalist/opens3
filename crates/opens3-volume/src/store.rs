// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 多卷目录管理：打开/创建 NeedleVolume，并提供共享读 / 独占写锁。

use crate::needle::{NeedleVolume, SyncMode, VolumeIoOpts};
use opens3_common::error::{Error, Result};
use opens3_common::types::DEFAULT_VOLUME_SIZE;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// 磁盘上的多卷集合：按 volume id 持有带 RwLock 的 NeedleVolume。
#[derive(Clone)]
pub struct VolumeStore {
    dir: PathBuf,      // 数据根目录（含 `{id}.dat` / `{id}.idx`）
    max_size: u64,     // 单卷上限（字节）
    io: VolumeIoOpts,  // 打开/创建卷时的 IO 选项
    /// 每卷 `RwLock`：共享读（pread）/ 独占写。
    volumes: Arc<Mutex<HashMap<u32, Arc<RwLock<NeedleVolume>>>>>,
}

impl VolumeStore {
    /// 打开目录；`max_size` 默认 [`DEFAULT_VOLUME_SIZE`]，IO 选项取自环境变量。
    pub fn open(dir: impl AsRef<Path>, max_size: Option<u64>) -> Result<Self> {
        Self::open_with_opts(dir, max_size, VolumeIoOpts::from_env())
    }

    /// 打开目录并指定 IO 选项；扫描已有 `.dat` 装入内存。
    pub fn open_with_opts(
        dir: impl AsRef<Path>,
        max_size: Option<u64>,
        io: VolumeIoOpts,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let max_size = max_size.unwrap_or(DEFAULT_VOLUME_SIZE);
        let store = Self {
            dir,
            max_size,
            io,
            volumes: Arc::new(Mutex::new(HashMap::new())),
        };
        store.load_existing()?;
        Ok(store)
    }

    /// 当前 IO 选项（只读引用）
    pub fn io_opts(&self) -> &VolumeIoOpts {
        &self.io
    }

    /// 扫描目录中已有 `{id}.dat` 并打开对应卷
    fn load_existing(&self) -> Result<()> {
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            let Some(id_str) = name.strip_suffix(".dat") else {
                continue; // 非卷数据文件
            };
            let Ok(id) = id_str.parse::<u32>() else {
                continue;
            };
            let vol = NeedleVolume::open_with_opts(&self.dir, id, self.max_size, &self.io)?;
            self.volumes.lock().insert(id, Arc::new(RwLock::new(vol)));
        }
        Ok(())
    }

    /// 创建卷；已存在则幂等成功。
    pub fn create_volume(&self, id: u32) -> Result<()> {
        let mut vols = self.volumes.lock();
        if vols.contains_key(&id) {
            return Ok(()); // 已在内存中
        }
        let vol = NeedleVolume::create_with_opts(&self.dir, id, self.max_size, &self.io)?;
        vols.insert(id, Arc::new(RwLock::new(vol)));
        Ok(())
    }

    /// 从磁盘重新打开卷并替换内存句柄（导入 / EC 重建后用）。
    pub fn reopen_volume(&self, id: u32) -> Result<()> {
        let vol = NeedleVolume::open_with_opts(&self.dir, id, self.max_size, &self.io)?;
        self.volumes.lock().insert(id, Arc::new(RwLock::new(vol)));
        Ok(())
    }

    /// 按 id 取得卷句柄；不存在则 NotFound。
    pub fn get(&self, id: u32) -> Result<Arc<RwLock<NeedleVolume>>> {
        self.volumes
            .lock()
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("volume {id}")))
    }

    /// 列出已加载卷 id（升序）
    pub fn list_ids(&self) -> Vec<u32> {
        let mut ids: Vec<_> = self.volumes.lock().keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    /// 返回 `(size, file_count, deleted_count, read_only, sealed)`
    pub fn volume_stats(&self, id: u32) -> Result<(u64, u64, u64, bool, bool)> {
        let vol = self.get(id)?;
        let g = vol.read();
        Ok((g.size, g.file_count, g.deleted_count, g.read_only, g.sealed))
    }

    /// 写入 needle；持有写锁。
    pub fn write(
        &self,
        volume_id: u32,
        file_key: u64,
        cookie: u32,
        data: &[u8],
        ttl_seconds: u32,
    ) -> Result<(u64, String)> {
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.write_needle(file_key, cookie, data, ttl_seconds)
    }

    /// 读取 needle；持有读锁（pread 不移动写游标）。
    pub fn read(&self, volume_id: u32, file_key: u64, cookie: u32) -> Result<Vec<u8>> {
        let vol = self.get(volume_id)?;
        let g = vol.read();
        g.read_needle(file_key, cookie)
    }

    /// 软删 needle；持有写锁。
    pub fn delete(&self, volume_id: u32, file_key: u64, cookie: u32) -> Result<()> {
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.delete_needle(file_key, cookie)
    }

    /// 一次性 compact（compact → commit → cleanup）
    pub fn compact(&self, volume_id: u32) -> Result<(u64, u64)> {
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.compact()
    }

    /// 垃圾比例：deleted / (live + deleted)
    pub fn vacuum_check(&self, volume_id: u32) -> Result<f64> {
        let vol = self.get(volume_id)?;
        let ratio = vol.read().vacuum_check();
        Ok(ratio)
    }

    /// 将存活 needle 压到 `.cpd`/`.cpx`，暂不替换正式文件。
    pub fn vacuum_compact(&self, volume_id: u32) -> Result<(u64, u64)> {
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.vacuum_compact()
    }

    /// 提交 vacuum：原子替换 `.dat`/`.idx`。
    pub fn vacuum_commit(&self, volume_id: u32) -> Result<u64> {
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.vacuum_commit()
    }

    /// 清理 vacuum 临时文件 / 中断状态。
    pub fn vacuum_cleanup(&self, volume_id: u32) -> Result<()> {
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.vacuum_cleanup()
    }

    /// 密封卷：flush 后只读。
    pub fn seal(&self, volume_id: u32) -> Result<()> {
        let vol = self.get(volume_id)?;
        vol.write().seal();
        Ok(())
    }

    /// 刷盘 dat/idx（含 SyncMode）
    pub fn flush(&self, volume_id: u32) -> Result<()> {
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.flush()
    }

    /// 运行时改写指定卷的 SyncMode
    pub fn set_sync_mode(&self, volume_id: u32, mode: SyncMode) -> Result<()> {
        let vol = self.get(volume_id)?;
        vol.write().sync_mode = mode;
        Ok(())
    }

    /// 数据根目录
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// 从内存移除并删除 dat/idx 及 vacuum 临时文件。
    pub fn drop_volume_files(&self, id: u32) -> Result<()> {
        self.volumes.lock().remove(&id);
        for ext in ["dat", "idx", "cpd", "cpx", "cpc"] {
            let _ = std::fs::remove_file(self.dir.join(format!("{id}.{ext}")));
        }
        Ok(())
    }

    /// 仅从内存摘掉句柄（磁盘文件保留，例如 EC 后等 reopen）
    pub fn invalidate(&self, id: u32) {
        self.volumes.lock().remove(&id);
    }

    /// 先本地读；失败则用 EC 分片重建 `.dat` 后 reopen 再读。
    pub fn read_with_ec_fallback(
        &self,
        volume_id: u32,
        file_key: u64,
        cookie: u32,
        data_shards: usize,
        parity_shards: usize,
    ) -> Result<Vec<u8>> {
        match self.read(volume_id, file_key, cookie) {
            Ok(d) => Ok(d),
            Err(e) => {
                let ec_dir = self.dir.join("ec");
                let total = data_shards + parity_shards;
                let shards = crate::ec::EcEncoder::find_shards(&ec_dir, volume_id, total);
                // 存活分片不足 data_shards 则无法重建
                if shards.iter().filter(|s| s.is_some()).count() < data_shards {
                    return Err(e);
                }
                let enc = crate::ec::EcEncoder::new(data_shards, parity_shards)?;
                let bytes = enc.reconstruct_bytes(&shards)?;
                let dat = self.dir.join(format!("{volume_id}.dat"));
                std::fs::write(&dat, &bytes)?;
                let idx = self.dir.join(format!("{volume_id}.idx"));
                // EC 只编码 .dat；idx 必须仍在磁盘上
                if !idx.exists() {
                    return Err(Error::Internal(format!(
                        "ec reconstruct ok but missing idx for volume {volume_id}"
                    )));
                }
                self.reopen_volume(volume_id)?;
                self.read(volume_id, file_key, cookie)
            }
        }
    }

    /// 可用磁盘字节（占位；后续可接真实 statvfs）
    pub fn free_disk_bytes(&self) -> u64 {
        100 * 1024 * 1024 * 1024
    }
}
