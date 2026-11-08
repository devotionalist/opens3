// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use crate::needle::{NeedleVolume, SyncMode, VolumeIoOpts};
use opens3_common::error::{Error, Result};
use opens3_common::types::DEFAULT_VOLUME_SIZE;
use parking_lot::{Mutex, RwLock};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
pub struct VolumeStore {// 卷存储
    dir: PathBuf, // 目录
    max_size: u64, // 最大大小
    io: VolumeIoOpts, // IO选项
    /// Per-volume `RwLock`: shared reads (pread) / exclusive writes.
    volumes: Arc<Mutex<HashMap<u32, Arc<RwLock<NeedleVolume>>>>>, // 卷
}

impl VolumeStore {
    pub fn open(dir: impl AsRef<Path>, max_size: Option<u64>) -> Result<Self> { // 打开卷存储
        Self::open_with_opts(dir, max_size, VolumeIoOpts::from_env())
    }

    pub fn open_with_opts(// 打开卷存储
        dir: impl AsRef<Path>, // 目录
        max_size: Option<u64>, // 最大大小
        io: VolumeIoOpts, // IO选项
    ) -> Result<Self> { // 打开卷存储
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

    pub fn io_opts(&self) -> &VolumeIoOpts {
        &self.io
    }

    fn load_existing(&self) -> Result<()> { // 加载现有卷
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?; // 读取目录
            let name = entry.file_name(); // 文件名
            let name = name.to_string_lossy(); // 转换为字符串
            let Some(id_str) = name.strip_suffix(".dat") else {
                continue; // 跳过
            };
            let Ok(id) = id_str.parse::<u32>() else {
                continue;
            };
            let vol = NeedleVolume::open_with_opts(&self.dir, id, self.max_size, &self.io)?;
            self.volumes.lock().insert(id, Arc::new(RwLock::new(vol)));
        }
        Ok(())
    }

    pub fn create_volume(&self, id: u32) -> Result<()> { // 创建卷
        let mut vols = self.volumes.lock();
        if vols.contains_key(&id) {
            return Ok(()); // 如果卷已存在，则返回
        }
        let vol = NeedleVolume::create_with_opts(&self.dir, id, self.max_size, &self.io)?;
        vols.insert(id, Arc::new(RwLock::new(vol)));
        Ok(())
    }

    pub fn reopen_volume(&self, id: u32) -> Result<()> { // 重新打开卷
        let vol = NeedleVolume::open_with_opts(&self.dir, id, self.max_size, &self.io)?;
        self.volumes.lock().insert(id, Arc::new(RwLock::new(vol)));
        Ok(())
    }

    pub fn get(&self, id: u32) -> Result<Arc<RwLock<NeedleVolume>>> { // 获取卷
        self.volumes
            .lock()
            .get(&id)
            .cloned()
            .ok_or_else(|| Error::NotFound(format!("volume {id}")))
    }

    pub fn list_ids(&self) -> Vec<u32> { // 列出卷ID
        let mut ids: Vec<_> = self.volumes.lock().keys().copied().collect();
        ids.sort_unstable();
        ids
    }

    // 大小，文件数量，删除数量，只读，密封
    pub fn volume_stats(&self, id: u32) -> Result<(u64, u64, u64, bool, bool)> { // 卷统计
        let vol = self.get(id)?;
        let g = vol.read();
        Ok((g.size, g.file_count, g.deleted_count, g.read_only, g.sealed))
    }

    // 写入数据
    pub fn write(&self, volume_id: u32, file_key: u64, cookie: u32, data: &[u8], ttl_seconds: u32) -> Result<(u64, String)> { // 写入数据
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.write_needle(file_key, cookie, data, ttl_seconds)
    }

    pub fn read(&self, volume_id: u32, file_key: u64, cookie: u32) -> Result<Vec<u8>> {
        let vol = self.get(volume_id)?;
        let g = vol.read();
        g.read_needle(file_key, cookie)
    }

    pub fn delete(&self, volume_id: u32, file_key: u64, cookie: u32) -> Result<()> {
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.delete_needle(file_key, cookie)
    }

    pub fn compact(&self, volume_id: u32) -> Result<(u64, u64)> {
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.compact()
    }

    pub fn vacuum_check(&self, volume_id: u32) -> Result<f64> {
        let vol = self.get(volume_id)?;
        let ratio = vol.read().vacuum_check();
        Ok(ratio)
    }

    pub fn vacuum_compact(&self, volume_id: u32) -> Result<(u64, u64)> {
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.vacuum_compact()
    }

    pub fn vacuum_commit(&self, volume_id: u32) -> Result<u64> {
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.vacuum_commit()
    }

    pub fn vacuum_cleanup(&self, volume_id: u32) -> Result<()> {
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.vacuum_cleanup()
    }

    pub fn seal(&self, volume_id: u32) -> Result<()> {
        let vol = self.get(volume_id)?;
        vol.write().seal();
        Ok(())
    }

    pub fn flush(&self, volume_id: u32) -> Result<()> {
        let vol = self.get(volume_id)?;
        let mut g = vol.write();
        g.flush()
    }

    pub fn set_sync_mode(&self, volume_id: u32, mode: SyncMode) -> Result<()> {
        let vol = self.get(volume_id)?;
        vol.write().sync_mode = mode;
        Ok(())
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn drop_volume_files(&self, id: u32) -> Result<()> {
        self.volumes.lock().remove(&id);
        for ext in ["dat", "idx", "cpd", "cpx", "cpc"] {
            let _ = std::fs::remove_file(self.dir.join(format!("{id}.{ext}")));
        }
        Ok(())
    }

    pub fn invalidate(&self, id: u32) {
        self.volumes.lock().remove(&id);
    }

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
                if shards.iter().filter(|s| s.is_some()).count() < data_shards {
                    return Err(e);
                }
                let enc = crate::ec::EcEncoder::new(data_shards, parity_shards)?;
                let bytes = enc.reconstruct_bytes(&shards)?;
                let dat = self.dir.join(format!("{volume_id}.dat"));
                std::fs::write(&dat, &bytes)?;
                let idx = self.dir.join(format!("{volume_id}.idx"));
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

    pub fn free_disk_bytes(&self) -> u64 {
        100 * 1024 * 1024 * 1024
    }
}
