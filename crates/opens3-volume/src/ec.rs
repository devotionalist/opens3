// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use opens3_common::error::{Error, Result};
use reed_solomon_erasure::galois_8::ReedSolomon;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Volume-level EC: split sealed volume dat into data+parity shard files.
pub struct EcEncoder {
    pub data_shards: usize,
    pub parity_shards: usize,
}

impl EcEncoder {
    pub fn new(data_shards: usize, parity_shards: usize) -> Result<Self> {
        if data_shards == 0 || parity_shards == 0 {
            return Err(Error::InvalidArgument("ec shards must be > 0".into()));
        }
        Ok(Self {
            data_shards,
            parity_shards,
        })
    }

    pub fn encode_file(&self, src: &Path, out_dir: &Path, volume_id: u32) -> Result<Vec<PathBuf>> {
        fs::create_dir_all(out_dir)?;
        let mut f = File::open(src)?;
        let mut data = Vec::new();
        f.read_to_end(&mut data)?;

        let shard_size = (data.len() + self.data_shards - 1) / self.data_shards;
        let mut shards: Vec<Vec<u8>> = Vec::with_capacity(self.data_shards + self.parity_shards);
        for i in 0..self.data_shards {
            let start = i * shard_size;
            let end = ((i + 1) * shard_size).min(data.len());
            let mut shard = vec![0u8; shard_size];
            if start < data.len() {
                shard[..end - start].copy_from_slice(&data[start..end]);
            }
            shards.push(shard);
        }
        for _ in 0..self.parity_shards {
            shards.push(vec![0u8; shard_size]);
        }

        let r = ReedSolomon::new(self.data_shards, self.parity_shards)
            .map_err(|e| Error::Internal(format!("rs: {e}")))?;
        r.encode(&mut shards)
            .map_err(|e| Error::Internal(format!("encode: {e}")))?;

        let mut paths = Vec::new();
        for (i, shard) in shards.iter().enumerate() {
            let path = out_dir.join(format!("{volume_id}.ec.{i}"));
            let mut out = File::create(&path)?;
            // header: original_len u64 + shard_size u64
            out.write_all(&(data.len() as u64).to_le_bytes())?;
            out.write_all(&(shard_size as u64).to_le_bytes())?;
            out.write_all(shard)?;
            paths.push(path);
        }
        Ok(paths)
    }

    pub fn reconstruct_bytes(&self, shard_paths: &[Option<PathBuf>]) -> Result<Vec<u8>> {
        let r = ReedSolomon::new(self.data_shards, self.parity_shards)
            .map_err(|e| Error::Internal(format!("rs: {e}")))?;

        let mut original_len = 0u64;
        let mut shard_size = 0usize;
        let mut shards: Vec<Option<Vec<u8>>> = vec![None; self.data_shards + self.parity_shards];

        for (i, path) in shard_paths.iter().enumerate() {
            if let Some(p) = path {
                let mut f = File::open(p)?;
                let mut header = [0u8; 16];
                f.read_exact(&mut header)?;
                original_len = u64::from_le_bytes(header[0..8].try_into().unwrap());
                shard_size = u64::from_le_bytes(header[8..16].try_into().unwrap()) as usize;
                let mut buf = Vec::new();
                f.read_to_end(&mut buf)?;
                if buf.len() < shard_size {
                    buf.resize(shard_size, 0);
                } else {
                    buf.truncate(shard_size);
                }
                shards[i] = Some(buf);
            }
        }

        r.reconstruct(&mut shards)
            .map_err(|e| Error::Internal(format!("reconstruct: {e}")))?;

        let mut data = Vec::with_capacity(original_len as usize);
        for i in 0..self.data_shards {
            let shard = shards[i]
                .as_ref()
                .ok_or_else(|| Error::Internal("missing data shard after reconstruct".into()))?;
            data.extend_from_slice(shard);
        }
        data.truncate(original_len as usize);
        Ok(data)
    }

    pub fn reconstruct(&self, shard_paths: &[Option<PathBuf>], out: &Path) -> Result<()> {
        let data = self.reconstruct_bytes(shard_paths)?;
        fs::write(out, data)?;
        Ok(())
    }

    /// Discover local EC shards for a volume under `ec_dir`.
    pub fn find_shards(ec_dir: &Path, volume_id: u32, total: usize) -> Vec<Option<PathBuf>> {
        let mut out = Vec::with_capacity(total);
        for i in 0..total {
            let p = ec_dir.join(format!("{volume_id}.ec.{i}"));
            out.push(if p.exists() { Some(p) } else { None });
        }
        out
    }
}
