// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! heal/balance 推送用的平均速率限速。

use bytes::Bytes;
use futures::stream::{self, Stream};
use std::io;
use std::time::{Duration, Instant};

/// 流式推送的分片大小（字节）
const CHUNK: usize = 64 * 1024;

/// 按已发送字节与起始时间休眠，使平均速率不超过 `max_bps`。
pub async fn pace(started: Instant, bytes_sent: u64, max_bps: u64) {
    if max_bps == 0 || bytes_sent == 0 {
        return; // 0 = 不限速
    }
    let elapsed = started.elapsed().as_secs_f64();
    let need = bytes_sent as f64 / max_bps as f64; // 理想耗时（秒）
    if need > elapsed {
        tokio::time::sleep(Duration::from_secs_f64(need - elapsed)).await;
    }
}

/// unfold 流状态：剩余数据与累计发送量
struct PaceState {
    data: Vec<u8>,   // 待发送缓冲
    offset: usize,   // 下一分片起点
    sent: u64,       // 已发送字节累计
    start: Instant,  // 限速计时起点
    max_bps: u64,    // 目标平均速率（字节/秒）；0 不限
}

/// 将 `data` 按 CHUNK 切流，休眠使平均发送速率 ≤ `max_bps`（`0` = 不延迟）。
pub fn rate_limited_byte_stream(
    data: Vec<u8>,
    max_bps: u64,
) -> impl Stream<Item = Result<Bytes, io::Error>> + Send {
    stream::unfold(
        PaceState {
            data,
            offset: 0,
            sent: 0,
            start: Instant::now(),
            max_bps,
        },
        |mut st| async move {
            if st.offset >= st.data.len() {
                return None; // 发完
            }
            let end = (st.offset + CHUNK).min(st.data.len());
            let chunk = Bytes::copy_from_slice(&st.data[st.offset..end]);
            st.offset = end;
            st.sent += chunk.len() as u64;
            pace(st.start, st.sent, st.max_bps).await;
            Some((Ok(chunk), st))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;

    /// max_bps=0 时应几乎无延迟跑完
    #[tokio::test]
    async fn unlimited_finishes_quickly() {
        let data = vec![1u8; 200_000];
        let t0 = Instant::now();
        let mut s = Box::pin(rate_limited_byte_stream(data, 0));
        let mut n = 0usize;
        while let Some(c) = s.next().await {
            n += c.unwrap().len();
        }
        assert_eq!(n, 200_000);
        assert!(t0.elapsed() < Duration::from_millis(200));
    }

    /// 限速时应接近理想平均速率
    #[tokio::test]
    async fn limited_respects_average_rate() {
        // 64 KiB at 32 KiB/s → about 2s
        let data = vec![7u8; 64 * 1024];
        let t0 = Instant::now();
        let mut s = Box::pin(rate_limited_byte_stream(data, 32 * 1024));
        while s.next().await.is_some() {}
        let elapsed = t0.elapsed();
        assert!(
            elapsed >= Duration::from_millis(1800),
            "elapsed={elapsed:?}"
        );
        assert!(
            elapsed < Duration::from_millis(3500),
            "elapsed={elapsed:?}"
        );
    }
}
