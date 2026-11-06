// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Average-rate throttle for heal/balance volume push.

use bytes::Bytes;
use futures::stream::{self, Stream};
use std::io;
use std::time::{Duration, Instant};

const CHUNK: usize = 64 * 1024;

/// Sleep so that `bytes_sent` over `started` stays at or below `max_bps`.
pub async fn pace(started: Instant, bytes_sent: u64, max_bps: u64) {
    if max_bps == 0 || bytes_sent == 0 {
        return;
    }
    let elapsed = started.elapsed().as_secs_f64();
    let need = bytes_sent as f64 / max_bps as f64;
    if need > elapsed {
        tokio::time::sleep(Duration::from_secs_f64(need - elapsed)).await;
    }
}

struct PaceState {
    data: Vec<u8>,
    offset: usize,
    sent: u64,
    start: Instant,
    max_bps: u64,
}

/// Stream `data` in chunks, sleeping so average send rate ≤ `max_bps` (`0` = no delay).
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
                return None;
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
