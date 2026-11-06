// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Read failover: first volume URL fails, BlobClient succeeds from second.

use axum::body::Bytes;
use axum::extract::Path;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use opens3_filer::client::BlobClient;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[tokio::test]
async fn read_blob_falls_back_to_second_replica() {
    let hits = Arc::new(AtomicUsize::new(0));
    let hits_a = hits.clone();
    let bad = Router::new().route(
        "/{*fid}",
        get(move || {
            let hits_a = hits_a.clone();
            async move {
                hits_a.fetch_add(1, Ordering::SeqCst);
                (StatusCode::INTERNAL_SERVER_ERROR, "boom").into_response()
            }
        }),
    );
    let good_data = b"replica-ok".to_vec();
    let good = Router::new().route(
        "/{*fid}",
        get(move |Path(_fid): Path<String>| {
            let good_data = good_data.clone();
            async move { (StatusCode::OK, Bytes::from(good_data)).into_response() }
        }),
    );

    let la = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_a: SocketAddr = la.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(la, bad).await.ok();
    });
    let lb = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_b: SocketAddr = lb.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(lb, good).await.ok();
    });

    // Tiny stand-in master that returns both locations for lookup
    let master = Router::new().route(
        "/dir/lookup/{vid}",
        get(move |Path(_vid): Path<u32>| {
            let a = format!("http://{addr_a}");
            let b = format!("http://{addr_b}");
            async move {
                axum::Json(serde_json::json!({
                    "locations": [
                        {"url": a, "publicUrl": a},
                        {"url": b, "publicUrl": b}
                    ]
                }))
            }
        }),
    );
    let lm = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr_m = lm.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(lm, master).await.ok();
    });

    let client = BlobClient::new(format!("http://{addr_m}"), "000");
    // fid volume_id=1 — lookup uses volume id from FileId
    let data = client
        .read_blob("1,0000000000000001deadbeef")
        .await
        .expect("failover read");
    assert_eq!(data, b"replica-ok");
    assert!(hits.load(Ordering::SeqCst) >= 1, "primary should have been tried");
}
