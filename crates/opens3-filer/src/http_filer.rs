// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! Filer path HTTP / WebDAV / store routes.

use crate::server::AppState;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{header, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{any, get, post};
use axum::{Json, Router};
use opens3_common::types::EntryMeta;
use opens3_common::util::join_path;
use serde::Deserialize;
use std::time::Instant;

/// Filer HTTP / WebDAV / store routes (no `/health` — owned by [`Gateway::serve`]).
pub fn http_router() -> Router<AppState> {
    Router::new()
        .route("/status", get(status))
        .route("/webhooks", post(add_webhook))
        .route(
            "/entries{*path}",
            get(get_entry).put(put_entry).delete(delete_entry),
        )
        .route("/list{*path}", get(list_dir))
        .route("/admin/replicate", post(replicate_dir))
        .route("/admin/replicate/status/{job_id}", get(replicate_status))
        .route(
            "/store/entry{*path}",
            get(store_get_entry)
                .put(store_put_entry)
                .delete(store_delete_entry),
        )
        .route("/store/list{*path}", get(store_list_dir))
        .route("/store/prefix", get(store_list_prefix))
        .route(
            "/store/kv/{ns}/{key}",
            get(store_kv_get).put(store_kv_put).delete(store_kv_delete),
        )
        .route("/store/kv/{ns}", get(store_kv_list))
        .route("/dav/{*path}", any(webdav))
        .route("/dav", any(webdav_root))
}

fn normalize_path(path: &str) -> String {
    let p = if path.is_empty() || path == "/" {
        "/".to_string()
    } else if path.starts_with('/') {
        path.trim_end_matches('/').to_string()
    } else {
        join_path("/", path)
    };
    if p.is_empty() {
        "/".into()
    } else {
        p
    }
}

async fn status(State(st): State<AppState>) -> impl IntoResponse {
    Json(serde_json::json!({
        "clerk": st.server.blob.clerk,
        "port": st.server.port,
        "encryption": st.server.encrypt_key.is_some(),
    }))
}

#[derive(Deserialize)]
struct WebhookBody {
    url: String,
}

async fn add_webhook(State(st): State<AppState>, Json(body): Json<WebhookBody>) -> impl IntoResponse {
    st.server.events.add_webhook(body.url);
    Json(serde_json::json!({"ok": true}))
}

#[derive(Deserialize)]
struct PutQuery {
    ttl: Option<u64>,
}

async fn put_entry(
    State(st): State<AppState>,
    Path(path): Path<String>,
    Query(q): Query<PutQuery>,
    body: Bytes,
) -> impl IntoResponse {
    st.server.filer_metrics.in_flight.inc();
    let out = match st.server.put_bytes(&path, body, q.ttl.unwrap_or(0)).await {
        Ok(e) => Json(e).into_response(),
        Err(e) => (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
            .into_response(),
    };
    st.server.filer_metrics.in_flight.dec();
    out
}

async fn get_entry(State(st): State<AppState>, Path(path): Path<String>) -> impl IntoResponse {
    st.server.filer_metrics.in_flight.inc();
    let out = match st.server.get_bytes(&path).await {
        Ok((entry, data)) => (
            StatusCode::OK,
            [
                (header::ETAG, format!("\"{}\"", entry.etag)),
                (header::CONTENT_TYPE, entry.mime.clone()),
                (
                    header::HeaderName::from_static("x-opens3-size"),
                    entry.size.to_string(),
                ),
            ],
            data,
        )
            .into_response(),
        Err(e) => (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
            .into_response(),
    };
    st.server.filer_metrics.in_flight.dec();
    out
}

async fn delete_entry(State(st): State<AppState>, Path(path): Path<String>) -> impl IntoResponse {
    st.server.filer_metrics.in_flight.inc();
    let out = match st.server.delete_path(&path).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
            .into_response(),
    };
    st.server.filer_metrics.in_flight.dec();
    out
}

async fn list_dir(State(st): State<AppState>, Path(path): Path<String>) -> impl IntoResponse {
    let t0 = Instant::now();
    let path = normalize_path(&path);
    st.server.filer_metrics.in_flight.inc();
    let out = match st.server.store.list_directory(&path).await {
        Ok(entries) => {
            st.server
                .filer_metrics
                .ops
                .with_label_values(&["list", "ok"])
                .inc();
            Json(serde_json::json!({"path": path, "entries": entries})).into_response()
        }
        Err(e) => {
            st.server
                .filer_metrics
                .ops
                .with_label_values(&["list", "error"])
                .inc();
            (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
        }
    };
    st.server
        .filer_metrics
        .request_duration
        .with_label_values(&["list"])
        .observe(t0.elapsed().as_secs_f64());
    st.server.filer_metrics.in_flight.dec();
    out
}

#[derive(Deserialize)]
struct ReplicateBody {
    prefix: String,
    target_filer: String,
}

async fn replicate_dir(State(st): State<AppState>, Json(body): Json<ReplicateBody>) -> impl IntoResponse {
    let job_id = uuid::Uuid::new_v4().to_string();
    let job = serde_json::json!({
        "id": job_id,
        "prefix": body.prefix,
        "target": body.target_filer,
        "status": "running",
        "ok": 0,
        "fail": 0,
        "retries": 0,
    });
    let _ = st
        .server
        .store
        .kv_put("xrepl", &job_id, job.to_string().as_bytes())
        .await;

    let filer = st.server.clone();
    let prefix = body.prefix.clone();
    let target = body.target_filer.clone();
    let jid = job_id.clone();
    tokio::spawn(async move {
        let mut ok = 0usize;
        let mut fail = 0usize;
        let mut retries = 0usize;
        let listed = filer
            .store
            .list_by_prefix(&prefix, 10_000)
            .await
            .unwrap_or_default();
        let client = reqwest::Client::new();
        for (path, _) in listed {
            let mut attempt = 0;
            loop {
                attempt += 1;
                match filer.get_bytes(&path).await {
                    Ok((_, data)) => {
                        let url = format!("{}/entries{}", target.trim_end_matches('/'), path);
                        match client.put(&url).body(data).send().await {
                            Ok(r) if r.status().is_success() => {
                                ok += 1;
                                break;
                            }
                            _ if attempt < 3 => {
                                retries += 1;
                                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                            }
                            _ => {
                                fail += 1;
                                break;
                            }
                        }
                    }
                    Err(_) if attempt < 3 => {
                        retries += 1;
                        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
                    }
                    Err(_) => {
                        fail += 1;
                        break;
                    }
                }
            }
        }
        let done = serde_json::json!({
            "id": jid,
            "prefix": prefix,
            "target": target,
            "status": "done",
            "ok": ok,
            "fail": fail,
            "retries": retries,
        });
        let _ = filer
            .store
            .kv_put("xrepl", &jid, done.to_string().as_bytes())
            .await;
    });

    Json(serde_json::json!({"job_id": job_id, "status": "running"})).into_response()
}

async fn replicate_status(
    State(st): State<AppState>,
    Path(job_id): Path<String>,
) -> impl IntoResponse {
    match st.server.store.kv_get("xrepl", &job_id).await {
        Ok(Some(raw)) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            raw,
        )
            .into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn webdav_root(
    State(st): State<AppState>,
    method: axum::http::Method,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    webdav_inner(&st, "/", method, &headers, body).await
}

async fn webdav(
    State(st): State<AppState>,
    Path(path): Path<String>,
    method: axum::http::Method,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let path = if path.starts_with('/') {
        path
    } else {
        format!("/{path}")
    };
    webdav_inner(&st, &path, method, &headers, body).await
}

fn webdav_dest_path(headers: &axum::http::HeaderMap) -> Option<String> {
    let raw = headers.get("destination")?.to_str().ok()?;
    // Absolute URL (`http://host/dav/path`) or path; strip optional /dav prefix.
    let after_scheme = raw
        .strip_prefix("http://")
        .or_else(|| raw.strip_prefix("https://"))
        .and_then(|rest| rest.find('/').map(|i| &rest[i..]))
        .unwrap_or(raw);
    let path = after_scheme
        .strip_prefix("/dav")
        .unwrap_or(after_scheme);
    let path = if path.is_empty() { "/" } else { path };
    Some(normalize_path(path))
}

fn webdav_prop_response(
    href: &str,
    is_dir: bool,
    size: u64,
    mtime: chrono::DateTime<chrono::Utc>,
) -> String {
    let rt = if is_dir {
        "<D:resourcetype><D:collection/></D:resourcetype>"
    } else {
        "<D:resourcetype/>"
    };
    let mtime_http = mtime.format("%a, %d %b %Y %H:%M:%S GMT").to_string();
    let name = href.trim_end_matches('/').rsplit('/').next().unwrap_or("");
    format!(
        "<D:response><D:href>{href}</D:href><D:propstat><D:prop>{rt}\
         <D:getcontentlength>{size}</D:getcontentlength>\
         <D:getlastmodified>{mtime_http}</D:getlastmodified>\
         <D:displayname>{name}</D:displayname>\
         </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>"
    )
}

async fn webdav_inner(
    st: &AppState,
    path: &str,
    method: axum::http::Method,
    headers: &axum::http::HeaderMap,
    body: Bytes,
) -> axum::response::Response {
    let path = normalize_path(path);
    let allow = "OPTIONS, HEAD, GET, PUT, DELETE, MKCOL, PROPFIND, COPY, MOVE";
    match method.as_str() {
        "OPTIONS" => (
            StatusCode::OK,
            [
                (header::HeaderName::from_static("allow"), allow),
                (header::HeaderName::from_static("dav"), "1, 2"),
            ],
            "",
        )
            .into_response(),
        "HEAD" => match st.server.store.find_entry(&path).await {
            Ok(Some(e)) => {
                let mut res = StatusCode::OK.into_response();
                if let Ok(v) = header::HeaderValue::from_str(&e.size.to_string()) {
                    res.headers_mut().insert(header::CONTENT_LENGTH, v);
                }
                if e.is_directory {
                    res.headers_mut().insert(
                        header::CONTENT_TYPE,
                        header::HeaderValue::from_static("httpd/unix-directory"),
                    );
                }
                res
            }
            Ok(None) => StatusCode::NOT_FOUND.into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        },
        "GET" => match st.server.get_bytes(&path).await {
            Ok((_, data)) => (StatusCode::OK, data).into_response(),
            Err(e) => (
                StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::NOT_FOUND),
                e.to_string(),
            )
                .into_response(),
        },
        "PUT" => match st.server.put_bytes(&path, body, 0).await {
            Ok(_) => StatusCode::CREATED.into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        },
        "DELETE" => match st.server.delete_path(&path).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
        },
        "MKCOL" => {
            let entry = EntryMeta::directory(opens3_common::util::base_name(&path));
            match st.server.store.insert_entry(&path, &entry).await {
                Ok(()) => StatusCode::CREATED.into_response(),
                Err(opens3_common::Error::AlreadyExists(_)) => {
                    StatusCode::METHOD_NOT_ALLOWED.into_response()
                }
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
        "PROPFIND" => {
            let depth = headers
                .get("depth")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("1");
            let self_entry = st.server.store.find_entry(&path).await.ok().flatten();
            let is_dir = self_entry
                .as_ref()
                .map(|e| e.is_directory)
                .unwrap_or(path == "/");
            let self_size = self_entry.as_ref().map(|e| e.size).unwrap_or(0);
            let self_mtime = self_entry
                .as_ref()
                .map(|e| e.mtime)
                .unwrap_or_else(chrono::Utc::now);
            let href_self = if path == "/" {
                "/dav/".to_string()
            } else {
                format!("/dav{path}")
            };
            let mut xml = String::from(r#"<?xml version="1.0"?><D:multistatus xmlns:D="DAV:">"#);
            xml.push_str(&webdav_prop_response(
                &href_self, is_dir, self_size, self_mtime,
            ));
            if depth != "0" && is_dir {
                let entries = st
                    .server
                    .store
                    .list_directory(&path)
                    .await
                    .unwrap_or_default();
                for e in entries {
                    let href = if path == "/" {
                        format!("/dav/{}", e.name)
                    } else {
                        format!("/dav{path}/{}", e.name)
                    };
                    xml.push_str(&webdav_prop_response(
                        &href,
                        e.is_directory,
                        e.size,
                        e.mtime,
                    ));
                }
            }
            xml.push_str("</D:multistatus>");
            (
                StatusCode::MULTI_STATUS,
                [
                    (header::CONTENT_TYPE, "application/xml; charset=utf-8"),
                    (header::HeaderName::from_static("dav"), "1, 2"),
                ],
                xml,
            )
                .into_response()
        }
        "COPY" | "MOVE" => {
            let Some(dest) = webdav_dest_path(headers) else {
                return (StatusCode::BAD_REQUEST, "Destination required").into_response();
            };
            if dest == path {
                return StatusCode::FORBIDDEN.into_response();
            }
            let overwrite = headers
                .get("overwrite")
                .and_then(|v| v.to_str().ok())
                .unwrap_or("T")
                != "F";
            if let Ok(Some(_)) = st.server.store.find_entry(&dest).await {
                if !overwrite {
                    return StatusCode::PRECONDITION_FAILED.into_response();
                }
                let _ = st.server.delete_path(&dest).await;
            }
            let Ok(Some(src)) = st.server.store.find_entry(&path).await else {
                return StatusCode::NOT_FOUND.into_response();
            };
            if src.is_directory {
                return (
                    StatusCode::NOT_IMPLEMENTED,
                    "directory COPY/MOVE not implemented",
                )
                    .into_response();
            }
            match st.server.get_bytes(&path).await {
                Ok((_, data)) => match st.server.put_bytes(&dest, data.into(), 0).await {
                    Ok(_) => {
                        if method.as_str() == "MOVE" {
                            let _ = st.server.delete_path(&path).await;
                        }
                        StatusCode::CREATED.into_response()
                    }
                    Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
                },
                Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
            }
        }
        _ => (
            StatusCode::METHOD_NOT_ALLOWED,
            [(header::HeaderName::from_static("allow"), allow)],
            "",
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct StorePutQuery {
    mode: Option<String>,
}

#[derive(Deserialize)]
struct StoreEntryBody {
    entry: EntryMeta,
}

async fn store_put_entry(
    State(st): State<AppState>,
    Path(path): Path<String>,
    Query(q): Query<StorePutQuery>,
    Json(body): Json<StoreEntryBody>,
) -> impl IntoResponse {
    let path = normalize_path(&path);
    let mode = q.mode.as_deref().unwrap_or("update");
    let res = if mode == "insert" {
        st.server.store.insert_entry(&path, &body.entry).await
    } else {
        st.server.store.update_entry(&path, &body.entry).await
    };
    match res {
        Ok(()) => StatusCode::OK.into_response(),
        Err(opens3_common::Error::AlreadyExists(_)) => StatusCode::CONFLICT.into_response(),
        Err(e) => (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            e.to_string(),
        )
            .into_response(),
    }
}

async fn store_get_entry(State(st): State<AppState>, Path(path): Path<String>) -> impl IntoResponse {
    let path = normalize_path(&path);
    match st.server.store.find_entry(&path).await {
        Ok(Some(entry)) => Json(serde_json::json!({"entry": entry})).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn store_delete_entry(
    State(st): State<AppState>,
    Path(path): Path<String>,
) -> impl IntoResponse {
    let path = normalize_path(&path);
    match st.server.store.delete_entry(&path).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn store_list_dir(State(st): State<AppState>, Path(path): Path<String>) -> impl IntoResponse {
    let path = normalize_path(&path);
    match st.server.store.list_directory(&path).await {
        Ok(entries) => Json(serde_json::json!({"entries": entries})).into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

#[derive(Deserialize)]
struct PrefixQuery {
    prefix: Option<String>,
    limit: Option<usize>,
}

async fn store_list_prefix(
    State(st): State<AppState>,
    Query(q): Query<PrefixQuery>,
) -> impl IntoResponse {
    let prefix = q.prefix.unwrap_or_else(|| "/".into());
    let limit = q.limit.unwrap_or(1000);
    match st.server.store.list_by_prefix(&prefix, limit).await {
        Ok(items) => {
            let items: Vec<_> = items
                .into_iter()
                .map(|(path, entry)| serde_json::json!({"path": path, "entry": entry}))
                .collect();
            Json(serde_json::json!({"items": items})).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn store_kv_put(
    State(st): State<AppState>,
    Path((ns, key)): Path<(String, String)>,
    body: Bytes,
) -> impl IntoResponse {
    match st.server.store.kv_put(&ns, &key, &body).await {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn store_kv_get(
    State(st): State<AppState>,
    Path((ns, key)): Path<(String, String)>,
) -> impl IntoResponse {
    match st.server.store.kv_get(&ns, &key).await {
        Ok(Some(v)) => (StatusCode::OK, v).into_response(),
        Ok(None) => StatusCode::NOT_FOUND.into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

async fn store_kv_delete(
    State(st): State<AppState>,
    Path((ns, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let _ = st.server.store.kv_delete(&ns, &key).await;
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Deserialize)]
struct KvListQuery {
    prefix: Option<String>,
}

async fn store_kv_list(
    State(st): State<AppState>,
    Path(ns): Path<String>,
    Query(q): Query<KvListQuery>,
) -> impl IntoResponse {
    use base64::Engine;
    let prefix = q.prefix.unwrap_or_default();
    match st.server.store.kv_list(&ns, &prefix).await {
        Ok(items) => {
            let items: Vec<_> = items
                .into_iter()
                .map(|(key, value)| {
                    serde_json::json!({
                        "key": key,
                        "value_b64": base64::engine::general_purpose::STANDARD.encode(value),
                    })
                })
                .collect();
            Json(serde_json::json!({"items": items})).into_response()
        }
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
