// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use crate::auth::payload_hash_from_headers;
use crate::error_xml::s3_error_xml;
use crate::lifecycle::parse_lifecycle_xml;
use crate::xml::*;
use crate::server::AppState;
use axum::body::Bytes;
use axum::extract::{OriginalUri, Path, Query, State};
use axum::http::{header, HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{any, get, put};
use axum::Router;
use opens3_common::metrics::S3Metrics;
use opens3_common::types::{ChunkRef, EntryMeta};
use opens3_common::util::join_path;
use serde::Deserialize;
use std::collections::HashMap;
use std::time::Instant;
use uuid::Uuid;

/// S3 + Admin routes (merged into the unified gateway listener).
pub fn http_router() -> Router<AppState> {
    Router::new()
        .route("/admin", get(admin_ui))
        .route("/admin/", get(admin_ui))
        .route("/admin/ui", get(admin_ui))
        .route("/admin/api/status", get(admin_status))
        .route("/admin/api/topology", get(crate::admin_cluster::admin_topology))
        .route(
            "/admin/api/topo-layout",
            get(crate::admin_cluster::admin_topo_layout_get)
                .put(crate::admin_cluster::admin_topo_layout_put)
                .delete(crate::admin_cluster::admin_topo_layout_delete),
        )
        .route(
            "/admin/api/ops/scale",
            axum::routing::post(crate::admin_cluster::admin_ops_scale),
        )
        .route(
            "/admin/api/ops/heal",
            axum::routing::post(crate::admin_cluster::admin_ops_heal),
        )
        .route(
            "/admin/api/ops/heal/status",
            axum::routing::get(crate::admin_cluster::admin_ops_heal_status),
        )
        .route(
            "/admin/api/ops/heal-bandwidth",
            axum::routing::get(crate::admin_cluster::admin_ops_heal_bandwidth_get)
                .post(crate::admin_cluster::admin_ops_heal_bandwidth_set),
        )
        .route(
            "/admin/api/ops/assign-policy",
            axum::routing::get(crate::admin_cluster::admin_ops_assign_policy_get)
                .post(crate::admin_cluster::admin_ops_assign_policy_set),
        )
        .route(
            "/admin/api/ops/balance",
            axum::routing::post(crate::admin_cluster::admin_ops_balance),
        )
        .route(
            "/admin/api/ops/create-volume",
            axum::routing::post(crate::admin_cluster::admin_ops_create_volume),
        )
        .route(
            "/admin/api/ops/set-replication",
            axum::routing::post(crate::admin_cluster::admin_ops_set_replication),
        )
        .route(
            "/admin/api/ops/grow-volumes",
            axum::routing::post(crate::admin_cluster::admin_ops_grow_volumes),
        )
        .route(
            "/admin/api/ops/delete-volume",
            axum::routing::post(crate::admin_cluster::admin_ops_delete_volume),
        )
        .route(
            "/admin/api/ops/set-volume-online",
            axum::routing::post(crate::admin_cluster::admin_ops_set_volume_online),
        )
        .route(
            "/admin/api/ops/set-node-online",
            axum::routing::post(crate::admin_cluster::admin_ops_set_node_online),
        )
        .route(
            "/admin/api/ops/k8s-nodes",
            axum::routing::get(crate::admin_cluster::admin_ops_k8s_nodes),
        )
        .route(
            "/admin/api/ops/add-data-node",
            axum::routing::post(crate::admin_cluster::admin_ops_add_data_node),
        )
        .route(
            "/admin/api/ops/delete-data-node",
            axum::routing::post(crate::admin_cluster::admin_ops_delete_data_node),
        )
        .route(
            "/admin/api/ops/place-replicas",
            axum::routing::post(crate::admin_cluster::admin_ops_place_replicas),
        )
        .route(
            "/admin/api/ops/clear-replica-of",
            axum::routing::post(crate::admin_cluster::admin_ops_clear_replica_of),
        )
        .route("/admin/api/webhooks", axum::routing::post(admin_webhook))
        .route("/admin/api/replicate", axum::routing::post(admin_replicate))
        .route("/admin/api/fs/buckets", get(admin_list_buckets))
        .route(
            "/admin/api/fs/buckets/{bucket}",
            put(admin_create_bucket).delete(admin_delete_bucket),
        )
        .route("/admin/api/fs/objects", get(admin_list_objects))
        .route(
            "/admin/api/fs/object",
            get(admin_get_object)
                .put(admin_put_object)
                .delete(admin_delete_object),
        )
        .route("/{*path}", any(dispatch))
        .route("/", any(dispatch))
}

fn headers_map(headers: &HeaderMap) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for (k, v) in headers.iter() {
        if let Ok(val) = v.to_str() {
            m.insert(k.as_str().to_ascii_lowercase(), val.to_string());
        }
    }
    // host required
    if let Some(h) = m.get("host").cloned() {
        m.insert("host".into(), h);
    }
    m
}

fn xml_response(status: StatusCode, body: String) -> Response {
    (
        status,
        [(header::CONTENT_TYPE, "application/xml")],
        body,
    )
        .into_response()
}

fn err_response(err: opens3_common::Error, resource: &str) -> Response {
    let id = Uuid::new_v4().to_string();
    xml_response(
        StatusCode::from_u16(err.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
        s3_error_xml(&err, resource, &id),
    )
}

fn bucket_path(bucket: &str) -> String {
    format!("/buckets/{bucket}")
}

fn object_path(bucket: &str, key: &str) -> String {
    join_path(&bucket_path(bucket), key)
}

async fn dispatch(
    State(st): State<AppState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let path = uri.path().to_string();
    let query = uri.query().unwrap_or("").to_string();
    let qmap = query_map(&query);
    let hmap = headers_map(&headers);

    let t0 = Instant::now();
    st.server.s3_metrics.in_flight.inc();
    let _inflight = InFlightGuard(&st.server.s3_metrics);
    let req_bytes = body.len() as u64;
    let payload_hash = payload_hash_from_headers(&hmap, &body);
    let identity = match st.server.auth.read().verify_sigv4(
        method.as_str(),
        &path,
        &canonical_query(&query),
        &hmap,
        &payload_hash,
    ) {
        Ok(id) => id,
        Err(e) => {
            st.server
                .s3_metrics
                .auth_fail
                .with_label_values(&[e.s3_code()])
                .inc();
            st.server.s3_metrics.errors.with_label_values(&[e.s3_code()]).inc();
            return finish_s3(
                &st,
                method.as_str(),
                "Auth",
                e.http_status(),
                t0,
                req_bytes,
                err_response(e, &path),
            );
        }
    };

    // parse bucket / key
    let trimmed = path.trim_start_matches('/');
    if trimmed.is_empty() {
        let resp = list_buckets(&st).await;
        return finish_s3(&st, method.as_str(), "ListBuckets", 200, t0, req_bytes, resp);
    }
    let (bucket, key) = match trimmed.split_once('/') {
        Some((b, k)) => (b.to_string(), k.to_string()),
        None => (trimmed.to_string(), String::new()),
    };

    let s3_action = if qmap.contains_key("uploads") && method == Method::POST {
        "s3:CreateMultipartUpload"
    } else if qmap.contains_key("uploadId") {
        match method {
            Method::PUT => "s3:UploadPart",
            Method::POST => "s3:CompleteMultipartUpload",
            Method::DELETE => "s3:AbortMultipartUpload",
            _ => "s3:Multipart",
        }
    } else {
        match method {
            Method::GET | Method::HEAD if key.is_empty() => "s3:ListBucket",
            Method::GET | Method::HEAD => "s3:GetObject",
            Method::PUT if key.is_empty() => "s3:CreateBucket",
            Method::PUT => "s3:PutObject",
            Method::DELETE if key.is_empty() => "s3:DeleteBucket",
            Method::DELETE => "s3:DeleteObject",
            Method::POST => "s3:PutObject",
            _ => "s3:*",
        }
    };
    if !st
        .server
        .policies
        .read()
        .allowed(&identity, s3_action, &bucket, &key)
    {
        st.server
            .s3_metrics
            .errors
            .with_label_values(&["AccessDenied"])
            .inc();
        return finish_s3(
            &st,
            method.as_str(),
            s3_action,
            403,
            t0,
            req_bytes,
            err_response(
                opens3_common::Error::Forbidden(format!("{identity} denied {s3_action}")),
                &path,
            ),
        );
    }
    st.server
        .s3_metrics
        .request_bytes
        .with_label_values(&["in"])
        .inc_by(req_bytes);

    let resp = if key.is_empty() {
        match method {
            Method::PUT if qmap.contains_key("versioning") => {
                put_versioning(&st, &bucket, &body).await
            }
            Method::PUT if qmap.contains_key("lifecycle") => {
                put_lifecycle(&st, &bucket, &body).await
            }
            Method::PUT if qmap.contains_key("policy") => {
                put_bucket_policy(&st, &bucket, &body).await
            }
            Method::PUT if qmap.contains_key("cors") => {
                put_bucket_cors(&st, &bucket, &body).await
            }
            Method::PUT if qmap.contains_key("tagging") => {
                put_bucket_tagging(&st, &bucket, &body).await
            }
            Method::PUT if qmap.contains_key("object-lock") => {
                put_object_lock(&st, &bucket, &body).await
            }
            Method::PUT if qmap.contains_key("acl") => {
                put_bucket_acl(&st, &bucket, &body, &hmap).await
            }
            Method::PUT => create_bucket(&st, &bucket, &hmap).await,
            Method::DELETE => delete_bucket(&st, &bucket).await,
            Method::GET | Method::HEAD => {
                if qmap.contains_key("versioning") {
                    get_versioning(&st, &bucket).await
                } else if qmap.contains_key("lifecycle") {
                    get_lifecycle(&st, &bucket).await
                } else if qmap.contains_key("policy") {
                    get_bucket_policy(&st, &bucket).await
                } else if qmap.contains_key("versions") {
                    list_object_versions(&st, &bucket, &qmap).await
                } else if qmap.contains_key("cors") {
                    get_bucket_cors(&st, &bucket).await
                } else if qmap.contains_key("tagging") {
                    get_bucket_tagging(&st, &bucket).await
                } else if qmap.contains_key("object-lock") {
                    get_object_lock(&st, &bucket).await
                } else if qmap.contains_key("acl") {
                    get_bucket_acl(&st, &bucket).await
                } else if qmap.contains_key("uploads") {
                    list_multipart_uploads(&st, &bucket, &qmap).await
                } else if qmap.contains_key("location") {
                    xml_response(
                        StatusCode::OK,
                        r#"<?xml version="1.0"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LocationConstraint>"#.into(),
                    )
                } else {
                    list_objects(&st, &bucket, &qmap).await
                }
            }
            Method::POST if qmap.contains_key("delete") => {
                delete_objects(&st, &bucket, &body).await
            }
            _ => err_response(
                opens3_common::Error::InvalidArgument("unsupported bucket op".into()),
                &path,
            ),
        }
    } else if qmap.contains_key("uploads") && method == Method::POST {
        initiate_multipart(&st, &bucket, &key).await
    } else if let Some(upload_id) = qmap.get("uploadId") {
        if method == Method::PUT {
            let part = qmap
                .get("partNumber")
                .and_then(|s| s.parse().ok())
                .unwrap_or(1);
            upload_part(&st, &bucket, &key, upload_id, part, body).await
        } else if method == Method::POST {
            complete_multipart(&st, &bucket, &key, upload_id, &body).await
        } else if method == Method::DELETE {
            abort_multipart(&st, &bucket, &key, upload_id).await
        } else if method == Method::GET {
            list_parts(&st, &bucket, &key, upload_id).await
        } else {
            err_response(
                opens3_common::Error::InvalidArgument("unsupported multipart".into()),
                &path,
            )
        }
    } else {
        match method {
            Method::PUT if qmap.contains_key("tagging") => {
                put_object_tagging(&st, &bucket, &key, &body).await
            }
            Method::PUT if qmap.contains_key("acl") => {
                put_object_acl(&st, &bucket, &key, &body, &hmap).await
            }
            Method::PUT => {
                if let Some(copy_src) = hmap.get("x-amz-copy-source") {
                    copy_object(&st, &bucket, &key, copy_src).await
                } else {
                    put_object(&st, &bucket, &key, body, &hmap).await
                }
            }
            Method::GET | Method::HEAD if qmap.contains_key("tagging") => {
                get_object_tagging(&st, &bucket, &key).await
            }
            Method::GET | Method::HEAD if qmap.contains_key("acl") => {
                get_object_acl(&st, &bucket, &key).await
            }
            Method::GET | Method::HEAD => {
                get_object(
                    &st,
                    &bucket,
                    &key,
                    method == Method::HEAD,
                    &hmap,
                    qmap.get("versionId").map(|s| s.as_str()),
                )
                .await
            }
            Method::DELETE => {
                delete_object(
                    &st,
                    &bucket,
                    &key,
                    qmap.get("versionId").map(|s| s.as_str()),
                    &hmap,
                )
                .await
            }
            _ => err_response(
                opens3_common::Error::InvalidArgument("unsupported".into()),
                &path,
            ),
        }
    };
    let status = resp.status().as_u16();
    if status >= 400 {
        st.server
            .s3_metrics
            .errors
            .with_label_values(&[&status.to_string()])
            .inc();
    }
    finish_s3(
        &st,
        method.as_str(),
        s3_action,
        status,
        t0,
        req_bytes,
        resp,
    )
}

struct InFlightGuard<'a>(&'a S3Metrics);
impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        self.0.in_flight.dec();
    }
}

fn finish_s3(
    st: &AppState,
    method: &str,
    action: &str,
    status: u16,
    t0: Instant,
    _req_bytes: u64,
    resp: Response,
) -> Response {
    st.server
        .s3_metrics
        .requests
        .with_label_values(&[method, action, &status.to_string()])
        .inc();
    st.server
        .s3_metrics
        .request_duration
        .with_label_values(&[action])
        .observe(t0.elapsed().as_secs_f64());
    if let Some(n) = resp
        .headers()
        .get(axum::http::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
    {
        st.server
            .s3_metrics
            .request_bytes
            .with_label_values(&["out"])
            .inc_by(n);
    }
    resp
}

fn query_map(q: &str) -> HashMap<String, String> {
    let mut m = HashMap::new();
    for pair in q.split('&') {
        if pair.is_empty() {
            continue;
        }
        let mut it = pair.splitn(2, '=');
        let k = it.next().unwrap_or("").to_string();
        let v = it.next().unwrap_or("").to_string();
        m.insert(k, v);
    }
    m
}

fn canonical_query(q: &str) -> String {
    let mut pairs: Vec<(String, String)> = query_map(q).into_iter().collect();
    pairs.sort_by(|a, b| a.0.cmp(&b.0));
    pairs
        .into_iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&")
}

async fn list_buckets(st: &AppState) -> Response {
    let entries = match st.server.store.list_directory("/buckets").await {
        Ok(e) => e,
        Err(_) => Vec::new(),
    };
    let buckets: Vec<_> = entries
        .into_iter()
        .filter(|e| e.is_directory)
        .map(|e| (base_name_owned(&e.name), e.crtime.to_rfc3339()))
        .collect();
    xml_response(StatusCode::OK, list_buckets_xml(&buckets))
}

fn base_name_owned(name: &str) -> String {
    name.rsplit('/').next().unwrap_or(name).to_string()
}

async fn create_bucket(
    st: &AppState,
    bucket: &str,
    headers: &HashMap<String, String>,
) -> Response {
    let path = bucket_path(bucket);
    if st.server.store.find_entry(&path).await.ok().flatten().is_some() {
        return err_response(
            opens3_common::Error::AlreadyExists(bucket.to_string()),
            &path,
        );
    }
    let entry = EntryMeta::directory(bucket);
    match st.server.store.insert_entry(&path, &entry).await {
        Ok(()) => {
            if headers
                .get("x-amz-bucket-object-lock-enabled")
                .map(|v| v.eq_ignore_ascii_case("true"))
                .unwrap_or(false)
            {
                let cfg = crate::lock::ObjectLockConfig {
                    enabled: true,
                    mode: Some("GOVERNANCE".into()),
                    days: Some(1),
                    years: None,
                };
                let _ = st
                    .server
                                        .store
                    .kv_put(
                        "lock",
                        bucket,
                        serde_json::to_vec(&cfg).unwrap_or_default().as_slice(),
                    )
                    .await;
                let _ = st
                    .server
                                        .store
                    .kv_put("versioning", bucket, b"Enabled")
                    .await;
            }
            if let Some(acl) = headers.get("x-amz-acl") {
                let canned = crate::acl::CannedAcl::parse(acl);
                let _ = st
                    .server
                                        .store
                    .kv_put("bacl", bucket, &crate::acl::store_value(&canned))
                    .await;
            }
            StatusCode::OK.into_response()
        }
        Err(e) => err_response(e, &path),
    }
}

async fn delete_bucket(st: &AppState, bucket: &str) -> Response {
    let path = bucket_path(bucket);
    let children = st.server.store.list_directory(&path).await.unwrap_or_default();
    if !children.is_empty() {
        return err_response(
            opens3_common::Error::InvalidArgument("BucketNotEmpty".into()),
            &path,
        );
    }
    let _ = st.server.store.delete_entry(&path).await;
    StatusCode::NO_CONTENT.into_response()
}

async fn list_objects(st: &AppState, bucket: &str, q: &HashMap<String, String>) -> Response {
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let delimiter = q.get("delimiter").cloned().unwrap_or_default();
    let max_keys = q
        .get("max-keys")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(1000)
        .clamp(1, 1000);
    let start_after = q
        .get("continuation-token")
        .or_else(|| q.get("start-after"))
        .cloned()
        .unwrap_or_default();
    let base = object_path(bucket, &prefix);
    // fetch a bit more than a page so we can detect truncation
    let fetch_limit = max_keys.saturating_mul(4).max(max_keys + 1).min(10_000);
    let listed = st
        .server
                .store
        .list_by_prefix(&base, fetch_limit)
        .await
        .unwrap_or_default();

    let mut contents = Vec::new();
    let mut common = std::collections::BTreeSet::new();
    let bucket_prefix = format!("{}/", bucket_path(bucket));

    for (path, meta) in &listed {
        if *path == bucket_path(bucket) {
            continue;
        }
        let key = path.strip_prefix(&bucket_prefix).unwrap_or(path);
        if !prefix.is_empty() && !key.starts_with(&prefix) {
            continue;
        }
        if !prefix.is_empty() && key == prefix.trim_end_matches('/') {
            continue;
        }
        if !start_after.is_empty() && key <= start_after.as_str() {
            continue;
        }
        if meta.is_directory {
            if delimiter.is_empty() {
                continue;
            }
            let rest = &key[prefix.len()..];
            if rest.is_empty() {
                continue;
            }
            let name = rest.split(&delimiter).next().unwrap_or(rest);
            if !name.is_empty() {
                common.insert(format!("{prefix}{name}{delimiter}"));
            }
            continue;
        }
        if !delimiter.is_empty() {
            let rest = &key[prefix.len()..];
            if let Some(i) = rest.find(&delimiter) {
                let cp = format!("{}{}", prefix, &rest[..=i]);
                common.insert(cp);
                continue;
            }
        }
        contents.push((key.to_string(), meta));
    }
    let common_prefixes: Vec<_> = common.into_iter().collect();

    // paginate across contents + common prefixes by key order
    let mut merged: Vec<ListItem<'_>> = contents
        .iter()
        .map(|(k, m)| ListItem::Content(k.clone(), *m))
        .chain(common_prefixes.iter().map(|p| ListItem::Prefix(p.clone())))
        .collect();
    merged.sort_by(|a, b| a.key().cmp(b.key()));
    let truncated = merged.len() > max_keys;
    merged.truncate(max_keys);
    let next_token = if truncated {
        merged.last().map(|i| i.key().to_string()).unwrap_or_default()
    } else {
        String::new()
    };
    let page_contents: Vec<(String, &EntryMeta)> = merged
        .iter()
        .filter_map(|i| match i {
            ListItem::Content(k, m) => Some((k.clone(), *m)),
            _ => None,
        })
        .collect();
    let page_prefixes: Vec<String> = merged
        .iter()
        .filter_map(|i| match i {
            ListItem::Prefix(p) => Some(p.clone()),
            _ => None,
        })
        .collect();
    let key_count = page_contents.len() + page_prefixes.len();
    xml_response(
        StatusCode::OK,
        list_objects_v2_xml(
            bucket,
            &prefix,
            &delimiter,
            key_count,
            max_keys,
            truncated,
            &next_token,
            &page_contents,
            &page_prefixes,
        ),
    )
}

enum ListItem<'a> {
    Content(String, &'a EntryMeta),
    Prefix(String),
}

impl ListItem<'_> {
    fn key(&self) -> &str {
        match self {
            ListItem::Content(k, _) => k,
            ListItem::Prefix(p) => p,
        }
    }
}

async fn delete_objects(st: &AppState, bucket: &str, body: &Bytes) -> Response {
    let keys = parse_delete_objects_xml(&String::from_utf8_lossy(body));
    let mut deleted = Vec::new();
    let mut errors: Vec<(String, &str, String)> = Vec::new();
    for key in keys {
        let path = object_path(bucket, &key);
        if let Ok(Some(entry)) = st.server.store.find_entry(&path).await {
            if let Err(e) = crate::lock::enforce_delete(&entry, false) {
                errors.push((key, e.s3_code(), e.to_string()));
                continue;
            }
        }
        match st.server.delete_path(&path).await {
            Ok(()) => deleted.push(key),
            Err(e) => errors.push((key, e.s3_code(), e.to_string())),
        }
    }
    let err_refs: Vec<(String, &str, &str)> = errors
        .iter()
        .map(|(k, c, m)| (k.clone(), *c, m.as_str()))
        .collect();
    xml_response(
        StatusCode::OK,
        delete_objects_result_xml(&deleted, &err_refs),
    )
}

/// Write bytes to a file and return its metadata.  
async fn put_object(// 上传对象
    st: &AppState, // 状态
    bucket: &str, // 桶
    key: &str, // 键
    body: Bytes, // 数据
    headers: &HashMap<String, String>, // 头
) -> Response {
    // 确保桶存在
    let bpath = bucket_path(bucket); // 桶路径  
    if st.server.store.find_entry(&bpath).await.ok().flatten().is_none() {  //
        // 创建桶   
        let _ = st
            .server
                        .store
            .insert_entry(&bpath, &EntryMeta::directory(bucket))
            .await;
    }
    let path = object_path(bucket, key); // 对象路径    
    // Object Lock: block overwrite of retained objects
    if let Ok(Some(existing)) = st.server.store.find_entry(&path).await { // 查找对象
        // 绕过对象锁
        let bypass = headers
            .get("x-amz-bypass-governance-retention")
            .map(|v| v.eq_ignore_ascii_case("true"))
            .unwrap_or(false); // 绕过对象锁
        if let Err(e) = crate::lock::enforce_delete(&existing, bypass) { // 强制删除对象
            return err_response(e, &path);
        }
    }
    let versioning = is_versioning_enabled(st, bucket).await; // 是否启用版本控制
    match st.server.put_bytes(&path, body.clone(), 0).await {
        Ok(mut entry) => {
            if versioning { // 启用版本控制
                let vid = Uuid::new_v4().to_string();
                entry.version_id = Some(vid.clone());
                let _ = st.server.store.update_entry(&path, &entry).await;
                // keep immutable version copy
                let vpath = crate::advanced::version_object_path(bucket, key, &vid);
                let _ = st.server.put_bytes(&vpath, body, 0).await;
                if let Ok(Some(mut ve)) = st.server.store.find_entry(&vpath).await {
                    ve.version_id = Some(vid.clone());
                    let _ = st.server.store.update_entry(&vpath, &ve).await;
                }
            }
            if let Some(mime) = headers.get("content-type") {
                entry.mime = mime.clone();
            }
            // Object Lock defaults + request headers
            if let Some(cfg) = load_lock_config(st, bucket).await {
                crate::lock::apply_default_retention(&mut entry, &cfg);
            }
            crate::lock::apply_request_lock_headers(&mut entry, headers); // 应用请求锁头
            // SSE
            apply_sse_headers_to_entry(st, &mut entry, headers); // 应用SSE头
            if let Some(acl) = headers.get("x-amz-acl") {
                let canned = crate::acl::CannedAcl::parse(acl); // 解析ACL
                let _ = st
                    .server
                                        .store
                    .kv_put(
                        "oacl",
                        &format!("{bucket}/{key}"),
                        &crate::acl::store_value(&canned),
                    )
                    .await;
            }
            let _ = st.server.store.update_entry(&path, &entry).await; // 更新对象
            let mut resp = (
                StatusCode::OK,
                [(header::ETAG, format!("\"{}\"", entry.etag))],
            )
                .into_response();
            if let Some(vid) = &entry.version_id {
                if let Ok(hv) = axum::http::HeaderValue::from_str(vid) {
                    resp.headers_mut().insert("x-amz-version-id", hv);
                }
            }
            inject_sse_response_headers(&mut resp, &entry);
            resp
        }
        Err(e) => err_response(e, &path),
    }
}

async fn get_object(
    st: &AppState,
    bucket: &str,
    key: &str,
    head: bool,
    headers: &HashMap<String, String>,
    version_id: Option<&str>,
) -> Response {
    let path = if let Some(vid) = version_id {
        crate::advanced::version_object_path(bucket, key, vid)
    } else {
        object_path(bucket, key)
    };
    match st.server.get_bytes(&path).await {
        Ok((entry, data)) => {
            if entry.delete_marker {
                return err_response(opens3_common::Error::NotFound(key.into()), &path);
            }
            let (status, slice, content_range) =
                if let Some((start, end)) = crate::advanced::parse_range(
                    headers.get("range").map(|s| s.as_str()),
                    data.len() as u64,
                ) {
                    let end_i = (end as usize).min(data.len().saturating_sub(1));
                    let start_i = (start as usize).min(end_i);
                    (
                        StatusCode::PARTIAL_CONTENT,
                        data[start_i..=end_i].to_vec(),
                        Some(format!("bytes {start}-{end}/{}", data.len())),
                    )
                } else {
                    (StatusCode::OK, data, None)
                };
            let mut builder = Response::builder()
                .status(status)
                .header(header::ETAG, format!("\"{}\"", entry.etag))
                .header(header::CONTENT_TYPE, entry.mime.clone())
                .header(header::CONTENT_LENGTH, slice.len().to_string())
                .header(
                    header::LAST_MODIFIED,
                    entry.mtime.format("%a, %d %b %Y %H:%M:%S GMT").to_string(),
                )
                .header(header::ACCEPT_RANGES, "bytes");
            if let Some(cr) = content_range {
                builder = builder.header(header::CONTENT_RANGE, cr);
            }
            if let Some(vid) = &entry.version_id {
                builder = builder.header("x-amz-version-id", vid);
            }
            if let Some(sse) = entry.user_metadata.get("x-amz-server-side-encryption") {
                builder = builder.header("x-amz-server-side-encryption", sse.as_str());
            }
            if let Some(kid) = entry
                .user_metadata
                .get("x-amz-server-side-encryption-aws-kms-key-id")
            {
                builder = builder.header(
                    "x-amz-server-side-encryption-aws-kms-key-id",
                    kid.as_str(),
                );
            }
            if head {
                builder.body(axum::body::Body::empty()).unwrap_or_else(|_| {
                    StatusCode::INTERNAL_SERVER_ERROR.into_response()
                })
            } else {
                builder
                    .body(axum::body::Body::from(slice))
                    .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
            }
        }
        Err(e) => err_response(e, &path),
    }
}

async fn delete_object(
    st: &AppState,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
    headers: &HashMap<String, String>,
) -> Response {
    let bypass = headers
        .get("x-amz-bypass-governance-retention")
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(false);
    if let Some(vid) = version_id {
        let vpath = crate::advanced::version_object_path(bucket, key, vid);
        if let Ok(Some(entry)) = st.server.store.find_entry(&vpath).await {
            if let Err(e) = crate::lock::enforce_delete(&entry, bypass) {
                return err_response(e, &vpath);
            }
        }
        return match st.server.delete_path(&vpath).await {
            Ok(()) => StatusCode::NO_CONTENT.into_response(),
            Err(e) => err_response(e, &vpath),
        };
    }
    let path = object_path(bucket, key);
    if let Ok(Some(entry)) = st.server.store.find_entry(&path).await {
        if let Err(e) = crate::lock::enforce_delete(&entry, bypass) {
            return err_response(e, &path);
        }
    }
    if is_versioning_enabled(st, bucket).await {
        let mut entry = EntryMeta::file(key, 0, String::new(), String::new());
        entry.delete_marker = true;
        entry.version_id = Some(Uuid::new_v4().to_string());
        let _ = st.server.store.update_entry(&path, &entry).await;
        return StatusCode::NO_CONTENT.into_response();
    }
    match st.server.delete_path(&path).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => err_response(e, &path),
    }
}

async fn copy_object(st: &AppState, bucket: &str, key: &str, copy_src: &str) -> Response {
    let src = copy_src.trim_start_matches('/');
    let (src_bucket, src_key) = src.split_once('/').unwrap_or((src, ""));
    let src_path = object_path(src_bucket, src_key);
    match st.server.get_bytes(&src_path).await {
        Ok((entry, data)) => match st
            .server
                        .put_bytes(&object_path(bucket, key), Bytes::from(data), 0)
            .await
        {
            Ok(new_e) => xml_response(
                StatusCode::OK,
                copy_object_xml(&new_e.etag, &entry.mtime.to_rfc3339()),
            ),
            Err(e) => err_response(e, key),
        },
        Err(e) => err_response(e, copy_src),
    }
}

async fn initiate_multipart(st: &AppState, bucket: &str, key: &str) -> Response {
    let upload_id = Uuid::new_v4().to_string();
    let initiated = chrono::Utc::now().to_rfc3339();
    let meta = serde_json::json!({
        "bucket": bucket,
        "key": key,
        "upload_id": upload_id,
        "initiated": initiated,
        "parts": {}
    });
    let kv_key = crate::advanced::multipart_kv_key(bucket, &upload_id);
    let _ = st
        .server
                .store
        .kv_put("multipart", &kv_key, meta.to_string().as_bytes())
        .await;
    st.server.s3_metrics.multipart_uploads.inc();
    xml_response(
        StatusCode::OK,
        initiate_multipart_xml(bucket, key, &upload_id),
    )
}

async fn multipart_get(st: &AppState, bucket: &str, upload_id: &str) -> Option<Vec<u8>> {
    let kv_key = crate::advanced::multipart_kv_key(bucket, upload_id);
    if let Ok(Some(raw)) = st.server.store.kv_get("multipart", &kv_key).await {
        return Some(raw);
    }
    // legacy key = upload_id only
    st.server
                .store
        .kv_get("multipart", upload_id)
        .await
        .ok()
        .flatten()
}

async fn multipart_put(st: &AppState, bucket: &str, upload_id: &str, raw: &[u8]) {
    let kv_key = crate::advanced::multipart_kv_key(bucket, upload_id);
    let _ = st.server.store.kv_put("multipart", &kv_key, raw).await;
    let _ = st.server.store.kv_delete("multipart", upload_id).await;
}

async fn multipart_del(st: &AppState, bucket: &str, upload_id: &str) {
    let kv_key = crate::advanced::multipart_kv_key(bucket, upload_id);
    let _ = st.server.store.kv_delete("multipart", &kv_key).await;
    let _ = st.server.store.kv_delete("multipart", upload_id).await;
}

async fn upload_part(
    st: &AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
    part: i32,
    body: Bytes,
) -> Response {
    let _ = key;
    match st.server.blob.write_blob("", &body, None, 0).await {
        Ok((fid, etag)) => {
            if let Some(raw) = multipart_get(st, bucket, upload_id).await {
                if let Ok(mut v) = serde_json::from_slice::<serde_json::Value>(&raw) {
                    v["parts"][part.to_string()] =
                        serde_json::json!({"fid": fid, "etag": etag, "size": body.len()});
                    multipart_put(st, bucket, upload_id, v.to_string().as_bytes()).await;
                }
            }
            (StatusCode::OK, [(header::ETAG, format!("\"{etag}\""))]).into_response()
        }
        Err(e) => err_response(e, key),
    }
}

async fn complete_multipart(
    st: &AppState,
    bucket: &str,
    key: &str,
    upload_id: &str,
    _body: &Bytes,
) -> Response {
    let raw = match multipart_get(st, bucket, upload_id).await {
        Some(r) => r,
        None => {
            return err_response(
                opens3_common::Error::NotFound("upload".into()),
                upload_id,
            )
        }
    };
    let v: serde_json::Value = serde_json::from_slice(&raw).unwrap_or_default();
    let mut chunks = Vec::new();
    let mut total = 0u64;
    let mut etag_src = String::new();
    if let Some(parts) = v.get("parts").and_then(|p| p.as_object()) {
        let mut nums: Vec<i32> = parts.keys().filter_map(|k| k.parse().ok()).collect();
        nums.sort_unstable();
        for n in nums {
            if let Some(p) = parts.get(&n.to_string()) {
                let fid = p["fid"].as_str().unwrap_or("").to_string();
                let etag = p["etag"].as_str().unwrap_or("").to_string();
                let size = p["size"].as_u64().unwrap_or(0);
                chunks.push(ChunkRef {
                    file_id: fid,
                    offset: total,
                    size,
                    etag: etag.clone(),
                });
                total += size;
                etag_src.push_str(&etag);
            }
        }
    }
    let etag = opens3_common::util::md5_hex(etag_src.as_bytes());
    let mut entry = EntryMeta::file(key, total, String::new(), etag.clone());
    entry.file_id = None;
    entry.chunks = chunks;
    if let Some(cfg) = load_lock_config(st, bucket).await {
        crate::lock::apply_default_retention(&mut entry, &cfg);
    }
    let path = object_path(bucket, key);
    let _ = st.server.store.update_entry(&path, &entry).await;
    if st.server.store.find_entry(&path).await.ok().flatten().is_none() {
        let _ = st.server.store.insert_entry(&path, &entry).await;
    }
    multipart_del(st, bucket, upload_id).await;
    st.server.s3_metrics.multipart_uploads.dec();
    xml_response(StatusCode::OK, complete_multipart_xml(bucket, key, &etag))
}

async fn abort_multipart(st: &AppState, bucket: &str, _key: &str, upload_id: &str) -> Response {
    if let Some(raw) = multipart_get(st, bucket, upload_id).await {
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&raw) {
            if let Some(parts) = v.get("parts").and_then(|p| p.as_object()) {
                for p in parts.values() {
                    if let Some(fid) = p.get("fid").and_then(|f| f.as_str()) {
                        let _ = st.server.blob.delete_blob(fid).await;
                    }
                }
            }
        }
    }
    multipart_del(st, bucket, upload_id).await;
    st.server.s3_metrics.multipart_uploads.dec();
    StatusCode::NO_CONTENT.into_response()
}

async fn list_multipart_uploads(
    st: &AppState,
    bucket: &str,
    qmap: &HashMap<String, String>,
) -> Response {
    let prefix = qmap.get("prefix").cloned().unwrap_or_default();
    let max: usize = qmap
        .get("max-uploads")
        .and_then(|s| s.parse().ok())
        .unwrap_or(1000)
        .min(1000);
    let items = st
        .server
                .store
        .kv_list("multipart", &format!("{bucket}/"))
        .await
        .unwrap_or_default();
    let mut uploads = Vec::new();
    for (_k, raw) in items {
        let v: serde_json::Value = serde_json::from_slice(&raw).unwrap_or_default();
        let key = v.get("key").and_then(|x| x.as_str()).unwrap_or("").to_string();
        if !prefix.is_empty() && !key.starts_with(&prefix) {
            continue;
        }
        let upload_id = v
            .get("upload_id")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        let initiated = v
            .get("initiated")
            .and_then(|x| x.as_str())
            .unwrap_or("")
            .to_string();
        if upload_id.is_empty() {
            continue;
        }
        uploads.push((key, upload_id, initiated));
        if uploads.len() >= max {
            break;
        }
    }
    xml_response(
        StatusCode::OK,
        crate::advanced::list_multipart_uploads_xml(bucket, &prefix, &uploads),
    )
}

async fn is_versioning_enabled(st: &AppState, bucket: &str) -> bool {
    st.server
                .store
        .kv_get("versioning", bucket)
        .await
        .ok()
        .flatten()
        .map(|v| v == b"Enabled")
        .unwrap_or(false)
}

async fn get_versioning(st: &AppState, bucket: &str) -> Response {
    let enabled = is_versioning_enabled(st, bucket).await;
    xml_response(StatusCode::OK, versioning_xml(enabled))
}

async fn put_versioning(st: &AppState, bucket: &str, body: &Bytes) -> Response {
    let text = String::from_utf8_lossy(body);
    let status = if text.contains("Enabled") {
        "Enabled"
    } else {
        "Suspended"
    };
    let _ = st
        .server
                .store
        .kv_put("versioning", bucket, status.as_bytes())
        .await;
    StatusCode::OK.into_response()
}

async fn get_lifecycle(st: &AppState, bucket: &str) -> Response {
    if let Some(cfg) = st.server.lifecycles.read().get(bucket) {
        let days = cfg
            .rules
            .first()
            .and_then(|r| r.expiration.as_ref())
            .and_then(|e| e.days)
            .unwrap_or(0);
        let prefix = cfg
            .rules
            .first()
            .map(|r| r.filter.prefix.clone())
            .unwrap_or_default();
        return xml_response(
            StatusCode::OK,
            format!(
                r#"<?xml version="1.0"?><LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Rule><ID>rule1</ID><Status>Enabled</Status><Filter><Prefix>{prefix}</Prefix></Filter><Expiration><Days>{days}</Days></Expiration></Rule></LifecycleConfiguration>"#
            ),
        );
    }
    xml_response(
        StatusCode::OK,
        r#"<?xml version="1.0"?><LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></LifecycleConfiguration>"#.into(),
    )
}

async fn put_lifecycle(st: &AppState, bucket: &str, body: &Bytes) -> Response {
    let cfg = parse_lifecycle_xml(&String::from_utf8_lossy(body));
    if let Ok(bytes) = serde_json::to_vec(&cfg) {
        let _ = st.server.store.kv_put("lifecycle", bucket, &bytes).await;
    }
    st.server.lifecycles.write().insert(bucket.to_string(), cfg);
    StatusCode::OK.into_response()
}

async fn get_bucket_policy(st: &AppState, bucket: &str) -> Response {
    match st.server.policies.read().bucket_policies.get(bucket) {
        Some(p) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            serde_json::to_string(p).unwrap_or_else(|_| "{}".into()),
        )
            .into_response(),
        None => err_response(
            opens3_common::Error::NotFound("policy".into()),
            bucket,
        ),
    }
}

async fn put_bucket_policy(st: &AppState, bucket: &str, body: &Bytes) -> Response {
    match serde_json::from_slice::<crate::policy::PolicyDocument>(body) {
        Ok(doc) => {
            if let Ok(bytes) = serde_json::to_vec(&doc) {
                let _ = st.server.store.kv_put("policy", bucket, &bytes).await;
            }
            st.server
                .policies
                .write()
                .bucket_policies
                .insert(bucket.to_string(), doc);
            StatusCode::OK.into_response()
        }
        Err(e) => err_response(
            opens3_common::Error::InvalidArgument(e.to_string()),
            bucket,
        ),
    }
}

async fn admin_ui() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("../../../admin/static/index.html"),
    )
}

async fn admin_status(State(st): State<AppState>) -> impl IntoResponse {
    axum::Json(serde_json::json!({
        "service": "opens3-s3",
        "port": st.server.port,
        "clerk": st.server.blob.clerk,
        "auth_enabled": st.server.auth.read().enabled,
    }))
}

async fn admin_list_buckets(State(st): State<AppState>) -> impl IntoResponse {
    let entries = st
        .server
                .store
        .list_directory("/buckets")
        .await
        .unwrap_or_default();
    let buckets: Vec<String> = entries
        .into_iter()
        .filter(|e| e.is_directory)
        .map(|e| base_name_owned(&e.name))
        .collect();
    axum::Json(serde_json::json!({ "buckets": buckets }))
}

async fn admin_create_bucket(
    State(st): State<AppState>,
    Path(bucket): Path<String>,
) -> Response {
    let path = bucket_path(&bucket);
    if st.server.store.find_entry(&path).await.ok().flatten().is_some() {
        return (
            StatusCode::CONFLICT,
            axum::Json(serde_json::json!({"error": "bucket exists"})),
        )
            .into_response();
    }
    match st
        .server
                .store
        .insert_entry(&path, &EntryMeta::directory(&bucket))
        .await
    {
        Ok(()) => StatusCode::OK.into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn admin_delete_bucket(
    State(st): State<AppState>,
    Path(bucket): Path<String>,
) -> Response {
    let path = bucket_path(&bucket);
    let children = st
        .server
                .store
        .list_directory(&path)
        .await
        .unwrap_or_default();
    if !children.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            axum::Json(serde_json::json!({"error": "BucketNotEmpty"})),
        )
            .into_response();
    }
    let _ = st.server.store.delete_entry(&path).await;
    StatusCode::NO_CONTENT.into_response()
}

#[derive(Deserialize)]
struct AdminListQuery {
    bucket: String,
    #[serde(default)]
    prefix: String,
    #[serde(default = "default_delimiter")]
    delimiter: String,
}

fn default_delimiter() -> String {
    "/".into()
}

async fn admin_list_objects(
    State(st): State<AppState>,
    Query(q): Query<AdminListQuery>,
) -> impl IntoResponse {
    let base = object_path(&q.bucket, &q.prefix);
    let listed = st
        .server
                .store
        .list_by_prefix(&base, 5000)
        .await
        .unwrap_or_default();
    let bucket_root = bucket_path(&q.bucket);
    let bucket_prefix = format!("{bucket_root}/");
    let mut prefixes = std::collections::BTreeSet::new();
    let mut objects = Vec::new();
    for (path, meta) in listed {
        // Skip the bucket root itself
        if path == bucket_root || path == format!("{bucket_root}/") {
            continue;
        }
        let rel = path
            .strip_prefix(&bucket_prefix)
            .unwrap_or(path.trim_start_matches('/'));
        if !q.prefix.is_empty() && !rel.starts_with(&q.prefix) {
            continue;
        }
        // Exact match of current prefix directory → skip
        if !q.prefix.is_empty() && rel == q.prefix.trim_end_matches('/') {
            continue;
        }
        let rest = if rel.len() >= q.prefix.len() {
            &rel[q.prefix.len()..]
        } else {
            continue;
        };
        if rest.is_empty() {
            continue;
        }
        if meta.is_directory {
            // Immediate child folder under current prefix
            if q.delimiter.is_empty() {
                continue;
            }
            let name = rest.split(&q.delimiter).next().unwrap_or(rest);
            if !name.is_empty() {
                prefixes.insert(format!("{}{}{}", q.prefix, name, q.delimiter));
            }
            continue;
        }
        if !q.delimiter.is_empty() {
            if let Some((head, _)) = rest.split_once(&q.delimiter) {
                if !head.is_empty() {
                    prefixes.insert(format!("{}{}{}", q.prefix, head, q.delimiter));
                }
                continue;
            }
        }
        objects.push(serde_json::json!({
            "key": rel,
            "size": meta.size,
            "mtime": meta.mtime.to_rfc3339(),
            "etag": meta.etag,
            "folder": false,
        }));
    }
    axum::Json(serde_json::json!({
        "bucket": q.bucket,
        "prefix": q.prefix,
        "prefixes": prefixes.into_iter().collect::<Vec<_>>(),
        "objects": objects,
    }))
}

#[derive(Deserialize)]
struct AdminObjectQuery {
    bucket: String,
    key: String,
}

async fn admin_get_object(
    State(st): State<AppState>,
    Query(q): Query<AdminObjectQuery>,
) -> Response {
    let path = object_path(&q.bucket, &q.key);
    match st.server.get_bytes(&path).await {
        Ok((entry, data)) => {
            let filename = q.key.rsplit('/').next().unwrap_or("download");
            let mut resp = Response::builder()
                .status(StatusCode::OK)
                .header(
                    header::CONTENT_TYPE,
                    if entry.mime.is_empty() {
                        "application/octet-stream"
                    } else {
                        entry.mime.as_str()
                    },
                )
                .header(header::CONTENT_LENGTH, data.len().to_string());
            if let Ok(cd) = HeaderValue::from_str(&format!(
                "attachment; filename=\"{}\"",
                filename.replace('"', "")
            )) {
                resp = resp.header(header::CONTENT_DISPOSITION, cd);
            }
            resp.body(axum::body::Body::from(data))
                .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
        }
        Err(e) => (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::NOT_FOUND),
            axum::Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn admin_put_object(
    State(st): State<AppState>,
    Query(q): Query<AdminObjectQuery>,
    body: Bytes,
) -> Response {
    let bpath = bucket_path(&q.bucket);
    if st.server.store.find_entry(&bpath).await.ok().flatten().is_none() {
        let _ = st
            .server
                        .store
            .insert_entry(&bpath, &EntryMeta::directory(&q.bucket))
            .await;
    }
    // Trailing slash → create a real directory entry (path normalize strips '/').
    if q.key.ends_with('/') {
        let dir_key = q.key.trim_end_matches('/');
        if dir_key.is_empty() {
            return (
                StatusCode::BAD_REQUEST,
                axum::Json(serde_json::json!({"error": "invalid folder name"})),
            )
                .into_response();
        }
        let path = object_path(&q.bucket, dir_key);
        let name = dir_key.rsplit('/').next().unwrap_or(dir_key);
        let entry = EntryMeta::directory(name);
        // Idempotent: already exists as dir is OK
        match st.server.store.find_entry(&path).await {
            Ok(Some(existing)) if existing.is_directory => {
                return axum::Json(serde_json::json!({"ok": true, "folder": true})).into_response();
            }
            Ok(Some(_)) => {
                return (
                    StatusCode::CONFLICT,
                    axum::Json(serde_json::json!({"error": "path exists as object"})),
                )
                    .into_response();
            }
            _ => {}
        }
        return match st.server.store.insert_entry(&path, &entry).await {
            Ok(()) => axum::Json(serde_json::json!({"ok": true, "folder": true})).into_response(),
            Err(opens3_common::Error::AlreadyExists(_)) => {
                axum::Json(serde_json::json!({"ok": true, "folder": true})).into_response()
            }
            Err(e) => (
                StatusCode::INTERNAL_SERVER_ERROR,
                axum::Json(serde_json::json!({"error": e.to_string()})),
            )
                .into_response(),
        };
    }
    let path = object_path(&q.bucket, &q.key);
    match st.server.put_bytes(&path, body, 0).await {
        Ok(entry) => axum::Json(serde_json::json!({
            "ok": true,
            "etag": entry.etag,
            "size": entry.size,
        }))
        .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            axum::Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn admin_delete_object(
    State(st): State<AppState>,
    Query(q): Query<AdminObjectQuery>,
) -> Response {
    let key = q.key.trim_end_matches('/');
    let path = object_path(&q.bucket, key);
    // Directories: delete_entry; files: delete_path (blob + meta)
    if let Ok(Some(meta)) = st.server.store.find_entry(&path).await {
        if meta.is_directory {
            let children = st
                .server
                                .store
                .list_directory(&path)
                .await
                .unwrap_or_default();
            if !children.is_empty() {
                return (
                    StatusCode::BAD_REQUEST,
                    axum::Json(serde_json::json!({"error": "folder not empty"})),
                )
                    .into_response();
            }
            return match st.server.store.delete_entry(&path).await {
                Ok(_) => StatusCode::NO_CONTENT.into_response(),
                Err(e) => (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    axum::Json(serde_json::json!({"error": e.to_string()})),
                )
                    .into_response(),
            };
        }
    }
    match st.server.delete_path(&path).await {
        Ok(()) => StatusCode::NO_CONTENT.into_response(),
        Err(e) => (
            StatusCode::from_u16(e.http_status()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            axum::Json(serde_json::json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Deserialize)]
struct WebhookReq {
    url: String,
}

async fn admin_webhook(State(st): State<AppState>, axum::Json(body): axum::Json<WebhookReq>) -> impl IntoResponse {
    st.server.events.add_webhook(body.url);
    axum::Json(serde_json::json!({"ok": true}))
}

#[derive(Deserialize)]
struct ReplicateReq {
    prefix: String,
    target_filer: String,
}

async fn admin_replicate(
    State(st): State<AppState>,
    axum::Json(body): axum::Json<ReplicateReq>,
) -> impl IntoResponse {
    let listed = st
        .server
                .store
        .list_by_prefix(&body.prefix, 10_000)
        .await
        .unwrap_or_default();
    let client = reqwest::Client::new();
    let mut ok = 0usize;
    let mut fail = 0usize;
    for (path, _) in listed {
        match st.server.get_bytes(&path).await {
            Ok((_, data)) => {
                let url = format!(
                    "{}/entries{}",
                    body.target_filer.trim_end_matches('/'),
                    path
                );
                match client.put(url).body(data).send().await {
                    Ok(r) if r.status().is_success() => ok += 1,
                    _ => fail += 1,
                }
            }
            Err(_) => fail += 1,
        }
    }
    axum::Json(serde_json::json!({"ok": ok, "fail": fail}))
}

#[derive(Deserialize)]
pub struct PutBucketVersioning {
    pub status: Option<String>,
}

async fn list_object_versions(
    st: &AppState,
    bucket: &str,
    q: &HashMap<String, String>,
) -> Response {
    let prefix = q.get("prefix").cloned().unwrap_or_default();
    let base = object_path(bucket, &prefix);
    let listed = st
        .server
                .store
        .list_by_prefix(&base, 1000)
        .await
        .unwrap_or_default();
    let bucket_prefix = format!("{}/", bucket_path(bucket));
    let mut owned: Vec<(String, EntryMeta)> = Vec::new();
    for (path, meta) in listed {
        let key = path.strip_prefix(&bucket_prefix).unwrap_or(&path).to_string();
        if key.contains("/.versions/") {
            continue;
        }
        owned.push((key, meta));
    }
    // also list version copies
    let vbase = format!("/buckets/{bucket}/.versions/");
    if let Ok(vlisted) = st.server.store.list_by_prefix(&vbase, 2000).await {
        for (path, meta) in vlisted {
            let rest = path.strip_prefix(&vbase).unwrap_or(&path);
            if let Some((key, _)) = rest.rsplit_once('/') {
                owned.push((key.to_string(), meta));
            }
        }
    }
    let versions: Vec<(String, &EntryMeta)> =
        owned.iter().map(|(k, m)| (k.clone(), m)).collect();
    xml_response(
        StatusCode::OK,
        crate::advanced::list_versions_xml(bucket, &prefix, &versions),
    )
}

async fn list_parts(st: &AppState, bucket: &str, key: &str, upload_id: &str) -> Response {
    let mut parts = Vec::new();
    if let Some(raw) = multipart_get(st, bucket, upload_id).await {
        if let Ok(v) = serde_json::from_slice::<serde_json::Value>(&raw) {
            if let Some(obj) = v.get("parts").and_then(|p| p.as_object()) {
                for (k, val) in obj {
                    if let Ok(n) = k.parse::<i32>() {
                        let etag = val
                            .get("etag")
                            .and_then(|x| x.as_str())
                            .unwrap_or("")
                            .to_string();
                        let size = val.get("size").and_then(|x| x.as_u64()).unwrap_or(0);
                        parts.push((n, etag, size));
                    }
                }
            }
        }
    }
    parts.sort_by_key(|p| p.0);
    xml_response(
        StatusCode::OK,
        crate::advanced::list_parts_xml(bucket, key, upload_id, &parts),
    )
}

async fn put_bucket_cors(st: &AppState, bucket: &str, body: &Bytes) -> Response {
    // Accept either JSON array of origins or XML with AllowedOrigin
    let text = String::from_utf8_lossy(body);
    let origins: Vec<String> = if text.trim_start().starts_with('[') {
        serde_json::from_str(&text).unwrap_or_default()
    } else {
        text.split("<AllowedOrigin>")
            .skip(1)
            .filter_map(|s| s.split("</AllowedOrigin>").next().map(|x| x.to_string()))
            .collect()
    };
    let _ = st
        .server
                .store
        .kv_put("cors", bucket, serde_json::to_vec(&origins).unwrap_or_default().as_slice())
        .await;
    StatusCode::OK.into_response()
}

async fn get_bucket_cors(st: &AppState, bucket: &str) -> Response {
    let raw = st
        .server
                .store
        .kv_get("cors", bucket)
        .await
        .ok()
        .flatten()
        .unwrap_or_else(|| b"[]".to_vec());
    xml_response(
        StatusCode::OK,
        crate::advanced::cors_xml(&String::from_utf8_lossy(&raw)),
    )
}

async fn put_bucket_tagging(st: &AppState, bucket: &str, body: &Bytes) -> Response {
    let _ = st.server.store.kv_put("btag", bucket, body).await;
    StatusCode::OK.into_response()
}

async fn get_bucket_tagging(st: &AppState, bucket: &str) -> Response {
    let tags = parse_tagging_xml(
        &String::from_utf8_lossy(
            &st.server
                                .store
                .kv_get("btag", bucket)
                .await
                .ok()
                .flatten()
                .unwrap_or_default(),
        ),
    );
    xml_response(StatusCode::OK, crate::advanced::tagging_xml(&tags))
}

async fn put_object_tagging(st: &AppState, bucket: &str, key: &str, body: &Bytes) -> Response {
    let k = format!("{bucket}/{key}");
    let _ = st.server.store.kv_put("otag", &k, body).await;
    StatusCode::OK.into_response()
}

async fn get_object_tagging(st: &AppState, bucket: &str, key: &str) -> Response {
    let k = format!("{bucket}/{key}");
    let tags = parse_tagging_xml(
        &String::from_utf8_lossy(
            &st.server
                                .store
                .kv_get("otag", &k)
                .await
                .ok()
                .flatten()
                .unwrap_or_default(),
        ),
    );
    xml_response(StatusCode::OK, crate::advanced::tagging_xml(&tags))
}

async fn put_object_lock(st: &AppState, bucket: &str, body: &Bytes) -> Response {
    let cfg = crate::lock::ObjectLockConfig::from_xml(&String::from_utf8_lossy(body));
    let _ = st
        .server
                .store
        .kv_put(
            "lock",
            bucket,
            serde_json::to_vec(&cfg).unwrap_or_default().as_slice(),
        )
        .await;
    if cfg.enabled {
        let _ = st
            .server
                        .store
            .kv_put("versioning", bucket, b"Enabled")
            .await;
    }
    StatusCode::OK.into_response()
}

async fn get_object_lock(st: &AppState, bucket: &str) -> Response {
    match load_lock_config(st, bucket).await {
        Some(cfg) => xml_response(StatusCode::OK, cfg.to_xml()),
        None => err_response(
            opens3_common::Error::NotFound("ObjectLockConfiguration".into()),
            bucket,
        ),
    }
}

async fn load_lock_config(st: &AppState, bucket: &str) -> Option<crate::lock::ObjectLockConfig> {
    let raw = st.server.store.kv_get("lock", bucket).await.ok().flatten()?;
    if let Ok(cfg) = serde_json::from_slice::<crate::lock::ObjectLockConfig>(&raw) {
        return Some(cfg);
    }
    // legacy raw XML
    Some(crate::lock::ObjectLockConfig::from_xml(
        &String::from_utf8_lossy(&raw),
    ))
}

fn apply_sse_headers_to_entry(
    st: &AppState,
    entry: &mut EntryMeta,
    headers: &HashMap<String, String>,
) {
    let sse = headers
        .get("x-amz-server-side-encryption")
        .cloned()
        .or_else(|| {
            if st.server.encrypt_key.is_some() {
                Some("AES256".into())
            } else {
                None
            }
        });
    if let Some(algo) = sse {
        if algo == "aws:kms" {
            entry
                .user_metadata
                .insert("x-amz-server-side-encryption".into(), "aws:kms".into());
            let kid = headers
                .get("x-amz-server-side-encryption-aws-kms-key-id")
                .cloned()
                .unwrap_or_else(|| "opens3-local-kms".into());
            entry.user_metadata.insert(
                "x-amz-server-side-encryption-aws-kms-key-id".into(),
                kid,
            );
            // Local KMS: mark encrypted when filer has key or kms_key present
            if st.server.encrypt_key.is_some() || st.server.kms_key.is_some() {
                entry.encrypted = true;
            }
        } else {
            entry
                .user_metadata
                .insert("x-amz-server-side-encryption".into(), "AES256".into());
            if st.server.encrypt_key.is_some() {
                entry.encrypted = true;
            }
        }
    }
}

fn inject_sse_response_headers(resp: &mut Response, entry: &EntryMeta) {
    if let Some(sse) = entry.user_metadata.get("x-amz-server-side-encryption") {
        if let Ok(hv) = axum::http::HeaderValue::from_str(sse) {
            resp.headers_mut()
                .insert("x-amz-server-side-encryption", hv);
        }
    }
    if let Some(kid) = entry
        .user_metadata
        .get("x-amz-server-side-encryption-aws-kms-key-id")
    {
        if let Ok(hv) = axum::http::HeaderValue::from_str(kid) {
            resp.headers_mut()
                .insert("x-amz-server-side-encryption-aws-kms-key-id", hv);
        }
    }
}

async fn put_bucket_acl(
    st: &AppState,
    bucket: &str,
    body: &Bytes,
    headers: &HashMap<String, String>,
) -> Response {
    let canned = if let Some(a) = headers.get("x-amz-acl") {
        crate::acl::CannedAcl::parse(a)
    } else {
        crate::acl::parse_canned_from_acl_xml(&String::from_utf8_lossy(body))
    };
    let _ = st
        .server
                .store
        .kv_put("bacl", bucket, &crate::acl::store_value(&canned))
        .await;
    StatusCode::OK.into_response()
}

async fn get_bucket_acl(st: &AppState, bucket: &str) -> Response {
    let canned = st
        .server
                .store
        .kv_get("bacl", bucket)
        .await
        .ok()
        .flatten()
        .map(|r| crate::acl::load_value(&r))
        .unwrap_or(crate::acl::CannedAcl::Private);
    xml_response(StatusCode::OK, crate::acl::acl_xml("opens3", &canned))
}

async fn put_object_acl(
    st: &AppState,
    bucket: &str,
    key: &str,
    body: &Bytes,
    headers: &HashMap<String, String>,
) -> Response {
    let canned = if let Some(a) = headers.get("x-amz-acl") {
        crate::acl::CannedAcl::parse(a)
    } else {
        crate::acl::parse_canned_from_acl_xml(&String::from_utf8_lossy(body))
    };
    let _ = st
        .server
                .store
        .kv_put(
            "oacl",
            &format!("{bucket}/{key}"),
            &crate::acl::store_value(&canned),
        )
        .await;
    StatusCode::OK.into_response()
}

async fn get_object_acl(st: &AppState, bucket: &str, key: &str) -> Response {
    let canned = st
        .server
                .store
        .kv_get("oacl", &format!("{bucket}/{key}"))
        .await
        .ok()
        .flatten()
        .map(|r| crate::acl::load_value(&r))
        .unwrap_or(crate::acl::CannedAcl::Private);
    xml_response(StatusCode::OK, crate::acl::acl_xml("opens3", &canned))
}

fn parse_tagging_xml(body: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut rest = body;
    while let Some(ks) = rest.find("<Key>") {
        rest = &rest[ks + 5..];
        let Some(ke) = rest.find("</Key>") else { break };
        let key = rest[..ke].to_string();
        rest = &rest[ke..];
        let Some(vs) = rest.find("<Value>") else { break };
        rest = &rest[vs + 7..];
        let Some(ve) = rest.find("</Value>") else { break };
        let val = rest[..ve].to_string();
        out.push((key, val));
        rest = &rest[ve..];
    }
    out
}
