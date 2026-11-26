// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! 高级 S3 辅助：Range、版本列表 XML、CORS/标签/锁、ListParts、预签名相关 KV。

use crate::error_xml::xml_escape;
use opens3_common::types::EntryMeta;
use opens3_common::util::join_path;
use opens3_store::FilerStore;
use std::sync::Arc;

/// 解析 `Range: bytes=...`；返回闭区间 (start, end)；非法或越界返回 None
pub fn parse_range(header: Option<&str>, total: u64) -> Option<(u64, u64)> {
    let h = header?;
    let h = h.strip_prefix("bytes=")?;
    let (start_s, end_s) = h.split_once('-')?;
    let start: u64 = if start_s.is_empty() {
        // 后缀形式 bytes=-N：取末尾 N 字节
        let n: u64 = end_s.parse().ok()?;
        total.saturating_sub(n)
    } else {
        start_s.parse().ok()?
    };
    let end: u64 = if end_s.is_empty() {
        total.saturating_sub(1) // 开放结尾到文件末尾
    } else {
        end_s.parse().ok()?
    };
    if start > end || start >= total {
        return None;
    }
    Some((start, end.min(total.saturating_sub(1))))
}

/// ListObjectVersions 响应；delete marker 与普通 Version 分支输出
pub fn list_versions_xml(
    bucket: &str,
    prefix: &str,
    versions: &[(String, &EntryMeta)],
) -> String {
    let mut body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>{}</Name>
  <Prefix>{}</Prefix>
  <MaxKeys>1000</MaxKeys>
  <IsTruncated>false</IsTruncated>"#,
        xml_escape(bucket),
        xml_escape(prefix)
    );
    for (key, meta) in versions {
        let vid = meta
            .version_id
            .clone()
            .unwrap_or_else(|| "null".into());
        if meta.delete_marker {
            body.push_str(&format!(
                "<DeleteMarker><Key>{}</Key><VersionId>{}</VersionId><IsLatest>true</IsLatest><LastModified>{}</LastModified></DeleteMarker>",
                xml_escape(key),
                xml_escape(&vid),
                meta.mtime.to_rfc3339()
            ));
        } else {
            body.push_str(&format!(
                "<Version><Key>{}</Key><VersionId>{}</VersionId><IsLatest>true</IsLatest><LastModified>{}</LastModified><ETag>\"{}\"</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Version>",
                xml_escape(key),
                xml_escape(&vid),
                meta.mtime.to_rfc3339(),
                xml_escape(&meta.etag),
                meta.size
            ));
        }
    }
    body.push_str("</ListVersionsResult>");
    body
}

/// ListParts 响应；`parts` 为 (PartNumber, ETag, Size)
pub fn list_parts_xml(bucket: &str, key: &str, upload_id: &str, parts: &[(i32, String, u64)]) -> String {
    let mut body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListPartsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <UploadId>{}</UploadId>
  <StorageClass>STANDARD</StorageClass>"#,
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id)
    );
    for (n, etag, size) in parts {
        body.push_str(&format!(
            "<Part><PartNumber>{}</PartNumber><ETag>\"{}\"</ETag><Size>{}</Size></Part>",
            n,
            xml_escape(etag),
            size
        ));
    }
    body.push_str("</ListPartsResult>");
    body
}

/// 由 JSON 源列表生成 CORSConfiguration；空列表时默认允许 `*` GET
pub fn cors_xml(rules_json: &str) -> String {
    // 存储侧用简单 JSON 数组保存 AllowedOrigin
    let origins: Vec<String> = serde_json::from_str(rules_json).unwrap_or_default();
    let mut body = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
    );
    if origins.is_empty() {
        body.push_str("<CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedHeader>*</AllowedHeader></CORSRule>");
    } else {
        for o in &origins {
            body.push_str(&format!(
                "<CORSRule><AllowedOrigin>{}</AllowedOrigin><AllowedMethod>GET</AllowedMethod><AllowedMethod>PUT</AllowedMethod><AllowedMethod>POST</AllowedMethod><AllowedMethod>DELETE</AllowedMethod><AllowedMethod>HEAD</AllowedMethod><AllowedHeader>*</AllowedHeader></CORSRule>",
                xml_escape(o)
            ));
        }
    }
    body.push_str("</CORSConfiguration>");
    body
}

/// 对象标签 Tagging XML
pub fn tagging_xml(tags: &[(String, String)]) -> String {
    let mut body = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><TagSet>"#,
    );
    for (k, v) in tags {
        body.push_str(&format!(
            "<Tag><Key>{}</Key><Value>{}</Value></Tag>",
            xml_escape(k),
            xml_escape(v)
        ));
    }
    body.push_str("</TagSet></Tagging>");
    body
}

/// ListMultipartUploads 响应；`uploads` 为 (key, upload_id, initiated)
pub fn list_multipart_uploads_xml(
    bucket: &str,
    prefix: &str,
    uploads: &[(String, String, String)],
) -> String {
    // (key, upload_id, initiated)
    let mut body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListMultipartUploadsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Bucket>{}</Bucket>
  <Prefix>{}</Prefix>
  <MaxUploads>1000</MaxUploads>
  <IsTruncated>false</IsTruncated>"#,
        xml_escape(bucket),
        xml_escape(prefix)
    );
    for (key, upload_id, initiated) in uploads {
        body.push_str(&format!(
            "<Upload><Key>{}</Key><UploadId>{}</UploadId><Initiated>{}</Initiated><StorageClass>STANDARD</StorageClass></Upload>",
            xml_escape(key),
            xml_escape(upload_id),
            xml_escape(initiated)
        ));
    }
    body.push_str("</ListMultipartUploadsResult>");
    body
}

/// multipart 元数据在 KV 中的复合键：`bucket/upload_id`
pub fn multipart_kv_key(bucket: &str, upload_id: &str) -> String {
    format!("{bucket}/{upload_id}")
}

/// 从 FilerStore KV 命名空间读取原始 JSON 字节
pub async fn load_kv_json(store: &Arc<dyn FilerStore>, ns: &str, key: &str) -> Option<Vec<u8>> {
    store.kv_get(ns, key).await.ok().flatten()
}

/// 版本对象落盘路径：`/buckets/{bucket}/.versions/{key}/{version_id}`
pub fn version_object_path(bucket: &str, key: &str, version_id: &str) -> String {
    join_path(
        &format!("/buckets/{bucket}/.versions/{key}"),
        version_id,
    )
}
