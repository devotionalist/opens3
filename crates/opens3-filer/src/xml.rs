// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! S3 API 响应 XML 构造与 DeleteObjects 请求解析。

use crate::error_xml::xml_escape;
use opens3_common::types::EntryMeta;

/// ListBuckets 响应 XML；`buckets` 为 (名称, 创建时间)
pub fn list_buckets_xml(buckets: &[(String, String)]) -> String {
    let mut body = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Owner><ID>opens3</ID><DisplayName>opens3</DisplayName></Owner>
  <Buckets>"#,
    );
    for (name, created) in buckets {
        body.push_str(&format!(
            "<Bucket><Name>{}</Name><CreationDate>{}</CreationDate></Bucket>",
            xml_escape(name),
            xml_escape(created)
        ));
    }
    body.push_str("</Buckets></ListAllMyBucketsResult>");
    body
}

/// ListObjectsV2 响应；含 Contents 与 CommonPrefixes，以及分页 token
pub fn list_objects_v2_xml(
    bucket: &str,
    prefix: &str,
    delimiter: &str,
    key_count: usize,
    max_keys: usize,
    is_truncated: bool,
    next_token: &str,
    contents: &[(String, &EntryMeta)],
    common_prefixes: &[String],
) -> String {
    let mut body = format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>{}</Name>
  <Prefix>{}</Prefix>
  <KeyCount>{}</KeyCount>
  <MaxKeys>{}</MaxKeys>
  <IsTruncated>{}</IsTruncated>
  <Delimiter>{}</Delimiter>"#,
        xml_escape(bucket),
        xml_escape(prefix),
        key_count,
        max_keys,
        is_truncated,
        xml_escape(delimiter)
    );
    if !next_token.is_empty() {
        body.push_str(&format!(
            "<NextContinuationToken>{}</NextContinuationToken>",
            xml_escape(next_token)
        ));
    }
    for (key, meta) in contents {
        body.push_str(&format!(
            "<Contents><Key>{}</Key><LastModified>{}</LastModified><ETag>\"{}\"</ETag><Size>{}</Size><StorageClass>STANDARD</StorageClass></Contents>",
            xml_escape(key),
            meta.mtime.to_rfc3339(),
            xml_escape(&meta.etag),
            meta.size
        ));
    }
    for p in common_prefixes {
        body.push_str(&format!(
            "<CommonPrefixes><Prefix>{}</Prefix></CommonPrefixes>",
            xml_escape(p)
        ));
    }
    body.push_str("</ListBucketResult>");
    body
}

/// InitiateMultipartUpload 成功响应
pub fn initiate_multipart_xml(bucket: &str, key: &str, upload_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<InitiateMultipartUploadResult>
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <UploadId>{}</UploadId>
</InitiateMultipartUploadResult>"#,
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(upload_id)
    )
}

/// CompleteMultipartUpload 成功响应
pub fn complete_multipart_xml(bucket: &str, key: &str, etag: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CompleteMultipartUploadResult>
  <Location>/{}/{}</Location>
  <Bucket>{}</Bucket>
  <Key>{}</Key>
  <ETag>\"{}\"</ETag>
</CompleteMultipartUploadResult>"#,
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(bucket),
        xml_escape(key),
        xml_escape(etag)
    )
}

/// CopyObject 结果 XML
pub fn copy_object_xml(etag: &str, last_modified: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<CopyObjectResult>
  <LastModified>{}</LastModified>
  <ETag>\"{}\"</ETag>
</CopyObjectResult>"#,
        xml_escape(last_modified),
        xml_escape(etag)
    )
}

/// 版本控制配置 XML；`enabled` 对应 Enabled / Suspended
pub fn versioning_xml(enabled: bool) -> String {
    let status = if enabled { "Enabled" } else { "Suspended" };
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>{status}</Status>
</VersioningConfiguration>"#
    )
}

/// DeleteObjects 批量删除结果；`errors` 为 (Key, Code, Message)
pub fn delete_objects_result_xml(deleted: &[String], errors: &[(String, &str, &str)]) -> String {
    let mut body = String::from(
        r#"<?xml version="1.0" encoding="UTF-8"?><DeleteResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#,
    );
    for k in deleted {
        body.push_str(&format!("<Deleted><Key>{}</Key></Deleted>", xml_escape(k)));
    }
    for (k, code, msg) in errors {
        body.push_str(&format!(
            "<Error><Key>{}</Key><Code>{}</Code><Message>{}</Message></Error>",
            xml_escape(k),
            xml_escape(code),
            xml_escape(msg)
        ));
    }
    body.push_str("</DeleteResult>");
    body
}

/// 解析 `<Delete><Object><Key>..</Key></Object>...</Delete>`，提取待删 key
pub fn parse_delete_objects_xml(body: &str) -> Vec<String> {
    let mut keys = Vec::new();
    let mut rest = body;
    while let Some(start) = rest.find("<Key>") {
        rest = &rest[start + 5..];
        if let Some(end) = rest.find("</Key>") {
            let key = rest[..end].trim();
            if !key.is_empty() {
                keys.push(xml_unescape(key));
            }
            rest = &rest[end + 6..];
        } else {
            break;
        }
    }
    keys
}

/// XML 实体反转义（顺序需先处理 &amp; 以外实体再处理 &amp;）
fn xml_unescape(s: &str) -> String {
    s.replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&quot;", "\"")
        .replace("&apos;", "'")
        .replace("&amp;", "&")
}
