// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
use opens3_common::Error;

pub fn s3_error_xml(err: &Error, resource: &str, request_id: &str) -> String {
    format!(
        r#"<?xml version="1.0" encoding="UTF-8"?>
<Error>
  <Code>{}</Code>
  <Message>{}</Message>
  <Resource>{}</Resource>
  <RequestId>{}</RequestId>
</Error>"#,
        err.s3_code(),
        xml_escape(&err.to_string()),
        xml_escape(resource),
        request_id
    )
}

pub fn xml_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
