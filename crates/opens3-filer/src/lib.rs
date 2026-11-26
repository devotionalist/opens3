// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! OpenS3 网关：Filer HTTP/WebDAV 与 S3 共用同一端口。

pub mod acl; // 罐头 ACL 存储与判定
pub mod admin_cluster; // 管理面 / 集群运维 API
pub mod advanced; // Range、版本列表、CORS/标签等高级 S3 辅助
pub mod auth; // SigV4 鉴权与 AccessKey 存储
pub mod client; // Filer 客户端（读/写/列举）
pub mod encryption; // SSE-S3 风格 AES-GCM 加解密
pub mod error_xml; // S3 错误 XML 与转义
pub mod events; // Filer 事件总线与 webhook
pub mod http_filer; // Filer/WebDAV HTTP 路由
pub mod http_s3; // S3 兼容 HTTP 路由
pub mod lifecycle; // Bucket lifecycle 过期
pub mod lock; // Object Lock / 保留策略
pub mod policy; // IAM / bucket policy 求值
pub mod server; // Gateway / FilerServer / S3Server 组装
pub mod xml; // S3 响应 XML 构造

pub use server::{FilerServer, Gateway, Protocols, S3Server, Server}; // 对外导出网关入口类型
