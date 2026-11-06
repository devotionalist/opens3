// Copyright 2026 OpenS3 Contributors
// SPDX-License-Identifier: Apache-2.0
//! OpenS3 gateway: Filer HTTP/WebDAV + S3 on one port.

pub mod acl;
pub mod admin_cluster;
pub mod advanced;
pub mod auth;
pub mod client;
pub mod encryption;
pub mod error_xml;
pub mod events;
pub mod http_filer;
pub mod http_s3;
pub mod lifecycle;
pub mod lock;
pub mod policy;
pub mod server;
pub mod xml;

pub use server::{FilerServer, Gateway, Protocols, S3Server, Server};