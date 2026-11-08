> Language: [English](./README.md) · [中文](../zh/README.md)

# OpenS3 Documentation

OpenS3 is a distributed object store written in Rust: Clerk (Raft) assigns volumes, Volume appends needles with replication, and a unified Filer gateway (S3 + HTTP/WebDAV) shares metadata.

## Getting started & operations

| Document | Description |
|----------|-------------|
| [Quick start](quickstart.md) | Run all-in-one and aws-cli in five minutes |
| [Install & build](install.md) | Dependencies, compile, release artifacts |
| [Deployment](deployment.md) | Single node, multi-Volume, Clerk HA |
| [Kubernetes](kubernetes.md) | Helm / Kustomize / scale-out and verification |
| [Configuration](configuration.md) | CLI flags and environment variables |
| [Operations](operations.md) | heal, balance, metrics, EC, compact |
| [Monitoring](monitoring.md) | Prometheus metrics, Grafana, alerts, ServiceMonitor |

## Architecture & modules

| Document | Description |
|----------|-------------|
| [Architecture overview](architecture.md) | System diagram, data model, read/write paths, crate deps |
| [Module docs](modules/README.md) | Per-crate responsibilities and internal design |
| → [Clerk](modules/clerk.md) | Topology, assign, Raft, heal/balance |
| → [Volume](modules/volume.md) | Needle, replication, compact, EC |
| → [Needle](modules/needle.md) | `.dat` / `.idx` byte layout |
| → [Filer](modules/filer.md) | Unified gateway: S3 + path API, multipart, encryption |
| → [S3](modules/s3.md) | S3 routing, SigV4, policies, lifecycle (same Filer crate) |
| → [Store](modules/store.md) | FilerStore: redb / postgres / mysql·tidb / http |
| → [Common](modules/common.md) / [Proto](modules/proto.md) / [CLI](modules/cli.md) | Shared types, gRPC, entrypoints |
| [Clients & gateways](gateways.md) | Protocol entrypoints and Helm unified Filer |

## S3 & development

| Document | Description |
|----------|-------------|
| [Using S3](s3.md) | API, auth, policies, lifecycle, versioning |
| [S3 compatibility matrix](s3-compat.md) | API coverage and smoke scripts |
| [Multi-instance metadata](multi-filer.md) | Postgres / MySQL multi-Filer replicas |
| [Development & testing](development.md) | Repo layout, tests, local loop |
| [Contributing](contributing.md) | PR-only workflow, owner approval, tags/releases |
| [Test coverage report](test-coverage.md) | Unit/E2E checklist and recent green runs |

License: [Apache License 2.0](../../LICENSE)
