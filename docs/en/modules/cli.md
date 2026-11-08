> Language: [English](./cli.md) · [中文](../../zh/modules/cli.md)

# CLI (`opens3`)

## Role

Binary entry crate that wires service crates into runnable processes. It does not implement storage logic; it only:

- Parses arguments (`clap`)
- Selects a metadata backend and builds the unified `Server` (Filer)
- Starts a single component, or **all-in-one** (`server`) with Clerk + Volume + Filer in one process
- Provides a simple shell (assign / lookup and other ops commands)

## Location

`crates/opens3/src/main.rs`

## Subcommands

| Command | Role |
|---------|------|
| `clerk` | Start Clerk (Raft dir, peers, advertise-url) |
| `volume` | Start Volume (data dir, topology labels, max-volumes) |
| `s3` | **Unified Filer**: S3 + `/entries` + `/dav` + Admin (Protocols::ALL) |
| `filer` | Path API / WebDAV only (Protocols::FILER) |
| `server` | Local development: Clerk + Volume + unified Filer in one process |
| `shell` | Lightweight ops: assign, lookup, etc. |
| `fuse` | Placeholder (guides S3/WebDAV mounts) |

## Design notes

- **advertise-url**: Under a K8s StatefulSet, Raft needs a stable DNS name; do not rely on `127.0.0.1` alone.  
- **meta-backend**: `redb` | `memory` | `postgres` | `mysql` | `tidb` | `http` (shared metadata: [multi-filer](../multi-filer.md)).  
- Helm / production runs the unified gateway via `opens3 s3`; use `opens3 filer` for path-only scenarios.  
- all-in-one suits development and smoke tests; production deploys components as separate processes / Pods.

## Dependencies

`opens3-clerk`, `opens3-volume`, `opens3-filer`, `opens3-store`, `opens3-common`
