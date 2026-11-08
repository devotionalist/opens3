> Language: [English](./configuration.md) · [中文](../zh/configuration.md)

# Configuration reference

OpenS3 is configured via **CLI flags + environment variables** (no mandatory standalone config file).

## Environment variables

| Variable | Purpose |
|----------|---------|
| `OPENS3_ACCESS_KEY` | S3 Access Key (SigV4 enabled only when Secret is also set) |
| `OPENS3_SECRET_KEY` | S3 Secret Key |
| `OPENS3_VOLUME_JWT_SECRET` | Shared by Clerk/Volume/Filer/S3; when set, assign issues JWT and Volume write/delete require Bearer |
| `OPENS3_TLS_CERT` / `OPENS3_TLS_KEY` | PEM cert and key; in-process HTTPS |
| `OPENS3_KMS_KEY` | Local SSE-KMS passphrase |
| `OPENS3_CLERK_PEERS` | Volume multi-Clerk follow list |
| `OPENS3_META_BACKEND` | `redb` / `memory` / `postgres` / `mysql` / `tidb` / `http` |
| `DATABASE_URL` | Postgres meta |
| `RUST_LOG` | Log level, e.g. `info`, `opens3_volume=debug` |

## `opens3 server` (all-in-one)

| Flag | Default | Description |
|------|---------|-------------|
| `--dir` | `./data` | Data root (volumes/filer/raft) |
| `--clerk-port` | `9333` | Clerk |
| `--volume-port` | `8080` | Volume HTTP |
| `--gateway-port` | `8333` | Unified Filer (S3 + /entries + /dav) |
| `--default-replication` | `000` | Default replication code |
| `--max-volume-size-mb` | (internal default ~30GiB) | Per-volume cap; lower for development |
| `--access-key` / `--secret-key` | none | Alternative to env |

## `opens3 clerk`

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `9333` | Listen port |
| `--ip` | `clerk-1` | Forms node_id: `{ip}:{port}` |
| `--default-replication` | `000` | Default replication |
| `--peers` | empty | Comma-separated Clerk URLs, Raft members |
| `--raft-dir` | `./data/raft` | Persist term/vote/log |
| `--advertise-url` | empty (default `http://127.0.0.1:port`) | Address peers can reach; K8s uses Pod DNS |
| `--jwt-secret` | none | Issue Volume write tokens |
| `--tls-cert` / `--tls-key` | none | HTTPS |

## `opens3 volume`

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `8080` | HTTP; gRPC is `port+10000` |
| `--ip` | `127.0.0.1` | Address reported to Clerk |
| `--dir` | `./data` | Volume directory (`.dat`/`.idx`) |
| `--clerk` | `http://127.0.0.1:9333` | Clerk |
| `--clerk-peers` | empty | Multi-Clerk follow (or `OPENS3_CLERK_PEERS`) |
| `OPENS3_VOLUME_SYNC` | `data` | Volume fsync: `none` / `data` / `all` |
| `OPENS3_VOLUME_IDX_FLUSH_EVERY` | `64` | Flush idx/dat every N changes (`0`=explicit/Drop only) |
| `--data-center` | `dc1` | Topology DC |
| `--rack` | `rack1` | Topology rack |
| `--max-volumes` | `1` | Max volumes this node may host; Clerk heartbeats pre-create up to this cap |
| `--max-volume-size-mb` | none | Override default volume size |
| `--node-id` | `{ip}:{port}` | Unique in the cluster |
| `--jwt-secret` | none | Validate write/delete Bearer |
| `--tls-cert` / `--tls-key` | none | HTTPS (`publicUrl` becomes https) |

## `opens3 s3` / `opens3 filer`

| Flag | Default | Description |
|------|---------|-------------|
| `--port` | `8333` | Listen port (`s3`=unified gateway; `filer`=path API only) |
| `--clerk` | `http://127.0.0.1:9333` | Clerk |
| `--clerk-peers` | empty | Clerk candidates (or `OPENS3_CLERK_PEERS`); pins to `leader` URL after 307 so redirects do not loop |
| `--default-replication` | `000` | Replication used on assign for writes |
| `--meta-dir` | `./filer` | redb metadata dir (single node; do not share across replicas) |
| `--memory` | false | Equivalent to `--meta-backend memory` |
| `--meta-backend` | `redb` | `redb` / `memory` / `postgres` / `mysql` / `tidb` / `http` |
| `--database-url` | none | Postgres / MySQL / TiDB URL (or `DATABASE_URL`) |
| `--filer-url` | none | Compat: HttpStore remote `/store` (`s3` only) |
| `--encrypt` | none | Passphrase-derived AES-256-GCM at-rest encryption |
| `--jwt-secret` | none | Forward/issue delete tokens to Volume |
| `--kms-key` | none | Local SSE-KMS (`s3`) |
| `--tls-cert` / `--tls-key` | none | HTTPS |

Helm injects `filer.port` / `filer.credentials` / `filer.metaBackend` into the unified Filer Deployment.

Multi-instance: [multi-filer.md](multi-filer.md).

## On-disk layout

```text
{volume-dir}/
  {volumeId}.dat
  {volumeId}.idx
  ec/{volumeId}.ec.{shard}

{meta-dir}/
  … (FilerStore implementation–specific)

{raft-dir}/
  openraft-log.wal / openraft-sm.json / openraft-snapshot.bin …
```
