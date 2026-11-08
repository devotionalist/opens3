> Language: [English](./gateways.md) · [中文](../gateways.md)

# Clients and gateways

Client protocols vary; the **unified Filer process** serves S3, HTTP `/entries`, WebDAV `/dav`, and the Admin UI together. Path metadata lives in a shared DB (Postgres/MySQL); object bytes live on Volume.

```text
  Clients (S3 SDK / FUSE / HTTP / WebDAV...)
                 │
                 ▼
           opens3 filer          single port (default 8333)
        S3 · /entries · /dav · Admin
                 │
         ┌───────┴────────┐
         ▼                ▼
      meta DB          Volume
   (DATABASE_URL)    (object bytes)
                 ▲
              Clerk
           assign/lookup
```

## Protocol comparison

| Protocol | Entry | Default port | Clients | Metadata |
|----------|-------|--------------|---------|----------|
| **S3** | `opens3 s3` (unified gateway) | 8333 | AWS SDK, aws-cli, s3fs, goofys, rclone | In-process FilerStore |
| **Filer HTTP** | Same process `/entries`, `/list` | 8333 | curl, custom HTTP, rclone HTTP | Same |
| **WebDAV** | Same process `/dav` | 8333 | davfs2, Cyberduck, macOS Finder | Same |
| **FUSE** | `opens3 fuse` → bootstraps mount | — | Prefer `mount.davfs`; or s3fs/goofys | — |

Admin UI: `http://<filer>:8333/admin/ui`.

CLI: `opens3 s3` = S3 + path APIs (production / Helm); `opens3 filer` = path APIs only (optional).

## Local one-shot (development)

```bash
# Terminal 1: control plane + data plane
opens3 clerk --port 9333 &
opens3 volume --port 8080 --clerk http://127.0.0.1:9333 --dir ./data/vol &

# Terminal 2: shared metadata DB + unified Filer (multiple replicas OK, same DATABASE_URL)
export DATABASE_URL=postgres://opens3:opens3@127.0.0.1:5432/opens3
opens3 s3 --port 8333 --clerk http://127.0.0.1:9333 --meta-backend=postgres
# Second replica (optional)
# opens3 s3 --port 8334 --clerk http://127.0.0.1:9333 --meta-backend=postgres
```

Single-node development can still use `opens3 server` or `--meta-dir` (redb). Production Helm defaults to Filer connecting directly to Postgres. `--filer-url` (HttpStore) is a compatibility path only; see [multi-filer.md](multi-filer.md).

## Client examples

### S3 SDK / CLI

```bash
export AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=...
aws --endpoint-url http://127.0.0.1:8333 s3 mb s3://demo
echo hi | aws --endpoint-url http://127.0.0.1:8333 s3 cp - s3://demo/hi.txt
```

Under the path API, S3 objects map to `/entries/buckets/<bucket>/<key>`.

### Filer HTTP

```bash
curl -T ./hi.txt http://127.0.0.1:8333/entries/demo/hi.txt
curl http://127.0.0.1:8333/entries/demo/hi.txt
curl http://127.0.0.1:8333/list/demo
```

### WebDAV

```bash
sudo mount -t davfs http://127.0.0.1:8333/dav /mnt/opens3
# or
opens3 fuse --filer http://127.0.0.1:8333 --mountpoint /mnt/opens3
```

### FUSE via S3

```bash
s3fs demo /mnt/opens3 -o url=http://127.0.0.1:8333,use_path_request_style
```

## Kubernetes / Helm

Helm deploys **one** `opens3-filer` Deployment (started with `opens3 s3`, Protocols::ALL):

- Service `opens3-filer` (primary exposure)
- Service `opens3-s3` (ClusterIP DNS alias to the same Pods)
- All config under `filer.*` (`port` / `credentials` / `metaBackend` / `service`)

See [Kubernetes](kubernetes.md).
