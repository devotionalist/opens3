> Language: [English](./deployment.md) · [中文](../zh/deployment.md)

# Deployment

## Topology recommendations

- Run multiple Volume processes per physical host (one `--dir` per disk) so `001`/`010` can distinguish failure domains  
- `--data-center` / `--rack` must match real topology; otherwise replica placement semantics are wrong  
- Production Clerk: **3-node Raft**; Volume / Filer are stateless and horizontally scalable (Filer needs a shared metadata DB)  

## Mode A: Single-node all-in-one

Suitable for development and PoC:

```bash
opens3 server \
  --dir /data/opens3 \
  --clerk-port 9333 \
  --volume-port 8080 \
  --gateway-port 8333 \
  --default-replication 000 \
  --max-volume-size-mb 1024
```

The same port serves S3, `/entries`, `/dav`, and Admin.

## Mode B: Separated processes (recommended for production intro)

### 1. Clerk

```bash
opens3 clerk \
  --ip 10.0.0.1 \
  --port 9333 \
  --default-replication 001 \
  --raft-dir /data/opens3/raft-1 \
  --peers http://10.0.0.1:9333,http://10.0.0.2:9333,http://10.0.0.3:9333
```

Start the other two hosts with different `--raft-dir` and the same `--peers`. Only the **leader** accepts `assign` / `heartbeat`; followers return a redirect (`leader` field).

### 2. Volume (one example per disk)

```bash
opens3 volume \
  --ip 10.0.1.10 \
  --port 8080 \
  --dir /data/disk1/opens3 \
  --clerk http://10.0.0.1:9333 \
  --data-center dc1 \
  --rack rackA \
  --node-id vs-10-disk1 \
  --max-volumes 100 \
  --max-volume-size-mb 30720
```

Add another disk: change `--dir` / `--port` / `--node-id`. Default is offline (or use `--start-online` / Helm bootstrap); Admin can also bring nodes online. For K8s see [kubernetes.md](kubernetes.md).

### 3. Unified Filer (can be multi-instance)

```bash
export OPENS3_ACCESS_KEY=admin
export OPENS3_SECRET_KEY='replace-with-a-long-enough-secret'
export DATABASE_URL=postgres://opens3:opens3@10.0.0.5:5432/opens3

opens3 s3 \
  --port 8333 \
  --clerk http://10.0.0.1:9333 \
  --meta-backend postgres \
  --default-replication 001
```

`opens3 s3` serves S3 + `/entries` + `/dav` + Admin on the same port. Multiple replicas must share a shareable FilerStore (Postgres / MySQL / TiDB); **do not** let multiple processes contend for the same local redb. See [multi-filer.md](multi-filer.md).

For path API only (no S3), use `opens3 filer`; production and Helm use `opens3 s3`.

## Choosing a replication policy

| Scenario | Recommended | Meaning |
|----------|-------------|---------|
| Single-node experiment | `000` | 1 replica |
| Multi-disk same rack | `001` | 2 replicas, same rack, different nodes |
| Cross-rack | `010` | 2 replicas, same DC, different racks |
| Higher redundancy | `011` / `002` | Evaluate by topology and disk count |

Writes require **all replicas to succeed** (strongly consistent write).

## Reverse proxy / load balancing

- Filer: L4/L7 LB across multiple `opens3 s3` (unified gateway) instances; pass through `Authorization` and `x-amz-*` headers; do not rewrite signature-related headers  
- Volume: usually not behind LB; Clerk lookup returns direct URLs (`--public-url` semantics surface as volume `public_url`)  

## Kubernetes

For production/test clusters use Helm or Kustomize; see **[Kubernetes deployment](kubernetes.md)**.

```bash
# Prefer the published image (or build locally — see kubernetes.md)
helm upgrade --install opens3 deploy/helm/opens3 -n opens3 --create-namespace \
  --set image.repository=jinopen/opens3 \
  --set image.tag=0.1.0 \
  --set filer.credentials.secretKey='please-change-me'
```

## systemd example (Volume)

```ini
[Unit]
Description=OpenS3 Volume
After=network.target

[Service]
ExecStart=/usr/local/bin/opens3 volume --ip 10.0.1.10 --port 8080 --dir /data/disk1/opens3 --clerk http://10.0.0.1:9333 --data-center dc1 --rack rackA --node-id vs-10-d1 --max-volumes 100
Restart=on-failure
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
```
