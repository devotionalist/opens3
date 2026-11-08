> Language: [English](./quickstart.md) · [中文](../quickstart.md)

# Quick start

## 1. Build

```bash
git clone <repo-url> opens3
cd opens3
cargo build --release
export PATH="$PWD/target/release:$PATH"
```

## 2. One-command start (dev / single node)

```bash
mkdir -p ./data
opens3 server --dir ./data --max-volume-size-mb 64
```

Default ports:

| Service | Address |
|---------|---------|
| Clerk | `http://127.0.0.1:9333` |
| Volume | `http://127.0.0.1:8080` |
| S3 / Admin | `http://127.0.0.1:8333` |

Health checks:

```bash
curl -sf http://127.0.0.1:8333/health
curl -sf http://127.0.0.1:9333/ready
open http://127.0.0.1:8333/admin/ui   # or open in a browser
```

## 3. Access S3 with curl (no-auth development mode)

When `OPENS3_ACCESS_KEY` / `OPENS3_SECRET_KEY` are unset, the gateway allows requests as root for local debugging.

```bash
# Create bucket
curl -X PUT http://127.0.0.1:8333/demo

# Upload
echo 'hello opens3' | curl -X PUT http://127.0.0.1:8333/demo/hello.txt --data-binary @-

# Download
curl http://127.0.0.1:8333/demo/hello.txt

# List
curl 'http://127.0.0.1:8333/demo?list-type=2'
```

## 4. Use AWS CLI

```bash
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=secretsecret
# Start with credentials first: see “Enable auth” below

aws --endpoint-url http://127.0.0.1:8333 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:8333 s3 cp ./file.bin s3://demo/
aws --endpoint-url http://127.0.0.1:8333 s3 ls s3://demo/
```

### Restart with auth enabled

```bash
export OPENS3_ACCESS_KEY=admin
export OPENS3_SECRET_KEY=secretsecret
opens3 server --dir ./data --max-volume-size-mb 64
```

## 5. Shell probes

```bash
opens3 shell cluster-status --clerk http://127.0.0.1:9333
opens3 shell assign --clerk http://127.0.0.1:9333
opens3 shell raft-status --clerk http://127.0.0.1:9333
```

Next: [Multi-node deployment](deployment.md) · [S3 details](s3.md) · [Operations](operations.md)
