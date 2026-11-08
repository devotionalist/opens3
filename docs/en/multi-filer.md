> Language: [English](./multi-filer.md) · [中文](../multi-filer.md)

# Multi-Filer shared metadata

The unified Filer (`opens3 s3`) is stateless and horizontally scalable. Single-node `redb` **cannot** be safely shared by multiple processes. For production multi-instance setups, use a shared database:

## 1. Postgres (recommended)

```bash
export DATABASE_URL=postgres://opens3:opens3@127.0.0.1:5432/opens3

# Multiple unified Filer replicas (same port on different hosts, or different ports on one host)
opens3 s3 --meta-backend postgres --database-url "$DATABASE_URL" --port 8333 ...
opens3 s3 --meta-backend postgres --database-url "$DATABASE_URL" --port 8334 ...
```

Tables `opens3_entries` / `opens3_kv` migrate automatically. Helm bundles Postgres by default, or set `filer.databaseUrl` to an external DB.

## 2. MySQL / TiDB

TiDB speaks the MySQL protocol and shares the same backend as MySQL (`--meta-backend mysql` or `tidb` are equivalent).

```bash
export DATABASE_URL='mysql://opens3:opens3@127.0.0.1:3306/opens3'
# TiDB (default port 4000)
# export DATABASE_URL='mysql://root@127.0.0.1:4000/opens3'

opens3 s3 --meta-backend mysql --database-url "$DATABASE_URL" --clerk http://127.0.0.1:9333 --port 8333
```

Create the database beforehand; tables are created automatically on startup. Paths are limited to about 768 characters (InnoDB primary-key prefix limit).

Helm / local k8s example:

```yaml
args:
  - s3
  - --port=8333
  - --clerk=http://opens3-clerk.opens3.svc:9333
  - --meta-backend=mysql
  - --database-url=mysql://opens3:opens3@mysql:3306/opens3
```

## 3. Compatibility: single path Filer + HttpStore (not recommended for production)

One `opens3 filer` (path APIs only) holds local redb; other processes use `--filer-url` against `/store/*`:

```bash
opens3 filer --port 8888 --meta-dir ./filer ...
opens3 s3 --port 8333 --filer-url http://127.0.0.1:8888 ...
```

Object data is still written directly to Volume by each gateway; only path metadata goes through the remote Filer. Prefer shared Postgres in production.

Verify: `bash tests/e2e_multi_s3.sh`
