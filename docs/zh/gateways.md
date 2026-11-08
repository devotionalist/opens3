> 语言: [English](../en/gateways.md) · [中文](./gateways.md)

# 客户端与网关

客户端协议多样；**统一 Filer 进程**同时提供 S3、HTTP `/entries`、WebDAV `/dav` 与 Admin UI。路径元数据在共享库（Postgres/MySQL），对象字节在 Volume。

```text
  Clients（S3 SDK / FUSE / HTTP / WebDAV…）
                 │
                 ▼
           opens3 filer          单端口（默认 8333）
        S3 · /entries · /dav · Admin
                 │
         ┌───────┴────────┐
         ▼                ▼
      meta DB          Volume
   （DATABASE_URL）    （对象字节）
                 ▲
              Clerk
           assign/lookup
```

## 协议对照

| 协议 | 入口 | 默认端口 | 客户端 | 元数据 |
|------|------|----------|--------|--------|
| **S3** | `opens3 s3`（统一网关） | 8333 | AWS SDK、aws-cli、s3fs、goofys、rclone | 进程内 FilerStore |
| **Filer HTTP** | 同进程 `/entries`、`/list` | 8333 | curl、自研 HTTP、rclone HTTP | 同上 |
| **WebDAV** | 同进程 `/dav` | 8333 | davfs2、Cyberduck、macOS Finder | 同上 |
| **FUSE** | `opens3 fuse` → 引导挂载 | — | 优先 `mount.davfs`；或 s3fs/goofys | — |

Admin UI：`http://<filer>:8333/admin/ui`。

CLI：`opens3 s3` = S3 + 路径 API（生产 / Helm）；`opens3 filer` = 仅路径 API（可选）。

## 本地一键（开发）

```bash
# 终端 1：控制面 + 数据面
opens3 clerk --port 9333 &
opens3 volume --port 8080 --clerk http://127.0.0.1:9333 --dir ./data/vol &

# 终端 2：共享元数据库 + 统一 Filer（可起多个副本，同一 DATABASE_URL）
export DATABASE_URL=postgres://opens3:opens3@127.0.0.1:5432/opens3
opens3 s3 --port 8333 --clerk http://127.0.0.1:9333 --meta-backend=postgres
# 第二副本（可选）
# opens3 s3 --port 8334 --clerk http://127.0.0.1:9333 --meta-backend=postgres
```

单机开发仍可用 `opens3 server` 或 `--meta-dir`（redb）。生产 Helm 默认 Filer 直连 Postgres。`--filer-url`（HttpStore）仅作兼容路径，见 [multi-filer.md](multi-filer.md)。

## 客户端示例

### S3 SDK / CLI

```bash
export AWS_ACCESS_KEY_ID=admin AWS_SECRET_ACCESS_KEY=...
aws --endpoint-url http://127.0.0.1:8333 s3 mb s3://demo
echo hi | aws --endpoint-url http://127.0.0.1:8333 s3 cp - s3://demo/hi.txt
```

S3 对象在路径 API 下对应 `/entries/buckets/<bucket>/<key>`。

### Filer HTTP

```bash
curl -T ./hi.txt http://127.0.0.1:8333/entries/demo/hi.txt
curl http://127.0.0.1:8333/entries/demo/hi.txt
curl http://127.0.0.1:8333/list/demo
```

### WebDAV

```bash
sudo mount -t davfs http://127.0.0.1:8333/dav /mnt/opens3
# 或
opens3 fuse --filer http://127.0.0.1:8333 --mountpoint /mnt/opens3
```

### FUSE via S3

```bash
s3fs demo /mnt/opens3 -o url=http://127.0.0.1:8333,use_path_request_style
```

## Kubernetes / Helm

Helm 部署 **一个** `opens3-filer` Deployment（`opens3 s3` 启动，Protocols::ALL）：

- Service `opens3-filer`（主暴露）
- Service `opens3-s3`（ClusterIP DNS 别名，指向同一批 Pod）
- 配置全部在 `filer.*`（`port` / `credentials` / `metaBackend` / `service`）

详见 [Kubernetes](kubernetes.md)。
