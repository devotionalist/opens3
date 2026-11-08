> 语言: [English](../en/multi-filer.md) · [中文](./multi-filer.md)

# 多 Filer 共享元数据

统一 Filer（`opens3 s3`）无状态，可水平扩展。单机 `redb` **不能**被多个进程安全共享。生产多实例请选共享库：

## 1. Postgres（推荐）

```bash
export DATABASE_URL=postgres://opens3:opens3@127.0.0.1:5432/opens3

# 多个统一 Filer 副本（同端口不同机，或同机不同端口）
opens3 s3 --meta-backend postgres --database-url "$DATABASE_URL" --port 8333 ...
opens3 s3 --meta-backend postgres --database-url "$DATABASE_URL" --port 8334 ...
```

表 `opens3_entries` / `opens3_kv` 自动迁移。Helm 默认捆绑 Postgres，或设 `filer.databaseUrl` 指向外部库。

## 2. MySQL / TiDB

TiDB 兼容 MySQL 协议，与 MySQL 共用同一后端（`--meta-backend mysql` 或 `tidb` 等价）。

```bash
export DATABASE_URL='mysql://opens3:opens3@127.0.0.1:3306/opens3'
# TiDB（默认 4000）
# export DATABASE_URL='mysql://root@127.0.0.1:4000/opens3'

opens3 s3 --meta-backend mysql --database-url "$DATABASE_URL" --clerk http://127.0.0.1:9333 --port 8333
```

库需事先创建；表结构启动时自动建。路径最长约 768 字符（InnoDB 主键前缀限制）。

Helm / 本地 k8s 示例：

```yaml
args:
  - s3
  - --port=8333
  - --clerk=http://opens3-clerk.opens3.svc:9333
  - --meta-backend=mysql
  - --database-url=mysql://opens3:opens3@mysql:3306/opens3
```

## 3. 兼容：单路径 Filer + HttpStore（不推荐生产）

一个 `opens3 filer`（仅路径 API）持有本地 redb；其他进程用 `--filer-url` 走 `/store/*`：

```bash
opens3 filer --port 8888 --meta-dir ./filer ...
opens3 s3 --port 8333 --filer-url http://127.0.0.1:8888 ...
```

对象数据仍由各网关直写 Volume；只有路径元数据走远程 Filer。生产请改用共享 Postgres。

验证：`bash tests/e2e_multi_s3.sh`
