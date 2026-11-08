> 语言: [English](../en/deployment.md) · [中文](./deployment.md)

# 部署

## 拓扑建议

- 每个物理机可跑多个 Volume 进程（每盘一个 `--dir`），便于 `001`/`010` 区分故障域  
- `--data-center` / `--rack` 必须与真实拓扑一致，否则副本放置语义错误  
- Clerk 生产部署 **3 节点 Raft**；Volume / Filer 无状态可水平扩展（Filer 需共享元数据库）  

## 模式 A：单机 all-in-one

适合开发与 PoC：

```bash
opens3 server \
  --dir /data/opens3 \
  --clerk-port 9333 \
  --volume-port 8080 \
  --gateway-port 8333 \
  --default-replication 000 \
  --max-volume-size-mb 1024
```

同一端口提供 S3、`/entries`、`/dav`、Admin。

## 模式 B：分离进程（推荐入门生产）

### 1. Clerk

```bash
opens3 clerk \
  --ip 10.0.0.1 \
  --port 9333 \
  --default-replication 001 \
  --raft-dir /data/opens3/raft-1 \
  --peers http://10.0.0.1:9333,http://10.0.0.2:9333,http://10.0.0.3:9333
```

在另外两台用不同 `--raft-dir`、相同 `--peers` 启动。只有 **leader** 接受 `assign` / `heartbeat`；follower 返回 redirect（`leader` 字段）。

### 2. Volume（每盘一例）

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

再开一块盘：换 `--dir` / `--port` / `--node-id`。默认 offline（或 `--start-online` / Helm bootstrap）；Admin 也可上线。K8s 部署见 [kubernetes.md](kubernetes.md)。

### 3. 统一 Filer（可多实例）

```bash
export OPENS3_ACCESS_KEY=admin
export OPENS3_SECRET_KEY='请换成足够长的密钥'
export DATABASE_URL=postgres://opens3:opens3@10.0.0.5:5432/opens3

opens3 s3 \
  --port 8333 \
  --clerk http://10.0.0.1:9333 \
  --meta-backend postgres \
  --default-replication 001
```

`opens3 s3` 在同一端口提供 S3 + `/entries` + `/dav` + Admin。多副本必须共用可共享 FilerStore（Postgres / MySQL / TiDB）；**不要**多进程抢同一本地 redb。详见 [multi-filer.md](multi-filer.md)。

仅路径 API（无 S3）时可用 `opens3 filer`；生产与 Helm 使用 `opens3 s3`。

## 副本策略选择

| 场景 | 推荐 | 含义 |
|------|------|------|
| 单机试验 | `000` | 1 副本 |
| 同机架多盘 | `001` | 2 副本，同 rack 不同节点 |
| 跨机架 | `010` | 2 副本，同 DC 不同 rack |
| 更高冗余 | `011` / `002` | 按拓扑与盘数评估 |

写时要求 **全部副本成功**（强一致写）。

## 反向代理 / 负载均衡

- Filer：对多个 `opens3 s3`（统一网关）做 L4/L7 LB，透传 `Authorization`、`x-amz-*` 头，勿改写签名相关头  
- Volume：一般不经 LB；由 Clerk lookup 返回直连 URL（`--public-url` 语义由 volume 的 `public_url` 体现）  

## Kubernetes

生产/测试集群请使用 Helm 或 Kustomize，详见 **[Kubernetes 部署](kubernetes.md)**。

```bash
docker build -t opens3:0.1.0 .
helm upgrade --install opens3 deploy/helm/opens3 -n opens3 --create-namespace \
  --set image.tag=0.1.0 \
  --set filer.credentials.secretKey='please-change-me'
```

## systemd 示例（Volume）

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
