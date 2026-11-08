> 语言: [中文](./kubernetes.md) · [English](en/kubernetes.md)

# Kubernetes 部署

OpenS3 提供两套交付物：

| 路径 | 用途 |
|------|------|
| [`deploy/kubernetes/`](../deploy/kubernetes/) | 原生 YAML + Kustomize |
| [`deploy/helm/opens3/`](../deploy/helm/opens3/) | Helm Chart（推荐） |

拓扑对应关系：

```text
StatefulSet opens3-clerk ×3     → Raft Clerk
StatefulSet opens3-volume ×N    → 每 Pod 一块 PVC（模拟多盘）
Deployment  opens3-filer ×N     → 统一网关：S3 + /entries + /dav + Admin
Deployment  opens3-postgres ×1  → 可选捆绑元数据库（filer.postgres）
```

客户端协议见 [gateways.md](gateways.md)。

## 前置条件

- Kubernetes 1.25+
- 默认 StorageClass（或在 values / ConfigMap 中指定）
- 构建并推送镜像，或导入本地集群：

```bash
# 构建
docker build -t opens3:0.1.0 .

# kind
kind load docker-image opens3:0.1.0

# Colima k3s
docker save opens3:0.1.0 | colima ssh -- sudo k3s ctr images import -

# 或推到私有仓库后改 image.repository / image.tag
```

## 方式一：Helm（推荐）

```bash
helm upgrade --install opens3 deploy/helm/opens3 \
  --namespace opens3 --create-namespace \
  --set image.repository=opens3 \
  --set image.tag=0.1.0 \
  --set filer.credentials.accessKey=admin \
  --set filer.credentials.secretKey='please-change-me' \
  --set defaultReplication=001 \
  --set volume.replicas=4
```

本地 / Colima 开发覆盖：

```bash
helm upgrade --install opens3 deploy/helm/opens3 -n opens3 --create-namespace \
  -f deploy/helm/opens3/values-dev.yaml
```

最小开发（关闭 Clerk PVC 需自行评估 Raft 风险；dev 默认已开 persistence）：

```bash
helm upgrade --install opens3 deploy/helm/opens3 -n opens3 --create-namespace \
  --set image.tag=0.1.0 \
  --set clerk.replicas=1 \
  --set volume.replicas=2 \
  --set volume.persistence.enabled=false \
  --set defaultReplication=000
```

启用 Ingress：

```bash
helm upgrade --install opens3 deploy/helm/opens3 -n opens3 \
  --set ingress.enabled=true \
  --set ingress.host=s3.example.com \
  --set image.tag=0.1.0
```

NodePort 暴露 Filer：

```bash
--set filer.service.type=NodePort --set filer.service.nodePort=30333
```

## 方式二：Kustomize / kubectl

```bash
# 按需改 deploy/kubernetes/configmap.yaml 中的 IMAGE / 副本相关字段
# 以及各 YAML 里的 image: opens3:latest

kubectl apply -k deploy/kubernetes/
kubectl -n opens3 get pods,svc,pvc
```

启用 Ingress：编辑 `kustomization.yaml` 取消注释 `ingress.yaml`。

## 验证

```bash
kubectl -n opens3 rollout status sts/opens3-clerk
kubectl -n opens3 rollout status sts/opens3-volume
kubectl -n opens3 rollout status deploy/opens3-filer

# Filer / S3 本地访问
kubectl -n opens3 port-forward svc/opens3-filer 8333:8333

export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY='please-change-me'
aws --endpoint-url http://127.0.0.1:8333 s3 mb s3://demo
echo hi | aws --endpoint-url http://127.0.0.1:8333 s3 cp - s3://demo/hi.txt
aws --endpoint-url http://127.0.0.1:8333 s3 ls s3://demo/

# 路径 API（同一端口）
curl http://127.0.0.1:8333/entries/buckets/demo/hi.txt

# Clerk / 运维
kubectl -n opens3 port-forward svc/opens3-clerk 9333:9333
curl -s http://127.0.0.1:9333/cluster/status | jq .
curl -s http://127.0.0.1:9333/raft/status | jq .
curl -X POST http://127.0.0.1:9333/cluster/heal
```

Admin UI：`kubectl port-forward svc/opens3-filer 8333:8333` 后打开 http://127.0.0.1:8333/admin/ui

## 设计说明

### Clerk Raft

- 3 副本 StatefulSet + Headless Service  
- 每 Pod `--advertise-url=http://<pod>.opens3-clerk-hl.<ns>.svc.cluster.local:9333`  
- `--peers` 列出全部 Clerk DNS（单副本时 peers 为空，立即单节点 leader）  
- Raft 日志落在 PVC `raft`（勿用 emptyDir）  

### Volume

- StatefulSet；`--ip=$(POD_IP)` 上报给 Clerk，集群内直连 Pod IP  
- `--node-id=$(POD_NAME)` 保证重启后身份稳定  
- 默认 `defaultReplication=001` 需要 ≥2 个 Volume Pod  
- Helm 声明的 volume ordinal（`0..replicas-1`）默认 **online**；可用 `volume.replicaOf` / `volume.nodes` 在部署时指定副本关系  
- Web「添加数据节点」扩出来的新 ordinal **无 bootstrap 文件 → offline**，在 Admin 设副本并上线  

### 统一 Filer

- 一个 Deployment：`opens3-filer`，容器 args 为 `s3`（Protocols::ALL）  
- Service `opens3-filer` 主暴露；`opens3-s3` 为 ClusterIP 别名  
- 共享库：`metaBackend=postgres|mysql|tidb`（Chart **不支持** 本地 redb）  
- 默认 Postgres：Chart 内附带 Postgres Deployment（可用 `filer.databaseUrl` 指向外部库，并设 `filer.postgres.enabled=false`）  
- 水平扩：`filer.replicas`  
- 鉴权：`filer.credentials` → Secret `*-filer-credentials`  

### 扩容

```bash
# 加 Volume 节点（新 PVC 自动创建）
kubectl -n opens3 scale sts/opens3-volume --replicas=6
# 或 helm
helm upgrade opens3 deploy/helm/opens3 -n opens3 --set volume.replicas=6 --reuse-values

# 加 Filer 副本
helm upgrade opens3 deploy/helm/opens3 -n opens3 --set filer.replicas=3 --reuse-values

# 触发 heal / balance（port-forward clerk 后）
curl -X POST http://127.0.0.1:9333/cluster/heal
curl -X POST http://127.0.0.1:9333/cluster/balance
```

Admin 拓扑：点 **Volume** 节点可上下线；点 **Filer** 节点可改 Clerk 分配策略。Filer 副本数用 Helm / kubectl 调整。

## Helm values 摘要

见 [`deploy/helm/opens3/values.yaml`](../deploy/helm/opens3/values.yaml)：

- `image.*`、`defaultReplication`、`dataCenter` / `rack`
- `clerk.replicas` / `persistence.size`
- `volume.replicas` / `maxVolumes` / `maxVolumeSizeMb`
- `filer.port` / `filer.replicas` / `filer.credentials` / `filer.metaBackend` / `filer.postgres.*` / `filer.databaseUrl`
- `filer.service.type` / `filer.service.nodePort` / `ingress`

## 卸载

```bash
helm uninstall opens3 -n opens3
# PVC 默认保留；确认无数据需求后再删
kubectl -n opens3 delete pvc --all
kubectl delete ns opens3
```

## 监控

```bash
# ServiceMonitor + PrometheusRule + Grafana dashboard ConfigMap
helm upgrade opens3 deploy/helm/opens3 -n opens3 --reuse-values \
  --set serviceMonitor.enabled=true \
  --set serviceMonitor.labels.release=kube-prometheus-stack \
  --set prometheusRule.enabled=true \
  --set prometheusRule.labels.release=kube-prometheus-stack \
  --set grafanaDashboard.enabled=true
```

指标、Grafana 看板与告警：见 [监控指南](monitoring.md)。产物位于 `deploy/monitoring/` 与 Helm `templates/`。

## 故障排查（K8s）

| 现象 | 排查 |
|------|------|
| Clerk 一直未 ready | `kubectl logs sts/opens3-clerk`；检查 `/ready` 与 peers DNS；Raft PVC |
| Volume 无心跳 | Clerk Service DNS；Volume 日志里 heartbeat 错误 |
| Filer Pending / Crash | 镜像、Postgres 就绪、`DATABASE_URL`；`kubectl logs deploy/opens3-filer` |
| 写失败 / 无可用卷 | Volume 是否已在 Admin **上线**；replication 与节点数是否匹配 |
| 跨节点读失败 | NetworkPolicy 是否放行 Volume Pod IP:8080 |
| 镜像拉不下来 | `imagePullPolicy` / kind load / 私有仓库密钥 |

更多通用问题见 [故障排查](troubleshooting.md)。
