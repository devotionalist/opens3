> Language: [English](./kubernetes.md) · [中文](../zh/kubernetes.md)

# Kubernetes deployment

OpenS3 ships two delivery packs:

| Path | Purpose |
|------|---------|
| [`deploy/kubernetes/`](../../deploy/kubernetes/) | Raw YAML + Kustomize |
| [`deploy/helm/opens3/`](../../deploy/helm/opens3/) | Helm Chart (recommended) |

Topology mapping:

```text
StatefulSet opens3-clerk ×3     → Raft Clerk
StatefulSet opens3-volume ×N    → one hostPath (default) or PVC per Pod
Deployment  opens3-filer ×N     → unified gateway: S3 + /entries + /dav + Admin
Deployment  opens3-postgres ×1  → optional bundled metadata DB (filer.postgres)
```

Client protocols: [gateways.md](gateways.md).

## Prerequisites

- Kubernetes 1.25+
- Disk on the node for hostPath (default root `/var/lib/opens3`), **or** a StorageClass when using `persistence.type=pvc`
- Container image: pull from Docker Hub, or build locally

**Recommended (Docker Hub):**

```bash
# Chart default: jinopen/opens3
docker pull jinopen/opens3:0.1.0
```

**Local build / air-gapped import:**

```bash
docker build -t opens3:0.1.0 .

# kind
kind load docker-image opens3:0.1.0

# Colima k3s
docker save opens3:0.1.0 | colima ssh -- sudo k3s ctr images import -
```

Clerk / Volume / Filer share **one** image; Helm selects the role via container `args`. See [Install · Docker image](install.md#docker-image).

### Persistence (default: hostPath)

Chart / raw manifests default to **hostPath** (no StorageClass required):

| Component | Path pattern |
|-----------|--------------|
| Clerk | `/var/lib/opens3/<ns>/<release>/clerk/<pod-name>` |
| Volume | `/var/lib/opens3/<ns>/<release>/volume/<pod-name>` |
| Postgres | `/var/lib/opens3/<ns>/<release>/postgres` |

Best for Colima / kind / single-node. Data stays on the node that ran the Pod — avoid frequent rescheduling across nodes.

Switch to PVC + StorageClass for multi-node production:

```bash
helm upgrade --install opens3 deploy/helm/opens3 -n opens3 --create-namespace \
  --set persistence.type=pvc \
  --set clerk.persistence.storageClass=fast-ssd \
  --set volume.persistence.storageClass=fast-ssd \
  --set filer.postgres.persistence.storageClass=fast-ssd
```

## Option 1: Helm (recommended)

Pull from Docker Hub (default repository; hostPath persistence):

```bash
helm upgrade --install opens3 deploy/helm/opens3 \
  --namespace opens3 --create-namespace \
  --set image.repository=jinopen/opens3 \
  --set image.tag=0.1.0 \
  --set image.pullPolicy=IfNotPresent \
  --set filer.credentials.accessKey=admin \
  --set filer.credentials.secretKey='please-change-me' \
  --set defaultReplication=001 \
  --set volume.replicas=4
```

Local / Colima development overlay (uses `opens3:0.1.0` + `pullPolicy: Never`, still hostPath):

```bash
# build + import first, then:
helm upgrade --install opens3 deploy/helm/opens3 -n opens3 --create-namespace \
  -f deploy/helm/opens3/values-dev.yaml
```

Minimal development (disabling persistence has Raft risk you must accept):

```bash
helm upgrade --install opens3 deploy/helm/opens3 -n opens3 --create-namespace \
  --set image.tag=0.1.0 \
  --set clerk.replicas=1 \
  --set volume.replicas=2 \
  --set volume.persistence.enabled=false \
  --set defaultReplication=000
```

Enable Ingress:

```bash
helm upgrade --install opens3 deploy/helm/opens3 -n opens3 \
  --set ingress.enabled=true \
  --set ingress.host=s3.example.com \
  --set image.tag=0.1.0
```

Expose Filer via NodePort:

```bash
--set filer.service.type=NodePort --set filer.service.nodePort=30333
```

## Option 2: Kustomize / kubectl

```bash
# Adjust IMAGE / replica-related fields in deploy/kubernetes/configmap.yaml as needed
# and image: opens3:latest in the YAML files

kubectl apply -k deploy/kubernetes/
kubectl -n opens3 get pods,svc
# hostPath data: /var/lib/opens3/opens3/opens3/{clerk,volume,postgres}
```


Enable Ingress: edit `kustomization.yaml` and uncomment `ingress.yaml`.

## Verification

```bash
kubectl -n opens3 rollout status sts/opens3-clerk
kubectl -n opens3 rollout status sts/opens3-volume
kubectl -n opens3 rollout status deploy/opens3-filer

# Local access to Filer / S3
kubectl -n opens3 port-forward svc/opens3-filer 8333:8333

export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY='please-change-me'
aws --endpoint-url http://127.0.0.1:8333 s3 mb s3://demo
echo hi | aws --endpoint-url http://127.0.0.1:8333 s3 cp - s3://demo/hi.txt
aws --endpoint-url http://127.0.0.1:8333 s3 ls s3://demo/

# Path API (same port)
curl http://127.0.0.1:8333/entries/buckets/demo/hi.txt

# Clerk / ops
kubectl -n opens3 port-forward svc/opens3-clerk 9333:9333
curl -s http://127.0.0.1:9333/cluster/status | jq .
curl -s http://127.0.0.1:9333/raft/status | jq .
curl -X POST http://127.0.0.1:9333/cluster/heal
```

Admin UI: after `kubectl port-forward svc/opens3-filer 8333:8333`, open http://127.0.0.1:8333/admin/ui

## Design notes

### Clerk Raft

- 3-replica StatefulSet + Headless Service  
- Each Pod `--advertise-url=http://<pod>.opens3-clerk-hl.<ns>.svc.cluster.local:9333`  
- `--peers` lists all Clerk DNS names (with a single replica, peers is empty and the node becomes leader immediately)  
- Raft log on hostPath (default) or PVC `raft` when `persistence.type=pvc` — do not use emptyDir  


### Volume

- StatefulSet; `--ip=$(POD_IP)` reported to Clerk for in-cluster direct Pod IP access  
- `--node-id=$(POD_NAME)` keeps identity stable across restarts  
- Default `defaultReplication=001` needs ≥2 Volume Pods  
- Volume ordinals declared by Helm (`0..replicas-1`) are **online** by default; use `volume.replicaOf` / `volume.nodes` to set replica relationships at deploy time  
- New ordinals added via the web “Add data node” flow have **no bootstrap file → offline**; set replicas in Admin and bring them online  

### Unified Filer

- One Deployment: `opens3-filer`, container args `s3` (Protocols::ALL)  
- Service `opens3-filer` is the primary expose; `opens3-s3` is a ClusterIP alias  
- Shared store: `metaBackend=postgres|mysql|tidb` (Chart does **not** support local redb)  
- Default Postgres: Chart includes a Postgres Deployment (point `filer.databaseUrl` at an external DB and set `filer.postgres.enabled=false`)  
- Scale out: `filer.replicas`  
- Auth: `filer.credentials` → Secret `*-filer-credentials`  

### Scaling

```bash
# Add Volume nodes (new hostPath dirs / PVCs created automatically)
kubectl -n opens3 scale sts/opens3-volume --replicas=6
# or helm
helm upgrade opens3 deploy/helm/opens3 -n opens3 --set volume.replicas=6 --reuse-values

# Add Filer replicas
helm upgrade opens3 deploy/helm/opens3 -n opens3 --set filer.replicas=3 --reuse-values

# Trigger heal / balance (after port-forwarding clerk)
curl -X POST http://127.0.0.1:9333/cluster/heal
curl -X POST http://127.0.0.1:9333/cluster/balance
```

Admin topology: click a **Volume** node to bring it online/offline; click a **Filer** node to change Clerk assign policy. Adjust Filer replica count with Helm / kubectl.

## Helm values summary

See [`deploy/helm/opens3/values.yaml`](../../deploy/helm/opens3/values.yaml):

- `image.*`, `defaultReplication`, `dataCenter` / `rack`
- `persistence.type` (`hostPath` \| `pvc`) / `persistence.hostPath.root`
- `clerk.replicas` / `clerk.persistence.*`
- `volume.replicas` / `maxVolumes` / `maxVolumeSizeMb` / `volume.persistence.*`
- `filer.port` / `filer.replicas` / `filer.credentials` / `filer.metaBackend` / `filer.postgres.*` / `filer.databaseUrl`
- `filer.service.type` / `filer.service.nodePort` / `ingress`

## Uninstall

```bash
helm uninstall opens3 -n opens3
# hostPath data remains on the node under /var/lib/opens3/... — remove manually if needed
# If you used persistence.type=pvc, PVCs are retained until deleted:
# kubectl -n opens3 delete pvc --all
kubectl delete ns opens3
```

## Monitoring

```bash
# ServiceMonitor + PrometheusRule + Grafana dashboard ConfigMap
helm upgrade opens3 deploy/helm/opens3 -n opens3 --reuse-values \
  --set serviceMonitor.enabled=true \
  --set serviceMonitor.labels.release=kube-prometheus-stack \
  --set prometheusRule.enabled=true \
  --set prometheusRule.labels.release=kube-prometheus-stack \
  --set grafanaDashboard.enabled=true
```

Metrics, Grafana dashboards, and alerts: see [Monitoring guide](monitoring.md). Artifacts live under `deploy/monitoring/` and Helm `templates/`.

## Troubleshooting (K8s)

| Symptom | Check |
|---------|-------|
| Clerk never ready | `kubectl logs sts/opens3-clerk`; check `/ready` and peers DNS; Raft PVC |
| Volume no heartbeat | Clerk Service DNS; heartbeat errors in Volume logs |
| Filer Pending / Crash | Image, Postgres readiness, `DATABASE_URL`; `kubectl logs deploy/opens3-filer` |
| Write fails / no writable volume | Are Volumes **online** in Admin; does replication match node count |
| Cross-node read fails | Does NetworkPolicy allow Volume Pod IP:8080 |
| Image pull failures | `imagePullPolicy` / kind load / private registry secret |

More general issues: [Troubleshooting](troubleshooting.md).
