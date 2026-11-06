# 快速开始

## 1. 编译

```bash
git clone <repo-url> opens3
cd opens3
cargo build --release
export PATH="$PWD/target/release:$PATH"
```

## 2. 一键启动（开发 / 单机）

```bash
mkdir -p ./data
opens3 server --dir ./data --max-volume-size-mb 64
```

默认端口：

| 服务 | 地址 |
|------|------|
| Clerk | `http://127.0.0.1:9333` |
| Volume | `http://127.0.0.1:8080` |
| S3 / Admin | `http://127.0.0.1:8333` |

健康检查：

```bash
curl -sf http://127.0.0.1:8333/health
curl -sf http://127.0.0.1:9333/ready
open http://127.0.0.1:8333/admin/ui   # 或浏览器打开
```

## 3. 用 curl 访问 S3（无鉴权开发模式）

未设置 `OPENS3_ACCESS_KEY` / `OPENS3_SECRET_KEY` 时，网关以 root 身份放行，便于本地调试。

```bash
# 建桶
curl -X PUT http://127.0.0.1:8333/demo

# 上传
echo 'hello opens3' | curl -X PUT http://127.0.0.1:8333/demo/hello.txt --data-binary @-

# 下载
curl http://127.0.0.1:8333/demo/hello.txt

# 列举
curl 'http://127.0.0.1:8333/demo?list-type=2'
```

## 4. 用 AWS CLI

```bash
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=secretsecret
# 需先用密钥启动：见下方「开启鉴权」

aws --endpoint-url http://127.0.0.1:8333 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:8333 s3 cp ./file.bin s3://demo/
aws --endpoint-url http://127.0.0.1:8333 s3 ls s3://demo/
```

### 开启鉴权后重启

```bash
export OPENS3_ACCESS_KEY=admin
export OPENS3_SECRET_KEY=secretsecret
opens3 server --dir ./data --max-volume-size-mb 64
```

## 5. Shell 探活

```bash
opens3 shell cluster-status --clerk http://127.0.0.1:9333
opens3 shell assign --clerk http://127.0.0.1:9333
opens3 shell raft-status --clerk http://127.0.0.1:9333
```

下一步：[部署多节点](deployment.md) · [S3 详解](s3.md) · [运维](operations.md)
