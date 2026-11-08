> 语言: [English](../en/install.md) · [中文](./install.md)

# 安装与构建

## 环境要求

- Rust **1.75+**（推荐使用 [rustup](https://rustup.rs/) 最新稳定版）
- `protoc`（Protocol Buffers 编译器，构建 `opens3-proto` 需要）
- Linux / macOS；生产建议 Linux + 本地 SSD/NVMe（JBOD，勿上层再套 RAID）

检查：

```bash
rustc --version
cargo --version
protoc --version
```

macOS 安装 protoc 示例：

```bash
brew install protobuf
```

## 从源码构建

```bash
cargo build --release -p opens3
# 二进制：target/release/opens3
```

仅跑测试：

```bash
cargo test --workspace
bash tests/e2e_fault.sh
```

## 安装到 PATH

```bash
cp target/release/opens3 /usr/local/bin/
opens3 --version
```

## Docker 镜像

已发布到 Docker Hub（单一二进制；用子命令切换角色）：

| 镜像 | 说明 |
|------|------|
| [`jinopen/opens3:0.1.0`](https://hub.docker.com/r/jinopen/opens3) | 固定版本 |
| `jinopen/opens3:latest` | 与最近一次推送的 tag 一致 |

```bash
docker pull jinopen/opens3:0.1.0

# 同一镜像，不同角色
docker run --rm jinopen/opens3:0.1.0 --help
docker run --rm -p 9333:9333 jinopen/opens3:0.1.0 clerk --port 9333
docker run --rm -p 8080:8080 jinopen/opens3:0.1.0 volume --clerk http://host.docker.internal:9333
docker run --rm -p 8333:8333 jinopen/opens3:0.1.0 s3 --clerk http://host.docker.internal:9333
```

自行构建并推送：

```bash
docker build -t opens3:0.1.0 .
docker tag opens3:0.1.0 jinopen/opens3:0.1.0
docker login -u jinopen
docker push jinopen/opens3:0.1.0
docker push jinopen/opens3:latest
```

Kubernetes / Helm 用法见 [Kubernetes](kubernetes.md)。

## 子命令一览

```text
opens3 clerk    # 元数据 / 卷分配 / Raft
opens3 volume   # 数据节点
opens3 s3       # 统一 Filer：S3 + /entries + /dav + Admin
opens3 filer    # 仅路径 API / WebDAV（可选）
opens3 server   # 单进程拉起 clerk+volume+filer（开发）
opens3 shell    # 运维命令
opens3 fuse     # FUSE 占位（请用 s3fs / davfs 挂载）
```

详细参数见 [配置参考](configuration.md)。
