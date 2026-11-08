> Language: [English](./install.md) · [中文](../zh/install.md)

# Install & build

## Requirements

- Rust **1.75+** (recommended: latest stable via [rustup](https://rustup.rs/))
- `protoc` (Protocol Buffers compiler; required to build `opens3-proto`)
- Linux / macOS; for production prefer Linux + local SSD/NVMe (JBOD; do not put RAID on top)

Check:

```bash
rustc --version
cargo --version
protoc --version
```

Example: install protoc on macOS:

```bash
brew install protobuf
```

## Build from source

```bash
cargo build --release -p opens3
# Binary: target/release/opens3
```

Tests only:

```bash
cargo test --workspace
bash tests/e2e_fault.sh
```

## Install to PATH

```bash
cp target/release/opens3 /usr/local/bin/
opens3 --version
```

## Docker image

Published image on Docker Hub (single binary; roles selected by CLI subcommand):

| Image | Notes |
|-------|--------|
| [`jinopen/opens3:0.1.0`](https://hub.docker.com/r/jinopen/opens3) | Pinned release tag |
| `jinopen/opens3:latest` | Same build as the latest pushed tag |

```bash
docker pull jinopen/opens3:0.1.0

# Roles share one image
docker run --rm jinopen/opens3:0.1.0 --help
docker run --rm -p 9333:9333 jinopen/opens3:0.1.0 clerk --port 9333
docker run --rm -p 8080:8080 jinopen/opens3:0.1.0 volume --clerk http://host.docker.internal:9333
docker run --rm -p 8333:8333 jinopen/opens3:0.1.0 s3 --clerk http://host.docker.internal:9333
```

Build and publish yourself:

```bash
docker build -t opens3:0.1.0 .
docker tag opens3:0.1.0 jinopen/opens3:0.1.0
docker login -u jinopen
docker push jinopen/opens3:0.1.0
docker push jinopen/opens3:latest
```

Kubernetes / Helm usage: [Kubernetes](kubernetes.md).

## Subcommands overview

```text
opens3 clerk    # Metadata / volume assign / Raft
opens3 volume   # Data node
opens3 s3       # Unified Filer: S3 + /entries + /dav + Admin
opens3 filer    # Path API / WebDAV only (optional)
opens3 server   # Single process: clerk+volume+filer (dev)
opens3 shell    # Ops commands
opens3 fuse     # FUSE placeholder (use s3fs / davfs to mount)
```

See [Configuration](configuration.md) for full flags.
