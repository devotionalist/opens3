> Language: [English](./install.md) · [中文](../install.md)

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
