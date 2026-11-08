> Language: [English](./development.md) · [中文](../zh/development.md)

# Development and testing

## Repository layout

```text
opens3/
  crates/
    opens3/           # CLI binary            → docs/en/modules/cli.md
    opens3-common/    # FileId, replicas, metrics → docs/en/modules/common.md
    opens3-proto/     # gRPC protobuf         → docs/en/modules/proto.md
    opens3-clerk/     # Raft, cluster, heal (Clerk control plane) → docs/en/modules/clerk.md
    opens3-volume/    # needle, EC, HTTP      → docs/en/modules/volume.md
    opens3-filer/     # Unified gateway: S3 + HTTP/WebDAV (http_s3 / http_filer) → docs/en/modules/filer.md / s3.md
    opens3-store/     # FilerStore backends   → docs/en/modules/store.md
  proto/opens3.proto
  admin/static/       # Admin UI
  docs/en/ · docs/zh/  # docs (English default)
  docs/en/modules/    # Per-crate design notes
  tests/
    e2e_*.sh / s3_compat/
  LICENSE             # Apache-2.0
```

Module responsibilities and dependency boundaries: **[Module design](modules/README.md)**; system paths: **[Architecture](architecture.md)**.

## Local development loop

```bash
cargo build -p opens3
./target/debug/opens3 server --dir /tmp/opens3-dev --max-volume-size-mb 32
```

Format and checks:

```bash
cargo fmt
cargo clippy --workspace --all-targets
cargo test --workspace
```

## Test layers

| Layer | Command / location | Coverage |
|-------|--------------------|----------|
| Unit | `cargo test -p opens3-common`, etc. | FileId, JWT, policy, EC, Object Lock |
| Component | `opens3-volume` needle/fault/volume_full | Corruption detection, replica import, volume full |
| Filer | `read_failover` | Read replica failover |
| Clerk | `raft_persistence`, `heal_plan` | Log persistence, replica planning |
| End-to-end fault | `tests/e2e_*.sh` | Kill replica, Clerk failover, Raft snapshot, seal/EC, multi-S3, JWT/TLS/Lock |
| S3 compat | `tests/s3_compat/run.sh` | List pagination, DeleteObjects, multipart |

Full inventory and latest pass/fail record: **[Test coverage report](test-coverage.md)**.

```bash
cargo test --workspace
bash tests/e2e_fault.sh
bash tests/e2e_kill_replica.sh
bash tests/e2e_master_failover.sh
bash tests/e2e_raft_snapshot.sh
bash tests/e2e_seal_ec.sh
bash tests/e2e_multi_s3.sh
bash tests/e2e_prod_hardening.sh
bash tests/s3_compat/run.sh
```

When adding fault scenarios, prefer **repeatable automated tests**, and update [test-coverage.md](test-coverage.md) / [troubleshooting.md](troubleshooting.md).

## License and contribution

- License: **Apache License 2.0** (see root `LICENSE` / `NOTICE`)  
- Contributions are licensed under Apache-2.0 by default  
- Do not commit secrets or real production `data/` directories  
- Submission rules (PR-only, owner approval, tags/releases): **[Contributing](contributing.md)**

## Related docs

- [Contributing](contributing.md)
- [Architecture](architecture.md)
- [Module design](modules/README.md)
- [Configuration](configuration.md)
- [Operations](operations.md)
