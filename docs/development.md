# 开发与测试

## 仓库结构

```text
opens3/
  crates/
    opens3/           # CLI 二进制          → docs/modules/cli.md
    opens3-common/    # FileId、副本、metrics → docs/modules/common.md
    opens3-proto/     # gRPC protobuf       → docs/modules/proto.md
    opens3-clerk/  # Raft、cluster、heal（Clerk 控制面）→ docs/modules/clerk.md
    opens3-volume/    # needle、EC、HTTP    → docs/modules/volume.md
    opens3-filer/     # 统一网关：S3 + HTTP/WebDAV（http_s3 / http_filer）→ docs/modules/filer.md / s3.md
    opens3-store/     # FilerStore 后端     → docs/modules/store.md
  proto/opens3.proto
  admin/static/       # Admin UI
  docs/               # 用户与架构文档
  docs/modules/       # 各 crate 设计说明
  tests/
    e2e_*.sh / s3_compat/
  LICENSE             # Apache-2.0
```

模块职责与依赖边界见 **[模块设计](modules/README.md)**、系统路径见 **[架构](architecture.md)**。

## 本地开发循环

```bash
cargo build -p opens3
./target/debug/opens3 server --dir /tmp/opens3-dev --max-volume-size-mb 32
```

格式与检查：

```bash
cargo fmt
cargo clippy --workspace --all-targets
cargo test --workspace
```

## 测试分层

| 层级 | 命令 / 位置 | 覆盖 |
|------|-------------|------|
| 单元 | `cargo test -p opens3-common` 等 | FileId、JWT、policy、EC、Object Lock |
| 组件 | `opens3-volume` needle/fault/volume_full | 损坏检测、副本导入、卷满 |
| Filer | `read_failover` | 读副本降级 |
| Clerk | `raft_persistence`、`heal_plan` | 日志持久化、副本规划 |
| 端到端故障 | `tests/e2e_*.sh` | kill 副本、Clerk 切换、Raft snapshot、seal/EC、多 S3、JWT/TLS/Lock |
| S3 兼容 | `tests/s3_compat/run.sh` | list 分页、DeleteObjects、multipart |

完整清单与最近一次通过/失败记录见 **[测试覆盖报告](test-coverage.md)**。

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

新增故障场景时，优先加 **可重复的自动化测试**，并更新 [test-coverage.md](test-coverage.md) / [troubleshooting.md](troubleshooting.md)。

## 协议与贡献

- 许可证：**Apache License 2.0**（见根目录 `LICENSE` / `NOTICE`）  
- 贡献默认按 Apache-2.0 授权  
- 请勿提交密钥、真实生产 `data/` 目录  

## 相关文档

- [架构](architecture.md)
- [模块设计](modules/README.md)
- [配置](configuration.md)
- [运维](operations.md)
