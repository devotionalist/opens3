> Language: [English](./proto.md) · [中文](../../modules/proto.md)

# Proto (`opens3-proto`)

## Role

Defines **ClerkService** / **VolumeService** gRPC contracts with Protobuf + tonic, generating Rust types and service traits. The Volume needle hot path (gateway I/O, sync replication) uses **gRPC**; the Clerk control plane remains primarily HTTP JSON.

## Location

- IDL: `proto/opens3.proto`  
- Crate: `crates/opens3-proto` (`tonic::include_proto!("opens3")`)

## Service overview

### ClerkService

| RPC | Role |
|-----|------|
| `Heartbeat` | Volume reports topology and local volumes; returns volume ids to create |
| `Assign` | Allocate FileId + write locations |
| `LookupVolume` | volumeId → locations |
| `ListVolumes` / `SealVolume` / `RegisterEcShards` | Management plane |
| `GetClusterInfo` | Cluster summary |

### VolumeService

| RPC | Role |
|-----|------|
| `WriteNeedle` / `ReadNeedle` / `DeleteNeedle` | Object bytes |
| `ReplicateNeedle` | Replica write |
| `CompactVolume` / `EncodeEc` | Maintenance |
| `Health` | Liveness |

## Design notes

- Message `Location` fields align with HTTP JSON (`url` / `public_url` / DC / rack / `node_id`).  
- HTTP and gRPC **share** the underlying `VolumeStore` / `ClusterState`, avoiding two business-logic stacks.  
- After changing the IDL, re-run `cargo build -p opens3-proto` (build.rs generates code).

## Dependencies

`tonic` / `prost`; referenced by `opens3-volume`.
