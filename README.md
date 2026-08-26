# Distributed Key-Value Store

A fault-tolerant distributed key-value store implemented in C++ using the Raft consensus algorithm.

## Current Status

This repository is under active development. The current implementation covers the core Raft path through leader election, log replication, async gRPC transport, RocksDB-backed persistence, committed-log application, and snapshot transfer/restore groundwork.

Approximate progress against the build plan: 86%.

## Implemented

- C++20 CMake build with optional gRPC and RocksDB targets.
- Protobuf definitions for Raft and KV APIs.
- Raft leader election with randomized election timeouts and leader heartbeats.
- In-memory Raft log implementation for fast unit tests.
- Log replication with conflict truncation, majority commit, and lagging follower catch-up.
- Async gRPC Raft server backed by `grpc::ServerCompletionQueue`.
- gRPC peer client for Raft-to-Raft RPCs.
- Client-facing KV gRPC service with leader redirect responses.
- RocksDB-backed store using column families for metadata, Raft log, and KV data.
- Durable Raft term, vote, commit index, last applied index, and snapshot metadata.
- Startup replay of committed log entries into the KV state machine.
- Background committed-log applier for persistent nodes.
- RocksDB checkpoint-based snapshot creation and log compaction before snapshot boundaries.
- `InstallSnapshot` RPC plumbing through Raft core, sync service, async service, and gRPC client.
- Snapshot checkpoint payload packing/restoring for RPC transfer.
- Offline promotion of restored snapshot payloads into a closed RocksDB store path.
- Crash-recovery subprocess coverage for synced RocksDB Raft/KV state.
- Deterministic majority/minority partition coverage for leader failover and healing.
- GitHub Actions CI workflow for vcpkg-backed full build and test runs.

## In Progress / Remaining

- Runtime orchestration for applying incoming snapshot payloads without manual restart.
- Broader chaos testing with randomized drops and restarts.
- Docker Compose and node config files.
- Design documentation.

## Build And Test

```bash
cmake -S . -B build -DBUILD_TESTING=ON -DBUILD_GRPC_TRANSPORT=ON -DBUILD_ROCKSDB_STORAGE=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

The latest verified test run passes all 51 tests.

## Run A Node

```bash
./build/src/server \
  --id node1 \
  --listen 0.0.0.0:5001 \
  --data-dir ./data/node1 \
  --peer node2=localhost:5002 \
  --peer node3=localhost:5003
```

Omit `--data-dir` for an in-memory development node.
