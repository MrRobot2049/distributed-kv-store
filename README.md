# Distributed Key-Value Store

A fault-tolerant distributed key-value store implemented in C++ using the Raft consensus algorithm.

## Current Status

This implementation covers the core Raft path through leader election, log replication, async gRPC transport, RocksDB-backed persistence, committed-log application, and live snapshot transfer/restore.

Approximate progress against the build plan: 100%.

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
- Runtime installation of received snapshot payloads into an active RocksDB store.
- Crash-recovery subprocess coverage for synced RocksDB Raft/KV state.
- Deterministic majority/minority partition coverage for leader failover and healing.
- GitHub Actions CI workflow for vcpkg-backed full build and test runs.
- Node config files and Docker Compose topology for a three-node local cluster.
- Architecture/design documentation covering core modules and known limitations.

## Scope Notes

- The build plan is complete for the implemented three-node topology and persistence path.
- Membership changes, client retry policies, and production-grade observability remain outside the current scope.

See [docs/design.md](docs/design.md) for the architecture notes.

## Build And Test

```bash
cmake -S . -B build -DBUILD_TESTING=ON -DBUILD_GRPC_TRANSPORT=ON -DBUILD_ROCKSDB_STORAGE=ON
cmake --build build
ctest --test-dir build --output-on-failure
```

The latest verified test run passes all 52 tests.

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

You can also run from a config file:

```bash
./build/src/server --config ./config/node1.conf
```

## Run With Docker Compose

```bash
docker compose up --build
```

The compose file starts `node1`, `node2`, and `node3` using the configs in `config/`.
