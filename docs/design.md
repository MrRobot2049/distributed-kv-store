# Distributed KV Store Design

## Goals

The project implements a small, durable, strongly consistent key-value store on top of the Raft consensus algorithm. The current design favors correctness, testability, and clear module boundaries over aggressive performance tuning.

## Process Model

Each server process owns one Raft node, one gRPC server, one peer transport client, and one key-value state machine. A node is started with an id, a listen address, optional persistent data directory, and the full set of peer endpoints.

Configuration can be passed through command-line flags or a simple key-value config file:

```text
id=node1
listen=0.0.0.0:5001
data_dir=/var/lib/raftkv/node1
peer=node2=node2:5002
peer=node3=node3:5003
```

The Docker Compose topology runs three nodes with persistent volumes and service-name peer addresses.

## Raft Core

`RaftNode` owns the consensus state machine:

- Persistent state: current term, voted-for node, log entries, commit index, and snapshot metadata.
- Volatile state: role, current leader, election reset time, next index, and match index.
- RPC handlers: `RequestVote`, `AppendEntries`, and `InstallSnapshot`.
- Leader operations: heartbeats, command submission, replication retry, majority commit, and commit broadcast.

The core is transport-agnostic. Tests can inject an in-memory peer client, while production nodes use the gRPC peer client.

## Storage

The production storage path uses one RocksDB database with separate column families:

- `metadata`: Raft term/vote, commit index, last applied index, and snapshot boundary.
- `raft_log`: serialized Raft log entries keyed by big-endian log index.
- `kv_data`: applied user key-value state.

All Raft metadata and log writes use synchronous RocksDB write options so committed state survives abrupt process exit. On startup, the server replays committed log entries after `last_applied` into the key-value state machine.

## Client API

The KV gRPC service exposes put/get/delete operations. Followers return a leader redirect when the leader is known. Leaders encode put/delete commands into binary log commands, submit them through Raft, and apply the committed command to the local state machine on success.

## Snapshots

Snapshots are built from RocksDB checkpoints. The snapshot manager can:

- Create checkpoints with Raft snapshot metadata.
- Compact old log entries before the snapshot boundary.
- Pack checkpoint files into a binary payload for `InstallSnapshot`.
- Restore a received payload into an incoming snapshot directory.
- Install a restored payload into an active RocksDB store by closing and reopening its handles around a directory promotion.


## Testing Strategy

The test suite covers:

- Leader election and voting safety.
- Log replication, conflicts, catch-up, commit advancement, and partition healing.
- Snapshot boundary behavior in Raft.
- Binary KV command encoding and replay.
- RocksDB persistence across reopen.
- Abrupt child-process exit recovery for synced RocksDB state.
- Async gRPC Raft smoke tests.
- Snapshot checkpoint, payload restore, compaction, offline promotion, and active-store installation.

## Known Limitations

- Membership changes are not implemented.
- Client retries and richer operational observability are still minimal.
