#pragma once

#include "common/log_entry.h"
#include "raft/raft_messages.h"
#include "raft.pb.h"

namespace raftkv {

RaftLogEntry ToCore(const raftkv::LogEntry& entry);
core::RequestVoteRequest ToCore(const raftkv::RequestVoteRequest& request);
core::AppendEntriesRequest ToCore(const raftkv::AppendEntriesRequest& request);
core::InstallSnapshotRequest ToCore(const raftkv::InstallSnapshotRequest& request);

raftkv::LogEntry ToProto(const RaftLogEntry& entry);
void FillProto(const core::RequestVoteResponse& source,
               raftkv::RequestVoteResponse* target);
void FillProto(const core::AppendEntriesResponse& source,
               raftkv::AppendEntriesResponse* target);
void FillProto(const core::InstallSnapshotResponse& source,
               raftkv::InstallSnapshotResponse* target);

}  // namespace raftkv
