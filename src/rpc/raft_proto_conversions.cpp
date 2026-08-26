#include "rpc/raft_proto_conversions.h"

namespace raftkv {

RaftLogEntry ToCore(const raftkv::LogEntry& entry) {
    return RaftLogEntry{
        .term = entry.term(),
        .index = entry.index(),
        .command = entry.command(),
    };
}

core::RequestVoteRequest ToCore(const raftkv::RequestVoteRequest& request) {
    return core::RequestVoteRequest{
        .term = request.term(),
        .candidate_id = request.candidate_id(),
        .last_log_index = request.last_log_index(),
        .last_log_term = request.last_log_term(),
    };
}

core::AppendEntriesRequest ToCore(const raftkv::AppendEntriesRequest& request) {
    core::AppendEntriesRequest core_request;
    core_request.term = request.term();
    core_request.leader_id = request.leader_id();
    core_request.prev_log_index = request.prev_log_index();
    core_request.prev_log_term = request.prev_log_term();
    core_request.leader_commit = request.leader_commit();
    core_request.entries.reserve(static_cast<std::size_t>(request.entries_size()));
    for (const auto& entry : request.entries()) {
        core_request.entries.push_back(ToCore(entry));
    }

    return core_request;
}

core::InstallSnapshotRequest ToCore(const raftkv::InstallSnapshotRequest& request) {
    return core::InstallSnapshotRequest{
        .term = request.term(),
        .leader_id = request.leader_id(),
        .last_included_index = request.last_included_index(),
        .last_included_term = request.last_included_term(),
        .data = request.data(),
    };
}

raftkv::LogEntry ToProto(const RaftLogEntry& entry) {
    raftkv::LogEntry proto_entry;
    proto_entry.set_term(entry.term);
    proto_entry.set_index(entry.index);
    proto_entry.set_command(entry.command);
    return proto_entry;
}

void FillProto(const core::RequestVoteResponse& source,
               raftkv::RequestVoteResponse* target) {
    target->set_term(source.term);
    target->set_vote_granted(source.vote_granted);
}

void FillProto(const core::AppendEntriesResponse& source,
               raftkv::AppendEntriesResponse* target) {
    target->set_term(source.term);
    target->set_success(source.success);
    target->set_conflict_index(source.conflict_index);
}

void FillProto(const core::InstallSnapshotResponse& source,
               raftkv::InstallSnapshotResponse* target) {
    target->set_term(source.term);
}

}  // namespace raftkv
