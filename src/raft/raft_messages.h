#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "common/log_entry.h"
#include "common/types.h"

namespace raftkv {
namespace core {

struct RequestVoteRequest {
    Term term{0};
    NodeId candidate_id;
    LogIndex last_log_index{0};
    Term last_log_term{0};
};

struct RequestVoteResponse {
    Term term{0};
    bool vote_granted{false};
};

struct AppendEntriesRequest {
    Term term{0};
    NodeId leader_id;
    LogIndex prev_log_index{0};
    Term prev_log_term{0};
    std::vector<RaftLogEntry> entries;
    LogIndex leader_commit{0};
};

struct AppendEntriesResponse {
    Term term{0};
    bool success{false};
    LogIndex conflict_index{0};
};

struct InstallSnapshotRequest {
    Term term{0};
    NodeId leader_id;
    LogIndex last_included_index{0};
    Term last_included_term{0};
    std::string data;
};

struct InstallSnapshotResponse {
    Term term{0};
};

}  // namespace core
}  // namespace raftkv
