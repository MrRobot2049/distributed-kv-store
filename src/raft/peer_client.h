#pragma once

#include "common/types.h"
#include "raft/raft_messages.h"

namespace raftkv {

class PeerClient {
 public:
    virtual ~PeerClient() = default;

    virtual core::RequestVoteResponse RequestVote(const NodeId& peer,
                                                  const core::RequestVoteRequest& request) = 0;
    virtual core::AppendEntriesResponse AppendEntries(
        const NodeId& peer, const core::AppendEntriesRequest& request) = 0;
    virtual core::InstallSnapshotResponse InstallSnapshot(
        const NodeId&, const core::InstallSnapshotRequest& request) {
        return core::InstallSnapshotResponse{.term = request.term};
    }
};

}  // namespace raftkv
