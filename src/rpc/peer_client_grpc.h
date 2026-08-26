#pragma once

#include <chrono>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "raft/peer_client.h"
#include "raft.grpc.pb.h"

namespace raftkv {

struct PeerEndpoint {
    NodeId id;
    std::string address;
};

class GrpcPeerClient final : public PeerClient {
 public:
    explicit GrpcPeerClient(
        const std::vector<PeerEndpoint>& peers,
        std::chrono::milliseconds rpc_timeout = std::chrono::milliseconds(150));

    core::RequestVoteResponse RequestVote(
        const NodeId& peer, const core::RequestVoteRequest& request) override;
    core::AppendEntriesResponse AppendEntries(
        const NodeId& peer, const core::AppendEntriesRequest& request) override;
    core::InstallSnapshotResponse InstallSnapshot(
        const NodeId& peer, const core::InstallSnapshotRequest& request) override;

 private:
    core::RequestVoteResponse UnavailableRequestVoteResponse(Term request_term) const;
    core::AppendEntriesResponse UnavailableAppendEntriesResponse(Term request_term) const;
    core::InstallSnapshotResponse UnavailableInstallSnapshotResponse(Term request_term) const;

    std::chrono::milliseconds rpc_timeout_;
    std::unordered_map<NodeId, std::unique_ptr<RaftService::Stub>> stubs_;
};

}  // namespace raftkv
