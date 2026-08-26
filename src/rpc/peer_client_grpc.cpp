#include "rpc/peer_client_grpc.h"

#include "rpc/raft_proto_conversions.h"

namespace raftkv {
namespace {

void ApplyDeadline(grpc::ClientContext* context, std::chrono::milliseconds timeout) {
    context->set_deadline(std::chrono::system_clock::now() + timeout);
}

}  // namespace

GrpcPeerClient::GrpcPeerClient(const std::vector<PeerEndpoint>& peers,
                               std::chrono::milliseconds rpc_timeout)
    : rpc_timeout_(rpc_timeout) {
    for (const auto& peer : peers) {
        auto channel = grpc::CreateChannel(peer.address, grpc::InsecureChannelCredentials());
        stubs_.emplace(peer.id, RaftService::NewStub(channel));
    }
}

core::RequestVoteResponse GrpcPeerClient::RequestVote(
    const NodeId& peer, const core::RequestVoteRequest& request) {
    auto stub = stubs_.find(peer);
    if (stub == stubs_.end()) {
        return UnavailableRequestVoteResponse(request.term);
    }

    raftkv::RequestVoteRequest rpc_request;
    rpc_request.set_term(request.term);
    rpc_request.set_candidate_id(request.candidate_id);
    rpc_request.set_last_log_index(request.last_log_index);
    rpc_request.set_last_log_term(request.last_log_term);

    grpc::ClientContext context;
    ApplyDeadline(&context, rpc_timeout_);

    raftkv::RequestVoteResponse rpc_response;
    const grpc::Status status =
        stub->second->RequestVote(&context, rpc_request, &rpc_response);
    if (!status.ok()) {
        return UnavailableRequestVoteResponse(request.term);
    }

    return core::RequestVoteResponse{
        .term = rpc_response.term(),
        .vote_granted = rpc_response.vote_granted(),
    };
}

core::AppendEntriesResponse GrpcPeerClient::AppendEntries(
    const NodeId& peer, const core::AppendEntriesRequest& request) {
    auto stub = stubs_.find(peer);
    if (stub == stubs_.end()) {
        return UnavailableAppendEntriesResponse(request.term);
    }

    raftkv::AppendEntriesRequest rpc_request;
    rpc_request.set_term(request.term);
    rpc_request.set_leader_id(request.leader_id);
    rpc_request.set_prev_log_index(request.prev_log_index);
    rpc_request.set_prev_log_term(request.prev_log_term);
    rpc_request.set_leader_commit(request.leader_commit);
    for (const auto& entry : request.entries) {
        *rpc_request.add_entries() = ToProto(entry);
    }

    grpc::ClientContext context;
    ApplyDeadline(&context, rpc_timeout_);

    raftkv::AppendEntriesResponse rpc_response;
    const grpc::Status status =
        stub->second->AppendEntries(&context, rpc_request, &rpc_response);
    if (!status.ok()) {
        return UnavailableAppendEntriesResponse(request.term);
    }

    return core::AppendEntriesResponse{
        .term = rpc_response.term(),
        .success = rpc_response.success(),
        .conflict_index = rpc_response.conflict_index(),
    };
}

core::InstallSnapshotResponse GrpcPeerClient::InstallSnapshot(
    const NodeId& peer, const core::InstallSnapshotRequest& request) {
    auto stub = stubs_.find(peer);
    if (stub == stubs_.end()) {
        return UnavailableInstallSnapshotResponse(request.term);
    }

    raftkv::InstallSnapshotRequest rpc_request;
    rpc_request.set_term(request.term);
    rpc_request.set_leader_id(request.leader_id);
    rpc_request.set_last_included_index(request.last_included_index);
    rpc_request.set_last_included_term(request.last_included_term);
    rpc_request.set_data(request.data);

    grpc::ClientContext context;
    ApplyDeadline(&context, rpc_timeout_);

    raftkv::InstallSnapshotResponse rpc_response;
    const grpc::Status status =
        stub->second->InstallSnapshot(&context, rpc_request, &rpc_response);
    if (!status.ok()) {
        return UnavailableInstallSnapshotResponse(request.term);
    }

    return core::InstallSnapshotResponse{.term = rpc_response.term()};
}

core::RequestVoteResponse GrpcPeerClient::UnavailableRequestVoteResponse(
    Term request_term) const {
    return core::RequestVoteResponse{.term = request_term, .vote_granted = false};
}

core::AppendEntriesResponse GrpcPeerClient::UnavailableAppendEntriesResponse(
    Term request_term) const {
    return core::AppendEntriesResponse{.term = request_term, .success = false};
}

core::InstallSnapshotResponse GrpcPeerClient::UnavailableInstallSnapshotResponse(
    Term request_term) const {
    return core::InstallSnapshotResponse{.term = request_term};
}

}  // namespace raftkv
