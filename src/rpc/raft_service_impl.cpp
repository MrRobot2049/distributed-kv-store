#include "rpc/raft_service_impl.h"

#include "rpc/raft_proto_conversions.h"

namespace raftkv {

RaftServiceImpl::RaftServiceImpl(RaftNode& raft_node) : raft_node_(raft_node) {}

grpc::Status RaftServiceImpl::RequestVote(grpc::ServerContext*,
                                          const raftkv::RequestVoteRequest* request,
                                          raftkv::RequestVoteResponse* response) {
    core::RequestVoteResponse core_response;
    raft_node_.OnRequestVote(ToCore(*request), &core_response);
    FillProto(core_response, response);
    return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::AppendEntries(
    grpc::ServerContext*,
    const raftkv::AppendEntriesRequest* request,
    raftkv::AppendEntriesResponse* response) {
    core::AppendEntriesResponse core_response;
    raft_node_.OnAppendEntries(ToCore(*request), &core_response);
    FillProto(core_response, response);
    return grpc::Status::OK;
}

grpc::Status RaftServiceImpl::InstallSnapshot(
    grpc::ServerContext*,
    const raftkv::InstallSnapshotRequest* request,
    raftkv::InstallSnapshotResponse* response) {
    core::InstallSnapshotResponse core_response;
    raft_node_.OnInstallSnapshot(ToCore(*request), &core_response);
    FillProto(core_response, response);
    return grpc::Status::OK;
}

}  // namespace raftkv
