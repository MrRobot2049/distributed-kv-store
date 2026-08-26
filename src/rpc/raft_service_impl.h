#pragma once

#include <grpcpp/grpcpp.h>

#include "raft/raft_node.h"
#include "raft.grpc.pb.h"

namespace raftkv {

class RaftServiceImpl final : public RaftService::Service {
 public:
    explicit RaftServiceImpl(RaftNode& raft_node);

    grpc::Status RequestVote(grpc::ServerContext* context,
                             const raftkv::RequestVoteRequest* request,
                             raftkv::RequestVoteResponse* response) override;

    grpc::Status AppendEntries(grpc::ServerContext* context,
                               const raftkv::AppendEntriesRequest* request,
                               raftkv::AppendEntriesResponse* response) override;

    grpc::Status InstallSnapshot(grpc::ServerContext* context,
                                 const raftkv::InstallSnapshotRequest* request,
                                 raftkv::InstallSnapshotResponse* response) override;

 private:
    RaftNode& raft_node_;
};

}  // namespace raftkv
