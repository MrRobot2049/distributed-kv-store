#pragma once

#include <functional>
#include <string>

#include <grpcpp/grpcpp.h>

#include "kv_store.grpc.pb.h"
#include "raft/raft_node.h"
#include "storage/state_machine.h"

namespace raftkv {

class KVServiceImpl final : public KVStoreService::Service {
 public:
    using LeaderHintResolver = std::function<std::string(const NodeId&)>;

    KVServiceImpl(RaftNode& raft_node, KeyValueStateMachine& state_machine,
                  LeaderHintResolver leader_hint_resolver);

    grpc::Status Put(grpc::ServerContext* context, const raftkv::PutRequest* request,
                     raftkv::KVResponse* response) override;
    grpc::Status Get(grpc::ServerContext* context, const raftkv::GetRequest* request,
                     raftkv::KVResponse* response) override;
    grpc::Status Delete(grpc::ServerContext* context, const raftkv::DeleteRequest* request,
                        raftkv::KVResponse* response) override;

 private:
    bool RedirectIfNotLeader(raftkv::KVResponse* response) const;

    RaftNode& raft_node_;
    KeyValueStateMachine& state_machine_;
    LeaderHintResolver leader_hint_resolver_;
};

}  // namespace raftkv
