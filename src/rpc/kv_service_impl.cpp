#include "rpc/kv_service_impl.h"

#include <utility>

#include "storage/kv_command.h"

namespace raftkv {

KVServiceImpl::KVServiceImpl(RaftNode& raft_node, KeyValueStateMachine& state_machine,
                             LeaderHintResolver leader_hint_resolver)
    : raft_node_(raft_node),
      state_machine_(state_machine),
      leader_hint_resolver_(std::move(leader_hint_resolver)) {}

grpc::Status KVServiceImpl::Put(grpc::ServerContext*, const raftkv::PutRequest* request,
                                raftkv::KVResponse* response) {
    if (RedirectIfNotLeader(response)) {
        return grpc::Status::OK;
    }

    const bool replicated =
        raft_node_.SubmitCommand(EncodePutCommand(request->key(), request->value()));
    response->set_success(replicated);
    response->set_is_leader(true);
    if (replicated) {
        state_machine_.Put(request->key(), request->value());
    }

    return grpc::Status::OK;
}

grpc::Status KVServiceImpl::Get(grpc::ServerContext*, const raftkv::GetRequest* request,
                                raftkv::KVResponse* response) {
    if (RedirectIfNotLeader(response)) {
        return grpc::Status::OK;
    }

    response->set_is_leader(true);
    const auto value = state_machine_.Get(request->key());
    response->set_success(value.has_value());
    if (value.has_value()) {
        response->set_value(*value);
    }

    return grpc::Status::OK;
}

grpc::Status KVServiceImpl::Delete(grpc::ServerContext*,
                                   const raftkv::DeleteRequest* request,
                                   raftkv::KVResponse* response) {
    if (RedirectIfNotLeader(response)) {
        return grpc::Status::OK;
    }

    const bool replicated = raft_node_.SubmitCommand(EncodeDeleteCommand(request->key()));
    response->set_success(replicated);
    response->set_is_leader(true);
    if (replicated) {
        state_machine_.Delete(request->key());
    }

    return grpc::Status::OK;
}

bool KVServiceImpl::RedirectIfNotLeader(raftkv::KVResponse* response) const {
    if (raft_node_.CurrentRole() == Role::Leader) {
        return false;
    }

    response->set_success(false);
    response->set_is_leader(false);
    const auto leader = raft_node_.CurrentLeader();
    if (leader.has_value()) {
        response->set_leader_hint(leader_hint_resolver_(*leader));
    }

    return true;
}

}  // namespace raftkv
