#include "rpc/async_raft_server.h"

#include <algorithm>
#include <utility>

#include "rpc/raft_proto_conversions.h"

namespace raftkv {
class AsyncRaftServer::CallData {
 public:
    virtual ~CallData() = default;
    virtual void Proceed(bool ok) = 0;
};

class RequestVoteCallData final : public AsyncRaftServer::CallData {
 public:
    RequestVoteCallData(RaftService::AsyncService* service,
                        grpc::ServerCompletionQueue* completion_queue,
                        RaftNode* raft_node)
        : service_(service),
          completion_queue_(completion_queue),
          raft_node_(raft_node),
          responder_(&context_) {
        Proceed(true);
    }

    void Proceed(bool ok) override {
        if (state_ == State::Create) {
            state_ = State::Process;
            service_->RequestRequestVote(&context_, &request_, &responder_,
                                         completion_queue_, completion_queue_, this);
            return;
        }

        if (state_ == State::Process) {
            if (!ok) {
                delete this;
                return;
            }

            new RequestVoteCallData(service_, completion_queue_, raft_node_);
            core::RequestVoteResponse core_response;
            raft_node_->OnRequestVote(ToCore(request_), &core_response);
            FillProto(core_response, &response_);
            state_ = State::Finish;
            responder_.Finish(response_, grpc::Status::OK, this);
            return;
        }

        delete this;
    }

 private:
    enum class State {
        Create,
        Process,
        Finish,
    };

    State state_{State::Create};
    RaftService::AsyncService* service_;
    grpc::ServerCompletionQueue* completion_queue_;
    RaftNode* raft_node_;
    grpc::ServerContext context_;
    raftkv::RequestVoteRequest request_;
    raftkv::RequestVoteResponse response_;
    grpc::ServerAsyncResponseWriter<raftkv::RequestVoteResponse> responder_;
};

class AppendEntriesCallData final : public AsyncRaftServer::CallData {
 public:
    AppendEntriesCallData(RaftService::AsyncService* service,
                          grpc::ServerCompletionQueue* completion_queue,
                          RaftNode* raft_node)
        : service_(service),
          completion_queue_(completion_queue),
          raft_node_(raft_node),
          responder_(&context_) {
        Proceed(true);
    }

    void Proceed(bool ok) override {
        if (state_ == State::Create) {
            state_ = State::Process;
            service_->RequestAppendEntries(&context_, &request_, &responder_,
                                           completion_queue_, completion_queue_, this);
            return;
        }

        if (state_ == State::Process) {
            if (!ok) {
                delete this;
                return;
            }

            new AppendEntriesCallData(service_, completion_queue_, raft_node_);
            core::AppendEntriesResponse core_response;
            raft_node_->OnAppendEntries(ToCore(request_), &core_response);
            FillProto(core_response, &response_);
            state_ = State::Finish;
            responder_.Finish(response_, grpc::Status::OK, this);
            return;
        }

        delete this;
    }

 private:
    enum class State {
        Create,
        Process,
        Finish,
    };

    State state_{State::Create};
    RaftService::AsyncService* service_;
    grpc::ServerCompletionQueue* completion_queue_;
    RaftNode* raft_node_;
    grpc::ServerContext context_;
    raftkv::AppendEntriesRequest request_;
    raftkv::AppendEntriesResponse response_;
    grpc::ServerAsyncResponseWriter<raftkv::AppendEntriesResponse> responder_;
};

class InstallSnapshotCallData final : public AsyncRaftServer::CallData {
 public:
    InstallSnapshotCallData(RaftService::AsyncService* service,
                            grpc::ServerCompletionQueue* completion_queue,
                            RaftNode* raft_node)
        : service_(service),
          completion_queue_(completion_queue),
          raft_node_(raft_node),
          responder_(&context_) {
        Proceed(true);
    }

    void Proceed(bool ok) override {
        if (state_ == State::Create) {
            state_ = State::Process;
            service_->RequestInstallSnapshot(&context_, &request_, &responder_,
                                             completion_queue_, completion_queue_, this);
            return;
        }

        if (state_ == State::Process) {
            if (!ok) {
                delete this;
                return;
            }

            new InstallSnapshotCallData(service_, completion_queue_, raft_node_);
            core::InstallSnapshotResponse core_response;
            raft_node_->OnInstallSnapshot(ToCore(request_), &core_response);
            FillProto(core_response, &response_);
            state_ = State::Finish;
            responder_.Finish(response_, grpc::Status::OK, this);
            return;
        }

        delete this;
    }

 private:
    enum class State {
        Create,
        Process,
        Finish,
    };

    State state_{State::Create};
    RaftService::AsyncService* service_;
    grpc::ServerCompletionQueue* completion_queue_;
    RaftNode* raft_node_;
    grpc::ServerContext context_;
    raftkv::InstallSnapshotRequest request_;
    raftkv::InstallSnapshotResponse response_;
    grpc::ServerAsyncResponseWriter<raftkv::InstallSnapshotResponse> responder_;
};

AsyncRaftServer::AsyncRaftServer(std::string listen_address, RaftNode& raft_node,
                                 std::uint32_t worker_count,
                                 std::vector<grpc::Service*> extra_services)
    : listen_address_(std::move(listen_address)),
      raft_node_(raft_node),
      worker_count_(std::max<std::uint32_t>(1, worker_count)),
      extra_services_(std::move(extra_services)) {}

AsyncRaftServer::~AsyncRaftServer() {
    Shutdown();
}

bool AsyncRaftServer::Start() {
    grpc::ServerBuilder builder;
    builder.AddListeningPort(listen_address_, grpc::InsecureServerCredentials(),
                             &selected_port_);
    builder.RegisterService(&service_);
    for (grpc::Service* extra_service : extra_services_) {
        builder.RegisterService(extra_service);
    }
    completion_queue_ = builder.AddCompletionQueue();
    server_ = builder.BuildAndStart();
    if (!server_) {
        return false;
    }

    SpawnInitialHandlers();
    workers_.reserve(worker_count_);
    for (std::uint32_t i = 0; i < worker_count_; ++i) {
        workers_.emplace_back([this] { HandleRpcs(); });
    }

    return true;
}

void AsyncRaftServer::Wait() {
    if (server_) {
        server_->Wait();
    }
}

void AsyncRaftServer::Shutdown() {
    if (shutdown_started_.exchange(true)) {
        return;
    }

    if (server_) {
        server_->Shutdown();
    }

    if (completion_queue_) {
        completion_queue_->Shutdown();
    }

    for (auto& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }
}

int AsyncRaftServer::selected_port() const {
    return selected_port_;
}

std::string AsyncRaftServer::bound_address() const {
    return listen_address_;
}

void AsyncRaftServer::HandleRpcs() {
    void* tag = nullptr;
    bool ok = false;
    while (completion_queue_->Next(&tag, &ok)) {
        static_cast<CallData*>(tag)->Proceed(ok);
    }
}

void AsyncRaftServer::SpawnInitialHandlers() {
    new RequestVoteCallData(&service_, completion_queue_.get(), &raft_node_);
    new AppendEntriesCallData(&service_, completion_queue_.get(), &raft_node_);
    new InstallSnapshotCallData(&service_, completion_queue_.get(), &raft_node_);
}

}  // namespace raftkv
