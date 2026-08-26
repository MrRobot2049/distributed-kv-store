#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "raft/raft_node.h"
#include "raft.grpc.pb.h"

namespace raftkv {

class AsyncRaftServer {
 public:
    class CallData;

    AsyncRaftServer(std::string listen_address, RaftNode& raft_node,
                    std::uint32_t worker_count = std::thread::hardware_concurrency(),
                    std::vector<grpc::Service*> extra_services = {});
    ~AsyncRaftServer();

    AsyncRaftServer(const AsyncRaftServer&) = delete;
    AsyncRaftServer& operator=(const AsyncRaftServer&) = delete;

    bool Start();
    void Wait();
    void Shutdown();

    int selected_port() const;
    std::string bound_address() const;

 private:
    void HandleRpcs();
    void SpawnInitialHandlers();

    std::string listen_address_;
    RaftNode& raft_node_;
    std::uint32_t worker_count_;
    std::vector<grpc::Service*> extra_services_;
    int selected_port_{0};
    std::atomic_bool shutdown_started_{false};

    RaftService::AsyncService service_;
    std::unique_ptr<grpc::ServerCompletionQueue> completion_queue_;
    std::unique_ptr<grpc::Server> server_;
    std::vector<std::thread> workers_;
};

}  // namespace raftkv
