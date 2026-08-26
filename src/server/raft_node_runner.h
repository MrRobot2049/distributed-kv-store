#pragma once

#include <chrono>
#include <thread>

#include "raft/election_timer.h"
#include "raft/raft_node.h"

namespace raftkv {

class RaftNodeRunner {
 public:
    explicit RaftNodeRunner(
        RaftNode& raft_node,
        std::chrono::milliseconds heartbeat_interval = std::chrono::milliseconds(50),
        std::chrono::milliseconds min_election_timeout = std::chrono::milliseconds(150),
        std::chrono::milliseconds max_election_timeout = std::chrono::milliseconds(300));
    ~RaftNodeRunner();

    RaftNodeRunner(const RaftNodeRunner&) = delete;
    RaftNodeRunner& operator=(const RaftNodeRunner&) = delete;

    void Start();
    void Stop();

 private:
    void Run(std::stop_token stop_token);

    RaftNode& raft_node_;
    std::chrono::milliseconds heartbeat_interval_;
    ElectionTimer election_timer_;
    std::jthread worker_;
};

}  // namespace raftkv
