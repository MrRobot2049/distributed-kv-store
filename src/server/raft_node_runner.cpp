#include "server/raft_node_runner.h"

namespace raftkv {

RaftNodeRunner::RaftNodeRunner(RaftNode& raft_node,
                               std::chrono::milliseconds heartbeat_interval,
                               std::chrono::milliseconds min_election_timeout,
                               std::chrono::milliseconds max_election_timeout)
    : raft_node_(raft_node),
      heartbeat_interval_(heartbeat_interval),
      election_timer_(min_election_timeout, max_election_timeout) {}

RaftNodeRunner::~RaftNodeRunner() {
    Stop();
}

void RaftNodeRunner::Start() {
    if (worker_.joinable()) {
        return;
    }

    worker_ = std::jthread([this](std::stop_token stop_token) { Run(stop_token); });
}

void RaftNodeRunner::Stop() {
    if (worker_.joinable()) {
        worker_.request_stop();
        worker_.join();
    }
}

void RaftNodeRunner::Run(std::stop_token stop_token) {
    auto election_timeout = election_timer_.NextTimeout();
    auto next_heartbeat = std::chrono::steady_clock::now() + heartbeat_interval_;

    while (!stop_token.stop_requested()) {
        const auto now = std::chrono::steady_clock::now();
        if (raft_node_.CurrentRole() == Role::Leader) {
            if (now >= next_heartbeat) {
                raft_node_.SendHeartbeats();
                next_heartbeat = now + heartbeat_interval_;
            }
        } else if (now - raft_node_.LastElectionReset() >= election_timeout) {
            raft_node_.Tick();
            election_timeout = election_timer_.NextTimeout();
            next_heartbeat = now + heartbeat_interval_;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
}

}  // namespace raftkv
