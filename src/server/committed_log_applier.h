#pragma once

#include <chrono>
#include <thread>

#include "raft/log_manager.h"
#include "raft/raft_metadata_store.h"
#include "storage/state_machine.h"

namespace raftkv {

class CommittedLogApplier {
 public:
    CommittedLogApplier(
        RaftLog& log, RaftMetadataStore& metadata_store,
        KeyValueStateMachine& state_machine,
        std::chrono::milliseconds poll_interval = std::chrono::milliseconds(25));
    ~CommittedLogApplier();

    CommittedLogApplier(const CommittedLogApplier&) = delete;
    CommittedLogApplier& operator=(const CommittedLogApplier&) = delete;

    LogIndex ApplyOnce();
    void Start();
    void Stop();

 private:
    void Run(std::stop_token stop_token);

    RaftLog& log_;
    RaftMetadataStore& metadata_store_;
    KeyValueStateMachine& state_machine_;
    std::chrono::milliseconds poll_interval_;
    std::jthread worker_;
};

}  // namespace raftkv
