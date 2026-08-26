#include "server/committed_log_applier.h"

#include "storage/kv_command.h"

namespace raftkv {

CommittedLogApplier::CommittedLogApplier(RaftLog& log, RaftMetadataStore& metadata_store,
                                         KeyValueStateMachine& state_machine,
                                         std::chrono::milliseconds poll_interval)
    : log_(log),
      metadata_store_(metadata_store),
      state_machine_(state_machine),
      poll_interval_(poll_interval) {}

CommittedLogApplier::~CommittedLogApplier() {
    Stop();
}

LogIndex CommittedLogApplier::ApplyOnce() {
    return ReplayCommittedEntries(log_, metadata_store_, state_machine_);
}

void CommittedLogApplier::Start() {
    if (worker_.joinable()) {
        return;
    }

    worker_ = std::jthread([this](std::stop_token stop_token) { Run(stop_token); });
}

void CommittedLogApplier::Stop() {
    if (worker_.joinable()) {
        worker_.request_stop();
        worker_.join();
    }
}

void CommittedLogApplier::Run(std::stop_token stop_token) {
    while (!stop_token.stop_requested()) {
        ApplyOnce();
        std::this_thread::sleep_for(poll_interval_);
    }
}

}  // namespace raftkv
