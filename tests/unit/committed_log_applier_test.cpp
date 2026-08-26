#include "server/committed_log_applier.h"

#include <gtest/gtest.h>

#include <chrono>
#include <optional>
#include <thread>

#include "storage/kv_command.h"
#include "storage/state_machine.h"

namespace raftkv {
namespace {

class InMemoryMetadataStore final : public RaftMetadataStore {
 public:
    void SaveTermAndVote(Term term, std::optional<NodeId> voted_for) override {
        current_term_ = term;
        voted_for_ = std::move(voted_for);
    }

    void SaveCommitIndex(LogIndex commit_index) override {
        commit_index_ = commit_index;
    }

    void SaveLastApplied(LogIndex last_applied) override {
        last_applied_ = last_applied;
    }

    Term CurrentTerm() const override {
        return current_term_;
    }

    std::optional<NodeId> VotedFor() const override {
        return voted_for_;
    }

    LogIndex CommitIndex() const override {
        return commit_index_;
    }

    LogIndex LastApplied() const override {
        return last_applied_;
    }

 private:
    Term current_term_{0};
    std::optional<NodeId> voted_for_;
    LogIndex commit_index_{0};
    LogIndex last_applied_{0};
};

TEST(CommittedLogApplierTest, ApplyOnceAppliesOnlyCommittedEntriesAfterLastApplied) {
    LogManager log;
    InMemoryMetadataStore metadata_store;
    StateMachine state_machine;

    log.Append(RaftLogEntry{.term = 1, .index = 1,
                            .command = EncodePutCommand("color", "blue")});
    log.Append(RaftLogEntry{.term = 1, .index = 2,
                            .command = EncodePutCommand("shape", "circle")});
    metadata_store.SaveCommitIndex(1);

    CommittedLogApplier applier(log, metadata_store, state_machine);

    EXPECT_EQ(applier.ApplyOnce(), 1);
    EXPECT_EQ(state_machine.Get("color"), std::optional<std::string>("blue"));
    EXPECT_EQ(state_machine.Get("shape"), std::nullopt);
    EXPECT_EQ(metadata_store.LastApplied(), 1);
}

TEST(CommittedLogApplierTest, BackgroundLoopAppliesWhenCommitIndexAdvances) {
    LogManager log;
    InMemoryMetadataStore metadata_store;
    StateMachine state_machine;

    log.Append(RaftLogEntry{.term = 1, .index = 1,
                            .command = EncodePutCommand("color", "blue")});

    CommittedLogApplier applier(log, metadata_store, state_machine,
                                std::chrono::milliseconds(5));
    applier.Start();
    metadata_store.SaveCommitIndex(1);

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(1);
    while (std::chrono::steady_clock::now() < deadline &&
           state_machine.Get("color") != std::optional<std::string>("blue")) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    applier.Stop();

    EXPECT_EQ(state_machine.Get("color"), std::optional<std::string>("blue"));
    EXPECT_EQ(metadata_store.LastApplied(), 1);
}

}  // namespace
}  // namespace raftkv
