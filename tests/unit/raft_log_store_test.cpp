#include "storage/raft_log_store.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>

#include "raft/raft_node.h"
#include "storage/kv_command.h"
#include "storage/state_machine.h"

namespace raftkv {
namespace {

std::filesystem::path TempDbPath(const std::string& test_name) {
    return std::filesystem::temp_directory_path() / ("raftkv_" + test_name);
}

class RaftLogStoreTest : public testing::Test {
 protected:
    void SetUp() override {
        db_path_ = TempDbPath(testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(db_path_);
    }

    void TearDown() override {
        std::filesystem::remove_all(db_path_);
    }

    std::filesystem::path db_path_;
};

class NoopPeerClient final : public PeerClient {
 public:
    core::RequestVoteResponse RequestVote(const NodeId&, const core::RequestVoteRequest& request) override {
        return core::RequestVoteResponse{.term = request.term, .vote_granted = false};
    }

    core::AppendEntriesResponse AppendEntries(const NodeId&,
                                              const core::AppendEntriesRequest& request) override {
        return core::AppendEntriesResponse{.term = request.term, .success = false};
    }
};

TEST_F(RaftLogStoreTest, AppendsAndReadsLogEntries) {
    RaftLogStore store(db_path_);

    store.Append(RaftLogEntry{.term = 1, .index = 1, .command = "put a 1"});
    store.Append(RaftLogEntry{.term = 2, .index = 2, .command = "put b 2"});

    const auto first = store.Read(1);
    const auto second = store.Read(2);

    ASSERT_TRUE(first.has_value());
    ASSERT_TRUE(second.has_value());
    EXPECT_EQ(first->term, 1);
    EXPECT_EQ(first->command, "put a 1");
    EXPECT_EQ(second->term, 2);
    EXPECT_EQ(second->command, "put b 2");
    EXPECT_FALSE(store.Read(3).has_value());
}

TEST_F(RaftLogStoreTest, PersistsEntriesAcrossReopen) {
    {
        RaftLogStore store(db_path_);
        store.Append(RaftLogEntry{.term = 3, .index = 7, .command = "put durable yes"});
    }

    RaftLogStore reopened(db_path_);
    const auto entry = reopened.Read(7);

    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->term, 3);
    EXPECT_EQ(entry->index, 7);
    EXPECT_EQ(entry->command, "put durable yes");
}

TEST_F(RaftLogStoreTest, TruncatesFromIndexWithSyncWrite) {
    RaftLogStore store(db_path_);
    store.Append(RaftLogEntry{.term = 1, .index = 1, .command = "a"});
    store.Append(RaftLogEntry{.term = 1, .index = 2, .command = "b"});
    store.Append(RaftLogEntry{.term = 2, .index = 3, .command = "c"});

    store.TruncateFrom(2);

    EXPECT_TRUE(store.Read(1).has_value());
    EXPECT_FALSE(store.Read(2).has_value());
    EXPECT_FALSE(store.Read(3).has_value());
}

TEST_F(RaftLogStoreTest, PersistsTermAndVoteAcrossReopen) {
    {
        RaftLogStore store(db_path_);
        store.SaveTermAndVote(9, NodeId("node-2"));
    }

    RaftLogStore reopened(db_path_);

    EXPECT_EQ(reopened.CurrentTerm(), 9);
    EXPECT_EQ(reopened.VotedFor(), std::optional<NodeId>("node-2"));

    reopened.SaveTermAndVote(10, std::nullopt);

    EXPECT_EQ(reopened.CurrentTerm(), 10);
    EXPECT_EQ(reopened.VotedFor(), std::nullopt);
}

TEST_F(RaftLogStoreTest, PersistsCommitAndLastAppliedAcrossReopen) {
    {
        RaftLogStore store(db_path_);
        store.SaveCommitIndex(4);
        store.SaveLastApplied(3);
    }

    RaftLogStore reopened(db_path_);

    EXPECT_EQ(reopened.CommitIndex(), 4);
    EXPECT_EQ(reopened.LastApplied(), 3);
}

TEST_F(RaftLogStoreTest, PersistsSnapshotMetadataAcrossReopen) {
    {
        RaftLogStore store(db_path_);
        store.SaveSnapshotMetadata(11, 3);
    }

    RaftLogStore reopened(db_path_);

    EXPECT_EQ(reopened.LastIncludedIndex(), 11);
    EXPECT_EQ(reopened.LastIncludedTerm(), 3);
}

TEST_F(RaftLogStoreTest, RaftNodePersistsElectionTermAndVote) {
    NoopPeerClient transport;
    {
        auto log_store = std::make_unique<RaftLogStore>(db_path_);
        RaftMetadataStore* metadata_store = log_store.get();
        RaftNode node("node-1", {}, transport, std::move(log_store), metadata_store);

        node.Tick();

        EXPECT_EQ(node.CurrentRole(), Role::Leader);
        EXPECT_EQ(node.CurrentTerm(), 1);
    }

    RaftLogStore reopened(db_path_);
    EXPECT_EQ(reopened.CurrentTerm(), 1);
    EXPECT_EQ(reopened.VotedFor(), std::optional<NodeId>("node-1"));
}

TEST_F(RaftLogStoreTest, RaftNodePersistsAcceptedAppendEntriesBeforeSuccess) {
    NoopPeerClient transport;
    {
        auto log_store = std::make_unique<RaftLogStore>(db_path_);
        RaftMetadataStore* metadata_store = log_store.get();
        RaftNode node("node-1", {}, transport, std::move(log_store), metadata_store);

        core::AppendEntriesRequest request;
        request.term = 2;
        request.leader_id = "node-2";
        request.entries = {RaftLogEntry{.term = 2, .index = 1, .command = "put a 1"}};
        core::AppendEntriesResponse response;

        node.OnAppendEntries(request, &response);

        EXPECT_TRUE(response.success);
        EXPECT_EQ(response.term, 2);
    }

    RaftLogStore reopened(db_path_);
    const auto entry = reopened.Read(1);
    ASSERT_TRUE(entry.has_value());
    EXPECT_EQ(entry->term, 2);
    EXPECT_EQ(entry->command, "put a 1");
    EXPECT_EQ(reopened.CurrentTerm(), 2);
}

TEST_F(RaftLogStoreTest, ReplaysCommittedEntriesAfterLastApplied) {
    {
        RaftLogStore store(db_path_);
        store.Append(RaftLogEntry{.term = 1, .index = 1,
                                  .command = EncodePutCommand("color", "blue")});
        store.Append(RaftLogEntry{.term = 1, .index = 2,
                                  .command = EncodePutCommand("shape", "circle")});
        store.Append(RaftLogEntry{.term = 1, .index = 3,
                                  .command = EncodeDeleteCommand("color")});
        store.SaveCommitIndex(3);
        store.SaveLastApplied(1);
    }

    RaftLogStore reopened(db_path_);
    StateMachine state_machine;
    state_machine.Put("color", "blue");

    const LogIndex last_applied = ReplayCommittedEntries(reopened, reopened, state_machine);

    EXPECT_EQ(last_applied, 3);
    EXPECT_EQ(reopened.LastApplied(), 3);
    EXPECT_EQ(state_machine.Get("color"), std::nullopt);
    EXPECT_EQ(state_machine.Get("shape"), std::optional<std::string>("circle"));
}

}  // namespace
}  // namespace raftkv
