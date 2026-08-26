#include "storage/rocksdb_store.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <memory>
#include <string>

#include "raft/raft_node.h"
#include "storage/kv_command.h"

namespace raftkv {
namespace {

std::filesystem::path TempDbPath(const std::string& test_name) {
    return std::filesystem::temp_directory_path() / ("raftkv_combined_" + test_name);
}

class RocksDbStoreTest : public testing::Test {
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

TEST_F(RocksDbStoreTest, PersistsRaftMetadataLogAndKvDataInOneDatabase) {
    {
        RocksDbStore store(db_path_);
        store.SaveTermAndVote(4, NodeId("node-2"));
        store.SaveCommitIndex(1);
        store.SaveLastApplied(0);
        store.Append(RaftLogEntry{.term = 4, .index = 1,
                                  .command = EncodePutCommand("color", "green")});
    }

    RocksDbStore reopened(db_path_);
    EXPECT_EQ(reopened.CurrentTerm(), 4);
    EXPECT_EQ(reopened.VotedFor(), std::optional<NodeId>("node-2"));
    ASSERT_TRUE(reopened.Read(1).has_value());

    EXPECT_EQ(ReplayCommittedEntries(reopened, reopened, reopened), 1);
    EXPECT_EQ(reopened.LastApplied(), 1);
    EXPECT_EQ(reopened.Get("color"), std::optional<std::string>("green"));
}

TEST_F(RocksDbStoreTest, RaftNodeUsesCombinedStoreForDurableState) {
    NoopPeerClient transport;
    {
        RocksDbStore store(db_path_);
        RaftNode node("node-1", {}, transport, store, &store);

        node.Tick();

        EXPECT_EQ(node.CurrentRole(), Role::Leader);
        EXPECT_EQ(node.CurrentTerm(), 1);
    }

    RocksDbStore reopened(db_path_);
    EXPECT_EQ(reopened.CurrentTerm(), 1);
    EXPECT_EQ(reopened.VotedFor(), std::optional<NodeId>("node-1"));
}

TEST_F(RocksDbStoreTest, PersistsSnapshotMetadata) {
    {
        RocksDbStore store(db_path_);
        store.SaveSnapshotMetadata(15, 6);
    }

    RocksDbStore reopened(db_path_);
    EXPECT_EQ(reopened.LastIncludedIndex(), 15);
    EXPECT_EQ(reopened.LastIncludedTerm(), 6);
}

}  // namespace
}  // namespace raftkv
