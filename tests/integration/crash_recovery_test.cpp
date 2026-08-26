#include <gtest/gtest.h>

#include <filesystem>
#include <optional>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

#include "storage/kv_command.h"
#include "storage/rocksdb_store.h"

namespace raftkv {
namespace {

std::filesystem::path TempDbPath(const std::string& test_name) {
    return std::filesystem::temp_directory_path() / ("raftkv_crash_" + test_name);
}

void WriteDurableStateAndCrash(const std::filesystem::path& db_path) {
    try {
        RocksDbStore store(db_path);
        store.SaveTermAndVote(6, NodeId("node-2"));
        store.SaveSnapshotMetadata(9, 4);
        store.Append(RaftLogEntry{.term = 6, .index = 10,
                                  .command = EncodePutCommand("color", "blue")});
        store.Append(RaftLogEntry{.term = 6, .index = 11,
                                  .command = EncodePutCommand("shape", "circle")});
        store.Append(RaftLogEntry{.term = 6, .index = 12,
                                  .command = EncodeDeleteCommand("color")});
        store.SaveCommitIndex(12);
        store.Put("color", "blue");
        store.SaveLastApplied(10);
    } catch (...) {
        _exit(2);
    }

    _exit(0);
}

class CrashRecoveryTest : public testing::Test {
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

TEST_F(CrashRecoveryTest, ReopensSyncedStateAfterAbruptProcessExit) {
    const pid_t child_pid = fork();
    ASSERT_NE(child_pid, -1);

    if (child_pid == 0) {
        WriteDurableStateAndCrash(db_path_);
    }

    int status = 0;
    ASSERT_EQ(waitpid(child_pid, &status, 0), child_pid);
    ASSERT_TRUE(WIFEXITED(status));
    ASSERT_EQ(WEXITSTATUS(status), 0);

    RocksDbStore reopened(db_path_);
    EXPECT_EQ(reopened.CurrentTerm(), 6);
    EXPECT_EQ(reopened.VotedFor(), std::optional<NodeId>("node-2"));
    EXPECT_EQ(reopened.LastIncludedIndex(), 9);
    EXPECT_EQ(reopened.LastIncludedTerm(), 4);
    EXPECT_EQ(reopened.CommitIndex(), 12);
    EXPECT_EQ(reopened.LastApplied(), 10);
    EXPECT_EQ(reopened.Get("color"), std::optional<std::string>("blue"));

    ASSERT_TRUE(reopened.Read(10).has_value());
    ASSERT_TRUE(reopened.Read(11).has_value());
    ASSERT_TRUE(reopened.Read(12).has_value());

    EXPECT_EQ(ReplayCommittedEntries(reopened, reopened, reopened), 12);
    EXPECT_EQ(reopened.LastApplied(), 12);
    EXPECT_EQ(reopened.Get("color"), std::nullopt);
    EXPECT_EQ(reopened.Get("shape"), std::optional<std::string>("circle"));
}

}  // namespace
}  // namespace raftkv
