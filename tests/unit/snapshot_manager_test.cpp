#include "storage/snapshot_manager.h"

#include <gtest/gtest.h>

#include <filesystem>
#include <string>

#include "storage/kv_command.h"

namespace raftkv {
namespace {

std::filesystem::path TempPath(const std::string& test_name) {
    return std::filesystem::temp_directory_path() / ("raftkv_snapshot_" + test_name);
}

class SnapshotManagerTest : public testing::Test {
 protected:
    void SetUp() override {
        root_path_ = TempPath(testing::UnitTest::GetInstance()->current_test_info()->name());
        std::filesystem::remove_all(root_path_);
    }

    void TearDown() override {
        std::filesystem::remove_all(root_path_);
    }

    std::filesystem::path root_path_;
};

TEST_F(SnapshotManagerTest, CreatesCheckpointWithMetadata) {
    const std::filesystem::path db_path = root_path_ / "db";
    const std::filesystem::path snapshots_path = root_path_ / "snapshots";

    {
        RocksDbStore store(db_path);
        store.Put("color", "blue");
        store.Append(RaftLogEntry{.term = 3, .index = 7,
                                  .command = EncodePutCommand("color", "blue")});
        store.SaveCommitIndex(7);
        store.SaveLastApplied(7);

        SnapshotManager manager(snapshots_path);
        const std::filesystem::path snapshot_path =
            manager.CreateSnapshot(store, SnapshotMetadata{.last_included_index = 7,
                                                           .last_included_term = 3});

        const auto metadata = manager.ReadMetadata(snapshot_path);
        ASSERT_TRUE(metadata.has_value());
        EXPECT_EQ(metadata->last_included_index, 7);
        EXPECT_EQ(metadata->last_included_term, 3);
    }

    SnapshotManager manager(snapshots_path);
    const auto latest_snapshot = manager.LatestSnapshot();
    ASSERT_TRUE(latest_snapshot.has_value());

    RocksDbStore checkpoint_store(*latest_snapshot);
    EXPECT_EQ(checkpoint_store.Get("color"), std::optional<std::string>("blue"));
    EXPECT_EQ(checkpoint_store.CommitIndex(), 7);
    EXPECT_EQ(checkpoint_store.LastApplied(), 7);
    ASSERT_TRUE(checkpoint_store.Read(7).has_value());
}

TEST_F(SnapshotManagerTest, LatestSnapshotChoosesHighestIncludedIndex) {
    const std::filesystem::path db_path = root_path_ / "db";
    const std::filesystem::path snapshots_path = root_path_ / "snapshots";

    RocksDbStore store(db_path);
    SnapshotManager manager(snapshots_path);

    manager.CreateSnapshot(store, SnapshotMetadata{.last_included_index = 3,
                                                   .last_included_term = 1});
    manager.CreateSnapshot(store, SnapshotMetadata{.last_included_index = 9,
                                                   .last_included_term = 2});

    const auto latest_snapshot = manager.LatestSnapshot();
    ASSERT_TRUE(latest_snapshot.has_value());

    const auto metadata = manager.ReadMetadata(*latest_snapshot);
    ASSERT_TRUE(metadata.has_value());
    EXPECT_EQ(metadata->last_included_index, 9);
    EXPECT_EQ(metadata->last_included_term, 2);
}

TEST_F(SnapshotManagerTest, SnapshotCompactionRemovesLogEntriesBeforeBoundary) {
    const std::filesystem::path db_path = root_path_ / "db";
    const std::filesystem::path snapshots_path = root_path_ / "snapshots";

    RocksDbStore store(db_path);
    store.Append(RaftLogEntry{.term = 1, .index = 1,
                              .command = EncodePutCommand("a", "1")});
    store.Append(RaftLogEntry{.term = 1, .index = 2,
                              .command = EncodePutCommand("b", "2")});
    store.Append(RaftLogEntry{.term = 2, .index = 3,
                              .command = EncodePutCommand("c", "3")});

    SnapshotManager manager(snapshots_path);
    manager.CreateSnapshotAndCompact(store, SnapshotMetadata{.last_included_index = 3,
                                                             .last_included_term = 2});

    EXPECT_FALSE(store.Read(1).has_value());
    EXPECT_FALSE(store.Read(2).has_value());
    ASSERT_TRUE(store.Read(3).has_value());
    EXPECT_EQ(store.TermAt(3), 2);
}

TEST_F(SnapshotManagerTest, SnapshotPayloadRestoresCheckpointBytes) {
    const std::filesystem::path db_path = root_path_ / "db";
    const std::filesystem::path snapshots_path = root_path_ / "snapshots";
    const std::filesystem::path incoming_path = root_path_ / "incoming";

    {
        RocksDbStore store(db_path);
        store.Put("color", "blue");
        store.SaveCommitIndex(5);
        store.SaveLastApplied(5);

        SnapshotManager manager(snapshots_path);
        const std::filesystem::path snapshot_path =
            manager.CreateSnapshot(store, SnapshotMetadata{.last_included_index = 5,
                                                           .last_included_term = 2});
        const std::string payload = manager.ReadSnapshotPayload(snapshot_path);

        SnapshotManager incoming_manager(incoming_path);
        const std::filesystem::path restored_path =
            incoming_manager.RestoreSnapshotPayload(payload, 5);

        RocksDbStore restored_store(restored_path);
        EXPECT_EQ(restored_store.Get("color"), std::optional<std::string>("blue"));
        EXPECT_EQ(restored_store.CommitIndex(), 5);
        EXPECT_EQ(restored_store.LastApplied(), 5);
    }
}

TEST_F(SnapshotManagerTest, PromotesRestoredSnapshotToClosedDatabasePath) {
    const std::filesystem::path source_db_path = root_path_ / "source_db";
    const std::filesystem::path target_db_path = root_path_ / "target_db";
    const std::filesystem::path snapshots_path = root_path_ / "snapshots";
    const std::filesystem::path incoming_path = root_path_ / "incoming";

    std::string payload;
    {
        RocksDbStore source_store(source_db_path);
        source_store.Put("color", "blue");
        source_store.SaveCommitIndex(8);
        source_store.SaveLastApplied(8);

        SnapshotManager manager(snapshots_path);
        const std::filesystem::path snapshot_path =
            manager.CreateSnapshot(source_store, SnapshotMetadata{.last_included_index = 8,
                                                                  .last_included_term = 4});
        payload = manager.ReadSnapshotPayload(snapshot_path);
    }

    {
        RocksDbStore target_store(target_db_path);
        target_store.Put("color", "red");
        target_store.SaveCommitIndex(2);
        target_store.SaveLastApplied(2);
    }

    SnapshotManager incoming_manager(incoming_path);
    const std::filesystem::path restored_path =
        incoming_manager.RestoreSnapshotPayload(payload, 8);
    SnapshotManager::PromoteRestoredSnapshot(restored_path, target_db_path);

    RocksDbStore promoted_store(target_db_path);
    EXPECT_EQ(promoted_store.Get("color"), std::optional<std::string>("blue"));
    EXPECT_EQ(promoted_store.CommitIndex(), 8);
    EXPECT_EQ(promoted_store.LastApplied(), 8);
    EXPECT_FALSE(std::filesystem::exists(restored_path));
    EXPECT_FALSE(std::filesystem::exists(target_db_path.string() + ".pre_snapshot"));
}

TEST_F(SnapshotManagerTest, InstallsPayloadIntoAnOpenStore) {
    const std::filesystem::path source_db_path = root_path_ / "source_db";
    const std::filesystem::path target_db_path = root_path_ / "target_db";
    const std::filesystem::path snapshots_path = root_path_ / "snapshots";
    const std::filesystem::path incoming_path = root_path_ / "incoming";

    SnapshotManager manager(snapshots_path);
    std::string payload;
    {
        RocksDbStore source_store(source_db_path);
        source_store.Put("color", "blue");
        source_store.SaveCommitIndex(12);
        source_store.SaveLastApplied(12);
        const auto snapshot_path = manager.CreateSnapshot(
            source_store, SnapshotMetadata{.last_included_index = 12,
                                           .last_included_term = 5});
        payload = manager.ReadSnapshotPayload(snapshot_path);
    }

    RocksDbStore target_store(target_db_path);
    target_store.Put("color", "red");
    target_store.Put("stale", "value");
    manager.InstallSnapshotPayload(
        target_store, payload,
        SnapshotMetadata{.last_included_index = 12, .last_included_term = 5});

    EXPECT_EQ(target_store.Get("color"), std::optional<std::string>("blue"));
    EXPECT_FALSE(target_store.Get("stale").has_value());
    EXPECT_EQ(target_store.CommitIndex(), 12);
    EXPECT_EQ(target_store.LastApplied(), 12);
    EXPECT_EQ(target_store.LastIncludedIndex(), 12);
    EXPECT_EQ(target_store.LastIncludedTerm(), 5);
    EXPECT_FALSE(std::filesystem::exists(target_db_path.string() + ".pre_snapshot"));
}

}  // namespace
}  // namespace raftkv
