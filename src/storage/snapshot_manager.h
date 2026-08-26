#pragma once

#include <filesystem>
#include <optional>
#include <string>

#include "common/types.h"
#include "storage/rocksdb_store.h"

namespace raftkv {

struct SnapshotMetadata {
    LogIndex last_included_index{0};
    Term last_included_term{0};
};

class SnapshotManager {
 public:
    explicit SnapshotManager(std::filesystem::path snapshots_dir);

    std::filesystem::path CreateSnapshot(const RocksDbStore& store,
                                         SnapshotMetadata metadata);
    std::filesystem::path CreateSnapshotAndCompact(RocksDbStore& store,
                                                   SnapshotMetadata metadata);
    std::optional<SnapshotMetadata> ReadMetadata(
        const std::filesystem::path& snapshot_path) const;
    std::optional<std::filesystem::path> LatestSnapshot() const;
    std::string ReadSnapshotPayload(const std::filesystem::path& snapshot_path) const;
    std::filesystem::path RestoreSnapshotPayload(std::string_view payload,
                                                 LogIndex last_included_index) const;
    static void PromoteRestoredSnapshot(const std::filesystem::path& restored_snapshot_path,
                                        const std::filesystem::path& live_db_path);

 private:
    static std::string SnapshotName(LogIndex index);
    static std::filesystem::path MetadataPath(const std::filesystem::path& snapshot_path);

    std::filesystem::path snapshots_dir_;
};

}  // namespace raftkv
