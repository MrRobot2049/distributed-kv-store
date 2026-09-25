#include "storage/snapshot_manager.h"

#include <algorithm>
#include <cstdint>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace raftkv {
namespace {

constexpr const char* kMetadataFile = "snapshot.meta";
constexpr std::string_view kSnapshotPayloadMagic = "RAFTKV_SNAPSHOT_V1";

void AppendUint32(std::string* output, std::uint32_t value) {
    for (int shift = 24; shift >= 0; shift -= 8) {
        output->push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

void AppendUint64(std::string* output, std::uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        output->push_back(static_cast<char>((value >> shift) & 0xff));
    }
}

std::uint32_t ReadUint32(std::string_view payload, std::size_t* offset) {
    if (payload.size() - *offset < sizeof(std::uint32_t)) {
        throw std::runtime_error("snapshot payload is truncated");
    }

    std::uint32_t value = 0;
    for (std::size_t i = 0; i < sizeof(std::uint32_t); ++i) {
        value <<= 8;
        value |= static_cast<unsigned char>(payload[(*offset)++]);
    }
    return value;
}

std::uint64_t ReadUint64(std::string_view payload, std::size_t* offset) {
    if (payload.size() - *offset < sizeof(std::uint64_t)) {
        throw std::runtime_error("snapshot payload is truncated");
    }

    std::uint64_t value = 0;
    for (std::size_t i = 0; i < sizeof(std::uint64_t); ++i) {
        value <<= 8;
        value |= static_cast<unsigned char>(payload[(*offset)++]);
    }
    return value;
}

std::string ReadFile(const std::filesystem::path& path) {
    std::ifstream input(path, std::ios::binary);
    if (!input) {
        throw std::runtime_error("open snapshot file for read failed");
    }

    return {std::istreambuf_iterator<char>(input), std::istreambuf_iterator<char>()};
}

void WriteFile(const std::filesystem::path& path, std::string_view content) {
    std::filesystem::create_directories(path.parent_path());
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output) {
        throw std::runtime_error("open snapshot file for write failed");
    }

    output.write(content.data(), static_cast<std::streamsize>(content.size()));
}

void WriteMetadataFile(const std::filesystem::path& path, SnapshotMetadata metadata) {
    std::ofstream output(path, std::ios::binary | std::ios::trunc);
    if (!output) {
        throw std::runtime_error("open snapshot metadata for write failed");
    }

    output << metadata.last_included_index << '\n' << metadata.last_included_term << '\n';
}

}  // namespace

SnapshotManager::SnapshotManager(std::filesystem::path snapshots_dir)
    : snapshots_dir_(std::move(snapshots_dir)) {}

std::filesystem::path SnapshotManager::CreateSnapshot(const RocksDbStore& store,
                                                      SnapshotMetadata metadata) {
    std::filesystem::create_directories(snapshots_dir_);
    const std::filesystem::path snapshot_path =
        snapshots_dir_ / SnapshotName(metadata.last_included_index);
    std::filesystem::remove_all(snapshot_path);

    store.CreateCheckpoint(snapshot_path);
    WriteMetadataFile(MetadataPath(snapshot_path), metadata);
    return snapshot_path;
}

std::filesystem::path SnapshotManager::CreateSnapshotAndCompact(
    RocksDbStore& store, SnapshotMetadata metadata) {
    const std::filesystem::path snapshot_path = CreateSnapshot(store, metadata);
    store.CompactLogBefore(metadata.last_included_index);
    return snapshot_path;
}

std::optional<SnapshotMetadata> SnapshotManager::ReadMetadata(
    const std::filesystem::path& snapshot_path) const {
    std::ifstream input(MetadataPath(snapshot_path), std::ios::binary);
    if (!input) {
        return std::nullopt;
    }

    SnapshotMetadata metadata;
    input >> metadata.last_included_index >> metadata.last_included_term;
    if (!input) {
        return std::nullopt;
    }

    return metadata;
}

std::optional<std::filesystem::path> SnapshotManager::LatestSnapshot() const {
    if (!std::filesystem::exists(snapshots_dir_)) {
        return std::nullopt;
    }

    std::vector<std::filesystem::path> snapshots;
    for (const auto& entry : std::filesystem::directory_iterator(snapshots_dir_)) {
        if (entry.is_directory() && ReadMetadata(entry.path()).has_value()) {
            snapshots.push_back(entry.path());
        }
    }

    if (snapshots.empty()) {
        return std::nullopt;
    }

    return *std::max_element(
        snapshots.begin(), snapshots.end(), [this](const auto& left, const auto& right) {
            return ReadMetadata(left)->last_included_index <
                   ReadMetadata(right)->last_included_index;
        });
}

std::string SnapshotManager::ReadSnapshotPayload(
    const std::filesystem::path& snapshot_path) const {
    std::vector<std::filesystem::path> files;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(snapshot_path)) {
        if (entry.is_regular_file()) {
            files.push_back(entry.path());
        }
    }
    std::sort(files.begin(), files.end());

    std::string payload(kSnapshotPayloadMagic);
    AppendUint32(&payload, static_cast<std::uint32_t>(files.size()));
    for (const auto& file : files) {
        const std::filesystem::path relative_path = std::filesystem::relative(file, snapshot_path);
        const std::string relative_path_string = relative_path.generic_string();
        const std::string content = ReadFile(file);

        AppendUint32(&payload, static_cast<std::uint32_t>(relative_path_string.size()));
        payload.append(relative_path_string);
        AppendUint64(&payload, static_cast<std::uint64_t>(content.size()));
        payload.append(content);
    }

    return payload;
}

std::filesystem::path SnapshotManager::RestoreSnapshotPayload(
    std::string_view payload, LogIndex last_included_index) const {
    if (!payload.starts_with(kSnapshotPayloadMagic)) {
        throw std::runtime_error("invalid snapshot payload");
    }

    std::filesystem::create_directories(snapshots_dir_);
    const std::filesystem::path restored_path =
        snapshots_dir_ / (SnapshotName(last_included_index) + "_incoming");
    std::filesystem::remove_all(restored_path);
    std::filesystem::create_directories(restored_path);

    std::size_t offset = kSnapshotPayloadMagic.size();
    const std::uint32_t file_count = ReadUint32(payload, &offset);
    for (std::uint32_t i = 0; i < file_count; ++i) {
        const std::uint32_t path_size = ReadUint32(payload, &offset);
        if (payload.size() - offset < path_size) {
            throw std::runtime_error("snapshot payload path is truncated");
        }
        const std::string relative_path(payload.substr(offset, path_size));
        offset += path_size;

        const std::uint64_t content_size = ReadUint64(payload, &offset);
        if (payload.size() - offset < content_size) {
            throw std::runtime_error("snapshot payload content is truncated");
        }

        WriteFile(restored_path / relative_path,
                  payload.substr(offset, static_cast<std::size_t>(content_size)));
        offset += static_cast<std::size_t>(content_size);
    }

    if (offset != payload.size()) {
        throw std::runtime_error("snapshot payload has trailing bytes");
    }

    return restored_path;
}

void SnapshotManager::InstallSnapshotPayload(RocksDbStore& store, std::string_view payload,
                                             SnapshotMetadata metadata) const {
    const std::filesystem::path restored_path =
        RestoreSnapshotPayload(payload, metadata.last_included_index);
    store.ReplaceFromDirectory(restored_path);
    store.SaveSnapshotMetadata(metadata.last_included_index, metadata.last_included_term);
    store.SaveCommitIndex(metadata.last_included_index);
    store.SaveLastApplied(metadata.last_included_index);
}

void SnapshotManager::PromoteRestoredSnapshot(
    const std::filesystem::path& restored_snapshot_path,
    const std::filesystem::path& live_db_path) {
    if (!std::filesystem::exists(restored_snapshot_path) ||
        !std::filesystem::is_directory(restored_snapshot_path)) {
        throw std::runtime_error("restored snapshot path does not exist");
    }

    if (live_db_path.empty()) {
        throw std::runtime_error("live database path is empty");
    }

    if (live_db_path.has_parent_path()) {
        std::filesystem::create_directories(live_db_path.parent_path());
    }
    const std::filesystem::path backup_path = live_db_path.string() + ".pre_snapshot";
    std::filesystem::remove_all(backup_path);

    bool backup_created = false;
    try {
        if (std::filesystem::exists(live_db_path)) {
            std::filesystem::rename(live_db_path, backup_path);
            backup_created = true;
        }

        std::filesystem::rename(restored_snapshot_path, live_db_path);
        std::filesystem::remove_all(backup_path);
    } catch (...) {
        if (backup_created && !std::filesystem::exists(live_db_path) &&
            std::filesystem::exists(backup_path)) {
            std::filesystem::rename(backup_path, live_db_path);
        }
        throw;
    }
}

std::string SnapshotManager::SnapshotName(LogIndex index) {
    return "snapshot_" + std::to_string(index);
}

std::filesystem::path SnapshotManager::MetadataPath(const std::filesystem::path& snapshot_path) {
    return snapshot_path / kMetadataFile;
}

}  // namespace raftkv
