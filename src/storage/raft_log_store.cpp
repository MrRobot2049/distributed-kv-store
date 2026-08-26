#include "storage/raft_log_store.h"

#include <algorithm>
#include <array>
#include <stdexcept>
#include <string_view>

#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/write_batch.h>

#include "raft.pb.h"

namespace raftkv {
namespace {

constexpr std::string_view kMetadataColumnFamily = "metadata";
constexpr std::string_view kRaftLogColumnFamily = "raft_log";
constexpr std::string_view kKvDataColumnFamily = "kv_data";
constexpr std::string_view kCurrentTermKey = "current_term";
constexpr std::string_view kVotedForKey = "voted_for";
constexpr std::string_view kCommitIndexKey = "commit_index";
constexpr std::string_view kLastAppliedKey = "last_applied";
constexpr std::string_view kLastIncludedIndexKey = "last_included_index";
constexpr std::string_view kLastIncludedTermKey = "last_included_term";

void ThrowIfNotOk(const rocksdb::Status& status, std::string_view operation) {
    if (!status.ok()) {
        throw std::runtime_error(std::string(operation) + ": " + status.ToString());
    }
}

std::string EncodeUint64(std::uint64_t value) {
    std::string encoded(sizeof(std::uint64_t), '\0');
    for (std::size_t i = 0; i < sizeof(std::uint64_t); ++i) {
        encoded[i] = static_cast<char>((value >> ((sizeof(std::uint64_t) - i - 1) * 8)) & 0xff);
    }

    return encoded;
}

std::uint64_t DecodeUint64(std::string_view encoded) {
    if (encoded.size() != sizeof(std::uint64_t)) {
        throw std::runtime_error("invalid uint64 encoding");
    }

    std::uint64_t value = 0;
    for (const char byte : encoded) {
        value <<= 8;
        value |= static_cast<unsigned char>(byte);
    }

    return value;
}

std::string Serialize(const RaftLogEntry& entry) {
    raftkv::LogEntry proto_entry;
    proto_entry.set_term(entry.term);
    proto_entry.set_index(entry.index);
    proto_entry.set_command(entry.command);

    std::string serialized;
    if (!proto_entry.SerializeToString(&serialized)) {
        throw std::runtime_error("failed to serialize raft log entry");
    }

    return serialized;
}

RaftLogEntry Deserialize(std::string_view serialized) {
    raftkv::LogEntry proto_entry;
    if (!proto_entry.ParseFromArray(serialized.data(), static_cast<int>(serialized.size()))) {
        throw std::runtime_error("failed to deserialize raft log entry");
    }

    return RaftLogEntry{
        .term = proto_entry.term(),
        .index = proto_entry.index(),
        .command = proto_entry.command(),
    };
}

}  // namespace

RaftLogStore::RaftLogStore(std::filesystem::path db_path) : db_path_(std::move(db_path)) {
    if (db_path_.has_parent_path()) {
        std::filesystem::create_directories(db_path_.parent_path());
    }

    rocksdb::Options options;
    options.create_if_missing = true;
    options.create_missing_column_families = true;

    std::vector<rocksdb::ColumnFamilyDescriptor> descriptors = {
        rocksdb::ColumnFamilyDescriptor(rocksdb::kDefaultColumnFamilyName,
                                        rocksdb::ColumnFamilyOptions()),
        rocksdb::ColumnFamilyDescriptor(std::string(kMetadataColumnFamily),
                                        rocksdb::ColumnFamilyOptions()),
        rocksdb::ColumnFamilyDescriptor(std::string(kRaftLogColumnFamily),
                                        rocksdb::ColumnFamilyOptions()),
        rocksdb::ColumnFamilyDescriptor(std::string(kKvDataColumnFamily),
                                        rocksdb::ColumnFamilyOptions()),
    };

    ThrowIfNotOk(rocksdb::DB::Open(options, db_path_.string(), descriptors, &column_families_,
                                   &db_),
                 "open rocksdb raft log store");
}

RaftLogStore::~RaftLogStore() {
    for (auto* column_family : column_families_) {
        if (column_family != nullptr) {
            db_->DestroyColumnFamilyHandle(column_family);
        }
    }
}

void RaftLogStore::Append(RaftLogEntry entry) {
    rocksdb::WriteOptions options;
    options.sync = true;

    ThrowIfNotOk(db_->Put(options, RaftLogCf(), IndexKey(entry.index), Serialize(entry)),
                 "append raft log entry");
}

std::optional<RaftLogEntry> RaftLogStore::Read(LogIndex index) const {
    std::string serialized;
    const rocksdb::Status status =
        db_->Get(rocksdb::ReadOptions(), RaftLogCf(), IndexKey(index), &serialized);
    if (status.IsNotFound()) {
        return std::nullopt;
    }

    ThrowIfNotOk(status, "read raft log entry");
    return Deserialize(serialized);
}

bool RaftLogStore::MatchesAt(LogIndex index, Term term) const {
    if (index == 0) {
        return term == 0;
    }

    const auto entry = Read(index);
    return entry.has_value() && entry->term == term;
}

void RaftLogStore::TruncateFrom(LogIndex index) {
    rocksdb::WriteBatch batch;
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(read_options, RaftLogCf()));
    for (iterator->Seek(IndexKey(index)); iterator->Valid(); iterator->Next()) {
        ThrowIfNotOk(batch.Delete(RaftLogCf(), iterator->key()), "stage raft log delete");
    }
    ThrowIfNotOk(iterator->status(), "scan raft log for truncate");

    rocksdb::WriteOptions write_options;
    write_options.sync = true;
    ThrowIfNotOk(db_->Write(write_options, &batch), "truncate raft log");
}

void RaftLogStore::CompactBefore(LogIndex index) {
    if (index <= 1) {
        return;
    }

    rocksdb::WriteBatch batch;
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(read_options, RaftLogCf()));
    for (iterator->SeekToFirst(); iterator->Valid(); iterator->Next()) {
        if (DecodeIndexKey(iterator->key().ToStringView()) >= index) {
            break;
        }
        ThrowIfNotOk(batch.Delete(RaftLogCf(), iterator->key()),
                     "stage compacted raft log delete");
    }
    ThrowIfNotOk(iterator->status(), "scan raft log for compaction");

    rocksdb::WriteOptions write_options;
    write_options.sync = true;
    ThrowIfNotOk(db_->Write(write_options, &batch), "compact raft log");
}

void RaftLogStore::SetBaseIndex(LogIndex) {}

std::vector<RaftLogEntry> RaftLogStore::EntriesFrom(LogIndex index) const {
    if (index == 0) {
        index = 1;
    }

    std::vector<RaftLogEntry> entries;
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(read_options, RaftLogCf()));
    for (iterator->Seek(IndexKey(index)); iterator->Valid(); iterator->Next()) {
        entries.push_back(Deserialize(iterator->value().ToStringView()));
    }
    ThrowIfNotOk(iterator->status(), "scan raft log entries");
    return entries;
}

LogIndex RaftLogStore::LastIndex() const {
    rocksdb::ReadOptions read_options;
    std::unique_ptr<rocksdb::Iterator> iterator(db_->NewIterator(read_options, RaftLogCf()));
    iterator->SeekToLast();
    if (!iterator->Valid()) {
        ThrowIfNotOk(iterator->status(), "read last raft log index");
        return 0;
    }

    const LogIndex index = DecodeIndexKey(iterator->key().ToStringView());
    ThrowIfNotOk(iterator->status(), "read last raft log index");
    return index;
}

Term RaftLogStore::TermAt(LogIndex index) const {
    if (index == 0) {
        return 0;
    }

    const auto entry = Read(index);
    if (!entry.has_value()) {
        throw std::out_of_range("log index is not present");
    }

    return entry->term;
}

void RaftLogStore::SaveTermAndVote(Term term, std::optional<NodeId> voted_for) {
    rocksdb::WriteBatch batch;
    ThrowIfNotOk(batch.Put(MetadataCf(), rocksdb::Slice(kCurrentTermKey), EncodeUint64(term)),
                 "stage current term");
    if (voted_for.has_value()) {
        ThrowIfNotOk(batch.Put(MetadataCf(), rocksdb::Slice(kVotedForKey), *voted_for),
                     "stage voted_for");
    } else {
        ThrowIfNotOk(batch.Delete(MetadataCf(), rocksdb::Slice(kVotedForKey)),
                     "stage voted_for delete");
    }

    rocksdb::WriteOptions options;
    options.sync = true;
    ThrowIfNotOk(db_->Write(options, &batch), "save term and vote");
}

void RaftLogStore::SaveCommitIndex(LogIndex commit_index) {
    rocksdb::WriteOptions options;
    options.sync = true;
    ThrowIfNotOk(db_->Put(options, MetadataCf(), rocksdb::Slice(kCommitIndexKey),
                          EncodeUint64(commit_index)),
                 "save commit index");
}

void RaftLogStore::SaveLastApplied(LogIndex last_applied) {
    rocksdb::WriteOptions options;
    options.sync = true;
    ThrowIfNotOk(db_->Put(options, MetadataCf(), rocksdb::Slice(kLastAppliedKey),
                          EncodeUint64(last_applied)),
                 "save last applied");
}

void RaftLogStore::SaveSnapshotMetadata(LogIndex last_included_index,
                                        Term last_included_term) {
    rocksdb::WriteBatch batch;
    ThrowIfNotOk(batch.Put(MetadataCf(), rocksdb::Slice(kLastIncludedIndexKey),
                           EncodeUint64(last_included_index)),
                 "stage last included index");
    ThrowIfNotOk(batch.Put(MetadataCf(), rocksdb::Slice(kLastIncludedTermKey),
                           EncodeUint64(last_included_term)),
                 "stage last included term");

    rocksdb::WriteOptions options;
    options.sync = true;
    ThrowIfNotOk(db_->Write(options, &batch), "save snapshot metadata");
}

Term RaftLogStore::CurrentTerm() const {
    std::string encoded;
    const rocksdb::Status status =
        db_->Get(rocksdb::ReadOptions(), MetadataCf(), rocksdb::Slice(kCurrentTermKey), &encoded);
    if (status.IsNotFound()) {
        return 0;
    }

    ThrowIfNotOk(status, "read current term");
    return DecodeUint64(encoded);
}

std::optional<NodeId> RaftLogStore::VotedFor() const {
    std::string voted_for;
    const rocksdb::Status status =
        db_->Get(rocksdb::ReadOptions(), MetadataCf(), rocksdb::Slice(kVotedForKey), &voted_for);
    if (status.IsNotFound()) {
        return std::nullopt;
    }

    ThrowIfNotOk(status, "read voted_for");
    return voted_for;
}

LogIndex RaftLogStore::CommitIndex() const {
    std::string encoded;
    const rocksdb::Status status =
        db_->Get(rocksdb::ReadOptions(), MetadataCf(), rocksdb::Slice(kCommitIndexKey), &encoded);
    if (status.IsNotFound()) {
        return 0;
    }

    ThrowIfNotOk(status, "read commit index");
    return DecodeUint64(encoded);
}

LogIndex RaftLogStore::LastApplied() const {
    std::string encoded;
    const rocksdb::Status status =
        db_->Get(rocksdb::ReadOptions(), MetadataCf(), rocksdb::Slice(kLastAppliedKey), &encoded);
    if (status.IsNotFound()) {
        return 0;
    }

    ThrowIfNotOk(status, "read last applied");
    return DecodeUint64(encoded);
}

LogIndex RaftLogStore::LastIncludedIndex() const {
    std::string encoded;
    const rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), MetadataCf(),
                                            rocksdb::Slice(kLastIncludedIndexKey), &encoded);
    if (status.IsNotFound()) {
        return 0;
    }

    ThrowIfNotOk(status, "read last included index");
    return DecodeUint64(encoded);
}

Term RaftLogStore::LastIncludedTerm() const {
    std::string encoded;
    const rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), MetadataCf(),
                                            rocksdb::Slice(kLastIncludedTermKey), &encoded);
    if (status.IsNotFound()) {
        return 0;
    }

    ThrowIfNotOk(status, "read last included term");
    return DecodeUint64(encoded);
}

std::string RaftLogStore::IndexKey(LogIndex index) {
    return EncodeUint64(index);
}

LogIndex RaftLogStore::DecodeIndexKey(std::string_view key) {
    return DecodeUint64(key);
}

rocksdb::ColumnFamilyHandle* RaftLogStore::MetadataCf() const {
    return column_families_.at(1);
}

rocksdb::ColumnFamilyHandle* RaftLogStore::RaftLogCf() const {
    return column_families_.at(2);
}

}  // namespace raftkv
