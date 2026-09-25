#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <rocksdb/db.h>

#include "raft/log_manager.h"
#include "raft/raft_metadata_store.h"
#include "storage/state_machine.h"

namespace raftkv {

class RocksDbStore final : public RaftLog, public RaftMetadataStore, public KeyValueStateMachine {
 public:
    explicit RocksDbStore(std::filesystem::path db_path);
    ~RocksDbStore();

    RocksDbStore(const RocksDbStore&) = delete;
    RocksDbStore& operator=(const RocksDbStore&) = delete;

    void Append(RaftLogEntry entry) override;
    std::optional<RaftLogEntry> Read(LogIndex index) const;
    bool MatchesAt(LogIndex index, Term term) const override;
    void TruncateFrom(LogIndex index) override;
    void CompactBefore(LogIndex index) override;
    void SetBaseIndex(LogIndex index) override;
    void CompactLogBefore(LogIndex index);
    std::vector<RaftLogEntry> EntriesFrom(LogIndex index) const override;
    LogIndex LastIndex() const override;
    Term TermAt(LogIndex index) const override;

    void SaveTermAndVote(Term term, std::optional<NodeId> voted_for) override;
    void SaveCommitIndex(LogIndex commit_index) override;
    void SaveLastApplied(LogIndex last_applied) override;
    void SaveSnapshotMetadata(LogIndex last_included_index,
                              Term last_included_term) override;
    Term CurrentTerm() const override;
    std::optional<NodeId> VotedFor() const override;
    LogIndex CommitIndex() const override;
    LogIndex LastApplied() const override;
    LogIndex LastIncludedIndex() const override;
    Term LastIncludedTerm() const override;

    void Put(std::string key, std::string value) override;
    std::optional<std::string> Get(const std::string& key) const override;
    void Delete(const std::string& key) override;
    void CreateCheckpoint(const std::filesystem::path& checkpoint_path) const;
    void ReplaceFromDirectory(const std::filesystem::path& restored_db_path);

 private:
    static std::string IndexKey(LogIndex index);
    static LogIndex DecodeIndexKey(std::string_view key);

    rocksdb::ColumnFamilyHandle* MetadataCf() const;
    rocksdb::ColumnFamilyHandle* RaftLogCf() const;
    rocksdb::ColumnFamilyHandle* KvDataCf() const;
    void Open();
    void Close();

    std::filesystem::path db_path_;
    std::unique_ptr<rocksdb::DB> db_;
    std::vector<rocksdb::ColumnFamilyHandle*> column_families_;
};

}  // namespace raftkv
