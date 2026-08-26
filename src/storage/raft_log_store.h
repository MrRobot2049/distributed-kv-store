#pragma once

#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include <rocksdb/db.h>

#include "common/log_entry.h"
#include "common/types.h"
#include "raft/log_manager.h"
#include "raft/raft_metadata_store.h"

namespace raftkv {

class RaftLogStore final : public RaftLog, public RaftMetadataStore {
 public:
    explicit RaftLogStore(std::filesystem::path db_path);
    ~RaftLogStore();

    RaftLogStore(const RaftLogStore&) = delete;
    RaftLogStore& operator=(const RaftLogStore&) = delete;

    void Append(RaftLogEntry entry) override;
    std::optional<RaftLogEntry> Read(LogIndex index) const;
    bool MatchesAt(LogIndex index, Term term) const override;
    void TruncateFrom(LogIndex index) override;
    void CompactBefore(LogIndex index) override;
    void SetBaseIndex(LogIndex index) override;
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

 private:
    static std::string IndexKey(LogIndex index);
    static LogIndex DecodeIndexKey(std::string_view key);

    rocksdb::ColumnFamilyHandle* MetadataCf() const;
    rocksdb::ColumnFamilyHandle* RaftLogCf() const;

    std::filesystem::path db_path_;
    std::unique_ptr<rocksdb::DB> db_;
    std::vector<rocksdb::ColumnFamilyHandle*> column_families_;
};

}  // namespace raftkv
