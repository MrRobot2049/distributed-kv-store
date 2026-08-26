#pragma once

#include <vector>

#include "common/log_entry.h"
#include "common/types.h"

namespace raftkv {

class RaftLog {
 public:
    virtual ~RaftLog() = default;

    virtual void Append(RaftLogEntry entry) = 0;
    virtual bool MatchesAt(LogIndex index, Term term) const = 0;
    virtual void TruncateFrom(LogIndex index) = 0;
    virtual void CompactBefore(LogIndex index) = 0;
    virtual void SetBaseIndex(LogIndex index) = 0;
    virtual std::vector<RaftLogEntry> EntriesFrom(LogIndex index) const = 0;
    virtual LogIndex LastIndex() const = 0;
    virtual Term TermAt(LogIndex index) const = 0;
};

class LogManager final : public RaftLog {
 public:
    void Append(RaftLogEntry entry) override;
    bool MatchesAt(LogIndex index, Term term) const override;
    void TruncateFrom(LogIndex index) override;
    void CompactBefore(LogIndex index) override;
    void SetBaseIndex(LogIndex index) override;
    std::vector<RaftLogEntry> EntriesFrom(LogIndex index) const override;
    LogIndex LastIndex() const override;
    Term TermAt(LogIndex index) const override;

 private:
    std::vector<RaftLogEntry> entries_;
    LogIndex base_index_{0};
};

}  // namespace raftkv
