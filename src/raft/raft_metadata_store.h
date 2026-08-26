#pragma once

#include <optional>

#include "common/types.h"

namespace raftkv {

class RaftMetadataStore {
 public:
    virtual ~RaftMetadataStore() = default;

    virtual void SaveTermAndVote(Term term, std::optional<NodeId> voted_for) = 0;
    virtual void SaveCommitIndex(LogIndex commit_index) = 0;
    virtual void SaveLastApplied(LogIndex last_applied) = 0;
    virtual void SaveSnapshotMetadata(LogIndex, Term) {}
    virtual Term CurrentTerm() const = 0;
    virtual std::optional<NodeId> VotedFor() const = 0;
    virtual LogIndex CommitIndex() const = 0;
    virtual LogIndex LastApplied() const = 0;
    virtual LogIndex LastIncludedIndex() const { return 0; }
    virtual Term LastIncludedTerm() const { return 0; }
};

}  // namespace raftkv
