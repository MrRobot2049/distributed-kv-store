#pragma once

#include <mutex>
#include <optional>
#include <string>
#include <chrono>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/raft_state.h"
#include "common/types.h"
#include "raft/log_manager.h"
#include "raft/raft_metadata_store.h"
#include "raft/peer_client.h"
#include "raft/raft_messages.h"

namespace raftkv {

class RaftNode {
 public:
    RaftNode(NodeId id, std::vector<NodeId> peers, PeerClient& transport);
    RaftNode(NodeId id, std::vector<NodeId> peers, PeerClient& transport,
             std::unique_ptr<RaftLog> log, RaftMetadataStore* metadata_store = nullptr);
    RaftNode(NodeId id, std::vector<NodeId> peers, PeerClient& transport, RaftLog& log,
             RaftMetadataStore* metadata_store = nullptr);

    void OnRequestVote(const core::RequestVoteRequest& request, core::RequestVoteResponse* response);
    void OnAppendEntries(const core::AppendEntriesRequest& request,
                         core::AppendEntriesResponse* response);
    void OnInstallSnapshot(const core::InstallSnapshotRequest& request,
                           core::InstallSnapshotResponse* response);
    void Tick();
    void SendHeartbeats();
    bool SubmitCommand(std::string command);

    Role CurrentRole() const;
    Term CurrentTerm() const;
    LogIndex CommitIndex() const;
    LogIndex LastLogIndex() const;
    Term LastLogTerm() const;
    LogIndex LastIncludedIndex() const;
    Term LastIncludedTerm() const;
    std::chrono::steady_clock::time_point LastElectionReset() const;
    std::optional<NodeId> CurrentLeader() const;
    std::optional<NodeId> VotedFor() const;

 private:
    void StartElection();
    void BecomeLeader();
    void StepDown(Term new_term);
    void PersistTermAndVote();
    void PersistCommitIndex();
    void PersistSnapshotMetadata();
    bool MatchesPreviousLog(LogIndex index, Term term) const;
    LogIndex EffectiveLastLogIndex() const;
    Term EffectiveLastLogTerm() const;
    bool IsCandidateLogAtLeastAsUpToDate(const core::RequestVoteRequest& request) const;
    bool ReplicateEntryToMajority(const RaftLogEntry& entry);
    bool ReplicateLogToPeer(const NodeId& peer);
    void BroadcastCommitIndex();
    void ResetElectionTimer();

    NodeId id_;
    std::vector<NodeId> peers_;
    PeerClient& transport_;
    std::unique_ptr<RaftLog> owned_log_;
    RaftLog& log_;
    RaftMetadataStore* metadata_store_{nullptr};

    Term current_term_{0};
    LogIndex commit_index_{0};
    LogIndex last_included_index_{0};
    Term last_included_term_{0};
    std::unordered_map<NodeId, LogIndex> next_index_;
    std::unordered_map<NodeId, LogIndex> match_index_;
    std::optional<NodeId> voted_for_;
    Role role_{Role::Follower};
    std::optional<NodeId> current_leader_;
    std::chrono::steady_clock::time_point election_reset_at_{std::chrono::steady_clock::now()};
    mutable std::mutex state_mutex_;
};

}  // namespace raftkv
