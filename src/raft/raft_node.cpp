#include "raft/raft_node.h"

#include <algorithm>
#include <utility>

namespace raftkv {

RaftNode::RaftNode(NodeId id, std::vector<NodeId> peers, PeerClient& transport)
    : RaftNode(std::move(id), std::move(peers), transport,
               std::make_unique<LogManager>()) {}

RaftNode::RaftNode(NodeId id, std::vector<NodeId> peers, PeerClient& transport,
                   std::unique_ptr<RaftLog> log, RaftMetadataStore* metadata_store)
    : id_(std::move(id)),
      peers_(std::move(peers)),
      transport_(transport),
      owned_log_(std::move(log)),
      log_(*owned_log_),
      metadata_store_(metadata_store) {
    if (metadata_store_ != nullptr) {
        current_term_ = metadata_store_->CurrentTerm();
        voted_for_ = metadata_store_->VotedFor();
        commit_index_ = metadata_store_->CommitIndex();
        last_included_index_ = metadata_store_->LastIncludedIndex();
        last_included_term_ = metadata_store_->LastIncludedTerm();
    }
}

RaftNode::RaftNode(NodeId id, std::vector<NodeId> peers, PeerClient& transport,
                   RaftLog& log, RaftMetadataStore* metadata_store)
    : id_(std::move(id)),
      peers_(std::move(peers)),
      transport_(transport),
      log_(log),
      metadata_store_(metadata_store) {
    if (metadata_store_ != nullptr) {
        current_term_ = metadata_store_->CurrentTerm();
        voted_for_ = metadata_store_->VotedFor();
        commit_index_ = metadata_store_->CommitIndex();
        last_included_index_ = metadata_store_->LastIncludedIndex();
        last_included_term_ = metadata_store_->LastIncludedTerm();
    }
}

void RaftNode::OnRequestVote(const core::RequestVoteRequest& request,
                             core::RequestVoteResponse* response) {
    std::lock_guard<std::mutex> lock(state_mutex_);

    if (request.term > current_term_) {
        StepDown(request.term);
    }

    response->term = current_term_;
    response->vote_granted = false;

    if (request.term < current_term_) {
        return;
    }

    const bool vote_available = !voted_for_.has_value() || voted_for_ == request.candidate_id;
    if (vote_available && IsCandidateLogAtLeastAsUpToDate(request)) {
        voted_for_ = request.candidate_id;
        current_leader_.reset();
        PersistTermAndVote();
        ResetElectionTimer();
        response->vote_granted = true;
    }
}

void RaftNode::OnAppendEntries(const core::AppendEntriesRequest& request,
                               core::AppendEntriesResponse* response) {
    std::lock_guard<std::mutex> lock(state_mutex_);

    if (request.term > current_term_) {
        StepDown(request.term);
    }

    response->term = current_term_;
    response->success = false;
    response->conflict_index = 0;

    if (request.term < current_term_) {
        return;
    }

    if (role_ != Role::Follower) {
        role_ = Role::Follower;
    }

    current_leader_ = request.leader_id;
    ResetElectionTimer();
    if (!MatchesPreviousLog(request.prev_log_index, request.prev_log_term)) {
        response->conflict_index = EffectiveLastLogIndex() + 1;
        return;
    }

    for (const auto& entry : request.entries) {
        if (entry.index <= last_included_index_) {
            continue;
        }

        if (entry.index <= log_.LastIndex() && log_.TermAt(entry.index) == entry.term) {
            continue;
        }

        log_.Append(entry);
    }

    if (request.leader_commit > commit_index_) {
        commit_index_ = std::min(request.leader_commit, EffectiveLastLogIndex());
        PersistCommitIndex();
    }

    response->term = current_term_;
    response->success = true;
}

void RaftNode::OnInstallSnapshot(const core::InstallSnapshotRequest& request,
                                 core::InstallSnapshotResponse* response) {
    std::lock_guard<std::mutex> lock(state_mutex_);

    if (request.term > current_term_) {
        StepDown(request.term);
    }

    response->term = current_term_;
    if (request.term < current_term_) {
        return;
    }

    role_ = Role::Follower;
    current_leader_ = request.leader_id;
    ResetElectionTimer();

    if (request.last_included_index > last_included_index_) {
        last_included_index_ = request.last_included_index;
        last_included_term_ = request.last_included_term;
        commit_index_ = std::max(commit_index_, last_included_index_);
        log_.SetBaseIndex(last_included_index_);
        log_.CompactBefore(last_included_index_);
        PersistSnapshotMetadata();
        PersistCommitIndex();
    }

    response->term = current_term_;
}

void RaftNode::Tick() {
    StartElection();
}

void RaftNode::SendHeartbeats() {
    for (const auto& peer : peers_) {
        core::AppendEntriesRequest request;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (role_ != Role::Leader) {
                return;
            }

            const LogIndex prev_log_index =
                next_index_.contains(peer) ? next_index_[peer] - 1 : log_.LastIndex();
            request.term = current_term_;
            request.leader_id = id_;
            request.prev_log_index = prev_log_index;
            request.prev_log_term = log_.TermAt(prev_log_index);
            request.leader_commit = commit_index_;
        }

        core::AppendEntriesResponse response = transport_.AppendEntries(peer, request);
        if (response.term > request.term) {
            std::lock_guard<std::mutex> lock(state_mutex_);
            StepDown(response.term);
            return;
        }
    }
}

bool RaftNode::SubmitCommand(std::string command) {
    RaftLogEntry entry;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        if (role_ != Role::Leader) {
            return false;
        }

        entry.term = current_term_;
        entry.index = log_.LastIndex() + 1;
        entry.command = std::move(command);
        log_.Append(entry);
        match_index_[id_] = entry.index;
    }

    return ReplicateEntryToMajority(entry);
}

Role RaftNode::CurrentRole() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return role_;
}

Term RaftNode::CurrentTerm() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return current_term_;
}

LogIndex RaftNode::CommitIndex() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return commit_index_;
}

LogIndex RaftNode::LastLogIndex() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return EffectiveLastLogIndex();
}

Term RaftNode::LastLogTerm() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return EffectiveLastLogTerm();
}

LogIndex RaftNode::LastIncludedIndex() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return last_included_index_;
}

Term RaftNode::LastIncludedTerm() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return last_included_term_;
}

std::chrono::steady_clock::time_point RaftNode::LastElectionReset() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return election_reset_at_;
}

std::optional<NodeId> RaftNode::CurrentLeader() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return current_leader_;
}

std::optional<NodeId> RaftNode::VotedFor() const {
    std::lock_guard<std::mutex> lock(state_mutex_);
    return voted_for_;
}

void RaftNode::StartElection() {
    Term election_term = 0;
    {
        std::lock_guard<std::mutex> lock(state_mutex_);
        role_ = Role::Candidate;
        current_term_ += 1;
        voted_for_ = id_;
        current_leader_.reset();
        PersistTermAndVote();
        ResetElectionTimer();
        election_term = current_term_;
    }

    std::size_t votes = 1;
    const std::size_t majority = (peers_.size() + 1) / 2 + 1;

    if (votes >= majority) {
        BecomeLeader();
        return;
    }

    for (const auto& peer : peers_) {
        core::RequestVoteRequest request;
        request.term = election_term;
        request.candidate_id = id_;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            request.last_log_index = log_.LastIndex();
            request.last_log_term = log_.TermAt(request.last_log_index);
        }

        core::RequestVoteResponse response = transport_.RequestVote(peer, request);
        if (response.term > election_term) {
            std::lock_guard<std::mutex> lock(state_mutex_);
            StepDown(response.term);
            return;
        }

        if (response.vote_granted) {
            votes += 1;
        }
    }

    if (votes >= majority) {
        BecomeLeader();
    }
}

void RaftNode::BecomeLeader() {
    std::lock_guard<std::mutex> lock(state_mutex_);
    if (role_ == Role::Candidate) {
        role_ = Role::Leader;
        current_leader_ = id_;
        const LogIndex next_index = EffectiveLastLogIndex() + 1;
        match_index_[id_] = EffectiveLastLogIndex();
        for (const auto& peer : peers_) {
            next_index_[peer] = next_index;
            match_index_[peer] = 0;
        }
    }
}

void RaftNode::StepDown(Term new_term) {
    current_term_ = new_term;
    role_ = Role::Follower;
    voted_for_.reset();
    current_leader_.reset();
    next_index_.clear();
    match_index_.clear();
    ResetElectionTimer();
    PersistTermAndVote();
}

void RaftNode::PersistTermAndVote() {
    if (metadata_store_ != nullptr) {
        metadata_store_->SaveTermAndVote(current_term_, voted_for_);
    }
}

void RaftNode::PersistCommitIndex() {
    if (metadata_store_ != nullptr) {
        metadata_store_->SaveCommitIndex(commit_index_);
    }
}

void RaftNode::PersistSnapshotMetadata() {
    if (metadata_store_ != nullptr) {
        metadata_store_->SaveSnapshotMetadata(last_included_index_, last_included_term_);
    }
}

bool RaftNode::IsCandidateLogAtLeastAsUpToDate(
    const core::RequestVoteRequest& request) const {
    const LogIndex local_last_log_index = EffectiveLastLogIndex();
    const Term local_last_log_term = EffectiveLastLogTerm();

    if (request.last_log_term != local_last_log_term) {
        return request.last_log_term > local_last_log_term;
    }

    return request.last_log_index >= local_last_log_index;
}

bool RaftNode::ReplicateEntryToMajority(const RaftLogEntry& entry) {
    std::size_t replicated = 1;
    const std::size_t majority = (peers_.size() + 1) / 2 + 1;

    for (const auto& peer : peers_) {
        if (ReplicateLogToPeer(peer)) {
            replicated += 1;
        }
    }

    if (replicated >= majority) {
        bool advanced_commit = false;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (role_ == Role::Leader && entry.index > commit_index_) {
                commit_index_ = entry.index;
                PersistCommitIndex();
                advanced_commit = true;
            }
        }

        if (advanced_commit) {
            BroadcastCommitIndex();
        }

        return advanced_commit;
    }

    return false;
}

bool RaftNode::ReplicateLogToPeer(const NodeId& peer) {
    while (true) {
        core::AppendEntriesRequest request;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (role_ != Role::Leader) {
                return false;
            }

            const LogIndex next_index = next_index_.contains(peer) ? next_index_[peer] : 1;
            request.term = current_term_;
            request.leader_id = id_;
            request.prev_log_index = next_index - 1;
            request.prev_log_term = request.prev_log_index == last_included_index_
                                        ? last_included_term_
                                        : log_.TermAt(request.prev_log_index);
            request.entries = log_.EntriesFrom(next_index);
            request.leader_commit = commit_index_;
        }

        core::AppendEntriesResponse response = transport_.AppendEntries(peer, request);
        if (response.term > request.term) {
            std::lock_guard<std::mutex> lock(state_mutex_);
            StepDown(response.term);
            return false;
        }

        if (response.success) {
            std::lock_guard<std::mutex> lock(state_mutex_);
            match_index_[peer] = EffectiveLastLogIndex();
            next_index_[peer] = EffectiveLastLogIndex() + 1;
            return true;
        }

        std::lock_guard<std::mutex> lock(state_mutex_);
        const LogIndex next_index = next_index_.contains(peer) ? next_index_[peer] : 1;
        if (next_index <= 1) {
            return false;
        }

        next_index_[peer] = response.conflict_index > 0 && response.conflict_index < next_index
                                ? response.conflict_index
                                : next_index - 1;
    }
}

void RaftNode::BroadcastCommitIndex() {
    for (const auto& peer : peers_) {
        core::AppendEntriesRequest request;
        {
            std::lock_guard<std::mutex> lock(state_mutex_);
            if (role_ != Role::Leader) {
                return;
            }

            request.term = current_term_;
            request.leader_id = id_;
            request.prev_log_index = EffectiveLastLogIndex();
            request.prev_log_term = EffectiveLastLogTerm();
            request.leader_commit = commit_index_;
        }

        core::AppendEntriesResponse response = transport_.AppendEntries(peer, request);
        if (response.term > request.term) {
            std::lock_guard<std::mutex> lock(state_mutex_);
            StepDown(response.term);
            return;
        }
    }
}

void RaftNode::ResetElectionTimer() {
    election_reset_at_ = std::chrono::steady_clock::now();
}

bool RaftNode::MatchesPreviousLog(LogIndex index, Term term) const {
    if (index == last_included_index_) {
        return term == last_included_term_;
    }

    return log_.MatchesAt(index, term);
}

LogIndex RaftNode::EffectiveLastLogIndex() const {
    return std::max(log_.LastIndex(), last_included_index_);
}

Term RaftNode::EffectiveLastLogTerm() const {
    const LogIndex last_log_index = log_.LastIndex();
    if (last_log_index > last_included_index_) {
        return log_.TermAt(last_log_index);
    }

    return last_included_term_;
}

}  // namespace raftkv
