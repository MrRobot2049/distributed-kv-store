#include "raft/raft_node.h"

#include <gtest/gtest.h>

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <unordered_map>
#include <unordered_set>

namespace raftkv {
namespace {

class InMemoryPeerClient : public PeerClient {
 public:
    void Register(const NodeId& id, RaftNode* node) {
        nodes_[id] = node;
    }

    void DropRequestsTo(const NodeId& id) {
        dropped_nodes_.insert(id);
    }

    void RestoreRequestsTo(const NodeId& id) {
        dropped_nodes_.erase(id);
    }

    void DropRequestsBetween(const NodeId& first, const NodeId& second) {
        dropped_edges_.insert({first, second});
        dropped_edges_.insert({second, first});
    }

    void RestoreRequestsBetween(const NodeId& first, const NodeId& second) {
        dropped_edges_.erase({first, second});
        dropped_edges_.erase({second, first});
    }

    core::RequestVoteResponse RequestVote(const NodeId& peer,
                                          const core::RequestVoteRequest& request) override {
        if (ShouldDrop(request.candidate_id, peer)) {
            return core::RequestVoteResponse{.term = request.term, .vote_granted = false};
        }

        core::RequestVoteResponse response;
        nodes_.at(peer)->OnRequestVote(request, &response);
        return response;
    }

    core::AppendEntriesResponse AppendEntries(
        const NodeId& peer, const core::AppendEntriesRequest& request) override {
        if (ShouldDrop(request.leader_id, peer)) {
            return core::AppendEntriesResponse{.term = request.term, .success = false};
        }

        core::AppendEntriesResponse response;
        nodes_.at(peer)->OnAppendEntries(request, &response);
        return response;
    }

 private:
    bool ShouldDrop(const NodeId& source, const NodeId& target) const {
        return dropped_nodes_.contains(target) || dropped_edges_.contains({source, target});
    }

    std::unordered_map<NodeId, RaftNode*> nodes_;
    std::unordered_set<NodeId> dropped_nodes_;
    std::set<std::pair<NodeId, NodeId>> dropped_edges_;
};

TEST(RaftNodeTest, SingleNodeBecomesLeaderImmediately) {
    InMemoryPeerClient transport;
    RaftNode node("node-1", {}, transport);

    node.Tick();

    EXPECT_EQ(node.CurrentRole(), Role::Leader);
    EXPECT_EQ(node.CurrentTerm(), 1);
    ASSERT_TRUE(node.CurrentLeader().has_value());
    EXPECT_EQ(*node.CurrentLeader(), "node-1");
}

TEST(RaftNodeTest, ThreeNodesConvergeOnOneLeader) {
    InMemoryPeerClient transport;
    RaftNode node1("node-1", {"node-2", "node-3"}, transport);
    RaftNode node2("node-2", {"node-1", "node-3"}, transport);
    RaftNode node3("node-3", {"node-1", "node-2"}, transport);
    transport.Register("node-1", &node1);
    transport.Register("node-2", &node2);
    transport.Register("node-3", &node3);

    node1.Tick();

    EXPECT_EQ(node1.CurrentRole(), Role::Leader);
    EXPECT_EQ(node2.CurrentRole(), Role::Follower);
    EXPECT_EQ(node3.CurrentRole(), Role::Follower);
    EXPECT_EQ(node2.VotedFor(), std::optional<NodeId>("node-1"));
    EXPECT_EQ(node3.VotedFor(), std::optional<NodeId>("node-1"));
}

TEST(RaftNodeTest, HigherTermAppendEntriesCausesStepDown) {
    InMemoryPeerClient transport;
    RaftNode node("node-1", {}, transport);
    node.Tick();
    ASSERT_EQ(node.CurrentRole(), Role::Leader);

    core::AppendEntriesRequest request;
    request.term = 4;
    request.leader_id = "node-2";
    core::AppendEntriesResponse response;

    node.OnAppendEntries(request, &response);

    EXPECT_TRUE(response.success);
    EXPECT_EQ(response.term, 4);
    EXPECT_EQ(node.CurrentRole(), Role::Follower);
    EXPECT_EQ(node.CurrentTerm(), 4);
    EXPECT_EQ(node.CurrentLeader(), std::optional<NodeId>("node-2"));
}

TEST(RaftNodeTest, StaleCandidateIsRejected) {
    InMemoryPeerClient transport;
    RaftNode node("node-1", {}, transport);

    core::AppendEntriesRequest append_request;
    append_request.term = 3;
    append_request.leader_id = "node-2";
    core::AppendEntriesResponse append_response;
    node.OnAppendEntries(append_request, &append_response);

    core::RequestVoteRequest vote_request;
    vote_request.term = 2;
    vote_request.candidate_id = "node-3";
    core::RequestVoteResponse vote_response;
    node.OnRequestVote(vote_request, &vote_response);

    EXPECT_FALSE(vote_response.vote_granted);
    EXPECT_EQ(vote_response.term, 3);
}

TEST(RaftNodeTest, CandidateStaysCandidateWithoutMajority) {
    InMemoryPeerClient transport;
    RaftNode node1("node-1", {"node-2", "node-3"}, transport);
    RaftNode node2("node-2", {"node-1", "node-3"}, transport);
    RaftNode node3("node-3", {"node-1", "node-2"}, transport);
    transport.Register("node-1", &node1);
    transport.Register("node-2", &node2);
    transport.Register("node-3", &node3);
    transport.DropRequestsTo("node-2");
    transport.DropRequestsTo("node-3");

    node1.Tick();

    EXPECT_EQ(node1.CurrentRole(), Role::Candidate);
    EXPECT_EQ(node1.CurrentLeader(), std::nullopt);
}

TEST(RaftNodeTest, LeaderReplicatesCommandToFollowers) {
    InMemoryPeerClient transport;
    RaftNode node1("node-1", {"node-2", "node-3"}, transport);
    RaftNode node2("node-2", {"node-1", "node-3"}, transport);
    RaftNode node3("node-3", {"node-1", "node-2"}, transport);
    transport.Register("node-1", &node1);
    transport.Register("node-2", &node2);
    transport.Register("node-3", &node3);
    node1.Tick();

    ASSERT_TRUE(node1.SubmitCommand("put color blue"));

    EXPECT_EQ(node1.LastLogIndex(), 1);
    EXPECT_EQ(node2.LastLogIndex(), 1);
    EXPECT_EQ(node3.LastLogIndex(), 1);
    EXPECT_EQ(node1.CommitIndex(), 1);
}

TEST(RaftNodeTest, CommitIndexAdvancesOnMajority) {
    InMemoryPeerClient transport;
    RaftNode node1("node-1", {"node-2", "node-3"}, transport);
    RaftNode node2("node-2", {"node-1", "node-3"}, transport);
    RaftNode node3("node-3", {"node-1", "node-2"}, transport);
    transport.Register("node-1", &node1);
    transport.Register("node-2", &node2);
    transport.Register("node-3", &node3);
    transport.DropRequestsTo("node-3");
    node1.Tick();

    ASSERT_TRUE(node1.SubmitCommand("put a 1"));

    EXPECT_EQ(node1.CommitIndex(), 1);
    EXPECT_EQ(node2.LastLogIndex(), 1);
    EXPECT_EQ(node3.LastLogIndex(), 0);
}

TEST(RaftNodeTest, CommandIsNotCommittedWithoutMajority) {
    InMemoryPeerClient transport;
    RaftNode node1("node-1", {"node-2", "node-3"}, transport);
    RaftNode node2("node-2", {"node-1", "node-3"}, transport);
    RaftNode node3("node-3", {"node-1", "node-2"}, transport);
    transport.Register("node-1", &node1);
    transport.Register("node-2", &node2);
    transport.Register("node-3", &node3);
    node1.Tick();
    transport.DropRequestsTo("node-2");
    transport.DropRequestsTo("node-3");

    EXPECT_FALSE(node1.SubmitCommand("put lonely value"));

    EXPECT_EQ(node1.CommitIndex(), 0);
    EXPECT_EQ(node1.LastLogIndex(), 1);
}

TEST(RaftNodeTest, FollowerCatchesUpAfterMissedEntry) {
    InMemoryPeerClient transport;
    RaftNode node1("node-1", {"node-2", "node-3"}, transport);
    RaftNode node2("node-2", {"node-1", "node-3"}, transport);
    RaftNode node3("node-3", {"node-1", "node-2"}, transport);
    transport.Register("node-1", &node1);
    transport.Register("node-2", &node2);
    transport.Register("node-3", &node3);
    transport.DropRequestsTo("node-3");
    node1.Tick();
    ASSERT_TRUE(node1.SubmitCommand("put a 1"));
    ASSERT_EQ(node3.LastLogIndex(), 0);

    transport.RestoreRequestsTo("node-3");
    ASSERT_TRUE(node1.SubmitCommand("put b 2"));

    EXPECT_EQ(node1.CommitIndex(), 2);
    EXPECT_EQ(node2.CommitIndex(), 2);
    EXPECT_EQ(node3.CommitIndex(), 2);
    EXPECT_EQ(node3.LastLogIndex(), 2);
}

TEST(RaftNodeTest, MajorityPartitionElectsNewLeaderAndHealsOldLeader) {
    InMemoryPeerClient transport;
    RaftNode node1("node-1", {"node-2", "node-3", "node-4", "node-5"}, transport);
    RaftNode node2("node-2", {"node-1", "node-3", "node-4", "node-5"}, transport);
    RaftNode node3("node-3", {"node-1", "node-2", "node-4", "node-5"}, transport);
    RaftNode node4("node-4", {"node-1", "node-2", "node-3", "node-5"}, transport);
    RaftNode node5("node-5", {"node-1", "node-2", "node-3", "node-4"}, transport);
    transport.Register("node-1", &node1);
    transport.Register("node-2", &node2);
    transport.Register("node-3", &node3);
    transport.Register("node-4", &node4);
    transport.Register("node-5", &node5);

    node1.Tick();
    ASSERT_EQ(node1.CurrentRole(), Role::Leader);

    for (const NodeId& minority_node : {NodeId("node-1"), NodeId("node-5")}) {
        for (const NodeId& majority_node : {NodeId("node-2"), NodeId("node-3"),
                                            NodeId("node-4")}) {
            transport.DropRequestsBetween(minority_node, majority_node);
        }
    }

    EXPECT_FALSE(node1.SubmitCommand("put isolated value"));
    EXPECT_EQ(node1.CommitIndex(), 0);
    EXPECT_EQ(node5.CommitIndex(), 0);

    node2.Tick();
    ASSERT_EQ(node2.CurrentRole(), Role::Leader);
    ASSERT_EQ(node2.CurrentTerm(), 2);
    ASSERT_TRUE(node2.SubmitCommand("put majority value"));
    EXPECT_EQ(node2.CommitIndex(), 1);
    EXPECT_EQ(node3.CommitIndex(), 1);
    EXPECT_EQ(node4.CommitIndex(), 1);

    for (const NodeId& minority_node : {NodeId("node-1"), NodeId("node-5")}) {
        for (const NodeId& majority_node : {NodeId("node-2"), NodeId("node-3"),
                                            NodeId("node-4")}) {
            transport.RestoreRequestsBetween(minority_node, majority_node);
        }
    }

    ASSERT_TRUE(node2.SubmitCommand("put healed value"));
    EXPECT_EQ(node1.CurrentRole(), Role::Follower);
    EXPECT_EQ(node1.CurrentTerm(), 2);
    EXPECT_EQ(node1.CommitIndex(), 2);
    EXPECT_EQ(node1.LastLogIndex(), 2);
    EXPECT_EQ(node1.LastLogTerm(), 2);
    EXPECT_EQ(node5.CommitIndex(), 2);
    EXPECT_EQ(node5.LastLogIndex(), 2);
}

TEST(RaftNodeTest, AppendEntriesRejectsMismatchedPreviousLog) {
    InMemoryPeerClient transport;
    RaftNode follower("node-2", {}, transport);

    core::AppendEntriesRequest request;
    request.term = 1;
    request.leader_id = "node-1";
    request.prev_log_index = 1;
    request.prev_log_term = 99;
    request.entries = {RaftLogEntry{.term = 1, .index = 2, .command = "put b 2"}};
    core::AppendEntriesResponse response;

    follower.OnAppendEntries(request, &response);

    EXPECT_FALSE(response.success);
    EXPECT_EQ(response.conflict_index, 1);
    EXPECT_EQ(follower.LastLogIndex(), 0);
}

TEST(RaftNodeTest, AppendEntriesTruncatesConflictingFollowerEntries) {
    InMemoryPeerClient transport;
    RaftNode follower("node-2", {}, transport);

    core::AppendEntriesRequest first_request;
    first_request.term = 1;
    first_request.leader_id = "node-1";
    first_request.entries = {
        RaftLogEntry{.term = 1, .index = 1, .command = "put a 1"},
        RaftLogEntry{.term = 1, .index = 2, .command = "put b 2"},
    };
    core::AppendEntriesResponse first_response;
    follower.OnAppendEntries(first_request, &first_response);
    ASSERT_TRUE(first_response.success);

    core::AppendEntriesRequest overwrite_request;
    overwrite_request.term = 2;
    overwrite_request.leader_id = "node-3";
    overwrite_request.prev_log_index = 1;
    overwrite_request.prev_log_term = 1;
    overwrite_request.entries = {RaftLogEntry{.term = 2, .index = 2, .command = "put b 3"}};
    core::AppendEntriesResponse overwrite_response;
    follower.OnAppendEntries(overwrite_request, &overwrite_response);

    EXPECT_TRUE(overwrite_response.success);
    EXPECT_EQ(follower.LastLogIndex(), 2);
    EXPECT_EQ(follower.LastLogTerm(), 2);
}

TEST(RaftNodeTest, StaleCandidateWithOlderLogIsRejected) {
    InMemoryPeerClient transport;
    RaftNode follower("node-2", {}, transport);

    core::AppendEntriesRequest append_request;
    append_request.term = 2;
    append_request.leader_id = "node-1";
    append_request.entries = {RaftLogEntry{.term = 2, .index = 1, .command = "put a 1"}};
    core::AppendEntriesResponse append_response;
    follower.OnAppendEntries(append_request, &append_response);
    ASSERT_TRUE(append_response.success);

    core::RequestVoteRequest vote_request;
    vote_request.term = 3;
    vote_request.candidate_id = "node-3";
    vote_request.last_log_index = 0;
    vote_request.last_log_term = 0;
    core::RequestVoteResponse vote_response;

    follower.OnRequestVote(vote_request, &vote_response);

    EXPECT_FALSE(vote_response.vote_granted);
    EXPECT_EQ(vote_response.term, 3);
}

TEST(RaftNodeTest, InstallSnapshotUpdatesTermLeaderAndSnapshotBoundary) {
    InMemoryPeerClient transport;
    RaftNode node("node-2", {}, transport);

    core::InstallSnapshotRequest request;
    request.term = 5;
    request.leader_id = "node-1";
    request.last_included_index = 10;
    request.last_included_term = 4;
    request.data = "snapshot-bytes";
    core::InstallSnapshotResponse response;

    node.OnInstallSnapshot(request, &response);

    EXPECT_EQ(response.term, 5);
    EXPECT_EQ(node.CurrentRole(), Role::Follower);
    EXPECT_EQ(node.CurrentTerm(), 5);
    EXPECT_EQ(node.CurrentLeader(), std::optional<NodeId>("node-1"));
    EXPECT_EQ(node.CommitIndex(), 10);
    EXPECT_EQ(node.LastIncludedIndex(), 10);
    EXPECT_EQ(node.LastIncludedTerm(), 4);
}

TEST(RaftNodeTest, InstallSnapshotRejectsStaleTerm) {
    InMemoryPeerClient transport;
    RaftNode node("node-2", {}, transport);

    core::AppendEntriesRequest append_request;
    append_request.term = 7;
    append_request.leader_id = "node-1";
    core::AppendEntriesResponse append_response;
    node.OnAppendEntries(append_request, &append_response);
    ASSERT_EQ(node.CurrentTerm(), 7);

    core::InstallSnapshotRequest snapshot_request;
    snapshot_request.term = 6;
    snapshot_request.leader_id = "node-3";
    snapshot_request.last_included_index = 20;
    snapshot_request.last_included_term = 6;
    core::InstallSnapshotResponse snapshot_response;

    node.OnInstallSnapshot(snapshot_request, &snapshot_response);

    EXPECT_EQ(snapshot_response.term, 7);
    EXPECT_EQ(node.LastIncludedIndex(), 0);
    EXPECT_EQ(node.CurrentLeader(), std::optional<NodeId>("node-1"));
}

TEST(RaftNodeTest, AppendEntriesCanContinueFromSnapshotBoundary) {
    InMemoryPeerClient transport;
    RaftNode node("node-2", {}, transport);

    core::InstallSnapshotRequest snapshot_request;
    snapshot_request.term = 4;
    snapshot_request.leader_id = "node-1";
    snapshot_request.last_included_index = 10;
    snapshot_request.last_included_term = 3;
    core::InstallSnapshotResponse snapshot_response;
    node.OnInstallSnapshot(snapshot_request, &snapshot_response);
    ASSERT_EQ(node.LastIncludedIndex(), 10);

    core::AppendEntriesRequest append_request;
    append_request.term = 4;
    append_request.leader_id = "node-1";
    append_request.prev_log_index = 10;
    append_request.prev_log_term = 3;
    append_request.entries = {RaftLogEntry{.term = 4, .index = 11,
                                           .command = "after snapshot"}};
    append_request.leader_commit = 11;
    core::AppendEntriesResponse append_response;

    node.OnAppendEntries(append_request, &append_response);

    EXPECT_TRUE(append_response.success);
    EXPECT_EQ(node.LastLogIndex(), 11);
    EXPECT_EQ(node.LastLogTerm(), 4);
    EXPECT_EQ(node.CommitIndex(), 11);
}

TEST(RaftNodeTest, AppendEntriesRejectsWrongSnapshotBoundaryTerm) {
    InMemoryPeerClient transport;
    RaftNode node("node-2", {}, transport);

    core::InstallSnapshotRequest snapshot_request;
    snapshot_request.term = 4;
    snapshot_request.leader_id = "node-1";
    snapshot_request.last_included_index = 10;
    snapshot_request.last_included_term = 3;
    core::InstallSnapshotResponse snapshot_response;
    node.OnInstallSnapshot(snapshot_request, &snapshot_response);

    core::AppendEntriesRequest append_request;
    append_request.term = 4;
    append_request.leader_id = "node-1";
    append_request.prev_log_index = 10;
    append_request.prev_log_term = 2;
    core::AppendEntriesResponse append_response;

    node.OnAppendEntries(append_request, &append_response);

    EXPECT_FALSE(append_response.success);
    EXPECT_EQ(node.LastLogIndex(), 10);
    EXPECT_EQ(node.LastLogTerm(), 3);
}

}  // namespace
}  // namespace raftkv
