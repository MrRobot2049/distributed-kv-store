#include "rpc/kv_service_impl.h"

#include <gtest/gtest.h>

#include <string>
#include <unordered_map>
#include <unordered_set>

namespace raftkv {
namespace {

class InMemoryPeerClient final : public PeerClient {
 public:
    void Register(const NodeId& id, RaftNode* node) {
        nodes_[id] = node;
    }

    core::RequestVoteResponse RequestVote(
        const NodeId& peer, const core::RequestVoteRequest& request) override {
        core::RequestVoteResponse response;
        nodes_.at(peer)->OnRequestVote(request, &response);
        return response;
    }

    core::AppendEntriesResponse AppendEntries(
        const NodeId& peer, const core::AppendEntriesRequest& request) override {
        core::AppendEntriesResponse response;
        nodes_.at(peer)->OnAppendEntries(request, &response);
        return response;
    }

 private:
    std::unordered_map<NodeId, RaftNode*> nodes_;
};

TEST(KVServiceImplTest, FollowerReturnsLeaderRedirectWhenKnown) {
    InMemoryPeerClient transport;
    RaftNode follower("node-2", {}, transport);

    core::AppendEntriesRequest heartbeat;
    heartbeat.term = 1;
    heartbeat.leader_id = "node-1";
    core::AppendEntriesResponse heartbeat_response;
    follower.OnAppendEntries(heartbeat, &heartbeat_response);
    ASSERT_TRUE(heartbeat_response.success);

    StateMachine state_machine;
    KVServiceImpl service(follower, state_machine, [](const NodeId& leader_id) {
        return leader_id == "node-1" ? "127.0.0.1:5001" : "";
    });

    GetRequest request;
    request.set_key("color");
    KVResponse response;

    ASSERT_TRUE(service.Get(nullptr, &request, &response).ok());

    EXPECT_FALSE(response.success());
    EXPECT_FALSE(response.is_leader());
    EXPECT_EQ(response.leader_hint(), "127.0.0.1:5001");
}

TEST(KVServiceImplTest, LeaderPutGetAndDeleteUseRaftCommitPath) {
    InMemoryPeerClient transport;
    RaftNode node1("node-1", {"node-2", "node-3"}, transport);
    RaftNode node2("node-2", {"node-1", "node-3"}, transport);
    RaftNode node3("node-3", {"node-1", "node-2"}, transport);
    transport.Register("node-1", &node1);
    transport.Register("node-2", &node2);
    transport.Register("node-3", &node3);
    node1.Tick();
    ASSERT_EQ(node1.CurrentRole(), Role::Leader);

    StateMachine state_machine;
    KVServiceImpl service(node1, state_machine, [](const NodeId&) { return ""; });

    PutRequest put_request;
    put_request.set_key("color");
    put_request.set_value("blue");
    KVResponse put_response;
    ASSERT_TRUE(service.Put(nullptr, &put_request, &put_response).ok());
    EXPECT_TRUE(put_response.success());
    EXPECT_TRUE(put_response.is_leader());

    GetRequest get_request;
    get_request.set_key("color");
    KVResponse get_response;
    ASSERT_TRUE(service.Get(nullptr, &get_request, &get_response).ok());
    EXPECT_TRUE(get_response.success());
    EXPECT_TRUE(get_response.is_leader());
    EXPECT_EQ(get_response.value(), "blue");

    DeleteRequest delete_request;
    delete_request.set_key("color");
    KVResponse delete_response;
    ASSERT_TRUE(service.Delete(nullptr, &delete_request, &delete_response).ok());
    EXPECT_TRUE(delete_response.success());
    EXPECT_TRUE(delete_response.is_leader());

    KVResponse after_delete_response;
    ASSERT_TRUE(service.Get(nullptr, &get_request, &after_delete_response).ok());
    EXPECT_FALSE(after_delete_response.success());
    EXPECT_TRUE(after_delete_response.is_leader());
}

}  // namespace
}  // namespace raftkv
