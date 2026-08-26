#include "rpc/async_raft_server.h"
#include "rpc/peer_client_grpc.h"
#include "server/raft_node_runner.h"

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

namespace raftkv {
namespace {

class NoopPeerClient final : public PeerClient {
 public:
    core::RequestVoteResponse RequestVote(const NodeId&, const core::RequestVoteRequest& request) override {
        return core::RequestVoteResponse{.term = request.term, .vote_granted = false};
    }

    core::AppendEntriesResponse AppendEntries(const NodeId&,
                                              const core::AppendEntriesRequest& request) override {
        return core::AppendEntriesResponse{.term = request.term, .success = false};
    }
};

std::string AllocateLoopbackAddress() {
    const int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return {};
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    address.sin_port = 0;
    if (bind(fd, reinterpret_cast<sockaddr*>(&address), sizeof(address)) != 0) {
        close(fd);
        return {};
    }

    socklen_t length = sizeof(address);
    if (getsockname(fd, reinterpret_cast<sockaddr*>(&address), &length) != 0) {
        close(fd);
        return {};
    }

    const int port = ntohs(address.sin_port);
    close(fd);
    return "127.0.0.1:" + std::to_string(port);
}

TEST(GrpcTransportSmokeTest, AsyncServerHandlesRequestVoteAndAppendEntries) {
    NoopPeerClient noop_transport;
    RaftNode raft_node("node-1", {}, noop_transport);
    AsyncRaftServer server("127.0.0.1:0", raft_node, 2);
    ASSERT_TRUE(server.Start());
    ASSERT_GT(server.selected_port(), 0);

    const std::string address = "127.0.0.1:" + std::to_string(server.selected_port());
    GrpcPeerClient client({PeerEndpoint{.id = "node-1", .address = address}});

    core::RequestVoteRequest vote_request;
    vote_request.term = 1;
    vote_request.candidate_id = "node-2";
    const auto vote_response = client.RequestVote("node-1", vote_request);
    EXPECT_TRUE(vote_response.vote_granted);
    EXPECT_EQ(vote_response.term, 1);

    core::AppendEntriesRequest append_request;
    append_request.term = 2;
    append_request.leader_id = "node-2";
    append_request.entries = {RaftLogEntry{.term = 2, .index = 1, .command = "put x 1"}};
    append_request.leader_commit = 1;
    const auto append_response = client.AppendEntries("node-1", append_request);
    EXPECT_TRUE(append_response.success);
    EXPECT_EQ(append_response.term, 2);
    EXPECT_EQ(raft_node.LastLogIndex(), 1);
    EXPECT_EQ(raft_node.CommitIndex(), 1);

    core::InstallSnapshotRequest snapshot_request;
    snapshot_request.term = 3;
    snapshot_request.leader_id = "node-2";
    snapshot_request.last_included_index = 4;
    snapshot_request.last_included_term = 3;
    snapshot_request.data = "snapshot-bytes";
    const auto snapshot_response = client.InstallSnapshot("node-1", snapshot_request);
    EXPECT_EQ(snapshot_response.term, 3);
    EXPECT_EQ(raft_node.LastIncludedIndex(), 4);
    EXPECT_EQ(raft_node.LastIncludedTerm(), 3);

    server.Shutdown();
}

TEST(GrpcTransportSmokeTest, ThreeAsyncServersElectSingleLeader) {
    const std::string address1 = AllocateLoopbackAddress();
    const std::string address2 = AllocateLoopbackAddress();
    const std::string address3 = AllocateLoopbackAddress();
    ASSERT_FALSE(address1.empty());
    ASSERT_FALSE(address2.empty());
    ASSERT_FALSE(address3.empty());

    GrpcPeerClient transport1({
        PeerEndpoint{.id = "node-2", .address = address2},
        PeerEndpoint{.id = "node-3", .address = address3},
    });
    GrpcPeerClient transport2({
        PeerEndpoint{.id = "node-1", .address = address1},
        PeerEndpoint{.id = "node-3", .address = address3},
    });
    GrpcPeerClient transport3({
        PeerEndpoint{.id = "node-1", .address = address1},
        PeerEndpoint{.id = "node-2", .address = address2},
    });

    RaftNode node1("node-1", {"node-2", "node-3"}, transport1);
    RaftNode node2("node-2", {"node-1", "node-3"}, transport2);
    RaftNode node3("node-3", {"node-1", "node-2"}, transport3);

    AsyncRaftServer server1(address1, node1, 2);
    AsyncRaftServer server2(address2, node2, 2);
    AsyncRaftServer server3(address3, node3, 2);
    ASSERT_TRUE(server1.Start());
    ASSERT_TRUE(server2.Start());
    ASSERT_TRUE(server3.Start());

    RaftNodeRunner runner1(node1);
    RaftNodeRunner runner2(node2);
    RaftNodeRunner runner3(node3);
    runner1.Start();
    runner2.Start();
    runner3.Start();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    std::size_t leader_count = 0;
    do {
        leader_count = 0;
        for (const Role role : {node1.CurrentRole(), node2.CurrentRole(), node3.CurrentRole()}) {
            if (role == Role::Leader) {
                leader_count += 1;
            }
        }

        if (leader_count == 1) {
            break;
        }

        std::this_thread::sleep_for(std::chrono::milliseconds(25));
    } while (std::chrono::steady_clock::now() < deadline);

    runner1.Stop();
    runner2.Stop();
    runner3.Stop();
    server1.Shutdown();
    server2.Shutdown();
    server3.Shutdown();

    EXPECT_EQ(leader_count, 1);
}

}  // namespace
}  // namespace raftkv
