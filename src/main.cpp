#include <iostream>

#ifdef RAFTKV_BUILD_GRPC_TRANSPORT
#include <memory>
#include <filesystem>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "raft/raft_node.h"
#include "rpc/async_raft_server.h"
#include "rpc/kv_service_impl.h"
#include "rpc/peer_client_grpc.h"
#include "server/committed_log_applier.h"
#include "server/raft_node_runner.h"
#ifdef RAFTKV_BUILD_ROCKSDB_STORAGE
#include "storage/kv_command.h"
#include "storage/rocksdb_store.h"
#endif
#include "storage/state_machine.h"
#endif

#ifdef RAFTKV_BUILD_GRPC_TRANSPORT
namespace {

void PrintUsage(const char* program_name) {
    std::cerr << "Usage: " << program_name
              << " --id <node-id> --listen <host:port>"
              << " [--data-dir <path>] [--peer <node-id=host:port> ...]\n";
}

bool ConsumeValue(int argc, char** argv, int* index, std::string* value) {
    if (*index + 1 >= argc) {
        return false;
    }

    *value = argv[++(*index)];
    return true;
}

bool ParsePeer(std::string_view raw_peer, raftkv::PeerEndpoint* endpoint) {
    const std::size_t separator = raw_peer.find('=');
    if (separator == std::string_view::npos || separator == 0 ||
        separator == raw_peer.size() - 1) {
        return false;
    }

    endpoint->id = std::string(raw_peer.substr(0, separator));
    endpoint->address = std::string(raw_peer.substr(separator + 1));
    return true;
}

}  // namespace
#endif

int main(int argc, char** argv) {
#ifndef RAFTKV_BUILD_GRPC_TRANSPORT
    static_cast<void>(argc);
    static_cast<void>(argv);
    std::cout << "Distributed KV Store\n";
    return 0;
#else
    std::string node_id;
    std::string listen_address;
    std::filesystem::path data_dir;
    std::vector<raftkv::PeerEndpoint> peer_endpoints;

    for (int i = 1; i < argc; ++i) {
        const std::string_view arg(argv[i]);
        if (arg == "--id") {
            if (!ConsumeValue(argc, argv, &i, &node_id)) {
                PrintUsage(argv[0]);
                return 1;
            }
        } else if (arg == "--listen") {
            if (!ConsumeValue(argc, argv, &i, &listen_address)) {
                PrintUsage(argv[0]);
                return 1;
            }
        } else if (arg == "--data-dir") {
            std::string raw_data_dir;
            if (!ConsumeValue(argc, argv, &i, &raw_data_dir)) {
                PrintUsage(argv[0]);
                return 1;
            }
            data_dir = raw_data_dir;
        } else if (arg == "--peer") {
            std::string raw_peer;
            if (!ConsumeValue(argc, argv, &i, &raw_peer)) {
                PrintUsage(argv[0]);
                return 1;
            }

            raftkv::PeerEndpoint endpoint;
            if (!ParsePeer(raw_peer, &endpoint)) {
                std::cerr << "Invalid peer: " << raw_peer << "\n";
                PrintUsage(argv[0]);
                return 1;
            }

            peer_endpoints.push_back(std::move(endpoint));
        } else if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            return 0;
        } else {
            std::cerr << "Unknown argument: " << arg << "\n";
            PrintUsage(argv[0]);
            return 1;
        }
    }

    if (node_id.empty() || listen_address.empty()) {
        PrintUsage(argv[0]);
        return 1;
    }

    std::vector<raftkv::NodeId> peer_ids;
    peer_ids.reserve(peer_endpoints.size());
    std::unordered_map<raftkv::NodeId, std::string> peer_addresses;
    for (const auto& peer : peer_endpoints) {
        peer_ids.push_back(peer.id);
        peer_addresses.emplace(peer.id, peer.address);
    }
    peer_addresses.emplace(node_id, listen_address);

    std::unique_ptr<raftkv::KeyValueStateMachine> state_machine;
    std::unique_ptr<raftkv::RaftNode> raft_node;
    std::unique_ptr<raftkv::RocksDbStore> persistent_store;
    raftkv::GrpcPeerClient transport(peer_endpoints);
#ifdef RAFTKV_BUILD_ROCKSDB_STORAGE
    if (!data_dir.empty()) {
        std::filesystem::create_directories(data_dir);
        persistent_store = std::make_unique<raftkv::RocksDbStore>(data_dir);
        raftkv::ReplayCommittedEntries(*persistent_store, *persistent_store, *persistent_store);
        raft_node = std::make_unique<raftkv::RaftNode>(
            node_id, std::move(peer_ids), transport, *persistent_store, persistent_store.get());
    } else {
        raft_node = std::make_unique<raftkv::RaftNode>(node_id, std::move(peer_ids), transport);
        state_machine = std::make_unique<raftkv::StateMachine>();
    }
#else
    if (!data_dir.empty()) {
        std::cerr << "--data-dir requires BUILD_ROCKSDB_STORAGE=ON\n";
        return 1;
    }
    raft_node = std::make_unique<raftkv::RaftNode>(node_id, std::move(peer_ids), transport);
    state_machine = std::make_unique<raftkv::StateMachine>();
#endif
    raftkv::KeyValueStateMachine& kv_state_machine =
        persistent_store == nullptr ? *state_machine : *persistent_store;
    raftkv::KVServiceImpl kv_service(
        *raft_node, kv_state_machine,
        [peer_addresses = std::move(peer_addresses)](const raftkv::NodeId& leader_id) {
            const auto it = peer_addresses.find(leader_id);
            return it == peer_addresses.end() ? std::string{} : it->second;
        });

    raftkv::AsyncRaftServer server(listen_address, *raft_node,
                                   std::thread::hardware_concurrency(), {&kv_service});
    if (!server.Start()) {
        std::cerr << "Failed to start server on " << listen_address << "\n";
        return 1;
    }

    raftkv::RaftNodeRunner runner(*raft_node);
    runner.Start();
    std::unique_ptr<raftkv::CommittedLogApplier> committed_log_applier;
    if (persistent_store != nullptr) {
        committed_log_applier = std::make_unique<raftkv::CommittedLogApplier>(
            *persistent_store, *persistent_store, *persistent_store);
        committed_log_applier->Start();
    }

    std::cout << "Raft node " << node_id << " listening on " << listen_address << "\n";
    server.Wait();
    if (committed_log_applier != nullptr) {
        committed_log_applier->Stop();
    }
    runner.Stop();
    return 0;
#endif
}
