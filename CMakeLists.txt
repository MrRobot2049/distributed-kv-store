cmake_minimum_required(VERSION 3.25)

project(DistributedKVStore LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(BUILD_TESTING "Build tests" ON)
option(BUILD_GRPC_TRANSPORT "Build generated protobuf/gRPC transport targets" ON)
option(BUILD_ROCKSDB_STORAGE "Build RocksDB-backed storage targets" ON)

if(BUILD_GRPC_TRANSPORT OR BUILD_ROCKSDB_STORAGE)
    find_package(Protobuf CONFIG REQUIRED)
    find_package(gRPC CONFIG REQUIRED)
    add_subdirectory(proto)
endif()

if(BUILD_ROCKSDB_STORAGE)
    find_package(RocksDB CONFIG REQUIRED)
endif()

add_library(raft_core
    src/storage/kv_command.cpp
    src/server/committed_log_applier.cpp
    src/server/raft_node_runner.cpp
    src/storage/state_machine.cpp
    src/raft/election_timer.cpp
    src/raft/log_manager.cpp
    src/raft/raft_node.cpp
)

target_include_directories(raft_core
    PUBLIC
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/src
)

add_subdirectory(src)

if(BUILD_TESTING)
    include(CTest)
    add_subdirectory(tests)
endif()
