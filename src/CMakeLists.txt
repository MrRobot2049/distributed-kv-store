set(SERVER_SOURCES
    main.cpp
)

add_executable(server
    ${SERVER_SOURCES}
)

target_include_directories(server
    PRIVATE
        ${PROJECT_SOURCE_DIR}/include
)

if(TARGET raftkv_proto)
    add_library(raft_rpc
        rpc/async_raft_server.cpp
        rpc/kv_service_impl.cpp
        rpc/raft_service_impl.cpp
        rpc/raft_proto_conversions.cpp
        rpc/peer_client_grpc.cpp
    )

    target_include_directories(raft_rpc
        PUBLIC
            ${PROJECT_SOURCE_DIR}/include
            ${PROJECT_SOURCE_DIR}/src
    )

    target_link_libraries(raft_rpc
        PUBLIC
            raft_core
            raftkv_proto
            gRPC::grpc++
    )

    target_compile_definitions(server
        PRIVATE
            RAFTKV_BUILD_GRPC_TRANSPORT
    )

    target_link_libraries(server
        PRIVATE
            raft_rpc
    )
else()
    target_link_libraries(server
        PRIVATE
            raft_core
    )
endif()

if(TARGET RocksDB::rocksdb-shared AND TARGET raftkv_proto)
    add_library(raft_storage
        storage/raft_log_store.cpp
        storage/rocksdb_state_machine.cpp
        storage/rocksdb_store.cpp
        storage/snapshot_manager.cpp
    )

    target_include_directories(raft_storage
        PUBLIC
            ${PROJECT_SOURCE_DIR}/include
            ${PROJECT_SOURCE_DIR}/src
    )

    target_link_libraries(raft_storage
        PUBLIC
            raft_core
            raftkv_proto
            RocksDB::rocksdb-shared
    )

    target_compile_definitions(server
        PRIVATE
            RAFTKV_BUILD_ROCKSDB_STORAGE
    )

    target_link_libraries(server
        PRIVATE
            raft_storage
    )
endif()
