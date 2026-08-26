include(FetchContent)

FetchContent_Declare(
    googletest
    URL https://github.com/google/googletest/archive/refs/tags/v1.15.2.zip
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(raft_node_test
    unit/raft_node_test.cpp
)

target_link_libraries(raft_node_test
    PRIVATE
        raft_core
        GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(raft_node_test)

add_executable(log_manager_test
    unit/log_manager_test.cpp
)

target_link_libraries(log_manager_test
    PRIVATE
        raft_core
        GTest::gtest_main
)

gtest_discover_tests(log_manager_test)

add_executable(kv_command_test
    unit/kv_command_test.cpp
)

target_link_libraries(kv_command_test
    PRIVATE
        raft_core
        GTest::gtest_main
)

gtest_discover_tests(kv_command_test)

add_executable(committed_log_applier_test
    unit/committed_log_applier_test.cpp
)

target_link_libraries(committed_log_applier_test
    PRIVATE
        raft_core
        GTest::gtest_main
)

gtest_discover_tests(committed_log_applier_test)

if(TARGET raft_rpc)
    add_executable(kv_service_impl_test
        unit/kv_service_impl_test.cpp
    )

    target_link_libraries(kv_service_impl_test
        PRIVATE
            raft_rpc
            GTest::gtest_main
    )

    gtest_discover_tests(kv_service_impl_test)

    add_executable(grpc_transport_smoke_test
        integration/grpc_transport_smoke_test.cpp
    )

    target_link_libraries(grpc_transport_smoke_test
        PRIVATE
            raft_rpc
            GTest::gtest_main
    )

    gtest_discover_tests(grpc_transport_smoke_test)
endif()

if(TARGET raft_storage)
    add_executable(raft_log_store_test
        unit/raft_log_store_test.cpp
    )

    target_link_libraries(raft_log_store_test
        PRIVATE
            raft_storage
            GTest::gtest_main
    )

    gtest_discover_tests(raft_log_store_test)

    add_executable(rocksdb_state_machine_test
        unit/rocksdb_state_machine_test.cpp
    )

    target_link_libraries(rocksdb_state_machine_test
        PRIVATE
            raft_storage
            GTest::gtest_main
    )

    gtest_discover_tests(rocksdb_state_machine_test)

    add_executable(rocksdb_store_test
        unit/rocksdb_store_test.cpp
    )

    target_link_libraries(rocksdb_store_test
        PRIVATE
            raft_storage
            GTest::gtest_main
    )

    gtest_discover_tests(rocksdb_store_test)

    add_executable(snapshot_manager_test
        unit/snapshot_manager_test.cpp
    )

    target_link_libraries(snapshot_manager_test
        PRIVATE
            raft_storage
            GTest::gtest_main
    )

    gtest_discover_tests(snapshot_manager_test)

    add_executable(crash_recovery_test
        integration/crash_recovery_test.cpp
    )

    target_link_libraries(crash_recovery_test
        PRIVATE
            raft_storage
            GTest::gtest_main
    )

    gtest_discover_tests(crash_recovery_test)
endif()
