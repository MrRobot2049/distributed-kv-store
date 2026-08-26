add_library(raftkv_proto
    kv_store.proto
    raft.proto
)

target_link_libraries(raftkv_proto
    PUBLIC
        protobuf::libprotobuf
        gRPC::grpc++
)

target_include_directories(raftkv_proto
    PUBLIC
        ${CMAKE_CURRENT_BINARY_DIR}
)

protobuf_generate(
    TARGET raftkv_proto
    LANGUAGE cpp
    IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
)

protobuf_generate(
    TARGET raftkv_proto
    LANGUAGE grpc
    GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc
    PLUGIN "protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin>"
    IMPORT_DIRS ${CMAKE_CURRENT_SOURCE_DIR}
)
