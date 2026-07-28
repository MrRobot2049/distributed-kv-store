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

target_link_libraries(server
    PRIVATE
        gRPC::grpc++
        protobuf::libprotobuf
        ${ROCKSDB_LIBRARY}
)