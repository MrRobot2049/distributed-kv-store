cmake_minimum_required(VERSION 3.16)

project(DistributedKVStore)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

#find_package(Protobuf REQUIRED)
find_package(gRPC REQUIRED)

find_library(ROCKSDB_LIBRARY rocksdb REQUIRED)

add_subdirectory(src)
# add_subdirectory(proto)   # We'll enable this in Milestone 2
# add_subdirectory(tests)   # We'll enable this when we add tests