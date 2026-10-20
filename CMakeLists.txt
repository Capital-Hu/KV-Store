cmake_minimum_required(VERSION 3.16)
project(replicated_kv_store LANGUAGES CXX)
option(KVSTORE_BUILD_GRPC "Build the gRPC node, admin tool and network tests" OFF)
set(CMAKE_CXX_EXTENSIONS OFF)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")
include(CTest)

add_library(kvstore_file_io STATIC src/common/file_io.cpp)
target_compile_features(kvstore_file_io PUBLIC cxx_std_17)

add_library(kvstore_engine STATIC src/engine/memtable.cpp src/engine/wal_record.cpp
    src/engine/store.cpp src/engine/sstable.cpp src/engine/manifest.cpp src/engine/compaction.cpp
    src/engine/background_compaction.cpp src/engine/background_flush.cpp
    src/engine/bloom_filter.cpp src/engine/block_cache.cpp)
target_include_directories(kvstore_engine PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_include_directories(kvstore_engine PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_compile_features(kvstore_engine PUBLIC cxx_std_17)
target_link_libraries(kvstore_engine PUBLIC Threads::Threads)
target_link_libraries(kvstore_engine PRIVATE kvstore_file_io)

add_library(kvstore_raft STATIC src/raft/node.cpp src/raft/replication.cpp src/raft/storage.cpp src/raft/storage_codec.cpp)
target_include_directories(kvstore_raft PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_include_directories(kvstore_raft PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_compile_features(kvstore_raft PUBLIC cxx_std_17)
target_link_libraries(kvstore_raft PRIVATE kvstore_file_io)

add_library(kvstore_replica STATIC src/replica/replicated_store.cpp src/replica/replica_checkpoint.cpp)
target_include_directories(kvstore_replica PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_include_directories(kvstore_replica PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/src")
target_compile_features(kvstore_replica PUBLIC cxx_std_17)
target_link_libraries(kvstore_replica PUBLIC kvstore_raft kvstore_engine PRIVATE kvstore_file_io)

if(KVSTORE_BUILD_GRPC)
    add_subdirectory(src/rpc)
endif()

add_executable(kvstore apps/kvstore_main.cpp)
target_link_libraries(kvstore PRIVATE kvstore_engine)

add_executable(raft_demo examples/raft_demo.cpp)
target_link_libraries(raft_demo PRIVATE kvstore_raft)

add_executable(replica_demo examples/replica_demo.cpp)
target_link_libraries(replica_demo PRIVATE kvstore_replica)

if(KVSTORE_BUILD_GRPC)
    add_executable(kvnode apps/kvnode_main.cpp)
    target_link_libraries(kvnode PRIVATE kvstore_rpc)
    add_executable(kvadmin apps/kvadmin_main.cpp)
    target_link_libraries(kvadmin PRIVATE kvstore_rpc)
    add_executable(kvclient apps/kvclient_main.cpp)
    target_link_libraries(kvclient PRIVATE kvstore_client)
endif()

if(BUILD_TESTING)
    add_subdirectory(tests)
endif()
