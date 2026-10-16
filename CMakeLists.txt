cmake_minimum_required(VERSION 3.16)
project(replicated_kv_store LANGUAGES CXX)
set(CMAKE_CXX_EXTENSIONS OFF)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(Threads REQUIRED)

add_library(kvstore_file_io STATIC src/file_io.cpp)
target_compile_features(kvstore_file_io PUBLIC cxx_std_17)

add_library(kvstore_engine STATIC src/memtable.cpp src/wal_record.cpp src/store.cpp src/sstable.cpp
    src/manifest.cpp src/compaction.cpp src/background_compaction.cpp src/background_flush.cpp
    src/bloom_filter.cpp src/block_cache.cpp)
target_include_directories(kvstore_engine PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_compile_features(kvstore_engine PUBLIC cxx_std_17)
target_link_libraries(kvstore_engine PUBLIC Threads::Threads)
target_link_libraries(kvstore_engine PRIVATE kvstore_file_io)

add_library(kvstore_raft STATIC src/raft/node.cpp src/raft/storage.cpp src/raft/storage_codec.cpp)
target_include_directories(kvstore_raft PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_compile_features(kvstore_raft PUBLIC cxx_std_17)
target_link_libraries(kvstore_raft PRIVATE kvstore_file_io)

add_executable(kvstore src/main.cpp)
target_link_libraries(kvstore PRIVATE kvstore_engine)

add_executable(raft_demo src/raft_demo.cpp)
target_link_libraries(raft_demo PRIVATE kvstore_raft)

include(CTest)
if(BUILD_TESTING)
    add_executable(memtable_test tests/memtable_test.cpp)
    target_link_libraries(memtable_test PRIVATE kvstore_engine)
    add_test(NAME memtable COMMAND memtable_test)

    add_executable(wal_record_test tests/wal_record_test.cpp)
    target_link_libraries(wal_record_test PRIVATE kvstore_engine)
    add_test(NAME wal_record COMMAND wal_record_test)

    add_executable(store_test tests/store_test.cpp)
    target_link_libraries(store_test PRIVATE kvstore_engine)
    add_test(NAME store COMMAND store_test)
    set_tests_properties(store PROPERTIES TIMEOUT 30)

    add_executable(sstable_test tests/sstable_test.cpp)
    target_link_libraries(sstable_test PRIVATE kvstore_engine)
    add_test(NAME sstable COMMAND sstable_test)

    add_executable(manifest_test tests/manifest_test.cpp)
    target_link_libraries(manifest_test PRIVATE kvstore_engine)
    add_test(NAME manifest COMMAND manifest_test)

    add_executable(store_flush_test tests/store_flush_test.cpp)
    target_link_libraries(store_flush_test PRIVATE kvstore_engine)
    add_test(NAME store_flush COMMAND store_flush_test)
    set_tests_properties(store_flush PROPERTIES TIMEOUT 60)

    add_executable(compaction_test tests/compaction_test.cpp)
    target_link_libraries(compaction_test PRIVATE kvstore_engine)
    add_test(NAME compaction COMMAND compaction_test)
    set_tests_properties(compaction PROPERTIES TIMEOUT 60)

    add_executable(background_test tests/background_test.cpp)
    target_link_libraries(background_test PRIVATE kvstore_engine)
    add_test(NAME background COMMAND background_test)
    set_tests_properties(background PROPERTIES TIMEOUT 60)

    add_executable(background_flush_test tests/background_flush_test.cpp)
    target_link_libraries(background_flush_test PRIVATE kvstore_engine)
    add_test(NAME background_flush COMMAND background_flush_test)
    set_tests_properties(background_flush PROPERTIES TIMEOUT 60)

    add_executable(bloom_filter_test tests/bloom_filter_test.cpp)
    target_link_libraries(bloom_filter_test PRIVATE kvstore_engine)
    add_test(NAME bloom_filter COMMAND bloom_filter_test)

    add_executable(sstable_bloom_test tests/sstable_bloom_test.cpp)
    target_link_libraries(sstable_bloom_test PRIVATE kvstore_engine)
    add_test(NAME sstable_bloom COMMAND sstable_bloom_test)
    set_tests_properties(sstable_bloom PROPERTIES TIMEOUT 60)

    add_executable(block_cache_test tests/block_cache_test.cpp)
    target_link_libraries(block_cache_test PRIVATE kvstore_engine)
    add_test(NAME block_cache COMMAND block_cache_test)
    set_tests_properties(block_cache PROPERTIES TIMEOUT 60)

    add_executable(sstable_index_test tests/sstable_index_test.cpp)
    target_link_libraries(sstable_index_test PRIVATE kvstore_engine)
    add_test(NAME sstable_index COMMAND sstable_index_test)
    set_tests_properties(sstable_index PROPERTIES TIMEOUT 60)

    add_executable(engine_integration_test tests/engine_integration_test.cpp)
    target_link_libraries(engine_integration_test PRIVATE kvstore_engine)
    add_test(NAME engine_integration COMMAND engine_integration_test)
    set_tests_properties(engine_integration PROPERTIES TIMEOUT 120)

    add_executable(raft_model_test tests/raft_model_test.cpp)
    target_link_libraries(raft_model_test PRIVATE kvstore_raft)
    add_test(NAME raft_model COMMAND raft_model_test)

    add_executable(raft_storage_test tests/raft_storage_test.cpp)
    target_link_libraries(raft_storage_test PRIVATE kvstore_raft)
    add_test(NAME raft_storage COMMAND raft_storage_test)
    set_tests_properties(raft_storage PROPERTIES TIMEOUT 120)

    add_executable(raft_election_test tests/raft_election_test.cpp)
    target_link_libraries(raft_election_test PRIVATE kvstore_raft)
    add_test(NAME raft_election COMMAND raft_election_test)
    set_tests_properties(raft_election PROPERTIES TIMEOUT 120)
endif()
