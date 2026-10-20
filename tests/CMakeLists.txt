add_executable(memtable_test engine/memtable_test.cpp)
target_link_libraries(memtable_test PRIVATE kvstore_engine)
add_test(NAME memtable COMMAND memtable_test)

add_executable(wal_record_test engine/wal_record_test.cpp)
target_link_libraries(wal_record_test PRIVATE kvstore_engine)
add_test(NAME wal_record COMMAND wal_record_test)

add_executable(store_test engine/store_test.cpp)
target_link_libraries(store_test PRIVATE kvstore_engine)
add_test(NAME store COMMAND store_test)
set_tests_properties(store PROPERTIES TIMEOUT 30)

add_executable(sstable_test engine/sstable_test.cpp)
target_link_libraries(sstable_test PRIVATE kvstore_engine)
add_test(NAME sstable COMMAND sstable_test)

add_executable(manifest_test engine/manifest_test.cpp)
target_include_directories(manifest_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(manifest_test PRIVATE kvstore_engine)
add_test(NAME manifest COMMAND manifest_test)

add_executable(store_flush_test engine/store_flush_test.cpp)
target_link_libraries(store_flush_test PRIVATE kvstore_engine)
add_test(NAME store_flush COMMAND store_flush_test)
set_tests_properties(store_flush PROPERTIES TIMEOUT 60)

add_executable(compaction_test engine/compaction_test.cpp)
target_link_libraries(compaction_test PRIVATE kvstore_engine)
add_test(NAME compaction COMMAND compaction_test)
set_tests_properties(compaction PROPERTIES TIMEOUT 60)

add_executable(background_test engine/background_test.cpp)
target_link_libraries(background_test PRIVATE kvstore_engine)
add_test(NAME background COMMAND background_test)
set_tests_properties(background PROPERTIES TIMEOUT 60)

add_executable(background_flush_test engine/background_flush_test.cpp)
target_link_libraries(background_flush_test PRIVATE kvstore_engine)
add_test(NAME background_flush COMMAND background_flush_test)
set_tests_properties(background_flush PROPERTIES TIMEOUT 60)

add_executable(bloom_filter_test engine/bloom_filter_test.cpp)
target_link_libraries(bloom_filter_test PRIVATE kvstore_engine)
add_test(NAME bloom_filter COMMAND bloom_filter_test)

add_executable(sstable_bloom_test engine/sstable_bloom_test.cpp)
target_link_libraries(sstable_bloom_test PRIVATE kvstore_engine)
add_test(NAME sstable_bloom COMMAND sstable_bloom_test)
set_tests_properties(sstable_bloom PROPERTIES TIMEOUT 60)

add_executable(block_cache_test engine/block_cache_test.cpp)
target_link_libraries(block_cache_test PRIVATE kvstore_engine)
add_test(NAME block_cache COMMAND block_cache_test)
set_tests_properties(block_cache PROPERTIES TIMEOUT 60)

add_executable(sstable_index_test engine/sstable_index_test.cpp)
target_link_libraries(sstable_index_test PRIVATE kvstore_engine)
add_test(NAME sstable_index COMMAND sstable_index_test)
set_tests_properties(sstable_index PROPERTIES TIMEOUT 60)

add_executable(engine_integration_test engine/engine_integration_test.cpp)
target_link_libraries(engine_integration_test PRIVATE kvstore_engine)
add_test(NAME engine_integration COMMAND engine_integration_test)
set_tests_properties(engine_integration PROPERTIES TIMEOUT 120)

add_executable(raft_model_test raft/raft_model_test.cpp)
target_link_libraries(raft_model_test PRIVATE kvstore_raft)
add_test(NAME raft_model COMMAND raft_model_test)

add_executable(raft_storage_test raft/raft_storage_test.cpp)
target_include_directories(raft_storage_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(raft_storage_test PRIVATE kvstore_raft)
add_test(NAME raft_storage COMMAND raft_storage_test)
set_tests_properties(raft_storage PROPERTIES TIMEOUT 120)

add_executable(raft_election_test raft/raft_election_test.cpp)
target_link_libraries(raft_election_test PRIVATE kvstore_raft)
add_test(NAME raft_election COMMAND raft_election_test)
set_tests_properties(raft_election PROPERTIES TIMEOUT 120)

add_executable(raft_replication_test raft/raft_replication_test.cpp)
target_link_libraries(raft_replication_test PRIVATE kvstore_raft)
add_test(NAME raft_replication COMMAND raft_replication_test)
set_tests_properties(raft_replication PROPERTIES TIMEOUT 120)

add_executable(replicated_store_test replica/replicated_store_test.cpp)
target_include_directories(replicated_store_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
target_link_libraries(replicated_store_test PRIVATE kvstore_replica)
add_test(NAME replicated_store COMMAND replicated_store_test)
set_tests_properties(replicated_store PROPERTIES TIMEOUT 120)

if(KVSTORE_BUILD_GRPC)
    add_executable(grpc_codec_test rpc/grpc_codec_test.cpp)
    target_include_directories(grpc_codec_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
    target_link_libraries(grpc_codec_test PRIVATE kvstore_rpc)
    add_test(NAME grpc_codec COMMAND grpc_codec_test)

    add_executable(grpc_process_test rpc/grpc_process_test.cpp)
    target_include_directories(grpc_process_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
    target_link_libraries(grpc_process_test PRIVATE kvstore_rpc)
    add_dependencies(grpc_process_test kvnode kvadmin)
    add_test(NAME grpc_process COMMAND grpc_process_test $<TARGET_FILE:kvnode> $<TARGET_FILE:kvadmin>)
    set_tests_properties(grpc_process PROPERTIES TIMEOUT 120)

    add_executable(client_process_test rpc/client_process_test.cpp)
    target_include_directories(client_process_test PRIVATE "${PROJECT_SOURCE_DIR}/src")
    target_link_libraries(client_process_test PRIVATE kvstore_client)
    add_dependencies(client_process_test kvnode kvadmin kvclient)
    add_test(NAME client_process COMMAND client_process_test $<TARGET_FILE:kvnode> $<TARGET_FILE:kvadmin> $<TARGET_FILE:kvclient>)
    set_tests_properties(client_process PROPERTIES TIMEOUT 120)
endif()

# Keep the working directory used before the test sources were grouped by module.
get_property(project_tests DIRECTORY PROPERTY TESTS)
set_tests_properties(${project_tests} PROPERTIES WORKING_DIRECTORY "${PROJECT_BINARY_DIR}")
if(KVSTORE_BUILD_GRPC)
    set_tests_properties(grpc_codec grpc_process PROPERTIES
        WORKING_DIRECTORY "${PROJECT_BINARY_DIR}/src/rpc")
endif()
