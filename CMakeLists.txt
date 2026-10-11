cmake_minimum_required(VERSION 3.16)
project(replicated_kv_store LANGUAGES CXX)
set(CMAKE_CXX_EXTENSIONS OFF)

add_library(kvstore_engine STATIC src/memtable.cpp src/wal_record.cpp)
target_include_directories(kvstore_engine PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include")
target_compile_features(kvstore_engine PUBLIC cxx_std_17)

add_executable(kvstore src/main.cpp)
target_link_libraries(kvstore PRIVATE kvstore_engine)

include(CTest)
if(BUILD_TESTING)
    add_executable(memtable_test tests/memtable_test.cpp)
    target_link_libraries(memtable_test PRIVATE kvstore_engine)
    add_test(NAME memtable COMMAND memtable_test)

    add_executable(wal_record_test tests/wal_record_test.cpp)
    target_link_libraries(wal_record_test PRIVATE kvstore_engine)
    add_test(NAME wal_record COMMAND wal_record_test)
endif()
