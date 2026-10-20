add_executable(kvbench kvbench.cpp)
target_link_libraries(kvbench PRIVATE kvstore_client)
target_compile_definitions(kvbench PRIVATE KVSTORE_BENCH_BUILD_TYPE="${CMAKE_BUILD_TYPE}")
