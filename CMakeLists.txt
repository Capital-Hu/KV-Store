cmake_minimum_required(VERSION 3.16)
project(replicated_kv_store LANGUAGES CXX)

add_executable(kvstore src/main.cpp)
target_compile_features(kvstore PRIVATE cxx_std_17)
set_target_properties(kvstore PROPERTIES CXX_EXTENSIONS OFF)
