find_package(Protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)

set(proto_file "${PROJECT_SOURCE_DIR}/proto/raft_node.proto")
set(generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
file(MAKE_DIRECTORY "${generated_dir}")
add_custom_command(
    OUTPUT "${generated_dir}/raft_node.pb.cc" "${generated_dir}/raft_node.pb.h"
           "${generated_dir}/raft_node.grpc.pb.cc" "${generated_dir}/raft_node.grpc.pb.h"
    COMMAND protobuf::protoc
    ARGS --proto_path=${PROJECT_SOURCE_DIR}/proto --cpp_out=${generated_dir}
         --grpc_out=${generated_dir} --plugin=protoc-gen-grpc=$<TARGET_FILE:gRPC::grpc_cpp_plugin> ${proto_file}
    DEPENDS "${proto_file}" protobuf::protoc gRPC::grpc_cpp_plugin
    VERBATIM)
add_library(kvstore_proto STATIC "${generated_dir}/raft_node.pb.cc" "${generated_dir}/raft_node.grpc.pb.cc")
target_include_directories(kvstore_proto PUBLIC "${generated_dir}")
target_compile_features(kvstore_proto PUBLIC cxx_std_17)
target_link_libraries(kvstore_proto PUBLIC protobuf::libprotobuf gRPC::grpc++)
if(CMAKE_CXX_COMPILER_ID MATCHES "Clang")
    # Generated protobuf uses Clang's supported nullability annotations.
    target_compile_options(kvstore_proto PRIVATE -Wno-nullability-extension)
endif()

add_library(kvstore_rpc STATIC node_server.cpp protocol_codec.cpp config.cpp)
target_include_directories(kvstore_rpc PUBLIC "${PROJECT_SOURCE_DIR}/include")
target_compile_features(kvstore_rpc PUBLIC cxx_std_17)
target_link_libraries(kvstore_rpc PUBLIC kvstore_replica kvstore_proto)
