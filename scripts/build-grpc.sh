#!/bin/sh
# Build pinned dependencies inside ignored build/; no system installation.
set -eu
project_root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
dependency_root="$project_root/build/deps"
mkdir -p "$dependency_root"
grpc_source="$dependency_root/grpc-src"
grpc_revision=3252a89f10d8e92997862167ca7d095ecda85973
if [ ! -d "$grpc_source/.git" ]; then
    git clone --depth 1 --branch v1.84.0 https://github.com/grpc/grpc.git "$grpc_source"
fi
if [ "$(git -C "$grpc_source" rev-parse HEAD)" != "$grpc_revision" ]; then
    echo "Unexpected gRPC revision in $grpc_source" >&2
    exit 1
fi
git -C "$grpc_source" submodule update --init --depth 1 --jobs 4 \
    third_party/abseil-cpp third_party/boringssl-with-bazel third_party/cares/cares \
    third_party/protobuf third_party/re2 third_party/zlib third_party/grpc-proto
cmake -S "$grpc_source" -B "$dependency_root/grpc-build" \
    -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_INSTALL_PREFIX="$dependency_root/install" \
    -DgRPC_INSTALL=ON -DgRPC_BUILD_TESTS=OFF -DgRPC_DOWNLOAD_ARCHIVES=OFF -DBUILD_TESTING=OFF \
    -DgRPC_BUILD_GRPC_CSHARP_PLUGIN=OFF -DgRPC_BUILD_GRPC_NODE_PLUGIN=OFF \
    -DgRPC_BUILD_GRPC_OBJECTIVE_C_PLUGIN=OFF -DgRPC_BUILD_GRPC_PHP_PLUGIN=OFF \
    -DgRPC_BUILD_GRPC_PYTHON_PLUGIN=OFF -DgRPC_BUILD_GRPC_RUBY_PLUGIN=OFF \
    -DABSL_ENABLE_INSTALL=ON -Dprotobuf_BUILD_TESTS=OFF -DRE2_BUILD_TESTING=OFF
cmake --build "$dependency_root/grpc-build" --parallel "${KVSTORE_BUILD_JOBS:-4}"
cmake --install "$dependency_root/grpc-build"
