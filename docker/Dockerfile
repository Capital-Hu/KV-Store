# Dependencies, project build and runtime are separate stages. The gRPC layer is
# cached until scripts/build-grpc.sh changes, so source edits rebuild in seconds.
FROM ubuntu:24.04 AS deps
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential cmake git ca-certificates pkg-config \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY scripts/build-grpc.sh scripts/build-grpc.sh
ARG KVSTORE_BUILD_JOBS=4
RUN KVSTORE_BUILD_JOBS=${KVSTORE_BUILD_JOBS} sh scripts/build-grpc.sh \
    && rm -rf build/deps/grpc-src build/deps/grpc-build

FROM deps AS build
COPY CMakeLists.txt ./
COPY apps apps
COPY benchmarks benchmarks
COPY examples examples
COPY include include
COPY proto proto
COPY src src
COPY tests tests
ARG KVSTORE_BUILD_JOBS=4
RUN cmake -S . -B build/docker -DCMAKE_BUILD_TYPE=Release -DKVSTORE_BUILD_GRPC=ON -DBUILD_TESTING=OFF \
        -DCMAKE_PREFIX_PATH=/src/build/deps/install \
    && cmake --build build/docker --parallel ${KVSTORE_BUILD_JOBS} --target kvnode kvadmin kvclient

FROM ubuntu:24.04 AS runtime
RUN useradd --system --uid 10001 --no-create-home kvstore \
    && install -d -o kvstore -g kvstore /data
COPY --from=build /src/build/docker/kvnode /src/build/docker/kvadmin /src/build/docker/kvclient /usr/local/bin/
COPY docker/entrypoint.sh /usr/local/bin/kvnode-entrypoint
USER kvstore
VOLUME /data
ENTRYPOINT ["kvnode-entrypoint"]
