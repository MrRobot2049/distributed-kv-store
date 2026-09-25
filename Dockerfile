FROM ubuntu:24.04 AS build

ENV DEBIAN_FRONTEND=noninteractive
ENV VCPKG_ROOT=/opt/vcpkg

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    cmake \
    curl \
    git \
    ninja-build \
    pkg-config \
    tar \
    unzip \
    zip \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --depth 1 https://github.com/microsoft/vcpkg "$VCPKG_ROOT" \
    && "$VCPKG_ROOT/bootstrap-vcpkg.sh" -disableMetrics

WORKDIR /src
COPY . .

RUN cmake -S . -B build -G Ninja \
    -DCMAKE_TOOLCHAIN_FILE="$VCPKG_ROOT/scripts/buildsystems/vcpkg.cmake" \
    -DBUILD_TESTING=OFF \
    -DBUILD_GRPC_TRANSPORT=ON \
    -DBUILD_ROCKSDB_STORAGE=ON \
    && cmake --build build --target server

FROM build AS runtime

COPY --from=build /src/build/src/server /usr/local/bin/raftkv-server
COPY config /etc/raftkv

EXPOSE 5001 5002 5003

ENTRYPOINT ["/usr/local/bin/raftkv-server"]
