SUMMARY = "Pinned host-side gRPC v1.82.0 grpc_cpp_plugin for egrpc"
DESCRIPTION = "Builds ONLY grpc_cpp_plugin from SHA-256-verified gRPC v1.82.0 \
sources against a static protobuf 35.0 libprotoc and static Abseil \
20250512.1, mirroring egrpc's tools/pin-codegen.sh. The gRPC runtime is never \
built. Host-only (-native): codegen tools are never cross-compiled to target. \
Installed as egrpc-grpc_cpp_plugin to avoid colliding with meta-oe's grpc-native."
HOMEPAGE = "https://github.com/grpc/grpc"
LICENSE = "Apache-2.0 & BSD-3-Clause"
LIC_FILES_CHKSUM = " \
    file://${WORKDIR}/grpc-1.82.0/LICENSE;md5=fe8c552b7748e45c00ac55ac0257f6c9 \
    file://${WORKDIR}/protobuf-35.0/LICENSE;md5=37b5762e07f0af8c74ce80a8bda4266b \
    file://${WORKDIR}/abseil-cpp-20250512.1/LICENSE;md5=df52c6edb7adc22e533b2bacc3bd3915 \
"

ABSEIL_VERSION = "20250512.1"
PROTOBUF_VERSION = "35.0"

SRC_URI = " \
    https://github.com/grpc/grpc/archive/refs/tags/v${PV}.tar.gz;downloadfilename=grpc-${PV}.tar.gz;name=grpc;unpack=1 \
    https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-${PROTOBUF_VERSION}.tar.gz;name=protobuf;unpack=1 \
    https://github.com/abseil/abseil-cpp/releases/download/${ABSEIL_VERSION}/abseil-cpp-${ABSEIL_VERSION}.tar.gz;name=abseil;unpack=1 \
"
SRC_URI[grpc.sha256sum] = "d6851f59b9c4edb3218d2659a3abf1138e5bb4d2871c9e89c9330ed71756ed0b"
SRC_URI[protobuf.sha256sum] = "8f907baca4b34a3b4854103ba5811e418fb6e2ff11fe0d8df9e8280b11d79926"
SRC_URI[abseil.sha256sum] = "9b7a064305e9fd94d124ffa6cc358592eb42b5da588fb4e07d09254aa40086db"

# The archives are pinned by SHA-256 (identical to tools/pin-codegen.sh), so
# GitHub archive instability is caught at fetch time rather than silently.
INSANE_SKIP += "src-uri-bad"

S = "${WORKDIR}/grpc-${PV}"

inherit native

DEPENDS = "cmake-native ninja-native"

EGRPC_CG_PREFIX = "${WORKDIR}/cg-prefix"

do_configure[noexec] = "1"

do_compile() {
    prefix_dir="${EGRPC_CG_PREFIX}"
    rm -rf "$prefix_dir" ${WORKDIR}/build-abseil ${WORKDIR}/build-protobuf ${WORKDIR}/build-plugin
    common="-G Ninja -DCMAKE_BUILD_TYPE=Release -DCMAKE_CXX_STANDARD=17 \
            -DCMAKE_POSITION_INDEPENDENT_CODE=ON -DCMAKE_INSTALL_LIBDIR=lib \
            -DCMAKE_PREFIX_PATH=$prefix_dir -DCMAKE_INSTALL_PREFIX=$prefix_dir"

    cmake -S ${WORKDIR}/abseil-cpp-${ABSEIL_VERSION} -B ${WORKDIR}/build-abseil $common \
        -DABSL_PROPAGATE_CXX_STD=ON -DABSL_ENABLE_INSTALL=ON -DBUILD_TESTING=OFF
    cmake --build ${WORKDIR}/build-abseil ${PARALLEL_MAKE}
    cmake --install ${WORKDIR}/build-abseil

    cmake -S ${WORKDIR}/protobuf-${PROTOBUF_VERSION} -B ${WORKDIR}/build-protobuf $common \
        -Dprotobuf_BUILD_TESTS=OFF -Dprotobuf_BUILD_SHARED_LIBS=OFF \
        -Dprotobuf_LOCAL_DEPENDENCIES_ONLY=ON -Dprotobuf_ABSL_PROVIDER=package
    cmake --build ${WORKDIR}/build-protobuf ${PARALLEL_MAKE}
    cmake --install ${WORKDIR}/build-protobuf

    # Minimal project: ONLY grpc_cpp_plugin, never the gRPC runtime.
    mkdir -p ${WORKDIR}/plugin-src
    cat > ${WORKDIR}/plugin-src/CMakeLists.txt <<'CMAKE_EOF'
cmake_minimum_required(VERSION 3.16)
project(grpc_cpp_plugin CXX)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
find_package(protobuf CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)
add_executable(grpc_cpp_plugin
  ${GRPC_SRC}/src/compiler/cpp_plugin.cc
  ${GRPC_SRC}/src/compiler/cpp_generator.cc
  ${GRPC_SRC}/src/compiler/proto_parser_helper.cc)
target_include_directories(grpc_cpp_plugin PRIVATE ${GRPC_SRC} ${GRPC_SRC}/include)
target_link_libraries(grpc_cpp_plugin PRIVATE protobuf::libprotoc protobuf::libprotobuf)
CMAKE_EOF
    cmake -S ${WORKDIR}/plugin-src -B ${WORKDIR}/build-plugin $common -DGRPC_SRC=${S}
    cmake --build ${WORKDIR}/build-plugin ${PARALLEL_MAKE}
}

do_install() {
    install -d ${D}${bindir}
    install -m 0755 ${WORKDIR}/build-plugin/grpc_cpp_plugin ${D}${bindir}/egrpc-grpc_cpp_plugin

    # Smoke test: the plugin must generate a service stub via protoc.
    smoke=${WORKDIR}/smoke
    rm -rf $smoke && mkdir -p $smoke/out
    printf 'syntax = "proto3";\npackage s;\nmessage M {}\nservice S { rpc U(M) returns (M); }\n' > $smoke/s.proto
    ${EGRPC_CG_PREFIX}/bin/protoc --proto_path=$smoke \
        --plugin=protoc-gen-grpc=${D}${bindir}/egrpc-grpc_cpp_plugin \
        --grpc_out=$smoke/out $smoke/s.proto
    test -s $smoke/out/s.grpc.pb.h
    test -s $smoke/out/s.grpc.pb.cc
}
