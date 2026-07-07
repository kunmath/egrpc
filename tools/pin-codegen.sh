#!/usr/bin/env bash
#
# pin-codegen.sh — install egrpc's pinned host codegen tools (x86_64 Linux).
#
# Produces, under tools/:
#   bin/protoc              official prebuilt protoc (SHA-256 verified)
#   bin/grpc_cpp_plugin     grpc_cpp_plugin built from SHA-256-verified source
#   include/                protoc's bundled well-known-type .proto files
#
# WHY the plugin is built from source: upstream gRPC publishes NO prebuilt
# grpc_cpp_plugin binaries (no release assets on grpc/grpc, nothing on Maven
# Central), so it is compiled here from a SHA-256-verified source tarball
# against a statically-built libprotoc. protoc itself has an official prebuilt.
#
# All downloads and build trees live in tools/.pin-codegen-work/ and are never
# committed. tools/bin, tools/include and the work dir are ignored via the
# repository-root .gitignore.
#
# Pinned versions and checksums (see docs/codegen-pinning.md):
#   gRPC          v1.82.0       source tarball
#     https://github.com/grpc/grpc/archive/refs/tags/v1.82.0.tar.gz
#     sha256 d6851f59b9c4edb3218d2659a3abf1138e5bb4d2871c9e89c9330ed71756ed0b
#     (extracts to grpc-1.82.0/)
#   protoc        35.0          official prebuilt (linux-x86_64)
#     https://github.com/protocolbuffers/protobuf/releases/download/v35.0/protoc-35.0-linux-x86_64.zip
#     sha256 a45cda0989c17dd950db55f6fbe1e5814c50fda08e87aa422980ac1f89dddbbc
#   protobuf      35.0          source (libprotoc, to build the plugin)
#     https://github.com/protocolbuffers/protobuf/releases/download/v35.0/protobuf-35.0.tar.gz
#     sha256 8f907baca4b34a3b4854103ba5811e418fb6e2ff11fe0d8df9e8280b11d79926
#     (extracts to protobuf-35.0/; does NOT bundle Abseil)
#   abseil-cpp    20250512.1    source (the version protobuf 35.0 pins)
#     https://github.com/abseil/abseil-cpp/releases/download/20250512.1/abseil-cpp-20250512.1.tar.gz
#     sha256 9b7a064305e9fd94d124ffa6cc358592eb42b5da588fb4e07d09254aa40086db
#
set -euo pipefail

# ---------------------------------------------------------------------------
# Pinned versions & checksums
# ---------------------------------------------------------------------------
GRPC_VERSION="1.82.0"
PROTOC_VERSION="35.0"
PROTOBUF_VERSION="35.0"
ABSEIL_VERSION="20250512.1"

GRPC_URL="https://github.com/grpc/grpc/archive/refs/tags/v${GRPC_VERSION}.tar.gz"
GRPC_SHA256="d6851f59b9c4edb3218d2659a3abf1138e5bb4d2871c9e89c9330ed71756ed0b"

PROTOC_ZIP_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-x86_64.zip"
PROTOC_ZIP_SHA256="a45cda0989c17dd950db55f6fbe1e5814c50fda08e87aa422980ac1f89dddbbc"

PROTOBUF_SRC_URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOBUF_VERSION}/protobuf-${PROTOBUF_VERSION}.tar.gz"
PROTOBUF_SRC_SHA256="8f907baca4b34a3b4854103ba5811e418fb6e2ff11fe0d8df9e8280b11d79926"

ABSEIL_URL="https://github.com/abseil/abseil-cpp/releases/download/${ABSEIL_VERSION}/abseil-cpp-${ABSEIL_VERSION}.tar.gz"
ABSEIL_SHA256="9b7a064305e9fd94d124ffa6cc358592eb42b5da588fb4e07d09254aa40086db"

# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------
TOOLS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_DIR="${TOOLS_DIR}/bin"
INCLUDE_DIR="${TOOLS_DIR}/include"
WORK_DIR="${TOOLS_DIR}/.pin-codegen-work"
DL_DIR="${WORK_DIR}/downloads"
PREFIX="${WORK_DIR}/prefix"
STAMP_FILE="${BIN_DIR}/.pinned-versions"

JOBS="${JOBS:-$(nproc)}"
FORCE=0

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
log()  { printf '\033[1;34m[pin-codegen]\033[0m %s\n' "$*"; }
warn() { printf '\033[1;33m[pin-codegen]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[pin-codegen] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<EOF
Usage: tools/pin-codegen.sh [--force]

Installs the pinned host codegen tools into tools/bin and tools/include.

Options:
  --force    Re-download-verify and rebuild everything from scratch.
  -h,--help  Show this help.

Environment:
  JOBS       Parallel build jobs (default: \$(nproc) = $(nproc)).
EOF
}

for arg in "$@"; do
  case "$arg" in
    --force) FORCE=1 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown argument: $arg (see --help)" ;;
  esac
done

# ---------------------------------------------------------------------------
# Preflight: host & tool requirements
# ---------------------------------------------------------------------------
preflight() {
  local hostspec
  hostspec="$(uname -sm)"
  if [ "$hostspec" != "Linux x86_64" ]; then
    die "this installer supports only x86_64 Linux; detected '${hostspec}'.
       (protoc prebuilt and the plugin build tree are host-specific.)"
  fi

  for cmd in curl unzip cmake tar sha256sum; do
    command -v "$cmd" >/dev/null 2>&1 || die "required tool not found on PATH: ${cmd}"
  done

  # C++17 compiler
  local cxx="${CXX:-c++}"
  command -v "$cxx" >/dev/null 2>&1 || command -v g++ >/dev/null 2>&1 \
    || die "no C++ compiler found (looked for '\$CXX'/'c++' and 'g++')."

  # cmake >= 3.16
  local cmv cmajor cminor
  cmv="$(cmake --version | head -1 | grep -oE '[0-9]+\.[0-9]+(\.[0-9]+)?' | head -1)"
  cmajor="${cmv%%.*}"
  cminor="$(printf '%s' "$cmv" | cut -d. -f2)"
  if [ "$cmajor" -lt 3 ] || { [ "$cmajor" -eq 3 ] && [ "$cminor" -lt 16 ]; }; then
    die "cmake >= 3.16 required; found ${cmv}."
  fi
  log "host OK: ${hostspec}, cmake ${cmv}, JOBS=${JOBS}"
}

# download_verify <url> <sha256> <dest>
# Re-uses an already-downloaded file if its checksum already matches.
download_verify() {
  local url="$1" sha="$2" dest="$3" actual
  mkdir -p "$(dirname "$dest")"
  if [ -f "$dest" ]; then
    actual="$(sha256sum "$dest" | awk '{print $1}')"
    if [ "$actual" = "$sha" ]; then
      log "reuse verified $(basename "$dest")"
      return 0
    fi
    warn "cached $(basename "$dest") checksum mismatch; re-downloading"
    rm -f "$dest"
  fi
  log "download $(basename "$dest")"
  curl -fSL --retry 3 -o "$dest" "$url" || die "download failed: $url"
  actual="$(sha256sum "$dest" | awk '{print $1}')"
  if [ "$actual" != "$sha" ]; then
    rm -f "$dest"
    die "checksum mismatch for $(basename "$dest")
       url:      ${url}
       expected: ${sha}
       actual:   ${actual}"
  fi
  log "verified $(basename "$dest")"
}

# ---------------------------------------------------------------------------
# Idempotency
# ---------------------------------------------------------------------------
already_pinned() {
  [ "$FORCE" -eq 0 ] || return 1
  [ -x "${BIN_DIR}/protoc" ] || return 1
  [ -x "${BIN_DIR}/grpc_cpp_plugin" ] || return 1
  [ -f "$STAMP_FILE" ] || return 1
  grep -q "^grpc=${GRPC_VERSION}$" "$STAMP_FILE" || return 1
  grep -q "^protoc=${PROTOC_VERSION}$" "$STAMP_FILE" || return 1
  grep -q "^protobuf=${PROTOBUF_VERSION}$" "$STAMP_FILE" || return 1
  grep -q "^abseil=${ABSEIL_VERSION}$" "$STAMP_FILE" || return 1
  local pv
  pv="$("${BIN_DIR}/protoc" --version 2>/dev/null || true)"
  [ "$pv" = "libprotoc ${PROTOC_VERSION}" ] || return 1
  return 0
}

# ---------------------------------------------------------------------------
# Build steps
# ---------------------------------------------------------------------------
install_protoc() {
  log "step 1/4: install prebuilt protoc ${PROTOC_VERSION}"
  local zip="${DL_DIR}/protoc-${PROTOC_VERSION}-linux-x86_64.zip"
  download_verify "$PROTOC_ZIP_URL" "$PROTOC_ZIP_SHA256" "$zip"
  local ex="${WORK_DIR}/protoc-unzip"
  rm -rf "$ex"; mkdir -p "$ex"
  unzip -q -o "$zip" -d "$ex"
  mkdir -p "$BIN_DIR" "$INCLUDE_DIR"
  install -m 0755 "${ex}/bin/protoc" "${BIN_DIR}/protoc"
  # Bundled well-known-type .proto files (google/protobuf/*.proto etc.)
  rm -rf "${INCLUDE_DIR:?}/google"
  cp -a "${ex}/include/." "${INCLUDE_DIR}/"
  log "protoc installed: $("${BIN_DIR}/protoc" --version)"
}

build_abseil() {
  log "step 2/4: build Abseil ${ABSEIL_VERSION} (static, PIC)"
  local tgz="${DL_DIR}/abseil-cpp-${ABSEIL_VERSION}.tar.gz"
  download_verify "$ABSEIL_URL" "$ABSEIL_SHA256" "$tgz"
  local src="${WORK_DIR}/abseil-cpp-${ABSEIL_VERSION}"
  rm -rf "$src"
  tar -xzf "$tgz" -C "$WORK_DIR"
  local bld="${WORK_DIR}/build-abseil"
  rm -rf "$bld"
  cmake -S "$src" -B "$bld" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_STANDARD=17 \
    -DABSL_PROPAGATE_CXX_STD=ON \
    -DABSL_ENABLE_INSTALL=ON \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -DBUILD_TESTING=OFF \
    -DCMAKE_INSTALL_PREFIX="$PREFIX"
  cmake --build "$bld" -j "$JOBS"
  cmake --install "$bld"
  log "Abseil installed into prefix"
}

build_protobuf() {
  log "step 3/4: build protobuf ${PROTOBUF_VERSION} (static libprotoc/libprotobuf)"
  local tgz="${DL_DIR}/protobuf-${PROTOBUF_VERSION}.tar.gz"
  download_verify "$PROTOBUF_SRC_URL" "$PROTOBUF_SRC_SHA256" "$tgz"
  local src="${WORK_DIR}/protobuf-${PROTOBUF_VERSION}"
  rm -rf "$src"
  tar -xzf "$tgz" -C "$WORK_DIR"
  local bld="${WORK_DIR}/build-protobuf"
  rm -rf "$bld"
  # protobuf_LOCAL_DEPENDENCIES_ONLY=ON disables the FetchContent fallback in
  # protobuf 35.0's cmake/abseil-cpp.cmake, so Abseil can only come from
  # find_package against $PREFIX (our pinned build) — that is the whole point
  # of pinning here. protobuf_ABSL_PROVIDER=package is the older spelling of
  # the same intent, kept for belt-and-suspenders.
  cmake -S "$src" -B "$bld" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_CXX_STANDARD=17 \
    -DCMAKE_POSITION_INDEPENDENT_CODE=ON \
    -Dprotobuf_BUILD_TESTS=OFF \
    -Dprotobuf_BUILD_SHARED_LIBS=OFF \
    -Dprotobuf_LOCAL_DEPENDENCIES_ONLY=ON \
    -Dprotobuf_ABSL_PROVIDER=package \
    -DCMAKE_PREFIX_PATH="$PREFIX" \
    -DCMAKE_INSTALL_PREFIX="$PREFIX"
  cmake --build "$bld" -j "$JOBS"
  cmake --install "$bld"
  log "protobuf installed into prefix"
}

build_plugin() {
  log "step 4/4: build grpc_cpp_plugin from gRPC ${GRPC_VERSION} sources"
  local tgz="${DL_DIR}/grpc-${GRPC_VERSION}.tar.gz"
  download_verify "$GRPC_URL" "$GRPC_SHA256" "$tgz"
  local gsrc="${WORK_DIR}/grpc-${GRPC_VERSION}"
  rm -rf "$gsrc"
  # We only need src/compiler and include/ (the plugin), not the gRPC runtime.
  tar -xzf "$tgz" -C "$WORK_DIR" \
    "grpc-${GRPC_VERSION}/src/compiler" \
    "grpc-${GRPC_VERSION}/include"

  local pdir="${WORK_DIR}/plugin-build-src"
  rm -rf "$pdir"; mkdir -p "$pdir"
  # Minimal CMake project: build ONLY grpc_cpp_plugin against pinned libprotoc.
  cat > "${pdir}/CMakeLists.txt" <<'CMAKE_EOF'
cmake_minimum_required(VERSION 3.16)
project(grpc_cpp_plugin CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

if(NOT DEFINED GRPC_SRC)
  message(FATAL_ERROR "GRPC_SRC must be set to the extracted gRPC source dir")
endif()

find_package(protobuf CONFIG REQUIRED)
find_package(absl CONFIG REQUIRED)

add_executable(grpc_cpp_plugin
  ${GRPC_SRC}/src/compiler/cpp_plugin.cc
  ${GRPC_SRC}/src/compiler/cpp_generator.cc
  ${GRPC_SRC}/src/compiler/proto_parser_helper.cc)

# gRPC's compiler sources #include "src/compiler/..." (rooted at GRPC_SRC) and
# <grpcpp/impl/codegen/config_protobuf.h> (under GRPC_SRC/include).
target_include_directories(grpc_cpp_plugin PRIVATE
  ${GRPC_SRC}
  ${GRPC_SRC}/include)

target_link_libraries(grpc_cpp_plugin PRIVATE
  protobuf::libprotoc
  protobuf::libprotobuf)
CMAKE_EOF

  local bld="${WORK_DIR}/build-plugin"
  rm -rf "$bld"
  cmake -S "$pdir" -B "$bld" \
    -DCMAKE_BUILD_TYPE=Release \
    -DGRPC_SRC="$gsrc" \
    -DCMAKE_PREFIX_PATH="$PREFIX"
  cmake --build "$bld" -j "$JOBS"

  mkdir -p "$BIN_DIR"
  install -m 0755 "${bld}/grpc_cpp_plugin" "${BIN_DIR}/grpc_cpp_plugin"
  log "grpc_cpp_plugin built and installed"
}

write_stamp() {
  mkdir -p "$BIN_DIR"
  cat > "$STAMP_FILE" <<EOF
# egrpc pinned codegen tools — generated by tools/pin-codegen.sh
grpc=${GRPC_VERSION}
protoc=${PROTOC_VERSION}
protobuf=${PROTOBUF_VERSION}
abseil=${ABSEIL_VERSION}
EOF
}

# ---------------------------------------------------------------------------
# Smoke test
# ---------------------------------------------------------------------------
smoke_test() {
  log "smoke test: protoc + grpc_cpp_plugin"
  local pv
  pv="$("${BIN_DIR}/protoc" --version)"
  [ "$pv" = "libprotoc ${PROTOC_VERSION}" ] \
    || die "protoc reports '${pv}', expected 'libprotoc ${PROTOC_VERSION}'"

  local td
  td="$(mktemp -d "${WORK_DIR}/smoke.XXXXXX")"
  cat > "${td}/pin_smoke.proto" <<'PROTO_EOF'
syntax = "proto3";
package pin.smoke;

message PingRequest { string msg = 1; }
message PingReply   { string msg = 1; }

service PingService {
  rpc Ping(PingRequest) returns (PingReply);
}
PROTO_EOF

  mkdir -p "${td}/out"
  "${BIN_DIR}/protoc" \
    --plugin=protoc-gen-grpc="${BIN_DIR}/grpc_cpp_plugin" \
    -I "${td}" \
    --cpp_out="${td}/out" \
    --grpc_out="${td}/out" \
    "${td}/pin_smoke.proto"

  local h="${td}/out/pin_smoke.grpc.pb.h"
  local c="${td}/out/pin_smoke.grpc.pb.cc"
  [ -s "$h" ] || die "smoke test failed: ${h} missing or empty"
  [ -s "$c" ] || die "smoke test failed: ${c} missing or empty"
  grep -q "PingService" "$h" \
    || die "smoke test failed: generated header lacks expected service symbol"

  log "smoke test PASSED"
  log "  $(basename "$h"): $(wc -l < "$h") lines, $(wc -c < "$h") bytes"
  log "  $(basename "$c"): $(wc -l < "$c") lines, $(wc -c < "$c") bytes"
  rm -rf "$td"
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
main() {
  preflight

  if already_pinned; then
    log "already pinned: gRPC ${GRPC_VERSION}, protoc ${PROTOC_VERSION} present in tools/bin. Nothing to do (use --force to rebuild)."
    exit 0
  fi

  if [ "$FORCE" -eq 1 ]; then
    log "--force: clearing installed binaries, includes and build trees (downloads reused if verified)"
    rm -rf "$PREFIX" "$BIN_DIR" "$INCLUDE_DIR" \
      "${WORK_DIR}/build-abseil" "${WORK_DIR}/build-protobuf" \
      "${WORK_DIR}/build-plugin" "${WORK_DIR}/plugin-build-src"
  fi

  mkdir -p "$DL_DIR" "$PREFIX" "$BIN_DIR" "$INCLUDE_DIR"

  install_protoc
  build_abseil
  build_protobuf
  build_plugin
  # Stamp only after the smoke test proves the toolchain works — otherwise a
  # broken install would satisfy already_pinned() on the next run.
  smoke_test
  write_stamp

  log "done. Installed:"
  log "  protoc          : $("${BIN_DIR}/protoc" --version)  -> ${BIN_DIR}/protoc"
  log "  grpc_cpp_plugin : gRPC ${GRPC_VERSION} (built from source) -> ${BIN_DIR}/grpc_cpp_plugin"
  log "  well-known .proto includes -> ${INCLUDE_DIR}/"
}

main "$@"
