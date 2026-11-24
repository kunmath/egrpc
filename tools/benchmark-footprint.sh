#!/usr/bin/env bash
# Head-to-head size & memory footprint benchmark: egrpc vs upstream Google
# gRPC C++ (Ubuntu 24.04's packaged libgrpc++ 1.51.1), x86-64 host.
#
# Builds the SAME minimal route_guide client (bench_client.cc below: TLS
# channel, GetFeature unary + ListFeatures server-streaming, then reads
# /proc/self/status VmRSS/VmHWM while the channel is still live) against both
# libraries, strips everything identically, runs both against the same live
# upstream-grpcio RouteGuideTestServer, and prints a Markdown report.
#
# Upstream gRPC is unpacked into a throwaway sysroot with `apt-get download` +
# `dpkg-deb -x` (no sudo, nothing installed system-wide).
#
# Usage: tools/benchmark-footprint.sh [--keep-tmp] [--runs N]
# Env:   PYTHON  python with grpcio + grpcio-tools (auto-detected otherwise)
#        JOBS    parallel build jobs (default: nproc)
#
# Host requirements: Ubuntu 24.04 (noble) x86-64, cmake, g++, pkg-config,
# openssl CLI, apt-get, dpkg-deb, binutils, and the egrpc build deps
# (libssl-dev, libnghttp2-dev, libprotobuf-dev incl. static archives).
set -euo pipefail
# Make command substitutions ($(...)) honour errexit too (bash >= 4.4).
shopt -s inherit_errexit

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KEEP_TMP=0
RUNS=3
JOBS="${JOBS:-$(nproc)}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep-tmp) KEEP_TMP=1 ;;
    --runs)
      if [[ $# -lt 2 || ! "$2" =~ ^[1-9][0-9]*$ ]]; then
        echo "--runs needs a positive integer" >&2
        exit 2
      fi
      RUNS="$2"
      shift
      ;;
    -h | --help)
      sed -n '2,21p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

if [[ -z "${PYTHON:-}" ]]; then
  for cand in "$REPO_ROOT/.venv/bin/python" /home/kunal/my_space/egrpc/.venv/bin/python python3; do
    if command -v "$cand" >/dev/null 2>&1 && "$cand" -c 'import grpc, grpc_tools' 2>/dev/null; then
      PYTHON="$cand"
      break
    fi
  done
fi
if [[ -z "${PYTHON:-}" ]]; then
  echo "no python with grpcio + grpcio-tools found; set PYTHON=" >&2
  exit 1
fi

mkdir -p /tmp/opencode
WORK="$(mktemp -d /tmp/opencode/egrpc-bench.XXXXXX)"
SERVER_PID=""
cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
    kill "$SERVER_PID" 2>/dev/null || true
    wait "$SERVER_PID" 2>/dev/null || true
  fi
  if [[ "$KEEP_TMP" -eq 1 ]]; then
    echo "keeping temp dir: $WORK" >&2
  else
    rm -rf "$WORK"
  fi
}
trap cleanup EXIT

log() { echo "[bench] $*" >&2; }

MULTIARCH="x86_64-linux-gnu"
SYSROOT="$WORK/upstream-root"
SR_LIB="$SYSROOT/usr/lib/$MULTIARCH"
CXX="${CXX:-g++}"
CXXFLAGS_COMMON=(-std=c++17 -O2 -DNDEBUG -pthread)

# ---------------------------------------------------------------- helpers ---
size_of() { stat -c%s "$1"; }

# strip_copy MODE SRC -> prints path of a stripped copy (MODE: unneeded|all|debug)
strip_copy() {
  local mode="$1" src="$2"
  local dst
  dst="$WORK/stripped/$(basename "$src").$mode.$RANDOM$RANDOM"
  mkdir -p "$WORK/stripped" || return 1
  cp "$(readlink -f "$src")" "$dst" || return 1
  if ! strip "--strip-$mode" "$dst"; then
    echo "strip --strip-$mode failed: $src" >&2
    return 1
  fi
  echo "$dst"
}
stripped_size() {
  local tmp
  tmp="$(strip_copy "$1" "$2")" || return 1
  size_of "$tmp"
}

human() {
  awk -v b="$1" 'BEGIN {
    if (b >= 1048576) printf "%.2f MiB", b / 1048576;
    else printf "%.1f KiB", b / 1024 }'
}
ratio() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%.1fx", a / b }'; }

# closure BINARY -> deduplicated realpaths of non-libc deps
closure() {
  local bin="$1" out
  out="$(ldd "$bin")"
  if grep -q 'not found' <<<"$out"; then
    echo "unresolved shared libraries in $bin:" >&2
    echo "$out" >&2
    return 1
  fi
  echo "$out" |
    awk '/=>/ && $3 ~ /^\// { print $3 }' |
    xargs -r -n1 readlink -f |
    grep -Ev '/(linux-vdso|ld-linux[^/]*|libc|libm|libgcc_s|libstdc\+\+|libpthread|librt|libdl)\.so[^/]*$' |
    sort -u || true
}
closure_stripped_total() {
  local total=0 f sz
  while read -r f; do
    [[ -n "$f" ]] || continue
    sz="$(stripped_size unneeded "$f")" || return 1
    total=$((total + sz))
  done
  echo "$total"
}

# Conventional median; an even count averages the middle pair (rounded half up).
median() {
  sort -n | awk '{ a[NR] = $1 }
    END {
      if (NR == 0) exit 1
      if (NR % 2) print a[(NR + 1) / 2]
      else print int((a[NR / 2] + a[NR / 2 + 1]) / 2 + 0.5)
    }'
}

# ------------------------------------------------------- bench_client.cc ---
mkdir -p "$WORK/src"
cat >"$WORK/src/bench_client.cc" <<'EOF'
// Minimal, library-agnostic route_guide client used for the footprint
// benchmark. Compiles unchanged against egrpc's grpcpp shim and upstream gRPC.
#include <grpcpp/grpcpp.h>

#include <cstdio>
#include <cstring>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>

#include "route_guide.grpc.pb.h"

namespace {
long StatusKb(const char* key) {
  std::ifstream in("/proc/self/status");
  std::string line;
  const size_t n = std::strlen(key);
  while (std::getline(in, line)) {
    if (line.compare(0, n, key) == 0) return std::stol(line.substr(n + 1));
  }
  return -1;
}
}  // namespace

int main(int argc, char** argv) {
  if (argc != 3) {
    std::fprintf(stderr, "usage: %s host:port ca.pem\n", argv[0]);
    return 2;
  }
  std::ifstream ca(argv[2], std::ios::binary);
  std::ostringstream pem;
  pem << ca.rdbuf();
  grpc::SslCredentialsOptions ssl;
  ssl.pem_root_certs = pem.str();
  if (ssl.pem_root_certs.empty()) return 2;

  std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(argv[1], grpc::SslCredentials(ssl));
  std::unique_ptr<routeguide::RouteGuide::Stub> stub = routeguide::RouteGuide::NewStub(channel);

  routeguide::Point point;
  point.set_latitude(1000);
  point.set_longitude(2000);
  routeguide::Feature feature;
  grpc::ClientContext unary_ctx;
  grpc::Status st = stub->GetFeature(&unary_ctx, point, &feature);
  if (!st.ok()) {
    std::fprintf(stderr, "GetFeature failed: %d %s\n", static_cast<int>(st.error_code()),
                 st.error_message().c_str());
    return 1;
  }

  routeguide::Rectangle rect;
  rect.mutable_lo()->set_latitude(0);
  rect.mutable_lo()->set_longitude(0);
  rect.mutable_hi()->set_latitude(0);
  rect.mutable_hi()->set_longitude(0);
  grpc::ClientContext stream_ctx;
  std::unique_ptr<grpc::ClientReader<routeguide::Feature>> reader(
      stub->ListFeatures(&stream_ctx, rect));
  int features = 0;
  while (reader->Read(&feature)) ++features;
  st = reader->Finish();
  if (!st.ok() || features != 5) {
    std::fprintf(stderr, "ListFeatures failed: %d %s features=%d\n",
                 static_cast<int>(st.error_code()), st.error_message().c_str(), features);
    return 1;
  }

  // Channel (and its TLS connection) is still alive here.
  std::printf("RESULT getfeature=ok features=%d vmrss_kb=%ld vmhwm_kb=%ld\n", features,
              StatusKb("VmRSS:"), StatusKb("VmHWM:"));
  return 0;
}
EOF

# ------------------------------------------------------------- egrpc build ---
log "building egrpc (Release) ..."
cmake -S "$REPO_ROOT" -B "$WORK/egrpc-build" -DCMAKE_BUILD_TYPE=Release \
  -DEGRPC_BUILD_TESTS=OFF -DEGRPC_BUILD_EXAMPLES=ON \
  -DCMAKE_INSTALL_PREFIX="$WORK/egrpc-prefix" >"$WORK/egrpc-cmake.log" 2>&1
cmake --build "$WORK/egrpc-build" -j "$JOBS" >"$WORK/egrpc-build.log" 2>&1
cmake --install "$WORK/egrpc-build" >"$WORK/egrpc-install.log" 2>&1

EG_PREFIX="$WORK/egrpc-prefix"
EG_LIBDIR="$(dirname "$(find "$EG_PREFIX" -name libegrpc.so.0.1.0 | head -n1)")"
EG_SO="$EG_LIBDIR/libegrpc.so.0.1.0"
EG_A="$EG_LIBDIR/libegrpc.a"
EG_GEN="$REPO_ROOT/examples/route_guide/gen"
EG_OBJ="$WORK/egrpc-obj"
mkdir -p "$EG_OBJ"

log "building egrpc bench clients ..."
for s in route_guide.pb route_guide.grpc.pb; do
  "$CXX" "${CXXFLAGS_COMMON[@]}" -w -I"$EG_PREFIX/include" -I"$EG_GEN" \
    -c "$EG_GEN/$s.cc" -o "$EG_OBJ/$s.o"
done
"$CXX" "${CXXFLAGS_COMMON[@]}" -I"$EG_PREFIX/include" -I"$EG_GEN" \
  -c "$WORK/src/bench_client.cc" -o "$EG_OBJ/bench_client.o"
EG_OBJS=("$EG_OBJ/bench_client.o" "$EG_OBJ/route_guide.pb.o" "$EG_OBJ/route_guide.grpc.pb.o")

EG_LINK_SHARED=("$CXX" -pthread -o "$WORK/egrpc_bench_shared" "${EG_OBJS[@]}"
  -L"$EG_LIBDIR" -Wl,-rpath,"$EG_LIBDIR" -legrpc -lnghttp2 -lssl -lcrypto -lprotobuf-lite)
EG_LINK_STATIC=("$CXX" -pthread -o "$WORK/egrpc_bench_static" "${EG_OBJS[@]}"
  "$EG_A" -lnghttp2 -lssl -lcrypto -lprotobuf-lite)
EG_LINK_FULLSTATIC=("$CXX" -pthread -o "$WORK/egrpc_bench_fullstatic" "${EG_OBJS[@]}"
  "$EG_A" -Wl,-Bstatic -lnghttp2 -lssl -lcrypto -lprotobuf-lite -Wl,-Bdynamic)
"${EG_LINK_SHARED[@]}"
"${EG_LINK_STATIC[@]}"
"${EG_LINK_FULLSTATIC[@]}"

# ---------------------------------------------------- upstream gRPC sysroot ---
UP_PKGS=(libgrpc29t64 libgrpc++1.51t64 libgrpc-dev libgrpc++-dev protobuf-compiler-grpc
  protobuf-compiler libabsl20220623t64 libabsl-dev libcares2 libc-ares-dev libre2-10
  libre2-dev libprotobuf32t64 libprotobuf-lite32t64 libprotobuf-dev libprotoc32t64)
log "fetching upstream gRPC packages: ${UP_PKGS[*]}"
mkdir -p "$WORK/debs" "$SYSROOT"
(cd "$WORK/debs" && apt-get download "${UP_PKGS[@]}" >"$WORK/apt.log" 2>&1)
for deb in "$WORK"/debs/*.deb; do
  dpkg-deb -x "$deb" "$SYSROOT"
done
UP_VERSIONS="$(for deb in "$WORK"/debs/*.deb; do
  dpkg-deb -f "$deb" Package Version | paste -sd' '
done)"

UP_GEN="$WORK/upstream-gen"
mkdir -p "$UP_GEN"
LD_LIBRARY_PATH="$SR_LIB" "$SYSROOT/usr/bin/protoc" -I"$REPO_ROOT/examples/route_guide" \
  --cpp_out="$UP_GEN" --grpc_out="$UP_GEN" \
  --plugin=protoc-gen-grpc="$SYSROOT/usr/bin/grpc_cpp_plugin" \
  "$REPO_ROOT/examples/route_guide/route_guide.proto"
PROTOC_VERSION="$(LD_LIBRARY_PATH="$SR_LIB" "$SYSROOT/usr/bin/protoc" --version)"

export PKG_CONFIG_SYSROOT_DIR="$SYSROOT"
# openssl.pc comes from the host libssl-dev.
export PKG_CONFIG_LIBDIR="$SR_LIB/pkgconfig:$SYSROOT/usr/share/pkgconfig:/usr/lib/$MULTIARCH/pkgconfig"
read -r -a UP_CFLAGS <<<"$(pkg-config --cflags grpc++ protobuf-lite)"
read -r -a UP_LIBS <<<"$(pkg-config --libs grpc++ protobuf-lite)"
UP_GRPC_VERSION="$(pkg-config --modversion grpc++)"
unset PKG_CONFIG_SYSROOT_DIR PKG_CONFIG_LIBDIR

log "building upstream gRPC $UP_GRPC_VERSION bench clients ..."
UP_OBJ="$WORK/upstream-obj"
mkdir -p "$UP_OBJ"
for s in route_guide.pb route_guide.grpc.pb; do
  "$CXX" "${CXXFLAGS_COMMON[@]}" -w -I"$SYSROOT/usr/include" -I"$UP_GEN" "${UP_CFLAGS[@]}" \
    -c "$UP_GEN/$s.cc" -o "$UP_OBJ/$s.o"
done
"$CXX" "${CXXFLAGS_COMMON[@]}" -w -I"$SYSROOT/usr/include" -I"$UP_GEN" "${UP_CFLAGS[@]}" \
  -c "$WORK/src/bench_client.cc" -o "$UP_OBJ/bench_client.o"
UP_OBJS=("$UP_OBJ/bench_client.o" "$UP_OBJ/route_guide.pb.o" "$UP_OBJ/route_guide.grpc.pb.o")

# Static variants: Ubuntu ships libcares as libcares_static.a.
UP_STATIC_LIBS=()
UP_GRPC_STATIC_LIBS=()
for l in "${UP_LIBS[@]}"; do
  case "$l" in
    -lcares) UP_STATIC_LIBS+=(-lcares_static) ;;
    *) UP_STATIC_LIBS+=("$l") ;;
  esac
done
# Upstream links use DT_RPATH (not RUNPATH) so the sysroot also resolves
# indirect deps such as libgrpc.so -> libupb.so.29.
UP_SHARED_DEPS=()
for l in "${UP_LIBS[@]}"; do
  case "$l" in
    -lgrpc++ | -lgrpc | -lgpr | -lupb | -laddress_sorting) UP_GRPC_STATIC_LIBS+=("$l") ;;
    *) UP_SHARED_DEPS+=("$l") ;;
  esac
done

UP_LINK_SHARED=("$CXX" -pthread -o "$WORK/upstream_bench_shared" "${UP_OBJS[@]}"
  -Wl,--disable-new-dtags,-rpath,"$SR_LIB" "${UP_LIBS[@]}")
UP_LINK_STATIC=("$CXX" -pthread -o "$WORK/upstream_bench_static" "${UP_OBJS[@]}"
  -L"$SR_LIB" -Wl,-Bstatic -Wl,--start-group "${UP_GRPC_STATIC_LIBS[@]}" -Wl,--end-group
  -Wl,-Bdynamic -Wl,--disable-new-dtags,-rpath,"$SR_LIB" "${UP_SHARED_DEPS[@]}")
UP_LINK_FULLSTATIC=("$CXX" -pthread -o "$WORK/upstream_bench_fullstatic" "${UP_OBJS[@]}"
  -L"$SR_LIB" -Wl,-Bstatic -Wl,--start-group "${UP_STATIC_LIBS[@]}" -Wl,--end-group
  -Wl,-Bdynamic)
"${UP_LINK_SHARED[@]}"
"${UP_LINK_STATIC[@]}"
"${UP_LINK_FULLSTATIC[@]}"

# ------------------------------------------------------------ measurements ---
log "measuring sizes ..."
EG_SO_RAW=$(size_of "$EG_SO")
EG_SO_STRIP=$(stripped_size unneeded "$EG_SO")
EG_A_RAW=$(size_of "$EG_A")
EG_A_STRIP=$(stripped_size debug "$EG_A")

UP_CORE=(grpc++ grpc gpr upb address_sorting)
UP_SO_RAW=0
UP_SO_STRIP=0
UP_A_RAW=0
UP_A_STRIP=0
UP_SO_ROWS=""
for n in "${UP_CORE[@]}"; do
  so="$SR_LIB/lib$n.so"
  a="$SR_LIB/lib$n.a"
  r=$(size_of "$(readlink -f "$so")")
  s=$(stripped_size unneeded "$so")
  ar=$(size_of "$a")
  as=$(stripped_size debug "$a")
  UP_SO_RAW=$((UP_SO_RAW + r))
  UP_SO_STRIP=$((UP_SO_STRIP + s))
  UP_A_RAW=$((UP_A_RAW + ar))
  UP_A_STRIP=$((UP_A_STRIP + as))
  UP_SO_ROWS+="| \`$(basename "$(readlink -f "$so")")\` | $r | $s | $ar | $as |"$'\n'
done

EG_CLOSURE="$(closure "$WORK/egrpc_bench_shared")"
UP_CLOSURE="$(closure "$WORK/upstream_bench_shared")"
EG_CLOSURE_N=$(grep -c . <<<"$EG_CLOSURE")
UP_CLOSURE_N=$(grep -c . <<<"$UP_CLOSURE")
EG_CLOSURE_SIZE=$(closure_stripped_total <<<"$EG_CLOSURE")
UP_CLOSURE_SIZE=$(closure_stripped_total <<<"$UP_CLOSURE")
UP_ABSL_N=$(grep -c '/libabsl_' <<<"$UP_CLOSURE" || true)
EG_ABSL_N=$(grep -c '/libabsl_' <<<"$EG_CLOSURE" || true)
EG_NEEDED="$(readelf -d "$EG_SO" | awk '/NEEDED/ { gsub(/[][]/, "", $5); print $5 }' | paste -sd' ')"
UP_NEEDED="$(readelf -d "$WORK/upstream_bench_shared" | awk '/NEEDED/ { gsub(/[][]/, "", $5); print $5 }' | paste -sd' ')"
UP_NEEDED_N=$(wc -w <<<"$UP_NEEDED")
EG_BIN_NEEDED="$(readelf -d "$WORK/egrpc_bench_shared" | awk '/NEEDED/ { gsub(/[][]/, "", $5); print $5 }' | paste -sd' ')"
EG_NEEDED_N=$(wc -w <<<"$EG_BIN_NEEDED")

closure_rows() {
  local f sz
  while read -r f; do
    [[ -n "$f" ]] || continue
    sz="$(stripped_size unneeded "$f")" || return 1
    echo "| \`$(basename "$f")\` | $sz |"
  done
}

EG_CLOSURE_ROWS="$(closure_rows <<<"$EG_CLOSURE")"
UP_CLOSURE_ROWS="$(closure_rows <<<"$UP_CLOSURE")"

declare -A BIN_STRIP BIN_RAW
for b in egrpc_bench_shared egrpc_bench_static egrpc_bench_fullstatic \
  upstream_bench_shared upstream_bench_static upstream_bench_fullstatic; do
  BIN_RAW[$b]=$(size_of "$WORK/$b")
  BIN_STRIP[$b]=$(stripped_size all "$WORK/$b")
done
# Static-variant runtime closures (deps left shared).
EG_STATIC_CLOSURE="$(closure "$WORK/egrpc_bench_static")"
UP_STATIC_CLOSURE="$(closure "$WORK/upstream_bench_static")"
EG_FULLSTATIC_CLOSURE="$(closure "$WORK/egrpc_bench_fullstatic")"
UP_FULLSTATIC_CLOSURE="$(closure "$WORK/upstream_bench_fullstatic")"
EG_STATIC_CLOSURE_SIZE=$(closure_stripped_total <<<"$EG_STATIC_CLOSURE")
UP_STATIC_CLOSURE_SIZE=$(closure_stripped_total <<<"$UP_STATIC_CLOSURE")
EG_STATIC_CLOSURE_N=$(grep -c . <<<"$EG_STATIC_CLOSURE" || true)
UP_STATIC_CLOSURE_N=$(grep -c . <<<"$UP_STATIC_CLOSURE" || true)
EG_FULLSTATIC_CLOSURE_N=$(grep -c . <<<"$EG_FULLSTATIC_CLOSURE" || true)
UP_FULLSTATIC_CLOSURE_N=$(grep -c . <<<"$UP_FULLSTATIC_CLOSURE" || true)

# ----------------------------------------------------------------- runtime ---
log "generating TLS cert and starting RouteGuideTestServer ($PYTHON) ..."
openssl req -x509 -newkey rsa:2048 -keyout "$WORK/key.pem" -out "$WORK/cert.pem" \
  -days 2 -nodes -subj /CN=localhost \
  -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" >/dev/null 2>&1

PORT_FILE="$WORK/port"
(
  cd "$REPO_ROOT"
  exec "$PYTHON" - "$WORK/cert.pem" "$WORK/key.pem" "$PORT_FILE" <<'EOF'
import signal, sys, threading
sys.path.insert(0, ".")
from tests.server.route_guide_server import RouteGuideTestServer
cert, key, port_file = sys.argv[1:4]
srv = RouteGuideTestServer(cert, key).start()
with open(port_file + ".tmp", "w") as fh:
    fh.write(str(srv.port))
import os
os.rename(port_file + ".tmp", port_file)
done = threading.Event()
signal.signal(signal.SIGTERM, lambda *_: done.set())
done.wait()
srv.stop()
EOF
) >"$WORK/server.log" 2>&1 &
SERVER_PID=$!
for _ in $(seq 1 200); do
  [[ -s "$PORT_FILE" ]] && break
  kill -0 "$SERVER_PID" 2>/dev/null || {
    cat "$WORK/server.log" >&2
    exit 1
  }
  sleep 0.05
done
[[ -s "$PORT_FILE" ]] || {
  echo "server did not start" >&2
  exit 1
}
TARGET="127.0.0.1:$(cat "$PORT_FILE")"
log "server listening on $TARGET"

declare -A RSS_MED HWM_MED
for b in egrpc_bench_shared egrpc_bench_static egrpc_bench_fullstatic \
  upstream_bench_shared upstream_bench_static upstream_bench_fullstatic; do
  bin="$(strip_copy all "$WORK/$b")"
  rss=()
  hwm=()
  for _ in $(seq 1 "$RUNS"); do
    out="$("$bin" "$TARGET" "$WORK/cert.pem")"
    log "$b: $out"
    [[ "$out" =~ ^RESULT\ getfeature=ok\ features=5\ vmrss_kb=([0-9]+)\ vmhwm_kb=([0-9]+)$ ]] || {
      echo "$b: unexpected output: $out" >&2
      exit 1
    }
    rss+=("${BASH_REMATCH[1]}")
    hwm+=("${BASH_REMATCH[2]}")
  done
  RSS_MED[$b]=$(printf '%s\n' "${rss[@]}" | median)
  HWM_MED[$b]=$(printf '%s\n' "${hwm[@]}" | median)
done

kill "$SERVER_PID"
wait "$SERVER_PID" 2>/dev/null || true
SERVER_PID=""

# ------------------------------------------------------------------ report ---
row() { # label egrpc upstream
  echo "| $1 | $2 ($(human "$2")) | $3 ($(human "$3")) | $(ratio "$3" "$2") smaller |"
}
kb_row() {
  echo "| $1 | $2 KiB | $3 KiB | $(ratio "$3" "$2") smaller |"
}

cat <<EOF
# egrpc vs upstream gRPC C++ footprint (x86-64)

- Host: $(. /etc/os-release && echo "$PRETTY_NAME"), $(uname -m), $("$CXX" --version | head -n1)
- egrpc: $(git -C "$REPO_ROOT" describe --always --dirty 2>/dev/null || echo unknown), CMake Release (-O3 -DNDEBUG), stubs: examples/route_guide/gen (pinned grpc_cpp_plugin)
- upstream: gRPC C++ $UP_GRPC_VERSION (Ubuntu packages), stubs regenerated with $PROTOC_VERSION + Ubuntu grpc_cpp_plugin
- bench client flags: ${CXXFLAGS_COMMON[*]}; runtime = median of $RUNS runs against live grpcio RouteGuideTestServer ($("$PYTHON" -c 'import grpc; print("grpcio", grpc.__version__)'))
- Stripping: .so --strip-unneeded, binaries --strip-all, .a --strip-debug

## Headline

| Metric | egrpc | upstream gRPC C++ | Ratio |
|---|---|---|---|
$(row "Library .so, stripped (egrpc: libegrpc.so; upstream: grpc++ + grpc + gpr + upb + address_sorting)" "$EG_SO_STRIP" "$UP_SO_STRIP")
$(row "Library .a, --strip-debug (same set)" "$EG_A_STRIP" "$UP_A_STRIP")
$(row "Full non-libc runtime .so closure of shared client, stripped" "$EG_CLOSURE_SIZE" "$UP_CLOSURE_SIZE")
| Shared libs in that closure (count) | $EG_CLOSURE_N | $UP_CLOSURE_N | |
| DT_NEEDED entries of shared client | $EG_NEEDED_N | $UP_NEEDED_N | |
| libabsl_* in closure | $EG_ABSL_N | $UP_ABSL_N | |
$(row "Client binary, gRPC lib static + deps shared, stripped" "${BIN_STRIP[egrpc_bench_static]}" "${BIN_STRIP[upstream_bench_static]}")
$(row "  ... + its runtime .so closure, stripped" "$((BIN_STRIP[egrpc_bench_static] + EG_STATIC_CLOSURE_SIZE))" "$((BIN_STRIP[upstream_bench_static] + UP_STATIC_CLOSURE_SIZE))")
$(row "Client binary, all non-libc deps static, stripped" "${BIN_STRIP[egrpc_bench_fullstatic]}" "${BIN_STRIP[upstream_bench_fullstatic]}")
$(kb_row "VmHWM (peak RSS), shared client" "${HWM_MED[egrpc_bench_shared]}" "${HWM_MED[upstream_bench_shared]}")
$(kb_row "VmRSS (channel live), shared client" "${RSS_MED[egrpc_bench_shared]}" "${RSS_MED[upstream_bench_shared]}")
$(kb_row "VmHWM (peak RSS), gRPC-static client" "${HWM_MED[egrpc_bench_static]}" "${HWM_MED[upstream_bench_static]}")
$(kb_row "VmRSS (channel live), gRPC-static client" "${RSS_MED[egrpc_bench_static]}" "${RSS_MED[upstream_bench_static]}")
$(kb_row "VmHWM (peak RSS), fully-static client" "${HWM_MED[egrpc_bench_fullstatic]}" "${HWM_MED[upstream_bench_fullstatic]}")
$(kb_row "VmRSS (channel live), fully-static client" "${RSS_MED[egrpc_bench_fullstatic]}" "${RSS_MED[upstream_bench_fullstatic]}")

## Library detail

| Artifact | unstripped | stripped |
|---|---|---|
| \`libegrpc.so.0.1.0\` | $EG_SO_RAW | $EG_SO_STRIP |
| \`libegrpc.a\` | $EG_A_RAW | $EG_A_STRIP |

| Upstream lib | .so unstripped | .so stripped | .a unstripped | .a stripped |
|---|---|---|---|---|
${UP_SO_ROWS}| **total** | $UP_SO_RAW | $UP_SO_STRIP | $UP_A_RAW | $UP_A_STRIP |

## Shared-client runtime closures (stripped --strip-unneeded)

egrpc (\`libegrpc.so\` NEEDED: $EG_NEEDED; client NEEDED: $EG_BIN_NEEDED)

| Library | stripped bytes |
|---|---|
$EG_CLOSURE_ROWS
| **total ($EG_CLOSURE_N libs)** | $EG_CLOSURE_SIZE |

upstream (client NEEDED: $UP_NEEDED)

| Library | stripped bytes |
|---|---|
$UP_CLOSURE_ROWS
| **total ($UP_CLOSURE_N libs)** | $UP_CLOSURE_SIZE |

## Client binaries (bytes)

| Binary | unstripped | stripped | non-libc .so deps | median VmRSS KiB | median VmHWM KiB |
|---|---|---|---|---|---|
| egrpc shared | ${BIN_RAW[egrpc_bench_shared]} | ${BIN_STRIP[egrpc_bench_shared]} | $EG_CLOSURE_N | ${RSS_MED[egrpc_bench_shared]} | ${HWM_MED[egrpc_bench_shared]} |
| egrpc gRPC-static | ${BIN_RAW[egrpc_bench_static]} | ${BIN_STRIP[egrpc_bench_static]} | $EG_STATIC_CLOSURE_N | ${RSS_MED[egrpc_bench_static]} | ${HWM_MED[egrpc_bench_static]} |
| egrpc fully-static | ${BIN_RAW[egrpc_bench_fullstatic]} | ${BIN_STRIP[egrpc_bench_fullstatic]} | $EG_FULLSTATIC_CLOSURE_N | ${RSS_MED[egrpc_bench_fullstatic]} | ${HWM_MED[egrpc_bench_fullstatic]} |
| upstream shared | ${BIN_RAW[upstream_bench_shared]} | ${BIN_STRIP[upstream_bench_shared]} | $UP_CLOSURE_N | ${RSS_MED[upstream_bench_shared]} | ${HWM_MED[upstream_bench_shared]} |
| upstream gRPC-static | ${BIN_RAW[upstream_bench_static]} | ${BIN_STRIP[upstream_bench_static]} | $UP_STATIC_CLOSURE_N | ${RSS_MED[upstream_bench_static]} | ${HWM_MED[upstream_bench_static]} |
| upstream fully-static | ${BIN_RAW[upstream_bench_fullstatic]} | ${BIN_STRIP[upstream_bench_fullstatic]} | $UP_FULLSTATIC_CLOSURE_N | ${RSS_MED[upstream_bench_fullstatic]} | ${HWM_MED[upstream_bench_fullstatic]} |

## Link commands (\$W = temp work dir)

\`\`\`
$(for v in EG_LINK_SHARED EG_LINK_STATIC EG_LINK_FULLSTATIC UP_LINK_SHARED UP_LINK_STATIC UP_LINK_FULLSTATIC; do
  declare -n arr="$v"
  echo "${arr[*]}" | sed "s|$WORK|\$W|g"
  unset -n arr
done)
\`\`\`

## Upstream packages

\`\`\`
$UP_VERSIONS
\`\`\`
EOF
