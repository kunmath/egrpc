# egrpc footprint benchmark vs upstream gRPC C++

This page compares egrpc's size and memory footprint with upstream Google gRPC
C++. It covers two things:

1. **x86-64 head-to-head.** One small `route_guide` client is built against
   egrpc and against upstream gRPC C++ 1.51.1 (Ubuntu 24.04 packages). Both are
   stripped the same way and run against the same live `grpcio` server. All of
   this is reproduced by `tools/benchmark-footprint.sh`.
2. **Yocto scarthgap aarch64.** Sizes measured from a real `bitbake egrpc` build
   (M8 Task 2). There is no upstream gRPC counterpart in this table.

## Headline (x86-64, Ubuntu 24.04, GCC 13.3.0)

| Metric | egrpc | upstream gRPC C++ 1.51.1 | Ratio |
|---|---|---|---|
| gRPC library `.so`, stripped (egrpc: `libegrpc.so`; upstream: `libgrpc++` + `libgrpc` + `libgpr` + `libupb` + `libaddress_sorting`) | 402,600 B (393.2 KiB) | 6,698,328 B (6.39 MiB) | **16.6x smaller** |
| gRPC library `.a`, `--strip-debug` (same set) | 723,872 B (706.9 KiB) | 23,336,044 B (22.25 MiB) | **32.2x smaller** |
| Full non-libc runtime `.so` closure of the shared client, stripped | 7,429,848 B (7.09 MiB) | 15,516,112 B (14.80 MiB) | **2.1x smaller** |
| ... same closure without the shared `libssl` + `libcrypto` (6,005,912 B) | 1,423,936 B (1.36 MiB) | 9,510,200 B (9.07 MiB) | **6.7x smaller** |
| Shared libraries in that closure | 5 | 51 | |
| `libabsl_*` libraries in that closure | 0 | 40 | |
| Client binary, gRPC library static + deps shared, stripped | 408,208 B (398.6 KiB) | 6,148,616 B (5.86 MiB) | **15.1x smaller** |
| ... plus its runtime `.so` closure, stripped | 7,435,456 B (7.09 MiB) | 14,966,400 B (14.27 MiB) | 2.0x smaller |
| Client binary, all non-libc deps static, stripped | 6,205,392 B (5.92 MiB) | 12,970,424 B (12.37 MiB) | 2.1x smaller |
| Peak RSS (`VmHWM`), shared client | 10,556 KiB | 17,480 KiB | 1.7x smaller |
| Peak RSS (`VmHWM`), gRPC-static client | 10,572 KiB | 16,744 KiB | 1.6x smaller |
| Peak RSS (`VmHWM`), fully static client | 9,280 KiB | 14,236 KiB | 1.5x smaller |

In every run, `VmRSS` (taken while the TLS channel is still open) was equal to
`VmHWM`: these short-lived clients never shrink their memory. The memory figures
are the median of 3 runs. Most of the RSS is the shared baseline of libc,
libstdc++ and OpenSSL. egrpc also runs one EventThread per channel, while
upstream runs its own thread pools.

### Client binaries (bytes)

| Binary | unstripped | stripped | non-libc `.so` deps | median VmRSS KiB | median VmHWM KiB |
|---|---|---|---|---|---|
| egrpc shared | 131,016 | 84,544 | 5 | 10,556 | 10,556 |
| egrpc gRPC-static | 538,792 | 408,208 | 4 | 10,572 | 10,572 |
| egrpc fully-static | 7,203,480 | 6,205,392 | 0 | 9,280 | 9,280 |
| upstream shared | 431,088 | 277,056 | 51 | 17,480 | 17,480 |
| upstream gRPC-static | 8,300,824 | 6,148,616 | 46 | 16,744 | 16,744 |
| upstream fully-static | 16,213,864 | 12,970,424 | 0 | 14,236 | 14,236 |

The client variants are:

- **shared**: links every library dynamically.
- **gRPC-static**: links `libegrpc.a`, or upstream's `libgrpc++.a libgrpc.a
  libgpr.a libupb.a libaddress_sorting.a`, statically. All other deps stay
  shared. This is egrpc's standard example/Yocto link mode.
- **fully-static**: links every non-libc/libstdc++ dependency statically from
  its `.a`. That means OpenSSL, nghttp2 and protobuf-lite for egrpc, and Abseil,
  re2, c-ares, zlib, OpenSSL and protobuf-lite for upstream.

Ubuntu 24.04 **does** ship every static archive upstream needs:

- `libgrpc++-dev` / `libgrpc-dev` provide `libgrpc++.a`, `libgrpc.a`,
  `libgpr.a`, `libupb.a` and `libaddress_sorting.a`.
- `libabsl-dev` provides 64 `libabsl_*.a`.
- `libre2-dev` provides `libre2.a`.
- `libc-ares-dev` ships its archive as `libcares_static.a`.

So the fully-static upstream link is real; nothing is estimated. The script
prints the exact link commands.

### Library detail

| Artifact | unstripped | stripped |
|---|---|---|
| `libegrpc.so.0.1.0` | 492,072 | 402,600 |
| `libegrpc.a` | 724,360 | 723,872 |

| Upstream lib | `.so` unstripped | `.so` stripped | `.a` unstripped | `.a` stripped |
|---|---|---|---|---|
| `libgrpc++.so.1.51.1` | 759,088 | 759,088 | 2,644,052 | 2,644,052 |
| `libgrpc.so.29.0.0` | 5,680,744 | 5,680,744 | 20,229,538 | 20,229,538 |
| `libgpr.so.29.0.0` | 60,664 | 60,664 | 136,340 | 136,340 |
| `libupb.so.29.0.0` | 183,184 | 183,184 | 316,186 | 316,186 |
| `libaddress_sorting.so.29.0.0` | 14,648 | 14,648 | 9,928 | 9,928 |
| **total** | 6,698,328 | 6,698,328 | 23,336,044 | 23,336,044 |

Ubuntu's packages are already stripped, which is why upstream's unstripped and
stripped sizes are identical.

### Runtime closures (stripped `--strip-unneeded`)

**egrpc** has 5 libraries: `libegrpc.so.0.1.0` (402,600), `libssl.so.3`
(696,512), `libcrypto.so.3` (5,309,400), `libnghttp2.so.14.26.0` (170,352) and
`libprotobuf-lite.so.32.0.12` (850,984).

- `libegrpc.so` DT_NEEDED: `libssl.so.3 libcrypto.so.3 libnghttp2.so.14
  libstdc++.so.6 libm.so.6 libgcc_s.so.1 libc.so.6 ld-linux-x86-64.so.2`.

**Upstream** has 51 libraries:

- the 5 gRPC libraries above, `libprotobuf-lite.so.32.0.12`, `libre2.so.10.0.0`
  (400,480), `libcares.so.2.12.0` (157,792), `libz.so.1.3` (113,000),
  `libssl.so.3` and `libcrypto.so.3`;
- 40 `libabsl_*.so.20220623.0.0` libraries.

Client DT_NEEDED: `libgrpc++.so.1.51 libgrpc.so.29 libgpr.so.29
libabsl_synchronization.so.20220623 libprotobuf-lite.so.32` plus libc and
libstdc++. `libgrpc.so.29` alone pulls in 18 `libabsl_*` entries, plus `libz`,
`libcares`, `libre2`, `libupb`, `libaddress_sorting` and OpenSSL.

## Yocto scarthgap 5.0.21 (aarch64)

These figures come from `bitbake egrpc` with `PACKAGECONFIG = "examples"` on
`qemuarm64` (`aarch64-poky-linux`, GCC 13.5.0, protobuf-lite 4.25.8, nghttp2
1.61.0, OpenSSL 3.5.9). See `docs/yocto-spike.md` and `meta-egrpc`.

| Artifact | unstripped | stripped / packaged |
|---|---|---|
| `libegrpc.so.0.1.0` | 6,309,600 B | 329,784 B (in `libegrpc0`: 329,800 B) |
| `libegrpc.a` | 15,483,566 B | 694,118 B (`--strip-debug`) |
| `libegrpc0` RPM (`libegrpc0-0.1.0+git0+9aac0048d3-r0.cortexa57.rpm`) | | 148,309 B |
| `egrpc_route_guide_client` (static `libegrpc.a`; shared `libssl`, `libcrypto`, `libnghttp2`, `libprotobuf-lite.so.25.8.0`) | 8,350,504 B | 396,424 B |
| `egrpc_resilient_subscribe` | 8,732,648 B | 396,424 B |

`libegrpc.so.0.1.0` DT_NEEDED on aarch64: `libssl.so.3`, `libcrypto.so.3`,
`libnghttp2.so.14`, `libstdc++.so.6`, `libm.so.6`, `libgcc_s.so.1`,
`libc.so.6`, `ld-linux-aarch64.so.1`. That is **0 `libabsl` entries**.

**Abseil caveat on scarthgap:** egrpc itself has no Abseil dependency, but
scarthgap's `protobuf 4.25.8` runtime does. `libprotobuf-lite.so.25.8.0`
transitively pulls in `abseil-cpp` 20240116 (`libabsl_*.so.2401.0.0`): 10
libraries through `libprotobuf-lite`, plus 2 referenced directly by the 4.25.8
protobuf gencode headers. Any image that ships generated protobuf stubs must
also ship `abseil-cpp`. This cost comes from protobuf, not egrpc. On the x86-64
host above, protobuf 3.21.12 predates Abseil, so the egrpc closure there has
zero `libabsl` libraries.

## Methodology

`tools/benchmark-footprint.sh` does the following in a temporary directory
(`/tmp/opencode/egrpc-bench.XXXXXX`, removed on exit unless `--keep-tmp` is
passed):

1. **Build egrpc.** It configures, builds and installs egrpc with
   `-DCMAKE_BUILD_TYPE=Release -DEGRPC_BUILD_TESTS=OFF -DEGRPC_BUILD_EXAMPLES=ON`
   (CMake Release flags: `-O3 -DNDEBUG`).
2. **Set up upstream gRPC.** It runs `apt-get download` and `dpkg-deb -x` to
   unpack Ubuntu 24.04's upstream gRPC into a private sysroot. No sudo is needed
   and nothing is installed. The packages are:
   - `libgrpc29t64`, `libgrpc++1.51t64`, `libgrpc-dev`, `libgrpc++-dev` and
     `protobuf-compiler-grpc` at 1.51.1-4.1build5;
   - `libabsl20220623t64` and `libabsl-dev` at 20220623.1-3.1ubuntu3.2;
   - `libcares2` and `libc-ares-dev` at 1.27.0-1.0ubuntu1.1;
   - `libre2-10` and `libre2-dev` at 20230301-3build1;
   - `libprotobuf32t64`, `libprotobuf-lite32t64`, `libprotobuf-dev`,
     `libprotoc32t64` and `protobuf-compiler` at 3.21.12-8.2ubuntu0.4.

   OpenSSL (3.0.13), zlib and nghttp2 (1.59.0) come from the host.
3. **Compile `bench_client.cc`.** The script embeds this source, which compiles
   unchanged against both libraries. The client creates a TLS channel and calls
   `GetFeature(1000, 2000)`. It then calls `ListFeatures(0,0,0,0)` and requires
   5 features plus OK status. While the channel is still open, it prints
   `VmRSS`/`VmHWM` from `/proc/self/status`. Build flags are
   `-std=c++17 -O2 -DNDEBUG -pthread` on both sides.
   - egrpc uses the checked-in stubs in `examples/route_guide/gen` (pinned
     `grpc_cpp_plugin`).
   - Upstream uses stubs regenerated from the same `route_guide.proto` with
     Ubuntu's `protoc` 3.21.12 and `grpc_cpp_plugin` 1.51.1.
   - Both link `protobuf-lite`, because the proto sets
     `optimize_for = LITE_RUNTIME`. `libgrpc++.so` does not need full
     `libprotobuf`; this was checked manually with `readelf -d`.
4. **Strip everything the same way.** Shared libraries get
   `strip --strip-unneeded`, binaries get `strip --strip-all`, and archives get
   `strip --strip-debug`. The script then measures sizes with `stat -c%s`.
5. **Measure the runtime closure.** It runs `ldd` on each client, takes the
   deduplicated `realpath`s, and drops `linux-vdso`, `ld-linux`, `libc`, `libm`,
   `libgcc_s`, `libstdc++`, `libpthread`, `librt` and `libdl`. Each remaining
   library is stripped and its size added to the total. The script fails if any
   dependency is unresolved. Upstream binaries use `DT_RPATH`
   (`--disable-new-dtags`), so indirect sysroot dependencies such as
   `libgrpc.so` → `libupb.so.29` resolve too.
6. **Run the clients.** It starts one live upstream-`grpcio` (1.82.1)
   `RouteGuideTestServer` (`tests/server/route_guide_server.py`, TLS,
   self-signed certificate for `127.0.0.1`). Every stripped variant runs 3 times
   against that server, and the script reports the median. Any client that
   fails or returns the wrong feature count aborts the script.

### Reproduce

```
tools/benchmark-footprint.sh            # prints the Markdown report on stdout
tools/benchmark-footprint.sh --keep-tmp # keep build dirs for inspection
PYTHON=/path/to/venv/bin/python tools/benchmark-footprint.sh --runs 5
```

The script needs an Ubuntu 24.04 x86-64 host. It also needs `cmake`, `g++`,
`pkg-config`, `openssl`, `apt-get`/`dpkg-deb`, binutils, egrpc's build deps
(including static `libssl.a`, `libcrypto.a`, `libnghttp2.a` and
`libprotobuf-lite.a` for the fully-static variant), and a Python that has
`grpcio` and `grpcio-tools`. The script looks for that Python in `$PYTHON`,
then `.venv/bin/python`, then `python3`.

## Caveats

- **The feature sets differ.** Upstream gRPC includes a server, client/bidi
  streaming, async completion queues, xDS/load balancing, a c-ares resolver,
  compression, interceptors, channelz and retries. egrpc deliberately leaves all
  of these out (design §1). The numbers show the cost of those features for a
  client that only needs unary and server-streaming calls. They do not compare
  equal functionality.
- **The gRPC versions differ.** Ubuntu 24.04 packages gRPC 1.51.1 and protobuf
  3.21.12, but egrpc pins `grpc_cpp_plugin` v1.82.0 / protoc 35.0 for its
  checked-in stubs. Newer upstream releases have dropped some dependencies and
  added others (for example, more Abseil and newer upb), so the absolute
  upstream numbers will move. A source build of upstream with custom flags
  (`-Os`, `--gc-sections`, no c-ares) could also come out smaller.
- **Shared libraries dominate the closure.** `libssl` and `libcrypto` make up
  about 6.0 MB of both closures, and most systems already have them. Without
  them, the gap is 6.7x (1.36 MiB vs 9.07 MiB).
- **The fully-static binaries depend on the linker.** The linker pulls in only
  the objects each client references, and both sides use the default GNU ld
  without `--gc-sections`.
- **RSS figures are host-specific.** They depend on glibc, the allocator and
  page size. They come from a short-lived process that keeps one connection
  open, not from a long-running service.
- **The x86-64 and Yocto rows come from different toolchains.** The Yocto build
  uses GCC 13.5.0 on aarch64, so its byte counts are not directly comparable
  with the x86-64 ones.
