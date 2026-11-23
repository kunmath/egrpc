# Pinned codegen tools

egrpc relies on **one pinned `grpc_cpp_plugin` + `protoc` pair per release**.
`grpc::internal` is explicitly unstable upstream, so the generated-stub contract
is frozen per egrpc release and verified in CI (design §4.5). `tools/pin-codegen.sh`
installs these host tools into `tools/bin/`; nothing here is cross-compiled to the
target (design §4, item 5 — codegen tools are host-side only).

## Pinned versions (v0.1 — gRPC v1.82.0)

| Component | Version | Kind | URL | SHA-256 |
|-----------|---------|------|-----|---------|
| gRPC | v1.82.0 | source tarball (extracts to `grpc-1.82.0/`) | https://github.com/grpc/grpc/archive/refs/tags/v1.82.0.tar.gz | `d6851f59b9c4edb3218d2659a3abf1138e5bb4d2871c9e89c9330ed71756ed0b` |
| protoc | 35.0 | official prebuilt (linux-x86_64) | https://github.com/protocolbuffers/protobuf/releases/download/v35.0/protoc-35.0-linux-x86_64.zip | `a45cda0989c17dd950db55f6fbe1e5814c50fda08e87aa422980ac1f89dddbbc` |
| protobuf | 35.0 | source (libprotoc, to build the plugin; extracts to `protobuf-35.0/`, does **not** bundle Abseil) | https://github.com/protocolbuffers/protobuf/releases/download/v35.0/protobuf-35.0.tar.gz | `8f907baca4b34a3b4854103ba5811e418fb6e2ff11fe0d8df9e8280b11d79926` |
| abseil-cpp | 20250512.1 | source (the version protobuf 35.0 pins in its `MODULE.bazel`) | https://github.com/abseil/abseil-cpp/releases/download/20250512.1/abseil-cpp-20250512.1.tar.gz | `9b7a064305e9fd94d124ffa6cc358592eb42b5da588fb4e07d09254aa40086db` |

protoc 35.0 is the protobuf version that gRPC v1.82.0 pins. Every download is
verified against its SHA-256 **before use**; a mismatch is a hard failure.

## Why the plugin is compiled from source

Upstream gRPC publishes **no prebuilt `grpc_cpp_plugin` binaries** — there are no
release assets for the plugin on `grpc/grpc` (verified on v1.82.0) and nothing on
Maven Central. `protoc` itself ships an official prebuilt binary, but the plugin
does not. So `pin-codegen.sh`:

1. Installs the prebuilt `protoc` from the verified zip (plus its bundled
   well-known-type `.proto` files into `tools/include/`).
2. Builds **Abseil** (static, PIC) from the pinned tarball into a private prefix.
3. Builds **protobuf 35.0** (static `libprotoc`/`libprotobuf`) against that Abseil,
   with `protobuf_LOCAL_DEPENDENCIES_ONLY=ON` so protobuf's CMake resolves its
   dependency on the pinned Abseil rather than silently `FetchContent`-ing an
   unpinned copy.
4. Compiles **only** `grpc_cpp_plugin` — `src/compiler/cpp_plugin.cc`,
   `cpp_generator.cc`, `proto_parser_helper.cc` from the gRPC tarball — against
   the static `libprotoc`. The gRPC runtime is **not** built.

Outputs land in `tools/bin/{protoc,grpc_cpp_plugin}` and `tools/include/`.
All downloads and build trees live in `tools/.pin-codegen-work/`. `tools/bin`,
`tools/include`, and the work dir are ignored via the repository-root `.gitignore`
and never committed. The script is idempotent (a stamp file, written only after the
smoke test passes, records all pinned versions; re-runs no-op) and re-does everything with `--force`. It runs only on x86_64 Linux
and requires cmake >= 3.16, a C++17 compiler, `curl`, and `unzip`. `JOBS` controls
build parallelism (default `$(nproc)`).

## Message codegen vs. stub codegen (M3 clarification)

The pin above freezes the **`grpc_cpp_plugin` stub contract** (`.grpc.pb.h/.cc`
↔ the `grpcpp/` shim). It does **not** govern protobuf *message* codegen
(`.pb.h/.cc`): protobuf generated code must match the protobuf **runtime** it
compiles against, and egrpc's runtime is the **system** protobuf-lite (design
§1 — no vendoring; Yocto provides it). protoc 35.0 output cannot compile
against the 3.21.x runtime that current Ubuntu/CI images ship, so using the
pinned protoc for message codegen is impossible by construction.

Policy for message codegen: **message gencode is generated per consuming
runtime, exact-minor matched.** There is no cross-version compatibility
window to lean on: since the protobuf 4.x C++ line, the runtime hard-rejects
older gencode at compile time via `PROTOBUF_MIN_PROTOC_VERSION` — e.g.
protobuf 4.25.3 (`port_def.inc`) sets it to `4025000`, so anything generated
by a pre-25.0 protoc fails with `#error`. Concretely:

| Consuming build | protobuf-lite runtime | protoc for `.pb.h/.cc` |
|---|---|---|
| CI / Ubuntu images (checked-in gencode, e.g. `examples/route_guide/gen/`) | 3.21.12 | **protoc 21.12** (official prebuilt, `protoc-21.12-linux-x86_64.zip`, SHA-256 `3a4c1e5f2516c639d3079b1586e703fc7bcfa2136d58bda24d1d54f949c315e8`) |
| Yocto **scarthgap** (the supported LTS, design §8) | 4.25.x — **4.25.8** resolved in the 2026-11-24 spike (scarthgap 5.0.21; stable-branch point updates stay on 4.25.x) | **protoc 25.x matching the recipe** — in-recipe regeneration via `protobuf-native`, the standard Yocto pattern; the checked-in 21.12 gencode is desktop/CI-only and **must not** ship to target |

The M8 `meta-egrpc` recipe therefore regenerates all message gencode from the
`.proto` files at build time with `protobuf-native` (which bitbake keeps in
lockstep with the target runtime); only stub gencode (`.grpc.pb.*`) uses the
pinned plugin. This was first validated analytically (2026-07) and then
**confirmed empirically by the pre-M5 bitbake spike** (design §8a,
docs/yocto-spike.md), executed 2026-11-24:

- Yocto scarthgap, `DISTRO_VERSION` 5.0.21; poky
  `86a777b24f93803dcac8950c03c55da602186af8`, meta-openembedded
  `96c2c78e13ff1b954715ff3c6d26bddd50a02509`; `MACHINE` `qemuarm64`,
  `TARGET_SYS` `aarch64-poky-linux`; egrpc at
  `9320f06b181e6aa14066a0f5f2f8ede8fd5e60cd`.
- Resolved recipes: protobuf / protobuf-native **4.25.8** (`protoc --version`
  → `libprotoc 25.8`), nghttp2 1.61.0, openssl 3.5.9, gcc-cross-aarch64 13.5.0.
- Outcome: `bitbake egrpc-spike` succeeded end-to-end; `libegrpc.so`,
  `libegrpc.a`, and `egrpc_route_guide_client` compiled and linked as
  aarch64 ELF. In-recipe 25.8 message gencode compiled against
  protobuf-lite 4.25.8, and the checked-in v1.82.0 `.grpc.pb.*` (byte-identical
  to the repo) compiled against 4.25.8 headers via `include/grpcpp/`. The 2
  generated TUs build with `-w` (per `examples/route_guide/CMakeLists.txt`), so
  that proves compilation, not warning-cleanliness; the 13 handwritten TUs
  (12 `src/**` + `client_main.cc`) built warning-free under GCC 13.5.0 with
  `-std=c++17 -Wall -Wextra -Werror`.
- Recipe deviation from the draft: `EXTRA_OECMAKE =
  "-DEGRPC_BUILD_TESTS=OFF -DEGRPC_BUILD_EXAMPLES=ON"` replaces
  `-DBUILD_TESTING=OFF` (egrpc's CMake options are `EGRPC_BUILD_TESTS` /
  `EGRPC_BUILD_EXAMPLES`). No egrpc code or CMake changes were required.
- Runtime note for M8: egrpc and `libegrpc.so` have no Abseil dependency, but
  scarthgap's protobuf 4.25.8 depends on abseil-cpp 20240116 (`.so.2401.0.0`):
  `libprotobuf-lite.so.25.8.0` NEEDs 10 `libabsl_*` libraries, and
  `egrpc_route_guide_client` links 2 directly via protobuf gencode/headers.
  Target images that use generated stubs must ship abseil-cpp.

Open item (M4 CI wiring, extended to M8): the pinned `grpc_cpp_plugin`
(built on protobuf 35.0) must accept the CodeGeneratorRequest produced by
whichever protoc drives it — 21.12 on CI, 25.x inside the Yocto recipe.
Verify per driver (still untested for 25.x: `tools/bin/grpc_cpp_plugin` was
not built during the 2026-11-24 spike, which used the checked-in v1.82.0 stub
copies; deferred to M8); if a driver's request is rejected, drive the plugin from
the pinned protoc 35.0 instead (the plugin emits no message code, so the
runtime match constraint does not apply to its output — only the `.pb.*`
generation must use the runtime-matched protoc).

## Policy (design §4.5)

- **One `grpc_cpp_plugin` version is pinned per egrpc release.** Never regenerate
  stubs with an unpinned tool.
- From **M4** onward, CI regenerates the `grpcpp/` shim's stubs with the pinned
  plugin and **diffs them against the checked-in copies**, then compiles them, so
  any drift in the generated-code contract is caught immediately.
- Shim support for a new plugin version must be **additive** — the README states
  the supported plugin-version range explicitly.

## How to bump the pin

1. Pick the new gRPC release (and the protobuf/protoc version it pins, plus the
   Abseil version that protobuf pins in its `MODULE.bazel`).
2. Update the versions **and** all four SHA-256 checksums in the header of
   `tools/pin-codegen.sh` (the `*_VERSION`, `*_URL`, `*_SHA256` variables).
3. Update this document (the table above and the version in the heading) and the
   "Pinned codegen tools" section of `CLAUDE.md`.
4. Re-run `tools/pin-codegen.sh --force` and confirm the smoke test passes.
5. Re-check the `grpcpp/` shim against the new plugin's output (the M4 CI diff);
   make any required shim changes additively.
