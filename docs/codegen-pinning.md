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

Policy for message codegen: checked-in `.pb.h/.cc` files (e.g.
`examples/route_guide/gen/`) are generated with the **official prebuilt protoc
matching the minimum supported system runtime** — for v0.1 that is
**protoc 21.12** (== protobuf C++ 3.21.12,
`protoc-21.12-linux-x86_64.zip`, SHA-256
`3a4c1e5f2516c639d3079b1586e703fc7bcfa2136d58bda24d1d54f949c315e8`).
Newer runtimes accept older gencode within protobuf's documented
compatibility window, so these files also build on newer distros; targets
whose runtime falls outside that window (e.g. a future Yocto LTS) regenerate
from the `.proto` with their own matching protoc.

Open item for M4: the CI stub-diff will drive the pinned `grpc_cpp_plugin`
from the host protoc; the plugin (built on protobuf 35.0) must accept the
CodeGeneratorRequest a 21.12 protoc produces — verify this when wiring the
M4 CI job, and if it does not hold, drive it from the pinned protoc 35.0
instead (the plugin does not emit message code, so the runtime constraint
does not apply to its output).

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
