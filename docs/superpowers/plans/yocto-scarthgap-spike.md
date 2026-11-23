# Yocto Scarthgap Feasibility Spike — Implementation Plan

**Spec / Authority:**
- `docs/yocto-spike.md` (Pre-M5 Yocto feasibility spike procedure, deliverables, and 4 known-unknowns)
- `docs/codegen-pinning.md` (Per-runtime message codegen matrix & pinned `grpc_cpp_plugin` v1.82.0 policy)
- `docs/egrpc-design-and-plan.md` (§8, §8a)
- `CLAUDE.md` (Coding standards, threading invariants, build quickstart)

**Goal:** Empirically execute `bitbake egrpc-spike` on Yocto **scarthgap** (`poky` + `meta-openembedded/meta-oe`) targeting `qemuarm64` (aarch64 cross-compilation), validating all 4 known-unknowns from `docs/yocto-spike.md`, fixing any real cross-build / protobuf-4.25.x / GCC-13 issues in egrpc while keeping desktop CI green, and recording the concrete empirical results in `docs/codegen-pinning.md` and `docs/yocto-spike.md`.

## Global Constraints

1. **Keep the git worktree clean of throwaway Yocto artifacts:** All `poky`, `meta-openembedded`, `meta-egrpc-spike`, `egrpc-mirror.git`, downloads, sstate-cache, and bitbake build directories MUST live under `/tmp/opencode/yocto-scarthgap/`. Do NOT commit throwaway `meta-egrpc-spike/`, poky checkouts, or bitbake build artifacts to git.
2. **Conflict avoidance:** Do NOT modify any file under `include/egrpc/*` (a parallel session is implementing M7).
3. **Host cache safety:** Do NOT mutate `/home/kunal/yocto-rpi-cache`. If mounted into Docker for optional read-only mirror reuse (`SSTATE_MIRRORS` / `PREMIRRORS`), it MUST be mounted read-only (`:ro`).
4. **Codegen pinning policy:** Never regenerate `.grpc.pb.*` stubs with an unpinned tool. Keep `examples/route_guide/gen/route_guide.grpc.pb.*` unmodified (pinned `grpc_cpp_plugin` v1.82.0). Never dirty `examples/route_guide/gen/route_guide.pb.*` in the git worktree — message gencode regeneration via `protobuf-native` happens strictly inside bitbake's unpacked `${S}` during `do_configure:prepend()`.
5. **Containerized Yocto host:** Execute bitbake inside the pre-installed `numaview-yocto:scarthgap` Docker image (Ubuntu 24.04.5 LTS with all Yocto scarthgap host packages pre-installed, UID/GID 1000 matching host `kunal`). Do not invoke interactive `sudo`.
6. **Genuine cross-compilation target:** Build with `MACHINE = "qemuarm64"` (x86_64 host → aarch64 target, GCC 13 `aarch64-poky-linux-g++`) so host vs. target sysroot separation is empirically verified.
7. **egrpc coding & CI standards:** Any code or CMake fix in this repo MUST adhere to C++17, `-Wall -Wextra -Werror`, system deps only (nghttp2, OpenSSL, protobuf-lite — no Abseil in egrpc source, no vendoring), pass host Release build + `ctest --output-on-failure`, pass `clang-format-18 --dry-run --Werror` on tracked C/C++ files (excluding `third_party/` and `examples/*/gen/`), and pass sanitized tests (ASan+UBSan and TSan) if any C++ source or header is modified.
8. **All 4 known-unknowns must be settled with concrete evidence** (commands + observed versions/outputs):
   1. `protobuf-lite` 4.25.x accepts in-recipe 25.x gencode for `route_guide.proto` (`LITE_RUNTIME`).
   2. egrpc's CMake finds scarthgap's `nghttp2`, `OpenSSL`, and `protobuf-lite` via pkg-config/`find_package` in a cross environment without host leakage.
   3. Unmodified checked-in stub gencode (`examples/route_guide/gen/route_guide.grpc.pb.*`, plugin v1.82.0) compiles cleanly against protobuf 4.25.x headers via `include/grpcpp/`.
   4. C++17 + `-Wall -Wextra -Werror` passes under scarthgap's GCC 13 cross-toolchain for `libegrpc` (static & shared) and `egrpc_route_guide_client`.

---

## Task 1: Set up isolated Yocto Scarthgap workspace, throwaway `meta-egrpc-spike` layer, and verify bitbake parse

**Repo commits expected:** None (all workspace files live outside the repo under `/tmp/opencode/yocto-scarthgap/`). The git worktree MUST remain clean.

### Requirements

1. Create `/tmp/opencode/yocto-scarthgap/` with subdirectories `downloads/`, `sstate-cache/`, `meta-egrpc-spike/conf/`, and `meta-egrpc-spike/recipes-spike/egrpc/`.
2. Clone official upstream Yocto **scarthgap** layers into `/tmp/opencode/yocto-scarthgap/`:
   - `poky` branch `scarthgap` (`https://git.yoctoproject.org/poky`, `--branch scarthgap --single-branch --depth 50`) into `/tmp/opencode/yocto-scarthgap/poky`.
   - `meta-openembedded` branch `scarthgap` (`https://github.com/openembedded/meta-openembedded.git`, `--branch scarthgap --single-branch --depth 50`) into `/tmp/opencode/yocto-scarthgap/meta-openembedded`.
   - Record exact commit SHAs (`git rev-parse HEAD`), commit dates/subjects, and `DISTRO_VERSION` from `poky/meta-poky/conf/distro/poky.conf`.
3. Create a bare git mirror of the current egrpc worktree HEAD at `/tmp/opencode/yocto-scarthgap/egrpc-mirror.git` (branch `yocto-scarthgap-spike`):
   - `git clone --bare --branch yocto-scarthgap-spike <worktree-path> /tmp/opencode/yocto-scarthgap/egrpc-mirror.git`
   - Record the exact HEAD SHA (`SRCREV`).
4. Author the throwaway spike layer under `/tmp/opencode/yocto-scarthgap/meta-egrpc-spike/`:
   - `conf/layer.conf`:
     ```bitbake
     BBPATH .= ":${LAYERDIR}"
     BBFILES += "${LAYERDIR}/recipes-spike/*/*.bb"
     BBFILE_COLLECTIONS += "egrpc-spike"
     BBFILE_PATTERN_egrpc-spike = "^${LAYERDIR}/"
     LAYERSERIES_COMPAT_egrpc-spike = "scarthgap"
     ```
   - `recipes-spike/egrpc/egrpc-spike_0.1.bb`:
     - `SUMMARY = "egrpc pre-M5 feasibility spike (design §8a) — throwaway"`
     - `LICENSE = "CLOSED"`
     - `SRC_URI = "git:///work/egrpc-mirror.git;protocol=file;branch=yocto-scarthgap-spike"`
     - `SRCREV = "<exact-HEAD-SHA>"`
     - `S = "${WORKDIR}/git"`
     - `DEPENDS = "nghttp2 openssl protobuf protobuf-native"`
     - `inherit cmake pkgconfig`
     - `do_configure:prepend()` removes `${S}/examples/route_guide/gen/route_guide.pb.*` and regenerates them using `protoc` from `protobuf-native`:
       ```bitbake
       do_configure:prepend() {
           rm -f ${S}/examples/route_guide/gen/route_guide.pb.*
           protoc --proto_path=${S}/examples/route_guide \
                  --cpp_out=${S}/examples/route_guide/gen \
                  ${S}/examples/route_guide/route_guide.proto
       }
       ```
     - `EXTRA_OECMAKE = "-DEGRPC_BUILD_TESTS=OFF -DEGRPC_BUILD_EXAMPLES=ON"` (note: `CMakeLists.txt` uses `EGRPC_BUILD_TESTS`, not `BUILD_TESTING`).
5. Initialize the BitBake build directory `/tmp/opencode/yocto-scarthgap/build` inside `numaview-yocto:scarthgap` Docker container:
   - Mount `/tmp/opencode/yocto-scarthgap:/work` (rw) and `/home/kunal/yocto-rpi-cache:/ro-cache:ro` (strictly read-only).
   - Run `source /work/poky/oe-init-build-env /work/build` and add layers `/work/meta-openembedded/meta-oe` and `/work/meta-egrpc-spike`.
   - Configure `/work/build/conf/local.conf` with:
     - `MACHINE = "qemuarm64"`
     - `DL_DIR = "/work/downloads"`
     - `SSTATE_DIR = "/work/sstate-cache"`
     - `PREMIRRORS:prepend` pointing at `file:///ro-cache/downloads/` (read-only fallback mirror)
     - `SSTATE_MIRRORS ?= "file://.* file:///ro-cache/sstate-cache/PATH"` (read-only hash-verified sstate reuse)
     - `BB_NUMBER_THREADS` / `PARALLEL_MAKE` left at default (16 cores available) or bounded sensibly (`-j 12` / `BB_NUMBER_THREADS = "12"` to avoid OOM while compiling GCC/protobuf on 31 GB RAM).
6. Verify parsing inside the container:
   - `bitbake-layers show-layers` lists `meta`, `meta-poky`, `meta-yocto-bsp`, `meta-oe`, and `meta-egrpc-spike` cleanly.
   - `bitbake-layers show-recipes egrpc-spike protobuf nghttp2 openssl` resolves exact versions for all recipes without warnings/errors.
   - `bitbake -e egrpc-spike` parses cleanly and extract `TARGET_SYS`, `MACHINE`, `PV`, `DEPENDS`, `EXTRA_OECMAKE`, `GCCVERSION` (or `bitbake -e gcc-cross-aarch64` for `PV`).
7. Save a concise environment summary to `/tmp/opencode/yocto-scarthgap/spike-env-info.txt` (poky SHA, `DISTRO_VERSION`, meta-openembedded SHA, recipe `PV`s for `protobuf`, `protobuf-native`, `nghttp2`, `openssl`, `gcc-cross-aarch64`, `MACHINE`, `TARGET_SYS`, and the exact `meta-egrpc-spike` files).
8. Verify `git status --short` in the egrpc worktree shows zero modified or untracked files.

### Acceptance Check
- `docker run --rm -v /tmp/opencode/yocto-scarthgap:/work -v /home/kunal/yocto-rpi-cache:/ro-cache:ro numaview-yocto:scarthgap bash -lc 'source /work/poky/oe-init-build-env /work/build >/dev/null && bitbake-layers show-layers && bitbake-layers show-recipes egrpc-spike protobuf nghttp2 openssl'` exits 0 with all layers and recipes resolved.
- `/tmp/opencode/yocto-scarthgap/spike-env-info.txt` exists and records all SHAs and resolved versions.
- `git status --short` in the egrpc worktree is completely clean.

---

## Task 2: Execute `bitbake egrpc-spike` end-to-end on Scarthgap (`qemuarm64`), settle all 4 known-unknowns, and fix any cross-build issues

**Repo commits expected:** Zero if egrpc builds cleanly out of the box on scarthgap; one or more clean, minimal commits (plus desktop verification) if real cross-build, CMake, `grpcpp/` shim, or GCC-13 `-Werror` issues are discovered in egrpc.

### Requirements

1. Run `bitbake egrpc-spike` inside `numaview-yocto:scarthgap` (mounting `/tmp/opencode/yocto-scarthgap:/work` and `/home/kunal/yocto-rpi-cache:/ro-cache:ro`), capturing logs cleanly (e.g., tee to `/tmp/opencode/yocto-scarthgap/bitbake-egrpc-spike.log`).
2. Diagnose and resolve any failures carefully:
   - **If a recipe-only issue occurs** (e.g. missing QA/FILES adjustment or `protoc` invocation detail in `egrpc-spike_0.1.bb`): fix `egrpc-spike_0.1.bb`, record the exact deviation from `docs/yocto-spike.md`, and re-run `bitbake egrpc-spike`.
   - **If a real issue surfaces in egrpc** (e.g. `CMakeLists.txt`, `cmake/FindNghttp2.cmake`, `include/grpcpp/*`, `src/*`, `examples/route_guide/*` failing under protobuf 4.25.x headers, pkg-config cross sysroot, or GCC 13 `-Wall -Wextra -Werror`):
     - Fix it cleanly in the egrpc worktree following `CLAUDE.md` standards (C++17, `-Wall -Wextra -Werror`, no Abseil in egrpc source, no vendoring, **NEVER modify `include/egrpc/*`**).
     - Run `clang-format-18 --dry-run --Werror` on modified C/C++ files.
     - Run host desktop build + `ctest --output-on-failure` (in `/tmp/opencode/egrpc-host-build`, outside the repo). If C++ code changed, also run ASan+UBSan and TSan unit tests (`-DEGRPC_SANITIZE=address,undefined` and `-DEGRPC_SANITIZE=thread`).
     - Commit the fix cleanly in the egrpc worktree.
     - Update `/tmp/opencode/yocto-scarthgap/egrpc-mirror.git` (`git push --force /tmp/opencode/yocto-scarthgap/egrpc-mirror.git HEAD:refs/heads/yocto-scarthgap-spike`) and update `SRCREV` in `/tmp/opencode/yocto-scarthgap/meta-egrpc-spike/recipes-spike/egrpc/egrpc-spike_0.1.bb`.
     - Run `bitbake -c cleansstate egrpc-spike && bitbake egrpc-spike` and confirm it completes cleanly end-to-end.
3. Once `bitbake egrpc-spike` succeeds cleanly, collect concrete empirical evidence for all 4 known-unknowns by inspecting bitbake's build directory (`${B}`), sysroot, logs (`log.do_configure`, `log.do_compile`, `log.do_install`, `log.do_package`), and produced binaries:
   - **Known-unknown 1 (`protobuf-lite` 4.25.x + in-recipe 25.x `LITE_RUNTIME` gencode):**
     - Record exact `protoc --version` from `protobuf-native` sysroot used during `do_configure`.
     - Verify generated `${S}/examples/route_guide/gen/route_guide.pb.h` contains `PROTOC_VERSION` matching 4.25.x (e.g. `4025003`) and compiles + links cleanly against target `libprotobuf-lite`.
   - **Known-unknown 2 (CMake cross-discovery of `nghttp2`, `OpenSSL`, `protobuf-lite`):**
     - Extract CMake configure output from `log.do_configure` showing resolved paths for `OpenSSL`, `Nghttp2`, and `ProtobufLite` — confirm all resolve inside `recipe-sysroot` (no `/usr/include` or `/usr/lib` host paths).
   - **Known-unknown 3 (Unmodified v1.82.0 `.grpc.pb.*` stubs vs. protobuf 4.25.x headers via `include/grpcpp/`):**
     - Confirm `route_guide.grpc.pb.cc` was NOT regenerated/modified (matches repo SHA-256) and compiled cleanly against scarthgap's protobuf 4.25.x headers and `include/grpcpp/`.
   - **Known-unknown 4 (C++17 + `-Wall -Wextra -Werror` under scarthgap GCC 13 cross-toolchain):**
     - Record exact `aarch64-poky-linux-g++ --version` from the recipe sysroot-native.
     - Inspect `log.do_compile` confirming all egrpc sources and `examples/route_guide/client_main.cc` compiled with `-Wall -Wextra -Werror` and `-std=c++17` with zero warnings/errors.
   - **ELF artifact verification:**
     - Run `file` and `aarch64-poky-linux-readelf -d` (or `readelf -h` / `readelf -d`) on built `libegrpc.so.0.1.0`, `libegrpc.a`, and `egrpc_route_guide_client` inside `${B}` (and packaged output under `packages-split/`) to confirm genuine `ARM aarch64` ELF binaries linked against `libprotobuf-lite`, `libnghttp2`, `libssl`, `libcrypto` (plus `libabsl_*` transitive deps from protobuf 4.25.x if present — note this explicitly!).
     - Record stripped/unstripped sizes of `libegrpc.so.0.1.0` and `egrpc_route_guide_client` as a useful empirical data point.
   - **Bonus check (if `tools/bin/grpc_cpp_plugin` is present, or explicitly note if not built):** note whether the open item in `docs/codegen-pinning.md` (driving pinned `grpc_cpp_plugin` with 25.x `protoc`) was tested or deferred to M8. (Wait: do NOT build `tools/bin/grpc_cpp_plugin` from scratch unless quick; if `tools/bin/grpc_cpp_plugin` does not exist, simply note that stub regeneration via `protoc` 25.x → pinned `grpc_cpp_plugin` remains the M8 open item, matching `docs/yocto-spike.md` line 16 "checked-in stub copies for the spike".)
4. Write all concrete empirical results (exact SHAs, resolved versions, configure/compile/ELF verification snippets, recipe deviations, and any code fixes) to `/tmp/opencode/yocto-scarthgap/spike-results.md`.

### Acceptance Check
- `bitbake egrpc-spike` exits 0 with `NOTE: Tasks Summary: ... all succeeded`.
- All 4 known-unknowns verified with concrete command outputs in `/tmp/opencode/yocto-scarthgap/spike-results.md`.
- `file` / `readelf` confirm genuine `aarch64` ELF outputs for `libegrpc.so.0.1.0`, `libegrpc.a`, and `egrpc_route_guide_client`.
- If any repo files were modified: committed cleanly, `include/egrpc/*` untouched, host Release build + `ctest` + sanitizer tests + `clang-format-18` all pass.
- No untracked/throwaway files left in the git worktree.

---

## Task 3: Record empirical results in `docs/codegen-pinning.md` and `docs/yocto-spike.md`, and run final desktop verification

**Repo commits expected:** 1 clean commit updating `docs/codegen-pinning.md` and `docs/yocto-spike.md`.

### Requirements

1. Read `/tmp/opencode/yocto-scarthgap/spike-env-info.txt` and `/tmp/opencode/yocto-scarthgap/spike-results.md` from Tasks 1–2. Never invent or guess any version, SHA, size, or command output — every recorded fact MUST come directly from those empirical outputs.
2. Update `docs/codegen-pinning.md`:
   - In the "Message codegen vs. stub codegen" section, replace the placeholder language (`"This was validated analytically against scarthgap's recipe and protobuf's version guards (2026-07); the pre-M5 bitbake spike (design §8a) confirms it empirically."`) with the concrete empirical result:
     - Scarthgap release (`DISTRO_VERSION`), exact `poky` and `meta-openembedded` commit SHAs, target `MACHINE` / arch (`qemuarm64` / `aarch64-poky-linux`).
     - Exact resolved recipe versions for `protobuf` / `protobuf-native`, `nghttp2`, `openssl`, and GCC cross-toolchain (`gcc-cross-aarch64`).
     - Build outcome (`bitbake egrpc-spike` succeeded end-to-end; `libegrpc` static/shared and `egrpc_route_guide_client` compiled and linked as `aarch64` ELF binaries).
     - Any recipe deviations from `docs/yocto-spike.md` (e.g. `-DEGRPC_BUILD_TESTS=OFF -DEGRPC_BUILD_EXAMPLES=ON` instead of `-DBUILD_TESTING=OFF`, any `do_configure` / CMake / code adjustments needed).
     - Update the table row for Yocto scarthgap if the exact `protobuf` 4.25.x version resolved differs from `4.25.3` (or confirm `4.25.x` exact version).
     - Preserve the open item note about driving pinned `grpc_cpp_plugin` from 25.x `protoc` (accurate to what was / wasn't tested).
3. Update `docs/yocto-spike.md`:
   - Update the Status line at the top from `"procedure ready, not yet executed"` to `"PASSED (executed <date> on Yocto scarthgap)"` with a concise summary.
   - Update the recipe snippet if any adjustment was needed (e.g. `EXTRA_OECMAKE = "-DEGRPC_BUILD_TESTS=OFF -DEGRPC_BUILD_EXAMPLES=ON"` and any other deviation discovered in Task 2).
   - Add an **Empirical Results** section recording:
     - Build environment (Yocto release, `poky` SHA, `meta-openembedded` SHA, `MACHINE`/`TARGET_SYS`, build container).
     - Resolved component versions table (`protobuf`, `protobuf-native` / `protoc`, `nghttp2`, `openssl`, `gcc-cross-aarch64`).
     - Outcome for each of the 4 known-unknowns (with concrete evidence: `protoc` version, CMake sysroot resolution, unmodified `.grpc.pb.*` compile check, GCC 13 `-Wall -Wextra -Werror` check, `aarch64` ELF linkage & notable transitive deps such as `libabsl_*` pulled via `protobuf-lite.pc`).
     - Any egrpc code/CMake fixes required (or "None required" if clean out of the box).
4. Run full desktop verification on the final repo state (building out-of-tree in `/tmp/opencode/egrpc-host-verify` so the repo stays clean):
   - `cmake -S . -B /tmp/opencode/egrpc-host-verify -DCMAKE_BUILD_TYPE=Release && cmake --build /tmp/opencode/egrpc-host-verify -j && ctest --test-dir /tmp/opencode/egrpc-host-verify --output-on-failure`
   - `clang-format-18 --dry-run --Werror` on all tracked `.h`/`.cc` files excluding `third_party/` and `examples/*/gen/` (matching `.github/workflows/ci.yml`).
   - Verify `git diff --name-only <merge-base>..HEAD` contains NO files under `include/egrpc/`, NO `meta-egrpc-spike/`, NO poky/bitbake artifacts, and `git status --short` is clean after committing.
5. Commit the documentation updates with a clear commit message (e.g., `docs: record empirical Yocto scarthgap spike results`).

### Acceptance Check
- `docs/codegen-pinning.md` and `docs/yocto-spike.md` accurately reflect all empirical observations from `/tmp/opencode/yocto-scarthgap/spike-results.md`.
- Host Release build + `ctest --output-on-failure` pass.
- `clang-format-18 --dry-run --Werror` passes with exit code 0.
- `git status --short` is clean; zero edits to `include/egrpc/*`; zero throwaway spike artifacts in git.
