# meta-egrpc

Yocto layer for **egrpc**, a client-only gRPC C++ library for embedded Linux.

## Supported releases

**scarthgap (5.0 LTS) only.** `kirkstone` is not supported: its protobuf
(3.19) and GCC 11 differ from what egrpc's pinned `grpc_cpp_plugin` v1.82.0
stubs and the C++17 shim were verified against, and only one LTS is kept
under test for v0.1. Verified on scarthgap 5.0.21, `MACHINE = "qemuarm64"`.

## Dependencies

- `poky` (`meta`) — layer collection `core`
- `meta-openembedded/meta-oe` — layer collection `openembedded-layer`

```
bitbake-layers add-layer /path/to/meta-openembedded/meta-oe
bitbake-layers add-layer /path/to/egrpc/meta-egrpc
```

## Recipes

| Recipe | Kind | Provides |
|---|---|---|
| `egrpc` (`egrpc_git.bb`) | target | `libegrpc.so.0`, `libegrpc.a`, headers, `egrpc.pc` |
| `egrpc-codegen-native` (`egrpc-codegen-native_1.82.0.bb`) | host only | `${bindir}/egrpc-grpc_cpp_plugin` (pinned gRPC v1.82.0) |

```
bitbake egrpc
bitbake egrpc-codegen-native
```

`egrpc_git.bb` defaults to upstream `main` with `AUTOREV`; pin it in your
distro, e.g. `SRCREV:pn-egrpc = "<sha>"`, or point `EGRPC_SRC_URI` at a mirror.
Packages follow Debian library renaming (`libegrpc0`, `libegrpc-dev`,
`libegrpc-staticdev`).

`PACKAGECONFIG:pn-egrpc = "examples"` additionally builds the route_guide and
resilient_subscribe example clients (package `egrpc-examples`, renamed
`libegrpc-examples`), regenerating their stubs in-recipe as described below.

## Generating stubs in application recipes

Codegen tools are **host-side `-native` only** and are never cross-compiled to
the target:

- `.pb.{h,cc}` — use `protobuf-native`'s `protoc` (25.8 on scarthgap). It must
  match the target `protobuf` runtime.
- `.grpc.pb.{h,cc}` — use `egrpc-codegen-native`'s pinned
  `egrpc-grpc_cpp_plugin`. It is named so it never collides with meta-oe's
  `grpc-native`. protoc 25.8 drives it cleanly (verified).

```
DEPENDS += "egrpc protobuf protobuf-native egrpc-codegen-native"

do_configure:prepend() {
    install -d ${S}/gen
    ${STAGING_BINDIR_NATIVE}/protoc --proto_path=${S}/proto \
        --cpp_out=${S}/gen \
        --plugin=protoc-gen-grpc=${STAGING_BINDIR_NATIVE}/egrpc-grpc_cpp_plugin \
        --grpc_out=${S}/gen \
        ${S}/proto/my_service.proto
}
```

Link the app with `pkg-config egrpc` (or `-legrpc`) plus `protobuf-lite`.

## Adding egrpc to an image

`recipes-devtools/egrpc/examples/core-image-minimal.bbappend` is an example
only (meta-egrpc's `BBFILES` does not match it). Copy it into your
distro/product layer, e.g. `meta-product/recipes-core/images/`.

## Abseil note

`libegrpc.so` has **zero** Abseil linkage (NEEDs only libssl, libcrypto,
libnghttp2 and the C/C++ runtime). However scarthgap's `protobuf-lite` 4.25.8
itself depends on `abseil-cpp` (20240116), and code generated from `.proto`
files links some `libabsl_*` libraries directly. Images running apps with
generated protobuf stubs therefore ship `abseil-cpp`; it is pulled in
automatically via shlib runtime dependencies.
