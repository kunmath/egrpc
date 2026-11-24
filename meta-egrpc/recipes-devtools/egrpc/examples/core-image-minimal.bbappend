# Example only: copy into your distro/product layer (e.g.
# meta-product/recipes-core/images/core-image-minimal.bbappend). It is not
# matched by meta-egrpc's BBFILES, so it never alters images on its own.
IMAGE_INSTALL:append = " egrpc"

# Optional: the route_guide / resilient_subscribe example clients
# (requires PACKAGECONFIG:pn-egrpc = "examples").
# IMAGE_INSTALL:append = " egrpc-examples"

# Applications using generated protobuf stubs need the protobuf-lite runtime.
# On scarthgap, protobuf 4.25.x itself depends on abseil-cpp, which is pulled
# in automatically as a runtime dependency. libegrpc.so has zero Abseil linkage.
# IMAGE_INSTALL:append = " protobuf-lite"
