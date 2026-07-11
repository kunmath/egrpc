# M1 integration probe: driven by the pytest harness (tests/test_tls.py),
# not registered with ctest — it needs a live server.

add_executable(egrpc_tls_connect tls_connect_main.cc)
target_include_directories(egrpc_tls_connect PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(egrpc_tls_connect PRIVATE egrpc_static egrpc_warnings)

# M2 probe: session bring-up + keepalive, driven by tests/test_keepalive.py.
add_executable(egrpc_h2_keepalive h2_keepalive_main.cc)
target_include_directories(egrpc_h2_keepalive PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(egrpc_h2_keepalive PRIVATE egrpc_static egrpc_warnings)

# route_guide protobuf-lite messages (checked-in gencode; see
# docs/codegen-pinning.md "Message codegen vs. stub codegen"). Separate
# target: generated code is not held to egrpc_warnings.
add_library(route_guide_proto STATIC
  ${PROJECT_SOURCE_DIR}/examples/route_guide/gen/route_guide.pb.cc)
target_include_directories(route_guide_proto PUBLIC
  ${PROJECT_SOURCE_DIR}/examples/route_guide/gen)
target_link_libraries(route_guide_proto PUBLIC PkgConfig::ProtobufLite)

# M3 probe: hand-rolled unary call via the temporary internal API, driven by
# tests/test_unary.py.
add_executable(egrpc_unary_call unary_call_main.cc)
target_include_directories(egrpc_unary_call PRIVATE ${PROJECT_SOURCE_DIR}/src)
target_link_libraries(egrpc_unary_call PRIVATE egrpc_static route_guide_proto egrpc_warnings)
