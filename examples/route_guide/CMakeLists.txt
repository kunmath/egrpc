# egrpc M4 acceptance example (design §8): the vendored route_guide client
# built against the checked-in, UNMODIFIED grpc_cpp_plugin v1.82.0 stubs in
# gen/. Compiling gen/route_guide.grpc.pb.cc against the grpcpp shim is the
# milestone's compile-level acceptance check.
add_executable(egrpc_route_guide_client
  client_main.cc
  gen/route_guide.pb.cc
  gen/route_guide.grpc.pb.cc
)

# Generated sources are not ours: exempt them from -Wall -Wextra -Werror
# (the vendored client_main.cc keeps the full warning set).
set_source_files_properties(
  gen/route_guide.pb.cc
  gen/route_guide.grpc.pb.cc
  PROPERTIES COMPILE_OPTIONS "-w"
)

target_include_directories(egrpc_route_guide_client PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/gen)
target_link_libraries(egrpc_route_guide_client PRIVATE egrpc_static egrpc_warnings)
