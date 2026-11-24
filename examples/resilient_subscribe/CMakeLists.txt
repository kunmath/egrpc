# egrpc M7 example: reconcile-on-reconnect subscription via
# egrpc::resilient_stream, built against the checked-in, UNMODIFIED
# route_guide stubs from examples/route_guide/gen/. Also drives
# tests/test_resilient_stream.py.
set(_rg_gen ${PROJECT_SOURCE_DIR}/examples/route_guide/gen)

add_executable(egrpc_resilient_subscribe
  main.cc
  ${_rg_gen}/route_guide.pb.cc
  ${_rg_gen}/route_guide.grpc.pb.cc
)

# Generated sources are not ours: exempt them from -Wall -Wextra -Werror.
set_source_files_properties(
  ${_rg_gen}/route_guide.pb.cc
  ${_rg_gen}/route_guide.grpc.pb.cc
  PROPERTIES COMPILE_OPTIONS "-w"
)

target_include_directories(egrpc_resilient_subscribe PRIVATE ${_rg_gen})
target_link_libraries(egrpc_resilient_subscribe PRIVATE egrpc_static egrpc_warnings)
