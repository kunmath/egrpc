# egrpc M8 interop subset (design §8): an upstream-style interop client built
# against the vendored gRPC v1.82.0 testing protos. The .pb.{h,cc} come from
# protoc 21.12 (matching the host libprotobuf-lite); test.grpc.pb.{h,cc} are the
# UNMODIFIED output of the pinned grpc_cpp_plugin v1.82.0.
add_library(interop_proto STATIC
  gen/src/proto/grpc/testing/empty.pb.cc
  gen/src/proto/grpc/testing/messages.pb.cc
  gen/src/proto/grpc/testing/test.pb.cc
)
# Generated sources are not ours: exempt them from -Wall -Wextra -Werror.
set_source_files_properties(
  gen/src/proto/grpc/testing/empty.pb.cc
  gen/src/proto/grpc/testing/messages.pb.cc
  gen/src/proto/grpc/testing/test.pb.cc
  gen/src/proto/grpc/testing/test.grpc.pb.cc
  PROPERTIES COMPILE_OPTIONS "-w"
)
target_include_directories(interop_proto PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/gen)
target_link_libraries(interop_proto PUBLIC PkgConfig::ProtobufLite)

add_executable(egrpc_interop_client
  interop_client_main.cc
  gen/src/proto/grpc/testing/test.grpc.pb.cc
)
target_link_libraries(egrpc_interop_client PRIVATE egrpc_static interop_proto egrpc_warnings)
