// egrpc grpcpp shim — grpc::ByteBuffer (design §4.5): a name the generated
// raw-method templates require; egrpc's serialization is plain byte strings
// (impl/proto_utils.h), so this carries no payload machinery.
#pragma once

namespace grpc {

class ByteBuffer {
 public:
  ByteBuffer() = default;
};

}  // namespace grpc
