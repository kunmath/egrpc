// egrpc grpcpp shim — grpc::ChannelInterface (design §4.5): the opaque base
// the generated stubs hold their channel through. The only implementation
// is grpc::Channel; the shim entry points (BlockingUnaryCall & friends)
// downcast to it.
#pragma once

namespace grpc {

class ChannelInterface {
 public:
  virtual ~ChannelInterface() = default;
};

}  // namespace grpc
