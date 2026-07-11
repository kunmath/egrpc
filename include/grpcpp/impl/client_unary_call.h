// egrpc grpcpp shim — grpc::internal::BlockingUnaryCall (design §4.5): the
// unary entry point every generated sync stub method funnels into. The
// template only serializes/deserializes; the transport work happens behind
// BlockingUnaryCallRaw (src/api/channel.cc), which keeps this header free
// of core types and keeps plugin-version adaptation localized (design
// §4.5's "thin shim entry points").
#pragma once

#include <grpcpp/impl/proto_utils.h>
#include <grpcpp/support/status.h>

#include <string>
#include <type_traits>

namespace grpc {

class ChannelInterface;
class ClientContext;

namespace internal {

class RpcMethod;

// Byte-level blocking unary call against a grpc::Channel; `channel` must be
// one (the generated stub can only hold channels created by
// grpc::CreateChannel). Implemented in src/api/channel.cc.
Status BlockingUnaryCallRaw(ChannelInterface* channel, const RpcMethod& method,
                            ClientContext* context, const std::string& request,
                            std::string* response);

// Matches the four-template-arg form the pinned plugin (gRPC v1.82.0)
// generates: Base{Input,Output}Message select the SerializationTraits
// specialization — protobuf::MessageLite for proto services.
template <class InputMessage, class OutputMessage, class BaseInputMessage = InputMessage,
          class BaseOutputMessage = OutputMessage>
Status BlockingUnaryCall(ChannelInterface* channel, const RpcMethod& method, ClientContext* context,
                         const InputMessage& request, OutputMessage* result) {
  static_assert(std::is_base_of<BaseInputMessage, InputMessage>::value,
                "InputMessage must derive from BaseInputMessage");
  static_assert(std::is_base_of<BaseOutputMessage, OutputMessage>::value,
                "OutputMessage must derive from BaseOutputMessage");

  std::string request_bytes;
  Status s = SerializationTraits<BaseInputMessage>::Serialize(request, &request_bytes);
  if (!s.ok()) return s;

  std::string response_bytes;
  s = BlockingUnaryCallRaw(channel, method, context, request_bytes, &response_bytes);
  if (!s.ok()) return s;

  return SerializationTraits<BaseOutputMessage>::Deserialize(response_bytes, result);
}

}  // namespace internal
}  // namespace grpc
