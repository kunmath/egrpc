// egrpc grpcpp shim — serialization (design §4.5): the
// SerializationTraits-equivalent specialized for protobuf-lite MessageLite,
// plus the grpc::protobuf aliases the generated code names. Everything here
// is deliberately byte-string based — no ByteBuffer plumbing, no reflection
// — so protobuf-lite is sufficient (design §4.5: LITE_RUNTIME).
#pragma once

#include <google/protobuf/message_lite.h>
#include <grpcpp/support/status.h>

#include <string>
#include <type_traits>

namespace grpc {

namespace protobuf {
using MessageLite = ::google::protobuf::MessageLite;
}  // namespace protobuf

template <class T, class Enable = void>
class SerializationTraits;

template <class T>
class SerializationTraits<
    T, typename std::enable_if<std::is_base_of<protobuf::MessageLite, T>::value>::type> {
 public:
  static Status Serialize(const protobuf::MessageLite& msg, std::string* out) {
    if (!msg.SerializeToString(out)) {
      return Status(StatusCode::INTERNAL, "failed to serialize request message");
    }
    return Status();
  }

  static Status Deserialize(const std::string& bytes, protobuf::MessageLite* msg) {
    if (!msg->ParseFromString(bytes)) {
      return Status(StatusCode::INTERNAL, "failed to parse response message");
    }
    return Status();
  }
};

}  // namespace grpc
