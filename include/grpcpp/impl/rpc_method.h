// egrpc grpcpp shim — grpc::internal::RpcMethod (design §4.5): the method
// descriptor the generated stub constructs once per RPC. egrpc keeps only
// the fully-qualified path and the type; upstream's channel registration
// (a lookup-table optimization) has no equivalent here, so the channel
// argument of the 4-arg constructor is deliberately unused.
#pragma once

#include <memory>

namespace grpc {

class ChannelInterface;

namespace internal {

class RpcMethod {
 public:
  enum RpcType {
    NORMAL_RPC = 0,
    CLIENT_STREAMING,
    SERVER_STREAMING,
    BIDI_STREAMING,
  };

  RpcMethod(const char* name, RpcType type)
      : name_(name), suffix_for_stats_(nullptr), method_type_(type) {}

  RpcMethod(const char* name, const char* suffix_for_stats, RpcType type,
            const std::shared_ptr<ChannelInterface>& channel)
      : name_(name), suffix_for_stats_(suffix_for_stats), method_type_(type) {
    (void)channel;
  }

  const char* name() const { return name_; }
  const char* suffix_for_stats() const { return suffix_for_stats_; }
  RpcType method_type() const { return method_type_; }
  void SetMethodType(RpcType type) { method_type_ = type; }

 private:
  const char* const name_;
  const char* const suffix_for_stats_;
  RpcType method_type_;
};

}  // namespace internal
}  // namespace grpc
