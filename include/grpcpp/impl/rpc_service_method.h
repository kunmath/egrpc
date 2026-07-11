// egrpc grpcpp shim — grpc::internal::MethodHandler / RpcServiceMethod
// (design §4.5). Server-side plumbing the generated Service constructor
// odr-uses: handlers are stored (owned) but never run — egrpc is
// client-only (design §2).
#pragma once

#include <grpcpp/impl/rpc_method.h>

#include <memory>

namespace grpc {
namespace internal {

class MethodHandler {
 public:
  virtual ~MethodHandler() = default;
};

class RpcServiceMethod : public RpcMethod {
 public:
  // Takes ownership of `handler`, matching upstream.
  RpcServiceMethod(const char* name, RpcMethod::RpcType type, MethodHandler* handler)
      : RpcMethod(name, type), handler_(handler) {}

  void SetHandler(MethodHandler* handler) { handler_.reset(handler); }
  MethodHandler* handler() const { return handler_.get(); }

 private:
  std::unique_ptr<MethodHandler> handler_;
};

}  // namespace internal
}  // namespace grpc
