// egrpc grpcpp shim — grpc::ServerContext / grpc::CallbackServerContext
// (design §4.5): signature parity only. egrpc is client-only (design §2);
// the generated server classes name these types but no server ever runs.
#pragma once

namespace grpc {

class ServerContext {
 public:
  virtual ~ServerContext() = default;
};

class CallbackServerContext {
 public:
  virtual ~CallbackServerContext() = default;
};

}  // namespace grpc
