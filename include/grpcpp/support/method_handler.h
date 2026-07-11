// egrpc grpcpp shim — the sync/streamed method-handler templates the
// generated Service constructors instantiate (design §4.5). Server-side and
// never run (design §2: client-only), so the functors are accepted for
// signature compatibility and deliberately discarded — storing them would
// only keep dead std::function machinery alive in embedded builds.
#pragma once

#include <grpcpp/impl/rpc_service_method.h>
#include <grpcpp/support/byte_buffer.h>

namespace grpc {
namespace internal {

template <class ServiceType, class RequestType, class ResponseType,
          class BaseRequestType = RequestType, class BaseResponseType = ResponseType>
class RpcMethodHandler : public MethodHandler {
 public:
  template <class F>
  RpcMethodHandler(F func, ServiceType* service) {
    (void)func;
    (void)service;
  }
};

template <class ServiceType, class RequestType, class ResponseType>
class ClientStreamingHandler : public MethodHandler {
 public:
  template <class F>
  ClientStreamingHandler(F func, ServiceType* service) {
    (void)func;
    (void)service;
  }
};

template <class ServiceType, class RequestType, class ResponseType>
class ServerStreamingHandler : public MethodHandler {
 public:
  template <class F>
  ServerStreamingHandler(F func, ServiceType* service) {
    (void)func;
    (void)service;
  }
};

template <class Streamer, bool WriteNeeded>
class TemplatedBidiStreamingHandler : public MethodHandler {
 public:
  template <class F>
  explicit TemplatedBidiStreamingHandler(F func) {
    (void)func;
  }
};

template <class ServiceType, class RequestType, class ResponseType>
class BidiStreamingHandler : public MethodHandler {
 public:
  template <class F>
  BidiStreamingHandler(F func, ServiceType* service) {
    (void)func;
    (void)service;
  }
};

template <class RequestType, class ResponseType>
class StreamedUnaryHandler : public MethodHandler {
 public:
  template <class F>
  explicit StreamedUnaryHandler(F func) {
    (void)func;
  }
};

template <class RequestType, class ResponseType>
class SplitServerStreamingHandler : public MethodHandler {
 public:
  template <class F>
  explicit SplitServerStreamingHandler(F func) {
    (void)func;
  }
};

}  // namespace internal
}  // namespace grpc
