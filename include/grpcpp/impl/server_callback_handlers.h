// egrpc grpcpp shim — server callback method handlers (design §4.5):
// instantiated by the generated CallbackService constructors, never run
// (design §2: client-only). Functors are accepted and discarded, like
// support/method_handler.h; SetMessageAllocator must exist because the
// generated SetMessageAllocatorFor_* static_casts to this type and calls it.
#pragma once

#include <grpcpp/impl/rpc_service_method.h>
#include <grpcpp/support/message_allocator.h>

namespace grpc {
namespace internal {

template <class RequestType, class ResponseType>
class CallbackUnaryHandler : public MethodHandler {
 public:
  template <class F>
  explicit CallbackUnaryHandler(F get_reactor) {
    (void)get_reactor;
  }
  void SetMessageAllocator(MessageAllocator<RequestType, ResponseType>* allocator) {
    (void)allocator;
  }
};

template <class RequestType, class ResponseType>
class CallbackClientStreamingHandler : public MethodHandler {
 public:
  template <class F>
  explicit CallbackClientStreamingHandler(F get_reactor) {
    (void)get_reactor;
  }
};

template <class RequestType, class ResponseType>
class CallbackServerStreamingHandler : public MethodHandler {
 public:
  template <class F>
  explicit CallbackServerStreamingHandler(F get_reactor) {
    (void)get_reactor;
  }
};

template <class RequestType, class ResponseType>
class CallbackBidiHandler : public MethodHandler {
 public:
  template <class F>
  explicit CallbackBidiHandler(F get_reactor) {
    (void)get_reactor;
  }
};

}  // namespace internal
}  // namespace grpc
