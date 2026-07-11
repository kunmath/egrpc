// egrpc grpcpp shim — grpc::Service (design §4.5): the base the generated
// (sync/async/callback/raw/streamed) service classes derive from. It exists
// so that generated code compiles and its constructors run leak-free under
// ASan; no server ever serves from it (design §2: client-only). The
// RequestAsync* entry points are fully generic no-op templates — the
// generated With* wrappers name them with concrete arg lists, and deduction
// swallows whatever those are.
#pragma once

#include <grpcpp/impl/rpc_service_method.h>

#include <memory>
#include <vector>

namespace grpc {

class Service {
 public:
  Service() = default;
  virtual ~Service() = default;

  Service(const Service&) = delete;
  Service& operator=(const Service&) = delete;

  // Takes ownership, matching upstream (the generated ctor passes `new`).
  void AddMethod(internal::RpcServiceMethod* method) { methods_.emplace_back(method); }

  void MarkMethodAsync(int index) { (void)index; }
  void MarkMethodRaw(int index) { (void)index; }
  void MarkMethodGeneric(int index) { (void)index; }

  // The callback/streamed marks hand over a `new` handler; ownership must
  // land in the method entry (or be reclaimed on a bad index) so the
  // generated constructors stay leak-free.
  void MarkMethodCallback(int index, internal::MethodHandler* handler) {
    SetHandler(index, handler);
  }
  void MarkMethodRawCallback(int index, internal::MethodHandler* handler) {
    SetHandler(index, handler);
  }
  void MarkMethodStreamed(int index, internal::MethodHandler* handler) {
    SetHandler(index, handler);
  }

  internal::MethodHandler* GetHandler(int index) {
    if (index < 0 || static_cast<size_t>(index) >= methods_.size()) return nullptr;
    return methods_[static_cast<size_t>(index)]->handler();
  }

  template <class... Args>
  void RequestAsyncUnary(int index, Args... args) {
    (void)index;
    ((void)args, ...);
  }
  template <class... Args>
  void RequestAsyncClientStreaming(int index, Args... args) {
    (void)index;
    ((void)args, ...);
  }
  template <class... Args>
  void RequestAsyncServerStreaming(int index, Args... args) {
    (void)index;
    ((void)args, ...);
  }
  template <class... Args>
  void RequestAsyncBidiStreaming(int index, Args... args) {
    (void)index;
    ((void)args, ...);
  }

 private:
  void SetHandler(int index, internal::MethodHandler* handler) {
    if (index < 0 || static_cast<size_t>(index) >= methods_.size()) {
      delete handler;
      return;
    }
    methods_[static_cast<size_t>(index)]->SetHandler(handler);
  }

  std::vector<std::unique_ptr<internal::RpcServiceMethod>> methods_;
};

}  // namespace grpc
