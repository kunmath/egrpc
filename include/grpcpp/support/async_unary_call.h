// egrpc grpcpp shim — async unary types (design §4.5): compile-present
// only. Async completion-queue calls are permanently out of scope (design
// §2); operations perform no I/O, but tagged ones deliver their tag through
// the bound CompletionQueue (Finish with ok=true after depositing an
// UNIMPLEMENTED status, per upstream's "client-side Finish is always ok"),
// so a well-formed async caller observes completion and terminates.
#pragma once

#include <grpcpp/client_context.h>
#include <grpcpp/completion_queue.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/impl/rpc_method.h>
#include <grpcpp/support/byte_buffer.h>
#include <grpcpp/support/status.h>

namespace grpc {

template <class R>
class ClientAsyncResponseReaderInterface {
 public:
  virtual ~ClientAsyncResponseReaderInterface() = default;
  virtual void StartCall() = 0;
  virtual void ReadInitialMetadata(void* tag) = 0;
  virtual void Finish(R* msg, Status* status, void* tag) = 0;
};

namespace internal {
class ClientAsyncResponseReaderHelper;
}  // namespace internal

template <class R>
class ClientAsyncResponseReader final : public ClientAsyncResponseReaderInterface<R> {
 public:
  void StartCall() override {}
  void ReadInitialMetadata(void* tag) override {
    if (cq_ != nullptr) cq_->EgrpcEnqueueTag(tag, false);
  }
  void Finish(R* msg, Status* status, void* tag) override {
    (void)msg;
    if (status != nullptr) {
      *status = Status(StatusCode::UNIMPLEMENTED, "async API not supported by egrpc");
    }
    if (cq_ != nullptr) cq_->EgrpcEnqueueTag(tag, true);
  }

 private:
  friend class internal::ClientAsyncResponseReaderHelper;
  explicit ClientAsyncResponseReader(CompletionQueue* cq) : cq_(cq) {}

  CompletionQueue* const cq_;
};

namespace internal {

class ClientAsyncResponseReaderHelper {
 public:
  template <class R, class W, class BaseR = R, class BaseW = W>
  static ClientAsyncResponseReader<R>* Create(ChannelInterface* channel, CompletionQueue* cq,
                                              const RpcMethod& method, grpc::ClientContext* context,
                                              const W& request) {
    (void)channel;
    (void)method;
    (void)context;
    (void)request;
    return new ClientAsyncResponseReader<R>(cq);
  }
};

}  // namespace internal

// Server-side: signature parity only (design §2: client-only).
template <class W>
class ServerAsyncResponseWriter {
 public:
  ServerAsyncResponseWriter() = default;
};

}  // namespace grpc
