// egrpc grpcpp shim — async streaming types (design §4.5): compile-present
// only, like async unary. Operations perform no I/O; every tagged op
// delivers its tag through the bound CompletionQueue — Finish with ok=true
// after depositing an UNIMPLEMENTED status (upstream: client-side Finish is
// always ok), everything else with ok=false (upstream's failed-operation
// signal) — so async drain loops observe completion and terminate.
#pragma once

#include <grpcpp/client_context.h>
#include <grpcpp/completion_queue.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/impl/rpc_method.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

namespace grpc {
namespace internal {

// The shared async-streaming interfaces live in grpc::internal, exactly as
// in upstream v1.82 — user code names them qualified.
class ClientAsyncStreamingInterface {
 public:
  virtual ~ClientAsyncStreamingInterface() = default;
  virtual void StartCall(void* tag) = 0;
  virtual void ReadInitialMetadata(void* tag) = 0;
  virtual void Finish(Status* status, void* tag) = 0;
};

template <class R>
class AsyncReaderInterface {
 public:
  virtual ~AsyncReaderInterface() = default;
  virtual void Read(R* msg, void* tag) = 0;
};

template <class W>
class AsyncWriterInterface {
 public:
  virtual ~AsyncWriterInterface() = default;
  virtual void Write(const W& msg, void* tag) = 0;
  virtual void Write(const W& msg, WriteOptions options, void* tag) = 0;
  virtual void WriteLast(const W& msg, WriteOptions options, void* tag) = 0;
  virtual void WritesDone(void* tag) = 0;
};

// Shared behavior of the concrete client async stubs below.
class AsyncClientStubBase {
 protected:
  explicit AsyncClientStubBase(grpc::CompletionQueue* cq) : cq_(cq) {}

  // Any op that would have needed the wire: tag back with ok=false.
  void EnqueueFailedOp(void* tag) {
    if (cq_ != nullptr) cq_->EgrpcEnqueueTag(tag, false);
  }
  // Client-side Finish: deposit UNIMPLEMENTED, tag back with ok=true.
  void EnqueueFinish(Status* status, void* tag) {
    if (status != nullptr) {
      *status = Status(StatusCode::UNIMPLEMENTED, "async API not supported by egrpc");
    }
    if (cq_ != nullptr) cq_->EgrpcEnqueueTag(tag, true);
  }

 private:
  grpc::CompletionQueue* const cq_;
};

}  // namespace internal

template <class R>
class ClientAsyncReaderInterface : public internal::ClientAsyncStreamingInterface,
                                   public internal::AsyncReaderInterface<R> {};

namespace internal {
template <class R>
class ClientAsyncReaderFactory;
}  // namespace internal

template <class R>
class ClientAsyncReader final : public ClientAsyncReaderInterface<R>,
                                private internal::AsyncClientStubBase {
 public:
  void StartCall(void* tag) override { this->EnqueueFailedOp(tag); }
  void ReadInitialMetadata(void* tag) override { this->EnqueueFailedOp(tag); }
  void Read(R* msg, void* tag) override {
    (void)msg;
    this->EnqueueFailedOp(tag);
  }
  void Finish(Status* status, void* tag) override { this->EnqueueFinish(status, tag); }

 private:
  template <class>
  friend class internal::ClientAsyncReaderFactory;
  explicit ClientAsyncReader(grpc::CompletionQueue* cq) : AsyncClientStubBase(cq) {}
};

template <class W>
class ClientAsyncWriterInterface : public internal::ClientAsyncStreamingInterface,
                                   public internal::AsyncWriterInterface<W> {};

namespace internal {
template <class W>
class ClientAsyncWriterFactory;
}  // namespace internal

template <class W>
class ClientAsyncWriter final : public ClientAsyncWriterInterface<W>,
                                private internal::AsyncClientStubBase {
 public:
  void StartCall(void* tag) override { this->EnqueueFailedOp(tag); }
  void ReadInitialMetadata(void* tag) override { this->EnqueueFailedOp(tag); }
  void Write(const W& msg, void* tag) override {
    (void)msg;
    this->EnqueueFailedOp(tag);
  }
  void Write(const W& msg, WriteOptions options, void* tag) override {
    (void)msg;
    (void)options;
    this->EnqueueFailedOp(tag);
  }
  void WriteLast(const W& msg, WriteOptions options, void* tag) override {
    (void)msg;
    (void)options;
    this->EnqueueFailedOp(tag);
  }
  void WritesDone(void* tag) override { this->EnqueueFailedOp(tag); }
  void Finish(Status* status, void* tag) override { this->EnqueueFinish(status, tag); }

 private:
  template <class>
  friend class internal::ClientAsyncWriterFactory;
  explicit ClientAsyncWriter(grpc::CompletionQueue* cq) : AsyncClientStubBase(cq) {}
};

template <class W, class R>
class ClientAsyncReaderWriterInterface : public internal::ClientAsyncStreamingInterface,
                                         public internal::AsyncWriterInterface<W>,
                                         public internal::AsyncReaderInterface<R> {};

namespace internal {
template <class W, class R>
class ClientAsyncReaderWriterFactory;
}  // namespace internal

template <class W, class R>
class ClientAsyncReaderWriter final : public ClientAsyncReaderWriterInterface<W, R>,
                                      private internal::AsyncClientStubBase {
 public:
  void StartCall(void* tag) override { this->EnqueueFailedOp(tag); }
  void ReadInitialMetadata(void* tag) override { this->EnqueueFailedOp(tag); }
  void Read(R* msg, void* tag) override {
    (void)msg;
    this->EnqueueFailedOp(tag);
  }
  void Write(const W& msg, void* tag) override {
    (void)msg;
    this->EnqueueFailedOp(tag);
  }
  void Write(const W& msg, WriteOptions options, void* tag) override {
    (void)msg;
    (void)options;
    this->EnqueueFailedOp(tag);
  }
  void WriteLast(const W& msg, WriteOptions options, void* tag) override {
    (void)msg;
    (void)options;
    this->EnqueueFailedOp(tag);
  }
  void WritesDone(void* tag) override { this->EnqueueFailedOp(tag); }
  void Finish(Status* status, void* tag) override { this->EnqueueFinish(status, tag); }

 private:
  template <class, class>
  friend class internal::ClientAsyncReaderWriterFactory;
  explicit ClientAsyncReaderWriter(grpc::CompletionQueue* cq) : AsyncClientStubBase(cq) {}
};

namespace internal {

template <class R>
class ClientAsyncReaderFactory {
 public:
  template <class W>
  static ClientAsyncReader<R>* Create(ChannelInterface* channel, grpc::CompletionQueue* cq,
                                      const RpcMethod& method, grpc::ClientContext* context,
                                      const W& request, bool start, void* tag) {
    (void)channel;
    (void)method;
    (void)context;
    (void)request;
    auto* reader = new ClientAsyncReader<R>(cq);
    if (start) reader->StartCall(tag);
    return reader;
  }
};

template <class W>
class ClientAsyncWriterFactory {
 public:
  template <class R>
  static ClientAsyncWriter<W>* Create(ChannelInterface* channel, grpc::CompletionQueue* cq,
                                      const RpcMethod& method, grpc::ClientContext* context,
                                      R* response, bool start, void* tag) {
    (void)channel;
    (void)method;
    (void)context;
    (void)response;
    auto* writer = new ClientAsyncWriter<W>(cq);
    if (start) writer->StartCall(tag);
    return writer;
  }
};

template <class W, class R>
class ClientAsyncReaderWriterFactory {
 public:
  static ClientAsyncReaderWriter<W, R>* Create(ChannelInterface* channel, grpc::CompletionQueue* cq,
                                               const RpcMethod& method,
                                               grpc::ClientContext* context, bool start,
                                               void* tag) {
    (void)channel;
    (void)method;
    (void)context;
    auto* stream = new ClientAsyncReaderWriter<W, R>(cq);
    if (start) stream->StartCall(tag);
    return stream;
  }
};

}  // namespace internal

// --- Server-side async types: signature parity only (design §2) ------------

template <class W, class R>
class ServerAsyncReader {
 public:
  ServerAsyncReader() = default;
};

template <class W>
class ServerAsyncWriter {
 public:
  ServerAsyncWriter() = default;
};

template <class W, class R>
class ServerAsyncReaderWriter {
 public:
  ServerAsyncReaderWriter() = default;
};

}  // namespace grpc
