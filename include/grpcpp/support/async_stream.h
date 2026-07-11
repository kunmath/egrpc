// egrpc grpcpp shim — async streaming types (design §4.5): compile-present
// only, like async unary. Async completion-queue calls are permanently out
// of scope (design §2); started operations never complete because the
// CompletionQueue behaves as shut down, which async drain loops handle.
#pragma once

#include <grpcpp/client_context.h>
#include <grpcpp/completion_queue.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/impl/rpc_method.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

namespace grpc {

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
  virtual void WritesDone(void* tag) = 0;
};

namespace internal {

// Shared no-op behavior for the concrete client async stubs below: every
// operation is accepted and dropped; Finish reports UNIMPLEMENTED into
// *status for anyone who inspects it without waiting on the (dead) queue.
class AsyncStubOps {
 public:
  static void FillUnimplemented(Status* status) {
    if (status != nullptr) {
      *status = Status(StatusCode::UNIMPLEMENTED, "async API not supported by egrpc");
    }
  }
};

}  // namespace internal

template <class R>
class ClientAsyncReaderInterface : public ClientAsyncStreamingInterface,
                                   public AsyncReaderInterface<R> {};

namespace internal {
template <class R>
class ClientAsyncReaderFactory;
}  // namespace internal

template <class R>
class ClientAsyncReader final : public ClientAsyncReaderInterface<R> {
 public:
  void StartCall(void* tag) override { (void)tag; }
  void ReadInitialMetadata(void* tag) override { (void)tag; }
  void Read(R* msg, void* tag) override {
    (void)msg;
    (void)tag;
  }
  void Finish(Status* status, void* tag) override {
    (void)tag;
    internal::AsyncStubOps::FillUnimplemented(status);
  }

 private:
  template <class>
  friend class internal::ClientAsyncReaderFactory;
  ClientAsyncReader() = default;
};

template <class W>
class ClientAsyncWriterInterface : public ClientAsyncStreamingInterface,
                                   public AsyncWriterInterface<W> {};

namespace internal {
template <class W>
class ClientAsyncWriterFactory;
}  // namespace internal

template <class W>
class ClientAsyncWriter final : public ClientAsyncWriterInterface<W> {
 public:
  void StartCall(void* tag) override { (void)tag; }
  void ReadInitialMetadata(void* tag) override { (void)tag; }
  void Write(const W& msg, void* tag) override {
    (void)msg;
    (void)tag;
  }
  void Write(const W& msg, WriteOptions options, void* tag) override {
    (void)msg;
    (void)options;
    (void)tag;
  }
  void WritesDone(void* tag) override { (void)tag; }
  void Finish(Status* status, void* tag) override {
    (void)tag;
    internal::AsyncStubOps::FillUnimplemented(status);
  }

 private:
  template <class>
  friend class internal::ClientAsyncWriterFactory;
  ClientAsyncWriter() = default;
};

template <class W, class R>
class ClientAsyncReaderWriterInterface : public ClientAsyncStreamingInterface,
                                         public AsyncWriterInterface<W>,
                                         public AsyncReaderInterface<R> {};

namespace internal {
template <class W, class R>
class ClientAsyncReaderWriterFactory;
}  // namespace internal

template <class W, class R>
class ClientAsyncReaderWriter final : public ClientAsyncReaderWriterInterface<W, R> {
 public:
  void StartCall(void* tag) override { (void)tag; }
  void ReadInitialMetadata(void* tag) override { (void)tag; }
  void Read(R* msg, void* tag) override {
    (void)msg;
    (void)tag;
  }
  void Write(const W& msg, void* tag) override {
    (void)msg;
    (void)tag;
  }
  void Write(const W& msg, WriteOptions options, void* tag) override {
    (void)msg;
    (void)options;
    (void)tag;
  }
  void WritesDone(void* tag) override { (void)tag; }
  void Finish(Status* status, void* tag) override {
    (void)tag;
    internal::AsyncStubOps::FillUnimplemented(status);
  }

 private:
  template <class, class>
  friend class internal::ClientAsyncReaderWriterFactory;
  ClientAsyncReaderWriter() = default;
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
    (void)cq;
    (void)method;
    (void)context;
    (void)request;
    (void)start;
    (void)tag;
    return new ClientAsyncReader<R>();
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
    (void)cq;
    (void)method;
    (void)context;
    (void)response;
    (void)start;
    (void)tag;
    return new ClientAsyncWriter<W>();
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
    (void)cq;
    (void)method;
    (void)context;
    (void)start;
    (void)tag;
    return new ClientAsyncReaderWriter<W, R>();
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
