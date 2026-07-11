// egrpc grpcpp shim — synchronous streaming surface (design §4.5).
//
// ClientReader<R> is the v0.1 server-streaming API; until M5 lands the real
// entry point, its factory hands back a stub whose Read() returns false and
// Finish() returns UNIMPLEMENTED. Client-streaming and bidi types
// (ClientWriter, ClientReaderWriter) are permanently compile-present-only,
// as are the server-side types — egrpc has no server (design §2).
#pragma once

#include <grpcpp/client_context.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/impl/rpc_method.h>
#include <grpcpp/support/status.h>

#include <cstdint>

namespace grpc {

// Per-Write options. Accepted for signature parity; egrpc sends every
// message identically (identity encoding, no corking).
class WriteOptions {
 public:
  WriteOptions& set_last_message() {
    last_message_ = true;
    return *this;
  }
  WriteOptions& clear_last_message() {
    last_message_ = false;
    return *this;
  }
  bool is_last_message() const { return last_message_; }
  WriteOptions& set_write_through() { return *this; }

 private:
  bool last_message_ = false;
};

namespace internal {

class ClientStreamingInterface {
 public:
  virtual ~ClientStreamingInterface() = default;
  // Blocks until the stream is closed; returns the call's final status.
  virtual Status Finish() = 0;
};

template <class R>
class ReaderInterface {
 public:
  virtual ~ReaderInterface() = default;
  // Blocking read; false ⇒ no further messages (check Finish() for why).
  virtual bool Read(R* msg) = 0;
  virtual bool NextMessageSize(uint32_t* sz) = 0;
};

template <class W>
class WriterInterface {
 public:
  virtual ~WriterInterface() = default;
  virtual bool Write(const W& msg, WriteOptions options) = 0;
  bool Write(const W& msg) { return Write(msg, WriteOptions()); }
};

}  // namespace internal

template <class R>
class ClientReaderInterface : public internal::ClientStreamingInterface,
                              public internal::ReaderInterface<R> {
 public:
  virtual void WaitForInitialMetadata() = 0;
};

namespace internal {
template <class R>
class ClientReaderFactory;
}  // namespace internal

// TODO(M5): the real server-streaming reader over CallState per-Read
// delivery. This stub only satisfies the generated code's contract shape.
template <class R>
class ClientReader final : public ClientReaderInterface<R> {
 public:
  void WaitForInitialMetadata() override {}
  bool Read(R* msg) override {
    (void)msg;
    return false;
  }
  bool NextMessageSize(uint32_t* sz) override {
    (void)sz;
    return false;
  }
  Status Finish() override {
    return Status(StatusCode::UNIMPLEMENTED, "server streaming not implemented yet (egrpc M5)");
  }

 private:
  template <class>
  friend class internal::ClientReaderFactory;
  ClientReader() = default;
};

template <class W>
class ClientWriterInterface : public internal::ClientStreamingInterface,
                              public internal::WriterInterface<W> {
 public:
  virtual bool WritesDone() = 0;
};

namespace internal {
template <class W>
class ClientWriterFactory;
}  // namespace internal

// Client streaming is permanently out of scope (design §2): compile-present,
// UNIMPLEMENTED at runtime.
template <class W>
class ClientWriter final : public ClientWriterInterface<W> {
 public:
  using internal::WriterInterface<W>::Write;
  bool Write(const W& msg, WriteOptions options) override {
    (void)msg;
    (void)options;
    return false;
  }
  bool WritesDone() override { return false; }
  Status Finish() override {
    return Status(StatusCode::UNIMPLEMENTED, "client streaming not supported by egrpc");
  }

 private:
  template <class>
  friend class internal::ClientWriterFactory;
  ClientWriter() = default;
};

template <class W, class R>
class ClientReaderWriterInterface : public internal::ClientStreamingInterface,
                                    public internal::WriterInterface<W>,
                                    public internal::ReaderInterface<R> {
 public:
  virtual void WaitForInitialMetadata() = 0;
  virtual bool WritesDone() = 0;
};

namespace internal {
template <class W, class R>
class ClientReaderWriterFactory;
}  // namespace internal

// Bidi streaming is permanently out of scope (design §2): compile-present,
// UNIMPLEMENTED at runtime.
template <class W, class R>
class ClientReaderWriter final : public ClientReaderWriterInterface<W, R> {
 public:
  using internal::WriterInterface<W>::Write;
  void WaitForInitialMetadata() override {}
  bool Read(R* msg) override {
    (void)msg;
    return false;
  }
  bool NextMessageSize(uint32_t* sz) override {
    (void)sz;
    return false;
  }
  bool Write(const W& msg, WriteOptions options) override {
    (void)msg;
    (void)options;
    return false;
  }
  bool WritesDone() override { return false; }
  Status Finish() override {
    return Status(StatusCode::UNIMPLEMENTED, "bidi streaming not supported by egrpc");
  }

 private:
  template <class, class>
  friend class internal::ClientReaderWriterFactory;
  ClientReaderWriter() = default;
};

namespace internal {

template <class R>
class ClientReaderFactory {
 public:
  template <class W>
  static ClientReader<R>* Create(ChannelInterface* channel, const RpcMethod& method,
                                 grpc::ClientContext* context, const W& request) {
    (void)channel;
    (void)method;
    (void)context;
    (void)request;
    return new ClientReader<R>();
  }
};

template <class W>
class ClientWriterFactory {
 public:
  template <class R>
  static ClientWriter<W>* Create(ChannelInterface* channel, const RpcMethod& method,
                                 grpc::ClientContext* context, R* response) {
    (void)channel;
    (void)method;
    (void)context;
    (void)response;
    return new ClientWriter<W>();
  }
};

template <class W, class R>
class ClientReaderWriterFactory {
 public:
  static ClientReaderWriter<W, R>* Create(ChannelInterface* channel, const RpcMethod& method,
                                          grpc::ClientContext* context) {
    (void)channel;
    (void)method;
    (void)context;
    return new ClientReaderWriter<W, R>();
  }
};

}  // namespace internal

// --- Server-side sync types: signature parity only (no server in egrpc) ----

template <class R>
class ServerReader {
 public:
  bool Read(R* msg) {
    (void)msg;
    return false;
  }
};

template <class W>
class ServerWriter {
 public:
  bool Write(const W& msg, WriteOptions options = WriteOptions()) {
    (void)msg;
    (void)options;
    return false;
  }
};

template <class W, class R>
class ServerReaderWriter {
 public:
  bool Read(R* msg) {
    (void)msg;
    return false;
  }
  bool Write(const W& msg, WriteOptions options = WriteOptions()) {
    (void)msg;
    (void)options;
    return false;
  }
};

template <class RequestType, class ResponseType>
class ServerUnaryStreamer {
 public:
  bool Read(RequestType* msg) {
    (void)msg;
    return false;
  }
  bool Write(const ResponseType& msg, WriteOptions options = WriteOptions()) {
    (void)msg;
    (void)options;
    return false;
  }
};

template <class RequestType, class ResponseType>
class ServerSplitStreamer {
 public:
  bool Write(const ResponseType& msg, WriteOptions options = WriteOptions()) {
    (void)msg;
    (void)options;
    return false;
  }
};

}  // namespace grpc
