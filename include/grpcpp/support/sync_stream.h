// egrpc grpcpp shim — synchronous streaming surface (design §4.5, §5.8).
//
// ClientReader<R> is the v0.1 server-streaming API, backed by an opaque
// byte-level ClientStreamRaw (implemented in src/api/channel.cc) so this
// public header never leaks core types. Client-streaming and bidi types
// (ClientWriter, ClientReaderWriter) are permanently compile-present-only,
// as are the server-side types — egrpc has no server (design §2).
#pragma once

#include <grpcpp/client_context.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/impl/proto_utils.h>
#include <grpcpp/impl/rpc_method.h>
#include <grpcpp/support/status.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

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

// Opaque byte-level server-streaming driver (implemented in src/api/channel.cc).
class ClientStreamRaw {
 public:
  virtual ~ClientStreamRaw() = default;
  virtual void WaitForInitialMetadata() = 0;
  virtual bool ReadRaw(std::string* response_bytes) = 0;
  virtual bool NextMessageSize(uint32_t* sz) = 0;
  virtual void CancelWithError(Status status) = 0;
  virtual Status Finish() = 0;
};

std::unique_ptr<ClientStreamRaw> StartServerStreamRaw(ChannelInterface* channel,
                                                      const RpcMethod& method,
                                                      ClientContext* context,
                                                      const std::string& request);

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

// Server-streaming reader implementing the §5.8 lifecycle contract:
//   - WaitForInitialMetadata(): legal before the first Read(); idempotent.
//   - Read(): single-reader (asserted out-of-line in debug); after the first
//     false every subsequent Read() returns false immediately.
//   - Finish(): at most once (asserted out-of-line in debug). Calling Finish()
//     without draining cancels-and-discards so flow control cannot deadlock.
//   - ~ClientReader(): if Finish() was not called, performs non-blocking
//     cancel-and-discard.
// All debug assertions live out-of-line in ClientStreamRawImpl so inline
// methods here remain ODR-identical across Debug and Release TUs.
template <class R>
class ClientReader final : public ClientReaderInterface<R> {
 public:
  void WaitForInitialMetadata() override {
    if (raw_) raw_->WaitForInitialMetadata();
  }

  bool Read(R* msg) override {
    if (!raw_) return false;
    std::string bytes;
    if (!raw_->ReadRaw(&bytes)) return false;
    Status parse = SerializationTraits<R>::Deserialize(bytes, msg);
    if (!parse.ok()) {
      raw_->CancelWithError(std::move(parse));
      return false;
    }
    return true;
  }

  bool NextMessageSize(uint32_t* sz) override {
    if (!raw_) return false;
    return raw_->NextMessageSize(sz);
  }

  Status Finish() override {
    if (!raw_) return lame_status_;
    return raw_->Finish();
  }

 private:
  template <class>
  friend class internal::ClientReaderFactory;

  explicit ClientReader(std::unique_ptr<internal::ClientStreamRaw> raw) : raw_(std::move(raw)) {}
  explicit ClientReader(Status lame_status) : lame_status_(std::move(lame_status)) {}

  std::unique_ptr<internal::ClientStreamRaw> raw_;
  Status lame_status_;
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
    std::string request_bytes;
    Status s = SerializationTraits<W>::Serialize(request, &request_bytes);
    if (!s.ok()) {
      ClientContextAccess::MarkStatusLocal(context);
      return new ClientReader<R>(std::move(s));
    }
    return new ClientReader<R>(StartServerStreamRaw(channel, method, context, request_bytes));
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
