// egrpc grpcpp shim — client callback (reactor) API (design §4.5):
// compile-present only, UNIMPLEMENTED at runtime. The generated async stub
// methods are real code in the .grpc.pb.cc, so these must link; behavior:
// the function-callback form completes synchronously with UNIMPLEMENTED,
// and reactors deliver OnDone(UNIMPLEMENTED) from StartCall() — nothing
// blocks forever, nothing leaks.
#pragma once

#include <grpcpp/client_context.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/impl/rpc_method.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <functional>

namespace grpc {

namespace internal {
inline Status CallbackUnimplementedStatus() {
  return Status(StatusCode::UNIMPLEMENTED, "callback API not supported by egrpc");
}
}  // namespace internal

class ClientUnaryReactor {
 public:
  virtual ~ClientUnaryReactor() = default;
  void StartCall() { OnDone(internal::CallbackUnimplementedStatus()); }
  virtual void OnReadInitialMetadataDone(bool ok) { (void)ok; }
  virtual void OnDone(const Status& s) { (void)s; }  // default no-op, matching upstream
};

template <class R>
class ClientReadReactor {
 public:
  virtual ~ClientReadReactor() = default;
  void StartCall() { OnDone(internal::CallbackUnimplementedStatus()); }
  void StartRead(R* msg) { (void)msg; }
  void AddHold() {}
  void AddMultipleHolds(int holds) { (void)holds; }
  void RemoveHold() {}
  virtual void OnReadInitialMetadataDone(bool ok) { (void)ok; }
  virtual void OnReadDone(bool ok) { (void)ok; }
  virtual void OnDone(const Status& s) { (void)s; }  // default no-op, matching upstream
};

template <class W>
class ClientWriteReactor {
 public:
  virtual ~ClientWriteReactor() = default;
  void StartCall() { OnDone(internal::CallbackUnimplementedStatus()); }
  void StartWrite(const W* msg) { (void)msg; }
  void StartWrite(const W* msg, WriteOptions options) {
    (void)msg;
    (void)options;
  }
  void StartWriteLast(const W* msg, WriteOptions options) {
    (void)msg;
    (void)options;
  }
  void StartWritesDone() {}
  void AddHold() {}
  void AddMultipleHolds(int holds) { (void)holds; }
  void RemoveHold() {}
  virtual void OnReadInitialMetadataDone(bool ok) { (void)ok; }
  virtual void OnWriteDone(bool ok) { (void)ok; }
  virtual void OnWritesDoneDone(bool ok) { (void)ok; }
  virtual void OnDone(const Status& s) { (void)s; }  // default no-op, matching upstream
};

template <class W, class R>
class ClientBidiReactor {
 public:
  virtual ~ClientBidiReactor() = default;
  void StartCall() { OnDone(internal::CallbackUnimplementedStatus()); }
  void StartRead(R* msg) { (void)msg; }
  void StartWrite(const W* msg) { (void)msg; }
  void StartWrite(const W* msg, WriteOptions options) {
    (void)msg;
    (void)options;
  }
  void StartWriteLast(const W* msg, WriteOptions options) {
    (void)msg;
    (void)options;
  }
  void StartWritesDone() {}
  void AddHold() {}
  void AddMultipleHolds(int holds) { (void)holds; }
  void RemoveHold() {}
  virtual void OnReadInitialMetadataDone(bool ok) { (void)ok; }
  virtual void OnReadDone(bool ok) { (void)ok; }
  virtual void OnWriteDone(bool ok) { (void)ok; }
  virtual void OnWritesDoneDone(bool ok) { (void)ok; }
  virtual void OnDone(const Status& s) { (void)s; }  // default no-op, matching upstream
};

namespace internal {

// The generated async::Method(..., std::function<void(Status)>) form:
// complete synchronously with UNIMPLEMENTED.
template <class InputMessage, class OutputMessage, class BaseInputMessage = InputMessage,
          class BaseOutputMessage = OutputMessage>
void CallbackUnaryCall(ChannelInterface* channel, const RpcMethod& method,
                       grpc::ClientContext* context, const InputMessage* request,
                       OutputMessage* result, std::function<void(Status)> on_completion) {
  (void)channel;
  (void)method;
  (void)context;
  (void)request;
  (void)result;
  on_completion(CallbackUnimplementedStatus());
}

// Reactor factories: binding is a no-op — the reactor's own StartCall()
// delivers OnDone(UNIMPLEMENTED), so user code still terminates.
class ClientCallbackUnaryFactory {
 public:
  template <class Request, class Response, class BaseRequest = Request,
            class BaseResponse = Response>
  static void Create(ChannelInterface* channel, const RpcMethod& method,
                     grpc::ClientContext* context, const Request* request, Response* response,
                     ClientUnaryReactor* reactor) {
    (void)channel;
    (void)method;
    (void)context;
    (void)request;
    (void)response;
    (void)reactor;
  }
};

template <class Response>
class ClientCallbackReaderFactory {
 public:
  template <class Request>
  static void Create(ChannelInterface* channel, const RpcMethod& method,
                     grpc::ClientContext* context, const Request* request,
                     ClientReadReactor<Response>* reactor) {
    (void)channel;
    (void)method;
    (void)context;
    (void)request;
    (void)reactor;
  }
};

template <class Request>
class ClientCallbackWriterFactory {
 public:
  template <class Response>
  static void Create(ChannelInterface* channel, const RpcMethod& method,
                     grpc::ClientContext* context, Response* response,
                     ClientWriteReactor<Request>* reactor) {
    (void)channel;
    (void)method;
    (void)context;
    (void)response;
    (void)reactor;
  }
};

template <class Request, class Response>
class ClientCallbackReaderWriterFactory {
 public:
  static void Create(ChannelInterface* channel, const RpcMethod& method,
                     grpc::ClientContext* context, ClientBidiReactor<Request, Response>* reactor) {
    (void)channel;
    (void)method;
    (void)context;
    (void)reactor;
  }
};

}  // namespace internal
}  // namespace grpc
