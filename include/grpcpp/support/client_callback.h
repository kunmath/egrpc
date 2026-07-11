// egrpc grpcpp shim — client callback (reactor) API (design §4.5):
// compile-present only, UNIMPLEMENTED at runtime. The generated async stub
// methods are real code in the .grpc.pb.cc, so these must link.
//
// Runtime contract of the stubs: every started operation completes
// unsuccessfully (On*Done(false), delivered synchronously from the Start*
// call), and OnDone(UNIMPLEMENTED) fires exactly once, only after
// StartCall() AND after every hold is removed — so the upstream reactor
// ordering (operation callbacks and hold removal before OnDone) is
// preserved for correctly-written reactors. Everything is delivered on the
// caller's thread; there is no executor.
#pragma once

#include <grpcpp/client_context.h>
#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/impl/rpc_method.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <atomic>
#include <functional>

namespace grpc {

namespace internal {
inline Status CallbackUnimplementedStatus() {
  return Status(StatusCode::UNIMPLEMENTED, "callback API not supported by egrpc");
}

// Hold/once bookkeeping shared by the streaming reactor stubs. The derived
// reactor calls MaybeDone() after any event that could make it deliverable;
// the exchange makes delivery exactly-once under concurrent callers.
class ReactorStubState {
 protected:
  void MarkStarted() { started_.store(true, std::memory_order_release); }
  void Hold(int holds) { holds_.fetch_add(holds, std::memory_order_acq_rel); }
  void Unhold() { holds_.fetch_sub(1, std::memory_order_acq_rel); }
  // True exactly once, when started and no holds remain.
  bool ConsumeDeliverDone() {
    return started_.load(std::memory_order_acquire) &&
           holds_.load(std::memory_order_acquire) <= 0 &&
           !done_.exchange(true, std::memory_order_acq_rel);
  }

 private:
  std::atomic<bool> started_{false};
  std::atomic<bool> done_{false};
  std::atomic<int> holds_{0};
};
}  // namespace internal

class ClientUnaryReactor {
 public:
  virtual ~ClientUnaryReactor() = default;
  void StartCall() { OnDone(internal::CallbackUnimplementedStatus()); }
  virtual void OnReadInitialMetadataDone(bool ok) { (void)ok; }
  virtual void OnDone(const Status& s) { (void)s; }  // default no-op, matching upstream
};

template <class R>
class ClientReadReactor : private internal::ReactorStubState {
 public:
  virtual ~ClientReadReactor() = default;
  void StartCall() {
    this->MarkStarted();
    MaybeDone();
  }
  void StartRead(R* msg) {
    (void)msg;
    OnReadDone(false);
  }
  void AddHold() { this->Hold(1); }
  void AddMultipleHolds(int holds) { this->Hold(holds); }
  void RemoveHold() {
    this->Unhold();
    MaybeDone();
  }
  virtual void OnReadInitialMetadataDone(bool ok) { (void)ok; }
  virtual void OnReadDone(bool ok) { (void)ok; }
  virtual void OnDone(const Status& s) { (void)s; }  // default no-op, matching upstream

 private:
  void MaybeDone() {
    if (this->ConsumeDeliverDone()) OnDone(internal::CallbackUnimplementedStatus());
  }
};

template <class W>
class ClientWriteReactor : private internal::ReactorStubState {
 public:
  virtual ~ClientWriteReactor() = default;
  void StartCall() {
    this->MarkStarted();
    MaybeDone();
  }
  void StartWrite(const W* msg) { StartWrite(msg, WriteOptions()); }
  void StartWrite(const W* msg, WriteOptions options) {
    (void)msg;
    (void)options;
    OnWriteDone(false);
  }
  void StartWriteLast(const W* msg, WriteOptions options) {
    StartWrite(msg, options);
    StartWritesDone();
  }
  void StartWritesDone() { OnWritesDoneDone(false); }
  void AddHold() { this->Hold(1); }
  void AddMultipleHolds(int holds) { this->Hold(holds); }
  void RemoveHold() {
    this->Unhold();
    MaybeDone();
  }
  virtual void OnReadInitialMetadataDone(bool ok) { (void)ok; }
  virtual void OnWriteDone(bool ok) { (void)ok; }
  virtual void OnWritesDoneDone(bool ok) { (void)ok; }
  virtual void OnDone(const Status& s) { (void)s; }  // default no-op, matching upstream

 private:
  void MaybeDone() {
    if (this->ConsumeDeliverDone()) OnDone(internal::CallbackUnimplementedStatus());
  }
};

template <class W, class R>
class ClientBidiReactor : private internal::ReactorStubState {
 public:
  virtual ~ClientBidiReactor() = default;
  void StartCall() {
    this->MarkStarted();
    MaybeDone();
  }
  void StartRead(R* msg) {
    (void)msg;
    OnReadDone(false);
  }
  void StartWrite(const W* msg) { StartWrite(msg, WriteOptions()); }
  void StartWrite(const W* msg, WriteOptions options) {
    (void)msg;
    (void)options;
    OnWriteDone(false);
  }
  void StartWriteLast(const W* msg, WriteOptions options) {
    StartWrite(msg, options);
    StartWritesDone();
  }
  void StartWritesDone() { OnWritesDoneDone(false); }
  void AddHold() { this->Hold(1); }
  void AddMultipleHolds(int holds) { this->Hold(holds); }
  void RemoveHold() {
    this->Unhold();
    MaybeDone();
  }
  virtual void OnReadInitialMetadataDone(bool ok) { (void)ok; }
  virtual void OnReadDone(bool ok) { (void)ok; }
  virtual void OnWriteDone(bool ok) { (void)ok; }
  virtual void OnWritesDoneDone(bool ok) { (void)ok; }
  virtual void OnDone(const Status& s) { (void)s; }  // default no-op, matching upstream

 private:
  void MaybeDone() {
    if (this->ConsumeDeliverDone()) OnDone(internal::CallbackUnimplementedStatus());
  }
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
// delivers OnDone(UNIMPLEMENTED) (after holds), so user code terminates.
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
