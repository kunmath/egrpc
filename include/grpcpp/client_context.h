// egrpc grpcpp shim — grpc::ClientContext (design §4.5, §5.7): deadline,
// initial metadata, wait_for_ready, thread-safe TryCancel, and post-call
// access to server metadata. Metadata storage is only written on the caller's
// thread (never the EventThread); TryCancel() is safe from any thread and
// works both before the RPC starts and mid-flight.
#pragma once

#include <grpcpp/support/string_ref.h>

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>

namespace grpc {

class Channel;
namespace internal {
class ClientStreamRawImpl;
class ClientContextAccess;
}  // namespace internal

class ClientContext {
 public:
  ClientContext() = default;

  ClientContext(const ClientContext&) = delete;
  ClientContext& operator=(const ClientContext&) = delete;

  // Absolute deadline; enforced locally by the channel's EventThread timer
  // (including while queued) and encoded as the grpc-timeout header (§5.1,
  // §5.6). An already-expired deadline fails immediately with
  // DEADLINE_EXCEEDED before anything is queued.
  void set_deadline(const std::chrono::system_clock::time_point& deadline) { deadline_ = deadline; }
  std::chrono::system_clock::time_point deadline() const { return deadline_; }

  // Initial metadata sent with the call, added before the RPC starts. Keys
  // are lowercased and `-bin` values base64-encoded by the core on the way
  // to the wire (§5.1), so both are passed raw here.
  void AddMetadata(const std::string& key, const std::string& value) {
    send_initial_metadata_.emplace(key, value);
  }

  // Wait-for-ready (design §4.4): when true, the call queues through
  // TRANSIENT_FAILURE / backoff / reconnect (and a GOAWAY drain) instead of
  // failing fast with UNAVAILABLE; it still honors the deadline and
  // TryCancel(). Default false (fail-fast), matching upstream.
  void set_wait_for_ready(bool wait_for_ready) { wait_for_ready_ = wait_for_ready; }
  bool wait_for_ready() const { return wait_for_ready_; }

  // Requests cancellation of this RPC (design §5.6, §5.7). Thread-safe and
  // idempotent; legal before the call begins or concurrently from another
  // thread while the call is in flight. Copying the shared_ptr hook under
  // cancel_mu_ is allocation-free and noexcept.
  void TryCancel() {
    std::shared_ptr<std::function<void()>> fn;
    {
      std::lock_guard<std::mutex> lock(cancel_mu_);
      if (cancel_requested_) return;
      cancel_requested_ = true;
      fn = cancel_fn_;
    }
    if (fn && *fn) (*fn)();
  }

  // Server metadata: initial metadata is valid after WaitForInitialMetadata(),
  // the first Read(), or Finish() / unary return; trailing metadata is valid
  // after Finish() / unary return. `-bin` values are base64-decoded. The
  // string_refs point into storage owned by this context and stay valid for
  // its lifetime.
  const std::multimap<grpc::string_ref, grpc::string_ref>& GetServerInitialMetadata() const {
    return recv_initial_view_;
  }
  const std::multimap<grpc::string_ref, grpc::string_ref>& GetServerTrailingMetadata() const {
    return recv_trailing_view_;
  }

  // --- egrpc internal (M6/M7) ----------------------------------------------
  // True when the completed call's terminal status originated locally
  // (TryCancel, local deadline, transport teardown, shutdown) rather than
  // from the server (trailers grpc-status, HTTP status, server RST_STREAM).
  bool egrpc_status_is_local() const { return egrpc_status_is_local_; }
  // egrpc extension: true once valid gRPC initial metadata (:status 200,
  // application/grpc content-type) was received for this call.
  bool egrpc_initial_metadata_received() const { return egrpc_initial_metadata_received_; }

 private:
  // grpc::Channel and ClientStreamRawImpl are the shim's call drivers: they
  // consume the send side, bind TryCancel, and deposit received metadata.
  friend class grpc::Channel;
  friend class grpc::internal::ClientStreamRawImpl;
  friend class grpc::internal::ClientContextAccess;

  // Binds a cancel action for an in-flight call. Returns true if TryCancel()
  // was already invoked on this context before the call started.
  bool BindCancel(std::function<void()> fn) {
    auto shared_fn = std::make_shared<std::function<void()>>(std::move(fn));
    std::lock_guard<std::mutex> lock(cancel_mu_);
    if (cancel_requested_) return true;
    cancel_fn_ = std::move(shared_fn);
    return false;
  }

  void UnbindCancel() {
    std::lock_guard<std::mutex> lock(cancel_mu_);
    cancel_fn_.reset();
  }

  std::chrono::system_clock::time_point deadline_ = std::chrono::system_clock::time_point::max();
  bool wait_for_ready_ = false;
  bool egrpc_status_is_local_ = false;
  bool egrpc_initial_metadata_received_ = false;
  std::multimap<std::string, std::string> send_initial_metadata_;

  // Received metadata storage + views into it. multimap nodes are stable,
  // so the views never dangle.
  std::multimap<std::string, std::string> recv_initial_metadata_;
  std::multimap<std::string, std::string> recv_trailing_metadata_;
  std::multimap<grpc::string_ref, grpc::string_ref> recv_initial_view_;
  std::multimap<grpc::string_ref, grpc::string_ref> recv_trailing_view_;

  // Leaf lock guarding TryCancel() across caller threads (§5.7). Never held
  // while invoking *cancel_fn_.
  std::mutex cancel_mu_;
  bool cancel_requested_ = false;
  std::shared_ptr<std::function<void()>> cancel_fn_;
};

namespace internal {

// Lets the shim's header templates (which serialize/deserialize messages
// before/after the byte-level call) flag their locally-generated failures.
class ClientContextAccess {
 public:
  static void MarkStatusLocal(ClientContext* context) {
    if (context != nullptr) context->egrpc_status_is_local_ = true;
  }
  static void MarkInitialMetadataReceived(ClientContext* context) {
    if (context != nullptr) context->egrpc_initial_metadata_received_ = true;
  }
};

}  // namespace internal
}  // namespace grpc
