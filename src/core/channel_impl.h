// egrpc — channel: owns the EventThread and the transport (design §4.4).
//
// M5 scope: unary and server-streaming calls, Read-driven HTTP/2 stream flow
// control (§5.4), per-call local deadline timers covering both queued and
// active calls (§5.6), thread-safe TryCancel via sticky token + RST_STREAM
// (§5.7), and immediate reset on framing violations. Reconnect/backoff lands
// in M6; until then transport failure is terminal for the channel.
//
// Threading (design §3): the EventThread exclusively owns the TlsSocket,
// the Http2Session, all timers, and every CallState object (unique_ptr).
// Caller threads interact only via EventThread::Post and CallCompletion
// (shared_ptr with zero transport references); Shutdown joins the loop.
#pragma once

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "core/call_state.h"
#include "core/event_thread.h"
#include "transport/http2_session.h"
#include "transport/tls_socket.h"

namespace egrpc {
namespace internal {

struct ChannelOptions {
  Http2SessionOptions http2;
  // Receive-path bound for the per-call reassembly scanner (design §4.3).
  size_t max_receive_message_size = 4 * 1024 * 1024;
  // Send-path bound, enforced before submission with kResourceExhausted.
  // Default unlimited, matching upstream; GRPC_ARG_MAX_SEND_MESSAGE_LENGTH
  // maps here (§4.5).
  size_t max_send_message_size = std::numeric_limits<size_t>::max();
  // Slack above max_receive_message_size + 5 B frame prefix used to derive
  // HTTP/2 SETTINGS_INITIAL_WINDOW_SIZE (§5.4; window is derived, never
  // independently smaller than max_receive_message_size + 5).
  size_t stream_window_slack = 64 * 1024;
  // Streaming delivery-queue bounds (§5.4). Whichever trips stops decoding
  // from the scanner and withholds stream WINDOW_UPDATE credit. An empty
  // queue always admits at least one legal message so Read() can make
  // progress even when max_queued_bytes < max_receive_message_size; the
  // effective byte ceiling is therefore max(max_queued_bytes,
  // max_receive_message_size + 5).
  size_t max_queued_messages = 16;
  // 0 defaults to the derived stream window (§5.4).
  size_t max_queued_bytes = 0;
  std::chrono::milliseconds connect_timeout{20000};
  // Prepended to the built-in user-agent (§5.1); upstream's
  // GRPC_ARG_PRIMARY_USER_AGENT maps here.
  std::string user_agent_prefix;
};

class ChannelImpl {
 public:
  struct CallHandle {
    uint64_t call_id = 0;
    std::shared_ptr<CallCompletion> completion;
  };

  // Does not connect; the transport comes up on the first call (design
  // §4.4: IDLE until a call needs the connection).
  ChannelImpl(std::string host, uint16_t port, TlsConfig tls, ChannelOptions options);
  ~ChannelImpl();  // Shutdown()s

  ChannelImpl(const ChannelImpl&) = delete;
  ChannelImpl& operator=(const ChannelImpl&) = delete;

  // Allocates a unique call id ahead of StartCall so callers (e.g.
  // ClientContext::TryCancel binding) can wire cancellation before the op is
  // posted. Safe from any thread.
  uint64_t AllocateCallId() { return next_call_id_.fetch_add(1, std::memory_order_relaxed); }

  // Starts a unary or server-streaming call and returns its CallHandle without
  // blocking. If `preallocated_id` is non-zero it is used as the call id; if
  // `prebound_completion` is non-null it is used as the CallCompletion block.
  // Safe from any thread.
  CallHandle StartCall(CallMode mode, const std::string& method_path, std::string request,
                       Http2Session::HeaderList metadata = {},
                       std::optional<std::chrono::steady_clock::time_point> deadline = std::nullopt,
                       uint64_t preallocated_id = 0,
                       std::shared_ptr<CallCompletion> prebound_completion = nullptr);

  // Blocking unary call, safe from any thread. Convenience wrapper over
  // StartCall(CallMode::kUnary, ...) + Wait().
  CallCompletion::Result UnaryCall(
      const std::string& method_path, std::string request, Http2Session::HeaderList metadata = {},
      std::optional<std::chrono::steady_clock::time_point> deadline = std::nullopt);

  // Requests cancellation of `call_id` (pending, active, or draining). Safe
  // from any thread; idempotent. Callers should also set the sticky cancel
  // token on CallCompletion before calling this so pre-admission races settle
  // deterministically (§5.7).
  void CancelCall(uint64_t call_id);
  // Allocation-free, noexcept cancellation path used by ClientContext::TryCancel()
  // and ~ClientStreamRawImpl(): sets the sticky token, links `completion` onto
  // an intrusive queue, and rings the eventfd without allocating.
  void CancelCall(const std::shared_ptr<CallCompletion>& completion) noexcept;

  // Notifies the EventThread that a streaming Read() popped a message of
  // `wire_bytes` (5 + payload.size()), allowing the scanner to drain further
  // messages and returning stream-level HTTP/2 flow-control credit (§5.4).
  // Safe from any thread.
  void NotifyReadConsumed(uint64_t call_id, size_t wire_bytes);

  // Derived HTTP/2 stream window (effective_max_receive_message_size + 5 + slack).
  uint32_t stream_window_size() const { return stream_window_; }
  // Effective receive message cap (clamped so stream_window_ >= cap + 5).
  size_t max_receive_message_size() const { return effective_max_receive_message_size_; }

  // Diagnostics/tests (§8/M5): internal high-water counters, readable from
  // any thread (atomics written on the EventThread).
  size_t max_active_calls() const { return max_active_calls_.load(std::memory_order_relaxed); }
  size_t max_reassembly_bytes() const {
    return max_reassembly_bytes_.load(std::memory_order_relaxed);
  }
  size_t max_queued_messages() const {
    return max_queued_messages_.load(std::memory_order_relaxed);
  }
  size_t max_queued_bytes() const { return max_queued_bytes_.load(std::memory_order_relaxed); }

  // Design §5.7: refuse new calls with kUnavailable, fail in-flight calls,
  // terminate the HTTP/2 session gracefully with a best-effort flush, close
  // the socket, join the EventThread. Idempotent; called by the destructor.
  void Shutdown();

 private:
  enum class State { kIdle, kConnecting, kReady, kFailed, kShutdown };

  struct PendingCall {
    uint64_t call_id = 0;
    CallMode mode = CallMode::kUnary;
    std::shared_ptr<CallCompletion> completion;
    std::string method_path;
    std::string framed_body;
    Http2Session::HeaderList metadata;
    std::optional<std::chrono::steady_clock::time_point> deadline;
    TimerId deadline_timer = 0;
  };

  // --- Event-thread only ----------------------------------------------------
  void DrainCancelQueue();
  void ClearCancelQueue();
  void HandleNewCall(PendingCall pc);
  void HandleCancelCall(uint64_t call_id);
  void HandleReadConsumed(uint64_t call_id, size_t wire_bytes);
  void OnCallDeadline(uint64_t call_id);
  void TerminateCallById(uint64_t call_id, StatusCode code, const char* message,
                         bool timer_already_fired);
  void StartConnecting();
  void OnConnectProgress();
  void StartSession();
  void OnSocketReady(short revents);
  void ProcessPendingIgnoredConsumes();
  void ProcessPendingProtocolResets();
  void ProcessPendingStreamResets();
  void FailPendingCalls(StatusCode code, const std::string& message);
  void SubmitCall(PendingCall pc);
  // Concurrent-stream admission cap: min(local max_concurrent_streams,
  // server's advertised SETTINGS value). Calls beyond it wait in
  // pending_calls_.
  size_t MaxConcurrentCalls() const;
  // Submits queued calls while below the cap. Called wherever a slot may
  // have freed (stream close, deadline/cancel/protocol-error reset) or the
  // cap may have grown (server SETTINGS) — never from inside an Http2Session
  // callback (the session must not be re-entered, §4.2).
  void PumpPendingCalls();
  // Schedules a deferred PumpPendingCalls() + FlushOutput() on the next loop
  // turn (safe to call from inside nghttp2 callbacks such as on_close).
  void SchedulePumpPendingCalls();
  void FlushOutput();
  void DrainSocket();
  void RearmKeepalive();
  // Terminal transport failure: fails every pending and active call with
  // kUnavailable(detail) (honoring preemptive cancel/deadline first), closes
  // the transport, state → kFailed. Already wire-complete draining_calls_
  // keep draining locally unless finalize_draining is true (fatal event-loop
  // exit, which fails them with kUnavailable so streams are never silently
  // truncated with OK). Taken by value so callers may pass h2_->error() /
  // sock_.error() safely before TearDown() destroys h2_ or closes sock_.
  void TearDown(std::string detail, bool finalize_draining = false);
  void CancelTimersAndUnwatch();
  void DoShutdown();

  const std::string host_;
  const uint16_t port_;
  const std::string authority_;  // host:port (§5.1)
  const TlsConfig tls_;
  const ChannelOptions options_;
  const size_t effective_max_receive_message_size_;
  const uint32_t stream_window_;
  const size_t effective_max_queued_bytes_;

  EventThread loop_;
  // Set once in the constructor. When false the loop never runs, so nothing
  // may be Post()ed — queued ops would strand their waiters forever.
  bool loop_ok_ = false;

  std::atomic<uint64_t> next_call_id_{1};

  // Serializes Shutdown() callers (they must not race the Stop/join).
  std::mutex shutdown_mu_;
  bool shutdown_done_ = false;
  // Intrusive no-alloc cancellation queue drained on EventThread wakeups.
  std::mutex cancel_mu_;
  bool cancel_closed_ = false;
  std::shared_ptr<CallCompletion> cancel_head_;
  // Snapshot of terminal transport/loop failure readable from caller threads
  // if loop_.Post() returns false after a fatal poll() exit.
  mutable std::mutex terminal_error_mu_;
  std::string terminal_error_;

  // --- Event-thread owned (design §3; never touched by callers) -------------
  State state_ = State::kIdle;
  TlsSocket sock_;
  std::unique_ptr<Http2Session> h2_;
  TimerId connect_timer_ = 0;
  TimerId keepalive_timer_ = 0;
  bool read_yield_pending_ = false;
  bool pump_scheduled_ = false;
  bool tls_read_wants_write_ = false;
  bool tls_write_pending_ = false;
  bool tls_write_wants_read_ = false;
  // Queued while kConnecting, awaiting initial server SETTINGS, or at the
  // concurrent-stream cap.
  std::deque<PendingCall> pending_calls_;
  // Streams currently open on the wire (mapped by call_id).
  std::map<uint64_t, std::unique_ptr<CallState>> active_calls_;
  // Server-streaming calls whose wire stream closed cleanly while complete
  // messages remained in scanner_ behind a full delivery queue.
  std::map<uint64_t, std::unique_ptr<CallState>> draining_calls_;
  // Ignored non-200/non-gRPC DATA chunks (stream_id, bytes) recorded inside
  // nghttp2 callbacks; returned via h2_->ConsumeStream once ReceiveBytes exits.
  std::vector<std::pair<int32_t, size_t>> pending_ignored_consumes_;
  // Active call_ids that hit a framing violation inside nghttp2 callbacks;
  // reset via h2_->ResetStream once DrainSocket() unwinds.
  std::vector<uint64_t> pending_protocol_resets_;
  // Stream IDs whose server side already sent END_STREAM while our upload
  // side remained open (e.g. zero request-body window + trailers-only error);
  // reset via h2_->ResetStream once ReceiveBytes() unwinds.
  std::vector<int32_t> pending_stream_resets_;
  std::string failure_detail_;  // why state_ == kFailed

  // Internal buffer/concurrency high-water marks; written on the EventThread,
  // read from test/diagnostic threads.
  std::atomic<size_t> max_active_calls_{0};
  std::atomic<size_t> max_reassembly_bytes_{0};
  std::atomic<size_t> max_queued_messages_{0};
  std::atomic<size_t> max_queued_bytes_{0};
};

}  // namespace internal
}  // namespace egrpc
