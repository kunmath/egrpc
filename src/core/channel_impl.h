// egrpc — channel: owns the EventThread and the transport (design §4.4).
//
// M6 scope: connectivity FSM IDLE → CONNECTING → READY → TRANSIENT_FAILURE →
// CONNECTING ... → SHUTDOWN with jittered exponential reconnect backoff
// (§5.3), non-blocking generation-tagged DNS (§4.1), wait-for-ready
// admission (§4.4), keepalive enforcement + ENHANCE_YOUR_CALM doubling
// (§5.2), GOAWAY drain & replace (§4.2), and bounded clean Shutdown (§5.7) on
// top of M5's unary/server-streaming calls, Read-driven flow control (§5.4),
// local deadlines (§5.6), and TryCancel (§5.7).
//
// Threading (design §3): the EventThread exclusively owns the TlsSocket,
// the Http2Session, all timers, the Backoff, the DnsResolver, and every
// CallState object (unique_ptr). Caller threads interact only via
// EventThread::Post and CallCompletion (shared_ptr with zero transport
// references); Shutdown joins the loop.
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

#include "core/backoff.h"
#include "core/call_state.h"
#include "core/event_thread.h"
#include "transport/dns_resolver.h"
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
  // Per-attempt connect deadline covering DNS, TCP, TLS, and the peer's
  // initial SETTINGS. Unified with gRPC's MIN_CONNECT_TIMEOUT (§5.3): each
  // attempt (including the first) gets max(connect_timeout, current_backoff).
  // GRPC_ARG_MIN_RECONNECT_BACKOFF_MS maps here.
  std::chrono::milliseconds connect_timeout{20000};
  // Reconnect backoff (§5.3).
  std::chrono::milliseconds initial_reconnect_backoff{1000};
  double reconnect_backoff_multiplier = 1.6;
  std::chrono::milliseconds max_reconnect_backoff{120000};
  double reconnect_backoff_jitter = 0.2;
  // Backoff resets only after a connection has held READY this long (§5.3).
  std::chrono::milliseconds ready_stability_threshold{60000};
  // Bound on draining streams <= GOAWAY last_stream_id before the old
  // connection is closed and replaced (§4.2).
  std::chrono::milliseconds goaway_drain_timeout{5000};
  // Total budget for Shutdown() (§5.7).
  std::chrono::milliseconds shutdown_timeout{5000};
  // DNS backend; null means ResolveBlocking on the resolver helper thread
  // (§4.1). Injectable for tests (e.g. slow or failing resolvers).
  DnsResolver::ResolveFn resolve_fn = nullptr;
  // Prepended to the built-in user-agent (§5.1); upstream's
  // GRPC_ARG_PRIMARY_USER_AGENT maps here.
  std::string user_agent_prefix;
  // EventThread stack size (§3); 0 = platform default.
  size_t event_thread_stack_size = 128 * 1024;
};

class ChannelImpl {
 public:
  // Connectivity FSM (design §4.4).
  enum class ConnectivityState {
    kIdle,
    kConnecting,
    kReady,
    kTransientFailure,
    kShutdown,
  };

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
  // `wait_for_ready` selects the §4.4 admission row: fail-fast calls fail
  // immediately with kUnavailable in TRANSIENT_FAILURE, wait-for-ready calls
  // queue through backoff/reconnect (both honor deadline and TryCancel).
  // Safe from any thread.
  CallHandle StartCall(CallMode mode, const std::string& method_path, std::string request,
                       Http2Session::HeaderList metadata = {},
                       std::optional<std::chrono::steady_clock::time_point> deadline = std::nullopt,
                       uint64_t preallocated_id = 0,
                       std::shared_ptr<CallCompletion> prebound_completion = nullptr,
                       bool wait_for_ready = false);

  // Blocking unary call, safe from any thread. Convenience wrapper over
  // StartCall(CallMode::kUnary, ...) + Wait().
  CallCompletion::Result UnaryCall(
      const std::string& method_path, std::string request, Http2Session::HeaderList metadata = {},
      std::optional<std::chrono::steady_clock::time_point> deadline = std::nullopt,
      bool wait_for_ready = false);

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

  // Diagnostics/tests (§8/M6): snapshots written on the EventThread,
  // readable from any thread.
  ConnectivityState state() const { return state_snapshot_.load(std::memory_order_acquire); }
  // Current keepalive_time (doubled on GOAWAY ENHANCE_YOUR_CALM/too_many_pings,
  // §5.2; applies from the next connection on).
  std::chrono::milliseconds effective_keepalive_time() const {
    return std::chrono::milliseconds(effective_keepalive_time_ms_.load(std::memory_order_relaxed));
  }
  // Number of connect attempts started (initial connect + reconnects).
  uint64_t connect_attempts() const { return connect_attempts_.load(std::memory_order_relaxed); }

  // Design §5.7: refuse new calls with kUnavailable, fail pending, active,
  // and draining calls (unblocking every waiter first), retire the DNS
  // resolver without blocking on a hung lookup, send GOAWAY(NO_ERROR) with a
  // flush bounded by shutdown_timeout, close the socket, join the
  // EventThread. Idempotent; called by the destructor.
  void Shutdown();

 private:
  struct PendingCall {
    uint64_t call_id = 0;
    CallMode mode = CallMode::kUnary;
    std::shared_ptr<CallCompletion> completion;
    std::string method_path;
    std::string framed_body;
    Http2Session::HeaderList metadata;
    std::optional<std::chrono::steady_clock::time_point> deadline;
    TimerId deadline_timer = 0;
    bool wait_for_ready = false;
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
  // --- Connectivity FSM (§4.4) ----------------------------------------------
  void SetState(ConnectivityState state);
  // h2_ and sock_ exist and belong to the current attempt (READY, or
  // CONNECTING after TLS while awaiting the peer's initial SETTINGS).
  bool HasLiveSession() const {
    return h2_ != nullptr && sock_ != nullptr &&
           (state_ == ConnectivityState::kReady || state_ == ConnectivityState::kConnecting);
  }
  // Starts a connect attempt: re-resolves DNS (§4.1) under an attempt
  // timer of backoff_.CurrentConnectTimeout() (§5.3).
  void StartConnecting();
  void OnDnsResolved(uint64_t generation);
  void OnConnectProgress();
  void StartSession();
  void OnSocketReady(short revents);
  void ProcessPendingIgnoredConsumes();
  void ProcessPendingProtocolResets();
  void ProcessPendingStreamResets();
  // GOAWAY drain & replace (§4.2), run once nghttp2 callbacks unwind.
  void ProcessPendingGoaway();
  // Closes a not-yet-submitted call locally (cancelling its deadline timer),
  // honoring an already-requested cancel or an elapsed deadline first.
  void FailPendingCall(PendingCall& pc, StatusCode code, const std::string& message);
  // Fails every pending call; when keep_wait_for_ready is true, wait-for-ready
  // calls stay queued (deadline timers intact) for the next connection.
  void FailPendingCalls(StatusCode code, const std::string& message,
                        bool keep_wait_for_ready = false);
  void FailActiveCalls(const std::string& message);
  void FailDrainingCalls(const std::string& message);
  void SubmitCall(PendingCall pc);
  // Concurrent-stream admission cap: min(local max_concurrent_streams,
  // server's advertised SETTINGS value). Calls beyond it wait in
  // pending_calls_.
  size_t MaxConcurrentCalls() const;
  // Submits queued calls while READY and below the cap (never onto a
  // GOAWAY-draining session; once that session has no active streams it is
  // replaced). Called wherever a slot may have freed (stream close,
  // deadline/cancel/protocol-error reset) or the cap may have grown (server
  // SETTINGS) — never from inside an Http2Session callback (the session must
  // not be re-entered, §4.2).
  void PumpPendingCalls();
  // Schedules a deferred PumpPendingCalls() + FlushOutput() on the next loop
  // turn (safe to call from inside nghttp2 callbacks such as on_close).
  void SchedulePumpPendingCalls();
  void FlushOutput();
  void DrainSocket();
  void RearmKeepalive();
  // Recoverable transport failure (connect/DNS error or timeout, keepalive
  // death, socket error, GOAWAY replace): closes the transport, fails active
  // calls and fail-fast pending calls with kUnavailable(detail) (honoring
  // preemptive cancel/deadline), keeps wait-for-ready calls queued and
  // wire-complete draining_calls_ draining, then arms the reconnect timer
  // from backoff_. Taken by value so callers may pass h2_->error() /
  // sock_->error() safely. Never called from inside an Http2Session callback.
  void EnterTransientFailure(std::string detail);
  // Terminal: the EventThread is exiting on a fatal poll() error. Fails every
  // call (including wait-for-ready and draining), closes the transport,
  // retires the resolver, state → kShutdown.
  void FatalLoopExit(std::string detail);
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

  // Set (release) by Shutdown() before the loop is stopped; StartCall and
  // HandleNewCall reject new calls without starting a connect (§5.7).
  std::atomic<bool> shutting_down_{false};
  // Absolute Shutdown() deadline (steady_clock ticks since epoch), stored
  // (release) by Shutdown() before shutting_down_ and loop_.Stop(); DoShutdown()
  // bounds every sub-step by it so queued work ahead of it cannot extend the
  // total shutdown_timeout budget (§5.7).
  std::atomic<int64_t> shutdown_deadline_ns_{0};

  // --- Event-thread owned (design §3; never touched by callers) -------------
  ConnectivityState state_ = ConnectivityState::kIdle;
  Backoff backoff_;
  DnsResolver resolver_;
  bool resolving_dns_ = false;
  uint64_t dns_gen_ = 0;
  uint64_t next_dns_gen_ = 0;
  std::unique_ptr<TlsSocket> sock_;
  std::unique_ptr<Http2Session> h2_;
  TimerId connect_timer_ = 0;
  TimerId keepalive_timer_ = 0;
  TimerId reconnect_timer_ = 0;
  TimerId goaway_drain_timer_ = 0;
  bool read_yield_pending_ = false;
  // FlushOutput() yielded after its per-turn write budget; a next-turn
  // continuation resumes the flush.
  bool write_yield_pending_ = false;
  bool pump_scheduled_ = false;
  bool tls_read_wants_write_ = false;
  bool tls_write_pending_ = false;
  bool tls_write_wants_read_ = false;
  // GOAWAY state of the current connection (§4.2). on_goaway (inside an
  // nghttp2 callback) only records; ProcessPendingGoaway() acts.
  bool goaway_received_ = false;
  bool pending_goaway_process_ = false;
  bool goaway_too_many_pings_applied_ = false;
  int32_t goaway_last_stream_id_ = std::numeric_limits<int32_t>::max();
  std::string goaway_detail_;
  // keepalive_time for new sessions; doubled on ENHANCE_YOUR_CALM (§5.2).
  std::chrono::milliseconds effective_keepalive_time_;
  // Queued while IDLE/CONNECTING, at the concurrent-stream cap, or (wait-for-
  // ready only) in TRANSIENT_FAILURE / GOAWAY drain.
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
  std::string last_failure_detail_;  // why the last attempt/connection failed

  std::atomic<ConnectivityState> state_snapshot_{ConnectivityState::kIdle};
  std::atomic<int64_t> effective_keepalive_time_ms_{0};
  std::atomic<uint64_t> connect_attempts_{0};

  // Internal buffer/concurrency high-water marks; written on the EventThread,
  // read from test/diagnostic threads.
  std::atomic<size_t> max_active_calls_{0};
  std::atomic<size_t> max_reassembly_bytes_{0};
  std::atomic<size_t> max_queued_messages_{0};
  std::atomic<size_t> max_queued_bytes_{0};
};

}  // namespace internal
}  // namespace egrpc
