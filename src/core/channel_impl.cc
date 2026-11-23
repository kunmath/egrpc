// egrpc — channel over one EventThread-owned transport (design §4.1, §4.2,
// §4.4, §5.2–§5.7): connectivity FSM with backoff reconnect, non-blocking
// DNS, wait-for-ready admission, keepalive enforcement, GOAWAY drain &
// replace, and bounded clean shutdown.
#include "core/channel_impl.h"

#include <egrpc/log.h>
#include <poll.h>

#include <algorithm>
#include <climits>
#include <string>
#include <utility>

namespace egrpc {
namespace internal {

namespace {

using std::chrono::milliseconds;
using std::chrono::steady_clock;

constexpr size_t kMaxHttp2Window = static_cast<size_t>(INT32_MAX);

// Upper bound for ENHANCE_YOUR_CALM keepalive_time doubling (§5.2).
constexpr milliseconds kMaxKeepaliveTime{24LL * 60 * 60 * 1000};

constexpr char kShutDownMessage[] = "channel is shut down";

// Clamps max_receive_message_size so that max_receive_message_size + 5 always
// fits within RFC 7540's INT32_MAX window limit (preserving the §5.4 invariant
// stream_window >= max_receive_message_size + 5 even if the caller asks for
// INT_MAX).
size_t ClampMaxReceiveMessageSize(size_t requested) {
  return std::min(requested, kMaxHttp2Window - 5);
}

// §5.4: stream_window = effective_max_receive_message_size + 5 B frame prefix
// + slack, clamped to INT32_MAX.
uint32_t ComputeStreamWindow(size_t effective_max_receive_message_size, size_t slack) {
  const size_t base = effective_max_receive_message_size + 5;
  if (slack >= kMaxHttp2Window - base) return static_cast<uint32_t>(kMaxHttp2Window);
  return static_cast<uint32_t>(base + slack);
}

BackoffOptions MakeBackoffOptions(const ChannelOptions& o) {
  BackoffOptions b;
  b.initial_backoff = o.initial_reconnect_backoff;
  b.multiplier = o.reconnect_backoff_multiplier;
  b.max_backoff = o.max_reconnect_backoff;
  b.jitter = o.reconnect_backoff_jitter;
  b.connect_timeout = o.connect_timeout;
  b.ready_stability_threshold = o.ready_stability_threshold;
  return b;
}

// Doubles keepalive_time, clamped to kMaxKeepaliveTime without overflow. A
// value already above the cap is left unchanged (never shortened).
milliseconds DoubleKeepaliveTime(milliseconds t) {
  if (t.count() <= 0 || t >= kMaxKeepaliveTime) return t;
  if (t >= kMaxKeepaliveTime / 2) return kMaxKeepaliveTime;
  return t * 2;
}

// Remaining whole milliseconds until `deadline` (0 if already passed).
int RemainingMs(steady_clock::time_point deadline) {
  const auto now = steady_clock::now();
  if (deadline <= now) return 0;
  const auto ms = std::chrono::duration_cast<milliseconds>(deadline - now).count();
  return static_cast<int>(std::min<int64_t>(ms, INT_MAX));
}

void UpdateMax(std::atomic<size_t>& high_water, size_t value) {
  if (value > high_water.load(std::memory_order_relaxed)) {
    high_water.store(value, std::memory_order_relaxed);
  }
}

void CloseCompletionLocal(const std::shared_ptr<CallCompletion>& completion, StatusCode code,
                          std::string message) {
  CallCompletion::Result r;
  r.code = code;
  r.message = std::move(message);
  r.origin = StatusOrigin::kLocal;
  completion->Close(std::move(r), /*discard_queued=*/true);
}

}  // namespace

ChannelImpl::ChannelImpl(std::string host, uint16_t port, TlsConfig tls, ChannelOptions options)
    : host_(std::move(host)),
      port_(port),
      // IPv6 literals must be re-bracketed in :authority (§5.1); host_ itself
      // stays bare for the resolver and SNI.
      authority_((host_.find(':') == std::string::npos ? host_ : "[" + host_ + "]") + ":" +
                 std::to_string(port)),
      tls_(std::move(tls)),
      options_(std::move(options)),
      effective_max_receive_message_size_(
          ClampMaxReceiveMessageSize(options_.max_receive_message_size)),
      stream_window_(
          ComputeStreamWindow(effective_max_receive_message_size_, options_.stream_window_slack)),
      effective_max_queued_bytes_(options_.max_queued_bytes == 0 ? stream_window_
                                                                 : options_.max_queued_bytes),
      loop_(EventThread::Options{options_.event_thread_stack_size}),
      backoff_(MakeBackoffOptions(options_)),
      resolver_(options_.resolve_fn),
      effective_keepalive_time_(options_.http2.keepalive_time) {
  effective_keepalive_time_ms_.store(effective_keepalive_time_.count(), std::memory_order_relaxed);
  loop_.SetWakeupCallback([this] { DrainCancelQueue(); });
  loop_.SetFatalExitCallback([this] {
    ClearCancelQueue();
    FatalLoopExit("event loop poll() error");
  });
  loop_ok_ = loop_.Start();
  if (!loop_ok_) {
    // No thread running: safe to write EventThread-owned state from here.
    SetState(ConnectivityState::kShutdown);
    last_failure_detail_ = "failed to start channel event thread";
    terminal_error_ = last_failure_detail_;
  }
}

ChannelImpl::~ChannelImpl() { Shutdown(); }

ChannelImpl::CallHandle ChannelImpl::StartCall(
    CallMode mode, const std::string& method_path, std::string request,
    Http2Session::HeaderList metadata,
    std::optional<std::chrono::steady_clock::time_point> deadline, uint64_t preallocated_id,
    std::shared_ptr<CallCompletion> prebound_completion, bool wait_for_ready) {
  const uint64_t call_id = preallocated_id != 0 ? preallocated_id : AllocateCallId();
  std::shared_ptr<CallCompletion> completion = prebound_completion
                                                   ? std::move(prebound_completion)
                                                   : std::make_shared<CallCompletion>(call_id);
  if (completion->call_id() == 0) completion->set_call_id(call_id);

  // Send-path bound (§4.5) and 32-bit gRPC length-prefix wire limit (§5.1):
  // checked before anything is queued, so the caller gets kResourceExhausted
  // without a wire exchange, like upstream.
  const size_t effective_max_send =
      std::min<size_t>(options_.max_send_message_size, std::numeric_limits<uint32_t>::max());
  if (request.size() > effective_max_send) {
    CloseCompletionLocal(completion, StatusCode::kResourceExhausted,
                         "Sent message larger than max (" + std::to_string(request.size()) +
                             " vs. " + std::to_string(effective_max_send) + ")");
    return {call_id, std::move(completion)};
  }

  // Pre-submission TryCancel (§5.7): fail immediately without touching the wire.
  if (completion->cancel_requested()) {
    CloseCompletionLocal(completion, StatusCode::kCancelled, "Cancelled");
    return {call_id, std::move(completion)};
  }

  auto fail_unavailable = [&] {
    std::string detail;
    {
      std::lock_guard<std::mutex> lock(terminal_error_mu_);
      detail = terminal_error_;
    }
    CloseCompletionLocal(completion, StatusCode::kUnavailable,
                         detail.empty() ? kShutDownMessage : std::move(detail));
  };

  // §5.7: once Shutdown() has begun, refuse without posting (and so without
  // ever starting a connect or DNS resolve).
  if (!loop_ok_ || shutting_down_.load(std::memory_order_acquire)) {
    fail_unavailable();
    return {call_id, std::move(completion)};
  }

  // shared_ptr wrapper: std::function requires copyable callables, and the
  // body is worth not copying.
  auto pc = std::make_shared<PendingCall>();
  pc->call_id = call_id;
  pc->mode = mode;
  pc->completion = completion;
  pc->method_path = method_path;
  AppendGrpcFramePrefix(request.size(), &pc->framed_body);
  pc->framed_body += request;
  pc->metadata = std::move(metadata);
  pc->deadline = deadline;
  pc->wait_for_ready = wait_for_ready;

  if (!loop_.Post([this, pc] { HandleNewCall(std::move(*pc)); })) fail_unavailable();
  return {call_id, std::move(completion)};
}

CallCompletion::Result ChannelImpl::UnaryCall(
    const std::string& method_path, std::string request, Http2Session::HeaderList metadata,
    std::optional<std::chrono::steady_clock::time_point> deadline, bool wait_for_ready) {
  CallHandle handle = StartCall(CallMode::kUnary, method_path, std::move(request),
                                std::move(metadata), deadline, /*preallocated_id=*/0,
                                /*prebound_completion=*/nullptr, wait_for_ready);
  return handle.completion->Wait();
}

void ChannelImpl::CancelCall(uint64_t call_id) {
  if (!loop_ok_ || call_id == 0) return;
  loop_.Post([this, call_id] { HandleCancelCall(call_id); });
}

void ChannelImpl::CancelCall(const std::shared_ptr<CallCompletion>& completion) noexcept {
  if (!completion) return;
  completion->RequestCancel();
  if (!loop_ok_) return;
  {
    std::lock_guard<std::mutex> lock(cancel_mu_);
    if (cancel_closed_) return;
    if (!completion->cancel_enqueued_) {
      completion->cancel_enqueued_ = true;
      completion->next_cancel_ = std::move(cancel_head_);
      cancel_head_ = completion;
    }
  }
  loop_.Wake();
}

void ChannelImpl::DrainCancelQueue() {
  std::shared_ptr<CallCompletion> head;
  {
    std::lock_guard<std::mutex> lock(cancel_mu_);
    head = std::move(cancel_head_);
  }
  while (head) {
    auto next = std::move(head->next_cancel_);
    const uint64_t cid = head->call_id();
    head.reset();
    if (cid != 0) HandleCancelCall(cid);
    head = std::move(next);
  }
}

void ChannelImpl::ClearCancelQueue() {
  std::shared_ptr<CallCompletion> head;
  {
    std::lock_guard<std::mutex> lock(cancel_mu_);
    cancel_closed_ = true;
    head = std::move(cancel_head_);
  }
  while (head) {
    auto next = std::move(head->next_cancel_);
    head.reset();
    head = std::move(next);
  }
}

void ChannelImpl::NotifyReadConsumed(uint64_t call_id, size_t wire_bytes) {
  if (!loop_ok_ || call_id == 0) return;
  loop_.Post([this, call_id, wire_bytes] { HandleReadConsumed(call_id, wire_bytes); });
}

void ChannelImpl::Shutdown() {
  std::lock_guard<std::mutex> lock(shutdown_mu_);
  if (shutdown_done_) return;
  shutdown_done_ = true;

  // The total budget starts now, not when DoShutdown() finally runs.
  const auto deadline = steady_clock::now() + std::max(options_.shutdown_timeout, milliseconds(0));
  shutdown_deadline_ns_.store(static_cast<int64_t>(deadline.time_since_epoch().count()),
                              std::memory_order_release);

  // Close the admission gate first: StartCall and any HandleNewCall still
  // queued ahead of DoShutdown() reject without starting a connect.
  shutting_down_.store(true, std::memory_order_release);

  if (!loop_ok_) return;  // loop never ran; nothing to drain or join

  // Atomically closes the EventThread Post() gate and appends DoShutdown() as
  // the last op before thread exit, preventing any call from slipping in after
  // DoShutdown() and avoiding deadlock if poll() exited early.
  loop_.Stop([this] { DoShutdown(); });
  // The EventThread has joined; drop any cancellation queued after its final
  // drain (also covers a fatal poll() exit that skipped DoShutdown()).
  ClearCancelQueue();
}

// --- Event-thread only -------------------------------------------------------

void ChannelImpl::SetState(ConnectivityState state) {
  state_ = state;
  state_snapshot_.store(state, std::memory_order_release);
}

void ChannelImpl::HandleNewCall(PendingCall pc) {
  if (shutting_down_.load(std::memory_order_acquire) || state_ == ConnectivityState::kShutdown) {
    CloseCompletionLocal(pc.completion, StatusCode::kUnavailable, kShutDownMessage);
    return;
  }
  if (pc.completion->cancel_requested()) {
    CloseCompletionLocal(pc.completion, StatusCode::kCancelled, "Cancelled");
    return;
  }
  const auto now = steady_clock::now();
  if (pc.deadline.has_value() && *pc.deadline <= now) {
    CloseCompletionLocal(pc.completion, StatusCode::kDeadlineExceeded, "Deadline Exceeded");
    return;
  }
  // §4.4 admission table: fail-fast calls fail immediately in
  // TRANSIENT_FAILURE and while the READY connection is draining after
  // GOAWAY; wait-for-ready calls queue for the next connection.
  if (!pc.wait_for_ready) {
    if (state_ == ConnectivityState::kTransientFailure) {
      CloseCompletionLocal(pc.completion, StatusCode::kUnavailable, last_failure_detail_);
      return;
    }
    if (state_ == ConnectivityState::kReady && goaway_received_) {
      CloseCompletionLocal(pc.completion, StatusCode::kUnavailable, goaway_detail_);
      return;
    }
  }

  // Local deadline timer (§5.6): armed at admission so even calls queued in
  // pending_calls_ (connecting, backoff, or at the concurrent-stream cap)
  // fail on time.
  if (pc.deadline.has_value()) {
    const uint64_t cid = pc.call_id;
    pc.deadline_timer = loop_.AddTimer(*pc.deadline - now, [this, cid] { OnCallDeadline(cid); });
  }

  switch (state_) {
    case ConnectivityState::kIdle:
      pending_calls_.push_back(std::move(pc));
      StartConnecting();
      return;
    case ConnectivityState::kConnecting:
    case ConnectivityState::kTransientFailure:
      pending_calls_.push_back(std::move(pc));
      return;
    case ConnectivityState::kReady:
      // FIFO behind any already-queued calls; submits immediately when below
      // the concurrent-stream cap and not draining after GOAWAY.
      pending_calls_.push_back(std::move(pc));
      PumpPendingCalls();
      if (HasLiveSession()) FlushOutput();
      RearmKeepalive();
      return;
    case ConnectivityState::kShutdown:
      FailPendingCall(pc, StatusCode::kUnavailable, kShutDownMessage);
      return;
  }
}

void ChannelImpl::HandleCancelCall(uint64_t call_id) {
  TerminateCallById(call_id, StatusCode::kCancelled, "Cancelled", /*timer_already_fired=*/false);
}

void ChannelImpl::OnCallDeadline(uint64_t call_id) {
  TerminateCallById(call_id, StatusCode::kDeadlineExceeded, "Deadline Exceeded",
                    /*timer_already_fired=*/true);
}

void ChannelImpl::TerminateCallById(uint64_t call_id, StatusCode code, const char* message,
                                    bool timer_already_fired) {
  for (auto it = pending_calls_.begin(); it != pending_calls_.end(); ++it) {
    if (it->call_id == call_id) {
      if (!timer_already_fired && it->deadline_timer != 0) {
        loop_.CancelTimer(it->deadline_timer);
      }
      auto completion = std::move(it->completion);
      pending_calls_.erase(it);
      CloseCompletionLocal(completion, code, message);
      return;
    }
  }

  auto active_it = active_calls_.find(call_id);
  if (active_it != active_calls_.end()) {
    std::unique_ptr<CallState> call = std::move(active_it->second);
    active_calls_.erase(active_it);
    if (!timer_already_fired && call->deadline_timer() != 0) {
      loop_.CancelTimer(call->deadline_timer());
    }
    const int32_t sid = call->stream_id();
    // Queue and flush RST_STREAM before waking the caller in FailLocal() so
    // the reset is already dispatched by the time Finish()/UnaryCall() returns.
    if (HasLiveSession() && sid > 0) {
      h2_->ResetStream(sid, Http2Session::kHttp2Cancel);
      FlushOutput();
    }
    call->FailLocal(code, message);
    // A freed slot admits queued calls; a GOAWAY-draining session whose last
    // stream just ended is replaced.
    if (HasLiveSession()) {
      PumpPendingCalls();
      if (HasLiveSession()) FlushOutput();
    }
    RearmKeepalive();
    return;
  }

  auto draining_it = draining_calls_.find(call_id);
  if (draining_it != draining_calls_.end()) {
    std::unique_ptr<CallState> call = std::move(draining_it->second);
    draining_calls_.erase(draining_it);
    if (!timer_already_fired && call->deadline_timer() != 0) {
      loop_.CancelTimer(call->deadline_timer());
    }
    // The wire RPC already finished on the server; discard unread local
    // messages and finalize with the server's already-received status so early
    // Finish()/TryCancel() is consistent regardless of message count.
    call->FinalizeDiscarding();
  }
}

void ChannelImpl::HandleReadConsumed(uint64_t call_id, size_t wire_bytes) {
  auto active_it = active_calls_.find(call_id);
  if (active_it != active_calls_.end()) {
    CallState* call = active_it->second.get();
    call->DrainScannerToQueue();
    if (HasLiveSession() && call->stream_id() > 0 && wire_bytes > 0) {
      h2_->ConsumeStream(call->stream_id(), wire_bytes);
      FlushOutput();
    }
    return;
  }

  auto draining_it = draining_calls_.find(call_id);
  if (draining_it != draining_calls_.end()) {
    CallState* call = draining_it->second.get();
    call->DrainScannerToQueue();
    if (call->is_closed()) {
      if (call->deadline_timer() != 0) loop_.CancelTimer(call->deadline_timer());
      draining_calls_.erase(draining_it);
    }
  }
}

size_t ChannelImpl::MaxConcurrentCalls() const {
  const uint32_t local = options_.http2.max_concurrent_streams;
  if (h2_ == nullptr) return local;
  // Wait for the server's initial SETTINGS before admitting streams so we
  // honor a peer's low SETTINGS_MAX_CONCURRENT_STREAMS from the very first RPC.
  if (!h2_->remote_settings_received()) return 0;
  return std::min(local, h2_->remote_max_concurrent_streams());
}

void ChannelImpl::PumpPendingCalls() {
  if (state_ != ConnectivityState::kReady || h2_ == nullptr) return;
  if (goaway_received_) {
    // §4.2: never open streams on a draining connection. Once its last
    // stream has finished, replace it (queued wait-for-ready calls go to the
    // replacement connection).
    if (active_calls_.empty()) EnterTransientFailure(goaway_detail_);
    return;
  }
  bool submitted = false;
  while (state_ == ConnectivityState::kReady && h2_ != nullptr && !pending_calls_.empty() &&
         active_calls_.size() < MaxConcurrentCalls()) {
    PendingCall pc = std::move(pending_calls_.front());
    pending_calls_.pop_front();
    SubmitCall(std::move(pc));
    submitted = true;
  }
  // 0 → 1 active streams re-enables keepalive when permit_without_calls=false.
  if (submitted) RearmKeepalive();
}

void ChannelImpl::SchedulePumpPendingCalls() {
  if (pump_scheduled_ || !HasLiveSession()) return;
  pump_scheduled_ = true;
  loop_.ScheduleNextTurn([this] {
    if (!pump_scheduled_) return;
    pump_scheduled_ = false;
    if (!HasLiveSession()) return;
    PumpPendingCalls();
    if (HasLiveSession()) FlushOutput();
    RearmKeepalive();
  });
}

void ChannelImpl::StartConnecting() {
  if (state_ == ConnectivityState::kShutdown || shutting_down_.load(std::memory_order_acquire)) {
    return;
  }
  for (TimerId* t : {&reconnect_timer_, &connect_timer_}) {
    if (*t != 0) {
      loop_.CancelTimer(*t);
      *t = 0;
    }
  }
  SetState(ConnectivityState::kConnecting);
  connect_attempts_.fetch_add(1, std::memory_order_relaxed);

  loop_.UnwatchFd();
  h2_.reset();
  sock_.reset();
  goaway_received_ = false;
  pending_goaway_process_ = false;
  goaway_too_many_pings_applied_ = false;
  goaway_last_stream_id_ = std::numeric_limits<int32_t>::max();
  read_yield_pending_ = false;
  write_yield_pending_ = false;
  pump_scheduled_ = false;
  tls_read_wants_write_ = false;
  tls_write_pending_ = false;
  tls_write_wants_read_ = false;

  // §5.3: every attempt gets max(connect_timeout, current_backoff). Covers
  // DNS, TCP, TLS, and the peer's initial SETTINGS.
  const milliseconds attempt_timeout = backoff_.CurrentConnectTimeout();
  connect_timer_ = loop_.AddTimer(attempt_timeout, [this] {
    connect_timer_ = 0;
    if (state_ != ConnectivityState::kConnecting) return;
    if (resolving_dns_) {
      resolver_.Invalidate();
      resolving_dns_ = false;
      EnterTransientFailure("DNS resolution timeout");
    } else if (h2_ != nullptr) {
      EnterTransientFailure("timed out waiting for peer HTTP/2 SETTINGS");
    } else {
      EnterTransientFailure("connect timeout");
    }
  });

  // §4.1: re-resolve on every attempt, off the EventThread. on_ready runs on
  // the resolver thread; DoShutdown()/FatalLoopExit() shut the resolver down
  // (clearing on_ready under its slot mutex) before this EventThread exits,
  // and a Post() refused during shutdown is benign. On an immediate resolver
  // failure (worker cap / thread creation) on_ready runs synchronously here;
  // Post() from the EventThread is safe (no EventThread lock is held while
  // ops/timers run) and defers OnDnsResolved() to the next turn.
  resolving_dns_ = true;
  dns_gen_ = ++next_dns_gen_;
  resolver_.Resolve(host_, port_, dns_gen_,
                    [this](uint64_t gen) { loop_.Post([this, gen] { OnDnsResolved(gen); }); });
}

void ChannelImpl::OnDnsResolved(uint64_t generation) {
  (void)generation;  // the slot's own generation tag is authoritative
  if (state_ != ConnectivityState::kConnecting || !resolving_dns_) return;
  std::optional<ResolveResult> res = resolver_.TakeResult();
  // Absent or superseded result (e.g. a duplicate or late wakeup): keep
  // waiting for the current generation's own wakeup.
  if (!res.has_value() || res->generation != dns_gen_) return;
  resolving_dns_ = false;
  if (!res->ok()) {
    EnterTransientFailure(res->error.empty() ? "DNS resolution failed" : res->error);
    return;
  }
  sock_ = std::make_unique<TlsSocket>();
  if (!sock_->StartConnect(host_, res->addresses, tls_)) {
    EnterTransientFailure(sock_->error().empty() ? "connect failed" : sock_->error());
    return;
  }
  loop_.WatchFd(
      sock_->fd(), [this] { return sock_ != nullptr ? sock_->DesiredPollEvents() : short{0}; },
      [this](short) { OnConnectProgress(); });
}

void ChannelImpl::OnConnectProgress() {
  if (state_ != ConnectivityState::kConnecting || sock_ == nullptr || h2_ != nullptr) return;
  const int prev_fd = sock_->fd();
  const TlsSocket::IoStatus st = sock_->ContinueConnect();
  if (st == TlsSocket::IoStatus::kWantRead || st == TlsSocket::IoStatus::kWantWrite) {
    // Address fallback: a failed TCP connect to one resolved address opens a
    // new socket for the next one — watch the new fd.
    if (sock_->fd() != prev_fd && sock_->fd() >= 0) {
      loop_.WatchFd(
          sock_->fd(), [this] { return sock_ != nullptr ? sock_->DesiredPollEvents() : short{0}; },
          [this](short) { OnConnectProgress(); });
    }
    return;
  }
  if (st == TlsSocket::IoStatus::kOk) {
    // Keep connect_timer_ armed (and state CONNECTING) until the server's
    // initial HTTP/2 SETTINGS arrives, so a peer that completes TLS/ALPN but
    // never speaks HTTP/2 still times out.
    StartSession();
    return;
  }
  EnterTransientFailure(sock_->error().empty() ? "connect failed" : sock_->error());
}

void ChannelImpl::StartSession() {
  Http2Session::Hooks hooks;
  // Runs inside an nghttp2 callback: only record state and defer work.
  hooks.on_remote_settings = [this] {
    if (connect_timer_ != 0) {
      loop_.CancelTimer(connect_timer_);
      connect_timer_ = 0;
    }
    if (state_ == ConnectivityState::kConnecting) {
      SetState(ConnectivityState::kReady);
      backoff_.OnReady(steady_clock::now());
    }
    SchedulePumpPendingCalls();
  };
  // GOAWAY (§4.2, §5.2): runs inside an nghttp2 callback — never destroy or
  // re-enter the session here; ProcessPendingGoaway() acts once it unwinds.
  hooks.on_goaway = [this](uint32_t error_code, int32_t last_stream_id, bool too_many_pings) {
    goaway_received_ = true;
    goaway_last_stream_id_ = std::min(goaway_last_stream_id_, last_stream_id);
    goaway_detail_ = "server sent GOAWAY (error_code=" + std::to_string(error_code) + ")";
    if (too_many_pings && !goaway_too_many_pings_applied_) {
      goaway_too_many_pings_applied_ = true;
      const milliseconds old_time = effective_keepalive_time_;
      effective_keepalive_time_ = DoubleKeepaliveTime(old_time);
      effective_keepalive_time_ms_.store(effective_keepalive_time_.count(),
                                         std::memory_order_relaxed);
      egrpc::Log(egrpc::LogLevel::kWarn, "keepalive",
                 "received GOAWAY(ENHANCE_YOUR_CALM/too_many_pings); doubling keepalive_time "
                 "from " +
                     std::to_string(old_time.count()) + " ms to " +
                     std::to_string(effective_keepalive_time_.count()) + " ms");
    }
    pending_goaway_process_ = true;
  };

  Http2SessionOptions h2_opts = options_.http2;
  // §5.4: derived stream window, never smaller than max_receive_message_size + 5.
  h2_opts.initial_window_size = stream_window_;
  // §5.2: carries any ENHANCE_YOUR_CALM doubling across reconnects.
  h2_opts.keepalive_time = effective_keepalive_time_;

  h2_ = std::make_unique<Http2Session>(h2_opts, std::move(hooks), steady_clock::now());
  if (!h2_->ok()) {
    EnterTransientFailure(h2_->error());
    return;
  }

  loop_.WatchFd(
      sock_->fd(),
      [this] {
        // Honor OpenSSL's pending-operation contract: when an SSL_write_ex or
        // SSL_read_ex returned WANT_READ/WANT_WRITE, poll ONLY the readiness
        // needed to retry that exact operation so we never issue the opposite
        // SSL call first or busy-spin on an unserviced event. (POSIX poll
        // still delivers POLLERR/POLLHUP regardless of requested events.)
        if (tls_write_pending_) {
          return static_cast<short>(tls_write_wants_read_ ? POLLIN : POLLOUT);
        }
        if (tls_read_wants_write_) {
          return static_cast<short>(POLLOUT);
        }
        short events = POLLIN;
        if (h2_ != nullptr && h2_->WantWrite()) events |= POLLOUT;
        return events;
      },
      [this](short revents) { OnSocketReady(revents); });

  // Connection preface + SETTINGS. State stays CONNECTING until the server's
  // SETTINGS arrives (on_remote_settings); pending calls are pumped then.
  FlushOutput();
}

void ChannelImpl::OnSocketReady(short revents) {
  if (!HasLiveSession()) return;
  // Retry an interrupted OpenSSL write before issuing any SSL_read_ex.
  if (tls_write_pending_) {
    const bool write_ready =
        (revents & (POLLERR | POLLHUP)) != 0 ||
        (tls_write_wants_read_ ? (revents & POLLIN) != 0 : (revents & POLLOUT) != 0);
    if (write_ready) FlushOutput();
    if (!HasLiveSession()) return;
    if (tls_write_pending_) {
      RearmKeepalive();
      return;
    }
  }

  const bool want_read = tls_read_wants_write_ ? (revents & (POLLOUT | POLLERR | POLLHUP)) != 0
                                               : (revents & (POLLIN | POLLERR | POLLHUP)) != 0;
  if (want_read) DrainSocket();
  // All Http2Session callbacks have unwound from here on.
  if (HasLiveSession()) ProcessPendingIgnoredConsumes();
  if (HasLiveSession()) ProcessPendingProtocolResets();
  if (HasLiveSession()) ProcessPendingStreamResets();
  if (HasLiveSession()) ProcessPendingGoaway();
  // Stream closes, protocol resets, and server SETTINGS can open admission
  // slots (or finish a GOAWAY drain).
  if (HasLiveSession()) PumpPendingCalls();
  if (HasLiveSession() && !tls_read_wants_write_) FlushOutput();
  RearmKeepalive();
}

void ChannelImpl::ProcessPendingIgnoredConsumes() {
  if (pending_ignored_consumes_.empty() || h2_ == nullptr) return;
  auto consumes = std::move(pending_ignored_consumes_);
  pending_ignored_consumes_.clear();
  for (const auto& entry : consumes) {
    if (entry.first > 0 && entry.second > 0) {
      h2_->ConsumeStream(entry.first, entry.second);
    }
  }
  if (HasLiveSession() && !tls_read_wants_write_) FlushOutput();
}

void ChannelImpl::ProcessPendingProtocolResets() {
  if (pending_protocol_resets_.empty()) return;
  std::vector<uint64_t> resets = std::move(pending_protocol_resets_);
  pending_protocol_resets_.clear();
  bool any_reset = false;
  for (uint64_t cid : resets) {
    auto it = active_calls_.find(cid);
    if (it == active_calls_.end()) continue;
    std::unique_ptr<CallState> call = std::move(it->second);
    active_calls_.erase(it);
    if (call->deadline_timer() != 0) loop_.CancelTimer(call->deadline_timer());
    const int32_t sid = call->stream_id();
    if (h2_ != nullptr && sid > 0) {
      h2_->ResetStream(sid, Http2Session::kHttp2Cancel);
      any_reset = true;
    }
  }
  if (any_reset && HasLiveSession() && !tls_read_wants_write_) {
    // Flush RST_STREAM before PumpPendingCalls so nghttp2 frees the stream.
    FlushOutput();
  }
}

void ChannelImpl::ProcessPendingStreamResets() {
  if (pending_stream_resets_.empty()) return;
  std::vector<int32_t> resets = std::move(pending_stream_resets_);
  pending_stream_resets_.clear();
  bool any_reset = false;
  for (int32_t sid : resets) {
    if (h2_ != nullptr && sid > 0) {
      h2_->ResetStream(sid, Http2Session::kHttp2Cancel);
      any_reset = true;
    }
  }
  if (any_reset && HasLiveSession() && !tls_read_wants_write_) {
    FlushOutput();
  }
}

void ChannelImpl::ProcessPendingGoaway() {
  if (!pending_goaway_process_ || !HasLiveSession()) return;
  pending_goaway_process_ = false;

  // 1. Queued calls can never use this connection: fail-fast ones fail now,
  //    wait-for-ready ones wait for the replacement (§4.4).
  FailPendingCalls(StatusCode::kUnavailable, goaway_detail_, /*keep_wait_for_ready=*/true);

  // 2. Streams above last_stream_id were never processed by the server; fail
  //    them now rather than waiting out the drain. (nghttp2 normally already
  //    closed them with REFUSED_STREAM; this covers any it did not.)
  for (auto it = active_calls_.begin(); it != active_calls_.end();) {
    if (it->second->stream_id() <= goaway_last_stream_id_) {
      ++it;
      continue;
    }
    std::unique_ptr<CallState> call = std::move(it->second);
    it = active_calls_.erase(it);
    if (call->deadline_timer() != 0) loop_.CancelTimer(call->deadline_timer());
    if (call->stream_id() > 0) h2_->ResetStream(call->stream_id(), Http2Session::kHttp2Cancel);
    if (!call->TryPreemptTerminal()) call->FailLocal(StatusCode::kUnavailable, goaway_detail_);
  }

  // 3. Nothing left to drain: replace the connection now.
  if (active_calls_.empty()) {
    EnterTransientFailure(goaway_detail_);
    return;
  }

  // 4. Bounded drain of streams <= last_stream_id (armed once across
  //    repeated GOAWAY frames).
  if (goaway_drain_timer_ == 0) {
    goaway_drain_timer_ =
        loop_.AddTimer(std::max(options_.goaway_drain_timeout, milliseconds(0)), [this] {
          goaway_drain_timer_ = 0;
          if (HasLiveSession() && goaway_received_) {
            EnterTransientFailure("GOAWAY drain timeout exceeded");
          }
        });
  }
}

void ChannelImpl::FailPendingCall(PendingCall& pc, StatusCode code, const std::string& message) {
  if (pc.deadline_timer != 0) {
    loop_.CancelTimer(pc.deadline_timer);
    pc.deadline_timer = 0;
  }
  if (pc.completion->cancel_requested()) {
    CloseCompletionLocal(pc.completion, StatusCode::kCancelled, "Cancelled");
  } else if (pc.deadline.has_value() && steady_clock::now() >= *pc.deadline) {
    CloseCompletionLocal(pc.completion, StatusCode::kDeadlineExceeded, "Deadline Exceeded");
  } else {
    CloseCompletionLocal(pc.completion, code, message);
  }
}

void ChannelImpl::FailPendingCalls(StatusCode code, const std::string& message,
                                   bool keep_wait_for_ready) {
  auto pending = std::move(pending_calls_);
  pending_calls_.clear();
  for (PendingCall& pc : pending) {
    if (keep_wait_for_ready && pc.wait_for_ready) {
      pending_calls_.push_back(std::move(pc));
    } else {
      FailPendingCall(pc, code, message);
    }
  }
}

void ChannelImpl::FailActiveCalls(const std::string& message) {
  auto active = std::move(active_calls_);
  active_calls_.clear();
  for (auto& entry : active) {
    if (entry.second->deadline_timer() != 0) loop_.CancelTimer(entry.second->deadline_timer());
    if (!entry.second->TryPreemptTerminal()) {
      entry.second->FailLocal(StatusCode::kUnavailable, message);
    }
  }
}

void ChannelImpl::FailDrainingCalls(const std::string& message) {
  auto draining = std::move(draining_calls_);
  draining_calls_.clear();
  for (auto& entry : draining) {
    if (entry.second->deadline_timer() != 0) loop_.CancelTimer(entry.second->deadline_timer());
    entry.second->FailLocal(StatusCode::kUnavailable, message);
  }
}

void ChannelImpl::SubmitCall(PendingCall pc) {
  auto cancel_pending_timer = [&] {
    if (pc.deadline_timer != 0) {
      loop_.CancelTimer(pc.deadline_timer);
      pc.deadline_timer = 0;
    }
  };

  if (pc.completion->cancel_requested()) {
    cancel_pending_timer();
    CloseCompletionLocal(pc.completion, StatusCode::kCancelled, "Cancelled");
    return;
  }
  if (state_ != ConnectivityState::kReady || h2_ == nullptr || goaway_received_) {
    cancel_pending_timer();
    CloseCompletionLocal(pc.completion, StatusCode::kUnavailable, "channel is not ready");
    return;
  }

  // grpc-timeout is computed here, not at call entry: a call that queued
  // while the channel was connecting must tell the server the time it has
  // *left*, not the time it started with. An expired deadline fails locally
  // without touching the wire, matching upstream's fail-fast.
  std::optional<std::chrono::nanoseconds> timeout;
  if (pc.deadline.has_value()) {
    const auto now = steady_clock::now();
    if (*pc.deadline <= now) {
      cancel_pending_timer();
      CloseCompletionLocal(pc.completion, StatusCode::kDeadlineExceeded, "Deadline Exceeded");
      return;
    }
    timeout = std::chrono::duration_cast<std::chrono::nanoseconds>(*pc.deadline - now);
  }

  const uint64_t cid = pc.call_id;
  CallState::Hooks call_hooks;
  call_hooks.on_reassembly_bytes = [this](size_t bytes) {
    UpdateMax(max_reassembly_bytes_, bytes);
  };
  call_hooks.on_queue_depth = [this](size_t msgs, size_t bytes) {
    UpdateMax(max_queued_messages_, msgs);
    UpdateMax(max_queued_bytes_, bytes);
  };
  call_hooks.on_protocol_error = [this, cid] { pending_protocol_resets_.push_back(cid); };

  auto call = std::make_unique<CallState>(effective_max_receive_message_size_, pc.mode,
                                          pc.completion, options_.max_queued_messages,
                                          effective_max_queued_bytes_, std::move(call_hooks));
  call->set_deadline_timer(pc.deadline_timer);
  call->set_deadline(pc.deadline);
  pc.deadline_timer = 0;

  const Http2Session::HeaderList headers = BuildRequestHeaders(
      authority_, pc.method_path, timeout, pc.metadata, options_.user_agent_prefix);

  // StreamHooks look up the EventThread-owned CallState by call_id so a
  // local reset/cancel/protocol-error that already removed the CallState
  // makes any subsequent nghttp2 callback a safe no-op (§5.7).
  Http2Session::StreamHooks hooks;
  hooks.on_response_headers = [this, cid](Http2Session::HeaderList h, bool end_stream) {
    auto it = active_calls_.find(cid);
    if (it != active_calls_.end()) it->second->OnResponseHeaders(std::move(h), end_stream);
  };
  hooks.on_trailers = [this, cid](Http2Session::HeaderList t) {
    auto it = active_calls_.find(cid);
    if (it != active_calls_.end()) it->second->OnTrailers(std::move(t));
  };
  hooks.on_data = [this, cid](const uint8_t* data, size_t len) {
    auto it = active_calls_.find(cid);
    if (it != active_calls_.end()) {
      const size_t ignored = it->second->OnData(data, len);
      if (ignored > 0 && it->second->stream_id() > 0) {
        pending_ignored_consumes_.emplace_back(it->second->stream_id(), ignored);
      }
    }
  };
  auto finish_active_call = [this, cid](uint32_t error_code, bool from_end_stream) {
    auto it = active_calls_.find(cid);
    if (it == active_calls_.end()) {
      SchedulePumpPendingCalls();
      return;
    }
    std::unique_ptr<CallState> c = std::move(it->second);
    active_calls_.erase(it);
    const int32_t sid = c->stream_id();
    // The wire RPC has finished; cancel the deadline timer immediately even if
    // buffered messages are still draining locally.
    if (c->deadline_timer() != 0) {
      loop_.CancelTimer(c->deadline_timer());
      c->set_deadline_timer(0);
    }
    // Resolve local cancel / expired deadline precedence at the terminal
    // transition so a late response arriving in the same drain turn cannot
    // beat an already-requested TryCancel() or elapsed local deadline (§5.6, §5.7).
    if (!c->TryPreemptTerminal()) {
      c->OnClose(error_code);
      if (c->is_draining()) {
        draining_calls_.emplace(cid, std::move(c));
      }
    }
    if (from_end_stream && h2_ != nullptr && sid > 0 && h2_->IsStreamLocalOpen(sid)) {
      // Server sent response END_STREAM while our request upload is still open
      // (e.g. zero request-body window + trailers-only error). Queue
      // RST_STREAM(CANCEL) once ReceiveBytes() unwinds to release the stream.
      pending_stream_resets_.push_back(sid);
    }
    // Deferred (we are inside an nghttp2 callback): admit queued calls into
    // the freed slot, finish a GOAWAY drain, and re-arm keepalive for the
    // 1 → 0 active-stream transition.
    SchedulePumpPendingCalls();
  };
  hooks.on_response_end_stream = [finish_active_call]() {
    finish_active_call(/*error_code=*/0, /*from_end_stream=*/true);
  };
  hooks.on_close = [finish_active_call](uint32_t error_code) {
    finish_active_call(error_code, /*from_end_stream=*/false);
  };

  const int32_t stream_id =
      (pc.mode == CallMode::kServerStreaming)
          ? h2_->SubmitServerStreamRequest(headers, std::move(pc.framed_body), std::move(hooks))
          : h2_->SubmitUnaryRequest(headers, std::move(pc.framed_body), std::move(hooks));
  if (stream_id < 0) {
    if (call->deadline_timer() != 0) loop_.CancelTimer(call->deadline_timer());
    call->FailLocal(StatusCode::kUnavailable, h2_->call_error());
    return;
  }
  call->OnSubmitted(stream_id);
  active_calls_.emplace(cid, std::move(call));
  UpdateMax(max_active_calls_, active_calls_.size());
}

void ChannelImpl::FlushOutput() {
  if (tls_read_wants_write_) return;
  // Bound writes per event-loop turn (mirrors DrainSocket()'s read bound) so
  // a huge outbound payload cannot starve Shutdown, Cancel, or deadline
  // timers; the remainder is flushed by a next-turn continuation.
  constexpr size_t kMaxWriteBytesPerTurn = 256 * 1024;
  size_t bytes_written_this_turn = 0;
  while (HasLiveSession()) {
    // Shutdown() is pending: let DoShutdown() run without delay.
    if (shutting_down_.load(std::memory_order_acquire)) return;
    if (bytes_written_this_turn >= kMaxWriteBytesPerTurn) {
      // Budget spent: do not refill h2_'s output buffer this turn.
      if (!h2_->WantWrite()) {
        if (!h2_->error().empty()) {
          EnterTransientFailure(h2_->error());
        } else if (!h2_->WantRead()) {
          EnterTransientFailure(goaway_received_ ? goaway_detail_ : "HTTP/2 session terminated");
        }
        return;
      }
      // Resume after queued ops and due timers have run.
      if (!write_yield_pending_) {
        write_yield_pending_ = true;
        loop_.ScheduleNextTurn([this] {
          if (!write_yield_pending_) return;
          write_yield_pending_ = false;
          if (!HasLiveSession() || tls_write_pending_) return;
          FlushOutput();
          RearmKeepalive();
        });
      }
      return;
    }
    size_t len = 0;
    const uint8_t* data = h2_->PendingOutput(&len);
    if (data == nullptr || len == 0) {
      tls_write_pending_ = false;
      tls_write_wants_read_ = false;
      if (!h2_->error().empty()) {
        EnterTransientFailure(h2_->error());
      } else if (!h2_->WantRead() && !h2_->WantWrite()) {
        EnterTransientFailure(goaway_received_ ? goaway_detail_ : "HTTP/2 session terminated");
      }
      return;
    }
    size_t written = 0;
    const TlsSocket::IoStatus st = sock_->Write(data, len, &written);
    if (st == TlsSocket::IoStatus::kOk) {
      tls_write_pending_ = false;
      tls_write_wants_read_ = false;
      h2_->ConsumeOutput(written);
      bytes_written_this_turn += written;
      continue;
    }
    if (st == TlsSocket::IoStatus::kWantWrite) {
      tls_write_pending_ = true;
      tls_write_wants_read_ = false;
      return;  // poll re-arms POLLOUT only until SSL_write_ex completes
    }
    if (st == TlsSocket::IoStatus::kWantRead) {
      tls_write_pending_ = true;
      tls_write_wants_read_ = true;
      return;  // wait for POLLIN and retry SSL_write_ex first
    }
    EnterTransientFailure(sock_->error().empty() ? "socket write failed" : sock_->error());
    return;
  }
}

void ChannelImpl::DrainSocket() {
  // Bound reads per event-loop turn (256 KiB) so sustained inbound traffic
  // cannot starve Cancel, ReadConsumed, Shutdown, or deadline timers. If more
  // data may remain (including in OpenSSL's SSL_pending buffer), schedule a
  // next-turn continuation so queued ops and due timers run first.
  constexpr int kMaxChunksPerTurn = 16;
  uint8_t buf[16384];
  for (int i = 0; i < kMaxChunksPerTurn && HasLiveSession(); ++i) {
    size_t n = 0;
    const TlsSocket::IoStatus st = sock_->Read(buf, sizeof(buf), &n);
    if (st == TlsSocket::IoStatus::kOk) {
      tls_read_wants_write_ = false;
      if (!h2_->ReceiveBytes(steady_clock::now(), buf, n)) {
        EnterTransientFailure(h2_->error());
        return;
      }
      if (!h2_->WantRead() && !h2_->WantWrite()) {
        // Nothing left to exchange on this connection (e.g. GOAWAY with no
        // streams left to drain): replace it.
        EnterTransientFailure(goaway_received_ ? goaway_detail_ : "HTTP/2 session terminated");
        return;
      }
      if (!pending_ignored_consumes_.empty()) {
        ProcessPendingIgnoredConsumes();
        if (!HasLiveSession() || tls_write_pending_) return;
      }
      continue;
    }
    if (st == TlsSocket::IoStatus::kWantRead) {
      tls_read_wants_write_ = false;
      return;
    }
    if (st == TlsSocket::IoStatus::kWantWrite) {
      tls_read_wants_write_ = true;
      return;  // poll arms POLLOUT and retries DrainSocket() on readiness
    }
    if (st == TlsSocket::IoStatus::kEof) {
      EnterTransientFailure("connection closed by server");
      return;
    }
    EnterTransientFailure(sock_->error().empty() ? "socket read failed" : sock_->error());
    return;
  }
  if (HasLiveSession() && !read_yield_pending_) {
    read_yield_pending_ = true;
    loop_.ScheduleNextTurn([this] {
      if (!read_yield_pending_) return;
      read_yield_pending_ = false;
      if (HasLiveSession()) OnSocketReady(POLLIN);
    });
  }
}

void ChannelImpl::RearmKeepalive() {
  if (state_ != ConnectivityState::kReady || h2_ == nullptr) return;
  if (keepalive_timer_ != 0) {
    loop_.CancelTimer(keepalive_timer_);
    keepalive_timer_ = 0;
  }
  const auto now = steady_clock::now();
  const auto deadline = h2_->NextKeepaliveDeadline(now);
  if (!deadline) return;
  const auto delay = *deadline > now ? *deadline - now : steady_clock::duration::zero();
  keepalive_timer_ = loop_.AddTimer(delay, [this] {
    keepalive_timer_ = 0;
    if (state_ != ConnectivityState::kReady || h2_ == nullptr) return;
    // §5.2: runs with or without calls in flight (permit_without_calls).
    if (h2_->CheckKeepalive(steady_clock::now()) == Http2Session::KeepaliveAction::kDead) {
      EnterTransientFailure("keepalive timeout: no response from peer within keepalive_timeout");
      return;
    }
    FlushOutput();  // a queued PING must go out even if the loop is idle
    RearmKeepalive();
  });
}

void ChannelImpl::EnterTransientFailure(std::string detail) {
  if (state_ == ConnectivityState::kShutdown) return;
  SetState(ConnectivityState::kTransientFailure);
  last_failure_detail_ = std::move(detail);

  // Captured before CancelTimersAndUnwatch() clears the flags: an SSL_read_ex
  // or SSL_write_ex awaiting retry forbids SSL_shutdown (OpenSSL's
  // pending-operation contract), so such a socket is closed abortively.
  const bool ssl_op_pending = tls_read_wants_write_ || tls_write_pending_;
  CancelTimersAndUnwatch();
  if (resolving_dns_) {
    resolver_.Invalidate();
    resolving_dns_ = false;
  }
  goaway_received_ = false;
  pending_goaway_process_ = false;
  goaway_too_many_pings_applied_ = false;
  goaway_last_stream_id_ = std::numeric_limits<int32_t>::max();
  pending_ignored_consumes_.clear();
  pending_protocol_resets_.clear();
  pending_stream_resets_.clear();
  // Safe: never reached from inside an Http2Session callback — ReceiveBytes /
  // PendingOutput report failure via return value first, and GOAWAY handling
  // is deferred to ProcessPendingGoaway().
  h2_.reset();
  if (sock_ != nullptr) {
    sock_->Close(/*graceful=*/!ssl_op_pending);
    sock_.reset();
  }

  FailActiveCalls(last_failure_detail_);
  // Wire-complete draining_calls_ already received all bytes and trailers;
  // they keep draining locally so a peer closing the socket right after
  // END_STREAM cannot clobber a completed RPC.
  FailPendingCalls(StatusCode::kUnavailable, last_failure_detail_,
                   /*keep_wait_for_ready=*/true);

  const Backoff::Step step = backoff_.OnFailure(steady_clock::now());
  reconnect_timer_ = loop_.AddTimer(step.delay, [this] {
    reconnect_timer_ = 0;
    if (state_ == ConnectivityState::kTransientFailure) StartConnecting();
  });
}

void ChannelImpl::FatalLoopExit(std::string detail) {
  SetState(ConnectivityState::kShutdown);
  last_failure_detail_ = std::move(detail);
  {
    std::lock_guard<std::mutex> lock(terminal_error_mu_);
    terminal_error_ = last_failure_detail_;
  }
  const bool ssl_op_pending = tls_read_wants_write_ || tls_write_pending_;
  CancelTimersAndUnwatch();
  resolving_dns_ = false;
  resolver_.Shutdown(std::chrono::milliseconds(0));
  h2_.reset();
  if (sock_ != nullptr) {
    sock_->Close(/*graceful=*/!ssl_op_pending);
    sock_.reset();
  }
  pending_ignored_consumes_.clear();
  pending_protocol_resets_.clear();
  pending_stream_resets_.clear();
  // The EventThread is exiting: nothing can drive draining calls further, so
  // fail them too rather than silently truncating unread messages with OK.
  FailActiveCalls(last_failure_detail_);
  FailDrainingCalls(last_failure_detail_);
  FailPendingCalls(StatusCode::kUnavailable, last_failure_detail_);
}

void ChannelImpl::CancelTimersAndUnwatch() {
  for (TimerId* t : {&connect_timer_, &keepalive_timer_, &reconnect_timer_, &goaway_drain_timer_}) {
    if (*t != 0) {
      loop_.CancelTimer(*t);
      *t = 0;
    }
  }
  read_yield_pending_ = false;
  write_yield_pending_ = false;
  pump_scheduled_ = false;
  tls_read_wants_write_ = false;
  tls_write_pending_ = false;
  tls_write_wants_read_ = false;
  loop_.UnwatchFd();
}

void ChannelImpl::DoShutdown() {
  // Only attempt the best-effort GOAWAY write if no OpenSSL read/write retry
  // is in flight (honoring OpenSSL's pending-operation contract).
  bool ssl_op_pending = tls_read_wants_write_ || tls_write_pending_;
  const bool can_send_goaway =
      HasLiveSession() && sock_->state() == TlsSocket::State::kConnected && !ssl_op_pending;
  SetState(ConnectivityState::kShutdown);
  DrainCancelQueue();  // settle already-requested cancels as CANCELLED
  ClearCancelQueue();
  CancelTimersAndUnwatch();

  const milliseconds budget = std::max(options_.shutdown_timeout, milliseconds(0));
  // Shutdown() stamped the absolute deadline before queuing us; ops that ran
  // ahead of DoShutdown() already consumed part of the budget.
  const int64_t deadline_ticks = shutdown_deadline_ns_.load(std::memory_order_acquire);
  const auto shutdown_deadline =
      deadline_ticks != 0 ? steady_clock::time_point(steady_clock::duration(deadline_ticks))
                          : steady_clock::now() + budget;

  // §5.7: unblock every waiter first, before any socket/DNS teardown.
  FailActiveCalls(kShutDownMessage);
  // Draining calls still hold undelivered messages in EventThread-owned
  // scanner_; because the EventThread is exiting, fail them with UNAVAILABLE
  // rather than silently truncating unread messages with Status::OK.
  FailDrainingCalls(kShutDownMessage);
  FailPendingCalls(StatusCode::kUnavailable, kShutDownMessage);

  // Never block on a hung getaddrinfo(): join an idle worker within a short
  // grace, detach a mid-resolve one (§4.1).
  resolving_dns_ = false;
  const auto now = steady_clock::now();
  const milliseconds remaining =
      shutdown_deadline > now ? std::chrono::duration_cast<milliseconds>(shutdown_deadline - now)
                              : milliseconds(0);
  const milliseconds sub_budget =
      std::max(std::min({milliseconds(50), budget / 4, remaining}), milliseconds(0));
  resolver_.Shutdown(sub_budget);

  if (can_send_goaway) {
    // Graceful close: GOAWAY(NO_ERROR) + flush bounded by a short sub-budget
    // (best-effort; no waiting for the peer).
    const auto close_deadline = std::min(shutdown_deadline, steady_clock::now() + sub_budget);
    h2_->Terminate();
    for (;;) {
      // Checked every iteration (also after kOk writes) so a peer that keeps
      // accepting bytes cannot stretch the flush past its budget.
      if (steady_clock::now() >= close_deadline) break;
      size_t len = 0;
      const uint8_t* data = h2_->PendingOutput(&len);
      if (data == nullptr || len == 0) break;
      size_t written = 0;
      const TlsSocket::IoStatus st = sock_->Write(data, len, &written);
      if (st == TlsSocket::IoStatus::kOk) {
        ssl_op_pending = false;
        h2_->ConsumeOutput(written);
        continue;
      }
      if (st != TlsSocket::IoStatus::kWantWrite && st != TlsSocket::IoStatus::kWantRead) break;
      // SSL_write_ex must be retried before any SSL_shutdown; if we give up
      // here the close has to be abortive.
      ssl_op_pending = true;
      const int rem_ms = RemainingMs(close_deadline);
      if (rem_ms <= 0) break;
      pollfd pfd{};
      pfd.fd = sock_->fd();
      pfd.events = st == TlsSocket::IoStatus::kWantWrite ? POLLOUT : POLLIN;
      if (poll(&pfd, 1, rem_ms) <= 0) break;
    }
  }
  h2_.reset();
  if (sock_ != nullptr) {
    sock_->Close(/*graceful=*/!ssl_op_pending);
    sock_.reset();
  }
  pending_ignored_consumes_.clear();
  pending_protocol_resets_.clear();
  pending_stream_resets_.clear();
}

}  // namespace internal
}  // namespace egrpc
