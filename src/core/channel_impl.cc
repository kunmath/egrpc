// egrpc — channel over one EventThread-owned transport (design §4.4, §5.4,
// §5.6, §5.7). Reconnect/backoff lands in M6; transport failure is terminal
// until then.
#include "core/channel_impl.h"

#include <poll.h>

#include <algorithm>
#include <climits>
#include <utility>

namespace egrpc {
namespace internal {

namespace {

using std::chrono::steady_clock;

constexpr size_t kMaxHttp2Window = static_cast<size_t>(INT32_MAX);

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
      options_(options),
      effective_max_receive_message_size_(
          ClampMaxReceiveMessageSize(options_.max_receive_message_size)),
      stream_window_(
          ComputeStreamWindow(effective_max_receive_message_size_, options_.stream_window_slack)),
      effective_max_queued_bytes_(options_.max_queued_bytes == 0 ? stream_window_
                                                                 : options_.max_queued_bytes) {
  loop_.SetWakeupCallback([this] { DrainCancelQueue(); });
  loop_.SetFatalExitCallback([this] {
    ClearCancelQueue();
    TearDown("event loop poll() error", /*finalize_draining=*/true);
  });
  loop_ok_ = loop_.Start();
  if (!loop_ok_) {
    state_ = State::kFailed;  // no thread running: safe to write from here
    failure_detail_ = "failed to start channel event thread";
    terminal_error_ = failure_detail_;
  }
}

ChannelImpl::~ChannelImpl() { Shutdown(); }

ChannelImpl::CallHandle ChannelImpl::StartCall(
    CallMode mode, const std::string& method_path, std::string request,
    Http2Session::HeaderList metadata,
    std::optional<std::chrono::steady_clock::time_point> deadline, uint64_t preallocated_id,
    std::shared_ptr<CallCompletion> prebound_completion) {
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

  if (!loop_ok_ || !loop_.Post([this, pc] { HandleNewCall(std::move(*pc)); })) {
    std::string detail;
    {
      std::lock_guard<std::mutex> lock(terminal_error_mu_);
      detail = terminal_error_;
    }
    CloseCompletionLocal(completion, StatusCode::kUnavailable,
                         detail.empty() ? "channel is shut down" : std::move(detail));
  }
  return {call_id, std::move(completion)};
}

CallCompletion::Result ChannelImpl::UnaryCall(
    const std::string& method_path, std::string request, Http2Session::HeaderList metadata,
    std::optional<std::chrono::steady_clock::time_point> deadline) {
  CallHandle handle =
      StartCall(CallMode::kUnary, method_path, std::move(request), std::move(metadata), deadline);
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

  if (!loop_ok_) return;  // loop never ran; nothing to drain or join

  // Atomically closes the EventThread Post() gate and appends DoShutdown() as
  // the last op before thread exit, preventing any call from slipping in after
  // DoShutdown() and avoiding deadlock if poll() exited early.
  loop_.Stop([this] { DoShutdown(); });
}

// --- Event-thread only -------------------------------------------------------

void ChannelImpl::HandleNewCall(PendingCall pc) {
  if (state_ == State::kShutdown) {
    CloseCompletionLocal(pc.completion, StatusCode::kUnavailable, "channel is shut down");
    return;
  }
  if (pc.completion->cancel_requested()) {
    CloseCompletionLocal(pc.completion, StatusCode::kCancelled, "Cancelled");
    return;
  }
  if (pc.deadline.has_value() && *pc.deadline <= steady_clock::now()) {
    CloseCompletionLocal(pc.completion, StatusCode::kDeadlineExceeded, "Deadline Exceeded");
    return;
  }
  if (state_ == State::kFailed) {
    CloseCompletionLocal(pc.completion, StatusCode::kUnavailable, failure_detail_);
    return;
  }

  // Local deadline timer (§5.6): armed at admission so even calls queued in
  // pending_calls_ (connecting or at concurrent-stream cap) fail on time.
  if (pc.deadline.has_value()) {
    const auto now = steady_clock::now();
    if (*pc.deadline <= now) {
      CloseCompletionLocal(pc.completion, StatusCode::kDeadlineExceeded, "Deadline Exceeded");
      return;
    }
    const uint64_t cid = pc.call_id;
    pc.deadline_timer = loop_.AddTimer(*pc.deadline - now, [this, cid] { OnCallDeadline(cid); });
  }

  switch (state_) {
    case State::kIdle:
      pending_calls_.push_back(std::move(pc));
      StartConnecting();
      return;
    case State::kConnecting:
      pending_calls_.push_back(std::move(pc));
      return;
    case State::kReady:
      if (h2_ != nullptr && h2_->draining()) {
        if (pc.deadline_timer != 0) loop_.CancelTimer(pc.deadline_timer);
        CloseCompletionLocal(pc.completion, StatusCode::kUnavailable,
                             "server sent GOAWAY; no new streams");
        return;
      }
      if (active_calls_.size() >= MaxConcurrentCalls()) {
        pending_calls_.push_back(std::move(pc));  // resumes in PumpPendingCalls
        return;
      }
      SubmitCall(std::move(pc));
      FlushOutput();
      return;
    case State::kFailed:
    case State::kShutdown:
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
    if (state_ == State::kReady && h2_ != nullptr && sid > 0) {
      h2_->ResetStream(sid, Http2Session::kHttp2Cancel);
      FlushOutput();
    }
    call->FailLocal(code, message);
    if (state_ == State::kReady) {
      PumpPendingCalls();
      FlushOutput();
    }
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
    if (state_ == State::kReady && h2_ != nullptr && call->stream_id() > 0 && wire_bytes > 0) {
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
  if (h2_ != nullptr && h2_->draining()) return;
  while (state_ == State::kReady && !pending_calls_.empty() &&
         active_calls_.size() < MaxConcurrentCalls()) {
    PendingCall pc = std::move(pending_calls_.front());
    pending_calls_.pop_front();
    SubmitCall(std::move(pc));
  }
}

void ChannelImpl::SchedulePumpPendingCalls() {
  if (pump_scheduled_ || state_ != State::kReady) return;
  pump_scheduled_ = true;
  loop_.ScheduleNextTurn([this] {
    if (!pump_scheduled_) return;
    pump_scheduled_ = false;
    if (state_ == State::kReady) {
      PumpPendingCalls();
      if (state_ == State::kReady) FlushOutput();
    }
  });
}

void ChannelImpl::StartConnecting() {
  state_ = State::kConnecting;
  connect_timer_ = loop_.AddTimer(options_.connect_timeout, [this] {
    connect_timer_ = 0;
    TearDown(h2_ != nullptr ? "timed out waiting for peer HTTP/2 SETTINGS" : "connect timeout");
  });
  if (!sock_.StartConnect(host_, port_, tls_)) {
    TearDown(sock_.error());
    return;
  }
  loop_.WatchFd(
      sock_.fd(), [this] { return sock_.DesiredPollEvents(); },
      [this](short) { OnConnectProgress(); });
}

void ChannelImpl::OnConnectProgress() {
  const TlsSocket::IoStatus st = sock_.ContinueConnect();
  if (st == TlsSocket::IoStatus::kWantRead || st == TlsSocket::IoStatus::kWantWrite) return;
  if (st == TlsSocket::IoStatus::kOk) {
    // Keep connect_timer_ armed until the server's initial HTTP/2 SETTINGS
    // frame arrives (cancelled in hooks.on_remote_settings) so a peer that
    // completes TLS/ALPN but never speaks HTTP/2 still times out.
    StartSession();
  } else {
    if (connect_timer_ != 0) {
      loop_.CancelTimer(connect_timer_);
      connect_timer_ = 0;
    }
    TearDown(sock_.error());
  }
}

void ChannelImpl::StartSession() {
  Http2Session::Hooks hooks;
  hooks.on_remote_settings = [this] {
    if (connect_timer_ != 0) {
      loop_.CancelTimer(connect_timer_);
      connect_timer_ = 0;
    }
  };
  // GOAWAY (design §4.2): the session marks itself draining — in-flight
  // calls run to completion, while queued and new calls fail immediately with
  // UNAVAILABLE. Reconnect-on-GOAWAY is M6.
  hooks.on_goaway = [this](uint32_t, int32_t, bool) {
    FailPendingCalls(StatusCode::kUnavailable, "server sent GOAWAY; no new streams");
  };

  Http2SessionOptions h2_opts = options_.http2;
  // §5.4: derived stream window, never smaller than max_receive_message_size + 5.
  h2_opts.initial_window_size = stream_window_;

  h2_ = std::make_unique<Http2Session>(h2_opts, std::move(hooks), steady_clock::now());
  if (!h2_->ok()) {
    TearDown(h2_->error());
    return;
  }

  loop_.WatchFd(
      sock_.fd(),
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
        if (h2_->WantWrite()) events |= POLLOUT;
        return events;
      },
      [this](short revents) { OnSocketReady(revents); });

  state_ = State::kReady;
  FlushOutput();  // connection preface + SETTINGS
  RearmKeepalive();

  // Pending calls stay queued until server SETTINGS arrives (or submit
  // immediately if already received) via PumpPendingCalls.
  PumpPendingCalls();
  if (state_ == State::kReady) FlushOutput();
}

void ChannelImpl::OnSocketReady(short revents) {
  // Retry an interrupted OpenSSL write before issuing any SSL_read_ex.
  if (tls_write_pending_) {
    const bool write_ready =
        (revents & (POLLERR | POLLHUP)) != 0 ||
        (tls_write_wants_read_ ? (revents & POLLIN) != 0 : (revents & POLLOUT) != 0);
    if (write_ready) FlushOutput();
    if (state_ != State::kReady) return;
    if (tls_write_pending_) {
      RearmKeepalive();
      return;
    }
  }

  const bool want_read = tls_read_wants_write_ ? (revents & (POLLOUT | POLLERR | POLLHUP)) != 0
                                               : (revents & (POLLIN | POLLERR | POLLHUP)) != 0;
  if (want_read) DrainSocket();
  if (state_ == State::kReady) ProcessPendingIgnoredConsumes();
  if (state_ == State::kReady) ProcessPendingProtocolResets();
  if (state_ == State::kReady) ProcessPendingStreamResets();
  // Stream closes, protocol resets, and server SETTINGS can open admission
  // slots. Safe here: all Http2Session callbacks have unwound.
  if (state_ == State::kReady) PumpPendingCalls();
  if (state_ == State::kReady && !tls_read_wants_write_) FlushOutput();
  if (state_ == State::kReady) RearmKeepalive();
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
  if (state_ == State::kReady && !tls_read_wants_write_) FlushOutput();
}

void ChannelImpl::FailPendingCalls(StatusCode code, const std::string& message) {
  auto pending = std::move(pending_calls_);
  pending_calls_.clear();
  const auto now = steady_clock::now();
  for (PendingCall& pc : pending) {
    if (pc.deadline_timer != 0) loop_.CancelTimer(pc.deadline_timer);
    if (pc.completion->cancel_requested()) {
      CloseCompletionLocal(pc.completion, StatusCode::kCancelled, "Cancelled");
    } else if (pc.deadline.has_value() && now >= *pc.deadline) {
      CloseCompletionLocal(pc.completion, StatusCode::kDeadlineExceeded, "Deadline Exceeded");
    } else {
      CloseCompletionLocal(pc.completion, code, message);
    }
  }
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
  if (any_reset && state_ == State::kReady && !tls_read_wants_write_) {
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
  if (any_reset && state_ == State::kReady && !tls_read_wants_write_) {
    FlushOutput();
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
  if (state_ != State::kReady || h2_ == nullptr) {
    cancel_pending_timer();
    CloseCompletionLocal(pc.completion, StatusCode::kUnavailable,
                         state_ == State::kFailed ? failure_detail_ : "channel is not ready");
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
      if (!pending_calls_.empty()) SchedulePumpPendingCalls();
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
    if (!pending_calls_.empty()) SchedulePumpPendingCalls();
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
  while (state_ == State::kReady) {
    size_t len = 0;
    const uint8_t* data = h2_->PendingOutput(&len);
    if (data == nullptr || len == 0) {
      tls_write_pending_ = false;
      tls_write_wants_read_ = false;
      if (!h2_->error().empty()) {
        TearDown(h2_->error());
      } else if (!h2_->WantRead() && !h2_->WantWrite()) {
        TearDown(h2_->draining() ? "server sent GOAWAY; session closed"
                                 : "HTTP/2 session terminated");
      }
      return;
    }
    size_t written = 0;
    const TlsSocket::IoStatus st = sock_.Write(data, len, &written);
    if (st == TlsSocket::IoStatus::kOk) {
      tls_write_pending_ = false;
      tls_write_wants_read_ = false;
      h2_->ConsumeOutput(written);
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
    TearDown(sock_.error());
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
  for (int i = 0; i < kMaxChunksPerTurn && state_ == State::kReady; ++i) {
    size_t n = 0;
    const TlsSocket::IoStatus st = sock_.Read(buf, sizeof(buf), &n);
    if (st == TlsSocket::IoStatus::kOk) {
      tls_read_wants_write_ = false;
      if (!h2_->ReceiveBytes(steady_clock::now(), buf, n)) {
        TearDown(h2_->error());
        return;
      }
      if (!h2_->WantRead() && !h2_->WantWrite()) {
        TearDown(h2_->draining() ? "server sent GOAWAY; session closed"
                                 : "HTTP/2 session terminated");
        return;
      }
      if (!pending_ignored_consumes_.empty()) {
        ProcessPendingIgnoredConsumes();
        if (state_ != State::kReady || tls_write_pending_) return;
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
      TearDown("connection closed by server");
      return;
    }
    TearDown(sock_.error());
    return;
  }
  if (state_ == State::kReady && !read_yield_pending_) {
    read_yield_pending_ = true;
    loop_.ScheduleNextTurn([this] {
      if (!read_yield_pending_) return;
      read_yield_pending_ = false;
      if (state_ == State::kReady) OnSocketReady(POLLIN);
    });
  }
}

void ChannelImpl::RearmKeepalive() {
  if (state_ != State::kReady || h2_ == nullptr) return;
  if (keepalive_timer_ != 0) {
    loop_.CancelTimer(keepalive_timer_);
    keepalive_timer_ = 0;
  }
  const auto deadline = h2_->NextKeepaliveDeadline(steady_clock::now());
  if (!deadline) return;
  keepalive_timer_ = loop_.AddTimer(*deadline - steady_clock::now(), [this] {
    keepalive_timer_ = 0;
    if (state_ != State::kReady || h2_ == nullptr) return;
    switch (h2_->CheckKeepalive(steady_clock::now())) {
      case Http2Session::KeepaliveAction::kDead:
        TearDown("keepalive timeout: no reads from server within keepalive_timeout");
        return;
      case Http2Session::KeepaliveAction::kPingSent:
      case Http2Session::KeepaliveAction::kNone:
        break;
    }
    FlushOutput();  // a queued PING must go out even if the loop is idle
    RearmKeepalive();
  });
}

void ChannelImpl::TearDown(std::string detail, bool finalize_draining) {
  if (state_ == State::kFailed || state_ == State::kShutdown) {
    if (finalize_draining) {
      auto draining = std::move(draining_calls_);
      draining_calls_.clear();
      for (auto& entry : draining) {
        if (entry.second->deadline_timer() != 0) loop_.CancelTimer(entry.second->deadline_timer());
        entry.second->FailLocal(StatusCode::kUnavailable, detail);
      }
    }
    return;
  }
  state_ = State::kFailed;
  failure_detail_ = std::move(detail);
  {
    std::lock_guard<std::mutex> lock(terminal_error_mu_);
    terminal_error_ = failure_detail_;
  }

  CancelTimersAndUnwatch();
  sock_.Close();
  // Safe: TearDown is never reached from inside an Http2Session callback —
  // ReceiveBytes/PendingOutput report failure via return value first.
  h2_.reset();
  pending_ignored_consumes_.clear();
  pending_protocol_resets_.clear();
  pending_stream_resets_.clear();

  auto active = std::move(active_calls_);
  active_calls_.clear();
  for (auto& entry : active) {
    if (entry.second->deadline_timer() != 0) loop_.CancelTimer(entry.second->deadline_timer());
    if (!entry.second->TryPreemptTerminal()) {
      entry.second->FailLocal(StatusCode::kUnavailable, failure_detail_);
    }
  }
  // Wire-complete draining_calls_ already received all bytes and trailers on
  // the wire; leave them draining locally on transport teardown so a peer
  // closing the socket right after END_STREAM cannot clobber a completed RPC.
  // Only fail them with kUnavailable if the EventThread itself is exiting on a
  // fatal poll() error (so undelivered scanner bytes are never silently
  // truncated with OK).
  if (finalize_draining) {
    auto draining = std::move(draining_calls_);
    draining_calls_.clear();
    for (auto& entry : draining) {
      if (entry.second->deadline_timer() != 0) loop_.CancelTimer(entry.second->deadline_timer());
      entry.second->FailLocal(StatusCode::kUnavailable, failure_detail_);
    }
  }
  FailPendingCalls(StatusCode::kUnavailable, failure_detail_);
}

void ChannelImpl::CancelTimersAndUnwatch() {
  for (TimerId* t : {&connect_timer_, &keepalive_timer_}) {
    if (*t != 0) {
      loop_.CancelTimer(*t);
      *t = 0;
    }
  }
  read_yield_pending_ = false;
  pump_scheduled_ = false;
  tls_read_wants_write_ = false;
  tls_write_pending_ = false;
  tls_write_wants_read_ = false;
  loop_.UnwatchFd();
}

void ChannelImpl::DoShutdown() {
  if (state_ == State::kShutdown) return;
  // Only attempt best-effort GOAWAY write if no OpenSSL read/write retry is
  // currently in flight (honoring OpenSSL's pending-operation contract).
  const bool can_send_goaway =
      state_ == State::kReady && h2_ != nullptr && !tls_read_wants_write_ && !tls_write_pending_;
  state_ = State::kShutdown;
  ClearCancelQueue();
  pending_ignored_consumes_.clear();
  pending_protocol_resets_.clear();
  pending_stream_resets_.clear();

  // §5.7: unblock every waiter first.
  auto active = std::move(active_calls_);
  active_calls_.clear();
  for (auto& entry : active) {
    if (entry.second->deadline_timer() != 0) loop_.CancelTimer(entry.second->deadline_timer());
    if (!entry.second->TryPreemptTerminal()) {
      entry.second->FailLocal(StatusCode::kUnavailable, "channel is shut down");
    }
  }
  // Draining calls still hold undelivered messages in EventThread-owned
  // scanner_; because the EventThread is exiting, fail them with UNAVAILABLE
  // (§5.7) rather than silently truncating unread messages with Status::OK.
  auto draining = std::move(draining_calls_);
  draining_calls_.clear();
  for (auto& entry : draining) {
    if (entry.second->deadline_timer() != 0) loop_.CancelTimer(entry.second->deadline_timer());
    entry.second->FailLocal(StatusCode::kUnavailable, "channel is shut down");
  }
  FailPendingCalls(StatusCode::kUnavailable, "channel is shut down");

  CancelTimersAndUnwatch();

  if (can_send_goaway) {
    // Graceful close: GOAWAY(NO_ERROR) + one non-blocking flush pass (§5.7 —
    // best-effort, bounded; no waiting for the peer).
    h2_->Terminate();
    for (;;) {
      size_t len = 0;
      const uint8_t* data = h2_->PendingOutput(&len);
      if (data == nullptr || len == 0) break;
      size_t written = 0;
      if (sock_.Write(data, len, &written) != TlsSocket::IoStatus::kOk) break;
      h2_->ConsumeOutput(written);
    }
  }
  sock_.Close();
  h2_.reset();
}

}  // namespace internal
}  // namespace egrpc
