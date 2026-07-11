// egrpc — channel over one EventThread-owned transport (design §4.4, M3
// subset: no reconnect; transport failure is terminal until M6).
#include "core/channel_impl.h"

#include <poll.h>

#include <future>
#include <utility>

namespace egrpc {
namespace internal {

using std::chrono::steady_clock;

ChannelImpl::ChannelImpl(std::string host, uint16_t port, TlsConfig tls, ChannelOptions options)
    : host_(std::move(host)),
      port_(port),
      // IPv6 literals must be re-bracketed in :authority (§5.1); host_ itself
      // stays bare for the resolver and SNI.
      authority_((host_.find(':') == std::string::npos ? host_ : "[" + host_ + "]") + ":" +
                 std::to_string(port)),
      tls_(std::move(tls)),
      options_(options) {
  loop_ok_ = loop_.Start();
  if (!loop_ok_) {
    state_ = State::kFailed;  // no thread running: safe to write from here
    failure_detail_ = "failed to start channel event thread";
  }
}

ChannelImpl::~ChannelImpl() { Shutdown(); }

CallState::Result ChannelImpl::UnaryCall(
    const std::string& method_path, std::string request, Http2Session::HeaderList metadata,
    std::optional<std::chrono::steady_clock::time_point> deadline) {
  // Send-path bound (§4.5): checked before anything is queued, so the
  // caller gets kResourceExhausted without a wire exchange, like upstream.
  if (request.size() > options_.max_send_message_size) {
    CallState::Result result;
    result.code = StatusCode::kResourceExhausted;
    result.message = "Sent message larger than max (" + std::to_string(request.size()) + " vs. " +
                     std::to_string(options_.max_send_message_size) + ")";
    return result;
  }

  auto call = std::make_shared<CallState>(options_.max_receive_message_size);

  // shared_ptr wrapper: std::function requires copyable callables, and the
  // body is worth not copying.
  auto pc = std::make_shared<PendingCall>();
  pc->call = call;
  pc->method_path = method_path;
  AppendGrpcFramePrefix(request.size(), &pc->framed_body);
  pc->framed_body += request;
  pc->metadata = std::move(metadata);
  pc->deadline = deadline;

  if (!loop_ok_ || !loop_.Post([this, pc] { HandleNewCall(std::move(*pc)); })) {
    // Off-event-thread FailLocal is safe only here: the op never reached the
    // loop, so this thread still exclusively owns the CallState (§3).
    call->FailLocal(StatusCode::kUnavailable, loop_ok_ ? "channel is shut down" : failure_detail_);
  }
  return call->Wait();
}

void ChannelImpl::Shutdown() {
  std::lock_guard<std::mutex> lock(shutdown_mu_);
  if (shutdown_done_) return;
  shutdown_done_ = true;

  if (!loop_ok_) return;  // loop never ran; nothing to drain or join

  std::promise<void> done;
  auto finished = done.get_future();
  if (loop_.Post([this, &done] {
        DoShutdown();
        done.set_value();
      })) {
    finished.wait();
  }
  loop_.Stop();
}

// --- Event-thread only -------------------------------------------------------

void ChannelImpl::HandleNewCall(PendingCall pc) {
  switch (state_) {
    case State::kShutdown:
      pc.call->FailLocal(StatusCode::kUnavailable, "channel is shut down");
      return;
    case State::kFailed:
      pc.call->FailLocal(StatusCode::kUnavailable, failure_detail_);
      return;
    case State::kIdle:
      pending_calls_.push_back(std::move(pc));
      StartConnecting();
      return;
    case State::kConnecting:
      pending_calls_.push_back(std::move(pc));
      return;
    case State::kReady:
      SubmitCall(std::move(pc));
      FlushOutput();
      return;
  }
}

void ChannelImpl::StartConnecting() {
  state_ = State::kConnecting;
  connect_timer_ = loop_.AddTimer(options_.connect_timeout, [this] {
    connect_timer_ = 0;
    TearDown("connect timeout");
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
  if (connect_timer_ != 0) {
    loop_.CancelTimer(connect_timer_);
    connect_timer_ = 0;
  }
  if (st == TlsSocket::IoStatus::kOk) {
    StartSession();
  } else {
    TearDown(sock_.error());
  }
}

void ChannelImpl::StartSession() {
  Http2Session::Hooks hooks;
  // GOAWAY (design §4.2): the session marks itself draining — in-flight
  // calls run to completion, new submits fail (SubmitUnaryRequest refuses).
  // Reconnect-on-GOAWAY is M6; nothing else to do here in M3.
  hooks.on_goaway = [](uint32_t, int32_t, bool) {};

  h2_ = std::make_unique<Http2Session>(options_.http2, std::move(hooks), steady_clock::now());
  if (!h2_->ok()) {
    TearDown(h2_->error());
    return;
  }

  loop_.WatchFd(
      sock_.fd(), [this] { return static_cast<short>(POLLIN | (h2_->WantWrite() ? POLLOUT : 0)); },
      [this](short revents) { OnSocketReady(revents); });

  state_ = State::kReady;
  FlushOutput();  // connection preface + SETTINGS
  RearmKeepalive();

  std::vector<PendingCall> pending = std::move(pending_calls_);
  pending_calls_.clear();
  for (PendingCall& pc : pending) {
    if (state_ != State::kReady) {  // a submit error tore the channel down
      pc.call->FailLocal(StatusCode::kUnavailable, failure_detail_);
      continue;
    }
    SubmitCall(std::move(pc));
  }
  if (state_ == State::kReady) FlushOutput();
}

void ChannelImpl::OnSocketReady(short revents) {
  if ((revents & (POLLIN | POLLERR | POLLHUP)) != 0) DrainSocket();
  if (state_ == State::kReady) FlushOutput();
  if (state_ == State::kReady) RearmKeepalive();
}

void ChannelImpl::SubmitCall(PendingCall pc) {
  if (state_ != State::kReady || h2_ == nullptr) {
    pc.call->FailLocal(StatusCode::kUnavailable,
                       state_ == State::kFailed ? failure_detail_ : "channel is not ready");
    return;
  }

  // grpc-timeout is computed here, not at call entry: a call that queued
  // while the channel was connecting must tell the server the time it has
  // *left*, not the time it started with. An expired deadline fails locally
  // without touching the wire, matching upstream's fail-fast.
  std::optional<std::chrono::nanoseconds> timeout;
  if (pc.deadline.has_value()) {
    const auto now = std::chrono::steady_clock::now();
    if (*pc.deadline <= now) {
      pc.call->FailLocal(StatusCode::kDeadlineExceeded, "Deadline Exceeded");
      return;
    }
    timeout = std::chrono::duration_cast<std::chrono::nanoseconds>(*pc.deadline - now);
  }

  const Http2Session::HeaderList headers = BuildRequestHeaders(
      authority_, pc.method_path, timeout, pc.metadata, options_.user_agent_prefix);

  Http2Session::StreamHooks hooks = CallState::MakeStreamHooks(pc.call);
  // Wrap on_close to also drop the channel's reference to the call.
  auto inner_close = std::move(hooks.on_close);
  CallState* key = pc.call.get();
  hooks.on_close = [this, key, inner_close = std::move(inner_close)](uint32_t error_code) {
    inner_close(error_code);
    active_calls_.erase(key);
  };

  const int32_t stream_id =
      h2_->SubmitUnaryRequest(headers, std::move(pc.framed_body), std::move(hooks));
  if (stream_id < 0) {
    pc.call->FailLocal(StatusCode::kUnavailable, h2_->error());
    return;
  }
  pc.call->OnSubmitted(stream_id);
  active_calls_.emplace(key, std::move(pc.call));
}

void ChannelImpl::FlushOutput() {
  while (state_ == State::kReady) {
    size_t len = 0;
    const uint8_t* data = h2_->PendingOutput(&len);
    if (data == nullptr || len == 0) {
      if (!h2_->error().empty()) TearDown(h2_->error());
      return;
    }
    size_t written = 0;
    const TlsSocket::IoStatus st = sock_.Write(data, len, &written);
    if (st == TlsSocket::IoStatus::kOk) {
      h2_->ConsumeOutput(written);
      continue;
    }
    if (st == TlsSocket::IoStatus::kWantWrite || st == TlsSocket::IoStatus::kWantRead) {
      return;  // poll re-arms POLLOUT via WantWrite() in the events mask
    }
    TearDown(sock_.error());
    return;
  }
}

void ChannelImpl::DrainSocket() {
  uint8_t buf[16384];
  while (state_ == State::kReady) {
    size_t n = 0;
    const TlsSocket::IoStatus st = sock_.Read(buf, sizeof(buf), &n);
    if (st == TlsSocket::IoStatus::kOk) {
      if (!h2_->ReceiveBytes(steady_clock::now(), buf, n)) {
        TearDown(h2_->error());
        return;
      }
      continue;
    }
    if (st == TlsSocket::IoStatus::kWantRead || st == TlsSocket::IoStatus::kWantWrite) return;
    if (st == TlsSocket::IoStatus::kEof) {
      TearDown("connection closed by server");
      return;
    }
    TearDown(sock_.error());
    return;
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

void ChannelImpl::TearDown(const std::string& detail) {
  if (state_ == State::kFailed || state_ == State::kShutdown) return;
  state_ = State::kFailed;
  failure_detail_ = detail;

  CancelTimersAndUnwatch();
  sock_.Close();
  // Safe: TearDown is never reached from inside an Http2Session callback —
  // ReceiveBytes/PendingOutput report failure via return value first.
  h2_.reset();

  auto active = std::move(active_calls_);
  active_calls_.clear();
  for (auto& entry : active) {
    entry.second->FailLocal(StatusCode::kUnavailable, detail);
  }
  auto pending = std::move(pending_calls_);
  pending_calls_.clear();
  for (PendingCall& pc : pending) {
    pc.call->FailLocal(StatusCode::kUnavailable, detail);
  }
}

void ChannelImpl::CancelTimersAndUnwatch() {
  for (TimerId* t : {&connect_timer_, &keepalive_timer_}) {
    if (*t != 0) {
      loop_.CancelTimer(*t);
      *t = 0;
    }
  }
  loop_.UnwatchFd();
}

void ChannelImpl::DoShutdown() {
  if (state_ == State::kShutdown) return;
  const bool was_ready = state_ == State::kReady && h2_ != nullptr;
  state_ = State::kShutdown;

  // §5.7: unblock every waiter first.
  auto active = std::move(active_calls_);
  active_calls_.clear();
  for (auto& entry : active) {
    entry.second->FailLocal(StatusCode::kUnavailable, "channel is shut down");
  }
  auto pending = std::move(pending_calls_);
  pending_calls_.clear();
  for (PendingCall& pc : pending) {
    pc.call->FailLocal(StatusCode::kUnavailable, "channel is shut down");
  }

  CancelTimersAndUnwatch();

  if (was_ready) {
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
