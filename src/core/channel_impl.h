// egrpc — channel: owns the EventThread and the transport (design §4.4).
//
// M3 scope: connect-on-first-call and a blocking unary entry point — the
// "temporary internal API" of design §8/M3, which M4's grpcpp shim and M6's
// full connectivity FSM (reconnect, backoff, wait-for-ready) build on. In
// M3 a transport failure is terminal for the channel: in-flight and
// subsequent calls fail with kUnavailable and no reconnect is attempted.
//
// Threading (design §3): the EventThread exclusively owns the TlsSocket,
// the Http2Session, all timers, and every CallState transition. Caller
// threads enter only via EventThread::Post + CallState::Wait; Shutdown
// additionally joins the loop.
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
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
  std::chrono::milliseconds connect_timeout{20000};
  // Prepended to the built-in user-agent (§5.1); upstream's
  // GRPC_ARG_PRIMARY_USER_AGENT maps here.
  std::string user_agent_prefix;
};

class ChannelImpl {
 public:
  // Does not connect; the transport comes up on the first call (design
  // §4.4: IDLE until a call needs the connection).
  ChannelImpl(std::string host, uint16_t port, TlsConfig tls, ChannelOptions options);
  ~ChannelImpl();  // Shutdown()s

  ChannelImpl(const ChannelImpl&) = delete;
  ChannelImpl& operator=(const ChannelImpl&) = delete;

  // Blocking unary call, safe from any thread. `request` is the serialized
  // request message (protobuf-lite bytes; framing is added here). `metadata`
  // is the caller's initial metadata, raw: keys are lowercased and `-bin`
  // values base64-encoded on the way to the wire (§5.1). `deadline` is
  // absolute (steady clock): the grpc-timeout header is computed from it
  // when the HEADERS are actually built on the EventThread, so time spent
  // queued (e.g. during connect) is not re-promised to the server; a call
  // whose deadline has passed by then fails with kDeadlineExceeded without
  // being submitted (local deadline timers land in M5). Returns the full
  // result: status, response bytes, metadata.
  CallState::Result UnaryCall(
      const std::string& method_path, std::string request, Http2Session::HeaderList metadata = {},
      std::optional<std::chrono::steady_clock::time_point> deadline = std::nullopt);

  // Design §5.7 (M3 subset): refuse new calls with kUnavailable, fail
  // in-flight calls, terminate the HTTP/2 session gracefully with a
  // best-effort flush, close the socket, join the EventThread. Idempotent;
  // called by the destructor. TODO(M6): the §5.7 bounded-time guarantee
  // (default 5 s) needs the loop-death handling in EventThread — until then
  // Shutdown assumes a live loop (all M3 paths keep it alive).
  void Shutdown();

 private:
  enum class State { kIdle, kConnecting, kReady, kFailed, kShutdown };

  struct PendingCall {
    std::shared_ptr<CallState> call;
    std::string method_path;
    std::string framed_body;
    Http2Session::HeaderList metadata;
    std::optional<std::chrono::steady_clock::time_point> deadline;
  };

  // --- Event-thread only ----------------------------------------------------
  void HandleNewCall(PendingCall pc);
  void StartConnecting();
  void OnConnectProgress();
  void StartSession();
  void OnSocketReady(short revents);
  void SubmitCall(PendingCall pc);
  void FlushOutput();
  void DrainSocket();
  void RearmKeepalive();
  // Terminal transport failure: fails every pending and in-flight call with
  // kUnavailable(detail), closes the transport, state → kFailed.
  void TearDown(const std::string& detail);
  void CancelTimersAndUnwatch();
  void DoShutdown();

  const std::string host_;
  const uint16_t port_;
  const std::string authority_;  // host:port (§5.1)
  const TlsConfig tls_;
  const ChannelOptions options_;

  EventThread loop_;
  // Set once in the constructor. When false the loop never runs, so nothing
  // may be Post()ed — queued ops would strand their waiters forever.
  bool loop_ok_ = false;

  // Serializes Shutdown() callers (they must not race the Stop/join).
  std::mutex shutdown_mu_;
  bool shutdown_done_ = false;

  // --- Event-thread owned (design §3; never touched by callers) -------------
  State state_ = State::kIdle;
  TlsSocket sock_;
  std::unique_ptr<Http2Session> h2_;
  TimerId connect_timer_ = 0;
  TimerId keepalive_timer_ = 0;
  std::vector<PendingCall> pending_calls_;  // queued while kConnecting
  std::map<CallState*, std::shared_ptr<CallState>> active_calls_;
  std::string failure_detail_;  // why state_ == kFailed
};

}  // namespace internal
}  // namespace egrpc
