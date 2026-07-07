// egrpc — HTTP/2 session: thin RAII wrapper over nghttp2 (design §4.2).
//
// Owned exclusively by the EventThread (design §3); every method is
// single-threaded by contract. The session is a pure byte/state machine: it
// never touches sockets, timers, or clocks on its own. The owner
//   - feeds inbound TLS plaintext via ReceiveBytes(now, ...),
//   - drains outbound frames via PendingOutput()/ConsumeOutput(),
//   - arms a timer from NextKeepaliveDeadline(now) and calls
//     CheckKeepalive(now) when it fires.
// Time is always injected (std::chrono::steady_clock::time_point), which
// makes the keepalive state machine unit-testable without sleeping.
//
// Keepalive semantics (design §5.2): after keepalive_time with no reads,
// submit a PING; if no PING ack — or any read — within keepalive_timeout,
// declare the connection dead (the owner then tears down the transport and
// enters backoff). permit_without_calls defaults to true deliberately
// (NAT-mapping use case; deviates from upstream, documented).
//
// GOAWAY (design §4.2): recorded and surfaced via hook; the session marks
// itself draining. ENHANCE_YOUR_CALM + "too_many_pings" is flagged so M6 can
// double keepalive_time.
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <string>
#include <vector>

// Forward declaration so this header does not leak <nghttp2/nghttp2.h>.
typedef struct nghttp2_session nghttp2_session;

namespace egrpc {
namespace internal {

struct Http2SessionOptions {
  // Advertised in our SETTINGS. Small by design (§4.2): one long-lived
  // stream + a handful of unary calls.
  uint32_t max_concurrent_streams = 8;

  // SETTINGS_INITIAL_WINDOW_SIZE for streams, and the target connection
  // window (§5.4). M5 ties stream WINDOW_UPDATE to Read() consumption; until
  // then nghttp2's automatic window management is used.
  uint32_t initial_window_size = 4 * 1024 * 1024;

  // Keepalive (§5.2). keepalive_time: send PING after this long with no
  // reads. keepalive_timeout: declare dead if neither the ack nor any other
  // read arrives within this window after the PING was sent.
  std::chrono::milliseconds keepalive_time{60000};
  std::chrono::milliseconds keepalive_timeout{20000};
  bool keepalive_permit_without_calls = true;
};

class Http2Session {
 public:
  using TimePoint = std::chrono::steady_clock::time_point;

  struct Hooks {
    // Server's SETTINGS frame arrived (first one completes the exchange).
    std::function<void()> on_remote_settings;
    // Server acknowledged our SETTINGS.
    std::function<void()> on_settings_ack;
    // A PING ack arrived (keepalive or otherwise).
    std::function<void()> on_ping_ack;
    // GOAWAY received. too_many_pings is true for ENHANCE_YOUR_CALM with
    // "too_many_pings" debug data (§5.2).
    std::function<void(uint32_t error_code, int32_t last_stream_id, bool too_many_pings)> on_goaway;
  };

  enum class KeepaliveAction {
    kNone,      // nothing due; re-arm from NextKeepaliveDeadline
    kPingSent,  // keepalive PING queued — flush output, re-arm
    kDead,      // no ack/read within keepalive_timeout — tear down, backoff
  };

  // Creates the client session. nghttp2 emits the connection preface and our
  // SETTINGS (max_concurrent_streams, initial_window_size, ENABLE_PUSH=0)
  // into the output buffer immediately; the connection-level window is
  // raised to initial_window_size via WINDOW_UPDATE on stream 0. `now` seeds
  // the keepalive read clock. ok() is false if nghttp2 setup failed.
  Http2Session(const Http2SessionOptions& options, Hooks hooks, TimePoint now);
  ~Http2Session();

  Http2Session(const Http2Session&) = delete;
  Http2Session& operator=(const Http2Session&) = delete;

  bool ok() const;

  // --- Inbound ------------------------------------------------------------
  // Feeds plaintext from TlsSocket::Read. Counts as a "read" for keepalive
  // (len > 0). Returns false on fatal session error (error() has detail);
  // the session is unusable afterwards.
  bool ReceiveBytes(TimePoint now, const uint8_t* data, size_t len);

  // --- Outbound -----------------------------------------------------------
  // Serialized frames waiting to go out. PendingOutput refills the internal
  // buffer from nghttp2 as needed and returns a view valid until the next
  // non-const call; empty view = nothing to send. ConsumeOutput(n) marks the
  // first n bytes as written to the socket (n ≤ the last returned size —
  // partial socket writes are the normal case).
  const uint8_t* PendingOutput(size_t* len);
  void ConsumeOutput(size_t n);
  // True if the session has (or may generate) output — poll for POLLOUT.
  bool WantWrite();

  // --- Keepalive (§5.2) ----------------------------------------------------
  // When the owner's keepalive timer should next fire, given current state:
  //   - ping outstanding → ping_sent_time + keepalive_timeout
  //   - otherwise        → last_read_time + keepalive_time
  //   - nullopt when keepalive is disabled by permit_without_calls=false
  //     with no active streams (M3+ tracks streams; M2 has none).
  std::optional<TimePoint> NextKeepaliveDeadline(TimePoint now) const;

  // Evaluates the state machine at `now`:
  //   - a read arrived since the outstanding PING → ping satisfied, fall
  //     through to the idle check;
  //   - PING outstanding and keepalive_timeout elapsed with no read → kDead;
  //   - idle for keepalive_time → submit PING (8-byte counter opaque data),
  //     → kPingSent;
  //   - else kNone.
  KeepaliveAction CheckKeepalive(TimePoint now);

  // --- GOAWAY / errors ------------------------------------------------------
  // True once GOAWAY was received: no new streams; in-flight work drains.
  bool draining() const;
  bool goaway_too_many_pings() const;
  // Human-readable fatal-error detail (valid once ReceiveBytes/PendingOutput
  // reported failure or ok() is false).
  const std::string& error() const;

  // Diagnostics/tests: outstanding keepalive ping, acked ping count.
  bool ping_outstanding() const;
  uint64_t pings_acked() const;

 private:
  struct Impl;  // nghttp2 callbacks live in the .cc
  friend struct Impl;

  Http2SessionOptions options_;
  Hooks hooks_;
  nghttp2_session* session_ = nullptr;
  std::string error_;

  // Outbound accumulation buffer (design §4.2: send via memory buffers
  // flushed to TlsSocket): [out_consumed_, out_.size()) is unsent.
  std::vector<uint8_t> out_;
  size_t out_consumed_ = 0;

  // Keepalive state (all times injected).
  TimePoint last_read_;
  TimePoint ping_sent_;
  bool ping_outstanding_ = false;
  uint64_t ping_counter_ = 0;
  uint64_t pings_acked_ = 0;
  int active_streams_ = 0;  // maintained from M3 on; 0 throughout M2

  bool draining_ = false;
  bool too_many_pings_ = false;
};

}  // namespace internal
}  // namespace egrpc
