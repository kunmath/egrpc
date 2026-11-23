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
#include <map>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

// Forward declaration so this header does not leak <nghttp2/nghttp2.h>.
typedef struct nghttp2_session nghttp2_session;

namespace egrpc {
namespace internal {

struct Http2SessionOptions {
  // Local cap on concurrently active outbound streams, enforced by the
  // channel's admission queue as min(this, server's advertised value) —
  // as a client-sent SETTINGS entry alone it would only limit
  // server-initiated streams, which don't exist in gRPC. Still advertised
  // for transparency. Small by design (§4.2): one long-lived stream + a
  // handful of unary calls.
  uint32_t max_concurrent_streams = 8;

  // SETTINGS_INITIAL_WINDOW_SIZE for streams, and the minimum target
  // connection window (§5.4). Unary streams return credit immediately on DATA
  // receipt; server-streaming streams tie stream WINDOW_UPDATE to Read()
  // consumption via ConsumeStream().
  uint32_t initial_window_size = 4 * 1024 * 1024;

  // Keepalive (§5.2). keepalive_time: send PING after this long with no
  // reads. keepalive_timeout: declare dead if neither the ack nor any other
  // read arrives within this window after the PING was sent.
  //
  // The 300 s default matches an unmodified upstream server's minimum ping
  // interval without data, so first contact never GOAWAY-churns on
  // too_many_pings. Cellular/NAT deployments that need 60 s must lower this
  // explicitly AND coordinate the server's enforcement settings (§5.2).
  std::chrono::milliseconds keepalive_time{300000};
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

  // One (name, value) header pair; names are already lowercase per HTTP/2.
  using Header = std::pair<std::string, std::string>;
  using HeaderList = std::vector<Header>;

  // Response-side events for one stream. All callbacks fire on the owning
  // thread from inside ReceiveBytes(); they may not call back into the
  // session (single-threaded byte machine, §4.2 — the owner queues follow-up
  // work instead).
  struct StreamHooks {
    // Initial response HEADERS. end_stream=true is the Trailers-Only case
    // (design §4.3): this HEADERS carries grpc-status and no DATA follows.
    std::function<void(HeaderList headers, bool end_stream)> on_response_headers;
    // Trailing HEADERS (always ends the stream).
    std::function<void(HeaderList trailers)> on_trailers;
    // One DATA chunk (may be any slice of a gRPC message; the CallState
    // scanner reassembles).
    std::function<void(const uint8_t* data, size_t len)> on_data;
    // Server sent END_STREAM on HEADERS or DATA (half-closed remote). Fires at
    // most once, right after on_response_headers / on_trailers / on_data for
    // that frame — lets the owner finalize the RPC immediately even if the
    // request upload side has not finished sending END_STREAM yet.
    std::function<void()> on_response_end_stream;
    // Stream fully closed. error_code is the HTTP/2 error code
    // (0 = NO_ERROR); fires exactly once, after which no other hook runs.
    // Streams still open when the session is torn down do NOT get on_close —
    // transport teardown is the owner's signal to fail remaining calls.
    std::function<void(uint32_t error_code)> on_close;
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
  // True if nghttp2 still expects to read frames. When both WantRead() and
  // WantWrite() are false, nghttp2 has terminated the session (e.g. after
  // sending GOAWAY on a connection error) and the owner must tear down.
  bool WantRead() const;
  // True if the client (upload) side of stream_id has not yet sent END_STREAM.
  bool IsStreamLocalOpen(int32_t stream_id) const;

  // HTTP/2 RST_STREAM error codes used by higher layers (RFC 7540 §7).
  static constexpr uint32_t kHttp2NoError = 0x0;
  static constexpr uint32_t kHttp2Cancel = 0x8;

  // --- Streams (§4.3 send path, §5.4 flow control) --------------------------
  // Submits a unary request: HEADERS (from `headers`, in order, pseudo-
  // headers first) followed by `body` — the already-framed gRPC message —
  // sent via a data provider with END_STREAM at the end (unary half-closes
  // immediately). An empty body still half-closes with an empty DATA/EOF.
  // Unary stream credit is returned immediately on DATA receipt.
  // Returns the stream id (> 0), or -1 on failure (error() has detail; the
  // draining() case fails here too — no new streams after GOAWAY).
  // The frames land in the output buffer on the next PendingOutput() pass.
  int32_t SubmitUnaryRequest(const HeaderList& headers, std::string body, StreamHooks hooks);

  // Submits a server-streaming request (single framed request message +
  // END_STREAM, multiple response messages). Stream-level flow-control credit
  // is withheld until the owner calls ConsumeStream() as Read() pops decoded
  // messages (§5.4); connection-level credit is still returned immediately so
  // one stalled stream cannot starve sibling calls.
  int32_t SubmitServerStreamRequest(const HeaderList& headers, std::string body, StreamHooks hooks);

  // Returns `bytes` of stream-level flow-control credit for a manual-flow
  // (server-streaming) stream. No-op if the stream is already closed or uses
  // immediate credit. May queue a stream WINDOW_UPDATE into PendingOutput().
  void ConsumeStream(int32_t stream_id, size_t bytes);

  // Queues RST_STREAM(error_code) for `stream_id` (e.g., kHttp2Cancel on
  // TryCancel, deadline expiry, early Finish, or framing error). Safe no-op
  // if the session is uninitialized or the stream is already closed.
  // Per-call failures record detail in call_error(), never fatal error().
  bool ResetStream(int32_t stream_id, uint32_t error_code = kHttp2Cancel);

  // Streams with hooks still registered (submitted, not yet closed).
  int active_streams() const { return active_streams_; }

  // True once the server's initial SETTINGS frame has been received.
  bool remote_settings_received() const { return remote_settings_received_; }

  // The server's advertised SETTINGS_MAX_CONCURRENT_STREAMS. Before its
  // first SETTINGS arrives this is nghttp2's RFC-default "unlimited"
  // ((1u << 31) - 1). The channel admission cap is
  // min(options.max_concurrent_streams, this) — see ChannelImpl.
  uint32_t remote_max_concurrent_streams() const;

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

  // Queues a graceful session termination (GOAWAY, NO_ERROR — design §5.7).
  // The owner flushes PendingOutput() best-effort afterwards. No new streams
  // after this; the session is draining.
  void Terminate();

  // --- GOAWAY / errors ------------------------------------------------------
  // True once GOAWAY was received: no new streams; in-flight work drains.
  bool draining() const;
  bool goaway_too_many_pings() const;
  // Human-readable fatal-error detail (valid once ReceiveBytes/PendingOutput
  // reported failure or ok() is false). Non-fatal per-call errors (such as
  // submit refused after GOAWAY) are reported via call_error() instead so
  // healthy in-flight streams are never torn down.
  const std::string& error() const;
  const std::string& call_error() const { return call_error_; }

  // Diagnostics/tests: outstanding keepalive ping, acked ping count.
  bool ping_outstanding() const;
  uint64_t pings_acked() const;

 private:
  struct Impl;  // nghttp2 callbacks live in the .cc
  friend struct Impl;

  // Per-stream state: the outbound body the data provider reads from, the
  // header list being accumulated for the current HEADERS frame, and the
  // owner's hooks. Owned by streams_; freed from the on_stream_close
  // callback after on_close fires.
  struct Stream {
    StreamHooks hooks;
    std::string body;
    size_t body_offset = 0;
    HeaderList pending_headers;  // filled by on_header, drained on frame end
    bool manual_flow_control = false;
    bool final_response_headers_seen = false;
    // Exact count of received DATA payload bytes not yet released via
    // ConsumeStream(). Subtracted from nghttp2's effective recv_data_length to
    // compute exact returnable credit (popped payload + unacked padding).
    size_t unconsumed_payload_bytes = 0;
  };

  int32_t SubmitRequest(const HeaderList& headers, std::string body, StreamHooks hooks,
                        bool manual_flow_control);
  void FlushManualStreamCredit(Stream* stream, int32_t stream_id);

  Http2SessionOptions options_;
  Hooks hooks_;
  nghttp2_session* session_ = nullptr;
  std::string error_;       // fatal session errors only
  std::string call_error_;  // non-fatal per-call errors (submit/reset)

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
  int active_streams_ = 0;  // == streams_.size(); kept as int for keepalive checks
  std::map<int32_t, std::unique_ptr<Stream>> streams_;

  bool draining_ = false;
  bool too_many_pings_ = false;
  bool remote_settings_received_ = false;
};

}  // namespace internal
}  // namespace egrpc
