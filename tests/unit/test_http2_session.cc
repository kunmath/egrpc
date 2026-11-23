// egrpc — M2 unit tests for Http2Session (design §4.2, §5.2).
//
// Http2Session is a pure byte/state machine owned by the EventThread. These
// tests exercise it against an in-process nghttp2 *server* session, pumping
// bytes both ways by hand. No sockets, no threads, no sleeping: time is
// synthetic (base = steady_clock::now(); everything is base + N ms), so the
// keepalive state machine is fully deterministic.
#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include "doctest/doctest.h"
#include "transport/http2_session.h"

namespace {

using egrpc::internal::Http2Session;
using egrpc::internal::Http2SessionOptions;
using KeepaliveAction = Http2Session::KeepaliveAction;
using TimePoint = Http2Session::TimePoint;

TimePoint Base() { return std::chrono::steady_clock::now(); }
TimePoint At(TimePoint base, int ms) { return base + std::chrono::milliseconds(ms); }
TimePoint AtSec(TimePoint base, int s) { return base + std::chrono::seconds(s); }

// The keepalive-schedule tests pin an explicit 60s/20s configuration (the
// "cellular" profile) so they test the schedule mechanics independently of
// the shipped server-safe default (300 s, asserted separately below).
Http2SessionOptions KeepaliveOpts() {
  Http2SessionOptions o;
  o.keepalive_time = std::chrono::milliseconds(60000);
  o.keepalive_timeout = std::chrono::milliseconds(20000);
  return o;
}

// RAII wrapper for a raw nghttp2 server session so no test leaks it (ASan runs
// with detect_leaks=1). A server needs its own SETTINGS submitted after
// creation for the handshake to complete.
struct ServerSession {
  nghttp2_session* s = nullptr;

  ServerSession() {
    nghttp2_session_callbacks* cb = nullptr;
    REQUIRE(nghttp2_session_callbacks_new(&cb) == 0);
    REQUIRE(nghttp2_session_server_new(&s, cb, nullptr) == 0);
    nghttp2_session_callbacks_del(cb);
    REQUIRE(nghttp2_submit_settings(s, NGHTTP2_FLAG_NONE, nullptr, 0) == 0);
  }
  ~ServerSession() { nghttp2_session_del(s); }

  ServerSession(const ServerSession&) = delete;
  ServerSession& operator=(const ServerSession&) = delete;
};

// Drains all pending client output into the server session.
void PumpClientToServer(Http2Session& client, nghttp2_session* server) {
  for (;;) {
    size_t len = 0;
    const uint8_t* p = client.PendingOutput(&len);
    if (len == 0) break;
    const ssize_t r = nghttp2_session_mem_recv(server, p, len);
    REQUIRE(r == static_cast<ssize_t>(len));
    client.ConsumeOutput(len);
  }
}

// Drains all pending server output into the client session at time `now`.
void PumpServerToClient(nghttp2_session* server, Http2Session& client, TimePoint now) {
  for (;;) {
    const uint8_t* chunk = nullptr;
    const ssize_t n = nghttp2_session_mem_send(server, &chunk);
    if (n <= 0) break;
    REQUIRE(client.ReceiveBytes(now, chunk, static_cast<size_t>(n)));
  }
}

// Runs the SETTINGS handshake (and any follow-on ACKs) to quiescence at `now`.
void Handshake(Http2Session& client, nghttp2_session* server, TimePoint now) {
  for (int i = 0; i < 100; ++i) {
    bool progressed = false;

    for (;;) {
      size_t len = 0;
      const uint8_t* p = client.PendingOutput(&len);
      if (len == 0) break;
      REQUIRE(nghttp2_session_mem_recv(server, p, len) == static_cast<ssize_t>(len));
      client.ConsumeOutput(len);
      progressed = true;
    }
    for (;;) {
      const uint8_t* chunk = nullptr;
      const ssize_t n = nghttp2_session_mem_send(server, &chunk);
      if (n <= 0) break;
      REQUIRE(client.ReceiveBytes(now, chunk, static_cast<size_t>(n)));
      progressed = true;
    }
    if (!progressed) break;
  }
}

Http2Session::HeaderList GrpcRequestHeaders(const std::string& path) {
  return {
      {":method", "POST"},         {":scheme", "https"}, {":path", path},
      {":authority", "localhost"}, {"te", "trailers"},   {"content-type", "application/grpc"},
  };
}

std::string MakePattern(size_t len, uint8_t seed = 0) {
  std::string out(len, '\0');
  for (size_t i = 0; i < len; ++i) {
    out[i] = static_cast<char>((i + seed) & 0xff);
  }
  return out;
}

struct ServerDataBuffer {
  std::string data;
  size_t offset = 0;
  bool eof_ready = true;
};

ssize_t ServerDataReadCallback(nghttp2_session* /*session*/, int32_t /*stream_id*/, uint8_t* buf,
                               size_t length, uint32_t* data_flags, nghttp2_data_source* source,
                               void* /*user_data*/) {
  auto* state = static_cast<ServerDataBuffer*>(source->ptr);
  const size_t remaining = state->data.size() - state->offset;
  if (remaining == 0) {
    if (state->eof_ready) {
      *data_flags |= NGHTTP2_DATA_FLAG_EOF;
      return 0;
    }
    return NGHTTP2_ERR_DEFERRED;
  }
  const size_t n = std::min(length, remaining);
  std::memcpy(buf, state->data.data() + state->offset, n);
  state->offset += n;
  if (state->offset == state->data.size() && state->eof_ready) {
    *data_flags |= NGHTTP2_DATA_FLAG_EOF;
  }
  return static_cast<ssize_t>(n);
}

void SubmitServerResponse(nghttp2_session* server, int32_t stream_id, ServerDataBuffer* body) {
  static constexpr char kStatusName[] = ":status";
  static constexpr char kStatusVal[] = "200";
  static constexpr char kCtName[] = "content-type";
  static constexpr char kCtVal[] = "application/grpc";
  const nghttp2_nv nva[] = {
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusVal)), sizeof(kStatusName) - 1,
       sizeof(kStatusVal) - 1, NGHTTP2_NV_FLAG_NONE},
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kCtName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kCtVal)), sizeof(kCtName) - 1,
       sizeof(kCtVal) - 1, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_data_provider provider{};
  provider.source.ptr = body;
  provider.read_callback = &ServerDataReadCallback;
  REQUIRE(nghttp2_submit_response(server, stream_id, nva, 2, &provider) == 0);
}

// Pumps both directions until neither side has pending output.
void PumpBothWays(Http2Session& client, nghttp2_session* server, TimePoint now) {
  Handshake(client, server, now);
}

struct RstCapture {
  int count = 0;
  int32_t stream_id = -1;
  uint32_t error_code = 0;
};

int CaptureRstFrameRecv(nghttp2_session* /*session*/, const nghttp2_frame* frame, void* user_data) {
  if (frame->hd.type == NGHTTP2_RST_STREAM) {
    auto* cap = static_cast<RstCapture*>(user_data);
    ++cap->count;
    cap->stream_id = frame->hd.stream_id;
    cap->error_code = frame->rst_stream.error_code;
  }
  return 0;
}

struct RstCapturingServerSession {
  nghttp2_session* s = nullptr;

  explicit RstCapturingServerSession(RstCapture* capture) {
    nghttp2_session_callbacks* cb = nullptr;
    REQUIRE(nghttp2_session_callbacks_new(&cb) == 0);
    nghttp2_session_callbacks_set_on_frame_recv_callback(cb, &CaptureRstFrameRecv);
    REQUIRE(nghttp2_session_server_new(&s, cb, capture) == 0);
    nghttp2_session_callbacks_del(cb);
    REQUIRE(nghttp2_submit_settings(s, NGHTTP2_FLAG_NONE, nullptr, 0) == 0);
  }
  ~RstCapturingServerSession() { nghttp2_session_del(s); }

  RstCapturingServerSession(const RstCapturingServerSession&) = delete;
  RstCapturingServerSession& operator=(const RstCapturingServerSession&) = delete;
};

}  // namespace

TEST_CASE("Http2Session: construction queues preface + SETTINGS, WantWrite tracks flush") {
  const auto base = Base();
  Http2Session client({}, {}, base);
  REQUIRE(client.ok());
  CHECK(client.error().empty());

  // Preface + SETTINGS + WINDOW_UPDATE are queued immediately.
  size_t len = 0;
  const uint8_t* p = client.PendingOutput(&len);
  CHECK(p != nullptr);
  CHECK(len > 0);
  CHECK(client.WantWrite());

  // After a full drain (all output consumed) there is nothing left to write.
  for (;;) {
    const uint8_t* q = client.PendingOutput(&len);
    if (len == 0) break;
    (void)q;
    client.ConsumeOutput(len);
  }
  CHECK_FALSE(client.WantWrite());
}

TEST_CASE("Http2Session: SETTINGS exchange fires each hook exactly once") {
  const auto base = Base();
  int remote_settings = 0;
  int settings_ack = 0;

  Http2Session::Hooks hooks;
  hooks.on_remote_settings = [&] { ++remote_settings; };
  hooks.on_settings_ack = [&] { ++settings_ack; };

  Http2Session client({}, std::move(hooks), base);
  REQUIRE(client.ok());

  ServerSession server;
  Handshake(client, server.s, base);

  CHECK(remote_settings == 1);
  CHECK(settings_ack == 1);
}

TEST_CASE("Http2Session: ConsumeOutput partial-write and defensive clamp") {
  const auto base = Base();
  Http2Session client({}, {}, base);
  REQUIRE(client.ok());

  size_t full_len = 0;
  const uint8_t* p0 = client.PendingOutput(&full_len);
  REQUIRE(p0 != nullptr);
  REQUIRE(full_len > 3);

  // Partial write of 3 bytes: the next view starts 3 bytes further and is 3
  // bytes shorter; there is still output pending.
  client.ConsumeOutput(3);
  size_t len2 = 0;
  const uint8_t* p1 = client.PendingOutput(&len2);
  CHECK(p1 == p0 + 3);
  CHECK(len2 == full_len - 3);
  CHECK(client.WantWrite());

  // Defensive clamp: over-consuming does not crash and empties the region.
  client.ConsumeOutput(1000000);
  size_t len3 = 0;
  const uint8_t* p2 = client.PendingOutput(&len3);
  CHECK(len3 == 0);
  CHECK(p2 == nullptr);
  CHECK_FALSE(client.WantWrite());
}

TEST_CASE("Http2Session: keepalive schedule sends PING and clears on ack") {
  const auto base = Base();
  int ping_acks = 0;
  Http2Session::Hooks hooks;
  hooks.on_ping_ack = [&] { ++ping_acks; };

  Http2Session client(KeepaliveOpts(), std::move(hooks), base);
  REQUIRE(client.ok());

  ServerSession server;
  Handshake(client, server.s, base);  // keeps last_read_ == base

  // keepalive_time 60s, timeout 20s. Deadline = last_read + 60s.
  auto nkd = client.NextKeepaliveDeadline(base);
  REQUIRE(nkd.has_value());
  CHECK(*nkd == AtSec(base, 60));

  // One second early: nothing due.
  CHECK(client.CheckKeepalive(AtSec(base, 59)) == KeepaliveAction::kNone);
  CHECK_FALSE(client.ping_outstanding());

  // At the deadline: a PING is submitted; deadline moves to ping_sent + 20s.
  CHECK(client.CheckKeepalive(AtSec(base, 60)) == KeepaliveAction::kPingSent);
  CHECK(client.ping_outstanding());
  nkd = client.NextKeepaliveDeadline(AtSec(base, 60));
  REQUIRE(nkd.has_value());
  CHECK(*nkd == AtSec(base, 80));

  // Flush the PING to the server and pump its auto-ack back.
  PumpClientToServer(client, server.s);
  PumpServerToClient(server.s, client, AtSec(base, 61));

  CHECK(ping_acks == 1);
  CHECK(client.pings_acked() == 1);
  CHECK_FALSE(client.ping_outstanding());
}

TEST_CASE("Http2Session: keepalive death when PING goes unacked") {
  const auto base = Base();
  Http2Session client(KeepaliveOpts(), {}, base);
  REQUIRE(client.ok());

  // Send a keepalive PING at base + 60s; do NOT pump the ack.
  CHECK(client.CheckKeepalive(AtSec(base, 60)) == KeepaliveAction::kPingSent);
  CHECK(client.ping_outstanding());

  // 1 ms before the timeout: still alive.
  CHECK(client.CheckKeepalive(At(base, 60000 + 19999)) == KeepaliveAction::kNone);
  // At the timeout: dead.
  CHECK(client.CheckKeepalive(At(base, 60000 + 20000)) == KeepaliveAction::kDead);
}

TEST_CASE("Http2Session: default keepalive_time is server-safe (300 s)") {
  // §5.2: the default must not trip an unmodified upstream server's
  // too_many_pings enforcement (5-minute minimum ping interval). 60 s is the
  // opt-in cellular profile, never the default.
  const Http2SessionOptions defaults;
  CHECK(defaults.keepalive_time == std::chrono::milliseconds(300000));
  CHECK(defaults.keepalive_timeout == std::chrono::milliseconds(20000));
  CHECK(defaults.keepalive_permit_without_calls);
}

TEST_CASE("Http2Session: any read counts as liveness while PING outstanding") {
  const auto base = Base();
  Http2Session client(KeepaliveOpts(), {}, base);
  REQUIRE(client.ok());

  ServerSession server;
  Handshake(client, server.s, base);

  // PING outstanding at base + 60s, not flushed/acked.
  CHECK(client.CheckKeepalive(AtSec(base, 60)) == KeepaliveAction::kPingSent);
  CHECK(client.ping_outstanding());

  // Unrelated server bytes arrive after the PING was sent (not a PING ack).
  const auto t1 = AtSec(base, 61);
  REQUIRE(nghttp2_submit_ping(server.s, NGHTTP2_FLAG_NONE, nullptr) == 0);
  PumpServerToClient(server.s, client, t1);

  // The read satisfies liveness: not dead, PING cleared.
  CHECK(client.CheckKeepalive(At(base, 61001)) == KeepaliveAction::kNone);
  CHECK_FALSE(client.ping_outstanding());
}

TEST_CASE("Http2Session: permit_without_calls=false disables keepalive with no streams") {
  const auto base = Base();
  Http2SessionOptions options;
  options.keepalive_permit_without_calls = false;

  Http2Session client(options, {}, base);
  REQUIRE(client.ok());

  CHECK(client.NextKeepaliveDeadline(base) == std::nullopt);
  CHECK(client.CheckKeepalive(AtSec(base, 600)) == KeepaliveAction::kNone);
  CHECK_FALSE(client.ping_outstanding());
}

TEST_CASE("Http2Session: GOAWAY marks draining and flags too_many_pings") {
  const auto base = Base();

  SUBCASE("ENHANCE_YOUR_CALM + too_many_pings") {
    uint32_t code = 0;
    int32_t last_stream = -1;
    bool too_many = false;
    Http2Session::Hooks hooks;
    hooks.on_goaway = [&](uint32_t c, int32_t l, bool t) {
      code = c;
      last_stream = l;
      too_many = t;
    };

    Http2Session client({}, std::move(hooks), base);
    REQUIRE(client.ok());
    ServerSession server;
    Handshake(client, server.s, base);

    REQUIRE(nghttp2_submit_goaway(server.s, NGHTTP2_FLAG_NONE, 0, NGHTTP2_ENHANCE_YOUR_CALM,
                                  reinterpret_cast<const uint8_t*>("too_many_pings"), 14) == 0);
    PumpServerToClient(server.s, client, AtSec(base, 1));

    CHECK(client.draining());
    CHECK(client.goaway_too_many_pings());
    CHECK(code == static_cast<uint32_t>(NGHTTP2_ENHANCE_YOUR_CALM));
    CHECK(code == 0xbu);
    CHECK(last_stream == 0);
    CHECK(too_many);
  }

  SUBCASE("plain GOAWAY drains but is not too_many_pings") {
    Http2Session client({}, {}, base);
    REQUIRE(client.ok());
    ServerSession server;
    Handshake(client, server.s, base);

    REQUIRE(nghttp2_submit_goaway(server.s, NGHTTP2_FLAG_NONE, 0, NGHTTP2_NO_ERROR, nullptr, 0) ==
            0);
    PumpServerToClient(server.s, client, AtSec(base, 1));

    CHECK(client.draining());
    CHECK_FALSE(client.goaway_too_many_pings());
  }
}

TEST_CASE("Http2Session: garbage input is survived and answered with GOAWAY") {
  const auto base = Base();
  Http2Session client({}, {}, base);
  REQUIRE(client.ok());

  // Drain the preface/SETTINGS so any later output is new.
  for (;;) {
    size_t len = 0;
    (void)client.PendingOutput(&len);
    if (len == 0) break;
    client.ConsumeOutput(len);
  }

  // A stream of 0xFF bytes is not valid HTTP/2 from a server. nghttp2 does
  // not report this as a fatal mem_recv error (those are flooding/callback/
  // OOM cases); it handles the protocol error internally and queues a GOAWAY
  // for the peer. The contract here is: no crash, no false-fatal, and the
  // session wants to transmit its GOAWAY.
  std::vector<uint8_t> garbage(24, 0xFF);
  CHECK(client.ReceiveBytes(base, garbage.data(), garbage.size()));
  CHECK(client.error().empty());
  CHECK(client.WantWrite());
}

TEST_CASE("Http2Session: manual stream flow control stalls server until ConsumeStream") {
  const auto base = Base();
  Http2SessionOptions opts;
  opts.initial_window_size = 4096;

  Http2Session client(opts, {}, base);
  REQUIRE(client.ok());

  ServerDataBuffer server_body;
  server_body.data = MakePattern(12288, 0x11);
  server_body.eof_ready = true;

  ServerSession server;
  Handshake(client, server.s, base);

  std::string received;
  bool closed = false;
  uint32_t close_error = 999;

  Http2Session::StreamHooks hooks;
  hooks.on_data = [&](const uint8_t* data, size_t len) {
    received.append(reinterpret_cast<const char*>(data), len);
  };
  hooks.on_close = [&](uint32_t err) {
    closed = true;
    close_error = err;
  };

  const int32_t stream_id = client.SubmitServerStreamRequest(GrpcRequestHeaders("/test.Svc/Stream"),
                                                             "", std::move(hooks));
  REQUIRE(stream_id > 0);
  PumpClientToServer(client, server.s);

  SubmitServerResponse(server.s, stream_id, &server_body);

  // Pump server -> client without calling ConsumeStream: window (4096) stalls server.
  PumpServerToClient(server.s, client, At(base, 1));
  CHECK(received.size() <= 4096);
  CHECK(received.size() == 4096);
  CHECK_FALSE(closed);

  // Additional pumping without ConsumeStream must not release more bytes.
  PumpBothWays(client, server.s, At(base, 2));
  CHECK(received.size() == 4096);
  CHECK_FALSE(closed);

  // Drain by returning consumed stream credit until the full 12288 bytes arrive.
  size_t consumed = 0;
  for (int iter = 0; iter < 32 && !closed; ++iter) {
    const size_t chunk = received.size() - consumed;
    if (chunk > 0) {
      client.ConsumeStream(stream_id, chunk);
      consumed += chunk;
    }
    PumpClientToServer(client, server.s);
    PumpServerToClient(server.s, client, At(base, 3 + iter));
  }

  CHECK(closed);
  CHECK(close_error == Http2Session::kHttp2NoError);
  CHECK(received.size() == 12288);
  CHECK(received == server_body.data);
}

TEST_CASE(
    "Http2Session: anti-deadlock — small consumed chunk followed by full-window message does not "
    "stall") {
  const auto base = Base();
  Http2SessionOptions opts;
  opts.initial_window_size = 8192;

  Http2Session client(opts, {}, base);
  REQUIRE(client.ok());

  const std::string first_chunk = MakePattern(1000, 0x22);
  const std::string second_chunk = MakePattern(8000, 0x77);
  ServerDataBuffer server_body;
  server_body.data = first_chunk;
  server_body.eof_ready = false;

  ServerSession server;
  Handshake(client, server.s, base);

  std::string received;
  bool closed = false;
  uint32_t close_error = 999;

  Http2Session::StreamHooks hooks;
  hooks.on_data = [&](const uint8_t* data, size_t len) {
    received.append(reinterpret_cast<const char*>(data), len);
  };
  hooks.on_close = [&](uint32_t err) {
    closed = true;
    close_error = err;
  };

  const int32_t stream_id = client.SubmitServerStreamRequest(
      GrpcRequestHeaders("/test.Svc/AntiDeadlock"), "", std::move(hooks));
  REQUIRE(stream_id > 0);
  PumpClientToServer(client, server.s);

  // Server sends 1000 bytes (< half window 4096), not END_STREAM.
  SubmitServerResponse(server.s, stream_id, &server_body);
  PumpServerToClient(server.s, client, At(base, 1));
  REQUIRE(received.size() == 1000);
  CHECK_FALSE(closed);

  // Client consumes the 1000 bytes; credit is buffered (< half window, remaining 7192 > 4096).
  client.ConsumeStream(stream_id, 1000);
  PumpClientToServer(client, server.s);

  // Server now sends an 8000-byte message (> remaining 7192 window, < full 8192 window) + EOF.
  server_body.data += second_chunk;
  server_body.eof_ready = true;
  REQUIRE(nghttp2_session_resume_data(server.s, stream_id) == 0);

  // Pump both ways WITHOUT calling ConsumeStream again: FlushManualStreamCredit must flush
  // the buffered 1000-byte credit once remaining window drops <= half window.
  PumpBothWays(client, server.s, At(base, 2));

  CHECK(closed);
  CHECK(close_error == Http2Session::kHttp2NoError);
  REQUIRE(received.size() == 9000);
  CHECK(received.substr(0, 1000) == first_chunk);
  CHECK(received.substr(1000) == second_chunk);
}

TEST_CASE(
    "Http2Session: stalled manual stream does not starve connection window for second stream") {
  const auto base = Base();
  Http2SessionOptions opts;
  opts.initial_window_size = 4096;

  Http2Session client(opts, {}, base);
  REQUIRE(client.ok());

  ServerDataBuffer stream1_body;
  stream1_body.data = MakePattern(4096, 0x33);
  stream1_body.eof_ready = false;

  ServerDataBuffer stream2_body;
  stream2_body.data = MakePattern(8192, 0x55);
  stream2_body.eof_ready = true;

  ServerSession server;
  Handshake(client, server.s, base);

  std::string s1_received;
  bool s1_closed = false;
  Http2Session::StreamHooks s1_hooks;
  s1_hooks.on_data = [&](const uint8_t* data, size_t len) {
    s1_received.append(reinterpret_cast<const char*>(data), len);
  };
  s1_hooks.on_close = [&](uint32_t) { s1_closed = true; };

  const int32_t stream1_id = client.SubmitServerStreamRequest(
      GrpcRequestHeaders("/test.Svc/StalledStream"), "", std::move(s1_hooks));
  REQUIRE(stream1_id > 0);
  PumpClientToServer(client, server.s);

  // Stream 1 fills its entire 4096-byte window and never calls ConsumeStream.
  SubmitServerResponse(server.s, stream1_id, &stream1_body);
  PumpServerToClient(server.s, client, At(base, 1));
  REQUIRE(s1_received.size() == 4096);
  CHECK_FALSE(s1_closed);

  // Open unary stream 2; server responds with 8192 bytes + END_STREAM.
  std::string s2_received;
  bool s2_closed = false;
  uint32_t s2_close_error = 999;
  Http2Session::StreamHooks s2_hooks;
  s2_hooks.on_data = [&](const uint8_t* data, size_t len) {
    s2_received.append(reinterpret_cast<const char*>(data), len);
  };
  s2_hooks.on_close = [&](uint32_t err) {
    s2_closed = true;
    s2_close_error = err;
  };

  const int32_t stream2_id =
      client.SubmitUnaryRequest(GrpcRequestHeaders("/test.Svc/Unary"), "", std::move(s2_hooks));
  REQUIRE(stream2_id > 0);
  PumpClientToServer(client, server.s);

  SubmitServerResponse(server.s, stream2_id, &stream2_body);
  PumpBothWays(client, server.s, At(base, 2));

  CHECK(s2_closed);
  CHECK(s2_close_error == Http2Session::kHttp2NoError);
  CHECK(s2_received.size() == 8192);
  CHECK(s2_received == stream2_body.data);
  CHECK(s1_received.size() == 4096);
  CHECK_FALSE(s1_closed);
}

TEST_CASE("Http2Session: ResetStream sends RST_STREAM(CANCEL) to server and closes client stream") {
  const auto base = Base();
  Http2Session client({}, {}, base);
  REQUIRE(client.ok());

  RstCapture capture;
  RstCapturingServerSession server(&capture);
  Handshake(client, server.s, base);

  bool client_closed = false;
  uint32_t client_close_error = 999;
  Http2Session::StreamHooks hooks;
  hooks.on_close = [&](uint32_t err) {
    client_closed = true;
    client_close_error = err;
  };

  const int32_t stream_id = client.SubmitServerStreamRequest(
      GrpcRequestHeaders("/test.Svc/CancelMe"), "", std::move(hooks));
  REQUIRE(stream_id > 0);
  PumpClientToServer(client, server.s);
  CHECK(client.active_streams() == 1);
  CHECK_FALSE(client_closed);

  REQUIRE(client.ResetStream(stream_id));
  PumpClientToServer(client, server.s);

  CHECK(capture.count == 1);
  CHECK(capture.stream_id == stream_id);
  CHECK(capture.error_code == static_cast<uint32_t>(NGHTTP2_CANCEL));
  CHECK(client_closed);
  CHECK(client_close_error == Http2Session::kHttp2Cancel);
  CHECK(client.active_streams() == 0);
}

TEST_CASE("Http2Session: padded DATA frames return padding credit without inflating window") {
  // Server adds 100 bytes of padding to every DATA frame. Verify that both
  // manual-flow and unary streams return padding credit (preventing window
  // leakage) without prematurely crediting unread payload bytes or inflating
  // the peer's send window beyond initial_window_size.
  struct PaddedServerSession {
    nghttp2_session* s = nullptr;

    static ssize_t SelectPadding(nghttp2_session* /*session*/, const nghttp2_frame* frame,
                                 size_t max_payloadlen, void* /*user_data*/) {
      if (frame->hd.type == NGHTTP2_DATA && frame->hd.length > 0) {
        const size_t padded = frame->hd.length + 100;
        return static_cast<ssize_t>(std::min(padded, max_payloadlen));
      }
      return static_cast<ssize_t>(frame->hd.length);
    }

    PaddedServerSession() {
      nghttp2_session_callbacks* cb = nullptr;
      REQUIRE(nghttp2_session_callbacks_new(&cb) == 0);
      nghttp2_session_callbacks_set_select_padding_callback(cb, &SelectPadding);
      REQUIRE(nghttp2_session_server_new(&s, cb, nullptr) == 0);
      nghttp2_session_callbacks_del(cb);
      REQUIRE(nghttp2_submit_settings(s, NGHTTP2_FLAG_NONE, nullptr, 0) == 0);
    }
    ~PaddedServerSession() { nghttp2_session_del(s); }

    PaddedServerSession(const PaddedServerSession&) = delete;
    PaddedServerSession& operator=(const PaddedServerSession&) = delete;
  };

  const auto base = Base();
  Http2SessionOptions opts;
  opts.initial_window_size = 4096;

  Http2Session client(opts, {}, base);
  REQUIRE(client.ok());

  PaddedServerSession server;
  Handshake(client, server.s, base);

  // 1) Manual-flow stream: send 4096 bytes of payload across padded chunks
  // without calling ConsumeStream yet. Padding credit must be returned so the
  // full 4096 bytes of payload fit, and the peer's stream window must stall at
  // 0 (unread payload is never prematurely credited).
  ServerDataBuffer manual_body;
  manual_body.data = MakePattern(4096, 0x71) + MakePattern(4096, 0x72);
  manual_body.eof_ready = true;

  std::string manual_received;
  bool manual_closed = false;
  uint32_t manual_close_err = 999;
  Http2Session::StreamHooks manual_hooks;
  manual_hooks.on_data = [&](const uint8_t* data, size_t len) {
    manual_received.append(reinterpret_cast<const char*>(data), len);
  };
  manual_hooks.on_close = [&](uint32_t err) {
    manual_closed = true;
    manual_close_err = err;
  };

  const int32_t manual_sid = client.SubmitServerStreamRequest(
      GrpcRequestHeaders("/test.Svc/PaddedStream"), "", std::move(manual_hooks));
  REQUIRE(manual_sid > 0);
  PumpClientToServer(client, server.s);

  SubmitServerResponse(server.s, manual_sid, &manual_body);
  PumpBothWays(client, server.s, At(base, 1));

  // Unread payload must cap delivery strictly at initial_window_size (4096).
  CHECK(manual_received.size() <= 4096);
  CHECK(manual_received.size() >= 3800);
  CHECK_FALSE(manual_closed);
  CHECK(nghttp2_session_get_stream_remote_window_size(server.s, manual_sid) <= 4096);

  // Now consume what arrived and pump until EOF: total 8192 bytes delivered
  // cleanly despite padding on every DATA frame.
  size_t consumed = 0;
  for (int i = 0; i < 32 && !manual_closed; ++i) {
    if (manual_received.size() > consumed) {
      const size_t delta = manual_received.size() - consumed;
      consumed += delta;
      client.ConsumeStream(manual_sid, delta);
    }
    PumpBothWays(client, server.s, At(base, 2 + i));
  }
  CHECK(manual_closed);
  CHECK(manual_close_err == Http2Session::kHttp2NoError);
  CHECK(manual_received == manual_body.data);

  // 2) Unary stream with padded frames exceeding the 4096-byte stream window:
  // padding + payload credit are both returned automatically without stalling.
  ServerDataBuffer unary_body;
  unary_body.data = MakePattern(12000, 0x88);
  unary_body.eof_ready = true;

  std::string unary_received;
  bool unary_closed = false;
  uint32_t unary_close_err = 999;
  Http2Session::StreamHooks unary_hooks;
  unary_hooks.on_data = [&](const uint8_t* data, size_t len) {
    unary_received.append(reinterpret_cast<const char*>(data), len);
  };
  unary_hooks.on_close = [&](uint32_t err) {
    unary_closed = true;
    unary_close_err = err;
  };

  const int32_t unary_sid = client.SubmitUnaryRequest(GrpcRequestHeaders("/test.Svc/PaddedUnary"),
                                                      "", std::move(unary_hooks));
  REQUIRE(unary_sid > 0);
  PumpClientToServer(client, server.s);

  SubmitServerResponse(server.s, unary_sid, &unary_body);
  PumpBothWays(client, server.s, At(base, 40));

  CHECK(unary_closed);
  CHECK(unary_close_err == Http2Session::kHttp2NoError);
  CHECK(unary_received == unary_body.data);
  CHECK(nghttp2_session_get_remote_window_size(server.s) <= 65535);
}

TEST_CASE("Http2Session: padded DATA frames with stalled reader do not double-count padding") {
  // Send 20 padded DATA frames (each 100 B payload + 200 B padding = 300 B
  // flow-controlled bytes, total 6000 B > 4096 B window) to a manual-flow
  // stream WITHOUT calling ConsumeStream(). Verify padding is never
  // double-counted in stream WINDOW_UPDATE frames, backpressure still engages
  // once unread payload fills the window, and ConsumeStream() resumes flow.
  struct FixedChunkDataSource {
    std::string data;
    size_t offset = 0;
    size_t max_chunk = 100;
    bool eof_ready = false;
  };

  struct PaddingState {
    size_t pad_bytes = 200;
    int padded_frames = 0;
    uint64_t total_padding_added = 0;
  };

  struct FixedPaddedServerSession {
    nghttp2_session* s = nullptr;

    static ssize_t SelectPadding(nghttp2_session* /*session*/, const nghttp2_frame* frame,
                                 size_t max_payloadlen, void* user_data) {
      auto* st = static_cast<PaddingState*>(user_data);
      if (frame->hd.type == NGHTTP2_DATA && frame->hd.length > 0 && st->pad_bytes > 0) {
        const size_t target = std::min(frame->hd.length + st->pad_bytes, max_payloadlen);
        st->total_padding_added += (target - frame->hd.length);
        ++st->padded_frames;
        return static_cast<ssize_t>(target);
      }
      return static_cast<ssize_t>(frame->hd.length);
    }

    static ssize_t ReadCb(nghttp2_session* /*session*/, int32_t /*stream_id*/, uint8_t* buf,
                          size_t length, uint32_t* data_flags, nghttp2_data_source* source,
                          void* /*user_data*/) {
      auto* state = static_cast<FixedChunkDataSource*>(source->ptr);
      const size_t remaining = state->data.size() - state->offset;
      if (remaining == 0) {
        if (state->eof_ready) {
          *data_flags |= NGHTTP2_DATA_FLAG_EOF;
          return 0;
        }
        return NGHTTP2_ERR_DEFERRED;
      }
      const size_t n = std::min({length, remaining, state->max_chunk});
      std::memcpy(buf, state->data.data() + state->offset, n);
      state->offset += n;
      if (state->offset == state->data.size() && state->eof_ready) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
      }
      return static_cast<ssize_t>(n);
    }

    explicit FixedPaddedServerSession(PaddingState* st) {
      nghttp2_session_callbacks* cb = nullptr;
      REQUIRE(nghttp2_session_callbacks_new(&cb) == 0);
      nghttp2_session_callbacks_set_select_padding_callback(cb, &SelectPadding);
      REQUIRE(nghttp2_session_server_new(&s, cb, st) == 0);
      nghttp2_session_callbacks_del(cb);
      REQUIRE(nghttp2_submit_settings(s, NGHTTP2_FLAG_NONE, nullptr, 0) == 0);
    }
    ~FixedPaddedServerSession() { nghttp2_session_del(s); }

    FixedPaddedServerSession(const FixedPaddedServerSession&) = delete;
    FixedPaddedServerSession& operator=(const FixedPaddedServerSession&) = delete;
  };

  // Parses raw client->server HTTP/2 frames emitted after handshake and sums
  // stream-level WINDOW_UPDATE (type 0x8) increments for target_sid.
  struct WindowUpdateTap {
    std::vector<uint8_t> buf;
    size_t pos = 0;
    int32_t target_sid = -1;
    uint64_t stream_wu_sum = 0;

    void Feed(const uint8_t* data, size_t len) {
      buf.insert(buf.end(), data, data + len);
      while (buf.size() - pos >= 9) {
        const uint32_t frame_len = (static_cast<uint32_t>(buf[pos]) << 16) |
                                   (static_cast<uint32_t>(buf[pos + 1]) << 8) |
                                   static_cast<uint32_t>(buf[pos + 2]);
        if (buf.size() - pos < 9 + static_cast<size_t>(frame_len)) break;
        const uint8_t frame_type = buf[pos + 3];
        const int32_t frame_sid = static_cast<int32_t>(
            ((static_cast<uint32_t>(buf[pos + 5]) & 0x7fu) << 24) |
            (static_cast<uint32_t>(buf[pos + 6]) << 16) |
            (static_cast<uint32_t>(buf[pos + 7]) << 8) | static_cast<uint32_t>(buf[pos + 8]));
        if (frame_type == 0x8 && frame_sid == target_sid && frame_len == 4) {
          const uint32_t inc = ((static_cast<uint32_t>(buf[pos + 9]) & 0x7fu) << 24) |
                               (static_cast<uint32_t>(buf[pos + 10]) << 16) |
                               (static_cast<uint32_t>(buf[pos + 11]) << 8) |
                               static_cast<uint32_t>(buf[pos + 12]);
          stream_wu_sum += inc;
        }
        pos += 9 + static_cast<size_t>(frame_len);
      }
      if (pos > 0) {
        buf.erase(buf.begin(), buf.begin() + static_cast<std::ptrdiff_t>(pos));
        pos = 0;
      }
    }
  };

  const auto base = Base();
  Http2SessionOptions opts;
  opts.initial_window_size = 4096;

  Http2Session client(opts, {}, base);
  REQUIRE(client.ok());

  PaddingState pad_state;
  FixedPaddedServerSession server(&pad_state);
  Handshake(client, server.s, base);

  std::string received;
  bool closed = false;
  uint32_t close_err = 999;
  Http2Session::StreamHooks hooks;
  hooks.on_data = [&](const uint8_t* data, size_t len) {
    received.append(reinterpret_cast<const char*>(data), len);
  };
  hooks.on_close = [&](uint32_t err) {
    closed = true;
    close_err = err;
  };

  const int32_t sid = client.SubmitServerStreamRequest(
      GrpcRequestHeaders("/test.Svc/PaddedNoDoubleCount"), "", std::move(hooks));
  REQUIRE(sid > 0);
  PumpClientToServer(client, server.s);

  WindowUpdateTap tap;
  tap.target_sid = sid;

  auto pump_both_with_tap = [&](TimePoint now) {
    for (int i = 0; i < 100; ++i) {
      bool progressed = false;
      for (;;) {
        size_t len = 0;
        const uint8_t* p = client.PendingOutput(&len);
        if (len == 0) break;
        tap.Feed(p, len);
        REQUIRE(nghttp2_session_mem_recv(server.s, p, len) == static_cast<ssize_t>(len));
        client.ConsumeOutput(len);
        progressed = true;
      }
      for (;;) {
        const uint8_t* chunk = nullptr;
        const ssize_t n = nghttp2_session_mem_send(server.s, &chunk);
        if (n <= 0) break;
        REQUIRE(client.ReceiveBytes(now, chunk, static_cast<size_t>(n)));
        progressed = true;
      }
      if (!progressed) break;
    }
  };

  // Submit response with an initially empty deferred body, then drip 20 frames
  // of 100 B payload + 200 B padding (= 6000 B flow-controlled > 4096 B window)
  // WITHOUT calling ConsumeStream().
  FixedChunkDataSource body;
  body.max_chunk = 100;
  body.eof_ready = false;

  static constexpr char kStatusName[] = ":status";
  static constexpr char kStatusVal[] = "200";
  static constexpr char kCtName[] = "content-type";
  static constexpr char kCtVal[] = "application/grpc";
  const nghttp2_nv nva[] = {
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusVal)), sizeof(kStatusName) - 1,
       sizeof(kStatusVal) - 1, NGHTTP2_NV_FLAG_NONE},
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kCtName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kCtVal)), sizeof(kCtName) - 1,
       sizeof(kCtVal) - 1, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_data_provider provider{};
  provider.source.ptr = &body;
  provider.read_callback = &FixedPaddedServerSession::ReadCb;
  REQUIRE(nghttp2_submit_response(server.s, sid, nva, 2, &provider) == 0);
  // Flush response HEADERS and let ReadCb enter NGHTTP2_ERR_DEFERRED.
  pump_both_with_tap(At(base, 1));

  for (int i = 0; i < 20; ++i) {
    body.data += MakePattern(100, static_cast<uint8_t>(i));
    REQUIRE(nghttp2_session_resume_data(server.s, sid) == 0);
    pump_both_with_tap(At(base, 2 + i));
  }

  CHECK(client.ok());
  CHECK(client.error().empty());
  CHECK(pad_state.padded_frames == 20);
  CHECK(pad_state.total_padding_added == 20u * 200u);
  CHECK(received.size() == 20u * 100u);
  CHECK_FALSE(closed);
  // Padding credit is returned by nghttp2 automatically, NEVER double-counted.
  CHECK(tap.stream_wu_sum > 0u);
  CHECK(tap.stream_wu_sum <= 20u * 200u);
  // Unread payload (2000 B) has reduced the peer's stream send window below 4096.
  CHECK(nghttp2_session_get_stream_remote_window_size(server.s, sid) < 4096);

  // Now push enough additional payload (4096 B + EOF) to fill the remaining
  // stream window WITHOUT calling ConsumeStream(): backpressure must block.
  body.data += MakePattern(4096, 0xaa);
  body.eof_ready = true;
  REQUIRE(nghttp2_session_resume_data(server.s, sid) == 0);
  pump_both_with_tap(At(base, 30));

  CHECK(client.ok());
  CHECK(client.error().empty());
  CHECK(received.size() <= 4096u);
  CHECK(received.size() > 2000u);
  CHECK_FALSE(closed);
  const int32_t blocked_window = nghttp2_session_get_stream_remote_window_size(server.s, sid);
  CHECK(blocked_window < 4096);
  // Additional pumping without ConsumeStream() must not advance delivery.
  const size_t stalled_bytes = received.size();
  pump_both_with_tap(At(base, 31));
  CHECK(received.size() == stalled_bytes);
  CHECK_FALSE(closed);

  // Calling ConsumeStream() resumes flow and drains the remainder cleanly.
  size_t consumed = 0;
  for (int i = 0; i < 64 && !closed; ++i) {
    if (received.size() > consumed) {
      const size_t delta = received.size() - consumed;
      consumed += delta;
      client.ConsumeStream(sid, delta);
    }
    pump_both_with_tap(At(base, 32 + i));
  }
  CHECK(closed);
  CHECK(close_err == Http2Session::kHttp2NoError);
  CHECK(received == body.data);
  CHECK(client.ok());
  CHECK(client.error().empty());
}

TEST_CASE("Http2Session: post-GOAWAY submit is non-fatal and healthy in-flight stream finishes") {
  const auto base = Base();
  Http2Session client({}, {}, base);
  REQUIRE(client.ok());

  ServerSession server;
  Handshake(client, server.s, base);

  bool s1_headers_seen = false;
  bool s1_headers_end_stream = true;
  std::string s1_data;
  Http2Session::HeaderList s1_trailers;
  bool s1_trailers_seen = false;
  bool s1_closed = false;
  uint32_t s1_close_err = 999;

  Http2Session::StreamHooks s1_hooks;
  s1_hooks.on_response_headers = [&](Http2Session::HeaderList /*headers*/, bool end_stream) {
    s1_headers_seen = true;
    s1_headers_end_stream = end_stream;
  };
  s1_hooks.on_data = [&](const uint8_t* data, size_t len) {
    s1_data.append(reinterpret_cast<const char*>(data), len);
  };
  s1_hooks.on_trailers = [&](Http2Session::HeaderList trailers) {
    s1_trailers_seen = true;
    s1_trailers = std::move(trailers);
  };
  s1_hooks.on_close = [&](uint32_t err) {
    s1_closed = true;
    s1_close_err = err;
  };

  const int32_t stream1 =
      client.SubmitUnaryRequest(GrpcRequestHeaders("/test.Svc/Unary1"), "", std::move(s1_hooks));
  REQUIRE(stream1 > 0);
  PumpClientToServer(client, server.s);

  // Server sends GOAWAY allowing stream1 to complete, then pumps to client.
  REQUIRE(nghttp2_submit_goaway(server.s, NGHTTP2_FLAG_NONE, stream1, NGHTTP2_NO_ERROR, nullptr,
                                0) == 0);
  PumpServerToClient(server.s, client, At(base, 1));
  REQUIRE(client.draining());

  // Submitting stream2 after GOAWAY must fail non-fatally: call_error() set,
  // fatal error() stays empty.
  const int32_t stream2 = client.SubmitUnaryRequest(GrpcRequestHeaders("/test.Svc/Unary2"), "", {});
  CHECK(stream2 < 0);
  CHECK_FALSE(client.call_error().empty());
  CHECK(client.error().empty());
  CHECK(client.ok());

  // Server now sends response headers, a gRPC-framed payload, and trailers
  // (grpc-status: 0) on stream1.
  struct FramedBodyWithTrailers {
    std::string payload;
    size_t offset = 0;

    static ssize_t ReadCb(nghttp2_session* session, int32_t stream_id, uint8_t* buf, size_t length,
                          uint32_t* data_flags, nghttp2_data_source* source, void* /*user_data*/) {
      auto* self = static_cast<FramedBodyWithTrailers*>(source->ptr);
      const size_t remaining = self->payload.size() - self->offset;
      const size_t n = std::min(length, remaining);
      std::memcpy(buf, self->payload.data() + self->offset, n);
      self->offset += n;
      if (self->offset == self->payload.size()) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF | NGHTTP2_DATA_FLAG_NO_END_STREAM;
        static constexpr char kGsName[] = "grpc-status";
        static constexpr char kGsVal[] = "0";
        const nghttp2_nv trailers[] = {
            {reinterpret_cast<uint8_t*>(const_cast<char*>(kGsName)),
             reinterpret_cast<uint8_t*>(const_cast<char*>(kGsVal)), sizeof(kGsName) - 1,
             sizeof(kGsVal) - 1, NGHTTP2_NV_FLAG_NONE},
        };
        REQUIRE(nghttp2_submit_trailer(session, stream_id, trailers, 1) == 0);
      }
      return static_cast<ssize_t>(n);
    }
  };

  // gRPC-framed message: flag=0, length=5, payload="hello".
  FramedBodyWithTrailers s1_body;
  s1_body.payload.assign("\x00\x00\x00\x00\x05hello", 10);

  static constexpr char kStatusName[] = ":status";
  static constexpr char kStatusVal[] = "200";
  static constexpr char kCtName[] = "content-type";
  static constexpr char kCtVal[] = "application/grpc";
  const nghttp2_nv resp_headers[] = {
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusVal)), sizeof(kStatusName) - 1,
       sizeof(kStatusVal) - 1, NGHTTP2_NV_FLAG_NONE},
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kCtName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kCtVal)), sizeof(kCtName) - 1,
       sizeof(kCtVal) - 1, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_data_provider provider{};
  provider.source.ptr = &s1_body;
  provider.read_callback = &FramedBodyWithTrailers::ReadCb;
  REQUIRE(nghttp2_submit_response(server.s, stream1, resp_headers, 2, &provider) == 0);

  PumpBothWays(client, server.s, At(base, 2));

  CHECK(s1_headers_seen);
  CHECK_FALSE(s1_headers_end_stream);
  CHECK(s1_data == s1_body.payload);
  CHECK(s1_trailers_seen);
  bool saw_grpc_status_0 = false;
  for (const auto& h : s1_trailers) {
    if (h.first == "grpc-status" && h.second == "0") saw_grpc_status_0 = true;
  }
  CHECK(saw_grpc_status_0);
  CHECK(s1_closed);
  CHECK(s1_close_err == Http2Session::kHttp2NoError);
  CHECK(client.error().empty());
}

TEST_CASE("Http2Session: connection window > 65535 B does not stall") {
  const auto base = Base();
  Http2SessionOptions opts;
  opts.initial_window_size = 32768;

  Http2Session client(opts, {}, base);
  REQUIRE(client.ok());

  ServerSession server;
  Handshake(client, server.s, base);

  constexpr size_t kTotalBytes = 100 * 1024;
  ServerDataBuffer server_body;
  server_body.data = MakePattern(kTotalBytes, 0x42);
  server_body.eof_ready = true;

  std::string received;
  bool closed = false;
  uint32_t close_err = 999;
  Http2Session::StreamHooks hooks;
  hooks.on_data = [&](const uint8_t* data, size_t len) {
    received.append(reinterpret_cast<const char*>(data), len);
  };
  hooks.on_close = [&](uint32_t err) {
    closed = true;
    close_err = err;
  };

  const int32_t sid = client.SubmitServerStreamRequest(GrpcRequestHeaders("/test.Svc/LargeStream"),
                                                       "", std::move(hooks));
  REQUIRE(sid > 0);
  PumpClientToServer(client, server.s);

  SubmitServerResponse(server.s, sid, &server_body);

  size_t consumed = 0;
  for (int iter = 0; iter < 64 && !closed; ++iter) {
    PumpServerToClient(server.s, client, At(base, 1 + iter));
    if (received.size() > consumed) {
      const size_t delta = received.size() - consumed;
      consumed += delta;
      client.ConsumeStream(sid, delta);
    }
    PumpClientToServer(client, server.s);
  }

  CHECK(closed);
  CHECK(close_err == Http2Session::kHttp2NoError);
  CHECK(received.size() == kTotalBytes);
  CHECK(received == server_body.data);
  CHECK(client.ok());
  CHECK(client.error().empty());
}

TEST_CASE("Http2Session: manual-flow padded long stream has zero window drift") {
  struct PaddedLongStreamState {
    size_t pad_bytes = 256;  // 1-byte Pad Length (= 255) + 255 zero padding bytes
    int padded_frames = 0;
    uint64_t total_padding_added = 0;
  };

  struct ChunkedDataSource {
    std::string data;
    size_t offset = 0;
    size_t max_chunk = 100;
    bool eof_ready = false;
  };

  struct PaddedServer {
    nghttp2_session* s = nullptr;

    static ssize_t SelectPadding(nghttp2_session* /*session*/, const nghttp2_frame* frame,
                                 size_t max_payloadlen, void* user_data) {
      auto* st = static_cast<PaddedLongStreamState*>(user_data);
      if (frame->hd.type == NGHTTP2_DATA && frame->hd.length > 0 && st->pad_bytes > 0) {
        const size_t target = std::min(frame->hd.length + st->pad_bytes, max_payloadlen);
        st->total_padding_added += (target - frame->hd.length);
        ++st->padded_frames;
        return static_cast<ssize_t>(target);
      }
      return static_cast<ssize_t>(frame->hd.length);
    }

    static ssize_t ReadCb(nghttp2_session* /*session*/, int32_t /*stream_id*/, uint8_t* buf,
                          size_t length, uint32_t* data_flags, nghttp2_data_source* source,
                          void* /*user_data*/) {
      auto* state = static_cast<ChunkedDataSource*>(source->ptr);
      const size_t remaining = state->data.size() - state->offset;
      if (remaining == 0) {
        if (state->eof_ready) {
          *data_flags |= NGHTTP2_DATA_FLAG_EOF;
          return 0;
        }
        return NGHTTP2_ERR_DEFERRED;
      }
      const size_t n = std::min({length, remaining, state->max_chunk});
      std::memcpy(buf, state->data.data() + state->offset, n);
      state->offset += n;
      if (state->offset == state->data.size() && state->eof_ready) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
      }
      return static_cast<ssize_t>(n);
    }

    explicit PaddedServer(PaddedLongStreamState* st) {
      nghttp2_session_callbacks* cb = nullptr;
      REQUIRE(nghttp2_session_callbacks_new(&cb) == 0);
      nghttp2_session_callbacks_set_select_padding_callback(cb, &SelectPadding);
      REQUIRE(nghttp2_session_server_new(&s, cb, st) == 0);
      nghttp2_session_callbacks_del(cb);
      REQUIRE(nghttp2_submit_settings(s, NGHTTP2_FLAG_NONE, nullptr, 0) == 0);
    }
    ~PaddedServer() { nghttp2_session_del(s); }

    PaddedServer(const PaddedServer&) = delete;
    PaddedServer& operator=(const PaddedServer&) = delete;
  };

  constexpr uint32_t kWindow = 65536;
  constexpr int kFrames = 400;
  constexpr size_t kPayloadPerFrame = 100;
  constexpr size_t kPadPerFrame = 256;  // NGHTTP2_FLAG_PADDED: 1-byte padlen (255) + 255 bytes

  const auto base = Base();
  Http2SessionOptions opts;
  opts.initial_window_size = kWindow;

  Http2Session client(opts, {}, base);
  REQUIRE(client.ok());

  PaddedLongStreamState pad_state;
  pad_state.pad_bytes = kPadPerFrame;
  PaddedServer server(&pad_state);
  Handshake(client, server.s, base);

  std::string received;
  bool closed = false;
  uint32_t close_err = 999;
  Http2Session::StreamHooks hooks;
  hooks.on_data = [&](const uint8_t* data, size_t len) {
    received.append(reinterpret_cast<const char*>(data), len);
  };
  hooks.on_close = [&](uint32_t err) {
    closed = true;
    close_err = err;
  };

  const int32_t sid = client.SubmitServerStreamRequest(
      GrpcRequestHeaders("/test.Svc/PaddedLongStream"), "", std::move(hooks));
  REQUIRE(sid > 0);
  PumpClientToServer(client, server.s);

  ChunkedDataSource body;
  body.max_chunk = kPayloadPerFrame;
  body.eof_ready = false;

  static constexpr char kStatusName[] = ":status";
  static constexpr char kStatusVal[] = "200";
  static constexpr char kCtName[] = "content-type";
  static constexpr char kCtVal[] = "application/grpc";
  const nghttp2_nv nva[] = {
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusVal)), sizeof(kStatusName) - 1,
       sizeof(kStatusVal) - 1, NGHTTP2_NV_FLAG_NONE},
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kCtName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kCtVal)), sizeof(kCtName) - 1,
       sizeof(kCtVal) - 1, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_data_provider provider{};
  provider.source.ptr = &body;
  provider.read_callback = &PaddedServer::ReadCb;
  REQUIRE(nghttp2_submit_response(server.s, sid, nva, 2, &provider) == 0);
  PumpBothWays(client, server.s, At(base, 1));
  REQUIRE(nghttp2_session_get_stream_remote_window_size(server.s, sid) ==
          static_cast<int32_t>(kWindow));

  std::string expected_payload;
  expected_payload.reserve(static_cast<size_t>(kFrames) * kPayloadPerFrame);

  for (int i = 0; i < kFrames; ++i) {
    const std::string chunk = MakePattern(kPayloadPerFrame, static_cast<uint8_t>(i & 0xff));
    expected_payload += chunk;
    body.data += chunk;
    REQUIRE(nghttp2_session_resume_data(server.s, sid) == 0);
    PumpServerToClient(server.s, client, At(base, 2 + i));
    REQUIRE(received.size() == static_cast<size_t>(i + 1) * kPayloadPerFrame);

    client.ConsumeStream(sid, kPayloadPerFrame);
    PumpClientToServer(client, server.s);

    // Server's tracked stream window must never stall at 0 across all 400 frames.
    CHECK(nghttp2_session_get_stream_remote_window_size(server.s, sid) > 0);
  }

  CHECK(pad_state.padded_frames == kFrames);
  CHECK(pad_state.total_padding_added == static_cast<uint64_t>(kFrames) * kPadPerFrame);
  CHECK(pad_state.total_padding_added == 102400u);
  CHECK(received.size() == 40000u);
  CHECK(received == expected_payload);
  CHECK_FALSE(closed);

  // Send a final full-window 65,536-byte unconsumed payload to prove zero
  // drift/leak across the 102,400 B of padding: all 65,536 B must arrive
  // without stalling, drop the remote window to exactly 0, and return to
  // exactly 65,536 after ConsumeStream(sid, 65536).
  pad_state.pad_bytes = 0;
  body.max_chunk = 16384;
  const std::string final_payload = MakePattern(kWindow, 0x5a);
  body.data += final_payload;
  REQUIRE(nghttp2_session_resume_data(server.s, sid) == 0);
  PumpBothWays(client, server.s, At(base, 500));

  CHECK(received.size() == 40000u + kWindow);
  CHECK(received.substr(40000u) == final_payload);
  CHECK(nghttp2_session_get_stream_remote_window_size(server.s, sid) == 0);

  client.ConsumeStream(sid, kWindow);
  PumpClientToServer(client, server.s);
  CHECK(nghttp2_session_get_stream_remote_window_size(server.s, sid) ==
        static_cast<int32_t>(kWindow));

  // Close stream cleanly (stream is un-deferred after WINDOW_UPDATE).
  body.eof_ready = true;
  PumpBothWays(client, server.s, At(base, 501));
  CHECK(closed);
  CHECK(close_err == Http2Session::kHttp2NoError);
  CHECK(client.ok());
  CHECK(client.error().empty());
}

TEST_CASE(
    "Http2Session: manual-flow tight zero-slack window with padded single message does not "
    "deadlock") {
  struct TightPadState {
    size_t pad_bytes = 120;
    uint64_t total_padding_added = 0;
  };

  struct TightDataSource {
    std::string data;
    size_t offset = 0;
    size_t max_chunk = 100;
    bool eof_ready = false;
  };

  struct TightPaddedServer {
    nghttp2_session* s = nullptr;

    static ssize_t SelectPadding(nghttp2_session* /*session*/, const nghttp2_frame* frame,
                                 size_t max_payloadlen, void* user_data) {
      auto* st = static_cast<TightPadState*>(user_data);
      if (frame->hd.type == NGHTTP2_DATA && frame->hd.length > 0 && st->pad_bytes > 0) {
        const size_t target = std::min(frame->hd.length + st->pad_bytes, max_payloadlen);
        st->total_padding_added += (target - frame->hd.length);
        return static_cast<ssize_t>(target);
      }
      return static_cast<ssize_t>(frame->hd.length);
    }

    static ssize_t ReadCb(nghttp2_session* /*session*/, int32_t /*stream_id*/, uint8_t* buf,
                          size_t length, uint32_t* data_flags, nghttp2_data_source* source,
                          void* /*user_data*/) {
      auto* state = static_cast<TightDataSource*>(source->ptr);
      const size_t remaining = state->data.size() - state->offset;
      if (remaining == 0) {
        if (state->eof_ready) {
          *data_flags |= NGHTTP2_DATA_FLAG_EOF;
          return 0;
        }
        return NGHTTP2_ERR_DEFERRED;
      }
      const size_t n = std::min({length, remaining, state->max_chunk});
      std::memcpy(buf, state->data.data() + state->offset, n);
      state->offset += n;
      if (state->offset == state->data.size() && state->eof_ready) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF;
      }
      return static_cast<ssize_t>(n);
    }

    explicit TightPaddedServer(TightPadState* st) {
      nghttp2_session_callbacks* cb = nullptr;
      REQUIRE(nghttp2_session_callbacks_new(&cb) == 0);
      nghttp2_session_callbacks_set_select_padding_callback(cb, &SelectPadding);
      REQUIRE(nghttp2_session_server_new(&s, cb, st) == 0);
      nghttp2_session_callbacks_del(cb);
      REQUIRE(nghttp2_submit_settings(s, NGHTTP2_FLAG_NONE, nullptr, 0) == 0);
    }
    ~TightPaddedServer() { nghttp2_session_del(s); }

    TightPaddedServer(const TightPaddedServer&) = delete;
    TightPaddedServer& operator=(const TightPaddedServer&) = delete;
  };

  constexpr uint32_t kWindow = 8192;

  const auto base = Base();
  Http2SessionOptions opts;
  opts.initial_window_size = kWindow;

  Http2Session client(opts, {}, base);
  REQUIRE(client.ok());

  TightPadState pad_state;
  TightPaddedServer server(&pad_state);
  Handshake(client, server.s, base);

  std::string received;
  bool closed = false;
  uint32_t close_err = 999;
  Http2Session::StreamHooks hooks;
  hooks.on_data = [&](const uint8_t* data, size_t len) {
    received.append(reinterpret_cast<const char*>(data), len);
  };
  hooks.on_close = [&](uint32_t err) {
    closed = true;
    close_err = err;
  };

  const int32_t sid = client.SubmitServerStreamRequest(
      GrpcRequestHeaders("/test.Svc/TightZeroSlack"), "", std::move(hooks));
  REQUIRE(sid > 0);
  PumpClientToServer(client, server.s);

  TightDataSource body;
  body.max_chunk = 100;
  body.eof_ready = false;

  static constexpr char kStatusName[] = ":status";
  static constexpr char kStatusVal[] = "200";
  static constexpr char kCtName[] = "content-type";
  static constexpr char kCtVal[] = "application/grpc";
  const nghttp2_nv nva[] = {
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusVal)), sizeof(kStatusName) - 1,
       sizeof(kStatusVal) - 1, NGHTTP2_NV_FLAG_NONE},
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kCtName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kCtVal)), sizeof(kCtName) - 1,
       sizeof(kCtVal) - 1, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_data_provider provider{};
  provider.source.ptr = &body;
  provider.read_callback = &TightPaddedServer::ReadCb;
  REQUIRE(nghttp2_submit_response(server.s, sid, nva, 2, &provider) == 0);
  PumpBothWays(client, server.s, At(base, 1));

  // Deliver a single 8,192-byte message split across padded DATA frames
  // (80 frames of 100 B payload + 120 B padding, then the final 192 B across
  // remaining window) WITHOUT calling ConsumeStream() at all.
  const std::string expected_msg = MakePattern(kWindow, 0x3c);
  for (int i = 0; i < 80; ++i) {
    CHECK(nghttp2_session_get_stream_remote_window_size(server.s, sid) >= 220);
    body.data.append(expected_msg, static_cast<size_t>(i) * 100u, 100u);
    REQUIRE(nghttp2_session_resume_data(server.s, sid) == 0);
    PumpServerToClient(server.s, client, At(base, 2 + i));
    PumpClientToServer(client, server.s);
    REQUIRE(received.size() == static_cast<size_t>(i + 1) * 100u);
  }

  // Final 192 bytes arrive without calling ConsumeStream(); total padding > 8,192 B.
  body.data.append(expected_msg, 8000u, 192u);
  REQUIRE(nghttp2_session_resume_data(server.s, sid) == 0);
  PumpBothWays(client, server.s, At(base, 90));

  CHECK(pad_state.total_padding_added > kWindow);
  CHECK(received.size() == kWindow);
  CHECK(received == expected_msg);
  CHECK_FALSE(closed);
  CHECK(nghttp2_session_get_stream_remote_window_size(server.s, sid) == 0);

  // Consume the single 8,192-byte message: remote window must return to 8,192.
  client.ConsumeStream(sid, kWindow);
  PumpClientToServer(client, server.s);
  CHECK(nghttp2_session_get_stream_remote_window_size(server.s, sid) ==
        static_cast<int32_t>(kWindow));

  // Clean close (stream is un-deferred after WINDOW_UPDATE).
  body.eof_ready = true;
  PumpBothWays(client, server.s, At(base, 91));
  CHECK(closed);
  CHECK(close_err == Http2Session::kHttp2NoError);
  CHECK(client.ok());
  CHECK(client.error().empty());
}

TEST_CASE("Http2Session: 1xx informational response HEADERS before final 200 are ignored") {
  const auto base = Base();
  Http2Session client({}, {}, base);
  REQUIRE(client.ok());

  ServerSession server;
  Handshake(client, server.s, base);

  int headers_calls = 0;
  Http2Session::HeaderList received_headers;
  bool headers_end_stream = true;
  std::string received_data;
  int trailers_calls = 0;
  Http2Session::HeaderList received_trailers;
  bool closed = false;
  uint32_t close_err = 999;

  Http2Session::StreamHooks hooks;
  hooks.on_response_headers = [&](Http2Session::HeaderList headers, bool end_stream) {
    ++headers_calls;
    received_headers = std::move(headers);
    headers_end_stream = end_stream;
  };
  hooks.on_data = [&](const uint8_t* data, size_t len) {
    received_data.append(reinterpret_cast<const char*>(data), len);
  };
  hooks.on_trailers = [&](Http2Session::HeaderList trailers) {
    ++trailers_calls;
    received_trailers = std::move(trailers);
  };
  hooks.on_close = [&](uint32_t err) {
    closed = true;
    close_err = err;
  };

  const int32_t sid =
      client.SubmitUnaryRequest(GrpcRequestHeaders("/test.Svc/EarlyHints"), "", std::move(hooks));
  REQUIRE(sid > 0);
  PumpClientToServer(client, server.s);

  // Send 103 Early Hints informational HEADERS without END_STREAM.
  static constexpr char kStatusName[] = ":status";
  static constexpr char kStatus103[] = "103";
  static constexpr char kLinkName[] = "link";
  static constexpr char kLinkVal[] = "</a.css>; rel=preload";
  const nghttp2_nv info_nva[] = {
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kStatus103)), sizeof(kStatusName) - 1,
       sizeof(kStatus103) - 1, NGHTTP2_NV_FLAG_NONE},
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kLinkName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kLinkVal)), sizeof(kLinkName) - 1,
       sizeof(kLinkVal) - 1, NGHTTP2_NV_FLAG_NONE},
  };
  REQUIRE(nghttp2_submit_headers(server.s, NGHTTP2_FLAG_NONE, sid, nullptr, info_nva, 2, nullptr) ==
          0);
  PumpServerToClient(server.s, client, At(base, 1));

  // Informational 103 must not invoke on_response_headers or on_trailers.
  CHECK(headers_calls == 0);
  CHECK(trailers_calls == 0);
  CHECK_FALSE(closed);

  // Now send final 200 HEADERS + DATA + grpc-status: 0 trailers with END_STREAM.
  struct BodyWithTrailers {
    std::string payload;
    size_t offset = 0;

    static ssize_t ReadCb(nghttp2_session* session, int32_t stream_id, uint8_t* buf, size_t length,
                          uint32_t* data_flags, nghttp2_data_source* source, void* /*user_data*/) {
      auto* self = static_cast<BodyWithTrailers*>(source->ptr);
      const size_t remaining = self->payload.size() - self->offset;
      const size_t n = std::min(length, remaining);
      std::memcpy(buf, self->payload.data() + self->offset, n);
      self->offset += n;
      if (self->offset == self->payload.size()) {
        *data_flags |= NGHTTP2_DATA_FLAG_EOF | NGHTTP2_DATA_FLAG_NO_END_STREAM;
        static constexpr char kGsName[] = "grpc-status";
        static constexpr char kGsVal[] = "0";
        const nghttp2_nv trailers[] = {
            {reinterpret_cast<uint8_t*>(const_cast<char*>(kGsName)),
             reinterpret_cast<uint8_t*>(const_cast<char*>(kGsVal)), sizeof(kGsName) - 1,
             sizeof(kGsVal) - 1, NGHTTP2_NV_FLAG_NONE},
        };
        REQUIRE(nghttp2_submit_trailer(session, stream_id, trailers, 1) == 0);
      }
      return static_cast<ssize_t>(n);
    }
  };

  BodyWithTrailers body;
  body.payload.assign("\x00\x00\x00\x00\x05hello", 10);

  static constexpr char kStatus200[] = "200";
  static constexpr char kCtName[] = "content-type";
  static constexpr char kCtVal[] = "application/grpc";
  const nghttp2_nv resp_nva[] = {
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kStatus200)), sizeof(kStatusName) - 1,
       sizeof(kStatus200) - 1, NGHTTP2_NV_FLAG_NONE},
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kCtName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kCtVal)), sizeof(kCtName) - 1,
       sizeof(kCtVal) - 1, NGHTTP2_NV_FLAG_NONE},
  };
  nghttp2_data_provider provider{};
  provider.source.ptr = &body;
  provider.read_callback = &BodyWithTrailers::ReadCb;
  REQUIRE(nghttp2_submit_response(server.s, sid, resp_nva, 2, &provider) == 0);

  PumpBothWays(client, server.s, At(base, 2));

  auto has_header = [](const Http2Session::HeaderList& list, const std::string& k,
                       const std::string& v) {
    for (const auto& h : list) {
      if (h.first == k && h.second == v) return true;
    }
    return false;
  };
  auto has_key = [](const Http2Session::HeaderList& list, const std::string& k) {
    for (const auto& h : list) {
      if (h.first == k) return true;
    }
    return false;
  };

  CHECK(headers_calls == 1);
  CHECK_FALSE(headers_end_stream);
  CHECK(has_header(received_headers, ":status", "200"));
  CHECK(has_header(received_headers, "content-type", "application/grpc"));
  CHECK_FALSE(has_header(received_headers, ":status", "103"));
  CHECK_FALSE(has_key(received_headers, "link"));

  CHECK(received_data == body.payload);

  CHECK(trailers_calls == 1);
  CHECK(has_header(received_trailers, "grpc-status", "0"));
  CHECK_FALSE(has_key(received_trailers, ":status"));
  CHECK_FALSE(has_key(received_trailers, "content-type"));

  CHECK(closed);
  CHECK(close_err == Http2Session::kHttp2NoError);
  CHECK(client.ok());
  CHECK(client.error().empty());
}

TEST_CASE(
    "on_response_end_stream fires on trailers-only response even with peer zero initial window") {
  struct ZeroWindowServer {
    nghttp2_session* s = nullptr;
    int headers_received = 0;
    int32_t last_stream_id = -1;
    bool last_headers_end_stream = false;

    static int OnFrameRecv(nghttp2_session* /*session*/, const nghttp2_frame* frame,
                           void* user_data) {
      auto* self = static_cast<ZeroWindowServer*>(user_data);
      if (frame->hd.type == NGHTTP2_HEADERS && frame->headers.cat == NGHTTP2_HCAT_REQUEST) {
        ++self->headers_received;
        self->last_stream_id = frame->hd.stream_id;
        self->last_headers_end_stream = (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0;
      }
      return 0;
    }

    ZeroWindowServer() {
      nghttp2_session_callbacks* cb = nullptr;
      REQUIRE(nghttp2_session_callbacks_new(&cb) == 0);
      nghttp2_session_callbacks_set_on_frame_recv_callback(cb, &OnFrameRecv);
      REQUIRE(nghttp2_session_server_new(&s, cb, this) == 0);
      nghttp2_session_callbacks_del(cb);
      const nghttp2_settings_entry iv[] = {
          {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, 0},
      };
      REQUIRE(nghttp2_submit_settings(s, NGHTTP2_FLAG_NONE, iv, 1) == 0);
    }
    ~ZeroWindowServer() { nghttp2_session_del(s); }

    ZeroWindowServer(const ZeroWindowServer&) = delete;
    ZeroWindowServer& operator=(const ZeroWindowServer&) = delete;
  };

  const auto base = Base();
  Http2Session client({}, {}, base);
  REQUIRE(client.ok());

  ZeroWindowServer server;
  Handshake(client, server.s, base);

  int32_t stream_id = -1;
  int headers_calls = 0;
  bool headers_end_stream = false;
  Http2Session::HeaderList received_headers;
  bool end_stream_fired = false;
  bool local_open_in_end_stream = false;
  bool closed = false;
  uint32_t close_err = 999;

  Http2Session::StreamHooks hooks;
  hooks.on_response_headers = [&](Http2Session::HeaderList headers, bool end_stream) {
    ++headers_calls;
    headers_end_stream = end_stream;
    received_headers = std::move(headers);
  };
  hooks.on_response_end_stream = [&] {
    end_stream_fired = true;
    local_open_in_end_stream = client.IsStreamLocalOpen(stream_id);
  };
  hooks.on_close = [&](uint32_t err) {
    closed = true;
    close_err = err;
  };

  const std::string body("\x00\x00\x00\x00\x05hello", 10);
  stream_id =
      client.SubmitUnaryRequest(GrpcRequestHeaders("/test.Svc/ZeroWindow"), body, std::move(hooks));
  REQUIRE(stream_id > 0);
  PumpClientToServer(client, server.s);

  // Client sent HEADERS without END_STREAM because server's initial window is 0.
  CHECK(server.headers_received == 1);
  CHECK(server.last_stream_id == stream_id);
  CHECK_FALSE(server.last_headers_end_stream);
  CHECK(client.IsStreamLocalOpen(stream_id));

  // Server immediately replies with a trailers-only HEADERS + END_STREAM and NO RST_STREAM.
  static constexpr char kStatusName[] = ":status";
  static constexpr char kStatusVal[] = "200";
  static constexpr char kCtName[] = "content-type";
  static constexpr char kCtVal[] = "application/grpc";
  static constexpr char kGsName[] = "grpc-status";
  static constexpr char kGsVal[] = "7";
  static constexpr char kGmName[] = "grpc-message";
  static constexpr char kGmVal[] = "denied";
  const nghttp2_nv nva[] = {
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kStatusVal)), sizeof(kStatusName) - 1,
       sizeof(kStatusVal) - 1, NGHTTP2_NV_FLAG_NONE},
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kCtName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kCtVal)), sizeof(kCtName) - 1,
       sizeof(kCtVal) - 1, NGHTTP2_NV_FLAG_NONE},
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kGsName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kGsVal)), sizeof(kGsName) - 1,
       sizeof(kGsVal) - 1, NGHTTP2_NV_FLAG_NONE},
      {reinterpret_cast<uint8_t*>(const_cast<char*>(kGmName)),
       reinterpret_cast<uint8_t*>(const_cast<char*>(kGmVal)), sizeof(kGmName) - 1,
       sizeof(kGmVal) - 1, NGHTTP2_NV_FLAG_NONE},
  };
  REQUIRE(nghttp2_submit_headers(server.s, NGHTTP2_FLAG_END_STREAM, stream_id, nullptr, nva,
                                 sizeof(nva) / sizeof(nva[0]), nullptr) == 0);
  PumpServerToClient(server.s, client, At(base, 1));

  auto has_header = [](const Http2Session::HeaderList& list, const std::string& k,
                       const std::string& v) {
    for (const auto& h : list) {
      if (h.first == k && h.second == v) return true;
    }
    return false;
  };

  CHECK(headers_calls == 1);
  CHECK(headers_end_stream);
  CHECK(has_header(received_headers, ":status", "200"));
  CHECK(has_header(received_headers, "content-type", "application/grpc"));
  CHECK(has_header(received_headers, "grpc-status", "7"));
  CHECK(has_header(received_headers, "grpc-message", "denied"));
  CHECK(end_stream_fired);
  CHECK(local_open_in_end_stream);
  CHECK(client.IsStreamLocalOpen(stream_id));
  CHECK_FALSE(closed);

  // Resetting the blocked upload side and flushing closes the stream cleanly.
  CHECK(client.ResetStream(stream_id, Http2Session::kHttp2Cancel));
  PumpClientToServer(client, server.s);

  CHECK(closed);
  CHECK(close_err == Http2Session::kHttp2Cancel);
  CHECK(client.active_streams() == 0);
  CHECK(client.ok());
  CHECK(client.error().empty());
}

TEST_CASE("WantRead and WantWrite both become false after peer connection-error GOAWAY") {
  const auto base = Base();
  Http2Session client({}, {}, base);
  REQUIRE(client.ok());

  ServerSession server;
  Handshake(client, server.s, base);
  CHECK(client.WantRead());

  // RFC 7540 §6.5: SETTINGS frame with ACK flag and non-zero payload length
  // triggers a connection-level PROTOCOL_ERROR and outbound GOAWAY.
  const uint8_t bad_settings_ack[] = {
      0x00, 0x00, 0x06,        // length = 6
      0x04,                    // type = SETTINGS
      0x01,                    // flags = ACK
      0x00, 0x00, 0x00, 0x00,  // stream_id = 0
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  };
  REQUIRE(client.ReceiveBytes(At(base, 1), bad_settings_ack, sizeof(bad_settings_ack)));

  // Flush all pending client output (GOAWAY(PROTOCOL_ERROR)).
  size_t flushed_bytes = 0;
  for (;;) {
    size_t len = 0;
    const uint8_t* p = client.PendingOutput(&len);
    if (len == 0) break;
    REQUIRE(p != nullptr);
    flushed_bytes += len;
    client.ConsumeOutput(len);
  }
  CHECK(flushed_bytes > 0u);
  CHECK_FALSE(client.WantRead());
  CHECK_FALSE(client.WantWrite());
}
