// egrpc — M2 unit tests for Http2Session (design §4.2, §5.2).
//
// Http2Session is a pure byte/state machine owned by the EventThread. These
// tests exercise it against an in-process nghttp2 *server* session, pumping
// bytes both ways by hand. No sockets, no threads, no sleeping: time is
// synthetic (base = steady_clock::now(); everything is base + N ms), so the
// keepalive state machine is fully deterministic.
#include <nghttp2/nghttp2.h>

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

  Http2Session client({}, std::move(hooks), base);
  REQUIRE(client.ok());

  ServerSession server;
  Handshake(client, server.s, base);  // keeps last_read_ == base

  // Defaults: keepalive_time 60s, timeout 20s. Deadline = last_read + 60s.
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
  Http2Session client({}, {}, base);
  REQUIRE(client.ok());

  // Send a keepalive PING at base + 60s; do NOT pump the ack.
  CHECK(client.CheckKeepalive(AtSec(base, 60)) == KeepaliveAction::kPingSent);
  CHECK(client.ping_outstanding());

  // 1 ms before the timeout: still alive.
  CHECK(client.CheckKeepalive(At(base, 60000 + 19999)) == KeepaliveAction::kNone);
  // At the timeout: dead.
  CHECK(client.CheckKeepalive(At(base, 60000 + 20000)) == KeepaliveAction::kDead);
}

TEST_CASE("Http2Session: any read counts as liveness while PING outstanding") {
  const auto base = Base();
  Http2Session client({}, {}, base);
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
