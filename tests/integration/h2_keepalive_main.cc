// egrpc — M2 acceptance probe (design §8, M2).
//
// Wires EventThread + TlsSocket + Http2Session the way ChannelImpl will:
// the EventThread owns the socket and session, drives the SETTINGS exchange,
// flushes session output off POLLOUT, and runs the keepalive state machine
// from timer-heap timers (design §5.2). Everything below main() runs on the
// event thread.
//
// stdout protocol (line-buffered; consumed by tests/test_keepalive.py):
//   CONNECTED alpn=h2 / REMOTE_SETTINGS / SETTINGS_ACKED / PING_ACK n=<k>
//   GOAWAY code=<c> too_many_pings=<0|1>
//   DONE pings_acked=<k>  → exit 0     (after --run-ms)
//   DEAD                  → exit 3     (keepalive declared the peer dead)
//   FAILED: <detail> (stderr) → exit 1
#include <poll.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <future>
#include <memory>
#include <string>

#include "core/event_thread.h"
#include "transport/http2_session.h"
#include "transport/tls_socket.h"

namespace {

using egrpc::internal::EventThread;
using egrpc::internal::Http2Session;
using egrpc::internal::Http2SessionOptions;
using egrpc::internal::TimerId;
using egrpc::internal::TlsSocket;
using std::chrono::milliseconds;
using std::chrono::steady_clock;

struct ProbeArgs {
  std::string host;
  uint16_t port = 0;
  egrpc::internal::TlsConfig tls;
  int keepalive_time_ms = 1000;
  int keepalive_timeout_ms = 2000;
  int run_ms = 5000;
  int connect_timeout_ms = 10000;
};

// All fields owned by the event thread after the initial Post (design §3);
// main() only waits on `done`.
struct Ctx {
  explicit Ctx(EventThread& l) : loop(l) {}
  EventThread& loop;
  ProbeArgs args;
  TlsSocket sock;
  std::unique_ptr<Http2Session> h2;
  TimerId connect_timer = 0;
  TimerId keepalive_timer = 0;
  TimerId run_timer = 0;
  std::promise<int> done;
  bool finished = false;
};

void FlushOutput(Ctx& c);
void RearmKeepalive(Ctx& c);

void Finish(Ctx& c, int exit_code, const std::string& stderr_detail) {
  if (c.finished) return;
  c.finished = true;
  if (!stderr_detail.empty()) std::fprintf(stderr, "FAILED: %s\n", stderr_detail.c_str());
  // No timer may fire after Finish — cancel all of them structurally rather
  // than relying on Stop() winning the race.
  for (TimerId* t : {&c.connect_timer, &c.keepalive_timer, &c.run_timer}) {
    if (*t != 0) {
      c.loop.CancelTimer(*t);
      *t = 0;
    }
  }
  c.loop.UnwatchFd();
  c.sock.Close();
  c.done.set_value(exit_code);
}

void RearmKeepalive(Ctx& c) {
  if (c.finished || !c.h2) return;
  if (c.keepalive_timer != 0) {
    c.loop.CancelTimer(c.keepalive_timer);
    c.keepalive_timer = 0;
  }
  const auto deadline = c.h2->NextKeepaliveDeadline(steady_clock::now());
  if (!deadline) return;
  c.keepalive_timer = c.loop.AddTimer(*deadline - steady_clock::now(), [&c] {
    if (c.finished || !c.h2) return;
    c.keepalive_timer = 0;
    switch (c.h2->CheckKeepalive(steady_clock::now())) {
      case Http2Session::KeepaliveAction::kDead:
        std::printf("DEAD\n");
        Finish(c, 3, "");
        return;
      case Http2Session::KeepaliveAction::kPingSent:
      case Http2Session::KeepaliveAction::kNone:
        break;
    }
    // A queued PING must go out even if the loop is idle in poll().
    FlushOutput(c);
    RearmKeepalive(c);
  });
}

void FlushOutput(Ctx& c) {
  while (!c.finished) {
    size_t len = 0;
    const uint8_t* data = c.h2->PendingOutput(&len);
    if (data == nullptr || len == 0) {
      if (!c.h2->error().empty()) Finish(c, 1, c.h2->error());
      return;
    }
    size_t written = 0;
    const TlsSocket::IoStatus st = c.sock.Write(data, len, &written);
    if (st == TlsSocket::IoStatus::kOk) {
      c.h2->ConsumeOutput(written);
      continue;
    }
    if (st == TlsSocket::IoStatus::kWantWrite || st == TlsSocket::IoStatus::kWantRead) {
      return;  // poll re-arms via WantWrite() in the events mask
    }
    Finish(c, 1, c.sock.error());
    return;
  }
}

void DrainSocket(Ctx& c) {
  uint8_t buf[16384];
  while (!c.finished) {
    size_t n = 0;
    const TlsSocket::IoStatus st = c.sock.Read(buf, sizeof(buf), &n);
    if (st == TlsSocket::IoStatus::kOk) {
      if (!c.h2->ReceiveBytes(steady_clock::now(), buf, n)) {
        Finish(c, 1, c.h2->error());
        return;
      }
      continue;
    }
    if (st == TlsSocket::IoStatus::kWantRead || st == TlsSocket::IoStatus::kWantWrite) return;
    if (st == TlsSocket::IoStatus::kEof) {
      Finish(c, 1, "connection closed by server");
      return;
    }
    Finish(c, 1, c.sock.error());
    return;
  }
}

void StartSession(Ctx& c) {
  std::printf("CONNECTED alpn=h2\n");

  Http2SessionOptions options;
  options.keepalive_time = milliseconds(c.args.keepalive_time_ms);
  options.keepalive_timeout = milliseconds(c.args.keepalive_timeout_ms);

  Http2Session::Hooks hooks;
  hooks.on_remote_settings = [] { std::printf("REMOTE_SETTINGS\n"); };
  hooks.on_settings_ack = [] { std::printf("SETTINGS_ACKED\n"); };
  hooks.on_ping_ack = [&c] {
    std::printf("PING_ACK n=%llu\n", static_cast<unsigned long long>(c.h2->pings_acked()));
  };
  hooks.on_goaway = [](uint32_t error_code, int32_t, bool too_many_pings) {
    std::printf("GOAWAY code=%u too_many_pings=%d\n", error_code, too_many_pings ? 1 : 0);
  };

  c.h2 = std::make_unique<Http2Session>(options, std::move(hooks), steady_clock::now());
  if (!c.h2->ok()) {
    Finish(c, 1, c.h2->error());
    return;
  }

  // Re-register the watch for session I/O: read-interest always, write-
  // interest only while the session has queued output.
  c.loop.WatchFd(
      c.sock.fd(), [&c] { return static_cast<short>(POLLIN | (c.h2->WantWrite() ? POLLOUT : 0)); },
      [&c](short revents) {
        if ((revents & (POLLIN | POLLERR | POLLHUP)) != 0) DrainSocket(c);
        if (!c.finished) FlushOutput(c);
        RearmKeepalive(c);
      });

  c.run_timer = c.loop.AddTimer(milliseconds(c.args.run_ms), [&c] {
    std::printf("DONE pings_acked=%llu\n", static_cast<unsigned long long>(c.h2->pings_acked()));
    Finish(c, 0, "");
  });

  FlushOutput(c);  // connection preface + SETTINGS
  RearmKeepalive(c);
}

void StartConnect(Ctx& c) {
  c.connect_timer = c.loop.AddTimer(milliseconds(c.args.connect_timeout_ms),
                                    [&c] { Finish(c, 1, "connect timeout"); });

  if (!c.sock.StartConnect(c.args.host, c.args.port, c.args.tls)) {
    c.loop.CancelTimer(c.connect_timer);
    Finish(c, 1, c.sock.error());
    return;
  }

  c.loop.WatchFd(
      c.sock.fd(), [&c] { return c.sock.DesiredPollEvents(); },
      [&c](short) {
        const TlsSocket::IoStatus st = c.sock.ContinueConnect();
        if (st == TlsSocket::IoStatus::kWantRead || st == TlsSocket::IoStatus::kWantWrite) return;
        c.loop.CancelTimer(c.connect_timer);
        c.connect_timer = 0;
        if (st == TlsSocket::IoStatus::kOk) {
          StartSession(c);
        } else {
          Finish(c, 1, c.sock.error());
        }
      });
}

int Usage(const char* argv0) {
  std::fprintf(stderr,
               "usage: %s --host H --port P [--ca FILE] [--insecure] [--keepalive-time-ms N]\n"
               "          [--keepalive-timeout-ms N] [--run-ms N] [--connect-timeout-ms N]\n",
               argv0);
  return 2;
}

}  // namespace

int main(int argc, char** argv) {
  setvbuf(stdout, nullptr, _IOLBF, 0);

  EventThread loop;
  Ctx ctx(loop);
  for (int i = 1; i < argc; ++i) {
    const std::string flag = argv[i];
    auto next = [&]() -> const char* { return (i + 1 < argc) ? argv[++i] : nullptr; };
    const char* v = nullptr;
    if (flag == "--insecure") {
      ctx.args.tls.insecure_skip_verify_for_testing_only = true;
    } else if ((v = next()) == nullptr) {
      return Usage(argv[0]);
    } else if (flag == "--host") {
      ctx.args.host = v;
    } else if (flag == "--port") {
      ctx.args.port = static_cast<uint16_t>(std::atoi(v));
    } else if (flag == "--ca") {
      ctx.args.tls.ca_bundle_path = v;
    } else if (flag == "--keepalive-time-ms") {
      ctx.args.keepalive_time_ms = std::atoi(v);
    } else if (flag == "--keepalive-timeout-ms") {
      ctx.args.keepalive_timeout_ms = std::atoi(v);
    } else if (flag == "--run-ms") {
      ctx.args.run_ms = std::atoi(v);
    } else if (flag == "--connect-timeout-ms") {
      ctx.args.connect_timeout_ms = std::atoi(v);
    } else {
      return Usage(argv[0]);
    }
  }
  if (ctx.args.host.empty() || ctx.args.port == 0) return Usage(argv[0]);

  if (!loop.Start()) {
    std::fprintf(stderr, "FAILED: could not start event thread\n");
    return 1;
  }

  auto result = ctx.done.get_future();
  if (!loop.Post([&ctx] { StartConnect(ctx); })) {
    std::fprintf(stderr, "FAILED: event thread refused op\n");
    return 1;
  }
  const int exit_code = result.get();
  loop.Stop();
  return exit_code;
}
