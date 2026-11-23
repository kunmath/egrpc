// egrpc M7 example: reconcile-on-reconnect subscription (design §4.6).
//
// Subscribes to RouteGuide.ListFeatures through egrpc::resilient_stream using
// only public headers and the UNMODIFIED generated stubs. The pattern:
//
//   1. The factory lambda builds each attempt's request from a cursor (the
//      last sequence number seen), so a re-established stream resumes where
//      the previous one stopped.
//   2. on_reconnected runs on this (the Read() caller) thread once the new
//      stream is established, which makes it the place to reconcile any state
//      that may have changed while disconnected -- here, with a unary call.
//
// Usage:
//   egrpc_resilient_subscribe --host HOST --port PORT [--ca FILE]
//       [--client-cert FILE --client-key FILE] [--cellular]
//       [--initial-backoff-ms N] [--max-backoff-ms N]
//       [--keepalive-time-ms N] [--keepalive-timeout-ms N]
//       [--initial-retry-delay-ms N] [--max-retry-delay-ms N]
//       [--fire-on-first-connect] [--per-attempt-deadline-ms N]
//       [--reconcile-unary] [--target-events N] [--list-mode N]
//       [--delay-ms N] [--fail-count N] [--stop-after-events N]
//       [--stop-on-stdin] [--capture-logs] [--wait-ms N]
//       [--mode subscribe|keepalive_log|unary]
//
// Output (line-buffered stdout): "EVENT ...", "RECONNECTED ...", "LOG ..."
// lines, then KEY=VALUE summary lines ending with DONE=1. Exit 0 when the
// scenario ran to completion, 2 on usage error.

#include <egrpc/channel_config.h>
#include <egrpc/log.h>
#include <egrpc/resilient_stream.h>
#include <grpcpp/grpcpp.h>
#include <poll.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>

#include "route_guide.grpc.pb.h"

namespace {

struct Args {
  std::string host = "localhost";
  int port = 0;
  std::string ca;
  std::string client_cert;
  std::string client_key;
  bool cellular = false;
  long initial_backoff_ms = -1;
  long max_backoff_ms = -1;
  long keepalive_time_ms = -1;
  long keepalive_timeout_ms = -1;
  long initial_retry_delay_ms = -1;
  long max_retry_delay_ms = -1;
  bool fire_on_first_connect = false;
  long per_attempt_deadline_ms = 0;
  bool reconcile_unary = false;
  long target_events = 0;
  long list_mode = 7;
  long delay_ms = 0;
  long fail_count = 0;
  long stop_after_events = 0;
  bool stop_on_stdin = false;
  bool capture_logs = false;
  long wait_ms = 3000;
  std::string mode = "subscribe";
};

bool ParseArgs(int argc, char** argv, Args* a) {
  for (int i = 1; i < argc; ++i) {
    const std::string k = argv[i];
    auto next = [&](std::string* out) {
      if (i + 1 >= argc) return false;
      *out = argv[++i];
      return true;
    };
    auto next_long = [&](long* out) {
      std::string v;
      if (!next(&v)) return false;
      errno = 0;
      char* end = nullptr;
      const long n = std::strtol(v.c_str(), &end, 10);
      if (errno != 0 || end == v.c_str() || *end != '\0' || n < INT32_MIN || n > INT32_MAX) {
        return false;
      }
      *out = n;
      return true;
    };
    long port = 0;
    bool ok = true;
    if (k == "--host") {
      ok = next(&a->host);
    } else if (k == "--port") {
      ok = next_long(&port);
      a->port = static_cast<int>(port);
    } else if (k == "--ca") {
      ok = next(&a->ca);
    } else if (k == "--client-cert") {
      ok = next(&a->client_cert);
    } else if (k == "--client-key") {
      ok = next(&a->client_key);
    } else if (k == "--cellular") {
      a->cellular = true;
    } else if (k == "--initial-backoff-ms") {
      ok = next_long(&a->initial_backoff_ms);
    } else if (k == "--max-backoff-ms") {
      ok = next_long(&a->max_backoff_ms);
    } else if (k == "--keepalive-time-ms") {
      ok = next_long(&a->keepalive_time_ms);
    } else if (k == "--keepalive-timeout-ms") {
      ok = next_long(&a->keepalive_timeout_ms);
    } else if (k == "--initial-retry-delay-ms") {
      ok = next_long(&a->initial_retry_delay_ms);
    } else if (k == "--max-retry-delay-ms") {
      ok = next_long(&a->max_retry_delay_ms);
    } else if (k == "--fire-on-first-connect") {
      a->fire_on_first_connect = true;
    } else if (k == "--per-attempt-deadline-ms") {
      ok = next_long(&a->per_attempt_deadline_ms);
    } else if (k == "--reconcile-unary") {
      a->reconcile_unary = true;
    } else if (k == "--target-events") {
      ok = next_long(&a->target_events);
    } else if (k == "--list-mode") {
      ok = next_long(&a->list_mode);
    } else if (k == "--delay-ms") {
      ok = next_long(&a->delay_ms);
    } else if (k == "--fail-count") {
      ok = next_long(&a->fail_count);
    } else if (k == "--stop-after-events") {
      ok = next_long(&a->stop_after_events);
    } else if (k == "--stop-on-stdin") {
      a->stop_on_stdin = true;
    } else if (k == "--capture-logs") {
      a->capture_logs = true;
    } else if (k == "--wait-ms") {
      ok = next_long(&a->wait_ms);
    } else if (k == "--mode") {
      ok = next(&a->mode);
    } else {
      std::fprintf(stderr, "unknown argument: %s\n", k.c_str());
      return false;
    }
    if (!ok) {
      std::fprintf(stderr, "missing or invalid value for %s\n", k.c_str());
      return false;
    }
  }
  if (a->port <= 0) {
    std::fprintf(stderr, "--port is required\n");
    return false;
  }
  return true;
}

egrpc::ChannelConfig MakeConfig(const Args& a) {
  using std::chrono::milliseconds;
  // Start from the cellular profile (60 s keepalive) or the spec defaults,
  // then apply explicit overrides.
  egrpc::ChannelConfig cfg =
      a.cellular ? egrpc::ChannelConfig::CellularProfile() : egrpc::ChannelConfig();
  cfg.ca_bundle_path = a.ca;
  cfg.client_cert_path = a.client_cert;
  cfg.client_key_path = a.client_key;
  if (a.initial_backoff_ms >= 0) cfg.initial_reconnect_backoff = milliseconds(a.initial_backoff_ms);
  if (a.max_backoff_ms >= 0) cfg.max_reconnect_backoff = milliseconds(a.max_backoff_ms);
  if (a.keepalive_time_ms >= 0) cfg.keepalive_time = milliseconds(a.keepalive_time_ms);
  if (a.keepalive_timeout_ms >= 0) cfg.keepalive_timeout = milliseconds(a.keepalive_timeout_ms);
  return cfg;
}

// Bounded in-memory log capture. The sink may run on a channel's EventThread,
// so it only takes a short lock and never touches stdio; overflow is dropped.
struct CapturedLog {
  egrpc::LogLevel level;
  std::string component;
  std::string message;
};
constexpr size_t kMaxCapturedLogs = 256;
std::mutex g_log_mu;
std::deque<CapturedLog> g_logs;
size_t g_logs_dropped = 0;

std::string Target(const Args& a) { return a.host + ":" + std::to_string(a.port); }

// One GetFeature call for the well-known test point; true on success.
bool UnaryCheck(routeguide::RouteGuide::Stub* stub, bool wait_for_ready, long deadline_ms,
                grpc::Status* status_out = nullptr) {
  grpc::ClientContext ctx;
  ctx.set_wait_for_ready(wait_for_ready);
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_ms));
  routeguide::Point point;
  point.set_latitude(1000);
  point.set_longitude(2000);
  routeguide::Feature feature;
  const grpc::Status s = stub->GetFeature(&ctx, point, &feature);
  if (status_out != nullptr) *status_out = s;
  return s.ok();
}

int RunSubscribe(const Args& a) {
  auto channel = egrpc::CreateChannel(Target(a), MakeConfig(a));
  std::unique_ptr<routeguide::RouteGuide::Stub> stub = routeguide::RouteGuide::NewStub(channel);

  // Caller-thread state: the factory and on_reconnected both run inside
  // Read() on this thread, so no locking is needed.
  long long last_seen_seq = 0;
  long long missed = 0;
  long long duplicates = 0;
  long long received = 0;
  int attempts = 0;
  int reconnects = 0;
  int reconcile_unary_ok = 0;
  std::set<long long> generations;

  egrpc::ResilientStreamOptions opts;
  opts.fire_on_first_connect = a.fire_on_first_connect;
  if (a.initial_retry_delay_ms >= 0) {
    opts.initial_retry_delay = std::chrono::milliseconds(a.initial_retry_delay_ms);
  }
  if (a.max_retry_delay_ms >= 0) {
    opts.max_retry_delay = std::chrono::milliseconds(a.max_retry_delay_ms);
  }
  opts.on_reconnected = [&] {
    ++reconnects;
    std::printf("RECONNECTED n=%d last_seq=%lld\n", reconnects, last_seen_seq);
    // Reconcile: anything that changed while we were disconnected can be
    // fetched here with ordinary blocking calls on the same channel.
    if (a.reconcile_unary && UnaryCheck(stub.get(), /*wait_for_ready=*/true, 5000)) {
      ++reconcile_unary_ok;
    }
  };

  egrpc::resilient_stream<routeguide::Feature> cmds(opts, [&](grpc::ClientContext& ctx) {
    ++attempts;
    if (a.per_attempt_deadline_ms > 0) {
      ctx.set_deadline(std::chrono::system_clock::now() +
                       std::chrono::milliseconds(a.per_attempt_deadline_ms));
    }
    // The test server reads its parameters from the Rectangle; in mode 7
    // lo.longitude is the resume cursor.
    routeguide::Rectangle req;
    req.mutable_lo()->set_latitude(static_cast<int32_t>(a.list_mode));
    req.mutable_lo()->set_longitude(
        static_cast<int32_t>(a.list_mode == 7 ? last_seen_seq : a.fail_count));
    req.mutable_hi()->set_latitude(static_cast<int32_t>(a.target_events));
    req.mutable_hi()->set_longitude(static_cast<int32_t>(a.delay_ms));
    return stub->ListFeatures(&ctx, req);
  });

  // Optional stdin stopper: a line ("stop") or EOF stops the stream.
  std::atomic<bool> done{false};
  std::thread stdin_thread;
  if (a.stop_on_stdin) {
    stdin_thread = std::thread([&] {
      while (!done.load()) {
        pollfd pfd{STDIN_FILENO, POLLIN, 0};
        if (::poll(&pfd, 1, 100) <= 0) continue;
        char buf[256];
        const ssize_t n = ::read(STDIN_FILENO, buf, sizeof(buf));
        if (n <= 0 || std::memchr(buf, '\n', static_cast<size_t>(n)) != nullptr) {
          cmds.stop();
          return;
        }
      }
    });
  }

  std::thread stopper;
  routeguide::Feature f;
  while (cmds.Read(&f)) {
    ++received;
    const long long seq = f.location().latitude();
    const long long gen = f.location().longitude();
    generations.insert(gen);
    if (seq <= last_seen_seq) {
      ++duplicates;
    } else {
      missed += seq - last_seen_seq - 1;
      last_seen_seq = seq;
    }
    std::printf("EVENT seq=%lld gen=%lld name=%s\n", seq, gen, f.name().c_str());
    if (a.stop_after_events > 0 && received == a.stop_after_events && !stopper.joinable()) {
      // Stop from another thread while this one keeps reading.
      stopper = std::thread([&] { cmds.stop(); });
    }
  }
  done.store(true);
  if (stopper.joinable()) stopper.join();
  if (stdin_thread.joinable()) stdin_thread.join();

  const grpc::Status s = cmds.status();
  std::printf("ATTEMPTS=%d\n", attempts);
  std::printf("RECONNECTS=%d\n", reconnects);
  std::printf("RECONCILE_UNARY_OK=%d\n", reconcile_unary_ok);
  std::printf("RECEIVED=%lld\n", received);
  std::printf("LAST_SEQ=%lld\n", last_seen_seq);
  std::printf("MISSED=%lld\n", missed);
  std::printf("DUPLICATES=%lld\n", duplicates);
  std::printf("GENERATIONS=%zu\n", generations.size());
  std::printf("FINAL_STATUS=%d\n", static_cast<int>(s.error_code()));
  std::printf("FINAL_MESSAGE=%s\n", s.error_message().c_str());
  return 0;
}

// Two unary calls separated by --wait-ms, so keepalive PINGs (and any
// GOAWAY(ENHANCE_YOUR_CALM) they provoke) happen on an otherwise idle channel.
int RunKeepaliveLog(const Args& a) {
  auto channel = egrpc::CreateChannel(Target(a), MakeConfig(a));
  auto stub = routeguide::RouteGuide::NewStub(channel);
  grpc::Status s1;
  grpc::Status s2;
  UnaryCheck(stub.get(), /*wait_for_ready=*/false, 5000, &s1);
  std::printf("CALL1_STATUS=%d\n", static_cast<int>(s1.error_code()));
  std::this_thread::sleep_for(std::chrono::milliseconds(a.wait_ms));
  UnaryCheck(stub.get(), /*wait_for_ready=*/true, 5000, &s2);
  std::printf("CALL2_STATUS=%d\n", static_cast<int>(s2.error_code()));
  return 0;
}

// A single fail-fast unary call (e.g. to check TLS/mTLS acceptance).
int RunUnary(const Args& a) {
  auto channel = egrpc::CreateChannel(Target(a), MakeConfig(a));
  auto stub = routeguide::RouteGuide::NewStub(channel);
  grpc::Status s;
  UnaryCheck(stub.get(), /*wait_for_ready=*/false, 5000, &s);
  std::printf("UNARY_STATUS=%d\n", static_cast<int>(s.error_code()));
  return 0;
}

}  // namespace

int main(int argc, char** argv) {
  std::setvbuf(stdout, nullptr, _IOLBF, 0);
  Args a;
  if (!ParseArgs(argc, argv, &a)) return 2;

  if (a.capture_logs) {
    egrpc::SetLogSink([](egrpc::LogLevel level, const char* comp, const std::string& msg) {
      std::lock_guard<std::mutex> lock(g_log_mu);
      if (g_logs.size() >= kMaxCapturedLogs) {
        ++g_logs_dropped;
        return;
      }
      g_logs.push_back(CapturedLog{level, comp, msg});
    });
  }

  int rc;
  if (a.mode == "subscribe") {
    rc = RunSubscribe(a);
  } else if (a.mode == "keepalive_log") {
    rc = RunKeepaliveLog(a);
  } else if (a.mode == "unary") {
    rc = RunUnary(a);
  } else {
    std::fprintf(stderr, "unknown mode: %s\n", a.mode.c_str());
    return 2;
  }
  if (a.capture_logs) {
    // Every channel is destroyed by now (its EventThread joined), so no sink
    // call can race with this drain.
    egrpc::SetLogSink(nullptr);
    std::lock_guard<std::mutex> lock(g_log_mu);
    for (const CapturedLog& l : g_logs) {
      std::printf("LOG level=%s comp=%s msg=%s\n", egrpc::LogLevelName(l.level),
                  l.component.c_str(), l.message.c_str());
    }
    std::printf("LOGS_DROPPED=%zu\n", g_logs_dropped);
  }
  if (rc == 0) std::printf("DONE=1\n");
  return rc;
}
