// egrpc M6 integration probe (design §4.1, §4.2, §4.4, §5.2, §5.3, §5.7,
// §8/M6): reconnect + backoff, wait-for-ready admission, keepalive
// enforcement + ENHANCE_YOUR_CALM doubling, GOAWAY drain & replace, bounded
// clean shutdown, and call failure origin. Drives ChannelImpl directly (so
// tests can configure small backoff / keepalive / drain / shutdown budgets and
// inject a resolver) and the public grpcpp shim via the unmodified generated
// route_guide stubs. Driven by tests/test_reconnect.py, tests/test_shutdown.py
// and tests/test_keepalive.py against tests/server/h2_test_server.py.
//
// Usage:
//   egrpc_resilience_probe --host HOST --port PORT --ca CERT --mode MODE
//       [--initial-backoff-ms N] [--max-backoff-ms N] [--multiplier X]
//       [--jitter X] [--connect-timeout-ms N] [--ready-stability-ms N]
//       [--keepalive-time-ms N] [--keepalive-timeout-ms N]
//       [--goaway-drain-ms N] [--shutdown-timeout-ms N] [--deadline-ms N]
//       [--wait-ms N] [--cancel-after-ms N] [--addr-ports P1,P2,...]
//       [--payload-bytes N]
//
// MODE: reconnect_after_drop | reconnect_after_restart |
//       wait_for_ready_matrix | wait_for_ready_late_server |
//       shim_wait_for_ready_reconnect | backoff_observe |
//       goaway_split_drain | goaway_drain_timeout |
//       keepalive_idle_reconnect | keepalive_too_many_pings |
//       cancel_origin_check | precancel_local_origin |
//       tls_addr_fallback | shutdown_active_and_outliving_reader |
//       shutdown_blackhole | shutdown_large_outbound | shutdown_hung_dns |
//       shutdown_graceful_goaway
//
// Output: line-buffered KEY=VALUE lines on stdout (status codes are numeric
// gRPC codes, origins are "local" / "server"), ending with DONE=1. Exit 0 when
// the scenario ran to completion (pytest asserts the values), 2 on usage error.

#include <arpa/inet.h>
#include <grpcpp/grpcpp.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "core/call_state.h"
#include "core/channel_impl.h"
#include "route_guide.grpc.pb.h"
#include "route_guide.pb.h"
#include "transport/dns_resolver.h"
#include "transport/tls_socket.h"

namespace {

using egrpc::internal::CallCompletion;
using egrpc::internal::CallMode;
using egrpc::internal::ChannelImpl;
using egrpc::internal::ChannelOptions;
using egrpc::internal::StatusCode;
using egrpc::internal::StatusOrigin;
using Clock = std::chrono::steady_clock;
using std::chrono::milliseconds;

constexpr char kGetFeature[] = "/routeguide.RouteGuide/GetFeature";
constexpr char kListFeatures[] = "/routeguide.RouteGuide/ListFeatures";

struct Args {
  std::string host = "localhost";
  uint16_t port = 0;
  std::string ca;
  std::string mode;
  int initial_backoff_ms = 150;
  int max_backoff_ms = 1000;
  double multiplier = 1.6;
  double jitter = 0.0;
  int connect_timeout_ms = 5000;
  int ready_stability_ms = 60000;
  int keepalive_time_ms = 300000;
  int keepalive_timeout_ms = 20000;
  int goaway_drain_ms = 400;
  int shutdown_timeout_ms = 1000;
  int deadline_ms = 5000;
  int wait_ms = 0;
  int cancel_after_ms = 150;
  std::vector<uint16_t> addr_ports;        // tls_addr_fallback: 127.0.0.1 ports, in order
  size_t payload_bytes = 2 * 1024 * 1024;  // shutdown_large_outbound request size
};

int64_t MsBetween(Clock::time_point from, Clock::time_point to) {
  return std::chrono::duration_cast<milliseconds>(to - from).count();
}

int64_t MsSince(Clock::time_point from) { return MsBetween(from, Clock::now()); }

std::optional<Clock::time_point> DeadlineIn(int ms) { return Clock::now() + milliseconds(ms); }

int Code(const CallCompletion::Result& r) { return static_cast<int>(r.code); }

// Deterministic readiness wait: polls `pred` every 5 ms, bounded by a 5 s
// safety cap (never reached on success). Returns the final predicate value.
bool PollUntil(const std::function<bool()>& pred, milliseconds cap = milliseconds(5000)) {
  const auto deadline = Clock::now() + cap;
  while (!pred()) {
    if (Clock::now() >= deadline) return pred();
    std::this_thread::sleep_for(milliseconds(5));
  }
  return true;
}

const char* OriginName(StatusOrigin o) { return o == StatusOrigin::kLocal ? "local" : "server"; }

const char* StateName(ChannelImpl::ConnectivityState s) {
  switch (s) {
    case ChannelImpl::ConnectivityState::kIdle:
      return "IDLE";
    case ChannelImpl::ConnectivityState::kConnecting:
      return "CONNECTING";
    case ChannelImpl::ConnectivityState::kReady:
      return "READY";
    case ChannelImpl::ConnectivityState::kTransientFailure:
      return "TRANSIENT_FAILURE";
    case ChannelImpl::ConnectivityState::kShutdown:
      return "SHUTDOWN";
  }
  return "UNKNOWN";
}

std::string PointBytes() {
  routeguide::Point pt;
  pt.set_latitude(409146138);
  pt.set_longitude(-746188906);
  std::string out;
  if (!pt.SerializeToString(&out)) out.clear();
  return out;
}

routeguide::Rectangle Rect() {
  routeguide::Rectangle rect;
  rect.mutable_lo()->set_latitude(1);
  rect.mutable_hi()->set_latitude(2);
  return rect;
}

std::string RectBytes() {
  std::string out;
  if (!Rect().SerializeToString(&out)) out.clear();
  return out;
}

// Feature name of a unary response, or "-" if absent/unparseable.
std::string FeatureName(const CallCompletion::Result& r) {
  if (r.code != StatusCode::kOk) return "-";
  routeguide::Feature f;
  if (!f.ParseFromString(r.response) || f.name().empty()) return "-";
  return f.name();
}

void PrintResult(const char* prefix, const CallCompletion::Result& r) {
  std::printf("%s_STATUS=%d\n", prefix, Code(r));
  std::printf("%s_ORIGIN=%s\n", prefix, OriginName(r.origin));
  std::printf("%s_NAME=%s\n", prefix, FeatureName(r).c_str());
}

ChannelOptions MakeOptions(const Args& a) {
  ChannelOptions o;
  o.initial_reconnect_backoff = milliseconds(a.initial_backoff_ms);
  o.max_reconnect_backoff = milliseconds(a.max_backoff_ms);
  o.reconnect_backoff_multiplier = a.multiplier;
  o.reconnect_backoff_jitter = a.jitter;
  o.connect_timeout = milliseconds(a.connect_timeout_ms);
  o.ready_stability_threshold = milliseconds(a.ready_stability_ms);
  o.http2.keepalive_time = milliseconds(a.keepalive_time_ms);
  o.http2.keepalive_timeout = milliseconds(a.keepalive_timeout_ms);
  o.goaway_drain_timeout = milliseconds(a.goaway_drain_ms);
  o.shutdown_timeout = milliseconds(a.shutdown_timeout_ms);
  return o;
}

std::unique_ptr<ChannelImpl> MakeChannel(const Args& a, ChannelOptions opts) {
  egrpc::internal::TlsConfig tls;
  tls.ca_bundle_path = a.ca;
  return std::make_unique<ChannelImpl>(a.host, a.port, std::move(tls), std::move(opts));
}

CallCompletion::Result Unary(ChannelImpl& ch, int deadline_ms, bool wait_for_ready,
                             const std::string& path = kGetFeature) {
  return ch.UnaryCall(path, PointBytes(), {}, DeadlineIn(deadline_ms), wait_for_ready);
}

std::string ReadFile(const std::string& path) {
  std::ifstream in(path, std::ios::binary);
  std::stringstream ss;
  ss << in.rdbuf();
  return ss.str();
}

std::shared_ptr<grpc::Channel> MakeShimChannel(const Args& a) {
  grpc::SslCredentialsOptions ssl;
  ssl.pem_root_certs = ReadFile(a.ca);
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, a.initial_backoff_ms);
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, a.max_backoff_ms);
  args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, a.connect_timeout_ms);
  return grpc::CreateCustomChannel(a.host + ":" + std::to_string(a.port), grpc::SslCredentials(ssl),
                                   args);
}

routeguide::Point ShimPoint() {
  routeguide::Point pt;
  pt.set_latitude(409146138);
  pt.set_longitude(-746188906);
  return pt;
}

// Waits on a CallCompletion from a helper thread and records when it closed.
class Waiter {
 public:
  explicit Waiter(std::shared_ptr<CallCompletion> c)
      : completion_(std::move(c)), thread_([this] {
          result_ = completion_->Wait();
          end_ = Clock::now();
        }) {}
  ~Waiter() { Join(); }
  Waiter(const Waiter&) = delete;
  Waiter& operator=(const Waiter&) = delete;

  void Join() {
    if (thread_.joinable()) thread_.join();
  }
  // Valid after Join().
  const CallCompletion::Result& result() const { return result_; }
  Clock::time_point end() const { return end_; }

 private:
  std::shared_ptr<CallCompletion> completion_;
  CallCompletion::Result result_;
  Clock::time_point end_;
  std::thread thread_;  // last: started after the other members exist
};

// --- Reconnect / wait-for-ready ---------------------------------------------

int RunReconnectAfterDrop(const Args& a) {
  auto ch = MakeChannel(a, MakeOptions(a));
  PrintResult("CALL1", Unary(*ch, a.deadline_ms, /*wait_for_ready=*/false));
  PrintResult("CALL2", Unary(*ch, a.deadline_ms, /*wait_for_ready=*/true));
  std::printf("CONNECT_ATTEMPTS=%llu\n", static_cast<unsigned long long>(ch->connect_attempts()));
  ch->Shutdown();
  return 0;
}

// Call 1 on the first server; then blocks until pytest writes a line on stdin
// (after restarting the server on the same port); then a wait-for-ready call 2.
int RunReconnectAfterRestart(const Args& a) {
  auto ch = MakeChannel(a, MakeOptions(a));
  PrintResult("CALL1", Unary(*ch, a.deadline_ms, /*wait_for_ready=*/false));
  std::printf("READY_FOR_RESTART=1\n");
  std::fflush(stdout);
  char line[64];
  if (std::fgets(line, sizeof(line), stdin) == nullptr) {
    std::fprintf(stderr, "stdin closed before restart signal\n");
    ch->Shutdown();
    return 1;
  }
  std::printf("CONNECT_ATTEMPTS_BEFORE_CALL2=%llu\n",
              static_cast<unsigned long long>(ch->connect_attempts()));
  PrintResult("CALL2", Unary(*ch, a.deadline_ms, /*wait_for_ready=*/true));
  std::printf("CONNECT_ATTEMPTS=%llu\n", static_cast<unsigned long long>(ch->connect_attempts()));
  ch->Shutdown();
  return 0;
}

// Against a port nothing listens on, with a long backoff so the channel stays
// in TRANSIENT_FAILURE for the whole scenario.
int RunWaitForReadyMatrix(const Args& a) {
  auto ch = MakeChannel(a, MakeOptions(a));

  const auto r1 = Unary(*ch, 5000, /*wait_for_ready=*/false);
  std::printf("CALL1_STATUS=%d\n", Code(r1));
  std::printf("CALL1_ORIGIN=%s\n", OriginName(r1.origin));
  std::printf("STATE_AFTER_CALL1=%s\n", StateName(ch->state()));

  auto t = Clock::now();
  const auto ff = Unary(*ch, 5000, /*wait_for_ready=*/false);
  std::printf("FF_STATUS=%d\n", Code(ff));
  std::printf("FF_ELAPSED_MS=%lld\n", static_cast<long long>(MsSince(t)));
  std::printf("FF_ORIGIN=%s\n", OriginName(ff.origin));

  t = Clock::now();
  const auto wd = Unary(*ch, a.deadline_ms, /*wait_for_ready=*/true);
  std::printf("WFR_DEADLINE_STATUS=%d\n", Code(wd));
  std::printf("WFR_DEADLINE_ELAPSED_MS=%lld\n", static_cast<long long>(MsSince(t)));
  std::printf("WFR_DEADLINE_ORIGIN=%s\n", OriginName(wd.origin));

  t = Clock::now();
  auto h = ch->StartCall(CallMode::kUnary, kGetFeature, PointBytes(), {}, DeadlineIn(10000),
                         /*preallocated_id=*/0, /*prebound_completion=*/nullptr,
                         /*wait_for_ready=*/true);
  std::thread canceller([&] {
    std::this_thread::sleep_for(milliseconds(a.cancel_after_ms));
    ch->CancelCall(h.completion);
  });
  const auto& wc = h.completion->Wait();
  const int64_t cancel_elapsed = MsSince(t);
  canceller.join();
  std::printf("WFR_CANCEL_STATUS=%d\n", Code(wc));
  std::printf("WFR_CANCEL_ELAPSED_MS=%lld\n", static_cast<long long>(cancel_elapsed));
  std::printf("WFR_CANCEL_ORIGIN=%s\n", OriginName(wc.origin));
  std::printf("STATE_AT_END=%s\n", StateName(ch->state()));
  std::printf("CONNECT_ATTEMPTS=%llu\n", static_cast<unsigned long long>(ch->connect_attempts()));
  ch->Shutdown();
  return 0;
}

int RunWaitForReadyLateServer(const Args& a) {
  auto ch = MakeChannel(a, MakeOptions(a));
  const auto t = Clock::now();
  auto h = ch->StartCall(CallMode::kUnary, kGetFeature, PointBytes(), {}, DeadlineIn(a.deadline_ms),
                         /*preallocated_id=*/0, /*prebound_completion=*/nullptr,
                         /*wait_for_ready=*/true);
  // Tell pytest (deterministically) that at least one connect has already
  // been attempted and failed, i.e. the channel has been in
  // TRANSIENT_FAILURE, before it starts the server.
  const bool attempted = PollUntil([&] {
    return ch->state() == ChannelImpl::ConnectivityState::kTransientFailure ||
           ch->connect_attempts() >= 2;
  });
  std::printf("CONNECT_ATTEMPTED=%d\n", attempted ? 1 : 0);
  std::fflush(stdout);
  const CallCompletion::Result r = h.completion->Wait();
  std::printf("ELAPSED_MS=%lld\n", static_cast<long long>(MsSince(t)));
  std::printf("STATUS=%d\n", Code(r));
  std::printf("ORIGIN=%s\n", OriginName(r.origin));
  std::printf("NAME=%s\n", FeatureName(r).c_str());
  std::printf("CONNECT_ATTEMPTS=%llu\n", static_cast<unsigned long long>(ch->connect_attempts()));
  ch->Shutdown();
  return 0;
}

// Public generated stub + GRPC_ARG_*_RECONNECT_BACKOFF_MS + set_wait_for_ready.
int RunShimWaitForReadyReconnect(const Args& a) {
  auto channel = MakeShimChannel(a);
  auto stub = routeguide::RouteGuide::NewStub(channel);
  const routeguide::Point pt = ShimPoint();

  {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + milliseconds(a.deadline_ms));
    routeguide::Feature f;
    const grpc::Status s = stub->GetFeature(&ctx, pt, &f);
    std::printf("CALL1_STATUS=%d\n", static_cast<int>(s.error_code()));
    std::printf("CALL1_LOCAL=%d\n", ctx.egrpc_status_is_local() ? 1 : 0);
  }
  {
    // Fail-fast control while the channel sits in reconnect backoff.
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + milliseconds(a.deadline_ms));
    routeguide::Feature f;
    const auto t = Clock::now();
    const grpc::Status s = stub->GetFeature(&ctx, pt, &f);
    std::printf("CALLFF_STATUS=%d\n", static_cast<int>(s.error_code()));
    std::printf("CALLFF_LOCAL=%d\n", ctx.egrpc_status_is_local() ? 1 : 0);
    std::printf("CALLFF_ELAPSED_MS=%lld\n", static_cast<long long>(MsSince(t)));
  }
  {
    grpc::ClientContext ctx;
    ctx.set_wait_for_ready(true);
    ctx.set_deadline(std::chrono::system_clock::now() + milliseconds(a.deadline_ms));
    routeguide::Feature f;
    const grpc::Status s = stub->GetFeature(&ctx, pt, &f);
    std::printf("CALL2_STATUS=%d\n", static_cast<int>(s.error_code()));
    std::printf("CALL2_LOCAL=%d\n", ctx.egrpc_status_is_local() ? 1 : 0);
    std::printf("CALL2_NAME=%s\n", f.name().empty() ? "-" : f.name().c_str());
  }
  stub.reset();
  channel.reset();
  return 0;
}

// Triggers the first connect, then lets the background reconnect loop run
// for --wait-ms; pytest inspects the server's per-connection timestamps.
int RunBackoffObserve(const Args& a) {
  auto ch = MakeChannel(a, MakeOptions(a));
  const auto r = Unary(*ch, a.deadline_ms, /*wait_for_ready=*/false);
  std::printf("CALL1_STATUS=%d\n", Code(r));
  std::this_thread::sleep_for(milliseconds(a.wait_ms));
  std::printf("CONNECT_ATTEMPTS=%llu\n", static_cast<unsigned long long>(ch->connect_attempts()));
  const auto t = Clock::now();
  ch->Shutdown();
  std::printf("SHUTDOWN_MS=%lld\n", static_cast<long long>(MsSince(t)));
  return 0;
}

// --- GOAWAY drain & replace (§4.2) ------------------------------------------

int RunGoawaySplitDrain(const Args& a) {
  auto ch = MakeChannel(a, MakeOptions(a));
  const auto t0 = Clock::now();
  // Posted in order from one thread: FIFO admission puts call 1 on stream 1
  // and call 2 on stream 3 of the first connection.
  auto h1 = ch->StartCall(CallMode::kUnary, kGetFeature, PointBytes(), {}, DeadlineIn(10000));
  auto h2 = ch->StartCall(CallMode::kUnary, kGetFeature, PointBytes(), {}, DeadlineIn(10000));
  Waiter w1(h1.completion);
  Waiter w2(h2.completion);
  w1.Join();
  w2.Join();
  PrintResult("CALL1", w1.result());
  std::printf("CALL1_ELAPSED_MS=%lld\n", static_cast<long long>(MsBetween(t0, w1.end())));
  PrintResult("CALL2", w2.result());
  std::printf("CALL2_ELAPSED_MS=%lld\n", static_cast<long long>(MsBetween(t0, w2.end())));
  PrintResult("CALL3", Unary(*ch, a.deadline_ms, /*wait_for_ready=*/true));
  std::printf("CONNECT_ATTEMPTS=%llu\n", static_cast<unsigned long long>(ch->connect_attempts()));
  ch->Shutdown();
  return 0;
}

int RunGoawayDrainTimeout(const Args& a) {
  auto ch = MakeChannel(a, MakeOptions(a));
  const auto t0 = Clock::now();
  const auto r1 = ch->UnaryCall(kGetFeature, PointBytes(), {}, DeadlineIn(10000));
  std::printf("CALL1_ELAPSED_MS=%lld\n", static_cast<long long>(MsSince(t0)));
  PrintResult("CALL1", r1);
  PrintResult("CALL2", Unary(*ch, a.deadline_ms, /*wait_for_ready=*/true));
  std::printf("CONNECT_ATTEMPTS=%llu\n", static_cast<unsigned long long>(ch->connect_attempts()));
  ch->Shutdown();
  return 0;
}

// --- Keepalive (§5.2) -------------------------------------------------------

int RunKeepaliveIdleReconnect(const Args& a) {
  auto ch = MakeChannel(a, MakeOptions(a));
  PrintResult("CALL1", Unary(*ch, a.deadline_ms, /*wait_for_ready=*/false));
  // Zero calls in flight from here on: only keepalive can notice the
  // blackholed connection and drive the reconnect.
  std::this_thread::sleep_for(milliseconds(a.wait_ms));
  std::printf("STATE_BEFORE_CALL2=%s\n", StateName(ch->state()));
  std::printf("CONNECT_ATTEMPTS_BEFORE_CALL2=%llu\n",
              static_cast<unsigned long long>(ch->connect_attempts()));
  PrintResult("CALL2", Unary(*ch, a.deadline_ms, /*wait_for_ready=*/true));
  std::printf("CONNECT_ATTEMPTS=%llu\n", static_cast<unsigned long long>(ch->connect_attempts()));
  ch->Shutdown();
  return 0;
}

int RunKeepaliveTooManyPings(const Args& a) {
  auto ch = MakeChannel(a, MakeOptions(a));
  std::printf("INITIAL_KEEPALIVE_MS=%lld\n",
              static_cast<long long>(ch->effective_keepalive_time().count()));
  PrintResult("CALL1", Unary(*ch, a.deadline_ms, /*wait_for_ready=*/false));
  std::this_thread::sleep_for(milliseconds(a.wait_ms));
  std::printf("EFFECTIVE_KEEPALIVE_MS=%lld\n",
              static_cast<long long>(ch->effective_keepalive_time().count()));
  std::printf("CONNECT_ATTEMPTS_BEFORE_CALL2=%llu\n",
              static_cast<unsigned long long>(ch->connect_attempts()));
  PrintResult("CALL2", Unary(*ch, a.deadline_ms, /*wait_for_ready=*/true));
  std::printf("CONNECT_ATTEMPTS=%llu\n", static_cast<unsigned long long>(ch->connect_attempts()));
  ch->Shutdown();
  return 0;
}

// --- Failure origin (§4.6 prerequisite) ---------------------------------------

int RunCancelOriginCheck(const Args& a) {
  {
    auto ch = MakeChannel(a, MakeOptions(a));

    // Local TryCancel of a stream the server holds open.
    auto h = ch->StartCall(CallMode::kServerStreaming, "/test.Stream/Stall", RectBytes(), {},
                           DeadlineIn(10000));
    h.completion->WaitForInitialMetadata();
    ch->CancelCall(h.completion);
    const auto& rl = h.completion->Wait();
    std::printf("LOCAL_STATUS=%d\n", Code(rl));
    std::printf("LOCAL_ORIGIN=%s\n", OriginName(rl.origin));

    // Server-originated CANCELLED via grpc-status trailers.
    const auto rs = Unary(*ch, a.deadline_ms, /*wait_for_ready=*/false);
    std::printf("SERVER_STATUS=%d\n", Code(rs));
    std::printf("SERVER_ORIGIN=%s\n", OriginName(rs.origin));
    std::printf("SERVER_MESSAGE=%s\n", rs.message.c_str());

    // Server-originated CANCELLED via RST_STREAM(CANCEL).
    const auto rr = Unary(*ch, a.deadline_ms, /*wait_for_ready=*/false, "/test.Stream/RstCancel");
    std::printf("SERVER_RST_STATUS=%d\n", Code(rr));
    std::printf("SERVER_RST_ORIGIN=%s\n", OriginName(rr.origin));
    ch->Shutdown();
  }

  // Same distinction through the public shim (ClientContext::egrpc_status_is_local).
  auto channel = MakeShimChannel(a);
  auto stub = routeguide::RouteGuide::NewStub(channel);
  {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + milliseconds(a.deadline_ms));
    routeguide::Feature f;
    const grpc::Status s = stub->GetFeature(&ctx, ShimPoint(), &f);
    std::printf("SHIM_SERVER_STATUS=%d\n", static_cast<int>(s.error_code()));
    std::printf("SHIM_SERVER_LOCAL=%d\n", ctx.egrpc_status_is_local() ? 1 : 0);
  }
  {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + milliseconds(10000));
    auto reader = stub->ListFeatures(&ctx, Rect());
    std::thread canceller([&] {
      std::this_thread::sleep_for(milliseconds(a.cancel_after_ms));
      ctx.TryCancel();
    });
    routeguide::Feature f;
    const bool read_ok = reader->Read(&f);
    canceller.join();
    const grpc::Status s = reader->Finish();
    std::printf("SHIM_LOCAL_READ_OK=%d\n", read_ok ? 1 : 0);
    std::printf("SHIM_LOCAL_STATUS=%d\n", static_cast<int>(s.error_code()));
    std::printf("SHIM_LOCAL_LOCAL=%d\n", ctx.egrpc_status_is_local() ? 1 : 0);
  }
  stub.reset();
  channel.reset();
  return 0;
}

// --- Shutdown (§5.7) ----------------------------------------------------------

int RunShutdownActiveAndOutlivingReader(const Args& a) {
  // Both parts run against the stall_after_headers server: it sends initial
  // response HEADERS only once a request stream has fully arrived (and was
  // recorded), then holds the stream open forever. Observing initial
  // metadata on the client therefore proves, without sleeps or polling of
  // client-internal counters, that the server has the request.
  //
  // Part A — public API: a ClientReader outlives its Channel while another
  // thread is blocked in Read().
  {
    std::shared_ptr<grpc::Channel> channel = MakeShimChannel(a);
    auto stub = routeguide::RouteGuide::NewStub(channel);
    grpc::ClientContext rctx;
    // Safety bound only (never reached on success): a broken server turns
    // into a DEADLINE_EXCEEDED assertion failure instead of a hung probe.
    rctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
    auto reader = stub->ListFeatures(&rctx, Rect());
    reader->WaitForInitialMetadata();  // server received the request
    bool read_ok = true;
    Clock::time_point read_end;
    std::promise<void> reader_started;
    std::future<void> reader_started_f = reader_started.get_future();
    std::thread reader_thread([&] {
      routeguide::Feature f;
      reader_started.set_value();
      read_ok = reader->Read(&f);
      read_end = Clock::now();
    });
    const bool entered =
        reader_started_f.wait_for(std::chrono::seconds(5)) == std::future_status::ready;
    std::printf("PART_A_SYNC=%d\n", entered ? 1 : 0);
    const auto drop_start = Clock::now();
    stub.reset();
    channel.reset();  // last reference: ~Channel shuts the channel down
    const int64_t drop_ms = MsSince(drop_start);
    reader_thread.join();
    std::printf("CHANNEL_DROP_MS=%lld\n", static_cast<long long>(drop_ms));
    std::printf("READER_READ_OK=%d\n", read_ok ? 1 : 0);
    // The server never sends DATA: Read() can only return once the channel
    // is dropped.
    std::printf("READER_ENDED_AFTER_DROP=%d\n", read_end >= drop_start ? 1 : 0);
    std::printf("READER_UNBLOCK_MS=%lld\n",
                static_cast<long long>(MsBetween(drop_start, read_end)));
    routeguide::Feature f2;
    std::printf("READER_READ_AGAIN_OK=%d\n", reader->Read(&f2) ? 1 : 0);
    const grpc::Status s = reader->Finish();
    std::printf("READER_FINISH_STATUS=%d\n", static_cast<int>(s.error_code()));
    std::printf("READER_FINISH_LOCAL=%d\n", rctx.egrpc_status_is_local() ? 1 : 0);
    reader.reset();  // destroyed after the channel: must be safe
  }

  // Part B — Shutdown() with an active stream (blocked reader), an active
  // unary call, and a wait-for-ready call queued behind a 2-stream cap.
  ChannelOptions opts = MakeOptions(a);
  opts.http2.max_concurrent_streams = 2;
  auto ch = MakeChannel(a, std::move(opts));
  // 10 s deadlines are a safety bound only (never reached on success).
  auto hs =
      ch->StartCall(CallMode::kServerStreaming, kListFeatures, RectBytes(), {}, DeadlineIn(10000));
  auto hu = ch->StartCall(CallMode::kUnary, kGetFeature, PointBytes(), {}, DeadlineIn(10000));
  // Initial metadata on both calls => the server received both requests and
  // both streams are open (stall_after_headers never closes them).
  hs.completion->WaitForInitialMetadata();
  hu.completion->WaitForInitialMetadata();
  const bool both_open = !hs.completion->is_done() && !hu.completion->is_done();
  const bool ready = ch->state() == ChannelImpl::ConnectivityState::kReady;
  // Both stream slots are taken: the wait-for-ready call is admitted to the
  // pending queue but cannot be submitted. Its HandleNewCall op is posted
  // here, synchronously, before Shutdown() appends DoShutdown(), so FIFO op
  // order has it queued when shutdown runs.
  auto hw = ch->StartCall(CallMode::kUnary, kGetFeature, PointBytes(), {}, DeadlineIn(10000),
                          /*preallocated_id=*/0, /*prebound_completion=*/nullptr,
                          /*wait_for_ready=*/true);
  std::printf("PART_B_SYNC=%d\n", both_open && ready ? 1 : 0);
  std::printf("STATE_BEFORE_SHUTDOWN=%s\n", StateName(ch->state()));
  std::printf("WFR_DONE_BEFORE_SHUTDOWN=%d\n", hw.completion->is_done() ? 1 : 0);
  bool pop_ok = true;
  Clock::time_point pop_end;
  std::thread stream_reader([&] {
    std::string msg;
    size_t wire = 0;
    pop_ok = hs.completion->PopMessage(&msg, &wire);
    pop_end = Clock::now();
  });
  Waiter wu(hu.completion);
  Waiter ww(hw.completion);

  const auto t = Clock::now();
  ch->Shutdown();
  const int64_t shutdown_ms = MsSince(t);
  stream_reader.join();
  wu.Join();
  ww.Join();
  const auto& rs = hs.completion->Wait();
  std::printf("SHUTDOWN_MS=%lld\n", static_cast<long long>(shutdown_ms));
  // Read after Shutdown() joined the EventThread: the final high-water mark,
  // so a wait-for-ready call that slipped past the cap would show as 3.
  std::printf("MAX_ACTIVE_CALLS=%zu\n", ch->max_active_calls());
  std::printf("STREAM_POP_OK=%d\n", pop_ok ? 1 : 0);
  std::printf("STREAM_STATUS=%d\n", Code(rs));
  std::printf("STREAM_ORIGIN=%s\n", OriginName(rs.origin));
  std::printf("UNARY_STATUS=%d\n", Code(wu.result()));
  std::printf("UNARY_ORIGIN=%s\n", OriginName(wu.result().origin));
  std::printf("WFR_STATUS=%d\n", Code(ww.result()));
  std::printf("WFR_ORIGIN=%s\n", OriginName(ww.result().origin));
  const int64_t unblock_max =
      std::max({MsBetween(t, pop_end), MsBetween(t, wu.end()), MsBetween(t, ww.end())});
  std::printf("UNBLOCK_MAX_MS=%lld\n", static_cast<long long>(unblock_max));
  const auto post = ch->UnaryCall(kGetFeature, PointBytes());
  std::printf("POST_SHUTDOWN_STATUS=%d\n", Code(post));
  return 0;
}

int RunShutdownBlackhole(const Args& a) {
  auto ch = MakeChannel(a, MakeOptions(a));
  auto h = ch->StartCall(CallMode::kServerStreaming, kListFeatures, RectBytes());
  h.completion->WaitForInitialMetadata();
  std::printf("INITIAL_METADATA=1\n");
  Waiter w(h.completion);
  const bool in_flight = PollUntil([&] {
    return ch->state() == ChannelImpl::ConnectivityState::kReady && ch->max_active_calls() >= 1;
  });
  std::printf("SYNC=%d\n", in_flight ? 1 : 0);
  const auto t = Clock::now();
  ch->Shutdown();
  const int64_t shutdown_ms = MsSince(t);
  w.Join();
  std::printf("SHUTDOWN_MS=%lld\n", static_cast<long long>(shutdown_ms));
  std::printf("CALL_STATUS=%d\n", Code(w.result()));
  std::printf("CALL_ORIGIN=%s\n", OriginName(w.result().origin));
  std::printf("CALL_UNBLOCK_MS=%lld\n", static_cast<long long>(MsBetween(t, w.end())));
  return 0;
}

// Fix #2 regression: a large unary request is queued for output on a
// connection whose peer has stopped reading (shutdown_blackhole server: it
// answers the first stream's headers, then goes dead), and Shutdown() is
// called right after submission. Shutdown must stay bounded by
// shutdown_timeout no matter how much outbound data is pending.
int RunShutdownLargeOutbound(const Args& a) {
  auto ch = MakeChannel(a, MakeOptions(a));
  auto hs = ch->StartCall(CallMode::kServerStreaming, kListFeatures, RectBytes());
  hs.completion->WaitForInitialMetadata();
  std::printf("INITIAL_METADATA=1\n");

  // Field 1 (latitude, varint) followed by a huge unknown length-delimited
  // field: still a parseable Point, but `payload_bytes` on the wire.
  std::string big = PointBytes();
  big.push_back(static_cast<char>((15 << 3) | 2));  // field 15, wire type 2
  size_t n = a.payload_bytes;
  while (n >= 0x80) {
    big.push_back(static_cast<char>((n & 0x7f) | 0x80));
    n >>= 7;
  }
  big.push_back(static_cast<char>(n));
  big.append(a.payload_bytes, 'x');
  std::printf("REQUEST_BYTES=%zu\n", big.size());

  auto hu = ch->StartCall(CallMode::kUnary, kGetFeature, std::move(big));
  Waiter ws(hs.completion);
  Waiter wu(hu.completion);
  const bool submitted = PollUntil([&] { return ch->max_active_calls() >= 2; });
  std::printf("SYNC=%d\n", submitted ? 1 : 0);

  const auto t = Clock::now();
  ch->Shutdown();
  const int64_t shutdown_ms = MsSince(t);
  ws.Join();
  wu.Join();
  std::printf("SHUTDOWN_MS=%lld\n", static_cast<long long>(shutdown_ms));
  std::printf("STREAM_STATUS=%d\n", Code(ws.result()));
  std::printf("STREAM_ORIGIN=%s\n", OriginName(ws.result().origin));
  std::printf("UNARY_STATUS=%d\n", Code(wu.result()));
  std::printf("UNARY_ORIGIN=%s\n", OriginName(wu.result().origin));
  const int64_t unblock_max = std::max(MsBetween(t, ws.end()), MsBetween(t, wu.end()));
  std::printf("UNBLOCK_MAX_MS=%lld\n", static_cast<long long>(unblock_max));
  return 0;
}

// Fix #3: a ClientContext TryCancel()ed before the call is issued fails with
// CANCELLED and egrpc_status_is_local() == true through the unmodified
// generated stubs — unary (GetFeature) and server-streaming (ListFeatures) —
// and the call never reaches the transport (no connect attempt at all).
int RunPrecancelLocalOrigin(const Args& a) {
  auto owned = MakeChannel(a, MakeOptions(a));
  ChannelImpl* impl = owned.get();  // valid while `channel` lives
  std::shared_ptr<grpc::Channel> channel = grpc::Channel::FromImpl(std::move(owned));
  auto stub = routeguide::RouteGuide::NewStub(channel);
  {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + milliseconds(a.deadline_ms));
    ctx.TryCancel();
    routeguide::Feature f;
    f.set_name("untouched");
    const grpc::Status s = stub->GetFeature(&ctx, ShimPoint(), &f);
    std::printf("UNARY_STATUS=%d\n", static_cast<int>(s.error_code()));
    std::printf("UNARY_LOCAL=%d\n", ctx.egrpc_status_is_local() ? 1 : 0);
    std::printf("UNARY_RESPONSE=%s\n", f.name().c_str());
  }
  {
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + milliseconds(a.deadline_ms));
    ctx.TryCancel();
    auto reader = stub->ListFeatures(&ctx, Rect());
    routeguide::Feature f;
    const bool read_ok = reader->Read(&f);
    const grpc::Status s = reader->Finish();
    std::printf("STREAM_READ_OK=%d\n", read_ok ? 1 : 0);
    std::printf("STREAM_STATUS=%d\n", static_cast<int>(s.error_code()));
    std::printf("STREAM_LOCAL=%d\n", ctx.egrpc_status_is_local() ? 1 : 0);
  }
  std::printf("CONNECT_ATTEMPTS=%llu\n", static_cast<unsigned long long>(impl->connect_attempts()));
  std::printf("STATE=%s\n", StateName(impl->state()));
  impl = nullptr;
  stub.reset();
  channel.reset();
  return 0;
}

// Fix #5: TlsSocket::StartConnect(host, {addr1, addr2, ...}) where earlier
// addresses accept TCP but fail the TLS handshake or ALPN must fall back to
// the next address. Drives the socket directly on this thread (its sole
// owner) with a poll() loop that re-reads fd() every turn, since a fallback
// replaces the fd.
const char* TlsStateName(egrpc::internal::TlsSocket::State s) {
  using S = egrpc::internal::TlsSocket::State;
  switch (s) {
    case S::kIdle:
      return "IDLE";
    case S::kTcpConnecting:
      return "TCP_CONNECTING";
    case S::kTlsHandshake:
      return "TLS_HANDSHAKE";
    case S::kConnected:
      return "CONNECTED";
    case S::kFailed:
      return "FAILED";
    case S::kClosed:
      return "CLOSED";
  }
  return "UNKNOWN";
}

int RunTlsAddrFallback(const Args& a) {
  using egrpc::internal::ResolvedAddress;
  using egrpc::internal::TlsSocket;
  std::vector<ResolvedAddress> addrs;
  for (uint16_t port : a.addr_ports) {
    ResolvedAddress ra;
    sockaddr_in sin{};
    sin.sin_family = AF_INET;
    sin.sin_port = htons(port);
    sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    std::memcpy(&ra.addr, &sin, sizeof(sin));
    ra.len = sizeof(sin);
    addrs.push_back(ra);
  }
  egrpc::internal::TlsConfig tls;
  tls.ca_bundle_path = a.ca;

  TlsSocket sock;
  const bool started = sock.StartConnect(a.host, addrs, tls);
  const auto deadline = Clock::now() + milliseconds(a.deadline_ms);
  while (started && (sock.state() == TlsSocket::State::kTcpConnecting ||
                     sock.state() == TlsSocket::State::kTlsHandshake)) {
    const int64_t remaining = MsBetween(Clock::now(), deadline);
    if (remaining <= 0) break;
    pollfd p{};
    p.fd = sock.fd();  // may change after a per-address fallback
    p.events = sock.DesiredPollEvents();
    const int rc = poll(&p, 1, static_cast<int>(std::min<int64_t>(remaining, 100)));
    if (rc < 0) break;
    if (rc == 0) continue;
    const TlsSocket::IoStatus st = sock.ContinueConnect();
    if (st == TlsSocket::IoStatus::kWantRead || st == TlsSocket::IoStatus::kWantWrite) continue;
    break;
  }

  const bool connected = sock.state() == TlsSocket::State::kConnected;
  int peer_port = 0;
  if (connected) {
    sockaddr_storage peer{};
    socklen_t len = sizeof(peer);
    if (getpeername(sock.fd(), reinterpret_cast<sockaddr*>(&peer), &len) == 0 &&
        peer.ss_family == AF_INET) {
      sockaddr_in sin{};
      std::memcpy(&sin, &peer, sizeof(sin));
      peer_port = ntohs(sin.sin_port);
    }
  }
  std::string error = sock.error();
  std::replace(error.begin(), error.end(), '\n', ' ');
  std::printf("CONNECTED=%d\n", connected ? 1 : 0);
  std::printf("TLS_STATE=%s\n", TlsStateName(sock.state()));
  std::printf("ALPN=%s\n", connected ? sock.alpn().c_str() : "-");
  std::printf("PEER_PORT=%d\n", peer_port);
  std::printf("ERROR=%s\n", error.empty() ? "-" : error.c_str());
  sock.Close();
  return 0;
}

// Latch shared between main and the injected resolver. `worker_gone` flips
// when the last copy of the resolver callable (held by the detached DNS
// worker thread) is destroyed.
struct HungDnsLatch {
  std::mutex mu;
  std::condition_variable cv;
  bool entered = false;
  bool released = false;
  bool worker_gone = false;
};

class ResolverSentinel {
 public:
  explicit ResolverSentinel(std::shared_ptr<HungDnsLatch> latch) : latch_(std::move(latch)) {}
  ~ResolverSentinel() {
    std::lock_guard<std::mutex> lock(latch_->mu);
    latch_->worker_gone = true;
    latch_->cv.notify_all();
  }
  ResolverSentinel(const ResolverSentinel&) = delete;
  ResolverSentinel& operator=(const ResolverSentinel&) = delete;

 private:
  std::shared_ptr<HungDnsLatch> latch_;
};

int RunShutdownHungDns(const Args& a) {
  auto latch = std::make_shared<HungDnsLatch>();
  bool entered = false;
  bool worker_exited = false;
  {
    ChannelOptions opts = MakeOptions(a);
    {
      auto sentinel = std::make_shared<ResolverSentinel>(latch);
      opts.resolve_fn = [latch, sentinel](const std::string&, uint16_t) {
        std::unique_lock<std::mutex> lock(latch->mu);
        latch->entered = true;
        latch->cv.notify_all();
        latch->cv.wait(lock, [&] { return latch->released; });
        egrpc::internal::ResolveResult r;
        r.error = "mock resolver released";
        return r;
      };
    }
    auto ch = MakeChannel(a, std::move(opts));
    opts.resolve_fn = nullptr;  // drop any copy left in the moved-from options

    auto h = ch->StartCall(CallMode::kUnary, kGetFeature, PointBytes());
    {
      std::unique_lock<std::mutex> lock(latch->mu);
      entered = latch->cv.wait_for(lock, std::chrono::seconds(5), [&] { return latch->entered; });
    }
    std::printf("RESOLVER_ENTERED=%d\n", entered ? 1 : 0);
    const auto t = Clock::now();
    ch->Shutdown();
    const int64_t shutdown_ms = MsSince(t);
    const auto& r = h.completion->Wait();
    std::printf("SHUTDOWN_MS=%lld\n", static_cast<long long>(shutdown_ms));
    std::printf("CALL_STATUS=%d\n", Code(r));
    std::printf("CALL_ORIGIN=%s\n", OriginName(r.origin));
    std::printf("CALL_UNBLOCK_MS=%lld\n", static_cast<long long>(MsSince(t)));
    ch.reset();
  }
  // Release the hung lookup and wait for the detached worker to drop its
  // last copy of the resolver callable, so nothing outlives main().
  {
    std::unique_lock<std::mutex> lock(latch->mu);
    latch->released = true;
    latch->cv.notify_all();
    worker_exited =
        latch->cv.wait_for(lock, std::chrono::seconds(5), [&] { return latch->worker_gone; });
  }
  // Let the detached thread finish unwinding past its last destructor.
  std::this_thread::sleep_for(milliseconds(100));
  std::printf("WORKER_EXITED=%d\n", worker_exited ? 1 : 0);
  return 0;
}

int RunShutdownGracefulGoaway(const Args& a) {
  auto ch = MakeChannel(a, MakeOptions(a));
  PrintResult("CALL1", Unary(*ch, a.deadline_ms, /*wait_for_ready=*/false));
  const auto t = Clock::now();
  ch->Shutdown();
  std::printf("SHUTDOWN_MS=%lld\n", static_cast<long long>(MsSince(t)));
  return 0;
}

bool ParseArgs(int argc, char** argv, Args* a) {
  for (int i = 1; i < argc; ++i) {
    const std::string flag = argv[i];
    if (i + 1 >= argc) {
      std::fprintf(stderr, "missing value for %s\n", flag.c_str());
      return false;
    }
    const char* v = argv[++i];
    if (flag == "--host") {
      a->host = v;
    } else if (flag == "--port") {
      a->port = static_cast<uint16_t>(std::atoi(v));
    } else if (flag == "--ca" || flag == "--ca-file") {
      a->ca = v;
    } else if (flag == "--mode") {
      a->mode = v;
    } else if (flag == "--initial-backoff-ms") {
      a->initial_backoff_ms = std::atoi(v);
    } else if (flag == "--max-backoff-ms") {
      a->max_backoff_ms = std::atoi(v);
    } else if (flag == "--multiplier") {
      a->multiplier = std::atof(v);
    } else if (flag == "--jitter") {
      a->jitter = std::atof(v);
    } else if (flag == "--connect-timeout-ms") {
      a->connect_timeout_ms = std::atoi(v);
    } else if (flag == "--ready-stability-ms") {
      a->ready_stability_ms = std::atoi(v);
    } else if (flag == "--keepalive-time-ms") {
      a->keepalive_time_ms = std::atoi(v);
    } else if (flag == "--keepalive-timeout-ms") {
      a->keepalive_timeout_ms = std::atoi(v);
    } else if (flag == "--goaway-drain-ms") {
      a->goaway_drain_ms = std::atoi(v);
    } else if (flag == "--shutdown-timeout-ms") {
      a->shutdown_timeout_ms = std::atoi(v);
    } else if (flag == "--deadline-ms") {
      a->deadline_ms = std::atoi(v);
    } else if (flag == "--wait-ms") {
      a->wait_ms = std::atoi(v);
    } else if (flag == "--cancel-after-ms") {
      a->cancel_after_ms = std::atoi(v);
    } else if (flag == "--addr-ports") {
      std::stringstream ss(v);
      std::string item;
      while (std::getline(ss, item, ',')) {
        if (!item.empty()) a->addr_ports.push_back(static_cast<uint16_t>(std::atoi(item.c_str())));
      }
    } else if (flag == "--payload-bytes") {
      a->payload_bytes = static_cast<size_t>(std::strtoull(v, nullptr, 10));
    } else {
      std::fprintf(stderr, "unknown flag: %s\n", flag.c_str());
      return false;
    }
  }
  return a->port != 0 && !a->mode.empty();
}

}  // namespace

int main(int argc, char** argv) {
  std::setvbuf(stdout, nullptr, _IOLBF, 0);
  Args a;
  if (!ParseArgs(argc, argv, &a)) {
    std::fprintf(stderr, "usage: %s --host H --port P --ca CERT --mode MODE [...]\n", argv[0]);
    return 2;
  }

  using Runner = int (*)(const Args&);
  const std::pair<const char*, Runner> kModes[] = {
      {"reconnect_after_drop", RunReconnectAfterDrop},
      {"reconnect_after_restart", RunReconnectAfterRestart},
      {"wait_for_ready_matrix", RunWaitForReadyMatrix},
      {"wait_for_ready_late_server", RunWaitForReadyLateServer},
      {"shim_wait_for_ready_reconnect", RunShimWaitForReadyReconnect},
      {"backoff_observe", RunBackoffObserve},
      {"goaway_split_drain", RunGoawaySplitDrain},
      {"goaway_drain_timeout", RunGoawayDrainTimeout},
      {"keepalive_idle_reconnect", RunKeepaliveIdleReconnect},
      {"keepalive_too_many_pings", RunKeepaliveTooManyPings},
      {"cancel_origin_check", RunCancelOriginCheck},
      {"precancel_local_origin", RunPrecancelLocalOrigin},
      {"tls_addr_fallback", RunTlsAddrFallback},
      {"shutdown_active_and_outliving_reader", RunShutdownActiveAndOutlivingReader},
      {"shutdown_blackhole", RunShutdownBlackhole},
      {"shutdown_large_outbound", RunShutdownLargeOutbound},
      {"shutdown_hung_dns", RunShutdownHungDns},
      {"shutdown_graceful_goaway", RunShutdownGracefulGoaway},
  };
  for (const auto& m : kModes) {
    if (a.mode == m.first) {
      const int rc = m.second(a);
      if (rc == 0) std::printf("DONE=1\n");
      return rc;
    }
  }
  std::fprintf(stderr, "unknown mode: %s\n", a.mode.c_str());
  return 2;
}
