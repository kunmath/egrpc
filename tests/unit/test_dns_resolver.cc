// egrpc — M6 unit tests for DnsResolver / ResolveBlocking and the
// pre-resolved TlsSocket::StartConnect overload (design §4.1, §5.7).
//
// Every test injects a mock ResolveFn (except the loopback cases, which use
// the real getaddrinfo on a numeric literal and never touch the network).
// Hung lookups are simulated with a Gate; every worker thread the mock ever
// runs on registers a thread_local exit notifier, and each test waits for all
// of them before returning, so no detached helper thread outlives its test
// (keeps TSan/ASan/LSan clean).
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstring>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "doctest/doctest.h"
#include "transport/dns_resolver.h"
#include "transport/tls_socket.h"

namespace {

using egrpc::internal::DnsResolver;
using egrpc::internal::ResolveBlocking;
using egrpc::internal::ResolvedAddress;
using egrpc::internal::ResolveResult;
using egrpc::internal::TlsConfig;
using egrpc::internal::TlsSocket;
using std::chrono::milliseconds;

constexpr milliseconds kWaitLimit{5000};  // generous upper bound, never hit on success

// One-shot latch: Set() once, many waiters. Also used as the "gate" that a
// mock lookup blocks on.
struct Flag {
  std::mutex mu;
  std::condition_variable cv;
  bool set = false;

  void Set() {
    std::lock_guard<std::mutex> lk(mu);
    set = true;
    cv.notify_all();
  }
  bool WaitFor(milliseconds d) {
    std::unique_lock<std::mutex> lk(mu);
    return cv.wait_for(lk, d, [&] { return set; });
  }
  void Wait() {
    std::unique_lock<std::mutex> lk(mu);
    cv.wait(lk, [&] { return set; });
  }
};

// Signals a Flag from a thread_local destructor, i.e. when the owning thread
// is actually exiting (after its thread function and arguments are gone).
struct ThreadExitNotifier {
  std::shared_ptr<Flag> flag;
  ~ThreadExitNotifier() {
    if (flag) flag->Set();
  }
};
thread_local ThreadExitNotifier tls_exit_notifier;

struct MockCall {
  std::string host;
  uint16_t port = 0;
  std::thread::id tid;
};

// Shared between the test body and every copy of the mock ResolveFn.
struct Backend {
  std::mutex mu;
  std::vector<MockCall> calls;
  std::vector<std::shared_ptr<Flag>> thread_exits;  // one per worker thread used

  std::string block_host;  // lookups of this host block on `gate`
  Flag gate;
  Flag entered;  // set when a blocking lookup has started
  std::condition_variable entered_cv;
  size_t entered_count = 0;  // blocking lookups started so far (guarded by mu)

  std::string error_host;  // lookups of this host fail
  std::string error_message = "mock: NXDOMAIN";

  std::vector<MockCall> Calls() {
    std::lock_guard<std::mutex> lk(mu);
    return calls;
  }
  bool WaitEntered(size_t n, milliseconds d) {
    std::unique_lock<std::mutex> lk(mu);
    return entered_cv.wait_for(lk, d, [&] { return entered_count >= n; });
  }
};

ResolvedAddress Loopback4(uint16_t port) {
  ResolvedAddress ra;
  sockaddr_in sin{};
  sin.sin_family = AF_INET;
  sin.sin_port = htons(port);
  sin.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
  std::memcpy(&ra.addr, &sin, sizeof(sin));
  ra.len = sizeof(sin);
  return ra;
}

uint16_t PortOf(const ResolvedAddress& ra) {
  sockaddr_in sin{};
  std::memcpy(&sin, &ra.addr, sizeof(sin));
  return ntohs(sin.sin_port);
}

DnsResolver::ResolveFn MakeMock(std::shared_ptr<Backend> be) {
  return [be](const std::string& host, uint16_t port) -> ResolveResult {
    if (!tls_exit_notifier.flag) {
      auto f = std::make_shared<Flag>();
      tls_exit_notifier.flag = f;
      std::lock_guard<std::mutex> lk(be->mu);
      be->thread_exits.push_back(std::move(f));
    }
    {
      std::lock_guard<std::mutex> lk(be->mu);
      be->calls.push_back(MockCall{host, port, std::this_thread::get_id()});
    }
    if (!be->block_host.empty() && host == be->block_host) {
      {
        std::lock_guard<std::mutex> lk(be->mu);
        ++be->entered_count;
        be->entered_cv.notify_all();
      }
      be->entered.Set();
      be->gate.Wait();
    }
    ResolveResult r;
    r.generation = 0xdeadbeef;  // the resolver must overwrite with the request's tag
    if (!be->error_host.empty() && host == be->error_host) {
      r.error = be->error_message;
      return r;
    }
    r.addresses.push_back(Loopback4(port));
    return r;
  };
}

// Waits until every worker thread the mock ran on has fully exited. Call only
// after the resolver has been shut down / destroyed and all gates released.
void RequireAllWorkersExited(Backend& be) {
  std::vector<std::shared_ptr<Flag>> exits;
  {
    std::lock_guard<std::mutex> lk(be.mu);
    exits = be.thread_exits;
  }
  for (auto& f : exits) REQUIRE(f->WaitFor(kWaitLimit));
}

// Records on_ready invocations (runs on the helper thread under the resolver's
// slot mutex; only touches its own leaf mutex).
struct ReadyLog {
  std::mutex mu;
  std::condition_variable cv;
  std::vector<uint64_t> gens;

  void Add(uint64_t g) {
    std::lock_guard<std::mutex> lk(mu);
    gens.push_back(g);
    cv.notify_all();
  }
  bool WaitForCount(size_t n, milliseconds d) {
    std::unique_lock<std::mutex> lk(mu);
    return cv.wait_for(lk, d, [&] { return gens.size() >= n; });
  }
  std::vector<uint64_t> Gens() {
    std::lock_guard<std::mutex> lk(mu);
    return gens;
  }
};

std::function<void(uint64_t)> Recorder(const std::shared_ptr<ReadyLog>& log) {
  return [log](uint64_t g) { log->Add(g); };
}

}  // namespace

TEST_SUITE("dns_resolver") {
  TEST_CASE("ResolveResult::ok requires addresses and no error") {
    ResolveResult r;
    CHECK_FALSE(r.ok());  // no addresses
    r.addresses.push_back(Loopback4(1));
    CHECK(r.ok());
    r.error = "boom";
    CHECK_FALSE(r.ok());
  }

  TEST_CASE("ResolveBlocking: 127.0.0.1 yields an AF_INET address with the port in NBO") {
    const ResolveResult r = ResolveBlocking("127.0.0.1", 443);
    CHECK(r.error.empty());
    REQUIRE(r.ok());
    REQUIRE_FALSE(r.addresses.empty());
    bool saw_inet = false;
    for (const ResolvedAddress& ra : r.addresses) {
      REQUIRE(ra.len > 0);
      if (ra.addr.ss_family != AF_INET) continue;
      saw_inet = true;
      CHECK(ra.len == sizeof(sockaddr_in));
      sockaddr_in sin{};
      std::memcpy(&sin, &ra.addr, sizeof(sin));
      CHECK(sin.sin_port == htons(443));
      CHECK(sin.sin_addr.s_addr == htonl(INADDR_LOOPBACK));
    }
    CHECK(saw_inet);
  }

  TEST_CASE("ResolveBlocking: invalid host fails cleanly") {
    // Contains characters no resolver accepts; fails locally without network.
    const ResolveResult r = ResolveBlocking("bad host name!", 443);
    CHECK_FALSE(r.ok());
    CHECK_FALSE(r.error.empty());
    CHECK(r.addresses.empty());
  }

  TEST_CASE("DnsResolver: TakeResult before any Resolve is nullopt") {
    auto be = std::make_shared<Backend>();
    DnsResolver resolver(MakeMock(be));
    CHECK_FALSE(resolver.TakeResult().has_value());
    resolver.Invalidate();  // harmless with no worker
    CHECK_FALSE(resolver.TakeResult().has_value());
    CHECK(be->Calls().empty());
  }

  TEST_CASE("DnsResolver: normal resolve, generation tagging, worker reuse") {
    auto be = std::make_shared<Backend>();
    auto ready = std::make_shared<ReadyLog>();
    {
      DnsResolver resolver(MakeMock(be));

      resolver.Resolve("example.test", 443, /*gen=*/1, Recorder(ready));
      REQUIRE(ready->WaitForCount(1, kWaitLimit));
      CHECK(ready->Gens()[0] == 1);

      std::optional<ResolveResult> r = resolver.TakeResult();
      REQUIRE(r.has_value());
      CHECK(r->generation == 1);
      CHECK(r->ok());
      CHECK(r->error.empty());
      REQUIRE(r->addresses.size() == 1);
      CHECK(r->addresses[0].addr.ss_family == AF_INET);
      CHECK(PortOf(r->addresses[0]) == 443);
      // Consumed: a second take yields nothing.
      CHECK_FALSE(resolver.TakeResult().has_value());

      resolver.Resolve("example.test", 8443, /*gen=*/2, Recorder(ready));
      REQUIRE(ready->WaitForCount(2, kWaitLimit));
      CHECK(ready->Gens()[1] == 2);

      r = resolver.TakeResult();
      REQUIRE(r.has_value());
      CHECK(r->generation == 2);
      CHECK(r->ok());
      REQUIRE(r->addresses.size() == 1);
      CHECK(PortOf(r->addresses[0]) == 8443);
      CHECK_FALSE(resolver.TakeResult().has_value());

      const std::vector<MockCall> calls = be->Calls();
      REQUIRE(calls.size() == 2);
      CHECK(calls[0].host == "example.test");
      CHECK(calls[0].port == 443);
      CHECK(calls[1].host == "example.test");
      CHECK(calls[1].port == 8443);
      // Idle worker was reused, and it is not the calling thread.
      const bool reused_worker = calls[0].tid == calls[1].tid;
      const bool off_caller_thread = calls[0].tid != std::this_thread::get_id();
      CHECK(reused_worker);
      CHECK(off_caller_thread);
    }
    RequireAllWorkersExited(*be);
    {
      std::lock_guard<std::mutex> lk(be->mu);
      CHECK(be->thread_exits.size() == 1);
    }
    CHECK(ready->Gens().size() == 2);
  }

  TEST_CASE("DnsResolver: default backend resolves a numeric literal") {
    auto ready = std::make_shared<ReadyLog>();
    {
      DnsResolver resolver;  // ResolveBlocking backend
      resolver.Resolve("127.0.0.1", 443, /*gen=*/5, Recorder(ready));
      REQUIRE(ready->WaitForCount(1, kWaitLimit));
      std::optional<ResolveResult> r = resolver.TakeResult();
      REQUIRE(r.has_value());
      CHECK(r->generation == 5);
      REQUIRE(r->ok());
      bool saw_inet = false;
      for (const ResolvedAddress& ra : r->addresses) {
        if (ra.addr.ss_family != AF_INET) continue;
        saw_inet = true;
        CHECK(PortOf(ra) == 443);
      }
      CHECK(saw_inet);
      // The idle worker is joined by Shutdown (destructor) well within grace.
    }
    CHECK(ready->Gens().size() == 1);
  }

  TEST_CASE("DnsResolver: resolution error propagates with the matching generation") {
    auto be = std::make_shared<Backend>();
    be->error_host = "broken.test";
    be->error_message = "mock: NXDOMAIN broken.test";
    auto ready = std::make_shared<ReadyLog>();
    {
      DnsResolver resolver(MakeMock(be));
      resolver.Resolve("broken.test", 443, /*gen=*/3, Recorder(ready));
      REQUIRE(ready->WaitForCount(1, kWaitLimit));
      CHECK(ready->Gens()[0] == 3);

      std::optional<ResolveResult> r = resolver.TakeResult();
      REQUIRE(r.has_value());
      CHECK_FALSE(r->ok());
      CHECK(r->generation == 3);
      CHECK(r->error == "mock: NXDOMAIN broken.test");
      CHECK(r->addresses.empty());
      CHECK_FALSE(resolver.TakeResult().has_value());
    }
    RequireAllWorkersExited(*be);
  }

  TEST_CASE("DnsResolver: Invalidate while in flight discards the late result") {
    auto be = std::make_shared<Backend>();
    be->block_host = "slow.test";
    auto ready = std::make_shared<ReadyLog>();
    {
      DnsResolver resolver(MakeMock(be));
      resolver.Resolve("slow.test", 443, /*gen=*/1, Recorder(ready));
      REQUIRE(be->entered.WaitFor(kWaitLimit));

      resolver.Invalidate();
      be->gate.Set();
      // Give the worker a chance to publish its (now stale) result. Whether
      // on_ready fires here is not under test, so this wait is not asserted.
      (void)ready->WaitForCount(1, milliseconds(1000));

      CHECK_FALSE(resolver.TakeResult().has_value());
      CHECK_FALSE(resolver.TakeResult().has_value());

      // The resolver remains usable for a new attempt afterwards.
      const size_t before = ready->Gens().size();
      resolver.Resolve("example.test", 443, /*gen=*/2, Recorder(ready));
      REQUIRE(ready->WaitForCount(before + 1, kWaitLimit));
      std::optional<ResolveResult> r = resolver.TakeResult();
      REQUIRE(r.has_value());
      CHECK(r->generation == 2);
      CHECK(r->ok());
    }
    RequireAllWorkersExited(*be);
  }

  TEST_CASE("DnsResolver: Invalidate after completion drops the unread result") {
    auto be = std::make_shared<Backend>();
    auto ready = std::make_shared<ReadyLog>();
    {
      DnsResolver resolver(MakeMock(be));
      resolver.Resolve("example.test", 443, /*gen=*/7, Recorder(ready));
      REQUIRE(ready->WaitForCount(1, kWaitLimit));
      resolver.Invalidate();
      CHECK_FALSE(resolver.TakeResult().has_value());
    }
    RequireAllWorkersExited(*be);
  }

  TEST_CASE(
      "DnsResolver: superseding a hung resolve retires its worker; stale result never lands") {
    auto be = std::make_shared<Backend>();
    be->block_host = "slow.test";
    auto ready = std::make_shared<ReadyLog>();
    {
      DnsResolver resolver(MakeMock(be));
      resolver.Resolve("slow.test", 443, /*gen=*/1, Recorder(ready));
      REQUIRE(be->entered.WaitFor(kWaitLimit));

      // gen=2 while gen=1 is stuck: must not queue behind the hung lookup.
      const auto t_start = std::chrono::steady_clock::now();
      resolver.Resolve("example.test", 8443, /*gen=*/2, Recorder(ready));
      REQUIRE(ready->WaitForCount(1, kWaitLimit));
      CHECK(std::chrono::steady_clock::now() - t_start < milliseconds(2000));

      std::optional<ResolveResult> r = resolver.TakeResult();
      REQUIRE(r.has_value());
      CHECK(r->generation == 2);
      CHECK(r->ok());
      REQUIRE(r->addresses.size() == 1);
      CHECK(PortOf(r->addresses[0]) == 8443);

      std::vector<MockCall> calls = be->Calls();
      REQUIRE(calls.size() == 2);
      CHECK(calls[0].host == "slow.test");
      CHECK(calls[1].host == "example.test");
      const bool fresh_worker = calls[0].tid != calls[1].tid;
      CHECK(fresh_worker);  // gen=2 ran on a new worker, not behind the hung one

      // Unblock the retired worker and wait for that thread to fully exit.
      std::shared_ptr<Flag> retired_exit;
      {
        std::lock_guard<std::mutex> lk(be->mu);
        REQUIRE(be->thread_exits.size() == 2);
        retired_exit = be->thread_exits[0];
      }
      be->gate.Set();
      REQUIRE(retired_exit->WaitFor(kWaitLimit));

      // gen=1's late on_ready never fired, and its result never overwrote
      // anything visible to the owner.
      const std::vector<uint64_t> gens = ready->Gens();
      REQUIRE(gens.size() == 1);
      CHECK(gens[0] == 2);
      CHECK_FALSE(resolver.TakeResult().has_value());

      // The live worker is still serviceable.
      resolver.Resolve("example.test", 443, /*gen=*/3, Recorder(ready));
      REQUIRE(ready->WaitForCount(2, kWaitLimit));
      r = resolver.TakeResult();
      REQUIRE(r.has_value());
      CHECK(r->generation == 3);
      calls = be->Calls();
      REQUIRE(calls.size() == 3);
      const bool live_worker_reused = calls[2].tid == calls[1].tid;
      CHECK(live_worker_reused);
    }
    RequireAllWorkersExited(*be);
    for (uint64_t g : ready->Gens()) CHECK(g != 1);
  }

  TEST_CASE("DnsResolver: Shutdown mid-hung-resolve returns promptly; on_ready never fires after") {
    struct CbState {
      std::atomic<bool> shutdown_done{false};
      std::atomic<bool> callback_after_shutdown{false};
      std::atomic<int> calls{0};
    };
    auto be = std::make_shared<Backend>();
    be->block_host = "hung.test";
    auto cb = std::make_shared<CbState>();
    {
      DnsResolver resolver(MakeMock(be));
      resolver.Resolve("hung.test", 443, /*gen=*/1, [cb](uint64_t) {
        cb->calls.fetch_add(1);
        if (cb->shutdown_done.load()) cb->callback_after_shutdown.store(true);
      });
      REQUIRE(be->entered.WaitFor(kWaitLimit));

      const auto t0 = std::chrono::steady_clock::now();
      resolver.Shutdown(milliseconds(20));
      const auto elapsed = std::chrono::steady_clock::now() - t0;
      CHECK(elapsed < milliseconds(500));
      cb->shutdown_done.store(true);

      // After Shutdown: no result, and Resolve is a no-op (no new lookup).
      CHECK_FALSE(resolver.TakeResult().has_value());
      resolver.Resolve("example.test", 443, /*gen=*/2, [cb](uint64_t) {
        cb->calls.fetch_add(1);
        if (cb->shutdown_done.load()) cb->callback_after_shutdown.store(true);
      });
      resolver.Shutdown();  // idempotent

      // Release the hung lookup and wait for the detached worker to exit.
      be->gate.Set();
      RequireAllWorkersExited(*be);

      CHECK_FALSE(resolver.TakeResult().has_value());
      CHECK(be->Calls().size() == 1);
    }
    CHECK_FALSE(cb->callback_after_shutdown.load());
    CHECK(cb->calls.load() == 0);
  }

  TEST_CASE("DnsResolver: destructor mid-hung-resolve does not block") {
    auto be = std::make_shared<Backend>();
    be->block_host = "hung.test";
    auto ready = std::make_shared<ReadyLog>();
    std::chrono::steady_clock::duration dtor_time{};
    {
      auto resolver = std::make_unique<DnsResolver>(MakeMock(be));
      resolver->Resolve("hung.test", 443, /*gen=*/1, Recorder(ready));
      REQUIRE(be->entered.WaitFor(kWaitLimit));
      const auto t0 = std::chrono::steady_clock::now();
      resolver.reset();
      dtor_time = std::chrono::steady_clock::now() - t0;
    }
    CHECK(dtor_time < milliseconds(500));
    be->gate.Set();
    RequireAllWorkersExited(*be);
    CHECK(ready->Gens().empty());
  }

  TEST_CASE("DnsResolver: hung workers are bounded; saturation fails fast and recovers") {
    auto be = std::make_shared<Backend>();
    be->block_host = "hung.test";
    auto ready = std::make_shared<ReadyLog>();
    {
      DnsResolver resolver(MakeMock(be), /*max_outstanding_workers=*/2);
      CHECK(resolver.live_workers() == 0);

      // gen 1 hangs on worker #1.
      resolver.Resolve("hung.test", 443, /*gen=*/1, Recorder(ready));
      REQUIRE(be->WaitEntered(1, kWaitLimit));
      CHECK(resolver.live_workers() == 1);

      // gen 2 retires (detaches) the stuck worker #1 and hangs on worker #2.
      resolver.Resolve("hung.test", 443, /*gen=*/2, Recorder(ready));
      REQUIRE(be->WaitEntered(2, kWaitLimit));
      CHECK(resolver.live_workers() == 2);

      // gen 3: both live threads are stuck and the cap is 2, so no third
      // thread may be spawned. on_ready(3) fires synchronously inside
      // Resolve() and the result is a non-ok "saturated" error.
      resolver.Resolve("example.test", 443, /*gen=*/3, Recorder(ready));
      {
        const std::vector<uint64_t> gens = ready->Gens();
        REQUIRE(gens.size() == 1);  // no wait: must already have fired
        CHECK(gens[0] == 3);
      }
      CHECK(resolver.live_workers() == 2);
      std::optional<ResolveResult> r = resolver.TakeResult();
      REQUIRE(r.has_value());
      CHECK(r->generation == 3);
      CHECK_FALSE(r->ok());
      CHECK(r->addresses.empty());
      CHECK(r->error.find("saturated") != std::string::npos);
      CHECK_FALSE(resolver.TakeResult().has_value());
      {
        // The backend never saw a lookup for gen 3, and only 2 threads ran.
        const std::vector<MockCall> calls = be->Calls();
        CHECK(calls.size() == 2);
        std::lock_guard<std::mutex> lk(be->mu);
        CHECK(be->thread_exits.size() == 2);
      }

      // Unblock both hung lookups; the retired workers drop their results at
      // their dead slots and exit, releasing the cap.
      be->gate.Set();
      const auto deadline = std::chrono::steady_clock::now() + kWaitLimit;
      while (resolver.live_workers() != 0 && std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(milliseconds(2));
      }
      REQUIRE(resolver.live_workers() == 0);

      // gen 4 spawns a fresh worker and succeeds: clean recovery.
      resolver.Resolve("example.test", 8443, /*gen=*/4, Recorder(ready));
      REQUIRE(ready->WaitForCount(2, kWaitLimit));
      r = resolver.TakeResult();
      REQUIRE(r.has_value());
      CHECK(r->generation == 4);
      CHECK(r->ok());
      REQUIRE(r->addresses.size() == 1);
      CHECK(PortOf(r->addresses[0]) == 8443);
      CHECK(resolver.live_workers() == 1);

      // Late results of the retired gen 1/2 lookups never surfaced.
      const std::vector<uint64_t> gens = ready->Gens();
      REQUIRE(gens.size() == 2);
      CHECK(gens[0] == 3);
      CHECK(gens[1] == 4);
    }
    // Every helper thread (2 detached retirees + the recovery worker) has
    // fully exited before the test returns.
    RequireAllWorkersExited(*be);
    {
      std::lock_guard<std::mutex> lk(be->mu);
      CHECK(be->thread_exits.size() == 3);
    }
  }

  TEST_CASE("DnsResolver: Shutdown of an idle resolver joins promptly") {
    auto be = std::make_shared<Backend>();
    auto ready = std::make_shared<ReadyLog>();
    DnsResolver resolver(MakeMock(be));
    resolver.Resolve("example.test", 443, /*gen=*/1, Recorder(ready));
    REQUIRE(ready->WaitForCount(1, kWaitLimit));
    const auto t0 = std::chrono::steady_clock::now();
    resolver.Shutdown();
    CHECK(std::chrono::steady_clock::now() - t0 < milliseconds(500));
    CHECK_FALSE(resolver.TakeResult().has_value());
    RequireAllWorkersExited(*be);
  }
}

TEST_SUITE("tls_socket_preresolved") {
  TEST_CASE("TlsSocket::StartConnect(pre-resolved): empty address list fails immediately") {
    TlsSocket sock;
    const std::vector<ResolvedAddress> none;
    CHECK_FALSE(sock.StartConnect("example.test", none, TlsConfig{}));
    CHECK(sock.state() == TlsSocket::State::kFailed);
    CHECK_FALSE(sock.error().empty());
    CHECK(sock.fd() < 0);
  }

  TEST_CASE("TlsSocket::StartConnect(pre-resolved): zero-length address fails immediately") {
    TlsSocket sock;
    std::vector<ResolvedAddress> addrs(1);  // len == 0
    CHECK_FALSE(sock.StartConnect("example.test", addrs, TlsConfig{}));
    CHECK(sock.state() == TlsSocket::State::kFailed);
    CHECK_FALSE(sock.error().empty());
    CHECK(sock.fd() < 0);
  }

  TEST_CASE("TlsSocket::StartConnect(pre-resolved): oversized address length fails immediately") {
    TlsSocket sock;
    ResolvedAddress bad = Loopback4(443);
    bad.len = static_cast<socklen_t>(sizeof(sockaddr_storage) + 1);
    std::vector<ResolvedAddress> addrs{bad, ResolvedAddress{}};
    CHECK_FALSE(sock.StartConnect("example.test", addrs, TlsConfig{}));
    CHECK(sock.state() == TlsSocket::State::kFailed);
    CHECK_FALSE(sock.error().empty());
    CHECK(sock.fd() < 0);
  }
}
