// egrpc — non-blocking, generation-tagged DNS resolution (design §4.1).
//
// getaddrinfo() never runs on the EventThread from M6 on. A lazily-started
// helper thread performs the lookup and hands the result back through a
// heap-allocated SharedState slot (mutex + alive flag + result) that it holds
// only via std::shared_ptr — never a raw pointer into the resolver, channel,
// or EventThread. Results are tagged with the generation of the connect
// attempt that requested them; the EventThread discards stale generations, so
// a slow lookup can never resurrect a superseded attempt.
//
// Shutdown never blocks on a hung getaddrinfo(): an idle worker is joined
// (bounded by a short grace), a mid-resolve worker is detached and its late
// result is dropped at the dead slot (design §4.1, §5.7). The number of live
// worker threads (current + detached retirees still stuck in a lookup) is
// bounded by max_outstanding_workers; when saturated, or when the OS refuses
// to start a thread, Resolve() fails the attempt immediately instead of
// spawning (or throwing), so repeated hung lookups cannot exhaust threads.
#pragma once

#include <sys/socket.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

namespace egrpc {
namespace internal {

struct ResolvedAddress {
  sockaddr_storage addr{};
  socklen_t len = 0;
};

struct ResolveResult {
  uint64_t generation = 0;
  std::vector<ResolvedAddress> addresses;
  std::string error;  // non-empty iff resolution failed
  bool ok() const { return error.empty() && !addresses.empty(); }
};

// Blocking getaddrinfo(AF_UNSPEC, SOCK_STREAM, IPPROTO_TCP). Never call on
// the EventThread of a channel that reconnects; it is the default DnsResolver
// backend and the engine of TlsSocket's blocking convenience overload.
ResolveResult ResolveBlocking(const std::string& host, uint16_t port);

// Owned exclusively by the EventThread: every public method must be called
// from that thread only (design §3). on_ready normally runs on the resolver
// helper thread while it holds the slot mutex, so the lock order is
// slot mutex -> (whatever on_ready locks, e.g. EventThread's op-queue mutex).
// When Resolve() fails immediately (worker cap reached, or thread creation
// failed) on_ready instead runs synchronously inside Resolve() on the
// calling thread, holding no resolver lock. Callers must therefore never
// invoke DnsResolver methods while holding a mutex that on_ready acquires.
class DnsResolver {
 public:
  using ResolveFn = std::function<ResolveResult(const std::string& host, uint16_t port)>;

  // resolve_fn defaults to ResolveBlocking; injectable for Ring-1 tests.
  // max_outstanding_workers bounds live helper threads (active + detached
  // retirees); values < 1 are clamped to 1.
  explicit DnsResolver(ResolveFn resolve_fn = nullptr, size_t max_outstanding_workers = 4);
  ~DnsResolver();

  DnsResolver(const DnsResolver&) = delete;
  DnsResolver& operator=(const DnsResolver&) = delete;
  DnsResolver(DnsResolver&&) = delete;
  DnsResolver& operator=(DnsResolver&&) = delete;

  // Requests resolution of host:port tagged with `generation` (must be
  // non-zero; 0 means "no attempt"). on_ready(generation) is invoked on the
  // helper thread once a result is available — it must only wake the
  // EventThread, which then calls TakeResult(). A worker stuck mid-resolve
  // from an earlier attempt is retired (detached) so this request is never
  // queued behind a hung lookup. If a new worker is needed but
  // max_outstanding_workers are still live, or std::thread creation fails,
  // no thread is spawned: a failed ResolveResult for `generation` is stored
  // and on_ready(generation) is invoked synchronously before returning.
  // No-op after Shutdown().
  void Resolve(const std::string& host, uint16_t port, uint64_t generation,
               std::function<void(uint64_t)> on_ready);

  // Returns the pending result if it matches the current generation; stale
  // or absent results yield std::nullopt (stale ones are consumed).
  std::optional<ResolveResult> TakeResult();

  // Forgets the current generation and any unread result, so a late result
  // from a timed-out attempt is discarded.
  void Invalidate();

  // Idempotent. Marks the slot dead and clears on_ready (on_ready can never
  // fire after this returns), then joins the worker if it exits within
  // detach_grace, otherwise detaches it.
  void Shutdown(std::chrono::milliseconds detach_grace = std::chrono::milliseconds(50));

  // Live helper threads, including detached ones still finishing a lookup.
  // Diagnostics/tests; safe from any thread.
  size_t live_workers() const { return live_workers_->load(std::memory_order_acquire); }

 private:
  struct SharedState {
    std::mutex mu;
    std::condition_variable cv;       // worker waits for a request / death
    std::condition_variable done_cv;  // owner waits for worker exit
    bool alive = true;
    bool exited = false;
    bool in_flight = false;
    bool has_request = false;
    std::string req_host;
    uint16_t req_port = 0;
    uint64_t req_generation = 0;
    std::function<void(uint64_t)> on_ready;
    bool has_result = false;
    ResolveResult result;
  };

  static void WorkerLoop(std::shared_ptr<SharedState> state, ResolveFn resolve_fn,
                         std::shared_ptr<std::atomic<size_t>> live_workers);
  // Installs a fresh worker-less slot_ holding a failed result for
  // `generation`, then invokes on_ready(generation) with no lock held.
  void FailImmediately(uint64_t generation, std::string error,
                       std::function<void(uint64_t)> on_ready);
  // Marks slot_ dead, then joins worker_ if it exits within grace or detaches
  // it otherwise; resets slot_.
  void RetireCurrentSlot(std::chrono::milliseconds grace);

  ResolveFn resolve_fn_;
  const size_t max_outstanding_workers_;
  // Shared with every worker thread so detached retirees can still decrement
  // it after this resolver is gone.
  std::shared_ptr<std::atomic<size_t>> live_workers_;
  std::shared_ptr<SharedState> slot_;
  std::thread worker_;
  uint64_t current_generation_ = 0;
  bool shut_down_ = false;
};

}  // namespace internal
}  // namespace egrpc
