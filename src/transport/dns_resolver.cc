// egrpc — non-blocking, generation-tagged DNS resolution (design §4.1).

#include "transport/dns_resolver.h"

#include <netdb.h>
#include <netinet/in.h>

#include <algorithm>
#include <cstring>
#include <system_error>
#include <utility>

namespace egrpc {
namespace internal {

ResolveResult ResolveBlocking(const std::string& host, uint16_t port) {
  ResolveResult res;
  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  addrinfo* list = nullptr;
  const std::string port_str = std::to_string(port);
  const int rc = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &list);
  if (rc != 0) {
    res.error = "failed to resolve '" + host + "': " + gai_strerror(rc);
    return res;
  }
  for (addrinfo* ai = list; ai != nullptr; ai = ai->ai_next) {
    if (ai->ai_addr == nullptr || ai->ai_addrlen == 0 ||
        ai->ai_addrlen > sizeof(sockaddr_storage)) {
      continue;
    }
    ResolvedAddress ra;
    std::memcpy(&ra.addr, ai->ai_addr, ai->ai_addrlen);
    ra.len = ai->ai_addrlen;
    res.addresses.push_back(ra);
  }
  freeaddrinfo(list);
  if (res.addresses.empty()) {
    res.error = "resolution of '" + host + "' returned no usable addresses";
  }
  return res;
}

DnsResolver::DnsResolver(ResolveFn resolve_fn, size_t max_outstanding_workers)
    : resolve_fn_(resolve_fn ? std::move(resolve_fn) : ResolveFn(&ResolveBlocking)),
      max_outstanding_workers_(std::max<size_t>(max_outstanding_workers, 1)),
      live_workers_(std::make_shared<std::atomic<size_t>>(0)) {}

DnsResolver::~DnsResolver() { Shutdown(); }

// Runs on the helper thread. Holds only the shared slot and its own copy of
// the resolve function — never a pointer into DnsResolver or the channel.
void DnsResolver::WorkerLoop(std::shared_ptr<SharedState> state, ResolveFn resolve_fn,
                             std::shared_ptr<std::atomic<size_t>> live_workers) {
  struct LiveWorkerGuard {
    std::atomic<size_t>& count;
    ~LiveWorkerGuard() { count.fetch_sub(1, std::memory_order_acq_rel); }
  } live_guard{*live_workers};
  for (;;) {
    std::string host;
    uint16_t port = 0;
    uint64_t gen = 0;
    {
      std::unique_lock<std::mutex> lk(state->mu);
      state->cv.wait(lk, [&] { return !state->alive || state->has_request; });
      if (!state->alive) {
        state->exited = true;
        state->done_cv.notify_all();
        return;
      }
      host = std::move(state->req_host);
      port = state->req_port;
      gen = state->req_generation;
      state->has_request = false;
      state->in_flight = true;
    }
    ResolveResult res = resolve_fn(host, port);
    res.generation = gen;
    {
      std::lock_guard<std::mutex> lk(state->mu);
      state->in_flight = false;
      if (!state->alive) {
        state->exited = true;
        state->done_cv.notify_all();
        return;
      }
      state->result = std::move(res);
      state->has_result = true;
      // Invoked under the slot mutex while alive: Shutdown()/retirement
      // clear on_ready under the same mutex, so it cannot fire afterwards.
      if (state->on_ready) state->on_ready(gen);
    }
  }
}

void DnsResolver::RetireCurrentSlot(std::chrono::milliseconds grace) {
  if (!slot_) return;
  std::shared_ptr<SharedState> state = std::move(slot_);
  bool exited = false;
  {
    std::unique_lock<std::mutex> lk(state->mu);
    state->alive = false;
    state->has_request = false;
    state->has_result = false;
    state->on_ready = nullptr;
    state->cv.notify_all();
    if (worker_.joinable()) {
      exited = state->done_cv.wait_for(lk, grace, [&] { return state->exited; });
    }
  }
  if (worker_.joinable()) {
    if (exited) {
      worker_.join();
    } else {
      // Mid-resolve (possibly hung getaddrinfo): never block on it. The
      // thread finishes the doomed lookup, sees the dead slot, and exits.
      worker_.detach();
    }
  }
}

void DnsResolver::FailImmediately(uint64_t generation, std::string error,
                                  std::function<void(uint64_t)> on_ready) {
  slot_ = std::make_shared<SharedState>();
  {
    std::lock_guard<std::mutex> lk(slot_->mu);
    slot_->result = ResolveResult{generation, {}, std::move(error)};
    slot_->has_result = true;
  }
  // Runs on the caller (the EventThread) with no resolver lock held; the
  // channel's on_ready only Post()s, so this cannot deadlock.
  if (on_ready) on_ready(generation);
}

void DnsResolver::Resolve(const std::string& host, uint16_t port, uint64_t generation,
                          std::function<void(uint64_t)> on_ready) {
  if (shut_down_) return;
  current_generation_ = generation;

  if (slot_) {
    bool stuck = false;
    {
      std::lock_guard<std::mutex> lk(slot_->mu);
      stuck = slot_->in_flight;
    }
    // A worker-less slot (from an immediate failure) cannot serve requests.
    if (stuck || !worker_.joinable()) RetireCurrentSlot(std::chrono::milliseconds(0));
  }

  if (!slot_) {
    if (live_workers_->load(std::memory_order_acquire) >= max_outstanding_workers_) {
      FailImmediately(generation, "DNS resolver saturated: too many in-flight lookups",
                      std::move(on_ready));
      return;
    }
    slot_ = std::make_shared<SharedState>();
    {
      std::lock_guard<std::mutex> lk(slot_->mu);
      slot_->req_host = host;
      slot_->req_port = port;
      slot_->req_generation = generation;
      slot_->on_ready = on_ready;
      slot_->has_request = true;
    }
    live_workers_->fetch_add(1, std::memory_order_acq_rel);
    try {
      worker_ = std::thread(&DnsResolver::WorkerLoop, slot_, resolve_fn_, live_workers_);
    } catch (const std::system_error& e) {
      live_workers_->fetch_sub(1, std::memory_order_acq_rel);
      slot_.reset();
      FailImmediately(generation, std::string("failed to start DNS resolver thread: ") + e.what(),
                      std::move(on_ready));
    }
    return;
  }

  {
    std::lock_guard<std::mutex> lk(slot_->mu);
    slot_->req_host = host;
    slot_->req_port = port;
    slot_->req_generation = generation;
    slot_->on_ready = std::move(on_ready);
    slot_->has_request = true;
    slot_->has_result = false;  // any unread result is from an older attempt
  }
  slot_->cv.notify_one();
}

std::optional<ResolveResult> DnsResolver::TakeResult() {
  if (!slot_) return std::nullopt;
  ResolveResult result;
  {
    std::lock_guard<std::mutex> lk(slot_->mu);
    if (!slot_->has_result) return std::nullopt;
    result = std::move(slot_->result);
    slot_->result = ResolveResult{};
    slot_->has_result = false;
  }
  if (current_generation_ == 0 || result.generation != current_generation_) return std::nullopt;
  return result;
}

void DnsResolver::Invalidate() {
  current_generation_ = 0;
  if (!slot_) return;
  std::lock_guard<std::mutex> lk(slot_->mu);
  slot_->has_result = false;
  slot_->result = ResolveResult{};
}

void DnsResolver::Shutdown(std::chrono::milliseconds detach_grace) {
  if (shut_down_) return;
  shut_down_ = true;
  current_generation_ = 0;
  RetireCurrentSlot(detach_grace);
}

}  // namespace internal
}  // namespace egrpc
