// egrpc — per-channel event thread (design §3).

#include "core/event_thread.h"

#include <limits.h>  // PTHREAD_STACK_MIN
#include <poll.h>
#include <signal.h>

#include <cassert>
#include <cerrno>
#include <utility>

namespace egrpc {
namespace internal {

namespace {

// TSan/ASan shadow frames need far more stack than production defaults; let
// the platform pick under sanitizers rather than exporting a bigger knob.
#if defined(__SANITIZE_ADDRESS__) || defined(__SANITIZE_THREAD__)
constexpr bool kSanitizerBuild = true;
#elif defined(__has_feature)
#if __has_feature(address_sanitizer) || __has_feature(thread_sanitizer)
constexpr bool kSanitizerBuild = true;
#else
constexpr bool kSanitizerBuild = false;
#endif
#else
constexpr bool kSanitizerBuild = false;
#endif

// Identifies the EventThread currently running on this OS thread without
// retaining a pthread_t across join (immune to OS pthread ID recycling).
thread_local EventThread* g_current_event_thread = nullptr;

}  // namespace

EventThread::EventThread() : EventThread(Options{}) {}

EventThread::EventThread(Options options) : options_(options) {}

EventThread::~EventThread() { Stop(); }

void* EventThread::ThreadMain(void* arg) {
  // Block SIGPIPE on the EventThread so OpenSSL writes to peer-reset sockets
  // return EPIPE instead of killing the process (without mutating the host
  // application's process-wide signal dispositions).
  sigset_t sigpipe_mask;
  sigemptyset(&sigpipe_mask);
  sigaddset(&sigpipe_mask, SIGPIPE);
  pthread_sigmask(SIG_BLOCK, &sigpipe_mask, nullptr);

  auto* self = static_cast<EventThread*>(arg);
  struct CurrentThreadGuard {
    explicit CurrentThreadGuard(EventThread* et) { g_current_event_thread = et; }
    ~CurrentThreadGuard() { g_current_event_thread = nullptr; }
  } guard(self);
  self->Loop();
  return nullptr;
}

void EventThread::SetFatalExitCallback(std::function<void()> cb) {
  std::lock_guard<std::mutex> lock(mu_);
  on_fatal_exit_ = std::move(cb);
}

void EventThread::SetWakeupCallback(std::function<void()> cb) {
  std::lock_guard<std::mutex> lock(mu_);
  on_wakeup_ = std::move(cb);
}

void EventThread::Wake() noexcept { wakeup_.Notify(); }

bool EventThread::Start() {
  // mu_ is held across pthread_create so a concurrent Stop() can never
  // observe started_ == true while thread_ is still uninitialized (it would
  // join garbage). The new thread only takes mu_ later, in RunPendingOps.
  std::lock_guard<std::mutex> lock(mu_);
  if (started_ || stop_requested_) return false;
  if (wakeup_.fd() < 0) return false;

  pthread_attr_t attr;
  pthread_attr_init(&attr);
  if (!kSanitizerBuild && options_.stack_size_bytes > 0) {
    size_t stack = options_.stack_size_bytes;
    if (stack < static_cast<size_t>(PTHREAD_STACK_MIN)) stack = PTHREAD_STACK_MIN;
    pthread_attr_setstacksize(&attr, stack);
  }
  const int rc = pthread_create(&thread_, &attr, &EventThread::ThreadMain, this);
  pthread_attr_destroy(&attr);
  if (rc != 0) return false;
  started_ = true;
  return true;
}

void EventThread::Stop(std::function<void()> final_op) {
  bool was_started = false;
  {
    std::lock_guard<std::mutex> lock(mu_);
    const bool first_stop = !stop_requested_;
    stop_requested_ = true;
    was_started = started_;
    if (first_stop && final_op && started_) {
      // Dedicated slot: move-assigning std::function is noexcept, so Stop() on
      // a destructor path never allocates or fails to register final_op.
      final_op_ = std::move(final_op);
    }
  }
  wakeup_.Notify();
  if (!was_started) return;

  if (IsOnEventThread()) {
    assert(!IsOnEventThread() && "EventThread::Stop() must not be called on the EventThread");
    return;
  }

  // Never hold mu_ across pthread_join: the event thread locks mu_ in
  // RunPendingOps(). join_mu_ ensures concurrent Stop() callers all wait
  // until the thread has actually exited.
  std::lock_guard<std::mutex> join_lock(join_mu_);
  if (!joined_) {
    const int rc = pthread_join(thread_, nullptr);
    if (rc == 0) joined_ = true;
  }
}

bool EventThread::Post(std::function<void()> op) {
  {
    std::lock_guard<std::mutex> lock(mu_);
    if (stop_requested_) return false;
    ops_.push_back(std::move(op));
  }
  wakeup_.Notify();
  return true;
}

TimerId EventThread::AddTimer(std::chrono::steady_clock::duration delay, TimerHeap::Callback cb) {
  return timers_.Add(std::chrono::steady_clock::now() + delay, std::move(cb));
}

bool EventThread::CancelTimer(TimerId id) { return timers_.Cancel(id); }

void EventThread::ScheduleNextTurn(std::function<void()> cb) {
  next_turn_ops_.push_back(std::move(cb));
}

void EventThread::WatchFd(int fd, std::function<short()> events_fn,
                          std::function<void(short)> on_ready) {
  watched_fd_ = fd;
  ++watch_generation_;
  watched_events_fn_ = std::move(events_fn);
  watched_on_ready_ = std::move(on_ready);
}

void EventThread::UnwatchFd() {
  watched_fd_ = -1;
  ++watch_generation_;
  watched_events_fn_ = nullptr;
  watched_on_ready_ = nullptr;
}

bool EventThread::IsOnEventThread() const { return g_current_event_thread == this; }

bool EventThread::RunPendingOps() {
  if (on_wakeup_) on_wakeup_();
  std::vector<std::function<void()>> ops;
  std::function<void()> final_op;
  bool stopping;
  {
    std::lock_guard<std::mutex> lock(mu_);
    ops.swap(ops_);
    stopping = stop_requested_;
    if (stopping) final_op = std::move(final_op_);
  }
  for (auto& op : ops) op();
  // Re-drain wakeup side-channel in case an op or concurrent caller queued
  // a no-alloc cancellation while ops were running.
  if (on_wakeup_) on_wakeup_();
  if (final_op) final_op();
  return stopping;
}

void EventThread::Loop() {
  for (;;) {
    // Capture callbacks deferred from the *previous* turn before draining
    // ops/timers, so any ScheduleNextTurn() called during this turn waits for
    // the next loop iteration (with timeout_ms = 0).
    std::vector<std::function<void()>> deferred;
    deferred.swap(next_turn_ops_);

    // Ops first: they may add timers or change fd interest before we sleep.
    // Stop() drains already-queued ops (Post refuses new ones) and runs
    // final_op_, then exits without running remaining timers — pending
    // deadlines/keepalives are meaningless on a dying channel (§5.7).
    if (RunPendingOps()) return;

    timers_.RunDue(std::chrono::steady_clock::now());

    if (!deferred.empty()) {
      for (auto& cb : deferred) cb();
      // Re-check pending ops immediately if a deferred callback ran long.
      if (RunPendingOps()) return;
      timers_.RunDue(std::chrono::steady_clock::now());
    }

    pollfd fds[2];
    fds[0].fd = wakeup_.fd();
    fds[0].events = POLLIN;
    fds[0].revents = 0;
    nfds_t nfds = 1;
    const uint64_t watch_gen = watch_generation_;
    if (watched_fd_ >= 0) {
      fds[1].fd = watched_fd_;
      fds[1].events = watched_events_fn_ ? watched_events_fn_() : 0;
      fds[1].revents = 0;
      nfds = 2;
    }

    const int timeout_ms =
        next_turn_ops_.empty() ? timers_.PollTimeoutMs(std::chrono::steady_clock::now()) : 0;
    const int rc = poll(fds, nfds, timeout_ms);
    if (rc < 0) {
      if (errno == EINTR) continue;
      // poll() can only otherwise fail on programmer error (EBADF/EINVAL).
      // Close the Post() gate, drain already-queued ops, and notify the owner
      // so blocked callers wake with UNAVAILABLE instead of hanging forever.
      std::vector<std::function<void()>> remaining_ops;
      std::function<void()> final_op;
      std::function<void()> fatal_cb;
      {
        std::lock_guard<std::mutex> lock(mu_);
        stop_requested_ = true;
        remaining_ops.swap(ops_);
        final_op = std::move(final_op_);
        fatal_cb = on_fatal_exit_;
      }
      for (auto& op : remaining_ops) op();
      if (final_op) final_op();
      if (fatal_cb) fatal_cb();
      return;
    }

    if (fds[0].revents != 0) {
      wakeup_.Drain();
      // Run queued control ops (Cancel, ReadConsumed, Shutdown) promptly before
      // socket I/O so busy streams cannot starve control operations.
      if (RunPendingOps()) return;
    }

    // Fire due timers before reading late socket bytes so expired deadlines
    // take precedence over delayed responses.
    timers_.RunDue(std::chrono::steady_clock::now());

    if (nfds == 2 && fds[1].revents != 0 && watch_generation_ == watch_gen &&
        watched_fd_ == fds[1].fd && watched_on_ready_) {
      // Copy so the callback may safely WatchFd/UnwatchFd (replacing the
      // member would otherwise destroy the std::function mid-execution).
      auto on_ready = watched_on_ready_;
      on_ready(fds[1].revents);
    }
  }
}

}  // namespace internal
}  // namespace egrpc
