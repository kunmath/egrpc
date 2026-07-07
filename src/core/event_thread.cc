// egrpc — per-channel event thread (design §3).

#include "core/event_thread.h"

#include <limits.h>  // PTHREAD_STACK_MIN
#include <poll.h>

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

}  // namespace

EventThread::EventThread() : EventThread(Options{}) {}

EventThread::EventThread(Options options) : options_(options) {}

EventThread::~EventThread() { Stop(); }

void* EventThread::ThreadMain(void* arg) {
  auto* self = static_cast<EventThread*>(arg);
  self->thread_id_ = pthread_self();
  self->thread_id_set_.store(true, std::memory_order_release);
  self->Loop();
  return nullptr;
}

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

void EventThread::Stop() {
  bool must_join = false;
  {
    std::lock_guard<std::mutex> lock(mu_);
    stop_requested_ = true;
    if (started_ && !joined_) {
      joined_ = true;
      must_join = true;
    }
  }
  wakeup_.Notify();
  if (must_join) pthread_join(thread_, nullptr);
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

void EventThread::WatchFd(int fd, std::function<short()> events_fn,
                          std::function<void(short)> on_ready) {
  watched_fd_ = fd;
  watched_events_fn_ = std::move(events_fn);
  watched_on_ready_ = std::move(on_ready);
}

void EventThread::UnwatchFd() {
  watched_fd_ = -1;
  watched_events_fn_ = nullptr;
  watched_on_ready_ = nullptr;
}

bool EventThread::IsOnEventThread() const {
  if (!thread_id_set_.load(std::memory_order_acquire)) return false;
  return pthread_equal(pthread_self(), thread_id_) != 0;
}

bool EventThread::RunPendingOps() {
  std::vector<std::function<void()>> ops;
  bool stopping;
  {
    std::lock_guard<std::mutex> lock(mu_);
    ops.swap(ops_);
    stopping = stop_requested_;
  }
  for (auto& op : ops) op();
  return stopping;
}

void EventThread::Loop() {
  for (;;) {
    // Ops first: they may add timers or change fd interest before we sleep.
    // Stop() drains already-queued ops (Post refuses new ones), then exits
    // without running remaining timers — pending deadlines/keepalives are
    // meaningless on a dying channel (design §5.7 cancels calls explicitly).
    if (RunPendingOps()) return;

    timers_.RunDue(std::chrono::steady_clock::now());

    pollfd fds[2];
    fds[0].fd = wakeup_.fd();
    fds[0].events = POLLIN;
    fds[0].revents = 0;
    nfds_t nfds = 1;
    if (watched_fd_ >= 0) {
      fds[1].fd = watched_fd_;
      fds[1].events = watched_events_fn_ ? watched_events_fn_() : 0;
      fds[1].revents = 0;
      nfds = 2;
    }

    const int timeout_ms = timers_.PollTimeoutMs(std::chrono::steady_clock::now());
    const int rc = poll(fds, nfds, timeout_ms);
    if (rc < 0) {
      if (errno == EINTR) continue;
      // poll() can only otherwise fail on programmer error (EBADF/EINVAL);
      // treat as fatal for the loop rather than spinning.
      // TODO(M6): surface loop death to ChannelImpl (TRANSIENT_FAILURE) so
      // Post() stops accepting ops into a dead loop.
      return;
    }

    if (fds[0].revents != 0) wakeup_.Drain();
    if (nfds == 2 && fds[1].revents != 0 && watched_on_ready_) {
      // Copy so the callback may safely WatchFd/UnwatchFd (replacing the
      // member would otherwise destroy the std::function mid-execution).
      auto on_ready = watched_on_ready_;
      on_ready(fds[1].revents);
    }
  }
}

}  // namespace internal
}  // namespace egrpc
