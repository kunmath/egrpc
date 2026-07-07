// egrpc — per-channel event thread (design §3).
//
// The EventThread exclusively owns everything below Layer 2: the socket fd,
// the SSL*, the nghttp2_session*, the timer heap, and all CallState
// transitions. Other threads interact only through Post() (the MPSC op
// queue + eventfd wakeup) and per-call condvar completion (from M3 on).
//
// The loop is poll() over {wakeup fd, optional watched socket fd} with a
// timeout equal to the next timer expiry; timers make progress with no
// application calls in flight (keepalive/reconnect/deadlines).
#pragma once

#include <pthread.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <functional>
#include <mutex>
#include <vector>

#include "transport/timer_heap.h"
#include "transport/wakeup.h"

namespace egrpc {
namespace internal {

class EventThread {
 public:
  struct Options {
    // Thread stack size (design §3: bounded memory; 0 = platform default).
    // Sanitizer builds ignore this and use the platform default, since
    // TSan/ASan shadow frames blow small stacks.
    size_t stack_size_bytes = 128 * 1024;
  };

  EventThread();  // default Options
  explicit EventThread(Options options);
  ~EventThread();  // Stop()s and joins

  EventThread(const EventThread&) = delete;
  EventThread& operator=(const EventThread&) = delete;

  // Any thread. Starts the loop; false on pthread_create failure or if
  // already started.
  bool Start();

  // Any thread; idempotent for serialized callers (a second concurrent
  // Stop() may return before the join completes). Wakes the loop, runs
  // already-queued ops, cancels pending timers without running them, joins.
  void Stop();

  // Any thread. Enqueues op to run on the event thread and wakes the loop.
  // Returns false (op dropped) once Stop() has begun — callers at higher
  // layers surface that as UNAVAILABLE (design §5.7).
  bool Post(std::function<void()> op);

  // --- Event-thread only (checked contract, not enforced) -----------------

  // Schedules cb on the loop after delay. Ops posted via Post() may call
  // this; direct cross-thread use is a §3 ownership violation.
  TimerId AddTimer(std::chrono::steady_clock::duration delay, TimerHeap::Callback cb);
  bool CancelTimer(TimerId id);

  // Watches one socket fd alongside the wakeup fd (one socket per channel by
  // design). events_fn is polled each iteration for the POLLIN/POLLOUT mask;
  // on_ready receives revents. Replaces any previous watch.
  void WatchFd(int fd, std::function<short()> events_fn, std::function<void(short)> on_ready);
  void UnwatchFd();

  bool IsOnEventThread() const;

 private:
  static void* ThreadMain(void* arg);
  void Loop();
  // Drains the op queue; returns true if a Stop was requested.
  bool RunPendingOps();

  const Options options_;

  Wakeup wakeup_;
  TimerHeap timers_;  // event-thread owned; no lock

  std::mutex mu_;  // guards ops_, stop_requested_, started_
  std::vector<std::function<void()>> ops_;
  bool stop_requested_ = false;
  bool started_ = false;
  bool joined_ = false;

  pthread_t thread_{};
  std::atomic<bool> thread_id_set_{false};
  pthread_t thread_id_{};  // written once by the thread before thread_id_set_

  // Event-thread owned.
  int watched_fd_ = -1;
  std::function<short()> watched_events_fn_;
  std::function<void(short)> watched_on_ready_;
};

}  // namespace internal
}  // namespace egrpc
