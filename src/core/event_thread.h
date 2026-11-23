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

  // Must be called before Start(). Invoked on the event thread if poll()
  // exits unexpectedly (non-EINTR error), after stop_requested_ is set and
  // any already-queued ops have been drained.
  void SetFatalExitCallback(std::function<void()> cb);

  // Must be called before Start(). Invoked on the event thread inside
  // RunPendingOps() before queued ops — lets the owner drain an allocation-free
  // side-channel (e.g. intrusive TryCancel queue) on Wake().
  void SetWakeupCallback(std::function<void()> cb);

  // Any thread; noexcept and allocation-free. Wakes the event loop so
  // SetWakeupCallback runs on the event thread.
  void Wake() noexcept;

  // Any thread. Starts the loop; false on pthread_create failure or if
  // already started.
  bool Start();

  // Any thread; idempotent and safe for concurrent callers (all callers block
  // until the event thread has joined). Atomically closes the Post() gate and
  // appends optional `final_op` as the last op run on the event thread before
  // exit, so no op can slip in after `final_op`. Cancels pending timers
  // without running them.
  void Stop(std::function<void()> final_op = {});

  // Any thread. Enqueues op to run on the event thread and wakes the loop.
  // Returns false (op dropped) once Stop() has begun — callers at higher
  // layers surface that as UNAVAILABLE (design §5.7).
  bool Post(std::function<void()> op);

  // --- Event-thread only (checked contract, not enforced) -----------------

  // Schedules cb on the loop after delay. Ops posted via Post() may call
  // this; direct cross-thread use is a §3 ownership violation.
  TimerId AddTimer(std::chrono::steady_clock::duration delay, TimerHeap::Callback cb);
  bool CancelTimer(TimerId id);

  // Schedules cb to run on the next iteration of the event loop (after queued
  // ops and due timers), causing poll() to use a 0 ms timeout instead of
  // spinning inside TimerHeap::RunDue(). Event-thread only.
  void ScheduleNextTurn(std::function<void()> cb);

  // Watches one socket fd alongside the wakeup fd (one socket per channel by
  // design). events_fn is polled each iteration for the POLLIN/POLLOUT mask;
  // on_ready receives revents. Replaces any previous watch.
  void WatchFd(int fd, std::function<short()> events_fn, std::function<void(short)> on_ready);
  void UnwatchFd();

  bool IsOnEventThread() const;

 private:
  static void* ThreadMain(void* arg);
  void Loop();
  // Drains the op queue (and final_op_ if stopping); returns true if a Stop
  // was requested.
  bool RunPendingOps();

  const Options options_;

  Wakeup wakeup_;
  TimerHeap timers_;  // event-thread owned; no lock

  std::mutex mu_;       // guards ops_, final_op_, stop_requested_, started_
  std::mutex join_mu_;  // serializes pthread_join across concurrent Stop() callers
  std::vector<std::function<void()>> ops_;
  std::function<void()> final_op_;  // allocation-free move-in on Stop()
  bool stop_requested_ = false;
  bool started_ = false;
  bool joined_ = false;

  std::function<void()> on_fatal_exit_;  // set before Start(); called on event thread
  std::function<void()> on_wakeup_;      // set before Start(); called on event thread

  pthread_t thread_{};

  // Event-thread owned.
  int watched_fd_ = -1;
  uint64_t watch_generation_ = 0;
  std::function<short()> watched_events_fn_;
  std::function<void(short)> watched_on_ready_;
  std::vector<std::function<void()>> next_turn_ops_;
};

}  // namespace internal
}  // namespace egrpc
