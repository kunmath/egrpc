// egrpc — timer heap for keepalive / backoff / deadlines (design §2, §3).
//
// Owned exclusively by the EventThread; not thread-safe by design. The event
// loop uses NextDeadline()/PollTimeoutMs() to size its poll() timeout and
// RunDue() to dispatch expired callbacks.
//
// Cancellation is lazy: Cancel() drops the callback immediately, and the heap
// entry is discarded when it surfaces. Timers with equal deadlines fire in
// insertion order.
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <unordered_map>
#include <vector>

namespace egrpc {
namespace internal {

using TimerId = uint64_t;
constexpr TimerId kInvalidTimerId = 0;

class TimerHeap {
 public:
  using Clock = std::chrono::steady_clock;
  using Callback = std::function<void()>;

  TimerHeap() = default;
  TimerHeap(const TimerHeap&) = delete;
  TimerHeap& operator=(const TimerHeap&) = delete;

  // Schedules cb at deadline. Never returns kInvalidTimerId.
  TimerId Add(Clock::time_point deadline, Callback cb);

  // True if the timer was still pending (callback dropped without running).
  bool Cancel(TimerId id);

  // Earliest pending deadline, or nullopt when no timers are pending.
  std::optional<Clock::time_point> NextDeadline();

  // poll() timeout to the next deadline: -1 when idle, 0 when already due,
  // else milliseconds rounded up (so a timer is never polled-for short).
  int PollTimeoutMs(Clock::time_point now);

  // Runs every callback whose deadline is <= now, in deadline order. A
  // callback may Add or Cancel timers; timers it adds with deadline <= now
  // also run in this pass. Returns the number of callbacks run.
  size_t RunDue(Clock::time_point now);

  size_t size() const { return callbacks_.size(); }
  bool empty() const { return callbacks_.empty(); }

 private:
  struct Entry {
    Clock::time_point deadline;
    TimerId id;
  };
  struct EntryLater {
    bool operator()(const Entry& a, const Entry& b) const {
      if (a.deadline != b.deadline) return a.deadline > b.deadline;
      return a.id > b.id;
    }
  };

  // Pops heap entries whose timers were cancelled until the top is live.
  void PruneCancelledTop();

  std::vector<Entry> heap_;  // min-heap via std::push_heap/pop_heap with EntryLater
  std::unordered_map<TimerId, Callback> callbacks_;
  TimerId next_id_ = 1;
};

}  // namespace internal
}  // namespace egrpc
