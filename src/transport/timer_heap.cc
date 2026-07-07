// egrpc — timer heap for keepalive / backoff / deadlines (design §2, §3).

#include "transport/timer_heap.h"

#include <algorithm>
#include <limits>
#include <utility>

namespace egrpc {
namespace internal {

TimerId TimerHeap::Add(Clock::time_point deadline, Callback cb) {
  const TimerId id = next_id_++;
  callbacks_.emplace(id, std::move(cb));
  heap_.push_back(Entry{deadline, id});
  std::push_heap(heap_.begin(), heap_.end(), EntryLater{});
  return id;
}

bool TimerHeap::Cancel(TimerId id) { return callbacks_.erase(id) != 0; }

void TimerHeap::PruneCancelledTop() {
  while (!heap_.empty() && callbacks_.find(heap_.front().id) == callbacks_.end()) {
    std::pop_heap(heap_.begin(), heap_.end(), EntryLater{});
    heap_.pop_back();
  }
}

std::optional<TimerHeap::Clock::time_point> TimerHeap::NextDeadline() {
  PruneCancelledTop();
  if (heap_.empty()) return std::nullopt;
  return heap_.front().deadline;
}

int TimerHeap::PollTimeoutMs(Clock::time_point now) {
  const auto next = NextDeadline();
  if (!next) return -1;
  if (*next <= now) return 0;
  // Round up so poll() never returns before the deadline.
  const auto delta = std::chrono::duration_cast<std::chrono::milliseconds>(*next - now) +
                     std::chrono::milliseconds(1);
  constexpr int64_t kMaxTimeoutMs = std::numeric_limits<int>::max();
  return static_cast<int>(std::min<int64_t>(delta.count(), kMaxTimeoutMs));
}

size_t TimerHeap::RunDue(Clock::time_point now) {
  size_t ran = 0;
  for (;;) {
    PruneCancelledTop();
    if (heap_.empty() || heap_.front().deadline > now) break;
    const TimerId id = heap_.front().id;
    std::pop_heap(heap_.begin(), heap_.end(), EntryLater{});
    heap_.pop_back();
    auto it = callbacks_.find(id);
    Callback cb = std::move(it->second);
    callbacks_.erase(it);
    cb();  // may re-enter Add()/Cancel(); heap entry already removed
    ++ran;
  }
  return ran;
}

}  // namespace internal
}  // namespace egrpc
