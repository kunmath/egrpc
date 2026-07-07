// egrpc — M1 unit tests for TimerHeap (design §2, §3).
//
// TimerHeap is single-threaded and event-thread-owned. These tests drive it
// with synthetic time_points (base = Clock::now(), offsets in ms) and explicit
// `now` arguments so nothing sleeps: deadlines and "now" are pure inputs.
#include <chrono>
#include <cstddef>
#include <optional>
#include <vector>

#include "doctest/doctest.h"
#include "transport/timer_heap.h"

namespace {

using egrpc::internal::kInvalidTimerId;
using egrpc::internal::TimerHeap;
using egrpc::internal::TimerId;
using Clock = TimerHeap::Clock;

// Base instant; all deadlines are expressed as base + N ms.
Clock::time_point Base() { return Clock::now(); }
Clock::time_point At(Clock::time_point base, int ms) {
  return base + std::chrono::milliseconds(ms);
}

}  // namespace

TEST_CASE("TimerHeap: timers fire in deadline order regardless of add order") {
  TimerHeap heap;
  const auto base = Base();
  std::vector<int> fired;

  // Added out of deadline order: 30, 10, 20.
  heap.Add(At(base, 30), [&] { fired.push_back(30); });
  heap.Add(At(base, 10), [&] { fired.push_back(10); });
  heap.Add(At(base, 20), [&] { fired.push_back(20); });
  CHECK(heap.size() == 3);

  // now = base + 25: only 10 and 20 are due, in deadline order.
  const size_t ran = heap.RunDue(At(base, 25));
  CHECK(ran == 2);
  REQUIRE(fired.size() == 2);
  CHECK(fired[0] == 10);
  CHECK(fired[1] == 20);
  CHECK(heap.size() == 1);

  // now = base + 100: the last one fires.
  CHECK(heap.RunDue(At(base, 100)) == 1);
  REQUIRE(fired.size() == 3);
  CHECK(fired[2] == 30);
  CHECK(heap.empty());
}

TEST_CASE("TimerHeap: equal deadlines fire in insertion order") {
  TimerHeap heap;
  const auto base = Base();
  std::vector<int> fired;

  for (int i = 0; i < 5; ++i) {
    heap.Add(At(base, 10), [&fired, i] { fired.push_back(i); });
  }
  const size_t ran = heap.RunDue(At(base, 10));
  CHECK(ran == 5);
  REQUIRE(fired.size() == 5);
  for (int i = 0; i < 5; ++i) {
    CHECK(fired[static_cast<size_t>(i)] == i);
  }
}

TEST_CASE("TimerHeap: Cancel returns true for pending, false otherwise") {
  TimerHeap heap;
  const auto base = Base();
  bool ran = false;

  const TimerId id = heap.Add(At(base, 10), [&] { ran = true; });
  CHECK(id != kInvalidTimerId);

  // Cancelling a pending timer succeeds; it then never runs.
  CHECK(heap.Cancel(id) == true);
  CHECK(heap.empty());
  CHECK(heap.RunDue(At(base, 100)) == 0);
  CHECK(ran == false);

  // Cancelling the same id again fails.
  CHECK(heap.Cancel(id) == false);

  // Cancelling an id that never existed fails.
  CHECK(heap.Cancel(id + 12345) == false);
  CHECK(heap.Cancel(kInvalidTimerId) == false);

  SUBCASE("Cancel of an already-run timer returns false") {
    const TimerId id2 = heap.Add(At(base, 5), [] {});
    CHECK(heap.RunDue(At(base, 10)) == 1);
    CHECK(heap.Cancel(id2) == false);
  }
}

TEST_CASE("TimerHeap: NextDeadline is nullopt when empty, skips cancelled") {
  TimerHeap heap;
  const auto base = Base();

  CHECK(heap.NextDeadline() == std::nullopt);

  const TimerId first = heap.Add(At(base, 10), [] {});
  heap.Add(At(base, 20), [] {});

  auto nd = heap.NextDeadline();
  REQUIRE(nd.has_value());
  CHECK(*nd == At(base, 10));

  // Cancel the earliest; NextDeadline should surface the next one.
  CHECK(heap.Cancel(first) == true);
  nd = heap.NextDeadline();
  REQUIRE(nd.has_value());
  CHECK(*nd == At(base, 20));
}

TEST_CASE("TimerHeap: PollTimeoutMs semantics") {
  TimerHeap heap;
  const auto base = Base();

  // No timers -> -1 (idle).
  CHECK(heap.PollTimeoutMs(base) == -1);

  heap.Add(At(base, 100), [] {});

  // Deadline already reached or passed -> 0.
  CHECK(heap.PollTimeoutMs(At(base, 100)) == 0);
  CHECK(heap.PollTimeoutMs(At(base, 250)) == 0);

  // Future deadline: rounds up, never early, and stays close.
  SUBCASE("exact millisecond remaining") {
    const int remaining = 100;  // now = base, deadline = base + 100ms
    const int to = heap.PollTimeoutMs(base);
    CHECK(to >= remaining);
    CHECK(to <= remaining + 2);
  }

  SUBCASE("rounds up sub-millisecond remainder, never short") {
    // now = base + 100ms - 500us: remaining is 0.5ms -> must round up to 1.
    const auto now = At(base, 100) - std::chrono::microseconds(500);
    const int to = heap.PollTimeoutMs(now);
    CHECK(to >= 1);
    CHECK(to <= 2);
  }
}

TEST_CASE("TimerHeap: re-entrant Add and Cancel within one RunDue pass") {
  const auto base = Base();

  SUBCASE("callback that Adds a due timer runs it in the same pass") {
    TimerHeap heap;
    std::vector<int> fired;
    heap.Add(At(base, 10), [&] {
      fired.push_back(1);
      // Adds a timer already due relative to the pass's `now` (= base + 50).
      heap.Add(At(base, 20), [&] { fired.push_back(2); });
    });

    const size_t ran = heap.RunDue(At(base, 50));
    CHECK(ran == 2);
    REQUIRE(fired.size() == 2);
    CHECK(fired[0] == 1);
    CHECK(fired[1] == 2);
    CHECK(heap.empty());
  }

  SUBCASE("callback that Cancels a later due timer prevents it running") {
    TimerHeap heap;
    std::vector<int> fired;
    TimerId later = kInvalidTimerId;
    // Earliest callback cancels the second, still-due timer.
    heap.Add(At(base, 10), [&] {
      fired.push_back(1);
      CHECK(heap.Cancel(later) == true);
    });
    later = heap.Add(At(base, 20), [&] { fired.push_back(2); });

    const size_t ran = heap.RunDue(At(base, 50));
    CHECK(ran == 1);
    REQUIRE(fired.size() == 1);
    CHECK(fired[0] == 1);
    CHECK(heap.empty());
  }
}

TEST_CASE("TimerHeap: size/empty track Add, Cancel, RunDue") {
  TimerHeap heap;
  const auto base = Base();

  CHECK(heap.empty());
  CHECK(heap.size() == 0);

  const TimerId a = heap.Add(At(base, 10), [] {});
  const TimerId b = heap.Add(At(base, 20), [] {});
  heap.Add(At(base, 30), [] {});
  CHECK(heap.size() == 3);
  CHECK_FALSE(heap.empty());

  CHECK(heap.Cancel(a) == true);
  CHECK(heap.size() == 2);

  // Run the one remaining due timer (b at +20); +30 stays pending.
  CHECK(heap.RunDue(At(base, 25)) == 1);
  CHECK(heap.size() == 1);
  (void)b;

  CHECK(heap.RunDue(At(base, 100)) == 1);
  CHECK(heap.empty());
  CHECK(heap.size() == 0);
}
