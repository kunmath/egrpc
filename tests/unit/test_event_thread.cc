// egrpc — M1 unit tests for EventThread (design §3).
//
// The EventThread runs a poll()-driven loop on a background thread and is the
// sole owner of the timer heap. Caller threads interact only through Post();
// AddTimer/CancelTimer are event-thread-only and are exercised from inside
// posted ops. All waits are bounded (wait_for + CHECK) so a regression cannot
// hang ctest; a bounded sleep is used only to assert a cancelled timer did NOT
// fire.
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "core/event_thread.h"
#include "doctest/doctest.h"

namespace {

using egrpc::internal::EventThread;
using egrpc::internal::TimerId;
using namespace std::chrono_literals;

// A one-shot latch: post an op, then Wait() for the event thread to run it.
struct Latch {
  std::mutex mu;
  std::condition_variable cv;
  bool done = false;

  void Signal() {
    {
      std::lock_guard<std::mutex> lk(mu);
      done = true;
    }
    cv.notify_all();
  }

  // Returns true if signalled within the timeout.
  bool Wait(std::chrono::milliseconds timeout) {
    std::unique_lock<std::mutex> lk(mu);
    return cv.wait_for(lk, timeout, [&] { return done; });
  }
};

}  // namespace

TEST_CASE("EventThread: Start/Stop lifecycle") {
  EventThread et;
  CHECK(et.Start() == true);
  // A second Start() while running must fail.
  CHECK(et.Start() == false);

  et.Stop();
  // Stop is idempotent.
  et.Stop();
  // Destructor after Stop() must be safe (runs at scope exit).
}

TEST_CASE("EventThread: ops run on the event thread") {
  EventThread et;
  REQUIRE(et.Start());

  // Main thread is not the event thread.
  CHECK(et.IsOnEventThread() == false);

  Latch latch;
  std::atomic<bool> on_event_thread{false};
  CHECK(et.Post([&] {
    on_event_thread.store(et.IsOnEventThread());
    latch.Signal();
  }));

  REQUIRE(latch.Wait(2000ms));
  CHECK(on_event_thread.load() == true);

  et.Stop();
}

TEST_CASE("EventThread: MPSC from many threads delivers every op") {
  EventThread et;
  REQUIRE(et.Start());

  std::atomic<int> counter{0};
  constexpr int kThreads = 4;
  constexpr int kPerThread = 250;

  std::vector<std::thread> producers;
  producers.reserve(kThreads);
  for (int t = 0; t < kThreads; ++t) {
    producers.emplace_back([&] {
      for (int i = 0; i < kPerThread; ++i) {
        CHECK(et.Post([&] { counter.fetch_add(1, std::memory_order_relaxed); }));
      }
    });
  }
  for (auto& p : producers) p.join();

  // A final flush op: once it runs, every earlier op has run (FIFO drain).
  Latch flushed;
  CHECK(et.Post([&] { flushed.Signal(); }));
  REQUIRE(flushed.Wait(5000ms));

  CHECK(counter.load(std::memory_order_relaxed) == kThreads * kPerThread);

  et.Stop();
}

TEST_CASE("EventThread: Post after Stop returns false") {
  EventThread et;
  REQUIRE(et.Start());
  et.Stop();

  bool ran = false;
  CHECK(et.Post([&] { ran = true; }) == false);
  // Give any (erroneous) delivery a chance, then confirm it did not run.
  std::this_thread::sleep_for(50ms);
  CHECK(ran == false);
}

TEST_CASE("EventThread: AddTimer fires on the event thread") {
  EventThread et;
  REQUIRE(et.Start());

  Latch fired;
  std::atomic<bool> on_event_thread{false};
  CHECK(et.Post([&] {
    et.AddTimer(50ms, [&] {
      on_event_thread.store(et.IsOnEventThread());
      fired.Signal();
    });
  }));

  REQUIRE(fired.Wait(2000ms));
  CHECK(on_event_thread.load() == true);

  et.Stop();
}

TEST_CASE("EventThread: CancelTimer prevents the callback from firing") {
  EventThread et;
  REQUIRE(et.Start());

  std::atomic<bool> ran{false};
  Latch cancelled;
  std::atomic<bool> cancel_ok{false};
  CHECK(et.Post([&] {
    const TimerId id = et.AddTimer(30ms, [&] { ran.store(true); });
    cancel_ok.store(et.CancelTimer(id));
    cancelled.Signal();
  }));

  REQUIRE(cancelled.Wait(2000ms));
  CHECK(cancel_ok.load() == true);

  // Bounded sleep (>30ms) to assert the callback did NOT fire.
  std::this_thread::sleep_for(100ms);
  CHECK(ran.load() == false);

  et.Stop();
}

TEST_CASE("EventThread: Stop returns promptly with a far-future timer pending") {
  EventThread et;
  REQUIRE(et.Start());

  Latch armed;
  CHECK(et.Post([&] {
    et.AddTimer(60s, [] {});  // never expected to fire
    armed.Signal();
  }));
  REQUIRE(armed.Wait(2000ms));

  const auto start = std::chrono::steady_clock::now();
  et.Stop();
  const auto elapsed = std::chrono::steady_clock::now() - start;
  CHECK(elapsed < 2s);
}

TEST_CASE("EventThread: ops queued before Stop still run (drain on shutdown)") {
  EventThread et;
  REQUIRE(et.Start());

  std::atomic<bool> ran{false};
  CHECK(et.Post([&] { ran.store(true); }));
  // Immediately request stop; queued ops must be drained before joining.
  et.Stop();
  CHECK(ran.load() == true);
}

TEST_CASE("EventThread: Stop(final_op) runs final_op atomically as last op and rejects Post()") {
  EventThread et;
  REQUIRE(et.Start());

  int count = 0;
  bool final_ran_on_loop = false;
  int final_seen_count = -1;

  for (int i = 0; i < 5; ++i) {
    REQUIRE(et.Post([&] { ++count; }));
  }

  et.Stop([&] {
    final_ran_on_loop = et.IsOnEventThread();
    final_seen_count = count;
  });

  CHECK(final_ran_on_loop);
  CHECK(final_seen_count == 5);

  bool after_ran = false;
  CHECK_FALSE(et.Post([&] { after_ran = true; }));
  CHECK_FALSE(after_ran);
}

TEST_CASE("EventThread: Concurrent Stop() and Post() stress is race-free") {
  EventThread et;
  REQUIRE(et.Start());

  constexpr int kPosters = 4;
  constexpr int kPostsPerThread = 500;
  constexpr int kStoppers = 2;

  std::atomic<bool> go{false};
  std::atomic<int> ops_accepted{0};
  std::atomic<int> ops_ran{0};
  std::atomic<int> final_runs{0};
  std::atomic<bool> final_done{false};
  std::atomic<bool> op_after_final{false};
  std::atomic<bool> stopper_saw_final_done[kStoppers]{{false}, {false}};

  std::vector<std::thread> threads;
  threads.reserve(kPosters + kStoppers);

  for (int p = 0; p < kPosters; ++p) {
    threads.emplace_back([&] {
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      for (int i = 0; i < kPostsPerThread; ++i) {
        const bool accepted = et.Post([&] {
          if (final_done.load(std::memory_order_acquire)) {
            op_after_final.store(true, std::memory_order_relaxed);
          }
          ops_ran.fetch_add(1, std::memory_order_relaxed);
        });
        if (accepted) {
          ops_accepted.fetch_add(1, std::memory_order_relaxed);
        }
      }
    });
  }

  for (int s = 0; s < kStoppers; ++s) {
    threads.emplace_back([&, s] {
      while (!go.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      // Let posters start hammering before both Stop() callers race.
      std::this_thread::sleep_for(1ms);
      et.Stop([&] {
        final_runs.fetch_add(1, std::memory_order_relaxed);
        // Hold the event thread briefly so the second Stop() caller must wait
        // on pthread_join() rather than returning early.
        std::this_thread::sleep_for(20ms);
        final_done.store(true, std::memory_order_release);
      });
      stopper_saw_final_done[s].store(final_done.load(std::memory_order_acquire),
                                      std::memory_order_relaxed);
    });
  }

  go.store(true, std::memory_order_release);
  for (auto& t : threads) t.join();

  CHECK(final_runs.load() == 1);
  CHECK(stopper_saw_final_done[0].load());
  CHECK(stopper_saw_final_done[1].load());
  CHECK_FALSE(op_after_final.load());
  CHECK(ops_ran.load() == ops_accepted.load());
}

TEST_CASE(
    "EventThread: ScheduleNextTurn runs after queued Post ops and due timers, and defers "
    "self-reschedule") {
  EventThread et;
  REQUIRE(et.Start());

  // Ensure the EventThread is parked in poll() before posting op1 so op1 runs
  // from the post-poll wakeup path.
  Latch ready;
  REQUIRE(et.Post([&] { ready.Signal(); }));
  REQUIRE(ready.Wait(2000ms));
  std::this_thread::sleep_for(10ms);

  // All vector writes occur strictly on the EventThread before done.Signal();
  // the main thread reads only after done.Wait().
  std::vector<std::string> order;
  bool in_first_next_turn = false;
  bool rescheduled_ran_sync = true;
  Latch done;

  REQUIRE(et.Post([&] {
    order.push_back("op1");
    et.ScheduleNextTurn([&] {
      order.push_back("next_turn_1");
      in_first_next_turn = true;
      et.ScheduleNextTurn([&] {
        rescheduled_ran_sync = in_first_next_turn;
        order.push_back("next_turn_2");
        done.Signal();
      });
      // A Post() submitted during next_turn_1 must run before the re-scheduled
      // next_turn_2 executes on the subsequent loop iteration.
      CHECK(et.Post([&] { order.push_back("post_between_turns"); }));
      in_first_next_turn = false;
    });
    CHECK(et.Post([&] { order.push_back("op2"); }));
    et.AddTimer(0ms, [&] { order.push_back("due_timer"); });
  }));

  REQUIRE(done.Wait(2000ms));
  et.Stop();

  auto index_of = [&](const std::string& label) -> size_t {
    for (size_t i = 0; i < order.size(); ++i) {
      if (order[i] == label) return i;
    }
    return order.size();
  };

  REQUIRE(order.size() == 6u);
  const size_t idx_op1 = index_of("op1");
  const size_t idx_op2 = index_of("op2");
  const size_t idx_timer = index_of("due_timer");
  const size_t idx_nt1 = index_of("next_turn_1");
  const size_t idx_between = index_of("post_between_turns");
  const size_t idx_nt2 = index_of("next_turn_2");

  CHECK(idx_op1 == 0u);
  CHECK(idx_op2 < idx_nt1);
  CHECK(idx_timer < idx_nt1);
  CHECK(idx_nt1 < idx_between);
  CHECK(idx_between < idx_nt2);
  CHECK_FALSE(rescheduled_ran_sync);
}

#ifdef NDEBUG
// Note: in Debug builds (!defined(NDEBUG)), EventThread::Stop() explicitly
// hits assert(!IsOnEventThread()) (src/core/event_thread.cc) and aborts.
// Under NDEBUG (Release builds), the self-call guard returns cleanly without
// deadlocking and a subsequent main-thread Stop() joins cleanly.
TEST_CASE("EventThread: Stop() from inside callback returns cleanly without deadlocking") {
  EventThread et;
  REQUIRE(et.Start());

  Latch returned;
  std::atomic<bool> stop_returned{false};
  REQUIRE(et.Post([&] {
    CHECK(et.IsOnEventThread());
    et.Stop();
    stop_returned.store(true, std::memory_order_release);
    returned.Signal();
  }));

  REQUIRE(returned.Wait(2000ms));
  CHECK(stop_returned.load(std::memory_order_acquire));

  // Main-thread Stop() afterward joins cleanly.
  et.Stop();
}
#endif

TEST_CASE("Wake invokes wakeup callback without posting ops") {
  EventThread et;
  static_assert(noexcept(et.Wake()));

  std::mutex mu;
  std::condition_variable cv;
  std::atomic<int> wakes{0};
  int target = 1;

  et.SetWakeupCallback([&] {
    const int count = wakes.fetch_add(1, std::memory_order_acq_rel) + 1;
    std::lock_guard<std::mutex> lk(mu);
    if (count >= target) {
      cv.notify_all();
    }
  });

  REQUIRE(et.Start());

  // Wait for startup loop entry, then let the thread park in poll() with no
  // Post() calls.
  {
    std::unique_lock<std::mutex> lk(mu);
    REQUIRE(cv.wait_for(lk, 2000ms, [&] { return wakes.load(std::memory_order_acquire) >= 1; }));
  }
  std::this_thread::sleep_for(20ms);

  int before_wake = 0;
  {
    std::lock_guard<std::mutex> lk(mu);
    before_wake = wakes.load(std::memory_order_acquire);
    target = before_wake + 1;
  }

  et.Wake();

  {
    std::unique_lock<std::mutex> lk(mu);
    REQUIRE(cv.wait_for(lk, 2000ms,
                        [&] { return wakes.load(std::memory_order_acquire) > before_wake; }));
  }
  CHECK(wakes.load(std::memory_order_acquire) >= 1);
  CHECK(wakes.load(std::memory_order_acquire) > before_wake);

  // Idempotent Stop() after the event thread has joined must succeed cleanly.
  et.Stop();
  et.Stop();
}

TEST_CASE("IsOnEventThread is true only inside event thread and false after Stop") {
  EventThread et;
  CHECK_FALSE(et.IsOnEventThread());

  Latch wakeup_ran;
  std::atomic<bool> wakeup_observed{false};
  std::atomic<bool> wakeup_on_event_thread{false};
  et.SetWakeupCallback([&] {
    if (!wakeup_observed.exchange(true, std::memory_order_acq_rel)) {
      wakeup_on_event_thread.store(et.IsOnEventThread(), std::memory_order_release);
      wakeup_ran.Signal();
    }
  });

  REQUIRE(et.Start());
  CHECK_FALSE(et.IsOnEventThread());

  Latch op_ran;
  std::atomic<bool> op_on_event_thread{false};
  REQUIRE(et.Post([&] {
    op_on_event_thread.store(et.IsOnEventThread(), std::memory_order_release);
    op_ran.Signal();
  }));

  REQUIRE(wakeup_ran.Wait(2000ms));
  REQUIRE(op_ran.Wait(2000ms));
  CHECK(wakeup_on_event_thread.load(std::memory_order_acquire));
  CHECK(op_on_event_thread.load(std::memory_order_acquire));

  et.Stop();
  CHECK_FALSE(et.IsOnEventThread());
}
