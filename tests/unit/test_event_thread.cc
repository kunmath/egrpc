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
#include <functional>
#include <mutex>
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
