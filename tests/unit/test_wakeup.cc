// egrpc — M1 unit tests for Wakeup (design §2, §3).
//
// Wakeup wraps a non-blocking eventfd. We probe readiness with poll(timeout=0)
// so nothing blocks, and use a real cross-thread poll() only for the wake test
// (bounded to 5 s so a regression cannot hang ctest).
#include <poll.h>

#include <atomic>
#include <chrono>
#include <thread>

#include "doctest/doctest.h"
#include "transport/wakeup.h"

namespace {

using egrpc::internal::Wakeup;

// True if fd currently reports POLLIN (non-blocking probe).
bool HasPollin(int fd) {
  struct pollfd pfd;
  pfd.fd = fd;
  pfd.events = POLLIN;
  pfd.revents = 0;
  const int n = ::poll(&pfd, 1, 0);
  REQUIRE(n >= 0);
  return n == 1 && (pfd.revents & POLLIN) != 0;
}

}  // namespace

TEST_CASE("Wakeup: fd is valid") {
  Wakeup w;
  CHECK(w.fd() >= 0);
}

TEST_CASE("Wakeup: Notify raises POLLIN, Drain clears it") {
  Wakeup w;
  REQUIRE(w.fd() >= 0);

  CHECK_FALSE(HasPollin(w.fd()));

  w.Notify();
  CHECK(HasPollin(w.fd()));

  w.Drain();
  CHECK_FALSE(HasPollin(w.fd()));
}

TEST_CASE("Wakeup: multiple Notify coalesce, one Drain clears all") {
  Wakeup w;
  REQUIRE(w.fd() >= 0);

  for (int i = 0; i < 100; ++i) {
    w.Notify();
  }
  CHECK(HasPollin(w.fd()));

  w.Drain();
  CHECK_FALSE(HasPollin(w.fd()));
}

TEST_CASE("Wakeup: cross-thread Notify wakes a blocked poll promptly") {
  Wakeup w;
  REQUIRE(w.fd() >= 0);

  std::atomic<int> poll_ret{-2};
  std::atomic<long> elapsed_ms{-1};

  std::thread waiter([&] {
    struct pollfd pfd;
    pfd.fd = w.fd();
    pfd.events = POLLIN;
    pfd.revents = 0;
    const auto start = std::chrono::steady_clock::now();
    const int n = ::poll(&pfd, 1, 5000);  // bounded: 5 s ceiling
    const auto end = std::chrono::steady_clock::now();
    elapsed_ms.store(std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count());
    poll_ret.store(n);
  });

  // Give the waiter a moment to reach poll(), then wake it.
  std::this_thread::sleep_for(std::chrono::milliseconds(50));
  w.Notify();

  waiter.join();

  CHECK(poll_ret.load() == 1);
  // Woken promptly: well under the 5 s poll ceiling.
  CHECK(elapsed_ms.load() >= 0);
  CHECK(elapsed_ms.load() < 2000);
}
