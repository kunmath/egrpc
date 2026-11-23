// egrpc — M6 unit tests for the reconnect Backoff state machine (design §5.3).
//
// Backoff is single-threaded and EventThread-owned. These tests drive it with
// deterministic seeds and synthetic Backoff::TimePoints (base + N ms), so
// nothing sleeps and every assertion is reproducible.
#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <limits>
#include <vector>

#include "core/backoff.h"
#include "doctest/doctest.h"

namespace {

using egrpc::internal::Backoff;
using egrpc::internal::BackoffOptions;
using std::chrono::milliseconds;
using TimePoint = Backoff::TimePoint;

TimePoint Base() { return TimePoint{} + std::chrono::hours(1); }
TimePoint At(TimePoint base, int64_t ms) { return base + milliseconds(ms); }

BackoffOptions NoJitter() {
  BackoffOptions o;
  o.jitter = 0.0;
  return o;
}

// Reference model of the un-jittered base backoff for failure n (1-based),
// mirroring §5.3: initial * multiplier^(n-1), capped at max_backoff.
std::vector<int64_t> ExpectedBases(const BackoffOptions& o, int count) {
  std::vector<int64_t> out;
  double base = static_cast<double>(o.initial_backoff.count());
  const double max_ms = static_cast<double>(o.max_backoff.count());
  for (int i = 0; i < count; ++i) {
    if (i != 0) base = std::min(max_ms, base * o.multiplier);
    out.push_back(std::llround(base));
  }
  return out;
}

}  // namespace

TEST_SUITE("backoff") {
  TEST_CASE("Backoff: default options match design §5.3") {
    const BackoffOptions o;
    CHECK(o.initial_backoff == milliseconds(1000));
    CHECK(o.multiplier == doctest::Approx(1.6));
    CHECK(o.max_backoff == milliseconds(120000));
    CHECK(o.jitter == doctest::Approx(0.2));
    CHECK(o.connect_timeout == milliseconds(20000));
    CHECK(o.ready_stability_threshold == milliseconds(60000));

    // A default-constructed Backoff (random seed) carries the same options
    // and starts with no failures recorded.
    Backoff b;
    CHECK(b.attempt() == 0);
    CHECK(b.options().initial_backoff == milliseconds(1000));
    CHECK(b.options().multiplier == doctest::Approx(1.6));
    CHECK(b.options().max_backoff == milliseconds(120000));
    CHECK(b.options().jitter == doctest::Approx(0.2));
    CHECK(b.options().connect_timeout == milliseconds(20000));
    CHECK(b.options().ready_stability_threshold == milliseconds(60000));
    CHECK(b.CurrentConnectTimeout() == milliseconds(20000));

    // Random-seeded instance still honours ±20% on the first failure.
    const Backoff::Step s = b.OnFailure(Base());
    CHECK(s.delay >= milliseconds(800));
    CHECK(s.delay <= milliseconds(1200));
    CHECK(b.attempt() == 1);
  }

  TEST_CASE("Backoff: exponential growth without jitter") {
    Backoff b(NoJitter(), /*seed=*/1);
    const TimePoint t = Base();

    Backoff::Step s = b.OnFailure(t);
    CHECK(s.delay == milliseconds(1000));
    CHECK(b.attempt() == 1);

    s = b.OnFailure(t);
    CHECK(s.delay == milliseconds(1600));
    CHECK(b.attempt() == 2);

    s = b.OnFailure(t);
    CHECK(s.delay == milliseconds(2560));
    CHECK(b.attempt() == 3);
  }

  TEST_CASE("Backoff: growth is 1.6x per failure until capped at max_backoff, no overflow") {
    const BackoffOptions o = NoJitter();
    Backoff b(o, /*seed=*/7);
    const std::vector<int64_t> expected = ExpectedBases(o, 200);
    const TimePoint t = Base();

    int64_t prev = 0;
    bool reached_cap = false;
    for (size_t i = 0; i < expected.size(); ++i) {
      CAPTURE(i);
      const Backoff::Step s = b.OnFailure(t);
      CHECK(b.attempt() == i + 1);
      CHECK(s.delay.count() == expected[i]);
      CHECK(s.delay.count() >= prev);  // monotonic non-decreasing
      CHECK(s.delay <= milliseconds(120000));
      if (s.delay == milliseconds(120000)) reached_cap = true;
      prev = s.delay.count();
    }
    CHECK(reached_cap);
    // 1000 * 1.6^10 ≈ 109951 < 120000; 1000 * 1.6^11 ≈ 175922 → capped.
    CHECK(expected[10] == 109951);
    CHECK(expected[11] == 120000);
    // Well past 100 failures: still exactly the cap.
    const Backoff::Step last = b.OnFailure(t);
    CHECK(last.delay == milliseconds(120000));
    CHECK(b.attempt() == 201);
  }

  TEST_CASE("Backoff: ±20% jitter stays in bounds and actually varies (initial step)") {
    BackoffOptions o;  // initial 1000ms, jitter 0.2
    bool saw_below = false;
    bool saw_above = false;
    int64_t min_seen = std::numeric_limits<int64_t>::max();
    int64_t max_seen = std::numeric_limits<int64_t>::min();
    for (uint64_t seed = 1; seed <= 500; ++seed) {
      Backoff b(o, seed);
      const Backoff::Step s = b.OnFailure(Base());
      CAPTURE(seed);
      CHECK(s.delay >= milliseconds(800));
      CHECK(s.delay <= milliseconds(1200));
      if (s.delay < milliseconds(1000)) saw_below = true;
      if (s.delay > milliseconds(1000)) saw_above = true;
      min_seen = std::min<int64_t>(min_seen, s.delay.count());
      max_seen = std::max<int64_t>(max_seen, s.delay.count());
    }
    CHECK(saw_below);
    CHECK(saw_above);
    CHECK(min_seen != max_seen);
    // 500 uniform samples over ±20% should cover most of the range.
    CHECK(min_seen < 850);
    CHECK(max_seen > 1150);
  }

  TEST_CASE("Backoff: same seed yields identical jittered sequence (determinism)") {
    BackoffOptions o;
    Backoff a(o, 42);
    Backoff b(o, 42);
    for (int i = 0; i < 30; ++i) {
      CAPTURE(i);
      CHECK(a.OnFailure(Base()).delay == b.OnFailure(Base()).delay);
    }
  }

  TEST_CASE("Backoff: jitter at the max_backoff cap stays within [96000, 144000]") {
    BackoffOptions o;  // max 120000, jitter 0.2
    bool saw_below = false;
    bool saw_above = false;
    for (uint64_t seed = 1; seed <= 50; ++seed) {
      Backoff b(o, seed);
      for (int i = 0; i < 40; ++i) b.OnFailure(Base());  // well past the cap
      for (int i = 0; i < 20; ++i) {
        const Backoff::Step s = b.OnFailure(Base());
        CAPTURE(seed);
        CHECK(s.delay >= milliseconds(0));
        CHECK(s.delay >= milliseconds(96000));
        CHECK(s.delay <= milliseconds(144000));
        if (s.delay < milliseconds(120000)) saw_below = true;
        if (s.delay > milliseconds(120000)) saw_above = true;
      }
    }
    CHECK(saw_below);
    CHECK(saw_above);
  }

  TEST_CASE(
      "Backoff: connect timeout before any failure is max(connect_timeout, initial_backoff)") {
    // §5.3 / grpc doc/connection-backoff.md: every attempt, including the
    // first, gets max(MIN_CONNECT_TIMEOUT, current_backoff), where the
    // pre-failure current_backoff is INITIAL_BACKOFF.
    // connect_timeout (20s) > initial_backoff (1s) -> 20s.
    Backoff def(BackoffOptions{}, 1);
    CHECK(def.attempt() == 0);
    CHECK(def.CurrentConnectTimeout() == milliseconds(20000));

    // connect_timeout (300ms) < initial_backoff (1000ms) -> 1000ms, NOT 300ms.
    BackoffOptions o;
    o.connect_timeout = milliseconds(300);
    o.initial_backoff = milliseconds(1000);
    Backoff b(o, 1);
    CHECK(b.attempt() == 0);
    CHECK(b.CurrentConnectTimeout() == milliseconds(1000));
    CHECK(b.CurrentConnectTimeout() != milliseconds(300));

    // Equal values: either side, same answer.
    BackoffOptions eq;
    eq.connect_timeout = milliseconds(1000);
    eq.initial_backoff = milliseconds(1000);
    CHECK(Backoff(eq, 1).CurrentConnectTimeout() == milliseconds(1000));

    // After Reset(), the pre-failure rule applies again.
    b.OnFailure(Base());
    b.Reset();
    CHECK(b.attempt() == 0);
    CHECK(b.CurrentConnectTimeout() == milliseconds(1000));

    // After the first failure: max(300, 1000) = 1000.
    const Backoff::Step s = b.OnFailure(Base());
    CHECK(s.attempt_timeout == milliseconds(1000));
    CHECK(b.CurrentConnectTimeout() == milliseconds(1000));
  }

  TEST_CASE("Backoff: attempt_timeout == max(connect_timeout, base backoff)") {
    const BackoffOptions o = NoJitter();  // connect_timeout 20000
    Backoff b(o, 3);
    const std::vector<int64_t> bases = ExpectedBases(o, 30);
    const TimePoint t = Base();

    for (size_t i = 0; i < bases.size(); ++i) {
      CAPTURE(i);
      const Backoff::Step s = b.OnFailure(t);
      const milliseconds expected_timeout = std::max(milliseconds(20000), milliseconds(bases[i]));
      CHECK(s.attempt_timeout == expected_timeout);
      CHECK(b.CurrentConnectTimeout() == s.attempt_timeout);
      CHECK(s.attempt_timeout <= std::max(o.connect_timeout, o.max_backoff));
    }
    // Spot checks of the crossover: failure 7 base ≈ 16777 < 20000,
    // failure 8 base ≈ 26844 > 20000.
    CHECK(bases[0] == 1000);
    CHECK(bases[6] == 16777);
    CHECK(bases[7] == 26844);
  }

  TEST_CASE("Backoff: attempt_timeout crossover values with jitter enabled") {
    // attempt_timeout derives from the un-jittered base, so it is exact even
    // with jitter on.
    BackoffOptions o;  // jitter 0.2
    Backoff b(o, 99);
    const TimePoint t = Base();
    Backoff::Step s = b.OnFailure(t);  // base 1000
    CHECK(s.attempt_timeout == milliseconds(20000));
    for (int i = 2; i <= 7; ++i) s = b.OnFailure(t);  // base 16777
    CHECK(s.attempt_timeout == milliseconds(20000));
    s = b.OnFailure(t);  // failure 8: base 26844
    CHECK(s.attempt_timeout == milliseconds(26844));
    CHECK(b.CurrentConnectTimeout() == milliseconds(26844));
    for (int i = 0; i < 50; ++i) s = b.OnFailure(t);
    CHECK(s.attempt_timeout == milliseconds(120000));
    CHECK(b.CurrentConnectTimeout() == milliseconds(120000));
  }

  TEST_CASE("Backoff: connect_timeout larger than max_backoff caps attempt_timeout there") {
    BackoffOptions o = NoJitter();
    o.connect_timeout = milliseconds(200000);
    Backoff b(o, 5);
    for (int i = 0; i < 50; ++i) {
      const Backoff::Step s = b.OnFailure(Base());
      CHECK(s.attempt_timeout == milliseconds(200000));
    }
  }

  TEST_CASE("Backoff: READY-stability reset (anti-flap, §5.3)") {
    Backoff b(NoJitter(), 11);
    const TimePoint base = Base();

    b.OnFailure(At(base, 0));
    b.OnFailure(At(base, 0));
    b.OnFailure(At(base, 0));
    REQUIRE(b.attempt() == 3);

    // READY at t0; not yet stable just below the threshold.
    const TimePoint t0 = At(base, 1000);
    b.OnReady(t0);
    CHECK_FALSE(b.CheckAndResetIfStable(At(t0, 59999)));
    CHECK(b.attempt() == 3);

    // Flap: failure after only 30s READY does NOT reset; backoff continues.
    Backoff::Step s = b.OnFailure(At(t0, 30000));
    CHECK(b.attempt() == 4);
    CHECK(s.delay == milliseconds(4096));  // 1000 * 1.6^3

    // READY again at t1; a failure exactly at the threshold DOES reset.
    const TimePoint t1 = At(t0, 100000);
    b.OnReady(t1);
    s = b.OnFailure(At(t1, 60000));
    CHECK(b.attempt() == 1);
    CHECK(s.delay == milliseconds(1000));  // initial, not step 5
    CHECK(s.attempt_timeout == milliseconds(20000));

    // And growth restarts from initial.
    s = b.OnFailure(At(t1, 60000));
    CHECK(b.attempt() == 2);
    CHECK(s.delay == milliseconds(1600));
  }

  TEST_CASE("Backoff: failure past the stability threshold resets to initial") {
    Backoff b(NoJitter(), 12);
    const TimePoint t = Base();
    for (int i = 0; i < 4; ++i) b.OnFailure(t);
    REQUIRE(b.attempt() == 4);
    b.OnReady(t);
    const Backoff::Step s = b.OnFailure(At(t, 61000));
    CHECK(b.attempt() == 1);
    CHECK(s.delay == milliseconds(1000));
  }

  TEST_CASE("Backoff: OnFailure clears READY so a later failure does not reset") {
    Backoff b(NoJitter(), 13);
    const TimePoint t = Base();
    b.OnFailure(t);
    b.OnReady(t);
    b.OnFailure(At(t, 1000));  // flap: attempt 2, READY cleared
    REQUIRE(b.attempt() == 2);
    // No new OnReady: even far in the future this is not "stable".
    CHECK_FALSE(b.CheckAndResetIfStable(At(t, 10 * 60 * 1000)));
    const Backoff::Step s = b.OnFailure(At(t, 10 * 60 * 1000));
    CHECK(b.attempt() == 3);
    CHECK(s.delay == milliseconds(2560));
  }

  TEST_CASE("Backoff: CheckAndResetIfStable at the threshold resets attempt and timeout") {
    Backoff b(NoJitter(), 14);
    const TimePoint t = Base();
    for (int i = 0; i < 9; ++i) b.OnFailure(t);
    REQUIRE(b.attempt() == 9);
    REQUIRE(b.CurrentConnectTimeout() > milliseconds(20000));  // base ≈ 42950

    const TimePoint t1 = At(t, 5000);
    b.OnReady(t1);
    CHECK_FALSE(b.CheckAndResetIfStable(At(t1, 59999)));
    CHECK(b.attempt() == 9);
    CHECK(b.CheckAndResetIfStable(At(t1, 60000)));
    CHECK(b.attempt() == 0);
    CHECK(b.CurrentConnectTimeout() == milliseconds(20000));

    // Reset consumed the READY mark: a second check is a no-op.
    CHECK_FALSE(b.CheckAndResetIfStable(At(t1, 120000)));

    const Backoff::Step s = b.OnFailure(At(t1, 70000));
    CHECK(b.attempt() == 1);
    CHECK(s.delay == milliseconds(1000));
  }

  TEST_CASE("Backoff: CheckAndResetIfStable without OnReady is false") {
    Backoff b(NoJitter(), 15);
    CHECK_FALSE(b.CheckAndResetIfStable(Base()));
    b.OnFailure(Base());
    CHECK_FALSE(b.CheckAndResetIfStable(At(Base(), 3600 * 1000)));
    CHECK(b.attempt() == 1);
  }

  TEST_CASE("Backoff: configurable ready_stability_threshold") {
    BackoffOptions o = NoJitter();
    o.ready_stability_threshold = milliseconds(500);
    Backoff b(o, 16);
    const TimePoint t = Base();
    for (int i = 0; i < 3; ++i) b.OnFailure(t);
    REQUIRE(b.attempt() == 3);

    b.OnReady(t);
    CHECK_FALSE(b.CheckAndResetIfStable(At(t, 499)));
    CHECK(b.attempt() == 3);

    // Flap below 500ms: no reset.
    Backoff::Step s = b.OnFailure(At(t, 300));
    CHECK(b.attempt() == 4);
    CHECK(s.delay == milliseconds(4096));

    // Failure at exactly 500ms READY: reset.
    const TimePoint t1 = At(t, 1000);
    b.OnReady(t1);
    s = b.OnFailure(At(t1, 500));
    CHECK(b.attempt() == 1);
    CHECK(s.delay == milliseconds(1000));

    // CheckAndResetIfStable honours the same threshold.
    b.OnFailure(At(t1, 600));
    REQUIRE(b.attempt() == 2);
    const TimePoint t2 = At(t1, 2000);
    b.OnReady(t2);
    CHECK_FALSE(b.CheckAndResetIfStable(At(t2, 499)));
    CHECK(b.CheckAndResetIfStable(At(t2, 500)));
    CHECK(b.attempt() == 0);
  }

  TEST_CASE("Backoff: Reset clears attempt count and READY mark") {
    Backoff b(NoJitter(), 17);
    const TimePoint t = Base();
    for (int i = 0; i < 5; ++i) b.OnFailure(t);
    REQUIRE(b.attempt() == 5);
    b.OnReady(t);

    b.Reset();
    CHECK(b.attempt() == 0);
    CHECK(b.CurrentConnectTimeout() == milliseconds(20000));
    // ready_since_ was cleared: no amount of elapsed time counts as stable.
    CHECK_FALSE(b.CheckAndResetIfStable(At(t, 3600 * 1000)));

    // Growth restarts at initial_backoff.
    Backoff::Step s = b.OnFailure(t);
    CHECK(b.attempt() == 1);
    CHECK(s.delay == milliseconds(1000));
    s = b.OnFailure(t);
    CHECK(s.delay == milliseconds(1600));
  }

  TEST_CASE("Backoff: negative durations are sanitized to safe non-negative values") {
    BackoffOptions o;
    o.initial_backoff = milliseconds(-1000);
    o.max_backoff = milliseconds(-5);
    o.connect_timeout = milliseconds(-20000);
    o.ready_stability_threshold = milliseconds(-1);
    Backoff b(o, 21);
    CHECK(b.options().initial_backoff >= milliseconds(0));
    CHECK(b.options().max_backoff >= milliseconds(0));
    CHECK(b.options().connect_timeout >= milliseconds(0));
    CHECK(b.options().ready_stability_threshold >= milliseconds(0));
    CHECK(b.CurrentConnectTimeout() >= milliseconds(0));
    for (int i = 0; i < 50; ++i) {
      const Backoff::Step s = b.OnFailure(Base());
      CHECK(s.delay >= milliseconds(0));
      CHECK(s.attempt_timeout >= milliseconds(0));
    }
  }

  TEST_CASE("Backoff: multiplier < 1.0 never shrinks the backoff below initial") {
    BackoffOptions o = NoJitter();
    o.multiplier = 0.5;
    Backoff b(o, 22);
    CHECK(b.options().multiplier >= 1.0);
    for (int i = 0; i < 20; ++i) {
      const Backoff::Step s = b.OnFailure(Base());
      CHECK(s.delay >= milliseconds(1000));
      CHECK(s.delay <= milliseconds(120000));
    }

    o.multiplier = -3.0;
    Backoff neg(o, 23);
    for (int i = 0; i < 20; ++i) {
      const Backoff::Step s = neg.OnFailure(Base());
      CHECK(s.delay >= milliseconds(1000));
      CHECK(s.delay <= milliseconds(120000));
    }
  }

  TEST_CASE("Backoff: jitter outside [0, 1] is clamped") {
    BackoffOptions o;
    o.jitter = -0.5;
    Backoff neg(o, 24);
    CHECK(neg.options().jitter >= 0.0);
    CHECK(neg.options().jitter <= 1.0);
    for (int i = 0; i < 20; ++i) {
      const Backoff::Step s = neg.OnFailure(Base());
      CHECK(s.delay >= milliseconds(0));
      CHECK(s.delay <= milliseconds(120000 * 2));
    }

    o.jitter = 5.0;  // would allow negative delays if unclamped
    for (uint64_t seed = 1; seed <= 200; ++seed) {
      Backoff big(o, seed);
      CHECK(big.options().jitter <= 1.0);
      const Backoff::Step s = big.OnFailure(Base());
      CHECK(s.delay >= milliseconds(0));
      CHECK(s.delay <= milliseconds(2000));  // base 1000 * (1 + 1.0)
    }
  }

  TEST_CASE("Backoff: max_backoff < initial_backoff is safe") {
    BackoffOptions o = NoJitter();
    o.initial_backoff = milliseconds(5000);
    o.max_backoff = milliseconds(100);
    Backoff b(o, 25);
    CHECK(b.options().max_backoff >= b.options().initial_backoff);
    for (int i = 0; i < 50; ++i) {
      const Backoff::Step s = b.OnFailure(Base());
      CHECK(s.delay >= milliseconds(0));
      CHECK(s.delay <= milliseconds(5000));
    }
  }

  TEST_CASE("Backoff: NaN / Inf multiplier and jitter produce safe delays") {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const double inf = std::numeric_limits<double>::infinity();
    const double values[] = {nan, inf, -inf};
    for (double m : values) {
      for (double j : values) {
        BackoffOptions o;
        o.multiplier = m;
        o.jitter = j;
        Backoff b(o, 26);
        CHECK(std::isfinite(b.options().multiplier));
        CHECK(b.options().multiplier >= 1.0);
        CHECK(std::isfinite(b.options().jitter));
        CHECK(b.options().jitter >= 0.0);
        CHECK(b.options().jitter <= 1.0);
        for (int i = 0; i < 50; ++i) {
          const Backoff::Step s = b.OnFailure(Base());
          CHECK(s.delay >= milliseconds(0));
          CHECK(s.delay <= milliseconds(240000));
          CHECK(s.attempt_timeout >= milliseconds(20000));
          CHECK(s.attempt_timeout <= milliseconds(120000));
        }
      }
    }
  }

  TEST_CASE("Backoff: huge multiplier and durations never overflow") {
    BackoffOptions o;
    o.multiplier = 1e300;  // base * multiplier overflows double to +inf
    o.initial_backoff = milliseconds(1000);
    o.max_backoff = milliseconds(std::chrono::hours(24 * 365 * 100));
    o.jitter = 1.0;
    Backoff b(o, 27);
    for (int i = 0; i < 20; ++i) {
      const Backoff::Step s = b.OnFailure(Base());
      CHECK(s.delay >= milliseconds(0));
      CHECK(s.delay <= 2 * o.max_backoff);
      CHECK(s.attempt_timeout <= o.max_backoff);
    }

    BackoffOptions extreme;
    extreme.initial_backoff = milliseconds::max();
    extreme.max_backoff = milliseconds::max();
    extreme.connect_timeout = milliseconds::max();
    extreme.jitter = 1.0;
    Backoff e(extreme, 28);
    CHECK(e.CurrentConnectTimeout() == milliseconds::max());
    for (int i = 0; i < 10; ++i) {
      const Backoff::Step s = e.OnFailure(Base());
      CHECK(s.delay >= milliseconds(0));
      CHECK(s.attempt_timeout >= milliseconds(0));
    }
  }
}
