// Unit tests for egrpc::resilient_stream<R> (design §4.6) using a scripted
// fake ClientReaderInterface; no network or EventThread involved.
#include <egrpc/resilient_stream.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <limits>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "doctest/doctest.h"

namespace {

using grpc::Status;
using grpc::StatusCode;
using Clock = std::chrono::steady_clock;

struct Script {
  bool metadata = true;
  std::vector<std::string> msgs;
  Status final_status{StatusCode::UNAVAILABLE, "drop"};
  bool local = false;
  bool block_in_wait = false;  // WaitForInitialMetadata blocks until cancel
  bool block_in_read = false;  // after msgs, Read blocks until cancel
};

class FakeClientReader : public grpc::ClientReaderInterface<std::string> {
 public:
  FakeClientReader(grpc::ClientContext* ctx, Script s) : ctx_(ctx), s_(std::move(s)) {
    auto state = state_;
    if (grpc::internal::ClientContextAccess::BindCancel(ctx_, [state] {
          std::lock_guard<std::mutex> lock(state->mu);
          state->cancelled = true;
          state->cv.notify_all();
        })) {
      state_->cancelled = true;
    }
  }

  void WaitForInitialMetadata() override {
    if (s_.block_in_wait) WaitCancel();
    if (Cancelled()) return;
    if (s_.metadata) grpc::internal::ClientContextAccess::MarkInitialMetadataReceived(ctx_);
  }

  bool Read(std::string* msg) override {
    if (!s_.metadata || Cancelled()) return false;
    if (next_ < s_.msgs.size()) {
      *msg = s_.msgs[next_++];
      return true;
    }
    if (s_.block_in_read) WaitCancel();
    return false;
  }

  bool NextMessageSize(uint32_t* sz) override {
    *sz = 0;
    return false;
  }

  Status Finish() override {
    if (Cancelled()) {
      grpc::internal::ClientContextAccess::MarkStatusLocal(ctx_);
      return Status(StatusCode::CANCELLED, "Cancelled");
    }
    if (s_.local) grpc::internal::ClientContextAccess::MarkStatusLocal(ctx_);
    return s_.final_status;
  }

 private:
  struct State {
    std::mutex mu;
    std::condition_variable cv;
    bool cancelled = false;
  };
  bool Cancelled() {
    std::lock_guard<std::mutex> lock(state_->mu);
    return state_->cancelled;
  }
  void WaitCancel() {
    std::unique_lock<std::mutex> lock(state_->mu);
    state_->cv.wait(lock, [this] { return state_->cancelled; });
  }

  grpc::ClientContext* ctx_;
  Script s_;
  size_t next_ = 0;
  std::shared_ptr<State> state_ = std::make_shared<State>();
};

// Scripted factory; after the scripts run out, returns a terminal
// UNIMPLEMENTED stream so a misbehaving test cannot loop forever.
struct Harness {
  std::vector<Script> scripts;
  std::atomic<int> calls{0};
  std::vector<bool> wait_for_ready_seen;
  std::mutex mu;

  egrpc::resilient_stream<std::string>::Factory factory() {
    return
        [this](
            grpc::ClientContext& ctx) -> std::unique_ptr<grpc::ClientReaderInterface<std::string>> {
          int i = calls++;
          {
            std::lock_guard<std::mutex> lock(mu);
            wait_for_ready_seen.push_back(ctx.wait_for_ready() &&
                                          !ctx.egrpc_initial_metadata_received() &&
                                          !ctx.egrpc_status_is_local());
          }
          Script s;
          if (static_cast<size_t>(i) < scripts.size()) {
            s = scripts[i];
          } else {
            s.metadata = false;
            s.final_status = Status(StatusCode::UNIMPLEMENTED, "exhausted");
          }
          return std::make_unique<FakeClientReader>(&ctx, s);
        };
  }
};

Script Msgs(std::vector<std::string> m, Status end = Status(StatusCode::UNAVAILABLE, "drop")) {
  Script s;
  s.msgs = std::move(m);
  s.final_status = std::move(end);
  return s;
}

Script NoMeta(Status end, bool local = false) {
  Script s;
  s.metadata = false;
  s.final_status = std::move(end);
  s.local = local;
  return s;
}

egrpc::ResilientStreamOptions FastOpts() {
  egrpc::ResilientStreamOptions o;
  o.initial_retry_delay = std::chrono::milliseconds(0);
  o.max_retry_delay = std::chrono::milliseconds(0);
  return o;
}

std::vector<std::string> Drain(egrpc::resilient_stream<std::string>& rs) {
  std::vector<std::string> out;
  std::string m;
  while (rs.Read(&m)) out.push_back(m);
  return out;
}

const Status kTerminal(StatusCode::UNIMPLEMENTED, "end");

}  // namespace

TEST_SUITE("resilient_stream") {
  TEST_CASE("DefaultShouldRetry matrix") {
    for (int c = 0; c <= 16; ++c) {
      const auto code = static_cast<StatusCode>(c);
      for (bool local : {false, true}) {
        bool expected = false;
        switch (code) {
          case StatusCode::UNAVAILABLE:
          case StatusCode::RESOURCE_EXHAUSTED:
          case StatusCode::ABORTED:
          case StatusCode::INTERNAL:
          case StatusCode::UNKNOWN:
          case StatusCode::DEADLINE_EXCEEDED:
            expected = true;
            break;
          case StatusCode::CANCELLED:
            expected = !local;
            break;
          default:
            expected = false;
        }
        CAPTURE(c);
        CAPTURE(local);
        CHECK(egrpc::DefaultShouldRetry(Status(code, ""), local) == expected);
      }
    }
  }

  TEST_CASE("custom should_retry overrides default") {
    SUBCASE("retries OK") {
      Harness h;
      h.scripts = {Msgs({"a"}, Status::OK), Msgs({"b"}, kTerminal)};
      auto o = FastOpts();
      o.should_retry = [](const Status& s, bool) { return s.ok(); };
      egrpc::resilient_stream<std::string> rs(o, h.factory());
      CHECK(Drain(rs) == std::vector<std::string>{"a", "b"});
      CHECK(rs.status().error_code() == StatusCode::UNIMPLEMENTED);
    }
    SUBCASE("INTERNAL terminal") {
      Harness h;
      h.scripts = {Msgs({"a"}, Status(StatusCode::INTERNAL, "x")), Msgs({"b"})};
      auto o = FastOpts();
      o.should_retry = [](const Status&, bool) { return false; };
      egrpc::resilient_stream<std::string> rs(o, h.factory());
      CHECK(Drain(rs) == std::vector<std::string>{"a"});
      CHECK(rs.status().error_code() == StatusCode::INTERNAL);
      CHECK(h.calls == 1);
    }
    SUBCASE("stop wins over always-retry") {
      Harness h;
      Script s = Msgs({"a"});
      s.block_in_read = true;
      h.scripts = {s, Msgs({"b"})};
      auto o = FastOpts();
      o.should_retry = [](const Status&, bool) { return true; };
      egrpc::resilient_stream<std::string> rs(o, h.factory());
      std::string m;
      REQUIRE(rs.Read(&m));
      std::thread t([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        rs.stop();
      });
      CHECK_FALSE(rs.Read(&m));
      t.join();
      CHECK(rs.status().error_code() == StatusCode::CANCELLED);
      CHECK(h.calls == 1);
    }
  }

  TEST_CASE("fresh wait_for_ready context per attempt; deadline retries") {
    Harness h;
    h.scripts = {Msgs({"a"}, Status(StatusCode::DEADLINE_EXCEEDED, "server")),
                 NoMeta(Status(StatusCode::DEADLINE_EXCEEDED, "local"), true),
                 Msgs({"b"}, kTerminal)};
    egrpc::resilient_stream<std::string> rs(FastOpts(), h.factory());
    CHECK(Drain(rs) == std::vector<std::string>{"a", "b"});
    CHECK(h.calls == 3);
    for (bool w : h.wait_for_ready_seen) CHECK(w);
  }

  TEST_CASE("reconnect callback semantics") {
    SUBCASE("first establishment does not fire; failed pre-metadata attempts do not count") {
      Harness h;
      h.scripts = {NoMeta(Status(StatusCode::UNAVAILABLE, "tonly")), Msgs({"a"}),
                   NoMeta(Status(StatusCode::UNAVAILABLE, "fail")), Msgs({"b"}, kTerminal)};
      auto o = FastOpts();
      std::vector<int> fired_at;
      o.on_reconnected = [&] { fired_at.push_back(h.calls.load()); };
      egrpc::resilient_stream<std::string> rs(o, h.factory());
      CHECK(Drain(rs) == std::vector<std::string>{"a", "b"});
      CHECK(fired_at == std::vector<int>{4});
    }
    SUBCASE("fire_on_first_connect") {
      Harness h;
      h.scripts = {Msgs({"a"}), Msgs({"b"}), Msgs({"c"}, kTerminal)};
      auto o = FastOpts();
      o.fire_on_first_connect = true;
      int fired = 0;
      o.on_reconnected = [&] { ++fired; };
      egrpc::resilient_stream<std::string> rs(o, h.factory());
      CHECK(Drain(rs).size() == 3);
      CHECK(fired == 3);
    }
    SUBCASE("runs on caller thread") {
      Harness h;
      h.scripts = {Msgs({"a"}), Msgs({"b"}, kTerminal)};
      auto o = FastOpts();
      std::thread::id cb_id;
      o.on_reconnected = [&] { cb_id = std::this_thread::get_id(); };
      std::thread::id reader_id;
      std::thread t([&] {
        reader_id = std::this_thread::get_id();
        egrpc::resilient_stream<std::string> rs(o, h.factory());
        Drain(rs);
      });
      t.join();
      CHECK(cb_id == reader_id);
    }
    SUBCASE("established-then-empty death fires and fires again") {
      Harness h;
      h.scripts = {Msgs({"a"}), Msgs({}), Msgs({"b"}, kTerminal)};
      auto o = FastOpts();
      std::vector<int> fired_at;
      o.on_reconnected = [&] { fired_at.push_back(h.calls.load()); };
      egrpc::resilient_stream<std::string> rs(o, h.factory());
      CHECK(Drain(rs) == std::vector<std::string>{"a", "b"});
      CHECK(fired_at == std::vector<int>{2, 3});
    }
  }

  TEST_CASE("local vs server CANCELLED") {
    SUBCASE("local CANCELLED terminates") {
      Harness h;
      h.scripts = {Msgs({"a"}, Status(StatusCode::CANCELLED, "local")), Msgs({"b"})};
      h.scripts[0].local = true;
      egrpc::resilient_stream<std::string> rs(FastOpts(), h.factory());
      CHECK(Drain(rs) == std::vector<std::string>{"a"});
      CHECK(rs.status().error_code() == StatusCode::CANCELLED);
    }
    SUBCASE("server CANCELLED retries") {
      Harness h;
      h.scripts = {Msgs({"a"}, Status(StatusCode::CANCELLED, "srv")), Msgs({"b"}, kTerminal)};
      egrpc::resilient_stream<std::string> rs(FastOpts(), h.factory());
      CHECK(Drain(rs) == std::vector<std::string>{"a", "b"});
    }
  }

  TEST_CASE("status() semantics") {
    Harness h;
    h.scripts = {Msgs({"a"}, Status(StatusCode::NOT_FOUND, "nf", "details"))};
    egrpc::resilient_stream<std::string> rs(FastOpts(), h.factory());
    CHECK(rs.status().ok());
    std::string m;
    CHECK(rs.Read(&m));
    CHECK(rs.status().ok());
    CHECK_FALSE(rs.Read(&m));
    CHECK(rs.status().error_code() == StatusCode::NOT_FOUND);
    CHECK(rs.status().error_message() == "nf");
    CHECK(rs.status().error_details() == "details");
    CHECK_FALSE(rs.Read(&m));
    CHECK(h.calls == 1);
  }

  TEST_CASE("exception from on_reconnected") {
    Harness h;
    h.scripts = {Msgs({"a"}), Msgs({"b", "c"}, kTerminal)};
    auto o = FastOpts();
    int fired = 0;
    o.on_reconnected = [&] {
      ++fired;
      throw std::runtime_error("boom");
    };
    egrpc::resilient_stream<std::string> rs(o, h.factory());
    std::string m;
    REQUIRE(rs.Read(&m));
    CHECK_THROWS_AS(rs.Read(&m), std::runtime_error);
    std::vector<std::string> rest = Drain(rs);
    CHECK(rest == std::vector<std::string>{"b", "c"});
    CHECK(fired == 1);
    rs.stop();  // must not deadlock
  }

  TEST_CASE("exception from on_reconnected then stop") {
    Harness h;
    h.scripts = {Msgs({"a"}), Msgs({"b"}, kTerminal)};
    auto o = FastOpts();
    o.on_reconnected = [] { throw std::runtime_error("boom"); };
    egrpc::resilient_stream<std::string> rs(o, h.factory());
    std::string m;
    REQUIRE(rs.Read(&m));
    CHECK_THROWS(rs.Read(&m));
    rs.stop();
    CHECK_FALSE(rs.Read(&m));
    CHECK(rs.status().error_code() == StatusCode::CANCELLED);
  }

  TEST_CASE("factory nullptr and throwing") {
    SUBCASE("nullptr") {
      egrpc::resilient_stream<std::string> rs(FastOpts(), [](grpc::ClientContext&) {
        return std::unique_ptr<grpc::ClientReaderInterface<std::string>>();
      });
      std::string m;
      CHECK_FALSE(rs.Read(&m));
      CHECK(rs.status().error_code() == StatusCode::INTERNAL);
    }
    SUBCASE("throws") {
      bool thrown = false;
      egrpc::resilient_stream<std::string> rs(
          FastOpts(),
          [&](grpc::ClientContext&) -> std::unique_ptr<grpc::ClientReaderInterface<std::string>> {
            if (!thrown) {
              thrown = true;
              throw std::runtime_error("factory");
            }
            return nullptr;
          });
      std::string m;
      CHECK_THROWS_AS(rs.Read(&m), std::runtime_error);
      rs.stop();  // active_ctx_ cleared; must not touch a dead context
      CHECK_FALSE(rs.Read(&m));
      CHECK(rs.status().error_code() == StatusCode::CANCELLED);
    }
  }

  TEST_CASE("stop before first Read") {
    Harness h;
    h.scripts = {Msgs({"a"})};
    auto o = FastOpts();
    int fired = 0;
    o.fire_on_first_connect = true;
    o.on_reconnected = [&] { ++fired; };
    egrpc::resilient_stream<std::string> rs(o, h.factory());
    rs.stop();
    std::string m;
    CHECK_FALSE(rs.Read(&m));
    CHECK(h.calls == 0);
    CHECK(fired == 0);
    CHECK(rs.status().error_code() == StatusCode::CANCELLED);
  }

  TEST_CASE("stop unblocks Read in WaitForInitialMetadata and Read") {
    for (bool in_wait : {true, false}) {
      CAPTURE(in_wait);
      Harness h;
      Script s = Msgs({});
      if (in_wait) {
        s.block_in_wait = true;
      } else {
        s.block_in_read = true;
      }
      h.scripts = {s, Msgs({"x"})};
      egrpc::resilient_stream<std::string> rs(FastOpts(), h.factory());
      std::thread t([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        rs.stop();
      });
      std::string m;
      CHECK_FALSE(rs.Read(&m));
      t.join();
      CHECK(rs.status().error_code() == StatusCode::CANCELLED);
      CHECK(h.calls == 1);
    }
  }

  TEST_CASE("stop wakes retry backoff quickly") {
    Harness h;
    h.scripts = {NoMeta(Status(StatusCode::UNAVAILABLE, "x")), Msgs({"a"})};
    egrpc::ResilientStreamOptions o;
    o.initial_retry_delay = std::chrono::milliseconds(10000);
    o.max_retry_delay = std::chrono::milliseconds(10000);
    o.retry_backoff_jitter = 0.0;
    egrpc::resilient_stream<std::string> rs(o, h.factory());
    Clock::time_point stop_at;
    std::thread t([&] {
      std::this_thread::sleep_for(std::chrono::milliseconds(100));
      stop_at = Clock::now();
      rs.stop();
    });
    std::string m;
    CHECK_FALSE(rs.Read(&m));
    const auto woke = Clock::now();
    t.join();
    CHECK(woke - stop_at < std::chrono::milliseconds(500));
    CHECK(rs.status().error_code() == StatusCode::CANCELLED);
    CHECK(h.calls == 1);
  }

  TEST_CASE("stop waits for in-flight on_reconnected") {
    Harness h;
    h.scripts = {Msgs({"a"}), Msgs({"post-stop"})};
    auto o = FastOpts();
    std::promise<void> entered;
    std::promise<void> release;
    auto release_f = release.get_future().share();
    std::atomic<bool> callback_finished{false};
    std::atomic<int> fired{0};
    o.on_reconnected = [&] {
      ++fired;
      entered.set_value();
      release_f.wait();
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      callback_finished = true;
    };
    egrpc::resilient_stream<std::string> rs(o, h.factory());
    std::string m;
    REQUIRE(rs.Read(&m));
    bool second = true;
    std::thread reader([&] { second = rs.Read(&m); });
    entered.get_future().wait();
    std::atomic<bool> stop_returned{false};
    std::thread stopper([&] {
      rs.stop();
      stop_returned = true;
      CHECK(callback_finished.load());
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK_FALSE(stop_returned.load());
    release.set_value();
    stopper.join();
    reader.join();
    CHECK_FALSE(second);
    CHECK(m == "a");
    CHECK(fired == 1);
    CHECK(rs.status().error_code() == StatusCode::CANCELLED);
  }

  TEST_CASE("concurrent stop is idempotent") {
    Harness h;
    Script s = Msgs({});
    s.block_in_read = true;
    h.scripts = {s};
    egrpc::resilient_stream<std::string> rs(FastOpts(), h.factory());
    std::thread reader([&] {
      std::string m;
      CHECK_FALSE(rs.Read(&m));
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    std::vector<std::thread> ts;
    for (int i = 0; i < 8; ++i) ts.emplace_back([&] { rs.stop(); });
    for (auto& t : ts) t.join();
    reader.join();
    CHECK(rs.status().error_code() == StatusCode::CANCELLED);
  }

  TEST_CASE("early destruction while open") {
    Harness h;
    h.scripts = {Msgs({"a", "b"})};
    {
      egrpc::resilient_stream<std::string> rs(FastOpts(), h.factory());
      std::string m;
      REQUIRE(rs.Read(&m));
    }
    CHECK(h.calls == 1);
  }

  TEST_CASE("single-arg constructor") {
    Harness h;
    h.scripts = {Msgs({"a"}, kTerminal)};
    egrpc::resilient_stream<std::string> rs(h.factory());
    CHECK(Drain(rs) == std::vector<std::string>{"a"});
  }

  TEST_CASE("backoff sanitization tolerates garbage options") {
    Harness h;
    h.scripts = {NoMeta(Status(StatusCode::UNAVAILABLE, "x")), Msgs({"a"}, kTerminal)};
    egrpc::ResilientStreamOptions o;
    o.initial_retry_delay = std::chrono::milliseconds(-5);
    o.max_retry_delay = std::chrono::milliseconds(-10);
    o.retry_backoff_multiplier = std::numeric_limits<double>::quiet_NaN();
    o.retry_backoff_jitter = 7.0;
    o.retry_reset_threshold = std::chrono::milliseconds(-1);
    egrpc::resilient_stream<std::string> rs(o, h.factory());
    CHECK(Drain(rs) == std::vector<std::string>{"a"});
  }

  TEST_CASE("stop with buffered messages on open reader") {
    Harness h;
    h.scripts = {Msgs({"a", "b", "c"})};
    egrpc::resilient_stream<std::string> rs(FastOpts(), h.factory());
    std::string m;
    REQUIRE(rs.Read(&m));
    rs.stop();
    CHECK_FALSE(rs.Read(&m));
    CHECK(m == "a");
    CHECK(rs.status().error_code() == StatusCode::CANCELLED);
  }

  TEST_CASE("stop concurrent with throwing on_reconnected") {
    Harness h;
    h.scripts = {Msgs({"a"}), Msgs({"b", "c"})};
    auto o = FastOpts();
    std::promise<void> entered;
    std::atomic<bool> stop_called{false};
    o.on_reconnected = [&] {
      entered.set_value();
      // Give stop() time to block on the in-flight callback.
      std::this_thread::sleep_for(std::chrono::milliseconds(50));
      throw std::runtime_error("boom");
    };
    egrpc::resilient_stream<std::string> rs(o, h.factory());
    std::string m;
    REQUIRE(rs.Read(&m));
    std::thread stopper([&] {
      entered.get_future().wait();
      rs.stop();
      stop_called = true;
    });
    CHECK_THROWS_AS(rs.Read(&m), std::runtime_error);
    stopper.join();
    CHECK_FALSE(rs.Read(&m));
    CHECK(m == "a");
    CHECK(rs.status().error_code() == StatusCode::CANCELLED);
  }

  TEST_CASE("stop waits for blocking should_retry; CANCELLED wins") {
    Harness h;
    h.scripts = {Msgs({"a"}, Status(StatusCode::NOT_FOUND, "nf"))};
    auto o = FastOpts();
    std::promise<void> entered;
    std::promise<void> release;
    auto release_f = release.get_future().share();
    std::atomic<bool> finished{false};
    o.should_retry = [&](const Status&, bool) {
      entered.set_value();
      release_f.wait();
      std::this_thread::sleep_for(std::chrono::milliseconds(20));
      finished = true;
      return false;
    };
    egrpc::resilient_stream<std::string> rs(o, h.factory());
    std::string m;
    REQUIRE(rs.Read(&m));
    bool second = true;
    std::thread reader([&] { second = rs.Read(&m); });
    entered.get_future().wait();
    std::atomic<bool> stop_returned{false};
    std::thread stopper([&] {
      rs.stop();
      stop_returned = true;
      CHECK(finished.load());
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    CHECK_FALSE(stop_returned.load());
    release.set_value();
    stopper.join();
    reader.join();
    CHECK_FALSE(second);
    CHECK(rs.status().error_code() == StatusCode::CANCELLED);
  }

  TEST_CASE("RetryBackoff slow multiplier reaches max beyond 64 retries; reset") {
    egrpc::detail::RetryBackoff b;
    b.Configure(std::chrono::milliseconds(100), 1.01, std::chrono::milliseconds(30000), 0.0);
    CHECK(b.Next() == std::chrono::milliseconds(100));
    std::chrono::milliseconds at64{0};
    std::chrono::milliseconds last{0};
    for (int i = 1; i < 1000; ++i) {
      last = b.Next();
      if (i == 64) at64 = last;
    }
    CHECK(at64 < std::chrono::milliseconds(200));
    CHECK(last == std::chrono::milliseconds(30000));
    b.Reset();
    CHECK(b.Next() == std::chrono::milliseconds(100));
    CHECK(b.Next() == std::chrono::milliseconds(101));
  }

  TEST_CASE("RetryBackoff extreme durations clamp safely") {
    for (double jitter : {0.0, 0.2}) {
      CAPTURE(jitter);
      egrpc::detail::RetryBackoff b;
      b.Configure(std::chrono::milliseconds::max(), 1.6, std::chrono::milliseconds::max(), jitter);
      for (int i = 0; i < 5; ++i) {
        const auto d = b.Next();
        CHECK(d.count() > 0);
        CHECK(d.count() <= 9000000000000000000LL);
      }
    }
  }

  TEST_CASE("stop wakes a max-duration backoff") {
    for (double jitter : {0.0, 0.2}) {
      CAPTURE(jitter);
      Harness h;
      h.scripts = {NoMeta(Status(StatusCode::UNAVAILABLE, "x"))};
      egrpc::ResilientStreamOptions o;
      o.initial_retry_delay = std::chrono::milliseconds::max();
      o.max_retry_delay = std::chrono::milliseconds::max();
      o.retry_backoff_jitter = jitter;
      egrpc::resilient_stream<std::string> rs(o, h.factory());
      std::thread t([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        rs.stop();
      });
      std::string m;
      CHECK_FALSE(rs.Read(&m));
      t.join();
      CHECK(rs.status().error_code() == StatusCode::CANCELLED);
    }
  }

  TEST_CASE("stable stream resets backoff") {
    Harness h;
    h.scripts = {NoMeta(Status(StatusCode::UNAVAILABLE, "x")), Msgs({"a"}), Msgs({"b"}, kTerminal)};
    egrpc::ResilientStreamOptions o;
    o.initial_retry_delay = std::chrono::milliseconds(1);
    o.retry_backoff_multiplier = 10000.0;
    o.max_retry_delay = std::chrono::milliseconds(10000);
    o.retry_backoff_jitter = 0.0;
    o.retry_reset_threshold = std::chrono::milliseconds(0);
    egrpc::resilient_stream<std::string> rs(o, h.factory());
    const auto start = Clock::now();
    // Without reset the second retry would wait 10 s.
    CHECK(Drain(rs) == std::vector<std::string>{"a", "b"});
    CHECK(Clock::now() - start < std::chrono::milliseconds(2000));
  }
}
