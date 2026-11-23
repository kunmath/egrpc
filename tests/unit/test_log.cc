// Unit tests for the pluggable logging API (include/egrpc/log.h).
#include <egrpc/log.h>

#include <atomic>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "doctest/doctest.h"

namespace {

struct LogStateGuard {
  LogStateGuard() = default;
  ~LogStateGuard() {
    egrpc::SetLogSink(nullptr);
    egrpc::SetLogLevel(egrpc::LogLevel::kWarn);
  }
};

struct Captured {
  egrpc::LogLevel level;
  std::string component;
  std::string message;
};

struct Capture {
  std::mutex mu;
  std::vector<Captured> entries;

  egrpc::LogSink Sink() {
    return [this](egrpc::LogLevel level, const char* component, const std::string& message) {
      std::lock_guard<std::mutex> lock(mu);
      entries.push_back({level, component == nullptr ? std::string("<null>") : component, message});
    };
  }
};

}  // namespace

TEST_SUITE("log") {
  TEST_CASE("LogLevelName returns canonical names") {
    CHECK(std::strcmp(egrpc::LogLevelName(egrpc::LogLevel::kDebug), "DEBUG") == 0);
    CHECK(std::strcmp(egrpc::LogLevelName(egrpc::LogLevel::kInfo), "INFO") == 0);
    CHECK(std::strcmp(egrpc::LogLevelName(egrpc::LogLevel::kWarn), "WARN") == 0);
    CHECK(std::strcmp(egrpc::LogLevelName(egrpc::LogLevel::kError), "ERROR") == 0);
    CHECK(std::strcmp(egrpc::LogLevelName(egrpc::LogLevel::kOff), "OFF") == 0);
  }

  TEST_CASE("default level is kWarn and filters lower levels") {
    LogStateGuard guard;
    CHECK(egrpc::GetLogLevel() == egrpc::LogLevel::kWarn);
    Capture cap;
    egrpc::SetLogSink(cap.Sink());
    egrpc::Log(egrpc::LogLevel::kDebug, "t", "d");
    egrpc::Log(egrpc::LogLevel::kInfo, "t", "i");
    egrpc::Log(egrpc::LogLevel::kWarn, "t", "w");
    egrpc::Log(egrpc::LogLevel::kError, "t", "e");
    REQUIRE(cap.entries.size() == 2);
    CHECK(cap.entries[0].level == egrpc::LogLevel::kWarn);
    CHECK(cap.entries[0].component == "t");
    CHECK(cap.entries[0].message == "w");
    CHECK(cap.entries[1].level == egrpc::LogLevel::kError);
    CHECK(cap.entries[1].message == "e");
  }

  TEST_CASE("kDebug delivers all levels; kOff drops everything") {
    LogStateGuard guard;
    Capture cap;
    egrpc::SetLogSink(cap.Sink());
    egrpc::SetLogLevel(egrpc::LogLevel::kDebug);
    CHECK(egrpc::GetLogLevel() == egrpc::LogLevel::kDebug);
    egrpc::Log(egrpc::LogLevel::kDebug, "t", "d");
    egrpc::Log(egrpc::LogLevel::kInfo, "t", "i");
    egrpc::Log(egrpc::LogLevel::kWarn, "t", "w");
    egrpc::Log(egrpc::LogLevel::kError, "t", "e");
    CHECK(cap.entries.size() == 4);

    egrpc::Log(egrpc::LogLevel::kOff, "t", "off");
    CHECK(cap.entries.size() == 4);

    egrpc::SetLogLevel(egrpc::LogLevel::kOff);
    egrpc::Log(egrpc::LogLevel::kError, "t", "e2");
    egrpc::Log(egrpc::LogLevel::kOff, "t", "off2");
    CHECK(cap.entries.size() == 4);
  }

  TEST_CASE("null or empty component is normalized") {
    LogStateGuard guard;
    Capture cap;
    egrpc::SetLogSink(cap.Sink());
    egrpc::Log(egrpc::LogLevel::kError, nullptr, "a");
    egrpc::Log(egrpc::LogLevel::kError, "", "b");
    REQUIRE(cap.entries.size() == 2);
    CHECK(cap.entries[0].component == "egrpc");
    CHECK(cap.entries[1].component == "egrpc");
  }

  TEST_CASE("re-entrant SetLogSink from within a sink does not deadlock") {
    LogStateGuard guard;
    Capture second;
    int first_calls = 0;
    egrpc::SetLogSink([&](egrpc::LogLevel, const char*, const std::string&) {
      ++first_calls;
      egrpc::SetLogSink(second.Sink());
    });
    egrpc::Log(egrpc::LogLevel::kError, "t", "one");
    egrpc::Log(egrpc::LogLevel::kError, "t", "two");
    CHECK(first_calls == 1);
    REQUIRE(second.entries.size() == 1);
    CHECK(second.entries[0].message == "two");
  }

  TEST_CASE("SetLogSink(nullptr) restores default stderr sink") {
    LogStateGuard guard;
    Capture cap;
    egrpc::SetLogSink(cap.Sink());
    egrpc::SetLogSink(nullptr);
    egrpc::Log(egrpc::LogLevel::kError, "test", "default sink message (expected on stderr)");
    CHECK(cap.entries.empty());
  }

  TEST_CASE("concurrent Log/SetLogLevel/SetLogSink is race-free") {
    LogStateGuard guard;
    std::atomic<int> delivered{0};
    auto counting = [&delivered](egrpc::LogLevel, const char*, const std::string&) {
      delivered.fetch_add(1, std::memory_order_relaxed);
    };
    egrpc::SetLogSink(counting);
    std::vector<std::thread> threads;
    for (int t = 0; t < 4; ++t) {
      threads.emplace_back([t, &counting] {
        for (int i = 0; i < 500; ++i) {
          egrpc::Log(egrpc::LogLevel::kError, "stress", "msg");
          if (i % 50 == 0) {
            egrpc::SetLogLevel(t % 2 == 0 ? egrpc::LogLevel::kDebug : egrpc::LogLevel::kWarn);
            egrpc::SetLogSink(counting);
          }
          (void)egrpc::GetLogLevel();
        }
      });
    }
    for (auto& th : threads) th.join();
    CHECK(delivered.load() == 4 * 500);
  }
}
