#include <egrpc/log.h>

#include <atomic>
#include <cstdio>
#include <mutex>
#include <utility>

namespace egrpc {

namespace {

struct LogState {
  std::atomic<int> min_level{static_cast<int>(LogLevel::kWarn)};
  std::mutex mu;
  LogSink sink;
};

LogState& State() {
  static LogState state;
  return state;
}

}  // namespace

void SetLogSink(LogSink sink) {
  LogState& s = State();
  LogSink old;
  {
    std::lock_guard<std::mutex> lock(s.mu);
    old = std::exchange(s.sink, std::move(sink));
  }
  // `old` is destroyed outside the lock.
}

void SetLogLevel(LogLevel min_level) {
  State().min_level.store(static_cast<int>(min_level), std::memory_order_relaxed);
}

LogLevel GetLogLevel() {
  return static_cast<LogLevel>(State().min_level.load(std::memory_order_relaxed));
}

void Log(LogLevel level, const char* component, const std::string& message) {
  LogState& s = State();
  const int min = s.min_level.load(std::memory_order_relaxed);
  if (level == LogLevel::kOff || min == static_cast<int>(LogLevel::kOff) ||
      static_cast<int>(level) < min) {
    return;
  }
  const char* comp = (component != nullptr && component[0] != '\0') ? component : "egrpc";
  LogSink sink;
  {
    std::lock_guard<std::mutex> lock(s.mu);
    sink = s.sink;
  }
  if (sink) {
    sink(level, comp, message);
  } else {
    std::fprintf(stderr, "egrpc: %s: [%s] %s\n", LogLevelName(level), comp, message.c_str());
  }
}

const char* LogLevelName(LogLevel level) {
  switch (level) {
    case LogLevel::kDebug:
      return "DEBUG";
    case LogLevel::kInfo:
      return "INFO";
    case LogLevel::kWarn:
      return "WARN";
    case LogLevel::kError:
      return "ERROR";
    case LogLevel::kOff:
      return "OFF";
  }
  return "UNKNOWN";
}

}  // namespace egrpc
