// egrpc native logging API (see docs/egrpc-design-and-plan.md §4.6).
#pragma once

#include <functional>
#include <string>

namespace egrpc {

enum class LogLevel { kDebug = 0, kInfo = 1, kWarn = 2, kError = 3, kOff = 4 };

// Sinks may be invoked on any thread, including a channel's EventThread, and
// possibly concurrently: they must be thread-safe and must not block.
// `component` is never null. Sinks must not throw: any exception escaping a
// sink is caught and suppressed (with a stderr note) to protect transport
// threads.
using LogSink =
    std::function<void(LogLevel level, const char* component, const std::string& message)>;

// Installs a process-wide sink. An empty sink restores the default stderr sink.
// Replacement/uninstallation does not wait for in-flight invocations of the
// previous sink (Log() snapshots it): resources it captures must outlive them.
void SetLogSink(LogSink sink);

// Messages below `min_level` are dropped. Default: LogLevel::kWarn.
void SetLogLevel(LogLevel min_level);
LogLevel GetLogLevel();

// Messages at LogLevel::kOff are always dropped.
void Log(LogLevel level, const char* component, const std::string& message);

// Returns "DEBUG", "INFO", "WARN", "ERROR" or "OFF".
const char* LogLevelName(LogLevel level);

}  // namespace egrpc
