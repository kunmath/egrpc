// egrpc — reconnect backoff state machine (design §5.3).
//
// gRPC's connection-backoff algorithm with two deliberate deltas:
//   - MIN_CONNECT_TIMEOUT is unified with connect_timeout: each attempt gets
//     max(connect_timeout, current_backoff) before being declared failed.
//   - READY-stability reset: backoff resets to initial only after a connection
//     has held READY for ready_stability_threshold, not on every connect.
//
// Single-threaded: owned and driven exclusively by the channel's EventThread.
#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <random>

namespace egrpc::internal {

struct BackoffOptions {
  std::chrono::milliseconds initial_backoff{1000};
  double multiplier = 1.6;
  std::chrono::milliseconds max_backoff{120000};
  double jitter = 0.2;
  std::chrono::milliseconds connect_timeout{20000};
  std::chrono::milliseconds ready_stability_threshold{60000};
};

class Backoff {
 public:
  using TimePoint = std::chrono::steady_clock::time_point;

  struct Step {
    std::chrono::milliseconds delay;
    std::chrono::milliseconds attempt_timeout;
  };

  explicit Backoff(BackoffOptions options = {});
  Backoff(BackoffOptions options, uint64_t seed);

  Backoff(const Backoff&) = delete;
  Backoff& operator=(const Backoff&) = delete;
  Backoff(Backoff&&) = default;
  Backoff& operator=(Backoff&&) = default;

  std::chrono::milliseconds CurrentConnectTimeout() const;

  void OnReady(TimePoint now);
  bool CheckAndResetIfStable(TimePoint now);
  Step OnFailure(TimePoint now);
  void Reset();

  uint64_t attempt() const { return attempt_; }
  const BackoffOptions& options() const { return options_; }

 private:
  static BackoffOptions Sanitize(BackoffOptions options);
  static uint64_t MakeSeed(const void* salt);
  bool IsStable(TimePoint now) const;

  BackoffOptions options_;
  std::mt19937_64 rng_;
  uint64_t attempt_ = 0;
  double current_base_ms_ = 0.0;
  std::chrono::milliseconds last_base_backoff_{0};
  std::optional<TimePoint> ready_since_;
};

}  // namespace egrpc::internal
