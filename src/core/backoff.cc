// egrpc — reconnect backoff state machine (design §5.3).
#include "core/backoff.h"

#include <algorithm>
#include <cmath>
#include <cstdint>

namespace egrpc::internal {

namespace {

// Largest double safely convertible to int64 milliseconds via llround.
constexpr double kMaxMs = 9.0e18;

std::chrono::milliseconds ToMs(double ms) {
  if (!(ms > 0.0)) return std::chrono::milliseconds(0);  // also catches NaN
  if (ms > kMaxMs) ms = kMaxMs;
  return std::chrono::milliseconds(std::llround(ms));
}

uint64_t SplitMix64(uint64_t x) {
  x += 0x9e3779b97f4a7c15ULL;
  x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
  x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
  return x ^ (x >> 31);
}

}  // namespace

Backoff::Backoff(BackoffOptions options)
    : Backoff(options, MakeSeed(reinterpret_cast<uintptr_t>(this))) {}

Backoff::Backoff(BackoffOptions options, uint64_t seed) : options_(Sanitize(options)), rng_(seed) {
  Reset();
}

BackoffOptions Backoff::Sanitize(BackoffOptions o) {
  using std::chrono::milliseconds;
  o.initial_backoff = std::max(o.initial_backoff, milliseconds(0));
  o.max_backoff = std::max(o.max_backoff, o.initial_backoff);
  if (!(o.multiplier >= 1.0) || !std::isfinite(o.multiplier)) o.multiplier = 1.0;
  if (!(o.jitter >= 0.0)) o.jitter = 0.0;  // also catches NaN
  o.jitter = std::min(o.jitter, 1.0);
  o.connect_timeout = std::max(o.connect_timeout, milliseconds(0));
  o.ready_stability_threshold = std::max(o.ready_stability_threshold, milliseconds(0));
  return o;
}

uint64_t Backoff::MakeSeed(uintptr_t salt) {
  uint64_t seed = 0;
  try {
    std::random_device rd;
    seed = (static_cast<uint64_t>(rd()) << 32) ^ static_cast<uint64_t>(rd());
  } catch (...) {
    // random_device may be unavailable; fall back to clock/address mixing.
  }
  seed ^= SplitMix64(
      static_cast<uint64_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
  seed ^= SplitMix64(static_cast<uint64_t>(salt));
  return SplitMix64(seed);
}

// §5.3 / gRPC connection-backoff.md: every attempt, including the first,
// gets max(MIN_CONNECT_TIMEOUT ≡ connect_timeout, current_backoff).
std::chrono::milliseconds Backoff::CurrentConnectTimeout() const {
  return std::max(options_.connect_timeout,
                  attempt_ == 0 ? options_.initial_backoff : last_base_backoff_);
}

void Backoff::OnReady(TimePoint now) { ready_since_ = now; }

bool Backoff::IsStable(TimePoint now) const {
  return ready_since_.has_value() && (now - *ready_since_) >= options_.ready_stability_threshold;
}

bool Backoff::CheckAndResetIfStable(TimePoint now) {
  if (!IsStable(now)) return false;
  Reset();
  return true;
}

Backoff::Step Backoff::OnFailure(TimePoint now) {
  if (IsStable(now)) Reset();
  ready_since_.reset();

  const double max_ms = static_cast<double>(options_.max_backoff.count());
  double base_ms = static_cast<double>(options_.initial_backoff.count());
  if (attempt_ != 0) {
    const double next = current_base_ms_ * options_.multiplier;
    base_ms = std::isfinite(next) ? std::min(max_ms, next) : max_ms;
  }
  base_ms = std::clamp(base_ms, 0.0, max_ms);
  current_base_ms_ = base_ms;
  last_base_backoff_ = std::min(ToMs(base_ms), options_.max_backoff);
  ++attempt_;

  double factor = 1.0;
  if (options_.jitter > 0.0) {
    std::uniform_real_distribution<double> dist(-options_.jitter, options_.jitter);
    factor += dist(rng_);
  }
  return Step{ToMs(base_ms * factor), CurrentConnectTimeout()};
}

void Backoff::Reset() {
  attempt_ = 0;
  last_base_backoff_ = std::chrono::milliseconds(0);
  current_base_ms_ = static_cast<double>(options_.initial_backoff.count());
  ready_since_.reset();
}

}  // namespace egrpc::internal
