// egrpc native API — resilient server-streaming helper (design §4.6).
//
// Pure Layer 3: built only on the public grpcpp shim surface. Every user
// callback runs on the caller's thread inside Read(), never on a channel's
// EventThread.
#pragma once

#include <egrpc/log.h>
#include <grpcpp/client_context.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

#include <algorithm>
#include <atomic>
#include <cassert>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <random>
#include <string>
#include <thread>
#include <utility>

namespace egrpc {

// Default retry classification (§4.6). CANCELLED retries only when it came
// from the server; a local cancel (TryCancel/stop) is terminal.
inline bool DefaultShouldRetry(const grpc::Status& status, bool status_is_local) {
  switch (status.error_code()) {
    case grpc::StatusCode::UNAVAILABLE:
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
    case grpc::StatusCode::ABORTED:
    case grpc::StatusCode::INTERNAL:
    case grpc::StatusCode::UNKNOWN:
    case grpc::StatusCode::DEADLINE_EXCEEDED:
      return true;
    case grpc::StatusCode::CANCELLED:
      return !status_is_local;
    default:
      return false;
  }
}

struct ResilientStreamOptions {
  // Invoked on the Read() caller thread when a stream is re-established
  // (initial metadata received after a previous establishment).
  std::function<void()> on_reconnected;
  bool fire_on_first_connect = false;
  // Overrides DefaultShouldRetry when set. stop() always terminates.
  std::function<bool(const grpc::Status& status, bool status_is_local)> should_retry;
  std::chrono::milliseconds initial_retry_delay{100};
  double retry_backoff_multiplier = 1.6;
  std::chrono::milliseconds max_retry_delay{30000};
  double retry_backoff_jitter = 0.2;
  // A stream that stayed established at least this long resets the backoff.
  std::chrono::milliseconds retry_reset_threshold{60000};
};

namespace detail {

// Exponential retry backoff with jitter for resilient_stream (mirrors
// src/core/backoff.cc). Inputs must already be sanitized.
class RetryBackoff {
 public:
  void Configure(std::chrono::milliseconds initial, double multiplier,
                 std::chrono::milliseconds max, double jitter) {
    initial_ms_ = static_cast<double>(initial.count());
    max_ms_ = static_cast<double>(max.count());
    multiplier_ = multiplier;
    jitter_ = jitter;
    Reset();
  }

  void Reset() {
    current_base_ms_ = initial_ms_;
    first_ = true;
  }

  // Unjittered base of the most recent Next().
  std::chrono::milliseconds last_base() const { return ToMs(current_base_ms_); }

  std::chrono::milliseconds Next() {
    if (first_) {
      first_ = false;
    } else {
      current_base_ms_ = std::min(max_ms_, current_base_ms_ * multiplier_);
    }
    double delay = current_base_ms_;
    if (jitter_ > 0.0 && delay > 0.0) {
      std::uniform_real_distribution<double> dist(-jitter_, jitter_);
      delay *= 1.0 + dist(rng_);
    }
    return ToMs(std::min(max_ms_, delay));
  }

 private:
  static std::chrono::milliseconds ToMs(double ms) {
    constexpr double kMaxMs = 9.0e18;
    if (!(ms > 0.0)) return std::chrono::milliseconds(0);
    if (ms > kMaxMs) ms = kMaxMs;
    return std::chrono::milliseconds(std::llround(ms));
  }

  double initial_ms_ = 0.0;
  double max_ms_ = 0.0;
  double multiplier_ = 1.0;
  double jitter_ = 0.0;
  double current_base_ms_ = 0.0;
  bool first_ = true;
  static std::minstd_rand::result_type MakeSeed() {
    try {
      std::random_device rd;
      return rd();
    } catch (...) {
      // No working random_device (some embedded targets): fall back to time.
      return static_cast<std::minstd_rand::result_type>(
          std::chrono::steady_clock::now().time_since_epoch().count());
    }
  }

  std::minstd_rand rng_{MakeSeed()};
};

}  // namespace detail

// Re-establishes a server stream on retryable failures.
//
// The factory is called once per attempt with a FRESH ClientContext
// (wait_for_ready set) and must start the RPC on it. A deadline set on that
// ClientContext in the factory applies to THIS ATTEMPT ONLY, never as an
// end-to-end deadline across retries. The factory must not call Read() or
// stop() on the same resilient_stream.
//
// Destroying a resilient_stream concurrently with Read() is undefined; call
// stop() and join the reader thread first.
template <class R>
class resilient_stream {
 public:
  using Factory =
      std::function<std::unique_ptr<grpc::ClientReaderInterface<R>>(grpc::ClientContext&)>;

  resilient_stream(ResilientStreamOptions options, Factory factory)
      : options_(std::move(options)), factory_(std::move(factory)) {
    Sanitize();
    backoff_.Configure(options_.initial_retry_delay, options_.retry_backoff_multiplier,
                       options_.max_retry_delay, options_.retry_backoff_jitter);
  }
  explicit resilient_stream(Factory factory)
      : resilient_stream(ResilientStreamOptions(), std::move(factory)) {}

  resilient_stream(const resilient_stream&) = delete;
  resilient_stream& operator=(const resilient_stream&) = delete;
  resilient_stream(resilient_stream&&) = delete;
  resilient_stream& operator=(resilient_stream&&) = delete;

  ~resilient_stream() {
    stop();
    active_reader_.reset();
    {
      std::lock_guard<std::mutex> lock(mu_);
      active_ctx_.reset();
    }
    reader_ctx_.reset();
  }

  // Single-reader. Returns false once the stream terminated; see status().
  // Calling Read() or stop() from inside the factory, on_reconnected or
  // should_retry is undefined behavior (asserted in debug builds).
  bool Read(R* msg) {
    assert(!IsCallbackThread() && "resilient_stream::Read() called from on_reconnected");
    const bool was_in_read = in_read_.exchange(true);
    assert(!was_in_read && "resilient_stream::Read() called concurrently");
    (void)was_in_read;
    struct InReadGuard {
      std::atomic<bool>& flag;
      ~InReadGuard() { flag.store(false); }
    } in_read_guard{in_read_};

    if (finished_) return false;

    for (;;) {
      if (!active_reader_) {
        if (IsStopped()) return FinishWith(Cancelled());
        auto ctx = std::make_shared<grpc::ClientContext>();
        ctx->set_wait_for_ready(true);
        {
          std::lock_guard<std::mutex> lock(mu_);
          if (stopped_) {
            terminal_status_ = Cancelled();
            finished_ = true;
            return false;
          }
          // The factory is a registered callback: stop() waits for it.
          active_ctx_ = ctx;
          in_callback_ = true;
          callback_thread_id_ = std::this_thread::get_id();
        }
        reader_ctx_ = ctx;
        {
          FactoryGuard guard{this};
          {
            CallbackGuard cb_guard{this};
            if (factory_) active_reader_ = factory_(*ctx);
          }
          guard.armed = false;
        }
        if (!active_reader_) {
          ClearContexts();
          return FinishWith(grpc::Status(grpc::StatusCode::INTERNAL,
                                         "resilient_stream factory returned null reader"));
        }
        if (IsStopped()) {
          AbandonActive();
          return FinishWith(Cancelled());
        }

        active_reader_->WaitForInitialMetadata();
        if (reader_ctx_->egrpc_initial_metadata_received()) {
          attempt_established_at_ = std::chrono::steady_clock::now();
          has_established_at_ = true;
          const bool is_reconnect = has_connected_once_;
          has_connected_once_ = true;
          if (options_.on_reconnected && (is_reconnect || options_.fire_on_first_connect)) {
            RunCallback([this] { options_.on_reconnected(); });
            if (IsStopped()) {
              AbandonActive();
              return FinishWith(Cancelled());
            }
          }
        }
      }

      if (IsStopped()) {
        AbandonActive();
        return FinishWith(Cancelled());
      }
      if (reader_ctx_->egrpc_initial_metadata_received() && active_reader_->Read(msg)) {
        return true;
      }

      const grpc::Status s = active_reader_->Finish();
      const bool is_local = reader_ctx_->egrpc_status_is_local();
      active_reader_.reset();
      ClearContexts();

      if (IsStopped()) {
        return FinishWith(s.error_code() == grpc::StatusCode::CANCELLED ? s : Cancelled());
      }
      bool retry = false;
      if (options_.should_retry) {
        RunCallback([&] { retry = options_.should_retry(s, is_local); });
        if (IsStopped()) return FinishWith(Cancelled());
      } else {
        retry = DefaultShouldRetry(s, is_local);
      }
      if (!retry) return FinishWith(s);

      if (has_established_at_) {
        if (std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() -
                                                                  attempt_established_at_) >=
            options_.retry_reset_threshold) {
          backoff_.Reset();
        }
        has_established_at_ = false;
      }
      const std::chrono::milliseconds delay = backoff_.Next();
      Log(LogLevel::kInfo, "resilient_stream",
          "stream ended (code " + std::to_string(static_cast<int>(s.error_code())) + ": " +
              s.error_message() + "); retrying in " + std::to_string(delay.count()) + " ms");
      if (delay.count() > 0) {
        std::unique_lock<std::mutex> lock(mu_);
        const auto now = std::chrono::steady_clock::now();
        const auto headroom = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::time_point::max() - now);
        bool woken_by_stop;
        if (delay >= headroom) {
          cv_.wait(lock, [this] { return stopped_; });
          woken_by_stop = true;
        } else {
          woken_by_stop = cv_.wait_until(lock, now + delay, [this] { return stopped_; });
        }
        if (woken_by_stop) {
          terminal_status_ = Cancelled();
          finished_ = true;
          return false;
        }
      }
    }
  }

  // Thread-safe, idempotent. Cancels the in-flight attempt, wakes any retry
  // backoff, and blocks until an in-flight factory, on_reconnected or
  // should_retry returns; none runs after stop() returns. Must not be called
  // from inside those callbacks.
  void stop() {
    assert(!IsCallbackThread() && "resilient_stream::stop() called from on_reconnected");
    std::shared_ptr<grpc::ClientContext> ctx_to_cancel;
    {
      std::lock_guard<std::mutex> lock(mu_);
      stopped_ = true;
      ctx_to_cancel = active_ctx_;
      cv_.notify_all();
    }
    if (ctx_to_cancel) ctx_to_cancel->TryCancel();
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this] { return !in_callback_; });
  }

  // OK until Read() returns false; then the terminal status.
  grpc::Status status() const {
    std::lock_guard<std::mutex> lock(mu_);
    return terminal_status_;
  }

 private:
  struct FactoryGuard {
    resilient_stream* self;
    bool armed = true;
    ~FactoryGuard() {
      if (armed) self->ClearContexts();
    }
  };

  struct CallbackGuard {
    resilient_stream* self;
    ~CallbackGuard() {
      std::lock_guard<std::mutex> lock(self->mu_);
      self->in_callback_ = false;
      self->callback_thread_id_ = std::thread::id();
      self->cv_.notify_all();
    }
  };

  // Runs a user callback unless stopped, registering it so stop() blocks
  // until it returns (even if it throws).
  template <class F>
  void RunCallback(F&& f) {
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (stopped_) return;
      in_callback_ = true;
      callback_thread_id_ = std::this_thread::get_id();
    }
    CallbackGuard cb_guard{this};
    f();
  }

  static grpc::Status Cancelled() {
    return grpc::Status(grpc::StatusCode::CANCELLED, "resilient_stream stopped");
  }

  bool IsStopped() const {
    std::lock_guard<std::mutex> lock(mu_);
    return stopped_;
  }

  bool IsCallbackThread() const {
    std::lock_guard<std::mutex> lock(mu_);
    return in_callback_ && callback_thread_id_ == std::this_thread::get_id();
  }

  bool FinishWith(grpc::Status s) {
    std::lock_guard<std::mutex> lock(mu_);
    // Checked under the publishing lock so stop() always wins with CANCELLED.
    if (stopped_ && s.error_code() != grpc::StatusCode::CANCELLED) s = Cancelled();
    terminal_status_ = std::move(s);
    finished_ = true;
    return false;
  }

  void ClearContexts() {
    {
      std::lock_guard<std::mutex> lock(mu_);
      active_ctx_.reset();
    }
    reader_ctx_.reset();
  }

  void AbandonActive() {
    if (reader_ctx_) reader_ctx_->TryCancel();
    if (active_reader_) active_reader_->Finish().IgnoreError();
    active_reader_.reset();
    ClearContexts();
  }

  void Sanitize() {
    using ms = std::chrono::milliseconds;
    if (options_.initial_retry_delay < ms(0)) options_.initial_retry_delay = ms(0);
    if (options_.max_retry_delay < options_.initial_retry_delay) {
      options_.max_retry_delay = options_.initial_retry_delay;
    }
    if (!std::isfinite(options_.retry_backoff_multiplier) ||
        options_.retry_backoff_multiplier < 1.0) {
      options_.retry_backoff_multiplier = 1.0;
    }
    if (!std::isfinite(options_.retry_backoff_jitter)) options_.retry_backoff_jitter = 0.0;
    options_.retry_backoff_jitter = std::min(1.0, std::max(0.0, options_.retry_backoff_jitter));
    if (options_.retry_reset_threshold < ms(0)) options_.retry_reset_threshold = ms(0);
  }

  ResilientStreamOptions options_;
  Factory factory_;

  // Reader-thread state.
  std::shared_ptr<grpc::ClientContext> reader_ctx_;  // must outlive active_reader_
  std::unique_ptr<grpc::ClientReaderInterface<R>> active_reader_;
  bool finished_ = false;
  bool has_connected_once_ = false;
  bool has_established_at_ = false;
  std::chrono::steady_clock::time_point attempt_established_at_{};
  detail::RetryBackoff backoff_;
  std::atomic<bool> in_read_{false};

  // Shared state guarded by mu_.
  mutable std::mutex mu_;
  std::condition_variable cv_;
  bool stopped_ = false;
  bool in_callback_ = false;
  std::thread::id callback_thread_id_;
  std::shared_ptr<grpc::ClientContext> active_ctx_;
  grpc::Status terminal_status_;
};

}  // namespace egrpc
