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

template <class R>
class resilient_stream {
 public:
  using Factory =
      std::function<std::unique_ptr<grpc::ClientReaderInterface<R>>(grpc::ClientContext&)>;

  resilient_stream(ResilientStreamOptions options, Factory factory)
      : options_(std::move(options)), factory_(std::move(factory)) {
    Sanitize();
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
          active_ctx_ = ctx;
        }
        reader_ctx_ = ctx;
        {
          FactoryGuard guard{this};
          if (factory_) active_reader_ = factory_(*ctx);
          guard.armed = false;
        }
        if (!active_reader_) {
          ClearContexts();
          return FinishWith(grpc::Status(grpc::StatusCode::INTERNAL,
                                         "resilient_stream factory returned null reader"));
        }

        active_reader_->WaitForInitialMetadata();
        if (reader_ctx_->egrpc_initial_metadata_received()) {
          attempt_established_at_ = std::chrono::steady_clock::now();
          has_established_at_ = true;
          const bool is_reconnect = has_connected_once_;
          has_connected_once_ = true;
          if (options_.on_reconnected && (is_reconnect || options_.fire_on_first_connect)) {
            bool fire = false;
            {
              std::lock_guard<std::mutex> lock(mu_);
              if (!stopped_) {
                in_callback_ = true;
                callback_thread_id_ = std::this_thread::get_id();
                fire = true;
              }
            }
            if (fire) {
              CallbackGuard cb_guard{this};
              options_.on_reconnected();
            }
            if (IsStopped()) {
              AbandonActive();
              return FinishWith(Cancelled());
            }
          }
        }
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
      const bool retry = options_.should_retry ? options_.should_retry(s, is_local)
                                               : DefaultShouldRetry(s, is_local);
      if (!retry) return FinishWith(s);

      const std::chrono::milliseconds delay = NextDelay();
      Log(LogLevel::kInfo, "resilient_stream",
          "stream ended (code " + std::to_string(static_cast<int>(s.error_code())) + ": " +
              s.error_message() + "); retrying in " + std::to_string(delay.count()) + " ms");
      if (delay.count() > 0) {
        std::unique_lock<std::mutex> lock(mu_);
        if (cv_.wait_for(lock, delay, [this] { return stopped_; })) {
          terminal_status_ = Cancelled();
          finished_ = true;
          return false;
        }
      }
    }
  }

  // Thread-safe, idempotent. Cancels the in-flight attempt, wakes any retry
  // backoff, and blocks until an in-flight on_reconnected returns. Must not
  // be called from inside on_reconnected.
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

  std::chrono::milliseconds NextDelay() {
    if (has_established_at_) {
      if (std::chrono::steady_clock::now() - attempt_established_at_ >=
          options_.retry_reset_threshold) {
        retry_attempt_ = 0;
      }
      has_established_at_ = false;
    }
    const double initial = static_cast<double>(options_.initial_retry_delay.count());
    const double max = static_cast<double>(options_.max_retry_delay.count());
    double base = initial * std::pow(options_.retry_backoff_multiplier, retry_attempt_);
    if (!std::isfinite(base) || base > max) base = max;
    if (retry_attempt_ < 64) ++retry_attempt_;
    double delay = base;
    if (options_.retry_backoff_jitter > 0.0 && base > 0.0) {
      std::uniform_real_distribution<double> dist(-options_.retry_backoff_jitter,
                                                  options_.retry_backoff_jitter);
      delay = base * (1.0 + dist(rng_));
    }
    delay = std::min(max, std::max(0.0, delay));
    return std::chrono::milliseconds(static_cast<long long>(delay));
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
  int retry_attempt_ = 0;
  std::minstd_rand rng_{std::random_device{}()};
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
