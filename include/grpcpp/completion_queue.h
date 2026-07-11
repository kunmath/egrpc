// egrpc grpcpp shim — grpc::CompletionQueue (design §4.5). Async CQ calls
// are permanently out of scope (design §2), but "UNIMPLEMENTED at runtime"
// must be observable through the CQ contract: every tagged async operation
// enqueues its tag here at the moment it is started (client-side Finish
// tags with ok=true after depositing an UNIMPLEMENTED status, everything
// else with ok=false, matching upstream's failed-operation semantics).
//
// Next() has upstream blocking semantics: it waits for a tag or for
// Shutdown(), and reports false only once the queue is shut down AND
// drained — a momentarily-empty queue racing a producer thread is not a
// false shutdown. As upstream requires, a CQ must be Shutdown() and
// drained before destruction.
#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <utility>

namespace grpc {

class CompletionQueue {
 public:
  enum NextStatus { SHUTDOWN, GOT_EVENT, TIMEOUT };

  virtual ~CompletionQueue() = default;

  bool Next(void** tag, bool* ok) {
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [this] { return !queue_.empty() || shutdown_; });
    if (queue_.empty()) return false;  // shut down and drained
    *tag = queue_.front().first;
    *ok = queue_.front().second;
    queue_.pop_front();
    return true;
  }

  // The deadline type is opaque here (upstream converts via TimePoint
  // traits egrpc doesn't ship), so AsyncNext never blocks: an empty live
  // queue reports TIMEOUT as if the deadline were "now".
  template <class T>
  NextStatus AsyncNext(void** tag, bool* ok, const T& deadline) {
    (void)deadline;
    std::unique_lock<std::mutex> lock(mu_);
    if (!queue_.empty()) {
      *tag = queue_.front().first;
      *ok = queue_.front().second;
      queue_.pop_front();
      return GOT_EVENT;
    }
    return shutdown_ ? SHUTDOWN : TIMEOUT;
  }

  void Shutdown() {
    std::lock_guard<std::mutex> lock(mu_);
    shutdown_ = true;
    cv_.notify_all();
  }

  // --- egrpc shim internal --------------------------------------------------
  // Called by the async stubs (async_unary_call.h, async_stream.h) when a
  // tagged operation is started. The CQ may be shared across caller
  // threads, like upstream.
  void EgrpcEnqueueTag(void* tag, bool ok) {
    std::lock_guard<std::mutex> lock(mu_);
    queue_.emplace_back(tag, ok);
    cv_.notify_one();
  }

 private:
  std::mutex mu_;
  std::condition_variable cv_;
  bool shutdown_ = false;
  std::deque<std::pair<void*, bool>> queue_;
};

class ServerCompletionQueue : public CompletionQueue {};

}  // namespace grpc
