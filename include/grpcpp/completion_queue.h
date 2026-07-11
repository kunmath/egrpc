// egrpc grpcpp shim — grpc::CompletionQueue (design §4.5). Async CQ calls
// are permanently out of scope (design §2), but "UNIMPLEMENTED at runtime"
// must be observable through the CQ contract: every tagged async operation
// enqueues its tag here at the moment it is started (client-side Finish
// tags with ok=true after depositing an UNIMPLEMENTED status, everything
// else with ok=false, matching upstream's failed-operation semantics), and
// Next() hands the tags back. An empty queue reports shutdown instead of
// blocking — no started operations means nothing will ever arrive, so a
// drain loop terminates rather than hangs.
#pragma once

#include <deque>
#include <mutex>
#include <utility>

namespace grpc {

class CompletionQueue {
 public:
  enum NextStatus { SHUTDOWN, GOT_EVENT, TIMEOUT };

  virtual ~CompletionQueue() = default;

  bool Next(void** tag, bool* ok) {
    std::lock_guard<std::mutex> lock(mu_);
    if (queue_.empty()) return false;
    *tag = queue_.front().first;
    *ok = queue_.front().second;
    queue_.pop_front();
    return true;
  }

  template <class T>
  NextStatus AsyncNext(void** tag, bool* ok, const T& deadline) {
    (void)deadline;
    return Next(tag, ok) ? GOT_EVENT : SHUTDOWN;
  }

  void Shutdown() {}

  // --- egrpc shim internal --------------------------------------------------
  // Called by the async stubs (async_unary_call.h, async_stream.h) when a
  // tagged operation is started. Locked: the CQ may be shared across
  // caller threads, like upstream.
  void EgrpcEnqueueTag(void* tag, bool ok) {
    std::lock_guard<std::mutex> lock(mu_);
    queue_.emplace_back(tag, ok);
  }

 private:
  std::mutex mu_;
  std::deque<std::pair<void*, bool>> queue_;
};

class ServerCompletionQueue : public CompletionQueue {};

}  // namespace grpc
