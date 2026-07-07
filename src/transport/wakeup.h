// egrpc — cross-thread wakeup primitive (design §2, §3).
//
// Wraps a non-blocking eventfd. Caller threads call Notify() after enqueuing
// an op; the EventThread polls fd() for POLLIN and calls Drain() before
// processing its op queue. Notify() is safe from any thread and never blocks;
// eventfd saturates at UINT64_MAX-1 so repeated notifies coalesce.
#pragma once

#include <cstdint>

namespace egrpc {
namespace internal {

class Wakeup {
 public:
  Wakeup();
  ~Wakeup();

  Wakeup(const Wakeup&) = delete;
  Wakeup& operator=(const Wakeup&) = delete;

  // Negative if construction failed (fd exhaustion); callers must check once
  // at channel setup.
  int fd() const { return fd_; }

  // Any thread. Wakes a poller blocked on fd(). Coalesces with pending
  // notifications; ignores EAGAIN (a wakeup is already pending).
  void Notify();

  // Event thread only. Clears the pending notification count so poll() does
  // not spin.
  void Drain();

 private:
  int fd_ = -1;
};

}  // namespace internal
}  // namespace egrpc
