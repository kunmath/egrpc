// egrpc grpcpp shim — grpc::CompletionQueue (design §4.5): compile-present
// only. egrpc has no async completion-queue machinery (design §2, permanently
// out of scope); a queue behaves as if immediately shut down, so misdirected
// async code exits its drain loop instead of hanging.
#pragma once

namespace grpc {

class CompletionQueue {
 public:
  enum NextStatus { SHUTDOWN, GOT_EVENT, TIMEOUT };

  virtual ~CompletionQueue() = default;

  // Shut-down semantics: no event will ever be delivered.
  bool Next(void** tag, bool* ok) {
    (void)tag;
    (void)ok;
    return false;
  }
  template <class T>
  NextStatus AsyncNext(void** tag, bool* ok, const T& deadline) {
    (void)tag;
    (void)ok;
    (void)deadline;
    return SHUTDOWN;
  }
  void Shutdown() {}
};

class ServerCompletionQueue : public CompletionQueue {};

}  // namespace grpc
