// egrpc — per-RPC call state machine and completion block (design §4.3, §5.7).
//
// Ownership split (§5.7):
//   - CallCompletion (shared_ptr): shared between the caller/ClientReader and
//     the EventThread. Holds ZERO transport references — only the mutex +
//     condvar, sticky cancel token, initial metadata, streaming delivery queue,
//     and terminal Result. A ClientReader can safely outlive ChannelImpl.
//   - CallState (unique_ptr): owned EXCLUSIVELY by the EventThread (design §3).
//     Holds protocol state (stream_id, GrpcMessageScanner, header/trailer
//     parsing, deadline timer id, draining state). All transitions happen on
//     the EventThread; no mutex on CallState itself.
//
// State machine:
//   CREATED → HEADERS_SENT → OPEN → [DRAINING] → CLOSED
//
// DRAINING (server-streaming only): HTTP/2 stream closed cleanly on the wire
// while complete messages still sit in the reassembly scanner waiting for
// caller Read()s to free delivery-queue slots. The stream slot in Http2Session
// is already released; CallState finishes draining into CallCompletion on
// ReadConsumed notifications and then closes.
#pragma once

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "core/grpc_wire.h"
#include "transport/http2_session.h"

namespace egrpc {
namespace internal {

// Builds the request HEADERS for a call (design §5.1): :method POST,
// :scheme https, :authority, :path, te: trailers, content-type,
// grpc-accept-encoding: identity, user-agent, grpc-timeout when a timeout
// is given, then the caller's metadata. Pseudo-headers come first as HTTP/2
// requires. Metadata keys are lowercased here and values of `-bin` keys are
// base64-encoded without padding (§5.1), so callers pass them raw; keys the
// transport owns (pseudo-headers, te, content-type, grpc-timeout, ...) are
// dropped rather than sent as duplicates. A non-empty `user_agent_prefix`
// is prepended to the built-in user-agent, space-separated, matching
// upstream's primary_user_agent semantics.
Http2Session::HeaderList BuildRequestHeaders(const std::string& authority,
                                             const std::string& method_path,
                                             std::optional<std::chrono::nanoseconds> timeout,
                                             const Http2Session::HeaderList& metadata = {},
                                             const std::string& user_agent_prefix = {});

enum class CallMode { kUnary, kServerStreaming };

// Where a call's terminal status came from (design §4.6: resilient_stream
// must tell a locally-originated CANCELLED from a server-originated one).
enum class StatusOrigin {
  kServer,  // server trailers (grpc-status), HTTP status mapping, or server RST_STREAM
  kLocal,   // local TryCancel, local deadline, framing violation, transport teardown, shutdown
};

// Caller-shared completion block (design §5.7). Holds no transport references;
// every member is synchronized by mu_.
class CallCompletion {
 public:
  struct Result {
    StatusCode code = StatusCode::kUnknown;
    std::string message;  // grpc-message, percent-decoded
    // Serialized response payload (unary only; valid when code == kOk).
    std::string response;
    // Response metadata, raw as received (pseudo-headers and
    // grpc-status/grpc-message stripped). For Trailers-Only responses all
    // metadata is trailing, matching upstream.
    Http2Session::HeaderList initial_metadata;
    Http2Session::HeaderList trailing_metadata;
    StatusOrigin origin = StatusOrigin::kServer;
  };

  struct QueuedMessage {
    std::string payload;
    size_t wire_bytes = 0;  // 5-byte gRPC prefix + payload.size()
  };

  explicit CallCompletion(uint64_t call_id = 0) : call_id_(call_id) {}
  CallCompletion(const CallCompletion&) = delete;
  CallCompletion& operator=(const CallCompletion&) = delete;

  uint64_t call_id() const noexcept { return call_id_.load(std::memory_order_acquire); }
  void set_call_id(uint64_t call_id) noexcept {
    call_id_.store(call_id, std::memory_order_release);
  }

  // --- Cancellation sticky token (any thread, §5.7) -------------------------
  // Marks cancellation requested. Returns true if this call flipped the flag
  // from false to true.
  bool RequestCancel() noexcept;
  bool cancel_requested() const;

  // --- Caller thread methods ------------------------------------------------
  // Blocks until initial response headers arrive or the call reaches CLOSED
  // (e.g. Trailers-Only or local failure).
  void WaitForInitialMetadata();
  // Snapshot of initial metadata (call after WaitForInitialMetadata/PopMessage/Wait).
  Http2Session::HeaderList InitialMetadataCopy() const;

  // Streaming: blocks until a message is queued or the call is CLOSED.
  // Returns true with the next message moved into *payload and its framed wire
  // length in *wire_bytes, or false once the queue is drained and done_ is set.
  bool PopMessage(std::string* payload, size_t* wire_bytes);

  // Blocks until the call reaches CLOSED. The returned reference stays valid
  // for the CallCompletion's lifetime and is immutable once returned.
  const Result& Wait();

  // True once the call has reached CLOSED (may still have queued messages to
  // drain in streaming mode unless DiscardQueued was called).
  bool is_done() const;

  // Discards any unread queued streaming messages.
  void DiscardQueued();

  // --- EventThread methods (called by CallState / ChannelImpl) --------------
  // Publishes initial response metadata and wakes WaitForInitialMetadata().
  void DeliverInitialMetadata(Http2Session::HeaderList metadata);

  // True if the delivery queue can accept a message of `next_wire_bytes` under
  // §5.4 bounds (queued_bytes + next_wire_bytes <= max_queued_bytes). Always
  // allows pushing into an empty queue so a single max-size message can never
  // deadlock against max_queued_bytes.
  bool CanAcceptMessage(size_t next_wire_bytes, size_t max_queued_messages,
                        size_t max_queued_bytes) const;

  // Enqueues one decoded streaming message and wakes Read()/NextMessageSize().
  // Returns {queue_size, queued_bytes} after the push for high-water tracking.
  std::pair<size_t, size_t> PushMessage(std::string payload, size_t wire_bytes);

  // Transitions to CLOSED (idempotent), optionally clearing unread queued
  // messages (true for cancel / deadline / transport teardown / shutdown), and
  // wakes all waiters.
  void Close(Result result, bool discard_queued);

 private:
  friend class ChannelImpl;

  std::atomic<uint64_t> call_id_{0};
  // Intrusive no-alloc cancel queue fields guarded by ChannelImpl::cancel_mu_.
  // cancel_enqueued_ flips false -> true at most once (idempotent cancel).
  bool cancel_enqueued_ = false;
  std::shared_ptr<CallCompletion> next_cancel_;

  mutable std::mutex mu_;
  std::condition_variable cv_;
  bool cancel_requested_ = false;
  bool initial_metadata_ready_ = false;
  Http2Session::HeaderList initial_metadata_;
  std::deque<QueuedMessage> queue_;
  size_t queued_bytes_ = 0;
  bool done_ = false;
  Result result_;
};

// EventThread-owned protocol side of a single RPC (design §4.3, §5.7).
class CallState {
 public:
  using Result = CallCompletion::Result;

  struct Hooks {
    // Called on the EventThread after Feed() with scanner_.buffered_bytes().
    std::function<void(size_t reassembly_bytes)> on_reassembly_bytes;
    // Called on the EventThread after pushing to the streaming delivery queue.
    std::function<void(size_t queued_messages, size_t queued_bytes)> on_queue_depth;
    // Called on the EventThread when Feed() detects a framing violation so the
    // channel can queue RST_STREAM(CANCEL) once the nghttp2 callback unwinds.
    std::function<void()> on_protocol_error;
  };

  explicit CallState(size_t max_receive_message_size, CallMode mode = CallMode::kUnary,
                     std::shared_ptr<CallCompletion> completion = nullptr,
                     size_t max_queued_messages = 16,
                     size_t max_queued_bytes = 4 * 1024 * 1024 + 5 + 64 * 1024, Hooks hooks = {});

  CallState(const CallState&) = delete;
  CallState& operator=(const CallState&) = delete;

  // --- EventThread only (design §3) ----------------------------------------

  // CREATED → HEADERS_SENT (unary and server-streaming both half-close local
  // immediately after their single request message).
  void OnSubmitted(int32_t stream_id);
  // Initial response HEADERS; end_stream=true is a Trailers-Only response.
  void OnResponseHeaders(Http2Session::HeaderList headers, bool end_stream);
  // DATA chunk → reassembly scanner. Returns the number of bytes deliberately
  // ignored (e.g. non-200 or non-gRPC HTTP error body) so manual-flow streams
  // can immediately return stream flow-control credit and drain to END_STREAM.
  // A framing violation fails the call immediately and invokes
  // hooks_.on_protocol_error (ChannelImpl resets the stream once nghttp2
  // returns).
  size_t OnData(const uint8_t* data, size_t len);
  void OnTrailers(Http2Session::HeaderList trailers);
  // Stream closed on the wire (fires once per stream). For streaming calls
  // with complete messages still waiting in the scanner behind a full delivery
  // queue, transitions to kDraining instead of closing immediately.
  void OnClose(uint32_t http2_error_code);
  // Moves as many complete messages as fit from scanner_ into completion_'s
  // delivery queue. If wire_closed_ and scanner_ is drained, finalizes and
  // closes the call.
  void DrainScannerToQueue();
  // Discards unread queued/scanner messages on a kDraining call and finalizes
  // using the wire status already received from the server (e.g. caller
  // invoked Finish() or TryCancel() after the HTTP/2 stream already closed).
  void FinalizeDiscarding();
  // Local/transport failure (connect error, deadline, cancel, teardown,
  // shutdown): discards unread queued messages and closes the call unless
  // already closed. Idempotent.
  void FailLocal(StatusCode code, std::string message);
  // Checks whether local cancellation or an elapsed deadline preempts another
  // terminal event (e.g. framing error, peer reset, or transport teardown) on
  // a live call. If preempted, closes the call with kCancelled or
  // kDeadlineExceeded and returns true. Never overwrites an already-closed or
  // wire-complete draining call.
  bool TryPreemptTerminal();

  // Convenience wrapper for single-threaded unit tests.
  const Result& Wait() { return completion_->Wait(); }

  const std::shared_ptr<CallCompletion>& completion() const { return completion_; }
  CallMode mode() const { return mode_; }
  int32_t stream_id() const { return stream_id_; }
  bool is_draining() const { return state_ == State::kDraining; }
  bool is_closed() const { return state_ == State::kClosed; }
  uint64_t deadline_timer() const { return deadline_timer_; }
  void set_deadline_timer(uint64_t timer_id) { deadline_timer_ = timer_id; }
  const std::optional<std::chrono::steady_clock::time_point>& deadline() const { return deadline_; }
  void set_deadline(std::optional<std::chrono::steady_clock::time_point> deadline) {
    deadline_ = deadline;
  }

 private:
  enum class State { kCreated, kHeadersSent, kOpen, kDraining, kClosed };

  // Computes the final status per §5.5 and closes completion_.
  void FinalizeAndClose(uint32_t http2_error_code, bool discard_queued);

  const CallMode mode_;
  const size_t max_queued_messages_;
  const size_t max_queued_bytes_;
  Hooks hooks_;
  std::shared_ptr<CallCompletion> completion_;

  State state_ = State::kCreated;
  int32_t stream_id_ = -1;
  uint64_t deadline_timer_ = 0;
  std::optional<std::chrono::steady_clock::time_point> deadline_;

  GrpcMessageScanner scanner_;
  // Unary: first message kept, the rest only counted (bounds memory against
  // a misbehaving server; >1 is an error at finalize).
  std::string first_response_;
  size_t response_count_ = 0;

  bool saw_initial_headers_ = false;
  int http_status_ = 0;  // :status (0 = never seen)
  std::string content_type_;
  std::optional<std::string> grpc_status_raw_;
  std::string grpc_message_raw_;
  Http2Session::HeaderList trailing_metadata_;

  bool wire_closed_ = false;
  uint32_t wire_close_error_code_ = 0;
};

}  // namespace internal
}  // namespace egrpc
