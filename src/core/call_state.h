// egrpc — per-RPC call state machine (design §4.3).
//
//   CREATED → HEADERS_SENT → OPEN → CLOSED
//                    (trailers received / RST / transport failure)
//
// Threading (design §3): the EventThread performs ALL state transitions via
// the On*/Fail methods (wired to Http2Session::StreamHooks); caller threads
// interact only through Wait(), blocking on the per-call mutex + condvar
// until the EventThread signals completion. The internal mutex makes the
// captured result safely readable from the caller afterwards.
//
// M3 scope: unary calls. The receive path is already the general
// length-prefix scanner (GrpcMessageScanner); M5 extends this class with
// per-Read delivery for server streaming.
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
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

class CallState {
 public:
  struct Result {
    StatusCode code = StatusCode::kUnknown;
    std::string message;  // grpc-message, percent-decoded
    // Serialized response payload (valid when code == kOk).
    std::string response;
    // Response metadata, raw as received (pseudo-headers and
    // grpc-status/grpc-message stripped). For Trailers-Only responses all
    // metadata is trailing, matching upstream.
    Http2Session::HeaderList initial_metadata;
    Http2Session::HeaderList trailing_metadata;
  };

  explicit CallState(size_t max_receive_message_size);

  CallState(const CallState&) = delete;
  CallState& operator=(const CallState&) = delete;

  // Canonical wiring of a CallState to an Http2Session stream. The hooks
  // hold a shared_ptr so an abandoned call outlives its waiter safely.
  static Http2Session::StreamHooks MakeStreamHooks(std::shared_ptr<CallState> call);

  // --- EventThread only (design §3) ----------------------------------------

  // CREATED → HEADERS_SENT (unary: half-closed local immediately, §4.3).
  void OnSubmitted(int32_t stream_id);
  // Initial response HEADERS; end_stream=true is a Trailers-Only response.
  void OnResponseHeaders(Http2Session::HeaderList headers, bool end_stream);
  // DATA chunk → reassembly scanner. A scanner protocol error records the
  // failure; the final status surfaces it at OnClose.
  void OnData(const uint8_t* data, size_t len);
  void OnTrailers(Http2Session::HeaderList trailers);
  // Stream closed (fires exactly once per stream). Computes the final
  // status per §5.5 and signals the waiter. http2_error_code 0 = NO_ERROR.
  void OnClose(uint32_t http2_error_code);
  // Local/transport failure (connect error, session teardown, shutdown):
  // closes the call with `code` unless it already completed. Idempotent.
  void FailLocal(StatusCode code, std::string message);

  // --- Caller threads --------------------------------------------------------

  // Blocks until the call reaches CLOSED. The result reference stays valid
  // for the CallState's lifetime and is immutable once returned.
  const Result& Wait();

  int32_t stream_id() const;

 private:
  enum class State { kCreated, kHeadersSent, kOpen, kClosed };

  // Computes the final status per §5.5 (caller holds mu_, state != kClosed):
  // scanner protocol error > grpc-status (wins when present) > HTTP-status
  // mapping > RST error code > missing grpc-status on clean close → kUnknown
  // (upstream's 200 → UNKNOWN fallback). On kOk enforces exactly one
  // response message (unary).
  void FinalizeLocked(uint32_t http2_error_code);
  // → CLOSED; wakes the waiter. Caller holds mu_.
  void CloseLocked();

  mutable std::mutex mu_;
  std::condition_variable cv_;
  bool done_ = false;

  State state_ = State::kCreated;
  int32_t stream_id_ = -1;

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

  Result result_;
};

}  // namespace internal
}  // namespace egrpc
