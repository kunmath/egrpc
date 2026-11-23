// egrpc — per-RPC call state machine (design §4.3, §5.1, §5.5).
#include "core/call_state.h"

#include <cstdlib>
#include <utility>

namespace egrpc {
namespace internal {

namespace {

// Matches the version in CMakeLists; bumped per release (design §5.1).
constexpr char kUserAgent[] = "grpc-c++-egrpc/0.1.0";

// HTTP/2 error codes referenced by the §5.5 mapping table (protocol
// constants; kept local so this file does not include nghttp2 headers).
constexpr uint32_t kHttp2RefusedStream = 0x7;
constexpr uint32_t kHttp2Cancel = 0x8;
constexpr uint32_t kHttp2EnhanceYourCalm = 0xb;
constexpr uint32_t kHttp2InadequateSecurity = 0xc;

bool IsGrpcContentType(const std::string& value) {
  return value.compare(0, 16, "application/grpc") == 0 &&
         (value.size() == 16 || value[16] == '+' || value[16] == ';');
}

std::string AsciiLower(std::string s) {
  for (char& c : s) {
    if (c >= 'A' && c <= 'Z') c = static_cast<char>(c - 'A' + 'a');
  }
  return s;
}

bool IsBinKey(const std::string& key) {
  return key.size() >= 4 && key.compare(key.size() - 4, 4, "-bin") == 0;
}

// Keys the transport owns (§5.1). Caller metadata colliding with these (or
// naming a pseudo-header) would produce a duplicate/misplaced HTTP/2 header
// and fail the whole call opaquely inside nghttp2, so it is dropped here —
// upstream likewise refuses reserved keys rather than sending them.
bool IsReservedMetadataKey(const std::string& key) {
  if (!key.empty() && key[0] == ':') return true;
  static constexpr const char* kReserved[] = {
      "te",           "host",          "content-type",         "user-agent",
      "grpc-timeout", "grpc-encoding", "grpc-accept-encoding",
  };
  for (const char* r : kReserved) {
    if (key == r) return true;
  }
  return false;
}

}  // namespace

Http2Session::HeaderList BuildRequestHeaders(const std::string& authority,
                                             const std::string& method_path,
                                             std::optional<std::chrono::nanoseconds> timeout,
                                             const Http2Session::HeaderList& metadata,
                                             const std::string& user_agent_prefix) {
  const std::string user_agent =
      user_agent_prefix.empty() ? kUserAgent : user_agent_prefix + " " + kUserAgent;
  Http2Session::HeaderList headers = {
      {":method", "POST"},        {":scheme", "https"},
      {":path", method_path},     {":authority", authority},
      {"te", "trailers"},         {"content-type", "application/grpc"},
      {"user-agent", user_agent}, {"grpc-accept-encoding", "identity"},
  };
  if (timeout.has_value()) {
    headers.emplace_back("grpc-timeout", EncodeGrpcTimeout(*timeout));
  }
  for (const auto& kv : metadata) {
    std::string key = AsciiLower(kv.first);
    if (IsReservedMetadataKey(key)) continue;
    std::string value = IsBinKey(key) ? Base64Encode(kv.second) : kv.second;
    headers.emplace_back(std::move(key), std::move(value));
  }
  return headers;
}

// --- CallCompletion (shared with caller/reader, §5.7) ------------------------

bool CallCompletion::RequestCancel() noexcept {
  std::lock_guard<std::mutex> lock(mu_);
  const bool first = !cancel_requested_;
  cancel_requested_ = true;
  return first;
}

bool CallCompletion::cancel_requested() const {
  std::lock_guard<std::mutex> lock(mu_);
  return cancel_requested_;
}

void CallCompletion::WaitForInitialMetadata() {
  std::unique_lock<std::mutex> lock(mu_);
  cv_.wait(lock, [this] { return initial_metadata_ready_ || done_; });
}

Http2Session::HeaderList CallCompletion::InitialMetadataCopy() const {
  std::lock_guard<std::mutex> lock(mu_);
  return initial_metadata_;
}

bool CallCompletion::PopMessage(std::string* payload, size_t* wire_bytes) {
  std::unique_lock<std::mutex> lock(mu_);
  cv_.wait(lock, [this] { return !queue_.empty() || done_; });
  if (queue_.empty()) return false;
  QueuedMessage msg = std::move(queue_.front());
  queue_.pop_front();
  queued_bytes_ -= msg.wire_bytes;
  *payload = std::move(msg.payload);
  if (wire_bytes != nullptr) *wire_bytes = msg.wire_bytes;
  return true;
}

const CallCompletion::Result& CallCompletion::Wait() {
  std::unique_lock<std::mutex> lock(mu_);
  cv_.wait(lock, [this] { return done_; });
  return result_;
}

bool CallCompletion::is_done() const {
  std::lock_guard<std::mutex> lock(mu_);
  return done_;
}

void CallCompletion::DiscardQueued() {
  std::lock_guard<std::mutex> lock(mu_);
  queue_.clear();
  queued_bytes_ = 0;
}

void CallCompletion::DeliverInitialMetadata(Http2Session::HeaderList metadata) {
  std::lock_guard<std::mutex> lock(mu_);
  if (done_ || initial_metadata_ready_) return;
  initial_metadata_ = std::move(metadata);
  initial_metadata_ready_ = true;
  cv_.notify_all();
}

bool CallCompletion::CanAcceptMessage(size_t next_wire_bytes, size_t max_queued_messages,
                                      size_t max_queued_bytes) const {
  std::lock_guard<std::mutex> lock(mu_);
  if (queue_.empty()) return true;
  if (queue_.size() >= max_queued_messages) return false;
  return next_wire_bytes <= max_queued_bytes && queued_bytes_ <= max_queued_bytes - next_wire_bytes;
}

std::pair<size_t, size_t> CallCompletion::PushMessage(std::string payload, size_t wire_bytes) {
  std::lock_guard<std::mutex> lock(mu_);
  if (done_) return {queue_.size(), queued_bytes_};
  queue_.push_back({std::move(payload), wire_bytes});
  queued_bytes_ += wire_bytes;
  const std::pair<size_t, size_t> depth{queue_.size(), queued_bytes_};
  cv_.notify_all();
  return depth;
}

void CallCompletion::Close(Result result, bool discard_queued) {
  std::lock_guard<std::mutex> lock(mu_);
  if (done_) return;
  if (discard_queued) {
    queue_.clear();
    queued_bytes_ = 0;
  }
  result_ = std::move(result);
  if (result_.initial_metadata.empty() && initial_metadata_ready_) {
    result_.initial_metadata = initial_metadata_;
  }
  done_ = true;
  cv_.notify_all();
}

// --- CallState (EventThread-owned protocol side, §4.3, §5.7) -----------------

CallState::CallState(size_t max_receive_message_size, CallMode mode,
                     std::shared_ptr<CallCompletion> completion, size_t max_queued_messages,
                     size_t max_queued_bytes, Hooks hooks)
    : mode_(mode),
      max_queued_messages_(max_queued_messages),
      max_queued_bytes_(max_queued_bytes),
      hooks_(std::move(hooks)),
      completion_(completion ? std::move(completion) : std::make_shared<CallCompletion>()),
      scanner_(max_receive_message_size) {}

void CallState::OnSubmitted(int32_t stream_id) {
  if (state_ != State::kCreated) return;
  stream_id_ = stream_id;
  state_ = State::kHeadersSent;
}

void CallState::OnResponseHeaders(Http2Session::HeaderList headers, bool end_stream) {
  if (state_ == State::kClosed) return;
  state_ = State::kOpen;
  saw_initial_headers_ = true;

  // Trailers-Only (design §4.3): this HEADERS is simultaneously the
  // trailers; all its metadata is trailing, matching upstream.
  Http2Session::HeaderList initial_metadata;
  Http2Session::HeaderList& metadata = end_stream ? trailing_metadata_ : initial_metadata;

  for (auto& h : headers) {
    if (!h.first.empty() && h.first[0] == ':') {
      if (h.first == ":status") http_status_ = std::atoi(h.second.c_str());
      continue;
    }
    if (h.first == "content-type") {
      content_type_ = h.second;
      continue;
    }
    if (end_stream && h.first == "grpc-status") {
      grpc_status_raw_ = std::move(h.second);
      continue;
    }
    if (end_stream && h.first == "grpc-message") {
      grpc_message_raw_ = std::move(h.second);
      continue;
    }
    metadata.emplace_back(std::move(h));
  }
  if (!end_stream) {
    completion_->DeliverInitialMetadata(std::move(initial_metadata));
  }
  // Final status is computed at OnClose, which nghttp2 fires right after an
  // END_STREAM HEADERS — no completion signal here.
}

size_t CallState::OnData(const uint8_t* data, size_t len) {
  if (state_ == State::kClosed || scanner_.failed()) return 0;
  // Non-200 or non-gRPC HTTP responses (e.g. a reverse proxy returning 502 with
  // an HTML error body) do not carry gRPC-framed messages. Ignore their DATA so
  // the raw body does not poison scanner_ and mask the §5.5 HTTP status code,
  // and report the ignored byte count so manual-flow streams return credit and
  // drain to END_STREAM instead of stalling.
  if (saw_initial_headers_ && (http_status_ != 200 || !IsGrpcContentType(content_type_))) {
    return len;
  }
  if (!scanner_.Feed(data, len)) {
    if (hooks_.on_reassembly_bytes) hooks_.on_reassembly_bytes(scanner_.buffered_bytes());
    // Framing violation (compressed flag or oversize message): fail the call
    // immediately rather than waiting for the server to close, discard any
    // queued messages, and tell the channel to send RST_STREAM(CANCEL) once
    // the nghttp2 callback unwinds. Still honor an already-requested local
    // cancel or elapsed deadline first (§5.6, §5.7).
    if (!TryPreemptTerminal()) {
      FinalizeAndClose(/*http2_error_code=*/0, /*discard_queued=*/true);
    }
    if (hooks_.on_protocol_error) hooks_.on_protocol_error();
    return 0;
  }
  if (hooks_.on_reassembly_bytes) hooks_.on_reassembly_bytes(scanner_.buffered_bytes());
  DrainScannerToQueue();
  return 0;
}

void CallState::DrainScannerToQueue() {
  if (state_ == State::kClosed) return;
  std::string message;
  if (mode_ == CallMode::kUnary) {
    while (scanner_.Next(&message)) {
      if (++response_count_ == 1) first_response_ = std::move(message);
    }
    return;
  }
  size_t next_wire_bytes = 0;
  while (scanner_.PeekNextWireBytes(&next_wire_bytes) &&
         completion_->CanAcceptMessage(next_wire_bytes, max_queued_messages_, max_queued_bytes_) &&
         scanner_.Next(&message)) {
    const size_t wire_bytes = 5 + message.size();
    const auto depth = completion_->PushMessage(std::move(message), wire_bytes);
    if (hooks_.on_queue_depth) hooks_.on_queue_depth(depth.first, depth.second);
  }
  if (wire_closed_ && !scanner_.HasCompleteMessage()) {
    FinalizeAndClose(wire_close_error_code_, /*discard_queued=*/false);
  }
}

void CallState::FinalizeDiscarding() {
  if (state_ == State::kClosed) return;
  FinalizeAndClose(wire_close_error_code_, /*discard_queued=*/true);
}

void CallState::OnTrailers(Http2Session::HeaderList trailers) {
  if (state_ == State::kClosed) return;
  for (auto& h : trailers) {
    if (!h.first.empty() && h.first[0] == ':') continue;
    if (h.first == "grpc-status") {
      grpc_status_raw_ = std::move(h.second);
      continue;
    }
    if (h.first == "grpc-message") {
      grpc_message_raw_ = std::move(h.second);
      continue;
    }
    trailing_metadata_.emplace_back(std::move(h));
  }
}

void CallState::OnClose(uint32_t http2_error_code) {
  if (state_ == State::kClosed) return;
  wire_closed_ = true;
  wire_close_error_code_ = http2_error_code;
  if (mode_ == CallMode::kServerStreaming) {
    DrainScannerToQueue();
    if (state_ == State::kClosed) return;
    if (!scanner_.failed() && scanner_.HasCompleteMessage()) {
      // Complete messages remain in scanner_ behind a full delivery queue.
      // Keep draining as caller Read()s pop messages without holding an HTTP/2
      // stream slot.
      state_ = State::kDraining;
      return;
    }
  }
  FinalizeAndClose(http2_error_code, /*discard_queued=*/false);
}

void CallState::FailLocal(StatusCode code, std::string message) {
  if (state_ == State::kClosed) return;
  Result result;
  result.code = code;
  result.message = std::move(message);
  result.origin = StatusOrigin::kLocal;
  state_ = State::kClosed;
  completion_->Close(std::move(result), /*discard_queued=*/true);
}

bool CallState::TryPreemptTerminal() {
  if (state_ == State::kClosed || state_ == State::kDraining) return false;
  if (completion_->cancel_requested()) {
    FailLocal(StatusCode::kCancelled, "Cancelled");
    return true;
  }
  if (deadline_.has_value() && std::chrono::steady_clock::now() >= *deadline_) {
    FailLocal(StatusCode::kDeadlineExceeded, "Deadline Exceeded");
    return true;
  }
  return false;
}

void CallState::FinalizeAndClose(uint32_t http2_error_code, bool discard_queued) {
  Result result;
  result.trailing_metadata = std::move(trailing_metadata_);

  // A framing violation poisons the call regardless of trailers (§5.1:
  // flag=1 fails the call with INTERNAL; oversize → RESOURCE_EXHAUSTED).
  if (scanner_.failed()) {
    result.code = scanner_.error_code();
    result.message = scanner_.error();
    result.origin = StatusOrigin::kLocal;
  } else if (grpc_status_raw_.has_value()) {
    // Trailers carrying grpc-status win over everything else (§5.5).
    StatusCode code;
    if (!ParseGrpcStatus(*grpc_status_raw_, &code)) {
      // Upstream maps an unparseable grpc-status to UNKNOWN, not INTERNAL.
      result.code = StatusCode::kUnknown;
      result.message = "malformed grpc-status trailer: \"" + *grpc_status_raw_ + "\"";
    } else {
      result.code = code;
      result.message = PercentDecode(grpc_message_raw_);
      if (code == StatusCode::kOk) {
        if (scanner_.HasPartialMessage()) {
          result.code = StatusCode::kInternal;
          result.message = "stream ended mid-message (truncated gRPC frame)";
          result.origin = StatusOrigin::kLocal;
        } else if (mode_ == CallMode::kUnary) {
          // OK unary calls must deliver exactly one complete response message.
          if (response_count_ == 0) {
            result.code = StatusCode::kInternal;
            result.message = "unary call completed with OK status but no response message";
            result.origin = StatusOrigin::kLocal;
          } else if (response_count_ > 1) {
            result.code = StatusCode::kInternal;
            result.message = "unary call received more than one response message";
            result.origin = StatusOrigin::kLocal;
          } else {
            result.response = std::move(first_response_);
          }
        }
      }
    }
  } else if (saw_initial_headers_ && http_status_ != 200) {
    // No grpc-status anywhere: non-200 HTTP status wins over RST_STREAM (e.g.
    // an HTTP proxy returning 404/502/503 and resetting the client upload with
    // RST_STREAM(CANCEL), matching upstream and the M3 §5.5 contract).
    result.code = HttpStatusToGrpcCode(http_status_);
    result.message = "unexpected HTTP status " + std::to_string(http_status_);
  } else if (saw_initial_headers_ && !IsGrpcContentType(content_type_)) {
    result.code = StatusCode::kInternal;
    result.message = "unexpected content-type \"" + content_type_ + "\"";
  } else if (http2_error_code != 0) {
    switch (http2_error_code) {
      case kHttp2RefusedStream:
        result.code = StatusCode::kUnavailable;
        result.message = "stream refused by server (REFUSED_STREAM)";
        break;
      case kHttp2Cancel:
        result.code = StatusCode::kCancelled;
        result.message = "stream cancelled by server (RST_STREAM CANCEL)";
        break;
      case kHttp2EnhanceYourCalm:
        result.code = StatusCode::kResourceExhausted;
        result.message = "stream reset by server (RST_STREAM ENHANCE_YOUR_CALM)";
        break;
      case kHttp2InadequateSecurity:
        result.code = StatusCode::kPermissionDenied;
        result.message = "stream reset by server (RST_STREAM INADEQUATE_SECURITY)";
        break;
      default:
        result.code = StatusCode::kInternal;
        result.message = "stream reset, HTTP/2 error code " + std::to_string(http2_error_code);
        break;
    }
  } else if (scanner_.HasPartialMessage()) {
    // Clean close mid-message without grpc-status is a framing error.
    result.code = StatusCode::kInternal;
    result.message = "stream ended mid-message (truncated gRPC frame)";
    result.origin = StatusOrigin::kLocal;
  } else {
    // Clean close without grpc-status (HTTP 200 maps to UNKNOWN per §5.5).
    result.code = StatusCode::kUnknown;
    result.message = "server closed stream without grpc-status";
  }

  state_ = State::kClosed;
  completion_->Close(std::move(result), discard_queued);
}

}  // namespace internal
}  // namespace egrpc
