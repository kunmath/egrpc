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

bool IsGrpcContentType(const std::string& value) {
  return value.compare(0, 16, "application/grpc") == 0;
}

}  // namespace

Http2Session::HeaderList BuildRequestHeaders(const std::string& authority,
                                             const std::string& method_path,
                                             std::optional<std::chrono::nanoseconds> timeout) {
  Http2Session::HeaderList headers = {
      {":method", "POST"},        {":scheme", "https"},
      {":path", method_path},     {":authority", authority},
      {"te", "trailers"},         {"content-type", "application/grpc"},
      {"user-agent", kUserAgent}, {"grpc-accept-encoding", "identity"},
  };
  if (timeout.has_value()) {
    headers.emplace_back("grpc-timeout", EncodeGrpcTimeout(*timeout));
  }
  return headers;
}

CallState::CallState(size_t max_receive_message_size) : scanner_(max_receive_message_size) {}

Http2Session::StreamHooks CallState::MakeStreamHooks(std::shared_ptr<CallState> call) {
  Http2Session::StreamHooks hooks;
  hooks.on_response_headers = [call](Http2Session::HeaderList headers, bool end_stream) {
    call->OnResponseHeaders(std::move(headers), end_stream);
  };
  hooks.on_trailers = [call](Http2Session::HeaderList trailers) {
    call->OnTrailers(std::move(trailers));
  };
  hooks.on_data = [call](const uint8_t* data, size_t len) { call->OnData(data, len); };
  hooks.on_close = [call](uint32_t error_code) { call->OnClose(error_code); };
  return hooks;
}

void CallState::OnSubmitted(int32_t stream_id) {
  std::lock_guard<std::mutex> lock(mu_);
  if (state_ != State::kCreated) return;
  stream_id_ = stream_id;
  state_ = State::kHeadersSent;
}

void CallState::OnResponseHeaders(Http2Session::HeaderList headers, bool end_stream) {
  std::lock_guard<std::mutex> lock(mu_);
  if (state_ == State::kClosed) return;
  state_ = State::kOpen;
  saw_initial_headers_ = true;

  // Trailers-Only (design §4.3): this HEADERS is simultaneously the
  // trailers; all its metadata is trailing, matching upstream.
  Http2Session::HeaderList& metadata =
      end_stream ? result_.trailing_metadata : result_.initial_metadata;

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
  // Final status is computed at OnClose, which nghttp2 fires right after an
  // END_STREAM HEADERS — no completion signal here.
}

void CallState::OnData(const uint8_t* data, size_t len) {
  std::lock_guard<std::mutex> lock(mu_);
  if (state_ == State::kClosed || scanner_.failed()) return;
  // A Feed error surfaces at finalize. TODO(M5): once TryCancel plumbing
  // exists, fail the call immediately with RST_STREAM(CANCEL) instead of
  // waiting for the server to close (a misbehaving server that holds the
  // stream open otherwise delays the failure until keepalive teardown).
  if (!scanner_.Feed(data, len)) return;
  std::string message;
  while (scanner_.Next(&message)) {
    if (++response_count_ == 1) first_response_ = std::move(message);
  }
}

void CallState::OnTrailers(Http2Session::HeaderList trailers) {
  std::lock_guard<std::mutex> lock(mu_);
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
    result_.trailing_metadata.emplace_back(std::move(h));
  }
}

void CallState::OnClose(uint32_t http2_error_code) {
  std::lock_guard<std::mutex> lock(mu_);
  if (state_ == State::kClosed) return;
  FinalizeLocked(http2_error_code);
  CloseLocked();
}

void CallState::FailLocal(StatusCode code, std::string message) {
  std::lock_guard<std::mutex> lock(mu_);
  if (state_ == State::kClosed) return;
  result_.code = code;
  result_.message = std::move(message);
  CloseLocked();
}

const CallState::Result& CallState::Wait() {
  std::unique_lock<std::mutex> lock(mu_);
  cv_.wait(lock, [this] { return done_; });
  return result_;
}

int32_t CallState::stream_id() const {
  std::lock_guard<std::mutex> lock(mu_);
  return stream_id_;
}

void CallState::FinalizeLocked(uint32_t http2_error_code) {
  // A framing violation poisons the call regardless of trailers (§5.1:
  // flag=1 fails the call with INTERNAL; oversize → RESOURCE_EXHAUSTED).
  if (scanner_.failed()) {
    result_.code = scanner_.error_code();
    result_.message = scanner_.error();
    return;
  }

  // Trailers carrying grpc-status win over everything else (§5.5).
  if (grpc_status_raw_.has_value()) {
    StatusCode code;
    if (!ParseGrpcStatus(*grpc_status_raw_, &code)) {
      result_.code = StatusCode::kInternal;
      result_.message = "malformed grpc-status trailer: \"" + *grpc_status_raw_ + "\"";
      return;
    }
    result_.code = code;
    result_.message = PercentDecode(grpc_message_raw_);
    if (code != StatusCode::kOk) return;

    // OK unary calls must deliver exactly one complete response message.
    if (scanner_.HasPartialMessage()) {
      result_.code = StatusCode::kInternal;
      result_.message = "stream ended mid-message (truncated gRPC frame)";
    } else if (response_count_ == 0) {
      result_.code = StatusCode::kInternal;
      result_.message = "unary call completed with OK status but no response message";
    } else if (response_count_ > 1) {
      result_.code = StatusCode::kInternal;
      result_.message = "unary call received more than one response message";
    } else {
      result_.response = std::move(first_response_);
    }
    return;
  }

  // No grpc-status anywhere: fall back per §5.5.
  if (saw_initial_headers_ && http_status_ != 200) {
    result_.code = HttpStatusToGrpcCode(http_status_);
    result_.message = "unexpected HTTP status " + std::to_string(http_status_);
    return;
  }
  if (saw_initial_headers_ && !IsGrpcContentType(content_type_)) {
    result_.code = StatusCode::kInternal;
    result_.message = "unexpected content-type \"" + content_type_ + "\"";
    return;
  }
  switch (http2_error_code) {
    case 0:  // NO_ERROR: clean close without grpc-status is a spec violation
      result_.code = StatusCode::kInternal;
      result_.message = "server closed stream without grpc-status";
      return;
    case kHttp2RefusedStream:
      result_.code = StatusCode::kUnavailable;
      result_.message = "stream refused by server (REFUSED_STREAM)";
      return;
    case kHttp2Cancel:
      result_.code = StatusCode::kCancelled;
      result_.message = "stream cancelled by server (RST_STREAM CANCEL)";
      return;
    default:
      result_.code = StatusCode::kInternal;
      result_.message = "stream reset, HTTP/2 error code " + std::to_string(http2_error_code);
      return;
  }
}

void CallState::CloseLocked() {
  state_ = State::kClosed;
  done_ = true;
  cv_.notify_all();
}

}  // namespace internal
}  // namespace egrpc
