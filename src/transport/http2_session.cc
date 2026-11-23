// egrpc — HTTP/2 session: thin RAII wrapper over nghttp2 (design §4.2, §5.2).

#include "transport/http2_session.h"

#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <utility>

namespace egrpc {
namespace internal {

// nghttp2 callbacks. Declared as a friend of Http2Session in the header so the
// static trampolines can reach the private state directly.
struct Http2Session::Impl {
  static Stream* FindStream(Http2Session* self, int32_t stream_id) {
    const auto it = self->streams_.find(stream_id);
    return it == self->streams_.end() ? nullptr : it->second.get();
  }

  static int OnFrameRecv(nghttp2_session* /*session*/, const nghttp2_frame* frame,
                         void* user_data) {
    auto* self = static_cast<Http2Session*>(user_data);
    switch (frame->hd.type) {
      case NGHTTP2_DATA: {
        Stream* stream = FindStream(self, frame->hd.stream_id);
        if (stream == nullptr) break;
        // Even with no_auto_window_update=1, nghttp2 accounts for DATA padding
        // internally, but only flushes WINDOW_UPDATE once its threshold is
        // crossed. Re-check FlushManualStreamCredit so padding on a tight
        // window can never deadlock the first legal max-size message.
        if (frame->data.padlen > 0 && stream->manual_flow_control) {
          self->FlushManualStreamCredit(stream, frame->hd.stream_id);
        }
        if ((frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0 &&
            stream->hooks.on_response_end_stream) {
          stream->hooks.on_response_end_stream();
        }
        break;
      }
      case NGHTTP2_HEADERS: {
        Stream* stream = FindStream(self, frame->hd.stream_id);
        if (stream == nullptr) break;
        HeaderList headers = std::move(stream->pending_headers);
        stream->pending_headers.clear();
        const bool end_stream = (frame->hd.flags & NGHTTP2_FLAG_END_STREAM) != 0;
        // Informational 1xx responses arrive as HCAT_RESPONSE; nghttp2 then
        // delivers the final response (e.g. 200) as HCAT_HEADERS. Ignore 1xx
        // blocks and route the first non-1xx block to on_response_headers.
        if (frame->headers.cat == NGHTTP2_HCAT_RESPONSE ||
            (!stream->final_response_headers_seen && frame->headers.cat == NGHTTP2_HCAT_HEADERS)) {
          int status = 0;
          for (const auto& h : headers) {
            if (h.first == ":status") {
              status = std::atoi(h.second.c_str());
              break;
            }
          }
          if (status >= 100 && status < 200 && !end_stream) {
            break;
          }
          stream->final_response_headers_seen = true;
          if (stream->hooks.on_response_headers) {
            stream->hooks.on_response_headers(std::move(headers), end_stream);
          }
        } else if (stream->hooks.on_trailers) {
          stream->hooks.on_trailers(std::move(headers));
        }
        if (end_stream && stream->hooks.on_response_end_stream) {
          stream->hooks.on_response_end_stream();
        }
        break;
      }
      case NGHTTP2_SETTINGS:
        if (frame->hd.flags & NGHTTP2_FLAG_ACK) {
          if (self->hooks_.on_settings_ack) self->hooks_.on_settings_ack();
        } else {
          self->remote_settings_received_ = true;
          if (self->hooks_.on_remote_settings) self->hooks_.on_remote_settings();
        }
        break;
      case NGHTTP2_PING:
        if (frame->hd.flags & NGHTTP2_FLAG_ACK) {
          self->ping_outstanding_ = false;
          ++self->pings_acked_;
          if (self->hooks_.on_ping_ack) self->hooks_.on_ping_ack();
        }
        break;
      case NGHTTP2_GOAWAY: {
        self->draining_ = true;
        const nghttp2_goaway& goaway = frame->goaway;
        static const char kTooManyPings[] = "too_many_pings";
        const size_t kTooManyPingsLen = sizeof(kTooManyPings) - 1;
        self->too_many_pings_ =
            goaway.error_code == NGHTTP2_ENHANCE_YOUR_CALM && goaway.opaque_data != nullptr &&
            goaway.opaque_data_len == kTooManyPingsLen &&
            std::memcmp(goaway.opaque_data, kTooManyPings, kTooManyPingsLen) == 0;
        if (self->hooks_.on_goaway) {
          self->hooks_.on_goaway(goaway.error_code, goaway.last_stream_id, self->too_many_pings_);
        }
        break;
      }
      default:
        break;
    }
    return 0;
  }

  static int OnHeader(nghttp2_session* /*session*/, const nghttp2_frame* frame, const uint8_t* name,
                      size_t namelen, const uint8_t* value, size_t valuelen, uint8_t /*flags*/,
                      void* user_data) {
    auto* self = static_cast<Http2Session*>(user_data);
    if (frame->hd.type != NGHTTP2_HEADERS) return 0;
    Stream* stream = FindStream(self, frame->hd.stream_id);
    if (stream == nullptr) return 0;
    stream->pending_headers.emplace_back(
        std::string(reinterpret_cast<const char*>(name), namelen),
        std::string(reinterpret_cast<const char*>(value), valuelen));
    return 0;
  }

  static int OnDataChunkRecv(nghttp2_session* session, uint8_t /*flags*/, int32_t stream_id,
                             const uint8_t* data, size_t len, void* user_data) {
    auto* self = static_cast<Http2Session*>(user_data);
    // Connection-level credit is always returned immediately (§5.4) so a
    // slow/stalled stream never starves sibling streams or leaks window on a
    // recently-reset stream.
    if (len > 0) {
      nghttp2_session_consume_connection(session, len);
    }
    Stream* stream = FindStream(self, stream_id);
    if (stream == nullptr) return 0;
    if (!stream->manual_flow_control && len > 0) {
      nghttp2_session_consume_stream(session, stream_id, len);
    } else if (stream->manual_flow_control) {
      stream->unconsumed_payload_bytes += len;
    }
    if (stream->hooks.on_data) stream->hooks.on_data(data, len);
    // For manual-flow streams, if prior Read()s or padding already freed
    // credit and this chunk pushed remaining peer window to half or less,
    // flush all eligible credit so a following max-size message cannot stall.
    if (stream->manual_flow_control) {
      self->FlushManualStreamCredit(stream, stream_id);
    }
    return 0;
  }

  static int OnStreamClose(nghttp2_session* /*session*/, int32_t stream_id, uint32_t error_code,
                           void* user_data) {
    auto* self = static_cast<Http2Session*>(user_data);
    const auto it = self->streams_.find(stream_id);
    if (it == self->streams_.end()) return 0;
    // Detach before invoking on_close so the stream is already gone if the
    // hook inspects session state; on_close is the last hook for a stream.
    const std::unique_ptr<Stream> stream = std::move(it->second);
    self->streams_.erase(it);
    --self->active_streams_;
    if (stream->hooks.on_close) stream->hooks.on_close(error_code);
    return 0;
  }

  // Data provider for SubmitUnaryRequest: reads from the stream's in-memory
  // body, EOF (→ END_STREAM, the unary half-close) once drained.
  static ssize_t DataSourceRead(nghttp2_session* /*session*/, int32_t /*stream_id*/, uint8_t* buf,
                                size_t length, uint32_t* data_flags, nghttp2_data_source* source,
                                void* /*user_data*/) {
    auto* stream = static_cast<Stream*>(source->ptr);
    const size_t remaining = stream->body.size() - stream->body_offset;
    const size_t n = std::min(length, remaining);
    std::memcpy(buf, stream->body.data() + stream->body_offset, n);
    stream->body_offset += n;
    if (stream->body_offset == stream->body.size()) *data_flags |= NGHTTP2_DATA_FLAG_EOF;
    return static_cast<ssize_t>(n);
  }
};

Http2Session::Http2Session(const Http2SessionOptions& options, Hooks hooks, TimePoint now)
    : options_(options), hooks_(std::move(hooks)), last_read_(now) {
  nghttp2_session_callbacks* callbacks = nullptr;
  if (nghttp2_session_callbacks_new(&callbacks) != 0) {
    error_ = "nghttp2_session_callbacks_new failed";
    return;
  }
  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, &Impl::OnFrameRecv);
  nghttp2_session_callbacks_set_on_header_callback(callbacks, &Impl::OnHeader);
  nghttp2_session_callbacks_set_on_data_chunk_recv_callback(callbacks, &Impl::OnDataChunkRecv);
  nghttp2_session_callbacks_set_on_stream_close_callback(callbacks, &Impl::OnStreamClose);

  // Disable automatic WINDOW_UPDATE globally (§5.4): connection credit and
  // unary stream credit are returned explicitly on DATA receipt, while
  // server-streaming streams return stream credit only via ConsumeStream().
  nghttp2_option* ng_opt = nullptr;
  if (nghttp2_option_new(&ng_opt) != 0) {
    nghttp2_session_callbacks_del(callbacks);
    error_ = "nghttp2_option_new failed";
    return;
  }
  nghttp2_option_set_no_auto_window_update(ng_opt, 1);

  const int new_rc = nghttp2_session_client_new2(&session_, callbacks, this, ng_opt);
  nghttp2_option_del(ng_opt);
  nghttp2_session_callbacks_del(callbacks);
  if (new_rc != 0) {
    error_ = std::string("nghttp2_session_client_new2 failed: ") + nghttp2_strerror(new_rc);
    session_ = nullptr;
    return;
  }

  // Advertise our SETTINGS. nghttp2 has already queued the connection preface.
  const nghttp2_settings_entry iv[] = {
      {NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS, options_.max_concurrent_streams},
      {NGHTTP2_SETTINGS_INITIAL_WINDOW_SIZE, options_.initial_window_size},
      {NGHTTP2_SETTINGS_ENABLE_PUSH, 0},
  };
  const int settings_rc =
      nghttp2_submit_settings(session_, NGHTTP2_FLAG_NONE, iv, sizeof(iv) / sizeof(iv[0]));
  if (settings_rc != 0) {
    error_ = std::string("nghttp2_submit_settings failed: ") + nghttp2_strerror(settings_rc);
    nghttp2_session_del(session_);
    session_ = nullptr;
    return;
  }

  // Raise the connection-level (stream 0) receive window to at least
  // initial_window_size (and at least the RFC 7540 65535 initial window so
  // small test stream windows never under-size the connection window).
  // SETTINGS_INITIAL_WINDOW_SIZE caps at 2^31-1; clamp so a user-supplied
  // value cannot go negative through the cast.
  const int32_t stream_window = static_cast<int32_t>(
      std::min<uint32_t>(options_.initial_window_size, static_cast<uint32_t>(INT32_MAX)));
  const int32_t conn_window = std::max<int32_t>(stream_window, 65535);
  const int window_rc =
      nghttp2_session_set_local_window_size(session_, NGHTTP2_FLAG_NONE, 0, conn_window);
  if (window_rc != 0) {
    error_ =
        std::string("nghttp2_session_set_local_window_size failed: ") + nghttp2_strerror(window_rc);
    nghttp2_session_del(session_);
    session_ = nullptr;
    return;
  }
}

Http2Session::~Http2Session() { nghttp2_session_del(session_); }

bool Http2Session::ok() const { return session_ != nullptr; }

bool Http2Session::ReceiveBytes(TimePoint now, const uint8_t* data, size_t len) {
  if (session_ == nullptr) return false;
  // A read is a read even if it turns out malformed — keepalive liveness is
  // measured at the transport level, so bump the clock before feeding nghttp2.
  if (len > 0) last_read_ = now;
  const ssize_t rc = nghttp2_session_mem_recv(session_, data, len);
  if (rc < 0) {
    error_ = std::string("nghttp2 receive error: ") + nghttp2_strerror(static_cast<int>(rc));
    return false;
  }
  return true;
}

int32_t Http2Session::SubmitUnaryRequest(const HeaderList& headers, std::string body,
                                         StreamHooks hooks) {
  return SubmitRequest(headers, std::move(body), std::move(hooks),
                       /*manual_flow_control=*/false);
}

int32_t Http2Session::SubmitServerStreamRequest(const HeaderList& headers, std::string body,
                                                StreamHooks hooks) {
  return SubmitRequest(headers, std::move(body), std::move(hooks),
                       /*manual_flow_control=*/true);
}

int32_t Http2Session::SubmitRequest(const HeaderList& headers, std::string body, StreamHooks hooks,
                                    bool manual_flow_control) {
  call_error_.clear();
  if (session_ == nullptr) {
    call_error_ = "session not initialized";
    return -1;
  }
  if (draining_) {
    call_error_ = "session is draining (GOAWAY received); no new streams";
    return -1;
  }

  auto stream = std::make_unique<Stream>();
  stream->hooks = std::move(hooks);
  stream->body = std::move(body);
  stream->manual_flow_control = manual_flow_control;

  // nghttp2_nv wants non-const pointers but copies names and values during
  // submit (no NO_COPY flags set), so the casts do not outlive this call.
  std::vector<nghttp2_nv> nva;
  nva.reserve(headers.size());
  for (const Header& h : headers) {
    nva.push_back({reinterpret_cast<uint8_t*>(const_cast<char*>(h.first.data())),
                   reinterpret_cast<uint8_t*>(const_cast<char*>(h.second.data())), h.first.size(),
                   h.second.size(), NGHTTP2_NV_FLAG_NONE});
  }

  nghttp2_data_provider provider{};
  provider.source.ptr = stream.get();
  provider.read_callback = &Impl::DataSourceRead;

  const int32_t stream_id =
      nghttp2_submit_request(session_, nullptr, nva.data(), nva.size(), &provider, nullptr);
  if (stream_id < 0) {
    call_error_ = std::string("nghttp2_submit_request failed: ") + nghttp2_strerror(stream_id);
    return -1;
  }
  streams_.emplace(stream_id, std::move(stream));
  ++active_streams_;
  return stream_id;
}

void Http2Session::ConsumeStream(int32_t stream_id, size_t bytes) {
  if (session_ == nullptr || bytes == 0) return;
  Stream* stream = Impl::FindStream(this, stream_id);
  if (stream == nullptr || !stream->manual_flow_control) return;
  stream->unconsumed_payload_bytes -= std::min(bytes, stream->unconsumed_payload_bytes);
  FlushManualStreamCredit(stream, stream_id);
}

void Http2Session::FlushManualStreamCredit(Stream* stream, int32_t stream_id) {
  if (session_ == nullptr || !stream->manual_flow_control) return;
  const int32_t effective_local =
      nghttp2_session_get_stream_effective_local_window_size(session_, stream_id);
  const int32_t recv_len =
      nghttp2_session_get_stream_effective_recv_data_length(session_, stream_id);
  const int32_t remaining = nghttp2_session_get_stream_local_window_size(session_, stream_id);
  if (effective_local <= 0 || recv_len <= 0 || remaining < 0) return;

  // recv_len is nghttp2's exact count of unacknowledged flow-controlled bytes
  // on the stream (held payload + popped payload + unacked padding).
  // Subtracting unconsumed_payload_bytes yields the exact credit eligible to
  // return (popped payload + padding) with zero drift, leak, or double-credit,
  // even when nghttp2 auto-flushes padding on its own.
  if (static_cast<size_t>(recv_len) <= stream->unconsumed_payload_bytes) return;
  const size_t available_credit = static_cast<size_t>(recv_len) - stream->unconsumed_payload_bytes;

  // Batch small reads/padding up to half the stream window, but flush
  // immediately whenever remaining peer window is at or below half so neither
  // small consumed messages nor padding can strand credit needed by a legal
  // max-size message.
  const int32_t half_window = std::max<int32_t>(1, effective_local / 2);
  if (available_credit < static_cast<size_t>(half_window) && remaining > half_window) {
    return;
  }

  const int32_t to_send =
      static_cast<int32_t>(std::min<size_t>(available_credit, static_cast<size_t>(INT32_MAX)));
  if (to_send > 0) {
    nghttp2_submit_window_update(session_, NGHTTP2_FLAG_NONE, stream_id, to_send);
  }
}

bool Http2Session::ResetStream(int32_t stream_id, uint32_t error_code) {
  call_error_.clear();
  if (session_ == nullptr || stream_id <= 0) return false;
  if (Impl::FindStream(this, stream_id) == nullptr) return true;
  const int rc = nghttp2_submit_rst_stream(session_, NGHTTP2_FLAG_NONE, stream_id, error_code);
  if (rc != 0) {
    call_error_ = std::string("nghttp2_submit_rst_stream failed: ") + nghttp2_strerror(rc);
    return false;
  }
  return true;
}

uint32_t Http2Session::remote_max_concurrent_streams() const {
  if (session_ == nullptr) return 0;
  return nghttp2_session_get_remote_settings(const_cast<nghttp2_session*>(session_),
                                             NGHTTP2_SETTINGS_MAX_CONCURRENT_STREAMS);
}

const uint8_t* Http2Session::PendingOutput(size_t* len) {
  if (session_ == nullptr) {
    *len = 0;
    return nullptr;
  }
  // Refill only once the previously produced region has been fully consumed
  // (an SSL_write_ex retry must see the same buffer). Each refill is capped so
  // a large peer flow-control window cannot make one call copy megabytes.
  constexpr size_t kMaxRefillBytes = 64 * 1024;
  if (out_consumed_ >= out_.size()) {
    out_.clear();
    out_consumed_ = 0;
    while (out_.size() < kMaxRefillBytes) {
      const uint8_t* chunk = nullptr;
      const ssize_t n = nghttp2_session_mem_send(session_, &chunk);
      if (n < 0) {
        error_ = std::string("nghttp2 send error: ") + nghttp2_strerror(static_cast<int>(n));
        *len = 0;
        return nullptr;
      }
      if (n == 0) break;
      out_.insert(out_.end(), chunk, chunk + n);
    }
  }
  if (out_consumed_ >= out_.size()) {
    *len = 0;
    return nullptr;
  }
  *len = out_.size() - out_consumed_;
  return out_.data() + out_consumed_;
}

void Http2Session::ConsumeOutput(size_t n) {
  out_consumed_ += n;
  if (out_consumed_ > out_.size()) out_consumed_ = out_.size();  // defensive clamp
}

bool Http2Session::WantWrite() {
  if (out_consumed_ < out_.size()) return true;
  return session_ != nullptr && nghttp2_session_want_write(session_) != 0;
}

bool Http2Session::WantRead() const {
  return session_ != nullptr &&
         nghttp2_session_want_read(const_cast<nghttp2_session*>(session_)) != 0;
}

bool Http2Session::IsStreamLocalOpen(int32_t stream_id) const {
  if (session_ == nullptr || stream_id <= 0) return false;
  return nghttp2_session_get_stream_local_close(const_cast<nghttp2_session*>(session_),
                                                stream_id) == 0;
}

std::optional<Http2Session::TimePoint> Http2Session::NextKeepaliveDeadline(
    TimePoint /*now*/) const {
  if (session_ == nullptr) return std::nullopt;
  if (!options_.keepalive_permit_without_calls && active_streams_ == 0) return std::nullopt;
  if (ping_outstanding_) return ping_sent_ + options_.keepalive_timeout;
  return last_read_ + options_.keepalive_time;
}

Http2Session::KeepaliveAction Http2Session::CheckKeepalive(TimePoint now) {
  if (session_ == nullptr) return KeepaliveAction::kNone;

  // (a) Any read since the outstanding PING was sent counts as liveness.
  if (ping_outstanding_ && last_read_ >= ping_sent_) {
    ping_outstanding_ = false;
  }

  // (b) A PING still outstanding: dead if keepalive_timeout has elapsed.
  if (ping_outstanding_) {
    if (now - ping_sent_ >= options_.keepalive_timeout) return KeepaliveAction::kDead;
    return KeepaliveAction::kNone;
  }

  // (c) No PING outstanding: keepalive is gated off without active streams
  // unless permit_without_calls is set.
  if (!options_.keepalive_permit_without_calls && active_streams_ == 0) {
    return KeepaliveAction::kNone;
  }
  if (now - last_read_ >= options_.keepalive_time) {
    const uint64_t counter = ++ping_counter_;
    uint8_t opaque[8];
    for (int i = 0; i < 8; ++i) {
      opaque[i] = static_cast<uint8_t>((counter >> (56 - 8 * i)) & 0xFFu);
    }
    const int rc = nghttp2_submit_ping(session_, NGHTTP2_FLAG_NONE, opaque);
    if (rc != 0) {
      // PING never queued: do not arm the death clock on it (a false kDead
      // would blame the peer for a local failure). Surface via error().
      error_ = std::string("nghttp2_submit_ping failed: ") + nghttp2_strerror(rc);
      return KeepaliveAction::kNone;
    }
    ping_sent_ = now;
    ping_outstanding_ = true;
    return KeepaliveAction::kPingSent;
  }
  return KeepaliveAction::kNone;
}

void Http2Session::Terminate() {
  if (session_ == nullptr) return;
  nghttp2_session_terminate_session(session_, NGHTTP2_NO_ERROR);
  draining_ = true;
}

bool Http2Session::draining() const { return draining_; }
bool Http2Session::goaway_too_many_pings() const { return too_many_pings_; }
const std::string& Http2Session::error() const { return error_; }
bool Http2Session::ping_outstanding() const { return ping_outstanding_; }
uint64_t Http2Session::pings_acked() const { return pings_acked_; }

}  // namespace internal
}  // namespace egrpc
