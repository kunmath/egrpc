// egrpc — HTTP/2 session: thin RAII wrapper over nghttp2 (design §4.2, §5.2).

#include "transport/http2_session.h"

#include <nghttp2/nghttp2.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <utility>

namespace egrpc {
namespace internal {

// nghttp2 callbacks. Declared as a friend of Http2Session in the header so the
// static trampolines can reach the private state directly.
struct Http2Session::Impl {
  static int OnFrameRecv(nghttp2_session* /*session*/, const nghttp2_frame* frame,
                         void* user_data) {
    auto* self = static_cast<Http2Session*>(user_data);
    switch (frame->hd.type) {
      case NGHTTP2_SETTINGS:
        if (frame->hd.flags & NGHTTP2_FLAG_ACK) {
          if (self->hooks_.on_settings_ack) self->hooks_.on_settings_ack();
        } else if (self->hooks_.on_remote_settings) {
          self->hooks_.on_remote_settings();
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
};

Http2Session::Http2Session(const Http2SessionOptions& options, Hooks hooks, TimePoint now)
    : options_(options), hooks_(std::move(hooks)), last_read_(now) {
  nghttp2_session_callbacks* callbacks = nullptr;
  if (nghttp2_session_callbacks_new(&callbacks) != 0) {
    error_ = "nghttp2_session_callbacks_new failed";
    return;
  }
  nghttp2_session_callbacks_set_on_frame_recv_callback(callbacks, &Impl::OnFrameRecv);

  const int new_rc = nghttp2_session_client_new(&session_, callbacks, this);
  nghttp2_session_callbacks_del(callbacks);
  if (new_rc != 0) {
    error_ = std::string("nghttp2_session_client_new failed: ") + nghttp2_strerror(new_rc);
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

  // Raise the connection-level (stream 0) receive window to match the stream
  // window; nghttp2 emits the WINDOW_UPDATE into the output buffer (§5.4).
  // SETTINGS_INITIAL_WINDOW_SIZE caps at 2^31-1; clamp so a future
  // user-supplied value cannot go negative through the cast.
  const int32_t window = static_cast<int32_t>(
      std::min<uint32_t>(options_.initial_window_size, static_cast<uint32_t>(INT32_MAX)));
  const int window_rc =
      nghttp2_session_set_local_window_size(session_, NGHTTP2_FLAG_NONE, 0, window);
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

const uint8_t* Http2Session::PendingOutput(size_t* len) {
  if (session_ == nullptr) {
    *len = 0;
    return nullptr;
  }
  // Refill only once the previously produced region has been fully consumed.
  if (out_consumed_ >= out_.size()) {
    out_.clear();
    out_consumed_ = 0;
    for (;;) {
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

bool Http2Session::draining() const { return draining_; }
bool Http2Session::goaway_too_many_pings() const { return too_many_pings_; }
const std::string& Http2Session::error() const { return error_; }
bool Http2Session::ping_outstanding() const { return ping_outstanding_; }
uint64_t Http2Session::pings_acked() const { return pings_acked_; }

}  // namespace internal
}  // namespace egrpc
