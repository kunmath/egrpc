// egrpc — gRPC wire-format helpers (design §5.1, §5.5). See grpc_wire.h.
#include "core/grpc_wire.h"

namespace egrpc {
namespace internal {

namespace {

// grpc-timeout values must fit 8 decimal digits (§5.1).
constexpr int64_t kMaxTimeoutDigitsValue = 99999999;

uint32_t ReadBigEndian32(const char* p) {
  return (static_cast<uint32_t>(static_cast<uint8_t>(p[0])) << 24) |
         (static_cast<uint32_t>(static_cast<uint8_t>(p[1])) << 16) |
         (static_cast<uint32_t>(static_cast<uint8_t>(p[2])) << 8) |
         static_cast<uint32_t>(static_cast<uint8_t>(p[3]));
}

int HexDigit(char c) {
  if (c >= '0' && c <= '9') return c - '0';
  if (c >= 'a' && c <= 'f') return c - 'a' + 10;
  if (c >= 'A' && c <= 'F') return c - 'A' + 10;
  return -1;
}

constexpr char kBase64Chars[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

int Base64Value(char c) {
  if (c >= 'A' && c <= 'Z') return c - 'A';
  if (c >= 'a' && c <= 'z') return c - 'a' + 26;
  if (c >= '0' && c <= '9') return c - '0' + 52;
  if (c == '+') return 62;
  if (c == '/') return 63;
  return -1;
}

}  // namespace

StatusCode HttpStatusToGrpcCode(int http_status) {
  switch (http_status) {
    case 400:
      return StatusCode::kInternal;
    case 401:
      return StatusCode::kUnauthenticated;
    case 403:
      return StatusCode::kPermissionDenied;
    case 404:
      return StatusCode::kUnimplemented;
    case 429:
    case 502:
    case 503:
    case 504:
      return StatusCode::kUnavailable;
    default:
      return StatusCode::kUnknown;
  }
}

bool ParseGrpcStatus(const std::string& value, StatusCode* code) {
  if (value.empty() || value.size() > 10) return false;
  int64_t n = 0;
  for (char c : value) {
    if (c < '0' || c > '9') return false;
    n = n * 10 + (c - '0');
  }
  *code = (n <= 16) ? static_cast<StatusCode>(n) : StatusCode::kUnknown;
  return true;
}

void AppendGrpcFramePrefix(size_t payload_len, std::string* out) {
  const uint32_t len = static_cast<uint32_t>(payload_len);
  const char prefix[5] = {
      0,  // compressed flag: egrpc never compresses (§5.1)
      static_cast<char>((len >> 24) & 0xff),
      static_cast<char>((len >> 16) & 0xff),
      static_cast<char>((len >> 8) & 0xff),
      static_cast<char>(len & 0xff),
  };
  out->append(prefix, sizeof(prefix));
}

GrpcMessageScanner::GrpcMessageScanner(size_t max_message_size)
    : max_message_size_(max_message_size) {}

bool GrpcMessageScanner::Feed(const uint8_t* data, size_t len) {
  if (failed_) return false;
  buf_.append(reinterpret_cast<const char*>(data), len);

  // Validate the prefix of every frame whose 5 header bytes are now present,
  // so oversize/compressed frames are rejected before their payload is
  // buffered (the max_message_size bound, design §4.3). The oversize check
  // also keeps `5 + msg_len` below any wrap on 32-bit size_t, provided
  // max_message_size is a sane bound (callers pass ≤ 4 MiB defaults).
  size_t offset = pos_;
  while (buf_.size() - offset >= 5) {
    const uint8_t flag = static_cast<uint8_t>(buf_[offset]);
    const uint32_t msg_len = ReadBigEndian32(buf_.data() + offset + 1);
    if (flag == 1) {
      failed_ = true;
      error_code_ = StatusCode::kInternal;
      error_ = "compressed message received but compression is not supported";
      return false;
    }
    if (flag != 0) {
      failed_ = true;
      error_code_ = StatusCode::kInternal;
      error_ = "malformed gRPC message: invalid compressed flag " + std::to_string(flag);
      return false;
    }
    if (msg_len > max_message_size_) {
      failed_ = true;
      error_code_ = StatusCode::kResourceExhausted;
      error_ = "received message of " + std::to_string(msg_len) +
               " bytes exceeds max_receive_message_size (" + std::to_string(max_message_size_) +
               ")";
      return false;
    }
    if (buf_.size() - offset < 5 + static_cast<size_t>(msg_len)) break;
    offset += 5 + static_cast<size_t>(msg_len);
  }
  return true;
}

bool GrpcMessageScanner::Next(std::string* message) {
  if (failed_) return false;
  const size_t available = buf_.size() - pos_;
  if (available < 5) return false;
  const uint32_t msg_len = ReadBigEndian32(buf_.data() + pos_ + 1);
  if (available < 5 + static_cast<size_t>(msg_len)) return false;

  message->assign(buf_, pos_ + 5, msg_len);
  pos_ += 5 + static_cast<size_t>(msg_len);

  if (pos_ == buf_.size()) {
    buf_.clear();
    pos_ = 0;
  } else if (pos_ >= 65536 && pos_ >= buf_.size() / 2) {
    buf_.erase(0, pos_);
    pos_ = 0;
  }
  return true;
}

bool GrpcMessageScanner::HasPartialMessage() const { return buf_.size() > pos_; }

std::string EncodeGrpcTimeout(std::chrono::nanoseconds timeout) {
  int64_t ns = timeout.count();
  if (ns <= 0) return "1n";

  struct Unit {
    char suffix;
    int64_t nanos;
  };
  // Most precise first; the first whose (rounded-up) value fits 8 digits
  // wins. Hours always fit: int64 nanoseconds max ≈ 2.6e6 hours.
  static constexpr Unit kUnits[] = {
      {'n', 1},          {'u', 1000},          {'m', 1000000},
      {'S', 1000000000}, {'M', 60000000000LL}, {'H', 3600000000000LL},
  };
  for (const Unit& u : kUnits) {
    const int64_t value = ns / u.nanos + (ns % u.nanos != 0 ? 1 : 0);  // round up
    if (value <= kMaxTimeoutDigitsValue) {
      return std::to_string(value) + u.suffix;
    }
  }
  return std::to_string(kMaxTimeoutDigitsValue) + 'H';  // ~11408 years; clamp
}

std::string PercentDecode(const std::string& in) {
  std::string out;
  out.reserve(in.size());
  for (size_t i = 0; i < in.size(); ++i) {
    if (in[i] == '%' && i + 2 < in.size()) {
      const int hi = HexDigit(in[i + 1]);
      const int lo = HexDigit(in[i + 2]);
      if (hi >= 0 && lo >= 0) {
        out.push_back(static_cast<char>(hi * 16 + lo));
        i += 2;
        continue;
      }
    }
    out.push_back(in[i]);  // invalid/truncated sequences pass through (§5.1)
  }
  return out;
}

std::string Base64Encode(const std::string& in) {
  std::string out;
  out.reserve((in.size() + 2) / 3 * 4);
  size_t i = 0;
  for (; i + 3 <= in.size(); i += 3) {
    const uint32_t v = (static_cast<uint8_t>(in[i]) << 16) |
                       (static_cast<uint8_t>(in[i + 1]) << 8) | static_cast<uint8_t>(in[i + 2]);
    out.push_back(kBase64Chars[(v >> 18) & 0x3f]);
    out.push_back(kBase64Chars[(v >> 12) & 0x3f]);
    out.push_back(kBase64Chars[(v >> 6) & 0x3f]);
    out.push_back(kBase64Chars[v & 0x3f]);
  }
  const size_t rest = in.size() - i;
  if (rest == 1) {
    const uint32_t v = static_cast<uint8_t>(in[i]) << 16;
    out.push_back(kBase64Chars[(v >> 18) & 0x3f]);
    out.push_back(kBase64Chars[(v >> 12) & 0x3f]);
  } else if (rest == 2) {
    const uint32_t v = (static_cast<uint8_t>(in[i]) << 16) | (static_cast<uint8_t>(in[i + 1]) << 8);
    out.push_back(kBase64Chars[(v >> 18) & 0x3f]);
    out.push_back(kBase64Chars[(v >> 12) & 0x3f]);
    out.push_back(kBase64Chars[(v >> 6) & 0x3f]);
  }
  return out;  // no padding (§5.1)
}

bool Base64Decode(const std::string& in, std::string* out) {
  // Accept padded and unpadded input: strip up to two trailing '='.
  size_t len = in.size();
  while (len > 0 && in[len - 1] == '=') --len;
  if (len % 4 == 1) return false;  // impossible base64 length

  out->clear();
  out->reserve(len / 4 * 3 + 2);
  uint32_t acc = 0;
  int bits = 0;
  for (size_t i = 0; i < len; ++i) {
    const int v = Base64Value(in[i]);
    if (v < 0) return false;
    acc = (acc << 6) | static_cast<uint32_t>(v);
    bits += 6;
    if (bits >= 8) {
      bits -= 8;
      out->push_back(static_cast<char>((acc >> bits) & 0xff));
    }
  }
  return true;
}

}  // namespace internal
}  // namespace egrpc
