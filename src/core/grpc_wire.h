// egrpc — gRPC wire-format helpers (design §5.1, §5.5): message framing and
// the receive-path reassembly scanner, grpc-timeout encoding, grpc-message
// percent-decoding, unpadded base64 for -bin metadata, and the HTTP→gRPC
// status mapping table.
//
// Everything here is a pure function or a single-threaded value type: no
// sockets, no clocks, no locks. The EventThread owns any scanner instance
// (design §3); unit tests drive these directly (Ring 1, design §7).
#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

namespace egrpc {
namespace internal {

// The 17 canonical gRPC status codes. Values are fixed by the gRPC spec and
// must match upstream grpc::StatusCode (the M4 shim aliases onto this).
enum class StatusCode : int {
  kOk = 0,
  kCancelled = 1,
  kUnknown = 2,
  kInvalidArgument = 3,
  kDeadlineExceeded = 4,
  kNotFound = 5,
  kAlreadyExists = 6,
  kPermissionDenied = 7,
  kResourceExhausted = 8,
  kFailedPrecondition = 9,
  kAborted = 10,
  kOutOfRange = 11,
  kUnimplemented = 12,
  kInternal = 13,
  kUnavailable = 14,
  kDataLoss = 15,
  kUnauthenticated = 16,
};

// HTTP :status → gRPC code fallback used only when trailers carry no
// grpc-status (design §5.5; when grpc-status is present it always wins).
// Unlisted HTTP codes map to kUnknown, matching upstream.
StatusCode HttpStatusToGrpcCode(int http_status);

// Parses a grpc-status trailer value: a base-10 non-negative integer with no
// leading/trailing garbage. Values in [0, 16] map to their code; larger valid
// integers map to kUnknown (matching upstream's unknown-code handling).
// Returns false on a non-integer (the caller then treats the stream as
// malformed → kInternal per §5.1).
bool ParseGrpcStatus(const std::string& value, StatusCode* code);

// --- Message framing (§5.1) -------------------------------------------------
// Every gRPC message on the wire is
//   [1-byte compressed flag = 0][4-byte big-endian length][payload].

// Appends the 5-byte prefix for a payload of `payload_len` bytes (flag = 0 —
// egrpc never compresses) to *out.
void AppendGrpcFramePrefix(size_t payload_len, std::string* out);

// Reassembles gRPC messages from HTTP/2 DATA chunks. Messages can span DATA
// frames and coalesce within one, so this is a length-prefix scanner over an
// accumulation buffer bounded by max_message_size (design §4.3).
//
// Usage (EventThread, per call): Feed() every DATA chunk, then drain with
// Next() until it returns false. A Feed() returning false is a terminal
// protocol error — error_code()/error() have the failure, and the scanner
// must not be used further. At end of stream, HasPartialMessage() reports a
// truncated message (caller fails the call with kInternal).
class GrpcMessageScanner {
 public:
  explicit GrpcMessageScanner(size_t max_message_size);

  // Appends bytes; validates any now-complete prefixes. Fails with
  //   kInternal          — compressed flag set (we advertise identity only)
  //                        or malformed flag byte,
  //   kResourceExhausted — declared length > max_message_size (upstream
  //                        parity for "received message larger than max").
  bool Feed(const uint8_t* data, size_t len);

  // Moves the next complete message payload into *message. False when no
  // complete message is buffered (never a protocol error — see Feed).
  bool Next(std::string* message);

  // True if bytes of an incomplete frame remain buffered.
  bool HasPartialMessage() const;

  bool failed() const { return failed_; }
  StatusCode error_code() const { return error_code_; }
  const std::string& error() const { return error_; }

 private:
  const size_t max_message_size_;
  std::string buf_;  // accumulation buffer; [pos_, size) is unconsumed
  size_t pos_ = 0;   // start of the current (possibly incomplete) frame
  bool failed_ = false;
  StatusCode error_code_ = StatusCode::kOk;
  std::string error_;
};

// --- Header value encodings (§5.1) -------------------------------------------

// Encodes a deadline-relative timeout as the grpc-timeout header value: the
// most precise unit of n/u/m/S/M/H whose value fits 8 decimal digits, rounded
// UP so the wire deadline is never earlier than the local one. Non-positive
// timeouts encode as "1n" (already expired; the server should fail fast).
std::string EncodeGrpcTimeout(std::chrono::nanoseconds timeout);

// Decodes the percent-encoding of grpc-message. Per spec, decoders are
// lenient: invalid or truncated %XX sequences pass through verbatim.
std::string PercentDecode(const std::string& in);

// Base64 for -bin metadata values: encode without padding (§5.1); decode
// accepts padded and unpadded input (spec requires accepting both). Decode
// returns false on non-base64 characters or an impossible length.
std::string Base64Encode(const std::string& in);
bool Base64Decode(const std::string& in, std::string* out);

}  // namespace internal
}  // namespace egrpc
