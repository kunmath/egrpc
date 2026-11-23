// egrpc — unit tests for CallState terminal-status mapping (design §5.5).
//
// CallState is the per-RPC state machine. These tests construct it directly
// (no transport) and drive the EventThread-side hooks
// (OnResponseHeaders/OnTrailers/OnData/OnClose), then inspect the finalized
// Result returned by Wait(). They exercise only the status-mapping precedence
// rules: scanner error > grpc-status > HTTP-status > RST error code > missing
// grpc-status on clean close → kUnknown.
//
// All cases live in the "call_state" doctest test suite so ctest can run just
// them (see tests/unit/CMakeLists.txt).
#include <cstdint>
#include <string>
#include <utility>

#include "core/call_state.h"
#include "core/grpc_wire.h"
#include "doctest/doctest.h"
#include "transport/http2_session.h"

namespace {

using egrpc::internal::CallState;
using egrpc::internal::StatusCode;
using HeaderList = egrpc::internal::Http2Session::HeaderList;

constexpr size_t kMax = 4 * 1024 * 1024;

// True when `haystack` contains `needle` as a substring.
bool Contains(const std::string& haystack, const std::string& needle) {
  return haystack.find(needle) != std::string::npos;
}

}  // namespace

TEST_SUITE("call_state") {
  TEST_CASE("Clean close without grpc-status maps to kUnknown") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    call.OnClose(0);

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kUnknown);
    CHECK(Contains(r.message, "without grpc-status"));
  }

  TEST_CASE("Malformed grpc-status in trailers-only response maps to kUnknown") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "200"},
                            {"content-type", "application/grpc"},
                            {"grpc-status", "abc"},
                            {"grpc-message", "boom"}},
                           /*end_stream=*/true);
    call.OnClose(0);

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kUnknown);
    CHECK(Contains(r.message, "malformed"));
  }

  TEST_CASE("Malformed grpc-status in separate trailers maps to kUnknown") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    call.OnTrailers({{"grpc-status", "4x"}});
    call.OnClose(0);

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kUnknown);
  }

  TEST_CASE("RST_STREAM error codes map per §5.5 when no headers seen") {
    // {http2_error_code, expected gRPC status}. Unlisted codes default to
    // kInternal.
    struct Case {
      uint32_t rst;
      StatusCode expected;
    };
    const Case cases[] = {
        {0x7, StatusCode::kUnavailable},        // REFUSED_STREAM
        {0x8, StatusCode::kCancelled},          // CANCEL
        {0xb, StatusCode::kResourceExhausted},  // ENHANCE_YOUR_CALM
        {0xc, StatusCode::kPermissionDenied},   // INADEQUATE_SECURITY
        {0x1, StatusCode::kInternal},           // PROTOCOL_ERROR (default)
        {0x2, StatusCode::kInternal},           // INTERNAL_ERROR (default)
        {0xd, StatusCode::kInternal},           // HTTP_1_1_REQUIRED (default)
    };
    for (const Case& c : cases) {
      CAPTURE(c.rst);
      CallState call(kMax);
      call.OnClose(c.rst);
      const CallState::Result& r = call.Wait();
      CHECK(r.code == c.expected);
    }
  }

  TEST_CASE("grpc-status in trailers wins over RST error code") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    call.OnTrailers({{"grpc-status", "5"}});
    call.OnClose(0x8);  // CANCEL — would be kCancelled if it won

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kNotFound);
  }

  TEST_CASE("Non-200 HTTP status wins over RST when grpc-status absent") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "404"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    call.OnClose(0x8);  // CANCEL — would be kCancelled if RST won

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kUnimplemented);
  }

  TEST_CASE("Bad content-type on 200 without grpc-status maps to kInternal") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "text/html"}},
                           /*end_stream=*/false);
    call.OnClose(0);

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kInternal);
  }

  TEST_CASE("Happy path: framed message and grpc-status 0 yields kOk") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    // One gRPC-framed message: flag 0x00 + 4-byte big-endian length 2 + "hi".
    const uint8_t frame[] = {0, 0, 0, 0, 2, 'h', 'i'};
    call.OnData(frame, sizeof(frame));
    call.OnTrailers({{"grpc-status", "0"}});
    call.OnClose(0);

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kOk);
    CHECK(r.response == "hi");
  }
}  // TEST_SUITE("call_state")
