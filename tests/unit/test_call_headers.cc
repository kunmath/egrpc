// egrpc — unit tests for the request-header builder (design §5.1) and the
// channel's send-size bound (§4.5), the two caller-facing contracts M4's
// shim depends on. Suite "call_headers", wired into ctest via
// egrpc_unit_tests.

#include <algorithm>
#include <optional>
#include <string>

#include "core/call_state.h"
#include "core/channel_impl.h"
#include "doctest/doctest.h"

using egrpc::internal::BuildRequestHeaders;
using egrpc::internal::ChannelImpl;
using egrpc::internal::ChannelOptions;
using egrpc::internal::Http2Session;
using egrpc::internal::StatusCode;
using egrpc::internal::TlsConfig;

namespace {

std::string Find(const Http2Session::HeaderList& headers, const std::string& key) {
  for (const auto& kv : headers) {
    if (kv.first == key) return kv.second;
  }
  return "<missing>";
}

size_t Count(const Http2Session::HeaderList& headers, const std::string& key) {
  return static_cast<size_t>(std::count_if(headers.begin(), headers.end(),
                                           [&](const auto& kv) { return kv.first == key; }));
}

}  // namespace

TEST_SUITE("call_headers") {
  TEST_CASE("metadata keys are lowercased and -bin values base64-encoded unpadded") {
    const auto headers =
        BuildRequestHeaders("h:443", "/pkg.Svc/M", std::nullopt,
                            {{"X-Custom", "v"}, {"Trace-Bin", std::string("\x00\x01", 2)}});
    CHECK(Find(headers, "x-custom") == "v");
    // 0x00 0x01 → "AAE" (unpadded; padded form would be "AAE=").
    CHECK(Find(headers, "trace-bin") == "AAE");
  }

  TEST_CASE("reserved and pseudo-header metadata keys are dropped, not duplicated") {
    const auto headers = BuildRequestHeaders("h:443", "/pkg.Svc/M", std::nullopt,
                                             {{"te", "gzip"},
                                              {"Content-Type", "text/plain"},
                                              {":authority", "evil"},
                                              {"grpc-timeout", "1S"},
                                              {"ok-key", "kept"}});
    CHECK(Count(headers, "te") == 1);
    CHECK(Find(headers, "te") == "trailers");
    CHECK(Count(headers, "content-type") == 1);
    CHECK(Find(headers, "content-type") == "application/grpc");
    CHECK(Count(headers, ":authority") == 1);
    CHECK(Find(headers, ":authority") == "h:443");
    CHECK(Count(headers, "grpc-timeout") == 0);  // no timeout was set
    CHECK(Find(headers, "ok-key") == "kept");
  }

  TEST_CASE("user_agent_prefix is prepended space-separated") {
    const auto plain = BuildRequestHeaders("h:443", "/pkg.Svc/M", std::nullopt, {});
    const auto prefixed = BuildRequestHeaders("h:443", "/pkg.Svc/M", std::nullopt, {}, "my-app/2");
    const std::string base = Find(plain, "user-agent");
    CHECK(base.find("grpc-c++-egrpc/") == 0);
    CHECK(Find(prefixed, "user-agent") == "my-app/2 " + base);
  }

  TEST_CASE("oversize request fails with kResourceExhausted before any I/O") {
    ChannelOptions options;
    options.max_send_message_size = 8;
    // Port 1 on localhost: the check must fire before any connect attempt,
    // so the bogus target is never touched.
    ChannelImpl channel("127.0.0.1", 1, TlsConfig{}, options);
    const auto result = channel.UnaryCall("/pkg.Svc/M", std::string(9, 'x'));
    CHECK(result.code == StatusCode::kResourceExhausted);
    CHECK(result.message == "Sent message larger than max (9 vs. 8)");
  }
}
