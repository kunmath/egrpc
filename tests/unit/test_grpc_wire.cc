// egrpc — unit tests for the gRPC wire-format helpers (design §5.1, §5.5).
//
// grpc_wire.h is a set of pure functions and a single-threaded scanner value
// type: no sockets, no clocks, no locks (Ring 1, design §7). These tests drive
// the public API directly with hand-built byte buffers and synthetic inputs.
//
// All cases live in the "grpc_wire" doctest test suite so ctest can run just
// them (see tests/unit/CMakeLists.txt: the egrpc_wire_tests registration).
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <string>

#include "core/grpc_wire.h"
#include "doctest/doctest.h"

namespace {

using egrpc::internal::AppendGrpcFramePrefix;
using egrpc::internal::Base64Decode;
using egrpc::internal::Base64Encode;
using egrpc::internal::EncodeGrpcTimeout;
using egrpc::internal::GrpcMessageScanner;
using egrpc::internal::HttpStatusToGrpcCode;
using egrpc::internal::ParseGrpcStatus;
using egrpc::internal::PercentDecode;
using egrpc::internal::StatusCode;

// Feeds a std::string chunk into the scanner.
bool Feed(GrpcMessageScanner* s, const std::string& chunk) {
  return s->Feed(reinterpret_cast<const uint8_t*>(chunk.data()), chunk.size());
}

// Builds a complete on-the-wire frame (5-byte prefix + payload) for `payload`.
std::string Frame(const std::string& payload) {
  std::string out;
  AppendGrpcFramePrefix(payload.size(), &out);
  out += payload;
  return out;
}

// Builds a raw 5-byte prefix with an arbitrary flag byte and declared length,
// so tests can inject malformed/oversize headers the framing API won't produce.
std::string RawPrefix(uint8_t flag, uint32_t declared_len) {
  std::string out;
  out.push_back(static_cast<char>(flag));
  out.push_back(static_cast<char>((declared_len >> 24) & 0xff));
  out.push_back(static_cast<char>((declared_len >> 16) & 0xff));
  out.push_back(static_cast<char>((declared_len >> 8) & 0xff));
  out.push_back(static_cast<char>(declared_len & 0xff));
  return out;
}

constexpr size_t kMax = 1024;

}  // namespace

TEST_SUITE("grpc_wire") {
  // --- GrpcMessageScanner -----------------------------------------------------

  TEST_CASE("Scanner: one complete message in one Feed") {
    GrpcMessageScanner s(kMax);
    CHECK(Feed(&s, Frame("hello")));

    std::string msg;
    REQUIRE(s.Next(&msg));
    CHECK(msg == "hello");
    CHECK_FALSE(s.Next(&msg));
    CHECK_FALSE(s.HasPartialMessage());
    CHECK_FALSE(s.failed());
  }

  TEST_CASE("Scanner: round-trips a payload framed by AppendGrpcFramePrefix") {
    const std::string payload = "the quick brown fox";
    std::string wire;
    AppendGrpcFramePrefix(payload.size(), &wire);
    wire += payload;
    CHECK(wire.size() == payload.size() + 5);

    GrpcMessageScanner s(kMax);
    CHECK(Feed(&s, wire));
    std::string msg;
    REQUIRE(s.Next(&msg));
    CHECK(msg == payload);
  }

  TEST_CASE("Scanner: empty payload (length 0) is a valid message") {
    GrpcMessageScanner s(kMax);
    CHECK(Feed(&s, Frame("")));

    std::string msg = "sentinel";
    REQUIRE(s.Next(&msg));
    CHECK(msg.empty());
    CHECK_FALSE(s.Next(&msg));
    CHECK_FALSE(s.HasPartialMessage());
  }

  TEST_CASE("Scanner: message split across many Feeds (byte by byte)") {
    const std::string wire = Frame("payload-data");
    GrpcMessageScanner s(kMax);
    std::string msg;

    for (size_t i = 0; i + 1 < wire.size(); ++i) {
      CHECK(s.Feed(reinterpret_cast<const uint8_t*>(&wire[i]), 1));
      CHECK_FALSE(s.Next(&msg));  // never complete until the last byte
    }
    // Final byte completes the frame.
    CHECK(s.Feed(reinterpret_cast<const uint8_t*>(&wire.back()), 1));
    REQUIRE(s.Next(&msg));
    CHECK(msg == "payload-data");
    CHECK_FALSE(s.Next(&msg));
  }

  TEST_CASE("Scanner: 5-byte prefix itself split across feeds") {
    const std::string wire = Frame("abc");  // prefix (5) + payload (3)
    GrpcMessageScanner s(kMax);
    std::string msg;

    CHECK(Feed(&s, wire.substr(0, 3)));  // first 3 prefix bytes
    CHECK_FALSE(s.Next(&msg));
    CHECK(Feed(&s, wire.substr(3, 2)));  // remaining 2 prefix bytes
    CHECK_FALSE(s.Next(&msg));
    CHECK(Feed(&s, wire.substr(5)));  // payload
    REQUIRE(s.Next(&msg));
    CHECK(msg == "abc");
  }

  TEST_CASE("Scanner: two coalesced messages in one Feed") {
    GrpcMessageScanner s(kMax);
    CHECK(Feed(&s, Frame("first") + Frame("second")));

    std::string msg;
    REQUIRE(s.Next(&msg));
    CHECK(msg == "first");
    REQUIRE(s.Next(&msg));
    CHECK(msg == "second");
    CHECK_FALSE(s.Next(&msg));
    CHECK_FALSE(s.HasPartialMessage());
  }

  TEST_CASE("Scanner: message spanning feeds, second message in final chunk") {
    const std::string a = Frame("alpha");
    const std::string b = Frame("beta");
    GrpcMessageScanner s(kMax);
    std::string msg;

    // Split the first message; the tail of a arrives together with all of b.
    CHECK(Feed(&s, a.substr(0, 4)));
    CHECK_FALSE(s.Next(&msg));
    CHECK(Feed(&s, a.substr(4) + b));

    REQUIRE(s.Next(&msg));
    CHECK(msg == "alpha");
    REQUIRE(s.Next(&msg));
    CHECK(msg == "beta");
    CHECK_FALSE(s.Next(&msg));
  }

  TEST_CASE("Scanner: compressed flag=1 fails with kInternal") {
    GrpcMessageScanner s(kMax);
    // Flag byte + 4 length bytes (declared length 0) is enough to trigger.
    CHECK_FALSE(Feed(&s, RawPrefix(1, 0)));
    CHECK(s.failed());
    CHECK(s.error_code() == StatusCode::kInternal);
    CHECK_FALSE(s.error().empty());
  }

  TEST_CASE("Scanner: invalid flag byte fails with kInternal") {
    GrpcMessageScanner s(kMax);
    CHECK_FALSE(Feed(&s, RawPrefix(7, 0)));
    CHECK(s.failed());
    CHECK(s.error_code() == StatusCode::kInternal);
  }

  TEST_CASE("Scanner: oversize declared length rejected before buffering payload") {
    GrpcMessageScanner s(kMax);
    // Declared length = max+1, only the 5-byte prefix fed (no payload yet).
    CHECK_FALSE(Feed(&s, RawPrefix(0, static_cast<uint32_t>(kMax + 1))));
    CHECK(s.failed());
    CHECK(s.error_code() == StatusCode::kResourceExhausted);
  }

  TEST_CASE("Scanner: oversize prefix after a complete message in same Feed") {
    GrpcMessageScanner s(kMax);
    // A valid message immediately followed by an oversize prefix: prefix
    // validation walks every complete prefix, so the whole Feed fails.
    CHECK_FALSE(Feed(&s, Frame("ok") + RawPrefix(0, static_cast<uint32_t>(kMax + 1))));
    CHECK(s.failed());
    CHECK(s.error_code() == StatusCode::kResourceExhausted);
  }

  TEST_CASE("Scanner: truncated frame at end of stream leaves a partial message") {
    GrpcMessageScanner s(kMax);
    // One complete message plus the start of a second (prefix only, no payload).
    CHECK(Feed(&s, Frame("done") + RawPrefix(0, 10)));

    std::string msg;
    REQUIRE(s.Next(&msg));
    CHECK(msg == "done");
    CHECK_FALSE(s.Next(&msg));     // second is incomplete
    CHECK(s.HasPartialMessage());  // truncated bytes remain buffered
  }

  TEST_CASE("Scanner: after a failure, Feed and Next both return false") {
    GrpcMessageScanner s(kMax);
    CHECK_FALSE(Feed(&s, RawPrefix(1, 0)));  // terminal error
    CHECK(s.failed());

    std::string msg;
    CHECK_FALSE(Feed(&s, Frame("more")));
    CHECK_FALSE(s.Next(&msg));
    // Error state is sticky.
    CHECK(s.error_code() == StatusCode::kInternal);
  }

  TEST_CASE("Scanner: message of exactly max_message_size is accepted") {
    GrpcMessageScanner s(kMax);
    const std::string payload(kMax, 'x');
    CHECK(Feed(&s, Frame(payload)));
    CHECK_FALSE(s.failed());

    std::string msg;
    REQUIRE(s.Next(&msg));
    CHECK(msg.size() == kMax);
    CHECK(msg == payload);
  }

  // --- EncodeGrpcTimeout ------------------------------------------------------

  TEST_CASE("EncodeGrpcTimeout: non-positive timeouts encode as 1n") {
    CHECK(EncodeGrpcTimeout(std::chrono::nanoseconds(0)) == "1n");
    CHECK(EncodeGrpcTimeout(std::chrono::nanoseconds(-1)) == "1n");
    CHECK(EncodeGrpcTimeout(std::chrono::nanoseconds(-1000000)) == "1n");
  }

  TEST_CASE("EncodeGrpcTimeout: nanosecond range fits 8 digits") {
    CHECK(EncodeGrpcTimeout(std::chrono::nanoseconds(1)) == "1n");
    CHECK(EncodeGrpcTimeout(std::chrono::nanoseconds(99999999)) == "99999999n");
  }

  TEST_CASE("EncodeGrpcTimeout: exact conversion does not inflate") {
    // 1e8 ns = 100000 us exactly -> no round-up.
    CHECK(EncodeGrpcTimeout(std::chrono::nanoseconds(100000000)) == "100000u");
  }

  TEST_CASE("EncodeGrpcTimeout: sub-unit remainder rounds up") {
    // 1e8 + 1 ns rounds up to 100001 us so the wire deadline is never earlier.
    CHECK(EncodeGrpcTimeout(std::chrono::nanoseconds(100000001)) == "100001u");
  }

  TEST_CASE("EncodeGrpcTimeout: larger units chosen to fit 8 digits") {
    CHECK(EncodeGrpcTimeout(std::chrono::hours(1)) == "3600000m");
    CHECK(EncodeGrpcTimeout(std::chrono::hours(100)) == "360000S");
  }

  TEST_CASE("EncodeGrpcTimeout: very large value clamps to hours, fits 8 digits") {
    const std::string enc = EncodeGrpcTimeout(std::chrono::nanoseconds::max());
    REQUIRE_FALSE(enc.empty());
    CHECK(enc.back() == 'H');
    const std::string digits = enc.substr(0, enc.size() - 1);
    CHECK(digits.size() <= 8);
    CHECK(digits.size() >= 1);
    for (char c : digits) CHECK((c >= '0' && c <= '9'));
  }

  // --- PercentDecode ----------------------------------------------------------

  TEST_CASE("PercentDecode: valid sequences") {
    CHECK(PercentDecode("Hello%20world") == "Hello world");
    // Multi-byte UTF-8 decodes bytewise: %e4%bd%a0.
    const std::string ni = PercentDecode("%e4%bd%a0");
    REQUIRE(ni.size() == 3);
    CHECK(static_cast<uint8_t>(ni[0]) == 0xe4);
    CHECK(static_cast<uint8_t>(ni[1]) == 0xbd);
    CHECK(static_cast<uint8_t>(ni[2]) == 0xa0);
    // Upper- and lower-case hex both decode.
    CHECK(PercentDecode("%2F") == "/");
    CHECK(PercentDecode("%2f") == "/");
    CHECK(PercentDecode("a%41b%42c") == "aAbBc");
  }

  TEST_CASE("PercentDecode: invalid/truncated sequences pass through verbatim") {
    CHECK(PercentDecode("%zz") == "%zz");
    CHECK(PercentDecode("bad%2") == "bad%2");          // truncated at end
    CHECK(PercentDecode("trailing%") == "trailing%");  // lone % at end
    CHECK(PercentDecode("plain text") == "plain text");
    CHECK(PercentDecode("") == "");
  }

  // --- Base64Encode / Base64Decode --------------------------------------------

  TEST_CASE("Base64Encode: RFC 4648 vectors, no padding") {
    CHECK(Base64Encode("") == "");
    CHECK(Base64Encode("f") == "Zg");
    CHECK(Base64Encode("fo") == "Zm8");
    CHECK(Base64Encode("foo") == "Zm9v");
    CHECK(Base64Encode("foob") == "Zm9vYg");
    CHECK(Base64Encode("fooba") == "Zm9vYmE");
    CHECK(Base64Encode("foobar") == "Zm9vYmFy");
  }

  TEST_CASE("Base64Decode: accepts padded and unpadded input") {
    std::string out;
    REQUIRE(Base64Decode("Zg", &out));
    CHECK(out == "f");
    REQUIRE(Base64Decode("Zg==", &out));
    CHECK(out == "f");
    REQUIRE(Base64Decode("Zm8", &out));
    CHECK(out == "fo");
    REQUIRE(Base64Decode("Zm8=", &out));
    CHECK(out == "fo");
    REQUIRE(Base64Decode("Zm9vYmFy", &out));
    CHECK(out == "foobar");
    REQUIRE(Base64Decode("", &out));
    CHECK(out == "");
  }

  TEST_CASE("Base64Decode: round-trips binary data with 0x00 and 0xff") {
    std::string bin;
    for (int i = 0; i < 256; ++i) bin.push_back(static_cast<char>(i));
    const std::string enc = Base64Encode(bin);
    std::string dec;
    REQUIRE(Base64Decode(enc, &dec));
    CHECK(dec == bin);
  }

  TEST_CASE("Base64Decode: rejects impossible length and non-alphabet chars") {
    std::string out;
    CHECK_FALSE(Base64Decode("Z", &out));     // len%4==1
    CHECK_FALSE(Base64Decode("Zg=a", &out));  // '=' mid-string
    CHECK_FALSE(Base64Decode("a!b", &out));   // '!' not in alphabet
  }

  // --- ParseGrpcStatus --------------------------------------------------------

  TEST_CASE("ParseGrpcStatus: valid codes map to their enum") {
    StatusCode code = StatusCode::kUnknown;
    REQUIRE(ParseGrpcStatus("0", &code));
    CHECK(code == StatusCode::kOk);
    REQUIRE(ParseGrpcStatus("16", &code));
    CHECK(code == StatusCode::kUnauthenticated);
    REQUIRE(ParseGrpcStatus("12", &code));
    CHECK(code == StatusCode::kUnimplemented);
  }

  TEST_CASE("ParseGrpcStatus: valid but out-of-range integers map to kUnknown") {
    StatusCode code = StatusCode::kOk;
    REQUIRE(ParseGrpcStatus("17", &code));
    CHECK(code == StatusCode::kUnknown);
    code = StatusCode::kOk;
    REQUIRE(ParseGrpcStatus("100", &code));
    CHECK(code == StatusCode::kUnknown);
  }

  TEST_CASE("ParseGrpcStatus: malformed values are rejected") {
    StatusCode code = StatusCode::kOk;
    CHECK_FALSE(ParseGrpcStatus("", &code));
    CHECK_FALSE(ParseGrpcStatus("abc", &code));
    CHECK_FALSE(ParseGrpcStatus("1 ", &code));
    CHECK_FALSE(ParseGrpcStatus(" 1", &code));
    CHECK_FALSE(ParseGrpcStatus("-1", &code));
    CHECK_FALSE(ParseGrpcStatus("+1", &code));
    // >10 characters is rejected outright.
    CHECK_FALSE(ParseGrpcStatus("12345678901", &code));
  }

  // --- HttpStatusToGrpcCode ---------------------------------------------------

  TEST_CASE("HttpStatusToGrpcCode: mapping table (design §5.5)") {
    CHECK(HttpStatusToGrpcCode(400) == StatusCode::kInternal);
    CHECK(HttpStatusToGrpcCode(401) == StatusCode::kUnauthenticated);
    CHECK(HttpStatusToGrpcCode(403) == StatusCode::kPermissionDenied);
    CHECK(HttpStatusToGrpcCode(404) == StatusCode::kUnimplemented);
    CHECK(HttpStatusToGrpcCode(429) == StatusCode::kUnavailable);
    CHECK(HttpStatusToGrpcCode(502) == StatusCode::kUnavailable);
    CHECK(HttpStatusToGrpcCode(503) == StatusCode::kUnavailable);
    CHECK(HttpStatusToGrpcCode(504) == StatusCode::kUnavailable);
    // Unlisted codes fall through to kUnknown.
    CHECK(HttpStatusToGrpcCode(500) == StatusCode::kUnknown);
    CHECK(HttpStatusToGrpcCode(200) == StatusCode::kUnknown);
  }

}  // TEST_SUITE("grpc_wire")
