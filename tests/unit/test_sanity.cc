// M0 sanity suite for egrpc unit tests.
//
// The library is still empty stubs at M0, so the frame-length helper below is
// defined locally inside this test file. It is a taste of the gRPC message
// framing described in the design doc §5.1:
//     [1-byte compressed flag][4-byte big-endian length][payload]
// Here we exercise only the 4-byte big-endian length field: pack/unpack a
// uint32_t and assert exact byte layout plus round-trip equality.

#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <array>
#include <cstdint>

#include "doctest/doctest.h"

namespace {

// Pack a uint32_t into 4 big-endian (network-order) bytes.
std::array<std::uint8_t, 4> pack_be32(std::uint32_t value) {
  return {
      static_cast<std::uint8_t>((value >> 24) & 0xFFu),
      static_cast<std::uint8_t>((value >> 16) & 0xFFu),
      static_cast<std::uint8_t>((value >> 8) & 0xFFu),
      static_cast<std::uint8_t>(value & 0xFFu),
  };
}

// Unpack 4 big-endian bytes back into a uint32_t.
std::uint32_t unpack_be32(const std::array<std::uint8_t, 4>& bytes) {
  return (static_cast<std::uint32_t>(bytes[0]) << 24) |
         (static_cast<std::uint32_t>(bytes[1]) << 16) |
         (static_cast<std::uint32_t>(bytes[2]) << 8) | (static_cast<std::uint32_t>(bytes[3]));
}

}  // namespace

TEST_CASE("basic arithmetic sanity") {
  CHECK(1 + 1 == 2);
  CHECK(2 * 3 == 6);
  CHECK(7 % 4 == 3);
  CHECK((1 << 4) == 16);
}

TEST_CASE("big-endian uint32 pack/unpack (grpc frame length field)") {
  SUBCASE("exact byte layout") {
    const auto bytes = pack_be32(0x01020304u);
    CHECK(bytes[0] == 0x01);
    CHECK(bytes[1] == 0x02);
    CHECK(bytes[2] == 0x03);
    CHECK(bytes[3] == 0x04);
  }

  SUBCASE("round-trip: representative value") {
    const std::uint32_t value = 0x01020304u;
    CHECK(unpack_be32(pack_be32(value)) == value);
  }

  SUBCASE("edge case: zero") {
    const auto bytes = pack_be32(0u);
    CHECK(bytes[0] == 0x00);
    CHECK(bytes[1] == 0x00);
    CHECK(bytes[2] == 0x00);
    CHECK(bytes[3] == 0x00);
    CHECK(unpack_be32(bytes) == 0u);
  }

  SUBCASE("edge case: all bits set") {
    const auto bytes = pack_be32(0xFFFFFFFFu);
    CHECK(bytes[0] == 0xFF);
    CHECK(bytes[1] == 0xFF);
    CHECK(bytes[2] == 0xFF);
    CHECK(bytes[3] == 0xFF);
    CHECK(unpack_be32(bytes) == 0xFFFFFFFFu);
  }
}
