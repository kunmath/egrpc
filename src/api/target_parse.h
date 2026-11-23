// egrpc — channel target parsing shared by grpc::CreateCustomChannel and
// egrpc::CreateChannel.
#pragma once

#include <cstdint>
#include <string>

namespace egrpc {
namespace internal {

// Accepts "host:port", "host" (⇒ 443), optionally prefixed "dns:///", and
// bracketed IPv6 ("[::1]:443"). Rejects other schemes, port 0, and ports
// above 65535. Returns false on any parse failure.
bool ParseChannelTarget(const std::string& target, std::string* host, uint16_t* port);

}  // namespace internal
}  // namespace egrpc
