// egrpc grpcpp shim — grpc::CreateChannel / CreateCustomChannel (design
// §4.5). Maps credentials onto TlsConfig and the honored ChannelArguments
// (keepalive §5.2, max receive size, primary user-agent) onto
// ChannelOptions; everything unsupported yields a lame channel with a
// descriptive status instead of failing at call time with less context.

#include <grpcpp/create_channel.h>

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "core/channel_impl.h"

namespace grpc {
namespace {

// "host:port", "host" (⇒ 443), optionally prefixed dns:///; bracketed IPv6.
bool ParseTarget(const std::string& target, std::string* host, uint16_t* port) {
  std::string rest = target;
  constexpr char kDnsPrefix[] = "dns:///";
  if (rest.compare(0, sizeof(kDnsPrefix) - 1, kDnsPrefix) == 0) {
    rest = rest.substr(sizeof(kDnsPrefix) - 1);
  } else if (rest.find("://") != std::string::npos || rest.compare(0, 5, "unix:") == 0) {
    return false;  // other resolver schemes are out of scope (design §2)
  }
  if (rest.empty()) return false;

  std::string port_part;
  if (rest[0] == '[') {  // bracketed IPv6: [::1]:443
    const size_t close = rest.find(']');
    if (close == std::string::npos) return false;
    *host = rest.substr(1, close - 1);
    if (close + 1 < rest.size()) {
      if (rest[close + 1] != ':') return false;
      port_part = rest.substr(close + 2);
    }
  } else {
    const size_t colon = rest.rfind(':');
    if (colon == std::string::npos) {
      *host = rest;
    } else {
      if (rest.find(':') != colon) return false;  // unbracketed IPv6
      *host = rest.substr(0, colon);
      port_part = rest.substr(colon + 1);
    }
  }
  if (host->empty()) return false;

  if (port_part.empty()) {
    *port = 443;
    return true;
  }
  uint32_t value = 0;
  for (char c : port_part) {
    if (c < '0' || c > '9') return false;
    value = value * 10 + static_cast<uint32_t>(c - '0');
    if (value > 65535) return false;
  }
  if (value == 0) return false;
  *port = static_cast<uint16_t>(value);
  return true;
}

}  // namespace

std::shared_ptr<Channel> CreateCustomChannel(const std::string& target,
                                             const std::shared_ptr<ChannelCredentials>& creds,
                                             const ChannelArguments& args) {
  if (!creds) {
    return Channel::Lame(Status(StatusCode::INVALID_ARGUMENT, "null channel credentials"));
  }
  if (!creds->egrpc_secure()) {
    return Channel::Lame(Status(StatusCode::UNIMPLEMENTED,
                                "insecure channels are not supported (egrpc v0.1 is TLS-only)"));
  }
  const SslCredentialsOptions& ssl = creds->egrpc_ssl_options();
  if (!ssl.pem_private_key.empty() || !ssl.pem_cert_chain.empty()) {
    return Channel::Lame(
        Status(StatusCode::UNIMPLEMENTED,
               "in-memory mTLS client identity is not supported yet (egrpc v0.1)"));
  }

  std::string host;
  uint16_t port = 0;
  if (!ParseTarget(target, &host, &port)) {
    return Channel::Lame(
        Status(StatusCode::INVALID_ARGUMENT, "unsupported channel target: " + target));
  }

  egrpc::internal::TlsConfig tls;
  tls.ca_bundle_pem = ssl.pem_root_certs;  // empty ⇒ system trust store

  // Honored args (design §4.5): keepalive (§5.2), max receive size,
  // primary user-agent. Everything else is accepted and ignored.
  egrpc::internal::ChannelOptions options;
  const auto& ints = args.egrpc_ints();
  auto int_arg = [&ints](const char* key) -> const int* {
    auto it = ints.find(key);
    return it == ints.end() ? nullptr : &it->second;
  };
  if (const int* v = int_arg(GRPC_ARG_KEEPALIVE_TIME_MS); v && *v > 0) {
    options.http2.keepalive_time = std::chrono::milliseconds(*v);
  }
  if (const int* v = int_arg(GRPC_ARG_KEEPALIVE_TIMEOUT_MS); v && *v > 0) {
    options.http2.keepalive_timeout = std::chrono::milliseconds(*v);
  }
  if (const int* v = int_arg(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS)) {
    options.http2.keepalive_permit_without_calls = (*v != 0);
  }
  // Upstream's -1 means "unlimited"; egrpc keeps its bounded default
  // instead — the receive scanner needs a bound (design §4.3).
  if (const int* v = int_arg(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH); v && *v > 0) {
    options.max_receive_message_size = static_cast<size_t>(*v);
  }
  if (const int* v = int_arg(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH); v && *v > 0) {
    options.max_send_message_size = static_cast<size_t>(*v);
  }
  const auto& strings = args.egrpc_strings();
  if (auto it = strings.find(GRPC_ARG_PRIMARY_USER_AGENT); it != strings.end()) {
    options.user_agent_prefix = it->second;
  }

  return Channel::FromImpl(
      std::make_unique<egrpc::internal::ChannelImpl>(host, port, std::move(tls), options));
}

std::shared_ptr<Channel> CreateChannel(const std::string& target,
                                       const std::shared_ptr<ChannelCredentials>& creds) {
  return CreateCustomChannel(target, creds, ChannelArguments());
}

}  // namespace grpc
