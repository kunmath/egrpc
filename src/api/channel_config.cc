// egrpc native API — ChannelConfig presets/validation and egrpc::CreateChannel
// (design §4.6).

#include "egrpc/channel_config.h"

#include <cmath>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "api/target_parse.h"
#include "core/channel_impl.h"

namespace egrpc {
namespace {

bool Fail(std::string* error, const char* message) {
  if (error != nullptr) *error = message;
  return false;
}

}  // namespace

ChannelConfig ChannelConfig::CellularProfile() {
  ChannelConfig config;
  config.keepalive_time = std::chrono::seconds(60);
  config.keepalive_timeout = std::chrono::seconds(20);
  config.keepalive_permit_without_calls = true;
  return config;
}

bool ChannelConfig::Validate(std::string* error) const {
  using std::chrono::milliseconds;
  if (client_cert_path.empty() != client_key_path.empty()) {
    return Fail(error, "client_cert_path and client_key_path must be set together");
  }
  if (max_concurrent_streams == 0) return Fail(error, "max_concurrent_streams must be > 0");
  if (keepalive_time <= milliseconds(0)) return Fail(error, "keepalive_time must be > 0");
  if (keepalive_timeout <= milliseconds(0)) return Fail(error, "keepalive_timeout must be > 0");
  if (connect_timeout <= milliseconds(0)) return Fail(error, "connect_timeout must be > 0");
  if (initial_reconnect_backoff <= milliseconds(0)) {
    return Fail(error, "initial_reconnect_backoff must be > 0");
  }
  if (max_reconnect_backoff < initial_reconnect_backoff) {
    return Fail(error, "max_reconnect_backoff must be >= initial_reconnect_backoff");
  }
  if (!std::isfinite(reconnect_backoff_multiplier) || reconnect_backoff_multiplier < 1.0) {
    return Fail(error, "reconnect_backoff_multiplier must be finite and >= 1.0");
  }
  if (!std::isfinite(reconnect_backoff_jitter) || reconnect_backoff_jitter < 0.0 ||
      reconnect_backoff_jitter > 1.0) {
    return Fail(error, "reconnect_backoff_jitter must be within [0.0, 1.0]");
  }
  if (ready_stability_threshold < milliseconds(0)) {
    return Fail(error, "ready_stability_threshold must be >= 0");
  }
  if (goaway_drain_timeout < milliseconds(0)) {
    return Fail(error, "goaway_drain_timeout must be >= 0");
  }
  if (shutdown_timeout < milliseconds(0)) return Fail(error, "shutdown_timeout must be >= 0");
  if (max_receive_message_size == 0) return Fail(error, "max_receive_message_size must be > 0");
  if (max_send_message_size == 0) return Fail(error, "max_send_message_size must be > 0");
  if (max_queued_messages == 0) return Fail(error, "max_queued_messages must be > 0");
  if (user_agent_prefix.find_first_of(std::string("\r\n\0", 3)) != std::string::npos) {
    return Fail(error, "user_agent_prefix must not contain CR, LF, or NUL");
  }
  return true;
}

std::shared_ptr<grpc::Channel> CreateChannel(const std::string& target,
                                             const ChannelConfig& config) {
  std::string error;
  if (!config.Validate(&error)) {
    return grpc::Channel::Lame(
        grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "invalid ChannelConfig: " + error));
  }
  std::string host;
  uint16_t port = 0;
  if (!internal::ParseChannelTarget(target, &host, &port)) {
    return grpc::Channel::Lame(
        grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "unsupported channel target: " + target));
  }

  internal::TlsConfig tls;
  tls.ca_bundle_path = config.ca_bundle_path;
  tls.ca_bundle_pem = config.ca_bundle_pem;
  tls.client_cert_path = config.client_cert_path;
  tls.client_key_path = config.client_key_path;
  tls.insecure_skip_verify_for_testing_only = config.insecure_skip_verify_for_testing_only;

  internal::ChannelOptions options;
  options.http2.max_concurrent_streams = config.max_concurrent_streams;
  options.http2.keepalive_time = config.keepalive_time;
  options.http2.keepalive_timeout = config.keepalive_timeout;
  options.http2.keepalive_permit_without_calls = config.keepalive_permit_without_calls;
  options.max_receive_message_size = config.max_receive_message_size;
  options.max_send_message_size = config.max_send_message_size;
  options.stream_window_slack = config.stream_window_slack;
  options.max_queued_messages = config.max_queued_messages;
  options.max_queued_bytes = config.max_queued_bytes;
  options.connect_timeout = config.connect_timeout;
  options.initial_reconnect_backoff = config.initial_reconnect_backoff;
  options.reconnect_backoff_multiplier = config.reconnect_backoff_multiplier;
  options.max_reconnect_backoff = config.max_reconnect_backoff;
  options.reconnect_backoff_jitter = config.reconnect_backoff_jitter;
  options.ready_stability_threshold = config.ready_stability_threshold;
  options.goaway_drain_timeout = config.goaway_drain_timeout;
  options.shutdown_timeout = config.shutdown_timeout;
  options.event_thread_stack_size = config.event_thread_stack_size;
  options.user_agent_prefix = config.user_agent_prefix;

  return grpc::Channel::FromImpl(
      std::make_unique<internal::ChannelImpl>(host, port, std::move(tls), std::move(options)));
}

}  // namespace egrpc
