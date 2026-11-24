// egrpc native channel configuration API (design §4.6).
//
// egrpc::ChannelConfig exposes every egrpc channel knob with its spec default;
// egrpc::CreateChannel builds a grpc::Channel usable with unmodified
// generated stubs. TLS-only (v0.1).
#pragma once

#include <grpcpp/channel.h>

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>

namespace egrpc {

struct ChannelConfig {
  // Upper bound (1 year) accepted by Validate() for every duration field.
  // Keeps internal nanosecond/steady_clock arithmetic (jitter, doubling,
  // now() + duration) far below int64_t overflow.
  static constexpr std::chrono::milliseconds kMaxDuration{365LL * 24 * 60 * 60 * 1000};

  // --- TLS -----------------------------------------------------------------
  // Root CA bundle: file path, or inline PEM (wins when both are set). When
  // neither is set, the system default trust store is used.
  std::string ca_bundle_path;
  std::string ca_bundle_pem;
  // Optional mTLS client identity (PEM file paths; both or neither).
  std::string client_cert_path;
  std::string client_key_path;
  // TEST RIGS ONLY: skips certificate chain and hostname verification.
  bool insecure_skip_verify_for_testing_only = false;

  // --- HTTP/2 & keepalive (§4.2, §5.2) -------------------------------------
  uint32_t max_concurrent_streams = 8;
  std::chrono::milliseconds keepalive_time{300000};
  std::chrono::milliseconds keepalive_timeout{20000};
  bool keepalive_permit_without_calls = true;

  // --- Backoff & timeouts (§4.2, §5.3, §5.7) -------------------------------
  // Per-attempt connect deadline (unified with gRPC's MIN_CONNECT_TIMEOUT).
  std::chrono::milliseconds connect_timeout{20000};
  std::chrono::milliseconds initial_reconnect_backoff{1000};
  double reconnect_backoff_multiplier = 1.6;
  std::chrono::milliseconds max_reconnect_backoff{120000};
  double reconnect_backoff_jitter = 0.2;
  // Backoff resets only after a connection has held READY this long.
  std::chrono::milliseconds ready_stability_threshold{60000};
  // Bound on draining streams after GOAWAY before the connection is replaced.
  std::chrono::milliseconds goaway_drain_timeout{5000};
  // Total budget for channel shutdown.
  std::chrono::milliseconds shutdown_timeout{5000};

  // --- Message & queue bounds (§4.3, §5.4) ---------------------------------
  size_t max_receive_message_size = 4 * 1024 * 1024;
  size_t max_send_message_size = std::numeric_limits<size_t>::max();
  size_t stream_window_slack = 64 * 1024;
  size_t max_queued_messages = 16;
  // 0 defaults to the derived stream window.
  size_t max_queued_bytes = 0;

  // --- Thread & identity (§3, §5.1) ----------------------------------------
  // EventThread stack size; 0 = platform default.
  size_t event_thread_stack_size = 128 * 1024;
  // Prepended to the built-in user-agent. Must not contain ASCII control
  // characters (0x00-0x1f, 0x7f).
  std::string user_agent_prefix;

  // Named profile for cellular links (§5.2): keepalive_time = 60 s,
  // keepalive_timeout = 20 s, keepalive_permit_without_calls = true; every
  // other field keeps its default.
  //
  // Server-side requirements: the upstream gRPC server must be configured
  // with GRPC_ARG_HTTP2_MIN_RECV_PING_INTERVAL_WITHOUT_DATA_MS <= 60000 and
  // GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS = 1. Otherwise the server treats
  // the pings as abusive and sends GOAWAY(ENHANCE_YOUR_CALM, "too_many_pings"),
  // after which egrpc doubles keepalive_time for subsequent connections.
  static ChannelConfig CellularProfile();

  // Returns true when the configuration is usable. Every duration field must
  // be <= kMaxDuration. On failure, returns false
  // and (if `error` is non-null) stores a human-readable reason.
  bool Validate(std::string* error = nullptr) const;
};

// Creates a TLS channel to `target` ("host", "host:port", "dns:///host:port",
// or bracketed IPv6 "[addr]:port"; default port 443). An invalid config or
// target yields a non-null lame channel whose calls fail with
// INVALID_ARGUMENT.
std::shared_ptr<grpc::Channel> CreateChannel(const std::string& target,
                                             const ChannelConfig& config = ChannelConfig());

}  // namespace egrpc
