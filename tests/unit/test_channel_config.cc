// M7 unit tests: egrpc::ChannelConfig defaults, CellularProfile(), Validate(),
// and egrpc::CreateChannel (lame channels on invalid config/target, live
// channels otherwise; design §4.6, §5.2, §5.3).

#include <chrono>
#include <cmath>
#include <functional>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "doctest/doctest.h"
#include "egrpc/channel_config.h"
#include "grpcpp/channel.h"
#include "grpcpp/client_context.h"
#include "grpcpp/impl/client_unary_call.h"
#include "grpcpp/impl/rpc_method.h"
#include "grpcpp/support/sync_stream.h"

namespace {

using std::chrono::milliseconds;

const grpc::internal::RpcMethod kUnaryMethod("/test.Svc/Unary",
                                             grpc::internal::RpcMethod::NORMAL_RPC);
const grpc::internal::RpcMethod kStreamMethod("/test.Svc/Stream",
                                              grpc::internal::RpcMethod::SERVER_STREAMING);

void CheckDefaultsExceptKeepalive(const egrpc::ChannelConfig& c) {
  CHECK(c.ca_bundle_path.empty());
  CHECK(c.ca_bundle_pem.empty());
  CHECK(c.client_cert_path.empty());
  CHECK(c.client_key_path.empty());
  CHECK_FALSE(c.insecure_skip_verify_for_testing_only);
  CHECK(c.max_concurrent_streams == 8u);
  CHECK(c.keepalive_timeout == milliseconds(20000));
  CHECK(c.keepalive_permit_without_calls);
  CHECK(c.connect_timeout == milliseconds(20000));
  CHECK(c.initial_reconnect_backoff == milliseconds(1000));
  CHECK(c.reconnect_backoff_multiplier == doctest::Approx(1.6));
  CHECK(c.max_reconnect_backoff == milliseconds(120000));
  CHECK(c.reconnect_backoff_jitter == doctest::Approx(0.2));
  CHECK(c.ready_stability_threshold == milliseconds(60000));
  CHECK(c.goaway_drain_timeout == milliseconds(5000));
  CHECK(c.shutdown_timeout == milliseconds(5000));
  CHECK(c.max_receive_message_size == size_t{4 * 1024 * 1024});
  CHECK(c.max_send_message_size == std::numeric_limits<size_t>::max());
  CHECK(c.stream_window_slack == size_t{64 * 1024});
  CHECK(c.max_queued_messages == 16u);
  CHECK(c.max_queued_bytes == 0u);
  CHECK(c.event_thread_stack_size == size_t{128 * 1024});
  CHECK(c.user_agent_prefix.empty());
}

void CheckInvalid(const std::function<void(egrpc::ChannelConfig&)>& mutate) {
  egrpc::ChannelConfig c;
  mutate(c);
  std::string err;
  CHECK_FALSE(c.Validate(&err));
  CHECK_FALSE(err.empty());
  CHECK_FALSE(c.Validate());  // null error pointer is allowed
}

void CheckValid(const std::function<void(egrpc::ChannelConfig&)>& mutate) {
  egrpc::ChannelConfig c;
  mutate(c);
  std::string err;
  CHECK(c.Validate(&err));
  CHECK(err.empty());
}

void CheckLame(const std::shared_ptr<grpc::Channel>& channel, grpc::StatusCode code) {
  REQUIRE(channel != nullptr);
  {
    grpc::ClientContext ctx;
    std::string response;
    const grpc::Status s =
        grpc::internal::BlockingUnaryCallRaw(channel.get(), kUnaryMethod, &ctx, "req", &response);
    CHECK(s.error_code() == code);
    CHECK(ctx.egrpc_status_is_local());
  }
  {
    grpc::ClientContext ctx;
    std::unique_ptr<grpc::internal::ClientStreamRaw> raw =
        grpc::internal::StartServerStreamRaw(channel.get(), kStreamMethod, &ctx, "req");
    REQUIRE(raw != nullptr);
    std::string msg;
    CHECK_FALSE(raw->ReadRaw(&msg));
    const grpc::Status s = raw->Finish();
    CHECK(s.error_code() == code);
    CHECK(ctx.egrpc_status_is_local());
  }
}

void CheckLiveCancelled(std::shared_ptr<grpc::Channel> channel) {
  REQUIRE(channel != nullptr);
  {
    grpc::ClientContext ctx;
    ctx.TryCancel();
    std::string response;
    const grpc::Status s =
        grpc::internal::BlockingUnaryCallRaw(channel.get(), kUnaryMethod, &ctx, "req", &response);
    CHECK(s.error_code() == grpc::StatusCode::CANCELLED);
    CHECK(ctx.egrpc_status_is_local());
  }
  {
    grpc::ClientContext ctx;
    ctx.TryCancel();
    std::unique_ptr<grpc::internal::ClientStreamRaw> raw =
        grpc::internal::StartServerStreamRaw(channel.get(), kStreamMethod, &ctx, "req");
    REQUIRE(raw != nullptr);
    std::string msg;
    CHECK_FALSE(raw->ReadRaw(&msg));
    const grpc::Status s = raw->Finish();
    CHECK(s.error_code() == grpc::StatusCode::CANCELLED);
    CHECK(ctx.egrpc_status_is_local());
  }
  channel.reset();  // must destruct cleanly
}

}  // namespace

TEST_SUITE("channel_config") {
  TEST_CASE("defaults match the spec and validate") {
    egrpc::ChannelConfig c;
    CHECK(c.keepalive_time == milliseconds(300000));
    CheckDefaultsExceptKeepalive(c);
    std::string err;
    CHECK(c.Validate(&err));
    CHECK(c.Validate());
  }

  TEST_CASE("CellularProfile sets 60 s keepalive and preserves other defaults") {
    const egrpc::ChannelConfig c = egrpc::ChannelConfig::CellularProfile();
    CHECK(c.keepalive_time == milliseconds(60000));
    CheckDefaultsExceptKeepalive(c);
    CHECK(c.Validate());
  }

  TEST_CASE("Validate rejects invalid configurations") {
    const double nan = std::numeric_limits<double>::quiet_NaN();
    const double inf = std::numeric_limits<double>::infinity();
    CheckInvalid([](auto& c) { c.client_cert_path = "cert.pem"; });
    CheckInvalid([](auto& c) { c.client_key_path = "key.pem"; });
    CheckInvalid([](auto& c) { c.max_concurrent_streams = 0; });
    CheckInvalid([](auto& c) { c.keepalive_time = milliseconds(0); });
    CheckInvalid([](auto& c) { c.keepalive_time = milliseconds(-1); });
    CheckInvalid([](auto& c) { c.keepalive_timeout = milliseconds(0); });
    CheckInvalid([](auto& c) { c.keepalive_timeout = milliseconds(-5); });
    CheckInvalid([](auto& c) { c.connect_timeout = milliseconds(0); });
    CheckInvalid([](auto& c) { c.connect_timeout = milliseconds(-5); });
    CheckInvalid([](auto& c) { c.initial_reconnect_backoff = milliseconds(0); });
    CheckInvalid([](auto& c) { c.initial_reconnect_backoff = milliseconds(-1); });
    CheckInvalid([](auto& c) {
      c.initial_reconnect_backoff = milliseconds(2000);
      c.max_reconnect_backoff = milliseconds(1999);
    });
    CheckInvalid([](auto& c) { c.reconnect_backoff_multiplier = 0.99; });
    CheckInvalid([nan](auto& c) { c.reconnect_backoff_multiplier = nan; });
    CheckInvalid([inf](auto& c) { c.reconnect_backoff_multiplier = inf; });
    CheckInvalid([](auto& c) { c.reconnect_backoff_jitter = -0.01; });
    CheckInvalid([](auto& c) { c.reconnect_backoff_jitter = 1.01; });
    CheckInvalid([nan](auto& c) { c.reconnect_backoff_jitter = nan; });
    CheckInvalid([](auto& c) { c.ready_stability_threshold = milliseconds(-1); });
    CheckInvalid([](auto& c) { c.goaway_drain_timeout = milliseconds(-1); });
    CheckInvalid([](auto& c) { c.shutdown_timeout = milliseconds(-1); });
    CheckInvalid([](auto& c) { c.max_receive_message_size = 0; });
    CheckInvalid([](auto& c) { c.max_send_message_size = 0; });
    CheckInvalid([](auto& c) { c.max_queued_messages = 0; });
    CheckInvalid([](auto& c) { c.user_agent_prefix = "bad\rua"; });
    CheckInvalid([](auto& c) { c.user_agent_prefix = "bad\nua"; });
    CheckInvalid([](auto& c) { c.user_agent_prefix = std::string("bad\0ua", 6); });
  }

  TEST_CASE("Validate accepts boundary-valid values") {
    CheckValid([](auto& c) { c.ready_stability_threshold = milliseconds(0); });
    CheckValid([](auto& c) { c.goaway_drain_timeout = milliseconds(0); });
    CheckValid([](auto& c) { c.shutdown_timeout = milliseconds(0); });
    CheckValid([](auto& c) { c.max_queued_bytes = 0; });
    CheckValid([](auto& c) { c.event_thread_stack_size = 0; });
    CheckValid([](auto& c) { c.stream_window_slack = 0; });
    CheckValid([](auto& c) { c.reconnect_backoff_multiplier = 1.0; });
    CheckValid([](auto& c) { c.reconnect_backoff_jitter = 0.0; });
    CheckValid([](auto& c) { c.reconnect_backoff_jitter = 1.0; });
    CheckValid([](auto& c) {
      c.client_cert_path = "cert.pem";
      c.client_key_path = "key.pem";
    });
    CheckValid([](auto& c) {
      c.initial_reconnect_backoff = milliseconds(5000);
      c.max_reconnect_backoff = milliseconds(5000);
    });
    CheckValid([](auto& c) { c.user_agent_prefix = "my-app/1.0"; });
  }

  TEST_CASE("CreateChannel with an invalid config returns a lame INVALID_ARGUMENT channel") {
    egrpc::ChannelConfig c;
    c.max_concurrent_streams = 0;
    CheckLame(egrpc::CreateChannel("127.0.0.1:1", c), grpc::StatusCode::INVALID_ARGUMENT);
  }

  TEST_CASE("CreateChannel with an invalid target returns a lame INVALID_ARGUMENT channel") {
    for (const char* target : {"", "unix:///tmp/x", "host:0", "host:70000"}) {
      CAPTURE(target);
      CheckLame(egrpc::CreateChannel(target), grpc::StatusCode::INVALID_ARGUMENT);
    }
  }

  TEST_CASE("CreateChannel with a valid config returns a live channel") {
    CheckLiveCancelled(egrpc::CreateChannel("127.0.0.1:1"));
    CheckLiveCancelled(
        egrpc::CreateChannel("127.0.0.1:1", egrpc::ChannelConfig::CellularProfile()));
    egrpc::ChannelConfig big;
    big.event_thread_stack_size = 256 * 1024;
    CheckLiveCancelled(egrpc::CreateChannel("127.0.0.1:1", big));
    egrpc::ChannelConfig platform;
    platform.event_thread_stack_size = 0;
    CheckLiveCancelled(egrpc::CreateChannel("127.0.0.1:1", platform));
    CheckLiveCancelled(egrpc::CreateChannel("dns:///127.0.0.1:1"));
    CheckLiveCancelled(egrpc::CreateChannel("[::1]:443"));
  }
}
