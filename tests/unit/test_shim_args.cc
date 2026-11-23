// egrpc — unit tests for the M6 shim wiring (design §4.4, §4.5, §5.2, §5.3):
// grpc::ClientContext wait_for_ready / egrpc_status_is_local accessors and the
// grpc::ChannelArguments keepalive + reconnect-backoff keys that
// CreateCustomChannel honors. Suite "shim_args".
//
// Also covers Fix #3 (design §4.6 prerequisite, §5.7): a ClientContext that
// was TryCancel()ed before the call is issued fails with CANCELLED and
// egrpc_status_is_local() == true, for both the unary and server-streaming
// shim entry points — without ever submitting to the EventThread (so no
// network is needed: the channel stays IDLE and never resolves/connects).

#include <atomic>
#include <map>
#include <memory>
#include <string>

#include "api/client_stream_raw_internal.h"
#include "core/call_state.h"
#include "core/channel_impl.h"
#include "doctest/doctest.h"
#include "grpcpp/channel.h"
#include "grpcpp/client_context.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/impl/client_unary_call.h"
#include "grpcpp/impl/rpc_method.h"
#include "grpcpp/security/credentials.h"
#include "grpcpp/support/channel_arguments.h"
#include "grpcpp/support/sync_stream.h"

namespace {

// Returns the stored int for `key`, or `fallback` when absent.
int IntOr(const grpc::ChannelArguments& args, const std::string& key, int fallback) {
  const auto& ints = args.egrpc_ints();
  auto it = ints.find(key);
  return it == ints.end() ? fallback : it->second;
}

const grpc::internal::RpcMethod kUnaryMethod("/test.Svc/Unary",
                                             grpc::internal::RpcMethod::NORMAL_RPC);
const grpc::internal::RpcMethod kStreamMethod("/test.Svc/Stream",
                                              grpc::internal::RpcMethod::SERVER_STREAMING);

// Port 1 on loopback: nothing listens there, but pre-cancelled calls must
// never get as far as resolving or connecting anyway.
std::shared_ptr<grpc::Channel> MakeIdleChannel() {
  return grpc::CreateChannel("127.0.0.1:1", grpc::SslCredentials(grpc::SslCredentialsOptions{}));
}

}  // namespace

TEST_SUITE("shim_args") {
  TEST_CASE("ClientContext wait_for_ready defaults to false and toggles") {
    grpc::ClientContext ctx;
    CHECK_FALSE(ctx.wait_for_ready());

    ctx.set_wait_for_ready(true);
    CHECK(ctx.wait_for_ready());

    // Idempotent re-set keeps the value.
    ctx.set_wait_for_ready(true);
    CHECK(ctx.wait_for_ready());

    ctx.set_wait_for_ready(false);
    CHECK_FALSE(ctx.wait_for_ready());
  }

  TEST_CASE("ClientContext egrpc_status_is_local defaults to false") {
    grpc::ClientContext ctx;
    CHECK_FALSE(ctx.egrpc_status_is_local());
    // Unrelated setters must not flip it.
    ctx.set_wait_for_ready(true);
    ctx.TryCancel();
    CHECK_FALSE(ctx.egrpc_status_is_local());
  }

  TEST_CASE("ClientContext egrpc_initial_metadata_received defaults to false and is markable") {
    grpc::ClientContext ctx;
    CHECK_FALSE(ctx.egrpc_initial_metadata_received());
    grpc::internal::ClientContextAccess::MarkInitialMetadataReceived(nullptr);
    grpc::internal::ClientContextAccess::MarkInitialMetadataReceived(&ctx);
    CHECK(ctx.egrpc_initial_metadata_received());
  }

  TEST_CASE("Finish() without WaitForInitialMetadata marks valid initial metadata arrival") {
    auto completion = std::make_shared<egrpc::internal::CallCompletion>();
    egrpc::internal::CallState call(4 * 1024 * 1024, egrpc::internal::CallMode::kServerStreaming,
                                    completion, 16, 4 * 1024 * 1024);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    call.OnTrailers({{"grpc-status", "0"}});
    call.OnClose(0);

    grpc::ClientContext ctx;
    std::unique_ptr<grpc::internal::ClientStreamRaw> raw =
        grpc::internal::MakeClientStreamRawForCompletion(&ctx, completion, 4 * 1024 * 1024);
    const grpc::Status s = raw->Finish();
    CHECK(s.ok());
    CHECK(ctx.egrpc_initial_metadata_received());
  }

  TEST_CASE("ChannelArguments M6 keys use upstream channel-arg names") {
    CHECK(std::string(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS) ==
          "grpc.initial_reconnect_backoff_ms");
    CHECK(std::string(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS) == "grpc.min_reconnect_backoff_ms");
    CHECK(std::string(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS) == "grpc.max_reconnect_backoff_ms");
    CHECK(std::string(GRPC_ARG_KEEPALIVE_TIME_MS) == "grpc.keepalive_time_ms");
    CHECK(std::string(GRPC_ARG_KEEPALIVE_TIMEOUT_MS) == "grpc.keepalive_timeout_ms");
    CHECK(std::string(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS) ==
          "grpc.keepalive_permit_without_calls");
  }

  TEST_CASE("ChannelArguments SetInt stores M6 keys retrievable via egrpc_ints") {
    grpc::ChannelArguments args;
    CHECK(args.egrpc_ints().empty());

    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 1000);
    args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 20000);
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 120000);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 30000);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIMEOUT_MS, 10000);
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);

    const std::map<std::string, int>& ints = args.egrpc_ints();
    CHECK(ints.size() == 6u);
    CHECK(IntOr(args, GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, -1) == 1000);
    CHECK(IntOr(args, GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, -1) == 20000);
    CHECK(IntOr(args, GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, -1) == 120000);
    CHECK(IntOr(args, GRPC_ARG_KEEPALIVE_TIME_MS, -1) == 30000);
    CHECK(IntOr(args, GRPC_ARG_KEEPALIVE_TIMEOUT_MS, -1) == 10000);
    CHECK(IntOr(args, GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, -1) == 1);

    // Int args never leak into the string map.
    CHECK(args.egrpc_strings().empty());
  }

  TEST_CASE("ChannelArguments SetInt overwrites a previous value for the same key") {
    grpc::ChannelArguments args;
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 1);
    args.SetInt(GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, 0);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 5000);
    args.SetInt(GRPC_ARG_KEEPALIVE_TIME_MS, 7000);

    CHECK(args.egrpc_ints().size() == 2u);
    CHECK(IntOr(args, GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS, -1) == 0);
    CHECK(IntOr(args, GRPC_ARG_KEEPALIVE_TIME_MS, -1) == 7000);
    // Unset keys stay absent (CreateCustomChannel then keeps its defaults).
    CHECK(args.egrpc_ints().count(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS) == 0u);
  }

  TEST_CASE("pre-submission TryCancel: unary is CANCELLED with local origin") {
    std::shared_ptr<grpc::Channel> channel = MakeIdleChannel();
    REQUIRE(channel != nullptr);

    grpc::ClientContext ctx;
    ctx.TryCancel();
    CHECK_FALSE(ctx.egrpc_status_is_local());  // only a call outcome sets it

    std::string response = "untouched";
    const grpc::Status s = grpc::internal::BlockingUnaryCallRaw(channel.get(), kUnaryMethod, &ctx,
                                                                "request-bytes", &response);
    CHECK(s.error_code() == grpc::StatusCode::CANCELLED);
    CHECK_FALSE(s.ok());
    CHECK(ctx.egrpc_status_is_local());
    CHECK(response == "untouched");
    CHECK(ctx.GetServerTrailingMetadata().empty());

    // A repeated TryCancel after the call is a harmless no-op.
    ctx.TryCancel();
    CHECK(ctx.egrpc_status_is_local());
  }

  TEST_CASE("pre-submission TryCancel: server-streaming is CANCELLED with local origin") {
    std::shared_ptr<grpc::Channel> channel = MakeIdleChannel();
    REQUIRE(channel != nullptr);

    grpc::ClientContext ctx;
    ctx.TryCancel();

    std::unique_ptr<grpc::internal::ClientStreamRaw> raw =
        grpc::internal::StartServerStreamRaw(channel.get(), kStreamMethod, &ctx, "request-bytes");
    REQUIRE(raw != nullptr);
    raw->WaitForInitialMetadata();  // must not block on a never-started call
    CHECK_FALSE(ctx.egrpc_initial_metadata_received());

    std::string msg = "untouched";
    CHECK_FALSE(raw->ReadRaw(&msg));
    CHECK_FALSE(raw->ReadRaw(&msg));  // sticky EOF
    CHECK(msg == "untouched");

    const grpc::Status s = raw->Finish();
    CHECK(s.error_code() == grpc::StatusCode::CANCELLED);
    CHECK(ctx.egrpc_status_is_local());
  }

  TEST_CASE("pre-submission TryCancel never resolves, connects, or leaves IDLE") {
    using egrpc::internal::ChannelImpl;
    auto resolves = std::make_shared<std::atomic<int>>(0);
    egrpc::internal::ChannelOptions opts;
    opts.resolve_fn = [resolves](const std::string&, uint16_t) {
      resolves->fetch_add(1);
      egrpc::internal::ResolveResult r;
      r.error = "mock resolver: must not be called";
      return r;
    };
    auto owned = std::make_unique<ChannelImpl>("127.0.0.1", 1, egrpc::internal::TlsConfig{}, opts);
    ChannelImpl* impl = owned.get();  // valid while `channel` lives
    std::shared_ptr<grpc::Channel> channel = grpc::Channel::FromImpl(std::move(owned));

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
      auto raw = grpc::internal::StartServerStreamRaw(channel.get(), kStreamMethod, &ctx, "req");
      REQUIRE(raw != nullptr);
      const grpc::Status s = raw->Finish();
      CHECK(s.error_code() == grpc::StatusCode::CANCELLED);
      CHECK(ctx.egrpc_status_is_local());
    }

    CHECK(impl->connect_attempts() == 0u);
    CHECK(impl->state() == ChannelImpl::ConnectivityState::kIdle);
    CHECK(impl->max_active_calls() == 0u);
    channel.reset();  // ~Channel shuts down and joins the EventThread
    CHECK(resolves->load() == 0);
  }
}  // TEST_SUITE("shim_args")
