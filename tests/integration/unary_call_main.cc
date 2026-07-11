// egrpc — M3 acceptance probe (design §8, M3): hand-rolled unary call,
// end-to-end through the temporary internal API (ChannelImpl::UnaryCall), no
// generated stubs. Round-trips protobuf-lite route_guide messages against a
// live gRPC server (tests/server/route_guide_server.py).
//
// stdout protocol (line-buffered; consumed by tests/test_unary.py):
//   STATUS code=<int> message=<grpc-message, percent-decoded>
//   FEATURE name=<name> latitude=<lat> longitude=<lon>   (only when code=0)
//   INITIAL <key>=<value>       (per initial-metadata entry)
//   TRAILER <key>=<value>       (per trailing-metadata entry)
// Exit codes: 0 = call completed (any grpc-status; asserted by pytest),
//             1 = internal probe failure, 2 = usage.
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <optional>
#include <string>

#include "core/channel_impl.h"
#include "route_guide.pb.h"

namespace {

int Usage(const char* argv0) {
  std::fprintf(stderr,
               "usage: %s --host H --port P [--ca FILE] [--insecure]\n"
               "          [--latitude N] [--longitude N] [--timeout-ms N]\n"
               "          [--path /svc/Method] [--calls N]\n",
               argv0);
  return 2;
}

}  // namespace

int main(int argc, char** argv) {
  setvbuf(stdout, nullptr, _IOLBF, 0);

  std::string host;
  uint16_t port = 0;
  egrpc::internal::TlsConfig tls;
  std::string path = "/routeguide.RouteGuide/GetFeature";
  int32_t latitude = 0;
  int32_t longitude = 0;
  std::optional<std::chrono::nanoseconds> timeout;
  int calls = 1;

  for (int i = 1; i < argc; ++i) {
    const std::string flag = argv[i];
    auto next = [&]() -> const char* { return (i + 1 < argc) ? argv[++i] : nullptr; };
    const char* v = nullptr;
    if (flag == "--insecure") {
      tls.insecure_skip_verify_for_testing_only = true;
    } else if ((v = next()) == nullptr) {
      return Usage(argv[0]);
    } else if (flag == "--host") {
      host = v;
    } else if (flag == "--port") {
      port = static_cast<uint16_t>(std::atoi(v));
    } else if (flag == "--ca") {
      tls.ca_bundle_path = v;
    } else if (flag == "--path") {
      path = v;
    } else if (flag == "--latitude") {
      latitude = std::atoi(v);
    } else if (flag == "--longitude") {
      longitude = std::atoi(v);
    } else if (flag == "--timeout-ms") {
      timeout = std::chrono::milliseconds(std::atoi(v));
    } else if (flag == "--calls") {
      calls = std::atoi(v);
    } else {
      return Usage(argv[0]);
    }
  }
  if (host.empty() || port == 0 || calls < 1) return Usage(argv[0]);

  egrpc::internal::ChannelImpl channel(host, port, tls, egrpc::internal::ChannelOptions{});

  // --calls > 1 exercises several unary calls over one connection (stream id
  // reuse, per-call scanner isolation).
  for (int call_index = 0; call_index < calls; ++call_index) {
    routeguide::Point point;
    point.set_latitude(latitude);
    point.set_longitude(longitude);
    std::string request;
    if (!point.SerializeToString(&request)) {
      std::fprintf(stderr, "FAILED: could not serialize request\n");
      return 1;
    }

    const egrpc::internal::CallState::Result result = channel.UnaryCall(path, request, timeout);

    std::printf("STATUS code=%d message=%s\n", static_cast<int>(result.code),
                result.message.c_str());
    if (result.code == egrpc::internal::StatusCode::kOk) {
      routeguide::Feature feature;
      if (!feature.ParseFromString(result.response)) {
        std::fprintf(stderr, "FAILED: could not parse Feature from response payload\n");
        return 1;
      }
      std::printf("FEATURE name=%s latitude=%d longitude=%d\n", feature.name().c_str(),
                  feature.location().latitude(), feature.location().longitude());
    }
    for (const auto& kv : result.initial_metadata) {
      std::printf("INITIAL %s=%s\n", kv.first.c_str(), kv.second.c_str());
    }
    for (const auto& kv : result.trailing_metadata) {
      std::printf("TRAILER %s=%s\n", kv.first.c_str(), kv.second.c_str());
    }
  }

  channel.Shutdown();
  return 0;
}
