// egrpc M4 acceptance example (design §8): the GetFeature path of the
// canonical gRPC route_guide client, built against UNMODIFIED
// grpc_cpp_plugin-generated stubs (gen/route_guide.grpc.pb.{h,cc}) with
// egrpc's grpcpp shim underneath. Structured after upstream
// examples/cpp/route_guide/route_guide_client.cc, trimmed to unary and
// adapted for TLS-only v0.1 (root CA from a file instead of insecure
// channels).
//
// Usage: egrpc_route_guide_client --target host:port --ca-file cert.pem
//                                 [--latitude N] [--longitude N]
//                                 [--deadline-ms N]
//
// Output (machine-checked by tests/test_shim_unary.py):
//   GETFEATURE ok name=<name> latitude=<lat> longitude=<lon>
//   GETFEATURE error code=<int> message=<grpc-message>

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>

#include "route_guide.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using routeguide::Feature;
using routeguide::Point;
using routeguide::RouteGuide;

namespace {

class RouteGuideClient {
 public:
  explicit RouteGuideClient(std::shared_ptr<Channel> channel)
      : stub_(RouteGuide::NewStub(channel)) {}

  // Returns true when the RPC succeeded (an empty feature name is a valid
  // "nothing there" answer, matching the route_guide service contract).
  bool GetFeature(int32_t latitude, int32_t longitude, int deadline_ms) {
    Point point;
    point.set_latitude(latitude);
    point.set_longitude(longitude);

    ClientContext context;
    if (deadline_ms > 0) {
      context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::milliseconds(deadline_ms));
    }

    Feature feature;
    Status status = stub_->GetFeature(&context, point, &feature);
    if (!status.ok()) {
      std::printf("GETFEATURE error code=%d message=%s\n", static_cast<int>(status.error_code()),
                  status.error_message().c_str());
      return false;
    }
    std::printf("GETFEATURE ok name=%s latitude=%d longitude=%d\n", feature.name().c_str(),
                feature.location().latitude(), feature.location().longitude());
    return true;
  }

 private:
  std::unique_ptr<RouteGuide::Stub> stub_;
};

bool ReadFile(const char* path, std::string* out) {
  std::ifstream in(path, std::ios::binary);
  if (!in) return false;
  std::ostringstream ss;
  ss << in.rdbuf();
  *out = ss.str();
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  std::string target;
  std::string ca_file;
  int32_t latitude = 1000;
  int32_t longitude = 2000;
  int deadline_ms = 0;

  for (int i = 1; i + 1 < argc; i += 2) {
    if (std::strcmp(argv[i], "--target") == 0) {
      target = argv[i + 1];
    } else if (std::strcmp(argv[i], "--ca-file") == 0) {
      ca_file = argv[i + 1];
    } else if (std::strcmp(argv[i], "--latitude") == 0) {
      latitude = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--longitude") == 0) {
      longitude = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--deadline-ms") == 0) {
      deadline_ms = std::atoi(argv[i + 1]);
    } else {
      std::fprintf(stderr, "unknown flag: %s\n", argv[i]);
      return 2;
    }
  }
  if (target.empty() || ca_file.empty()) {
    std::fprintf(stderr,
                 "usage: %s --target host:port --ca-file cert.pem"
                 " [--latitude N] [--longitude N] [--deadline-ms N]\n",
                 argv[0]);
    return 2;
  }

  grpc::SslCredentialsOptions ssl;
  if (!ReadFile(ca_file.c_str(), &ssl.pem_root_certs)) {
    std::fprintf(stderr, "cannot read CA file: %s\n", ca_file.c_str());
    return 2;
  }

  RouteGuideClient client(grpc::CreateChannel(target, grpc::SslCredentials(ssl)));
  return client.GetFeature(latitude, longitude, deadline_ms) ? 0 : 1;
}
