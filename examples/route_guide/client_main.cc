// egrpc M4 acceptance example (design §8): the GetFeature path of the
// canonical gRPC route_guide client, built against UNMODIFIED
// grpc_cpp_plugin-generated stubs (gen/route_guide.grpc.pb.{h,cc}) with
// egrpc's grpcpp shim underneath. Structured after upstream
// examples/cpp/route_guide/route_guide_client.cc, trimmed to unary and
// adapted for TLS-only v0.1 (root CA from a file instead of insecure
// channels).
//
// Usage: egrpc_route_guide_client --target host:port --ca-file cert.pem
//                                 [--mode getfeature|listfeatures|concurrent]
//                                 [--latitude N] [--longitude N]
//                                 [--deadline-ms N] [--threads N]
//
// Output (machine-checked by tests/test_shim_unary.py):
//   GETFEATURE ok name=<name> latitude=<lat> longitude=<lon>
//   GETFEATURE error code=<int> message=<grpc-message>
//   INITIAL <key>=<value>   TRAILING <key>=<value>   (escaped, sorted)
//   LISTFEATURES code=<int> messages=<n>
//   CONCURRENT ok=<n> total=<n>

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "route_guide.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::Status;
using routeguide::Feature;
using routeguide::Point;
using routeguide::Rectangle;
using routeguide::RouteGuide;

namespace {

// Metadata values may be binary (-bin keys are exposed base64-decoded), so
// non-printable bytes are \xNN-escaped for line-oriented output.
std::string Escape(grpc::string_ref value) {
  std::string out;
  for (char c : value) {
    const auto u = static_cast<unsigned char>(c);
    if (u >= 0x20 && u < 0x7f && c != '\\') {
      out += c;
    } else {
      char buf[5];
      std::snprintf(buf, sizeof(buf), "\\x%02x", u);
      out += buf;
    }
  }
  return out;
}

void PrintMetadata(const char* label,
                   const std::multimap<grpc::string_ref, grpc::string_ref>& metadata) {
  for (const auto& kv : metadata) {
    std::printf("%s %s=%s\n", label, Escape(kv.first).c_str(), Escape(kv.second).c_str());
  }
}

class RouteGuideClient {
 public:
  explicit RouteGuideClient(std::shared_ptr<Channel> channel)
      : stub_(RouteGuide::NewStub(channel)) {}

  // Returns true when the RPC succeeded (an empty feature name is a valid
  // "nothing there" answer, matching the route_guide service contract).
  // `verbose` also dumps server metadata; the concurrent mode keeps output
  // single-line-per-run instead.
  bool GetFeature(int32_t latitude, int32_t longitude, int deadline_ms, bool verbose) {
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
      if (verbose) {
        std::printf("GETFEATURE error code=%d message=%s\n", static_cast<int>(status.error_code()),
                    status.error_message().c_str());
        PrintMetadata("INITIAL", context.GetServerInitialMetadata());
        PrintMetadata("TRAILING", context.GetServerTrailingMetadata());
      }
      return false;
    }
    if (verbose) {
      std::printf("GETFEATURE ok name=%s latitude=%d longitude=%d\n", feature.name().c_str(),
                  feature.location().latitude(), feature.location().longitude());
      PrintMetadata("INITIAL", context.GetServerInitialMetadata());
      PrintMetadata("TRAILING", context.GetServerTrailingMetadata());
    }
    return true;
  }

  // M4 contract check for the not-yet-implemented server-streaming path
  // (real implementation lands in M5): Read() must yield nothing and
  // Finish() must report UNIMPLEMENTED — never a hang or a crash.
  void ListFeatures() {
    Rectangle rect;
    ClientContext context;
    std::unique_ptr<grpc::ClientReader<Feature>> reader(stub_->ListFeatures(&context, rect));
    Feature feature;
    int messages = 0;
    while (reader->Read(&feature)) ++messages;
    Status status = reader->Finish();
    std::printf("LISTFEATURES code=%d messages=%d\n", static_cast<int>(status.error_code()),
                messages);
  }

  // TSan fodder: hammer the channel from `threads` caller threads at once
  // (design §3: callers only touch the op queue + per-call condvar).
  // Returns true only when every call succeeded.
  bool ConcurrentGetFeature(int threads, int32_t latitude, int32_t longitude, int deadline_ms) {
    std::atomic<int> ok{0};
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(threads));
    for (int i = 0; i < threads; ++i) {
      workers.emplace_back([&] {
        if (GetFeature(latitude, longitude, deadline_ms, /*verbose=*/false)) {
          ok.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }
    for (auto& w : workers) w.join();
    std::printf("CONCURRENT ok=%d total=%d\n", ok.load(), threads);
    return ok.load() == threads;
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
  std::string mode = "getfeature";
  int32_t latitude = 1000;
  int32_t longitude = 2000;
  int deadline_ms = 0;
  int threads = 8;

  for (int i = 1; i + 1 < argc; i += 2) {
    if (std::strcmp(argv[i], "--target") == 0) {
      target = argv[i + 1];
    } else if (std::strcmp(argv[i], "--ca-file") == 0) {
      ca_file = argv[i + 1];
    } else if (std::strcmp(argv[i], "--mode") == 0) {
      mode = argv[i + 1];
    } else if (std::strcmp(argv[i], "--latitude") == 0) {
      latitude = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--longitude") == 0) {
      longitude = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--deadline-ms") == 0) {
      deadline_ms = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--threads") == 0) {
      threads = std::atoi(argv[i + 1]);
    } else {
      std::fprintf(stderr, "unknown flag: %s\n", argv[i]);
      return 2;
    }
  }
  if (target.empty() || ca_file.empty()) {
    std::fprintf(stderr,
                 "usage: %s --target host:port --ca-file cert.pem"
                 " [--mode getfeature|listfeatures|concurrent]"
                 " [--latitude N] [--longitude N] [--deadline-ms N] [--threads N]\n",
                 argv[0]);
    return 2;
  }

  grpc::SslCredentialsOptions ssl;
  if (!ReadFile(ca_file.c_str(), &ssl.pem_root_certs)) {
    std::fprintf(stderr, "cannot read CA file: %s\n", ca_file.c_str());
    return 2;
  }

  RouteGuideClient client(grpc::CreateChannel(target, grpc::SslCredentials(ssl)));
  if (mode == "getfeature") {
    return client.GetFeature(latitude, longitude, deadline_ms, /*verbose=*/true) ? 0 : 1;
  }
  if (mode == "listfeatures") {
    client.ListFeatures();
    return 0;
  }
  if (mode == "concurrent") {
    return client.ConcurrentGetFeature(threads, latitude, longitude, deadline_ms) ? 0 : 1;
  }
  std::fprintf(stderr, "unknown mode: %s\n", mode.c_str());
  return 2;
}
