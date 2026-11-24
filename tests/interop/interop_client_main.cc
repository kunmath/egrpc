// egrpc M8 acceptance (design §8): the in-scope subset of the upstream gRPC
// interop test client, built against UNMODIFIED grpc_cpp_plugin v1.85.4 stubs
// for grpc.testing.TestService (gen/src/proto/grpc/testing/test.grpc.pb.*) with
// egrpc's grpcpp shim underneath. See docs/interop.md.
//
// Usage: egrpc_interop_client [--server-host HOST] --server-port PORT
//                             --ca-file CERT.pem --test-case CASE
//
// Prints "INTEROP_CASE <name> PASS" or "INTEROP_CASE <name> FAIL <reason>" per
// case. Exit 0 = all passed, 1 = a case failed, 2 = usage error.

#include <grpcpp/grpcpp.h>

#include <chrono>
#include <cstdio>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "src/proto/grpc/testing/test.grpc.pb.h"

using grpc::ClientContext;
using grpc::Status;
using grpc::StatusCode;
using grpc::testing::COMPRESSABLE;
using grpc::testing::Empty;
using grpc::testing::SimpleRequest;
using grpc::testing::SimpleResponse;
using grpc::testing::StreamingInputCallRequest;
using grpc::testing::StreamingInputCallResponse;
using grpc::testing::StreamingOutputCallRequest;
using grpc::testing::StreamingOutputCallResponse;
using grpc::testing::TestService;

namespace {

constexpr int kExitPass = 0;
constexpr int kExitFail = 1;
constexpr int kExitUsage = 2;

const char kStatusMessage[] = "test status message";
const char kInitialKey[] = "x-grpc-test-echo-initial";
const char kInitialValue[] = "test_initial_metadata_value";
const char kTrailingKey[] = "x-grpc-test-echo-trailing-bin";
const std::string kTrailingValue("\x0a\x0b\x0a\x0b\x0a\x0b", 6);

// A case returns an empty string on success, else the failure reason.
using CaseFn = std::function<std::string(TestService::Stub&)>;

std::string StatusStr(const Status& s) {
  std::ostringstream os;
  os << "code=" << static_cast<int>(s.error_code()) << " message=\"" << s.error_message() << "\"";
  return os.str();
}

std::string ExpectCode(const char* what, const Status& s, StatusCode want) {
  if (s.error_code() == want) return "";
  std::ostringstream os;
  os << what << ": want code=" << static_cast<int>(want) << " got " << StatusStr(s);
  return os.str();
}

std::string EmptyUnary(TestService::Stub& stub) {
  ClientContext ctx;
  Empty resp;
  Status s = stub.EmptyCall(&ctx, Empty(), &resp);
  return s.ok() ? "" : "EmptyCall: " + StatusStr(s);
}

std::string LargeUnary(TestService::Stub& stub) {
  SimpleRequest req;
  req.set_response_type(COMPRESSABLE);
  req.set_response_size(314159);
  req.mutable_payload()->set_body(std::string(271828, '\0'));
  ClientContext ctx;
  SimpleResponse resp;
  Status s = stub.UnaryCall(&ctx, req, &resp);
  if (!s.ok()) return "UnaryCall: " + StatusStr(s);
  if (resp.payload().type() != COMPRESSABLE) return "payload type not COMPRESSABLE";
  if (resp.payload().body().size() != 314159) {
    return "payload size " + std::to_string(resp.payload().body().size()) + " != 314159";
  }
  return "";
}

std::string ServerStreaming(TestService::Stub& stub) {
  const std::vector<int> sizes = {31415, 9, 2653, 58979};
  StreamingOutputCallRequest req;
  req.set_response_type(COMPRESSABLE);
  for (int sz : sizes) req.add_response_parameters()->set_size(sz);
  ClientContext ctx;
  std::unique_ptr<grpc::ClientReader<StreamingOutputCallResponse>> reader(
      stub.StreamingOutputCall(&ctx, req));
  StreamingOutputCallResponse resp;
  size_t n = 0;
  std::string err;
  while (reader->Read(&resp)) {
    if (!err.empty()) continue;
    if (n >= sizes.size()) {
      err = "more than " + std::to_string(sizes.size()) + " responses";
    } else if (resp.payload().type() != COMPRESSABLE) {
      err = "response " + std::to_string(n) + " type not COMPRESSABLE";
    } else if (resp.payload().body().size() != static_cast<size_t>(sizes[n])) {
      err = "response " + std::to_string(n) + " size " +
            std::to_string(resp.payload().body().size()) + " != " + std::to_string(sizes[n]);
    }
    ++n;
  }
  Status s = reader->Finish();
  if (!s.ok()) return "Finish: " + StatusStr(s);
  if (!err.empty()) return err;
  if (n != sizes.size()) return "got " + std::to_string(n) + " responses, want 4";
  return "";
}

std::string StatusCodeAndMessage(TestService::Stub& stub) {
  {
    SimpleRequest req;
    req.set_response_size(1);
    req.mutable_payload()->set_body(std::string(1, '\0'));
    req.mutable_response_status()->set_code(2);
    req.mutable_response_status()->set_message(kStatusMessage);
    ClientContext ctx;
    SimpleResponse resp;
    Status s = stub.UnaryCall(&ctx, req, &resp);
    std::string err = ExpectCode("UnaryCall", s, StatusCode::UNKNOWN);
    if (!err.empty()) return err;
    if (s.error_message() != kStatusMessage) return "UnaryCall: wrong message " + StatusStr(s);
  }
  {
    StreamingOutputCallRequest req;
    req.add_response_parameters()->set_size(1);
    req.mutable_response_status()->set_code(2);
    req.mutable_response_status()->set_message(kStatusMessage);
    ClientContext ctx;
    std::unique_ptr<grpc::ClientReader<StreamingOutputCallResponse>> reader(
        stub.StreamingOutputCall(&ctx, req));
    StreamingOutputCallResponse resp;
    while (reader->Read(&resp)) {
    }
    Status s = reader->Finish();
    std::string err = ExpectCode("StreamingOutputCall", s, StatusCode::UNKNOWN);
    if (!err.empty()) return err;
    if (s.error_message() != kStatusMessage) {
      return "StreamingOutputCall: wrong message " + StatusStr(s);
    }
  }
  return "";
}

bool HasMetadata(const std::multimap<grpc::string_ref, grpc::string_ref>& md, const char* key,
                 const std::string& value) {
  auto range = md.equal_range(grpc::string_ref(key));
  for (auto it = range.first; it != range.second; ++it) {
    if (std::string(it->second.data(), it->second.size()) == value) return true;
  }
  return false;
}

std::string CustomMetadata(TestService::Stub& stub) {
  SimpleRequest req;
  req.set_response_size(1);
  req.mutable_payload()->set_body(std::string(1, '\0'));
  ClientContext ctx;
  ctx.AddMetadata(kInitialKey, kInitialValue);
  ctx.AddMetadata(kTrailingKey, kTrailingValue);
  SimpleResponse resp;
  Status s = stub.UnaryCall(&ctx, req, &resp);
  if (!s.ok()) return "UnaryCall: " + StatusStr(s);
  if (!HasMetadata(ctx.GetServerInitialMetadata(), kInitialKey, kInitialValue)) {
    return "initial metadata echo missing or wrong";
  }
  if (!HasMetadata(ctx.GetServerTrailingMetadata(), kTrailingKey, kTrailingValue)) {
    return "trailing -bin metadata echo missing or wrong";
  }
  return "";
}

StreamingOutputCallRequest SleepingRequest() {
  StreamingOutputCallRequest req;
  auto* p = req.add_response_parameters();
  p->set_size(1);
  p->set_interval_us(10 * 1000 * 1000);
  return req;
}

std::string TimeoutOnSleepingServer(TestService::Stub& stub) {
  ClientContext ctx;
  const auto start = std::chrono::steady_clock::now();
  ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(100));
  std::unique_ptr<grpc::ClientReader<StreamingOutputCallResponse>> reader(
      stub.StreamingOutputCall(&ctx, SleepingRequest()));
  StreamingOutputCallResponse resp;
  if (reader->Read(&resp)) return "Read() unexpectedly returned a message";
  Status s = reader->Finish();
  const auto elapsed_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                              std::chrono::steady_clock::now() - start)
                              .count();
  std::string err = ExpectCode("StreamingOutputCall", s, StatusCode::DEADLINE_EXCEEDED);
  if (!err.empty()) return err;
  if (elapsed_ms >= 5000) return "deadline took " + std::to_string(elapsed_ms) + " ms";
  return "";
}

std::string CancelAfterBegin(TestService::Stub& stub) {
  ClientContext ctx;
  std::unique_ptr<grpc::ClientReader<StreamingOutputCallResponse>> reader(
      stub.StreamingOutputCall(&ctx, SleepingRequest()));
  ctx.TryCancel();
  StreamingOutputCallResponse resp;
  if (reader->Read(&resp)) return "Read() unexpectedly returned a message";
  return ExpectCode("StreamingOutputCall", reader->Finish(), StatusCode::CANCELLED);
}

std::string UnimplementedMethod(TestService::Stub& stub) {
  ClientContext ctx;
  Empty resp;
  return ExpectCode("UnimplementedCall", stub.UnimplementedCall(&ctx, Empty(), &resp),
                    StatusCode::UNIMPLEMENTED);
}

// Client-streaming and bidi are permanently out of scope (design §2): their
// generated stubs must compile and return UNIMPLEMENTED at runtime.
std::string ExcludedStubs(TestService::Stub& stub) {
  {
    ClientContext ctx;
    StreamingInputCallResponse resp;
    std::unique_ptr<grpc::ClientWriter<StreamingInputCallRequest>> writer(
        stub.StreamingInputCall(&ctx, &resp));
    writer->Write(StreamingInputCallRequest());
    writer->WritesDone();
    Status s = writer->Finish();
    std::printf("EXCLUDED StreamingInputCall code=%d\n", static_cast<int>(s.error_code()));
    std::string err = ExpectCode("StreamingInputCall", s, StatusCode::UNIMPLEMENTED);
    if (!err.empty()) return err;
  }
  {
    ClientContext ctx;
    std::unique_ptr<
        grpc::ClientReaderWriter<StreamingOutputCallRequest, StreamingOutputCallResponse>>
        stream(stub.FullDuplexCall(&ctx));
    stream->Write(StreamingOutputCallRequest());
    stream->WritesDone();
    StreamingOutputCallResponse resp;
    while (stream->Read(&resp)) {
    }
    Status s = stream->Finish();
    std::printf("EXCLUDED FullDuplexCall code=%d\n", static_cast<int>(s.error_code()));
    std::string err = ExpectCode("FullDuplexCall", s, StatusCode::UNIMPLEMENTED);
    if (!err.empty()) return err;
  }
  return "";
}

const std::vector<std::pair<std::string, CaseFn>>& AllCases() {
  static const std::vector<std::pair<std::string, CaseFn>> cases = {
      {"empty_unary", EmptyUnary},
      {"large_unary", LargeUnary},
      {"server_streaming", ServerStreaming},
      {"status_code_and_message", StatusCodeAndMessage},
      {"custom_metadata", CustomMetadata},
      {"timeout_on_sleeping_server", TimeoutOnSleepingServer},
      {"cancel_after_begin", CancelAfterBegin},
      {"unimplemented_method", UnimplementedMethod},
  };
  return cases;
}

int Usage(const std::string& why) {
  std::fprintf(stderr,
               "error: %s\n"
               "usage: egrpc_interop_client [--server-host HOST] --server-port PORT\n"
               "                            --ca-file CERT.pem --test-case CASE\n"
               "CASE: empty_unary|large_unary|server_streaming|status_code_and_message|\n"
               "      custom_metadata|timeout_on_sleeping_server|cancel_after_begin|\n"
               "      unimplemented_method|excluded_stubs|all\n",
               why.c_str());
  return kExitUsage;
}

// Strict decimal port in 1..65535: digits only, no sign/whitespace/suffix.
bool ParsePort(const std::string& s, int* out) {
  if (s.empty() || s.size() > 5) return false;
  int v = 0;
  for (char c : s) {
    if (c < '0' || c > '9') return false;
    v = v * 10 + (c - '0');
  }
  if (v < 1 || v > 65535) return false;
  *out = v;
  return true;
}

bool ReadFile(const std::string& path, std::string* out) {
  std::ifstream f(path, std::ios::binary);
  if (!f) return false;
  std::ostringstream ss;
  ss << f.rdbuf();
  *out = ss.str();
  return true;
}

}  // namespace

int main(int argc, char** argv) {
  std::string host = "localhost";
  std::string ca_file;
  std::string test_case;
  int port = 0;
  bool have_port = false;

  for (int i = 1; i < argc; ++i) {
    const std::string flag = argv[i];
    if (flag != "--server-host" && flag != "--server-port" && flag != "--ca-file" &&
        flag != "--test-case") {
      return Usage("unknown argument '" + flag + "'");
    }
    if (i + 1 >= argc) return Usage("missing value for " + flag);
    const std::string value = argv[++i];
    if (flag == "--server-host") {
      if (value.empty()) return Usage("empty --server-host");
      host = value;
    } else if (flag == "--server-port") {
      if (!ParsePort(value, &port)) return Usage("invalid --server-port '" + value + "'");
      have_port = true;
    } else if (flag == "--ca-file") {
      ca_file = value;
    } else {
      test_case = value;
    }
  }
  if (!have_port) return Usage("--server-port is required");
  if (ca_file.empty()) return Usage("--ca-file is required");
  if (test_case.empty()) return Usage("--test-case is required");

  std::vector<std::pair<std::string, CaseFn>> selected;
  if (test_case == "all") {
    selected = AllCases();
  } else if (test_case == "excluded_stubs") {
    selected.emplace_back(test_case, ExcludedStubs);
  } else {
    for (const auto& c : AllCases()) {
      if (c.first == test_case) selected.push_back(c);
    }
    if (selected.empty()) return Usage("unknown --test-case '" + test_case + "'");
  }

  grpc::SslCredentialsOptions ssl;
  if (!ReadFile(ca_file, &ssl.pem_root_certs)) return Usage("cannot read --ca-file " + ca_file);

  const std::string target = host + ":" + std::to_string(port);
  auto channel = grpc::CreateChannel(target, grpc::SslCredentials(ssl));
  std::unique_ptr<TestService::Stub> stub = TestService::NewStub(channel);

  int rc = kExitPass;
  for (const auto& c : selected) {
    const std::string err = c.second(*stub);
    if (err.empty()) {
      std::printf("INTEROP_CASE %s PASS\n", c.first.c_str());
    } else {
      std::printf("INTEROP_CASE %s FAIL %s\n", c.first.c_str(), err.c_str());
      rc = kExitFail;
    }
    std::fflush(stdout);
  }
  return rc;
}
