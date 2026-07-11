// egrpc — unit tests for the shim's compile-present async/callback stubs
// (design §4.5, §2): out-of-scope APIs must be observable as UNIMPLEMENTED
// through their own contracts — tags through the CompletionQueue, OnDone
// through the reactor — never a hang. Suite "shim_stubs".
//
// The stubs are type-agnostic (no serialization happens), so std::string
// stands in for message types.

#include <memory>
#include <string>

#include "doctest/doctest.h"
#include "grpcpp/completion_queue.h"
#include "grpcpp/support/async_stream.h"
#include "grpcpp/support/async_unary_call.h"
#include "grpcpp/support/client_callback.h"

namespace {

const grpc::internal::RpcMethod kMethod("/test.Svc/M", grpc::internal::RpcMethod::NORMAL_RPC);

}  // namespace

TEST_SUITE("shim_stubs") {
  TEST_CASE("async unary Finish delivers its tag with ok=true and UNIMPLEMENTED") {
    grpc::CompletionQueue cq;
    std::unique_ptr<grpc::ClientAsyncResponseReader<std::string>> reader(
        grpc::internal::ClientAsyncResponseReaderHelper::Create<std::string, std::string>(
            nullptr, &cq, kMethod, nullptr, "request"));
    reader->StartCall();

    std::string response;
    grpc::Status status;
    int finish_tag = 0;
    reader->Finish(&response, &status, &finish_tag);

    void* tag = nullptr;
    bool ok = false;
    REQUIRE(cq.Next(&tag, &ok));
    CHECK(tag == &finish_tag);
    CHECK(ok);
    CHECK(status.error_code() == grpc::StatusCode::UNIMPLEMENTED);
  }

  TEST_CASE("async reader start/read tags come back ok=false, Finish ok=true") {
    grpc::CompletionQueue cq;
    int start_tag = 0, read_tag = 0, finish_tag = 0;
    std::unique_ptr<grpc::ClientAsyncReader<std::string>> reader(
        grpc::internal::ClientAsyncReaderFactory<std::string>::Create(
            nullptr, &cq, kMethod, nullptr, std::string("request"), /*start=*/true, &start_tag));

    std::string msg;
    reader->Read(&msg, &read_tag);
    grpc::Status status;
    reader->Finish(&status, &finish_tag);

    void* tag = nullptr;
    bool ok = true;
    REQUIRE(cq.Next(&tag, &ok));
    CHECK(tag == &start_tag);
    CHECK_FALSE(ok);
    REQUIRE(cq.Next(&tag, &ok));
    CHECK(tag == &read_tag);
    CHECK_FALSE(ok);
    REQUIRE(cq.Next(&tag, &ok));
    CHECK(tag == &finish_tag);
    CHECK(ok);
    CHECK(status.error_code() == grpc::StatusCode::UNIMPLEMENTED);
  }

  TEST_CASE("a drained CompletionQueue reports shutdown, not a hang") {
    grpc::CompletionQueue cq;
    void* tag = nullptr;
    bool ok = false;
    CHECK_FALSE(cq.Next(&tag, &ok));
    CHECK(cq.AsyncNext(&tag, &ok, 0) == grpc::CompletionQueue::SHUTDOWN);
  }

  TEST_CASE("reactor StartCall delivers OnDone(UNIMPLEMENTED) synchronously") {
    struct Reactor final : grpc::ClientUnaryReactor {
      grpc::Status seen{grpc::StatusCode::OK, ""};
      bool done = false;
      void OnDone(const grpc::Status& s) override {
        seen = s;
        done = true;
      }
    } reactor;
    reactor.StartCall();
    CHECK(reactor.done);
    CHECK(reactor.seen.error_code() == grpc::StatusCode::UNIMPLEMENTED);
  }

  TEST_CASE("reactor subclasses need not override OnDone (upstream parity)") {
    // Compile-level check: OnDone has a default no-op implementation.
    struct MinimalReactor final : grpc::ClientReadReactor<std::string> {
    } reactor;
    reactor.StartCall();  // must not crash with the default OnDone
  }
}
