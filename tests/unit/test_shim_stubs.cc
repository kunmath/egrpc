// egrpc — unit tests for the shim's compile-present async/callback stubs
// (design §4.5, §2): out-of-scope APIs must be observable as UNIMPLEMENTED
// through their own contracts — tags through the CompletionQueue, OnDone
// through the reactor — never a hang. Suite "shim_stubs".
//
// The stubs are type-agnostic (no serialization happens), so std::string
// stands in for message types.

#include <chrono>
#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "doctest/doctest.h"
#include "grpcpp/completion_queue.h"
#include "grpcpp/support/async_stream.h"
#include "grpcpp/support/async_unary_call.h"
#include "grpcpp/support/client_callback.h"
#include "grpcpp/support/string_ref.h"

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

  TEST_CASE("CQ reports shutdown only after Shutdown(); live-but-empty is TIMEOUT") {
    grpc::CompletionQueue cq;
    void* tag = nullptr;
    bool ok = false;
    CHECK(cq.AsyncNext(&tag, &ok, 0) == grpc::CompletionQueue::TIMEOUT);
    cq.Shutdown();
    CHECK(cq.AsyncNext(&tag, &ok, 0) == grpc::CompletionQueue::SHUTDOWN);
    CHECK_FALSE(cq.Next(&tag, &ok));
  }

  TEST_CASE("Next blocks across a producer/consumer race instead of false shutdown") {
    grpc::CompletionQueue cq;
    int finish_tag = 0;
    std::vector<std::pair<void*, bool>> received;

    // Consumer first: it must WAIT, not report shutdown, until the
    // producer's Finish lands and then until Shutdown drains it out.
    std::thread consumer([&] {
      void* tag = nullptr;
      bool ok = false;
      while (cq.Next(&tag, &ok)) received.emplace_back(tag, ok);
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    std::unique_ptr<grpc::ClientAsyncResponseReader<std::string>> reader(
        grpc::internal::ClientAsyncResponseReaderHelper::Create<std::string, std::string>(
            nullptr, &cq, kMethod, nullptr, "request"));
    std::string response;
    grpc::Status status;
    reader->Finish(&response, &status, &finish_tag);
    cq.Shutdown();
    consumer.join();

    REQUIRE(received.size() == 1);
    CHECK(received[0].first == &finish_tag);
    CHECK(received[0].second);
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

  TEST_CASE("reactor OnDone waits for hold removal and fires exactly once") {
    struct Reactor final : grpc::ClientReadReactor<std::string> {
      std::vector<std::string> events;
      void OnReadDone(bool ok) override { events.push_back(ok ? "read-ok" : "read-fail"); }
      void OnDone(const grpc::Status& s) override {
        CHECK(s.error_code() == grpc::StatusCode::UNIMPLEMENTED);
        events.push_back("done");
      }
    } reactor;

    reactor.AddHold();
    reactor.StartCall();
    CHECK(reactor.events.empty());  // held: no OnDone yet

    std::string msg;
    reactor.StartRead(&msg);  // completes unsuccessfully, before OnDone
    reactor.RemoveHold();

    REQUIRE(reactor.events.size() == 2);
    CHECK(reactor.events[0] == "read-fail");
    CHECK(reactor.events[1] == "done");
  }

  TEST_CASE("string_ref substr clamps huge lengths instead of overflowing") {
    const std::string storage = "hello world";
    grpc::string_ref ref(storage);
    CHECK(ref.substr(2, static_cast<size_t>(-2)).size() == storage.size() - 2);
    CHECK(ref.substr(2) == grpc::string_ref("llo world"));
    CHECK(ref.substr(200).empty());
    CHECK(ref.max_size() == storage.size());  // upstream v1.82 parity
    CHECK(ref.starts_with("hello"));
    CHECK(ref.find('w') == 6);
  }
}
