// egrpc — unit tests for CallState terminal-status mapping (design §5.5).
//
// CallState is the per-RPC state machine. These tests construct it directly
// (no transport) and drive the EventThread-side hooks
// (OnResponseHeaders/OnTrailers/OnData/OnClose), then inspect the finalized
// Result returned by Wait(). They exercise only the status-mapping precedence
// rules: scanner error > grpc-status > HTTP-status > RST error code > missing
// grpc-status on clean close → kUnknown.
//
// All cases live in the "call_state" doctest test suite so ctest can run just
// them (see tests/unit/CMakeLists.txt).
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "core/call_state.h"
#include "core/grpc_wire.h"
#include "doctest/doctest.h"
#include "transport/http2_session.h"

namespace {

using egrpc::internal::AppendGrpcFramePrefix;
using egrpc::internal::CallCompletion;
using egrpc::internal::CallMode;
using egrpc::internal::CallState;
using egrpc::internal::StatusCode;
using egrpc::internal::StatusOrigin;
using HeaderList = egrpc::internal::Http2Session::HeaderList;

constexpr size_t kMax = 4 * 1024 * 1024;

// True when `haystack` contains `needle` as a substring.
bool Contains(const std::string& haystack, const std::string& needle) {
  return haystack.find(needle) != std::string::npos;
}

std::string Frame(const std::string& payload) {
  std::string out;
  AppendGrpcFramePrefix(payload.size(), &out);
  out += payload;
  return out;
}

std::string RawPrefix(uint8_t flag, uint32_t declared_len) {
  std::string out;
  out.push_back(static_cast<char>(flag));
  out.push_back(static_cast<char>((declared_len >> 24) & 0xff));
  out.push_back(static_cast<char>((declared_len >> 16) & 0xff));
  out.push_back(static_cast<char>((declared_len >> 8) & 0xff));
  out.push_back(static_cast<char>(declared_len & 0xff));
  return out;
}

void FeedData(CallState* call, const std::string& wire) {
  call->OnData(reinterpret_cast<const uint8_t*>(wire.data()), wire.size());
}

bool HasHeader(const HeaderList& list, const std::string& key, const std::string& value) {
  for (const auto& h : list) {
    if (h.first == key && h.second == value) return true;
  }
  return false;
}

}  // namespace

TEST_SUITE("call_state") {
  TEST_CASE("Clean close without grpc-status maps to kUnknown") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    call.OnClose(0);

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kUnknown);
    CHECK(Contains(r.message, "without grpc-status"));
  }

  TEST_CASE("Malformed grpc-status in trailers-only response maps to kUnknown") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "200"},
                            {"content-type", "application/grpc"},
                            {"grpc-status", "abc"},
                            {"grpc-message", "boom"}},
                           /*end_stream=*/true);
    call.OnClose(0);

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kUnknown);
    CHECK(Contains(r.message, "malformed"));
  }

  TEST_CASE("Malformed grpc-status in separate trailers maps to kUnknown") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    call.OnTrailers({{"grpc-status", "4x"}});
    call.OnClose(0);

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kUnknown);
  }

  TEST_CASE("RST_STREAM error codes map per §5.5 when no headers seen") {
    // {http2_error_code, expected gRPC status}. Unlisted codes default to
    // kInternal.
    struct Case {
      uint32_t rst;
      StatusCode expected;
    };
    const Case cases[] = {
        {0x7, StatusCode::kUnavailable},        // REFUSED_STREAM
        {0x8, StatusCode::kCancelled},          // CANCEL
        {0xb, StatusCode::kResourceExhausted},  // ENHANCE_YOUR_CALM
        {0xc, StatusCode::kPermissionDenied},   // INADEQUATE_SECURITY
        {0x1, StatusCode::kInternal},           // PROTOCOL_ERROR (default)
        {0x2, StatusCode::kInternal},           // INTERNAL_ERROR (default)
        {0xd, StatusCode::kInternal},           // HTTP_1_1_REQUIRED (default)
    };
    for (const Case& c : cases) {
      CAPTURE(c.rst);
      CallState call(kMax);
      call.OnClose(c.rst);
      const CallState::Result& r = call.Wait();
      CHECK(r.code == c.expected);
    }
  }

  TEST_CASE("grpc-status in trailers wins over RST error code") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    call.OnTrailers({{"grpc-status", "5"}});
    call.OnClose(0x8);  // CANCEL — would be kCancelled if it won

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kNotFound);
  }

  TEST_CASE("Non-200 HTTP status wins over RST when grpc-status absent") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "404"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    call.OnClose(0x8);  // CANCEL — would be kCancelled if RST won

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kUnimplemented);
  }

  TEST_CASE("Bad content-type on 200 without grpc-status maps to kInternal") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "text/html"}},
                           /*end_stream=*/false);
    call.OnClose(0);

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kInternal);
  }

  TEST_CASE("Happy path: framed message and grpc-status 0 yields kOk") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    // One gRPC-framed message: flag 0x00 + 4-byte big-endian length 2 + "hi".
    const uint8_t frame[] = {0, 0, 0, 0, 2, 'h', 'i'};
    call.OnData(frame, sizeof(frame));
    call.OnTrailers({{"grpc-status", "0"}});
    call.OnClose(0);

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kOk);
    CHECK(r.response == "hi");
  }

  TEST_CASE("Server streaming: multi-message delivery, WaitForInitialMetadata, and kOk") {
    auto completion = std::make_shared<CallCompletion>();
    CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);

    call.OnResponseHeaders(
        {{":status", "200"}, {"content-type", "application/grpc"}, {"x-init", "hello"}},
        /*end_stream=*/false);
    completion->WaitForInitialMetadata();
    CHECK(HasHeader(completion->InitialMetadataCopy(), "x-init", "hello"));

    FeedData(&call, Frame("alpha") + Frame("beta"));

    std::string payload;
    size_t wire_bytes = 0;

    REQUIRE(completion->PopMessage(&payload, &wire_bytes));
    CHECK(payload == "alpha");
    CHECK(wire_bytes == 10);

    REQUIRE(completion->PopMessage(&payload, &wire_bytes));
    CHECK(payload == "beta");
    CHECK(wire_bytes == 9);

    call.OnTrailers({{"grpc-status", "0"}, {"x-trail", "bye"}});
    call.OnClose(0);

    CHECK_FALSE(completion->PopMessage(&payload, &wire_bytes));
    const CallState::Result& r = completion->Wait();
    CHECK(r.code == StatusCode::kOk);
    CHECK(HasHeader(r.trailing_metadata, "x-trail", "bye"));
    CHECK(HasHeader(r.initial_metadata, "x-init", "hello"));
  }

  TEST_CASE(
      "Server streaming: trailers-only error delivers no messages and wakes "
      "WaitForInitialMetadata") {
    auto completion = std::make_shared<CallCompletion>();
    CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);

    call.OnResponseHeaders({{":status", "200"},
                            {"content-type", "application/grpc"},
                            {"grpc-status", "7"},
                            {"grpc-message", "denied"}},
                           /*end_stream=*/true);
    call.OnClose(0);

    completion->WaitForInitialMetadata();
    CHECK(completion->InitialMetadataCopy().empty());

    std::string payload;
    size_t wire_bytes = 0;
    CHECK_FALSE(completion->PopMessage(&payload, &wire_bytes));

    const CallState::Result& r = completion->Wait();
    CHECK(r.code == StatusCode::kPermissionDenied);
    CHECK(r.message == "denied");
  }

  TEST_CASE(
      "Server streaming: messages delivered before error trailer remain readable before error") {
    auto completion = std::make_shared<CallCompletion>();
    CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);

    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    FeedData(&call, Frame("part1"));
    call.OnTrailers({{"grpc-status", "13"}, {"grpc-message", "boom"}});
    call.OnClose(0);

    std::string payload;
    size_t wire_bytes = 0;
    REQUIRE(completion->PopMessage(&payload, &wire_bytes));
    CHECK(payload == "part1");
    CHECK(wire_bytes == 10);
    CHECK_FALSE(completion->PopMessage(&payload, &wire_bytes));

    const CallState::Result& r = completion->Wait();
    CHECK(r.code == StatusCode::kInternal);
    CHECK(r.message == "boom");
  }

  TEST_CASE("Server streaming: bounded queue + kDraining delivers >max_queued_messages cleanly") {
    auto completion = std::make_shared<CallCompletion>();
    CallState call(kMax, CallMode::kServerStreaming, completion, /*max_queued_messages=*/2,
                   /*max_queued_bytes=*/1024);

    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);

    std::string batch;
    for (int i = 0; i < 5; ++i) {
      batch += Frame("m" + std::to_string(i));
    }
    FeedData(&call, batch);
    call.OnTrailers({{"grpc-status", "0"}});
    call.OnClose(0);

    // Queue holds only 2 messages; remaining 3 sit in the scanner while draining.
    CHECK(call.is_draining());
    CHECK_FALSE(completion->is_done());

    for (int i = 0; i < 5; ++i) {
      std::string payload;
      size_t wire_bytes = 0;
      REQUIRE(completion->PopMessage(&payload, &wire_bytes));
      CHECK(payload == "m" + std::to_string(i));
      CHECK(wire_bytes == 7);
      call.DrainScannerToQueue();
    }

    CHECK_FALSE(call.is_draining());
    CHECK(call.is_closed());

    std::string extra;
    size_t wire_bytes = 0;
    CHECK_FALSE(completion->PopMessage(&extra, &wire_bytes));
    const CallState::Result& r = completion->Wait();
    CHECK(r.code == StatusCode::kOk);
  }

  TEST_CASE("Server streaming: FailLocal discards queued messages and unblocks PopMessage/Wait") {
    auto completion = std::make_shared<CallCompletion>();
    CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);

    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    FeedData(&call, Frame("one") + Frame("two"));

    call.FailLocal(StatusCode::kCancelled, "Cancelled");
    CHECK(completion->is_done());

    std::string payload;
    size_t wire_bytes = 0;
    CHECK_FALSE(completion->PopMessage(&payload, &wire_bytes));

    const CallState::Result& r = completion->Wait();
    CHECK(r.code == StatusCode::kCancelled);
    CHECK(r.message == "Cancelled");
  }

  TEST_CASE(
      "Server streaming: framing error (compressed flag=1) fires on_protocol_error immediately and "
      "fails call") {
    int protocol_errors = 0;
    int reassembly_calls = 0;
    size_t last_reassembly_bytes = 0;
    size_t max_queue_msgs = 0;
    size_t max_queue_bytes = 0;

    CallState::Hooks hooks;
    hooks.on_protocol_error = [&] { ++protocol_errors; };
    hooks.on_reassembly_bytes = [&](size_t bytes) {
      ++reassembly_calls;
      last_reassembly_bytes = bytes;
    };
    hooks.on_queue_depth = [&](size_t msgs, size_t bytes) {
      if (msgs > max_queue_msgs) max_queue_msgs = msgs;
      if (bytes > max_queue_bytes) max_queue_bytes = bytes;
    };

    auto completion = std::make_shared<CallCompletion>();
    CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax, std::move(hooks));

    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    FeedData(&call, Frame("valid"));
    CHECK(max_queue_msgs == 1);
    CHECK(max_queue_bytes == 10);

    FeedData(&call, RawPrefix(/*flag=*/1, /*declared_len=*/0));

    CHECK(reassembly_calls == 2);
    CHECK(last_reassembly_bytes == 5);
    CHECK(protocol_errors == 1);
    CHECK(completion->is_done());

    std::string payload;
    size_t wire_bytes = 0;
    CHECK_FALSE(completion->PopMessage(&payload, &wire_bytes));

    const CallState::Result& r = completion->Wait();
    CHECK(r.code == StatusCode::kInternal);
  }

  TEST_CASE("CallCompletion: concurrent PopMessage wakes on PushMessage and Close (multi-thread)") {
    auto completion = std::make_shared<CallCompletion>();

    std::thread producer([completion] {
      const char* kMessages[] = {"one", "two", "three"};
      for (const char* m : kMessages) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        std::string payload(m);
        const size_t wire_bytes = 5 + payload.size();
        completion->PushMessage(std::move(payload), wire_bytes);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
      CallCompletion::Result res;
      res.code = StatusCode::kOk;
      completion->Close(std::move(res), /*discard_queued=*/false);
    });

    std::vector<std::string> received;
    std::vector<size_t> wire_sizes;
    std::string payload;
    size_t wire_bytes = 0;
    while (completion->PopMessage(&payload, &wire_bytes)) {
      received.push_back(std::move(payload));
      wire_sizes.push_back(wire_bytes);
    }
    const CallCompletion::Result& r = completion->Wait();
    producer.join();

    REQUIRE(received.size() == 3);
    CHECK(received[0] == "one");
    CHECK(received[1] == "two");
    CHECK(received[2] == "three");
    CHECK(wire_sizes[0] == 8);
    CHECK(wire_sizes[1] == 8);
    CHECK(wire_sizes[2] == 10);
    CHECK(r.code == StatusCode::kOk);
  }

  TEST_CASE("Non-200 HTTP proxy response with HTML body preserves HTTP status mapping") {
    {
      CallState call(kMax);
      call.OnResponseHeaders({{":status", "502"}, {"content-type", "text/html"}},
                             /*end_stream=*/false);
      FeedData(&call, "<html>502 Bad Gateway</html>");
      call.OnClose(0);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kUnavailable);
    }

    {
      CallState call(kMax);
      call.OnResponseHeaders({{":status", "200"}, {"content-type", "text/html"}},
                             /*end_stream=*/false);
      FeedData(&call, "<html>oops</html>");
      call.OnClose(0);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kInternal);
      CHECK(Contains(r.message, "unexpected content-type"));
    }
  }

  TEST_CASE("Clean close without grpc-status and truncated gRPC frame maps to kInternal") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    FeedData(&call, RawPrefix(/*flag=*/0, /*declared_len=*/100) + "abc");
    call.OnClose(0);

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kInternal);
    CHECK(Contains(r.message, "truncated"));
  }

  TEST_CASE("Draining call FinalizeDiscarding preserves server grpc-status") {
    SUBCASE("grpc-status 0") {
      auto completion = std::make_shared<CallCompletion>();
      CallState call(kMax, CallMode::kServerStreaming, completion, /*max_queued_messages=*/4, kMax);

      call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                             /*end_stream=*/false);
      std::string batch;
      for (int i = 0; i < 10; ++i) {
        batch += Frame("msg-" + std::to_string(i));
      }
      FeedData(&call, batch);
      call.OnTrailers({{"grpc-status", "0"}});
      call.OnClose(0);

      REQUIRE(call.is_draining());
      CHECK_FALSE(call.is_closed());

      call.FinalizeDiscarding();
      CHECK(call.is_closed());
      CHECK_FALSE(call.is_draining());
      CHECK(completion->Wait().code == StatusCode::kOk);

      std::string payload;
      size_t wire_bytes = 0;
      CHECK_FALSE(completion->PopMessage(&payload, &wire_bytes));
    }

    SUBCASE("non-OK grpc-status preserved") {
      auto completion = std::make_shared<CallCompletion>();
      CallState call(kMax, CallMode::kServerStreaming, completion, /*max_queued_messages=*/4, kMax);

      call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                             /*end_stream=*/false);
      std::string batch;
      for (int i = 0; i < 10; ++i) {
        batch += Frame("msg-" + std::to_string(i));
      }
      FeedData(&call, batch);
      call.OnTrailers({{"grpc-status", "7"}, {"grpc-message", "denied"}});
      call.OnClose(0);

      REQUIRE(call.is_draining());
      call.FinalizeDiscarding();
      CHECK(call.is_closed());

      const CallState::Result& r = completion->Wait();
      CHECK(r.code == StatusCode::kPermissionDenied);
      CHECK(r.message == "denied");

      std::string payload;
      size_t wire_bytes = 0;
      CHECK_FALSE(completion->PopMessage(&payload, &wire_bytes));
    }
  }

  TEST_CASE("CanAcceptMessage enforces exact queued_bytes + next_wire_bytes budget") {
    {
      CallCompletion completion;
      // Single-message exception: empty queue accepts even when > max_queued_bytes.
      CHECK(completion.CanAcceptMessage(/*next_wire_bytes=*/120, /*max_queued_messages=*/16,
                                        /*max_queued_bytes=*/100));
      completion.PushMessage(std::string(115, 'a'), /*wire_bytes=*/120);
      CHECK_FALSE(completion.CanAcceptMessage(/*next_wire_bytes=*/10, /*max_queued_messages=*/16,
                                              /*max_queued_bytes=*/100));
    }

    {
      CallCompletion completion;
      completion.PushMessage(std::string(55, 'b'), /*wire_bytes=*/60);
      // 60 + 40 <= 100: accepted.
      CHECK(completion.CanAcceptMessage(/*next_wire_bytes=*/40, /*max_queued_messages=*/16,
                                        /*max_queued_bytes=*/100));
      // 60 + 41 > 100: rejected.
      CHECK_FALSE(completion.CanAcceptMessage(/*next_wire_bytes=*/41, /*max_queued_messages=*/16,
                                              /*max_queued_bytes=*/100));
    }
  }

  TEST_CASE("IsGrpcContentType boundary check") {
    SUBCASE("application/grpcfoo is rejected as non-gRPC content-type") {
      CallState call(kMax);
      call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpcfoo"}},
                             /*end_stream=*/false);
      call.OnClose(0);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kInternal);
      CHECK(Contains(r.message, "unexpected content-type"));
    }

    SUBCASE("application/grpc+proto is accepted as gRPC") {
      CallState call(kMax);
      call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc+proto"}},
                             /*end_stream=*/false);
      const std::string frame = Frame("proto-ok");
      CHECK(call.OnData(reinterpret_cast<const uint8_t*>(frame.data()), frame.size()) == 0u);
      call.OnTrailers({{"grpc-status", "0"}});
      call.OnClose(0);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kOk);
      CHECK(r.response == "proto-ok");
    }

    SUBCASE("application/grpc; charset=utf-8 is accepted as gRPC") {
      CallState call(kMax);
      call.OnResponseHeaders(
          {{":status", "200"}, {"content-type", "application/grpc; charset=utf-8"}},
          /*end_stream=*/false);
      const std::string frame = Frame("param-ok");
      CHECK(call.OnData(reinterpret_cast<const uint8_t*>(frame.data()), frame.size()) == 0u);
      call.OnTrailers({{"grpc-status", "0"}});
      call.OnClose(0);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kOk);
      CHECK(r.response == "param-ok");
    }

    SUBCASE("streaming with application/grpc+proto and application/grpc; charset=utf-8") {
      const char* kValidTypes[] = {"application/grpc+proto", "application/grpc; charset=utf-8"};
      for (const char* ct : kValidTypes) {
        CAPTURE(ct);
        auto completion = std::make_shared<CallCompletion>();
        CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);
        call.OnResponseHeaders({{":status", "200"}, {"content-type", ct}}, /*end_stream=*/false);
        call.OnTrailers({{"grpc-status", "0"}});
        call.OnClose(0);

        const CallState::Result& r = completion->Wait();
        CHECK(r.code == StatusCode::kOk);
      }
    }
  }

  TEST_CASE("OnData returns ignored byte count on non-200 and non-gRPC responses") {
    SUBCASE("502 text/html ignores body bytes and preserves HTTP status mapping") {
      CallState call(kMax);
      call.OnResponseHeaders({{":status", "502"}, {"content-type", "text/html"}},
                             /*end_stream=*/false);
      const std::string html(1024, 'x');
      const size_t ignored =
          call.OnData(reinterpret_cast<const uint8_t*>(html.data()), html.size());
      CHECK(ignored == 1024u);
      call.OnClose(0);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kUnavailable);
    }

    SUBCASE("200 non-gRPC content-type ignores body bytes") {
      CallState call(kMax);
      call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpcfoo"}},
                             /*end_stream=*/false);
      const std::string body(256, 'y');
      const size_t ignored =
          call.OnData(reinterpret_cast<const uint8_t*>(body.data()), body.size());
      CHECK(ignored == 256u);
      call.OnClose(0);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kInternal);
    }

    SUBCASE("200 application/grpc consumes valid frame and returns 0 ignored bytes") {
      CallState call(kMax);
      call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                             /*end_stream=*/false);
      const std::string valid_frame = Frame("hello");
      const size_t ignored =
          call.OnData(reinterpret_cast<const uint8_t*>(valid_frame.data()), valid_frame.size());
      CHECK(ignored == 0u);
      call.OnTrailers({{"grpc-status", "0"}});
      call.OnClose(0);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kOk);
      CHECK(r.response == "hello");
    }
  }

  TEST_CASE("OnData framing violation after RequestCancel yields kCancelled") {
    int protocol_errors = 0;
    CallState::Hooks hooks;
    hooks.on_protocol_error = [&] { ++protocol_errors; };

    auto completion = std::make_shared<CallCompletion>();
    CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax, std::move(hooks));

    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    REQUIRE(completion->RequestCancel());

    // Compressed frame (flag = 1, length = 1, payload 'x'): preemptive cancel
    // must win over framing kInternal.
    FeedData(&call, RawPrefix(/*flag=*/1, /*declared_len=*/1) + "x");

    CHECK(call.is_closed());
    CHECK(protocol_errors == 1);
    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kCancelled);
  }

  TEST_CASE("Empty streaming queue admits one legal message exceeding max_queued_bytes") {
    int queue_pushes = 0;
    size_t last_queue_msgs = 0;
    size_t last_queue_bytes = 0;
    CallState::Hooks hooks;
    hooks.on_queue_depth = [&](size_t msgs, size_t bytes) {
      ++queue_pushes;
      last_queue_msgs = msgs;
      last_queue_bytes = bytes;
    };

    auto completion = std::make_shared<CallCompletion>();
    CallState call(/*max_receive_message_size=*/1024, CallMode::kServerStreaming, completion,
                   /*max_queued_messages=*/16, /*max_queued_bytes=*/32, std::move(hooks));

    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);

    const std::string msg1(200, 'a');
    const std::string msg2(200, 'b');
    FeedData(&call, Frame(msg1) + Frame(msg2));

    // Only the first 200-byte message (205 wire bytes) is admitted into the
    // empty delivery queue; the second stays buffered in scanner_.
    CHECK(queue_pushes == 1);
    CHECK(last_queue_msgs == 1u);
    CHECK(last_queue_bytes == 205u);
    CHECK_FALSE(completion->CanAcceptMessage(/*next_wire_bytes=*/205, /*max_queued_messages=*/16,
                                             /*max_queued_bytes=*/32));

    std::string payload;
    size_t wire_bytes = 0;
    REQUIRE(completion->PopMessage(&payload, &wire_bytes));
    CHECK(payload == msg1);
    CHECK(wire_bytes == 205u);

    // Before DrainScannerToQueue(), msg2 has not been pushed yet.
    CHECK(queue_pushes == 1);

    call.DrainScannerToQueue();
    CHECK(queue_pushes == 2);
    CHECK(last_queue_msgs == 1u);
    CHECK(last_queue_bytes == 205u);

    REQUIRE(completion->PopMessage(&payload, &wire_bytes));
    CHECK(payload == msg2);
    CHECK(wire_bytes == 205u);

    call.OnTrailers({{"grpc-status", "0"}});
    call.OnClose(0);
    CHECK_FALSE(completion->PopMessage(&payload, &wire_bytes));
    CHECK(call.Wait().code == StatusCode::kOk);
  }

  // --- StatusOrigin (design §4.6: resilient_stream must distinguish a
  // locally-originated CANCELLED from a server-originated one) -------------

  TEST_CASE("StatusOrigin: server trailers grpc-status 0 is kServer") {
    SUBCASE("unary with one response message") {
      CallState call(kMax);
      call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                             /*end_stream=*/false);
      FeedData(&call, Frame("ok"));
      call.OnTrailers({{"grpc-status", "0"}});
      call.OnClose(0);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kOk);
      CHECK(r.response == "ok");
      CHECK(r.origin == StatusOrigin::kServer);
    }

    SUBCASE("server streaming") {
      auto completion = std::make_shared<CallCompletion>();
      CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);
      call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                             /*end_stream=*/false);
      call.OnTrailers({{"grpc-status", "0"}});
      call.OnClose(0);

      const CallState::Result& r = completion->Wait();
      CHECK(r.code == StatusCode::kOk);
      CHECK(r.origin == StatusOrigin::kServer);
    }
  }

  TEST_CASE("StatusOrigin: server trailers grpc-status 1 is kCancelled from kServer") {
    auto completion = std::make_shared<CallCompletion>();
    CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    call.OnTrailers({{"grpc-status", "1"}, {"grpc-message", "server%20cancelled"}});
    call.OnClose(0);

    const CallState::Result& r = completion->Wait();
    CHECK(r.code == StatusCode::kCancelled);
    CHECK(r.message == "server cancelled");
    CHECK(r.origin == StatusOrigin::kServer);
  }

  TEST_CASE("StatusOrigin: trailers-only grpc-status 1 is kCancelled from kServer") {
    SUBCASE("unary") {
      CallState call(kMax);
      call.OnResponseHeaders(
          {{":status", "200"}, {"content-type", "application/grpc"}, {"grpc-status", "1"}},
          /*end_stream=*/true);
      call.OnClose(0);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kCancelled);
      CHECK(r.origin == StatusOrigin::kServer);
    }

    SUBCASE("server streaming") {
      auto completion = std::make_shared<CallCompletion>();
      CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);
      call.OnResponseHeaders(
          {{":status", "200"}, {"content-type", "application/grpc"}, {"grpc-status", "1"}},
          /*end_stream=*/true);
      call.OnClose(0);

      const CallState::Result& r = completion->Wait();
      CHECK(r.code == StatusCode::kCancelled);
      CHECK(r.origin == StatusOrigin::kServer);
    }
  }

  TEST_CASE("StatusOrigin: server RST_STREAM(CANCEL) without trailers is kCancelled from kServer") {
    SUBCASE("before any response headers") {
      CallState call(kMax);
      call.OnClose(egrpc::internal::Http2Session::kHttp2Cancel);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kCancelled);
      CHECK(r.origin == StatusOrigin::kServer);
    }

    SUBCASE("after 200 response headers on a streaming call") {
      auto completion = std::make_shared<CallCompletion>();
      CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);
      call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                             /*end_stream=*/false);
      call.OnClose(egrpc::internal::Http2Session::kHttp2Cancel);

      const CallState::Result& r = completion->Wait();
      CHECK(r.code == StatusCode::kCancelled);
      CHECK(r.origin == StatusOrigin::kServer);
    }
  }

  TEST_CASE("StatusOrigin: non-200 HTTP status fallback is kServer") {
    CallState call(kMax);
    call.OnResponseHeaders({{":status", "503"}, {"content-type", "text/plain"}},
                           /*end_stream=*/false);
    FeedData(&call, "service unavailable");
    call.OnClose(0);

    const CallState::Result& r = call.Wait();
    CHECK(r.code == StatusCode::kUnavailable);
    CHECK(r.origin == StatusOrigin::kServer);
  }

  TEST_CASE("StatusOrigin: FailLocal(kCancelled) is kCancelled from kLocal") {
    auto completion = std::make_shared<CallCompletion>();
    CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    call.FailLocal(StatusCode::kCancelled, "Cancelled");
    CHECK(call.is_closed());

    // Late server events after local close must not rewrite the result.
    call.OnTrailers({{"grpc-status", "0"}});
    call.OnClose(0);

    const CallState::Result& r = completion->Wait();
    CHECK(r.code == StatusCode::kCancelled);
    CHECK(r.message == "Cancelled");
    CHECK(r.origin == StatusOrigin::kLocal);
  }

  TEST_CASE("StatusOrigin: TryPreemptTerminal after RequestCancel is kCancelled from kLocal") {
    auto completion = std::make_shared<CallCompletion>();
    CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);
    REQUIRE(completion->RequestCancel());

    CHECK(call.TryPreemptTerminal());
    CHECK(call.is_closed());
    // The wire close that follows must be a no-op.
    call.OnClose(0);

    const CallState::Result& r = completion->Wait();
    CHECK(r.code == StatusCode::kCancelled);
    CHECK(r.origin == StatusOrigin::kLocal);
  }

  TEST_CASE("StatusOrigin: TryPreemptTerminal after elapsed deadline is kDeadlineExceeded/kLocal") {
    auto completion = std::make_shared<CallCompletion>();
    CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);
    call.set_deadline(std::chrono::steady_clock::now() - std::chrono::milliseconds(1));
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);

    CHECK(call.TryPreemptTerminal());
    CHECK(call.is_closed());
    call.OnClose(0);

    const CallState::Result& r = completion->Wait();
    CHECK(r.code == StatusCode::kDeadlineExceeded);
    CHECK(r.origin == StatusOrigin::kLocal);
  }

  TEST_CASE("StatusOrigin: TryPreemptTerminal without cancel or elapsed deadline keeps kServer") {
    // Edge case: no preemption → returns false, call stays live, and the
    // server's later status (even CANCELLED) is attributed to the server.
    auto completion = std::make_shared<CallCompletion>();
    CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);
    call.set_deadline(std::chrono::steady_clock::now() + std::chrono::hours(1));
    call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                           /*end_stream=*/false);

    CHECK_FALSE(call.TryPreemptTerminal());
    CHECK_FALSE(call.is_closed());

    call.OnTrailers({{"grpc-status", "1"}});
    call.OnClose(0);

    const CallState::Result& r = completion->Wait();
    CHECK(r.code == StatusCode::kCancelled);
    CHECK(r.origin == StatusOrigin::kServer);
  }

  TEST_CASE("StatusOrigin: TryPreemptTerminal never overwrites an already-closed server status") {
    // Edge case: cancel requested after the server already closed the call;
    // the server-originated status and origin must stand.
    auto completion = std::make_shared<CallCompletion>();
    CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax);
    call.OnResponseHeaders(
        {{":status", "200"}, {"content-type", "application/grpc"}, {"grpc-status", "1"}},
        /*end_stream=*/true);
    call.OnClose(0);
    REQUIRE(call.is_closed());

    REQUIRE(completion->RequestCancel());
    CHECK_FALSE(call.TryPreemptTerminal());
    call.FailLocal(StatusCode::kCancelled, "Cancelled");

    const CallState::Result& r = completion->Wait();
    CHECK(r.code == StatusCode::kCancelled);
    CHECK(r.origin == StatusOrigin::kServer);
  }

  TEST_CASE("StatusOrigin: local framing violations are kLocal") {
    SUBCASE("compressed flag = 1 fails with kInternal") {
      int protocol_errors = 0;
      CallState::Hooks hooks;
      hooks.on_protocol_error = [&] { ++protocol_errors; };
      auto completion = std::make_shared<CallCompletion>();
      CallState call(kMax, CallMode::kServerStreaming, completion, 16, kMax, std::move(hooks));
      call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                             /*end_stream=*/false);
      FeedData(&call, RawPrefix(/*flag=*/1, /*declared_len=*/1) + "x");

      CHECK(call.is_closed());
      CHECK(protocol_errors == 1);
      // Server trailers arriving afterwards must not flip origin to kServer.
      call.OnTrailers({{"grpc-status", "0"}});
      call.OnClose(0);

      const CallState::Result& r = completion->Wait();
      CHECK(r.code == StatusCode::kInternal);
      CHECK(r.origin == StatusOrigin::kLocal);
    }

    SUBCASE("oversize message fails with kResourceExhausted") {
      int protocol_errors = 0;
      CallState::Hooks hooks;
      hooks.on_protocol_error = [&] { ++protocol_errors; };
      CallState call(/*max_receive_message_size=*/16, CallMode::kUnary, nullptr, 16, kMax,
                     std::move(hooks));
      call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                             /*end_stream=*/false);
      FeedData(&call, RawPrefix(/*flag=*/0, /*declared_len=*/17));

      CHECK(call.is_closed());
      CHECK(protocol_errors == 1);
      call.OnClose(0);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kResourceExhausted);
      CHECK(r.origin == StatusOrigin::kLocal);
    }

    SUBCASE("truncated frame at clean close fails with kInternal") {
      // Edge case: framing error detected only at finalize (no grpc-status).
      CallState call(kMax);
      call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                             /*end_stream=*/false);
      FeedData(&call, RawPrefix(/*flag=*/0, /*declared_len=*/10) + "abc");
      call.OnClose(0);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kInternal);
      CHECK(r.origin == StatusOrigin::kLocal);
    }

    SUBCASE("unary grpc-status 0 without a response message fails with kInternal") {
      // Edge case: the server said OK, but the client rejects the protocol
      // violation locally.
      CallState call(kMax);
      call.OnResponseHeaders({{":status", "200"}, {"content-type", "application/grpc"}},
                             /*end_stream=*/false);
      call.OnTrailers({{"grpc-status", "0"}});
      call.OnClose(0);

      const CallState::Result& r = call.Wait();
      CHECK(r.code == StatusCode::kInternal);
      CHECK(r.origin == StatusOrigin::kLocal);
    }
  }
}  // TEST_SUITE("call_state")
