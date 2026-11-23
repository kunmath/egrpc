// egrpc M5 integration probe (design §5.4, §8/M5): exercises ChannelImpl
// directly so pytest can configure a small max_receive_message_size /
// stream_window_slack / max_queued_messages and assert the internal memory
// high-water counters against a fast upstream grpcio server.
//
// Usage:
//   egrpc_stream_probe --host HOST --port PORT --ca-file CERT
//                      [--mode backpressure|shutdown_race|
//                              502_large_body_then_ok|burst_then_close|
//                              no_settings|drop_channel_blocked_read|
//                              queued_after_malformed|conn_error_no_deadline|
//                              trailers_only_zero_window|shutdown_draining_call]
//                      [--max-msg-bytes N] [--slack-bytes N]
//                      [--max-queued-msgs N] [--max-queued-bytes N]
//                      [--count N] [--payload-bytes N]
//                      [--read-delay-ms N]
//
// Output:
//   BACKPRESSURE code=<int> messages=<n> window=<w> max_reassembly=<r>
//                max_queued_msgs=<qm> max_queued_bytes=<qb>
//   SHUTDOWN_RACE ok=<0|1> / SHUTDOWN_MS=<ms> / POST_SHUTDOWN_STATUS=<code>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "core/call_state.h"
#include "core/channel_impl.h"
#include "route_guide.pb.h"

namespace {

int RunShutdownRace(const std::string& host, uint16_t port, const std::string& ca_file) {
  egrpc::internal::TlsConfig tls;
  tls.ca_bundle_path = ca_file;
  egrpc::internal::ChannelOptions opts;
  egrpc::internal::ChannelImpl channel(host, port, std::move(tls), opts);

  routeguide::Point pt;
  pt.set_latitude(1000);
  pt.set_longitude(2000);
  std::string req_bytes;
  if (!pt.SerializeToString(&req_bytes)) {
    std::fprintf(stderr, "failed to serialize Point\n");
    return 2;
  }

  std::atomic<bool> stop{false};
  std::atomic<bool> all_valid{true};
  std::atomic<uint64_t> total_calls{0};
  std::atomic<uint64_t> latest_call_id{0};

  constexpr int kCallerThreads = 8;
  std::vector<std::thread> workers;
  workers.reserve(kCallerThreads + 1);
  for (int t = 0; t < kCallerThreads; ++t) {
    workers.emplace_back([&, t] {
      for (int iter = 0; !stop.load(std::memory_order_acquire); ++iter) {
        auto handle = channel.StartCall(egrpc::internal::CallMode::kUnary,
                                        "/routeguide.RouteGuide/GetFeature", req_bytes, {},
                                        std::chrono::steady_clock::now() + std::chrono::seconds(5));
        latest_call_id.store(handle.call_id, std::memory_order_relaxed);
        if ((t + iter) % 3 == 0) {
          std::this_thread::sleep_for(std::chrono::milliseconds(2));
        }
        channel.CancelCall(handle.call_id);
        const auto& res = handle.completion->Wait();
        if (res.code != egrpc::internal::StatusCode::kOk &&
            res.code != egrpc::internal::StatusCode::kCancelled &&
            res.code != egrpc::internal::StatusCode::kUnavailable) {
          all_valid.store(false, std::memory_order_relaxed);
        }
        total_calls.fetch_add(1, std::memory_order_relaxed);
      }
    });
  }
  // Dedicated canceller firing CancelCall() concurrently with StartCall() and Shutdown().
  workers.emplace_back([&] {
    while (!stop.load(std::memory_order_acquire)) {
      const uint64_t cid = latest_call_id.load(std::memory_order_relaxed);
      if (cid != 0) channel.CancelCall(cid);
      std::this_thread::yield();
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  const auto t_shutdown = std::chrono::steady_clock::now();
  std::thread s1([&] { channel.Shutdown(); });
  std::thread s2([&] { channel.Shutdown(); });
  s1.join();
  s2.join();
  const auto shutdown_ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                               std::chrono::steady_clock::now() - t_shutdown)
                               .count();

  auto post_handle = channel.StartCall(egrpc::internal::CallMode::kUnary,
                                       "/routeguide.RouteGuide/GetFeature", req_bytes);
  const int post_status = static_cast<int>(post_handle.completion->Wait().code);

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  stop.store(true, std::memory_order_release);
  for (auto& w : workers) w.join();

  const bool ok = all_valid.load(std::memory_order_relaxed) &&
                  total_calls.load(std::memory_order_relaxed) > 0 &&
                  post_status == static_cast<int>(egrpc::internal::StatusCode::kUnavailable);
  std::printf("SHUTDOWN_RACE ok=%d\n", ok ? 1 : 0);
  std::printf("SHUTDOWN_MS=%lld\n", static_cast<long long>(shutdown_ms));
  std::printf("POST_SHUTDOWN_STATUS=%d\n", post_status);
  return ok ? 0 : 1;
}

int Run502LargeBodyThenOk(const std::string& host, uint16_t port, const std::string& ca_file) {
  egrpc::internal::TlsConfig tls;
  tls.ca_bundle_path = ca_file;

  egrpc::internal::ChannelOptions opts;
  opts.max_receive_message_size = 4096;
  opts.stream_window_slack = 0;  // stream window = 4096 + 5 = 4101 B < 32 KiB body
  opts.max_queued_messages = 2;
  opts.max_queued_bytes = 4096;

  egrpc::internal::ChannelImpl channel(host, port, std::move(tls), opts);

  routeguide::Rectangle rect;
  std::string req_bytes;
  if (!rect.SerializeToString(&req_bytes)) {
    std::fprintf(stderr, "failed to serialize Rectangle\n");
    return 2;
  }

  auto handle1 = channel.StartCall(egrpc::internal::CallMode::kServerStreaming,
                                   "/routeguide.RouteGuide/ListFeatures", req_bytes, {},
                                   std::chrono::steady_clock::now() + std::chrono::seconds(10));
  std::string raw_msg;
  size_t wire_bytes = 0;
  while (handle1.completion->PopMessage(&raw_msg, &wire_bytes)) {
    channel.NotifyReadConsumed(handle1.call_id, wire_bytes);
  }
  const auto& res1 = handle1.completion->Wait();
  const int call1_status = static_cast<int>(res1.code);

  auto handle2 = channel.StartCall(egrpc::internal::CallMode::kServerStreaming,
                                   "/routeguide.RouteGuide/ListFeatures", req_bytes, {},
                                   std::chrono::steady_clock::now() + std::chrono::seconds(10));
  int call2_messages = 0;
  bool call2_payload_ok = true;
  while (handle2.completion->PopMessage(&raw_msg, &wire_bytes)) {
    routeguide::Feature f;
    if (!f.ParseFromString(raw_msg) || f.name() != "ok-after-502") {
      call2_payload_ok = false;
    }
    ++call2_messages;
    channel.NotifyReadConsumed(handle2.call_id, wire_bytes);
  }
  const auto& res2 = handle2.completion->Wait();
  const int call2_status = static_cast<int>(res2.code);

  const uint32_t window = channel.stream_window_size();
  channel.Shutdown();

  std::printf("STREAM_WINDOW=%u\n", window);
  std::printf("CALL1_STATUS=%d\n", call1_status);
  std::printf("CALL2_STATUS=%d\n", call2_status);
  std::printf("CALL2_MESSAGES=%d\n", call2_messages);
  return (window == 4101 &&
          call1_status == static_cast<int>(egrpc::internal::StatusCode::kUnavailable) &&
          call2_status == static_cast<int>(egrpc::internal::StatusCode::kOk) &&
          call2_messages == 1 && call2_payload_ok)
             ? 0
             : 1;
}

int RunBurstThenClose(const std::string& host, uint16_t port, const std::string& ca_file) {
  egrpc::internal::TlsConfig tls;
  tls.ca_bundle_path = ca_file;

  egrpc::internal::ChannelOptions opts;
  opts.max_receive_message_size = 65536;
  opts.max_queued_messages = 2;
  opts.max_queued_bytes = 65536;

  egrpc::internal::ChannelImpl channel(host, port, std::move(tls), opts);

  routeguide::Rectangle rect;
  std::string req_bytes;
  if (!rect.SerializeToString(&req_bytes)) {
    std::fprintf(stderr, "failed to serialize Rectangle\n");
    return 2;
  }

  auto handle = channel.StartCall(egrpc::internal::CallMode::kServerStreaming,
                                  "/routeguide.RouteGuide/ListFeatures", req_bytes, {},
                                  std::chrono::steady_clock::now() + std::chrono::seconds(10));

  // Give the server time to send all 30 messages + trailers + close TLS socket.
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  int received = 0;
  bool payload_ok = true;
  std::string raw_msg;
  size_t wire_bytes = 0;
  while (handle.completion->PopMessage(&raw_msg, &wire_bytes)) {
    routeguide::Feature f;
    if (!f.ParseFromString(raw_msg) || f.name() != "msg-" + std::to_string(received)) {
      payload_ok = false;
    }
    ++received;
    channel.NotifyReadConsumed(handle.call_id, wire_bytes);
    std::this_thread::sleep_for(std::chrono::milliseconds(2));
  }

  const auto& res = handle.completion->Wait();
  const int code = static_cast<int>(res.code);
  channel.Shutdown();

  std::printf("MESSAGES=%d\n", received);
  std::printf("STATUS=%d\n", code);
  return (received == 30 && code == static_cast<int>(egrpc::internal::StatusCode::kOk) &&
          payload_ok)
             ? 0
             : 1;
}

int RunNoSettings(const std::string& host, uint16_t port, const std::string& ca_file) {
  egrpc::internal::TlsConfig tls;
  tls.ca_bundle_path = ca_file;

  egrpc::internal::ChannelOptions opts;
  opts.connect_timeout = std::chrono::milliseconds(300);

  egrpc::internal::ChannelImpl channel(host, port, std::move(tls), opts);

  routeguide::Point pt;
  pt.set_latitude(1000);
  pt.set_longitude(2000);
  std::string req_bytes;
  if (!pt.SerializeToString(&req_bytes)) {
    std::fprintf(stderr, "failed to serialize Point\n");
    return 2;
  }

  const auto t0 = std::chrono::steady_clock::now();
  const auto res = channel.UnaryCall("/routeguide.RouteGuide/GetFeature", req_bytes);
  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
          .count();
  channel.Shutdown();

  const int code = static_cast<int>(res.code);
  std::printf("STATUS=%d\n", code);
  std::printf("MESSAGE=%s\n", res.message.c_str());
  std::printf("ELAPSED_MS=%lld\n", static_cast<long long>(elapsed_ms));
  return code == static_cast<int>(egrpc::internal::StatusCode::kUnavailable) ? 0 : 1;
}

int RunDropChannelBlockedRead(const std::string& host, uint16_t port, const std::string& ca_file) {
  egrpc::internal::TlsConfig tls;
  tls.ca_bundle_path = ca_file;
  egrpc::internal::ChannelOptions opts;

  auto channel = std::make_unique<egrpc::internal::ChannelImpl>(host, port, std::move(tls), opts);

  routeguide::Rectangle rect;
  std::string req_bytes;
  if (!rect.SerializeToString(&req_bytes)) {
    std::fprintf(stderr, "failed to serialize Rectangle\n");
    return 2;
  }

  auto handle = channel->StartCall(egrpc::internal::CallMode::kServerStreaming,
                                   "/routeguide.RouteGuide/ListFeatures", req_bytes, {},
                                   std::chrono::steady_clock::now() + std::chrono::seconds(20));

  std::mutex mu;
  std::condition_variable cv;
  bool reader_entered = false;
  bool read_ret = true;
  int status_code = -1;
  std::chrono::steady_clock::time_point unblock_time;

  auto completion = handle.completion;
  std::thread reader([&] {
    {
      std::lock_guard<std::mutex> lock(mu);
      reader_entered = true;
    }
    cv.notify_one();
    std::string raw_msg;
    size_t wire_bytes = 0;
    read_ret = completion->PopMessage(&raw_msg, &wire_bytes);
    const auto& res = completion->Wait();
    status_code = static_cast<int>(res.code);
    unblock_time = std::chrono::steady_clock::now();
  });

  {
    std::unique_lock<std::mutex> lock(mu);
    cv.wait(lock, [&] { return reader_entered; });
  }
  // Give the reader thread time to block on CallCompletion::cv_ inside PopMessage().
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  const auto t_shutdown = std::chrono::steady_clock::now();
  channel.reset();  // destroys ChannelImpl (calls Shutdown()) while reader is blocked
  reader.join();

  const auto unblock_ms = std::max<long long>(
      0, std::chrono::duration_cast<std::chrono::milliseconds>(unblock_time - t_shutdown).count());
  std::printf("READ_RET=%d\n", read_ret ? 1 : 0);
  std::printf("STATUS=%d\n", status_code);
  std::printf("UNBLOCK_MS=%lld\n", static_cast<long long>(unblock_ms));
  return (!read_ret && status_code == static_cast<int>(egrpc::internal::StatusCode::kUnavailable))
             ? 0
             : 1;
}

int RunQueuedAfterMalformed(const std::string& host, uint16_t port, const std::string& ca_file) {
  egrpc::internal::TlsConfig tls;
  tls.ca_bundle_path = ca_file;
  egrpc::internal::ChannelOptions opts;
  egrpc::internal::ChannelImpl channel(host, port, std::move(tls), opts);

  const auto t0 = std::chrono::steady_clock::now();
  auto handle1 =
      channel.StartCall(egrpc::internal::CallMode::kUnary, "/test.Stream/MalformedThenOk", "");
  auto handle2 =
      channel.StartCall(egrpc::internal::CallMode::kUnary, "/test.Stream/MalformedThenOk", "");

  const auto& res1 = handle1.completion->Wait();
  const auto& res2 = handle2.completion->Wait();
  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
          .count();
  channel.Shutdown();

  const int call1_status = static_cast<int>(res1.code);
  const int call2_status = static_cast<int>(res2.code);
  std::printf("CALL1_STATUS=%d\n", call1_status);
  std::printf("CALL2_STATUS=%d\n", call2_status);
  std::printf("CALL2_MSG=%s\n", res2.response.c_str());
  std::printf("ELAPSED_MS=%lld\n", static_cast<long long>(elapsed_ms));
  // libnghttp2 < 1.67 resets only stream 1 (Call 1 fails, Call 2 succeeds);
  // libnghttp2 >= 1.67 (CVE-2025-8671) terminates the session with GOAWAY
  // (both calls fail promptly with UNAVAILABLE). Pytest correlates the exact
  // wire frame (RST_STREAM vs GOAWAY) with the observed call outcomes.
  const bool stream_reset_ok =
      call1_status == static_cast<int>(egrpc::internal::StatusCode::kInternal) &&
      call2_status == static_cast<int>(egrpc::internal::StatusCode::kOk) &&
      res2.response == "second-ok";
  const bool conn_terminated_ok =
      call1_status == static_cast<int>(egrpc::internal::StatusCode::kUnavailable) &&
      call2_status == static_cast<int>(egrpc::internal::StatusCode::kUnavailable) &&
      res2.response.empty();
  return (stream_reset_ok || conn_terminated_ok) ? 0 : 1;
}

int RunConnErrorNoDeadline(const std::string& host, uint16_t port, const std::string& ca_file) {
  egrpc::internal::TlsConfig tls;
  tls.ca_bundle_path = ca_file;
  egrpc::internal::ChannelOptions opts;  // default 300 s keepalive
  egrpc::internal::ChannelImpl channel(host, port, std::move(tls), opts);

  const auto t0 = std::chrono::steady_clock::now();
  auto handle = channel.StartCall(egrpc::internal::CallMode::kServerStreaming,
                                  "/test.Stream/ConnProtocolErrorThenPing", "");
  std::string raw_msg;
  size_t wire_bytes = 0;
  while (handle.completion->PopMessage(&raw_msg, &wire_bytes)) {
    channel.NotifyReadConsumed(handle.call_id, wire_bytes);
  }
  const auto& res = handle.completion->Wait();
  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
          .count();
  channel.Shutdown();

  const int code = static_cast<int>(res.code);
  std::printf("STATUS=%d\n", code);
  std::printf("ELAPSED_MS=%lld\n", static_cast<long long>(elapsed_ms));
  return code == static_cast<int>(egrpc::internal::StatusCode::kUnavailable) ? 0 : 1;
}

int RunTrailersOnlyZeroWindow(const std::string& host, uint16_t port, const std::string& ca_file) {
  egrpc::internal::TlsConfig tls;
  tls.ca_bundle_path = ca_file;
  egrpc::internal::ChannelOptions opts;
  egrpc::internal::ChannelImpl channel(host, port, std::move(tls), opts);

  const auto t0 = std::chrono::steady_clock::now();
  const auto res = channel.UnaryCall("/test.Stream/TrailersOnlyDenied", std::string(64, 'x'));
  const auto elapsed_ms =
      std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
          .count();
  channel.Shutdown();

  const int code = static_cast<int>(res.code);
  std::printf("STATUS=%d\n", code);
  std::printf("MESSAGE=%s\n", res.message.c_str());
  std::printf("ELAPSED_MS=%lld\n", static_cast<long long>(elapsed_ms));
  return (code == static_cast<int>(egrpc::internal::StatusCode::kPermissionDenied) &&
          res.message == "denied")
             ? 0
             : 1;
}

int RunShutdownDrainingCall(const std::string& host, uint16_t port, const std::string& ca_file) {
  egrpc::internal::TlsConfig tls;
  tls.ca_bundle_path = ca_file;

  egrpc::internal::ChannelOptions opts;
  opts.max_receive_message_size = 65536;
  opts.max_queued_messages = 2;
  opts.max_queued_bytes = 65536;

  egrpc::internal::ChannelImpl channel(host, port, std::move(tls), opts);

  auto handle =
      channel.StartCall(egrpc::internal::CallMode::kServerStreaming, "/test.Stream/BurstThenClose",
                        "", {}, std::chrono::steady_clock::now() + std::chrono::seconds(10));
  handle.completion->WaitForInitialMetadata();
  // Wait for all burst messages + trailers + END_STREAM to arrive so 2 sit in
  // queue_ and the remaining messages sit in scanner_ (call in draining_calls_).
  std::this_thread::sleep_for(std::chrono::milliseconds(150));

  int received = 0;
  std::string raw_msg;
  size_t wire_bytes = 0;
  if (handle.completion->PopMessage(&raw_msg, &wire_bytes)) {
    ++received;
    channel.NotifyReadConsumed(handle.call_id, wire_bytes);
  }

  channel.Shutdown();

  while (handle.completion->PopMessage(&raw_msg, &wire_bytes)) {
    ++received;
  }
  const auto& res = handle.completion->Wait();
  const int code = static_cast<int>(res.code);
  std::printf("RECEIVED=%d\n", received);
  std::printf("STATUS=%d\n", code);
  return (received < 8 && code == static_cast<int>(egrpc::internal::StatusCode::kUnavailable)) ? 0
                                                                                               : 1;
}

}  // namespace

int main(int argc, char** argv) {
  std::string host = "localhost";
  uint16_t port = 0;
  std::string ca_file;
  std::string mode = "backpressure";
  size_t max_msg_bytes = 8192;
  size_t slack_bytes = 4096;
  size_t max_queued_msgs = 4;
  size_t max_queued_bytes = 0;
  int32_t count = 60;
  int32_t payload_bytes = 4096;
  int read_delay_ms = 5;

  for (int i = 1; i + 1 < argc; i += 2) {
    if (std::strcmp(argv[i], "--host") == 0) {
      host = argv[i + 1];
    } else if (std::strcmp(argv[i], "--port") == 0) {
      port = static_cast<uint16_t>(std::atoi(argv[i + 1]));
    } else if (std::strcmp(argv[i], "--ca-file") == 0) {
      ca_file = argv[i + 1];
    } else if (std::strcmp(argv[i], "--mode") == 0) {
      mode = argv[i + 1];
    } else if (std::strcmp(argv[i], "--max-msg-bytes") == 0) {
      max_msg_bytes = static_cast<size_t>(std::strtoull(argv[i + 1], nullptr, 10));
    } else if (std::strcmp(argv[i], "--slack-bytes") == 0) {
      slack_bytes = static_cast<size_t>(std::strtoull(argv[i + 1], nullptr, 10));
    } else if (std::strcmp(argv[i], "--max-queued-msgs") == 0) {
      max_queued_msgs = static_cast<size_t>(std::strtoull(argv[i + 1], nullptr, 10));
    } else if (std::strcmp(argv[i], "--max-queued-bytes") == 0) {
      max_queued_bytes = static_cast<size_t>(std::strtoull(argv[i + 1], nullptr, 10));
    } else if (std::strcmp(argv[i], "--count") == 0) {
      count = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--payload-bytes") == 0) {
      payload_bytes = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--read-delay-ms") == 0) {
      read_delay_ms = std::atoi(argv[i + 1]);
    } else {
      std::fprintf(stderr, "unknown flag: %s\n", argv[i]);
      return 2;
    }
  }
  if (port == 0 || ca_file.empty()) {
    std::fprintf(stderr, "usage: %s --host H --port P --ca-file CERT [...]\n", argv[0]);
    return 2;
  }

  if (mode == "shutdown_race") {
    return RunShutdownRace(host, port, ca_file);
  }
  if (mode == "502_large_body_then_ok") {
    return Run502LargeBodyThenOk(host, port, ca_file);
  }
  if (mode == "burst_then_close") {
    return RunBurstThenClose(host, port, ca_file);
  }
  if (mode == "no_settings") {
    return RunNoSettings(host, port, ca_file);
  }
  if (mode == "drop_channel_blocked_read") {
    return RunDropChannelBlockedRead(host, port, ca_file);
  }
  if (mode == "queued_after_malformed") {
    return RunQueuedAfterMalformed(host, port, ca_file);
  }
  if (mode == "conn_error_no_deadline") {
    return RunConnErrorNoDeadline(host, port, ca_file);
  }
  if (mode == "trailers_only_zero_window") {
    return RunTrailersOnlyZeroWindow(host, port, ca_file);
  }
  if (mode == "shutdown_draining_call") {
    return RunShutdownDrainingCall(host, port, ca_file);
  }
  if (mode != "backpressure") {
    std::fprintf(stderr, "unknown mode: %s\n", mode.c_str());
    return 2;
  }

  egrpc::internal::TlsConfig tls;
  tls.ca_bundle_path = ca_file;

  egrpc::internal::ChannelOptions opts;
  opts.max_receive_message_size = max_msg_bytes;
  opts.stream_window_slack = slack_bytes;
  opts.max_queued_messages = max_queued_msgs;
  opts.max_queued_bytes = max_queued_bytes;

  egrpc::internal::ChannelImpl channel(host, port, std::move(tls), opts);

  routeguide::Rectangle rect;
  rect.mutable_lo()->set_latitude(5);  // fast producer mode on RouteGuideTestServer
  rect.mutable_hi()->set_latitude(count);
  rect.mutable_hi()->set_longitude(payload_bytes);
  std::string req_bytes;
  if (!rect.SerializeToString(&req_bytes)) {
    std::fprintf(stderr, "failed to serialize Rectangle\n");
    return 2;
  }

  auto handle = channel.StartCall(egrpc::internal::CallMode::kServerStreaming,
                                  "/routeguide.RouteGuide/ListFeatures", req_bytes, {},
                                  std::chrono::steady_clock::now() + std::chrono::seconds(20));

  int received = 0;
  bool payload_ok = true;
  std::string raw_msg;
  size_t wire_bytes = 0;
  while (handle.completion->PopMessage(&raw_msg, &wire_bytes)) {
    if (read_delay_ms > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(read_delay_ms));
    }
    routeguide::Feature f;
    if (!f.ParseFromString(raw_msg) || f.location().latitude() != received ||
        static_cast<int32_t>(f.name().size()) != payload_bytes) {
      payload_ok = false;
    }
    ++received;
    channel.NotifyReadConsumed(handle.call_id, wire_bytes);
  }

  const auto& result = handle.completion->Wait();
  const int code = static_cast<int>(result.code);
  std::printf(
      "BACKPRESSURE code=%d messages=%d payload_ok=%d window=%u max_reassembly=%zu "
      "max_queued_msgs=%zu max_queued_bytes=%zu\n",
      code, received, payload_ok ? 1 : 0, channel.stream_window_size(),
      channel.max_reassembly_bytes(), channel.max_queued_messages(), channel.max_queued_bytes());

  channel.Shutdown();
  return (code == 0 && received == count && payload_ok) ? 0 : 1;
}
