// egrpc M4/M5 acceptance example (design §8): unary + server-streaming paths
// of the canonical gRPC route_guide client, built against UNMODIFIED
// grpc_cpp_plugin-generated stubs (gen/route_guide.grpc.pb.{h,cc}) with
// egrpc's grpcpp shim underneath.
//
// Usage: egrpc_route_guide_client --target host:port --ca-file cert.pem
//                                 [--mode getfeature|listfeatures|concurrent|
//                                         unimplemented_streams|cancel_prestart|
//                                         early_finish|drop_reader|
//                                         queued_deadline|mixed_concurrent|
//                                         drop_channel_mid_stream|
//                                         framing_error_then_unary|
//                                         early_finish_then_unary|
//                                         draining_early_finish|
//                                         next_size_nonblocking]
//                                 [--latitude N] [--longitude N]
//                                 [--lo-lat N] [--lo-lon N] [--hi-lat N] [--hi-lon N]
//                                 [--deadline-ms N] [--cancel-after-ms N]
//                                 [--stop-after N] [--threads N]
//                                 [--wait-init 0|1] [--check-next-size 0|1]

#include <grpcpp/grpcpp.h>

#include <atomic>
#include <chrono>
#include <cstdint>
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
using routeguide::RouteNote;
using routeguide::RouteSummary;

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

Rectangle MakeRect(int32_t lo_lat, int32_t lo_lon, int32_t hi_lat, int32_t hi_lon) {
  Rectangle rect;
  rect.mutable_lo()->set_latitude(lo_lat);
  rect.mutable_lo()->set_longitude(lo_lon);
  rect.mutable_hi()->set_latitude(hi_lat);
  rect.mutable_hi()->set_longitude(hi_lon);
  return rect;
}

class RouteGuideClient {
 public:
  explicit RouteGuideClient(std::shared_ptr<Channel> channel)
      : stub_(RouteGuide::NewStub(channel)) {}

  // Returns true when the RPC succeeded (an empty feature name is a valid
  // "nothing there" answer, matching the route_guide service contract).
  bool GetFeature(int32_t latitude, int32_t longitude, int deadline_ms, int cancel_after_ms,
                  bool verbose) {
    Point point;
    point.set_latitude(latitude);
    point.set_longitude(longitude);

    ClientContext context;
    if (deadline_ms > 0) {
      context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::milliseconds(deadline_ms));
    }

    std::thread canceller;
    if (cancel_after_ms > 0) {
      canceller = std::thread([&context, cancel_after_ms] {
        std::this_thread::sleep_for(std::chrono::milliseconds(cancel_after_ms));
        context.TryCancel();
      });
    }

    const auto t0 = std::chrono::steady_clock::now();
    Feature feature;
    Status status = stub_->GetFeature(&context, point, &feature);
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    if (canceller.joinable()) canceller.join();

    if (!status.ok()) {
      if (verbose) {
        std::printf("GETFEATURE error code=%d message=%s\n", static_cast<int>(status.error_code()),
                    status.error_message().c_str());
        std::printf("ELAPSED_MS %lld\n", static_cast<long long>(elapsed_ms));
        PrintMetadata("INITIAL", context.GetServerInitialMetadata());
        PrintMetadata("TRAILING", context.GetServerTrailingMetadata());
      }
      return false;
    }
    if (verbose) {
      std::printf("GETFEATURE ok name=%s latitude=%d longitude=%d\n", feature.name().c_str(),
                  feature.location().latitude(), feature.location().longitude());
      std::printf("ELAPSED_MS %lld\n", static_cast<long long>(elapsed_ms));
      PrintMetadata("INITIAL", context.GetServerInitialMetadata());
      PrintMetadata("TRAILING", context.GetServerTrailingMetadata());
    }
    return true;
  }

  // Server-streaming ListFeatures via unmodified generated stubs (M5).
  bool ListFeatures(int32_t lo_lat, int32_t lo_lon, int32_t hi_lat, int32_t hi_lon, int deadline_ms,
                    int cancel_after_ms, int stop_after, bool wait_init, bool check_next_size,
                    bool drop_without_finish) {
    const Rectangle rect = MakeRect(lo_lat, lo_lon, hi_lat, hi_lon);
    ClientContext context;
    if (deadline_ms > 0) {
      context.set_deadline(std::chrono::system_clock::now() +
                           std::chrono::milliseconds(deadline_ms));
    }

    std::thread canceller;
    if (cancel_after_ms > 0) {
      canceller = std::thread([&context, cancel_after_ms] {
        std::this_thread::sleep_for(std::chrono::milliseconds(cancel_after_ms));
        context.TryCancel();
      });
    }

    const auto t0 = std::chrono::steady_clock::now();
    int messages = 0;
    bool next_size_ok = true;
    Status status;
    {
      std::unique_ptr<grpc::ClientReader<Feature>> reader(stub_->ListFeatures(&context, rect));
      if (wait_init) {
        reader->WaitForInitialMetadata();
        std::printf("INIT_METADATA_READY\n");
        PrintMetadata("INITIAL_EARLY", context.GetServerInitialMetadata());
      }

      Feature feature;
      while (true) {
        if (stop_after >= 0 && messages >= stop_after) break;
        uint32_t expected_sz = 0;
        bool has_next = false;
        if (check_next_size) {
          has_next = reader->NextMessageSize(&expected_sz);
        }
        if (!reader->Read(&feature)) {
          // Upstream gRPC v1.82.0 parity: NextMessageSize() stays non-blocking
          // and returns true with max_receive_message_size even after EOF,
          // while subsequent Read() must return false immediately (§5.8).
          uint32_t eof_sz = 0;
          if (check_next_size && (!reader->NextMessageSize(&eof_sz) || eof_sz == 0)) {
            next_size_ok = false;
          }
          if (reader->Read(&feature)) next_size_ok = false;
          break;
        }
        if (check_next_size &&
            (!has_next || expected_sz < static_cast<uint32_t>(feature.ByteSizeLong()))) {
          next_size_ok = false;
        }
        std::printf("FEATURE name=%s latitude=%d longitude=%d\n", feature.name().c_str(),
                    feature.location().latitude(), feature.location().longitude());
        ++messages;
      }

      if (!drop_without_finish) {
        status = reader->Finish();
      }
    }
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    if (canceller.joinable()) canceller.join();

    if (drop_without_finish) {
      std::printf("DROPPED messages=%d elapsed_ms=%lld\n", messages,
                  static_cast<long long>(elapsed_ms));
      return true;
    }

    std::printf("LISTFEATURES code=%d messages=%d next_size_ok=%d message=%s\n",
                static_cast<int>(status.error_code()), messages, next_size_ok ? 1 : 0,
                status.error_message().c_str());
    std::printf("ELAPSED_MS %lld\n", static_cast<long long>(elapsed_ms));
    PrintMetadata("INITIAL", context.GetServerInitialMetadata());
    PrintMetadata("TRAILING", context.GetServerTrailingMetadata());
    return status.ok() && next_size_ok;
  }

  // Pre-start TryCancel() must fail immediately with CANCELLED for both unary
  // and server-streaming RPCs.
  bool CancelPrestart() {
    ClientContext unary_ctx;
    unary_ctx.TryCancel();
    Point pt;
    pt.set_latitude(1000);
    pt.set_longitude(2000);
    Feature feat;
    Status s_unary = stub_->GetFeature(&unary_ctx, pt, &feat);

    ClientContext stream_ctx;
    stream_ctx.TryCancel();
    Rectangle rect = MakeRect(0, 0, 0, 0);
    auto reader = stub_->ListFeatures(&stream_ctx, rect);
    const bool read_ok = reader->Read(&feat);
    Status s_stream = reader->Finish();

    std::printf("PRESTART unary_code=%d stream_code=%d stream_read=%d\n",
                static_cast<int>(s_unary.error_code()), static_cast<int>(s_stream.error_code()),
                read_ok ? 1 : 0);
    return s_unary.error_code() == grpc::StatusCode::CANCELLED &&
           s_stream.error_code() == grpc::StatusCode::CANCELLED && !read_ok;
  }

  // Out-of-scope streaming stubs (client-streaming RecordRoute and bidi
  // RouteChat) must stay compile-present and return UNIMPLEMENTED at runtime.
  bool UnimplementedStreams() {
    ClientContext wctx;
    RouteSummary summary;
    auto writer = stub_->RecordRoute(&wctx, &summary);
    Point pt;
    const bool write_ok = writer->Write(pt);
    const bool wd_ok = writer->WritesDone();
    Status ws = writer->Finish();

    ClientContext rwctx;
    auto rw = stub_->RouteChat(&rwctx);
    RouteNote note;
    const bool rw_write_ok = rw->Write(note);
    const bool rw_read_ok = rw->Read(&note);
    Status rws = rw->Finish();

    std::printf("RECORDROUTE code=%d write=%d done=%d\n", static_cast<int>(ws.error_code()),
                write_ok ? 1 : 0, wd_ok ? 1 : 0);
    std::printf("ROUTECHAT code=%d write=%d read=%d\n", static_cast<int>(rws.error_code()),
                rw_write_ok ? 1 : 0, rw_read_ok ? 1 : 0);
    return ws.error_code() == grpc::StatusCode::UNIMPLEMENTED &&
           rws.error_code() == grpc::StatusCode::UNIMPLEMENTED && !write_ok && !wd_ok &&
           !rw_write_ok && !rw_read_ok;
  }

  // Queued-call deadline test: assumes server advertises MAX_CONCURRENT_STREAMS=1.
  // Thread 1 holds the single stream slot open; Thread 2 starts a call with a
  // short deadline (which queues in pending_calls_) and must fail with
  // DEADLINE_EXCEEDED while still queued (before Thread 1 completes).
  bool QueuedDeadline(int deadline_ms) {
    std::atomic<int> first_code{-1};
    std::atomic<int> second_code{-1};
    std::atomic<long long> second_ms{0};

    std::thread holder([&] {
      ClientContext ctx;
      ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(3000));
      Point pt;
      pt.set_latitude(-6);
      pt.set_longitude(1500);
      Feature f;
      Status s = stub_->GetFeature(&ctx, pt, &f);
      first_code.store(static_cast<int>(s.error_code()));
    });

    // Give the holder time to establish the connection and occupy the sole stream slot.
    std::this_thread::sleep_for(std::chrono::milliseconds(300));

    const auto t0 = std::chrono::steady_clock::now();
    ClientContext qctx;
    qctx.set_deadline(std::chrono::system_clock::now() +
                      std::chrono::milliseconds(deadline_ms > 0 ? deadline_ms : 200));
    Point qpt;
    qpt.set_latitude(1000);
    qpt.set_longitude(2000);
    Feature qf;
    Status qs = stub_->GetFeature(&qctx, qpt, &qf);
    second_ms.store(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
            .count());
    second_code.store(static_cast<int>(qs.error_code()));

    holder.join();
    std::printf("QUEUED first_code=%d second_code=%d second_ms=%lld\n", first_code.load(),
                second_code.load(), second_ms.load());
    return second_code.load() == static_cast<int>(grpc::StatusCode::DEADLINE_EXCEEDED);
  }

  // TSan fodder: hammer the channel from `threads` caller threads at once.
  bool ConcurrentGetFeature(int threads, int32_t latitude, int32_t longitude, int deadline_ms) {
    std::atomic<int> ok{0};
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(threads));
    for (int i = 0; i < threads; ++i) {
      workers.emplace_back([&] {
        if (GetFeature(latitude, longitude, deadline_ms, /*cancel_after_ms=*/0,
                       /*verbose=*/false)) {
          ok.fetch_add(1, std::memory_order_relaxed);
        }
      });
    }
    for (auto& w : workers) w.join();
    std::printf("CONCURRENT ok=%d total=%d\n", ok.load(), threads);
    return ok.load() == threads;
  }

  // TSan fodder: concurrent mix of unary calls, streaming calls, and mid-flight
  // cancellations on a single shared channel.
  bool MixedConcurrent(int threads) {
    std::atomic<int> ok{0};
    std::vector<std::thread> workers;
    workers.reserve(static_cast<size_t>(threads));
    for (int i = 0; i < threads; ++i) {
      workers.emplace_back([&, i] {
        if (i % 3 == 0) {
          // Unary OK
          if (GetFeature(1000, 2000, 10000, 0, /*verbose=*/false)) {
            ok.fetch_add(1, std::memory_order_relaxed);
          }
        } else if (i % 3 == 1) {
          // Normal 5-message stream
          ClientContext ctx;
          ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
          auto r = stub_->ListFeatures(&ctx, MakeRect(0, 0, 0, 0));
          Feature f;
          int count = 0;
          while (r->Read(&f)) ++count;
          if (r->Finish().ok() && count == 5) ok.fetch_add(1, std::memory_order_relaxed);
        } else {
          // Slow stream cancelled mid-flight from another thread
          ClientContext ctx;
          std::thread cancel_t([&ctx] {
            std::this_thread::sleep_for(std::chrono::milliseconds(60));
            ctx.TryCancel();
          });
          auto r = stub_->ListFeatures(&ctx, MakeRect(6, 1, 5000, 0));
          Feature f;
          while (r->Read(&f)) {
          }
          Status s = r->Finish();
          cancel_t.join();
          if (s.error_code() == grpc::StatusCode::CANCELLED) {
            ok.fetch_add(1, std::memory_order_relaxed);
          }
        }
      });
    }
    for (auto& w : workers) w.join();
    std::printf("MIXED ok=%d total=%d\n", ok.load(), threads);
    return ok.load() == threads;
  }

  // Starts ListFeatures, drains until EOF, calls Finish(), then immediately
  // issues a GetFeature unary call on the same channel to prove framing errors
  // reset only the bad stream and leave the channel usable.
  bool FramingErrorThenUnary(int32_t lo_lat, int32_t lo_lon, int32_t hi_lat, int32_t hi_lon) {
    ClientContext sctx;
    auto reader = stub_->ListFeatures(&sctx, MakeRect(lo_lat, lo_lon, hi_lat, hi_lon));
    Feature feat;
    while (reader->Read(&feat)) {
    }
    Status s_stream = reader->Finish();

    ClientContext uctx;
    Point pt;
    pt.set_latitude(1000);
    pt.set_longitude(2000);
    Feature ufeat;
    Status s_unary = stub_->GetFeature(&uctx, pt, &ufeat);

    const int stream_code = static_cast<int>(s_stream.error_code());
    const int unary_code = static_cast<int>(s_unary.error_code());
    std::printf("FRAMING_THEN_UNARY stream_code=%d unary_code=%d\n", stream_code, unary_code);
    return unary_code == 0;
  }

  // Starts ListFeatures, reads 1 feature, calls Finish() without draining,
  // then runs GetFeature on the same channel.
  bool EarlyFinishThenUnary(int32_t lo_lat, int32_t lo_lon, int32_t hi_lat, int32_t hi_lon) {
    ClientContext sctx;
    auto reader = stub_->ListFeatures(&sctx, MakeRect(lo_lat, lo_lon, hi_lat, hi_lon));
    Feature feat;
    (void)reader->Read(&feat);
    Status s_stream = reader->Finish();

    ClientContext uctx;
    Point pt;
    pt.set_latitude(1000);
    pt.set_longitude(2000);
    Feature ufeat;
    Status s_unary = stub_->GetFeature(&uctx, pt, &ufeat);

    const int stream_code = static_cast<int>(s_stream.error_code());
    const int unary_code = static_cast<int>(s_unary.error_code());
    std::printf("EARLY_THEN_UNARY stream_code=%d unary_code=%d\n", stream_code, unary_code);
    return unary_code == 0;
  }

  // Starts a 40-message burst (exceeding default max_queued_messages = 16),
  // sleeps 250ms so the HTTP/2 stream finishes on the wire into kDraining,
  // reads 1 feature, then calls Finish() without draining the rest.
  bool DrainingEarlyFinish(int32_t lo_lat, int32_t lo_lon, int32_t hi_lat, int32_t hi_lon) {
    const int32_t eff_lo_lat = lo_lat != 0 ? lo_lat : 5;
    const int32_t eff_hi_lat = hi_lat != 0 ? hi_lat : 40;
    const int32_t eff_hi_lon = hi_lon != 0 ? hi_lon : 16;
    ClientContext ctx;
    auto reader = stub_->ListFeatures(&ctx, MakeRect(eff_lo_lat, lo_lon, eff_hi_lat, eff_hi_lon));
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    Feature feat;
    const bool read_ok = reader->Read(&feat);
    Status status = reader->Finish();
    const int code = static_cast<int>(status.error_code());
    std::printf("DRAINING_EARLY code=%d\n", code);
    return code == 0 && read_ok;
  }

  // Verifies ClientReader::NextMessageSize() returns immediately on a stalled
  // stream before any server DATA arrives, then cancels cleanly.
  bool NextSizeNonblocking() {
    ClientContext ctx;
    auto reader = stub_->ListFeatures(&ctx, MakeRect(0, 0, 0, 0));
    uint32_t sz = 0;
    const auto t0 = std::chrono::steady_clock::now();
    const bool has_next = reader->NextMessageSize(&sz);
    const auto elapsed_ms =
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now() - t0)
            .count();
    ctx.TryCancel();
    Status status = reader->Finish();
    const bool ok = has_next && sz > 0 && elapsed_ms < 50;
    std::printf("NEXT_SIZE_OK=%d\n", ok ? 1 : 0);
    std::printf("NEXT_SIZE_MS=%lld\n", static_cast<long long>(elapsed_ms));
    std::printf("STATUS=%d\n", static_cast<int>(status.error_code()));
    return ok && status.error_code() == grpc::StatusCode::CANCELLED;
  }

 private:
  std::unique_ptr<RouteGuide::Stub> stub_;
};

// Destroys both stub and channel while a ClientReader is still alive mid-stream,
// then drains Read() until false and verifies Finish() returns UNAVAILABLE.
bool RunDropChannelMidStream(std::shared_ptr<Channel> channel, int32_t lo_lat, int32_t lo_lon,
                             int32_t hi_lat, int32_t hi_lon) {
  auto stub = RouteGuide::NewStub(channel);
  ClientContext context;
  auto reader = stub->ListFeatures(&context, MakeRect(lo_lat, lo_lon, hi_lat, hi_lon));
  Feature feature;
  if (!reader->Read(&feature)) {
    Status s = reader->Finish();
    std::printf("DROP_CHANNEL read_after_drop=0 code=%d\n", static_cast<int>(s.error_code()));
    return false;
  }

  stub.reset();
  channel.reset();

  int read_after_drop = 0;
  while (reader->Read(&feature)) {
    ++read_after_drop;
  }
  Status status = reader->Finish();
  std::printf("DROP_CHANNEL read_after_drop=%d code=%d\n", read_after_drop,
              static_cast<int>(status.error_code()));
  return status.error_code() == grpc::StatusCode::UNAVAILABLE;
}

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
  int32_t lo_lat = 0;
  int32_t lo_lon = 0;
  int32_t hi_lat = 0;
  int32_t hi_lon = 0;
  int deadline_ms = 0;
  int cancel_after_ms = 0;
  int stop_after = -1;
  int threads = 8;
  int max_recv_bytes = 0;
  bool wait_init = false;
  bool check_next_size = false;

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
    } else if (std::strcmp(argv[i], "--lo-lat") == 0) {
      lo_lat = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--lo-lon") == 0) {
      lo_lon = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--hi-lat") == 0) {
      hi_lat = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--hi-lon") == 0) {
      hi_lon = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--deadline-ms") == 0) {
      deadline_ms = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--cancel-after-ms") == 0) {
      cancel_after_ms = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--stop-after") == 0) {
      stop_after = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--threads") == 0) {
      threads = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--max-recv-bytes") == 0) {
      max_recv_bytes = std::atoi(argv[i + 1]);
    } else if (std::strcmp(argv[i], "--wait-init") == 0) {
      wait_init = std::atoi(argv[i + 1]) != 0;
    } else if (std::strcmp(argv[i], "--check-next-size") == 0) {
      check_next_size = std::atoi(argv[i + 1]) != 0;
    } else {
      std::fprintf(stderr, "unknown flag: %s\n", argv[i]);
      return 2;
    }
  }
  if (target.empty() || ca_file.empty()) {
    std::fprintf(stderr, "usage: %s --target host:port --ca-file cert.pem [--mode ...]\n", argv[0]);
    return 2;
  }

  grpc::SslCredentialsOptions ssl;
  if (!ReadFile(ca_file.c_str(), &ssl.pem_root_certs)) {
    std::fprintf(stderr, "cannot read CA file: %s\n", ca_file.c_str());
    return 2;
  }

  std::shared_ptr<Channel> channel;
  if (max_recv_bytes > 0) {
    grpc::ChannelArguments args;
    args.SetMaxReceiveMessageSize(max_recv_bytes);
    channel = grpc::CreateCustomChannel(target, grpc::SslCredentials(ssl), args);
  } else {
    channel = grpc::CreateChannel(target, grpc::SslCredentials(ssl));
  }
  if (mode == "drop_channel_mid_stream") {
    return RunDropChannelMidStream(std::move(channel), lo_lat, lo_lon, hi_lat, hi_lon) ? 0 : 1;
  }
  RouteGuideClient client(std::move(channel));
  if (mode == "getfeature") {
    return client.GetFeature(latitude, longitude, deadline_ms, cancel_after_ms, /*verbose=*/true)
               ? 0
               : 1;
  }
  if (mode == "listfeatures") {
    return client.ListFeatures(lo_lat, lo_lon, hi_lat, hi_lon, deadline_ms, cancel_after_ms,
                               stop_after, wait_init, check_next_size,
                               /*drop_without_finish=*/false)
               ? 0
               : 1;
  }
  if (mode == "early_finish") {
    return client.ListFeatures(lo_lat, lo_lon, hi_lat, hi_lon, deadline_ms, cancel_after_ms,
                               stop_after >= 0 ? stop_after : 2, wait_init, check_next_size,
                               /*drop_without_finish=*/false)
               ? 0
               : 1;
  }
  if (mode == "drop_reader") {
    return client.ListFeatures(lo_lat, lo_lon, hi_lat, hi_lon, deadline_ms, cancel_after_ms,
                               stop_after >= 0 ? stop_after : 2, wait_init, check_next_size,
                               /*drop_without_finish=*/true)
               ? 0
               : 1;
  }
  if (mode == "cancel_prestart") {
    return client.CancelPrestart() ? 0 : 1;
  }
  if (mode == "unimplemented_streams") {
    return client.UnimplementedStreams() ? 0 : 1;
  }
  if (mode == "queued_deadline") {
    return client.QueuedDeadline(deadline_ms) ? 0 : 1;
  }
  if (mode == "concurrent") {
    return client.ConcurrentGetFeature(threads, latitude, longitude, deadline_ms) ? 0 : 1;
  }
  if (mode == "mixed_concurrent") {
    return client.MixedConcurrent(threads) ? 0 : 1;
  }
  if (mode == "framing_error_then_unary") {
    return client.FramingErrorThenUnary(lo_lat, lo_lon, hi_lat, hi_lon) ? 0 : 1;
  }
  if (mode == "early_finish_then_unary") {
    return client.EarlyFinishThenUnary(lo_lat, lo_lon, hi_lat, hi_lon) ? 0 : 1;
  }
  if (mode == "draining_early_finish") {
    return client.DrainingEarlyFinish(lo_lat, lo_lon, hi_lat, hi_lon) ? 0 : 1;
  }
  if (mode == "next_size_nonblocking") {
    return client.NextSizeNonblocking() ? 0 : 1;
  }
  std::fprintf(stderr, "unknown mode: %s\n", mode.c_str());
  return 2;
}
