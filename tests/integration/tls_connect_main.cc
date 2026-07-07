// egrpc — M1 acceptance probe (design §8, M1).
//
// Connects to host:port over TLS using EventThread + TlsSocket exactly the
// way ChannelImpl will: the caller thread posts a connect op, the EventThread
// drives the non-blocking connect + handshake off poll(), and a timer bounds
// the attempt. Prints one line and exits:
//   CONNECTED alpn=h2          → exit 0
//   FAILED: <detail>           → exit 1
//   USAGE error                → exit 2
//
// Driven by tests/test_tls.py against Python TLS servers (ALPN h2,
// http/1.1-only, untrusted cert).

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <future>
#include <memory>
#include <string>

#include "core/event_thread.h"
#include "transport/tls_socket.h"

namespace {

struct ProbeArgs {
  std::string host;
  uint16_t port = 0;
  egrpc::internal::TlsConfig tls;
  int timeout_ms = 10000;
};

int Usage(const char* argv0) {
  std::fprintf(stderr,
               "usage: %s --host H --port P [--ca FILE] [--client-cert FILE --client-key FILE]\n"
               "          [--insecure] [--timeout-ms N]\n",
               argv0);
  return 2;
}

}  // namespace

int main(int argc, char** argv) {
  ProbeArgs args;
  for (int i = 1; i < argc; ++i) {
    const std::string flag = argv[i];
    auto next = [&]() -> const char* { return (i + 1 < argc) ? argv[++i] : nullptr; };
    if (flag == "--host") {
      const char* v = next();
      if (!v) return Usage(argv[0]);
      args.host = v;
    } else if (flag == "--port") {
      const char* v = next();
      if (!v) return Usage(argv[0]);
      args.port = static_cast<uint16_t>(std::atoi(v));
    } else if (flag == "--ca") {
      const char* v = next();
      if (!v) return Usage(argv[0]);
      args.tls.ca_bundle_path = v;
    } else if (flag == "--client-cert") {
      const char* v = next();
      if (!v) return Usage(argv[0]);
      args.tls.client_cert_path = v;
    } else if (flag == "--client-key") {
      const char* v = next();
      if (!v) return Usage(argv[0]);
      args.tls.client_key_path = v;
    } else if (flag == "--insecure") {
      args.tls.insecure_skip_verify_for_testing_only = true;
    } else if (flag == "--timeout-ms") {
      const char* v = next();
      if (!v) return Usage(argv[0]);
      args.timeout_ms = std::atoi(v);
    } else {
      return Usage(argv[0]);
    }
  }
  if (args.host.empty() || args.port == 0) return Usage(argv[0]);

  using egrpc::internal::EventThread;
  using egrpc::internal::TlsSocket;

  EventThread loop;
  if (!loop.Start()) {
    std::fprintf(stderr, "FAILED: could not start event thread\n");
    return 1;
  }

  // Owned by the EventThread after the connect op runs (design §3); the main
  // thread only waits on the future, mirroring condvar completion.
  auto socket = std::make_unique<TlsSocket>();
  std::promise<std::string> done;  // empty string = success
  auto result = done.get_future();

  const bool posted = loop.Post([&loop, &args, &done, sock = socket.get()]() {
    auto finish = [&loop, &done, sock](const std::string& error) {
      loop.UnwatchFd();
      sock->Close();
      done.set_value(error);
    };
    const egrpc::internal::TimerId deadline = loop.AddTimer(
        std::chrono::milliseconds(args.timeout_ms), [finish] { finish("connect timeout"); });

    if (!sock->StartConnect(args.host, args.port, args.tls)) {
      loop.CancelTimer(deadline);
      finish(sock->error());
      return;
    }

    loop.WatchFd(
        sock->fd(), [sock] { return sock->DesiredPollEvents(); },
        [&loop, sock, finish, deadline](short) {
          const TlsSocket::IoStatus st = sock->ContinueConnect();
          if (st == TlsSocket::IoStatus::kWantRead || st == TlsSocket::IoStatus::kWantWrite) {
            return;  // re-armed via DesiredPollEvents on the next poll
          }
          loop.CancelTimer(deadline);
          if (st == TlsSocket::IoStatus::kOk) {
            finish("");
          } else {
            finish(sock->error());
          }
        });
  });
  if (!posted) {
    std::fprintf(stderr, "FAILED: event thread refused op\n");
    return 1;
  }

  const std::string error = result.get();
  loop.Stop();

  if (error.empty()) {
    std::printf("CONNECTED alpn=h2\n");
    return 0;
  }
  std::fprintf(stderr, "FAILED: %s\n", error.c_str());
  return 1;
}
