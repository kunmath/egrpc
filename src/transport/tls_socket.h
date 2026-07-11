// egrpc — TLS client socket: non-blocking connect + OpenSSL handshake,
// ALPN h2-only, certificate verification on by default (design §4.1).
//
// Owned exclusively by the EventThread after StartConnect(); all methods are
// single-threaded by contract (design §3). The socket never blocks: the owner
// polls fd() for DesiredPollEvents() and calls ContinueConnect() until the
// state leaves the connecting phases, then uses Read()/Write(). Connect
// timeout is the owner's job (a TimerHeap entry that calls Close()).
#pragma once

#include <netinet/in.h>
#include <sys/socket.h>

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

// Forward-declare OpenSSL types so this header does not leak <openssl/*.h>.
typedef struct ssl_ctx_st SSL_CTX;
typedef struct ssl_st SSL;

namespace egrpc {
namespace internal {

struct TlsConfig {
  // Root CA bundle: file path, or inline PEM (wins over the path when both
  // are set). When neither is set, the system default trust store is used.
  std::string ca_bundle_path;
  std::string ca_bundle_pem;

  // Optional mTLS client identity (PEM file paths; both or neither).
  std::string client_cert_path;
  std::string client_key_path;

  // TEST RIGS ONLY: skips certificate chain and hostname verification.
  // Never set this in production code paths.
  bool insecure_skip_verify_for_testing_only = false;
};

class TlsSocket {
 public:
  enum class State {
    kIdle,           // constructed, StartConnect not yet called
    kTcpConnecting,  // non-blocking connect() in flight
    kTlsHandshake,   // TCP up, SSL_connect in progress
    kConnected,      // handshake done, ALPN h2 verified
    kFailed,         // terminal; error() has detail
    kClosed,         // Close() called
  };

  enum class IoStatus {
    kOk,
    kWantRead,   // re-arm poll for POLLIN and retry
    kWantWrite,  // re-arm poll for POLLOUT and retry
    kEof,        // clean TLS shutdown by peer
    kError,      // terminal; error() has detail
  };

  TlsSocket() = default;
  ~TlsSocket();

  TlsSocket(const TlsSocket&) = delete;
  TlsSocket& operator=(const TlsSocket&) = delete;

  // Resolves host (blocking getaddrinfo — acceptable only while transport
  // failure is channel-terminal, i.e. through M5; design §4.1 moves
  // resolution to a helper thread with generation-tagged results before M6's
  // reconnect, after which Connect takes pre-resolved addresses) and starts
  // a non-blocking connect to the first address; remaining addresses are
  // fallbacks tried on connect failure. Returns false and sets state kFailed
  // on immediate failure (resolution, socket creation, or CA/cert config
  // errors).
  bool StartConnect(const std::string& host, uint16_t port, const TlsConfig& config);

  // Drives TCP connect + TLS handshake. Call after StartConnect() and then
  // whenever poll() reports readiness on fd(), until state() is kConnected
  // or kFailed. kOk means kConnected.
  IoStatus ContinueConnect();

  // POLLIN/POLLOUT the owner should poll for in the current state.
  short DesiredPollEvents() const;

  // Valid only in kConnected. kOk sets *n > 0.
  IoStatus Read(void* buf, size_t len, size_t* n);
  IoStatus Write(const void* buf, size_t len, size_t* n);

  // Best-effort close: one non-blocking SSL_shutdown attempt, then close(2).
  // Idempotent. (Design §5.7: bounded, never hangs.)
  void Close();

  int fd() const { return fd_; }
  State state() const { return state_; }
  // Human-readable failure detail (valid in kFailed).
  const std::string& error() const { return error_; }
  // Negotiated ALPN protocol (valid in kConnected; always "h2").
  const std::string& alpn() const { return alpn_; }

 private:
  bool CreateSslContext(const TlsConfig& config);
  // Opens a non-blocking socket to addresses_[next_address_] and starts
  // connect(); advances next_address_. Returns false when out of addresses.
  bool TryNextAddress();
  IoStatus CheckTcpConnected();  // SO_ERROR check after POLLOUT
  IoStatus ContinueTlsHandshake();
  IoStatus Fail(const std::string& message);  // → kFailed, closes fd
  // Maps an SSL_get_error result to an IoStatus; fills error_ on kError.
  IoStatus MapSslError(int ssl_error, int syscall_errno, const char* op);

  State state_ = State::kIdle;
  int fd_ = -1;
  SSL_CTX* ssl_ctx_ = nullptr;
  SSL* ssl_ = nullptr;
  std::string host_;  // for SNI + hostname verification
  bool verify_peer_ = true;
  std::string error_;
  std::string alpn_;
  short desired_events_ = 0;

  std::vector<sockaddr_storage> addresses_;
  std::vector<socklen_t> address_lens_;
  size_t next_address_ = 0;
};

}  // namespace internal
}  // namespace egrpc
