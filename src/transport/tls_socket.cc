// egrpc — TLS client socket (design §4.1).

#include "transport/tls_socket.h"

#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/tcp.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509v3.h>
#include <poll.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>

namespace egrpc {
namespace internal {

namespace {

// ALPN protocol list in wire format: length-prefixed "h2", nothing else
// (design §4.1: no h2c fallback, no http/1.1 tolerance).
const unsigned char kAlpnH2[] = {2, 'h', '2'};

std::string SslErrorQueueString() {
  std::string out;
  unsigned long code;  // NOLINT(runtime/int) — OpenSSL API type
  while ((code = ERR_get_error()) != 0) {
    char buf[256];
    ERR_error_string_n(code, buf, sizeof(buf));
    if (!out.empty()) out += "; ";
    out += buf;
  }
  return out.empty() ? "unknown OpenSSL error" : out;
}

bool IsIpLiteral(const std::string& host) {
  in_addr v4;
  in6_addr v6;
  return inet_pton(AF_INET, host.c_str(), &v4) == 1 || inet_pton(AF_INET6, host.c_str(), &v6) == 1;
}

}  // namespace

TlsSocket::~TlsSocket() { Close(); }

bool TlsSocket::CreateSslContext(const TlsConfig& config) {
  ssl_ctx_ = SSL_CTX_new(TLS_client_method());
  if (ssl_ctx_ == nullptr) {
    error_ = "SSL_CTX_new failed: " + SslErrorQueueString();
    return false;
  }
  if (SSL_CTX_set_min_proto_version(ssl_ctx_, TLS1_2_VERSION) != 1) {
    error_ = "failed to set TLS 1.2 minimum: " + SslErrorQueueString();
    return false;
  }
  SSL_CTX_set_mode(ssl_ctx_, SSL_MODE_ENABLE_PARTIAL_WRITE | SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);
  if (SSL_CTX_set_alpn_protos(ssl_ctx_, kAlpnH2, sizeof(kAlpnH2)) != 0) {
    error_ = "failed to set ALPN protocols: " + SslErrorQueueString();
    return false;
  }

  verify_peer_ = !config.insecure_skip_verify_for_testing_only;
  if (verify_peer_) {
    SSL_CTX_set_verify(ssl_ctx_, SSL_VERIFY_PEER, nullptr);
    if (!config.ca_bundle_pem.empty()) {
      BIO* bio = BIO_new_mem_buf(config.ca_bundle_pem.data(),
                                 static_cast<int>(config.ca_bundle_pem.size()));
      if (bio == nullptr) {
        error_ = "failed to allocate BIO for CA bundle";
        return false;
      }
      X509_STORE* store = SSL_CTX_get_cert_store(ssl_ctx_);
      int added = 0;
      for (;;) {
        X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
        if (cert == nullptr) break;
        const int ok = X509_STORE_add_cert(store, cert);
        X509_free(cert);
        if (ok == 1) ++added;
      }
      ERR_clear_error();  // expected PEM_R_NO_START_LINE at end of bundle
      BIO_free(bio);
      if (added == 0) {
        error_ = "CA bundle PEM contained no certificates";
        return false;
      }
    } else if (!config.ca_bundle_path.empty()) {
      if (SSL_CTX_load_verify_locations(ssl_ctx_, config.ca_bundle_path.c_str(), nullptr) != 1) {
        error_ =
            "failed to load CA bundle '" + config.ca_bundle_path + "': " + SslErrorQueueString();
        return false;
      }
    } else if (SSL_CTX_set_default_verify_paths(ssl_ctx_) != 1) {
      error_ = "failed to load system default CA paths: " + SslErrorQueueString();
      return false;
    }
  } else {
    SSL_CTX_set_verify(ssl_ctx_, SSL_VERIFY_NONE, nullptr);
  }

  if (!config.client_cert_path.empty() || !config.client_key_path.empty()) {
    if (config.client_cert_path.empty() || config.client_key_path.empty()) {
      error_ = "mTLS config requires both client_cert_path and client_key_path";
      return false;
    }
    if (SSL_CTX_use_certificate_chain_file(ssl_ctx_, config.client_cert_path.c_str()) != 1) {
      error_ = "failed to load client certificate '" + config.client_cert_path +
               "': " + SslErrorQueueString();
      return false;
    }
    if (SSL_CTX_use_PrivateKey_file(ssl_ctx_, config.client_key_path.c_str(), SSL_FILETYPE_PEM) !=
        1) {
      error_ =
          "failed to load client key '" + config.client_key_path + "': " + SslErrorQueueString();
      return false;
    }
    if (SSL_CTX_check_private_key(ssl_ctx_) != 1) {
      error_ = "client certificate and key do not match: " + SslErrorQueueString();
      return false;
    }
  }
  return true;
}

bool TlsSocket::StartConnect(const std::string& host, uint16_t port, const TlsConfig& config) {
  if (state_ != State::kIdle) {
    Fail("StartConnect called twice on the same TlsSocket");
    return false;
  }
  host_ = host;

  if (!CreateSslContext(config)) {
    Fail(error_);
    return false;
  }

  addrinfo hints{};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  addrinfo* result = nullptr;
  const std::string port_str = std::to_string(port);
  const int rc = getaddrinfo(host.c_str(), port_str.c_str(), &hints, &result);
  if (rc != 0) {
    Fail("failed to resolve '" + host + "': " + gai_strerror(rc));
    return false;
  }
  for (addrinfo* ai = result; ai != nullptr; ai = ai->ai_next) {
    if (ai->ai_addrlen > sizeof(sockaddr_storage)) continue;
    sockaddr_storage ss{};
    std::memcpy(&ss, ai->ai_addr, ai->ai_addrlen);
    addresses_.push_back(ss);
    address_lens_.push_back(ai->ai_addrlen);
  }
  freeaddrinfo(result);
  if (addresses_.empty()) {
    Fail("resolution of '" + host + "' returned no usable addresses");
    return false;
  }

  if (!TryNextAddress()) {
    Fail("could not connect to '" + host + ":" + port_str + "': " + error_);
    return false;
  }
  return true;
}

bool TlsSocket::TryNextAddress() {
  while (next_address_ < addresses_.size()) {
    const size_t i = next_address_++;
    const auto* addr = reinterpret_cast<const sockaddr*>(&addresses_[i]);
    fd_ = socket(addr->sa_family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
    if (fd_ < 0) {
      error_ = std::string("socket() failed: ") + std::strerror(errno);
      continue;
    }
    const int one = 1;
    setsockopt(fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    if (connect(fd_, addr, address_lens_[i]) == 0) {
      state_ = State::kTlsHandshake;
      desired_events_ = POLLOUT;
      error_.clear();  // drop failure text from earlier fallback addresses
      return true;
    }
    if (errno == EINPROGRESS) {
      state_ = State::kTcpConnecting;
      desired_events_ = POLLOUT;
      error_.clear();
      return true;
    }
    error_ = std::string("connect() failed: ") + std::strerror(errno);
    close(fd_);
    fd_ = -1;
  }
  if (error_.empty()) error_ = "no addresses to try";
  return false;
}

TlsSocket::IoStatus TlsSocket::CheckTcpConnected() {
  int so_error = 0;
  socklen_t len = sizeof(so_error);
  if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &so_error, &len) != 0) so_error = errno;
  if (so_error != 0) {
    error_ = std::string("connect() failed: ") + std::strerror(so_error);
    close(fd_);
    fd_ = -1;
    if (TryNextAddress()) {
      // Moved on to the next resolved address; still connecting.
      return state_ == State::kTlsHandshake ? ContinueTlsHandshake() : IoStatus::kWantWrite;
    }
    return Fail("could not connect to '" + host_ + "': " + error_);
  }
  state_ = State::kTlsHandshake;
  return ContinueTlsHandshake();
}

TlsSocket::IoStatus TlsSocket::ContinueTlsHandshake() {
  if (ssl_ == nullptr) {
    ssl_ = SSL_new(ssl_ctx_);
    if (ssl_ == nullptr) return Fail("SSL_new failed: " + SslErrorQueueString());
    if (SSL_set_fd(ssl_, fd_) != 1) return Fail("SSL_set_fd failed: " + SslErrorQueueString());
    SSL_set_connect_state(ssl_);
    if (IsIpLiteral(host_)) {
      // No SNI for IP literals (RFC 6066); verify against the certificate's
      // IP subjectAltName instead.
      if (verify_peer_) {
        X509_VERIFY_PARAM* param = SSL_get0_param(ssl_);
        if (X509_VERIFY_PARAM_set1_ip_asc(param, host_.c_str()) != 1) {
          return Fail("failed to set IP for certificate verification: " + SslErrorQueueString());
        }
      }
    } else {
      if (SSL_set_tlsext_host_name(ssl_, host_.c_str()) != 1) {
        return Fail("failed to set SNI host name: " + SslErrorQueueString());
      }
      if (verify_peer_ && SSL_set1_host(ssl_, host_.c_str()) != 1) {
        return Fail("failed to set verification host name: " + SslErrorQueueString());
      }
    }
  }

  ERR_clear_error();
  const int rc = SSL_connect(ssl_);
  if (rc == 1) {
    const unsigned char* proto = nullptr;
    unsigned int proto_len = 0;
    SSL_get0_alpn_selected(ssl_, &proto, &proto_len);
    if (proto_len != 2 || std::memcmp(proto, "h2", 2) != 0) {
      return Fail("server did not negotiate HTTP/2 via ALPN");
    }
    alpn_.assign(reinterpret_cast<const char*>(proto), proto_len);
    state_ = State::kConnected;
    desired_events_ = POLLIN;
    return IoStatus::kOk;
  }
  const int saved_errno = errno;
  const int ssl_error = SSL_get_error(ssl_, rc);
  if (ssl_error == SSL_ERROR_WANT_READ) {
    desired_events_ = POLLIN;
    return IoStatus::kWantRead;
  }
  if (ssl_error == SSL_ERROR_WANT_WRITE) {
    desired_events_ = POLLOUT;
    return IoStatus::kWantWrite;
  }
  // Terminal handshake failure — surface ALPN and certificate problems
  // specifically. A strict server rejects our h2-only offer with a fatal
  // no_application_protocol alert before the handshake completes.
  if (ERR_GET_REASON(ERR_peek_error()) == SSL_AD_REASON_OFFSET + TLS1_AD_NO_APPLICATION_PROTOCOL) {
    return Fail("server did not negotiate HTTP/2 via ALPN");
  }
  const long verify = SSL_get_verify_result(ssl_);  // NOLINT(runtime/int)
  std::string detail = SslErrorQueueString();
  if (verify != X509_V_OK) {
    detail = std::string("certificate verification failed: ") +
             X509_verify_cert_error_string(verify) + " (" + detail + ")";
  } else if (ssl_error == SSL_ERROR_SYSCALL) {
    detail = saved_errno != 0
                 ? std::string("TLS handshake I/O error: ") + std::strerror(saved_errno)
                 : "connection closed during TLS handshake";
  }
  return Fail("TLS handshake with '" + host_ + "' failed: " + detail);
}

TlsSocket::IoStatus TlsSocket::ContinueConnect() {
  switch (state_) {
    case State::kTcpConnecting:
      return CheckTcpConnected();
    case State::kTlsHandshake:
      return ContinueTlsHandshake();
    case State::kConnected:
      return IoStatus::kOk;
    default:
      error_ = error_.empty() ? "ContinueConnect on unconnected socket" : error_;
      return IoStatus::kError;
  }
}

short TlsSocket::DesiredPollEvents() const { return desired_events_; }

TlsSocket::IoStatus TlsSocket::MapSslError(int ssl_error, int syscall_errno, const char* op) {
  switch (ssl_error) {
    case SSL_ERROR_WANT_READ:
      return IoStatus::kWantRead;
    case SSL_ERROR_WANT_WRITE:
      return IoStatus::kWantWrite;
    case SSL_ERROR_ZERO_RETURN:
      return IoStatus::kEof;
    case SSL_ERROR_SYSCALL:
      error_ = syscall_errno != 0 ? std::string(op) + " failed: " + std::strerror(syscall_errno)
                                  : std::string(op) + " failed: connection closed abruptly by peer";
      return IoStatus::kError;
    default:
      error_ = std::string(op) + " failed: " + SslErrorQueueString();
      return IoStatus::kError;
  }
}

TlsSocket::IoStatus TlsSocket::Read(void* buf, size_t len, size_t* n) {
  *n = 0;
  if (state_ != State::kConnected) {
    error_ = "Read on socket that is not connected";
    return IoStatus::kError;
  }
  ERR_clear_error();
  size_t nread = 0;
  const int rc = SSL_read_ex(ssl_, buf, len, &nread);
  if (rc == 1) {
    *n = nread;
    return IoStatus::kOk;
  }
  const int saved_errno = errno;  // before SSL_get_error, which may clobber it
  return MapSslError(SSL_get_error(ssl_, rc), saved_errno, "SSL_read");
}

TlsSocket::IoStatus TlsSocket::Write(const void* buf, size_t len, size_t* n) {
  *n = 0;
  if (state_ != State::kConnected) {
    error_ = "Write on socket that is not connected";
    return IoStatus::kError;
  }
  ERR_clear_error();
  size_t nwritten = 0;
  const int rc = SSL_write_ex(ssl_, buf, len, &nwritten);
  if (rc == 1) {
    *n = nwritten;
    return IoStatus::kOk;
  }
  const int saved_errno = errno;  // before SSL_get_error, which may clobber it
  return MapSslError(SSL_get_error(ssl_, rc), saved_errno, "SSL_write");
}

TlsSocket::IoStatus TlsSocket::Fail(const std::string& message) {
  error_ = message;
  state_ = State::kFailed;
  desired_events_ = 0;
  if (ssl_ != nullptr) {
    SSL_free(ssl_);
    ssl_ = nullptr;
  }
  if (ssl_ctx_ != nullptr) {
    SSL_CTX_free(ssl_ctx_);
    ssl_ctx_ = nullptr;
  }
  if (fd_ >= 0) {
    close(fd_);
    fd_ = -1;
  }
  return IoStatus::kError;
}

void TlsSocket::Close() {
  if (ssl_ != nullptr) {
    if (state_ == State::kConnected) SSL_shutdown(ssl_);  // best-effort, one try
    SSL_free(ssl_);
    ssl_ = nullptr;
  }
  if (ssl_ctx_ != nullptr) {
    SSL_CTX_free(ssl_ctx_);
    ssl_ctx_ = nullptr;
  }
  if (fd_ >= 0) {
    close(fd_);
    fd_ = -1;
  }
  if (state_ != State::kFailed) state_ = State::kClosed;
  desired_events_ = 0;
}

}  // namespace internal
}  // namespace egrpc
