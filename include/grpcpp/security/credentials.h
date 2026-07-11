// egrpc grpcpp shim — channel credentials (design §4.5). v0.1 is TLS-only:
// SslCredentials is the real thing and maps onto TlsConfig in
// CreateChannel; InsecureChannelCredentials is compile-present but yields a
// channel whose calls fail with UNIMPLEMENTED.
#pragma once

#include <memory>
#include <string>

namespace grpc {

struct SslCredentialsOptions {
  // PEM root CA bundle the server certificate is verified against. Empty ⇒
  // the system default trust store.
  std::string pem_root_certs;
  // Optional mTLS client identity (both or neither). NOTE: in-memory client
  // identity is not plumbed into the transport yet — setting these yields
  // an UNIMPLEMENTED-failing channel in v0.1.
  std::string pem_private_key;
  std::string pem_cert_chain;
};

// Opaque credential handle consumed by CreateChannel. Not user-derivable;
// only the factories below create it.
class ChannelCredentials {
 public:
  // --- egrpc shim internal --------------------------------------------------
  bool egrpc_secure() const { return secure_; }
  const SslCredentialsOptions& egrpc_ssl_options() const { return ssl_options_; }

 private:
  friend std::shared_ptr<ChannelCredentials> SslCredentials(const SslCredentialsOptions&);
  friend std::shared_ptr<ChannelCredentials> InsecureChannelCredentials();

  ChannelCredentials(bool secure, SslCredentialsOptions ssl_options)
      : secure_(secure), ssl_options_(std::move(ssl_options)) {}

  const bool secure_;
  const SslCredentialsOptions ssl_options_;
};

inline std::shared_ptr<ChannelCredentials> SslCredentials(const SslCredentialsOptions& options) {
  return std::shared_ptr<ChannelCredentials>(new ChannelCredentials(true, options));
}

inline std::shared_ptr<ChannelCredentials> InsecureChannelCredentials() {
  return std::shared_ptr<ChannelCredentials>(new ChannelCredentials(false, {}));
}

}  // namespace grpc
