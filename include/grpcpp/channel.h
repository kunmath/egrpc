// egrpc grpcpp shim — grpc::Channel (design §4.5): the opaque handle over
// egrpc::internal::ChannelImpl. Created via grpc::CreateChannel; the
// generated stubs only ever see the ChannelInterface base.
#pragma once

#include <grpcpp/impl/channel_interface.h>
#include <grpcpp/support/status.h>

#include <memory>
#include <string>

namespace egrpc {
namespace internal {
class ChannelImpl;
}  // namespace internal
}  // namespace egrpc

namespace grpc {

class ClientContext;
namespace internal {
class RpcMethod;
}  // namespace internal

class Channel final : public ChannelInterface {
 public:
  ~Channel() override;

  // --- egrpc shim internal, not upstream API -------------------------------
  // The funnel BlockingUnaryCall ends in: sends the context's metadata and
  // the serialized request, blocks until the call closes, deposits response
  // metadata back into *context (design §3: the caller thread blocks in
  // CallState::Wait; the EventThread owns everything else).
  Status BlockingUnaryCallShim(const internal::RpcMethod& method, ClientContext* context,
                               const std::string& request, std::string* response);

  // Internal factories used by CreateChannel (src/api/create_channel.cc).
  // A "lame" channel has no transport and fails every call with
  // `lame_status` — how InsecureChannelCredentials and other unsupported
  // configurations surface UNIMPLEMENTED in v0.1 (design §4.5).
  static std::shared_ptr<Channel> FromImpl(std::unique_ptr<egrpc::internal::ChannelImpl> impl);
  static std::shared_ptr<Channel> Lame(Status lame_status);

 private:
  Channel(std::unique_ptr<egrpc::internal::ChannelImpl> impl, Status lame_status);

  const std::unique_ptr<egrpc::internal::ChannelImpl> impl_;  // null ⇒ lame
  const Status lame_status_;
};

}  // namespace grpc
