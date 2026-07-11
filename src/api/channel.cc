// egrpc grpcpp shim — grpc::Channel over egrpc::internal::ChannelImpl, and
// the byte-level BlockingUnaryCallRaw entry point (design §4.5). Everything
// here runs on the caller's thread: metadata is read out of the
// ClientContext before submission and the results written back after
// CallState::Wait returned (design §3).

#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/impl/client_unary_call.h>
#include <grpcpp/impl/rpc_method.h>

#include <chrono>
#include <optional>
#include <utility>

#include "core/channel_impl.h"
#include "core/grpc_wire.h"

namespace grpc {
namespace {

bool IsBinKey(const std::string& key) {
  return key.size() >= 4 && key.compare(key.size() - 4, 4, "-bin") == 0;
}

// Received metadata → context storage + string_ref views. `-bin` values
// arrive base64-encoded (§5.1) and are exposed decoded, like upstream; a
// value that fails to decode is kept raw rather than silently dropped.
// multimap nodes are stable, so the views point into the storage safely.
void DepositMetadata(const egrpc::internal::Http2Session::HeaderList& in,
                     std::multimap<std::string, std::string>* storage,
                     std::multimap<string_ref, string_ref>* view) {
  storage->clear();
  view->clear();
  for (const auto& kv : in) {
    std::string value = kv.second;
    if (IsBinKey(kv.first)) {
      std::string decoded;
      if (egrpc::internal::Base64Decode(kv.second, &decoded)) value = std::move(decoded);
    }
    auto it = storage->emplace(kv.first, std::move(value));
    view->emplace(string_ref(it->first), string_ref(it->second));
  }
}

}  // namespace

Channel::Channel(std::unique_ptr<egrpc::internal::ChannelImpl> impl, Status lame_status)
    : impl_(std::move(impl)), lame_status_(std::move(lame_status)) {}

Channel::~Channel() = default;

std::shared_ptr<Channel> Channel::FromImpl(std::unique_ptr<egrpc::internal::ChannelImpl> impl) {
  return std::shared_ptr<Channel>(new Channel(std::move(impl), Status()));
}

std::shared_ptr<Channel> Channel::Lame(Status lame_status) {
  return std::shared_ptr<Channel>(new Channel(nullptr, std::move(lame_status)));
}

Status Channel::BlockingUnaryCallShim(const internal::RpcMethod& method, ClientContext* context,
                                      const std::string& request, std::string* response) {
  if (!impl_) return lame_status_;

  // Deadline: already-expired deadlines fail before anything is queued,
  // matching upstream's fail-fast. Otherwise the context's wall-clock
  // deadline converts to an absolute steady-clock deadline here; the
  // grpc-timeout header (§5.1) is computed from it on the EventThread when
  // the HEADERS are built, so time spent queued (e.g. during connect) is
  // not re-promised to the server.
  std::optional<std::chrono::steady_clock::time_point> deadline;
  if (context->deadline_ != std::chrono::system_clock::time_point::max()) {
    const auto now = std::chrono::system_clock::now();
    if (context->deadline_ <= now) {
      return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline Exceeded");
    }
    deadline = std::chrono::steady_clock::now() +
               std::chrono::duration_cast<std::chrono::nanoseconds>(context->deadline_ - now);
  }

  egrpc::internal::Http2Session::HeaderList metadata;
  metadata.reserve(context->send_initial_metadata_.size());
  for (const auto& kv : context->send_initial_metadata_) {
    metadata.emplace_back(kv.first, kv.second);
  }

  egrpc::internal::CallState::Result result =
      impl_->UnaryCall(method.name(), request, std::move(metadata), deadline);

  DepositMetadata(result.initial_metadata, &context->recv_initial_metadata_,
                  &context->recv_initial_view_);
  DepositMetadata(result.trailing_metadata, &context->recv_trailing_metadata_,
                  &context->recv_trailing_view_);

  if (result.code == egrpc::internal::StatusCode::kOk) {
    *response = std::move(result.response);
    return Status();
  }

  // grpc-status-details-bin (already base64-decoded above) rides along as
  // Status::error_details, like upstream.
  std::string details;
  auto it = context->recv_trailing_metadata_.find("grpc-status-details-bin");
  if (it != context->recv_trailing_metadata_.end()) details = it->second;

  return Status(static_cast<StatusCode>(result.code), result.message, details);
}

namespace internal {

Status BlockingUnaryCallRaw(ChannelInterface* channel, const RpcMethod& method,
                            ClientContext* context, const std::string& request,
                            std::string* response) {
  auto* egrpc_channel = dynamic_cast<Channel*>(channel);
  if (egrpc_channel == nullptr) {
    return Status(StatusCode::INTERNAL, "channel was not created by egrpc's grpc::CreateChannel");
  }
  return egrpc_channel->BlockingUnaryCallShim(method, context, request, response);
}

}  // namespace internal
}  // namespace grpc
