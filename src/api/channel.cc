// egrpc grpcpp shim — grpc::Channel over egrpc::internal::ChannelImpl,
// byte-level BlockingUnaryCallRaw, and server-streaming ClientStreamRaw
// (design §4.5, §5.7, §5.8). Everything here runs on caller threads:
// metadata is read out of ClientContext before submission and results are
// written back on the caller's thread (design §3). TryCancel() is safe from
// any thread via weak_ptrs into CallCompletion and ChannelImpl.

#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/impl/client_unary_call.h>
#include <grpcpp/impl/rpc_method.h>
#include <grpcpp/support/sync_stream.h>

#include <atomic>
#include <cassert>
#include <chrono>
#include <limits>
#include <memory>
#include <optional>
#include <utility>

#include "core/call_state.h"
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

// Converts ClientContext's wall-clock deadline to a steady_clock deadline.
// Returns false if the deadline is already in the past (caller fails fast
// with DEADLINE_EXCEEDED before queuing anything).
bool ComputeDeadline(const std::chrono::system_clock::time_point& wall_deadline,
                     std::optional<std::chrono::steady_clock::time_point>* out) {
  out->reset();
  if (wall_deadline == std::chrono::system_clock::time_point::max()) return true;
  const auto now = std::chrono::system_clock::now();
  if (wall_deadline <= now) return false;
  const auto delta =
      std::chrono::duration_cast<std::chrono::steady_clock::duration>(wall_deadline - now);
  const auto steady_now = std::chrono::steady_clock::now();
  if (delta >= std::chrono::steady_clock::time_point::max() - steady_now) {
    return true;
  }
  *out = steady_now + delta;
  return true;
}

Status ResultToStatus(const egrpc::internal::CallCompletion::Result& result,
                      const std::multimap<std::string, std::string>* trailing_metadata) {
  if (result.code == egrpc::internal::StatusCode::kOk) return Status();
  std::string details;
  if (trailing_metadata != nullptr) {
    auto it = trailing_metadata->find("grpc-status-details-bin");
    if (it != trailing_metadata->end()) details = it->second;
  }
  return Status(static_cast<StatusCode>(result.code), result.message, details);
}

// Builds a weak-referenced TryCancel hook that never keeps either
// CallCompletion or ChannelImpl alive and performs zero heap allocations.
std::function<void()> MakeCancelHook(const std::shared_ptr<egrpc::internal::ChannelImpl>& impl,
                                     const std::shared_ptr<egrpc::internal::CallCompletion>& comp) {
  std::weak_ptr<egrpc::internal::ChannelImpl> weak_ch = impl;
  std::weak_ptr<egrpc::internal::CallCompletion> weak_comp = comp;
  return [weak_ch, weak_comp]() noexcept {
    if (auto c = weak_comp.lock()) {
      c->RequestCancel();
      if (auto ch = weak_ch.lock()) ch->CancelCall(c);
    }
  };
}

}  // namespace

namespace internal {

class ClientStreamRawImpl final : public ClientStreamRaw {
 public:
  ClientStreamRawImpl(ClientContext* context, std::weak_ptr<egrpc::internal::ChannelImpl> channel,
                      uint64_t call_id, std::shared_ptr<egrpc::internal::CallCompletion> completion,
                      uint32_t max_receive_message_size)
      : context_(context),
        channel_(std::move(channel)),
        call_id_(call_id),
        completion_(std::move(completion)),
        max_receive_message_size_(max_receive_message_size) {}

  // Locally-failed reader (lame channel, expired deadline, pre-cancel, ...).
  // `context` may be null; when set, Finish() marks its status local.
  ClientStreamRawImpl(ClientContext* context, Status lame_status)
      : context_(context), saw_eof_(true), lame_status_(std::move(lame_status)) {}

  ~ClientStreamRawImpl() override {
    // §5.8: destroying a ClientReader without Finish() performs non-blocking
    // allocation-free cancel-and-discard and never touches ClientContext*
    // (which may already have been destroyed or out of scope).
    if (!finished_ && completion_) {
      completion_->RequestCancel();
      if (auto ch = channel_.lock()) ch->CancelCall(completion_);
    }
  }

  void WaitForInitialMetadata() override {
    assert(!finished_ && !read_started_ &&
           "WaitForInitialMetadata() is only legal before the first Read()");
    if (lame_status_.has_value() || !completion_) return;
    completion_->WaitForInitialMetadata();
    EnsureInitialMetadataDeposited();
  }

  bool ReadRaw(std::string* response_bytes) override {
    assert(!finished_ && "Read() called after Finish()");
    if (saw_eof_ || lame_status_.has_value() || !completion_) {
      saw_eof_ = true;
      return false;
    }
    read_started_ = true;
#ifndef NDEBUG
    // Single-reader guard lives out-of-line here (never in the public header)
    // so mixed Debug/Release TUs never hit an ODR/ABI layout mismatch.
    const bool already = in_read_.exchange(true, std::memory_order_acq_rel);
    assert(!already && "concurrent ClientReader::Read() is undefined");
    struct Guard {
      std::atomic<bool>& flag;
      ~Guard() { flag.store(false, std::memory_order_release); }
    } guard{in_read_};
#endif
    size_t wire_bytes = 0;
    if (!completion_->PopMessage(response_bytes, &wire_bytes)) {
      EnsureInitialMetadataDeposited();
      saw_eof_ = true;
      return false;
    }
    EnsureInitialMetadataDeposited();
    if (auto ch = channel_.lock()) ch->NotifyReadConsumed(call_id_, wire_bytes);
    return true;
  }

  bool NextMessageSize(uint32_t* sz) override {
    assert(!finished_ && "NextMessageSize() called after Finish()");
    // Matches upstream gRPC v1.82.0 ClientReader::NextMessageSize: non-blocking
    // upper bound equal to the channel's max_receive_message_size (returns true
    // even after EOF; returns false only for null out-param or lame reader).
    if (sz == nullptr || lame_status_.has_value() || !completion_) return false;
    *sz = max_receive_message_size_;
    return true;
  }

  void CancelWithError(Status status) override {
    local_error_override_ = std::move(status);
    saw_eof_ = true;
    if (completion_) {
      completion_->RequestCancel();
      if (auto ch = channel_.lock()) ch->CancelCall(completion_);
    }
  }

  Status Finish() override {
    assert(!finished_ && "ClientReader::Finish() called more than once");
    finished_ = true;
    if (lame_status_.has_value()) {
      if (context_ != nullptr) context_->egrpc_status_is_local_ = true;
      return *lame_status_;
    }

    // §5.8: calling Finish() without draining cancels-and-discards so
    // Read-driven HTTP/2 flow control cannot deadlock.
    if (!saw_eof_) {
      if (!completion_->is_done()) {
        completion_->RequestCancel();
        if (auto ch = channel_.lock()) ch->CancelCall(completion_);
      } else {
        completion_->DiscardQueued();
      }
      saw_eof_ = true;
    }

    const egrpc::internal::CallCompletion::Result& r = completion_->Wait();
    completion_->DiscardQueued();

    if (context_ != nullptr) {
      context_->UnbindCancel();
      // A local override (e.g. a response that failed to parse) wins over
      // whatever terminal status the wire produced.
      context_->egrpc_status_is_local_ =
          local_error_override_.has_value() || r.origin == egrpc::internal::StatusOrigin::kLocal;
      if (!initial_metadata_deposited_) {
        DepositMetadata(r.initial_metadata, &context_->recv_initial_metadata_,
                        &context_->recv_initial_view_);
        initial_metadata_deposited_ = true;
      }
      DepositMetadata(r.trailing_metadata, &context_->recv_trailing_metadata_,
                      &context_->recv_trailing_view_);
    }

    if (local_error_override_.has_value()) return *local_error_override_;
    return ResultToStatus(r, context_ != nullptr ? &context_->recv_trailing_metadata_ : nullptr);
  }

 private:
  void EnsureInitialMetadataDeposited() {
    if (initial_metadata_deposited_ || context_ == nullptr || !completion_) return;
    DepositMetadata(completion_->InitialMetadataCopy(), &context_->recv_initial_metadata_,
                    &context_->recv_initial_view_);
    initial_metadata_deposited_ = true;
  }

  ClientContext* context_ = nullptr;
  std::weak_ptr<egrpc::internal::ChannelImpl> channel_;
  uint64_t call_id_ = 0;
  std::shared_ptr<egrpc::internal::CallCompletion> completion_;
  uint32_t max_receive_message_size_ = 0;
  bool initial_metadata_deposited_ = false;
  bool read_started_ = false;
  bool saw_eof_ = false;
  bool finished_ = false;
#ifndef NDEBUG
  std::atomic<bool> in_read_{false};
#endif
  std::optional<Status> local_error_override_;
  std::optional<Status> lame_status_;
};

}  // namespace internal

Channel::Channel(std::shared_ptr<egrpc::internal::ChannelImpl> impl, Status lame_status)
    : impl_(std::move(impl)), lame_status_(std::move(lame_status)) {}

Channel::~Channel() {
  // Deterministic shutdown on Channel destruction so any surviving
  // ClientReader / TryCancel weak_ptr observers see the channel closed and
  // the EventThread is joined before ~Channel() returns.
  if (impl_) impl_->Shutdown();
}

std::shared_ptr<Channel> Channel::FromImpl(std::unique_ptr<egrpc::internal::ChannelImpl> impl) {
  return std::shared_ptr<Channel>(
      new Channel(std::shared_ptr<egrpc::internal::ChannelImpl>(std::move(impl)), Status()));
}

std::shared_ptr<Channel> Channel::Lame(Status lame_status) {
  return std::shared_ptr<Channel>(new Channel(nullptr, std::move(lame_status)));
}

Status Channel::BlockingUnaryCallShim(const internal::RpcMethod& method, ClientContext* context,
                                      const std::string& request, std::string* response) {
  // Every early return below is a locally-generated status.
  context->egrpc_status_is_local_ = true;
  if (!impl_) return lame_status_;

  std::optional<std::chrono::steady_clock::time_point> deadline;
  if (!ComputeDeadline(context->deadline_, &deadline)) {
    return Status(StatusCode::DEADLINE_EXCEEDED, "Deadline Exceeded");
  }

  const uint64_t call_id = impl_->AllocateCallId();
  auto completion = std::make_shared<egrpc::internal::CallCompletion>(call_id);
  if (context->BindCancel(MakeCancelHook(impl_, completion))) {
    return Status(StatusCode::CANCELLED, "Cancelled");
  }

  egrpc::internal::Http2Session::HeaderList metadata;
  metadata.reserve(context->send_initial_metadata_.size());
  for (const auto& kv : context->send_initial_metadata_) {
    metadata.emplace_back(kv.first, kv.second);
  }

  (void)impl_->StartCall(egrpc::internal::CallMode::kUnary, method.name(), request,
                         std::move(metadata), deadline, call_id, completion,
                         context->wait_for_ready_);
  const egrpc::internal::CallCompletion::Result& result = completion->Wait();
  context->UnbindCancel();
  context->egrpc_status_is_local_ = (result.origin == egrpc::internal::StatusOrigin::kLocal);

  DepositMetadata(result.initial_metadata, &context->recv_initial_metadata_,
                  &context->recv_initial_view_);
  DepositMetadata(result.trailing_metadata, &context->recv_trailing_metadata_,
                  &context->recv_trailing_view_);

  if (result.code == egrpc::internal::StatusCode::kOk) {
    *response = result.response;
    return Status();
  }
  return ResultToStatus(result, &context->recv_trailing_metadata_);
}

std::unique_ptr<internal::ClientStreamRaw> Channel::StartServerStreamShim(
    const internal::RpcMethod& method, ClientContext* context, const std::string& request) {
  // Local early returns are marked local by ClientStreamRawImpl::Finish().
  if (!impl_) return std::make_unique<internal::ClientStreamRawImpl>(context, lame_status_);

  std::optional<std::chrono::steady_clock::time_point> deadline;
  if (!ComputeDeadline(context->deadline_, &deadline)) {
    return std::make_unique<internal::ClientStreamRawImpl>(
        context, Status(StatusCode::DEADLINE_EXCEEDED, "Deadline Exceeded"));
  }

  const uint64_t call_id = impl_->AllocateCallId();
  auto completion = std::make_shared<egrpc::internal::CallCompletion>(call_id);
  if (context->BindCancel(MakeCancelHook(impl_, completion))) {
    return std::make_unique<internal::ClientStreamRawImpl>(
        context, Status(StatusCode::CANCELLED, "Cancelled"));
  }

  egrpc::internal::Http2Session::HeaderList metadata;
  metadata.reserve(context->send_initial_metadata_.size());
  for (const auto& kv : context->send_initial_metadata_) {
    metadata.emplace_back(kv.first, kv.second);
  }

  (void)impl_->StartCall(egrpc::internal::CallMode::kServerStreaming, method.name(), request,
                         std::move(metadata), deadline, call_id, completion,
                         context->wait_for_ready_);

  const size_t max_recv = impl_->max_receive_message_size();
  const uint32_t max_recv_u32 = max_recv >= std::numeric_limits<uint32_t>::max()
                                    ? std::numeric_limits<uint32_t>::max()
                                    : static_cast<uint32_t>(max_recv);
  return std::make_unique<internal::ClientStreamRawImpl>(context, impl_, call_id,
                                                         std::move(completion), max_recv_u32);
}

namespace internal {

Status BlockingUnaryCallRaw(ChannelInterface* channel, const RpcMethod& method,
                            ClientContext* context, const std::string& request,
                            std::string* response) {
  auto* egrpc_channel = dynamic_cast<Channel*>(channel);
  if (egrpc_channel == nullptr) {
    ClientContextAccess::MarkStatusLocal(context);
    return Status(StatusCode::INTERNAL, "channel was not created by egrpc's grpc::CreateChannel");
  }
  return egrpc_channel->BlockingUnaryCallShim(method, context, request, response);
}

std::unique_ptr<ClientStreamRaw> StartServerStreamRaw(ChannelInterface* channel,
                                                      const RpcMethod& method,
                                                      ClientContext* context,
                                                      const std::string& request) {
  auto* egrpc_channel = dynamic_cast<Channel*>(channel);
  if (egrpc_channel == nullptr) {
    return std::make_unique<ClientStreamRawImpl>(
        context,
        Status(StatusCode::INTERNAL, "channel was not created by egrpc's grpc::CreateChannel"));
  }
  return egrpc_channel->StartServerStreamShim(method, context, request);
}

}  // namespace internal
}  // namespace grpc
