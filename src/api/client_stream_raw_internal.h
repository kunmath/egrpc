// egrpc — internal factory for the server-streaming reader. Lets unit tests
// drive a ClientStreamRaw from a caller-supplied CallCompletion without a
// live channel. Not part of the public API.
#pragma once

#include <cstdint>
#include <memory>

#include "core/call_state.h"
#include "grpcpp/client_context.h"
#include "grpcpp/support/sync_stream.h"

namespace grpc {
namespace internal {

std::unique_ptr<ClientStreamRaw> MakeClientStreamRawForCompletion(
    ClientContext* context, std::shared_ptr<egrpc::internal::CallCompletion> completion,
    uint32_t max_receive_message_size);

}  // namespace internal
}  // namespace grpc
