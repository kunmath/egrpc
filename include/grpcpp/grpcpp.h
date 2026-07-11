// egrpc grpcpp shim — the umbrella header upstream user code includes
// (design §4.5). Pulls in the client-facing surface; the generated
// .grpc.pb.h files include their own dependencies directly.
#pragma once

#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/completion_queue.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>
#include <grpcpp/support/status.h>
#include <grpcpp/support/string_ref.h>
#include <grpcpp/support/sync_stream.h>
