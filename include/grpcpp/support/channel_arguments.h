// egrpc grpcpp shim — grpc::ChannelArguments (design §4.5): accept-and-
// ignore unknown args; CreateCustomChannel honors the keepalive args
// (§5.2), the max message sizes, and the primary user-agent prefix. The
// GRPC_ARG_* names are upstream's channel-arg keys (normally from
// grpc/impl/channel_arg_names.h), defined here so user code compiles
// unchanged.
#pragma once

#include <map>
#include <string>

#ifndef GRPC_ARG_KEEPALIVE_TIME_MS
#define GRPC_ARG_KEEPALIVE_TIME_MS "grpc.keepalive_time_ms"
#endif
#ifndef GRPC_ARG_KEEPALIVE_TIMEOUT_MS
#define GRPC_ARG_KEEPALIVE_TIMEOUT_MS "grpc.keepalive_timeout_ms"
#endif
#ifndef GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS
#define GRPC_ARG_KEEPALIVE_PERMIT_WITHOUT_CALLS "grpc.keepalive_permit_without_calls"
#endif
#ifndef GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH
#define GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH "grpc.max_receive_message_length"
#endif
#ifndef GRPC_ARG_MAX_SEND_MESSAGE_LENGTH
#define GRPC_ARG_MAX_SEND_MESSAGE_LENGTH "grpc.max_send_message_length"
#endif
#ifndef GRPC_ARG_PRIMARY_USER_AGENT
#define GRPC_ARG_PRIMARY_USER_AGENT "grpc.primary_user_agent"
#endif

namespace grpc {

class ChannelArguments {
 public:
  void SetInt(const std::string& key, int value) { ints_[key] = value; }
  void SetString(const std::string& key, const std::string& value) { strings_[key] = value; }

  // Upstream convenience setters for the args egrpc honors.
  void SetMaxReceiveMessageSize(int size) { SetInt(GRPC_ARG_MAX_RECEIVE_MESSAGE_LENGTH, size); }
  void SetMaxSendMessageSize(int size) { SetInt(GRPC_ARG_MAX_SEND_MESSAGE_LENGTH, size); }
  void SetUserAgentPrefix(const std::string& user_agent_prefix) {
    SetString(GRPC_ARG_PRIMARY_USER_AGENT, user_agent_prefix);
  }

  // --- egrpc shim internal --------------------------------------------------
  const std::map<std::string, int>& egrpc_ints() const { return ints_; }
  const std::map<std::string, std::string>& egrpc_strings() const { return strings_; }

 private:
  std::map<std::string, int> ints_;
  std::map<std::string, std::string> strings_;
};

}  // namespace grpc
