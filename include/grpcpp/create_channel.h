// egrpc grpcpp shim — grpc::CreateChannel / CreateCustomChannel (design
// §4.5). Target syntax: "host:port", "host" (port 443), or
// "dns:///host:port". Anything else — other resolver schemes, unix sockets
// — yields a lame channel failing with UNIMPLEMENTED rather than a nullptr
// surprise at call time.
#pragma once

#include <grpcpp/channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/channel_arguments.h>

#include <memory>
#include <string>

namespace grpc {

std::shared_ptr<Channel> CreateChannel(const std::string& target,
                                       const std::shared_ptr<ChannelCredentials>& creds);

std::shared_ptr<Channel> CreateCustomChannel(const std::string& target,
                                             const std::shared_ptr<ChannelCredentials>& creds,
                                             const ChannelArguments& args);

}  // namespace grpc
