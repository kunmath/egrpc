// egrpc grpcpp shim — grpc::StubOptions (design §4.5). The generated
// NewStub() passes this through to internal::RpcMethod; egrpc keeps the
// suffix but attaches no stats machinery to it.
#pragma once

namespace grpc {

class StubOptions {
 public:
  StubOptions() = default;
  explicit StubOptions(const char* suffix_for_stats) : suffix_for_stats_(suffix_for_stats) {}

  void set_suffix_for_stats(const char* suffix) { suffix_for_stats_ = suffix; }
  const char* suffix_for_stats() const { return suffix_for_stats_; }

 private:
  const char* suffix_for_stats_ = nullptr;
};

}  // namespace grpc
