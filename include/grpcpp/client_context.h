// egrpc grpcpp shim — grpc::ClientContext (design §4.5): deadline, initial
// metadata, wait_for_ready, TryCancel, and post-call access to server
// metadata. One context per call, created before and inspected after; it is
// never touched by the EventThread — grpc::Channel reads it before
// submitting and writes the results back after CallState::Wait returns, all
// on the caller's thread (design §3).
#pragma once

#include <grpcpp/support/string_ref.h>

#include <chrono>
#include <map>
#include <string>

namespace grpc {

class Channel;

class ClientContext {
 public:
  ClientContext() = default;

  ClientContext(const ClientContext&) = delete;
  ClientContext& operator=(const ClientContext&) = delete;

  // Absolute deadline; encoded as the grpc-timeout request header (§5.1).
  // A deadline already in the past fails the call with DEADLINE_EXCEEDED
  // before anything is sent. TODO(M5): local timer so an unresponsive
  // server cannot outlive the deadline either.
  void set_deadline(const std::chrono::system_clock::time_point& deadline) { deadline_ = deadline; }
  std::chrono::system_clock::time_point deadline() const { return deadline_; }

  // Initial metadata sent with the call, added before the RPC starts. Keys
  // are lowercased and `-bin` values base64-encoded by the core on the way
  // to the wire (§5.1), so both are passed raw here.
  void AddMetadata(const std::string& key, const std::string& value) {
    send_initial_metadata_.emplace(key, value);
  }

  // Stored for upstream parity; not honored until the M6 connectivity FSM
  // (M3-core semantics apply: calls queue while connecting, fail once the
  // channel has failed).
  void set_wait_for_ready(bool wait_for_ready) { wait_for_ready_ = wait_for_ready; }

  // TODO(M5): wired to CallState cancellation. No-op until then.
  void TryCancel() {}

  // Server metadata, valid once the call has finished. `-bin` values are
  // base64-decoded. The string_refs point into storage owned by this
  // context and stay valid for its lifetime.
  const std::multimap<grpc::string_ref, grpc::string_ref>& GetServerInitialMetadata() const {
    return recv_initial_view_;
  }
  const std::multimap<grpc::string_ref, grpc::string_ref>& GetServerTrailingMetadata() const {
    return recv_trailing_view_;
  }

 private:
  // grpc::Channel is the shim's call driver: it consumes the send side and
  // deposits the received metadata (building the string_ref views).
  friend class grpc::Channel;

  std::chrono::system_clock::time_point deadline_ = std::chrono::system_clock::time_point::max();
  bool wait_for_ready_ = false;
  std::multimap<std::string, std::string> send_initial_metadata_;

  // Received metadata storage + views into it. multimap nodes are stable,
  // so the views never dangle.
  std::multimap<std::string, std::string> recv_initial_metadata_;
  std::multimap<std::string, std::string> recv_trailing_metadata_;
  std::multimap<grpc::string_ref, grpc::string_ref> recv_initial_view_;
  std::multimap<grpc::string_ref, grpc::string_ref> recv_trailing_view_;
};

}  // namespace grpc
