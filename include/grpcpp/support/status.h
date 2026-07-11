// egrpc grpcpp shim — grpc::Status (design §4.5): code, message, and the
// binary error-details payload (grpc-status-details-bin). Value type,
// header-only.
#pragma once

#include <grpcpp/support/status_code_enum.h>

#include <string>

namespace grpc {

class Status {
 public:
  Status() = default;
  Status(StatusCode code, const std::string& error_message)
      : code_(code), error_message_(error_message) {}
  Status(StatusCode code, const std::string& error_message, const std::string& error_details)
      : code_(code), error_message_(error_message), binary_error_details_(error_details) {}

  StatusCode error_code() const { return code_; }
  const std::string& error_message() const { return error_message_; }
  const std::string& error_details() const { return binary_error_details_; }

  bool ok() const { return code_ == StatusCode::OK; }

  // Upstream parity: explicit "I checked and don't care" marker.
  void IgnoreError() const {}

  // Upstream compatibility: pre-defined convenience instances.
  static const Status& OK;
  static const Status& CANCELLED;

 private:
  StatusCode code_ = StatusCode::OK;
  std::string error_message_;
  std::string binary_error_details_;
};

namespace internal {
// Backing storage for Status::OK / Status::CANCELLED (C++17 inline
// variables, so the shim stays header-only here).
inline const Status g_status_ok{};
inline const Status g_status_cancelled{StatusCode::CANCELLED, ""};
}  // namespace internal

inline const Status& Status::OK = internal::g_status_ok;
inline const Status& Status::CANCELLED = internal::g_status_cancelled;

}  // namespace grpc
