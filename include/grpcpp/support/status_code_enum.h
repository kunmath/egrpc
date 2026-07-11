// egrpc grpcpp shim — the canonical gRPC status codes (design §4.5).
//
// Values are fixed by the gRPC spec and must match both upstream
// grpc::StatusCode and egrpc::internal::StatusCode (the core enum this shim
// maps onto); a mismatch would corrupt every status crossing the shim
// boundary.
#pragma once

namespace grpc {

enum StatusCode {
  OK = 0,
  CANCELLED = 1,
  UNKNOWN = 2,
  INVALID_ARGUMENT = 3,
  DEADLINE_EXCEEDED = 4,
  NOT_FOUND = 5,
  ALREADY_EXISTS = 6,
  PERMISSION_DENIED = 7,
  RESOURCE_EXHAUSTED = 8,
  FAILED_PRECONDITION = 9,
  ABORTED = 10,
  OUT_OF_RANGE = 11,
  UNIMPLEMENTED = 12,
  INTERNAL = 13,
  UNAVAILABLE = 14,
  DATA_LOSS = 15,
  UNAUTHENTICATED = 16,

  // Upstream sentinel; never a real status.
  DO_NOT_USE = -1,
};

}  // namespace grpc
