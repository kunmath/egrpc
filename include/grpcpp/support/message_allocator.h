// egrpc grpcpp shim — grpc::MessageAllocator (design §4.5): named by the
// generated callback-service SetMessageAllocatorFor_* methods. Server-side,
// so signature parity only (design §2: client-only).
#pragma once

namespace grpc {

class RpcAllocatorState {
 public:
  virtual ~RpcAllocatorState() = default;
};

template <class RequestT, class ResponseT>
class MessageHolder : public RpcAllocatorState {
 public:
  RequestT* request() { return request_; }
  ResponseT* response() { return response_; }

 protected:
  RequestT* request_ = nullptr;
  ResponseT* response_ = nullptr;
};

template <class RequestT, class ResponseT>
class MessageAllocator {
 public:
  virtual ~MessageAllocator() = default;
  virtual MessageHolder<RequestT, ResponseT>* AllocateMessages() = 0;
};

}  // namespace grpc
