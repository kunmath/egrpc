// egrpc grpcpp shim — server callback reactors (design §4.5): signature
// parity only; the generated callback-service classes name these as return
// types but no server ever runs them (design §2: client-only).
#pragma once

#include <grpcpp/support/status.h>
#include <grpcpp/support/sync_stream.h>

namespace grpc {

class ServerUnaryReactor {
 public:
  virtual ~ServerUnaryReactor() = default;
  void Finish(Status s) { (void)s; }
  void StartSendInitialMetadata() {}
  virtual void OnSendInitialMetadataDone(bool ok) { (void)ok; }
  virtual void OnDone() {}
  virtual void OnCancel() {}
};

template <class R>
class ServerReadReactor {
 public:
  virtual ~ServerReadReactor() = default;
  void Finish(Status s) { (void)s; }
  void StartSendInitialMetadata() {}
  void StartRead(R* msg) { (void)msg; }
  virtual void OnSendInitialMetadataDone(bool ok) { (void)ok; }
  virtual void OnReadDone(bool ok) { (void)ok; }
  virtual void OnDone() {}
  virtual void OnCancel() {}
};

template <class W>
class ServerWriteReactor {
 public:
  virtual ~ServerWriteReactor() = default;
  void Finish(Status s) { (void)s; }
  void StartSendInitialMetadata() {}
  void StartWrite(const W* msg) { (void)msg; }
  void StartWrite(const W* msg, WriteOptions options) {
    (void)msg;
    (void)options;
  }
  void StartWriteAndFinish(const W* msg, WriteOptions options, Status s) {
    (void)msg;
    (void)options;
    (void)s;
  }
  virtual void OnSendInitialMetadataDone(bool ok) { (void)ok; }
  virtual void OnWriteDone(bool ok) { (void)ok; }
  virtual void OnDone() {}
  virtual void OnCancel() {}
};

template <class R, class W>
class ServerBidiReactor {
 public:
  virtual ~ServerBidiReactor() = default;
  void Finish(Status s) { (void)s; }
  void StartSendInitialMetadata() {}
  void StartRead(R* msg) { (void)msg; }
  void StartWrite(const W* msg) { (void)msg; }
  void StartWrite(const W* msg, WriteOptions options) {
    (void)msg;
    (void)options;
  }
  void StartWriteAndFinish(const W* msg, WriteOptions options, Status s) {
    (void)msg;
    (void)options;
    (void)s;
  }
  virtual void OnSendInitialMetadataDone(bool ok) { (void)ok; }
  virtual void OnReadDone(bool ok) { (void)ok; }
  virtual void OnWriteDone(bool ok) { (void)ok; }
  virtual void OnDone() {}
  virtual void OnCancel() {}
};

}  // namespace grpc
