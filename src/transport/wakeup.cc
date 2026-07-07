// egrpc — cross-thread wakeup primitive (design §2, §3).

#include "transport/wakeup.h"

#include <sys/eventfd.h>
#include <unistd.h>

#include <cerrno>

namespace egrpc {
namespace internal {

Wakeup::Wakeup() { fd_ = eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC); }

Wakeup::~Wakeup() {
  if (fd_ >= 0) close(fd_);
}

void Wakeup::Notify() {
  if (fd_ < 0) return;
  const uint64_t one = 1;
  ssize_t rc;
  do {
    rc = write(fd_, &one, sizeof(one));
  } while (rc < 0 && errno == EINTR);
  // EAGAIN: counter saturated — a wakeup is already pending, nothing to do.
}

void Wakeup::Drain() {
  if (fd_ < 0) return;
  uint64_t count;
  ssize_t rc;
  do {
    rc = read(fd_, &count, sizeof(count));
  } while (rc < 0 && errno == EINTR);
  // EAGAIN: already drained (level-triggered poll may report spuriously).
}

}  // namespace internal
}  // namespace egrpc
