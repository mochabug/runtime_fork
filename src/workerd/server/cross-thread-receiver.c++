// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "cross-thread-receiver.h"

#include <cstring>

namespace workerd::server {

// =======================================================================================
// PrependedAsyncIoStream

PrependedAsyncIoStream::PrependedAsyncIoStream(
    kj::Array<kj::byte> preamble, kj::Own<kj::AsyncIoStream> inner)
    : preamble(kj::mv(preamble)),
      inner(kj::mv(inner)) {}

kj::Promise<size_t> PrependedAsyncIoStream::tryRead(
    void* buffer, size_t minBytes, size_t maxBytes) {
  auto dst = reinterpret_cast<kj::byte*>(buffer);
  size_t bytesRead = 0;

  // Serve from preamble first.
  size_t remaining = preamble.size() - offset;
  if (remaining > 0) {
    size_t toCopy = kj::min(remaining, maxBytes);
    memcpy(dst, preamble.begin() + offset, toCopy);
    offset += toCopy;
    bytesRead += toCopy;

    if (bytesRead >= minBytes) {
      co_return bytesRead;
    }
    dst += toCopy;
    maxBytes -= toCopy;
    minBytes -= toCopy;
  }

  // Need more bytes from inner stream.
  auto innerRead = co_await inner->tryRead(dst, minBytes, maxBytes);
  co_return bytesRead + innerRead;
}

kj::Promise<void> PrependedAsyncIoStream::write(kj::ArrayPtr<const kj::byte> buffer) {
  return inner->write(buffer);
}

kj::Promise<void> PrependedAsyncIoStream::write(
    kj::ArrayPtr<const kj::ArrayPtr<const kj::byte>> pieces) {
  return inner->write(pieces);
}

kj::Promise<void> PrependedAsyncIoStream::whenWriteDisconnected() {
  return inner->whenWriteDisconnected();
}

void PrependedAsyncIoStream::shutdownWrite() {
  inner->shutdownWrite();
}

void PrependedAsyncIoStream::abortRead() {
  inner->abortRead();
}

void PrependedAsyncIoStream::getsockopt(int level, int option, void* value, uint* length) {
  inner->getsockopt(level, option, value, length);
}

void PrependedAsyncIoStream::setsockopt(
    int level, int option, const void* value, uint length) {
  inner->setsockopt(level, option, value, length);
}

void PrependedAsyncIoStream::getsockname(struct sockaddr* addr, uint* length) {
  inner->getsockname(addr, length);
}

void PrependedAsyncIoStream::getpeername(struct sockaddr* addr, uint* length) {
  inner->getpeername(addr, length);
}

kj::Maybe<int> PrependedAsyncIoStream::getFd() const {
  return inner->getFd();
}

// =======================================================================================
// CrossThreadConnectionReceiver

CrossThreadConnectionReceiver::CrossThreadConnectionReceiver(
    kj::LowLevelAsyncIoProvider& lowLevel)
    : lowLevel(lowLevel) {}

void CrossThreadConnectionReceiver::pushConnection(int fd, kj::Array<kj::byte> preamble) {
  queue.lockExclusive()->add(PendingConn{fd, kj::mv(preamble)});
  notifier->notify();
}

kj::Promise<kj::Own<kj::AsyncIoStream>> CrossThreadConnectionReceiver::accept() {
  for (;;) {
    // Check queue first (handles batched pushes without waiting).
    {
      auto lock = queue.lockExclusive();
      if (lock->size() > 0) {
        auto conn = kj::mv((*lock)[0]);
        // Shift remaining elements.
        for (size_t i = 1; i < lock->size(); i++) {
          (*lock)[i - 1] = kj::mv((*lock)[i]);
        }
        lock->removeLast();

        auto stream = lowLevel.wrapSocketFd(conn.fd,
            kj::LowLevelAsyncIoProvider::TAKE_OWNERSHIP |
                kj::LowLevelAsyncIoProvider::ALREADY_NONBLOCK);

        if (conn.preamble.size() > 0) {
          co_return kj::heap<PrependedAsyncIoStream>(kj::mv(conn.preamble), kj::mv(stream));
        }
        co_return kj::mv(stream);
      }
    }
    // Queue empty — wait for notification.
    co_await notifier->awaitNotification();
  }
}

}  // namespace workerd::server
