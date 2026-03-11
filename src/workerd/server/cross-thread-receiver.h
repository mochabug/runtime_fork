// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once
#include <workerd/util/xthreadnotifier.h>

#include <kj/async-io.h>
#include <kj/common.h>
#include <kj/mutex.h>
#include <kj/vector.h>

using kj::uint;

namespace workerd::server {

// Wraps an AsyncIoStream, prepending already-read bytes before delegating.
class PrependedAsyncIoStream final: public kj::AsyncIoStream {
public:
  PrependedAsyncIoStream(kj::Array<kj::byte> preamble, kj::Own<kj::AsyncIoStream> inner);

  // AsyncInputStream
  kj::Promise<size_t> tryRead(void* buffer, size_t minBytes, size_t maxBytes) override;

  // AsyncOutputStream
  kj::Promise<void> write(kj::ArrayPtr<const kj::byte> buffer) override;
  kj::Promise<void> write(kj::ArrayPtr<const kj::ArrayPtr<const kj::byte>> pieces) override;
  kj::Promise<void> whenWriteDisconnected() override;

  // AsyncIoStream
  void shutdownWrite() override;
  void abortRead() override;
  void getsockopt(int level, int option, void* value, uint* length) override;
  void setsockopt(int level, int option, const void* value, uint length) override;
  void getsockname(struct sockaddr* addr, uint* length) override;
  void getpeername(struct sockaddr* addr, uint* length) override;
  kj::Maybe<int> getFd() const override;

private:
  kj::Array<kj::byte> preamble;
  size_t offset = 0;
  kj::Own<kj::AsyncIoStream> inner;
};

// A ConnectionReceiver that receives connections dispatched from the main thread.
// Worker threads own instances of this; the main thread pushes fds via pushConnection().
class CrossThreadConnectionReceiver final: public kj::ConnectionReceiver {
public:
  explicit CrossThreadConnectionReceiver(kj::LowLevelAsyncIoProvider& lowLevel, uint port = 0);

  // Thread-safe. Called from dispatcher thread.
  void pushConnection(int fd, kj::Array<kj::byte> preamble);

  // ConnectionReceiver interface — called on the owning worker thread.
  kj::Promise<kj::Own<kj::AsyncIoStream>> accept() override;
  uint getPort() override {
    return port_;
  }

private:
  kj::LowLevelAsyncIoProvider& lowLevel;
  uint port_;
  struct PendingConn {
    int fd;
    kj::Array<kj::byte> preamble;
  };
  kj::MutexGuarded<kj::Vector<PendingConn>> queue;
  kj::Own<XThreadNotifier> notifier = XThreadNotifier::create();
};

}  // namespace workerd::server
