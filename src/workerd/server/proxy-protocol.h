// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once
#include <kj/async-io.h>
#include <kj/common.h>
#include <kj/string.h>

namespace workerd::server {

struct ProxyV2Result {
  bool success = false;              // false if read failed (caller should close fd)
  kj::Array<kj::byte> preamble;     // consumed bytes to replay to worker thread (may be empty)
  kj::Maybe<kj::String> workerId;   // from TLV 0xE0, if present
};

// Parses PROXY v2 header from a non-blocking fd that has data ready.
// Uses MSG_PEEK to check the signature without consuming data for non-PROXY connections.
// For PROXY v2 connections, consumes the header+payload and returns them as preamble.
// The fd must be non-blocking and have data available (caller should wait for readability first).
ProxyV2Result parseProxyV2Header(int fd);

// Async PROXY v2 parser for kj::AsyncInputStream (used by Server per-connection).
// Returns the parsed result. The stream will be positioned after the PROXY v2 header.
kj::Promise<ProxyV2Result> parseProxyV2FromStream(kj::AsyncInputStream& stream);

// Build a PROXY v2 header with a worker ID TLV (for tests and tooling).
// Creates a minimal header with LOCAL command, AF_UNSPEC, and TLV 0xE0 containing workerId.
kj::Array<kj::byte> buildProxyV2Header(kj::StringPtr workerId);

}  // namespace workerd::server
