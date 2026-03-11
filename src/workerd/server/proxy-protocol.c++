// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "proxy-protocol.h"

#include <kj/array.h>

#include <cstring>
#include <sys/socket.h>

namespace workerd::server {

namespace {

constexpr kj::byte PROXY_V2_SIGNATURE[12] = {
    0x0D, 0x0A, 0x0D, 0x0A, 0x00, 0x0D, 0x0A, 0x51, 0x55, 0x49, 0x54, 0x0A};
constexpr uint PROXY_V2_HEADER_LEN = 16;  // signature(12) + ver/cmd(1) + fam(1) + len(2)
constexpr kj::byte PP2_TYPE_WORKER_ID = 0xE0;

// Address block sizes by address family (high nibble of byte 13).
uint addressBlockSize(kj::byte familyByte) {
  switch (familyByte >> 4) {
    case 0x0:
      return 0;  // AF_UNSPEC (LOCAL command)
    case 0x1:
      return 12;  // AF_INET:  src(4) + dst(4) + srcport(2) + dstport(2)
    case 0x2:
      return 36;  // AF_INET6: src(16) + dst(16) + srcport(2) + dstport(2)
    case 0x3:
      return 216;  // AF_UNIX:  src(108) + dst(108)
    default:
      return 0;
  }
}

// Non-blocking exact-read helper. Returns false if read would block or fails.
bool readExactNonBlocking(int fd, kj::byte* buf, size_t len, int extraFlags = 0) {
  size_t pos = 0;
  while (pos < len) {
    ssize_t n = recv(fd, buf + pos, len - pos, MSG_DONTWAIT | extraFlags);
    if (n <= 0) return false;
    pos += n;
    // MSG_PEEK always reads from buffer start, so only one iteration for peek.
    if (extraFlags & MSG_PEEK) return pos >= len;
  }
  return true;
}

}  // namespace

ProxyV2Result parseProxyV2Header(int fd) {
  // Peek at the first 16 bytes without consuming them.
  kj::byte header[PROXY_V2_HEADER_LEN];
  if (!readExactNonBlocking(fd, header, PROXY_V2_HEADER_LEN, MSG_PEEK)) {
    // Not enough data available or read error. For non-PROXY connections
    // (plain HTTP), the client might not have sent 16 bytes yet. Treat as
    // non-PROXY: return success with no preamble.
    return {.success = true};
  }

  // Check signature.
  if (memcmp(header, PROXY_V2_SIGNATURE, 12) != 0) {
    // Not PROXY v2 — data stays in socket buffer, nothing consumed.
    return {.success = true};
  }

  // It IS PROXY v2. Consume the 16-byte header.
  if (!readExactNonBlocking(fd, header, PROXY_V2_HEADER_LEN)) {
    return {};  // read failed
  }

  // Parse payload length (bytes 14-15, big-endian).
  uint16_t payloadLen = (static_cast<uint16_t>(header[14]) << 8) | header[15];

  // Read and consume payload (address block + TLVs).
  auto payload = kj::heapArray<kj::byte>(payloadLen);
  if (payloadLen > 0 && !readExactNonBlocking(fd, payload.begin(), payloadLen)) {
    return {};  // read failed
  }

  // Build full preamble (header + payload) for replay to worker thread.
  auto preamble = kj::heapArray<kj::byte>(PROXY_V2_HEADER_LEN + payloadLen);
  memcpy(preamble.begin(), header, PROXY_V2_HEADER_LEN);
  memcpy(preamble.begin() + PROXY_V2_HEADER_LEN, payload.begin(), payloadLen);

  // Scan TLVs for worker ID (after the address block).
  uint addrSize = addressBlockSize(header[13]);
  kj::Maybe<kj::String> workerId;

  if (addrSize < payloadLen) {
    size_t pos = addrSize;
    while (pos + 3 <= payloadLen) {
      kj::byte tlvType = payload[pos];
      uint16_t tlvLen = (static_cast<uint16_t>(payload[pos + 1]) << 8) | payload[pos + 2];
      pos += 3;
      if (pos + tlvLen > payloadLen) break;

      if (tlvType == PP2_TYPE_WORKER_ID) {
        workerId = kj::heapString(reinterpret_cast<const char*>(payload.begin() + pos), tlvLen);
        break;
      }
      pos += tlvLen;
    }
  }

  return {.success = true, .preamble = kj::mv(preamble), .workerId = kj::mv(workerId)};
}

kj::Promise<ProxyV2Result> parseProxyV2FromStream(kj::AsyncInputStream& stream) {
  // Read the 16-byte header.
  auto header = kj::heapArray<kj::byte>(PROXY_V2_HEADER_LEN);
  size_t n = co_await stream.tryRead(header.begin(), PROXY_V2_HEADER_LEN, PROXY_V2_HEADER_LEN);
  if (n < PROXY_V2_HEADER_LEN) {
    co_return ProxyV2Result{};  // read failed
  }

  // Check signature.
  if (memcmp(header.begin(), PROXY_V2_SIGNATURE, 12) != 0) {
    co_return ProxyV2Result{};  // not PROXY v2
  }

  // Parse payload length (bytes 14-15, big-endian).
  uint16_t payloadLen = (static_cast<uint16_t>(header[14]) << 8) | header[15];

  // Read payload (address block + TLVs).
  auto payload = kj::heapArray<kj::byte>(payloadLen);
  if (payloadLen > 0) {
    n = co_await stream.tryRead(payload.begin(), payloadLen, payloadLen);
    if (n < payloadLen) {
      co_return ProxyV2Result{};  // read failed
    }
  }

  // Build full preamble (header + payload) for reference.
  auto preamble = kj::heapArray<kj::byte>(PROXY_V2_HEADER_LEN + payloadLen);
  memcpy(preamble.begin(), header.begin(), PROXY_V2_HEADER_LEN);
  memcpy(preamble.begin() + PROXY_V2_HEADER_LEN, payload.begin(), payloadLen);

  // Scan TLVs for worker ID (after the address block).
  uint addrSize = addressBlockSize(header[13]);
  kj::Maybe<kj::String> workerId;

  if (addrSize < payloadLen) {
    size_t pos = addrSize;
    while (pos + 3 <= payloadLen) {
      kj::byte tlvType = payload[pos];
      uint16_t tlvLen = (static_cast<uint16_t>(payload[pos + 1]) << 8) | payload[pos + 2];
      pos += 3;
      if (pos + tlvLen > payloadLen) break;

      if (tlvType == PP2_TYPE_WORKER_ID) {
        workerId = kj::heapString(reinterpret_cast<const char*>(payload.begin() + pos), tlvLen);
        break;
      }
      pos += tlvLen;
    }
  }

  co_return ProxyV2Result{.success = true, .preamble = kj::mv(preamble), .workerId = kj::mv(workerId)};
}

kj::Array<kj::byte> buildProxyV2Header(kj::StringPtr workerId) {
  // TLV: type(1) + length(2) + value(workerId.size())
  uint16_t tlvLen = workerId.size();
  uint16_t payloadLen = 3 + tlvLen;  // TLV header(3) + value

  auto result = kj::heapArray<kj::byte>(PROXY_V2_HEADER_LEN + payloadLen);
  auto ptr = result.begin();

  // Signature (12 bytes)
  memcpy(ptr, PROXY_V2_SIGNATURE, 12);
  ptr += 12;

  // Version (0x2) | Command (LOCAL = 0x0) → 0x20
  *ptr++ = 0x20;

  // Address family (AF_UNSPEC = 0x00) | Transport (UNSPEC = 0x00) → 0x00
  *ptr++ = 0x00;

  // Payload length (big-endian)
  *ptr++ = static_cast<kj::byte>(payloadLen >> 8);
  *ptr++ = static_cast<kj::byte>(payloadLen & 0xFF);

  // TLV: type = PP2_TYPE_WORKER_ID (0xE0)
  *ptr++ = PP2_TYPE_WORKER_ID;
  // TLV length (big-endian)
  *ptr++ = static_cast<kj::byte>(tlvLen >> 8);
  *ptr++ = static_cast<kj::byte>(tlvLen & 0xFF);
  // TLV value
  memcpy(ptr, workerId.begin(), tlvLen);

  return result;
}

}  // namespace workerd::server
