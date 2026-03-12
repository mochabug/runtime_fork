// Copyright (c) 2024 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "s3-auth.h"

#include <openssl/hmac.h>
#include <openssl/sha.h>

#include <kj/encoding.h>

#include <ctime>

namespace workerd::server {

namespace {

kj::Array<kj::byte> sha256(kj::ArrayPtr<const kj::byte> data) {
  auto result = kj::heapArray<kj::byte>(SHA256_DIGEST_LENGTH);
  SHA256(data.begin(), data.size(), result.begin());
  return result;
}

kj::String sha256Hex(kj::ArrayPtr<const kj::byte> data) {
  auto hash = sha256(data);
  return kj::encodeHex(hash);
}

kj::Array<kj::byte> hmacSha256(kj::ArrayPtr<const kj::byte> key, kj::ArrayPtr<const kj::byte> data) {
  auto result = kj::heapArray<kj::byte>(SHA256_DIGEST_LENGTH);
  unsigned int len = SHA256_DIGEST_LENGTH;
  HMAC(EVP_sha256(), key.begin(), key.size(), data.begin(), data.size(), result.begin(), &len);
  return result;
}

kj::String getUtcTimestamp() {
  time_t now = time(nullptr);
  struct tm tm;
  gmtime_r(&now, &tm);
  char buf[17];
  strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm);
  return kj::str(buf);
}

kj::String getDateStamp(kj::StringPtr amzDate) {
  // amzDate is "YYYYMMDDTHHmmSSZ", date stamp is first 8 chars.
  return kj::str(amzDate.first(8));
}

}  // namespace

S3SignedHeaders signS3Request(kj::StringPtr method,
    kj::StringPtr path,
    kj::StringPtr query,
    kj::StringPtr host,
    kj::StringPtr accessKey,
    kj::StringPtr secretKey,
    kj::StringPtr region,
    kj::StringPtr service) {
  auto amzDate = getUtcTimestamp();
  auto dateStamp = getDateStamp(amzDate);

  // Empty body hash (all our requests have empty bodies).
  auto payloadHash = sha256Hex(kj::ArrayPtr<const kj::byte>(nullptr, (size_t)0));

  // Canonical headers (must be sorted by lowercase header name).
  auto canonicalHeaders = kj::str(
      "host:", host, "\n",
      "x-amz-content-sha256:", payloadHash, "\n",
      "x-amz-date:", amzDate, "\n");
  auto signedHeaders = kj::str("host;x-amz-content-sha256;x-amz-date");

  // Canonical request.
  auto canonicalRequest = kj::str(
      method, "\n",
      path, "\n",
      query, "\n",
      canonicalHeaders, "\n",
      signedHeaders, "\n",
      payloadHash);

  // Credential scope.
  auto credentialScope = kj::str(dateStamp, "/", region, "/", service, "/aws4_request");

  // String to sign.
  auto canonicalRequestHash = sha256Hex(canonicalRequest.asBytes());
  auto stringToSign = kj::str(
      "AWS4-HMAC-SHA256\n",
      amzDate, "\n",
      credentialScope, "\n",
      canonicalRequestHash);

  // Signing key.
  auto kSecret = kj::str("AWS4", secretKey);
  auto kDate = hmacSha256(kSecret.asBytes(), dateStamp.asBytes());
  auto kRegion = hmacSha256(kDate, region.asBytes());
  auto kService = hmacSha256(kRegion, service.asBytes());
  auto kSigning = hmacSha256(kService, kj::StringPtr("aws4_request").asBytes());

  // Signature.
  auto signatureBytes = hmacSha256(kSigning, stringToSign.asBytes());
  auto signature = kj::encodeHex(signatureBytes);

  auto authorization = kj::str(
      "AWS4-HMAC-SHA256 Credential=", accessKey, "/", credentialScope,
      ", SignedHeaders=", signedHeaders,
      ", Signature=", signature);

  return S3SignedHeaders{
    .authorization = kj::mv(authorization),
    .amzDate = kj::mv(amzDate),
    .amzContentSha256 = kj::mv(payloadHash),
  };
}

}  // namespace workerd::server
