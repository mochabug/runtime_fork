// Copyright (c) 2024 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <kj/string.h>

namespace workerd::server {

// Minimal AWS Signature V4 signing for S3 GET/LIST requests (empty body).
// Adds Authorization, X-Amz-Date, X-Amz-Content-Sha256, and Host headers.
struct S3SignedHeaders {
  kj::String authorization;
  kj::String amzDate;
  kj::String amzContentSha256;
};

S3SignedHeaders signS3Request(kj::StringPtr method,
    kj::StringPtr path,
    kj::StringPtr query,
    kj::StringPtr host,
    kj::StringPtr accessKey,
    kj::StringPtr secretKey,
    kj::StringPtr region,
    kj::StringPtr service = "s3"_kj);

}  // namespace workerd::server
