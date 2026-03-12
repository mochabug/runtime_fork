// Copyright (c) 2024 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#pragma once

#include <kj/filesystem.h>
#include <kj/string.h>

namespace workerd::server {

struct S3Config {
  kj::String endpoint;
  kj::String accessKey;   // read from file at startup
  kj::String secretKey;   // read from file at startup
  kj::String bucket;
  kj::String cacheDir;
  kj::String compatDate;
};

struct FetchedWorkerModules {
  kj::Array<kj::byte> mainModule;
  bool isCjs;  // true if .cjs (CommonJS), false if .js (ESM)
  kj::String mainModuleName;

  struct WasmFile {
    kj::String name;
    kj::Array<kj::byte> data;
  };
  kj::Array<WasmFile> wasmModules;
};

class S3WorkerFetcher {
public:
  S3WorkerFetcher(S3Config config);

  // Parse "org/name/version", check disk cache, fetch from S3 on miss.
  // Returns kj::none if not found anywhere.
  kj::Maybe<FetchedWorkerModules> fetchWorker(kj::StringPtr workerId);

  const S3Config& getConfig() const { return config; }

private:
  struct WorkerKey {
    kj::String org;
    kj::String name;
    kj::String version;
  };

  kj::Maybe<WorkerKey> parseWorkerId(kj::StringPtr id);
  kj::Maybe<FetchedWorkerModules> loadFromDisk(const WorkerKey& key);
  kj::Maybe<FetchedWorkerModules> fetchFromS3AndCache(const WorkerKey& key);

  S3Config config;
};

}  // namespace workerd::server
