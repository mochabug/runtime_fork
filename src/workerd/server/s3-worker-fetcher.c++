// Copyright (c) 2024 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "s3-worker-fetcher.h"

#include "s3-auth.h"

#include <kj/debug.h>
#include <kj/encoding.h>

#include <stdio.h>
#include <dirent.h>
#include <fcntl.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

namespace workerd::server {

using kj::byte;

namespace {

// Read entire file contents from disk path.
kj::Maybe<kj::Array<byte>> readFileBytes(kj::StringPtr path) {
  int fd = open(path.cStr(), O_RDONLY);
  if (fd < 0) return kj::none;
  KJ_DEFER(close(fd));

  struct stat st;
  if (fstat(fd, &st) < 0) return kj::none;

  auto data = kj::heapArray<byte>(st.st_size);
  ssize_t n = read(fd, data.begin(), data.size());
  if (n < 0 || (size_t)n != data.size()) return kj::none;

  return kj::mv(data);
}

// List files in a directory.
kj::Vector<kj::String> listDirectory(kj::StringPtr dirPath) {
  kj::Vector<kj::String> result;
  DIR* d = opendir(dirPath.cStr());
  if (d == nullptr) return result;
  KJ_DEFER(closedir(d));

  while (auto* entry = readdir(d)) {
    kj::StringPtr name = entry->d_name;
    if (name == "." || name == "..") continue;
    result.add(kj::str(name));
  }
  return result;
}

// Ensure directory exists, creating parents as needed.
void mkdirRecursive(kj::StringPtr path) {
  if (mkdir(path.cStr(), 0755) == 0) return;
  if (errno == EEXIST) return;
  if (errno == ENOENT) {
    auto pathStr = kj::str(path);
    KJ_IF_SOME(pos, pathStr.findLast('/')) {
      auto parent = kj::str(pathStr.first(pos));
      mkdirRecursive(parent);
      mkdir(path.cStr(), 0755);
    }
  }
}

// Write bytes to a file atomically.
void writeFileBytes(kj::StringPtr path, kj::ArrayPtr<const byte> data) {
  auto tmpPath = kj::str(path, ".tmp");
  int fd = open(tmpPath.cStr(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
  KJ_REQUIRE(fd >= 0, "Failed to create file", tmpPath, strerror(errno));
  KJ_DEFER(close(fd));

  ssize_t n = write(fd, data.begin(), data.size());
  KJ_REQUIRE((size_t)n == data.size(), "Short write", path);

  KJ_SYSCALL(::rename(tmpPath.cStr(), path.cStr()));
}

// Parse host and port from an S3 endpoint URL.
struct ParsedUrl {
  kj::String scheme;
  kj::String host;
  kj::String port;
};

ParsedUrl parseEndpointUrl(kj::StringPtr url) {
  kj::StringPtr remaining = url;
  kj::String scheme;
  if (remaining.startsWith("https://")) {
    scheme = kj::str("https");
    remaining = remaining.slice(8);
  } else if (remaining.startsWith("http://")) {
    scheme = kj::str("http");
    remaining = remaining.slice(7);
  } else {
    scheme = kj::str("https");
  }

  kj::String host;
  kj::String port;
  KJ_IF_SOME(colonPos, remaining.findFirst(':')) {
    host = kj::str(remaining.first(colonPos));
    port = kj::str(remaining.slice(colonPos + 1));
    KJ_IF_SOME(slashPos, kj::StringPtr(port).findFirst('/')) {
      port = kj::str(kj::StringPtr(port).first(slashPos));
    }
  } else {
    KJ_IF_SOME(slashPos, remaining.findFirst('/')) {
      host = kj::str(remaining.first(slashPos));
    } else {
      host = kj::str(remaining);
    }
    port = kj::str(scheme == "https" ? "443" : "80");
  }

  return ParsedUrl{kj::mv(scheme), kj::mv(host), kj::mv(port)};
}

// Read all bytes from a socket until EOF or error.
kj::Array<byte> readAll(int fd) {
  kj::Vector<byte> buffer;
  byte chunk[8192];
  for (;;) {
    ssize_t n = ::read(fd, chunk, sizeof(chunk));
    if (n <= 0) break;
    buffer.addAll(kj::ArrayPtr<const byte>(chunk, n));
  }
  return buffer.releaseAsArray();
}

// Minimal HTTP/1.1 GET using POSIX sockets.
struct HttpResponse {
  int statusCode;
  kj::Array<byte> body;
};

HttpResponse httpGet(kj::StringPtr endpoint, kj::StringPtr path, kj::StringPtr query,
    kj::StringPtr accessKey, kj::StringPtr secretKey, kj::StringPtr region) {
  auto parsed = parseEndpointUrl(endpoint);

  auto hostHeader = parsed.port == "443" || parsed.port == "80"
      ? kj::str(parsed.host) : kj::str(parsed.host, ":", parsed.port);

  auto signed_ = signS3Request("GET", path, query, hostHeader, accessKey, secretKey, region);

  // Build request line and headers.
  kj::String requestLine;
  if (query.size() > 0) {
    requestLine = kj::str("GET ", path, "?", query, " HTTP/1.1\r\n");
  } else {
    requestLine = kj::str("GET ", path, " HTTP/1.1\r\n");
  }

  auto request = kj::str(
      requestLine,
      "Host: ", hostHeader, "\r\n",
      "Authorization: ", signed_.authorization, "\r\n",
      "X-Amz-Date: ", signed_.amzDate, "\r\n",
      "X-Amz-Content-Sha256: ", signed_.amzContentSha256, "\r\n",
      "Connection: close\r\n",
      "\r\n");

  // Resolve and connect.
  struct addrinfo hints = {};
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;

  struct addrinfo* result;
  int gaiResult = getaddrinfo(parsed.host.cStr(), parsed.port.cStr(), &hints, &result);
  KJ_REQUIRE(gaiResult == 0, "DNS resolution failed", parsed.host, gai_strerror(gaiResult));
  KJ_DEFER(freeaddrinfo(result));

  int sockfd = socket(result->ai_family, result->ai_socktype, result->ai_protocol);
  KJ_REQUIRE(sockfd >= 0, "socket() failed");
  KJ_DEFER(close(sockfd));

  KJ_REQUIRE(connect(sockfd, result->ai_addr, result->ai_addrlen) == 0,
      "connect() failed", parsed.host, parsed.port, strerror(errno));

  // Send request.
  auto requestBytes = request.asBytes();
  size_t sent = 0;
  while (sent < requestBytes.size()) {
    ssize_t n = ::write(sockfd, requestBytes.begin() + sent, requestBytes.size() - sent);
    KJ_REQUIRE(n > 0, "write failed");
    sent += n;
  }

  // Read response.
  auto rawResponse = readAll(sockfd);

  // Parse HTTP response: find status code and body.
  auto responseStr = kj::StringPtr(
      reinterpret_cast<const char*>(rawResponse.begin()), rawResponse.size());

  // Find status code from "HTTP/1.1 200 OK\r\n"
  int statusCode = 0;
  if (responseStr.size() > 12) {
    // Parse status code from bytes 9-11
    auto statusStr = responseStr.slice(9, 12);
    statusCode = (statusStr[0] - '0') * 100 + (statusStr[1] - '0') * 10 + (statusStr[2] - '0');
  }

  // Find body after "\r\n\r\n"
  kj::Array<byte> body;
  for (size_t i = 0; i + 3 < rawResponse.size(); i++) {
    if (rawResponse[i] == '\r' && rawResponse[i+1] == '\n' &&
        rawResponse[i+2] == '\r' && rawResponse[i+3] == '\n') {
      size_t bodyStart = i + 4;

      // Check if response uses chunked transfer encoding.
      auto headersStr = responseStr.first(i);
      bool isChunked = false;
      // Simple case-insensitive check for "transfer-encoding: chunked"
      for (size_t j = 0; j + 25 < headersStr.size(); j++) {
        if ((headersStr[j] == 'T' || headersStr[j] == 't') &&
            (headersStr[j+1] == 'r' || headersStr[j+1] == 'R')) {
          auto slice = headersStr.slice(j);
          if (slice.size() >= 26) {
            // Just look for "chunked" anywhere in headers for simplicity.
            for (size_t k = 0; k + 6 < slice.size() && k < 80; k++) {
              if ((slice[k] == 'c' || slice[k] == 'C') &&
                  (slice[k+1] == 'h' || slice[k+1] == 'H') &&
                  (slice[k+2] == 'u' || slice[k+2] == 'U') &&
                  (slice[k+3] == 'n' || slice[k+3] == 'N') &&
                  (slice[k+4] == 'k' || slice[k+4] == 'K') &&
                  (slice[k+5] == 'e' || slice[k+5] == 'E') &&
                  (slice[k+6] == 'd' || slice[k+6] == 'D')) {
                isChunked = true;
                break;
              }
            }
          }
          if (isChunked) break;
        }
      }

      if (isChunked) {
        // Decode chunked transfer encoding.
        kj::Vector<byte> decoded;
        size_t pos = bodyStart;
        while (pos < rawResponse.size()) {
          // Read chunk size (hex).
          size_t chunkSizeEnd = pos;
          while (chunkSizeEnd < rawResponse.size() && rawResponse[chunkSizeEnd] != '\r') {
            chunkSizeEnd++;
          }
          if (chunkSizeEnd >= rawResponse.size()) break;

          auto chunkSizeStr = responseStr.slice(pos, chunkSizeEnd);
          size_t chunkSize = 0;
          for (size_t ci = 0; ci < chunkSizeStr.size(); ci++) {
            char c = chunkSizeStr[ci];
            chunkSize *= 16;
            if (c >= '0' && c <= '9') chunkSize += c - '0';
            else if (c >= 'a' && c <= 'f') chunkSize += c - 'a' + 10;
            else if (c >= 'A' && c <= 'F') chunkSize += c - 'A' + 10;
            else break;
          }

          if (chunkSize == 0) break;  // Last chunk.

          pos = chunkSizeEnd + 2;  // Skip \r\n after chunk size.
          if (pos + chunkSize > rawResponse.size()) break;
          decoded.addAll(kj::ArrayPtr<const byte>(rawResponse.begin() + pos, chunkSize));
          pos += chunkSize + 2;  // Skip chunk data + \r\n.
        }
        body = decoded.releaseAsArray();
      } else {
        body = kj::heapArray<byte>(rawResponse.slice(bodyStart));
      }
      break;
    }
  }

  return HttpResponse{
    .statusCode = statusCode,
    .body = kj::mv(body),
  };
}

// Parse S3 ListObjectsV2 XML response to extract object keys.
kj::Vector<kj::String> parseListObjectKeys(kj::ArrayPtr<const byte> xmlBody) {
  kj::Vector<kj::String> keys;
  auto body = kj::StringPtr(reinterpret_cast<const char*>(xmlBody.begin()), xmlBody.size());

  size_t pos = 0;
  while (pos < body.size()) {
    auto keyStart = body.slice(pos);
    // Find <Key>
    size_t found = 0;
    bool foundTag = false;
    for (size_t i = 0; i + 4 < keyStart.size(); i++) {
      if (keyStart[i] == '<' && keyStart[i+1] == 'K' && keyStart[i+2] == 'e' &&
          keyStart[i+3] == 'y' && keyStart[i+4] == '>') {
        found = i + 5;
        foundTag = true;
        break;
      }
    }
    if (!foundTag) break;

    auto valueStart = keyStart.slice(found);
    // Find </Key>
    bool foundEnd = false;
    for (size_t i = 0; i + 5 < valueStart.size(); i++) {
      if (valueStart[i] == '<' && valueStart[i+1] == '/' && valueStart[i+2] == 'K' &&
          valueStart[i+3] == 'e' && valueStart[i+4] == 'y' && valueStart[i+5] == '>') {
        keys.add(kj::str(valueStart.first(i)));
        pos += found + i + 6;
        foundEnd = true;
        break;
      }
    }
    if (!foundEnd) break;
  }

  return keys;
}

}  // namespace

S3WorkerFetcher::S3WorkerFetcher(S3Config config)
    : config(kj::mv(config)) {}

kj::Maybe<S3WorkerFetcher::WorkerKey> S3WorkerFetcher::parseWorkerId(kj::StringPtr id) {
  // Expected format: "org/name/version"
  KJ_IF_SOME(firstSlash, id.findFirst('/')) {
    auto rest = id.slice(firstSlash + 1);
    KJ_IF_SOME(secondSlash, rest.findFirst('/')) {
      auto org = kj::str(id.first(firstSlash));
      auto name = kj::str(rest.first(secondSlash));
      auto version = kj::str(rest.slice(secondSlash + 1));
      if (org.size() > 0 && name.size() > 0 && version.size() > 0) {
        return WorkerKey{kj::mv(org), kj::mv(name), kj::mv(version)};
      }
    }
  }
  return kj::none;
}

kj::Maybe<FetchedWorkerModules> S3WorkerFetcher::fetchWorker(kj::StringPtr workerId) {
  KJ_IF_SOME(key, parseWorkerId(workerId)) {
    // Try disk cache first.
    KJ_IF_SOME(modules, loadFromDisk(key)) {
      KJ_LOG(INFO, "Loaded worker from disk cache", workerId);
      return kj::mv(modules);
    }

    // Try S3.
    try {
      KJ_IF_SOME(modules, fetchFromS3AndCache(key)) {
        KJ_LOG(INFO, "Fetched worker from S3 and cached", workerId);
        return kj::mv(modules);
      }
    } catch (kj::Exception& e) {
      KJ_LOG(WARNING, "S3 fetch failed", workerId, e.getDescription());
    }

    return kj::none;
  } else {
    return kj::none;
  }
}

kj::Maybe<FetchedWorkerModules> S3WorkerFetcher::loadFromDisk(const WorkerKey& key) {
  auto basePath = kj::str(
      config.cacheDir, "/", key.org, "/", key.name, "/", key.version, "/modules/executor");

  // Check for .incomplete marker — treat as cache miss.
  auto incompletePath = kj::str(basePath, "/.incomplete");
  if (access(incompletePath.cStr(), F_OK) == 0) {
    return kj::none;
  }

  // List directory contents.
  auto entries = listDirectory(basePath);
  if (entries.empty()) return kj::none;

  // Find main module: file starting with "executor" and ending .js or .cjs
  kj::Maybe<kj::String> mainModulePath;
  bool isCjs = false;
  kj::String mainModuleName;
  kj::Vector<FetchedWorkerModules::WasmFile> wasmFiles;

  for (auto& entry : entries) {
    auto filePath = kj::str(basePath, "/", entry);

    if (entry.startsWith("executor") && (entry.endsWith(".js") || entry.endsWith(".cjs"))) {
      mainModulePath = kj::mv(filePath);
      isCjs = entry.endsWith(".cjs");
      mainModuleName = kj::str(entry);
    } else if (entry.endsWith(".wasm")) {
      KJ_IF_SOME(data, readFileBytes(filePath)) {
        wasmFiles.add(FetchedWorkerModules::WasmFile{kj::str(entry), kj::mv(data)});
      }
    }
  }

  KJ_IF_SOME(path, mainModulePath) {
    KJ_IF_SOME(data, readFileBytes(path)) {
      return FetchedWorkerModules{
        .mainModule = kj::mv(data),
        .isCjs = isCjs,
        .mainModuleName = kj::mv(mainModuleName),
        .wasmModules = wasmFiles.releaseAsArray(),
      };
    }
  }

  return kj::none;
}

kj::Maybe<FetchedWorkerModules> S3WorkerFetcher::fetchFromS3AndCache(const WorkerKey& key) {
  // Build S3 prefix.
  auto prefix = kj::str(key.org, "/", key.name, "/", key.version, "/modules/executor/");

  // URL-encode the prefix for the query parameter.
  auto encodedPrefix = kj::encodeUriComponent(prefix);

  // List objects at prefix.
  auto listPath = kj::str("/", config.bucket);
  auto listQuery = kj::str("list-type=2&prefix=", encodedPrefix);

  auto listResponse = httpGet(config.endpoint, listPath, listQuery,
      config.accessKey, config.secretKey, "us-east-1"_kj);

  if (listResponse.statusCode != 200) {
    KJ_LOG(WARNING, "S3 ListObjects failed", listResponse.statusCode);
    return kj::none;
  }

  auto objectKeys = parseListObjectKeys(listResponse.body);
  if (objectKeys.empty()) return kj::none;

  // Fetch each object.
  kj::Maybe<kj::Array<byte>> mainModuleData;
  bool isCjs = false;
  kj::String mainModuleName;
  kj::Vector<FetchedWorkerModules::WasmFile> wasmFiles;

  for (auto& objKey : objectKeys) {
    // Extract filename from key (last component after /).
    kj::StringPtr filename = objKey;
    KJ_IF_SOME(lastSlash, objKey.findLast('/')) {
      filename = objKey.slice(lastSlash + 1);
    }

    if (filename.size() == 0) continue;  // directory marker

    auto getPath = kj::str("/", config.bucket, "/", objKey);
    auto getResponse = httpGet(config.endpoint, getPath, ""_kj,
        config.accessKey, config.secretKey, "us-east-1"_kj);

    if (getResponse.statusCode != 200) {
      KJ_LOG(WARNING, "S3 GetObject failed", objKey, getResponse.statusCode);
      continue;
    }

    if (filename.startsWith("executor") && (filename.endsWith(".js") || filename.endsWith(".cjs"))) {
      isCjs = filename.endsWith(".cjs");
      mainModuleName = kj::str(filename);
      mainModuleData = kj::mv(getResponse.body);
    } else if (filename.endsWith(".wasm")) {
      wasmFiles.add(FetchedWorkerModules::WasmFile{kj::str(filename), kj::mv(getResponse.body)});
    }
  }

  KJ_IF_SOME(mainData, mainModuleData) {
    // Write to disk cache.
    auto basePath = kj::str(
        config.cacheDir, "/", key.org, "/", key.name, "/", key.version, "/modules/executor");
    mkdirRecursive(basePath);

    // Write .incomplete marker.
    auto incompletePath = kj::str(basePath, "/.incomplete");
    writeFileBytes(incompletePath, kj::ArrayPtr<const byte>(nullptr, (size_t)0));

    // Write main module.
    auto mainPath = kj::str(basePath, "/", mainModuleName);
    writeFileBytes(mainPath, mainData);

    // Write wasm files.
    for (auto& wasm : wasmFiles) {
      auto wasmPath = kj::str(basePath, "/", wasm.name);
      writeFileBytes(wasmPath, wasm.data);
    }

    // Remove .incomplete marker.
    unlink(incompletePath.cStr());

    return FetchedWorkerModules{
      .mainModule = kj::mv(mainData),
      .isCjs = isCjs,
      .mainModuleName = kj::mv(mainModuleName),
      .wasmModules = wasmFiles.releaseAsArray(),
    };
  }

  return kj::none;
}

}  // namespace workerd::server
