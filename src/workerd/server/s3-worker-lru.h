#pragma once

#include <kj/debug.h>
#include <kj/list.h>
#include <kj/map.h>
#include <kj/string.h>
#include <kj/time.h>

namespace workerd::server {

class Server;  // forward decl

struct S3WorkerLruOptions {
  size_t maxWorkers = 200;
  size_t softMemoryLimit = 1ULL * 1024 * 1024 * 1024;   // 1 GB
  size_t hardMemoryLimit = 2ULL * 1024 * 1024 * 1024;   // 2 GB
  kj::Duration staleTimeout = 15 * kj::MINUTES;
  kj::Duration minAge = 30 * kj::SECONDS;
  kj::Duration evictionInterval = 60 * kj::SECONDS;
};

struct S3WorkerEntry {
  kj::String workerId;
  uint32_t activeRequests = 0;
  kj::TimePoint lastAccessed = kj::origin<kj::TimePoint>();
  kj::TimePoint createdAt = kj::origin<kj::TimePoint>();
  size_t estimatedHeapSize = 0;
  bool isCreating = false;
  kj::ListLink<S3WorkerEntry> lruLink;
};

class S3WorkerLru;

// Disposer that decrements active request count when an S3 service borrow goes out of scope.
// Disposer that decrements active request count when an S3 service borrow goes out of scope.
// Used as the disposer for kj::Own<Service> to track active connections.
class S3ServiceBorrower final: public kj::Disposer {
public:
  S3ServiceBorrower(S3WorkerLru& lru, kj::StringPtr workerId)
      : lru(lru), workerId(kj::str(workerId)) {}

protected:
  void disposeImpl(void* pointer) const override;

private:
  S3WorkerLru& lru;
  kj::String workerId;
};

// RAII guard for tracking active requests. Decrements the count when destroyed.
class S3RequestGuard {
public:
  S3RequestGuard(S3WorkerLru& lru, kj::StringPtr workerId)
      : lru(lru), workerId(kj::str(workerId)) {}
  ~S3RequestGuard() noexcept(false);
  KJ_DISALLOW_COPY(S3RequestGuard);
  S3RequestGuard(S3RequestGuard&&) = default;
  S3RequestGuard& operator=(S3RequestGuard&&) = delete;

private:
  S3WorkerLru& lru;
  kj::String workerId;
};

class S3WorkerLru {
public:
  using Options = S3WorkerLruOptions;

  S3WorkerLru(Server& server, const kj::MonotonicClock& clock, Options options = {});
  ~S3WorkerLru() noexcept(false);
  KJ_DISALLOW_COPY_AND_MOVE(S3WorkerLru);

  // Called when a cached S3 worker is accessed. Moves entry to back of LRU list.
  void touch(kj::StringPtr workerId);

  // Increment active request count. Returns an RAII guard that decrements on destruction.
  kj::Own<S3RequestGuard> acquireRequest(kj::StringPtr workerId);

  // Decrement active request count. Called by S3ServiceBorrower disposer.
  void releaseRequest(kj::StringPtr workerId);

  // Register a newly created S3 worker. Adds to LRU list and entries map.
  void insert(kj::StringPtr workerId, size_t estimatedHeapSize = 0);

  // Mark an entry as being created (async). Prevents eviction during co_await.
  void markCreating(kj::StringPtr workerId);

  // Clear the creating flag after createS3WorkerService completes.
  void markCreated(kj::StringPtr workerId, size_t estimatedHeapSize = 0);

  // Remove a placeholder entry that was being created (e.g. on creation failure).
  void removeCreating(kj::StringPtr workerId);

  // Try to evict workers to make room. Returns false if we cannot make room
  // (all workers active and at/over hard limit or maxWorkers).
  bool evictIfNeeded();

  // Evict stale entries. Called by periodic timer.
  void evictStale();

  // Check if a workerId is tracked (i.e., is an S3 worker).
  bool contains(kj::StringPtr workerId) const;

  // Get current process RSS in bytes (platform-specific).
  static size_t getProcessRssBytes();

  // Number of tracked entries.
  size_t size() const { return entries.size(); }

  Options options;

private:
  Server& server;
  const kj::MonotonicClock& clock;

  kj::List<S3WorkerEntry, &S3WorkerEntry::lruLink> lruList;
  kj::HashMap<kj::String, kj::Own<S3WorkerEntry>> entries;
  size_t totalEstimatedMemory = 0;

  // Evict a single entry. Removes from LRU, entries, and Server::services.
  // The entry must have activeRequests == 0 and isCreating == false.
  void evictEntry(S3WorkerEntry& entry);
};

}  // namespace workerd::server
