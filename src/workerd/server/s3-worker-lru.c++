#include "s3-worker-lru.h"

#include "server.h"

#include <kj/debug.h>
#include <kj/vector.h>

#if __linux__
#include <fstream>
#include <unistd.h>
#elif __APPLE__
#include <mach/mach.h>
#endif

namespace workerd::server {

// -- S3ServiceBorrower --

void S3ServiceBorrower::disposeImpl(void* pointer) const {
  // const_cast because disposeImpl is const but releaseRequest mutates LRU state
  const_cast<S3WorkerLru&>(lru).releaseRequest(workerId);
}

// -- S3RequestGuard --

S3RequestGuard::~S3RequestGuard() noexcept(false) {
  lru.releaseRequest(workerId);
}

// -- S3WorkerLru --

S3WorkerLru::S3WorkerLru(Server& server, const kj::MonotonicClock& clock, Options options)
    : options(options), server(server), clock(clock) {}

S3WorkerLru::~S3WorkerLru() noexcept(false) {
  // Remove all entries from the LRU list before destroying them,
  // since kj::ListLink asserts it's not linked on destruction.
  while (lruList.size() > 0) {
    auto& front = lruList.front();
    lruList.remove(front);
  }
}

void S3WorkerLru::touch(kj::StringPtr workerId) {
  KJ_IF_SOME(entry, entries.find(workerId)) {
    auto& e = *entry;
    e.lastAccessed = clock.now();
    // Move to back of LRU list (most recently used)
    if (e.lruLink.isLinked()) {
      lruList.remove(e);
    }
    lruList.add(e);
  }
}

kj::Own<S3RequestGuard> S3WorkerLru::acquireRequest(kj::StringPtr workerId) {
  KJ_IF_SOME(entry, entries.find(workerId)) {
    auto& e = *entry;
    e.activeRequests++;
    e.lastAccessed = clock.now();
  }
  return kj::heap<S3RequestGuard>(*this, workerId);
}

void S3WorkerLru::releaseRequest(kj::StringPtr workerId) {
  KJ_IF_SOME(entry, entries.find(workerId)) {
    auto& e = *entry;
    KJ_ASSERT(e.activeRequests > 0, "releaseRequest called with zero active requests");
    e.activeRequests--;
  }
}

void S3WorkerLru::insert(kj::StringPtr workerId, size_t estimatedHeapSize) {
  auto now = clock.now();
  auto entry = kj::heap<S3WorkerEntry>();
  entry->workerId = kj::str(workerId);
  entry->lastAccessed = now;
  entry->createdAt = now;
  entry->estimatedHeapSize = estimatedHeapSize;

  auto& ref = *entry;
  entries.upsert(kj::str(workerId), kj::mv(entry), [](auto&, auto&&) {
    KJ_FAIL_ASSERT("S3WorkerLru::insert called for existing entry");
  });
  lruList.add(ref);
  totalEstimatedMemory += estimatedHeapSize;
}

void S3WorkerLru::markCreating(kj::StringPtr workerId) {
  // Create a placeholder entry that can't be evicted
  auto now = clock.now();
  auto entry = kj::heap<S3WorkerEntry>();
  entry->workerId = kj::str(workerId);
  entry->lastAccessed = now;
  entry->createdAt = now;
  entry->isCreating = true;

  auto& ref = *entry;
  entries.upsert(kj::str(workerId), kj::mv(entry), [](auto&, auto&&) {});
  lruList.add(ref);
}

void S3WorkerLru::markCreated(kj::StringPtr workerId, size_t estimatedHeapSize) {
  KJ_IF_SOME(entry, entries.find(workerId)) {
    auto& e = *entry;
    e.isCreating = false;
    e.estimatedHeapSize = estimatedHeapSize;
    totalEstimatedMemory += estimatedHeapSize;
  }
}

void S3WorkerLru::removeCreating(kj::StringPtr workerId) {
  KJ_IF_SOME(entry, entries.find(workerId)) {
    if (entry->isCreating) {
      lruList.remove(*entry);
      entries.erase(workerId);
    }
  }
}

bool S3WorkerLru::evictIfNeeded() {
  auto now = clock.now();

  // Phase 1: Evict stale entries regardless of memory pressure
  evictStale();

  // Phase 2: Memory pressure eviction (process RSS vs soft limit)
  size_t rss = getProcessRssBytes();
  if (rss > options.softMemoryLimit) {
    // Collect eviction candidates from LRU (oldest first)
    kj::Vector<S3WorkerEntry*> candidates;
    for (auto& entry: lruList) {
      if (entry.activeRequests == 0 &&
          !entry.isCreating &&
          (now - entry.createdAt) > options.minAge) {
        candidates.add(&entry);
      }
    }
    for (auto* entry: candidates) {
      if (getProcessRssBytes() <= options.softMemoryLimit) break;
      evictEntry(*entry);
    }
  }

  // Phase 3: Hard limit check
  if (getProcessRssBytes() > options.hardMemoryLimit) {
    KJ_LOG(WARNING, "S3 worker LRU: process RSS exceeds hard memory limit, rejecting new load",
        getProcessRssBytes(), options.hardMemoryLimit);
    return false;
  }

  // Phase 4: Count limit check
  if (entries.size() >= options.maxWorkers) {
    // Try to evict the LRU idle entry
    for (auto& entry: lruList) {
      if (entry.activeRequests == 0 && !entry.isCreating) {
        evictEntry(entry);
        return true;  // made room
      }
    }
    KJ_LOG(WARNING, "S3 worker LRU: at max workers and all active, rejecting new load",
        entries.size(), options.maxWorkers);
    return false;  // all active, can't make room
  }

  return true;
}

void S3WorkerLru::evictStale() {
  auto now = clock.now();
  kj::Vector<S3WorkerEntry*> toEvict;
  for (auto& entry: lruList) {
    if (entry.activeRequests == 0 &&
        !entry.isCreating &&
        (now - entry.lastAccessed) > options.staleTimeout) {
      toEvict.add(&entry);
    }
  }
  for (auto* entry: toEvict) {
    KJ_LOG(INFO, "S3 worker LRU: evicting stale worker", entry->workerId);
    evictEntry(*entry);
  }
}

void S3WorkerLru::evictEntry(S3WorkerEntry& entry) {
  KJ_ASSERT(entry.activeRequests == 0, "Cannot evict entry with active requests");
  KJ_ASSERT(!entry.isCreating, "Cannot evict entry that is still being created");

  auto workerId = kj::str(entry.workerId);
  totalEstimatedMemory -= entry.estimatedHeapSize;

  // Remove from LRU list
  lruList.remove(entry);

  // Remove and destroy the service from Server::services.
  server.evictS3WorkerService(workerId);

  // Remove from entries map (destroys S3WorkerEntry)
  entries.erase(workerId);

  KJ_LOG(INFO, "S3 worker LRU: evicted worker", workerId);
}

bool S3WorkerLru::contains(kj::StringPtr workerId) const {
  return entries.find(workerId) != kj::none;
}

size_t S3WorkerLru::getProcessRssBytes() {
#if __linux__
  std::ifstream statm("/proc/self/statm");
  if (statm.is_open()) {
    size_t vmSize, rss;
    statm >> vmSize >> rss;
    return rss * sysconf(_SC_PAGESIZE);
  }
  return 0;
#elif __APPLE__
  mach_task_basic_info_data_t info;
  mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
  if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                reinterpret_cast<task_info_t>(&info), &count) == KERN_SUCCESS) {
    return info.resident_size;
  }
  return 0;
#else
  return 0;  // unsupported platform
#endif
}

}  // namespace workerd::server
