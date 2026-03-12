// Copyright (c) 2017-2022 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

#include "s3-worker-lru.h"
#include "server.h"

#include <kj/async-io.h>
#include <kj/filesystem.h>
#include <kj/test.h>
#include <kj/thread.h>

namespace workerd::server {
namespace {

// ---------------------------------------------------------------------------
// Minimal mock infrastructure for constructing a Server that the LRU can
// reference.  We never call Server::run(); we only need the Server object
// (and its `services` HashMap) to exist.
// ---------------------------------------------------------------------------

class MockNetwork final: public kj::Network {
public:
  kj::Promise<kj::Own<kj::NetworkAddress>> parseAddress(
      kj::StringPtr, uint) override {
    KJ_UNIMPLEMENTED("unused in LRU tests");
  }
  kj::Own<kj::NetworkAddress> getSockaddr(const void*, uint) override {
    KJ_UNIMPLEMENTED("unused in LRU tests");
  }
  kj::Own<kj::Network> restrictPeers(kj::ArrayPtr<const kj::StringPtr>,
      kj::ArrayPtr<const kj::StringPtr>) override {
    KJ_UNIMPLEMENTED("unused in LRU tests");
  }
};

// We never actually access the filesystem in LRU tests, but Server requires
// a kj::Filesystem& at construction.  kj::newDiskFilesystem() provides one.

class MockEntropy final: public kj::EntropySource {
public:
  void generate(kj::ArrayPtr<kj::byte> buffer) override {
    buffer.fill(4);  // chosen by fair dice roll
  }
};

// A controllable clock for deterministic time-based testing.
class FakeClock final: public kj::MonotonicClock {
public:
  kj::TimePoint now() const override { return currentTime; }
  void advance(kj::Duration d) { currentTime = currentTime + d; }
  kj::TimePoint currentTime = kj::origin<kj::TimePoint>() + 1 * kj::HOURS;
};

// Provides a minimal Server + FakeClock for LRU testing.
//
// The Server's services map is empty; evictEntry's call to
// evictS3WorkerService() is a safe no-op when the workerId isn't found.
struct LruTestContext {
  kj::EventLoop loop;
  kj::WaitScope ws = kj::WaitScope(loop);
  kj::Own<kj::Filesystem> fs = kj::newDiskFilesystem();
  MockEntropy entropy;
  MockNetwork network;
  FakeClock clock;
  kj::TimerImpl timer = kj::TimerImpl(clock.currentTime);
  Server server;

  LruTestContext()
      : server(*fs, timer, clock, network, entropy,
            Worker::LoggingOptions{}, [](kj::String) {}) {}

  kj::Own<S3WorkerLru> makeLru(S3WorkerLruOptions opts = {}) {
    return kj::heap<S3WorkerLru>(server, clock, kj::mv(opts));
  }

  // Helper: make options that disable memory-pressure eviction
  // so tests focus on count-based / stale eviction.
  static S3WorkerLruOptions noMemoryPressureOpts(size_t maxWorkers = 200) {
    return S3WorkerLruOptions{
      .maxWorkers = maxWorkers,
      .softMemoryLimit = SIZE_MAX,
      .hardMemoryLimit = SIZE_MAX,
    };
  }
};

// ============================================================================
// Basic operations
// ============================================================================

KJ_TEST("S3WorkerLru: insert and contains") {
  LruTestContext ctx;
  auto lru = ctx.makeLru();

  KJ_EXPECT(lru->size() == 0);
  KJ_EXPECT(!lru->contains("worker-a"));

  lru->insert("worker-a");
  KJ_EXPECT(lru->size() == 1);
  KJ_EXPECT(lru->contains("worker-a"));

  lru->insert("worker-b");
  KJ_EXPECT(lru->size() == 2);
  KJ_EXPECT(lru->contains("worker-b"));
  KJ_EXPECT(lru->contains("worker-a"));
}

KJ_TEST("S3WorkerLru: count-based eviction at maxWorkers") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(5);
  auto lru = ctx.makeLru(opts);

  for (int i = 0; i < 5; i++) {
    lru->insert(kj::str("worker-", i));
  }
  KJ_EXPECT(lru->size() == 5);

  // At capacity — evictIfNeeded should evict the oldest.
  KJ_EXPECT(lru->evictIfNeeded());
  KJ_EXPECT(lru->size() == 4);
  KJ_EXPECT(!lru->contains("worker-0"));
  for (int i = 1; i < 5; i++) {
    KJ_EXPECT(lru->contains(kj::str("worker-", i)));
  }
}

KJ_TEST("S3WorkerLru: sequential eviction evicts in LRU order") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(3);
  auto lru = ctx.makeLru(opts);

  lru->insert("a");
  lru->insert("b");
  lru->insert("c");

  // Evict 1 — oldest is "a"
  KJ_EXPECT(lru->evictIfNeeded());
  KJ_EXPECT(!lru->contains("a"));
  KJ_EXPECT(lru->contains("b"));
  KJ_EXPECT(lru->contains("c"));

  // Insert "d", fill again
  lru->insert("d");
  KJ_EXPECT(lru->size() == 3);

  // Evict 1 — oldest is now "b"
  KJ_EXPECT(lru->evictIfNeeded());
  KJ_EXPECT(!lru->contains("b"));
  KJ_EXPECT(lru->contains("c"));
  KJ_EXPECT(lru->contains("d"));
}

KJ_TEST("S3WorkerLru: touch reorders LRU") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(3);
  auto lru = ctx.makeLru(opts);

  lru->insert("a");
  lru->insert("b");
  lru->insert("c");

  // Touch "a" — moves it to the back (most recently used).
  lru->touch("a");

  // At capacity. Evict should remove "b" (oldest untouched), not "a".
  KJ_EXPECT(lru->evictIfNeeded());
  KJ_EXPECT(lru->size() == 2);
  KJ_EXPECT(!lru->contains("b"));
  KJ_EXPECT(lru->contains("a"));
  KJ_EXPECT(lru->contains("c"));
}

// ============================================================================
// Active request tracking
// ============================================================================

KJ_TEST("S3WorkerLru: active requests prevent eviction") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(2);
  auto lru = ctx.makeLru(opts);

  lru->insert("a");
  lru->insert("b");

  // Pin "a" (the oldest).
  auto borrower = lru->acquireRequest("a");

  // At capacity. "a" has active requests so skip it, evict "b" instead.
  KJ_EXPECT(lru->evictIfNeeded());
  KJ_EXPECT(lru->size() == 1);
  KJ_EXPECT(lru->contains("a"));
  KJ_EXPECT(!lru->contains("b"));
}

KJ_TEST("S3WorkerLru: all workers active returns false") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(2);
  auto lru = ctx.makeLru(opts);

  lru->insert("a");
  lru->insert("b");

  auto borrowerA = lru->acquireRequest("a");
  auto borrowerB = lru->acquireRequest("b");

  // At capacity and all workers have active requests — cannot make room.
  KJ_EXPECT_LOG(WARNING, "at max workers and all active");
  KJ_EXPECT(!lru->evictIfNeeded());
  KJ_EXPECT(lru->size() == 2);
}

KJ_TEST("S3WorkerLru: RAII borrower releases on destruction") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(3);
  auto lru = ctx.makeLru(opts);

  lru->insert("a");
  lru->insert("b");

  {
    // Pin both entries.
    auto borrowerA = lru->acquireRequest("a");
    auto borrowerB = lru->acquireRequest("b");
    // "a" and "b" are pinned — verify eviction of a third entry still works.
    lru->insert("c");
    KJ_EXPECT(lru->size() == 3);
  }
  // Both borrowers destroyed — "a" and "b" now evictable.
  // Verify we can evict them.
  KJ_EXPECT(lru->evictIfNeeded());
  KJ_EXPECT(lru->size() == 2);
  KJ_EXPECT(!lru->contains("a"));  // "a" was oldest
}

KJ_TEST("S3WorkerLru: multiple acquireRequest on same worker") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(3);
  auto lru = ctx.makeLru(opts);

  lru->insert("a");

  // Acquire 3 requests on "a".
  auto b1 = lru->acquireRequest("a");
  auto b2 = lru->acquireRequest("a");
  auto b3 = lru->acquireRequest("a");

  // "a" has 3 active requests — verify it's not evictable via count check
  // by filling to capacity and checking which entry is evicted.
  lru->insert("b");
  lru->insert("c");

  // At capacity. "a" has active requests, skip. Evict "b" (oldest idle).
  KJ_EXPECT(lru->evictIfNeeded());
  KJ_EXPECT(lru->contains("a"));
  KJ_EXPECT(!lru->contains("b"));

  // Fill again.
  lru->insert("d");
  KJ_EXPECT(lru->evictIfNeeded());
  KJ_EXPECT(lru->contains("a"));  // still pinned
  KJ_EXPECT(!lru->contains("c"));

  // Release all requests and verify "a" can now be evicted.
  b1 = nullptr;
  b2 = nullptr;
  b3 = nullptr;

  lru->insert("e");
  KJ_EXPECT(lru->evictIfNeeded());
  // "a" is now the oldest idle entry.
  KJ_EXPECT(!lru->contains("a"));
}

// ============================================================================
// Creating state
// ============================================================================

KJ_TEST("S3WorkerLru: markCreating prevents eviction") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(2);
  auto lru = ctx.makeLru(opts);

  lru->insert("a");
  lru->markCreating("b");

  // At capacity. "b" is creating, skip. "a" is idle, evict.
  KJ_EXPECT(lru->evictIfNeeded());
  KJ_EXPECT(!lru->contains("a"));
  KJ_EXPECT(lru->contains("b"));

  lru->markCreated("b", 1024);
  KJ_EXPECT(lru->contains("b"));
}

KJ_TEST("S3WorkerLru: removeCreating cleans up placeholder") {
  LruTestContext ctx;
  auto lru = ctx.makeLru();

  lru->markCreating("worker-x");
  KJ_EXPECT(lru->size() == 1);
  KJ_EXPECT(lru->contains("worker-x"));

  lru->removeCreating("worker-x");
  KJ_EXPECT(lru->size() == 0);
  KJ_EXPECT(!lru->contains("worker-x"));
}

KJ_TEST("S3WorkerLru: removeCreating does not remove completed entries") {
  LruTestContext ctx;
  auto lru = ctx.makeLru();

  lru->insert("worker-x");  // fully created
  lru->removeCreating("worker-x");  // should be a no-op
  KJ_EXPECT(lru->size() == 1);
  KJ_EXPECT(lru->contains("worker-x"));
}

// ============================================================================
// Stale eviction
// ============================================================================

KJ_TEST("S3WorkerLru: stale timeout eviction") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts();
  opts.staleTimeout = 60 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  lru->insert("a");
  lru->insert("b");

  lru->evictStale();
  KJ_EXPECT(lru->size() == 2);  // not stale yet

  ctx.clock.advance(61 * kj::SECONDS);
  lru->evictStale();
  KJ_EXPECT(lru->size() == 0);
}

KJ_TEST("S3WorkerLru: touch prevents stale eviction") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts();
  opts.staleTimeout = 60 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  lru->insert("a");
  lru->insert("b");

  // Advance to just before stale timeout, touch "b".
  ctx.clock.advance(59 * kj::SECONDS);
  lru->touch("b");

  // Advance past original stale timeout.
  ctx.clock.advance(2 * kj::SECONDS);

  lru->evictStale();
  KJ_EXPECT(lru->size() == 1);
  KJ_EXPECT(!lru->contains("a"));
  KJ_EXPECT(lru->contains("b"));
}

KJ_TEST("S3WorkerLru: active requests prevent stale eviction") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts();
  opts.staleTimeout = 10 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  lru->insert("a");
  auto borrower = lru->acquireRequest("a");

  ctx.clock.advance(20 * kj::SECONDS);
  lru->evictStale();
  KJ_EXPECT(lru->size() == 1);  // "a" is stale by time but pinned
  KJ_EXPECT(lru->contains("a"));
}

KJ_TEST("S3WorkerLru: markCreating prevents stale eviction") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts();
  opts.staleTimeout = 10 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  lru->markCreating("a");

  ctx.clock.advance(20 * kj::SECONDS);
  lru->evictStale();
  KJ_EXPECT(lru->size() == 1);  // "a" is creating — immune
}

// ============================================================================
// Memory pressure
// ============================================================================

KJ_TEST("S3WorkerLru: soft memory limit triggers eviction") {
  LruTestContext ctx;
  S3WorkerLruOptions opts;
  opts.maxWorkers = 10000;
  opts.softMemoryLimit = 1;          // 1 byte — always over limit
  opts.hardMemoryLimit = SIZE_MAX;   // don't reject
  opts.minAge = 0 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  lru->insert("a");
  lru->insert("b");
  lru->insert("c");

  // Advance clock so entries pass the minAge > 0 check (0 duration is not > 0).
  ctx.clock.advance(1 * kj::MILLISECONDS);

  // Process RSS always exceeds 1 byte. Phase 2 evicts all eligible entries.
  KJ_EXPECT(lru->evictIfNeeded());
  KJ_EXPECT(lru->size() == 0);
}

KJ_TEST("S3WorkerLru: hard memory limit rejects new loads") {
  LruTestContext ctx;
  S3WorkerLruOptions opts;
  opts.maxWorkers = 10000;
  opts.softMemoryLimit = 1;
  opts.hardMemoryLimit = 1;          // 1 byte — always over limit
  opts.minAge = 0 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  // Even with no entries, process RSS > 1 byte → returns false.
  KJ_EXPECT_LOG(WARNING, "process RSS exceeds hard memory limit");
  KJ_EXPECT(!lru->evictIfNeeded());
}

KJ_TEST("S3WorkerLru: minAge prevents soft-limit eviction of young entries") {
  LruTestContext ctx;
  S3WorkerLruOptions opts;
  opts.maxWorkers = 10000;
  opts.softMemoryLimit = 1;          // always over
  opts.hardMemoryLimit = SIZE_MAX;
  opts.minAge = 300 * kj::SECONDS;   // 5 minutes
  auto lru = ctx.makeLru(opts);

  lru->insert("young");

  // Entry is < 5 min old, so Phase 2 skips it despite memory pressure.
  lru->evictIfNeeded();
  KJ_EXPECT(lru->size() == 1);
  KJ_EXPECT(lru->contains("young"));

  // Advance past minAge.
  ctx.clock.advance(301 * kj::SECONDS);
  lru->evictIfNeeded();
  KJ_EXPECT(lru->size() == 0);
}

// ============================================================================
// Edge cases
// ============================================================================

KJ_TEST("S3WorkerLru: touch non-existent worker is no-op") {
  LruTestContext ctx;
  auto lru = ctx.makeLru();
  lru->touch("ghost");
  KJ_EXPECT(lru->size() == 0);
}

KJ_TEST("S3WorkerLru: acquireRequest on non-existent worker disposes cleanly") {
  LruTestContext ctx;
  auto lru = ctx.makeLru();

  // Should not crash — releaseRequest on a non-existent entry is a no-op.
  auto borrower = lru->acquireRequest("ghost");
  borrower = nullptr;
  KJ_EXPECT(lru->size() == 0);
}

KJ_TEST("S3WorkerLru: evictIfNeeded on empty LRU") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts();
  auto lru = ctx.makeLru(opts);
  KJ_EXPECT(lru->evictIfNeeded());
  KJ_EXPECT(lru->size() == 0);
}

KJ_TEST("S3WorkerLru: evictStale on empty LRU") {
  LruTestContext ctx;
  auto lru = ctx.makeLru();
  lru->evictStale();
  KJ_EXPECT(lru->size() == 0);
}

KJ_TEST("S3WorkerLru: getProcessRssBytes returns nonzero") {
  size_t rss = S3WorkerLru::getProcessRssBytes();
  KJ_EXPECT(rss > 0, "expected nonzero RSS for a running process", rss);
}

// ============================================================================
// Stress tests — single-threaded
// ============================================================================

KJ_TEST("S3WorkerLru: stress 1000 workers with maxWorkers=50") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(50);
  opts.staleTimeout = 3600 * kj::SECONDS;  // won't fire
  auto lru = ctx.makeLru(opts);

  for (int i = 0; i < 1000; i++) {
    if (lru->size() >= opts.maxWorkers) {
      KJ_ASSERT(lru->evictIfNeeded());
    }
    lru->insert(kj::str("w-", i));
    KJ_ASSERT(lru->size() <= opts.maxWorkers);
  }

  // The most recent 50 workers should remain.
  KJ_EXPECT(lru->size() == 50);
  for (int i = 950; i < 1000; i++) {
    KJ_EXPECT(lru->contains(kj::str("w-", i)));
  }
  for (int i = 0; i < 950; i++) {
    KJ_EXPECT(!lru->contains(kj::str("w-", i)));
  }
}

KJ_TEST("S3WorkerLru: stress 1000 stale workers evicted in one pass") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(10000);
  opts.staleTimeout = 60 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  for (int i = 0; i < 1000; i++) {
    lru->insert(kj::str("stale-", i));
  }
  KJ_EXPECT(lru->size() == 1000);

  ctx.clock.advance(61 * kj::SECONDS);
  lru->evictStale();
  KJ_EXPECT(lru->size() == 0);
}

KJ_TEST("S3WorkerLru: stress interleaved touch keeps hot set alive") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(10000);
  opts.staleTimeout = 30 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  // Insert 100 workers.
  for (int i = 0; i < 100; i++) {
    lru->insert(kj::str("w-", i));
  }

  // Over 10 rounds of 5 seconds each (50s total), keep touching workers 50-99.
  for (int round = 0; round < 10; round++) {
    ctx.clock.advance(5 * kj::SECONDS);
    for (int i = 50; i < 100; i++) {
      lru->touch(kj::str("w-", i));
    }
  }

  // Workers 0-49 last accessed 50s ago (> 30s staleTimeout).
  // Workers 50-99 last touched ≤5s ago.
  lru->evictStale();

  for (int i = 0; i < 50; i++) {
    KJ_EXPECT(!lru->contains(kj::str("w-", i)), i);
  }
  for (int i = 50; i < 100; i++) {
    KJ_EXPECT(lru->contains(kj::str("w-", i)), i);
  }
  KJ_EXPECT(lru->size() == 50);
}

KJ_TEST("S3WorkerLru: stress pinned workers survive 1000 evictions") {
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(20);
  auto lru = ctx.makeLru(opts);

  // Insert and pin 5 workers.
  kj::Vector<kj::Own<S3RequestGuard>> borrowers;
  for (int i = 0; i < 5; i++) {
    auto id = kj::str("pinned-", i);
    lru->insert(id);
    borrowers.add(lru->acquireRequest(id));
  }

  // Insert 1000 more transient workers, forcing repeated eviction.
  // The 5 pinned workers must never be evicted.
  for (int i = 0; i < 1000; i++) {
    while (lru->size() >= opts.maxWorkers) {
      KJ_ASSERT(lru->evictIfNeeded());
    }
    lru->insert(kj::str("transient-", i));
  }

  for (int i = 0; i < 5; i++) {
    KJ_EXPECT(lru->contains(kj::str("pinned-", i)), i);
  }
  KJ_EXPECT(lru->size() <= opts.maxWorkers);
}

KJ_TEST("S3WorkerLru: repeated insert-evict cycles verify LRU order") {
  // Each insert at capacity triggers one eviction. Verify the evicted entry
  // is always the oldest.
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(50);
  auto lru = ctx.makeLru(opts);

  // Fill to capacity.
  for (int i = 0; i < 50; i++) {
    lru->insert(kj::str("w-", i));
  }

  // Insert 100 more, each forcing eviction of the oldest.
  for (int i = 50; i < 150; i++) {
    KJ_ASSERT(lru->size() == 50);
    KJ_ASSERT(lru->evictIfNeeded());  // evicts oldest
    KJ_EXPECT(lru->size() == 49);
    // The oldest was w-(i-50).
    KJ_EXPECT(!lru->contains(kj::str("w-", i - 50)));
    lru->insert(kj::str("w-", i));
  }
  KJ_EXPECT(lru->size() == 50);

  // Only w-100 through w-149 should remain.
  for (int i = 100; i < 150; i++) {
    KJ_EXPECT(lru->contains(kj::str("w-", i)));
  }
}

KJ_TEST("S3WorkerLru: stress rapid insert-touch-evict cycle") {
  // Simulates a realistic pattern: insert worker, handle a request (touch),
  // finish request, then evict oldest when limit is hit.
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(10);
  auto lru = ctx.makeLru(opts);

  for (int i = 0; i < 500; i++) {
    while (lru->size() >= opts.maxWorkers) {
      KJ_ASSERT(lru->evictIfNeeded());
    }
    auto id = kj::str("rapid-", i);
    lru->insert(id);

    // Simulate handling a request: acquire, touch, then release.
    {
      auto borrower = lru->acquireRequest(id);
      lru->touch(id);
    }  // borrower released here
  }

  KJ_EXPECT(lru->size() <= opts.maxWorkers);
  // Most recent workers should remain.
  for (int i = 490; i < 500; i++) {
    KJ_EXPECT(lru->contains(kj::str("rapid-", i)), i);
  }
}

// ============================================================================
// Multi-threaded stress tests
// ============================================================================

// Helper function that runs the LRU stress workload inside a thread.
// Extracted so it can be called from kj::Thread lambdas without duplicating
// the heavy logic.
static void runThreadLruStress(uint threadId, uint numWorkers, uint maxWorkers) {
  kj::EventLoop loop;
  kj::WaitScope ws(loop);
  auto fs = kj::newDiskFilesystem();
  MockEntropy entropy;
  MockNetwork network;
  FakeClock clock;
  kj::TimerImpl timer(clock.currentTime);

  Server server(*fs, timer, clock, network, entropy,
      Worker::LoggingOptions{}, [](kj::String) {});

  S3WorkerLruOptions opts;
  opts.maxWorkers = maxWorkers;
  opts.softMemoryLimit = SIZE_MAX;
  opts.hardMemoryLimit = SIZE_MAX;
  opts.staleTimeout = 3600 * kj::SECONDS;
  auto lru = kj::heap<S3WorkerLru>(server, clock, opts);

  for (uint i = 0; i < numWorkers; i++) {
    auto id = kj::str("t", threadId, "-w", i);
    while (lru->size() >= opts.maxWorkers) {
      KJ_ASSERT(lru->evictIfNeeded(), "thread should evict idle workers", threadId, i);
    }
    lru->insert(id);
    KJ_ASSERT(lru->size() <= opts.maxWorkers, threadId, lru->size());
  }

  KJ_ASSERT(lru->size() == maxWorkers, threadId, lru->size());
  for (uint i = numWorkers - maxWorkers; i < numWorkers; i++) {
    KJ_ASSERT(lru->contains(kj::str("t", threadId, "-w", i)), threadId, i);
  }
}

KJ_TEST("S3WorkerLru: 4 threads x 250 workers = 1000 total, maxWorkers=30") {
  static constexpr uint NUM_THREADS = 4;
  static constexpr uint WORKERS_PER_THREAD = 250;
  static constexpr uint MAX_WORKERS = 30;

  kj::MutexGuarded<uint> completed(0);
  {
    auto threads = kj::heapArray<kj::Own<kj::Thread>>(NUM_THREADS);
    for (uint t = 0; t < NUM_THREADS; t++) {
      threads[t] = kj::heap<kj::Thread>([t, &completed]() {
        runThreadLruStress(t, WORKERS_PER_THREAD, MAX_WORKERS);
        *completed.lockExclusive() += 1;
      });
    }
    // Thread destructors join here.
  }
  KJ_EXPECT(*completed.lockExclusive() == NUM_THREADS);
}

KJ_TEST("S3WorkerLru: 4 threads x 500 workers = 2000 total, maxWorkers=25") {
  static constexpr uint NUM_THREADS = 4;
  static constexpr uint WORKERS_PER_THREAD = 500;
  static constexpr uint MAX_WORKERS = 25;

  kj::MutexGuarded<uint> completed(0);
  {
    auto threads = kj::heapArray<kj::Own<kj::Thread>>(NUM_THREADS);
    for (uint t = 0; t < NUM_THREADS; t++) {
      threads[t] = kj::heap<kj::Thread>([t, &completed]() {
        runThreadLruStress(t, WORKERS_PER_THREAD, MAX_WORKERS);
        *completed.lockExclusive() += 1;
      });
    }
  }
  KJ_EXPECT(*completed.lockExclusive() == NUM_THREADS);
}

// Thread workload with mixed active requests + stale eviction.
static void runThreadMixedStress(uint threadId, uint numWorkers, uint maxWorkers) {
  kj::EventLoop loop;
  kj::WaitScope ws(loop);
  auto fs = kj::newDiskFilesystem();
  MockEntropy entropy;
  MockNetwork network;
  FakeClock clock;
  kj::TimerImpl timer(clock.currentTime);

  Server server(*fs, timer, clock, network, entropy,
      Worker::LoggingOptions{}, [](kj::String) {});

  S3WorkerLruOptions opts;
  opts.maxWorkers = maxWorkers;
  opts.softMemoryLimit = SIZE_MAX;
  opts.hardMemoryLimit = SIZE_MAX;
  opts.staleTimeout = 30 * kj::SECONDS;
  auto lru = kj::heap<S3WorkerLru>(server, clock, opts);

  kj::Vector<kj::Own<S3RequestGuard>> borrowers;

  // Phase 1: Insert workers. Pin every 20th.
  for (uint i = 0; i < numWorkers; i++) {
    auto id = kj::str("t", threadId, "-m", i);

    while (lru->size() >= opts.maxWorkers) {
      if (!lru->evictIfNeeded()) {
        // All slots pinned — release all borrowers so we can proceed.
        borrowers.clear();
        KJ_ASSERT(lru->evictIfNeeded());
      }
    }
    lru->insert(id);

    if (i % 20 == 0) {
      borrowers.add(lru->acquireRequest(id));
    }
  }

  // Phase 2: Advance time past stale timeout and touch the last 5 workers.
  clock.advance(31 * kj::SECONDS);
  for (uint i = numWorkers - 5; i < numWorkers; i++) {
    auto id = kj::str("t", threadId, "-m", i);
    if (lru->contains(id)) {
      lru->touch(id);
    }
  }

  // Phase 3: Run stale eviction.
  lru->evictStale();

  // Phase 4: Release borrowers and verify LRU is consistent.
  borrowers.clear();
  KJ_ASSERT(lru->size() <= maxWorkers, threadId, lru->size());

  // The 5 touched workers should survive stale eviction.
  for (uint i = numWorkers - 5; i < numWorkers; i++) {
    auto id = kj::str("t", threadId, "-m", i);
    KJ_ASSERT(lru->contains(id), "touched worker should survive", threadId, i);
  }
}

KJ_TEST("S3WorkerLru: 4 threads mixed stress (active + stale), 250 workers each") {
  static constexpr uint NUM_THREADS = 4;
  static constexpr uint WORKERS_PER_THREAD = 250;
  static constexpr uint MAX_WORKERS = 40;

  kj::MutexGuarded<uint> completed(0);
  {
    auto threads = kj::heapArray<kj::Own<kj::Thread>>(NUM_THREADS);
    for (uint t = 0; t < NUM_THREADS; t++) {
      threads[t] = kj::heap<kj::Thread>([t, &completed]() {
        runThreadMixedStress(t, WORKERS_PER_THREAD, MAX_WORKERS);
        *completed.lockExclusive() += 1;
      });
    }
  }
  KJ_EXPECT(*completed.lockExclusive() == NUM_THREADS);
}

// Thread workload with markCreating/markCreated interleaved with eviction.
static void runThreadCreatingStress(uint threadId, uint numWorkers, uint maxWorkers) {
  kj::EventLoop loop;
  kj::WaitScope ws(loop);
  auto fs = kj::newDiskFilesystem();
  MockEntropy entropy;
  MockNetwork network;
  FakeClock clock;
  kj::TimerImpl timer(clock.currentTime);

  Server server(*fs, timer, clock, network, entropy,
      Worker::LoggingOptions{}, [](kj::String) {});

  S3WorkerLruOptions opts;
  opts.maxWorkers = maxWorkers;
  opts.softMemoryLimit = SIZE_MAX;
  opts.hardMemoryLimit = SIZE_MAX;
  auto lru = kj::heap<S3WorkerLru>(server, clock, opts);

  for (uint i = 0; i < numWorkers; i++) {
    auto id = kj::str("t", threadId, "-c", i);

    while (lru->size() >= opts.maxWorkers) {
      KJ_ASSERT(lru->evictIfNeeded());
    }

    // Simulate the createS3WorkerService async pattern:
    // 1. markCreating (placeholder)
    // 2. <async work would happen here>
    // 3. markCreated
    lru->markCreating(id);
    lru->markCreated(id, 4096);  // pretend 4KB heap

    KJ_ASSERT(lru->size() <= opts.maxWorkers);
    KJ_ASSERT(lru->contains(id));
  }

  KJ_ASSERT(lru->size() == maxWorkers, threadId, lru->size());
}

KJ_TEST("S3WorkerLru: 4 threads creating stress, 250 workers each") {
  static constexpr uint NUM_THREADS = 4;
  static constexpr uint WORKERS_PER_THREAD = 250;
  static constexpr uint MAX_WORKERS = 35;

  kj::MutexGuarded<uint> completed(0);
  {
    auto threads = kj::heapArray<kj::Own<kj::Thread>>(NUM_THREADS);
    for (uint t = 0; t < NUM_THREADS; t++) {
      threads[t] = kj::heap<kj::Thread>([t, &completed]() {
        runThreadCreatingStress(t, WORKERS_PER_THREAD, MAX_WORKERS);
        *completed.lockExclusive() += 1;
      });
    }
  }
  KJ_EXPECT(*completed.lockExclusive() == NUM_THREADS);
}

// ============================================================================
// Random-access stress tests — simulate realistic traffic patterns
// ============================================================================

// Simple deterministic PRNG (xorshift32) to avoid std::random dependencies.
static uint32_t xorshift32(uint32_t& state) {
  state ^= state << 13;
  state ^= state >> 17;
  state ^= state << 5;
  return state;
}

KJ_TEST("S3WorkerLru: random access 5000 requests across 1000 workers, maxWorkers=50") {
  // Simulates realistic traffic: each "request" randomly picks a worker ID from
  // a pool of 1000. If the worker is cached, touch it and acquire a request.
  // If not, evict if needed, insert it, then acquire. This exercises the full
  // load/unload/touch/evict cycle with unpredictable access patterns.
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(50);
  opts.staleTimeout = 3600 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  static constexpr uint32_t NUM_WORKERS = 1000;
  static constexpr uint32_t NUM_REQUESTS = 5000;

  uint32_t rng = 42;  // seed
  uint32_t loads = 0, hits = 0, evictions = 0;
  kj::Vector<kj::Own<S3RequestGuard>> activeRequests;

  for (uint32_t r = 0; r < NUM_REQUESTS; r++) {
    // Pick a random worker.
    auto workerId = kj::str("rnd-", xorshift32(rng) % NUM_WORKERS);

    // Release some active requests randomly (simulate request completion).
    if (activeRequests.size() > 0 && (xorshift32(rng) % 3) == 0) {
      // Release a random subset — pop last 1-3 entries.
      uint32_t toRelease = 1 + xorshift32(rng) % kj::min((uint32_t)3, (uint32_t)activeRequests.size());
      for (uint32_t j = 0; j < toRelease && activeRequests.size() > 0; j++) {
        activeRequests.removeLast();
      }
    }

    if (lru->contains(workerId)) {
      // Cache hit — touch and acquire.
      lru->touch(workerId);
      activeRequests.add(lru->acquireRequest(workerId));
      hits++;
    } else {
      // Cache miss — need to load.
      size_t sizeBefore = lru->size();
      while (lru->size() >= opts.maxWorkers) {
        if (!lru->evictIfNeeded()) {
          // All slots pinned — release everything and retry.
          activeRequests.clear();
          KJ_ASSERT(lru->evictIfNeeded());
        }
        evictions += sizeBefore - lru->size();
        sizeBefore = lru->size();
      }
      lru->insert(workerId);
      activeRequests.add(lru->acquireRequest(workerId));
      loads++;
    }

    KJ_ASSERT(lru->size() <= opts.maxWorkers);
  }

  KJ_LOG(INFO, "random access results", loads, "loads,", hits, "hits,", evictions, "evictions");
  KJ_EXPECT(loads > 0);
  KJ_EXPECT(hits > 0);
  KJ_EXPECT(evictions > 0);
  KJ_EXPECT(lru->size() <= opts.maxWorkers);
}

KJ_TEST("S3WorkerLru: random access with stale eviction, 5000 requests across 500 workers") {
  // Random access with stale timeout active. We use a large maxWorkers so
  // count-based eviction never fires — only stale eviction removes entries.
  // Clock advances between bursts to make entries stale, then they get re-loaded.
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(5000);  // large enough: no count eviction
  opts.staleTimeout = 30 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  static constexpr uint32_t NUM_WORKERS = 500;
  static constexpr uint32_t NUM_REQUESTS = 5000;

  uint32_t rng = 7;
  uint32_t loads = 0, hits = 0, staleEvictions = 0;

  for (uint32_t r = 0; r < NUM_REQUESTS; r++) {
    // Every 100 requests, advance clock by 15s and run stale eviction.
    // After 2 rounds without touch (30s), entries go stale.
    if (r % 100 == 0 && r > 0) {
      ctx.clock.advance(15 * kj::SECONDS);
      size_t before = lru->size();
      lru->evictStale();
      staleEvictions += before - lru->size();
    }

    auto workerId = kj::str("stale-rnd-", xorshift32(rng) % NUM_WORKERS);

    if (lru->contains(workerId)) {
      lru->touch(workerId);
      hits++;
    } else {
      lru->insert(workerId);
      loads++;
    }
  }

  KJ_LOG(INFO, "stale random access results", loads, "loads,", hits, "hits,",
      staleEvictions, "stale evictions");
  KJ_EXPECT(loads > 0);
  KJ_EXPECT(hits > 0);
  KJ_EXPECT(staleEvictions > 0);
}

KJ_TEST("S3WorkerLru: random access with active pinning, 3000 reqs across 500 workers, max=30") {
  // Stress: tiny cache (30), large worker pool (500), with random pinning.
  // Some workers get pinned with active requests, others flow through.
  // This tests the scenario where the LRU has to skip pinned entries repeatedly.
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(30);
  opts.staleTimeout = 3600 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  static constexpr uint32_t NUM_WORKERS = 500;
  static constexpr uint32_t NUM_REQUESTS = 3000;

  uint32_t rng = 123;
  uint32_t loads = 0, hits = 0;
  kj::Vector<kj::Own<S3RequestGuard>> pinned;

  for (uint32_t r = 0; r < NUM_REQUESTS; r++) {
    auto workerId = kj::str("pin-rnd-", xorshift32(rng) % NUM_WORKERS);

    // Randomly release some pinned requests (20% chance per iteration).
    if (pinned.size() > 0 && (xorshift32(rng) % 5) == 0) {
      uint32_t toRelease = 1 + xorshift32(rng) % kj::min((uint32_t)5, (uint32_t)pinned.size());
      for (uint32_t j = 0; j < toRelease && pinned.size() > 0; j++) {
        pinned.removeLast();
      }
    }

    if (lru->contains(workerId)) {
      lru->touch(workerId);
      // 30% chance to pin this worker.
      if (xorshift32(rng) % 3 == 0) {
        pinned.add(lru->acquireRequest(workerId));
      }
      hits++;
    } else {
      while (lru->size() >= opts.maxWorkers) {
        if (!lru->evictIfNeeded()) {
          // All pinned — release everything.
          pinned.clear();
          KJ_ASSERT(lru->evictIfNeeded());
        }
      }
      lru->insert(workerId);
      // 30% chance to pin the new worker.
      if (xorshift32(rng) % 3 == 0) {
        pinned.add(lru->acquireRequest(workerId));
      }
      loads++;
    }

    KJ_ASSERT(lru->size() <= opts.maxWorkers);
  }

  KJ_LOG(INFO, "pinned random access", loads, "loads,", hits, "hits, pinned=", pinned.size());
  KJ_EXPECT(loads > 0);
  KJ_EXPECT(hits > 0);
}

// Thread workload: random access with high churn.
static void runThreadRandomStress(uint threadId, uint32_t seed, uint numRequests,
                                  uint numWorkerIds, uint maxWorkers) {
  kj::EventLoop loop;
  kj::WaitScope ws(loop);
  auto fs = kj::newDiskFilesystem();
  MockEntropy entropy;
  MockNetwork network;
  FakeClock clock;
  kj::TimerImpl timer(clock.currentTime);

  Server server(*fs, timer, clock, network, entropy,
      Worker::LoggingOptions{}, [](kj::String) {});

  S3WorkerLruOptions opts;
  opts.maxWorkers = maxWorkers;
  opts.softMemoryLimit = SIZE_MAX;
  opts.hardMemoryLimit = SIZE_MAX;
  opts.staleTimeout = 3600 * kj::SECONDS;
  auto lru = kj::heap<S3WorkerLru>(server, clock, opts);

  uint32_t rng = seed;
  uint32_t loads = 0, hits = 0;
  kj::Vector<kj::Own<S3RequestGuard>> activeReqs;

  for (uint i = 0; i < numRequests; i++) {
    auto workerId = kj::str("t", threadId, "-r-", xorshift32(rng) % numWorkerIds);

    // Release some active requests randomly.
    if (activeReqs.size() > 0 && (xorshift32(rng) % 4) == 0) {
      uint32_t toRelease = 1 + xorshift32(rng) % kj::min((uint32_t)3, (uint32_t)activeReqs.size());
      for (uint32_t j = 0; j < toRelease && activeReqs.size() > 0; j++) {
        activeReqs.removeLast();
      }
    }

    if (lru->contains(workerId)) {
      lru->touch(workerId);
      activeReqs.add(lru->acquireRequest(workerId));
      hits++;
    } else {
      while (lru->size() >= opts.maxWorkers) {
        if (!lru->evictIfNeeded()) {
          activeReqs.clear();
          KJ_ASSERT(lru->evictIfNeeded());
        }
      }
      lru->insert(workerId);
      activeReqs.add(lru->acquireRequest(workerId));
      loads++;
    }
    KJ_ASSERT(lru->size() <= opts.maxWorkers);
  }
  KJ_ASSERT(lru->size() <= opts.maxWorkers);
}

KJ_TEST("S3WorkerLru: 4 threads random access, 2500 reqs each, 1000 worker IDs, max=40") {
  static constexpr uint NUM_THREADS = 4;
  static constexpr uint REQUESTS_PER_THREAD = 2500;
  static constexpr uint WORKER_POOL = 1000;
  static constexpr uint MAX_WORKERS = 40;

  kj::MutexGuarded<uint> completed(0);
  {
    auto threads = kj::heapArray<kj::Own<kj::Thread>>(NUM_THREADS);
    for (uint t = 0; t < NUM_THREADS; t++) {
      threads[t] = kj::heap<kj::Thread>([t, &completed]() {
        runThreadRandomStress(t, 42 + t * 7, REQUESTS_PER_THREAD, WORKER_POOL, MAX_WORKERS);
        *completed.lockExclusive() += 1;
      });
    }
  }
  KJ_EXPECT(*completed.lockExclusive() == NUM_THREADS);
}

KJ_TEST("S3WorkerLru: 4 threads random access, 5000 reqs each, 2000 worker IDs, max=25") {
  // Extreme churn: large worker pool, tiny cache. Lots of evictions.
  static constexpr uint NUM_THREADS = 4;
  static constexpr uint REQUESTS_PER_THREAD = 5000;
  static constexpr uint WORKER_POOL = 2000;
  static constexpr uint MAX_WORKERS = 25;

  kj::MutexGuarded<uint> completed(0);
  {
    auto threads = kj::heapArray<kj::Own<kj::Thread>>(NUM_THREADS);
    for (uint t = 0; t < NUM_THREADS; t++) {
      threads[t] = kj::heap<kj::Thread>([t, &completed]() {
        runThreadRandomStress(t, 99 + t * 13, REQUESTS_PER_THREAD, WORKER_POOL, MAX_WORKERS);
        *completed.lockExclusive() += 1;
      });
    }
  }
  KJ_EXPECT(*completed.lockExclusive() == NUM_THREADS);
}

// ============================================================================
// Latency benchmarks — measure per-operation cost in the hot path
// ============================================================================

KJ_TEST("S3WorkerLru: benchmark getProcessRssBytes latency") {
  // Measure the cost of the syscall that's in the hot path.
  static constexpr int ITERATIONS = 10000;
  auto start = std::chrono::steady_clock::now();
  size_t rss = 0;
  for (int i = 0; i < ITERATIONS; i++) {
    rss += S3WorkerLru::getProcessRssBytes();
  }
  auto end = std::chrono::steady_clock::now();
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  auto perCallNs = ns / ITERATIONS;
  KJ_LOG(INFO, "getProcessRssBytes", ITERATIONS, "calls in",
      ns / 1000000, "ms →", perCallNs, "ns/call", rss);
  // Should be well under 100μs per call.
  KJ_EXPECT(perCallNs < 100000, "getProcessRssBytes too slow", perCallNs);
}

KJ_TEST("S3WorkerLru: benchmark evictIfNeeded happy path (under limit)") {
  // Measure evictIfNeeded when NOT at capacity — this is the common case.
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(1000);
  opts.staleTimeout = 3600 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  // Fill to 200 workers (well under 1000 limit).
  for (int i = 0; i < 200; i++) {
    lru->insert(kj::str("w-", i));
  }

  static constexpr int ITERATIONS = 10000;
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < ITERATIONS; i++) {
    lru->evictIfNeeded();
  }
  auto end = std::chrono::steady_clock::now();
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  auto perCallNs = ns / ITERATIONS;
  KJ_LOG(INFO, "evictIfNeeded (under limit, 200 entries)", ITERATIONS, "calls in",
      ns / 1000000, "ms →", perCallNs, "ns/call");
  // Must be under 1ms per call for millisecond-level latency.
  KJ_EXPECT(perCallNs < 1000000, "evictIfNeeded too slow", perCallNs);
}

KJ_TEST("S3WorkerLru: benchmark touch latency") {
  LruTestContext ctx;
  auto lru = ctx.makeLru(LruTestContext::noMemoryPressureOpts(10000));

  for (int i = 0; i < 1000; i++) {
    lru->insert(kj::str("w-", i));
  }

  static constexpr int ITERATIONS = 100000;
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < ITERATIONS; i++) {
    lru->touch(kj::str("w-", i % 1000));
  }
  auto end = std::chrono::steady_clock::now();
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  auto perCallNs = ns / ITERATIONS;
  KJ_LOG(INFO, "touch (1000 entries)", ITERATIONS, "calls in",
      ns / 1000000, "ms →", perCallNs, "ns/call");
  KJ_EXPECT(perCallNs < 100000, "touch too slow", perCallNs);
}

KJ_TEST("S3WorkerLru: benchmark single eviction latency") {
  // Measure the cost of a single evict (the actual service teardown is a
  // no-op in tests, so this measures LRU bookkeeping only).
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(200);
  opts.staleTimeout = 3600 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  static constexpr int ITERATIONS = 10000;
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < ITERATIONS; i++) {
    // Fill to capacity, then evict one.
    while (lru->size() < opts.maxWorkers) {
      lru->insert(kj::str("bench-", i, "-", lru->size()));
    }
    lru->evictIfNeeded();  // evicts one
  }
  auto end = std::chrono::steady_clock::now();
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  auto perCallNs = ns / ITERATIONS;
  KJ_LOG(INFO, "single eviction (200 capacity)", ITERATIONS, "cycles in",
      ns / 1000000, "ms →", perCallNs, "ns/cycle");
  KJ_EXPECT(perCallNs < 1000000, "eviction too slow", perCallNs);
}

KJ_TEST("S3WorkerLru: benchmark evictStale with 200 entries none stale") {
  // This is the worst case for the stale scan: many entries, none stale.
  LruTestContext ctx;
  auto opts = LruTestContext::noMemoryPressureOpts(10000);
  opts.staleTimeout = 3600 * kj::SECONDS;
  auto lru = ctx.makeLru(opts);

  for (int i = 0; i < 200; i++) {
    lru->insert(kj::str("w-", i));
  }

  static constexpr int ITERATIONS = 10000;
  auto start = std::chrono::steady_clock::now();
  for (int i = 0; i < ITERATIONS; i++) {
    lru->evictStale();
  }
  auto end = std::chrono::steady_clock::now();
  auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(end - start).count();
  auto perCallNs = ns / ITERATIONS;
  KJ_LOG(INFO, "evictStale (200 entries, none stale)", ITERATIONS, "calls in",
      ns / 1000000, "ms →", perCallNs, "ns/call");
  KJ_EXPECT(perCallNs < 1000000, "evictStale scan too slow", perCallNs);
}

}  // namespace
}  // namespace workerd::server
