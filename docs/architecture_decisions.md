# Architectural Decisions

**Project:** InMemoryDB — High-Throughput C++ Key-Value Store  
**Author:** Debanshu Ghosh

> This document provides an in-depth analysis of the core architectural decisions that drive the engine's performance, durability, and scalability. Each decision is examined from the problem it solves, the approach taken, and the concrete code mechanism that implements it.

---

## Table of Contents

1. [Sharded Lock Striping](#1-sharded-lock-striping)
2. [Lazy Cancellation via Tombstoning](#2-lazy-cancellation-via-tombstoning)
3. [Fast-Fail Network Pipeline](#3-fast-fail-network-pipeline)
4. [Dual-Buffer AOF with Circuit Breaker](#4-dual-buffer-aof-with-circuit-breaker)
5. [Polymorphic Memory Resources](#5-polymorphic-memory-resources-stdpmr)
6. [WSAPoll Reactor with Completion Queue](#6-wsapoll-reactor-with-completion-queue)
7. [Sequence-Stamped Pipeline Reassembly](#7-sequence-stamped-pipeline-reassembly)
8. [TCP Backpressure via POLLRDNORM Masking](#8-tcp-backpressure-via-pollrdnorm-masking)

---

## 1. Sharded Lock Striping

### Problem
A naive key-value store uses a single global `std::mutex` over its entire hash map. Under concurrent load, every read and every write blocks all other threads — this is O(1) reads/writes in isolation but degrades to serial execution under contention. With a thread pool and hundreds of concurrent clients, a single mutex becomes the primary bottleneck.

### Approach
The key space is partitioned into **64 independent shards** using a fast, non-cryptographic hash function (MurmurHash3 with 32-bit output). The shard index is computed via bitwise masking (`hash & (N-1)`) which requires N to be a power of two — hence the fixed shard count of 64.

### Implementation

**ShardRouter** (`include/engine/hasher.h`) hashes the key and routes to the correct shard:
```cpp
Shard& shard_for_key(const string& key) {
    size_t idx = murmur_hash3(key) & shard_mask_; // shard_mask_ = 63
    return *shards_[idx];
}
```

Each **Shard** (`include/engine/shard.h`) owns:
- Its own `std::pmr::unordered_map` (data store)
- Its own `PmrArena` (memory arena)
- Its own `std::shared_mutex` (read-write lock)

This means concurrent `GET` operations on keys from different shards run truly in parallel with zero contention. A `SET` on shard 12 and a `GET` on shard 47 are fully independent.

**RateLimiter** (`include/network/rate_limiter.h`) applies the same pattern at the network layer:
```cpp
static constexpr size_t NUM_SHARDS = 64;
// ...
struct RateLimitShard {
    std::unordered_map<std::string, TokenBucket> buckets;
    std::shared_mutex mutex;
};
std::array<RateLimitShard, NUM_SHARDS> shards_;
```

An IP lookup hashes to one of 64 independent sub-maps. Two clients with IPs that hash to different shards perform their rate-limit checks concurrently with zero synchronization overhead.

### Trade-off
- **Pro:** Scales linearly with CPU cores on read-heavy workloads.
- **Con:** Cross-shard transactions (e.g., atomic RENAME) require locking two shards and must be done in a canonical order to prevent deadlock. The current engine does not support cross-shard atomics.

---

## 2. Lazy Cancellation via Tombstoning

### Problem
The `TimingWheel` daemon uses a hierarchical bucket array of 86,400 slots (one per second for 24 hours) to schedule key evictions. When a client issues a `DEL key` command, the naive solution is to scan the entire wheel to find and remove the scheduled entry — an O(N) operation where N = 86,400.

At 28,000 RPS with a mix of `DEL` commands, this would stall the worker threads for an unacceptable amount of time. Worse, the `TimingWheel` is protected by its own mutex, creating a cross-subsystem lock dependency between workers and the sweeper daemon.

### Approach
**Lazy cancellation** introduces a `tombstoned` boolean flag directly on the `CacheAlignedValue` struct. Rather than modifying the timing wheel at all, `DEL` simply marks the value and immediately returns. The timing wheel sweeper is responsible for physical cleanup.

### Implementation

**CacheAlignedValue** (`include/memory/variant_type.h`):
```cpp
struct alignas(64) CacheAlignedValue {
    ValueVariant data;
    std::chrono::steady_clock::time_point created_at;
    std::chrono::steady_clock::time_point last_accessed;
    int64_t ttl_seconds;
    bool tombstoned = false; // lazy deletion flag
};
```

**Shard::del()** (`src/engine/shard.cpp`):
```cpp
bool Shard::del(const std::string& key) {
    std::unique_lock lock(mutex_);
    auto it = data_.find(lookup_key);
    if (it == data_.end() || it->second.tombstoned) return false;
    it->second.tombstoned = true; // O(1) — no wheel interaction
    return true;
}
```

**TimingWheel::advance_tick()** calls `evict_expired()` which only erases if the entry is tombstoned or TTL-expired:
```cpp
bool Shard::evict_expired(const std::string& key) {
    // Called with exclusive lock held by the sweeper
    if (it->second.tombstoned || it->second.is_expired()) {
        data_.erase(it);
        return true;
    }
    return false; // re-SET key survives the sweep
}
```

### Correctness Guarantee
A key that was `DEL`'d and then immediately `SET` again is **not** incorrectly erased by the sweeper because `evict_expired` checks `tombstoned` — and a fresh `SET` resets the flag to `false`. This prevents the timing wheel from accidentally evicting live data.

### Trade-off
- **Pro:** `DEL` is a constant-time operation with no cross-subsystem locking.
- **Con:** Tombstoned entries consume arena memory until the sweeper tick. In heavy `DEL` workloads, there is a brief window of elevated memory pressure. The monotonic buffer resource does not reclaim this memory at the PMR level — it accumulates until the shard is destroyed.

---

## 3. Fast-Fail Network Pipeline

### Problem
CPU-intensive operations like string tokenization and command parsing are non-trivial under extreme load. A volumetric attack (or misconfigured client) that floods the server with thousands of requests per second can saturate the CPU in parsing work before any database logic runs.

### Approach
The `Pipeline::process()` method evaluates guards in **cheapest-to-most-expensive** order:

```
Rate Limit Check (hash lookup, O(1))
    ↓ PASS
Whitespace Tokenization (O(len))
    ↓ PASS
CommandFactory::parse() (string compare, O(cmd_len))
    ↓ PASS
WorkerPool::submit() (queue push, O(1))
```

Any step that fails immediately posts an error `TaskResult` to the `CompletionQueue` and returns. The worker pool is never touched, and no memory is allocated on the PMR arena for rejected requests.

### Implementation

**Pipeline::process()** (`src/network/pipeline.cpp`):
```cpp
void Pipeline::process(SOCKET client, const std::string& line,
                       uint64_t seq_id, const std::string& client_ip) {
    // Step 1: Rate limit (O(1) hash lookup)
    if (!limiter_.allow(client_ip)) {
        pool_.completion_queue().push(
            {client, seq_id, "-ERR rate limit exceeded\r\n"});
        return; // Short-circuit — no parsing
    }

    // Step 2: Tokenize (only if allowed)
    std::vector<std::string> tokens;
    // ...

    // Step 3: Parse command
    auto command = CommandFactory::parse(tokens);
    if (!command) {
        pool_.completion_queue().push(
            {client, seq_id, "-ERR unknown command\r\n"});
        return;
    }

    // Step 4: Submit to worker pool
    if (!pool_.submit(std::move(task))) {
        pool_.completion_queue().push(
            {client, seq_id, "-ERR server busy\r\n"});
    }
}
```

The `RateLimiter` allows 1,000 tokens per IP per 60 seconds, with a token bucket that refills proportionally to elapsed time. At 28,000 RPS with localhost testing, temporarily disabling the rate limiter is required to measure raw engine throughput.

---

## 4. Dual-Buffer AOF with Circuit Breaker

### Problem
A naive "write every command to disk" approach introduces disk I/O latency into the critical path. On the other hand, skipping persistence entirely risks data loss on crash. During server restart, replaying the entire AOF file would re-execute every command — but without a guard, the commands would also be re-logged to the AOF, doubling every entry.

### Approach: Dual Buffer Swap
The `AofLogger` uses two `std::vector<std::string>` buffers — `buffer_a_` and `buffer_b_`:
- Commands are `log()`'d into the **active** buffer (lock-guarded, fast)
- A background `flush_thread_` wakes every second, **swaps** the active and flush buffers, then writes the flush buffer to disk
- While disk I/O happens, new commands continue accumulating in the now-active other buffer — zero blocking on the fast path

### Approach: Replay Circuit Breaker
The `is_replaying_` atomic flag short-circuits the AOF `log()` call inside each command's `execute()` method:

```cpp
// In SetCommand::execute():
if (router.set(key_, value_, ttl_)) {
    if (aof && !aof->is_replaying()) // Circuit breaker
        aof->log(serialize());
    return "+OK\r\n";
}
```

**ServerBuilder::build_and_run()** orchestrates the startup sequence:
```cpp
// Phase 1: Replay historical state
{
    std::ifstream aof_file(aof_path_);
    if (aof_file.is_open()) {
        aof.set_replaying(true); // Open the circuit
        // ... parse and execute every line
        aof.set_replaying(false); // Close the circuit
    }
}

// Phase 2: Start background logger (only AFTER replay)
aof.start();
```

This guarantees that replayed commands populate the in-memory shards without touching the disk file, and that new commands are only logged after the daemon is running.

### Trade-off
- **Pro:** At most 1 second of data loss (fsync-per-second). Zero write amplification during replay.
- **Con:** If the process is killed in the window between a `log()` call and the flush thread's next swap, up to 1 second of writes may be lost. This is an explicit design trade-off for throughput over strict durability.

---

## 5. Polymorphic Memory Resources (`std::pmr`)

### Problem
The C++ default allocator (`new`/`delete`, which calls `malloc`/`free`) is a global resource protected by a mutex. Under heavy concurrent allocation from multiple threads, this becomes a serialization point. Additionally, allocations are scattered across the heap, defeating CPU cache locality.

### Approach
Each shard owns a **pre-allocated PMR arena** — a `PmrArena` object that wraps a chain of `std::pmr::memory_resource` objects:

```
Raw byte buffer (vector<byte>)
    ↓ fed to
monotonic_buffer_resource (fast, append-only sub-allocation)
    ↓ wrapped by
unsynchronized_pool_resource (recycles freed blocks per-size)
    ↓ wrapped by
TrackingMemoryResource (counts bytes, enforces soft/hard limits)
```

All PMR containers within a shard (`pmr::unordered_map`, `pmr::string`, `pmr::vector`) allocate from this chain — never calling `malloc`.

### Cache Alignment
The `CacheAlignedValue` struct is declared with `alignas(64)`:
```cpp
struct alignas(64) CacheAlignedValue {
    ValueVariant data;         // The actual value
    int64_t ttl_seconds;
    bool tombstoned = false;
    // ...
};
```

A standard x86-64 CPU cache line is 64 bytes. This alignment guarantees each value node occupies a complete cache line, preventing **false sharing** — a scenario where two CPU cores writing to adjacent (but logically independent) data cause each other's caches to invalidate repeatedly.

### Memory Limits
`TrackingMemoryResource` enforces two thresholds:
- **Soft limit (85%):** The `TimingWheel` is triggered for proactive eviction.
- **Hard limit (100%):** `do_allocate()` throws `OomException`, which command executors catch and convert to `-ERR OOM ...` RESP responses without crashing.

---

## 6. WSAPoll Reactor with Completion Queue

### Problem
A traditional blocking server uses one thread per connection. At 10,000 concurrent connections, this requires 10,000 threads — each consuming ~1MB of stack space (10GB total), with the OS spending significant time context-switching between them.

### Approach
A single **Reactor** thread manages all connections via `WSAPoll` (the Windows analog of POSIX `poll`). It maintains a `vector<WSAPOLLFD>` poll set, waits for readiness events, and dispatches work to the `WorkerPool` without blocking.

### Thread Boundary Safety
Workers must **never** touch `ConnectionState` objects — they live on the Reactor's thread. This was enforced by replacing callback-based task completion with a typed `CompletionQueue`:

```cpp
struct TaskResult {
    SOCKET client_fd;
    uint64_t sequence_id;
    std::string response;
};
```

Workers post `TaskResult` values. The Reactor drains this queue at the top of each event loop tick and appends responses to the appropriate connection's write buffer. No shared state is ever touched across the thread boundary.

---

## 7. Sequence-Stamped Pipeline Reassembly

### Problem
Redis-style pipelining allows clients to send multiple commands without waiting for each response. The Reactor parses these as independent tasks submitted to the SPMC worker pool. A fast `PING` command submitted after a slow `LPUSH` may complete first, causing responses to arrive out of order — breaking RESP pipeline semantics.

### Approach
Each parsed command is assigned a monotonically increasing `sequence_id` per connection. Workers include this ID in their `TaskResult`. The Reactor maintains a `std::map<uint64_t, std::string>` reassembly buffer per connection and only flushes contiguous runs to the write buffer:

```cpp
// In ConnectionState:
uint64_t next_seq_issue    = 0;   // Stamped on each parsed command
uint64_t next_seq_expected = 0;   // Next response to flush
std::map<uint64_t, std::string> out_of_order_buf;

// In drain_completion_queue():
state.out_of_order_buf[r.sequence_id] = std::move(r.response);
while (state.out_of_order_buf.count(state.next_seq_expected)) {
    state.write_buffer += state.out_of_order_buf[state.next_seq_expected];
    state.out_of_order_buf.erase(state.next_seq_expected);
    state.next_seq_expected++;
}
```

---

## 8. TCP Backpressure via POLLRDNORM Masking

### Problem
If the worker pool becomes saturated, blindly accepting more data from the TCP stream grows the in-memory queue unboundedly, eventually causing `OOM` crashes or request loss.

### Approach
The Reactor tracks overloaded connections in a `paused_reads_` set. When the queue exceeds **90% capacity**, new socket reads are paused. In `rebuild_poll_set()`, paused sockets do not register `POLLRDNORM`:

```cpp
if (paused_reads_.find(sock) == paused_reads_.end())
    pfd.events |= POLLRDNORM; // Only read-ready if NOT paused
```

Without `POLLRDNORM`, the OS does not deliver read events for that socket. Data accumulates in the kernel's TCP receive buffer, which eventually fills up and shrinks the TCP receive window advertised to the client — signaling the client to slow its send rate. This is true **kernel-level TCP backpressure** with no data loss.

Backpressure is released when the queue drops below **70% capacity**, providing hysteresis to prevent rapid oscillation.
