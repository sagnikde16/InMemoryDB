# High-Throughput C++ In-Memory Key-Value Store

**Architect & Lead Developer:** Debanshu Ghosh

## Description

This project is a high-performance, in-memory key-value database built in C++17 designed to handle highly concurrent read/write workloads with deterministic, low-latency performance. By moving away from standard monolithic database locks and standard heap allocators, this architecture shifts the performance bottleneck away from kernel-space contention down to CPU cache locality and hardware-level isolation.

The engine achieves this by marrying a non-blocking network reactor architecture with a **Sharded Memory Engine** backed by C++17 **Polymorphic Memory Resources (PMR)**. Each database shard manages its own pre-allocated memory arena, eliminating both database lock contention and global operating system allocator (`malloc`/`new`) lock contention during execution.

---

## Features

* **Non-Blocking Network Layer:** High-efficiency Winsock2 event demultiplexing capable of managing thousands of concurrent TCP client connections on a single network thread.
* **Fine-Grained Sharded Engine:** Key space is divided into $N$ separate buckets (where $N$ is a power of two), each guarded by an independent `std::shared_mutex` to allow concurrent multi-threaded reads and parallelized, localized writes.
* **Polymorphic Cache-Aligned Storage:** Support for core data structures (Strings, Integers, Hashes, and Lists) packed inside `std::variant` structures explicitly aligned to CPU cache lines (`alignas(64)`).
* **Dual-Buffered Group Commit AOF:** Highly durable Append-Only File (AOF) system that stages transactions to a lock-free queue, allowing a background thread to batch operations and flush to disk every second (`AOF_FSYNC_EVERYSEC`).
* **Isolated Ephemeral Messaging:** A standalone Pub/Sub message broker built via the Observer Pattern that bypasses the core memory shards entirely to broadcast messages directly to active socket connections.

---

## Architectural Decisions & Memory Protection Model

### The Dual-Layer Memory Protection System

To prevent the application from experiencing unpredictable tail-latency spikes or crashing due to Out-of-Memory (OOM) conditions, the engine enforces a user-defined hard memory limit (e.g., 512MB or 1GB) configured at server initialization. This limit is divided evenly across all shards and enforced via a **Two-Layer Protection Subsystem**:

```text
[ User Configuration: Max Memory Limit (e.g., 1GB) ]
                       │
                       ▼
[ Split Evenly Across Shards: Max Shard Capacity (e.g., 16MB/Shard) ]
                       │
       ┌───────────────┴───────────────┐
       ▼                               ▼
 [ Layer 1: Proactive Eviction ] [ Layer 2: Hard Saturation Block ]
  - Triggers at 85% capacity.     - Triggers at 100% capacity.
  - Active timing wheel purges    - Immediately rejects writes.
    expired/cold data.            - Returns Redis-style native OOM error.

```

#### Layer 1: Proactive Eviction (The Strategy Pattern)

Each shard monitors its active memory footprint within its local PMR pool. When memory consumption crosses a configurable high-water mark (e.g., 85% of the shard's capacity), the engine triggers an internal `IEvictionStrategy` (such as LRU or Hierarchical Timing Wheel TTL purging). Cold or expired keys are dropped immediately, recycling memory blocks back to the shard's local pool before a bottleneck occurs.

#### Layer 2: Hard Saturation Block (The Allocator Boundary)

If influx outpaces eviction and a shard reaches 100% of its allocation ceiling, the database prevents the underlying PMR allocator from making a fallback call to the operating system's global heap. The shard enters a write-protected state. Any incoming write command targeting that shard is instantly intercepted, aborted, and returns a native Redis error string to the client: `(error) OOM command not allowed when used memory > 'maxmemory'`.

### Memory Layout: PMR Arena Isolation

Rather than utilizing standard C++ containers that hit global allocator locks, the database isolates allocations per shard using C++17's `<memory_resource>` primitives. Each shard encapsulates a `std::pmr::unsynchronized_pool_resource` assigned to a fixed, pre-allocated memory chunk carved out during startup.

Because the memory pool is wrapped inside the shard, and the shard is already guarded by its own thread-safe mutex, utilizing the **unsynchronized** pool resource strips away redundant, internal allocator-level synchronization overhead, keeping operations fully optimized.

---

## Design Patterns Used

### 1. Structural Patterns

* **Chain of Responsibility Pattern:** Structures the request-handling pipeline sequentially. Raw bytes from a socket pass through a series of decoupled middleware handlers: `ConnectionLimiter` (IP-level throttling) $\rightarrow$ `AuthInterceptor` (Credential confirmation) $\rightarrow$ `RespParser` (Token execution mapping).

### 2. Behavioral Patterns

* **Reactor Pattern:** Implemented via a single, dedicated network loop that multiplexes incoming Winsock TCP readiness events and offloads parsed packets to the worker engine.
* **Command Pattern:** Encapsulates database transactions (`GET`, `SET`, `DEL`). Incoming RESP byte blocks are parsed into standalone, abstract `Command` tokens, completely isolating the network parser from data modification logic.
* **Observer Pattern:** Powers the Pub/Sub messaging engine. Sockets register as observers to isolated channels, routing real-time traffic cleanly around the main memory store.
* **State Pattern:** Governs the lifetime of active client sockets (`AuthenticatingState`, `StandardState`, `SubscribedState`), strictly controlling which API endpoints are legally available to a socket at any given microsecond.

### 3. Creational Patterns

* **Builder Pattern:** Simplifies instantiation of the database instance by chaining complex configurations at startup.
```cpp
KVServer server = ServerBuilder()
                      .setPort(6379)
                      .setWorkerThreads(8)
                      .setMaxMemoryLimit("1GB") // Set pool ceiling
                      .setShardCount(64)       // Enforce power-of-two masking
                      .build();

```


* **Variant Factory:** Evaluates commands and constructs cache-friendly, contiguous data allocations inline via `std::variant` rather than passing heap-allocated polymorphic base pointers.
* **Object Pool Pattern:** Maintains pre-allocated reusable byte buffers for network I/O, ensuring zero runtime dynamic allocations on the critical network path.

---

## Technical Specifications & Micro-Optimizations

* **Bitwise Shard Masking:** To eliminate expensive modulo calculation overheads (`Index = Hash % N`), the engine enforces that the shard count $N$ must be a strict power of two. This replaces division with a fast bitwise AND operation (`Index = Hash & (N - 1)`) on every single key query.
* **Deterministic Concurrency Boundaries:** The task transport lines are strictly optimized to eliminate deadlocks and race conditions:
* **Network $\rightarrow$ Workers:** Managed by a lock-free Multi-Producer Multi-Consumer (MPMC) or Single-Producer Multi-Consumer (SPMC) task queue.
* **Workers $\rightarrow$ AOF Disk Thread:** Powered by a fast, lock-free Multi-Producer Single-Consumer (MPSC) ring buffer routing log frames down to the group commit flusher.


* **TCP Read Suspension (Backpressure Model):** To prevent OOM crashes under extreme load, the Network-to-Worker task queue has a strict capacity limit (e.g., 65,536 tasks). If this limit is hit, the Network Reactor temporarily stops polling `POLLRDNORM` for active sockets. This forces the OS TCP stack buffers to fill up, applying natural backpressure to clients without allocating unmanaged memory. Polling resumes once the queue depth drops below a safe threshold.
* **Controlled Open Addressing with Re-Sharding:** Internal shard maps utilize an optimized open-addressing flat hash map design (similar to `absl::flat_hash_map` backed by PMR) to maximize L1/L2 cache line hits. The engine enforces a strict $0.7$ load factor. When exceeded, re-sharding occurs locally within that specific shard's exclusive lock boundary, leaving the other $N-1$ shards completely unaffected and online.
* **Hierarchical Timing Wheel:** To achieve $O(1)$ TTL eviction without wasting CPU cycles scanning cold data, keys with expiration times are registered into a timing wheel bucket corresponding to their expiration second. A background daemon advances a pointer tick-by-tick, instantly executing cleanups on expired buckets.
* **Cache Alignment:** Internal nodes and bucket headers are forced onto hardware-level boundaries via `alignas(64)` expressions, ensuring that concurrent operations executing on adjacent memory shards never trigger CPU false-sharing cache invalidations.

---

## Conclusion

This C++ key-value store demonstrates how traditional networking concepts can be scaled up to achieve maximum performance by tightly managing memory ownership and hardware boundaries. By substituting global locks with fine-grained sharding, utilizing C++17 polymorphic memory structures, and implementing a strict dual-layer memory protection system, the engine operates completely decoupled from common OS allocation bottlenecks. The resulting architecture remains predictable, modular, and designed to fully saturate modern multi-core server hardware.