# Implementation Plan: In-Memory KV Store Stabilization

**Context for implementation:** This document outlines 8 critical architectural fixes for a C++ PMR-based in-memory sharded key-value store. The engine uses a Winsock2 `WSAPoll` reactor, an SPMC worker pool, and a custom timing wheel.

Please implement these fixes serially. Do not deviate from the architectural constraints outlined below, specifically regarding thread boundaries and lock management.

---

### B1: Missing Commands for Lists and Hashes

**The Issue:** The `std::variant` memory primitive supports `pmr::vector` and `pmr::unordered_map`, but the command pipeline lacks the hooks to access them.
**The Fix:** Factory Pattern Expansion with Read/Write Locking.
**Implementation Steps:**

1. Define new command interfaces: `LPushCommand`, `LRangeCommand`, `HSetCommand`, and `HGetCommand` inheriting from `ICommand`.
2. Update `CommandFactory::create()` to parse these RESP strings and route them to the new classes.
3. Inside the `execute()` methods, apply strict lock grading: acquire `std::shared_lock` for read-only operations (`HGET`, `LRANGE`) and `std::unique_lock` for mutations (`LPUSH`, `HSET`).
4. Use `std::get_if` to validate the variant type. If a client queries a string key with a hash command, return a `-WRONGTYPE` RESP error.

### B2: Thread Safety Between Worker Pool and Reactor

**The Issue:** Worker threads write directly to connection buffers while the Reactor may concurrently drop those connections, causing use-after-free memory corruption.
**The Fix:** Global Lock-Free Completion Queue (Single Source of Truth).
**Implementation Steps:**

1. **Constraint:** Worker threads must *never* touch the `Connection` object.
2. Define a `TaskResult` struct containing `fd` (SOCKET), `sequence_id` (uint64_t), and `response` (std::string).
3. Implement a lock-free concurrent queue (e.g., `moodycamel::ConcurrentQueue` or a mutex-guarded `std::queue`) owned by the Reactor.
4. Workers post the `TaskResult` to this completion queue upon finishing command execution.
5. At the start of every `WSAPoll` tick, the Reactor thread drains this queue, verifies the `fd` still exists in its connection map, and moves the response into the connection's output buffer.

### B3: Out-of-Order Pipelined Responses

**The Issue:** Due to SPMC thread race conditions, fast lightweight commands finish before earlier heavy commands, scrambling the pipelined RESP sequence.
**The Fix:** Sequence Stamping and `std::map` Reassembly.
**Implementation Steps:**

1. Add two counters to the `Connection` state: `next_seq_issue` and `next_seq_expected`.
2. Add `std::map<uint64_t, std::string> out_of_order_buf` to the `Connection` state.
3. In the read handler, stamp each parsed command with `next_seq_issue++` before enqueuing to the worker pool.
4. During the Reactor's completion queue drain (from B2), insert the worker's response into `out_of_order_buf` using `sequence_id` as the key.
5. Check if `next_seq_expected` exists in the map. If so, iterate and flush contiguous sequential responses to the final socket write buffer, incrementing `next_seq_expected` and erasing the flushed keys.

### B4: Dropped Reads on Full Worker Pool

**The Issue:** Pushing into a full worker queue fails silently or drops the connection, breaking TCP flow control.
**The Fix:** Dynamic TCP Backpressure via `POLLRDNORM` Manipulation.
**Implementation Steps:**

1. Maintain a `std::unordered_set<SOCKET> paused_reads_` in the Reactor.
2. In `handle_read()`, if the worker pool queue is > 90% full, abort parsing, insert the `fd` into `paused_reads_`, and return without re-arming read events.
3. In the `WSAPoll` setup loop, bitwise AND NOT (`&= ~POLLRDNORM`) the polling flags for any `fd` in the paused set. This forces the OS TCP receive buffer to fill, naturally shrinking the TCP window to 0.
4. In the completion drain loop, if the worker pool size drops below 70%, clear the `paused_reads_` set so regular polling resumes.

### B5: Connection and Pipeline Limiters

**The Issue:** Vulnerability to file descriptor exhaustion and unbounded in-flight requests per connection.
**The Fix:** Chain-of-Responsibility Gatekeeping with Sliding Pipeline Caps.
**Implementation Steps:**

1. Implement a `ConnectionLimiter`. Maintain an atomic integer tracking total active connections. Reject new `accept()` calls with an immediate error and close if the threshold is met.
2. Add an `in_flight_requests` counter to the `Connection` state.
3. Increment this counter when parsing a request; decrement it when flushing a response in the Reactor.
4. If a connection hits `MAX_PIPELINE_DEPTH` (e.g., 256), pause further reads on that specific socket until responses drain. **Do not terminate the connection** (preserves long-lived clients).

### B6: IP-Based Rate Limiting

**The Issue:** Volumetric abuse from a single IP address with no rate constraints.
**The Fix:** Sharded Token Bucket Algorithm.
**Implementation Steps:**

1. Implement a `RateLimiter` class. To avoid global lock contention, use lock striping: create an array of 64 `RateLimitShard` structs, each holding its own `std::unordered_map` and `std::shared_mutex`.
2. Hash the incoming IP address (`hash % 64`) to route to a specific shard.
3. Implement a standard token bucket (e.g., 1000 requests / 60 seconds).
4. If tokens < 1, intercept the request in the Pipeline, bypass the worker pool entirely, and queue a `-ERR rate limit exceeded` response.

### B7: Disconnected AOF Logic and Double-Logging Bug

**The Issue:** The dual-buffer AOF logger is not writing mutations, and blind replay on restart will duplicate the AOF file.
**The Fix:** Write-Through Logging with Startup Bypass Flag.
**Implementation Steps:**

1. Add `std::atomic<bool> is_replaying_{false};` to the `AofLogger`.
2. In `ServerBuilder`, before starting the Reactor, set `is_replaying_ = true`, open the AOF file, parse it line-by-line, and route commands directly to the engine. Once done, set `is_replaying_ = false`.
3. Inject the `AofLogger` into all mutation commands (`SET`, `DEL`, `HSET`, etc.).
4. Inside `execute()` of mutation commands, immediately after updating memory, check `if (!logger.is_replaying_)` and write the raw RESP string to the AOF active buffer.
5. In the `TimingWheel` background thread, explicitly log `DEL` commands to the AOF when a key natively expires.

### B8: O(N) Timing Wheel Deletions

**The Issue:** Explicit user `DEL` commands trigger a linear scan across 86,400 timing wheel buckets to find the scheduled eviction node.
**The Fix:** Lazy Cancellation via Tombstoning.
**Implementation Steps:**

1. Add a `bool tombstoned = false;` flag to the core `CacheAlignedValue` struct.
2. Modify the `DEL` command: lock the shard, find the key, set `tombstoned = true`, and return success. **Do not touch the TimingWheel.**
3. Modify all read commands (`GET`, `HGETALL`, etc.) to treat `tombstoned == true` as a missing key (`$-1\r\n`).
4. Modify `TimingWheel::sweep()`: when evaluating a bucket, check the key in the shard. If it is marked as `tombstoned`, erase it from the map completely and drop the timing wheel node.