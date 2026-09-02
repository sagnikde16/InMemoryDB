# Stabilization Changelog

This document outlines the 8 architectural fixes (B1-B8) applied to stabilize the in-memory key-value store.

## B1: List and Hash Commands
- **Issue**: Memory primitives supported `pmr::vector` and `pmr::unordered_map` but were inaccessible.
- **Fix**: Added `LPushCommand`, `LRangeCommand`, `HSetCommand`, and `HGetCommand`.
- **Implementation**: These commands manage their own locking via `shard.mutex()` (`shared_lock` for read, `unique_lock` for write) and use `std::get_if` for WRONGTYPE validation.

## B2: Thread Safety Between Worker Pool and Reactor
- **Issue**: Workers directly mutated connection buffers, risking use-after-free on dropped connections.
- **Fix**: Introduced a lock-free `CompletionQueue` as the single source of truth.
- **Implementation**: Workers execute tasks and post `TaskResult{fd, sequence_id, response}`. The reactor drains this queue at the start of each tick and safely updates connection buffers.

## B3: Out-of-Order Pipelined Responses
- **Issue**: SPMC concurrency meant fast commands could overtake slow ones, breaking RESP pipeline order.
- **Fix**: Sequence stamping and reassembly.
- **Implementation**: Connections track `next_seq_issue` and `next_seq_expected`. The reactor uses a `std::map<uint64_t, std::string> out_of_order_buf` to buffer completed tasks and flushes them to the write buffer only when the expected sequence ID is available.

## B4: Dropped Reads on Full Worker Pool
- **Issue**: A full worker queue dropped incoming tasks, breaking TCP flow control.
- **Fix**: TCP Backpressure via `POLLRDNORM` stripping.
- **Implementation**: The reactor maintains a `paused_reads_` set. When the worker pool hits 90% capacity, reads are paused. Paused sockets omit `POLLRDNORM` in `WSAPoll`, naturally shrinking the TCP window. They unpause when capacity drops below 70%.

## B5: Connection and Pipeline Limiters
- **Issue**: Vulnerability to FD exhaustion and unbounded in-flight pipelined requests.
- **Fix**: Hard limits in the reactor.
- **Implementation**: `accept_connection` rejects beyond 10,000 active connections. A per-connection `in_flight_requests` counter pauses further reads for that specific client if it hits 256 pending pipeline requests.

## B6: IP-Based Rate Limiting
- **Issue**: No protection against volumetric abuse.
- **Fix**: Sharded token bucket rate limiter.
- **Implementation**: `RateLimiter` uses 64 lock-striped shards. Each IP gets 1000 tokens per 60 seconds. The `Pipeline` checks this before parsing; rejected requests bypass the worker pool and immediately queue a `-ERR rate limit exceeded` response.

## B7: Disconnected AOF Logic and Double-Logging Bug
- **Issue**: dual-buffer AOF didn't log mutations and blind replay would duplicate data.
- **Fix**: Write-Through logging with a startup bypass flag.
- **Implementation**: Added `is_replaying_` flag to `AofLogger`. `ServerBuilder` replays the AOF line-by-line into the engine before starting the reactor. All mutation commands (`SET`, `LPUSH`, etc.) now explicitly call `aof->log()` upon success, skipped if `is_replaying_` is true. `TimingWheel` also logs `DEL` upon expiration.

## B8: O(N) Timing Wheel Deletions
- **Issue**: Explicit `DEL` commands caused an O(N) linear scan over 86,400 timing wheel buckets.
- **Fix**: Lazy Cancellation via Tombstoning.
- **Implementation**: `CacheAlignedValue` gained a `bool tombstoned = false` flag. `DEL` now sets this flag in O(1) instead of touching the timing wheel. Read commands treat tombstoned keys as missing. The timing wheel sweeps tombstoned keys and physically erases them.
