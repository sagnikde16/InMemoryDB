# Performance Benchmarks

**Project:** InMemoryDB — High-Throughput C++ Key-Value Store  
**Author:** Debanshu Ghosh

> All benchmarks were performed locally on Windows with the server running as a background process. Rate limiting was disabled (RateLimiter::allow returns true unconditionally) to measure raw engine throughput. The benchmark and stress test Python scripts are located in `app/scripts/`.

---

## Table of Contents

1. [Environment & Methodology](#environment--methodology)
2. [Standard Throughput Benchmark](#standard-throughput-benchmark)
3. [Chaos Stress Test](#chaos-stress-test)
4. [Analysis & Observations](#analysis--observations)
5. [Reproducing the Results](#reproducing-the-results)

---

## Environment & Methodology

| Parameter | Value |
|-----------|-------|
| OS | Windows 10/11 |
| Server binary | `ServerApp.exe` (MinGW-w64 / MSYS2, Release build) |
| Memory limit | 1 GB |
| Shard count | 64 |
| Worker threads | `std::thread::hardware_concurrency()` |
| Network | Loopback (`127.0.0.1`) |
| Client | Python 3 asyncio (`app/scripts/`) |
| Rate Limiter | **Disabled** for raw throughput measurement |

> **Why disable rate limiting?** The token bucket allows 1,000 requests/minute per IP. With 16 workers on loopback, the rate limiter fires within the first 100ms. Disabling it isolates the measurement to the engine's true processing capacity.

---

## Standard Throughput Benchmark

**Script:** `app/scripts/benchmark.py`  
**Mode:** Pipelined `SET` operations

### Configuration

| Parameter | Value |
|-----------|-------|
| Concurrent workers | 16 |
| Total requests | 100,000 |
| Pipeline depth | 50 (50 SET commands per TCP batch) |
| Command mix | 100% SET |
| Key pattern | Unique per request (`bench:{worker}:{seq}`) |

### Results

| Metric | Value |
|--------|-------|
| **Throughput** | ~28,000 Requests Per Second |
| **Total Duration** | ~3.57 seconds |
| **p50 Latency** | ~0.4 ms |
| **p90 Latency** | ~0.8 ms |
| **p99 Latency** | ~1.2 ms |
| **Max Latency** | ~2.1 ms |
| **Errors** | 0 |
| **Disconnects** | 0 |

### Interpretation

The p99 latency of **1.2 ms** is the tail — at the 99th percentile, the slowest requests took under 1.2 ms. This includes:
- TCP round-trip on loopback (~0.1 ms)
- Reactor read + tokenization + CommandFactory::parse
- WorkerPool queue enqueue + worker wakeup
- MurmurHash3 key routing + shard lock acquisition
- PMR arena allocation + `pmr::unordered_map::insert_or_assign`
- Completion queue post + Reactor drain + TCP send

A p99 of 1.2 ms under these conditions demonstrates that the lock striping and PMR arena strategy is effective — the 99th percentile is dominated by OS scheduling jitter, not lock contention.

---

## Chaos Stress Test

**Script:** `app/scripts/stress_test.py`  
**Mode:** Mixed command chaos with high key overlap

### Configuration

| Parameter | Value |
|-----------|-------|
| Concurrent clients | 1,000 |
| Total requests | 1,000,000 |
| Key overlap | 100 unique keys per worker (`key % 100`) — forces heavy shard contention |
| Command mix | 30% GET, 20% SET, 15% LPUSH, 15% HSET, 10% LRANGE, 10% HGET |

The **key overlap** design is intentional. With 1,000 workers each cycling through only 100 unique keys, multiple workers frequently target the same shard simultaneously. This maximally stresses the `std::shared_mutex` contention path and verifies the lock striping architecture under worst-case conditions.

The **mixed command types** exercise all new B1 data structures (Lists and Hashes) alongside the core string engine, testing WRONGTYPE error handling and variant dispatch correctness at scale.

### Results

| Metric | Value |
|--------|-------|
| **Total Duration** | 36.33 seconds |
| **Sustained Throughput** | ~27,525 Requests Per Second |
| **Connections Established** | 1,000 / 1,000 (100%) |
| **Successful Requests** | 1,000,000 |
| **Errors** | 0 |
| **Dropped Connections** | 0 |
| **Lock Deadlocks** | 0 |
| **OOM Events** | 0 |

### Throughput Stability
The sustained throughput of **~27,525 RPS** across 36 seconds with 1,000 concurrent clients (vs. ~28,000 RPS in the single-type benchmark) shows a degradation of only **~1.7%** despite:
- 62x more concurrent clients (16 → 1,000)
- Mixed command types including complex List and Hash operations
- Intentionally adversarial key overlap forcing shard mutex contention

This near-flat throughput curve under extreme concurrency is the direct result of the 64-shard lock striping architecture.

---

## Analysis & Observations

### Lock Contention at Scale
With 1,000 clients and 100 unique keys, multiple workers frequently acquire the same shard's `shared_mutex` simultaneously. The `std::shared_mutex` allows many concurrent readers. Writers (`SET`, `LPUSH`, `HSET`, `DEL`) obtain exclusive locks. The near-identical throughput in chaos vs. clean mode confirms that shard granularity is sufficient.

### AOF Write Load
During the stress test, the `AofLogger` dual-buffer received ~700,000 write commands (30% GET + 10% LRANGE + 10% HGET are read-only). The `appendonly.aof` file grew to approximately **4.4 MB** over 36 seconds, confirming write-through logging was active.

### Memory Behavior
1,000,000 requests with 100% unique keys (1,000 workers × 100 key slots) = 100,000 unique shard entries across 64 shards (~1,563 entries per shard). Each entry is a `CacheAlignedValue` at 64 bytes = ~6.1 MB total value storage. Well within the 1 GB arena limit — OOM eviction was never triggered.

### B5 Connection Limit
The `MAX_CONNECTIONS` limit of 10,000 was not reached in this test (1,000 < 10,000). To hit the limit, run with `-c 10001`.

### Per-Connection Pipeline Depth
The `MAX_PIPELINE_DEPTH` of 256 in-flight requests per connection was not hit at 1 request per batch. To test this, increase `--pipeline` to 300 in the benchmark script.

---

## Reproducing the Results

### Prerequisites
- Server compiled and running (see [main README.md](../README.md) for build instructions)
- Python 3 with `numpy` installed: `pip install numpy`
- Rate limiter disabled in `src/network/pipeline.cpp`:
  ```cpp
  // Temporarily return true unconditionally in RateLimiter::allow()
  bool RateLimiter::allow(const std::string& ip) { return true; }
  ```

### Run Standard Benchmark
```bash
# From the project root (server must be running)
python app/scripts/benchmark.py -c 16 -r 100000 -p 50
```

### Run Chaos Stress Test
```bash
python app/scripts/stress_test.py -c 1000 -r 1000000
```

### Custom Configurations

Push connection limits:
```bash
python app/scripts/stress_test.py -c 5000 -r 500000
```

Maximum pipeline depth test:
```bash
python app/scripts/benchmark.py -c 50 -r 1000000 -p 256
```

Ping-pong (lowest throughput, measures raw RTT):
```bash
python app/scripts/benchmark.py -c 16 -r 10000 -p 1
```
