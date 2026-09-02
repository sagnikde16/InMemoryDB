# InMemoryDB


[![Language](https://img.shields.io/badge/Language-C%2B%2B17-blue.svg)](https://en.cppreference.com/w/cpp/17)
[![Platform](https://img.shields.io/badge/Platform-Windows%20(Winsock2)-informational.svg)](https://learn.microsoft.com/en-us/windows/win32/winsock/windows-sockets-start-page-2)
[![Status](https://img.shields.io/badge/Status-Stable-brightgreen.svg)]()
[![Throughput](https://img.shields.io/badge/Throughput-~28%2C000%20RPS-success.svg)]()

---

**InMemoryDB** is a high-throughput, low-latency in-memory key-value store built in modern C++. It is engineered from the ground up for extreme concurrent workloads by combining a non-blocking Winsock2 `WSAPoll` reactor, a sharded PMR arena allocator, and an SPMC worker pool with strict thread boundary safety.

Rather than scaling by adding raw threads, InMemoryDB achieves high concurrency through **architectural isolation**: the key space is partitioned into 64 independent shards each with its own memory arena and reader-writer lock, eliminating the global allocator and global mutex contention that bottleneck conventional database designs.

---

## Table of Contents

- [Features](#features)
- [Architecture Overview](#architecture-overview)
- [UML Class Diagram](#uml-class-diagram)
- [Request Lifecycle](#request-lifecycle)
- [Supported Commands](#supported-commands)
- [Benchmark Results](#benchmark-results)
- [Build & Run Instructions](#build--run-instructions)
- [Running Tests & Benchmarks](#running-tests--benchmarks)
- [Technical Documentation](#technical-documentation)

---

## Features

| Feature | Detail |
|---------|--------|
| **Non-blocking Reactor** | Single-thread Winsock2 `WSAPoll` manages up to 10,000 concurrent TCP connections |
| **64-way Shard Striping** | MurmurHash3 routes keys to independent shards ÔÇö zero cross-shard lock contention |
| **PMR Arena Allocation** | Per-shard `std::pmr` monotonic + pool resource chain; no `malloc` on fast path |
| **Cache-line Alignment** | `alignas(64)` on all value nodes prevents CPU false sharing |
| **SPMC Worker Pool** | Hardware-concurrency threads execute commands; results posted via `CompletionQueue` |
| **Pipelined RESP Support** | Sequence-stamped commands; in-order response reassembly via per-connection `out_of_order_buf` |
| **TCP Backpressure** | `POLLRDNORM` masking causes kernel-level TCP window shrink under load |
| **Token Bucket Rate Limiting** | 64-shard IP rate limiter (1,000 req/min per IP); short-circuits before parsing |
| **Connection Limits** | Hard cap: 10,000 connections, 256 in-flight pipeline depth per client |
| **Lazy Tombstone Deletion** | `DEL` is O(1) ÔÇö sets `tombstoned` flag; physical erasure deferred to `TimingWheel` |
| **Dual-buffer AOF Logger** | Double-buffered, 1-second fsync; startup replay with `is_replaying_` circuit breaker |
| **Nested Data Structures** | Lists (`LPUSH`/`LRANGE`) and Hashes (`HSET`/`HGET`) via `std::variant` + `std::get_if` |
| **Dual-layer OOM Protection** | Soft limit (85%) triggers proactive eviction; hard limit (100%) returns `-ERR OOM` |
| **Pub/Sub Broker** | Ephemeral fan-out messaging via `PubSubBroker` bypassing the shard engine |

---

## Architecture Overview

The engine is structured in four horizontal layers that communicate strictly downward:

```
ÔöîÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÉ
Ôöé              Network Layer                   Ôöé
Ôöé   Reactor (WSAPoll) ÔåÆ Pipeline ÔåÆ RateLimiter Ôöé
ÔööÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔö¼ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÿ
                   Ôöé submit(Task)
ÔöîÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔû╝ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÉ
Ôöé              Execution Layer                 Ôöé
Ôöé   WorkerPool (SPMC) ÔåÆ ICommand::execute()    Ôöé
ÔööÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔö¼ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÿ
                   Ôöé CompletionQueue::push()
ÔöîÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔû╝ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÉ
Ôöé              Storage Engine                  Ôöé
Ôöé   ShardRouter ÔåÆ Shard ÔåÆ PmrArena             Ôöé
ÔööÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔö¼ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÿ
                   Ôöé log() / evict_expired()
ÔöîÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔû╝ÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÉ
Ôöé              Daemon Layer                    Ôöé
Ôöé   AofLogger (fsync/1s) + TimingWheel (sweep) Ôöé
ÔööÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÇÔöÿ
```

---

## UML Class Diagram

```mermaid
classDiagram
    direction TB

    class ServerBuilder {
        -port_ : uint16_t
        -worker_threads_ : size_t
        -max_memory_ : size_t
        -shard_count_ : size_t
        -aof_path_ : string
        +setPort(uint16_t) ServerBuilder
        +setWorkerThreads(size_t) ServerBuilder
        +setMaxMemoryLimit(string) ServerBuilder
        +setShardCount(size_t) ServerBuilder
        +setAofPath(string) ServerBuilder
        +build_and_run()
    }

    class Reactor {
        -connections_ : map~SOCKET, ConnectionState~
        -paused_reads_ : set~SOCKET~
        -total_connections_ : atomic~size_t~
        +start()
        +stop()
        -drain_completion_queue()
        -rebuild_poll_set()
    }

    class ConnectionState {
        +read_buffer : string
        +write_buffer : string
        +next_seq_issue : uint64_t
        +next_seq_expected : uint64_t
        +in_flight_requests : uint64_t
        +out_of_order_buf : map~uint64_t, string~
    }

    class Pipeline {
        -head_ : PipelineHandler
        +process(SOCKET, string, uint64_t, string)
    }

    class PipelineHandler {
        <<abstract>>
        +handle(PipelineContext)
        +set_next(PipelineHandler)
    }

    class RateLimitHandler {
        +handle(PipelineContext)
    }
    class TokenizeHandler {
        +handle(PipelineContext)
    }
    class CommandParseHandler {
        +handle(PipelineContext)
    }
    class DispatchHandler {
        +handle(PipelineContext)
    }

    class RateLimiter {
        -shards_ : array~RateLimitShard, 64~
        +allow(ip) bool
    }

    class WorkerPool {
        -queue_ : queue~Task~
        -workers_ : vector~thread~
        +submit(Task) bool
        +completion_queue() CompletionQueue
        +shutdown()
    }

    class CompletionQueue {
        -queue_ : queue~TaskResult~
        +push(TaskResult)
        +drain(out)
    }

    class ICommand {
        <<interface>>
        +execute(ShardRouter, AofLogger) string
        +is_write() bool
        +serialize() string
    }

    class CommandFactory {
        <<static>>
        +parse(tokens) unique_ptr~ICommand~
    }

    class ShardRouter {
        -shards_ : vector~Shard, 64~
        -shard_mask_ : size_t
        +shard_for_key(key) Shard
        +set() bool
        +get() optional~string~
        +del() bool
    }

    class Shard {
        -mutex_ : shared_mutex
        -arena_ : PmrArena
        -data_ : pmr_unordered_map
        +set() bool
        +get() optional~string~
        +del() bool
        +evict_expired() bool
    }

    class PmrArena {
        -monotonic_ : monotonic_buffer_resource
        -pool_ : unsynchronized_pool_resource
        -tracker_ : TrackingMemoryResource
        +resource() memory_resource*
        +is_hard_limit_reached() bool
    }

    class CacheAlignedValue {
        <<alignas64>>
        +data : ValueVariant
        +ttl_seconds : int64_t
        +tombstoned : bool
        +is_expired() bool
    }

    class TimingWheel {
        -buckets_ : array~vector, 86400~
        +schedule(key, expiry)
        +advance_tick()
        -sweeper_loop()
    }

    class AofLogger {
        -active_buffer_ : vector~string~
        -is_replaying_ : atomic~bool~
        +log(command)
        +start()
        +set_replaying(bool)
    }

    ServerBuilder ..> Reactor : Creates
    ServerBuilder ..> WorkerPool : Creates
    ServerBuilder ..> Pipeline : Creates
    ServerBuilder ..> ShardRouter : Creates
    ServerBuilder ..> AofLogger : Creates
    ServerBuilder ..> TimingWheel : Creates

    Reactor "1" *-- "0..*" ConnectionState : owns
    Reactor --> Pipeline : delegates
    Reactor --> CompletionQueue : drains

    Pipeline --> PipelineHandler : head
    PipelineHandler <|-- RateLimitHandler
    PipelineHandler <|-- TokenizeHandler
    PipelineHandler <|-- CommandParseHandler
    PipelineHandler <|-- DispatchHandler
    RateLimitHandler --> RateLimiter : checks
    DispatchHandler --> WorkerPool : submits

    WorkerPool "1" *-- "1" CompletionQueue : owns
    WorkerPool ..> ICommand : executes

    CommandFactory ..> ICommand : creates
    ICommand --> ShardRouter : reads/writes
    ICommand --> AofLogger : write-through

    ShardRouter "1" *-- "64" Shard : stripe
    Shard "1" *-- "1" PmrArena : owns
    Shard "1" *-- "0..*" CacheAlignedValue : stores

    TimingWheel --> ShardRouter : sweeps tombstones
    TimingWheel --> AofLogger : logs expiry DEL
```

---

## Request Lifecycle

A complete single request from TCP to response:

```
1. Client sends "SET mykey myvalue\r\n" over TCP

2. Reactor (WSAPoll, POLLRDNORM fires):
   ÔåÆ append to ConnectionState.read_buffer
   ÔåÆ extract newline-terminated line
   ÔåÆ stamp with sequence_id = next_seq_issue++
   ÔåÆ increment in_flight_requests

3. Pipeline::process(fd, line, seq_id, ip):
   ÔåÆ RateLimitHandler: check token bucket for client IP
   ÔåÆ TokenizeHandler:  split "SET mykey myvalue" into tokens
   ÔåÆ CommandParseHandler: CommandFactory::parse() ÔåÆ SetCommand
   ÔåÆ DispatchHandler: WorkerPool::submit(Task{cmd, fd, seq_id})

4. WorkerPool (worker thread wakeup):
   ÔåÆ SetCommand::execute(router, aof)
   ÔåÆ router.shard_for_key("mykey") ÔåÆ Shard #42
   ÔåÆ unique_lock(shard.mutex_)
   ÔåÆ pmr_arena.allocate() ÔåÆ unordered_map.insert_or_assign()
   ÔåÆ aof->log("SET mykey myvalue 86400") [if !is_replaying_]
   ÔåÆ CompletionQueue::push({fd, seq_id, "+OK\r\n"})

5. Reactor (next tick, drain_completion_queue()):
   ÔåÆ insert into ConnectionState.out_of_order_buf[seq_id]
   ÔåÆ flush contiguous responses to write_buffer
   ÔåÆ decrement in_flight_requests
   ÔåÆ if write_buffer non-empty: set write_pending = true

6. Reactor (WSAPoll, POLLWRNORM fires):
   ÔåÆ send(write_buffer) ÔåÆ "+OK\r\n" reaches client
```

---

## Supported Commands

| Command | Syntax | Type | Description |
|---------|--------|------|-------------|
| `SET` | `SET key value [ttl]` | Write | Store a string value with optional TTL (seconds) |
| `GET` | `GET key` | Read | Retrieve a string value |
| `DEL` | `DEL key` | Write | Tombstone-mark a key for lazy deletion |
| `EXISTS` | `EXISTS key` | Read | Check if a key exists (not tombstoned/expired) |
| `PING` | `PING` | Read | Connection health check ÔåÆ `+PONG` |
| `LPUSH` | `LPUSH key value` | Write | Prepend value to a list |
| `LRANGE` | `LRANGE key start stop` | Read | Return a range from a list |
| `HSET` | `HSET key field value` | Write | Set a field in a hash |
| `HGET` | `HGET key field` | Read | Get a field from a hash |

All responses conform to the RESP (Redis Serialization Protocol) format.

---

## Benchmark Results

All tests run on localhost with rate limiting disabled to measure raw engine throughput.

### Standard Throughput Benchmark

| Parameter | Value |
|-----------|-------|
| Workers | 16 |
| Requests | 100,000 |
| Pipeline depth | 50 |
| Command | 100% SET |

| Metric | Result |
|--------|--------|
| **Throughput** | **~28,000 RPS** |
| **p50 Latency** | 0.4 ms |
| **p99 Latency** | 1.2 ms |
| **Max Latency** | 2.1 ms |
| **Errors** | 0 |

### Chaos Stress Test (1 Million Mixed Requests)

| Parameter | Value |
|-----------|-------|
| Concurrent clients | 1,000 |
| Total requests | 1,000,000 |
| Command mix | 30% GET, 20% SET, 15% LPUSH, 15% HSET, 10% LRANGE, 10% HGET |
| Key overlap | High (100 unique keys per worker ÔÇö forces shard contention) |

| Metric | Result |
|--------|--------|
| **Duration** | 36.33 seconds |
| **Throughput** | **~27,525 RPS sustained** |
| **Connections** | 1,000 / 1,000 (100% success) |
| **Errors** | 0 |
| **Dropped connections** | 0 |
| **Deadlocks** | 0 |

> The throughput degradation from 28,000 ÔåÆ 27,525 RPS (+62x clients, +mixed commands) is only **1.7%** ÔÇö direct evidence that 64-way lock striping eliminates contention at scale.

*See [docs/benchmarks.md](docs/benchmarks.md) for full methodology, reproduction commands, and analysis.*

---

## Build & Run Instructions

### Prerequisites

| Dependency | Version | Purpose |
|-----------|---------|---------|
| CMake | ÔëÑ 3.20 | Build system |
| GCC (MSYS2/MinGW-w64) or MSVC | C++17 support | Compiler |
| Python 3 + `numpy` | Optional | Benchmark scripts |

### Step 1: Clone and Navigate

```bash
cd app
```

### Step 2: Configure the Build

**Using MSYS2 / MinGW (recommended on Windows):**
```bash
cmake -B build -G "MinGW Makefiles"
```

**Using Visual Studio / MSVC:**
```bash
cmake -B build
```

### Step 3: Compile

```bash
cmake --build build
```

### Step 4: Start the Server

**MSYS2 / MinGW build:**
```bash
.\build\ServerApp.exe
```

**MSVC Debug build:**
```bash
.\build\Debug\ServerApp.exe
```

You should see:
```
=== InMemoryDB Server ===
  Port:           8080
  Workers:        8
  Max Memory:     1024 MB
  Shards:         64
  AOF Path:       appendonly.aof

[Server] Starting...
[Reactor] Listening on port 8080
```

### Step 5: Connect a Client

Use any Redis-compatible client or raw `netcat`:
```bash
# Using netcat (MSYS2)
echo -e "SET hello world\nGET hello\nPING" | nc 127.0.0.1 8080
```

Expected output:
```
+OK
$5
world
+PONG
```

---

## Running Tests & Benchmarks

### Unit Tests (Google Test)

```bash
.\build\Debug\RunTests.exe
```

Or with MSYS2:
```bash
.\build\RunTests.exe
```

### Benchmark (Pipelined Throughput)

Start the server first, then in a second terminal:
```bash
pip install numpy
python app/scripts/benchmark.py -c 16 -r 100000 -p 50
```

### Chaos Stress Test

```bash
python app/scripts/stress_test.py -c 1000 -r 1000000
```

> **Note:** Disable the rate limiter for local testing by temporarily making `RateLimiter::allow()` return `true`. See [docs/benchmarks.md](docs/benchmarks.md) for details.

---

## Technical Documentation

| Document | Description |
|----------|-------------|
| [docs/architecture_decisions.md](docs/architecture_decisions.md) | Deep dives on all 8 architectural decisions (sharding, tombstoning, backpressure, AOF circuit breaker, PMR, reactor, sequencing) with code excerpts and trade-off analysis |
| [docs/design_patterns.md](docs/design_patterns.md) | GoF pattern catalogue: Builder, Reactor, Command, Chain of Responsibility (with full `PipelineHandler` breakdown), Factory, Thread Pool, Observer, Strategy |
| [docs/benchmarks.md](docs/benchmarks.md) | Benchmark methodology, environment tables, full latency percentile data, and reproduction commands |
| [docs/class_diagram.md](docs/class_diagram.md) | Comprehensive Mermaid UML class diagram, full request lifecycle sequence diagram, and PMR resource chain flowchart |
