# Design Patterns

**Project:** InMemoryDB — High-Throughput C++ Key-Value Store  
**Author:** Debanshu Ghosh

> This document catalogues the Gang of Four (GoF) software design patterns actively implemented in the codebase. Each section explains the specific problem it solves, how it is applied, and where in the code it lives.

---

## Table of Contents

1. [Builder Pattern — `ServerBuilder`](#1-builder-pattern--serverbuilder)
2. [Reactor Pattern — `Reactor`](#2-reactor-pattern--reactor)
3. [Command Pattern — `ICommand`](#3-command-pattern--icommand)
4. [Chain of Responsibility — `Pipeline`](#4-chain-of-responsibility--pipeline)
5. [Factory Method — `CommandFactory`](#5-factory-method--commandfactory)
6. [Thread Pool — `WorkerPool`](#6-thread-pool--workerpool)
7. [Observer Pattern — `PubSubBroker`](#7-observer-pattern--pubsubbroker)
8. [Strategy Pattern — Memory Eviction](#8-strategy-pattern--memory-eviction)

---

## 1. Builder Pattern — `ServerBuilder`

**File:** `include/core/server_builder.h`  
**Intent:** Separate the construction of a complex object from its representation, so the same construction process can create different representations.

### Problem
Creating a production-grade database server requires a strict initialization sequence: the AOF logger must be opened before replay, replay must complete before the AOF daemon starts writing, the worker pool must be up before the reactor accepts connections, etc. A flat `main()` function that constructs all these objects risks initialization order errors and is impossible to unit test.

### Implementation
`ServerBuilder` exposes a fluent chainable API:

```cpp
kvstore::ServerBuilder()
    .setPort(8080)
    .setWorkerThreads(std::thread::hardware_concurrency())
    .setMaxMemoryLimit("1GB")
    .setShardCount(64)
    .setAofPath("appendonly.aof")
    .build_and_run();
```

Each setter stores configuration into private member variables. `build_and_run()` is the single method that:
1. Opens the AOF file and runs the startup replay (`is_replaying_ = true`)
2. Starts the `AofLogger` background flush daemon
3. Starts the `TimingWheel` sweeper daemon
4. Constructs the `WorkerPool` with the configured thread count
5. Constructs `RateLimiter`, `Pipeline`, and `Reactor`
6. Blocks on the Reactor event loop

This guarantees that subsystems are always initialized in the correct dependency order. Any misconfiguration throws a typed exception before the first connection is accepted.

### Alternative Considered
A single constructor `Server(port, threads, memory, shards, aofPath)` was rejected because the parameter list becomes unmanageable, parameter ordering is fragile (swapping two `size_t`s compiles silently), and the startup sequence is non-obvious to readers.

---

## 2. Reactor Pattern — `Reactor`

**File:** `include/network/reactor.h`  
**Intent:** Handle service requests delivered concurrently by one or more clients by dispatching them synchronously to associated handlers.

### Problem
The server must manage thousands of simultaneous TCP connections without dedicating a thread per client. A blocking architecture (one-thread-per-connection) does not scale beyond ~1,000 connections due to OS scheduling overhead and stack memory.

### Implementation
The `Reactor` runs a single event loop on its own thread using `WSAPoll`, Windows' non-blocking I/O multiplexer:

```cpp
while (running_) {
    drain_completion_queue();   // Process worker results
    rebuild_poll_set();         // Apply backpressure masks
    
    int result = WSAPoll(poll_fds_.data(),
                         static_cast<ULONG>(poll_fds_.size()), 100);
    
    for (auto& pfd : poll_fds_) {
        if (pfd.revents & POLLRDNORM) handle_read(pfd.fd);
        if (pfd.revents & POLLWRNORM) handle_write(pfd.fd);
    }
}
```

The Reactor owns the **event demultiplexing** (which socket needs attention) and the **dispatch** (call `handle_read` or `handle_write`). The actual command execution is delegated to the `WorkerPool`, keeping the Reactor loop non-blocking.

Connection state (`read_buffer`, `write_buffer`, `sequence_id`, `in_flight_requests`) is exclusively owned by the Reactor thread — workers never access it, eliminating data races.

---

## 3. Command Pattern — `ICommand`

**Files:** `include/core/command.h`, `src/core/command.cpp`  
**Intent:** Encapsulate a request as an object, thereby letting you parameterize clients with different requests, queue or log requests, and support undoable operations.

### Problem
The network layer receives raw byte strings from TCP. The database engine needs executable operations. Directly coupling the parser to the engine would make the system rigid, untestable, and incapable of supporting features like AOF serialization and command queueing.

### Implementation
Every database operation is a concrete subclass of `ICommand`:

```cpp
class ICommand {
public:
    virtual ~ICommand() = default;
    virtual std::string execute(ShardRouter& router, AofLogger* aof) = 0;
    virtual bool is_write() const { return false; }
    virtual std::string serialize() const = 0; // For AOF persistence
};
```

Concrete commands: `SetCommand`, `GetCommand`, `DelCommand`, `ExistsCommand`, `PingCommand`, `LPushCommand`, `LRangeCommand`, `HSetCommand`, `HGetCommand`.

Each command is self-contained:
- It stores its own parameters at parse time
- `execute()` is the database operation
- `serialize()` returns the canonical string representation for AOF

This decoupling allows the `WorkerPool` to execute commands without knowing anything about RESP parsing, and the `AofLogger` to persist commands without knowing about shard internals.

### AOF Serialization
Because each command knows how to serialize itself, the AOF log contains human-readable entries:
```
SET stress:0:42 some_value 86400
LPUSH list:stress:7:13 item_291
HSET hash:stress:2:8 field_3 val_77
DEL stress:0:42
```

On startup, these are re-parsed by `CommandFactory::parse()` and re-executed — a clean full-cycle for durability.

---

## 4. Chain of Responsibility — `Pipeline` / `PipelineHandler`

**Files:** `include/network/pipeline.h`, `src/network/pipeline.cpp`  
**Intent:** Avoid coupling the sender of a request to its receiver by giving more than one object a chance to handle it. Chain the receiving objects and pass the request along the chain until one handles it.

### Problem
A network request goes through multiple processing stages before reaching the database engine: rate limiting, tokenization, parsing, and dispatch. Each stage can reject the request. Coupling all these stages into a single function creates an unmaintainable monolith with deeply nested conditionals.

### Implementation
The codebase uses a **proper GoF Chain of Responsibility** via the `PipelineContext` struct and `PipelineHandler` abstract base class:

```cpp
// Shared context passed through all handlers
struct PipelineContext {
    SOCKET client;
    std::string line;
    uint64_t seq_id;
    std::string client_ip;
    std::vector<std::string> tokens;       // populated by TokenizeHandler
    std::unique_ptr<ICommand> command;     // populated by CommandParseHandler
    WorkerPool& pool;
};

// Abstract handler base with set_next() linkage
class PipelineHandler {
public:
    void set_next(std::shared_ptr<PipelineHandler> next) { next_ = next; }
    virtual void handle(PipelineContext& ctx) {
        if (next_) next_->handle(ctx); // propagate by default
    }
protected:
    std::shared_ptr<PipelineHandler> next_;
};
```

Four concrete handlers form the chain:

| Handler | Responsibility | Short-circuits when |
|---------|---------------|---------------------|
| `RateLimitHandler` | Checks `RateLimiter::allow(ip)` | IP over token limit → `-ERR rate limit exceeded` |
| `TokenizeHandler` | Splits the raw line into `tokens` | Empty input → `-ERR empty command` |
| `CommandParseHandler` | Calls `CommandFactory::parse(tokens)` | Unknown verb → `-ERR unknown command` |
| `DispatchHandler` | Submits `Task` to `WorkerPool` | Queue full → `-ERR server busy` |

The chain is assembled once at `Pipeline` construction using `set_next()`:

```
RateLimitHandler → TokenizeHandler → CommandParseHandler → DispatchHandler
```

`Pipeline::process()` creates a `PipelineContext` and calls `head_->handle(ctx)`. Any handler that detects a failure posts a `TaskResult` directly to the `CompletionQueue` and **returns without calling `next_->handle(ctx)`** — this is the short-circuit.

### Why This Matters
Short-circuiting is critical for performance and security:
- A rate-limited request **never** allocates a `std::vector<std::string>` for tokenization
- An unknown command **never** touches the PMR arena or the `WorkerPool`
- Under volumetric attack, only the `RateLimitHandler` runs — the rest of the pipeline is never invoked

### Extensibility
Adding a new validation stage (e.g., `AuthHandler` for client authentication) requires:
1. Subclass `PipelineHandler` with the new check
2. Insert it into the chain in `Pipeline`'s constructor

No existing handler code changes.

---

## 5. Factory Method — `CommandFactory`

**Files:** `include/core/command.h`, `src/core/command.cpp`  
**Intent:** Define an interface for creating an object, but let subclasses decide which class to instantiate.

### Problem
The RESP parser produces a `vector<string>` of tokens. Determining which `ICommand` subclass to instantiate based on the verb (`tokens[0]`) requires a dispatch mechanism that is centralized, easily extensible, and decoupled from both the network layer and the engine.

### Implementation
`CommandFactory::parse()` is a static factory method:

```cpp
std::unique_ptr<ICommand> CommandFactory::parse(
    const std::vector<std::string>& tokens) {

    if (cmd == "SET" && tokens.size() >= 3)
        return std::make_unique<SetCommand>(tokens[1], tokens[2], ttl);
    if (cmd == "GET" && tokens.size() >= 2)
        return std::make_unique<GetCommand>(tokens[1]);
    if (cmd == "LPUSH" && tokens.size() >= 3)
        return std::make_unique<LPushCommand>(tokens[1], tokens[2]);
    if (cmd == "HSET" && tokens.size() >= 4)
        return std::make_unique<HSetCommand>(tokens[1], tokens[2], tokens[3]);
    // ...
    return nullptr; // Unknown command
}
```

The factory:
1. Normalizes the command verb to uppercase
2. Validates the minimum token count (arity check)
3. Constructs the correct subclass with strongly-typed constructor arguments
4. Returns `nullptr` for unknown commands (handled upstream as an error)

Ownership is transferred to the caller via `std::unique_ptr` — there is no raw pointer management.

### Adding a New Command
To add a `INCR key` command:
1. Add `IncrCommand` class to `command.h`
2. Implement `execute()` and `serialize()` in `command.cpp`
3. Add one `if (cmd == "INCR" ...)` line to `CommandFactory::parse()`

No other files need modification.

---

## 6. Thread Pool — `WorkerPool`

**Files:** `include/core/worker_pool.h`, `src/core/worker_pool.cpp`  
**Intent:** Reuse a pool of threads to execute short-lived tasks, avoiding the overhead of creating and destroying threads for each unit of work.

### Problem
Command execution involves hash computation, shared mutex acquisition on a shard, potential PMR allocation, and string formatting for the RESP response. These operations cannot run on the Reactor thread without blocking it. Creating a new thread per command has unacceptable overhead (~microseconds per creation, plus stack memory).

### Implementation
`WorkerPool` maintains N persistent worker threads consuming from a bounded `std::queue<Task>`:

```cpp
// In WorkerPool::worker_loop():
while (true) {
    Task task;
    {
        std::unique_lock lock(mutex_);
        cv_.wait(lock, [this] { return !queue_.empty() || !running_; });
        task = std::move(queue_.front());
        queue_.pop();
    }
    // Execute on the worker thread — safe to block here
    std::string response = task.command->execute(router_, aof_);

    // Post result to the completion queue — Reactor drains this
    completion_queue_.push({task.client_fd, task.sequence_id,
                            std::move(response)});
}
```

**Key design decisions:**
- `Task` carries `client_fd` and `sequence_id` — workers never access the `ConnectionState` struct
- `CompletionQueue` is a separate `mutex + queue<TaskResult>` structure, drained by the Reactor at the top of each event loop tick
- The task queue is bounded to 10,000 entries. `submit()` returns `false` if full, allowing the `Pipeline` to post an immediate `-ERR server busy` response — a form of load shedding

### Worker Count
`std::thread::hardware_concurrency()` is used as the default, which equals the number of logical CPU cores. This is optimal for CPU-bound workloads. The `setWorkerThreads()` builder method allows tuning for I/O-heavy workloads where over-subscribing may improve latency.

---

## 7. Observer Pattern — `PubSubBroker`

**File:** `include/network/pubsub_broker.h`  
**Intent:** Define a one-to-many dependency between objects so that when one object changes state, all its dependents are notified and updated automatically.

### Problem
Pub/Sub messaging requires the broker to maintain a registry of subscriber sockets per channel and fan-out messages to all of them on `PUBLISH`. This is a textbook observer relationship: the channel is the subject, subscriber connections are observers.

### Implementation
`PubSubBroker` maintains a `std::unordered_map<string, vector<SOCKET>>` mapping channel names to connected subscriber sockets. On `SUBSCRIBE`, a socket is appended. On `PUBLISH`, the broker iterates all registered sockets for the channel and sends the message.

Because Pub/Sub bypasses the shard engine entirely (it is ephemeral messaging, not persistent storage), it uses the Reactor's socket handles directly — making it a pure in-memory fan-out with minimal overhead.

---

## 8. Strategy Pattern — Memory Eviction

**Files:** `include/daemons/timing_wheel.h`, `include/memory/pmr_allocator.h`  
**Intent:** Define a family of algorithms, encapsulate each one, and make them interchangeable.

### Problem
Memory eviction is a policy decision: at what threshold do we evict, which keys do we choose, and how aggressively? Hardcoding a single eviction strategy into the engine makes it impossible to tune or replace.

### Implementation
The `TrackingMemoryResource` tracks utilization and `Shard::is_soft_limit_reached()` exposes a clean check. The `TimingWheel` is the eviction strategy — it implements TTL-based expiry as a scheduled sweep. The system uses two-tier eviction:

- **Tier 1 (Proactive):** TTL-based eviction via the `TimingWheel` sweeper. Keys scheduled for expiry are automatically removed when their bucket tick fires.
- **Tier 2 (Reactive):** Allocation hard-block via `OomException` when utilization reaches 100%. The command executor converts this into a RESP error without crashing.

Replacing the eviction strategy (e.g., switching to LRU) would require implementing a new daemon class and updating `ServerBuilder` to use it — the `Shard` and `ShardRouter` interfaces remain untouched.
