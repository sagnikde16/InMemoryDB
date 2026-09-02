# UML Class Diagram

**Project:** InMemoryDB — High-Throughput C++ Key-Value Store

> This diagram maps the complete class hierarchy, ownership relationships, and data flow of the application from the network layer down to the PMR memory arena.

---

## Full System Class Diagram

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
        -parse_memory_limit(string) size_t
    }

    class Reactor {
        -port_ : uint16_t
        -listen_socket_ : SOCKET
        -running_ : atomic~bool~
        -poll_fds_ : vector~WSAPOLLFD~
        -connections_ : unordered_map~SOCKET, ConnectionState~
        -paused_reads_ : unordered_set~SOCKET~
        -total_connections_ : atomic~size_t~
        +start()
        +stop()
        -accept_connection()
        -handle_read(SOCKET)
        -handle_write(SOCKET)
        -disconnect(SOCKET)
        -drain_completion_queue()
        -rebuild_poll_set()
    }

    class ConnectionState {
        +read_buffer : string
        +write_buffer : string
        +write_pending : bool
        +client_ip : string
        +next_seq_issue : uint64_t
        +next_seq_expected : uint64_t
        +in_flight_requests : uint64_t
        +out_of_order_buf : map~uint64_t, string~
    }

    class Pipeline {
        +process(SOCKET, string, uint64_t, string)
    }

    class RateLimiter {
        -shards_ : array~RateLimitShard, 64~
        +allow(ip) bool
        -shard_for_ip(ip) size_t
    }

    class RateLimitShard {
        +buckets : unordered_map~string, TokenBucket~
        +mutex : shared_mutex
    }

    class TokenBucket {
        +tokens : double
        +last_refill : time_point
    }

    class WorkerPool {
        -max_queue_size_ : size_t
        -running_ : atomic~bool~
        -queue_ : queue~Task~
        -workers_ : vector~thread~
        +submit(Task) bool
        +queue_size() size_t
        +is_full() bool
        +completion_queue() CompletionQueue
        +shutdown()
        -worker_loop()
    }

    class CompletionQueue {
        -queue_ : queue~TaskResult~
        -mutex_ : mutex
        +push(TaskResult)
        +drain(vector~TaskResult~)
    }

    class Task {
        +command : unique_ptr~ICommand~
        +client_fd : SOCKET
        +sequence_id : uint64_t
    }

    class TaskResult {
        +client_fd : SOCKET
        +sequence_id : uint64_t
        +response : string
    }

    class CommandFactory {
        <<static>>
        +parse(tokens) unique_ptr~ICommand~
    }

    class ICommand {
        <<interface>>
        +execute(ShardRouter, AofLogger) string
        +is_write() bool
        +serialize() string
    }

    class SetCommand {
        -key_ : string
        -value_ : string
        -ttl_ : int64_t
        +execute(ShardRouter, AofLogger) string
        +serialize() string
    }

    class GetCommand {
        -key_ : string
        +execute(ShardRouter, AofLogger) string
    }

    class DelCommand {
        -key_ : string
        +execute(ShardRouter, AofLogger) string
    }

    class LPushCommand {
        -key_ : string
        -value_ : string
        -ttl_ : int64_t
        +execute(ShardRouter, AofLogger) string
    }

    class HSetCommand {
        -key_ : string
        -field_ : string
        -value_ : string
        +execute(ShardRouter, AofLogger) string
    }

    class ShardRouter {
        -shard_count_ : size_t
        -shard_mask_ : size_t
        -shards_ : vector~unique_ptr~Shard~~
        +set(key, value, ttl) bool
        +get(key) optional~string~
        +del(key) bool
        +exists(key) bool
        +shard_for_key(key) Shard
        +shard_index(key) size_t
    }

    class Shard {
        -mutex_ : shared_mutex
        -arena_ : PmrArena
        -data_ : pmr_unordered_map
        +set(key, value, ttl) bool
        +get(key) optional~string~
        +del(key) bool
        +exists(key) bool
        +evict_expired(key) bool
        +data() pmr_unordered_map
        +arena() PmrArena
        +mutex() shared_mutex
    }

    class PmrArena {
        -capacity_ : size_t
        -buffer_ : vector~byte~
        -monotonic_ : monotonic_buffer_resource
        -pool_ : unsynchronized_pool_resource
        -tracker_ : TrackingMemoryResource
        +resource() memory_resource*
        +bytes_used() size_t
        +capacity() size_t
        +is_soft_limit_reached() bool
        +is_hard_limit_reached() bool
    }

    class CacheAlignedValue {
        <<alignas64>>
        +data : ValueVariant
        +created_at : time_point
        +last_accessed : time_point
        +ttl_seconds : int64_t
        +tombstoned : bool
        +type() ValueType
        +is_expired() bool
        +touch()
    }

    class TimingWheel {
        -buckets_ : array~vector~Entry~, 86400~
        -running_ : atomic~bool~
        -current_tick_ : size_t
        +schedule(key, shard_idx, expiry)
        +start()
        +stop()
        +advance_tick()
        -sweeper_loop()
    }

    class AofLogger {
        -filepath_ : string
        -file_ : ofstream
        -active_buffer_ : vector~string~*
        -flush_buffer_ : vector~string~*
        -running_ : atomic~bool~
        -is_replaying_ : atomic~bool~
        +log(command)
        +flush()
        +start()
        +shutdown()
        +is_replaying() bool
        +set_replaying(bool)
    }

    %% -- Ownership & Assembly --
    ServerBuilder ..> Reactor : Creates
    ServerBuilder ..> WorkerPool : Creates
    ServerBuilder ..> AofLogger : Creates
    ServerBuilder ..> TimingWheel : Creates
    ServerBuilder ..> ShardRouter : Creates
    ServerBuilder ..> Pipeline : Creates
    ServerBuilder ..> RateLimiter : Creates

    %% -- Reactor owns connections & delegates --
    Reactor "1" *-- "0..*" ConnectionState : owns
    Reactor --> Pipeline : delegates parsed lines
    Reactor --> WorkerPool : references
    Reactor --> CompletionQueue : drains

    %% -- Pipeline guards & dispatches --
    Pipeline --> RateLimiter : validates IP
    Pipeline ..> CommandFactory : calls parse
    Pipeline --> WorkerPool : submits Task

    %% -- RateLimiter internals --
    RateLimiter "1" *-- "64" RateLimitShard : stripe array
    RateLimitShard "1" *-- "0..*" TokenBucket : per-IP bucket

    %% -- WorkerPool internals --
    WorkerPool "1" *-- "1" CompletionQueue : owns
    WorkerPool --> Task : consumes
    WorkerPool ..> TaskResult : produces

    %% -- Command hierarchy --
    CommandFactory ..> ICommand : instantiates
    ICommand <|-- SetCommand
    ICommand <|-- GetCommand
    ICommand <|-- DelCommand
    ICommand <|-- LPushCommand
    ICommand <|-- HSetCommand

    %% -- Execution path --
    ICommand --> ShardRouter : reads/writes
    ICommand --> AofLogger : write-through log

    %% -- Shard engine internals --
    ShardRouter "1" *-- "64" Shard : stripe array
    Shard "1" *-- "1" PmrArena : owns arena
    Shard "1" *-- "0..*" CacheAlignedValue : stores

    %% -- Daemon relationships --
    TimingWheel --> ShardRouter : sweeps tombstones
    TimingWheel --> AofLogger : logs DEL on expiry
```

---

## Request Lifecycle Flow

```mermaid
sequenceDiagram
    participant C as Client (TCP)
    participant R as Reactor
    participant P as Pipeline
    participant RL as RateLimiter
    participant CF as CommandFactory
    participant WP as WorkerPool
    participant W as Worker Thread
    participant SR as ShardRouter
    participant S as Shard
    participant AOF as AofLogger
    participant CQ as CompletionQueue

    C->>R: TCP data arrives (POLLRDNORM)
    R->>R: append to read_buffer, extract line
    R->>P: process(fd, line, seq_id, ip)
    P->>RL: allow(ip)?
    alt Rate Limited
        RL-->>P: false
        P->>CQ: push TaskResult("-ERR rate limit exceeded")
    else Allowed
        RL-->>P: true
        P->>CF: parse(tokens)
        CF-->>P: unique_ptr ICommand
        P->>WP: submit(Task{cmd, fd, seq_id})
        WP->>W: wakeup (condition_variable)
        W->>SR: cmd->execute(router, aof)
        SR->>S: lock(shared_mutex) + data op
        S-->>SR: result string
        W->>AOF: aof->log(serialize())
        W->>CQ: push TaskResult{fd, seq_id, response}
        R->>CQ: drain() on next tick
        R->>R: reassemble in-order via out_of_order_buf
        R->>C: send(write_buffer) via POLLWRNORM
    end
```

---

## Memory Resource Chain

```mermaid
graph TD
    A["Raw byte buffer\nvector&lt;byte&gt; 16MB per shard"] --> B
    B["monotonic_buffer_resource\nFast, append-only sub-allocation"] --> C
    C["unsynchronized_pool_resource\nRecycles freed blocks by size class"] --> D
    D["TrackingMemoryResource\nCounts bytes_used, enforces limits"]
    D -- "85% full" --> E["TimingWheel eviction triggered"]
    D -- "100% full" --> F["OomException thrown → -ERR OOM"]
    D -- "allocate()" --> G["pmr::unordered_map\npmr::string\npmr::vector"]
```
