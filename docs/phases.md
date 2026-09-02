Here is the complete, master implementation guide. This document merges your architectural blueprint, the project directory structure, the build configuration, the benchmark scripts, and the phase-by-phase execution plan into a single, cohesive roadmap.

Crucially, the default 24-hour Time-To-Live (TTL) has been integrated directly into the core storage and eviction phases.

---

### 1. The Master Directory Structure

Keep your separation of concerns strict. This structure isolates interfaces (`include`) from implementations (`src`) to keep the compiler and Copilot focused.

```text
/in-memory-db
├── CMakeLists.txt              # Master build configuration
├── /include                    # Interfaces & Architecture Definitions
│   ├── /memory                 # Phase 1
│   │   ├── variant_type.h
│   │   └── pmr_allocator.h
│   ├── /engine                 # Phase 2
│   │   ├── shard.h
│   │   └── hasher.h
│   ├── /core                   # Phase 3 & 6
│   │   ├── command.h
│   │   ├── worker_pool.h
│   │   └── server_builder.h
│   ├── /network                # Phase 4 & 6
│   │   ├── reactor.h
│   │   ├── pipeline.h
│   │   └── pubsub_broker.h
│   └── /daemons                # Phase 5
│       ├── aof_logger.h
│       └── timing_wheel.h
├── /src                        # Implementations
│   ├── main.cpp                
│   ├── /memory                 
│   ├── /engine                 
│   ├── /core                   
│   ├── /network                
│   └── /daemons                
├── /scripts                    
│   └── benchmark.py            # Async throughput/latency tester
└── /tests                      # Isolated GTest Files
    ├── test_memory.cpp         
    ├── test_engine.cpp         
    └── test_eviction.cpp       

```

---

### 2. The Build System (`CMakeLists.txt`)

Using CMake ensures you can compile via VS Code (using MSYS2/MinGW) or natively open the folder in Visual Studio 2022. This script automatically fetches Google Test and links the necessary Windows Sockets (Winsock2) library.

```cmake
cmake_minimum_required(VERSION 3.20)
project(InMemoryDB VERSION 1.0 LANGUAGES CXX)

# Enforce C++23 standard
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED True)

# Add Include Directories
include_directories(${CMAKE_SOURCE_DIR}/include)

# Find OS Threads and Winsock2 (for Windows)
find_package(Threads REQUIRED)
if(WIN32)
    set(NETWORK_LIBS ws2_32)
endif()

# Fetch Google Test for Unit Testing
include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
)
FetchContent_MakeAvailable(googletest)

# --- TARGET: Main Database Server ---
file(GLOB_RECURSE SRC_FILES "src/*.cpp")
# Exclude main.cpp if building as a library for tests, or create an executable
add_executable(ServerApp ${SRC_FILES})
target_link_libraries(ServerApp PRIVATE Threads::Threads ${NETWORK_LIBS})

# --- TARGET: Unit Tests ---
add_executable(RunTests 
    tests/test_memory.cpp 
    tests/test_engine.cpp 
    tests/test_eviction.cpp
    # Include all src files EXCEPT main.cpp for testing
)
target_link_libraries(RunTests PRIVATE gtest_main Threads::Threads ${NETWORK_LIBS})

```

---

### 3. The Python Benchmark Script (`scripts/benchmark.py`)

This script blasts the server with concurrent connections to measure raw Throughput (RPS) and Latency distributions.

```python
import asyncio
import time
import numpy as np

HOST = "127.0.0.1"
PORT = 8080
TOTAL_REQUESTS = 100_000
CONCURRENCY = 50 

async def client_worker(worker_id, requests_per_worker, latency_records):
    try:
        reader, writer = await asyncio.open_connection(HOST, PORT)
    except Exception as e:
        print(f"[Worker {worker_id}] Connection failed: {e}")
        return

    # Payloads rely on the server's default 24hr TTL injection
    payloads = [f"SET key:{worker_id}:{i} val_{i}\n".encode() for i in range(requests_per_worker)]

    for payload in payloads:
        start_time = time.perf_counter()
        
        writer.write(payload)
        await writer.drain()
        await reader.readline()
        
        latency_records.append(time.perf_counter() - start_time)

    writer.close()
    await writer.wait_closed()

async def run_benchmark():
    latency_records = []
    requests_per_worker = TOTAL_REQUESTS // CONCURRENCY
    
    print(f"[*] Spawning {CONCURRENCY} workers for {TOTAL_REQUESTS} requests...")
    start_bench = time.perf_counter()
    
    tasks = [client_worker(i, requests_per_worker, latency_records) for i in range(CONCURRENCY)]
    await asyncio.gather(*tasks)
    
    total_duration = time.perf_counter() - start_bench

    if latency_records:
        latencies_ms = np.array(latency_records) * 1000
        print(f"Throughput: {len(latencies_ms) / total_duration:.2f} RPS")
        print(f"p50 Latency: {np.percentile(latencies_ms, 50):.3f} ms")
        print(f"p99 Latency: {np.percentile(latencies_ms, 99):.3f} ms")

if __name__ == "__main__":
    asyncio.run(run_benchmark())

```

---

### 4. Phase-by-Phase Implementation Plan

#### Phase 1: The Core Memory & Storage Primitives

* **Step 1.1: `variant_type.h`:** Define `std::variant<std::string, int64_t, std::vector, std::unordered_map>`. Wrap this in a struct mapped with `alignas(64)` to strictly fit cache lines.
* **Step 1.2: `pmr_allocator.h`:** Initialize `std::pmr::unsynchronized_pool_resource`. Build a flat hash map that uses this PMR resource for dynamic data (like inner strings).
* **Step 1.3: Saturation Checks:** Add variables to track byte allocation. Set the soft limit at 85% and the hard limit at 100% (throwing an OOM exception).
* **Testing (`test_memory.cpp`):** Assert `sizeof` alignment and write a loop that hits the PMR 100% limit to verify OOM logic.

#### Phase 2: The Sharded Concurrency Engine

* **Step 2.1: `shard.h`:** Define the `Shard` struct containing one PMR Map and one `std::shared_mutex`.
* **Step 2.2: `hasher.h`:** Implement MurmurHash3. Create the N-sized array of `Shard` objects (N = 64). Route strings using `Hash(key) & 63`.
* **Step 2.3: Read/Write Access:** Create thread-safe wrappers to lock the `shared_mutex` for exclusive access (Writes) and shared access (Reads).
* **Testing (`test_engine.cpp`):** Spin up 50 threads in GTest. Have them blast concurrent Read/Write operations to different keys. Verify data consistency and lack of race conditions.

#### Phase 3: The Command Pipeline & Worker Pool

* **Step 3.1: `command.h`:** Implement the Base Command and specific overrides (`SetCommand`, `GetCommand`).
* **Step 3.2: 24-Hour TTL Injection (Crucial):** Within `SetCommand::execute()`, write the logic: *If no TTL argument is provided by the network payload, default the TTL variable to 86,400 seconds (24 hours).*
* **Step 3.3: `worker_pool.h`:** Create a thread-safe SPMC queue using `std::mutex` and `std::condition_variable`. Spin up `std::thread` workers (matching your CPU core count) that wait on this queue, execute commands, and map responses to output buffers.

#### Phase 4: The Network Reactor (Winsock2)

* **Step 4.1: `reactor.h`:** Setup `WSASocket` (WSA_FLAG_OVERLAPPED) and bind to port 8080. Implement the `WSAPoll` loop checking `POLLRDNORM` for reads and `POLLWRNORM` for writes.
* **Step 4.2: `pipeline.h`:** Parse incoming byte streams by splitting on `\n`. Translate strings into `Command` objects and push them to the Phase 3 SPMC queue.
* **Step 4.3: Backpressure Strategy:** If the SPMC queue size exceeds 10,000 pending items, skip reading from sockets in the `WSAPoll` loop to force OS TCP windows to close down, preventing server OOM.

#### Phase 5: Persistence & Eviction (Background Daemons)

* **Step 5.1: `aof_logger.h`:** Create a secondary queue strictly for successful write operations. A background thread pulls from this, buffers it, and calls `fsync()` to disk every 1 second.
* **Step 5.2: `timing_wheel.h`:** Implement a Hierarchical Timing Wheel. When Phase 3 inserts a key with the default 24-hour TTL, calculate its expiration timestamp and place a reference into the corresponding bucket.
* **Step 5.3: The Sweeper Thread:** Run a background loop that sleeps for 1 second. On wake, it checks the current time bucket in the wheel, extracts expired keys, hashes them, acquires the exclusive lock on the respective `Shard` (from Phase 2), and deletes the data, returning bytes to the PMR arena.
* **Testing (`test_eviction.cpp`):** Insert keys with 1-second and 24-hour TTLs into the Wheel. Artificially advance the mock clock and assert that only the correct keys trigger the deletion callback.

#### Phase 6: Ephemeral Messaging & Final Facade

* **Step 6.1: `pubsub_broker.h`:** Implement a standard observer dictionary mapping `Channel_Name -> List<Socket_FD>`. Keep this lock-free relative to the Phase 2 engine.
* **Step 6.2: `server_builder.h`:** Create the facade. `ServerBuilder().withThreads(8).withPort(8080).start()`.
* **Step 6.3: Graceful Shutdown:** Catch `Ctrl+C`. Flip an atomic boolean `isRunning = false`. Wake all CVs, flush the final AOF buffer, and `join()` all threads cleanly.