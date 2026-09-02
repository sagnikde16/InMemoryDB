#pragma once

#include <cstddef>
#include <cstdint>
#include <string>

using namespace std;

namespace kvstore {

// Builder pattern for constructing and starting the database server.
// Chains configuration calls and assembles all subsystems on build_and_run().
class ServerBuilder {
public:
    ServerBuilder& setPort(uint16_t port);
    ServerBuilder& setWorkerThreads(size_t count);
    ServerBuilder& setMaxMemoryLimit(const string& limit);
    ServerBuilder& setShardCount(size_t count);
    ServerBuilder& setAofPath(const string& path);

    // Assembles all components and starts the server (blocking)
    void build_and_run();

private:
    // Parse human-readable memory strings like "512MB" or "1GB"
    static size_t parse_memory_limit(const string& limit);

    uint16_t port_ = 8080;
    size_t worker_threads_ = 4;
    size_t max_memory_ = 512ULL * 1024 * 1024;  // 512MB default
    size_t shard_count_ = 64;
    string aof_path_ = "appendonly.aof";
};

} // namespace kvstore
