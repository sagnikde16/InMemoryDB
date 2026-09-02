#pragma once

#include "memory/pmr_allocator.h"
#include "memory/variant_type.h"
#include <memory_resource>
#include <optional>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace kvstore {

// A single database shard: owns a PMR arena, a data map, and a reader-writer lock.
class Shard {
public:
    explicit Shard(size_t arena_capacity);

    // Self-locking CRUD (used by ShardRouter facade)
    bool set(const std::string& key, const std::string& value, int64_t ttl = 86400);
    bool del(const std::string& key);
    std::optional<std::string> get(const std::string& key);
    bool exists(const std::string& key);

    // Caller must already hold exclusive lock (timing wheel eviction)
    bool evict_expired(const std::string& key);

    // Direct accessors for commands that manage their own locking (B1)
    std::pmr::unordered_map<std::pmr::string, CacheAlignedValue>& data();
    PmrArena& arena();

    size_t memory_used() const;
    size_t memory_capacity() const;
    bool is_soft_limit_reached() const;
    size_t key_count() const;
    std::shared_mutex& mutex();

private:
    PmrArena arena_;
    std::pmr::unordered_map<std::pmr::string, CacheAlignedValue> data_;
    mutable std::shared_mutex mutex_;
};

} // namespace kvstore
