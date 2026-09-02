#pragma once

#include "engine/shard.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

using namespace std;

namespace kvstore {

// MurmurHash3 (32-bit) — fast non-cryptographic hash for shard routing
uint32_t murmur_hash3(const string& key, uint32_t seed = 0);

// Routes keys to shards using bitwise AND masking: index = hash & (N-1).
// Shard count N must be a power of two.
class ShardRouter {
public:
    static constexpr size_t DEFAULT_SHARD_COUNT = 64;

    // total_memory is split evenly across shard_count arenas
    explicit ShardRouter(size_t total_memory,
                         size_t shard_count = DEFAULT_SHARD_COUNT);

    bool set(const string& key, const string& value, int64_t ttl = 86400);
    optional<string> get(const string& key);
    bool del(const string& key);
    bool exists(const string& key);

    Shard& shard_for_key(const string& key);
    size_t shard_index(const string& key) const;
    size_t shard_count() const;

private:
    size_t shard_count_;
    size_t shard_mask_;   // shard_count_ - 1
    vector<unique_ptr<Shard>> shards_;
};

} // namespace kvstore
