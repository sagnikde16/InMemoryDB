#include "engine/hasher.h"
#include <cassert>
#include <cstring>

using namespace std;

namespace kvstore {

// MurmurHash3 32-bit finalizer — optimized for shard routing, not cryptography
uint32_t murmur_hash3(const string& key, uint32_t seed) {
    const auto* data = reinterpret_cast<const uint8_t*>(key.data());
    const int len = static_cast<int>(key.size());
    const int nblocks = len / 4;

    uint32_t h1 = seed;
    constexpr uint32_t c1 = 0xcc9e2d51;
    constexpr uint32_t c2 = 0x1b873593;

    // Process 4-byte blocks
    const auto* blocks = reinterpret_cast<const uint32_t*>(data);
    for (int i = 0; i < nblocks; i++) {
        uint32_t k1;
        memcpy(&k1, &blocks[i], sizeof(k1));

        k1 *= c1;
        k1 = (k1 << 15) | (k1 >> 17);
        k1 *= c2;

        h1 ^= k1;
        h1 = (h1 << 13) | (h1 >> 19);
        h1 = h1 * 5 + 0xe6546b64;
    }

    // Process remaining tail bytes
    const uint8_t* tail = data + nblocks * 4;
    uint32_t k1 = 0;
    switch (len & 3) {
        case 3: k1 ^= tail[2] << 16; [[fallthrough]];
        case 2: k1 ^= tail[1] << 8;  [[fallthrough]];
        case 1: k1 ^= tail[0];
                k1 *= c1;
                k1 = (k1 << 15) | (k1 >> 17);
                k1 *= c2;
                h1 ^= k1;
    }

    // Finalization mix — ensures avalanche
    h1 ^= static_cast<uint32_t>(len);
    h1 ^= h1 >> 16;
    h1 *= 0x85ebca6b;
    h1 ^= h1 >> 13;
    h1 *= 0xc2b2ae35;
    h1 ^= h1 >> 16;

    return h1;
}

// --- ShardRouter ---

ShardRouter::ShardRouter(size_t total_memory, size_t shard_count)
    : shard_count_(shard_count)
    , shard_mask_(shard_count - 1) {
    // Enforce power-of-two constraint for bitwise masking
    assert((shard_count & (shard_count - 1)) == 0 && "Shard count must be a power of two");

    size_t per_shard = total_memory / shard_count;
    shards_.reserve(shard_count);
    for (size_t i = 0; i < shard_count; i++) {
        shards_.push_back(make_unique<Shard>(per_shard));
    }
}

size_t ShardRouter::shard_index(const string& key) const {
    return murmur_hash3(key) & shard_mask_;
}

Shard& ShardRouter::shard_for_key(const string& key) {
    return *shards_[shard_index(key)];
}

size_t ShardRouter::shard_count() const { return shard_count_; }

bool ShardRouter::set(const string& key, const string& value, int64_t ttl) {
    return shard_for_key(key).set(key, value, ttl);
}

optional<string> ShardRouter::get(const string& key) {
    return shard_for_key(key).get(key);
}

bool ShardRouter::del(const string& key) {
    return shard_for_key(key).del(key);
}

bool ShardRouter::exists(const string& key) {
    return shard_for_key(key).exists(key);
}

} // namespace kvstore
