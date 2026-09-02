#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <shared_mutex>
#include <string>
#include <unordered_map>

namespace kvstore {

// B6: Sharded token bucket rate limiter.
// 64 lock-striped shards to avoid global contention on IP lookups.
class RateLimiter {
public:
    static constexpr size_t NUM_SHARDS = 64;
    static constexpr double MAX_TOKENS = 1000.0;
    static constexpr double REFILL_RATE = MAX_TOKENS / 60.0; // tokens/sec

    // Returns true if the request from this IP is allowed
    bool allow(const std::string& ip);

private:
    struct TokenBucket {
        double tokens = MAX_TOKENS;
        std::chrono::steady_clock::time_point last_refill =
            std::chrono::steady_clock::now();
    };

    struct RateLimitShard {
        std::unordered_map<std::string, TokenBucket> buckets;
        std::shared_mutex mutex;
    };

    size_t shard_for_ip(const std::string& ip) const;

    std::array<RateLimitShard, NUM_SHARDS> shards_;
};

} // namespace kvstore
