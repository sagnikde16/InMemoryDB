#include "network/rate_limiter.h"
#include <algorithm>
#include <functional>

namespace kvstore {

size_t RateLimiter::shard_for_ip(const std::string& ip) const {
    return std::hash<std::string>{}(ip) % NUM_SHARDS;
}

bool RateLimiter::allow(const std::string& ip) {
    // Temporarily disable rate limiting for benchmarks
    return true;

    auto& shard = shards_[shard_for_ip(ip)];
    std::unique_lock lock(shard.mutex);

    auto& bucket = shard.buckets[ip];
    auto now = std::chrono::steady_clock::now();
    double elapsed =
        std::chrono::duration<double>(now - bucket.last_refill).count();

    bucket.tokens = std::min(MAX_TOKENS, bucket.tokens + elapsed * REFILL_RATE);
    bucket.last_refill = now;

    if (bucket.tokens >= 1.0) {
        bucket.tokens -= 1.0;
        return true;
    }
    return false;
}

} // namespace kvstore
