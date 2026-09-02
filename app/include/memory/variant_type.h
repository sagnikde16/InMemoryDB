#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory_resource>
#include <string>
#include <unordered_map>
#include <variant>
#include <vector>

namespace kvstore {

using ValueVariant = std::variant<
    std::pmr::string,
    int64_t,
    std::pmr::vector<std::pmr::string>,
    std::pmr::unordered_map<std::pmr::string, std::pmr::string>
>;

enum class ValueType : uint8_t {
    String  = 0,
    Integer = 1,
    List    = 2,
    Hash    = 3
};

// Cache-line aligned storage node. alignas(64) prevents false sharing
// when adjacent entries are accessed by different CPU cores.
struct alignas(64) CacheAlignedValue {
    ValueVariant data;
    std::chrono::steady_clock::time_point created_at;
    std::chrono::steady_clock::time_point last_accessed;
    int64_t ttl_seconds;    // -1 = no expiry, default = 86400 (24 hours)
    bool tombstoned = false; // B8: lazy cancellation flag for timing wheel

    CacheAlignedValue() = default;
    CacheAlignedValue(ValueVariant value, int64_t ttl = 86400);

    ValueType type() const;
    size_t estimated_memory_usage() const;
    bool is_expired() const;
    void touch();
    std::chrono::steady_clock::time_point expiration_time() const;
};

} // namespace kvstore
