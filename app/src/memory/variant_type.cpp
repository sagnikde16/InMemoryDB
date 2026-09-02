#include "memory/variant_type.h"

using namespace std;

namespace kvstore {

CacheAlignedValue::CacheAlignedValue(ValueVariant value, int64_t ttl)
    : data(move(value))
    , created_at(chrono::steady_clock::now())
    , last_accessed(created_at)
    , ttl_seconds(ttl) {}

ValueType CacheAlignedValue::type() const {
    return static_cast<ValueType>(data.index());
}

size_t CacheAlignedValue::estimated_memory_usage() const {
    size_t base = sizeof(CacheAlignedValue);

    size_t dynamic = visit([](const auto& val) -> size_t {
        using T = decay_t<decltype(val)>;

        if constexpr (is_same_v<T, pmr::string>) {
            return val.capacity();
        } else if constexpr (is_same_v<T, int64_t>) {
            return 0;  // stored inline in the variant
        } else if constexpr (is_same_v<T, pmr::vector<pmr::string>>) {
            size_t total = val.capacity() * sizeof(pmr::string);
            for (const auto& s : val) total += s.capacity();
            return total;
        } else if constexpr (is_same_v<T, pmr::unordered_map<pmr::string, pmr::string>>) {
            size_t total = val.bucket_count() * sizeof(void*);
            for (const auto& [k, v] : val) {
                total += k.capacity() + v.capacity()
                       + sizeof(pair<const pmr::string, pmr::string>);
            }
            return total;
        }
        return 0;
    }, data);

    return base + dynamic;
}

bool CacheAlignedValue::is_expired() const {
    if (ttl_seconds < 0) return false;
    auto elapsed = chrono::duration_cast<chrono::seconds>(
        chrono::steady_clock::now() - created_at
    ).count();
    return elapsed >= ttl_seconds;
}

void CacheAlignedValue::touch() {
    last_accessed = chrono::steady_clock::now();
}

chrono::steady_clock::time_point CacheAlignedValue::expiration_time() const {
    if (ttl_seconds < 0) {
        return chrono::steady_clock::time_point::max();
    }
    return created_at + chrono::seconds(ttl_seconds);
}

} // namespace kvstore
