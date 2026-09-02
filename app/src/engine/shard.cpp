#include "engine/shard.h"
#include <string>
#include <variant>

namespace kvstore {

Shard::Shard(size_t arena_capacity)
    : arena_(arena_capacity)
    , data_(arena_.resource()) {}

bool Shard::set(const std::string& key, const std::string& value, int64_t ttl) {
    std::unique_lock lock(mutex_);
    if (arena_.is_hard_limit_reached()) return false;

    try {
        std::pmr::string pmr_key(key, arena_.resource());
        std::pmr::string pmr_val(value, arena_.resource());
        CacheAlignedValue entry(std::move(pmr_val), ttl);
        data_.insert_or_assign(std::move(pmr_key), std::move(entry));
        return true;
    } catch (const OomException&) {
        return false;
    }
}

std::optional<std::string> Shard::get(const std::string& key) {
    std::shared_lock lock(mutex_);
    std::pmr::string lookup_key(key, std::pmr::new_delete_resource());
    auto it = data_.find(lookup_key);
    if (it == data_.end()) return std::nullopt;
    if (it->second.tombstoned || it->second.is_expired()) return std::nullopt;

    return std::visit([](const auto& v) -> std::string {
        using T = std::decay_t<decltype(v)>;
        if constexpr (std::is_same_v<T, std::pmr::string>) {
            return std::string(v);
        } else if constexpr (std::is_same_v<T, int64_t>) {
            return std::to_string(v);
        } else if constexpr (std::is_same_v<T, std::pmr::vector<std::pmr::string>>) {
            return "(list)";
        } else {
            return "(hash)";
        }
    }, it->second.data);
}

// B8: tombstone instead of erase — timing wheel cleans up later
bool Shard::del(const std::string& key) {
    std::unique_lock lock(mutex_);
    std::pmr::string lookup_key(key, std::pmr::new_delete_resource());
    auto it = data_.find(lookup_key);
    if (it == data_.end() || it->second.tombstoned) return false;
    it->second.tombstoned = true;
    return true;
}

bool Shard::exists(const std::string& key) {
    std::shared_lock lock(mutex_);
    std::pmr::string lookup_key(key, std::pmr::new_delete_resource());
    auto it = data_.find(lookup_key);
    if (it == data_.end()) return false;
    return !it->second.is_expired() && !it->second.tombstoned;
}

// Called by timing wheel with exclusive lock already held
bool Shard::evict_expired(const std::string& key) {
    std::pmr::string lookup_key(key, std::pmr::new_delete_resource());
    auto it = data_.find(lookup_key);
    if (it == data_.end()) return false;
    if (it->second.tombstoned || it->second.is_expired()) {
        data_.erase(it);
        return true;
    }
    return false;
}

std::pmr::unordered_map<std::pmr::string, CacheAlignedValue>& Shard::data() { return data_; }
PmrArena& Shard::arena() { return arena_; }
size_t Shard::memory_used() const { return arena_.bytes_used(); }
size_t Shard::memory_capacity() const { return arena_.capacity(); }
bool Shard::is_soft_limit_reached() const { return arena_.is_soft_limit_reached(); }

size_t Shard::key_count() const {
    std::shared_lock lock(mutex_);
    return data_.size();
}

std::shared_mutex& Shard::mutex() { return mutex_; }

} // namespace kvstore
