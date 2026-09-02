#include "core/command.h"
#include "daemons/aof_logger.h"
#include <algorithm>
#include <cctype>
#include <variant>

namespace kvstore {

static std::string to_upper(const std::string& s) {
    std::string r = s;
    std::transform(r.begin(), r.end(), r.begin(),
                   [](unsigned char c) { return std::toupper(c); });
    return r;
}

// --- SetCommand ---

SetCommand::SetCommand(std::string key, std::string value, int64_t ttl)
    : key_(std::move(key)), value_(std::move(value)), ttl_(ttl) {}

std::string SetCommand::execute(ShardRouter& router, AofLogger* aof) {
    if (router.set(key_, value_, ttl_)) {
        if (aof && !aof->is_replaying()) aof->log(serialize());
        return "+OK\r\n";
    }
    return "-ERR OOM command not allowed when used memory > 'maxmemory'\r\n";
}

std::string SetCommand::serialize() const {
    return "SET " + key_ + " " + value_ + " " + std::to_string(ttl_);
}

// --- GetCommand ---

GetCommand::GetCommand(std::string key) : key_(std::move(key)) {}

std::string GetCommand::execute(ShardRouter& router, AofLogger*) {
    auto val = router.get(key_);
    if (!val.has_value()) return "$-1\r\n";
    return "$" + std::to_string(val->size()) + "\r\n" + *val + "\r\n";
}

std::string GetCommand::serialize() const { return "GET " + key_; }

// --- DelCommand ---

DelCommand::DelCommand(std::string key) : key_(std::move(key)) {}

std::string DelCommand::execute(ShardRouter& router, AofLogger* aof) {
    int removed = router.del(key_) ? 1 : 0;
    if (removed && aof && !aof->is_replaying()) aof->log(serialize());
    return ":" + std::to_string(removed) + "\r\n";
}

std::string DelCommand::serialize() const { return "DEL " + key_; }

// --- ExistsCommand ---

ExistsCommand::ExistsCommand(std::string key) : key_(std::move(key)) {}

std::string ExistsCommand::execute(ShardRouter& router, AofLogger*) {
    return ":" + std::to_string(router.exists(key_) ? 1 : 0) + "\r\n";
}

std::string ExistsCommand::serialize() const { return "EXISTS " + key_; }

// --- PingCommand ---

std::string PingCommand::execute(ShardRouter&, AofLogger*) { return "+PONG\r\n"; }
std::string PingCommand::serialize() const { return "PING"; }

// --- LPushCommand (B1) ---

LPushCommand::LPushCommand(std::string key, std::string value, int64_t ttl)
    : key_(std::move(key)), value_(std::move(value)), ttl_(ttl) {}

std::string LPushCommand::execute(ShardRouter& router, AofLogger* aof) {
    auto& shard = router.shard_for_key(key_);
    std::unique_lock lock(shard.mutex());

    if (shard.arena().is_hard_limit_reached())
        return "-ERR OOM command not allowed when used memory > 'maxmemory'\r\n";

    try {
        auto* res = shard.arena().resource();
        std::pmr::string pmr_key(key_, res);
        auto& map = shard.data();
        auto it = map.find(pmr_key);

        if (it != map.end() && !it->second.tombstoned && !it->second.is_expired()) {
            auto* list = std::get_if<std::pmr::vector<std::pmr::string>>(&it->second.data);
            if (!list)
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
            list->emplace_back(value_);
            if (aof && !aof->is_replaying()) aof->log(serialize());
            return ":" + std::to_string(list->size()) + "\r\n";
        }

        std::pmr::vector<std::pmr::string> list(res);
        list.emplace_back(value_);
        CacheAlignedValue entry(std::move(list), ttl_);
        map.insert_or_assign(std::move(pmr_key), std::move(entry));
        if (aof && !aof->is_replaying()) aof->log(serialize());
        return ":1\r\n";
    } catch (const OomException&) {
        return "-ERR OOM command not allowed when used memory > 'maxmemory'\r\n";
    }
}

std::string LPushCommand::serialize() const { return "LPUSH " + key_ + " " + value_; }

// --- LRangeCommand (B1) ---

LRangeCommand::LRangeCommand(std::string key, int64_t start, int64_t stop)
    : key_(std::move(key)), start_(start), stop_(stop) {}

std::string LRangeCommand::execute(ShardRouter& router, AofLogger*) {
    auto& shard = router.shard_for_key(key_);
    std::shared_lock lock(shard.mutex());

    std::pmr::string lookup(key_, std::pmr::new_delete_resource());
    auto& map = shard.data();
    auto it = map.find(lookup);

    if (it == map.end() || it->second.tombstoned || it->second.is_expired())
        return "*0\r\n";

    auto* list = std::get_if<std::pmr::vector<std::pmr::string>>(&it->second.data);
    if (!list)
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

    int64_t len = static_cast<int64_t>(list->size());
    int64_t s = start_ < 0 ? start_ + len : start_;
    int64_t e = stop_  < 0 ? stop_  + len : stop_;
    if (s < 0) s = 0;
    if (e >= len) e = len - 1;
    if (s > e) return "*0\r\n";

    int64_t count = e - s + 1;
    std::string result = "*" + std::to_string(count) + "\r\n";
    for (int64_t i = s; i <= e; i++) {
        const auto& elem = (*list)[static_cast<size_t>(i)];
        result += "$" + std::to_string(elem.size()) + "\r\n" + std::string(elem) + "\r\n";
    }
    return result;
}

std::string LRangeCommand::serialize() const {
    return "LRANGE " + key_ + " " + std::to_string(start_) + " " + std::to_string(stop_);
}

// --- HSetCommand (B1) ---

HSetCommand::HSetCommand(std::string key, std::string field, std::string value, int64_t ttl)
    : key_(std::move(key)), field_(std::move(field)), value_(std::move(value)), ttl_(ttl) {}

std::string HSetCommand::execute(ShardRouter& router, AofLogger* aof) {
    auto& shard = router.shard_for_key(key_);
    std::unique_lock lock(shard.mutex());

    if (shard.arena().is_hard_limit_reached())
        return "-ERR OOM command not allowed when used memory > 'maxmemory'\r\n";

    try {
        auto* res = shard.arena().resource();
        std::pmr::string pmr_key(key_, res);
        auto& map = shard.data();
        auto it = map.find(pmr_key);

        if (it != map.end() && !it->second.tombstoned && !it->second.is_expired()) {
            auto* hash = std::get_if<
                std::pmr::unordered_map<std::pmr::string, std::pmr::string>>(&it->second.data);
            if (!hash)
                return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

            std::pmr::string pf(field_, res);
            bool is_new = (hash->find(pf) == hash->end());
            (*hash)[std::move(pf)] = std::pmr::string(value_, res);
            if (aof && !aof->is_replaying()) aof->log(serialize());
            return ":" + std::to_string(is_new ? 1 : 0) + "\r\n";
        }

        std::pmr::unordered_map<std::pmr::string, std::pmr::string> hash(res);
        hash[std::pmr::string(field_, res)] = std::pmr::string(value_, res);
        CacheAlignedValue entry(std::move(hash), ttl_);
        map.insert_or_assign(std::move(pmr_key), std::move(entry));
        if (aof && !aof->is_replaying()) aof->log(serialize());
        return ":1\r\n";
    } catch (const OomException&) {
        return "-ERR OOM command not allowed when used memory > 'maxmemory'\r\n";
    }
}

std::string HSetCommand::serialize() const {
    return "HSET " + key_ + " " + field_ + " " + value_;
}

// --- HGetCommand (B1) ---

HGetCommand::HGetCommand(std::string key, std::string field)
    : key_(std::move(key)), field_(std::move(field)) {}

std::string HGetCommand::execute(ShardRouter& router, AofLogger*) {
    auto& shard = router.shard_for_key(key_);
    std::shared_lock lock(shard.mutex());

    std::pmr::string lookup(key_, std::pmr::new_delete_resource());
    auto& map = shard.data();
    auto it = map.find(lookup);

    if (it == map.end() || it->second.tombstoned || it->second.is_expired())
        return "$-1\r\n";

    auto* hash = std::get_if<
        std::pmr::unordered_map<std::pmr::string, std::pmr::string>>(&it->second.data);
    if (!hash)
        return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";

    std::pmr::string pf(field_, std::pmr::new_delete_resource());
    auto fit = hash->find(pf);
    if (fit == hash->end()) return "$-1\r\n";
    return "$" + std::to_string(fit->second.size()) + "\r\n" +
           std::string(fit->second) + "\r\n";
}

std::string HGetCommand::serialize() const { return "HGET " + key_ + " " + field_; }

// --- CommandFactory ---

std::unique_ptr<ICommand> CommandFactory::parse(const std::vector<std::string>& tokens) {
    if (tokens.empty()) return nullptr;
    std::string cmd = to_upper(tokens[0]);

    if (cmd == "SET" && tokens.size() >= 3) {
        int64_t ttl = 86400;
        if (tokens.size() >= 4) {
            try { ttl = std::stoll(tokens[3]); } catch (...) { ttl = 86400; }
        }
        return std::make_unique<SetCommand>(tokens[1], tokens[2], ttl);
    }
    if (cmd == "GET" && tokens.size() >= 2)
        return std::make_unique<GetCommand>(tokens[1]);
    if (cmd == "DEL" && tokens.size() >= 2)
        return std::make_unique<DelCommand>(tokens[1]);
    if (cmd == "EXISTS" && tokens.size() >= 2)
        return std::make_unique<ExistsCommand>(tokens[1]);
    if (cmd == "PING")
        return std::make_unique<PingCommand>();
    if (cmd == "LPUSH" && tokens.size() >= 3)
        return std::make_unique<LPushCommand>(tokens[1], tokens[2]);
    if (cmd == "LRANGE" && tokens.size() >= 4) {
        try {
            return std::make_unique<LRangeCommand>(
                tokens[1], std::stoll(tokens[2]), std::stoll(tokens[3]));
        } catch (...) { return nullptr; }
    }
    if (cmd == "HSET" && tokens.size() >= 4)
        return std::make_unique<HSetCommand>(tokens[1], tokens[2], tokens[3]);
    if (cmd == "HGET" && tokens.size() >= 3)
        return std::make_unique<HGetCommand>(tokens[1], tokens[2]);

    return nullptr;
}

} // namespace kvstore
