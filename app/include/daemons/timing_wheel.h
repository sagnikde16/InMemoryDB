#pragma once

#include "engine/hasher.h"
#include <array>
#include <atomic>
#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace kvstore {

class AofLogger;

// Hierarchical timing wheel — 86400 buckets (one per second for 24h).
// B8: sweep checks tombstone flag; cancel() removed (O(N) scan eliminated).
class TimingWheel {
public:
    static constexpr size_t WHEEL_SIZE = 86400;

    TimingWheel(ShardRouter& router, AofLogger* aof = nullptr);
    ~TimingWheel();

    TimingWheel(const TimingWheel&) = delete;
    TimingWheel& operator=(const TimingWheel&) = delete;

    void schedule(const std::string& key, size_t shard_index,
                  std::chrono::steady_clock::time_point expiry);

    void start();
    void stop();
    void advance_tick();

private:
    struct Entry {
        std::string key;
        size_t shard_index;
    };

    void sweeper_loop();
    size_t time_to_bucket(std::chrono::steady_clock::time_point expiry) const;

    ShardRouter& router_;
    AofLogger* aof_;
    std::array<std::vector<Entry>, WHEEL_SIZE> buckets_;
    std::mutex mutex_;
    std::thread sweeper_thread_;
    std::atomic<bool> running_{false};
    std::chrono::steady_clock::time_point start_time_;
    size_t current_tick_ = 0;
};

} // namespace kvstore
