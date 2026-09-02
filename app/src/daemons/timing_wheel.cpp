#include "daemons/timing_wheel.h"
#include "daemons/aof_logger.h"
#include <memory_resource>

namespace kvstore {

TimingWheel::TimingWheel(ShardRouter& router, AofLogger* aof)
    : router_(router), aof_(aof)
    , start_time_(std::chrono::steady_clock::now()) {}

TimingWheel::~TimingWheel() { stop(); }

void TimingWheel::schedule(const std::string& key, size_t /*shard_index*/,
                           std::chrono::steady_clock::time_point expiry) {
    std::lock_guard lock(mutex_);
    size_t bucket = time_to_bucket(expiry);
    buckets_[bucket].push_back({key, 0});
}

void TimingWheel::start() {
    running_ = true;
    start_time_ = std::chrono::steady_clock::now();
    current_tick_ = 0;
    sweeper_thread_ = std::thread(&TimingWheel::sweeper_loop, this);
}

void TimingWheel::stop() {
    if (!running_.exchange(false)) return;
    if (sweeper_thread_.joinable()) sweeper_thread_.join();
}

// B8: check tombstoned/expired — only erase if marked, skip re-SET keys
void TimingWheel::advance_tick() {
    std::vector<Entry> expired;
    {
        std::lock_guard lock(mutex_);
        size_t idx = current_tick_ % WHEEL_SIZE;
        expired = std::move(buckets_[idx]);
        buckets_[idx].clear();
        current_tick_++;
    }

    for (const auto& entry : expired) {
        auto& shard = router_.shard_for_key(entry.key);
        std::unique_lock shard_lock(shard.mutex());
        bool evicted = shard.evict_expired(entry.key);
        shard_lock.unlock();

        // B7: log DEL to AOF when a key expires naturally
        if (evicted && aof_ && !aof_->is_replaying())
            aof_->log("DEL " + entry.key);
    }
}

void TimingWheel::sweeper_loop() {
    while (running_) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        if (!running_) break;
        advance_tick();
    }
}

size_t TimingWheel::time_to_bucket(
    std::chrono::steady_clock::time_point expiry) const {
    auto delta = std::chrono::duration_cast<std::chrono::seconds>(
        expiry - start_time_).count();
    if (delta < 0) delta = 0;
    return static_cast<size_t>(delta) % WHEEL_SIZE;
}

} // namespace kvstore
