#pragma once

#include <cstddef>
#include <memory_resource>
#include <stdexcept>
#include <vector>

using namespace std;

namespace kvstore {

// Thrown when a shard's arena reaches its hard memory limit (100%)
class OomException : public runtime_error {
public:
    using runtime_error::runtime_error;
};

// Memory resource wrapper that tracks allocation bytes and enforces limits.
// Resource chain: user → TrackingMemoryResource → pool → monotonic → raw buffer
class TrackingMemoryResource : public pmr::memory_resource {
public:
    TrackingMemoryResource(pmr::memory_resource* upstream, size_t capacity);

    size_t bytes_used() const noexcept;
    size_t capacity() const noexcept;
    double utilization() const noexcept;
    bool is_soft_limit_reached() const noexcept;   // >= 85%
    bool is_hard_limit_reached() const noexcept;    // >= 100%

    static constexpr double SOFT_LIMIT_RATIO = 0.85;

private:
    void* do_allocate(size_t bytes, size_t alignment) override;
    void do_deallocate(void* p, size_t bytes, size_t alignment) override;
    bool do_is_equal(const pmr::memory_resource& other) const noexcept override;

    pmr::memory_resource* upstream_;
    size_t bytes_used_ = 0;
    size_t capacity_;
};

// Pre-allocated PMR memory arena for a single database shard.
// Owns the full resource chain: raw buffer → monotonic → pool → tracker.
// Non-copyable and non-movable because internal resources hold pointers to each other.
class PmrArena {
public:
    explicit PmrArena(size_t capacity_bytes);

    PmrArena(const PmrArena&) = delete;
    PmrArena& operator=(const PmrArena&) = delete;
    PmrArena(PmrArena&&) = delete;
    PmrArena& operator=(PmrArena&&) = delete;

    // Returns the tracking resource for use with PMR containers
    pmr::memory_resource* resource() noexcept;

    size_t bytes_used() const noexcept;
    size_t capacity() const noexcept;
    double utilization() const noexcept;
    bool is_soft_limit_reached() const noexcept;
    bool is_hard_limit_reached() const noexcept;

private:
    // Declaration order matters: initialization follows this order
    size_t capacity_;
    vector<byte> buffer_;
    pmr::monotonic_buffer_resource monotonic_;
    pmr::unsynchronized_pool_resource pool_;
    TrackingMemoryResource tracker_;
};

} // namespace kvstore
