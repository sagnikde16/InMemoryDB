#include "memory/pmr_allocator.h"

using namespace std;

namespace kvstore {

// --- TrackingMemoryResource ---

TrackingMemoryResource::TrackingMemoryResource(
    pmr::memory_resource* upstream, size_t capacity)
    : upstream_(upstream), capacity_(capacity) {}

size_t TrackingMemoryResource::bytes_used() const noexcept { return bytes_used_; }
size_t TrackingMemoryResource::capacity() const noexcept { return capacity_; }

double TrackingMemoryResource::utilization() const noexcept {
    if (capacity_ == 0) return 1.0;
    return static_cast<double>(bytes_used_) / static_cast<double>(capacity_);
}

bool TrackingMemoryResource::is_soft_limit_reached() const noexcept {
    return utilization() >= SOFT_LIMIT_RATIO;
}

bool TrackingMemoryResource::is_hard_limit_reached() const noexcept {
    return bytes_used_ >= capacity_;
}

void* TrackingMemoryResource::do_allocate(size_t bytes, size_t alignment) {
    if (bytes_used_ + bytes > capacity_) {
        throw OomException(
            "(error) OOM command not allowed when used memory > 'maxmemory'");
    }
    // Upstream pool/monotonic may also throw if the raw buffer is exhausted
    try {
        void* p = upstream_->allocate(bytes, alignment);
        bytes_used_ += bytes;
        return p;
    } catch (const bad_alloc&) {
        throw OomException(
            "(error) OOM command not allowed when used memory > 'maxmemory'");
    }
}

void TrackingMemoryResource::do_deallocate(void* p, size_t bytes, size_t alignment) {
    upstream_->deallocate(p, bytes, alignment);
    bytes_used_ = (bytes_used_ >= bytes) ? (bytes_used_ - bytes) : 0;
}

bool TrackingMemoryResource::do_is_equal(
    const pmr::memory_resource& other) const noexcept {
    return this == &other;
}

// --- PmrArena ---

PmrArena::PmrArena(size_t capacity_bytes)
    : capacity_(capacity_bytes)
    , buffer_(capacity_bytes)
    , monotonic_(buffer_.data(), buffer_.size(), pmr::null_memory_resource())
    , pool_(pmr::pool_options{}, &monotonic_)
    , tracker_(&pool_, capacity_bytes) {}

pmr::memory_resource* PmrArena::resource() noexcept {
    return &tracker_;
}

size_t PmrArena::bytes_used() const noexcept { return tracker_.bytes_used(); }
size_t PmrArena::capacity() const noexcept { return capacity_; }
double PmrArena::utilization() const noexcept { return tracker_.utilization(); }

bool PmrArena::is_soft_limit_reached() const noexcept {
    return tracker_.is_soft_limit_reached();
}

bool PmrArena::is_hard_limit_reached() const noexcept {
    return tracker_.is_hard_limit_reached();
}

} // namespace kvstore
