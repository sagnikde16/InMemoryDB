#include <gtest/gtest.h>
#include "memory/variant_type.h"
#include "memory/pmr_allocator.h"
#include <thread>
#include <chrono>
#include <vector>

using namespace std;

using namespace kvstore;

// =====================================================================
// CacheAlignedValue — Alignment
// =====================================================================

TEST(MemoryAlignmentTest, SizeIsMultipleOf64) {
    EXPECT_EQ(0u, sizeof(CacheAlignedValue) % 64)
        << "CacheAlignedValue must be a multiple of 64 bytes";
}

TEST(MemoryAlignmentTest, AlignofIs64) {
    EXPECT_EQ(64u, alignof(CacheAlignedValue));
}

// =====================================================================
// CacheAlignedValue — Variant type construction
// =====================================================================

TEST(VariantTypeTest, StringConstruction) {
    pmr::string s("hello", pmr::new_delete_resource());
    CacheAlignedValue val(move(s));

    EXPECT_EQ(ValueType::String, val.type());
    EXPECT_EQ("hello", get<pmr::string>(val.data));
}

TEST(VariantTypeTest, IntegerConstruction) {
    CacheAlignedValue val(int64_t{42});
    EXPECT_EQ(ValueType::Integer, val.type());
    EXPECT_EQ(42, get<int64_t>(val.data));
}

TEST(VariantTypeTest, ListConstruction) {
    pmr::vector<pmr::string> list(pmr::new_delete_resource());
    list.emplace_back("one");
    list.emplace_back("two");
    CacheAlignedValue val(move(list));

    EXPECT_EQ(ValueType::List, val.type());
    EXPECT_EQ(2u, get<pmr::vector<pmr::string>>(val.data).size());
}

TEST(VariantTypeTest, HashConstruction) {
    pmr::unordered_map<pmr::string, pmr::string>
        map(pmr::new_delete_resource());
    map["field1"] = "value1";
    CacheAlignedValue val(move(map));

    EXPECT_EQ(ValueType::Hash, val.type());
    auto& stored = get<
        pmr::unordered_map<pmr::string, pmr::string>>(val.data);
    EXPECT_EQ(1u, stored.size());
}

// =====================================================================
// CacheAlignedValue — TTL and expiration
// =====================================================================

TEST(TtlTest, DefaultTtlIs24Hours) {
    CacheAlignedValue val(int64_t{1});
    EXPECT_EQ(86400, val.ttl_seconds);
}

TEST(TtlTest, CustomTtl) {
    CacheAlignedValue val(int64_t{1}, 300);
    EXPECT_EQ(300, val.ttl_seconds);
}

TEST(TtlTest, NoExpiryNeverExpires) {
    CacheAlignedValue val(int64_t{1}, -1);
    EXPECT_FALSE(val.is_expired());
}

TEST(TtlTest, NotExpiredImmediately) {
    CacheAlignedValue val(int64_t{1}, 86400);
    EXPECT_FALSE(val.is_expired());
}

TEST(TtlTest, ExpirationTimeMaxForNoExpiry) {
    CacheAlignedValue val(int64_t{1}, -1);
    EXPECT_EQ(chrono::steady_clock::time_point::max(), val.expiration_time());
}

// =====================================================================
// CacheAlignedValue — Memory estimation
// =====================================================================

TEST(MemoryEstimationTest, StringHasDynamicOverhead) {
    pmr::string s("test data payload", pmr::new_delete_resource());
    CacheAlignedValue val(move(s));
    EXPECT_GT(val.estimated_memory_usage(), sizeof(CacheAlignedValue));
}

TEST(MemoryEstimationTest, IntegerIsBaseOnly) {
    CacheAlignedValue val(int64_t{42});
    EXPECT_EQ(sizeof(CacheAlignedValue), val.estimated_memory_usage());
}

// =====================================================================
// CacheAlignedValue — Touch
// =====================================================================

TEST(TouchTest, UpdatesLastAccessed) {
    CacheAlignedValue val(int64_t{1});
    auto first = val.last_accessed;
    this_thread::sleep_for(chrono::milliseconds(15));
    val.touch();
    EXPECT_GT(val.last_accessed, first);
}

// =====================================================================
// PmrArena — Initial state
// =====================================================================

TEST(PmrArenaTest, InitialStateIsEmpty) {
    PmrArena arena(1024 * 1024);
    EXPECT_EQ(0u, arena.bytes_used());
    EXPECT_EQ(1024u * 1024u, arena.capacity());
    EXPECT_DOUBLE_EQ(0.0, arena.utilization());
    EXPECT_FALSE(arena.is_soft_limit_reached());
    EXPECT_FALSE(arena.is_hard_limit_reached());
}

// =====================================================================
// PmrArena — Allocation tracking
// =====================================================================

TEST(PmrArenaTest, AllocateAndDeallocateTracksBytes) {
    PmrArena arena(1024 * 1024);
    auto* res = arena.resource();

    void* p = res->allocate(512, 8);
    EXPECT_EQ(512u, arena.bytes_used());

    res->deallocate(p, 512, 8);
    EXPECT_EQ(0u, arena.bytes_used());
}

TEST(PmrArenaTest, MultipleAllocationsAccumulate) {
    PmrArena arena(1024 * 1024);
    auto* res = arena.resource();

    void* p1 = res->allocate(256, 8);
    void* p2 = res->allocate(256, 8);
    EXPECT_EQ(512u, arena.bytes_used());

    res->deallocate(p1, 256, 8);
    EXPECT_EQ(256u, arena.bytes_used());

    res->deallocate(p2, 256, 8);
    EXPECT_EQ(0u, arena.bytes_used());
}

// =====================================================================
// PmrArena — Soft limit (85%)
// =====================================================================

TEST(PmrArenaTest, SoftLimitTriggersAt85Percent) {
    constexpr size_t capacity = 10000;
    pmr::monotonic_buffer_resource mono(capacity * 2); // huge underlying buffer
    TrackingMemoryResource tracker(&mono, capacity);
    auto* res = &tracker;

    // Allocate chunks until we cross 85%
    vector<void*> ptrs;
    size_t allocated = 0;
    while (allocated < static_cast<size_t>(capacity * 0.85)) {
        constexpr size_t chunk = 100;
        if (allocated + chunk > capacity) break;
        ptrs.push_back(res->allocate(chunk, 8));
        allocated += chunk;
    }

    EXPECT_TRUE(tracker.is_soft_limit_reached());
    EXPECT_FALSE(tracker.is_hard_limit_reached());
}

// =====================================================================
// PmrArena — Hard limit (OOM)
// =====================================================================

TEST(PmrArenaTest, HardLimitThrowsOomException) {
    constexpr size_t capacity = 4096;
    PmrArena arena(capacity);
    auto* res = arena.resource();

    EXPECT_THROW(res->allocate(capacity + 1, 8), OomException);
}

TEST(PmrArenaTest, OomMessageContainsErrorString) {
    PmrArena arena(1024);
    auto* res = arena.resource();

    try {
        res->allocate(2048, 8);
        FAIL() << "Expected OomException";
    } catch (const OomException& e) {
        EXPECT_NE(string::npos, string(e.what()).find("OOM"));
    }
}

// =====================================================================
// PmrArena — PMR containers work with arena resource
// =====================================================================

TEST(PmrArenaTest, PmrStringUsesArena) {
    PmrArena arena(1024 * 1024);
    auto* res = arena.resource();

    pmr::string s("Hello from PMR arena!", res);
    EXPECT_EQ("Hello from PMR arena!", s);
    EXPECT_GT(arena.bytes_used(), 0u);
}

TEST(PmrArenaTest, PmrVectorUsesArena) {
    PmrArena arena(1024 * 1024);
    auto* res = arena.resource();

    pmr::vector<pmr::string> vec(res);
    vec.emplace_back("item1");
    vec.emplace_back("item2");
    vec.emplace_back("item3");

    EXPECT_EQ(3u, vec.size());
    EXPECT_GT(arena.bytes_used(), 0u);
}
