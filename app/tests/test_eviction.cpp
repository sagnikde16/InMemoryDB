#include <gtest/gtest.h>
#include "daemons/timing_wheel.h"
#include "engine/hasher.h"
#include <chrono>
#include <memory_resource>

using namespace kvstore;

// B8: tombstoned key is cleaned up by timing wheel sweep
TEST(TimingWheelTest, TombstonedKeyCleanedUpOnSweep) {
    ShardRouter router(64 * 1024 * 1024, 64);
    TimingWheel wheel(router);

    router.set("key1", "value1", 86400);
    ASSERT_TRUE(router.exists("key1"));

    // DEL tombstones the key (B8: does NOT erase)
    router.del("key1");
    EXPECT_FALSE(router.exists("key1"));

    // Schedule and sweep — should physically erase the tombstoned entry
    auto now = std::chrono::steady_clock::now();
    wheel.schedule("key1", 0, now);
    wheel.advance_tick();

    auto& shard = router.shard_for_key("key1");
    std::shared_lock lock(shard.mutex());
    std::pmr::string lookup("key1", std::pmr::new_delete_resource());
    EXPECT_TRUE(shard.data().find(lookup) == shard.data().end());
}

// Re-SET key survives timing wheel sweep (not tombstoned, not expired)
TEST(TimingWheelTest, ReSetKeySurvivesSweep) {
    ShardRouter router(64 * 1024 * 1024, 64);
    TimingWheel wheel(router);

    router.set("key2", "original", 1);
    router.set("key2", "updated", 86400); // re-SET with long TTL

    auto now = std::chrono::steady_clock::now();
    wheel.schedule("key2", 0, now);
    wheel.advance_tick();

    // Key should survive — it was re-SET and is neither tombstoned nor expired
    EXPECT_TRUE(router.exists("key2"));
    auto val = router.get("key2");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ(*val, "updated");
}

TEST(TimingWheelTest, MultipleKeysInSameBucket) {
    ShardRouter router(64 * 1024 * 1024, 64);
    TimingWheel wheel(router);

    router.set("a", "1", 86400);
    router.set("b", "2", 86400);
    router.del("a");
    router.del("b");

    auto now = std::chrono::steady_clock::now();
    wheel.schedule("a", 0, now);
    wheel.schedule("b", 0, now);
    wheel.advance_tick();

    // Both tombstoned keys should be physically erased
    EXPECT_FALSE(router.exists("a"));
    EXPECT_FALSE(router.exists("b"));
}

// B8: DEL does NOT touch the timing wheel — no O(N) scan
TEST(TimingWheelTest, DelDoesNotPanicWithoutCancel) {
    ShardRouter router(64 * 1024 * 1024, 64);
    TimingWheel wheel(router);

    router.set("safe", "data", 86400);
    auto future = std::chrono::steady_clock::now() + std::chrono::seconds(100);
    wheel.schedule("safe", 0, future);

    // DEL just tombstones — no cancel() call needed
    router.del("safe");
    EXPECT_FALSE(router.exists("safe"));

    // Advance a few ticks — the scheduled entry is far in the future
    for (int i = 0; i < 5; i++) wheel.advance_tick();

    // Key should remain tombstoned but not yet swept (bucket is in the future)
    auto& shard = router.shard_for_key("safe");
    std::shared_lock lock(shard.mutex());
    std::pmr::string lookup("safe", std::pmr::new_delete_resource());
    auto it = shard.data().find(lookup);
    ASSERT_TRUE(it != shard.data().end());
    EXPECT_TRUE(it->second.tombstoned);
}
