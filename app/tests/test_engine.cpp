#include <gtest/gtest.h>
#include "engine/hasher.h"
#include <string>
#include <thread>
#include <vector>
#include <set>
#include <mutex>

using namespace std;

using namespace kvstore;

// =====================================================================
// MurmurHash3
// =====================================================================

TEST(HasherTest, DeterministicHash) {
    auto h1 = murmur_hash3("test_key");
    auto h2 = murmur_hash3("test_key");
    EXPECT_EQ(h1, h2);
}

TEST(HasherTest, DifferentKeysProduceDifferentHashes) {
    auto h1 = murmur_hash3("key_a");
    auto h2 = murmur_hash3("key_b");
    EXPECT_NE(h1, h2);
}

TEST(HasherTest, EmptyStringHashes) {
    auto h = murmur_hash3("");
    // Just verify it doesn't crash and produces a value
    EXPECT_EQ(h, murmur_hash3(""));
}

// =====================================================================
// ShardRouter — Routing
// =====================================================================

TEST(ShardRouterTest, ShardCountMatches) {
    ShardRouter router(64 * 1024 * 1024, 64);  // 64MB, 64 shards
    EXPECT_EQ(64u, router.shard_count());
}

TEST(ShardRouterTest, DeterministicRouting) {
    ShardRouter router(64 * 1024 * 1024, 64);
    auto idx1 = router.shard_index("mykey");
    auto idx2 = router.shard_index("mykey");
    EXPECT_EQ(idx1, idx2);
    EXPECT_LT(idx1, 64u);
}

TEST(ShardRouterTest, DistributesAcrossShards) {
    ShardRouter router(64 * 1024 * 1024, 64);
    set<size_t> seen_shards;
    for (int i = 0; i < 1000; i++) {
        seen_shards.insert(router.shard_index("key_" + to_string(i)));
    }
    // With 1000 random keys, expect reasonable distribution across 64 shards
    EXPECT_GT(seen_shards.size(), 30u);
}

// =====================================================================
// ShardRouter — CRUD operations
// =====================================================================

TEST(ShardRouterTest, SetAndGet) {
    ShardRouter router(64 * 1024 * 1024, 64);
    EXPECT_TRUE(router.set("hello", "world"));
    auto val = router.get("hello");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ("world", *val);
}

TEST(ShardRouterTest, GetMissingKeyReturnsNullopt) {
    ShardRouter router(64 * 1024 * 1024, 64);
    EXPECT_FALSE(router.get("nonexistent").has_value());
}

TEST(ShardRouterTest, DeleteKey) {
    ShardRouter router(64 * 1024 * 1024, 64);
    router.set("temp", "data");
    EXPECT_TRUE(router.del("temp"));
    EXPECT_FALSE(router.get("temp").has_value());
}

TEST(ShardRouterTest, DeleteNonexistentReturnsFalse) {
    ShardRouter router(64 * 1024 * 1024, 64);
    EXPECT_FALSE(router.del("ghost"));
}

TEST(ShardRouterTest, ExistsCheck) {
    ShardRouter router(64 * 1024 * 1024, 64);
    router.set("present", "yes");
    EXPECT_TRUE(router.exists("present"));
    EXPECT_FALSE(router.exists("absent"));
}

TEST(ShardRouterTest, OverwriteExistingKey) {
    ShardRouter router(64 * 1024 * 1024, 64);
    router.set("key", "first");
    router.set("key", "second");
    auto val = router.get("key");
    ASSERT_TRUE(val.has_value());
    EXPECT_EQ("second", *val);
}

// =====================================================================
// Concurrent access — 50 threads blasting read/writes
// =====================================================================

TEST(ShardRouterTest, ConcurrentReadWrite) {
    ShardRouter router(128 * 1024 * 1024, 64);
    constexpr int THREADS = 50;
    constexpr int OPS_PER_THREAD = 200;

    vector<thread> threads;
    mutex error_mutex;
    vector<string> errors;

    for (int t = 0; t < THREADS; t++) {
        threads.emplace_back([&, t]() {
            for (int i = 0; i < OPS_PER_THREAD; i++) {
                string key = "t" + to_string(t) + "_k" + to_string(i);
                string val = "v" + to_string(i);

                if (!router.set(key, val)) {
                    lock_guard lg(error_mutex);
                    errors.push_back("SET failed for " + key);
                    continue;
                }

                auto result = router.get(key);
                if (!result.has_value() || *result != val) {
                    lock_guard lg(error_mutex);
                    errors.push_back("GET mismatch for " + key);
                }
            }
        });
    }

    for (auto& t : threads) t.join();

    EXPECT_TRUE(errors.empty())
        << "First error: " << (errors.empty() ? "none" : errors[0]);
}
