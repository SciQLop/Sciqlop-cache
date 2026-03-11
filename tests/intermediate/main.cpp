#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <optional>
#include <random>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../common.hpp"
#include "sciqlop_cache/database.hpp"
#include "sciqlop_cache/sciqlop_cache.hpp"
#include "sciqlop_cache/utils/time.hpp"
using namespace std::chrono_literals;
#include <cpp_utils/lifetime/scope_leaving_guards.hpp>

SCENARIO("Limit testing sciqlop_cache", "[cache]")
{
    AutoCleanDirectory db_path { "LimitTest01" };
    Cache cache(db_path.path());
    auto scope_guard = cpp_utils::lifetime::scope_leaving_guard<
        Cache, [](Cache* c) { std::filesystem::remove_all(c->path()); }>(&cache);

    std::string test_key = "random/test";
    std::vector<char> original_value1(128);
    std::generate(original_value1.begin(), original_value1.end(), std::rand);
    std::vector<char> original_value2(128);
    std::generate(original_value2.begin(), original_value2.end(), std::rand);

    GIVEN("Empty key and empty value")
    {
        std::vector<char> empty_data;
        std::string empty_str = "";
        std::string test_value = "test_value";

        WHEN("We try to set an empty key")
        {
            REQUIRE(cache.set(empty_str, test_value));
            auto value = cache.get(empty_str);
            REQUIRE(value.has_value());
            REQUIRE(std::string(value->data(), value->data() + value->size()) == test_value);
        }

        WHEN("We try to set a key with empty value")
        {
            REQUIRE(cache.set(test_value, empty_data));
            auto loaded = cache.get(test_value);
            REQUIRE(loaded.has_value()); // fails because empty vectors are treated as no value
            REQUIRE(loaded->size() == 0);
        }
    }

    GIVEN("Items with immediate expiry")
    {
        std::vector<char> value(100, 'a');
        cache.set("will_expire", value, 0s);
        cache.expire();

        WHEN("We retrieve immediately after setting with 0s expiry")
        {
            REQUIRE_FALSE(cache.get("will_expire").has_value());
        }
    }

    GIVEN("A corrupt DB file")
    {
        AutoCleanDirectory db_path { "CorruptDBTest01" };
        std::ofstream corrupt(db_path.path() / Cache::db_fname, std::ios::binary);
        corrupt << "NOT A REAL SQLITE FILE";
        corrupt.close();

        THEN("Cache initialization should throw")
        {
            REQUIRE_THROWS_AS(
                [&]()
                {
                    Cache c(db_path.path(), 1000);
                    c.opened();
                }(),
                std::runtime_error);
        }
    }

    GIVEN("A cache with max_size 0")
    {
        Cache cache(db_path.path(), 0);
        std::vector<char> value(100, 'x');

        WHEN("We try to add an item")
        {
            REQUIRE(cache.set("key", value));
            REQUIRE(cache.count() == 1);
        }
    }
}

SCENARIO("User transaction with large values does not cause nested transaction error", "[transaction]")
{
    AutoCleanDirectory db_path { "TxnLargeValueTest01" };
    Cache cache(db_path.path());

    // Value larger than 8KB threshold to trigger file storage path
    std::vector<char> large_value(16 * 1024, 'L');
    std::vector<char> small_value(100, 'S');

    GIVEN("a user transaction")
    {
        WHEN("we set a large value inside the transaction")
        {
            auto txn = cache.begin_user_transaction();
            REQUIRE_NOTHROW(cache.set("large_key", large_value));
            REQUIRE_NOTHROW(cache.set("small_key", small_value));
            txn.commit();

            THEN("both values are retrievable")
            {
                auto large_result = cache.get("large_key");
                REQUIRE(large_result.has_value());
                REQUIRE(large_result->size() == large_value.size());

                auto small_result = cache.get("small_key");
                REQUIRE(small_result.has_value());
                REQUIRE(small_result->size() == small_value.size());
            }
        }

        WHEN("we set multiple large values inside the transaction")
        {
            auto txn = cache.begin_user_transaction();
            for (int i = 0; i < 5; ++i)
                REQUIRE_NOTHROW(cache.set("batch_" + std::to_string(i), large_value));
            txn.commit();

            THEN("all values are retrievable and count is correct")
            {
                REQUIRE(cache.count() == 5);
                for (int i = 0; i < 5; ++i)
                {
                    auto r = cache.get("batch_" + std::to_string(i));
                    REQUIRE(r.has_value());
                    REQUIRE(r->size() == large_value.size());
                }
            }
        }
    }
}

SCENARIO("LRU eviction enforces max_size in bytes", "[eviction]")
{
    AutoCleanDirectory db_path { "EvictionTest01" };
    std::vector<char> value(100, 'v');

    GIVEN("a cache with max_size = 350 bytes (fits ~3 entries of 100 bytes)")
    {
        Cache cache(db_path.path(), 350);

        WHEN("we insert 5 entries and access some to affect LRU order")
        {
            cache.set("k1", value);
            cache.set("k2", value);
            cache.set("k3", value);

            REQUIRE(cache.count() == 3);
            REQUIRE(cache.size() == 300);

            // Access k1 to make it recently used
            cache.get("k1");

            cache.set("k4", value);

            // Eviction is deferred to background thread; call manually for test
            cache.evict();

            THEN("the least recently used entry is evicted")
            {
                // k2 is LRU (k1 was refreshed by get), so k2 gets evicted
                REQUIRE_FALSE(cache.exists("k2"));
                REQUIRE(cache.exists("k1"));
                REQUIRE(cache.exists("k3"));
                REQUIRE(cache.exists("k4"));
                REQUIRE(cache.count() == 3);
            }
        }
    }

    GIVEN("a cache with max_size large enough for all entries")
    {
        Cache cache(db_path.path(), 10000);

        WHEN("we insert entries within the limit")
        {
            cache.set("a", value);
            cache.set("b", value);

            THEN("no eviction occurs")
            {
                REQUIRE(cache.count() == 2);
                REQUIRE(cache.exists("a"));
                REQUIRE(cache.exists("b"));
            }
        }
    }
}
