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

#if __has_include(<catch2/catch_all.hpp>)
#include <catch2/catch_all.hpp>
// #include <catch2/catch_test_macros.hpp>
#else
#include <catch.hpp>
#endif
// #include "tests_config.hpp"

#include "../../include/sciqlop_cache/sciqlop_cache.hpp"
#include "../../include/sciqlop_cache/database.hpp"
#include "../include/utils/time.hpp"
using namespace std::chrono_literals;

SCENARIO("Testing time conversions", "[time]")
{
    GIVEN("a time point")
    {
        using namespace std::chrono_literals;

        WHEN("we test time conversions")
        {
            const auto now = std::chrono::floor<std::chrono::nanoseconds>(std::chrono::system_clock::now());
            REQUIRE(std::abs(time_point_to_epoch(now) - time_point_to_epoch(epoch_to_time_point(time_point_to_epoch(now)))) < 1e-6);
        }
    }
}

// test open, close and open again to test database persistence
SCENARIO("Testing sciqlop_cache", "[cache]")
{
    std::string db_path = "test_cache.db";
    if (std::filesystem::exists(db_path))
        std::filesystem::remove(db_path);

    std::string test_key = "random/test";
    std::vector<char> original_value1(128);
    std::generate(original_value1.begin(), original_value1.end(), std::rand);
    std::vector<char> original_value2(128);
    std::generate(original_value2.begin(), original_value2.end(), std::rand);

    GIVEN("a cache we'll open and close")
    {
        WHEN("We insert random data and close the cache")
        {
            {
                Cache cache(db_path, 1000);
                REQUIRE(cache.set(test_key, original_value1));
            }
            THEN("Data should persist after reopening")
            {
                Cache reopened_cache(db_path, 1000);
                REQUIRE(reopened_cache.check() == true);

                auto loaded_value = reopened_cache.get(test_key);
                REQUIRE(loaded_value.has_value());
                REQUIRE(loaded_value->size() == original_value1.size());
                REQUIRE(std::memcmp(
                            loaded_value->data(), original_value1.data(), original_value1.size())
                    == 0);
            }
            THEN("We can count the number of items in the cache")
            {
                Cache reopened_cache(db_path, 1000);
                REQUIRE(reopened_cache.check() == true);
                auto count = reopened_cache.count();
                REQUIRE(count == 1);
            }
        }
    }

    GIVEN("a newly initialized cache")
    {
        Cache cache(db_path, 1000);
        REQUIRE(cache.check() == true);

        WHEN("we test check")
        {
            REQUIRE(cache.check() == true);
        }

        WHEN("we test set and get")
        {
            REQUIRE(cache.set("key1", original_value1));
            REQUIRE(cache.set("key2", original_value2));
            auto value1 = cache.get("key1");
            auto value2 = cache.get("key2");
            REQUIRE(value1.has_value());
            REQUIRE(value1.value() == original_value1);
            REQUIRE(value2.has_value());
            REQUIRE(value2.value() == original_value2);

            THEN("We can count the number of items in the cache")
            {
                auto count = cache.count();
                REQUIRE(count == 2);
            }

            THEN("we test delete and clear")
            {
                REQUIRE(cache.del("key1"));
                REQUIRE_FALSE(cache.get("key1").has_value());
                REQUIRE(cache.get("key2").has_value());
                cache.clear();
                REQUIRE_FALSE(cache.get("key2").has_value());
            }
        }

        WHEN("we test evict")
        {
            cache.set("key1", original_value1, 0s);
            cache.set("key2", original_value1);
            cache.evict();
           //REQUIRE_FALSE(cache.get("key1").has_value()); // evict isn't made
            REQUIRE(cache.get("key2").has_value());
        }

        WHEN("we test touch")
        {
            cache.set("key1", original_value1);
            cache.touch("key1", 0s);
            cache.expire();
            REQUIRE_FALSE(cache.get("key1").has_value());
        }

        WHEN("we test add")
        {
            cache.clear();
            cache.set("key1", original_value1);
            REQUIRE_FALSE(cache.add("key1", original_value2));
            REQUIRE(cache.add("key2", original_value2));
            auto value1 = cache.get("key1");
            REQUIRE(value1.value() == original_value1);
            auto value2 = cache.get("key2");
            REQUIRE(value2.value() == original_value2);
        }

        WHEN("we test pop")
        {
            cache.set("key_pop", original_value1);
            auto popped_value = cache.pop("key_pop");
            REQUIRE(popped_value.has_value());
            REQUIRE(popped_value.value() == original_value1);
            REQUIRE_FALSE(cache.get("key_pop").has_value());
        }

        WHEN("we test expire")
        {
            cache.set("key1", original_value1, 0s);
            cache.set("key2", original_value1);
            REQUIRE(cache.get("key2").has_value());
            cache.expire();
            REQUIRE_FALSE(cache.get("key1").has_value());
            REQUIRE(cache.get("key2").has_value());
        }

        /*WHEN("we test stats") {
            cache.set("key1", original_value1);
            cache.set("key2", original_value2);
            auto stats = cache.stats();
            REQUIRE(stats.size == 2);
        }*/

        std::filesystem::remove(db_path);
    }
}
/*
SCENARIO("Testing sciqlop_cache with file storage fallback", "[cache][file]")
{
    std::string db_path = "test_cache_file.db";
    if (std::filesystem::exists(db_path))
        std::filesystem::remove(db_path);

    std::string large_key = "large/file_key";
    std::vector<char> large_value(1024);
    std::generate(large_value.begin(), large_value.end(), std::rand);

    GIVEN("a cache and a large value to trigger file fallback")
    {
        Cache cache(db_path, 1000);
        REQUIRE(cache.check());

        WHEN("we set a large value")
        {
            REQUIRE(cache.set(large_key, large_value));
            THEN("the value should be retrievable and match the original")
            {
                auto result = cache.get(large_key);
                REQUIRE(result.has_value());
                REQUIRE(result->size() == large_value.size());
                REQUIRE(result.value() == large_value);
            }

            THEN("we should not overwrite existing key with add()")
            {
                std::vector<char> other_value(1024);
                std::generate(other_value.begin(), other_value.end(), std::rand);
                REQUIRE_FALSE(cache.add(large_key, other_value));
                auto result = cache.get(large_key);
                REQUIRE(result.value() == large_value); // still original
            }

            THEN("pop should return the value and delete it")
            {
                auto popped = cache.pop(large_key);
                REQUIRE(popped.has_value());
                REQUIRE(popped.value() == large_value);
                REQUIRE_FALSE(cache.get(large_key).has_value());
            }

            THEN("delete should remove file-backed key")
            {
                REQUIRE(cache.del(large_key));
                REQUIRE_FALSE(cache.get(large_key).has_value());
            }

            THEN("clear should remove all including file-backed keys")
            {
                cache.clear();
                REQUIRE_FALSE(cache.get(large_key).has_value());
            }
        }

        WHEN("we test expiration of file-backed values")
        {
            REQUIRE(cache.set(large_key, large_value, 0s)); // immediately expired
            cache.expire();
            REQUIRE_FALSE(cache.get(large_key).has_value());
        }
    }

    std::filesystem::remove(db_path);
}
*/