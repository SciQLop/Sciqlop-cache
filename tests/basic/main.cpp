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
    std::vector<uint8_t> original_value1(128);
    std::generate(original_value1.begin(), original_value1.end(), std::rand);
    std::string original_str1(original_value1.begin(), original_value1.end());
    std::vector<uint8_t> original_value2(128);
    std::generate(original_value2.begin(), original_value2.end(), std::rand);
    std::string original_str2(original_value2.begin(), original_value2.end());

    GIVEN("a cache we'll open and close")
    {


        WHEN("We insert random data and close the cache")
        {
            {
                Cache cache(db_path, 1000);
                REQUIRE(cache.set(test_key, original_str1));
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
            REQUIRE(cache.set("key1", original_str1));
            REQUIRE(cache.set("key2", original_str2));
            auto value1 = cache.get("key1");
            auto value2 = cache.get("key2");
            REQUIRE(value1.has_value());
            REQUIRE(value1.value() == original_value1);
            REQUIRE(value2.has_value());
            REQUIRE(value2.value() == original_value2);

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
            cache.set("key1", original_str1, 0s);
            cache.set("key2", original_str1);
            cache.evict();
            REQUIRE_FALSE(cache.get("key1").has_value()); // evict isn't made
            REQUIRE(cache.get("key2").has_value());
        }

        WHEN("we test touch")
        {
            cache.set("key1", original_str1);
            cache.touch("key1", 0s);
            cache.expire();
            REQUIRE_FALSE(cache.get("key1").has_value());
        }

        WHEN("we test add")
        {
            cache.clear();
            REQUIRE(cache.set("key1", original_str1));
            REQUIRE_FALSE(cache.add("key1", original_str2));
            REQUIRE(cache.add("key2", original_str2));
            auto value1 = cache.get("key1");
            REQUIRE(value1.value() == original_value1);
            auto value2 = cache.get("key2");
            REQUIRE(value2.value() == original_value2);
        }

        WHEN("we test pop")
        {
            REQUIRE(cache.set("key_pop", original_str1));
            auto popped_value = cache.pop("key_pop");
            REQUIRE(popped_value.has_value());
            REQUIRE(popped_value.value() == original_value1);
            REQUIRE_FALSE(cache.get("key_pop").has_value());
        }

        WHEN("we test expire")
        {
            cache.set("key1", original_str1, 0s);
            cache.set("key2", original_str1);
            REQUIRE(cache.get("key2").has_value());
            cache.expire();
            REQUIRE_FALSE(cache.get("key1").has_value());
            REQUIRE(cache.get("key2").has_value());
        }

        /*WHEN("we test stats") {
            cache.set("key1", original_str1);
            cache.set("key2", original_str2);
            auto stats = cache.stats();
            REQUIRE(stats.size == 2);
        }*/

        std::filesystem::remove(db_path);
    }
}
