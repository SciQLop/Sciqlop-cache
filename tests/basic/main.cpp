#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <cstring>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <filesystem>
#include <chrono>
#include <random>

#if __has_include(<catch2/catch_all.hpp>)
    #include <catch2/catch_all.hpp>
//#include <catch2/catch_test_macros.hpp>
#else
    #include <catch.hpp>
#endif
//#include "tests_config.hpp"

#include "../include/sciqlop_cache/sciqlop_cache.hpp"
#include "../include/utils/time.hpp"

SCENARIO("Testing time conversions", "[time]")
{
    GIVEN("a time point")
    {
        using namespace std::chrono_literals;
        const auto now = std::chrono::system_clock::now();

        WHEN("we test time conversions") {
            std::time_t epoch_1 = time_to_epoch(now);
            double double_ = epoch_to_double(epoch_1);
            std::time_t epoch_2 = double_to_epoch(double_);
            THEN("the epoch should be consistent")
                REQUIRE(epoch_1 == epoch_2);

            std::chrono::system_clock::time_point time_ = epoch_to_time(epoch_1);
            THEN("the time point should be consistent") {
                REQUIRE(std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count()
                == std::chrono::duration_cast<std::chrono::seconds>(time_.time_since_epoch()).count());
            }
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
    std::vector<uint8_t> original_value(128);
    std::generate(original_value.begin(), original_value.end(), std::rand);
    std::string original_str1(original_value.begin(), original_value.end());
    std::generate(original_value.begin(), original_value.end(), std::rand);
    std::string original_str2(original_value.begin(), original_value.end());

    GIVEN("a cache we'll open and close")
    {
        Cache cache(db_path, 1000);
        REQUIRE(cache.check() == true);

        WHEN("We insert random data and close the cache") {
            REQUIRE(cache.set(test_key, original_str1));
            cache.db.close();

            THEN("Data should persist after reopening") {
                Cache reopened_cache(db_path, 1000);
                REQUIRE(reopened_cache.check() == true);

                auto loaded_value = reopened_cache.get(test_key);
                REQUIRE(loaded_value.has_value());
                REQUIRE(loaded_value->size() == original_value.size());
                REQUIRE(std::memcmp(loaded_value->data(), original_value.data(), original_value.size()) == 0);
            }
        }
    }

    GIVEN("a newly initialized cache")
    {
        Cache cache(db_path, 1000);
        REQUIRE(cache.check() == true);

        WHEN("we test check") {
            REQUIRE(cache.check() == true);
        }

        WHEN("we test set and get") {
            REQUIRE(cache.set("key1", original_str1));
            REQUIRE(cache.set("key2", original_str2));
            auto value1 = cache.get("key1");
            auto value2 = cache.get("key2");
            REQUIRE(value1.has_value());
            REQUIRE(value1.value() == original_str1);
            REQUIRE(value2.has_value());
            REQUIRE(value2.value() == original_str2);

            THEN("we test delete and clear") {
                REQUIRE(cache.del("key1"));
                REQUIRE_FALSE(cache.get("key1").has_value());
                REQUIRE(cache.get("key2").has_value());
                cache.clear();
                REQUIRE_FALSE(cache.get("key2").has_value());
            }
        }

        WHEN("we test evict") {
            cache.set("key1", original_str1, 0);
            cache.set("key2", original_str1);
            cache.evict();
            REQUIRE_FALSE(cache.get("key1").has_value());
            REQUIRE(cache.get("key2").has_value());
        }

        WHEN("we test touch") {
            cache.set("key1", original_str1);
            cache.touch("key1", 0);
            cache.evict();
            REQUIRE_FALSE(cache.get("key1").has_value());
        }

        WHEN("we test add") {
            cache.clear();
            REQUIRE(cache.set("key1", original_str1));
            REQUIRE_FALSE(cache.add("key1", original_str2));
            REQUIRE(cache.add("key2", original_str2));
            auto value1 = cache.get("key1");
            REQUIRE(value1.value() == original_str1);
            auto value2 = cache.get("key2");
            REQUIRE(value2.value() == original_str2);
        }

        WHEN("we test pop") {
            REQUIRE(cache.set("key_pop", original_str1));
            auto popped_value = cache.pop("key_pop");
            REQUIRE(popped_value.has_value());
            REQUIRE(popped_value.value() == original_str1);
            REQUIRE_FALSE(cache.get("key_pop").has_value());
        }

        WHEN("we test expire") {
            cache.set("key1", original_str1, 0);
            cache.set("key2", original_str1);
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
