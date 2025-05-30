#include <algorithm>
#include <cstdint>
#include <optional>
#include <string>
#include <tuple>
#include <unordered_map>
#include <vector>


#if __has_include(<catch2/catch_all.hpp>)
#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#else
#include <catch.hpp>
#endif

#include <filesystem>
#include <chrono>

#include "tests_config.hpp"

#include "sciqlop_cache/sciqlop_cache.hpp"


SCENARIO("Testing sciqlop_cache", "[cache]")
{
    GIVEN("a newly initialized cache")
    {
        std::string db_path = "test_cache.db";
        if (std::filesystem::exists(db_path))
            std::filesystem::remove(db_path);
        Cache cache(db_path, 1000);

        REQUIRE(cache.check_ == SQLITE_OK);

        WHEN("we test set and get") {
            REQUIRE(cache.set("key1", "hello-world"));
            REQUIRE(cache.set("key2", "banana"));
            auto value = cache.get("key1");
            auto value = cache.get("key2");
            REQUIRE(value.has_value());
            REQUIRE(value.value() == "hello-world");

            THEN("we test delete and clear") {
                REQUIRE(cache.delete("key1"));
                REQUIRE_FALSE(cache.get("key1").has_value());
                REQUIRE(cache.get("key2").has_value());
                cache.clear();
                REQUIRE_FALSE(cache.get("key2").has_value());
            }
        }

        WHEN("we test evict") {
            cache.set("key1", "hello-world", 0);
            cache.set("key2", "hello-world");
            cache.evict();
            REQUIRE_FALSE(cache.get("key1").has_value());
            REQUIRE(cache.get("key2").has_value());
        }

        WHEN("we test touch") {
            cache.set("key1", "hello-world");
            cache.touch("key1", 0);
            cache.evict();
            REQUIRE_FALSE(cache.get("key1").has_value());
        }

        WHEN("we test add") {
            cache.clear();
            cache.set("key1", "hello-world");
            cache.add("key1", "new-value");
            cache.add("key2", "new-value");
            auto value = cache.get("key1");
            REQUIRE(value.value() == "hello-world");
            auto value = cache.get("key2");
            REQUIRE(value.value() == "new-value");
        }

        WHEN("we test pop") {
            REQUIRE(cache.set("key_pop", "to-be-popped"));
            auto popped_value = cache.pop("key_pop");
            REQUIRE(popped_value.has_value());
            REQUIRE(popped_value.value() == "to-be-popped");
            REQUIRE_FALSE(cache.get("key_pop").has_value());
        }
        WHEN("we test expire") {
            cache.set("key1", "hello-world", 0);
            cache.set("key2", "hello-world");
            cache.expire();
            REQUIRE_FALSE(cache.get("key1").has_value());
            REQUIRE(cache.get("key2").has_value());
        }

        WHEN("we test stats") {
            cache.set("key1", "hello-world");
            cache.set("key2", "banana");
            auto stats = cache.stats();
            REQUIRE(stats.size == 2);
        }

        WHEN("we test check") {
            REQUIRE(cache.check() == true);
        }

        WHEN("we test cull") {
            for (int i = 0; i < 100; ++i) {
                cache.set("key" + std::to_string(i), "data");
            }
            cache.cull();
            auto stats = cache.stats();
            REQUIRE(stats.size <= 1000);
        }

        std::filesystem::remove(db_path);
    }
}
