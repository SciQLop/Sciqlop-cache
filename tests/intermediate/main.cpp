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

SCENARIO("Limit testing sciqlop_cache", "[cache]")
{
    std::filesystem::path db_path = std::filesystem::temp_directory_path() / "LimitTest01";
    if (std::filesystem::exists(db_path))
        std::filesystem::remove_all(db_path);
    Cache cache(db_path, 1000);

    std::string test_key = "random/test";
    std::vector<char> original_value1(128);
    std::generate(original_value1.begin(), original_value1.end(), std::rand);
    std::vector<char> original_value2(128);
    std::generate(original_value2.begin(), original_value2.end(), std::rand);

    GIVEN("Empty key and empty value") {
        std::vector<char> empty_data;
        std::string empty_str = "";
        std::string test_value = "test_value";

        WHEN("We try to set an empty key") {
            REQUIRE(cache.set(empty_str, test_value));
            auto value = cache.get(empty_str);
            REQUIRE(value.has_value());
            REQUIRE(std::string(value->data(), value->data()+value->size()) == test_value);
        }

        WHEN("We try to set a key with empty value") {
            REQUIRE(cache.set(test_value, empty_data));
            auto loaded = cache.get(test_value);
            REQUIRE(loaded.has_value()); // fails because empty vectors are treated as no value
            REQUIRE(loaded->size()==0);
        }
    }

    GIVEN("A read-only .cache/ directory") {
        std::filesystem::remove_all(".readonly_cache");
        std::filesystem::create_directory(".readonly_cache");

        Cache cache(".readonly_cache/", 1000);

        std::vector<char> large_data(600, 'A');

        std::filesystem::permissions(".readonly_cache",
                                     std::filesystem::perms::owner_read,
                                     std::filesystem::perm_options::replace);

        WHEN("Trying to store a large value") {
            REQUIRE_FALSE(cache.set("some_key", large_data));
        }

        std::filesystem::permissions(".readonly_cache",
            std::filesystem::perms::owner_all,
            std::filesystem::perm_options::replace);
        std::filesystem::remove_all(".readonly_cache");
    }

    GIVEN("Items with immediate expiry") {
        std::vector<char> value(100, 'a');
        cache.set("will_expire", value, 0s);
        cache.expire();

        WHEN("We retrieve immediately after setting with 0s expiry") {
            REQUIRE_FALSE(cache.get("will_expire").has_value());
        }
    }

    GIVEN("A corrupt DB file") {
        std::ofstream corrupt(db_path/Cache::db_fname, std::ios::binary);
        corrupt << "NOT A REAL SQLITE FILE";
        corrupt.close();

        THEN("Cache initialization should throw") {
            REQUIRE_THROWS_AS(Cache(db_path, 1000), std::runtime_error);
        }

        std::filesystem::remove_all(db_path);
    }

    GIVEN("A cache with max_size 0") {
        Cache cache(db_path, 0);
        std::vector<char> value(100, 'x');

        WHEN("We try to add an item") {
            REQUIRE(cache.set("key", value));
            REQUIRE(cache.count() == 1);
        }
    }

    std::filesystem::remove_all(db_path);
}
