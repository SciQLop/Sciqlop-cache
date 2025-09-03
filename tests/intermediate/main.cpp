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

#include "sciqlop_cache/sciqlop_cache.hpp"
#include "sciqlop_cache/database.hpp"
#include "sciqlop_cache/utils/time.hpp"
#include "../common.hpp"
using namespace std::chrono_literals;
#include <cpp_utils/lifetime/scope_leaving_guards.hpp>

SCENARIO("Limit testing sciqlop_cache", "[cache]")
{
    AutoCleanDirectory db_path{ "LimitTest01"};
    Cache cache(db_path.path(), 1000);
    auto scope_guard = cpp_utils::lifetime::scope_leaving_guard<Cache, [](Cache* c) { std::filesystem::remove_all(c->path()); }>(&cache);

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

    GIVEN("Items with immediate expiry") {
        std::vector<char> value(100, 'a');
        cache.set("will_expire", value, 0s);
        cache.expire();

        WHEN("We retrieve immediately after setting with 0s expiry") {
            REQUIRE_FALSE(cache.get("will_expire").has_value());
        }
    }

    GIVEN("A corrupt DB file") {
        AutoCleanDirectory db_path { "CorruptDBTest01"};
        std::ofstream corrupt(db_path.path()/Cache::db_fname, std::ios::binary);
        corrupt << "NOT A REAL SQLITE FILE";
        corrupt.close();

        THEN("Cache initialization should throw") {
            REQUIRE_THROWS_AS(Cache(db_path.path(), 1000), std::runtime_error);
        }
    }

    GIVEN("A cache with max_size 0") {
        Cache cache(db_path.path(), 0);
        std::vector<char> value(100, 'x');

        WHEN("We try to add an item") {
            REQUIRE(cache.set("key", value));
            REQUIRE(cache.count() == 1);
        }
    }
}
