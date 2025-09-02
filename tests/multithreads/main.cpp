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
#include <thread>

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
#include <cpp_utils/lifetime/scope_leaving_guards.hpp>


void read_write_cache(std::filesystem::path db_path, const std::string& key, const std::vector<char>& value, int iterations)
{
    try {
        Cache cache(db_path, 1000);
        REQUIRE(cache.check() == true);

        for (int i = 0; i < iterations; ++i) {
            // Write to cache
            REQUIRE(cache.set(key, value));
            // Read from cache
            auto loaded_value = cache.get(key);
            REQUIRE(loaded_value.has_value());
            REQUIRE(loaded_value->size() == value.size());
            REQUIRE(std::memcmp(loaded_value->data(), value.data(), value.size()) == 0);
        }
    } catch (const std::exception& e) {
        FAIL("Exception in thread: " << e.what());
    }
}


SCENARIO("Testing time conversions", "[time]")
{
    std::vector<std::thread> threads;
    std::filesystem::path db_path = std::filesystem::temp_directory_path() / "MultiThreadTest01";
    std::string test_key = "random/test";
    std::vector<char> original_value(128);
    std::generate(original_value.begin(), original_value.end(), std::rand);
    int thread_count = std::thread::hardware_concurrency()*2;
    int iterations_per_thread = 1000;
    auto scope_guard = cpp_utils::lifetime::scope_leaving_guard<std::filesystem::path, [](std::filesystem::path* p) { std::filesystem::remove_all(*p); }>(&db_path);

    GIVEN("a cache accessed by multiple threads")
    {
        WHEN("multiple threads read and write to the cache concurrently")
        {
            for (int i = 0; i < thread_count; ++i) {
                threads.emplace_back(read_write_cache, db_path, test_key + std::to_string(i), original_value, iterations_per_thread);
            }

            for (auto& t : threads) {
                if (t.joinable()) {
                    t.join();
                }
            }

            THEN("the cache should remain consistent and correct")
            {
                Cache final_cache(db_path, 1000);
                REQUIRE(final_cache.check() == true);

                for (int i = 0; i < thread_count; ++i) {
                    auto loaded_value = final_cache.get(test_key + std::to_string(i));
                    REQUIRE(loaded_value.has_value());
                    REQUIRE(loaded_value->size() == original_value.size());
                    REQUIRE(std::memcmp(loaded_value->data(), original_value.data(), original_value.size()) == 0);
                }
            }
        }
    }
}
