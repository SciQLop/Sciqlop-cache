#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../common.hpp"
#include "sciqlop_cache/database.hpp"
#include "sciqlop_cache/sciqlop_cache.hpp"
#include "sciqlop_cache/utils/time.hpp"

void read_write_cache(std::filesystem::path db_path, const std::string& key,
                      const std::vector<char>& value, int iterations)
{
    try
    {
        Cache cache(db_path);
        REQUIRE(cache.check() == true);

        for (int i = 0; i < iterations; ++i)
        {
            REQUIRE(cache.set(key, value));
            auto loaded_value = cache.get(key);
            REQUIRE(loaded_value.has_value());
            REQUIRE(loaded_value->size() == value.size());
            REQUIRE(std::memcmp(loaded_value->data(), value.data(), value.size()) == 0);
        }
    }
    catch (const std::exception& e)
    {
        FAIL("Exception in thread: " << e.what());
    }
}

void read_write_shared_cache(Cache& cache, const std::string& key,
                             const std::vector<char>& value, int iterations)
{
    try
    {
        for (int i = 0; i < iterations; ++i)
        {
            REQUIRE(cache.set(key, value));
            auto loaded_value = cache.get(key);
            REQUIRE(loaded_value.has_value());
            REQUIRE(loaded_value->size() == value.size());
            REQUIRE(std::memcmp(loaded_value->data(), value.data(), value.size()) == 0);
        }
    }
    catch (const std::exception& e)
    {
        FAIL("Exception in thread: " << e.what());
    }
}

SCENARIO("Testing time conversions", "[time]")
{
    std::vector<std::thread> threads;
    AutoCleanDirectory db_path { "MultiThreadTest" };
    std::string test_key = "random/test";
    std::vector<char> original_value(128);
    std::generate(original_value.begin(), original_value.end(), std::rand);
    int thread_count = std::thread::hardware_concurrency() * 2;
    int iterations_per_thread = 1000;

    GIVEN("a cache accessed by multiple threads")
    {
        WHEN("multiple threads read and write to the cache concurrently")
        {
            for (int i = 0; i < thread_count; ++i)
            {
                threads.emplace_back(read_write_cache, db_path.path(), test_key + std::to_string(i),
                                     original_value, iterations_per_thread);
            }

            for (auto& t : threads)
            {
                if (t.joinable())
                {
                    t.join();
                }
            }

            THEN("the cache should remain consistent and correct")
            {
                Cache final_cache(db_path.path(), 1000);
                REQUIRE(final_cache.check() == true);

                for (int i = 0; i < thread_count; ++i)
                {
                    auto loaded_value = final_cache.get(test_key + std::to_string(i));
                    REQUIRE(loaded_value.has_value());
                    REQUIRE(loaded_value->size() == original_value.size());
                    REQUIRE(std::memcmp(loaded_value->data(), original_value.data(),
                                        original_value.size())
                            == 0);
                }
            }
        }
    }
}

SCENARIO("Shared cache instance across threads", "[threads]")
{
    AutoCleanDirectory db_path { "SharedCacheTest" };
    std::vector<char> original_value(128);
    std::generate(original_value.begin(), original_value.end(), std::rand);
    int thread_count = std::thread::hardware_concurrency() * 2;
    int iterations_per_thread = 1000;

    GIVEN("a single cache instance shared by multiple threads")
    {
        Cache shared_cache(db_path.path());

        WHEN("multiple threads read and write concurrently")
        {
            std::vector<std::thread> threads;
            for (int i = 0; i < thread_count; ++i)
            {
                threads.emplace_back(read_write_shared_cache, std::ref(shared_cache),
                                     "key/" + std::to_string(i), original_value,
                                     iterations_per_thread);
            }

            for (auto& t : threads)
                t.join();

            THEN("all keys should be present and correct")
            {
                for (int i = 0; i < thread_count; ++i)
                {
                    auto loaded_value = shared_cache.get("key/" + std::to_string(i));
                    REQUIRE(loaded_value.has_value());
                    REQUIRE(loaded_value->size() == original_value.size());
                    REQUIRE(std::memcmp(loaded_value->data(), original_value.data(),
                                        original_value.size())
                            == 0);
                }
            }
        }
    }
}
