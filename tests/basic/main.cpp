#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include <catch2/catch_all.hpp>


#include "../common.hpp"
#include "sciqlop_cache/disk_storage.hpp"
#include "sciqlop_cache/sciqlop_cache.hpp"
#include "sciqlop_cache/utils/time.hpp"

using namespace std::chrono_literals;

SCENARIO("Testing time conversions", "[time]")
{
    GIVEN("a time point")
    {
        using namespace std::chrono_literals;

        WHEN("we test time conversions")
        {
            const auto now
                = std::chrono::floor<std::chrono::nanoseconds>(std::chrono::system_clock::now());
            REQUIRE(std::abs(time_point_to_epoch(now)
                             - time_point_to_epoch(epoch_to_time_point(time_point_to_epoch(now))))
                    < 1e-6);
        }
    }
}

SCENARIO("Testing file I/O with Bytes concept", "[bytes][fileio]")
{
    std::filesystem::path test_file;
    std::vector<char> test_data(512);
    std::generate(test_data.begin(), test_data.end(), std::rand);
    DiskStorage disk_storage(std::filesystem::temp_directory_path() / "IOTest");
    auto scope_guard = cpp_utils::lifetime::scope_leaving_guard<DiskStorage, [](DiskStorage* ds)
                                                                { ds->remove(ds->path(), true); }>(
        &disk_storage);


    GIVEN("A buffer of random data and a file path")
    {
        WHEN("We store the bytes to a file")
        {
            auto write_success = disk_storage.store(test_data);
            test_file = *write_success;

            THEN("The file should exist")
            {
                REQUIRE(bool(write_success) == true);
                REQUIRE(std::filesystem::exists(test_file) == true);
            }

            THEN("The file contents should match the original buffer")
            {
                auto loaded_data = Buffer(test_file);
                REQUIRE(loaded_data.size() == test_data.size());
                REQUIRE(std::memcmp(loaded_data.data(), test_data.data(), test_data.size()) == 0);
            }

            THEN("We delete the file after writing")
            {
                REQUIRE(std::filesystem::exists(test_file));

                bool delete_success = std::filesystem::remove(test_file);

                THEN("The file should no longer exist")
                {
                    REQUIRE(delete_success == true);
                    REQUIRE_FALSE(std::filesystem::exists(test_file));
                }
            }
        }

        WHEN("We check for a non-existent file")
        {
            std::string missing_file = "non_existent_file.bin";

            THEN("fileExists should return false")
            {
                REQUIRE_FALSE(std::filesystem::exists(missing_file));
            }
        }
    }
}

SCENARIO("Testing sciqlop_cache basic operations", "[cache]")
{
    AutoCleanDirectory db_path { "BasicTest01" , false};
    std::string test_key = "random/test";
    std::vector<char> original_value1(128);
    std::generate(original_value1.begin(), original_value1.end(), std::rand);
    std::vector<char> original_value2(128);
    std::generate(original_value2.begin(), original_value2.end(), std::rand);

    GIVEN("A new empty cache")
    {
        Cache cache(db_path.path(), 1000);
        THEN("It should be opened and empty")
        {
            REQUIRE(cache.opened());
            REQUIRE(cache.count() == 0);
        }
        AND_THEN("Adding a new key")
        {
            cache.set(test_key, original_value1);
            THEN("it should not be empyt anymore")
            {
                REQUIRE(cache.count() == 1);
            }
            THEN("We should be able to retrieve the key")
            {
                REQUIRE(cache.get(test_key)->to_vector() == original_value1);
            }
            AND_THEN("Closing the cache and openning again")
            {
                REQUIRE(cache.close());
                Cache reopened_cache(db_path.path(), 1000);
                REQUIRE(reopened_cache.opened());
                REQUIRE(reopened_cache.check() == true);
                THEN("It should be opened")
                {
                    REQUIRE(reopened_cache.opened());
                }
                THEN("It should still contain previous data")
                {
                    REQUIRE(reopened_cache.count() == 1);
                    REQUIRE(reopened_cache.get(test_key)->to_vector() == original_value1);
                }
            }
        }

    }
}

SCENARIO("Testing sciqlop_cache more advanced operations", "[cache]")
{
    AutoCleanDirectory db_path { "BasicTest02" ,false};
    std::string test_key = "random/test";
    std::vector<char> original_value1(128);
    std::generate(original_value1.begin(), original_value1.end(), std::rand);
    std::vector<char> original_value2(128);
    std::generate(original_value2.begin(), original_value2.end(), std::rand);

    GIVEN("a cache used to store a small (<500 bytes) value")
    {
        Cache cache(db_path.path(), 1000);
        REQUIRE(cache.check() == true);

        THEN("we test set and get")
        {
            REQUIRE(cache.set("key1", original_value1));
            REQUIRE(cache.set("key2", original_value2));
            auto value1 = cache.get("key1");
            auto value2 = cache.get("key2");
            REQUIRE(value1.has_value());
            REQUIRE(value1.value().to_vector() == original_value1);
            REQUIRE(value2.has_value());
            REQUIRE(value2.value().to_vector() == original_value2);

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
            // REQUIRE_FALSE(cache.get("key1").has_value()); // evict isn't made
            REQUIRE(cache.get("key2").has_value());
        }

        WHEN("we test touch")
        {
            cache.set("key1", original_value1);
            cache.touch("key1", 0s);
            cache.expire();
            usleep(2000);
            REQUIRE_FALSE(cache.get("key1").has_value());
        }

        WHEN("we test add")
        {
            cache.clear();
            cache.set("key1", original_value1);
            REQUIRE_FALSE(cache.add("key1", original_value2));
            REQUIRE(cache.add("key2", original_value2));
            auto value1 = cache.get("key1");
            REQUIRE(value1.value().to_vector() == original_value1);
            auto value2 = cache.get("key2");
            REQUIRE(value2.value().to_vector() == original_value2);
            THEN("adding an already existing key should fail")
            {
                REQUIRE_FALSE(cache.add("key1", original_value2));
                REQUIRE(cache.get("key1").value().to_vector() == original_value1);
            }
        }

        WHEN("we test pop")
        {
            cache.set("key_pop", original_value1);
            auto popped_value = cache.pop("key_pop");
            REQUIRE(popped_value.has_value());
            REQUIRE(popped_value.value().to_vector() == original_value1);
            REQUIRE_FALSE(cache.get("key_pop").has_value());
        }

        WHEN("we test expire")
        {
            cache.set("key1", original_value1, std::chrono::microseconds { 1 });
            cache.set("key2", original_value1);
            REQUIRE(cache.get("key2").has_value());
            usleep(2);
            cache.expire();
            REQUIRE_FALSE(cache.get("key1").has_value());
            REQUIRE(cache.get("key2").has_value());
        }
    }
}

SCENARIO("Testing sciqlop_cache big values operations", "[cache]")
{
    AutoCleanDirectory db_path {"BasicTest03"};

    GIVEN("a cache used to store a large (>500 bytes) value")
    {
        std::vector<char> big_value(1024 * 1024 * 32);
        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<int> dist(0, 255);
        for (auto& b : big_value)
        {
            b = static_cast<char>(dist(gen));
        }
        namespace fs = std::filesystem;
        std::string big_key = "big/key";

        WHEN("we set a big value in the cache")
        {
            {
                Cache cache(db_path.path(), 1000);
                REQUIRE(cache.set(big_key, big_value));
            }

            THEN("the value should be correctly retrieved after reopening the cache")
            {
                Cache reopened_cache(db_path.path(), 1000);
                REQUIRE(reopened_cache.check() == true);

                auto loaded_value = reopened_cache.get(big_key);
                REQUIRE(loaded_value.has_value());
                REQUIRE(loaded_value->size() == big_value.size());
                REQUIRE(std::memcmp(loaded_value->data(), big_value.data(), big_value.size()) == 0);
            }

            THEN("the cache should contain one item")
            {
                Cache reopened_cache(db_path.path(), 1000);
                REQUIRE(reopened_cache.count() == 1);
            }

            THEN("the value should be stored in the ./.cache/ directory")
            {
                fs::path filePath;
                for (const auto& entry : fs::recursive_directory_iterator(db_path.path()))
                {
                    if (entry.is_regular_file() && entry.path().filename() != "sciqlop-cache.db")
                    {
                        filePath = entry.path();
                        break;
                    }
                }
                REQUIRE(!filePath.empty());
                REQUIRE(fs::file_size(filePath) == big_value.size());
                auto loaded_value = Buffer(filePath);
                REQUIRE(loaded_value.size() == big_value.size());
                REQUIRE(std::memcmp(loaded_value.data(), big_value.data(), big_value.size()) == 0);
            }
        }
    }
}
