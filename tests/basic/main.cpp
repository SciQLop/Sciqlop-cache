#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>


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
        Cache cache(db_path.path());
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
                Cache reopened_cache(db_path.path());
                REQUIRE(reopened_cache.opened());
                REQUIRE(reopened_cache.check().ok);
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
        Cache cache(db_path.path());
        REQUIRE(cache.check().ok);

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
                Cache cache(db_path.path());
                REQUIRE(cache.set(big_key, big_value));
            }

            THEN("the value should be correctly retrieved after reopening the cache")
            {
                Cache reopened_cache(db_path.path());
                REQUIRE(reopened_cache.check().ok);

                auto loaded_value = reopened_cache.get(big_key);
                REQUIRE(loaded_value.has_value());
                REQUIRE(loaded_value->size() == big_value.size());
                REQUIRE(std::memcmp(loaded_value->data(), big_value.data(), big_value.size()) == 0);
            }

            THEN("the cache should contain one item")
            {
                Cache reopened_cache(db_path.path());
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

SCENARIO("Testing cache size tracking", "[cache]")
{
    AutoCleanDirectory db_path {"SizeTest01"};
    Cache cache(db_path.path());

    GIVEN("an empty cache")
    {
        REQUIRE(cache.size() == 0);

        WHEN("we add items and delete them")
        {
            std::vector<char> v100(100, 'a');
            std::vector<char> v200(200, 'b');

            cache.set("k1", v100);
            REQUIRE(cache.size() == 100);

            cache.set("k2", v200);
            REQUIRE(cache.size() == 300);

            cache.del("k1");
            REQUIRE(cache.size() == 200);

            cache.set("k2", v100);
            REQUIRE(cache.size() == 100);

            cache.clear();
            REQUIRE(cache.size() == 0);
        }
    }
}

SCENARIO("Testing sciqlop_cache tag operations", "[cache][tags]")
{
    AutoCleanDirectory db_path {"TagTest01"};
    std::vector<char> v1(100, 'a');
    std::vector<char> v2(200, 'b');
    std::vector<char> v3(150, 'c');

    GIVEN("a cache with tagged entries")
    {
        Cache cache(db_path.path());

        cache.set("k1", v1, "groupA");
        cache.set("k2", v2, "groupA");
        cache.set("k3", v3, "groupB");
        cache.set("k4", v1);

        REQUIRE(cache.count() == 4);

        WHEN("we evict by tag")
        {
            auto evicted = cache.evict_tag("groupA");
            THEN("only tagged entries are removed")
            {
                REQUIRE(evicted == 2);
                REQUIRE(cache.count() == 2);
                REQUIRE_FALSE(cache.get("k1").has_value());
                REQUIRE_FALSE(cache.get("k2").has_value());
                REQUIRE(cache.get("k3").has_value());
                REQUIRE(cache.get("k4").has_value());
            }
        }

        WHEN("we evict a non-existent tag")
        {
            auto evicted = cache.evict_tag("nosuchtag");
            THEN("nothing is removed")
            {
                REQUIRE(evicted == 0);
                REQUIRE(cache.count() == 4);
            }
        }
    }

    GIVEN("a cache with tagged entries using add()")
    {
        Cache cache(db_path.path());

        REQUIRE(cache.add("a1", v1, "mytag"));
        REQUIRE(cache.add("a2", v2, "mytag"));
        REQUIRE_FALSE(cache.add("a1", v3, "mytag"));

        WHEN("we evict the tag")
        {
            cache.evict_tag("mytag");
            THEN("all tagged entries are gone")
            {
                REQUIRE(cache.count() == 0);
            }
        }
    }

    GIVEN("a cache with tagged big values")
    {
        Cache cache(db_path.path());
        std::vector<char> big(1024 * 1024, 'x');

        cache.set("big1", big, "filetag");
        cache.set("big2", big, "filetag");
        REQUIRE(cache.count() == 2);

        WHEN("we evict by tag")
        {
            auto evicted = cache.evict_tag("filetag");
            THEN("entries and files are removed")
            {
                REQUIRE(evicted == 2);
                REQUIRE(cache.count() == 0);
            }
        }
    }
}

SCENARIO("Testing sciqlop_cache incr/decr operations", "[cache][incr]")
{
    AutoCleanDirectory db_path {"IncrTest01"};

    GIVEN("an empty cache")
    {
        Cache cache(db_path.path());

        WHEN("we incr a non-existent key")
        {
            auto result = cache.incr("counter");
            THEN("it returns default (0) + 1 = 1")
            {
                REQUIRE(result == 1);
            }
        }

        WHEN("we incr a non-existent key with custom default")
        {
            auto result = cache.incr("counter", 1, 10);
            THEN("it returns default (10) + 1 = 11")
            {
                REQUIRE(result == 11);
            }
        }

        WHEN("we incr multiple times")
        {
            cache.incr("counter");
            cache.incr("counter");
            auto result = cache.incr("counter");
            THEN("it accumulates correctly")
            {
                REQUIRE(result == 3);
            }
        }

        WHEN("we decr a key")
        {
            cache.incr("counter", 5, 0);
            auto result = cache.decr("counter", 2);
            THEN("the value is decremented")
            {
                REQUIRE(result == 3);
            }
        }

        WHEN("we incr with a custom delta")
        {
            auto result = cache.incr("counter", 42, 0);
            THEN("it returns the delta")
            {
                REQUIRE(result == 42);
            }
        }

        WHEN("we decr below zero")
        {
            auto result = cache.decr("counter", 5, 0);
            THEN("it goes negative")
            {
                REQUIRE(result == -5);
            }
        }
    }
}

SCENARIO("Testing sciqlop_cache keys()", "[cache]")
{
    AutoCleanDirectory db_path {"KeysTest01"};

    GIVEN("a cache with several entries")
    {
        Cache cache(db_path.path());
        std::vector<char> v(50, 'x');

        cache.set("alpha", v);
        cache.set("beta", v);
        cache.set("gamma", v);

        WHEN("we call keys()")
        {
            auto k = cache.keys();
            THEN("all keys are returned")
            {
                REQUIRE(k.size() == 3);
                std::sort(k.begin(), k.end());
                REQUIRE(k == std::vector<std::string>{"alpha", "beta", "gamma"});
            }
        }

        WHEN("one key expires and we call keys()")
        {
            cache.set("ephemeral", v, 0s);
            usleep(2000);
            auto k = cache.keys();
            THEN("expired keys are excluded")
            {
                std::sort(k.begin(), k.end());
                REQUIRE(k == std::vector<std::string>{"alpha", "beta", "gamma"});
            }
        }

        WHEN("we clear and call keys()")
        {
            cache.clear();
            THEN("no keys are returned")
            {
                REQUIRE(cache.keys().empty());
            }
        }
    }
}

SCENARIO("Testing set and add with expire+tag combinations", "[cache][tags]")
{
    AutoCleanDirectory db_path {"CombinedTest01"};
    std::vector<char> v(80, 'z');

    GIVEN("a cache")
    {
        Cache cache(db_path.path());

        WHEN("we set with both expire and tag")
        {
            cache.set("k1", v, 3600s, "group1");
            REQUIRE(cache.count() == 1);
            REQUIRE(cache.get("k1").has_value());

            THEN("evict_tag removes it")
            {
                cache.evict_tag("group1");
                REQUIRE(cache.count() == 0);
            }
        }

        WHEN("we set with immediate expire and tag, then expire()")
        {
            cache.set("k2", v, 0s, "group2");
            usleep(2000);
            cache.expire();
            THEN("the entry is gone")
            {
                REQUIRE_FALSE(cache.get("k2").has_value());
                REQUIRE(cache.count() == 0);
            }
        }

        WHEN("we add with expire")
        {
            REQUIRE(cache.add("k3", v, 3600s));
            REQUIRE(cache.get("k3").has_value());
            REQUIRE_FALSE(cache.add("k3", v, 3600s));
        }

        WHEN("we add with expire+tag")
        {
            REQUIRE(cache.add("k4", v, 3600s, "tagX"));
            REQUIRE(cache.get("k4").has_value());
            cache.evict_tag("tagX");
            REQUIRE(cache.count() == 0);
        }

        WHEN("we add with immediate expire+tag")
        {
            cache.add("k5", v, 0s, "tagY");
            usleep(2000);
            THEN("the entry expires")
            {
                REQUIRE_FALSE(cache.get("k5").has_value());
            }
        }
    }
}

SCENARIO("Testing del and pop edge cases", "[cache]")
{
    AutoCleanDirectory db_path {"EdgeTest01"};
    std::vector<char> v(64, 'e');

    GIVEN("a cache with one entry")
    {
        Cache cache(db_path.path());
        cache.set("existing", v);

        WHEN("we del a non-existent key")
        {
            THEN("it returns false")
            {
                REQUIRE_FALSE(cache.del("nonexistent"));
            }
        }

        WHEN("we pop a non-existent key")
        {
            auto result = cache.pop("nonexistent");
            THEN("it returns nullopt")
            {
                REQUIRE_FALSE(result.has_value());
            }
        }

        WHEN("we del an existing key twice")
        {
            REQUIRE(cache.del("existing"));
            THEN("the second del returns false")
            {
                REQUIRE_FALSE(cache.del("existing"));
            }
        }
    }
}

SCENARIO("Testing sciqlop_cache clear with big values", "[cache]")
{
    AutoCleanDirectory db_path {"ClearTest01"};

    GIVEN("a cache with large values stored as files")
    {
        Cache cache(db_path.path());
        std::vector<char> big_value(1024 * 1024);
        std::generate(big_value.begin(), big_value.end(), std::rand);

        cache.set("big1", big_value);
        cache.set("big2", big_value);
        REQUIRE(cache.count() == 2);

        WHEN("we clear the cache")
        {
            cache.clear();

            THEN("count should be zero")
            {
                REQUIRE(cache.count() == 0);
            }

            THEN("no data files should remain on disk")
            {
                namespace fs = std::filesystem;
                int file_count = 0;
                for (const auto& entry : fs::recursive_directory_iterator(db_path.path()))
                {
                    if (entry.is_regular_file())
                    {
                        auto fname = entry.path().filename().string();
                        bool is_db_file = fname == "sciqlop-cache.db"
                            || fname.ends_with("-wal") || fname.ends_with("-shm");
                        if (!is_db_file)
                            ++file_count;
                    }
                }
                REQUIRE(file_count == 0);
            }
        }
    }
}

SCENARIO("Cache statistics track hits and misses", "[stats]")
{
    AutoCleanDirectory db_path { "StatsTest" };
    std::vector<char> value(64, 'x');

    GIVEN("a fresh cache")
    {
        Cache cache(db_path.path());
        auto s = cache.stats();
        REQUIRE(s.hits == 0);
        REQUIRE(s.misses == 0);

        WHEN("getting an existing key")
        {
            cache.set("k1", value);
            cache.get("k1");

            THEN("hits increments")
            {
                auto s = cache.stats();
                REQUIRE(s.hits == 1);
                REQUIRE(s.misses == 0);
            }
        }

        WHEN("getting a missing key")
        {
            cache.get("nonexistent");

            THEN("misses increments")
            {
                auto s = cache.stats();
                REQUIRE(s.hits == 0);
                REQUIRE(s.misses == 1);
            }
        }

        WHEN("getting an expired key")
        {
            cache.set("k2", value, 1s);
            std::this_thread::sleep_for(2s);
            cache.get("k2");

            THEN("it counts as a miss")
            {
                auto s = cache.stats();
                REQUIRE(s.hits == 0);
                REQUIRE(s.misses == 1);
            }
        }

        WHEN("performing a mix of hits and misses")
        {
            cache.set("a", value);
            cache.set("b", value);
            cache.get("a");
            cache.get("b");
            cache.get("c");
            cache.get("d");

            THEN("counts are accurate")
            {
                auto s = cache.stats();
                REQUIRE(s.hits == 2);
                REQUIRE(s.misses == 2);
            }
        }

        WHEN("reset_stats is called")
        {
            cache.set("k", value);
            cache.get("k");
            cache.get("missing");
            cache.reset_stats();

            THEN("counters return to zero")
            {
                auto s = cache.stats();
                REQUIRE(s.hits == 0);
                REQUIRE(s.misses == 0);
            }
        }
    }
}

SCENARIO("expire() correctly updates size and count counters", "[cache][expire]")
{
    AutoCleanDirectory db_path { "ExpireCounters01" };
    std::vector<char> v100(100, 'a');
    std::vector<char> v200(200, 'b');

    GIVEN("a cache with expiring and non-expiring entries")
    {
        Cache cache(db_path.path());
        cache.set("permanent1", v100);
        cache.set("permanent2", v200);
        cache.set("expiring1", v100, 1s);
        cache.set("expiring2", v200, 1s);

        REQUIRE(cache.size() == 600);

        WHEN("entries expire and expire() is called")
        {
            std::this_thread::sleep_for(2s);
            cache.expire();

            THEN("size reflects only the remaining entries")
            {
                REQUIRE(cache.size() == 300);
                REQUIRE(cache.count() == 2);
            }
        }

        WHEN("we add more entries between expiration check and expire()")
        {
            std::this_thread::sleep_for(2s);
            cache.set("late_arrival", v100);
            cache.expire();

            THEN("late arrival is preserved and counters are correct")
            {
                REQUIRE(cache.get("late_arrival").has_value());
                REQUIRE(cache.size() == 400);
                REQUIRE(cache.count() == 3);
            }
        }
    }

    GIVEN("a cache where all entries expire")
    {
        Cache cache(db_path.path());
        cache.set("e1", v100, 1s);
        cache.set("e2", v100, 1s);
        cache.set("e3", v100, 1s);

        REQUIRE(cache.size() == 300);
        REQUIRE(cache.count() == 3);

        WHEN("all entries expire")
        {
            std::this_thread::sleep_for(2s);
            cache.expire();

            THEN("counters are zero")
            {
                REQUIRE(cache.size() == 0);
                REQUIRE(cache.count() == 0);
            }
        }
    }

    GIVEN("a cache with large (file-backed) expiring entries")
    {
        Cache cache(db_path.path());
        std::vector<char> big(16000, 'x');
        cache.set("big_expire", big, 1s);
        cache.set("big_keep", big);

        REQUIRE(cache.size() == 32000);

        WHEN("the expiring entry expires")
        {
            std::this_thread::sleep_for(2s);
            cache.expire();

            THEN("size accounts for the removed file-backed entry")
            {
                REQUIRE(cache.size() == 16000);
                REQUIRE(cache.count() == 1);
            }
        }
    }
}
