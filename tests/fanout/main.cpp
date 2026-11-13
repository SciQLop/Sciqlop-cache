#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <thread>
#include <vector>

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../common.hpp"
#include "sciqlop_cache/sciqlop_cache.hpp"

using namespace std::chrono_literals;

SCENARIO("FanoutCache basic CRUD", "[fanout]")
{
    AutoCleanDirectory db_path { "FanoutTest01" };
    std::vector<char> v1(100, 'a');
    std::vector<char> v2(200, 'b');

    GIVEN("a new FanoutCache with 4 shards")
    {
        FanoutCache fc(db_path.path(), 4);
        REQUIRE(fc.shard_count() == 4);

        WHEN("we set and get a key")
        {
            fc.set("hello", v1);
            auto result = fc.get("hello");
            THEN("we retrieve the same value")
            {
                REQUIRE(result.has_value());
                REQUIRE(result->to_vector() == v1);
            }
        }

        WHEN("we check exists")
        {
            fc.set("hello", v1);
            THEN("exists returns true for present key, false for absent")
            {
                REQUIRE(fc.exists("hello"));
                REQUIRE_FALSE(fc.exists("missing"));
            }
        }

        WHEN("we delete a key")
        {
            fc.set("hello", v1);
            REQUIRE(fc.del("hello"));
            THEN("it is gone") { REQUIRE_FALSE(fc.get("hello").has_value()); }
        }

        WHEN("we pop a key")
        {
            fc.set("hello", v1);
            auto result = fc.pop("hello");
            THEN("we get the value and it is removed")
            {
                REQUIRE(result.has_value());
                REQUIRE(result->to_vector() == v1);
                REQUIRE_FALSE(fc.exists("hello"));
            }
        }
    }
}

SCENARIO("FanoutCache set with expiration", "[fanout][expire]")
{
    AutoCleanDirectory db_path { "FanoutTestExpire" };
    std::vector<char> v1(100, 'a');

    GIVEN("a FanoutCache")
    {
        FanoutCache fc(db_path.path(), 4);

        WHEN("we set a key with expiration")
        {
            fc.set("expkey", v1, 1s);
            REQUIRE(fc.get("expkey").has_value());

            THEN("it expires after the timeout")
            {
                std::this_thread::sleep_for(2s);
                REQUIRE_FALSE(fc.get("expkey").has_value());
            }
        }

        WHEN("we set a key with a tag")
        {
            fc.set("tagged", v1, "mytag");
            REQUIRE(fc.get("tagged").has_value());
        }

        WHEN("we touch a key without an expiration")
        {
            fc.set("expkey", v1, 2s);
            REQUIRE(fc.touch("expkey"));
            std::this_thread::sleep_for(2100ms);
            REQUIRE(fc.get("expkey").has_value());
        }

        WHEN("we touch a missing key")
        {
            REQUIRE_FALSE(fc.touch("missing", 10s));
        }
    }
}

SCENARIO("FanoutCache aggregated operations", "[fanout][aggregate]")
{
    AutoCleanDirectory db_path { "FanoutTestAgg" };
    std::vector<char> v1(100, 'a');

    GIVEN("a FanoutCache with several entries spread across shards")
    {
        FanoutCache fc(db_path.path(), 4);
        for (int i = 0; i < 20; ++i)
            fc.set("key" + std::to_string(i), v1);

        THEN("count returns total across all shards")
        {
            REQUIRE(fc.count() == 20);
        }

        THEN("size returns total across all shards")
        {
            REQUIRE(fc.size() == 20 * 100);
        }

        THEN("keys returns all keys")
        {
            auto k = fc.keys();
            REQUIRE(k.size() == 20);
        }

        WHEN("we clear")
        {
            fc.clear();
            THEN("everything is gone")
            {
                REQUIRE(fc.count() == 0);
                REQUIRE(fc.size() == 0);
            }
        }

        THEN("check returns true") { REQUIRE(fc.check().ok); }
    }
}

SCENARIO("FanoutCache eviction", "[fanout][eviction]")
{
    AutoCleanDirectory db_path { "FanoutTestEvict" };
    std::vector<char> v1(100, 'a');

    GIVEN("a FanoutCache with max_size per shard")
    {
        FanoutCache fc(db_path.path(), 4, 200);

        WHEN("we exceed max_size across shards")
        {
            for (int i = 0; i < 40; ++i)
                fc.set("key" + std::to_string(i), v1);

            fc.evict();

            THEN("total size is reduced") { REQUIRE(fc.size() <= 4 * 200); }
        }
    }
}

SCENARIO("FanoutCache tags", "[fanout][tags]")
{
    AutoCleanDirectory db_path { "FanoutTestTags" };
    std::vector<char> v1(100, 'a');

    GIVEN("a FanoutCache with tagged entries")
    {
        FanoutCache fc(db_path.path(), 4);
        for (int i = 0; i < 10; ++i)
            fc.set("tagged" + std::to_string(i), v1, "group1");
        for (int i = 0; i < 5; ++i)
            fc.set("other" + std::to_string(i), v1);

        WHEN("we evict by tag")
        {
            auto evicted = fc.evict_tag("group1");

            THEN("tagged entries are removed")
            {
                REQUIRE(evicted == 10);
                REQUIRE(fc.count() == 5);
            }
        }
    }
}

SCENARIO("FanoutCache stats", "[fanout][stats]")
{
    AutoCleanDirectory db_path { "FanoutTestStats" };
    std::vector<char> v1(100, 'a');

    GIVEN("a FanoutCache with some entries")
    {
        FanoutCache fc(db_path.path(), 4);
        fc.set("k1", v1);
        fc.set("k2", v1);

        WHEN("we get existing and missing keys")
        {
            (void)fc.get("k1");
            (void)fc.get("k2");
            (void)fc.get("missing");

            auto s = fc.stats();
            THEN("hits and misses are aggregated")
            {
                REQUIRE(s.hits == 2);
                REQUIRE(s.misses == 1);
            }
        }

        WHEN("we reset stats")
        {
            (void)fc.get("k1");
            fc.reset_stats();
            auto s = fc.stats();
            THEN("counters are zero")
            {
                REQUIRE(s.hits == 0);
                REQUIRE(s.misses == 0);
            }
        }
    }
}

SCENARIO("FanoutCache incr/decr", "[fanout][incr]")
{
    AutoCleanDirectory db_path { "FanoutTestIncr" };

    GIVEN("a FanoutCache")
    {
        FanoutCache fc(db_path.path(), 4);

        WHEN("we incr a new key")
        {
            auto val = fc.incr("counter", 5, 10);
            THEN("it starts from default + delta") { REQUIRE(val == 15); }
        }

        WHEN("we decr")
        {
            fc.incr("counter", 0, 100);
            auto val = fc.decr("counter", 3);
            THEN("value decreases") { REQUIRE(val == 97); }
        }
    }
}

SCENARIO("FanoutCache meta", "[fanout][meta]")
{
    AutoCleanDirectory db_path { "FanoutTestMeta" };

    GIVEN("a FanoutCache")
    {
        FanoutCache fc(db_path.path(), 4);

        WHEN("we set and get meta")
        {
            fc.set_meta("version", "1.0");
            THEN("we retrieve it") { REQUIRE(fc.get_meta("version") == "1.0"); }
        }
    }
}

SCENARIO("FanoutIndex basic CRUD", "[fanout][index]")
{
    AutoCleanDirectory db_path { "FanoutIndexTest01" };
    std::vector<char> v1(100, 'a');
    std::vector<char> v2(200, 'b');

    GIVEN("a new FanoutIndex with 4 shards")
    {
        FanoutIndex fi(db_path.path(), 4);

        WHEN("we set, get, count, size, keys, del, clear")
        {
            fi.set("k1", v1);
            fi.set("k2", v2);

            THEN("count and size aggregate")
            {
                REQUIRE(fi.count() == 2);
                REQUIRE(fi.size() == 300);
            }

            THEN("keys returns all keys")
            {
                auto k = fi.keys();
                REQUIRE(k.size() == 2);
            }

            THEN("get returns correct values")
            {
                REQUIRE(fi.get("k1")->to_vector() == v1);
                REQUIRE(fi.get("k2")->to_vector() == v2);
            }
        }

        WHEN("we delete and clear")
        {
            fi.set("k1", v1);
            fi.set("k2", v2);
            fi.del("k1");
            REQUIRE(fi.count() == 1);
            fi.clear();
            REQUIRE(fi.count() == 0);
        }
    }
}

SCENARIO("FanoutCache stores ByteChunks as their concatenation", "[fanout][chunks]")
{
    AutoCleanDirectory db_path { "FanoutChunks" };
    FanoutCache fc(db_path.path(), 4);
    std::vector<char> head(10, 'h'), body(20000, 'b');
    ByteChunks chunks { { std::span<const char>(head), std::span<const char>(body) } };
    std::vector<char> expected(head);
    expected.insert(expected.end(), body.begin(), body.end());

    REQUIRE(fc.set("k", chunks, std::string("tag")));
    REQUIRE(fc.get("k")->to_vector() == expected);
    REQUIRE(fc.add("k2", chunks, 60s));
    REQUIRE(fc.get("k2")->to_vector() == expected);
}

SCENARIO("FanoutCache forwards add() with expire and tag, decr() and set_max_cache_size()",
         "[fanout]")
{
    AutoCleanDirectory db_path { "FanoutForwarding" };
    FanoutCache fc(db_path.path(), 4);
    std::vector<char> v(10, 'v');

    REQUIRE(fc.add("a", v, std::string("t")));
    REQUIRE(fc.add("b", v, 60s, std::string("t")));
    REQUIRE(fc.evict_tag("t") == 2);
    REQUIRE(fc.decr("n", 3) == -3);
    fc.set_max_cache_size(1'000'000);
    REQUIRE(fc.set("c", v));
    REQUIRE(fc.get("c")->to_vector() == v);
}

static bool shard_holds(const std::filesystem::path& shard_dir, const std::string& key)
{
    Index shard(shard_dir);
    return shard.exists(key);
}

SCENARIO("A new fanout store routes keys with XXH3, the same on every platform",
         "[fanout][portable]")
{
    AutoCleanDirectory db_path { "FanoutXXH3" };
    std::vector<char> v(10, 'v');
    {
        FanoutIndex fi(db_path.path(), 8);
        fi.set("", v);
        fi.set("hello", v);
        fi.set("speasy/amda/imf/2020-01-01", v);
    }
    // Golden shards: XXH3_64bits(key) % 8, see subprojects/xxhash.wrap.
    REQUIRE(shard_holds(db_path.path() / "xxh3_02", ""));
    REQUIRE(shard_holds(db_path.path() / "xxh3_05", "hello"));
    REQUIRE(shard_holds(db_path.path() / "xxh3_05", "speasy/amda/imf/2020-01-01"));
    REQUIRE_FALSE(std::filesystem::exists(db_path.path() / "00"));
}

SCENARIO("An existing fanout store keeps its std::hash routing", "[fanout][portable]")
{
    AutoCleanDirectory db_path { "FanoutLegacy" };
    std::vector<char> v(10, 'v');
    const std::string key = "hello";
    const auto legacy_shard = std::hash<std::string> {}(key) % 4;
    for (std::size_t i = 0; i < 4; ++i)
    {
        auto dir = db_path.path() / fmt::format("{:02d}", i);
        std::filesystem::create_directories(dir);
        if (i == legacy_shard)
            Index(dir).set(key, v);
    }

    FanoutIndex fi(db_path.path(), 4);
    REQUIRE(fi.get(key)->to_vector() == v);
    fi.set("other", v);
    REQUIRE(fi.exists("other"));
    REQUIRE_FALSE(std::filesystem::exists(db_path.path() / "xxh3_00"));
}

SCENARIO("FanoutCache set_many() writes each shard's items", "[fanout][set_many]")
{
    AutoCleanDirectory db_path { "FanoutSetMany" };
    FanoutCache fc(db_path.path(), 4);
    std::vector<std::pair<std::string, std::vector<char>>> items;
    for (int i = 0; i < 100; ++i)
        items.emplace_back("k" + std::to_string(i), std::vector<char>(i % 3 ? 10 : 20000, 'v'));

    fc.set_many(items, std::chrono::seconds { 3600 }, std::string { "batch" });
    REQUIRE(fc.count() == 100);
    for (const auto& [key, value] : items)
        REQUIRE(fc.get(key)->to_vector() == value);
    REQUIRE(fc.evict_tag("batch") == 100);

    FanoutIndex fi(db_path.path() / "index", 4);
    fi.set_many(items);
    REQUIRE(fi.count() == 100);
}
