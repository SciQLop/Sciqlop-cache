#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <string>
#include <vector>

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>

#include "../common.hpp"
#include "sciqlop_cache/sciqlop_cache.hpp"

SCENARIO("Index basic CRUD", "[index]")
{
    AutoCleanDirectory db_path { "IndexTest01" };
    std::vector<char> v1(100, 'a');
    std::vector<char> v2(200, 'b');

    GIVEN("a new empty index")
    {
        Index idx(db_path.path());
        REQUIRE(idx.opened());
        REQUIRE(idx.count() == 0);

        WHEN("we set and get")
        {
            idx.set("k1", v1);
            REQUIRE(idx.count() == 1);
            REQUIRE(idx.get("k1")->to_vector() == v1);
        }

        WHEN("we overwrite a key")
        {
            idx.set("k1", v1);
            idx.set("k1", v2);
            REQUIRE(idx.count() == 1);
            REQUIRE(idx.get("k1")->to_vector() == v2);
        }

        WHEN("we use add (insert-if-absent)")
        {
            REQUIRE(idx.add("k1", v1));
            REQUIRE_FALSE(idx.add("k1", v2));
            REQUIRE(idx.get("k1")->to_vector() == v1);
        }

        WHEN("we delete")
        {
            idx.set("k1", v1);
            REQUIRE(idx.del("k1"));
            REQUIRE_FALSE(idx.get("k1").has_value());
            REQUIRE_FALSE(idx.del("k1"));
        }

        WHEN("we pop")
        {
            idx.set("k1", v1);
            auto popped = idx.pop("k1");
            REQUIRE(popped.has_value());
            REQUIRE(popped->to_vector() == v1);
            REQUIRE_FALSE(idx.get("k1").has_value());
        }

        WHEN("we pop a non-existent key")
        {
            REQUIRE_FALSE(idx.pop("nope").has_value());
        }

        WHEN("we use exists and keys")
        {
            idx.set("a", v1);
            idx.set("b", v2);
            REQUIRE(idx.exists("a"));
            REQUIRE_FALSE(idx.exists("c"));

            auto k = idx.keys();
            std::sort(k.begin(), k.end());
            REQUIRE(k == std::vector<std::string> { "a", "b" });
        }

        WHEN("we use size")
        {
            idx.set("k1", v1);
            idx.set("k2", v2);
            REQUIRE(idx.size() == 300);
        }

        WHEN("we clear")
        {
            idx.set("k1", v1);
            idx.set("k2", v2);
            idx.clear();
            REQUIRE(idx.count() == 0);
            REQUIRE(idx.size() == 0);
        }

        WHEN("we use incr/decr")
        {
            REQUIRE(idx.incr("counter") == 1);
            REQUIRE(idx.incr("counter") == 2);
            REQUIRE(idx.incr("counter", 10) == 12);
            REQUIRE(idx.decr("counter") == 11);
            REQUIRE(idx.decr("counter", 5) == 6);
        }
    }
}

SCENARIO("Index persistence across reopen", "[index]")
{
    AutoCleanDirectory db_path { "IndexTest02" };
    std::vector<char> v1(100, 'a');

    GIVEN("an index with data")
    {
        {
            Index idx(db_path.path());
            idx.set("persist", v1);
        }

        WHEN("we reopen the index")
        {
            Index idx2(db_path.path());
            THEN("data is still there")
            {
                REQUIRE(idx2.count() == 1);
                REQUIRE(idx2.get("persist")->to_vector() == v1);
            }
        }
    }
}

SCENARIO("Index big values (file storage)", "[index]")
{
    AutoCleanDirectory db_path { "IndexTest03" };
    std::vector<char> big(1024 * 1024, 'x');

    GIVEN("an index storing large values")
    {
        Index idx(db_path.path());
        idx.set("big", big);

        WHEN("we retrieve the value")
        {
            auto loaded = idx.get("big");
            THEN("it matches")
            {
                REQUIRE(loaded.has_value());
                REQUIRE(loaded->size() == big.size());
                REQUIRE(std::memcmp(loaded->data(), big.data(), big.size()) == 0);
            }
        }
    }
}

SCENARIO("Index entries never expire", "[index]")
{
    AutoCleanDirectory db_path { "IndexTest04" };
    std::vector<char> v(50, 'z');

    GIVEN("an index with entries (no expiration possible)")
    {
        Index idx(db_path.path());
        idx.set("permanent", v);

        WHEN("we retrieve it")
        {
            THEN("it is always available")
            {
                REQUIRE(idx.get("permanent").has_value());
                REQUIRE(idx.count() == 1);
            }
        }
    }
}

SCENARIO("Index meta operations", "[index]")
{
    AutoCleanDirectory db_path { "IndexTest05" };

    GIVEN("an index")
    {
        Index idx(db_path.path());

        WHEN("we use set_meta and get_meta")
        {
            idx.set_meta("version", "1.0");
            REQUIRE(idx.get_meta("version") == "1.0");
            REQUIRE_FALSE(idx.get_meta("nonexistent").has_value());
        }
    }
}
