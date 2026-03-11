// tests/check/main.cpp
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include <catch2/catch_all.hpp>
#include <catch2/catch_test_macros.hpp>
#include <sqlite3.h>

#include "../common.hpp"
#include "sciqlop_cache/sciqlop_cache.hpp"

SCENARIO("check() on a clean cache reports no issues", "[check]")
{
    AutoCleanDirectory dir("check_clean");
    Cache cache(dir.path().string());

    GIVEN("A cache with a few entries")
    {
        std::string small = "hello";
        std::vector<char> large(16 * 1024, 'x');
        cache.set("small", std::span(small.data(), small.size()));
        cache.set("large", std::span(large.data(), large.size()));

        WHEN("check() is called without fix")
        {
            auto result = cache.check();

            THEN("Everything is ok")
            {
                REQUIRE(result.ok);
                REQUIRE(result.orphaned_files == 0);
                REQUIRE(result.dangling_rows == 0);
                REQUIRE(result.size_mismatches == 0);
                REQUIRE(result.counters_consistent);
                REQUIRE(result.sqlite_integrity_ok);
            }
        }
    }
}
