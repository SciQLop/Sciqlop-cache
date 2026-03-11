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

SCENARIO("check() detects dangling rows", "[check]")
{
    AutoCleanDirectory dir("check_dangling");
    Cache cache(dir.path().string());

    GIVEN("A large entry whose file is deleted externally")
    {
        std::vector<char> large(16 * 1024, 'x');
        cache.set("big", std::span(large.data(), large.size()));
        auto size_before = cache.size();
        auto count_before = cache.count();

        // Query the DB directly to get the file path
        std::filesystem::path file_path;
        {
            sqlite3* raw_db = nullptr;
            auto db_path = dir.path() / "sciqlop-cache.db";
            sqlite3_open(db_path.string().c_str(), &raw_db);
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(raw_db,
                "SELECT path FROM cache WHERE key = 'big';", -1, &stmt, nullptr);
            if (sqlite3_step(stmt) == SQLITE_ROW)
                file_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            sqlite3_finalize(stmt);
            sqlite3_close(raw_db);
        }
        REQUIRE(!file_path.empty());
        REQUIRE(std::filesystem::exists(file_path));
        std::filesystem::remove(file_path);

        WHEN("check() is called without fix")
        {
            auto result = cache.check();

            THEN("It detects the dangling row")
            {
                REQUIRE_FALSE(result.ok);
                REQUIRE(result.dangling_rows == 1);
            }
        }

        WHEN("check(fix=true) is called")
        {
            auto result = cache.check(true);

            THEN("The dangling row is removed and counters adjusted")
            {
                REQUIRE(result.dangling_rows == 1);
                REQUIRE(cache.count() == count_before - 1);
                REQUIRE(cache.size() < size_before);
                REQUIRE_FALSE(cache.exists("big"));
            }

            AND_THEN("A second check is clean")
            {
                auto result2 = cache.check();
                REQUIRE(result2.ok);
            }
        }
    }
}

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
