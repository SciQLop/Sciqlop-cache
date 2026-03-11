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

SCENARIO("check() detects orphaned files", "[check]")
{
    AutoCleanDirectory dir("check_orphan");
    Cache cache(dir.path().string());

    GIVEN("An extra file planted in the storage directory")
    {
        // Plant an orphan file in the UUID directory structure
        auto orphan_dir = dir.path() / "ab" / "cd";
        std::filesystem::create_directories(orphan_dir);
        auto orphan_path = orphan_dir / "abcd-fake-uuid-orphan";
        {
            std::ofstream ofs(orphan_path, std::ios::binary);
            ofs << "orphan data";
        }
        REQUIRE(std::filesystem::exists(orphan_path));

        WHEN("check() is called without fix")
        {
            auto result = cache.check();

            THEN("It detects the orphaned file")
            {
                REQUIRE_FALSE(result.ok);
                REQUIRE(result.orphaned_files == 1);
            }
        }

        WHEN("check(fix=true) is called")
        {
            auto result = cache.check(true);

            THEN("The orphan is deleted")
            {
                REQUIRE(result.orphaned_files == 1);
                REQUIRE_FALSE(std::filesystem::exists(orphan_path));
            }

            AND_THEN("A second check is clean")
            {
                auto result2 = cache.check();
                REQUIRE(result2.ok);
            }
        }
    }
}

SCENARIO("check() detects size mismatches", "[check]")
{
    AutoCleanDirectory dir("check_size");
    Cache cache(dir.path().string());

    GIVEN("A large entry whose file is truncated externally")
    {
        std::vector<char> large(16 * 1024, 'x');
        cache.set("truncated", std::span(large.data(), large.size()));
        auto original_size = cache.size();

        // Query the DB directly to get the file path
        std::filesystem::path file_path;
        {
            sqlite3* raw_db = nullptr;
            auto db_path = dir.path() / "sciqlop-cache.db";
            sqlite3_open(db_path.string().c_str(), &raw_db);
            sqlite3_stmt* stmt = nullptr;
            sqlite3_prepare_v2(raw_db,
                "SELECT path FROM cache WHERE key = 'truncated';", -1, &stmt, nullptr);
            if (sqlite3_step(stmt) == SQLITE_ROW)
                file_path = reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
            sqlite3_finalize(stmt);
            sqlite3_close(raw_db);
        }
        REQUIRE(!file_path.empty());
        REQUIRE(std::filesystem::exists(file_path));
        {
            std::ofstream ofs(file_path, std::ios::binary | std::ios::trunc);
            ofs << "short";
        }

        WHEN("check() is called without fix")
        {
            auto result = cache.check();

            THEN("It detects the size mismatch")
            {
                REQUIRE_FALSE(result.ok);
                REQUIRE(result.size_mismatches == 1);
            }
        }

        WHEN("check(fix=true) is called")
        {
            auto result = cache.check(true);

            THEN("The DB size column is corrected")
            {
                REQUIRE(result.size_mismatches == 1);
                REQUIRE(cache.size() < original_size);
            }

            AND_THEN("A second check is clean")
            {
                auto result2 = cache.check();
                REQUIRE(result2.ok);
            }
        }
    }
}

SCENARIO("check() detects counter drift", "[check]")
{
    AutoCleanDirectory dir("check_counters");
    Index index(dir.path().string());

    GIVEN("An index with entries added via raw SQL (bypassing counters)")
    {
        std::string val = "hello";
        index.set("legit", std::span(val.data(), val.size()));

        // Insert a row directly via SQL, bypassing counter updates
        sqlite3* raw_db = nullptr;
        auto db_path = dir.path() / "sciqlop-cache.db";
        sqlite3_open(db_path.string().c_str(), &raw_db);
        sqlite3_exec(raw_db,
            "INSERT INTO cache (key, value, size) VALUES ('sneaky', X'AABB', 2);",
            nullptr, nullptr, nullptr);
        sqlite3_close(raw_db);

        WHEN("check() is called without fix")
        {
            auto result = index.check();

            THEN("It detects counter inconsistency")
            {
                REQUIRE_FALSE(result.ok);
                REQUIRE_FALSE(result.counters_consistent);
            }
        }

        WHEN("check(fix=true) is called")
        {
            auto result = index.check(true);

            THEN("Counters are reloaded from DB")
            {
                REQUIRE_FALSE(result.counters_consistent);
                // After fix, count and size should reflect both rows
                REQUIRE(index.count() == 2);
            }

            AND_THEN("A second check is clean")
            {
                auto result2 = index.check();
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
