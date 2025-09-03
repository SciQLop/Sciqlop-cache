#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <optional>
#include <random>
#include <string>
#include <vector>

#include <catch2/catch_all.hpp>

#include <cpp_utils/lifetime/scope_leaving_guards.hpp>

#include "../common.hpp"
#include "sciqlop_cache/database.hpp"
#include "sciqlop_cache/sciqlop_cache.hpp"
using namespace std::chrono_literals;

SCENARIO("Simple sqlite database", "[database]")
{
    AutoCleanDirectory db_path { "DBTest01" };

    GIVEN("A new database")
    {
        Database db;
        REQUIRE(db.open(db_path.path() / "test.db"));
        REQUIRE(db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);"));
        THEN("It should be empty")
        {
            auto count = db.exec<std::size_t>("SELECT COUNT(*) FROM test;");
            REQUIRE(count.has_value());
            REQUIRE(*count == 0);
        }
        WHEN("we insert a row")
        {
            REQUIRE(db.exec("INSERT INTO test (value) VALUES (?);", "Hello, World!"));
            THEN("we should be able to retrieve it")
            {
                auto result = db.exec<std::string>("SELECT value FROM test WHERE id = ?;", 1);
                REQUIRE(result.has_value());
                REQUIRE(*result == "Hello, World!");
            }
            THEN("we should be able to count rows")
            {
                auto count = db.exec<std::size_t>("SELECT COUNT(*) FROM test;");
                REQUIRE(count.has_value());
                REQUIRE(*count == 1);
            }
            THEN("We should be able to remove it")
            {
                REQUIRE(db.exec("DELETE FROM test WHERE id = ?;", 1));
                auto count = db.exec<std::size_t>("SELECT COUNT(*) FROM test;");
                REQUIRE(count.has_value());
                REQUIRE(*count == 0);
            }
        }
        WHEN("we insert multiple rows")
        {
            REQUIRE(db.exec("INSERT INTO test (value) VALUES (?);", "First"));
            REQUIRE(db.exec("INSERT INTO test (value) VALUES (?);", "Second"));
            REQUIRE(db.exec("INSERT INTO test (value) VALUES (?);", "Third"));
            THEN("we should be able to retrieve them all")
            {
                auto results
                    = db.exec<std::vector<std::string>>("SELECT value FROM test ORDER BY id;");
                REQUIRE(results.has_value());
                REQUIRE(results->size() == 3);
                REQUIRE((*results)[0] == "First");
                REQUIRE((*results)[1] == "Second");
                REQUIRE((*results)[2] == "Third");
            }
        }
    }
}

SCENARIO("Database reopen", "[database]")
{
    AutoCleanDirectory db_path { "DBTest02" };

    GIVEN("A populated database")
    {
        Database db;
        REQUIRE(db.open(db_path.path() / "test.db", INIT_STMTS));
        REQUIRE(db.exec("CREATE TABLE test (id INTEGER PRIMARY KEY, value TEXT);"));
        REQUIRE(db.exec("INSERT INTO test (value) VALUES (?);", "Hello, World!"));
        REQUIRE(db.exec("INSERT INTO test (value) VALUES (?);", "Second Row"));
        REQUIRE(db.exec("INSERT INTO test (value) VALUES (?);", "Third Row"));
        REQUIRE(db.exec("INSERT INTO test (value) VALUES (?);", "Fourth Row"));
        THEN("we we should be able to retrieve data after reopening")
        {
            REQUIRE(db.close());
            auto new_db = Database();
            REQUIRE(new_db.open(db_path.path() / "test.db"));
            auto count = new_db.exec<std::size_t>("SELECT COUNT(*) FROM test;");
            REQUIRE(count.has_value());
            REQUIRE(*count == 4);
            auto results
                = new_db.exec<std::vector<std::string>>("SELECT value FROM test ORDER BY id;");
            REQUIRE(results.has_value());
            REQUIRE(results->size() == 4);
            REQUIRE((*results)[0] == "Hello, World!");
            REQUIRE((*results)[1] == "Second Row");
            REQUIRE((*results)[2] == "Third Row");
            REQUIRE((*results)[3] == "Fourth Row");
        }
    }
}
