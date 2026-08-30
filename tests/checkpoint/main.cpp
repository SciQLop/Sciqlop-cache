#include <catch2/catch_test_macros.hpp>
#include <sciqlop_cache.hpp>
#include "../common.hpp"
#include <string>
#include <thread>
#include <chrono>
#include <filesystem>

SCENARIO("size() and count() are consistent across instances on the same directory without waiting", "[counters]")
{
    GIVEN("two Cache instances opened on the same directory")
    {
        AutoCleanDirectory dir("checkpoint_shared_counters");
        Cache a { dir.path() };
        Cache b { dir.path() };
        WHEN("one instance writes")
        {
            const std::string v(100, 'x');
            REQUIRE(a.set("k1", v));
            REQUIRE(a.set("k2", v));
            THEN("the other sees the new size and count immediately")
            {
                REQUIRE(b.count() == 2);
                REQUIRE(b.size() == 200);
            }
        }
        WHEN("one instance overwrites and deletes")
        {
            REQUIRE(a.set("k1", std::string(100, 'x')));
            REQUIRE(a.set("k1", std::string(50, 'y')));
            REQUIRE(a.set("k2", std::string(100, 'x')));
            REQUIRE(a.del("k2"));
            THEN("size reflects the replace and the delete")
            {
                REQUIRE(b.count() == 1);
                REQUIRE(b.size() == 50);
            }
        }
    }
}

SCENARIO("size() and count() are consistent across instances on the same directory without waiting", "[counters][index]")
{
    GIVEN("two Index instances opened on the same directory")
    {
        AutoCleanDirectory dir("checkpoint_shared_counters_index");
        Index a { dir.path() };
        Index b { dir.path() };
        WHEN("one instance writes")
        {
            const std::string v(100, 'x');
            REQUIRE(a.set("k1", v));
            REQUIRE(a.set("k2", v));
            THEN("the other sees the new size and count immediately")
            {
                REQUIRE(b.count() == 2);
                REQUIRE(b.size() == 200);
            }
        }
        WHEN("one instance overwrites and deletes")
        {
            REQUIRE(a.set("k1", std::string(100, 'x')));
            REQUIRE(a.set("k1", std::string(50, 'y')));
            REQUIRE(a.set("k2", std::string(100, 'x')));
            REQUIRE(a.del("k2"));
            THEN("size reflects the replace and the delete")
            {
                REQUIRE(b.count() == 1);
                REQUIRE(b.size() == 50);
            }
        }
    }
}

SCENARIO("WAL does not grow without bound under a continuous writer", "[wal]")
{
    GIVEN("a Cache written to continuously for several seconds")
    {
        AutoCleanDirectory dir("checkpoint_wal_bound");
        std::size_t written = 0;
        {
            Cache cache { dir.path() };
            const std::string v(8000, 'x');
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(6);
            std::size_t i = 0;
            while (std::chrono::steady_clock::now() < deadline)
            {
                REQUIRE(cache.set("key-" + std::to_string(i++), v));
                written += v.size();
            }
            THEN("the WAL file is a fraction of what was written (it has been reset)")
            {
                auto wal = dir.path() / "sciqlop-cache.db-wal";
                REQUIRE(std::filesystem::exists(wal));
                REQUIRE(std::filesystem::file_size(wal) < written / 2);
            }
        }
    }
}
