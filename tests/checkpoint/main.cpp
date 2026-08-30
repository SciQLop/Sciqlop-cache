#include <catch2/catch_test_macros.hpp>
#include <sciqlop_cache.hpp>
#include "../common.hpp"
#include <string>
#include <chrono>
#include <filesystem>
#include <set>
#include <sqlite3.h>
#include <stdexcept>
#include <thread>

static std::size_t read_meta_uint(const std::filesystem::path& db_path, const std::string& key)
{
    sqlite3* db = nullptr;
    REQUIRE(sqlite3_open_v2(db_path.string().c_str(), &db, SQLITE_OPEN_READONLY, nullptr)
        == SQLITE_OK);
    sqlite3_stmt* stmt = nullptr;
    REQUIRE(sqlite3_prepare_v2(db, "SELECT value FROM meta WHERE key = ?;", -1, &stmt, nullptr)
        == SQLITE_OK);
    sqlite3_bind_text(stmt, 1, key.c_str(), -1, SQLITE_TRANSIENT);
    std::size_t value = 0;
    if (sqlite3_step(stmt) == SQLITE_ROW)
        value = static_cast<std::size_t>(sqlite3_column_int64(stmt, 0));
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return value;
}

SCENARIO("volume() is derived from maintained counters", "[counters][volume]")
{
    GIVEN("a Cache with two file-backed values and one inline blob")
    {
        AutoCleanDirectory dir("checkpoint_volume_counters");
        auto db_path = dir.path() / "sciqlop-cache.db";
        Cache cache { dir.path() };
        const std::string file_value(12 * 1024, 'x');
        const std::string inline_value(100, 'y');

        REQUIRE(cache.set("f1", file_value));
        REQUIRE(cache.set("f2", file_value));
        REQUIRE(cache.set("b1", inline_value));

        WHEN("volume() is queried")
        {
            THEN("it reflects both file-backed values plus a bounded DB overhead, in O(1)")
            {
                auto v = cache.volume();
                REQUIRE(v >= 2 * 12 * 1024);
                REQUIRE(v < 2 * 12 * 1024 + 5 * 1024 * 1024);
                REQUIRE(read_meta_uint(db_path, "file_size") == 2 * 12 * 1024);
            }

            AND_THEN("a second instance on the same directory sees the same volume and count")
            {
                Cache other { dir.path() };
                REQUIRE(other.volume() == cache.volume());
                REQUIRE(other.count() == cache.count());
            }
        }

        WHEN("a file-backed key is overwritten with a small inline blob")
        {
            REQUIRE(cache.set("f1", inline_value));
            THEN("the file_size counter drops by the file-backed value's size")
            {
                REQUIRE(read_meta_uint(db_path, "file_size") == 12 * 1024);
                REQUIRE(cache.volume() < 12 * 1024 + 5 * 1024 * 1024);
            }

            AND_WHEN("the remaining file-backed key is deleted")
            {
                REQUIRE(cache.del("f2"));
                THEN("file_size no longer includes it")
                {
                    REQUIRE(read_meta_uint(db_path, "file_size") == 0);
                }
            }
        }
    }
}

SCENARIO("file_size counter is unaffected by the non-file UPDATE trigger path", "[counters][volume]")
{
    GIVEN("a Cache with a blob-backed counter key updated in place via incr()")
    {
        AutoCleanDirectory dir("checkpoint_volume_incr");
        auto db_path = dir.path() / "sciqlop-cache.db";
        Cache cache { dir.path() };

        // First incr() inserts the row with path = NULL (no fsize trigger
        // effect); the second goes through INCR_UPDATE_STMT, a real UPDATE
        // OF size, path that flips NULL -> NULL. This is exactly the path
        // the WHEN-less update trigger must leave file_size alone on.
        cache.incr("counter");
        cache.incr("counter");

        THEN("file_size stays zero")
        {
            REQUIRE(read_meta_uint(db_path, "file_size") == 0);
        }
    }
}

SCENARIO("count() is O(1) and includes not-yet-expired entries", "[counters][count]")
{
    GIVEN("a Cache with three entries, one carrying a future expiration")
    {
        AutoCleanDirectory dir("checkpoint_count_o1");
        Cache cache { dir.path() };
        REQUIRE(cache.set("k1", std::string(10, 'a')));
        REQUIRE(cache.set("k2", std::string(10, 'b'), std::chrono::hours(1)));
        REQUIRE(cache.set("k3", std::string(10, 'c')));

        THEN("count() reflects all three")
        {
            REQUIRE(cache.count() == 3);
        }

        WHEN("one entry is deleted")
        {
            REQUIRE(cache.del("k2"));
            THEN("count() drops accordingly")
            {
                REQUIRE(cache.count() == 2);
            }
        }
    }
}

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

SCENARIO("size() and count() are consistent across instances on the same directory without waiting (Index)", "[counters][index]")
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

SCENARIO("Cache schema creates indexes on expire and last_use", "[schema][index]")
{
    GIVEN("a Cache opened on a fresh directory")
    {
        AutoCleanDirectory dir("checkpoint_indexes");
        Cache cache { dir.path() };
        WHEN("the underlying database is inspected directly")
        {
            sqlite3* db = nullptr;
            REQUIRE(sqlite3_open_v2((dir.path() / "sciqlop-cache.db").string().c_str(), &db,
                        SQLITE_OPEN_READONLY, nullptr)
                == SQLITE_OK);

            std::set<std::string> index_names;
            sqlite3_stmt* stmt = nullptr;
            REQUIRE(sqlite3_prepare_v2(db, "SELECT name FROM sqlite_master WHERE type='index';",
                        -1, &stmt, nullptr)
                == SQLITE_OK);
            while (sqlite3_step(stmt) == SQLITE_ROW)
                index_names.insert(
                    reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0)));
            sqlite3_finalize(stmt);
            sqlite3_close(db);

            THEN("both the expire and last_use indexes are present")
            {
                REQUIRE(index_names.count("idx_cache_expire") == 1);
                REQUIRE(index_names.count("idx_cache_last_use") == 1);
            }
        }
    }
}

SCENARIO("WAL does not grow without bound under a continuous writer", "[wal]")
{
    GIVEN("a Cache written to continuously until a data or time bound is hit")
    {
        AutoCleanDirectory dir("checkpoint_wal_bound");
        std::size_t written = 0;
        {
            Cache cache { dir.path() };
            const std::string v(8000, 'x');
            const std::size_t target = 64 * 1024 * 1024;
            const auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(30);
            std::size_t i = 0;
            while (written < target && std::chrono::steady_clock::now() < deadline)
            {
                REQUIRE(cache.set("key-" + std::to_string(i++), v));
                written += v.size();
            }
            THEN("the WAL file stays bounded (it has been reset), regardless of total volume written")
            {
                auto wal = dir.path() / "sciqlop-cache.db-wal";
                REQUIRE(std::filesystem::exists(wal));
                REQUIRE(std::filesystem::file_size(wal) < 32 * 1024 * 1024);
            }
        }
    }
}

SCENARIO("background LRU eviction never leaves dangling rows under a sustained writer", "[eviction][bg]")
{
    GIVEN("a Cache with a small max_size written to continuously for several seconds")
    {
        AutoCleanDirectory dir("checkpoint_bg_evict_dangling");
        constexpr std::size_t max_size = 2 * 1024 * 1024;
        constexpr std::size_t converged_bound = 4 * 1024 * 1024;
        {
            Cache cache { dir.path(), max_size };
            const std::string v(12 * 1024, 'x'); // file-backed: above the 8 KB threshold
            const auto write_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(4);
            std::size_t i = 0;
            while (std::chrono::steady_clock::now() < write_deadline)
                cache.set("key-" + std::to_string(i++), v);
            // cache destructs here, immediately: joins the bg checkpoint
            // thread without giving it any further, now-uncontended ticks.
            // A dangling row created under contention would otherwise be
            // re-picked as the oldest LRU candidate and cleaned up by a
            // later, uncontended tick — self-healing the very state this
            // test needs to observe. The persisted on-disk state below is
            // exactly what eviction left behind while the writer was live.
        }

        THEN("check() right after reopening finds no dangling rows or orphaned files")
        {
            Cache reopened { dir.path(), max_size };
            auto r = reopened.check(false);
            REQUIRE(r.dangling_rows == 0);
            REQUIRE(r.orphaned_files == 0);

            AND_THEN("left running, it converges size back down to a generous but finite bound")
            {
                const auto settle_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
                while (reopened.size() > converged_bound
                       && std::chrono::steady_clock::now() < settle_deadline)
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                REQUIRE(reopened.size() <= converged_bound);
            }
        }
    }
}

SCENARIO("set_meta rejects reserved keys", "[meta]")
{
    GIVEN("a Cache instance")
    {
        AutoCleanDirectory dir("checkpoint_meta_reserved");
        Cache cache { dir.path() };

        WHEN("setting a reserved key")
        {
            THEN("it throws")
            {
                REQUIRE_THROWS_AS(cache.set_meta("size", "0"), std::runtime_error);
                REQUIRE_THROWS_AS(cache.set_meta("count", "0"), std::runtime_error);
                REQUIRE_THROWS_AS(cache.set_meta("counters_v", "0"), std::runtime_error);
            }
        }
        WHEN("setting a user key")
        {
            cache.set_meta("version", "1.0");
            THEN("get_meta retrieves it")
            {
                REQUIRE(cache.get_meta("version") == "1.0");
            }
        }
    }
}
